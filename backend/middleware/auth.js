/**
 * middleware/auth.js
 * Validates Telegram Mini App initData (per official Telegram docs) and
 * gates admin routes.
 */
'use strict';

const crypto = require('crypto');
const { setDoc } = require('../services/firebase');
const { isAdmin } = require('../services/bot');
const { sanitizeText } = require('../utils/validators');
const { makeLogger } = require('../utils/logger');
const log = makeLogger('middleware/auth.js');

function validateTelegramAuth(initData) {
  try {
    if (!initData || typeof initData !== 'string' || initData.length > 4000) return null;

    const params = new URLSearchParams(initData);
    const hash = params.get('hash');
    if (!hash) return null;
    params.delete('hash');

    const arr = [];
    params.forEach((v, k) => arr.push(`${k}=${v}`));
    arr.sort();
    const dataCheckString = arr.join('\n');

    const secretKey = crypto.createHmac('sha256', 'WebAppData')
      .update(process.env.TELEGRAM_BOT_TOKEN).digest();
    const computedHash = crypto.createHmac('sha256', secretKey)
      .update(dataCheckString).digest('hex');

    const hashBuf = Buffer.from(hash, 'hex');
    const computedBuf = Buffer.from(computedHash, 'hex');
    if (hashBuf.length !== computedBuf.length || !crypto.timingSafeEqual(hashBuf, computedBuf)) return null;

    const authDate = parseInt(params.get('auth_date'), 10);
    if (!Number.isFinite(authDate) || Date.now() / 1000 - authDate > 86400) return null;

    const userStr = params.get('user');
    if (!userStr) return null;
    const user = JSON.parse(userStr);
    if (!user || !Number.isFinite(Number(user.id))) return null;
    return user;
  } catch (err) {
    log.error('validateTelegramAuth', 'Validation error', err);
    return null;
  }
}

async function requireAuth(req, res, next) {
  try {
    const initData = req.headers['x-telegram-init-data'] || req.query.initData;

    if (!initData) {
      if (process.env.NODE_ENV !== 'production') {
        req.telegramUser = { id: 'dev-user', first_name: 'Developer' };
        req.telegramUserId = 'dev-user';
        return next();
      }
      return res.status(401).json({ error: 'Missing authentication' });
    }

    const user = validateTelegramAuth(initData);
    if (!user) return res.status(401).json({ error: 'Invalid authentication' });

    req.telegramUser = user;
    req.telegramUserId = String(user.id);

    upsertUser(user).catch((err) => log.warn('requireAuth', 'upsertUser failed (non-fatal)', { reason: err.message }));

    next();
  } catch (err) {
    log.error('requireAuth', 'Authentication failed', err);
    res.status(500).json({ error: 'Authentication failed' });
  }
}

function timingSafeStringEqual(a, b) {
  const bufA = Buffer.from(String(a || ''));
  const bufB = Buffer.from(String(b || ''));
  if (bufA.length !== bufB.length) return false;
  return crypto.timingSafeEqual(bufA, bufB);
}

async function requireAdmin(req, res, next) {
  try {
    const initData = req.headers['x-telegram-init-data'] || req.query.initData;
    const adminKey = req.headers['x-admin-key'];

    if (adminKey && process.env.JWT_SECRET && timingSafeStringEqual(adminKey, process.env.JWT_SECRET)) {
      req.isServerAdmin = true;
      return next();
    }
    if (!initData) return res.status(401).json({ error: 'Missing authentication' });

    const user = validateTelegramAuth(initData);
    if (!user) return res.status(401).json({ error: 'Invalid authentication' });
    if (!isAdmin(user.id)) {
      log.warn('requireAdmin', 'Unauthorized admin access attempt', { userId: user.id });
      return res.status(403).json({ error: 'Admin access required' });
    }

    req.telegramUser = user;
    req.telegramUserId = String(user.id);
    next();
  } catch (err) {
    log.error('requireAdmin', 'Authentication failed', err);
    res.status(500).json({ error: 'Authentication failed' });
  }
}

async function softAuth(req, res, next) {
  try {
    const initData = req.headers['x-telegram-init-data'] || req.query.initData;
    if (initData) {
      const user = validateTelegramAuth(initData);
      if (user) {
        req.telegramUser = user;
        req.telegramUserId = String(user.id);
        upsertUser(user).catch((err) => log.warn('softAuth', 'upsertUser failed (non-fatal)', { reason: err.message }));
      }
    }
    next();
  } catch (err) {
    log.error('softAuth', 'Soft auth error (continuing unauthenticated)', err);
    next();
  }
}

async function upsertUser(tgUser) {
  await setDoc('users', String(tgUser.id), {
    telegramId: tgUser.id,
    firstName: sanitizeText(tgUser.first_name, { max: 100 }),
    lastName: sanitizeText(tgUser.last_name, { max: 100 }),
    username: sanitizeText(tgUser.username, { max: 100 }),
    photoUrl: sanitizeText(tgUser.photo_url, { max: 500 }),
    languageCode: sanitizeText(tgUser.language_code, { max: 10 }) || 'en',
    lastSeen: new Date().toISOString(),
  });
}

module.exports = { requireAuth, requireAdmin, softAuth, validateTelegramAuth };
                           
