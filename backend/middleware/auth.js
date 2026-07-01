/**
 * Authentication Middleware
 * Validates Telegram Mini App initData (per official Telegram docs)
 */
const crypto = require('crypto');
const { setDoc } = require('../services/firebase');
const { isAdmin } = require('../services/bot');

function validateTelegramAuth(initData) {
  try {
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

    if (computedHash !== hash) return null;

    const authDate = parseInt(params.get('auth_date'));
    if (Date.now() / 1000 - authDate > 86400) return null;

    const userStr = params.get('user');
    if (!userStr) return null;
    return JSON.parse(userStr);
  } catch (err) {
    console.error('[AUTH] validation error:', err.message);
    return null;
  }
}

async function requireAuth(req, res, next) {
  try {
    const initData = req.headers['x-telegram-init-data'] || req.query.initData;
    if (!initData) return res.status(401).json({ error: 'Missing authentication' });

    const user = validateTelegramAuth(initData);
    if (!user) return res.status(401).json({ error: 'Invalid authentication' });

    req.telegramUser = user;
    req.telegramUserId = String(user.id);
    await upsertUser(user);
    next();
  } catch (err) {
    console.error('[AUTH] middleware error:', err.message);
    res.status(500).json({ error: 'Authentication failed' });
  }
}

async function requireAdmin(req, res, next) {
  try {
    const initData = req.headers['x-telegram-init-data'] || req.query.initData;
    const adminKey = req.headers['x-admin-key'];

    if (adminKey && adminKey === process.env.JWT_SECRET) {
      req.isServerAdmin = true;
      return next();
    }
    if (!initData) return res.status(401).json({ error: 'Missing authentication' });

    const user = validateTelegramAuth(initData);
    if (!user) return res.status(401).json({ error: 'Invalid authentication' });
    if (!isAdmin(user.id)) return res.status(403).json({ error: 'Admin access required' });

    req.telegramUser = user;
    req.telegramUserId = String(user.id);
    next();
  } catch (err) {
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
        await upsertUser(user);
      }
    }
    next();
  } catch { next(); }
}

async function upsertUser(tgUser) {
  try {
    await setDoc('users', String(tgUser.id), {
      telegramId: tgUser.id,
      firstName: tgUser.first_name || '',
      lastName: tgUser.last_name || '',
      username: tgUser.username || '',
      photoUrl: tgUser.photo_url || '',
      languageCode: tgUser.language_code || 'en',
      lastSeen: new Date().toISOString(),
    });
  } catch (err) { console.error('[AUTH] upsertUser error:', err.message); }
}

module.exports = { requireAuth, requireAdmin, softAuth, validateTelegramAuth };
