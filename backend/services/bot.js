/**
 * ============================================================================
 *  MYFLIX ADMIN BOT — bot.js
 * ============================================================================
 *  Production-grade Telegram admin bot for the MYFLIX platform.
 *
 *  Stack:
 *    - node-telegram-bot-api (webhook mode, driven by ../routes/webhook.js)
 *    - Firebase Admin SDK / Firestore (via ./firebase.js -> getDB / getAdmin)
 *    - Designed to run inside an Express app on Render
 *
 *  Exports (kept 100% compatible with the existing server.js / webhook.js /
 *  middleware/auth.js consumers):
 *    - initBot()               -> sets the Telegram webhook, boots the bot
 *    - isAdmin(chatId)         -> boolean admin check
 *    - processUpdate(update)   -> feeds a raw Telegram update into the bot
 *
 *  Firestore index design note (Requirement 5 — "auto indexes"):
 *  --------------------------------------------------------------------------
 *  Firestore automatically maintains a single-field index for every field,
 *  and it can resolve *any* combination of pure equality ("==") filters
 *  using those single-field indexes without a manual composite index.
 *  A composite index is only required when a query mixes an equality
 *  filter with a range/orderBy on a *different* field.
 *
 *  Every query in this file is deliberately written to avoid that
 *  situation: list/filter queries use equality-only filters and sort the
 *  (small, capped) result set in memory, while range/orderBy queries
 *  (stats, "largest video", "last upload", "uploads today") only ever use
 *  a single field. The net effect is that this bot never needs a manual
 *  composite index. As a safety net, saveIndexAwareError() below detects
 *  Firestore's FAILED_PRECONDITION "missing index" error and surfaces the
 *  one-click index-creation link Firestore itself generates, straight to
 *  the admin chat and the console logs.
 * ============================================================================
 */

'use strict';

const crypto = require('crypto');
const TelegramBot = require('node-telegram-bot-api');
const { getDB, getAdmin } = require('./firebase');
const { parseMediaInfo } = require('./parser');
const mtproto = require('./mtproto');

// ============================================================================
// SECTION 1 — CONFIG & CONSTANTS
// ============================================================================

const token = process.env.TELEGRAM_BOT_TOKEN;
const BASE_URL = process.env.RENDER_EXTERNAL_URL || process.env.TELEGRAM_WEBHOOK_URL;
const ADMIN_IDS = (process.env.ADMIN_USER_IDS || '')
  .split(',')
  .map((s) => Number(String(s).trim()))
  .filter((n) => Number.isFinite(n) && n !== 0);

const CATEGORIES = Object.freeze({
  ANIME: 'Anime',
  MOVIES: 'Movies',
  WEBSERIES: 'Web Series',
});

const CATEGORY_CODE = { [CATEGORIES.ANIME]: 'A', [CATEGORIES.MOVIES]: 'M', [CATEGORIES.WEBSERIES]: 'W' };
const CODE_CATEGORY = { A: CATEGORIES.ANIME, M: CATEGORIES.MOVIES, W: CATEGORIES.WEBSERIES };
const TYPE_CATEGORY = { anime: CATEGORIES.ANIME, movie: CATEGORIES.MOVIES, webseries: CATEGORIES.WEBSERIES };

const VIDEOS_COLLECTION = 'videos';

const DEFAULT_MAX_BUFFER = 150;
const HARD_MAX_BUFFER = 500;

const RATE_LIMIT_WINDOW_MS = 10_000;
const RATE_LIMIT_MAX_COMMANDS = 20;

const PENDING_ACTION_TTL_MS = 2 * 60_000;
const PENDING_CLEANUP_INTERVAL_MS = 60_000;

const CACHE_TTL_MS = 20_000;
const LIST_CACHE_TTL_MS = 30_000;
const LIST_PAGE_SIZE = 8;
const LIST_FETCH_LIMIT = 500;

const FIND_RESULT_LIMIT = 15;
const IN_QUERY_CHUNK = 10;
const BATCH_WRITE_LIMIT = 450; // stay under Firestore's 500 op/batch hard cap
const GET_ALL_CHUNK = 300;
const DELETE_QUERY_PAGE = 400;

const LOG_RING_SIZE = 200;

// ============================================================================
// SECTION 2 — LOG RING BUFFER & LOGGER
// ============================================================================

const LOG_RING = [];

function pushLog(level, message) {
  LOG_RING.push({ t: Date.now(), level, message });
  if (LOG_RING.length > LOG_RING_SIZE) LOG_RING.shift();
}

function fmtMeta(meta) {
  if (!meta || Object.keys(meta).length === 0) return '';
  const parts = Object.entries(meta)
    .filter(([, v]) => v !== undefined && v !== null && v !== '')
    .map(([k, v]) => `${k}=${v}`);
  return parts.length ? ` | ${parts.join(' ')}` : '';
}

function logSuccess(msg, meta) {
  const line = `✅ ${msg}${fmtMeta(meta)}`;
  console.log(line);
  pushLog('success', line);
}
function logWarn(msg, meta) {
  const line = `⚠️ ${msg}${fmtMeta(meta)}`;
  console.warn(line);
  pushLog('warn', line);
}
function logError(msg, err, meta) {
  const line = `❌ ${msg}${fmtMeta(meta)}${err ? ` | ${err.message}` : ''}`;
  console.error(line);
  if (err && err.stack) console.error(err.stack);
  pushLog('error', line + (err && err.stack ? `\n${err.stack}` : ''));
}
function logInfo(msg, meta) {
  const line = `ℹ️ ${msg}${fmtMeta(meta)}`;
  console.log(line);
  pushLog('info', line);
}

// ============================================================================
// SECTION 3 — GENERIC UTILITIES
// ============================================================================

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function slugify(str) {
  return String(str)
    .toLowerCase()
    .trim()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/(^-+|-+$)/g, '')
    .slice(0, 80) || 'untitled';
}

function formatBytes(bytes) {
  if (!bytes || bytes <= 0) return '0 B';
  const units = ['B', 'KB', 'MB', 'GB', 'TB'];
  let i = 0;
  let n = bytes;
  while (n >= 1024 && i < units.length - 1) {
    n /= 1024;
    i++;
  }
  return `${n.toFixed(n >= 10 || i === 0 ? 0 : 2)} ${units[i]}`;
}

function formatDuration(ms) {
  if (ms < 1000) return `${ms}ms`;
  return `${(ms / 1000).toFixed(2)}s`;
}

function formatUptime(seconds) {
  const d = Math.floor(seconds / 86400);
  const h = Math.floor((seconds % 86400) / 3600);
  const m = Math.floor((seconds % 3600) / 60);
  const s = Math.floor(seconds % 60);
  return `${d}d ${h}h ${m}m ${s}s`;
}

function toMillis(ts) {
  if (!ts) return 0;
  if (typeof ts.toMillis === 'function') return ts.toMillis();
  if (ts instanceof Date) return ts.getTime();
  if (typeof ts === 'number') return ts;
  return 0;
}

function shortToken() {
  return crypto.randomBytes(5).toString('hex'); // 10 chars
}

function chunkArray(arr, size) {
  const out = [];
  for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size));
  return out;
}

function isTransientError(err) {
  if (!err) return false;
  const code = err.code;
  const transientCodes = new Set([4, 8, 10, 13, 14, 'unavailable', 'deadline-exceeded', 'aborted', 'internal', 'resource-exhausted']);
  const transientMsgs = ['ECONNRESET', 'ETIMEDOUT', 'socket hang up', 'UNAVAILABLE'];
  if (transientCodes.has(code)) return true;
  if (err.message && transientMsgs.some((m) => err.message.includes(m))) return true;
  return false;
}

async function withRetry(fn, { retries = 3, baseDelayMs = 300, label = 'operation' } = {}) {
  let attempt = 0;
  // eslint-disable-next-line no-constant-condition
  while (true) {
    try {
      return await fn();
    } catch (err) {
      attempt++;
      if (attempt > retries || !isTransientError(err)) throw err;
      const delay = baseDelayMs * 2 ** (attempt - 1) + Math.floor(Math.random() * 150);
      logWarn(`Retrying ${label} (attempt ${attempt}/${retries}) after transient error`, { reason: err.message });
      await sleep(delay);
    }
  }
}

async function withTimeout(promise, ms, label = 'operation') {
  let timer;
  const timeout = new Promise((_, reject) => {
    timer = setTimeout(() => reject(new Error(`⏱ Timeout after ${ms}ms: ${label}`)), ms);
  });
  try {
    return await Promise.race([promise, timeout]);
  } finally {
    clearTimeout(timer);
  }
}

class ValidationError extends Error {
  constructor(message) {
    super(message);
    this.name = 'ValidationError';
  }
}

function assertValid(condition, message) {
  if (!condition) throw new ValidationError(message);
}

// ============================================================================
// SECTION 4 — VALIDATION
// ============================================================================

function validateTitle(title) {
  const t = String(title || '').trim();
  assertValid(t.length >= 1 && t.length <= 200, 'Title must be 1-200 characters.');
  assertValid(!t.includes('|'), 'Title cannot contain the "|" character.');
  return t;
}

function validateSeason(season) {
  const n = parseInt(season, 10);
  assertValid(Number.isInteger(n) && n >= 1 && n <= 999, 'Season must be a whole number between 1 and 999.');
  return n;
}

function validateEpisode(episode) {
  const n = parseInt(episode, 10);
  assertValid(Number.isInteger(n) && n >= 1 && n <= 9999, 'Episode must be a whole number between 1 and 9999.');
  return n;
}

function validateLanguage(language) {
  const l = String(language || '').trim();
  assertValid(l.length >= 1 && l.length <= 60, 'Language must be 1-60 characters.');
  assertValid(/^[\p{L}\p{N}\s,/-]+$/u.test(l), 'Language contains invalid characters.');
  return l;
}

function validateChannelId(channelId) {
  const n = Number(channelId);
  assertValid(Number.isInteger(n) && n !== 0, 'Invalid channelId.');
  return n;
}

function validateMessageId(messageId) {
  const n = Number(messageId);
  assertValid(Number.isInteger(n) && n > 0, 'Invalid messageId.');
  return n;
}

function normalizeCategory(input) {
  const raw = String(input || '').trim().toLowerCase();
  if (['anime', 'a', 'animes'].includes(raw)) return CATEGORIES.ANIME;
  if (['movie', 'movies', 'm'].includes(raw)) return CATEGORIES.MOVIES;
  if (['webseries', 'web series', 'web-series', 'series', 'w', 'ws'].includes(raw)) return CATEGORIES.WEBSERIES;
  throw new ValidationError('Category must be one of: anime, movie, webseries.');
}

function typeForCategory(category) {
  if (category === CATEGORIES.ANIME) return 'anime';
  if (category === CATEGORIES.MOVIES) return 'movie';
  return 'webseries';
}

// ============================================================================
// SECTION 5 — RUNTIME STATE
// ============================================================================

/** @type {Record<number, Array<{file_id:string,file_unique_id:string,channelId:number,messageId:number,fileSizeBytes?:number,addedAt:number}>>} */
const adminBuffer = {};
/** @type {Record<number, Set<string>>} */
const bufferSeenIds = {};
/** @type {Record<number, number>} */
const maxBufferByChat = {};

/** @type {Map<string, {chatId:number, userId:number, kind:string, payload:any, createdAt:number, promptMessageId:number}>} */
const pendingActions = new Map();

/** @type {Map<string, {docs:any[], fetchedAt:number}>} */
const listCache = new Map();

/** @type {Map<string, {value:any, fetchedAt:number}>} */
const genericCache = new Map();

/** @type {Map<number, {count:number, windowStart:number}>} */
const rateLimitState = new Map();

const BOT_START_TIME = Date.now();

setInterval(() => {
  const now = Date.now();
  for (const [k, v] of pendingActions.entries()) {
    if (now - v.createdAt > PENDING_ACTION_TTL_MS) pendingActions.delete(k);
  }
  for (const [k, v] of listCache.entries()) {
    if (now - v.fetchedAt > LIST_CACHE_TTL_MS * 3) listCache.delete(k);
  }
  for (const [k, v] of genericCache.entries()) {
    if (now - v.fetchedAt > CACHE_TTL_MS * 3) genericCache.delete(k);
  }
}, PENDING_CLEANUP_INTERVAL_MS).unref?.();

function getCache(key) {
  const hit = genericCache.get(key);
  if (hit && Date.now() - hit.fetchedAt < CACHE_TTL_MS) return hit.value;
  return null;
}
function setCache(key, value) {
  genericCache.set(key, { value, fetchedAt: Date.now() });
}
function invalidateCategoryCaches(category) {
  genericCache.delete('stats');
  genericCache.delete('storage');
  if (category) listCache.delete(`list:${CATEGORY_CODE[category]}`);
}

// ============================================================================
// SECTION 6 — BOT INSTANCE
// ============================================================================

if (!token) {
  logError('TELEGRAM_BOT_TOKEN is not set — bot cannot start', new Error('Missing TELEGRAM_BOT_TOKEN'));
}

const bot = new TelegramBot(token, { webHook: true });
let db;
let botUsername = '';

async function initBot() {
  try {
    db = getDB();

    if (!BASE_URL) {
      logError('Missing webhook base URL (RENDER_EXTERNAL_URL / TELEGRAM_WEBHOOK_URL)', new Error('Missing base URL'));
      return;
    }

    const webhookUrl = `${BASE_URL.replace(/\/+$/, '')}/webhook`;
    await withRetry(
      () => bot.setWebHook(webhookUrl, { secret_token: process.env.WEBHOOK_SECRET, drop_pending_updates: true }),
      { retries: 3, label: 'setWebHook' }
    );
    logSuccess('Webhook registered', { url: webhookUrl });

    try {
      const me = await bot.getMe();
      botUsername = me?.username || '';
      logInfo('Bot identity resolved', { username: botUsername, id: me?.id });
    } catch (err) {
      logWarn('Could not resolve bot identity via getMe()', { reason: err.message });
    }

    logSuccess('MYFLIX Admin Bot started', { admins: ADMIN_IDS.length });
  } catch (err) {
    logError('initBot() failed', err);
  }
}

function isAdmin(chatId) {
  return ADMIN_IDS.includes(Number(chatId));
}

// ---- global safety nets (Requirement 1) -----------------------------------

process.on('uncaughtException', (err) => {
  logError('uncaughtException — bot kept alive', err);
});
process.on('unhandledRejection', (reason) => {
  const err = reason instanceof Error ? reason : new Error(String(reason));
  logError('unhandledRejection — bot kept alive', err);
});

bot.on('polling_error', (err) => logError('Telegram polling_error', err));
bot.on('webhook_error', (err) => logError('Telegram webhook_error', err));
bot.on('error', (err) => logError('Telegram generic error', err));

// ============================================================================
// SECTION 7 — SAFE TELEGRAM HELPERS
// ============================================================================

async function safeSendMessage(chatId, text, options = {}) {
  try {
    return await bot.sendMessage(chatId, text, { parse_mode: 'HTML', disable_web_page_preview: true, ...options });
  } catch (err) {
    logError('safeSendMessage failed', err, { chatId });
    return null;
  }
}

async function safeEditMessageText(text, options = {}) {
  try {
    return await bot.editMessageText(text, { parse_mode: 'HTML', disable_web_page_preview: true, ...options });
  } catch (err) {
    // "message is not modified" is harmless (e.g. refresh with identical content)
    if (!/message is not modified/i.test(err.message)) {
      logError('safeEditMessageText failed', err);
    }
    return null;
  }
}

async function safeAnswerCallback(callbackQueryId, options = {}) {
  try {
    await bot.answerCallbackQuery(callbackQueryId, options);
  } catch (err) {
    logError('safeAnswerCallback failed', err);
  }
}

function userMeta(msg) {
  return {
    userId: msg.from?.id,
    username: msg.from?.username ? `@${msg.from.username}` : msg.from?.first_name,
    chatId: msg.chat?.id,
  };
}

function friendlyError(err) {
  if (err instanceof ValidationError) return `⚠️ ${err.message}`;
  return `❌ Something went wrong: ${err.message}\nThe error has been logged — please try again.`;
}

// Detects Firestore's own "create this index" links (Requirement 5 fallback).
function surfaceIndexLink(chatId, err) {
  if (err && typeof err.message === 'string' && /index/i.test(err.message) && /https?:\/\//.test(err.message)) {
    const url = err.message.match(/https?:\/\/\S+/)?.[0];
    logWarn('Firestore requested a composite index — one-click link below', { url });
    if (url) safeSendMessage(chatId, `⚠️ Firestore needs an index for this query.\nCreate it here (one click):\n${url}`);
    return true;
  }
  return false;
}

// ============================================================================
// SECTION 8 — RATE LIMITING & COMMAND WRAPPER
// ============================================================================

function checkRateLimit(userId) {
  const now = Date.now();
  const state = rateLimitState.get(userId);
  if (!state || now - state.windowStart > RATE_LIMIT_WINDOW_MS) {
    rateLimitState.set(userId, { count: 1, windowStart: now });
    return true;
  }
  state.count++;
  return state.count <= RATE_LIMIT_MAX_COMMANDS;
}

function cmdRegex(name) {
  return new RegExp(`^\\/${name}(?:@\\w+)?(?:\\s+([\\s\\S]+))?$`, 'i');
}

/**
 * Registers an admin command with unified logging, auth, rate limiting,
 * timing and error handling. `handler(msg, argString)` may throw —
 * ValidationError becomes a friendly warning, anything else is logged with
 * a full stack trace and reported back without ever crashing the process.
 */
function registerCommand(name, handler, { adminOnly = true } = {}) {
  bot.onText(cmdRegex(name), async (msg, match) => {
    const start = Date.now();
    const meta = userMeta(msg);
    const argString = (match && match[1]) ? match[1].trim() : '';

    try {
      if (adminOnly && !isAdmin(meta.chatId)) {
        logWarn(`Unauthorized access attempt: /${name}`, meta);
        await safeSendMessage(meta.chatId, '⛔ Access denied. This attempt has been logged.');
        return;
      }

      if (!checkRateLimit(meta.userId)) {
        logWarn(`Rate limit exceeded: /${name}`, meta);
        await safeSendMessage(meta.chatId, '⚠️ Too many commands — please slow down and try again in a few seconds.');
        return;
      }

      logInfo(`Command received: /${name}`, meta);

      await handler(msg, argString);

      const elapsed = Date.now() - start;
      logSuccess(`Command completed: /${name}`, { ...meta, responseTime: formatDuration(elapsed) });
    } catch (err) {
      const elapsed = Date.now() - start;
      logError(`Command failed: /${name}`, err, { ...meta, responseTime: formatDuration(elapsed) });
      if (!surfaceIndexLink(meta.chatId, err)) {
        await safeSendMessage(meta.chatId, friendlyError(err));
      }
    }
  });
}

// ============================================================================
// SECTION 9 — FIRESTORE HELPERS
// ============================================================================

function videoDocId(type, title, season, episode) {
  return `${type}_${slugify(title)}_s${season}_ep${episode}`;
}

function buildVideoData({ title, category, season, episode, fileId, fileUniqueId, channelId, messageId, language, fileSizeBytes, quality }) {
  const admin = getAdmin();
  const data = {
    title,
    seriesTitle: title,
    category,
    season,
    episode,
    telegram_file_id: fileId,
    file_unique_id: fileUniqueId,
    channelId,
    messageId,
    language,
    published: true,
    createdAt: admin.firestore.FieldValue.serverTimestamp(),
    updatedAt: admin.firestore.FieldValue.serverTimestamp(),
  };
  // Ignore undefined values explicitly (belt-and-braces on top of
  // db.settings({ ignoreUndefinedProperties: true }) in firebase.js).
  if (fileSizeBytes !== undefined && fileSizeBytes !== null) data.fileSizeBytes = fileSizeBytes;
  if (quality) data.quality = quality;
  return data;
}

/** Commits an array of {ref, data} set-operations in Firestore-safe batches. */
async function batchedSet(items) {
  const chunks = chunkArray(items, BATCH_WRITE_LIMIT);
  let written = 0;
  for (const chunk of chunks) {
    await withRetry(async () => {
      const batch = db.batch();
      for (const { ref, data } of chunk) batch.set(ref, data, { merge: false });
      await batch.commit();
    }, { label: 'batchedSet' });
    written += chunk.length;
  }
  return written;
}

/** Commits an array of doc refs as deletes in Firestore-safe batches. */
async function batchedDelete(refs) {
  const chunks = chunkArray(refs, BATCH_WRITE_LIMIT);
  let deleted = 0;
  for (const chunk of chunks) {
    await withRetry(async () => {
      const batch = db.batch();
      for (const ref of chunk) batch.delete(ref);
      await batch.commit();
    }, { label: 'batchedDelete' });
    deleted += chunk.length;
  }
  return deleted;
}

/** Repeatedly pages through a query and deletes matches until none remain. */
async function deleteByQuery(queryBuilder) {
  let totalDeleted = 0;
  // eslint-disable-next-line no-constant-condition
  while (true) {
    const snap = await withRetry(() => queryBuilder().limit(DELETE_QUERY_PAGE).get(), { label: 'deleteByQuery.get' });
    if (snap.empty) break;
    const refs = snap.docs.map((d) => d.ref);
    totalDeleted += await batchedDelete(refs);
    if (snap.size < DELETE_QUERY_PAGE) break;
  }
  return totalDeleted;
}

async function getAllChunked(refs) {
  const chunks = chunkArray(refs, GET_ALL_CHUNK);
  const out = [];
  for (const chunk of chunks) {
    if (chunk.length === 0) continue;
    const docs = await withRetry(() => db.getAll(...chunk), { label: 'getAllChunked' });
    out.push(...docs);
  }
  return out;
}

async function findExistingFileUniqueIds(uniqueIds) {
  const found = new Set();
  const chunks = chunkArray([...new Set(uniqueIds)], IN_QUERY_CHUNK);
  for (const chunk of chunks) {
    const snap = await withRetry(
      () => db.collection(VIDEOS_COLLECTION).where('file_unique_id', 'in', chunk).select('file_unique_id').get(),
      { label: 'findExistingFileUniqueIds' }
    );
    snap.forEach((d) => found.add(d.get('file_unique_id')));
  }
  return found;
}

// ============================================================================
// SECTION 10 — BUFFER MANAGEMENT (Requirement 6)
// ============================================================================

function getMaxBuffer(chatId) {
  return maxBufferByChat[chatId] || DEFAULT_MAX_BUFFER;
}

bot.on('video', (msg) => handleVideoBuffer(msg, msg.video).catch((err) => logError('video buffer handler failed', err)));
bot.on('document', (msg) => {
  if (msg.document?.mime_type?.startsWith('video/')) {
    handleVideoBuffer(msg, msg.document).catch((err) => logError('document buffer handler failed', err));
  }
});

async function handleVideoBuffer(msg, media) {
  const chatId = msg.chat.id;
  if (!isAdmin(chatId)) return;

  if (!adminBuffer[chatId]) adminBuffer[chatId] = [];
  if (!bufferSeenIds[chatId]) bufferSeenIds[chatId] = new Set();

  const max = getMaxBuffer(chatId);
  if (adminBuffer[chatId].length >= max) {
    return safeSendMessage(chatId, `⚠️ Buffer full (${max}). Save it with /saveanime, /savemovie or /savewebseries first, or /clearbuffer.`);
  }

  if (bufferSeenIds[chatId].has(media.file_unique_id)) {
    return safeSendMessage(chatId, '⚠️ Duplicate video skipped (already in buffer).');
  }

  const rawText = msg.caption || media.file_name || '';
  const detected = parseMediaInfo(rawText);

  bufferSeenIds[chatId].add(media.file_unique_id);
  adminBuffer[chatId].push({
    file_id: media.file_id,
    file_unique_id: media.file_unique_id,
    channelId: msg.chat.id,
    messageId: msg.message_id,
    fileSizeBytes: media.file_size,
    addedAt: Date.now(),
    detected,
  });

  logInfo('Video buffered', { ...userMeta(msg), bufferCount: adminBuffer[chatId].length, detectedTitle: detected.title, detectedS: detected.season, detectedE: detected.episode });

  const detectedLine = detected.title
    ? `\n🔎 Detected: ${detected.title}${detected.season != null ? ` S${detected.season}` : ''}${detected.episode != null ? `E${detected.episode}` : ''}${detected.language ? ` · ${detected.language}` : ''}${detected.quality ? ` · ${detected.quality}` : ''}`
    : '';
  safeSendMessage(chatId, `✅ Buffered (${adminBuffer[chatId].length}/${max})${detectedLine}`);
}

// ============================================================================
// SECTION 11 — COMMANDS: start / help
// ============================================================================

registerCommand('start', async (msg) => {
  await safeSendMessage(msg.chat.id, [
    '🎬 <b>Welcome to MYFLIX Admin Bot</b>',
    '',
    isAdmin(msg.chat.id) ? 'You are recognized as an admin.' : 'This bot is restricted to admins.',
    '',
    'Send /help to see every available command.',
  ].join('\n'), {});
}, { adminOnly: false });

registerCommand('help', async (msg) => {
  const text = [
    '📚 <b>MYFLIX Admin Commands</b>',
    '',
    '<b>Saving</b>',
    '/saveanime Title|Season|Language|StartEp',
    '/savemovie Title|Season|Language|StartEp',
    '/savewebseries Title|Season|Language|StartEp',
    '/autosave Title|Season|Language|StartEp — category auto-detected too',
    '<i>Any field may be blank or "auto" to use what was detected from the caption/filename (e.g. "Demon Slayer S02E04 Hindi 720p").</i>',
    '',
    '<b>Buffer</b>',
    '/showbuffer — view current buffer',
    '/maxbuffer &lt;n&gt; — set buffer capacity (1-500)',
    '/clearbuffer — empty the buffer',
    '',
    '<b>Deleting</b> (all ask for confirmation)',
    '/deleteanime Title',
    '/devletewebseries Title'.replace('devlete', 'delete'),
    '/deletemovie Title',
    '/deleteseason category|Title|Season',
    '/deleteepisode category|Title|Season|Episode',
    '/deletetitle docId  <i>or</i>  category|Title',
    '/deletelanguage category|Language',
    '/deleteallanime',
    '/deleteallmovies',
    '/deleteallwebseries',
    '',
    '<b>Browsing</b>',
    '/listanime · /listmovies · /listwebseries',
    '/find title|episode|season|language|category &lt;value&gt;',
    '',
    '<b>Monitoring</b>',
    '/stats · /storage · /health · /logs · /ping',
    '',
    '<i>category can be: anime, movie, webseries</i>',
  ].join('\n');
  await safeSendMessage(msg.chat.id, text, {});
});

// ============================================================================
// SECTION 12 — COMMANDS: save (Requirement 3 & 5)
// ============================================================================

const AUTO_TOKEN = /^(auto|)$/i;

/**
 * @param {object} msg
 * @param {'anime'|'movie'|'webseries'|null} type null => category is
 *        auto-detected per batch from the first buffered item's caption.
 * @param {string} argString "Title|Season|Language|StartEpisode", any
 *        segment may be blank or "auto" to fall back to what the parser
 *        detected from Telegram captions/filenames.
 */
async function executeSave(msg, type, argString) {
  const chatId = msg.chat.id;
  const start = Date.now();

  const buf = adminBuffer[chatId] || [];
  assertValid(buf.length > 0, 'Buffer is empty. Forward videos to the bot first.');

  const parts = (argString || '').split('|').map((s) => s.trim());
  const first = buf[0].detected || {};

  const category = type ? TYPE_CATEGORY[type] : normalizeCategory(first.category || 'Movies');
  const resolvedType = type || typeForCategory(category);

  const titleRaw = AUTO_TOKEN.test(parts[0]) ? first.title : parts[0];
  assertValid(titleRaw, 'Could not detect a title automatically — usage: /save' + resolvedType + ' Title|Season|Language|StartEpisode');
  const title = validateTitle(titleRaw);

  const explicitSeason = parts[1] && !AUTO_TOKEN.test(parts[1]) ? validateSeason(parts[1]) : null;
  const explicitLanguage = parts[2] && !AUTO_TOKEN.test(parts[2]) ? validateLanguage(parts[2]) : null;
  const startEpisode = parts[3] && !AUTO_TOKEN.test(parts[3]) ? validateEpisode(parts[3]) : 1;

  const language = explicitLanguage || (first.language ? validateLanguage(first.language) : null) || 'Hindi';

  // Validate every buffered item before touching Firestore.
  const validatedBuf = buf.map((item) => ({
    fileId: item.file_id,
    fileUniqueId: item.file_unique_id,
    channelId: validateChannelId(item.channelId),
    messageId: validateMessageId(item.messageId),
    fileSizeBytes: item.fileSizeBytes,
    detected: item.detected || {},
  }));

  // Cross-title duplicate prevention: has this exact Telegram file already been saved?
  const alreadySaved = await findExistingFileUniqueIds(validatedBuf.map((v) => v.fileUniqueId));

  // Best-effort MTProto verification that each source message still exists
  // and actually carries video/document media before we write anything.
  const verification = new Map();
  if (mtproto.isEnabled()) {
    const CONCURRENCY = 5;
    for (let i = 0; i < validatedBuf.length; i += CONCURRENCY) {
      const slice = validatedBuf.slice(i, i + CONCURRENCY);
      const results = await Promise.all(slice.map((item) => mtproto.verifyMessage(item.channelId, item.messageId)));
      slice.forEach((item, idx) => verification.set(item.fileUniqueId, results[idx]));
    }
  }

  const candidates = [];
  let skippedInvalidSource = 0;
  let autoIndex = 0;
  validatedBuf.forEach((item) => {
    if (alreadySaved.has(item.fileUniqueId)) return;
    const v = verification.get(item.fileUniqueId);
    if (v && v.ok === false) {
      skippedInvalidSource++;
      logWarn('Skipped invalid source before save', { fileUniqueId: item.fileUniqueId, channelId: item.channelId, messageId: item.messageId, reason: v.reason });
      return;
    }

    const season = explicitSeason ?? (item.detected.season != null ? validateSeason(item.detected.season) : 1);
    const episode = item.detected.episode != null ? validateEpisode(item.detected.episode) : validateEpisode(startEpisode + autoIndex++);
    const quality = item.detected.quality || null;

    const id = videoDocId(resolvedType, title, season, episode);
    candidates.push({
      id,
      ref: db.collection(VIDEOS_COLLECTION).doc(id),
      data: buildVideoData({
        title, category, season, episode,
        fileId: item.fileId, fileUniqueId: item.fileUniqueId,
        channelId: item.channelId, messageId: item.messageId,
        language, fileSizeBytes: item.fileSizeBytes, quality,
      }),
      season,
    });
  });

  // Prevent duplicate documents: filter out IDs that already exist.
  const existingDocs = await getAllChunked(candidates.map((c) => c.ref));
  const existingIds = new Set(existingDocs.filter((d) => d.exists).map((d) => d.id));
  const toWrite = candidates.filter((c) => !existingIds.has(c.id));

  const skippedDuplicateFile = validatedBuf.length - candidates.length - skippedInvalidSource;
  const skippedExistingDoc = candidates.length - toWrite.length;

  const savedCount = await batchedSet(toWrite);

  invalidateCategoryCaches(category);
  adminBuffer[chatId] = [];
  if (bufferSeenIds[chatId]) bufferSeenIds[chatId].clear();

  const elapsed = Date.now() - start;
  logSuccess(`Saved ${category}`, {
    ...userMeta(msg),
    title,
    saved: savedCount,
    skippedDup: skippedDuplicateFile + skippedExistingDoc,
    skippedInvalidSource,
    firestoreLatency: formatDuration(elapsed),
  });

  const seasonsUsed = [...new Set(toWrite.map((c) => c.season))].sort((a, b) => a - b);
  const lines = [
    `✅ <b>${category === CATEGORIES.WEBSERIES ? 'Web Series' : category.replace(/s$/, '')} Saved</b>`,
    '',
    `Title: ${title}`,
    `Season: ${seasonsUsed.join(', ') || '—'}`,
    `Episodes Saved: ${savedCount}`,
    `Language: ${language}`,
  ];
  if (skippedDuplicateFile + skippedExistingDoc > 0) lines.push(`Skipped Duplicates: ${skippedDuplicateFile + skippedExistingDoc}`);
  if (skippedInvalidSource > 0) lines.push(`Skipped Invalid Source: ${skippedInvalidSource}`);
  lines.push('', `Firestore ID: <code>${toWrite[0]?.id || '—'}</code>`, '', `Time: ${formatDuration(elapsed)}`);

  await safeSendMessage(chatId, lines.join('\n'), {});
}

registerCommand('saveanime', (msg, arg) => executeSave(msg, 'anime', arg));
registerCommand('savemovie', (msg, arg) => executeSave(msg, 'movie', arg));
registerCommand('savewebseries', (msg, arg) => executeSave(msg, 'webseries', arg));
registerCommand('autosave', (msg, arg) => executeSave(msg, null, arg));

// ============================================================================
// SECTION 13 — COMMANDS: buffer utilities
// ============================================================================

registerCommand('maxbuffer', async (msg, arg) => {
  const chatId = msg.chat.id;
  assertValid(arg, `Usage: /maxbuffer <1-${HARD_MAX_BUFFER}> (current: ${getMaxBuffer(chatId)})`);
  const n = parseInt(arg, 10);
  assertValid(Number.isInteger(n) && n >= 1 && n <= HARD_MAX_BUFFER, `Buffer size must be between 1 and ${HARD_MAX_BUFFER}.`);
  maxBufferByChat[chatId] = n;
  await safeSendMessage(chatId, `✅ Max buffer size set to ${n}.`, {});
});

registerCommand('showbuffer', async (msg) => {
  const chatId = msg.chat.id;
  const buf = adminBuffer[chatId] || [];
  if (buf.length === 0) {
    await safeSendMessage(chatId, 'ℹ️ Buffer is empty.', {});
    return;
  }
  const totalBytes = buf.reduce((sum, b) => sum + (b.fileSizeBytes || 0), 0);
  const oldestAgeSec = Math.round((Date.now() - buf[0].addedAt) / 1000);
  const text = [
    `📦 <b>Buffer Status</b>`,
    '',
    `Videos: ${buf.length} / ${getMaxBuffer(chatId)}`,
    `Approx. Size: ${formatBytes(totalBytes)}`,
    `Oldest item: ${oldestAgeSec}s ago`,
  ].join('\n');
  await safeSendMessage(chatId, text, {});
});

registerCommand('clearbuffer', async (msg) => {
  const chatId = msg.chat.id;
  const count = (adminBuffer[chatId] || []).length;
  adminBuffer[chatId] = [];
  if (bufferSeenIds[chatId]) bufferSeenIds[chatId].clear();
  await safeSendMessage(chatId, `🗑 Buffer cleared (${count} item${count === 1 ? '' : 's'} removed).`, {});
});

// ============================================================================
// SECTION 14 — CONFIRMATION FLOW (Requirement 4 & 14)
// ============================================================================

async function requestConfirmation(chatId, userId, promptText, kind, payload) {
  const tok = shortToken();
  const sent = await safeSendMessage(chatId, `⚠️ ${promptText}\n\nAre you sure?`, {
    reply_markup: {
      inline_keyboard: [[
        { text: '✅ Yes', callback_data: `cy:${tok}` },
        { text: '❌ Cancel', callback_data: `cn:${tok}` },
      ]],
    },
  });
  pendingActions.set(tok, { chatId, userId, kind, payload, createdAt: Date.now(), promptMessageId: sent?.message_id });
}

bot.on('callback_query', async (query) => {
  const chatId = query.message?.chat?.id;
  const userId = query.from?.id;
  const data = query.data || '';

  try {
    if (!isAdmin(chatId)) {
      logWarn('Unauthorized callback_query attempt', { userId, chatId, data });
      await safeAnswerCallback(query.id, { text: '⛔ Access denied.', show_alert: true });
      return;
    }

    if (data.startsWith('cy:') || data.startsWith('cn:')) {
      const confirmed = data.startsWith('cy:');
      const tok = data.slice(3);
      const pending = pendingActions.get(tok);

      if (!pending) {
        await safeAnswerCallback(query.id, { text: '⌛ This confirmation expired.', show_alert: true });
        return;
      }
      pendingActions.delete(tok);

      if (!confirmed) {
        await safeAnswerCallback(query.id, { text: 'Cancelled.' });
        await safeEditMessageText('❎ Cancelled — nothing was deleted.', { chat_id: chatId, message_id: query.message.message_id });
        return;
      }

      await safeAnswerCallback(query.id, { text: 'Processing…' });
      await safeEditMessageText('⏳ Deleting…', { chat_id: chatId, message_id: query.message.message_id });

      const start = Date.now();
      const result = await runDeleteAction(pending.kind, pending.payload);
      const elapsed = Date.now() - start;

      logSuccess('Delete executed', { userId, chatId, kind: pending.kind, deleteTime: formatDuration(elapsed), removed: result.count });
      await safeEditMessageText(result.message + `\n\nTime: ${formatDuration(elapsed)}`, {
        chat_id: chatId,
        message_id: query.message.message_id,
      });
      return;
    }

    if (data.startsWith('pg:')) {
      const [, code, pageStr] = data.split(':');
      await renderListPage(chatId, code, parseInt(pageStr, 10), query.message.message_id);
      await safeAnswerCallback(query.id);
      return;
    }

    if (data.startsWith('rf:')) {
      const target = data.slice(3);
      await safeAnswerCallback(query.id, { text: '🔄 Refreshed' });
      if (target === 'stats') await renderStats(chatId, query.message.message_id);
      else if (target === 'storage') await renderStorage(chatId, query.message.message_id);
      else if (target === 'health') await renderHealth(chatId, query.message.message_id);
      return;
    }

    await safeAnswerCallback(query.id);
  } catch (err) {
    logError('callback_query handler failed', err, { chatId, userId, data });
    await safeAnswerCallback(query.id, { text: '❌ Something went wrong.', show_alert: true });
  }
});

// ============================================================================
// SECTION 15 — DELETE LOGIC (Requirement 4)
// ============================================================================

function pluralCategory(category) {
  return category === CATEGORIES.WEBSERIES ? 'Web Series' : category;
}

async function runDeleteAction(kind, payload) {
  switch (kind) {
    case 'deleteEntireTitle': {
      const { category, title } = payload;
      const refsByTitle = await withRetry(() => db.collection(VIDEOS_COLLECTION).where('category', '==', category).where('title', '==', title).get());
      const refsBySeriesTitle = await withRetry(() => db.collection(VIDEOS_COLLECTION).where('category', '==', category).where('seriesTitle', '==', title).get());
      const map = new Map();
      refsByTitle.forEach((d) => map.set(d.id, d.ref));
      refsBySeriesTitle.forEach((d) => map.set(d.id, d.ref));
      const count = await batchedDelete([...map.values()]);
      invalidateCategoryCaches(category);
      return { count, message: `🗑 <b>Deleted</b>\n\nCategory: ${pluralCategory(category)}\nTitle: ${title}\nEpisodes Removed: ${count}` };
    }
    case 'deleteSeason': {
      const { category, title, season } = payload;
      const count = await deleteByQuery(() =>
        db.collection(VIDEOS_COLLECTION).where('category', '==', category).where('title', '==', title).where('season', '==', season)
      );
      invalidateCategoryCaches(category);
      return { count, message: `🗑 <b>Deleted</b>\n\nCategory: ${pluralCategory(category)}\nTitle: ${title}\nSeason: ${season}\nEpisodes Removed: ${count}` };
    }
    case 'deleteEpisode': {
      const { category, title, season, episode } = payload;
      const count = await deleteByQuery(() =>
        db.collection(VIDEOS_COLLECTION)
          .where('category', '==', category).where('title', '==', title)
          .where('season', '==', season).where('episode', '==', episode)
      );
      invalidateCategoryCaches(category);
      return { count, message: `🗑 <b>Deleted</b>\n\nCategory: ${pluralCategory(category)}\nTitle: ${title}\nSeason: ${season}\nEpisode: ${episode}\nRemoved: ${count}` };
    }
    case 'deleteDoc': {
      const { docId } = payload;
      const ref = db.collection(VIDEOS_COLLECTION).doc(docId);
      const snap = await ref.get();
      if (!snap.exists) return { count: 0, message: `⚠️ Document <code>${docId}</code> does not exist (already deleted?).` };
      const data = snap.data();
      await ref.delete();
      invalidateCategoryCaches(data.category);
      return { count: 1, message: `🗑 <b>Deleted</b>\n\nDocument ID: <code>${docId}</code>\nTitle: ${data.title || '—'}\nCategory: ${data.category || '—'}` };
    }
    case 'deleteLanguage': {
      const { category, language } = payload;
      const count = await deleteByQuery(() =>
        db.collection(VIDEOS_COLLECTION).where('category', '==', category).where('language', '==', language)
      );
      invalidateCategoryCaches(category);
      return { count, message: `🗑 <b>Deleted</b>\n\nCategory: ${pluralCategory(category)}\nLanguage: ${language}\nEpisodes Removed: ${count}` };
    }
    case 'deleteEntireCategory': {
      const { category } = payload;
      const count = await deleteByQuery(() => db.collection(VIDEOS_COLLECTION).where('category', '==', category));
      invalidateCategoryCaches(category);
      return { count, message: `🗑 <b>Deleted</b>\n\nCategory: ${pluralCategory(category)} (entire catalog)\nDocuments Removed: ${count}` };
    }
    default:
      return { count: 0, message: '❌ Unknown delete action.' };
  }
}

function registerEntireTitleDelete(command, category) {
  registerCommand(command, async (msg, arg) => {
    assertValid(arg, `Usage: /${command} Title`);
    const title = validateTitle(arg);
    await requestConfirmation(
      msg.chat.id, msg.from.id,
      `Delete <b>ALL</b> seasons/episodes of "${title}" in ${pluralCategory(category)}?`,
      'deleteEntireTitle', { category, title }
    );
  });
}
registerEntireTitleDelete('deleteanime', CATEGORIES.ANIME);
registerEntireTitleDelete('deletemovie', CATEGORIES.MOVIES);
registerEntireTitleDelete('deletewebseries', CATEGORIES.WEBSERIES);

registerCommand('deleteseason', async (msg, arg) => {
  assertValid(arg, 'Usage: /deleteseason category|Title|Season');
  const [catRaw, titleRaw, seasonRaw] = arg.split('|').map((s) => s.trim());
  const category = normalizeCategory(catRaw);
  const title = validateTitle(titleRaw);
  const season = validateSeason(seasonRaw);
  await requestConfirmation(
    msg.chat.id, msg.from.id,
    `Delete Season ${season} of "${title}" (${pluralCategory(category)})?`,
    'deleteSeason', { category, title, season }
  );
});

registerCommand('deleteepisode', async (msg, arg) => {
  assertValid(arg, 'Usage: /deleteepisode category|Title|Season|Episode');
  const [catRaw, titleRaw, seasonRaw, episodeRaw] = arg.split('|').map((s) => s.trim());
  const category = normalizeCategory(catRaw);
  const title = validateTitle(titleRaw);
  const season = validateSeason(seasonRaw);
  const episode = validateEpisode(episodeRaw);
  await requestConfirmation(
    msg.chat.id, msg.from.id,
    `Delete S${season}E${episode} of "${title}" (${pluralCategory(category)})?`,
    'deleteEpisode', { category, title, season, episode }
  );
});

registerCommand('deletetitle', async (msg, arg) => {
  assertValid(arg, 'Usage: /deletetitle docId  OR  /deletetitle category|Title');
  if (!arg.includes('|')) {
    const docId = arg.trim();
    assertValid(/^[A-Za-z0-9_-]{3,200}$/.test(docId), 'That does not look like a valid document ID.');
    await requestConfirmation(msg.chat.id, msg.from.id, `Delete document <code>${docId}</code>?`, 'deleteDoc', { docId });
    return;
  }
  const [catRaw, titleRaw] = arg.split('|').map((s) => s.trim());
  const category = normalizeCategory(catRaw);
  const title = validateTitle(titleRaw);
  await requestConfirmation(
    msg.chat.id, msg.from.id,
    `Delete <b>ALL</b> seasons/episodes of "${title}" in ${pluralCategory(category)}?`,
    'deleteEntireTitle', { category, title }
  );
});

registerCommand('deletelanguage', async (msg, arg) => {
  assertValid(arg, 'Usage: /deletelanguage category|Language');
  const [catRaw, langRaw] = arg.split('|').map((s) => s.trim());
  const category = normalizeCategory(catRaw);
  const language = validateLanguage(langRaw);
  await requestConfirmation(
    msg.chat.id, msg.from.id,
    `Delete every ${pluralCategory(category)} video in "${language}"?`,
    'deleteLanguage', { category, language }
  );
});

function registerDeleteAllCommand(command, category) {
  registerCommand(command, async (msg) => {
    await requestConfirmation(
      msg.chat.id, msg.from.id,
      `Delete the <b>ENTIRE</b> ${pluralCategory(category)} catalog? This cannot be undone.`,
      'deleteEntireCategory', { category }
    );
  });
}
registerDeleteAllCommand('deleteallanime', CATEGORIES.ANIME);
registerDeleteAllCommand('deleteallmovies', CATEGORIES.MOVIES);
registerDeleteAllCommand('deleteallwebseries', CATEGORIES.WEBSERIES);

// ============================================================================
// SECTION 16 — LISTING & PAGINATION (Requirement 14)
// ============================================================================

async function fetchCategoryDocsCached(category) {
  const code = CATEGORY_CODE[category];
  const key = `list:${code}`;
  const cached = listCache.get(key);
  if (cached && Date.now() - cached.fetchedAt < LIST_CACHE_TTL_MS) return cached.docs;

  const snap = await withRetry(
    () => db.collection(VIDEOS_COLLECTION).where('category', '==', category)
      .select('title', 'season', 'episode', 'language', 'published', 'createdAt')
      .limit(LIST_FETCH_LIMIT).get(),
    { label: 'fetchCategoryDocsCached' }
  );
  const docs = snap.docs
    .map((d) => ({ id: d.id, ...d.data() }))
    .sort((a, b) => toMillis(b.createdAt) - toMillis(a.createdAt));

  listCache.set(key, { docs, fetchedAt: Date.now() });
  return docs;
}

function renderListText(category, docs, page) {
  const totalPages = Math.max(1, Math.ceil(docs.length / LIST_PAGE_SIZE));
  const clampedPage = Math.min(Math.max(page, 0), totalPages - 1);
  const slice = docs.slice(clampedPage * LIST_PAGE_SIZE, clampedPage * LIST_PAGE_SIZE + LIST_PAGE_SIZE);

  const lines = [`📋 <b>${pluralCategory(category)}</b> — ${docs.length} video${docs.length === 1 ? '' : 's'}`, ''];
  if (slice.length === 0) {
    lines.push('No videos found.');
  } else {
    for (const d of slice) {
      lines.push(`• <b>${d.title}</b> S${d.season}E${d.episode} · ${d.language || '—'} ${d.published ? '' : '(unpublished)'}`);
      lines.push(`  <code>${d.id}</code>`);
    }
  }
  lines.push('', `Page ${clampedPage + 1}/${totalPages}`);
  return { text: lines.join('\n'), clampedPage, totalPages };
}

async function renderListPage(chatId, code, page, messageId) {
  const category = CODE_CATEGORY[code];
  if (!category) return;
  const docs = await fetchCategoryDocsCached(category);
  const { text, clampedPage, totalPages } = renderListText(category, docs, page || 0);

  const buttons = [];
  if (clampedPage > 0) buttons.push({ text: '⬅ Prev', callback_data: `pg:${code}:${clampedPage - 1}` });
  buttons.push({ text: `${clampedPage + 1}/${totalPages}`, callback_data: `pg:${code}:${clampedPage}` });
  if (clampedPage < totalPages - 1) buttons.push({ text: 'Next ➡', callback_data: `pg:${code}:${clampedPage + 1}` });

  const options = { chat_id: chatId, message_id: messageId, reply_markup: { inline_keyboard: [buttons] } };
  if (messageId) {
    await safeEditMessageText(text, options);
  } else {
    await safeSendMessage(chatId, text, { reply_markup: options.reply_markup });
  }
}

function registerListCommand(command, category) {
  registerCommand(command, async (msg) => {
    await renderListPage(msg.chat.id, CATEGORY_CODE[category], 0, null);
  });
}
registerListCommand('listanime', CATEGORIES.ANIME);
registerListCommand('listmovies', CATEGORIES.MOVIES);
registerListCommand('listwebseries', CATEGORIES.WEBSERIES);

// ============================================================================
// SECTION 17 — STATS / STORAGE / HEALTH / LOGS / PING (Requirement 8 & 10)
// ============================================================================

async function computeStats() {
  const cached = getCache('stats');
  if (cached) return cached;

  const col = db.collection(VIDEOS_COLLECTION);

  const [animeCount, movieCount, seriesCount] = await Promise.all([
    withRetry(() => col.where('category', '==', CATEGORIES.ANIME).count().get()),
    withRetry(() => col.where('category', '==', CATEGORIES.MOVIES).count().get()),
    withRetry(() => col.where('category', '==', CATEGORIES.WEBSERIES).count().get()),
  ]);

  const startOfDay = new Date();
  startOfDay.setHours(0, 0, 0, 0);

  const [uploadsTodaySnap, lastUploadSnap, largestSnap] = await Promise.all([
    withRetry(() => col.where('createdAt', '>=', startOfDay).count().get()),
    withRetry(() => col.orderBy('createdAt', 'desc').limit(1).get()),
    withRetry(() => col.orderBy('fileSizeBytes', 'desc').limit(1).get()),
  ]);

  const stats = {
    animeTotal: animeCount.data().count,
    movieTotal: movieCount.data().count,
    seriesTotal: seriesCount.data().count,
    total: animeCount.data().count + movieCount.data().count + seriesCount.data().count,
    uploadsToday: uploadsTodaySnap.data().count,
    lastUpload: lastUploadSnap.empty ? null : lastUploadSnap.docs[0].data(),
    largest: largestSnap.empty ? null : largestSnap.docs[0].data(),
  };
  setCache('stats', stats);
  return stats;
}

async function renderStats(chatId, editMessageId) {
  const start = Date.now();
  const s = await computeStats();
  const elapsed = Date.now() - start;

  const lines = [
    '📊 <b>MYFLIX Stats</b>',
    '',
    `Total Videos: ${s.total}`,
    `Anime: ${s.animeTotal}`,
    `Movies: ${s.movieTotal}`,
    `Web Series: ${s.seriesTotal}`,
    '',
    `Uploads Today: ${s.uploadsToday}`,
    `Last Upload: ${s.lastUpload ? s.lastUpload.title + ` (S${s.lastUpload.season}E${s.lastUpload.episode})` : '—'}`,
    `Largest Video: ${s.largest ? `${s.largest.title} (${formatBytes(s.largest.fileSizeBytes)})` : '—'}`,
    '',
    `Firestore Latency: ${formatDuration(elapsed)}`,
  ];
  const options = { reply_markup: { inline_keyboard: [[{ text: '🔄 Refresh', callback_data: 'rf:stats' }]] } };
  if (editMessageId) await safeEditMessageText(lines.join('\n'), { chat_id: chatId, message_id: editMessageId, ...options });
  else await safeSendMessage(chatId, lines.join('\n'), options);
}
registerCommand('stats', (msg) => renderStats(msg.chat.id, null));

async function computeStorage() {
  const cached = getCache('storage');
  if (cached) return cached;

  const col = db.collection(VIDEOS_COLLECTION);
  const perCategory = {};
  let totalBytes = 0;

  for (const category of Object.values(CATEGORIES)) {
    const snap = await withRetry(() => col.where('category', '==', category).select('fileSizeBytes').get(), { label: 'computeStorage' });
    let bytes = 0;
    snap.forEach((d) => { bytes += d.get('fileSizeBytes') || 0; });
    perCategory[category] = bytes;
    totalBytes += bytes;
  }

  const result = { totalBytes, perCategory };
  setCache('storage', result);
  return result;
}

async function renderStorage(chatId, editMessageId) {
  const start = Date.now();
  const s = await computeStorage();
  const elapsed = Date.now() - start;

  const lines = [
    '💾 <b>Storage Breakdown</b>',
    '',
    `Total: ${formatBytes(s.totalBytes)}`,
    `Anime: ${formatBytes(s.perCategory[CATEGORIES.ANIME])}`,
    `Movies: ${formatBytes(s.perCategory[CATEGORIES.MOVIES])}`,
    `Web Series: ${formatBytes(s.perCategory[CATEGORIES.WEBSERIES])}`,
    '',
    `Computed in: ${formatDuration(elapsed)}`,
    '<i>Note: figures rely on Telegram-reported file sizes captured at upload time.</i>',
  ];
  const options = { reply_markup: { inline_keyboard: [[{ text: '🔄 Refresh', callback_data: 'rf:storage' }]] } };
  if (editMessageId) await safeEditMessageText(lines.join('\n'), { chat_id: chatId, message_id: editMessageId, ...options });
  else await safeSendMessage(chatId, lines.join('\n'), options);
}
registerCommand('storage', (msg) => renderStorage(msg.chat.id, null));

async function renderHealth(chatId, editMessageId) {
  const dbStart = Date.now();
  let firestoreLatency = -1;
  try {
    await withTimeout(db.collection(VIDEOS_COLLECTION).limit(1).get(), 5000, 'firestore health check');
    firestoreLatency = Date.now() - dbStart;
  } catch (err) {
    logError('Firestore health check failed', err);
  }

  const mem = process.memoryUsage();
  const totalBufferCount = Object.values(adminBuffer).reduce((sum, arr) => sum + arr.length, 0);

  const lines = [
    '🩺 <b>Bot Health</b>',
    '',
    `Status: ${firestoreLatency >= 0 ? '✅ Healthy' : '❌ Firestore unreachable'}`,
    `Firestore Latency: ${firestoreLatency >= 0 ? formatDuration(firestoreLatency) : 'timeout'}`,
    `Uptime: ${formatUptime(process.uptime())}`,
    `Memory (RSS): ${formatBytes(mem.rss)}`,
    `Memory (Heap): ${formatBytes(mem.heapUsed)} / ${formatBytes(mem.heapTotal)}`,
    `Buffered Videos (all chats): ${totalBufferCount}`,
    `Pending Confirmations: ${pendingActions.size}`,
    `Node: ${process.version}`,
  ];
  const options = { reply_markup: { inline_keyboard: [[{ text: '🔄 Refresh', callback_data: 'rf:health' }]] } };
  if (editMessageId) await safeEditMessageText(lines.join('\n'), { chat_id: chatId, message_id: editMessageId, ...options });
  else await safeSendMessage(chatId, lines.join('\n'), options);
}
registerCommand('health', (msg) => renderHealth(msg.chat.id, null));

registerCommand('logs', async (msg) => {
  if (LOG_RING.length === 0) {
    await safeSendMessage(msg.chat.id, 'ℹ️ No logs recorded yet.', {});
    return;
  }
  const recent = LOG_RING.slice(-20);
  const body = recent.map((l) => `<code>${new Date(l.t).toISOString().slice(11, 19)}</code> ${escapeHtml(l.message.split('\n')[0]).slice(0, 200)}`).join('\n');
  const text = `🧾 <b>Recent Logs</b> (last ${recent.length})\n\n${body}`.slice(0, 4000);
  await safeSendMessage(msg.chat.id, text, {});
});

registerCommand('ping', async (msg) => {
  const start = Date.now();
  const sent = await safeSendMessage(msg.chat.id, '🏓 Pinging…', {});
  const elapsed = Date.now() - start;
  if (sent) {
    await safeEditMessageText(`🏓 Pong! ${elapsed}ms`, { chat_id: msg.chat.id, message_id: sent.message_id });
  }
});

function escapeHtml(str) {
  return String(str).replace(/[&<>]/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;' }[c]));
}

// ============================================================================
// SECTION 18 — SEARCH (Requirement 7)
// ============================================================================

registerCommand('find', async (msg, arg) => {
  assertValid(arg, 'Usage: /find title|episode|season|language|category <value>');
  const spaceIdx = arg.indexOf(' ');
  assertValid(spaceIdx > 0, 'Usage: /find title|episode|season|language|category <value>');

  const field = arg.slice(0, spaceIdx).trim().toLowerCase();
  const value = arg.slice(spaceIdx + 1).trim();
  assertValid(value, 'Please provide a value to search for.');

  const col = db.collection(VIDEOS_COLLECTION);
  let snaps = [];

  switch (field) {
    case 'title': {
      const t = validateTitle(value);
      const [byTitle, bySeries] = await Promise.all([
        withRetry(() => col.where('title', '>=', t).where('title', '<=', t + '\uf8ff').limit(FIND_RESULT_LIMIT).get()),
        withRetry(() => col.where('seriesTitle', '>=', t).where('seriesTitle', '<=', t + '\uf8ff').limit(FIND_RESULT_LIMIT).get()),
      ]);
      snaps = [...byTitle.docs, ...bySeries.docs];
      break;
    }
    case 'episode': {
      const e = validateEpisode(value);
      const r = await withRetry(() => col.where('episode', '==', e).limit(FIND_RESULT_LIMIT).get());
      snaps = r.docs;
      break;
    }
    case 'season': {
      const s = validateSeason(value);
      const r = await withRetry(() => col.where('season', '==', s).limit(FIND_RESULT_LIMIT).get());
      snaps = r.docs;
      break;
    }
    case 'language': {
      const l = validateLanguage(value);
      const r = await withRetry(() => col.where('language', '==', l).limit(FIND_RESULT_LIMIT).get());
      snaps = r.docs;
      break;
    }
    case 'category': {
      const c = normalizeCategory(value);
      const r = await withRetry(() => col.where('category', '==', c).limit(FIND_RESULT_LIMIT).get());
      snaps = r.docs;
      break;
    }
    default:
      throw new ValidationError('First word must be one of: title, episode, season, language, category.');
  }

  const seen = new Map();
  for (const d of snaps) seen.set(d.id, d.data());

  if (seen.size === 0) {
    await safeSendMessage(msg.chat.id, `🔍 No results for ${field} "${value}".`, {});
    return;
  }

  const lines = [`🔍 <b>Search results</b> — ${field}: "${value}" (${seen.size})`, ''];
  let i = 0;
  for (const [id, d] of seen.entries()) {
    if (i >= FIND_RESULT_LIMIT) break;
    lines.push(`• <b>${d.title}</b> [${d.category}] S${d.season}E${d.episode} · ${d.language}`);
    lines.push(`  <code>${id}</code>`);
    i++;
  }
  await safeSendMessage(msg.chat.id, lines.join('\n'), {});
});

// ============================================================================
// SECTION 19 — UNKNOWN COMMAND FALLBACK
// ============================================================================

bot.onText(/^\/(\w+)/, async (msg, match) => {
  const known = new Set([
    'start', 'help', 'saveanime', 'savemovie', 'savewebseries', 'autosave',
    'deleteanime', 'deletemovie', 'deletewebseries', 'deleteseason', 'deleteepisode',
    'deletetitle', 'deletelanguage', 'deleteallanime', 'deleteallmovies', 'deleteallwebseries',
    'listanime', 'listmovies', 'listwebseries', 'stats', 'storage', 'health', 'logs', 'ping',
    'maxbuffer', 'showbuffer', 'clearbuffer', 'find',
  ]);
  const cmd = match[1].toLowerCase();
  if (known.has(cmd)) return; // already handled by a dedicated listener
  if (!isAdmin(msg.chat.id)) return;
  await safeSendMessage(msg.chat.id, `❓ Unknown command /${cmd}. Send /help to see everything I support.`, {});
});

// ============================================================================
// SECTION 20 — EXPORTS
// ============================================================================

module.exports = {
  initBot,
  isAdmin,
  processUpdate: (update) => {
    try {
      bot.processUpdate(update);
    } catch (err) {
      logError('processUpdate failed', err);
    }
  },
};
