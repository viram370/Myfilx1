/**
 * queue/pipeline.js
 * ----------------------------------------------------------------------
 * Batch upload pipeline for handlers/adminUpload.js
 * (/add anime | webseries | anime-movie | movie).
 *
 * BATCH-COMMIT WORKFLOW (this is the core of this file's design):
 *   - Every video the admin sends during upload mode is only BUFFERED —
 *     addItem() does no download, no compression, no upload, and assigns
 *     no worker. It just records the item and logs "Buffered".
 *   - Nothing runs until the admin sends "Done" / "/done". At that point
 *     startBatch() LOCKS the batch (no more videos accepted) and, in
 *     order:
 *       1. Validates every buffered item (missing metadata, direct
 *          uploads over the Bot API's 20MB download cap).
 *       2. Detects duplicates — both within the batch itself and against
 *          Firestore — and marks them 'skipped' rather than processing
 *          them.
 *       3. Assigns episode numbers to whatever's left (anime/webseries
 *          only), sequentially, skipping over failed/skipped items so
 *          numbering has no gaps.
 *       4. Starts the download/compress/upload queues for every item
 *          that survived validation.
 *   - A failure at any stage only fails THAT episode — every other
 *     episode in the batch keeps processing (tickSessionUpload's "find
 *     the next unsettled item" already skips over 'failed'/'skipped'
 *     items, so this falls out of the existing design for free).
 *   - The final summary always reports "✅ Uploaded: X" / "❌ Failed: X"
 *     (plus a Skipped line when relevant).
 *
 * MTPROTO IS CHANNEL-ONLY (fixes "Source message not found
 * (channelId=<userId>, messageId=<n>)"):
 *   - A video uploaded DIRECTLY to the bot is identified by its Telegram
 *     `file_id` and downloaded ONLY via the plain Bot API
 *     (services/telegramUpload.js#downloadViaBotApi). MTProto is NEVER
 *     invoked for it, under any circumstances — a private bot chat is a
 *     user peer, not a channel, and MTProto's channel-message lookup does
 *     not apply to it. A direct upload over the Bot API's 20MB download
 *     cap fails validation up front with a clear message; there is no
 *     fallback for that case.
 *   - A video FORWARDED from a channel (or posted straight into a private
 *     storage channel the bot administers) carries a REAL channel id +
 *     message id from Telegram itself (`forward_origin`/`forward_from_chat`,
 *     or the channel_post's own chat/message id). These always go through
 *     MTProto — first a fast server-side `bot.copyMessage()` straight into
 *     the storage channel (no download at all), falling back to
 *     `downloadFromChannel()` (also MTProto, same real channel id) if the
 *     copy isn't possible.
 *   - A "Copy"'d message (Telegram's own Copy feature, as opposed to
 *     Forward) carries no forward metadata at all — Telegram makes it
 *     indistinguishable from a direct upload — so it naturally goes
 *     through the direct-upload (Bot-API-only) path, which is correct.
 *
 * One session = one admin's currently-open /add wizard + its batch of
 * episodes. Sessions live only in memory (single-process, matches the
 * rest of this bot's existing buffer/cache design) — a process restart
 * mid-batch loses in-flight items, same trade-off the existing
 * adminBuffer already makes.
 * ----------------------------------------------------------------------
 */
'use strict';

const fs = require('fs');
const os = require('os');
const path = require('path');
const { makeLogger } = require('../utils/logger');
const log = makeLogger('queue/pipeline.js');
const compress = require('../services/compress');
const transfer = require('../services/telegramUpload');
const { getDB, getAdmin } = require('../services/firebase');

// Uses the OS temp dir rather than a path inside the repo — safer across
// hosts (Render, containers, etc.) where the app directory's writability
// or persistence guarantees can vary, but /tmp is always writable.
const TEMP_DIR = path.join(os.tmpdir(), 'myflix-add-pipeline');
const UPLOADS_DIR = path.join(TEMP_DIR, 'uploads');
const CONVERTED_DIR = path.join(TEMP_DIR, 'converted');
for (const dir of [UPLOADS_DIR, CONVERTED_DIR]) {
  fs.mkdirSync(dir, { recursive: true });
}

const VIDEOS_COLLECTION = 'videos';
const COMPRESS_CONCURRENCY = Math.max(1, parseInt(process.env.COMPRESS_CONCURRENCY || '1', 10));
const MAX_ATTEMPTS = 3;
const RETRY_BASE_DELAY_MS = 1500;
const SAFETY_TICK_MS = 2000;
const TERMINAL_STATUSES = ['done', 'failed', 'skipped'];

function sleep(ms) { return new Promise((r) => setTimeout(r, ms)); }
function isTerminal(status) { return TERMINAL_STATUSES.includes(status); }

function slugify(str) {
  return String(str).toLowerCase().trim().replace(/[^a-z0-9]+/g, '-').replace(/(^-+|-+$)/g, '').slice(0, 80) || 'untitled';
}

// Set once via setBotInstance() at startup (handlers/adminUpload.js) —
// needed here for Bot-API downloads and copyMessage.
let botInstance = null;
function setBotInstance(bot) { botInstance = bot; }

// ============================================================================
// SESSIONS
// ============================================================================

/** @type {Map<number, object>} keyed by admin chatId */
const sessions = new Map();

/**
 * @param {number} chatId
 * @param {object} meta { kind, category, hasSeason, title, season, language, quality, year, storageChannelId }
 * @param {(session:object)=>Promise<void>} callbacks.onProgress called whenever item states change
 * @param {(session:object)=>Promise<void>} callbacks.onFinished called once every item reaches a terminal state after the batch is locked
 */
function createSession(chatId, meta, { onProgress, onFinished }) {
  const session = {
    chatId,
    ...meta,
    items: [],
    locked: false, // true once "Done"/"/done" locks the batch and processing starts
    episodeBase: null, // resolved lazily, once, during startBatch()
    finalizing: false,
    closed: false,
    onProgress,
    onFinished,
  };
  sessions.set(chatId, session);
  log.info('createSession', 'New /add session created', { chatId, kind: meta.kind, title: meta.title, season: meta.season });
  return session;
}

function getSession(chatId) {
  return sessions.get(chatId);
}

function endSession(chatId) {
  sessions.delete(chatId);
}

function isSessionActive(chatId) {
  return sessions.has(chatId);
}

async function maybeCompleteSession(session) {
  if (!session.locked || session.closed) return;
  const allSettled = session.items.every((it) => isTerminal(it.status));
  if (!allSettled) return;
  await closeSession(session);
}

async function closeSession(session) {
  if (session.closed) return;
  session.closed = true;
  const done = session.items.filter((it) => it.status === 'done').length;
  const failed = session.items.filter((it) => it.status === 'failed').length;
  const skipped = session.items.filter((it) => it.status === 'skipped').length;
  log.info('closeSession', 'Batch complete', { chatId: session.chatId, done, failed, skipped, total: session.items.length });
  try {
    await session.onFinished(session);
  } catch (err) {
    log.error('closeSession', 'onFinished callback threw', err, { chatId: session.chatId, stack: err.stack });
  }
  endSession(session.chatId);
}

// ============================================================================
// EPISODE NUMBERING (anime / webseries only)
// ============================================================================

async function resolveEpisodeBase(session) {
  if (session.episodeBase !== null) return session.episodeBase;
  if (!session.hasSeason) {
    session.episodeBase = 1;
    return session.episodeBase;
  }
  const db = getDB();
  const snap = await db.collection(VIDEOS_COLLECTION)
    .where('category', '==', session.category)
    .where('title', '==', session.title)
    .where('season', '==', session.season)
    .select('episode')
    .get();

  let max = 0;
  snap.forEach((d) => { const e = d.get('episode'); if (typeof e === 'number' && e > max) max = e; });
  session.episodeBase = max + 1;
  log.info('resolveEpisodeBase', 'Auto-numbering resolved', {
    title: session.title, season: session.season, startingEpisode: session.episodeBase,
  });
  return session.episodeBase;
}

// ============================================================================
// SOURCE DETECTION
// ============================================================================

/**
 * Detects the REAL Telegram channel a video's bytes live in, if any —
 * never a private chat. Covers:
 *   - Forwarded-to-the-bot: carries `forward_origin` (Bot API 7.0+) or
 *     the legacy `forward_from_chat`/`forward_from_message_id` pair,
 *     pointing at the original channel + message.
 *   - Channel-native (a private storage channel the bot administers): the
 *     message itself IS a `channel_post`/`edited_channel_post` — there's
 *     nothing to forward from, the post itself is the source — so the
 *     caller tells us via `opts.channelNative` that `msg` already lives
 *     in the channel we should treat as the origin.
 * Returns null for a direct upload OR a Telegram "Copy"'d message —
 * Telegram attaches no forward metadata to copied messages, so those are
 * indistinguishable from direct uploads and correctly fall through to the
 * Bot-API-only direct-upload path.
 */
function detectChannelOrigin(msg, opts = {}) {
  if (opts.channelNative && msg.chat && msg.chat.id && msg.message_id) {
    return { chatId: msg.chat.id, messageId: msg.message_id };
  }
  const origin = msg.forward_origin;
  if (origin && origin.type === 'channel' && origin.chat && origin.message_id) {
    return { chatId: origin.chat.id, messageId: origin.message_id };
  }
  if (msg.forward_from_chat && msg.forward_from_chat.type === 'channel' && msg.forward_from_message_id) {
    return { chatId: msg.forward_from_chat.id, messageId: msg.forward_from_message_id };
  }
  return null;
}

// ============================================================================
// BUFFERING (called from the Telegram video/document/channel_post handlers)
// — this is step 1 of the batch-commit workflow: ONLY buffers. No
// download, no compression, no upload, no worker assignment.
// ============================================================================

/**
 * @param {object} session
 * @param {{file_id:string, file_unique_id:string, file_size?:number, duration?:number, width?:number, height?:number}} media
 * @param {object} msg the raw Telegram message carrying the video (a private-chat
 *   message for direct/forwarded uploads, or the channel_post message itself
 *   for videos posted straight into a private storage channel)
 * @param {{channelNative?:boolean}} opts pass channelNative:true when `msg` IS a
 *   channel_post/edited_channel_post — see detectChannelOrigin() above
 * @returns {object} the buffered item
 */
function addItem(session, media, msg, opts = {}) {
  if (session.locked) {
    log.warn('addItem', 'Ignored — batch is already locked', { chatId: session.chatId, fileUniqueId: media.file_unique_id });
    return null;
  }

  const seq = session.items.length;
  const channelOrigin = detectChannelOrigin(msg, opts);

  const item = {
    seq,
    fileId: media.file_id,
    fileUniqueId: media.file_unique_id,
    fileSizeBytes: media.file_size || null,
    // The message the direct upload itself lives in (the admin's private
    // chat). Kept for logging/debugging only — NEVER used to drive an
    // MTProto call (see file header).
    chatMessageId: msg.message_id || null,
    downloadMethod: null, // 'bot-api' | 'mtproto', set once a download starts
    downloadProgress: null, // 0-100, only meaningful while status === 'downloading'
    durationHint: media.duration || 0,
    widthHint: media.width || 0,
    heightHint: media.height || 0,
    sourceType: channelOrigin ? 'forwarded' : 'direct',
    forwardChatId: channelOrigin ? channelOrigin.chatId : null,
    forwardMessageId: channelOrigin ? channelOrigin.messageId : null,
    needsCopyAttempt: !!channelOrigin,
    status: 'buffered', // buffered -> [validation: failed|skipped|waiting] -> [copying ->] downloading -> compressing -> ready -> uploading -> done | failed
    attempts: { copy: 0, compress: 0, upload: 0 },
    episode: null,
    tempIn: null,
    tempOut: null,
    probe: null,
    error: null,
    uploading: false,
  };
  session.items.push(item);

  log.info('addItem', 'Buffered', {
    chatId: session.chatId, seq, sourceType: item.sourceType,
    channelId: item.forwardChatId, messageId: item.forwardMessageId,
    fileUniqueId: item.fileUniqueId, fileSizeBytes: item.fileSizeBytes,
  });

  // Deliberately does nothing else — no compressQueue push, no
  // pumpUploadQueue call. Processing only begins in startBatch().
  return item;
}

// ============================================================================
// BATCH LOCK — step 2 of the batch-commit workflow. Validates, dedupes,
// numbers, and only THEN starts the download/compress/upload queues.
// ============================================================================

/**
 * @param {number} chatId
 * @returns {{started:boolean, reason?:string}} started:false with a reason if there was nothing to process
 */
async function startBatch(chatId) {
  const session = sessions.get(chatId);
  if (!session) return { started: false, reason: 'no-session' };
  if (session.locked) return { started: false, reason: 'already-locked' };
  if (session.items.length === 0) return { started: false, reason: 'empty' };

  session.locked = true;
  log.info('startBatch', 'Batch started', { chatId, itemCount: session.items.length, title: session.title });

  await validateItems(session);
  await detectDuplicates(session);
  await assignEpisodeNumbers(session);

  await session.onProgress(session, { force: true });

  for (const item of session.items) {
    if (item.status !== 'validated') continue;
    item.status = 'waiting';
    if (item.sourceType === 'forwarded') {
      item.status = 'ready'; // copy is attempted in the strictly-ordered publish step
    } else {
      compressQueue.push({ chatId: session.chatId, seq: item.seq });
    }
  }
  pumpCompressQueue();
  pumpUploadQueue();
  await maybeCompleteSession(session); // covers the edge case where every item failed/was skipped in validation

  return { started: true };
}

async function validateItems(session) {
  for (const item of session.items) {
    if (!item.fileId || !item.fileUniqueId) {
      item.status = 'failed';
      item.error = 'Missing file metadata from Telegram — cannot process.';
      log.error('validateItems', 'Failed — missing file metadata', new Error(item.error), { chatId: session.chatId, seq: item.seq });
      continue;
    }
    if (item.sourceType === 'direct' && item.fileSizeBytes && item.fileSizeBytes > transfer.BOT_API_DOWNLOAD_LIMIT_BYTES) {
      const sizeMb = (item.fileSizeBytes / 1024 / 1024).toFixed(1);
      item.status = 'failed';
      item.error = `File is ${sizeMb}MB — direct uploads over Telegram's Bot API 20MB download limit cannot be ` +
        `processed (MTProto cannot be used for a private chat). Forward this video from a channel instead.`;
      log.error('validateItems', 'Failed — direct upload exceeds Bot API 20MB limit', new Error(item.error), {
        chatId: session.chatId, seq: item.seq, fileSizeBytes: item.fileSizeBytes,
      });
      continue;
    }
    if (item.sourceType === 'forwarded' && (!item.forwardChatId || !item.forwardMessageId)) {
      item.status = 'failed';
      item.error = 'Forwarded video is missing its source channel/message id — cannot process.';
      log.error('validateItems', 'Failed — incomplete forward metadata', new Error(item.error), { chatId: session.chatId, seq: item.seq });
      continue;
    }
    item.status = 'validated';
  }
}

async function detectDuplicates(session) {
  const seenInBatch = new Set();
  for (const item of session.items) {
    if (item.status !== 'validated') continue;

    if (seenInBatch.has(item.fileUniqueId)) {
      item.status = 'skipped';
      item.error = 'Duplicate of another episode already in this batch.';
      log.warn('detectDuplicates', 'Skipped — duplicate within batch', { chatId: session.chatId, seq: item.seq, fileUniqueId: item.fileUniqueId });
      continue;
    }
    seenInBatch.add(item.fileUniqueId);

    const existingDocId = await findExistingDocId(item.fileUniqueId);
    if (existingDocId) {
      item.status = 'skipped';
      item.error = 'Duplicate — this exact file was already saved previously.';
      log.warn('detectDuplicates', 'Skipped — duplicate already in Firestore', {
        chatId: session.chatId, seq: item.seq, fileUniqueId: item.fileUniqueId, existingDocId,
      });
    }
  }
}

async function findExistingDocId(fileUniqueId) {
  const db = getDB();
  const snap = await db.collection(VIDEOS_COLLECTION).where('file_unique_id', '==', fileUniqueId).limit(1).get();
  return snap.empty ? null : snap.docs[0].id;
}

/** Numbers only the items that survived validation/dedup, sequentially, so there are no gaps. */
async function assignEpisodeNumbers(session) {
  if (!session.hasSeason) return;
  const base = await resolveEpisodeBase(session);
  let offset = 0;
  for (const item of session.items) {
    if (item.status !== 'validated') continue;
    item.episode = base + offset;
    offset += 1;
  }
  log.info('assignEpisodeNumbers', 'Episode numbers assigned', {
    chatId: session.chatId, title: session.title, season: session.season, startingEpisode: base, count: offset,
  });
}

// ============================================================================
// COMPRESSION QUEUE (bounded concurrency, order-independent)
// Only ever fed 'direct' items — 'forwarded' items go through the
// copy-first path in the publish queue below.
// ============================================================================

const compressQueue = [];
let compressActive = 0;

function pumpCompressQueue() {
  while (compressActive < COMPRESS_CONCURRENCY && compressQueue.length > 0) {
    const job = compressQueue.shift();
    compressActive++;
    runCompressJob(job)
      .catch((err) => log.error('pumpCompressQueue', 'Unhandled compress job error', err, { ...job, stack: err.stack }))
      .finally(() => {
        compressActive--;
        pumpCompressQueue();
      });
  }
}

function cleanupFile(p) {
  if (!p) return;
  fs.unlink(p, (err) => {
    if (err && err.code !== 'ENOENT') log.warn('cleanupFile', 'Failed to remove temp file', { path: p, reason: err.message });
  });
}

async function runCompressJob({ chatId, seq }) {
  const session = sessions.get(chatId);
  if (!session || session.closed) return;
  const item = session.items[seq];
  if (!item) return;

  const stamp = `${chatId}_${seq}_${Date.now()}`;
  const inPath = path.join(UPLOADS_DIR, `${stamp}.src`);
  const outPath = path.join(CONVERTED_DIR, `${stamp}.mp4`);

  for (let attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
    item.attempts.compress = attempt;
    try {
      item.status = 'downloading';
      item.downloadProgress = 0;
      item.downloadMethod = 'bot-api'; // 'direct' items ALWAYS use the Bot API — never MTProto
      log.info('runCompressJob', `Download started (attempt ${attempt}/${MAX_ATTEMPTS})`, {
        chatId, seq, sourceType: item.sourceType, pipeline: 'bot-api', fileId: item.fileId, fileSizeBytes: item.fileSizeBytes,
      });
      await session.onProgress(session);

      await transfer.downloadViaBotApi(botInstance, item.fileId, inPath);
      item.tempIn = inPath;
      log.success('runCompressJob', 'Download completed', { chatId, seq, path: inPath, method: 'bot-api' });

      item.status = 'compressing';
      item.downloadProgress = null;
      log.info('runCompressJob', 'Compression started', { chatId, seq });
      await session.onProgress(session);

      const info = await compress.processFile(inPath, outPath);
      item.tempOut = outPath;
      item.probe = info;
      log.success('runCompressJob', 'Compression completed', { chatId, seq, mode: info.mode, duration: info.duration });

      item.status = 'ready';
      await session.onProgress(session);
      pumpUploadQueue();
      return;
    } catch (err) {
      const tooBig = /too big/i.test(err.message || '');
      log.error('runCompressJob', `Attempt ${attempt}/${MAX_ATTEMPTS} failed`, err, { chatId, seq, stack: err.stack });
      cleanupFile(inPath);
      cleanupFile(outPath);
      item.tempIn = null;
      item.tempOut = null;
      item.downloadProgress = null;

      // A "too big" rejection from the Bot API can never succeed on retry
      // (there is no MTProto fallback for a direct upload) — fail fast.
      if (attempt >= MAX_ATTEMPTS || tooBig) {
        // Only THIS episode fails — the batch and every other episode
        // keep going (tickSessionUpload skips terminal-status items).
        item.status = 'failed';
        item.error = tooBig ? err.message : `Download/compression failed after ${MAX_ATTEMPTS} attempts: ${err.message}`;
        log.error('runCompressJob', 'Failed', new Error(item.error), { chatId, seq });
        await session.onProgress(session);
        await maybeCompleteSession(session);
        pumpUploadQueue();
        return;
      }
      await sleep(RETRY_BASE_DELAY_MS * attempt);
    }
  }
}

// ============================================================================
// PUBLISH QUEUE (strict FIFO across the whole session — never skips ahead)
// ============================================================================

let uploadTickScheduled = false;

function pumpUploadQueue() {
  if (uploadTickScheduled) return;
  uploadTickScheduled = true;
  setImmediate(async () => {
    uploadTickScheduled = false;
    try {
      for (const session of sessions.values()) {
        if (!session.locked || session.closed) continue;
        await tickSessionUpload(session);
      }
    } catch (err) {
      log.error('pumpUploadQueue', 'Tick failed', err, { stack: err.stack });
    }
  });
}

// Safety-net tick in case an edge case (e.g. a progress-callback throw)
// ever leaves the queue stalled without a pump call being triggered.
setInterval(pumpUploadQueue, SAFETY_TICK_MS).unref?.();

async function tickSessionUpload(session) {
  const next = session.items.find((it) => !isTerminal(it.status));
  if (!next) {
    await maybeCompleteSession(session);
    return;
  }
  if (next.status !== 'ready' || next.uploading) return; // strict order: wait, never skip ahead

  next.uploading = true;
  try {
    if (next.needsCopyAttempt) {
      const copied = await tryCopyPath(session, next);
      if (!copied) {
        // Fell back to download+compress — pumpUploadQueue() will revisit
        // this same item once compression marks it 'ready' again.
        next.uploading = false;
        return;
      }
    } else {
      await uploadPreparedFile(session, next);
    }
  } finally {
    next.uploading = false;
  }
  pumpUploadQueue(); // immediately check whether the next item is also ready
}

/** @returns {boolean} true if the copy succeeded and the item is fully done */
async function tryCopyPath(session, item) {
  item.attempts.copy += 1;
  item.status = 'copying';
  log.info('tryCopyPath', 'Upload started (server-side copy)', {
    chatId: session.chatId, seq: item.seq, sourceType: item.sourceType, pipeline: 'copyMessage',
    channelId: item.forwardChatId, messageId: item.forwardMessageId, attempt: item.attempts.copy,
  });
  await session.onProgress(session);

  try {
    const { messageId } = await transfer.copyMessageToStorage(
      botInstance, item.forwardChatId, item.forwardMessageId, session.storageChannelId
    );

    await writeFirestoreDoc(session, item, item.episode, {
      channelId: session.storageChannelId,
      messageId,
      documentId: item.fileId,
      size: item.fileSizeBytes || 0,
    });
    log.success('tryCopyPath', 'Firestore updated', { chatId: session.chatId, seq: item.seq, episode: item.episode });

    item.status = 'done';
    log.success('tryCopyPath', 'Completed', { chatId: session.chatId, seq: item.seq });
    log.success('tryCopyPath', 'Cleanup completed (nothing to clean up — no local files were used)', { chatId: session.chatId, seq: item.seq });
    await session.onProgress(session);
    return true;
  } catch (err) {
    log.error('tryCopyPath', 'Copy not possible — falling back to download+compress+upload via MTProto', err, {
      chatId: session.chatId, seq: item.seq, sourceType: item.sourceType,
      channelId: item.forwardChatId, messageId: item.forwardMessageId, stack: err.stack,
    });
    item.needsCopyAttempt = false;
    item.status = 'waiting';
    await session.onProgress(session);
    forwardedFallbackQueue.push({ chatId: session.chatId, seq: item.seq });
    pumpForwardedFallbackQueue();
    return false;
  }
}

// ---- fallback download for 'forwarded' items whose copy attempt failed ----
// Always uses the REAL source channel (item.forwardChatId/forwardMessageId)
// via MTProto — never Bot API, never a private chat id.
const forwardedFallbackQueue = [];
let forwardedFallbackActive = 0;

function pumpForwardedFallbackQueue() {
  while (forwardedFallbackActive < COMPRESS_CONCURRENCY && forwardedFallbackQueue.length > 0) {
    const job = forwardedFallbackQueue.shift();
    forwardedFallbackActive++;
    runForwardedFallbackJob(job)
      .catch((err) => log.error('pumpForwardedFallbackQueue', 'Unhandled fallback job error', err, { ...job, stack: err.stack }))
      .finally(() => {
        forwardedFallbackActive--;
        pumpForwardedFallbackQueue();
      });
  }
}

async function runForwardedFallbackJob({ chatId, seq }) {
  const session = sessions.get(chatId);
  if (!session || session.closed) return;
  const item = session.items[seq];
  if (!item) return;

  const stamp = `${chatId}_${seq}_${Date.now()}`;
  const inPath = path.join(UPLOADS_DIR, `${stamp}.src`);
  const outPath = path.join(CONVERTED_DIR, `${stamp}.mp4`);

  for (let attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
    item.attempts.compress = attempt;
    try {
      item.status = 'downloading';
      item.downloadProgress = 0;
      item.downloadMethod = 'mtproto'; // 'forwarded' items ALWAYS use MTProto with the real channel
      log.info('runForwardedFallbackJob', `Download started (attempt ${attempt}/${MAX_ATTEMPTS})`, {
        chatId, seq, sourceType: item.sourceType, pipeline: 'mtproto',
        channelId: item.forwardChatId, messageId: item.forwardMessageId,
      });
      await session.onProgress(session);

      const onProgress = (written, total) => {
        item.downloadProgress = total ? Math.min(100, Math.floor((written / total) * 100)) : null;
        Promise.resolve(session.onProgress(session)).catch(() => {});
      };
      await transfer.downloadFromChannel(item.forwardChatId, item.forwardMessageId, inPath, { onProgress });
      item.tempIn = inPath;
      log.success('runForwardedFallbackJob', 'Download completed', { chatId, seq, path: inPath, method: 'mtproto' });

      item.status = 'compressing';
      item.downloadProgress = null;
      log.info('runForwardedFallbackJob', 'Compression started', { chatId, seq });
      await session.onProgress(session);

      const info = await compress.processFile(inPath, outPath);
      item.tempOut = outPath;
      item.probe = info;
      log.success('runForwardedFallbackJob', 'Compression completed', { chatId, seq, mode: info.mode, duration: info.duration });

      item.status = 'ready';
      await session.onProgress(session);
      pumpUploadQueue();
      return;
    } catch (err) {
      log.error('runForwardedFallbackJob', `Attempt ${attempt}/${MAX_ATTEMPTS} failed`, err, { chatId, seq, stack: err.stack });
      cleanupFile(inPath);
      cleanupFile(outPath);
      item.tempIn = null;
      item.tempOut = null;
      item.downloadProgress = null;

      if (attempt >= MAX_ATTEMPTS) {
        item.status = 'failed';
        item.error = `Download/compression failed after ${MAX_ATTEMPTS} attempts: ${err.message}`;
        log.error('runForwardedFallbackJob', 'Failed', new Error(item.error), { chatId, seq });
        await session.onProgress(session);
        await maybeCompleteSession(session);
        pumpUploadQueue();
        return;
      }
      await sleep(RETRY_BASE_DELAY_MS * attempt);
    }
  }
}

async function uploadPreparedFile(session, item) {
  for (let attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
    item.attempts.upload = attempt;
    try {
      item.status = 'uploading';
      log.info('uploadPreparedFile', `Upload started (attempt ${attempt}/${MAX_ATTEMPTS})`, {
        chatId: session.chatId, seq: item.seq, sourceType: item.sourceType, pipeline: 'mtproto-upload',
      });
      await session.onProgress(session);

      const fileName = buildFileName(session, item, item.episode);
      const result = await transfer.uploadEpisode(session.storageChannelId, item.tempOut, {
        fileName,
        duration: item.probe?.duration || item.durationHint || 0,
        width: item.probe?.width || item.widthHint || 0,
        height: item.probe?.height || item.heightHint || 0,
      });
      log.success('uploadPreparedFile', 'Upload completed', { chatId: session.chatId, seq: item.seq, newMessageId: result.messageId });

      await writeFirestoreDoc(session, item, item.episode, result);
      log.success('uploadPreparedFile', 'Firestore updated', { chatId: session.chatId, seq: item.seq, episode: item.episode });

      item.status = 'done';
      cleanupFile(item.tempIn);
      cleanupFile(item.tempOut);
      log.success('uploadPreparedFile', 'Completed', { chatId: session.chatId, seq: item.seq });
      log.success('uploadPreparedFile', 'Cleanup completed', { chatId: session.chatId, seq: item.seq });

      await session.onProgress(session);
      return;
    } catch (err) {
      log.error('uploadPreparedFile', `Attempt ${attempt}/${MAX_ATTEMPTS} failed`, err, { chatId: session.chatId, seq: item.seq, stack: err.stack });
      if (attempt >= MAX_ATTEMPTS) {
        // Only THIS episode fails — the batch and every other episode
        // keep going (tickSessionUpload skips terminal-status items).
        item.status = 'failed';
        item.error = `Upload failed after ${MAX_ATTEMPTS} attempts: ${err.message}`;
        log.error('uploadPreparedFile', 'Failed', new Error(item.error), { chatId: session.chatId, seq: item.seq });
        cleanupFile(item.tempIn);
        cleanupFile(item.tempOut);
        await session.onProgress(session);
        return;
      }
      await sleep(RETRY_BASE_DELAY_MS * attempt);
    }
  }
}

function buildFileName(session, item, episode) {
  const base = session.title.replace(/[/\\?%*:|"<>]/g, '');
  if (session.hasSeason) {
    return `${base} S${String(session.season).padStart(2, '0')}E${String(episode).padStart(2, '0')}.mp4`;
  }
  const part = item.seq > 0 ? ` Part ${item.seq + 1}` : '';
  return `${base}${part}.mp4`;
}

// ============================================================================
// FIRESTORE WRITE
// ============================================================================

function buildDocId(session, item, episode) {
  if (session.hasSeason) {
    return `${session.kind}_${slugify(session.title)}_s${session.season}_ep${episode}`;
  }
  const suffix = item.seq > 0 ? `_part${item.seq + 1}` : '';
  return `${session.kind.replace('-', '_')}_${slugify(session.title)}${suffix}`;
}

async function writeFirestoreDoc(session, item, episode, uploadResult) {
  const db = getDB();
  const admin = getAdmin();

  // Defensive duplicate re-check — detectDuplicates() already filtered the
  // batch before processing started, but this guards against a race with
  // a concurrent batch saving the same source in the meantime.
  const existingDocId = await findExistingDocId(item.fileUniqueId);
  if (existingDocId) {
    log.warn('writeFirestoreDoc', 'Skipped — source was saved by another batch while this one was processing', {
      chatId: session.chatId, seq: item.seq, fileUniqueId: item.fileUniqueId, existingDocId,
    });
    return;
  }

  const id = buildDocId(session, item, episode);
  const data = {
    title: session.title,
    seriesTitle: session.title,
    category: session.category,
    season: session.hasSeason ? session.season : null,
    episode,
    telegram_file_id: uploadResult.documentId,
    file_unique_id: item.fileUniqueId,
    channelId: Number(uploadResult.channelId),
    messageId: uploadResult.messageId,
    language: session.language,
    quality: session.quality || null,
    year: session.year || null,
    duration: item.probe?.duration || item.durationHint || 0,
    fileSizeBytes: uploadResult.size,
    published: true,
    createdAt: admin.firestore.FieldValue.serverTimestamp(),
    updatedAt: admin.firestore.FieldValue.serverTimestamp(),
  };

  await db.collection(VIDEOS_COLLECTION).doc(id).set(data, { merge: false });
  log.success('writeFirestoreDoc', 'Episode saved to Firestore', { id, title: session.title, episode });
}

// ============================================================================
// PROGRESS RENDERING HELPERS (consumed by handlers/adminUpload.js)
// ============================================================================

const STATUS_ICON = {
  buffered: '📦',
  validated: '📦',
  waiting: '⏳',
  copying: '🔗',
  downloading: '📥',
  compressing: '🔄',
  ready: '📦',
  uploading: '⬆️',
  done: '✅',
  failed: '❌',
  skipped: '⏭',
};
const STATUS_LABEL = {
  buffered: 'buffered',
  validated: 'Validated',
  waiting: 'Waiting',
  copying: 'Copying from channel',
  downloading: 'Downloading',
  compressing: 'Compressing',
  ready: 'Queued',
  uploading: 'Uploading',
  done: 'Uploaded',
  failed: 'Failed',
  skipped: 'Skipped',
};

function episodeLabel(session, item) {
  if (session.hasSeason) return `Episode ${item.episode ?? item.seq + 1}`;
  return item.seq > 0 ? `Part ${item.seq + 1}` : session.title;
}

function renderSessionText(session) {
  const lines = [];
  const label = session.hasSeason ? `${session.title} — Season ${session.season}` : session.title;
  lines.push(`🎬 <b>${escapeHtml(label)}</b>`);
  lines.push('');

  if (!session.locked) {
    // Pre-lock view: exactly the simple checklist requested — nothing has
    // started processing yet, so there's nothing more to report.
    session.items.forEach((item) => {
      lines.push(`📦 ${escapeHtml(episodeLabel(session, item))} buffered`);
    });
    lines.push('', 'ℹ️ Still buffering — send more episodes, or send <b>Done</b> (or /done) to start processing.');
    return lines.join('\n');
  }

  // Post-lock view: live per-item status.
  session.items.forEach((item) => {
    const icon = STATUS_ICON[item.status] || '❓';
    const statusText = STATUS_LABEL[item.status] || item.status;
    let line = `${escapeHtml(episodeLabel(session, item))} ${icon} ${statusText}`;
    if (item.status === 'downloading' && typeof item.downloadProgress === 'number') {
      line += item.downloadMethod === 'mtproto' ? ` (channel, ${item.downloadProgress}%)` : ` (${item.downloadProgress}%)`;
    }
    if ((item.status === 'failed' || item.status === 'skipped') && item.error) {
      line += ` — ${escapeHtml(item.error.slice(0, 200))}`;
    }
    lines.push(line);
  });

  const stillActive = session.items.some((it) => !isTerminal(it.status));
  if (stillActive) lines.push('', '⏳ Processing…');

  return lines.join('\n');
}

function escapeHtml(str) {
  return String(str).replace(/[&<>]/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;' }[c]));
}

module.exports = {
  setBotInstance,
  createSession,
  getSession,
  endSession,
  isSessionActive,
  addItem,
  startBatch,
  renderSessionText,
};
