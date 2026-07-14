/**
 * queue/pipeline.js
 * ----------------------------------------------------------------------
 * Background compress → upload pipeline for handlers/adminUpload.js
 * (/add anime | webseries | anime-movie | movie).
 *
 * SOURCE HANDLING (fixes "Source message not found (channelId=<userId>,
 * messageId=<n>)"):
 *   - A video uploaded DIRECTLY to the bot is identified by its Telegram
 *     `file_id` (for identity/dedup) and downloaded by SIZE:
 *       - <=20MB: the plain Bot API (services/telegramUpload.js
 *         #downloadViaBotApi / bot.getFile), same as before.
 *       - >20MB: the existing MTProto (GramJS) client — logged in as this
 *         same bot — reads the file straight out of the admin's private
 *         chat with the bot (services/telegramUpload.js
 *         #downloadViaMTProto), using the message's own chatId+messageId.
 *         This is NOT the channel-message lookup that broke before: a
 *         private chat is a user peer rather than a channel, but
 *         mtproto.js's entity/message resolution is peer-agnostic and
 *         handles both the same way. The admin never needs to forward
 *         anything to a channel — large files are picked up automatically.
 *       - If the reported size is missing/wrong and the Bot API rejects
 *         the file as "too big" anyway, the download transparently falls
 *         back to MTProto rather than failing the episode.
 *   - A video FORWARDED from a channel is detected from the incoming
 *     message's forward metadata. We first try a server-side
 *     bot.copyMessage() straight into the storage channel (no download
 *     at all); if that isn't possible (bot not in the source channel,
 *     message gone, etc.) we transparently fall back to the same
 *     file_id-based download+compress+upload path used for direct
 *     uploads.
 *   - A "Copy"'d message (Telegram's own copy feature, as opposed to
 *     Forward) carries no forward metadata at all — Telegram makes it
 *     indistinguishable from a direct upload — so it naturally goes
 *     through the direct-upload path, which is the correct behavior.
 *
 * QUEUE BEHAVIOR:
 *   - Each item moves through its own state machine independently:
 *     waiting -> (copying -> [done]) OR (downloading -> compressing ->
 *     ready -> uploading -> done), with 'failed' reachable from any
 *     active stage after 3 retries of THAT stage only.
 *   - Compression/download runs in the background with bounded
 *     concurrency, order-independent — the bot keeps accepting new
 *     episodes while others are mid-pipeline.
 *   - The final "publish" step (copy, or upload-to-storage-channel +
 *     Firestore write) is strictly ordered: episode 3 never gets
 *     published before episode 1 and 2, even if it finished compressing
 *     first.
 *   - A failed item does NOT stop the batch — remaining episodes keep
 *     processing, and the exact error is shown to the admin against that
 *     episode's line in the progress message.
 *
 * One session = one admin's currently-open /add wizard + its queue of
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

function sleep(ms) { return new Promise((r) => setTimeout(r, ms)); }

function slugify(str) {
  return String(str).toLowerCase().trim().replace(/[^a-z0-9]+/g, '-').replace(/(^-+|-+$)/g, '').slice(0, 80) || 'untitled';
}

// Set once via setBotInstance() at startup (handlers/adminUpload.js) —
// needed here for Bot-API downloads and copyMessage, neither of which
// go through the MTProto client.
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
 * @param {(session:object)=>Promise<void>} callbacks.onFinished called once every item reaches done/failed and the wizard is finalized
 */
function createSession(chatId, meta, { onProgress, onFinished }) {
  const session = {
    chatId,
    ...meta,
    items: [],
    episodeBase: null, // resolved lazily, once, from Firestore
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

/** Marks the wizard as done accepting new episodes; pipeline keeps draining what's queued. */
function finalizeSession(chatId) {
  const session = sessions.get(chatId);
  if (!session) return;
  session.finalizing = true;
  maybeCompleteSession(session);
}

async function maybeCompleteSession(session) {
  if (!session.finalizing || session.closed) return;
  const allSettled = session.items.every((it) => it.status === 'done' || it.status === 'failed');
  if (!allSettled) return;
  await closeSession(session);
}

async function closeSession(session) {
  if (session.closed) return;
  session.closed = true;
  const done = session.items.filter((it) => it.status === 'done').length;
  const failed = session.items.filter((it) => it.status === 'failed').length;
  log.info('closeSession', 'Session complete', { chatId: session.chatId, done, failed, total: session.items.length });
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
 * Detects whether an incoming message was forwarded from a channel, using
 * the current Bot API's `forward_origin` field (Bot API 7.0+) with a
 * fallback to the legacy `forward_from_chat`/`forward_from_message_id`
 * fields for older clients/library versions. Returns null for a direct
 * upload OR a Telegram "Copy"'d message — Telegram does not attach any
 * forward metadata to copied messages, so those are indistinguishable
 * from direct uploads and correctly fall through to the download path.
 */
function detectForwardOrigin(msg) {
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
// ADDING ITEMS (called from the Telegram video/document handler)
// ============================================================================

/**
 * @param {object} session
 * @param {{file_id:string, file_unique_id:string, file_size?:number, duration?:number, width?:number, height?:number}} media
 * @param {object} msg the raw Telegram message carrying the video
 */
function addItem(session, media, msg) {
  const seq = session.items.length;
  const forwardOrigin = detectForwardOrigin(msg);

  const item = {
    seq,
    fileId: media.file_id,
    fileUniqueId: media.file_unique_id,
    fileSizeBytes: media.file_size || null,
    // The chat + message the video itself lives in (the admin's private
    // chat with the bot). Used by downloadSourceFile() to fetch the file
    // via MTProto when it's too large for the Bot API — set regardless of
    // sourceType, since a failed channel-copy attempt also falls back to
    // downloading straight from this same message (see tryCopyPath).
    chatMessageId: msg.message_id || null,
    downloadMethod: null, // 'bot-api' | 'mtproto', set once a download starts
    downloadProgress: null, // 0-100, only meaningful while status === 'downloading'
    durationHint: media.duration || 0,
    widthHint: media.width || 0,
    heightHint: media.height || 0,
    sourceType: forwardOrigin ? 'forwarded' : 'direct',
    forwardChatId: forwardOrigin ? forwardOrigin.chatId : null,
    forwardMessageId: forwardOrigin ? forwardOrigin.messageId : null,
    needsCopyAttempt: !!forwardOrigin,
    status: 'waiting', // waiting -> [copying ->] downloading -> compressing -> ready -> uploading -> done | failed
    attempts: { copy: 0, compress: 0, upload: 0 },
    episode: null,
    tempIn: null,
    tempOut: null,
    probe: null,
    error: null,
    uploading: false,
  };
  session.items.push(item);

  log.info('addItem', 'Upload received', {
    chatId: session.chatId, seq, sourceType: item.sourceType,
    fileUniqueId: item.fileUniqueId, fileSizeBytes: item.fileSizeBytes,
  });

  if (item.sourceType === 'forwarded') {
    // copyMessage is a cheap, near-instant server-side Telegram call, so
    // there's no need to background it like compression — it's attempted
    // when the item's turn comes up in the strictly-ordered publish step.
    item.status = 'ready';
    pumpUploadQueue();
  } else {
    compressQueue.push({ chatId: session.chatId, seq });
    pumpCompressQueue();
  }
  return item;
}

// ============================================================================
// COMPRESSION QUEUE (bounded concurrency, order-independent)
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
      log.info('runCompressJob', `Download started (attempt ${attempt}/${MAX_ATTEMPTS})`, {
        chatId, seq, fileId: item.fileId, fileSizeBytes: item.fileSizeBytes,
      });
      await session.onProgress(session);

      await downloadSourceFile(session, item, inPath);
      item.tempIn = inPath;
      log.success('runCompressJob', 'Download completed', { chatId, seq, path: inPath, method: item.downloadMethod });

      item.status = 'compressing';
      item.downloadProgress = null;
      log.info('runCompressJob', 'Compression started', { chatId, seq });
      await session.onProgress(session);

      const info = await compress.processFile(inPath, outPath);
      item.tempOut = outPath;
      item.probe = info;
      log.success('runCompressJob', 'Compression completed', { chatId, seq, mode: info.mode, duration: info.duration });

      item.status = 'ready';
      item.needsCopyAttempt = false;
      await session.onProgress(session);
      pumpUploadQueue();
      return;
    } catch (err) {
      log.error('runCompressJob', `Attempt ${attempt}/${MAX_ATTEMPTS} failed`, err, { chatId, seq, stack: err.stack });
      cleanupFile(inPath);
      cleanupFile(outPath);
      item.tempIn = null;
      item.tempOut = null;
      item.downloadProgress = null;

      if (attempt >= MAX_ATTEMPTS) {
        // Only THIS episode fails — the batch and every other episode
        // keep going (tickSessionUpload skips 'failed' items).
        item.status = 'failed';
        item.error = `Download/compression failed after ${MAX_ATTEMPTS} attempts: ${err.message}`;
        await session.onProgress(session);
        await maybeCompleteSession(session);
        pumpUploadQueue();
        return;
      }
      await sleep(RETRY_BASE_DELAY_MS * attempt);
    }
  }
}

/**
 * Downloads the source video for `item` into `inPath`, choosing the
 * transport purely by size:
 *   - at or under the Bot API's 20MB download cap: bot.getFile() via
 *     transfer.downloadViaBotApi (unchanged, fast, no MTProto round trip).
 *   - over that cap: the existing MTProto (GramJS) client, reading the
 *     file directly out of the message it arrived in
 *     (session.chatId + item.chatMessageId) — the admin's private chat
 *     with the bot. No forwarding to a channel is required.
 * If the reported size is missing/wrong and the Bot API rejects the file
 * as "too big" anyway, this transparently retries over MTProto instead of
 * failing the attempt outright.
 */
async function downloadSourceFile(session, item, inPath) {
  const knownSize = typeof item.fileSizeBytes === 'number' && item.fileSizeBytes > 0;
  const tooBigForBotApi = knownSize && item.fileSizeBytes > transfer.BOT_API_DOWNLOAD_LIMIT_BYTES;

  const onProgress = (written, total) => {
    item.downloadProgress = total ? Math.min(100, Math.floor((written / total) * 100)) : null;
    // Debounced internally by renderProgressBySession — safe to call on
    // every chunk without spamming Telegram's editMessageText rate limit.
    Promise.resolve(session.onProgress(session)).catch(() => {});
  };

  if (tooBigForBotApi) {
    item.downloadMethod = 'mtproto';
    log.info('downloadSourceFile', 'File exceeds Bot API 20MB cap — downloading via MTProto', {
      chatId: session.chatId, seq: item.seq, fileSizeBytes: item.fileSizeBytes,
    });
    return transfer.downloadViaMTProto(session.chatId, item.chatMessageId, inPath, { onProgress });
  }

  item.downloadMethod = 'bot-api';
  try {
    return await transfer.downloadViaBotApi(botInstance, item.fileId, inPath);
  } catch (err) {
    if (/too big/i.test(err.message || '')) {
      log.warn('downloadSourceFile', 'Bot API rejected file as too big despite reported size — falling back to MTProto', {
        chatId: session.chatId, seq: item.seq, fileSizeBytes: item.fileSizeBytes,
      });
      item.downloadMethod = 'mtproto';
      return transfer.downloadViaMTProto(session.chatId, item.chatMessageId, inPath, { onProgress });
    }
    throw err;
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
        if (session.closed) continue;
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
  const next = session.items.find((it) => it.status !== 'done' && it.status !== 'failed');
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
  log.info('tryCopyPath', 'Copy attempt started', { chatId: session.chatId, seq: item.seq, attempt: item.attempts.copy });
  await session.onProgress(session);

  try {
    const { messageId } = await transfer.copyMessageToStorage(
      botInstance, item.forwardChatId, item.forwardMessageId, session.storageChannelId
    );

    const episode = session.hasSeason ? (await resolveEpisodeBase(session)) + item.seq : null;
    item.episode = episode;

    await writeFirestoreDoc(session, item, episode, {
      channelId: session.storageChannelId,
      messageId,
      documentId: item.fileId,
      size: item.fileSizeBytes || 0,
    });
    log.success('tryCopyPath', 'Firestore updated', { chatId: session.chatId, seq: item.seq, episode });

    item.status = 'done';
    log.success('tryCopyPath', 'Cleanup completed (nothing to clean up — no local files were used)', { chatId: session.chatId, seq: item.seq });
    await session.onProgress(session);
    return true;
  } catch (err) {
    log.error('tryCopyPath', 'Copy not possible — falling back to download+compress+upload', err, { chatId: session.chatId, seq: item.seq, stack: err.stack });
    item.needsCopyAttempt = false;
    item.status = 'waiting';
    await session.onProgress(session);
    compressQueue.push({ chatId: session.chatId, seq: item.seq });
    pumpCompressQueue();
    return false;
  }
}

async function uploadPreparedFile(session, item) {
  for (let attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
    item.attempts.upload = attempt;
    try {
      item.status = 'uploading';
      log.info('uploadPreparedFile', `Upload started (attempt ${attempt}/${MAX_ATTEMPTS})`, { chatId: session.chatId, seq: item.seq });
      await session.onProgress(session);

      const episode = session.hasSeason ? (await resolveEpisodeBase(session)) + item.seq : null;
      item.episode = episode;

      const fileName = buildFileName(session, item, episode);
      const result = await transfer.uploadEpisode(session.storageChannelId, item.tempOut, {
        fileName,
        duration: item.probe?.duration || item.durationHint || 0,
        width: item.probe?.width || item.widthHint || 0,
        height: item.probe?.height || item.heightHint || 0,
      });
      log.success('uploadPreparedFile', 'Upload completed', { chatId: session.chatId, seq: item.seq, newMessageId: result.messageId });

      await writeFirestoreDoc(session, item, episode, result);
      log.success('uploadPreparedFile', 'Firestore updated', { chatId: session.chatId, seq: item.seq, episode });

      item.status = 'done';
      cleanupFile(item.tempIn);
      cleanupFile(item.tempOut);
      log.success('uploadPreparedFile', 'Cleanup completed', { chatId: session.chatId, seq: item.seq });

      await session.onProgress(session);
      return;
    } catch (err) {
      log.error('uploadPreparedFile', `Attempt ${attempt}/${MAX_ATTEMPTS} failed`, err, { chatId: session.chatId, seq: item.seq, stack: err.stack });
      if (attempt >= MAX_ATTEMPTS) {
        // Only THIS episode fails — the batch and every other episode
        // keep going (tickSessionUpload skips 'failed' items).
        item.status = 'failed';
        item.error = `Upload failed after ${MAX_ATTEMPTS} attempts: ${err.message}`;
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

  // Duplicate-source guard, same pattern as the existing /saveanime flow.
  const dupSnap = await db.collection(VIDEOS_COLLECTION)
    .where('file_unique_id', '==', item.fileUniqueId).limit(1).get();
  if (!dupSnap.empty) {
    log.warn('writeFirestoreDoc', 'Skipped write — source already saved under another doc', { fileUniqueId: item.fileUniqueId });
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
  waiting: '⏳',
  copying: '🔗',
  downloading: '📥',
  compressing: '🔄',
  ready: '📦',
  uploading: '⬆️',
  done: '✅',
  failed: '❌',
};
const STATUS_LABEL = {
  waiting: 'Waiting',
  copying: 'Copying from channel',
  downloading: 'Downloading',
  compressing: 'Compressing',
  ready: 'Queued',
  uploading: 'Uploading',
  done: 'Uploaded',
  failed: 'Failed',
};

function renderSessionText(session) {
  const lines = [];
  const label = session.hasSeason ? `${session.title} — Season ${session.season}` : session.title;
  lines.push(`🎬 <b>${escapeHtml(label)}</b>`);
  lines.push('');

  session.items.forEach((item) => {
    const epLabel = session.hasSeason
      ? `Episode ${item.episode ?? session.items.indexOf(item) + 1}`
      : (item.seq > 0 ? `Part ${item.seq + 1}` : session.title);
    const icon = STATUS_ICON[item.status] || '❓';
    const statusText = STATUS_LABEL[item.status] || item.status;
    let line = `${escapeHtml(epLabel)} ${icon} ${statusText}`;
    if (item.status === 'downloading' && typeof item.downloadProgress === 'number') {
      line += item.downloadMethod === 'mtproto' ? ` (large file, ${item.downloadProgress}%)` : ` (${item.downloadProgress}%)`;
    }
    if (item.status === 'failed' && item.error) line += ` — ${escapeHtml(item.error.slice(0, 200))}`;
    lines.push(line);
  });

  const stillActive = session.items.some((it) => it.status !== 'done' && it.status !== 'failed');
  if (!session.finalizing) {
    lines.push('', 'ℹ️ Still accepting episodes — send more, or send <b>Done</b> when finished.');
  } else if (stillActive) {
    lines.push('', '⏳ Finishing remaining episodes…');
  }

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
  finalizeSession,
  addItem,
  renderSessionText,
};
