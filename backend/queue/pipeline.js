/**
 * queue/pipeline.js
 * ----------------------------------------------------------------------
 * Background compress → upload pipeline for handlers/adminUpload.js
 * (/add anime | webseries | anime-movie | movie).
 *
 *   Telegram video received
 *     -> immediately acknowledged, pushed onto the compression queue
 *     -> (background) downloaded from Telegram
 *     -> (background) FFmpeg remux/transcode -> browser-streamable MP4
 *     -> marked "ready"
 *     -> upload worker uploads READY items to the storage channel,
 *        but STRICTLY in the order they were received — an episode
 *        that finishes compressing early still waits for every earlier
 *        episode to finish uploading first.
 *     -> Firestore doc written (auto season/episode numbering)
 *     -> temp files deleted
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
const path = require('path');
const { makeLogger } = require('../utils/logger');
const log = makeLogger('queue/pipeline.js');
const compress = require('../services/compress');
const transfer = require('../services/telegramUpload');
const { getDB, getAdmin } = require('../services/firebase');

const TEMP_DIR = path.join(__dirname, '..', 'temp');
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

// ============================================================================
// SESSIONS
// ============================================================================

/** @type {Map<number, object>} keyed by admin chatId */
const sessions = new Map();

/**
 * @param {number} chatId
 * @param {object} meta { kind: 'anime'|'webseries'|'anime-movie'|'movie', category, title, season, language, quality, year, storageChannelId }
 * @param {(text:string)=>Promise<void>} onProgress called (debounced by caller) whenever item states change
 * @param {(session:object)=>Promise<void>} onFinished called once every item is done or the session is paused on failure
 */
function createSession(chatId, meta, { onProgress, onFinished }) {
  const session = {
    chatId,
    ...meta,
    items: [],
    episodeBase: null, // resolved lazily, once, from Firestore
    paused: false,
    finalizing: false,
    closed: false,
    onProgress,
    onFinished,
  };
  sessions.set(chatId, session);
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

/**
 * Ends a session right away and reports the final summary. Used both for
 * the normal "everything settled" completion path and for the pause
 * path below — pausing means "stop everything, nothing more happens
 * automatically", so the chat shouldn't stay permanently blocked from
 * starting a new /add session just because some items never got past
 * "waiting" (still-queued jobs bail out harmlessly once the session is
 * gone — see the `if (!session ...) return;` guards below).
 */
async function closeSession(session) {
  if (session.closed) return;
  session.closed = true;
  try {
    await session.onFinished(session);
  } catch (err) {
    log.error('closeSession', 'onFinished callback threw', err, { chatId: session.chatId });
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
// ADDING ITEMS (called from the Telegram video/document handler)
// ============================================================================

/**
 * @param {object} session
 * @param {{file_id:string, file_unique_id:string, file_size?:number}} media
 * @param {{chat:{id:number}, message_id:number}} msg the raw Telegram message carrying the video
 */
function addItem(session, media, msg) {
  const seq = session.items.length;
  const item = {
    seq,
    fileUniqueId: media.file_unique_id,
    sourceChannelId: msg.chat.id,
    sourceMessageId: msg.message_id,
    fileSizeBytes: media.file_size || null,
    status: 'waiting', // waiting -> downloading -> compressing -> ready -> uploading -> done | failed
    attempts: { compress: 0, upload: 0 },
    episode: null,
    tempIn: null,
    tempOut: null,
    probe: null,
    error: null,
    uploading: false,
  };
  session.items.push(item);
  compressQueue.push({ chatId: session.chatId, seq });
  pumpCompressQueue();
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
      .catch((err) => log.error('pumpCompressQueue', 'Unhandled compress job error', err, job))
      .finally(() => {
        compressActive--;
        pumpCompressQueue();
      });
  }
}

function cleanupFile(p) {
  if (!p) return;
  fs.unlink(p, () => {}); // best-effort, never blocks the pipeline on cleanup errors
}

async function runCompressJob({ chatId, seq }) {
  const session = sessions.get(chatId);
  if (!session || session.closed) return;
  const item = session.items[seq];
  if (!item || session.paused) return;

  const stamp = `${chatId}_${seq}_${Date.now()}`;
  const inPath = path.join(UPLOADS_DIR, `${stamp}.src`);
  const outPath = path.join(CONVERTED_DIR, `${stamp}.mp4`);

  for (let attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
    item.attempts.compress = attempt;
    try {
      item.status = 'downloading';
      await session.onProgress(session);

      await transfer.downloadEpisode(item.sourceChannelId, item.sourceMessageId, inPath);
      item.tempIn = inPath;

      item.status = 'compressing';
      await session.onProgress(session);

      const info = await compress.processFile(inPath, outPath);
      item.tempOut = outPath;
      item.probe = info;

      item.status = 'ready';
      await session.onProgress(session);
      pumpUploadQueue();
      return;
    } catch (err) {
      log.error('runCompressJob', `Compression attempt ${attempt}/${MAX_ATTEMPTS} failed`, err, { chatId, seq });
      cleanupFile(inPath);
      cleanupFile(outPath);
      if (attempt >= MAX_ATTEMPTS) {
        item.status = 'failed';
        item.error = `Compression failed after ${MAX_ATTEMPTS} attempts: ${err.message}`;
        session.paused = true;
        await session.onProgress(session);
        await closeSession(session);
        return;
      }
      await sleep(RETRY_BASE_DELAY_MS * attempt);
    }
  }
}

// ============================================================================
// UPLOAD QUEUE (strict FIFO across the whole session — never skips ahead)
// ============================================================================

let uploadTickScheduled = false;

function pumpUploadQueue() {
  if (uploadTickScheduled) return;
  uploadTickScheduled = true;
  setImmediate(async () => {
    uploadTickScheduled = false;
    try {
      for (const session of sessions.values()) {
        if (session.paused || session.closed) continue;
        await tickSessionUpload(session);
      }
    } catch (err) {
      log.error('pumpUploadQueue', 'Tick failed', err);
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
    await uploadItem(session, next);
  } finally {
    next.uploading = false;
  }
}

async function uploadItem(session, item) {
  for (let attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
    item.attempts.upload = attempt;
    try {
      item.status = 'uploading';
      await session.onProgress(session);

      const episode = session.hasSeason ? (await resolveEpisodeBase(session)) + item.seq : null;
      item.episode = episode;

      const fileName = buildFileName(session, item, episode);
      const result = await transfer.uploadEpisode(session.storageChannelId, item.tempOut, {
        fileName,
        duration: item.probe?.duration || 0,
        width: item.probe?.width || 0,
        height: item.probe?.height || 0,
      });

      await writeFirestoreDoc(session, item, episode, result);

      item.status = 'done';
      cleanupFile(item.tempIn);
      cleanupFile(item.tempOut);
      await session.onProgress(session);
      pumpUploadQueue();
      return;
    } catch (err) {
      log.error('uploadItem', `Upload attempt ${attempt}/${MAX_ATTEMPTS} failed`, err, { chatId: session.chatId, seq: item.seq });
      if (attempt >= MAX_ATTEMPTS) {
        item.status = 'failed';
        item.error = `Upload failed after ${MAX_ATTEMPTS} attempts: ${err.message}`;
        session.paused = true; // "pause queue, do not continue with later episodes"
        await session.onProgress(session);
        await closeSession(session);
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
    duration: item.probe?.duration || 0,
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
  downloading: '📥',
  compressing: '🔄',
  ready: '📦',
  uploading: '⬆️',
  done: '✅',
  failed: '❌',
};
const STATUS_LABEL = {
  waiting: 'Waiting',
  downloading: 'Downloading',
  compressing: 'Compressing',
  ready: 'Queued for upload',
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
    if (item.status === 'failed' && item.error) line += ` — ${escapeHtml(item.error.slice(0, 120))}`;
    lines.push(line);
  });

  if (session.paused) {
    lines.push('', '⏸ <b>Queue paused</b> — an episode failed after 3 retries. Fix the issue and re-upload it, or clear this batch and start again.');
  } else if (!session.finalizing) {
    lines.push('', 'ℹ️ Still accepting episodes — send more, or send <b>Done</b> when finished.');
  }

  return lines.join('\n');
}

function escapeHtml(str) {
  return String(str).replace(/[&<>]/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;' }[c]));
}

module.exports = {
  createSession,
  getSession,
  endSession,
  isSessionActive,
  finalizeSession,
  addItem,
  renderSessionText,
};
