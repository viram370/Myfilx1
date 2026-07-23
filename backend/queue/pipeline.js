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
 *       4. Starts the download/upload transfer queues for every item
 *          that survived validation. There is no compression/re-encode
 *          stage — every item is uploaded exactly as Telegram delivered
 *          it (see the "NO COMPRESSION" note further down).
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
 *     MTProto's `downloadFromChannel()` — a genuine download to local disk,
 *     verified, then re-uploaded as a brand-new message via `uploadEpisode()`.
 *     HARD RULE: bot.copyMessage()/forwardMessage() are NEVER used for
 *     videos, even as a "fast path" — that was tried once and removed
 *     because it sometimes copied the original message instead of actually
 *     processing the video, producing incorrect/unusable output.
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
const transfer = require('../services/telegramUpload');
const compress = require('../services/compress');
const { getDB, getAdmin } = require('../services/firebase');

// Uses the OS temp dir rather than a path inside the repo — safer across
// hosts (Render, containers, etc.) where the app directory's writability
// or persistence guarantees can vary, but /tmp is always writable.
const TEMP_DIR = path.join(os.tmpdir(), 'myflix-add-pipeline');
const UPLOADS_DIR = path.join(TEMP_DIR, 'uploads'); // downloaded originals — also what gets uploaded, now that there's no compression stage
const CONVERTED_DIR = path.join(TEMP_DIR, 'converted'); // unused now (no compression output), kept/created harmlessly in case anything external still expects the folder to exist
for (const dir of [UPLOADS_DIR, CONVERTED_DIR]) {
  fs.mkdirSync(dir, { recursive: true });
}

const VIDEOS_COLLECTION = 'videos';
// No compression stage anymore — this now governs how many videos are
// transferred (download + upload) at once. Defaults to 1 so only a single
// video's worth of buffers/temp file ever exists at a time, which is what
// keeps this safe to run on a low-RAM host (e.g. Render's free 512MB tier).
// TRANSFER_CONCURRENCY is the current name; COMPRESS_CONCURRENCY is still
// read as a fallback so an existing deployment's env vars keep working.
const TRANSFER_CONCURRENCY = Math.max(1, parseInt(process.env.TRANSFER_CONCURRENCY || process.env.COMPRESS_CONCURRENCY || '1', 10));
const MAX_ATTEMPTS = 3;
const RETRY_BASE_DELAY_MS = 1500;
const SAFETY_TICK_MS = 2000;
const TERMINAL_STATUSES = ['done', 'failed', 'skipped'];

function sleep(ms) { return new Promise((r) => setTimeout(r, ms)); }
function isTerminal(status) { return TERMINAL_STATUSES.includes(status); }

/**
 * Rounds a 0-100 percent down to the nearest 10% step (0, 10, 20 ... 100).
 * Used so progress-message edits only fire once per 10% of real progress
 * (as requested), instead of on every 1% tick — that would spam Telegram's
 * editMessageText rate limit for nothing.
 */
function bucket10(pct) {
  if (typeof pct !== 'number' || Number.isNaN(pct)) return null;
  return Math.max(0, Math.min(100, Math.floor(pct / 10) * 10));
}

/**
 * Updates item[field] to the new 10%-bucketed value and, if (and only if)
 * that bucket actually changed since the last report, asks the session to
 * re-render/edit its single progress message. This is what turns raw
 * byte-by-byte or frame-by-frame progress into the "every 10% only"
 * single-message edit behavior.
 */
function reportBucketedProgress(session, item, field, rawPercent) {
  const b = bucket10(rawPercent);
  if (b === null || item[field] === b) return;
  item[field] = b;
  Promise.resolve(session.onProgress(session)).catch(() => {});
}

/**
 * Tracks a rolling bytes/sec figure for one transfer direction on an item.
 * Called on every raw progress tick (not just on 10%-bucket changes) so the
 * speed reading stays current; it's cheap in-memory arithmetic, not a
 * Telegram call, so calling it often costs nothing. `field` is either
 * 'downloadSpeedState' or 'uploadSpeedState'.
 */
function updateTransferSpeed(item, field, bytesNow) {
  const now = Date.now();
  let state = item[field];
  if (!state) {
    state = { bytes: bytesNow, ts: now, bps: 0 };
    item[field] = state;
    return;
  }
  const dtSeconds = (now - state.ts) / 1000;
  if (dtSeconds < 0.2) return; // avoid noisy readings from back-to-back ticks
  const deltaBytes = bytesNow - state.bytes;
  if (deltaBytes >= 0) state.bps = deltaBytes / dtSeconds;
  state.bytes = bytesNow;
  state.ts = now;
}

function formatSpeed(state) {
  if (!state || !Number.isFinite(state.bps) || state.bps <= 0) return null;
  return `${(state.bps / (1024 * 1024)).toFixed(1)} MB/s`;
}

/** Renders a 10-block text bar like `██████░░░░` for a 0-100 percent value. */
function renderProgressBar(percent, size = 10) {
  const pct = Math.max(0, Math.min(100, percent || 0));
  const filled = Math.round((pct / 100) * size);
  return '█'.repeat(filled) + '░'.repeat(size - filled);
}

function slugify(str) {
  return String(str).toLowerCase().trim().replace(/[^a-z0-9]+/g, '-').replace(/(^-+|-+$)/g, '').slice(0, 80) || 'untitled';
}

// Set once via setBotInstance() at startup (handlers/adminUpload.js) —
// needed here for Bot-API downloads.
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
    // Only present for genuine Documents (Telegram videos never carry a
    // filename of their own) — used purely as a filename hint when
    // building the re-uploaded file's name.
    originalFileName: media.file_name || null,
    originalMimeType: media.mime_type || null,
    // The message the direct upload itself lives in (the admin's private
    // chat). Kept for logging/debugging only — NEVER used to drive an
    // MTProto call (see file header).
    chatMessageId: msg.message_id || null,
    downloadMethod: null, // 'bot-api' | 'mtproto', set once a download starts
    downloadProgress: null, // 0-100 (10% steps), only meaningful while status === 'downloading'
    downloadSpeedState: null, // { bytes, ts, bps } rolling speed tracker for the live progress message
    uploadProgress: null, // 0-100 (10% steps), only meaningful while status === 'uploading'
    uploadSpeedState: null, // { bytes, ts, bps } rolling speed tracker for the live progress message
    startedAt: null, // Date.now() when this item's processing (download) began
    finishedAt: null, // Date.now() when this item reached a terminal state
    durationHint: media.duration || 0,
    widthHint: media.width || 0,
    heightHint: media.height || 0,
    sourceType: channelOrigin ? 'forwarded' : 'direct',
    forwardChatId: channelOrigin ? channelOrigin.chatId : null,
    forwardMessageId: channelOrigin ? channelOrigin.messageId : null,
    // No compression stage anymore: buffered -> [validation: failed|skipped|waiting] -> downloading -> ready -> uploading -> done | failed
    status: 'buffered',
    attempts: { copy: 0, compress: 0, upload: 0 }, // field names kept for compatibility with existing logging/metrics
    episode: null,
    tempIn: null,
    tempOut: null, // now always the same path as tempIn — kept as a separate field only so uploadPreparedFile()'s "file ready to upload" reference doesn't need renaming throughout
    // Populated by verifyDownloadedFile() right after download, from a
    // REAL ffprobe pass over the actual bytes on disk — this is what
    // gets sent to Telegram (see uploadPreparedFile), never the
    // durationHint/widthHint/heightHint above. Telegram's own message
    // metadata is only a fallback hint used for logging/thumbnail
    // seeking; it is frequently 0/absent (documents carry none at all,
    // and even genuine video messages from some clients omit it), and
    // sending DocumentAttributeVideo with duration:0/width:0/height:0 is
    // exactly what makes Telegram's server store the upload as a plain
    // document instead of playable video media.
    // Shape once set: { valid, sizeBytes, duration, width, height, frameRate, mimeType }
    probe: null,
    error: null,
    uploading: false,
    // Local path of the ffmpeg-generated per-EPISODE display thumbnail
    // (see generateEpisodeThumbnailForItem below) — distinct from the
    // anime/series poster (session.thumbnailFileId), which always stays
    // whatever the admin uploaded manually in /addanime. Best-effort:
    // stays null if generation ever fails, and the episode simply gets
    // no episodeThumbnailFileId (frontend falls back to a placeholder).
    tempThumb: null,
    episodeThumbnailFileId: null,
  };
  session.items.push(item);

  log.info('addItem', 'Buffered', {
    chatId: session.chatId, seq, sourceType: item.sourceType,
    channelId: item.forwardChatId, messageId: item.forwardMessageId,
    fileUniqueId: item.fileUniqueId, fileSizeBytes: item.fileSizeBytes,
  });

  // Deliberately does nothing else — no transferQueue push, no
  // pumpUploadQueue call. Processing only begins in startBatch().
  return item;
}

// ============================================================================
// BATCH LOCK — step 2 of the batch-commit workflow. Validates, dedupes,
// numbers, and only THEN starts the download/upload transfer queues.
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

  try {
    await validateItems(session);
    await detectDuplicates(session);
    await assignEpisodeNumbers(session);
  } catch (err) {
    // A failure here (e.g. a Firestore query error during dedup/episode
    // numbering) must never leave the batch silently "locked" with
    // nothing visibly happening — that looks exactly like "the bot isn't
    // doing anything" from the admin's side. Fail every non-terminal item
    // with the real reason and still let the batch reach its normal
    // "complete" state so a clear error is shown instead of dead silence.
    log.error('startBatch', 'Validation/dedup/numbering stage failed — failing the whole batch', err, {
      chatId, stack: err.stack,
    });
    for (const item of session.items) {
      if (isTerminal(item.status)) continue;
      item.status = 'failed';
      item.error = `Batch setup failed: ${err.message}`;
      item.finishedAt = Date.now();
    }
    await session.onProgress(session, { force: true });
    await maybeCompleteSession(session);
    return { started: true };
  }

  await session.onProgress(session, { force: true });

  for (const item of session.items) {
    if (item.status !== 'validated') continue;
    item.status = 'waiting';
    // No compression stage anymore — every upload is the exact original
    // file, always via a real download + brand-new upload (never a
    // copyMessage/forwardMessage shortcut — see runForwardedTransferJob).
    if (item.sourceType === 'forwarded') {
      forwardedFallbackQueue.push({ chatId: session.chatId, seq: item.seq });
    } else {
      transferQueue.push({ chatId: session.chatId, seq: item.seq });
    }
  }
  pumpTransferQueue();
  pumpForwardedFallbackQueue();
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

const transferQueue = [];
let transferActive = 0;

function pumpTransferQueue() {
  while (transferActive < TRANSFER_CONCURRENCY && transferQueue.length > 0) {
    const job = transferQueue.shift();
    transferActive++;
    runDirectTransferJob(job)
      .catch((err) => log.error('pumpTransferQueue', 'Unhandled transfer job error', err, { ...job, stack: err.stack }))
      .finally(() => {
        transferActive--;
        pumpTransferQueue();
      });
  }
}

function cleanupFile(p) {
  if (!p) return;
  fs.unlink(p, (err) => {
    if (err && err.code !== 'ENOENT') log.warn('cleanupFile', 'Failed to remove temp file', { path: p, reason: err.message });
  });
}

/**
 * Auto-generates the per-EPISODE display thumbnail from the just-
 * downloaded video file (see compress.js#generateEpisodeThumbnail —
 * seeks 15-30s in, or ~10% for shorter clips). This is the thumbnail
 * shown for the episode itself (episode list, continue watching,
 * recently watched, recommendations, player) — it is completely
 * separate from the anime/series poster the admin uploads by hand in
 * /addanime, which is never touched here or anywhere in this file.
 *
 * NEVER fatal: any failure (ffmpeg missing, decode error, etc.) is
 * logged and swallowed — item.tempThumb simply stays null, and the
 * episode is saved with no episodeThumbnailFileId. The frontend/
 * serializer treats a missing episode thumbnail as "use a placeholder",
 * never a crash.
 */
/**
 * Runs an ACTUAL ffprobe pass over the just-downloaded file — never
 * trusts Telegram's own duration/width/height message metadata, which is
 * frequently 0/missing (documents carry none of it at all; some client
 * apps omit it even on genuine video messages) and, when sent through
 * unchanged into DocumentAttributeVideo, is exactly what causes Telegram
 * to fall back to storing the upload as a plain document with no inline
 * player — the bug this function fixes. Also confirms the container is
 * genuinely a valid, playable MP4 (real video stream, non-zero duration/
 * resolution/frame rate) BEFORE any upload attempt is made, and derives
 * the real MIME type from the file's own bytes rather than an assumed
 * extension. It does NOT require H.264/AAC or a "faststart" moov-atom
 * position — those are logged for visibility but never block the
 * upload; only Telegram's own post-upload response decides whether a
 * given codec/container/moov-position combination is accepted as
 * playable video.
 *
 * On success, overwrites item.durationHint/widthHint/heightHint with the
 * probed ground-truth values (kept as the same field names so the
 * thumbnail-seek call below and any other reader doesn't need to change)
 * and stores the full probe result on item.probe for the upload step and
 * for debug logging.
 *
 * On failure, marks the item 'failed' directly — an invalid/corrupt
 * container can never become uploadable by retrying the upload step, so
 * this fails fast with a clear, specific reason instead of only being
 * discovered after Telegram rejects the upload as a generic document.
 *
 * @returns {boolean} true if the item is verified-good and processing should continue
 */
async function verifyDownloadedFile(session, item) {
  log.info('verifyDownloadedFile', 'Running ffprobe verification on downloaded file before upload', {
    chatId: session.chatId, seq: item.seq, path: item.tempOut,
    telegramReportedDuration: item.durationHint, telegramReportedWidth: item.widthHint, telegramReportedHeight: item.heightHint,
  });

  let result;
  try {
    result = await compress.verifyOutputFile(item.tempOut);
  } catch (err) {
    log.error('verifyDownloadedFile', 'ffprobe verification crashed', err, { chatId: session.chatId, seq: item.seq, path: item.tempOut, stack: err.stack });
    result = { valid: false, reason: `ffprobe verification crashed: ${err.message}` };
  }

  if (!result.valid) {
    log.error('verifyDownloadedFile', 'Downloaded file failed pre-upload verification — refusing to upload it as video', new Error(result.reason), {
      chatId: session.chatId, seq: item.seq, path: item.tempOut,
    });
    cleanupFile(item.tempOut);
    item.tempIn = null;
    item.tempOut = null;
    item.probe = null;
    item.status = 'failed';
    item.error = `Downloaded file is not a valid playable MP4: ${result.reason}`;
    item.finishedAt = Date.now();
    await session.onProgress(session, { force: true });
    await maybeCompleteSession(session);
    return false;
  }

  item.probe = result;
  // Ground truth from the actual bytes replaces whatever hint Telegram's
  // message metadata carried (which may have been 0/absent/stale).
  item.durationHint = result.duration || 0;
  item.widthHint = result.width || 0;
  item.heightHint = result.height || 0;

  log.success('verifyDownloadedFile', 'ffprobe verification passed', {
    chatId: session.chatId, seq: item.seq, path: item.tempOut,
    mimeType: result.mimeType, duration: result.duration, width: result.width, height: result.height,
    frameRate: result.frameRate, sizeBytes: result.sizeBytes,
    videoCodec: result.videoCodec, audioCodec: result.audioCodec, container: result.container,
  });

  if (!(result.duration > 0) || !(result.width > 0) || !(result.height > 0)) {
    // verifyOutputFile() should already have rejected any of these as
    // invalid, but this is the exact condition that produces Telegram's
    // "no DocumentAttributeVideo" symptom, so it gets its own explicit,
    // impossible-to-miss guard right at the point of use rather than
    // relying solely on that earlier check never regressing.
    log.error('verifyDownloadedFile', 'Probed metadata still has a zero/missing duration, width, or height — refusing to upload with attributes that would make Telegram reject video classification', new Error(`duration=${result.duration} width=${result.width} height=${result.height}`), {
      chatId: session.chatId, seq: item.seq, path: item.tempOut,
    });
    cleanupFile(item.tempOut);
    item.tempIn = null;
    item.tempOut = null;
    item.probe = null;
    item.status = 'failed';
    item.error = `Video metadata is incomplete (duration=${result.duration}, width=${result.width}, height=${result.height}) — cannot upload as playable video.`;
    item.finishedAt = Date.now();
    await session.onProgress(session, { force: true });
    await maybeCompleteSession(session);
    return false;
  }

  return true;
}

async function generateEpisodeThumbnailForItem(session, item) {
  if (!item.tempIn) return;
  const thumbPath = path.join(UPLOADS_DIR, `${session.chatId}_${item.seq}_${Date.now()}.thumb.jpg`);
  try {
    await compress.generateEpisodeThumbnail(item.tempIn, thumbPath, item.durationHint || 0);
    item.tempThumb = thumbPath;
    log.success('generateEpisodeThumbnailForItem', 'Episode thumbnail generated', {
      chatId: session.chatId, seq: item.seq, thumbPath,
    });
  } catch (err) {
    log.warn('generateEpisodeThumbnailForItem', 'Episode thumbnail generation failed — continuing without one', {
      chatId: session.chatId, seq: item.seq, reason: err.message,
    });
    cleanupFile(thumbPath);
    item.tempThumb = null;
  }
}

/**
 * Best-effort thumbnail extraction — NEVER fatal to the item. Telegram's
 * own server-side auto-thumbnail generation for MTProto uploads isn't
 * fully reliable, and a video that ends up with no preview looks
 * identical to one that was sent as a plain document. A failure here
 * just means the upload proceeds without a custom thumbnail (Telegram
 * still gets a chance to generate its own).
 */
async function runDirectTransferJob({ chatId, seq }) {
  const session = sessions.get(chatId);
  if (!session || session.closed) return;
  const item = session.items[seq];
  if (!item) return;

  const stamp = `${chatId}_${seq}_${Date.now()}`;
  const inPath = path.join(UPLOADS_DIR, `${stamp}.src`);

  for (let attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
    item.attempts.compress = attempt; // field name kept for compatibility with anything reading it; this is now the one-and-only transfer attempt counter
    try {
      item.status = 'downloading';
      item.downloadProgress = 0;
      item.downloadSpeedState = null;
      item.downloadMethod = 'bot-api'; // 'direct' items ALWAYS use the Bot API — never MTProto
      if (!item.startedAt) item.startedAt = Date.now();
      log.info('runDirectTransferJob', `Download started (attempt ${attempt}/${MAX_ATTEMPTS})`, {
        chatId, seq, sourceType: item.sourceType, pipeline: 'bot-api', fileId: item.fileId, fileSizeBytes: item.fileSizeBytes,
      });
      await session.onProgress(session, { force: true });

      await transfer.downloadViaBotApi(botInstance, item.fileId, inPath, {
        onProgress: (written, total) => {
          if (!total) return;
          updateTransferSpeed(item, 'downloadSpeedState', written);
          reportBucketedProgress(session, item, 'downloadProgress', (written / total) * 100);
        },
      });
      // No compression stage — the file downloaded here is exactly what
      // gets uploaded. item.tempOut is kept (rather than renaming every
      // downstream reference) as "the file ready to upload"; it's simply
      // now always the same path as item.tempIn.
      item.tempIn = inPath;
      item.tempOut = inPath;
      item.downloadProgress = 100;
      log.success('runDirectTransferJob', 'Download completed — verifying before upload (no re-encoding)', { chatId, seq, path: inPath, method: 'bot-api' });

      const verified = await verifyDownloadedFile(session, item);
      if (!verified) { pumpUploadQueue(); return; }

      await generateEpisodeThumbnailForItem(session, item);

      item.status = 'ready';
      await session.onProgress(session, { force: true });
      pumpUploadQueue();
      return;
    } catch (err) {
      const tooBig = /too big/i.test(err.message || '');
      log.error('runDirectTransferJob', `Attempt ${attempt}/${MAX_ATTEMPTS} failed`, err, { chatId, seq, stack: err.stack });
      cleanupFile(inPath);
      item.tempIn = null;
      item.tempOut = null;
      item.downloadProgress = null;

      // A "too big" rejection from the Bot API can never succeed on retry
      // (there is no MTProto fallback for a direct upload) — fail fast.
      if (attempt >= MAX_ATTEMPTS || tooBig) {
        // Only THIS episode fails — the batch and every other episode
        // keep going (tickSessionUpload skips terminal-status items).
        item.status = 'failed';
        item.error = tooBig ? err.message : `Download failed after ${MAX_ATTEMPTS} attempts: ${err.message}`;
        item.finishedAt = Date.now();
        log.error('runDirectTransferJob', 'Failed', new Error(item.error), { chatId, seq });
        await session.onProgress(session, { force: true });
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
    await uploadPreparedFile(session, next);
  } finally {
    next.uploading = false;
  }
  pumpUploadQueue(); // immediately check whether the next item is also ready
}

// ---- transfer for 'forwarded' items (videos forwarded to the bot from ----
// a channel) — always a real MTProto download + brand-new re-upload,
// never bot.copyMessage()/forwardMessage(). Always uses the REAL source
// channel (item.forwardChatId/forwardMessageId), never Bot API, never a
// private chat id. See runForwardedTransferJob.
const forwardedFallbackQueue = [];
let forwardedFallbackActive = 0;

function pumpForwardedFallbackQueue() {
  while (forwardedFallbackActive < TRANSFER_CONCURRENCY && forwardedFallbackQueue.length > 0) {
    const job = forwardedFallbackQueue.shift();
    forwardedFallbackActive++;
    runForwardedTransferJob(job)
      .catch((err) => log.error('pumpForwardedFallbackQueue', 'Unhandled fallback job error', err, { ...job, stack: err.stack }))
      .finally(() => {
        forwardedFallbackActive--;
        pumpForwardedFallbackQueue();
      });
  }
}

async function runForwardedTransferJob({ chatId, seq }) {
  const session = sessions.get(chatId);
  if (!session || session.closed) return;
  const item = session.items[seq];
  if (!item) return;

  if (!item.startedAt) item.startedAt = Date.now();

  // NEVER use bot.copyMessage()/forwardMessage() for videos — this was
  // tried as a zero-download "fast path" and removed again: it sometimes
  // copied the original Telegram message instead of actually processing
  // the video, producing incorrect/unusable output. Every forwarded item,
  // no exceptions, goes through the real MTProto download below, then
  // uploadPreparedFile() sends it back out as a genuinely new upload —
  // never a copy or forward of anything.
  const stamp = `${chatId}_${seq}_${Date.now()}`;
  const inPath = path.join(UPLOADS_DIR, `${stamp}.src`);

  for (let attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
    item.attempts.compress = attempt; // field name kept for compatibility with anything reading it; this is now the one-and-only transfer attempt counter
    try {
      item.status = 'downloading';
      item.downloadProgress = 0;
      item.downloadSpeedState = null;
      item.downloadMethod = 'mtproto'; // 'forwarded' items ALWAYS use MTProto with the real channel
      log.info('runForwardedTransferJob', `Download started (attempt ${attempt}/${MAX_ATTEMPTS})`, {
        chatId, seq, sourceType: item.sourceType, pipeline: 'mtproto',
        channelId: item.forwardChatId, messageId: item.forwardMessageId,
      });
      await session.onProgress(session, { force: true });

      await transfer.downloadFromChannel(item.forwardChatId, item.forwardMessageId, inPath, {
        onProgress: (written, total) => {
          if (!total) return;
          updateTransferSpeed(item, 'downloadSpeedState', written);
          reportBucketedProgress(session, item, 'downloadProgress', (written / total) * 100);
        },
      });
      // No compression stage — the file downloaded here is exactly what
      // gets uploaded as a brand-new message (same pattern as runDirectTransferJob).
      item.tempIn = inPath;
      item.tempOut = inPath;
      item.downloadProgress = 100;
      log.success('runForwardedTransferJob', 'Download completed — verifying before upload as a new file (no re-encoding, no copy/forward)', { chatId, seq, path: inPath, method: 'mtproto' });

      const verified = await verifyDownloadedFile(session, item);
      if (!verified) { pumpUploadQueue(); return; }

      await generateEpisodeThumbnailForItem(session, item);

      item.status = 'ready';
      await session.onProgress(session, { force: true });
      pumpUploadQueue();
      return;
    } catch (err) {
      log.error('runForwardedTransferJob', `Attempt ${attempt}/${MAX_ATTEMPTS} failed`, err, { chatId, seq, stack: err.stack });
      cleanupFile(inPath);
      item.tempIn = null;
      item.tempOut = null;
      item.downloadProgress = null;

      // A verified-source failure (deleted message / wrong channel / no
      // media) can never succeed on retry — fail fast with the SPECIFIC
      // reason instead of retrying blindly and burying it under a
      // generic "failed after N attempts" wrapper.
      const isSourceError = err.name === 'SourceNotFoundError';

      if (attempt >= MAX_ATTEMPTS || isSourceError) {
        item.status = 'failed';
        item.error = isSourceError ? err.message : `Download failed after ${MAX_ATTEMPTS} attempts: ${err.message}`;
        item.finishedAt = Date.now();
        log.error('runForwardedTransferJob', 'Failed', new Error(item.error), { chatId, seq });
        await session.onProgress(session, { force: true });
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
      item.uploadProgress = 0;
      item.uploadSpeedState = null;
      const uploadStartedAt = Date.now();
      let localFileSizeBytes = null;
      try { localFileSizeBytes = fs.statSync(item.tempOut).size; } catch (_) { /* logged as null, uploadEpisode will hard-fail if the file is truly gone */ }

      log.info('uploadPreparedFile', `Upload started (attempt ${attempt}/${MAX_ATTEMPTS})`, {
        chatId: session.chatId, seq: item.seq, sourceType: item.sourceType, pipeline: 'mtproto-upload',
        uploading: item.tempOut, localFileSizeBytes,
      });
      await session.onProgress(session, { force: true });

      // item.probe is guaranteed non-null here: uploadPreparedFile only
      // ever runs on items that reached 'ready', and the only path to
      // 'ready' (both runDirectTransferJob and runForwardedTransferJob)
      // requires verifyDownloadedFile() to have succeeded first. If that
      // invariant is ever violated, fail loudly here rather than
      // silently falling back to the unverified Telegram hint fields —
      // the exact bug this whole pipeline exists to fix.
      if (!item.probe || !item.probe.valid) {
        throw new Error('uploadPreparedFile: item reached the upload step with no verified ffprobe result — refusing to upload with unverified video metadata.');
      }
      const fileName = buildFileName(session, item, item.episode);
      const result = await transfer.uploadEpisode(session.storageChannelId, item.tempOut, {
        fileName,
        // Real, ffprobe-verified values from the actual bytes about to
        // be uploaded (see verifyDownloadedFile) — never Telegram's own
        // message metadata, which is frequently 0/absent and is exactly
        // what causes Telegram to store the upload as a plain document.
        duration: item.probe.duration,
        width: item.probe.width,
        height: item.probe.height,
        mimeType: item.probe.mimeType || 'video/mp4',
        // Informational only (debug logging) — never used to gate the
        // upload. The file is uploaded exactly as downloaded regardless
        // of codec/container (e.g. HEVC/H.265 sources are uploaded
        // unmodified; only Telegram's own post-upload response decides
        // acceptance — see telegramUpload.js#uploadEpisode).
        videoCodec: item.probe.videoCodec,
        audioCodec: item.probe.audioCodec,
        container: item.probe.container,
        thumbPath: item.tempThumb || undefined, // reuse the auto-generated episode thumbnail (if any) as Telegram's own message preview too
        onProgress: (percent) => {
          if (Number.isFinite(localFileSizeBytes)) updateTransferSpeed(item, 'uploadSpeedState', Math.round((percent / 100) * localFileSizeBytes));
          reportBucketedProgress(session, item, 'uploadProgress', percent);
        },
      });
      item.uploadProgress = 100;
      log.success('uploadPreparedFile', 'Upload completed', {
        chatId: session.chatId, seq: item.seq, newMessageId: result.messageId,
        localFileSizeBytes, telegramDocumentSizeBytes: result.size, elapsedMs: Date.now() - uploadStartedAt,
      });

      // Best-effort: push the ffmpeg-generated episode thumbnail (if one
      // was produced) up to Telegram as its own photo message so we get a
      // resolvable file_id — see telegramUpload.js#uploadEpisodeThumbnailPhoto.
      // A failure here is NEVER fatal to the episode: it just gets saved
      // with no episodeThumbnailFileId, and the frontend falls back to a
      // placeholder image instead of crashing.
      if (item.tempThumb) {
        try {
          item.episodeThumbnailFileId = await transfer.uploadEpisodeThumbnailPhoto(botInstance, session.storageChannelId, item.tempThumb);
        } catch (thumbErr) {
          log.warn('uploadPreparedFile', 'Episode thumbnail photo upload failed — saving episode with no episode thumbnail', {
            chatId: session.chatId, seq: item.seq, reason: thumbErr.message,
          });
          item.episodeThumbnailFileId = null;
        }
        cleanupFile(item.tempThumb);
        item.tempThumb = null;
      }

      await writeFirestoreDoc(session, item, item.episode, result);
      log.success('uploadPreparedFile', 'Firestore updated', { chatId: session.chatId, seq: item.seq, episode: item.episode });

      item.status = 'done';
      item.uploadProgress = null;
      item.finishedAt = Date.now();
      cleanupFile(item.tempIn);
      const totalElapsedMs = item.startedAt ? item.finishedAt - item.startedAt : null;
      const mem = process.memoryUsage();
      log.success('uploadPreparedFile', 'Completed', {
        chatId: session.chatId, seq: item.seq, totalElapsedMs, finishedAt: new Date(item.finishedAt).toISOString(),
        memoryRssMB: Math.round(mem.rss / 1024 / 1024), memoryHeapUsedMB: Math.round(mem.heapUsed / 1024 / 1024),
      });
      log.success('uploadPreparedFile', 'Cleanup completed', { chatId: session.chatId, seq: item.seq });

      await session.onProgress(session, { force: true });
      return;
    } catch (err) {
      log.error('uploadPreparedFile', `Attempt ${attempt}/${MAX_ATTEMPTS} failed`, err, { chatId: session.chatId, seq: item.seq, stack: err.stack });
      item.uploadProgress = null;
      if (attempt >= MAX_ATTEMPTS) {
        // Only THIS episode fails — the batch and every other episode
        // keep going (tickSessionUpload skips terminal-status items).
        item.status = 'failed';
        item.error = `Upload failed after ${MAX_ATTEMPTS} attempts: ${err.message}`;
        item.finishedAt = Date.now();
        log.error('uploadPreparedFile', 'Failed', new Error(item.error), { chatId: session.chatId, seq: item.seq });
        cleanupFile(item.tempIn);
        cleanupFile(item.tempThumb);
        item.tempThumb = null;
        await session.onProgress(session, { force: true });
        return;
      }
      await sleep(RETRY_BASE_DELAY_MS * attempt);
    }
  }
}

/** Maps a Telegram mimeType to a reasonable file extension. */
function extensionForMimeType(mimeType) {
  const m = (mimeType || '').toLowerCase();
  if (m.includes('matroska')) return '.mkv';
  if (m.includes('webm')) return '.webm';
  if (m.includes('quicktime')) return '.mov';
  if (m === 'video/x-msvideo') return '.avi';
  if (m.includes('mp2t')) return '.ts';
  if (m.includes('mp4')) return '.mp4';
  return null;
}

/**
 * No compression/remux stage anymore — every upload is the original file
 * exactly as Telegram delivered it, so the extension has to come from
 * what Telegram told us about the source (filename, then mimeType),
 * never assumed to always be MP4.
 */
function resolveExtension(item) {
  if (item.originalFileName) {
    const ext = path.extname(item.originalFileName);
    if (ext) return ext;
  }
  return extensionForMimeType(item.originalMimeType) || '.mp4';
}

function buildFileName(session, item, episode) {
  const base = session.title.replace(/[/\\?%*:|"<>]/g, '');
  const ext = resolveExtension(item);
  if (session.hasSeason) {
    return `${base} S${String(session.season).padStart(2, '0')}E${String(episode).padStart(2, '0')}${ext}`;
  }
  const part = item.seq > 0 ? ` Part ${item.seq + 1}` : '';
  return `${base}${part}${ext}`;
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
    // The anime/series poster — ALWAYS the image the admin uploaded by
    // hand during /addanime. Never overwritten with an episode thumbnail.
    thumbnailFileId: session.thumbnailFileId || null,
    // The auto-generated per-EPISODE display thumbnail (see
    // generateEpisodeThumbnailForItem above) — separate field, separate
    // image, used only when displaying this individual episode. Null if
    // generation/upload failed for this episode; the frontend falls back
    // to a placeholder rather than ever substituting the anime poster.
    episodeThumbnailFileId: item.episodeThumbnailFileId || null,
    duration: item.durationHint || 0,
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
  downloading: '⬇️',
  ready: '📦',
  uploading: '⬆️',
  done: '✅',
  failed: '❌',
  skipped: '⏭',
};
const STATUS_LABEL = {
  buffered: 'Waiting...',
  validated: 'Validated',
  waiting: 'Waiting...',
  copying: 'Copying from channel...',
  downloading: 'Downloading...',
  ready: 'Queued...',
  uploading: 'Uploading...',
  done: 'Completed',
  failed: 'Failed',
  skipped: 'Skipped',
};

function episodeLabel(session, item) {
  if (session.hasSeason) return `Episode ${item.episode ?? item.seq + 1}`;
  return item.seq > 0 ? `Part ${item.seq + 1}` : session.title;
}

/**
 * Renders one item's block in the single, repeatedly-edited progress
 * message, in the requested format:
 *   ⬇️ Downloading...
 *   ██████░░░░ 63%
 *   Speed: 24.8 MB/s
 * — only the current 10%-stepped percent and the latest speed reading are
 * shown (not a history of every value passed through), because this whole
 * block gets replaced in place on every edit. No "Compressing" phase
 * exists anymore — every item goes straight from Downloading to Uploading.
 */
function renderItemBlock(session, item) {
  const icon = STATUS_ICON[item.status] || '❓';
  const label = episodeLabel(session, item);
  const lines = [`${icon} <b>${escapeHtml(label)}</b>`];

  if (item.status === 'done') {
    lines[0] = `✅ <b>${escapeHtml(label)}</b>`;
    lines.push('✅ Upload completed.');
    return lines.join('\n');
  }

  if (item.status === 'failed') {
    lines[0] = `❌ <b>${escapeHtml(label)} Failed</b>`;
    lines.push('Reason:');
    lines.push(escapeHtml((item.error || 'Unknown error').slice(0, 300)));
    return lines.join('\n');
  }

  if (item.status === 'skipped') {
    lines.push(`Skipped — ${escapeHtml((item.error || 'duplicate').slice(0, 200))}`);
    return lines.join('\n');
  }

  if (item.status === 'downloading' && typeof item.downloadProgress === 'number') {
    lines.push('⬇️ Downloading...');
    lines.push(`${renderProgressBar(item.downloadProgress)} ${item.downloadProgress}%`);
    const speed = formatSpeed(item.downloadSpeedState);
    if (speed) lines.push(`Speed: ${speed}`);
    return lines.join('\n');
  }

  if (item.status === 'uploading' && typeof item.uploadProgress === 'number') {
    lines.push('⬆️ Uploading...');
    lines.push(`${renderProgressBar(item.uploadProgress)} ${item.uploadProgress}%`);
    const speed = formatSpeed(item.uploadSpeedState);
    if (speed) lines.push(`Speed: ${speed}`);
    return lines.join('\n');
  }

  if (item.status === 'copying') {
    lines.push('Please wait...');
    return lines.join('\n');
  }

  lines.push(STATUS_LABEL[item.status] || item.status);
  return lines.join('\n');
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
      lines.push(`📦 ${escapeHtml(episodeLabel(session, item))}`);
      lines.push('Status: Waiting...');
      lines.push('');
    });
    lines.push('ℹ️ Still buffering — send more episodes, or send <b>Done</b> (or /done) to start processing.');
    return lines.join('\n');
  }

  // Post-lock view: live per-item status, one block per episode, all
  // inside this SAME single message (never a new message per episode).
  session.items.forEach((item) => {
    lines.push(renderItemBlock(session, item));
    lines.push('');
  });

  const stillActive = session.items.some((it) => !isTerminal(it.status));
  if (stillActive) lines.push('⏳ Processing…');
  else lines.pop(); // drop the trailing blank line on the final render

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
