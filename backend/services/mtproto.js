/**
 * services/mtproto.js
 * ----------------------------------------------------------------------
 * True MTProto client (GramJS) used ONLY for streaming large video files.
 * The classic Telegram Bot API (`getFile`) caps downloads at 20MB, which
 * is why the old implementation broke on anything bigger than a short
 * clip. GramJS talks MTProto directly (the same protocol Telegram apps
 * use) and can pull arbitrary byte ranges out of a document without ever
 * loading the whole file into memory — exactly what HTTP Range streaming
 * needs.
 *
 * Setup (one-time):
 *   1. Get TELEGRAM_API_ID / TELEGRAM_API_HASH from https://my.telegram.org
 *   2. Run `node scripts/generateSession.js` once to mint a
 *      TELEGRAM_STRING_SESSION (bot-token login, fully non-interactive).
 *   3. Put both in your environment. If they're missing, this module
 *      simply stays disabled — save-time verification and streaming will
 *      degrade gracefully with a clear error instead of crashing the app.
 * ----------------------------------------------------------------------
 */
'use strict';

const { makeLogger } = require('../utils/logger');
const log = makeLogger('services/mtproto.js');

const API_ID = parseInt(process.env.TELEGRAM_API_ID, 10);
const API_HASH = process.env.TELEGRAM_API_HASH;
const STRING_SESSION = process.env.TELEGRAM_STRING_SESSION || '';
const BOT_TOKEN = process.env.TELEGRAM_BOT_TOKEN;

const CONFIGURED = Boolean(API_ID && API_HASH && BOT_TOKEN);

const ENTITY_CACHE_TTL_MS = 10 * 60_000;
const MAX_CONCURRENT_STREAMS = parseInt(process.env.MAX_CONCURRENT_STREAMS || '12', 10);
const RECONNECT_CHECK_INTERVAL_MS = 30_000;
const DEFAULT_CHUNK_SIZE = 512 * 1024; // 512KB — Telegram-friendly, aligned chunk size

let client = null;
let connecting = null;
let lastError = null;
/** @type {Map<string, {doc:any, size:number, mimeType:string, fileName:?string, fetchedAt:number}>} */
const entityCache = new Map();

// ---- tiny semaphore: caps concurrent MTProto downloads so Render's ----
// ---- limited CPU/RAM/socket budget never gets overrun by many users. --
let activeStreams = 0;
const waitQueue = [];
function acquireSlot() {
  if (activeStreams < MAX_CONCURRENT_STREAMS) {
    activeStreams++;
    return Promise.resolve();
  }
  return new Promise((resolve) => waitQueue.push(resolve));
}
function releaseSlot() {
  activeStreams = Math.max(0, activeStreams - 1);
  const next = waitQueue.shift();
  if (next) {
    activeStreams++;
    next();
  }
}

class SourceNotFoundError extends Error {
  constructor(msg) {
    super(msg);
    this.name = 'SourceNotFoundError';
  }
}
class MTProtoDisabledError extends Error {
  constructor() {
    super('MTProto streaming is not configured (missing TELEGRAM_API_ID / TELEGRAM_API_HASH / TELEGRAM_STRING_SESSION).');
    this.name = 'MTProtoDisabledError';
  }
}
/**
 * Thrown when we could not write exactly the number of bytes we already
 * promised the browser via Content-Length. The caller (routes/stream.js)
 * MUST destroy the connection on this error rather than end() it - see
 * streamRange() below for why.
 */
class StreamIntegrityError extends Error {
  constructor(msg) {
    super(msg);
    this.name = 'StreamIntegrityError';
  }
}

// Each individual Telegram upload.GetFile RPC gets its own timeout, so a
// single stuck request fails fast and loud instead of hanging silently
// until the browser gives up and closes the connection.
const CHUNK_REQUEST_TIMEOUT_MS = 20000;

function withTimeout(promise, ms, label) {
  let timer;
  const timeout = new Promise((_, reject) => {
    timer = setTimeout(() => reject(new Error(`${label} timed out after ${ms}ms`)), ms);
  });
  return Promise.race([promise, timeout]).finally(() => clearTimeout(timer));
}

/**
 * Build a ready-to-use Api.InputDocumentFileLocation for a video Document.
 * client.iterDownload({ file: <Api.Document> }) depends on GramJS's
 * internal FileLike -> InputFileLocation auto-cast recognizing a bare
 * Document, which is not reliable across GramJS versions. Building the
 * location ourselves sidesteps that entirely.
 */
function buildFileLocation(doc) {
  const { Api } = require('telegram');
  return new Api.InputDocumentFileLocation({
    id: doc.id,
    accessHash: doc.accessHash,
    fileReference: doc.fileReference,
    thumbSize: '', // '' = full file, not a thumbnail
  });
}

function isEnabled() {
  return CONFIGURED;
}

async function connect() {
  if (!CONFIGURED) return null;
  if (client && client.connected) return client;
  if (connecting) return connecting;

  connecting = (async () => {
    try {
      // Lazy-require so a deployment without these packages installed
      // doesn't crash at boot for features that don't need streaming yet.
      const { TelegramClient } = require('telegram');
      const { StringSession } = require('telegram/sessions');

      const session = new StringSession(STRING_SESSION);
      const newClient = new TelegramClient(session, API_ID, API_HASH, {
        connectionRetries: 8,
        retryDelay: 1000,
        autoReconnect: true,
        floodSleepThreshold: 60,
        useWSS: false,
      });

      await newClient.start({
        botAuthToken: BOT_TOKEN,
        onError: (err) => log.error('connect', 'GramJS auth error', err),
      });

      if (!STRING_SESSION) {
        log.warn('connect', 'No TELEGRAM_STRING_SESSION was set — save this to your env to skip re-auth next boot', {
          session: session.save(),
        });
      }

      // Populate the entity/access-hash cache for every chat the bot is in,
      // so getMessages() on a channel works without a prior interaction.
      try {
        await newClient.getDialogs({ limit: 200 });
      } catch (err) {
        log.warn('connect', 'Initial getDialogs() warm-up failed (non-fatal)', { reason: err.message });
      }

      client = newClient;
      lastError = null;
      log.success('connect', 'MTProto client connected');
      return client;
    } catch (err) {
      lastError = err;
      log.error('connect', 'MTProto connection failed', err);
      throw err;
    } finally {
      connecting = null;
    }
  })();

  return connecting;
}

async function initMTProto() {
  if (!CONFIGURED) {
    log.warn('initMTProto', 'MTProto disabled — set TELEGRAM_API_ID/TELEGRAM_API_HASH/TELEGRAM_STRING_SESSION to enable true large-file streaming');
    return;
  }
  try {
    await connect();
  } catch {
    // already logged in connect(); keep the app alive and retry via watchdog
  }

  setInterval(async () => {
    if (!CONFIGURED) return;
    if (client && client.connected) return;
    log.warn('watchdog', 'MTProto client disconnected — attempting reconnect');
    try {
      await connect();
    } catch (err) {
      log.error('watchdog', 'Reconnect attempt failed', err);
    }
  }, RECONNECT_CHECK_INTERVAL_MS).unref?.();
}

function normalizeChannelId(channelId) {
  // Bot-API style IDs look like -1001234567890. GramJS's entity resolver
  // (utils.resolveId) understands this "marked ID" form directly.
  const n = Number(channelId);
  if (!Number.isInteger(n) || n === 0) {
    throw new MTProtoValidationError(`Invalid channelId: ${channelId}`);
  }
  return n;
}

class MTProtoValidationError extends Error {
  constructor(msg) {
    super(msg);
    this.name = 'MTProtoValidationError';
  }
}

async function resolveEntity(channelId, { retried = false } = {}) {
  const c = await connect();
  if (!c) throw new MTProtoDisabledError();
  const id = normalizeChannelId(channelId);
  try {
    return await c.getEntity(id);
  } catch (err) {
    if (!retried) {
      log.warn('resolveEntity', 'Entity not cached — refreshing dialogs and retrying once', { channelId: id });
      try {
        await c.getDialogs({ limit: 200 });
      } catch (e2) {
        log.warn('resolveEntity', 'Dialog refresh failed', { reason: e2.message });
      }
      return resolveEntity(id, { retried: true });
    }
    throw new SourceNotFoundError(`Cannot resolve channel ${channelId}: ${err.message}. Ensure the bot account is a member/admin of this channel.`);
  }
}

/**
 * Fetches message + document metadata for (channelId, messageId), with a
 * short TTL cache so repeated Range requests during one viewing session
 * don't re-hit Telegram's getMessages for every seek.
 */
async function resolveVideoSource(channelId, messageId) {
  const cacheKey = `${channelId}:${messageId}`;
  const cached = entityCache.get(cacheKey);
  if (cached && Date.now() - cached.fetchedAt < ENTITY_CACHE_TTL_MS) return cached;

  const c = await connect();
  if (!c) throw new MTProtoDisabledError();

  const messageIdNum = Number(messageId);
  if (!Number.isInteger(messageIdNum) || messageIdNum <= 0) {
    throw new MTProtoValidationError(`Invalid messageId: ${messageId}`);
  }

  const entity = await resolveEntity(channelId);
  const messages = await c.getMessages(entity, { ids: [messageIdNum] });
  const message = messages && messages[0];

  if (!message || message.className === 'MessageEmpty') {
    throw new SourceNotFoundError(`Source message not found (channelId=${channelId}, messageId=${messageId}). It may have been deleted.`);
  }
  if (!message.media || (!message.media.document && !message.video && !message.document)) {
    throw new SourceNotFoundError(`Message ${messageId} in channel ${channelId} has no video/document media (corrupted or wrong reference).`);
  }

  const doc = message.media.document || message.document;
  if (!doc || !doc.size) {
    throw new SourceNotFoundError(`Message ${messageId} media has no readable document (corrupted Telegram message).`);
  }

  const fileNameAttr = (doc.attributes || []).find((a) => a.className === 'DocumentAttributeFilename');

  const result = {
    message,
    doc,
    size: Number(doc.size),
    mimeType: doc.mimeType || 'video/mp4',
    fileName: fileNameAttr?.fileName || null,
    fetchedAt: Date.now(),
  };
  entityCache.set(cacheKey, result);
  return result;
}

/** Best-effort validation used by the bot before saving a video. */
async function verifyMessage(channelId, messageId) {
  if (!CONFIGURED) return { ok: true, skipped: true };
  try {
    const src = await resolveVideoSource(channelId, messageId);
    return { ok: true, size: src.size, mimeType: src.mimeType, fileName: src.fileName };
  } catch (err) {
    return { ok: false, reason: err.message };
  }
}

/**
 * Streams bytes [start, end] (inclusive) for a Telegram video document
 * directly into an HTTP response, honoring backpressure and client
 * disconnects, without ever buffering the whole file in memory.
 *
 * ---------------------------------------------------------------------
 * FIX (playback corruption / "Could not play video"): this previously used
 * client.iterDownload({ limit: bigInt(length), ... }). Two problems:
 *
 *  1. GramJS's DownloadIter treats ANY chunk shorter than `requestSize` as
 *     end-of-stream and stops the generator immediately - even when that
 *     short chunk lands in the middle of the requested range, not at the
 *     real end of the file. Telegram can and does return short chunks
 *     depending on account tier/DC, well before the actual EOF. This is
 *     exactly what produced the truncated response: the loop would exit
 *     having written fewer bytes than were requested.
 *  2. The old `finally` block called res.end() unconditionally, with NO
 *     check that the bytes actually written matched the Content-Length
 *     already sent in headers. That produces a response that LOOKS
 *     complete (right status, right headers) but has a body shorter than
 *     promised - a classically corrupt HTTP response that HTML5 <video>
 *     correctly refuses to play, even though bytes were clearly received.
 *
 * Fix: drive the download manually with direct upload.GetFile RPCs (the
 * same low-level primitive GramJS's iterDownload wraps), track bytesSent
 * ourselves, and verify bytesSent === contentLength before ever calling
 * res.end(). On mismatch we throw StreamIntegrityError instead - the
 * caller (routes/stream.js) destroys the connection rather than ending it
 * normally, so the browser sees a hard failure instead of a silently
 * truncated "successful" download. Each chunk request also gets its own
 * timeout so a stuck RPC fails fast instead of hanging until the browser
 * gives up.
 * ---------------------------------------------------------------------
 */
async function streamRange(channelId, messageId, start, end, res, { onAbort } = {}) {
  await acquireSlot();
  let aborted = false;
  const abortHandler = () => { aborted = true; };
  if (onAbort) onAbort(abortHandler);

  try {
    const c = await connect();
    if (!c) throw new MTProtoDisabledError();

    const { doc } = await resolveVideoSource(channelId, messageId);
    const bigInt = require('big-integer');
    const { Api } = require('telegram');

    const contentLength = end - start + 1;

    // MTProto requires the offset passed to upload.GetFile to be an exact
    // multiple of the requested chunk size, so align down to the nearest
    // chunk boundary and trim the leading bytes of the first chunk to
    // land exactly on the requested `start`.
    const alignedStart = start - (start % DEFAULT_CHUNK_SIZE);
    const skipBytes = start - alignedStart;

    const fileLocation = buildFileLocation(doc);

    let offset = bigInt(alignedStart);
    let bytesSent = 0;
    let isFirstChunk = true;
    let chunkIndex = 0;

    while (!aborted && bytesSent < contentLength) {
      chunkIndex += 1;

      let result;
      try {
        result = await withTimeout(
          c.invoke(new Api.upload.GetFile({ location: fileLocation, offset, limit: DEFAULT_CHUNK_SIZE })),
          CHUNK_REQUEST_TIMEOUT_MS,
          `GetFile chunk#${chunkIndex} offset=${offset.toString()}`
        );
      } catch (err) {
        log.error('streamRange', 'GetFile chunk request failed or timed out', err, {
          channelId, messageId, chunkIndex, offset: offset.toString(),
        });
        throw err;
      }

      if (result.className === 'upload.FileCdnRedirect') {
        throw new Error('TELEGRAM_CDN_REDIRECT_UNSUPPORTED');
      }

      const bytes = result.bytes;
      if (!bytes || bytes.length === 0) {
        // Genuine end of file - Telegram has nothing more to give us.
        log.warn('streamRange', 'Telegram returned an empty chunk (end of file)', {
          channelId, messageId, chunkIndex, bytesSent, contentLength,
        });
        break;
      }

      let piece = bytes;
      if (isFirstChunk) {
        isFirstChunk = false;
        if (skipBytes > 0) piece = piece.subarray(skipBytes);
      }

      const remaining = contentLength - bytesSent;
      if (piece.length > remaining) piece = piece.subarray(0, remaining);

      if (piece.length > 0 && !aborted && !res.destroyed && !res.writableEnded) {
        const ok = res.write(piece);
        bytesSent += piece.length;
        if (!ok) {
          await new Promise((resolve) => res.once('drain', resolve));
        }
      }

      offset = offset.add(bytes.length);

      // A chunk shorter than the requested size means Telegram has
      // genuinely reached the end of the file at this DC/account tier -
      // there is nothing more to fetch, regardless of how much of the
      // requested range remains unfilled.
      if (bytes.length < DEFAULT_CHUNK_SIZE) break;
    }

    if (aborted) {
      // Client disconnected mid-stream (paused, seeked again, closed the
      // tab). The socket is already gone or going away - nothing to
      // verify or write further.
      log.info('streamRange', 'Aborted by client disconnect', { channelId, messageId, bytesSent, contentLength });
      return;
    }

    if (bytesSent !== contentLength) {
      // We already promised `contentLength` bytes via Content-Length but
      // wrote fewer (or more). Ending the response normally here would
      // silently hand the browser a body that doesn't match the header
      // it already parsed - exactly the "Could not play video" failure
      // mode. Throw so the caller destroys the connection instead.
      const integrityErr = new StreamIntegrityError(
        `Streamed ${bytesSent} of ${contentLength} promised bytes for ${channelId}:${messageId} ` +
          `(range ${start}-${end})`
      );
      log.error('streamRange', 'Integrity check failed - byte count mismatch', integrityErr, {
        channelId, messageId, bytesSent, contentLength, start, end,
      });
      throw integrityErr;
    }

    // Success: exactly the promised number of bytes were written. The
    // caller (routes/stream.js) is responsible for calling res.end() -
    // streamRange never ends or destroys the response itself on the
    // success path, so callers always control exactly when/how the
    // response finishes.
  } finally {
    releaseSlot();
  }
}

/**
 * Downloads a full Telegram document to a local file path. Used by the
 * upload-pipeline compression worker (services/telegramTransfer.js) to
 * pull a source video onto disk before handing it to FFmpeg — this is a
 * plain sequential download (not range-based like streamRange), so it
 * does not share the streaming semaphore with live playback requests.
 */
async function downloadToFile(channelId, messageId, destPath, { onProgress } = {}) {
  const c = await connect();
  if (!c) throw new MTProtoDisabledError();

  const fs = require('fs');
  const bigInt = require('big-integer');
  const { Api } = require('telegram');

  const { doc, size } = await resolveVideoSource(channelId, messageId);
  const fileLocation = buildFileLocation(doc);
  const out = fs.createWriteStream(destPath);

  let offset = bigInt(0);
  let written = 0;
  let chunkIndex = 0;

  try {
    while (written < size) {
      chunkIndex += 1;
      const result = await withTimeout(
        c.invoke(new Api.upload.GetFile({ location: fileLocation, offset, limit: DEFAULT_CHUNK_SIZE })),
        CHUNK_REQUEST_TIMEOUT_MS,
        `downloadToFile chunk#${chunkIndex} offset=${offset.toString()}`
      );

      if (result.className === 'upload.FileCdnRedirect') {
        throw new Error('TELEGRAM_CDN_REDIRECT_UNSUPPORTED');
      }

      const bytes = result.bytes;
      if (!bytes || bytes.length === 0) break;

      await new Promise((resolve, reject) => {
        out.write(bytes, (err) => (err ? reject(err) : resolve()));
      });

      written += bytes.length;
      offset = offset.add(bytes.length);
      if (onProgress) onProgress(written, size);
      if (bytes.length < DEFAULT_CHUNK_SIZE) break;
    }

    if (written !== size) {
      throw new Error(`downloadToFile: wrote ${written} of ${size} expected bytes for ${channelId}:${messageId}`);
    }
  } finally {
    await new Promise((resolve) => out.end(resolve));
  }

  const fileNameAttr = (doc.attributes || []).find((a) => a.className === 'DocumentAttributeFilename');
  return { path: destPath, size: written, mimeType: doc.mimeType || 'video/mp4', fileName: fileNameAttr?.fileName || null };
}

function health() {
  return {
    configured: CONFIGURED,
    connected: Boolean(client && client.connected),
    lastError: lastError?.message || null,
    activeStreams,
    queuedStreams: waitQueue.length,
    cachedSources: entityCache.size,
  };
}

async function shutdown() {
  try {
    if (client) await client.disconnect();
    log.info('shutdown', 'MTProto client disconnected cleanly');
  } catch (err) {
    log.error('shutdown', 'Error during MTProto disconnect', err);
  }
}

setInterval(() => {
  const now = Date.now();
  for (const [k, v] of entityCache.entries()) {
    if (now - v.fetchedAt > ENTITY_CACHE_TTL_MS * 3) entityCache.delete(k);
  }
}, ENTITY_CACHE_TTL_MS).unref?.();

module.exports = {
  initMTProto,
  isEnabled,
  resolveVideoSource,
  verifyMessage,
  streamRange,
  downloadToFile,
  connect,
  resolveEntity,
  health,
  shutdown,
  SourceNotFoundError,
  MTProtoDisabledError,
  MTProtoValidationError,
  StreamIntegrityError,
};
