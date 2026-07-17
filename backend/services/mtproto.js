/**
 * services/mtproto.js
 * ----------------------------------------------------------------------
 * True MTProto client (GramJS), using public multi-client pooling for
 * seamless and production-ready data center migration.
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

// ---- DC migration / retry tuning -----------------------------------
const CHUNK_MAX_RETRIES = parseInt(process.env.MTPROTO_CHUNK_MAX_RETRIES || '5', 10);
const CHUNK_RETRY_BASE_DELAY_MS = 800;
const CHUNK_RETRY_MAX_DELAY_MS = 8000;

let client = null;
let connecting = null;
let lastError = null;
/** DC id the primary connection is currently authorized against, once known. */
let currentDcId = null;

/** @type {Map<number, import('telegram').TelegramClient>} Cache for multi-client DC connections */
const dcClients = new Map();
/** @type {Map<number, Promise<import('telegram').TelegramClient>>} In-flight client factory synchronization */
const dcClientCreationInFlight = new Map();
/** @type {Map<string, {doc:any, size:number, mimeType:string, fileName:?string, fetchedAt:number}>} */
const entityCache = new Map();

// ---- semaphore for concurrent stream capping ----
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
class StreamIntegrityError extends Error {
  constructor(msg) {
    super(msg);
    this.name = 'StreamIntegrityError';
  }
}

const CHUNK_REQUEST_TIMEOUT_MS = 20000;

function withTimeout(promise, ms, label) {
  let timer;
  const timeout = new Promise((_, reject) => {
    timer = setTimeout(() => reject(new Error(`${label} timed out after ${ms}ms`)), ms);
  });
  return Promise.race([promise, timeout]).finally(() => clearTimeout(timer));
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function buildFileLocation(doc) {
  const { Api } = require('telegram');
  return new Api.InputDocumentFileLocation({
    id: doc.id,
    accessHash: doc.accessHash,
    fileReference: doc.fileReference,
    thumbSize: '',
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

      try {
        await newClient.getDialogs({ limit: 200 });
      } catch (err) {
        log.warn('connect', 'Initial getDialogs() warm-up failed (non-fatal)', { reason: err.message });
      }

      client = newClient;
      lastError = null;
      try {
        currentDcId = newClient.session?.dcId ?? null;
      } catch (_) {
        currentDcId = null;
      }
      if (currentDcId) {
        dcClients.set(currentDcId, client);
      }
      log.success('connect', 'MTProto client connected', { homeDc: currentDcId });
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
    // Retried via watchdog
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

/**
 * Resolves a dedicated, fully authorized TelegramClient pool connection for a target DC
 * using entirely standard public APIs.
 */
async function getClientForDC(dcId) {
  if (!client) {
    throw new Error("Primary Telegram client is not started yet.");
  }

  const homeDc = currentDcId || client.session.dcId;
  if (dcId === homeDc) {
    return client;
  }

  const cachedClient = dcClients.get(dcId);
  if (cachedClient) {
    if (cachedClient.connected) return cachedClient;
    try {
      await cachedClient.connect();
      return cachedClient;
    } catch (err) {
      dcClients.delete(dcId);
    }
  }

  if (dcClientCreationInFlight.has(dcId)) {
    return dcClientCreationInFlight.get(dcId);
  }

  const creationPromise = (async () => {
    console.log(`[MTProto] Migration method used: Creating target client pool for DC ${dcId}`);
    const { TelegramClient } = require('telegram');
    const { StringSession } = require('telegram/sessions');
    const { Api } = require('telegram');

    const config = await client.invoke(new Api.help.GetConfig());
    const options = (config.dcOptions || []).filter((o) => o.id === dcId && !o.cdn);
    const best = options.find((o) => !o.ipv6 && !o.mediaOnly) || options.find((o) => !o.ipv6) || options[0];
    
    if (!best) {
      throw new Error(`No reachable network endpoint resolved for DC ${dcId}`);
    }

    console.log(`[MTProto] Client created: Initializing fresh connection to DC ${dcId}`);
    const session = new StringSession("");
    const newClient = new TelegramClient(session, API_ID, API_HASH, {
      connectionRetries: 5,
      retryDelay: 2000,
      autoReconnect: true,
      floodSleepThreshold: 60,
      useWSS: false,
    });

    newClient.session.setDC(dcId, best.ipAddress, best.port);
    await newClient.connect();

    console.log("[MTProto] Exporting authorization...");
    const exported = await client.invoke(new Api.auth.ExportAuthorization({ dcId }));

    console.log("[MTProto] Authorization imported...");
    await newClient.invoke(
      new Api.auth.ImportAuthorization({ id: exported.id, bytes: exported.bytes })
    );

    console.log(`[MTProto] Client for DC ${dcId} successfully added to pool cache.`);
    dcClients.set(dcId, newClient);
    return newClient;
  })().finally(() => {
    dcClientCreationInFlight.delete(dcId);
  });

  dcClientCreationInFlight.set(dcId, creationPromise);
  return creationPromise;
}

function parseMigrateError(err) {
  const msg = (err && (err.errorMessage || err.message)) || '';
  const match = /^(FILE|NETWORK|USER|PHONE)_MIGRATE_(\d+)$/.exec(String(msg).trim());
  if (match) return parseInt(match[2], 10);
  
  const stringMatch = /currently stored in DC (\d+)/i.exec(String(msg));
  if (stringMatch) return parseInt(stringMatch[1], 10);
  
  return null;
}

/**
 * Handles target DC execution, error-catching migration, and retry loop configurations
 */
async function requestChunkWithRetry(c, fileLocation, offset, limit, context, targetDcId) {
  const { Api } = require('telegram');
  let lastErr;
  let activeDcId = targetDcId;

  for (let attempt = 1; attempt <= CHUNK_MAX_RETRIES; attempt++) {
    try {
      const activeClient = await getClientForDC(activeDcId);
      
      console.log(`[MTProto] Current DC: ${currentDcId || c.session.dcId}`);
      console.log(`[MTProto] File DC: ${activeDcId}`);

      const result = await withTimeout(
        activeClient.invoke(new Api.upload.GetFile({ location: fileLocation, offset, limit })),
        CHUNK_REQUEST_TIMEOUT_MS,
        `GetFile offset=${offset.toString()}`
      );
      
      return { result, finalDcId: activeDcId };
    } catch (err) {
      lastErr = err;
      const migratedDc = parseMigrateError(err);

      if (migratedDc) {
        console.log(`[MTProto] Migration method used: Catching migration runtime error redirection to DC ${migratedDc}`);
        activeDcId = migratedDc;
        console.log(`[MTProto] Download resumed: Retrying payload extraction at target DC ${migratedDc}`);
        continue;
      }

      if (attempt === CHUNK_MAX_RETRIES) break;
      const delay = Math.min(CHUNK_RETRY_BASE_DELAY_MS * 2 ** (attempt - 1), CHUNK_RETRY_MAX_DELAY_MS);
      await sleep(delay);
    }
  }
  throw lastErr;
}

async function resolveEntity(channelId, { retried = false } = {}) {
  const c = await connect();
  if (!c) throw new MTProtoDisabledError();
  const id = normalizeChannelId(channelId);
  try {
    return await c.getEntity(id);
  } catch (err) {
    if (!retried) {
      try {
        await c.getDialogs({ limit: 200 });
      } catch (e2) {
        // Safe to ignore
      }
      return resolveEntity(id, { retried: true });
    }
    throw new SourceNotFoundError(
      `Wrong source channel: cannot resolve Telegram channel ${channelId} (${err.message}).`
    );
  }
}

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
  let messages;
  try {
    messages = await c.getMessages(entity, { ids: [messageIdNum] });
  } catch (err) {
    throw new SourceNotFoundError(
      `Could not verify message ${messageId} in channel ${channelId}: Telegram getMessages failed (${err.message}).`
    );
  }

  if (!messages || messages.length === 0) {
    throw new SourceNotFoundError(
      `Could not verify message ${messageId} exists in channel ${channelId}.`
    );
  }

  const message = messages[0];
  if (!message || message.className === 'MessageEmpty') {
    throw new SourceNotFoundError(
      `Source message not found (channelId=${channelId}, messageId=${messageId}) — it has been deleted.`
    );
  }
  if (!message.media || (!message.media.document && !message.video && !message.document)) {
    throw new SourceNotFoundError(
      `Message ${messageId} in channel ${channelId} exists but has no media attached.`
    );
  }

  const doc = message.media.document || message.document;
  if (!doc || !doc.size) {
    throw new SourceNotFoundError(`Message ${messageId} media has no readable document.`);
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
 * directly into an HTTP response.
 *
 * Guarantees (this is the contract routes/stream.js relies on):
 *   - Exactly the requested [start, end] range is written — never more,
 *     never less, never re-ordered, never duplicated. Every chunk is
 *     fetched strictly in order and only ever written once, so a corrupt
 *     partial write can't leave stale bytes ahead of the current offset.
 *   - Every chunk's length is validated against the remaining budget
 *     BEFORE it's written — a chunk is trimmed to fit, never allowed to
 *     overshoot `contentLength`.
 *   - `res.write()` is only ever called with the raw bytes GramJS
 *     returned (optionally sliced at the two ends of the range) — the
 *     binary content itself is never transformed, re-encoded, or touched.
 *   - On success, returns { bytesSent, contentLength, chunkCount } with
 *     bytesSent === contentLength always true — if that can't be
 *     guaranteed (Telegram ran out of bytes early, a chunk request
 *     failed after retries, DC migration failed, etc.) this throws
 *     instead of ever resolving with a short count, so the caller can
 *     destroy the connection rather than silently end() a truncated body.
 *   - A client disconnect (onAbort fires) stops the loop cleanly without
 *     throwing — there is nothing left to verify once the socket is gone.
 *
 * @param {{onAbort?:(fn:()=>void)=>void, videoId?:string}} opts
 */
async function streamRange(channelId, messageId, start, end, res, { onAbort, videoId } = {}) {
  await acquireSlot();
  let aborted = false;
  const abortHandler = () => { aborted = true; };
  if (onAbort) onAbort(abortHandler);

  const logCtx = { videoId, channelId, messageId };

  try {
    const c = await connect();
    if (!c) throw new MTProtoDisabledError();

    const { doc } = await resolveVideoSource(channelId, messageId);
    const bigInt = require('big-integer');
    const contentLength = end - start + 1;

    const alignedStart = start - (start % DEFAULT_CHUNK_SIZE);
    const skipBytes = start - alignedStart;
    const fileLocation = buildFileLocation(doc);

    let offset = bigInt(alignedStart);
    let bytesSent = 0;
    let isFirstChunk = true;
    let chunkIndex = 0;

    const telegramDcId = doc.dcId ?? null;
    let activeDcId = telegramDcId || currentDcId || c.session.dcId;

    log.info('streamRange', 'Stream started', {
      ...logCtx,
      telegramDc: telegramDcId, currentDc: currentDcId || c.session.dcId,
      requestedRange: `${start}-${end}`, startOffset: start, endOffset: end, expectedBytes: contentLength,
    });

    while (!aborted && bytesSent < contentLength) {
      chunkIndex += 1;
      const chunkOffset = offset.toString();

      let chunkRes;
      try {
        chunkRes = await requestChunkWithRetry(c, fileLocation, offset, DEFAULT_CHUNK_SIZE, {
          channelId, messageId, chunkIndex,
        }, activeDcId);
        activeDcId = chunkRes.finalDcId;
      } catch (err) {
        log.error('streamRange', 'GetFile chunk request failed after retries', err, {
          ...logCtx, chunkIndex, chunkOffset, totalBytesWritten: bytesSent, expectedBytes: contentLength, finalStatus: 'ERROR',
        });
        throw err;
      }

      if (aborted) break; // client vanished while we were awaiting the chunk — nothing left to write

      const result = chunkRes.result;
      if (result.className === 'upload.FileCdnRedirect') {
        throw new Error('TELEGRAM_CDN_REDIRECT_UNSUPPORTED');
      }

      const bytes = result.bytes;
      const rawChunkSize = bytes ? bytes.length : 0;
      if (!bytes || rawChunkSize === 0) {
        log.info('streamRange', 'Telegram returned an empty chunk — end of file reached', {
          ...logCtx, chunkIndex, chunkOffset, totalBytesWritten: bytesSent,
        });
        break;
      }

      // Trim exactly at the two ends of the requested range — this is the
      // ONLY slicing ever applied, and it never touches the bytes in
      // between, so the payload reaching res.write() is always a
      // contiguous, unmodified sub-slice of what Telegram returned.
      let piece = bytes;
      if (isFirstChunk) {
        isFirstChunk = false;
        if (skipBytes > 0) piece = piece.subarray(skipBytes);
      }
      const remaining = contentLength - bytesSent;
      if (piece.length > remaining) piece = piece.subarray(0, remaining);

      // Validate before writing: never write more than the remaining
      // budget, never write a negative/undefined-length slice.
      if (piece.length < 0 || piece.length > remaining) {
        throw new StreamIntegrityError(
          `Computed an invalid chunk length (${piece.length}) against remaining=${remaining} for ${channelId}:${messageId}`
        );
      }

      let chunkBytesWritten = 0;
      if (piece.length > 0 && !aborted && !res.destroyed && !res.writableEnded) {
        const ok = res.write(piece);
        chunkBytesWritten = piece.length;
        bytesSent += piece.length;
        if (!ok) {
          await new Promise((resolve) => res.once('drain', resolve));
        }
      }

      log.info('streamRange', 'Chunk written', {
        ...logCtx, chunkIndex, chunkOffset, chunkSize: rawChunkSize,
        bytesWritten: chunkBytesWritten, totalBytesWritten: bytesSent, expectedBytes: contentLength,
      });

      // Advance by the RAW bytes Telegram actually returned (not the
      // trimmed piece length) — this keeps every subsequent GetFile
      // offset exactly aligned with what Telegram has already given us,
      // which is what prevents skipped, duplicated, or re-ordered chunks.
      offset = offset.add(bytes.length);
      if (bytes.length < DEFAULT_CHUNK_SIZE) break; // short chunk = genuine EOF at this DC/tier
    }

    if (aborted) {
      log.info('streamRange', 'Aborted by client disconnect', {
        ...logCtx, totalBytesWritten: bytesSent, expectedBytes: contentLength, finalStatus: 'ABORTED',
      });
      return { bytesSent, contentLength, chunkCount: chunkIndex, aborted: true };
    }

    if (bytesSent !== contentLength) {
      const integrityErr = new StreamIntegrityError(
        `Streamed ${bytesSent} of ${contentLength} promised bytes for ${channelId}:${messageId} (range ${start}-${end})`
      );
      log.error('streamRange', 'Integrity check failed — byte count mismatch', integrityErr, {
        ...logCtx, totalBytesWritten: bytesSent, expectedBytes: contentLength, finalStatus: 'INTEGRITY_MISMATCH',
      });
      throw integrityErr;
    }

    log.success('streamRange', 'Stream completed — bytes sent match Content-Length exactly', {
      ...logCtx, totalBytesWritten: bytesSent, expectedBytes: contentLength, chunkCount: chunkIndex, finalStatus: 'SUCCESS',
    });
    return { bytesSent, contentLength, chunkCount: chunkIndex, aborted: false };
  } finally {
    releaseSlot();
  }
}

async function downloadToFile(channelId, messageId, destPath, { onProgress } = {}) {
  const startedAt = Date.now();
  const c = await connect();
  if (!c) throw new MTProtoDisabledError();

  const fs = require('fs');
  const bigInt = require('big-integer');

  const { doc, size } = await resolveVideoSource(channelId, messageId);
  const fileLocation = buildFileLocation(doc);
  const out = fs.createWriteStream(destPath);

  let offset = bigInt(0);
  let written = 0;
  let chunkIndex = 0;
  
  let activeDcId = doc.dcId || currentDcId || c.session.dcId;

  try {
    while (written < size) {
      chunkIndex += 1;
      
      const chunkRes = await requestChunkWithRetry(c, fileLocation, offset, DEFAULT_CHUNK_SIZE, {
        channelId, messageId, chunkIndex,
      }, activeDcId);
      
      activeDcId = chunkRes.finalDcId;
      const result = chunkRes.result;

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

  const elapsedMs = Date.now() - startedAt;
  log.success('downloadToFile', 'Download completed', {
    channelId, messageId, destPath, bytes: written, elapsedMs, finishedAt: new Date().toISOString(),
  });

  const fileNameAttr = (doc.attributes || []).find((a) => a.className === 'DocumentAttributeFilename');
  return { path: destPath, size: written, mimeType: doc.mimeType || 'video/mp4', fileName: fileNameAttr?.fileName || null };
}

function health() {
  return {
    configured: CONFIGURED,
    connected: Boolean(client && client.connected),
    homeDc: currentDcId,
    lastError: lastError?.message || null,
    activeStreams,
    queuedStreams: waitQueue.length,
    cachedSources: entityCache.size,
  };
}

async function shutdown() {
  try {
    if (client) await client.disconnect();
    for (const cachedClient of dcClients.values()) {
      if (cachedClient !== client) {
        await cachedClient.disconnect();
      }
    }
    log.info('shutdown', 'All pooled MTProto clients disconnected cleanly');
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
