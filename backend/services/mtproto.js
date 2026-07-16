/**
 * services/mtproto.js
 * ----------------------------------------------------------------------
 * Fixed MTProto stream assembler. Replaces brittle private APIs with a 
 * robust DC pool, automatically detects MKV/WebM/MP4 original containers, 
 * and actively protects against backpressure deadlocks.
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
const DEFAULT_CHUNK_SIZE = 512 * 1024;

const CHUNK_MAX_RETRIES = parseInt(process.env.MTPROTO_CHUNK_MAX_RETRIES || '5', 10);
const CHUNK_RETRY_BASE_DELAY_MS = 800;
const CHUNK_RETRY_MAX_DELAY_MS = 8000;

let client = null;
let connecting = null;
let lastError = null;
let currentDcId = null;

const entityCache = new Map();
const dcClients = new Map();
const dcClientCreationInFlight = new Map();

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
  constructor(msg) { super(msg); this.name = 'SourceNotFoundError'; }
}
class MTProtoDisabledError extends Error {
  constructor() {
    super('MTProto streaming is not configured.');
    this.name = 'MTProtoDisabledError';
  }
}
class StreamIntegrityError extends Error {
  constructor(msg) { super(msg); this.name = 'StreamIntegrityError'; }
}
class MTProtoValidationError extends Error {
  constructor(msg) { super(msg); this.name = 'MTProtoValidationError'; }
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

      try { await newClient.getDialogs({ limit: 200 }); } catch (err) {}

      client = newClient;
      lastError = null;
      currentDcId = newClient.session?.dcId ?? null;
      if (currentDcId) dcClients.set(currentDcId, client);

      log.success('connect', 'MTProto client connected', { homeDc: currentDcId });
      return client;
    } catch (err) {
      lastError = err;
      throw err;
    } finally {
      connecting = null;
    }
  })();

  return connecting;
}

async function initMTProto() {
  if (!CONFIGURED) return;
  try { await connect(); } catch {}
  setInterval(async () => {
    if (!CONFIGURED) return;
    if (client && client.connected) return;
    try { await connect(); } catch (err) {}
  }, RECONNECT_CHECK_INTERVAL_MS).unref?.();
}

function normalizeChannelId(channelId) {
  const n = Number(channelId);
  if (!Number.isInteger(n) || n === 0) throw new MTProtoValidationError(`Invalid channelId: ${channelId}`);
  return n;
}

function parseMigrateError(err) {
  const msg = (err && (err.errorMessage || err.message)) || '';
  const match = /^(FILE|NETWORK|USER|PHONE)_MIGRATE_(\d+)$/.exec(String(msg).trim());
  if (match) return { kind: match[1], dcId: parseInt(match[2], 10) };
  return null;
}

function parseFloodWait(err) {
  const msg = (err && (err.errorMessage || err.message)) || '';
  const match = /^FLOOD_WAIT_(\d+)$/.exec(String(msg).trim());
  if (!match) return null;
  return parseInt(match[1], 10);
}

// Fix: Robust multi-client DC pool to replace private internal APIs
async function getClientForDC(dcId) {
  if (!client) throw new Error("Primary client not started");
  if (dcId === currentDcId) return client;

  const cached = dcClients.get(dcId);
  if (cached && cached.connected) return cached;
  
  if (dcClientCreationInFlight.has(dcId)) return dcClientCreationInFlight.get(dcId);

  const creation = (async () => {
    const { TelegramClient } = require('telegram');
    const { StringSession } = require('telegram/sessions');
    const { Api } = require('telegram');

    const config = await client.invoke(new Api.help.GetConfig());
    const options = (config.dcOptions || []).filter((o) => o.id === dcId && !o.cdn);
    const best = options.find((o) => !o.ipv6 && !o.mediaOnly) || options.find((o) => !o.ipv6) || options[0];
    
    if (!best) throw new Error(`No DC address found for DC ${dcId}`);

    const newClient = new TelegramClient(new StringSession(""), API_ID, API_HASH, {
      connectionRetries: 5, retryDelay: 2000, autoReconnect: true, useWSS: false,
    });
    newClient.session.setDC(dcId, best.ipAddress, best.port);
    await newClient.connect();

    const exported = await client.invoke(new Api.auth.ExportAuthorization({ dcId }));
    await newClient.invoke(new Api.auth.ImportAuthorization({ id: exported.id, bytes: exported.bytes }));

    dcClients.set(dcId, newClient);
    return newClient;
  })().finally(() => dcClientCreationInFlight.delete(dcId));

  dcClientCreationInFlight.set(dcId, creation);
  return creation;
}

// Fix: Perform chunk request directly against the correct pooled client
async function requestChunkWithRetry(fileLocation, offset, limit, context, initialDcId) {
  const { Api } = require('telegram');
  let lastErr;
  let activeDcId = initialDcId;

  for (let attempt = 1; attempt <= CHUNK_MAX_RETRIES; attempt++) {
    try {
      const activeClient = await getClientForDC(activeDcId);
      const result = await withTimeout(
        activeClient.invoke(new Api.upload.GetFile({ location: fileLocation, offset, limit })),
        CHUNK_REQUEST_TIMEOUT_MS,
        `GetFile chunk offset=${offset.toString()}`
      );
      return { result, finalDcId: activeDcId };
    } catch (err) {
      lastErr = err;
      const migrate = parseMigrateError(err);
      if (migrate && (migrate.kind === 'FILE' || migrate.kind === 'NETWORK')) {
        activeDcId = migrate.dcId;
        continue;
      }
      
      const floodWaitSeconds = parseFloodWait(err);
      if (floodWaitSeconds) {
          await sleep(Math.min(floodWaitSeconds, 120) * 1000);
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
      try { await c.getDialogs({ limit: 200 }); } catch (e2) {}
      return resolveEntity(id, { retried: true });
    }
    throw new SourceNotFoundError(`Wrong source channel: cannot resolve Telegram channel ${channelId}`);
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
    throw new SourceNotFoundError(`Could not verify message ${messageId}: Telegram getMessages failed.`);
  }

  if (!messages || messages.length === 0) {
    throw new SourceNotFoundError(`Message ${messageId} returned no data.`);
  }

  const message = messages[0];
  if (!message || message.className === 'MessageEmpty') {
    throw new SourceNotFoundError(`Source message not found — it has been deleted.`);
  }
  if (!message.media || (!message.media.document && !message.video && !message.document)) {
    throw new SourceNotFoundError(`Message exists but has no media attached.`);
  }

  const doc = message.media.document || message.document;
  if (!doc || !doc.size) {
    throw new SourceNotFoundError(`Media has no readable document.`);
  }

  const fileNameAttr = (doc.attributes || []).find((a) => a.className === 'DocumentAttributeFilename');
  const fileName = fileNameAttr?.fileName || '';
  
  // Fix: Detect MIME type from Telegram media and filename strictly to preserve original container
  let mimeType = doc.mimeType;
  const ext = fileName.split('.').pop().toLowerCase();
  
  if (!mimeType || mimeType === 'application/octet-stream') {
      if (ext === 'mkv') mimeType = 'video/x-matroska';
      else if (ext === 'webm') mimeType = 'video/webm';
      else mimeType = 'video/mp4'; // Default safe fallback
  }

  const result = {
    message,
    doc,
    size: Number(doc.size),
    mimeType,
    fileName: fileName || null,
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

async function streamRange(channelId, messageId, start, end, res, options = {}) {
  await acquireSlot();
  let aborted = false;
  const abortHandler = () => { aborted = true; };
  if (options.onAbort) options.onAbort(abortHandler);

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
    let activeDcId = doc.dcId || currentDcId || c.session.dcId;

    while (!aborted && bytesSent < contentLength) {
      chunkIndex += 1;
      let chunkRes;
      
      try {
        chunkRes = await requestChunkWithRetry(fileLocation, offset, DEFAULT_CHUNK_SIZE, { channelId, messageId }, activeDcId);
        activeDcId = chunkRes.finalDcId;
      } catch (err) {
        throw err;
      }

      if (chunkRes.result.className === 'upload.FileCdnRedirect') throw new Error('TELEGRAM_CDN_REDIRECT_UNSUPPORTED');

      const bytes = chunkRes.result.bytes;
      if (!bytes || bytes.length === 0) break;

      let piece = bytes;
      if (isFirstChunk) {
        isFirstChunk = false;
        if (skipBytes > 0) piece = piece.subarray(skipBytes);
      }

      const remaining = contentLength - bytesSent;
      if (piece.length > remaining) piece = piece.subarray(0, remaining);

      if (piece.length > 0 && !aborted && !res.destroyed && !res.writableEnded) {
        
        // Fix: Requested Debug Logging implementation
        log.info('streamRange', 'Chunk written', {
          videoId: options.videoId || 'unknown',
          channelId,
          messageId,
          telegramDc: activeDcId,
          currentDc: currentDcId,
          requestedRange: options.rangeHeader || 'FULL',
          startOffset: start,
          endOffset: end,
          chunkIndex,
          chunkOffset: offset.toString(),
          chunkSize: bytes.length,
          bytesWritten: piece.length,
          totalBytesWritten: bytesSent + piece.length,
          expectedBytes: contentLength,
          finalStatus: (bytesSent + piece.length === contentLength) ? 'Completed' : 'Streaming'
        });

        const ok = res.write(piece);
        bytesSent += piece.length;
        
        // Fix: Backpressure deadlock prevention - Ensure the listener doesn't hang if socket drops
        if (!ok) {
          await new Promise((resolve, reject) => {
            const onDrain = () => { cleanup(); resolve(); };
            const onClose = () => { cleanup(); reject(new Error('Client disconnected during drain')); };
            const cleanup = () => {
              res.removeListener('drain', onDrain);
              res.removeListener('close', onClose);
              res.removeListener('error', onClose);
            };
            res.once('drain', onDrain);
            res.once('close', onClose);
            res.once('error', onClose);
          });
        }
      }

      offset = offset.add(bytes.length);
      if (bytes.length < DEFAULT_CHUNK_SIZE) break;
    }

    if (aborted) return;

    // Fix: Final Verification Check
    if (bytesSent !== contentLength) {
      log.error('streamRange', 'Mismatch: aborting response', { expected: contentLength, written: bytesSent });
      throw new StreamIntegrityError(`Streamed ${bytesSent} of ${contentLength} promised bytes for ${channelId}:${messageId}`);
    }
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
  let activeDcId = doc.dcId || currentDcId || c.session.dcId;

  let offset = bigInt(0);
  let written = 0;

  try {
    while (written < size) {
      const chunkRes = await requestChunkWithRetry(fileLocation, offset, DEFAULT_CHUNK_SIZE, { channelId, messageId }, activeDcId);
      activeDcId = chunkRes.finalDcId;
      
      if (chunkRes.result.className === 'upload.FileCdnRedirect') throw new Error('TELEGRAM_CDN_REDIRECT_UNSUPPORTED');
      const bytes = chunkRes.result.bytes;
      if (!bytes || bytes.length === 0) break;

      await new Promise((resolve, reject) => {
        out.write(bytes, (err) => (err ? reject(err) : resolve()));
      });

      written += bytes.length;
      offset = offset.add(bytes.length);
      if (onProgress) onProgress(written, size);
      if (bytes.length < DEFAULT_CHUNK_SIZE) break;
    }

    if (written !== size) throw new Error(`downloadToFile: wrote ${written} of ${size} bytes`);
  } finally {
    await new Promise((resolve) => out.end(resolve));
  }

  const elapsedMs = Date.now() - startedAt;
  log.success('downloadToFile', 'Download completed', {
    channelId, messageId, destPath, bytes: written, elapsedMs
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
        if (cachedClient !== client) await cachedClient.disconnect();
    }
    log.info('shutdown', 'MTProto clients disconnected cleanly');
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
