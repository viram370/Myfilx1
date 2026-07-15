/**
 * services/mtproto.js
 * ----------------------------------------------------------------------
 * True MTProto client (GramJS), used for:
 *   1. Streaming large video files out of the storage channel for playback
 *      (streamRange, below).
 *   2. Downloading source videos for the admin upload pipeline that live
 *      in a REAL Telegram channel (downloadToFile, below, via
 *      services/telegramUpload.js#downloadFromChannel) — forwarded
 *      videos and videos posted straight into a channel the bot admins.
 * The classic Telegram Bot API (`getFile`) caps downloads at 20MB, which
 * is why the old streaming implementation broke on anything bigger than a
 * short clip. GramJS talks MTProto directly (the same protocol Telegram
 * apps use) and can pull arbitrary byte ranges — or a full file — out of
 * a document without that cap.
 *
 * IMPORTANT — every function below that takes a `channelId` requires a
 * REAL Telegram channel id. None of them work on a private chat id (e.g.
 * an admin's own user id): a private chat is a different kind of MTProto
 * peer ("user"), and resolveEntity()/resolveVideoSource() only know how
 * to resolve channels. Treating a private chat id as a channelId is
 * exactly what produced "Source message not found (channelId=<userId>,
 * messageId=<n>)" in an earlier version of the upload pipeline. Videos
 * uploaded directly to the bot are downloaded via the plain Bot API
 * instead (services/telegramUpload.js#downloadViaBotApi) — this file is
 * never involved in that path.
 *
 * DC MIGRATION (FILE_MIGRATE_X / NETWORK_MIGRATE_X / USER_MIGRATE_X /
 * PHONE_MIGRATE_X) is handled transparently for every manual RPC this
 * file makes — see invokeWithMigration() below. A download or stream
 * never fails just because the requested file happens to live on a
 * different Telegram data center than the one the bot's main connection
 * is on.
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

// ---- DC migration / retry tuning -----------------------------------
const DC_MIGRATE_MAX_HOPS = 3;
const CHUNK_MAX_RETRIES = parseInt(process.env.MTPROTO_CHUNK_MAX_RETRIES || '5', 10);
const CHUNK_RETRY_BASE_DELAY_MS = 800;
const CHUNK_RETRY_MAX_DELAY_MS = 8000;

let client = null;
let connecting = null;
let lastError = null;
/** DC id the primary connection is currently authorized against, once known. */
let currentDcId = null;
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

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
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
      try {
        currentDcId = newClient.session?.dcId ?? null;
      } catch (_) {
        currentDcId = null;
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

/**
 * ---------------------------------------------------------------------
 * DC MIGRATION HANDLING
 * ---------------------------------------------------------------------
 * Telegram tells clients that a piece of data (a file, or in rarer cases
 * the whole account's home DC) actually lives on a different data center
 * by failing the RPC with one of:
 *   FILE_MIGRATE_X    - this specific file lives on DC X
 *   NETWORK_MIGRATE_X - this connection should move to DC X
 *   USER_MIGRATE_X    - this bot/user account's home DC is actually X
 *   PHONE_MIGRATE_X   - (login-time) this phone number's home DC is X
 * A raw client.invoke() call does NOT auto-follow these the way
 * high-level helpers like sendFile/downloadMedia do internally, so every
 * manual upload.GetFile RPC in this file is routed through
 * invokeWithMigration() below, which:
 *   1. Detects the *_MIGRATE_X error by parsing the RPC error message.
 *   2. For FILE_MIGRATE/NETWORK_MIGRATE (only this request needs a
 *      different DC — the session itself is fine): borrows an authorized
 *      sender for that DC via GramJS's exported-sender pool. Internally
 *      that performs auth.exportAuthorization on the current (home) DC
 *      and auth.importAuthorization on the target DC — the "export and
 *      import authorization" step this file needs — then the exact same
 *      request is re-invoked against that sender.
 *   3. For USER_MIGRATE/PHONE_MIGRATE (the whole session's home DC
 *      changed): migrates the client's primary connection to the new DC
 *      and re-invokes there.
 *   4. Retries up to DC_MIGRATE_MAX_HOPS times (Telegram can, rarely,
 *      redirect more than once), then gives up with a clear error
 *      instead of ever surfacing a raw "FILE_MIGRATE_2" to a caller.
 * Every hop is logged with the current DC, the target DC, and the reason,
 * satisfying the "current DC / target DC" logging requirement.
 * ---------------------------------------------------------------------
 */
function parseMigrateError(err) {
  const msg = (err && (err.errorMessage || err.message)) || '';
  const match = /^(FILE|NETWORK|USER|PHONE)_MIGRATE_(\d+)$/.exec(String(msg).trim());
  if (!match) return null;
  return { kind: match[1], dcId: parseInt(match[2], 10) };
}

function parseFloodWait(err) {
  const msg = (err && (err.errorMessage || err.message)) || '';
  const match = /^FLOOD_WAIT_(\d+)$/.exec(String(msg).trim());
  if (!match) return null;
  return parseInt(match[1], 10);
}

async function borrowDcSender(c, dcId) {
  if (typeof c._borrowExportedSender === 'function') {
    try {
      return await c._borrowExportedSender(dcId);
    } catch (err) {
      log.warn('borrowDcSender', 'Failed to borrow exported sender for DC — will fall back to switching the primary connection', {
        dcId, reason: err.message,
      });
      return null;
    }
  }
  return null; // signals "no per-request sender support"; caller falls back to switching the whole client's DC
}

async function releaseDcSender(c, dcId, sender) {
  if (!sender) return;
  try {
    if (typeof c._unborrowExportedSender === 'function') {
      await c._unborrowExportedSender(dcId, sender);
    } else if (typeof sender.disconnect === 'function') {
      await sender.disconnect();
    }
  } catch (err) {
    log.warn('releaseDcSender', 'Failed to release exported sender (non-fatal)', { dcId, reason: err.message });
  }
}

async function switchPrimaryDc(c, dcId) {
  if (typeof c._switchDC === 'function') {
    await c._switchDC(dcId);
    currentDcId = dcId;
    return true;
  }
  return false;
}

/**
 * Invokes `request` against `c`, transparently following any DC
 * migration Telegram asks for, and transparently sleeping through
 * FLOOD_WAIT_X responses above the client's floodSleepThreshold (GramJS
 * already sleeps through short ones itself). `label`/`context` are only
 * used for logging.
 */
async function invokeWithMigration(c, request, { label = 'invoke', context = {} } = {}) {
  let sender = null;
  let borrowedDcId = null;

  try {
    for (let hop = 0; hop <= DC_MIGRATE_MAX_HOPS; hop++) {
      try {
        return sender ? await c.invoke(request, sender) : await c.invoke(request);
      } catch (err) {
        const migrate = parseMigrateError(err);
        const floodWaitSeconds = !migrate ? parseFloodWait(err) : null;

        if (floodWaitSeconds) {
          log.warn('invokeWithMigration', `${label}: Telegram asked us to wait (FLOOD_WAIT)`, {
            ...context, waitSeconds: floodWaitSeconds,
          });
          await sleep(Math.min(floodWaitSeconds, 120) * 1000);
          continue;
        }

        if (!migrate || hop === DC_MIGRATE_MAX_HOPS) throw err;

        log.warn('invokeWithMigration', `${label}: Telegram requested a DC migration`, {
          ...context,
          migrateType: migrate.kind,
          currentDc: currentDcId ?? 'unknown',
          targetDc: migrate.dcId,
          hop: hop + 1,
        });

        if (sender) {
          await releaseDcSender(c, borrowedDcId, sender);
          sender = null;
          borrowedDcId = null;
        }

        if (migrate.kind === 'USER' || migrate.kind === 'PHONE') {
          const switched = await switchPrimaryDc(c, migrate.dcId);
          if (!switched) {
            throw new Error(
              `Telegram reported ${migrate.kind}_MIGRATE_${migrate.dcId} but this GramJS version does not ` +
              `expose a DC-switch primitive — cannot follow the migration.`
            );
          }
          log.success('invokeWithMigration', `${label}: primary connection switched to DC ${migrate.dcId}`, context);
          continue;
        }

        // FILE_MIGRATE / NETWORK_MIGRATE — only this request needs a
        // different DC; borrow (export/import authorization under the
        // hood) an authorized sender for it instead of moving the whole
        // client's primary connection.
        const borrowed = await borrowDcSender(c, migrate.dcId);
        if (!borrowed) {
          const switched = await switchPrimaryDc(c, migrate.dcId);
          if (!switched) {
            throw new Error(
              `Telegram reported ${migrate.kind}_MIGRATE_${migrate.dcId} but this GramJS version exposes ` +
              `neither an exported-sender pool nor a DC-switch primitive — cannot follow the migration.`
            );
          }
          log.success('invokeWithMigration', `${label}: no exported-sender support — switched primary connection to DC ${migrate.dcId} instead`, context);
          continue;
        }
        sender = borrowed;
        borrowedDcId = migrate.dcId;
        log.success('invokeWithMigration', `${label}: borrowed authorized sender for DC ${migrate.dcId} (exported/imported authorization)`, context);
      }
    }
  } finally {
    if (sender) await releaseDcSender(c, borrowedDcId, sender);
  }
  // Unreachable in practice (the loop always returns or throws), but keeps
  // control-flow analyzers happy.
  throw new Error(`${label}: exhausted DC migration hops without success.`);
}

/**
 * Requests one upload.GetFile chunk, transparently handling DC migration
 * (invokeWithMigration) AND generic transient failures (timeouts, socket
 * resets, temporary Telegram-side hiccups) with capped exponential
 * backoff — so a single flaky chunk retries in place instead of failing
 * (or restarting) the whole download.
 */
async function requestChunkWithRetry(c, fileLocation, offset, limit, context) {
  const { Api } = require('telegram');
  let lastErr;

  for (let attempt = 1; attempt <= CHUNK_MAX_RETRIES; attempt++) {
    try {
      const result = await withTimeout(
        invokeWithMigration(
          c,
          new Api.upload.GetFile({ location: fileLocation, offset, limit }),
          { label: 'GetFile', context: { ...context, offset: offset.toString(), attempt } }
        ),
        CHUNK_REQUEST_TIMEOUT_MS,
        `GetFile offset=${offset.toString()}`
      );
      if (attempt > 1) {
        log.success('requestChunkWithRetry', 'Chunk succeeded after retry', { ...context, offset: offset.toString(), attempt });
      }
      return result;
    } catch (err) {
      lastErr = err;
      if (attempt === CHUNK_MAX_RETRIES) break;
      const delay = Math.min(CHUNK_RETRY_BASE_DELAY_MS * 2 ** (attempt - 1), CHUNK_RETRY_MAX_DELAY_MS);
      log.warn('requestChunkWithRetry', `Chunk request failed — retrying (attempt ${attempt}/${CHUNK_MAX_RETRIES})`, {
        ...context, offset: offset.toString(), reason: err.message, retryInMs: delay,
      });
      await sleep(delay);
    }
  }

  log.error('requestChunkWithRetry', `Chunk request failed after ${CHUNK_MAX_RETRIES} attempts`, lastErr, {
    ...context, offset: offset.toString(),
  });
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
      log.warn('resolveEntity', 'Entity not cached — refreshing dialogs and retrying once', { channelId: id });
      try {
        await c.getDialogs({ limit: 200 });
      } catch (e2) {
        log.warn('resolveEntity', 'Dialog refresh failed', { reason: e2.message });
      }
      return resolveEntity(id, { retried: true });
    }
    // This is the "wrong source channel" case — the channel/message pair
    // itself may be perfectly valid, but the bot cannot see this chat at
    // all (wrong id, not a member, kicked, etc). Deliberately NOT worded
    // as "message not found" — that phrase is reserved for a genuinely
    // deleted message inside a channel we CAN see (resolveVideoSource,
    // below).
    throw new SourceNotFoundError(
      `Wrong source channel: cannot resolve Telegram channel ${channelId} (${err.message}). Ensure the bot ` +
      `account is a member/admin of this channel and that the channel id is correct. (This function only ever ` +
      `resolves real channels — if you're seeing this for what should be a direct upload to the bot, that's a ` +
      `bug: direct uploads must use the Bot API, not MTProto.)`
    );
  }
}

/**
 * Fetches message + document metadata for (channelId, messageId), with a
 * short TTL cache so repeated Range requests during one viewing session
 * don't re-hit Telegram's getMessages for every seek.
 *
 * Verifies the source message BEFORE any byte is downloaded, and takes
 * care to only ever report "deleted" when Telegram genuinely says so
 * (an explicit MessageEmpty). Every other failure mode — wrong channel,
 * Telegram returning nothing at all for the id, a message with no video
 * attached — gets its own distinct, actionable reason instead of being
 * lumped into "Source message not found".
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

  // Step 1: resolve the channel itself. If this fails, the problem is the
  // CHANNEL (wrong id / bot not a member), not the message — resolveEntity
  // already reports that distinctly.
  const entity = await resolveEntity(channelId);

  // Step 2: verify the message exists in that channel before attempting
  // any download.
  let messages;
  try {
    messages = await c.getMessages(entity, { ids: [messageIdNum] });
  } catch (err) {
    throw new SourceNotFoundError(
      `Could not verify message ${messageId} in channel ${channelId}: Telegram getMessages failed (${err.message}).`
    );
  }

  if (!messages || messages.length === 0) {
    // Telegram returned nothing at all for this id — distinct from an
    // explicit MessageEmpty placeholder (which really does mean deleted).
    throw new SourceNotFoundError(
      `Could not verify message ${messageId} exists in channel ${channelId} — Telegram returned no data for ` +
      `this id. Double-check the message id and that it belongs to this channel.`
    );
  }

  const message = messages[0];
  if (!message || message.className === 'MessageEmpty') {
    // The only case genuinely worded as "not found / deleted" — Telegram
    // explicitly told us there's nothing at this id anymore.
    throw new SourceNotFoundError(
      `Source message not found (channelId=${channelId}, messageId=${messageId}) — it has been deleted.`
    );
  }
  if (!message.media || (!message.media.document && !message.video && !message.document)) {
    throw new SourceNotFoundError(
      `Message ${messageId} in channel ${channelId} exists but has no video/document media attached ` +
      `(wrong message id, or the media was stripped/edited out).`
    );
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
 * timeout, automatic DC-migration handling, and retry-with-backoff, so a
 * stuck/failed RPC recovers in place instead of hanging or aborting the
 * whole stream.
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
        result = await requestChunkWithRetry(c, fileLocation, offset, DEFAULT_CHUNK_SIZE, {
          channelId, messageId, chunkIndex,
        });
      } catch (err) {
        log.error('streamRange', 'GetFile chunk request failed after retries', err, {
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
 * upload pipeline (services/telegramUpload.js#downloadFromChannel) to
 * pull a source video onto disk before handing it to FFmpeg — this is a
 * plain sequential download (not range-based like streamRange), so it
 * does not share the streaming semaphore with live playback requests.
 * `channelId` must be a REAL Telegram channel id — never a private chat
 * id (see the file header for why). Every chunk goes through
 * requestChunkWithRetry(), so a DC migration or a transient network blip
 * is handled in place — the download only fails after Telegram/network
 * problems persist past CHUNK_MAX_RETRIES attempts on the same chunk.
 */
async function downloadToFile(channelId, messageId, destPath, { onProgress } = {}) {
  const startedAt = Date.now();
  const c = await connect();
  if (!c) throw new MTProtoDisabledError();

  const fs = require('fs');
  const bigInt = require('big-integer');

  const { doc, size } = await resolveVideoSource(channelId, messageId);
  const fileLocation = buildFileLocation(doc);
  const out = fs.createWriteStream(destPath);

  log.info('downloadToFile', 'Download starting', {
    channelId, messageId, destPath, sizeBytes: size, homeDc: currentDcId,
  });

  let offset = bigInt(0);
  let written = 0;
  let chunkIndex = 0;

  try {
    while (written < size) {
      chunkIndex += 1;
      const result = await requestChunkWithRetry(c, fileLocation, offset, DEFAULT_CHUNK_SIZE, {
        channelId, messageId, chunkIndex,
      });

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
