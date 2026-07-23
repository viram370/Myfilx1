/**
 * services/telegramUpload.js
 * ----------------------------------------------------------------------
 * Transfer primitives for the admin upload pipeline (queue/pipeline.js).
 *
 * STRICT RULE (fixes "Source message not found (channelId=<userId>,
 * messageId=<n>)"): MTProto is ONLY ever used with a real Telegram
 * channel id. It is never given a private chat id, and a private chat id
 * is never passed off as a "channelId". Concretely:
 *
 *   downloadViaBotApi(bot, fileId, destPath) — the ONLY way a video
 *   uploaded DIRECTLY to the bot is downloaded. Plain Bot API
 *   (bot.getFile + the /file/bot<token>/... URL). Telegram hard-caps this
 *   at 20MB regardless of how large a file the bot can *receive* — there
 *   is no MTProto fallback for this case (a private bot chat is a user
 *   peer, not a channel, and MTProto's channel-message lookup does not
 *   apply to it — that mismatch is exactly what caused the bug above).
 *   A direct upload over 20MB fails validation before any download is
 *   attempted (see queue/pipeline.js's batch-lock validation step) with a
 *   clear message asking the admin to forward it from a channel instead.
 *
 *   downloadFromChannel(channelId, messageId, destPath, opts) — for
 *   videos whose bytes genuinely live in a real Telegram channel: either
 *   forwarded to the bot (Telegram's `forward_origin`/`forward_from_chat`
 *   gives us the real source channel id + message id) or posted straight
 *   into a channel the bot administers. Uses the existing GramJS MTProto
 *   client (services/mtproto.js), which is logged in as this bot via
 *   botAuthToken and is not subject to the Bot API's download cap.
 *
 *   uploadEpisode(targetChannelId, filePath, opts) — pushes the locally
 *   downloaded file into the storage channel (a real channel) over the
 *   MTProto client, which chunks large uploads automatically and isn't
 *   subject to the Bot API's ~50MB send limit.
 *
 * HARD RULE — no copyMessage/forwardMessage for videos, ever: this module
 * used to also expose copyMessageToStorage(), a server-side
 * bot.copyMessage() shortcut that skipped downloading entirely. It has
 * been removed. Every video, no exceptions, goes through the real
 * download -> verify -> upload -> delete-temp-file pipeline below and in
 * queue/pipeline.js — copyMessage/forwardMessage produced incorrect/
 * unusable results for this workflow and must never be reintroduced as
 * a "fast path".
 * ----------------------------------------------------------------------
 */
'use strict';

const fs = require('fs');
const path = require('path');
const axios = require('axios');
const { makeLogger } = require('../utils/logger');
const log = makeLogger('services/telegramUpload.js');
const mtproto = require('./mtproto');

const BOT_API_DOWNLOAD_LIMIT_BYTES = 20 * 1024 * 1024;
const BOT_API_DOWNLOAD_MAX_RETRIES = 3;
const BOT_API_DOWNLOAD_RETRY_BASE_DELAY_MS = 1500;
// GramJS chunks large uploads internally and can push several chunks in
// parallel instead of one at a time when told how many workers to use —
// this is the single biggest upload-speed lever available here, since
// GramJS already picks Telegram's largest allowed chunk size per file
// automatically. Kept modest by default to stay well clear of FLOOD_WAIT;
// override with MTPROTO_UPLOAD_WORKERS.
const UPLOAD_WORKERS = Math.max(1, parseInt(process.env.MTPROTO_UPLOAD_WORKERS || '4', 10));
// axios is given `timeout: 0` (no fixed request timeout) below because a
// large file can legitimately take a long time end-to-end — but that
// also means a connection that stalls (TCP stays open, no bytes ever
// arrive again) would hang forever with no error, which silently starves
// the retry loop below of any chance to run. This watchdog resets on
// every chunk of data received and only fires on genuine inactivity.
const BOT_API_STALL_TIMEOUT_MS = 30000;

function sleep(ms) { return new Promise((resolve) => setTimeout(resolve, ms)); }

/**
 * Downloads a video uploaded DIRECTLY to the bot. file_id based, never
 * channelId/messageId — a direct upload has no channel at all. Retries
 * transient network failures (timeouts, connection resets) automatically
 * — a "file too big" rejection from the Bot API is never retried since it
 * can never succeed.
 *
 * @param {import('node-telegram-bot-api')} bot
 * @param {string} fileId
 * @param {string} destPath
 * @param {{onProgress?:(written:number, total:?number)=>void}} [opts]
 */
async function downloadViaBotApi(bot, fileId, destPath, opts = {}) {
  if (!bot) throw new Error('downloadViaBotApi: no bot instance available.');

  const startedAt = Date.now();
  let lastErr;

  for (let attempt = 1; attempt <= BOT_API_DOWNLOAD_MAX_RETRIES; attempt++) {
    try {
      log.info('downloadViaBotApi', `Requesting file path from Bot API (attempt ${attempt}/${BOT_API_DOWNLOAD_MAX_RETRIES})`, { fileId });

      let file;
      try {
        file = await bot.getFile(fileId);
      } catch (err) {
        if (/file is too big/i.test(err.message || '')) {
          // Never retryable — fail fast with an actionable message.
          throw new Error(
            `File exceeds Telegram's Bot API 20MB download limit. Direct uploads have no other download path — ` +
            `forward this video from a channel the bot is a member of instead.`
          );
        }
        throw new Error(`Telegram getFile failed: ${err.message}`);
      }

      if (!file || !file.file_path) {
        throw new Error('Telegram returned no file_path — the file may exceed the Bot API\'s 20MB download limit.');
      }

      const token = process.env.TELEGRAM_BOT_TOKEN;
      if (!token) throw new Error('TELEGRAM_BOT_TOKEN is not set — cannot build the file download URL.');

      const url = `https://api.telegram.org/file/bot${token}/${file.file_path}`;
      const expectedSize = file.file_size || null;
      log.info('downloadViaBotApi', 'Download started', { fileId, destPath, expectedSize, attempt });

      const response = await axios.get(url, { responseType: 'stream', timeout: 0 });
      // Larger highWaterMark = fewer, bigger write() syscalls for the same
      // total bytes — meaningful at video file sizes, free otherwise.
      const out = fs.createWriteStream(destPath, { highWaterMark: 1024 * 1024 });
      let written = 0;

      await new Promise((resolve, reject) => {
        let settled = false;
        const finish = (fn, arg) => {
          if (settled) return;
          settled = true;
          clearTimeout(stallTimer);
          fn(arg);
        };

        let stallTimer = setTimeout(() => {
          const err = new Error(`Download stalled — no data received for ${BOT_API_STALL_TIMEOUT_MS}ms`);
          response.data.destroy(err);
          out.destroy();
          finish(reject, err);
        }, BOT_API_STALL_TIMEOUT_MS);
        const resetStallTimer = () => {
          clearTimeout(stallTimer);
          stallTimer = setTimeout(() => {
            const err = new Error(`Download stalled — no data received for ${BOT_API_STALL_TIMEOUT_MS}ms`);
            response.data.destroy(err);
            out.destroy();
            finish(reject, err);
          }, BOT_API_STALL_TIMEOUT_MS);
        };

        response.data.on('data', (chunk) => {
          written += chunk.length;
          resetStallTimer();
          if (opts.onProgress) {
            try { opts.onProgress(written, expectedSize); } catch (_) { /* never let a UI callback break the download */ }
          }
        });
        response.data.on('error', (err) => finish(reject, err));
        out.on('error', (err) => finish(reject, err));
        out.on('finish', () => finish(resolve));
        response.data.pipe(out);
      });

      if (written === 0) {
        throw new Error('Downloaded file is empty (0 bytes) — Telegram returned no content.');
      }

      const elapsedMs = Date.now() - startedAt;
      log.success('downloadViaBotApi', 'Download completed', {
        fileId, destPath, bytes: written, attempt, elapsedMs, finishedAt: new Date().toISOString(),
      });
      return { path: destPath, size: written };
    } catch (err) {
      lastErr = err;
      const nonRetryable = /20MB download limit/.test(err.message || '');
      if (nonRetryable || attempt === BOT_API_DOWNLOAD_MAX_RETRIES) {
        log.error('downloadViaBotApi', `Download failed${nonRetryable ? '' : ` after ${attempt} attempts`}`, err, { fileId, destPath });
        throw err;
      }
      const delay = BOT_API_DOWNLOAD_RETRY_BASE_DELAY_MS * attempt;
      log.warn('downloadViaBotApi', `Attempt ${attempt}/${BOT_API_DOWNLOAD_MAX_RETRIES} failed — retrying`, {
        fileId, reason: err.message, retryInMs: delay,
      });
      await sleep(delay);
    }
  }

  throw lastErr;
}

/**
 * Downloads a video that lives in a REAL Telegram channel — never a
 * private chat. `channelId` here must always come from Telegram's own
 * forward metadata (`forward_origin.chat.id` / `forward_from_chat.id`) or
 * from a channel_post's own `chat.id`; callers must never pass an admin's
 * private chat id here (see queue/pipeline.js — 'direct' source items
 * never call this function at all).
 *
 * @param {number|string} channelId
 * @param {number} messageId
 * @param {string} destPath
 * @param {{onProgress?:(written:number, total:number)=>void}} opts
 */
async function downloadFromChannel(channelId, messageId, destPath, opts = {}) {
  if (!mtproto.isEnabled()) {
    throw new Error(
      'MTProto is not configured (set TELEGRAM_API_ID / TELEGRAM_API_HASH / TELEGRAM_STRING_SESSION) — ' +
      'cannot download from the source channel.'
    );
  }
  if (!channelId || !messageId) {
    throw new Error(`downloadFromChannel: missing channelId/messageId (channelId=${channelId}, messageId=${messageId}).`);
  }

  const startedAt = Date.now();
  log.info('downloadFromChannel', 'Channel download started', { channelId, messageId, destPath });

  let result;
  try {
    result = await mtproto.downloadToFile(channelId, messageId, destPath, {
      onProgress: opts.onProgress,
    });
  } catch (err) {
    if (err instanceof mtproto.SourceNotFoundError) {
      // Preserve the specific reason (deleted / wrong channel / no media)
      // instead of collapsing everything into a generic wrapper message.
      log.error('downloadFromChannel', 'Source verification failed', err, { channelId, messageId, stack: err.stack });
      throw err;
    }
    log.error('downloadFromChannel', 'MTProto channel download failed', err, { channelId, messageId, stack: err.stack });
    throw new Error(`MTProto channel download failed: ${err.message}`);
  }

  if (!result || !result.size) {
    throw new Error('MTProto channel download returned no bytes.');
  }

  const elapsedMs = Date.now() - startedAt;
  log.success('downloadFromChannel', 'Download completed', {
    channelId, messageId, destPath, bytes: result.size, elapsedMs, finishedAt: new Date().toISOString(),
  });
  return { path: destPath, size: result.size };
}

/**
 * @param {string|number} targetChannelId storage channel to upload into
 * @param {string} filePath local path of the converted MP4
 * @param {{fileName?:string, caption?:string, duration?:number, width?:number, height?:number, onProgress?:(fraction:number)=>void}} opts
 * @returns {{channelId:*, messageId:number, documentId:string, accessHash:string, size:number, mimeType:string}}
 */
async function uploadEpisode(targetChannelId, filePath, opts = {}) {
  const startedAt = Date.now();
  let localSizeBytes = null;
  try {
    localSizeBytes = fs.statSync(filePath).size;
  } catch (err) {
    throw new Error(`uploadEpisode: cannot stat local file before upload (${err.message}) — refusing to upload a file that may not exist/be fully written.`);
  }
  log.info('uploadEpisode', 'Upload started', {
    targetChannelId, filePath, localSizeBytes,
    videoAttributesSent: { duration: opts.duration || 0, w: opts.width || 0, h: opts.height || 0, supportsStreaming: true, nosound: false, roundMessage: false },
    mimeType: 'video/mp4', forceDocument: false, hasThumb: !!opts.thumbPath,
  });

  const client = await mtproto.connect();
  if (!client) throw new mtproto.MTProtoDisabledError();

  const entity = await mtproto.resolveEntity(targetChannelId);
  const { Api } = require('telegram');

  const fileName = opts.fileName || path.basename(filePath);
  const attributes = [
    new Api.DocumentAttributeVideo({
      duration: opts.duration || 0,
      w: opts.width || 0,
      h: opts.height || 0,
      supportsStreaming: true,
      // THE LIKELY ROOT CAUSE of "tapping the video doesn't play it
      // correctly, sometimes only plays in a small PiP-style window
      // without audio" on Android: `nosound` is a real TL flag on
      // DocumentAttributeVideo (flags.5, "this video has no audio
      // track"). When it's true, EVERY Telegram client — Android
      // included — renders the video exactly like an animated GIF: it
      // auto-plays muted, often minimized/looping, with no audio
      // control at all. That is a precise match for the reported
      // symptom. This constructor call never set `nosound` at all
      // before, leaving it `undefined` — and depending on the exact
      // GramJS/gramjs-core version's TL-flag serialization, an
      // `undefined` optional boolean is not guaranteed to serialize
      // identically to an explicit `false` in the flags bitmask that
      // gets sent over the wire. Explicitly forcing `false` removes
      // that ambiguity entirely: every file this pipeline uploads has
      // already been verified (verifyOutputFile in compress.js) to
      // contain a real AAC audio stream, so `nosound` must always be
      // `false` here, never left to chance.
      nosound: false,
      // Also explicit for the same reason — round "video message"
      // bubbles (the circular player) are a completely different UI
      // from what's reported here, but leaving this undefined instead
      // of an explicit false is the same category of ambiguity as
      // `nosound` above, so it's pinned down too.
      roundMessage: false,
    }),
    // Explicit filename attribute alongside the video attribute — some
    // Telegram clients use this (rather than re-deriving it from the
    // upload's mimeType alone) when deciding how to render/play a
    // document, so it's worth setting directly instead of relying purely
    // on extension-based mimeType inference.
    new Api.DocumentAttributeFilename({ fileName }),
  ];

  // GramJS reports upload progress as a 0..1 fraction — convert to a
  // plain 0-100 percent so callers (queue/pipeline.js) can bucket it into
  // the same 10%-step reporting used for download/compress.
  const sendOnce = (thumbPath) => client.sendFile(entity, {
    file: filePath,
    fileName,
    caption: opts.caption || '',
    // Parallel chunk upload (see UPLOAD_WORKERS above) — GramJS still
    // picks the chunk size itself (its own size-based logic already
    // matches what Telegram's upload.saveFilePart/saveBigFilePart allow
    // per file size), this only controls how many of those chunks are
    // in flight to Telegram at once.
    workers: UPLOAD_WORKERS,
    // THE BUG: this used to be `forceDocument: true`, which tells
    // Telegram to store the upload as a generic file attachment (the
    // blue download-icon "97.2 MB .mp4" seen in Telegram, no thumbnail,
    // no inline player) no matter how correctly the MP4 itself was
    // encoded — forceDocument overrides everything else, including the
    // DocumentAttributeVideo/supportsStreaming below. Explicitly false
    // so GramJS sends it as real video media: Telegram then generates
    // its own thumbnail/preview and enables in-app streaming playback.
    forceDocument: false,
    // FIX: `supportsStreaming` is NOT a documented top-level option on
    // GramJS's sendFile() — the only place Telegram actually reads it
    // from is the DocumentAttributeVideo attribute above. This line
    // used to be here as a (non-functional) "belt and suspenders" —
    // GramJS silently ignores unknown option keys, so it did nothing at
    // all and just implied a safety net that didn't exist. Removed
    // rather than left in as false reassurance; explicitly setting
    // `mimeType` below is the actual belt-and-suspenders that matters.
    //
    // Explicit mimeType instead of relying purely on GramJS deriving it
    // from the `fileName` extension via its internal mime-type lookup.
    // Every file this pipeline ever hands to sendFile is guaranteed
    // (verifyOutputFile) to be H.264/AAC in an MP4 container, so this is
    // always exactly correct — and removes any dependency on filename
    // extension parsing succeeding for every possible generated name.
    mimeType: 'video/mp4',
    // A real thumbnail (see services/compress.js#generateThumbnail)
    // removes any dependency on Telegram's own server-side auto-
    // thumbnail generation, which isn't fully reliable and can silently
    // leave a video-tagged upload looking like a plain attachment with
    // no preview. Entirely optional — omitted if generation failed.
    thumb: thumbPath || undefined,
    attributes,
    progressCallback: opts.onProgress
      ? (fraction) => {
          try {
            const percent = Math.max(0, Math.min(100, Math.round(fraction * 100)));
            opts.onProgress(percent);
          } catch (_) { /* never let a UI callback break the upload */ }
        }
      : undefined,
  });

  let sent;
  try {
    sent = await sendOnce(opts.thumbPath);
  } catch (err) {
    if (!opts.thumbPath) throw err;
    log.warn('uploadEpisode', 'Upload with thumbnail failed — retrying once without it', { targetChannelId, filePath, reason: err.message });
    sent = await sendOnce(null);
  }

  const doc = sent?.media?.document;
  if (!doc) {
    throw new Error('Telegram accepted the upload but returned no document — cannot record it.');
  }

  // FINAL GATE: a successful RPC only means Telegram accepted the bytes —
  // it does NOT mean Telegram stored them as a *playable video*. Both a
  // real inline-streamable video and a plain "97.2 MB file.mp4" generic
  // attachment come back as the exact same `MessageMediaDocument` shape;
  // the only reliable signal that Telegram's backend actually recognized
  // and processed this as video media is the returned document's own
  // `attributes` array containing a `DocumentAttributeVideo` entry (this
  // is Telegram's own confirmation, not just an echo of what we sent —
  // if the file didn't qualify as valid video media server-side, this
  // attribute comes back missing even though `forceDocument` was false
  // and the RPC "succeeded"). Treating that case as success is exactly
  // how a broken/unplayable upload used to silently reach Firestore.
  const sentAttributes = doc.attributes || [];
  const videoAttr = sentAttributes.find((a) => a.className === 'DocumentAttributeVideo');
  if (!videoAttr) {
    log.error(
      'uploadEpisode',
      'Telegram stored this upload as a plain document, not a video — refusing to treat it as a successful streamable upload',
      new Error(`attributes=[${sentAttributes.map((a) => a.className).join(', ') || 'none'}]`),
      { targetChannelId, filePath, messageId: sent.id, mimeType: doc.mimeType }
    );
    // Never leave a failed/misrecognized upload sitting in the storage
    // channel as orphaned junk while the caller retries — delete it
    // immediately. Best-effort: if the delete itself fails, the retry
    // still proceeds (the caller's next attempt sends a brand-new
    // message either way), but we log it loudly since it means a stray
    // document is now sitting in the channel that a human may need to
    // clean up manually.
    try {
      await client.deleteMessages(entity, [sent.id], { revoke: true });
      log.warn('uploadEpisode', 'Deleted the failed upload from the storage channel', { targetChannelId, messageId: sent.id });
    } catch (delErr) {
      log.error('uploadEpisode', 'Failed to delete the bad upload — a stray non-video document may remain in the storage channel', delErr, {
        targetChannelId, messageId: sent.id, stack: delErr.stack,
      });
    }
    throw new Error(
      'Telegram did not recognize the uploaded file as playable video media (no DocumentAttributeVideo on the ' +
      'stored document) — it would show up as a plain file attachment with no inline player. Not saving this as a successful upload.'
    );
  }
  if (!videoAttr.supportsStreaming) {
    log.warn('uploadEpisode', 'Uploaded video is missing supportsStreaming on the stored document — playback may require a full download before it starts', {
      targetChannelId, filePath, messageId: sent.id,
    });
  }
  if (videoAttr.nosound) {
    // Telegram's own confirmation that it will render this as a silent,
    // GIF-style auto-playing clip instead of a normal video with
    // controls and audio — exactly the reported Android symptom. The
    // local file is already guaranteed (verifyOutputFile in compress.js)
    // to contain a real AAC stream, and this call explicitly sent
    // `nosound: false` — if Telegram's server-side analysis still came
    // back with nosound=true, something is genuinely wrong with this
    // specific upload (not just a logging concern), so it's treated the
    // same as the missing-DocumentAttributeVideo case: delete it and let
    // the caller's retry loop try again with a fresh upload.
    log.error('uploadEpisode', 'Telegram stored this video with nosound=true — it will play muted/minimized like a GIF, not as a normal video', new Error('nosound flag set on stored document'), {
      targetChannelId, filePath, messageId: sent.id,
    });
    try {
      await client.deleteMessages(entity, [sent.id], { revoke: true });
      log.warn('uploadEpisode', 'Deleted the nosound=true upload from the storage channel', { targetChannelId, messageId: sent.id });
    } catch (delErr) {
      log.error('uploadEpisode', 'Failed to delete the nosound=true upload — a stray silent-playback document may remain in the storage channel', delErr, {
        targetChannelId, messageId: sent.id, stack: delErr.stack,
      });
    }
    throw new Error('Telegram stored this upload with nosound=true (silent/GIF-style playback) despite a verified AAC audio track locally — not saving this as a successful upload.');
  }
  if (!/^video\//i.test(doc.mimeType || '')) {
    log.warn('uploadEpisode', 'Stored document has a non-video mimeType despite carrying DocumentAttributeVideo', {
      targetChannelId, filePath, messageId: sent.id, mimeType: doc.mimeType,
    });
  }

  const remoteSizeBytes = Number(doc.size);
  const elapsedMs = Date.now() - startedAt;
  log.success('uploadEpisode', 'Upload completed and verified as playable video', {
    targetChannelId, messageId: sent.id, localSizeBytes, remoteSizeBytes, elapsedMs, finishedAt: new Date().toISOString(),
    verifiedDuration: videoAttr.duration, verifiedWidth: videoAttr.w, verifiedHeight: videoAttr.h,
    supportsStreaming: !!videoAttr.supportsStreaming, nosound: !!videoAttr.nosound,
  });

  if (Number.isFinite(remoteSizeBytes) && remoteSizeBytes !== localSizeBytes) {
    // Not thrown as a hard failure — GramJS already confirmed the upload
    // RPC succeeded — but a mismatch here means what Telegram actually
    // stored doesn't match what's on disk, which is worth surfacing
    // loudly rather than silently trusting a "success".
    log.error(
      'uploadEpisode',
      'Uploaded document size does not match the local file size — investigate before trusting this upload',
      new Error(`local=${localSizeBytes} remote=${remoteSizeBytes}`),
      { targetChannelId, filePath }
    );
  }

  return {
    channelId: targetChannelId,
    messageId: sent.id,
    documentId: doc.id.toString(),
    accessHash: doc.accessHash ? doc.accessHash.toString() : null,
    size: remoteSizeBytes,
    mimeType: doc.mimeType || 'video/mp4',
  };
}

module.exports = {
  downloadViaBotApi,
  downloadFromChannel,
  uploadEpisode,
  BOT_API_DOWNLOAD_LIMIT_BYTES,
};
