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
 *   copyMessageToStorage(bot, fromChatId, fromMessageId, toChatId) — for
 *   the same real-channel case above: asks Telegram to copy the message
 *   server-side directly into the storage channel, no download at all.
 *   Tried first; downloadFromChannel is the fallback if it isn't possible.
 *
 *   uploadEpisode(targetChannelId, filePath, opts) — pushes a locally
 *   compressed file into the storage channel (a real channel) over the
 *   MTProto client, which chunks large uploads automatically and isn't
 *   subject to the Bot API's ~50MB send limit.
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
      const out = fs.createWriteStream(destPath);
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
 * Attempts a server-side copy of a channel video straight into the
 * storage channel, with no download at all. `fromChatId` must be a real
 * channel id (see downloadFromChannel's contract above). Throws if the
 * bot can't see the source chat/message (e.g. it isn't a member of that
 * channel) — callers should catch this and fall back to
 * downloadFromChannel.
 *
 * @param {import('node-telegram-bot-api')} bot
 * @param {number|string} fromChatId the real source channel
 * @param {number} fromMessageId the message id in that channel
 * @param {number|string} toChatId the storage channel
 * @returns {{messageId:number}}
 */
async function copyMessageToStorage(bot, fromChatId, fromMessageId, toChatId) {
  if (!bot) throw new Error('copyMessageToStorage: no bot instance available.');

  log.info('copyMessageToStorage', 'Attempting server-side copy', { fromChatId, fromMessageId, toChatId });

  let result;
  try {
    result = await bot.copyMessage(toChatId, fromChatId, fromMessageId);
  } catch (err) {
    log.error('copyMessageToStorage', 'copyMessage failed', err, { fromChatId, fromMessageId, toChatId, stack: err.stack });
    throw new Error(`Telegram copyMessage failed: ${err.message}`);
  }

  const messageId = typeof result === 'object' && result !== null ? result.message_id : result;
  if (!messageId) {
    throw new Error('copyMessage returned no message_id.');
  }

  log.success('copyMessageToStorage', 'Copy completed', { fromChatId, fromMessageId, toChatId, newMessageId: messageId });
  return { messageId };
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
  log.info('uploadEpisode', 'Upload started', { targetChannelId, filePath, localSizeBytes });

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
    // THE BUG: this used to be `forceDocument: true`, which tells
    // Telegram to store the upload as a generic file attachment (the
    // blue download-icon "97.2 MB .mp4" seen in Telegram, no thumbnail,
    // no inline player) no matter how correctly the MP4 itself was
    // encoded — forceDocument overrides everything else, including the
    // DocumentAttributeVideo/supportsStreaming below. Explicitly false
    // so GramJS sends it as real video media: Telegram then generates
    // its own thumbnail/preview and enables in-app streaming playback.
    forceDocument: false,
    // Set both as a top-level convenience flag AND on the attribute
    // above — belt and suspenders against GramJS/Telegram version
    // differences in which one it actually reads.
    supportsStreaming: true,
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
  let attempt = 0;
  const maxAttempts = 2;
  let lastVerificationErr;

  while (attempt < maxAttempts) {
    attempt += 1;
    log.info('uploadEpisode', `Sending via MTProto sendFile (attempt ${attempt}/${maxAttempts})`, {
      targetChannelId, filePath, fileName, mimeTypeSent: 'video/mp4 (inferred from .mp4 extension)',
      attributesSent: attributes.map((a) => a.className),
      forceDocument: false, supportsStreaming: true, hasThumb: Boolean(opts.thumbPath),
    });

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

    // ---- Post-upload verification: does Telegram actually see this as a
    // playable VIDEO, or did it land as a generic document? ------------
    // `sent.video` is GramJS's own getter — it only returns the document
    // when the message's media is a document carrying a
    // DocumentAttributeVideo (and isn't a round/voice note). That's the
    // most direct signal available of what Telegram itself thinks this
    // upload is, so it's checked first; the attribute/mimeType checks
    // beneath it are a fallback for GramJS versions where that getter
    // might behave differently.
    const attributeClassNames = Array.isArray(doc.attributes) ? doc.attributes.map((a) => a.className) : [];
    const hasVideoAttribute = attributeClassNames.includes('DocumentAttributeVideo');
    const mimeType = doc.mimeType || '';
    const isVideoMime = mimeType.toLowerCase().startsWith('video/');
    const recognizedByGetter = Boolean(sent.video);
    const recognizedAsVideo = recognizedByGetter || (hasVideoAttribute && isVideoMime);
    const thumbCount = Array.isArray(doc.thumbs) ? doc.thumbs.length : 0;

    log.info('uploadEpisode', 'Post-upload verification', {
      targetChannelId, messageId: sent.id,
      mediaClassName: sent.media?.className || null,
      mimeType, attributeClassNames, hasVideoAttribute, isVideoMime,
      recognizedByVideoGetter: recognizedByGetter, thumbCount, recognizedAsVideo,
    });

    if (recognizedAsVideo) {
      log.success('uploadEpisode', 'Verified: Telegram recognizes this upload as a playable video', {
        targetChannelId, messageId: sent.id, thumbCount,
      });

      const remoteSizeBytes = Number(doc.size);
      const elapsedMs = Date.now() - startedAt;
      log.success('uploadEpisode', 'Upload completed', {
        targetChannelId, messageId: sent.id, localSizeBytes, remoteSizeBytes, elapsedMs, finishedAt: new Date().toISOString(),
      });

      if (Number.isFinite(remoteSizeBytes) && remoteSizeBytes !== localSizeBytes) {
        // Not thrown as a hard failure — GramJS already confirmed the
        // upload RPC succeeded — but a mismatch here means what Telegram
        // actually stored doesn't match what's on disk, worth surfacing
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
        // `fileReference` and `dcId` are saved to Firestore purely as
        // reference/debug metadata (per the requested schema) — NOT used
        // by the streaming path. fileReference expires (~1hr) and
        // mtproto.js's resolveVideoSource() always re-resolves a live one
        // by channelId+messageId at request time; a stored copy here
        // would just go stale. `dcId` is the DC that stores THIS
        // document's bytes (a real per-document field, independent of
        // the client's home DC) and could be used as a fast-path initial
        // guess to skip one FILE_MIGRATE round-trip, but is optional.
        fileReference: doc.fileReference ? Buffer.from(doc.fileReference).toString('base64') : null,
        dcId: typeof doc.dcId === 'number' ? doc.dcId : null,
        width: opts.width || 0,
        height: opts.height || 0,
        duration: opts.duration || 0,
        size: remoteSizeBytes,
        mimeType: mimeType || 'video/mp4',
      };
    }

    // Verification failed — this upload technically succeeded as an RPC
    // call but Telegram is treating it as a generic file, which is
    // exactly the "Use external video player" failure mode. Don't leave
    // a broken duplicate sitting in the storage channel: remove it before
    // retrying (or giving up) so a caller-level retry never accumulates
    // junk messages.
    lastVerificationErr = new Error(
      `Telegram stored this upload as a generic document, not a playable video ` +
      `(mimeType="${mimeType}", attributes=[${attributeClassNames.join(', ')}], messageId=${sent.id}).`
    );
    log.error('uploadEpisode', `Verification failed on attempt ${attempt}/${maxAttempts} — removing the bad message`, lastVerificationErr, {
      targetChannelId, messageId: sent.id,
    });
    try {
      await client.deleteMessages(entity, [sent.id], { revoke: true });
    } catch (delErr) {
      log.warn('uploadEpisode', 'Could not remove the non-video message after failed verification', { targetChannelId, messageId: sent.id, reason: delErr.message });
    }
  }

  throw lastVerificationErr || new Error('uploadEpisode: verification failed for an unknown reason.');
}

module.exports = {
  downloadViaBotApi,
  downloadFromChannel,
  copyMessageToStorage,
  uploadEpisode,
  BOT_API_DOWNLOAD_LIMIT_BYTES,
};
