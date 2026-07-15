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
        response.data.on('data', (chunk) => {
          written += chunk.length;
          if (opts.onProgress) {
            try { opts.onProgress(written, expectedSize); } catch (_) { /* never let a UI callback break the download */ }
          }
        });
        response.data.on('error', reject);
        out.on('error', reject);
        out.on('finish', resolve);
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
  log.info('uploadEpisode', 'Upload started', { targetChannelId, filePath });

  const client = await mtproto.connect();
  if (!client) throw new mtproto.MTProtoDisabledError();

  const entity = await mtproto.resolveEntity(targetChannelId);
  const { Api } = require('telegram');

  const attributes = [
    new Api.DocumentAttributeVideo({
      duration: opts.duration || 0,
      w: opts.width || 0,
      h: opts.height || 0,
      supportsStreaming: true,
    }),
  ];

  // GramJS reports upload progress as a 0..1 fraction — convert to a
  // plain 0-100 percent so callers (queue/pipeline.js) can bucket it into
  // the same 10%-step reporting used for download/compress.
  const sent = await client.sendFile(entity, {
    file: filePath,
    fileName: opts.fileName || path.basename(filePath),
    caption: opts.caption || '',
    forceDocument: true,
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

  const doc = sent?.media?.document;
  if (!doc) {
    throw new Error('Telegram accepted the upload but returned no document — cannot record it.');
  }

  const elapsedMs = Date.now() - startedAt;
  log.success('uploadEpisode', 'Upload completed', {
    targetChannelId, messageId: sent.id, size: Number(doc.size), elapsedMs, finishedAt: new Date().toISOString(),
  });

  return {
    channelId: targetChannelId,
    messageId: sent.id,
    documentId: doc.id.toString(),
    accessHash: doc.accessHash ? doc.accessHash.toString() : null,
    size: Number(doc.size),
    mimeType: doc.mimeType || 'video/mp4',
  };
}

module.exports = {
  downloadViaBotApi,
  downloadFromChannel,
  copyMessageToStorage,
  uploadEpisode,
  BOT_API_DOWNLOAD_LIMIT_BYTES,
};
