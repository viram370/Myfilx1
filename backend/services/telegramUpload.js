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

/**
 * Downloads a video uploaded DIRECTLY to the bot. file_id based, never
 * channelId/messageId — a direct upload has no channel at all.
 *
 * @param {import('node-telegram-bot-api')} bot
 * @param {string} fileId
 * @param {string} destPath
 */
async function downloadViaBotApi(bot, fileId, destPath) {
  if (!bot) throw new Error('downloadViaBotApi: no bot instance available.');

  log.info('downloadViaBotApi', 'Requesting file path from Bot API', { fileId });

  let file;
  try {
    file = await bot.getFile(fileId);
  } catch (err) {
    log.error('downloadViaBotApi', 'bot.getFile failed', err, { fileId, stack: err.stack });
    if (/file is too big/i.test(err.message || '')) {
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
  log.info('downloadViaBotApi', 'Download started', { fileId, destPath, expectedSize: file.file_size || null });

  const response = await axios.get(url, { responseType: 'stream', timeout: 0 });
  const out = fs.createWriteStream(destPath);
  let written = 0;

  await new Promise((resolve, reject) => {
    response.data.on('data', (chunk) => { written += chunk.length; });
    response.data.on('error', reject);
    out.on('error', reject);
    out.on('finish', resolve);
    response.data.pipe(out);
  });

  if (written === 0) {
    throw new Error('Downloaded file is empty (0 bytes) — Telegram returned no content.');
  }

  log.success('downloadViaBotApi', 'Download completed', { fileId, destPath, bytes: written });
  return { path: destPath, size: written };
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

  log.info('downloadFromChannel', 'Channel download started', { channelId, messageId, destPath });

  let result;
  try {
    result = await mtproto.downloadToFile(channelId, messageId, destPath, {
      onProgress: opts.onProgress,
    });
  } catch (err) {
    log.error('downloadFromChannel', 'MTProto channel download failed', err, { channelId, messageId, stack: err.stack });
    throw new Error(`MTProto channel download failed: ${err.message}`);
  }

  if (!result || !result.size) {
    throw new Error('MTProto channel download returned no bytes.');
  }

  log.success('downloadFromChannel', 'Download completed', { channelId, messageId, destPath, bytes: result.size });
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

  const sent = await client.sendFile(entity, {
    file: filePath,
    fileName: opts.fileName || path.basename(filePath),
    caption: opts.caption || '',
    forceDocument: true,
    attributes,
    progressCallback: opts.onProgress
      ? (progress) => {
          try { opts.onProgress(progress); } catch (_) { /* never let a UI callback break the upload */ }
        }
      : undefined,
  });

  const doc = sent?.media?.document;
  if (!doc) {
    throw new Error('Telegram accepted the upload but returned no document — cannot record it.');
  }

  log.success('uploadEpisode', 'Upload completed', {
    targetChannelId, messageId: sent.id, size: Number(doc.size),
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
