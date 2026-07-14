/**
 * services/telegramUpload.js
 * ----------------------------------------------------------------------
 * Transfer primitives for the admin upload pipeline (queue/pipeline.js).
 *
 * IMPORTANT — source videos uploaded directly to the bot are never looked
 * up by channelId+messageId for the *identity* of the file (that's what
 * `file_id`/`file_unique_id` are for — the Bot API hands those out for
 * every video regardless of where it came from, and Firestore/dedup logic
 * still keys off them). But downloading the actual bytes of a video that
 * lives in the bot's private chat with the admin now has TWO transports,
 * chosen purely by file size:
 *
 *   downloadViaBotApi(bot, fileId, destPath) — plain Bot API (bot.getFile
 *   + the /file/bot<token>/... URL). Telegram hard-caps this at 20MB
 *   regardless of how large a file the bot can *receive* — used for
 *   anything at or under that cap.
 *
 *   downloadViaMTProto(chatId, messageId, destPath, opts) — for anything
 *   over 20MB. Pulls the file directly out of the admin's private chat
 *   with the bot using the existing GramJS MTProto client
 *   (services/mtproto.js), which is logged in as this same bot via
 *   botAuthToken and is not subject to the Bot API's download cap. A
 *   private chat is a user peer rather than a channel, but
 *   mtproto.js's entity/message resolution (resolveEntity + getMessages)
 *   is peer-agnostic and works the same way for it — no forwarding to a
 *   channel is required.
 *
 * Two more operations round out the pipeline:
 *
 *   copyMessageToStorage(bot, fromChatId, fromMessageId, toChatId) — for
 *   videos forwarded from a channel the bot can see: asks Telegram to
 *   copy the message server-side directly into the storage channel, with
 *   no download/re-upload through this server at all.
 *
 *   uploadEpisode(targetChannelId, filePath, opts) — pushes a locally
 *   compressed file into the storage channel over the existing GramJS
 *   MTProto client (services/mtproto.js), which chunks large uploads
 *   automatically and isn't subject to the Bot API's ~50MB send limit.
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
 * Downloads a video the bot received (directly or as a fallback when a
 * channel copy isn't possible) using the plain Bot API — file_id based,
 * never channelId/messageId.
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
      // Caller (queue/pipeline.js#downloadSourceFile) catches this specific
      // message and transparently retries over MTProto — surfaced here in
      // case this function is ever called directly without that fallback.
      throw new Error(
        `File exceeds Telegram's Bot API 20MB download limit (file is too big). Use downloadViaMTProto instead.`
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
 * Downloads a video the bot received directly (in its private chat with
 * the admin) using the existing MTProto (GramJS) client instead of the
 * Bot API — for anything over the Bot API's 20MB download cap. No
 * forwarding to a channel is required: the client is logged in as this
 * same bot, so it can read the message straight out of that private chat
 * the same way it reads storage-channel messages elsewhere in this file.
 *
 * @param {number|string} chatId the private chat id the video was received in (the admin's chat)
 * @param {number} messageId the id of the message carrying the video, in that chat
 * @param {string} destPath
 * @param {{onProgress?:(written:number, total:number)=>void}} opts
 */
async function downloadViaMTProto(chatId, messageId, destPath, opts = {}) {
  if (!mtproto.isEnabled()) {
    throw new Error(
      'File exceeds the Bot API\'s 20MB download limit and MTProto is not configured ' +
      '(set TELEGRAM_API_ID / TELEGRAM_API_HASH / TELEGRAM_STRING_SESSION) — cannot download it.'
    );
  }
  if (!messageId) {
    throw new Error('downloadViaMTProto: no messageId available for this upload — cannot locate it via MTProto.');
  }

  log.info('downloadViaMTProto', 'Large-file download started via MTProto', { chatId, messageId, destPath });

  let result;
  try {
    result = await mtproto.downloadToFile(chatId, messageId, destPath, {
      onProgress: opts.onProgress,
    });
  } catch (err) {
    log.error('downloadViaMTProto', 'MTProto download failed', err, { chatId, messageId, stack: err.stack });
    throw new Error(`MTProto download failed: ${err.message}`);
  }

  if (!result || !result.size) {
    throw new Error('MTProto download returned no bytes.');
  }

  log.success('downloadViaMTProto', 'Download completed', { chatId, messageId, destPath, bytes: result.size });
  return { path: destPath, size: result.size };
}

/**
 * Attempts a server-side copy of a channel-forwarded video straight into
 * the storage channel, with no download at all. Throws if the bot can't
 * see the source chat/message (e.g. it isn't a member of that channel) —
 * callers should catch this and fall back to downloadViaBotApi.
 *
 * @param {import('node-telegram-bot-api')} bot
 * @param {number|string} fromChatId the original channel the video was forwarded from
 * @param {number} fromMessageId the original message id in that channel
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
  downloadViaMTProto,
  copyMessageToStorage,
  uploadEpisode,
  BOT_API_DOWNLOAD_LIMIT_BYTES,
};
