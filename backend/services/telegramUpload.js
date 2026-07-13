/**
 * services/telegramUpload.js
 * ----------------------------------------------------------------------
 * Big-file transfer primitives for the admin upload pipeline, built on
 * top of the existing GramJS MTProto client in services/mtproto.js
 * (same client already used for streaming). Two operations:
 *
 *   downloadEpisode(channelId, messageId, destPath) — pulls a source
 *   video fully to disk so FFmpeg can work on it.
 *
 *   uploadEpisode(targetChannelId, filePath, opts) — pushes the
 *   converted file to the storage channel. Uses GramJS's client.sendFile,
 *   which chunks large uploads automatically (upload.saveBigFilePart)
 *   and is not subject to the Bot API's 50MB send limit.
 * ----------------------------------------------------------------------
 */
'use strict';

const path = require('path');
const { makeLogger } = require('../utils/logger');
const log = makeLogger('services/telegramUpload.js');
const mtproto = require('./mtproto');

async function downloadEpisode(channelId, messageId, destPath, { onProgress } = {}) {
  return mtproto.downloadToFile(channelId, messageId, destPath, { onProgress });
}

/**
 * @param {string|number} targetChannelId storage channel to upload into
 * @param {string} filePath local path of the converted MP4
 * @param {{fileName?:string, caption?:string, duration?:number, width?:number, height?:number, onProgress?:(fraction:number)=>void}} opts
 * @returns {{channelId:*, messageId:number, documentId:string, accessHash:string, size:number, mimeType:string}}
 */
async function uploadEpisode(targetChannelId, filePath, opts = {}) {
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

  log.success('uploadEpisode', 'Episode uploaded to storage channel', {
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

module.exports = { downloadEpisode, uploadEpisode };
