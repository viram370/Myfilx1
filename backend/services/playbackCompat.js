/**
 * services/playbackCompat.js
 * ----------------------------------------------------------------------
 * Fixes the "audio plays, video is black" class of bugs at the source:
 * that symptom means the browser decoded the AUDIO track fine but
 * couldn't decode/demux the VIDEO track or container at all — almost
 * always an MKV/AVI/MOV/WEBM container, or H.265/HEVC (or another
 * non-H.264) video codec, none of which Chrome/Edge/the Telegram WebView
 * reliably play inline via a plain <video src>.
 *
 * This module never blocks a playback request. When routes/stream.js
 * sees a video whose container/mime looks unsafe for inline HTML5
 * playback, it calls scheduleCompatibilityCheck() and keeps serving the
 * CURRENT bytes immediately (best effort — may still show the same bug
 * once). In the background, this module downloads the source (reusing
 * services/telegramUpload.js#downloadFromChannel — MTProto, real channel
 * only, same as the rest of the pipeline), probes + transcodes it with
 * FFmpeg (services/compress.js — the exact same remux-or-transcode logic
 * the /add upload pipeline already uses), uploads the guaranteed
 * H.264/AAC/MP4 result back into the storage channel, and swaps the
 * Firestore doc's channelId/messageId/mimeType over to it. Every request
 * from that point on streams the fixed copy.
 *
 * Firestore fields added (purely additive — no existing field is
 * renamed, removed, or repurposed, so this does not change the schema
 * any existing route/serializer depends on):
 *   playbackCompatible: boolean — true once we're confident (or have
 *     made) the file is safe for inline playback; skips all future checks.
 *   transcoding: boolean — true while a background fix is in progress,
 *     so a flood of Range requests for the same video doesn't schedule
 *     the same expensive job dozens of times.
 *   transcodedAt: ISO date string — set once a swap completes.
 * ----------------------------------------------------------------------
 */
'use strict';

const os = require('os');
const path = require('path');
const fs = require('fs');
const { makeLogger } = require('../utils/logger');
const log = makeLogger('services/playbackCompat.js');
const compress = require('./compress');
const transfer = require('./telegramUpload');
const { updateDoc } = require('./firebase');

const TEMP_DIR = path.join(os.tmpdir(), 'myflix-playback-compat');
fs.mkdirSync(TEMP_DIR, { recursive: true });

// Containers/mime hints browsers cannot reliably demux inline. This is a
// fast, download-free heuristic based on what Telegram reports — the
// authoritative check (actual codec) only happens once we've downloaded
// the file for a real transcode anyway (see compress.js#analyze).
const INCOMPATIBLE_MIME = [/matroska/i, /x-msvideo/i, /quicktime/i, /webm/i, /x-flv/i, /mpegts/i];
const INCOMPATIBLE_EXT = /\.(mkv|avi|mov|wmv|flv|ts|m2ts)$/i;

function looksIncompatible(mimeType, fileName) {
  if (INCOMPATIBLE_MIME.some((re) => re.test(mimeType || ''))) return true;
  if (INCOMPATIBLE_EXT.test(fileName || '')) return true;
  return false;
}

// In-process guard, cheap and immediate — belt-and-suspenders alongside
// the Firestore `transcoding` flag, which is what actually prevents a
// second Render instance (or a process restart) from double-scheduling.
const inFlight = new Set();

/**
 * @param {string} videoId
 * @param {object} video the Firestore video doc
 * @param {{mimeType?:string, fileName?:string}} source from mtproto.resolveVideoSource()
 */
function scheduleCompatibilityCheck(videoId, video, source) {
  if (video.playbackCompatible) return;
  if (video.transcoding) return;
  if (inFlight.has(videoId)) return;

  if (!looksIncompatible(source.mimeType, source.fileName)) {
    // Nothing about the container/mime looks unsafe — trust it so we
    // don't re-run this check on every single Range request for this video.
    updateDoc('videos', videoId, { playbackCompatible: true }).catch(() => {});
    return;
  }

  inFlight.add(videoId);
  updateDoc('videos', videoId, { transcoding: true }).catch((err) => {
    log.warn('scheduleCompatibilityCheck', 'Failed to set transcoding flag', { videoId, reason: err.message });
  });
  log.info('scheduleCompatibilityCheck', 'Compatibility issue detected — scheduling background transcode', {
    videoId, mimeType: source.mimeType, fileName: source.fileName,
  });

  runTranscode(videoId, video)
    .catch((err) => log.error('scheduleCompatibilityCheck', 'Background transcode failed', err, { videoId, stack: err.stack }))
    .finally(() => {
      inFlight.delete(videoId);
      updateDoc('videos', videoId, { transcoding: false }).catch(() => {});
    });
}

async function runTranscode(videoId, video) {
  const stamp = `${videoId}_${Date.now()}`;
  const inPath = path.join(TEMP_DIR, `${stamp}.src`);
  const outPath = path.join(TEMP_DIR, `${stamp}.mp4`);

  try {
    log.info('runTranscode', 'Download started', { videoId, channelId: video.channelId, messageId: video.messageId });
    await transfer.downloadFromChannel(video.channelId, video.messageId, inPath);
    log.success('runTranscode', 'Download completed', { videoId });

    log.info('runTranscode', 'Compression started', { videoId });
    const info = await compress.processFile(inPath, outPath);
    log.success('runTranscode', 'Compression completed', { videoId, mode: info.mode, videoCodec: info.videoCodec, audioCodec: info.audioCodec });

    const storageChannelId = process.env.STORAGE_CHANNEL_ID;
    if (!storageChannelId) throw new Error('STORAGE_CHANNEL_ID is not set — cannot upload the fixed copy.');

    log.info('runTranscode', 'Upload started', { videoId });
    const result = await transfer.uploadEpisode(storageChannelId, outPath, {
      fileName: `${String(video.title || videoId).replace(/[/\\?%*:|"<>]/g, '')}.mp4`,
      duration: info.duration || video.duration || 0,
      width: info.width || 0,
      height: info.height || 0,
    });
    log.success('runTranscode', 'Upload completed', { videoId, newChannelId: result.channelId, newMessageId: result.messageId });

    await updateDoc('videos', videoId, {
      channelId: Number(result.channelId),
      messageId: result.messageId,
      mimeType: result.mimeType || 'video/mp4',
      fileSizeBytes: result.size,
      playbackCompatible: true,
      transcodedAt: new Date().toISOString(),
    });
    log.success('runTranscode', 'Firestore updated — subsequent requests will stream the fixed copy', { videoId });
  } finally {
    fs.unlink(inPath, () => {});
    fs.unlink(outPath, () => {});
    log.info('runTranscode', 'Cleanup completed', { videoId });
  }
}

module.exports = { scheduleCompatibilityCheck, looksIncompatible };
