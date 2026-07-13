/**
 * services/compress.js
 * ----------------------------------------------------------------------
 * FFmpeg wrapper for the admin upload pipeline (handlers/adminUpload.js +
 * queue/pipeline.js). Uses ffmpeg-static / ffprobe-static so no system
 * FFmpeg install is required on the host (Render, a VPS, etc.) — the
 * binaries ship inside node_modules.
 *
 * Philosophy (per product decision): only pay for a full re-encode when a
 * source file actually needs it. Browsers reliably play MP4 containers
 * with H.264 video + AAC audio and the moov atom at the front of the file
 * ("faststart"). So:
 *   - If video is already H.264 and audio is already AAC (or there's no
 *     audio track), we do a cheap stream-copy remux with +faststart.
 *     This just rewrites the container, not the encoded frames — fast
 *     and lossless.
 *   - Otherwise we do a full transcode to H.264/AAC.
 *   - If a remux unexpectedly fails (some sources claim h264/aac but use
 *     an incompatible profile/level), we fall back to a full transcode
 *     rather than failing the episode outright.
 * ----------------------------------------------------------------------
 */
'use strict';

const ffmpegPath = require('ffmpeg-static');
const ffprobePath = require('ffprobe-static').path;
const ffmpeg = require('fluent-ffmpeg');
const { makeLogger } = require('../utils/logger');
const log = makeLogger('services/compress.js');

ffmpeg.setFfmpegPath(ffmpegPath);
ffmpeg.setFfprobePath(ffprobePath);

function probe(filePath) {
  return new Promise((resolve, reject) => {
    ffmpeg.ffprobe(filePath, (err, metadata) => {
      if (err) return reject(err);
      resolve(metadata);
    });
  });
}

/** Inspects a downloaded source file and decides what work it needs. */
async function analyze(filePath) {
  const meta = await probe(filePath);
  const streams = meta.streams || [];
  const videoStream = streams.find((s) => s.codec_type === 'video');
  const audioStream = streams.find((s) => s.codec_type === 'audio');

  if (!videoStream) {
    throw new Error('Source file has no video stream.');
  }

  const videoOk = videoStream.codec_name === 'h264';
  const audioOk = !audioStream || audioStream.codec_name === 'aac';
  const mode = videoOk && audioOk ? 'remux' : 'transcode';

  return {
    mode,
    duration: Math.round(Number(meta.format?.duration) || 0),
    width: videoStream.width || 0,
    height: videoStream.height || 0,
    videoCodec: videoStream.codec_name || null,
    audioCodec: audioStream?.codec_name || null,
  };
}

function runFfmpeg(inputPath, outputPath, mode, onProgress) {
  return new Promise((resolve, reject) => {
    const cmd = ffmpeg(inputPath).output(outputPath);

    if (mode === 'remux') {
      cmd.outputOptions(['-c copy', '-movflags +faststart']);
    } else {
      cmd
        .videoCodec('libx264')
        .audioCodec('aac')
        .outputOptions([
          '-preset veryfast',
          '-crf 23',
          '-pix_fmt yuv420p',
          '-movflags +faststart',
          '-ac 2',
        ]);
    }

    cmd
      .on('start', (line) => log.info('runFfmpeg', 'FFmpeg started', { mode, cmd: line }))
      .on('progress', (p) => { if (onProgress) onProgress(p); })
      .on('error', (err) => reject(err))
      .on('end', () => resolve())
      .run();
  });
}

/**
 * Converts a downloaded source file into a browser-streamable MP4 at
 * outputPath. Returns the probed source info (plus the mode actually
 * used, which may differ from the initial plan on remux-fallback).
 */
async function processFile(inputPath, outputPath, onProgress) {
  const info = await analyze(inputPath);

  try {
    await runFfmpeg(inputPath, outputPath, info.mode, onProgress);
    return info;
  } catch (err) {
    if (info.mode === 'remux') {
      log.warn('processFile', 'Remux failed — falling back to full transcode', { reason: err.message });
      await runFfmpeg(inputPath, outputPath, 'transcode', onProgress);
      return { ...info, mode: 'transcode-fallback' };
    }
    throw err;
  }
}

module.exports = { probe, analyze, processFile };
