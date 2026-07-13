/**
 * services/compress.js
 * ----------------------------------------------------------------------
 * FFmpeg wrapper for the admin upload pipeline (handlers/adminUpload.js +
 * queue/pipeline.js).
 *
 * Uses the SYSTEM ffmpeg/ffprobe binaries via child_process.spawn — no
 * npm package (ffmpeg-static / ffprobe-static / fluent-ffmpeg) required.
 * This avoids the class of "Cannot find module 'ffmpeg-static'" failures
 * those packages cause on hosts like Render, where their postinstall
 * step (downloading a prebuilt binary from GitHub releases at npm-install
 * time) can silently fail or get skipped by a cached/partial install.
 *
 * DEPLOYMENT REQUIREMENT: the host this runs on must have `ffmpeg` and
 * `ffprobe` on PATH (or point FFMPEG_PATH / FFPROBE_PATH env vars at
 * their binaries). Render's default native Node "Web Service" runtime
 * does NOT include FFmpeg — you need either:
 *   - A Docker-based Render deploy with `RUN apt-get update && apt-get
 *     install -y ffmpeg` in the Dockerfile, or
 *   - Any other host/buildpack that guarantees an ffmpeg binary on PATH.
 * If FFmpeg isn't found, every /add upload will fail at the compression
 * step with a clear "not found on PATH" error (see ensureAvailable()) —
 * not a cryptic module-resolution crash.
 *
 * Conversion strategy: only pay for a full re-encode when a source file
 * actually needs it. Browsers reliably play MP4 containers with H.264
 * video + AAC audio and the moov atom at the front of the file
 * ("faststart"). So:
 *   - If video is already H.264 and audio is already AAC (or there's no
 *     audio track), do a cheap stream-copy remux with +faststart —
 *     rewrites the container only, not the encoded frames.
 *   - Otherwise, do a full transcode to H.264/AAC.
 *   - If a remux unexpectedly fails (some sources claim h264/aac but use
 *     an incompatible profile/level), fall back to a full transcode.
 * ----------------------------------------------------------------------
 */
'use strict';

const { spawn } = require('child_process');
const { makeLogger } = require('../utils/logger');
const log = makeLogger('services/compress.js');

const FFMPEG_BIN = process.env.FFMPEG_PATH || 'ffmpeg';
const FFPROBE_BIN = process.env.FFPROBE_PATH || 'ffprobe';

function run(bin, args) {
  return new Promise((resolve, reject) => {
    let child;
    try {
      child = spawn(bin, args);
    } catch (err) {
      reject(err);
      return;
    }

    let stdout = '';
    let stderr = '';
    child.stdout?.on('data', (d) => { stdout += d; });
    child.stderr?.on('data', (d) => { stderr += d; });

    child.on('error', (err) => {
      if (err.code === 'ENOENT') {
        reject(new Error(
          `"${bin}" was not found on PATH. Install FFmpeg on this host, or set ` +
          `${bin === FFMPEG_BIN ? 'FFMPEG_PATH' : 'FFPROBE_PATH'} to its full binary path.`
        ));
      } else {
        reject(err);
      }
    });

    child.on('close', (code) => {
      if (code === 0) resolve({ stdout, stderr });
      else reject(new Error(`${bin} exited with code ${code}: ${stderr.slice(-2000) || '(no stderr output)'}`));
    });
  });
}

// Checked once, lazily, on first real use — not at module-load time, so
// importing this file never crashes the whole process on a host that
// hasn't got FFmpeg installed; only the /add pipeline's compression step
// fails, with an actionable error message.
let availabilityPromise = null;
function ensureAvailable() {
  if (!availabilityPromise) {
    availabilityPromise = (async () => {
      try {
        await run(FFMPEG_BIN, ['-version']);
        await run(FFPROBE_BIN, ['-version']);
      } catch (err) {
        availabilityPromise = null; // allow retrying later (e.g. after an admin fixes PATH and doesn't restart the process)
        throw new Error(
          `FFmpeg is not available on this host (${err.message}). The /add upload pipeline needs a ` +
          `system FFmpeg install — see the deployment note at the top of services/compress.js.`
        );
      }
    })();
  }
  return availabilityPromise;
}

async function probe(filePath) {
  await ensureAvailable();
  const { stdout } = await run(FFPROBE_BIN, [
    '-v', 'error',
    '-print_format', 'json',
    '-show_format',
    '-show_streams',
    filePath,
  ]);
  try {
    return JSON.parse(stdout);
  } catch (err) {
    throw new Error(`Could not parse ffprobe output: ${err.message}`);
  }
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

function parseTimemark(stderrChunk) {
  // ffmpeg prints progress lines like: "... time=00:01:23.45 bitrate=... speed=..."
  const match = stderrChunk.match(/time=(\d+):(\d+):(\d+\.?\d*)/);
  if (!match) return null;
  const [, hh, mm, ss] = match;
  const seconds = Number(hh) * 3600 + Number(mm) * 60 + parseFloat(ss);
  return { timemark: `${hh}:${mm}:${ss}`, seconds };
}

function runFfmpeg(inputPath, outputPath, mode, onProgress) {
  const args = ['-y', '-i', inputPath];

  if (mode === 'remux') {
    args.push('-c', 'copy', '-movflags', '+faststart');
  } else {
    args.push(
      '-c:v', 'libx264',
      '-preset', 'veryfast',
      '-crf', '23',
      '-pix_fmt', 'yuv420p',
      '-c:a', 'aac',
      '-ac', '2',
      '-movflags', '+faststart'
    );
  }
  args.push(outputPath);

  return new Promise((resolve, reject) => {
    let child;
    try {
      child = spawn(FFMPEG_BIN, args);
    } catch (err) {
      reject(err);
      return;
    }

    let stderrTail = '';
    child.stderr?.on('data', (d) => {
      const text = d.toString();
      stderrTail = (stderrTail + text).slice(-4000);
      if (onProgress) {
        const progress = parseTimemark(text);
        if (progress) onProgress(progress);
      }
    });

    child.on('error', (err) => {
      if (err.code === 'ENOENT') {
        reject(new Error(`"${FFMPEG_BIN}" was not found on PATH. Install FFmpeg on this host, or set FFMPEG_PATH.`));
      } else {
        reject(err);
      }
    });

    child.on('close', (code) => {
      if (code === 0) resolve();
      else reject(new Error(`ffmpeg exited with code ${code}: ${stderrTail || '(no stderr output)'}`));
    });
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

module.exports = { probe, analyze, processFile, ensureAvailable };
