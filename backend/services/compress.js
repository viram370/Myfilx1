/**
 * services/compress.js
 * ----------------------------------------------------------------------
 * FFmpeg wrapper for the admin upload pipeline (handlers/adminUpload.js +
 * queue/pipeline.js) and the background playback-compatibility fixer
 * (services/playbackCompat.js).
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
 * ---------------------------------------------------------------------
 * FIX (compression failed for Matroska/MKV sources):
 * The previous version never passed `-map` to FFmpeg at all — it relied
 * entirely on FFmpeg's implicit "grab the best stream of each type"
 * stream selection. That's exactly what breaks MKV (and often MOV/TS)
 * sources, which routinely carry things a plain MP4 container can't
 * hold and that FFmpeg auto-selects unless told otherwise:
 *   - Embedded subtitle tracks (SRT/ASS/PGS) — `-c copy`/`-c:s` with no
 *     explicit mapping tries to stream-copy whatever subtitle codec is
 *     in the source, and MP4's muxer rejects most of them outright
 *     ("Could not find tag for codec ... in stream ..., codec not
 *     currently supported in container", or "Automatic encoder
 *     selection failed" when a full transcode was attempted instead).
 *     This is the #1 reason MKV rips (which almost always ship at least
 *     one subtitle track) failed on BOTH the remux and the transcode
 *     fallback path — neither excluded subtitles, so both hit the same
 *     wall.
 *   - Cover-art / attached-picture streams (`disposition.attached_pic`)
 *     — some MKV/MP3-style files expose a thumbnail image as a second
 *     "video" stream; naive `find(s => s.codec_type === 'video')` can
 *     grab that instead of the real video track.
 *   - Chapter/timecode/data streams that don't map cleanly onto MP4.
 *
 * The fix: every mapped stream is now explicit (`-map 0:<index>` for
 * exactly the real video stream, every audio stream, and — separately —
 * every *text-based* subtitle stream converted to `mov_text`, MP4's only
 * subtitle codec). Bitmap subtitle formats (PGS/DVD/VOBSUB/DVB) can't be
 * represented in MP4 at all, so they're detected and dropped with a
 * logged warning instead of aborting the whole conversion. If FFmpeg
 * still rejects an attempt for any other reason, processFile() steps
 * through a small ordered set of more conservative fallbacks (drop
 * subtitles → force a full re-encode → drop audio entirely as a last
 * resort) before giving up, so one unsupported side-stream never sinks
 * an otherwise-perfectly-convertible video.
 *
 * Conversion strategy (unchanged in spirit): only pay for a full
 * re-encode when a source file actually needs it.
 *   - If video is already H.264 (in a browser-safe 4:2:0 pixel format)
 *     and every audio track is already AAC (or there's no audio at all),
 *     stream-copy both — a cheap remux with +faststart that only
 *     rewrites the container, never the encoded frames.
 *   - Otherwise, re-encode video to H.264 and every audio track to AAC.
 *   - Orientation/aspect ratio: no scale/crop filter is ever applied, so
 *     the source aspect ratio always survives untouched. Rotation
 *     (a `rotate` tag or a display-matrix side_data entry — the way
 *     phone-shot MOV/MKV/MP4 sources record "this frame is actually
 *     sideways") is preserved automatically: stream-copy carries the
 *     rotation metadata through unchanged, and FFmpeg's own autorotate
 *     behavior bakes the same rotation into the pixels whenever a real
 *     re-encode happens — so playback is upright either way without any
 *     custom transpose filter here (which would risk double-rotating a
 *     file FFmpeg already auto-corrected).
 * ----------------------------------------------------------------------
 */
'use strict';

const { spawn } = require('child_process');
const fs = require('fs');
const { makeLogger } = require('../utils/logger');
const log = makeLogger('services/compress.js');

const FFMPEG_BIN = process.env.FFMPEG_PATH || 'ffmpeg';
const FFPROBE_BIN = process.env.FFPROBE_PATH || 'ffprobe';

// Text-based subtitle codecs FFmpeg can losslessly convert to MP4's
// `mov_text` codec.
const TEXT_SUBTITLE_CODECS = new Set([
  'subrip', 'srt', 'ass', 'ssa', 'mov_text', 'webvtt', 'subviewer', 'text', 'ttml',
]);
// Bitmap/image-based subtitle codecs — MP4 has no way to carry these at
// all (mov_text is text-only), so they're always dropped, never copied.
const BITMAP_SUBTITLE_CODECS = new Set([
  'dvd_subtitle', 'dvdsub', 'hdmv_pgs_subtitle', 'pgssub', 'xsub', 'dvb_subtitle', 'dvb_teletext',
]);

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

// ── Hardware-accelerated encoder auto-detection ────────────────────────
// Checked once per process, lazily — same pattern as ensureAvailable()
// above. Every one of these is used with a quality target equal to the
// CPU path's own resolution-adaptive CRF (see pickCrf/buildFfmpegArgs) —
// this is purely a speed optimization, never a quality trade-off, and
// buildFfmpegArgs()/processFile() always fall back to the proven libx264
// CPU path (per compression *attempt*, not just once) if a hardware
// encoder is unavailable or fails partway through.
const HW_ENCODER_CANDIDATES = [
  { name: 'h264_nvenc', label: 'NVIDIA NVENC' },
  { name: 'h264_qsv', label: 'Intel QuickSync' },
  { name: 'h264_videotoolbox', label: 'Apple VideoToolbox' },
  { name: 'h264_vaapi', label: 'VA-API' },
];

/** Actually tries a 1-frame encode — an encoder can be compiled into this ffmpeg build (shows up in `-encoders`) with no matching GPU/driver present on the host, which only a real encode attempt reveals. */
async function probeHardwareEncoderWorks(encoderName) {
  const args = ['-y', '-f', 'lavfi', '-i', 'color=c=black:s=64x64:d=0.1'];
  if (encoderName === 'h264_vaapi') {
    args.unshift('-vaapi_device', process.env.VAAPI_DEVICE || '/dev/dri/renderD128');
    args.push('-vf', 'format=nv12,hwupload');
  }
  args.push('-frames:v', '1', '-c:v', encoderName, '-f', 'null', '-');
  try {
    await run(FFMPEG_BIN, args);
    return true;
  } catch (err) {
    log.info('probeHardwareEncoderWorks', `${encoderName} is not usable on this host`, { reason: err.message });
    return false;
  }
}

let hwEncoderPromise = null;
/** Resolves to a HW_ENCODER_CANDIDATES entry, or null to mean "use CPU (libx264)". */
function detectHardwareEncoder() {
  if (!hwEncoderPromise) {
    hwEncoderPromise = (async () => {
      if (process.env.DISABLE_HWACCEL === '1') {
        log.info('detectHardwareEncoder', 'Hardware acceleration disabled via DISABLE_HWACCEL=1 — using CPU (libx264)');
        return null;
      }
      if (process.env.FORCE_HWACCEL) {
        const forced = HW_ENCODER_CANDIDATES.find((c) => c.name === process.env.FORCE_HWACCEL);
        if (forced) {
          log.info('detectHardwareEncoder', `Hardware encoder forced via FORCE_HWACCEL=${forced.name} (not re-probed)`);
          return forced;
        }
        log.warn('detectHardwareEncoder', `FORCE_HWACCEL="${process.env.FORCE_HWACCEL}" is not a recognized encoder name — ignoring and auto-detecting instead`);
      }

      let encodersOutput = '';
      try {
        await ensureAvailable();
        const { stdout } = await run(FFMPEG_BIN, ['-hide_banner', '-encoders']);
        encodersOutput = stdout;
      } catch (err) {
        log.warn('detectHardwareEncoder', 'Could not list ffmpeg encoders — using CPU (libx264)', { reason: err.message });
        return null;
      }

      for (const candidate of HW_ENCODER_CANDIDATES) {
        if (!encodersOutput.includes(candidate.name)) continue;
        const works = await probeHardwareEncoderWorks(candidate.name);
        if (works) {
          log.success('detectHardwareEncoder', `Hardware encoder available: ${candidate.label} (${candidate.name}) — used for re-encodes with automatic per-attempt fallback to CPU if it ever fails`);
          return candidate;
        }
      }
      log.info('detectHardwareEncoder', 'No usable hardware encoder found on this host — using CPU (libx264)');
      return null;
    })();
  }
  return hwEncoderPromise;
}

/**
 * Picks the real video track out of every `codec_type === 'video'`
 * stream ffprobe reports — explicitly excluding attached-picture/cover-
 * art streams (a still image tagged as a "video" stream, common in MKV
 * and some MP3/M4A rips). If a source genuinely has more than one real
 * video track (rare — some odd MKV muxes), the largest by pixel area is
 * used since that's virtually always the main feature rather than a
 * secondary/commentary angle.
 */
function pickVideoStream(streams) {
  const real = streams.filter(
    (s) => s.codec_type === 'video' && !(s.disposition && Number(s.disposition.attached_pic) === 1)
  );
  if (real.length === 0) return null;
  return real.reduce((best, s) => {
    const area = (s.width || 0) * (s.height || 0);
    const bestArea = (best.width || 0) * (best.height || 0);
    return area > bestArea ? s : best;
  });
}

function pickAudioStreams(streams) {
  return streams.filter((s) => s.codec_type === 'audio');
}

/**
 * Splits subtitle streams into the ones that can be safely converted to
 * MP4's `mov_text` codec, and the ones (bitmap formats, or anything
 * unrecognized) that have to be dropped rather than risk aborting the
 * whole conversion.
 */
function classifySubtitleStreams(streams) {
  const subStreams = streams.filter((s) => s.codec_type === 'subtitle');
  const keep = [];
  const drop = [];
  for (const s of subStreams) {
    const codec = (s.codec_name || '').toLowerCase();
    if (TEXT_SUBTITLE_CODECS.has(codec)) keep.push(s);
    else drop.push({ index: s.index, codec: codec || 'unknown', bitmap: BITMAP_SUBTITLE_CODECS.has(codec) });
  }
  return { keep, drop };
}

/** Reads rotation off either the classic `rotate` tag (common in MOV/MP4) or a display-matrix side_data entry (common in MKV/newer MP4). */
function detectRotation(videoStream) {
  const tagRotate = videoStream.tags && videoStream.tags.rotate;
  if (tagRotate !== undefined) {
    const n = parseInt(tagRotate, 10);
    if (Number.isFinite(n)) return n;
  }
  const sideData = (videoStream.side_data_list || []).find((d) => typeof d.rotation === 'number');
  return sideData ? sideData.rotation : 0;
}

/**
 * Inspects a downloaded source file — of ANY common container (MP4, MKV,
 * WebM, AVI, MOV, MPEG-TS, FLV, ...) — and decides what work it needs.
 * Never assumes the container based on file extension; everything here
 * comes from ffprobe's actual stream inspection.
 */
async function analyze(filePath) {
  const meta = await probe(filePath);
  const streams = meta.streams || [];
  const containerFormat = (meta.format?.format_name || '').toLowerCase();

  const videoStream = pickVideoStream(streams);
  if (!videoStream) {
    throw new Error(
      'Source file has no playable video stream (only audio, cover-art, or subtitle streams were found).'
    );
  }

  const audioStreams = pickAudioStreams(streams);
  const { keep: subtitleStreams, drop: droppedSubtitles } = classifySubtitleStreams(streams);

  const videoCodec = (videoStream.codec_name || '').toLowerCase();
  const pixFmt = (videoStream.pix_fmt || '').toLowerCase();
  // Only a plain 4:2:0 H.264 stream is safe to hand to browsers untouched
  // — anything else (HEVC/VP9/MPEG-4/etc, or an exotic 4:2:2/4:4:4 pixel
  // format) is treated as needing a real re-encode.
  const videoOk = videoCodec === 'h264' && (pixFmt === '' || /^yuvj?420p$/.test(pixFmt));
  const audioOk = audioStreams.length === 0
    || audioStreams.every((s) => {
      if ((s.codec_name || '').toLowerCase() !== 'aac') return false;
      // A remux (audioMode 'copy') carries the source's audio stream
      // through byte-for-byte, unprofiled by codec_name alone. Two real-
      // world shapes of "aac" both survive that check but are routinely
      // rejected by Telegram's inline player (and plenty of browsers)
      // even though the container/video are perfectly fine:
      //   - more than stereo (5.1/7.1 surround — common in MKV rips)
      //   - the HE-AAC/HE-AACv2 profile (common in older phone-camera
      //     and some screen-recording exports) rather than plain LC-AAC
      // Either one forces this file down the transcode tier instead,
      // where `-c:a aac -ac 2` (no -profile:a flag) always produces
      // plain stereo LC-AAC — see buildFfmpegArgs.
      const channels = Number(s.channels) || 0;
      if (channels > 2) return false;
      const profile = (s.profile || '').toUpperCase();
      if (profile.includes('HE-AAC')) return false;
      return true;
    });

  const rotation = detectRotation(videoStream);

  if (droppedSubtitles.length > 0) {
    log.warn('analyze', `${droppedSubtitles.length} subtitle track(s) use a format MP4 can't hold and will be dropped`, {
      filePath, dropped: droppedSubtitles,
    });
  }

  return {
    mode: videoOk && audioOk ? 'remux' : 'transcode',
    duration: Math.round(Number(meta.format?.duration) || 0),
    width: videoStream.width || 0,
    height: videoStream.height || 0,
    videoCodec: videoStream.codec_name || null,
    audioCodec: audioStreams[0]?.codec_name || null,
    container: containerFormat,
    rotation,
    videoOk,
    audioOk,
    videoStreamIndex: videoStream.index,
    audioStreamIndexes: audioStreams.map((s) => s.index),
    subtitleStreamIndexes: subtitleStreams.map((s) => s.index),
    droppedSubtitleCount: droppedSubtitles.length,
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

/** Turns a raw FFmpeg failure into a message that names the likely cause instead of just dumping the stderr tail. */
function describeFfmpegFailure(code, stderrTail, signal) {
  const tail = stderrTail || '(no stderr output)';
  if (signal) return `ffmpeg was killed by signal ${signal}: ${tail}`;
  const patterns = [
    {
      re: /codec not currently supported in container|Could not find tag for codec/i,
      hint: 'a mapped stream uses a codec the MP4 container cannot hold (typically a subtitle or data/timecode track)',
    },
    {
      re: /Automatic encoder selection failed|Default encoder for format .* is probably disabled/i,
      hint: 'FFmpeg had no compatible encoder for one of the mapped streams (often a subtitle track)',
    },
    { re: /Invalid data found when processing input/i, hint: 'the source file appears corrupted or truncated' },
    { re: /moov atom not found/i, hint: 'the source MP4 is incomplete or corrupted (missing moov atom)' },
    { re: /Unknown encoder|Encoder not found/i, hint: 'this FFmpeg build is missing a required encoder (confirm libx264/aac support)' },
    { re: /Unsupported codec|Decoder not found/i, hint: 'this FFmpeg build cannot decode one of the source streams' },
  ];
  const hit = patterns.find((p) => p.re.test(tail));
  const hintText = hit ? ` — likely cause: ${hit.hint}` : '';
  return `ffmpeg exited with code ${code}${hintText}: ${tail}`;
}

/**
 * Runs FFmpeg with a fully pre-built argument list (see buildFfmpegArgs).
 * @param {string[]} args
 * @param {(info:{timemark:string, seconds:number, percent:?number})=>void} [onProgress]
 * @param {number} [totalDurationSeconds]
 */
function runFfmpeg(args, onProgress, totalDurationSeconds) {
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
        if (progress) {
          const percent = totalDurationSeconds > 0
            ? Math.max(0, Math.min(100, Math.round((progress.seconds / totalDurationSeconds) * 100)))
            : null;
          onProgress({ ...progress, percent });
        }
      }
    });

    child.on('error', (err) => {
      if (err.code === 'ENOENT') {
        reject(new Error(`"${FFMPEG_BIN}" was not found on PATH. Install FFmpeg on this host, or set FFMPEG_PATH.`));
      } else {
        reject(err);
      }
    });

    child.on('close', (code, signal) => {
      log.info('runFfmpeg', 'FFmpeg process exited', { code, signal: signal || null });
      if (code === 0) resolve();
      else reject(new Error(describeFfmpegFailure(code, stderrTail, signal)));
    });
  });
}

/**
 * Adaptive quality target instead of one fixed CRF for every input. Lower
 * CRF = higher quality/bitrate. x264's perceptual quality-per-CRF-step
 * isn't resolution-independent — the same CRF looks visibly softer on a
 * small frame than a large one relative to its own detail level, and a
 * fixed "safe" CRF for 4K content wastes enormous bitrate on a 480p
 * source. Bucketed by pixel count instead of a magic single number:
 *   - small (≤480p): CRF 20 — near-source quality; the frame is small
 *     enough that the bitrate cost of doing so is trivial.
 *   - medium (≤1080p): CRF 22 — minimal, essentially imperceptible loss.
 *   - large (>1080p, i.e. 1440p/4K): CRF 24 — still visually
 *     transparent at that pixel density while avoiding an enormous file.
 */
function pickCrf(info) {
  const pixels = (info.width || 0) * (info.height || 0);
  if (pixels > 0 && pixels <= 720 * 480) return 20; // small
  if (pixels <= 1920 * 1080) return 22; // medium
  return 24; // large (1440p/4K+)
}

/**
 * Builds a full FFmpeg argument list for one conversion attempt. Every
 * mapped stream is explicit — this is the core of the MKV fix: FFmpeg
 * never gets to guess which streams belong in the output.
 *
 * @param {string} inputPath
 * @param {string} outputPath
 * @param {{videoMode:'copy'|'encode', audioMode:'copy'|'encode', includeSubtitles:boolean, dropAudio?:boolean}} tier
 * @param {ReturnType<typeof analyze> extends Promise<infer T> ? T : never} info
 * @param {?{name:string, label:string}} [hwEncoder] result of detectHardwareEncoder(), or null for CPU (libx264)
 */
function buildFfmpegArgs(inputPath, outputPath, tier, info, hwEncoder = null) {
  const { videoMode, audioMode, includeSubtitles, dropAudio } = tier;
  const args = ['-y', '-i', inputPath];

  // Exactly one real video stream, always.
  args.push('-map', `0:${info.videoStreamIndex}`);

  // Every audio track — multi-audio sources (dual-language releases,
  // commentary tracks, etc.) all get carried through, not just Telegram/
  // FFmpeg's "first" pick. The trailing `?` makes each map optional so a
  // stream that vanishes between probe and encode never hard-fails.
  if (!dropAudio) {
    for (const idx of info.audioStreamIndexes) {
      args.push('-map', `0:${idx}?`);
    }
  }

  // Only text-based subtitle tracks are ever mapped — bitmap formats are
  // already excluded by analyze()/classifySubtitleStreams().
  if (includeSubtitles) {
    for (const idx of info.subtitleStreamIndexes) {
      args.push('-map', `0:${idx}?`);
    }
  }

  // Chapters copy cleanly from MKV/MOV far less reliably than they fail
  // silently when dropped — and nothing in this pipeline needs them.
  args.push('-map_chapters', '-1');

  if (videoMode === 'copy') {
    args.push('-c:v', 'copy');
  } else {
    const crf = pickCrf(info);
    // Same no-op-unless-needed scale filter as before — guards against
    // odd (non-even) source dimensions, which yuv420p requires. VA-API
    // additionally needs the frame explicitly uploaded into GPU memory,
    // so its filter chain extends this one rather than replacing it.
    const scaleFilter = 'scale=trunc(iw/2)*2:trunc(ih/2)*2';

    if (hwEncoder?.name === 'h264_nvenc') {
      args.push(
        '-c:v', 'h264_nvenc',
        '-preset', 'p6', // NVENC p1(fastest)..p7(slowest) — p6 biases toward quality; still far faster than CPU
        '-tune', 'hq',
        '-rc', 'vbr',
        '-cq', String(crf), // same numeric quality target as the CPU CRF path below — not a lower one
        '-b:v', '0',
        '-pix_fmt', 'yuv420p',
        '-profile:v', 'high',
        '-vsync', 'cfr',
        '-vf', scaleFilter
      );
    } else if (hwEncoder?.name === 'h264_qsv') {
      args.push(
        '-c:v', 'h264_qsv',
        '-preset', 'medium', // QSV veryfast..veryslow — medium favors quality over QSV's fastest option
        '-global_quality', String(crf),
        '-look_ahead', '1',
        '-pix_fmt', 'nv12',
        '-profile:v', 'high',
        '-vsync', 'cfr',
        '-vf', scaleFilter
      );
    } else if (hwEncoder?.name === 'h264_vaapi') {
      // VAAPI needs the device initialized as a GLOBAL option (before
      // -i) and frames explicitly uploaded into GPU memory via the
      // filter chain — unlike NVENC/QSV it can't be dropped in as a
      // plain -c:v swap.
      args.unshift('-vaapi_device', process.env.VAAPI_DEVICE || '/dev/dri/renderD128');
      args.push(
        '-c:v', 'h264_vaapi',
        '-qp', String(crf),
        '-profile:v', 'high',
        '-vsync', 'cfr',
        '-vf', `${scaleFilter},format=nv12,hwupload`
      );
    } else if (hwEncoder?.name === 'h264_videotoolbox') {
      args.push(
        '-c:v', 'h264_videotoolbox',
        // VideoToolbox's -q:v is 1(worst)-100(best), not CRF-like — biased
        // deliberately high (never below 60) so this never undercuts the
        // CPU path's quality target, only approximates it from the safe side.
        '-q:v', String(Math.max(60, 100 - crf * 2)),
        '-pix_fmt', 'yuv420p',
        '-profile:v', 'high',
        '-vsync', 'cfr',
        '-vf', scaleFilter
      );
    } else {
      args.push(
        '-c:v', 'libx264',
        '-preset', 'veryfast',
        '-crf', String(crf),
        '-pix_fmt', 'yuv420p',
        '-profile:v', 'high',
        // Explicit auto thread count — libx264 already defaults to this,
        // but pins it instead of leaving it implicit so every core on the
        // host actually gets used for the video encode.
        '-threads', '0',
        // Constant frame rate: many MKV/phone-camera/screen-recording
        // sources are variable-frame-rate (frames stored irregularly in
        // time). VFR H.264 is legal but several players — Telegram's
        // inline player among them — handle it poorly (stutter, audio
        // drift, or duration/seek-bar mismatches). `-vsync cfr` re-times
        // every re-encoded output to the source's average frame rate,
        // duplicating/dropping frames as needed so the output timeline is
        // perfectly even. Only applied when a real re-encode is already
        // happening — a stream-copy tier can't change frame timing without
        // decoding, and analyze()/verifyOutputFile() don't require CFR for
        // the (already H.264) sources that qualify for a copy.
        '-vsync', 'cfr',
        // Deliberately NOT forcing a fixed -level here. A hardcoded level
        // (e.g. "4.1") is only valid up to a certain resolution/bitrate —
        // anything above that (a 1440p/4K source, or just a higher-bitrate
        // 1080p one) produces a technically non-conformant bitstream tagged
        // with a level it doesn't actually satisfy. Some decoders tolerate
        // that; Telegram's inline player is one of the ones that doesn't,
        // and falls back to "Use external video player" for exactly this
        // reason. Omitting -level lets libx264 pick the correct minimum
        // level for the actual output automatically.
        '-vf', scaleFilter
      );
    }
  }

  if (!dropAudio && info.audioStreamIndexes.length > 0) {
    if (audioMode === 'copy') {
      args.push('-c:a', 'copy');
    } else {
      // Applies to every mapped audio output stream — each of a multi-
      // track source gets transcoded to stereo AAC independently.
      args.push('-c:a', 'aac', '-b:a', '128k', '-ac', '2');
    }
    args.push('-disposition:a:0', 'default');
  } else {
    args.push('-an');
  }

  if (includeSubtitles && info.subtitleStreamIndexes.length > 0) {
    args.push('-c:s', 'mov_text');
  }

  args.push('-movflags', '+faststart');
  args.push(outputPath);
  return args;
}

/**
 * Confirms a finished FFmpeg run actually produced a working, Telegram-
 * playable MP4 on disk — not just that the process exited 0. Checks (in
 * order): the file exists and isn't suspiciously tiny, ffprobe can
 * actually open it (a file with a truncated/missing moov atom fails
 * right here), it contains a real video stream, it reports a sane (>0)
 * duration, AND — the check that used to be missing — the video is
 * actually H.264 and the audio (if any) is actually AAC. A stream-copy
 * tier can only ever produce those codecs by construction (buildFfmpegArgs
 * only allows `-c:v copy` when analyze() already confirmed h264/aac), but
 * this re-verifies it directly against the file that's actually about to
 * be uploaded rather than trusting that chain of assumptions — codec
 * mismatches are exactly what make Telegram fall back to "Use external
 * video player" while still showing a normal-looking .mp4 file. Also
 * used by the upload pipeline as a final gate immediately before upload,
 * and to decide whether to fall back to uploading the original source
 * file instead of a broken/incompatible compressed one.
 */
async function verifyOutputFile(outputPath) {
  let stat;
  try {
    stat = fs.statSync(outputPath);
  } catch (err) {
    return { valid: false, reason: `output file is missing on disk: ${err.message}` };
  }
  if (!stat.size || stat.size < 1024) {
    return { valid: false, reason: `output file is suspiciously small (${stat.size} bytes) — likely truncated` };
  }

  let meta;
  try {
    meta = await probe(outputPath);
  } catch (err) {
    return { valid: false, reason: `ffprobe could not read the output (likely a corrupt/incomplete file): ${err.message}` };
  }

  const streams = meta.streams || [];
  const videoStream = pickVideoStream(streams);
  const audioStreams = pickAudioStreams(streams);
  const duration = Number(meta.format?.duration) || 0;

  if (!videoStream) {
    return { valid: false, reason: 'output has no video stream' };
  }
  if (!(duration > 0)) {
    return { valid: false, reason: 'output reports zero/invalid duration (likely a truncated or missing moov atom)' };
  }
  const outVideoCodec = (videoStream.codec_name || '').toLowerCase();
  if (outVideoCodec !== 'h264') {
    return { valid: false, reason: `output video codec is "${outVideoCodec || 'unknown'}", not H.264 — Telegram's inline player will reject this` };
  }
  const outPixFmt = (videoStream.pix_fmt || '').toLowerCase();
  if (outPixFmt && !/^yuvj?420p$/.test(outPixFmt)) {
    return { valid: false, reason: `output pixel format is "${outPixFmt}", not yuv420p — many decoders (including Telegram's inline player) reject 4:2:2/4:4:4/10-bit output` };
  }
  if (!(videoStream.width > 0) || !(videoStream.height > 0)) {
    return { valid: false, reason: `output reports an invalid resolution (${videoStream.width || 0}x${videoStream.height || 0})` };
  }
  const badAudio = audioStreams.find((s) => (s.codec_name || '').toLowerCase() !== 'aac');
  if (badAudio) {
    return { valid: false, reason: `output has a non-AAC audio track ("${badAudio.codec_name || 'unknown'}") — Telegram's inline player will reject this` };
  }
  const frameRate = parseFrameRate(videoStream.avg_frame_rate) || parseFrameRate(videoStream.r_frame_rate);
  if (!(frameRate > 0)) {
    return { valid: false, reason: `output reports an invalid/zero frame rate ("${videoStream.avg_frame_rate || videoStream.r_frame_rate || 'unknown'}")` };
  }
  const faststart = checkFaststart(outputPath);
  if (!faststart.ok) {
    return { valid: false, reason: `output is not faststart (moov atom is not before mdat: ${faststart.reason}) — many players won't start playback until the whole file downloads` };
  }

  return {
    valid: true,
    sizeBytes: stat.size,
    duration: Math.round(duration),
    width: videoStream.width || 0,
    height: videoStream.height || 0,
    frameRate,
  };
}

function parseFrameRate(raw) {
  if (!raw || typeof raw !== 'string') return 0;
  const [num, den] = raw.split('/').map(Number);
  if (!num || !den) return 0;
  return num / den;
}

/**
 * Reads just enough of the MP4's top-level box structure to confirm
 * `moov` appears before `mdat` — the actual, structural definition of
 * "faststart". `-movflags +faststart` normally guarantees this, but
 * FFmpeg can silently fail to apply it in a handful of edge cases (some
 * odd stream-copy combinations, certain muxer/codec pairings), and
 * nothing was ever actually checking that the flag took effect. This is
 * a real, on-disk verification instead of trusting the command-line flag
 * was honored. MP4 boxes are simply [4-byte size][4-byte fourcc][data...]
 * at the top level, so this only needs to walk the top-level box list —
 * no full MP4 parser required.
 */
function checkFaststart(filePath) {
  const fd = fs.openSync(filePath, 'r');
  try {
    const stat = fs.fstatSync(fd);
    let offset = 0;
    const buf = Buffer.alloc(8);
    // Cap how far we scan — a well-formed faststart file has moov within
    // the first handful of boxes; if we haven't seen either box within a
    // few MB, something's structurally unusual and we bail out honestly
    // rather than scanning gigabytes of mdat looking for a moov that
    // isn't there (i.e. not-faststart), or spinning forever.
    const scanLimit = Math.min(stat.size, 8 * 1024 * 1024);
    while (offset < scanLimit) {
      const bytesRead = fs.readSync(fd, buf, 0, 8, offset);
      if (bytesRead < 8) break;
      let boxSize = buf.readUInt32BE(0);
      const fourcc = buf.toString('ascii', 4, 8);
      if (boxSize === 1) {
        // 64-bit extended size — read the next 8 bytes.
        const bigBuf = Buffer.alloc(8);
        fs.readSync(fd, bigBuf, 0, 8, offset + 8);
        const big = bigBuf.readBigUInt64BE(0);
        boxSize = Number(big);
      }
      if (fourcc === 'moov') return { ok: true };
      if (fourcc === 'mdat') return { ok: false, reason: 'mdat encountered before moov' };
      if (!(boxSize > 0)) return { ok: false, reason: `unreadable box size at offset ${offset}` };
      offset += boxSize;
    }
    return { ok: false, reason: 'moov box not found within the first 8MB — file is not faststart' };
  } catch (err) {
    return { ok: false, reason: `could not inspect box structure: ${err.message}` };
  } finally {
    fs.closeSync(fd);
  }
}

/**
 * Extracts a single JPEG frame to use as the video's Telegram thumbnail.
 * Telegram's own server-side auto-thumbnail generation for MTProto
 * uploads isn't fully reliable — it can silently fail or time out, which
 * looks exactly like a video that got sent as a plain attachment (no
 * preview, no play button) even when it's correctly tagged as video
 * media. Supplying a real thumbnail ourselves removes that dependency
 * entirely. Failure here is never fatal to the upload — callers should
 * treat a thrown/rejected result as "skip the thumbnail", not "fail the
 * whole item".
 */
async function generateThumbnail(videoPath, outputJpgPath, durationSeconds) {
  // A frame ~10% into the video (capped at 3s) is far more likely to be a
  // real representative frame than 0:00, which is frequently a black/
  // fade-in frame for movies and openings.
  const seekSeconds = durationSeconds > 1 ? Math.min(3, Math.max(0, Math.floor(durationSeconds * 0.1))) : 0;
  const args = [
    '-y',
    '-ss', String(seekSeconds),
    '-i', videoPath,
    '-frames:v', '1',
    '-vf', 'scale=320:-2',
    '-q:v', '4',
    outputJpgPath,
  ];
  await runFfmpeg(args, null, 0);

  const stat = fs.statSync(outputJpgPath);
  if (!stat.size) {
    throw new Error('Thumbnail generation produced an empty file');
  }
  return outputJpgPath;
}

function cleanupTierOutput(outputPath) {
  try { fs.unlinkSync(outputPath); } catch (err) { if (err.code !== 'ENOENT') log.warn('cleanupTierOutput', 'Failed to remove invalid output file', { outputPath, reason: err.message }); }
}

/**
 * Converts a downloaded source file — MP4, MKV, WebM, AVI, MOV, MPEG-TS,
 * FLV, or anything else FFmpeg can demux — into a browser-streamable MP4
 * (H.264/AAC) at outputPath. Automatically detects the container and
 * codecs (never assumes MP4/H.264 going in), preserves orientation and
 * aspect ratio, keeps text-based subtitles when present, carries every
 * audio track through, and steps through progressively more conservative
 * fallbacks if FFmpeg rejects an attempt, instead of failing outright.
 *
 * Returns the probed source info (plus the strategy actually used, which
 * may differ from the initial plan after a fallback).
 */
async function processFile(inputPath, outputPath, onProgress) {
  const info = await analyze(inputPath);
  const hwEncoder = await detectHardwareEncoder();
  let inputSizeBytes = null;
  try { inputSizeBytes = fs.statSync(inputPath).size; } catch (_) { /* logged as null below, not fatal */ }
  log.info('processFile', 'Compression starting', {
    inputPath, outputPath, inputSizeBytes, mode: info.mode, container: info.container,
    videoCodec: info.videoCodec, audioCodec: info.audioCodec,
    audioTracks: info.audioStreamIndexes.length,
    subtitleTracks: info.subtitleStreamIndexes.length, droppedSubtitles: info.droppedSubtitleCount,
    durationSeconds: info.duration, rotation: info.rotation,
    resolution: `${info.width}x${info.height}`,
    hwEncoder: hwEncoder ? hwEncoder.name : null,
  });

  const videoMode = info.videoOk ? 'copy' : 'encode';
  const audioMode = info.audioOk ? 'copy' : 'encode';
  const hasSubs = info.subtitleStreamIndexes.length > 0;
  const hasAudio = info.audioStreamIndexes.length > 0;

  // Ordered fallback tiers — try the cheapest plan that should work
  // first, and only fall back to a more conservative one (dropping
  // subtitles, then forcing a full re-encode, then finally dropping
  // audio outright as a last resort) if FFmpeg actually rejects the
  // previous attempt. Every tier still produces a real, correctly
  // demuxed H.264 MP4 — this never silently serves the raw/broken
  // source file.
  const tiers = [];
  tiers.push({
    label: `${info.mode}${hasSubs ? '+subs' : ''}`,
    videoMode, audioMode, includeSubtitles: hasSubs,
  });
  if (hasSubs) {
    tiers.push({ label: info.mode, videoMode, audioMode, includeSubtitles: false });
  }
  if (videoMode === 'copy' || audioMode === 'copy') {
    tiers.push({ label: 'transcode+subs', videoMode: 'encode', audioMode: 'encode', includeSubtitles: hasSubs });
    if (hasSubs) {
      tiers.push({ label: 'transcode', videoMode: 'encode', audioMode: 'encode', includeSubtitles: false });
    }
  }
  if (hasAudio) {
    tiers.push({
      label: 'transcode-video-only',
      videoMode: 'encode', audioMode: 'encode', includeSubtitles: false, dropAudio: true,
    });
  }

  let lastErr;
  for (let i = 0; i < tiers.length; i++) {
    const tier = tiers[i];
    // Only a real re-encode tier benefits from a hardware encoder — a
    // stream-copy tier's -c:v copy is already free. When one's available,
    // try it first and fall back to the proven CPU (libx264) path for
    // this SAME tier if it fails, before ever giving up on the tier
    // itself — a broken/missing GPU driver should only cost some speed,
    // never correctness or quality.
    const encoderChoices = tier.videoMode === 'encode' && hwEncoder ? [hwEncoder, null] : [null];
    let tierResult = null;

    for (let ei = 0; ei < encoderChoices.length; ei++) {
      const encoderChoice = encoderChoices[ei];
      try {
        const args = buildFfmpegArgs(inputPath, outputPath, tier, info, encoderChoice);
        await runFfmpeg(args, onProgress, info.duration);

        // FFmpeg exiting 0 only means the process didn't crash — it does
        // NOT guarantee a playable file (a killed disk, a race with disk
        // space, or an edge-case source can still produce a file with no
        // moov atom / zero duration). Re-probe the actual output on disk
        // before trusting this attempt, so a broken file is caught here and
        // retried under the next fallback tier instead of reaching the
        // upload step and shipping something that shows a duration but
        // never plays.
        const verified = await verifyOutputFile(outputPath);
        if (!verified.valid) {
          // Never leave an invalid file sitting on disk where a later step
          // could accidentally pick it up — delete immediately, then either
          // the next (more conservative) tier retries from scratch or, if
          // this was the last tier, processFile() throws and the caller
          // fails the item outright instead of falling back to shipping it.
          cleanupTierOutput(outputPath);
          throw new Error(`FFmpeg exited cleanly but the output failed verification: ${verified.reason}`);
        }

        if (tier.dropAudio) {
          log.warn('processFile', 'Compression succeeded but audio had to be dropped — source audio track(s) were not decodable/encodable', {
            inputPath, outputPath,
          });
        }
        log.success('processFile', 'Compression completed', {
          inputPath, outputPath, strategy: tier.label,
          encoder: encoderChoice ? encoderChoice.name : 'libx264 (CPU)',
          attempt: i + 1, totalAttempts: tiers.length,
          inputSizeBytes, outputSizeBytes: verified.sizeBytes, outputDurationSeconds: verified.duration,
        });
        tierResult = {
          ...info,
          mode: tier.label,
          subtitlesIncluded: !!tier.includeSubtitles,
          audioDropped: !!tier.dropAudio,
          outputSizeBytes: verified.sizeBytes,
        };
        break; // this tier succeeded — the CPU fallback entry (if any) is skipped
      } catch (err) {
        lastErr = err;
        const hasMoreEncoders = ei + 1 < encoderChoices.length;
        const hasMoreTiers = i + 1 < tiers.length;
        if (encoderChoice) {
          log.warn('processFile', `Hardware encoder ${encoderChoice.name} failed for tier "${tier.label}" — retrying this tier on CPU (libx264)`, {
            inputPath, reason: err.message,
          });
        } else {
          log.warn('processFile', `Compression attempt failed (${tier.label}) — ${hasMoreTiers ? 'trying next fallback' : 'no fallback strategies left'}`, {
            inputPath, attempt: i + 1, totalAttempts: tiers.length, reason: err.message,
          });
        }
        if (!hasMoreEncoders) break; // exhausted this tier's encoder choices — the outer loop moves to the next tier
      }
    }

    if (tierResult) return tierResult;
  }

  log.error('processFile', 'All compression strategies exhausted', lastErr, { inputPath, outputPath, attempts: tiers.length });
  cleanupTierOutput(outputPath); // belt-and-suspenders — every tier already cleans up its own failed attempt, but never leave a stray file behind on the final failure either
  throw new Error(`FFmpeg compression failed after ${tiers.length} attempt(s): ${lastErr ? lastErr.message : 'unknown error'}`);
}

module.exports = { probe, analyze, processFile, verifyOutputFile, generateThumbnail, ensureAvailable, detectHardwareEncoder };
