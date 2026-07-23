/**
 * routes/stream.js
 * GET  /api/stream/:videoId            — resolves a proxy stream URL (JSON,
 *                                         same contract the frontend already
 *                                         expects: { streamUrl, video })
 * GET  /api/stream/file/:videoId       — the actual byte-range video proxy.
 *                                         <video src> hits this directly;
 *                                         supports Range/seek/pause/resume
 *                                         for files up to 4GB without ever
 *                                         buffering the whole file.
 * POST /api/stream/:videoId/progress   — save watch progress
 * GET  /api/stream/:videoId/progress   — get saved progress (resume)
 *
 * The frontend cannot be modified, and a <video> element cannot send custom
 * headers — so the resolver embeds the same Telegram initData it already
 * validated into the streamUrl's query string, and the file endpoint
 * re-validates it with the same auth logic (which already supports reading
 * initData from the query string).
 *
 * ---------------------------------------------------------------------
 * FIX (frontend showed "Missing authentication" on every play attempt):
 * the resolver and file routes previously used requireAuth, which hard-
 * rejects with 401 whenever window.Telegram.WebApp.initData is empty -
 * which it legitimately is any time the Mini App is opened outside a real
 * Telegram launch context (e.g. previewing the Hosting URL directly in a
 * browser during testing). Every other content route in this backend
 * (videos.js, categories.js, search.js) uses softAuth instead - it
 * validates and attaches the Telegram user when initData IS present, but
 * never hard-blocks the request when it's absent. stream.js was the only
 * route family enforcing the strict version, which is why browsing worked
 * fine but every Play tap failed. Switched the resolver and file routes to
 * softAuth to match the rest of the API's security posture. The two
 * per-user progress endpoints (save/get watch position) still require a
 * real Telegram user via requireAuth, since they're keyed by
 * telegramUserId and are meaningless without one - the frontend already
 * tolerates that failing gracefully (Api.getProgress(...).catch(() =>
 * ({position:0}))), so this doesn't reintroduce the bug.
 */
'use strict';

const express = require('express');
const rateLimit = require('express-rate-limit');
const router = express.Router();

const { getDoc, updateDoc, addDoc } = require('../services/firebase');
const mtproto = require('../services/mtproto');
const playbackCompat = require('../services/playbackCompat');
const { requireAuth, softAuth } = require('../middleware/auth');
const { requireDocId, ApiValidationError, clampInt, slugify } = require('../utils/validators');
const { makeLogger } = require('../utils/logger');
const log = makeLogger('routes/stream.js');

// Range requests fire constantly while seeking/buffering — give the raw
// file endpoint a much more generous limiter than the rest of the API.
const fileLimiter = rateLimit({ windowMs: 60_000, max: 600, standardHeaders: true, legacyHeaders: false });
const resolverLimiter = rateLimit({ windowMs: 60_000, max: 40, standardHeaders: true, legacyHeaders: false });

// Never let a single Range response run away with the whole remainder of
// a multi-GB file — cap it, so players are forced to issue follow-up
// Range requests. This bounds per-request memory/socket lifetime and is
// what keeps many simultaneous viewers stable on Render's limited RAM.
const MAX_CHUNK_BYTES = 8 * 1024 * 1024; // 8MB per response

router.get('/:videoId', resolverLimiter, softAuth, async (req, res) => {
  try {
    const videoId = requireDocId(req.params.videoId, 'videoId');
    const video = await getDoc('videos', videoId);
    if (!video) return res.status(404).json({ error: 'Video not found' });
    if (!video.published) return res.status(403).json({ error: 'Video not available' });
    if (!video.channelId || !video.messageId) {
      return res.status(500).json({ error: 'Video source is not configured' });
    }

    const initData = req.headers['x-telegram-init-data'] || req.query.initData || '';
    const origin = `${req.protocol}://${req.get('host')}`;
    const streamUrl = `${origin}/api/stream/file/${videoId}${initData ? `?initData=${encodeURIComponent(initData)}` : ''}`;

    if (req.telegramUserId) {
      logWatch(req.telegramUserId, videoId, video.title);
    }
    updateDoc('videos', videoId, { views: (video.views || 0) + 1 }).catch((err) => log.warn('viewCounter', 'Failed to bump views', { videoId, reason: err.message }));

    res.json({
      streamUrl,
      video: { id: videoId, title: video.title, duration: video.duration, mimeType: video.mimeType || 'video/mp4' },
      expiresIn: 21600, // matches the ~24h initData validity window, session-scoped
    });
  } catch (err) {
    if (err instanceof ApiValidationError) return res.status(err.status).json({ error: err.message });
    log.error('resolve', 'Failed to resolve stream URL', err, { videoId: req.params.videoId });
    res.status(500).json({ error: 'Failed to get stream URL' });
  }
});

router.get('/file/:videoId', fileLimiter, softAuth, async (req, res) => {
  const videoId = req.params.videoId;
  const userAgent = req.headers['user-agent'] || 'unknown';
  const range = req.headers.range;

  try {
    requireDocId(videoId, 'videoId');

    if (!mtproto.isEnabled()) {
      return res.status(501).json({ error: 'Streaming is not configured on this server (missing MTProto credentials).' });
    }

    const video = await getDoc('videos', videoId);
    if (!video) return res.status(404).json({ error: 'Video not found' });
    if (!video.published) return res.status(403).json({ error: 'Video not available' });
    if (!video.channelId || !video.messageId) {
      return res.status(500).json({ error: 'Video source is not configured' });
    }

    let source;
    try {
      source = await mtproto.resolveVideoSource(video.channelId, video.messageId);
    } catch (err) {
      if (err instanceof mtproto.SourceNotFoundError) {
        log.warn('fileStream', 'Source message missing/corrupted — flagging document', { videoId, channelId: video.channelId, messageId: video.messageId, reason: err.message });
        updateDoc('videos', videoId, { sourceMissing: true }).catch(() => {});
        return res.status(404).json({ error: 'Source video is no longer available on Telegram.' });
      }
      throw err;
    }

    const fileSize = video.fileSizeBytes || source.size;
    if (!video.fileSizeBytes || video.mimeType !== source.mimeType) {
      updateDoc('videos', videoId, { fileSizeBytes: source.size, mimeType: source.mimeType }).catch(() => {});
    }

    const mimeType = normalizeMimeType(source.mimeType || video.mimeType, source.fileName);
    const resolution = extractResolution(source.doc);

    // Never blocks this request — if the container/codec looks unsafe for
    // inline HTML5 playback, this schedules a one-time background
    // transcode (services/playbackCompat.js) that swaps the doc's
    // channelId/messageId over to a guaranteed H.264/AAC/MP4 copy. This
    // request still serves whatever's there right now, best effort.
    playbackCompat.scheduleCompatibilityCheck(videoId, video, source);

    log.info('fileStream', 'Stream request', {
      videoId, mimeType, container: extractContainerGuess(mimeType, source.fileName), resolution,
      range: range || 'none', userAgent, playbackCompatible: !!video.playbackCompatible, transcoding: !!video.transcoding,
    });

    let start = 0;
    let end = fileSize - 1;
    let hadRangeHeader = false;

    if (range) {
      const match = /^bytes=(\d*)-(\d*)$/.exec(range.trim());
      if (!match || (match[1] === '' && match[2] === '')) {
        log.warn('fileStream', 'Malformed Range header', { videoId, range, userAgent });
        return res.status(416).set('Content-Range', `bytes */${fileSize}`).end();
      }
      hadRangeHeader = true;
      const rangeStart = match[1] ? parseInt(match[1], 10) : null;
      const rangeEnd = match[2] ? parseInt(match[2], 10) : null;

      if (rangeStart === null && rangeEnd !== null) {
        // suffix range: last N bytes
        start = Math.max(0, fileSize - rangeEnd);
        end = fileSize - 1;
      } else {
        start = rangeStart || 0;
        end = rangeEnd !== null ? Math.min(rangeEnd, fileSize - 1) : fileSize - 1;
      }

      if (start >= fileSize || start > end || start < 0) {
        log.warn('fileStream', 'Range not satisfiable', { videoId, range, fileSize, httpStatus: 416 });
        return res.status(416).set('Content-Range', `bytes */${fileSize}`).end();
      }
    }

    // Cap how much a SINGLE Range response can hand back so one request
    // can't hold a socket open for the entire remainder of a multi-GB
    // file — a client that gets less than it asked for in a 206 is
    // required by HTTP to just issue another Range request for the rest,
    // and every real player (Chrome/Firefox/Safari/ExoPlayer/AVPlayer)
    // does exactly that.
    //
    // Crucially this ONLY applies when the client actually sent a Range
    // header. A plain GET with no Range means "give me the whole
    // resource" — responding 200 with a Content-Length far smaller than
    // fileSize would tell the browser that truncated blob IS the entire
    // video. For anything bigger than MAX_CHUNK_BYTES that silently
    // clips off the end of the file (or, for a non-faststart MP4 whose
    // moov atom lives at the end, clips off the ONLY part that lets the
    // browser parse the container at all) — which is exactly the "plays
    // audio, corrupt/black video" failure mode this fix targets.
    if (hadRangeHeader && end - start + 1 > MAX_CHUNK_BYTES) {
      end = start + MAX_CHUNK_BYTES - 1;
    }

    // Weak ETag tied to the video's actual source identity — automatically
    // changes if playbackCompat swaps in a transcoded copy (different
    // messageId), so browsers never serve a stale cached range from
    // before a fix was applied.
    const etag = `W/"${videoId}-${video.channelId}-${video.messageId}-${fileSize}"`;
    const contentLength = end - start + 1;
    res.status(hadRangeHeader ? 206 : 200);
    res.set({
      'Content-Type': mimeType,
      'Accept-Ranges': 'bytes',
      'Content-Length': contentLength,
      // Byte ranges for a given channelId+messageId never change, so the
      // same browser can safely reuse them across a seek/scrub within one
      // session — cuts redundant re-fetches without risking stale bytes
      // (the ETag changes the moment the underlying source does).
      'Cache-Control': 'private, max-age=3600, immutable',
      ETag: etag,
      'X-Video-Compat': video.playbackCompatible ? 'compatible' : (video.transcoding ? 'transcoding' : 'unverified'),
    });
    if (hadRangeHeader) res.set('Content-Range', `bytes ${start}-${end}/${fileSize}`);

    log.info('fileStream', 'Serving request', {
      videoId, channelId: video.channelId, messageId: video.messageId,
      requestedRange: range || 'none', startOffset: start, endOffset: end, expectedBytes: contentLength,
      httpStatus: hadRangeHeader ? 206 : 200, mimeType, userAgent,
    });

    if (req.method === 'HEAD') return res.end();

    // Flush the header block to the socket immediately, before any video
    // bytes are written — some HTTP/1.1 clients (notably a few Android
    // WebViews and the Telegram Mini App's embedded browser) won't start
    // buffering/decoding the body until they've seen a complete header
    // frame arrive as its own TCP write.
    res.flushHeaders();

    let abortHandler = null;
    req.on('close', () => { if (abortHandler) abortHandler(); });

    let streamResult;
    try {
      streamResult = await mtproto.streamRange(video.channelId, video.messageId, start, end, res, {
        onAbort: (fn) => { abortHandler = fn; },
        videoId,
      });
    } catch (err) {
      log.error('fileStream', 'streamRange failed', err, {
        videoId, channelId: video.channelId, messageId: video.messageId,
        startOffset: start, endOffset: end, expectedBytes: contentLength, finalStatus: 'ERROR',
      });
      throw err;
    }

    if (streamResult?.aborted) {
      log.info('fileStream', 'Client disconnected mid-stream', {
        videoId, totalBytesWritten: streamResult.bytesSent, expectedBytes: contentLength, finalStatus: 'ABORTED',
      });
      return; // socket is already gone or going — nothing left to verify or end()
    }

    // Final integrity gate: never call res.end() unless exactly the
    // number of bytes promised in Content-Length were actually written.
    // streamRange() already throws on a short count, but this is a
    // second, cheap belt-and-suspenders check right at the boundary
    // where we're about to close out the HTTP response.
    if (streamResult.bytesSent !== contentLength) {
      log.error('fileStream', 'Refusing to end response — bytesWritten does not match Content-Length', null, {
        videoId, totalBytesWritten: streamResult.bytesSent, expectedBytes: contentLength, finalStatus: 'INTEGRITY_MISMATCH',
      });
      if (!res.writableEnded) res.destroy(new Error('bytesWritten mismatch against Content-Length'));
      return;
    }

    log.success('fileStream', 'Request completed', {
      videoId, totalBytesWritten: streamResult.bytesSent, expectedBytes: contentLength, finalStatus: 'SUCCESS',
    });
    if (!res.writableEnded) res.end();
  } catch (err) {
    log.error('fileStream', 'Streaming failed', err, {
      videoId, range: range || 'none', userAgent, httpStatus: res.headersSent ? res.statusCode : null, stack: err.stack,
    });
    if (!res.headersSent) {
      if (err instanceof mtproto.MTProtoDisabledError) return res.status(501).json({ error: err.message });
      if (err instanceof ApiValidationError) return res.status(err.status).json({ error: err.message });
      return res.status(500).json({ error: 'Streaming failed' });
    }
    // Headers (and a Content-Length promise) were already sent - calling
    // res.end() here would silently deliver a body shorter than what we
    // already told the browser to expect, which is exactly the "looks
    // complete but isn't" response that breaks HTML5 <video> playback.
    // Destroy the connection instead so the client sees a hard failure.
    if (!res.writableEnded) res.destroy(err);
  }
});
router.head('/file/:videoId', fileLimiter, softAuth, (req, res, next) => {
  req.method = 'HEAD';
  return router.handle(req, res, next);
});

// ── Content-Type / debug helpers ──────────────────────────────────────

/**
 * Telegram echoes back whatever mime type the uploading client declared,
 * which is frequently wrong (a very common case: an MKV/WebM file was
 * uploaded and Telegram's client still reported "video/mp4" because
 * that's what the uploader's app defaulted to). Trusting that label at
 * face value is exactly what causes the "audio plays, video looks
 * corrupt" symptom: the browser demuxer is told to parse an MP4 box
 * structure against what's actually a Matroska/WebM EBML stream (or
 * vice versa) and fails on the video track while sometimes still
 * managing to sniff out an audio stream.
 *
 * So the filename extension — the most reliable signal Telegram gives
 * us, since it's just the literal name the file was uploaded with — is
 * checked FIRST and wins on conflict. The declared mime is only trusted
 * when the filename doesn't clearly say otherwise. The original
 * container is always preserved (MKV stays video/x-matroska, WebM stays
 * video/webm, etc.) — never forced to video/mp4 just because that's the
 * common case.
 */
function normalizeMimeType(rawMime, fileName) {
  const mime = (rawMime || '').toLowerCase();
  const name = (fileName || '').toLowerCase();

  // 1. Filename extension — most reliable, checked first, wins on conflict.
  if (/\.mkv$/.test(name)) return 'video/x-matroska';
  if (/\.webm$/.test(name)) return 'video/webm';
  if (/\.mov$/.test(name)) return 'video/quicktime';
  if (/\.avi$/.test(name)) return 'video/x-msvideo';
  if (/\.(m4v|mp4)$/.test(name)) return 'video/mp4';
  if (/\.ts$/.test(name) || /\.m2ts$/.test(name)) return 'video/mp2t';
  if (/\.flv$/.test(name)) return 'video/x-flv';

  // 2. No usable filename hint — fall back to whatever Telegram declared,
  //    as long as it's a real, specific video container type.
  if (/matroska/.test(mime)) return 'video/x-matroska';
  if (/webm/.test(mime)) return 'video/webm';
  if (/quicktime/.test(mime)) return 'video/quicktime';
  if (/x-msvideo/.test(mime)) return 'video/x-msvideo';
  if (mime.startsWith('video/') && mime !== 'video/octet-stream') return rawMime;

  // 3. No reliable signal at all — mp4 is the safest last-resort default.
  return 'video/mp4';
}

function extractContainerGuess(mimeType, fileName) {
  const ext = (fileName || '').split('.').pop();
  if (ext && ext.length <= 4) return ext.toLowerCase();
  return (mimeType || '').split('/')[1] || 'unknown';
}

/** Pulls width/height straight out of Telegram's own document attributes — no download/ffprobe needed just for a debug log line. */
function extractResolution(doc) {
  const attr = (doc?.attributes || []).find((a) => a.className === 'DocumentAttributeVideo');
  if (!attr) return 'unknown';
  return `${attr.w || '?'}x${attr.h || '?'}`;
}

router.post('/:videoId/progress', requireAuth, async (req, res) => {
  try {
    const videoId = requireDocId(req.params.videoId, 'videoId');
    const userId = req.telegramUserId;
    const position = Number(req.body.position);
    const duration = Number(req.body.duration) || 0;

    if (!Number.isFinite(position) || position < 0) {
      return res.status(400).json({ error: 'Invalid position' });
    }

    const progressPercent = duration ? Math.round((position / duration) * 100) : 0;
    const completed = progressPercent >= 90;
    const watchedAt = new Date().toISOString();
    const docId = `${userId}_${videoId}`;

    const { setDoc } = require('../services/firebase');
    // Per-EPISODE resume position — keyed by the exact video, used only by
    // GET /:videoId/progress below to restore playback of THIS episode.
    await setDoc('continueWatching', docId, {
      userId, videoId, position, duration, progressPercent,
      completed,
      watchedAt,
    });

    // ---- Continue Watching (home row) summary ----------------------------
    // FIX for "shows Episode 12 even though only Episode 1 was watched":
    // the old design keyed this summary per-episode too, so watching
    // episode 1 and later episode 3 of the same anime created TWO separate
    // rows instead of updating one — whichever had the more recent
    // watchedAt could win the sort, showing a stale/unexpected episode.
    // Keying this by SERIES (userId + a stable slug of seriesTitle/title)
    // means any episode of the same anime always overwrites the single
    // existing entry with the real season/episode/position/duration/
    // timestamp just watched — never a duplicate, never a wrong episode.
    try {
      const video = await getDoc('videos', videoId);
      if (video) {
        const seriesKey = slugify(video.seriesTitle || video.title || videoId);
        const seriesDocId = `${userId}_${seriesKey}`;
        await setDoc('continueWatchingSeries', seriesDocId, {
          userId,
          seriesKey,
          videoId,
          title: video.title || '',
          seriesTitle: video.seriesTitle || video.title || '',
          season: video.season != null ? Number(video.season) : null,
          episode: video.episode != null ? Number(video.episode) : null,
          position, duration, progressPercent,
          completed,
          watchedAt,
        });
      }
    } catch (seriesErr) {
      // Never let the summary-row update fail the actual progress save —
      // the per-episode resume position above already succeeded.
      log.warn('saveProgress', 'Failed to update Continue Watching series summary', { videoId, reason: seriesErr.message });
    }

    res.json({ saved: true, progressPercent });
  } catch (err) {
    if (err instanceof ApiValidationError) return res.status(err.status).json({ error: err.message });
    log.error('saveProgress', 'Failed to save progress', err, { videoId: req.params.videoId });
    res.status(500).json({ error: 'Failed to save progress' });
  }
});

router.get('/:videoId/progress', requireAuth, async (req, res) => {
  try {
    const videoId = requireDocId(req.params.videoId, 'videoId');
    const docId = `${req.telegramUserId}_${videoId}`;
    const progress = await getDoc('continueWatching', docId);
    res.json({
      position: progress?.position || 0,
      duration: progress?.duration || 0,
      progressPercent: progress?.progressPercent || 0,
      completed: progress?.completed || false,
    });
  } catch (err) {
    if (err instanceof ApiValidationError) return res.status(err.status).json({ error: err.message });
    res.status(500).json({ error: 'Failed to get progress' });
  }
});

async function logWatch(userId, videoId, title) {
  try {
    await addDoc('history', { userId, videoId, title, watchedAt: new Date().toISOString() });
  } catch (err) {
    log.warn('logWatch', 'Failed to log history', { videoId, reason: err.message });
  }
}

module.exports = router;
                                                                          
