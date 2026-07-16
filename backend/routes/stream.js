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
const { requireDocId, ApiValidationError, clampInt } = require('../utils/validators');
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

    if (range) {
      const match = /bytes=(\d*)-(\d*)/.exec(range);
      if (!match) {
        log.warn('fileStream', 'Malformed Range header', { videoId, range, userAgent });
        return res.status(416).set('Content-Range', `bytes */${fileSize}`).end();
      }
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

    // Cap the served chunk so one Range request can't hold a socket open
    // for the entire remainder of a multi-GB file.
    if (end - start + 1 > MAX_CHUNK_BYTES) end = start + MAX_CHUNK_BYTES - 1;

    // Weak ETag tied to the video's actual source identity — automatically
    // changes if playbackCompat swaps in a transcoded copy (different
    // messageId), so browsers never serve a stale cached range from
    // before a fix was applied.
    const etag = `W/"${videoId}-${video.channelId}-${video.messageId}-${fileSize}"`;
    res.status(range ? 206 : 200);
    res.set({
      'Content-Type': mimeType,
      'Accept-Ranges': 'bytes',
      'Content-Length': end - start + 1,
      // Byte ranges for a given channelId+messageId never change, so the
      // same browser can safely reuse them across a seek/scrub within one
      // session — cuts redundant re-fetches without risking stale bytes
      // (the ETag changes the moment the underlying source does).
      'Cache-Control': 'private, max-age=3600, immutable',
      ETag: etag,
      'X-Video-Compat': video.playbackCompatible ? 'compatible' : (video.transcoding ? 'transcoding' : 'unverified'),
    });
    if (range) res.set('Content-Range', `bytes ${start}-${end}/${fileSize}`);

    if (req.method === 'HEAD') return res.end();

    let abortHandler = null;
    req.on('close', () => { if (abortHandler) abortHandler(); });

    await mtproto.streamRange(video.channelId, video.messageId, start, end, res, {
      onAbort: (fn) => { abortHandler = fn; },
    });

    // streamRange only resolves once it has written exactly the number of
    // bytes promised in Content-Length (or the client disconnected, in
    // which case ending an already-closing socket is a harmless no-op).
    // Only res.end() here, on the verified-good path.
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
 * which is frequently wrong or generic (application/octet-stream is
 * common). Rather than blindly trusting or blindly overriding it, map
 * known container signatures (from the mime OR the file extension) to
 * their correct standard type, and only fall back to video/mp4 as a last
 * resort. Mislabeling a real MKV as "video/mp4" doesn't fix playback —
 * it just hides the problem from the debugging logs — so this stays
 * honest; services/playbackCompat.js is what actually fixes playback.
 */
function normalizeMimeType(rawMime, fileName) {
  const mime = (rawMime || '').toLowerCase();
  const name = (fileName || '').toLowerCase();

  if (mime.startsWith('video/') && mime !== 'video/octet-stream') return rawMime;

  if (/\.mkv$/.test(name) || /matroska/.test(mime)) return 'video/x-matroska';
  if (/\.webm$/.test(name) || /webm/.test(mime)) return 'video/webm';
  if (/\.mov$/.test(name) || /quicktime/.test(mime)) return 'video/quicktime';
  if (/\.avi$/.test(name) || /x-msvideo/.test(mime)) return 'video/x-msvideo';
  if (/\.(m4v|mp4)$/.test(name)) return 'video/mp4';
  return 'video/mp4'; // safest default — most sources are already MP4-in-disguise
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
    const docId = `${userId}_${videoId}`;

    const { setDoc } = require('../services/firebase');
    await setDoc('continueWatching', docId, {
      userId, videoId, position, duration, progressPercent,
      completed: progressPercent >= 90,
      watchedAt: new Date().toISOString(),
    });

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
                                                                          
