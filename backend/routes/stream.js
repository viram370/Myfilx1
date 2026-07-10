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
 * re-validates it with the same requireAuth logic (which already supports
 * reading initData from the query string).
 */
'use strict';

const express = require('express');
const rateLimit = require('express-rate-limit');
const router = express.Router();

const { getDoc, updateDoc, addDoc } = require('../services/firebase');
const mtproto = require('../services/mtproto');
const { requireAuth } = require('../middleware/auth');
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

router.get('/:videoId', resolverLimiter, requireAuth, async (req, res) => {
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

    logWatch(req.telegramUserId, videoId, video.title);
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

router.get('/file/:videoId', fileLimiter, requireAuth, async (req, res) => {
  const videoId = req.params.videoId;
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

    const mimeType = source.mimeType || video.mimeType || 'video/mp4';
    const range = req.headers.range;

    let start = 0;
    let end = fileSize - 1;

    if (range) {
      const match = /bytes=(\d*)-(\d*)/.exec(range);
      if (!match) return res.status(416).set('Content-Range', `bytes */${fileSize}`).end();
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
        return res.status(416).set('Content-Range', `bytes */${fileSize}`).end();
      }
    }

    // Cap the served chunk so one Range request can't hold a socket open
    // for the entire remainder of a multi-GB file.
    if (end - start + 1 > MAX_CHUNK_BYTES) end = start + MAX_CHUNK_BYTES - 1;

    res.status(range ? 206 : 200);
    res.set({
      'Content-Type': mimeType,
      'Accept-Ranges': 'bytes',
      'Content-Length': end - start + 1,
      'Cache-Control': 'private, max-age=0, no-cache',
    });
    if (range) res.set('Content-Range', `bytes ${start}-${end}/${fileSize}`);

    if (req.method === 'HEAD') return res.end();

    let abortHandler = null;
    req.on('close', () => { if (abortHandler) abortHandler(); });

    await mtproto.streamRange(video.channelId, video.messageId, start, end, res, {
      onAbort: (fn) => { abortHandler = fn; },
    });
  } catch (err) {
    log.error('fileStream', 'Streaming failed', err, { videoId, range: req.headers.range });
    if (!res.headersSent) {
      if (err instanceof mtproto.MTProtoDisabledError) return res.status(501).json({ error: err.message });
      if (err instanceof ApiValidationError) return res.status(err.status).json({ error: err.message });
      res.status(500).json({ error: 'Streaming failed' });
    } else if (!res.writableEnded) {
      res.end();
    }
  }
});
router.head('/file/:videoId', fileLimiter, requireAuth, (req, res, next) => {
  req.method = 'HEAD';
  return router.handle(req, res, next);
});

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
                                                                          
