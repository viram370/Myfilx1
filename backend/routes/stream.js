/**
 * routes/stream.js
 * ---------------------------------------------------------------------
 * Fixed streaming implementation. Correctly handles HTTP Ranges, 
 * flushes headers immediately, and destroys the socket on truncation 
 * to prevent the browser from caching corrupted video files.
 */
'use strict';

const express = require('express');
const rateLimit = require('express-rate-limit');
const router = express.Router();

const { getDoc, updateDoc, addDoc } = require('../services/firebase');
const mtproto = require('../services/mtproto');
const { requireAuth, softAuth } = require('../middleware/auth');
const { requireDocId, ApiValidationError } = require('../utils/validators');
const { makeLogger } = require('../utils/logger');
const log = makeLogger('routes/stream.js');

const fileLimiter = rateLimit({ windowMs: 60_000, max: 600, standardHeaders: true, legacyHeaders: false });
const resolverLimiter = rateLimit({ windowMs: 60_000, max: 40, standardHeaders: true, legacyHeaders: false });

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
      expiresIn: 21600,
    });
  } catch (err) {
    if (err instanceof ApiValidationError) return res.status(err.status).json({ error: err.message });
    log.error('resolve', 'Failed to resolve stream URL', err, { videoId: req.params.videoId });
    res.status(500).json({ error: 'Failed to get stream URL' });
  }
});

router.get('/file/:videoId', fileLimiter, softAuth, async (req, res) => {
  const videoId = req.params.videoId;
  try {
    requireDocId(videoId, 'videoId');

    if (!mtproto.isEnabled()) {
      return res.status(501).json({ error: 'Streaming is not configured on this server.' });
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
        log.warn('fileStream', 'Source message missing', { videoId, reason: err.message });
        updateDoc('videos', videoId, { sourceMissing: true }).catch(() => {});
        return res.status(404).json({ error: 'Source video is no longer available.' });
      }
      throw err;
    }

    const fileSize = video.fileSizeBytes || source.size;
    if (!video.fileSizeBytes || video.mimeType !== source.mimeType) {
      updateDoc('videos', videoId, { fileSizeBytes: source.size, mimeType: source.mimeType }).catch(() => {});
    }

    const mimeType = source.mimeType || 'video/mp4';
    const range = req.headers.range;

    let start = 0;
    let end = fileSize - 1;

    if (range) {
      const match = /bytes=(\d*)-(\d*)/.exec(range);
      if (!match) {
        res.set('Content-Range', `bytes */${fileSize}`);
        return res.status(416).end();
      }
      const rangeStart = match[1] ? parseInt(match[1], 10) : null;
      const rangeEnd = match[2] ? parseInt(match[2], 10) : null;

      if (rangeStart === null && rangeEnd !== null) {
        start = Math.max(0, fileSize - rangeEnd);
        end = fileSize - 1;
      } else {
        start = rangeStart || 0;
        end = rangeEnd !== null ? Math.min(rangeEnd, fileSize - 1) : fileSize - 1;
      }

      if (start >= fileSize || start > end || start < 0) {
        res.set('Content-Range', `bytes */${fileSize}`);
        return res.status(416).end();
      }
    }

    if (end - start + 1 > MAX_CHUNK_BYTES) end = start + MAX_CHUNK_BYTES - 1;
    const contentLength = end - start + 1;

    // Set precise HTTP 206 / 200 headers
    res.status(range ? 206 : 200);
    res.set({
      'Content-Type': mimeType,
      'Accept-Ranges': 'bytes',
      'Content-Length': contentLength,
      'Cache-Control': 'public, max-age=3600',
      'ETag': `"${videoId}-${fileSize}"`
    });
    
    if (range) res.set('Content-Range', `bytes ${start}-${end}/${fileSize}`);

    if (req.method === 'HEAD') return res.end();

    // Fix: Flush headers BEFORE any chunk downloads so browser loads the UI instantly
    res.flushHeaders();

    let abortHandler = null;
    req.on('close', () => { if (abortHandler) abortHandler(); });
    req.on('aborted', () => { if (abortHandler) abortHandler(); });

    await mtproto.streamRange(video.channelId, video.messageId, start, end, res, {
      videoId,
      rangeHeader: range,
      onAbort: (fn) => { abortHandler = fn; },
    });

    if (!res.writableEnded) res.end();
  } catch (err) {
    log.error('fileStream', 'Streaming failed', err, { videoId, range: req.headers.range });
    if (!res.headersSent) {
      if (err instanceof mtproto.MTProtoDisabledError) return res.status(501).json({ error: err.message });
      if (err instanceof ApiValidationError) return res.status(err.status).json({ error: err.message });
      return res.status(500).json({ error: 'Streaming failed' });
    }
    // Fix: If headers were sent, DESTROY the connection. Do not call res.end().
    // res.end() caches a corrupted file in the browser.
    if (!res.writableEnded) res.destroy(err);
  }
});

router.head('/file/:videoId', fileLimiter, softAuth, (req, res, next) => {
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
