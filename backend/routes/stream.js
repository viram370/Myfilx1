/**
 * Stream Routes
 * GET  /api/stream/:videoId            — resolve Telegram file -> streamable URL
 * POST /api/stream/:videoId/progress   — save watch progress (continueWatching)
 * GET  /api/stream/:videoId/progress   — get saved progress (for resume)
 */
const express = require('express');
const router = express.Router();
const { getDoc, setDoc, addDoc, updateDoc } = require('../services/firebase');
const { requireAuth } = require('../middleware/auth');
const { resolveFileUrl } = require('../utils/serialize');

router.get('/:videoId', requireAuth, async (req, res) => {
  try {
    const { videoId } = req.params;
    const userId = req.telegramUserId;

    const video = await getDoc('videos', videoId);
    if (!video) return res.status(404).json({ error: 'Video not found' });
    if (!video.published) return res.status(403).json({ error: 'Video not available' });
    if (!video.telegram_file_id) return res.status(500).json({ error: 'Video file not configured' });

    // Reuse the shared, TTL-cached resolver instead of re-hitting Telegram's
    // getFile API on every single play request for the same video.
    // throwOnError: true so the "file is too big" 413 case below still works.
    const streamUrl = await resolveFileUrl(video.telegram_file_id, { throwOnError: true });
    if (!streamUrl) throw new Error('Failed to resolve file from Telegram');

    logWatch(userId, videoId, video.title);
    updateDoc('videos', videoId, { views: (video.views || 0) + 1 }).catch(() => {});

    res.json({
      streamUrl,
      video: { id: videoId, title: video.title, duration: video.duration, mimeType: video.mimeType || 'video/mp4' },
      expiresIn: 3000,
    });
  } catch (err) {
    console.error('[stream] error:', err.message);
    if (err.response?.data?.description?.includes('file is too big')) {
      return res.status(413).json({ error: 'File too large for direct streaming.' });
    }
    res.status(500).json({ error: 'Failed to get stream URL' });
  }
});

router.post('/:videoId/progress', requireAuth, async (req, res) => {
  try {
    const { videoId } = req.params;
    const userId = req.telegramUserId;
    const { position, duration } = req.body;

    if (typeof position !== 'number' || position < 0) {
      return res.status(400).json({ error: 'Invalid position' });
    }

    const progressPercent = duration ? Math.round((position / duration) * 100) : 0;
    const docId = `${userId}_${videoId}`;

    await setDoc('continueWatching', docId, {
      userId, videoId, position, duration, progressPercent,
      completed: progressPercent >= 90,
      watchedAt: new Date().toISOString(),
    });

    res.json({ saved: true, progressPercent });
  } catch (err) {
    res.status(500).json({ error: 'Failed to save progress' });
  }
});

router.get('/:videoId/progress', requireAuth, async (req, res) => {
  try {
    const docId = `${req.telegramUserId}_${req.params.videoId}`;
    const progress = await getDoc('continueWatching', docId);
    res.json({
      position: progress?.position || 0,
      duration: progress?.duration || 0,
      progressPercent: progress?.progressPercent || 0,
      completed: progress?.completed || false,
    });
  } catch (err) {
    res.status(500).json({ error: 'Failed to get progress' });
  }
});

async function logWatch(userId, videoId, title) {
  try { await addDoc('history', { userId, videoId, title, watchedAt: new Date().toISOString() }); }
  catch (e) { console.error('[stream] history log error:', e.message); }
}

module.exports = router;
