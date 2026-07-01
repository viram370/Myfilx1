/**
 * Users Routes
 * GET /api/users/me                   — profile
 * GET /api/users/me/continue-watching — list for "Continue Watching" row
 */
const express = require('express');
const router = express.Router();
const { getDoc, queryDocs } = require('../services/firebase');
const { requireAuth } = require('../middleware/auth');
const { serializeVideo } = require('../utils/serialize');

router.get('/me', requireAuth, async (req, res) => {
  try {
    const user = await getDoc('users', req.telegramUserId);
    res.json({ user: user || { telegramId: req.telegramUser.id, ...req.telegramUser } });
  } catch (err) {
    res.status(500).json({ error: 'Failed to fetch profile' });
  }
});

router.get('/me/continue-watching', requireAuth, async (req, res) => {
  try {
    const items = await queryDocs(
      'continueWatching',
      [['userId', '==', req.telegramUserId], ['completed', '==', false]],
      ['watchedAt', 'desc'], 10
    );

    const enriched = await Promise.all(items.map(async (item) => {
      const video = await getDoc('videos', item.videoId);
      if (!video || !video.published) return null;
      const serialized = await serializeVideo(video, { withImage: true });
      return { ...item, video: { ...serialized, watchProgress: item.progressPercent || 0 } };
    }));

    res.json({ items: enriched.filter(Boolean) });
  } catch (err) {
    console.error('[users] continue-watching error:', err.message);
    res.status(500).json({ error: 'Failed to fetch continue watching' });
  }
});

module.exports = router;
