/**
 * routes/users.js
 */
'use strict';

const express = require('express');
const router = express.Router();
const { getDoc, queryDocs } = require('../services/firebase');
const { requireAuth } = require('../middleware/auth');
const { clampInt } = require('../utils/validators');
const { serializeVideo } = require('../utils/serialize');
const { makeLogger } = require('../utils/logger');
const log = makeLogger('routes/users.js');

router.get('/me', requireAuth, async (req, res) => {
  try {
    const user = await getDoc('users', req.telegramUserId);
    res.json({ user: user || { telegramId: req.telegramUser.id, ...req.telegramUser } });
  } catch (err) {
    log.error('me', 'Failed to fetch profile', err);
    res.status(500).json({ error: 'Failed to fetch profile' });
  }
});

router.get('/me/continue-watching', requireAuth, async (req, res) => {
  try {
    const limit = clampInt(req.query.limit, { min: 1, max: 50, fallback: 10 });
    const items = await queryDocs(
      'continueWatching',
      [['userId', '==', req.telegramUserId], ['completed', '==', false]],
      ['watchedAt', 'desc'], limit
    );

    const enriched = await Promise.all(items.map(async (item) => {
      const video = await getDoc('videos', item.videoId);
      if (!video || !video.published) return null;
      const serialized = await serializeVideo(video, { withImage: true });
      return { ...item, video: { ...serialized, watchProgress: item.progressPercent || 0 } };
    }));

    res.json({ items: enriched.filter(Boolean) });
  } catch (err) {
    log.error('continueWatching', 'Failed to fetch continue watching', err);
    res.status(500).json({ error: 'Failed to fetch continue watching' });
  }
});

module.exports = router;
