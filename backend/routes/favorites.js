/**
 * routes/favorites.js
 */
'use strict';

const express = require('express');
const router = express.Router();
const { setDoc, deleteDoc, getDoc, queryDocs } = require('../services/firebase');
const { requireAuth } = require('../middleware/auth');
const { requireDocId, ApiValidationError } = require('../utils/validators');
const { serializeVideo } = require('../utils/serialize');
const { makeLogger } = require('../utils/logger');
const log = makeLogger('routes/favorites.js');

router.post('/', requireAuth, async (req, res) => {
  try {
    const videoId = requireDocId(req.body.videoId, 'videoId');
    const video = await getDoc('videos', videoId);
    if (!video || !video.published) return res.status(404).json({ error: 'Video not found' });

    const userId = req.telegramUserId;
    const docId = `${userId}_${videoId}`;
    await setDoc('favorites', docId, { userId, videoId, addedAt: new Date().toISOString() });

    res.status(201).json({ added: true, videoId });
  } catch (err) {
    if (err instanceof ApiValidationError) return res.status(err.status).json({ error: err.message });
    log.error('add', 'Failed to add favorite', err);
    res.status(500).json({ error: 'Failed to add favorite' });
  }
});

router.get('/', requireAuth, async (req, res) => {
  try {
    const favs = await queryDocs('favorites', [['userId', '==', req.telegramUserId]], ['addedAt', 'desc']);
    const enriched = await Promise.all(favs.map(async (f) => {
      const video = await getDoc('videos', f.videoId);
      if (!video || !video.published) return null;
      return { ...f, video: await serializeVideo(video, { withImage: true }) };
    }));
    res.json({ favorites: enriched.filter(Boolean) });
  } catch (err) {
    log.error('list', 'Failed to fetch favorites', err);
    res.status(500).json({ error: 'Failed to fetch favorites' });
  }
});

router.get('/check/:videoId', requireAuth, async (req, res) => {
  try {
    const videoId = requireDocId(req.params.videoId, 'videoId');
    const docId = `${req.telegramUserId}_${videoId}`;
    const fav = await getDoc('favorites', docId);
    res.json({ isFavorite: !!fav });
  } catch (err) {
    if (err instanceof ApiValidationError) return res.status(err.status).json({ error: err.message });
    res.status(500).json({ error: 'Failed to check favorite' });
  }
});

router.delete('/:videoId', requireAuth, async (req, res) => {
  try {
    const videoId = requireDocId(req.params.videoId, 'videoId');
    const docId = `${req.telegramUserId}_${videoId}`;
    await deleteDoc('favorites', docId);
    res.json({ removed: true, videoId });
  } catch (err) {
    if (err instanceof ApiValidationError) return res.status(err.status).json({ error: err.message });
    log.error('remove', 'Failed to remove favorite', err);
    res.status(500).json({ error: 'Failed to remove favorite' });
  }
});

module.exports = router;
