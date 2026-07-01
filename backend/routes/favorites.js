/**
 * POST   /api/favorites        — add a favorite ({ videoId })
 * GET    /api/favorites        — list current user's favorites
 * DELETE /api/favorites/:id    — remove a favorite
 * GET    /api/favorites/check/:id — check if a video is favorited
 */
const express = require('express');
const router = express.Router();
const { setDoc, deleteDoc, getDoc, queryDocs } = require('../services/firebase');
const { requireAuth } = require('../middleware/auth');
const { serializeVideo } = require('../utils/serialize');

router.post('/', requireAuth, async (req, res) => {
  try {
    const { videoId } = req.body;
    if (!videoId) return res.status(400).json({ error: 'videoId is required' });

    const video = await getDoc('videos', videoId);
    if (!video || !video.published) return res.status(404).json({ error: 'Video not found' });

    const userId = req.telegramUserId;
    const docId = `${userId}_${videoId}`;
    await setDoc('favorites', docId, { userId, videoId, addedAt: new Date().toISOString() });

    res.status(201).json({ added: true, videoId });
  } catch (err) {
    console.error('[favorites] add error:', err.message);
    res.status(500).json({ error: 'Failed to add favorite' });
  }
});

router.get('/', requireAuth, async (req, res) => {
  try {
    const favs = await queryDocs(
      'favorites', [['userId', '==', req.telegramUserId]], ['addedAt', 'desc']
    );

    const enriched = await Promise.all(favs.map(async (f) => {
      const video = await getDoc('videos', f.videoId);
      if (!video || !video.published) return null;
      return { ...f, video: await serializeVideo(video, { withImage: true }) };
    }));

    res.json({ favorites: enriched.filter(Boolean) });
  } catch (err) {
    console.error('[favorites] list error:', err.message);
    res.status(500).json({ error: 'Failed to fetch favorites' });
  }
});

router.get('/check/:videoId', requireAuth, async (req, res) => {
  try {
    const docId = `${req.telegramUserId}_${req.params.videoId}`;
    const fav = await getDoc('favorites', docId);
    res.json({ isFavorite: !!fav });
  } catch (err) {
    res.status(500).json({ error: 'Failed to check favorite' });
  }
});

router.delete('/:videoId', requireAuth, async (req, res) => {
  try {
    const docId = `${req.telegramUserId}_${req.params.videoId}`;
    await deleteDoc('favorites', docId);
    res.json({ removed: true, videoId: req.params.videoId });
  } catch (err) {
    res.status(500).json({ error: 'Failed to remove favorite' });
  }
});

module.exports = router;
