/**
 * Admin Routes (protected)
 */
const express = require('express');
const router = express.Router();
const { getDoc, updateDoc, deleteDoc, queryDocs, addDoc } = require('../services/firebase');
const { requireAdmin } = require('../middleware/auth');

router.get('/videos', requireAdmin, async (req, res) => {
  try {
    const { category, limit = 100 } = req.query;
    const filters = [];
    if (category) filters.push(['category', '==', category]);
    const videos = await queryDocs('videos', filters, ['createdAt', 'desc'], parseInt(limit));
    res.json({ videos, total: videos.length });
  } catch (err) {
    res.status(500).json({ error: 'Failed to fetch videos' });
  }
});

router.post('/videos', requireAdmin, async (req, res) => {
  try {
    const { title, telegram_file_id, category, season, episode, description, duration, seriesTitle } = req.body;
    if (!title || !telegram_file_id) {
      return res.status(400).json({ error: 'title and telegram_file_id are required' });
    }

    // Coerce + validate numeric fields so they can't silently become NaN
    // downstream in groupIntoSeasons()'s numeric sort.
    const parsedSeason = season !== undefined && season !== null && season !== '' ? Number(season) : null;
    const parsedEpisode = episode !== undefined && episode !== null && episode !== '' ? Number(episode) : null;
    const parsedDuration = duration !== undefined && duration !== null && duration !== '' ? Number(duration) : 0;
    if (parsedSeason !== null && !Number.isFinite(parsedSeason)) {
      return res.status(400).json({ error: 'season must be numeric' });
    }
    if (parsedEpisode !== null && !Number.isFinite(parsedEpisode)) {
      return res.status(400).json({ error: 'episode must be numeric' });
    }
    if (!Number.isFinite(parsedDuration)) {
      return res.status(400).json({ error: 'duration must be numeric' });
    }

    const videoId = await addDoc('videos', {
      title, description: description || '', category: category || 'Uncategorized',
      season: parsedSeason, episode: parsedEpisode,
      // Default seriesTitle to title when this is episodic content, so
      // routes/video.js never needs a fallback query for missing seriesTitle.
      seriesTitle: parsedSeason !== null ? (seriesTitle || title) : (seriesTitle || null),
      telegram_file_id, duration: parsedDuration,
      views: 0, likes: 0, published: false,
      uploadDate: new Date().toISOString(),
    });
    res.status(201).json({ videoId, created: true });
  } catch (err) {
    res.status(500).json({ error: 'Failed to create video' });
  }
});

router.patch('/videos/:id', requireAdmin, async (req, res) => {
  try {
    const video = await getDoc('videos', req.params.id);
    if (!video) return res.status(404).json({ error: 'Video not found' });

    const allowed = ['title', 'description', 'category', 'season', 'episode', 'published', 'telegram_file_id', 'duration', 'bannerFileId'];
    const updates = {};
    for (const f of allowed) if (req.body[f] !== undefined) updates[f] = req.body[f];

    await updateDoc('videos', req.params.id, updates);
    res.json({ updated: true, id: req.params.id });
  } catch (err) {
    res.status(500).json({ error: 'Failed to update video' });
  }
});

router.delete('/videos/:id', requireAdmin, async (req, res) => {
  try {
    const video = await getDoc('videos', req.params.id);
    if (!video) return res.status(404).json({ error: 'Video not found' });
    await deleteDoc('videos', req.params.id);
    res.json({ deleted: true, id: req.params.id });
  } catch (err) {
    res.status(500).json({ error: 'Failed to delete video' });
  }
});

router.get('/stats', requireAdmin, async (req, res) => {
  try {
    const [videos, users, history] = await Promise.all([
      queryDocs('videos'), queryDocs('users'), queryDocs('history', [], ['watchedAt', 'desc'], 500),
    ]);
    const published = videos.filter(v => v.published).length;
    const byCategory = {};
    videos.forEach(v => {
      const c = v.category || 'Uncategorized';
      byCategory[c] = byCategory[c] || { total: 0, published: 0 };
      byCategory[c].total++;
      if (v.published) byCategory[c].published++;
    });
    const watchers = new Set(history.map(h => h.userId)).size;

    res.json({
      videos: { total: videos.length, published, drafts: videos.length - published, byCategory },
      users: { total: users.length, activeWatchers: watchers },
      engagement: { totalWatchEvents: history.length },
    });
  } catch (err) {
    res.status(500).json({ error: 'Failed to fetch stats' });
  }
});

module.exports = router;
