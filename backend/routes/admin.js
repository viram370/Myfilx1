/**
 * routes/admin.js — Admin Routes (protected)
 */
'use strict';

const express = require('express');
const router = express.Router();
const { getDoc, updateDoc, deleteDoc, queryDocs, queryDocsPaginated, addDoc, findExistingFileUniqueIds, invalidateCache } = require('../services/firebase');
const { requireAdmin } = require('../middleware/auth');
const { requireDocId, sanitizeText, clampInt, isValidCategory, paginationParams, ApiValidationError } = require('../utils/validators');
const { makeLogger } = require('../utils/logger');
const log = makeLogger('routes/admin.js');

router.get('/videos', requireAdmin, async (req, res) => {
  try {
    const { category } = req.query;
    const { limit, cursor } = paginationParams(req.query, { defaultLimit: 50, maxLimit: 200 });
    const filters = [];
    if (category) {
      if (!isValidCategory(category)) throw new ApiValidationError('Invalid category.');
      filters.push(['category', '==', category]);
    }
    const { docs, nextCursor } = await queryDocsPaginated('videos', filters, { limit, cursorId: cursor });
    res.json({ videos: docs, total: docs.length, nextCursor });
  } catch (err) {
    if (err instanceof ApiValidationError) return res.status(err.status).json({ error: err.message });
    log.error('listVideos', 'Failed to fetch videos', err);
    res.status(500).json({ error: 'Failed to fetch videos' });
  }
});

router.post('/videos', requireAdmin, async (req, res) => {
  try {
    const title = sanitizeText(req.body.title, { max: 200 });
    const telegram_file_id = sanitizeText(req.body.telegram_file_id, { max: 300 });
    if (!title || !telegram_file_id) {
      return res.status(400).json({ error: 'title and telegram_file_id are required' });
    }

    const category = req.body.category && isValidCategory(req.body.category) ? req.body.category : 'Uncategorized';
    const description = sanitizeText(req.body.description, { max: 2000 });
    const seriesTitle = sanitizeText(req.body.seriesTitle, { max: 200 }) || null;

    const parsedSeason = req.body.season !== undefined && req.body.season !== null && req.body.season !== '' ? Number(req.body.season) : null;
    const parsedEpisode = req.body.episode !== undefined && req.body.episode !== null && req.body.episode !== '' ? Number(req.body.episode) : null;
    const parsedDuration = req.body.duration !== undefined && req.body.duration !== null && req.body.duration !== '' ? Number(req.body.duration) : 0;

    if (parsedSeason !== null && (!Number.isFinite(parsedSeason) || parsedSeason < 0)) return res.status(400).json({ error: 'season must be a non-negative number' });
    if (parsedEpisode !== null && (!Number.isFinite(parsedEpisode) || parsedEpisode < 0)) return res.status(400).json({ error: 'episode must be a non-negative number' });
    if (!Number.isFinite(parsedDuration) || parsedDuration < 0) return res.status(400).json({ error: 'duration must be a non-negative number' });

    // Duplicate-file guard: this route previously allowed the same Telegram
    // file to be registered as multiple videos.
    if (req.body.file_unique_id) {
      const existing = await findExistingFileUniqueIds([req.body.file_unique_id]);
      if (existing.has(req.body.file_unique_id)) {
        return res.status(409).json({ error: 'This Telegram file has already been saved as a video.' });
      }
    }

    const videoId = await addDoc('videos', {
      title, description, category,
      season: parsedSeason, episode: parsedEpisode,
      seriesTitle: parsedSeason !== null ? (seriesTitle || title) : seriesTitle,
      telegram_file_id,
      file_unique_id: req.body.file_unique_id || null,
      channelId: req.body.channelId ? Number(req.body.channelId) : null,
      messageId: req.body.messageId ? Number(req.body.messageId) : null,
      language: sanitizeText(req.body.language, { max: 60 }) || 'Unknown',
      quality: sanitizeText(req.body.quality, { max: 20 }) || null,
      duration: parsedDuration,
      views: 0, likes: 0, published: false,
      uploadDate: new Date().toISOString(),
    });
    res.status(201).json({ videoId, created: true });
  } catch (err) {
    log.error('createVideo', 'Failed to create video', err);
    res.status(500).json({ error: 'Failed to create video' });
  }
});

router.patch('/videos/:id', requireAdmin, async (req, res) => {
  try {
    const id = requireDocId(req.params.id);
    const video = await getDoc('videos', id);
    if (!video) return res.status(404).json({ error: 'Video not found' });

    const allowed = ['title', 'description', 'category', 'season', 'episode', 'published', 'telegram_file_id', 'duration', 'bannerFileId', 'language', 'quality'];
    const updates = {};
    for (const f of allowed) {
      if (req.body[f] === undefined) continue;
      if (f === 'category' && !isValidCategory(req.body[f])) return res.status(400).json({ error: 'Invalid category.' });
      updates[f] = typeof req.body[f] === 'string' ? sanitizeText(req.body[f], { max: 2000 }) : req.body[f];
    }

    await updateDoc('videos', id, updates);
    res.json({ updated: true, id });
  } catch (err) {
    if (err instanceof ApiValidationError) return res.status(err.status).json({ error: err.message });
    log.error('updateVideo', 'Failed to update video', err, { id: req.params.id });
    res.status(500).json({ error: 'Failed to update video' });
  }
});

router.delete('/videos/:id', requireAdmin, async (req, res) => {
  try {
    const id = requireDocId(req.params.id);
    const video = await getDoc('videos', id);
    if (!video) return res.status(404).json({ error: 'Video not found' });
    await deleteDoc('videos', id);
    res.json({ deleted: true, id });
  } catch (err) {
    if (err instanceof ApiValidationError) return res.status(err.status).json({ error: err.message });
    log.error('deleteVideo', 'Failed to delete video', err, { id: req.params.id });
    res.status(500).json({ error: 'Failed to delete video' });
  }
});

let statsCache = null;
router.get('/stats', requireAdmin, async (req, res) => {
  try {
    if (statsCache && Date.now() - statsCache.fetchedAt < 20_000) return res.json(statsCache.value);

    const [videos, users, history] = await Promise.all([
      queryDocs('videos'), queryDocs('users'), queryDocs('history', [], ['watchedAt', 'desc'], 500),
    ]);
    const published = videos.filter((v) => v.published).length;
    const byCategory = {};
    videos.forEach((v) => {
      const c = v.category || 'Uncategorized';
      byCategory[c] = byCategory[c] || { total: 0, published: 0 };
      byCategory[c].total++;
      if (v.published) byCategory[c].published++;
    });
    const watchers = new Set(history.map((h) => h.userId)).size;

    const value = {
      videos: { total: videos.length, published, drafts: videos.length - published, byCategory },
      users: { total: users.length, activeWatchers: watchers },
      engagement: { totalWatchEvents: history.length },
    };
    statsCache = { value, fetchedAt: Date.now() };
    res.json(value);
  } catch (err) {
    log.error('stats', 'Failed to fetch stats', err);
    res.status(500).json({ error: 'Failed to fetch stats' });
  }
});

module.exports = router;
