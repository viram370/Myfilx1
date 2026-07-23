/**
 * routes/history.js
 */
'use strict';

const express = require('express');
const router = express.Router();
const { addDoc, queryDocs, getDoc, batchDelete } = require('../services/firebase');
const { requireAuth } = require('../middleware/auth');
const { requireDocId, sanitizeText, clampInt, ApiValidationError } = require('../utils/validators');
const { serializeVideo } = require('../utils/serialize');
const { makeLogger } = require('../utils/logger');
const log = makeLogger('routes/history.js');

router.post('/', requireAuth, async (req, res) => {
  try {
    const videoId = requireDocId(req.body.videoId, 'videoId');
    const title = sanitizeText(req.body.title, { max: 200 });

    await addDoc('history', {
      userId: req.telegramUserId,
      videoId,
      title,
      watchedAt: new Date().toISOString(),
    });

    res.status(201).json({ saved: true });
  } catch (err) {
    if (err instanceof ApiValidationError) return res.status(err.status).json({ error: err.message });
    log.error('post', 'Failed to save history', err);
    res.status(500).json({ error: 'Failed to save history' });
  }
});

router.get('/', requireAuth, async (req, res) => {
  try {
    const limit = clampInt(req.query.limit, { min: 1, max: 200, fallback: 50 });
    const history = await queryDocs('history', [['userId', '==', req.telegramUserId]], ['watchedAt', 'desc'], limit);

    // De-dupe by videoId (keeping the most recent watchedAt, which is
    // already what the desc-ordered query returns first) before doing any
    // per-episode lookups below — a title watched multiple times used to
    // otherwise show up as repeated rows in Watch History.
    const seen = new Set();
    const deduped = [];
    for (const h of history) {
      if (!h.videoId || seen.has(h.videoId)) continue;
      seen.add(h.videoId);
      deduped.push(h);
    }

    // Enrich each entry with the actual episode's own record (season,
    // episode, episodeThumbnail, etc.) — the frontend's Watch History page
    // needs the specific episode that was watched, not just its id/title,
    // so it can show the real season/episode number and that episode's
    // own thumbnail instead of falling back to the parent show's poster.
    const enriched = await Promise.all(deduped.map(async (h) => {
      const watchedAt = h.watchedAt?.toDate ? h.watchedAt.toDate().toISOString() : h.watchedAt;
      const video = await getDoc('videos', h.videoId);
      if (!video || !video.published) {
        return { ...h, watchedAt, video: null };
      }
      const serialized = await serializeVideo(video, { withImage: true });
      return { ...h, watchedAt, video: serialized };
    }));

    res.json({ history: enriched });
  } catch (err) {
    log.error('list', 'Failed to fetch history', err);
    res.status(500).json({ error: 'Failed to fetch history' });
  }
});

router.delete('/', requireAuth, async (req, res) => {
  try {
    const history = await queryDocs('history', [['userId', '==', req.telegramUserId]]);
    const deleted = await batchDelete('history', history.map((h) => h.id));
    res.json({ deleted });
  } catch (err) {
    log.error('clear', 'Failed to clear history', err);
    res.status(500).json({ error: 'Failed to clear history' });
  }
});

module.exports = router;
