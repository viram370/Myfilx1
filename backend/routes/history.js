/**
 * routes/history.js
 */
'use strict';

const express = require('express');
const router = express.Router();
const { addDoc, queryDocs, getDB, batchDelete } = require('../services/firebase');
const { requireAuth } = require('../middleware/auth');
const { requireDocId, sanitizeText, clampInt, ApiValidationError } = require('../utils/validators');
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
    const normalized = history.map((h) => ({
      ...h,
      watchedAt: h.watchedAt?.toDate ? h.watchedAt.toDate().toISOString() : h.watchedAt,
    }));
    res.json({ history: normalized });
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
