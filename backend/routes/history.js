/**
 * POST /api/history   — log a watch event
 * GET  /api/history   — fetch current user's history (used by Profile page)
 * DELETE /api/history — clear history
 */
const express = require('express');
const router = express.Router();
const { addDoc, queryDocs, getDB } = require('../services/firebase');
const { requireAuth } = require('../middleware/auth');

router.post('/', requireAuth, async (req, res) => {
  try {
    const { videoId, title } = req.body;
    if (!videoId) return res.status(400).json({ error: 'videoId is required' });

    await addDoc('history', {
      userId: req.telegramUserId,
      videoId,
      title: title || '',
      watchedAt: new Date().toISOString(),
    });

    res.status(201).json({ saved: true });
  } catch (err) {
    console.error('[history] post error:', err.message);
    res.status(500).json({ error: 'Failed to save history' });
  }
});

router.get('/', requireAuth, async (req, res) => {
  try {
    const limit = parseInt(req.query.limit) || 50;
    const history = await queryDocs(
      'history', [['userId', '==', req.telegramUserId]], ['watchedAt', 'desc'], limit
    );
    const normalized = history.map(h => ({
      ...h,
      watchedAt: h.watchedAt?.toDate ? h.watchedAt.toDate().toISOString() : h.watchedAt,
    }));
    res.json({ history: normalized });
  } catch (err) {
    res.status(500).json({ error: 'Failed to fetch history' });
  }
});

router.delete('/', requireAuth, async (req, res) => {
  try {
    const history = await queryDocs('history', [['userId', '==', req.telegramUserId]]);
    const db = getDB();
    const batch = db.batch();
    history.forEach(h => batch.delete(db.collection('history').doc(h.id)));
    await batch.commit();
    res.json({ deleted: history.length });
  } catch (err) {
    res.status(500).json({ error: 'Failed to clear history' });
  }
});

module.exports = router;
