/**
 * GET /api/search?q=...&category=...
 */
const express = require('express');
const router = express.Router();
const { queryDocs } = require('../services/firebase');
const { softAuth } = require('../middleware/auth');
const { serializeVideos } = require('../utils/serialize');

router.get('/', softAuth, async (req, res) => {
  try {
    const { q = '', category } = req.query;
    const filters = [['published', '==', true]];
    if (category && category !== 'all') filters.push(['category', '==', category]);

    const docs = await queryDocs('videos', filters);
    const ql = q.trim().toLowerCase();

    const filtered = ql
      ? docs.filter(v =>
          (v.title || '').toLowerCase().includes(ql) ||
          (v.description || '').toLowerCase().includes(ql) ||
          (v.genre || '').toLowerCase().includes(ql) ||
          (v.category || '').toLowerCase().includes(ql))
      : docs;

    const results = await serializeVideos(filtered.slice(0, 40), { withImage: true });
    res.json({ results, total: results.length, query: q });
  } catch (err) {
    console.error('[search] error:', err.message);
    res.status(500).json({ error: 'Search failed' });
  }
});

module.exports = router;
