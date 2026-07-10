/**
 * routes/search.js — GET /api/search?q=...&category=...
 */
'use strict';

const express = require('express');
const router = express.Router();
const { queryDocs } = require('../services/firebase');
const { softAuth } = require('../middleware/auth');
const { serializeVideos } = require('../utils/serialize');
const { sanitizeText, isValidCategory, clampInt } = require('../utils/validators');
const { makeLogger } = require('../utils/logger');
const log = makeLogger('routes/search.js');

router.get('/', softAuth, async (req, res) => {
  try {
    const q = sanitizeText(req.query.q, { max: 100 });
    const category = req.query.category;
    if (category && category !== 'all' && !isValidCategory(category)) {
      return res.status(400).json({ error: 'Invalid category.' });
    }

    const limit = clampInt(req.query.limit, { min: 1, max: 100, fallback: 40 });
    const filters = [['published', '==', true]];
    if (category && category !== 'all') filters.push(['category', '==', category]);

    const docs = await queryDocs('videos', filters);
    const ql = q.toLowerCase();

    const filtered = ql
      ? docs.filter((v) =>
          (v.title || '').toLowerCase().includes(ql) ||
          (v.description || '').toLowerCase().includes(ql) ||
          (v.genre || '').toLowerCase().includes(ql) ||
          (v.category || '').toLowerCase().includes(ql))
      : docs;

    const results = await serializeVideos(filtered.slice(0, limit), { withImage: true });
    res.json({ results, total: results.length, query: q });
  } catch (err) {
    log.error('search', 'Search failed', err, { q: req.query.q });
    res.status(500).json({ error: 'Search failed' });
  }
});

module.exports = router;
