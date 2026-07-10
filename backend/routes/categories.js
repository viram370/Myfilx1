/**
 * routes/categories.js — GET /api/categories
 */
'use strict';

const express = require('express');
const router = express.Router();
const { queryDocs } = require('../services/firebase');
const { softAuth } = require('../middleware/auth');
const { makeLogger } = require('../utils/logger');
const log = makeLogger('routes/categories.js');

const ICONS = { Anime: '⛩️', 'Web Series': '📺', Series: '📺', Movies: '🎬', Cartoons: '🎨' };
const ICONS_LOWER = Object.fromEntries(Object.entries(ICONS).map(([k, v]) => [k.toLowerCase(), v]));

let cache = null;
const CACHE_TTL_MS = 30_000;

router.get('/', softAuth, async (req, res) => {
  try {
    if (cache && Date.now() - cache.fetchedAt < CACHE_TTL_MS) return res.json(cache.value);

    const docs = await queryDocs('videos', [['published', '==', true]]);
    const counts = {};
    for (const d of docs) {
      const cat = d.category || 'Uncategorized';
      counts[cat] = (counts[cat] || 0) + 1;
    }
    const categories = Object.entries(counts).map(([name, count]) => ({
      name, count, icon: ICONS_LOWER[name.toLowerCase()] || '🎬',
    }));

    const value = { categories };
    cache = { value, fetchedAt: Date.now() };
    res.json(value);
  } catch (err) {
    log.error('list', 'Failed to fetch categories', err);
    res.status(500).json({ error: 'Failed to fetch categories' });
  }
});

module.exports = router;
