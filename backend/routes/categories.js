/**
 * GET /api/categories
 * Returns distinct categories with counts.
 */
const express = require('express');
const router = express.Router();
const { queryDocs } = require('../services/firebase');
const { softAuth } = require('../middleware/auth');

const ICONS = { Anime: '⛩️', 'Web Series': '📺', Series: '📺', Movies: '🎬', Cartoons: '🎨' };

router.get('/', softAuth, async (req, res) => {
  try {
    const docs = await queryDocs('videos', [['published', '==', true]]);
    const counts = {};
    for (const d of docs) {
      const cat = d.category || 'Uncategorized';
      counts[cat] = (counts[cat] || 0) + 1;
    }
    const categories = Object.entries(counts).map(([name, count]) => ({
      name, count, icon: ICONS[name] || '🎬',
    }));
    res.json({ categories });
  } catch (err) {
    console.error('[categories] error:', err.message);
    res.status(500).json({ error: 'Failed to fetch categories' });
  }
});

module.exports = router;
