/**
 * GET /api/videos - FIXED
 */
const express = require('express');
const router = express.Router();
const { queryDocs } = require('../services/firebase');
const { softAuth } = require('../middleware/auth');
const { serializeVideo, groupIntoSeasons } = require('../utils/serialize');

router.get('/', softAuth, async (req, res) => {
  try {
    const { category, limit = 200 } = req.query;
    const filters = [['published', '==', true]];
    if (category && category !== 'all') filters.push(['category', '==', category]);

    let docs = await queryDocs('videos', filters, ['createdAt', 'desc'], parseInt(limit));

    // Fallback client-side sort (handles number timestamps from bot)
    docs.sort((a, b) => (b.createdAt || 0) - (a.createdAt || 0));

    // Grouping fix
    const standalone = [];
    const seriesMap = new Map();

    for (const doc of docs) {
      if (doc.season != null) {  // stricter check
        const key = doc.seriesTitle || doc.title || 'Unknown';
        if (!seriesMap.has(key)) seriesMap.set(key, []);
        seriesMap.get(key).push(doc);
      } else {
        standalone.push(doc);
      }
    }

    const [standaloneResults, seriesResults] = await Promise.all([
      Promise.all(standalone.map(doc => serializeVideo(doc, { withImage: true }))),
      Promise.all(Array.from(seriesMap.entries()).map(async ([seriesTitle, episodes]) => {
        const rep = episodes[0];
        const serialized = await serializeVideo(rep, { withImage: true });
        serialized.title = seriesTitle;
        serialized.seasons = groupIntoSeasons(episodes);
        return serialized;
      })),
    ]);

    const results = [...standaloneResults, ...seriesResults];

    res.json({ videos: results, total: results.length });
  } catch (err) {
    console.error('[videos] list error:', err.message, err.stack);
    res.status(500).json({ error: 'Failed to fetch videos', details: err.message });
  }
});

module.exports = router;
