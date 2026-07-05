/**
 * GET /api/videos
 * Returns all published videos (optionally filtered by category),
 * grouped so multi-episode entries become single series cards with
 * a `seasons` array — matching the frontend's DEMO_* shape.
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

    const docs = await queryDocs('videos', filters, ['createdAt', 'desc'], parseInt(limit));

    // Group by seriesTitle (or title, if season is set) to build episode lists
    const standalone = [];
    const seriesMap = new Map();

    for (const doc of docs) {
      if (doc.season) {
        const key = doc.seriesTitle || doc.title;
        if (!seriesMap.has(key)) seriesMap.set(key, []);
        seriesMap.get(key).push(doc);
      } else {
        standalone.push(doc);
      }
    }

    // Standalone (movies, one-offs) and series representative cards are each
    // independent — resolve them concurrently instead of one-by-one, since
    // serializeVideo() may make a network call to Telegram on a cache miss.
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
    console.error('[videos] list error:', err.message);
    res.status(500).json({ error: 'Failed to fetch videos' });
  }
});

module.exports = router;
