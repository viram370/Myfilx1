/**
 * GET /api/video/:id
 * Full detail for a single video, including grouped seasons/episodes
 * if it belongs to a series.
 */
const express = require('express');
const router = express.Router();
const { getDoc, queryDocs } = require('../services/firebase');
const { softAuth } = require('../middleware/auth');
const { serializeVideo, groupIntoSeasons } = require('../utils/serialize');

router.get('/:id', softAuth, async (req, res) => {
  try {
    const doc = await getDoc('videos', req.params.id);
    if (!doc || !doc.published) {
      return res.status(404).json({ error: 'Video not found' });
    }

    const serialized = await serializeVideo(doc, { withImage: true });

    // If part of a series, attach full season/episode list
    if (doc.season) {
      const seriesTitle = doc.seriesTitle || doc.title;
      const episodes = await queryDocs('videos', [
        ['published', '==', true],
        ['seriesTitle', '==', seriesTitle],
      ]);
      // Fallback: some docs may store title instead of seriesTitle
      const all = episodes.length ? episodes : await queryDocs('videos', [
        ['published', '==', true],
        ['title', '==', seriesTitle],
      ]);
      serialized.seasons = groupIntoSeasons(all.length ? all : [doc]);
    }

    res.json({ video: serialized });
  } catch (err) {
    console.error('[video] get error:', err.message);
    res.status(500).json({ error: 'Failed to fetch video' });
  }
});

module.exports = router;
