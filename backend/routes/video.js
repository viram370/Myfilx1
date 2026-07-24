/**
 * routes/video.js — GET /api/video/:id
 */
'use strict';

const express = require('express');
const router = express.Router();
const { getDoc, queryDocs } = require('../services/firebase');
const { softAuth } = require('../middleware/auth');
const { serializeVideo, groupIntoSeasons } = require('../utils/serialize');
const { requireDocId, ApiValidationError } = require('../utils/validators');
const { makeLogger } = require('../utils/logger');
const log = makeLogger('routes/video.js');

router.get('/:id', softAuth, async (req, res) => {
  try {
    const id = requireDocId(req.params.id);
    const doc = await getDoc('videos', id);
    if (!doc || !doc.published) return res.status(404).json({ error: 'Video not found' });

    const serialized = await serializeVideo(doc, { withImage: true });

    if (doc.season != null) {
      // Same fix as routes/videos.js: `doc` here is a single EPISODE
      // record (fetched by id — often episodes[0] of the series from the
      // list view). Its own episodeThumbnail is correct when this
      // response is used to represent THAT episode, but this branch
      // builds the ANIME detail view (title/seasons for the whole
      // series), which must always show the anime poster, never
      // whichever episode happened to be requested. Reset before the
      // seasons/episodes are grouped below (those keep their own
      // per-episode thumbnails independently, untouched).
      serialized.episodeThumbnail = serialized.thumbnail;
      const seriesTitle = doc.seriesTitle || doc.title;
      let episodes = await queryDocs('videos', [['published', '==', true], ['seriesTitle', '==', seriesTitle]]);
      if (!episodes.length) {
        episodes = await queryDocs('videos', [['published', '==', true], ['title', '==', seriesTitle]]);
      }
      serialized.seasons = await groupIntoSeasons(episodes.length ? episodes : [doc], serialized.thumbnail);
    }

    res.json({ video: serialized });
  } catch (err) {
    if (err instanceof ApiValidationError) return res.status(err.status).json({ error: err.message });
    log.error('get', 'Failed to fetch video', err, { id: req.params.id });
    res.status(500).json({ error: 'Failed to fetch video' });
  }
});

module.exports = router;
