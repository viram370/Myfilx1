/**
 * routes/videos.js — GET /api/videos
 */
'use strict';

const express = require('express');
const router = express.Router();
const { queryDocs } = require('../services/firebase');
const { softAuth } = require('../middleware/auth');
const { serializeVideo, groupIntoSeasons } = require('../utils/serialize');
const { isValidCategory, clampInt, ApiValidationError } = require('../utils/validators');
const { makeLogger } = require('../utils/logger');
const log = makeLogger('routes/videos.js');

const listCache = new Map();
const LIST_CACHE_TTL_MS = 15_000;

router.get('/', softAuth, async (req, res) => {
  try {
    const { category } = req.query;
    const limit = clampInt(req.query.limit, { min: 1, max: 500, fallback: 200 });
    if (category && category !== 'all' && !isValidCategory(category)) {
      return res.status(400).json({ error: 'Invalid category.' });
    }

    const cacheKey = `${category || 'all'}:${limit}`;
    const cached = listCache.get(cacheKey);
    if (cached && Date.now() - cached.fetchedAt < LIST_CACHE_TTL_MS) {
      return res.json(cached.value);
    }

    const filters = [['published', '==', true]];
    if (category && category !== 'all') filters.push(['category', '==', category]);

    let docs = await queryDocs('videos', filters, ['createdAt', 'desc'], limit);
    docs.sort((a, b) => (toComparable(b.createdAt)) - (toComparable(a.createdAt)));

    const standalone = [];
    const seriesMap = new Map();
    for (const doc of docs) {
      if (doc.season != null) {
        const key = doc.seriesTitle || doc.title || 'Unknown';
        if (!seriesMap.has(key)) seriesMap.set(key, []);
        seriesMap.get(key).push(doc);
      } else {
        standalone.push(doc);
      }
    }

    const [standaloneResults, seriesResults] = await Promise.all([
      Promise.all(standalone.map((doc) => serializeVideo(doc, { withImage: true }))),
      Promise.all(Array.from(seriesMap.entries()).map(async ([seriesTitle, episodes]) => {
        const rep = episodes[0];
        const serialized = await serializeVideo(rep, { withImage: true });
        serialized.title = seriesTitle;
        serialized.seasons = groupIntoSeasons(episodes);
        return serialized;
      })),
    ]);

    const results = [...standaloneResults, ...seriesResults];
    const payload = { videos: results, total: results.length };
    listCache.set(cacheKey, { value: payload, fetchedAt: Date.now() });
    if (listCache.size > 50) {
      const oldestKey = [...listCache.entries()].sort((a, b) => a[1].fetchedAt - b[1].fetchedAt)[0]?.[0];
      if (oldestKey) listCache.delete(oldestKey);
    }

    res.json(payload);
  } catch (err) {
    if (err instanceof ApiValidationError) return res.status(err.status).json({ error: err.message });
    log.error('list', 'Failed to fetch videos', err);
    res.status(500).json({ error: 'Failed to fetch videos' });
  }
});

function toComparable(ts) {
  if (!ts) return 0;
  if (typeof ts.toMillis === 'function') return ts.toMillis();
  if (typeof ts === 'number') return ts;
  const parsed = Date.parse(ts);
  return Number.isFinite(parsed) ? parsed : 0;
}

module.exports = router;
