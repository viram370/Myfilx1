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
        // ROOT CAUSE FIX (anime/episode thumbnails "mixed together"):
        // `rep` is a single EPISODE document (whichever happened to be
        // episodes[0] for this series) used only to build the anime-level
        // card. serializeVideo() correctly returns `rep`'s OWN per-episode
        // `episodeThumbnail` alongside the anime poster (`thumbnail`) —
        // that's the right behavior for an actual episode object. But the
        // frontend's shared pickThumbnail() helper (public/index.html)
        // prefers `episodeThumbnail` over `thumbnail` whenever it's
        // present, on the assumption that a populated episodeThumbnail
        // means "this object describes a specific episode". Handing that
        // same field straight through on the anime-level card object
        // broke that assumption: Home/Search/Categories/Recommendations/
        // the anime detail view (which all render this exact object) were
        // silently showing episodes[0]'s own generated frame instead of
        // the anime poster the admin uploaded in /addanime — and since
        // which episode happens to be episodes[0] can change as episodes
        // are added, the anime card's image could visibly change even
        // though nothing about the anime poster itself ever did.
        // The anime-level card must always resolve to the anime poster,
        // so episodeThumbnail is reset to match `thumbnail` here — this
        // does not touch `serialized.seasons[].eps[].thumbnail` below,
        // which groupIntoSeasons() already resolves independently per
        // episode from each episode's own episodeThumbnailFileId.
        serialized.episodeThumbnail = serialized.thumbnail;
        serialized.seasons = await groupIntoSeasons(episodes, serialized.thumbnail);
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
