/**
 * routes/users.js
 */
'use strict';

const express = require('express');
const router = express.Router();
const { getDoc, queryDocs } = require('../services/firebase');
const { requireAuth } = require('../middleware/auth');
const { clampInt } = require('../utils/validators');
const { serializeVideo } = require('../utils/serialize');
const { makeLogger } = require('../utils/logger');
const log = makeLogger('routes/users.js');

router.get('/me', requireAuth, async (req, res) => {
  try {
    const user = await getDoc('users', req.telegramUserId);
    res.json({ user: user || { telegramId: req.telegramUser.id, ...req.telegramUser } });
  } catch (err) {
    log.error('me', 'Failed to fetch profile', err);
    res.status(500).json({ error: 'Failed to fetch profile' });
  }
});

router.get('/me/continue-watching', requireAuth, async (req, res) => {
  try {
    const limit = clampInt(req.query.limit, { min: 1, max: 50, fallback: 10 });
    // One row per anime/series (see routes/stream.js's progress-save handler
    // — 'continueWatchingSeries' is keyed by userId + a slug of the
    // series/title, so watching a different episode of the same anime
    // updates this SAME doc instead of adding a new row). Sorted by
    // watchedAt desc, so the most recently watched show is always first.
    const items = await queryDocs(
      'continueWatchingSeries',
      [['userId', '==', req.telegramUserId], ['completed', '==', false]],
      ['watchedAt', 'desc'], limit
    );

    // Defensive de-dupe by seriesKey — belt-and-suspenders in case an
    // older per-episode-keyed doc is still lingering in this collection
    // from before this fix; never show the same anime twice.
    const seen = new Set();
    const deduped = [];
    for (const item of items) {
      const key = item.seriesKey || item.videoId;
      if (!key || seen.has(key)) continue;
      seen.add(key);
      deduped.push(item);
    }

    const enriched = await Promise.all(deduped.map(async (item) => {
      // item.videoId is always the exact episode last watched — fetching
      // THAT doc (never a different "representative" episode) is what
      // guarantees the season/episode/thumbnail shown here match what was
      // actually watched.
      const video = await getDoc('videos', item.videoId);
      if (!video || !video.published) return null;
      const serialized = await serializeVideo(video, { withImage: true });
      return {
        ...item,
        video: {
          ...serialized,
          // Prefer the season/episode captured at save-time (item.*) —
          // it's what the user actually watched — falling back to the
          // video doc's own fields only if that's somehow missing. Never
          // guessed, never left to whatever a different episode's doc
          // happened to say.
          season: item.season != null ? item.season : serialized.season,
          episode: item.episode != null ? item.episode : serialized.episode,
          watchProgress: item.progressPercent || 0,
        },
      };
    }));

    res.json({ items: enriched.filter(Boolean) });
  } catch (err) {
    log.error('continueWatching', 'Failed to fetch continue watching', err);
    res.status(500).json({ error: 'Failed to fetch continue watching' });
  }
});

module.exports = router;
