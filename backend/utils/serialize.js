
/**
 * Video Serializer
 * Converts Firestore `videos` documents into the exact object shape
 * the existing MYFLIX frontend expects (same as its DEMO_* arrays):
 *   { id, category, title, year, rating, genre, description,
 *     duration, thumbnail, poster, banner, seasons:[{num,eps:[...]}] }
 *
 * This lets the frontend's existing mkCard/showDetail/playVideo functions
 * work unmodified against real backend data.
 */
const axios = require('axios');
const { makeLogger } = require('./logger');
const log = makeLogger('utils/serialize.js');

const BOT_TOKEN = process.env.TELEGRAM_BOT_TOKEN;
const fileUrlCache = new Map();

/**
 * Resolve Telegram file URL with robust caching and error handling
 * Never throws — returns null on failure
 */
async function resolveFileUrl(fileId, { throwOnError = false } = {}) {
  if (!fileId || typeof fileId !== 'string') {
    console.warn('[resolveFileUrl] Invalid fileId:', fileId);
    return null;
  }

  const cached = fileUrlCache.get(fileId);
  if (cached && cached.expiresAt > Date.now()) {
    return cached.url;
  }

  try {
    const { data } = await axios.get(
      `https://api.telegram.org/bot${BOT_TOKEN}/getFile`,
      {
        params: { file_id: fileId },
        timeout: 10000
      }
    );

    if (!data.ok || !data.result?.file_path) {
      console.warn('[resolveFileUrl] Telegram API error:', data.description || 'No file_path');
      if (throwOnError) throw new Error(data.description || 'Telegram getFile failed');
      return null;
    }

    const url = `https://api.telegram.org/file/bot${BOT_TOKEN}/${data.result.file_path}`;

    // Cache with 50-minute TTL (Telegram URLs expire ~1 hour)
    fileUrlCache.set(fileId, {
      url,
      expiresAt: Date.now() + 50 * 60 * 1000
    });

    // Optional: cleanup old cache entries (prevent memory leak)
    if (fileUrlCache.size > 500) {
      const now = Date.now();
      for (const [key, entry] of fileUrlCache.entries()) {
        if (entry.expiresAt < now) fileUrlCache.delete(key);
      }
    }

    return url;

  } catch (err) {
    console.error('[resolveFileUrl] Error for fileId:', fileId, err.message || err);
    if (throwOnError) throw err;
    return null;
  }
}

/**
 * Serialize single video document
 */
async function serializeVideo(doc, { withImage = true } = {}) {
  if (!doc || typeof doc !== 'object') {
    console.warn('[serializeVideo] Invalid doc:', doc);
    return null;
  }

  let thumbnail = doc.thumbnail || null;

  if (withImage && !thumbnail) {
    const imageFileId = doc.thumbnailFileId || doc.bannerFileId || doc.thumbFileId || doc.telegram_file_id; // fallback
    if (imageFileId) {
      thumbnail = await resolveFileUrl(imageFileId);
    }
  }

  // The per-EPISODE display thumbnail — auto-generated from the episode's
  // own video (see backend/services/compress.js#generateEpisodeThumbnail),
  // completely separate from `thumbnail` above (which is always the
  // anime/series poster the admin uploaded by hand in /addanime and must
  // never be replaced by an episode frame). Falls back to the anime
  // poster — never null-vs-crash — if this specific episode has no
  // generated thumbnail yet (e.g. generation failed for it).
  let episodeThumbnail = thumbnail;
  if (withImage && doc.episodeThumbnailFileId) {
    const resolved = await resolveFileUrl(doc.episodeThumbnailFileId);
    if (resolved) episodeThumbnail = resolved;
  }

  return {
    id: doc.id || doc._id || null,

    category: doc.category || "Uncategorized",

    title: doc.title || "Untitled",

    year: doc.uploadDate
      ? new Date(doc.uploadDate).getFullYear().toString()
      : (doc.createdAt ? new Date(doc.createdAt).getFullYear().toString() : ""),

    rating: doc.rating || null,

    genre: doc.genre || doc.category || "",

    description: doc.description || "",

    duration: Number(doc.duration) || 0,

    thumbnail,
    poster: thumbnail,
    banner: thumbnail,
    // Individual-episode thumbnail — use ONLY when displaying this one
    // episode (episode list rows, continue watching, recently watched,
    // recommendations of individual episodes, the player's next-episode
    // card). Anime cards, search results, home page, categories and the
    // detail page header must keep using `thumbnail`/`poster`/`banner`
    // above instead.
    episodeThumbnail,

    season: doc.season != null ? Number(doc.season) : null,
    episode: doc.episode != null ? Number(doc.episode) : null,

    seriesTitle: doc.seriesTitle || (doc.season != null ? doc.title : null),

    views: Number(doc.views) || 0,
    likes: Number(doc.likes) || 0,

    published: !!doc.published,

    createdAt: doc.createdAt?.toDate
      ? doc.createdAt.toDate().toISOString()
      : (typeof doc.createdAt === 'number' ? new Date(doc.createdAt).toISOString() : doc.createdAt)
  };
}

/**
 * Serialize multiple videos (concurrent)
 */
async function serializeVideos(docs, opts = {}) {
  if (!Array.isArray(docs)) return [];
  const promises = docs.map(doc => serializeVideo(doc, opts));
  return Promise.all(promises);
}

/**
 * Group episodes into seasons for series cards.
 *
 * `fallbackThumbnail` should be the series/anime poster (already-resolved
 * URL) — used only when an individual episode has no auto-generated
 * thumbnail of its own (e.g. generation failed for that one episode), so
 * the UI never shows a broken image, and never silently reuses a STALE
 * value from `ep.thumbnail` the way this used to (that field is never
 * actually set on a raw Firestore doc, so every episode row used to come
 * back with `thumbnail: null` regardless of what was uploaded).
 */
async function groupIntoSeasons(episodes, fallbackThumbnail = null) {
  if (!Array.isArray(episodes) || episodes.length === 0) {
    return [];
  }

  const bySeasonNum = {};

  for (const ep of episodes) {
    if (!ep) continue;
    const s = ep.season != null ? Number(ep.season) : 1;
    if (!bySeasonNum[s]) bySeasonNum[s] = [];

    let epThumbnail = fallbackThumbnail;
    if (ep.episodeThumbnailFileId) {
      const resolved = await resolveFileUrl(ep.episodeThumbnailFileId);
      if (resolved) epThumbnail = resolved;
    }

    bySeasonNum[s].push({
      ep: ep.episode != null ? Number(ep.episode) : 1,
      title: ep.title || `Episode ${ep.episode || 1}`,
      duration: Number(ep.duration) || 0,
      prog: Number(ep.progressPercent) || 0,
      videoId: ep.id || ep._id,
      thumbnail: epThumbnail || null
    });
  }

  return Object.keys(bySeasonNum)
    .sort((a, b) => Number(a) - Number(b))
    .map(num => ({
      num: Number(num),
      eps: bySeasonNum[num].sort((a, b) => a.ep - b.ep)
    }));
}

module.exports = { 
  resolveFileUrl, 
  serializeVideo, 
  serializeVideos, 
  groupIntoSeasons 
};

