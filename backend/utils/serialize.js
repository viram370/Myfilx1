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
    const imageFileId = doc.bannerFileId || doc.thumbFileId || doc.telegram_file_id; // fallback
    if (imageFileId) {
      thumbnail = await resolveFileUrl(imageFileId);
    }
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
 * Group episodes into seasons for series cards
 */
function groupIntoSeasons(episodes) {
  if (!Array.isArray(episodes) || episodes.length === 0) {
    return [];
  }

  const bySeasonNum = {};

  for (const ep of episodes) {
    if (!ep) continue;
    const s = ep.season != null ? Number(ep.season) : 1;
    if (!bySeasonNum[s]) bySeasonNum[s] = [];
    
    bySeasonNum[s].push({
      ep: ep.episode != null ? Number(ep.episode) : 1,
      title: ep.title || `Episode ${ep.episode || 1}`,
      duration: Number(ep.duration) || 0,
      prog: Number(ep.progressPercent) || 0,
      videoId: ep.id || ep._id,
      thumbnail: ep.thumbnail || null
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
