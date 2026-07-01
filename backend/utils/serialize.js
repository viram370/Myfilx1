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

async function resolveFileUrl(fileId) {
  if (!fileId) return null;
  const cached = fileUrlCache.get(fileId);
  if (cached && cached.expiresAt > Date.now()) return cached.url;

  try {
    const { data } = await axios.get(`https://api.telegram.org/bot${BOT_TOKEN}/getFile`, {
      params: { file_id: fileId },
    });
    if (!data.ok) return null;
    const url = `https://api.telegram.org/file/bot${BOT_TOKEN}/${data.result.file_path}`;
    fileUrlCache.set(fileId, { url, expiresAt: Date.now() + 50 * 60 * 1000 });
    return url;
  } catch (err) {
    console.error('[serializer] resolveFileUrl error:', err.message);
    return null;
  }
}

async function serializeVideo(doc, { withImage = true } = {}) {
  if (!doc) return null;

  let thumbnail = doc.thumbnail || null;
  if (withImage && !thumbnail) {
    const imageFileId = doc.bannerFileId || doc.thumbFileId || null;
    thumbnail = await resolveFileUrl(imageFileId);
  }

  return {
    id: doc.id,
    category: doc.category || 'Uncategorized',
    title: doc.title || 'Untitled',
    year: doc.uploadDate ? new Date(doc.uploadDate).getFullYear().toString() : '',
    rating: doc.rating || null,
    genre: doc.genre || doc.category || '',
    description: doc.description || '',
    duration: doc.duration || 0,
    thumbnail,
    poster: thumbnail,
    banner: thumbnail,
    season: doc.season || null,
    episode: doc.episode || null,
    seriesTitle: doc.seriesTitle || (doc.season ? doc.title : null),
    views: doc.views || 0,
    likes: doc.likes || 0,
    published: !!doc.published,
    createdAt: doc.createdAt?.toDate ? doc.createdAt.toDate().toISOString() : doc.createdAt,
  };
}

async function serializeVideos(docs, opts) {
  return Promise.all(docs.map(d => serializeVideo(d, opts)));
}

function groupIntoSeasons(episodes) {
  const bySeasonNum = {};
  for (const ep of episodes) {
    const s = ep.season || 1;
    if (!bySeasonNum[s]) bySeasonNum[s] = [];
    bySeasonNum[s].push({
      ep: ep.episode || 1,
      title: ep.title,
      duration: ep.duration || 0,
      prog: ep.progressPercent || 0,
      videoId: ep.id,
      thumbnail: ep.thumbnail || null,
    });
  }
  return Object.keys(bySeasonNum)
    .sort((a, b) => Number(a) - Number(b))
    .map(num => ({
      num: Number(num),
      eps: bySeasonNum[num].sort((a, b) => a.ep - b.ep),
    }));
}

module.exports = { resolveFileUrl, serializeVideo, serializeVideos, groupIntoSeasons };
