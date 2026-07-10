/**
 * services/parser.js
 * Parses Telegram captions / filenames like:
 *   "Demon Slayer S02E04 Hindi 720p"
 *   "The.Boys.S03E05.1080p.WEB-DL.mkv"
 *   "Pathaan (2023) 1080p Hindi Movie"
 * into { title, season, episode, language, quality, category }.
 *
 * This is intentionally a best-effort heuristic parser — Telegram captions
 * are unstructured text, so 100% accuracy is not achievable. Every field
 * is nullable; callers should fall back to sane defaults or ask the admin
 * to confirm when a field is missing.
 */
'use strict';

const QUALITY_TOKENS = ['4K', '2160p', '1440p', '1080p', '1080P', '720p', '720P', '480p', '360p', '240p', 'HDRip', 'HDTV', 'WEB-DL', 'WEBRip', 'BluRay', 'BRRip', 'DVDRip', 'CAM', 'HDCAM'];

const LANGUAGE_TOKENS = [
  'Hindi', 'English', 'Tamil', 'Telugu', 'Malayalam', 'Kannada', 'Bengali', 'Marathi',
  'Punjabi', 'Gujarati', 'Urdu', 'Japanese', 'Korean', 'Chinese', 'Spanish', 'French',
  'German', 'Russian', 'Arabic', 'Dual Audio', 'Multi Audio', 'Multi', 'Dubbed', 'Subbed', 'Sub',
];

const ANIME_HINTS = ['anime', 'ova', 'oad', 'sub', 'dub', 'japanese', 'crunchyroll'];
const NOISE_TOKENS = [
  /\[[^\]]*\]/g,          // [SiteName.com]
  /\([^)]*\d{4}[^)]*\)/g, // (2023) kept separately if needed, stripped here
  /@\w+/g,                // @channelusername
  /https?:\/\/\S+/gi,     // stray URLs
  /\.(mkv|mp4|avi|mov|webm|ts|m4v)$/i, // file extension
];

function findSeasonEpisode(text) {
  let m = text.match(/\bS(\d{1,2})\s*[-. ]?\s*E(?:P)?(\d{1,4})\b/i);
  if (m) return { season: parseInt(m[1], 10), episode: parseInt(m[2], 10), matched: [m[0]] };

  const seasonM = text.match(/\bSeason\s*[-. ]?\s*(\d{1,2})\b/i);
  const epM = text.match(/\bE(?:P|pisode)?\.?\s*[-. ]?\s*(\d{1,4})\b/i);
  if (seasonM || epM) {
    return {
      season: seasonM ? parseInt(seasonM[1], 10) : null,
      episode: epM ? parseInt(epM[1], 10) : null,
      matched: [seasonM?.[0], epM?.[0]].filter(Boolean),
    };
  }
  return { season: null, episode: null, matched: [] };
}

function findQuality(text) {
  const allMatched = [];
  let primary = null;
  for (const q of QUALITY_TOKENS) {
    const re = new RegExp(`\\b${q.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}\\b`, 'ig');
    const m = text.match(re);
    if (m) {
      allMatched.push(...m);
      if (!primary) primary = q;
    }
  }
  return { quality: primary, matched: allMatched };
}

function findLanguages(text) {
  const found = [];
  const matched = [];
  for (const lang of LANGUAGE_TOKENS) {
    const re = new RegExp(`\\b${lang.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}\\b`, 'i');
    const m = text.match(re);
    if (m) {
      found.push(lang);
      matched.push(m[0]);
    }
  }
  return { language: found.length ? found.join('+') : null, matched };
}

function guessCategory(text, { season, episode }) {
  const lower = text.toLowerCase();
  const hasAnimeHint = ANIME_HINTS.some((h) => lower.includes(h));
  if (hasAnimeHint) return 'Anime';
  if (season !== null || episode !== null) return 'Web Series';
  return 'Movies';
}

function cleanTitle(raw, matchedTokens) {
  let t = raw;
  for (const re of NOISE_TOKENS) t = t.replace(re, ' ');
  for (const tok of matchedTokens) {
    if (!tok) continue;
    t = t.replace(new RegExp(tok.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'), 'i'), ' ');
  }
  t = t.replace(/[._]+/g, ' ');
  t = t.replace(/[-|:]+$/g, ' ');
  t = t.replace(/\s{2,}/g, ' ').trim();
  t = t.replace(/^[\s\-–—.]+|[\s\-–—.]+$/g, '');
  return t || null;
}

/**
 * Parse a raw caption or filename string into structured media info.
 * Never throws — returns a fully-null-safe object on empty/garbage input.
 */
function parseMediaInfo(rawText) {
  const text = String(rawText || '').trim();
  if (!text) {
    return { title: null, season: null, episode: null, language: null, quality: null, category: null, raw: rawText || '' };
  }

  const se = findSeasonEpisode(text);
  const q = findQuality(text);
  const l = findLanguages(text);
  const category = guessCategory(text, se);

  const allMatched = [...se.matched, ...q.matched, ...l.matched].filter(Boolean);
  const title = cleanTitle(text, allMatched);

  return {
    title,
    season: se.season,
    episode: se.episode,
    language: l.language,
    quality: q.quality,
    category,
    raw: text,
  };
}

module.exports = { parseMediaInfo, QUALITY_TOKENS, LANGUAGE_TOKENS };
