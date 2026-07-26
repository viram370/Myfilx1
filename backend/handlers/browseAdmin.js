/**
 * handlers/browseAdmin.js
 * ----------------------------------------------------------------------
 * Hierarchical catalog browser: /anime, /movie, /webseries.
 * Replaces the old flat /listanime, /listmovies, /listwebseries commands
 * with a drill-down flow driven entirely by inline-keyboard buttons:
 *
 *   /anime            -> pick a title
 *     -> pick a season (+ 🗑 Delete Anime)
 *       -> pick an episode (+ ⬅ Back, 🗑 Delete Season)
 *         -> manage: 🎥 Replace Video / ✏ Edit Details / 🗑 Delete Episode (+ ⬅ Back)
 *
 *   /webseries works identically to /anime (same season/episode hierarchy).
 *
 *   /movie            -> pick a title
 *     -> manage: 🎥 Replace Video / ✏ Edit Name / 🗑 Delete (+ ⬅ Back)
 *
 * This module is entirely additive — it's a new, self-contained feature
 * registered alongside (not instead of) the existing /add and /saveanime
 * flows. It never touches their state, only reads/writes Firestore's
 * `videos` collection via services/firebase.js's existing helpers.
 *
 * Callback data stays well under Telegram's 64-byte limit by never
 * embedding the title string itself — navigation only ever carries a
 * category code + small integers (a title's index into that chat's
 * currently-cached title list, or a season/episode number). The actual
 * title/season/episode context lives server-side in `browseState`, keyed
 * by chatId — exactly the model /add's own wizard state already uses.
 * ----------------------------------------------------------------------
 */
'use strict';

const crypto = require('crypto');
const path = require('path');
const fs = require('fs');
const os = require('os');
const { queryDocs, getDoc, updateDoc, deleteDoc, batchDelete } = require('../services/firebase');
const compress = require('../services/compress');
const transfer = require('../services/telegramUpload');
const mtproto = require('../services/mtproto');
const uploadRecovery = require('../services/uploadRecovery');
const { makeLogger } = require('../utils/logger');
const log = makeLogger('handlers/browseAdmin.js');

const VIDEOS_COLLECTION = 'videos';
const CATEGORIES = Object.freeze({ ANIME: 'Anime', MOVIES: 'Movies', WEBSERIES: 'Web Series' });
const CATEGORY_CODE = { [CATEGORIES.ANIME]: 'A', [CATEGORIES.MOVIES]: 'M', [CATEGORIES.WEBSERIES]: 'W' };
const CODE_CATEGORY = { A: CATEGORIES.ANIME, M: CATEGORIES.MOVIES, W: CATEGORIES.WEBSERIES };
// Only anime/webseries ever reach the season/episode screens (movies branch
// to renderMovieScreen before this matters) — used by the new Add Season /
// Add Episode buttons to call adminUpload.startPrefilledBatch() with the
// same "kind" the normal /add command would have used.
const KIND_FOR_CATEGORY = { [CATEGORIES.ANIME]: 'anime', [CATEGORIES.WEBSERIES]: 'webseries' };

const TITLE_CACHE_TTL_MS = 20_000;
const PENDING_TTL_MS = 2 * 60_000;

// Compress & Replace Episode feature — its own temp dir, entirely separate
// from queue/pipeline.js's UPLOADS_DIR/CONVERTED_DIR (never shares state
// with the /add pipeline).
const COMPRESS_TMP_DIR = path.join(os.tmpdir(), 'myflix-compress-episode');
fs.mkdirSync(COMPRESS_TMP_DIR, { recursive: true });

// Button budget (per admin request): every screen tops out at 27 buttons
// total, counting the Delete button and any pagination arrows. That means
// up to 25 item buttons (title/season/episode) when nothing else needs to
// share the screen, or up to 23 when pagination arrows (⬅/➡, up to 2) are
// also present — 23 + 2 arrows + Delete + Back = 27 in the worst case.
const NO_PAGE_MAX = 25;
const PAGED_SIZE = 23;

/** Slices `items` into a page, deciding per-page size from the button budget above. */
function paginateItems(items, page) {
  const needsPaging = items.length > NO_PAGE_MAX;
  const perPage = needsPaging ? PAGED_SIZE : (items.length || 1);
  const totalPages = Math.max(1, Math.ceil(items.length / perPage));
  const clamped = Math.min(Math.max(page, 0), totalPages - 1);
  const slice = items.slice(clamped * perPage, clamped * perPage + perPage);
  return { slice, clamped, totalPages, needsPaging };
}

/** Builds a ⬅/➡ row for the given callback-prefix; empty array if only one page. */
function navRowFor(prefix, clamped, totalPages) {
  if (totalPages <= 1) return [];
  const row = [];
  if (clamped > 0) row.push({ text: '⬅', callback_data: `${prefix}${clamped - 1}` });
  if (clamped < totalPages - 1) row.push({ text: '➡', callback_data: `${prefix}${clamped + 1}` });
  return row.length ? [row] : [];
}

/** @type {Map<string, {titles:object[], fetchedAt:number}>} category code -> cached title groups */
const titleCache = new Map();
/** @type {Map<number, object>} chatId -> current browse/navigation state */
const browseState = new Map();
/** @type {Map<string, {chatId:number, kind:string, payload:object, createdAt:number}>} */
const pendingActions = new Map();

setInterval(() => {
  const now = Date.now();
  for (const [k, v] of pendingActions.entries()) {
    if (now - v.createdAt > PENDING_TTL_MS) pendingActions.delete(k);
  }
}, 60_000).unref?.();

function shortToken() { return crypto.randomBytes(5).toString('hex'); }
function escapeHtml(str) { return String(str).replace(/[&<>]/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;' }[c])); }
function pluralCategory(category) { return category === CATEGORIES.WEBSERIES ? 'Web Series' : category; }
function extractVideoMedia(msg) {
  if (msg.video) return msg.video;
  if (msg.document?.mime_type?.startsWith('video/')) return msg.document;
  return null;
}

let bot, isAdmin, safeSendMessage, safeEditMessageText, isAddSessionActive, startPrefilledBatch;

function registerBrowseAdmin(botInstance, deps) {
  bot = botInstance;
  isAdmin = deps.isAdmin;
  safeSendMessage = deps.safeSendMessage;
  safeEditMessageText = deps.safeEditMessageText;
  isAddSessionActive = deps.isAddSessionActive || (() => false);
  startPrefilledBatch = deps.startPrefilledBatch;

  bot.onText(/^\/anime(?:@\w+)?\s*$/i, (msg) => handleEntry(msg, CATEGORIES.ANIME).catch(logErr('handleEntry(anime)')));
  bot.onText(/^\/movie(?:@\w+)?\s*$/i, (msg) => handleEntry(msg, CATEGORIES.MOVIES).catch(logErr('handleEntry(movie)')));
  bot.onText(/^\/webseries(?:@\w+)?\s*$/i, (msg) => handleEntry(msg, CATEGORIES.WEBSERIES).catch(logErr('handleEntry(webseries)')));

  bot.on('callback_query', (query) => handleCallback(query).catch(logErr('handleCallback')));
  bot.on('message', (msg) => handleText(msg).catch(logErr('handleText')));
  bot.on('video', (msg) => handleMedia(msg, msg.video).catch(logErr('handleMedia(video)')));
  bot.on('document', (msg) => {
    const media = extractVideoMedia(msg);
    if (media) handleMedia(msg, media).catch(logErr('handleMedia(document)'));
  });

  // Compress & Replace Episode: resume anything a Render restart
  // interrupted. Fire-and-forget — must never block bot startup.
  resumePendingCompressJobs().catch(logErr('resumePendingCompressJobs'));
}

function logErr(label) {
  return (err) => log.error(label, 'failed', err, { stack: err.stack });
}

/** Exported so services/bot.js's legacy direct-video buffer can skip a chat mid "Replace Video". */
function isAwaitingMedia(chatId) {
  const state = browseState.get(chatId);
  return !!(state && state.awaiting === 'replaceVideo');
}

// ============================================================================
// ENTRY POINTS
// ============================================================================

async function handleEntry(msg, category) {
  const chatId = msg.chat.id;
  if (!isAdmin(chatId)) return;
  browseState.set(chatId, { category, code: CATEGORY_CODE[category], page: 0, titles: [], createdAt: Date.now() });
  await renderTitleList(chatId, category, 0, null);
}

// ============================================================================
// DATA
// ============================================================================

async function fetchTitles(category) {
  const code = CATEGORY_CODE[category];
  const cached = titleCache.get(code);
  if (cached && Date.now() - cached.fetchedAt < TITLE_CACHE_TTL_MS) return cached.titles;

  const docs = await queryDocs(VIDEOS_COLLECTION, [['category', '==', category]], null, 1000);
  const byTitle = new Map();
  for (const d of docs) {
    const key = d.title || '(untitled)';
    if (!byTitle.has(key)) byTitle.set(key, []);
    byTitle.get(key).push(d);
  }
  const titles = [...byTitle.entries()].map(([title, ds]) => {
    ds.sort((a, b) => (a.season || 0) - (b.season || 0) || (a.episode || 0) - (b.episode || 0));
    const rep = ds[ds.length - 1];
    return {
      title,
      docs: ds,
      language: rep.language || '—',
      quality: rep.quality || '—',
    };
  }).sort((a, b) => a.title.localeCompare(b.title));

  titleCache.set(code, { titles, fetchedAt: Date.now() });
  return titles;
}

function invalidateTitles(category) { titleCache.delete(CATEGORY_CODE[category]); }

// ============================================================================
// RENDERERS
// ============================================================================

function fieldBlock(t, category, extra = {}) {
  const lines = [
    `ID: ${escapeHtml(t.title)}`,
    `Language: ${escapeHtml(t.language)}`,
    `Quality: ${escapeHtml(t.quality)}`,
    `Type: ${pluralCategory(category)}`,
  ];
  if (extra.season != null) lines.push(`Season: ${extra.season}`);
  if (extra.episode != null) lines.push(`Episode: ${extra.episode}`);
  return lines.join('\n');
}

async function renderTitleList(chatId, category, page, messageId) {
  const titles = await fetchTitles(category);
  const { slice, clamped, totalPages } = paginateItems(titles, page);
  const code = CATEGORY_CODE[category];

  browseState.set(chatId, { category, code, page: clamped, titles, awaiting: null });

  const label = pluralCategory(category);
  const pageInfo = totalPages > 1 ? ` — Page ${clamped + 1}/${totalPages}` : '';
  const text = titles.length
    ? `📂 <b>${label}</b> — ${titles.length} title${titles.length === 1 ? '' : 's'}${pageInfo}\n\nSelect one to manage:`
    : `📂 <b>${label}</b>\n\nNothing here yet.`;

  const rows = slice.map((t) => [{ text: t.title.slice(0, 60), callback_data: `bx:t:${titles.indexOf(t)}` }]);
  rows.push(...navRowFor(`bx:cat:${code}:`, clamped, totalPages));

  const options = { reply_markup: { inline_keyboard: rows } };
  if (messageId) await safeEditMessageText(text, { chat_id: chatId, message_id: messageId, ...options });
  else await safeSendMessage(chatId, text, options);
}

/**
 * Every field a fresh /add wizard would normally ask for is already known
 * for an EXISTING title — reused here (never re-asked) so Add Season /
 * Add Episode only ever need the video file itself. Poster thumbnail is
 * deliberately taken from an existing doc so it's preserved exactly, per
 * requirement ("Preserve anime thumbnail").
 */
function prefillFieldsFor(t, overrides = {}) {
  const rep = t.docs[t.docs.length - 1];
  return {
    title: t.title,
    language: rep.language || 'Hindi',
    quality: rep.quality || null,
    year: rep.year || null,
    thumbnailFileId: rep.thumbnailFileId || null,
    ...overrides,
  };
}

async function renderTitleScreen(chatId, titleIdx, messageId, page = 0) {
  const state = browseState.get(chatId);
  if (!state) return;
  const t = state.titles[titleIdx];
  if (!t) return;
  state.titleIdx = titleIdx; state.season = null; state.episode = null; state.awaiting = null;

  if (state.category === CATEGORIES.MOVIES) return renderMovieScreen(chatId, titleIdx, messageId);

  const seasons = [...new Set(t.docs.map((d) => d.season).filter((s) => s != null))].sort((a, b) => a - b);
  const { slice, clamped, totalPages } = paginateItems(seasons, page);
  state.seasonPage = clamped;

  const pageInfo = totalPages > 1 ? ` (Page ${clamped + 1}/${totalPages})` : '';
  const text = `${fieldBlock(t, state.category)}\n\nChoose a season:${pageInfo}`;
  const rows = slice.map((s) => [{ text: `Season ${s}`, callback_data: `bx:s:${s}` }]);
  rows.push(...navRowFor('bx:sp:', clamped, totalPages));
  rows.push([{ text: '➕ Add Season', callback_data: 'bx:addSeason' }]);
  const deleteLabel = state.category === CATEGORIES.WEBSERIES ? '🗑 Delete Series' : '🗑 Delete Anime';
  rows.push([{ text: deleteLabel, callback_data: 'bx:delTitle' }]);
  rows.push([{ text: '⬅ Back', callback_data: 'bx:back:cat' }]);
  await safeEditMessageText(text, { chat_id: chatId, message_id: messageId, reply_markup: { inline_keyboard: rows } });
}

async function renderMovieScreen(chatId, titleIdx, messageId) {
  const state = browseState.get(chatId);
  const t = state.titles[titleIdx];
  const text = `${fieldBlock(t, state.category)}\n\nManage this movie:`;
  const rows = [
    [{ text: '🎥 Replace Video', callback_data: 'bx:replaceVideo' }],
    [{ text: '✏ Edit Name', callback_data: 'bx:editName' }],
    [{ text: '🗑 Delete', callback_data: 'bx:delMovie' }],
    [{ text: '⬅ Back', callback_data: 'bx:back:cat' }],
  ];
  await safeEditMessageText(text, { chat_id: chatId, message_id: messageId, reply_markup: { inline_keyboard: rows } });
}

async function renderSeasonScreen(chatId, season, messageId, page = 0) {
  const state = browseState.get(chatId);
  if (!state) return;
  const t = state.titles[state.titleIdx];
  if (!t) return;
  state.season = season; state.episode = null; state.awaiting = null;

  const episodes = t.docs.filter((d) => d.season === season).map((d) => d.episode).filter((e) => e != null).sort((a, b) => a - b);
  const { slice, clamped, totalPages } = paginateItems(episodes, page);
  state.episodePage = clamped;

  const pageInfo = totalPages > 1 ? ` (Page ${clamped + 1}/${totalPages})` : '';
  const text = `${fieldBlock(t, state.category, { season })}\n\nChoose an episode:${pageInfo}`;
  const rows = [];
  for (let i = 0; i < slice.length; i += 4) {
    rows.push(slice.slice(i, i + 4).map((e) => ({ text: `Episode ${e}`, callback_data: `bx:e:${e}` })));
  }
  rows.push(...navRowFor('bx:ep:', clamped, totalPages));
  rows.push([{ text: '➕ Add Episode', callback_data: 'bx:addEpisode' }]);
  rows.push([{ text: '🗑 Delete Season', callback_data: 'bx:delSeason' }]);
  rows.push([{ text: '⬅ Back', callback_data: 'bx:back:title' }]);
  await safeEditMessageText(text, { chat_id: chatId, message_id: messageId, reply_markup: { inline_keyboard: rows } });
}

async function renderEpisodeScreen(chatId, episode, messageId) {
  const state = browseState.get(chatId);
  if (!state) return;
  const t = state.titles[state.titleIdx];
  if (!t) return;
  state.episode = episode; state.awaiting = null;

  const text = `${fieldBlock(t, state.category, { season: state.season, episode })}\n\nManage this episode:`;
  const rows = [
    [{ text: '🎥 Replace Video', callback_data: 'bx:replaceVideo' }],
    [{ text: '🎥 Compress', callback_data: 'bx:compress' }],
    [{ text: '✏ Edit Details', callback_data: 'bx:editDetails' }],
    [{ text: '🗑 Delete Episode', callback_data: 'bx:delEpisode' }],
    [{ text: '⬅ Back', callback_data: 'bx:back:season' }],
  ];
  await safeEditMessageText(text, { chat_id: chatId, message_id: messageId, reply_markup: { inline_keyboard: rows } });
}

// ============================================================================
// CALLBACKS
// ============================================================================

async function handleCallback(query) {
  const chatId = query.message?.chat?.id;
  const messageId = query.message?.message_id;
  const data = query.data || '';
  if (!chatId || !data.startsWith('bx:') || !isAdmin(chatId)) return;

  const ack = (opts) => bot.answerCallbackQuery(query.id, opts).catch(() => {});

  if (data === 'bx:noop') { await ack(); return; }

  if (data.startsWith('bx:cat:')) {
    const [, , code, pageStr] = data.split(':');
    await renderTitleList(chatId, CODE_CATEGORY[code], parseInt(pageStr, 10) || 0, messageId);
    await ack();
    return;
  }

  if (data.startsWith('bx:cy:') || data.startsWith('bx:cn:')) {
    const confirmed = data.startsWith('bx:cy:');
    const tok = data.slice(6);
    const pending = pendingActions.get(tok);
    if (!pending) { await ack({ text: '⌛ This confirmation expired.', show_alert: true }); return; }
    pendingActions.delete(tok);
    if (!confirmed) {
      await ack({ text: 'Cancelled.' });
      await safeEditMessageText('❌ Cancelled.', { chat_id: chatId, message_id: messageId });
      return;
    }
    await ack({ text: 'Processing…' });
    await runPendingAction(pending, messageId);
    return;
  }

  if (data.startsWith('bx:cxo:') || data.startsWith('bx:cxf:')) {
    const docId = data.slice(7);
    const resolver = pendingQualityChoice.get(docId);
    if (resolver) { resolver(data.startsWith('bx:cxo:') ? 'original' : 'force'); await ack(); }
    else await ack({ text: '⌛ This prompt expired.', show_alert: true });
    return;
  }

  const state = browseState.get(chatId);
  if (!state) { await ack({ text: 'Session expired — send /anime, /movie or /webseries again.', show_alert: true }); return; }

  if (data.startsWith('bx:t:')) { await renderTitleScreen(chatId, parseInt(data.slice(5), 10), messageId); await ack(); return; }
  if (data === 'bx:back:cat') { await renderTitleList(chatId, state.category, state.page || 0, messageId); await ack(); return; }
  if (data.startsWith('bx:sp:')) { await renderTitleScreen(chatId, state.titleIdx, messageId, parseInt(data.slice(6), 10)); await ack(); return; }
  if (data.startsWith('bx:s:')) { await renderSeasonScreen(chatId, parseInt(data.slice(5), 10), messageId); await ack(); return; }
  if (data === 'bx:back:title') { await renderTitleScreen(chatId, state.titleIdx, messageId, state.seasonPage || 0); await ack(); return; }
  if (data.startsWith('bx:ep:')) { await renderSeasonScreen(chatId, state.season, messageId, parseInt(data.slice(6), 10)); await ack(); return; }
  if (data.startsWith('bx:e:')) { await renderEpisodeScreen(chatId, parseInt(data.slice(5), 10), messageId); await ack(); return; }
  if (data === 'bx:back:season') { await renderSeasonScreen(chatId, state.season, messageId, state.episodePage || 0); await ack(); return; }

  if (data === 'bx:delTitle') { await confirmDelete(chatId, messageId, state, 'deleteTitle'); await ack(); return; }
  if (data === 'bx:delSeason') { await confirmDelete(chatId, messageId, state, 'deleteSeason'); await ack(); return; }
  if (data === 'bx:delEpisode') { await confirmDelete(chatId, messageId, state, 'deleteEpisode'); await ack(); return; }
  if (data === 'bx:delMovie') { await confirmDelete(chatId, messageId, state, 'deleteMovie'); await ack(); return; }
  if (data === 'bx:compress') { await confirmCompress(chatId, messageId, state); await ack(); return; }

  if (data === 'bx:addSeason') {
    if (!KIND_FOR_CATEGORY[state.category]) { await ack(); return; } // movies have no seasons — button is never rendered for them, but guard anyway
    if (isAddSessionActive(chatId)) {
      await ack({ text: 'Finish or /done your current /add batch first.', show_alert: true });
      return;
    }
    state.awaiting = 'addSeason';
    await safeEditMessageText('🔢 Send the new season number (e.g. 4).', { chat_id: chatId, message_id: messageId });
    await ack();
    return;
  }

  if (data === 'bx:addEpisode') {
    if (!KIND_FOR_CATEGORY[state.category]) { await ack(); return; }
    if (isAddSessionActive(chatId)) {
      await ack({ text: 'Finish or /done your current /add batch first.', show_alert: true });
      return;
    }
    const t = state.titles[state.titleIdx];
    const kind = KIND_FOR_CATEGORY[state.category];
    const started = startPrefilledBatch(chatId, kind, prefillFieldsFor(t, { season: state.season }));
    if (!started) {
      await ack({ text: 'Finish or /done your current /add batch first.', show_alert: true });
      return;
    }
    await safeEditMessageText(
      `✅ <b>${escapeHtml(t.title)}</b> — Season ${state.season}\n\n` +
      `Send the video for the next episode, then send <b>Done</b> (or /done). ` +
      `The episode number is assigned automatically.`,
      { chat_id: chatId, message_id: messageId, parse_mode: 'HTML' }
    );
    await ack();
    return;
  }

  if (data === 'bx:replaceVideo') {
    state.awaiting = 'replaceVideo';
    const t = state.titles[state.titleIdx];
    const label = state.category === CATEGORIES.MOVIES
      ? escapeHtml(t.title)
      : `${escapeHtml(t.title)}\nSeason ${state.season}\nEpisode ${state.episode}`;
    await safeEditMessageText(`Send the new video for:\n\n${label}`, { chat_id: chatId, message_id: messageId });
    await ack();
    return;
  }
  if (data === 'bx:editDetails') {
    state.awaiting = 'editDetails';
    await safeEditMessageText(
      '✏ Send the new details as:\n<code>Language|Quality</code>\n\nLeave a part blank to keep it unchanged (e.g. <code>|1080p</code> to change only quality).',
      { chat_id: chatId, message_id: messageId }
    );
    await ack();
    return;
  }
  if (data === 'bx:editName') {
    state.awaiting = 'editName';
    await safeEditMessageText('✏ Send the new name for this movie.', { chat_id: chatId, message_id: messageId });
    await ack();
    return;
  }

  await ack();
}

// ============================================================================
// DELETE (confirm + execute)
// ============================================================================

async function confirmDelete(chatId, messageId, state, kind) {
  const t = state.titles[state.titleIdx];
  let prompt;
  let payload;

  if (kind === 'deleteTitle') {
    prompt = `Delete <b>${escapeHtml(t.title)}</b>?\n\nThis will delete every season and every episode.`;
    payload = { category: state.category, title: t.title };
  } else if (kind === 'deleteSeason') {
    prompt = `Delete Season ${state.season}?\n\nAll episodes in this season will be deleted.`;
    payload = { category: state.category, title: t.title, season: state.season };
  } else if (kind === 'deleteEpisode') {
    const doc = t.docs.find((d) => d.season === state.season && d.episode === state.episode);
    prompt = `Are you sure you want to delete\n<b>${escapeHtml(t.title)}</b>\nSeason ${state.season}\nEpisode ${state.episode}?`;
    payload = { docId: doc?.id, category: state.category };
  } else if (kind === 'deleteMovie') {
    const doc = t.docs[0];
    prompt = `Delete <b>${escapeHtml(t.title)}</b>?`;
    payload = { docId: doc?.id, category: state.category };
  }

  const tok = shortToken();
  pendingActions.set(tok, { chatId, kind, payload, createdAt: Date.now() });
  await safeEditMessageText(`⚠ ${prompt}`, {
    chat_id: chatId,
    message_id: messageId,
    reply_markup: {
      inline_keyboard: [[
        { text: kind === 'deleteSeason' ? '✅ Delete Season' : kind === 'deleteTitle' ? `✅ Delete ${pluralCategory(state.category) === 'Web Series' ? 'Series' : 'Anime'}` : '✅ Yes, Delete',
          callback_data: `bx:cy:${tok}` },
        { text: '❌ Cancel', callback_data: `bx:cn:${tok}` },
      ]],
    },
  });
}

async function runPendingAction(pending, messageId) {
  const { chatId, kind, payload } = pending;
  const backRow = (category) => ({ inline_keyboard: [[{ text: '⬅ Back to List', callback_data: `bx:cat:${CATEGORY_CODE[category]}:0` }]] });

  try {
    if (kind === 'deleteTitle') {
      const docs = await queryDocs(VIDEOS_COLLECTION, [['category', '==', payload.category], ['title', '==', payload.title]]);
      await batchDelete(VIDEOS_COLLECTION, docs.map((d) => d.id));
      invalidateTitles(payload.category);
      await safeEditMessageText(`🗑 Deleted <b>${escapeHtml(payload.title)}</b> — ${docs.length} document(s) removed.`, { chat_id: chatId, message_id: messageId, reply_markup: backRow(payload.category) });
    } else if (kind === 'deleteSeason') {
      const docs = await queryDocs(VIDEOS_COLLECTION, [['category', '==', payload.category], ['title', '==', payload.title], ['season', '==', payload.season]]);
      await batchDelete(VIDEOS_COLLECTION, docs.map((d) => d.id));
      invalidateTitles(payload.category);
      await safeEditMessageText(`🗑 Deleted Season ${payload.season} — ${docs.length} episode(s) removed.`, { chat_id: chatId, message_id: messageId, reply_markup: backRow(payload.category) });
    } else if (kind === 'deleteEpisode') {
      if (payload.docId) await deleteDoc(VIDEOS_COLLECTION, payload.docId);
      invalidateTitles(payload.category);
      await safeEditMessageText('🗑 Episode deleted.', { chat_id: chatId, message_id: messageId, reply_markup: backRow(payload.category) });
    } else if (kind === 'deleteMovie') {
      if (payload.docId) await deleteDoc(VIDEOS_COLLECTION, payload.docId);
      invalidateTitles(payload.category);
      await safeEditMessageText('🗑 Movie deleted.', { chat_id: chatId, message_id: messageId, reply_markup: backRow(payload.category) });
    } else if (kind === 'renameMovie') {
      await updateDoc(VIDEOS_COLLECTION, payload.docId, { title: payload.newTitle, seriesTitle: payload.newTitle });
      invalidateTitles(payload.category);
      await safeEditMessageText(`✅ Renamed to <b>${escapeHtml(payload.newTitle)}</b>.`, { chat_id: chatId, message_id: messageId, reply_markup: backRow(payload.category) });
    } else if (kind === 'compressEpisode') {
      // Long-running (download+compress+upload can take minutes) — this
      // manages its own progress message via renderCompressProgress() as
      // it moves through phases, so nothing further happens here after
      // it returns; see runCompressEpisodeJob for the full flow and its
      // safety ordering (Firestore/delete only after full verification).
      await runCompressEpisodeJob({ ...payload, chatId, messageId });
    }
  } catch (err) {
    log.error('runPendingAction', `${kind} failed`, err, { stack: err.stack });
    await safeEditMessageText(`❌ Something went wrong: ${err.message}`, { chat_id: chatId, message_id: messageId });
  }
}

// ============================================================================
// TEXT INPUT (Edit Details / Edit Name)
// ============================================================================

async function handleText(msg) {
  const chatId = msg.chat.id;
  if (!isAdmin(chatId)) return;
  if (!msg.text) return;
  if (msg.text.startsWith('/')) return; // commands handled by their own listeners
  if (isAddSessionActive(chatId)) return; // /add wizard owns this chat right now

  const state = browseState.get(chatId);
  if (!state || !state.awaiting) return;

  if (state.awaiting === 'editDetails') {
    const t = state.titles[state.titleIdx];
    const doc = t?.docs.find((d) => d.season === state.season && d.episode === state.episode);
    if (!doc) { state.awaiting = null; return; }

    const [langRaw, qualRaw] = msg.text.split('|');
    const updates = {};
    if (langRaw && langRaw.trim()) updates.language = langRaw.trim();
    if (qualRaw && qualRaw.trim()) updates.quality = qualRaw.trim();
    if (Object.keys(updates).length === 0) { await safeSendMessage(chatId, '⚠️ Nothing to update — send at least one value.'); return; }

    await updateDoc(VIDEOS_COLLECTION, doc.id, updates);
    invalidateTitles(state.category);
    state.awaiting = null;
    await safeSendMessage(chatId, '✅ Episode updated successfully.');
    return;
  }

  if (state.awaiting === 'editName') {
    const newTitle = msg.text.trim();
    if (!newTitle || newTitle.length > 200) { await safeSendMessage(chatId, '⚠️ Title must be 1-200 characters.'); return; }
    const t = state.titles[state.titleIdx];
    const doc = t?.docs[0];
    if (!doc) { state.awaiting = null; return; }
    state.awaiting = null;

    const tok = shortToken();
    pendingActions.set(tok, { chatId, kind: 'renameMovie', payload: { docId: doc.id, newTitle, category: state.category }, createdAt: Date.now() });
    await safeSendMessage(chatId, `Rename <b>${escapeHtml(t.title)}</b> to <b>${escapeHtml(newTitle)}</b>?`, {
      reply_markup: {
        inline_keyboard: [[
          { text: '✅ Save', callback_data: `bx:cy:${tok}` },
          { text: '❌ Cancel', callback_data: `bx:cn:${tok}` },
        ]],
      },
    });
    return;
  }

  if (state.awaiting === 'addSeason') {
    const raw = msg.text.trim();
    const n = parseInt(raw, 10);
    if (!Number.isInteger(n) || String(n) !== raw || n < 1 || n > 999) {
      await safeSendMessage(chatId, '⚠️ Season must be a whole number between 1 and 999. Send it again.');
      return;
    }
    const t = state.titles[state.titleIdx];
    if (!t) { state.awaiting = null; return; }
    const existingSeasons = new Set(t.docs.map((d) => d.season).filter((s) => s != null));
    if (existingSeasons.has(n)) {
      await safeSendMessage(chatId, `⚠️ Season ${n} already exists — open it from the season list to add more episodes, or send a different season number.`);
      return; // stay in 'addSeason' so they can just retry
    }
    state.awaiting = null;

    if (isAddSessionActive(chatId)) {
      await safeSendMessage(chatId, '⚠️ Finish or /done your current /add batch first, then try Add Season again.');
      return;
    }
    const kind = KIND_FOR_CATEGORY[state.category];
    const started = startPrefilledBatch(chatId, kind, prefillFieldsFor(t, { season: n }));
    if (!started) {
      await safeSendMessage(chatId, '⚠️ Finish or /done your current /add batch first, then try Add Season again.');
      return;
    }
    invalidateTitles(state.category); // the season won't actually appear until an episode is uploaded, but keeps the cache honest for whatever's pending
    await safeSendMessage(
      chatId,
      `✅ <b>${escapeHtml(t.title)}</b> — Season ${n} will be created with its first episode.\n\n` +
      `Send the video for Season ${n} Episode 1, then send <b>Done</b> (or /done).`
    );
    return;
  }
}

// ============================================================================
// MEDIA INPUT (Replace Video)
// ============================================================================

async function handleMedia(msg, media) {
  const chatId = msg.chat.id;
  if (!isAdmin(chatId)) return;
  const state = browseState.get(chatId);
  if (!state || state.awaiting !== 'replaceVideo') return;

  const t = state.titles[state.titleIdx];
  const doc = state.category === CATEGORIES.MOVIES
    ? t?.docs[0]
    : t?.docs.find((d) => d.season === state.season && d.episode === state.episode);
  if (!doc) { state.awaiting = null; return; }

  await updateDoc(VIDEOS_COLLECTION, doc.id, {
    telegram_file_id: media.file_id,
    file_unique_id: media.file_unique_id,
    channelId: msg.chat.id,
    messageId: msg.message_id,
    fileSizeBytes: media.file_size || null,
  });
  invalidateTitles(state.category);
  state.awaiting = null;
  const label = state.category === CATEGORIES.MOVIES ? 'Movie' : 'Episode';
  await safeSendMessage(chatId, `✅ ${label} updated successfully.`);
}

// ============================================================================
// COMPRESS & REPLACE EPISODE
// ----------------------------------------------------------------------
// Reuses the existing download/verify/upload primitives (services/
// telegramUpload.js, services/compress.js) exactly as the main pipeline
// does — this is not a second pipeline, just a new caller of the same
// building blocks, for a single admin-triggered episode instead of a
// batch. queue/pipeline.js itself is never imported here and is not
// touched by this feature.
//
// Safety ordering (see runCompressEpisodeJob): download -> analyze ->
// compress -> upload -> verify -> generate thumbnail -> update Firestore
// -> ONLY THEN delete the old Telegram video. Any failure before the
// Firestore update leaves the existing episode completely untouched.
// ============================================================================

const pendingQualityChoice = new Map(); // docId -> resolver function, while askQualityChoice() is awaiting a button press

function progressBlocks(pct) {
  const filled = Math.max(0, Math.min(5, Math.round((pct || 0) / 20)));
  return '⬛'.repeat(filled) + '⬜'.repeat(5 - filled);
}

const COMPRESS_PHASE_ORDER = ['download', 'compress', 'upload', 'replacing', 'deleting', 'done'];

async function renderCompressProgress(chatId, messageId, label, state) {
  const idx = COMPRESS_PHASE_ORDER.indexOf(state.phase);
  const lines = [`Compressing <b>${escapeHtml(label)}</b>`, ''];
  const phaseInfo = [['download', '⬇ Downloading'], ['compress', '🗜 Compressing'], ['upload', '⬆ Uploading']];
  for (const [phase, phaseLabel] of phaseInfo) {
    if (COMPRESS_PHASE_ORDER.indexOf(phase) > idx) continue;
    if (phase === state.phase && state.started === false) {
      // Requirement: never show "0%" before FFmpeg has actually started
      // processing frames — this is the in-between state where the
      // phase has begun (download kicked off / FFmpeg spawned) but no
      // real progress has been reported back yet.
      lines.push(phaseLabel, 'Starting…', '');
      continue;
    }
    const pct = phase === state.phase ? state.pct : 100;
    lines.push(phaseLabel, `${pct}%`, progressBlocks(pct), '');
  }
  if (idx >= COMPRESS_PHASE_ORDER.indexOf('replacing')) lines.push('Replacing metadata...');
  if (idx >= COMPRESS_PHASE_ORDER.indexOf('deleting')) lines.push('Deleting old Telegram video...');
  if (state.phase === 'done') lines.push('', '✅ Completed.');
  const text = lines.join('\n').replace(/\n{3,}/g, '\n\n').trim();
  await safeEditMessageText(text, { chat_id: chatId, message_id: messageId }).catch(() => {});
}

function buildCompressedFileName(title, season, episode) {
  const base = season != null ? `${title} S${season}E${episode}` : title;
  return `${base}.mp4`.replace(/[\\/:*?"<>|]/g, '_').slice(0, 180);
}

function cleanupCompressFile(p) {
  if (!p) return;
  fs.unlink(p, (err) => { if (err && err.code !== 'ENOENT') log.warn('cleanupCompressFile', 'Failed to remove temp file', { path: p, reason: err.message }); });
}

/**
 * Pauses a compress job on an already-compatible source and asks the
 * admin whether to keep it as-is or force a recompress. Resolves via the
 * bx:cxo:/bx:cxf: callback branch in handleCallback(), or to 'force'
 * after 5 minutes of no response (a safe default — it always produces a
 * compliant file either way, this only decides whether libx264 runs).
 */
function askQualityChoice(chatId, messageId, docId, label) {
  return new Promise((resolve) => {
    let settled = false;
    const timeout = setTimeout(() => { if (settled) return; settled = true; pendingQualityChoice.delete(docId); resolve('force'); }, 5 * 60_000);
    pendingQualityChoice.set(docId, (choice) => {
      if (settled) return;
      settled = true;
      clearTimeout(timeout);
      pendingQualityChoice.delete(docId);
      resolve(choice);
    });
    safeEditMessageText(
      `ℹ️ <b>${escapeHtml(label)}</b> is already stream-compatible (H.264/AAC).\n\nCompress it anyway, or keep the current quality?`,
      { chat_id: chatId, message_id: messageId, reply_markup: { inline_keyboard: [[
        { text: '✅ Use Original', callback_data: `bx:cxo:${docId}` },
        { text: '🗜 Force Recompress', callback_data: `bx:cxf:${docId}` },
      ]] } }
    ).catch(() => { const r = pendingQualityChoice.get(docId); if (r) r('force'); });
  });
}

async function confirmCompress(chatId, messageId, state) {
  const t = state.titles[state.titleIdx];
  const doc = state.category === CATEGORIES.MOVIES
    ? t?.docs[0]
    : t?.docs.find((d) => d.season === state.season && d.episode === state.episode);
  if (!doc) {
    await safeEditMessageText('❌ Original source video not found.', { chat_id: chatId, message_id: messageId });
    return;
  }
  const payload = {
    docId: doc.id, category: state.category, title: t.title,
    season: state.category === CATEGORIES.MOVIES ? null : state.season,
    episode: state.category === CATEGORIES.MOVIES ? null : state.episode,
  };
  const tok = shortToken();
  pendingActions.set(tok, { chatId, kind: 'compressEpisode', payload, createdAt: Date.now() });
  await safeEditMessageText(
    'Compress this episode?\n\n⚠ This will replace the current Telegram video.',
    { chat_id: chatId, message_id: messageId, reply_markup: { inline_keyboard: [[
      { text: '✅ Compress', callback_data: `bx:cy:${tok}` },
      { text: '❌ Cancel', callback_data: `bx:cn:${tok}` },
    ]] } }
  );
}

/**
 * The actual Compress & Replace flow. Reused as-is on bot startup for any
 * job a Render restart interrupted (see resumePendingCompressJobs) — this
 * always restarts from step 1 (download) rather than trying to resume a
 * partial download/encode, since a temp file on disk doesn't survive a
 * process restart anyway.
 */
async function runCompressEpisodeJob({ chatId, messageId, docId, category, title, season, episode }) {
  const label = season != null ? `${title} S${season} E${episode}` : title;

  await uploadRecovery.upsertCompressJob(docId, { chatId, messageId, title, season, episode, category, status: 'downloading' });

  const doc = await getDoc(VIDEOS_COLLECTION, docId);
  if (!doc) {
    await safeEditMessageText('❌ Original source video not found.', { chat_id: chatId, message_id: messageId }).catch(() => {});
    await uploadRecovery.clearCompressJob(docId);
    return;
  }

  const progress = { phase: 'download', pct: 0, started: true };
  let lastRenderKey = '';
  const render = async (force) => {
    const key = `${progress.phase}:${progress.pct}:${progress.started}`;
    if (!force && key === lastRenderKey) return;
    lastRenderKey = key;
    await renderCompressProgress(chatId, messageId, label, progress);
  };
  await render(true);

  const stamp = `compress_${docId}_${Date.now()}`;
  const srcPath = path.join(COMPRESS_TMP_DIR, `${stamp}.src`);
  let compressedPath = null;
  let thumbPath = null;

  try {
    // 1. Locate + download the ORIGINAL source video — the exact same
    // MTProto download primitive the main pipeline uses. downloadFromChannel
    // throws mtproto.SourceNotFoundError if the message is gone, which is
    // exactly the "original source video not found" case.
    await transfer.downloadFromChannel(doc.channelId, doc.messageId, srcPath, {
      onProgress: (written, total) => {
        if (!total) return;
        progress.phase = 'download';
        progress.pct = Math.max(0, Math.min(100, Math.floor(((written / total) * 100) / 10) * 10));
        render();
      },
    });

    // 2. Analyze codec.
    await uploadRecovery.upsertCompressJob(docId, { status: 'compressing' });
    const info = await compress.analyze(srcPath);

    // Already stream-compatible? Let the admin choose, per requirements.
    let useOriginal = false;
    if (info.videoOk && info.audioOk) {
      const choice = await askQualityChoice(chatId, messageId, docId, label);
      useOriginal = choice === 'original';
      progress.phase = 'compress'; progress.pct = 0; progress.started = false;
    }

    // 3. Compress using FFmpeg (existing dormant compression pipeline —
    // H.264/High/yuv420p/AAC/faststart/CRF, hardware-accel with automatic
    // CPU fallback — reused unmodified from services/compress.js).
    progress.phase = 'compress'; progress.pct = 0; progress.started = false; await render(true);
    compressedPath = path.join(COMPRESS_TMP_DIR, `${stamp}.mp4`);

    let result;
    if (useOriginal) {
      // "Use Original" still guarantees a streaming-compatible faststart
      // MP4 (the one thing this feature must never skip) via the exact
      // same lossless remux the main upload pipeline uses — never a full
      // re-encode, so quality is genuinely untouched.
      const faststartCheck = compress.checkFaststart(srcPath);
      if (faststartCheck.ok) fs.copyFileSync(srcPath, compressedPath);
      else await compress.remuxToFaststart(srcPath, compressedPath);
      const verified = await compress.verifyOutputFile(compressedPath);
      if (!verified.valid) throw new Error(`Original file failed verification: ${verified.reason}`);
      result = { ...info, ...verified, mode: 'original+faststart' };
      progress.pct = 100; progress.started = true; await render(true);
    } else {
      result = await compress.processFile(srcPath, compressedPath, ({ percent, frame }) => {
        progress.phase = 'compress';
        // FIX (requirement 5): only start showing a percentage once
        // FFmpeg has actually reported real progress (a parsed stats
        // line with a frame count, or — once duration is known — a real
        // percent). Before that first tick, renderCompressProgress()
        // shows "Starting..." instead of a misleading "0%" that used to
        // display the instant this phase began, before FFmpeg had even
        // started processing a single frame.
        if (!progress.started && (frame > 0 || percent != null)) progress.started = true;
        // A still-null percent (source duration unknown even after the
        // analyze() fallback chain) no longer gets coerced to 0 — once
        // started, it holds the last known percent rather than falsely
        // reporting 0% while genuinely mid-encode.
        if (percent != null) {
          progress.pct = Math.max(0, Math.min(100, Math.floor(percent / 10) * 10));
        }
        render();
      });
    }

    // 4/5. Streaming-compatible MP4 confirmed by verifyOutputFile() above
    // (called internally by processFile(), or explicitly for the
    // "Use Original" path) — upload through the EXISTING MTProto upload.
    await uploadRecovery.upsertCompressJob(docId, { status: 'uploading' });
    progress.phase = 'upload'; progress.pct = 0; progress.started = true; await render(true);

    const uploadResult = await transfer.uploadEpisode(doc.channelId, compressedPath, {
      fileName: buildCompressedFileName(title, season, episode),
      duration: result.duration || doc.duration || 0,
      width: result.width || 0,
      height: result.height || 0,
      mimeType: result.mimeType || 'video/mp4',
      videoCodec: result.videoCodec, audioCodec: result.audioCodec, container: result.container,
      onProgress: (percent) => {
        progress.phase = 'upload';
        progress.pct = Math.max(0, Math.min(100, Math.floor((percent || 0) / 10) * 10));
        render();
      },
    });
    // 6. uploadEpisode() only returns after Telegram confirms a real
    // DocumentAttributeVideo came back — that IS the upload verification.

    // 7. Generate a new episode thumbnail from the NEW file — reuses the
    // exact thumbnail-generation/upload primitives the main pipeline
    // uses; thumbnail GENERATION logic itself is untouched.
    let episodeThumbnailFileId = doc.episodeThumbnailFileId || null;
    try {
      thumbPath = path.join(COMPRESS_TMP_DIR, `${stamp}.jpg`);
      await compress.generateEpisodeThumbnail(compressedPath, thumbPath, result.duration || 0);
      episodeThumbnailFileId = await transfer.uploadEpisodeThumbnailPhoto(bot, uploadResult.channelId, thumbPath);
    } catch (thumbErr) {
      log.warn('runCompressEpisodeJob', 'New thumbnail generation failed — keeping the existing thumbnail', { docId, reason: thumbErr.message });
    }

    // 8. SAFETY: Firestore is only ever updated here, after the new
    // upload is fully verified — never before.
    await uploadRecovery.upsertCompressJob(docId, { status: 'replacing' });
    progress.phase = 'replacing'; await render(true);

    await updateDoc(VIDEOS_COLLECTION, docId, {
      channelId: Number(uploadResult.channelId),
      messageId: uploadResult.messageId,
      telegram_file_id: uploadResult.documentId,
      file_unique_id: uploadResult.documentId,
      fileSizeBytes: uploadResult.size,
      mimeType: uploadResult.mimeType,
      duration: result.duration || doc.duration || 0,
      episodeThumbnailFileId,
      compressedAt: new Date().toISOString(),
    });
    invalidateTitles(category);

    // 9. SAFETY: the OLD Telegram video is only deleted now — strictly
    // after the new upload succeeded AND Firestore was updated above.
    // Never delete first.
    const oldChannelId = doc.channelId;
    const oldMessageId = doc.messageId;
    progress.phase = 'deleting'; await render(true);
    await transfer.deleteChannelMessages(oldChannelId, [oldMessageId]);

    progress.phase = 'done'; await render(true);
    await uploadRecovery.clearCompressJob(docId);
    log.success('runCompressEpisodeJob', 'Compress & Replace completed', { docId, title, season, episode });
  } catch (err) {
    // FAILURE: do not delete the old video, do not touch Firestore —
    // both of those only ever happen after this point in the try block,
    // so any error above them leaves the existing episode exactly as it
    // was.
    log.error('runCompressEpisodeJob', 'Compress & Replace failed — existing episode left untouched', err, { docId, stack: err.stack });
    const reason = err instanceof mtproto.SourceNotFoundError
      ? '❌ Original source video not found.'
      : `❌ Compression failed — the existing episode was <b>not</b> modified.\n\nReason: ${escapeHtml((err.message || 'Unknown error').slice(0, 300))}`;
    await safeEditMessageText(reason, { chat_id: chatId, message_id: messageId }).catch(() => {});
    await uploadRecovery.upsertCompressJob(docId, { status: 'failed', error: (err.message || '').slice(0, 500) });
  } finally {
    cleanupCompressFile(srcPath);
    cleanupCompressFile(compressedPath);
    cleanupCompressFile(thumbPath);
  }
}

/** Checked once at startup — resumes any compress job a Render restart interrupted. */
async function resumePendingCompressJobs() {
  try {
    const jobs = await uploadRecovery.getPendingCompressJobs();
    for (const job of jobs) {
      log.info('resumePendingCompressJobs', 'Resuming an interrupted compress job after restart', { docId: job.id, title: job.title, episode: job.episode });
      runCompressEpisodeJob({
        chatId: job.chatId, messageId: job.messageId, docId: job.id,
        category: job.category, title: job.title, season: job.season, episode: job.episode,
      }).catch((err) => log.error('resumePendingCompressJobs', 'Resumed compress job failed', err, { docId: job.id, stack: err.stack }));
    }
  } catch (err) {
    log.warn('resumePendingCompressJobs', 'Failed to check for pending compress jobs', { reason: err.message });
  }
}

module.exports = { registerBrowseAdmin, isAwaitingMedia };
