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
const { queryDocs, updateDoc, deleteDoc, batchDelete } = require('../services/firebase');
const { makeLogger } = require('../utils/logger');
const log = makeLogger('handlers/browseAdmin.js');

const VIDEOS_COLLECTION = 'videos';
const CATEGORIES = Object.freeze({ ANIME: 'Anime', MOVIES: 'Movies', WEBSERIES: 'Web Series' });
const CATEGORY_CODE = { [CATEGORIES.ANIME]: 'A', [CATEGORIES.MOVIES]: 'M', [CATEGORIES.WEBSERIES]: 'W' };
const CODE_CATEGORY = { A: CATEGORIES.ANIME, M: CATEGORIES.MOVIES, W: CATEGORIES.WEBSERIES };

const TITLE_CACHE_TTL_MS = 20_000;
const PENDING_TTL_MS = 2 * 60_000;

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

let bot, isAdmin, safeSendMessage, safeEditMessageText, isAddSessionActive;

function registerBrowseAdmin(botInstance, deps) {
  bot = botInstance;
  isAdmin = deps.isAdmin;
  safeSendMessage = deps.safeSendMessage;
  safeEditMessageText = deps.safeEditMessageText;
  isAddSessionActive = deps.isAddSessionActive || (() => false);

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

module.exports = { registerBrowseAdmin, isAwaitingMedia };
