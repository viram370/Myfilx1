/**
 * handlers/adminUpload.js
 * ----------------------------------------------------------------------
 * New, alternative upload flow that lives alongside the existing
 * /saveanime /savemovie /savewebseries buffer commands (those are
 * untouched). This flow is for episodes/movies that need FFmpeg
 * re-encoding + a re-upload into the storage channel:
 *
 *   /add anime | webseries | anime-movie | movie
 *     -> bot asks Title (+ Season, for anime/webseries) + Language + Quality
 *        (+ Year, for movie/anime-movie)
 *     -> "upload mode": admin forwards videos, each is immediately
 *        acknowledged and queued for background compression + upload
 *        (queue/pipeline.js), auto-numbering episodes from Firestore
 *     -> admin sends "Done" to close upload mode; the pipeline keeps
 *        draining whatever is still compressing/uploading
 *     -> ONE message is edited throughout with live per-episode status
 *
 * This module intentionally does not touch bot.js's internals — it only
 * needs `bot`, `isAdmin`, and safe send/edit helpers, which are passed in
 * from services/bot.js. It also exports isSessionActive() so bot.js's
 * existing legacy buffer handler can skip a chat while this flow owns it.
 * ----------------------------------------------------------------------
 */
'use strict';

const { makeLogger } = require('../utils/logger');
const log = makeLogger('handlers/adminUpload.js');
const pipeline = require('../queue/pipeline');
const mtproto = require('../services/mtproto');

const KIND_ALIASES = {
  anime: 'anime',
  webseries: 'webseries',
  'web-series': 'webseries',
  'web_series': 'webseries',
  series: 'webseries',
  'anime-movie': 'anime-movie',
  'anime_movie': 'anime-movie',
  animemovie: 'anime-movie',
  movie: 'movie',
};
const KIND_CATEGORY = {
  anime: 'Anime',
  webseries: 'Web Series',
  'anime-movie': 'Movies',
  movie: 'Movies',
};
const HAS_SEASON = { anime: true, webseries: true, 'anime-movie': false, movie: false };
const KIND_LABEL = { anime: 'Anime', webseries: 'Web Series', 'anime-movie': 'Anime Movie', movie: 'Movie' };

const PROGRESS_DEBOUNCE_MS = 1200;

/** @type {Map<number, object>} wizard state, one per admin chat */
const wizards = new Map();

function isSessionActive(chatId) {
  return wizards.has(chatId) || pipeline.isSessionActive(chatId);
}

function stepsFor(kind) {
  return HAS_SEASON[kind]
    ? ['title', 'season', 'language', 'quality']
    : ['title', 'language', 'quality', 'year'];
}

function promptFor(step) {
  switch (step) {
    case 'title': return '📝 Send the title.';
    case 'season': return '🔢 Send the season number (e.g. 1).';
    case 'language': return '🌐 Send the language (e.g. Hindi, or "Hindi, English").';
    case 'quality': return '🎞 Send the quality (e.g. 720p, 1080p) — or send "skip" to leave it blank.';
    case 'year': return '📅 Send the release year (e.g. 2024) — or send "skip" to leave it blank.';
    default: return '';
  }
}

function validateField(step, raw) {
  const value = String(raw || '').trim();
  switch (step) {
    case 'title':
      if (!value || value.length > 200) throw new Error('Title must be 1-200 characters.');
      return value;
    case 'season': {
      const n = parseInt(value, 10);
      if (!Number.isInteger(n) || n < 1 || n > 999) throw new Error('Season must be a whole number between 1 and 999.');
      return n;
    }
    case 'language':
      if (!value || value.length > 60) throw new Error('Language must be 1-60 characters.');
      return value;
    case 'quality':
      if (/^skip$/i.test(value)) return null;
      if (value.length > 20) throw new Error('Quality must be 20 characters or fewer.');
      return value;
    case 'year': {
      if (/^skip$/i.test(value)) return null;
      const n = parseInt(value, 10);
      if (!Number.isInteger(n) || n < 1900 || n > 2100) throw new Error('Year must be a 4-digit year, or "skip".');
      return n;
    }
    default:
      return value;
  }
}

// ============================================================================
// PUBLIC ENTRY POINT
// ============================================================================

// Set once at registerAdminUpload() startup — stable for the process's
// lifetime, so pipeline.js's onProgress/onFinished callbacks (which only
// receive `session`, not these helpers) can reach them without a race.
let sharedSafeSendMessage = null;
let sharedSafeEditMessageText = null;

function registerAdminUpload(bot, { isAdmin, safeSendMessage, safeEditMessageText }) {
  sharedSafeSendMessage = safeSendMessage;
  sharedSafeEditMessageText = safeEditMessageText;
  pipeline.setBotInstance(bot);

  const kindPattern = Object.keys(KIND_ALIASES).sort((a, b) => b.length - a.length).join('|');
  const addRegex = new RegExp(`^\\/add(?:@\\w+)?\\s+(${kindPattern})\\s*$`, 'i');

  bot.onText(addRegex, async (msg, match) => {
    const chatId = msg.chat.id;
    if (!isAdmin(chatId)) return;

    const kind = KIND_ALIASES[match[1].toLowerCase()];

    if (wizards.has(chatId) || pipeline.isSessionActive(chatId)) {
      await safeSendMessage(chatId, '⚠️ You already have an /add session in progress. Send /canceladd to abandon it first.');
      return;
    }
    if (!mtproto.isEnabled()) {
      await safeSendMessage(chatId, '❌ MTProto is not configured (TELEGRAM_API_ID / TELEGRAM_API_HASH / TELEGRAM_STRING_SESSION), so I cannot download or re-upload large files. Set those up first.');
      return;
    }
    if (!process.env.STORAGE_CHANNEL_ID) {
      await safeSendMessage(chatId, '❌ STORAGE_CHANNEL_ID is not set — I need to know which channel to upload converted episodes into.');
      return;
    }

    const steps = stepsFor(kind);
    wizards.set(chatId, { kind, steps, stepIndex: 0, fields: {} });

    await safeSendMessage(chatId, `🎬 <b>Add ${KIND_LABEL[kind]}</b>\n\n${promptFor(steps[0])}`, { parse_mode: 'HTML' });
  });

  bot.onText(/^\/canceladd(?:@\w+)?\s*$/i, async (msg) => {
    const chatId = msg.chat.id;
    if (!isAdmin(chatId)) return;
    const hadWizard = wizards.delete(chatId);
    const session = pipeline.getSession(chatId);
    if (session && !session.closed) {
      session.finalizing = true; // let in-flight items finish; stop describing it as "active" to new videos
      wizards.delete(chatId);
    }
    await safeSendMessage(chatId, hadWizard || session ? '❎ /add session cancelled. Already-queued episodes will still finish processing.' : 'ℹ️ No /add session in progress.');
  });

  // ---- text steps of the wizard --------------------------------------
  bot.on('message', async (msg) => {
    const chatId = msg.chat.id;
    if (!isAdmin(chatId)) return;
    if (!msg.text) return;
    if (msg.text.startsWith('/')) return; // commands handled by their own listeners

    const wizard = wizards.get(chatId);
    if (!wizard) return;

    if (wizard.stepIndex < wizard.steps.length) {
      await handleWizardTextStep(chatId, wizard, msg.text, safeSendMessage);
      return;
    }

    // Upload mode: waiting for "Done".
    if (/^done$/i.test(msg.text.trim())) {
      await finishUploadMode(chatId, safeSendMessage);
    }
  });

  // ---- video capture during upload mode -------------------------------
  bot.on('video', (msg) => captureEpisode(chatIdOf(msg), msg, msg.video).catch((err) => log.error('video handler', 'capture failed', err)));
  bot.on('document', (msg) => {
    const media = extractVideoMedia(msg);
    if (media) captureEpisode(chatIdOf(msg), msg, media).catch((err) => log.error('document handler', 'capture failed', err));
  });
}

function chatIdOf(msg) { return msg.chat.id; }

function extractVideoMedia(msg) {
  if (msg.video) return msg.video;
  if (msg.document?.mime_type?.startsWith('video/')) return msg.document;
  return null;
}

// ============================================================================
// WIZARD TEXT STEPS
// ============================================================================

async function handleWizardTextStep(id, wizard, text, safeSendMessage) {
  const step = wizard.steps[wizard.stepIndex];
  let value;
  try {
    value = validateField(step, text);
  } catch (err) {
    await safeSendMessage(id, `⚠️ ${err.message}`);
    return;
  }

  wizard.fields[step] = value;
  wizard.stepIndex += 1;

  if (wizard.stepIndex < wizard.steps.length) {
    await safeSendMessage(id, promptFor(wizard.steps[wizard.stepIndex]));
    return;
  }

  // All fields collected — enter upload mode.
  const kind = wizard.kind;
  const session = pipeline.createSession(id, {
    kind,
    category: KIND_CATEGORY[kind],
    hasSeason: HAS_SEASON[kind],
    title: wizard.fields.title,
    season: wizard.fields.season || null,
    language: wizard.fields.language,
    quality: wizard.fields.quality || null,
    year: wizard.fields.year || null,
    storageChannelId: process.env.STORAGE_CHANNEL_ID,
  }, {
    onProgress: makeProgressRenderer(id),
    onFinished: makeFinishedHandler(id),
  });
  session.progressMessageId = null;
  session.lastRenderAt = 0;

  await safeSendMessage(
    id,
    `✅ Got it. Now send the video files (one at a time or in a row).\n\nSend <b>Done</b> when you've sent them all.`,
    { parse_mode: 'HTML' }
  );
}

// ============================================================================
// UPLOAD MODE — CAPTURING EPISODES
// ============================================================================

async function captureEpisode(id, msg, media) {
  const wizard = wizards.get(id);
  const session = pipeline.getSession(id);
  if (!wizard || !session || session.finalizing || session.closed) return; // not in an /add upload-mode session
  if (wizard.stepIndex < wizard.steps.length) return; // still collecting title/season/etc.

  pipeline.addItem(session, media, msg);

  // First video creates the progress message; every subsequent update
  // (including later videos) edits that SAME message — never spam chat.
  await renderProgressBySession(session, id, { force: true });
}

async function finishUploadMode(id, safeSendMessage) {
  const session = pipeline.getSession(id);
  if (!session) {
    wizards.delete(id);
    await safeSendMessage(id, 'ℹ️ No episodes were sent — /add session closed.');
    return;
  }
  if (session.items.length === 0) {
    pipeline.endSession(id);
    wizards.delete(id);
    await safeSendMessage(id, 'ℹ️ No episodes were sent — /add session closed.');
    return;
  }
  pipeline.finalizeSession(id);
  await renderProgressBySession(session, id, { force: true });
}

// ============================================================================
// PROGRESS RENDERING (single message, debounced edits)
// ============================================================================

function makeProgressRenderer(id) {
  return async (session) => renderProgressBySession(session, id);
}

async function renderProgressBySession(session, id, { force = false } = {}) {
  const now = Date.now();
  if (!force && now - (session.lastRenderAt || 0) < PROGRESS_DEBOUNCE_MS) return;
  session.lastRenderAt = now;

  const text = pipeline.renderSessionText(session);

  try {
    if (session.progressMessageId) {
      await sharedSafeEditMessageText(text, { chat_id: id, message_id: session.progressMessageId, parse_mode: 'HTML' });
    } else {
      const sent = await sharedSafeSendMessage(id, text, { parse_mode: 'HTML' });
      session.progressMessageId = sent?.message_id || null;
    }
  } catch (err) {
    log.error('renderProgressBySession', 'Failed to render progress message', err, { chatId: id });
  }
}

// ============================================================================
// SESSION COMPLETION
// ============================================================================

function makeFinishedHandler(id) {
  return async (session) => {
    wizards.delete(id);
    const done = session.items.filter((it) => it.status === 'done').length;
    const failed = session.items.filter((it) => it.status === 'failed').length;

    const summary = [
      failed > 0 && done > 0 ? '⚠️ <b>Batch complete (with failures)</b>' : (failed > 0 ? '❌ <b>Batch failed</b>' : '✅ <b>Batch complete</b>'),
      '',
      `Title: ${session.title}`,
      session.hasSeason ? `Season: ${session.season}` : null,
      `Uploaded: ${done}`,
      failed ? `Failed: ${failed} (see details above — retry those specific episodes with a new /add)` : null,
    ].filter(Boolean).join('\n');

    try {
      if (session.progressMessageId && sharedSafeEditMessageText) {
        await sharedSafeEditMessageText(`${pipeline.renderSessionText(session)}\n\n${summary}`, {
          chat_id: id, message_id: session.progressMessageId, parse_mode: 'HTML',
        });
      } else if (sharedSafeSendMessage) {
        await sharedSafeSendMessage(id, summary, { parse_mode: 'HTML' });
      }
    } catch (err) {
      log.error('makeFinishedHandler', 'Failed to render final summary', err, { chatId: id });
    }
  };
}

module.exports = { registerAdminUpload, isSessionActive };
