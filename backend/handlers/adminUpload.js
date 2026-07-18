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
 *     -> "batch mode": the admin sends episodes any of three ways, all
 *        auto-detected — but each is ONLY buffered (queue/pipeline.js's
 *        addItem does no download/compression/upload/worker-assignment):
 *          - direct upload to the bot -> Bot API only (file_id)
 *          - forwarded from a channel -> real channel id + message id
 *            (from Telegram's forward metadata), used later for
 *            copyMessage()/MTProto
 *          - posted straight into a private storage channel the bot
 *            admins (channel_post/edited_channel_post) -> same real
 *            channel id + message id, joining whichever /add batch(es)
 *            are currently waiting for episodes
 *        A Telegram "Copy" (as opposed to Forward) carries no metadata at
 *        all, so it's indistinguishable from a direct upload and correctly
 *        buffers as one.
 *     -> admin sends "Done" or "/done" to LOCK the batch — this is when
 *        validation, duplicate detection, episode numbering, and the
 *        actual download/compress/upload queues all start (see
 *        queue/pipeline.js#startBatch)
 *     -> ONE message is edited throughout: a simple "buffered" checklist
 *        before Done, live per-episode status after
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
      await safeSendMessage(chatId, '❌ MTProto is not configured (TELEGRAM_API_ID / TELEGRAM_API_HASH / TELEGRAM_STRING_SESSION), so I cannot process forwarded/channel videos or upload converted episodes. Set those up first.');
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
      pipeline.endSession(chatId);
      wizards.delete(chatId);
    }
    await safeSendMessage(chatId, hadWizard || session ? '❎ /add session cancelled.' : 'ℹ️ No /add session in progress.');
  });

  // ---- text steps of the wizard, plus "Done"/"/done" -------------------
  bot.on('message', async (msg) => {
    const chatId = msg.chat.id;
    if (!isAdmin(chatId)) return;
    if (!msg.text) return;

    const trimmed = msg.text.trim();
    const isDoneTrigger = /^\/?done(?:@\w+)?$/i.test(trimmed);

    if (msg.text.startsWith('/') && !isDoneTrigger) return; // other commands handled by their own listeners

    const wizard = wizards.get(chatId);
    if (!wizard) return;

    if (wizard.stepIndex < wizard.steps.length) {
      if (isDoneTrigger) return; // ignore Done while still collecting title/season/etc.
      await handleWizardTextStep(chatId, wizard, msg.text, safeSendMessage);
      return;
    }

    if (isDoneTrigger) {
      await lockAndStartBatch(chatId, safeSendMessage);
    }
  });

  // ---- video capture (buffer-only) during batch mode -------------------
  bot.on('video', (msg) => captureEpisode(chatIdOf(msg), msg, msg.video).catch((err) => log.error('video handler', 'capture failed', err)));
  bot.on('document', (msg) => {
    const media = extractVideoMedia(msg);
    if (media) captureEpisode(chatIdOf(msg), msg, media).catch((err) => log.error('document handler', 'capture failed', err));
  });

  // ---- videos posted straight into a private storage channel ----------
  // These never pass through 'video'/'document' (node-telegram-bot-api
  // only emits those for regular 'message' updates) and carry no forward
  // metadata of their own (there's nothing to forward from — the post
  // itself IS the source), so they need their own capture path that flags
  // channelNative so pipeline.js treats msg.chat/msg.message_id as the
  // origin instead of expecting forward_origin.
  bot.on('channel_post', (msg) => captureChannelEpisode(msg, 'channel_post').catch((err) => log.error('channel_post handler', 'capture failed', err)));
  bot.on('edited_channel_post', (msg) => captureChannelEpisode(msg, 'edited_channel_post').catch((err) => log.error('edited_channel_post handler', 'capture failed', err)));
}

function chatIdOf(msg) { return msg.chat.id; }

function extractVideoMedia(msg) {
  if (msg.video) return msg.video;
  if (msg.document?.mime_type?.startsWith('video/')) return msg.document;
  return null;
}

/**
 * Chat ids of admins whose /add wizard has finished collecting fields and
 * is actively buffering episodes — i.e. genuinely "in batch mode", not
 * mid-wizard (still typing title/season/etc.) and not already locked.
 * Used to decide which running /add batch(es) a channel-posted video (no
 * admin chat of its own) should join.
 */
function activeBatchModeChatIds() {
  const ids = [];
  for (const [chatId, wizard] of wizards.entries()) {
    if (wizard.stepIndex < wizard.steps.length) continue; // still collecting title/season/etc.
    const session = pipeline.getSession(chatId);
    if (!session || session.locked || session.closed) continue;
    ids.push(chatId);
  }
  return ids;
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

  // All fields collected — enter batch (buffering) mode.
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
    `✅ Got it. Now send the video files (one at a time or in a row) — each will be buffered, nothing is ` +
    `processed yet.\n\nSend <b>Done</b> (or /done) when you've sent them all to start processing.`,
    { parse_mode: 'HTML' }
  );
}

// ============================================================================
// BATCH MODE — BUFFERING EPISODES (no processing happens here)
// ============================================================================

async function captureEpisode(id, msg, media) {
  const wizard = wizards.get(id);
  const session = pipeline.getSession(id);
  if (!wizard || !session || session.locked || session.closed) return; // not in an /add batch-mode session
  if (wizard.stepIndex < wizard.steps.length) return; // still collecting title/season/etc.

  pipeline.addItem(session, media, msg);

  // First video creates the progress message; every subsequent update
  // (including later videos) edits that SAME message — never spam chat.
  await renderProgressBySession(session, id, { force: true });
}

/**
 * Mirrors services/bot.js's isStorageChannel(): if STORAGE_CHANNEL_ID is
 * set, only react to channel_post updates from that channel — if it's
 * unset, trust any channel_post, since Telegram only delivers those for
 * channels the bot actually administers.
 */
function isRecognizedChannel(channelId) {
  const configured = process.env.STORAGE_CHANNEL_ID;
  if (!configured) return true;
  return Number(channelId) === Number(configured);
}

/**
 * Videos posted straight into a private storage channel the bot admins
 * (channel_post/edited_channel_post) have no admin chat of their own, so
 * they're routed into every /add batch currently waiting for episodes
 * (mirrors the legacy buffer's multi-admin broadcast in services/bot.js).
 * Each gets flagged channelNative so pipeline.js treats the post itself —
 * not a forward — as the source, buffers it, and it gets picked up (via
 * copyMessage/MTProto against that same real channel) once the batch is
 * locked.
 */
async function captureChannelEpisode(msg, source) {
  if (!isRecognizedChannel(msg.chat.id)) return;

  const media = extractVideoMedia(msg);
  if (!media) return;

  const targets = activeBatchModeChatIds();
  if (targets.length === 0) return; // no /add batch waiting — the legacy /saveanime buffer handles it instead

  for (const chatId of targets) {
    const session = pipeline.getSession(chatId);
    if (!session || session.locked || session.closed) continue;

    // edited_channel_post can re-fire for the same post (e.g. a caption
    // edit) — don't double-buffer an already-captured video.
    if (session.items.some((it) => it.fileUniqueId === media.file_unique_id)) continue;

    pipeline.addItem(session, media, msg, { channelNative: true });
    log.info('captureChannelEpisode', 'Channel-posted video buffered into /add batch', {
      source, chatId, channelId: msg.chat.id, messageId: msg.message_id, fileUniqueId: media.file_unique_id,
    });

    await renderProgressBySession(session, chatId, { force: true });
  }
}

async function lockAndStartBatch(id, safeSendMessage) {
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

  const result = await pipeline.startBatch(id);
  if (!result.started && result.reason === 'already-locked') {
    await safeSendMessage(id, 'ℹ️ This batch is already locked and processing.');
    return;
  }

  await renderProgressBySession(session, id, { force: true });
}

// ============================================================================
// PROGRESS RENDERING (single message, debounced edits)
// ============================================================================

function makeProgressRenderer(id) {
  return async (session, opts) => renderProgressBySession(session, id, opts);
}

async function renderProgressBySession(session, id, { force = false } = {}) {
  const now = Date.now();
  if (!force && now - (session.lastRenderAt || 0) < PROGRESS_DEBOUNCE_MS) return;
  session.lastRenderAt = now;

  const text = pipeline.renderSessionText(session);

  try {
    if (session.progressMessageId) {
      await sharedSafeEditMessageText(text, { chat_id: id, message_id: session.progressMessageId, parse_mode: 'HTML' });
      return;
    }

    // Two videos captured close enough together (or a status change
    // firing while the very first message is still being sent) could
    // both reach this point before either has set session.progressMessageId
    // — without this guard each would send its own NEW message instead of
    // sharing one edited message for the whole batch. Whichever call gets
    // here first "owns" creating the message; everyone else waits for it
    // and then edits the message that was actually created.
    if (session.creatingProgressMessage) {
      await session.creatingProgressMessage;
      if (session.progressMessageId) {
        await sharedSafeEditMessageText(text, { chat_id: id, message_id: session.progressMessageId, parse_mode: 'HTML' });
      }
      return;
    }

    session.creatingProgressMessage = (async () => {
      const sent = await sharedSafeSendMessage(id, text, { parse_mode: 'HTML' });
      session.progressMessageId = sent?.message_id || null;
    })();
    try {
      await session.creatingProgressMessage;
    } finally {
      session.creatingProgressMessage = null;
    }
  } catch (err) {
    session.creatingProgressMessage = null;
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
    const skipped = session.items.filter((it) => it.status === 'skipped').length;

    const summary = [
      '<b>Batch complete</b>',
      '',
      `Title: ${session.title}`,
      session.hasSeason ? `Season: ${session.season}` : null,
      `✅ Uploaded: ${done}`,
      `❌ Failed: ${failed}`,
      skipped ? `⏭ Skipped (duplicates): ${skipped}` : null,
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
