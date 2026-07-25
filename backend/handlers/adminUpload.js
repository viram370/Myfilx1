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
 *            (from Telegram's forward metadata), used later for a real
 *            MTProto download (never copyMessage — see queue/pipeline.js)
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
const recovery = require('../services/uploadRecovery');

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
let sharedBot = null;
let isAdminRef = null;

function registerAdminUpload(bot, { isAdmin, safeSendMessage, safeEditMessageText }) {
  sharedSafeSendMessage = safeSendMessage;
  sharedSafeEditMessageText = safeEditMessageText;
  sharedBot = bot;
  isAdminRef = isAdmin;
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

  // ---- /continue — admin-only manual recovery trigger (Requirement 4) --
  bot.onText(/^\/continue(?:@\w+)?\s*$/i, async (msg) => {
    const chatId = msg.chat.id;
    if (!isAdmin(chatId)) return;
    try {
      const sessions = await recovery.getPendingSessions();
      const jobs = await recovery.getPendingJobs();

      if (sessions.length === 0 && jobs.length === 0) {
        const allJobsCount = await recovery.getAllJobsCount();
        if (allJobsCount > 0) {
          log.warn('/continue', `Firestore contains ${allJobsCount} jobs total, but 0 pending. If this is wrong, the status/completed filters failed to match any documents.`);
        }
        await safeSendMessage(chatId, 'No pending uploads.');
        return;
      }

      let logMsg = `Recovery Sessions Found: ${sessions.length}\n`;
      logMsg += `Recovery Jobs Found: ${jobs.length}\n`;
      logMsg += `Pending Jobs Found: ${jobs.length}\n`;

      if (jobs.length > 0) {
        logMsg += `\nPending Statuses:\n`;
        jobs.forEach(j => {
          logMsg += `Episode ${j.episode || 'Unknown'} -> ${j.status}\n`;
        });
      }

      logMsg += `\nQueue rebuilt.\nWorker restarted.\nRecovery started.`;

      await safeSendMessage(chatId, logMsg);
      await resumeAllPendingSessions();
    } catch (err) {
      log.error('/continue', 'failed to resume pending uploads', err, { stack: err.stack });
      await safeSendMessage(chatId, `❌ Failed to resume uploads: ${err.message}`);
    }
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

    if (wizard.awaitingThumbnail) {
      await safeSendMessage(chatId, '❌ Please send a valid image.');
      return;
    }

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
  bot.on('video', (msg) => captureEpisode(chatIdOf(msg), msg, msg.video).catch((err) => notifyCaptureFailure(chatIdOf(msg), 'video handler', err)));
  bot.on('document', (msg) => {
    const media = extractVideoMedia(msg);
    if (media) captureEpisode(chatIdOf(msg), msg, media).catch((err) => notifyCaptureFailure(chatIdOf(msg), 'document handler', err));
  });

  bot.on('channel_post', (msg) => captureChannelEpisode(msg, 'channel_post').catch((err) => log.error('channel_post handler', 'capture failed', err, { stack: err.stack })));
  bot.on('edited_channel_post', (msg) => captureChannelEpisode(msg, 'edited_channel_post').catch((err) => log.error('edited_channel_post handler', 'capture failed', err, { stack: err.stack })));

  bot.on('photo', (msg) => handleThumbnailPhoto(msg).catch((err) => log.error('photo handler', 'thumbnail capture failed', err, { stack: err.stack })));
  bot.on('document', (msg) => handleThumbnailDocument(msg).catch((err) => log.error('document handler', 'thumbnail capture failed', err, { stack: err.stack })));
  bot.on('video', (msg) => handleThumbnailRejectVideo(msg).catch((err) => log.error('video handler', 'thumbnail rejection failed', err, { stack: err.stack })));

  bot.on('callback_query', (query) => handleThumbnailCallback(query).catch((err) => log.error('callback_query', 'thumbnail callback failed', err, { stack: err.stack })));
}

function isImageDocument(doc) {
  if (!doc) return false;
  const mt = (doc.mime_type || '').toLowerCase();
  if (mt.startsWith('image/')) return true;
  return /\.(jpe?g|png|webp)$/i.test(doc.file_name || '');
}

async function sendThumbnailConfirmation(chatId) {
  await sharedSafeSendMessage(chatId, '✅ Thumbnail received successfully.', {
    reply_markup: {
      inline_keyboard: [
        [{ text: '💾 Save Anime', callback_data: 'thumb:save' }, { text: '🔁 Replace Thumbnail', callback_data: 'thumb:replace' }],
        [{ text: '❌ Cancel', callback_data: 'thumb:cancel' }],
      ],
    },
  });
}

async function handleThumbnailPhoto(msg) {
  const chatId = msg.chat.id;
  if (!isAdminRef(chatId)) return;
  const wizard = wizards.get(chatId);
  if (!wizard || !wizard.awaitingThumbnail) return;
  const sizes = msg.photo || [];
  const best = sizes[sizes.length - 1];
  if (!best) { await sharedSafeSendMessage(chatId, '❌ Please send a valid image.'); return; }
  wizard.thumbnailFileId = best.file_id;
  await sendThumbnailConfirmation(chatId);
}

async function handleThumbnailDocument(msg) {
  const chatId = msg.chat.id;
  if (!isAdminRef(chatId)) return;
  const wizard = wizards.get(chatId);
  if (!wizard || !wizard.awaitingThumbnail) return;
  if (!isImageDocument(msg.document)) { await sharedSafeSendMessage(chatId, '❌ Please send a valid image.'); return; }
  wizard.thumbnailFileId = msg.document.file_id;
  await sendThumbnailConfirmation(chatId);
}

async function handleThumbnailRejectVideo(msg) {
  const chatId = msg.chat.id;
  if (!isAdminRef(chatId)) return;
  const wizard = wizards.get(chatId);
  if (!wizard || !wizard.awaitingThumbnail) return;
  await sharedSafeSendMessage(chatId, '❌ Please send a valid image.');
}

async function handleThumbnailCallback(query) {
  const chatId = query.message?.chat?.id;
  const data = query.data || '';
  if (!chatId || !data.startsWith('thumb:') || !isAdminRef(chatId)) return;

  const wizard = wizards.get(chatId);
  if (!wizard || !wizard.awaitingThumbnail) {
    try { await sharedBot.answerCallbackQuery(query.id, { text: '⌛ This step has expired.', show_alert: true }); } catch (_) {}
    return;
  }

  const action = data.slice('thumb:'.length);
  try { await sharedBot.answerCallbackQuery(query.id); } catch (_) {}

  if (action === 'replace') {
    wizard.thumbnailFileId = null;
    await sharedSafeSendMessage(chatId, '🖼 Please send the new anime thumbnail.');
    return;
  }
  if (action === 'cancel') {
    wizards.delete(chatId);
    const session = pipeline.getSession(chatId);
    if (session && !session.closed) pipeline.endSession(chatId);
    await sharedSafeSendMessage(chatId, '❎ /add session cancelled.');
    return;
  }
  if (action === 'save') {
    if (!wizard.thumbnailFileId) { await sharedSafeSendMessage(chatId, '⚠️ Send a thumbnail image first.'); return; }
    wizard.awaitingThumbnail = false;
    await enterBatchMode(chatId, wizard, sharedSafeSendMessage);
  }
}

async function notifyCaptureFailure(chatId, source, err) {
  log.error(source, 'capture failed', err, { stack: err.stack });
  if (!sharedSafeSendMessage) return;
  try {
    await sharedSafeSendMessage(chatId, `⚠️ Failed to buffer that video: ${err.message}`);
  } catch (_) { /* best effort — don't throw out of an error handler */ }
}

function chatIdOf(msg) { return msg.chat.id; }

function extractVideoMedia(msg) {
  if (msg.video) return msg.video;
  if (msg.document?.mime_type?.startsWith('video/')) return msg.document;
  return null;
}

function activeBatchModeChatIds() {
  const ids = [];
  for (const [chatId, wizard] of wizards.entries()) {
    if (wizard.stepIndex < wizard.steps.length) continue;
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

  wizard.awaitingThumbnail = true;
  await safeSendMessage(id, '🖼 Please send the anime thumbnail.');
}

async function enterBatchMode(id, wizard, safeSendMessage) {
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
    thumbnailFileId: wizard.thumbnailFileId || null,
    storageChannelId: process.env.STORAGE_CHANNEL_ID,
  }, {
    onProgress: makeProgressRenderer(id),
    onFinished: makeFinishedHandler(id),
  });
  session.progressMessageId = null;
  session.lastRenderAt = 0;

  recovery.upsertSessionMaster(id, session).catch(() => {});

  await safeSendMessage(
    id,
    `✅ Got it. Now send the video files (one at a time or in a row) — each will be buffered, nothing is ` +
    `processed yet.\n\nSend <b>Done</b> (or /done) when you've sent them all to start processing.`,
    { parse_mode: 'HTML' }
  );
}

function startPrefilledBatch(chatId, kind, fields) {
  if (isSessionActive(chatId)) return false;

  const steps = stepsFor(kind);
  wizards.set(chatId, {
    kind,
    steps,
    stepIndex: steps.length,
    fields: { ...fields },
    thumbnailFileId: fields.thumbnailFileId || null,
    awaitingThumbnail: false,
  });

  const session = pipeline.createSession(chatId, {
    kind,
    category: KIND_CATEGORY[kind],
    hasSeason: HAS_SEASON[kind],
    title: fields.title,
    season: fields.season || null,
    language: fields.language,
    quality: fields.quality || null,
    year: fields.year || null,
    thumbnailFileId: fields.thumbnailFileId || null,
    storageChannelId: process.env.STORAGE_CHANNEL_ID,
  }, {
    onProgress: makeProgressRenderer(chatId),
    onFinished: makeFinishedHandler(chatId),
  });
  session.progressMessageId = null;
  session.lastRenderAt = 0;

  recovery.upsertSessionMaster(chatId, session).catch(() => {});

  log.info('startPrefilledBatch', 'Pre-filled batch session started (Add Season / Add Episode)', {
    chatId, kind, title: fields.title, season: fields.season || null,
  });
  return true;
}

// ============================================================================
// BATCH MODE — BUFFERING EPISODES
// ============================================================================

async function captureEpisode(id, msg, media) {
  const wizard = wizards.get(id);
  const session = pipeline.getSession(id);
  if (!wizard || !session || session.locked || session.closed) return;
  if (wizard.stepIndex < wizard.steps.length) return;

  pipeline.addItem(session, media, msg);
  await renderProgressBySession(session, id, { force: true });
}

function isRecognizedChannel(channelId) {
  const configured = process.env.STORAGE_CHANNEL_ID;
  if (!configured) return true;
  return Number(channelId) === Number(configured);
}

async function captureChannelEpisode(msg, source) {
  if (!isRecognizedChannel(msg.chat.id)) return;

  const media = extractVideoMedia(msg);
  if (!media) return;

  const targets = activeBatchModeChatIds();
  if (targets.length === 0) return;

  for (const chatId of targets) {
    const session = pipeline.getSession(chatId);
    if (!session || session.locked || session.closed) continue;

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

  try {
    const result = await pipeline.startBatch(id);
    if (!result.started && result.reason === 'already-locked') {
      await safeSendMessage(id, 'ℹ️ This batch is already locked and processing.');
      return;
    }
    await renderProgressBySession(session, id, { force: true });
  } catch (err) {
    log.error('lockAndStartBatch', 'Unexpected failure starting batch', err, { chatId: id, stack: err.stack });
    await safeSendMessage(id, `❌ Something went wrong starting this batch: ${err.message}`);
  }
}

// ============================================================================
// PROGRESS RENDERING
// ============================================================================

function makeProgressRenderer(id) {
  return async (session, opts) => renderProgressBySession(session, id, opts);
}

async function renderProgressBySession(session, id, { force = false } = {}) {
  const now = Date.now();
  if (!force && now - (session.lastRenderAt || 0) < PROGRESS_DEBOUNCE_MS) return;
  session.lastRenderAt = now;

  const text = pipeline.renderSessionText(session);

  for (const item of session.items) {
    recovery.recordItem(id, item, { title: session.title, category: session.category, season: session.season }).catch(() => {});
  }

  try {
    if (session.progressMessageId) {
      await sharedSafeEditMessageText(text, { chat_id: id, message_id: session.progressMessageId, parse_mode: 'HTML' });
      return;
    }

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

    for (const item of session.items) {
      recovery.recordItem(id, item, { title: session.title, category: session.category, season: session.season }).catch(() => {});
    }
    recovery.markSessionCompleted(id).catch(() => {});

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

// ============================================================================
// RECOVERY — rebuild and resume unfinished sessions
// ============================================================================

async function resumeAllPendingSessions() {
  let jobs = [];
  let sessions = [];

  try {
    jobs = await recovery.getPendingJobs();
    sessions = await recovery.getPendingSessions();
  } catch (err) {
    log.error('resumeAllPendingSessions', 'failed to query pending jobs/sessions', err, { stack: err.stack });
    return { resumed: 0, sessions: 0 };
  }

  if (jobs.length === 0 && sessions.length === 0) return { resumed: 0, sessions: 0 };

  const grouped = recovery.groupJobsBySession(jobs);
  let resumedCount = 0;
  let sessionsResumed = 0;

  const sessionIdsToProcess = new Set([
    ...sessions.map(s => String(s.chatId)),
    ...Array.from(grouped.keys())
  ]);

  for (const sessionIdStr of sessionIdsToProcess) {
    const chatId = Number(sessionIdStr);
    if (!Number.isFinite(chatId)) continue;
    if (isSessionActive(chatId)) continue;

    const sessionJobs = grouped.get(sessionIdStr) || [];
    
    let master = sessions.find(s => String(s.chatId) === sessionIdStr);
    if (!master) {
      master = await recovery.getSessionMaster(chatId);
    }
    if (!master) {
      log.warn('resumeAllPendingSessions', 'Pending jobs found with no session context — cannot rebuild, skipping', { chatId, count: sessionJobs.length });
      continue;
    }

    const itemsToResume = [];

    for (const job of sessionJobs) {
      const alreadyThere = await recovery.isAlreadyInLibrary(job.fileUniqueId).catch(() => false);
      if (alreadyThere) {
        await recovery.recordItem(chatId, { fileUniqueId: job.fileUniqueId, status: 'done' }, { title: master.title, category: master.category, season: master.season }).catch(() => {});
        continue;
      }

      const { retryCount, exceeded } = await recovery.incrementRetryAndCheck(job).catch(() => ({ retryCount: job.retryCount || 0, exceeded: false }));
      if (exceeded) {
        log.warn('resumeAllPendingSessions', 'Job exceeded max retries — marked failed', { chatId, title: master.title, episode: job.episode, retryCount });
        continue;
      }
      itemsToResume.push(job);
    }

    const started = startPrefilledBatch(chatId, master.kind, {
      title: master.title,
      season: master.season,
      language: master.language,
      quality: master.quality,
      year: master.year,
      thumbnailFileId: master.thumbnailFileId,
    });

    if (!started) {
      log.warn('resumeAllPendingSessions', 'Could not start a rebuilt session — chat already busy', { chatId });
      continue;
    }

    const session = pipeline.getSession(chatId);
    for (const job of itemsToResume) {
      const media = {
        file_id: job.fileId,
        file_unique_id: job.fileUniqueId,
        file_size: job.fileSizeBytes,
        file_name: job.originalFileName,
        mime_type: job.originalMimeType
      };

      // Requirement fix: detectChannelOrigin() with opts.channelNative:true
      // reads ONLY msg.chat.id/msg.message_id — it does not look at
      // forward_origin at all in that branch (see queue/pipeline.js). So
      // for a channel-sourced job, msg.chat.id/message_id must themselves
      // BE the original storage-channel coordinates — never the admin
      // chat, and never wrapped inside a forward_origin object, which
      // would silently be ignored and let the wrong (admin-chat) values
      // through instead.
      const isForwarded = job.sourceType === 'forwarded' || (job.forwardChatId && job.forwardMessageId);

      let fakeMsg;
      const opts = {};

      if (isForwarded) {
        if (!job.forwardChatId || !job.forwardMessageId) {
          log.error('resumeAllPendingSessions', 'Forwarded job missing forwardChatId/forwardMessageId — cannot safely resume, skipping rather than guessing', null, { chatId, episode: job.episode, jobId: job.id });
          continue;
        }
        fakeMsg = { chat: { id: job.forwardChatId }, message_id: job.forwardMessageId };
        opts.channelNative = true;
      } else {
        fakeMsg = { message_id: job.chatMessageId || 0 };
      }

      try {
        pipeline.addItem(session, media, fakeMsg, opts);

        const addedItem = session.items[session.items.length - 1];
        addedItem.episode = job.episode;
        addedItem.status = 'waiting';

        // Defense in depth: if the coordinates addItem() actually recorded
        // ever drift from what Firestore had stored, that's this exact bug
        // class recurring — fail loudly instead of silently querying the
        // wrong channel.
        if (isForwarded && (addedItem.forwardChatId !== job.forwardChatId || addedItem.forwardMessageId !== job.forwardMessageId)) {
          log.error('resumeAllPendingSessions', 'Restored forwardChatId/forwardMessageId do not match Firestore — refusing to resume this item', null, {
            chatId, episode: job.episode, expected: { chatId: job.forwardChatId, messageId: job.forwardMessageId }, got: { chatId: addedItem.forwardChatId, messageId: addedItem.forwardMessageId },
          });
          addedItem.status = 'failed';
          addedItem.error = 'Recovery mismatch: restored source coordinates did not match the stored forward metadata.';
          continue;
        }

        log.info('resumeAllPendingSessions', 'Recovery loaded — restored original source coordinates', {
          chatId, episode: job.episode, sourceType: addedItem.sourceType,
          forwardChatId: addedItem.forwardChatId, forwardMessageId: addedItem.forwardMessageId,
          storedForwardChatId: job.forwardChatId, storedForwardMessageId: job.forwardMessageId,
        });
        log.info('resumeAllPendingSessions', recovery.formatRecoveryLog({
          title: master.title, episode: job.episode, previousStatus: job.status, newStatus: addedItem.status,
        }));
      } catch (err) {
        log.error('resumeAllPendingSessions', 'Failed to re-add a job to the rebuilt session', err, { chatId, episode: job.episode });
      }
    }

    if (itemsToResume.length > 0) {
      try {
        await pipeline.startBatch(chatId);
        await renderProgressBySession(session, chatId, { force: true });
        resumedCount += itemsToResume.length;
      } catch (err) {
        log.error('resumeAllPendingSessions', 'Failed to start the rebuilt batch', err, { chatId });
      }
    }
    sessionsResumed += 1;
  }

  return { resumed: resumedCount, sessions: sessionsResumed };
}

module.exports = { registerAdminUpload, isSessionActive, startPrefilledBatch, resumeAllPendingSessions };
