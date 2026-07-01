/**
 * Telegram Bot Service
 * Handles video ingestion (file_id, file_unique_id, thumb, etc.)
 * and all admin commands: /add /edit /delete /publish /unpublish /stats /users
 */

const TelegramBot = require('node-telegram-bot-api');
const {
  addDoc, getDoc, setDoc, updateDoc, deleteDoc, queryDocs,
} = require('./firebase');

let bot;
const ADMIN_IDS = (process.env.ADMIN_USER_IDS || '')
  .split(',').map(s => parseInt(s.trim())).filter(Boolean);

// In-memory multi-step conversation state per admin user
const flow = {};

function initBot() {
  const token = process.env.TELEGRAM_BOT_TOKEN;
  if (!token) { console.error('❌ TELEGRAM_BOT_TOKEN not set'); return; }

  if (process.env.NODE_ENV === 'production') {
    bot = new TelegramBot(token);
    setupWebhook();
  } else {
    bot = new TelegramBot(token, { polling: true });
  }

  registerHandlers();
  console.log('✅ Telegram bot initialized');
  return bot;
}

async function setupWebhook() {
  try {
    const url = `${process.env.TELEGRAM_WEBHOOK_URL}/webhook`;
    await bot.setWebHook(url, { secret_token: process.env.WEBHOOK_SECRET });
    console.log(`✅ Webhook set: ${url}`);
  } catch (err) {
    console.error('❌ setWebhook failed:', err.message);
  }
}

function isAdmin(id) { return ADMIN_IDS.includes(id); }

/* ════════════════════════════════════════════════════════════════════
   MAIN HANDLERS
════════════════════════════════════════════════════════════════════ */
function registerHandlers() {

  // start
  bot.onText(/\/start/, async (msg) => {
    const chatId = msg.chat.id;
    await upsertUser(msg.from);

    const admin = isAdmin(msg.from.id);
    await bot.sendMessage(chatId,
      `🎬 *Welcome to MYFLIX!*\n\n` +
      `Your personal streaming library, right inside Telegram.\n\n` +
      `${admin ? '👑 Admin access enabled.\n\n' : ''}` +
      `Tap below to open the app.`,
      {
        parse_mode: 'Markdown',
        reply_markup: {
          inline_keyboard: [
            [{ text: '🎬 Open MYFLIX', web_app: { url: process.env.MINI_APP_URL } }],
            ...(admin ? [[{ text: '⚙️ Admin Help', callback_data: 'admin_help' }]] : []),
          ],
        },
      }
    );
  });

  // help
  bot.onText(/\/help/, async (msg) => {
    if (!isAdmin(msg.from.id)) {
      return bot.sendMessage(msg.chat.id, '📚 Open the Mini App to browse and watch!');
    }
    await sendAdminHelp(msg.chat.id);
  });

  // Video ingestion: direct video or forwarded video/document
  bot.on('video', async (msg) => {
    if (!isAdmin(msg.from.id)) return;
    await ingestVideo(msg, msg.video);
  });

  bot.on('document', async (msg) => {
    if (!isAdmin(msg.from.id)) return;
    const doc = msg.document;
    if (doc.mime_type && doc.mime_type.startsWith('video/')) {
      await ingestVideo(msg, doc);
    }
  });

  // Banner image upload (used mid-flow when bot asks for "Banner image")
  bot.on('photo', async (msg) => {
    if (!isAdmin(msg.from.id)) return;
    const state = flow[msg.from.id];
    if (state && state.step === 'await_banner') {
      const photo = msg.photo[msg.photo.length - 1];
      state.data.bannerFileId = photo.file_id;
      await advanceFlow(msg.from.id, msg.chat.id);
    }
  });

  // /add — manually start metadata flow by file_id
  bot.onText(/\/add$/, async (msg) => {
    if (!isAdmin(msg.from.id)) return;
    flow[msg.from.id] = { step: 'await_fileid_manual', data: {} };
    await bot.sendMessage(msg.chat.id,
      '📥 Send the Telegram *file_id* of the video (or just send/forward the video file directly).',
      { parse_mode: 'Markdown' });
  });

  // /edit <id>
  bot.onText(/\/edit (.+)/, async (msg, match) => {
    if (!isAdmin(msg.from.id)) return;
    const id = match[1].trim();
    const video = await getDoc('videos', id);
    if (!video) return bot.sendMessage(msg.chat.id, `❌ Not found: \`${id}\``, { parse_mode: 'Markdown' });

    flow[msg.from.id] = { step: 'editing', videoId: id, data: { ...video } };
    await bot.sendMessage(msg.chat.id,
      `✏️ *Editing: ${video.title}*\n\nSend updates as \`field: value\`\n\n` +
      `Fields: title, description, category, season, episode, poster, banner, thumbnail\n\n` +
      `Send /done when finished.`,
      { parse_mode: 'Markdown' });
  });

  // /delete <id>
  bot.onText(/\/delete (.+)/, async (msg, match) => {
    if (!isAdmin(msg.from.id)) return;
    const id = match[1].trim();
    const video = await getDoc('videos', id);
    if (!video) return bot.sendMessage(msg.chat.id, `❌ Not found: \`${id}\``, { parse_mode: 'Markdown' });

    await bot.sendMessage(msg.chat.id,
      `⚠️ Delete *${video.title}*?\nID: \`${id}\`\nThis cannot be undone.`,
      {
        parse_mode: 'Markdown',
        reply_markup: { inline_keyboard: [[
          { text: '✅ Confirm', callback_data: `del:${id}` },
          { text: '❌ Cancel', callback_data: 'cancel' },
        ]] },
      });
  });

  // /publish & /unpublish
  bot.onText(/\/publish (.+)/, async (msg, match) => {
    if (!isAdmin(msg.from.id)) return;
    const id = match[1].trim();
    try {
      await updateDoc('videos', id, { published: true, publishedAt: new Date().toISOString() });
      await bot.sendMessage(msg.chat.id, `✅ Published: \`${id}\``, { parse_mode: 'Markdown' });
    } catch (e) { await bot.sendMessage(msg.chat.id, `❌ ${e.message}`); }
  });

  bot.onText(/\/unpublish (.+)/, async (msg, match) => {
    if (!isAdmin(msg.from.id)) return;
    const id = match[1].trim();
    try {
      await updateDoc('videos', id, { published: false });
      await bot.sendMessage(msg.chat.id, `✅ Unpublished: \`${id}\``, { parse_mode: 'Markdown' });
    } catch (e) { await bot.sendMessage(msg.chat.id, `❌ ${e.message}`); }
  });

  // /stats
  bot.onText(/\/stats/, async (msg) => {
    if (!isAdmin(msg.from.id)) return;
    try {
      const [videos, users, history] = await Promise.all([
        queryDocs('videos'), queryDocs('users'), queryDocs('history', [], ['watchedAt', 'desc'], 200),
      ]);
      const published = videos.filter(v => v.published).length;
      const totalViews = videos.reduce((a, v) => a + (v.views || 0), 0);
      const totalLikes = videos.reduce((a, v) => a + (v.likes || 0), 0);
      const cats = [...new Set(videos.map(v => v.category))];
      const watchers = new Set(history.map(h => h.userId)).size;

      await bot.sendMessage(msg.chat.id,
        `📊 *MYFLIX Stats*\n\n` +
        `🎬 Videos: ${videos.length} (${published} published)\n` +
        `📂 Categories: ${cats.join(', ') || 'none'}\n` +
        `👁 Total views: ${totalViews}\n` +
        `❤️ Total likes: ${totalLikes}\n` +
        `👥 Users: ${users.length} (${watchers} active)\n` +
        `▶️ Watch events (last 200): ${history.length}`,
        { parse_mode: 'Markdown' });
    } catch (e) { await bot.sendMessage(msg.chat.id, `❌ ${e.message}`); }
  });

  // /users
  bot.onText(/\/users/, async (msg) => {
    if (!isAdmin(msg.from.id)) return;
    const users = await queryDocs('users', [], ['lastSeen', 'desc'], 15);
    if (!users.length) return bot.sendMessage(msg.chat.id, '📭 No users yet.');
    const list = users.map((u, i) =>
      `${i + 1}. ${u.firstName || ''} ${u.lastName || ''} ${u.username ? '(@' + u.username + ')' : ''}`.trim()
    ).join('\n');
    await bot.sendMessage(msg.chat.id, `👥 *Recent Users*\n\n${list}`, { parse_mode: 'Markdown' });
  });

  // /list
  bot.onText(/\/list/, async (msg) => {
    if (!isAdmin(msg.from.id)) return;
    const videos = await queryDocs('videos', [], ['createdAt', 'desc'], 10);
    if (!videos.length) return bot.sendMessage(msg.chat.id, '📭 No videos found.');
    const list = videos.map((v, i) =>
      `${i + 1}. *${v.title}*\n   ID: \`${v.id}\`\n   ${v.category || 'N/A'} | ${v.published ? '🟢' : '🔴'}`
    ).join('\n\n');
    await bot.sendMessage(msg.chat.id, `📋 *Recent Videos*\n\n${list}`, { parse_mode: 'Markdown' });
  });

  // /done — finish editing session
  bot.onText(/\/done/, async (msg) => {
    if (!isAdmin(msg.from.id)) return;
    const state = flow[msg.from.id];
    if (!state || state.step !== 'editing') return bot.sendMessage(msg.chat.id, '❌ No active edit session.');
    try {
      await updateDoc('videos', state.videoId, state.data);
      delete flow[msg.from.id];
      await bot.sendMessage(msg.chat.id, `✅ Updated \`${state.videoId}\``, { parse_mode: 'Markdown' });
    } catch (e) { await bot.sendMessage(msg.chat.id, `❌ ${e.message}`); }
  });

  // /skip — skip optional banner step
  bot.onText(/\/skip/, async (msg) => {
    if (!isAdmin(msg.from.id)) return;
    const state = flow[msg.from.id];
    if (state && state.step === 'await_banner') {
      await advanceFlow(msg.from.id, msg.chat.id);
    }
  });

  // Generic text handler — drives the multi-step add flow
  bot.on('message', async (msg) => {
    if (!isAdmin(msg.from.id)) return;
    if (!msg.text || msg.text.startsWith('/')) return;
    const state = flow[msg.from.id];
    if (!state) return;

    if (state.step === 'editing') return handleEditInput(msg, state);
    if (state.step.startsWith('await_')) return handleFlowInput(msg, state);
  });

  // Callback buttons (cancel / admin_help / delete / publish / category)
  bot.on('callback_query', async (q) => {
    const chatId = q.message.chat.id;
    const data = q.data;

    if (data.startsWith('cat:')) {
      const category = data.slice(4);
      await setCategoryAndAdvance(q.from.id, chatId, category);
      return bot.answerCallbackQuery(q.id);
    }
    if (data === 'cancel') {
      delete flow[q.from.id];
      await bot.editMessageText('❌ Cancelled.', { chat_id: chatId, message_id: q.message.message_id });
      return bot.answerCallbackQuery(q.id);
    }
    if (data === 'admin_help') {
      await sendAdminHelp(chatId);
      return bot.answerCallbackQuery(q.id);
    }
    if (data.startsWith('del:')) {
      const id = data.slice(4);
      try {
        await deleteDoc('videos', id);
        await bot.editMessageText(`✅ Deleted \`${id}\``, { chat_id: chatId, message_id: q.message.message_id, parse_mode: 'Markdown' });
      } catch (e) { await bot.answerCallbackQuery(q.id, { text: e.message }); }
      return bot.answerCallbackQuery(q.id);
    }
    if (data.startsWith('pub:')) {
      const id = data.slice(4);
      try {
        await updateDoc('videos', id, { published: true, publishedAt: new Date().toISOString() });
        await bot.editMessageText(`✅ Published! ID: \`${id}\``, { chat_id: chatId, message_id: q.message.message_id, parse_mode: 'Markdown' });
      } catch (e) { await bot.answerCallbackQuery(q.id, { text: e.message }); }
      return bot.answerCallbackQuery(q.id);
    }
    await bot.answerCallbackQuery(q.id);
  });
}

/* ════════════════════════════════════════════════════════════════════
   VIDEO INGESTION — full metadata capture per spec
════════════════════════════════════════════════════════════════════ */
async function ingestVideo(msg, fileObj) {
  const chatId = msg.chat.id;
  const userId = msg.from.id;

  const data = {
    fileId: fileObj.file_id,
    fileUniqueId: fileObj.file_unique_id || null,
    duration: fileObj.duration || 0,
    width: fileObj.width || 0,
    height: fileObj.height || 0,
    fileSize: fileObj.file_size || 0,
    mimeType: fileObj.mime_type || 'video/mp4',
    thumbFileId: fileObj.thumb?.file_id || fileObj.thumbnail?.file_id || null,
    caption: msg.caption || '',
    uploadedBy: userId,
  };

  flow[userId] = { step: 'await_title', data };

  await bot.sendMessage(chatId,
    `📹 *Video received!*\n\n` +
    `file_id: \`${data.fileId}\`\n` +
    `file_unique_id: \`${data.fileUniqueId}\`\n` +
    `Size: ${fmtSize(data.fileSize)}\n` +
    `Duration: ${fmtDur(data.duration)}\n` +
    `Resolution: ${data.width}×${data.height}\n\n` +
    `Let's add the details. *What's the title?*`,
    { parse_mode: 'Markdown' }
  );
}

/* ════════════════════════════════════════════════════════════════════
   CONVERSATIONAL ADD FLOW
   title → category → season → episode → description → banner → save
════════════════════════════════════════════════════════════════════ */
async function handleFlowInput(msg, state) {
  const chatId = msg.chat.id;
  const text = msg.text.trim();

  switch (state.step) {
    case 'await_fileid_manual':
      state.data.fileId = text;
      state.step = 'await_title';
      return bot.sendMessage(chatId, "Got it. *What's the title?*", { parse_mode: 'Markdown' });

    case 'await_title':
      state.data.title = text;
      state.step = 'await_category';
      return bot.sendMessage(chatId, 'Category?', {
        reply_markup: { inline_keyboard: [
          [{ text: '⛩️ Anime', callback_data: 'cat:Anime' }, { text: '📺 Web Series', callback_data: 'cat:Web Series' }],
          [{ text: '🎬 Movies', callback_data: 'cat:Movies' }, { text: '🎨 Cartoons', callback_data: 'cat:Cartoons' }],
        ] },
      });

    case 'await_season':
      state.data.season = parseInt(text) || null;
      state.step = 'await_episode';
      return bot.sendMessage(chatId, 'Episode number? (send 0 or /skip if not applicable)');

    case 'await_episode':
      state.data.episode = text === '0' ? null : (parseInt(text) || null);
      state.step = 'await_description';
      return bot.sendMessage(chatId, 'Short description?');

    case 'await_description':
      state.data.description = text;
      state.step = 'await_banner';
      return bot.sendMessage(chatId, 'Send a *banner/poster image* now, or /skip to save without one.', { parse_mode: 'Markdown' });
  }
}

// Category chosen via inline button
async function setCategoryAndAdvance(userId, chatId, category) {
  const state = flow[userId];
  if (!state) return;
  state.data.category = category;

  if (category === 'Movies') {
    state.step = 'await_description';
    await bot.sendMessage(chatId, 'Short description?');
  } else {
    state.step = 'await_season';
    await bot.sendMessage(chatId, 'Season number? (e.g. 1)');
  }
}

// Called after banner image received or /skip
async function advanceFlow(userId, chatId) {
  const state = flow[userId];
  if (!state) return;
  const d = state.data;

  if (!d.title) d.title = d.caption || `Video ${Date.now()}`;

  try {
    const videoId = await addDoc('videos', {
      title: d.title,
      description: d.description || '',
      category: d.category || 'Uncategorized',
      season: d.season || null,
      episode: d.episode || null,
      bannerFileId: d.bannerFileId || null,
      poster: null,
      thumbnail: null,
      telegram_file_id: d.fileId,
      telegram_unique_file_id: d.fileUniqueId,
      thumbFileId: d.thumbFileId || null,
      duration: d.duration || 0,
      size: d.fileSize || 0,
      mimeType: d.mimeType || 'video/mp4',
      views: 0,
      likes: 0,
      uploadDate: new Date().toISOString(),
      published: false,
      uploadedBy: d.uploadedBy,
    });

    delete flow[userId];

    await bot.sendMessage(chatId,
      `✅ *Saved!*\n\nTitle: *${d.title}*\nCategory: ${d.category}\nID: \`${videoId}\`\n\nPublish now?`,
      {
        parse_mode: 'Markdown',
        reply_markup: { inline_keyboard: [[
          { text: '🟢 Publish Now', callback_data: `pub:${videoId}` },
          { text: '📝 Keep Draft', callback_data: 'cancel' },
        ]] },
      });
  } catch (err) {
    await bot.sendMessage(chatId, `❌ Error saving: ${err.message}`);
  }
}

/* ════════════════════════════════════════════════════════════════════
   EDIT FLOW
════════════════════════════════════════════════════════════════════ */
async function handleEditInput(msg, state) {
  const chatId = msg.chat.id;
  const colon = msg.text.indexOf(':');
  if (colon === -1) return bot.sendMessage(chatId, '⚠️ Format: `field: value`', { parse_mode: 'Markdown' });

  const field = msg.text.slice(0, colon).trim().toLowerCase();
  const value = msg.text.slice(colon + 1).trim();
  const allowed = ['title', 'description', 'category', 'season', 'episode', 'poster', 'banner', 'thumbnail'];
  if (!allowed.includes(field)) return bot.sendMessage(chatId, `⚠️ Unknown field. Allowed: ${allowed.join(', ')}`);

  state.data[field] = (field === 'season' || field === 'episode') ? parseInt(value) : value;
  await bot.sendMessage(chatId, `✅ Set *${field}* = \`${value}\`\n\nSend more, or /done to save.`, { parse_mode: 'Markdown' });
}

/* ════════════════════════════════════════════════════════════════════
   UTILITIES
════════════════════════════════════════════════════════════════════ */
async function sendAdminHelp(chatId) {
  await bot.sendMessage(chatId,
    `*MYFLIX Admin Commands*\n\n` +
    `📤 Send/forward a video to begin upload\n` +
    `🖼 Then send a banner image when asked\n\n` +
    `/add — add manually by file_id\n` +
    `/edit <id> — edit metadata\n` +
    `/delete <id> — delete a video\n` +
    `/publish <id> — make visible\n` +
    `/unpublish <id> — hide from app\n` +
    `/stats — platform statistics\n` +
    `/users — recent users\n` +
    `/list — recent videos\n` +
    `/done — finish an edit session\n` +
    `/skip — skip optional banner step`,
    { parse_mode: 'Markdown' });
}

async function upsertUser(tgUser) {
  try {
    await setDoc('users', String(tgUser.id), {
      telegramId: tgUser.id,
      firstName: tgUser.first_name || '',
      lastName: tgUser.last_name || '',
      username: tgUser.username || '',
      languageCode: tgUser.language_code || 'en',
      lastSeen: new Date().toISOString(),
    });
  } catch (e) { console.error('[BOT] upsertUser error:', e.message); }
}

function fmtSize(bytes) {
  if (!bytes) return 'Unknown';
  const mb = bytes / (1024 * 1024);
  return mb > 1024 ? `${(mb / 1024).toFixed(2)} GB` : `${mb.toFixed(2)} MB`;
}
function fmtDur(s) {
  if (!s) return 'Unknown';
  const h = Math.floor(s / 3600), m = Math.floor((s % 3600) / 60), sec = s % 60;
  return h > 0 ? `${h}:${String(m).padStart(2, '0')}:${String(sec).padStart(2, '0')}` : `${m}:${String(sec).padStart(2, '0')}`;
}

function getBot() { return bot; }
function processUpdate(update) { if (bot) bot.processUpdate(update); }

module.exports = { initBot, getBot, processUpdate, isAdmin };
