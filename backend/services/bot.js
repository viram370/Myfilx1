const TelegramBot = require('node-telegram-bot-api');
const { getDB } = require('./firebase');

const token = process.env.TELEGRAM_BOT_TOKEN;
const BASE_URL = process.env.RENDER_EXTERNAL_URL || process.env.TELEGRAM_WEBHOOK_URL;

const bot = new TelegramBot(token, { webHook: true });
let db;

const ADMIN_IDS = (process.env.ADMIN_USER_IDS || '').split(',').map(Number).filter(Boolean);
let adminBuffer = {};

async function initBot() {
  db = getDB();
  if (!BASE_URL) return console.error('❌ Missing webhook URL');
  
  const webhookUrl = `${BASE_URL.replace(/\/+$/, '')}/webhook`;
  bot.setWebHook(webhookUrl, { secret_token: process.env.WEBHOOK_SECRET, drop_pending_updates: true })
    .then(() => console.log(`✅ Webhook: ${webhookUrl}`))
    .catch(err => console.error('Webhook Failed:', err.message));

  console.log(`🚀 Bot Started | Admins: ${ADMIN_IDS.length}`);
}

function isAdmin(chatId) { return ADMIN_IDS.includes(Number(chatId)); }

async function safeSendMessage(chatId, text) {
  try { await bot.sendMessage(chatId, text); } catch(e) {}
}

// Buffer System
bot.on('video', (msg) => handleVideoBuffer(msg, msg.video));
bot.on('document', (msg) => {
  if (msg.document?.mime_type?.startsWith('video/')) handleVideoBuffer(msg, msg.document);
});

async function handleVideoBuffer(msg, media) {
  const chatId = msg.chat.id;
  if (!isAdmin(chatId)) return;

  if (!adminBuffer[chatId]) adminBuffer[chatId] = [];
  if (adminBuffer[chatId].length >= 150) return safeSendMessage(chatId, "⚠️ Buffer full. Save first.");
adminBuffer[chatId].push({
    file_id: media.file_id,
    file_unique_id: media.file_unique_id,

    // Required for MTProto streaming
    channelId: msg.chat.id,
    messageId: msg.message_id
});
  
  safeSendMessage(chatId, `✅ Buffered (${adminBuffer[chatId].length})`);
}
bot.onText(/^\/start$/, async (msg) => {
  const chatId = msg.chat.id;

  await bot.sendMessage(
    chatId,
    `🎬 Welcome to MYFLIX

Choose an option:

/help - Commands
/list - Saved videos`
  );
});
bot.onText(/^\/help$/, async (msg) => {
  const chatId = msg.chat.id;

  await bot.sendMessage(chatId,
`📚 MYFLIX Commands

/saveanime Title|Season|Language
/savemovie Title|Season|Language
/savewebseries Title|Season|Language

/clearbuffer
/list`);
});
// Save Commands
bot.onText(/^\/saveanime (.+)/, async (msg, match) => executeSave(msg, 'anime', match[1]));
bot.onText(/^\/savemovie (.+)/, async (msg, match) => executeSave(msg, 'movie', match[1]));
bot.onText(/^\/savewebseries (.+)/, async (msg, match) => executeSave(msg, 'webseries', match[1]));

async function executeSave(msg, type, payload) {
  const chatId = msg.chat.id;
  if (!isAdmin(chatId)) return;

  const buf = adminBuffer[chatId] || [];
  if (buf.length === 0) return safeSendMessage(chatId, "❌ Buffer empty. Forward videos first.");

  const args = payload.split('|').map(s => s.trim());
  const title = args[0];
  const season = parseInt(args[1]) || 1;
  const language = args[2] || "Hindi";

  const category = type === 'anime' ? 'Anime' : type === 'movie' ? 'Movies' : 'Web Series';

  let savedCount = 0;

  for (let i = 0; i < buf.length; i++) {
    const epNum = i + 1;
    const videoId = `${type}_${title.toLowerCase().replace(/\s+/g, '-')}_s${season}_ep${epNum}`;
    
    // Duplicate check
    const existing = await db.collection('videos').doc(videoId).get();
    if (existing.exists) continue;
await db.collection('videos').doc(videoId).set({
  title,
  seriesTitle: title,
  category,
  season,
  episode: epNum,

  telegram_file_id: buf[i].file_id,
  file_unique_id: buf[i].file_unique_id,

  // Required for MTProto
  channelId: buf[i].channelId,
  messageId: buf[i].messageId,

  language,
  published: true,
  createdAt: Date.now()
});
    
    savedCount++;
  }

  safeSendMessage(chatId, `✅ Saved ${category}: ${title} Season ${season} (${savedCount} episodes)`);
  adminBuffer[chatId] = [];
}

bot.onText(/^\/clearbuffer$/, (msg) => {
  if (!isAdmin(msg.chat.id)) return;
  adminBuffer[msg.chat.id] = [];
  safeSendMessage(msg.chat.id, "🗑 Buffer cleared.");
});

bot.onText(/^\/list$/, async (msg) => {
  if (!isAdmin(msg.chat.id)) return;
  safeSendMessage(msg.chat.id, "📋 Recent videos saved. Check /api/videos");
});

module.exports = {
  initBot,
  isAdmin,
  processUpdate: (update) => bot.processUpdate(update),
};
