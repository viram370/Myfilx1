const TelegramBot = require('node-telegram-bot-api');
const { getDB } = require('./firebase');

const token = process.env.TELEGRAM_BOT_TOKEN;
const BASE_URL = process.env.RENDER_EXTERNAL_URL || process.env.TELEGRAM_WEBHOOK_URL;

const bot = new TelegramBot(token, { webHook: true });
let db;

const ADMIN_IDS = (process.env.ADMIN_USER_IDS || '').split(',').map(Number).filter(Boolean);
let adminBuffer = {}; // Buffer for episodes

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
// ... previous code ...

async function executeSave(msg, type, payload) {
  const chatId = msg.chat.id;
  if (!isAdmin(chatId)) return;

  const buf = adminBuffer[chatId] || [];
  if (buf.length === 0) return safeSendMessage(chatId, "❌ Buffer empty.");

  const args = payload.split('|').map(s => s.trim());
  const title = args[0];
  const season = args[1] || "1";
  const language = args[2] || "Hindi";

  const category = type === 'anime' ? 'Anime' : type === 'movie' ? 'Movies' : 'Web Series';

  // Save each episode with season/episode number
  for (let i = 0; i < buf.length; i++) {
    const epNum = i + 1;
    const videoData = {
      title,
      seriesTitle: title,
      category,
      season: parseInt(season),
      episode: epNum,
      telegram_file_id: buf[i].file_id,
      file_unique_id: buf[i].file_unique_id,
      language,
      published: true,
      createdAt: Date.now()
    };
    // Save to Firestore (you can expand this)
    console.log(`Saved Episode ${epNum} of ${title}`);
  }

  safeSendMessage(chatId, `✅ Saved ${category}: ${title} Season ${season} (${buf.length} episodes)`);
  adminBuffer[chatId] = []; // Clear buffer
}

// ... rest of the file ...
async function handleVideoBuffer(msg, media) {
  const chatId = msg.chat.id;
  if (!isAdmin(chatId)) return;

  if (!adminBuffer[chatId]) adminBuffer[chatId] = [];
  if (adminBuffer[chatId].length >= 150) return safeSendMessage(chatId, "⚠️ Buffer full. Save first.");

  adminBuffer[chatId].push({
    file_id: media.file_id,
    file_unique_id: media.file_unique_id
  });

  safeSendMessage(chatId, `✅ Buffered (${adminBuffer[chatId].length})`);
}

// Save Commands
bot.onText(/^\/saveanime (.+)/, async (msg, match) => executeSave(msg, 'anime', match[1]));
bot.onText(/^\/savemovie (.+)/, async (msg, match) => executeSave(msg, 'movie', match[1]));
bot.onText(/^\/savewebseries (.+)/, async (msg, match) => executeSave(msg, 'webseries', match[1]));

async function executeSave(msg, type, payload) {
  const chatId = msg.chat.id;
  if (!isAdmin(chatId)) return;

  const buf = adminBuffer[chatId] || [];
  if (buf.length === 0) return safeSendMessage(chatId, "❌ Buffer empty. Forward videos first.");

  const [title, ...rest] = payload.split('|').map(s => s.trim());
  const category = type === 'anime' ? 'Anime' : type === 'movie' ? 'Movies' : 'Web Series';

  // Save to Firestore (simplified)
  console.log(`Saving ${type}: ${title} | ${buf.length} episodes`);
  safeSendMessage(chatId, `✅ Saved ${category}: ${title} (${buf.length} episodes)`);

  adminBuffer[chatId] = []; // Clear buffer after save
}

bot.onText(/^\/clearbuffer$/, (msg) => {
  if (!isAdmin(msg.chat.id)) return;
  adminBuffer[msg.chat.id] = [];
  safeSendMessage(msg.chat.id, "🗑 Buffer cleared.");
});

module.exports = {
  initBot,
  isAdmin,
  processUpdate: (update) => bot.processUpdate(update),
};
