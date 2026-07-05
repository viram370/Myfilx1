const TelegramBot = require('node-telegram-bot-api');
const { getDB } = require('./firebase');

const token = process.env.TELEGRAM_BOT_TOKEN;
const BASE_URL = process.env.RENDER_EXTERNAL_URL || process.env.TELEGRAM_WEBHOOK_URL;

const bot = new TelegramBot(token, { webHook: true });
let db;

const ADMIN_IDS = (process.env.ADMIN_USER_IDS || '').split(',').map(Number).filter(Boolean);

async function initBot() {
  db = getDB();

  if (!BASE_URL) {
    console.error('❌ FATAL: Missing webhook URL');
    return;
  }

  const webhookUrl = `${BASE_URL.replace(/\/+$/, '')}/webhook`;
  bot.setWebHook(webhookUrl, {
    secret_token: process.env.WEBHOOK_SECRET,
    drop_pending_updates: true,
  })
  .then(() => console.log(`✅ Webhook registered at: ${webhookUrl}`))
  .catch(err => console.error('❌ Webhook Failed:', err.message));

  console.log(`🚀 Telegram Bot Started | ${ADMIN_IDS.length} admins`);
}

function isAdmin(chatId) { return ADMIN_IDS.includes(Number(chatId)); }

module.exports = {
  initBot,
  isAdmin,
  processUpdate: (update) => bot.processUpdate(update),
};
