// ==========================================
// REQUIRED PACKAGES & INITIALIZATION
// ==========================================
require('dotenv').config();
const TelegramBot = require('node-telegram-bot-api');
const express = require('express');
const { initializeApp } = require("firebase/app");
const {
  getFirestore, doc, setDoc, getDoc, collection,
  getDocs, deleteDoc, updateDoc, runTransaction, writeBatch
} = require("firebase/firestore");

// --- ENVIRONMENT & BOT_TOKEN VALIDATION ---
const token = process.env.BOT_TOKEN;
const RENDER_URL = process.env.RENDER_EXTERNAL_URL;

if (!token || !RENDER_URL) {
  console.error("❌ FATAL: Missing BOT_TOKEN or RENDER_EXTERNAL_URL.");
  process.exit(1);
}

if (!/^\d+:[A-Za-z0-9_-]+$/.test(token)) {
  console.warn("⚠️ WARNING: Your BOT_TOKEN format looks invalid. Ensure it is copied exactly from BotFather.");
}

// --- GLOBAL ERROR HANDLERS ---
process.on('unhandledRejection', (reason, promise) => {
  console.error('[FATAL] Unhandled Rejection:', reason);
});
process.on('uncaughtException', (err) => {
  console.error('[FATAL] Uncaught Exception:', err);
});

// --- FIREBASE CONFIGURATION ---
const firebaseConfig = {
  apiKey: process.env.FIREBASE_API_KEY || "AIzaSyBqWwfapX_rvJLeYFA7ikzl-hvfnabp6Z8",
  authDomain: "myfilx-635aa.firebaseapp.com",
  projectId: "myfilx-635aa",
  storageBucket: "myfilx-635aa.firebasestorage.app",
  messagingSenderId: "759079187430",
  appId: "1:759079187430:web:05f9480cecb84f1712dc27",
  measurementId: "G-XPYJS7PTWD"
};

const firebaseApp = initializeApp(firebaseConfig);
const db = getFirestore(firebaseApp);

// --- BOT & WEBHOOK INITIALIZATION ---
const bot = new TelegramBot(token, { webHook: true });
const app = express();
const PORT = process.env.PORT || 10000;

bot.setWebHook(`${RENDER_URL}/bot${token}`).then(() => {
  console.log(`✅ Webhook successfully registered at: ${RENDER_URL}/bot${token}`);
}).catch(err => {
  console.error(`❌ Webhook Registration Failed:`, err.message);
});

app.use(express.json());

// --- IN-MEMORY STATE ---
let redeemMode = {};
const adminBuffer = {}; 
const ADMIN_IDS = [6097315530];

// ==========================================
// UTILITY FUNCTIONS & LOGGING
// ==========================================
function isAdmin(chatId) { return ADMIN_IDS.includes(Number(chatId)); }
function verifyAdmin(msg) {
  if (!isAdmin(msg.chat.id)) {
    bot.sendMessage(msg.chat.id, "❌ You are not authorized to use this command.");
    return false;
  }
  return true;
}

const esc = (str) => String(str || '').replace(/[&<>"']/g, match => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[match]));
const cleanId = (text) => text.trim().toLowerCase().replace(/[^a-z0-9_-]/g, '');

function logInfo(action, data) { 
  console.log(`[INFO] [${new Date().toISOString()}] [${action}]`, data ? JSON.stringify(data, null, 2) : ''); 
}
function logError(action, error) { 
  console.error(`[ERROR] [${new Date().toISOString()}] [${action}]`, error); 
}

// ==========================================
// TEST VIDEO COMMAND (DIAGNOSTICS)
// ==========================================
bot.onText(/^\/testvideo (.+)$/i, async (msg, match) => {
  const chatId = msg.chat.id;
  if (!verifyAdmin(msg)) return;

  const animeId = cleanId(match[1]);
  bot.sendMessage(chatId, `⏳ <b>Running Diagnostics for:</b> <code>${animeId}</code>`, { parse_mode: 'HTML' });

  try {
    const epRef = doc(db, 'anime', animeId, 'seasons', 'season1', 'episodes', 'ep1');
    const snap = await getDoc(epRef);
    
    if (!snap.exists()) {
      return bot.sendMessage(chatId, "❌ <b>Document Not Found:</b> ep1 missing in season1.", { parse_mode: 'HTML' });
    }

    const epData = snap.data();
    logInfo("TEST_VIDEO_FIRESTORE_PAYLOAD", epData);

    if (!epData.file_id) {
      return bot.sendMessage(chatId, "❌ <b>Data Error:</b> Document exists but <code>file_id</code> is missing or undefined.", { parse_mode: 'HTML' });
    }

    const safeFileId = String(epData.file_id); 
    
    logInfo("TEST_SEND_ATTEMPT", { 
      chatId, 
      original_file_id: epData.file_id, 
      stringified_file_id: safeFileId,
      file_unique_id: epData.file_unique_id 
    });

    bot.sendMessage(chatId, `📤 <b>Attempting to send:</b>\nID: <code>${safeFileId}</code>`, { parse_mode: 'HTML' });

    const sentMsg = await bot.sendVideo(chatId, safeFileId, { 
      caption: `🧪 <b>Test Send Successful</b>\nEpisode Number: ${epData.episodeNumber}`,
      parse_mode: 'HTML'
    });

    logInfo("TEST_SEND_SUCCESS", { message_id: sentMsg.message_id });
    bot.sendMessage(chatId, "✅ <b>Diagnostics Passed:</b> Telegram accepted the file_id.", { parse_mode: 'HTML' });

  } catch (err) {
    const apiErrorDetails = {
      code: err.code || "UNKNOWN_CODE",
      message: err.message || "No error message",
      body: err.response?.body || "No response body available"
    };
    
    logError("TEST_SEND_FAILED", apiErrorDetails);
    
    bot.sendMessage(chatId, 
      `❌ <b>Telegram API Error Details:</b>\n\n` +
      `<b>Code:</b> ${esc(apiErrorDetails.code)}\n` +
      `<b>Message:</b> ${esc(apiErrorDetails.message)}\n\n` +
      `<i>Check your server logs for the full response body. Note: If the file_id belongs to another bot, Telegram will reject it here.</i>`, 
      { parse_mode: 'HTML' }
    );
  }
});

// ==========================================
// BUFFER-BASED VIDEO UPLOAD SYSTEM
// ==========================================
bot.on('video', (msg) => handleVideoBuffer(msg, msg.video));
bot.on('document', (msg) => {
  if (msg.document && msg.document.mime_type && msg.document.mime_type.startsWith('video/')) {
    handleVideoBuffer(msg, msg.document);
  }
});

async function handleVideoBuffer(msg, mediaObj) {
  const chatId = msg.chat.id;
  if (!isAdmin(chatId)) return;

  if (!adminBuffer[chatId]) adminBuffer[chatId] = [];

  adminBuffer[chatId].push({
    file_id: String(mediaObj.file_id),
    file_unique_id: String(mediaObj.file_unique_id)
  });

  bot.sendMessage(chatId, `✅ Episode buffered (${adminBuffer[chatId].length})`);
}

bot.onText(/^\/status$/, (msg) => {
  if (!verifyAdmin(msg)) return;
  const count = adminBuffer[msg.chat.id]?.length || 0;
  bot.sendMessage(msg.chat.id, `ℹ️ <b>Upload Buffer Status:</b>\nCurrently holding ${count} video(s).`, { parse_mode: 'HTML' });
});

bot.onText(/^\/clearbuffer$/, (msg) => {
  if (!verifyAdmin(msg)) return;
  adminBuffer[msg.chat.id] = [];
  bot.sendMessage(msg.chat.id, `🗑 <b>Buffer cleared successfully.</b>`, { parse_mode: 'HTML' });
});

bot.onText(/^\/saveanime (.+)$/i, async (msg, match) => {
  const chatId = msg.chat.id;
  if (!verifyAdmin(msg)) return;

  const buf = adminBuffer[chatId] || [];
  if (buf.length === 0) return bot.sendMessage(chatId, "❌ Buffer is empty. Forward some videos first.");

  const args = match[1].split('|').map(s => s.trim());
  if (args.length < 3) return bot.sendMessage(chatId, "❌ <b>Format:</b>\n/saveanime Anime Name | Season 1 | Hindi", { parse_mode: 'HTML' });

  const [animeName, seasonRaw, languageRaw] = args;
  const animeId = cleanId(animeName);
  const seasonId = cleanId(seasonRaw); 
  const language = languageRaw.toLowerCase();

  bot.sendMessage(chatId, "⏳ <b>Locking transactions and saving episodes...</b>", { parse_mode: 'HTML' });

  let savedCount = 0;
  let duplicateCount = 0;

  try {
    await setDoc(doc(db, 'anime', animeId), {
      title: animeName,
      searchId: animeId,
      updatedAt: Date.now()
    }, { merge: true });

    const seasonRef = doc(db, 'anime', animeId, 'seasons', seasonId);

    await runTransaction(db, async (t) => {
      const seasonDoc = await t.get(seasonRef);
      let nextEp = 1;
      
      if (seasonDoc.exists() && seasonDoc.data().lastEpisodeNumber) {
        nextEp = seasonDoc.data().lastEpisodeNumber + 1;
      }

      const dupChecks = [];
      for (const vid of buf) {
        const dupRef = doc(db, 'all_videos', vid.file_unique_id);
        dupChecks.push({ vid, dupRef, dupDoc: await t.get(dupRef) });
      }

      for (const item of dupChecks) {
        if (item.dupDoc.exists()) {
          duplicateCount++;
          continue; 
        }

        const epId = `ep${nextEp}`;
        const epRef = doc(db, 'anime', animeId, 'seasons', seasonId, 'episodes', epId);
        
        t.set(epRef, {
          file_id: String(item.vid.file_id),
          file_unique_id: String(item.vid.file_unique_id),
          episodeNumber: nextEp,
          language: language,
          quality: "720p",
          published: true,
          createdAt: Date.now()
        });

        t.set(item.dupRef, { type: 'anime', path: epRef.path });
        
        nextEp++;
        savedCount++;
      }

      t.set(seasonRef, { lastEpisodeNumber: nextEp - 1, updatedAt: Date.now() }, { merge: true });
    });

    adminBuffer[chatId] = [];
    bot.sendMessage(chatId, `✅ <b>Save Complete!</b>\n\nEpisodes Saved: ${savedCount}\nDuplicates Skipped: ${duplicateCount}`, { parse_mode: 'HTML' });

  } catch (err) {
    logError("SAVE_ANIME_FAILED", err);
    bot.sendMessage(chatId, `❌ <b>Error saving anime:</b>\n${esc(err.message)}`, { parse_mode: 'HTML' });
  }
});

// ==========================================
// CENTRAL MESSAGE CONTROLLER
// ==========================================
bot.on('message', async (msg) => {
  const chatId = msg.chat.id;
  const text = msg.text ? msg.text.trim() : "";
  if (!text) return;

  try {
    if (text.startsWith('/')) {
      const cmd = text.split(' ')[0].toLowerCase();
      const adminCmds = ['/delete', '/publish', '/unpublish', '/edit', '/stats', '/users', '/list', '/creategift', '/deletegift', '/setplan', '/testvideo'];
      if (adminCmds.includes(cmd) && !verifyAdmin(msg)) return;
    }

    await ensureUser(chatId);
    await checkExpiry(chatId);

    if (text === "🔍 Search") {
      return bot.sendMessage(chatId, "🔍 <b>Search Catalog</b>\n\n🎌 <b>Anime:</b> /anime [name]\n🎬 <b>Movies:</b> /movie [name]", { parse_mode: 'HTML' });
    }
    if (text === "👤 Account") {
      return bot.sendMessage(chatId, "👤 <b>MyFlix Account</b>", {
        parse_mode: 'HTML',
        reply_markup: { keyboard: [["💎 Plans","💳 Payment"], ["👤 Account Info","🎁 Gift Code"], ["🔙 Back"]], resize_keyboard: true }
      });
    }
    
    if (text.toLowerCase().startsWith('/anime ')) return executeSearch(chatId, text, 'anime');
    if (text.toLowerCase().startsWith('/movie ')) return executeSearch(chatId, text, 'movies');

  } catch (err) {
    logError("MESSAGE_ROUTER_CRASH", err);
  }
});

// ==========================================
// ADVANCED SEARCH ENGINE & VIDEO DISPATCHER
// ==========================================
async function executeSearch(chatId, commandText, collectionName) {
  const queryTerm = cleanId(commandText.replace(/^\/(anime|movie|webseries|cartoon)\s+/i, ''));
  if (queryTerm.length < 2) return bot.sendMessage(chatId, "❌ Please provide a longer search term.");

  bot.sendMessage(chatId, "🔍 <i>Searching database...</i>", { parse_mode: 'HTML' });

  try {
    const snap = await getDocs(collection(db, collectionName));
    let matchedDoc = null;
    let matchedId = null;

    snap.forEach(d => {
      const searchId = d.data().searchId || cleanId(d.data().title || d.id);
      if (searchId.includes(queryTerm)) {
        matchedDoc = d.data();
        matchedId = d.id;
      }
    });

    if (!matchedDoc || (matchedDoc.published === false)) {
      return bot.sendMessage(chatId, `❌ <b>No content found matching your query.</b>`, { parse_mode: 'HTML' });
    }

    if (collectionName === 'anime') {
      const seasonsSnap = await getDocs(collection(db, collectionName, matchedId, "seasons"));
      if (seasonsSnap.empty) return bot.sendMessage(chatId, "❌ No episodes uploaded yet.");
      
      const latestSeason = seasonsSnap.docs[seasonsSnap.docs.length - 1];
      const epSnap = await getDocs(collection(db, collectionName, matchedId, "seasons", latestSeason.id, "episodes"));
      
      let episodes = [];
      epSnap.forEach(d => { if (d.data().published) episodes.push(d.data()); });
      episodes.sort((a, b) => a.episodeNumber - b.episodeNumber);

      bot.sendMessage(chatId, `🎌 <b>Found: ${esc(matchedDoc.title)} (${latestSeason.id})</b>\n<i>Sending episodes...</i>`, { parse_mode: 'HTML' });
      
      for (const ep of episodes) {
        try {
          const targetFileId = String(ep.file_id);
          
          logInfo("USER_SEND_ATTEMPT", { chatId, episode: ep.episodeNumber, file_id: targetFileId });
          
          await bot.sendVideo(chatId, targetFileId, { 
            caption: `🎌 <b>${esc(matchedDoc.title)}</b>\n🎬 Episode ${ep.episodeNumber}`, 
            parse_mode: 'HTML' 
          });

          logInfo("USER_SEND_SUCCESS", { episode: ep.episodeNumber });
          await new Promise(r => setTimeout(r, 1200)); 
          
        } catch (err) {
          const apiErrorDetails = {
            code: err.code || "UNKNOWN_CODE",
            message: err.message || "No error message",
            body: err.response?.body || "No response body available"
          };
          
          logError(`USER_SEND_FAILED_EPISODE_${ep.episodeNumber}`, apiErrorDetails);
          
          bot.sendMessage(chatId, 
            `❌ <b>Failed to load Episode ${ep.episodeNumber}</b>\n` + 
            `If this persists, the file_id may belong to a different bot instance.`, 
            { parse_mode: 'HTML' }
          );
        }
      }
    }
  } catch (err) { 
    logError("SEARCH_CRASH", err); 
    bot.sendMessage(chatId, "❌ Search query failed."); 
  }
}

// ==========================================
// CORE USER ACCOUNT LOGIC
// ==========================================
async function ensureUser(chatId) {
  try {
    const ref = doc(db, "users", String(chatId));
    const snap = await getDoc(ref);
    if (!snap.exists()) await setDoc(ref, { plan: "Free", balance: 0, expiry: null });
  } catch(err) { logError("ensureUser", err); }
}

async function getUser(chatId) {
  try { return (await getDoc(doc(db, "users", String(chatId)))).data(); } 
  catch(err) { return { plan: 'Free', balance: 0 }; }
}

async function checkExpiry(chatId) {
  try {
    const user = await getUser(chatId);
    if (user && user.expiry && Date.now() > user.expiry) {
      await setDoc(doc(db, "users", String(chatId)), { ...user, plan: "Free", expiry: null, balance: 0 }, { merge: true });
    }
  } catch(err) { logError("checkExpiry", err); }
}

// ==========================================
// RENDER COMPATIBILITY & WEBHOOK ENDPOINTS
// ==========================================
app.post(`/bot${token}`, (req, res) => {
  res.sendStatus(200);
  try { bot.processUpdate(req.body); } 
  catch (err) { logError("WebhookUpdate", err); }
});

app.get('/', (req, res) => {
  res.send('✅ MyFlix Enterprise Bot Engine is Active & Webhook is listening.');
});

app.listen(PORT, () => {
  console.log(`🚀 Production Server initialized on port ${PORT}`);
});
