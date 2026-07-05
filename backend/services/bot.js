// ==========================================
// 🚀 ENTERPRISE TELEGRAM BOT - MYFLIX
// ENGINEERED FOR 100K+ USERS & VIDEOS
// ==========================================
require('dotenv').config();
const TelegramBot = require('node-telegram-bot-api');
const express = require('express');
const { initializeApp } = require("firebase/app");
const {
  getFirestore, doc, setDoc, getDoc, collection, query, where, limit,
  getDocs, deleteDoc, updateDoc, runTransaction, writeBatch
} = require("firebase/firestore");
const os = require('os');

// ==========================================
// 1. ENVIRONMENT & VALIDATION
// ==========================================
const token = process.env.BOT_TOKEN;
const RENDER_URL = process.env.RENDER_EXTERNAL_URL;

if (!token || !RENDER_URL) {
  console.error("❌ FATAL: Missing BOT_TOKEN or RENDER_EXTERNAL_URL.");
  process.exit(1);
}
if (!/^\d+:[A-Za-z0-9_-]+$/.test(token)) {
  console.warn("⚠️ WARNING: BOT_TOKEN format looks invalid. Ensure it's correct.");
}

// ==========================================
// 2. ROBUST ERROR HANDLING (Prevents Node Crashes)
// ==========================================
process.on('unhandledRejection', (reason) => logError('Unhandled Rejection', reason));
process.on('uncaughtException', (err) => logError('Uncaught Exception', err));

// ==========================================
// 3. FIREBASE INITIALIZATION
// ==========================================
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

// ==========================================
// 4. BOT & WEBHOOK INITIALIZATION
// ==========================================
const bot = new TelegramBot(token, { webHook: true });
const app = express();
const PORT = process.env.PORT || 10000;

bot.setWebHook(`${RENDER_URL}/bot${token}`, { drop_pending_updates: true })
  .then(() => console.log(`✅ Webhook registered at: ${RENDER_URL}/bot${token}`))
  .catch(err => console.error(`❌ Webhook Registration Failed:`, err.message));

app.use(express.json());

// ==========================================
// 5. GLOBAL STATE & CONFIG
// ==========================================
let redeemMode = {};
const adminBuffer = {}; 
const ADMIN_IDS = [6097315530];
const MAX_BUFFER_SIZE = 150; // FIX: Prevents Firestore Transaction Limit (500 docs)

// ==========================================
// 6. CORE UTILITIES & LOGGING
// ==========================================
function isAdmin(chatId) { return ADMIN_IDS.includes(Number(chatId)); }
function verifyAdmin(msg) {
  if (!isAdmin(msg.chat.id)) {
    safeSendMessage(msg.chat.id, "❌ You are not authorized.");
    return false;
  }
  return true;
}

// FIX: Strictly escape HTML to prevent Telegram 400 Bad Request Entity parse errors
const esc = (str) => String(str || '').replace(/[&<>"']/g, m => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[m]));
const cleanId = (text) => text.trim().toLowerCase().replace(/[^a-z0-9_-]/g, '');

function logInfo(action, data) { console.log(`[INFO] [${new Date().toISOString()}] [${action}]`, data ? JSON.stringify(data) : ''); }
function logError(action, error) { console.error(`[ERROR] [${new Date().toISOString()}] [${action}]`, error.message || error); }

// FIX: Generates scalable N-Gram search tokens for O(1) Firestore array-contains searches
function generateSearchTokens(title) {
  const clean = title.toLowerCase().replace(/[^a-z0-9\s]/g, '');
  const words = clean.split(/\s+/);
  const tokens = new Set();
  
  words.forEach(w => {
    for (let i = 2; i <= w.length; i++) tokens.add(w.substring(0, i)); // e.g. na, nar, naru, naruto
  });
  
  const noSpace = clean.replace(/\s/g, '');
  for (let i = 3; i <= Math.min(noSpace.length, 15); i++) {
    tokens.add(noSpace.substring(0, i)); // e.g. jujutsukaisen
  }
  return Array.from(tokens).slice(0, 30); // Firestore max array-contains limit safety
}

// FIX: Resilient Telegram API wrapper that perfectly handles 429 Flood Waits
async function safeSendVideo(chatId, fileId, options = {}, retries = 3) {
  try {
    return await bot.sendVideo(chatId, fileId, options);
  } catch (err) {
    if (err.response?.statusCode === 429 && retries > 0) {
      const waitTime = (err.response.body.parameters?.retry_after || 3) * 1000;
      logInfo("RATE_LIMIT_WAIT", `Waiting ${waitTime}ms for chat ${chatId}`);
      await new Promise(r => setTimeout(r, waitTime));
      return safeSendVideo(chatId, fileId, options, retries - 1);
    }
    throw err;
  }
}
async function safeSendMessage(chatId, text, options = {}) {
  try { return await bot.sendMessage(chatId, text, options); } 
  catch (err) { logError("SEND_MSG_FAIL", err); }
}

// ==========================================
// 7. DIAGNOSTIC & DEBUG MODE (SENIOR ENGINEER TOOLSET)
// ==========================================
bot.onText(/^\/debugbot$/, async (msg) => {
  if (!verifyAdmin(msg)) return;
  const webhook = await bot.getWebHookInfo();
  const mem = process.memoryUsage();
  const text = `🛠 <b>Bot Diagnostics:</b>\n\n` +
    `<b>Webhook URL:</b> ${webhook.url}\n` +
    `<b>Pending Updates:</b> ${webhook.pending_update_count}\n` +
    `<b>Last Error:</b> ${webhook.last_error_message || 'None'}\n` +
    `<b>RAM Usage:</b> ${(mem.rss / 1024 / 1024).toFixed(2)} MB\n` +
    `<b>Buffer Active:</b> ${Object.keys(adminBuffer).length} users`;
  safeSendMessage(msg.chat.id, text, { parse_mode: 'HTML' });
});

bot.onText(/^\/debugfirebase$/, async (msg) => {
  if (!verifyAdmin(msg)) return;
  try {
    const testRef = doc(db, 'system', 'diagnostics');
    await setDoc(testRef, { timestamp: Date.now(), status: "OK" });
    const snap = await getDoc(testRef);
    safeSendMessage(msg.chat.id, `🔥 <b>Firebase Diagnostics:</b>\n\nWrite: OK\nRead: OK\nData: ${snap.data().status}`, { parse_mode: 'HTML' });
  } catch (err) {
    safeSendMessage(msg.chat.id, `🔥 <b>Firebase Error:</b>\n<code>${esc(err.message)}</code>`, { parse_mode: 'HTML' });
  }
});

bot.onText(/^\/debugfile (.+)$/, async (msg, match) => {
  if (!verifyAdmin(msg)) return;
  const fileId = match[1].trim();
  try {
    const file = await bot.getFile(fileId);
    safeSendMessage(msg.chat.id, `📂 <b>File ID Validated by Telegram:</b>\nSize: ${file.file_size} bytes\nPath: ${file.file_path}`, { parse_mode: 'HTML' });
  } catch (err) {
    safeSendMessage(msg.chat.id, `❌ <b>File ID Error:</b>\n<code>${esc(err.message)}</code>\nThis means the file_id is invalid or belongs to another bot.`, { parse_mode: 'HTML' });
  }
});

bot.onText(/^\/testvideo (.+)$/i, async (msg, match) => {
  if (!verifyAdmin(msg)) return;
  const animeId = cleanId(match[1]);
  safeSendMessage(msg.chat.id, `⏳ <b>Testing Video Delivery for:</b> <code>${animeId}</code>`, { parse_mode: 'HTML' });

  try {
    const epRef = doc(db, 'anime', animeId, 'seasons', 'season1', 'episodes', 'ep1');
    const snap = await getDoc(epRef);
    if (!snap.exists()) return safeSendMessage(msg.chat.id, "❌ <b>Document Not Found:</b> ep1 missing in season1.", { parse_mode: 'HTML' });

    const epData = snap.data();
    if (!epData.file_id) return safeSendMessage(msg.chat.id, "❌ <b>Data Error:</b> <code>file_id</code> missing.", { parse_mode: 'HTML' });

    const safeFileId = String(epData.file_id); 
    safeSendMessage(msg.chat.id, `📤 <b>Attempting to send:</b>\nID: <code>${safeFileId}</code>`, { parse_mode: 'HTML' });

    await safeSendVideo(msg.chat.id, safeFileId, { caption: `🧪 <b>Test Successful</b>`, parse_mode: 'HTML' });
    safeSendMessage(msg.chat.id, "✅ <b>Diagnostics Passed</b>", { parse_mode: 'HTML' });
  } catch (err) {
    safeSendMessage(msg.chat.id, `❌ <b>Telegram API Error:</b>\n<code>${esc(err.message)}</code>`, { parse_mode: 'HTML' });
  }
});

// ==========================================
// 8. HIGH-SPEED BUFFER UPLOAD SYSTEM
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
  
  if (adminBuffer[chatId].length >= MAX_BUFFER_SIZE) {
    return safeSendMessage(chatId, `⚠️ <b>Buffer Limit Reached (${MAX_BUFFER_SIZE}).</b> Please save current episodes before adding more.`, { parse_mode: 'HTML' });
  }

  // FIX: Early duplicate check before buffering
  const isDup = await getDoc(doc(db, 'all_videos', String(mediaObj.file_unique_id)));
  if (isDup.exists()) {
    return safeSendMessage(chatId, `⚠️ <b>Skipped Duplicate:</b> Video already in database.`, { parse_mode: 'HTML' });
  }

  adminBuffer[chatId].push({
    file_id: String(mediaObj.file_id),
    file_unique_id: String(mediaObj.file_unique_id)
  });

  safeSendMessage(chatId, `✅ Episode buffered (${adminBuffer[chatId].length})`);
}

bot.onText(/^\/status$/, (msg) => {
  if (!verifyAdmin(msg)) return;
  const count = adminBuffer[msg.chat.id]?.length || 0;
  safeSendMessage(msg.chat.id, `ℹ️ <b>Buffer Status:</b> ${count} video(s) ready.`, { parse_mode: 'HTML' });
});

bot.onText(/^\/clearbuffer$/, (msg) => {
  if (!verifyAdmin(msg)) return;
  adminBuffer[msg.chat.id] = [];
  safeSendMessage(msg.chat.id, `🗑 <b>Buffer cleared.</b>`, { parse_mode: 'HTML' });
});

bot.onText(/^\/preview$/, (msg) => {
  if (!verifyAdmin(msg)) return;
  const buf = adminBuffer[msg.chat.id] || [];
  if (buf.length === 0) return safeSendMessage(msg.chat.id, "❌ Buffer empty.");
  
  let txt = "📋 <b>Buffered Episodes:</b>\n\n";
  buf.forEach((v, i) => txt += `<b>${i + 1}.</b> <code>${v.file_unique_id}</code>\n`);
  safeSendMessage(msg.chat.id, txt, { parse_mode: 'HTML' });
});

bot.onText(/^\/remove (\d+)$/, (msg, match) => {
  if (!verifyAdmin(msg)) return;
  const idx = parseInt(match[1]) - 1;
  const buf = adminBuffer[msg.chat.id] || [];
  
  if (idx >= 0 && idx < buf.length) {
    buf.splice(idx, 1);
    safeSendMessage(msg.chat.id, `✅ <b>Removed episode ${idx + 1}.</b>`, { parse_mode: 'HTML' });
  } else {
    safeSendMessage(msg.chat.id, "❌ Invalid number.");
  }
});

// ==========================================
// 9. TRANSACTION-SAFE FIRESTORE COMMITS
// ==========================================
// Universal Save Handler for all content types
bot.onText(/^\/(saveanime|savewebseries) (.+)$/i, (msg, match) => executeEpisodicSave(msg, match[1].toLowerCase(), match[2]));
bot.onText(/^\/(savemovie|savecartoon) (.+)$/i, (msg, match) => executeFlatSave(msg, match[1].toLowerCase(), match[2]));

async function executeEpisodicSave(msg, type, payload) {
  const chatId = msg.chat.id;
  if (!verifyAdmin(msg)) return;

  const buf = adminBuffer[chatId] || [];
  if (buf.length === 0) return safeSendMessage(chatId, "❌ Buffer is empty.");

  const args = payload.split('|').map(s => s.trim());
  if (args.length < 3) return safeSendMessage(chatId, `❌ <b>Format:</b>\n/${type} Name | Season 1 | Hindi`, { parse_mode: 'HTML' });

  const [title, seasonRaw, languageRaw] = args;
  const parentId = cleanId(title);
  const seasonId = cleanId(seasonRaw); 
  const language = languageRaw.toLowerCase();
  const searchTokens = generateSearchTokens(title); // O(1) Indexing
  const collectionName = type.replace('save', ''); // 'anime' or 'webseries'

  safeSendMessage(chatId, "⏳ <b>Committing transaction to Firestore...</b>", { parse_mode: 'HTML' });
  let savedCount = 0, duplicateCount = 0;

  try {
    await setDoc(doc(db, collectionName, parentId), {
      title, searchTokens, searchId: parentId, updatedAt: Date.now()
    }, { merge: true });

    const seasonRef = doc(db, collectionName, parentId, 'seasons', seasonId);

    // FIX: Intra-buffer deduplication logic (Prevents same-batch duplication)
    const uniqueBuffer = [];
    const seenIds = new Set();
    for (const v of buf) {
      if (!seenIds.has(v.file_unique_id)) {
        seenIds.add(v.file_unique_id);
        uniqueBuffer.push(v);
      } else duplicateCount++;
    }

    await runTransaction(db, async (t) => {
      const seasonDoc = await t.get(seasonRef);
      let nextEp = seasonDoc.exists() && seasonDoc.data().lastEpisodeNumber ? seasonDoc.data().lastEpisodeNumber + 1 : 1;
      
      const dupChecks = await Promise.all(uniqueBuffer.map(async vid => {
        const dupRef = doc(db, 'all_videos', vid.file_unique_id);
        return { vid, dupRef, dupDoc: await t.get(dupRef) };
      }));

      for (const item of dupChecks) {
        if (item.dupDoc.exists()) { duplicateCount++; continue; }

        const epRef = doc(db, collectionName, parentId, 'seasons', seasonId, 'episodes', `ep${nextEp}`);
        
        t.set(epRef, {
          file_id: String(item.vid.file_id), file_unique_id: String(item.vid.file_unique_id),
          episodeNumber: nextEp, language, quality: "720p", published: true, createdAt: Date.now()
        });
        t.set(item.dupRef, { type: collectionName, path: epRef.path });
        
        nextEp++; savedCount++;
      }
      t.set(seasonRef, { lastEpisodeNumber: nextEp - 1, updatedAt: Date.now() }, { merge: true });
    });

    adminBuffer[chatId] = [];
    safeSendMessage(chatId, `✅ <b>Save Complete!</b>\nEpisodes Saved: ${savedCount}\nDuplicates: ${duplicateCount}`, { parse_mode: 'HTML' });
  } catch (err) {
    logError("EPISODIC_SAVE_FAIL", err);
    safeSendMessage(chatId, `❌ <b>Transaction Error:</b>\n<code>${esc(err.message)}</code>`, { parse_mode: 'HTML' });
  }
}

async function executeFlatSave(msg, type, payload) {
  const chatId = msg.chat.id;
  if (!verifyAdmin(msg)) return;

  const buf = adminBuffer[chatId] || [];
  if (buf.length === 0) return safeSendMessage(chatId, "❌ Buffer is empty.");

  const args = payload.split('|').map(s => s.trim());
  if (args.length < 2) return safeSendMessage(chatId, `❌ <b>Format:</b>\n/${type} Name | Hindi`, { parse_mode: 'HTML' });

  const [title, languageRaw] = args;
  const docId = cleanId(title);
  const language = languageRaw.toLowerCase();
  const searchTokens = generateSearchTokens(title);
  const collectionName = type.replace('save', '') === 'movie' ? 'movies' : 'cartoons';

  // Flat saves only need the first item in buffer
  const vid = buf[0];

  try {
    const dupRef = doc(db, 'all_videos', vid.file_unique_id);
    const docRef = doc(db, collectionName, docId);

    await runTransaction(db, async (t) => {
      const dupCheck = await t.get(dupRef);
      if (dupCheck.exists()) throw new Error("Duplicate Video");

      t.set(docRef, {
        title, searchTokens, searchId: docId,
        file_id: String(vid.file_id), file_unique_id: String(vid.file_unique_id),
        language, quality: "720p", published: true, createdAt: Date.now()
      });
      t.set(dupRef, { type: collectionName, path: docRef.path });
    });

    adminBuffer[chatId].shift(); // Remove only the first
    safeSendMessage(chatId, `✅ <b>Saved ${title} successfully!</b>`, { parse_mode: 'HTML' });
  } catch (err) {
    safeSendMessage(chatId, `❌ <b>Save Failed:</b> ${esc(err.message)}`, { parse_mode: 'HTML' });
  }
}

// ==========================================
// 10. MESSAGE ROUTER & SEARCH
// ==========================================
bot.on('message', async (msg) => {
  const chatId = msg.chat.id;
  const text = msg.text ? msg.text.trim() : "";
  if (!text) return; // FIX: Prevents undefined crash on non-text messages

  try {
    // Avoid double-processing admin commands registered via bot.onText
    if (text.startsWith('/')) {
      const cmd = text.split(' ')[0].toLowerCase();
      const adminCmds = ['/saveanime', '/savemovie', '/savewebseries', '/savecartoon', '/status', '/clearbuffer', '/preview', '/remove', '/testvideo', '/debugbot', '/debugfirebase', '/debugfile', '/delete', '/publish', '/unpublish', '/edit', '/stats', '/users', '/list', '/creategift', '/deletegift', '/setplan'];
      if (adminCmds.includes(cmd)) return; // Handled by bot.onText
    }

    await ensureUser(chatId);
    await checkExpiry(chatId);

    if (text === "🔍 Search") {
      return safeSendMessage(chatId, "🔍 <b>Search Catalog</b>\n\n🎌 <b>Anime:</b> /anime [name] season [number]\n🎬 <b>Movies:</b> /movie [name]\n📺 <b>Web Series:</b> /webseries [name]\n🎨 <b>Cartoons:</b> /cartoon [name]", { parse_mode: 'HTML' });
    }
    if (text === "👤 Account") {
      return safeSendMessage(chatId, "👤 <b>MyFlix Account</b>", {
        parse_mode: 'HTML',
        reply_markup: { keyboard: [["💎 Plans","💳 Payment"], ["👤 Account Info","🎁 Gift Code"], ["🔙 Back"]], resize_keyboard: true }
      });
    }
    if (text === "👤 Account Info") {
      const user = await getUser(chatId);
      return safeSendMessage(chatId, `👤 <b>Account Details</b>\n\n🆔 ID: <code>${chatId}</code>\n💎 Plan: ${esc(user.plan)}\n💰 Balance: ₹${user.balance}\n📅 Expiry: ${user.expiry ? new Date(user.expiry).toLocaleDateString() : "None"}`, { parse_mode: 'HTML' });
    }
    if (text === "💎 Plans") return safeSendMessage(chatId, "💎 <b>Plans:</b>\n🍿 ₹20 Basic\n🎬 ₹50 WebSeries\n🔥 ₹100 Premium HD", { parse_mode: 'HTML' });
    if (text === "💳 Payment") {
      return safeSendMessage(chatId, "💳 <b>Select Plan:</b>", {
        parse_mode: 'HTML', reply_markup: { inline_keyboard: [[{ text: "🍿 ₹20", callback_data: "pay20" }], [{ text: "🎬 ₹50", callback_data: "pay50" }], [{ text: "🔥 ₹100", callback_data: "pay100" }]] }
      });
    }
    if (text === "🔙 Back") return safeSendMessage(chatId, "🎬 <b>Main Menu</b>", { parse_mode: 'HTML', reply_markup: { keyboard: [["🔍 Search", "👤 Account"], ["📝 Waitlist", "🆘 Support"]], resize_keyboard: true } });
    if (text === "🎁 Gift Code") {
      redeemMode[chatId] = true;
      return safeSendMessage(chatId, "🎁 <b>Enter Gift Code:</b>", { parse_mode: 'HTML' });
    }
    if (redeemMode[chatId] && !text.startsWith('/')) {
      delete redeemMode[chatId];
      return processGiftCode(chatId, text);
    }

    if (text.toLowerCase().startsWith('/anime ')) return executeSearch(chatId, text, 'anime');
    if (text.toLowerCase().startsWith('/movie ')) return executeSearch(chatId, text, 'movies');
    if (text.toLowerCase().startsWith('/webseries ')) return executeSearch(chatId, text, 'webseries');
    if (text.toLowerCase().startsWith('/cartoon ')) return executeSearch(chatId, text, 'cartoons');

  } catch (err) { logError("MESSAGE_ROUTER", err); }
});

bot.on('callback_query', async (query) => {
  const chatId = query.message.chat.id;
  const data = query.data;
  bot.answerCallbackQuery(query.id).catch(() => {});
  
  let cap = "", photoId = "";
  if (data === "pay20") { photoId = "AgACAgUAAxkBAAICJWn_BX9bvt0HOVooXrS_Y7VwpOngAAIQEGsbf1P4V02Yna5OBauhAQADAgADeAADOwQ"; cap = `🍿 ₹20 Plan\n📩 Send screenshot to @MyflixO`; }
  else if (data === "pay50") { photoId = "AgACAgUAAxkBAAICJGn_BUqfwwN0FHe7EzRRfhGHb8n2AAIPEGsbf1P4V4ijMEK46jkNAQADAgADeAADOwQ"; cap = `🎬 ₹50 Plan\n📩 Send screenshot to @MyflixO`; }
  else if (data === "pay100") { photoId = "AgACAgUAAxkBAAICI2n_BMY4S8rRS53FvZ9B71iSeybAAAIOEGsbf1P4V3jkTRZMtrsZAQADAgADeAADOwQ"; cap = `🔥 ₹100 Plan\n📩 Send screenshot to @MyflixO`; }
  
  if (photoId) {
    try { await bot.sendPhoto(chatId, photoId, { caption: cap }); }
    catch (err) { safeSendMessage(chatId, cap); } // Fallback if photoId is deleted
  }
});

// ==========================================
// 11. HIGH-PERFORMANCE SEARCH ENGINE
// ==========================================
async function executeSearch(chatId, commandText, collectionName) {
  const user = await getUser(chatId);
  if (collectionName === 'anime' && !["20", "50", "100"].includes(user.plan)) return safeSendMessage(chatId, "⚠️ Upgrade plan for Anime.");
  if (collectionName === 'movies' && user.plan !== "100") return safeSendMessage(chatId, "⚠️ ₹100 Plan required for Movies.");
  
  // FIX: Accurate Season Parsing from query string[cite: 2]
  let queryTerm = commandText.replace(/^\/(anime|movie|webseries|cartoon)\s+/i, '');
  const seasonMatch = queryTerm.match(/season\s*(\d+)/i);
  let targetSeason = null;
  if (seasonMatch) {
    targetSeason = `season${seasonMatch[1]}`;
    queryTerm = queryTerm.replace(seasonMatch[0], '').trim();
  }
  
  const tokenTerm = cleanId(queryTerm);
  if (tokenTerm.length < 2) return safeSendMessage(chatId, "❌ Search term too short.");

  safeSendMessage(chatId, "🔍 <i>Searching...</i>", { parse_mode: 'HTML' });

  try {
    // FIX: O(1) Array-Contains Search (No more Full Table Scans!)[cite: 1]
    const q = query(collection(db, collectionName), where('searchTokens', 'array-contains', tokenTerm), limit(1));
    const snap = await getDocs(q);

    if (snap.empty) {
      // Fallback for old data without tokens
      const oldSnap = await getDocs(collection(db, collectionName));
      let found = false;
      for (const d of oldSnap.docs) {
        if (d.id.includes(tokenTerm)) {
          await processSearchResult(chatId, d.id, d.data(), collectionName, targetSeason);
          found = true; break;
        }
      }
      if (!found) return safeSendMessage(chatId, `❌ <b>No published content found.</b>`, { parse_mode: 'HTML' });
      return;
    }

    const docData = snap.docs[0].data();
    if (docData.published === false) return safeSendMessage(chatId, `❌ Content is private.`, { parse_mode: 'HTML' });
    await processSearchResult(chatId, snap.docs[0].id, docData, collectionName, targetSeason);
    
  } catch (err) { logError("executeSearch", err); safeSendMessage(chatId, "❌ Search failed."); }
}

async function processSearchResult(chatId, docId, docData, col, requestedSeason) {
  if (['movies', 'cartoons'].includes(col)) {
    return safeSendVideo(chatId, docData.file_id, { caption: `🎬 <b>${esc(docData.title)}</b>`, parse_mode: 'HTML' });
  }

  // Episodic Fetching
  const seasonsSnap = await getDocs(collection(db, col, docId, "seasons"));
  if (seasonsSnap.empty) return safeSendMessage(chatId, "❌ No episodes uploaded yet.");
  
  let targetSeasonDoc;
  if (requestedSeason) {
    targetSeasonDoc = seasonsSnap.docs.find(d => d.id === requestedSeason);
    if (!targetSeasonDoc) return safeSendMessage(chatId, `❌ Season ${requestedSeason.replace('season','')} not found.`);
  } else {
    // FIX: Sort seasons numerically, not alphabetically (e.g. season10 > season2)
    const sorted = seasonsSnap.docs.sort((a,b) => parseInt(a.id.replace('season','')) - parseInt(b.id.replace('season','')));
    targetSeasonDoc = sorted[sorted.length - 1]; 
  }

  const epSnap = await getDocs(collection(db, col, docId, "seasons", targetSeasonDoc.id, "episodes"));
  let episodes = [];
  epSnap.forEach(d => { if (d.data().published) episodes.push(d.data()); });
  episodes.sort((a, b) => a.episodeNumber - b.episodeNumber);

  safeSendMessage(chatId, `🎌 <b>Found: ${esc(docData.title)} (${targetSeasonDoc.id})</b>\n<i>Sending episodes...</i>`, { parse_mode: 'HTML' });
  
  for (const ep of episodes) {
    try {
      await safeSendVideo(chatId, String(ep.file_id), { caption: `🎌 <b>${esc(docData.title)}</b>\n🎬 Episode ${ep.episodeNumber}`, parse_mode: 'HTML' });
    } catch (err) {
      logError(`SEND_EPISODE_FAIL`, err);
      safeSendMessage(chatId, `❌ Failed to load Ep ${ep.episodeNumber}.`, { parse_mode: 'HTML' });
    }
  }
}

// ==========================================
// 12. ADMIN MANAGEMENT COMMANDS
// ==========================================
bot.onText(/^\/delete (.+)/, async (msg, match) => {
  if (!verifyAdmin(msg)) return;
  const targetId = cleanId(match[1]);
  safeSendMessage(msg.chat.id, "⏳ <i>Executing recursive delete...</i>", { parse_mode: 'HTML' });
  try {
    let deleted = false;
    for (const col of ['movies', 'cartoons', 'anime', 'webseries']) {
      const ref = doc(db, col, targetId);
      const snap = await getDoc(ref);
      if (snap.exists()) {
        const batch = writeBatch(db);
        if (['anime', 'webseries'].includes(col)) {
          const sSnap = await getDocs(collection(db, col, targetId, "seasons"));
          for (const s of sSnap.docs) {
            const eSnap = await getDocs(collection(db, col, targetId, "seasons", s.id, "episodes"));
            eSnap.forEach(ep => { batch.delete(doc(db, "all_videos", ep.data().file_unique_id)); batch.delete(ep.ref); });
            batch.delete(s.ref);
          }
        } else {
          batch.delete(doc(db, "all_videos", snap.data().file_unique_id));
        }
        batch.delete(ref);
        await batch.commit(); // FIX: Batch operations ensure atomic cleanup of orphaned data
        deleted = true;
        safeSendMessage(msg.chat.id, `🗑 <b>Fully Deleted:</b> ${esc(targetId)} from ${col}`, { parse_mode: 'HTML' });
        break;
      }
    }
    if (!deleted) safeSendMessage(msg.chat.id, "❌ ID not found.");
  } catch (err) { safeSendMessage(msg.chat.id, "❌ Recursive delete failed."); }
});

bot.onText(/^\/(publish|unpublish) (.+)/, async (msg, match) => {
  if (!verifyAdmin(msg)) return;
  const status = match[1] === 'publish';
  const targetId = cleanId(match[2]);
  try {
    const batch = writeBatch(db);
    let found = false;
    for (const col of ['movies', 'cartoons', 'anime', 'webseries']) {
      const ref = doc(db, col, targetId);
      const snap = await getDoc(ref);
      if (snap.exists()) {
        found = true; batch.update(ref, { published: status });
        if (['anime', 'webseries'].includes(col)) {
          const sSnap = await getDocs(collection(db, col, targetId, "seasons"));
          for (const s of sSnap.docs) {
            const eSnap = await getDocs(collection(db, col, targetId, "seasons", s.id, "episodes"));
            eSnap.forEach(ep => batch.update(ep.ref, { published: status }));
          }
        }
        await batch.commit(); break;
      }
    }
    safeSendMessage(msg.chat.id, found ? `✅ <b>Cascade Published = ${status}</b>` : "❌ Not found.", { parse_mode: 'HTML' });
  } catch (err) { safeSendMessage(msg.chat.id, "❌ Publish execution failed."); }
});

// Stats, Plan & Gift Code Functions Restored & Fortified
bot.onText(/^\/stats$/, async (msg) => {
  if (!verifyAdmin(msg)) return;
  safeSendMessage(msg.chat.id, "⏳ <i>Aggregating data...</i>", { parse_mode: 'HTML' });
  try {
    const [uSnap, aSnap, mSnap, vSnap] = await Promise.all([
      getDocs(collection(db, "users")), getDocs(collection(db, "anime")),
      getDocs(collection(db, "movies")), getDocs(collection(db, "all_videos"))
    ]);
    safeSendMessage(msg.chat.id, `📊 <b>Production Stats</b>\nUsers: ${uSnap.size}\nAnime: ${aSnap.size}\nMovies: ${mSnap.size}\nVideos Indexed: ${vSnap.size}`, { parse_mode: 'HTML' });
  } catch (err) { logError("renderStats", err); }
});

bot.onText(/^\/creategift (.+)/, async (msg, match) => {
  if (!verifyAdmin(msg)) return;
  const args = match[1].split("|");
  if (args.length < 3) return safeSendMessage(msg.chat.id, "❌ Format: /creategift CODE|PLAN|DAYS");
  try {
    await setDoc(doc(db, "giftcodes", args[0].trim().toUpperCase()), { plan: args[1].trim(), days: Number(args[2]), used: false });
    safeSendMessage(msg.chat.id, `✅ <b>Gift Code Created</b>`, { parse_mode: 'HTML' });
  } catch (err) { logError("createGift", err); }
});

bot.onText(/^\/setplan (.+)/, async (msg, match) => {
  if (!verifyAdmin(msg)) return;
  const args = match[1].split(' ');
  if (args.length < 2) return safeSendMessage(msg.chat.id, "❌ Usage: /setplan [userID] [planAmount]");
  try {
    await ensureUser(args[0]);
    await setDoc(doc(db, "users", args[0]), { plan: args[1], expiry: Date.now() + (30*24*60*60*1000) }, { merge: true });
    safeSendMessage(msg.chat.id, "✅ User updated.");
  } catch (err) { logError("setPlan", err); }
});

// ==========================================
// 13. CORE ACCOUNT SYSTEM
// ==========================================
async function processGiftCode(chatId, text) {
  try {
    const code = text.trim().toUpperCase();
    const ref = doc(db, "giftcodes", code);
    const snap = await getDoc(ref);
    if (!snap.exists() || snap.data().used) return safeSendMessage(chatId, "❌ Invalid or used code.");
    
    const user = await getUser(chatId);
    await setDoc(doc(db, "users", String(chatId)), { plan: snap.data().plan, expiry: Date.now() + (snap.data().days * 24*60*60*1000) }, { merge: true });
    await updateDoc(ref, { used: true, usedBy: chatId });
    safeSendMessage(chatId, `✅ <b>Success! Plan activated.</b>`, { parse_mode: 'HTML' });
  } catch(err) { logError("processGiftCode", err); }
}

async function ensureUser(chatId) {
  try {
    const ref = doc(db, "users", String(chatId));
    if (!(await getDoc(ref)).exists()) await setDoc(ref, { plan: "Free", balance: 0, expiry: null });
  } catch(err) {}
}

async function getUser(chatId) {
  try { return (await getDoc(doc(db, "users", String(chatId)))).data() || { plan: 'Free' }; } catch(err) { return { plan: 'Free' }; }
}

async function checkExpiry(chatId) {
  try {
    const user = await getUser(chatId);
    if (user.expiry && Date.now() > user.expiry) await updateDoc(doc(db, "users", String(chatId)), { plan: "Free", expiry: null });
  } catch(err) {}
}
// ==========================================
// 14. PUBLIC MODULE INTERFACE
// ==========================================
function initBot() {
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

module.exports = {
  initBot,
  isAdmin,
  processUpdate: (update) => bot.processUpdate(update),
};
// ==========================================
// 14. EXPRESS & WEBHOOK INFRASTRUCTURE
// ==========================================
// FIX: Decouple payload processing to immediately return 200, preventing Webhook Timeout Retries
app.post(`/bot${token}`, (req, res) => {
  res.sendStatus(200); 
  setImmediate(() => {
    try { bot.processUpdate(req.body); } 
    catch (err) { logError("WebhookUpdate", err); }
  });
});

app.get('/', (req, res) => res.send('✅ MyFlix Enterprise Bot Engine is Active.'));

app.listen(PORT, () => console.log(`🚀 Production Server running on port ${PORT}`));
