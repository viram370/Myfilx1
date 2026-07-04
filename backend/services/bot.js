// ==========================================
// REQUIRED PACKAGES & INITIALIZATION
// ==========================================
require('dotenv').config();
const TelegramBot = require('node-telegram-bot-api');
const express = require('express');
const { initializeApp } = require("firebase/app");
const {
  getFirestore, doc, setDoc, getDoc, collection, addDoc,  
  getDocs, deleteDoc, updateDoc, runTransaction, writeBatch
} = require("firebase/firestore");

// --- ENVIRONMENT VALIDATION ---
if (!process.env.BOT_TOKEN || !process.env.RENDER_EXTERNAL_URL) {
  console.error("❌ FATAL: Missing BOT_TOKEN or RENDER_EXTERNAL_URL in environment variables.");
  process.exit(1);
}

// --- GLOBAL ERROR HANDLERS ---
process.on('unhandledRejection', (reason, promise) => {
  console.error('[FATAL] Unhandled Rejection at:', promise, 'reason:', reason);
});
process.on('uncaughtException', (err) => {
  console.error('[FATAL] Uncaught Exception:', err);
});

// --- FIREBASE CONFIGURATION ---
const firebaseConfig = {
  apiKey: "AIzaSyBqWwfapX_rvJLeYFA7ikzl-hvfnabp6Z8",
  authDomain: "myfilx-635aa.firebaseapp.com",
  projectId: "myfilx-635aa",
  storageBucket: "myfilx-635aa.firebasestorage.app",
  messagingSenderId: "759079187430",
  appId: "1:759079187430:web:05f9480cecb84f1712dc27",
  measurementId: "G-XPYJS7PTWD"
};

const firebaseApp = initializeApp(firebaseConfig);
const db = getFirestore(firebaseApp);

// --- BOT & SERVER CONFIGURATION ---
const token = process.env.BOT_TOKEN;
const bot = new TelegramBot(token, { webHook: true });
const app = express();
const PORT = process.env.PORT || 10000;
const RENDER_URL = process.env.RENDER_EXTERNAL_URL;
bot.setWebHook(`${RENDER_URL}/bot${token}`);

app.use(express.json());

// --- IN-MEMORY FALLBACKS ---
let redeemMode = {};
const ADMIN_IDS = [6097315530];
const SESSION_TIMEOUT_MS = 1000 * 60 * 60; // 1 Hour TTL for uploads

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

// Strict HTML Escaping to prevent Telegram 400 Errors
const esc = (str) => String(str || '').replace(/[&<>"']/g, match => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[match]));
const cleanId = (text) => text.trim().toLowerCase().replace(/[^a-z0-9_-]/g, '');

function logInfo(action, data) { console.log(`[INFO] [${new Date().toISOString()}] [${action}]`, data ? JSON.stringify(data) : ''); }
function logError(action, error) { console.error(`[ERROR] [${new Date().toISOString()}] [${action}]`, error.stack || error); }

// ==========================================
// PERSISTENT ADMIN UPLOAD STATE MANAGER
// ==========================================
async function getAdminState(chatId) {
  try {
    const snap = await getDoc(doc(db, "admin_sessions", String(chatId)));
    if (!snap.exists()) return null;
    const state = snap.data();
    
    // TTL Garbage Collection Check
    if (Date.now() - state.updatedAt > SESSION_TIMEOUT_MS) {
      await saveAdminState(chatId, null);
      return null;
    }
    return state;
  } catch (err) { logError("getAdminState", err); return null; }
}

async function saveAdminState(chatId, state) {
  try {
    if (state === null) {
      await deleteDoc(doc(db, "admin_sessions", String(chatId)));
    } else {
      state.updatedAt = Date.now();
      await setDoc(doc(db, "admin_sessions", String(chatId)), state);
    }
  } catch (err) { logError("saveAdminState", err); }
}

// ==========================================
// DUPLICATE PREVENTION ENGINE
// ==========================================
async function isDuplicateVideo(fileUniqueId) {
  if (!fileUniqueId) return false;
  try {
    const snap = await getDoc(doc(db, "all_videos", fileUniqueId));
    return snap.exists();
  } catch (err) { logError("isDuplicateVideo", err); return false; }
}

// ==========================================
// INTERACTIVE MULTI-STEP UPLOAD WIZARD
// ==========================================
bot.on('video', (msg) => handleMediaUpload(msg, msg.video));
bot.on('document', (msg) => {
  if (msg.document && msg.document.mime_type && msg.document.mime_type.startsWith('video/')) {
    handleMediaUpload(msg, msg.document);
  }
});

async function handleMediaUpload(msg, mediaObj) {
  const chatId = msg.chat.id;
  if (!isAdmin(chatId)) return;

  try {
    const isDup = await isDuplicateVideo(mediaObj.file_unique_id);
    if (isDup) return bot.sendMessage(chatId, "❌ <b>Duplicate Detected:</b> This exact video file already exists.", { parse_mode: 'HTML' });

    let state = await getAdminState(chatId);

    // Bulk Episode Uploading (Transaction Safe Buffering)
    if (state && state.step === 'UPLOAD_VIDEOS') {
      state.videos.push({ file_id: mediaObj.file_id, file_unique_id: mediaObj.file_unique_id });
      await saveAdminState(chatId, state);
      return bot.sendMessage(chatId, `⏳ <b>Buffer Updated:</b> ${state.videos.length} episode(s) ready.\n<i>Send the next episode, or type /finish to proceed.</i>`, { parse_mode: 'HTML' });
    }

    // Initialize New Wizard Session
    state = {
      step: 'CHOOSE_TYPE',
      videos: [{ file_id: mediaObj.file_id, file_unique_id: mediaObj.file_unique_id }],
      meta: {},
      uploaderId: msg.from.id,
      createdAt: Date.now(),
      updatedAt: Date.now()
    };
    await saveAdminState(chatId, state);

    bot.sendMessage(chatId, "🎌 <b>New Upload Started! Choose Content Type:</b>", {
      parse_mode: 'HTML',
      reply_markup: {
        inline_keyboard: [
          [{ text: "🎌 Anime", callback_data: "type_anime" }, { text: "🎥 Anime Movie", callback_data: "type_animemovie" }],
          [{ text: "🎬 Movie", callback_data: "type_movie" }, { text: "📺 Web Series", callback_data: "type_webseries" }],
          [{ text: "🎨 Cartoon", callback_data: "type_cartoon" }],
          [{ text: "❌ Cancel Upload", callback_data: "cancel_upload" }]
        ]
      }
    });
  } catch (err) { logError("handleMediaUpload", err); bot.sendMessage(chatId, "❌ Internal error parsing media upload."); }
}

bot.on('callback_query', async (query) => {
  const chatId = query.message.chat.id;
  const data = query.data;
  
  try {
    // Acknowledge immediately
    bot.answerCallbackQuery(query.id).catch(() => {});

    if (data === "cancel_upload") {
      await saveAdminState(chatId, null);
      return bot.editMessageText("❌ <b>Upload Wizard Cancelled.</b>", { chat_id: chatId, message_id: query.message.message_id, parse_mode: 'HTML' });
    }

    // 1. Wizard Type Selection
    if (data.startsWith("type_")) {
      let state = await getAdminState(chatId);
      if (!state) return bot.sendMessage(chatId, "❌ Session expired or invalid. Please send the video again.");
      
      state.type = data.replace("type_", "");
      
      if (state.type === 'animemovie') {
        state.step = 'ENTER_PARENT_NAME';
        await saveAdminState(chatId, state);
        return bot.sendMessage(chatId, "📝 <b>Step 1: Enter Parent Anime Name:</b>\n<i>(e.g., Jujutsu Kaisen)</i>", { parse_mode: 'HTML' });
      } else {
        state.step = 'ENTER_NAME';
        await saveAdminState(chatId, state);
        return bot.sendMessage(chatId, "📝 <b>Step 1: Enter Title/Name:</b>", { parse_mode: 'HTML' });
      }
    }

    // 2. Publish Decision
    if (data.startsWith("commit_")) {
      let state = await getAdminState(chatId);
      if (!state) return bot.sendMessage(chatId, "❌ Upload session expired.");
      
      const isPublish = data === "commit_publish";
      bot.sendMessage(chatId, "⏳ <b>Locking transactions and executing batch writes...</b>", { parse_mode: 'HTML' });
      
      await commitUploadToFirestore(chatId, state, isPublish);
      await saveAdminState(chatId, null);
      return bot.sendMessage(chatId, "✅ <b>Saved Successfully!</b>", { parse_mode: 'HTML' });
    }

    // 3. User Payment Callbacks
    if (["pay20", "pay50", "pay100"].includes(data)) {
      let cap = "", photoId = "";
      if (data === "pay20") {
        photoId = "AgACAgUAAxkBAAICJWn_BX9bvt0HOVooXrS_Y7VwpOngAAIQEGsbf1P4V02Yna5OBauhAQADAgADeAADOwQ";
        cap = `🍿 ₹20 Anime Basic Plan\n✅ Anime Access\n✅ Hindi Dubbed\n✅ 480p Quality\n\n📩 Send payment screenshot to @MyflixO`;
      } else if (data === "pay50") {
        photoId = "AgACAgUAAxkBAAICJGn_BUqfwwN0FHe7EzRRfhGHb8n2AAIPEGsbf1P4V4ijMEK46jkNAQADAgADeAADOwQ";
        cap = `🎬 ₹50 Anime + WebSeries Plan\n✅ Anime Access\n✅ WebSeries Access\n✅ 720p HD Quality\n\n📩 Send payment screenshot to @MyflixO`;
      } else if (data === "pay100") {
        photoId = "AgACAgUAAxkBAAICI2n_BMY4S8rRS53FvZ9B71iSeybAAAIOEGsbf1P4V3jkTRZMtrsZAQADAgADeAADOwQ";
        cap = `🔥 ₹100 Premium HD Plan\n✅ Access to All Content\n✅ Premium HD Streaming\n\n📩 Send payment screenshot to @MyflixO`;
      }
      return bot.sendPhoto(chatId, photoId, { caption: cap });
    }
  } catch (err) {
    logError("callback_query", err);
    bot.sendMessage(chatId, `❌ Callback execution failed: ${esc(err.message)}`, { parse_mode: 'HTML' });
  }
});

// WIZARD TEXT INPUT PROCESSOR
async function processWizardInput(chatId, text, state) {
  try {
    switch (state.step) {
      case 'ENTER_PARENT_NAME':
        state.meta.parentAnime = text;
        state.step = 'ENTER_NAME';
        bot.sendMessage(chatId, "📝 <b>Step 2: Enter Movie Name:</b>", { parse_mode: 'HTML' });
        break;
      case 'ENTER_NAME':
        state.meta.name = text;
        if (['anime', 'webseries'].includes(state.type)) {
          state.step = 'ENTER_SEASON';
          bot.sendMessage(chatId, "🔢 <b>Step 2: Enter Season Number:</b>\n<i>(e.g., 1 or 2)</i>", { parse_mode: 'HTML' });
        } else {
          state.step = 'ENTER_LANGUAGE';
          bot.sendMessage(chatId, "🌐 <b>Step 2: Enter Language:</b>", { parse_mode: 'HTML' });
        }
        break;
      case 'ENTER_SEASON':
        state.meta.season = "season" + text.replace(/\s+/g, '');
        state.step = 'ENTER_LANGUAGE';
        bot.sendMessage(chatId, "🌐 <b>Step 3: Enter Language:</b>", { parse_mode: 'HTML' });
        break;
      case 'ENTER_LANGUAGE':
        state.meta.language = text.toLowerCase();
        state.step = 'ENTER_QUALITY';
        bot.sendMessage(chatId, "📀 <b>Step 4: Enter Quality:</b>\n<i>(e.g., 1080p, 720p)</i>", { parse_mode: 'HTML' });
        break;
      case 'ENTER_QUALITY':
        state.meta.quality = text;
        if (['anime', 'webseries'].includes(state.type)) {
          state.step = 'UPLOAD_VIDEOS';
          bot.sendMessage(chatId, "📥 <b>Step 5: Bulk Episode Upload:</b>\n<i>Continuously forward the remaining episodes for this season. Type /finish when done.</i>", { parse_mode: 'HTML' });
        } else {
          state.step = 'ENTER_POSTER';
          bot.sendMessage(chatId, "🖼 <b>Step 5: Enter Poster Image URL:</b>", { parse_mode: 'HTML' });
        }
        break;
      case 'ENTER_POSTER':
        state.meta.poster = text;
        state.step = 'ENTER_BANNER';
        bot.sendMessage(chatId, "📐 <b>Step 6: Enter Banner Image URL:</b>", { parse_mode: 'HTML' });
        break;
      case 'ENTER_BANNER':
        state.meta.banner = text;
        state.step = 'ENTER_DESCRIPTION';
        bot.sendMessage(chatId, "📝 <b>Step 7: Enter Description:</b>", { parse_mode: 'HTML' });
        break;
      case 'ENTER_DESCRIPTION':
        state.meta.description = text;
        state.step = 'ENTER_GENRES';
        bot.sendMessage(chatId, "🎨 <b>Step 8: Enter Genres:</b>\n<i>(Comma separated)</i>", { parse_mode: 'HTML' });
        break;
      case 'ENTER_GENRES':
        state.meta.genres = text.split(',').map(s => s.trim()).filter(Boolean);
        state.step = 'ENTER_YEAR';
        bot.sendMessage(chatId, "📅 <b>Step 9: Enter Release Year:</b>", { parse_mode: 'HTML' });
        break;
      case 'ENTER_YEAR':
        state.meta.year = text;
        state.step = 'ENTER_RATING';
        bot.sendMessage(chatId, "⭐ <b>Step 10: Enter Rating:</b>\n<i>(e.g., 8.5)</i>", { parse_mode: 'HTML' });
        break;
      case 'ENTER_RATING':
        state.meta.rating = text;
        state.step = 'DECIDE_PUBLISH';
        bot.sendMessage(chatId, "❓ <b>Final Step: Publish Visibility</b>", {
          parse_mode: 'HTML',
          reply_markup: {
            inline_keyboard: [
              [{ text: "🚀 Publish Live", callback_data: "commit_publish" }],
              [{ text: "📝 Save as Draft", callback_data: "commit_draft" }]
            ]
          }
        });
        break;
    }
    await saveAdminState(chatId, state);
  } catch (err) { logError("processWizardInput", err); }
}

// ==========================================
// FIRESTORE ATOMIC UPLOAD TRANSACTIONS
// ==========================================
async function commitUploadToFirestore(chatId, state, isPublish) {
  const meta = state.meta;
  const rootPayload = {
    title: meta.name,
    searchId: cleanId(meta.name),
    poster: meta.poster,
    banner: meta.banner,
    description: meta.description,
    genres: meta.genres,
    year: meta.year,
    rating: meta.rating,
    published: isPublish,
    uploaderId: state.uploaderId,
    updatedAt: Date.now()
  };

  const filePayload = {
    language: meta.language,
    quality: meta.quality,
    published: isPublish,
    createdAt: Date.now()
  };

  // 1. Anime & Web Series (Transaction-Safe Bulk Upload)
  if (['anime', 'webseries'].includes(state.type)) {
    const rootCol = state.type;
    const seriesId = cleanId(meta.name);
    
    // Save metadata once at series level
    await setDoc(doc(db, rootCol, seriesId), rootPayload, { merge: true });

    await runTransaction(db, async (t) => {
      const seasonRef = doc(db, rootCol, seriesId, "seasons", meta.season);
      const seasonDoc = await t.get(seasonRef);
      
      let nextEp = 1;
      if (seasonDoc.exists() && seasonDoc.data().lastEpisodeNumber) {
        nextEp = seasonDoc.data().lastEpisodeNumber + 1;
      }

      for (const vid of state.videos) {
        const epRef = doc(db, rootCol, seriesId, "seasons", meta.season, "episodes", `ep${nextEp}`);
        t.set(epRef, { ...filePayload, episodeNumber: nextEp, file_id: vid.file_id, file_unique_id: vid.file_unique_id });
        t.set(doc(db, "all_videos", vid.file_unique_id), { type: state.type, path: epRef.path });
        nextEp++;
      }
      
      t.set(seasonRef, { lastEpisodeNumber: nextEp - 1, updatedAt: Date.now() }, { merge: true });
    });
  } 
  
  // 2. Anime Movies (Saved beneath Anime Parent)
  else if (state.type === 'animemovie') {
    const parentId = cleanId(meta.parentAnime);
    const movieId = cleanId(meta.name);
    
    await setDoc(doc(db, "anime", parentId), { title: meta.parentAnime, searchId: parentId, updatedAt: Date.now() }, { merge: true });
    
    const movieRef = doc(db, "anime", parentId, "movies", movieId);
    const vid = state.videos[0];
    await setDoc(movieRef, { ...rootPayload, ...filePayload, file_id: vid.file_id, file_unique_id: vid.file_unique_id });
    await setDoc(doc(db, "all_videos", vid.file_unique_id), { type: state.type, path: movieRef.path });
  }

  // 3. Movies & Cartoons (Flat Level)
  else {
    const rootCol = state.type === 'movie' ? 'movies' : 'cartoons';
    const docId = cleanId(meta.name);
    const docRef = doc(db, rootCol, docId);
    const vid = state.videos[0];
    
    await setDoc(docRef, { ...rootPayload, ...filePayload, file_id: vid.file_id, file_unique_id: vid.file_unique_id });
    await setDoc(doc(db, "all_videos", vid.file_unique_id), { type: state.type, path: docRef.path });
  }
}

// ==========================================
// CENTRAL MESSAGE CONTROLLER
// ==========================================
bot.on('message', async (msg) => {
  const chatId = msg.chat.id;
  const text = msg.text ? msg.text.trim() : "";
  if (!text) return;

  try {
    // 1. Intercept Admin Wizard Flow
    if (isAdmin(chatId)) {
      const state = await getAdminState(chatId);
      if (state) {
        if (text === '/cancel') {
          await saveAdminState(chatId, null);
          return bot.sendMessage(chatId, "❌ <b>Upload Wizard Cancelled.</b>", { parse_mode: 'HTML' });
        }
        if (text === '/status') {
          return bot.sendMessage(chatId, `ℹ️ <b>Upload Status:</b>\nStep: ${state.step}\nType: ${state.type}\nBuffered Videos: ${state.videos.length}`, { parse_mode: 'HTML' });
        }
        if (text === '/finish' && state.step === 'UPLOAD_VIDEOS') {
          state.step = 'ENTER_POSTER';
          await saveAdminState(chatId, state);
          return bot.sendMessage(chatId, "🖼 <b>Step 5: Enter Poster Image URL:</b>", { parse_mode: 'HTML' });
        }
        if (!text.startsWith('/')) {
          return await processWizardInput(chatId, text, state);
        }
      }
    }

    // 2. Base Command Check
    if (text.startsWith('/')) {
      const cmd = text.split(' ')[0].toLowerCase();
      const adminCmds = ['/delete', '/publish', '/unpublish', '/edit', '/stats', '/users', '/list', '/creategift', '/deletegift', '/setplan'];
      if (adminCmds.includes(cmd) && !verifyAdmin(msg)) return;
    }

    await ensureUser(chatId);
    await checkExpiry(chatId);

    // 3. Admin Command Execution
    if (text.startsWith('/delete ')) return executeDelete(chatId, text);
    if (text.startsWith('/publish ')) return cascadePublish(chatId, text, true);
    if (text.startsWith('/unpublish ')) return cascadePublish(chatId, text, false);
    if (text.startsWith('/edit ')) return executeEdit(chatId, text);
    if (text === '/stats') return renderStats(chatId);
    if (text === '/users') return listUsers(chatId);
    if (text === '/list') return listContent(chatId);
    if (text.startsWith('/creategift ')) return createGift(chatId, text);
    if (text.startsWith('/deletegift ')) return deleteGift(chatId, text);
    if (text.startsWith('/setplan ')) return setPlan(chatId, text);

    // 4. User System Commands
    if (text === "🔍 Search") {
      return bot.sendMessage(chatId, "🔍 <b>Search Catalog</b>\n\n🎌 <b>Anime:</b> /anime [name]\n🎬 <b>Movies:</b> /movie [name]\n📺 <b>Web Series:</b> /webseries [name]\n🎨 <b>Cartoons:</b> /cartoon [name]", { parse_mode: 'HTML' });
    }
    if (text === "👤 Account") {
      return bot.sendMessage(chatId, "👤 <b>MyFlix Account</b>", {
        parse_mode: 'HTML',
        reply_markup: { keyboard: [["💎 Plans","💳 Payment"], ["👤 Account Info","🎁 Gift Code"], ["🔙 Back"]], resize_keyboard: true }
      });
    }
    if (text === "👤 Account Info") {
      const user = await getUser(chatId);
      return bot.sendMessage(chatId, `👤 <b>Account Details</b>\n\n🆔 User ID: <code>${chatId}</code>\n💎 Plan: ${esc(user.plan)}\n💰 Balance: ₹${user.balance}\n📅 Expiry: ${user.expiry ? new Date(user.expiry).toLocaleDateString() : "None"}`, { parse_mode: 'HTML' });
    }
    if (text === "💎 Plans") {
      return bot.sendMessage(chatId, "💎 <b>Premium Access Tiers:</b>\n\n🍿 ₹20 Basic Anime\n🎬 ₹50 Anime + WebSeries\n🔥 ₹100 Ultimate Premium HD", { parse_mode: 'HTML' });
    }
    if (text === "💳 Payment") {
      return bot.sendMessage(chatId, "💳 <b>Select Plan:</b>", {
        parse_mode: 'HTML',
        reply_markup: { inline_keyboard: [[{ text: "🍿 ₹20", callback_data: "pay20" }], [{ text: "🎬 ₹50", callback_data: "pay50" }], [{ text: "🔥 ₹100", callback_data: "pay100" }]] }
      });
    }
    if (text === "🔙 Back") {
      return bot.sendMessage(chatId, "🎬 <b>Main Menu</b>", { parse_mode: 'HTML', reply_markup: { keyboard: [["🔍 Search", "👤 Account"], ["📝 Waitlist", "🆘 Support"], ["📜 Terms & Privacy"]], resize_keyboard: true } });
    }
    
    // Gift Code System
    if (text === "🎁 Gift Code") {
      redeemMode[chatId] = true;
      return bot.sendMessage(chatId, "🎁 <b>Enter Gift Code:</b>", { parse_mode: 'HTML' });
    }
    if (redeemMode[chatId] && !text.startsWith('/')) {
      delete redeemMode[chatId];
      return processGiftCode(chatId, text);
    }

    // 5. Smart Search Routing
    if (text.toLowerCase().startsWith('/anime ')) return executeSearch(chatId, text, 'anime');
    if (text.toLowerCase().startsWith('/movie ')) return executeSearch(chatId, text, 'movies');
    if (text.toLowerCase().startsWith('/webseries ')) return executeSearch(chatId, text, 'webseries');
    if (text.toLowerCase().startsWith('/cartoon ')) return executeSearch(chatId, text, 'cartoons');

  } catch (err) {
    logError("MessageRouter", err);
    bot.sendMessage(chatId, "❌ An internal server error occurred while routing the command.");
  }
});

// ==========================================
// ADVANCED SEARCH ENGINE (Fuzzy + Caseless)
// ==========================================
async function executeSearch(chatId, commandText, collectionName) {
  const user = await getUser(chatId);
  const typeMap = { 'anime': 'Anime', 'movies': 'Movies', 'webseries': 'Web Series', 'cartoons': 'Cartoons' };
  
  if (collectionName === 'anime' && !["20", "50", "100"].includes(user.plan)) return bot.sendMessage(chatId, "⚠️ Upgrade plan to access Anime.");
  if (collectionName === 'movies' && user.plan !== "100") return bot.sendMessage(chatId, "⚠️ ₹100 Plan required for Movies.");
  if (collectionName === 'webseries' && !["50", "100"].includes(user.plan)) return bot.sendMessage(chatId, "⚠️ Upgrade to ₹50 plan for Web Series.");

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

    if (!matchedDoc || (matchedDoc.published === false)) return bot.sendMessage(chatId, `❌ <b>No published ${typeMap[collectionName]} found matching your query.</b>`, { parse_mode: 'HTML' });

    // Handle Flat Collections
    if (['movies', 'cartoons'].includes(collectionName)) {
      return bot.sendVideo(chatId, matchedDoc.file_id, {
        caption: `🎬 <b>${esc(matchedDoc.title)}</b>\n⭐ Rating: ${esc(matchedDoc.rating)}\n📅 Year: ${esc(matchedDoc.year)}\n\n📝 ${esc(matchedDoc.description)}`,
        parse_mode: 'HTML'
      });
    }

    // Handle Episodic Collections (Anime / Web Series)
    if (['anime', 'webseries'].includes(collectionName)) {
      const seasonsSnap = await getDocs(collection(db, collectionName, matchedId, "seasons"));
      if (seasonsSnap.empty) return bot.sendMessage(chatId, "❌ No episodes uploaded yet.");
      
      const latestSeason = seasonsSnap.docs[seasonsSnap.docs.length - 1];
      const epSnap = await getDocs(collection(db, collectionName, matchedId, "seasons", latestSeason.id, "episodes"));
      
      let episodes = [];
      epSnap.forEach(d => { if (d.data().published) episodes.push(d.data()); });
      episodes.sort((a, b) => a.episodeNumber - b.episodeNumber);

      bot.sendMessage(chatId, `🎌 <b>Found: ${esc(matchedDoc.title)} (${latestSeason.id})</b>\n<i>Sending episodes...</i>`, { parse_mode: 'HTML' });
      for (const ep of episodes) {
        await bot.sendVideo(chatId, ep.file_id, { caption: `🎌 <b>${esc(matchedDoc.title)}</b>\n🎬 Episode ${ep.episodeNumber}`, parse_mode: 'HTML' });
        await new Promise(r => setTimeout(r, 1200)); // Rate limit safety
      }
    }
  } catch (err) { logError("executeSearch", err); bot.sendMessage(chatId, "❌ Search query failed."); }
}

// ==========================================
// RECURSIVE DELETE ENGINE
// ==========================================
async function executeDelete(chatId, text) {
  const targetId = cleanId(text.replace('/delete ', ''));
  bot.sendMessage(chatId, "⏳ <i>Executing recursive delete...</i>", { parse_mode: 'HTML' });
  try {
    const rootCollections = ['movies', 'cartoons', 'anime', 'webseries'];
    let deleted = false;

    for (const col of rootCollections) {
      const ref = doc(db, col, targetId);
      const snap = await getDoc(ref);
      
      if (snap.exists()) {
        const batch = writeBatch(db);
        
        // Target nested subcollections for Episodic content
        if (['anime', 'webseries'].includes(col)) {
          const seasonsSnap = await getDocs(collection(db, col, targetId, "seasons"));
          for (const season of seasonsSnap.docs) {
            const epSnap = await getDocs(collection(db, col, targetId, "seasons", season.id, "episodes"));
            epSnap.forEach(ep => {
              batch.delete(doc(db, "all_videos", ep.data().file_unique_id));
              batch.delete(ep.ref);
            });
            batch.delete(season.ref);
          }
          if (col === 'anime') {
            const moviesSnap = await getDocs(collection(db, col, targetId, "movies"));
            moviesSnap.forEach(m => {
              batch.delete(doc(db, "all_videos", m.data().file_unique_id));
              batch.delete(m.ref);
            });
          }
        } else {
          batch.delete(doc(db, "all_videos", snap.data().file_unique_id));
        }

        batch.delete(ref);
        await batch.commit();
        deleted = true;
        bot.sendMessage(chatId, `🗑 <b>Fully Deleted:</b> ${esc(targetId)} from ${col} (Including all subcollections).`, { parse_mode: 'HTML' });
        break;
      }
    }
    if (!deleted) bot.sendMessage(chatId, "❌ ID not found in root records.");
  } catch (err) { logError("executeDelete", err); bot.sendMessage(chatId, "❌ Recursive delete failed."); }
}

// ==========================================
// CASCADE PUBLISH / UNPUBLISH
// ==========================================
async function cascadePublish(chatId, text, status) {
  const targetId = cleanId(text.replace(/^\/(unpublish|publish)\s+/, ''));
  try {
    const batch = writeBatch(db);
    const collections = ['movies', 'cartoons', 'anime', 'webseries'];
    let found = false;

    for (const col of collections) {
      const ref = doc(db, col, targetId);
      const snap = await getDoc(ref);
      if (snap.exists()) {
        found = true;
        batch.update(ref, { published: status });
        
        if (['anime', 'webseries'].includes(col)) {
          const sSnap = await getDocs(collection(db, col, targetId, "seasons"));
          for (const season of sSnap.docs) {
            const eSnap = await getDocs(collection(db, col, targetId, "seasons", season.id, "episodes"));
            eSnap.forEach(ep => batch.update(ep.ref, { published: status }));
          }
        }
        await batch.commit();
        break;
      }
    }
    if (found) bot.sendMessage(chatId, `✅ <b>Status updated:</b> Cascade Published = ${status}`, { parse_mode: 'HTML' });
    else bot.sendMessage(chatId, "❌ Series/Movie not found.");
  } catch (err) { logError("cascadePublish", err); bot.sendMessage(chatId, "❌ Publish execution failed."); }
}

// ==========================================
// REMAINING ADMIN COMMANDS
// ==========================================
async function executeEdit(chatId, text) {
  const args = text.split(' ');
  if (args.length < 4) return bot.sendMessage(chatId, "❌ Usage: /edit [collection] [id] [field] [value]");
  const col = args[1], id = args[2], field = args[3], value = args.slice(4).join(' ');
  try {
    await updateDoc(doc(db, col, id), { [field]: value, updatedAt: Date.now() });
    bot.sendMessage(chatId, `✅ <b>Updated</b> <code>${col}/${id}</code>:\n${field} = ${value}`, { parse_mode: 'HTML' });
  } catch (err) { logError("executeEdit", err); bot.sendMessage(chatId, "❌ Edit failed. Check ID and Collection."); }
}

async function renderStats(chatId) {
  try {
    bot.sendMessage(chatId, "⏳ <i>Aggregating data...</i>", { parse_mode: 'HTML' });
    const uSnap = await getDocs(collection(db, "users"));
    const aSnap = await getDocs(collection(db, "anime"));
    const mSnap = await getDocs(collection(db, "movies"));
    const wSnap = await getDocs(collection(db, "webseries"));
    const vSnap = await getDocs(collection(db, "all_videos"));

    const msg = `📊 <b>Production Stats</b>\n\n` +
      `👥 Total Users: ${uSnap.size}\n` +
      `🎌 Anime Series: ${aSnap.size}\n` +
      `📺 Web Series: ${wSnap.size}\n` +
      `🎬 Standalone Movies: ${mSnap.size}\n` +
      `📦 Total Unique Videos: ${vSnap.size}`;

    bot.sendMessage(chatId, msg, { parse_mode: 'HTML' });
  } catch (err) { logError("renderStats", err); }
}

async function listUsers(chatId) {
  try {
    const snap = await getDocs(collection(db, "users"));
    let txt = "👥 <b>Recent Users:</b>\n\n";
    let count = 0;
    snap.forEach(d => { if (count++ < 20) txt += `• ID: <code>${d.id}</code> (Plan: ${esc(d.data().plan)})\n`; });
    bot.sendMessage(chatId, txt, { parse_mode: 'HTML' });
  } catch(err) { logError("listUsers", err); }
}

async function listContent(chatId) {
  try {
    const snap = await getDocs(collection(db, "all_videos"));
    bot.sendMessage(chatId, `📋 <b>Total Video Files Indexed:</b> ${snap.size}\n<i>Detailed listings must be accessed via Firebase Console to prevent buffer overflows.</i>`, { parse_mode: 'HTML' });
  } catch(err) { logError("listContent", err); }
}

async function createGift(chatId, text) {
  const args = text.replace('/creategift ', '').split("|");
  if (args.length < 3) return bot.sendMessage(chatId, "❌ Format:\n/creategift CODE|PLAN|DAYS");
  try {
    const code = args[0].trim().toUpperCase(), plan = args[1].trim(), days = Number(args[2]);
    await setDoc(doc(db, "giftcodes", code), { code, plan, days, used: false, createdAt: Date.now() });
    bot.sendMessage(chatId, `✅ <b>Gift Code Created</b>\n🎁 Code: <code>${code}</code>\n💎 Plan: ₹${plan}\n📅 Days: ${days}`, { parse_mode: 'HTML' });
  } catch (err) { logError("createGift", err); bot.sendMessage(chatId, "❌ Failed to create code."); }
}

async function deleteGift(chatId, text) {
  const code = text.replace('/deletegift ', '').trim().toUpperCase();
  try {
    await deleteDoc(doc(db, "giftcodes", code));
    bot.sendMessage(chatId, `✅ <b>Gift Code Deleted:</b> <code>${code}</code>`, { parse_mode: 'HTML' });
  } catch (err) { logError("deleteGift", err); bot.sendMessage(chatId, "❌ Failed to delete code."); }
}

async function setPlan(chatId, text) {
  const args = text.split(' ');
  if (args.length < 3) return bot.sendMessage(chatId, "❌ Usage: /setplan [userID] [planAmount]");
  try {
    const uId = args[1], planAmount = args[2];
    await ensureUser(uId);
    await setDoc(doc(db, "users", String(uId)), { plan: planAmount, balance: 0, expiry: Date.now() + (30 * 24 * 60 * 60 * 1000) }, { merge: true });
    bot.sendMessage(uId, `✅ <b>Subscription Updated</b>\n💎 Plan: ₹${planAmount}/month\n📅 Validity: 30 Days.`, { parse_mode: 'HTML' });
    bot.sendMessage(chatId, "✅ User updated successfully.");
  } catch (err) { logError("setPlan", err); bot.sendMessage(chatId, "❌ Failed to set plan."); }
}

// ==========================================
// CORE USER UTILITIES & ACCOUNT LOGIC
// ==========================================
async function processGiftCode(chatId, text) {
  try {
    const code = text.trim().toUpperCase();
    const ref = doc(db, "giftcodes", code);
    const snap = await getDoc(ref);
    if (!snap.exists()) return bot.sendMessage(chatId, "❌ Invalid gift code.");
    const gift = snap.data();
    if (gift.used) return bot.sendMessage(chatId, "❌ Code already used.");

    const user = await getUser(chatId);
    let activeVal = user.plan === "100" ? 100 : user.plan === "50" ? 50 : user.plan === "20" ? 20 : 0;
    const totalBal = activeVal + Number(user.balance || 0) + Number(gift.plan || 0);
    
    const pInfo = totalBal >= 100 ? {p:"100", d:30} : totalBal >= 50 ? {p:"50", d:30} : totalBal >= 20 ? {p:"20", d:30} : {p:"Free", d:0};
    const leftover = totalBal - (pInfo.p !== "Free" ? Number(pInfo.p) : 0);
    const totalDays = pInfo.d + Math.floor(leftover / 2);

    await setDoc(doc(db, "users", String(chatId)), { ...user, balance: leftover, plan: pInfo.p, expiry: Date.now() + (totalDays * 24 * 60 * 60 * 1000) }, { merge: true });
    await setDoc(ref, { ...gift, used: true, usedBy: chatId, usedAt: Date.now() }, { merge: true });
    bot.sendMessage(chatId, `✅ <b>Success!</b>\nPlan: ₹${pInfo.p}\nValidity: ${totalDays} Days.`, { parse_mode: 'HTML' });
  } catch(err) { logError("processGiftCode", err); }
}

async function ensureUser(chatId) {
  try {
    const ref = doc(db, "users", String(chatId));
    const snap = await getDoc(ref);
    if (!snap.exists()) await setDoc(ref, { plan: "Free", balance: 0, expiry: null });
  } catch(err) { logError("ensureUser", err); }
}

async function getUser(chatId) {
  try { return (await getDoc(doc(db, "users", String(chatId)))).data(); } 
  catch(err) { logError("getUser", err); return { plan: 'Free', balance: 0 }; }
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
  // Acknowledge immediately to prevent Webhook Flooding/Timeouts
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
