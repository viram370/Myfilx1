// =========================
// REQUIRED PACKAGES
// =========================

const TelegramBot = require('node-telegram-bot-api');
const express = require('express');
const { initializeApp } = require("firebase/app");
const {
  getFirestore,
  doc,
  setDoc,
  getDoc,
  collection,
  addDoc,  
  getDocs,
  deleteDoc,
  updateDoc,
  query,
  where,
  orderBy,
  limit
} = require("firebase/firestore");

const app = express();
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

// State-tracking variables
const adminFlow = {};
const redeemMode = {};
const token = process.env.BOT_TOKEN;
const bot = new TelegramBot(token, { webHook: true });

const PORT = process.env.PORT || 10000;
const RENDER_URL = process.env.RENDER_EXTERNAL_URL;
bot.setWebHook(`${RENDER_URL}/bot${token}`);

app.use(express.json());

// =========================
// ADMIN POLICY & GUARDS
// =========================

const ADMIN_IDS = [6097315530];

function isAdmin(chatId) {
  return ADMIN_IDS.includes(Number(chatId));
}

function verifyAdmin(msg) {
  if (!isAdmin(msg.chat.id)) {
    bot.sendMessage(msg.chat.id, "❌ You are not authorized to use this command.");
    return false;
  }
  return true;
}

// Helper utility to sanitize text for Firestore document IDs
function sanitizeId(text) {
  return text.trim().toLowerCase().replace(/[^a-z0-9_-]/g, '_');
}

// =========================
// DUPLICATE PROTECTION UTILITY
// =========================
async function isDuplicateVideo(fileUniqueId) {
  if (!fileUniqueId) return false;
  
  const collectionsToCheck = ['anime', 'movies', 'webseries', 'cartoons'];
  
  for (const colName of collectionsToCheck) {
    if (colName === 'anime') {
      const animeSnap = await getDocs(collection(db, 'anime'));
      for (const animeDoc of animeSnap.docs) {
        // Check episodes inside seasons
        const seasonsSnap = await getDocs(collection(db, 'anime', animeDoc.id, 'seasons'));
        for (const seasonDoc of seasonsSnap.docs) {
          const epSnap = await getDocs(collection(db, 'anime', animeDoc.id, 'seasons', seasonDoc.id, 'episodes'));
          const found = epSnap.docs.some(d => d.data().file_unique_id === fileUniqueId);
          if (found) return true;
        }
        // Check standalone anime movies
        const movieSnap = await getDocs(collection(db, 'anime', animeDoc.id, 'movies'));
        const foundMovie = movieSnap.docs.some(d => d.data().file_unique_id === fileUniqueId);
        if (foundMovie) return true;
      }
    } else {
      const snap = await getDocs(collection(db, colName));
      const found = snap.docs.some(d => d.data().file_unique_id === fileUniqueId);
      if (found) return true;
    }
  }
  return false;
}

// Helper to extract the next logical index safely from any serial list
async function getNextEpisodeNumber(ref) {
  const snap = await getDocs(ref);
  let maxEp = 0;
  snap.forEach(doc => {
    const data = doc.data();
    if (data && typeof data.episode === 'number') {
      if (data.episode > maxEp) maxEp = data.episode;
    }
  });
  return maxEp + 1;
}

// =========================
// ADMIN ADVANCED HELP
// =========================
function sendAdminHelp(chatId) {
  bot.sendMessage(chatId, 
`🎬 *MYFLIX ADMIN*

📤 *Upload*
Send any video directly to the chat to begin the setup flow.

✏ *Edit*
/edit ID

🗑 *Delete*
/delete ID

📢 *Publish*
/publish ID

🙈 *Unpublish*
/unpublish ID

📊 *Statistics*
/stats

👥 *Users*
/users

📋 *Recent Uploads*
/list`, { parse_mode: 'Markdown' });
}

bot.onText(/\/help/, (msg) => {
  if (verifyAdmin(msg)) {
    sendAdminHelp(msg.chat.id);
  }
});

// =========================
// ADMIN GIFT CODE COMMANDS
// =========================
bot.onText(/\/creategift (.+)/, async (msg, match) => {
  if (!verifyAdmin(msg)) return;

  const args = match[1].split("|");
  if (args.length < 3) {
    return bot.sendMessage(msg.chat.id, "❌ Format:\n\n/creategift CODE|PLAN|DAYS");
  }

  const code = args[0].trim().toUpperCase();
  const plan = args[1].trim();
  const days = Number(args[2]);

  await setDoc(doc(db, "giftcodes", code), {
    code: code,
    plan: plan,
    days: days,
    used: false,
    createdAt: Date.now()
  }, { merge: true });

  bot.sendMessage(msg.chat.id,
`✅ Gift code created

🎁 Code:
${code}

💎 Plan:
₹${plan}

📅 Days:
${days}`);
});

bot.onText(/\/deletegift (.+)/, async (msg, match) => {
  if (!verifyAdmin(msg)) return;

  const code = match[1].trim().toUpperCase();
  const codeRef = doc(db, "giftcodes", code);
  const snap = await getDoc(codeRef);

  if (!snap.exists()) {
    return bot.sendMessage(msg.chat.id, "❌ Gift code not found");
  }

  await deleteDoc(codeRef);
  bot.sendMessage(msg.chat.id, `✅ Gift code deleted\n\n🎁 ${code}`);
});

// =========================
// USER ACCESS SUBSYSTEM
// =========================
async function ensureUser(chatId) {
  const ref = doc(db, "users", String(chatId));
  const snap = await getDoc(ref);
  if (!snap.exists()) {
    await setDoc(ref, { plan: "Free", balance: 0, expiry: null });
  }
}

async function getUser(chatId) {
  const ref = doc(db, "users", String(chatId));
  const snap = await getDoc(ref);
  return snap.data();
}

async function checkExpiry(chatId){
  const ref = doc(db, "users", String(chatId));
  const snap = await getDoc(ref);
  if(!snap.exists()) return;

  const user = snap.data();
  if(user.expiry && Date.now() > user.expiry){
    const balance = Number(user.balance || 0);
    const result = getPlanFromBalance(balance);
    const leftover = balance - result.used;
    const extraDays = Math.floor(leftover / 2);

    if(result.plan === "Free"){
      await setDoc(ref, {
        ...user,
        plan: "Free",
        expiry: null,
        balance: 0
      }, { merge: true });
      return;
    }

    const totalDays = result.days + extraDays;
    const expiry = Date.now() + (totalDays * 24 * 60 * 60 * 1000);

    await setDoc(ref, {
      ...user,
      plan: result.plan,
      expiry: expiry,
      balance: leftover
    }, { merge: true });
  }
}

function getPlanFromBalance(balance){
  if(balance >= 100) return { plan: "100", used: 100, days: 30 };
  if(balance >= 50)  return { plan: "50", used: 50, days: 30 };
  if(balance >= 20)  return { plan: "20", used: 20, days: 30 };
  return { plan: "Free", used: 0, days: 0 };
}

function canUseAnime(plan) { return ["20", "50", "100"].includes(plan); }
function canUseMovie(plan) { return plan === "100"; }
function canUseWebseries(plan) { return ["50", "100"].includes(plan); }

function getPlanBenefits(plan) {
  if (plan === "20") return `🍿 ₹20 Anime Basic\n\n✅ Anime Access\n✅ Hindi Dubbed\n✅ 480p Quality\n✅ Downloads`;
  if (plan === "50") return `🎬 ₹50 Anime + WebSeries\n\n✅ Anime Access\n✅ WebSeries Access\n✅ 720p HD\n✅ Hindi + Some English`;
  if (plan === "100") return `🔥 ₹100 Premium HD\n\n✅ Anime Access\n✅ Movies Access\n✅ WebSeries Access\n✅ 720p HD\n✅ Hindi + English`;
  return `Free Plan\n\n❌ No Premium Access`;
}

// =========================
// INTERACTIVE UPLOAD FLOW ENGINE
// =========================

bot.on('video', async (msg) => {
  if (!isAdmin(msg.chat.id)) return;
  await processMediaUpload(msg, msg.video);
});

bot.on('document', async (msg) => {
  if (!isAdmin(msg.chat.id)) return;
  if (msg.document && msg.document.mime_type && msg.document.mime_type.startsWith('video/')) {
    await processMediaUpload(msg, msg.document);
  }
});

async function processMediaUpload(msg, mediaObj) {
  const chatId = msg.chat.id;
  
  // Duplicate check
  const duplicate = await isDuplicateVideo(mediaObj.file_unique_id);
  if (duplicate) {
    return bot.sendMessage(chatId, "❌ This video already exists.");
  }

  // If we are already running an active content collection flow, add video straight to its stack
  if (adminFlow[chatId] && (adminFlow[chatId].step === 'await_episodes')) {
    adminFlow[chatId].videos.push({
      file_id: mediaObj.file_id,
      file_unique_id: mediaObj.file_unique_id
    });
    return bot.sendMessage(chatId, `⏳ Received episode video (${adminFlow[chatId].videos.length} buffered). Continue uploading or type /finish.`);
  }

  // Start fresh interactive categorization tree
  adminFlow[chatId] = {
    step: 'choose_type',
    initialVideo: {
      file_id: mediaObj.file_id,
      file_unique_id: mediaObj.file_unique_id
    },
    videos: [],
    meta: {}
  };

  bot.sendMessage(chatId, "🎌 *Choose Content Type* 🎌", {
    parse_mode: "Markdown",
    reply_markup: {
      inline_keyboard: [
        [{ text: "🎌 Anime", callback_data: "type_anime" }],
        [{ text: "🎥 Anime Movie", callback_data: "type_animemovie" }],
        [{ text: "🎬 Movie", callback_data: "type_movie" }],
        [{ text: "📺 Web Series", callback_data: "type_webseries" }],
        [{ text: "🎨 Cartoon", callback_data: "type_cartoon" }]
      ]
    }
  });
}

// Process structural choice selections
bot.on('callback_query', async (query) => {
  const chatId = query.message.chat.id;
  const action = query.data;

  // Global cancel action
  if (action === "flow_cancel") {
    delete adminFlow[chatId];
    bot.editMessageText("❌ Action cancelled.", { chat_id: chatId, message_id: query.message.message_id });
    return bot.answerCallbackQuery(query.id);
  }

  if (!adminFlow[chatId]) {
    return bot.answerCallbackQuery(query.id, { text: "No active processing session found." });
  }

  const currentFlow = adminFlow[chatId];

  if (action.startsWith("type_")) {
    currentFlow.contentType = action.replace("type_", "");
    bot.answerCallbackQuery(query.id);
    
    if (currentFlow.contentType === 'anime') {
      currentFlow.step = 'await_anime_name';
      return bot.sendMessage(chatId, "📝 Enter **Anime Name**:");
    } else if (currentFlow.contentType === 'animemovie') {
      currentFlow.step = 'await_anime_group';
      return bot.sendMessage(chatId, "📝 Enter parent **Anime Series Name** (e.g. Jujutsu Kaisen):");
    } else if (currentFlow.contentType === 'movie') {
      currentFlow.step = 'await_movie_name';
      return bot.sendMessage(chatId, "📝 Enter **Movie Name**:");
    } else if (currentFlow.contentType === 'webseries') {
      currentFlow.step = 'await_series_name';
      return bot.sendMessage(chatId, "📝 Enter **Web Series Name**:");
    } else if (currentFlow.contentType === 'cartoon') {
      currentFlow.step = 'await_cartoon_name';
      return bot.sendMessage(chatId, "📝 Enter **Cartoon Name**:");
    }
  }

  if (action.startsWith("seasons_")) {
    const multiSeasons = action.replace("seasons_", "");
    bot.answerCallbackQuery(query.id);
    if (multiSeasons === 'yes') {
      currentFlow.step = 'await_season_number';
      return bot.sendMessage(chatId, "🔢 Enter **Season Number**:");
    } else {
      currentFlow.meta.season = "season 1";
      currentFlow.step = 'await_language';
      return bot.sendMessage(chatId, "🌐 Enter **Language**:");
    }
  }

  // Publish / Draft Decision Nodes
  if (action.startsWith("save_")) {
    const choice = action.replace("save_", "");
    bot.answerCallbackQuery(query.id);
    const isPublished = (choice === 'publish');

    try {
      await commitFlowDataToFirestore(chatId, isPublished);
      bot.sendMessage(chatId, isPublished ? "🚀 Content successfully published live!" : "📝 Saved successfully as a Draft.");
    } catch(err) {
      bot.sendMessage(chatId, `❌ Error while saving: ${err.message}`);
    }
    delete adminFlow[chatId];
    return;
  }

  // Legacy dynamic payments inline query mapping
  if (["pay20", "pay50", "pay100"].includes(action)) {
    bot.answerCallbackQuery(query.id);
    let cap = "", photoId = "";
    if (action === "pay20") {
      photoId = "AgACAgUAAxkBAAICJWn_BX9bvt0HOVooXrS_Y7VwpOngAAIQEGsbf1P4V02Yna5OBauhAQADAgADeAADOwQ";
      cap = `🍿 ₹20 Anime Basic Plan\n\n━━━━━━━━━━━━━━\n💰 Payment Details\n━━━━━━━━━━━━━━\n👤 Name:\nGarming hack king\n\n💳 UPI ID:\nviramdevraj20@fam\n\n━━━━━━━━━━━━━━\n📌 Plan Benefits\n━━━━━━━━━━━━━━\n✅ Anime Access\n✅ Hindi Dubbed\n✅ 480p Quality\n✅ Download Support\n✅ 30 Days Validity\n\n📩 Send payment screenshot to: @MyflixO`;
    } else if (action === "pay50") {
      photoId = "AgACAgUAAxkBAAICJGn_BUqfwwN0FHe7EzRRfhGHb8n2AAIPEGsbf1P4V4ijMEK46jkNAQADAgADeAADOwQ";
      cap = `🎬 ₹50 Anime + WebSeries Plan\n\n━━━━━━━━━━━━━━\n💰 Payment Details\n━━━━━━━━━━━━━━\n👤 Name:\nGarming hack king\n\n💳 UPI ID:\nviramdevraj20@fam\n\n━━━━━━━━━━━━━━\n📌 Plan Benefits\n━━━━━━━━━━━━━━\n✅ Anime Access\n✅ WebSeries Access\n✅ 720p HD Quality\n✅ Hindi + Some English\n✅ Download Support\n✅ 30 Days Validity\n\n📩 Send payment screenshot to: @MyflixO`;
    } else if (action === "pay100") {
      photoId = "AgACAgUAAxkBAAICI2n_BMY4S8rRS53FvZ9B71iSeybAAAIOEGsbf1P4V3jkTRZMtrsZAQADAgADeAADOwQ";
      cap = `🔥 ₹100 Premium HD Plan\n\n━━━━━━━━━━━━━━\n💰 Payment Details\n━━━━━━━━━━━━━━\n👤 Name:\nGarming hack king\n\n💳 UPI ID:\nviramdevraj20@fam\n\n━━━━━━━━━━━━━━\n📌 Plan Benefits\n━━━━━━━━━━━━━━\n✅ Anime Access\n✅ Movies Access\n✅ WebSeries Access\n✅ 720p HD Streaming\n✅ Hindi + English\n✅ Download Support\n✅ 30 Days Validity\n\n📩 Send payment screenshot to: @MyflixO`;
    }
    bot.sendPhoto(chatId, photoId, { caption: cap });
  }
});

// =========================
// TEXTUAL FLOW DRIVE CONTROLLER
// =========================
bot.on('message', async (msg) => {
  const chatId = msg.chat.id;
  const text = msg.text ? msg.text.trim() : "";

  if (!text) return;
  
  // Guard admin state configuration entries from leaking to standard text checks
  if (isAdmin(chatId) && adminFlow[chatId]) {
    if (text.startsWith('/')) {
      if (text === '/finish') {
        await handleFlowFinishTrigger(chatId);
        return;
      }
      if (text === '/cancel') {
        delete adminFlow[chatId];
        bot.sendMessage(chatId, "❌ Setup workflow terminated.");
        return;
      }
    } else {
      await handleStatefulTextEntry(chatId, text);
      return;
    }
  }

  // Basic authorization routing check for generic text message matching fallback commands
  if (text.startsWith('/')) {
    const cmd = text.split(' ')[0];
    if (['/edit', '/delete', '/publish', '/unpublish', '/stats', '/users', '/list', '/setplan'].includes(cmd)) {
      if (!verifyAdmin(msg)) return;
    }
  }

  // Standard application routing configuration
  await ensureUser(chatId);
  await checkExpiry(chatId);

  // User interactive triggers
  if (text === "🔍 Search") {
    return bot.sendMessage(chatId, `🔍 Search Commands\n\n━━━━━━━━━━━━━━\n🎌 Anime\n━━━━━━━━━━━━━━\n/anime anime-name season 1\n\nExample:\n• /anime naruto season 1\n• /anime naruto season 1 english\n\n━━━━━━━━━━━━━━\n🎬 Movies\n━━━━━━━━━━━━━━\n/movie movie-name\n\n━━━━━━━━━━━━━━\n📺 WebSeries\n━━━━━━━━━━━━━━\n/webseries series-name`);
  }
  if (text === "👤 Account") {
    return bot.sendMessage(chatId, `👤 MyFlix Account Center`, {
      reply_markup: {
        keyboard: [["💎 Plans","💳 Payment"], ["👤 Account Info","🎁 Gift Code"], ["🔙 Back"]],
        resize_keyboard: true
      }
    });
  }
  if (text === "👤 Account Info") {
    const user = await getUser(chatId);
    return bot.sendMessage(chatId, `👤 MyFlix Premium Account\n\n🆔 User ID:\n${chatId}\n\n💎 Current Plan:\n${user.plan}\n\n💰 Wallet Balance:\n₹${user.balance}\n\n📅 Plan Expiry:\n${user.expiry ? new Date(user.expiry).toLocaleDateString() : "No Active Plan"}\n\n━━━━━━━━━━━━━━\n🎁 Plan Benefits\n━━━━━━━━━━━━━━\n${getPlanBenefits(user.plan)}\n\n👉 @MyflixO`);
  }
  if (text === "💎 Plans") {
    return bot.sendMessage(chatId, `💎 MyFlix Premium Plans\n\n🍿 ₹20 / Month\n• Anime Only (Hindi, 480p)\n\n🎬 ₹50 / Month\n• Anime + WebSeries (Hindi/Eng, 720p HD)\n\n🔥 ₹100 / Month\n• Full Premium HD Access (All Content)`);
  }
  if (text === "💳 Payment") {
    return bot.sendMessage(chatId, `💳 MyFlix Payment Center\n\n👤 Name:\nGarming hack king\n\n💰 UPI:\nviramdevraj20@fam\n\nSelect your plan below 👇`, {
      reply_markup: {
        inline_keyboard: [
          [{ text: "🍿 ₹20 Plan", callback_data: "pay20" }],
          [{ text: "🎬 ₹50 Plan", callback_data: "pay50" }],
          [{ text: "🔥 ₹100 Plan", callback_data: "pay100" }]
        ]
      }
    });
  }
  if (text === "📝 Waitlist") {
    return bot.sendMessage(chatId, `📝 MyFlix Request Waitlist\n\nCan't find a title? Send specifications to @MyflixO.\nFormat:\nName, Language, Season, Quality.`);
  }
  if (text === "🆘 Support") {
    return bot.sendMessage(chatId, `🆘 MyFlix Premium Support Center\n\nContact: @MyflixO\nResponse delays might happen during high queue periods.`);
  }
  if (text === "📜 Terms & Privacy") {
    return bot.sendMessage(chatId, `📜 Terms & Privacy Guidelines\nSub validity lasts 30 days. Shared accounts are strictly prohibited.`);
  }
  if (text === "🔙 Back") {
    return bot.sendMessage(chatId, `🎬 Main Menu`, {
      reply_markup: {
        keyboard: [["🔍 Search", "👤 Account"], ["📝 Waitlist", "🆘 Support"], ["📜 Terms & Privacy"]],
        resize_keyboard: true
      }
    });
  }

  if (text === "🎁 Gift Code") {
    redeemMode[chatId] = true;
    return bot.sendMessage(chatId, `🎁 Type your gift code below:`);
  }

  if (redeemMode[chatId]) {
    delete redeemMode[chatId];
    const code = text.trim().toUpperCase();
    const codeRef = doc(db, "giftcodes", code);
    const codeSnap = await getDoc(codeRef);

    if (!codeSnap.exists()) return bot.sendMessage(chatId, "❌ Invalid gift code.");
    const giftData = codeSnap.data();
    if (giftData.used) return bot.sendMessage(chatId, "❌ Gift code already used.");

    const user = await getUser(chatId);
    let activePlanValue = 0;
    if (user.plan === "20") activePlanValue = 20;
    if (user.plan === "50") activePlanValue = 50;
    if (user.plan === "100") activePlanValue = 100;

    const totalBalance = activePlanValue + Number(user.balance || 0) + Number(giftData.plan || 0);
    const result = getPlanFromBalance(totalBalance);
    const leftover = totalBalance - result.used;
    const totalDays = result.days + Math.floor(leftover / 2);
    const expiry = Date.now() + (totalDays * 24 * 60 * 60 * 1000);

    await setDoc(doc(db, "users", String(chatId)), { ...user, balance: leftover, plan: result.plan, expiry: expiry }, { merge: true });
    await setDoc(codeRef, { ...giftData, used: true, usedBy: chatId, usedAt: Date.now() }, { merge: true });

    return bot.sendMessage(chatId, `✅ Gift Code Redeemed!\n\nPlan: ₹${result.plan}\nValidity: ${totalDays} Days.`);
  }
});

// =========================
// INGESTION STATE FLOW ENGINE SWITCH
// =========================
async function handleStatefulTextEntry(chatId, text) {
  const current = adminFlow[chatId];

  switch(current.step) {
    // Anime sub-flows
    case 'await_anime_name':
      current.meta.name = text;
      current.step = 'await_anime_seasons_option';
      bot.sendMessage(chatId, "Does this anime have multiple seasons?", {
        reply_markup: {
          inline_keyboard: [
            [{ text: "✅ YES", callback_data: "seasons_yes" }, { text: "❌ NO", callback_data: "seasons_no" }]
          ]
        }
      });
      break;
    case 'await_season_number':
      current.meta.season = "season " + text;
      current.step = 'await_language';
      bot.sendMessage(chatId, "🌐 Enter **Language**:");
      break;
    case 'await_language':
      current.meta.language = text.toLowerCase();
      // Push first structural video onto active list tracking array buffer
      current.videos.push(current.initialVideo);
      current.step = 'await_episodes';
      bot.sendMessage(chatId, "📥 First episode buffered successfully! Send additional video files continuously now. When completely finished, type /finish.");
      break;

    // Anime Movie Flow
    case 'await_anime_group':
      current.meta.animeGroup = text;
      current.step = 'await_animemovie_title';
      bot.sendMessage(chatId, "🎬 Enter **Movie Title**:");
      break;
    case 'await_animemovie_title':
      current.meta.movieTitle = text;
      current.step = 'await_movie_lang';
      bot.sendMessage(chatId, "🌐 Enter **Language**:");
      break;

    // Core Flat Movie Configuration Flow
    case 'await_movie_name':
      current.meta.movieTitle = text;
      current.step = 'await_movie_lang';
      bot.sendMessage(chatId, "🌐 Enter **Language**:");
      break;
    case 'await_movie_lang':
      current.meta.language = text;
      current.step = 'await_movie_quality';
      bot.sendMessage(chatId, "📀 Enter **Quality** (e.g. 1080p / 720p):");
      break;
    case 'await_movie_quality':
      current.meta.quality = text;
      current.step = 'await_metadata_poster';
      bot.sendMessage(chatId, "🖼 Enter **Poster Image URL**:");
      break;

    // Web Series Flows
    case 'await_series_name':
      current.meta.name = text;
      current.step = 'await_series_season';
      bot.sendMessage(chatId, "🔢 Enter **Season Number**:");
      break;
    case 'await_series_season':
      current.meta.season = "season " + text;
      current.step = 'await_series_lang';
      bot.sendMessage(chatId, "🌐 Enter **Language**:");
      break;
    case 'await_series_lang':
      current.meta.language = text.toLowerCase();
      current.videos.push(current.initialVideo);
      current.step = 'await_episodes';
      bot.sendMessage(chatId, "📥 First episode buffered successfully! Upload remaining episodes sequentially, then type /finish.");
      break;

    // Cartoon Ingestion Flow
    case 'await_cartoon_name':
      current.meta.name = text;
      current.step = 'await_cartoon_lang';
      bot.sendMessage(chatId, "🌐 Enter **Language**:");
      break;
    case 'await_cartoon_lang':
      current.meta.language = text;
      current.step = 'await_metadata_poster';
      bot.sendMessage(chatId, "🖼 Enter **Poster Image URL**:");
      break;

    // Unified Metadata Appending Pipeline Step Nodes
    case 'await_metadata_poster':
      current.meta.poster = text;
      current.step = 'await_metadata_banner';
      bot.sendMessage(chatId, "📐 Enter **Banner Image URL**:");
      break;
    case 'await_metadata_banner':
      current.meta.banner = text;
      current.step = 'await_metadata_desc';
      bot.sendMessage(chatId, "📝 Enter **Description**:");
      break;
    case 'await_metadata_desc':
      current.meta.description = text;
      current.step = 'await_metadata_genres';
      bot.sendMessage(chatId, "🎨 Enter **Genres** (comma-separated):");
      break;
    case 'await_metadata_genres':
      current.meta.genres = text.split(",").map(g => g.trim());
      current.step = 'await_metadata_year';
      bot.sendMessage(chatId, "📅 Enter **Release Year**:");
      break;
    case 'await_metadata_year':
      current.meta.year = text;
      current.step = 'await_metadata_rating';
      bot.sendMessage(chatId, "⭐ Enter **Rating** (e.g. 8.5):");
      break;
    case 'await_metadata_rating':
      current.meta.rating = text;
      
      // Routing back execution to terminal checks based on content classifications
      if (['movie', 'cartoon', 'animemovie'].includes(current.contentType)) {
        promptPublishDecision(chatId);
      }
      break;
  }
}

// Processing terminal episodes arrays collections
async function handleFlowFinishTrigger(chatId) {
  const current = adminFlow[chatId];
  if (!current || !['await_episodes'].includes(current.step)) return;

  current.step = 'await_metadata_poster';
  bot.sendMessage(chatId, "✅ Episodes buffered successfully. Continuing metadata processing...\n\n🖼 Enter **Poster Image URL**:");
}

function promptPublishDecision(chatId) {
  bot.sendMessage(chatId, "❓ *Publish content live now?*", {
    parse_mode: "Markdown",
    reply_markup: {
      inline_keyboard: [
        [{ text: "✅ Publish Live", callback_data: "save_publish" }],
        [{ text: "📝 Save as Draft", callback_data: "save_draft" }]
      ]
    }
  });
}

// =========================
// FIRESTORE COMMIT EXECUTION WRAPPER
// =========================
async function commitFlowDataToFirestore(chatId, isPublished) {
  const current = adminFlow[chatId];
  const m = current.meta;
  
  const basePayload = {
    poster: m.poster || "",
    banner: m.banner || "",
    description: m.description || "",
    genres: m.genres || [],
    language: m.language || "",
    quality: m.quality || "HD",
    year: m.year || "",
    rating: m.rating || "",
    published: isPublished,
    createdAt: Date.now()
  };

  if (current.contentType === 'anime') {
    const animeId = sanitizeId(m.name);
    const animeRef = doc(db, "anime", animeId);
    
    // Maintain top-level structural integrity mapping
    await setDoc(animeRef, { name: m.name }, { merge: true });
    
    const epRef = collection(db, "anime", animeId, "seasons", m.season, "episodes");
    let nextEp = await getNextEpisodeNumber(epRef);

    for (const vid of current.videos) {
      await setDoc(doc(db, "anime", animeId, "seasons", m.season, "episodes", "ep" + nextEp), {
        episode: nextEp,
        file_id: vid.file_id,
        file_unique_id: vid.file_unique_id,
        ...basePayload
      });
      nextEp++;
    }
  } 
  else if (current.contentType === 'animemovie') {
    const parentAnimeId = sanitizeId(m.animeGroup);
    await setDoc(doc(db, "anime", parentAnimeId), { name: m.animeGroup }, { merge: true });
    
    const movId = sanitizeId(m.movieTitle);
    await setDoc(doc(db, "anime", parentAnimeId, "movies", movId), {
      movieTitle: m.movieTitle,
      file_id: current.initialVideo.file_id,
      file_unique_id: current.initialVideo.file_unique_id,
      ...basePayload
    });
  } 
  else if (current.contentType === 'movie') {
    const docId = sanitizeId(m.movieTitle);
    await setDoc(doc(db, "movies", docId), {
      title: m.movieTitle,
      file_id: current.initialVideo.file_id,
      file_unique_id: current.initialVideo.file_unique_id,
      ...basePayload
    });
  } 
  else if (current.contentType === 'webseries') {
    const wsId = sanitizeId(m.name);
    await setDoc(doc(db, "webseries", wsId), { name: m.name }, { merge: true });

    const epRef = collection(db, "webseries", wsId, "seasons", m.season, "episodes");
    let nextEp = await getNextEpisodeNumber(epRef);

    for (const vid of current.videos) {
      await setDoc(doc(db, "webseries", wsId, "seasons", m.season, "episodes", "ep" + nextEp), {
        episode: nextEp,
        file_id: vid.file_id,
        file_unique_id: vid.file_unique_id,
        ...basePayload
      });
      nextEp++;
    }
  } 
  else if (current.contentType === 'cartoon') {
    const cartId = sanitizeId(m.name);
    await setDoc(doc(db, "cartoons", cartId), {
      title: m.name,
      file_id: current.initialVideo.file_id,
      file_unique_id: current.initialVideo.file_unique_id,
      ...basePayload
    });
  }
  
  // Track system operations metrics incrementations
  await setDoc(doc(db, "statistics", "uploads"), { total: Date.now() }, { merge: true });
}

// =========================
// RUNTIME USER CONTENT STREAMS
// =========================
bot.onText(/\/anime (.+)/i, async (msg, match) => {
  const chatId = msg.chat.id;
  await ensureUser(chatId);
  await checkExpiry(chatId);
  const user = await getUser(chatId);

  if (!canUseAnime(user.plan)) {
    return bot.sendMessage(chatId, `⚠️ Buy a premium plan to access Anime.`);
  }

  const input = match[1].toLowerCase();
  let language = "hindi";
  if (input.includes("english")) language = "english";

  const cleaned = input.replace("english", "").replace("hindi", "").trim();
  const parts = cleaned.split("season");

  if (parts.length < 2) {
    return bot.sendMessage(chatId, `❌ Correct Format:\n\n/anime anime-name season 1`);
  }

  const animeName = sanitizeId(parts[0]);
  const season = "season " + parts[1].replace(/\s+/g, " ").trim();

  const epRef = collection(db, "anime", animeName, "seasons", season, "episodes");
  const snap = await getDocs(epRef);

  if (snap.empty) return bot.sendMessage(chatId, `❌ Anime or requested season parameters not found.`);

  const episodes = [];
  snap.forEach(doc => {
    const data = doc.data();
    if (data.language === language) episodes.push(data);
  });

  if (episodes.length === 0) return bot.sendMessage(chatId, `❌ Selected language tracking tier not found.`);
  episodes.sort((a, b) => a.episode - b.episode);

  bot.sendMessage(chatId, `🎌 Sending ${parts[0]} ${season}\n🌐 Language: ${language}`);
  const sentMessages = [];

  for (const ep of episodes) {
    try {
      const sent = await bot.sendVideo(chatId, ep.file_id, {
        caption: `🎌 ${parts[0].toUpperCase()}\n📀 ${season.toUpperCase()}\n🎬 Episode ${ep.episode}`
      });
      sentMessages.push(sent.message_id);
      await new Promise(r => setTimeout(r, 2000));
    } catch (err) {
      console.error(err);
    }
  }

  // 30-minute self-destruct cleanup execution track logic loop
  setTimeout(async () => {
    for (const id of sentMessages) {
      try { await bot.deleteMessage(chatId, id); } catch(e){}
    }
  }, 30 * 60 * 1000);
});

bot.onText(/\/movie (.+)/, async (msg, match) => {
  const chatId = msg.chat.id;
  await ensureUser(chatId);
  const user = await getUser(chatId);
  if (!canUseMovie(user.plan)) return bot.sendMessage(chatId, `⚠️ ₹100 Premium Plan required for Movies.`);

  const queryId = sanitizeId(match[1]);
  const movieSnap = await getDoc(doc(db, "movies", queryId));

  if (!movieSnap.exists()) return bot.sendMessage(chatId, "❌ Movie not found.");
  const mov = movieSnap.data();
  if(!mov.published) return bot.sendMessage(chatId, "🔒 This content is currently an unpublished draft.");

  await bot.sendVideo(chatId, mov.file_id, { caption: `🎬 *${mov.title}*\n⭐ Rating: ${mov.rating}\n\n${mov.description}`, parse_mode: "Markdown" });
});

bot.onText(/\/webseries (.+)/, async (msg, match) => {
  const chatId = msg.chat.id;
  await ensureUser(chatId);
  const user = await getUser(chatId);
  if (!canUseWebseries(user.plan)) return bot.sendMessage(chatId, `⚠️ Upgrade to plan for WebSeries.`);

  bot.sendMessage(chatId, "📺 WebSeries engine running. Search operations processed through UI mini-app endpoints.");
});

// =========================
// ADDITIONAL ADMIN TELEMETRY / CONTROL CHANNELS
// =========================
bot.onText(/\/edit (.+)/, async (msg, match) => {
  if (!verifyAdmin(msg)) return;
  bot.sendMessage(msg.chat.id, "✏ Operational edits are directly manageable live via your integrated Firestore Database Console.");
});

bot.onText(/\/delete (.+)/, async (msg, match) => {
  if (!verifyAdmin(msg)) return;
  const targetId = sanitizeId(match[1]);
  // Flat cascading check delete pattern routines execution target
  await deleteDoc(doc(db, "movies", targetId));
  await deleteDoc(doc(db, "cartoons", targetId));
  bot.sendMessage(msg.chat.id, `🗑 Deletion execution commands sent completely for index ID key references matching: ${targetId}`);
});

bot.onText(/\/publish (.+)/, async (msg, match) => {
  if (!verifyAdmin(msg)) return;
  const targetId = sanitizeId(match[1]);
  await updateDoc(doc(db, "movies", targetId), { published: true }).catch(()=>{});
  bot.sendMessage(msg.chat.id, `🟢 Component entity status flag flipped to [Published] for token target: ${targetId}`);
});

bot.onText(/\/unpublish (.+)/, async (msg, match) => {
  if (!verifyAdmin(msg)) return;
  const targetId = sanitizeId(match[1]);
  await updateDoc(doc(db, "movies", targetId), { published: false }).catch(()=>{});
  bot.sendMessage(msg.chat.id, `🙈 Component status toggled to [Draft] for: ${targetId}`);
});

bot.onText(/\/stats/, async (msg) => {
  if (!verifyAdmin(msg)) return;
  const uSnap = await getDocs(collection(db, "users"));
  bot.sendMessage(msg.chat.id, `📊 *MYFLIX Engine Dashboard Metric Telemetry*\n\n👥 Registered Users Database Records: ${uSnap.size}`, { parse_mode: "Markdown" });
});

bot.onText(/\/users/, async (msg) => {
  if (!verifyAdmin(msg)) return;
  const snap = await getDocs(collection(db, "users"));
  let listText = "👥 *Recent Registered User Index Keys:*\n\n";
  snap.forEach(d => { listText += `• ID: \`${d.id}\` (Plan: ${d.data().plan})\n`; });
  bot.sendMessage(msg.chat.id, listText, { parse_mode: "Markdown" });
});

bot.onText(/\/list/, async (msg) => {
  if (!verifyAdmin(msg)) return;
  bot.sendMessage(msg.chat.id, "📋 Detailed structural collections listings are readable from the Firestore console.");
});

bot.onText(/\/setplan (.+) (.+)/, async (msg, match) => {
  if (!verifyAdmin(msg)) return;
  const userId = match[1];
  const plan = match[2];

  await ensureUser(userId);
  await setDoc(doc(db, "users", String(userId)), {
    plan: plan,
    balance: 0,
    expiry: Date.now() + (30 * 24 * 60 * 60 * 1000)
  }, { merge: true });

  bot.sendMessage(userId, `✅ Subscription Activated\n\n💎 Active Plan: ₹${plan}/month\n📅 Validity: 30 Days.`);
  bot.sendMessage(msg.chat.id, `✅ Subscription profile configured successfully.`);
});

// =========================
// STANDARD PLATFORM ROOT EXPORTS WEBHOOK ENTRYPORTS
// =========================
app.post(`/bot${token}`, (req, res) => {
  bot.processUpdate(req.body);
  res.sendStatus(200);
});

app.get('/', (req, res) => {
  res.send('MyFlix Engine Network Array Active');
});

app.listen(PORT, () => {
  console.log(`Server handling operational requests smoothly across active pipeline listening allocation block port: ${PORT}`);
});
