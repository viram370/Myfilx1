'use strict';

const crypto = require('crypto');
const fs = require('fs');
const path = require('path');
const { spawn } = require('child_process');
const TelegramBot = require('node-telegram-bot-api');
const { default: PQueue } = require('p-queue');
const { getDB, getAdmin } = require('./firebase');

// ============================================================================
// STAGE 1 — STARTUP VALIDATION & ENVIRONMENT CONFIGURATION
// ============================================================================
console.log(`[${new Date().toISOString()}] [STARTUP] Validating environment configurations...`);

const REQUIRED_ENV = ['TELEGRAM_BOT_TOKEN', 'STORAGE_CHANNEL_ID', 'ADMIN_USER_IDS'];
const missingEnv = REQUIRED_ENV.filter(key => !process.env[key]);
if (missingEnv.length > 0) {
  console.error(`[${new Date().toISOString()}] [STARTUP] CRITICAL ERROR: Missing vital environment keys: ${missingEnv.join(', ')}`);
  process.exit(1);
}

const token = process.env.TELEGRAM_BOT_TOKEN;
const BASE_URL = process.env.RENDER_EXTERNAL_URL || process.env.TELEGRAM_WEBHOOK_URL;
const STORAGE_CHANNEL_ID = Number(process.env.STORAGE_CHANNEL_ID);
const ADMIN_ID = Number(process.env.ADMIN_USER_IDS.split(',')[0].trim());

const COLLECTION_MAP = {
  'anime': 'anime',
  'webseries': 'webseries',
  'anime-movie': 'animeMovies',
  'movie': 'movies'
};

const UPLOAD_DIR = path.resolve(__dirname, '../uploads');
const CONVERT_DIR = path.resolve(__dirname, '../converted');

// Ensure working disk nodes are allocated cleanly at boot
if (!fs.existsSync(UPLOAD_DIR)) fs.mkdirSync(UPLOAD_DIR, { recursive: true });
if (!fs.existsSync(CONVERT_DIR)) fs.mkdirSync(CONVERT_DIR, { recursive: true });

// Background Infrastructure Concurrency Pools
const downloadQueue = new PQueue({ concurrency: 5 });
const compressionQueue = new PQueue({ concurrency: 2 });
const uploadQueue = new PQueue({ concurrency: 1 }); // Sequential Upload Order Preservation Guarantee

let db;
let botUsername = '';
const userState = new Map();
let queuePaused = false;
let activeJobsCount = 0;
let isShuttingDown = false;

// Unified Central Logging System
function diagLog(stage, message, meta = {}) {
  const ts = new Date().toISOString();
  const metaStr = Object.keys(meta).length ? ` | Meta: ${JSON.stringify(meta)}` : '';
  console.log(`[${ts}] [${stage.toUpperCase()}] ${message}${metaStr}`);
}

// ============================================================================
// STAGE 2 — INITIALIZATION & AUTOMATIC QUEUE RECOVERY
// ============================================================================
const bot = new TelegramBot(token, { webHook: true });

async function initBot() {
  try {
    db = getDB();
    diagLog('startup', 'Firestore reference captured successfully.');

    if (BASE_URL) {
      const webhookUrl = `${BASE_URL.replace(/\/+$/, '')}/webhook`;
      await bot.setWebHook(webhookUrl, { secret_token: process.env.WEBHOOK_SECRET, drop_pending_updates: true });
      diagLog('webhook', 'Webhook setup registration complete', { url: webhookUrl });
    }

    const me = await bot.getMe();
    botUsername = me?.username || 'Bot';
    diagLog('startup', `Identity verified under handle: @${botUsername}`);

    // Automatically recover interlocked files processing loops upon boot initialization
    await recoverPendingQueue();
  } catch (err) {
    diagLog('errors', 'Failed framework bootstrapping within initBot() context', { error: err.message });
  }
}

async function recoverPendingQueue() {
  diagLog('queue', 'Scanning Firestore records to identify orphan items requiring active recovery...');
  try {
    for (const colName of Object.values(COLLECTION_MAP)) {
      const snapshot = await db.collection(colName)
        .where('status', 'in', ['pending_processing', 'downloading', 'compressing', 'uploading'])
        .get();

      snapshot.forEach(doc => {
        if (isShuttingDown) return;
        const data = doc.data();
        diagLog('queue', `Orphan task matched! Re-routing to background pipeline worker`, { docId: doc.id, step: data.status });
        
        // Dynamic re-routing entry matrix based on legacy state markers
        if (data.status === 'pending_processing' || data.status === 'downloading') {
          downloadQueue.add(() => managedTaskRetry(() => downloadTaskWorker(doc.id, data, colName), 'download', doc.id, colName));
        } else if (data.status === 'compressing') {
          const ext = data.mimeType === 'video/x-matroska' ? 'mkv' : 'mp4';
          const downloadPath = path.join(UPLOAD_DIR, `${doc.id}.${ext}`);
          compressionQueue.add(() => managedTaskRetry(() => compressionTaskWorker(doc.id, data, colName, downloadPath), 'compression', doc.id, colName));
        } else if (data.status === 'uploading') {
          const targetPath = path.join(CONVERT_DIR, `${doc.id}.mp4`);
          uploadQueue.add(() => managedTaskRetry(() => uploadTaskWorker(doc.id, data, colName, targetPath), 'upload', doc.id, colName));
        }
      });
    }
  } catch (err) {
    diagLog('errors', 'Queue state synchronization scanning phase crash', { error: err.message });
  }
}

function isAdmin(chatId) {
  return Number(chatId) === ADMIN_ID;
}

// ============================================================================
// STAGE 3 — DISTRIBUTED WORKER ENGINES & COMPRESSION PIPELINES
// ============================================================================
async function runPipelineRouter(docId, data, collection) {
  if (queuePaused || isShuttingDown) {
    diagLog('queue', 'Task held or rejected. Execution pool constrained.', { docId, queuePaused, isShuttingDown });
    return;
  }
  downloadQueue.add(() => managedTaskRetry(() => downloadTaskWorker(docId, data, collection), 'download', docId, collection));
}

async function managedTaskRetry(taskFn, stageName, docId, collection, attempt = 1) {
  activeJobsCount++;
  try {
    const result = await taskFn();
    activeJobsCount = Math.max(0, activeJobsCount - 1);
    return result;
  } catch (err) {
    activeJobsCount = Math.max(0, activeJobsCount - 1);
    diagLog('retry', `Operational execution fault caught at stage [${stageName}]. Attempt ${attempt}/3`, { docId, error: err.message });
    
    if (attempt < 3 && !isShuttingDown) {
      return await managedTaskRetry(taskFn, stageName, docId, collection, attempt + 1);
    } else {
      diagLog('errors', `Max transactional fault thresholds breached on task stage [${stageName}]. Halting updates pipeline.`, { docId });
      queuePaused = true;
      await db.collection(collection).doc(docId).update({ status: 'failed_pipeline', lastError: err.message });
      
      bot.sendMessage(ADMIN_ID, `❌ <b>Pipeline Execution Halted!</b>\nStage [${stageName}] failed after max retries.\nDoc ID: <code>${docId}</code>\nError: ${err.message}\nResolve using /admin resume after inspection.`, { parse_mode: 'HTML' });
      throw err;
    }
  }
}

async function downloadTaskWorker(docId, data, collection) {
  diagLog('download', 'Beginning asset extraction sequence...', { docId });
  const ext = data.mimeType === 'video/x-matroska' ? 'mkv' : 'mp4';
  const downloadPath = path.join(UPLOAD_DIR, `${docId}.${ext}`);
  
  await db.collection(collection).doc(docId).update({ status: 'downloading', updatedAt: getAdmin().firestore.FieldValue.serverTimestamp() });

  let progressMessageId = null;
  try {
    const statusText = await bot.sendMessage(ADMIN_ID, `📥 Starting download for ${data.title} S${data.season}...`);
    progressMessageId = statusText.message_id;
  } catch (e) {
    diagLog('telegram api', 'Status push target unreachable', { error: e.message });
  }

  return new Promise((resolve, reject) => {
    const fileStream = fs.createWriteStream(downloadPath);
    const telegramStream = bot.getFileStream(data.telegram_file_id);
    
    let downloadedBytes = 0;
    let lastUpdateTs = 0;

    telegramStream.on('data', (chunk) => {
      downloadedBytes += chunk.length;
      const now = Date.now();
      if (data.fileSizeBytes && now - lastUpdateTs > 4000) {
        lastUpdateTs = now;
        const pct = Math.floor((downloadedBytes / data.fileSizeBytes) * 100);
        bot.editMessageText(`📥 Downloading progress tracking status: <b>${pct}%</b> completed.`, { chat_id: ADMIN_ID, message_id: progressMessageId, parse_mode: 'HTML' }).catch(() => {});
      }
    });

    telegramStream.pipe(fileStream);

    fileStream.on('finish', () => {
      fileStream.close();
      diagLog('download', 'Asset downloaded completely onto disk node buffers.', { downloadPath });
      if (progressMessageId) bot.deleteMessage(ADMIN_ID, progressMessageId).catch(() => {});
      
      compressionQueue.add(() => managedTaskRetry(() => compressionTaskWorker(docId, data, collection, downloadPath), 'compression', docId, collection));
      resolve();
    });

    fileStream.on('error', (err) => {
      fileStream.close();
      cleanFileNode(downloadPath);
      if (progressMessageId) bot.deleteMessage(ADMIN_ID, progressMessageId).catch(() => {});
      reject(err);
    });
    
    telegramStream.on('error', (err) => {
      fileStream.close();
      cleanFileNode(downloadPath);
      if (progressMessageId) bot.deleteMessage(ADMIN_ID, progressMessageId).catch(() => {});
      reject(err);
    });
  });
}

async function compressionTaskWorker(docId, data, collection, sourcePath) {
  diagLog('compression', 'Initializing FFmpeg pipeline mapping interfaces...', { docId });
  const targetPath = path.join(CONVERT_DIR, `${docId}.mp4`);
  
  await db.collection(collection).doc(docId).update({ status: 'compressing', updatedAt: getAdmin().firestore.FieldValue.serverTimestamp() });

  let progressMessageId = null;
  try {
    const msg = await bot.sendMessage(ADMIN_ID, `⏳ Encoding stream pipeline elements for ${data.title} Ep ${data.episode}...`);
    progressMessageId = msg.message_id;
  } catch (e) {}

  return new Promise((resolve, reject) => {
    // Replaced exec() shell execution completely with spawn() to protect context bounds and handle memory leaks
    const ffmpegArgs = [
      '-y', '-i', sourcePath,
      '-c:v', 'libx264', '-pix_fmt', 'yuv420p', '-profile:v', 'main', '-level', '3.1',
      '-movflags', '+faststart',
      '-c:a', 'aac', '-b:a', '128k',
      targetPath
    ];

    diagLog('ffmpeg', 'Spawning sub-process instances dynamically matching properties', { args: ffmpegArgs.join(' ') });
    const child = spawn('ffmpeg', ffmpegArgs);

    let duration = data.duration || 0;
    let lastProgressTs = 0;

    child.stderr.on('data', (buffer) => {
      const dataStr = buffer.toString();
      
      // Parse dynamic duration data mappings directly from output context if not explicitly passed earlier
      if (!duration) {
        const durMatch = dataStr.match(/Duration:\s*(\d+):(\d+):(\d+)/);
        if (durMatch) {
          duration = (parseInt(durMatch[1], 10) * 3600) + (parseInt(durMatch[2], 10) * 60) + parseInt(durMatch[3], 10);
        }
      }

      const timeMatch = dataStr.match(/time=\s*(\d+):(\d+):(\d+)/);
      if (timeMatch && duration) {
        const currentTime = (parseInt(timeMatch[1], 10) * 3600) + (parseInt(timeMatch[2], 10) * 60) + parseInt(timeMatch[3], 10);
        const pct = Math.min(100, Math.floor((currentTime / duration) * 100));
        const now = Date.now();
        if (now - lastProgressTs > 4000 && progressMessageId) {
          lastProgressTs = now;
          bot.editMessageText(`⏳ Transcoding compression profile status execution: <b>${pct}%</b> compiled.`, { chat_id: ADMIN_ID, message_id: progressMessageId, parse_mode: 'HTML' }).catch(() => {});
        }
      }
    });

    child.on('close', (code) => {
      cleanFileNode(sourcePath); // Always clean up temporary input immediately following computation closure
      if (progressMessageId) bot.deleteMessage(ADMIN_ID, progressMessageId).catch(() => {});

      if (code === 0) {
        diagLog('compression', 'FFmpeg task iteration exited cleanly.', { targetPath });
        uploadQueue.add(() => managedTaskRetry(() => uploadTaskWorker(docId, data, collection, targetPath), 'upload', docId, collection));
        resolve();
      } else {
        cleanFileNode(targetPath);
        reject(new Error(`FFmpeg engine execution closed with unexpected error code variant state: ${code}`));
      }
    });

    child.on('error', (err) => {
      cleanFileNode(sourcePath);
      cleanFileNode(targetPath);
      if (progressMessageId) bot.deleteMessage(ADMIN_ID, progressMessageId).catch(() => {});
      reject(err);
    });
  });
}

async function uploadTaskWorker(docId, data, collection, targetPath) {
  diagLog('upload', 'Routing file system stream buffers to main cloud endpoints...', { docId });
  await db.collection(collection).doc(docId).update({ status: 'uploading', updatedAt: getAdmin().firestore.FieldValue.serverTimestamp() });

  if (!fs.existsSync(targetPath)) {
    throw new Error(`Critical asset resolution tracking index mapping missing at address point: ${targetPath}`);
  }

  let progressMessageId = null;
  try {
    const msg = await bot.sendMessage(ADMIN_ID, `📤 Transmitting optimized payload assets to target channel nodes...`);
    progressMessageId = msg.message_id;
  } catch (e) {}

  try {
    const captionMsg = `${data.title} - S${data.season}E${data.episode} [${data.language}] [${data.quality}]`;
    
    // Core transmission implementation framework directly to private storage environments
    const uploadedMsg = await bot.sendVideo(STORAGE_CHANNEL_ID, targetPath, {
      caption: captionMsg
    });

    const videoMeta = uploadedMsg.video;
    if (!videoMeta) {
      throw new Error("Telegram API accepted standard parameter packet upload payload structural array components verification failure error payload missing.");
    }

    await db.collection(collection).doc(docId).update({
      status: 'completed',
      messageId: uploadedMsg.message_id,
      channelId: STORAGE_CHANNEL_ID,
      telegram_file_id: videoMeta.file_id,
      file_unique_id: videoMeta.file_unique_id,
      fileSizeBytes: videoMeta.file_size,
      duration: videoMeta.duration,
      updatedAt: getAdmin().firestore.FieldValue.serverTimestamp()
    });

    diagLog('firestore', 'Metadata structures successfully synchronized to structural targets.', { docId });
    if (progressMessageId) bot.deleteMessage(ADMIN_ID, progressMessageId).catch(() => {});
    
    cleanFileNode(targetPath);
    return true;
  } catch (err) {
    cleanFileNode(targetPath);
    if (progressMessageId) bot.deleteMessage(ADMIN_ID, progressMessageId).catch(() => {});
    throw err;
  }
}

function cleanFileNode(filePath) {
  try {
    if (fs.existsSync(filePath)) {
      fs.unlinkSync(filePath);
      diagLog('cleanup', 'Cleaned up temporary disk file asset successfully', { filePath });
    }
  } catch (e) {
    diagLog('errors', 'Failed to remove file from disk buffer', { filePath, error: e.message });
  }
}

// ============================================================================
// STAGE 4 — WIZARD MANAGEMENT DISPATCH ROUTERS
// ============================================================================
bot.onText(/^\/add (anime|webseries|anime-movie|movie)$/i, async (msg, match) => {
  const chatId = msg.chat.id;
  if (!isAdmin(chatId)) return;

  const targetType = match[1].toLowerCase();
  diagLog('commands', `Initialization parsing context requested: /add ${targetType}`, { chatId });
  
  userState.set(chatId, { mode: 'collect_metadata', type: targetType, step: 0, collected: {} });
  await bot.sendMessage(chatId, `🎬 <b>Entering Setup Mode for [${targetType.toUpperCase()}]</b>\nPlease enter the <b>Title</b>:`, { parse_mode: 'HTML' });
});

bot.on('message', async (msg) => {
  const chatId = msg.chat.id;
  if (!isAdmin(chatId) || !userState.has(chatId) || isShuttingDown) return;

  const state = userState.get(chatId);
  if (msg.text && msg.text.startsWith('/')) {
    if (msg.text.toLowerCase() === '/done' && state.mode === 'upload_mode') {
      userState.delete(chatId);
      await bot.sendMessage(chatId, '🏁 <b>Upload Session Finished!</b> Exited loop configuration scopes cleanly.', { parse_mode: 'HTML' });
      diagLog('commands', 'Terminated target channel upload active configuration session loops context references.');
    }
    return;
  }

  if (state.mode === 'collect_metadata') {
    const steps = ['title', 'season', 'language', 'quality', 'poster', 'banner', 'description'];
    const currentField = steps[state.step];

    if (currentField === 'poster' || currentField === 'banner') {
      if (msg.photo) {
        state.collected[currentField] = msg.photo[msg.photo.length - 1].file_id;
      } else {
        state.collected[currentField] = (msg.text && msg.text.toLowerCase() === 'skip') ? null : msg.text;
      }
    } else if (currentField === 'description') {
      state.collected[currentField] = (msg.text && msg.text.toLowerCase() === 'skip') ? null : msg.text;
    } else {
      state.collected[currentField] = msg.text;
    }

    state.step++;
    
    if (state.step < steps.length) {
      const nextField = steps[state.step];
      const isOptional = state.step >= 4;
      await bot.sendMessage(chatId, `Enter <b>${nextField.charAt(0).toUpperCase() + nextField.slice(1)}</b>${isOptional ? ' (optional - type "skip" to ignore)' : ''}:`, { parse_mode: 'HTML' });
      userState.set(chatId, state);
    } else {
      state.mode = 'upload_mode';
      userState.set(chatId, state);
      diagLog('commands', 'Session parameters parsed and staged.', state.collected);
      await bot.sendMessage(chatId, `🚀 <b>Configuration Profile Active!</b>\n\nYou are now in <b>Upload Mode</b>.\nForward files or video payloads directly.\n\nType <code>/done</code> when you are finished uploading episodes.`, { parse_mode: 'HTML' });
    }
    return;
  }

  if (state.mode === 'upload_mode') {
    const isVideo = msg.video || (msg.document && msg.document.mime_type?.startsWith('video/'));
    if (!isVideo) return;

    const mediaObj = msg.video || msg.document;
    diagLog('telegram api', 'Incoming media hook caught inside operational tracking configurations.', { fileUniqueId: mediaObj.file_unique_id });

    const collection = COLLECTION_MAP[state.type];
    const parsedSeason = parseInt(state.collected.season, 10) || 1;

    try {
      // Step 1: Prevent duplicate processing matching exact unique file hash marks immediately
      const dupFileCheck = await db.collection(collection)
        .where('file_unique_id', '==', mediaObj.file_unique_id)
        .limit(1)
        .get();

      if (!dupFileCheck.empty) {
        diagLog('firestore', 'Blocking duplicate attempt matching exact fingerprint hash metadata criteria', { fileUniqueId: mediaObj.file_unique_id });
        await bot.sendMessage(chatId, `⚠️ <b>Duplicate Upload Dropped!</b> This exact video file fingerprint has already been registered in the database.`, { parse_mode: 'HTML' });
        return;
      }

      // Dynamic forward direct optimization detection block
      // Copies the video instantly to the private storage channel without processing if Telegram permits it
      if (msg.forward_from_chat || msg.chat.type === 'channel') {
        diagLog('telegram api', 'Attempting direct structural link cloning mapping parameters without network routing passes...');
        try {
          const directCopyMsg = await bot.copyMessage(STORAGE_CHANNEL_ID, msg.chat.id, msg.message_id, {
            caption: `${state.collected.title} - S${parsedSeason} [Direct Copy Transfer Mapping]`
          });
          
          if (directCopyMsg && directCopyMsg.message_id) {
            diagLog('telegram api', 'Direct link duplication operation executed successfully.');
            await bot.sendMessage(chatId, `⚡ <b>Direct Forward Copy Succeeded!</b> Skimmed conversion process paths safely.`, { parse_mode: 'HTML' });
            
            // Register item inside Firestore system models directly
            await db.collection(collection).add({
              title: state.collected.title,
              season: parsedSeason,
              episode: 999, // Static boundary or placeholder for standalone files transfers configurations mappings
              language: state.collected.language,
              quality: state.collected.quality,
              status: 'completed',
              messageId: directCopyMsg.message_id,
              channelId: STORAGE_CHANNEL_ID,
              file_unique_id: mediaObj.file_unique_id,
              createdAt: getAdmin().firestore.FieldValue.serverTimestamp()
            });
            return;
          }
        } catch (copyErr) {
          diagLog('warnings', 'Direct transfer replication rejected. Falling back to local pipeline structures.', { reason: copyErr.message });
        }
      }

      // Step 2: ACID Transactions Block to auto-index and allocate unique episode numbers safely
      let nextEpisode = 1;
      const targetDocId = `${collection}_${crypto.randomBytes(4).toString('hex')}`;
      const docRef = db.collection(collection).doc(targetDocId);

      await db.runTransaction(async (transaction) => {
        const queryRef = db.collection(collection)
          .where('title', '==', state.collected.title)
          .where('season', '==', parsedSeason);
          
        const querySnapshot = await transaction.get(queryRef);
        
        if (!querySnapshot.empty) {
          const activeEpisodes = querySnapshot.docs.map(d => parseInt(d.data().episode, 10) || 0);
          nextEpisode = Math.max(...activeEpisodes) + 1;
        }

        const payloadData = {
          title: state.collected.title,
          season: parsedSeason,
          episode: nextEpisode,
          language: state.collected.language,
          quality: state.collected.quality,
          poster: state.collected.poster || null,
          banner: state.collected.banner || null,
          description: state.collected.description || null,
          telegram_file_id: mediaObj.file_id,
          file_unique_id: mediaObj.file_unique_id,
          mimeType: mediaObj.mime_type || 'video/mp4',
          fileSizeBytes: mediaObj.file_size || 0,
          duration: mediaObj.duration || 0,
          status: 'pending_processing',
          channelId: chatId,
          messageId: null,
          createdAt: getAdmin().firestore.FieldValue.serverTimestamp(),
          updatedAt: getAdmin().firestore.FieldValue.serverTimestamp()
        };

        transaction.set(docRef, payloadData);
      });

      diagLog('firestore', 'ACID transaction committed safely.', { docId: targetDocId, episode: nextEpisode });
      await bot.sendMessage(chatId, `📥 Staged: <b>Episode ${nextEpisode}</b> for processing sequence arrays pipelines.`, { parse_mode: 'HTML' });

      // Fetch freshly instantiated parameters map payload elements from document target context data values
      const postFetchDoc = await docRef.get();
      runPipelineRouter(targetDocId, postFetchDoc.data(), collection);

    } catch (err) {
      diagLog('errors', 'Failed transactional parsing validation matrix logic routines inside worker flows.', { error: err.message });
      await bot.sendMessage(chatId, `❌ Pipeline execution failure context error: ${err.message}`);
    }
  }
});

// ============================================================================
// STAGE 5 — ADMINISTRATIVE AND MANAGEMENT MODULE COMMAND INTERFACES
// ============================================================================
registerAdminCommand('admin status', async (msg) => {
  const statusReport = [
    '⚙️ <b>System Pipeline Operations Status</b>',
    `Processing Core State: ${queuePaused ? '⏸ PAUSED' : '▶️ ACTIVE'}`,
    `Download Cluster Jobs: ${downloadQueue.pending} active / ${downloadQueue.size} pending`,
    `Compression Cluster Jobs: ${compressionQueue.pending} active / ${compressionQueue.size} pending`,
    `Preservation Upload Queue: ${uploadQueue.pending} active / ${uploadQueue.size} pending`,
    `Running Structural Workers Count: ${activeJobsCount}`
  ].join('\n');
  await bot.sendMessage(msg.chat.id, statusReport, { parse_mode: 'HTML' });
});

registerAdminCommand('admin queue', async (msg) => {
  await bot.sendMessage(msg.chat.id, `📋 Core operational backlog count value metric: <b>${downloadQueue.size + compressionQueue.size + uploadQueue.size} items pending execution</b>.`, { parse_mode: 'HTML' });
});

registerAdminCommand('admin pause', async (msg) => {
  queuePaused = true;
  diagLog('queue', 'Pipeline operations execution loops suspended by administrative override directive.');
  await bot.sendMessage(msg.chat.id, '⏸ <b>Processing Workers Suspended!</b> Incoming objects will store state properties until /admin resume.');
});

registerAdminCommand('admin resume', async (msg) => {
  queuePaused = false;
  diagLog('queue', 'Pipeline processing modules operationalized again.');
  await bot.sendMessage(msg.chat.id, '▶️ <b>Processing Workers Reactivated!</b> Flushing backlog queue elements across pipeline modules.');
  await recoverPendingQueue();
});

registerAdminCommand('admin retry', async (msg) => {
  await bot.sendMessage(msg.chat.id, '🔄 Refreshing and re-evaluating broken/failed process components within structural tracking tables...');
  await recoverPendingQueue();
});

registerAdminCommand('admin cancel', async (msg) => {
  downloadQueue.clear();
  compressionQueue.clear();
  uploadQueue.clear();
  diagLog('queue', 'Purged configuration execution blocks inside volatile cluster tracking spaces.');
  await bot.sendMessage(msg.chat.id, '🗑 <b>Backlog Queues Completely Purged!</b> All cached memory processing item array references dropped.');
});

function registerAdminCommand(commandTokenStr, handlerFn) {
  const chunks = commandTokenStr.split(' ');
  bot.onText(new RegExp(`^\\/${chunks[0]}(?:\\s+(.*))?$`, 'i'), async (msg, match) => {
    if (!isAdmin(msg.chat.id)) return;
    if (chunks.length > 1) {
      const argsText = match[1] ? match[1].trim().toLowerCase() : '';
      if (!argsText.startsWith(chunks[1])) return;
    }
    diagLog('commands', `Processing incoming valid administrative command sequence string signature match verification line: /${commandTokenStr}`);
    try {
      await handlerFn(msg);
    } catch (err) {
      diagLog('errors', `Internal exception runtime interrupt processing command routing token execution block matching: /${commandTokenStr}`, { error: err.message });
    }
  });
}

// ============================================================================
// STAGE 6 — SYSTEM GRACEFUL SHUTDOWN & LIFECYCLE CONTROLS
// ============================================================================
async function handleSystemShutdown(signal) {
  if (isShuttingDown) return;
  isShuttingDown = true;
  
  diagLog('shutdown', `Intercepted framework lifecycle shutdown signal intercept node event: ${signal}. Commencing isolation workflows.`);
  queuePaused = true;

  downloadQueue.pause();
  compressionQueue.pause();

  let checkIterations = 0;
  while (activeJobsCount > 0 && checkIterations < 15) {
    diagLog('shutdown', `Waiting for active pipeline routines to drain cleanly... Threads running: ${activeJobsCount}`);
    await new Promise(r => setTimeout(r, 2000));
    checkIterations++;
  }

  diagLog('shutdown', 'System clean down procedures completely terminated. Finalizing processes.');
  process.exit(0);
}

process.on('SIGINT', () => handleSystemShutdown('SIGINT'));
process.on('SIGTERM', () => handleSystemShutdown('SIGTERM'));

module.exports = {
  initBot,
  isAdmin,
  processUpdate: (update) => {
    try {
      if (isShuttingDown) return;
      bot.processUpdate(update);
    } catch (err) {
      diagLog('errors', 'Critical interruption handled inside raw webhook update dispatcher processing thread', { error: err.message });
    }
  },
};
