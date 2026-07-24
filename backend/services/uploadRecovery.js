/**
 * services/uploadRecovery.js
 * ----------------------------------------------------------------------
 * Firestore-backed persistence for the /add upload pipeline so in-progress
 * uploads survive a Render free-tier restart (low-RAM OOM restarts kill the
 * process and wipe queue/pipeline.js's in-memory `sessions` Map — see that
 * file's own header note: "Sessions live only in memory ... a process
 * restart mid-batch loses in-flight items").
 *
 * This module is intentionally "dumb" — it knows nothing about Telegram or
 * queue/pipeline.js internals beyond the shape of `session`/`item` objects
 * pipeline.js already hands to `onProgress`/`onFinished`. It only reads/
 * writes two Firestore collections and exposes small helpers. All
 * orchestration (rebuilding a pipeline session, re-adding items, calling
 * startBatch again) lives in handlers/adminUpload.js, which already owns
 * that logic — this keeps queue/pipeline.js completely untouched.
 *
 * Collections:
 *   upload_recovery_sessions/{chatId}
 *     - one doc per admin chat's current /add batch. Holds everything
 *       needed to rebuild a pipeline session from scratch: kind, category,
 *       hasSeason, title, season, language, quality, year, thumbnailFileId,
 *       storageChannelId.
 *
 *   upload_recovery_jobs/{chatId_fileUniqueId}
 *     - one doc per episode/video item in a batch. Holds status, progress,
 *       retry count, and the source location (item.fileId for a direct
 *       upload, or item.forwardChatId/forwardMessageId for anything that
 *       needs a channel download) needed to re-queue it.
 *
 * Status enum written to Firestore (Requirement 2 — kept distinct from
 * pipeline.js's own internal item.status values, which are more granular:
 * buffered/validated/waiting/copying/downloading/ready/uploading/done/
 * failed/skipped). mapPipelineStatus() below is the single place that
 * translates one into the other:
 *   waiting -> downloading -> downloaded -> uploading -> uploaded -> saving -> completed
 *   (any state) -> failed
 * pipeline.js has no separate "uploaded" vs "saving" status of its own —
 * writeFirestoreDoc() runs synchronously inside the same step that flips
 * an item straight from 'uploading' to 'done' — so those two are only ever
 * observed together, both collapsing into 'completed'.
 * ----------------------------------------------------------------------
 */
'use strict';

const { getDB, getAdmin } = require('./firebase');
const { makeLogger } = require('../utils/logger');
const log = makeLogger('services/uploadRecovery.js');

const SESSIONS_COLLECTION = 'upload_recovery_sessions';
const JOBS_COLLECTION = 'upload_recovery_jobs';
const VIDEOS_COLLECTION = 'videos';

const MAX_RETRY_COUNT = 3;

const STATUS = Object.freeze({
  WAITING: 'waiting',
  DOWNLOADING: 'downloading',
  DOWNLOADED: 'downloaded',
  UPLOADING: 'uploading',
  UPLOADED: 'uploaded',
  SAVING: 'saving',
  COMPLETED: 'completed',
  FAILED: 'failed',
});

// Tracks which job docs this process has already given a createdAt to, so
// repeated progress-tick writes don't keep resetting it via serverTimestamp().
// In-memory only (resets on restart) — recovered jobs simply get a fresh
// createdAt after a restart, which is harmless bookkeeping.
const seenJobIds = new Set();

/** Normalizes pipeline.js's internal item.status into our fixed 8-value enum. */
function mapPipelineStatus(item) {
  const raw = String(item?.status || '').toLowerCase();
  if (raw === 'done') return STATUS.COMPLETED;
  if (raw === 'skipped') return STATUS.COMPLETED; // duplicate — nothing left to do
  if (raw === 'failed') return STATUS.FAILED;
  if (raw === 'downloading' || raw === 'copying') return STATUS.DOWNLOADING;
  if (raw === 'ready') return STATUS.DOWNLOADED;
  if (raw === 'uploading') return STATUS.UPLOADING;
  if (raw === 'buffered' || raw === 'validated' || raw === 'waiting' || !raw) return STATUS.WAITING;
  return STATUS.WAITING;
}

function jobId(chatId, item) {
  const key = item.fileUniqueId || item.fileId || `${item.season ?? 0}_${item.episode ?? item.seq ?? 0}`;
  return `${chatId}_${key}`;
}

/** Upserts the session-level context needed to rebuild a batch after a restart. */
async function upsertSessionMaster(chatId, session) {
  try {
    const db = getDB();
    const admin = getAdmin();
    await db.collection(SESSIONS_COLLECTION).doc(String(chatId)).set({
      chatId,
      kind: session.kind,
      category: session.category,
      hasSeason: !!session.hasSeason,
      title: session.title,
      season: session.season ?? null,
      language: session.language,
      quality: session.quality ?? null,
      year: session.year ?? null,
      thumbnailFileId: session.thumbnailFileId ?? null,
      storageChannelId: session.storageChannelId ?? null,
      completed: false,
      updatedAt: admin.firestore.FieldValue.serverTimestamp(),
      createdAt: admin.firestore.FieldValue.serverTimestamp(),
    }, { merge: true });
  } catch (err) {
    log.error('upsertSessionMaster', 'failed to persist session master', err, { chatId });
  }
}

async function markSessionCompleted(chatId) {
  try {
    const db = getDB();
    const admin = getAdmin();
    await db.collection(SESSIONS_COLLECTION).doc(String(chatId)).set({
      completed: true,
      updatedAt: admin.firestore.FieldValue.serverTimestamp(),
    }, { merge: true });
  } catch (err) {
    log.error('markSessionCompleted', 'failed to mark session completed', err, { chatId });
  }
}

async function getSessionMaster(chatId) {
  const db = getDB();
  const doc = await db.collection(SESSIONS_COLLECTION).doc(String(chatId)).get();
  return doc.exists ? doc.data() : null;
}

/**
 * Upserts one job (episode/video) document from a live pipeline item.
 * Fire-and-forget from the caller's perspective — never throws.
 */
async function recordItem(chatId, item, extra = {}) {
  try {
    const db = getDB();
    const admin = getAdmin();
    const id = jobId(chatId, item);
    const status = mapPipelineStatus(item);
    const ref = db.collection(JOBS_COLLECTION).doc(id);

    const data = {
      sessionId: String(chatId),
      chatId,
      title: extra.title ?? null,
      category: extra.category ?? null,
      season: extra.season ?? null,
      episode: item.episode ?? null,
      fileUniqueId: item.fileUniqueId || null,
      fileId: item.fileId || null,
      sourceType: item.sourceType || null, // 'direct' | 'forwarded'
      forwardChatId: item.forwardChatId || null,
      forwardMessageId: item.forwardMessageId || null,
      chatMessageId: item.chatMessageId || null,
      fileSizeBytes: item.fileSizeBytes || null,
      originalFileName: item.originalFileName || null,
      originalMimeType: item.originalMimeType || null,
      status,
      stage: item.status || status,
      downloadProgress: typeof item.downloadProgress === 'number' ? item.downloadProgress : (status === STATUS.WAITING ? 0 : 100),
      uploadProgress: typeof item.uploadProgress === 'number' ? item.uploadProgress : ([STATUS.UPLOADED, STATUS.SAVING, STATUS.COMPLETED].includes(status) ? 100 : 0),
      retryCount: (item.attempts && (item.attempts.upload || item.attempts.compress)) || 0,
      error: item.error || null,
      completed: status === STATUS.COMPLETED,
      updatedAt: admin.firestore.FieldValue.serverTimestamp(),
    };

    if (!seenJobIds.has(id)) {
      data.createdAt = admin.firestore.FieldValue.serverTimestamp();
      seenJobIds.add(id);
    }

    await ref.set(data, { merge: true });
  } catch (err) {
    log.error('recordItem', 'failed to persist recovery job', err, { chatId });
  }
}

/**
 * Bumps a job's retry count on resume. Returns whether it has now exceeded
 * MAX_RETRY_COUNT (Requirement 7 — safe retries, mark failed after limit).
 */
async function incrementRetryAndCheck(job) {
  const db = getDB();
  const admin = getAdmin();
  const ref = db.collection(JOBS_COLLECTION).doc(job.id);
  const current = job.retryCount || 0;
  const next = current + 1;
  const exceeded = next > MAX_RETRY_COUNT;
  await ref.set({
    retryCount: next,
    status: exceeded ? STATUS.FAILED : STATUS.WAITING,
    completed: false,
    updatedAt: admin.firestore.FieldValue.serverTimestamp(),
  }, { merge: true });
  return { retryCount: next, exceeded };
}

/**
 * Fetches every job that isn't finished yet. Uses a single equality filter
 * (`completed == false`) and filters `failed` out in memory — mirroring
 * this codebase's existing policy (see services/bot.js's own header note)
 * of avoiding composite Firestore indexes.
 */
async function getPendingJobs() {
  const db = getDB();
  const snap = await db.collection(JOBS_COLLECTION).where('completed', '==', false).get();
  const jobs = [];
  snap.forEach((d) => {
    const data = d.data();
    if (data.status !== STATUS.FAILED) jobs.push({ id: d.id, ...data });
  });
  return jobs;
}

function groupJobsBySession(jobs) {
  const map = new Map();
  for (const j of jobs) {
    if (!map.has(j.sessionId)) map.set(j.sessionId, []);
    map.get(j.sessionId).push(j);
  }
  return map;
}

/**
 * Requirement 6 — never upload the same episode twice. Checks the real
 * `videos` collection (not just our recovery bookkeeping) by fileUniqueId —
 * exactly the same field pipeline.js's own findExistingDocId() checks, so
 * this agrees with what startBatch()'s built-in duplicate detection will
 * find anyway; kept here so /continue can report "already done" up front
 * without waiting for the batch to run.
 */
async function isAlreadyInLibrary(fileUniqueId) {
  if (!fileUniqueId) return false;
  const db = getDB();
  const snap = await db.collection(VIDEOS_COLLECTION).where('file_unique_id', '==', fileUniqueId).limit(1).get();
  return !snap.empty;
}

function formatRecoveryLog({ title, episode, previousStatus, newStatus }) {
  return `Recovered upload:\nAnime: ${title}\nEpisode: ${episode ?? '—'}\nPrevious status: ${previousStatus}\nNew status: ${newStatus}`;
}

module.exports = {
  STATUS,
  MAX_RETRY_COUNT,
  mapPipelineStatus,
  upsertSessionMaster,
  markSessionCompleted,
  getSessionMaster,
  recordItem,
  incrementRetryAndCheck,
  getPendingJobs,
  groupJobsBySession,
  isAlreadyInLibrary,
  formatRecoveryLog,
};
