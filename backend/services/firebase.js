/**
 * services/firebase.js
 * Firestore access layer used by every route + the bot. Adds retry on
 * transient errors, a small read-through cache for hot documents, batched
 * write helpers, and duplicate-detection utilities.
 */
'use strict';

const admin = require('firebase-admin');
const { makeLogger } = require('../utils/logger');
const log = makeLogger('services/firebase.js');

let db;
let initialized = false;

const READ_CACHE_TTL_MS = 15_000;
const readCache = new Map();
const BATCH_LIMIT = 450;

function initFirebase() {
  if (initialized) return;

  const serviceAccount = {
    type: 'service_account',
    project_id: process.env.FIREBASE_PROJECT_ID,
    private_key_id: process.env.FIREBASE_PRIVATE_KEY_ID,
    private_key: process.env.FIREBASE_PRIVATE_KEY?.replace(/\\n/g, '\n'),
    client_email: process.env.FIREBASE_CLIENT_EMAIL,
    client_id: process.env.FIREBASE_CLIENT_ID,
    auth_uri: 'https://accounts.google.com/o/oauth2/auth',
    token_uri: 'https://oauth2.googleapis.com/token',
  };

  if (!serviceAccount.project_id || !serviceAccount.private_key || !serviceAccount.client_email) {
    log.error('initFirebase', 'Missing required Firebase service-account env vars', new Error('Incomplete Firebase config'));
    throw new Error('Firebase service account is not fully configured.');
  }

  admin.initializeApp({
    credential: admin.credential.cert(serviceAccount),
    databaseURL: process.env.FIREBASE_DATABASE_URL,
  });

  db = admin.firestore();
  db.settings({ ignoreUndefinedProperties: true });

  initialized = true;
  log.success('initFirebase', 'Firebase initialized');
}

function getDB() {
  if (!db) throw new Error('Firebase not initialized — call initFirebase() first.');
  return db;
}
function getAdmin() {
  return admin;
}

function isTransientError(err) {
  if (!err) return false;
  const transientCodes = new Set([4, 8, 10, 13, 14, 'unavailable', 'deadline-exceeded', 'aborted', 'internal', 'resource-exhausted']);
  if (transientCodes.has(err.code)) return true;
  return /ECONNRESET|ETIMEDOUT|socket hang up|UNAVAILABLE/i.test(err.message || '');
}

async function withRetry(fn, { retries = 3, baseDelayMs = 250, label = 'firestore-op' } = {}) {
  let attempt = 0;
  // eslint-disable-next-line no-constant-condition
  while (true) {
    try {
      return await fn();
    } catch (err) {
      attempt++;
      if (attempt > retries || !isTransientError(err)) throw err;
      const delay = baseDelayMs * 2 ** (attempt - 1) + Math.floor(Math.random() * 100);
      log.warn('withRetry', `Retrying ${label} (${attempt}/${retries})`, { reason: err.message });
      await new Promise((r) => setTimeout(r, delay));
    }
  }
}

function cacheKey(collection, id) {
  return `${collection}/${id}`;
}
function invalidateCache(collection, id) {
  readCache.delete(cacheKey(collection, id));
}

async function getDoc(collection, id, { useCache = true } = {}) {
  if (!id) return null;
  const key = cacheKey(collection, id);
  if (useCache) {
    const hit = readCache.get(key);
    if (hit && Date.now() - hit.fetchedAt < READ_CACHE_TTL_MS) return hit.value;
  }
  const doc = await withRetry(() => getDB().collection(collection).doc(String(id)).get(), { label: `getDoc(${collection})` });
  const value = doc.exists ? { id: doc.id, ...doc.data() } : null;
  readCache.set(key, { value, fetchedAt: Date.now() });
  return value;
}

async function setDoc(collection, id, data) {
  await withRetry(
    () => getDB().collection(collection).doc(String(id)).set(
      { ...data, updatedAt: admin.firestore.FieldValue.serverTimestamp() },
      { merge: true }
    ),
    { label: `setDoc(${collection})` }
  );
  invalidateCache(collection, id);
  return id;
}

async function updateDoc(collection, id, data) {
  await withRetry(
    () => getDB().collection(collection).doc(String(id)).update({
      ...data,
      updatedAt: admin.firestore.FieldValue.serverTimestamp(),
    }),
    { label: `updateDoc(${collection})` }
  );
  invalidateCache(collection, id);
  return id;
}

async function deleteDoc(collection, id) {
  await withRetry(() => getDB().collection(collection).doc(String(id)).delete(), { label: `deleteDoc(${collection})` });
  invalidateCache(collection, id);
  return id;
}

async function addDoc(collection, data) {
  const ref = await withRetry(
    () => getDB().collection(collection).add({
      ...data,
      createdAt: admin.firestore.FieldValue.serverTimestamp(),
      updatedAt: admin.firestore.FieldValue.serverTimestamp(),
    }),
    { label: `addDoc(${collection})` }
  );
  return ref.id;
}

async function queryDocs(collection, filters = [], orderBy = null, limit = null) {
  let q = getDB().collection(collection);
  for (const [field, op, value] of filters) q = q.where(field, op, value);
  if (orderBy) {
    const [field, dir = 'asc'] = Array.isArray(orderBy) ? orderBy : [orderBy];
    q = q.orderBy(field, dir);
  }
  if (limit) q = q.limit(limit);
  const snap = await withRetry(() => q.get(), { label: `queryDocs(${collection})` });
  return snap.docs.map((d) => ({ id: d.id, ...d.data() }));
}

/**
 * Cursor-paginated query (createdAt desc by default). Avoids composite
 * indexes for mixed filters by only ever combining equality filters with
 * a single orderBy field.
 */
async function queryDocsPaginated(collection, filters, { orderField = 'createdAt', direction = 'desc', limit = 30, cursorId = null } = {}) {
  let q = getDB().collection(collection);
  for (const [field, op, value] of filters) q = q.where(field, op, value);
  q = q.orderBy(orderField, direction).limit(limit + 1);

  if (cursorId) {
    const cursorDoc = await getDB().collection(collection).doc(cursorId).get();
    if (cursorDoc.exists) q = q.startAfter(cursorDoc);
  }

  const snap = await withRetry(() => q.get(), { label: `queryDocsPaginated(${collection})` });
  const docs = snap.docs.slice(0, limit).map((d) => ({ id: d.id, ...d.data() }));
  const hasMore = snap.docs.length > limit;
  return { docs, nextCursor: hasMore ? docs[docs.length - 1]?.id || null : null };
}

/** Batched writes (set), chunked under Firestore's 500-op batch cap. */
async function batchSet(collection, items) {
  const chunks = [];
  for (let i = 0; i < items.length; i += BATCH_LIMIT) chunks.push(items.slice(i, i + BATCH_LIMIT));
  let written = 0;
  for (const chunk of chunks) {
    await withRetry(async () => {
      const batch = getDB().batch();
      for (const { id, data } of chunk) batch.set(getDB().collection(collection).doc(String(id)), data, { merge: false });
      await batch.commit();
    }, { label: `batchSet(${collection})` });
    written += chunk.length;
  }
  return written;
}

/** Batched deletes, chunked under Firestore's 500-op batch cap. */
async function batchDelete(collection, ids) {
  const chunks = [];
  for (let i = 0; i < ids.length; i += BATCH_LIMIT) chunks.push(ids.slice(i, i + BATCH_LIMIT));
  let deleted = 0;
  for (const chunk of chunks) {
    await withRetry(async () => {
      const batch = getDB().batch();
      for (const id of chunk) batch.delete(getDB().collection(collection).doc(String(id)));
      await batch.commit();
    }, { label: `batchDelete(${collection})` });
    deleted += chunk.length;
  }
  return deleted;
}

/**
 * Duplicate detection: checks a batch of file_unique_id values against
 * existing `videos` documents. Chunked to respect Firestore's 'in' query
 * cap (30 values, we use 10 for older-SDK safety).
 */
async function findExistingFileUniqueIds(uniqueIds) {
  const found = new Set();
  const unique = [...new Set(uniqueIds)].filter(Boolean);
  const chunkSize = 10;
  for (let i = 0; i < unique.length; i += chunkSize) {
    const chunk = unique.slice(i, i + chunkSize);
    const snap = await withRetry(
      () => getDB().collection('videos').where('file_unique_id', 'in', chunk).select('file_unique_id').get(),
      { label: 'findExistingFileUniqueIds' }
    );
    snap.forEach((d) => found.add(d.get('file_unique_id')));
  }
  return found;
}

/** Repair pass: fills in any missing-but-required fields on a video doc. */
function repairVideoFields(data) {
  const repaired = { ...data };
  if (repaired.published === undefined) repaired.published = true;
  if (repaired.views === undefined) repaired.views = 0;
  if (repaired.likes === undefined) repaired.likes = 0;
  if (!repaired.seriesTitle && repaired.title) repaired.seriesTitle = repaired.title;
  if (repaired.season === undefined) repaired.season = null;
  if (repaired.episode === undefined) repaired.episode = null;
  if (!repaired.language) repaired.language = 'Unknown';
  if (!repaired.category) repaired.category = 'Uncategorized';
  return repaired;
}

async function healthCheck() {
  const start = Date.now();
  try {
    await getDB().collection('videos').limit(1).get();
    return { ok: true, latencyMs: Date.now() - start };
  } catch (err) {
    return { ok: false, latencyMs: Date.now() - start, error: err.message };
  }
}

module.exports = {
  initFirebase, getDB, getAdmin,
  getDoc, setDoc, updateDoc, deleteDoc, addDoc, queryDocs, queryDocsPaginated,
  batchSet, batchDelete, findExistingFileUniqueIds, repairVideoFields,
  withRetry, isTransientError, invalidateCache, healthCheck,
};
