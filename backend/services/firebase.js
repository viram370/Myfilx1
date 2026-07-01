/**
 * Firebase Admin SDK Service
 */
const admin = require('firebase-admin');

let db;
let initialized = false;

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

  admin.initializeApp({
    credential: admin.credential.cert(serviceAccount),
    databaseURL: process.env.FIREBASE_DATABASE_URL,
  });

  db = admin.firestore();
  db.settings({ ignoreUndefinedProperties: true });

  initialized = true;
  console.log('✅ Firebase initialized');
}

function getDB() {
  if (!db) throw new Error('Firebase not initialized');
  return db;
}
function getAdmin() { return admin; }

async function getDoc(collection, id) {
  const doc = await getDB().collection(collection).doc(id).get();
  return doc.exists ? { id: doc.id, ...doc.data() } : null;
}

async function setDoc(collection, id, data) {
  await getDB().collection(collection).doc(id).set(
    { ...data, updatedAt: admin.firestore.FieldValue.serverTimestamp() },
    { merge: true }
  );
  return id;
}

async function updateDoc(collection, id, data) {
  await getDB().collection(collection).doc(id).update({
    ...data,
    updatedAt: admin.firestore.FieldValue.serverTimestamp(),
  });
  return id;
}

async function deleteDoc(collection, id) {
  await getDB().collection(collection).doc(id).delete();
  return id;
}

async function addDoc(collection, data) {
  const ref = await getDB().collection(collection).add({
    ...data,
    createdAt: admin.firestore.FieldValue.serverTimestamp(),
    updatedAt: admin.firestore.FieldValue.serverTimestamp(),
  });
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
  const snap = await q.get();
  return snap.docs.map(d => ({ id: d.id, ...d.data() }));
}

module.exports = {
  initFirebase, getDB, getAdmin,
  getDoc, setDoc, updateDoc, deleteDoc, addDoc, queryDocs,
};
