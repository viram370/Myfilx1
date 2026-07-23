/**
 * utils/validators.js
 * Shared, defensive input validation for every API route — prevents
 * injection, malformed Firestore doc IDs, and unbounded pagination.
 */
'use strict';

const DOC_ID_RE = /^[A-Za-z0-9_-]{1,200}$/;
const CATEGORY_RE = /^(Anime|Movies|Web Series)$/;

class ApiValidationError extends Error {
  constructor(message, status = 400) {
    super(message);
    this.name = 'ApiValidationError';
    this.status = status;
  }
}

function isValidDocId(id) {
  return typeof id === 'string' && DOC_ID_RE.test(id);
}

function requireDocId(id, label = 'id') {
  if (!isValidDocId(id)) throw new ApiValidationError(`Invalid ${label}.`);
  return id;
}

function sanitizeText(input, { max = 500 } = {}) {
  if (input === undefined || input === null) return '';
  return String(input).replace(/[\u0000-\u001F\u007F]/g, '').trim().slice(0, max);
}

function clampInt(value, { min = 0, max = Number.MAX_SAFE_INTEGER, fallback }) {
  const n = parseInt(value, 10);
  if (!Number.isFinite(n)) return fallback;
  return Math.min(Math.max(n, min), max);
}

function isValidCategory(category) {
  return typeof category === 'string' && CATEGORY_RE.test(category);
}

function paginationParams(query, { defaultLimit = 30, maxLimit = 100 } = {}) {
  const limit = clampInt(query.limit, { min: 1, max: maxLimit, fallback: defaultLimit });
  const cursor = query.cursor && /^[A-Za-z0-9_\-:.]{1,300}$/.test(query.cursor) ? query.cursor : null;
  return { limit, cursor };
}

/** Stable, URL/doc-id-safe key for grouping — e.g. one Continue Watching
 * entry per anime/series regardless of which episode was last watched. */
function slugify(str) {
  return String(str || '').toLowerCase().trim().replace(/[^a-z0-9]+/g, '-').replace(/(^-+|-+$)/g, '').slice(0, 80) || 'untitled';
}

module.exports = {
  ApiValidationError,
  isValidDocId,
  requireDocId,
  sanitizeText,
  clampInt,
  isValidCategory,
  paginationParams,
  slugify,
  DOC_ID_RE,
};
