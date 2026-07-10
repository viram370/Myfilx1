/**
 * utils/logger.js
 * Structured logger — every error log carries file/function/timestamp/stack
 * plus optional Telegram/Firestore/HTTP context fields.
 */
'use strict';

const RING_SIZE = 300;
const ring = [];

function push(entry) {
  ring.push(entry);
  if (ring.length > RING_SIZE) ring.shift();
}

function base(file, fn) {
  return { file, function: fn, timestamp: new Date().toISOString() };
}

function fmt(meta) {
  if (!meta) return '';
  const parts = Object.entries(meta)
    .filter(([, v]) => v !== undefined && v !== null && v !== '')
    .map(([k, v]) => `${k}=${typeof v === 'object' ? JSON.stringify(v) : v}`);
  return parts.length ? ` | ${parts.join(' ')}` : '';
}

function makeLogger(file) {
  return {
    info(fn, msg, meta = {}) {
      const entry = { ...base(file, fn), level: 'info', msg, ...meta };
      console.log(`ℹ️ [${file}:${fn}] ${msg}${fmt(meta)}`);
      push(entry);
    },
    success(fn, msg, meta = {}) {
      const entry = { ...base(file, fn), level: 'success', msg, ...meta };
      console.log(`✅ [${file}:${fn}] ${msg}${fmt(meta)}`);
      push(entry);
    },
    warn(fn, msg, meta = {}) {
      const entry = { ...base(file, fn), level: 'warn', msg, ...meta };
      console.warn(`⚠️ [${file}:${fn}] ${msg}${fmt(meta)}`);
      push(entry);
    },
    error(fn, msg, err, meta = {}) {
      const stack = err?.stack || new Error(msg).stack;
      const line = (stack || '').split('\n')[1]?.trim() || 'unknown';
      const entry = {
        ...base(file, fn),
        level: 'error',
        msg,
        errorMessage: err?.message,
        line,
        stack,
        ...meta,
      };
      console.error(`❌ [${file}:${fn}] ${msg}${fmt({ ...meta, error: err?.message })}`);
      if (stack) console.error(stack);
      push(entry);
    },
    recent(n = 50) {
      return ring.slice(-n);
    },
  };
}

module.exports = { makeLogger, recentLogs: (n) => ring.slice(-n) };
