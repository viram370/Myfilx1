/**
 * services/playbackCompat.js
 * ----------------------------------------------------------------------
 * DISABLED — no FFmpeg/compression pipeline remains anywhere in this app.
 *
 * This module used to detect videos in browser-unfriendly containers/
 * codecs (MKV/AVI/MOV/HEVC, etc. — the "audio plays, video is black"
 * class of bug) and fix them with a background download -> FFmpeg
 * transcode -> re-upload, swapping the Firestore doc over to the fixed
 * copy. Per an explicit decision to remove all compression/re-encoding
 * (the bot must only ever download and re-upload the exact original
 * file — see queue/pipeline.js's file header), that transcode is gone.
 *
 * scheduleCompatibilityCheck() and looksIncompatible() are kept as
 * no-ops (rather than removed) purely so routes/stream.js doesn't need
 * to change — it calls scheduleCompatibilityCheck() fire-and-forget
 * (no return value used) on every stream request. With this disabled,
 * a video in an incompatible container/codec will keep streaming its
 * original bytes exactly as uploaded, and may still hit that inline-
 * playback bug in some browsers/WebViews. That trade-off is intentional
 * here: zero re-encoding anywhere, full stop.
 *
 * Firestore fields this used to write (playbackCompatible, transcoding,
 * transcodedAt) are simply never set now; existing docs that already
 * have them are unaffected, and nothing depends on them being present.
 * ----------------------------------------------------------------------
 */
'use strict';

// Kept for backward compatibility with any caller that still checks a
// container/mime hint — it does no downloading or transcoding itself.
const INCOMPATIBLE_MIME = [/matroska/i, /x-msvideo/i, /quicktime/i, /webm/i, /x-flv/i, /mpegts/i];
const INCOMPATIBLE_EXT = /\.(mkv|avi|mov|wmv|flv|ts|m2ts)$/i;

function looksIncompatible(mimeType, fileName) {
  if (INCOMPATIBLE_MIME.some((re) => re.test(mimeType || ''))) return true;
  if (INCOMPATIBLE_EXT.test(fileName || '')) return true;
  return false;
}

/**
 * No-op — see file header. Never downloads, transcodes, or writes to
 * Firestore. Safe to call exactly as before; it simply does nothing now.
 * @param {string} videoId
 * @param {object} video the Firestore video doc
 * @param {{mimeType?:string, fileName?:string}} source from mtproto.resolveVideoSource()
 */
function scheduleCompatibilityCheck(_videoId, _video, _source) {
  // Intentionally empty.
}

module.exports = { scheduleCompatibilityCheck, looksIncompatible };
