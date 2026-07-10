/**
 * routes/webhook.js — receives Telegram bot updates in production
 */
'use strict';

const express = require('express');
const router = express.Router();
const crypto = require('crypto');
const { processUpdate } = require('../services/bot');
const { makeLogger } = require('../utils/logger');
const log = makeLogger('routes/webhook.js');

function timingSafeEqual(a, b) {
  const bufA = Buffer.from(String(a || ''));
  const bufB = Buffer.from(String(b || ''));
  if (bufA.length !== bufB.length) return false;
  return crypto.timingSafeEqual(bufA, bufB);
}

router.post('/', (req, res) => {
  try {
    const secretToken = req.headers['x-telegram-bot-api-secret-token'];
    if (process.env.WEBHOOK_SECRET && !timingSafeEqual(secretToken, process.env.WEBHOOK_SECRET)) {
      log.warn('receive', 'Rejected webhook with invalid secret token', { ip: req.ip });
      return res.status(401).json({ error: 'Unauthorized' });
    }

    let update;
    try {
      const body = Buffer.isBuffer(req.body) ? req.body.toString('utf8') : JSON.stringify(req.body);
      update = JSON.parse(body);
    } catch (parseErr) {
      log.error('receive', 'Failed to parse webhook body', parseErr);
      return res.status(400).json({ error: 'Invalid payload' });
    }

    setImmediate(() => {
      try {
        processUpdate(update);
      } catch (err) {
        log.error('process', 'processUpdate threw', err, { updateId: update?.update_id });
      }
    });

    res.json({ ok: true });
  } catch (err) {
    log.error('receive', 'Webhook handler error', err);
    res.status(500).json({ error: 'Webhook error' });
  }
});

module.exports = router;
