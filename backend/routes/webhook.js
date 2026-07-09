/**
 * Webhook Route — receives Telegram bot updates in production
 */
const express = require('express');
const router = express.Router();
const { processUpdate } = require('../services/bot');

router.post('/', (req, res) => {
  try {
    const secretToken = req.headers['x-telegram-bot-api-secret-token'];
    if (process.env.WEBHOOK_SECRET && secretToken !== process.env.WEBHOOK_SECRET) {
      return res.status(401).json({ error: 'Unauthorized' });
    }

    let update;
    try {
      const body = Buffer.isBuffer(req.body) ? req.body.toString('utf8') : JSON.stringify(req.body);
      update = JSON.parse(body);
    } catch { update = req.body; }

    setImmediate(() => {
      console.log("Telegram Update:");
console.log(JSON.stringify(update, null, 2));
      try { processUpdate(update); }
      catch (err) { console.error('[webhook] process error:', err.message); }
    });

    res.json({ ok: true });
  } catch (err) {
    console.error('[webhook] error:', err.message);
    res.status(500).json({ error: 'Webhook error' });
  }
});

module.exports = router;
