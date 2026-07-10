/**
 * scripts/generateSession.js
 * One-time, non-interactive helper: logs the MTProto client in with the
 * bot token and prints a STRING_SESSION to paste into your environment
 * as TELEGRAM_STRING_SESSION. Run locally with:
 *
 *   TELEGRAM_API_ID=... TELEGRAM_API_HASH=... TELEGRAM_BOT_TOKEN=... \
 *     node scripts/generateSession.js
 */
'use strict';

require('dotenv').config();
const { TelegramClient } = require('telegram');
const { StringSession } = require('telegram/sessions');

async function main() {
  const apiId = parseInt(process.env.TELEGRAM_API_ID, 10);
  const apiHash = process.env.TELEGRAM_API_HASH;
  const botToken = process.env.TELEGRAM_BOT_TOKEN;

  if (!apiId || !apiHash || !botToken) {
    console.error('❌ Missing TELEGRAM_API_ID, TELEGRAM_API_HASH, or TELEGRAM_BOT_TOKEN in your environment.');
    process.exit(1);
  }

  const client = new TelegramClient(new StringSession(''), apiId, apiHash, { connectionRetries: 5 });
  await client.start({ botAuthToken: botToken });

  const sessionString = client.session.save();
  console.log('\n✅ Success! Add this to your environment as TELEGRAM_STRING_SESSION:\n');
  console.log(sessionString);
  console.log('\nKeep it secret — it grants full access to your bot account.\n');

  await client.disconnect();
  process.exit(0);
}

main().catch((err) => {
  console.error('❌ Session generation failed:', err.message);
  process.exit(1);
});
