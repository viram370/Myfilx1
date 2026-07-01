# MYFLIX — Telegram Mini App (Upgrade)

This upgrades your existing MYFLIX frontend into a fully working Telegram Mini App,
backed by a Node.js/Express + Firebase Firestore backend, with the original
Netflix-style UI **left completely intact** (every CSS rule, every screen, the
card/hero/modal/episode/search/profile/player components are all the same code
you already had — only the data source changed, from hardcoded demo arrays to
real API calls).

## What changed in `public/index.html`

Only three things were touched:

1. **`<head>`** — added the Telegram WebApp SDK script and a `window.MYFLIX_API`
   config variable.
2. **Demo data block** — the `DEMO_ANIME`/`DEMO_SERIES`/`DEMO_MOVIES` arrays and
   the `thumb()` gradient generator were replaced with a small `Api` client and
   a `fetchAllVideos()` function that populates the *same* `CAT` object
   (`{anime, series, movies, cartoons}`) your UI already reads from.
3. **Three functions patched in place**: `playVideo()` (now calls
   `/api/stream/:id`, resumes from saved progress, autosaves every 8s),
   `toggleFav()` (now persists to `/api/favorites`), and the episode-row click
   handler inside `buildEps()` (now plays the *specific* episode's own video,
   not the parent series — this was a latent bug in the demo version where
   every episode played the same placeholder).

Everything else — every CSS rule, every HTML section, `mkCard`, `fillCar`,
`fillGrid`, `showDetail`, `renderHero`, `renderMiniHero`, search, bottom nav,
profile page, section pages — is byte-for-byte what you already had.

---

## Project Structure

```
myflix/
├── public/
│   └── index.html              # Your existing UI, lightly patched (see above)
├── backend/
│   ├── server.js
│   ├── package.json
│   ├── .env.example
│   ├── services/
│   │   ├── firebase.js         # Admin SDK + Firestore helpers
│   │   └── bot.js              # Telegram bot: ingestion + admin commands
│   ├── middleware/
│   │   └── auth.js             # Telegram initData HMAC validation
│   ├── utils/
│   │   └── serialize.js        # Firestore doc -> frontend's expected shape
│   └── routes/
│       ├── videos.js           # GET /api/videos
│       ├── video.js            # GET /api/video/:id
│       ├── categories.js       # GET /api/categories
│       ├── search.js           # GET /api/search
│       ├── history.js          # POST/GET/DELETE /api/history
│       ├── favorites.js        # POST/GET/DELETE /api/favorites
│       ├── stream.js           # GET /api/stream/:id (+ progress)
│       ├── users.js            # GET /api/users/me, continue-watching
│       ├── admin.js            # Protected CRUD + stats
│       └── webhook.js          # Telegram bot webhook receiver
├── firebase.json
├── firestore.rules
├── firestore.indexes.json
└── .firebaserc
```

---

## REST API (exactly as requested)

| Method | Path | Purpose |
|---|---|---|
| GET | `/api/videos` | All published videos, grouped into series with `seasons[]` |
| GET | `/api/video/:id` | Single video detail + full episode list if part of a series |
| POST | `/api/history` | Log a watch event `{ videoId, title }` |
| POST | `/api/favorites` | Add a favorite `{ videoId }` |
| GET | `/api/categories` | Distinct categories with counts |
| GET | `/api/search?q=&category=` | Search by title/description/genre/category |

Additional supporting endpoints the frontend also calls:
`GET/DELETE /api/history`, `GET/DELETE /api/favorites/:id`,
`GET /api/favorites/check/:id`, `GET /api/stream/:id`,
`POST/GET /api/stream/:id/progress`, `GET /api/users/me`,
`GET /api/users/me/continue-watching`.

All `/api/*` routes that touch personal data (history, favorites, stream,
continue-watching) validate the Telegram `initData` signature sent in the
`x-telegram-init-data` header. `/api/videos`, `/api/video/:id`,
`/api/categories`, and `/api/search` work read-only without auth for fast
browsing, but still upsert the user record if `initData` is present.

---

## Firestore Structure

```
videos/{id}
  title, description, category, season, episode, seriesTitle
  poster, banner, thumbnail            // resolved at request time, not stored
  telegram_file_id, telegram_unique_file_id
  thumbFileId, bannerFileId            // Telegram file_ids for images
  duration, size, mimeType
  views, likes
  uploadDate, published, uploadedBy

users/{telegramId}
  telegramId, firstName, lastName, username, languageCode, lastSeen

favorites/{userId_videoId}
  userId, videoId, addedAt

history/{autoId}
  userId, videoId, title, watchedAt

continueWatching/{userId_videoId}
  userId, videoId, position, duration, progressPercent, completed, watchedAt

categories/{name}            // optional — current build derives categories
  count, icon                // dynamically from videos.category instead
```

> Firestore security rules deny **all** direct client access — every read/write
> goes through the backend's Firebase Admin SDK, which bypasses rules entirely.
> This matches the "stream only via authorized backend" requirement.

---

## Bot: Adding a Video (full metadata flow)

1. Send or forward a video to your bot (as admin).
2. The bot replies with the captured Telegram metadata: `file_id`,
   `file_unique_id`, duration, resolution, size.
3. It then asks, in order: **Title → Category → Season → Episode →
   Description → Banner image**.
   - Category is chosen via inline buttons (Anime / Web Series / Movies / Cartoons).
   - Movies skip season/episode and go straight to description.
   - Send `/skip` to save without a banner image.
4. On save, you're offered **Publish Now** or **Keep Draft**.

### Admin Commands

| Command | Action |
|---|---|
| `/add` | Add manually by pasting a `file_id` |
| `/edit <id>` | Edit `title/description/category/season/episode/poster/banner/thumbnail` |
| `/delete <id>` | Delete with confirmation |
| `/publish <id>` / `/unpublish <id>` | Toggle visibility |
| `/stats` | Videos, views, likes, users, watch events |
| `/users` | Recent users |
| `/list` | Recent videos with IDs |
| `/done` | Finish an `/edit` session |
| `/skip` | Skip the optional banner step during upload |

---

## Setup

### 1. Telegram Bot
Create via [@BotFather](https://t.me/BotFather), save the token. Get your
numeric Telegram ID from [@userinfobot](https://t.me/userinfobot) for
`ADMIN_USER_IDS`.

### 2. Firebase
```bash
npm install -g firebase-tools
firebase login
firebase use YOUR_PROJECT_ID
firebase deploy --only firestore:rules,firestore:indexes
```
Generate a service account key: Firebase Console → Project Settings →
Service Accounts → Generate new private key.

### 3. Backend
```bash
cd backend
cp .env.example .env
# fill in TELEGRAM_BOT_TOKEN, ADMIN_USER_IDS, FIREBASE_*, MINI_APP_URL, etc.
npm install
npm run dev      # local polling mode
# or
npm start         # production (set NODE_ENV=production, uses webhook)
```

Deploy anywhere that runs Node (Railway, Render, a VPS with PM2 + Nginx).
After deploying, set the webhook:
```bash
curl -X POST "https://api.telegram.org/bot<TOKEN>/setWebhook" \
  -H "Content-Type: application/json" \
  -d '{"url":"https://your-backend-domain.com/webhook","secret_token":"<WEBHOOK_SECRET>"}'
```

### 4. Frontend (Firebase Hosting)
Edit the one line near the top of `public/index.html`:
```js
window.MYFLIX_API = 'https://your-backend-domain.com/api';
```
Then:
```bash
firebase deploy --only hosting
```

### 5. Connect Bot → Mini App
BotFather → your bot → Bot Settings → Menu Button → set URL to your
Firebase Hosting URL (e.g. `https://your-project.web.app`).

---

## Notes on Telegram File Streaming

- `getFile` URLs are valid for about an hour; the backend resolves a fresh URL
  on every `/api/stream/:id` call and caches it in memory for ~50 minutes to
  avoid redundant Telegram API calls.
- Telegram's Bot API caps `getFile` at 20MB for bots (without a local Bot API
  server). Files larger than that will return a 413 from `/api/stream/:id` —
  for big libraries, consider running a self-hosted Bot API server, which
  raises this limit substantially.
- Poster/thumbnail images are resolved the same way (via `bannerFileId` /
  `thumbFileId`), with the same in-memory cache to keep list views fast.

---

## Performance Notes

- `GET /api/videos` groups multi-episode docs into series cards server-side,
  so the frontend never has to do that work.
- Image URL resolution is cached per `file_id` for ~50 minutes, avoiding a
  Telegram API round-trip on every page load.
- Firestore composite indexes are predefined in `firestore.indexes.json` for
  every compound query the routes issue (published+category+createdAt,
  userId+watchedAt, userId+completed+watchedAt, etc).
- Search is in-memory over already-fetched published docs — fine at the
  catalog sizes a personal/licensed library typically has; if your catalog
  grows very large, swap in Algolia or Typesense behind the same
  `/api/search` route without touching the frontend.

---

## Security

- This app is intended only for streaming videos you own or are licensed to
  distribute.
- All Firestore access is gated behind the backend; rules deny direct client
  reads/writes.
- `/api/admin/*` requires either a valid admin Telegram `initData` or the
  `x-admin-key` header matching `JWT_SECRET` (for server-to-server tooling).
- `telegram_file_id` is never sent to the frontend — only resolved, short-lived
  CDN URLs are.
