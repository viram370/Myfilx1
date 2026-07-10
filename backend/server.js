'use strict';

require('dotenv').config();
const express = require('express');
const cors = require('cors');
const helmet = require('helmet');
const morgan = require('morgan');
const rateLimit = require('express-rate-limit');

const { initFirebase, healthCheck: firestoreHealthCheck } = require('./services/firebase');
const botService = require('./services/bot');
const mtproto = require('./services/mtproto');
const { makeLogger, recentLogs } = require('./utils/logger');
const log = makeLogger('server.js');

const videosRoutes = require('./routes/videos');
const videoRoutes = require('./routes/video');
const categoriesRoutes = require('./routes/categories');
const searchRoutes = require('./routes/search');
const historyRoutes = require('./routes/history');
const favoritesRoutes = require('./routes/favorites');
const streamRoutes = require('./routes/stream');
const usersRoutes = require('./routes/users');
const adminRoutes = require('./routes/admin');
const webhookRoutes = require('./routes/webhook');

const app = express();
const PORT = process.env.PORT || 3000;

// Render sits behind a reverse proxy — trust it so req.protocol/req.ip and
// express-rate-limit see the real client, not the proxy hop.
app.set('trust proxy', 1);

async function bootstrap() {
  try {
    initFirebase();
  } catch (err) {
    log.error('bootstrap', 'Firebase init failed — exiting', err);
    process.exit(1);
  }

  try {
    await botService.initBot();
  } catch (err) {
    log.error('bootstrap', 'Bot init failed (continuing without Telegram bot)', err);
  }

  try {
    await mtproto.initMTProto();
  } catch (err) {
    log.error('bootstrap', 'MTProto init failed (streaming will be degraded)', err);
  }

  app.use(helmet({ crossOriginEmbedderPolicy: false, crossOriginResourcePolicy: { policy: 'cross-origin' } }));
  app.use(cors({ origin: true, credentials: true }));

  const apiLimiter = rateLimit({ windowMs: 15 * 60_000, max: 300, standardHeaders: true, legacyHeaders: false });

  app.use(morgan('dev', { skip: (req) => req.path === '/webhook' }));
  app.use('/webhook', express.raw({ type: 'application/json', limit: '2mb' }));
  app.use(express.json({ limit: '10mb' }));

  app.use('/webhook', webhookRoutes);
  app.use('/api/videos', apiLimiter, videosRoutes);
  app.use('/api/video', apiLimiter, videoRoutes);
  app.use('/api/categories', apiLimiter, categoriesRoutes);
  app.use('/api/search', apiLimiter, searchRoutes);
  app.use('/api/history', apiLimiter, historyRoutes);
  app.use('/api/favorites', apiLimiter, favoritesRoutes);
  // stream.js applies its own dedicated limiters per sub-route (the raw
  // file endpoint needs a much higher ceiling than the JSON resolver).
  app.use('/api/stream', streamRoutes);
  app.use('/api/users', apiLimiter, usersRoutes);
  app.use('/api/admin', apiLimiter, adminRoutes);

  app.get('/health', async (req, res) => {
    const [firestore] = await Promise.all([firestoreHealthCheck()]);
    const mt = mtproto.health();
    const mem = process.memoryUsage();
    const healthy = firestore.ok;
    res.status(healthy ? 200 : 503).json({
      status: healthy ? 'ok' : 'degraded',
      uptimeSeconds: Math.round(process.uptime()),
      firestore,
      mtproto: mt,
      memory: { rssMB: Math.round(mem.rss / 1024 / 1024), heapUsedMB: Math.round(mem.heapUsed / 1024 / 1024) },
    });
  });

  app.get('/health/logs', (req, res) => {
    // Lightweight, unauthenticated-but-low-value ops endpoint — intended
    // for platform log aggregation, not for exposing user data.
    res.json({ logs: recentLogs(50) });
  });

  app.use((req, res) => res.status(404).json({ error: 'Route not found' }));

  // Centralized error handler — guarantees no unhandled route error can
  // crash the process or leak a raw stack trace to the client.
  // eslint-disable-next-line no-unused-vars
  app.use((err, req, res, next) => {
    log.error('errorHandler', 'Unhandled route error', err, { path: req.path, method: req.method });
    if (res.headersSent) return;
    res.status(500).json({ error: 'Internal server error' });
  });

  const server = app.listen(PORT, () => {
    log.success('bootstrap', `MYFLIX backend running on port ${PORT}`);
  });

  // Prevent Render from killing slow/large-file connections prematurely,
  // while still bounding idle sockets to avoid leaks.
  server.keepAliveTimeout = 65_000;
  server.headersTimeout = 66_000;
  server.requestTimeout = 0; // long video streams must not be forcibly cut off

  const shutdown = async (signal) => {
    log.warn('shutdown', `Received ${signal} — shutting down gracefully`);
    server.close(() => log.info('shutdown', 'HTTP server closed'));
    await mtproto.shutdown();
    setTimeout(() => process.exit(0), 3000).unref();
  };
  process.on('SIGTERM', () => shutdown('SIGTERM'));
  process.on('SIGINT', () => shutdown('SIGINT'));
}

process.on('uncaughtException', (err) => {
  log.error('process', 'uncaughtException — process kept alive', err);
});
process.on('unhandledRejection', (reason) => {
  const err = reason instanceof Error ? reason : new Error(String(reason));
  log.error('process', 'unhandledRejection — process kept alive', err);
});

bootstrap();
