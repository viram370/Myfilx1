/**
 * MYFLIX Backend — Entry Point (FIXED)
 */
require('dotenv').config();
const express = require('express');
const cors = require('cors');
const helmet = require('helmet');
const morgan = require('morgan');
const rateLimit = require('express-rate-limit');

const { initFirebase } = require('./services/firebase');
const botService = require('./services/bot'); // Fixed import

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

initFirebase();
botService.initBot(); // Fixed call

app.use(helmet({ crossOriginEmbedderPolicy: false }));
app.use(cors({
  origin: [
    process.env.MINI_APP_URL,
    'http://localhost:8080',
    'https://web.telegram.org',
    'https://t.me',
    /\.telegram\.org$/,
  ],
  credentials: true,
}));

const apiLimiter = rateLimit({
  windowMs: 15 * 60 * 1000,
  max: 300,
});

const streamLimiter = rateLimit({ windowMs: 60 * 1000, max: 40 });

app.use(morgan(process.env.NODE_ENV === 'production' ? 'combined' : 'dev', {
  skip: (req) => req.path === '/webhook',
}));

app.use('/webhook', express.raw({ type: 'application/json' }));
app.use(express.json({ limit: '10mb' }));
app.use(express.urlencoded({ extended: true }));

// Routes
app.use('/webhook', webhookRoutes);
app.use('/api/videos', apiLimiter, videosRoutes);
app.use('/api/video', apiLimiter, videoRoutes);
app.use('/api/categories', apiLimiter, categoriesRoutes);
app.use('/api/search', apiLimiter, searchRoutes);
app.use('/api/history', apiLimiter, historyRoutes);
app.use('/api/favorites', apiLimiter, favoritesRoutes);
app.use('/api/stream', streamLimiter, streamRoutes);
app.use('/api/users', apiLimiter, usersRoutes);
app.use('/api/admin', apiLimiter, adminRoutes);

app.get('/health', (req, res) => res.json({ status: 'ok', time: new Date().toISOString() }));

app.use((req, res) => res.status(404).json({ error: 'Route not found' }));

app.use((err, req, res, next) => {
  console.error('[ERROR]', err.stack);
  res.status(500).json({ error: 'Internal server error' });
});

app.listen(PORT, () => {
  console.log(`✅ MYFLIX backend running on port ${PORT}`);
  console.log(`📱 Mini App URL: ${process.env.MINI_APP_URL || 'Not configured'}`);
});

module.exports = app;
