require('dotenv').config();
const express = require('express');
const cors = require('cors');
const helmet = require('helmet');
const morgan = require('morgan');
const rateLimit = require('express-rate-limit');

const { initFirebase } = require('./services/firebase');
const botService = require('./services/bot');

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
botService.initBot();

app.use(helmet({ crossOriginEmbedderPolicy: false }));
app.use(cors({ origin: true, credentials: true }));

const apiLimiter = rateLimit({ windowMs: 15*60*1000, max: 300 });
const streamLimiter = rateLimit({ windowMs: 60*1000, max: 40 });

app.use(morgan('dev', { skip: req => req.path === '/webhook' }));
app.use('/webhook', express.raw({ type: 'application/json' }));
app.use(express.json({ limit: '10mb' }));

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

app.get('/health', (req, res) => res.json({ status: 'ok' }));

app.use((req, res) => res.status(404).json({ error: 'Route not found' }));

app.listen(PORT, () => {
  console.log(`✅ MYFLIX backend running on port ${PORT}`);
});
