const express = require('express');
const cors = require('cors');
const helmet = require('helmet');
const morgan = require('morgan');
const { z } = require('zod');
const db = require('./db');
const { migrate } = require('./migrate');

const app = express();
app.use(helmet());
app.use(express.json({ limit: '2mb' }));
app.use(morgan('tiny'));

// CORS: set ALLOWED_ORIGINS to a comma-separated list of desktop/web origins
const allowed = (process.env.ALLOWED_ORIGINS || '').split(',').map(s => s.trim()).filter(Boolean);
app.use(cors({
  origin: (origin, cb) => {
    if (!origin) return cb(null, true); // allow non-browser clients
    if (allowed.length === 0 || allowed.includes(origin)) return cb(null, true);
    return cb(new Error('CORS blocked'));
  }
}));

// Simple API key auth (recommended when exposed on the internet)
function requireApiKey(req, res, next) {
  const required = process.env.API_KEY;
  if (!required) return next();
  const got = req.header('x-api-key');
  if (got && got === required) return next();
  return res.status(401).json({ ok: false, error: 'unauthorized' });
}

app.get('/health', async (req, res) => {
  try {
    const r = await db.query('select 1 as ok');
    res.json({ ok: true, db: r.rows[0].ok });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

// --- Example endpoints (replace with your real Acrilsoft routes) ---
// This gives you a working baseline to prove multi-PC shared DB is ok.

app.get('/api/ping', requireApiKey, (req, res) => {
  res.json({ ok: true, ts: new Date().toISOString() });
});

// Simple shared "notes" table to validate multi-user writes immediately.
app.get('/api/notes', requireApiKey, async (req, res) => {
  const r = await db.query('select id, text, created_at from notes order by id desc limit 200');
  res.json({ ok: true, rows: r.rows });
});

app.post('/api/notes', requireApiKey, async (req, res) => {
  const schema = z.object({ text: z.string().min(1).max(500) });
  const body = schema.parse(req.body);
  const r = await db.query('insert into notes(text) values($1) returning id, text, created_at', [body.text]);
  res.json({ ok: true, row: r.rows[0] });
});

const port = Number(process.env.PORT || 10000);

(async () => {
  try {
    if (process.env.MIGRATE_ON_START === '1') {
      await migrate();
      console.log('Migrations applied');
    }
    app.listen(port, () => console.log(`API listening on :${port}`));
  } catch (e) {
    console.error('Failed to start:', e);
    process.exit(1);
  }
})();
