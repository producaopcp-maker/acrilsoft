// Minimal migration runner (no external migration framework)
// Runs schema.sql on deploy/start if MIGRATE_ON_START=1
const fs = require('fs');
const path = require('path');
const db = require('./db');

async function migrate() {
  const p = path.join(__dirname, '..', 'schema.sql');
  const sql = fs.readFileSync(p, 'utf8');
  if (!sql.trim()) return;
  await db.query(sql);
}

module.exports = { migrate };
