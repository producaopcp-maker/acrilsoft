import express from "express";
import pkg from "pg";

const { Pool } = pkg;
const app = express();

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false },
});

app.get("/health", (req, res) => {
  res.json({ status: "ok" });
});

app.get("/api/ping", async (req, res) => {
  const r = await pool.query("SELECT 1 AS ok");
  res.json(r.rows[0]);
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log("API rodando na porta", PORT);
});
