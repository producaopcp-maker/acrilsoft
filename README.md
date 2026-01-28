# Acrilsoft API (Render Free)

This is a starter **API backend** meant to be deployed on **Render Free** with a managed **Render Postgres** database.

Why: your Electron app was timing out when connecting to Postgres directly. With this approach:

**Desktop (Electron) → HTTPS → API → Postgres**

## What you get
- `render.yaml` blueprint creates:
  - 1 Render Web Service (free)
  - 1 Render Postgres DB (free)
- `/health` endpoint to verify DB
- `/api/notes` as a quick shared-write test between multiple PCs

## Important limitations on Render Free
- Free web services sleep after inactivity (cold start on first request).
- Free instances have limits; don’t treat as production-grade.

## Deploy
1. Put this folder in a GitHub repo.
2. In Render: **New → Blueprint** and select the repo.
3. After creation:
   - Set `API_KEY` (secret) in the service env vars.
   - Optionally set `ALLOWED_ORIGINS`.

## Test
- Open: `https://<your-service>.onrender.com/health`
- With API key:
  - GET `https://<service>/api/notes` header `x-api-key: ...`
  - POST `https://<service>/api/notes` JSON `{ "text": "teste" }`

## Next step
We’ll migrate your real modules (Produtos/Estoque → OP → Brindes) from SQLite to Postgres inside this API.
Then the Electron app will call these endpoints instead of accessing the database.
