// Banco de dados
// - LOCAL: SQLite (libsql) em %APPDATA%\Estoque Acrilico\database.sqlite
// - ONLINE (recomendado): PostgreSQL (Railway/Render) via PG_URL / DATABASE_URL (baixa latência no Brasil)
//   Configure via:
//     - %APPDATA%\\Estoque Acrilico\\config.json
//       { "pgUrl": "postgresql://user:pass@host:6543/postgres", "pgSsl": true }
//     - ou variáveis de ambiente: PG_URL / DATABASE_URL  (e opcional PG_SSL=true)
//
// OBS: Mantemos o modo local para funcionar offline.
// Nesta versão o sistema inicia SEMPRE no modo LOCAL e só tenta o ONLINE se o usuário habilitar
// explicitamente em Configurações → Banco de Dados (onlineEnabled=true).
const Database = require("libsql");
const { Pool } = require("pg");
const path = require("path");
const fs = require("fs");
const os = require("os");

function getAppDataDir() {
  // Desktop (Windows): mantém compatível com instalações antigas (%APPDATA%\Estoque Acrilico)
  if (process.platform === "win32") {
    const base = process.env.APPDATA || path.join(os.homedir(), "AppData", "Roaming");
    const dir = path.join(base, "Estoque Acrilico");
    if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
    return dir;
  }

  // Webapp/servidor (Linux/macOS): usa uma pasta local do projeto (ou DATA_DIR)
  const base = process.env.DATA_DIR || path.join(process.cwd(), "data");
  const dir = path.join(base, "estoque-acrilico");
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
  return dir;
}


function readConfigJson() {
  try {
    const cfgPath = path.join(getAppDataDir(), "config.json");
    if (!fs.existsSync(cfgPath)) return null;
    const raw = fs.readFileSync(cfgPath, "utf8");
    const cfg = JSON.parse(raw);
    return cfg && typeof cfg === "object" ? cfg : null;
  } catch (e) {
    console.warn("Falha ao ler config.json:", e.message);
    return null;
  }
}

const cfg = readConfigJson() || {};

// Postgres (Railway/Render)
const PG_URL = process.env.PG_URL || process.env.DATABASE_URL || cfg.pgUrl;
const PG_SSL = String(process.env.PG_SSL || cfg.pgSsl || "").toLowerCase() === "true";
const ONLINE_ENABLED = String(process.env.ONLINE_ENABLED || cfg.onlineEnabled || "").toLowerCase() === "true";

const dbPath = path.join(getAppDataDir(), "database.sqlite");

let DB_MODE = "local";
let db; // wrapper final exportado
let authDb; // SQLite local usado para autenticação/emergência (não troca para Postgres)
let pgPool = null;
let ONLINE_OK = false;

// IMPORTANTE (arquitetura offline-first):
// O sistema atual foi construído em cima de uma API SINCRONA (prepare().get/all/run)
// típica de SQLite/better-sqlite3/libsql.
// O driver do Postgres (pg) é ASSÍNCRONO e, para usar o Postgres como banco "principal",
// seria necessário refatorar as rotas/queries para async/await.
// Para evitar travamentos e manter o app responsivo, nesta versão o SQLite local é SEMPRE
// o banco operacional. A conexão Postgres serve para:
//  - testar conectividade (status ONLINE/OFFLINE)
//  - preparar o caminho para sincronização (upgrade seguinte)

function withTimeout(promise, ms, label = "operation") {
  return Promise.race([
    promise,
    new Promise((_, reject) => setTimeout(() => reject(new Error(`${label} timed out after ${ms}ms`)), ms)),
  ]);
}

function toNowText() {
  // YYYY-MM-DD HH:mm:ss
  const d = new Date();
  const pad = (n) => String(n).padStart(2, "0");
  return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())} ${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`;
}

function makePgWrapper(pool) {
  function convertSql(sql) {
    let s = String(sql);

    // SQLite -> Postgres compat
    s = s.replace(/\bIFNULL\s*\(/gi, "COALESCE(");

    // datetime('now') / date('now')
    s = s.replace(/datetime\s*\(\s*'now'\s*\)/gi, "to_char(now(), 'YYYY-MM-DD HH24:MI:SS')");
    s = s.replace(/date\s*\(\s*'now'\s*\)/gi, "to_char(now(), 'YYYY-MM-DD')");

    // Placeholders ? -> $1..$n
    let i = 0;
    s = s.replace(/\?/g, () => {
      i += 1;
      return `$${i}`;
    });

    // ALTER TABLE add column: compat
    s = s.replace(/ADD COLUMN\s+([a-zA-Z0-9_]+)\s+/i, (m, col) => `ADD COLUMN IF NOT EXISTS ${col} `);

    return s;
  }

  async function pgExec(sql) {
    const s = convertSql(sql);
    return pool.query(s);
  }

  function pgPrepare(sql) {
    const baseSql = String(sql);

    return {
      async get(...params) {
        const s = convertSql(baseSql);
        const r = await pool.query(s, params);
        return r.rows && r.rows[0] ? r.rows[0] : undefined;
      },
      async all(...params) {
        const s = convertSql(baseSql);
        const r = await pool.query(s, params);
        return r.rows || [];
      },
      async run(...params) {
        let s = convertSql(baseSql);

        // Emula libsql: INSERT retorna lastInsertRowid
        const isInsert = /^\s*insert\s+/i.test(s);
        const hasReturning = /\breturning\b/i.test(s);
        if (isInsert && !hasReturning) s = `${s} RETURNING id`;

        const r = await pool.query(s, params);
        const changes = r.rowCount || 0;
        const lastInsertRowid = (r.rows && r.rows[0] && (r.rows[0].id ?? r.rows[0].ID)) || undefined;

        return { changes, lastInsertRowid };
      },
    };
  }

  return {
    mode: "online_pg",
    async exec(sql) {
      return pgExec(sql);
    },
    prepare(sql) {
      return pgPrepare(sql);
    },
    // no-op pragmas
    pragma() {},
    // close
    async close() {
      await pool.end();
    },
  };
}

function makeSqliteDb() {
  // Modo offline/local: sempre SQLite local.
  console.log("Banco LOCAL (SQLite) em:", dbPath);
  DB_MODE = "local";
  return new Database(dbPath);
}

async function init() {
  // SEMPRE inicia local (não bloqueia o Electron / tela de login)
  db = makeSqliteDb();
  // authDb fica sempre apontando para o SQLite local, mesmo quando o app troca para Postgres.
  // Isso evita travas no login quando o Postgres estiver lento/instável.
  authDb = db;
  try { db.pragma("journal_mode = WAL"); } catch (_) {}
  try { db.pragma("foreign_keys = ON"); } catch (_) {}
  ensureSchemaSqlite();
  seedSqlite();

  // Offline-first:
  // - O app SEMPRE opera em SQLite local (responsivo, sem travar)
  // - Se o usuário habilitar "Conectar no ONLINE ao iniciar" (onlineEnabled=true),
  //   tentamos abrir conexão com o Postgres em BACKGROUND (sem bloquear o Electron).
  if (ONLINE_ENABLED && PG_URL) {
    console.log("ONLINE habilitado: tentando conectar ao Postgres em background...");
    connectPgInBackground().catch((e) => {
      ONLINE_OK = false;
      console.warn("⚠️ Falha ao conectar no Postgres no boot (background):", e?.message || e);
    });
  }
}

function getAuthDb() {
  return authDb || db;
}

async function connectPgInBackground() {
  // cria pool sob demanda
  if (!pgPool) {
    pgPool = new Pool({
      connectionString: PG_URL,
      ssl: PG_SSL ? { rejectUnauthorized: false } : undefined,
      connectionTimeoutMillis: 8000,
      idleTimeoutMillis: 30000,
      max: 5,
      allowExitOnIdle: true,
    });
  }
  try {
    await withTimeout(pgPool.query("SELECT 1"), 8000, "Postgres connect");
    ONLINE_OK = true;
    DB_MODE = "local"; // operação continua local
    console.log("✅ Postgres ONLINE disponível (conectado).");
    return true;
  } catch (e) {
    ONLINE_OK = false;
    console.warn("⚠️ Postgres indisponível. Mantendo modo LOCAL. Motivo:", e?.message || e);
    try { await pgPool.end(); } catch (_) {}
    pgPool = null;
    DB_MODE = "local";
    return false;
  }
}

// API pública: aciona tentativa de conexão no Postgres (sem travar a UI)
async function connectOnline() {
  if (!PG_URL) throw new Error("PG_URL/pgUrl não configurado");
  console.log("Tentando conectar no banco ONLINE (Postgres Railway/Render):", PG_URL.replace(/:\/\/([^:]+):[^@]+@/, "://$1:***@"));
  const ok = await connectPgInBackground();
  return { ok, mode: ok ? "online_pg" : "local" };
}

function getDbMode() {
  return DB_MODE;
}

async function pingDb() {
  try {
    // Sempre valida o SQLite local (operações do app)
    db.prepare("SELECT 1 AS ok").get();

    // Se o ONLINE está conectado (mesmo operando em SQLite), reportamos como ONLINE
    // para o indicador no topo.
    if (ONLINE_OK && pgPool) {
      await withTimeout(pgPool.query("SELECT 1"), 3000, "Postgres ping");
      return { ok: true, mode: "online_pg" };
    }

    return { ok: true, mode: "local" };
  } catch (e) {
    return { ok: false, mode: DB_MODE, error: e?.message || String(e) };
  }
}

/* =========================
   SCHEMA - SQLITE/LIBSQL
   ========================= */
function ensureSchemaSqlite() {
  db.exec(`
CREATE TABLE IF NOT EXISTS fornecedores (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  nome TEXT NOT NULL,
  whatsapp TEXT,
  email TEXT,
  observacao TEXT,
  observacao_interna TEXT,
  observacao_cliente TEXT,
  criado_em TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS produtos (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  codigo_interno TEXT UNIQUE,
  descricao TEXT NOT NULL,
  espessura_mm INTEGER NOT NULL,
  cor TEXT NOT NULL,
  largura_mm INTEGER NOT NULL,
  altura_mm INTEGER NOT NULL,
  localizacao TEXT,
  marca TEXT,
  fornecedor_id INTEGER,
  estoque_atual INTEGER NOT NULL DEFAULT 0,
  estoque_minimo INTEGER NOT NULL DEFAULT 0,
  criado_em TEXT DEFAULT (datetime('now')),
  FOREIGN KEY (fornecedor_id) REFERENCES fornecedores(id)
);

CREATE TABLE IF NOT EXISTS movimentacoes (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  produto_id INTEGER NOT NULL,
  tipo TEXT NOT NULL,
  quantidade INTEGER NOT NULL,
  observacao TEXT,
  observacao_interna TEXT,
  observacao_cliente TEXT,
  criado_em TEXT DEFAULT (datetime('now')),
  FOREIGN KEY (produto_id) REFERENCES produtos(id)
);

CREATE TABLE IF NOT EXISTS ordens_producao (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  codigo_op TEXT UNIQUE NOT NULL,
  cliente TEXT,
  vendedor_nome TEXT,
  status TEXT NOT NULL DEFAULT 'ABERTA',
  prioridade TEXT NOT NULL DEFAULT 'NORMAL',
  produto_final TEXT,
  quantidade_final INTEGER,
  pedido_venda TEXT,
  data_abertura TEXT,
  data_entrega TEXT,
  data_prevista TEXT,
  material_espessura_mm INTEGER,
  material_cor TEXT,
  pecas_json TEXT,
  observacao TEXT,
  observacao_interna TEXT,
  observacao_cliente TEXT,
  criado_em TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS op_logs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  op_id INTEGER NOT NULL,
  usuario_id INTEGER,
  usuario_nome TEXT,
  acao TEXT NOT NULL,
  campo TEXT NOT NULL,
  valor_anterior TEXT,
  valor_novo TEXT,
  criado_em TEXT DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_op_logs_op_id ON op_logs(op_id);

CREATE TABLE IF NOT EXISTS insumos (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  nome TEXT NOT NULL,
  unidade TEXT,
  estoque_atual REAL NOT NULL DEFAULT 0,
  estoque_minimo REAL NOT NULL DEFAULT 0,
  criado_em TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS op_insumos (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  ordem_id INTEGER NOT NULL,
  insumo_id INTEGER NOT NULL,
  quantidade REAL NOT NULL,
  criado_em TEXT DEFAULT (datetime('now')),
  FOREIGN KEY (ordem_id) REFERENCES ordens_producao(id),
  FOREIGN KEY (insumo_id) REFERENCES insumos(id)
);

CREATE TABLE IF NOT EXISTS op_anexos (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  ordem_id INTEGER NOT NULL,
  nome TEXT NOT NULL,
  caminho TEXT NOT NULL,
  mime TEXT,
  criado_em TEXT DEFAULT (datetime('now')),
  FOREIGN KEY (ordem_id) REFERENCES ordens_producao(id)
);
CREATE INDEX IF NOT EXISTS idx_op_anexos_ordem_id ON op_anexos(ordem_id);

CREATE TABLE IF NOT EXISTS settings (
  key TEXT PRIMARY KEY,
  value TEXT
);

/* ===== Usuários (controle de acesso) ===== */
CREATE TABLE IF NOT EXISTS users (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  nome TEXT NOT NULL,
  usuario TEXT NOT NULL UNIQUE,
  senha_hash TEXT NOT NULL,
  role TEXT NOT NULL DEFAULT 'operador',
  modulos TEXT,
  ativo INTEGER NOT NULL DEFAULT 1,
  created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS brindes (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  nome TEXT NOT NULL UNIQUE,
  categoria TEXT,
  unidade TEXT NOT NULL DEFAULT 'UN',
  estoque_atual INTEGER NOT NULL DEFAULT 0,
  estoque_minimo INTEGER NOT NULL DEFAULT 0,
  ativo INTEGER NOT NULL DEFAULT 1,
  criado_em TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS brindes_movimentacoes (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  brinde_id INTEGER NOT NULL,
  tipo TEXT NOT NULL,
  quantidade INTEGER NOT NULL,
  observacao TEXT,
  op_id INTEGER,
  criado_em TEXT DEFAULT (datetime('now')),
  FOREIGN KEY (brinde_id) REFERENCES brindes(id),
  FOREIGN KEY (op_id) REFERENCES ordens_producao(id)
);

CREATE TABLE IF NOT EXISTS op_brindes (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  op_id INTEGER NOT NULL,
  brinde_id INTEGER NOT NULL,
  quantidade INTEGER NOT NULL DEFAULT 1,
  criado_em TEXT DEFAULT (datetime('now')),
  FOREIGN KEY (op_id) REFERENCES ordens_producao(id),
  FOREIGN KEY (brinde_id) REFERENCES brindes(id)
);

/* ===== Brindes (Catálogo + Orçamentos) ===== */
CREATE TABLE IF NOT EXISTS brindes_fornecedores (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  nome TEXT NOT NULL UNIQUE,
  status INTEGER NOT NULL DEFAULT 1,
  observacoes TEXT,
  created_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS brindes_catalogo (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  fornecedor_id INTEGER NOT NULL,
  codigo_fornecedor TEXT NOT NULL,
  nome TEXT NOT NULL,
  descricao TEXT,
  categoria TEXT,
  unidade TEXT NOT NULL DEFAULT 'UN',
  imagem_url TEXT,
  -- vínculo opcional com item do estoque interno (tabela brindes)
  brinde_estoque_id INTEGER,
  ativo INTEGER NOT NULL DEFAULT 1,
  updated_at TEXT DEFAULT (datetime('now')),
  UNIQUE (fornecedor_id, codigo_fornecedor),
  FOREIGN KEY (fornecedor_id) REFERENCES brindes_fornecedores(id)
);
CREATE INDEX IF NOT EXISTS idx_brindes_catalogo_fornecedor ON brindes_catalogo(fornecedor_id);
CREATE INDEX IF NOT EXISTS idx_brindes_catalogo_nome ON brindes_catalogo(nome);
CREATE INDEX IF NOT EXISTS idx_brindes_catalogo_brinde_estoque ON brindes_catalogo(brinde_estoque_id);

CREATE TABLE IF NOT EXISTS brindes_catalogo_precos (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  catalogo_id INTEGER NOT NULL,
  qtd_min INTEGER NOT NULL DEFAULT 1,
  qtd_max INTEGER,
  custo_unitario REAL NOT NULL DEFAULT 0,
  moeda TEXT DEFAULT 'BRL',
  updated_at TEXT DEFAULT (datetime('now')),
  FOREIGN KEY (catalogo_id) REFERENCES brindes_catalogo(id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_brindes_catalogo_precos_cat ON brindes_catalogo_precos(catalogo_id);

CREATE TABLE IF NOT EXISTS brindes_personalizacao_tipos (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  nome TEXT NOT NULL UNIQUE,
  cobra_setup INTEGER NOT NULL DEFAULT 0,
  ativo INTEGER NOT NULL DEFAULT 1
);

CREATE TABLE IF NOT EXISTS brindes_personalizacao_precos (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  tipo_id INTEGER NOT NULL,
  qtd_min INTEGER NOT NULL DEFAULT 1,
  qtd_max INTEGER,
  custo_unitario REAL NOT NULL DEFAULT 0,
  setup_padrao REAL NOT NULL DEFAULT 0,
  updated_at TEXT DEFAULT (datetime('now')),
  FOREIGN KEY (tipo_id) REFERENCES brindes_personalizacao_tipos(id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_brindes_pers_precos_tipo ON brindes_personalizacao_precos(tipo_id);

CREATE TABLE IF NOT EXISTS orcamentos_brindes (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  cliente_id INTEGER,
  cliente_nome TEXT,
  status TEXT NOT NULL DEFAULT 'RASCUNHO',
  validade_em_dias INTEGER NOT NULL DEFAULT 7,
  prazo_entrega_texto TEXT,
  observacoes TEXT,
  subtotal REAL NOT NULL DEFAULT 0,
  desconto REAL NOT NULL DEFAULT 0,
  total REAL NOT NULL DEFAULT 0,
  created_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS orcamentos_brindes_itens (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  orcamento_id INTEGER NOT NULL,
  catalogo_id INTEGER NOT NULL,
  descricao_custom TEXT,
  quantidade INTEGER NOT NULL DEFAULT 1,
  custo_unit_base REAL NOT NULL DEFAULT 0,
  custo_total_base REAL NOT NULL DEFAULT 0,
  margem_percent REAL NOT NULL DEFAULT 0,
  preco_unit_venda REAL NOT NULL DEFAULT 0,
  total_item REAL NOT NULL DEFAULT 0,
  observacoes_item TEXT,
  FOREIGN KEY (orcamento_id) REFERENCES orcamentos_brindes(id) ON DELETE CASCADE,
  FOREIGN KEY (catalogo_id) REFERENCES brindes_catalogo(id)
);
CREATE INDEX IF NOT EXISTS idx_orc_brindes_itens_orc ON orcamentos_brindes_itens(orcamento_id);

CREATE TABLE IF NOT EXISTS orcamentos_brindes_itens_personalizacoes (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  item_id INTEGER NOT NULL,
  tipo_id INTEGER NOT NULL,
  cores INTEGER,
  posicao TEXT,
  tamanho TEXT,
  setup_valor REAL NOT NULL DEFAULT 0,
  custo_unit_personalizacao REAL NOT NULL DEFAULT 0,
  total_personalizacao REAL NOT NULL DEFAULT 0,
  FOREIGN KEY (item_id) REFERENCES orcamentos_brindes_itens(id) ON DELETE CASCADE,
  FOREIGN KEY (tipo_id) REFERENCES brindes_personalizacao_tipos(id)
);
CREATE INDEX IF NOT EXISTS idx_orc_brindes_item_pers_item ON orcamentos_brindes_itens_personalizacoes(item_id);



CREATE INDEX IF NOT EXISTS idx_users_usuario ON users(usuario);


-- --------------------------
-- PEDIDOS (pré-OP)
-- --------------------------
CREATE TABLE IF NOT EXISTS pedidos (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  cliente_nome TEXT,
  cliente_contato TEXT,
  vendedor_nome TEXT,
  prazo_entrega TEXT,
  prioridade TEXT DEFAULT 'NORMAL',
  status TEXT DEFAULT 'ABERTO',
  observacoes_vendedor TEXT,
  observacoes_internas TEXT,
  op_id INTEGER,
  anexo_arquivo TEXT,
  criado_em TEXT DEFAULT (datetime('now')),
  atualizado_em TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS pedido_itens (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  pedido_id INTEGER NOT NULL,
  descricao TEXT,
  quantidade REAL,
  unidade TEXT,
  observacao TEXT,
  criado_em TEXT DEFAULT (datetime('now')),
  FOREIGN KEY (pedido_id) REFERENCES pedidos(id) ON DELETE CASCADE
);

`);

  // migrações leves
  const migs = [
    "ALTER TABLE ordens_producao ADD COLUMN material_espessura_mm INTEGER",
    "ALTER TABLE ordens_producao ADD COLUMN material_cor TEXT",
    "ALTER TABLE ordens_producao ADD COLUMN pecas_json TEXT",
    "ALTER TABLE users ADD COLUMN modulos TEXT",
    "ALTER TABLE ordens_producao ADD COLUMN produto_final TEXT",
    "ALTER TABLE ordens_producao ADD COLUMN quantidade_final INTEGER",
    "ALTER TABLE ordens_producao ADD COLUMN data_abertura TEXT",
    "ALTER TABLE ordens_producao ADD COLUMN data_entrega TEXT",
    "ALTER TABLE ordens_producao ADD COLUMN pedido_venda TEXT",
    "ALTER TABLE ordens_producao ADD COLUMN vendedor_nome TEXT",
    "ALTER TABLE ordens_producao ADD COLUMN brindes_baixados INTEGER NOT NULL DEFAULT 0",
    "ALTER TABLE ordens_producao ADD COLUMN arquivada INTEGER DEFAULT 0",
    "ALTER TABLE ordens_producao ADD COLUMN excluida INTEGER DEFAULT 0",

    // Brindes: vínculo catálogo -> estoque
    "ALTER TABLE brindes_catalogo ADD COLUMN brinde_estoque_id INTEGER",
  ];
  for (const s of migs) { try { db.prepare(s).run(); } catch (_) {} }
}

/* =========================
   SCHEMA - POSTGRES
   ========================= */
async function ensureSchemaPg() {
  // DDL em Postgres (Supabase)
  // Obs: usamos "TEXT" para datas/horários pra manter compatibilidade com o app atual
  await db.exec(`
CREATE TABLE IF NOT EXISTS fornecedores (
  id BIGSERIAL PRIMARY KEY,
  nome TEXT NOT NULL,
  whatsapp TEXT,
  email TEXT,
  observacao TEXT,
  observacao_interna TEXT,
  observacao_cliente TEXT,
  criado_em TEXT DEFAULT (to_char(now(), 'YYYY-MM-DD HH24:MI:SS'))
);

CREATE TABLE IF NOT EXISTS produtos (
  id BIGSERIAL PRIMARY KEY,
  codigo_interno TEXT UNIQUE,
  descricao TEXT NOT NULL,
  espessura_mm INTEGER NOT NULL,
  cor TEXT NOT NULL,
  largura_mm INTEGER NOT NULL,
  altura_mm INTEGER NOT NULL,
  localizacao TEXT,
  marca TEXT,
  fornecedor_id BIGINT REFERENCES fornecedores(id),
  estoque_atual INTEGER NOT NULL DEFAULT 0,
  estoque_minimo INTEGER NOT NULL DEFAULT 0,
  criado_em TEXT DEFAULT (to_char(now(), 'YYYY-MM-DD HH24:MI:SS'))
);

CREATE TABLE IF NOT EXISTS movimentacoes (
  id BIGSERIAL PRIMARY KEY,
  produto_id BIGINT NOT NULL REFERENCES produtos(id),
  tipo TEXT NOT NULL,
  quantidade INTEGER NOT NULL,
  observacao TEXT,
  observacao_interna TEXT,
  observacao_cliente TEXT,
  criado_em TEXT DEFAULT (to_char(now(), 'YYYY-MM-DD HH24:MI:SS'))
);

CREATE TABLE IF NOT EXISTS ordens_producao (
  id BIGSERIAL PRIMARY KEY,
  codigo_op TEXT UNIQUE NOT NULL,
  cliente TEXT,
  vendedor_nome TEXT,
  status TEXT NOT NULL DEFAULT 'ABERTA',
  prioridade TEXT NOT NULL DEFAULT 'NORMAL',
  produto_final TEXT,
  quantidade_final INTEGER,
  pedido_venda TEXT,
  data_abertura TEXT,
  data_entrega TEXT,
  data_prevista TEXT,
  material_espessura_mm INTEGER,
  material_cor TEXT,
  pecas_json TEXT,
  observacao TEXT,
  observacao_interna TEXT,
  observacao_cliente TEXT,
  brindes_baixados INTEGER NOT NULL DEFAULT 0,
  arquivada INTEGER NOT NULL DEFAULT 0,
  excluida INTEGER NOT NULL DEFAULT 0,
  criado_em TEXT DEFAULT (to_char(now(), 'YYYY-MM-DD HH24:MI:SS'))
);

CREATE TABLE IF NOT EXISTS op_logs (
  id BIGSERIAL PRIMARY KEY,
  op_id BIGINT NOT NULL,
  usuario_id BIGINT,
  usuario_nome TEXT,
  acao TEXT NOT NULL,
  campo TEXT NOT NULL,
  valor_anterior TEXT,
  valor_novo TEXT,
  criado_em TIMESTAMP DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_op_logs_op_id ON op_logs(op_id);

CREATE TABLE IF NOT EXISTS insumos (
  id BIGSERIAL PRIMARY KEY,
  nome TEXT NOT NULL,
  categoria TEXT,
  unidade TEXT,
  estoque_atual DOUBLE PRECISION NOT NULL DEFAULT 0,
  estoque_minimo DOUBLE PRECISION NOT NULL DEFAULT 0,
  ativo INTEGER NOT NULL DEFAULT 1,
  created_at TEXT DEFAULT (to_char(now(), 'YYYY-MM-DD HH24:MI:SS'))
);

CREATE TABLE IF NOT EXISTS insumos_movimentacoes (
  id BIGSERIAL PRIMARY KEY,
  insumo_id BIGINT NOT NULL REFERENCES insumos(id),
  tipo TEXT NOT NULL,
  quantidade DOUBLE PRECISION NOT NULL,
  motivo TEXT,
  data TEXT DEFAULT (to_char(now(), 'YYYY-MM-DD HH24:MI:SS'))
);

CREATE TABLE IF NOT EXISTS op_insumos (
  id BIGSERIAL PRIMARY KEY,
  ordem_id BIGINT NOT NULL REFERENCES ordens_producao(id),
  insumo_id BIGINT NOT NULL REFERENCES insumos(id),
  quantidade DOUBLE PRECISION NOT NULL,
  criado_em TEXT DEFAULT (to_char(now(), 'YYYY-MM-DD HH24:MI:SS'))
);

CREATE TABLE IF NOT EXISTS op_checklist_final (
  id BIGSERIAL PRIMARY KEY,
  ordem_id BIGINT NOT NULL REFERENCES ordens_producao(id),
  item TEXT NOT NULL,
  concluido INTEGER NOT NULL DEFAULT 0,
  data_conclusao TEXT,
  UNIQUE(ordem_id, item)
);

CREATE TABLE IF NOT EXISTS op_checklist_assinatura (
  ordem_id BIGINT PRIMARY KEY REFERENCES ordens_producao(id),
  responsavel TEXT,
  data_assinatura TEXT
);

CREATE TABLE IF NOT EXISTS op_anexos (
  id BIGSERIAL PRIMARY KEY,
  ordem_id BIGINT NOT NULL REFERENCES ordens_producao(id),
  nome TEXT NOT NULL,
  caminho TEXT NOT NULL,
  mime TEXT,
  criado_em TEXT DEFAULT (to_char(now(), 'YYYY-MM-DD HH24:MI:SS'))
);
CREATE INDEX IF NOT EXISTS idx_op_anexos_ordem_id ON op_anexos(ordem_id);

CREATE TABLE IF NOT EXISTS settings (
  key TEXT PRIMARY KEY,
  value TEXT
);

CREATE TABLE IF NOT EXISTS users (
  id BIGSERIAL PRIMARY KEY,
  nome TEXT NOT NULL,
  usuario TEXT NOT NULL UNIQUE,
  senha_hash TEXT NOT NULL,
  role TEXT NOT NULL DEFAULT 'operador',
  modulos TEXT,
  ativo INTEGER NOT NULL DEFAULT 1,
  created_at TEXT NOT NULL DEFAULT (to_char(now(), 'YYYY-MM-DD HH24:MI:SS'))
);
CREATE INDEX IF NOT EXISTS idx_users_usuario ON users(usuario);

CREATE TABLE IF NOT EXISTS brindes (
  id BIGSERIAL PRIMARY KEY,
  nome TEXT NOT NULL UNIQUE,
  categoria TEXT,
  unidade TEXT NOT NULL DEFAULT 'UN',
  estoque_atual INTEGER NOT NULL DEFAULT 0,
  estoque_minimo INTEGER NOT NULL DEFAULT 0,
  ativo INTEGER NOT NULL DEFAULT 1,
  criado_em TEXT DEFAULT (to_char(now(), 'YYYY-MM-DD HH24:MI:SS'))
);

CREATE TABLE IF NOT EXISTS brindes_movimentacoes (
  id BIGSERIAL PRIMARY KEY,
  brinde_id BIGINT NOT NULL REFERENCES brindes(id),
  tipo TEXT NOT NULL,
  quantidade INTEGER NOT NULL,
  observacao TEXT,
  op_id BIGINT REFERENCES ordens_producao(id),
  criado_em TEXT DEFAULT (to_char(now(), 'YYYY-MM-DD HH24:MI:SS'))
);

CREATE TABLE IF NOT EXISTS op_brindes (
  id BIGSERIAL PRIMARY KEY,
  op_id BIGINT NOT NULL REFERENCES ordens_producao(id),
  brinde_id BIGINT NOT NULL REFERENCES brindes(id),
  quantidade INTEGER NOT NULL DEFAULT 1,
  criado_em TEXT DEFAULT (to_char(now(), 'YYYY-MM-DD HH24:MI:SS'))
);
`);

  // Migrações: garantir colunas novas (idempotente)
  const migs = [
    "ALTER TABLE ordens_producao ADD COLUMN material_espessura_mm INTEGER",
    "ALTER TABLE ordens_producao ADD COLUMN material_cor TEXT",
    "ALTER TABLE ordens_producao ADD COLUMN pecas_json TEXT",
    "ALTER TABLE users ADD COLUMN modulos TEXT",
    "ALTER TABLE ordens_producao ADD COLUMN produto_final TEXT",
    "ALTER TABLE ordens_producao ADD COLUMN quantidade_final INTEGER",
    "ALTER TABLE ordens_producao ADD COLUMN data_abertura TEXT",
    "ALTER TABLE ordens_producao ADD COLUMN data_entrega TEXT",
    "ALTER TABLE ordens_producao ADD COLUMN pedido_venda TEXT",
    "ALTER TABLE ordens_producao ADD COLUMN vendedor_nome TEXT",
    "ALTER TABLE ordens_producao ADD COLUMN brindes_baixados INTEGER NOT NULL DEFAULT 0",
    "ALTER TABLE ordens_producao ADD COLUMN arquivada INTEGER NOT NULL DEFAULT 0",
    "ALTER TABLE ordens_producao ADD COLUMN excluida INTEGER NOT NULL DEFAULT 0",
    "ALTER TABLE ordens_producao ADD COLUMN data_arquivada TEXT",
    "ALTER TABLE ordens_producao ADD COLUMN data_excluida TEXT",
    "ALTER TABLE insumos ADD COLUMN categoria TEXT",
    "ALTER TABLE insumos ADD COLUMN unidade TEXT",
    "ALTER TABLE insumos ADD COLUMN ativo INTEGER NOT NULL DEFAULT 1",
    "ALTER TABLE insumos ADD COLUMN created_at TEXT",
  ];
  for (const s of migs) { try { await db.prepare(s).run(); } catch (_) {} }
}

/* =========================
   SEED
   ========================= */
function seedSqlite() {
  // Bootstrap: cria o primeiro admin se não existir nenhum usuário
  try {
    const { hashSync } = require("bcryptjs");
    const c = db.prepare("SELECT COUNT(*) as c FROM users").get().c;
    const hasAdmin = db.prepare("SELECT COUNT(*) as c FROM users WHERE role='admin'").get().c;
    if (c === 0 || hasAdmin === 0) {
      const senha_hash = hashSync("admin123", 10);
      db.prepare("INSERT INTO users (nome, usuario, senha_hash, role, ativo) VALUES (?,?,?,?,1)")
        .run("Administrador", "admin", senha_hash, "admin");
      console.log("\n[ACRILSOFT] Admin criado: usuario=admin senha=admin123 (troque depois!)\n");
    }
  } catch (e) {}

  // Seed: brindes iniciais (se tabela vazia)
  try {
    const c = db.prepare("SELECT COUNT(*) as c FROM brindes").get().c;
    if (c === 0) {
      const insert = db.prepare("INSERT INTO brindes (nome, categoria, unidade, estoque_atual, estoque_minimo, ativo) VALUES (?,?,?,?,?,1)");
      const defaults = [
        ["Caneta", "Brindes", "UN", 0, 0],
        ["Garrafa", "Brindes", "UN", 0, 0],
        ["Caderneta", "Brindes", "UN", 0, 0],
        ["Mochila", "Brindes", "UN", 0, 0],
      ];
      for (const r of defaults) insert.run(...r);
      console.log("[ACRILSOFT] Brindes iniciais cadastrados.");
    }
  } catch (e) {}
}

async function seedPg() {
  try {
    const { hashSync } = require("bcryptjs");
    const c = (await db.prepare("SELECT COUNT(*)::int as c FROM users").get()).c;
    const hasAdmin = (await db.prepare("SELECT COUNT(*)::int as c FROM users WHERE role='admin'").get()).c;
    if (c === 0 || hasAdmin === 0) {
      const senha_hash = hashSync("admin123", 10);
      await db.prepare("INSERT INTO users (nome, usuario, senha_hash, role, ativo, created_at) VALUES (?,?,?,?,1,?)")
        .run("Administrador", "admin", senha_hash, "admin", toNowText());
      console.log("\n[ACRILSOFT] Admin criado: usuario=admin senha=admin123 (troque depois!)\n");
    }
  } catch (e) {}

  try {
    const c = (await db.prepare("SELECT COUNT(*)::int as c FROM brindes").get()).c;
    if (c === 0) {
      const insert = db.prepare("INSERT INTO brindes (nome, categoria, unidade, estoque_atual, estoque_minimo, ativo, criado_em) VALUES (?,?,?,?,?,1,?)");
      const defaults = [
        ["Caneta", "Brindes", "UN", 0, 0],
        ["Garrafa", "Brindes", "UN", 0, 0],
        ["Caderneta", "Brindes", "UN", 0, 0],
        ["Mochila", "Brindes", "UN", 0, 0],
      ];
      for (const r of defaults) await insert.run(...r, toNowText());
      console.log("[ACRILSOFT] Brindes iniciais cadastrados.");
    }
  } catch (e) {}
}


/* =========================
   SYNC (SQLite local -> Postgres)
   ========================= */

// Faz upsert de todas as tabelas do SQLite no Postgres.
// Modelo simples: "snapshot push" (1 direção). Ideal para começar.
// (Multi-master e resolução de conflito fica para etapa seguinte.)
async function syncLocalToOnline(options = {}) {
  const { onProgress } = options;
  if (!PG_URL) throw new Error("PG_URL/pgUrl não configurado");
  const ok = await connectPgInBackground();
  if (!ok || !pgPool) throw new Error("Banco ONLINE indisponível");

  // Garante schema no Postgres
  // Usamos o wrapper do Postgres (async) temporariamente para rodar o DDL.
  const pgDb = makePgWrapper(pgPool);
  const prevDb = db;
  try {
    db = pgDb;
    await ensureSchemaPg();
    await seedPg();
  } finally {
    db = prevDb; // volta pro SQLite operacional
  }

  // Tabelas para sincronizar
  const tables = [
    { name: "fornecedores", conflict: ["id"] },
    { name: "produtos", conflict: ["id"] },
    { name: "movimentacoes", conflict: ["id"] },
    { name: "ordens_producao", conflict: ["id"] },
    { name: "insumos", conflict: ["id"] },
    { name: "op_insumos", conflict: ["id"] },
    { name: "op_anexos", conflict: ["id"] },
    { name: "settings", conflict: ["key"] },
    { name: "users", conflict: ["id"] },
    { name: "brindes", conflict: ["id"] },
    { name: "brindes_movimentacoes", conflict: ["id"] },
    { name: "op_brindes", conflict: ["id"] },
  ];

  const sqlite = authDb || db; // SQLite local real
  const total = tables.length;
  let step = 0;

  for (const t of tables) {
    step += 1;
    const table = t.name;

    // pula se tabela não existir no SQLite
    try {
      sqlite.prepare(`SELECT 1 FROM ${table} LIMIT 1`).get();
    } catch (_) {
      if (onProgress) onProgress({ table, step, total, skipped: true });
      continue;
    }

    const cols = sqlite.prepare(`PRAGMA table_info(${table})`).all().map(r => r.name);
    if (!cols.length) continue;

    const rows = sqlite.prepare(`SELECT ${cols.join(",")} FROM ${table}`).all();
    if (onProgress) onProgress({ table, step, total, rows: rows.length });

    if (!rows.length) continue;

    // Build upsert
    const conflictCols = t.conflict;
    const nonConflictCols = cols.filter(c => !conflictCols.includes(c));

    const colSql = cols.map(c => `"${c}"`).join(",");
    const placeholders = cols.map((_,i)=>`$${i+1}`).join(",");

    let updateSql = "";
    if (nonConflictCols.length) {
      updateSql = nonConflictCols.map(c => `"${c}"=EXCLUDED."${c}"`).join(",");
    } else {
      updateSql = conflictCols.map(c => `"${c}"=EXCLUDED."${c}"`).join(",");
    }

    const upsertSql = `INSERT INTO "${table}" (${colSql}) VALUES (${placeholders}) ON CONFLICT (${conflictCols.map(c=>`"${c}"`).join(",")}) DO UPDATE SET ${updateSql};`;

    // Use transaction por tabela (melhor desempenho)
    const client = await pgPool.connect();
    try {
      await client.query("BEGIN");
      for (const r of rows) {
        const vals = cols.map(c => r[c]);
        await client.query(upsertSql, vals);
      }
      await client.query("COMMIT");
    } catch (e) {
      try { await client.query("ROLLBACK"); } catch (_) {}
      throw e;
    } finally {
      client.release();
    }

    // Ajusta sequence (quando existe id)
    if (conflictCols.length === 1 && conflictCols[0] === "id") {
      try {
        await pgPool.query(`SELECT setval(pg_get_serial_sequence('"${table}"','id'), COALESCE((SELECT MAX(id) FROM "${table}"), 1));`);
      } catch (_) {}
    }
  }

  // salva lastSync no settings local
  try {
    const now = toNowText();
    sqlite.prepare("INSERT OR REPLACE INTO settings (key, value) VALUES ('last_sync', ?)").run(now);
  } catch (_) {}

  return { ok: true };
}

function isOnlineOk() {
  return !!ONLINE_OK;
}

// init imediatamente (server.js usa db sincrono; então exportamos um "dbProxy" que espera init)
const ready = init();

// Proxy: permite manter chamadas db.prepare().get() no código existente.
// No modo Postgres, os métodos são async -> aqui fornecemos versões sync-like usando deasync? NÃO.
// Então, em Postgres precisamos que o server use await (não é o caso). Para manter compatibilidade,
// disponibilizamos um wrapper que bloqueia com Atomics.wait em Node (simples e sem deps).
function block(promise) {
  const sab = new SharedArrayBuffer(4);
  const ia = new Int32Array(sab);
  let res, err;
  promise.then((r) => { res = r; Atomics.store(ia, 0, 1); Atomics.notify(ia, 0); })
         .catch((e) => { err = e; Atomics.store(ia, 0, 1); Atomics.notify(ia, 0); });
  Atomics.wait(ia, 0, 0);
  if (err) throw err;
  return res;
}

const dbProxy = {
  exec(sql) {
    // garante que init terminou antes do primeiro uso
    if (!db) block(ready);
    if (DB_MODE === "online_pg") return block(db.exec(sql));
    return db.exec(sql);
  },
  prepare(sql) {
    if (!db) block(ready);
    const stmt = db.prepare(sql);
    if (DB_MODE !== "online_pg") return stmt;

    return {
      get: (...params) => block(stmt.get(...params)),
      all: (...params) => block(stmt.all(...params)),
      run: (...params) => block(stmt.run(...params)),
    };
  },
  pragma(...args) {
    if (!db) block(ready);
    if (DB_MODE === "online_pg") return;
    return db.pragma(...args);
  },

  // Compat: better-sqlite3 style transactions
  transaction(fn) {
    if (!db) block(ready);
    if (DB_MODE === "online_pg") {
      return () => {
        block(db.exec("BEGIN"));
        try {
          const r = fn();
          block(db.exec("COMMIT"));
          return r;
        } catch (e) {
          try { block(db.exec("ROLLBACK")); } catch (_) {}
          throw e;
        }
      };
    }
    if (typeof db.transaction === "function") return db.transaction(fn);
    return fn;
  },

  // Compat: used by backup/restore flows (SQLite only)
  backup(dest) {
    if (!db) block(ready);
    if (DB_MODE === "online_pg") throw new Error("Backup não suportado no modo Postgres");
    if (typeof db.backup === "function") return db.backup(dest);
    throw new Error("Backup não suportado por este driver");
  },
  close() {
    if (!db) block(ready);
    if (DB_MODE === "online_pg") return block(db.close());
    if (typeof db.close === "function") return db.close();
  },
};

module.exports = { db: dbProxy, getAuthDb, getAppDataDir, getDbMode, pingDb, connectOnline, syncLocalToOnline, isOnlineOk, ready };
