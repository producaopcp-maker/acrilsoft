

const express = require("express");
const { db, getAuthDb, getAppDataDir, pingDb, getDbMode, ready } = require("./db");

// No modo Postgres/Supabase, o schema/migrações são garantidos em db.js.
// Aqui evitamos PRAGMAs e DDLs específicos de SQLite para não travar o boot.
const IS_PG = () => getDbMode() === "online_pg";

/**
 * Garante colunas novas sem quebrar instalações antigas.
 * (evita "no such column" quando o banco ainda não tem a coluna)
 */
function ensureColumn(table, column, ddl) {
  if (IS_PG()) return; // migrações são tratadas no Postgres pelo ensureSchemaPg()
  const cols = db.prepare(`PRAGMA table_info(${table})`).all().map(r => r.name);
  if (!cols.includes(column)) {
    try { db.prepare(ddl).run(); } catch (e) {}
  }
}

// ---------------------------------------------------------------------------
// ⚠️ IMPORTANTE: NUNCA acesse `db.prepare/db.exec` no topo do arquivo.
// O db.js inicializa o Postgres de forma assíncrona; se tocarmos no db antes
// do `ready` resolver, o Electron pode ficar travado no boot.
// ---------------------------------------------------------------------------

async function bootDbAdjustments() {
  // Aguarda o db.js terminar de escolher/validar o banco (Postgres ou fallback SQLite)
  await ready;

  // Brindes (catálogo/orçamentos): seeds básicos
  try { ensureDefaultBrindeFornecedores(); } catch (e) {}

  // Arquivamento manual de OPs: só somem da lista quando arquivada=1
  ensureColumn("ordens_producao", "arquivada", "ALTER TABLE ordens_producao ADD COLUMN arquivada INTEGER DEFAULT 0");
  ensureColumn("ordens_producao", "data_arquivada", "ALTER TABLE ordens_producao ADD COLUMN data_arquivada TEXT");

  ensureColumn("ordens_producao", "excluida", "ALTER TABLE ordens_producao ADD COLUMN excluida INTEGER DEFAULT 0");
  ensureColumn("ordens_producao", "data_excluida", "ALTER TABLE ordens_producao ADD COLUMN data_excluida TEXT");
  ensureColumn("ordens_producao", "status", "ALTER TABLE ordens_producao ADD COLUMN status TEXT NOT NULL DEFAULT 'ABERTA'");
  ensureColumn("ordens_producao", "prioridade", "ALTER TABLE ordens_producao ADD COLUMN prioridade TEXT NOT NULL DEFAULT 'NORMAL'");
  ensureColumn("ordens_producao", "material_espessura_mm", "ALTER TABLE ordens_producao ADD COLUMN material_espessura_mm INTEGER");
  ensureColumn("ordens_producao", "material_cor", "ALTER TABLE ordens_producao ADD COLUMN material_cor TEXT");
  ensureColumn("ordens_producao", "pecas_json", "ALTER TABLE ordens_producao ADD COLUMN pecas_json TEXT");
  ensureColumn("ordens_producao", "observacao_interna", "ALTER TABLE ordens_producao ADD COLUMN observacao_interna TEXT");
  ensureColumn("ordens_producao", "observacao_cliente", "ALTER TABLE ordens_producao ADD COLUMN observacao_cliente TEXT");
  ensureColumn("insumos", "ativo", "ALTER TABLE insumos ADD COLUMN ativo INTEGER DEFAULT 1");
  ensureColumn("users", "modulos", "ALTER TABLE users ADD COLUMN modulos TEXT");

  try { db.prepare("UPDATE ordens_producao SET excluida = 0 WHERE excluida IS NULL").run(); } catch (e) {}
  try { db.prepare("UPDATE insumos SET ativo = 1 WHERE ativo IS NULL").run(); } catch (e) {}
  try { db.prepare("UPDATE ordens_producao SET arquivada = 0 WHERE arquivada IS NULL").run(); } catch (e) {}

  /* ===== Checklist final de produção ===== */
  if (IS_PG()) {
    // Postgres (Supabase)
    await db.exec(`
      CREATE TABLE IF NOT EXISTS op_checklist_final (
        id BIGSERIAL PRIMARY KEY,
        ordem_id BIGINT NOT NULL,
        item TEXT NOT NULL,
        concluido INTEGER DEFAULT 0,
        data_conclusao TEXT,
        UNIQUE(ordem_id, item)
      );
      CREATE TABLE IF NOT EXISTS op_checklist_assinatura (
        ordem_id BIGINT PRIMARY KEY,
        responsavel TEXT,
        data_assinatura TEXT
      );
    `);
  } else {
    // SQLite
    db.prepare(`
      CREATE TABLE IF NOT EXISTS op_checklist_final (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ordem_id INTEGER NOT NULL,
        item TEXT NOT NULL,
        concluido INTEGER DEFAULT 0,
        data_conclusao TEXT,
        UNIQUE(ordem_id, item)
      )
    `).run();

    db.prepare(`
      CREATE TABLE IF NOT EXISTS op_checklist_assinatura (
        ordem_id INTEGER PRIMARY KEY,
        responsavel TEXT,
        data_assinatura TEXT
      )
    `).run();
  }
}

// roda sem bloquear o boot do Electron/Express
bootDbAdjustments().catch((e) => console.error("Boot DB adjustments failed:", e));

const CHECKLIST_FINAL_ITENS = [
  "Medidas conferidas",
  "Material correto",
  "Acabamento revisado",
  "Furação / corte ok",
  "Limpeza final",
  "Impressão",
  "Embalagem ok"
];

function ensureChecklistFinal(ordemId) {
  const ins = IS_PG()
    ? db.prepare("INSERT INTO op_checklist_final (ordem_id, item, concluido) VALUES (?, ?, 0) ON CONFLICT (ordem_id, item) DO NOTHING")
    : db.prepare("INSERT OR IGNORE INTO op_checklist_final (ordem_id, item, concluido) VALUES (?, ?, 0)");
  // libSQL (Hrana) pode lançar "cannot rollback - no transaction is active" em alguns cenários
  // quando usamos o helper .transaction() (ex.: chamadas aninhadas durante criação da OP).
  // Aqui não precisamos de transação: são INSERT OR IGNORE idempotentes.
  for (const it of CHECKLIST_FINAL_ITENS) {
    try {
      ins.run(ordemId, it);
    } catch (e) {
      // não quebra a criação da OP por causa do checklist
      console.warn("Falha ao garantir checklist final:", e.message);
    }
  }
}

const ExcelJS = require("exceljs");
const PDFDocument = require("pdfkit");
const fs = require("fs");
const path = require("path");
const cron = require("node-cron");
const multer = require("multer");
// helper: parse multipart/form-data for small config forms (used by fetch + FormData)
const uploadNone = multer();
const session = require("express-session");
const { compareSync, hashSync } = require("bcryptjs");
const { roleHasModule, requireAuth, requireModule, requireRole } = require('./middlewares/auth');

const app = express();

// Render/Railway/Proxy: garante que req.protocol use x-forwarded-proto
// (evita redirect_uri_mismatch no OAuth do Bling)
app.set("trust proxy", 1);



// SAFE_REMOVE_HEADER_PATCH: evita crash do Node/Express quando algum middleware chama next() após resposta.
// (workaround defensivo para ERR_HTTP_HEADERS_SENT: Cannot remove headers after they are sent)
app.use((req, res, next) => {
  const _remove = res.removeHeader;
  res.removeHeader = function(name) {
    try { return _remove.call(this, name); }
    catch (e) {
      if (e && e.code === 'ERR_HTTP_HEADERS_SENT') return;
      throw e;
    }
  };
  next();
});

app.set("view engine", "ejs");
app.set("views", path.join(__dirname, "views"));
app.use(express.urlencoded({ extended: false }));
app.use(express.static(path.join(__dirname, "public")));

/* ===== Sessão / Login ===== */
app.use(session({
  secret: process.env.SESSION_SECRET || "acrilsoft_mude_essa_chave",
  resave: false,
  saveUninitialized: false,
  cookie: { maxAge: 1000 * 60 * 60 * 10 }
}));

// Locals globais do layout (evita ReferenceError no EJS)
app.use((req, res, next) => {
  const user = req.session?.user || null;
  res.locals.user = user;
  res.locals.userName = user?.nome || "Operator";
  res.locals.activeMenu = res.locals.activeMenu || "";
  res.locals.title = res.locals.title || "Acrilsoft";
  res.locals.dbMode = getDbMode();
  res.locals.canModule = (moduleKey) => {
    if (!user) return false;
    const mods = user.modulos;
    if (Array.isArray(mods) && mods.length > 0) {
      return mods.includes('*') || mods.includes(moduleKey);
    }
    return roleHasModule(user.role, moduleKey);
  };
  next();
});

/* ===== Permissões (Calendário) ===== */
function canEditCalendar(user) {
  const role = String(user?.role || '').toLowerCase();
  // financeiro: só visualiza
  return role === 'admin' || role === 'operador' || role === 'producao' || role === 'produção';
}

/* ===== Log de mudanças em OP ===== */
function logOpChange(opId, user, acao, campo, valorAnterior, valorNovo) {
  try {
    db.prepare(`
      INSERT INTO op_logs (op_id, usuario_id, usuario_nome, acao, campo, valor_anterior, valor_novo)
      VALUES (?, ?, ?, ?, ?, ?, ?)
    `).run(
      Number(opId),
      user?.id ? Number(user.id) : null,
      user?.nome || user?.usuario || user?.email || null,
      String(acao || ''),
      String(campo || ''),
      (valorAnterior == null ? null : String(valorAnterior)),
      (valorNovo == null ? null : String(valorNovo))
    );
  } catch (e) {
    // não derruba o sistema se o log falhar
    console.warn('[OP_LOG] falhou:', e?.message || e);
  }
}

// Health check do banco (para indicador ONLINE/OFFLINE no topo)
app.get('/api/db-status', async (req, res) => {
  const st = await pingDb();
  // Nunca retorna token/url. Apenas status.
  res.json({
    ok: !!st.ok,
    mode: st.mode || getDbMode(),
    error: st.ok ? null : (st.error || 'Falha ao conectar no banco')
  });
});

// Rotas públicas (login)
app.get("/login", (req, res) => {
  if (req.session?.user) return res.redirect("/dashboard");
  res.render("login", { error: null });
});

app.post("/login", (req, res) => {
  const usuario = String(req.body.usuario || "").trim();
  const senha = String(req.body.senha || "");

  // Autenticação SEMPRE pelo SQLite local (não trava se o Postgres estiver lento).
  const adb = getAuthDb();
  const row = adb.prepare("SELECT id, nome, usuario, senha_hash, role, ativo, modulos FROM users WHERE usuario = ?").get(usuario);
  if (!row || row.ativo !== 1) {
    return res.status(401).render("login", { error: "Usuário ou senha inválidos." });
  }
    // Aceita instalações antigas onde a senha estava salva em texto (sem bcrypt).
  // Se detectar hash não-bcrypt, compara direto e migra para bcrypt ao logar.
  const isBcrypt = typeof row.senha_hash === "string" && row.senha_hash.startsWith("$2");
  let ok = false;

  if (isBcrypt) {
    ok = compareSync(senha, row.senha_hash);
  } else {
    ok = senha === String(row.senha_hash || "");
    if (ok) {
      try {
        const newHash = hashSync(senha, 10);
        adb.prepare("UPDATE users SET senha_hash=? WHERE id=?").run(newHash, row.id);
      } catch (e) {}
    }
  }

  if (!ok) {
    return res.status(401).render("login", { error: "Usuário ou senha inválidos." });
  }
  let modulos = [];
  try { modulos = JSON.parse(row.modulos || '[]'); } catch (e) { modulos = []; }
  if (!Array.isArray(modulos)) modulos = [];
  req.session.user = { id: row.id, nome: row.nome, usuario: row.usuario, role: row.role, modulos };
  return res.redirect("/dashboard");
});

app.get("/logout", (req, res) => {
  try {
    req.session.destroy(() => res.redirect("/login"));
  } catch (e) {
    return res.redirect("/login");
  }
});

/* ===== Reset de Admin (Emergência) =====
   Use somente quando não conseguir logar.
   Ao iniciar o servidor, um token é impresso no terminal.
*/
const ADMIN_RESET_TOKEN = process.env.ADMIN_RESET_TOKEN || (Math.random().toString(16).slice(2, 10));
console.log(`\n[ACRILSOFT] Token de reset do admin: ${ADMIN_RESET_TOKEN}\nAcesse /reset-admin e informe o token para redefinir a senha.\n`);

app.get("/reset-admin", (req, res) => {
  res.render("reset-admin", { title: "Reset Admin", error: null, ok: null });
});

app.post("/reset-admin", (req, res) => {
  const { token, novaSenha } = req.body || {};
  if (!token || String(token).trim() !== String(ADMIN_RESET_TOKEN)) {
    return res.render("reset-admin", { title: "Reset Admin", error: "Token inválido.", ok: null });
  }
  const senha = String(novaSenha || "admin123").trim() || "admin123";
  const senha_hash = hashSync(senha, 10);

  const adb = getAuthDb();
  const exists = adb.prepare("SELECT id FROM users WHERE usuario = ?").get("admin");
  if (exists) {
    adb.prepare("UPDATE users SET senha_hash=?, role='admin', ativo=1 WHERE usuario=?").run(senha_hash, "admin");
  } else {
    adb.prepare("INSERT INTO users (nome, usuario, senha_hash, role, ativo) VALUES (?,?,?,?,1)")
      .run("Administrador", "admin", senha_hash, "admin");
  }
  return res.render("reset-admin", { title: "Reset Admin", error: null, ok: `Senha do admin redefinida com sucesso. Usuário: admin | Senha: ${senha}` });
});

// A partir daqui: tudo exige login
app.use(requireAuth);

// Guardas por módulo (por prefixo de rota)
app.use("/dashboard", requireModule("dashboard"));
app.use("/calendario-saidas", requireModule("calendario"));
app.use("/ops", requireModule("ops"));
app.use("/estoque", requireModule("estoque"));
app.use("/produtos", requireModule("estoque"));
app.use("/movimentar", requireModule("estoque"));
app.use("/historico", requireModule("estoque"));
app.use("/insumos", requireModule("insumos"));
app.use("/servicos", requireModule("servicos"));
app.use("/fornecedores", requireModule("fornecedores"));
app.use("/relatorio", requireModule("relatorios"));
app.use("/exportar", requireModule("relatorios"));
app.use("/backup", requireModule("backup"));
app.use("/config", requireModule("branding"));
app.use("/usuarios", requireModule("usuarios"));
app.use("/admin", requireModule("backup"));

/* ===== Uploads (AppData) ===== */
const uploadsDir = path.join(getAppDataDir(), "uploads");
if (!fs.existsSync(uploadsDir)) fs.mkdirSync(uploadsDir, { recursive: true });

const storage = multer.diskStorage({
  destination: (req, file, cb) => cb(null, uploadsDir),
  filename: (req, file, cb) => {
    const safeExt = path.extname(file.originalname || "").toLowerCase().slice(0, 10);
    const rand = Math.random().toString(16).slice(2);
    cb(null, `op_${Date.now()}_${rand}${safeExt}`);
  }
});

const upload = multer({
  storage,
  limits: { fileSize: 8 * 1024 * 1024 },
  fileFilter: (req, file, cb) => {
    if (!file.mimetype || !file.mimetype.startsWith("image/")) return cb(new Error("Apenas imagens"));
    cb(null, true);
  }
});

// Upload opcional de anexo do Pedido (imagem/pdf)
const storagePedido = multer.diskStorage({
  destination: (req, file, cb) => cb(null, uploadsDir),
  filename: (req, file, cb) => {
    const safeExt = path.extname(file.originalname || "").toLowerCase().slice(0, 10);
    const rand = Math.random().toString(16).slice(2);
    cb(null, `pedido_${Date.now()}_${rand}${safeExt}`);
  }
});
const uploadPedido = multer({
  storage: storagePedido,
  fileFilter: (req, file, cb) => {
    const mim = String(file.mimetype || "").toLowerCase();
    if (mim.includes("pdf") || mim.includes("image/")) return cb(null, true);
    return cb(new Error("Arquivo inválido. Envie PDF ou imagem."));
  },
  limits: { fileSize: 20 * 1024 * 1024 }
});

app.use("/uploads", express.static(uploadsDir));
/* ===== Branding (logo do sistema) ===== */
const brandLogoDir = path.join(getAppDataDir(), "uploads", "branding");
if (!fs.existsSync(brandLogoDir)) fs.mkdirSync(brandLogoDir, { recursive: true });
app.use("/branding", express.static(brandLogoDir));

function getSetting(key) {
  try {
    const row = db.prepare("SELECT value FROM settings WHERE key = ?").get(key);
    return row ? row.value : null;
  } catch (e) { return null; }
}
function setSetting(key, value) {
  try {
    db.prepare("INSERT INTO settings(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value").run(key, value);
  } catch (e) {}
}
function delSetting(key) {
  try { db.prepare("DELETE FROM settings WHERE key = ?").run(key); } catch (e) {}
}
function resolveBrandLogoPath() {
  const rel = getSetting("brand_logo_path");
  if (rel) {
    const abs = path.join(getAppDataDir(), rel);
    if (fs.existsSync(abs)) return { abs, rel };
  }
  // fallback: logo em public (compat)
  const candidates = ["logo.png","logo.jpg","logo.jpeg"];
  for (const c of candidates) {
    const p = path.join(__dirname, "public", c);
    if (fs.existsSync(p)) return { abs: p, rel: null };
  }
  return null;
}



// Upload dedicado para o logo do sistema
const brandStorage = multer.diskStorage({
  destination: (req, file, cb) => cb(null, brandLogoDir),
  filename: (req, file, cb) => {
    const ext = (path.extname(file.originalname || "") || "").toLowerCase();
    const safe = (ext === ".png" || ext === ".jpg" || ext === ".jpeg") ? ext : ".png";
    cb(null, "logo" + safe);
  }
});
const uploadBrandLogo = multer({
  storage: brandStorage,
  limits: { fileSize: 4 * 1024 * 1024 },
  fileFilter: (req, file, cb) => {
    const ok = file.mimetype && (file.mimetype === "image/png" || file.mimetype === "image/jpeg");
    if (!ok) return cb(new Error("Use PNG ou JPG"));
    cb(null, true);
  }
});

app.get("/config/branding", (req, res) => {
  const info = resolveBrandLogoPath();
  const logoUrl = info && info.rel ? ("/" + info.rel.replace(/\\/g, "/")) : (info ? "/logo.png" : null);
  res.render("layout", {
    view: "branding-page",
    title: "Identidade Visual",
    pageTitle: "Identidade Visual",
    pageSubtitle: "Escolha o logo usado no PDF e na impressão",
    activeMenu: "branding",
    logoUrl
  });
});

app.post("/config/branding/logo", uploadBrandLogo.single("logo"), (req, res) => {
  // salva caminho relativo no settings
  const ext = path.extname(req.file.filename).toLowerCase();
  const rel = path.join("uploads", "branding", "logo" + ext);
  setSetting("brand_logo_path", rel);
  res.redirect("/config/branding");
});

app.post("/config/branding/logo/remover", (req, res) => {
  const info = resolveBrandLogoPath();
  if (info && info.rel) {
    try { fs.unlinkSync(info.abs); } catch (e) {}
  }
  delSetting("brand_logo_path");
  res.redirect("/config/branding");
});

// Upload temporário de logo (somente para este PDF)
app.post("/ops/:id(\\d+)/pdf/logo-temp", upload.single("logoTemp"), (req, res) => {
  const id = Number(req.params.id);
  if (!req.file) return res.redirect(`/ops/${id}`);
  res.redirect(`/ops/${id}/pdf?tempLogo=${encodeURIComponent(req.file.filename)}`);
});


function mmToM(mm) { return (mm / 1000).toFixed(2); }

/* ===== Backup manual (exportar/restaurar) ===== */
const backupsDir = path.join(getAppDataDir(), "backups");
if (!fs.existsSync(backupsDir)) fs.mkdirSync(backupsDir, { recursive: true });

// Reinício do app (quando rodando via Electron, relança automaticamente)
function restartApp() {
  try {
    const { app } = require("electron");
    if (app && typeof app.relaunch === "function") {
      app.relaunch();
      return app.exit(0);
    }
  } catch (e) {
    // Ignora: provavelmente não está rodando via Electron
  }
  process.exit(0);
}

const tmpDir = path.join(getAppDataDir(), "tmp");
if (!fs.existsSync(tmpDir)) fs.mkdirSync(tmpDir, { recursive: true });

const restoreUpload = multer({
  dest: tmpDir,
  limits: { fileSize: 50 * 1024 * 1024 }, // até 50MB
  fileFilter: (req, file, cb) => {
    const ext = (path.extname(file.originalname || "") || "").toLowerCase();
    if (ext !== ".sqlite" && ext !== ".db") return cb(new Error("Envie um arquivo .sqlite"));
    cb(null, true);
  }
});



function gerarCodigoOP() {
  const ano = new Date().getFullYear();

  const row = db.prepare(`
    SELECT codigo_op
    FROM ordens_producao
    WHERE codigo_op LIKE ?
    ORDER BY codigo_op DESC
    LIMIT 1
  `).get(`OP-${ano}-%`);

  let seq = 1;
  if (row?.codigo_op) {
    const m = row.codigo_op.match(/^OP-(\d{4})-(\d{4})$/);
    if (m && Number(m[1]) === ano) {
      seq = Number(m[2]) + 1;
    }
  }
  return `OP-${ano}-${String(seq).padStart(4, "0")}`;
}

/* ===== Backup automático 20:00 ===== */
function backupDb() {
  try {
    if (IS_PG()) return; // backup automático aplica-se apenas ao SQLite local
    const row = db.prepare("PRAGMA database_list").get();
    const dbFile = row?.file;
    if (!dbFile || !fs.existsSync(dbFile)) return;

    const dir = path.join(getAppDataDir(), "backups");
    if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });

    const stamp = new Date().toISOString().replace(/[:.]/g, "-");
    const dest = path.join(dir, `database-backup-${stamp}.sqlite`);
    fs.copyFileSync(dbFile, dest);

    // Se estiver em modo WAL, copia também -wal e -shm quando existirem
    try {
      if (fs.existsSync(dbFile + '-wal')) fs.copyFileSync(dbFile + '-wal', dest + '-wal');
      if (fs.existsSync(dbFile + '-shm')) fs.copyFileSync(dbFile + '-shm', dest + '-shm');
    } catch (e) {}

    const files = fs
      .readdirSync(dir)
      .filter((f) => f.startsWith("database-backup-") && f.endsWith(".sqlite"))
      .map((f) => ({ f, t: fs.statSync(path.join(dir, f)).mtimeMs }))
      .sort((a, b) => b.t - a.t);

    for (const item of files.slice(30)) fs.unlinkSync(path.join(dir, item.f));
  } catch (e) {
    console.log("Backup erro:", e.message);
  }
}
cron.schedule("0 20 * * *", backupDb);

/* ===== Home ===== */
app.use((req, res, next) => { res.locals.query = req.query; next(); });

app.get("/", (req, res) => res.redirect("/dashboard"));

/* ===== Dashboard ===== */
app.get("/dashboard", (req, res) => {
  // Período (padrão: últimos 30 dias)
  const dias = Math.max(1, Math.min(365, Number(req.query.dias || 30) || 30));
  const desde = db.prepare("SELECT date('now', ?) as d").get(`-${dias} day`).d;

  const ops_atrasadas = db.prepare(`
    SELECT COUNT(*) as n
    FROM ordens_producao
    WHERE status NOT IN ('FINALIZADA','CANCELADA')
      AND data_entrega IS NOT NULL
      AND date(data_entrega) < date('now')
  `).get().n;

  const ops_andamento = db.prepare(`
    SELECT COUNT(*) as n
    FROM ordens_producao
    WHERE status IN ('ABERTA','EM ANDAMENTO','PRODUCAO','PRODUÇÃO','SEPARACAO','SEPARAÇÃO')
  `).get().n;

  const ops_vencendo_7d = db.prepare(`
    SELECT COUNT(*) as n
    FROM ordens_producao
    WHERE status NOT IN ('FINALIZADA','CANCELADA')
      AND data_entrega IS NOT NULL
      AND date(data_entrega) >= date('now')
      AND date(data_entrega) <= date('now', '+7 day')
  `).get().n;

  const consumo_produtos = db.prepare(`
    SELECT p.descricao, p.codigo_interno, p.cor, p.espessura_mm,
           SUM(oi.quantidade) as qtd_total,
           COUNT(DISTINCT oi.ordem_id) as ops
    FROM ordem_itens oi
    JOIN ordens_producao op ON op.id = oi.ordem_id
    JOIN produtos p ON p.id = oi.produto_id
    WHERE op.data_abertura IS NOT NULL
      AND date(op.data_abertura) >= date(?)
    GROUP BY p.id
    ORDER BY qtd_total DESC, p.descricao ASC
    LIMIT 50
  `).all(desde);

  // Estoque crítico (produtos)
  const produtos_abaixo_minimo = db.prepare(`
    SELECT COUNT(*) as n
    FROM produtos
    WHERE COALESCE(estoque_atual,0) < COALESCE(estoque_minimo,0)
  `).get().n;

  const produtos_criticos = db.prepare(`
    SELECT id, descricao, codigo_interno, cor, espessura_mm,
           COALESCE(estoque_atual,0) as estoque_atual,
           COALESCE(estoque_minimo,0) as estoque_minimo
    FROM produtos
    WHERE COALESCE(estoque_atual,0) < COALESCE(estoque_minimo,0)
    ORDER BY (COALESCE(estoque_minimo,0) - COALESCE(estoque_atual,0)) DESC, descricao ASC
    LIMIT 12
  `).all();

  // Estoque crítico (insumos) + consumo (try/catch para ambientes sem módulo)
  let insumos_abaixo_minimo = 0;
  let insumos_criticos = [];
  let top_insumos = [];
  let ult_mov_insumos = [];
  try {
    insumos_abaixo_minimo = db.prepare(`
      SELECT COUNT(*) as n
      FROM insumos
      WHERE COALESCE(estoque_atual,0) < COALESCE(estoque_minimo,0)
    `).get().n;

    insumos_criticos = db.prepare(`
      SELECT id, nome, categoria, unidade,
             COALESCE(estoque_atual,0) as estoque_atual,
             COALESCE(estoque_minimo,0) as estoque_minimo
      FROM insumos
      WHERE COALESCE(estoque_atual,0) < COALESCE(estoque_minimo,0)
      ORDER BY (COALESCE(estoque_minimo,0) - COALESCE(estoque_atual,0)) DESC, nome ASC
      LIMIT 12
    `).all();

    top_insumos = db.prepare(`
      SELECT i.nome, i.unidade,
             SUM(m.quantidade) as qtd_total,
             COUNT(*) as eventos
      FROM insumos_movimentacoes m
      JOIN insumos i ON i.id = m.insumo_id
      WHERE m.tipo = 'saida'
        AND m.data IS NOT NULL
        AND date(m.data) >= date(?)
      GROUP BY m.insumo_id
      ORDER BY qtd_total DESC, i.nome ASC
      LIMIT 10
    `).all(desde);

    ult_mov_insumos = db.prepare(`
      SELECT m.id, m.tipo, m.quantidade, m.motivo, m.data, i.nome, i.unidade
      FROM insumos_movimentacoes m
      JOIN insumos i ON i.id = m.insumo_id
      ORDER BY m.id DESC
      LIMIT 12
    `).all();
  } catch (e) {
    insumos_abaixo_minimo = 0;
    insumos_criticos = [];
    top_insumos = [];
    ult_mov_insumos = [];
  }

  // Movimentações recentes (produtos)
  let ult_mov_produtos = [];
  try {
    ult_mov_produtos = db.prepare(`
      SELECT m.id, m.tipo, m.quantidade, m.motivo, m.data, p.descricao, p.codigo_interno
      FROM movimentacoes m
      LEFT JOIN produtos p ON p.id = m.produto_id
      ORDER BY m.id DESC
      LIMIT 12
    `).all();
  } catch (e) {
    ult_mov_produtos = [];
  }

  const lista_atrasadas = db.prepare(`
    SELECT id, codigo_op, cliente, pedido_venda, produto_final, quantidade_final, data_entrega, prioridade
    FROM ordens_producao
    WHERE status NOT IN ('FINALIZADA','CANCELADA')
      AND data_entrega IS NOT NULL
      AND date(data_entrega) < date('now')
    ORDER BY date(data_entrega) ASC, prioridade DESC, id DESC
    LIMIT 12
  `).all();

  res.render("layout", {
    title: "Dashboard",
    view: "dashboard-page",
    dias,
    desde,
    kpis: {
      ops_atrasadas,
      ops_andamento,
      ops_vencendo_7d,
      produtos_abaixo_minimo,
      insumos_abaixo_minimo
    },
    produtos_criticos,
    insumos_criticos,
    consumo_produtos,
    top_insumos,
    ult_mov_produtos,
    ult_mov_insumos,
    lista_atrasadas
  });
});

/* ===== Calendário de saídas ===== */
app.get("/calendario-saidas", (req, res) => {
  res.render("layout", {
    title: "Calendário de saídas",
    view: "calendario-saidas-page",
    pageTitle: "Calendário de saídas",
    pageSubtitle: "Mostra as OPs abertas / em andamento / finalizadas com data de entrega",
    activeMenu: "calendario",
    canEditCalendar: canEditCalendar(req.session?.user)
  });
});

// Eventos do calendário (FullCalendar)
app.get("/api/dashboard/ops-calendario", (req, res) => {
  // FullCalendar manda start/end no formato ISO
  const start = String(req.query.start || '').slice(0, 10);
  const end = String(req.query.end || '').slice(0, 10);

  // fallback seguro: última semana
  const startParam = (/^\d{4}-\d{2}-\d{2}$/.test(start)) ? start : db.prepare("SELECT date('now','-7 day') as d").get().d;
  const endParam = (/^\d{4}-\d{2}-\d{2}$/.test(end)) ? end : db.prepare("SELECT date('now','+7 day') as d").get().d;

  const rows = db.prepare(`
    SELECT id, codigo_op, cliente, vendedor_nome, status, data_entrega, produto_final, prioridade
    FROM ordens_producao
    WHERE data_entrega IS NOT NULL
      AND date(data_entrega) >= date(?)
      AND date(data_entrega) < date(?)
      AND COALESCE(excluida,0) = 0
  `).all(startParam, endParam);

  const events = (rows || []).map(r => ({
    id: String(r.id),
    title: r.codigo_op + (r.vendedor_nome ? (' • ' + r.vendedor_nome) : ''),
    start: String(r.data_entrega).slice(0, 10),
    allDay: true,
    url: `/ops/${r.id}`,
    extendedProps: {
      opId: r.id,
      codigo_op: r.codigo_op,
      cliente: r.cliente || '',
      vendedor_nome: r.vendedor_nome || '',
      status: r.status || 'ABERTA',
      produto_final: r.produto_final || '',
      prioridade: r.prioridade || 'NORMAL'
    }
  }));

  res.json(events);
});

// Resumo da OP para o modal
app.get("/api/ops/:id(\\d+)/resumo", (req, res) => {
  const id = Number(req.params.id);
  const op = db.prepare(`
    SELECT id, codigo_op, cliente, vendedor_nome, status, prioridade, produto_final, quantidade_final,
           data_entrega,
           COALESCE(observacao_cliente, observacao, '') AS observacoes,
           COALESCE(observacao_interna, '') AS observacoes_internas
    FROM ordens_producao
    WHERE id = ?
  `).get(id);

  if (!op) return res.status(404).json({ error: 'OP não encontrada' });
  res.json(op);
});

// Logs da OP
app.get("/api/ops/:id(\\d+)/logs", (req, res) => {
  const id = Number(req.params.id);
  try {
    const logs = db.prepare(`
      SELECT id, usuario_nome, acao, campo, valor_anterior, valor_novo, criado_em
      FROM op_logs
      WHERE op_id = ?
      ORDER BY id DESC
      LIMIT 30
    `).all(id);
    res.json({ op_id: id, logs: logs || [] });
  } catch (e) {
    res.status(500).json({ error: 'Falha ao carregar logs' });
  }
});

// Atualizar data de entrega (drag & drop)
app.post("/ops/:id(\\d+)/data-entrega", (req, res) => {
  const user = req.session?.user;
  if (!canEditCalendar(user)) return res.status(403).json({ error: 'Sem permissão' });

  const ordemId = Number(req.params.id);
  const data_entrega = String(req.body.data_entrega || '').trim();
  if (!/^\d{4}-\d{2}-\d{2}$/.test(data_entrega)) {
    return res.status(400).json({ error: 'Data inválida' });
  }

  const before = db.prepare("SELECT data_entrega FROM ordens_producao WHERE id = ?").get(ordemId);
  if (!before) return res.status(404).json({ error: 'OP não encontrada' });

  db.prepare("UPDATE ordens_producao SET data_entrega = ? WHERE id = ?").run(data_entrega, ordemId);
  logOpChange(ordemId, user, 'DATA_ENTREGA', 'data_entrega', before.data_entrega, data_entrega);
  res.json({ ok: true, data_entrega });
});

/* ===== Usuários (Admin) ===== */
const USER_ROLES = ["admin", "operador", "financeiro", "producao"]; // pode adicionar mais

const MODULE_OPTIONS = [
  { key: "dashboard", label: "Dashboard" },
  { key: "calendario", label: "Calendário" },
  { key: "ops", label: "OPs" },
  { key: "estoque", label: "Estoque" },
  { key: "insumos", label: "Insumos" },
  { key: "servicos", label: "Serviços" },
  { key: "fornecedores", label: "Fornecedores" },
  { key: "relatorios", label: "Relatórios" },
  { key: "backup", label: "Backup" },
  { key: "branding", label: "Configurações" },
  { key: "usuarios", label: "Usuários" },
  { key: "pedidos", label: "Pedidos" },
  { key: "bling", label: "Pedidos (Bling)" },
  { key: "novo_pedido", label: "Novo Pedido" },

];

app.get("/usuarios", (req, res) => {
  const users = db.prepare("SELECT id, nome, usuario, role, ativo, created_at, modulos FROM users ORDER BY id DESC").all();
  const usersView = users.map(u => {
    let mods = [];
    try { mods = JSON.parse(u.modulos || "[]"); } catch (e) { mods = []; }
    if (!Array.isArray(mods)) mods = [];
    const modulosText = (mods.length === 0) ? "Padrão do perfil" : mods.join(", ");
    return { ...u, modulosText, _mods: mods };
  });
  res.render("layout", {
    title: "Usuários",
    view: "usuarios-page",
    pageTitle: "Usuários",
    pageSubtitle: "Controle de acesso por módulos",
    activeMenu: "usuarios",
    users: usersView
  });
});

app.get("/usuarios/novo", (req, res) => {
  res.render("layout", {
    title: "Novo usuário",
    view: "usuarios-form-page",
    pageTitle: "Novo usuário",
    pageSubtitle: "Crie um novo login para o sistema",
    activeMenu: "usuarios",
    action: "/usuarios/novo",
    roles: USER_ROLES,
    moduleOptions: MODULE_OPTIONS,
    userEdit: null,
    error: null
  });
});

app.post("/usuarios/novo", (req, res) => {
  const nome = String(req.body.nome || "").trim();
  const usuario = String(req.body.usuario || "").trim();
  const role = String(req.body.role || "operador").trim();
  const senha = String(req.body.senha || "");
  let modulos = req.body.modulos;
  if (typeof modulos === "string" && modulos.trim()) modulos = [modulos];
  if (!Array.isArray(modulos)) modulos = [];
  modulos = modulos.map(m => String(m).trim()).filter(Boolean);

  if (!nome || !usuario || !senha) {
    return res.status(400).render("layout", {
      title: "Novo usuário",
      view: "usuarios-form-page",
      pageTitle: "Novo usuário",
      pageSubtitle: "Crie um novo login para o sistema",
      activeMenu: "usuarios",
      action: "/usuarios/novo",
      roles: USER_ROLES,
      moduleOptions: MODULE_OPTIONS,
      userEdit: { nome, usuario, role, modulos },
      error: "Preencha nome, usuário e senha."
    });
  }
  if (!USER_ROLES.includes(role)) {
    return res.status(400).render("layout", {
      title: "Novo usuário",
      view: "usuarios-form-page",
      pageTitle: "Novo usuário",
      pageSubtitle: "Crie um novo login para o sistema",
      activeMenu: "usuarios",
      action: "/usuarios/novo",
      roles: USER_ROLES,
      moduleOptions: MODULE_OPTIONS,
      userEdit: { nome, usuario, role, modulos },
      error: "Perfil inválido."
    });
  }

  try {
    const senha_hash = hashSync(senha, 10);
    db.prepare("INSERT INTO users (nome, usuario, senha_hash, role, modulos, ativo) VALUES (?,?,?,?,?,1)")
      .run(nome, usuario, senha_hash, role, modulos.length ? JSON.stringify(modulos) : null);
    return res.redirect("/usuarios?ok=" + encodeURIComponent("Usuário criado com sucesso."));
  } catch (e) {
    const msg = String(e.message || "");
    const human = msg.includes("UNIQUE") ? "Já existe um usuário com esse login." : "Erro ao criar usuário.";
    return res.status(400).render("layout", {
      title: "Novo usuário",
      view: "usuarios-form-page",
      pageTitle: "Novo usuário",
      pageSubtitle: "Crie um novo login para o sistema",
      activeMenu: "usuarios",
      action: "/usuarios/novo",
      roles: USER_ROLES,
      moduleOptions: MODULE_OPTIONS,
      userEdit: { nome, usuario, role, modulos },
      error: human
    });
  }
});

app.get("/usuarios/:id/editar", (req, res) => {
  const id = Number(req.params.id);
  const userEdit = db.prepare("SELECT id, nome, usuario, role, ativo, modulos FROM users WHERE id = ?").get(id);
  if (!userEdit) return res.redirect("/usuarios?erro=" + encodeURIComponent("Usuário não encontrado."));
  res.render("layout", {
    title: "Editar usuário",
    view: "usuarios-form-page",
    pageTitle: "Editar usuário",
    pageSubtitle: "Ajuste o perfil e o nome do usuário",
    activeMenu: "usuarios",
    action: `/usuarios/${id}/editar`,
    roles: USER_ROLES,
    moduleOptions: MODULE_OPTIONS,
    userEdit: (() => { let mods=[]; try{mods=JSON.parse(userEdit.modulos||'[]')}catch(e){mods=[]}; if(!Array.isArray(mods)) mods=[]; return { ...userEdit, modulosArr: mods }; })(),
    error: null
  });
});

app.post("/usuarios/:id/editar", (req, res) => {
  const id = Number(req.params.id);
  const nome = String(req.body.nome || "").trim();
  const role = String(req.body.role || "operador").trim();
  let modulos = req.body.modulos;
  if (typeof modulos === "string" && modulos.trim()) modulos = [modulos];
  if (!Array.isArray(modulos)) modulos = [];
  modulos = modulos.map(m => String(m).trim()).filter(Boolean);
  const userEdit = db.prepare("SELECT id, nome, usuario, role, ativo, modulos FROM users WHERE id = ?").get(id);
  if (!userEdit) return res.redirect("/usuarios?erro=" + encodeURIComponent("Usuário não encontrado."));
  if (!nome) {
    return res.status(400).render("layout", {
      title: "Editar usuário",
      view: "usuarios-form-page",
      pageTitle: "Editar usuário",
      pageSubtitle: "Ajuste o perfil e o nome do usuário",
      activeMenu: "usuarios",
      action: `/usuarios/${id}/editar`,
      roles: USER_ROLES,
      moduleOptions: MODULE_OPTIONS,
      userEdit: { ...userEdit, nome, role, modulosArr: modulos },
      error: "Informe o nome."
    });
  }
  if (!USER_ROLES.includes(role)) {
    return res.status(400).render("layout", {
      title: "Editar usuário",
      view: "usuarios-form-page",
      pageTitle: "Editar usuário",
      pageSubtitle: "Ajuste o perfil e o nome do usuário",
      activeMenu: "usuarios",
      action: `/usuarios/${id}/editar`,
      roles: USER_ROLES,
      moduleOptions: MODULE_OPTIONS,
      userEdit: { ...userEdit, nome, role, modulosArr: modulos },
      error: "Perfil inválido."
    });
  }

  db.prepare("UPDATE users SET nome = ?, role = ?, modulos = ? WHERE id = ?").run(nome, role, modulos.length ? JSON.stringify(modulos) : null, id);
  // se o admin editou a si mesmo, atualiza a sessão
  if (req.session?.user && req.session.user.id === id) {
    req.session.user.nome = nome;
    req.session.user.role = role;
    req.session.user.modulos = modulos;
  }
  return res.redirect("/usuarios?ok=" + encodeURIComponent("Usuário atualizado."));
});

app.post("/usuarios/:id/toggle", (req, res) => {
  const id = Number(req.params.id);
  const row = db.prepare("SELECT id, ativo FROM users WHERE id = ?").get(id);
  if (!row) return res.redirect("/usuarios?erro=" + encodeURIComponent("Usuário não encontrado."));
  const novo = row.ativo ? 0 : 1;
  db.prepare("UPDATE users SET ativo = ? WHERE id = ?").run(novo, id);
  return res.redirect("/usuarios?ok=" + encodeURIComponent(novo ? "Usuário ativado." : "Usuário desativado."));
});

function randomPassword(len = 8) {
  const chars = "ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz23456789";
  let out = "";
  for (let i = 0; i < len; i++) out += chars[Math.floor(Math.random() * chars.length)];
  return out;
}

app.post("/usuarios/:id/reset-senha", (req, res) => {
  const id = Number(req.params.id);
  const row = db.prepare("SELECT id FROM users WHERE id = ?").get(id);
  if (!row) return res.redirect("/usuarios?erro=" + encodeURIComponent("Usuário não encontrado."));
  const novaSenha = randomPassword(8);
  const senha_hash = hashSync(novaSenha, 10);
  db.prepare("UPDATE users SET senha_hash = ? WHERE id = ?").run(senha_hash, id);
  return res.redirect("/usuarios?ok=" + encodeURIComponent("Senha resetada: " + novaSenha));
});

/* ===== Backup (tela) ===== */
app.get("/backup", (req, res) => {
  let backups = [];
  try {
    if (fs.existsSync(backupsDir)) {
      backups = fs
        .readdirSync(backupsDir)
        .filter((f) => f.endsWith(".sqlite") || f.endsWith(".db"))
        .map((name) => {
          const full = path.join(backupsDir, name);
          const stat = fs.statSync(full);
          return { name, t: stat.mtimeMs, date: new Date(stat.mtimeMs).toLocaleString("pt-BR") };
        })
        .sort((a, b) => b.t - a.t);
    }
  } catch (e) {}

  res.render("layout", {
    title: "Backup",
    view: "backup-page",
    backups,
    backupsDir
  });
});

// Reiniciar o sistema (Electron relança automaticamente)
app.post("/admin/restart", (req, res) => {
  res.setHeader("Content-Type", "application/json; charset=utf-8");
  res.end(JSON.stringify({ ok: true }));
  setTimeout(() => restartApp(), 300);
});

/* Exporta um backup consistente do SQLite */
app.get("/backup/exportar", async (req, res) => {
  try {
    if (IS_PG()) throw new Error("Backup/restore disponíveis apenas no modo SQLite local");
    const stamp = new Date().toISOString().replace(/[:.]/g, "-");
    const dest = path.join(backupsDir, `backup-manual-${stamp}.sqlite`);

    // backup consistente (better-sqlite3)
    if (typeof db.backup === "function") {
      await db.backup(dest);
    } else {
      // fallback: copia arquivo do db (pode ser inconsistente em WAL)
      const row = db.prepare("PRAGMA database_list").get();
      const dbFile = row?.file;
      if (!dbFile || !fs.existsSync(dbFile)) throw new Error("Banco não encontrado");
      fs.copyFileSync(dbFile, dest);
    }

    res.download(dest);
  } catch (err) {
    console.error(err);
    res.redirect(`/backup?erro=${encodeURIComponent("Erro ao exportar backup")}`);
  }
});

/* Baixa um backup existente na pasta de backups */
app.get("/backup/baixar", (req, res) => {
  try {
    const name = String(req.query.file || "");
    if (!name || name.includes("..") || name.includes("/") || name.includes("\\")) {
      return res.redirect(`/backup?erro=${encodeURIComponent("Arquivo inválido")}`);
    }
    const full = path.join(backupsDir, name);
    if (!fs.existsSync(full)) return res.redirect(`/backup?erro=${encodeURIComponent("Arquivo não encontrado")}`);
    return res.download(full);
  } catch (e) {
    return res.redirect(`/backup?erro=${encodeURIComponent("Erro ao baixar")}`);
  }
});

/* Restaura banco a partir de arquivo enviado
   - Substitui o banco atual
   - Fecha o processo para o usuário abrir novamente (evita db em memória apontando para arquivo antigo)
*/
app.post("/backup/restaurar", restoreUpload.single("backup"), (req, res) => {
  try {
    if (IS_PG()) return res.redirect(`/backup?erro=${encodeURIComponent("Backup/restore disponíveis apenas no modo SQLite local")}`);
    if (!req.file) return res.redirect(`/backup?erro=${encodeURIComponent("Nenhum arquivo enviado")}`);

    // Caminho do banco atual
    const row = db.prepare("PRAGMA database_list").get();
    const dbFile = row?.file;
    if (!dbFile) throw new Error("Caminho do banco não encontrado");

    const uploadedPath = req.file.path;

    // Fecha DB antes de substituir
    try { db.close(); } catch (e) {}

    // Substitui
    fs.copyFileSync(uploadedPath, dbFile);

    // Limpa WAL/SHM antigos (se existirem) para evitar inconsistência
    try { if (fs.existsSync(dbFile + "-wal")) fs.unlinkSync(dbFile + "-wal"); } catch (e) {}
    try { if (fs.existsSync(dbFile + "-shm")) fs.unlinkSync(dbFile + "-shm"); } catch (e) {}

    // Limpa upload temporário
    try { fs.unlinkSync(uploadedPath); } catch (e) {}

    // Resposta + encerra
    res.setHeader("Content-Type", "text/html; charset=utf-8");
    res.end(`
      <html><body style="font-family:Arial; padding:24px;">
        <h2>Backup restaurado com sucesso ✅</h2>
        <p>O sistema será fechado em instantes para finalizar a restauração.</p>
        <p>Abra o sistema novamente pelo atalho/ícone.</p>
      </body></html>
    `);
    setTimeout(() => restartApp(), 1500);
  } catch (err) {
    console.error(err);
    res.redirect(`/backup?erro=${encodeURIComponent("Erro ao restaurar backup")}`);
  }
});

/* ===== Estoque ===== */
app.get("/estoque", (req, res) => {
  const q = (req.query.q || "").trim();

  const produtos = q
    ? db.prepare(`
        SELECT *, (estoque_atual <= estoque_minimo) AS baixo
        FROM produtos
        WHERE descricao LIKE ? OR cor LIKE ? OR codigo_interno LIKE ?
        ORDER BY baixo DESC, descricao ASC
      `).all(`%${q}%`, `%${q}%`, `%${q}%`)
    : db.prepare(`
        SELECT *, (estoque_atual <= estoque_minimo) AS baixo
        FROM produtos
        ORDER BY baixo DESC, descricao ASC
      `).all();

  const kpis = db.prepare(`
    SELECT
      COUNT(*) as total_itens,
      SUM(CASE WHEN estoque_atual <= estoque_minimo THEN 1 ELSE 0 END) as abaixo_minimo,
      SUM(estoque_atual) as total_chapas
    FROM produtos
  `).get();

  const mesAtual = new Date().toISOString().slice(0, 7);
  const movMes = db.prepare(`
    SELECT COUNT(*) as movs
    FROM movimentacoes
    WHERE criado_em LIKE ?
  `).get(mesAtual + '%');

  res.render("layout", { title: "Estoque", view: "estoque-page", produtos, mmToM, q, kpis, movMes });
});

/* ===== Produtos ===== */
app.get("/produtos/novo", (req, res) => {
  const fornecedores = db.prepare(`SELECT id, nome FROM fornecedores ORDER BY nome ASC`).all();
  res.render("layout", { title: "Nova Chapa", view: "novo-produto-page", fornecedores });
});

app.post("/produtos", (req, res) => {
  const { codigo_interno, descricao, espessura_mm, cor, largura_mm, altura_mm,
    localizacao, estoque_atual, estoque_minimo, fornecedor_id, marca } = req.body;

  const cod = (codigo_interno || "").trim();
  if (cod) {
    const exists = db.prepare(`SELECT id FROM produtos WHERE codigo_interno = ? LIMIT 1`).get(cod);
    if (exists) return res.redirect(`/produtos/novo?erro=${encodeURIComponent("Código interno já existe. Use outro.")}`);
}

  db.prepare(`
    INSERT INTO produtos
    (codigo_interno, descricao, espessura_mm, cor, largura_mm, altura_mm, localizacao,
     estoque_atual, estoque_minimo, fornecedor_id, marca)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `).run(
    cod || null,
    descricao.trim(),
    Number(espessura_mm),
    cor.trim(),
    Number(largura_mm),
    Number(altura_mm),
    localizacao?.trim() || null,
    Number(estoque_atual || 0),
    Number(estoque_minimo || 0),
    fornecedor_id ? Number(fornecedor_id) : null,
    (marca || "").trim() || null
  );

  res.redirect("/estoque");
});

/* ===== Movimentar ===== */
app.get("/movimentar/:id", (req, res) => {
  const produto = db.prepare(`
    SELECT p.*, f.nome as fornecedor_nome
    FROM produtos p
    LEFT JOIN fornecedores f ON f.id = p.fornecedor_id
    WHERE p.id = ?
  `).get(Number(req.params.id));
  if (!produto) return res.status(404).send("Produto não encontrado");

  res.render("layout", { title: "Movimentar", view: "movimentar-page", produto, mmToM });
});

app.post("/movimentar/:id", (req, res) => {
  const produtoId = Number(req.params.id);
  const { tipo, quantidade, observacao } = req.body;

  const qtd = Number(quantidade);
  if (!["ENTRADA", "SAIDA", "AJUSTE"].includes(tipo)) return res.status(400).send("Tipo inválido");
  if (!Number.isInteger(qtd) || qtd <= 0) return res.status(400).send("Quantidade inválida");

  const produto = db.prepare("SELECT * FROM produtos WHERE id = ?").get(produtoId);
  if (!produto) return res.status(404).send("Produto não encontrado");

  let novoEstoque = produto.estoque_atual;
  if (tipo === "ENTRADA") novoEstoque += qtd;
  if (tipo === "SAIDA") {
    if (produto.estoque_atual - qtd < 0) return res.status(400).send("Sem estoque suficiente");
    novoEstoque -= qtd;
  }
  if (tipo === "AJUSTE") novoEstoque = qtd;

  const tx = db.transaction(() => {
    db.prepare(`
      INSERT INTO movimentacoes (produto_id, tipo, quantidade, observacao)
      VALUES (?, ?, ?, ?)
    `).run(produtoId, tipo, qtd, observacao?.trim() || null);

    db.prepare("UPDATE produtos SET estoque_atual = ? WHERE id = ?").run(novoEstoque, produtoId);
  });

  tx();
  res.redirect("/estoque");
});

/* ===== Histórico ===== */
app.get("/historico", (req, res) => {
  const rows = db.prepare(`
    SELECT m.*, p.descricao, p.espessura_mm, p.cor, p.codigo_interno
    FROM movimentacoes m
    JOIN produtos p ON p.id = m.produto_id
    ORDER BY m.id DESC
    LIMIT 300
  `).all();

  res.render("layout", { title: "Histórico", view: "historico-page", rows });
});

/* ===== Fornecedores ===== */
app.get("/fornecedores", (req, res) => {
  const fornecedores = db.prepare(`SELECT * FROM fornecedores ORDER BY nome ASC`).all();
  res.render("layout", { title: "Fornecedores", view: "fornecedores-page", fornecedores });
});

app.get("/fornecedores/novo", (req, res) => {
  res.render("layout", { title: "Novo Fornecedor", view: "novo-fornecedor-page" });
});

app.post("/fornecedores", (req, res) => {
  const { nome, whatsapp, email, observacao } = req.body;
  db.prepare(`
    INSERT INTO fornecedores (nome, whatsapp, email, observacao)
    VALUES (?, ?, ?, ?)
  `).run(nome.trim(), whatsapp?.trim() || null, email?.trim() || null, observacao?.trim() || null);

  res.redirect("/fornecedores");
});

/* ===== Relatório mensal ===== */
app.get("/relatorio/mensal", (req, res) => {
  const mes = (req.query.mes || new Date().toISOString().slice(0, 7)).slice(0, 7);

  const rows = db.prepare(`
    SELECT m.criado_em, m.tipo, m.quantidade, m.observacao,
           p.codigo_interno, p.descricao, p.espessura_mm, p.cor
    FROM movimentacoes m
    JOIN produtos p ON p.id = m.produto_id
    WHERE m.criado_em LIKE ?
    ORDER BY m.criado_em DESC
  `).all(mes + '%');

  const totais = db.prepare(`
    SELECT
      SUM(CASE WHEN tipo='ENTRADA' THEN quantidade ELSE 0 END) as entradas,
      SUM(CASE WHEN tipo='SAIDA' THEN quantidade ELSE 0 END) as saidas,
      COUNT(*) as movs
    FROM movimentacoes
    WHERE criado_em LIKE ?
  `).get(mes + '%');

  res.render("layout", { title: "Relatório Mensal", view: "relatorio-mensal-page", mes, rows, totais });
});

/* ===== Exportar Excel ===== */
app.get("/exportar/estoque.xlsx", async (req, res) => {
  const produtos = db.prepare(`
    SELECT p.*, f.nome AS fornecedor
    FROM produtos p
    LEFT JOIN fornecedores f ON f.id = p.fornecedor_id
    ORDER BY p.descricao
  `).all();

  const wb = new ExcelJS.Workbook();
  const ws = wb.addWorksheet("Estoque");

  ws.columns = [
    { header: "Código", key: "codigo_interno", width: 20 },
    { header: "Descrição", key: "descricao", width: 32 },
    { header: "Espessura (mm)", key: "espessura_mm", width: 14 },
    { header: "Cor", key: "cor", width: 14 },
    { header: "Fornecedor", key: "fornecedor", width: 22 },
    { header: "Estoque", key: "estoque_atual", width: 10 },
    { header: "Mínimo", key: "estoque_minimo", width: 10 }
  ];
  ws.getRow(1).font = { bold: true };
  ws.addRows(produtos);

  res.setHeader("Content-Type", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet");
  res.setHeader("Content-Disposition", 'attachment; filename="estoque.xlsx"');

  await wb.xlsx.write(res);
  res.end();
});

/* ===== OP ===== */

// --------------------------------------------
// PEDIDOS (pré-OP)
// --------------------------------------------

app.get("/pedidos", requireModule("pedidos"), (req, res) => {
  try {
    const q = String(req.query.q || "").trim();
    const status = String(req.query.status || "").trim();
    const vendedor = String(req.query.vendedor || "").trim();
    const de = String(req.query.de || "").trim();
    const ate = String(req.query.ate || "").trim();

    const where = [];
    const params = [];

    if (q) {
      where.push("(cliente_nome LIKE ? OR cliente_contato LIKE ? OR vendedor_nome LIKE ? OR CAST(id AS TEXT) LIKE ?)");
      const like = `%${q}%`;
      params.push(like, like, like, like);
    }
    if (status) { where.push("status = ?"); params.push(status); }
    if (vendedor) { where.push("vendedor_nome LIKE ?"); params.push(`%${vendedor}%`); }
    if (de) { where.push("date(criado_em) >= date(?)"); params.push(de); }
    if (ate) { where.push("date(criado_em) <= date(?)"); params.push(ate); }

    const sql = `
      SELECT *
      FROM pedidos
      ${where.length ? "WHERE " + where.join(" AND ") : ""}
      ORDER BY id DESC
      LIMIT 500
    `;
    const pedidos = db.prepare(sql).all(...params);

    res.render("layout", {
      title: "Pedidos",
      view: "pedidos-list-page",
      pedidos,
      filtros: { q, status, vendedor, de, ate }
    });
  } catch (err) {
    console.error("Erro ao listar pedidos:", err);
    res.status(500).send(err.message);
  }
});

app.get("/pedidos/novo", requireModule("novo_pedido"), (req, res) => {
  res.render("layout", {
    title: "Novo Pedido",
    view: "pedido-form-page",
    pedido: null,
    itens: [],
    modo: "novo"
  });
});

app.post("/pedidos", requireModule("novo_pedido"), uploadPedido.single("anexo"), (req, res) => {
  try {
    const cliente_nome = String(req.body.cliente_nome || "").trim();
    const cliente_contato = String(req.body.cliente_contato || "").trim();
    const vendedor_nome = String(req.body.vendedor_nome || "").trim();
    const prazo_entrega = String(req.body.prazo_entrega || "").trim();
    const prioridade = String(req.body.prioridade || "NORMAL").trim() || "NORMAL";
    const status = String(req.body.status || "ABERTO").trim() || "ABERTO";
    const observacoes_vendedor = String(req.body.observacoes_vendedor || "").trim();
    const observacoes_internas = String(req.body.observacoes_internas || "").trim();
    const anexo_arquivo = req.file ? req.file.filename : null;

    if (!cliente_nome) return res.status(400).send("Cliente é obrigatório.");

    const now = new Date().toISOString();
    const tx = db.transaction(() => {
      const info = db.prepare(`
        INSERT INTO pedidos
          (cliente_nome, cliente_contato, vendedor_nome, prazo_entrega, prioridade, status, observacoes_vendedor, observacoes_internas, anexo_arquivo, criado_em, atualizado_em)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      `).run(
        cliente_nome || null,
        cliente_contato || null,
        vendedor_nome || null,
        prazo_entrega || null,
        prioridade,
        status,
        observacoes_vendedor || null,
        observacoes_internas || null,
        anexo_arquivo,
        now, now
      );
      const pedido_id = info.lastInsertRowid;

      const descs = Array.isArray(req.body.item_descricao) ? req.body.item_descricao : (req.body.item_descricao ? [req.body.item_descricao] : []);
      const qtds  = Array.isArray(req.body.item_quantidade) ? req.body.item_quantidade : (req.body.item_quantidade ? [req.body.item_quantidade] : []);
      const unis  = Array.isArray(req.body.item_unidade) ? req.body.item_unidade : (req.body.item_unidade ? [req.body.item_unidade] : []);
      const obsis = Array.isArray(req.body.item_observacao) ? req.body.item_observacao : (req.body.item_observacao ? [req.body.item_observacao] : []);

      for (let i = 0; i < descs.length; i++) {
        const d = String(descs[i] || "").trim();
        if (!d) continue;
        const q = Number(qtds[i] || 0) || 0;
        const u = String(unis[i] || "").trim();
        const o = String(obsis[i] || "").trim();
        db.prepare(`
          INSERT INTO pedido_itens (pedido_id, descricao, quantidade, unidade, observacao)
          VALUES (?, ?, ?, ?, ?)
        `).run(pedido_id, d, q || null, u || null, o || null);
      }

      return pedido_id;
    });

    const pid = tx();
    res.redirect(`/pedidos/${pid}`);
  } catch (err) {
    console.error("Erro ao criar pedido:", err);
    res.status(500).send(err.message);
  }
});

app.get("/pedidos/:id", requireModule("pedidos"), (req, res) => {
  try {
    const id = Number(req.params.id);
    const pedido = db.prepare(`SELECT * FROM pedidos WHERE id = ?`).get(id);
    if (!pedido) return res.status(404).send("Pedido não encontrado.");
    const itens = db.prepare(`SELECT * FROM pedido_itens WHERE pedido_id = ? ORDER BY id ASC`).all(id);

    res.render("layout", {
      title: `Pedido #${id}`,
      view: "pedido-detalhe-page",
      pedido,
      itens
    });
  } catch (err) {
    console.error("Erro ao abrir pedido:", err);
    res.status(500).send(err.message);
  }
});

app.get("/pedidos/:id/editar", requireModule("pedidos"), (req, res) => {
  try {
    const id = Number(req.params.id);
    const pedido = db.prepare(`SELECT * FROM pedidos WHERE id = ?`).get(id);
    if (!pedido) return res.status(404).send("Pedido não encontrado.");
    const itens = db.prepare(`SELECT * FROM pedido_itens WHERE pedido_id = ? ORDER BY id ASC`).all(id);

    res.render("layout", {
      title: `Editar Pedido #${id}`,
      view: "pedido-form-page",
      pedido,
      itens,
      modo: "editar"
    });
  } catch (err) {
    console.error("Erro ao editar pedido:", err);
    res.status(500).send(err.message);
  }
});

app.post("/pedidos/:id", requireModule("pedidos"), uploadPedido.single("anexo"), (req, res) => {
  try {
    const id = Number(req.params.id);
    const pedidoAtual = db.prepare(`SELECT * FROM pedidos WHERE id = ?`).get(id);
    if (!pedidoAtual) return res.status(404).send("Pedido não encontrado.");

    const cliente_nome = String(req.body.cliente_nome || "").trim();
    const cliente_contato = String(req.body.cliente_contato || "").trim();
    const vendedor_nome = String(req.body.vendedor_nome || "").trim();
    const prazo_entrega = String(req.body.prazo_entrega || "").trim();
    const prioridade = String(req.body.prioridade || "NORMAL").trim() || "NORMAL";
    const status = String(req.body.status || "ABERTO").trim() || "ABERTO";
    const observacoes_vendedor = String(req.body.observacoes_vendedor || "").trim();
    const observacoes_internas = String(req.body.observacoes_internas || "").trim();
    const anexo_arquivo = req.file ? req.file.filename : (pedidoAtual.anexo_arquivo || null);
    const now = new Date().toISOString();

    if (!cliente_nome) return res.status(400).send("Cliente é obrigatório.");

    const tx = db.transaction(() => {
      db.prepare(`
        UPDATE pedidos SET
          cliente_nome = ?, cliente_contato = ?, vendedor_nome = ?, prazo_entrega = ?,
          prioridade = ?, status = ?, observacoes_vendedor = ?, observacoes_internas = ?,
          anexo_arquivo = ?, atualizado_em = ?
        WHERE id = ?
      `).run(
        cliente_nome || null,
        cliente_contato || null,
        vendedor_nome || null,
        prazo_entrega || null,
        prioridade,
        status,
        observacoes_vendedor || null,
        observacoes_internas || null,
        anexo_arquivo,
        now,
        id
      );

      db.prepare(`DELETE FROM pedido_itens WHERE pedido_id = ?`).run(id);

      const descs = Array.isArray(req.body.item_descricao) ? req.body.item_descricao : (req.body.item_descricao ? [req.body.item_descricao] : []);
      const qtds  = Array.isArray(req.body.item_quantidade) ? req.body.item_quantidade : (req.body.item_quantidade ? [req.body.item_quantidade] : []);
      const unis  = Array.isArray(req.body.item_unidade) ? req.body.item_unidade : (req.body.item_unidade ? [req.body.item_unidade] : []);
      const obsis = Array.isArray(req.body.item_observacao) ? req.body.item_observacao : (req.body.item_observacao ? [req.body.item_observacao] : []);

      for (let i = 0; i < descs.length; i++) {
        const d = String(descs[i] || "").trim();
        if (!d) continue;
        const q = Number(qtds[i] || 0) || 0;
        const u = String(unis[i] || "").trim();
        const o = String(obsis[i] || "").trim();
        db.prepare(`
          INSERT INTO pedido_itens (pedido_id, descricao, quantidade, unidade, observacao)
          VALUES (?, ?, ?, ?, ?)
        `).run(id, d, q || null, u || null, o || null);
      }
    });

    tx();
    res.redirect(`/pedidos/${id}`);
  } catch (err) {
    console.error("Erro ao atualizar pedido:", err);
    res.status(500).send(err.message);
  }
});

app.post("/pedidos/:id/cancelar", requireModule("pedidos"), (req, res) => {
  try {
    const id = Number(req.params.id);
    db.prepare(`UPDATE pedidos SET status = 'CANCELADO', atualizado_em = ? WHERE id = ?`).run(new Date().toISOString(), id);
    res.redirect(`/pedidos/${id}`);
  } catch (err) {
    console.error("Erro ao cancelar pedido:", err);
    res.status(500).send(err.message);
  }
});

app.post("/pedidos/:id/gerar-op", requireModule("pedidos"), (req, res) => {
  try {
    const id = Number(req.params.id);
    const pedido = db.prepare(`SELECT * FROM pedidos WHERE id = ?`).get(id);
    if (!pedido) return res.status(404).send("Pedido não encontrado.");

    if (pedido.op_id) {
      return res.redirect(`/ops/${pedido.op_id}`);
    }

    const itens = db.prepare(`SELECT * FROM pedido_itens WHERE pedido_id = ? ORDER BY id ASC`).all(id);
    const itensResumo = itens
      .map(it => {
        const qtd = (it.quantidade !== null && it.quantidade !== undefined && it.quantidade !== "") ? String(it.quantidade) : "";
        const un = (it.unidade || "").trim();
        const desc = (it.descricao || "").trim();
        const core = [qtd, un, desc].filter(Boolean).join(" ");
        return core || null;
      })
      .filter(Boolean)
      .join(" / ");

    const linhaPedido = `Pedido #${id}${pedido.vendedor_nome ? ` (Vendedor: ${pedido.vendedor_nome})` : ""}`;
    const linhaItens = itensResumo ? `Itens: ${itensResumo}` : "Itens: (não informado)";
    const linhaObs = pedido.observacoes_vendedor ? `Obs: ${pedido.observacoes_vendedor}` : "";
    const obsInterna = [linhaPedido, linhaItens, linhaObs].filter(Boolean).join("\\n");

    const codigo_op = gerarCodigoOP();

    const info = db.prepare(`
      INSERT INTO ordens_producao
        (codigo_op, cliente, vendedor_nome, prioridade, produto_final, quantidade_final, pedido_venda, data_abertura, data_entrega, observacao, observacao_interna, observacao_cliente, material_espessura_mm, material_cor, pecas_json)
      VALUES
        (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `).run(
      codigo_op,
      (pedido.cliente_nome || "").trim() || null,
      (pedido.vendedor_nome || "").trim() || null,
      (pedido.prioridade || "NORMAL").trim(),
      null,
      null,
      `PED-${id}`,
      new Date().toISOString().slice(0, 10),
      (pedido.prazo_entrega || "").trim() || null,
      (pedido.observacoes_vendedor || "").trim() || null,
      obsInterna,
      null,
      null,
      null,
      null
    );

    const op_id = info.lastInsertRowid;

    // Copia os itens do pedido (texto livre) para dentro da OP
    // (principalmente pedidos sincronizados do Bling/ML/Shopee)
    try {
      const stmtIns = db.prepare(`
        INSERT INTO ordem_itens_pedido (ordem_id, descricao, quantidade, unidade, observacao)
        VALUES (?, ?, ?, ?, ?)
      `);

      // Também salva um resumo em pecas_json para uso futuro (medidas podem ser preenchidas depois)
      const pecasPayload = [];

      for (const it of (itens || [])) {
        const desc = String(it.descricao || "").trim();
        if (!desc) continue;
        const qtd = Number(it.quantidade || 0) || 0;
        const un = String(it.unidade || "").trim() || null;
        const obs = String(it.observacao || "").trim() || null;

        stmtIns.run(op_id, desc, qtd > 0 ? qtd : 1, un, obs);
        pecasPayload.push({ nome: desc, medidas: null, quantidade: Math.max(1, Math.floor(qtd > 0 ? qtd : 1)) });
      }

      if (pecasPayload.length) {
        db.prepare(`UPDATE ordens_producao SET pecas_json = ? WHERE id = ?`).run(JSON.stringify(pecasPayload), op_id);
      }
    } catch (e) {
      console.warn("Aviso: não foi possível copiar itens do pedido para a OP:", e.message);
    }

    db.prepare(`UPDATE pedidos SET status = 'CONVERTIDO', op_id = ?, atualizado_em = ? WHERE id = ?`)
      .run(op_id, new Date().toISOString(), id);

    res.redirect(`/ops/${op_id}`);
  } catch (err) {
    console.error("Erro ao gerar OP do pedido:", err);
    res.status(500).send(err.message);
  }
});


app.get("/ops", (req, res) => {
  const q = (req.query.q || "").trim();
  const status = (req.query.status || "").trim();

  let sql = `
    SELECT op.*,
      (SELECT COUNT(*) FROM ordem_itens oi WHERE oi.ordem_id = op.id) as itens_count,
      (CASE WHEN op.status NOT IN ('FINALIZADA','CANCELADA') AND op.data_entrega IS NOT NULL AND date(op.data_entrega) < date('now') THEN 1 ELSE 0 END) as atrasada
    FROM ordens_producao op
    WHERE 1=1
      AND IFNULL(op.arquivada,0) = 0 AND IFNULL(op.excluida,0)=0
  `;
  const params = [];

  if (q) { sql += " AND (op.codigo_op LIKE ? OR op.cliente LIKE ?)"; params.push(`%${q}%`, `%${q}%`); }
  if (status) { sql += " AND op.status = ?"; params.push(status); }

  sql += " ORDER BY op.id DESC LIMIT 300";
  const ops = db.prepare(sql).all(...params);

  res.render("layout", { title: "Ordens de Produção", view: "ops-list-page",
    arquivadas: false, ops, q, status });
});

// ===== Arquivo de OPs (somente as OPs enviadas ao arquivo) =====
app.get("/ops/arquivadas", (req, res) => {
  const q = (req.query.q || "").trim();
  const where = ["IFNULL(op.arquivada,0) = 1", "IFNULL(op.excluida,0) = 0"];
  const params = {};

  if (q) {
    where.push("(op.codigo_op LIKE @q OR op.cliente LIKE @q OR op.pedido_venda LIKE @q OR op.produto_final LIKE @q)");
    params.q = `%${q}%`;
  }

  const sql = `
    SELECT
      op.*,
      (SELECT COUNT(*) FROM ordem_itens oi WHERE oi.ordem_id = op.id) as itens_count,
      CASE
        WHEN op.status NOT IN ('FINALIZADA','CANCELADA')
          AND op.data_entrega IS NOT NULL
          AND date(op.data_entrega) < date('now')
        THEN 1 ELSE 0 END as atrasada
    FROM ordens_producao op
    WHERE ${where.join(" AND ")}
    ORDER BY COALESCE(op.data_arquivada, op.id) DESC, op.id DESC
  `;

  const ops = db.prepare(sql).all(params);

  res.render("layout", {
    title: "Arquivo de OPs",
    view: "ops-list-page",
    ops,
    q,
    status: "",
    arquivadas: true
  });
});

// Enviar OP para o Arquivo (apenas FINALIZADA/CANCELADA)
app.post("/ops/:id(\\d+)/arquivar", (req, res) => {
  const id = Number(req.params.id);
  const op = db.prepare("SELECT id, status, arquivada FROM ordens_producao WHERE id = ?").get(id);
  if (!op) return res.redirect("/ops");

  if (Number(op.arquivada || 0) === 1) {
    return res.redirect(`/ops/arquivadas?ok=${encodeURIComponent("OP já estava arquivada.")}`);
  }

  const st = String(op.status || "").toUpperCase();
  if (st !== "FINALIZADA" && st !== "CANCELADA") {
    return res.redirect(`/ops/${id}?erro=${encodeURIComponent("Apenas OPs FINALIZADAS ou CANCELADAS podem ser arquivadas.")}`);
  }

  // Se for FINALIZADA, exige checklist 100% antes de arquivar
  if (st === "FINALIZADA") {
    ensureChecklistFinal(id);
    const row = db.prepare(
      "SELECT COUNT(*) as total, SUM(CASE WHEN concluido = 1 THEN 1 ELSE 0 END) as ok FROM op_checklist_final WHERE ordem_id = ?"
    ).get(id);
    const total = Number(row?.total || 0);
    const ok = Number(row?.ok || 0);
    if (total > 0 && ok < total) {
      return res.redirect(`/ops/${id}?erro=${encodeURIComponent("Checklist final incompleto. Conclua todos os itens antes de arquivar a OP.")}`);
    }
  }

  db.prepare("UPDATE ordens_producao SET arquivada = 1, data_arquivada = datetime('now') WHERE id = ?").run(id);
  return res.redirect(`/ops/arquivadas?ok=${encodeURIComponent("OP arquivada com sucesso.")}`);
});

// Excluir OP (permitido para: (a) OPs arquivadas; (b) OPs canceladas)
app.post("/ops/:id(\\d+)/excluir", (req, res) => {
  const id = Number(req.params.id);
  const op = db.prepare("SELECT * FROM ordens_producao WHERE id = ?").get(id);
	let pecas = [];
	try {
	  pecas = op && op.pecas_json ? JSON.parse(op.pecas_json) : [];
	  if (!Array.isArray(pecas)) pecas = [];
	} catch (e) { pecas = []; }

  if (!op) return res.redirect("/ops");

  const st = String(op.status || "").toUpperCase();
  const isArquivada = Number(op.arquivada || 0) === 1;
  const podeExcluir = isArquivada || st === "CANCELADA";

  if (!podeExcluir) {
    return res.redirect(`/ops/${id}?erro=${encodeURIComponent("Só é possível excluir OPs CANCELADAS ou que já estejam no Arquivo.")}`);
  }

  // captura anexos para remover do disco depois
  const anexos = db.prepare("SELECT filename FROM op_anexos WHERE ordem_id = ?").all(id);

  if (IS_PG()) {
    try {
      // Remove dependências conhecidas
      try { db.prepare("DELETE FROM op_brindes WHERE op_id=?").run(id); } catch (_) {}
      try { db.prepare("DELETE FROM brindes_movimentacoes WHERE op_id=?").run(id); } catch (_) {}
      try { db.prepare("DELETE FROM op_servicos WHERE ordem_id=?").run(id); } catch (_) {}
      try { db.prepare("DELETE FROM op_insumos WHERE ordem_id=?").run(id); } catch (_) {}
      try { db.prepare("DELETE FROM op_checklist_final WHERE ordem_id=?").run(id); } catch (_) {}
      try { db.prepare("DELETE FROM op_checklist_assinatura WHERE ordem_id=?").run(id); } catch (_) {}
      try { db.prepare("DELETE FROM op_anexos WHERE ordem_id=?").run(id); } catch (_) {}
      db.prepare("DELETE FROM ordens_producao WHERE id=?").run(id);

      for (const a of anexos) {
        try {
          if (!a?.filename) continue;
          const fp = path.join(uploadsDir, String(a.filename));
          if (fs.existsSync(fp)) fs.unlinkSync(fp);
        } catch (_) {}
      }

      return res.redirect(`/ops?ok=${encodeURIComponent("OP excluída definitivamente.")}`);
    } catch (e) {
      return res.redirect(`/ops/${id}?erro=${encodeURIComponent(e.message || e)}`);
    }
  }

  const tx = db.transaction(() => {
    // Apaga de forma dinâmica todas as tabelas que possuem FK para ordens_producao
    const fkTables = db.prepare(`
      SELECT name FROM sqlite_master
      WHERE type='table'
        AND sql LIKE '%REFERENCES ordens_producao%'
    `).all();

    for (const t of fkTables) {
      const table = String(t.name || '').trim();
      if (!/^[a-zA-Z0-9_]+$/.test(table)) continue;

      const fks = db.prepare(`PRAGMA foreign_key_list(${table})`).all();
      const cols = fks
        .filter(fk => fk.table === 'ordens_producao')
        .map(fk => fk.from)
        .filter(Boolean);

      for (const col of cols) {
        if (!/^[a-zA-Z0-9_]+$/.test(col)) continue;
        db.prepare(`DELETE FROM ${table} WHERE ${col} = ?`).run(id);
      }
    }

    // Por fim, apaga a OP
    db.prepare('DELETE FROM ordens_producao WHERE id=?').run(id);
  });

  try {
    tx();

    // remove arquivos anexos do disco (pasta uploads em AppData)
    for (const a of anexos) {
      try {
        if (!a?.filename) continue;
        const fp = path.join(uploadsDir, String(a.filename));
        if (fs.existsSync(fp)) fs.unlinkSync(fp);
      } catch (e) {}
    }

    // volta para a tela adequada
    if (isArquivada) return res.redirect(`/ops/arquivadas?ok=${encodeURIComponent("OP excluída definitivamente.")}`);
    return res.redirect(`/ops?ok=${encodeURIComponent("OP excluída definitivamente.")}`);
  } catch (e) {
    console.error(e);
    return res.redirect(`/ops/${id}?erro=${encodeURIComponent("Não foi possível excluir definitivamente. Existem vínculos.")}`);
  }
});



app.get("/ops/nova", (req, res) => {
  let insumos = [];
  try {
    insumos = db.prepare("SELECT id, nome, unidade, estoque_atual, estoque_minimo FROM insumos ORDER BY nome ASC").all();
  } catch (e) {
    // se não existir tabela ainda, só ignora (ex.: primeiro boot antes do módulo)
    insumos = [];
  }
  let servicosCatalogo = [];
  try {
    servicosCatalogo = db.prepare("SELECT id, nome, unidade, preco_unit, ativo FROM servicos WHERE ativo=1 ORDER BY nome ASC").all();
  } catch (e) {
    servicosCatalogo = [];
  }
    let brindesCatalogo = [];
  try {
    brindesCatalogo = db.prepare("SELECT id, nome, unidade, estoque_atual, estoque_minimo, ativo FROM brindes WHERE ativo=1 ORDER BY nome ASC").all();
  } catch (e) {
    brindesCatalogo = [];
  }
  res.render("layout", { title: "Nova OP", view: "ops-nova-page", insumos, servicosCatalogo, brindesCatalogo });
});
app.post("/ops", (req, res) => {
  const { cliente, prioridade, data_entrega, observacao_interna, observacao_cliente, produto_final, quantidade_final, pedido_venda, material_espessura_mm, material_cor } = req.body;
  const observacao = (observacao_interna || "").toString(); // compat: campo antigo "observacao"
  const codigo_op = gerarCodigoOP();

// Peças (nome + medida + quantidade) vindas do formulário
const pNomesRaw = req.body.peca_nome || req.body.pecas_nome || [];
const pMedidasRaw = req.body.peca_medidas || req.body.pecas_medidas || req.body.peca_medida || req.body.pecas_medida || [];
const pQtdRaw = req.body.peca_qtd || req.body.pecas_qtd || req.body.peca_quantidade || req.body.pecas_quantidade || [];

const pNomes = Array.isArray(pNomesRaw) ? pNomesRaw : [pNomesRaw];
const pMedidas = Array.isArray(pMedidasRaw) ? pMedidasRaw : [pMedidasRaw];
const pQtds = Array.isArray(pQtdRaw) ? pQtdRaw : [pQtdRaw];

const pecas = [];
const pLen = Math.max(pNomes.length, pMedidas.length, pQtds.length);
for (let i = 0; i < pLen; i++) {
  const nome = (pNomes[i] ?? "").toString().trim();
  const medida = (pMedidas[i] ?? "").toString().trim();
  let quantidade = Number(pQtds[i] ?? 1);
  if (!Number.isFinite(quantidade) || quantidade <= 0) quantidade = 1;
  quantidade = Math.floor(quantidade);

  if (nome || medida) pecas.push({ nome: nome || null, medidas: medida || null, quantidade });
}
const pecas_json = pecas.length ? JSON.stringify(pecas) : null;



  const info = db.prepare(`
    INSERT INTO ordens_producao
      (codigo_op, cliente, vendedor_nome, prioridade, produto_final, quantidade_final, pedido_venda, data_abertura, data_entrega, observacao, observacao_interna, observacao_cliente, material_espessura_mm, material_cor, pecas_json)
    VALUES
      (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `).run(
    codigo_op,
    (cliente || "").trim() || null,
    (prioridade || "NORMAL").trim(),
    (produto_final || "").trim() || null,
    (quantidade_final ? Number(quantidade_final) : null),
    (pedido_venda || "").trim() || null,
    new Date().toISOString().slice(0, 10),
    (data_entrega || "").trim() || null,
    (observacao || "").trim() || null,
     (observacao_interna || "").trim() || null,
     (observacao_cliente || "").trim() || null,
     (material_espessura_mm ? Number(material_espessura_mm) : null),
    (material_cor || "").trim() || null,
    pecas_json
  );

  const opId = info.lastInsertRowid;

  // Brindes vinculados na abertura da OP (opcional) - NÃO dá baixa aqui (baixa ao FINALIZAR)
  const bIdsRaw = req.body.brinde_id || [];
  const bQtdsRaw = req.body.brinde_qtd || [];
  const bIds = Array.isArray(bIdsRaw) ? bIdsRaw : [bIdsRaw];
  const bQtds = Array.isArray(bQtdsRaw) ? bQtdsRaw : [bQtdsRaw];

  try {
    const del = db.prepare("DELETE FROM op_brindes WHERE op_id = ?");
    del.run(opId);
    const ins = db.prepare("INSERT INTO op_brindes (op_id, brinde_id, quantidade) VALUES (?,?,?)");
    const tx = db.transaction(() => {
      for (let i = 0; i < Math.max(bIds.length, bQtds.length); i++) {
        const brindeId = Number(bIds[i] || 0);
        let qtd = Number(bQtds[i] || 0);
        if (!Number.isFinite(qtd) || qtd <= 0) qtd = 0;
        qtd = Math.floor(qtd);
        if (brindeId > 0 && qtd > 0) ins.run(opId, brindeId, qtd);
      }
    });
    tx();
  } catch (e) {
    // se ainda não existir tabela (primeiro boot), ignora
  }


  // Serviços usados na abertura da OP (opcional)
  const sIdsRaw = req.body.servico_id || [];
  const sQtdsRaw = req.body.servico_qtd || [];
  const sPrecosRaw = req.body.servico_preco || [];
  const sObsRaw = req.body.servico_obs || [];

  const sIds = Array.isArray(sIdsRaw) ? sIdsRaw : [sIdsRaw];
  const sQtds = Array.isArray(sQtdsRaw) ? sQtdsRaw : [sQtdsRaw];
  const sPrecos = Array.isArray(sPrecosRaw) ? sPrecosRaw : [sPrecosRaw];
  const sObs = Array.isArray(sObsRaw) ? sObsRaw : [sObsRaw];

  try {
    for (let i = 0; i < sIds.length; i++) {
      const servico_id = Number(sIds[i] || 0);
      const quantidade = Number(sQtds[i] || 0);
      const preco_unit = Number(sPrecos[i] || 0);
      const observacao = (sObs[i] || "").trim();
      if (!servico_id || !quantidade || quantidade <= 0) continue;

      db.prepare("INSERT INTO op_servicos (ordem_id, servico_id, quantidade, preco_unit, observacao) VALUES (?, ?, ?, ?, ?)")
        .run(opId, servico_id, quantidade, preco_unit, observacao);
    }
  } catch (e) {
    console.error("Erro ao integrar serviços na OP:", e);
  }

  // Insumos usados na abertura da OP (opcional)
  const idsRaw = req.body.insumo_id || [];
  const qtdsRaw = req.body.insumo_qtd || [];

  const ids = Array.isArray(idsRaw) ? idsRaw : [idsRaw];
  const qtds = Array.isArray(qtdsRaw) ? qtdsRaw : [qtdsRaw];

  try {
    for (let i = 0; i < ids.length; i++) {
      const insumo_id = Number(ids[i] || 0);
      const quantidade = Number(qtds[i] || 0);
      if (!insumo_id || !quantidade || quantidade <= 0) continue;

      // registra vínculo OP x Insumo
      db.prepare("INSERT INTO op_insumos (ordem_id, insumo_id, quantidade) VALUES (?, ?, ?)").run(opId, insumo_id, quantidade);

      // registra movimentação e baixa estoque
      db.prepare(
        "INSERT INTO insumos_movimentacoes (insumo_id, tipo, quantidade, motivo) VALUES (?, 'saida', ?, ?)"
      ).run(insumo_id, quantidade, `OP ${codigo_op}`);

      db.prepare(
        "UPDATE insumos SET estoque_atual = COALESCE(estoque_atual,0) - ? WHERE id=?"
      ).run(quantidade, insumo_id);
    }
  } catch (e) {
    console.error("Erro ao integrar insumos na OP:", e);
  }

  res.redirect(`/ops/${opId}`);
});

app.get("/ops/:id(\\d+)", (req, res) => {
  const id = Number(req.params.id);
  const op = db.prepare("SELECT op.* FROM ordens_producao op WHERE id = ?").get(id);
let pecas = [];
try {
  pecas = op && op.pecas_json ? JSON.parse(op.pecas_json) : [];
  if (!Array.isArray(pecas)) pecas = [];
  pecas = (pecas || []).map(p => ({ nome: p?.nome || null, medidas: p?.medidas || p?.medida || null, quantidade: Math.max(1, Math.floor(Number(p?.quantidade ?? p?.qtd ?? 1) || 1)) }));
} catch (e) { pecas = []; }

  if (!op) return res.status(404).send("OP não encontrada");

  // Checklist final (garante itens padrão)
  ensureChecklistFinal(id);
  const checklist = db.prepare(
    "SELECT item, concluido, data_conclusao FROM op_checklist_final WHERE ordem_id = ?"
  ).all(id);
  const checklistMap = new Map((checklist || []).map(r => [r.item, r]));
  const checklistOrdenado = CHECKLIST_FINAL_ITENS.map(item => {
    const r = checklistMap.get(item) || { item, concluido: 0, data_conclusao: null };
    return { item, concluido: Number(r.concluido || 0), data_conclusao: r.data_conclusao || null };
  });
  const checklistConcluidos = checklistOrdenado.reduce((acc, r) => acc + (r.concluido ? 1 : 0), 0);
  const checklistTotal = checklistOrdenado.length;

  const assinaturaChecklist = db.prepare(
    'SELECT responsavel, data_assinatura FROM op_checklist_assinatura WHERE ordem_id = ?'
  ).get(id) || null;

  const itens = db.prepare(`
    SELECT oi.*, p.descricao, p.cor, p.espessura_mm, p.codigo_interno, p.estoque_atual
    FROM ordem_itens oi
    JOIN produtos p ON p.id = oi.produto_id
    WHERE oi.ordem_id = ?
    ORDER BY oi.id DESC
  `).all(id);

  // Itens do pedido (texto livre) que foram puxados na conversão Pedido -> OP
  let itensPedido = [];
  try {
    itensPedido = db.prepare(`
      SELECT id, descricao, quantidade, unidade, observacao
      FROM ordem_itens_pedido
      WHERE ordem_id = ?
      ORDER BY id DESC
    `).all(id);
  } catch (e) {
    itensPedido = [];
  }

  const produtos = db.prepare(`
    SELECT id, descricao, cor, espessura_mm, codigo_interno, estoque_atual
    FROM produtos
    ORDER BY descricao ASC
  `).all();

  const anexos = db.prepare(`
    SELECT * FROM op_anexos
    WHERE ordem_id = ?
    ORDER BY id DESC
  `).all(id);

  
  let insumosUsados = [];
  try {
    insumosUsados = db.prepare(`
      SELECT oi.id, oi.quantidade, i.id as insumo_id, i.nome, i.unidade
      FROM op_insumos oi
      JOIN insumos i ON i.id = oi.insumo_id
      WHERE oi.ordem_id = ?
      ORDER BY oi.id DESC
    `).all(id);
  } catch (e) {
    insumosUsados = [];
  }


  let servicosUsados = [];
  try {
    servicosUsados = db.prepare(`
      SELECT os.id, os.quantidade, os.preco_unit, os.observacao,
             s.nome, s.unidade
      FROM op_servicos os
      JOIN servicos s ON s.id = os.servico_id
      WHERE os.ordem_id = ?
      ORDER BY os.id DESC
    `).all(id);
  } catch (e) {
    servicosUsados = [];
  }

  // Catálogo de serviços (para edição na tela da OP)
  let servicosCatalogo = [];
  try {
    servicosCatalogo = db.prepare("SELECT id, nome, unidade, preco_unit, ativo FROM servicos WHERE ativo=1 ORDER BY nome ASC").all();
  } catch (e) {
    servicosCatalogo = [];
  }

  // Totais (Nível 2): total de serviços
  const totalServicos = (servicosUsados || []).reduce((acc, r) => {
    return acc + (Number(r.preco_unit || 0) * Number(r.quantidade || 0));
  }, 0);

  let brindesUsados = [];
  try {
    brindesUsados = db.prepare(`
      SELECT ob.id, ob.quantidade, b.id as brinde_id, b.nome, b.unidade
      FROM op_brindes ob
      JOIN brindes b ON b.id = ob.brinde_id
      WHERE ob.op_id = ?
      ORDER BY ob.id DESC
    `).all(id);
  } catch (e) {
    brindesUsados = [];
  }

  let brindesCatalogo = [];
  try {
    brindesCatalogo = db.prepare("SELECT id, nome, unidade, ativo FROM brindes WHERE ativo=1 ORDER BY nome ASC").all();
  } catch (e) {
    brindesCatalogo = [];
  }


  // Itens do pedido dentro da OP (texto livre: Bling/ML/Shopee)
  // IMPORTANTE: esta variável precisa existir sempre para o EJS não quebrar.
  let itensPedido = [];
  try {
    itensPedido = db.prepare(`
      SELECT id, descricao, quantidade, unidade, observacao
      FROM ordem_itens_pedido
      WHERE ordem_id = ?
      ORDER BY id ASC
    `).all(id);
    if (!Array.isArray(itensPedido)) itensPedido = [];
  } catch (e) {
    itensPedido = [];
  }

  res.render("layout", {
    title: `OP ${op.codigo_op}`,
    view: "ops-detalhe-page",
    op,
    itens,
    itensPedido,
    produtos,
    anexos,
    insumosUsados,
    servicosUsados,
		servicosCatalogo,
		totalServicos,
    brindesUsados,
    brindesCatalogo,
    checklist: checklistOrdenado,
    checklistConcluidos,
    checklistTotal,
    assinaturaChecklist,
    pecas
  });
});

// Salvar serviços da OP (edição direto na tela da OP)
app.post("/ops/:id(\\d+)/servicos", (req, res) => {
  const id = Number(req.params.id);
  const op = db.prepare("SELECT id, status FROM ordens_producao WHERE id = ?").get(id);
  if (!op) return res.status(404).send("OP não encontrada");

  const st = String(op.status || "").toUpperCase();
  // Mantém edição liberada para a maioria dos status; bloqueia só quando já está entregue/arquivada
  if (st === "ENTREGUE") {
    return res.redirect(`/ops/${id}?erro=${encodeURIComponent("OP ENTREGUE não pode ser editada.")}`);
  }

  // Normaliza payload (campos podem vir como string ou array)
  const toArr = (v) => Array.isArray(v) ? v : (v !== undefined && v !== null ? [v] : []);
  const servicoIds = toArr(req.body.servico_id);
  const qtds = toArr(req.body.servico_qtd);
  const precos = toArr(req.body.servico_preco);
  const obss = toArr(req.body.servico_obs);

  try {
    db.exec("BEGIN");
    db.prepare("DELETE FROM op_servicos WHERE ordem_id = ?").run(id);

    const ins = db.prepare(`
      INSERT INTO op_servicos (ordem_id, servico_id, quantidade, preco_unit, observacao)
      VALUES (?, ?, ?, ?, ?)
    `);

    for (let i = 0; i < servicoIds.length; i++) {
      const sid = Number(servicoIds[i] || 0);
      if (!sid) continue;
      const qtd = Number(String(qtds[i] ?? 1).replace(",", ".")) || 1;
      const preco = Number(String(precos[i] ?? 0).replace(",", ".")) || 0;
      const obs = (obss[i] || "").toString().trim() || null;
      ins.run(id, sid, qtd, preco, obs);
    }

    db.exec("COMMIT");
    return res.redirect(`/ops/${id}?ok=${encodeURIComponent("Serviços salvos.")}`);
  } catch (e) {
    try { db.exec("ROLLBACK"); } catch (_) {}
    console.error("Erro ao salvar serviços da OP:", e);
    return res.redirect(`/ops/${id}?erro=${encodeURIComponent(e.message || e)}`);
  }
});

// Atualizar material e peças da OP
        app.post("/ops/:id(\\d+)/material", (req, res) => {
          const id = Number(req.params.id);
          const op = db.prepare("SELECT id, codigo_op FROM ordens_producao WHERE id = ?").get(id);
          if (!op) return res.status(404).send("OP não encontrada");

          const esp = req.body.material_espessura_mm ? Number(req.body.material_espessura_mm) : null;
          const cor = (req.body.material_cor || "").trim() || null;

          const pNomesRaw = req.body.peca_nome || [];
          const pMedidasRaw = req.body.peca_medidas || [];
          const pQtdRaw = req.body.peca_qtd || req.body.peca_quantidade || [];

          const pNomes = Array.isArray(pNomesRaw) ? pNomesRaw : [pNomesRaw];
          const pMedidas = Array.isArray(pMedidasRaw) ? pMedidasRaw : [pMedidasRaw];
          const pQtds = Array.isArray(pQtdRaw) ? pQtdRaw : [pQtdRaw];

          const pecas = [];
          for (let i = 0; i < Math.max(pNomes.length, pMedidas.length, pQtds.length); i++) {
            const nome = String(pNomes[i] || "").trim();
            const medidas = String(pMedidas[i] || "").trim();
            let quantidade = Number(pQtds[i] ?? 1);
            if (!Number.isFinite(quantidade) || quantidade <= 0) quantidade = 1;
            quantidade = Math.floor(quantidade);
            if (!nome && !medidas) continue;
            pecas.push({ nome: nome || "Peça", medidas, quantidade });
          }

          const pecas_json = pecas.length ? JSON.stringify(pecas) : null;

          db.prepare("UPDATE ordens_producao SET material_espessura_mm=?, material_cor=?, pecas_json=? WHERE id=?")
            .run(esp, cor, pecas_json, id);

          res.redirect(`/ops/${id}`);
        });


// Atualizar brindes vinculados na OP (não dá baixa aqui)
app.post("/ops/:id(\\d+)/brindes", (req, res) => {
  const opId = Number(req.params.id);
  const op = db.prepare("SELECT id, codigo_op, status, brindes_baixados FROM ordens_producao WHERE id = ?").get(opId);
  if (!op) return res.status(404).send("OP não encontrada");

  // Se já baixou brindes, bloqueia edição para evitar inconsistência
  if (Number(op.brindes_baixados || 0) === 1) {
    return res.redirect(`/ops/${opId}?erro=` + encodeURIComponent("Esta OP já teve os brindes baixados no estoque. Não é possível alterar a lista de brindes."));
  }

  const bIdsRaw = req.body.brinde_id || [];
  const bQtdsRaw = req.body.brinde_qtd || [];
  const bIds = Array.isArray(bIdsRaw) ? bIdsRaw : [bIdsRaw];
  const bQtds = Array.isArray(bQtdsRaw) ? bQtdsRaw : [bQtdsRaw];

  try {
    const del = db.prepare("DELETE FROM op_brindes WHERE op_id = ?");
    const ins = db.prepare("INSERT INTO op_brindes (op_id, brinde_id, quantidade) VALUES (?,?,?)");
    const tx = db.transaction(() => {
      del.run(opId);
      for (let i = 0; i < Math.max(bIds.length, bQtds.length); i++) {
        const brindeId = Number(bIds[i] || 0);
        let qtd = Number(bQtds[i] || 0);
        if (!Number.isFinite(qtd) || qtd <= 0) qtd = 0;
        qtd = Math.floor(qtd);
        if (brindeId > 0 && qtd > 0) ins.run(opId, brindeId, qtd);
      }
    });
    tx();
  } catch (e) {
    return res.redirect(`/ops/${opId}?erro=` + encodeURIComponent("Erro ao salvar brindes: " + (e.message || e)));
  }

  res.redirect(`/ops/${opId}`);
});

// Atualizar observações (interna x cliente)
app.post("/ops/:id(\\d+)/observacoes", (req, res) => {
  const id = Number(req.params.id);
  const op = db.prepare("SELECT id FROM ordens_producao WHERE id = ?").get(id);
  if (!op) return res.status(404).send("OP não encontrada");

  const obsCliente = (req.body.observacao_cliente || "").toString().trim() || null;
  const obsInterna = (req.body.observacao_interna || "").toString().trim() || null;

  // compat: mantém o campo antigo "observacao" como espelho do interno
  db.prepare("UPDATE ordens_producao SET observacao_cliente=?, observacao_interna=?, observacao=? WHERE id=?")
    .run(obsCliente, obsInterna, obsInterna, id);

  res.redirect(`/ops/${id}`);
});


// Marcar/desmarcar checklist final
app.post("/ops/:id(\\d+)/checklist", (req, res) => {
  const ordemId = Number(req.params.id);
  const item = String(req.body.item || "").trim();
  const concluido = req.body.concluido === "1" ? 1 : 0;

  if (!CHECKLIST_FINAL_ITENS.includes(item)) {
    return res.redirect(`/ops/${ordemId}?erro=${encodeURIComponent("Item de checklist inválido.")}`);
  }

  const op = db.prepare("SELECT id FROM ordens_producao WHERE id = ?").get(ordemId);
  if (!op) return res.status(404).send("OP não encontrada");

  ensureChecklistFinal(ordemId);
  db.prepare(
    "UPDATE op_checklist_final SET concluido = ?, data_conclusao = CASE WHEN ? = 1 THEN datetime('now') ELSE NULL END WHERE ordem_id = ? AND item = ?"
  ).run(concluido, concluido, ordemId, item);

  res.redirect(`/ops/${ordemId}#checklist`);
});

// Assinar checklist final (responsável)
app.post('/ops/:id(\\d+)/checklist/assinar', (req, res) => {
  const ordemId = Number(req.params.id);
  const responsavel = String(req.body.responsavel || '').trim();
  if (!responsavel) {
    return res.redirect(`/ops/${ordemId}?erro=${encodeURIComponent('Informe o nome do responsável para assinar.') }#checklist`);
  }

  // Só permite assinar quando checklist estiver 100% concluído
  try { ensureChecklistFinal(ordemId); } catch (e) {}
  const r = db.prepare(
    'SELECT COUNT(*) as total, SUM(CASE WHEN concluido = 1 THEN 1 ELSE 0 END) as ok FROM op_checklist_final WHERE ordem_id = ?'
  ).get(ordemId);
  const total = Number(r?.total || 0);
  const ok = Number(r?.ok || 0);
  if (total > 0 && ok < total) {
    return res.redirect(`/ops/${ordemId}?erro=${encodeURIComponent('Conclua 100% do checklist antes de assinar.') }#checklist`);
  }

  db.prepare(`
    INSERT INTO op_checklist_assinatura (ordem_id, responsavel, data_assinatura)
    VALUES (?, ?, datetime('now'))
    ON CONFLICT(ordem_id) DO UPDATE SET
      responsavel = excluded.responsavel,
      data_assinatura = datetime('now')
  `).run(ordemId, responsavel);

  res.redirect(`/ops/${ordemId}#checklist`);
});

// Limpar assinatura do checklist
app.post('/ops/:id(\\d+)/checklist/limpar-assinatura', (req, res) => {
  const ordemId = Number(req.params.id);
  db.prepare('DELETE FROM op_checklist_assinatura WHERE ordem_id = ?').run(ordemId);
  res.redirect(`/ops/${ordemId}#checklist`);
});

app.post("/ops/:id(\\d+)/itens", (req, res) => {
  const ordemId = Number(req.params.id);
  const { produto_id, quantidade, observacao } = req.body;

  const pid = Number(produto_id);
  const qtd = Number(quantidade);

  if (!Number.isInteger(pid) || pid <= 0) return res.status(400).send("Produto inválido");
  if (!Number.isInteger(qtd) || qtd <= 0) return res.status(400).send("Quantidade inválida");

  const op = db.prepare("SELECT id FROM ordens_producao WHERE id = ?").get(ordemId);
  if (!op) return res.status(404).send("OP não encontrada");

  db.prepare(`
    INSERT INTO ordem_itens (ordem_id, produto_id, quantidade, observacao)
    VALUES (?, ?, ?, ?)
  `).run(ordemId, pid, qtd, (observacao || "").trim() || null);

  res.redirect(`/ops/${ordemId}`);
});

app.post("/ops/:id(\\d+)/itens/:itemId/remover", (req, res) => {
  const ordemId = Number(req.params.id);
  const itemId = Number(req.params.itemId);
  db.prepare("DELETE FROM ordem_itens WHERE id = ? AND ordem_id = ?").run(itemId, ordemId);
  res.redirect(`/ops/${ordemId}`);
});

app.post("/ops/:id(\\d+)/anexos", (req, res) => {
  const ordemId = Number(req.params.id);
  const op = db.prepare("SELECT id FROM ordens_producao WHERE id = ?").get(ordemId);
  if (!op) return res.status(404).send("OP não encontrada");

  upload.single("imagem")(req, res, (err) => {
    if (err) return res.status(400).send(err.message);
    if (!req.file) return res.status(400).send("Selecione uma imagem.");

    db.prepare(`
      INSERT INTO op_anexos (ordem_id, filename, original_name, mime)
      VALUES (?, ?, ?, ?)
    `).run(ordemId, req.file.filename, req.file.originalname || null, req.file.mimetype || null);

    res.redirect(`/ops/${ordemId}`);
  });
});

app.post("/ops/:id(\\d+)/anexos/:anexoId/remover", (req, res) => {
  const ordemId = Number(req.params.id);
  const anexoId = Number(req.params.anexoId);

  const anexo = db.prepare("SELECT * FROM op_anexos WHERE id = ? AND ordem_id = ?").get(anexoId, ordemId);
  if (anexo) {
    const filePath = path.join(uploadsDir, anexo.filename);
    try { if (fs.existsSync(filePath)) fs.unlinkSync(filePath); } catch (e) {}
    db.prepare("DELETE FROM op_anexos WHERE id = ? AND ordem_id = ?").run(anexoId, ordemId);
  }
  res.redirect(`/ops/${ordemId}`);
});

app.post("/ops/:id(\\d+)/status", (req, res) => {
  const user = req.session?.user;
  if (!canEditCalendar(user)) return res.status(403).send("Sem permissão.");

  const ordemId = Number(req.params.id);
  const { status } = req.body;
  const allowed = ["ABERTA", "EM_PRODUCAO", "AGUARDANDO_MATERIAL", "FINALIZADA", "CANCELADA"];
  if (!allowed.includes(status)) return res.status(400).send("Status inválido");

  const before = db.prepare("SELECT status FROM ordens_producao WHERE id = ?").get(ordemId);

  // Se for FINALIZADA, baixar brindes (uma única vez)
  if (status === "FINALIZADA") {
    const op = db.prepare("SELECT id, brindes_baixados FROM ordens_producao WHERE id = ?").get(ordemId);
    if (!op) return res.status(404).send("OP não encontrada");

    if (Number(op.brindes_baixados || 0) === 0) {
      try {
        const itens = db.prepare(`
          SELECT ob.brinde_id, ob.quantidade, b.nome, b.estoque_atual
          FROM op_brindes ob
          JOIN brindes b ON b.id = ob.brinde_id
          WHERE ob.op_id = ?
        `).all(ordemId);

        // valida saldo
        for (const it of (itens || [])) {
          const saldo = Number(it.estoque_atual || 0);
          const qtd = Math.floor(Number(it.quantidade || 0));
          if (qtd > 0 && saldo < qtd) {
            return res.redirect(`/ops/${ordemId}?erro=` + encodeURIComponent(`Estoque insuficiente do brinde "${it.nome}". Saldo: ${saldo}. Necessário: ${qtd}.`));
          }
        }

        const tx = db.transaction(() => {
          for (const it of (itens || [])) {
            const qtd = Math.floor(Number(it.quantidade || 0));
            if (qtd <= 0) continue;
            const saldo = Number(it.estoque_atual || 0);
            const novo = saldo - qtd;
            db.prepare("UPDATE brindes SET estoque_atual = ? WHERE id = ?").run(novo, it.brinde_id);
            db.prepare("INSERT INTO brindes_movimentacoes (brinde_id, tipo, quantidade, observacao, op_id) VALUES (?,?,?,?,?)")
              .run(it.brinde_id, "saida", qtd, "Baixa automática por OP FINALIZADA", ordemId);
          }
          db.prepare("UPDATE ordens_producao SET brindes_baixados = 1 WHERE id = ?").run(ordemId);
        });
        tx();
      } catch (e) {
        return res.redirect(`/ops/${ordemId}?erro=` + encodeURIComponent("Erro ao baixar brindes: " + (e.message || e)));
      }
    }
  }

  const before = db.prepare("SELECT status FROM ordens_producao WHERE id = ?").get(ordemId);
  db.prepare("UPDATE ordens_producao SET status = ? WHERE id = ?").run(status, ordemId);
  logOpChange(ordemId, user, 'STATUS', 'status', before ? before.status : null, status);
  res.redirect(`/ops/${ordemId}`);
});

app.get("/ops/:id(\\d+)/imprimir", (req, res) => {
  const id = Number(req.params.id);
  const op = db.prepare("SELECT * FROM ordens_producao WHERE id = ?").get(id);
  if (!op) return res.status(404).send("OP não encontrada");

  // Peças (JSON)
  let pecas = [];
  try {
    pecas = JSON.parse(op.pecas_json || "[]");
    if (!Array.isArray(pecas)) pecas = [];
    pecas = (pecas || []).map(p => ({ nome: p?.nome || null, medidas: p?.medidas || p?.medida || null, quantidade: Math.max(1, Math.floor(Number(p?.quantidade ?? p?.qtd ?? 1) || 1)) }));
  } catch (e) { pecas = []; }


  // Checklist final (para impressão)
  try { ensureChecklistFinal(id); } catch (e) {}
  const checklistFinal = db.prepare(`
    SELECT item, concluido, data_conclusao
    FROM op_checklist_final
    WHERE ordem_id = ?
    ORDER BY id ASC
  `).all(id);

  const assinaturaChecklist = db.prepare(
    'SELECT responsavel, data_assinatura FROM op_checklist_assinatura WHERE ordem_id = ?'
  ).get(id) || null;

  const itens = db.prepare(`
    SELECT oi.*, p.descricao, p.cor, p.espessura_mm, p.codigo_interno
    FROM ordem_itens oi
    JOIN produtos p ON p.id = oi.produto_id
    WHERE oi.ordem_id = ?
    ORDER BY oi.id ASC
  `).all(id);

  // Insumos usados
  let insumosUsados = [];
  try {
    insumosUsados = db.prepare(`
      SELECT oi.id, oi.quantidade, i.nome, i.unidade
      FROM op_insumos oi
      JOIN insumos i ON i.id = oi.insumo_id
      WHERE oi.ordem_id = ?
      ORDER BY oi.id ASC
    `).all(id);
  } catch (e) { insumosUsados = []; }

  // Serviços usados
  let servicosUsados = [];
  try {
    servicosUsados = db.prepare(`
      SELECT os.id, os.quantidade, os.preco_unit, os.observacao, s.nome, s.unidade
      FROM op_servicos os
      JOIN servicos s ON s.id = os.servico_id
      WHERE os.ordem_id = ?
      ORDER BY os.id ASC
    `).all(id);
  } catch (e) { servicosUsados = []; }

  const anexos = db.prepare("SELECT * FROM op_anexos WHERE ordem_id = ? ORDER BY id DESC").all(id);
  
  let brindesUsados = [];
  try {
    brindesUsados = db.prepare(`
      SELECT ob.id, ob.quantidade, b.nome, b.unidade
      FROM op_brindes ob
      JOIN brindes b ON b.id = ob.brinde_id
      WHERE ob.op_id = ?
      ORDER BY ob.id ASC
    `).all(id);
  } catch (e) {
    brindesUsados = [];
  }

const _logoInfo = resolveBrandLogoPath();
  const logoUrl = (_logoInfo && _logoInfo.rel) ? ("/" + _logoInfo.rel.replace(/\\/g, "/")) : (_logoInfo ? "/logo.png" : null);
  res.render("ops-print", { op, itens, anexos, insumosUsados, servicosUsados, brindesUsados, checklistFinal, assinaturaChecklist, logoUrl, pecas });
});


app.get("/ops/:id(\\d+)/pdf", (req, res) => {
  const id = Number(req.params.id);
  const op = db.prepare("SELECT * FROM ordens_producao WHERE id = ?").get(id);
  if (!op) return res.status(404).send("OP não encontrada");

  // Peças (JSON)
  let pecas = [];
  try {
    pecas = JSON.parse(op.pecas_json || "[]");
    if (!Array.isArray(pecas)) pecas = [];
    pecas = (pecas || []).map(p => ({ nome: p?.nome || null, medidas: p?.medidas || p?.medida || null, quantidade: Math.max(1, Math.floor(Number(p?.quantidade ?? p?.qtd ?? 1) || 1)) }));
  } catch (e) { pecas = []; }

  // Insumos usados
  let insumosUsados = [];
  try {
    insumosUsados = db.prepare(`
      SELECT oi.id, oi.quantidade, i.nome, i.unidade
      FROM op_insumos oi
      JOIN insumos i ON i.id = oi.insumo_id
      WHERE oi.ordem_id = ?
      ORDER BY oi.id ASC
    `).all(id);
  } catch (e) { insumosUsados = []; }

  // Serviços usados
  let servicosUsados = [];
  try {
    servicosUsados = db.prepare(`
      SELECT os.id, os.quantidade, os.preco_unit, os.observacao, s.nome, s.unidade
      FROM op_servicos os
      JOIN servicos s ON s.id = os.servico_id
      WHERE os.ordem_id = ?
      ORDER BY os.id ASC
    `).all(id);
  } catch (e) { servicosUsados = []; }

  // Brindes vinculados
  let brindesUsados = [];
  try {
    brindesUsados = db.prepare(`
      SELECT ob.id, ob.quantidade, b.nome, b.unidade
      FROM op_brindes ob
      JOIN brindes b ON b.id = ob.brinde_id
      WHERE ob.op_id = ?
      ORDER BY ob.id ASC
    `).all(id);
  } catch (e) { brindesUsados = []; }

  // Checklist final  // Checklist final
  try { ensureChecklistFinal(id); } catch (e) {}
  const checklistFinal = db.prepare(`
    SELECT item, concluido, data_conclusao
    FROM op_checklist_final
    WHERE ordem_id = ?
    ORDER BY id ASC
  `).all(id);

  const assinaturaChecklist = db.prepare(
    "SELECT responsavel, data_assinatura FROM op_checklist_assinatura WHERE ordem_id = ?"
  ).get(id) || null;

  const itens = db.prepare(`
    SELECT oi.*, p.descricao, p.cor, p.espessura_mm, p.codigo_interno
    FROM ordem_itens oi
    JOIN produtos p ON p.id = oi.produto_id
    WHERE oi.ordem_id = ?
    ORDER BY oi.id ASC
  `).all(id);

  const anexos = db.prepare("SELECT * FROM op_anexos WHERE ordem_id = ? ORDER BY id ASC").all(id);

  // Cabeçalhos
  res.setHeader("Content-Type", "application/pdf");
  res.setHeader("Content-Disposition", `inline; filename="OP-${op.codigo_op}.pdf"`);

  // ====== 1 única página A4 ======
  // Observação: para garantir 1 folha, o PDF truncará listas (itens/insumos/serviços/checklist/imagens)
  // quando não houver espaço.
  const doc = new PDFDocument({ size: "A4", margin: 24 }); // margem menor para caber em 1 folha
  doc.pipe(res);

  const pageWidth = doc.page.width - doc.page.margins.left - doc.page.margins.right;
  const leftX = doc.page.margins.left;
  const rightX = doc.page.width - doc.page.margins.right;
  const bottomY = () => doc.page.height - doc.page.margins.bottom;

  const hasSpace = (h) => (doc.y + h) <= bottomY();
  const noteTrunc = (txt) => {
    doc.fillColor("#6B7280").font("Helvetica").fontSize(7).text(txt, { width: pageWidth });
    doc.moveDown(0.2);
  };

  let logoPath = null;
  const qLogo = String(req.query.logo || "").toLowerCase();
  const tempLogo = String(req.query.tempLogo || "");
  if (qLogo === "none") {
    logoPath = null;
  } else if (tempLogo) {
    const absTemp = path.join(uploadsDir, tempLogo);
    if (fs.existsSync(absTemp)) logoPath = absTemp;
  } else {
    const info = resolveBrandLogoPath();
    if (info) logoPath = info.abs;
  }

  const headerY = doc.y;

  // ===== Cabeçalho compacto =====
  const headerH = 46;

  if (logoPath) {
    try { doc.image(logoPath, leftX, headerY, { fit: [120, headerH] }); } catch (e) {}
  }

  doc.fillColor("#111827").font("Helvetica-Bold").fontSize(14)
    .text("ORDEM DE PRODUÇÃO", leftX, headerY + 4, { width: pageWidth, align: "center" });

  doc.fillColor("#111827").font("Helvetica-Bold").fontSize(11)
    .text(`OP: ${op.codigo_op || op.id}`, leftX, headerY + 6, { width: pageWidth, align: "right" });

  doc.fillColor("#6B7280").font("Helvetica").fontSize(8)
    .text("Acrilsoft • A4 (1 folha)", leftX, headerY + 26, { width: pageWidth, align: "center" });

  doc.moveTo(leftX, headerY + headerH).lineTo(rightX, headerY + headerH).lineWidth(1).stroke("#E5E7EB");
  doc.y = headerY + headerH + 10;

  // Bloco dados (compacto)
  const f = (label, value, x, y, w) => {
    doc.fillColor("#6b7280").font("Helvetica").fontSize(7).text(label, x, y, { width: w });
    doc.fillColor("#111827").font("Helvetica-Bold").fontSize(7).text(value || "-", x, y + 9, { width: w });
  };

  const colGap = 10;
  const colW = (pageWidth - colGap) / 2;
  let y0 = doc.y;

  f("Produto final", op.produto_final, leftX, y0, colW);
  f("Quantidade", String(op.quantidade_final || "-"), leftX + colW + colGap, y0, colW);
  y0 += 22;

  f("Cliente", op.cliente, leftX, y0, colW);
  f("Pedido de venda", op.pedido_venda, leftX + colW + colGap, y0, colW);
  y0 += 22;

  f("Status", (op.status || "").replace("_"," "), leftX, y0, colW);
  f("Prioridade", op.prioridade, leftX + colW + colGap, y0, colW);
  y0 += 22;

  f("Abertura", op.data_abertura, leftX, y0, colW);
  f("Entrega", op.data_entrega, leftX + colW + colGap, y0, colW);
  y0 += 22;

  f("Gerado em", new Date().toISOString().slice(0,19).replace("T"," "), leftX, y0, colW);
  f("Responsável", (assinaturaChecklist && assinaturaChecklist.responsavel) ? assinaturaChecklist.responsavel : "-", leftX + colW + colGap, y0, colW);
  y0 += 26;

  doc.y = y0;

  if (op.observacao && hasSpace(28)) {
    doc.fillColor("#6b7280").font("Helvetica").fontSize(7).text("Observação", leftX, doc.y);
    doc.fillColor("#111827").font("Helvetica").fontSize(7).text(op.observacao, { width: pageWidth });
    doc.moveDown(0.4);
  }

  // Material e peças (compacto)
  if (hasSpace(22) && (op.material_espessura_mm || op.material_cor || (pecas && pecas.length))) {
    doc.fillColor("#111827").font("Helvetica-Bold").fontSize(7).text("Material / Peças");
    const matTxt = [
      op.material_espessura_mm ? `${op.material_espessura_mm}mm` : null,
      op.material_cor ? String(op.material_cor) : null
    ].filter(Boolean).join(" • ");
    if (matTxt) doc.fillColor("#111827").font("Helvetica").fontSize(7).text(`Material: ${matTxt}`, { width: pageWidth });

    if (pecas && pecas.length) {
      const maxLines = Math.max(0, Math.floor((bottomY() - doc.y - 10) / 10));
      const show = pecas.slice(0, Math.min(8, maxLines));
      show.forEach((p) => {
        const nome = (p && p.nome) ? String(p.nome) : "Peça";
        const medidas = (p && p.medidas) ? String(p.medidas) : "-";
        doc.fillColor("#111827").font("Helvetica").fontSize(7).text(`• ${nome}: ${medidas}`, { width: pageWidth });
      });
      if (pecas.length > show.length) noteTrunc(`(+ ${pecas.length - show.length} peças)`);
    }
    doc.moveDown(0.2);
  }

  // ===================== Itens =====================
  if (hasSpace(50)) {
    doc.fillColor("#111827").font("Helvetica-Bold").fontSize(7).text("Itens (Chapas / Materiais)");
    doc.moveDown(0.2);

    const rowH = 14;
    const cols = [
      { key: "descricao", title: "Produto", w: 200 },
      { key: "codigo_interno", title: "Cód.", w: 55 },
      { key: "espessura_mm", title: "Esp.", w: 40 },
      { key: "cor", title: "Cor", w: 65 },
      { key: "quantidade", title: "Qtd", w: 35 },
    ];
    const tableW = cols.reduce((s,c)=>s+c.w,0);
    const startX = leftX;

    const drawHeader = (y) => {
      doc.fillColor("#374151").font("Helvetica-Bold").fontSize(7);
      doc.rect(startX, y, tableW, rowH).fill("#f3f4f6");
      doc.fillColor("#374151");
      let x = startX;
      for (const c of cols) {
        doc.text(c.title, x + 5, y + 4, { width: c.w - 8 });
        x += c.w;
      }
      doc.strokeColor("#e5e7eb").rect(startX, y, tableW, rowH).stroke();
    };

    let y = doc.y;
    drawHeader(y);
    y += rowH;

    const availH = bottomY() - y - 6;
    const maxRows = Math.max(0, Math.floor(availH / rowH));
    const rowsToShow = (itens || []).slice(0, maxRows);

    doc.font("Helvetica").fontSize(7).fillColor("#111827");
    if (!rowsToShow || rowsToShow.length === 0) {
      doc.text("Nenhum item cadastrado.", startX + 6, y + 3);
      y += rowH;
    } else {
      for (const it of rowsToShow) {
        let x = startX;
        const values = {
          descricao: it.descricao,
          codigo_interno: it.codigo_interno || "-",
          espessura_mm: `${it.espessura_mm}mm`,
          cor: it.cor,
          quantidade: String(it.quantidade),
        };
        doc.strokeColor("#e5e7eb").rect(startX, y, tableW, rowH).stroke();
        for (const c of cols) {
          doc.text(values[c.key] || "-", x + 5, y + 3, { width: c.w - 8, ellipsis: true });
          x += c.w;
        }
        y += rowH;
      }
    }

    doc.y = y + 4;
    if ((itens || []).length > rowsToShow.length) {
      noteTrunc(`Itens: exibindo ${rowsToShow.length} de ${(itens || []).length} (restante oculto para caber em 1 página).`);
    }
  }

  // ===== Insumos / Serviços (compacto, sem nova página) =====
  const drawSimpleTableIS = (title, cols, rows) => {
    if (!hasSpace(30)) return;

    doc.fillColor("#111827").font("Helvetica-Bold").fontSize(7).text(title);
    doc.moveDown(0.15);

    if (!rows || rows.length === 0) {
      doc.fillColor("#6b7280").font("Helvetica").fontSize(7).text("Nenhum registro.");
      doc.moveDown(0.2);
      return;
    }

    const rowH2 = 12;
    const startX2 = leftX;
    const pageW2 = pageWidth;
    let y2 = doc.y;

    // header
    doc.fillColor("#374151").font("Helvetica-Bold").fontSize(7);
    doc.rect(startX2, y2, pageW2, rowH2).fill("#f3f4f6");
    let x2 = startX2;

    const widths = cols.map(c => Math.max(35, pageW2 * Number(c.frac || 0)));
    const sum = widths.reduce((a,b)=>a+b,0) || 1;
    const factor = pageW2 / sum;
    const widthsAdj = widths.map(w => w * factor);

    cols.forEach((c, idx) => {
      const w = widthsAdj[idx];
      doc.fillColor("#374151").text(c.title, x2 + 5, y2 + 3, { width: w - 8 });
      x2 += w;
    });
    doc.strokeColor("#e5e7eb").rect(startX2, y2, pageW2, rowH2).stroke();
    y2 += rowH2;

    const availH = bottomY() - y2 - 6;
    const maxRows = Math.max(0, Math.floor(availH / rowH2));
    const showRows = rows.slice(0, maxRows);

    doc.font("Helvetica").fontSize(7).fillColor("#111827");
    showRows.forEach(r => {
      let xx = startX2;
      r.forEach((cell, i) => {
        const w = widthsAdj[i] || (pageW2 / cols.length);
        doc.text(String(cell ?? ""), xx + 5, y2 + 3, { width: w - 8, ellipsis: true });
        xx += w;
      });
      doc.strokeColor("#f3f4f6").moveTo(startX2, y2 + rowH2).lineTo(startX2 + pageW2, y2 + rowH2).stroke();
      y2 += rowH2;
    });

    doc.y = y2 + 3;
    if (rows.length > showRows.length) noteTrunc(`${title}: exibindo ${showRows.length} de ${rows.length} (restante oculto).`);
  };

  drawSimpleTableIS(
    "Insumos utilizados",
    [
      { title: "Insumo", frac: 0.65 },
      { title: "Un.", frac: 0.15 },
      { title: "Qtd", frac: 0.20 }
    ],
    (insumosUsados || []).map(r => [r.nome, r.unidade || "un", r.quantidade])
  );


  drawSimpleTableIS(
    "Brindes",
    [
      { title: "Brinde", frac: 0.65 },
      { title: "Un.", frac: 0.15 },
      { title: "Qtd", frac: 0.20 }
    ],
    (brindesUsados || []).map(r => [r.nome, r.unidade || "UN", r.quantidade])
  );

  drawSimpleTableIS(
    "Serviços",
    [
      { title: "Serviço", frac: 0.45 },
      { title: "Un.", frac: 0.10 },
      { title: "Qtd", frac: 0.15 },
      { title: "Preço", frac: 0.15 },
      { title: "Total", frac: 0.15 }
    ],
    (servicosUsados || []).map(r => {
      const pu = Number(r.preco_unit || 0);
      const q = Number(r.quantidade || 0);
      return [r.nome, r.unidade || "un", q, pu.toFixed(2), (pu*q).toFixed(2)];
    })
  );

  // ===== Checklist (compacto, sem nova página) =====
  if (hasSpace(40)) {
    doc.fillColor("#111827").font("Helvetica-Bold").fontSize(7).text("Checklist final");
    doc.moveDown(0.1);

    const colWc = pageWidth / 2;
    const lineH = 11;

    const availH = bottomY() - doc.y - 30;
    const maxLines = Math.max(1, Math.floor(availH / lineH));
    const maxItems = maxLines * 2; // 2 colunas
    const show = (checklistFinal || []).slice(0, maxItems);

    doc.font("Helvetica").fontSize(7).fillColor("#111827");
    const startY = doc.y;
    show.forEach((ch, idx) => {
      const x = leftX + (idx % 2) * colWc;
      const y = startY + Math.floor(idx / 2) * lineH;
      const mark = ch.concluido ? "[X]" : "[ ]";
      doc.text(`${mark} ${ch.item}`, x, y, { width: colWc - 10, ellipsis: true });
    });

    doc.y = startY + Math.ceil(show.length / 2) * lineH + 2;
    if ((checklistFinal || []).length > show.length) {
      noteTrunc(`Checklist: exibindo ${show.length} de ${(checklistFinal || []).length} (restante oculto).`);
    }
  }

  // ===== Assinatura (sempre) =====
  if (hasSpace(22)) {
    doc.fillColor("#111827").font("Helvetica-Bold").fontSize(7).text("Responsável pela liberação");
    doc.moveDown(0.1);
    const ass = assinaturaChecklist;
    if (ass && ass.responsavel) {
      doc.fillColor("#111827").font("Helvetica").fontSize(7).text(`Responsável: ${ass.responsavel}`);
      if (ass.data_assinatura) {
        doc.fillColor("#6b7280").font("Helvetica").fontSize(7).text(`Assinado em: ${ass.data_assinatura}`);
      }
    } else {
      const y = doc.y + 4;
      doc.fillColor("#111827").font("Helvetica").fontSize(7).text("Responsável: ________________________________", leftX, y);
      doc.fillColor("#111827").font("Helvetica").fontSize(7).text("Data: ____/____/______", leftX + (pageWidth*0.62), y);
      doc.y = y + 12;
    }
    doc.moveDown(0.1);
  }

  // ===== Imagens (opcional, só se couber) =====
  if ((anexos || []).length && hasSpace(140)) {
    doc.fillColor("#111827").font("Helvetica-Bold").fontSize(7).text("Layout / Imagens");
    doc.moveDown(0.2);

    const imgs = (anexos || []).slice(0, 2); // no modo 1 página, no máximo 2
    const gridGap = 8;
    const cellW = (pageWidth - gridGap) / 2;
    const cellH = 110;
    let ix = leftX;
    let iy = doc.y;

    for (let i=0; i<imgs.length; i++) {
      const a = imgs[i];
      const imgPath = path.join(uploadsDir, a.filename);

      doc.strokeColor("#e5e7eb").roundedRect(ix, iy, cellW, cellH, 8).stroke();
      try {
        if (fs.existsSync(imgPath)) {
          doc.image(imgPath, ix + 6, iy + 6, { fit: [cellW - 12, 78], align: "center", valign: "center" });
        }
      } catch (e) {}
      doc.fillColor("#6b7280").font("Helvetica").fontSize(7)
        .text((a.original_name || a.filename).slice(0, 50), ix + 6, iy + 88, { width: cellW - 12 });

      ix = ix + cellW + gridGap;
    }

    doc.y = iy + cellH + 2;
    if ((anexos || []).length > imgs.length) noteTrunc(`Imagens: exibindo ${imgs.length} de ${(anexos || []).length}.`);
  } else if ((anexos || []).length && !hasSpace(60)) {
    // sem espaço pra imagens
    noteTrunc("Imagens anexadas: ocultas (sem espaço em 1 página).");
  }

  doc.end();
});




/* ===== Estoque Brindes ===== */
app.get("/estoque/brindes", (req, res) => {
  const q = (req.query.q || "").trim();

  let brindes = [];
  try {
    brindes = q
      ? db.prepare(`
          SELECT *, (estoque_atual <= estoque_minimo) AS baixo
          FROM brindes
          WHERE nome LIKE ? OR categoria LIKE ?
          ORDER BY baixo DESC, nome ASC
        `).all(`%${q}%`, `%${q}%`)
      : db.prepare(`
          SELECT *, (estoque_atual <= estoque_minimo) AS baixo
          FROM brindes
          ORDER BY baixo DESC, nome ASC
        `).all();
  } catch (e) {
    brindes = [];
  }

  res.render("layout", { title: "Estoque - Brindes", view: "brindes-page", brindes, q });
});

app.get("/brindes/novo", (req, res) => {
  res.render("layout", { title: "Novo Brinde", view: "brinde-novo-page" });
});

app.post("/brindes/novo", (req, res) => {
  const { nome, categoria, unidade, estoque_atual, estoque_minimo, ativo } = req.body;
  try {
    db.prepare(`
      INSERT INTO brindes (nome, categoria, unidade, estoque_atual, estoque_minimo, ativo)
      VALUES (?, ?, ?, ?, ?, ?)
    `).run(
      (nome || "").trim(),
      (categoria || "").trim() || null,
      (unidade || "UN").trim(),
      Number(estoque_atual || 0),
      Number(estoque_minimo || 0),
      ativo === "0" ? 0 : 1
    );
  } catch (e) {
    return res.redirect("/estoque/brindes?erro=" + encodeURIComponent("Erro ao salvar brinde: " + (e.message || e)));
  }
  res.redirect("/estoque/brindes");
});

app.get("/brindes/movimentar/:id(\\d+)", (req, res) => {
  const id = Number(req.params.id);
  const brinde = db.prepare("SELECT * FROM brindes WHERE id = ?").get(id);
  if (!brinde) return res.status(404).send("Brinde não encontrado");
  res.render("layout", { title: "Movimentar Brinde", view: "brinde-movimentar-page", brinde });
});

app.post("/brindes/movimentar/:id(\\d+)", (req, res) => {
  const id = Number(req.params.id);
  const { tipo, quantidade, observacao } = req.body;
  const qtd = Math.floor(Number(quantidade || 0));
  const allowed = ["entrada", "saida", "ajuste"];
  if (!allowed.includes(tipo)) return res.redirect("/estoque/brindes?erro=" + encodeURIComponent("Tipo inválido"));
  if (!Number.isFinite(qtd)) return res.redirect("/estoque/brindes?erro=" + encodeURIComponent("Quantidade inválida"));

  const brinde = db.prepare("SELECT * FROM brindes WHERE id = ?").get(id);
  if (!brinde) return res.redirect("/estoque/brindes?erro=" + encodeURIComponent("Brinde não encontrado"));

  try {
    const tx = db.transaction(() => {
      let novo = Number(brinde.estoque_atual || 0);
      if (tipo === "entrada") novo += Math.abs(qtd);
      else if (tipo === "saida") novo -= Math.abs(qtd);
      else if (tipo === "ajuste") novo = qtd;

      if (novo < 0) throw new Error("Estoque não pode ficar negativo");

      db.prepare("UPDATE brindes SET estoque_atual = ? WHERE id = ?").run(novo, id);
      db.prepare("INSERT INTO brindes_movimentacoes (brinde_id, tipo, quantidade, observacao) VALUES (?,?,?,?)")
        .run(id, tipo, qtd, (observacao || "").trim() || null);
    });
    tx();
  } catch (e) {
    return res.redirect("/brindes/movimentar/" + id + "?erro=" + encodeURIComponent(e.message || e));
  }

  res.redirect("/estoque/brindes");
});

app.get("/brindes/historico", (req, res) => {
  const q = (req.query.q || "").trim();
  let movs = [];
  try {
    movs = q
      ? db.prepare(`
          SELECT m.*, b.nome as brinde_nome, b.unidade
          FROM brindes_movimentacoes m
          JOIN brindes b ON b.id = m.brinde_id
          WHERE b.nome LIKE ? OR (m.observacao LIKE ?)
          ORDER BY m.id DESC
          LIMIT 500
        `).all(`%${q}%`, `%${q}%`)
      : db.prepare(`
          SELECT m.*, b.nome as brinde_nome, b.unidade
          FROM brindes_movimentacoes m
          JOIN brindes b ON b.id = m.brinde_id
          ORDER BY m.id DESC
          LIMIT 500
        `).all();
  } catch (e) {
    movs = [];
  }
  res.render("layout", { title: "Histórico - Brindes", view: "brindes-historico-page", movs, q });
});



/* ===== Brindes (Catálogo + Orçamentos) ===== */

function ensureDefaultBrindeFornecedores() {
  try {
    db.prepare("INSERT OR IGNORE INTO brindes_fornecedores (nome, status) VALUES (?, 1)").run("XBZ");
    db.prepare("INSERT OR IGNORE INTO brindes_fornecedores (nome, status) VALUES (?, 1)").run("Ásia Imports");
  } catch (e) {}
}

function getCustoUnitByFaixa(catalogo_id, quantidade) {
  const qtd = Math.max(1, Math.floor(Number(quantidade || 1)));
  const rows = db.prepare(`
    SELECT * FROM brindes_catalogo_precos
    WHERE catalogo_id = ?
    ORDER BY qtd_min ASC
  `).all(catalogo_id);

  // Se não houver faixa cadastrada, custo 0
  if (!rows || rows.length === 0) return 0;

  // Regra: pega a faixa cujo [min..max] contém a qtd. Se max null, é "∞"
  const found = rows.find(r => qtd >= Number(r.qtd_min || 1) && (r.qtd_max == null || qtd <= Number(r.qtd_max)));
  return Number((found || rows[rows.length - 1]).custo_unitario || 0);
}

app.get("/brindes/catalogo", (req, res) => {
  ensureDefaultBrindeFornecedores();
  const q = (req.query.q || "").trim();
  let itens = [];
  try {
    itens = q
      ? db.prepare(`
          SELECT c.*, f.nome AS fornecedor_nome
          FROM brindes_catalogo c
          JOIN brindes_fornecedores f ON f.id = c.fornecedor_id
          WHERE c.nome LIKE ? OR c.codigo_fornecedor LIKE ?
          ORDER BY c.id DESC
          LIMIT 500
        `).all(`%${q}%`, `%${q}%`)
      : db.prepare(`
          SELECT c.*, f.nome AS fornecedor_nome
          FROM brindes_catalogo c
          JOIN brindes_fornecedores f ON f.id = c.fornecedor_id
          ORDER BY c.id DESC
          LIMIT 500
        `).all();
  } catch (e) {
    itens = [];
  }
  res.render("layout", { title: "Brindes - Catálogo", view: "brindes-catalogo-page", itens, q });
});

app.get("/brindes/catalogo/novo", (req, res) => {
  ensureDefaultBrindeFornecedores();
  const fornecedores = db.prepare("SELECT * FROM brindes_fornecedores WHERE status = 1 ORDER BY nome ASC").all();
  res.render("layout", { title: "Brindes - Novo SKU", view: "brindes-catalogo-novo-page", fornecedores });
});

app.post("/brindes/catalogo/novo", (req, res) => {
  ensureDefaultBrindeFornecedores();
  const { fornecedor_id, codigo_fornecedor, nome, descricao, categoria, unidade, ativo } = req.body;
  try {
    db.prepare(`
      INSERT INTO brindes_catalogo (fornecedor_id, codigo_fornecedor, nome, descricao, categoria, unidade, ativo, updated_at)
      VALUES (?, ?, ?, ?, ?, ?, ?, datetime('now'))
    `).run(
      Number(fornecedor_id),
      (codigo_fornecedor || "").trim(),
      (nome || "").trim(),
      (descricao || "").trim() || null,
      (categoria || "").trim() || null,
      (unidade || "UN").trim(),
      ativo === "0" ? 0 : 1
    );
  } catch (e) {
    return res.redirect("/brindes/catalogo/novo?erro=" + encodeURIComponent(e.message || e));
  }
  res.redirect("/brindes/catalogo");
});

app.get("/brindes/catalogo/:id(\\d+)", (req, res) => {
  const id = Number(req.params.id);
  const item = db.prepare(`
    SELECT c.*, f.nome AS fornecedor_nome
    FROM brindes_catalogo c
    JOIN brindes_fornecedores f ON f.id = c.fornecedor_id
    WHERE c.id = ?
  `).get(id);
  if (!item) return res.status(404).send("SKU não encontrado");
  const precos = db.prepare("SELECT * FROM brindes_catalogo_precos WHERE catalogo_id = ? ORDER BY qtd_min ASC").all(id);
  res.render("layout", { title: "Brindes - SKU", view: "brindes-catalogo-item-page", item, precos });
});

app.post("/brindes/catalogo/:id(\\d+)/precos", (req, res) => {
  const catalogo_id = Number(req.params.id);
  const { qtd_min, qtd_max, custo_unitario } = req.body;
  try {
    db.prepare(`
      INSERT INTO brindes_catalogo_precos (catalogo_id, qtd_min, qtd_max, custo_unitario, updated_at)
      VALUES (?, ?, ?, ?, datetime('now'))
    `).run(
      catalogo_id,
      Math.max(1, Math.floor(Number(qtd_min || 1))),
      (qtd_max === "" || qtd_max == null) ? null : Math.max(1, Math.floor(Number(qtd_max))),
      Number(custo_unitario || 0)
    );
  } catch (e) {
    return res.redirect(`/brindes/catalogo/${catalogo_id}?erro=` + encodeURIComponent(e.message || e));
  }
  res.redirect(`/brindes/catalogo/${catalogo_id}`);
});

app.post("/brindes/catalogo/:id(\\d+)/precos/:pid(\\d+)/remover", (req, res) => {
  const catalogo_id = Number(req.params.id);
  const pid = Number(req.params.pid);
  try {
    db.prepare("DELETE FROM brindes_catalogo_precos WHERE id = ? AND catalogo_id = ?").run(pid, catalogo_id);
  } catch (e) {}
  res.redirect(`/brindes/catalogo/${catalogo_id}`);
});

app.get("/brindes/orcamentos", (req, res) => {
  const orcamentos = db.prepare("SELECT * FROM orcamentos_brindes ORDER BY id DESC LIMIT 500").all();
  res.render("layout", { title: "Brindes - Orçamentos", view: "brindes-orcamentos-page", orcamentos });
});

app.get("/brindes/orcamentos/novo", (req, res) => {
  res.render("layout", { title: "Brindes - Novo Orçamento", view: "brindes-orcamento-novo-page" });
});

app.post("/brindes/orcamentos/novo", (req, res) => {
  const { cliente_nome, validade_em_dias, prazo_entrega_texto, observacoes } = req.body;
  let id = null;
  try {
    const r = db.prepare(`
      INSERT INTO orcamentos_brindes (cliente_nome, validade_em_dias, prazo_entrega_texto, observacoes, status, created_at)
      VALUES (?, ?, ?, ?, 'RASCUNHO', datetime('now'))
    `).run(
      (cliente_nome || "").trim(),
      Math.max(1, Math.floor(Number(validade_em_dias || 7))),
      (prazo_entrega_texto || "").trim() || null,
      (observacoes || "").trim() || null
    );
    id = r.lastInsertRowid;
  } catch (e) {
    return res.redirect("/brindes/orcamentos/novo?erro=" + encodeURIComponent(e.message || e));
  }
  res.redirect(`/brindes/orcamentos/${id}`);
});

function recalcOrcamentoBrindes(orcamento_id) {
  const itens = db.prepare("SELECT * FROM orcamentos_brindes_itens WHERE orcamento_id = ?").all(orcamento_id);
  const subtotal = (itens || []).reduce((acc, it) => acc + Number(it.total_item || 0), 0);
  db.prepare("UPDATE orcamentos_brindes SET subtotal = ?, total = ( ? - desconto ) WHERE id = ?").run(subtotal, subtotal, orcamento_id);
}

function ensureFornecedorEstoque() {
  let f = db.prepare("SELECT * FROM brindes_fornecedores WHERE lower(nome)=lower(?)").get('Estoque');
  if (!f) {
    const r = db.prepare("INSERT INTO brindes_fornecedores (nome, status) VALUES (?,1)").run('Estoque');
    f = db.prepare("SELECT * FROM brindes_fornecedores WHERE id=?").get(Number(r.lastInsertRowid));
  }
  return f;
}

function getOrCreateCatalogoFromBrindeEstoque(brinde_id) {
  const bid = Number(brinde_id);
  const br = db.prepare("SELECT * FROM brindes WHERE id=? AND ativo=1").get(bid);
  if (!br) throw new Error('Brinde do estoque não encontrado');

  let cat = null;
  try { cat = db.prepare("SELECT * FROM brindes_catalogo WHERE brinde_estoque_id=? LIMIT 1").get(bid); } catch (_) {}
  if (cat) return cat;

  const f = ensureFornecedorEstoque();
  const codigo = `ESTOQUE-${bid}`;
  cat = db.prepare("SELECT * FROM brindes_catalogo WHERE fornecedor_id=? AND codigo_fornecedor=?").get(f.id, codigo);
  if (!cat) {
    const r = db.prepare(`
      INSERT INTO brindes_catalogo (fornecedor_id, codigo_fornecedor, nome, categoria, unidade, ativo, updated_at, brinde_estoque_id)
      VALUES (?, ?, ?, ?, ?, 1, datetime('now'), ?)
    `).run(f.id, codigo, br.nome, br.categoria || null, br.unidade || 'UN', bid);
    cat = db.prepare("SELECT * FROM brindes_catalogo WHERE id=?").get(Number(r.lastInsertRowid));
  } else {
    try { db.prepare("UPDATE brindes_catalogo SET brinde_estoque_id=? WHERE id=?").run(bid, cat.id); } catch (_) {}
  }
  return cat;
}

app.get("/brindes/orcamentos/:id(\\d+)", (req, res) => {
  const id = Number(req.params.id);
  const orcamento = db.prepare("SELECT * FROM orcamentos_brindes WHERE id = ?").get(id);
  if (!orcamento) return res.status(404).send("Orçamento não encontrado");

  const catalogo = db.prepare(`
    SELECT c.*, f.nome AS fornecedor_nome,
           b.estoque_atual AS estoque_atual
    FROM brindes_catalogo c
    JOIN brindes_fornecedores f ON f.id = c.fornecedor_id
    LEFT JOIN brindes b ON b.id = c.brinde_estoque_id
    WHERE c.ativo = 1
    ORDER BY f.nome ASC, c.nome ASC
    LIMIT 1000
  `).all();

  const estoqueBrindes = db.prepare(`
    SELECT id, nome, categoria, unidade, estoque_atual, estoque_minimo
    FROM brindes
    WHERE ativo=1
    ORDER BY nome ASC
  `).all();

  const itens = db.prepare(`
    SELECT i.*, c.nome, c.codigo_fornecedor, f.nome AS fornecedor_nome,
           c.brinde_estoque_id,
           b.estoque_atual AS estoque_atual
    FROM orcamentos_brindes_itens i
    JOIN brindes_catalogo c ON c.id = i.catalogo_id
    JOIN brindes_fornecedores f ON f.id = c.fornecedor_id
    LEFT JOIN brindes b ON b.id = c.brinde_estoque_id
    WHERE i.orcamento_id = ?
    ORDER BY i.id DESC
  `).all(id);

  res.render("layout", { title: `Brindes - Orçamento #${id}`, view: "brindes-orcamento-editar-page", orcamento, itens, catalogo, estoqueBrindes });
});

app.post("/brindes/orcamentos/:id(\\d+)/itens", (req, res) => {
  const orcamento_id = Number(req.params.id);
  const { catalogo_id, brinde_id, quantidade, margem_percent } = req.body;

  try {
    let catId = Number(catalogo_id);
    if (brinde_id) {
      const cat = getOrCreateCatalogoFromBrindeEstoque(brinde_id);
      catId = Number(cat.id);
    }
    const qtd = Math.max(1, Math.floor(Number(quantidade || 1)));
    const margem = Number(margem_percent || 0);

    const custoUnit = getCustoUnitByFaixa(catId, qtd);
    const custoTotalBase = custoUnit * qtd;

    const precoTotal = custoTotalBase * (1 + (margem / 100));
    const precoUnit = precoTotal / qtd;

    db.prepare(`
      INSERT INTO orcamentos_brindes_itens (
        orcamento_id, catalogo_id, quantidade,
        custo_unit_base, custo_total_base,
        margem_percent, preco_unit_venda, total_item
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    `).run(
      orcamento_id, catId, qtd,
      custoUnit, custoTotalBase,
      margem, precoUnit, precoTotal
    );

    recalcOrcamentoBrindes(orcamento_id);
  } catch (e) {
    return res.redirect(`/brindes/orcamentos/${orcamento_id}?erro=` + encodeURIComponent(e.message || e));
  }

  res.redirect(`/brindes/orcamentos/${orcamento_id}`);
});

app.post("/brindes/orcamentos/:id(\\d+)/itens/:itemId(\\d+)/remover", (req, res) => {
  const orcamento_id = Number(req.params.id);
  const itemId = Number(req.params.itemId);
  try {
    db.prepare("DELETE FROM orcamentos_brindes_itens WHERE id = ? AND orcamento_id = ?").run(itemId, orcamento_id);
    recalcOrcamentoBrindes(orcamento_id);
  } catch (e) {}
  res.redirect(`/brindes/orcamentos/${orcamento_id}`);
});


/* ===== Insumos ===== */
function ensureInsumosTables(){
  try{
    if (IS_PG()) {
      // Já garantido em db.js (ensureSchemaPg), mas mantemos como segurança
      db.exec(`
        CREATE TABLE IF NOT EXISTS insumos_movimentacoes (
          id BIGSERIAL PRIMARY KEY,
          insumo_id BIGINT NOT NULL REFERENCES insumos(id),
          tipo TEXT NOT NULL,
          quantidade DOUBLE PRECISION NOT NULL,
          motivo TEXT,
          data TEXT DEFAULT (to_char(now(), 'YYYY-MM-DD HH24:MI:SS'))
        );
      `);
      return;
    }
    // better-sqlite3: use exec() for schema changes
    db.exec(`
      CREATE TABLE IF NOT EXISTS insumos (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        nome TEXT NOT NULL,
        categoria TEXT,
        unidade TEXT,
        estoque_atual REAL DEFAULT 0,
        estoque_minimo REAL DEFAULT 0,
        created_at TEXT DEFAULT (datetime('now'))
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

      CREATE TABLE IF NOT EXISTS insumos_movimentacoes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        insumo_id INTEGER NOT NULL,
        tipo TEXT NOT NULL, -- 'entrada' | 'saida'
        quantidade REAL NOT NULL,
        motivo TEXT,
        data TEXT DEFAULT (datetime('now')),
        FOREIGN KEY (insumo_id) REFERENCES insumos(id)
      );
    `);
  } catch (e) {
    console.error('Erro ao garantir tabelas de insumos:', e);
  }
}
ensureInsumosTables();

// ===== INSUMOS (better-sqlite3) =====
app.get("/insumos", (req, res) => {
  try {
    const insumos = db.prepare("SELECT * FROM insumos WHERE IFNULL(ativo,1)=1 ORDER BY nome ASC").all();
    const abaixoMinimo = (insumos || []).filter(i => Number(i.estoque_atual || 0) < Number(i.estoque_minimo || 0)).length;

    res.render("layout", {
      title: "Insumos",
      view: "insumos-page",
      insumos,
      kpis: {
        total: (insumos || []).length,
        abaixo_minimo: abaixoMinimo
      }
    });
  } catch (err) {
    console.error(err);
    return res.status(500).send(err.message);
  }
});

app.get("/insumos/novo", (req, res) => {
  res.render("layout", { title: "Novo insumo", view: "insumo-novo-page" });
});

app.post("/insumos", (req, res) => {
  try {
    const nome = (req.body.nome || "").trim();
    const categoria = (req.body.categoria || "").trim();
    const unidade = (req.body.unidade || "").trim();
    const estoque_atual = Number(req.body.estoque_atual || 0);
    const estoque_minimo = Number(req.body.estoque_minimo || 0);

    if (!nome) return res.status(400).send("Nome é obrigatório.");

    db.prepare(
      "INSERT INTO insumos (nome, categoria, unidade, estoque_atual, estoque_minimo) VALUES (?, ?, ?, ?, ?)"
    ).run(nome, categoria, unidade, estoque_atual, estoque_minimo);

    return res.redirect("/insumos");
  } catch (err) {
    console.error(err);
    return res.status(500).send(err.message);
  }
});

app.get("/insumos/:id/editar", (req, res) => {
  try {
    const id = Number(req.params.id);
    const insumo = db.prepare("SELECT * FROM insumos WHERE id=?").get(id);
    if (!insumo) return res.status(404).send("Insumo não encontrado.");

    res.render("layout", { title: "Editar insumo", view: "insumo-editar-page", insumo });
  } catch (err) {
    console.error(err);
    return res.status(500).send(err.message);
  }
});

app.post("/insumos/:id", (req, res) => {
  try {
    const id = Number(req.params.id);
    const nome = (req.body.nome || "").trim();
    const categoria = (req.body.categoria || "").trim();
    const unidade = (req.body.unidade || "").trim();
    const estoque_atual = Number(req.body.estoque_atual || 0);
    const estoque_minimo = Number(req.body.estoque_minimo || 0);

    if (!nome) return res.status(400).send("Nome é obrigatório.");

    db.prepare(
      "UPDATE insumos SET nome=?, categoria=?, unidade=?, estoque_atual=?, estoque_minimo=? WHERE id=?"
    ).run(nome, categoria, unidade, estoque_atual, estoque_minimo, id);

    return res.redirect("/insumos");
  } catch (err) {
    console.error(err);
    return res.status(500).send(err.message);
  }
});

app.post("/insumos/:id/excluir", (req, res) => {
  const id = Number(req.params.id);
  try {
    // Se o insumo já foi usado em alguma OP, não pode excluir (FK).
    const usadoEmOp = db.prepare("SELECT COUNT(*) as c FROM op_insumos WHERE insumo_id=?").get(id)?.c || 0;
    if (usadoEmOp > 0) {
      return res.redirect("/insumos?erro=" + encodeURIComponent("Não é possível excluir: este insumo já foi usado em " + usadoEmOp + " OP(s). Remova o vínculo antes."));
    }

    const tx = db.transaction(() => {
      // remove histórico de movimentações
      if (__tableExists("insumos_movimentacoes")) {
        db.prepare("DELETE FROM insumos_movimentacoes WHERE insumo_id=?").run(id);
      }
      // remove o insumo
      db.prepare("DELETE FROM insumos WHERE id=?").run(id);
    });

    tx();
    return res.redirect("/insumos?ok=" + encodeURIComponent("Insumo excluído definitivamente."));
  } catch (err) {
    // Segurança extra: caso exista outro vínculo, retorna mensagem amigável
    if (err && (err.code === "SQLITE_CONSTRAINT_FOREIGNKEY" || String(err.message || "").includes("FOREIGN KEY"))) {
      return res.redirect("/insumos?erro=" + encodeURIComponent("Não foi possível excluir: existem registros vinculados a este insumo."));
    }
    console.error(err);
    return res.status(500).send(err.message);
  }
});

app.get("/insumos/:id/movimentar", (req, res) => {
  try {
    const id = Number(req.params.id);
    const insumo = db.prepare("SELECT * FROM insumos WHERE id=?").get(id);
    if (!insumo) return res.status(404).send("Insumo não encontrado.");

    const movs = db
      .prepare("SELECT * FROM insumos_movimentacoes WHERE insumo_id=? ORDER BY id DESC LIMIT 30")
      .all(id);

    res.render("layout", {
      title: "Movimentar insumo",
      view: "insumo-movimentar-page",
      insumo,
      movs
    });
  } catch (err) {
    console.error(err);
    return res.status(500).send(err.message);
  }
});

app.post("/insumos/:id/movimentar", (req, res) => {
  try {
    const id = Number(req.params.id);
    const tipo = (req.body.tipo || "").trim(); // entrada | saida
    const quantidade = Number(req.body.quantidade || 0);
    const motivo = (req.body.motivo || "").trim();

    if (!["entrada", "saida"].includes(tipo)) return res.status(400).send("Tipo inválido.");
    if (!quantidade || quantidade <= 0) return res.status(400).send("Quantidade inválida.");

    const insumo = db.prepare("SELECT * FROM insumos WHERE id=?").get(id);
    if (!insumo) return res.status(404).send("Insumo não encontrado.");

    // registra movimentação
    db.prepare(
      "INSERT INTO insumos_movimentacoes (insumo_id, tipo, quantidade, motivo) VALUES (?, ?, ?, ?)"
    ).run(id, tipo, quantidade, motivo);

    // atualiza estoque
    const delta = tipo === "entrada" ? quantidade : -quantidade;
    db.prepare("UPDATE insumos SET estoque_atual = COALESCE(estoque_atual,0) + ? WHERE id=?").run(delta, id);

    return res.redirect("/insumos/" + id + "/movimentar");
  } catch (err) {
    console.error(err);
    return res.status(500).send(err.message);
  }
});



/* ===== Start ===== */
const PORT = process.env.PORT ? Number(process.env.PORT) : 3000;
// =====================
// OPs - Exclusão definitiva (ADMIN)
// =====================
function __tableExists(name) {
  if (IS_PG()) {
    // Postgres: to_regclass retorna o nome da relation ou NULL
    const r = db.prepare("SELECT to_regclass(?) as name").get(`public.${name}`);
    return !!r?.name;
  }
  return !!db.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name=?").get(name);
}
function __pickFkColumn(table, candidates) {
  if (IS_PG()) {
    const cols = db.prepare("SELECT column_name FROM information_schema.columns WHERE table_schema='public' AND table_name=?").all(table).map(r => String(r.column_name));
    return candidates.find(c => cols.includes(c)) || null;
  }
  const cols = db.prepare(`PRAGMA table_info(${table})`).all().map(c => String(c.name));
  return candidates.find(c => cols.includes(c)) || null;
}

// (rota /ops/:id/excluir-definitivo removida: exclusão agora é definitiva via /ops/:id/excluir)


app.listen(PORT, () => console.log(`Rodando em http://0.0.0.0:${PORT} (ou na URL pública do Render/Railway)`));



// ===== SERVIÇOS =====
function ensureServicosTables(){
  try{
    if (IS_PG()) {
      db.exec(`
        CREATE TABLE IF NOT EXISTS servicos (
          id BIGSERIAL PRIMARY KEY,
          nome TEXT NOT NULL,
          categoria TEXT,
          unidade TEXT,
          preco_unit DOUBLE PRECISION DEFAULT 0,
          ativo INTEGER DEFAULT 1,
          criado_em TEXT DEFAULT (to_char(now(), 'YYYY-MM-DD HH24:MI:SS'))
        );

        CREATE TABLE IF NOT EXISTS op_servicos (
          id BIGSERIAL PRIMARY KEY,
          ordem_id BIGINT NOT NULL REFERENCES ordens_producao(id),
          servico_id BIGINT NOT NULL REFERENCES servicos(id),
          quantidade DOUBLE PRECISION NOT NULL DEFAULT 1,
          preco_unit DOUBLE PRECISION DEFAULT 0,
          observacao TEXT,
          criado_em TEXT DEFAULT (to_char(now(), 'YYYY-MM-DD HH24:MI:SS'))
        );
      `);
      return;
    }
    db.exec(`
      CREATE TABLE IF NOT EXISTS servicos (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        nome TEXT NOT NULL,
        categoria TEXT,
        unidade TEXT,
        preco_unit REAL DEFAULT 0,
        ativo INTEGER DEFAULT 1,
        criado_em TEXT DEFAULT (datetime('now'))
      );

      CREATE TABLE IF NOT EXISTS op_servicos (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ordem_id INTEGER NOT NULL,
        servico_id INTEGER NOT NULL,
        quantidade REAL NOT NULL DEFAULT 1,
        preco_unit REAL DEFAULT 0,
        observacao TEXT,
        criado_em TEXT DEFAULT (datetime('now')),
        FOREIGN KEY (ordem_id) REFERENCES ordens_producao(id),
        FOREIGN KEY (servico_id) REFERENCES servicos(id)
      );
    `);
  } catch (e) {
    console.error('Erro ao garantir tabelas de serviços:', e);
  }
}
ensureServicosTables();

app.get("/servicos", (req, res) => {
  try {
    const servicos = db.prepare(`SELECT * FROM servicos ORDER BY ativo DESC, nome ASC`).all();
    res.render("layout", { title: "Serviços", view: "servicos-page", servicos });
  } catch (err) {
    console.error(err);
    res.status(500).send(err.message);
  }
});

app.get("/servicos/novo", (req, res) => {
  res.render("layout", { title: "Novo serviço", view: "servico-novo-page" });
});

app.post("/servicos", (req, res) => {
  try {
    const nome = (req.body.nome || "").trim();
    const categoria = (req.body.categoria || "").trim();
    const unidade = (req.body.unidade || "").trim();
    const preco_unit = Number(req.body.preco_unit || 0);
    const ativo = req.body.ativo ? 1 : 0;

    if (!nome) return res.status(400).send("Nome é obrigatório.");

    db.prepare(`INSERT INTO servicos (nome, categoria, unidade, preco_unit, ativo) VALUES (?, ?, ?, ?, ?)`)
      .run(nome, categoria, unidade, preco_unit, ativo);

    res.redirect("/servicos");
  } catch (err) {
    console.error(err);
    res.status(500).send(err.message);
  }
});

app.get("/servicos/:id/editar", (req, res) => {
  try {
    const id = Number(req.params.id);
    const servico = db.prepare("SELECT * FROM servicos WHERE id=?").get(id);
    if (!servico) return res.status(404).send("Serviço não encontrado.");
    res.render("layout", { title: "Editar serviço", view: "servico-editar-page", servico });
  } catch (err) {
    console.error(err);
    res.status(500).send(err.message);
  }
});

app.post("/servicos/:id", (req, res) => {
  try {
    const id = Number(req.params.id);
    const nome = (req.body.nome || "").trim();
    const categoria = (req.body.categoria || "").trim();
    const unidade = (req.body.unidade || "").trim();
    const preco_unit = Number(req.body.preco_unit || 0);
    const ativo = req.body.ativo ? 1 : 0;

    if (!nome) return res.status(400).send("Nome é obrigatório.");

    db.prepare(`UPDATE servicos SET nome=?, categoria=?, unidade=?, preco_unit=?, ativo=? WHERE id=?`)
      .run(nome, categoria, unidade, preco_unit, ativo, id);

    res.redirect("/servicos");
  } catch (err) {
    console.error(err);
    res.status(500).send(err.message);
  }
});

app.post("/servicos/:id/excluir", (req, res) => {
  try {
    const id = Number(req.params.id);
    db.prepare("DELETE FROM servicos WHERE id=?").run(id);
    res.redirect("/servicos");
  } catch (err) {
    console.error(err);
    res.status(500).send(err.message);
  }
});




// (Lixeira de OPs removida: exclusão agora é definitiva)