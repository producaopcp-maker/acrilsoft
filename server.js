

const express = require("express");
const https = require("https");
const crypto = require("crypto");

// Gera um segredo aleatório (string) com tamanho aproximado em caracteres.
// Usado para secrets de webhook e tokens internos.
function genSecret(len = 32) {
  const n = Math.max(8, Number(len) || 32);
  return crypto.randomBytes(Math.ceil(n / 2)).toString("hex").slice(0, n);
}
const { db, getAuthDb, getAppDataDir, pingDb, getDbMode, ready } = require("./db");

// Utilitários centralizados
const { arrify, num, calcAreaM2, recalcPedidoTotals, PEDIDO_STATUS } = require("./app/utils/commerce");
const { asyncHandler, sendError } = require("./app/utils/http");

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

/**
 * Garante que existe uma config fiscal para o modelo (55 NF-e / 65 NFC-e)
 * e retorna a linha atual.
 * Não executa DDL no Postgres (schema é tratado em db.js).
 */
function ensureFiscalConfig(modelo) {
  const m = Number(modelo);
  if (!m) return null;
  let row = db.prepare("SELECT * FROM fiscal_config WHERE modelo = ? ORDER BY id DESC LIMIT 1").get(m);
  if (!row) {
    try {
      db.prepare("INSERT INTO fiscal_config (modelo, ambiente, serie, proximo_numero) VALUES (?, 2, 1, 1)").run(m);
    } catch (e) {}
    row = db.prepare("SELECT * FROM fiscal_config WHERE modelo = ? ORDER BY id DESC LIMIT 1").get(m);
  }
  return row || null;
}

// ---------------------------------------------------------------------------
// ⚠️ IMPORTANTE: NUNCA acesse `db.prepare/db.exec` no topo do arquivo.
// O db.js inicializa o Postgres de forma assíncrona; se tocarmos no db antes
// do `ready` resolver, o Electron pode ficar travado no boot.
// ---------------------------------------------------------------------------

async function bootDbAdjustments() {
  // Aguarda o db.js terminar de escolher/validar o banco (Postgres ou fallback SQLite)
  await ready;

  // Links públicos de PDF (SaaS): tokens para compartilhar PDFs via link.
  // (No Postgres evitamos DDL aqui; schema é tratado em db.js.)
  try {
    if (!IS_PG()) {
      db.prepare(`
        CREATE TABLE IF NOT EXISTS pdf_public_links (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          token TEXT NOT NULL UNIQUE,
          kind TEXT NOT NULL,              -- ex: ORCAMENTO_BRINDES
          ref_id INTEGER NOT NULL,         -- id do orçamento/pedido/op...
          file_rel TEXT NOT NULL,          -- caminho relativo dentro de /uploads
          created_at TEXT DEFAULT (datetime('now')),
          expires_at TEXT                  -- ISO datetime
        );
      `).run();

      try { db.prepare("CREATE INDEX IF NOT EXISTS idx_pdf_public_links_kind_ref ON pdf_public_links(kind, ref_id)").run(); } catch (_) {}
      try { db.prepare("CREATE INDEX IF NOT EXISTS idx_pdf_public_links_expires ON pdf_public_links(expires_at)").run(); } catch (_) {}
    }
  } catch (e) {
    console.warn("Falha ao garantir tabela pdf_public_links:", e?.message || e);
  }

  // Brindes (catálogo/orçamentos): seeds básicos
  try { ensureDefaultBrindeFornecedores(); } catch (e) {}

  // Brindes: colunas extras para proposta comercial (SQLite legados)
  ensureColumn("orcamentos_brindes", "pagamento_texto", "ALTER TABLE orcamentos_brindes ADD COLUMN pagamento_texto TEXT");
  ensureColumn("orcamentos_brindes", "frete_texto", "ALTER TABLE orcamentos_brindes ADD COLUMN frete_texto TEXT");
  ensureColumn("orcamentos_brindes", "frete_valor", "ALTER TABLE orcamentos_brindes ADD COLUMN frete_valor REAL DEFAULT 0");
  ensureColumn("orcamentos_brindes", "impostos_texto", "ALTER TABLE orcamentos_brindes ADD COLUMN impostos_texto TEXT");
  ensureColumn("orcamentos_brindes", "condicoes_comerciais", "ALTER TABLE orcamentos_brindes ADD COLUMN condicoes_comerciais TEXT");
  // vínculo com Pedido (centralizado)
  ensureColumn("orcamentos_brindes", "pedido_id", "ALTER TABLE orcamentos_brindes ADD COLUMN pedido_id INTEGER");

  // Fiscal: garantir colunas/tabelas em bases antigas (SQLite)
  ensureColumn("fiscal_documentos", "pedido_id", "ALTER TABLE fiscal_documentos ADD COLUMN pedido_id INTEGER");
  ensureColumn("fiscal_documentos", "modelo", "ALTER TABLE fiscal_documentos ADD COLUMN modelo INTEGER");
  ensureColumn("fiscal_documentos", "serie", "ALTER TABLE fiscal_documentos ADD COLUMN serie INTEGER");
  ensureColumn("fiscal_documentos", "numero", "ALTER TABLE fiscal_documentos ADD COLUMN numero INTEGER");
  ensureColumn("fiscal_documentos", "chave", "ALTER TABLE fiscal_documentos ADD COLUMN chave TEXT");
  ensureColumn("fiscal_documentos", "status", "ALTER TABLE fiscal_documentos ADD COLUMN status TEXT");
  ensureColumn("fiscal_documentos", "xml", "ALTER TABLE fiscal_documentos ADD COLUMN xml TEXT");
  ensureColumn("fiscal_documentos", "ambiente", "ALTER TABLE fiscal_documentos ADD COLUMN ambiente INTEGER");
  ensureColumn("fiscal_documentos", "criado_em", "ALTER TABLE fiscal_documentos ADD COLUMN criado_em TEXT DEFAULT (datetime('now'))");
  ensureColumn("fiscal_documentos", "atualizado_em", "ALTER TABLE fiscal_documentos ADD COLUMN atualizado_em TEXT");

  ensureColumn("fiscal_config", "modelo", "ALTER TABLE fiscal_config ADD COLUMN modelo INTEGER");
  ensureColumn("fiscal_config", "ambiente", "ALTER TABLE fiscal_config ADD COLUMN ambiente INTEGER NOT NULL DEFAULT 2");
  ensureColumn("fiscal_config", "serie", "ALTER TABLE fiscal_config ADD COLUMN serie INTEGER NOT NULL DEFAULT 1");
  ensureColumn("fiscal_config", "proximo_numero", "ALTER TABLE fiscal_config ADD COLUMN proximo_numero INTEGER NOT NULL DEFAULT 1");
  ensureColumn("fiscal_config", "csc_id", "ALTER TABLE fiscal_config ADD COLUMN csc_id TEXT");
  ensureColumn("fiscal_config", "csc", "ALTER TABLE fiscal_config ADD COLUMN csc TEXT");
  ensureColumn("fiscal_config", "uf", "ALTER TABLE fiscal_config ADD COLUMN uf TEXT");
  // Financeiro: suporte a estorno (SQLite)
  ensureColumn("financeiro_movimentos", "estornado", "ALTER TABLE financeiro_movimentos ADD COLUMN estornado INTEGER DEFAULT 0");
  ensureColumn("financeiro_movimentos", "estorno_de_id", "ALTER TABLE financeiro_movimentos ADD COLUMN estorno_de_id INTEGER");


  // Catálogo de Produtos (Venda): colunas e integrações (SQLite)
  ensureColumn("movimentacoes", "origem", "ALTER TABLE movimentacoes ADD COLUMN origem TEXT");
  ensureColumn("movimentacoes", "origem_id", "ALTER TABLE movimentacoes ADD COLUMN origem_id INTEGER");
  ensureColumn("movimentacoes", "item_id", "ALTER TABLE movimentacoes ADD COLUMN item_id INTEGER");

  ensureColumn("pedido_itens", "produto_id", "ALTER TABLE pedido_itens ADD COLUMN produto_id INTEGER");

  // Seed inicial do catálogo (se vazio)
  try {
    const hasAny = db.prepare("SELECT id FROM catalogo_produtos LIMIT 1").get();
    if (!hasAny) {
      db.prepare(`INSERT INTO catalogo_produtos (nome, sku, tipo, unidade, preco_venda, custo_unit, ativo)
                  VALUES (?, ?, 'PRODUTO', 'UN', 0, 0, 1)`).run("Produto Exemplo", "EXEMPLO");
    }
  } catch(e) {}



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
  ensureColumn("ordens_producao", "materiais_baixados", "ALTER TABLE ordens_producao ADD COLUMN materiais_baixados INTEGER NOT NULL DEFAULT 0");
  ensureColumn("ordens_producao", "observacao_interna", "ALTER TABLE ordens_producao ADD COLUMN observacao_interna TEXT");
  ensureColumn("ordens_producao", "observacao_cliente", "ALTER TABLE ordens_producao ADD COLUMN observacao_cliente TEXT");
  ensureColumn("insumos", "ativo", "ALTER TABLE insumos ADD COLUMN ativo INTEGER DEFAULT 1");
  // Insumos: a tela nova usa categoria; em bases antigas (SQLite) essa coluna não existia.
  ensureColumn("insumos", "categoria", "ALTER TABLE insumos ADD COLUMN categoria TEXT");
  // OP x Insumos: suporte a consumo por peça (ex.: cm/peça) e total calculado
  ensureColumn("op_insumos", "qtd_por_peca", "ALTER TABLE op_insumos ADD COLUMN qtd_por_peca REAL");
  ensureColumn("op_insumos", "qtd_total", "ALTER TABLE op_insumos ADD COLUMN qtd_total REAL");
  ensureColumn("users", "modulos", "ALTER TABLE users ADD COLUMN modulos TEXT");
  // Clientes (cadastro completo)
  ensureColumn("clientes", "tipo", "ALTER TABLE clientes ADD COLUMN tipo TEXT DEFAULT 'PF'");
  ensureColumn("clientes", "razao_social", "ALTER TABLE clientes ADD COLUMN razao_social TEXT");
  ensureColumn("clientes", "fantasia", "ALTER TABLE clientes ADD COLUMN fantasia TEXT");
  ensureColumn("clientes", "cpf_cnpj", "ALTER TABLE clientes ADD COLUMN cpf_cnpj TEXT");
  ensureColumn("clientes", "ie", "ALTER TABLE clientes ADD COLUMN ie TEXT");
  ensureColumn("clientes", "ie_isento", "ALTER TABLE clientes ADD COLUMN ie_isento INTEGER DEFAULT 0");
  ensureColumn("clientes", "im", "ALTER TABLE clientes ADD COLUMN im TEXT");
  ensureColumn("clientes", "email", "ALTER TABLE clientes ADD COLUMN email TEXT");
  ensureColumn("clientes", "telefone", "ALTER TABLE clientes ADD COLUMN telefone TEXT");
  ensureColumn("clientes", "whatsapp", "ALTER TABLE clientes ADD COLUMN whatsapp TEXT");
  ensureColumn("clientes", "observacoes", "ALTER TABLE clientes ADD COLUMN observacoes TEXT");
  ensureColumn("clientes", "ativo", "ALTER TABLE clientes ADD COLUMN ativo INTEGER DEFAULT 1");

  // Pedidos: cliente opcional (balcão/avulso)
  ensureColumn("pedidos", "cliente_id", "ALTER TABLE pedidos ADD COLUMN cliente_id INTEGER");
  ensureColumn("pedidos", "cliente_nome_avulso", "ALTER TABLE pedidos ADD COLUMN cliente_nome_avulso TEXT");
  ensureColumn("pedidos", "cliente_telefone_avulso", "ALTER TABLE pedidos ADD COLUMN cliente_telefone_avulso TEXT");


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
      );
    `).run();

    db.prepare(`
      CREATE TABLE IF NOT EXISTS op_checklist_assinatura (
        ordem_id INTEGER PRIMARY KEY,
        responsavel TEXT,
        data_assinatura TEXT
      );
    `).run();
  }
  /* ===== Clientes / Endereços ===== */
  if (IS_PG()) {
    await db.exec(`
      CREATE TABLE IF NOT EXISTS cliente_enderecos (
        id BIGSERIAL PRIMARY KEY,
        cliente_id BIGINT NOT NULL,
        tipo TEXT NOT NULL DEFAULT 'ENTREGA',
        cep TEXT, logradouro TEXT, numero TEXT, complemento TEXT, bairro TEXT, cidade TEXT, uf TEXT, pais TEXT DEFAULT 'BR',
        codigo_ibge_municipio TEXT,
        principal INTEGER DEFAULT 0,
        criado_em TEXT DEFAULT (to_char(now(), 'YYYY-MM-DD HH24:MI:SS')),
        atualizado_em TEXT DEFAULT (to_char(now(), 'YYYY-MM-DD HH24:MI:SS'))
      );
    `);
  } else {
    db.prepare(`
      CREATE TABLE IF NOT EXISTS cliente_enderecos (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        cliente_id INTEGER NOT NULL,
        tipo TEXT NOT NULL DEFAULT 'ENTREGA',
        cep TEXT, logradouro TEXT, numero TEXT, complemento TEXT, bairro TEXT, cidade TEXT, uf TEXT, pais TEXT DEFAULT 'BR',
        codigo_ibge_municipio TEXT,
        principal INTEGER DEFAULT 0,
        criado_em TEXT DEFAULT (datetime('now')),
        atualizado_em TEXT DEFAULT (datetime('now')),
        FOREIGN KEY (cliente_id) REFERENCES clientes(id) ON DELETE CASCADE
      );
    `).run();
  }

  // Catálogo: colunas novas (preço por m² + produto pronto)
  ensureColumn("catalogo_produtos", "estoque_atual", "ALTER TABLE catalogo_produtos ADD COLUMN estoque_atual REAL DEFAULT 0");
  ensureColumn("catalogo_produtos", "estoque_minimo", "ALTER TABLE catalogo_produtos ADD COLUMN estoque_minimo REAL DEFAULT 0");
  ensureColumn("catalogo_produtos", "preco_por_m2", "ALTER TABLE catalogo_produtos ADD COLUMN preco_por_m2 INTEGER DEFAULT 0");
  ensureColumn("catalogo_produtos", "preco_m2", "ALTER TABLE catalogo_produtos ADD COLUMN preco_m2 REAL DEFAULT 0");
  ensureColumn("catalogo_produtos", "custo_m2", "ALTER TABLE catalogo_produtos ADD COLUMN custo_m2 REAL DEFAULT 0");
  ensureColumn("catalogo_produtos", "imagem_url", "ALTER TABLE catalogo_produtos ADD COLUMN imagem_url TEXT");
  ensureColumn("catalogo_produtos", "profundidade_mm", "ALTER TABLE catalogo_produtos ADD COLUMN profundidade_mm REAL");


  // Logística / Expedição (SQLite)
  if (!IS_PG()) {
    try {
      db.exec(`
        CREATE TABLE IF NOT EXISTS logistica_envios (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          pedido_id INTEGER NOT NULL UNIQUE,
          canal TEXT,
          external_order_id TEXT,
          status_envio TEXT DEFAULT 'PENDENTE',
          transportadora TEXT,
          codigo_rastreio TEXT,
          url_rastreio TEXT,
          data_postagem TEXT,
          data_entrega TEXT,
          obs TEXT,
          criado_em TEXT DEFAULT (datetime('now')),
          atualizado_em TEXT DEFAULT (datetime('now'))
        );
        CREATE TABLE IF NOT EXISTS logistica_eventos (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          envio_id INTEGER NOT NULL,
          tipo TEXT,
          descricao TEXT,
          criado_em TEXT DEFAULT (datetime('now'))
        );
      `);
    } catch (e) {}
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
const { blingGet, normContato, normPedidoVenda } = require("./lib/bling");
// helper: parse multipart/form-data for small config forms (used by fetch + FormData)
const uploadNone = multer();
const session = require("express-session");
const { compareSync, hashSync } = require("bcryptjs");
const { roleHasModule, userHasModule, requireAuth, requireModule, requireRole } = require('./middlewares/auth');
const { securityHeaders, applyTrustProxy, buildSessionCookie } = require('./middlewares/security');
const { errorHandler } = require('./middlewares/error-handler');

const app = express();

// Render/Railway/Proxy: garante que req.protocol use x-forwarded-proto
// (evita redirect_uri_mismatch no OAuth do Bling)
app.set("trust proxy", 1);

applyTrustProxy(app);
app.use(securityHeaders);



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


// Bloqueia alterações em OPs já ENVIADAS/ENTREGUES
function requireOpEditable(req, res, next) {
  const id = Number(req.params.id);
  if (!id) return next();
  try {
    const op = db.prepare("SELECT id, status FROM ordens_producao WHERE id = ?").get(id);
    const st = String(op && op.status ? op.status : "").trim().toUpperCase();
    if (st === "ENVIADO" || st === "ENTREGUE") {
      const accept = String(req.headers.accept || "");
      const wantsJson = accept.includes("application/json") || String(req.headers["x-requested-with"] || "").toLowerCase() === "xmlhttprequest" || req.path.startsWith("/api/");
      if (wantsJson) return res.status(423).json({ error: "OP bloqueada para edição (ENVIADO/ENTREGUE)." });
      return res.redirect(`/ops/${id}?erro=` + encodeURIComponent("OP está ENVIADA/ENTREGUE e não pode mais ser editada."));
    }
  } catch (e) {}
  return next();
}


/* ===== Sessão / Login ===== */
// validate required env in production
if (process.env.NODE_ENV === "production") {
  const secret = process.env.SESSION_SECRET;
  if (!secret || secret === "acrilsoft_mude_essa_chave") {
    console.warn("[WARN] SESSION_SECRET não configurada em produção. Configure SESSION_SECRET para segurança.");
  }
}


app.use(session({
  secret: process.env.SESSION_SECRET || "acrilsoft_mude_essa_chave",
  resave: false,
  saveUninitialized: false,
  cookie: buildSessionCookie()
}));

// Locals globais do layout (evita ReferenceError no EJS)
app.use((req, res, next) => {
  const user = req.session?.user || null;
  res.locals.user = user;
  res.locals.userName = user?.nome || "Operator";
  res.locals.activeMenu = res.locals.activeMenu || "";
  res.locals.title = res.locals.title || "Acrilsoft";
  res.locals.dbMode = getDbMode();

  // Helpers globais de formatação (evita floats "sujos" tipo 161.60000000000002)
  res.locals.formatCurrency = (v) => {
    const n = Number(v ?? 0);
    const fixed = Math.round((n + Number.EPSILON) * 100) / 100;
    return fixed.toLocaleString('pt-BR', { style: 'currency', currency: 'BRL' });
  };
  res.locals.formatNumber2 = (v) => {
    const n = Number(v ?? 0);
    const fixed = Math.round((n + Number.EPSILON) * 100) / 100;
    return fixed.toLocaleString('pt-BR', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
  };
  res.locals.canModule = (moduleKey) => {
    if (!user) return false;
    const mods = user.modulos;
    if (Array.isArray(mods) && mods.length > 0) {
      return mods.includes('*') || mods.includes(moduleKey);
    }
    return roleHasModule(user.role, moduleKey);
  };
  res.locals.flash = req.session.flash || null;
  if (req.session && req.session.flash) delete req.session.flash;
  next();
});

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
// Em produção, só habilita se o token for definido via ENV.
const ADMIN_RESET_TOKEN = process.env.ADMIN_RESET_TOKEN || (process.env.NODE_ENV !== 'production'
  ? Math.random().toString(16).slice(2, 10)
  : null);

if (ADMIN_RESET_TOKEN) {
  if (process.env.NODE_ENV !== 'production') {
    console.log(`\n[ACRILSOFT] Token de reset do admin: ${ADMIN_RESET_TOKEN}\nAcesse /reset-admin e informe o token para redefinir a senha.\n`);
  }

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
}

// A partir daqui: tudo exige login
app.use(requireAuth);

// ===== Helpers API (CEP / CNPJ) =====
// Usa BrasilAPI para preencher endereço por CEP e dados básicos por CNPJ.
// Protegido por requireAuth (já aplicado globalmente acima).
app.get("/api/cep/:cep", async (req, res) => {
  try {
    const cep = String(req.params.cep || "").replace(/\D/g, "");
    if (cep.length !== 8) return sendError(res, "CEP inválido", 400);

    const r = await fetch(`https://brasilapi.com.br/api/cep/v1/${cep}`);
    if (!r.ok) return res.status(404).json({ error: "CEP não encontrado" });

    const d = await r.json();
    return res.json({
      cep: d.cep || cep,
      logradouro: d.street || "",
      bairro: d.neighborhood || "",
      cidade: d.city || "",
      uf: d.state || ""
    });
  } catch (e) {
    return res.status(500).json({ error: "Falha ao consultar CEP" });
  }
});

app.get("/api/cnpj/:cnpj", async (req, res) => {
  try {
    const cnpj = String(req.params.cnpj || "").replace(/\D/g, "");
    if (cnpj.length !== 14) return sendError(res, "CNPJ inválido", 400);

    const r = await fetch(`https://brasilapi.com.br/api/cnpj/v1/${cnpj}`);
    if (!r.ok) return res.status(404).json({ error: "CNPJ não encontrado" });

    const d = await r.json();
    return res.json({
      razao_social: d.razao_social || "",
      nome_fantasia: d.nome_fantasia || "",
      telefone: d.ddd_telefone_1 || "",
      email: d.email || "",
      cep: d.cep || "",
      logradouro: d.logradouro || "",
      numero: d.numero || "",
      complemento: d.complemento || "",
      bairro: d.bairro || "",
      cidade: d.municipio || "",
      uf: d.uf || ""
    });
  } catch (e) {
    return res.status(500).json({ error: "Falha ao consultar CNPJ" });
  }
});

// ===== fim helpers API =====


// Guardas por módulo (por prefixo de rota)
app.use("/dashboard", requireModule("dashboard"));
app.use("/ops", requireModule("ops"));
app.use("/estoque", requireModule("estoque"));
app.use("/produtos", requireModule("estoque"));
app.use("/cadastro-produtos", requireModule("pedidos"));
app.use("/movimentar", requireModule("estoque"));
app.use("/historico", requireModule("estoque"));
app.use("/insumos", requireModule("insumos"));
app.use("/brindes", requireModule("brindes"));
app.use("/servicos", requireModule("servicos"));
app.use("/fornecedores", requireModule("fornecedores"));
app.use("/relatorio", requireModule("relatorios"));
app.use("/exportar", requireModule("relatorios"));
app.use("/backup", requireModule("backup"));
// /config/* agora é protegido por rota específica (Bling e Branding)
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
  limits: { fileSize: 3 * 1024 * 1024 },
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

// ===== Configuração persistida (DB settings) =====
// Armazena chaves de configuração no SQLite/Postgres via tabela `settings`.
// Isso substitui qualquer dependência de config.json e funciona bem em Railway/SaaS.
const __APP_CFG_KEYS = [
  "blingToken",
  "blingClientId",
  "blingClientSecret",
  "blingAccessToken",
  "blingRefreshToken",
  "blingExpiresAt",
  "blingScope",
  "blingLastAuthAt",
];

function readDbConfig() {
  const out = {};
  try {
    for (const k of __APP_CFG_KEYS) {
      const v = getSetting("cfg_" + k);
      if (v !== null && v !== undefined) out[k] = v;
    }
  } catch (e) {}
  return out;
}

function writeAppConfigPatch(patch) {
  const obj = patch || {};
  try {
    for (const [k, v] of Object.entries(obj)) {
      if (!__APP_CFG_KEYS.includes(k)) continue;
      if (v === undefined) continue;
      if (v === null) {
        delSetting("cfg_" + k);
      } else {
        setSetting("cfg_" + k, String(v));
      }
    }
  } catch (e) {}
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



// Gate de permissões para área Bling/Integrações (evita links mortos e protege rotas)
function requireBling(req, res, next) {
  const u = req.session?.user;
  if (!u) return res.redirect('/login');
  if (userHasModule(u, 'integracoes') || userHasModule(u, 'bling')) return next();
  return res.status(403).render('layout', {
    view: 'forbidden-page',
    title: 'Sem permissão',
    pageTitle: 'Sem permissão',
    pageSubtitle: 'Você não tem acesso à integração Bling',
    activeMenu: 'configuracoes'
  });
}

// Gate de permissões para Branding
function requireBranding(req, res, next) {
  const u = req.session?.user;
  if (!u) return res.redirect('/login');
  if (userHasModule(u, 'branding') || userHasModule(u, 'admin') || userHasModule(u, '*')) return next();
  return res.status(403).render('layout', {
    view: 'forbidden-page',
    title: 'Sem permissão',
    pageTitle: 'Sem permissão',
    pageSubtitle: 'Você não tem acesso à identidade visual',
    activeMenu: 'configuracoes'
  });
}


app.get("/config/bling", requireAuth, requireBling, (req, res) => {
  const cfg = readDbConfig() || {};
  res.render("layout", {
    view: "config-bling-page",
    title: "Integração Bling",
    pageTitle: "Integração Bling",
    pageSubtitle: "Configure o token para importar clientes e pedidos de venda",
    activeMenu: "configbling",
    ok: req.query.ok ? String(req.query.ok) : null,
    err: req.query.err ? String(req.query.err) : null,
    blingToken: cfg.blingToken || process.env.BLING_TOKEN || "",
    blingClientId: cfg.blingClientId || process.env.BLING_CLIENT_ID || "",
    blingClientSecret: cfg.blingClientSecret || process.env.BLING_CLIENT_SECRET || "",
    blingHasOauth: !!(cfg.blingRefreshToken && (cfg.blingClientId || process.env.BLING_CLIENT_ID) && (cfg.blingClientSecret || process.env.BLING_CLIENT_SECRET)),
    blingExpiresAt: cfg.blingExpiresAt || 0,
    blingLastAuthAt: cfg.blingLastAuthAt || 0,
    blingCallbackUrl: (function(){ try { return getBlingCallbackUrl(req); } catch(e){ return ""; } })(),
  });
});

app.get("/configuracoes", requireAuth, (req, res) => {
  const tab = (req.query.tab ? String(req.query.tab) : "geral").toLowerCase();
  const allowed = new Set(["geral", "aparencia", "usuarios", "integracoes", "sistema"]);
  const safeTab = allowed.has(tab) ? tab : "geral";

  // Validação por módulo (evita links mortos / acesso via URL)
  // - geral/aparencia: liberado para qualquer usuário logado (preferências pessoais)
  // - usuarios: requer módulo 'usuarios'
  // - integracoes: requer 'integracoes' OU 'bling'
  // - sistema: requer 'backup' OU 'branding'
  const u = req.session?.user || null;
  const can = (m) => userHasModule(u, m);

  const needs = {
    usuarios: () => can('usuarios'),
    integracoes: () => can('integracoes') || can('bling'),
    sistema: () => can('backup') || can('branding'),
  };

  if (needs[safeTab] && !needs[safeTab]()) {
    return res.status(403).render('layout', {
      view: 'forbidden-page',
      title: 'Sem permissão',
      pageTitle: 'Sem permissão',
      pageSubtitle: 'Você não tem acesso a esta área de configurações',
      activeMenu: 'configuracoes'
    });
  }

  return res.render("layout", {
    view: "configuracoes-page",
    title: "Configurações",
    pageTitle: "Configurações",
    pageSubtitle: "Preferências do sistema e administração",
    activeMenu: "configuracoes",
    tab: safeTab,
    dbModeText: getDbMode()
  });
});


app.post("/config/bling/salvar", requireAuth, requireBling, uploadNone.none(), (req, res) => {
  try {
    const blingToken = String(req.body.blingToken || "").trim();
    const blingClientId = String(req.body.blingClientId || "").trim();
    const blingClientSecret = String(req.body.blingClientSecret || "").trim();
    writeAppConfigPatch({ blingToken, blingClientId, blingClientSecret });
    if (req.body.restart === "1") {
      return restartApp();
    }
    return res.redirect("/config/bling?ok=1");
  } catch (e) {
    return res.redirect("/config/bling?err=" + encodeURIComponent(e.message || "Falha ao salvar"));
  }
});

app.post("/config/bling/testar", requireAuth, requireBling, uploadNone.none(), async (req, res) => {
  try {
    // tenta usar token informado na tela; se vazio, usa OAuth salvo (refresh automático)
    const blingTokenFromForm = String(req.body.blingToken || "").trim();
    let token = blingTokenFromForm;
    if (!token) {
      token = await getBlingAccessToken(req);
    }
    // testa pegando 1 contato (limite mínimo)
    await blingGet({ token, path: "/contatos", params: { limite: 1, pagina: 1 } });
    return res.redirect("/config/bling?ok=1");
  } catch (e) {
    return res.redirect("/config/bling?err=" + encodeURIComponent(e.message || "Falha ao testar"));
  }
});

// Inicia o fluxo OAuth (abre a tela do Bling para autorizar e gerar refresh_token)
app.get("/integracoes/bling/oauth/iniciar", requireAuth, requireBling, async (req, res) => {
  try {
    const cfg = getBlingCfg();
    if (!cfg.clientId || !cfg.clientSecret) {
      return res.redirect("/config/bling?err=" + encodeURIComponent("Informe Client ID e Client Secret antes de conectar."));
    }

    // state anti-CSRF simples (em memória, válido por 10 min)
    const state = Math.random().toString(16).slice(2) + Math.random().toString(16).slice(2);
    blingOauthState.value = state;
    blingOauthState.createdAt = Date.now();

    const callbackUrl = getBlingCallbackUrl(req);

    // OBS: scope é opcional e o Bling usa o scope configurado no app, mas deixamos a query limpa.
    const authUrl =
      "https://www.bling.com.br/Api/v3/oauth/authorize" +
      "?response_type=code" +
      "&client_id=" + encodeURIComponent(cfg.clientId) +
      "&state=" + encodeURIComponent(state) +
      "&redirect_uri=" + encodeURIComponent(callbackUrl);

    return res.redirect(authUrl);
  } catch (e) {
    return res.redirect("/config/bling?err=" + encodeURIComponent(e.message || "Falha ao iniciar OAuth"));
  }
});

// Callback OAuth: troca o code por access_token + refresh_token e salva no config.json
app.get("/integracoes/bling/oauth/callback", requireAuth, requireBling, async (req, res) => {
  try {
    const cfg = getBlingCfg();
    const code = String(req.query.code || "").trim();
    const state = String(req.query.state || "").trim();
    const err = String(req.query.error || "").trim();

    if (err) {
      return res.redirect("/config/bling?err=" + encodeURIComponent("OAuth Bling: " + err));
    }
    if (!code) {
      return res.redirect("/config/bling?err=" + encodeURIComponent("OAuth Bling: code não recebido."));
    }

    // valida state (10 min)
    const stateOk =
      state &&
      blingOauthState.value &&
      state === blingOauthState.value &&
      (Date.now() - blingOauthState.createdAt) < (10 * 60 * 1000);

    // zera state após uso
    blingOauthState.value = null;
    blingOauthState.createdAt = 0;

    if (!stateOk) {
      return res.redirect("/config/bling?err=" + encodeURIComponent("OAuth Bling: state inválido ou expirado."));
    }

    if (!cfg.clientId || !cfg.clientSecret) {
      return res.redirect("/config/bling?err=" + encodeURIComponent("OAuth Bling: Client ID/Secret não configurados."));
    }

    const callbackUrl = getBlingCallbackUrl(req);

    const tokenResp = await blingOauthPostToken({
      clientId: cfg.clientId,
      clientSecret: cfg.clientSecret,
      bodyObj: { grant_type: "authorization_code", code, redirect_uri: callbackUrl },
    });

    const accessToken = (tokenResp && tokenResp.access_token) ? String(tokenResp.access_token) : "";
    const refreshToken = (tokenResp && tokenResp.refresh_token) ? String(tokenResp.refresh_token) : "";
    const expiresIn = Number(tokenResp && tokenResp.expires_in) || 3600;

    if (!accessToken || !refreshToken) {
      return res.redirect("/config/bling?err=" + encodeURIComponent("OAuth Bling: resposta sem tokens (access/refresh)."));
    }

    const expiresAt = Date.now() + (Math.max(60, expiresIn) * 1000);

    writeAppConfigPatch({
      blingAccessToken: accessToken,
      blingRefreshToken: refreshToken,
      blingExpiresAt: expiresAt,
      blingScope: (tokenResp && tokenResp.scope) ? String(tokenResp.scope) : "",
      blingLastAuthAt: Date.now(),
    });

    return res.redirect("/config/bling?ok=1");
  } catch (e) {
    return res.redirect("/config/bling?err=" + encodeURIComponent(e.message || "Falha no callback OAuth"));
  }
});

// Desconecta (remove tokens OAuth salvos)
app.post("/integracoes/bling/oauth/desconectar", requireAuth, requireBling, uploadNone.none(), (req, res) => {
  try {
    writeAppConfigPatch({
      blingAccessToken: "",
      blingRefreshToken: "",
      blingExpiresAt: 0,
      blingScope: "",
      blingLastAuthAt: 0,
    });
    return res.redirect("/config/bling?ok=1");
  } catch (e) {
    return res.redirect("/config/bling?err=" + encodeURIComponent(e.message || "Falha ao desconectar"));
  }
});


// ===== Bling OAuth2 (Authorization Code + Refresh Token) =====
// Docs: https://developer.bling.com.br/aplicativos  (Bling usa Authorization Code)
const querystring = require("querystring");

function getBlingCfg() {
  const cfg = readDbConfig() || {};
  return {
    manualToken: (cfg.blingToken || process.env.BLING_TOKEN || "").trim(),
    clientId: (cfg.blingClientId || process.env.BLING_CLIENT_ID || "").trim(),
    clientSecret: (cfg.blingClientSecret || process.env.BLING_CLIENT_SECRET || "").trim(),
    accessToken: (cfg.blingAccessToken || "").trim(),
    refreshToken: (cfg.blingRefreshToken || "").trim(),
    expiresAt: Number(cfg.blingExpiresAt || 0) || 0, // ms epoch
    scope: (cfg.blingScope || "").trim(),
    lastAuthAt: Number(cfg.blingLastAuthAt || 0) || 0,
  };
}

function getBlingCallbackUrl(req) {
  // Se definido, usa uma URL fixa (recomendado em produção)
  const fixed = String(process.env.BLING_REDIRECT_URI || "").trim();
  if (fixed) return fixed;

  // Fallback: usa o host atual do sistema (ex.: http://localhost:31337)
  const proto = (req.headers["x-forwarded-proto"] || req.protocol || "http").split(",")[0].trim();
  const host = req.get("host");
  return `${proto}://${host}/integracoes/bling/oauth/callback`;
}

function blingBasicAuthHeader(clientId, clientSecret) {
  const raw = `${clientId}:${clientSecret}`;
  return "Basic " + Buffer.from(raw, "utf8").toString("base64");
}

function blingOauthPostToken({ clientId, clientSecret, bodyObj }) {
  return new Promise((resolve, reject) => {
    const postData = querystring.stringify(bodyObj || {});
    const opts = {
      method: "POST",
      hostname: "www.bling.com.br",
      path: "/Api/v3/oauth/token",
      headers: {
        "Content-Type": "application/x-www-form-urlencoded",
        "Accept": "application/json",
        "Authorization": blingBasicAuthHeader(clientId, clientSecret),
        "Content-Length": Buffer.byteLength(postData),
      },
    };

    const req = https.request(opts, (res) => {
      let data = "";
      res.on("data", (chunk) => (data += chunk));
      res.on("end", () => {
        let parsed = null;
        try { parsed = data ? JSON.parse(data) : null; } catch (_) {}
        if (res.statusCode >= 200 && res.statusCode < 300) return resolve(parsed);
        const msg =
          (parsed && (parsed.message || parsed.error || parsed.mensagem)) ||
          (parsed && parsed.error && parsed.error.message) ||
          (data && String(data).slice(0, 400)) ||
          `HTTP ${res.statusCode}`;
        const err = new Error(`OAuth Bling: ${msg}`);
        err.status = res.statusCode;
        err.payload = parsed;
        return reject(err);
      });
    });
    req.on("error", reject);
    req.write(postData);
    req.end();
  });
}

// garante token válido (usa refresh_token). Se não houver tokens OAuth, cai no token manual (se existir).
async function getBlingAccessToken(reqForCallbackUrl = null) {
  const cfg = getBlingCfg();

  // 1) Se já tem OAuth access token válido, usa.
  const now = Date.now();
  if (cfg.accessToken && cfg.expiresAt && now < (cfg.expiresAt - 60_000)) {
    return cfg.accessToken;
  }

  // 2) Se tem refresh_token + credenciais, renova.
  if (cfg.refreshToken && cfg.clientId && cfg.clientSecret) {
    const refreshed = await blingOauthPostToken({
      clientId: cfg.clientId,
      clientSecret: cfg.clientSecret,
      bodyObj: { grant_type: "refresh_token", refresh_token: cfg.refreshToken },
    });

    const accessToken = (refreshed && refreshed.access_token) ? String(refreshed.access_token) : "";
    const refreshToken = (refreshed && refreshed.refresh_token) ? String(refreshed.refresh_token) : cfg.refreshToken; // pode vir ou não
    const expiresIn = Number(refreshed && refreshed.expires_in) || 3600;

    if (!accessToken) throw new Error("OAuth Bling: refresh não retornou access_token.");

    const expiresAt = Date.now() + (Math.max(60, expiresIn) * 1000);

    writeAppConfigPatch({
      blingAccessToken: accessToken,
      blingRefreshToken: refreshToken,
      blingExpiresAt: expiresAt,
      blingScope: (refreshed && refreshed.scope) ? String(refreshed.scope) : cfg.scope,
      blingLastAuthAt: Date.now(),
    });

    return accessToken;
  }

  // 3) fallback: token manual
  if (cfg.manualToken) return cfg.manualToken;

  // 4) nada configurado
  throw new Error("Bling não configurado: conecte via OAuth em Config > Bling ou informe um token manual.");
}

// Estado simples (em memória) para validar o callback do OAuth
const blingOauthState = {
  value: null,
  createdAt: 0,
};


app.get("/integracoes/bling/clientes", requireAuth, requireBling, async (req, res) => {
  try {
    await ready;
    const token = await getBlingAccessToken(req);
    const pagina = Number(req.query.pagina || 1);
    const limite = Number(req.query.limite || 100);
    const raw = await blingGet({ token, path: "/contatos", params: { pagina, limite } });
    const data = raw && (raw.data || raw.dados || raw);
    const list = Array.isArray(data) ? data : (Array.isArray(data?.contatos) ? data.contatos : []);
    const norm = list.map(normContato).filter(Boolean).map(c => ({
      codigo: String(c.codigo || ""),
      nome: c.nome || "",
      cnpjcpf: c.cnpjcpf || "",
      contato: c.contato || {},
      endereco: c.endereco || null,
    }));
    return res.json({ ok: true, pagina, limite, total: norm.length, clientes: norm });
  } catch (e) {
    return sendError(res, e.message || String(e), 400);
  }
});

app.get("/integracoes/bling/pedidos", requireAuth, requireBling, async (req, res) => {
  try {
    await ready;
    const token = await getBlingAccessToken(req);
    const pagina = Number(req.query.pagina || 1);
    const limite = Number(req.query.limite || 50);

    // Pass-through de filtros comuns (se existirem na conta)
    const params = {
      pagina,
      limite,
      dataInicial: req.query.dataInicial,
      dataFinal: req.query.dataFinal,
      situacao: req.query.situacao,
      numero: req.query.numero,
    };

    const raw = await blingGet({ token, path: "/pedidos/vendas", params });
    const data = raw && (raw.data || raw.dados || raw);
    const list = Array.isArray(data) ? data : (Array.isArray(data?.pedidos) ? data.pedidos : []);
    const norm = list.map(normPedidoVenda).filter(Boolean).map(p => ({
      id: p.id,
      numero: p.numero,
      data: p.data,
      situacao: p.situacao,
      cliente: p.contato ? { codigo: p.contato.codigo, nome: p.contato.nome, cnpjcpf: p.contato.cnpjcpf } : null,
      itens: p.itens || [],
    }));
    return res.json({ ok: true, pagina, limite, total: norm.length, pedidos: norm });
  } catch (e) {
    return sendError(res, e.message || String(e), 400);
  }
});

// Converte datas comuns do Bling para ISO (YYYY-MM-DD) para preencher <input type="date">
function toIsoDateMaybe(s) {
  if (!s) return "";
  const str = String(s).trim();
  if (!str) return "";
  // já está em ISO
  if (/^\d{4}-\d{2}-\d{2}/.test(str)) return str.slice(0, 10);
  // dd/mm/yyyy ou dd-mm-yyyy
  const m = str.match(/^(\d{1,2})[\/\-](\d{1,2})[\/\-](\d{4})/);
  if (m) {
    const dd = String(m[1]).padStart(2, "0");
    const mm = String(m[2]).padStart(2, "0");
    const yyyy = m[3];
    return `${yyyy}-${mm}-${dd}`;
  }
  // timestamps / Date parseável
  const d = new Date(str);
  if (!isNaN(d.getTime())) return d.toISOString().slice(0, 10);
  return "";
}

// Resolve "Prazo" a partir do Pedido de Venda do Bling.
// Requisito: o campo "Prazo" do nosso sistema deve vir da "Data prevista" do Bling.
// (Não fazemos fallback para dataSaida/dataVenda aqui, para não preencher errado.)
function getPrazoEntregaFromBling(pedido) {
  if (!pedido) return "";
  const raw = pedido.raw || {};

  const cand =
    pedido.dataPrevista ||
    raw.dataPrevista ||
    raw.dataPrevistaEntrega ||
    raw.dataEntregaPrevista ||
    raw.dataPrevisao ||
    raw.transporte?.dataPrevista ||
    raw.transporte?.dataPrevistaEntrega ||
    raw.transporte?.dataEntregaPrevista ||
    "";

  return toIsoDateMaybe(cand);
}

// Resolve nome do vendedor (quando o payload vier só com ID ou estrutura diferente)
// Tenta buscar no Bling (endpoints mais comuns) e usa cache em memória.
const __blingVendedorCache = new Map();
async function resolveBlingVendedorNome({ pedido, token }) {
  if (!pedido) return "";
  if (pedido.vendedor && String(pedido.vendedor).trim()) return String(pedido.vendedor).trim();
  const raw = pedido.raw || {};

  const id =
    raw.idVendedor ||
    raw.vendedor?.id ||
    raw.vendedor?.codigo ||
    raw.vendedorId ||
    raw.vendedor_codigo ||
    null;

  if (!id) return "";
  const key = String(id);
  if (__blingVendedorCache.has(key)) return __blingVendedorCache.get(key);

  const tryParseNome = (obj) => {
    if (!obj || typeof obj !== "object") return "";
    return (
      obj.nome ||
      obj.apelido ||
      obj.login ||
      obj.descricao ||
      obj.nomeCompleto ||
      ""
    );
  };

  // Tenta alguns endpoints possíveis. Tudo em try/catch para não quebrar a importação.
  const paths = [
    `/vendedores/${encodeURIComponent(key)}`,
    `/usuarios/${encodeURIComponent(key)}`,
    `/contatos/${encodeURIComponent(key)}`,
  ];

  for (const pth of paths) {
    try {
      const rawV = await blingGet({ token, path: pth });
      const d = rawV && (rawV.data || rawV.dados || rawV);
      const nome = tryParseNome(d) || tryParseNome(d?.vendedor) || tryParseNome(d?.usuario) || "";
      if (nome && String(nome).trim()) {
        const finalNome = String(nome).trim();
        __blingVendedorCache.set(key, finalNome);
        return finalNome;
      }
    } catch (_) {
      // segue tentando
    }
  }

  return "";
}


// Baixa o primeiro anexo (imagem/pdf) do pedido do Bling (se houver) e salva em /uploads.
// Retorna o nome do arquivo salvo (filename) ou null.
async function downloadFirstBlingAttachment({ pedido, token }) {
  try {
    if (!pedido || !pedido.id) return null;
    // 1) tenta achar anexos já no payload
    let anexos = Array.isArray(pedido.anexos) ? pedido.anexos : [];

    // 2) se não veio, tenta endpoint de anexos (tolerante)
    if (!anexos.length) {
      try {
        const rawAx = await blingGet({ token, path: `/pedidos/vendas/${encodeURIComponent(String(pedido.id))}/anexos` });
        const axData = rawAx && (rawAx.data || rawAx.dados || rawAx);
        anexos = Array.isArray(axData) ? axData : (Array.isArray(axData?.anexos) ? axData.anexos : []);
      } catch (_) {
        // sem anexos/sem permissão: ignora
      }
    }

    if (!anexos.length) return null;

    // escolhe o primeiro com url/link
    const first = anexos.map((a) => ({
      nome: a?.nome || a?.filename || a?.arquivo || a?.descricao || "",
      url: a?.url || a?.link || a?.downloadUrl || a?.href || "",
      contentType: a?.contentType || a?.tipo || a?.mimeType || a?.mimetype || "",
      raw: a,
    })).find((a) => a.url);

    if (!first || !first.url) return null;

    // baixa
    const urlStr = String(first.url);
    const u = new URL(urlStr);
    const isBling = /bling\.com\.br$/i.test(u.hostname) || /api\.bling\.com\.br$/i.test(u.hostname);

    const rand = Math.random().toString(16).slice(2);
    const hintedExt = (() => {
      const n = String(first.nome || "").toLowerCase();
      const ext = path.extname(n).toLowerCase().slice(0, 10);
      if (ext) return ext;
      if (String(first.contentType || "").includes("pdf")) return ".pdf";
      if (String(first.contentType || "").includes("png")) return ".png";
      if (String(first.contentType || "").includes("jpeg") || String(first.contentType || "").includes("jpg")) return ".jpg";
      return "";
    })();

    const filename = `pedido_bling_${String(pedido.id)}_${Date.now()}_${rand}${hintedExt || ""}`;
    const dest = path.join(uploadsDir, filename);

    await new Promise((resolve, reject) => {
      const https = require("https");
      const opts = {
        method: "GET",
        headers: {
          "Accept": "*/*",
        },
      };
      if (isBling && token) {
        opts.headers["Authorization"] = `Bearer ${token}`;
      }

      const req = https.request(u, opts, (res) => {
        if (res.statusCode < 200 || res.statusCode >= 300) {
          let data = "";
          res.on("data", (c) => (data += c));
          res.on("end", () => reject(new Error(`Falha ao baixar anexo Bling: HTTP ${res.statusCode} ${data ? String(data).slice(0,200) : ""}`)));
          return;
        }

        // se não tinha extensão, tenta inferir pelo content-type
        if (!hintedExt) {
          const ct = String(res.headers["content-type"] || "").toLowerCase();
          let ext = "";
          if (ct.includes("pdf")) ext = ".pdf";
          else if (ct.includes("png")) ext = ".png";
          else if (ct.includes("jpeg") || ct.includes("jpg")) ext = ".jpg";
          else if (ct.includes("webp")) ext = ".webp";

          if (ext) {
            // renomeia destino com ext
            try {
              const withExt = dest + ext;
              const file = fs.createWriteStream(withExt);
              res.pipe(file);
              file.on("finish", () => file.close(() => resolve(withExt)));
              file.on("error", reject);
              return;
            } catch (e) {
              // fallback pro dest sem ext
            }
          }
        }

        const file = fs.createWriteStream(dest);
        res.pipe(file);
        file.on("finish", () => file.close(() => resolve(dest)));
        file.on("error", reject);
      });

      req.on("error", reject);
      req.end();
    });

    // retorna só o filename final (pode ter ext adicionada)
    const finalName = path.basename(dest);
    const possible = fs.existsSync(dest) ? dest : null;
    if (possible) return finalName;

    // caso tenha sido salvo com ext inferida (dest+ext)
    const globBase = `pedido_bling_${String(pedido.id)}_${Date.now()}`; // não confiável; melhor varrer pelo prefixo
    const prefix = `pedido_bling_${String(pedido.id)}_`;
    const candidates = fs.readdirSync(uploadsDir).filter((f) => f.startsWith(prefix)).sort().reverse();
    return candidates[0] || null;
  } catch (e) {
    // não impede importação
    return null;
  }
}


app.post("/integracoes/bling/importar-pedido/:id", requireAuth, requireBling, uploadNone.none(), async (req, res) => {
  try {
    await ready;
    if (IS_PG()) return sendError(res, "Importação do Bling está disponível no modo SQLite local (offline-first).", 400);
    const token = await getBlingAccessToken(req);
    const id = String(req.params.id || "").trim();
    if (!id) return sendError(res, "ID do pedido inválido.", 400);

    const raw = await blingGet({ token, path: `/pedidos/vendas/${encodeURIComponent(id)}` });
    const data = raw && (raw.data || raw.dados || raw);
    const pedido = normPedidoVenda(data);
    if (!pedido) return sendError(res, "Pedido não encontrado / payload inválido.", 400);

    // Evita duplicar
    const existing = db.prepare("SELECT id, anexo_arquivo FROM pedidos WHERE bling_id = ?").get(String(pedido.id || ""));
    if (existing && existing.id) {
      return res.json({ ok: true, imported: false, pedido_id: existing.id, message: "Pedido já importado." });
    }

    const clienteNome = pedido.contato?.nome || "";
    const clienteContato = (() => {
      const c = pedido.contato?.contato || {};
      return [c.telefone, c.celular, c.email].filter(Boolean).join(" | ");
    })();

    const enderecoStr = pedido.contato?.endereco ? JSON.stringify(pedido.contato.endereco) : null;

	    const vendedorNome = (await resolveBlingVendedorNome({ pedido, token })) || "";
	    const prazoEntregaIso = getPrazoEntregaFromBling(pedido);

    const anexoBling = await downloadFirstBlingAttachment({ pedido, token });

    const info = db.prepare(`
      INSERT INTO pedidos (
        cliente_nome, cliente_contato, vendedor_nome, prazo_entrega, data_venda, prioridade, status,
        observacoes_vendedor, observacoes_internas, op_id, anexo_arquivo,
        bling_id, bling_numero, bling_pedido_compra, bling_situacao, cliente_codigo, cliente_cnpjcpf, cliente_endereco,
        criado_em, atualizado_em
      ) VALUES (?, ?, ?, ?, ?, 'NORMAL', 'ABERTO', ?, ?, NULL, ?, ?, ?, ?, ?, ?, ?, datetime('now'), datetime('now'))
    `).run(
      clienteNome,
      clienteContato,
	      vendedorNome,
	      prazoEntregaIso,
      toIsoDateMaybe(pedido.data || ""),
      (pedido.observacoes && String(pedido.observacoes).trim()) ? String(pedido.observacoes).trim() : null,
      (pedido.observacoesInternas && String(pedido.observacoesInternas).trim()) ? String(pedido.observacoesInternas).trim() : null,
      anexoBling || null,
      String(pedido.id || ""),
      pedido.numero != null ? String(pedido.numero) : "",
      (pedido.pedidoCompra || ""),
      pedido.situacao || "",
      pedido.contato?.codigo != null ? String(pedido.contato.codigo) : "",
      pedido.contato?.cnpjcpf || "",
      enderecoStr
    );

    const pedidoIdLocal = info.lastInsertRowid || info.lastInsertId || info.insertId;

    // Itens
    const insItem = db.prepare(`INSERT INTO pedido_itens (pedido_id, descricao, quantidade, unidade, observacao) VALUES (?, ?, ?, ?, ?)`);
    for (const it of (pedido.itens || [])) {
      insItem.run(pedidoIdLocal, it.descricao || "", Number(it.quantidade || 0), it.unidade || "", it.observacao || "");
    }

    return res.json({ ok: true, imported: true, pedido_id: pedidoIdLocal });
  } catch (e) {
    return sendError(res, e.message || String(e), 400);
  }
});

// Re-sincroniza 1 pedido do Bling (sem duplicar)
// - por ID do pedido no Bling (idPedidoVenda)
app.post("/integracoes/bling/ressincronizar-pedido/:id", requireAuth, requireBling, uploadNone.none(), async (req, res) => {
  try {
    await ready;
    if (IS_PG()) return sendError(res, "Sync do Bling está disponível no modo SQLite local (offline-first).", 400);
    const token = await getBlingAccessToken(req);
    const id = String(req.params.id || "").trim();
    if (!id) return sendError(res, "ID do pedido inválido.", 400);

    const r = await importOrUpdatePedidoFromBlingId(id, token);
    if (!r.ok) return res.status(400).json({ ok: false, error: r.message || "Falha ao sincronizar" });
    return res.json({ ok: true, updated: !r.created, created: !!r.created, pedido_id: r.id_local });
  } catch (e) {
    return sendError(res, e.message || String(e), 400);
  }
});

// Re-sincroniza usando o ID LOCAL do pedido (pega bling_id do registro)
app.post("/integracoes/bling/ressincronizar-pedido-local/:pedidoId", requireAuth, requireBling, uploadNone.none(), async (req, res) => {
  try {
    await ready;
    if (IS_PG()) return sendError(res, "Sync do Bling está disponível no modo SQLite local (offline-first).", 400);
    const token = await getBlingAccessToken(req);
    const pedidoId = Number(req.params.pedidoId);
    if (!pedidoId) return sendError(res, "ID do pedido inválido.", 400);

    const row = db.prepare("SELECT id, bling_id FROM pedidos WHERE id = ?").get(pedidoId);
    const blingId = row?.bling_id ? String(row.bling_id).trim() : "";
    if (!blingId) return sendError(res, "ID do Bling inválido.", 400);

    const r = await importOrUpdatePedidoFromBlingId(blingId, token);
    if (!r.ok) return res.status(400).json({ ok: false, error: r.message || "Falha ao sincronizar" });
    return res.json({ ok: true, updated: !r.created, created: !!r.created, pedido_id: r.id_local });
  } catch (e) {
    return sendError(res, e.message || String(e), 400);
  }
});

// ===================== BLING SYNC (background) =====================
let BLING_SYNC_STATE = { lastRunAt: null, lastOk: null, lastMessage: "", running: false };


async function importOrUpdatePedidoFromBlingId(id, token) {
  const raw = await blingGet({ token, path: `/pedidos/vendas/${encodeURIComponent(id)}` });
  const data = raw && (raw.data || raw.dados || raw);
  const pedido = normPedidoVenda(data);
  if (!pedido || !pedido.id) return { ok: false, message: "Pedido inválido" };

  // já existe?
  const existing = db.prepare("SELECT id, anexo_arquivo FROM pedidos WHERE bling_id = ?").get(String(pedido.id));
  const clienteNome = pedido.contato?.nome || "";
  const clienteContato = (() => {
    const c = pedido.contato?.contato || {};
    return [c.telefone, c.celular, c.email].filter(Boolean).join(" | ");
  })();
  const enderecoStr = pedido.contato?.endereco ? JSON.stringify(pedido.contato.endereco) : null;
  const vendedorNome = (await resolveBlingVendedorNome({ pedido, token })) || "";
  const prazoEntregaIso = getPrazoEntregaFromBling(pedido);

  if (existing && existing.id) {
    // baixa anexo apenas se ainda não existe localmente
    const anexoBling = (!existing.anexo_arquivo || String(existing.anexo_arquivo).trim() === "")
      ? await downloadFirstBlingAttachment({ pedido, token })
      : null;

    // atualiza situação + básicos (sem sobrescrever campos já preenchidos manualmente)
    db.prepare(`
      UPDATE pedidos
      SET bling_numero = ?, bling_pedido_compra = ?, bling_situacao = ?, cliente_nome = ?, cliente_contato = ?, vendedor_nome = ?,
          cliente_codigo = ?, cliente_cnpjcpf = ?, cliente_endereco = ?,
          canal_venda = CASE WHEN (canal_venda IS NULL OR canal_venda = '') THEN 'BLING' ELSE canal_venda END,
          prazo_entrega = CASE WHEN (prazo_entrega IS NULL OR prazo_entrega = '') THEN ? ELSE prazo_entrega END,
          data_venda = CASE WHEN (data_venda IS NULL OR data_venda = '') THEN ? ELSE data_venda END,
          criado_em = CASE WHEN (criado_em IS NULL OR criado_em = '') THEN ? ELSE criado_em END,
          anexo_arquivo = CASE WHEN (anexo_arquivo IS NULL OR anexo_arquivo = '') THEN ? ELSE anexo_arquivo END,
          observacoes_vendedor = CASE WHEN (observacoes_vendedor IS NULL OR observacoes_vendedor = '') THEN ? ELSE observacoes_vendedor END,
          observacoes_internas = CASE WHEN (observacoes_internas IS NULL OR observacoes_internas = '') THEN ? ELSE observacoes_internas END,
          atualizado_em = datetime('now')
      WHERE id = ?
    `).run(
      pedido.numero != null ? String(pedido.numero) : "",
      (pedido.pedidoCompra || ""),
      pedido.situacao || "",
      clienteNome,
      clienteContato,
      vendedorNome,
      pedido.contato?.codigo != null ? String(pedido.contato.codigo) : "",
      pedido.contato?.cnpjcpf || "",
      enderecoStr,
      prazoEntregaIso,
      toIsoDateMaybe(pedido.data || ""),
      toIsoDateMaybe(pedido.data || ""),
      anexoBling || null,
      (pedido.observacoes && String(pedido.observacoes).trim()) ? String(pedido.observacoes).trim() : "",
      (pedido.observacoesInternas && String(pedido.observacoesInternas).trim()) ? String(pedido.observacoesInternas).trim() : "",
      Number(existing.id)
    );

    // itens: substitui (mantém simples e consistente)
    db.prepare("DELETE FROM pedido_itens WHERE pedido_id = ?").run(Number(existing.id));
    const insItem = db.prepare("INSERT INTO pedido_itens (pedido_id, descricao, quantidade, unidade, observacao) VALUES (?, ?, ?, ?, ?)");
    for (const it of (pedido.itens || [])) {
      insItem.run(Number(existing.id), it.descricao || "", Number(it.quantidade || 0), it.unidade || "", it.observacao || "");
    }

    return { ok: true, created: false, id_local: Number(existing.id) };
  }

  const anexoBling = await downloadFirstBlingAttachment({ pedido, token });

  const info = db.prepare(`
    INSERT INTO pedidos (
      cliente_nome, cliente_contato, vendedor_nome, prazo_entrega, data_venda, prioridade, status,
      observacoes_vendedor, observacoes_internas, op_id, anexo_arquivo,
      bling_id, bling_numero, bling_pedido_compra, bling_situacao, cliente_codigo, cliente_cnpjcpf, cliente_endereco,
      canal_venda, criado_em, atualizado_em
    ) VALUES (?, ?, ?, ?, ?, 'NORMAL', 'ABERTO', ?, ?, NULL, ?, ?, ?, ?, ?, ?, ?, ?, 'BLING', ?, datetime('now'))
  `).run(
    clienteNome,
    clienteContato,
    vendedorNome,
    prazoEntregaIso,
    toIsoDateMaybe(pedido.data || ""),
    (pedido.observacoes && String(pedido.observacoes).trim()) ? String(pedido.observacoes).trim() : null,
    (pedido.observacoesInternas && String(pedido.observacoesInternas).trim()) ? String(pedido.observacoesInternas).trim() : null,
    anexoBling || null,
    String(pedido.id || ""),
    pedido.numero != null ? String(pedido.numero) : "",
    (pedido.pedidoCompra || ""),
    pedido.situacao || "",
    pedido.contato?.codigo != null ? String(pedido.contato.codigo) : "",
    pedido.contato?.cnpjcpf || "",
    enderecoStr,
    // criado_em: usa a data do pedido no Bling (importante para filtros por período)
    toIsoDateMaybe(pedido.data || "")
  );

  const pedidoIdLocal = info.lastInsertRowid || info.lastInsertId || info.insertId;

  const insItem = db.prepare("INSERT INTO pedido_itens (pedido_id, descricao, quantidade, unidade, observacao) VALUES (?, ?, ?, ?, ?)");
  for (const it of (pedido.itens || [])) {
    insItem.run(pedidoIdLocal, it.descricao || "", Number(it.quantidade || 0), it.unidade || "", it.observacao || "");
  }

  return { ok: true, created: true, id_local: pedidoIdLocal };
}

// Sincroniza pedidos do Bling.
// lookbackDays > 0  -> busca por janela de datas (dataInicial/dataFinal)
// lookbackDays <= 0 -> busca *todos* os pedidos (sem filtro de data)
// Sincroniza pedidos do Bling (modo SaaS "inteligente"):
// - incremental por dataAlteracaoInicial/dataAlteracaoFinal
// - janela (chunk) de 30 dias (configurável)
// - armazena o último sync bem sucedido no banco
function getBlingSyncDbState() {
  try {
    const row = db.prepare("SELECT * FROM bling_sync_state WHERE id = 1").get();
    return row || null;
  } catch (e) {
    return null;
  }
}

function saveBlingSyncDbState(patch = {}) {
  const cur = getBlingSyncDbState() || {};
  const next = {
    id: 1,
    last_success_at: patch.last_success_at ?? cur.last_success_at ?? null,
    last_run_at: patch.last_run_at ?? cur.last_run_at ?? null,
    last_error: patch.last_error ?? cur.last_error ?? null,
  };

  db.prepare(`
    INSERT INTO bling_sync_state (id, last_success_at, last_run_at, last_error)
    VALUES (@id, @last_success_at, @last_run_at, @last_error)
    ON CONFLICT(id) DO UPDATE SET
      last_success_at=excluded.last_success_at,
      last_run_at=excluded.last_run_at,
      last_error=excluded.last_error
  `).run(next);

  return next;
}

function isoToDateSafe(v) {
  try {
    if (!v) return null;
    const d = new Date(String(v));
    if (Number.isNaN(d.getTime())) return null;
    return d;
  } catch (e) {
    return null;
  }
}

function fmtIsoDate(d) {
  return d.toISOString().slice(0, 10); // YYYY-MM-DD
}

async function runBlingSync({
  mode = "inteligente",
  windowDays = 30,
  backfillDays = 365,
  bufferMinutes = 10,
  forceFull = false,
  // Sync por período de criação (data do pedido no Bling).
  // Se informado, a sync ignora o cursor incremental e busca somente dentro do período.
  createdFrom = null, // YYYY-MM-DD
  createdTo = null    // YYYY-MM-DD
} = {}) {
  if (IS_PG()) return { ok: false, message: "Sync Bling disponível no modo SQLite local (offline-first)." };
  if (BLING_SYNC_STATE.running) return { ok: false, message: "Sync já está em execução." };
  BLING_SYNC_STATE.running = true;

  try {
    await ready;
    const token = await getBlingAccessToken(null);

    const now = new Date();
    const bufferMs = Math.max(0, Number(bufferMinutes) || 0) * 60 * 1000;

    // Se o usuário escolheu período (pela data do pedido no Bling), fazemos sync por criação.
    // Isso é ideal para telas de pedidos (ex: "deste mês em diante"), independente do status.
    const createdFromDate = isoToDateSafe(createdFrom);
    const createdToDate = isoToDateSafe(createdTo);
    const useCreatedPeriod = Boolean(createdFromDate || createdToDate);

    // Estado persistido (no DB) — usado apenas no modo incremental
    let dbState = getBlingSyncDbState();

    // "forceFull": reprocessa o backfill (últimos backfillDays) no modo incremental
    if (forceFull && !useCreatedPeriod) {
      dbState = null;
      saveBlingSyncDbState({ last_success_at: null, last_error: null });
    }

    let from = null;
    let to = null;

    if (useCreatedPeriod) {
      // Período por criação (data do pedido no Bling)
      // Se não vier "até", usa agora; se não vier "de", usa 30 dias pra trás.
      to = createdToDate || now;
      from = createdFromDate || new Date(to.getTime() - (30 * 24 * 60 * 60 * 1000));
    } else {
      // Incremental por alteração
      from = isoToDateSafe(dbState?.last_success_at);
      if (!from) {
        // primeiro sync: busca "backfillDays" para trás (padrão: 365)
        from = new Date(now.getTime() - (Math.max(1, Number(backfillDays) || 365) * 24 * 60 * 60 * 1000));
      }
      // buffer para não perder alterações por latência / relógio
      from = new Date(from.getTime() - bufferMs);
      to = now;
    }

    const limite = 50; // mantém leve e estável
    let created = 0;
    let updated = 0;

    // Sync em janelas de windowDays
    const stepMs = Math.max(1, Number(windowDays) || 30) * 24 * 60 * 60 * 1000;

    let cursor = new Date(from.getTime());
    let loops = 0;

    while (cursor.getTime() < to.getTime()) {
      loops++;
      // Evita loop infinito em caso de bug
      if (loops > 1000) break;

      const chunkEnd = new Date(Math.min(to.getTime(), cursor.getTime() + stepMs));
      const dataInicial = fmtIsoDate(cursor);
      const dataFinal = fmtIsoDate(chunkEnd);
      const dataAlteracaoInicial = dataInicial;
      const dataAlteracaoFinal = dataFinal;

      let pagina = 1;

      while (true) {
        // Quando o usuário escolhe período, filtramos pela data do pedido (criação/emissão).
        // Caso contrário, usamos data de alteração (incremental SaaS).
        const params = useCreatedPeriod
          ? { pagina, limite, dataInicial, dataFinal }
          : { pagina, limite, dataAlteracaoInicial, dataAlteracaoFinal };

        const raw = await blingGet({
          token,
          path: "/pedidos/vendas",
          params,
        });

        const data = raw && (raw.data || raw.dados || raw);
        const list = Array.isArray(data) ? data : (Array.isArray(data?.pedidos) ? data.pedidos : []);
        if (!list.length) break;

        for (const row of list) {
          const p = normPedidoVenda(row);
          if (!p?.id) continue;
          const r = await importOrUpdatePedidoFromBlingId(String(p.id), token);
          if (r.ok) {
            if (r.created) created++;
            else updated++;
          }
        }

        if (list.length < limite) break;
        pagina++;
      }

      cursor = chunkEnd;
    }

    // Atualiza estado persistido (somente modo incremental)
    if (!useCreatedPeriod) {
      saveBlingSyncDbState({
        last_success_at: now.toISOString(),
        last_run_at: now.toISOString(),
        last_error: null,
      });
    } else {
      saveBlingSyncDbState({
        last_run_at: now.toISOString(),
        last_error: null,
      });
    }

    BLING_SYNC_STATE.lastRunAt = now.toISOString();
    BLING_SYNC_STATE.lastOk = true;
    BLING_SYNC_STATE.lastMessage = useCreatedPeriod
      ? `Sync Bling por período (${fmtIsoDate(from)} → ${fmtIsoDate(to)}): Criados ${created} • Atualizados ${updated}`
      : `Sync inteligente (${windowDays}d): Criados ${created} • Atualizados ${updated}`;
    return { ok: true, created, updated, message: BLING_SYNC_STATE.lastMessage };
  } catch (e) {
    const nowIso = new Date().toISOString();
    saveBlingSyncDbState({
      last_run_at: nowIso,
      last_error: e?.message || String(e),
    });

    BLING_SYNC_STATE.lastRunAt = nowIso;
    BLING_SYNC_STATE.lastOk = false;
    BLING_SYNC_STATE.lastMessage = e?.message || String(e);
    return { ok: false, message: BLING_SYNC_STATE.lastMessage };
  } finally {
    BLING_SYNC_STATE.running = false;
  }
}

app.get("/integracoes/bling/sync/status", requireAuth, requireBling, async (req, res) => {
  await ready;
  return res.json({ ok: true, state: BLING_SYNC_STATE });
});

app.post("/integracoes/bling/sync/run", requireAuth, requireBling, uploadNone.none(), async (req, res) => {
  // Sync inteligente (incremental por dataAlteracao)
  // - windowDays: tamanho da janela de paginação
  // - forceFull=1: reprocessa o backfill
  const windowDays = Number(req.body?.windowDays ?? 30) || 30;
  const forceFull = String(req.body?.forceFull ?? "") === "1";
  // Se o usuário informou um período (data do pedido no Bling), sincroniza por criação.
  const createdFrom = String(req.body?.de ?? req.body?.createdFrom ?? "").trim() || null;
  const createdTo = String(req.body?.ate ?? req.body?.createdTo ?? "").trim() || null;

  const r = await runBlingSync({ windowDays, forceFull, createdFrom, createdTo });
  return res.status(r.ok ? 200 : 400).json({ ok: r.ok, message: r.message, created: r.created, updated: r.updated, error: r.ok ? null : r.message });
});



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

app.get("/config/branding", requireAuth, requireBranding, (req, res) => {
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

app.post("/config/branding/logo", requireAuth, requireBranding, uploadBrandLogo.single("logo"), (req, res) => {
  // salva caminho relativo no settings
  const ext = path.extname(req.file.filename).toLowerCase();
  const rel = path.join("uploads", "branding", "logo" + ext);
  setSetting("brand_logo_path", rel);
  res.redirect("/config/branding");
});

app.post("/config/branding/logo/remover", requireAuth, requireBranding, (req, res) => {
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


// Formatação pt-BR (vírgula decimal) para exibição
function fmtBR(value, decimals = 2, trimZeros = false) {
  const n = Number(value);
  const safe = Number.isFinite(n) ? n : 0;
  let s = safe.toLocaleString("pt-BR", {
    minimumFractionDigits: decimals,
    maximumFractionDigits: decimals,
  });
  if (trimZeros) {
    // remove ,00 ou zeros finais (ex: 2,50 -> 2,5)
    s = s.replace(/,00$/, "").replace(/(,\d)0$/, "$1");
  }
  return s;
}

function fmtChapas(value) {
  // chapas podem ser fracionadas; exibe até 2 casas e remove zeros finais
  return fmtBR(value, 2, true);
}

function mmToM(mm) {
  return fmtBR((Number(mm) || 0) / 1000, 2, false);
}

function parseMedidaMm(str) {
  const raw = (str || "").toString().trim();
  if (!raw) return null;
  // aceita: "120x110", "120 x 110", "120X110", "120×110"
  const m = raw.replace(/×/g, "x").match(/(\d+(?:[\.,]\d+)?)\s*[xX]\s*(\d+(?:[\.,]\d+)?)/);
  if (!m) return null;
  const a = Number(String(m[1]).replace(",", "."));
  const b = Number(String(m[2]).replace(",", "."));
  if (!Number.isFinite(a) || !Number.isFinite(b) || a <= 0 || b <= 0) return null;
  // arredonda para 2 casas e depois para int mm (mantém compat com mm)
  return { largura_mm: Math.round(a), altura_mm: Math.round(b) };
}
function areaM2FromMm(largura_mm, altura_mm) {
  const w = Number(largura_mm || 0);
  const h = Number(altura_mm || 0);
  if (!Number.isFinite(w) || !Number.isFinite(h) || w <= 0 || h <= 0) return 0;
  return (w * h) / 1000000.0;
}

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
  // Dashboard único (operacional + executivo na mesma tela)
  // Período (padrão: últimos 30 dias)
  const dias = Math.max(1, Math.min(365, Number(req.query.dias || 30) || 30));
  const desde = db.prepare("SELECT date('now', ?) as d").get(`-${dias} day`).d;

  const ops_atrasadas = db.prepare(`
    SELECT COUNT(*) as n
    FROM ordens_producao
    WHERE COALESCE(arquivada,0)=0
      AND status NOT IN ('FINALIZADA','CANCELADA','ENVIADO','ENTREGUE')
      AND data_entrega IS NOT NULL
      AND date(data_entrega) < date('now')
  `).get().n;

  const ops_andamento = db.prepare(`
    SELECT COUNT(*) as n
    FROM ordens_producao
    WHERE COALESCE(arquivada,0)=0
      AND status IN ('ABERTA','EM ANDAMENTO','PRODUCAO','PRODUÇÃO','SEPARACAO','SEPARAÇÃO')
  `).get().n;

  const ops_vencendo_7d = db.prepare(`
    SELECT COUNT(*) as n
    FROM ordens_producao
    WHERE COALESCE(arquivada,0)=0
      AND status NOT IN ('FINALIZADA','CANCELADA','ENVIADO','ENTREGUE')
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

  const lista_andamento = db.prepare(`
    SELECT id, codigo_op, cliente, pedido_venda, produto_final, quantidade_final, data_entrega, prioridade, status
    FROM ordens_producao
    WHERE COALESCE(arquivada,0)=0
      AND status IN ('ABERTA','EM ANDAMENTO','PRODUCAO','PRODUÇÃO','SEPARACAO','SEPARAÇÃO')
    ORDER BY 
      CASE WHEN data_entrega IS NULL OR data_entrega = '' THEN 1 ELSE 0 END,
      date(data_entrega) ASC,
      prioridade DESC,
      id DESC
    LIMIT 12
  `).all();

  

// --- Bling x Produção (métricas locais) ---
let bling_stats = { total_importados: 0, no_periodo: 0, por_situacao: [], itens_no_periodo: 0, convertidos_em_op: 0 };
if (!IS_PG()) {
  try {
    bling_stats.total_importados = db.prepare("SELECT COUNT(*) as n FROM pedidos WHERE bling_id IS NOT NULL AND bling_id <> ''").get().n || 0;
    bling_stats.no_periodo = db.prepare("SELECT COUNT(*) as n FROM pedidos WHERE bling_id IS NOT NULL AND bling_id <> '' AND date(criado_em) >= date(?)").get(desde).n || 0;
    bling_stats.convertidos_em_op = db.prepare("SELECT COUNT(*) as n FROM pedidos WHERE bling_id IS NOT NULL AND bling_id <> '' AND op_id IS NOT NULL").get().n || 0;
    bling_stats.por_situacao = db.prepare(`
      SELECT COALESCE(NULLIF(bling_situacao,''),'(sem)') as situacao, COUNT(*) as n
      FROM pedidos
      WHERE bling_id IS NOT NULL AND bling_id <> ''
      GROUP BY COALESCE(NULLIF(bling_situacao,''),'(sem)')
      ORDER BY n DESC
    `).all();
    bling_stats.itens_no_periodo = db.prepare(`
      SELECT COALESCE(SUM(pi.quantidade),0) as n
      FROM pedido_itens pi
      JOIN pedidos p ON p.id = pi.pedido_id
      WHERE p.bling_id IS NOT NULL AND p.bling_id <> ''
        AND date(p.criado_em) >= date(?)
    `).get(desde).n || 0;
  } catch (_) {}
}

  // Carrega dados do dashboard executivo para a mesma página
  const exec = getDashboardExecutivoData(req);

  res.render("layout", {
    title: "Dashboard",
    view: "dashboard-hub-page",
    tab: 'integrado',
    bling_stats,
    exec,
    extraHead: `
      <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/fullcalendar@6.1.11/index.global.min.css">
      <style>
        /* Calendário compacto (evita "vazio" gigante quando não há eventos) */
        #opsCalendar{height:420px; max-height:420px;}
        @media (max-width: 900px){
          #opsCalendar{height:520px; max-height:520px;}
        }
        .fc .fc-view-harness{height:100% !important;}
        .fc .fc-scroller{overflow:auto;}
        .fc .fc-toolbar-title{font-size:1.05rem;}

        /* ===== Cores por status ===== */
        /* classes geradas via JS: op-status-<status_normalizado> */
        .fc .fc-event.op-status-aberta,
        .fc .fc-event.op-status-em_andamento{
          background: rgba(37, 99, 235, .18);
          border-color: rgba(37, 99, 235, .35);
        }
        .fc .fc-event.op-status-producao,
        .fc .fc-event.op-status-producao_ {
          background: rgba(245, 158, 11, .18);
          border-color: rgba(245, 158, 11, .38);
        }
        .fc .fc-event.op-status-separacao{
          background: rgba(20, 184, 166, .18);
          border-color: rgba(20, 184, 166, .38);
        }
        .fc .fc-event.op-status-enviado{
          background: rgba(99, 102, 241, .18);
          border-color: rgba(99, 102, 241, .38);
        }
        .fc .fc-event.op-status-entregue{
          background: rgba(34, 197, 94, .22);
          border-color: rgba(34, 197, 94, .45);
        }

        .fc .fc-event.op-status-finalizada{
          background: rgba(34, 197, 94, .18);
          border-color: rgba(34, 197, 94, .38);
        }
        .fc .fc-event.op-status-cancelada{
          background: rgba(148, 163, 184, .18);
          border-color: rgba(148, 163, 184, .38);
        }

        /* ===== OP atrasada: borda vermelha + "pisca" suave ===== */
        @keyframes opPulse {
          0%, 100% { box-shadow: 0 0 0 rgba(239, 68, 68, 0); }
          50% { box-shadow: 0 0 0.6rem rgba(239, 68, 68, .24); }
        }
        .fc .fc-event.op-overdue{
          border-color: rgba(239, 68, 68, .75) !important;
          background: rgba(239, 68, 68, .14) !important;
          animation: opPulse 1.2s ease-in-out infinite;
        }
        .fc .fc-event.op-overdue .fc-event-main{color:#7f1d1d;}

        /* Badge "ATRASADA" (visão lista) */
        .op-overdue-badge{display:inline-flex;align-items:center;gap:6px;padding:3px 8px;border-radius:999px;background:rgba(239,68,68,.16);border:1px solid rgba(239,68,68,.35);color:#7f1d1d;font-weight:1000;font-size:11px;letter-spacing:.02em;}

        /* deixa o texto legível em qualquer cor */
        .fc .fc-event .fc-event-main{color:#0f172a;}
        .fc .fc-event.op-status-finalizada .fc-event-main{color:#064e3b;}
        .fc .fc-event.op-status-cancelada .fc-event-main{color:#334155;}

        /* ===== Modal rápido (sem sair do calendário) ===== */
        .op-modal-backdrop{position:fixed;inset:0;background:rgba(0,0,0,.55);display:flex;align-items:center;justify-content:center;z-index:9999;padding:18px;opacity:0;pointer-events:none;transition:opacity .16s ease;}
        .op-modal-backdrop.show{opacity:1;pointer-events:auto;}
        .op-modal{transform:translateY(10px) scale(.98);transition:transform .16s ease, opacity .16s ease;opacity:0;}
        .op-modal-backdrop.show .op-modal{transform:none;opacity:1;}
        .op-modal{width:min(720px, 96vw);background:#fff;color:#0f172a;border-radius:16px;box-shadow:0 20px 60px rgba(0,0,0,.35);overflow:hidden;}
        .op-modal-header{display:flex;align-items:flex-start;justify-content:space-between;gap:12px;padding:16px 16px 10px;border-bottom:1px solid rgba(0,0,0,.08);}
        .op-modal-title{font-weight:900;font-size:16px;line-height:1.2;margin:0;}
        .op-modal-sub{opacity:.8;font-size:12px;margin-top:6px;}
        .op-modal-close{border:0;background:transparent;font-size:20px;line-height:1;cursor:pointer;padding:6px 8px;border-radius:10px;}
        .op-modal-close:hover{background:rgba(0,0,0,.06);}
        .op-modal-body{padding:14px 16px;}
        .op-kv{display:grid;grid-template-columns:160px 1fr;gap:8px 12px;font-size:13px;}
        .op-k{opacity:.7;}
        .op-v{font-weight:700;}
        .op-modal-footer{display:flex;justify-content:flex-end;gap:10px;padding:12px 16px 16px;border-top:1px solid rgba(0,0,0,.08);}
        .op-btn{display:inline-flex;align-items:center;justify-content:center;gap:8px;border-radius:12px;padding:10px 14px;font-weight:900;border:1px solid rgba(0,0,0,.12);background:#fff;cursor:pointer;text-decoration:none;color:#0f172a;}
        .op-btn:hover{background:rgba(0,0,0,.04);}
        .op-btn-primary{background:#0f172a;color:#fff;border-color:#0f172a;}
        .op-btn-primary:hover{filter:brightness(1.05);}

        /* ===== Calendário: garantir OP sempre visível (status só no hover na grade) ===== */
        .op-event{display:flex;align-items:center;gap:8px;min-width:0;}
        .op-title{font-weight:800;font-size:12px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;}

        /* Badge de status (mostrado na visão LISTA) */
        .op-status{
          font-size:10px;
          line-height:1;
          padding:3px 8px;
          border-radius:999px;
          font-weight:800;
          border:1px solid rgba(0,0,0,.12);
          background:rgba(0,0,0,.08);
          white-space:nowrap;
          max-width:150px;
          overflow:hidden;
          text-overflow:ellipsis;
          opacity:.9;
        }

        /* Na LISTA, título não deve sumir e o badge deve caber */
        .fc-list-event .op-title{overflow:visible;text-overflow:clip;}
        .fc-list-event .op-event{flex-wrap:wrap;row-gap:6px;}

        /* Na visão "semana" (grade), não cortar o número da OP */
        .fc-timegrid-event .op-title{overflow:visible;text-overflow:clip;}

        /* um respiro na lista semanal */
        .fc .fc-list-event-title{padding:10px 12px !important;}
        .fc .fc-list-event:hover{background:rgba(0,0,0,.03);}
      
        .op-modal-footer{display:flex;justify-content:space-between;gap:10px;align-items:center;padding:12px 16px;border-top:1px solid rgba(0,0,0,.08);flex-wrap:wrap;}
        .op-modal-actions{display:flex;gap:8px;flex-wrap:wrap;}
        .op-modal-actions-right{display:flex;gap:8px;align-items:center;}
        .op-btn{border:1px solid rgba(0,0,0,.12);background:#fff;border-radius:12px;padding:8px 10px;font-weight:800;font-size:12px;cursor:pointer;}
        .op-btn:hover{background:rgba(0,0,0,.04);}
        .op-btn:disabled{opacity:.55;cursor:not-allowed;}
        .op-btn-primary{background:#111827;color:#fff;border-color:#111827;text-decoration:none;display:inline-flex;align-items:center;}
        .op-btn-primary:hover{opacity:.95;}
        .op-btn-ghost{background:rgba(0,0,0,.03);}
        .op-btn-ok{background:#16a34a;border-color:#16a34a;color:#fff;}
        .op-btn-ok:hover{opacity:.95;}
        .op-status-badge{display:inline-flex;align-items:center;gap:6px;padding:3px 8px;border-radius:999px;background:rgba(0,0,0,.06);font-weight:900;font-size:11px;}
        @media (prefers-color-scheme: dark){
          /* Garantir contraste no modal no modo escuro */
          .op-modal{background:#0b1220;color:rgba(255,255,255,.92);}
          .op-modal-header,.op-modal-footer{border-color:rgba(255,255,255,.10);}
          .op-modal-sub{color:rgba(255,255,255,.78);}
          .op-modal-title{color:#fff;}
          .op-k{opacity:1;color:rgba(255,255,255,.72);}
          .op-v{color:#fff;}
          .op-modal-close:hover{background:rgba(255,255,255,.08);}
          .op-btn{background:#0f172a;border-color:rgba(255,255,255,.14);color:#fff;}
          .op-btn:hover{background:rgba(255,255,255,.06);}
          .op-btn-ghost{background:rgba(255,255,255,.06);}
          .op-status-badge{background:rgba(255,255,255,.10);color:#fff;}
        }

      </style>
    `,
    extraScripts: `
      <script src="https://cdn.jsdelivr.net/npm/fullcalendar@6.1.11/index.global.min.js"></script>
      <script src="https://cdn.jsdelivr.net/npm/fullcalendar@6.1.11/locales-all.global.min.js"></script>
      <script>
      document.addEventListener("DOMContentLoaded", function(){
        // Normaliza status para virar classe CSS (remove acentos, espaços, etc.)
        const normStatus = (s) => {
          s = String(s || "").trim();
          try { s = s.normalize('NFD').replace(/[\u0300-\u036f]/g, ''); } catch(e) {}
          s = s.toUpperCase();
          s = s.replace(/[^A-Z0-9]+/g, "_");
          s = s.replace(/^_+|_+$/g, "");
          return s.toLowerCase();
        };

        // ===== Modal rápido =====
        const ensureOpModal = () => {
          let backdrop = document.getElementById('opModalBackdrop');
          if (backdrop) return backdrop;

          backdrop = document.createElement('div');
          backdrop.id = 'opModalBackdrop';
          backdrop.className = 'op-modal-backdrop';
          backdrop.innerHTML = \`
            <div class="op-modal" role="dialog" aria-modal="true" aria-labelledby="opModalTitle">
              <div class="op-modal-header">
                <div>
                  <h3 class="op-modal-title" id="opModalTitle">OP</h3>
                  <div class="op-modal-sub" id="opModalSub"></div>
                </div>
                <button class="op-modal-close" type="button" aria-label="Fechar" id="opModalClose">✕</button>
              </div>
              <div class="op-modal-body">
                <div id="opModalBody">Carregando…</div>
              </div>
              <div class="op-modal-footer">
                <div class="op-modal-actions">
                  <button class="op-btn op-btn-ghost" type="button" id="opQuickProducao" data-status="EM_PRODUCAO">Produção</button>
                  <button class="op-btn op-btn-ghost" type="button" id="opQuickMaterial" data-status="AGUARDANDO_MATERIAL">Aguard. material</button>
                  <button class="op-btn op-btn-ok" type="button" id="opQuickFinalizar" data-status="FINALIZADA">Finalizar</button>
                  <button class="op-btn op-btn-primary" type="button" id="opQuickEnviado" data-status="ENVIADO" style="display:none">Enviado</button>
                  <button class="op-btn op-btn-primary" type="button" id="opQuickEntregue" data-status="ENTREGUE" style="display:none">Entregue</button>
                </div>
                <div class="op-modal-actions-right">
                  <button class="op-btn" type="button" id="opModalFechar">Fechar</button>
                  <a class="op-btn op-btn-primary" id="opModalAbrir" href="#">Abrir OP</a>
                </div>
              </div>
            </div>
          \`;

          document.body.appendChild(backdrop);

          const close = () => {
            backdrop.classList.remove('show');
            setTimeout(() => { backdrop.style.display = 'none'; }, 170);
            document.body.style.overflow = '';
          };
          backdrop.addEventListener('click', (e) => {
            if (e.target === backdrop) close();
          });
          backdrop.querySelector('#opModalClose').addEventListener('click', close);
          backdrop.querySelector('#opModalFechar').addEventListener('click', close);
          document.addEventListener('keydown', (e) => {
            if (backdrop.style.display === 'flex' && e.key === 'Escape') close();
          });

          backdrop._close = close;
          return backdrop;
        };

        const openOpModal = (opId, opUrl) => {
          const backdrop = ensureOpModal();
          const titleEl = backdrop.querySelector('#opModalTitle');
          const subEl = backdrop.querySelector('#opModalSub');
          const bodyEl = backdrop.querySelector('#opModalBody');
          const abrirEl = backdrop.querySelector('#opModalAbrir');
          const btnProducao = backdrop.querySelector('#opQuickProducao');
          const btnMaterial = backdrop.querySelector('#opQuickMaterial');
          const btnFinalizar = backdrop.querySelector('#opQuickFinalizar');
          const btnEnviado = backdrop.querySelector('#opQuickEnviado');
          const btnEntregue = backdrop.querySelector('#opQuickEntregue');

          const setLoading = (isLoading) => {
            [btnProducao, btnMaterial, btnFinalizar, btnEnviado, btnEntregue].forEach(b => { if (b) b.disabled = !!isLoading; });
          };

          const fmtDateTime = (s) => {
            if (!s) return '';
            try {
              const dt = new Date(s);
              if (!isNaN(dt.getTime())) return dt.toLocaleString('pt-BR');
            } catch(e) {}
            return String(s);
          };

          const renderResumo = (data) => {
            const labelMap = { ABERTA: 'ABERTA', EM_PRODUCAO: 'EM PRODUÇÃO', AGUARDANDO_MATERIAL: 'AGUARD. MATERIAL', FINALIZADA: 'FINALIZADA', ENVIADO: 'ENVIADO', ENTREGUE: 'ENTREGUE', CANCELADA: 'CANCELADA' };
            const stLabel = labelMap[data.status] || data.status || '';

            if (btnEnviado) btnEnviado.style.display = (data.status === 'FINALIZADA') ? '' : 'none';
            if (btnEntregue) btnEntregue.style.display = (data.status === 'ENVIADO') ? '' : 'none';
            const locked = (data.status === 'ENVIADO' || data.status === 'ENTREGUE');
            if (btnProducao) btnProducao.style.display = locked ? 'none' : '';
            if (btnMaterial) btnMaterial.style.display = locked ? 'none' : '';
            if (btnFinalizar) btnFinalizar.style.display = locked ? 'none' : '';
            subEl.setAttribute('data-cliente', (data.cliente || ''));
            subEl.innerHTML = (data.cliente || '') + (stLabel ? (' • <span class="op-status-badge">' + stLabel + '</span>') : '');

            const kv = (k, v) => '<div class="op-k">' + k + '</div><div class="op-v">' + ((v ?? '').toString() || '-') + '</div>';

            const enviadoTxt = data.enviado_em
              ? (fmtDateTime(data.enviado_em) + (data.enviado_por ? (' • ' + data.enviado_por) : ''))
              : '';
            const entregueTxt = data.entregue_em
              ? (fmtDateTime(data.entregue_em) + (data.entregue_por ? (' • ' + data.entregue_por) : ''))
              : '';

            bodyEl.innerHTML = (
              '<div class="op-kv">'
              + kv('Cliente', data.cliente)
              + kv('Vendedor', data.vendedor_nome || '')
              + kv('Status', data.status)
              + kv('Entrega', data.data_entrega || '')
              + kv('Prioridade', data.prioridade || '')
              + kv('Pedido', data.pedido_venda || '')
              + kv('Produto final', data.produto_final || '')
              + kv('Qtd', (data.quantidade_final != null ? data.quantidade_final : ''))
              + (enviadoTxt ? kv('Enviado', enviadoTxt) : '')
              + (entregueTxt ? kv('Entregue', entregueTxt) : '')
              + kv('Obs.', data.observacoes_internas || data.observacoes || '')
              + '</div>'
            );

            abrirEl.href = data.op_url || ('/ops/' + opId);
          };

          const postStatus = (novoStatus) => {
            if (!novoStatus) return;
            setLoading(true);
            return fetch('/ops/' + encodeURIComponent(opId) + '/status', {
              method: 'POST',
              headers: { 'Content-Type': 'application/x-www-form-urlencoded;charset=UTF-8' },
              body: 'status=' + encodeURIComponent(novoStatus)
            })
              .then(r => {
                if (!r.ok) return r.text().then(t => { throw new Error(t || 'Falha ao atualizar status'); });
                return r.text();
              })
              .then(() => {
                // Recarrega o resumo (para atualizar histórico de Enviado/Entregue)
                return fetch('/api/ops/' + encodeURIComponent(opId) + '/resumo', { cache: 'no-store' })
                  .then(r => r.ok ? r.json() : Promise.reject())
                  .then(renderResumo)
                  .catch(() => {
                    // fallback: pelo menos atualiza o badge
                    const st = novoStatus;
                    const labelMap = { ABERTA: 'ABERTA', EM_PRODUCAO: 'EM PRODUÇÃO', AGUARDANDO_MATERIAL: 'AGUARD. MATERIAL', FINALIZADA: 'FINALIZADA', ENVIADO: 'ENVIADO', ENTREGUE: 'ENTREGUE', CANCELADA: 'CANCELADA' };
                    subEl.innerHTML = (subEl.getAttribute('data-cliente') || '') + ' • <span class="op-status-badge">' + (labelMap[st] || st) + '</span>';
                  })
                  .finally(() => {
                    if (window.__opsCalendar) window.__opsCalendar.refetchEvents();
                  });
              })
              .catch(err => {
                bodyEl.innerHTML = '<div style="opacity:.85">Não consegui atualizar o status agora.</div><div class="muted" style="margin-top:8px;">' + (err && err.message ? err.message : '') + '</div>';
              })
              .finally(() => setLoading(false));
          };

          [btnProducao, btnMaterial, btnFinalizar, btnEnviado, btnEntregue].forEach(btn => {
            if (!btn) return;
            btn.onclick = () => postStatus(btn.getAttribute('data-status'));
          });

          titleEl.textContent = \`OP #\${opId}\`;
          subEl.textContent = '';
          bodyEl.textContent = 'Carregando…';
          abrirEl.href = opUrl || ('/ops/' + opId);

          backdrop.style.display = 'flex';
          requestAnimationFrame(() => backdrop.classList.add('show'));
          document.body.style.overflow = 'hidden';

          fetch(\`/api/ops/\${encodeURIComponent(opId)}/resumo\`, { cache: 'no-store' })
            .then(r => r.ok ? r.json() : r.json().catch(() => ({})).then(j => { throw j; }))
            .then(data => {
              const codigo = data.codigo_op || (\`OP #\${opId}\`);
              titleEl.textContent = codigo;
              renderResumo(data);
            })
            .catch(() => {
              bodyEl.innerHTML = \`<div style="opacity:.8">Não consegui carregar o resumo dessa OP agora. Você ainda pode abrir a OP pelo botão abaixo.</div>\`;
            });
        };

        const el = document.getElementById("opsCalendar");
        if(!el) return;

        function getCalHeight(){
          try {
            return (window.matchMedia && window.matchMedia('(max-width: 900px)').matches) ? 520 : 420;
          } catch(_) { return 420; }
        }

        const cal = new FullCalendar.Calendar(el, {
          initialView: "listWeek",
          locale: "pt-br",
          firstDay: 1,
          height: getCalHeight(),
          contentHeight: getCalHeight() - 60,
          expandRows: false,
          nowIndicator: true,
          buttonText: {
            today: "hoje",
            month: "mês",
            week: "semana",
            day: "dia",
            list: "lista"
          },
          headerToolbar: {
            left: "prev,next today",
            center: "title",
            right: "listWeek,timeGridWeek,dayGridMonth"
          },
          editable: true,
          eventDurationEditable: false,
          eventStartEditable: true,
          eventClassNames: function(arg){
            const st = normStatus(arg.event.extendedProps && arg.event.extendedProps.status);
            const overdue = !!(arg.event.extendedProps && arg.event.extendedProps.overdue);
            const out = [];
            if (st) out.push("op-status-" + st);
            if (overdue) out.push("op-overdue");
            return out;
          },
          eventContent: function(arg){
            // Mostrar sempre o número/código da OP. Na LISTA, mostrar também o status ao lado.
            const codigo = (arg.event.extendedProps && arg.event.extendedProps.codigo_op) || "";
            const vendedor = (arg.event.extendedProps && arg.event.extendedProps.vendedor_nome) ? String(arg.event.extendedProps.vendedor_nome).trim() : "";
            const titleOnly = codigo || (arg.event.title || "").split(" - ")[0];
            const status = (arg.event.extendedProps && arg.event.extendedProps.status) || "";
            const overdue = !!(arg.event.extendedProps && arg.event.extendedProps.overdue);

            const wrap = document.createElement("div");
            wrap.className = "op-event";

            const title = document.createElement("span");
            title.className = "op-title";
            title.textContent = vendedor ? (titleOnly + " • " + vendedor) : titleOnly;
            wrap.appendChild(title);

            // Na visão de LISTA (listWeek/listDay), o status fica visível como badge.
            const viewType = (arg.view && arg.view.type) ? String(arg.view.type) : "";
            if (status && viewType.startsWith("list")) {
              const badge = document.createElement("span");
              badge.className = "op-status";
              badge.textContent = String(status).replace(/_/g, " ");
              wrap.appendChild(badge);
            }

            // Badge de atraso (bem visível na LISTA)
            if (overdue && viewType.startsWith("list")) {
              const b = document.createElement("span");
              b.className = "op-overdue-badge";
              b.textContent = "ATRASADA";
              wrap.appendChild(b);
            }

            return { domNodes: [wrap] };
          },
          eventDidMount: function(info){
            const status = (info.event.extendedProps && info.event.extendedProps.status) || "";
            const cliente = (info.event.title || "").includes(" - ")
              ? (info.event.title.split(" - ").slice(1).join(" - ").trim())
              : "";
            const codigo = (info.event.extendedProps && info.event.extendedProps.codigo_op) || (info.event.title || "").split(" - ")[0];
            const parts = [codigo];
            const vendedor = (info.event.extendedProps && info.event.extendedProps.vendedor_nome) ? String(info.event.extendedProps.vendedor_nome).trim() : "";
            if (vendedor) parts.push(vendedor);
            if (cliente) parts.push(cliente);
            if (status) parts.push(status);
            info.el.title = parts.filter(Boolean).join(" — ");

            // Deixa explícito que é clicável e abre modal rápido (principalmente na visão de lista)
            const opUrl = (info.event.extendedProps && info.event.extendedProps.op_url) || info.event.url || (info.event.id ? ("/ops/" + info.event.id) : null);
            if (opUrl) {
              info.el.style.cursor = "pointer";
              info.el.setAttribute("role", "link");
              info.el.setAttribute("tabindex", "0");

              // No FullCalendar listWeek, o link real costuma ser um <a> dentro do título
              const anchor = info.el.querySelector("a.fc-list-event-title") || info.el.querySelector("a");
              if (anchor) {
                anchor.href = opUrl;
                anchor.style.cursor = "pointer";
              }

              const go = (ev) => {
                if (ev) {
                  ev.preventDefault();
                  ev.stopPropagation();
                }
                // Se usuário quiser abrir em nova aba (ctrl/cmd), mantém o comportamento padrão.
                if (ev && (ev.ctrlKey || ev.metaKey)) {
                  window.open(opUrl, '_blank');
                  return;
                }
                openOpModal(info.event.id, opUrl);
              };

              // Clique no elemento do evento
              info.el.addEventListener("click", go);
              // Clique na linha inteira (visão lista)
              const row = info.el.closest("tr");
              if (row) row.addEventListener("click", go);
              // Clique no anchor (se existir)
              if (anchor) anchor.addEventListener("click", go);

              info.el.addEventListener("keydown", function(ev){
                if (ev.key === "Enter" || ev.key === " ") go(ev);
              });
            }
          },
          events: function(fetchInfo, success, failure){
            const qs = new URLSearchParams({ start: fetchInfo.startStr, end: fetchInfo.endStr });
            fetch("/api/dashboard/ops-calendario?" + qs.toString(), { cache: "no-store" })
              .then(r => r.json())
              .then(success)
              .catch(failure);
          },
          eventClick: function(info){
            // Abre modal rápido ao clicar no item (inclusive na visão "lista")
            info.jsEvent.preventDefault();
            const opUrl = (info.event.extendedProps && info.event.extendedProps.op_url) || info.event.url || (info.event && info.event.id ? ("/ops/" + info.event.id) : null);
            if (!opUrl) return;
            if (info.jsEvent && (info.jsEvent.ctrlKey || info.jsEvent.metaKey)) {
              window.open(opUrl, '_blank');
              return;
            }
            openOpModal(info.event.id, opUrl);
          },
          eventDrop: function(info){
            try {
              const id = info.event && info.event.id ? String(info.event.id) : "";
              const dt = info.event && info.event.start ? info.event.start : null;
              if (!id || !dt) {
                info.revert();
                return;
              }
              // Converte para YYYY-MM-DD
              const y = dt.getFullYear();
              const m = String(dt.getMonth() + 1).padStart(2, "0");
              const d = String(dt.getDate()).padStart(2, "0");
              // Evita template literals aqui porque este script pode estar embutido
              // dentro de outra template string (extraScripts), o que quebraria o parse.
              const dateStr = y + "-" + m + "-" + d;

              // O app está com express.urlencoded() (não express.json()).
              // Então enviamos como x-www-form-urlencoded para o backend ler em req.body.
              const formBody = new URLSearchParams({ data_entrega: dateStr }).toString();
              fetch("/ops/" + encodeURIComponent(id) + "/data-entrega", {
                method: "POST",
                headers: { "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8" },
                body: formBody
              }).then(async (r) => {
                if (!r.ok) throw new Error(await r.text());
                return r.json().catch(() => ({}));
              }).then(() => {
                // Atualiza classes (ex: virou atrasada / deixou de ser)
                if (btnEnviado) btnEnviado.style.display = (st === 'FINALIZADA') ? '' : 'none';
                if (btnEntregue) btnEntregue.style.display = (st === 'ENVIADO') ? '' : 'none';
                const locked = (st === 'ENVIADO' || st === 'ENTREGUE');
                if (btnProducao) btnProducao.style.display = locked ? 'none' : '';
                if (btnMaterial) btnMaterial.style.display = locked ? 'none' : '';
                if (btnFinalizar) btnFinalizar.style.display = locked ? 'none' : '';
                if (window.__opsCalendar) window.__opsCalendar.refetchEvents();
              }).catch((e) => {
                alert("Falha ao atualizar a data da OP: " + (e && e.message ? e.message : e));
                info.revert();
              });
            } catch (e) {
              info.revert();
            }
          }
        });
        window.__opsCalendar = cal;
        cal.render();

        // Ajusta altura ao redimensionar/trocar orientação
        window.addEventListener('resize', () => {
          try {
            const h = getCalHeight();
            cal.setOption('height', h);
            cal.setOption('contentHeight', h - 60);
          } catch(_) {}
        });
      });
      </script>
    `,
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
    lista_andamento
  });
});

function getDashboardExecutivoData(req) {
  // Datas (padrão: mês atual)
  const de = (req.query.de || db.prepare("SELECT date('now','start of month') as d").get().d);
  const ate = (req.query.ate || db.prepare("SELECT date('now') as d").get().d);
  const canal = (req.query.canal || "").trim();
  const statusRaw = (typeof req.query.status === 'string' ? req.query.status.trim() : 'FATURADO,ENTREGUE');
  const statusArr = statusRaw ? statusRaw.split(',').map(s => s.trim()).filter(Boolean) : [];

  const fmtMoney = (v) => (Number(v || 0)).toLocaleString('pt-BR', { minimumFractionDigits: 2, maximumFractionDigits: 2 });

  // canal normalizado (para bases antigas)
  const canalExprFor = (alias) => {
    const pfx = alias ? `${alias}.` : '';
    return `CASE
      WHEN COALESCE(NULLIF(${pfx}canal_venda,''), NULL) IS NOT NULL THEN UPPER(${pfx}canal_venda)
      WHEN COALESCE(NULLIF(${pfx}bling_id,''), NULL) IS NOT NULL THEN 'BLING'
      ELSE 'MANUAL'
    END`;
  };
  const canalExpr = canalExprFor('');
  const canalExprP = canalExprFor('p');

  const params = [];
  const whereNoAlias = [];
  const whereP = [];

  // Datas (aplicado em pedidos)
  whereNoAlias.push(`date(COALESCE(data_venda, criado_em)) >= date(?)`);
  whereP.push(`date(COALESCE(p.data_venda, p.criado_em)) >= date(?)`);
  params.push(de);
  whereNoAlias.push(`date(COALESCE(data_venda, criado_em)) <= date(?)`);
  whereP.push(`date(COALESCE(p.data_venda, p.criado_em)) <= date(?)`);
  params.push(ate);

  // Status
  if (statusArr.length) {
    whereNoAlias.push(`status IN (${statusArr.map(() => '?').join(',')})`);
    whereP.push(`p.status IN (${statusArr.map(() => '?').join(',')})`);
    params.push(...statusArr);
  }

  // Canal
  if (canal) {
    whereNoAlias.push(`${canalExpr} = ?`);
    whereP.push(`${canalExprP} = ?`);
    params.push(canal.toUpperCase());
  }

  const wherePedidos = whereNoAlias.length ? `WHERE ${whereNoAlias.join(' AND ')}` : '';
  const wherePedidosP = whereP.length ? `WHERE ${whereP.join(' AND ')}` : '';

  // KPIs
  const kpiRow = db.prepare(`
    SELECT
      COALESCE(SUM(total),0) as faturamento,
      COUNT(*) as pedidos
    FROM pedidos
    ${wherePedidos}
  `).get(...params);

  const pendRow = db.prepare(`
    SELECT COUNT(DISTINCT p.id) as pendentes
    FROM pedidos p
    LEFT JOIN (
      SELECT pedido_id, COALESCE(SUM(valor),0) as pago
      FROM pedido_pagamentos
      GROUP BY pedido_id
    ) pg ON pg.pedido_id = p.id
    ${wherePedidosP}
      AND COALESCE(pg.pago,0) < COALESCE(p.total,0)
  `).get(...params);

  const custosRow = db.prepare(`
    SELECT
      COALESCE(SUM(COALESCE(pi.custo_unit,0) * COALESCE(pi.quantidade,0)),0) as custo_total
    FROM pedido_itens pi
    JOIN pedidos p ON p.id = pi.pedido_id
    ${wherePedidosP}
  `).get(...params);

  const faturamento = Number(kpiRow?.faturamento || 0);
  const pedidosN = Number(kpiRow?.pedidos || 0);
  const custoTotal = Number(custosRow?.custo_total || 0);
  const margem = faturamento > 0 ? ((faturamento - custoTotal) / faturamento) : 0;

  // Cobertura de custo (itens com custo preenchido)
  const coberturaRow = db.prepare(`
    SELECT
      SUM(CASE WHEN COALESCE(pi.custo_unit,0) > 0 THEN 1 ELSE 0 END) as itens_com_custo,
      COUNT(*) as itens_total
    FROM pedido_itens pi
    JOIN pedidos p ON p.id = pi.pedido_id
    ${wherePedidosP}
  `).get(...params);
  const itensTotal = Number(coberturaRow?.itens_total || 0);
  const itensComCusto = Number(coberturaRow?.itens_com_custo || 0);
  const cobertura = itensTotal > 0 ? `${Math.round((itensComCusto / itensTotal) * 100)}%` : '—';

  const kpis = {
    faturamento_fmt: fmtMoney(faturamento),
    pedidos: pedidosN,
    ticket_fmt: fmtMoney(pedidosN ? (faturamento / pedidosN) : 0),
    pendentes: Number(pendRow?.pendentes || 0),
    margem_pct_fmt: (itensComCusto > 0 ? `${Math.round(margem * 100)}%` : '—'),
    cobertura_custo: cobertura,
  };

  // Série diária (para gráficos)
  const serieDias = db.prepare(`
    SELECT date(COALESCE(data_venda, criado_em)) as dia,
           COALESCE(SUM(total),0) as total,
           COUNT(*) as pedidos
    FROM pedidos
    ${wherePedidos}
    GROUP BY date(COALESCE(data_venda, criado_em))
    ORDER BY dia ASC
  `).all(...params).map(r => ({
    dia: r.dia,
    total: Number(r.total || 0),
    pedidos: Number(r.pedidos || 0)
  }));

  // Comparativo com período anterior (mesma duração)
  let kpisDelta = {
    faturamento_pct: null,
    pedidos_pct: null,
    ticket_pct: null,
    margem_pp: null,
  };
  try {
    const lenRow = db.prepare(`SELECT CAST((julianday(date(?)) - julianday(date(?)) + 1) AS INTEGER) as n`).get(ate, de);
    const nDias = Math.max(1, Number(lenRow?.n || 1));
    const prevAte = db.prepare("SELECT date(?, '-1 day') as d").get(de).d;
    const prevDe = db.prepare("SELECT date(?, ?) as d").get(prevAte, `-${Math.max(0, nDias - 1)} day`).d;

    const paramsPrev = [];
    const wherePrev = [];
    const wherePrevP = [];

    wherePrev.push(`date(COALESCE(data_venda, criado_em)) >= date(?)`);
    wherePrevP.push(`date(COALESCE(p.data_venda, p.criado_em)) >= date(?)`);
    paramsPrev.push(prevDe);
    wherePrev.push(`date(COALESCE(data_venda, criado_em)) <= date(?)`);
    wherePrevP.push(`date(COALESCE(p.data_venda, p.criado_em)) <= date(?)`);
    paramsPrev.push(prevAte);

    if (statusArr.length) {
      wherePrev.push(`status IN (${statusArr.map(() => '?').join(',')})`);
      wherePrevP.push(`p.status IN (${statusArr.map(() => '?').join(',')})`);
      paramsPrev.push(...statusArr);
    }
    if (canal) {
      wherePrev.push(`${canalExpr} = ?`);
      wherePrevP.push(`${canalExprP} = ?`);
      paramsPrev.push(canal.toUpperCase());
    }

    const wherePedidosPrev = wherePrev.length ? `WHERE ${wherePrev.join(' AND ')}` : '';
    const wherePedidosPrevP = wherePrevP.length ? `WHERE ${wherePrevP.join(' AND ')}` : '';

    const kpiPrev = db.prepare(`
      SELECT COALESCE(SUM(total),0) as faturamento, COUNT(*) as pedidos
      FROM pedidos
      ${wherePedidosPrev}
    `).get(...paramsPrev);

    const custosPrev = db.prepare(`
      SELECT COALESCE(SUM(COALESCE(pi.custo_unit,0) * COALESCE(pi.quantidade,0)),0) as custo_total
      FROM pedido_itens pi
      JOIN pedidos p ON p.id = pi.pedido_id
      ${wherePedidosPrevP}
    `).get(...paramsPrev);

    const covPrev = db.prepare(`
      SELECT
        SUM(CASE WHEN COALESCE(pi.custo_unit,0) > 0 THEN 1 ELSE 0 END) as itens_com_custo,
        COUNT(*) as itens_total
      FROM pedido_itens pi
      JOIN pedidos p ON p.id = pi.pedido_id
      ${wherePedidosPrevP}
    `).get(...paramsPrev);

    const fatPrev = Number(kpiPrev?.faturamento || 0);
    const pedPrev = Number(kpiPrev?.pedidos || 0);
    const ticketPrev = pedPrev ? (fatPrev / pedPrev) : 0;
    const custoPrev = Number(custosPrev?.custo_total || 0);
    const margemPrev = fatPrev > 0 ? ((fatPrev - custoPrev) / fatPrev) : 0;
    const itensComCustoPrev = Number(covPrev?.itens_com_custo || 0);

    const pct = (cur, prev) => (prev > 0 ? ((cur - prev) / prev) * 100 : null);

    kpisDelta = {
      faturamento_pct: pct(faturamento, fatPrev),
      pedidos_pct: pct(pedidosN, pedPrev),
      ticket_pct: pct(pedidosN ? (faturamento / pedidosN) : 0, ticketPrev),
      // margem em pontos percentuais (pp) só se houver custo preenchido em ambos os períodos
      margem_pp: (itensComCusto > 0 && itensComCustoPrev > 0) ? ((margem * 100) - (margemPrev * 100)) : null,
      _prev: { de: prevDe, ate: prevAte, dias: nDias },
    };
  } catch (e) {
    // ignora comparação se ambiente/banco não suportar
  }

  // Vendas por canal
  const porCanalRaw = db.prepare(`
    SELECT ${canalExpr} as canal, COUNT(*) as pedidos, COALESCE(SUM(total),0) as total
    FROM pedidos
    ${wherePedidos}
    GROUP BY ${canalExpr}
    ORDER BY total DESC
  `).all(...params);
  const totalCanal = porCanalRaw.reduce((a, r) => a + Number(r.total || 0), 0) || 0;
  const canalLabel = (c) => ({ MERCADOLIVRE: 'Mercado Livre', SHOPEE: 'Shopee', BLING: 'Bling', MANUAL: 'Manual' }[c] || (c || 'Manual'));
  const porCanal = porCanalRaw.map(r => ({
    canal: r.canal || 'MANUAL',
    canal_label: canalLabel(r.canal || 'MANUAL'),
    pedidos: Number(r.pedidos || 0),
    total: Number(r.total || 0),
    total_fmt: fmtMoney(r.total || 0),
    pct: totalCanal ? Math.round((Number(r.total || 0) / totalCanal) * 100) : 0,
  }));

  // Funil por status
  const funil = db.prepare(`
    SELECT status, COUNT(*) as pedidos, COALESCE(SUM(total),0) as total
    FROM pedidos
    ${wherePedidos}
    GROUP BY status
    ORDER BY pedidos DESC
  `).all(...params).map(r => ({
    status: r.status,
    pedidos: Number(r.pedidos || 0),
    total: Number(r.total || 0),
    total_fmt: fmtMoney(r.total || 0)
  }));

  // Top produtos
  // Observação: algumas bases antigas não possuem campos extras em pedido_itens.
  // O nome do item oficial no Acrilsoft é pi.descricao.
  const topProdutos = db.prepare(`
    SELECT COALESCE(pi.descricao, 'Item') as produto,
           COALESCE(SUM(pi.quantidade),0) as quantidade,
           COALESCE(SUM(COALESCE(pi.total_item,0)),0) as total
    FROM pedido_itens pi
    JOIN pedidos p ON p.id = pi.pedido_id
    ${wherePedidosP}
    GROUP BY produto
    ORDER BY total DESC
    LIMIT 20
  `).all(...params).map(r => ({
    produto: r.produto,
    quantidade: Number(r.quantidade || 0),
    total: Number(r.total || 0),
    total_fmt: fmtMoney(r.total || 0)
  }));

  // Top clientes (considera cadastro e avulso)
  const topClientes = db.prepare(`
    SELECT COALESCE(c.nome, c.razao_social, p.cliente_nome_avulso, 'Consumidor final') as cliente,
           COUNT(*) as pedidos,
           COALESCE(SUM(p.total),0) as total
    FROM pedidos p
    LEFT JOIN clientes c ON c.id = p.cliente_id
    ${wherePedidosP}
    GROUP BY cliente
    ORDER BY total DESC
    LIMIT 20
  `).all(...params).map(r => ({
    cliente: r.cliente,
    pedidos: Number(r.pedidos || 0),
    total: Number(r.total || 0),
    total_fmt: fmtMoney(r.total || 0)
  }));

  return {
    filtros: { de, ate, canal, status: statusRaw },
    kpis,
    kpisDelta,
    serieDias,
    porCanal,
    funil,
    topProdutos,
    topClientes,
  };
}



/* ===== API: Calendário da Dashboard ===== */
app.get("/api/dashboard/ops-calendario", (req, res) => {
  try {
    const start = (req.query.start || '').slice(0, 10);
    const end = (req.query.end || '').slice(0, 10);

    let sql = `
      SELECT id, codigo_op, cliente, vendedor_nome, status, data_entrega
      FROM ordens_producao
      WHERE COALESCE(arquivada,0)=0
        AND status <> 'CANCELADA'
        AND data_entrega IS NOT NULL
        AND TRIM(COALESCE(data_entrega,'')) <> ''
    `;
    const params = [];
    if (start && end) {
      sql += " AND date(data_entrega) BETWEEN date(?) AND date(?) ";
      params.push(start, end);
    }
    sql += " ORDER BY date(data_entrega) ASC, id DESC ";

    const rows = db.prepare(sql).all(...params);

    // Data de hoje (local) em YYYY-MM-DD
    const now = new Date();
    const todayStr = [
      String(now.getFullYear()).padStart(4, "0"),
      String(now.getMonth() + 1).padStart(2, "0"),
      String(now.getDate()).padStart(2, "0")
    ].join("-");

    const statusColors = {
      ABERTA: "#2563eb",
      EM_PRODUCAO: "#f59e0b",
      AGUARDANDO_MATERIAL: "#a855f7",
      FINALIZADA: "#16a34a",
      ENVIADO: "#0ea5e9",
      ENTREGUE: "#10b981",
      CANCELADA: "#6b7280"
    };

    const events = (rows || []).map(op => {
      const status = op.status || "ABERTA";
      const color = statusColors[status] || "#2563eb";
      const due = (op.data_entrega || "").slice(0, 10);
      const isFinal = status === "FINALIZADA" || status === "CANCELADA" || status === "ENVIADO" || status === "ENTREGUE";
      const overdue = !!(due && due < todayStr && !isFinal);
      return {
        id: String(op.id),
        title: (op.codigo_op || ('OP #' + op.id)) + ' - ' + (op.cliente || 'Cliente'),
        start: due,
        allDay: true,
        url: '/ops/' + op.id,
        backgroundColor: color,
        borderColor: color,
        editable: !isFinal,
        extendedProps: { status, codigo_op: op.codigo_op, cliente: op.cliente, vendedor_nome: op.vendedor_nome, op_url: '/ops/' + op.id, overdue }
      };
    });

    res.json(events);
  } catch (e) {
    console.error('ops-calendario erro:', e);
    res.status(500).json({ error: 'Falha ao carregar calendário' });
  }
});

/* ===== API: Resumo rápido da OP (para modal do calendário) ===== */
app.get("/api/ops/:id/resumo", (req, res) => {
  try {
    const id = String(req.params.id || '').trim();
    if (!id) return res.status(400).json({ error: 'ID inválido' });

    // Campos principais que aparecem no modal
    // Observação: no schema atual (SQLite/Postgres) as colunas são:
    //  - observacao_cliente (ou observacao) => anotações visíveis/gerais
    //  - observacao_interna => anotações internas
    // Para manter compat com o front do modal (que espera `observacoes` e
    // `observacoes_internas`), fazemos alias + COALESCE.
    const op = db.prepare(`
      SELECT id, codigo_op, cliente, vendedor_nome, status, data_entrega, prioridade,
             pedido_venda, produto_final, quantidade_final,
             enviado_em, enviado_por, entregue_em, entregue_por,
             COALESCE(observacao_cliente, observacao, '') AS observacoes,
             COALESCE(observacao_interna, '') AS observacoes_internas
      FROM ordens_producao
      WHERE id = ?
      LIMIT 1
    `).get(id);

    if (!op) return res.status(404).json({ error: 'OP não encontrada' });

    res.json({
      ...op,
      op_url: '/ops/' + op.id,
    });
  } catch (e) {
    console.error('op resumo erro:', e);
    res.status(500).json({ error: 'Falha ao carregar resumo da OP' });
  }
});
/* ===== Usuários (Admin) ===== */
const USER_ROLES = ["admin", "operador", "financeiro", "producao"]; // pode adicionar mais

const MODULE_OPTIONS = [
  { key: "dashboard", label: "Dashboard" },
  { key: "ops", label: "OPs" },
  { key: "estoque", label: "Estoque" },
  { key: "brindes", label: "Brindes" },
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
      SUM(estoque_atual) as total_chapas,
      -- total em m² = soma(chapas * (largura_mm*altura_mm)/1.000.000)
      SUM(estoque_atual * ((largura_mm * altura_mm) / 1000000.0)) as total_m2
    FROM produtos
  `).get();

  const mesAtual = new Date().toISOString().slice(0, 7);
  const movMes = db.prepare(`
    SELECT COUNT(*) as movs
    FROM movimentacoes
    WHERE criado_em LIKE ?
  `).get(mesAtual + '%');

  // Normaliza e pré-calcula m² por item (para usar no front sem repetir lógica)
  const produtosComM2 = (produtos || []).map(p => {
    const areaPorChapa = (Number(p.largura_mm) * Number(p.altura_mm)) / 1000000;
    const estoqueM2 = Number(p.estoque_atual || 0) * areaPorChapa;
    const minimoM2 = Number(p.estoque_minimo || 0) * areaPorChapa;
    return { ...p, areaPorChapa, estoqueM2, minimoM2 };
  });

  res.render("layout", {
    title: "Estoque",
    view: "estoque-page",
    produtos: produtosComM2,
    mmToM,
    fmtBR,
    fmtChapas,
    q,
    kpis,
    movMes,
  });
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


/* ===== Cadastro de Produtos (Catálogo de Venda) ===== */
app.get("/cadastro-produtos", (req, res) => {
  const q = String(req.query.q || "").trim();
  const tipo = String(req.query.tipo || "").trim();

  const where = [];
  const params = [];
  if (q) {
    where.push("(nome LIKE ? OR sku LIKE ? OR ncm LIKE ?)");
    params.push(`%${q}%`, `%${q}%`, `%${q}%`);
  }
  if (tipo) {
    where.push("tipo = ?");
    params.push(tipo);
  }
  const sql = `
    SELECT *
    FROM catalogo_produtos
    ${where.length ? ("WHERE " + where.join(" AND ")) : ""}
    ORDER BY ativo DESC, nome ASC
    LIMIT 1000
  `;
  const produtos = db.prepare(sql).all(...params);

  res.render("layout", { title: "Produtos", view: "cadastro-produtos-page", produtos, q, tipo, fmtBR });
});

app.get("/cadastro-produtos/novo", (req, res) => {
  res.render("layout", { title: "Novo Produto", view: "cadastro-produto-form-page", produto: null, modo: "novo", erro: null, ok: null });
});

app.post("/cadastro-produtos", (req, res) => {
  try {
    const nome = String(req.body.nome || "").trim();
    if (!nome) throw new Error("Informe o nome.");
    const sku = String(req.body.sku || "").trim() || null;
    const tipo = String(req.body.tipo || "PRODUTO").trim();
    const unidade = String(req.body.unidade || "UN").trim();
    const ativo = Number(req.body.ativo ?? 1) ? 1 : 0;
    const controla_estoque = Number(req.body.controla_estoque ?? 1) ? 1 : 0;

    const largura_mm = req.body.largura_mm !== "" && req.body.largura_mm !== undefined ? Number(req.body.largura_mm) : null;
    const altura_mm = req.body.altura_mm !== "" && req.body.altura_mm !== undefined ? Number(req.body.altura_mm) : null;
    const profundidade_mm = req.body.profundidade_mm !== "" && req.body.profundidade_mm !== undefined ? Number(req.body.profundidade_mm) : null;

    const preco_venda = Number(req.body.preco_venda || 0);
    const custo_unit = Number(req.body.custo_unit || 0);

    const ncm = String(req.body.ncm || "").trim() || null;
    const cfop = String(req.body.cfop || "").trim() || null;
    const origem = (req.body.origem !== "" && req.body.origem !== undefined) ? Number(req.body.origem) : null;

    const obs = String(req.body.obs || "").trim() || null;

    db.prepare(`
      INSERT INTO catalogo_produtos
      (nome, sku, tipo, unidade, ativo, controla_estoque, largura_mm, altura_mm, profundidade_mm, preco_venda, custo_unit, ncm, cfop, origem, obs, atualizado_em)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
    `).run(nome, sku, tipo, unidade, ativo, controla_estoque, largura_mm, altura_mm, profundidade_mm, preco_venda, custo_unit, ncm, cfop, origem, obs);

    return res.redirect("/cadastro-produtos");
  } catch (e) {
    return res.render("layout", { title: "Novo Produto", view: "cadastro-produto-form-page", produto: req.body, modo: "novo", erro: e.message || String(e), ok: null });
  }
});

app.get("/cadastro-produtos/:id", (req, res) => {
  const id = Number(req.params.id);
  const produto = db.prepare("SELECT * FROM catalogo_produtos WHERE id = ?").get(id);
  if (!produto) return res.status(404).send("Produto não encontrado");
  res.render("layout", { title: "Editar Produto", view: "cadastro-produto-form-page", produto, modo: "editar", erro: null, ok: null });
});

app.post("/cadastro-produtos/:id", (req, res) => {
  const id = Number(req.params.id);
  try {
    const nome = String(req.body.nome || "").trim();
    if (!nome) throw new Error("Informe o nome.");
    const sku = String(req.body.sku || "").trim() || null;
    const tipo = String(req.body.tipo || "PRODUTO").trim();
    const unidade = String(req.body.unidade || "UN").trim();
    const ativo = Number(req.body.ativo ?? 1) ? 1 : 0;
    const controla_estoque = Number(req.body.controla_estoque ?? 1) ? 1 : 0;

    const largura_mm = req.body.largura_mm !== "" && req.body.largura_mm !== undefined ? Number(req.body.largura_mm) : null;
    const altura_mm = req.body.altura_mm !== "" && req.body.altura_mm !== undefined ? Number(req.body.altura_mm) : null;
    const profundidade_mm = req.body.profundidade_mm !== "" && req.body.profundidade_mm !== undefined ? Number(req.body.profundidade_mm) : null;

    const preco_venda = Number(req.body.preco_venda || 0);
    const custo_unit = Number(req.body.custo_unit || 0);

    const ncm = String(req.body.ncm || "").trim() || null;
    const cfop = String(req.body.cfop || "").trim() || null;
    const origem = (req.body.origem !== "" && req.body.origem !== undefined) ? Number(req.body.origem) : null;
    const obs = String(req.body.obs || "").trim() || null;

    db.prepare(`
      UPDATE catalogo_produtos
      SET nome=?, sku=?, tipo=?, unidade=?, ativo=?, controla_estoque=?,
          largura_mm=?, altura_mm=?, profundidade_mm=?, preco_venda=?, custo_unit=?,
          ncm=?, cfop=?, origem=?, obs=?, atualizado_em=datetime('now')
      WHERE id=?
    `).run(nome, sku, tipo, unidade, ativo, controla_estoque, largura_mm, altura_mm, profundidade_mm, preco_venda, custo_unit, ncm, cfop, origem, obs, id);

    const produto = db.prepare("SELECT * FROM catalogo_produtos WHERE id = ?").get(id);
    return res.render("layout", { title: "Editar Produto", view: "cadastro-produto-form-page", produto, modo: "editar", erro: null, ok: "Salvo com sucesso." });
  } catch (e) {
    const produto = { ...req.body, id };
    return res.render("layout", { title: "Editar Produto", view: "cadastro-produto-form-page", produto, modo: "editar", erro: e.message || String(e), ok: null });
  }
});

// BOM / ficha técnica
app.get("/cadastro-produtos/:id/bom", (req, res) => {
  const id = Number(req.params.id);
  const produto = db.prepare("SELECT * FROM catalogo_produtos WHERE id = ?").get(id);
  if (!produto) return res.status(404).send("Produto não encontrado");

  const chapas = db.prepare("SELECT id, descricao, espessura_mm, cor FROM produtos ORDER BY descricao ASC").all();
  const itens = db.prepare(`
    SELECT b.*, p.descricao as chapa_desc, p.espessura_mm, p.cor
    FROM catalogo_produto_bom b
    JOIN produtos p ON p.id = b.chapa_id
    WHERE b.catalogo_produto_id = ?
    ORDER BY b.id DESC
  `).all(id);

  const itensPedido = db.prepare(`
    SELECT *
    FROM ordem_itens_pedido
    WHERE ordem_id = ?
    ORDER BY id ASC
  `).all(id);

  let consumo_total_m2 = 0;
  for (const it of itens) {
    const consumo = (Number(it.qtd||0) * Number(it.largura_mm||0) * Number(it.altura_mm||0)) / 1000000;
    it.consumo_m2 = consumo;
    consumo_total_m2 += consumo;
  }

  res.render("layout", { title: "Ficha técnica", view: "cadastro-produto-bom-page", produto, chapas, itens, consumo_total_m2 });
});

app.post("/cadastro-produtos/:id/bom", (req, res) => {
  const id = Number(req.params.id);
  const produto = db.prepare("SELECT * FROM catalogo_produtos WHERE id = ?").get(id);
  if (!produto) return res.status(404).send("Produto não encontrado");

  const chapa_id = Number(req.body.chapa_id);
  const qtd = Number(req.body.qtd || 1);
  const largura_mm = Number(req.body.largura_mm || 0);
  const altura_mm = Number(req.body.altura_mm || 0);
  const obs = String(req.body.obs || "").trim() || null;

  if (!chapa_id || !Number.isFinite(qtd) || qtd<=0 || !Number.isFinite(largura_mm) || !Number.isFinite(altura_mm) || largura_mm<=0 || altura_mm<=0) {
    return res.redirect(`/cadastro-produtos/${id}/bom`);
  }

  db.prepare(`
    INSERT INTO catalogo_produto_bom (catalogo_produto_id, chapa_id, qtd, largura_mm, altura_mm, obs)
    VALUES (?, ?, ?, ?, ?, ?)
  `).run(id, chapa_id, qtd, largura_mm, altura_mm, obs);

  res.redirect(`/cadastro-produtos/${id}/bom`);
});

app.post("/cadastro-produtos/:id/bom/:bomId/remover", (req, res) => {
  const id = Number(req.params.id);
  const bomId = Number(req.params.bomId);
  db.prepare(`DELETE FROM catalogo_produto_bom WHERE id = ? AND catalogo_produto_id = ?`).run(bomId, id);
  res.redirect(`/cadastro-produtos/${id}/bom`);
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

  res.render("layout", { title: "Movimentar", view: "movimentar-page", produto, mmToM, fmtBR, fmtChapas });
});

app.post("/movimentar/:id", (req, res) => {
  const produtoId = Number(req.params.id);
  const { tipo, quantidade, observacao } = req.body;

  const qtd = Number(quantidade);
  if (!["ENTRADA", "SAIDA", "AJUSTE"].includes(tipo)) return res.status(400).send("Tipo inválido");
  if (!Number.isFinite(qtd) || qtd <= 0) return res.status(400).send("Quantidade inválida");

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

/* ===== Dashboard Executivo (Financeiro) ===== */
app.get("/financeiro/dashboard", requireAuth, (req, res) => {
  // Mantido por compatibilidade — o dashboard executivo agora está integrado no /dashboard
  return res.redirect('/dashboard#executivo');
});

/* ===== Financeiro (ERP) ===== */
app.get("/financeiro", requireAuth, requireModule("financeiro"), (req, res) => {
  try {
    const receber = db.prepare(`
      SELECT COALESCE(SUM(valor - COALESCE(valor_pago,0)),0) as v
      FROM financeiro_titulos
      WHERE tipo='RECEBER' AND status IN ('ABERTO','PARCIAL')
    `).get().v;

    const pagar = db.prepare(`
      SELECT COALESCE(SUM(valor - COALESCE(valor_pago,0)),0) as v
      FROM financeiro_titulos
      WHERE tipo='PAGAR' AND status IN ('ABERTO','PARCIAL')
    `).get().v;

    const receberVenc = db.prepare(`
      SELECT COUNT(*) as c
      FROM financeiro_titulos
      WHERE tipo='RECEBER' AND status IN ('ABERTO','PARCIAL') AND date(vencimento) < date('now')
    `).get().c;

    const pagarVenc = db.prepare(`
      SELECT COUNT(*) as c
      FROM financeiro_titulos
      WHERE tipo='PAGAR' AND status IN ('ABERTO','PARCIAL') AND date(vencimento) < date('now')
    `).get().c;

    const saldosPorConta = db.prepare(`
      SELECT c.id, c.nome, c.tipo,
        COALESCE(SUM(CASE WHEN m.tipo='ENTRADA' THEN m.valor ELSE 0 END),0)
        - COALESCE(SUM(CASE WHEN m.tipo='SAIDA' THEN m.valor ELSE 0 END),0) as saldo
      FROM financeiro_contas c
      LEFT JOIN financeiro_movimentos m ON m.conta_id = c.id
      WHERE c.ativo = 1
      GROUP BY c.id
      ORDER BY c.nome ASC
    `).all();

    const saldoTotal = saldosPorConta.reduce((acc, r) => acc + (Number(r.saldo||0)||0), 0);

    const movs30d = db.prepare(`
      SELECT COUNT(*) as c
      FROM financeiro_movimentos
      WHERE date(data_mov) >= date('now','-30 day')
    `).get().c;

    return res.render("layout", {
      title: "Financeiro",
      view: "financeiro-dashboard-page",
      activeMenu: "financeiro",
      resumo: {
        receber_aberto: receber,
        pagar_aberto: pagar,
        receber_vencidos: receberVenc,
        pagar_vencidos: pagarVenc,
        saldo_total: saldoTotal,
        movs_30d: movs30d
      },
      saldosPorConta
    });
  } catch (e) {
    console.error("Financeiro /financeiro erro:", e);
    return res.render("layout", {
      title: "Financeiro",
      view: "forbidden-page",
      activeMenu: "financeiro",
      pageTitle: "Financeiro não inicializado",
      pageSubtitle: "Verifique as tabelas do banco (financeiro_contas / financeiro_titulos)."
    });
  }
});


/* ===== Fluxo de Caixa (projeção) ===== */
app.get("/financeiro/fluxo", requireAuth, requireModule("financeiro"), (req, res) => {
  try {
    const dias = Math.min(365, Math.max(7, Number(req.query.dias || 30) || 30));
    const inicio = String(req.query.inicio || new Date().toISOString().slice(0,10)).slice(0,10);

    const saldosPorConta = db.prepare(`
      SELECT c.id, c.nome, c.tipo,
        COALESCE(SUM(CASE WHEN m.tipo='ENTRADA' THEN m.valor ELSE 0 END),0)
        - COALESCE(SUM(CASE WHEN m.tipo='SAIDA' THEN m.valor ELSE 0 END),0) as saldo
      FROM financeiro_contas c
      LEFT JOIN financeiro_movimentos m ON m.conta_id = c.id
      WHERE c.ativo = 1
      GROUP BY c.id
      ORDER BY c.nome ASC
    `).all();
    const saldoInicial = saldosPorConta.reduce((acc, r) => acc + (Number(r.saldo||0)||0), 0);

    const receber = db.prepare(`
      SELECT date(vencimento) as d, COALESCE(SUM(valor - COALESCE(valor_pago,0)),0) as v
      FROM financeiro_titulos
      WHERE tipo='RECEBER' AND status IN ('ABERTO','PARCIAL')
        AND date(vencimento) >= date(?) AND date(vencimento) < date(?, '+' || ? || ' day')
      GROUP BY date(vencimento)
    `).all(inicio, inicio, dias);

    const pagar = db.prepare(`
      SELECT date(vencimento) as d, COALESCE(SUM(valor - COALESCE(valor_pago,0)),0) as v
      FROM financeiro_titulos
      WHERE tipo='PAGAR' AND status IN ('ABERTO','PARCIAL')
        AND date(vencimento) >= date(?) AND date(vencimento) < date(?, '+' || ? || ' day')
      GROUP BY date(vencimento)
    `).all(inicio, inicio, dias);

    const mapIn = new Map(receber.map(r => [r.d, Number(r.v||0)||0]));
    const mapOut = new Map(pagar.map(r => [r.d, Number(r.v||0)||0]));

    const rows = [];
    let saldo = saldoInicial;

    for (let i=0; i<dias; i++){
      const d = db.prepare("SELECT date(?, '+' || ? || ' day') as d").get(inicio, i).d;
      const entradas = mapIn.get(d) || 0;
      const saidas = mapOut.get(d) || 0;
      const liquido = entradas - saidas;
      saldo = saldo + liquido;
      rows.push({ data: d, entradas, saidas, liquido, saldo });
    }

    const totalEntradas = rows.reduce((a,r)=>a+r.entradas,0);
    const totalSaidas = rows.reduce((a,r)=>a+r.saidas,0);

    return res.render("layout", {
      title: "Fluxo de Caixa",
      view: "financeiro-fluxo-page",
      activeMenu: "financeiro",
      filtros: { dias, inicio },
      resumo: { saldo_inicial: saldoInicial, entradas: totalEntradas, saidas: totalSaidas, saldo_final: saldo },
      rows,
      saldosPorConta
    });
  } catch (e) {
    console.error("Fluxo de caixa erro:", e);
    req.session.flash = { error: e.message };
    return res.redirect("/financeiro");
  }
});

function financeiroListTitulos({ tipo, q, de, ate, status }) {
  const where = ["t.tipo = ?"];
  const params = [tipo];

  if (status) {
    const st = String(status).toUpperCase();
    if (st === 'ABERTO') {
      // Por padrão, ABERTO inclui títulos parcialmente pagos
      where.push("t.status IN ('ABERTO','PARCIAL')");
    } else {
      where.push("UPPER(t.status) = ?");
      params.push(st);
    }
  }
  if (de) { where.push("date(t.vencimento) >= date(?)"); params.push(de); }
  if (ate) { where.push("date(t.vencimento) <= date(?)"); params.push(ate); }
  if (q) {
    where.push(`(
      UPPER(COALESCE(t.descricao,'')) LIKE ?
      OR UPPER(COALESCE(t.origem,'')) LIKE ?
      OR CAST(COALESCE(t.origem_id,0) AS TEXT) LIKE ?
      OR UPPER(COALESCE(c.nome,c.razao_social,f.nome,'')) LIKE ?
    )`);
    const qq = `%${String(q).toUpperCase()}%`;
    params.push(qq, qq, `%${String(q)}%`, qq);
  }

  const sql = `
    SELECT
      t.*,
      COALESCE(c.nome, c.razao_social, f.nome) as pessoa_nome,
      pc.nome as plano_nome,
      (SELECT m.id FROM financeiro_movimentos m WHERE m.titulo_id=t.id AND COALESCE(m.estornado,0)=0 ORDER BY date(m.data_mov) DESC, m.id DESC LIMIT 1) as last_mov_id,
      (SELECT m.valor FROM financeiro_movimentos m WHERE m.titulo_id=t.id AND COALESCE(m.estornado,0)=0 ORDER BY date(m.data_mov) DESC, m.id DESC LIMIT 1) as last_mov_valor,
      (SELECT m.data_mov FROM financeiro_movimentos m WHERE m.titulo_id=t.id AND COALESCE(m.estornado,0)=0 ORDER BY date(m.data_mov) DESC, m.id DESC LIMIT 1) as last_mov_data
    FROM financeiro_titulos t
    LEFT JOIN clientes c ON (t.pessoa_tipo='CLIENTE' AND t.pessoa_id=c.id)
    LEFT JOIN fornecedores f ON (t.pessoa_tipo='FORNECEDOR' AND t.pessoa_id=f.id)
    LEFT JOIN financeiro_plano_contas pc ON pc.id = t.plano_conta_id
    WHERE ${where.join(" AND ")}
    ORDER BY date(t.vencimento) ASC, t.id DESC
    LIMIT 500
  `;
  return db.prepare(sql).all(...params);
}

app.get("/financeiro/receber", requireAuth, requireModule("financeiro"), (req, res) => {
  const filtros = {
    q: req.query.q || "",
    de: req.query.de || "",
    ate: req.query.ate || "",
    status: req.query.status || "ABERTO",
  };
  const titulos = financeiroListTitulos({ tipo: "RECEBER", ...filtros });
  const contasList = db.prepare("SELECT id, nome, tipo, ativo FROM financeiro_contas WHERE ativo=1 ORDER BY nome ASC").all();
  res.render("layout", {
    title: "Contas a Receber",
    view: "financeiro-titulos-page",
    activeMenu: "financeiro",
    listTipo: "RECEBER",
    filtros,
    titulos,
    contasList
  });
});

app.get("/financeiro/pagar", requireAuth, requireModule("financeiro"), (req, res) => {
  const filtros = {
    q: req.query.q || "",
    de: req.query.de || "",
    ate: req.query.ate || "",
    status: req.query.status || "ABERTO",
  };
  const titulos = financeiroListTitulos({ tipo: "PAGAR", ...filtros });
  const contasList = db.prepare("SELECT id, nome, tipo, ativo FROM financeiro_contas WHERE ativo=1 ORDER BY nome ASC").all();
  res.render("layout", {
    title: "Contas a Pagar",
    view: "financeiro-titulos-page",
    activeMenu: "financeiro",
    listTipo: "PAGAR",
    filtros,
    titulos,
    contasList
  });
});

app.post("/financeiro/titulos/:id/baixar", requireAuth, requireModule("financeiro"), (req, res) => {
  try {
    const id = Number(req.params.id);
    const contaId = Number(req.body.conta_id || 0) || null;
    const dataMov = (req.body.data_mov || new Date().toISOString().slice(0,10)).slice(0,10);
    const valorBaixaInput = Number(String(req.body.valor_baixa || "").replace(",", ".")) || 0;

    const t = db.prepare("SELECT * FROM financeiro_titulos WHERE id=?").get(id);
    if (!t) return res.redirect("back");

    const st = String(t.status || "").toUpperCase();
    if (st === "PAGO" || st === "CANCELADO") {
      req.session.flash = { error: "Este título não pode receber baixa (status: " + st + ")." };
      return res.redirect("back");
    }

    if (!contaId) {
      req.session.flash = { error: "Selecione uma conta para dar baixa." };
      return res.redirect("back");
    }

    const valorTotal = Number(t.valor || 0) || 0;
    const valorPagoAtual = Number(t.valor_pago || 0) || 0;
    const restante = Math.max(0, valorTotal - valorPagoAtual);

    // Se não informar valor, baixa o restante (baixa total).
    const valorBaixa = Math.max(0, Math.min(restante, (valorBaixaInput > 0 ? valorBaixaInput : restante)));

    if (valorBaixa <= 0) {
      req.session.flash = { error: "Valor de baixa inválido." };
      return res.redirect("back");
    }

    const novoPago = valorPagoAtual + valorBaixa;
    const novoStatus = (novoPago + 0.00001 >= valorTotal) ? "PAGO" : "PARCIAL";

    const tipoMov = (String(t.tipo).toUpperCase() === "PAGAR") ? "SAIDA" : "ENTRADA";
    const desc = (t.descricao || "") + (t.origem === "PEDIDO" && t.origem_id ? ` (Pedido #${t.origem_id})` : "");

    db.prepare(`
      UPDATE financeiro_titulos
      SET status=?, pago_em=?, valor_pago=?, conta_id=?, atualizado_em=datetime('now')
      WHERE id=?
    `).run(novoStatus, dataMov, novoPago, contaId, id);

    db.prepare(`
      INSERT INTO financeiro_movimentos (conta_id, titulo_id, tipo, valor, data_mov, descricao)
      VALUES (?,?,?,?,?,?)
    `).run(contaId, id, tipoMov, valorBaixa, dataMov, desc);

    return res.redirect("back");
  } catch (e) {
    console.error("Baixa (parcial) título erro:", e);
    req.session.flash = { error: e.message };
    return res.redirect("back");
  }
});


app.post("/financeiro/titulos/baixar-lote", requireAuth, requireModule("financeiro"), (req, res) => {
  try {
    let ids = req.body.titulo_ids || req.body.titulo_id || [];
    if (!Array.isArray(ids)) ids = [ids];
    ids = ids.map(x => Number(x)).filter(Boolean);

    const contaId = Number(req.body.conta_id || 0) || null;
    const dataMov = (req.body.data_mov || new Date().toISOString().slice(0,10)).slice(0,10);

    if (!ids.length) {
      req.session.flash = { error: "Selecione pelo menos 1 título para dar baixa em lote." };
      return res.redirect("back");
    }
    if (!contaId) {
      req.session.flash = { error: "Selecione uma conta para dar baixa em lote." };
      return res.redirect("back");
    }

    let ok = 0;
    ids.forEach(id => {
      const t = db.prepare("SELECT * FROM financeiro_titulos WHERE id=?").get(id);
      if (!t) return;
      const st = String(t.status || "").toUpperCase();
      if (!["ABERTO","PARCIAL"].includes(st)) return;

      const valorTotal = Number(t.valor || 0) || 0;
      const valorPagoAtual = Number(t.valor_pago || 0) || 0;
      const restante = Math.max(0, valorTotal - valorPagoAtual);
      if (restante <= 0) return;

      const tipoMov = (String(t.tipo).toUpperCase() === "PAGAR") ? "SAIDA" : "ENTRADA";
      const desc = (t.descricao || "") + (t.origem === "PEDIDO" && t.origem_id ? ` (Pedido #${t.origem_id})` : "");

      const novoPago = valorPagoAtual + restante;
      const novoStatus = "PAGO";

      db.prepare(`
        UPDATE financeiro_titulos
        SET status=?, pago_em=?, valor_pago=?, conta_id=?, atualizado_em=datetime('now')
        WHERE id=?
      `).run(novoStatus, dataMov, novoPago, contaId, id);

      db.prepare(`
        INSERT INTO financeiro_movimentos (conta_id, titulo_id, tipo, valor, data_mov, descricao, estornado, estorno_de_id)
        VALUES (?,?,?,?,?,?,0,NULL)
      `).run(contaId, id, tipoMov, restante, dataMov, desc);

      ok += 1;
    });

    req.session.flash = { success: `Baixa em lote concluída: ${ok} título(s) baixado(s).` };
    return res.redirect("back");
  } catch (e) {
    console.error("Baixa em lote erro:", e);
    req.session.flash = { error: e.message };
    return res.redirect("back");
  }
});

app.post("/financeiro/movimentos/:id/estornar", requireAuth, requireModule("financeiro"), (req, res) => {
  try {
    const movId = Number(req.params.id);
    const mov = db.prepare("SELECT * FROM financeiro_movimentos WHERE id=?").get(movId);
    if (!mov) {
      req.session.flash = { error: "Movimento não encontrado." };
      return res.redirect("back");
    }
    if (Number(mov.estornado || 0) === 1) {
      req.session.flash = { error: "Este movimento já foi estornado." };
      return res.redirect("back");
    }

    // Não permitir estornar um movimento que já é um estorno (evita "estorno do estorno")
    if (mov.estorno_de_id) {
      req.session.flash = { error: "Este movimento é um estorno. Selecione a baixa original para estornar." };
      return res.redirect("back");
    }

    const t = mov.titulo_id ? db.prepare("SELECT * FROM financeiro_titulos WHERE id=?").get(mov.titulo_id) : null;
    if (!t) {
      req.session.flash = { error: "Título do movimento não encontrado." };
      return res.redirect("back");
    }

    // Registra o estorno como novo movimento (oposto)
    const tipoOposto = (String(mov.tipo).toUpperCase() === "ENTRADA") ? "SAIDA" : "ENTRADA";
    const dataMov = (req.body.data_mov || new Date().toISOString().slice(0,10)).slice(0,10);
    const desc = `ESTORNO: ${mov.descricao || ""}`.trim();

    db.prepare(`
      INSERT INTO financeiro_movimentos (conta_id, titulo_id, tipo, valor, data_mov, descricao, estornado, estorno_de_id)
      VALUES (?,?,?,?,?,?,0,?)
    `).run(mov.conta_id, mov.titulo_id, tipoOposto, mov.valor, dataMov, desc, movId);

    db.prepare("UPDATE financeiro_movimentos SET estornado=1 WHERE id=?").run(movId);

    // Ajusta o título (reduz valor_pago e recalcula status)
    const valorTotal = Number(t.valor || 0) || 0;
    const valorPagoAtual = Number(t.valor_pago || 0) || 0;
    const novoPago = Math.max(0, valorPagoAtual - (Number(mov.valor || 0) || 0));

    let novoStatus = "ABERTO";
    let pagoEm = null;
    if (novoPago <= 0.00001) {
      novoStatus = "ABERTO";
    } else if (novoPago + 0.00001 >= valorTotal) {
      novoStatus = "PAGO";
      pagoEm = t.pago_em || dataMov;
    } else {
      novoStatus = "PARCIAL";
    }

    db.prepare(`
      UPDATE financeiro_titulos
      SET status=?, pago_em=?, valor_pago=?, atualizado_em=datetime('now')
      WHERE id=?
    `).run(novoStatus, pagoEm, novoPago, t.id);

    req.session.flash = { success: "Estorno realizado com sucesso." };
    return res.redirect("back");
  } catch (e) {
    console.error("Estorno erro:", e);
    req.session.flash = { error: e.message };
    return res.redirect("back");
  }
});


app.get("/financeiro/titulos/:id/historico", requireAuth, requireModule("financeiro"), (req, res) => {
  try {
    const id = Number(req.params.id);
    const titulo = db.prepare(`
      SELECT
        t.*,
        COALESCE(c.nome, c.razao_social, f.nome) as pessoa_nome,
        pc.nome as plano_nome,
        ct.nome as conta_nome
      FROM financeiro_titulos t
      LEFT JOIN clientes c ON (t.pessoa_tipo='CLIENTE' AND t.pessoa_id=c.id)
      LEFT JOIN fornecedores f ON (t.pessoa_tipo='FORNECEDOR' AND t.pessoa_id=f.id)
      LEFT JOIN financeiro_plano_contas pc ON pc.id = t.plano_conta_id
      LEFT JOIN financeiro_contas ct ON ct.id = t.conta_id
      WHERE t.id=?
    `).get(id);

    if (!titulo) {
      req.session.flash = { error: "Título não encontrado." };
      return res.redirect("/financeiro");
    }

    const movimentos = db.prepare(`
      SELECT m.*, c.nome as conta_nome
      FROM financeiro_movimentos m
      LEFT JOIN financeiro_contas c ON c.id = m.conta_id
      WHERE m.titulo_id=?
      ORDER BY date(m.data_mov) ASC, m.id ASC
    `).all(id);

    const contasList = db.prepare("SELECT id, nome, tipo, ativo FROM financeiro_contas WHERE ativo=1 ORDER BY nome ASC").all();

    res.render("layout", {
      title: `Histórico do Título #${id}`,
      view: "financeiro-titulo-historico-page",
      activeMenu: "financeiro",
      titulo,
      movimentos,
      contasList,
    });
  } catch (e) {
    console.error("Histórico do título erro:", e);
    req.session.flash = { error: e.message };
    return res.redirect("back");
  }
});


app.post("/financeiro/titulos/:id/cancelar", requireAuth, requireModule("financeiro"), (req, res) => {
  try {
    const id = Number(req.params.id);
    db.prepare("UPDATE financeiro_titulos SET status='CANCELADO', atualizado_em=datetime('now') WHERE id=?").run(id);
    return res.redirect("back");
  } catch (e) {
    console.error("Cancelar título erro:", e);
    req.session.flash = { error: e.message };
    return res.redirect("back");
  }
});

app.get("/financeiro/contas", requireAuth, requireModule("financeiro"), (req, res) => {
  const contasList = db.prepare("SELECT id, nome, tipo, ativo, criado_em FROM financeiro_contas ORDER BY nome ASC").all();
  res.render("layout", { title: "Contas", view: "financeiro-contas-page", activeMenu: "financeiro", contasList });
});

app.post("/financeiro/contas", requireAuth, requireModule("financeiro"), (req, res) => {
  const nome = String(req.body.nome || "").trim();
  const tipo = String(req.body.tipo || "BANCO").trim().toUpperCase();
  if (!nome) return res.redirect("/financeiro/contas");
  db.prepare("INSERT INTO financeiro_contas (nome, tipo, ativo) VALUES (?,?,1)").run(nome, tipo);
  res.redirect("/financeiro/contas");
});

app.get("/financeiro/plano-contas", requireAuth, requireModule("financeiro"), (req, res) => {
  const planoList = db.prepare("SELECT id, codigo, nome, tipo, ativo FROM financeiro_plano_contas ORDER BY tipo ASC, codigo ASC, nome ASC").all();
  res.render("layout", { title: "Plano de Contas", view: "financeiro-plano-contas-page", activeMenu: "financeiro", planoList });
});

app.post("/financeiro/plano-contas", requireAuth, requireModule("financeiro"), (req, res) => {
  const codigo = (req.body.codigo || "").toString().trim() || null;
  const nome = (req.body.nome || "").toString().trim();
  const tipo = (req.body.tipo || "DESPESA").toString().trim().toUpperCase();
  if (!nome) return res.redirect("/financeiro/plano-contas");
  db.prepare("INSERT INTO financeiro_plano_contas (codigo, nome, tipo, pai_id, ativo) VALUES (?,?,?,?,1)")
    .run(codigo, nome, tipo, null);
  res.redirect("/financeiro/plano-contas");
});



app.get("/financeiro/fiscal", requireAuth, requireModule("fiscal"), (req, res) => {
  const emitente = db.prepare("SELECT * FROM fiscal_emitente WHERE id = 1").get() || null;
  const cfg55 = ensureFiscalConfig(55);
  const cfg65 = null; // NFC-e removida (NF-e apenas)
  const docs = db.prepare(`
    SELECT id, pedido_id, modelo, serie, numero, chave, status, criado_em
    FROM fiscal_documentos
    ORDER BY id DESC
    LIMIT 30
  `).all();

  res.render("layout", {
    title: "Fiscal (NF-e/NFC-e)",
    view: "fiscal-config-page",
    activeMenu: "fiscal",
    emitente,
    cfg55,
    cfg65,
    docs,
    flash: res.locals.flash || null
  });
});

app.post("/financeiro/fiscal/salvar", requireAuth, requireModule("fiscal"), upload.single("certificado"), (req, res) => {
  const b = req.body || {};
  // Emitente (linha única)
  const emit = {
    cnpj: b.emitente_cnpj || null,
    razao_social: b.emitente_razao || null,
    fantasia: b.emitente_fantasia || null,
    ie: b.emitente_ie || null,
    im: b.emitente_im || null,
    crt: b.emitente_crt || null,
    cnae: b.emitente_cnae || null,
    cep: b.emitente_cep || null,
    logradouro: b.emitente_logradouro || null,
    numero: b.emitente_numero || null,
    complemento: b.emitente_complemento || null,
    bairro: b.emitente_bairro || null,
    cidade: b.emitente_cidade || null,
    uf: b.emitente_uf || null,
    codigo_ibge_municipio: b.emitente_ibge || null,
    telefone: b.emitente_telefone || null,
    email: b.emitente_email || null,
  };

  const hasEmit = db.prepare("SELECT id FROM fiscal_emitente WHERE id = 1").get();
  if (hasEmit) {
    db.prepare(`
      UPDATE fiscal_emitente
      SET cnpj=?, razao_social=?, fantasia=?, ie=?, im=?, crt=?, cnae=?, cep=?, logradouro=?, numero=?, complemento=?, bairro=?, cidade=?, uf=?, codigo_ibge_municipio=?, telefone=?, email=?, atualizado_em=datetime('now')
      WHERE id=1
    `).run(
      emit.cnpj, emit.razao_social, emit.fantasia, emit.ie, emit.im, emit.crt, emit.cnae,
      emit.cep, emit.logradouro, emit.numero, emit.complemento, emit.bairro, emit.cidade, emit.uf,
      emit.codigo_ibge_municipio, emit.telefone, emit.email
    );
  } else {
    db.prepare(`
      INSERT INTO fiscal_emitente (id, cnpj, razao_social, fantasia, ie, im, crt, cnae, cep, logradouro, numero, complemento, bairro, cidade, uf, codigo_ibge_municipio, telefone, email)
      VALUES (1,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    `).run(
      emit.cnpj, emit.razao_social, emit.fantasia, emit.ie, emit.im, emit.crt, emit.cnae,
      emit.cep, emit.logradouro, emit.numero, emit.complemento, emit.bairro, emit.cidade, emit.uf,
      emit.codigo_ibge_municipio, emit.telefone, emit.email
    );
  }

  const certPath = req.file ? req.file.path : null;
  const certSenha = (b.certificado_senha || "").trim() || null;

  const updCfg = (modelo, ambiente, serie, prox, uf, cscId, csc) => {
    ensureFiscalConfig(modelo);
    const curr = getFiscalConfig(modelo);
    db.prepare(`
      UPDATE fiscal_config
      SET ambiente=?, serie=?, proximo_numero=?, uf=?,
          certificado_path=COALESCE(?, certificado_path),
          certificado_senha=COALESCE(?, certificado_senha),
          csc_id=COALESCE(?, csc_id),
          csc=COALESCE(?, csc),
          atualizado_em=datetime('now')
      WHERE modelo=?
    `).run(
      Number(ambiente || curr.ambiente || 2),
      Number(serie || curr.serie || 1),
      Number(prox || curr.proximo_numero || 1),
      (uf || curr.uf || emit.uf || null),
      certPath,
      certSenha,
      (cscId || curr.csc_id || null),
      (csc || curr.csc || null),
      modelo
    );
  };

  updCfg(55, b.nfe_ambiente, b.nfe_serie, b.nfe_proximo, b.nfe_uf, null, null);
  try { db.prepare(`UPDATE fiscal_config SET service_url = ?, atualizado_em=datetime('now') WHERE modelo=55`).run((b.service_url||null)); } catch (e) {}

  req.session.flash = { success: "Configurações fiscais salvas." };
  res.redirect("/financeiro/fiscal");
});

function registrarDocumentoFiscalSimulado({ pedidoId, modelo }) {
  const cfg = ensureFiscalConfig(modelo);
  const serie = Number(cfg.serie || 1);
  const numero = Number(cfg.proximo_numero || 1);
  const uf = cfg.uf || (db.prepare("SELECT uf FROM fiscal_emitente WHERE id=1").get() || {}).uf || '00';
  const chave = genFiscalChaveSimulada({ uf, modelo, serie, numero });

  db.prepare(`
    INSERT INTO fiscal_documentos (pedido_id, modelo, serie, numero, chave, status, ambiente, xml)
    VALUES (?,?,?,?,?,?,?,?)
  `).run(pedidoId, modelo, serie, numero, chave, "EM_PROCESSAMENTO", cfg.ambiente, null);

  db.prepare(`UPDATE fiscal_config SET proximo_numero = proximo_numero + 1, atualizado_em=datetime('now') WHERE modelo=?`).run(modelo);
}

app.post("/fiscal/emitir/nfe/:pedidoId", requireAuth, requireModule("fiscal"), async (req, res) => {
  const pedidoId = Number(req.params.pedidoId);
  if (!pedidoId) return res.redirect("/pedidos");

  const cfg = ensureFiscalConfig(55);
  const serviceUrl = (cfg && cfg.service_url) ? String(cfg.service_url).trim() : (process.env.FISCAL_SERVICE_URL || "");
  if (!serviceUrl) {
    registrarDocumentoFiscalSimulado({ pedidoId, modelo: 55 });
    req.session.flash = { success: "NF-e registrada (modo simulado). Configure a URL do Serviço Fiscal para emitir de verdade." };
    return res.redirect(`/pedidos/${pedidoId}`);
  }

  try {
    const emitente = db.prepare("SELECT * FROM fiscal_emitente WHERE id = 1").get() || null;
    if (!emitente || !emitente.cnpj || !emitente.uf) {
      req.session.flash = { error: "Configure o Emitente (CNPJ e UF) antes de emitir NF-e." };
      return res.redirect("/financeiro/fiscal");
    }

    const pedido = db.prepare("SELECT * FROM pedidos WHERE id = ?").get(pedidoId);
    if (!pedido) return res.redirect("/pedidos");

    const itens = db.prepare("SELECT * FROM pedido_itens WHERE pedido_id = ? ORDER BY id").all(pedidoId);

    let cliente = null;
    if (pedido.cliente_id) cliente = db.prepare("SELECT * FROM clientes WHERE id = ?").get(pedido.cliente_id) || null;

    const serie = Number(cfg.serie || 1);
    const numero = Number(cfg.proximo_numero || 1);
    const ambiente = Number(cfg.ambiente || 2);

    const payload = {
      pedido_id: pedidoId,
      ambiente: ambiente === 1 ? "PRODUCAO" : "HOMOLOGACAO",
      emitente: {
        uf: emitente.uf,
        cnpj: emitente.cnpj,
        razao: emitente.razao_social,
        fantasia: emitente.fantasia,
        ie: emitente.ie,
        crt: emitente.crt,
        cidade_ibge: emitente.codigo_ibge_municipio,
        endereco: {
          logradouro: emitente.logradouro,
          numero: emitente.numero,
          bairro: emitente.bairro,
          cep: emitente.cep,
          municipio: emitente.municipio,
          uf: emitente.uf,
          codigo_ibge_municipio: emitente.codigo_ibge_municipio
        }
      },
      certificado: {
        pfx_path: cfg.certificado_path || null,
        senha: cfg.certificado_senha || null
      },
      config: { serie, numero, natureza: cfg.natureza_operacao || null },
      destinatario: {
        nome: (cliente?.razao_social || cliente?.nome || pedido.cliente_nome_avulso || "CONSUMIDOR").toString(),
        cpf_cnpj: (cliente?.cpf_cnpj || "").toString(),
        ie: (cliente?.ie || "").toString(),
        endereco: {
          logradouro: (cliente?.logradouro || "").toString(),
          numero: (cliente?.numero || "").toString(),
          bairro: (cliente?.bairro || "").toString(),
          cep: (cliente?.cep || "").toString(),
          municipio: (cliente?.municipio || "").toString(),
          uf: (cliente?.uf || emitente.uf || "").toString(),
          codigo_ibge_municipio: (cliente?.codigo_ibge_municipio || emitente.codigo_ibge_municipio || "").toString()
        }
      },
      itens: (itens || []).map((i) => ({
        descricao: (i.descricao || "").toString(),
        qtd: Number(i.quantidade || 1),
        vl_unit: Number(i.preco_unitario || i.valor_unitario || 0),
        vl_total: Number(i.total || 0),
        ncm: (i.ncm || "").toString(),
        cfop: (i.cfop || "").toString(),
        cprod: String(i.produto_id || i.id || ""),
        ucom: "UN"
      })),
      totais: {
        produtos: Number(pedido.subtotal_itens || pedido.subtotal || 0),
        frete: Number(pedido.frete_valor || pedido.frete || 0),
        desconto: Number(pedido.desconto_valor || pedido.desconto || 0),
        total: Number(pedido.total || 0)
      },
      observacao: (pedido.observacao_fiscal || pedido.observacoes || `Pedido ${pedidoId}`).toString()
    };

    const resp = await fetch(serviceUrl.replace(/\/$/, "") + "/nfe/emitir", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload)
    });

    const raw = await resp.text();
    let data = null;
    try { data = JSON.parse(raw); } catch (e) { data = { status: "ERRO", mensagem: raw }; }

    if (!resp.ok || !data) throw new Error(data?.mensagem || "Falha ao emitir NF-e no serviço fiscal.");

    const chave = data.chave || null;
    const protocolo = data.protocolo || null;
    const xml = data.xml_autorizado || data.nfeProc || data.xml_rascunho || null;
    const status = data.status || "EM_PROCESSAMENTO";
    const mensagem = data.mensagem || null;

    db.prepare(`
      INSERT INTO fiscal_documentos (pedido_id, modelo, serie, numero, chave, protocolo, status, ambiente, xml, mensagem)
      VALUES (?,?,?,?,?,?,?,?,?,?)
    `).run(pedidoId, 55, serie, numero, chave, protocolo, status, (ambiente === 1 ? 1 : 2), xml, mensagem);

    if (status === "AUTORIZADA" || status === "ENVIADA" || status === "PRONTO_PARA_ENVIO") {
      db.prepare(`UPDATE fiscal_config SET proximo_numero = proximo_numero + 1, atualizado_em=datetime('now') WHERE modelo=55`).run();
    }

    req.session.flash = { success: `NF-e: ${status}. ${mensagem || ""}`.trim() };
    return res.redirect(`/pedidos/${pedidoId}`);
  } catch (e) {
    req.session.flash = { error: "Erro ao emitir NF-e: " + (e.message || e) };
    return res.redirect(`/pedidos/${pedidoId}`);
  }
});

app.post("/fiscal/emitir/nfce/:pedidoId", requireAuth, requireModule("fiscal"), (req, res) => {
  const pedidoId = Number(req.params.pedidoId);
  req.session.flash = { error: "NFC-e está desativada neste sistema (NF-e apenas)." };
  return res.redirect(pedidoId ? `/pedidos/${pedidoId}` : "/pedidos");
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



/* =====================
   CLIENTES (cadastro completo)
   ===================== */

// Rotas de clientes modularizadas (refactor gradual)
app.use('/', require('./routes/clientes'));
app.use('/', require('./routes/orcamentos_produtos'));

app.get("/pedidos", requireModule("pedidos"), (req, res) => {
  try {
    const q = String(req.query.q || "").trim();
    const status = String(req.query.status || "").trim();
    const vendedor = String(req.query.vendedor || "").trim();
    const de = String(req.query.de || "").trim();
    const ate = String(req.query.ate || "").trim();
    const canal = String(req.query.canal || "").trim();

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
    if (canal) { where.push("UPPER(COALESCE(canal_venda,'MANUAL')) = ?"); params.push(canal.toUpperCase()); }

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
      filtros: { q, status, vendedor, de, ate, canal }
    });
  } catch (err) {
    console.error("Erro ao listar pedidos:", err);
    res.status(500).send(err.message);
  }
});

// Central (Kanban) de Pedidos
app.get("/pedidos/kanban", requireModule("pedidos"), (req, res) => {
  try {
    const q = String(req.query.q || "").trim();
    const de = String(req.query.de || "").trim();
    const ate = String(req.query.ate || "").trim();
    const canal = String(req.query.canal || "").trim();

    res.render("layout", {
      title: "Central de Pedidos",
      view: "pedidos-kanban-page",
      filtros: { q, de, ate, canal }
    });
  } catch (err) {
    console.error("Erro ao abrir kanban de pedidos:", err);
    res.status(500).send(err.message);
  }
});

// API: dados do Kanban (agrupados por status)
app.get("/api/pedidos/kanban", requireModule("pedidos"), (req, res) => {
  try {
    const q = String(req.query.q || "").trim();
    const de = String(req.query.de || "").trim();
    const ate = String(req.query.ate || "").trim();
    const canal = String(req.query.canal || "").trim();

    const where = [];
    const params = [];
    if (q) {
      where.push("(COALESCE(cliente_nome_avulso, cliente_nome, '') LIKE ? OR COALESCE(cliente_contato,'') LIKE ? OR COALESCE(vendedor_nome,'') LIKE ? OR CAST(p.id AS TEXT) LIKE ? OR COALESCE(bling_numero,'') LIKE ?)");
      const like = `%${q}%`;
      params.push(like, like, like, like, like);
    }
    if (de) { where.push("date(p.criado_em) >= date(?)"); params.push(de); }
    if (ate) { where.push("date(p.criado_em) <= date(?)"); params.push(ate); }
    if (canal) { where.push("UPPER(COALESCE(p.canal_venda,'MANUAL')) = ?"); params.push(canal.toUpperCase()); }

    // Kanban mostra somente as fases operacionais
    const allowed = ['RASCUNHO','ORCAMENTO','APROVADO','EM_PRODUCAO','PRONTO','FATURADO','ENTREGUE','CANCELADO'];
    where.push(`p.status IN (${allowed.map(()=>'?').join(',')})`);
    params.push(...allowed);

    const sql = `
      SELECT
        p.*,
        COALESCE(pp.pago_total, 0) AS pago_total,
        ip.external_status AS external_status
      FROM pedidos p
      LEFT JOIN (
        SELECT pedido_id, SUM(valor) AS pago_total
        FROM pedido_pagamentos
        GROUP BY pedido_id
      ) pp ON pp.pedido_id = p.id
      LEFT JOIN integracao_pedidos ip ON ip.pedido_id = p.id
      ${where.length ? "WHERE " + where.join(" AND ") : ""}
      ORDER BY p.id DESC
      LIMIT 1200
    `;

    const rows = db.prepare(sql).all(...params);

    const columns = [
      { key: 'ORCAMENTOS', label: 'Orçamentos', statuses: ['RASCUNHO','ORCAMENTO'] },
      { key: 'APROVADO', label: 'Aprovados', statuses: ['APROVADO'] },
      { key: 'EM_PRODUCAO', label: 'Em Produção', statuses: ['EM_PRODUCAO'] },
      { key: 'PRONTO', label: 'Pronto', statuses: ['PRONTO'] },
      { key: 'FATURADO', label: 'Faturado', statuses: ['FATURADO'] },
      { key: 'ENTREGUE', label: 'Entregue', statuses: ['ENTREGUE'] },
      { key: 'CANCELADO', label: 'Cancelado', statuses: ['CANCELADO'] },
    ];

    const colMap = new Map(columns.map(c => [c.key, { ...c, pedidos: [], metrics: { count: 0, total: 0, pendentes: 0 } }]));
    const eps = 0.009;

    for (const p of rows) {
      const st = String(p.status || '').toUpperCase();
      const col = columns.find(c => c.statuses.includes(st));
      if (!col) continue;
      const bucket = colMap.get(col.key);

      const total = Number(p.total || 0) || 0;
      const pago = Number(p.pago_total || 0) || 0;
      const pendente = (total > 0 && (pago + eps) < total);

      bucket.pedidos.push({
        id: p.id,
        status: st,
        canal_venda: (p.canal_venda || 'MANUAL'),
        external_status: p.external_status || null,
        cliente: (p.cliente_nome_avulso || p.cliente_nome || ''),
        telefone: (p.cliente_telefone_avulso || p.cliente_contato || ''),
        total,
        pago_total: pago,
        criado_em: p.criado_em,
        op_id: p.op_id || null,
      });

      bucket.metrics.count += 1;
      bucket.metrics.total += total;
      if (pendente) bucket.metrics.pendentes += 1;
    }

    const out = Array.from(colMap.values()).map(c => ({
      key: c.key,
      label: c.label,
      statuses: c.statuses,
      metrics: {
        count: c.metrics.count,
        total: Math.round((c.metrics.total + Number.EPSILON) * 100) / 100,
        pendentes: c.metrics.pendentes,
      },
      pedidos: c.pedidos
    }));

    return res.json({ ok: true, filtros: { q, de, ate, canal }, columns: out });
  } catch (err) {
    console.error("Erro ao montar kanban:", err);
    res.status(500).json({ ok: false, error: err.message });
  }
});

// API: muda status (para Drag & Drop no Kanban)
app.post(
  "/api/pedidos/:id/status",
  express.json({ type: ["application/json", "text/json", "*/json"] }),
  requireModule("pedidos"),
  (req, res) => {
    try {
      const id = Number(req.params.id);
      const novo = String((req.body && req.body.status) ? req.body.status : "").toUpperCase();
      if (!PEDIDO_STATUS.includes(novo)) return sendError(res, "Status inválido.", 400);

      const row = db.prepare("SELECT id, status, cliente_id, total FROM pedidos WHERE id = ?").get(id);
      const antigo = String(row?.status || "").toUpperCase();
      const nowIso = new Date().toISOString();

      db.prepare("UPDATE pedidos SET status = ?, atualizado_em = ? WHERE id = ?").run(novo, nowIso, id);

      // Integração Financeiro:
      // Quando entrar em APROVADO, gera automaticamente contas a receber (1 título por pagamento).
      try {
        if (antigo !== "APROVADO" && novo === "APROVADO") {
          const ja = db.prepare(`
            SELECT COUNT(*) as c
            FROM financeiro_titulos
            WHERE tipo='RECEBER' AND origem='PEDIDO' AND origem_id=? AND status <> 'CANCELADO'
          `).get(id)?.c || 0;

          if (ja === 0) {
            const contaPadrao = db.prepare("SELECT id FROM financeiro_contas WHERE ativo=1 ORDER BY id ASC LIMIT 1").get();
            const planoReceita = db.prepare("SELECT id FROM financeiro_plano_contas WHERE UPPER(tipo)='RECEITA' AND ativo=1 ORDER BY id ASC LIMIT 1").get();

            const contaId = contaPadrao ? Number(contaPadrao.id) : null;
            const planoId = planoReceita ? Number(planoReceita.id) : null;

            const pags = db.prepare("SELECT * FROM pedido_pagamentos WHERE pedido_id = ? ORDER BY id ASC").all(id);
            const totalPedido = Number(row?.total || 0) || 0;

            const hoje = new Date().toISOString().slice(0, 10);

            const mkVenc = (d) => {
              if (!d) return hoje;
              const s = String(d).trim();
              // aceita "YYYY-MM-DD" ou "YYYY-MM-DD HH:MM:SS"
              if (s.length >= 10) return s.slice(0, 10);
              return hoje;
            };

            const insert = db.prepare(`
              INSERT INTO financeiro_titulos
                (tipo, origem, origem_id, pessoa_tipo, pessoa_id, descricao, plano_conta_id, conta_id, valor, vencimento, competencia, status, forma_pagamento, criado_em, atualizado_em)
              VALUES
                ('RECEBER', 'PEDIDO', ?, 'CLIENTE', ?, ?, ?, ?, ?, ?, ?, 'ABERTO', ?, datetime('now'), datetime('now'))
            `);

            if (Array.isArray(pags) && pags.length) {
              for (const pg of pags) {
                const venc = mkVenc(pg.data_prevista);
                const comp = venc.slice(0, 7);
                const desc = `Pedido #${id} - ${String(pg.forma || "Pagamento").toUpperCase()}${(pg.parcelas && Number(pg.parcelas) > 1) ? ` (${pg.parcelas}x)` : ""}`;
                insert.run(id, row?.cliente_id || null, desc, planoId, contaId, Number(pg.valor || 0), venc, comp, (pg.forma || null));
              }
            } else {
              // Se não tem pagamentos cadastrados, gera 1 título único do total do pedido
              const venc = hoje;
              const comp = hoje.slice(0, 7);
              const desc = `Pedido #${id} - Venda`;
              insert.run(id, row?.cliente_id || null, desc, planoId, contaId, totalPedido, venc, comp, null);
            }
          }
        }
      } catch (e) {
        console.error("[Financeiro] Falha ao gerar contas a receber do pedido:", e);
        // não impede mudança de status
      }


      // Integração Estoque (consumo de chapas via ficha técnica):
      // Quando entrar em APROVADO, se os itens do pedido estiverem vinculados a um produto do catálogo,
      // baixa automaticamente as chapas conforme BOM (m²).
      try {
        if (antigo !== "APROVADO" && novo === "APROVADO") {
          const jaMov = db.prepare(`
            SELECT COUNT(*) as c
            FROM movimentacoes
            WHERE origem='PEDIDO' AND origem_id=? AND tipo='SAIDA'
          `).get(id)?.c || 0;

          if (jaMov === 0) {
            const itens = db.prepare(`SELECT id, produto_id, quantidade FROM pedido_itens WHERE pedido_id = ?`).all(id);

            const movIns = db.prepare(`
              INSERT INTO movimentacoes (produto_id, tipo, quantidade, observacao, observacao_interna, criado_em, origem, origem_id, item_id)
              VALUES (?, 'SAIDA', ?, ?, ?, datetime('now'), 'PEDIDO', ?, ?)
            `);

            for (const it of itens) {
              const catId = it.produto_id ? Number(it.produto_id) : null;
              const qtdItem = Number(it.quantidade || 0);
              if (!catId || !(qtdItem > 0)) continue;

              const bom = db.prepare(`
                SELECT b.*, p.id as chapa_id
                FROM catalogo_produto_bom b
                JOIN produtos p ON p.id = b.chapa_id
                WHERE b.catalogo_produto_id = ?
              `).all(catId);

              for (const bi of bom) {
                const consumo_m2_unit = (Number(bi.qtd || 0) * Number(bi.largura_mm || 0) * Number(bi.altura_mm || 0)) / 1000000;
                const consumo_total = consumo_m2_unit * qtdItem;
                if (!(consumo_total > 0)) continue;

                // baixa no estoque da chapa
                db.prepare(`UPDATE produtos SET estoque_atual = COALESCE(estoque_atual,0) - ? WHERE id = ?`).run(consumo_total, Number(bi.chapa_id));

                // registra movimento
                const obs = `Consumo automático (Pedido #${id})`;
                const obsInt = `AUTO_PEDIDO_${id}`;
                movIns.run(Number(bi.chapa_id), consumo_total, obs, obsInt, id, Number(it.id));
              }
            }
          }
        }
      } catch (e) {
        console.error("[Estoque] Falha ao baixar consumo do pedido:", e);
        // não impede mudança de status
      }


      return res.json({ ok: true, id, status: novo });
    } catch (err) {
      console.error(err);
      res.status(500).json({ ok: false, error: err.message });
    }
  }
);


// ===================== BLING (UI) =====================
app.get("/bling/pedidos", requireModule("bling"), async (req, res) => {
  try {
    await ready;
    const token = await getBlingAccessToken(req);
    const pagina = Number(req.query.pagina || 1);
    const limite = Number(req.query.limite || 50);
    const filtros = {
      pagina,
      limite,
      situacao: req.query.situacao || "",
      dataInicial: req.query.dataInicial || "",
      dataFinal: req.query.dataFinal || "",
    };

    const params = {
      pagina,
      limite,
      dataInicial: filtros.dataInicial || undefined,
      dataFinal: filtros.dataFinal || undefined,
      situacao: filtros.situacao || undefined,
    };

    const raw = await blingGet({ token, path: "/pedidos/vendas", params });
    const data = raw && (raw.data || raw.dados || raw);
    const list = Array.isArray(data) ? data : (Array.isArray(data?.pedidos) ? data.pedidos : []);
    const norm = list.map(normPedidoVenda).filter(Boolean);

    // Marca quais já estão importados localmente
    let importedMap = new Map();
    try {
      const rows = db.prepare("SELECT id, bling_id FROM pedidos WHERE bling_id IS NOT NULL AND bling_id <> ''").all();
      for (const r of rows) importedMap.set(String(r.bling_id), Number(r.id));
    } catch (_) {}

    const pedidos = norm.map(p => ({
      id: p.id,
      numero: p.numero,
      data: p.data,
      situacao: p.situacao,
      cliente: p.contato ? { codigo: p.contato.codigo, nome: p.contato.nome, cnpjcpf: p.contato.cnpjcpf } : null,
      itens: p.itens || [],
      ja_importado: importedMap.has(String(p.id || "")),
      pedido_id_local: importedMap.get(String(p.id || "")) || null,
    }));

    return res.render("layout", {
    title: "Pedidos Bling",
    view: "bling-pedidos-page",
    activeMenu: "blingpedidos",
    pedidos,
    filtros,
  });
  } catch (e) {
    const status = e && e.status ? `\nHTTP: ${e.status}` : "";
    const payload = (e && e.payload) ? `\nPayload: ${JSON.stringify(e.payload, null, 2)}` : "";
    return res.status(400).send(`<pre>Falha ao carregar pedidos do Bling: ${(e && e.message) ? e.message : String(e)}${status}${payload}</pre>`);
  }
});





function loadClientesSelect(limit = 500) {
  try {
    return db.prepare(`
      SELECT
        c.id,
        COALESCE(c.razao_social, c.nome) AS nome_exibicao,
        c.fantasia,
        COALESCE(c.cpf_cnpj, c.cnpjcpf) AS doc,
        COALESCE(c.whatsapp, c.telefone, c.contato) AS contato,
        c.email,
        e.cep,
        e.logradouro,
        e.numero,
        e.complemento,
        e.bairro,
        e.cidade,
        e.uf
      FROM clientes c
      LEFT JOIN cliente_enderecos e
        ON e.id = (
          SELECT id
          FROM cliente_enderecos
          WHERE cliente_id = c.id
          ORDER BY principal DESC, id DESC
          LIMIT 1
        )
      WHERE COALESCE(c.ativo, 1) = 1
      ORDER BY nome_exibicao COLLATE NOCASE
      LIMIT ?
`).all(limit);
  } catch (e) {
    return [];
  }

}



app.get("/pedidos/novo", requireModule("novo_pedido"), (req, res) => {
  const clientes = loadClientesSelect(500);
  const catalogoProdutos = db.prepare("SELECT id, nome, sku, unidade, preco_venda, custo_unit, largura_mm, altura_mm FROM catalogo_produtos WHERE ativo=1 ORDER BY nome ASC").all();
  res.render("layout", {
    title: "Novo Pedido",
    view: "pedido-form-page",
    pedido: null,
    itens: [],
    clientes,
    catalogoProdutos,
    modo: "novo"
  });
});

app.post("/pedidos", requireModule("novo_pedido"), uploadPedido.single("anexo"), (req, res) => {
  try {
    const cliente_nome = String(req.body.cliente_nome || "").trim();
    const cliente_contato = String(req.body.cliente_contato || "").trim();
    const cliente_id = (req.body.cliente_id !== undefined && String(req.body.cliente_id).trim() !== '') ? Number(req.body.cliente_id) : null;
    const cliente_nome_avulso = String(req.body.cliente_nome_avulso || '').trim();
    const cliente_telefone_avulso = String(req.body.cliente_telefone_avulso || '').trim();

    // Se veio cliente_id, puxa dados do cadastro e preenche campos de exibição
    let cliente_nome_eff = cliente_nome;
    let cliente_contato_eff = cliente_contato;
    let cliente_codigo_eff = null;
    let cliente_cnpjcpf_eff = null;
    let cliente_endereco_eff = null;
    if (cliente_id) {
      const c = db.prepare('SELECT * FROM clientes WHERE id = ?').get(cliente_id);
      if (c) {
        cliente_nome_eff = (String(c.tipo||'').toUpperCase()==='PJ') ? (c.razao_social || c.nome || '') : (c.nome || c.razao_social || '');
        cliente_contato_eff = c.telefone || c.whatsapp || c.contato || cliente_contato_eff;
        cliente_codigo_eff = c.codigo || null;
        cliente_cnpjcpf_eff = c.cpf_cnpj || c.cnpjcpf || null;
        const e = db.prepare("SELECT * FROM cliente_enderecos WHERE cliente_id = ? AND principal = 1 ORDER BY id DESC LIMIT 1").get(cliente_id);
        if (e) {
          const parts = [e.logradouro, e.numero, e.bairro, e.cidade, e.uf].filter(Boolean);
          cliente_endereco_eff = parts.join(' - ') || null;
        }
      }
    } else {
      // avulso
      if (cliente_nome_avulso) cliente_nome_eff = cliente_nome_avulso;
      if (cliente_telefone_avulso) cliente_contato_eff = cliente_telefone_avulso;
      if (!cliente_nome_eff) cliente_nome_eff = 'CONSUMIDOR FINAL';
    }

    const vendedor_nome = String(req.body.vendedor_nome || "").trim();
    const prazo_entrega = String(req.body.prazo_entrega || "").trim();
    const data_venda = String(req.body.data_venda || "").trim();
    const data_validade = String(req.body.data_validade || "").trim();
    const canal_venda = String(req.body.canal_venda || "").trim();
    const tipo_venda = String(req.body.tipo_venda || "PRODUTO").trim() || "PRODUTO";
    const tipo_entrega = String(req.body.tipo_entrega || "").trim();
    const endereco_entrega_texto = String(req.body.endereco_entrega_texto || "").trim();
    const endereco_entrega_id = (req.body.endereco_entrega_id !== undefined && String(req.body.endereco_entrega_id).trim() !== '') ? Number(req.body.endereco_entrega_id) : null;

    const bling_numero = String(req.body.bling_numero || "").trim();
    const bling_pedido_compra = String(req.body.bling_pedido_compra || "").trim();
    const prioridade = String(req.body.prioridade || "NORMAL").trim() || "NORMAL";
    const status = String(req.body.status || "RASCUNHO").trim() || "RASCUNHO";

    const desconto_tipo = String(req.body.desconto_tipo || "VALOR").trim() || "VALOR";
    const desconto_valor = num(req.body.desconto_valor);
    const frete_valor = num(req.body.frete_valor);

    const natureza_operacao = String(req.body.natureza_operacao || "").trim();
    const consumidor_final = (String(req.body.consumidor_final || '1') === '1') ? 1 : 0;
    const presenca_comprador = String(req.body.presenca_comprador || "").trim();
    const observacao_fiscal = String(req.body.observacao_fiscal || "").trim();

    const observacoes_vendedor = String(req.body.observacoes_vendedor || "").trim();
    const observacoes_internas = String(req.body.observacoes_internas || "").trim();
    const anexo_arquivo = req.file ? req.file.filename : null;

    // Itens
    const descs = arrify(req.body.item_descricao).map(x => String(x||'').trim());
    const qtds  = arrify(req.body.item_quantidade);
    const unis  = arrify(req.body.item_unidade);
    const obsis = arrify(req.body.item_observacao);
    const largs = arrify(req.body.item_largura);
    const alts  = arrify(req.body.item_altura);
    const udims = arrify(req.body.item_unidade_dim);
    const precs = arrify(req.body.item_preco_unit);
    const descItem = arrify(req.body.item_desconto);
    const pids = arrify(req.body.item_produto_id);

    const itensNorm = [];
    for (let i = 0; i < descs.length; i++) {
      const d = descs[i];
      if (!d) continue;
      const q = num(qtds[i]);
      const u = String(unis[i] || '').trim() || 'UN';
      const o = String(obsis[i] || '').trim();
      const largura = num(largs[i]);
      const altura = num(alts[i]);
      const unidade_dim = String(udims[i] || 'MM').toUpperCase();
      const area_m2 = calcAreaM2(largura, altura, unidade_dim);
      const preco_unit = num(precs[i]);
      const desconto_item = num(descItem[i]);
      const total_item = (q * preco_unit) - desconto_item;

      const pidRaw = (pids && pids[i] !== undefined) ? String(pids[i]).trim() : "";
      const produto_id = pidRaw ? Number(pidRaw) : null;
      itensNorm.push({ produto_id, descricao: d, quantidade: q, unidade: u, observacao: o, largura, altura, unidade_dim, area_m2, preco_unit, desconto_item, total_item });
    }

    const totals = recalcPedidoTotals(itensNorm, desconto_tipo, desconto_valor, frete_valor);

    const now = new Date().toISOString();
    const tx = db.transaction(() => {
      const info = db.prepare(`
        INSERT INTO pedidos
          (cliente_id, cliente_nome, cliente_contato, cliente_nome_avulso, cliente_telefone_avulso,
           vendedor_nome, prazo_entrega, data_venda, data_validade, canal_venda, tipo_venda, tipo_entrega, endereco_entrega_texto, endereco_entrega_id,
           prioridade, status,
           subtotal_itens, desconto_tipo, desconto_valor, frete_valor, total,
           natureza_operacao, consumidor_final, presenca_comprador, observacao_fiscal,
           observacoes_vendedor, observacoes_internas, anexo_arquivo, bling_numero, bling_pedido_compra,
           cliente_codigo, cliente_cnpjcpf, cliente_endereco, criado_em, atualizado_em)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      `).run(
        cliente_id || null,
        (cliente_nome_eff || null),
        (cliente_contato_eff || null),
        (cliente_id ? null : (cliente_nome_avulso || null)),
        (cliente_id ? null : (cliente_telefone_avulso || null)),

        vendedor_nome || null,
        prazo_entrega || null,
        data_venda || null,
        data_validade || null,
        canal_venda || null,
        tipo_venda,
        tipo_entrega || null,
        endereco_entrega_texto || null,
        endereco_entrega_id || null,

        prioridade,
        status,

        totals.subtotal_itens,
        totals.desconto_tipo,
        totals.desconto_valor,
        totals.frete_valor,
        totals.total,

        natureza_operacao || null,
        consumidor_final,
        presenca_comprador || null,
        observacao_fiscal || null,

        observacoes_vendedor || null,
        observacoes_internas || null,
        anexo_arquivo,
        bling_numero || null,
        bling_pedido_compra || null,

        cliente_codigo_eff,
        cliente_cnpjcpf_eff,
        cliente_endereco_eff,
        now,
        now
      );
      const pedido_id = info.lastInsertRowid;

      for (const it of itensNorm) {
        db.prepare(`
          INSERT INTO pedido_itens (pedido_id, produto_id, descricao, quantidade, unidade, observacao, largura, altura, unidade_dim, area_m2, preco_unit, desconto_item, total_item)
          VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        `).run(
          pedido_id,
          it.produto_id,
          it.descricao,
          it.quantidade || null,
          it.unidade || null,
          it.observacao || null,
          it.largura || null,
          it.altura || null,
          it.unidade_dim || 'MM',
          it.area_m2 || 0,
          it.preco_unit || 0,
          it.desconto_item || 0,
          it.total_item || 0
        );
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
    const pagamentos = db.prepare(`SELECT * FROM pedido_pagamentos WHERE pedido_id = ? ORDER BY id DESC`).all(id);

    let opResumo = null;
    try {
      if (pedido && pedido.op_id) {
        opResumo = db.prepare('SELECT id, codigo_op, status FROM ordens_producao WHERE id = ?').get(pedido.op_id) || null;
        if (opResumo) {
          const row = db.prepare('SELECT COALESCE(SUM(quantidade * preco_unit),0) as t FROM op_servicos WHERE ordem_id = ?').get(opResumo.id);
          opResumo.total_servicos = Number(row?.t || 0);
          opResumo.custo_total = opResumo.total_servicos;
        }
      }
    } catch (e) { opResumo = opResumo || null; }

    const clientes = loadClientesSelect(500);
  const catalogoProdutos = db.prepare("SELECT id, nome, sku, unidade, preco_venda, custo_unit, largura_mm, altura_mm FROM catalogo_produtos WHERE ativo=1 ORDER BY nome ASC").all();

    res.render("layout", {
      title: `Pedido #${id}`,
      view: "pedido-detalhe-page",
      pedido,
      itens,
    itensPedido,
      pagamentos,
      clientes,
      opResumo
    });
  } catch (err) {
    console.error("Erro ao abrir pedido:", err);
    res.status(500).send(err.message);
  }
});



app.post('/pedidos/:id/vincular-cliente', requireModule('pedidos'), (req, res) => {
  try {
    const id = Number(req.params.id);
    const cliente_id = (req.body.cliente_id !== undefined && String(req.body.cliente_id).trim() !== '') ? Number(req.body.cliente_id) : null;

    let cliente_nome_eff = null;
    let cliente_contato_eff = null;
    let cliente_codigo_eff = null;
    let cliente_cnpjcpf_eff = null;
    let cliente_endereco_eff = null;

    if (cliente_id) {
      const c = db.prepare('SELECT * FROM clientes WHERE id = ?').get(cliente_id);
      if (c) {
        cliente_nome_eff = (String(c.tipo||'').toUpperCase()==='PJ') ? (c.razao_social || c.nome || '') : (c.nome || c.razao_social || '');
        cliente_contato_eff = c.telefone || c.whatsapp || c.contato || null;
        cliente_codigo_eff = c.codigo || null;
        cliente_cnpjcpf_eff = c.cpf_cnpj || c.cnpjcpf || null;
        const e = db.prepare("SELECT * FROM cliente_enderecos WHERE cliente_id = ? AND principal = 1 ORDER BY id DESC LIMIT 1").get(cliente_id);
        if (e) {
          const parts = [e.logradouro, e.numero, e.bairro, e.cidade, e.uf].filter(Boolean);
          cliente_endereco_eff = parts.join(' - ') || null;
        }
      }
    }

    db.prepare(`
      UPDATE pedidos SET
        cliente_id = ?,
        cliente_nome = COALESCE(?, cliente_nome),
        cliente_contato = COALESCE(?, cliente_contato),
        cliente_nome_avulso = CASE WHEN ? IS NULL THEN cliente_nome_avulso ELSE NULL END,
        cliente_telefone_avulso = CASE WHEN ? IS NULL THEN cliente_telefone_avulso ELSE NULL END,
        cliente_codigo = ?,
        cliente_cnpjcpf = ?,
        cliente_endereco = ?,
        atualizado_em = ?
      WHERE id = ?
    `).run(
      cliente_id || null,
      cliente_nome_eff,
      cliente_contato_eff,
      cliente_id || null,
      cliente_id || null,
      cliente_codigo_eff,
      cliente_cnpjcpf_eff,
      cliente_endereco_eff,
      new Date().toISOString(),
      id
    );

    return res.redirect(`/pedidos/${id}`);
  } catch (e) {
    console.error(e);
    return res.status(400).send(`Falha ao vincular cliente: ${e.message}`);
  }
});
app.get("/pedidos/:id/editar", requireModule("pedidos"), (req, res) => {
  try {
    const id = Number(req.params.id);
    const pedido = db.prepare(`SELECT * FROM pedidos WHERE id = ?`).get(id);
    if (!pedido) return res.status(404).send("Pedido não encontrado.");
    const itens = db.prepare(`SELECT * FROM pedido_itens WHERE pedido_id = ? ORDER BY id ASC`).all(id);

    const clientes = loadClientesSelect(500);
  const catalogoProdutos = db.prepare("SELECT id, nome, sku, unidade, preco_venda, custo_unit, largura_mm, altura_mm FROM catalogo_produtos WHERE ativo=1 ORDER BY nome ASC").all();

      res.render("layout", {
      title: `Editar Pedido #${id}`,
      view: "pedido-form-page",
      pedido,
      itens,
      clientes,
      catalogoProdutos,
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

    // trava edição após faturado (exceto admin)
    const u = req.session?.user;
    if (String(pedidoAtual.status||'').toUpperCase() === 'FATURADO' && String(u?.role||'').toLowerCase() !== 'admin') {
      return res.status(403).send("Pedido FATURADO não pode ser alterado.");
    }

    const cliente_nome = String(req.body.cliente_nome || "").trim();
    const cliente_contato = String(req.body.cliente_contato || "").trim();
    const cliente_id = (req.body.cliente_id !== undefined && String(req.body.cliente_id).trim() !== '') ? Number(req.body.cliente_id) : null;
    const cliente_nome_avulso = String(req.body.cliente_nome_avulso || '').trim();
    const cliente_telefone_avulso = String(req.body.cliente_telefone_avulso || '').trim();

    // Se veio cliente_id, puxa dados do cadastro e preenche campos de exibição
    let cliente_nome_eff = cliente_nome;
    let cliente_contato_eff = cliente_contato;
    let cliente_codigo_eff = null;
    let cliente_cnpjcpf_eff = null;
    let cliente_endereco_eff = null;
    if (cliente_id) {
      const c = db.prepare('SELECT * FROM clientes WHERE id = ?').get(cliente_id);
      if (c) {
        cliente_nome_eff = (String(c.tipo||'').toUpperCase()==='PJ') ? (c.razao_social || c.nome || '') : (c.nome || c.razao_social || '');
        cliente_contato_eff = c.telefone || c.whatsapp || c.contato || cliente_contato_eff;
        cliente_codigo_eff = c.codigo || null;
        cliente_cnpjcpf_eff = c.cpf_cnpj || c.cnpjcpf || null;
        const e = db.prepare("SELECT * FROM cliente_enderecos WHERE cliente_id = ? AND principal = 1 ORDER BY id DESC LIMIT 1").get(cliente_id);
        if (e) {
          const parts = [e.logradouro, e.numero, e.bairro, e.cidade, e.uf].filter(Boolean);
          cliente_endereco_eff = parts.join(' - ') || null;
        }
      }
    } else {
      // avulso
      if (cliente_nome_avulso) cliente_nome_eff = cliente_nome_avulso;
      if (cliente_telefone_avulso) cliente_contato_eff = cliente_telefone_avulso;
      if (!cliente_nome_eff) cliente_nome_eff = 'CONSUMIDOR FINAL';
    }

    const vendedor_nome = String(req.body.vendedor_nome || "").trim();
    const prazo_entrega = String(req.body.prazo_entrega || "").trim();
    const data_venda = String(req.body.data_venda || "").trim();
    const data_validade = String(req.body.data_validade || "").trim();
    const canal_venda = String(req.body.canal_venda || "").trim();
    const tipo_venda = String(req.body.tipo_venda || "PRODUTO").trim() || "PRODUTO";
    const tipo_entrega = String(req.body.tipo_entrega || "").trim();
    const endereco_entrega_texto = String(req.body.endereco_entrega_texto || "").trim();
    const endereco_entrega_id = (req.body.endereco_entrega_id !== undefined && String(req.body.endereco_entrega_id).trim() !== '') ? Number(req.body.endereco_entrega_id) : null;

    const bling_numero = String(req.body.bling_numero || "").trim();
    const bling_pedido_compra = String(req.body.bling_pedido_compra || "").trim();
    const prioridade = String(req.body.prioridade || "NORMAL").trim() || "NORMAL";
    const status = String(req.body.status || pedidoAtual.status || "RASCUNHO").trim() || "RASCUNHO";

    const desconto_tipo = String(req.body.desconto_tipo || "VALOR").trim() || "VALOR";
    const desconto_valor = num(req.body.desconto_valor);
    const frete_valor = num(req.body.frete_valor);

    const natureza_operacao = String(req.body.natureza_operacao || "").trim();
    const consumidor_final = (String(req.body.consumidor_final || '1') === '1') ? 1 : 0;
    const presenca_comprador = String(req.body.presenca_comprador || "").trim();
    const observacao_fiscal = String(req.body.observacao_fiscal || "").trim();

    const observacoes_vendedor = String(req.body.observacoes_vendedor || "").trim();
    const observacoes_internas = String(req.body.observacoes_internas || "").trim();
    const anexo_arquivo = req.file ? req.file.filename : (pedidoAtual.anexo_arquivo || null);

    // Itens
    const descs = arrify(req.body.item_descricao).map(x => String(x||'').trim());
    const qtds  = arrify(req.body.item_quantidade);
    const unis  = arrify(req.body.item_unidade);
    const obsis = arrify(req.body.item_observacao);
    const largs = arrify(req.body.item_largura);
    const alts  = arrify(req.body.item_altura);
    const udims = arrify(req.body.item_unidade_dim);
    const precs = arrify(req.body.item_preco_unit);
    const descItem = arrify(req.body.item_desconto);
    const pids = arrify(req.body.item_produto_id);

    const itensNorm = [];
    for (let i = 0; i < descs.length; i++) {
      const d = descs[i];
      if (!d) continue;
      const q = num(qtds[i]);
      const u = String(unis[i] || '').trim() || 'UN';
      const o = String(obsis[i] || '').trim();
      const largura = num(largs[i]);
      const altura = num(alts[i]);
      const unidade_dim = String(udims[i] || 'MM').toUpperCase();
      const area_m2 = calcAreaM2(largura, altura, unidade_dim);
      const preco_unit = num(precs[i]);
      const desconto_item = num(descItem[i]);
      const total_item = (q * preco_unit) - desconto_item;

      const pidRaw = (pids && pids[i] !== undefined) ? String(pids[i]).trim() : "";
      const produto_id = pidRaw ? Number(pidRaw) : null;
      itensNorm.push({ produto_id, descricao: d, quantidade: q, unidade: u, observacao: o, largura, altura, unidade_dim, area_m2, preco_unit, desconto_item, total_item });
    }

    const totals = recalcPedidoTotals(itensNorm, desconto_tipo, desconto_valor, frete_valor);
    const now = new Date().toISOString();

    const tx = db.transaction(() => {
      db.prepare(`
        UPDATE pedidos SET
          cliente_id = ?,
          cliente_nome = ?,
          cliente_contato = ?,
          cliente_nome_avulso = ?,
          cliente_telefone_avulso = ?,

          vendedor_nome = ?,
          prazo_entrega = ?,
          data_venda = ?,
          data_validade = ?,
          canal_venda = ?,
          tipo_venda = ?,
          tipo_entrega = ?,
          endereco_entrega_texto = ?,
          endereco_entrega_id = ?,

          prioridade = ?,
          status = ?,

          subtotal_itens = ?,
          desconto_tipo = ?,
          desconto_valor = ?,
          frete_valor = ?,
          total = ?,

          natureza_operacao = ?,
          consumidor_final = ?,
          presenca_comprador = ?,
          observacao_fiscal = ?,

          observacoes_vendedor = ?,
          observacoes_internas = ?,
          bling_numero = ?,
          bling_pedido_compra = ?,
          cliente_codigo = ?,
          cliente_cnpjcpf = ?,
          cliente_endereco = ?,
          anexo_arquivo = ?,
          atualizado_em = ?
        WHERE id = ?
      `).run(
        cliente_id || null,
        (cliente_nome_eff || null),
        (cliente_contato_eff || null),
        (cliente_id ? null : (cliente_nome_avulso || null)),
        (cliente_id ? null : (cliente_telefone_avulso || null)),

        vendedor_nome || null,
        prazo_entrega || null,
        data_venda || null,
        data_validade || null,
        canal_venda || null,
        tipo_venda,
        tipo_entrega || null,
        endereco_entrega_texto || null,
        endereco_entrega_id || null,

        prioridade,
        status,

        totals.subtotal_itens,
        totals.desconto_tipo,
        totals.desconto_valor,
        totals.frete_valor,
        totals.total,

        natureza_operacao || null,
        consumidor_final,
        presenca_comprador || null,
        observacao_fiscal || null,

        observacoes_vendedor || null,
        observacoes_internas || null,
        bling_numero || null,
        bling_pedido_compra || null,
        cliente_codigo_eff,
        cliente_cnpjcpf_eff,
        cliente_endereco_eff,
        anexo_arquivo,
        now,
        id
      );

      db.prepare(`DELETE FROM pedido_itens WHERE pedido_id = ?`).run(id);
      for (const it of itensNorm) {
        db.prepare(`
          INSERT INTO pedido_itens (pedido_id, produto_id, descricao, quantidade, unidade, observacao, largura, altura, unidade_dim, area_m2, preco_unit, desconto_item, total_item)
          VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        `).run(
          id,
          it.descricao,
          it.quantidade || null,
          it.unidade || null,
          it.observacao || null,
          it.largura || null,
          it.altura || null,
          it.unidade_dim || 'MM',
          it.area_m2 || 0,
          it.preco_unit || 0,
          it.desconto_item || 0,
          it.total_item || 0
        );
      }
    });

    tx();
    res.redirect(`/pedidos/${id}`);
  } catch (err) {
    console.error("Erro ao atualizar pedido:", err);
    res.status(500).send(err.message);
  }
});

// Atualiza status do pedido (workflow)
app.post("/pedidos/:id/status", requireModule("pedidos"), (req, res) => {
  try {
    const id = Number(req.params.id);
    const novo = String(req.body.status || '').toUpperCase();
    if (!PEDIDO_STATUS.includes(novo)) return res.status(400).send("Status inválido.");
    const now = new Date().toISOString();
    db.prepare("UPDATE pedidos SET status = ?, atualizado_em = ? WHERE id = ?").run(novo, now, id);
    res.redirect(`/pedidos/${id}`);
  } catch (e) {
    console.error(e);
    res.status(500).send(e.message);
  }
});

// Pagamentos do pedido
app.post("/pedidos/:id/pagamentos", requireModule("pedidos"), (req, res) => {
  try {
    const id = Number(req.params.id);
    const forma = String(req.body.forma || '').trim() || 'OUTROS';
    const parcelas = Math.max(1, Number(req.body.parcelas || 1) || 1);
    const valor = num(req.body.valor);
    const data_prevista = String(req.body.data_prevista || '').trim();
    const obs = String(req.body.obs || '').trim();
    if (!valor || valor <= 0) return res.status(400).send("Valor inválido.");
    db.prepare(`INSERT INTO pedido_pagamentos (pedido_id, forma, parcelas, valor, data_prevista, obs) VALUES (?, ?, ?, ?, ?, ?)`)
      .run(id, forma, parcelas, valor, data_prevista || null, obs || null);
    res.redirect(`/pedidos/${id}#pagamentos`);
  } catch (e) {
    console.error(e);
    res.status(500).send(e.message);
  }
});

app.post("/pedidos/:id/pagamentos/:pid/remover", requireModule("pedidos"), (req, res) => {
  try {
    const id = Number(req.params.id);
    const pid = Number(req.params.pid);
    db.prepare("DELETE FROM pedido_pagamentos WHERE id = ? AND pedido_id = ?").run(pid, id);
    res.redirect(`/pedidos/${id}#pagamentos`);
  } catch (e) {
    console.error(e);
    res.status(500).send(e.message);
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

    const linhaPedido = `Pedido #${id}${pedido.bling_numero ? ` (Bling: ${pedido.bling_numero})` : ""}${pedido.vendedor_nome ? ` (Vendedor: ${pedido.vendedor_nome})` : ""}`;
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
      (pedido.bling_numero ? `BLING-${pedido.bling_numero}` : `PED-${id}`),
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

// Copiar itens do pedido (Bling/ML/Shopee) para a OP (texto livre)
try {
  (itens || []).forEach((it) => {
    db.prepare(`
      INSERT INTO ordem_itens_pedido (ordem_id, descricao, quantidade, unidade, observacao)
      VALUES (?, ?, ?, ?, ?)
    `).run(
      op_id,
      (it.descricao || "").trim() || "(sem descrição)",
      (it.quantidade != null ? it.quantidade : 1),
      (it.unidade || "").trim() || null,
      (it.observacao || "").trim() || null
    );
  });
} catch (e) {
  console.warn("Aviso: falha ao copiar itens do pedido para a OP:", e.message);
}


    db.prepare(`UPDATE pedidos SET status = 'EM_PRODUCAO', op_id = ?, atualizado_em = ? WHERE id = ?`)
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
      (SELECT COALESCE(SUM(os.quantidade * os.preco_unit),0) FROM op_servicos os WHERE os.ordem_id = op.id) as total_servicos,
      (SELECT COALESCE(SUM(os.quantidade * os.preco_unit),0) FROM op_servicos os WHERE os.ordem_id = op.id) as total_servicos,
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

// Enviar OP para o Arquivo (apenas ENTREGUE/CANCELADA)
app.post("/ops/:id(\\d+)/arquivar", (req, res) => {
  const id = Number(req.params.id);
  const op = db.prepare("SELECT id, status, arquivada FROM ordens_producao WHERE id = ?").get(id);
  if (!op) return res.redirect("/ops");

  if (Number(op.arquivada || 0) === 1) {
    return res.redirect(`/ops/arquivadas?ok=${encodeURIComponent("OP já estava arquivada.")}`);
  }

  const st = String(op.status || "").toUpperCase();
  if (st !== "ENTREGUE" && st !== "CANCELADA") {
    return res.redirect(`/ops/${id}?erro=${encodeURIComponent("Apenas OPs ENTREGUES ou CANCELADAS podem ser arquivadas.")}`);
  }

  // Se for ENTREGUE, exige checklist 100% antes de arquivar
  if (st === "ENTREGUE") {
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

  // captura anexos para remover do disco depois (compat: filename/caminho)
  const anexos = db.prepare("SELECT * FROM op_anexos WHERE ordem_id = ?").all(id);

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
          const stored = a && (a.filename || a.caminho || a.path) ? String(a.filename || a.caminho || a.path) : "";
          if (!stored) continue;
          const fp = path.join(uploadsDir, stored);
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
        const stored = (a && (a.filename || a.caminho || a.path)) ? String(a.filename || a.caminho || a.path) : "";
        if (!stored) continue;
        const fp = path.join(uploadsDir, stored);
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

  // Obs: aqui é tela de NOVA OP (não existe id ainda), então não carrega itens do pedido.
    let brindesCatalogo = [];
  try {
    brindesCatalogo = db.prepare("SELECT id, nome, unidade, estoque_atual, estoque_minimo, ativo FROM brindes WHERE ativo=1 ORDER BY nome ASC").all();
  } catch (e) {
    brindesCatalogo = [];
  }
  
  let materiaisChapas = [];
  try {
    const produtos = db.prepare("SELECT * FROM produtos ORDER BY descricao ASC").all();
    materiaisChapas = (produtos || []).map(p => {
      const areaPorChapa = areaM2FromMm(p.largura_mm, p.altura_mm);
      const estoqueM2 = Number(p.estoque_atual || 0) * areaPorChapa;
      return { ...p, areaPorChapa, estoqueM2 };
    });
  } catch (e) {
    materiaisChapas = [];
  }
  res.render("layout", { title: "Nova OP", view: "ops-nova-page", insumos, servicosCatalogo, brindesCatalogo, materiaisChapas });
});
app.post("/ops", (req, res) => {
  const { cliente, prioridade, data_entrega, observacao_interna, observacao_cliente, produto_final, quantidade_final, pedido_venda } = req.body;
  const observacao = (observacao_interna || "").toString(); // compat: campo antigo "observacao"
  const codigo_op = gerarCodigoOP();

// Peças (nome + medida + quantidade) vindas do formulário
const pNomesRaw = req.body.peca_nome || req.body.pecas_nome || [];
const pMedidasRaw = req.body.peca_medidas || req.body.pecas_medidas || req.body.peca_medida || req.body.pecas_medida || [];
const pQtdRaw = req.body.peca_qtd || req.body.pecas_qtd || req.body.peca_quantidade || req.body.pecas_quantidade || [];
const pMatRaw = req.body.peca_material_id || req.body.pecas_material_id || [];

const pNomes = Array.isArray(pNomesRaw) ? pNomesRaw : [pNomesRaw];
const pMedidas = Array.isArray(pMedidasRaw) ? pMedidasRaw : [pMedidasRaw];
const pQtds = Array.isArray(pQtdRaw) ? pQtdRaw : [pQtdRaw];
const pMats = Array.isArray(pMatRaw) ? pMatRaw : [pMatRaw];

const pecas = [];
const pLen = Math.max(pNomes.length, pMedidas.length, pQtds.length);
for (let i = 0; i < pLen; i++) {
  const nome = (pNomes[i] ?? "").toString().trim();
  const medida = (pMedidas[i] ?? "").toString().trim();
  const matIdRaw = (pMats[i] ?? "").toString().trim();
  const material_id = matIdRaw ? Number(matIdRaw) : null;

  let quantidade = Number(pQtds[i] ?? 1);
  if (!Number.isFinite(quantidade) || quantidade <= 0) quantidade = 1;
  quantidade = Math.floor(quantidade);

  const dim = parseMedidaMm(medida);
  const largura_mm = dim ? dim.largura_mm : null;
  const altura_mm = dim ? dim.altura_mm : null;
  const area_m2 = dim ? (areaM2FromMm(largura_mm, altura_mm) * quantidade) : null;

  if (nome || medida || material_id) {
    pecas.push({ nome: nome || null, medidas: medida || null, quantidade, material_id, largura_mm, altura_mm, area_m2 });
  }
}
const pecas_json = pecas.length ? JSON.stringify(pecas) : null;



  const info = db.prepare(`
    INSERT INTO ordens_producao
      (codigo_op, cliente, prioridade, produto_final, quantidade_final, pedido_venda, data_abertura, data_entrega, observacao, observacao_interna, observacao_cliente, material_espessura_mm, material_cor, pecas_json)
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
     null,
    null,
    pecas_json
  );

  const opId = info.lastInsertRowid;

  // Quantidade base da OP (para consumo por peça)
  // Preferência: quantidade_final; fallback: soma das peças; fallback: 1
  let opQtdBase = Number(quantidade_final || 0);
  if (!Number.isFinite(opQtdBase) || opQtdBase <= 0) {
    opQtdBase = (pecas || []).reduce((acc, p) => acc + (Number(p?.quantidade || 0) || 0), 0);
  }
  if (!Number.isFinite(opQtdBase) || opQtdBase <= 0) opQtdBase = 1;
  opQtdBase = Math.floor(opQtdBase);

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
      const qtdPorPecaInformada = Number(qtds[i] || 0);
      if (!insumo_id || !qtdPorPecaInformada || qtdPorPecaInformada <= 0) continue;

      // Detecta se o insumo é linear em metros (estoque em m, consumo informado em cm/peça)
      const insumo = db.prepare("SELECT id, unidade FROM insumos WHERE id=?").get(insumo_id);
      const unidade = String(insumo?.unidade || "").trim().toLowerCase();
      const isMetro = unidade === "m" || unidade === "ml";

      // Consumo total (na unidade do estoque):
      // - Se estoque em metros, o usuário digita cm/peça -> converte para m
      // - Caso contrário, considera a unidade do insumo (un, rolo, ml etc.) por peça
      const qtdTotal = isMetro
        ? (qtdPorPecaInformada * opQtdBase) / 100.0
        : (qtdPorPecaInformada * opQtdBase);

      if (!Number.isFinite(qtdTotal) || qtdTotal <= 0) continue;

      // registra vínculo OP x Insumo (guarda por peça e total)
      db.prepare(
        "INSERT INTO op_insumos (ordem_id, insumo_id, quantidade, qtd_por_peca, qtd_total) VALUES (?, ?, ?, ?, ?)"
      ).run(opId, insumo_id, qtdTotal, qtdPorPecaInformada, qtdTotal);

      // registra movimentação e baixa estoque
      db.prepare(
        "INSERT INTO insumos_movimentacoes (insumo_id, tipo, quantidade, motivo) VALUES (?, 'saida', ?, ?)"
      ).run(insumo_id, qtdTotal, `OP ${codigo_op}`);

      db.prepare(
        "UPDATE insumos SET estoque_atual = COALESCE(estoque_atual,0) - ? WHERE id=?"
      ).run(qtdTotal, insumo_id);
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
  pecas = (pecas || []).map(p => ({
    nome: p?.nome || null,
    medidas: p?.medidas || p?.medida || null,
    quantidade: Math.max(1, Math.floor(Number(p?.quantidade ?? p?.qtd ?? 1) || 1)),
    material_id: p?.material_id ? Number(p.material_id) : null,
    largura_mm: p?.largura_mm ? Number(p.largura_mm) : null,
    altura_mm: p?.altura_mm ? Number(p.altura_mm) : null,
    area_m2: (p?.area_m2 != null) ? Number(p.area_m2) : null
  }));
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
      SELECT oi.id, oi.quantidade, oi.qtd_por_peca, oi.qtd_total, i.id as insumo_id, i.nome, i.unidade
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
      SELECT os.id, os.servico_id, os.quantidade, os.preco_unit, os.observacao,
             s.nome, s.unidade
      FROM op_servicos os
      JOIN servicos s ON s.id = os.servico_id
      WHERE os.ordem_id = ?
      ORDER BY os.id DESC
    `).all(id);
  } catch (e) {
    servicosUsados = [];
  }

  // Totais (Nível 3)
  const totalServicos = (servicosUsados || []).reduce((acc, r) => acc + (Number(r.preco_unit||0) * Number(r.quantidade||0)), 0);
  // Por enquanto, o custo total da OP considera serviços (mão de obra).
  // Materiais/insumos podem entrar depois quando houver custo unitário cadastrado.
  const custoTotalOP = totalServicos;

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

  let servicosCatalogo = [];
  try {
    servicosCatalogo = db.prepare("SELECT id, nome, unidade, preco_unit, ativo FROM servicos WHERE ativo=1 ORDER BY nome ASC").all();
  } catch (e) {
    servicosCatalogo = [];
  }


  // Materiais (chapas) para seleção nas peças (baixa por m²)
  let materiaisChapas = [];
  try {
    const mats = db.prepare("SELECT * FROM produtos ORDER BY descricao ASC").all();
    materiaisChapas = (mats || []).map(p => {
      const areaPorChapa = areaM2FromMm(p.largura_mm, p.altura_mm);
      const estoqueM2 = Number(p.estoque_atual || 0) * areaPorChapa;
      return { ...p, areaPorChapa, estoqueM2 };
    });
  } catch (e) { materiaisChapas = []; }

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
    brindesUsados,
    brindesCatalogo,
    checklist: checklistOrdenado,
    checklistConcluidos,
    checklistTotal,
    assinaturaChecklist,
    totalServicos,
    custoTotalOP,
    pecas,
    materiaisChapas
  });
});

// Atualizar material e peças da OP
        app.post("/ops/:id(\\d+)/material", requireOpEditable, (req, res) => {
          const id = Number(req.params.id);
          const op = db.prepare("SELECT id, codigo_op FROM ordens_producao WHERE id = ?").get(id);
          if (!op) return res.status(404).send("OP não encontrada");

                    const pNomesRaw = req.body.peca_nome || [];
          const pMedidasRaw = req.body.peca_medidas || [];
          const pQtdRaw = req.body.peca_qtd || req.body.peca_quantidade || [];
          const pMatRaw = req.body.peca_material_id || [];

          const pNomes = Array.isArray(pNomesRaw) ? pNomesRaw : [pNomesRaw];
          const pMedidas = Array.isArray(pMedidasRaw) ? pMedidasRaw : [pMedidasRaw];
          const pQtds = Array.isArray(pQtdRaw) ? pQtdRaw : [pQtdRaw];
const pMats = Array.isArray(pMatRaw) ? pMatRaw : [pMatRaw];

          const pecas = [];
          for (let i = 0; i < Math.max(pNomes.length, pMedidas.length, pQtds.length, pMats.length); i++) {
            const nome = String(pNomes[i] || "").trim();
            const medidas = String(pMedidas[i] || "").trim();
            let quantidade = Number(pQtds[i] ?? 1);
            if (!Number.isFinite(quantidade) || quantidade <= 0) quantidade = 1;
            quantidade = Math.floor(quantidade);

            const matIdRaw = String(pMats[i] || "").trim();
            const material_id = matIdRaw ? Number(matIdRaw) : null;

            if (!nome && !medidas && !material_id) continue;

            const dim = parseMedidaMm(medidas);
            const largura_mm = dim ? dim.largura_mm : null;
            const altura_mm = dim ? dim.altura_mm : null;
            const area_m2 = dim ? (areaM2FromMm(largura_mm, altura_mm) * quantidade) : null;

            pecas.push({ nome: nome || "Peça", medidas, quantidade, material_id, largura_mm, altura_mm, area_m2 });
          }

          const pecas_json = pecas.length ? JSON.stringify(pecas) : null;

          db.prepare("UPDATE ordens_producao SET pecas_json=? WHERE id=?").run(pecas_json, id);

          res.redirect(`/ops/${id}`);
        });


// Atualizar brindes vinculados na OP (não dá baixa aqui)
app.post("/ops/:id(\\d+)/brindes", requireOpEditable, (req, res) => {
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


// Salvar serviços usados na OP
app.post("/ops/:id(\\d+)/servicos", requireOpEditable, (req, res) => {
  const opId = Number(req.params.id);
  const op = db.prepare("SELECT id, codigo_op, status FROM ordens_producao WHERE id = ?").get(opId);
  if (!op) return res.status(404).send("OP não encontrada");

  const sIdsRaw = req.body.servico_id || [];
  const sQtdsRaw = req.body.servico_qtd || [];
  const sPrecosRaw = req.body.servico_preco || [];
  const sObsRaw = req.body.servico_obs || [];

  const sIds = Array.isArray(sIdsRaw) ? sIdsRaw : [sIdsRaw];
  const sQtds = Array.isArray(sQtdsRaw) ? sQtdsRaw : [sQtdsRaw];
  const sPrecos = Array.isArray(sPrecosRaw) ? sPrecosRaw : [sPrecosRaw];
  const sObs = Array.isArray(sObsRaw) ? sObsRaw : [sObsRaw];

  try {
    const del = db.prepare("DELETE FROM op_servicos WHERE ordem_id = ?");
    const ins = db.prepare("INSERT INTO op_servicos (ordem_id, servico_id, quantidade, preco_unit, observacao) VALUES (?,?,?,?,?)");

    const tx = db.transaction(() => {
      del.run(opId);
      const n = Math.max(sIds.length, sQtds.length, sPrecos.length, sObs.length);
      for (let i = 0; i < n; i++) {
        const servicoId = Number(sIds[i] || 0);
        let qtd = Number(sQtds[i] || 0);
        let preco = Number(String(sPrecos[i] || '0').replace(',', '.'));
        const obs = (sObs[i] || '').toString().trim();

        if (!Number.isFinite(qtd) || qtd <= 0) qtd = 0;
        if (!Number.isFinite(preco) || preco < 0) preco = 0;

        if (servicoId > 0 && qtd > 0) {
          ins.run(opId, servicoId, qtd, preco, obs);
        }
      }
    });
    tx();
  } catch (e) {
    return res.redirect(`/ops/${opId}?erro=` + encodeURIComponent("Erro ao salvar serviços: " + (e.message || e)));
  }

  res.redirect(`/ops/${opId}`);
});


// Atualizar observações (interna x cliente)
app.post("/ops/:id(\\d+)/observacoes", requireOpEditable, (req, res) => {
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
app.post("/ops/:id(\\d+)/checklist", requireOpEditable, (req, res) => {
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

app.post("/ops/:id(\\d+)/itens", requireOpEditable, (req, res) => {
  const ordemId = Number(req.params.id);
  const { produto_id, quantidade, observacao } = req.body;

  const pid = Number(produto_id);
  const qtd = Number(quantidade);

  if (!Number.isInteger(pid) || pid <= 0) return res.status(400).send("Produto inválido");
  if (!Number.isFinite(qtd) || qtd <= 0) return res.status(400).send("Quantidade inválida");

  const op = db.prepare("SELECT id FROM ordens_producao WHERE id = ?").get(ordemId);
  if (!op) return res.status(404).send("OP não encontrada");

  db.prepare(`
    INSERT INTO ordem_itens (ordem_id, produto_id, quantidade, observacao)
    VALUES (?, ?, ?, ?)
  `).run(ordemId, pid, qtd, (observacao || "").trim() || null);

  res.redirect(`/ops/${ordemId}`);
});

app.post("/ops/:id(\\d+)/itens/:itemId/remover", requireOpEditable, (req, res) => {
  const ordemId = Number(req.params.id);
  const itemId = Number(req.params.itemId);
  db.prepare("DELETE FROM ordem_itens WHERE id = ? AND ordem_id = ?").run(itemId, ordemId);
  res.redirect(`/ops/${ordemId}`);
});

app.post("/ops/:id(\\d+)/anexos", requireOpEditable, (req, res) => {
  const ordemId = Number(req.params.id);
  const op = db.prepare("SELECT id FROM ordens_producao WHERE id = ?").get(ordemId);
  if (!op) return res.status(404).send("OP não encontrada");

  upload.single("imagem")(req, res, (err) => {
    if (err) {
      const msg = (err.code === "LIMIT_FILE_SIZE") ? "Arquivo muito grande. Limite: 3MB." : err.message;
      return res.status(400).send(msg);
    }
    if (!req.file) return res.status(400).send("Selecione uma imagem.");

    // Compat: a tabela op_anexos historicamente usa (nome, caminho, mime).
    // Alguns bancos antigos não têm as colunas filename/original_name.
    const nome = (req.file.originalname || req.file.filename || "arquivo").toString();
    const caminho = (req.file.filename || "").toString();
    const mime = req.file.mimetype || null;

    try {
      // Versão nova (se existir)
      db.prepare(`
        INSERT INTO op_anexos (ordem_id, filename, original_name, mime)
        VALUES (?, ?, ?, ?)
      `).run(ordemId, caminho, nome, mime);
    } catch (e) {
      // Versão compat (schema padrão deste projeto)
      db.prepare(`
        INSERT INTO op_anexos (ordem_id, nome, caminho, mime)
        VALUES (?, ?, ?, ?)
      `).run(ordemId, nome, caminho, mime);
    }

    res.redirect(`/ops/${ordemId}`);
  });
});

app.post("/ops/:id(\\d+)/anexos/:anexoId/remover", requireOpEditable, (req, res) => {
  const ordemId = Number(req.params.id);
  const anexoId = Number(req.params.anexoId);

  const anexo = db.prepare("SELECT * FROM op_anexos WHERE id = ? AND ordem_id = ?").get(anexoId, ordemId);
  if (anexo) {
    const stored = (anexo && (anexo.filename || anexo.caminho || anexo.path)) ? String(anexo.filename || anexo.caminho || anexo.path) : "";
    const filePath = stored ? path.join(uploadsDir, stored) : null;
    try { if (filePath && fs.existsSync(filePath)) fs.unlinkSync(filePath); } catch (e) {}
    db.prepare("DELETE FROM op_anexos WHERE id = ? AND ordem_id = ?").run(anexoId, ordemId);
  }
  res.redirect(`/ops/${ordemId}`);
});

app.post("/ops/:id(\\d+)/status", (req, res) => {
  const ordemId = Number(req.params.id);
  const { status } = req.body;
  const allowed = ["ABERTA", "EM_PRODUCAO", "AGUARDANDO_MATERIAL", "FINALIZADA", "ENVIADO", "ENTREGUE", "CANCELADA"];
  if (!allowed.includes(status)) return res.status(400).send("Status inválido");


  // Regras de transição:
  // FINALIZADA -> ENVIADO -> ENTREGUE
  const opAtual = db.prepare("SELECT id, status FROM ordens_producao WHERE id = ?").get(ordemId);
  if (!opAtual) return res.status(404).send("OP não encontrada");
  const atual = String(opAtual.status || "").trim().toUpperCase();

  // Travamento: depois de ENVIADO, só permite avançar para ENTREGUE (ou manter ENVIADO).
  // Depois de ENTREGUE, não permite mais mudanças.
  if (atual === "ENTREGUE" && status !== "ENTREGUE") {
    return res.redirect(`/ops/${ordemId}?erro=` + encodeURIComponent("OP já está ENTREGUE e não permite mudança de status."));
  }
  if (atual === "ENVIADO" && !(status === "ENVIADO" || status === "ENTREGUE")) {
    return res.redirect(`/ops/${ordemId}?erro=` + encodeURIComponent("OP já está ENVIADA e não permite voltar status."));
  }

  if (status === "ENVIADO" && atual !== "FINALIZADA") {
    return res.redirect(`/ops/${ordemId}?erro=` + encodeURIComponent("Só é possível marcar como ENVIADO após FINALIZADA."));
  }

  if (status === "ENTREGUE" && atual !== "ENVIADO") {
    return res.redirect(`/ops/${ordemId}?erro=` + encodeURIComponent("Só é possível marcar como ENTREGUE após ENVIADO."));
  }


  // Se for FINALIZADA, baixar brindes (uma única vez)
  if (status === "FINALIZADA") {
    const op = db.prepare("SELECT id, brindes_baixados, materiais_baixados, pecas_json FROM ordens_producao WHERE id = ?").get(ordemId);
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

  
  // Se for FINALIZADA, baixar consumo de chapas por m² (uma única vez)
  if (status === "FINALIZADA") {
    const op2 = db.prepare("SELECT id, materiais_baixados, pecas_json FROM ordens_producao WHERE id = ?").get(ordemId);
    if (op2 && Number(op2.materiais_baixados || 0) === 0) {
      try {
        let pecas = [];
        try {
          pecas = JSON.parse(op2.pecas_json || "[]");
          if (!Array.isArray(pecas)) pecas = [];
        } catch (e) { pecas = []; }

        // soma consumo por material_id (m²)
        const consumoPorMaterial = new Map(); // material_id -> m2
        for (const p of pecas) {
          const material_id = p && p.material_id ? Number(p.material_id) : null;
          const w = p && p.largura_mm ? Number(p.largura_mm) : null;
          const h = p && p.altura_mm ? Number(p.altura_mm) : null;
          const qtd = Math.max(1, Math.floor(Number(p?.quantidade ?? p?.qtd ?? 1) || 1));
          if (!material_id || !w || !h) continue;
          const m2 = areaM2FromMm(w, h) * qtd;
          if (m2 <= 0) continue;
          consumoPorMaterial.set(material_id, (consumoPorMaterial.get(material_id) || 0) + m2);
        }

        if (consumoPorMaterial.size > 0) {
          // valida saldo (em m² e em chapas equivalentes)
          for (const [materialId, consumoM2] of consumoPorMaterial.entries()) {
            const mat = db.prepare("SELECT id, descricao, largura_mm, altura_mm, estoque_atual FROM produtos WHERE id = ?").get(materialId);
            if (!mat) {
              return res.redirect(`/ops/${ordemId}?erro=` + encodeURIComponent("Material selecionado não encontrado no estoque (ID " + materialId + ")."));
            }
            const areaPorChapa = areaM2FromMm(mat.largura_mm, mat.altura_mm);
            if (areaPorChapa <= 0) {
              return res.redirect(`/ops/${ordemId}?erro=` + encodeURIComponent(`Material "${mat.descricao}" está sem dimensões válidas (largura/altura).`));
            }
            const saldoM2 = Number(mat.estoque_atual || 0) * areaPorChapa;
            if (saldoM2 + 1e-9 < consumoM2) {
              return res.redirect(`/ops/${ordemId}?erro=` + encodeURIComponent(`Estoque insuficiente do material "${mat.descricao}". Saldo: ${saldoM2.toFixed(2)} m². Necessário: ${consumoM2.toFixed(2)} m².`));
            }
          }

          const txMat = db.transaction(() => {
            for (const [materialId, consumoM2] of consumoPorMaterial.entries()) {
              const mat = db.prepare("SELECT id, descricao, largura_mm, altura_mm, estoque_atual FROM produtos WHERE id = ?").get(materialId);
              const areaPorChapa = areaM2FromMm(mat.largura_mm, mat.altura_mm);
              const consumoChapasEq = consumoM2 / areaPorChapa;
              const novo = Number(mat.estoque_atual || 0) - consumoChapasEq;

              db.prepare("UPDATE produtos SET estoque_atual = ? WHERE id = ?").run(novo, materialId);

              // registra movimentação (quantidade em chapas equivalentes; observação traz m²)
              db.prepare("INSERT INTO movimentacoes (produto_id, tipo, quantidade, observacao) VALUES (?,?,?,?)")
                .run(materialId, "saida", consumoChapasEq, `Consumo automático por OP FINALIZADA (${consumoM2.toFixed(3)} m²)`);
            }
            db.prepare("UPDATE ordens_producao SET materiais_baixados = 1 WHERE id = ?").run(ordemId);
          });
          txMat();
        } else {
          // nada a baixar
          db.prepare("UPDATE ordens_producao SET materiais_baixados = 1 WHERE id = ?").run(ordemId);
        }
      } catch (e) {
        return res.redirect(`/ops/${ordemId}?erro=` + encodeURIComponent("Erro ao baixar materiais (m²): " + (e.message || e)));
      }
    }
  }

if (status === "ENVIADO") {
    const who = (req.session && req.session.user && req.session.user.nome) ? String(req.session.user.nome) : "";
    db.prepare(
      "UPDATE ordens_producao SET status = ?, enviado_em = COALESCE(enviado_em, datetime('now')), enviado_por = COALESCE(enviado_por, ?) WHERE id = ?"
    ).run(status, who, ordemId);
  } else if (status === "ENTREGUE") {
    const who = (req.session && req.session.user && req.session.user.nome) ? String(req.session.user.nome) : "";
    db.prepare(
      "UPDATE ordens_producao SET status = ?, entregue_em = COALESCE(entregue_em, datetime('now')), entregue_por = COALESCE(entregue_por, ?) WHERE id = ?"
    ).run(status, who, ordemId);
  } else {
    db.prepare("UPDATE ordens_producao SET status = ? WHERE id = ?").run(status, ordemId);
  }
  res.redirect(`/ops/${ordemId}`);
});

// Atualiza a data de entrega (usado pelo arrastar/soltar no calendário)
app.post("/ops/:id(\\d+)/data-entrega", requireOpEditable, (req, res) => {
  try {
    const ordemId = Number(req.params.id);
    const body = req.body || {};
    const data_entrega = String(body.data_entrega || body.dataEntrega || "").slice(0, 10);
    if (!/^\d{4}-\d{2}-\d{2}$/.test(data_entrega)) {
      return res.status(400).json({ error: "Data inválida" });
    }

    const op = db.prepare("SELECT id FROM ordens_producao WHERE id = ?").get(ordemId);
    if (!op) return res.status(404).json({ error: "OP não encontrada" });

    db.prepare("UPDATE ordens_producao SET data_entrega = ? WHERE id = ?").run(data_entrega, ordemId);
    return res.json({ ok: true, data_entrega });
  } catch (e) {
    console.error("data-entrega erro:", e);
    return res.status(500).json({ error: "Falha ao atualizar data" });
  }
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
    pecas = (pecas || []).map(p => ({
    nome: p?.nome || null,
    medidas: p?.medidas || p?.medida || null,
    quantidade: Math.max(1, Math.floor(Number(p?.quantidade ?? p?.qtd ?? 1) || 1)),
    material_id: p?.material_id ? Number(p.material_id) : null,
    largura_mm: p?.largura_mm ? Number(p.largura_mm) : null,
    altura_mm: p?.altura_mm ? Number(p.altura_mm) : null,
    area_m2: (p?.area_m2 != null) ? Number(p.area_m2) : null
  }));
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
      SELECT oi.id, oi.quantidade, oi.qtd_por_peca, oi.qtd_total, i.nome, i.unidade
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
    pecas = (pecas || []).map(p => ({
    nome: p?.nome || null,
    medidas: p?.medidas || p?.medida || null,
    quantidade: Math.max(1, Math.floor(Number(p?.quantidade ?? p?.qtd ?? 1) || 1)),
    material_id: p?.material_id ? Number(p.material_id) : null,
    largura_mm: p?.largura_mm ? Number(p.largura_mm) : null,
    altura_mm: p?.altura_mm ? Number(p.altura_mm) : null,
    area_m2: (p?.area_m2 != null) ? Number(p.area_m2) : null
  }));
  } catch (e) { pecas = []; }

  // Insumos usados
  let insumosUsados = [];
  try {
    insumosUsados = db.prepare(`
      SELECT oi.id, oi.quantidade, oi.qtd_por_peca, oi.qtd_total, i.nome, i.unidade
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

  // ====== A4 (2 páginas) ======
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
    doc.fillColor("#6B7280").font("Helvetica").fontSize(9).text(txt, { width: pageWidth });
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

  doc.fillColor("#6B7280").font("Helvetica").fontSize(9)
    .text("Acrilsoft • A4 (2 páginas)", leftX, headerY + 26, { width: pageWidth, align: "center" });

  doc.moveTo(leftX, headerY + headerH).lineTo(rightX, headerY + headerH).lineWidth(1).stroke("#E5E7EB");
  doc.y = headerY + headerH + 10;

  // Bloco dados (compacto)
  const f = (label, value, x, y, w) => {
    doc.fillColor("#6b7280").font("Helvetica").fontSize(9).text(label, x, y, { width: w });
    doc.fillColor("#111827").font("Helvetica-Bold").fontSize(9).text(value || "-", x, y + 9, { width: w });
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
    doc.fillColor("#6b7280").font("Helvetica").fontSize(9).text("Observação", leftX, doc.y);
    doc.fillColor("#111827").font("Helvetica").fontSize(9).text(op.observacao, { width: pageWidth });
    doc.moveDown(0.4);
  }

  // Material e peças (compacto)
  if (hasSpace(22) && (op.material_espessura_mm || op.material_cor || (pecas && pecas.length))) {
    doc.fillColor("#111827").font("Helvetica-Bold").fontSize(9).text("Material / Peças");
    const matTxt = [
      op.material_espessura_mm ? `${op.material_espessura_mm}mm` : null,
      op.material_cor ? String(op.material_cor) : null
    ].filter(Boolean).join(" • ");
    if (matTxt) doc.fillColor("#111827").font("Helvetica").fontSize(9).text(`Material: ${matTxt}`, { width: pageWidth });

    if (pecas && pecas.length) {
      const maxLines = Math.max(0, Math.floor((bottomY() - doc.y - 10) / 10));
      const show = pecas.slice(0, Math.min(8, maxLines));
      show.forEach((p) => {
        const nome = (p && p.nome) ? String(p.nome) : "Peça";
        const medidas = (p && p.medidas) ? String(p.medidas) : "-";
        doc.fillColor("#111827").font("Helvetica").fontSize(9).text(`• ${nome}: ${medidas}`, { width: pageWidth });
      });
      if (pecas.length > show.length) noteTrunc(`(+ ${pecas.length - show.length} peças)`);
    }
    doc.moveDown(0.2);
  }

  // ===================== Itens =====================
  if (hasSpace(50)) {
    doc.fillColor("#111827").font("Helvetica-Bold").fontSize(9).text("Itens (Chapas / Materiais)");
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
      doc.fillColor("#374151").font("Helvetica-Bold").fontSize(9);
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

    doc.font("Helvetica").fontSize(9).fillColor("#111827");
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

    doc.fillColor("#111827").font("Helvetica-Bold").fontSize(9).text(title);
    doc.moveDown(0.15);

    if (!rows || rows.length === 0) {
      doc.fillColor("#6b7280").font("Helvetica").fontSize(9).text("Nenhum registro.");
      doc.moveDown(0.2);
      return;
    }

    const rowH2 = 12;
    const startX2 = leftX;
    const pageW2 = pageWidth;
    let y2 = doc.y;

    // header
    doc.fillColor("#374151").font("Helvetica-Bold").fontSize(9);
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

    doc.font("Helvetica").fontSize(9).fillColor("#111827");
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
    doc.fillColor("#111827").font("Helvetica-Bold").fontSize(9).text("Checklist final");
    doc.moveDown(0.1);

    const colWc = pageWidth / 2;
    const lineH = 11;

    const availH = bottomY() - doc.y - 30;
    const maxLines = Math.max(1, Math.floor(availH / lineH));
    const maxItems = maxLines * 2; // 2 colunas
    const show = (checklistFinal || []).slice(0, maxItems);

    doc.font("Helvetica").fontSize(9).fillColor("#111827");
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
    doc.fillColor("#111827").font("Helvetica-Bold").fontSize(9).text("Responsável pela liberação");
    doc.moveDown(0.1);
    const ass = assinaturaChecklist;
    if (ass && ass.responsavel) {
      doc.fillColor("#111827").font("Helvetica").fontSize(9).text(`Responsável: ${ass.responsavel}`);
      if (ass.data_assinatura) {
        doc.fillColor("#6b7280").font("Helvetica").fontSize(9).text(`Assinado em: ${ass.data_assinatura}`);
      }
    } else {
      const y = doc.y + 4;
      doc.fillColor("#111827").font("Helvetica").fontSize(9).text("Responsável: ________________________________", leftX, y);
      doc.fillColor("#111827").font("Helvetica").fontSize(9).text("Data: ____/____/______", leftX + (pageWidth*0.62), y);
      doc.y = y + 12;
    }
    doc.moveDown(0.1);
  }

  // ===== Página 2: Layout / Imagens (sempre) =====
  doc.addPage({ size: "A4", margin: 24 });

  const pageWidth2 = doc.page.width - doc.page.margins.left - doc.page.margins.right;
  const leftX2 = doc.page.margins.left;
  const rightX2 = doc.page.width - doc.page.margins.right;

  doc.fillColor("#111827").font("Helvetica-Bold").fontSize(12).text("Layout / Imagens anexadas", leftX2, doc.y, { width: pageWidth2 });
  doc.moveDown(0.4);

  if (!(anexos || []).length) {
    doc.fillColor("#6b7280").font("Helvetica").fontSize(10).text("Sem anexos.", leftX2, doc.y, { width: pageWidth2 });
  } else {
    const imgs = (anexos || []).slice(0, 6);
    const gap = 10;

    const cols = 2;
    const rows = 3;

    const cellW = (pageWidth2 - gap) / 2;
    const availH = (doc.page.height - doc.page.margins.bottom) - doc.y;
    const cellH = Math.max(140, (availH - gap * (rows - 1)) / rows);

    let baseY = doc.y;

    for (let i = 0; i < imgs.length; i++) {
      const a = imgs[i] || {};
      const col = i % cols;
      const row = Math.floor(i / cols);

      const x = leftX2 + col * (cellW + gap);
      const y = baseY + row * (cellH + gap);

      doc.strokeColor("#e5e7eb").roundedRect(x, y, cellW, cellH, 10).stroke();

      // Normaliza caminho (antigos/novos) e busca no uploadsDir
      const storedRaw = (a.filename || a.caminho || a.path) ? String(a.filename || a.caminho || a.path) : "";
      // Em Windows às vezes vem com barra invertida (\\). Normaliza para "/"
      let stored = storedRaw.replace(/\\\\/g, "/");
      if (stored.startsWith("uploads/")) stored = stored.slice("uploads/".length);
      if (stored.startsWith("/uploads/")) stored = stored.slice("/uploads/".length);
      const imgPath = stored ? path.join(uploadsDir, stored) : null;

      try {
        if (imgPath && fs.existsSync(imgPath)) {
          doc.image(imgPath, x + 8, y + 8, { fit: [cellW - 16, cellH - 34], align: "center", valign: "center" });
        }
      } catch (e) {}

      const label = (a.original_name || a.nome || stored || "arquivo").toString();
      doc.fillColor("#6b7280").font("Helvetica").fontSize(9)
        .text(label.slice(0, 60), x + 8, y + cellH - 20, { width: cellW - 16, ellipsis: true });
    }

    if ((anexos || []).length > imgs.length) {
      doc.fillColor("#6b7280").font("Helvetica").fontSize(9)
        .text(`(Mostrando ${imgs.length} de ${(anexos || []).length} anexos)`, leftX2, baseY + (cellH + gap) * rows + 2, { width: pageWidth2 });
    }
  }


  doc.end();
});




/* ===== Estoque Brindes ===== */
// Mantido por compatibilidade (instalações antigas/links salvos)
app.get("/estoque/brindes", (req, res) => res.redirect("/brindes/estoque"));

// Rota oficial do módulo Brindes
app.get("/brindes/estoque", (req, res) => {
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

  res.render("layout", { title: "Brindes - Estoque", view: "brindes-page", brindes, q });
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
    return res.redirect("/brindes/estoque?erro=" + encodeURIComponent("Erro ao salvar brinde: " + (e.message || e)));
  }
  res.redirect("/brindes/estoque");
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
  if (!allowed.includes(tipo)) return res.redirect("/brindes/estoque?erro=" + encodeURIComponent("Tipo inválido"));
  if (!Number.isFinite(qtd)) return res.redirect("/brindes/estoque?erro=" + encodeURIComponent("Quantidade inválida"));

  const brinde = db.prepare("SELECT * FROM brindes WHERE id = ?").get(id);
  if (!brinde) return res.redirect("/brindes/estoque?erro=" + encodeURIComponent("Brinde não encontrado"));

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

  res.redirect("/brindes/estoque");
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

function ensureDefaultBrindePersonalizacoes() {
  // Seeds leves (não atrapalham quem não usa)
  try {
    db.prepare("INSERT OR IGNORE INTO brindes_personalizacao_tipos (nome, cobra_setup, prazo_dias, ativo) VALUES (?,?,?,?)").run("Silk", 1, 5, 1);
    db.prepare("INSERT OR IGNORE INTO brindes_personalizacao_tipos (nome, cobra_setup, prazo_dias, ativo) VALUES (?,?,?,?)").run("Laser", 0, 2, 1);
    db.prepare("INSERT OR IGNORE INTO brindes_personalizacao_tipos (nome, cobra_setup, prazo_dias, ativo) VALUES (?,?,?,?)").run("UV DTF", 0, 3, 1);
    db.prepare("INSERT OR IGNORE INTO brindes_personalizacao_tipos (nome, cobra_setup, prazo_dias, ativo) VALUES (?,?,?,?)").run("Tampografia", 1, 5, 1);
    db.prepare("INSERT OR IGNORE INTO brindes_personalizacao_tipos (nome, cobra_setup, prazo_dias, ativo) VALUES (?,?,?,?)").run("Sublimação", 0, 4, 1);

    // Se já existia antes do campo prazo_dias, garante um padrão (não sobrescreve customização)
    db.prepare("UPDATE brindes_personalizacao_tipos SET prazo_dias = 5 WHERE nome = 'Silk' AND COALESCE(prazo_dias,0)=0").run();
    db.prepare("UPDATE brindes_personalizacao_tipos SET prazo_dias = 2 WHERE nome = 'Laser' AND COALESCE(prazo_dias,0)=0").run();
    db.prepare("UPDATE brindes_personalizacao_tipos SET prazo_dias = 3 WHERE nome = 'UV DTF' AND COALESCE(prazo_dias,0)=0").run();
    db.prepare("UPDATE brindes_personalizacao_tipos SET prazo_dias = 5 WHERE nome = 'Tampografia' AND COALESCE(prazo_dias,0)=0").run();
    db.prepare("UPDATE brindes_personalizacao_tipos SET prazo_dias = 4 WHERE nome = 'Sublimação' AND COALESCE(prazo_dias,0)=0").run();
  } catch (e) {}
}

function getPersonalizacaoSugestao(tipo_id, quantidade) {
  const qtd = Math.max(1, Math.floor(Number(quantidade || 1)));
  const rows = db.prepare(`
    SELECT * FROM brindes_personalizacao_precos
    WHERE tipo_id = ?
    ORDER BY qtd_min ASC
  `).all(tipo_id);
  if (!rows || rows.length === 0) return { custo_unitario: 0, setup_padrao: 0 };
  const found = rows.find(r => qtd >= Number(r.qtd_min || 1) && (r.qtd_max == null || qtd <= Number(r.qtd_max)));
  const r = (found || rows[rows.length - 1]);
  return { custo_unitario: Number(r.custo_unitario || 0), setup_padrao: Number(r.setup_padrao || 0) };
}

// API: sugestão de custo/setup para uma personalização (por quantidade)
app.get("/api/brindes/personalizacoes/sugestao", (req, res) => {
  try {
    ensureDefaultBrindePersonalizacoes();
    const tipoId = Number(req.query.tipo_id || 0);
    const qtd = Math.max(1, Math.floor(Number(req.query.qtd || 1)));
    if (!tipoId) return res.json({ ok: false, error: "tipo_id obrigatório" });

    const tipo = db.prepare("SELECT id, nome, cobra_setup, COALESCE(prazo_dias,0) AS prazo_dias, ativo FROM brindes_personalizacao_tipos WHERE id = ?").get(tipoId);
    if (!tipo || Number(tipo.ativo || 0) !== 1) return res.json({ ok: false, error: "Tipo inválido" });

    const sug = getPersonalizacaoSugestao(tipoId, qtd);
    const custo_unitario = Number(sug.custo_unitario || 0);
    const setup_padrao = Number(tipo.cobra_setup || 0) ? Number(sug.setup_padrao || 0) : 0;

    return res.json({
      ok: true,
      tipo: { id: tipo.id, nome: tipo.nome, cobra_setup: Number(tipo.cobra_setup || 0), prazo_dias: Number(tipo.prazo_dias || 0) },
      qtd,
      custo_unitario,
      setup_padrao
    });
  } catch (e) {
    return res.json({ ok: false, error: String(e && e.message ? e.message : e) });
  }
});

function recalcItemOrcamentoBrindes(item_id) {
  const item = db.prepare("SELECT id, orcamento_id, quantidade, custo_total_base, margem_percent FROM orcamentos_brindes_itens WHERE id = ?").get(item_id);
  if (!item) return;
  const pers = db.prepare("SELECT COALESCE(SUM(total_personalizacao),0) AS total FROM orcamentos_brindes_itens_personalizacoes WHERE item_id = ?").get(item_id);
  const totalPers = Number((pers && pers.total) || 0);
  const custoTotal = Number(item.custo_total_base || 0) + totalPers;
  const margem = Number(item.margem_percent || 0);
  const totalVenda = custoTotal * (1 + (margem / 100));
  const precoUnit = totalVenda / Math.max(1, Number(item.quantidade || 1));
  db.prepare("UPDATE orcamentos_brindes_itens SET total_item = ?, preco_unit_venda = ? WHERE id = ?").run(totalVenda, precoUnit, item_id);
  recalcOrcamentoBrindes(Number(item.orcamento_id));
}

/* ===== Brindes - Personalizações (biblioteca) ===== */

app.get("/brindes/personalizacoes", (req, res) => {
  ensureDefaultBrindePersonalizacoes();
  const tipos = db.prepare("SELECT * FROM brindes_personalizacao_tipos ORDER BY nome ASC").all();
  const precos = db.prepare(`
    SELECT p.*, t.nome AS tipo_nome
    FROM brindes_personalizacao_precos p
    JOIN brindes_personalizacao_tipos t ON t.id = p.tipo_id
    ORDER BY t.nome ASC, p.qtd_min ASC
  `).all();
  res.render("layout", { title: "Brindes - Personalizações", view: "brindes-personalizacoes-page", tipos, precos });
});

/* ===== Brindes - Pacotes de Personalização ===== */

function listPacotesPersonalizacao(onlyAtivos = true) {
  try {
    const where = onlyAtivos ? "WHERE ativo=1" : "";
    return db.prepare(`SELECT id, nome, descricao, ativo, created_at, updated_at FROM brindes_personalizacao_pacotes ${where} ORDER BY nome ASC`).all();
  } catch (_) {
    return [];
  }
}

function getPacotePersonalizacao(pacoteId) {
  const p = db.prepare("SELECT * FROM brindes_personalizacao_pacotes WHERE id=?").get(pacoteId);
  if (!p) return null;
  const itens = db.prepare(`
    SELECT i.*, t.nome AS tipo_nome, t.cobra_setup, COALESCE(t.prazo_dias,0) AS prazo_dias_tipo
    FROM brindes_personalizacao_pacote_itens i
    JOIN brindes_personalizacao_tipos t ON t.id = i.tipo_id
    WHERE i.pacote_id = ?
    ORDER BY i.ordem ASC, i.id ASC
  `).all(pacoteId);
  return { ...p, itens };
}

app.get("/brindes/personalizacoes/pacotes", (req, res) => {
  ensureDefaultBrindePersonalizacoes();
  const pacotes = listPacotesPersonalizacao(false);
  const tipos = db.prepare("SELECT id, nome, ativo FROM brindes_personalizacao_tipos ORDER BY nome ASC").all();
  res.render("layout", {
    title: "Brindes - Pacotes de Personalização",
    view: "brindes-personalizacao-pacotes-page",
    pacotes,
    tipos
  });
});

app.post("/brindes/personalizacoes/pacotes/novo", (req, res) => {
  const { nome, descricao } = req.body;
  try {
    const n = String(nome || "").trim();
    if (!n) return res.redirect("/brindes/personalizacoes/pacotes?erro=" + encodeURIComponent("Nome do pacote é obrigatório"));
    db.prepare("INSERT INTO brindes_personalizacao_pacotes (nome, descricao, ativo) VALUES (?,?,1)").run(
      n,
      (String(descricao || "").trim() || null)
    );
  } catch (e) {
    return res.redirect("/brindes/personalizacoes/pacotes?erro=" + encodeURIComponent(e.message || e));
  }
  res.redirect("/brindes/personalizacoes/pacotes?ok=" + encodeURIComponent("Pacote criado"));
});

app.post("/brindes/personalizacoes/pacotes/:pid(\\d+)/toggle", (req, res) => {
  const pid = Number(req.params.pid);
  try {
    const p = db.prepare("SELECT id, ativo FROM brindes_personalizacao_pacotes WHERE id=?").get(pid);
    if (p) db.prepare("UPDATE brindes_personalizacao_pacotes SET ativo=?, updated_at=datetime('now') WHERE id=?").run(Number(p.ativo || 0) === 1 ? 0 : 1, pid);
  } catch (_) {}
  res.redirect("/brindes/personalizacoes/pacotes");
});

app.post("/brindes/personalizacoes/pacotes/:pid(\\d+)/delete", (req, res) => {
  const pid = Number(req.params.pid);
  try { db.prepare("DELETE FROM brindes_personalizacao_pacotes WHERE id=?").run(pid); } catch (_) {}
  res.redirect("/brindes/personalizacoes/pacotes");
});

app.post("/brindes/personalizacoes/pacotes/:pid(\\d+)/itens/add", (req, res) => {
  const pid = Number(req.params.pid);
  const { tipo_id, cores, posicao, tamanho, prazo_dias_override, setup_override, custo_unit_override } = req.body;
  try {
    const tipoId = Number(tipo_id);
    const tipo = db.prepare("SELECT id FROM brindes_personalizacao_tipos WHERE id=?").get(tipoId);
    if (!tipo) return res.redirect("/brindes/personalizacoes/pacotes?erro=" + encodeURIComponent("Tipo inválido"));
    const maxOrd = db.prepare("SELECT COALESCE(MAX(ordem),0) AS m FROM brindes_personalizacao_pacote_itens WHERE pacote_id=?").get(pid);
    const ordem = Number((maxOrd && maxOrd.m) || 0) + 1;
    db.prepare(`
      INSERT INTO brindes_personalizacao_pacote_itens
      (pacote_id, tipo_id, cores, posicao, tamanho, prazo_dias_override, setup_override, custo_unit_override, ordem)
      VALUES (?,?,?,?,?,?,?,?,?)
    `).run(
      pid,
      tipoId,
      (cores === "" || cores == null) ? null : Math.max(0, Math.floor(Number(cores))),
      (String(posicao || "").trim() || null),
      (String(tamanho || "").trim() || null),
      (prazo_dias_override === "" || prazo_dias_override == null) ? null : Math.max(0, Math.floor(Number(prazo_dias_override))),
      (setup_override === "" || setup_override == null) ? null : Number(setup_override),
      (custo_unit_override === "" || custo_unit_override == null) ? null : Number(custo_unit_override),
      ordem
    );
    db.prepare("UPDATE brindes_personalizacao_pacotes SET updated_at=datetime('now') WHERE id=?").run(pid);
  } catch (e) {
    return res.redirect("/brindes/personalizacoes/pacotes?erro=" + encodeURIComponent(e.message || e));
  }
  res.redirect("/brindes/personalizacoes/pacotes?ok=" + encodeURIComponent("Item adicionado"));
});

app.post("/brindes/personalizacoes/pacotes/:pid(\\d+)/itens/:iid(\\d+)/delete", (req, res) => {
  const pid = Number(req.params.pid);
  const iid = Number(req.params.iid);
  try {
    db.prepare("DELETE FROM brindes_personalizacao_pacote_itens WHERE id=? AND pacote_id=?").run(iid, pid);
    db.prepare("UPDATE brindes_personalizacao_pacotes SET updated_at=datetime('now') WHERE id=?").run(pid);
  } catch (_) {}
  res.redirect("/brindes/personalizacoes/pacotes");
});

app.post("/brindes/personalizacoes/tipos", (req, res) => {
  const { nome, cobra_setup, ativo, prazo_dias } = req.body;
  try {
    db.prepare("INSERT INTO brindes_personalizacao_tipos (nome, cobra_setup, prazo_dias, ativo) VALUES (?,?,?,?)").run(
      (nome || "").trim(),
      cobra_setup === "1" ? 1 : 0,
      Math.max(0, Math.floor(Number(prazo_dias || 0))),
      ativo === "0" ? 0 : 1
    );
  } catch (e) {
    return res.redirect("/brindes/personalizacoes?erro=" + encodeURIComponent(e.message || e));
  }
  res.redirect("/brindes/personalizacoes");
});

app.post("/brindes/personalizacoes/tipos/:id(\\d+)/update", (req, res) => {
  const id = Number(req.params.id);
  const { nome, cobra_setup, prazo_dias } = req.body;
  try {
    db.prepare("UPDATE brindes_personalizacao_tipos SET nome = ?, cobra_setup = ?, prazo_dias = ? WHERE id = ?").run(
      (nome || "").trim(),
      cobra_setup === "1" ? 1 : 0,
      Math.max(0, Math.floor(Number(prazo_dias || 0))),
      id
    );
  } catch (e) {
    return res.redirect("/brindes/personalizacoes?erro=" + encodeURIComponent(e.message || e));
  }
  res.redirect("/brindes/personalizacoes");
});

app.post("/brindes/personalizacoes/tipos/:id(\\d+)/toggle", (req, res) => {
  const id = Number(req.params.id);
  try {
    const t = db.prepare("SELECT id, ativo FROM brindes_personalizacao_tipos WHERE id = ?").get(id);
    if (t) db.prepare("UPDATE brindes_personalizacao_tipos SET ativo = ? WHERE id = ?").run(Number(t.ativo || 0) === 1 ? 0 : 1, id);
  } catch (e) {}
  res.redirect("/brindes/personalizacoes");
});

app.post("/brindes/personalizacoes/precos", (req, res) => {
  const { tipo_id, qtd_min, qtd_max, custo_unitario, setup_padrao } = req.body;
  try {
    db.prepare(`
      INSERT INTO brindes_personalizacao_precos (tipo_id, qtd_min, qtd_max, custo_unitario, setup_padrao, updated_at)
      VALUES (?, ?, ?, ?, ?, datetime('now'))
    `).run(
      Number(tipo_id),
      Math.max(1, Math.floor(Number(qtd_min || 1))),
      (qtd_max === "" || qtd_max == null) ? null : Math.max(1, Math.floor(Number(qtd_max))),
      Number(custo_unitario || 0),
      Number(setup_padrao || 0)
    );
  } catch (e) {
    return res.redirect("/brindes/personalizacoes?erro=" + encodeURIComponent(e.message || e));
  }
  res.redirect("/brindes/personalizacoes");
});

app.post("/brindes/personalizacoes/precos/:id(\\d+)/remover", (req, res) => {
  const id = Number(req.params.id);
  try { db.prepare("DELETE FROM brindes_personalizacao_precos WHERE id = ?").run(id); } catch (e) {}
  res.redirect("/brindes/personalizacoes");
});

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
  // Padrão igual aos Pedidos: cria o cabeçalho primeiro e depois adiciona itens/personalizações na edição.
  ensureDefaultBrindePersonalizacoes();
  const tiposPersonalizacao = db.prepare("SELECT * FROM brindes_personalizacao_tipos WHERE ativo=1 ORDER BY nome ASC").all();

  // Para permitir criar o orçamento já completo (itens + personalizações) na própria tela de novo orçamento.
  const estoqueBrindes = db.prepare(`
    SELECT id, nome, categoria, unidade, estoque_atual, estoque_minimo
    FROM brindes
    WHERE ativo=1
    ORDER BY nome ASC
  `).all();

  const clientes = loadClientesSelect(500);

  res.render("layout", {
    title: "Novo Orçamento (Brindes)",
    view: "brindes-orcamento-form-page",
    modo: "novo",
    orcamento: null,
    clientes,
    tiposPersonalizacao,
    estoqueBrindes
  });
});


app.post("/brindes/orcamentos/novo", uploadPedido.fields([
  { name: "layout_file", maxCount: 1 }
]), (req, res) => {
  const {
    cliente_id,
    status,
    cliente_nome_avulso,
    cliente_whatsapp,
    cliente_email,
    cliente_documento,
    cliente_endereco,
    cliente_cidade,
    cliente_uf,
    vendedor_nome,
    canal_venda,
    validade_em_dias,
    prazo_entrega_texto,
    prazo_base_dias,
    observacoes,
    pagamento_texto,
    frete_texto,
    frete_valor,
    desconto,
    impostos_texto,
    condicoes_comerciais,
    layout_obs
  } = req.body;

  let id = null;
  try {
    const layoutFile = (req.files && req.files["layout_file"] && req.files["layout_file"][0]) ? req.files["layout_file"][0] : null;

    // Cliente cadastrado: se informado, preenche snapshot a partir do cadastro (sem bloquear se falhar)
    const clienteId = Number(cliente_id || 0) || null;

    let clienteNome = (cliente_nome_avulso || "").trim() || null;
    let clienteWhatsapp = (cliente_whatsapp || "").trim() || null;
    let clienteEmail = (cliente_email || "").trim() || null;
    let clienteDocumento = (cliente_documento || "").trim() || null;
    let clienteEndereco = (cliente_endereco || "").trim() || null;
    let clienteCidade = (cliente_cidade || "").trim() || null;
    let clienteUf = (cliente_uf || "").trim() || null;

    if (clienteId) {
      try {
        const c = db.prepare(`
          SELECT
            COALESCE(c.razao_social, c.nome) AS nome,
            COALESCE(c.whatsapp, c.telefone, c.contato) AS whatsapp,
            c.email AS email,
            COALESCE(c.cpf_cnpj, c.cnpjcpf) AS documento,
            e.logradouro, e.numero, e.complemento, e.bairro, e.cidade, e.uf
          FROM clientes c
          LEFT JOIN cliente_enderecos e
            ON e.id = (
              SELECT id
              FROM cliente_enderecos
              WHERE cliente_id = c.id
              ORDER BY principal DESC, id DESC
              LIMIT 1
            )
          WHERE c.id = ?
          LIMIT 1
        `).get(clienteId);

        if (c) {
          clienteNome = c.nome || clienteNome;
          clienteWhatsapp = c.whatsapp || clienteWhatsapp;
          clienteEmail = c.email || clienteEmail;
          clienteDocumento = c.documento || clienteDocumento;

          if (!clienteEndereco) {
            const parts = [];
            if (c.logradouro) parts.push(c.logradouro);
            let numComp = '';
            if (c.numero) numComp += c.numero;
            if (c.complemento) numComp += (numComp ? ' ' : '') + c.complemento;
            if (numComp) parts.push(numComp);
            if (c.bairro) parts.push(c.bairro);
            clienteEndereco = parts.join(', ') || clienteEndereco;
          }

          clienteCidade = c.cidade || clienteCidade;
          clienteUf = c.uf || clienteUf;
        }
      } catch (e) {}
    }


    // Itens + personalizações (opcional) enviados via JSON pela tela "Novo Orçamento"
    let itensPayload = [];
    try {
      itensPayload = req.body.itens_json ? JSON.parse(String(req.body.itens_json)) : [];
      if (!Array.isArray(itensPayload)) itensPayload = [];
    } catch (_) {
      itensPayload = [];
    }

    const r = db.prepare(`
      INSERT INTO orcamentos_brindes
      (cliente_id, cliente_nome, cliente_whatsapp,
       cliente_email, cliente_documento, cliente_endereco, cliente_cidade, cliente_uf,
       vendedor_nome, canal_venda,
       validade_em_dias, prazo_base_dias, prazo_entrega_texto, observacoes,
       pagamento_texto, frete_texto, frete_valor, desconto, impostos_texto, condicoes_comerciais,
       layout_arquivo, layout_nome_original, layout_obs,
       status, created_at)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
    `).run(
      clienteId,
      (clienteNome || "").trim(),
      (clienteWhatsapp || "").trim() || null,
      (clienteEmail || "").trim() || null,
      (clienteDocumento || "").trim() || null,
      (clienteEndereco || "").trim() || null,
      (clienteCidade || "").trim() || null,
      (clienteUf || "").trim() || null,
      (vendedor_nome || "").trim() || null,
      (canal_venda || "").trim() || null,
      Math.max(1, Math.floor(Number(validade_em_dias || 7))),
      Math.max(0, Math.floor(Number(prazo_base_dias || 10))),
      (prazo_entrega_texto || "").trim() || null,
      (observacoes || "").trim() || null,
      (pagamento_texto || "").trim() || null,
      (frete_texto || "").trim() || null,
      Math.max(0, Number(frete_valor || 0)),
      Math.max(0, Number(desconto || 0)),
      (impostos_texto || "").trim() || null,
      (condicoes_comerciais || "").trim() || null,
      layoutFile ? String(layoutFile.filename) : null,
      layoutFile ? String(layoutFile.originalname || layoutFile.filename) : null,
      (layout_obs || "").trim() || null,
      (status || "RASCUNHO").trim() || "RASCUNHO"
    );

    id = Number(r.lastInsertRowid);

    // Se veio a lista de itens, cria já tudo vinculado ao estoque e às personalizações.
    if (itensPayload && itensPayload.length) {
      const tx = db.transaction(() => {
        (itensPayload || []).forEach((it) => {
          const brindeId = Number(it.brinde_id);
          if (!brindeId) return;
          const qtd = Math.max(1, Math.floor(Number(it.quantidade || 1)));
          const margem = Math.max(0, Number(it.margem_percent || 0));

          const cat = getOrCreateCatalogoFromBrindeEstoque(brindeId);
          const catId = Number(cat.id);

          const custoUnit = getCustoUnitByFaixa(catId, qtd);
          const custoTotalBase = custoUnit * qtd;
          const precoTotal = custoTotalBase * (1 + (margem / 100));
          const precoUnit = precoTotal / qtd;

          const rItem = db.prepare(`
            INSERT INTO orcamentos_brindes_itens (
              orcamento_id, catalogo_id, quantidade,
              custo_unit_base, custo_total_base,
              margem_percent, preco_unit_venda, total_item
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
          `).run(
            id, catId, qtd,
            custoUnit, custoTotalBase,
            margem, precoUnit, precoTotal
          );

          const itemId = Number(rItem.lastInsertRowid);
          const pers = Array.isArray(it.personalizacoes) ? it.personalizacoes : [];
          (pers || []).forEach((p) => {
            const tipoId = Number(p.tipo_id);
            if (!tipoId) return;
            const custoUnitPers = Math.max(0, Number(p.custo_unit_personalizacao || 0));
            const setup = Math.max(0, Number(p.setup_valor || 0));
            const totalPers = (custoUnitPers * qtd) + setup;

            db.prepare(`
              INSERT INTO orcamentos_brindes_itens_personalizacoes
              (item_id, tipo_id, cores, posicao, tamanho, prazo_dias, setup_valor, custo_unit_personalizacao, total_personalizacao)
              VALUES (?,?,?,?,?,?,?,?,?)
            `).run(
              itemId,
              tipoId,
              (p.cores === '' || p.cores == null) ? null : String(p.cores),
              (p.posicao === '' || p.posicao == null) ? null : String(p.posicao),
              (p.tamanho === '' || p.tamanho == null) ? null : String(p.tamanho),
              (p.prazo_dias === '' || p.prazo_dias == null) ? null : Number(p.prazo_dias),
              setup,
              custoUnitPers,
              totalPers
            );
          });

          // Recalcula o item com base nas personalizações recém inseridas
          recalcItemOrcamentoBrindes(itemId);
        });
      });
      tx();
    }

    // padrão dos Pedidos: cria cabeçalho e vai para edição para adicionar itens e personalizações
    recalcOrcamentoBrindes(id);
    return res.redirect(`/brindes/orcamentos/${id}`);
  } catch (e) {
    console.error(e);
    const catalogo = db.prepare(`
      SELECT c.id, c.nome, c.codigo_fornecedor, f.nome AS fornecedor_nome
      FROM brindes_catalogo c
      JOIN brindes_fornecedores f ON f.id = c.fornecedor_id
      WHERE c.ativo = 1
      ORDER BY c.nome ASC
    `).all();
    ensureDefaultBrindePersonalizacoes();
    const tiposPersonalizacao = db.prepare("SELECT * FROM brindes_personalizacao_tipos WHERE ativo=1 ORDER BY nome ASC").all();
    const estoqueBrindes = db.prepare(`
      SELECT id, nome, categoria, unidade, estoque_atual, estoque_minimo
      FROM brindes
      WHERE ativo=1
      ORDER BY nome ASC
    `).all();

    return res.render("layout", {
      title: "Novo Orçamento (Brindes)",
      view: "brindes-orcamento-form-page",
      modo: "novo",
      orcamento: req.body || null,
      catalogo,
      tiposPersonalizacao,
      estoqueBrindes,
      erro: "Não foi possível salvar o orçamento. Verifique os campos e tente novamente."
    });
  }
});

// Compat: alguns zips antigos redirecionavam para /editar
app.get("/brindes/orcamentos/:id(\\d+)/editar", (req, res) => {
  return res.redirect(`/brindes/orcamentos/${Number(req.params.id)}`);
});

function ensureFornecedorEstoque() {
  // fornecedor virtual para itens do estoque interno
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

  // Já existe vínculo?
  let cat = null;
  try {
    cat = db.prepare("SELECT * FROM brindes_catalogo WHERE brinde_estoque_id=? LIMIT 1").get(bid);
  } catch (_) {
    // coluna pode não existir em bancos antigos
  }
  if (cat) return cat;

  const f = ensureFornecedorEstoque();
  const codigo = `ESTOQUE-${bid}`;

  // Se já existir por código, reaproveita
  cat = db.prepare("SELECT * FROM brindes_catalogo WHERE fornecedor_id=? AND codigo_fornecedor=?").get(f.id, codigo);
  if (!cat) {
    const r = db.prepare(`
      INSERT INTO brindes_catalogo (fornecedor_id, codigo_fornecedor, nome, descricao, categoria, unidade, ativo, updated_at, brinde_estoque_id)
      VALUES (?, ?, ?, ?, ?, ?, 1, datetime('now'), ?)
    `).run(f.id, codigo, br.nome, null, br.categoria || null, br.unidade || 'UN', bid);
    cat = db.prepare("SELECT * FROM brindes_catalogo WHERE id=?").get(Number(r.lastInsertRowid));
  } else {
    // garante o vínculo
    try { db.prepare("UPDATE brindes_catalogo SET brinde_estoque_id=? WHERE id=?").run(bid, cat.id); } catch (_) {}
  }
  return cat;
}


function recalcOrcamentoBrindes(orcamento_id) {
  const itens = db.prepare("SELECT * FROM orcamentos_brindes_itens WHERE orcamento_id = ?").all(orcamento_id);
  const subtotal = (itens || []).reduce((acc, it) => acc + Number(it.total_item || 0), 0);
  db.prepare("UPDATE orcamentos_brindes SET subtotal = ?, total = ( ? - desconto + COALESCE(frete_valor,0) ) WHERE id = ?").run(subtotal, subtotal, orcamento_id);
}

function calcPrazoOrcamentoBrindes(orcamento_id) {
  const orc = db.prepare("SELECT prazo_base_dias FROM orcamentos_brindes WHERE id = ?").get(orcamento_id);
  const base = Math.max(0, Math.floor(Number((orc && orc.prazo_base_dias) ?? 10)));
  const rows = db.prepare(`
    SELECT item_id, COALESCE(SUM(COALESCE(prazo_dias,0)),0) AS add_dias
    FROM orcamentos_brindes_itens_personalizacoes
    WHERE item_id IN (SELECT id FROM orcamentos_brindes_itens WHERE orcamento_id = ?)
    GROUP BY item_id
  `).all(orcamento_id);
  const maxAdd = (rows || []).reduce((acc, r) => Math.max(acc, Number(r.add_dias || 0)), 0);
  return { base_dias: base, add_personalizacoes_dias: maxAdd, total_dias: base + maxAdd };
}

// =========================
// ORÇAMENTOS (PRODUTOS GERAIS)
// =========================
function recalcOrcamentoProdutos(orcamento_id) {
  const itens = db.prepare("SELECT * FROM orcamentos_produtos_itens WHERE orcamento_id = ?").all(orcamento_id);
  // total_item já é persistido; garantimos consistência caso algum item esteja desatualizado
  for (const it of itens) {
    const qtd = num(it.quantidade);
    const vu = num(it.valor_unitario);
    const desc = num(it.desconto_item);
    const totalItem = (qtd * vu) - desc;
    if (Math.abs(num(it.total_item) - totalItem) > 0.00001) {
      db.prepare("UPDATE orcamentos_produtos_itens SET total_item = ? WHERE id = ?").run(totalItem, it.id);
    }
  }

  const subtotal = itens.reduce((acc, it) => acc + num(it.total_item), 0);
  const orc = db.prepare("SELECT desconto, frete_valor FROM orcamentos_produtos WHERE id = ?").get(orcamento_id) || {};
  const desconto = num(orc.desconto);
  const frete = num(orc.frete_valor);
  const total = subtotal - desconto + frete;
  db.prepare("UPDATE orcamentos_produtos SET subtotal = ?, total = ?, atualizado_em = datetime('now') WHERE id = ?").run(subtotal, total, orcamento_id);
}
// (migrado)

// Impressão / PDF (Produtos) - padronizado com Brindes
app.get("/orcamentos/:id(\\d+)/imprimir", requireModule("pedidos"), (req, res) => {
  const id = Number(req.params.id);
  const isPdf = String(req.query.pdf || "").trim() === "1";

  // garante totais
  try { recalcOrcamentoProdutos(id); } catch (_) {}

  const orcamento = db.prepare(`
    SELECT o.*, 
           COALESCE(c.nome, o.cliente_nome_avulso, '—') AS cliente_nome,
           COALESCE(c.whatsapp, o.cliente_whatsapp) AS cliente_whatsapp_eff,
           COALESCE(c.email, o.cliente_email) AS cliente_email_eff,
           COALESCE(c.cpf_cnpj, o.cliente_documento) AS cliente_documento_eff
    FROM orcamentos_produtos o
    LEFT JOIN clientes c ON c.id = o.cliente_id
    WHERE o.id = ?
  `).get(id);
  if (!orcamento) return res.status(404).send("Orçamento não encontrado");

  // marca como ENVIADO ao imprimir (se estiver rascunho)
  try {
    if ((orcamento.status || "RASCUNHO") === "RASCUNHO") {
      db.prepare("UPDATE orcamentos_produtos SET status='ENVIADO' WHERE id=?").run(id);
      orcamento.status = "ENVIADO";
    }
  } catch (_) {}

  const itens = db.prepare(`
    SELECT i.*, cp.nome AS produto_nome, cp.unidade AS produto_unidade
    FROM orcamentos_produtos_itens i
    LEFT JOIN catalogo_produtos cp ON cp.id = i.produto_id
    WHERE i.orcamento_id = ?
    ORDER BY i.id ASC
  `).all(id);

  const _logoInfo = resolveBrandLogoPath();
  const logoUrl = (_logoInfo && _logoInfo.rel) ? ("/" + _logoInfo.rel.replace(/\\/g, "/")) : (_logoInfo ? "/logo.png" : null);

  return res.render("orcamento-produto-print", { orcamento, itens, logoUrl, isPdf });
});

// Download direto em PDF (Produtos) usando Puppeteer (mesmo layout do /imprimir)
app.get("/orcamentos/:id(\\d+)/baixar-pdf", requireModule("pedidos"), async (req, res) => {
  const id = Number(req.params.id);
  try {
    const orc = db.prepare("SELECT id FROM orcamentos_produtos WHERE id=?").get(id);
    if (!orc) return res.status(404).send("Orçamento não encontrado");

    const baseUrl = getBaseUrlFromReq(req);
    const url = `${baseUrl}/orcamentos/${id}/imprimir?pdf=1`;

    let puppeteer;
    try { puppeteer = require("puppeteer"); }
    catch (e) {
      console.error("Puppeteer não instalado. Rode: npm i puppeteer");
      return res.status(500).send("Dependência ausente: puppeteer");
    }

    const browser = await puppeteer.launch({
      headless: "new",
      args: ["--no-sandbox", "--disable-setuid-sandbox"],
      executablePath: process.env.PUPPETEER_EXECUTABLE_PATH || undefined,
    });

    try {
      const page = await browser.newPage();
      const cookieHeader = req.headers.cookie || "";
      if (cookieHeader) await page.setExtraHTTPHeaders({ cookie: cookieHeader });
      await page.goto(url, { waitUntil: "networkidle0" });
      await page.emulateMediaType("print");

      const pdfBuffer = await page.pdf({
        format: "A4",
        printBackground: true,
        preferCSSPageSize: true,
        margin: { top: "12mm", right: "12mm", bottom: "12mm", left: "12mm" },
      });

      // salva cópia em /uploads e cria link público (se sqlite)
      try {
        const relDir = path.join("pdfs", "orcamentos_produtos");
        const absDir = path.join(uploadsDir, relDir);
        if (!fs.existsSync(absDir)) fs.mkdirSync(absDir, { recursive: true });
        const ts = new Date();
        const pad = (n) => String(n).padStart(2, "0");
        const stamp = `${ts.getFullYear()}${pad(ts.getMonth() + 1)}${pad(ts.getDate())}-${pad(ts.getHours())}${pad(ts.getMinutes())}${pad(ts.getSeconds())}`;
        const storedName = `ORCAMENTO-PRODUTOS-${String(id).padStart(4, "0")}-${stamp}.pdf`;
        const absFile = path.join(absDir, storedName);
        fs.writeFileSync(absFile, pdfBuffer);

        const token = upsertPublicPdfLink({
          kind: "ORCAMENTO_PRODUTOS",
          refId: id,
          fileRel: path.join(relDir, storedName).replace(/\\/g, "/"),
          expiresDays: 7,
        });
        if (token) res.setHeader("X-Public-Pdf-Link", `${baseUrl}/p/${token}`);
      } catch (e) {
        console.warn("Falha ao salvar PDF/link público (produtos):", e?.message || e);
      }

      const filename = `ORCAMENTO-PRODUTOS-${String(id).padStart(4, "0")}.pdf`;
      res.setHeader("Content-Type", "application/pdf");
      res.setHeader("Content-Disposition", `attachment; filename=\"${filename}\"`);
      return res.end(pdfBuffer);
    } finally {
      await browser.close();
    }
  } catch (e) {
    console.error(e);
    if (res.headersSent) return;
    return res.status(500).send("Erro ao gerar PDF");
  }
});

// SaaS: gera (se necessário) o PDF, cria um link público e abre WhatsApp com mensagem pronta. (Produtos)
app.get("/orcamentos/:id(\\d+)/whatsapp", requireModule("pedidos"), async (req, res) => {
  const id = Number(req.params.id);
  try {
    const orc = db.prepare("SELECT * FROM orcamentos_produtos WHERE id = ?").get(id);
    if (!orc) return res.status(404).send("Orçamento não encontrado");

    // tenta obter whatsapp do cadastro do cliente, ou do campo avulso do orçamento
    let waRaw = String(orc.cliente_whatsapp || "");
    try {
      if (orc.cliente_id) {
        const c = db.prepare("SELECT whatsapp FROM clientes WHERE id=?").get(Number(orc.cliente_id));
        if (c && c.whatsapp) waRaw = String(c.whatsapp);
      }
    } catch (_) {}

    const waDigits = String(waRaw || "").replace(/\D/g, "");
    if (waDigits.length < 10) {
      return res.redirect(`/orcamentos/${id}/imprimir?erro=` + encodeURIComponent("WhatsApp do cliente não informado/ inválido."));
    }

    // tenta achar link existente válido
    const baseUrl = getBaseUrlFromReq(req);
    let token = null;
    try {
      if (!IS_PG()) {
        const row = db.prepare(
          "SELECT token, expires_at FROM pdf_public_links WHERE kind='ORCAMENTO_PRODUTOS' AND ref_id=? ORDER BY id DESC LIMIT 1"
        ).get(id);
        if (row && row.token) {
          if (!row.expires_at) token = row.token;
          else {
            const exp = new Date(String(row.expires_at).replace(' ', 'T'));
            if (!isNaN(exp.getTime()) && exp.getTime() > Date.now()) token = row.token;
          }
        }
      }
    } catch (_) {}

    // se não houver token válido, gera o PDF agora (salvando e criando token)
    if (!token) {
      let puppeteer;
      try { puppeteer = require("puppeteer"); } catch (_) { puppeteer = null; }
      if (!puppeteer) {
        return res.redirect(`/orcamentos/${id}/imprimir?erro=` + encodeURIComponent("Dependência ausente: puppeteer"));
      }

      const url = `${baseUrl}/orcamentos/${id}/imprimir?pdf=1`;
      const browser = await puppeteer.launch({
        headless: "new",
        args: ["--no-sandbox", "--disable-setuid-sandbox"],
        executablePath: process.env.PUPPETEER_EXECUTABLE_PATH || undefined,
      });
      try {
        const page = await browser.newPage();
        const cookieHeader = req.headers.cookie || "";
        if (cookieHeader) await page.setExtraHTTPHeaders({ cookie: cookieHeader });
        await page.goto(url, { waitUntil: "networkidle0" });
        await page.emulateMediaType("print");

        const pdfBuffer = await page.pdf({
          format: "A4",
          printBackground: true,
          preferCSSPageSize: true,
          margin: { top: "12mm", right: "12mm", bottom: "12mm", left: "12mm" },
        });

        const relDir = path.join("pdfs", "orcamentos_produtos");
        const absDir = path.join(uploadsDir, relDir);
        if (!fs.existsSync(absDir)) fs.mkdirSync(absDir, { recursive: true });
        const ts = new Date();
        const pad = (n) => String(n).padStart(2, "0");
        const stamp = `${ts.getFullYear()}${pad(ts.getMonth() + 1)}${pad(ts.getDate())}-${pad(ts.getHours())}${pad(ts.getMinutes())}${pad(ts.getSeconds())}`;
        const storedName = `ORCAMENTO-PRODUTOS-${String(id).padStart(4, "0")}-${stamp}.pdf`;
        const absFile = path.join(absDir, storedName);
        fs.writeFileSync(absFile, pdfBuffer);

        token = upsertPublicPdfLink({
          kind: "ORCAMENTO_PRODUTOS",
          refId: id,
          fileRel: path.join(relDir, storedName).replace(/\\/g, "/"),
          expiresDays: 7,
        });
      } finally {
        await browser.close();
      }
    }

    const publicUrl = token ? `${baseUrl}/p/${token}` : `${baseUrl}/orcamentos/${id}/imprimir`;
    const msg = `Olá! Segue o orçamento de produtos #${id}.\n\nPDF (link): ${publicUrl}\n\nQualquer dúvida, me chama aqui.`;
    const waUrl = `https://wa.me/${waDigits}?text=${encodeURIComponent(msg)}`;
    return res.redirect(waUrl);
  } catch (e) {
    console.error(e);
    return res.redirect(`/orcamentos/${id}/imprimir?erro=` + encodeURIComponent("Falha ao preparar WhatsApp"));
  }
});

// (migrado)
// Upload de layout/anexo da proposta (PDF ou imagem)
app.post("/orcamentos/:id(\\d+)/layout", requireModule("pedidos"), uploadPedido.single("layout_file"), (req, res) => {
  try {
    const id = Number(req.params.id);
    const file = req.file;
    if (!file) return res.redirect(`/orcamentos/${id}`);
    const publicPath = `/uploads/${file.filename}`;
    db.prepare("UPDATE orcamentos_produtos SET layout_arquivo = ?, atualizado_em = datetime('now') WHERE id = ?").run(publicPath, id);
    res.redirect(`/orcamentos/${id}`);
  } catch (err) {
    console.error("Erro upload layout orçamento produtos:", err);
    res.status(500).send(err.message);
  }
});


app.get("/brindes/orcamentos/:id(\\d+)", (req, res) => {
  const id = Number(req.params.id);
  const orcamento = db.prepare("SELECT * FROM orcamentos_brindes WHERE id = ?").get(id);
  if (!orcamento) return res.status(404).send("Orçamento não encontrado");

  ensureDefaultBrindePersonalizacoes();
  const tiposPersonalizacao = db.prepare("SELECT * FROM brindes_personalizacao_tipos WHERE ativo=1 ORDER BY nome ASC").all();

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

  // Personalizações por item
  const persRows = db.prepare(`
    SELECT p.*, t.nome AS tipo_nome
    FROM orcamentos_brindes_itens_personalizacoes p
    JOIN brindes_personalizacao_tipos t ON t.id = p.tipo_id
    WHERE p.item_id IN (
      SELECT id FROM orcamentos_brindes_itens WHERE orcamento_id = ?
    )
    ORDER BY p.id DESC
  `).all(id);
  const personalizacoesByItem = {};
  (persRows || []).forEach(r => {
    personalizacoesByItem[r.item_id] = personalizacoesByItem[r.item_id] || [];
    personalizacoesByItem[r.item_id].push(r);
  });

  const prazoInfo = calcPrazoOrcamentoBrindes(id);

  // Totais financeiros (custo x venda) para painel lateral
  let custoTotal = 0;
  let custoBaseTotal = 0;
  let persTotal = 0;
  let setupTotal = 0;

  (itens || []).forEach((it) => {
    const qtd = Math.max(1, Number(it.quantidade || 1));
    const base = Number(it.custo_unit_base || 0) * qtd;
    const pers = (personalizacoesByItem && personalizacoesByItem[it.id]) ? personalizacoesByItem[it.id] : [];
    const persT = (pers || []).reduce((acc, p) => acc + Number(p.total_personalizacao || 0), 0);
    const setupT = (pers || []).reduce((acc, p) => acc + Number(p.setup_valor || 0), 0);

    custoBaseTotal += base;
    persTotal += persT;
    setupTotal += setupT;
    custoTotal += (base + persT);
  });

  const vendaSubtotal = Number(orcamento.subtotal || 0);
  const vendaTotal = Number(orcamento.total || 0);
  const lucroEstimado = vendaSubtotal - custoTotal;
  const markupReal = custoTotal > 0 ? ((vendaSubtotal - custoTotal) / custoTotal) * 100 : 0;

  const templates = [
    { key: "simples", nome: "Brinde simples (sem personalização)" },

    { key: "silk_1_frente", nome: "Silk 1 cor (Frente)" },
    { key: "silk_2_frente", nome: "Silk 2 cores (Frente)" },
    { key: "silk_1_frente_verso", nome: "Silk 1 cor (Frente + Verso)" },
    { key: "silk_2_frente_verso", nome: "Silk 2 cores (Frente + Verso)" },

    { key: "uvdtf_frente", nome: "UV DTF (Frente)" },
    { key: "uvdtf_frente_verso", nome: "UV DTF (Frente + Verso)" },

    { key: "laser_frente", nome: "Laser (Frente)" },
  ];

  // Texto pronto para WhatsApp (com copiar)
  const baseUrl = getBaseUrlFromReq(req);
  const pdfUrl = `${baseUrl}/brindes/orcamentos/${id}/imprimir`;
  const prazoTextoOverride = (orcamento.prazo_entrega_texto || "").trim();
  const prazoTextoCalc = `${prazoInfo.total_dias} dias úteis`;
  const validadeDias = Math.max(1, Math.floor(Number(orcamento.validade_em_dias || 7)));
  const totalFmt = Number(orcamento.total || 0).toLocaleString("pt-BR", { minimumFractionDigits: 2, maximumFractionDigits: 2 });

  const itensLinhas = (itens || []).slice(0, 8).map((it) => {
    const unit = Number(it.preco_unit_venda || 0).toLocaleString("pt-BR", { minimumFractionDigits: 2, maximumFractionDigits: 2 });
    return `• ${it.quantidade}x ${it.nome} (${it.codigo_fornecedor}) — R$ ${unit}/un`;
  });
  const mais = (itens || []).length > 8 ? `\n• + ${(itens.length - 8)} item(ns)` : "";
  const whatsappMsg = [
    `*Orçamento de Brindes #${id}*`,
    `Cliente: *${(orcamento.cliente_nome || "-").trim() || "-"}*`,
    "",
    (itensLinhas.length ? ["Itens:", ...itensLinhas].join("\n") : "Itens: -"),
    mais,
    "",
    `Total: *R$ ${totalFmt}*`,
    `Prazo: ${prazoTextoOverride || prazoTextoCalc}`,
    `Validade: ${validadeDias} dia(s)`,
    `PDF: ${pdfUrl}`
  ].filter(Boolean).join("\n");

  res.render("layout", {
    title: `Brindes - Orçamento #${id}`,
    view: "brindes-orcamento-editar-page",
    orcamento,
    itens,
    catalogo,
    estoqueBrindes,
    tiposPersonalizacao,
    personalizacoesByItem,
    prazoInfo,
    financeiro: { custoTotal, custoBaseTotal, persTotal, setupTotal, lucroEstimado, markupReal, vendaSubtotal, vendaTotal },
    templates,
    pacotesPersonalizacao: listPacotesPersonalizacao(true),
    whatsappMsg
  });
});

app.post("/brindes/orcamentos/:id(\\d+)/update", (req, res) => {
  const id = Number(req.params.id);
  const {
    cliente_id,
    cliente_nome_avulso,
    cliente_whatsapp,
    cliente_email,
    cliente_documento,
    cliente_endereco,
    cliente_cidade,
    cliente_uf,
    vendedor_nome,
    canal_venda,
    validade_em_dias,
    prazo_base_dias,
    prazo_entrega_texto,
    observacoes,
    desconto,
    status,
    pagamento_texto,
    frete_texto,
    frete_valor,
    impostos_texto,
    condicoes_comerciais
  } = req.body;

  try {
    db.prepare(`
      UPDATE orcamentos_brindes
      SET cliente_id = ?,
          cliente_nome = ?,
          cliente_whatsapp = ?,
          cliente_email = ?,
          cliente_documento = ?,
          cliente_endereco = ?,
          cliente_cidade = ?,
          cliente_uf = ?,
          vendedor_nome = ?,
          canal_venda = ?,
          validade_em_dias = ?,
          prazo_base_dias = ?,
          prazo_entrega_texto = ?,
          observacoes = ?,
          desconto = ?,
          status = ?,
          pagamento_texto = ?,
          frete_texto = ?,
          frete_valor = ?,
          impostos_texto = ?,
          condicoes_comerciais = ?
      WHERE id = ?
    `).run(
      Number(cliente_id || 0) || null,
      (cliente_nome_avulso || "").trim() || null,
      (cliente_whatsapp || "").trim() || null,
      (cliente_email || "").trim() || null,
      (cliente_documento || "").trim() || null,
      (cliente_endereco || "").trim() || null,
      (cliente_cidade || "").trim() || null,
      (cliente_uf || "").trim() || null,
      (vendedor_nome || "").trim() || null,
      (canal_venda || "").trim() || null,
      Math.max(1, Math.floor(Number(validade_em_dias || 7))),
      Math.max(0, Math.floor(Number(prazo_base_dias ?? 10))),
      (prazo_entrega_texto || "").trim() || null,
      (observacoes || "").trim() || null,
      Number(desconto || 0),
      (status || "RASCUNHO").trim() || "RASCUNHO",
      (pagamento_texto || "").trim() || null,
      (frete_texto || "").trim() || null,
      Number(frete_valor || 0),
      (impostos_texto || "").trim() || null,
      (condicoes_comerciais || "").trim() || null,
      id
    );
    recalcOrcamentoBrindes(id);
  } catch (e) {
    return res.redirect(`/brindes/orcamentos/${id}?erro=` + encodeURIComponent(e.message || e));
  }
  res.redirect(`/brindes/orcamentos/${id}`);
});

// ===== Brindes: Aprovar e Gerar Pedido (centralizado no módulo de Pedidos) =====
app.post("/brindes/orcamentos/:id(\\d+)/gerar-pedido", (req, res) => {
  const id = Number(req.params.id);
  const u = req.session?.user;
  if (!u) return res.redirect('/login');
  // Gera pedido dentro do módulo de pedidos — exige permissão de Pedidos também.
  if (!userHasModule(u, 'pedidos')) {
    return res.status(403).render('layout', {
      view: 'forbidden-page',
      title: 'Sem permissão',
      pageTitle: 'Sem permissão',
      pageSubtitle: 'Você não tem acesso ao módulo de Pedidos',
      activeMenu: ''
    });
  }

  const orc = db.prepare("SELECT * FROM orcamentos_brindes WHERE id = ?").get(id);
  if (!orc) return res.redirect('/brindes/orcamentos?erro=' + encodeURIComponent('Orçamento não encontrado'));

  // Se já existe pedido vinculado, só abre.
  if (orc.pedido_id) return res.redirect(`/pedidos/${orc.pedido_id}`);

  const itens = db.prepare(`
    SELECT i.*, c.nome AS catalogo_nome, c.codigo_fornecedor, c.unidade, f.nome AS fornecedor_nome
    FROM orcamentos_brindes_itens i
    JOIN brindes_catalogo c ON c.id = i.catalogo_id
    JOIN brindes_fornecedores f ON f.id = c.fornecedor_id
    WHERE i.orcamento_id = ?
    ORDER BY i.id ASC
  `).all(id);
  if (!itens || itens.length === 0) {
    return res.redirect(`/brindes/orcamentos/${id}?erro=` + encodeURIComponent('Adicione ao menos 1 item antes de gerar o pedido.'));
  }

  const persRows = db.prepare(`
    SELECT p.*, t.nome AS tipo_nome
    FROM orcamentos_brindes_itens_personalizacoes p
    JOIN brindes_personalizacao_tipos t ON t.id = p.tipo_id
    WHERE p.item_id IN (
      SELECT id FROM orcamentos_brindes_itens WHERE orcamento_id = ?
    )
    ORDER BY p.id ASC
  `).all(id);
  const persByItem = {};
  (persRows || []).forEach(r => {
    persByItem[r.item_id] = persByItem[r.item_id] || [];
    persByItem[r.item_id].push(r);
  });

  const fmtMoney = (v) => (Number(v || 0)).toLocaleString('pt-BR', { minimumFractionDigits: 2, maximumFractionDigits: 2 });

  try {
    const tx = db.transaction(() => {
      const prazoInfo = calcPrazoOrcamentoBrindes(id);
      const prazoTxt = (orc.prazo_entrega_texto || '').trim() || `${prazoInfo.total_dias} dias úteis`;

      const pedidoRes = db.prepare(`
        INSERT INTO pedidos (
          cliente_nome_avulso, cliente_telefone_avulso,
          vendedor_nome, prazo_entrega, data_venda,
          status, tipo_venda, canal_venda,
          subtotal_itens, desconto_tipo, desconto_valor, frete_valor, total,
          observacoes_vendedor, observacoes_internas,
          criado_em, atualizado_em
        ) VALUES (?,?,?,?,?, ?,?,?, ?,?,?,?, ?, ?,?, datetime('now'), datetime('now'))
      `).run(
        (orc.cliente_nome || '').trim() || null,
        (orc.cliente_whatsapp || '').trim() || null,
        (u?.nome || u?.usuario || '').trim() || null,
        prazoTxt,
        new Date().toISOString().slice(0,10),
        'APROVADO',
        'MISTO',
        'MANUAL',
        Number(orc.subtotal || 0),
        'VALOR',
        Number(orc.desconto || 0),
        Number(orc.frete_valor || 0),
        Number(orc.total || 0),
        (orc.observacoes || '').trim() || null,
        `Gerado do Orçamento de Brindes #${id}`
      );

      const pedidoId = Number(pedidoRes.lastInsertRowid);

      itens.forEach(it => {
        const qtd = Math.max(1, Number(it.quantidade || 1));
        const pers = (persByItem[it.id] || []);
        const totalPers = (pers || []).reduce((acc, p) => acc + Number(p.total_personalizacao || 0), 0);
        const custoTotal = Number(it.custo_total_base || 0) + totalPers;
        const custoUnit = custoTotal / qtd;

        const nomeBase = (it.descricao_custom || it.catalogo_nome || 'Brinde').trim();
        const desc = `${nomeBase} • ${it.fornecedor_nome} • ${it.codigo_fornecedor}`;

        const obsParts = [];
        if (pers && pers.length > 0) {
          obsParts.push('Personalizações:');
          pers.forEach(p => {
            const bits = [];
            bits.push(String(p.tipo_nome || '').trim() || 'Personalização');
            if (p.posicao) bits.push(`pos: ${p.posicao}`);
            if (p.cores) bits.push(`${p.cores} cor(es)`);
            if (p.tamanho) bits.push(`tam: ${p.tamanho}`);
            if (Number(p.setup_valor || 0) > 0) bits.push(`setup R$ ${fmtMoney(p.setup_valor)}`);
            if (Number(p.custo_unit_personalizacao || 0) > 0) bits.push(`custo/un R$ ${fmtMoney(p.custo_unit_personalizacao)}`);
            if (Number(p.total_personalizacao || 0) > 0) bits.push(`total R$ ${fmtMoney(p.total_personalizacao)}`);
            obsParts.push('• ' + bits.join(' | '));
          });
        }
        if (it.observacoes_item) obsParts.push(String(it.observacoes_item));

        db.prepare(`
          INSERT INTO pedido_itens (
            pedido_id, descricao, quantidade, unidade, observacao,
            preco_unit, total_item, custo_unit
          ) VALUES (?,?,?,?,?, ?,?,?)
        `).run(
          pedidoId,
          desc,
          qtd,
          (it.unidade || 'UN'),
          obsParts.length ? obsParts.join('\n') : null,
          Number(it.preco_unit_venda || 0),
          Number(it.total_item || 0),
          Number(custoUnit || 0)
        );
      });

      // Marca orçamento como aprovado e vincula ao pedido
      db.prepare("UPDATE orcamentos_brindes SET status='APROVADO', pedido_id=? WHERE id=?").run(pedidoId, id);

      return pedidoId;
    });

    const pedidoId = tx();
    return res.redirect(`/pedidos/${pedidoId}/editar?ok=` + encodeURIComponent(`Pedido #${pedidoId} gerado a partir do Orçamento #${id}`));
  } catch (e) {
    console.error(e);
    return res.redirect(`/brindes/orcamentos/${id}?erro=` + encodeURIComponent(e.message || e));
  }
});

// ===== PDF: Orçamento de Brindes =====

// Links públicos (SaaS) para download/visualização de PDFs
app.get("/p/:token", (req, res) => {
  const token = String(req.params.token || "").trim();
  if (!token) return res.status(404).send("Link inválido");

  try {
    // Se a tabela ainda não existir (instalação antiga), apenas 404.
    if (IS_PG()) return res.status(404).send("Link indisponível neste modo");

    const row = db.prepare("SELECT * FROM pdf_public_links WHERE token = ? LIMIT 1").get(token);
    if (!row) return res.status(404).send("Link não encontrado");

    // expiração
    if (row.expires_at) {
      const exp = new Date(String(row.expires_at).replace(' ', 'T'));
      if (!isNaN(exp.getTime()) && exp.getTime() < Date.now()) {
        return res.status(410).send("Link expirado");
      }
    }

    // arquivo
    let rel = String(row.file_rel || "");
    if (rel.startsWith("uploads/")) rel = rel.slice("uploads/".length);
    if (rel.startsWith("/uploads/")) rel = rel.slice("/uploads/".length);
    const abs = path.join(uploadsDir, rel);
    if (!fs.existsSync(abs)) return res.status(404).send("Arquivo não encontrado");

    const download = String(req.query.download || "") === "1";
    const filename = path.basename(abs);
    res.setHeader("Content-Type", "application/pdf");
    res.setHeader("Content-Disposition", `${download ? "attachment" : "inline"}; filename=\"${filename}\"`);
    return res.sendFile(abs);
  } catch (e) {
    console.error(e);
    if (res.headersSent) return;
    return res.status(500).send("Erro ao abrir link");
  }
});

function makePdfToken() {
  return crypto.randomBytes(18).toString("hex");
}

function upsertPublicPdfLink({ kind, refId, fileRel, expiresDays = 7 }) {
  if (IS_PG()) return null;
  const k = String(kind || "");
  const rid = Number(refId || 0);
  const fr = String(fileRel || "");
  if (!k || !rid || !fr) return null;

  // reaproveita link válido mais recente, se existir
  try {
    const existing = db.prepare(
      "SELECT * FROM pdf_public_links WHERE kind=? AND ref_id=? ORDER BY id DESC LIMIT 1"
    ).get(k, rid);
    if (existing && existing.token) {
      // se não expirou, reaproveita
      if (!existing.expires_at) return existing.token;
      const exp = new Date(String(existing.expires_at).replace(' ', 'T'));
      if (!isNaN(exp.getTime()) && exp.getTime() > Date.now()) return existing.token;
    }
  } catch (_) {}

  const token = makePdfToken();
  // expires_at em texto (YYYY-MM-DD HH:mm:ss)
  const exp = new Date(Date.now() + (Math.max(1, Number(expiresDays) || 7) * 24 * 60 * 60 * 1000));
  const pad = (n) => String(n).padStart(2, "0");
  const expiresAt = `${exp.getFullYear()}-${pad(exp.getMonth()+1)}-${pad(exp.getDate())} ${pad(exp.getHours())}:${pad(exp.getMinutes())}:${pad(exp.getSeconds())}`;

  db.prepare(
    "INSERT INTO pdf_public_links (token, kind, ref_id, file_rel, expires_at) VALUES (?,?,?,?,?)"
  ).run(token, k, rid, fr, expiresAt);

  return token;
}

app.get("/brindes/orcamentos/:id(\\d+)/pdf", (req, res) => {
  // Para padronizar com o visual do sistema (mesmo tema do "Imprimir OP"),
  // este módulo usa impressão via HTML (window.print). Mantemos /pdf por
  // compatibilidade e redirecionamos para /imprimir.
  return res.redirect(`/brindes/orcamentos/${req.params.id}/imprimir`);
});

// Download direto em PDF (mesmo layout do /imprimir) usando Puppeteer.
// Preparado para Railway: usa --no-sandbox e respeita PUPPETEER_EXECUTABLE_PATH.
app.get("/brindes/orcamentos/:id(\\d+)/baixar-pdf", async (req, res) => {
  const id = Number(req.params.id);
  try {
    const orc = db.prepare("SELECT id FROM orcamentos_brindes WHERE id = ?").get(id);
    if (!orc) return res.status(404).send("Orçamento não encontrado");

    const baseUrl = getBaseUrlFromReq(req);
    const url = `${baseUrl}/brindes/orcamentos/${id}/imprimir?pdf=1`;

    let puppeteer;
    try {
      puppeteer = require("puppeteer");
    } catch (e) {
      console.error("Puppeteer não instalado. Rode: npm i puppeteer");
      return res.status(500).send("Dependência ausente: puppeteer");
    }

    const browser = await puppeteer.launch({
      headless: "new",
      args: ["--no-sandbox", "--disable-setuid-sandbox"],
      executablePath: process.env.PUPPETEER_EXECUTABLE_PATH || undefined,
    });

    try {
      const page = await browser.newPage();
      // IMPORTANT: a rota /imprimir pode exigir autenticação (cookie de sessão).
      // Sem repassar o cookie do usuário, o Puppeteer cai na tela de login.
      const cookieHeader = req.headers.cookie || "";
      if (cookieHeader) {
        await page.setExtraHTTPHeaders({ cookie: cookieHeader });
      }
      await page.goto(url, { waitUntil: "networkidle0" });
      await page.emulateMediaType("print");

      const pdfBuffer = await page.pdf({
        format: "A4",
        printBackground: true,
        preferCSSPageSize: true,
        margin: { top: "12mm", right: "12mm", bottom: "12mm", left: "12mm" },
      });

      // SaaS: salva uma cópia em /uploads para gerar link público.
      // (O download continua funcionando como antes.)
      try {
        const relDir = path.join("pdfs", "orcamentos_brindes");
        const absDir = path.join(uploadsDir, relDir);
        if (!fs.existsSync(absDir)) fs.mkdirSync(absDir, { recursive: true });
        const ts = new Date();
        const pad = (n) => String(n).padStart(2, "0");
        const stamp = `${ts.getFullYear()}${pad(ts.getMonth() + 1)}${pad(ts.getDate())}-${pad(ts.getHours())}${pad(ts.getMinutes())}${pad(ts.getSeconds())}`;
        const storedName = `ORCAMENTO-BRINDES-${String(id).padStart(4, "0")}-${stamp}.pdf`;
        const absFile = path.join(absDir, storedName);
        fs.writeFileSync(absFile, pdfBuffer);

        // cria/atualiza link público (7 dias)
        const token = upsertPublicPdfLink({
          kind: "ORCAMENTO_BRINDES",
          refId: id,
          fileRel: path.join(relDir, storedName).replace(/\\/g, "/"),
          expiresDays: 7,
        });

        // expõe link no header (útil para integrações)
        if (token) {
          res.setHeader("X-Public-Pdf-Link", `${baseUrl}/p/${token}`);
        }
      } catch (e) {
        console.warn("Falha ao salvar PDF/link público:", e?.message || e);
      }

      const filename = `ORCAMENTO-BRINDES-${String(id).padStart(4, "0")}.pdf`;
      res.setHeader("Content-Type", "application/pdf");
      res.setHeader("Content-Disposition", `attachment; filename=\"${filename}\"`);
      return res.end(pdfBuffer);
    } finally {
      await browser.close();
    }
  } catch (e) {
    console.error(e);
    if (res.headersSent) return;
    return res.status(500).send("Erro ao gerar PDF");
  }
});

// SaaS: gera (se necessário) o PDF, cria um link público e abre WhatsApp com mensagem pronta.
app.get("/brindes/orcamentos/:id(\\d+)/whatsapp", async (req, res) => {
  const id = Number(req.params.id);
  try {
    const orc = db.prepare("SELECT * FROM orcamentos_brindes WHERE id = ?").get(id);
    if (!orc) return res.status(404).send("Orçamento não encontrado");

    const waDigits = String(orc.cliente_whatsapp || "").replace(/\D/g, "");
    if (waDigits.length < 10) {
      return res.redirect(`/brindes/orcamentos/${id}/imprimir?erro=` + encodeURIComponent("WhatsApp do cliente não informado/ inválido."));
    }

    // tenta achar link existente válido
    const baseUrl = getBaseUrlFromReq(req);
    let token = null;
    try {
      if (!IS_PG()) {
        const row = db.prepare(
          "SELECT token, expires_at FROM pdf_public_links WHERE kind='ORCAMENTO_BRINDES' AND ref_id=? ORDER BY id DESC LIMIT 1"
        ).get(id);
        if (row && row.token) {
          if (!row.expires_at) token = row.token;
          else {
            const exp = new Date(String(row.expires_at).replace(' ', 'T'));
            if (!isNaN(exp.getTime()) && exp.getTime() > Date.now()) token = row.token;
          }
        }
      }
    } catch (_) {}

    // se não houver token válido, gera o PDF agora (salvando e criando token) chamando internamente o mesmo pipeline do /baixar-pdf
    if (!token) {
      // Gera PDF via Puppeteer e salva. Aqui não enviamos download ao usuário.
      let puppeteer;
      try { puppeteer = require("puppeteer"); } catch (_) { puppeteer = null; }
      if (!puppeteer) {
        return res.redirect(`/brindes/orcamentos/${id}/imprimir?erro=` + encodeURIComponent("Dependência ausente: puppeteer"));
      }

      const url = `${baseUrl}/brindes/orcamentos/${id}/imprimir?pdf=1`;
      const browser = await puppeteer.launch({
        headless: "new",
        args: ["--no-sandbox", "--disable-setuid-sandbox"],
        executablePath: process.env.PUPPETEER_EXECUTABLE_PATH || undefined,
      });
      try {
        const page = await browser.newPage();
        const cookieHeader = req.headers.cookie || "";
        if (cookieHeader) await page.setExtraHTTPHeaders({ cookie: cookieHeader });
        await page.goto(url, { waitUntil: "networkidle0" });
        await page.emulateMediaType("print");
        const pdfBuffer = await page.pdf({
          format: "A4",
          printBackground: true,
          preferCSSPageSize: true,
          margin: { top: "12mm", right: "12mm", bottom: "12mm", left: "12mm" },
        });

        const relDir = path.join("pdfs", "orcamentos_brindes");
        const absDir = path.join(uploadsDir, relDir);
        if (!fs.existsSync(absDir)) fs.mkdirSync(absDir, { recursive: true });
        const ts = new Date();
        const pad = (n) => String(n).padStart(2, "0");
        const stamp = `${ts.getFullYear()}${pad(ts.getMonth() + 1)}${pad(ts.getDate())}-${pad(ts.getHours())}${pad(ts.getMinutes())}${pad(ts.getSeconds())}`;
        const storedName = `ORCAMENTO-BRINDES-${String(id).padStart(4, "0")}-${stamp}.pdf`;
        const absFile = path.join(absDir, storedName);
        fs.writeFileSync(absFile, pdfBuffer);

        token = upsertPublicPdfLink({
          kind: "ORCAMENTO_BRINDES",
          refId: id,
          fileRel: path.join(relDir, storedName).replace(/\\/g, "/"),
          expiresDays: 7,
        });
      } finally {
        await browser.close();
      }
    }

    const publicUrl = token ? `${baseUrl}/p/${token}` : `${baseUrl}/brindes/orcamentos/${id}/imprimir`;
    const msg = `Olá! Segue o orçamento de brindes #${id}.\n\nPDF (link): ${publicUrl}\n\nQualquer dúvida, me chama aqui.`;
    const waUrl = `https://wa.me/${waDigits}?text=${encodeURIComponent(msg)}`;
    return res.redirect(waUrl);
  } catch (e) {
    console.error(e);
    return res.redirect(`/brindes/orcamentos/${id}/imprimir?erro=` + encodeURIComponent("Falha ao preparar WhatsApp"));
  }
});

app.get("/brindes/orcamentos/:id(\\d+)/imprimir", (req, res) => {
  const id = Number(req.params.id);
  const isPdf = String(req.query.pdf || "").trim() === "1";

  // garante totais
  try {
    const itemIds = db.prepare("SELECT id FROM orcamentos_brindes_itens WHERE orcamento_id=?").all(id);
    (itemIds || []).forEach(r => { try { recalcItemOrcamentoBrindes(Number(r.id)); } catch (_) {} });
    try { recalcOrcamentoBrindes(id); } catch (_) {}
  } catch (_) {}

  const orcamento = db.prepare("SELECT * FROM orcamentos_brindes WHERE id = ?").get(id);
  if (!orcamento) return res.status(404).send("Orçamento não encontrado");

  // marca como enviado ao imprimir
  try {
    if ((orcamento.status || "RASCUNHO") === "RASCUNHO") {
      db.prepare("UPDATE orcamentos_brindes SET status='ENVIADO' WHERE id = ?").run(id);
      orcamento.status = "ENVIADO";
    }
  } catch (_) {}

  const prazoInfo = (() => {
    try { return calcPrazoOrcamentoBrindes(id); }
    catch (_) {
      const base = Math.max(0, Math.floor(Number(orcamento.prazo_base_dias ?? 10)));
      return { base_dias: base, add_personalizacoes_dias: 0, total_dias: base };
    }
  })();

  const itens = db.prepare(`
    SELECT i.*, c.nome, c.codigo_fornecedor, f.nome AS fornecedor_nome
    FROM orcamentos_brindes_itens i
    JOIN brindes_catalogo c ON c.id = i.catalogo_id
    JOIN brindes_fornecedores f ON f.id = c.fornecedor_id
    WHERE i.orcamento_id = ?
    ORDER BY i.id ASC
  `).all(id);

  const persRows = db.prepare(`
    SELECT p.*, t.nome AS tipo_nome
    FROM orcamentos_brindes_itens_personalizacoes p
    JOIN brindes_personalizacao_tipos t ON t.id = p.tipo_id
    WHERE p.item_id IN (
      SELECT id FROM orcamentos_brindes_itens WHERE orcamento_id = ?
    )
    ORDER BY p.id ASC
  `).all(id);

  const personalizacoesByItem = {};
  (persRows || []).forEach(r => {
    personalizacoesByItem[r.item_id] = personalizacoesByItem[r.item_id] || [];
    personalizacoesByItem[r.item_id].push(r);
  });

  const _logoInfo = resolveBrandLogoPath();
  const logoUrl = (_logoInfo && _logoInfo.rel) ? ("/" + _logoInfo.rel.replace(/\\/g, "/")) : (_logoInfo ? "/logo.png" : null);

  const baseUrl = getBaseUrlFromReq(req);
  return res.render("brindes-orcamento-print", { orcamento, itens, personalizacoesByItem, prazoInfo, logoUrl, isPdf, baseUrl });
});

// ===== Brindes - Ações do orçamento (WhatsApp / Duplicar / Templates / Editar item) =====

function getBaseUrlFromReq(req) {
  const proto = (req.headers["x-forwarded-proto"] || req.protocol || "http").split(",")[0].trim();
  const host = (req.headers["x-forwarded-host"] || req.get("host") || "").split(",")[0].trim();
  return `${proto}://${host}`;
}

app.post("/brindes/orcamentos/:id(\\d+)/mark-enviado", (req, res) => {
  const id = Number(req.params.id);
  try {
    const orc = db.prepare("SELECT status FROM orcamentos_brindes WHERE id = ?").get(id);
    if (orc && (orc.status || "RASCUNHO") === "RASCUNHO") {
      db.prepare("UPDATE orcamentos_brindes SET status='ENVIADO' WHERE id = ?").run(id);
    }
  } catch (_) {}
  res.json({ ok: true });
});

app.get("/brindes/orcamentos/:id(\\d+)/whatsapp", (req, res) => {
  const id = Number(req.params.id);
  const orc = db.prepare("SELECT * FROM orcamentos_brindes WHERE id = ?").get(id);
  if (!orc) return res.status(404).send("Orçamento não encontrado");

  // Ao abrir WhatsApp, marca como ENVIADO (sem sobrescrever statuses finais)
  try {
    if ((orc.status || "RASCUNHO") === "RASCUNHO") {
      db.prepare("UPDATE orcamentos_brindes SET status='ENVIADO' WHERE id = ?").run(id);
      orc.status = "ENVIADO";
    }
  } catch (_) {}

  const baseUrl = getBaseUrlFromReq(req);
  const pdfUrl = `${baseUrl}/brindes/orcamentos/${id}/imprimir`;

  const prazoInfo = calcPrazoOrcamentoBrindes(id);
  const prazoTextoOverride = (orc.prazo_entrega_texto || "").trim();
  const prazoTextoCalc = `${prazoInfo.total_dias} dias úteis`;

  const validadeDias = Math.max(1, Math.floor(Number(orc.validade_em_dias || 7)));
  const total = Number(orc.total || 0).toLocaleString("pt-BR", { minimumFractionDigits: 2, maximumFractionDigits: 2 });

  const itens = db.prepare(`
    SELECT i.quantidade, c.nome, c.codigo_fornecedor, i.preco_unit_venda
    FROM orcamentos_brindes_itens i
    JOIN brindes_catalogo c ON c.id = i.catalogo_id
    WHERE i.orcamento_id = ?
    ORDER BY i.id ASC
  `).all(id);

  const itensLinhas = (itens || []).slice(0, 8).map((it) => {
    const unit = Number(it.preco_unit_venda || 0).toLocaleString("pt-BR", { minimumFractionDigits: 2, maximumFractionDigits: 2 });
    return `• ${it.quantidade}x ${it.nome} (${it.codigo_fornecedor}) — R$ ${unit}/un`;
  });
  const mais = (itens || []).length > 8 ? `\n• + ${(itens.length - 8)} item(ns)` : "";

  const msg = [
    `*Orçamento de Brindes #${id}*`,
    `Cliente: *${(orc.cliente_nome || "-").trim() || "-"}*`,
    "",
    (itensLinhas.length ? ["Itens:", ...itensLinhas].join("\n") : "Itens: -"),
    mais,
    "",
    `Total: *R$ ${total}*`,
    `Prazo: ${prazoTextoOverride || prazoTextoCalc}`,
    `Validade: ${validadeDias} dia(s)`,
    `PDF: ${pdfUrl}`
  ].filter(Boolean).join("\n");

  const url = "https://wa.me/?text=" + encodeURIComponent(msg);
  res.redirect(url);
});

app.post("/brindes/orcamentos/:id(\\d+)/duplicar", (req, res) => {
  const id = Number(req.params.id);
  const orc = db.prepare("SELECT * FROM orcamentos_brindes WHERE id = ?").get(id);
  if (!orc) return res.status(404).send("Orçamento não encontrado");

  try {
    const r = db.prepare(`
      INSERT INTO orcamentos_brindes (
        cliente_nome, status,
        validade_em_dias, prazo_base_dias, prazo_entrega_texto,
        observacoes, pagamento_texto, frete_texto, frete_valor, impostos_texto, condicoes_comerciais,
        subtotal, desconto, total, created_at
      ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?, datetime('now'))
    `).run(
      (orc.cliente_nome || null),
      "RASCUNHO",
      Number(orc.validade_em_dias || 7),
      Number(orc.prazo_base_dias || 10),
      orc.prazo_entrega_texto || null,
      orc.observacoes || null,
      orc.pagamento_texto || null,
      orc.frete_texto || null,
      Number(orc.frete_valor || 0),
      orc.impostos_texto || null,
      orc.condicoes_comerciais || null,
      0,
      Number(orc.desconto || 0),
      0
    );

    const newId = r.lastInsertRowid;

    const itens = db.prepare("SELECT * FROM orcamentos_brindes_itens WHERE orcamento_id = ? ORDER BY id ASC").all(id);
    const mapItem = new Map();

    (itens || []).forEach((it) => {
      const rIt = db.prepare(`
        INSERT INTO orcamentos_brindes_itens (
          orcamento_id, catalogo_id, quantidade,
          custo_unit_base, custo_total_base,
          margem_percent, preco_unit_venda, total_item,
          layout_arquivo, layout_nome_original
        ) VALUES (?,?,?,?,?,?,?,?,?,?)
      `).run(
        newId,
        it.catalogo_id,
        it.quantidade,
        it.custo_unit_base,
        it.custo_total_base,
        it.margem_percent,
        it.preco_unit_venda,
        it.total_item,
        it.layout_arquivo || null,
        it.layout_nome_original || null
      );
      mapItem.set(it.id, rIt.lastInsertRowid);
    });

    const pers = db.prepare("SELECT * FROM orcamentos_brindes_itens_personalizacoes WHERE item_id IN (SELECT id FROM orcamentos_brindes_itens WHERE orcamento_id = ?) ORDER BY id ASC").all(id);
    (pers || []).forEach((p) => {
      const newItemId = mapItem.get(p.item_id);
      if (!newItemId) return;
      db.prepare(`
        INSERT INTO orcamentos_brindes_itens_personalizacoes
        (item_id, tipo_id, cores, posicao, tamanho, prazo_dias, setup_valor, custo_unit_personalizacao, total_personalizacao)
        VALUES (?,?,?,?,?,?,?,?,?)
      `).run(
        newItemId,
        p.tipo_id,
        p.cores,
        p.posicao,
        p.tamanho,
        p.prazo_dias,
        p.setup_valor,
        p.custo_unit_personalizacao,
        p.total_personalizacao
      );
    });

    recalcOrcamentoBrindes(Number(newId));
    res.redirect(`/brindes/orcamentos/${newId}?ok=` + encodeURIComponent("Orçamento duplicado"));
  } catch (e) {
    return res.redirect(`/brindes/orcamentos/${id}?erro=` + encodeURIComponent(e.message || e));
  }
});

app.post("/brindes/orcamentos/:id(\\d+)/itens/:itemId(\\d+)/update", (req, res) => {
  const orcamento_id = Number(req.params.id);
  const itemId = Number(req.params.itemId);
  const { quantidade, custo_unit_base, margem_percent } = req.body;

  const item = db.prepare("SELECT * FROM orcamentos_brindes_itens WHERE id = ? AND orcamento_id = ?").get(itemId, orcamento_id);
  if (!item) return res.redirect(`/brindes/orcamentos/${orcamento_id}?erro=` + encodeURIComponent("Item não encontrado"));

  try {
    const qtd = Math.max(1, Math.floor(Number(quantidade || item.quantidade || 1)));
    const custoUnit = Math.max(0, Number(custo_unit_base ?? item.custo_unit_base ?? 0));
    const custoTotalBase = custoUnit * qtd;
    const margem = Math.max(0, Number(margem_percent ?? item.margem_percent ?? 0));

    db.prepare("UPDATE orcamentos_brindes_itens SET quantidade=?, custo_unit_base=?, custo_total_base=?, margem_percent=? WHERE id=? AND orcamento_id=?")
      .run(qtd, custoUnit, custoTotalBase, margem, itemId, orcamento_id);

    // Recalcula totais das personalizações (porque depende da quantidade)
    const pers = db.prepare("SELECT id, setup_valor, custo_unit_personalizacao FROM orcamentos_brindes_itens_personalizacoes WHERE item_id = ?").all(itemId);
    (pers || []).forEach((p) => {
      const totalPers = (Number(p.custo_unit_personalizacao || 0) * qtd) + Number(p.setup_valor || 0);
      db.prepare("UPDATE orcamentos_brindes_itens_personalizacoes SET total_personalizacao=? WHERE id=?").run(totalPers, p.id);
    });

    recalcItemOrcamentoBrindes(itemId);
  } catch (e) {
    return res.redirect(`/brindes/orcamentos/${orcamento_id}?erro=` + encodeURIComponent(e.message || e));
  }

  res.redirect(`/brindes/orcamentos/${orcamento_id}#item-${itemId}`);
});

// Upload do layout/arte do item (PDF ou imagem)
app.post("/brindes/orcamentos/:id(\\d+)/itens/:itemId(\\d+)/layout", uploadPedido.single("layout_file"), (req, res) => {
  const orcamento_id = Number(req.params.id);
  const itemId = Number(req.params.itemId);
  try {
    if (!req.file) return res.redirect(`/brindes/orcamentos/${orcamento_id}#item-${itemId}`);
    db.prepare(`
      UPDATE orcamentos_brindes_itens
      SET layout_arquivo = ?, layout_nome_original = ?
      WHERE id = ? AND orcamento_id = ?
    `).run(req.file.filename, req.file.originalname || null, itemId, orcamento_id);
  } catch (e) {
    return res.redirect(`/brindes/orcamentos/${orcamento_id}?erro=` + encodeURIComponent(e.message || e));
  }
  res.redirect(`/brindes/orcamentos/${orcamento_id}#item-${itemId}`);
});

app.post("/brindes/orcamentos/:id(\\d+)/itens/:itemId(\\d+)/layout/remover", (req, res) => {
  const orcamento_id = Number(req.params.id);
  const itemId = Number(req.params.itemId);
  try {
    const it = db.prepare("SELECT layout_arquivo FROM orcamentos_brindes_itens WHERE id=? AND orcamento_id=?").get(itemId, orcamento_id);
    if (it && it.layout_arquivo) {
      const abs = path.join(uploadsDir, it.layout_arquivo);
      try { if (fs.existsSync(abs)) fs.unlinkSync(abs); } catch (_) {}
    }
    db.prepare("UPDATE orcamentos_brindes_itens SET layout_arquivo=NULL, layout_nome_original=NULL WHERE id=? AND orcamento_id=?").run(itemId, orcamento_id);
  } catch (_) {}
  res.redirect(`/brindes/orcamentos/${orcamento_id}#item-${itemId}`);
});

app.post("/brindes/orcamentos/:id(\\d+)/itens/:itemId(\\d+)/template", (req, res) => {
  const orcamento_id = Number(req.params.id);
  const itemId = Number(req.params.itemId);
  const { template_key } = req.body;

  const item = db.prepare("SELECT id, quantidade FROM orcamentos_brindes_itens WHERE id = ? AND orcamento_id = ?").get(itemId, orcamento_id);
  if (!item) return res.redirect(`/brindes/orcamentos/${orcamento_id}?erro=` + encodeURIComponent("Item não encontrado"));

  try {
    ensureDefaultBrindePersonalizacoes();

    // Sempre aplica "limpando" personalizações anteriores
    db.prepare("DELETE FROM orcamentos_brindes_itens_personalizacoes WHERE item_id = ?").run(itemId);

    const keyRaw = String(template_key || "simples");
    const key = keyRaw.trim().toLowerCase();
    if (key === "simples") {
      recalcItemOrcamentoBrindes(itemId);
      return res.redirect(`/brindes/orcamentos/${orcamento_id}#item-${itemId}`);
    }

    // Compatibilidade (keys antigas)
    const alias = {
      "silk1": "silk_1_frente",
      "uvdtf": "uvdtf_frente",
      "laser": "laser_frente"
    };
    const k = alias[key] || key;

    const templateMap = {
      "silk_1_frente": [{ tipo: "Silk", cores: 1, posicao: "frente" }],
      "silk_2_frente": [{ tipo: "Silk", cores: 2, posicao: "frente" }],
      "silk_1_frente_verso": [{ tipo: "Silk", cores: 1, posicao: "frente" }, { tipo: "Silk", cores: 1, posicao: "verso" }],
      "silk_2_frente_verso": [{ tipo: "Silk", cores: 2, posicao: "frente" }, { tipo: "Silk", cores: 2, posicao: "verso" }],

      "uvdtf_frente": [{ tipo: "UV DTF", cores: null, posicao: "frente" }],
      "uvdtf_frente_verso": [{ tipo: "UV DTF", cores: null, posicao: "frente" }, { tipo: "UV DTF", cores: null, posicao: "verso" }],

      "laser_frente": [{ tipo: "Laser", cores: null, posicao: "frente" }]
    };

    const defs = templateMap[k];
    if (!defs) {
      recalcItemOrcamentoBrindes(itemId);
      return res.redirect(`/brindes/orcamentos/${orcamento_id}#item-${itemId}`);
    }

    const qtd = Math.max(1, Math.floor(Number(item.quantidade || 1)));

    // Setup: aplica somente na primeira personalização de um mesmo tipo
    const setupUsadoPorTipo = new Set();

    for (const d of defs) {
      const tipo = db.prepare("SELECT * FROM brindes_personalizacao_tipos WHERE lower(nome) LIKE lower(?) LIMIT 1").get(`%${d.tipo}%`);
      if (!tipo) {
        return res.redirect(`/brindes/orcamentos/${orcamento_id}?erro=` + encodeURIComponent(`Tipo de personalização "${d.tipo}" não cadastrado`));
      }

      const sug = getPersonalizacaoSugestao(tipo.id, qtd);
      const custoUnit = Number(sug.custo_unitario || 0);

      let setup = 0;
      if (Number(tipo.cobra_setup || 0)) {
        if (!setupUsadoPorTipo.has(tipo.id)) {
          setup = Number(sug.setup_padrao || 0);
          setupUsadoPorTipo.add(tipo.id);
        }
      }

      const totalPers = (custoUnit * qtd) + setup;
      db.prepare(`
        INSERT INTO orcamentos_brindes_itens_personalizacoes
        (item_id, tipo_id, cores, posicao, tamanho, prazo_dias, setup_valor, custo_unit_personalizacao, total_personalizacao)
        VALUES (?,?,?,?,?,?,?,?,?)
      `).run(
        itemId,
        tipo.id,
        (d.cores == null ? null : Number(d.cores)),
        d.posicao || null,
        null,
        Math.max(0, Math.floor(Number(tipo.prazo_dias || 0))),
        setup,
        custoUnit,
        totalPers
      );
    }

    recalcItemOrcamentoBrindes(itemId);
  } catch (e) {
    return res.redirect(`/brindes/orcamentos/${orcamento_id}?erro=` + encodeURIComponent(e.message || e));
  }

  res.redirect(`/brindes/orcamentos/${orcamento_id}#item-${itemId}`);
});

// Pacotes dinâmicos (salvar/aplicar)
app.post("/brindes/orcamentos/:id(\\d+)/itens/:itemId(\\d+)/pacote/aplicar", (req, res) => {
  const orcamento_id = Number(req.params.id);
  const itemId = Number(req.params.itemId);
  const { pacote_id, modo } = req.body;

  const item = db.prepare("SELECT id, quantidade FROM orcamentos_brindes_itens WHERE id = ? AND orcamento_id = ?").get(itemId, orcamento_id);
  if (!item) return res.redirect(`/brindes/orcamentos/${orcamento_id}?erro=` + encodeURIComponent("Item não encontrado"));

  try {
    ensureDefaultBrindePersonalizacoes();
    const pid = Number(pacote_id);
    const pacote = getPacotePersonalizacao(pid);
    if (!pacote || Number(pacote.ativo || 0) !== 1) {
      return res.redirect(`/brindes/orcamentos/${orcamento_id}?erro=` + encodeURIComponent("Pacote inválido ou inativo"));
    }

    const replace = String(modo || "substituir").toLowerCase() !== "adicionar";
    if (replace) db.prepare("DELETE FROM orcamentos_brindes_itens_personalizacoes WHERE item_id = ?").run(itemId);

    const qtd = Math.max(1, Math.floor(Number(item.quantidade || 1)));
    const setupUsadoPorTipo = new Set();

    for (const d of (pacote.itens || [])) {
      const tipo = db.prepare("SELECT id, cobra_setup, COALESCE(prazo_dias,0) AS prazo_dias FROM brindes_personalizacao_tipos WHERE id=?").get(Number(d.tipo_id));
      if (!tipo) continue;

      const sug = getPersonalizacaoSugestao(tipo.id, qtd);
      const custoUnit = (d.custo_unit_override == null) ? Number(sug.custo_unitario || 0) : Number(d.custo_unit_override || 0);

      let setup = 0;
      const overrideSetup = (d.setup_override != null);
      if (overrideSetup) {
        setup = Number(d.setup_override || 0);
        if (Number(tipo.cobra_setup || 0) === 0) setup = 0; // só mantém setup se o tipo cobra
      } else {
        if (Number(tipo.cobra_setup || 0)) {
          if (!setupUsadoPorTipo.has(tipo.id)) {
            setup = Number(sug.setup_padrao || 0);
            setupUsadoPorTipo.add(tipo.id);
          }
        }
      }

      const prazo = (d.prazo_dias_override == null) ? Math.max(0, Math.floor(Number(tipo.prazo_dias || 0))) : Math.max(0, Math.floor(Number(d.prazo_dias_override || 0)));
      const totalPers = (custoUnit * qtd) + setup;

      db.prepare(`
        INSERT INTO orcamentos_brindes_itens_personalizacoes
        (item_id, tipo_id, cores, posicao, tamanho, prazo_dias, setup_valor, custo_unit_personalizacao, total_personalizacao)
        VALUES (?,?,?,?,?,?,?,?,?)
      `).run(
        itemId,
        tipo.id,
        (d.cores == null) ? null : Math.max(0, Math.floor(Number(d.cores))),
        (String(d.posicao || "").trim() || null),
        (String(d.tamanho || "").trim() || null),
        prazo,
        setup,
        custoUnit,
        totalPers
      );
    }

    recalcItemOrcamentoBrindes(itemId);
  } catch (e) {
    return res.redirect(`/brindes/orcamentos/${orcamento_id}?erro=` + encodeURIComponent(e.message || e));
  }

  res.redirect(`/brindes/orcamentos/${orcamento_id}#item-${itemId}`);
});

app.post("/brindes/orcamentos/:id(\\d+)/itens/:itemId(\\d+)/pacote/salvar", (req, res) => {
  const orcamento_id = Number(req.params.id);
  const itemId = Number(req.params.itemId);
  const { nome, descricao } = req.body;

  const item = db.prepare("SELECT id FROM orcamentos_brindes_itens WHERE id = ? AND orcamento_id = ?").get(itemId, orcamento_id);
  if (!item) return res.redirect(`/brindes/orcamentos/${orcamento_id}?erro=` + encodeURIComponent("Item não encontrado"));

  try {
    const n = String(nome || "").trim();
    if (!n) return res.redirect(`/brindes/orcamentos/${orcamento_id}?erro=` + encodeURIComponent("Nome do pacote é obrigatório"));

    const pers = db.prepare(`
      SELECT p.*
      FROM orcamentos_brindes_itens_personalizacoes p
      WHERE p.item_id = ?
      ORDER BY p.id ASC
    `).all(itemId);

    if (!pers || pers.length === 0) {
      return res.redirect(`/brindes/orcamentos/${orcamento_id}?erro=` + encodeURIComponent("Este item não tem personalizações para salvar"));
    }

    const tx = db.transaction(() => {
      const r = db.prepare("INSERT INTO brindes_personalizacao_pacotes (nome, descricao, ativo) VALUES (?,?,1)").run(
        n,
        (String(descricao || "").trim() || null)
      );
      const pid = Number(r.lastInsertRowid);
      let ordem = 1;
      for (const p of pers) {
        db.prepare(`
          INSERT INTO brindes_personalizacao_pacote_itens
          (pacote_id, tipo_id, cores, posicao, tamanho, prazo_dias_override, setup_override, custo_unit_override, ordem)
          VALUES (?,?,?,?,?,?,?,?,?)
        `).run(
          pid,
          Number(p.tipo_id),
          (p.cores == null ? null : Number(p.cores)),
          p.posicao || null,
          p.tamanho || null,
          (p.prazo_dias == null ? null : Number(p.prazo_dias)),
          (p.setup_valor == null ? null : Number(p.setup_valor)),
          (p.custo_unit_personalizacao == null ? null : Number(p.custo_unit_personalizacao)),
          ordem++
        );
      }
      return pid;
    });

    tx();
  } catch (e) {
    return res.redirect(`/brindes/orcamentos/${orcamento_id}?erro=` + encodeURIComponent(e.message || e));
  }

  res.redirect(`/brindes/orcamentos/${orcamento_id}?ok=` + encodeURIComponent("Pacote salvo e disponível para usar"));
});

app.post("/brindes/orcamentos/:id(\\d+)/itens/:itemId(\\d+)/personalizacoes", (req, res) => {
  const orcamento_id = Number(req.params.id);
  const itemId = Number(req.params.itemId);
  const { tipo_id, cores, posicao, tamanho, custo_unit_personalizacao, setup_valor } = req.body;

  try {
    const item = db.prepare("SELECT id, quantidade FROM orcamentos_brindes_itens WHERE id = ? AND orcamento_id = ?").get(itemId, orcamento_id);
    if (!item) return res.redirect(`/brindes/orcamentos/${orcamento_id}?erro=` + encodeURIComponent("Item não encontrado"));

    const tipoId = Number(tipo_id);
    const tipo = db.prepare("SELECT id, cobra_setup, COALESCE(prazo_dias,0) AS prazo_dias FROM brindes_personalizacao_tipos WHERE id = ?").get(tipoId);
    if (!tipo) return res.redirect(`/brindes/orcamentos/${orcamento_id}?erro=` + encodeURIComponent("Tipo de personalização inválido"));

    const sugestao = getPersonalizacaoSugestao(tipoId, Number(item.quantidade || 1));

    const custoUnit = (custo_unit_personalizacao === "" || custo_unit_personalizacao == null)
      ? Number(sugestao.custo_unitario || 0)
      : Number(custo_unit_personalizacao || 0);

    let setup = (setup_valor === "" || setup_valor == null)
      ? Number(sugestao.setup_padrao || 0)
      : Number(setup_valor || 0);
    if (Number(tipo.cobra_setup || 0) === 0 && (setup_valor === "" || setup_valor == null)) setup = 0;

    const qtd = Math.max(1, Math.floor(Number(item.quantidade || 1)));
    const totalPers = (custoUnit * qtd) + setup;

    db.prepare(`
      INSERT INTO orcamentos_brindes_itens_personalizacoes
      (item_id, tipo_id, cores, posicao, tamanho, prazo_dias, setup_valor, custo_unit_personalizacao, total_personalizacao)
      VALUES (?,?,?,?,?,?,?,?,?)
    `).run(
      itemId,
      tipoId,
      (cores === "" || cores == null) ? null : Math.max(0, Math.floor(Number(cores))),
      (posicao || "").trim() || null,
      (tamanho || "").trim() || null,
      Math.max(0, Math.floor(Number(tipo.prazo_dias || 0))),
      setup,
      custoUnit,
      totalPers
    );

    recalcItemOrcamentoBrindes(itemId);
  } catch (e) {
    return res.redirect(`/brindes/orcamentos/${orcamento_id}?erro=` + encodeURIComponent(e.message || e));
  }

  res.redirect(`/brindes/orcamentos/${orcamento_id}`);
});

app.post("/brindes/orcamentos/:id(\\d+)/itens/:itemId(\\d+)/personalizacoes/:pid(\\d+)/remover", (req, res) => {
  const orcamento_id = Number(req.params.id);
  const itemId = Number(req.params.itemId);
  const pid = Number(req.params.pid);
  try {
    db.prepare("DELETE FROM orcamentos_brindes_itens_personalizacoes WHERE id = ? AND item_id = ?").run(pid, itemId);
    recalcItemOrcamentoBrindes(itemId);
  } catch (e) {}
  res.redirect(`/brindes/orcamentos/${orcamento_id}`);
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



// =====================
// INTEGRAÇÕES - MERCADO LIVRE (1 conta por instalação)
// =====================
// requireAuth/requireModule já foram importados no topo (middlewares/auth)

function nowText() {
  const d = new Date();
  const pad = (n) => String(n).padStart(2, "0");
  return `${d.getFullYear()}-${pad(d.getMonth()+1)}-${pad(d.getDate())} ${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`;
}

function getMlCfgRow() {
  try {
    const row = db.prepare("SELECT * FROM integracao_ml WHERE id=1").get();
    return row || null;
  } catch (_) {
    return null;
  }
}

function upsertMlCfg(patch = {}) {
  const current = getMlCfgRow() || { id: 1 };
  const next = { ...current, ...patch, id: 1, atualizado_em: nowText() };
  if (!next.webhook_secret) next.webhook_secret = crypto.randomBytes(16).toString("hex");
  if (next.sync_lookback_days == null) next.sync_lookback_days = current.sync_lookback_days ?? 7;
  if (next.sync_limit == null) next.sync_limit = current.sync_limit ?? 50;
  try {
    db.prepare(
      `INSERT INTO integracao_ml (
        id, client_id, client_secret, redirect_uri,
        access_token, refresh_token, token_expires_at,
        seller_id, site_id, webhook_secret, sync_lookback_days, sync_limit, last_sync_at,
        criado_em, atualizado_em
      ) VALUES (1,?,?,?,?,?,?,?,?,?,?,?,COALESCE(?,?),?)
      ON CONFLICT(id) DO UPDATE SET
        client_id=excluded.client_id,
        client_secret=excluded.client_secret,
        redirect_uri=excluded.redirect_uri,
        access_token=excluded.access_token,
        refresh_token=excluded.refresh_token,
        token_expires_at=excluded.token_expires_at,
        seller_id=excluded.seller_id,
        site_id=excluded.site_id,
        webhook_secret=excluded.webhook_secret,
        sync_lookback_days=excluded.sync_lookback_days,
        sync_limit=excluded.sync_limit,
        last_sync_at=excluded.last_sync_at,
        atualizado_em=excluded.atualizado_em
      `
    ).run(
      next.client_id || null,
      next.client_secret || null,
      next.redirect_uri || null,
      next.access_token || null,
      next.refresh_token || null,
      next.token_expires_at || null,
      next.seller_id || null,
      next.site_id || null,
      next.webhook_secret || null,
      Number(next.sync_lookback_days || 7) || 7,
      Number(next.sync_limit || 50) || 50,
      next.last_sync_at || null,
      next.criado_em || nowText(),
      next.criado_em || nowText(),
      next.atualizado_em || nowText()
    );
  } catch (e) {
    try { db.prepare("DELETE FROM integracao_ml WHERE id=1").run(); } catch (_) {}
    db.prepare(
      `INSERT INTO integracao_ml (
        id, client_id, client_secret, redirect_uri,
        access_token, refresh_token, token_expires_at,
        seller_id, site_id, webhook_secret, sync_lookback_days, sync_limit, last_sync_at,
        criado_em, atualizado_em
      ) VALUES (1,?,?,?,?,?,?,?,?,?,?,?, ?,?)`
    ).run(
      next.client_id || null,
      next.client_secret || null,
      next.redirect_uri || null,
      next.access_token || null,
      next.refresh_token || null,
      next.token_expires_at || null,
      next.seller_id || null,
      next.site_id || null,
      next.webhook_secret || null,
      Number(next.sync_lookback_days || 7) || 7,
      Number(next.sync_limit || 50) || 50,
      next.last_sync_at || null,
      next.criado_em || nowText(),
      next.atualizado_em || nowText()
    );
  }
  return next;
}

function mlHttpRequest({ method, hostname, path, headers, body }) {
  return new Promise((resolve, reject) => {
    const req = https.request(
      { method, hostname, path, headers: headers || {} },
      (res) => {
        let data = "";
        res.on("data", (c) => (data += c));
        res.on("end", () => {
          let json = null;
          try { json = data ? JSON.parse(data) : null; } catch (_) {}
          if (res.statusCode >= 200 && res.statusCode < 300) return resolve(json);
          const msg = (json && (json.message || json.error || json.error_description)) || (data ? String(data).slice(0, 400) : `HTTP ${res.statusCode}`);
          const err = new Error(msg);
          err.status = res.statusCode;
          err.payload = json;
          return reject(err);
        });
      }
    );
    req.on("error", reject);
    if (body) req.write(body);
    req.end();
  });
}

async function mlOauthToken(bodyObj) {
  const form = querystring.stringify(bodyObj || {});
  return mlHttpRequest({
    method: "POST",
    hostname: "api.mercadolibre.com",
    path: "/oauth/token",
    headers: {
      "Content-Type": "application/x-www-form-urlencoded",
      "Accept": "application/json",
      "Content-Length": Buffer.byteLength(form),
    },
    body: form,
  });
}

async function mlApiGet(path, accessToken) {
  return mlHttpRequest({
    method: "GET",
    hostname: "api.mercadolibre.com",
    path,
    headers: {
      "Accept": "application/json",
      "Authorization": `Bearer ${accessToken}`,
    },
  });
}

async function mlApiPut(path, accessToken, bodyObj) {
  const url = "https://api.mercadolibre.com" + path;
  const resp = await fetch(url, {
    method: "PUT",
    headers: { "Authorization": "Bearer " + accessToken, "Content-Type": "application/json" },
    body: JSON.stringify(bodyObj || {}),
  });
  const text = await resp.text();
  let data;
  try { data = JSON.parse(text); } catch { data = { raw: text }; }
  if (!resp.ok) {
    const e = new Error(`ML HTTP ${resp.status}: ${text}`);
    e.status = resp.status;
    e.payload = data;
    throw e;
  }
  return data;
}


async function ensureMlAccessToken() {
  const cfg = getMlCfgRow();
  if (!cfg || !cfg.client_id || !cfg.client_secret) {
    throw new Error("Mercado Livre não configurado: informe Client ID/Secret.");
  }

  const expiresAt = Number(cfg.token_expires_at || 0) || 0;
  const now = Date.now();
  if (cfg.access_token && expiresAt && now < (expiresAt - 60_000)) return cfg.access_token;

  if (cfg.refresh_token) {
    const refreshed = await mlOauthToken({
      grant_type: "refresh_token",
      client_id: cfg.client_id,
      client_secret: cfg.client_secret,
      refresh_token: cfg.refresh_token,
    });
    const access_token = String(refreshed?.access_token || "");
    const refresh_token = String(refreshed?.refresh_token || cfg.refresh_token || "");
    const expires_in = Number(refreshed?.expires_in || 3600);
    if (!access_token) throw new Error("Mercado Livre: refresh não retornou access_token");
    const token_expires_at = String(Date.now() + Math.max(60, expires_in) * 1000);
    upsertMlCfg({ access_token, refresh_token, token_expires_at });
    return access_token;
  }

  throw new Error("Mercado Livre: conecte a conta (OAuth) para gerar tokens.");
}

function mapMlStatusToPedido(order) {
  const st = String(order?.status || "").toLowerCase();
  if (st === "cancelled") return "CANCELADO";
  // Status do envio costuma ser mais confiável para entrega
  const shipSt = String(order?.shipping?.status || "").toLowerCase();
  if (shipSt === "delivered") return "ENTREGUE";
  if (st === "paid" || st === "confirmed") {
    if (["ready_to_ship", "handling", "shipped"].includes(shipSt)) return "EM_PRODUCAO";
    return "APROVADO";
  }
  return "ORCAMENTO"; // payment_required / etc.
}


function upsertIntegracaoCliente({ canal, external_cliente_id, cliente_id, raw_json }) {
  try {
    db.prepare(
      `INSERT INTO integracao_clientes (canal, external_cliente_id, cliente_id, raw_json, atualizado_em)
       VALUES (?,?,?,?,?)
       ON CONFLICT(canal, external_cliente_id) DO UPDATE SET
         cliente_id=COALESCE(excluded.cliente_id, integracao_clientes.cliente_id),
         raw_json=excluded.raw_json,
         atualizado_em=excluded.atualizado_em`
    ).run(
      canal,
      String(external_cliente_id),
      cliente_id || null,
      raw_json ? JSON.stringify(raw_json) : null,
      nowText()
    );
  } catch (e) {
    // fallback sqlite antigo
    try {
      const row = db.prepare("SELECT id FROM integracao_clientes WHERE canal=? AND external_cliente_id=?").get(canal, String(external_cliente_id));
      if (row?.id) {
        db.prepare("UPDATE integracao_clientes SET cliente_id=COALESCE(?,cliente_id), raw_json=?, atualizado_em=? WHERE id=?")
          .run(cliente_id || null, raw_json ? JSON.stringify(raw_json) : null, nowText(), row.id);
      } else {
        db.prepare("INSERT INTO integracao_clientes (canal, external_cliente_id, cliente_id, raw_json, atualizado_em) VALUES (?,?,?,?,?)")
          .run(canal, String(external_cliente_id), cliente_id || null, raw_json ? JSON.stringify(raw_json) : null, nowText());
      }
    } catch (_) {}
  }
}

function findClienteByExternalId(canal, externalClienteId) {
  try {
    return db.prepare("SELECT * FROM integracao_clientes WHERE canal=? AND external_cliente_id=?").get(canal, String(externalClienteId));
  } catch (_) {
    return null;
  }
}

function upsertClienteBasico({ nome, telefone, email }) {
  const n = String(nome || "").trim() || "Consumidor Final";
  const tel = telefone ? String(telefone).trim() : null;
  const em = email ? String(email).trim() : null;

  // tenta achar por email, depois por telefone+nome
  let c = null;
  if (em) {
    c = db.prepare("SELECT * FROM clientes WHERE email=? ORDER BY id DESC LIMIT 1").get(em);
  }
  if (!c && tel) {
    c = db.prepare("SELECT * FROM clientes WHERE (telefone=? OR whatsapp=?) AND (nome=? OR razao_social=?) ORDER BY id DESC LIMIT 1")
      .get(tel, tel, n, n);
  }
  if (c?.id) {
    try {
      db.prepare("UPDATE clientes SET nome=COALESCE(?,nome), telefone=COALESCE(?,telefone), whatsapp=COALESCE(?,whatsapp), email=COALESCE(?,email), atualizado_em=datetime('now') WHERE id=?")
        .run(n, tel, tel, em, c.id);
    } catch (_) {}
    return Number(c.id);
  }

  const ins = db.prepare("INSERT INTO clientes (tipo, nome, email, telefone, whatsapp, ativo, criado_em, atualizado_em) VALUES ('PF', ?, ?, ?, ?, 1, datetime('now'), datetime('now'))")
    .run(n, em, tel, tel);
  return Number(ins.lastInsertRowid);
}

function upsertClienteEnderecoPrincipal(clienteId, addr) {
  if (!clienteId) return;
  const a = addr || {};
  const cep = a.cep ? String(a.cep).replace(/\D/g, "").slice(0, 8) : null;
  const logradouro = a.logradouro ? String(a.logradouro) : null;
  const numero = a.numero ? String(a.numero) : null;
  const complemento = a.complemento ? String(a.complemento) : null;
  const bairro = a.bairro ? String(a.bairro) : null;
  const cidade = a.cidade ? String(a.cidade) : null;
  const uf = a.uf ? String(a.uf) : null;

  try {
    const ex = db.prepare("SELECT * FROM cliente_enderecos WHERE cliente_id=? AND principal=1 ORDER BY id DESC LIMIT 1").get(clienteId);
    if (ex?.id) {
      db.prepare("UPDATE cliente_enderecos SET cep=COALESCE(?,cep), logradouro=COALESCE(?,logradouro), numero=COALESCE(?,numero), complemento=COALESCE(?,complemento), bairro=COALESCE(?,bairro), cidade=COALESCE(?,cidade), uf=COALESCE(?,uf), atualizado_em=datetime('now') WHERE id=?")
        .run(cep, logradouro, numero, complemento, bairro, cidade, uf, ex.id);
      return;
    }
    db.prepare("INSERT INTO cliente_enderecos (cliente_id, tipo, cep, logradouro, numero, complemento, bairro, cidade, uf, principal, criado_em, atualizado_em) VALUES (?,?,?,?,?,?,?,?,?,1, datetime('now'), datetime('now'))")
      .run(clienteId, "ENTREGA", cep, logradouro, numero, complemento, bairro, cidade, uf);
  } catch (_) {}
}

function upsertIntegracaoCatalogoProduto({ canal, catalogo_produto_id, external_item_id, external_sku, raw_json }) {
  try {
    db.prepare(
      `INSERT INTO integracao_catalogo_produtos (canal, catalogo_produto_id, external_item_id, external_sku, raw_json, last_sync_at, atualizado_em)
       VALUES (?,?,?,?,?,?,?)
       ON CONFLICT(canal, catalogo_produto_id) DO UPDATE SET
         external_item_id=COALESCE(excluded.external_item_id, integracao_catalogo_produtos.external_item_id),
         external_sku=COALESCE(excluded.external_sku, integracao_catalogo_produtos.external_sku),
         raw_json=COALESCE(excluded.raw_json, integracao_catalogo_produtos.raw_json),
         atualizado_em=excluded.atualizado_em`
    ).run(
      canal,
      Number(catalogo_produto_id),
      external_item_id ? String(external_item_id) : null,
      external_sku ? String(external_sku) : null,
      raw_json ? JSON.stringify(raw_json) : null,
      nowText(),
      nowText()
    );
  } catch (_) {}
}

function listCatalogoProdutos() {
  try {
    return db.prepare("SELECT * FROM catalogo_produtos ORDER BY nome").all();
  } catch (_) {
    return [];
  }
}

function listCatalogoProdutosMapeados(canal) {
  try {
    return db.prepare("SELECT * FROM integracao_catalogo_produtos WHERE canal=?").all(canal);
  } catch (_) {
    return [];
  }
}

function round2(n) {
  const x = Number(n || 0);
  if (!Number.isFinite(x)) return 0;
  return Math.round((x + Number.EPSILON) * 100) / 100;
}

/**
 * Preço final para marketplaces:
 * - se preco_por_m2=1: usa dimensões padrão (largura_mm/altura_mm) para calcular area_m2 e aplica preco_m2
 * - fallback: preco_venda
 */
function calcCatalogoPrecoFinalParaMarketplace(p) {
  const precoVenda = Number(p.preco_venda || 0) || 0;
  const porM2 = Number(p.preco_por_m2 || 0) === 1;
  if (!porM2) return round2(precoVenda);

  const precoM2 = Number(p.preco_m2 || 0) || 0;
  const larg = Number(p.largura_mm || 0) || 0;
  const alt = Number(p.altura_mm || 0) || 0;

  if (precoM2 > 0 && larg > 0 && alt > 0) {
    const areaM2 = (larg * alt) / 1_000_000; // mm² -> m²
    const final = areaM2 * precoM2;
    if (final > 0) return round2(final);
  }
  return round2(precoVenda);
}

/**
 * Reserva de estoque: soma quantidades dos pedidos ainda não "comprometidos" (sem baixa automática)
 * Como a baixa automática ocorre ao mover para APROVADO, reservamos apenas RASCUNHO/ORCAMENTO.
 */
function getReservaEstoqueCatalogoProduto(produtoId) {
  try {
    const row = db.prepare(`
      SELECT COALESCE(SUM(i.quantidade),0) AS reservado
      FROM pedido_itens i
      JOIN pedidos p ON p.id = i.pedido_id
      WHERE i.produto_id = ?
        AND UPPER(COALESCE(p.status,'')) IN ('RASCUNHO','ORCAMENTO')
    `).get(Number(produtoId));
    return Number(row?.reservado || 0) || 0;
  } catch (_) {
    return 0;
  }
}

function calcEstoqueDisponivelParaMarketplace(p) {
  const controla = Number(p.controla_estoque || 0) === 1;
  if (!controla) return null; // não envia estoque
  const atual = Number(p.estoque_atual || 0) || 0;
  const reservado = getReservaEstoqueCatalogoProduto(p.id);
  const disponivel = Math.max(0, Math.floor(atual - reservado));
  return disponivel;
}

function formatMlReceiverAddress(order) {
  const a = order?.shipping?.receiver_address || null;
  if (!a) return null;
  const parts = [];
  const line1 = [a.street_name, a.street_number].filter(Boolean).join(", ");
  if (line1) parts.push(line1);
  if (a.comment) parts.push(String(a.comment));
  const line2 = [a.neighborhood?.name, a.city?.name, a.state?.name].filter(Boolean).join(" - ");
  if (line2) parts.push(line2);
  if (a.zip_code) parts.push(`CEP ${a.zip_code}`);
  return parts.join(" | ") || null;
}

function upsertIntegracaoPedido({ canal, external_order_id, pedido_id, external_status, raw_json }) {
  try {
    db.prepare(
      `INSERT INTO integracao_pedidos (canal, external_order_id, pedido_id, external_status, last_update_at, raw_json)
       VALUES (?,?,?,?,?,?)
       ON CONFLICT(canal, external_order_id) DO UPDATE SET
         pedido_id=COALESCE(excluded.pedido_id, integracao_pedidos.pedido_id),
         external_status=excluded.external_status,
         last_update_at=excluded.last_update_at,
         raw_json=excluded.raw_json`
    ).run(
      canal,
      String(external_order_id),
      pedido_id || null,
      external_status || null,
      nowText(),
      raw_json ? JSON.stringify(raw_json) : null
    );
  } catch (e) {
    try {
      const exists = db.prepare("SELECT id FROM integracao_pedidos WHERE canal=? AND external_order_id=?").get(canal, String(external_order_id));
      if (exists?.id) {
        db.prepare("UPDATE integracao_pedidos SET pedido_id=COALESCE(?,pedido_id), external_status=?, last_update_at=?, raw_json=? WHERE id=?")
          .run(pedido_id || null, external_status || null, nowText(), raw_json ? JSON.stringify(raw_json) : null, exists.id);
      } else {
        db.prepare("INSERT INTO integracao_pedidos (canal, external_order_id, pedido_id, external_status, last_update_at, raw_json) VALUES (?,?,?,?,?,?)")
          .run(canal, String(external_order_id), pedido_id || null, external_status || null, nowText(), raw_json ? JSON.stringify(raw_json) : null);
      }
    } catch (_) {}
  }
}

function findPedidoByExternalId(externalId) {
  try {
    const row = db.prepare("SELECT * FROM integracao_pedidos WHERE canal='MERCADOLIVRE' AND external_order_id=?").get(String(externalId));
    return row || null;
  } catch (_) {
    return null;
  }
}

async function createOrUpdatePedidoFromMlOrder(order) {
  const externalId = String(order?.id || "").trim();
  if (!externalId) return { ok: false, error: "Pedido ML sem id" };

  const existingMap = findPedidoByExternalId(externalId);
  const status = mapMlStatusToPedido(order);
  const buyer = order?.buyer || {};
  const nome = [buyer.first_name, buyer.last_name].filter(Boolean).join(" ").trim() || buyer.nickname || "Consumidor Final";
  const phone = buyer?.phone?.number ? String(buyer.phone.number) : (buyer?.phone ? String(buyer.phone) : null);
  const enderecoTxt = formatMlReceiverAddress(order);

  // cria/atualiza cliente local para NF-e (nome/telefone/endereço)
  let cliente_id = null;
  try {
    const buyerId = buyer?.id ? String(buyer.id) : null;
    if (buyerId) {
      const mapCli = findClienteByExternalId("MERCADOLIVRE", buyerId);
      if (mapCli?.cliente_id) {
        cliente_id = Number(mapCli.cliente_id);
      } else {
        // busca dados extras do comprador quando possível
        let buyerFull = null;
        try { buyerFull = await mlApiGet(`/users/${encodeURIComponent(buyerId)}`, await ensureMlAccessToken()); } catch (_) {}
        const email = buyerFull?.email || null;
        cliente_id = upsertClienteBasico({ nome, telefone: phone, email });
        // endereço principal (usa receiver/shipping do pedido)
        const a = (order?.shipping?.receiver_address) || (order?.receiver_address) || {};
        upsertClienteEnderecoPrincipal(cliente_id, {
          cep: a.zip_code || a.zipcode,
          logradouro: a.street_name || a.address_line,
          numero: a.street_number,
          complemento: a.comment,
          bairro: a.neighborhood?.name,
          cidade: a.city?.name,
          uf: a.state?.id ? String(a.state.id).replace(/^BR-/, "") : (a.state?.name || null),
        });
        upsertIntegracaoCliente({ canal: "MERCADOLIVRE", external_cliente_id: buyerId, cliente_id, raw_json: buyerFull || buyer });
      }
    }
  } catch (_) {}

  const items = Array.isArray(order?.order_items) ? order.order_items : [];
  const subtotal = items.reduce((acc, it) => {
    const q = Number(it?.quantity || 0) || 0;
    const p = Number(it?.unit_price || 0) || 0;
    return acc + (q * p);
  }, 0);
  const total = Number(order?.total_amount || subtotal) || subtotal;

  if (existingMap?.pedido_id) {
    try {
      db.prepare("UPDATE pedidos SET status=?, canal_venda='MERCADOLIVRE', subtotal_itens=?, total=?, cliente_id=COALESCE(?,cliente_id), cliente_nome_avulso=COALESCE(?,cliente_nome_avulso), cliente_telefone_avulso=COALESCE(?,cliente_telefone_avulso), endereco_entrega_texto=COALESCE(?,endereco_entrega_texto), atualizado_em=? WHERE id=?")
        .run(status, subtotal, total, cliente_id || null, nome || null, phone || null, enderecoTxt || null, nowText(), existingMap.pedido_id);
    } catch (_) {}
    upsertIntegracaoPedido({ canal: "MERCADOLIVRE", external_order_id: externalId, pedido_id: existingMap.pedido_id, external_status: String(order?.status || ""), raw_json: order });
    return { ok: true, pedido_id: existingMap.pedido_id, updated: true };
  }

  const tx = db.transaction(() => {
    const ins = db.prepare(
      `INSERT INTO pedidos (
        cliente_id, cliente_nome_avulso, cliente_telefone_avulso,
        cliente_nome, cliente_contato, vendedor_nome, prazo_entrega, data_venda, prioridade,
        status, tipo_venda, data_validade, canal_venda, tipo_entrega, endereco_entrega_texto, endereco_entrega_id,
        subtotal_itens, desconto_tipo, desconto_valor, frete_valor, total,
        natureza_operacao, consumidor_final, presenca_comprador, observacao_fiscal,
        observacoes_vendedor, observacoes_internas, op_id, anexo_arquivo,
        criado_em, atualizado_em
      ) VALUES (
        NULL, ?, ?,
        ?, ?, NULL, NULL, ?, 'NORMAL',
        ?, 'PRODUTO', NULL, 'MERCADOLIVRE', 'ENTREGA', ?, NULL,
        ?, 'VALOR', 0, 0, ?,
        'VENDA', 1, 'INTERNET', ?,
        ?, NULL, NULL, NULL,
        ?, ?
      )`
    );

    const dataVenda = (order?.date_created ? String(order.date_created).slice(0, 19).replace('T',' ') : nowText());
    const obsFiscal = `Pedido Mercado Livre #${externalId}`;
    const obsVend = `Importado do Mercado Livre. Status: ${String(order?.status || '')}`;

    const r = ins.run(
      nome,
      phone,
      nome,
      null,
      dataVenda,
      status,
      enderecoTxt,
      subtotal,
      total,
      obsFiscal,
      obsVend,
      nowText(),
      nowText()
    );
    const pedidoId = r.lastInsertRowid;

    const insItem = db.prepare(
      `INSERT INTO pedido_itens (
        pedido_id, descricao, quantidade, unidade, observacao,
        largura, altura, unidade_dim, area_m2,
        preco_unit, desconto_item, total_item, criado_em
      ) VALUES (?,?,?,?, NULL, NULL, NULL, 'MM', 0, ?, 0, ?, ?)`
    );
    for (const it of items) {
      const title = it?.item?.title || it?.item?.id || "Item";
      const q = Number(it?.quantity || 0) || 0;
      const p = Number(it?.unit_price || 0) || 0;
      const tot = q * p;
      if (q > 0) insItem.run(pedidoId, String(title), q, "UN", p, tot, nowText());
    }

    if (String(order?.status || "").toLowerCase() === "paid") {
      try {
        db.prepare("INSERT INTO pedido_pagamentos (pedido_id, forma, parcelas, valor, data_prevista, obs) VALUES (?,?,?,?,?,?)")
          .run(pedidoId, "MERCADOLIVRE", 1, total, nowText().slice(0, 10), "Pago no Mercado Livre");
      } catch (_) {}
    }

    upsertIntegracaoPedido({ canal: "MERCADOLIVRE", external_order_id: externalId, pedido_id: pedidoId, external_status: String(order?.status || ""), raw_json: order });
    return pedidoId;
  });

  const pedidoId = tx();
  return { ok: true, pedido_id: pedidoId, created: true };
}

const mlOauthState = { value: null, createdAt: 0 };

app.get("/integracoes/mercadolivre", requireAuth, requireModule("integracoes"), async (req, res) => {
  try {
    await ready;
    const cfg = getMlCfgRow();
    const proto = (req.headers["x-forwarded-proto"] || req.protocol || "http").split(",")[0].trim();
    const host = req.get("host");
    const suggestedRedirect = `${proto}://${host}/integracoes/mercadolivre/oauth/callback`;
    const webhookUrl = `${proto}://${host}/webhooks/mercadolivre`;

    const catalogoProdutos = listCatalogoProdutos();
    const catalogoMap = listCatalogoProdutosMapeados("MERCADOLIVRE");
    return res.render("layout", {
      view: "integracao-mercadolivre-page",
      title: "Mercado Livre",
      pageTitle: "Mercado Livre",
      pageSubtitle: "Conecte e sincronize pedidos",
      activeMenu: "ml",
      cfg,
      suggestedRedirect,
      webhookUrl,
      ok: req.query.ok ? String(req.query.ok) : null,
      err: req.query.err ? String(req.query.err) : null,
      catalogoProdutos,
      catalogoMap,
    });
  } catch (e) {
    return res.status(500).send(e.message || String(e));
  }
});

app.post("/integracoes/mercadolivre/salvar", requireAuth, requireModule("integracoes"), uploadNone.none(), (req, res) => {
  try {
    const client_id = String(req.body.client_id || "").trim();
    const client_secret = String(req.body.client_secret || "").trim();
    const redirect_uri = String(req.body.redirect_uri || "").trim();
    const sync_lookback_days = Number(req.body.sync_lookback_days || 7) || 7;
    const sync_limit = Math.min(200, Math.max(10, Number(req.body.sync_limit || 50) || 50));
    const regenSecret = String(req.body.regen_secret || "").trim() === "1";
    const webhook_secret = regenSecret ? crypto.randomBytes(16).toString("hex") : undefined;
    upsertMlCfg({ client_id, client_secret, redirect_uri, sync_lookback_days, sync_limit, ...(webhook_secret ? { webhook_secret } : {}) });
    return res.redirect("/integracoes/mercadolivre?ok=" + encodeURIComponent("Configuração salva."));
  } catch (e) {
    return res.redirect("/integracoes/mercadolivre?err=" + encodeURIComponent(e.message || "Falha ao salvar"));
  }
});

app.get("/integracoes/mercadolivre/oauth/iniciar", requireAuth, requireModule("integracoes"), async (req, res) => {
  try {
    await ready;
    const cfg = getMlCfgRow();
    if (!cfg?.client_id || !cfg?.client_secret) {
      return res.redirect("/integracoes/mercadolivre?err=" + encodeURIComponent("Informe Client ID e Client Secret antes de conectar."));
    }
    const proto = (req.headers["x-forwarded-proto"] || req.protocol || "http").split(",")[0].trim();
    const host = req.get("host");
    const redirectUri = (cfg.redirect_uri || `${proto}://${host}/integracoes/mercadolivre/oauth/callback`).trim();
    const state = crypto.randomBytes(16).toString("hex");
    mlOauthState.value = state;
    mlOauthState.createdAt = Date.now();
    const authUrl =
      "https://auth.mercadolivre.com.br/authorization" +
      "?response_type=code" +
      "&client_id=" + encodeURIComponent(cfg.client_id) +
      "&redirect_uri=" + encodeURIComponent(redirectUri) +
      "&state=" + encodeURIComponent(state);
    return res.redirect(authUrl);
  } catch (e) {
    return res.redirect("/integracoes/mercadolivre?err=" + encodeURIComponent(e.message || "Falha ao iniciar OAuth"));
  }
});

app.get("/integracoes/mercadolivre/oauth/callback", requireAuth, requireModule("integracoes"), async (req, res) => {
  try {
    await ready;
    const cfg = getMlCfgRow();
    const code = String(req.query.code || "").trim();
    const state = String(req.query.state || "").trim();
    if (!code) throw new Error("Callback sem code");
    const stOk = mlOauthState.value && state && mlOauthState.value === state && (Date.now() - mlOauthState.createdAt) < 10 * 60 * 1000;
    if (!stOk) throw new Error("State inválido/expirado. Tente conectar novamente.");

    const proto = (req.headers["x-forwarded-proto"] || req.protocol || "http").split(",")[0].trim();
    const host = req.get("host");
    const redirectUri = (cfg?.redirect_uri || `${proto}://${host}/integracoes/mercadolivre/oauth/callback`).trim();

    const token = await mlOauthToken({
      grant_type: "authorization_code",
      client_id: cfg.client_id,
      client_secret: cfg.client_secret,
      code,
      redirect_uri: redirectUri,
    });

    const access_token = String(token?.access_token || "");
    const refresh_token = String(token?.refresh_token || "");
    const expires_in = Number(token?.expires_in || 3600);
    if (!access_token || !refresh_token) throw new Error("OAuth ML não retornou tokens");

    const token_expires_at = String(Date.now() + Math.max(60, expires_in) * 1000);
    upsertMlCfg({ access_token, refresh_token, token_expires_at });

    const me = await mlApiGet("/users/me", access_token);
    upsertMlCfg({ seller_id: String(me?.id || ""), site_id: String(me?.site_id || "") });

    return res.redirect("/integracoes/mercadolivre?ok=" + encodeURIComponent("Conta conectada com sucesso."));
  } catch (e) {
    return res.redirect("/integracoes/mercadolivre?err=" + encodeURIComponent(e.message || "Falha no callback"));
  }
});

app.post("/integracoes/mercadolivre/sync", requireAuth, requireModule("integracoes"), uploadNone.none(), async (req, res) => {
  try {
    await ready;
    const cfg = getMlCfgRow();
    if (!cfg?.seller_id) throw new Error("Conecte a conta antes de sincronizar.");
    const token = await ensureMlAccessToken();
    const limit = Math.min(200, Math.max(10, Number(cfg.sync_limit || 50) || 50));
    const search = await mlApiGet(`/orders/search?seller=${encodeURIComponent(cfg.seller_id)}&sort=date_desc&limit=${limit}`, token);
    const results = Array.isArray(search?.results) ? search.results : [];
    let created = 0, updated = 0;
    for (const order of results) {
      // garante dados completos (endereço/envio) mesmo que o search venha resumido
      const full = order?.id ? await mlApiGet(`/orders/${encodeURIComponent(order.id)}`, token).catch(() => order) : order;
      const r = await createOrUpdatePedidoFromMlOrder(full);
      if (r?.created) created += 1;
      if (r?.updated) updated += 1;
    }
    upsertMlCfg({ last_sync_at: nowText() });
    return res.redirect("/integracoes/mercadolivre?ok=" + encodeURIComponent(`Sincronizado. Novos: ${created} • Atualizados: ${updated}`));
  } catch (e) {
    return res.redirect("/integracoes/mercadolivre?err=" + encodeURIComponent(e.message || "Falha ao sincronizar"));
  }
});

app.post("/integracoes/mercadolivre/produtos/mapeamentos", requireAuth, requireModule("integracoes"), uploadNone.none(), async (req, res) => {
  try {
    await ready;
    const itemsRaw = req.body.items;
    const items = Array.isArray(itemsRaw) ? itemsRaw : (itemsRaw && typeof itemsRaw === "object" ? Object.values(itemsRaw) : []);
    for (const it of items) {
      const id = Number(it.catalogo_produto_id || 0);
      if (!id) continue;
      const external_item_id = String(it.external_item_id || "").trim() || null;
      const external_sku = String(it.external_sku || "").trim() || null;
      upsertIntegracaoCatalogoProduto({ canal: "MERCADOLIVRE", catalogo_produto_id: id, external_item_id, external_sku });
    }
    return res.redirect("/integracoes/mercadolivre?ok=" + encodeURIComponent("Mapeamentos salvos."));
  } catch (e) {
    return res.redirect("/integracoes/mercadolivre?err=" + encodeURIComponent(e.message || "Falha ao salvar mapeamentos"));
  }
});

app.post("/integracoes/mercadolivre/produtos/sync", requireAuth, requireModule("integracoes"), uploadNone.none(), async (req, res) => {
  try {
    await ready;
    const cfg = getMlCfgRow();
    if (!cfg?.seller_id) throw new Error("Conecte a conta antes de sincronizar.");
    const token = await ensureMlAccessToken();

    const catalogoProdutos = listCatalogoProdutos();
    const map = listCatalogoProdutosMapeados("MERCADOLIVRE");
    const mapByCatalogo = new Map(map.map(m => [Number(m.catalogo_produto_id), m]));

    let ok = 0, skip = 0, fail = 0;
    for (const p of catalogoProdutos) {
      const m = mapByCatalogo.get(Number(p.id));
      const itemId = m?.external_item_id ? String(m.external_item_id) : null;
      if (!itemId) { skip++; continue; }

      const payload = {};
      const price = calcCatalogoPrecoFinalParaMarketplace(p);
      if (!Number.isNaN(price) && price > 0) payload.price = price;

      const q = calcEstoqueDisponivelParaMarketplace(p);
      if (q !== null) payload.available_quantity = q;

      if (Object.keys(payload).length === 0) { skip++; continue; }

      try {
        await mlApiPut(`/items/${encodeURIComponent(itemId)}`, token, payload);
        upsertIntegracaoCatalogoProduto({ canal: "MERCADOLIVRE", catalogo_produto_id: p.id, external_item_id: itemId, external_sku: p.sku, raw_json: payload });
        ok++;
      } catch (e) {
        fail++;
        try { db.prepare("INSERT INTO integracao_logs (canal, tipo_evento, payload) VALUES (?,?,?)").run("MERCADOLIVRE", "PRODUTO_SYNC_FAIL", JSON.stringify({ itemId, payload, err: e.message })); } catch (_) {}
      }
    }

    return res.redirect("/integracoes/mercadolivre?ok=" + encodeURIComponent(`Produtos sincronizados. OK: ${ok} • Sem mapa/sem dados: ${skip} • Falhas: ${fail}`));
  } catch (e) {
    return res.redirect("/integracoes/mercadolivre?err=" + encodeURIComponent(e.message || "Falha ao sincronizar produtos"));
  }
});


app.post("/webhooks/mercadolivre", express.json({ type: ["application/json", "text/json", "*/json"] }), async (req, res) => {
  // Dica oficial: responda 200 rápido e busque detalhes pela API para evitar re-tentativas/duplicidades.
  // https://developers.mercadolivre.com.br/pt_br/produto-receba-notificacoes
  res.status(200).json({ ok: true });
  try {
    await ready;
    const cfg = getMlCfgRow();

    // valida segredo opcional (se configurado no sistema)
    const provided = String(req.headers["x-acrilsoft-webhook-secret"] || req.query.secret || "").trim();
    if (cfg?.webhook_secret && (!provided || provided !== String(cfg.webhook_secret))) {
      try {
        db.prepare("INSERT INTO integracao_logs (canal, tipo_evento, payload) VALUES (?,?,?)")
          .run("MERCADOLIVRE", "WEBHOOK_DENY", JSON.stringify({ headers: req.headers, query: req.query }));
      } catch (_) {}
      return;
    }

    try {
      db.prepare("INSERT INTO integracao_logs (canal, tipo_evento, payload) VALUES (?,?,?)")
        .run("MERCADOLIVRE", "WEBHOOK", JSON.stringify(req.body || {}));
    } catch (_) {}

    const body = req.body || {};
    const topic = String(body.topic || body.type || "");
    const resource = String(body.resource || "");
    if (!cfg?.seller_id) return;
    if (!resource || !resource.startsWith("/")) return;

    const token = await ensureMlAccessToken();
    if (topic.includes("orders") || resource.startsWith("/orders/")) {
      const order = await mlApiGet(resource, token).catch(() => null);
      if (order) await createOrUpdatePedidoFromMlOrder(order);
    }
  } catch (_) {}
});


// ===================== SHOPEE (INTEGRAÇÃO) =====================
function getShopeeCfgRow() {
  const row = db.prepare("SELECT * FROM integracao_shopee WHERE id = 1").get();
  if (row) return row;
  db.prepare("INSERT OR IGNORE INTO integracao_shopee (id, webhook_secret) VALUES (1, ?)").run(genSecret(32));
  return db.prepare("SELECT * FROM integracao_shopee WHERE id = 1").get();
}

function shopeeBaseHost(cfg) {
  const prod = Number(cfg.production || 0) === 1;
  return (prod ? (cfg.host_url || "https://partner.shopeemobile.com") : (cfg.sandbox_host_url || "https://partner.test-stable.shopeemobile.com")).replace(/\/+$/, "");
}

function shopeeSign(cfg, apiPath, timestamp, accessToken, shopId) {
  // Common v2 rule: baseString = partner_id + api_path + timestamp + access_token + shop_id, HMAC-SHA256 with partner_key.
  const pid = String(cfg.partner_id || "");
  const token = accessToken ? String(accessToken) : "";
  const sid = shopId ? String(shopId) : "";
  const base = pid + apiPath + String(timestamp) + token + sid;
  const sign = crypto.createHmac("sha256", String(cfg.partner_key || "")).update(base).digest("hex");
  return sign;
}

async function shopeeCall(cfg, apiPath, queryParams = {}, method = "GET", bodyObj = null) {
  const host = shopeeBaseHost(cfg);
  const ts = Math.floor(Date.now() / 1000);
  const shopId = queryParams.shop_id || cfg.shop_id || "";
  const accessToken = queryParams.access_token || cfg.access_token || "";

  const sign = shopeeSign(cfg, apiPath, ts, accessToken, shopId);

  const qp = new URLSearchParams({
    partner_id: String(cfg.partner_id || ""),
    timestamp: String(ts),
    sign,
    ...Object.fromEntries(Object.entries(queryParams).filter(([_, v]) => v !== undefined && v !== null && v !== "")),
  });

  const url = `${host}${apiPath}?${qp.toString()}`;
  const opts = { method, headers: { "Content-Type": "application/json" } };
  if (bodyObj && method !== "GET") opts.body = JSON.stringify(bodyObj);
  const resp = await fetch(url, opts);
  const text = await resp.text();
  let data;
  try { data = JSON.parse(text); } catch { data = { raw: text }; }
  if (!resp.ok) {
    const msg = `Shopee HTTP ${resp.status}: ${text}`;
    const e = new Error(msg);
    e.status = resp.status;
    e.payload = data;
    throw e;
  }
  return data;
}

app.get("/integracoes/shopee", requireAuth, requireModule("integracoes"), async (req, res) => {
  try {
    await ready;
    const cfg = getShopeeCfgRow();
    const proto = (req.headers["x-forwarded-proto"] || req.protocol || "http").split(",")[0].trim();
    const host = req.get("host");
    const suggestedRedirect = `${proto}://${host}/integracoes/shopee/oauth/callback`;
    const webhookUrl = `${proto}://${host}/webhooks/shopee`;

    const catalogoProdutos = listCatalogoProdutos();
    const catalogoMap = listCatalogoProdutosMapeados("SHOPEE");
    return res.render("layout", {
      view: "integracao-shopee-page",
      title: "Shopee",
      activeMenu: "shopee",
      cfg,
      suggestedRedirect,
      webhookUrl,
      ok: req.query.ok ? String(req.query.ok) : null,
      err: req.query.err ? String(req.query.err) : null,
    });
  } catch (err) {
    console.error("Erro /integracoes/shopee:", err);
    res.status(500).send(err.message);
  }
});

app.post("/integracoes/shopee/salvar", requireAuth, requireModule("integracoes"), async (req, res) => {
  try {
    await ready;
    const cfg = getShopeeCfgRow();
    const production = req.body.production ? 1 : 0;
    const partner_id = String(req.body.partner_id || "").trim();
    const partner_key = String(req.body.partner_key || "").trim();
    const redirect_uri = String(req.body.redirect_uri || "").trim();
    const shop_id = String(req.body.shop_id || "").trim();

    const host_url = String(req.body.host_url || cfg.host_url || "https://partner.shopeemobile.com").trim();
    const sandbox_host_url = String(req.body.sandbox_host_url || cfg.sandbox_host_url || "https://partner.test-stable.shopeemobile.com").trim();
    const sync_lookback_days = Number(req.body.sync_lookback_days || cfg.sync_lookback_days || 7);
    const sync_limit = Number(req.body.sync_limit || cfg.sync_limit || 50);

    db.prepare(`
      UPDATE integracao_shopee
      SET production=?, host_url=?, sandbox_host_url=?, partner_id=?, partner_key=?, redirect_uri=?, shop_id=?,
          sync_lookback_days=?, sync_limit=?, atualizado_em=datetime('now')
      WHERE id=1
    `).run(production, host_url, sandbox_host_url, partner_id, partner_key, redirect_uri, shop_id, sync_lookback_days, sync_limit);

    res.redirect("/integracoes/shopee?ok=" + encodeURIComponent("Configuração salva."));
  } catch (err) {
    console.error("Erro salvar shopee:", err);
    res.redirect("/integracoes/shopee?err=" + encodeURIComponent(err.message));
  }
});

app.post("/integracoes/shopee/secret/regen", requireAuth, requireModule("integracoes"), async (req, res) => {
  try {
    await ready;
    db.prepare("UPDATE integracao_shopee SET webhook_secret=?, atualizado_em=datetime('now') WHERE id=1").run(genSecret(32));
    res.redirect("/integracoes/shopee?ok=" + encodeURIComponent("Secret do webhook regenerado."));
  } catch (err) {
    res.redirect("/integracoes/shopee?err=" + encodeURIComponent(err.message));
  }
});

app.get("/integracoes/shopee/connect", requireAuth, requireModule("integracoes"), async (req, res) => {
  try {
    await ready;
    const cfg = getShopeeCfgRow();
    if (!cfg.partner_id || !cfg.partner_key) throw new Error("Informe Partner ID e Partner Key.");
    const proto = (req.headers["x-forwarded-proto"] || req.protocol || "http").split(",")[0].trim();
    const host = req.get("host");
    const redirect = (cfg.redirect_uri && cfg.redirect_uri.trim()) ? cfg.redirect_uri.trim() : `${proto}://${host}/integracoes/shopee/oauth/callback`;

    const apiPath = "/api/v2/shop/auth_partner";
    const ts = Math.floor(Date.now() / 1000);
    const sign = shopeeSign(cfg, apiPath, ts, "", "");
    const url = `${shopeeBaseHost(cfg)}${apiPath}?partner_id=${encodeURIComponent(cfg.partner_id)}&timestamp=${ts}&sign=${sign}&redirect=${encodeURIComponent(redirect)}`;
    return res.redirect(url);
  } catch (err) {
    console.error("Erro connect shopee:", err);
    res.redirect("/integracoes/shopee?err=" + encodeURIComponent(err.message));
  }
});

app.get("/integracoes/shopee/oauth/callback", async (req, res) => {
  try {
    await ready;
    const code = String(req.query.code || "").trim();
    const shop_id = String(req.query.shop_id || "").trim();
    if (!code || !shop_id) throw new Error("Callback inválido. Parâmetros code/shop_id ausentes.");

    const cfg = getShopeeCfgRow();
    const proto = (req.headers["x-forwarded-proto"] || req.protocol || "http").split(",")[0].trim();
    const host = req.get("host");
    const redirect = (cfg.redirect_uri && cfg.redirect_uri.trim()) ? cfg.redirect_uri.trim() : `${proto}://${host}/integracoes/shopee/oauth/callback`;

    const apiPath = "/api/v2/auth/token/get";
    const ts = Math.floor(Date.now() / 1000);
    const sign = shopeeSign(cfg, apiPath, ts, "", "");

    const url = `${shopeeBaseHost(cfg)}${apiPath}?partner_id=${encodeURIComponent(cfg.partner_id)}&timestamp=${ts}&sign=${sign}`;
    const resp = await fetch(url, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ code, shop_id: Number(shop_id), partner_id: Number(cfg.partner_id), redirect_uri: redirect }),
    });
    const data = await resp.json().catch(() => ({}));

    if (!resp.ok || data.error) {
      throw new Error("Erro ao obter token Shopee: " + (data.message || data.error || resp.status));
    }

    const access_token = data.access_token || data.accessToken;
    const refresh_token = data.refresh_token || data.refreshToken;
    const expire_in = Number(data.expire_in || data.expireIn || 0);
    const token_expires_at = expire_in ? new Date(Date.now() + expire_in * 1000).toISOString() : null;

    db.prepare(`
      UPDATE integracao_shopee
      SET shop_id=?, access_token=?, refresh_token=?, token_expires_at=?, atualizado_em=datetime('now')
      WHERE id=1
    `).run(shop_id, access_token, refresh_token, token_expires_at);

    res.redirect("/integracoes/shopee?ok=" + encodeURIComponent("Conta Shopee conectada."));
  } catch (err) {
    console.error("Erro callback shopee:", err);
    res.redirect("/integracoes/shopee?err=" + encodeURIComponent(err.message));
  }
});

async function shopeeEnsureToken(cfg) {
  if (!cfg.access_token || !cfg.refresh_token) return cfg;
  const exp = cfg.token_expires_at ? Date.parse(cfg.token_expires_at) : 0;
  if (exp && Date.now() < (exp - 2 * 60 * 1000)) return cfg;

  const apiPath = "/api/v2/auth/access_token/get";
  const ts = Math.floor(Date.now() / 1000);
  const sign = shopeeSign(cfg, apiPath, ts, "", "");
  const url = `${shopeeBaseHost(cfg)}${apiPath}?partner_id=${encodeURIComponent(cfg.partner_id)}&timestamp=${ts}&sign=${sign}`;

  const resp = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ refresh_token: cfg.refresh_token, shop_id: Number(cfg.shop_id), partner_id: Number(cfg.partner_id) }),
  });
  const data = await resp.json().catch(() => ({}));
  if (!resp.ok || data.error) throw new Error("Erro ao renovar token Shopee: " + (data.message || data.error || resp.status));

  const access_token = data.access_token || data.accessToken;
  const refresh_token = data.refresh_token || data.refreshToken || cfg.refresh_token;
  const expire_in = Number(data.expire_in || data.expireIn || 0);
  const token_expires_at = expire_in ? new Date(Date.now() + expire_in * 1000).toISOString() : null;

  db.prepare(`
    UPDATE integracao_shopee
    SET access_token=?, refresh_token=?, token_expires_at=?, atualizado_em=datetime('now')
    WHERE id=1
  `).run(access_token, refresh_token, token_expires_at);

  return { ...cfg, access_token, refresh_token, token_expires_at };
}

async function upsertPedidoShopeeFromOrder(orderDetail, rawJson) {
  const externalId = String(orderDetail.order_sn || orderDetail.orderSn || orderDetail.order_id || "");
  if (!externalId) return null;

  const nome = String(orderDetail.buyer_username || orderDetail.buyerUsername || "Cliente Shopee").trim();
  const contato = String((orderDetail.recipient_address || {}).phone || "").trim();

  const addr = orderDetail.recipient_address || {};
  const endereco = [addr.name, addr.phone, addr.full_address, addr.address, addr.city, addr.state, addr.zipcode]
    .filter(Boolean)
    .join(" - ");


  // cria/atualiza cliente local para NF-e (Shopee não fornece CPF/CNPJ por padrão)
  let cliente_id = null;
  try {
    const externalCli = String(orderDetail.buyer_username || orderDetail.buyerUsername || nome || "").trim() || null;
    if (externalCli) {
      const mapCli = findClienteByExternalId("SHOPEE", externalCli);
      if (mapCli?.cliente_id) {
        cliente_id = Number(mapCli.cliente_id);
      } else {
        cliente_id = upsertClienteBasico({ nome, telefone: contato, email: null });
        // tenta decompor endereço básico
        const addr2 = orderDetail.recipient_address || {};
        upsertClienteEnderecoPrincipal(cliente_id, {
          cep: addr2.zipcode,
          logradouro: addr2.full_address || addr2.address,
          numero: null,
          complemento: null,
          bairro: null,
          cidade: addr2.city,
          uf: addr2.state,
        });
        upsertIntegracaoCliente({ canal: "SHOPEE", external_cliente_id: externalCli, cliente_id, raw_json: orderDetail });
      }
    }
  } catch (_) {}
  const total = Number(orderDetail.total_amount || orderDetail.totalAmount || orderDetail.total || 0);

  const st = String(orderDetail.order_status || orderDetail.orderStatus || "").toUpperCase();
  let statusInterno = "APROVADO";
  if (st.includes("CANCEL")) statusInterno = "CANCELADO";
  if (st.includes("COMPLET") || st.includes("DELIVER")) statusInterno = "ENTREGUE";

  const map = db.prepare("SELECT * FROM integracao_pedidos WHERE canal='SHOPEE' AND external_order_id=?").get(externalId);
  if (map) {
    db.prepare("UPDATE integracao_pedidos SET external_status=?, last_update_at=datetime('now'), raw_json=? WHERE id=?")
      .run(st, JSON.stringify(rawJson || orderDetail), map.id);
    db.prepare("UPDATE pedidos SET status=?, cliente_id=COALESCE(?,cliente_id), cliente_nome_avulso=?, cliente_telefone_avulso=?, endereco_entrega_texto=?, canal_venda='SHOPEE' WHERE id=?")
      .run(statusInterno, cliente_id || null, nome, contato, endereco, map.pedido_id);
    return map.pedido_id;
  }

  const pedidoId = db.prepare(`
    INSERT INTO pedidos (cliente_nome, cliente_contato, status, canal_venda, subtotal_itens, desconto_tipo, desconto_valor, frete_valor, total, endereco_entrega_texto, criado_em, cliente_id)
    VALUES (?, ?, ?, 'SHOPEE', ?, 'VALOR', 0, 0, ?, ?, datetime('now'), ?)
  `).run(nome, contato, statusInterno, total, total, endereco, cliente_id || null).lastInsertRowid;

  db.prepare(`
    INSERT INTO integracao_pedidos (canal, external_order_id, pedido_id, external_status, last_update_at, raw_json, criado_em)
    VALUES ('SHOPEE', ?, ?, ?, datetime('now'), ?, datetime('now'))
  `).run(externalId, Number(pedidoId), st, JSON.stringify(rawJson || orderDetail));

  return Number(pedidoId);
}

app.post("/integracoes/shopee/sync", requireAuth, requireModule("integracoes"), async (req, res) => {
  try {
    await ready;
    let cfg = getShopeeCfgRow();
    if (!cfg.partner_id || !cfg.partner_key || !cfg.shop_id) throw new Error("Configure Partner ID/Key e Shop ID.");
    cfg = await shopeeEnsureToken(cfg);
    if (!cfg.access_token) throw new Error("Conecte a conta para obter access_token.");

    const lookback = Number(cfg.sync_lookback_days || 7);
    const limit = Number(cfg.sync_limit || 50);
    const now = Math.floor(Date.now() / 1000);
    const time_from = now - lookback * 86400;

    const listPath = "/api/v2/order/get_order_list";
    const list = await shopeeCall(cfg, listPath, {
      shop_id: cfg.shop_id,
      access_token: cfg.access_token,
      time_from,
      time_to: now,
      page_size: Math.min(limit, 100),
      cursor: "",
      time_range_field: "create_time",
    }, "GET");

    const orderList = list.order_list || list.order_sn_list || [];
    const orderSns = orderList.map(o => o.order_sn || o).filter(Boolean);
    let imported = 0;

    if (orderSns.length) {
      const detailPath = "/api/v2/order/get_order_detail";
      const details = await shopeeCall(cfg, detailPath, {
        shop_id: cfg.shop_id,
        access_token: cfg.access_token,
        order_sn_list: orderSns.join(","),
        response_optional_fields: "buyer_username,recipient_address,total_amount,order_status,item_list",
      }, "GET");

      const detailList = details.order_list || [];
      for (const od of detailList) {
        const pid = await upsertPedidoShopeeFromOrder(od, od);
        if (pid) imported += 1;
      }
    }

    db.prepare("UPDATE integracao_shopee SET last_sync_at=datetime('now') WHERE id=1").run();
    res.redirect("/integracoes/shopee?ok=" + encodeURIComponent(`Sync concluído. ${imported} pedido(s) processado(s).`));
  } catch (err) {
    console.error("Erro sync shopee:", err);
    res.redirect("/integracoes/shopee?err=" + encodeURIComponent(err.message));
  }
});

app.post("/integracoes/shopee/produtos/mapeamentos", requireAuth, requireModule("integracoes"), async (req, res) => {
  try {
    await ready;
    const itemsRaw = req.body.items;
    const items = Array.isArray(itemsRaw) ? itemsRaw : (itemsRaw && typeof itemsRaw === "object" ? Object.values(itemsRaw) : []);
    for (const it of items) {
      const id = Number(it.catalogo_produto_id || 0);
      if (!id) continue;
      const external_item_id = String(it.external_item_id || "").trim() || null;
      const external_sku = String(it.external_sku || "").trim() || null;
      upsertIntegracaoCatalogoProduto({ canal: "SHOPEE", catalogo_produto_id: id, external_item_id, external_sku });
    }
    return res.redirect("/integracoes/shopee?ok=" + encodeURIComponent("Mapeamentos salvos."));
  } catch (e) {
    return res.redirect("/integracoes/shopee?err=" + encodeURIComponent(e.message || "Falha ao salvar mapeamentos"));
  }
});

app.post("/integracoes/shopee/produtos/sync", requireAuth, requireModule("integracoes"), async (req, res) => {
  try {
    await ready;
    let cfg = getShopeeCfgRow();
    if (!cfg.partner_id || !cfg.partner_key || !cfg.shop_id) throw new Error("Configure Partner ID/Key e Shop ID.");
    cfg = await shopeeEnsureToken(cfg);
    if (!cfg.access_token) throw new Error("Conecte a conta para obter access_token.");

    const catalogoProdutos = listCatalogoProdutos();
    const map = listCatalogoProdutosMapeados("SHOPEE");
    const mapByCatalogo = new Map(map.map(m => [Number(m.catalogo_produto_id), m]));

    let ok = 0, skip = 0, fail = 0;

    for (const p of catalogoProdutos) {
      const m = mapByCatalogo.get(Number(p.id));
      const itemId = m?.external_item_id ? String(m.external_item_id) : null;
      if (!itemId) { skip++; continue; }

      // Atualiza preço
      try {
        const price = calcCatalogoPrecoFinalParaMarketplace(p);
        if (price > 0) {
          await shopeeCall(cfg, "/api/v2/product/update_price", { shop_id: cfg.shop_id, access_token: cfg.access_token }, "POST", {
            item_id: Number(itemId),
            price_list: [{ model_id: 0, original_price: price, current_price: price }]
          });
        }
      } catch (e) {
        fail++;
        try { db.prepare("INSERT INTO integracao_logs (canal, tipo_evento, payload) VALUES (?,?,?)").run("SHOPEE", "PRODUTO_PRECO_FAIL", JSON.stringify({ itemId, err: e.message })); } catch (_) {}
        continue;
      }

      // Atualiza estoque
      try {
        const q = calcEstoqueDisponivelParaMarketplace(p);
        if (q !== null) {
          await shopeeCall(cfg, "/api/v2/product/update_stock", { shop_id: cfg.shop_id, access_token: cfg.access_token }, "POST", {
            item_id: Number(itemId),
            stock_list: [{ model_id: 0, seller_stock: [{ location_id: 0, stock: q }] }]
          });
        }
        upsertIntegracaoCatalogoProduto({ canal: "SHOPEE", catalogo_produto_id: p.id, external_item_id: itemId, external_sku: p.sku, raw_json: { syncedAt: nowText() } });
        ok++;
      } catch (e) {
        fail++;
        try { db.prepare("INSERT INTO integracao_logs (canal, tipo_evento, payload) VALUES (?,?,?)").run("SHOPEE", "PRODUTO_ESTOQUE_FAIL", JSON.stringify({ itemId, err: e.message })); } catch (_) {}
      }
    }

    return res.redirect("/integracoes/shopee?ok=" + encodeURIComponent(`Produtos sincronizados. OK: ${ok} • Sem mapa/sem dados: ${skip} • Falhas: ${fail}`));
  } catch (e) {
    return res.redirect("/integracoes/shopee?err=" + encodeURIComponent(e.message || "Falha ao sincronizar produtos"));
  }
});


app.post("/webhooks/shopee", async (req, res) => {
  try {
    await ready;
    const cfg = getShopeeCfgRow();
    const provided = String(req.query.secret || req.headers["x-acrilsoft-webhook-secret"] || "");
    if (cfg.webhook_secret && provided !== cfg.webhook_secret) {
      db.prepare("INSERT INTO integracao_logs (canal, tipo_evento, payload, criado_em) VALUES ('SHOPEE','WEBHOOK_DENY',?,datetime('now'))")
        .run(JSON.stringify({ headers: req.headers, query: req.query }));
      return res.sendStatus(200);
    }

    db.prepare("INSERT INTO integracao_logs (canal, tipo_evento, payload, criado_em) VALUES ('SHOPEE','WEBHOOK',?,datetime('now'))")
      .run(JSON.stringify(req.body || {}));

    // MVP: apenas registra. Para produção, podemos consultar order_detail a partir do payload.
    res.sendStatus(200);
  } catch (err) {
    console.error("Erro webhook shopee:", err);
    res.sendStatus(200);
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


/* ===== Painel Multicanal ===== */
function getMulticanalData({ dias = 30, status = "" } = {}) {
  const safeDias = Math.max(1, Math.min(365, Number(dias) || 30));
  const desde = db.prepare("SELECT date('now', ?) as d").get(`-${safeDias} day`).d;

  // Status padrão: considera vendas fechadas (exclui rascunho/orçamento/cancelado)
  const statusFilter = String(status || "").trim().toUpperCase();
  const whereStatus = statusFilter ? "AND UPPER(COALESCE(p.status,'')) = ?" : "AND UPPER(COALESCE(p.status,'')) NOT IN ('RASCUNHO','ORCAMENTO','ORÇAMENTO','CANCELADO','CANCELADA')";

  const rows = db.prepare(`
    SELECT
      COALESCE(NULLIF(TRIM(p.canal_venda),''),'Loja') as canal,
      COUNT(DISTINCT p.id) as pedidos,
      COALESCE(SUM(COALESCE(pi.total_item,0)),0) as faturamento,
      COALESCE(SUM(COALESCE(pi.custo_unit,0) * COALESCE(pi.quantidade,0)),0) as custo,
      COALESCE(SUM(COALESCE(pi.total_item,0)) - SUM(COALESCE(pi.custo_unit,0) * COALESCE(pi.quantidade,0)),0) as margem
    FROM pedidos p
    LEFT JOIN pedido_itens pi ON pi.pedido_id = p.id
    WHERE date(COALESCE(p.data_venda, p.criado_em)) >= date(?)
      ${whereStatus}
    GROUP BY canal
    ORDER BY faturamento DESC
  `).all(desde, ...(statusFilter ? [statusFilter] : []));

  const totals = rows.reduce((acc, r) => {
    acc.pedidos += Number(r.pedidos || 0);
    acc.faturamento += Number(r.faturamento || 0);
    acc.custo += Number(r.custo || 0);
    acc.margem += Number(r.margem || 0);
    return acc;
  }, { pedidos: 0, faturamento: 0, custo: 0, margem: 0 });

  totals.ticket = totals.pedidos ? totals.faturamento / totals.pedidos : 0;
  totals.margemPct = totals.faturamento ? totals.margem / totals.faturamento : 0;

  const enriched = (rows || []).map(r => {
    const pedidos = Number(r.pedidos || 0);
    const fatur = Number(r.faturamento || 0);
    const custo = Number(r.custo || 0);
    const margem = Number(r.margem || 0);
    return {
      canal: r.canal,
      pedidos,
      faturamento: fatur,
      custo,
      margem,
      ticket: pedidos ? fatur / pedidos : 0,
      margemPct: fatur ? margem / fatur : 0
    };
  });

  return { dias: safeDias, desde, status: statusFilter || "PADRAO", rows: enriched, totals };
}

app.get("/dashboard/multicanal", requireModule("dashboard"), (req, res) => {
  const dias = Number(req.query.dias || 30) || 30;
  const status = req.query.status || "";
  const data = getMulticanalData({ dias, status });

  res.render("layout", {
    req,
    title: "Painel Multicanal",
    view: "dashboard-multicanal-page",
    ...data
  });
});



// =========================
// LOGÍSTICA / EXPEDIÇÃO
// =========================

function safeJsonParse(s) {
  try { return JSON.parse(s); } catch { return null; }
}

function getOrCreateEnvioByPedido(pedidoId) {
  const pid = Number(pedidoId);
  if (!pid) return null;

  let envio = db.prepare("SELECT * FROM logistica_envios WHERE pedido_id=?").get(pid);
  if (!envio) {
    const p = db.prepare("SELECT id, canal_venda FROM pedidos WHERE id=?").get(pid);
    const canal = p?.canal_venda || "MANUAL";
    const integ = db.prepare("SELECT canal, external_order_id FROM integracao_pedidos WHERE pedido_id=? ORDER BY id DESC LIMIT 1").get(pid);
    const external_order_id = integ?.external_order_id || null;
    const now = nowText();
    db.prepare(`
      INSERT INTO logistica_envios
      (pedido_id, canal, external_order_id, status_envio, criado_em, atualizado_em)
      VALUES (?,?,?,?,?,?)
    `).run(pid, canal, external_order_id, "PENDENTE", now, now);
    envio = db.prepare("SELECT * FROM logistica_envios WHERE pedido_id=?").get(pid);
  }
  return envio;
}

function addEnvioEvento(envioId, tipo, descricao) {
  try {
    db.prepare("INSERT INTO logistica_eventos (envio_id, tipo, descricao, criado_em) VALUES (?,?,?,datetime('now'))")
      .run(Number(envioId), String(tipo||''), String(descricao||''));
  } catch (_) {}
}

function listExpedicao({ dias = 30, status = "", canal = "" }) {
  const d = Math.max(1, Math.min(3650, Number(dias)||30));
  const since = new Date(Date.now() - d*24*60*60*1000).toISOString().slice(0,19).replace('T',' ');
  const wh = [];
  const params = [];
  wh.push("p.criado_em >= ?");
  params.push(since);
  if (status) { wh.push("COALESCE(e.status_envio,'PENDENTE') = ?"); params.push(status); }
  if (canal) { wh.push("COALESCE(e.canal, p.canal_venda, 'MANUAL') = ?"); params.push(canal); }

  const where = wh.length ? ("WHERE " + wh.join(" AND ")) : "";
  const rows = db.prepare(`
    SELECT
      p.id as pedido_id,
      COALESCE(NULLIF(p.bling_numero,''), CAST(p.id AS TEXT)) as pedido_numero,
      p.status as pedido_status,
      p.total as pedido_total,
      p.criado_em as criado_em,
      COALESCE(c.nome, c.razao_social, p.cliente_nome_avulso, 'Consumidor final') as cliente,
      COALESCE(e.id, 0) as envio_id,
      COALESCE(e.canal, p.canal_venda, 'MANUAL') as canal,
      COALESCE(e.external_order_id, ip.external_order_id) as external_order_id,
      COALESCE(e.status_envio, 'PENDENTE') as status_envio,
      e.transportadora,
      e.codigo_rastreio,
      e.url_rastreio,
      e.data_postagem,
      e.data_entrega,
      e.obs
    FROM pedidos p
    LEFT JOIN clientes c ON c.id = p.cliente_id
    LEFT JOIN logistica_envios e ON e.pedido_id = p.id
    LEFT JOIN integracao_pedidos ip ON ip.pedido_id = p.id
    ${where}
    GROUP BY p.id
    ORDER BY COALESCE(e.atualizado_em, p.criado_em) DESC
    LIMIT 500
  `).all(...params);

  const canais = [
    { key: "", label: "Todos" },
    { key: "MANUAL", label: "Loja" },
    { key: "MERCADOLIVRE", label: "Mercado Livre" },
    { key: "SHOPEE", label: "Shopee" },
  ];

  const statusList = [
    { key: "", label: "Todos" },
    { key: "PENDENTE", label: "Pendente" },
    { key: "SEPARANDO", label: "Separando" },
    { key: "PRONTO", label: "Pronto p/ Postar" },
    { key: "POSTADO", label: "Postado" },
    { key: "EM_TRANSITO", label: "Em trânsito" },
    { key: "ENTREGUE", label: "Entregue" },
    { key: "CANCELADO", label: "Cancelado" },
  ];

  return { rows, dias: d, status, canal, canais, statusList };
}

app.get("/logistica/expedicao", requireAuth, requireModule("logistica"), async (req, res) => {
  try {
    await ready;
    const dias = Number(req.query.dias || 30) || 30;
    const status = String(req.query.status || "");
    const canal = String(req.query.canal || "");
    const data = listExpedicao({ dias, status, canal });

    res.render("layout", {
      req,
      title: "Expedição",
      view: "logistica-expedicao-page",
      pageTitle: "Expedição",
      pageSubtitle: "Controle interno + rastreio + atualização no marketplace",
      activeMenu: "logistica",
      ...data
    });
  } catch (e) {
    console.error("Erro expedição:", e);
    res.status(500).send(e.message || String(e));
  }
});

app.post("/logistica/expedicao/:pedidoId/salvar", requireAuth, requireModule("logistica"), uploadNone.none(), async (req, res) => {
  try {
    await ready;
    const pedidoId = Number(req.params.pedidoId);
    const envio = getOrCreateEnvioByPedido(pedidoId);
    if (!envio) return res.status(400).send("Pedido inválido.");

    const status_envio = String(req.body.status_envio || envio.status_envio || "PENDENTE");
    const transportadora = String(req.body.transportadora || "").trim() || null;
    const codigo_rastreio = String(req.body.codigo_rastreio || "").trim() || null;
    const url_rastreio = String(req.body.url_rastreio || "").trim() || null;
    const data_postagem = String(req.body.data_postagem || "").trim() || null;
    const data_entrega = String(req.body.data_entrega || "").trim() || null;
    const obs = String(req.body.obs || "").trim() || null;

    db.prepare(`
      UPDATE logistica_envios
      SET status_envio=?, transportadora=?, codigo_rastreio=?, url_rastreio=?, data_postagem=?, data_entrega=?, obs=?, atualizado_em=datetime('now')
      WHERE pedido_id=?
    `).run(status_envio, transportadora, codigo_rastreio, url_rastreio, data_postagem, data_entrega, obs, pedidoId);

    addEnvioEvento(envio.id, "UPDATE", `Atualizado: status=${status_envio}${codigo_rastreio ? `, rastreio=${codigo_rastreio}` : ""}`);
    res.redirect("/logistica/expedicao?ok=" + encodeURIComponent("Envio atualizado."));
  } catch (e) {
    console.error("Erro salvar envio:", e);
    res.redirect("/logistica/expedicao?err=" + encodeURIComponent(e.message || "Falha ao salvar"));
  }
});

// Atualizar rastreio no Mercado Livre / Shopee (best-effort)
app.post("/logistica/expedicao/:pedidoId/marketplace", requireAuth, requireModule("logistica"), uploadNone.none(), async (req, res) => {
  try {
    await ready;
    const pedidoId = Number(req.params.pedidoId);
    const envio = getOrCreateEnvioByPedido(pedidoId);
    if (!envio) throw new Error("Envio não encontrado.");
    if (!envio.codigo_rastreio) throw new Error("Informe o código de rastreio antes de enviar ao marketplace.");

    const canal = String(envio.canal || "").toUpperCase();
    if (canal === "MERCADOLIVRE") {
      const integ = db.prepare("SELECT * FROM integracao_pedidos WHERE pedido_id=? ORDER BY id DESC LIMIT 1").get(pedidoId);
      const externalId = integ?.external_order_id || envio.external_order_id;
      if (!externalId) throw new Error("Pedido não tem external_order_id do Mercado Livre.");

      const order = integ?.raw_json ? safeJsonParse(integ.raw_json) : null;
      const access = await ensureMlAccessToken();
      const orderLive = order?.id ? order : await mlApiGet(`/orders/${encodeURIComponent(externalId)}`, access);
      const shipmentId = orderLive?.shipping?.id || orderLive?.shipping?.shipment_id;
      const receiverId = orderLive?.buyer?.id;
      if (!shipmentId) throw new Error("Não encontrei shipment_id no pedido do ML.");
      if (!receiverId) throw new Error("Não encontrei receiver_id (buyer.id) no pedido do ML.");

      // Docs indicam PUT em /shipments/{id} com receiver_id + tracking_number.
      await mlApiPut(`/shipments/${encodeURIComponent(shipmentId)}`, access, {
        receiver_id: receiverId,
        tracking_number: envio.codigo_rastreio,
      });

      addEnvioEvento(envio.id, "MARKETPLACE", `Tracking enviado ao Mercado Livre (shipment ${shipmentId}).`);
      return res.redirect("/logistica/expedicao?ok=" + encodeURIComponent("Tracking atualizado no Mercado Livre."));
    }

    if (canal === "SHOPEE") {
      const integ = db.prepare("SELECT * FROM integracao_pedidos WHERE pedido_id=? ORDER BY id DESC LIMIT 1").get(pedidoId);
      const orderSn = integ?.external_order_id || envio.external_order_id;
      if (!orderSn) throw new Error("Pedido não tem external_order_id do Shopee (order_sn).");

      const cfg = getShopeeCfgRow();
      if (!cfg?.partner_id || !cfg?.partner_key || !cfg?.access_token || !cfg?.shop_id) throw new Error("Shopee não configurado/conectado.");

      // Tentativa 1: set_tracking_number (algumas contas exigem ship_order)
      const apiPath = "/api/v2/logistics/set_tracking_number";
      try {
        await shopeeCall(cfg, apiPath, { shop_id: cfg.shop_id, access_token: cfg.access_token }, "POST", {
          order_sn: orderSn,
          tracking_number: envio.codigo_rastreio,
        });
      } catch (e1) {
        // Tentativa 2: ship_order (fallback)
        const apiPath2 = "/api/v2/logistics/ship_order";
        await shopeeCall(cfg, apiPath2, { shop_id: cfg.shop_id, access_token: cfg.access_token }, "POST", {
          order_sn: orderSn,
          tracking_number: envio.codigo_rastreio,
        });
      }

      addEnvioEvento(envio.id, "MARKETPLACE", `Tracking enviado ao Shopee (order_sn ${orderSn}).`);
      return res.redirect("/logistica/expedicao?ok=" + encodeURIComponent("Tracking atualizado no Shopee (best-effort)."));
    }

    throw new Error("Canal não suportado para atualização automática.");
  } catch (e) {
    console.error("Erro marketplace tracking:", e);
    res.redirect("/logistica/expedicao?err=" + encodeURIComponent(e.message || "Falha ao atualizar marketplace"));
  }
});

// =========================
// Error handler (último middleware)
// =========================
app.use((err, req, res, next) => {
  try {
    const isAjax = req.xhr || (req.headers.accept || '').includes('application/json');
    const msg = err && (err.message || String(err)) || 'Erro interno';
    console.error('Erro:', msg);
    if (isAjax) return sendError(res, msg, 500);
    // fallback para telas
    return res.status(500).send('Erro interno: ' + msg);
  } catch (e) {
    return res.status(500).send('Erro interno');
  }
});

app.use(errorHandler);

app.listen(PORT, () => console.log(`Rodando em http://0.0.0.0:${PORT} (ou na URL pública do Render/Railway)`));

// Background sync Bling a cada 10 minutos (somente SQLite/local)
// - Importa novos pedidos (últimos 7 dias)
// - Atualiza situação/itens dos já importados
setTimeout(() => {
  if (IS_PG()) return;
  // roda 1x no boot e depois a cada 10min
  runBlingSync({ lookbackDays: 7 }).catch(() => {});
  setInterval(async () => {
    try { await runBlingSync({ lookbackDays: 7 }); } catch (_) {}
  }, 10 * 60 * 1000);
}, 20 * 1000); // aguarda o boot estabilizar





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
// Integração Bling (pedidos/clientes)
ensureColumn("pedidos", "bling_id", "ALTER TABLE pedidos ADD COLUMN bling_id TEXT");
ensureColumn("pedidos", "bling_numero", "ALTER TABLE pedidos ADD COLUMN bling_numero TEXT");
ensureColumn("pedidos", "bling_situacao", "ALTER TABLE pedidos ADD COLUMN bling_situacao TEXT");
ensureColumn("pedidos", "cliente_codigo", "ALTER TABLE pedidos ADD COLUMN cliente_codigo TEXT");
ensureColumn("pedidos", "cliente_cnpjcpf", "ALTER TABLE pedidos ADD COLUMN cliente_cnpjcpf TEXT");
ensureColumn("pedidos", "cliente_endereco", "ALTER TABLE pedidos ADD COLUMN cliente_endereco TEXT");
try { db.exec("CREATE UNIQUE INDEX IF NOT EXISTS idx_pedidos_bling_id ON pedidos(bling_id)"); } catch (_) {}

});




// (Lixeira de OPs removida: exclusão agora é definitiva

// --- Bling x Produção (métricas locais) ---
let bling_stats = { total_importados: 0, no_periodo: 0, por_situacao: [], itens_no_periodo: 0, convertidos_em_op: 0 };
if (!IS_PG()) {
  try {
    bling_stats.total_importados = db.prepare("SELECT COUNT(*) as n FROM pedidos WHERE bling_id IS NOT NULL AND bling_id <> ''").get().n || 0;
    bling_stats.no_periodo = db.prepare("SELECT COUNT(*) as n FROM pedidos WHERE bling_id IS NOT NULL AND bling_id <> '' AND date(criado_em) >= date(?)").get(desde).n || 0;
    bling_stats.convertidos_em_op = db.prepare("SELECT COUNT(*) as n FROM pedidos WHERE bling_id IS NOT NULL AND bling_id <> '' AND op_id IS NOT NULL").get().n || 0;
    bling_stats.por_situacao = db.prepare(`
      SELECT COALESCE(NULLIF(bling_situacao,''),'(sem)') as situacao, COUNT(*) as n
      FROM pedidos
      WHERE bling_id IS NOT NULL AND bling_id <> ''
      GROUP BY COALESCE(NULLIF(bling_situacao,''),'(sem)')
      ORDER BY n DESC
    `).all();
    bling_stats.itens_no_periodo = db.prepare(`
      SELECT COALESCE(SUM(pi.quantidade),0) as n
      FROM pedido_itens pi
      JOIN pedidos p ON p.id = pi.pedido_id
      WHERE p.bling_id IS NOT NULL AND p.bling_id <> ''
        AND date(p.criado_em) >= date(?)
    `).get(desde).n || 0;
  } catch (_) {}
}