// Integração Bling API v3 (Node sem dependências externas)
// Base URL oficial: https://api.bling.com.br/Api/v3
// Autenticação: Bearer <access_token> (token obtido via OAuth2 / JWT conforme doc do Bling)

const https = require("https");

const DEFAULT_BASE_URL = "https://api.bling.com.br/Api/v3";

// ===============================
// Rate limit / throttling helpers
// ===============================
// O Bling possui limite agressivo. Para evitar 429 (TOO_MANY_REQUESTS),
// serializamos requisições e aplicamos um intervalo mínimo entre chamadas.
const MIN_INTERVAL_MS = Number(process.env.BLING_MIN_INTERVAL_MS || 350);
let _queue = Promise.resolve();
let _lastAt = 0;

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function enqueue(fn) {
  // Garante 1 requisição por vez.
  _queue = _queue.then(fn, fn);
  return _queue;
}

async function throttle() {
  const now = Date.now();
  const wait = Math.max(0, (_lastAt + MIN_INTERVAL_MS) - now);
  if (wait) await sleep(wait);
  _lastAt = Date.now();
}

function buildUrl(baseUrl, path, params) {
  const u = new URL(path.replace(/^\//, ""), baseUrl.replace(/\/+$/, "") + "/");
  if (params && typeof params === "object") {
    for (const [k, v] of Object.entries(params)) {
      if (v === undefined || v === null || v === "") continue;
      u.searchParams.set(k, String(v));
    }
  }
  return u;
}

function httpJson({ method, url, token }) {
  return new Promise((resolve, reject) => {
    const opts = {
      method: method || "GET",
      headers: {
        "Accept": "application/json",
      },
    };
    if (token) opts.headers["Authorization"] = `Bearer ${token}`;

    const req = https.request(url, opts, (res) => {
      let data = "";
      res.on("data", (chunk) => (data += chunk));
      res.on("end", () => {
        let parsed = null;
        try {
          parsed = data ? JSON.parse(data) : null;
        } catch (e) {
          // resposta não-JSON
        }

        if (res.statusCode >= 200 && res.statusCode < 300) {
          return resolve(parsed);
        }

        // O Bling às vezes devolve estruturas (objeto/array) em "message"/"error".
        // Para não virar "[object Object]", serializamos quando necessário.
        let msg =
          (parsed && (parsed.message || parsed.error || parsed.mensagem)) ||
          (parsed && parsed.error && parsed.error.message) ||
          (data && String(data).slice(0, 400)) ||
          `HTTP ${res.statusCode}`;

        if (typeof msg === "object") {
          try {
            msg = JSON.stringify(msg);
          } catch (e) {
            msg = String(msg);
          }
        }

        const err = new Error(`Bling API: ${msg}`);
        err.status = res.statusCode;
        err.payload = parsed;
        // Retry-After pode vir em segundos
        const ra = res.headers && (res.headers["retry-after"] || res.headers["Retry-After"]);
        if (ra) {
          const n = Number(ra);
          if (!Number.isNaN(n) && Number.isFinite(n)) err.retryAfterMs = Math.max(0, n * 1000);
        }
        return reject(err);
      });
    });

    req.on("error", reject);
    req.end();
  });
}

async function blingRequestWithRetry({ method = "GET", url, token }, { maxRetries = 5 } = {}) {
  let attempt = 0;
  while (true) {
    try {
      return await httpJson({ method, url, token });
    } catch (err) {
      const status = err && err.status;
      // 429: limite de requisições
      if (status === 429 && attempt < maxRetries) {
        const base = 1500; // ms
        const backoff = Math.min(20000, base * Math.pow(2, attempt));
        const wait = Math.max(err.retryAfterMs || 0, backoff) + Math.floor(Math.random() * 250);
        await sleep(wait);
        attempt++;
        continue;
      }
      throw err;
    }
  }
}

async function blingGet({ token, baseUrl, path, params }) {
  if (!token) throw new Error("Bling token não configurado (BLING_TOKEN ou Config > Bling).");
  const url = buildUrl(baseUrl || DEFAULT_BASE_URL, path, params);
  // Serializa e aplica throttle para evitar 429
  return enqueue(async () => {
    await throttle();
    return blingRequestWithRetry({ method: "GET", url, token }, { maxRetries: Number(process.env.BLING_MAX_RETRIES || 5) });
  });
}

// --- Normalizadores (tolerantes a variações do payload) ---
function normContato(c) {
  if (!c || typeof c !== "object") return null;
  const endereco = c.endereco || c.enderecos?.[0] || c.enderecos || null;
  return {
    codigo: c.id ?? c.codigo ?? c.idContato ?? null,
    nome: c.nome ?? c.nomeRazaoSocial ?? c.razaoSocial ?? c.fantasia ?? "",
    cnpjcpf: c.numeroDocumento ?? c.cnpj ?? c.cpf ?? c.cnpjCpf ?? "",
    contato: {
      telefone: c.telefone ?? c.fone ?? c.telefone1 ?? "",
      celular: c.celular ?? "",
      email: c.email ?? "",
    },
    endereco: endereco ? {
      endereco: endereco.endereco ?? endereco.logradouro ?? "",
      numero: endereco.numero ?? "",
      complemento: endereco.complemento ?? "",
      bairro: endereco.bairro ?? "",
      cidade: endereco.cidade ?? "",
      uf: endereco.uf ?? endereco.estado ?? "",
      cep: endereco.cep ?? "",
      pais: endereco.pais ?? "",
    } : null,
    raw: c,
  };
}

function normPedidoVenda(p) {
  if (!p || typeof p !== "object") return null;
  const contato = p.contato || p.cliente || p.contatos || null;
  const situacao = p.situacao || p.status || null;
  const vendedor = p.vendedor || null;
  const itens = p.itens || p.itensPedido || p.produtos || p.itensProdutos || [];

  // Observações (no Bling aparecem com nomes diferentes dependendo do endpoint/versão)
  const observacoes =
    p.observacoes ??
    p.observacao ??
    p.obs ??
    p.observacoesCliente ??
    p.observacaoCliente ??
    "";

  const observacoesInternas =
    p.observacoesInternas ??
    p.observacaoInterna ??
    p.obsInterna ??
    p.observacoes_internas ??
    "";

  // Data de saída/prevista/entrega (campo pode variar bastante)
  const dataPrevista =
    p.dataPrevista ??
    p.dataPrevistaEntrega ??
    p.dataEntregaPrevista ??
    p.dataPrevistaEntrega ??
    p.dataPrevisao ??
    p.transporte?.dataPrevista ??
    p.transporte?.dataPrevistaEntrega ??
    "";

  const dataSaida =
    p.dataSaida ??
    p.data_saida ??
    p.transporte?.dataSaida ??
    p.transporte?.data_saida ??
    "";

  const pedidoCompra =
    p.pedidoCompra ??
    p.pedido_compra ??
    p.numeroPedidoCompra ??
    p.numeroPedidoCompraCliente ??
    p.pedidoDeCompra ??
    "";

  const anexos = p.anexos ?? p.anexo ?? p.arquivos ?? [];
  return {
    id: p.id ?? p.idPedidoVenda ?? null,
    numero: p.numero ?? p.numeroPedido ?? p.numeroLoja ?? null,
    data: p.data ?? p.dataEmissao ?? p.dataCriacao ?? p.dataPedido ?? null,

    dataPrevista,
    dataSaida,
    pedidoCompra: (typeof pedidoCompra === "string" ? pedidoCompra : (pedidoCompra ? String(pedidoCompra) : "")),
    anexos: Array.isArray(anexos) ? anexos.map((a) => ({
      id: a.id ?? a.codigo ?? a.idAnexo ?? null,
      nome: a.nome ?? a.filename ?? a.arquivo ?? a.descricao ?? "",
      url: a.url ?? a.link ?? a.downloadUrl ?? a.href ?? "",
      contentType: a.contentType ?? a.tipo ?? a.mimeType ?? a.mimetype ?? "",
      raw: a,
    })) : [],

    situacao: typeof situacao === "object" ? (situacao.nome ?? situacao.descricao ?? situacao.valor ?? "") : (situacao ?? ""),
    contato: normContato(contato) || null,
    vendedor: vendedor ? (vendedor.nome ?? vendedor.apelido ?? vendedor.login ?? "") : "",
    observacoes: (typeof observacoes === "string" ? observacoes : (observacoes ? String(observacoes) : "")),
    observacoesInternas: (typeof observacoesInternas === "string" ? observacoesInternas : (observacoesInternas ? String(observacoesInternas) : "")),
    itens: Array.isArray(itens) ? itens.map((it) => ({
      descricao: it.descricao ?? it.nome ?? it.produto?.descricao ?? it.produto?.nome ?? "",
      quantidade: Number(it.quantidade ?? it.qtde ?? it.qtd ?? it.produto?.quantidade ?? 0),
      unidade: it.unidade ?? it.un ?? it.produto?.unidade ?? "",
      observacao: it.observacao ?? it.obs ?? "",
      raw: it,
    })) : [],
    raw: p,
  };
}

module.exports = {
  DEFAULT_BASE_URL,
  blingGet,
  normContato,
  normPedidoVenda,
};
