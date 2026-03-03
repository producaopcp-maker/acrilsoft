/**
 * Central error handler:
 * - AJAX/JSON requests => { ok:false, error, code }
 * - Normal pages => flash-like message via querystring fallback
 */
function errorHandler(err, req, res, next) {
  try {
    const status = err.status || err.statusCode || 500;
    const code = err.code || "ERR_INTERNAL";
    const message = err.expose ? err.message : (status === 500 ? "Erro interno do servidor" : err.message);

    // Basic logging (keep stack server-side)
    console.error("[ERROR]", { status, code, path: req.originalUrl, message });
    if (err.stack) console.error(err.stack);

    const wantsJson =
      req.xhr ||
      (req.headers.accept && req.headers.accept.includes("application/json")) ||
      (req.headers["content-type"] && req.headers["content-type"].includes("application/json"));

    if (wantsJson) {
      return res.status(status).json({ ok: false, error: message, code });
    }

    // fallback for pages
    return res.status(status).send(
      `<!doctype html><html><head><meta charset="utf-8"><title>Erro</title>
      <style>body{font-family:system-ui,Segoe UI,Arial;margin:40px} .box{max-width:720px;padding:20px;border:1px solid #eee;border-radius:12px}</style>
      </head><body><div class="box"><h2>Ops!</h2><p>${escapeHtml(message)}</p><p><a href="javascript:history.back()">Voltar</a></p></div></body></html>`
    );
  } catch (e) {
    // Last resort
    return res.status(500).json({ ok: false, error: "Erro interno do servidor" });
  }
}

function escapeHtml(s) {
  return String(s || "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

module.exports = { errorHandler };
