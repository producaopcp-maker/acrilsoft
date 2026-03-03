// app/utils/http.js
// Helpers para padronizar tratamento de erro em rotas.

function asyncHandler(fn) {
  return function wrapped(req, res, next) {
    Promise.resolve(fn(req, res, next)).catch(next);
  };
}

function sendError(res, message, status = 400, extra = {}) {
  // Padrão único para erros AJAX
  return res.status(status).json({ ok: false, error: message, ...extra });
}

module.exports = {
  asyncHandler,
  sendError,
};
