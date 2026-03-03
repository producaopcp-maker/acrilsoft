/**
 * Security hardening without extra dependencies.
 * - Trust proxy when running behind Railway/Render/Nginx
 * - Safer session cookies
 * - Basic security headers
 */
function securityHeaders(req, res, next) {
  res.setHeader("X-Content-Type-Options", "nosniff");
  res.setHeader("X-Frame-Options", "SAMEORIGIN");
  res.setHeader("Referrer-Policy", "same-origin");
  res.setHeader("Permissions-Policy", "geolocation=(), microphone=(), camera=()");
  // Do NOT set CSP here because the app uses inline scripts/styles in some views.
  next();
}

function applyTrustProxy(app) {
  // When behind a proxy (Railway/Render), secure cookies need trust proxy enabled.
  // Respect explicit override TRUST_PROXY=0/1
  const v = process.env.TRUST_PROXY;
  if (v === "0") return;
  if (v === "1" || process.env.NODE_ENV === "production") {
    app.set("trust proxy", 1);
  }
}

function buildSessionCookie() {
  const isProd = process.env.NODE_ENV === "production";
  return {
    maxAge: 1000 * 60 * 60 * 10, // 10h
    httpOnly: true,
    sameSite: "lax",
    secure: isProd, // requires trust proxy when behind HTTPS terminator
  };
}

module.exports = { securityHeaders, applyTrustProxy, buildSessionCookie };
