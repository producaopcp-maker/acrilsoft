// middlewares/auth.js

// Controle por módulos (RBAC simples).
// Se quiser granular mais depois, dá pra trocar por permissões em tabela.
const ROLE_MODULES = {
  admin: ['*'],
  vendedor: ['dashboard', 'pedidos', 'novo_pedido', 'clientes', 'integracoes', 'logistica', 'brindes'],
  operador: ['dashboard', 'estoque', 'insumos', 'servicos', 'ops', 'fornecedores', 'relatorios', 'pedidos', 'bling', 'novo_pedido', 'clientes', 'fiscal', 'integracoes', 'logistica', 'brindes'],
  financeiro: ['dashboard', 'financeiro', 'relatorios', 'fiscal', 'integracoes', 'logistica'],
  producao: ['dashboard', 'ops', 'servicos', 'insumos', 'pedidos', 'novo_pedido', 'clientes', 'integracoes', 'logistica', 'brindes'],
};

function roleHasModule(role, moduleKey) {
  const mods = ROLE_MODULES[String(role || '').toLowerCase()] || [];
  return mods.includes('*') || mods.includes(moduleKey);
}

function userHasModule(user, moduleKey) {
  if (!user) return false;
  const mods = user.modulos;
  if (Array.isArray(mods) && mods.length > 0) {
    return mods.includes('*') || mods.includes(moduleKey);
  }
  return roleHasModule(user.role, moduleKey);
}

function requireAuth(req, res, next) {
  if (req.session?.user) return next();
  return res.redirect('/login');
}

function requireModule(moduleKey) {
  return (req, res, next) => {
    const u = req.session?.user;
    if (!u) return res.redirect('/login');
    if (userHasModule(u, moduleKey)) return next();
    return res.status(403).render('layout', {
      view: 'forbidden-page',
      title: 'Sem permissão',
      pageTitle: 'Sem permissão',
      pageSubtitle: 'Você não tem acesso a este módulo',
      activeMenu: ''
    });
  };
}


function requireRole(roles = []) {
  return (req, res, next) => {
    const u = req.session?.user;
    if (!u) return res.redirect('/login');
    if (!Array.isArray(roles) || roles.length === 0) return next();
    if (roles.includes(u.role)) return next();
    return res.status(403).send('Sem permissão.');
  };
}

module.exports = {ROLE_MODULES, roleHasModule, userHasModule, requireAuth, requireModule, requireRole };
