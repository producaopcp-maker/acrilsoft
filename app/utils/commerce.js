// app/utils/commerce.js
// Helpers de negócio (valores, itens, áreas, totais) usados em pedidos/orçamentos.

function arrify(v) {
  if (Array.isArray(v)) return v;
  if (v === undefined || v === null) return [];
  return [v];
}

function num(v) {
  if (v === undefined || v === null || v === '') return 0;
  let s = String(v).trim();
  if (!s) return 0;
  s = s.replace(/\s/g, '');
  // remove símbolos (R$, etc) e mantém apenas número/.,-
  s = s.replace(/[^0-9,\.\-]/g, '');
  // pt-BR: se tiver vírgula, assume vírgula como decimal e remove pontos de milhar
  if (s.includes(',')) {
    s = s.replace(/\./g, '').replace(',', '.');
  }
  const n = Number(s);
  return Number.isFinite(n) ? n : 0;
}

function calcAreaM2(largura, altura, unidadeDim) {
  const w = num(largura);
  const h = num(altura);
  if (!w || !h) return 0;
  const u = String(unidadeDim || 'MM').toUpperCase();
  if (u === 'CM') return (w * h) / 10000; // cm² -> m²
  return (w * h) / 1000000; // mm² -> m²
}

function recalcPedidoTotals(itens, descontoTipo, descontoValor, freteValor) {
  const subtotal = (itens || []).reduce((acc, it) => acc + num(it.total_item), 0);
  const dt = String(descontoTipo || 'VALOR').toUpperCase();
  const dv = num(descontoValor);
  const desconto = (dt === 'PERCENT') ? (subtotal * (dv / 100)) : dv;
  const frete = num(freteValor);
  const total = subtotal - desconto + frete;
  return {
    subtotal_itens: subtotal,
    desconto_tipo: (dt === 'PERCENT') ? 'PERCENT' : 'VALOR',
    desconto_valor: dv,
    frete_valor: frete,
    total: total,
  };
}

// Observação: alguns canais/integradores (ex.: Bling) usam status como "ABERTO".
// Mantemos esse status no workflow para evitar erro de validação ao atualizar/sincronizar.
const PEDIDO_STATUS = ['ABERTO', 'RASCUNHO', 'ORCAMENTO', 'APROVADO', 'EM_PRODUCAO', 'PRONTO', 'FATURADO', 'ENTREGUE', 'CANCELADO'];

module.exports = {
  arrify,
  num,
  calcAreaM2,
  recalcPedidoTotals,
  PEDIDO_STATUS,
};
