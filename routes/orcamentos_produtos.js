const express = require('express');
const router = express.Router();

const { db } = require('../db');
const { requireModule } = require('../middlewares/auth');
const { num } = require('../app/utils/commerce');

// ===== Helpers locais =====
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

function recalcOrcamentoProdutos(orcamento_id) {
  const itens = db.prepare('SELECT * FROM orcamentos_produtos_itens WHERE orcamento_id = ?').all(orcamento_id);
  for (const it of itens) {
    const qtd = num(it.quantidade);
    const vu = num(it.valor_unitario);
    const desc = num(it.desconto_item);
    const totalItem = (qtd * vu) - desc;
    if (Math.abs(num(it.total_item) - totalItem) > 0.00001) {
      db.prepare('UPDATE orcamentos_produtos_itens SET total_item = ? WHERE id = ?').run(totalItem, it.id);
    }
  }
  const subtotal = (itens || []).reduce((acc, it) => acc + num(it.total_item), 0);
  const orc = db.prepare('SELECT desconto, frete_valor FROM orcamentos_produtos WHERE id = ?').get(orcamento_id) || {};
  const desconto = num(orc.desconto);
  const frete = num(orc.frete_valor);
  const total = subtotal - desconto + frete;
  db.prepare("UPDATE orcamentos_produtos SET subtotal = ?, total = ?, atualizado_em = datetime('now') WHERE id = ?")
    .run(subtotal, total, orcamento_id);
}

// ===== Orçamentos (Produtos) =====
router.get('/orcamentos', requireModule('pedidos'), (req, res) => {
  const orcamentos = db.prepare(`
    SELECT o.*, 
           COALESCE(c.nome, o.cliente_nome_avulso, '—') AS cliente_nome
    FROM orcamentos_produtos o
    LEFT JOIN clientes c ON c.id = o.cliente_id
    ORDER BY o.id DESC
    LIMIT 500
  `).all();
  res.render('layout', {
    title: 'Orçamentos (Produtos)',
    view: 'orcamentos-produtos-page',
    orcamentos
  });
});

router.get('/orcamentos/novo', requireModule('pedidos'), (req, res) => {
  const clientes = loadClientesSelect(500);
  res.render('layout', {
    title: 'Novo Orçamento (Produtos)',
    view: 'orcamento-produto-form-page',
    orcamento: null,
    clientes
  });
});

router.post('/orcamentos/novo', requireModule('pedidos'), (req, res) => {
  try {
    const cliente_id = Number(req.body.cliente_id || 0) || null;
    const cliente_nome_avulso = (req.body.cliente_nome_avulso || '').trim() || null;
    const cliente_whatsapp = (req.body.cliente_whatsapp || '').trim() || null;
    const cliente_email = (req.body.cliente_email || '').trim() || null;
    const cliente_documento = (req.body.cliente_documento || '').trim() || null;
    const cliente_endereco = (req.body.cliente_endereco || '').trim() || null;
    const cliente_cidade = (req.body.cliente_cidade || '').trim() || null;
    const cliente_uf = (req.body.cliente_uf || '').trim() || null;
    const vendedor_nome = (req.body.vendedor_nome || '').trim() || null;
    const canal_venda = (req.body.canal_venda || '').trim() || null;
    const status = (req.body.status || 'RASCUNHO').trim().toUpperCase();
    const data_validade = (req.body.data_validade || '').trim() || null;
    const prazo_entrega = (req.body.prazo_entrega || '').trim() || null;
    const condicoes_pagamento = (req.body.condicoes_pagamento || '').trim() || null;
    const garantia = (req.body.garantia || '').trim() || null;
    const observacoes_vendedor = (req.body.observacoes_vendedor || '').trim() || null;
    const observacoes_internas = (req.body.observacoes_internas || '').trim() || null;

    const info = db.prepare(`
      INSERT INTO orcamentos_produtos
        (cliente_id, cliente_nome_avulso, cliente_whatsapp, cliente_email, cliente_documento, cliente_endereco, cliente_cidade, cliente_uf,
         vendedor_nome, canal_venda, status, data_validade,
         subtotal, desconto, frete_valor, total,
         prazo_entrega, condicoes_pagamento, garantia, layout_arquivo, layout_obs,
         observacoes_vendedor, observacoes_internas, criado_em, atualizado_em)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, 0, 0, 0, ?, ?, ?, NULL, NULL, ?, ?, datetime('now'), datetime('now'))
    `).run(
      cliente_id,
      (cliente_id ? null : cliente_nome_avulso),
      cliente_whatsapp,
      (cliente_id ? null : cliente_email),
      (cliente_id ? null : cliente_documento),
      (cliente_id ? null : cliente_endereco),
      (cliente_id ? null : cliente_cidade),
      (cliente_id ? null : cliente_uf),
      vendedor_nome,
      canal_venda,
      status,
      data_validade,
      prazo_entrega,
      condicoes_pagamento,
      garantia,
      observacoes_vendedor,
      observacoes_internas
    );
    const id = info.lastInsertRowid;
    res.redirect(`/orcamentos/${id}`);
  } catch (err) {
    console.error('Erro ao criar orçamento (produtos):', err);
    res.status(500).send(err.message);
  }
});

router.get('/orcamentos/:id(\\d+)', requireModule('pedidos'), (req, res) => {
  const id = Number(req.params.id);
  const orcamento = db.prepare(`
    SELECT o.*, 
           COALESCE(c.nome, o.cliente_nome_avulso, '—') AS cliente_nome,
           COALESCE(c.whatsapp, o.cliente_whatsapp) AS cliente_whatsapp_eff,
           COALESCE(c.email, o.cliente_email) AS cliente_email_eff,
           COALESCE(c.cpf_cnpj, o.cliente_documento) AS cliente_documento_eff,
           COALESCE(c.telefone, NULL) AS cliente_telefone_eff
    FROM orcamentos_produtos o
    LEFT JOIN clientes c ON c.id = o.cliente_id
    WHERE o.id = ?
  `).get(id);
  if (!orcamento) return res.status(404).send('Orçamento não encontrado');

  const itens = db.prepare(`
    SELECT i.*, cp.nome AS produto_nome, cp.unidade AS produto_unidade
    FROM orcamentos_produtos_itens i
    LEFT JOIN catalogo_produtos cp ON cp.id = i.produto_id
    WHERE i.orcamento_id = ?
    ORDER BY i.id ASC
  `).all(id);

  const clientes = loadClientesSelect(500);
  const catalogoProdutos = db.prepare('SELECT id, nome, sku, unidade, preco_venda, custo_unit FROM catalogo_produtos WHERE ativo=1 ORDER BY nome ASC').all();

  res.render('layout', {
    title: `Orçamento #${id} (Produtos)`,
    view: 'orcamento-produto-detalhe-page',
    orcamento,
    itens,
    clientes,
    catalogoProdutos
  });
});

router.post('/orcamentos/:id(\\d+)/atualizar', requireModule('pedidos'), (req, res) => {
  try {
    const id = Number(req.params.id);
    const cliente_id = Number(req.body.cliente_id || 0) || null;
    const cliente_nome_avulso = (req.body.cliente_nome_avulso || '').trim() || null;
    const cliente_whatsapp = (req.body.cliente_whatsapp || '').trim() || null;
    const cliente_email = (req.body.cliente_email || '').trim() || null;
    const cliente_documento = (req.body.cliente_documento || '').trim() || null;
    const cliente_endereco = (req.body.cliente_endereco || '').trim() || null;
    const cliente_cidade = (req.body.cliente_cidade || '').trim() || null;
    const cliente_uf = (req.body.cliente_uf || '').trim() || null;
    const vendedor_nome = (req.body.vendedor_nome || '').trim() || null;
    const canal_venda = (req.body.canal_venda || '').trim() || null;
    const status = (req.body.status || 'RASCUNHO').trim().toUpperCase();
    const data_validade = (req.body.data_validade || '').trim() || null;
    const prazo_entrega = (req.body.prazo_entrega || '').trim() || null;
    const condicoes_pagamento = (req.body.condicoes_pagamento || '').trim() || null;
    const garantia = (req.body.garantia || '').trim() || null;
    const layout_obs = (req.body.layout_obs || '').trim() || null;
    const desconto = num(req.body.desconto);
    const frete_valor = num(req.body.frete_valor);
    const observacoes_vendedor = (req.body.observacoes_vendedor || '').trim() || null;
    const observacoes_internas = (req.body.observacoes_internas || '').trim() || null;

    db.prepare(`
      UPDATE orcamentos_produtos
      SET cliente_id = ?,
          cliente_nome_avulso = ?,
          cliente_whatsapp = ?,
          cliente_email = ?,
          cliente_documento = ?,
          cliente_endereco = ?,
          cliente_cidade = ?,
          cliente_uf = ?,
          vendedor_nome = ?,
          canal_venda = ?,
          status = ?,
          data_validade = ?,
          desconto = ?,
          frete_valor = ?,
          prazo_entrega = ?,
          condicoes_pagamento = ?,
          garantia = ?,
          layout_obs = ?,
          observacoes_vendedor = ?,
          observacoes_internas = ?,
          atualizado_em = datetime('now')
      WHERE id = ?
    `).run(
      cliente_id,
      (cliente_id ? null : cliente_nome_avulso),
      cliente_whatsapp,
      (cliente_id ? null : cliente_email),
      (cliente_id ? null : cliente_documento),
      (cliente_id ? null : cliente_endereco),
      (cliente_id ? null : cliente_cidade),
      (cliente_id ? null : cliente_uf),
      vendedor_nome,
      canal_venda,
      status,
      data_validade,
      desconto,
      frete_valor,
      prazo_entrega,
      condicoes_pagamento,
      garantia,
      layout_obs,
      observacoes_vendedor,
      observacoes_internas,
      id
    );

    recalcOrcamentoProdutos(id);
    res.redirect(`/orcamentos/${id}`);
  } catch (err) {
    console.error('Erro ao atualizar orçamento (produtos):', err);
    res.status(500).send(err.message);
  }
});

router.post('/orcamentos/:id(\\d+)/itens/adicionar', requireModule('pedidos'), (req, res) => {
  try {
    const orcamento_id = Number(req.params.id);
    const tipo_item = String(req.body.tipo_item || 'SOB_MEDIDA').toUpperCase();

    let produto_id = null;
    let descricao = (req.body.descricao || '').trim() || null;
    let unidade = (req.body.unidade || 'UN').trim().toUpperCase();
    let custo_unitario = num(req.body.custo_unitario);
    let valor_unitario = num(req.body.valor_unitario);

    if (tipo_item === 'ESTOQUE') {
      produto_id = Number(req.body.produto_id || 0) || null;
      const p = produto_id ? db.prepare('SELECT nome, unidade, custo_unit, preco_venda FROM catalogo_produtos WHERE id = ?').get(produto_id) : null;
      if (p) {
        descricao = descricao || p.nome;
        unidade = (p.unidade || unidade);
        if (!custo_unitario) custo_unitario = num(p.custo_unit);
        if (!valor_unitario) valor_unitario = num(p.preco_venda);
      }
    }

    const quantidade = num(req.body.quantidade || 1) || 1;
    const desconto_item = num(req.body.desconto_item);
    const total_item = (quantidade * valor_unitario) - desconto_item;

    db.prepare(`
      INSERT INTO orcamentos_produtos_itens
        (orcamento_id, tipo_item, produto_id, descricao, categoria, material, largura_mm, altura_mm, espessura_mm,
         quantidade, unidade, custo_unitario, valor_unitario, desconto_item, total_item, observacao)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `).run(
      orcamento_id,
      (tipo_item === 'ESTOQUE' ? 'ESTOQUE' : 'SOB_MEDIDA'),
      produto_id,
      descricao,
      (req.body.categoria || '').trim() || null,
      (req.body.material || '').trim() || null,
      num(req.body.largura_mm) || null,
      num(req.body.altura_mm) || null,
      num(req.body.espessura_mm) || null,
      quantidade,
      unidade,
      custo_unitario,
      valor_unitario,
      desconto_item,
      total_item,
      (req.body.observacao || '').trim() || null
    );

    recalcOrcamentoProdutos(orcamento_id);
    res.redirect(`/orcamentos/${orcamento_id}`);
  } catch (err) {
    console.error('Erro ao adicionar item no orçamento (produtos):', err);
    res.status(500).send(err.message);
  }
});

router.post('/orcamentos/:id(\\d+)/itens/:itemId(\\d+)/atualizar', requireModule('pedidos'), (req, res) => {
  try {
    const orcamento_id = Number(req.params.id);
    const itemId = Number(req.params.itemId);
    const quantidade = num(req.body.quantidade || 1) || 1;
    const valor_unitario = num(req.body.valor_unitario);
    const desconto_item = num(req.body.desconto_item);
    const total_item = (quantidade * valor_unitario) - desconto_item;

    db.prepare(`
      UPDATE orcamentos_produtos_itens
      SET descricao = ?,
          categoria = ?,
          material = ?,
          largura_mm = ?,
          altura_mm = ?,
          espessura_mm = ?,
          quantidade = ?,
          unidade = ?,
          custo_unitario = ?,
          valor_unitario = ?,
          desconto_item = ?,
          total_item = ?,
          observacao = ?
      WHERE id = ? AND orcamento_id = ?
    `).run(
      (req.body.descricao || '').trim() || null,
      (req.body.categoria || '').trim() || null,
      (req.body.material || '').trim() || null,
      (num(req.body.largura_mm) || null),
      (num(req.body.altura_mm) || null),
      (num(req.body.espessura_mm) || null),
      quantidade,
      (req.body.unidade || 'UN').trim().toUpperCase(),
      num(req.body.custo_unitario),
      valor_unitario,
      desconto_item,
      total_item,
      (req.body.observacao || '').trim() || null,
      itemId,
      orcamento_id
    );

    recalcOrcamentoProdutos(orcamento_id);
    res.redirect(`/orcamentos/${orcamento_id}`);
  } catch (err) {
    console.error('Erro ao atualizar item do orçamento (produtos):', err);
    res.status(500).send(err.message);
  }
});

router.post('/orcamentos/:id(\\d+)/itens/:itemId(\\d+)/excluir', requireModule('pedidos'), (req, res) => {
  try {
    const orcamento_id = Number(req.params.id);
    const itemId = Number(req.params.itemId);
    db.prepare('DELETE FROM orcamentos_produtos_itens WHERE id = ? AND orcamento_id = ?').run(itemId, orcamento_id);
    recalcOrcamentoProdutos(orcamento_id);
    res.redirect(`/orcamentos/${orcamento_id}`);
  } catch (err) {
    console.error('Erro ao excluir item do orçamento (produtos):', err);
    res.status(500).send(err.message);
  }
});

router.post('/orcamentos/:id(\\d+)/converter-pedido', requireModule('pedidos'), (req, res) => {
  try {
    const orcamento_id = Number(req.params.id);
    const orc = db.prepare('SELECT * FROM orcamentos_produtos WHERE id = ?').get(orcamento_id);
    if (!orc) return res.status(404).send('Orçamento não encontrado');
    if (orc.pedido_id) return res.redirect(`/pedidos/${orc.pedido_id}`);

    const itens = db.prepare(`
      SELECT i.*, cp.unidade AS produto_unidade
      FROM orcamentos_produtos_itens i
      LEFT JOIN catalogo_produtos cp ON cp.id = i.produto_id
      WHERE i.orcamento_id = ?
      ORDER BY i.id ASC
    `).all(orcamento_id);

    const tx = db.transaction(() => {
      const totals = {
        subtotal_itens: num(orc.subtotal),
        desconto_tipo: 'VALOR',
        desconto_valor: num(orc.desconto),
        frete_valor: num(orc.frete_valor),
        total: num(orc.total)
      };

      const pedido = db.prepare(`
        INSERT INTO pedidos
          (cliente_id, cliente_nome_avulso, cliente_whatsapp, cliente_email, cliente_documento, cliente_endereco,
           cliente_cidade, cliente_uf,
           status, data_pedido,
           subtotal_itens, desconto_tipo, desconto_valor, frete_valor, total,
           observacoes_vendedor, observacoes_internas, canal_venda, vendedor_nome,
           criado_em, atualizado_em)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'ABERTO', date('now'), ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'), datetime('now'))
      `).run(
        orc.cliente_id || null,
        orc.cliente_nome_avulso || null,
        orc.cliente_whatsapp || null,
        orc.cliente_email || null,
        orc.cliente_documento || null,
        orc.cliente_endereco || null,
        orc.cliente_cidade || null,
        orc.cliente_uf || null,
        totals.subtotal_itens,
        totals.desconto_tipo,
        totals.desconto_valor,
        totals.frete_valor,
        totals.total,
        orc.observacoes_vendedor || null,
        orc.observacoes_internas || null,
        orc.canal_venda || null,
        orc.vendedor_nome || null
      );

      const pedido_id = Number(pedido.lastInsertRowid);

      for (const it of itens) {
        const qtd = num(it.quantidade) || 1;
        const valor_unitario = num(it.valor_unitario);
        const desconto_item = num(it.desconto_item);
        const total_item = (qtd * valor_unitario) - desconto_item;

        db.prepare(`
          INSERT INTO pedido_itens
            (pedido_id, tipo_item, produto_id, descricao, categoria, material, largura_mm, altura_mm, espessura_mm,
             quantidade, unidade, custo_unitario, valor_unitario, desconto_item, total_item, observacao)
          VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        `).run(
          pedido_id,
          it.tipo_item || 'SOB_MEDIDA',
          it.produto_id || null,
          it.descricao || null,
          it.categoria || null,
          it.material || null,
          it.largura_mm || null,
          it.altura_mm || null,
          it.espessura_mm || null,
          qtd,
          (it.produto_unidade || it.unidade || 'UN'),
          num(it.custo_unitario),
          valor_unitario,
          desconto_item,
          total_item,
          it.observacao || null
        );
      }

      db.prepare("UPDATE orcamentos_produtos SET pedido_id = ?, status = ?, atualizado_em = datetime('now') WHERE id = ?")
        .run(pedido_id, 'CONVERTIDO', orcamento_id);

      return pedido_id;
    });

    const pedido_id = tx();
    return res.redirect(`/pedidos/${pedido_id}`);
  } catch (err) {
    console.error('Erro ao converter orçamento em pedido:', err);
    res.status(500).send('Erro ao converter orçamento em pedido: ' + (err.message || err));
  }
});

module.exports = router;
