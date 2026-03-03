const express = require('express');
const router = express.Router();

const { db } = require('../db');
const { requireAuth, requireModule } = require('../middlewares/auth');

function clienteNomeExibicao(row) {
  if (!row) return '';
  const tipo = String(row.tipo || '').toUpperCase();
  if (tipo === 'PJ') return row.razao_social || row.nome || '';
  return row.nome || row.razao_social || '';
}

// LISTA
router.get('/clientes', requireModule('clientes'), (req, res) => {
  const q = String(req.query.q || '').trim();
  const tipo = String(req.query.tipo || '').trim().toUpperCase();

  let where = '1=1';
  const params = [];
  if (tipo === 'PF' || tipo === 'PJ') {
    where += " AND UPPER(COALESCE(tipo,'PF')) = ?";
    params.push(tipo);
  }
  if (q) {
    where += ' AND (nome LIKE ? OR razao_social LIKE ? OR fantasia LIKE ? OR cpf_cnpj LIKE ? OR cnpjcpf LIKE ? OR telefone LIKE ? OR whatsapp LIKE ? OR contato LIKE ?)';
    const like = `%${q}%`;
    params.push(like, like, like, like, like, like, like, like);
  }

  const clientes = db.prepare(`
    SELECT *
      , COALESCE(cpf_cnpj, cnpjcpf) as doc
    FROM clientes
    WHERE ${where}
    ORDER BY COALESCE(razao_social, nome) COLLATE NOCASE
    LIMIT 500
  `).all(...params).map(r => ({
    ...r,
    nome_exibicao: clienteNomeExibicao(r)
  }));

  res.render('layout', {
    title: 'Clientes',
    view: 'clientes-list-page',
    activeMenu: 'clientes',
    clientes,
    filtros: { q, tipo }
  });
});

// NOVO
router.get('/clientes/novo', requireModule('clientes'), (req, res) => {
  res.render('layout', {
    title: 'Novo Cliente',
    view: 'cliente-form-page',
    activeMenu: 'clientes',
    modo: 'novo',
    cliente: { tipo: 'PF' },
    endereco: null
  });
});

// API: obter dados completos de um cliente (auto-preenchimento)
router.get('/api/clientes/:id', requireAuth, (req, res) => {
  try {
    const id = Number(req.params.id);
    if (!id) return res.status(400).json({ error: 'ID inválido' });

    const row = db.prepare(`
      SELECT
        c.id,
        c.tipo,
        COALESCE(c.razao_social, c.nome) AS nome,
        c.fantasia,
        COALESCE(c.cpf_cnpj, c.cnpjcpf) AS documento,
        c.ie,
        c.email,
        COALESCE(c.whatsapp, c.telefone, c.contato) AS whatsapp,
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
      WHERE c.id = ?
      LIMIT 1
    `).get(id);

    if (!row) return res.status(404).json({ error: 'Cliente não encontrado' });
    return res.json(row);
  } catch (e) {
    return res.status(500).json({ error: 'Falha ao buscar cliente' });
  }
});

// CRIAR
router.post('/clientes', requireModule('clientes'), (req, res) => {
  try {
    const tipo = String(req.body.tipo || 'PF').toUpperCase() === 'PJ' ? 'PJ' : 'PF';
    const codigo = String(req.body.codigo || '').trim() || null;
    const nome = String(req.body.nome || '').trim();
    const fantasia = String(req.body.fantasia || '').trim() || null;
    const cpf_cnpj = String(req.body.cpf_cnpj || '').trim() || null;
    const ie = String(req.body.ie || '').trim() || null;
    const ie_isento = (String(req.body.ie_isento || '') === '1') ? 1 : 0;
    const im = String(req.body.im || '').trim() || null;
    const email = String(req.body.email || '').trim() || null;
    const telefone = String(req.body.telefone || '').trim() || null;
    const whatsapp = String(req.body.whatsapp || '').trim() || null;
    const contato = String(req.body.contato || '').trim() || null;
    const observacoes = String(req.body.observacoes || '').trim() || null;

    if (!nome) return res.status(400).send('Nome/Razão social é obrigatório.');

    const now = new Date().toISOString();
    const tx = db.transaction(() => {
      const info = db.prepare(`
        INSERT INTO clientes (tipo, codigo, nome, razao_social, fantasia, cpf_cnpj, ie, ie_isento, im, email, telefone, whatsapp, contato, observacoes, ativo, criado_em, atualizado_em)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?)
      `).run(
        tipo,
        codigo,
        (tipo === 'PF') ? nome : null,
        (tipo === 'PJ') ? nome : null,
        fantasia,
        cpf_cnpj,
        ie,
        ie_isento,
        im,
        email,
        telefone,
        whatsapp,
        contato,
        observacoes,
        now,
        now
      );
      const clienteId = info.lastInsertRowid;

      // endereço principal (ENTREGA)
      const cep = String(req.body.cep || '').trim() || null;
      const logradouro = String(req.body.logradouro || '').trim() || null;
      const numero = String(req.body.numero || '').trim() || null;
      const complemento = String(req.body.complemento || '').trim() || null;
      const bairro = String(req.body.bairro || '').trim() || null;
      const cidade = String(req.body.cidade || '').trim() || null;
      const uf = String(req.body.uf || '').trim() || null;
      const codigo_ibge_municipio = String(req.body.codigo_ibge_municipio || '').trim() || null;

      if (cep || logradouro || cidade || uf) {
        db.prepare(`
          INSERT INTO cliente_enderecos (cliente_id, tipo, cep, logradouro, numero, complemento, bairro, cidade, uf, codigo_ibge_municipio, principal, criado_em, atualizado_em)
          VALUES (?, 'ENTREGA', ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?)
        `).run(clienteId, cep, logradouro, numero, complemento, bairro, cidade, uf, codigo_ibge_municipio, now, now);
      }

      return clienteId;
    });

    const clienteId = tx();
    return res.redirect(`/clientes/${clienteId}`);
  } catch (e) {
    console.error(e);
    return res.status(400).send(`Falha ao salvar cliente: ${e.message}`);
  }
});

// DETALHE
router.get('/clientes/:id', requireModule('clientes'), (req, res) => {
  const id = Number(req.params.id);
  const c = db.prepare('SELECT * FROM clientes WHERE id = ?').get(id);
  if (!c) return res.status(404).send('Cliente não encontrado.');
  const endereco = db.prepare("SELECT * FROM cliente_enderecos WHERE cliente_id = ? AND principal = 1 ORDER BY id DESC LIMIT 1").get(id) || null;
  const pedidos = db.prepare('SELECT * FROM pedidos WHERE cliente_id = ? ORDER BY id DESC LIMIT 200').all(id);

  res.render('layout', {
    title: 'Cliente',
    view: 'cliente-detalhe-page',
    activeMenu: 'clientes',
    cliente: { ...c, nome_exibicao: clienteNomeExibicao(c) },
    endereco,
    pedidos
  });
});

// EDITAR
router.get('/clientes/:id/editar', requireModule('clientes'), (req, res) => {
  const id = Number(req.params.id);
  const c = db.prepare('SELECT * FROM clientes WHERE id = ?').get(id);
  if (!c) return res.status(404).send('Cliente não encontrado.');
  const endereco = db.prepare("SELECT * FROM cliente_enderecos WHERE cliente_id = ? AND principal = 1 ORDER BY id DESC LIMIT 1").get(id) || null;

  res.render('layout', {
    title: 'Editar Cliente',
    view: 'cliente-form-page',
    activeMenu: 'clientes',
    modo: 'editar',
    cliente: { ...c },
    endereco
  });
});

// SALVAR (editar)
router.post('/clientes/:id', requireModule('clientes'), (req, res) => {
  try {
    const id = Number(req.params.id);
    const exists = db.prepare('SELECT id FROM clientes WHERE id = ?').get(id);
    if (!exists) return res.status(404).send('Cliente não encontrado.');

    const tipo = String(req.body.tipo || 'PF').toUpperCase() === 'PJ' ? 'PJ' : 'PF';
    const codigo = String(req.body.codigo || '').trim() || null;
    const nome = String(req.body.nome || '').trim();
    const fantasia = String(req.body.fantasia || '').trim() || null;
    const cpf_cnpj = String(req.body.cpf_cnpj || '').trim() || null;
    const ie = String(req.body.ie || '').trim() || null;
    const ie_isento = (String(req.body.ie_isento || '') === '1') ? 1 : 0;
    const im = String(req.body.im || '').trim() || null;
    const email = String(req.body.email || '').trim() || null;
    const telefone = String(req.body.telefone || '').trim() || null;
    const whatsapp = String(req.body.whatsapp || '').trim() || null;
    const contato = String(req.body.contato || '').trim() || null;
    const observacoes = String(req.body.observacoes || '').trim() || null;

    if (!nome) return res.status(400).send('Nome/Razão social é obrigatório.');

    const now = new Date().toISOString();
    const tx = db.transaction(() => {
      db.prepare(`
        UPDATE clientes SET
          tipo = ?,
          codigo = ?,
          nome = ?,
          razao_social = ?,
          fantasia = ?,
          cpf_cnpj = ?,
          ie = ?,
          ie_isento = ?,
          im = ?,
          email = ?,
          telefone = ?,
          whatsapp = ?,
          contato = ?,
          observacoes = ?,
          atualizado_em = ?
        WHERE id = ?
      `).run(
        tipo,
        codigo,
        (tipo === 'PF') ? nome : null,
        (tipo === 'PJ') ? nome : null,
        fantasia,
        cpf_cnpj,
        ie,
        ie_isento,
        im,
        email,
        telefone,
        whatsapp,
        contato,
        observacoes,
        now,
        id
      );

      // endereço principal (ENTREGA)
      const cep = String(req.body.cep || '').trim() || null;
      const logradouro = String(req.body.logradouro || '').trim() || null;
      const numero = String(req.body.numero || '').trim() || null;
      const complemento = String(req.body.complemento || '').trim() || null;
      const bairro = String(req.body.bairro || '').trim() || null;
      const cidade = String(req.body.cidade || '').trim() || null;
      const uf = String(req.body.uf || '').trim() || null;
      const codigo_ibge_municipio = String(req.body.codigo_ibge_municipio || '').trim() || null;

      const existingEnd = db.prepare("SELECT id FROM cliente_enderecos WHERE cliente_id = ? AND principal = 1 ORDER BY id DESC LIMIT 1").get(id);
      if (existingEnd) {
        db.prepare(`
          UPDATE cliente_enderecos SET
            cep = ?, logradouro = ?, numero = ?, complemento = ?, bairro = ?, cidade = ?, uf = ?, codigo_ibge_municipio = ?, atualizado_em = ?
          WHERE id = ?
        `).run(cep, logradouro, numero, complemento, bairro, cidade, uf, codigo_ibge_municipio, now, existingEnd.id);
      } else if (cep || logradouro || cidade || uf) {
        db.prepare(`
          INSERT INTO cliente_enderecos (cliente_id, tipo, cep, logradouro, numero, complemento, bairro, cidade, uf, codigo_ibge_municipio, principal, criado_em, atualizado_em)
          VALUES (?, 'ENTREGA', ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?)
        `).run(id, cep, logradouro, numero, complemento, bairro, cidade, uf, codigo_ibge_municipio, now, now);
      }
    });

    tx();
    return res.redirect(`/clientes/${id}`);
  } catch (e) {
    console.error(e);
    return res.status(400).send(`Falha ao salvar cliente: ${e.message}`);
  }
});

module.exports = router;
