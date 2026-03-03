# Acrilsoft — Mapa de Módulos (Organização)

Este projeto ainda roda com um `server.js` monolítico, porém a **organização por módulos** (para virar ERP) foi definida aqui para guiar o refactor e o crescimento.

## Módulos (ERP)

### Comercial
- **Pedidos** (`/pedidos`)
- **Novo pedido** (`/pedidos/novo`)
- **Bling** (pedidos) (`/bling/pedidos`)
- **Config Bling** (`/config/bling`)

### Produção
- **OPs** (Ordens de Produção) (`/ops`, `/ops/arquivadas`)
- **Serviços** (`/servicos`)

### Estoque
- **Estoque (placas/produtos)** (`/estoque`)
- **Movimentações / Histórico** (`/historico`)
- **Brindes** (`/estoque/brindes`, `/brindes/novo`, `/brindes/historico`)
- **Insumos** (`/insumos`)

### Cadastros
- **Fornecedores** (`/fornecedores`)
- **Usuários** (`/usuarios`)
- *(Produtos hoje estão ligados ao fluxo de estoque/chapas; quando virar ERP, pode virar um cadastro separado.)*

### Financeiro
- **Relatório Mensal** (`/relatorio/mensal`)
- *(Futuro: NF-e / faturamento / contas a pagar/receber / caixa.)*

### Configurações
- **Backup** (`/backup`)
- **Identidade visual** (`/config/branding`)
- **Bling** (`/config/bling`)

## Permissões (RBAC)
As permissões atuais estão em `middlewares/auth.js` e já cobrem a maior parte dos módulos:
- `dashboard`, `pedidos`, `novo_pedido`, `ops`, `estoque`, `insumos`, `servicos`, `fornecedores`, `relatorios`, `bling`, `backup`, `branding`, `usuarios`.

## Próximo passo do refactor
Criar routers por módulo em `src/modules/*` e fazer o `server.js` apenas registrar esses routers.
