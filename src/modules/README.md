# src/modules

Pasta reservada para o refactor por módulos.

**Objetivo:** cada módulo exporta um `router` (Express) e concentra rotas e regras daquele domínio.

Exemplo (futuro):

- `src/modules/pedidos/pedidos.routes.js`
- `src/modules/ops/ops.routes.js`
- `src/modules/estoque/estoque.routes.js`

Por enquanto, o sistema continua rodando pelo `server.js` original (sem mudanças funcionais).
