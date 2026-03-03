-- Minimal schema used just to validate shared writes.
-- Replace/extend with Acrilsoft schema as we migrate routes.
create table if not exists notes (
  id bigserial primary key,
  text text not null,
  created_at timestamptz not null default now()
);
