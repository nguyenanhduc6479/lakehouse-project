-- =========================================================
-- Source Postgres (OLTP) — schema for the data platform
-- Tables: shop.users, shop.orders, shop.order_details
--
-- Applied by the one-shot `source-db-setup` job (compose/source.yml) on every `up`, so every
-- statement is idempotent: re-running changes nothing.
--
-- psql variable (passed with -v by source-db-setup, from .env):
--   debezium_password  <- SOURCE_POSTGRES_DB_DEBEZIUM_PASSWORD
-- Manual run:
--   psql -v ON_ERROR_STOP=1 -v debezium_password=... -f source/schema.sql
-- =========================================================

CREATE SCHEMA IF NOT EXISTS shop;

-- ---------------------------------------------------------
-- Shared trigger function: keep updated_at current on UPDATE
-- ---------------------------------------------------------
CREATE OR REPLACE FUNCTION shop.set_updated_at()
RETURNS trigger AS $$
BEGIN
    NEW.updated_at := now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- ---------------------------------------------------------
-- users
-- ---------------------------------------------------------
CREATE TABLE IF NOT EXISTS shop.users (
    user_id      bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    email        varchar(255) NOT NULL UNIQUE,
    full_name    varchar(150) NOT NULL,
    phone        varchar(20),
    city         varchar(100),
    status       varchar(20)  NOT NULL DEFAULT 'active'
                 CHECK (status IN ('active', 'locked', 'closed')),
    created_at   timestamptz  NOT NULL DEFAULT now(),
    updated_at   timestamptz  NOT NULL DEFAULT now()
);

CREATE OR REPLACE TRIGGER trg_users_updated_at
BEFORE UPDATE ON shop.users
FOR EACH ROW EXECUTE FUNCTION shop.set_updated_at();

-- ---------------------------------------------------------
-- orders
-- ---------------------------------------------------------
CREATE TABLE IF NOT EXISTS shop.orders (
    order_id         bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    user_id          bigint        NOT NULL REFERENCES shop.users (user_id),
    status           varchar(20)   NOT NULL DEFAULT 'pending'
                     CHECK (status IN ('pending', 'paid', 'shipped',
                                       'completed', 'cancelled', 'refunded')),
    payment_method   varchar(20)
                     CHECK (payment_method IN ('cod', 'card', 'bank_transfer', 'e_wallet')),
    shipping_city    varchar(100),
    total_amount     numeric(18,2) NOT NULL DEFAULT 0 CHECK (total_amount >= 0),
    currency         char(3)       NOT NULL DEFAULT 'VND',
    ordered_at       timestamptz   NOT NULL DEFAULT now(),
    created_at       timestamptz   NOT NULL DEFAULT now(),
    updated_at       timestamptz   NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_orders_user_id    ON shop.orders (user_id);
CREATE INDEX IF NOT EXISTS idx_orders_status     ON shop.orders (status);
CREATE INDEX IF NOT EXISTS idx_orders_ordered_at ON shop.orders (ordered_at);

CREATE OR REPLACE TRIGGER trg_orders_updated_at
BEFORE UPDATE ON shop.orders
FOR EACH ROW EXECUTE FUNCTION shop.set_updated_at();

-- ---------------------------------------------------------
-- order_details
-- ---------------------------------------------------------
CREATE TABLE IF NOT EXISTS shop.order_details (
    order_detail_id  bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    order_id         bigint        NOT NULL REFERENCES shop.orders (order_id) ON DELETE CASCADE,
    line_number      smallint      NOT NULL,
    sku              varchar(50)   NOT NULL,
    product_name     varchar(200)  NOT NULL,
    category         varchar(100),
    quantity         integer       NOT NULL CHECK (quantity > 0),
    unit_price       numeric(18,2) NOT NULL CHECK (unit_price >= 0),
    discount_amount  numeric(18,2) NOT NULL DEFAULT 0 CHECK (discount_amount >= 0),
    -- = quantity * unit_price - discount_amount, computed by the application (a generated column
    -- would not be sent by logical replication before PostgreSQL 18)
    line_total       numeric(18,2) NOT NULL CHECK (line_total >= 0),
    created_at       timestamptz   NOT NULL DEFAULT now(),
    updated_at       timestamptz   NOT NULL DEFAULT now(),
    UNIQUE (order_id, line_number)
);

CREATE INDEX IF NOT EXISTS idx_order_details_order_id ON shop.order_details (order_id);
CREATE INDEX IF NOT EXISTS idx_order_details_sku      ON shop.order_details (sku);

CREATE OR REPLACE TRIGGER trg_order_details_updated_at
BEFORE UPDATE ON shop.order_details
FOR EACH ROW EXECUTE FUNCTION shop.set_updated_at();

-- =========================================================
-- CDC setup for Debezium
-- (needs wal_level = logical: set by `postgres -c wal_level=logical` in compose/source.yml)
-- =========================================================

-- Write the full old row into the WAL for UPDATE/DELETE events
ALTER TABLE shop.users         REPLICA IDENTITY FULL;
ALTER TABLE shop.orders        REPLICA IDENTITY FULL;
ALTER TABLE shop.order_details REPLICA IDENTITY FULL;

-- Publication with only the three tracked tables. There is no CREATE PUBLICATION IF NOT EXISTS:
-- build the statement and run it (\gexec) only when the publication is missing.
SELECT 'CREATE PUBLICATION dbz_publication FOR TABLE shop.users, shop.orders, shop.order_details'
WHERE NOT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = 'dbz_publication')
\gexec

-- Dedicated Debezium user: replication + read only. There is no CREATE ROLE IF NOT EXISTS, and psql
-- does not substitute :'vars' inside DO $$ … $$ blocks, hence \gexec again. The password is
-- re-applied on every run so it always matches .env.
SELECT format('CREATE ROLE debezium WITH LOGIN REPLICATION PASSWORD %L', :'debezium_password')
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'debezium')
\gexec
ALTER ROLE debezium WITH LOGIN REPLICATION PASSWORD :'debezium_password';

GRANT USAGE  ON SCHEMA shop TO debezium;
GRANT SELECT ON ALL TABLES IN SCHEMA shop TO debezium;
-- tables created later in shop (e.g. schema-evolution tests) are readable by Debezium too
ALTER DEFAULT PRIVILEGES IN SCHEMA shop GRANT SELECT ON TABLES TO debezium;

-- Heartbeat table: Debezium upserts its single row every heartbeat (heartbeat.action.query in
-- connect/connectors/source-shop.json). pgoutput skips transactions that touch no published
-- table, so while shop.* is idle Debezium would never see a newer LSN and the slot would keep all
-- WAL written meanwhile (checkpoints, other tables). The table is in the publication but not in
-- the connector's table.include.list: it advances the slot, but produces no Kafka topic.
CREATE TABLE IF NOT EXISTS public.debezium_heartbeat (
    id  smallint    PRIMARY KEY,
    ts  timestamptz NOT NULL DEFAULT now()
);
GRANT SELECT, INSERT, UPDATE ON public.debezium_heartbeat TO debezium;

SELECT 'ALTER PUBLICATION dbz_publication ADD TABLE public.debezium_heartbeat'
WHERE NOT EXISTS (SELECT 1 FROM pg_publication_tables
                  WHERE pubname = 'dbz_publication'
                    AND schemaname = 'public' AND tablename = 'debezium_heartbeat')
\gexec
