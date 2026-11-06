-- =========================================================
-- data-platform Postgres — silver layer (SCD Type 2 history of shop.*), staging, ETL checkpoints
-- Design: docs/dataplatform-design.md §6.
--
-- Applied to DB `data-platform` by the one-shot `postgres-setup` job (postgres/scripts/init.sh,
-- compose/infra.yml) on every `up`, so every statement is idempotent: re-running changes nothing.
-- Manual run (from the repo root):
--   docker exec -i postgres sh -c 'psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB"' < postgres/schema.sql
-- =========================================================

CREATE SCHEMA IF NOT EXISTS config;

-- One row per (pipeline step, table): where the next incremental run starts.
CREATE TABLE IF NOT EXISTS config.etl_checkpoint (
    pipeline         text        NOT NULL,   -- 'silver' | 'es_sync'
    table_name       text        NOT NULL,   -- target, e.g. 'silver.orders'
    source_table     text        NOT NULL,   -- e.g. 'lakehouse.bronze.orders_cdc'
    key_columns      text[],                 -- e.g. '{order_id}'; only for a generic silver job
    last_snapshot_id bigint,                 -- silver: last applied bronze snapshot (NULL = full read)
    last_watermark   timestamptz,            -- es_sync: max _silver_updated_at processed
    is_active        boolean     NOT NULL DEFAULT true,
    updated_at       timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (pipeline, table_name)
);

-- =========================================================
-- Silver tables: one row per version of a source row.
--
-- Rules shared by the three tables:
-- - Source columns keep the source types, except strings are `text`: widening a varchar at the
--   source must not break the apply. They are nullable (only the key is NOT NULL): columns added
--   later by the apply step (ALTER TABLE ... ADD COLUMN) are nullable anyway, and silver mirrors
--   the source instead of re-validating it — no CHECKs and no foreign keys (tables are processed
--   independently, so an order may land before its user, design §6.2).
-- - PRIMARY KEY (<key>, _lsn): a version is identified by the event that produced it, so applying
--   the same event twice fails loudly instead of creating a duplicate version.
-- - _is_current is true exactly when _valid_to is NULL (CHECK) and for at most one version per key
--   (partial unique index). A deleted key keeps its last version current, with _is_deleted = true.
-- - _silver_updated_at: when silver last wrote the row (insert or close) — the watermark for ES
--   and gold, hence the index.
-- - _tx_id: the source transaction of the version. A sink commit can split one transaction's
--   events across two silver runs; the apply then replaces the version of the first run instead
--   of closing it, which would leave a zero-length version (_valid_to = _valid_from).
-- =========================================================

CREATE SCHEMA IF NOT EXISTS silver;

-- ---------------------------------------------------------
-- users
-- ---------------------------------------------------------
CREATE TABLE IF NOT EXISTS silver.users (
    user_id             bigint      NOT NULL,
    email               text,
    full_name           text,
    phone               text,
    city                text,
    status              text,
    created_at          timestamptz,
    updated_at          timestamptz,
    -- SCD2 / technical
    _valid_from         timestamptz NOT NULL,                -- source.ts_ms of the event
    _valid_to           timestamptz,                         -- next version's _valid_from; NULL = current
    _is_current         boolean     NOT NULL,
    _is_deleted         boolean     NOT NULL DEFAULT false,
    _lsn                bigint      NOT NULL,                -- source.lsn of the event
    _tx_id              bigint,                              -- source.txId: see silver_apply (split transactions)
    _silver_updated_at  timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (user_id, _lsn),
    CHECK ((_valid_to IS NULL) = _is_current),
    CHECK (_valid_to IS NULL OR _valid_to >= _valid_from)
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_users_current
    ON silver.users (user_id) WHERE _is_current;
CREATE INDEX IF NOT EXISTS idx_users_silver_updated_at
    ON silver.users (_silver_updated_at);

-- ---------------------------------------------------------
-- orders
-- ---------------------------------------------------------
CREATE TABLE IF NOT EXISTS silver.orders (
    order_id            bigint        NOT NULL,
    user_id             bigint,
    status              text,
    payment_method      text,
    shipping_city       text,
    total_amount        numeric(18,2),
    currency            text,
    ordered_at          timestamptz,
    created_at          timestamptz,
    updated_at          timestamptz,
    -- SCD2 / technical
    _valid_from         timestamptz   NOT NULL,
    _valid_to           timestamptz,
    _is_current         boolean       NOT NULL,
    _is_deleted         boolean       NOT NULL DEFAULT false,
    _lsn                bigint        NOT NULL,
    _tx_id              bigint,
    _silver_updated_at  timestamptz   NOT NULL DEFAULT now(),
    PRIMARY KEY (order_id, _lsn),
    CHECK ((_valid_to IS NULL) = _is_current),
    CHECK (_valid_to IS NULL OR _valid_to >= _valid_from)
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_orders_current
    ON silver.orders (order_id) WHERE _is_current;
CREATE INDEX IF NOT EXISTS idx_orders_silver_updated_at
    ON silver.orders (_silver_updated_at);
-- ES sync: a user who changed must refresh all of their current orders (design §7)
CREATE INDEX IF NOT EXISTS idx_orders_current_user_id
    ON silver.orders (user_id) WHERE _is_current;

-- ---------------------------------------------------------
-- order_details
-- ---------------------------------------------------------
CREATE TABLE IF NOT EXISTS silver.order_details (
    order_detail_id     bigint        NOT NULL,
    order_id            bigint,
    line_number         smallint,
    sku                 text,
    product_name        text,
    category            text,
    quantity            integer,
    unit_price          numeric(18,2),
    discount_amount     numeric(18,2),
    line_total          numeric(18,2),
    created_at          timestamptz,
    updated_at          timestamptz,
    -- SCD2 / technical
    _valid_from         timestamptz   NOT NULL,
    _valid_to           timestamptz,
    _is_current         boolean       NOT NULL,
    _is_deleted         boolean       NOT NULL DEFAULT false,
    _lsn                bigint        NOT NULL,
    _tx_id              bigint,
    _silver_updated_at  timestamptz   NOT NULL DEFAULT now(),
    PRIMARY KEY (order_detail_id, _lsn),
    CHECK ((_valid_to IS NULL) = _is_current),
    CHECK (_valid_to IS NULL OR _valid_to >= _valid_from)
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_order_details_current
    ON silver.order_details (order_detail_id) WHERE _is_current;
CREATE INDEX IF NOT EXISTS idx_order_details_silver_updated_at
    ON silver.order_details (_silver_updated_at);
-- ES sync: an order document is rebuilt from its current lines (design §7)
CREATE INDEX IF NOT EXISTS idx_order_details_current_order_id
    ON silver.order_details (order_id) WHERE _is_current;

-- =========================================================
-- Staging: hand-over from the Spark job (bronze -> staging) to silver_apply (staging -> silver).
-- staging.<table> is dropped and recreated by every Spark run (JDBC overwrite), so it is not
-- declared here and always has the current bronze columns. staging.batch is truncated and
-- refilled by the same run (JDBC overwrite + truncate keeps this definition): one row per table
-- telling silver_apply which bronze range staging.<table> holds.
-- =========================================================

CREATE SCHEMA IF NOT EXISTS staging;

CREATE TABLE IF NOT EXISTS staging.batch (
    table_name        text        PRIMARY KEY,   -- target, e.g. 'silver.orders'
    start_snapshot_id bigint,                    -- checkpoint the read started after (NULL = full read)
    end_snapshot_id   bigint,                    -- bronze snapshot read up to -> the new checkpoint
    row_count         bigint      NOT NULL,      -- rows in staging.<table>; 0 = nothing to apply
    created_at        timestamptz NOT NULL DEFAULT now(),
    -- true = job_silver --full-refresh staged the whole bronze table: silver_apply replaces the
    -- silver table, then sets it back to false (a full refresh is applied once, never again by a
    -- re-run). Explicit, not inferred from start IS NULL: the very first batch has no start either
    full_refresh      boolean     NOT NULL DEFAULT false
);
-- databases created before the column existed (same position: last)
ALTER TABLE staging.batch ADD COLUMN IF NOT EXISTS full_refresh boolean NOT NULL DEFAULT false;

-- ---------------------------------------------------------
-- Checkpoint rows for the silver step (NULL snapshot = first run reads the whole bronze table).
-- ON CONFLICT DO NOTHING: re-running never resets a checkpoint that has already advanced.
-- ---------------------------------------------------------
INSERT INTO config.etl_checkpoint (pipeline, table_name, source_table, key_columns)
VALUES
    ('silver', 'silver.users',         'lakehouse.bronze.users_cdc',         '{user_id}'),
    ('silver', 'silver.orders',        'lakehouse.bronze.orders_cdc',        '{order_id}'),
    ('silver', 'silver.order_details', 'lakehouse.bronze.order_details_cdc', '{order_detail_id}')
ON CONFLICT (pipeline, table_name) DO NOTHING;
