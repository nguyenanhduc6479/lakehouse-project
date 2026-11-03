# Roadmap

The build order for the data platform described in [dataplatform-design.md](dataplatform-design.md).
Each phase ends with something you can see working; don't start a phase before the previous one's
**Done when** checks pass. A finished phase records what was built and its **Lessons learned**. Vietnamese version: [roadmap.vi.md](roadmap.vi.md).

| # | Phase | Status |
|---|---|---|
| 1 | Clean slate | Done |
| 2 | Source | Done |
| 3 | CDC | Not started |
| 4 | Bronze | Not started |
| 5 | Silver | Not started |
| 6 | Gold | Not started |
| 7 | Search + API | Not started |
| 8 | Maintenance + test scenarios | Not started |

```
generator → source-db ─(2)─→ Debezium → Kafka ─(3)─→ Iceberg bronze ─(4)─→ silver ─(5)─→ gold ─(6)─→ Superset
                                                                              └──────────→ ES → API ─(7)─→ web page
```

---

## Phase 1 — Clean slate *(done)*

**Goal:** start from reusable infrastructure only.

- Removed the old vnstock OHLCV pipeline (Spark jobs, ingest, DAG, tests) and the old docs.
- Removed DataHub; moved Schema Registry into `compose/infra.yml` (needed for Avro).
- Kept: Postgres, Kafka + Zookeeper, Schema Registry, Elasticsearch, object storage (now RustFS), Polaris, Spark, Trino,
  Airflow, Grafana/Loki/Alloy.
- Rebuilt from empty volumes (`docker compose down -v`); both Postgres instances on `postgres:16`.
- Infra hardening (2026-11-02): switched to the native Docker Engine in WSL; MinIO → RustFS
  (`S3_*` variables, one bucket `data-platform`); removed `spark-thrift` and dbt-spark; every
  image pinned; `TZ` on every long-running service; Kafka transaction log at RF 1; Spark event logs
  off and `spark.master` fixed (REST jobs used to run in `local[*]`); Grafana now shows the logs of
  every container (Docker json-file logs, startup gated on Alloy) and of Spark jobs.
- Verified: all 21 services up; every service reachable by name on the `lakehouse` network
  (TCP + app-level: Airflow connections, Polaris OAuth2, Kafka round-trip, Schema Registry write);
  `trino_conn` fixed (it had no user → HTTP 401).

**Lessons learned:**
- Check that a setting really took effect, not just that the service is up: Spark REST jobs ran
  in `local[*]` without any error, Polaris runs in-memory unless persistence is set, and
  `curl -s` exits 0 on HTTP 4xx/5xx.
- Log collection must not depend on timing: Docker-API discovery missed one-shot jobs that exit
  in under a second; tailing Docker's json-file logs and gating startup on Alloy catches everything.
- Pin versions and expect upstreams to change: MinIO stopped publishing images; keeping every
  engine on the same `S3_*` names made the switch to RustFS a one-file change.
- Single-file bind mounts break when an editor replaces the file: `up -d --force-recreate`, not
  `restart`.

## Phase 2 — Source *(done)*

**Goal:** a Postgres OLTP database that changes continuously and is ready for CDC.

- [x] `source-db` instance (`compose/source.yml`): database `source-db`, host port 5433,
  `wal_level=logical`, `max_slot_wal_keep_size=1GB`.
- [x] `source/schema.sql`: schema `shop` with `users`, `orders`, `order_details` (design §3.1) —
  `varchar` + `CHECK` statuses, `line_total` as a plain column, `updated_at` trigger,
  `ON DELETE CASCADE` on `order_details.order_id`.
- [x] CDC setup (design §3.2) in the same script, applied by the idempotent one-shot
  `source-db-setup` job: `REPLICA IDENTITY FULL` on the three tables, publication
  `dbz_publication`, role `debezium` (`REPLICATION` + `SELECT` on `shop`, password from
  `SOURCE_POSTGRES_DB_DEBEZIUM_PASSWORD` via a psql variable). Verified 2026-11-02: applied twice
  (second run changes nothing), `debezium` logs in over the network with the `.env` password and
  can create a logical slot and decode changes (UPDATE/DELETE carry the full old row; a cascaded
  delete gives two DELETEs in one transaction).
- [x] `source/generator.py` (Python 3.12 + Faker 40.44 + psycopg 3.3, `--rate` events/second):
  create orders in one transaction, move statuses forward, create/update/lock users, occasionally
  delete an order. Pure helpers unit-tested (`tests/test_generator.py`, host).
- [x] `generator` Compose service (image `custom-generator:0.1.0`) under profile `generator`
  (started and stopped by hand), rate from `GENERATOR_RATE` in `.env`. Verified 2026-11-02: rows
  grow live (~8 orders / 10 s at rate 2), every `total_amount` equals the sum of its lines, no
  transaction left open, `docker compose stop` exits cleanly in ~1 s, logs reach Grafana.

**Done when:**
- `docker compose --profile generator up -d generator` → row counts in `shop.*` keep growing in
  DataGrip / `psql`.
- `SELECT * FROM pg_publication_tables WHERE pubname = 'dbz_publication'` lists the three tables.
- `\du debezium` shows the `Replication` attribute.

**Lessons learned:**
- CDC only sees committed transactions. A psycopg connection without `autocommit=True` kept one
  implicit transaction open, so nothing would reach Debezium until the generator exited. Know where
  each transaction really ends.
- Transaction boundaries at the source drive downstream consistency: one order (`orders` +
  `order_details`) is one transaction but lands in two topics — accepted on purpose, silver is
  eventually consistent across tables.
- CDC settings live at two levels: instance (`wal_level`, slot WAL retention — hence a separate
  `source-db`) and table (replica identity, publication). Not every column travels through WAL:
  generated columns don't before PostgreSQL 18, so `line_total` is a plain column.
- Setup scripts run on every `up`, so they must be idempotent; Postgres lacks `IF NOT EXISTS` for
  roles and publications (`SELECT format(...) WHERE NOT EXISTS ... \gexec`). Secrets go in as psql
  variables, which are not substituted inside `DO $$`.
- Test the real path: inside the Postgres container `localhost` logins are trusted, so a wrong
  password still works — test over the network (`-h source-db`).
- Python as PID 1 ignores SIGTERM unless it handles it; look at generated data, not just counts
  (Faker `vi_VN.first_name()` only returns "John"/"Jane").
- `timestamptz` stores an instant; the time zone is only how a client displays it (DataGrip showed
  +00 while the server was `Asia/Ho_Chi_Minh` — fix the client, not the data).

## Phase 3 — CDC

**Goal:** every change in `source-db` arrives in Kafka as an Avro event.

- [ ] Kafka Connect image on `confluentinc/cp-kafka-connect:7.6.0` + the Debezium Postgres plugin
  (pin a version that supports Kafka 3.6).
- [ ] New module `compose/connect.yml`: service `kafka-connect` (REST 8083 internal), internal
  topics with replication factor 1, `config.providers=env` (`EnvVarConfigProvider`) so connector
  configs read secrets from `.env`.
- [ ] One-shot `connect-setup` job: registers the Debezium connector (design §4.2) through the
  Connect REST API — idempotent, checks every HTTP code.

**Done when:**
- Connector status is `RUNNING` (`GET /connectors/source-shop/status`).
- Schema Registry lists `src.shop.users-key/-value`, `src.shop.orders-*`, `src.shop.order_details-*`.
- `kafka-avro-console-consumer` on `src.shop.orders` shows snapshot events (`op = r`) followed by
  live `c` / `u` / `d` events.
- `pg_replication_slots` on `source-db` shows `dbz_slot` as active.

## Phase 4 — Bronze

**Goal:** the change events land as append-only Iceberg tables, queryable from Trino.

- [ ] Build the Iceberg Kafka Connect sink zip from the Iceberg source (multi-stage build in the
  Connect Dockerfile — it is not published as an artifact). Pick an Iceberg version whose REST
  client supports the OAuth2 `scope` property.
- [ ] Register the sink connector (design §5): `InsertField` SMT for `_topic` / `_partition` /
  `_offset` / `_ingested_at`, static routing of the three topics, auto-create + schema
  evolution, `day(_ingested_at)` partitioning, `format-version=2`, Polaris + S3 (RustFS) config.

**Done when:**
- `SHOW TABLES FROM lakehouse.bronze` in Trino → `users_cdc`, `orders_cdc`, `order_details_cdc`.
- `SELECT op, after.order_id, source.lsn FROM lakehouse.bronze.orders_cdc ORDER BY source.lsn DESC LIMIT 20`
  returns recent events, and new rows appear about every minute (commit interval).
- `SELECT * FROM lakehouse.bronze."orders_cdc$snapshots"` shows one `append` snapshot per commit.
- Spark (Iceberg 1.5.2) can read a bronze table via `helper.get_spark`.

## Phase 5 — Silver

**Goal:** a history-keeping (SCD Type 2) copy of the source in Postgres `data-platform`.

- [ ] DDL in `data-platform`: schemas `silver` and `staging`, tables `silver.users` /
  `silver.orders` / `silver.order_details` (design §6.3), partial unique index
  `(<key>) WHERE is_current`, table `silver.etl_checkpoint`.
- [ ] `spark/jobs/job_silver.py`: incremental read from the checkpoint snapshot, events →
  versions (last event per key per transaction, `lead` for `valid_to`), write
  `staging.<table>` + `staging.batch`. Transform logic in pure functions with pytest tests.
- [ ] Postgres credentials for the Spark services (JDBC to `data-platform`); Airflow already has
  `postgres_conn` pointing at `data-platform`, so no new connection is needed.
- [ ] Task `silver_apply`: in one transaction, add new columns, run the two SCD2 statements
  (design §6.4), advance the checkpoint.
- [ ] DAG `platform_pipeline` (`schedule=None`): `silver_spark` → `silver_apply`.

**Done when:**
- Triggering the DAG fills `silver.*`; one order shows several versions with consistent
  `valid_from` / `valid_to` and exactly one `is_current`.
- Triggering it again with no new events changes nothing (idempotent).
- A deleted order ends with an `is_deleted = true` current version.
- Spark transform tests pass in the `spark-master` container.

## Phase 6 — Gold

**Goal:** analytical models in ClickHouse and a dashboard in Superset.

- [ ] ClickHouse service (new module), named collection pointing at `data-platform`.
- [ ] Add dbt-clickhouse to the Airflow image (check it resolves against the
  Airflow 2.9.2 constraints); create `dbt/profiles.yml` (removed with dbt-spark) and fill
  `dbt/dbt_project.yml`.
- [ ] Models (design §8): ephemeral staging over `postgresql()`; `dim_users`, `dim_date`,
  `fct_orders`, `fct_order_lines` (`append` + `ReplacingMergeTree(_silver_updated_at, is_deleted)`);
  marts for revenue per day and per city; `get_watermark` macro; dbt tests.
- [ ] Add `dbt run` + `dbt test` to `platform_pipeline` after `silver_apply`.
- [ ] Superset service (image + `clickhouse-connect`), ClickHouse database connection, one
  dashboard.

**Done when:**
- `dbt run` and `dbt test` pass from the DAG.
- `SELECT count() FROM gold.fct_orders FINAL` matches the current, non-deleted orders in silver.
- The Superset dashboard shows revenue per day and per city.

## Phase 7 — Search + API

**Goal:** a web page that searches orders and shows their history and statistics.

- [ ] Task `sync_es` in `platform_pipeline` (parallel with dbt): build one document per order,
  rebuild orders whose row, lines or user changed, delete orders that became `is_deleted`;
  `elasticsearch` client 7.x.
- [ ] FastAPI service: `GET /search?q=` (ES), `GET /orders/{id}` and `/orders/{id}/history`
  (silver), `GET /stats/...` (gold).
- [ ] A single static HTML page served by the API.

**Done when:**
- Searching a user's name or a product name returns their orders.
- An order's history page lists its SCD2 versions.
- Numbers on the page match the Superset dashboard.

## Phase 8 — Maintenance + test scenarios

**Goal:** prove the platform behaves correctly under failure and change.

- [ ] DAG `platform_maintenance` (`schedule=None`): Iceberg compaction / expire snapshots (never
  past the silver checkpoint) / remove orphan files; source-vs-silver row-count check; replication
  slot lag.
- [ ] Run every scenario in design §10 and record what happened in the README.

**Done when:** every scenario in design §10 has a recorded result.

---

## Throughout

- Pin every new image and library to an explicit version.
- Every new service gets `logging: *default-logging` and waits (directly or through its
  dependencies) for `alloy`, otherwise its logs are missing from Grafana.
- At the end of each phase: update the **Status** column, tick the phase's items with what was
  actually built and verified, add a **Lessons learned** list, and keep `CLAUDE.md` current.
- Use Grafana (3000) → "Container logs" to debug any service; Spark job logs are under
  `service=spark-job`.
