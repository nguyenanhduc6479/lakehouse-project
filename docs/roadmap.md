# Roadmap

The build order for the data platform described in [dataplatform-design.md](dataplatform-design.md).
Each phase ends with something you can see working; don't start a phase before the previous one's
**Done when** checks pass. A finished phase records what was built and its **Lessons learned**. Vietnamese version: [roadmap.vi.md](roadmap.vi.md).

| # | Phase | Status |
|---|---|---|
| 1 | Clean slate | Done |
| 2 | Source | Done |
| 3 | CDC | Done |
| 4 | Bronze | Done |
| 5 | Silver | Done |
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
- `SELECT * FROM pg_publication_tables WHERE pubname = 'dbz_publication'` lists the three tables
  (plus `public.debezium_heartbeat`, added in phase 3).
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

## Phase 3 — CDC *(done)*

**Goal:** every change in `source-db` arrives in Kafka as an Avro event.

- [x] Kafka Connect image `custom-connect:7.6.0` (`connect/Dockerfile`) on
  `confluentinc/cp-kafka-connect:7.6.0` + the Debezium Postgres plugin **2.7.4.Final** (Maven
  Central tarball, SHA-1 checked). The base image runs Java 11, which rules out Debezium 3.x
  (Java 17); 2.7 runs on Kafka Connect 3.x.
- [x] New module `compose/connect.yml`: service `kafka-connect` (distributed mode, one worker, REST
  8083 internal), internal topics `_connect-configs` / `_connect-offsets` / `_connect-status` with
  replication factor 1, Avro as the worker default converters, `config.providers=env`
  (`EnvVarConfigProvider`) so connector configs read `${env:SOURCE_POSTGRES_DB}` and
  `${env:SOURCE_POSTGRES_DB_DEBEZIUM_PASSWORD}` from the container's environment.
- [x] One-shot `connect-setup` job (`connect/scripts/init.sh`): `PUT`s every
  `connect/connectors/<name>.json` (now `source-shop.json`, design §4.2) to
  `/connectors/<name>/config` — creates or updates, so re-running is safe — checks every HTTP code
  and fails unless the connector and its task reach `RUNNING`.
- [x] Kafka UI (`kafbat/kafka-ui:v1.5.0`, http://localhost:8082, read-only) to see topics,
  Avro-decoded events, schemas and connector state in the browser; `connect/scripts/watch_events.py`
  prints the live event stream one line per change in the terminal.
- [x] Verified 2026-11-03: connector + task `RUNNING`; 6 subjects in Schema Registry
  (compatibility `BACKWARD`); the snapshot (`op = r`) matched the source exactly (48 / 174 / 521
  rows); live `c` / `u` / `d` events with the generator on; replaying every event per key
  reproduced all three source tables exactly (keys and `updated_at`); `total_amount` is an Avro
  `decimal(18,2)`; timestamps are ISO-8601 UTC strings; a cascaded delete gave 4 `order_details`
  deletes + 1 `orders` delete with the same `txId`; stopping Connect left `dbz_slot` inactive with
  growing lag, and the restart resumed from the stored LSN (snapshot skipped, nothing lost);
  re-running `connect-setup` reports "updated"; Connect logs reach Grafana.
- [x] Review fixes (2026-11-03): `init.sh` no longer dies silently while a connector has no
  `RUNNING` state yet (`grep` exit 1 under `pipefail`); heartbeat with `heartbeat.action.query` on
  `public.debezium_heartbeat` (added to the publication by `source/schema.sql`) — idle slot lag
  went from stuck at ~2.7 MB to under 1 KB; `watch_events.py` decodes decimals by column name.

**Done when:**
- Connector status is `RUNNING` (`GET /connectors/source-shop/status`).
- Schema Registry lists `src.shop.users-key/-value`, `src.shop.orders-*`, `src.shop.order_details-*`.
- `kafka-avro-console-consumer` on `src.shop.orders` shows snapshot events (`op = r`) followed by
  live `c` / `u` / `d` events.
- `pg_replication_slots` on `source-db` shows `dbz_slot` as active.

**Lessons learned:**
- Check the base image's runtime before choosing a plugin version: `java -version` in the image
  decided Debezium 2.7 over 3.x.
- Keep secrets as `${env:...}` in the connector JSON: Connect resolves them at runtime, so the
  password is never stored in the `_connect-configs` topic or returned by the REST API.
- `PUT /connectors/<name>/config` is the idempotent way to register a connector (`POST` fails on
  the second run).
- A one-shot job that reuses a service image inherits its `HEALTHCHECK` — disable it.
- Kafka message counts are events, not rows. The convincing CDC check is to replay the events into
  per-key state and compare it with the source.
- Events reflect how the application writes: the generator inserts an order then updates its total
  in the same transaction, so a new order is `c` + `u` with one `txId`; a cascaded delete spans two
  topics with one `txId`. Silver must collapse events per key per transaction.
- The offset (last LSN) lives in Kafka (`_connect-offsets`), the slot keeps the WAL: a restart
  resumes exactly ("previous offset ... completed snapshot" → snapshot skipped). While Connect is
  down the slot is inactive and WAL piles up; even while active, `confirmed_flush_lsn` trails by up
  to one offset flush, so slot lag is small but not zero. (Since phase 4 the worker flushes every
  10 s instead of 60 s — `CONNECT_OFFSET_FLUSH_INTERVAL_MS=10000` — so idle slot lag stays under
  ~0.5 KB and Debezium re-sends fewer duplicates after a crash.)
- `timeout`/Ctrl+C on `docker exec` kills only the host-side client: the process inside the
  container keeps running (two forgotten console consumers showed up as live groups in Kafka UI).
  For long-running clients use `docker run --rm`, which forwards signals.
- `set -euo pipefail` + `grep | wc -l`: `grep` exits 1 when nothing matches, so the whole script
  exits with no message. Test the "not ready yet" path, not only the happy one.
- A Debezium heartbeat alone does not move the slot: pgoutput sends nothing for transactions on
  unpublished tables, so the offset LSN stays put. It takes `heartbeat.action.query` writing to a
  published table. Measure (slot lag before/after) instead of trusting the config name.
- Don't guess types from content: raw decimal bytes can be printable (167.06 = `"AB"`).
- Kafka UI defaults to the `String` serde (Avro shows as raw bytes) unless a `SchemaRegistry` serde
  is bound to the topic pattern.
- `kafka-avro-console-consumer` prints its logs to stdout (filter lines starting with `{`) and
  shows Avro decimals as raw bytes — decode them with the schema's `scale`.

## Phase 4 — Bronze *(done)*

**Goal:** the change events land as append-only Iceberg tables, queryable from Trino.

Steps (each records its result when done, so the phase can be followed step by step):

| Step | What | Status |
|---|---|---|
| 4.1 | Choose the Iceberg version for the sink | Done 2026-11-03 |
| 4.2 | Build the Iceberg sink into `custom-connect` (multi-stage Dockerfile) | Done 2026-11-03 |
| 4.3 | Pass Polaris + S3 credentials to `kafka-connect`; write `connect/connectors/iceberg-bronze.json` (design §5) | Done 2026-11-03 |
| 4.4 | Register through `connect-setup`, check the three bronze tables in Trino | Done 2026-11-03 |
| 4.5 | Read bronze from Spark (Iceberg 1.5.2) via `helper.get_spark` | Done 2026-11-03 |
| 4.6 | Docs, roadmap status, lessons learned | Done 2026-11-03 |

### 4.1 — Iceberg version: **1.10.2** *(done)*

Constraints: the `kafka-connect` worker runs **Java 11** (`cp-kafka-connect:7.6.0`, Zulu 11.0.21);
the REST client must send the OAuth2 **`scope`** Polaris requires (`PRINCIPAL_ROLE:ALL`); tables
must stay readable by Spark's Iceberg **1.5.2**.

Findings (checked 2026-11-03 against Maven Central and the `apache-iceberg-1.10.2` tag):

- **Java level** — read from the class files of each published jar:

  | Iceberg | `iceberg-core` / `iceberg-kafka-connect` bytecode |
  |---|---|
  | 1.6.1 | Java 8 |
  | 1.7.2 – **1.10.2** | **Java 11** |
  | 1.11.0, 1.12.0 (latest) | Java 17 — would fail on the worker (`UnsupportedClassVersionError`) |

  So **1.10.2 is the newest version that runs on Java 11**.
- **OAuth2 scope** — `org.apache.iceberg.rest.auth.OAuth2Properties` in 1.10.2 has `scope`
  (next to `credential`, `oauth2-server-uri`); the sink passes every `iceberg.catalog.*` property
  to the catalog, so `iceberg.catalog.scope=PRINCIPAL_ROLE:ALL` reaches the REST client.
- **Not published ready-to-use** — `iceberg-kafka-connect` (the connector jar) is on Maven Central,
  but `iceberg-kafka-connect-runtime` (the plugin zip with all dependencies: Parquet, AWS SDK,
  REST catalog…) is not. It must be built from source: `./gradlew -x test -x integrationTest
  :iceberg-kafka-connect:iceberg-kafka-connect-runtime:distZip` → zip under
  `kafka-connect/kafka-connect-runtime/build/distributions/` (two variants; the one without the
  Hive metastore client is enough for a REST catalog).
- **Source** — `https://archive.apache.org/dist/iceberg/apache-iceberg-1.10.2/apache-iceberg-1.10.2.tar.gz`,
  SHA-512 `7eba5c85…65b9bd` (full value in the `.sha512` file next to it; the Dockerfile will
  verify it). The release has no `gradle-wrapper.jar`; Iceberg's `gradlew` downloads it (Gradle
  8.14.3).
- **Build JDK** — the build accepts JDK 11, 17 or 21 and always compiles with `options.release = 11`,
  so building on a JDK 17 image still yields Java 11 bytecode.
- **Format** — tables are auto-created with `format-version=2` (design §5), which Spark's Iceberg
  1.5.2 reads; 1.10 only writes v3 features (deletion vectors, row lineage) on v3 tables.

Risks left for the next steps:

- **Kafka API level** — 1.10.2 is compiled against Kafka **3.9.1**; the worker provides Kafka
  **3.6** (`connect-api`/`kafka-clients` are compile-only, not bundled). Expected to work (the
  Connect API is backward compatible), but only a real run proves it → 4.4.
- **Transactional commits** — the sink commits Kafka offsets and Iceberg snapshots with a
  transactional producer; the broker's transaction log is already at RF 1 (phase 1), otherwise
  this would fail on a single broker → 4.4.
- **Polaris + RustFS from the sink** — S3FileIO with path-style access and the RustFS endpoint, no
  credential vending; same settings Trino and Spark already use → 4.4.

### 4.2 — Build the sink into `custom-connect` *(done)*

`connect/Dockerfile` is now multi-stage:

1. **`iceberg-build`** (`eclipse-temurin:17.0.20_8-jdk-noble`): downloads the 1.10.2 source
   release, checks its SHA-512, runs `./gradlew --no-daemon -x test -x integrationTest
   :iceberg-kafka-connect:iceberg-kafka-connect-runtime:distZip` and unpacks the zip (with `jar xf`
   — the JDK image has no `unzip`). Gradle's downloads sit in a BuildKit cache mount
   (`/root/.gradle`), not in the image.
2. **Connect worker** (`cp-kafka-connect:7.6.0`): Debezium as before, plus
   `COPY --from=iceberg-build` of the plugin directory into
   `/usr/share/confluent-hub-components/iceberg-kafka-connect-runtime-1.10.2/` (owned by
   `appuser`). Each plugin directory gets its own class loader, so Iceberg's and Debezium's jars
   don't clash.

Results (2026-11-03):

- First build **~6.5 min** (Gradle ~5.5 min: it downloads its dependencies and compiles the needed
  Iceberg modules). Later builds reuse the Docker layer cache — the stage only reruns when the
  version/checksum args change. Image `custom-connect:7.6.0`: 3.26 GB (the Confluent base image is
  most of it).
- Plugin: 257 jars, 181 MB. **No `kafka-clients` / `connect-api` bundled** → the sink runs on the
  worker's Kafka 3.6 libraries (the API-level risk from 4.1 is still open until 4.4).
- **Bytecode scan of all 257 jars** (highest class-file version per jar, ignoring
  `META-INF/versions`): Java 3–8: 235 jars, Java 11: 15 jars, **above Java 11: none** — the
  dependencies (AWS SDK 2.33, Parquet 1.16…) are fine on the worker's Java 11 too.
- Worker restarted with the new image: `GET /connector-plugins` lists
  `org.apache.iceberg.connect.IcebergSinkConnector 1.10.2 sink` next to Debezium; the log shows the
  plugin loaded (plus its transforms `DebeziumTransform`, `KafkaMetadataTransform`…); `source-shop`
  is still `RUNNING` (offsets kept, no new snapshot).

### 4.3 — Sink config and credentials *(done)*

- **`connect/connectors/iceberg-bronze.json`** (connector `iceberg-bronze`): design §5 as is, plus
  `tasks.max=1` and an explicit control topic `control-iceberg` (the sink's coordination topic;
  auto-created on first start). Every property name was checked against the 1.10.2 reference
  (`docs/docs/kafka-connect.md` at tag `apache-iceberg-1.10.2`).
- **Secrets stay `${env:...}`**: `compose/connect.yml` now passes `POLARIS_SECRET`, `S3_ENDPOINT`,
  `S3_ACCESS_KEY`, `S3_SECRET_KEY`, `S3_REGION` from `.env` into `kafka-connect`; the JSON only
  names them (`"iceberg.catalog.credential": "root:${env:POLARIS_SECRET}"` — a provider reference
  works inside a longer string).
- **Startup order**: `connect-setup` now also waits for `polaris-setup` (catalog `lakehouse` +
  namespace `bronze`, which itself waits for the RustFS bucket).

Results (2026-11-03), without registering the connector yet:

- Inside `kafka-connect`: the five variables are set; a Polaris OAuth2 token request with
  `scope=PRINCIPAL_ROLE:ALL` → HTTP 200 with an `access_token`; `GET ${S3_ENDPOINT}/health` → 200.
- `PUT /connector-plugins/IcebergSinkConnector/config/validate` with the JSON →
  **`error_count = 0`** over 48 checked settings (connector, converters and the `InsertField`
  transform).
- `GET /connectors` still lists only `source-shop`. Note: the next run of `connect-setup` registers
  every `connect/connectors/*.json`, i.e. it starts the sink — that is step 4.4.

### 4.4 — Register and check in Trino *(done)*

`docker compose up -d --force-recreate connect-setup` → `Connector iceberg-bronze created` +
`RUNNING`. Results (2026-11-03):

- **Tables**: the sink auto-created `bronze.users_cdc` / `orders_cdc` / `order_details_cdc` in
  Polaris at its first records (`SHOW TABLES FROM lakehouse.bronze`): `before`/`after`/`source`
  as nested rows, `op`, `ts_ms`, the four `_` metadata columns, Parquet, `format_version = 2`,
  `partitioning = ARRAY['day(_ingested_at)']`, location `s3://data-platform/bronze/<table>`.
- **Catch-up**: the group `connect-iceberg-bronze` had no offset, so the sink read the topics from
  the start (snapshot `r` events included). First data commit ~95 s after start (the first two
  commits hit the 30 s timeout while the worker was still loading tables, and committed nothing).
- **Steady state** (generator on): a commit every ~60 s, each done in under 2 s and touching all
  three tables (`valid-through` = commit time); one `append` snapshot per commit; one ~23 KB
  Parquet file per table per commit. When the topics are idle, commits time out after 30 s and
  commit nothing — harmless.
- **Exactly-once, including a restart**: `kafka-connect` was restarted mid-stream; afterwards, per
  table, bronze rows = Kafka messages = distinct `_offset` values, offsets contiguous from 0 (users
  139, orders 837, order_details 1359), consumer lag 0.
- **Bronze = source**: rebuilding the current state from bronze (last event per key by
  `source.lsn`, deletes dropped) gives exactly the source tables — same keys and `updated_at` for
  89 users, 401 orders, 1237 order lines.
- The three open risks from 4.1 are closed: Kafka 3.9-built sink on the 3.6 worker, transactional
  commits on one broker, Polaris OAuth2 scope + RustFS from the sink — all work.
- Two startup warnings removed by setting `iceberg.catalog.rest.auth.type=oauth2` and
  `iceberg.catalog.oauth2-server-uri` explicitly (Iceberg 1.10 inferred both).
- Types: money is `decimal(38,2)` in bronze (the sink keeps the scale, not the Avro precision 18);
  timestamps inside `before`/`after` stay ISO-8601 strings; `_ingested_at` is
  `timestamp(6) with time zone` = the Kafka record time.

### 4.5 — Read from Spark *(done)*

A throwaway script (`get_spark` + `spark.table("lakehouse.bronze.<table>")`, submitted with
`/opt/spark/bin/spark-submit --py-files /opt/spark/jobs/helper.py`) ran on the cluster
(`spark://spark-master:7077`) with Iceberg **1.5.2**: all three tables readable, counts equal to
Trino, `format-version=2`, nested structs and `Decimal` values intact, `op` counts c 249 / d 22 /
r 174 / u 392 for orders. Tables written by Iceberg 1.10.2 at v2 are readable by 1.5.2.

### 4.6 — Docs *(done)*

Design §4.3 (decimal in bronze) and §5 (final sink JSON, commit mechanics, consumer groups,
`_ingested_at` meaning), `docs/services.md` ("Checking bronze"), `CLAUDE.md`, this roadmap.
Cleanup: the Iceberg namespaces `silver` and `gold` (created by `polaris-setup` since the old
all-Iceberg design, always empty) were dropped and `polaris/scripts/init.sh` now creates only
`bronze` — silver lives in Postgres and gold in ClickHouse, so the catalog shows only what is on
Iceberg (`SHOW SCHEMAS FROM lakehouse` → `bronze`, `information_schema`).

**Done when** (whole phase):
- `SHOW TABLES FROM lakehouse.bronze` in Trino → `users_cdc`, `orders_cdc`, `order_details_cdc`.
- `SELECT op, after.order_id, source.lsn FROM lakehouse.bronze.orders_cdc ORDER BY source.lsn DESC LIMIT 20`
  returns recent events, and new rows appear about every minute (commit interval).
- `SELECT * FROM lakehouse.bronze."orders_cdc$snapshots"` shows one `append` snapshot per commit.
- Spark (Iceberg 1.5.2) can read a bronze table via `helper.get_spark`.

### After the phase — review fixes (2026-11-03)

- **Iceberg in Spark upgraded 1.5.2 → 1.10.2** (`spark/Dockerfile`: `iceberg-spark-runtime-3.5_2.12`
  + `iceberg-aws-bundle`), the same version as the sink, before phase 8 has Spark maintain the
  tables the sink writes. Checked on the cluster (Spark 3.5.1, Java 11; Iceberg 1.10.2 is built
  against Spark 3.5.6 but runs on 3.5.1): the three bronze tables read with the same counts; on a
  scratch table `bronze._spark_check` (dropped afterwards) — five inserts, `rewrite_data_files`
  (5 files → 1), `expire_snapshots` (5 data files, 5 manifests removed), `remove_orphan_files`
  dry run — all work. `remove_orphan_files` needs `prefix_listing => true`: by default it lists
  through Hadoop, which has no `s3://` filesystem here (`No FileSystem for scheme "s3"`).
- Documented (design §5): `day(_ingested_at)` cuts days in UTC, so 00:00–07:00 Vietnam time lands
  in the previous day's partition; the sink, Trino and Spark share Polaris' `root` principal (a
  per-engine principal is a possible extension); `connect-setup` waits for `polaris-setup`, so a
  broken catalog also blocks Debezium's registration (accepted; one job per connector would
  decouple them).
- **Adding a source table — runbook tested end to end** (`docs/add-source-table.md`): a scratch
  table `shop.test_products` with 5 existing rows went through schema.sql → both connectors →
  signal snapshot (5 `r` events) → live insert/update/delete → `bronze.test_products_cdc` (8 rows,
  correct before/after), then out again with the removal steps (connectors, bronze table, topic +
  subjects, leftover S3 prefix, publication + table) — no trace left, main pipeline unaffected
  (30 new events reached bronze afterwards). Needed for it: signal table `public.debezium_signal`
  (in the publication) + `signal.data.collection`, and a `Filter` SMT (`dropSignals`) because
  Debezium otherwise publishes the signal table to `src.public.debezium_signal`.
- **Two platform bugs found by that test, fixed:**
  - *OAuth2 tokens*: Trino got `Not authorized` on every catalog call. Polaris' access log showed
    Trino fetched a token at 16:46:40, refreshed it once at 17:41:40, never again, and got HTTP 401
    from 18:41:46; the sink showed the same pattern (16:25:19 → 17:20:19 → nothing). Polaris accepts
    the refresh (token exchange returns a fresh 1 h token with `expires_in`), so the clients stop
    scheduling. Fix: Polaris tokens last 30 days (`max-token-generation=P30D`).
  - *Idle sink*: after a restart the sink's control consumer rejoined its group every minute and
    the worker never answered `START_COMMIT` while the topics were idle, so the 8 test records sat
    uncommitted. Connect calls `put()` (where the sink reads its control topic) only once per
    `offset.flush.interval.ms` when idle — 60 s, longer than the 45 s session. Fix:
    `CONNECT_OFFSET_FLUSH_INTERVAL_MS=10000`; the pending records were committed ~65 s later
    without new traffic, and idle commits now complete in ~15 s instead of timing out.
- **Review fixes (same day):**
  - *Fixed token signing key*: Polaris' default `rsa-key-pair` broker (no key files) generates a key
    at every start, so a Polaris restart invalidated every token at once — the sink task went
    `FAILED` and Trino said `Not authorized` until restarted by hand (reproduced). Polaris now uses
    `token-broker.type=symmetric-key` with `symmetric-key.secret=${POLARIS_TOKEN_SIGNING_KEY}`
    (new `.env` variable; property names read from the 1.5.0 jars). Checked: tokens are HS256;
    after a Polaris restart Trino kept answering and the sink committed 20 new events with 0 errors.
  - *Signal id length*: `debezium_signal.id` is `varchar(42)` and the runbook's id
    `snapshot-<table>-<timestamp>` overflowed for table names over 18 characters. The id is now
    `snapshot-<timestamp to µs>` (29 characters; the table is named in `data`); tested with a
    33-character table name.
  - The phase-3 lesson on slot lag now reflects the 10 s offset flush (idle lag measured
    248–496 bytes).
- Inputs for phase 5 are listed under Phase 5 below.

**Lessons learned:**
- Read the bytecode, not the release notes: scanning class-file versions of the published jars
  (and later of all 257 bundled jars) answered "does it run on Java 11?" with certainty.
- A plugin's dependencies matter as much as the plugin: the runtime zip bundles Parquet, AWS SDK,
  Hadoop… but deliberately not Kafka — it runs on the worker's Kafka libraries.
- `validate` before `create`: Connect's `PUT /connector-plugins/<class>/config/validate` catches
  config mistakes without starting anything; connectivity (Polaris token, S3) can be checked from
  inside the worker container with the same env variables.
- Wait on the right condition: twice a check waited on the wrong thing (old commit lines in the
  log, a snapshot count a table never reaches because a commit skipped it). Wait on what you
  actually need (bronze count = Kafka end offset), not on a proxy.
- Commit timeouts in the log are not errors: an idle or just-started sink times out and commits
  nothing; data is never lost because Kafka offsets only move with a successful Iceberg commit.
- Exactly-once into Iceberg holds through a restart (no gap, no duplicate `_offset`); duplicates
  can only come from upstream (Debezium at-least-once) — silver still dedups.
- Streaming into Iceberg makes many small files (one per table per commit): fine for a demo,
  compaction is phase 8's job.
- Python 3.8 in the Spark image: an f-string expression cannot contain a backslash.
- Keep every engine that writes or maintains a table on the same Iceberg version as the writer;
  "it reads fine" says nothing about compaction or snapshot expiry under a concurrent writer.
- A real end-to-end run finds what short checks don't: both bugs above only show up after ~2 h
  (token) or after a restart followed by an idle period (sink). Run things for a while, and read
  the server's access log — it pinpointed the token problem to the minute.
- Spark procedure arguments must be literals (`TIMESTAMP '...'`, not `current_timestamp()`).

## Phase 5 — Silver *(done)*

**Goal:** a history-keeping (SCD Type 2) copy of the source in Postgres `data-platform`.

- [x] DDL `postgres/schema.sql`, applied idempotently by `postgres-setup` on every `up` (it fails
  the job on any SQL error): schema `config` with `etl_checkpoint` (one row per
  `(pipeline, table_name)`: `source_table`, `key_columns`, `last_snapshot_id`, `last_watermark`,
  `is_active`); schema `silver` with `users` / `orders` / `order_details` (design §6.3) —
  `PRIMARY KEY (<key>, _lsn)`, partial unique index `(<key>) WHERE _is_current`,
  `CHECK ((_valid_to IS NULL) = _is_current)`, source strings as `text`, no FKs/CHECKs on source
  columns, `_tx_id` (source transaction, added by the review fix below); schema `staging` with
  `staging.batch`. SCD2/system columns start with `_`.
- [x] `spark/jobs/job_silver.py`, **generic**: the tables and keys come from
  `config.etl_checkpoint`. Per table: current snapshot from `<table>.refs` (`main`); no new
  snapshot = no-op; otherwise an Iceberg incremental read `(last_snapshot_id, current]` (full read
  when NULL); `events_to_versions` (pure function, 6 Spark tests) → `staging.<table>` (JDBC
  overwrite, recreated each run); `staging.batch` (start, end, row count) is written **last**.
  `helper.postgres_jdbc_options()`; `POSTGRES_*` added to `spark-master` / `spark-worker`.
- [x] `airflow/dags/lib/silver_apply.py` (task `silver_apply`): per table, one transaction —
  refuse a batch whose start ≠ checkpoint, skip one already applied, `ADD COLUMN` for new staging
  columns, then supersede (a version whose transaction continues in staging) + close + insert
  (design §6.4) with every value cast to the silver type, advance the checkpoint. Tables are
  applied independently; the task fails at the end if any failed. Tests: SQL builder on the host
  (3), the apply itself on a scratch Postgres DB (9, marker `postgres`). Shared DAG code lives in
  `airflow/dags/lib/` (moved from `airflow/plugins/`; `airflow/dags/.airflowignore` keeps the DAG
  processor out of it).
- [x] DAG `airflow/dags/platform_pipeline.py`: `silver_spark` → `silver_apply` (→ `silver_verify`, added after the phase), `schedule=None`,
  `max_active_runs=1`.
- [x] Verified 2026-11-04 (DAG runs on the live stack):
  - first run (full read): 156 / 640 / 1459 versions; rows = distinct (key, txId) in bronze,
    current versions = distinct keys, `_is_current AND NOT _is_deleted` = source counts
    (93 / 426 / 1309), and every current row equals the source column by column;
  - no-op runs: "batch already applied", nothing changed;
  - after 90 s of generator: incremental run staged 41 / 129 / 297 rows, closed 23 / 28 / 20
    versions; all checks above still hold (103 / 506 / 1566, identical to the source); no key
    with ≠ 1 current version, no gap in any `_valid_from` / `_valid_to` chain;
  - checkpoint of `orders` rewound one run: 129 events re-read, 0 closed, 0 inserted (idempotent);
  - a stale `staging.batch` (start ≠ checkpoint): the table fails, checkpoint unchanged, other
    tables applied;
  - e.g. order 45: pending → paid → shipped → completed → refunded; a deleted order ends with an
    `_is_deleted` current version carrying its last values.

**Done when:**
- Triggering the DAG fills `silver.*`; one order shows several versions with consistent
  `_valid_from` / `_valid_to` and exactly one `_is_current`.
- Triggering it again with no new events changes nothing (idempotent).
- A deleted order ends with an `_is_deleted = true` current version.
- Spark transform tests pass in the `spark-master` container.

**Inputs from phase 4** (all handled):
- Order versions by `source.lsn`, never by `_ingested_at` (Kafka record time, not commit order).
- Money is `decimal(38,2)` in bronze: cast explicitly to `numeric(18,2)` for silver — done in the
  apply SQL (`st.col::numeric(18,2)`).
- Timestamps in `before` / `after` are ISO-8601 strings (UTC, `...Z`): parsed to `timestamptz` by
  the same cast.
- An idle sink commits nothing, so often there is no new snapshot since the checkpoint: the job
  treats that as a no-op (`row_count = 0`, checkpoint unchanged).
- Bronze can contain duplicate events (Debezium is at-least-once): deduplicated on (key, lsn).

**Lessons learned:**
- **Spark JDBC can only append or overwrite** — no UPDATE/MERGE and no transaction spanning
  partitions — so SCD2 needs a staging table and a set-based SQL apply in Postgres. That also
  gives the one place where silver and the checkpoint move together.
- **Write the "what staging holds" record last, and check it on apply.** With `staging.batch`
  written after the data and the apply refusing a batch whose start ≠ checkpoint, every failure
  mode makes silver lag (re-read later, skipped by `_lsn`), never skip data.
- **Rewinding the checkpoint is the real idempotency test**: it replays already-applied events
  through the whole path. Comparing rows column by column with the source caught nothing here,
  but it is the check that would catch a bad cast.
- **Cast in SQL to the target's types** instead of mirroring types in Spark: staging keeps what
  Spark produces (text timestamps with an explicit offset, `numeric(38,2)`), Postgres casts to
  silver's types, and a new source column arrives with staging's type through `ADD COLUMN`.
  Writing Spark timestamps over JDBC would depend on the session/JVM time zone.
- **Iceberg snapshot IDs are random**: the current one comes from `<table>.refs` (`main`), and
  incremental reads follow the parent chain — never compare IDs.
- Use `source.ts_us`, not `ts_ms`: two transactions on one key in the same millisecond would
  otherwise give a zero-length version.
- Snapshot events (`op = r`) all share one LSN and txId, and their `_valid_from` is the snapshot
  time, not when the row was really created — history before the initial snapshot is unknown.
- A new DAG file is not triggerable until the scheduler has serialized it (`DagNotFound`): wait
  for the next DAG-folder scan (up to 5 min) or run `airflow dags reserialize`.

**After the phase — review fixes (2026-11-04):**
- **Split transaction → zero-length version (bug, fixed).** Grouping by transaction only works
  inside one batch, but a sink commit can cut a transaction's events in two (bronze already had 2
  key/transaction pairs in two commits). A silver run between the two commits inserted the first
  part as current; the next run closed it with `_valid_to = _valid_from` (allowed by the `>=`
  CHECK, invisible to the gap check). Fix: silver keeps `_tx_id`; the apply first **deletes** a
  current version whose transaction continues in staging (statement 0, design §6.4), so the chain
  stays intact. Silver was rebuilt from bronze with the new column (same counts, identical to the
  source); new check `count(*) WHERE _valid_to = _valid_from = 0` (`docs/services.md`).
- **SCD2 apply tested on a real database:** `tests/test_silver_apply_db.py` (marker `postgres`,
  9 tests on a scratch DB built from `postgres/schema.sql`): first batch, casts, same batch again,
  rewound checkpoint, overlapping batch, delete, split transaction, stale batch, new column. The
  split-transaction test fails against the old apply.
- 5 DAG runs while the generator was writing + 1 after draining: all checks hold, 0 zero-length
  versions, identical to the source (140 / 718 / 2198).
- New-column types (accepted limit, design §10): a new `timestamptz` source column becomes `text`
  in silver, `numeric(18,2)` becomes `numeric(38,2)`; mapping types from the Avro schema is the
  fix if it matters. Added to the phase 8 scenarios with: silver after bronze compaction, and
  re-snapshot while the generator runs (the `_valid_to >= _valid_from` CHECK may fail).
- **DAG param `table`** (2026-11-04): `all` (default) or one table (`users` / `silver.users`).
  `submit_and_wait(app_args=)` passes `--table` to `job_silver.py` through the REST `appArgs`;
  `silver_apply` filters the same way. A malformed name is refused at trigger time (`Param`
  pattern); an unknown one is checked against `config.etl_checkpoint` before submitting, so it
  fails in ~1 s with the list of active tables (the Spark job's own check only showed up as
  `driverState FAILED` in Airflow, ~1 min later). Verified: `users` run touched only users;
  the following `all` run did the other two; silver still identical to the source.
- **`staging.batch` keeps one row per active table** (2026-11-04): the job reads the table,
  replaces the rows of the tables it processed (`merge_batches`, 2 host tests), keeps the others
  with their `created_at`, and writes it back — before, a `--table` run truncated the other
  tables' rows, so a pending batch of another table waited for one more Spark run. Verified:
  `users` run → `orders` row untouched; `all` → 3 rows; `users` again → only its row changed.
  Two traps on the way: `ruff check --fix` (target py311) rewrote `timezone.utc` to
  `datetime.UTC`, an ImportError on the Spark image's Python 3.8 — fixed for good with
  `per-file-target-version` `py38` for `spark/jobs/**`; and `.options(**jdbc, url=...)` raises
  "multiple values for keyword argument" — the three failed runs only showed that the safety
  holds (staging rewritten, batch not, checkpoint unchanged, next run re-read everything).
- **Task `silver_verify`** (2026-11-04), last in `platform_pipeline`, same `table` param: the
  silver checks that were manual queries now run every time and fail the DAG — per table
  `keys_without_one_current`, `zero_length_versions`, `broken_chains` = 0, and silver rows /
  current rows = distinct (key, txId) / distinct keys of bronze read **at the checkpoint
  snapshot** (Trino `FOR VERSION AS OF`), which makes the comparison exact while the generator
  keeps writing (verified: run with the generator on, all OK, 3023 / 1395 / 358 rows = bronze).
  A `_valid_to` shifted by 1 s on one real version made it fail with `broken_chains` (reverted).
  Tests: 3 host (SQL), 4 on the scratch DB (normal apply, broken histories, bronze mismatch,
  before the first apply). Only "silver = source" stays manual: it holds only once drained.
- Lesson: **a grouping rule applied per batch is only correct if a batch boundary can't cut the
  group.** Here batches are cut by the sink's commits, which know nothing about source
  transactions — so the rule had to be finished across batches in the apply. Test the boundary,
  not just the steady state: the live runs never hit it, the DB test does on demand.

## Phase 6 — Gold

**Goal:** analytical models in ClickHouse and a dashboard in Superset.

- [ ] ClickHouse service (new module), named collection pointing at `data-platform`.
- [ ] Add dbt-clickhouse to the Airflow image (check it resolves against the
  Airflow 2.9.2 constraints); create `dbt/profiles.yml` (removed with dbt-spark) and fill
  `dbt/dbt_project.yml`.
- [ ] Models (design §8): ephemeral staging over `postgresql()`; `dim_users`, `dim_date`,
  `fct_orders`, `fct_order_lines` (`append` + `ReplacingMergeTree(_silver_updated_at, _is_deleted)`);
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
  rebuild orders whose row, lines or user changed, delete orders that became `_is_deleted`;
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
- [ ] Run every scenario in design §10 and record what happened in the README — including the
  ones added by the phase 5 review: new-column types in silver, silver after bronze compaction,
  re-snapshot while the generator runs.

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
