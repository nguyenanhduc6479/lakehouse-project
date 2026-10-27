# Local Lakehouse — System Overview

> Status: infrastructure is up and was tested end-to-end on **2026-10-27** (WSL2 Ubuntu + Docker
> Desktop). The medallion pipeline is written but **has not run end-to-end** because the
> `vnstock` data source is quarantined on PyPI (see [section 11](#11-status--known-limitations)).

## 1. What this system is

A **local lakehouse** built entirely with Docker Compose. Object storage, the table catalog,
processing, query, orchestration and metadata management all run on **a single Docker network**
(`lakehouse`), and services call each other by container name.

The demo use case is a **medallion pipeline** for daily Vietnamese stock prices (OHLCV):

```
vnstock (daily OHLCV) ──ingest──▶ MinIO  s3://lakehouse/bronze/_landing/…/*.parquet
   ──job_bronze──▶ lakehouse.bronze.stock_ohlcv          (raw, deduplicated)
   ──job_silver──▶ lakehouse.silver.stock_ohlcv          (cleaned + quality checks)
   ──job_gold────▶ lakehouse.gold.stock_daily_metrics    (returns, MA5/MA20, range)
   ──────────────▶ queried with Trino (catalog `lakehouse`)
```

Each layer is a real **Apache Iceberg table**, registered in the **Apache Polaris** catalog, with
data on **MinIO**. That is why Spark can write and Trino can read the same table.

Deliberately small: 5 symbols (`FPT, VNM, ACB, HPG, MWG`), and each run **overwrites the whole**
table (`createOrReplace`) so the flow is easy to follow; switch to `MERGE INTO` for incremental.

## 2. Architecture

```
                         ┌──────────────────────────┐
                         │   Airflow 2.9.2          │  orchestration: DAG lakehouse_ohlcv
                         │   webserver · scheduler  │  (18:00 Mon–Fri)
                         └─────┬──────────────┬─────┘
              ingest (Python)  │              │  submit jobs via REST :6066
                               ▼              ▼
┌───────────┐  parquet  ┌────────────┐   ┌──────────────────────────────┐
│ vnstock   │──────────▶│   MinIO    │◀──│ Spark 3.5.1 cluster          │
│ (source)  │           │  (S3, data)│   │ master · worker · thrift ·   │
└───────────┘           └─────▲──────┘   │ history                      │
                              │          └──────────────┬───────────────┘
                   read/write │                         │ Iceberg REST + OAuth2
                   data files │                         ▼
                        ┌─────┴──────┐  REST+OAuth2 ┌────────────────────┐   JDBC  ┌──────────┐
                        │ Trino 454  │─────────────▶│ Polaris 1.5.0      │────────▶│ Postgres │
                        │  (query)   │              │ (Iceberg catalog)  │         │    15    │
                        └────────────┘              └────────────────────┘         └────▲─────┘
                                                                                        │
   DataHub v0.12.1 (lineage/metadata) ── Kafka · Zookeeper · Elasticsearch · schema-registry ──┘
```

`docker-compose.yml` only `include`s the 7 modules in `compose/` and declares the shared network:

| Module | Main services |
|---|---|
| `infra.yml` | Postgres 15, Zookeeper, Kafka, Elasticsearch 7.17 |
| `storage.yml` | MinIO |
| `catalog.yml` | Polaris (+ bootstrap, setup) |
| `datahub.yml` | DataHub GMS, frontend, actions, schema-registry |
| `spark.yml` | Spark master, worker, thrift, history |
| `trino.yml` | Trino |
| `airflow.yml` | Airflow init, webserver, scheduler |

## 3. Services (27 containers)

### Long-running services (17)

| Service | Image | Role | Host port |
|---|---|---|---|
| `postgres` | `postgres:15` | Shared DB for Airflow, Polaris, DataHub | 5432 |
| `minio` | `minio/minio` | S3 object storage, holds every data file | 9000 (API), 9001 (console) |
| `polaris` | `apache/polaris:1.5.0` | Iceberg REST catalog, OAuth2 | 8181 |
| `spark-master` | `custom-spark:3.5.1` | Spark master + REST submit | 8090 (UI), 7077 (RPC), 6066 (REST) |
| `spark-worker` | `custom-spark:3.5.1` | Executor/driver (`SPARK_WORKER_CORES`/`_MEMORY`, 4 cores / 3 GB) | 8081 |
| `spark-thrift` | `custom-spark:3.5.1` | Hive Thrift JDBC for dbt | 10000, 4040 (UI) |
| `spark-history` | `custom-spark:3.5.1` | Spark History Server | 18080 |
| `trino` | `trinodb/trino:454` | SQL query engine | 8080 |
| `airflow-webserver` | `custom-airflow:2.9.2` | Airflow UI | 8085 |
| `airflow-scheduler` | `custom-airflow:2.9.2` | Scheduling + task execution (LocalExecutor) | — |
| `zookeeper` | `confluentinc/cp-zookeeper:7.6.0` | Kafka coordination | — |
| `kafka` | `confluentinc/cp-kafka:7.6.0` | Message bus for DataHub | — |
| `elasticsearch` | `elasticsearch:7.17.10` | Search + graph store for DataHub | — |
| `schema-registry` | `confluentinc/cp-schema-registry:7.6.0` | Schema registry required by DataHub | — |
| `datahub-gms` | `linkedin/datahub-gms:v0.12.1` | Metadata API | 8083 |
| `datahub-frontend-react` | `linkedin/datahub-frontend-react:v0.12.1` | DataHub UI | 9002 |
| `datahub-actions` | `acryldata/datahub-actions:v0.0.14` | Actions framework | — |

"—" = reachable only inside the Docker network (verified with `docker ps`). Kafka advertises a
`localhost:9092` listener but **does not publish the port**, so it can't be reached from the host.

### One-shot jobs (10)

These jobs exit with code `0` when done; downstream services wait via
`service_completed_successfully`.

| Job | What it does |
|---|---|
| `postgres-setup` | Creates roles + databases `airflow`, `polaris`, `datahub` (idempotent) |
| `minio-setup` | Creates bucket `lakehouse` with prefixes `bronze/ silver/ gold/` (skipped if present) |
| `polaris-bootstrap` | `polaris-admin-tool bootstrap`: creates schema + realm `POLARIS` in Postgres |
| `polaris-setup` | Creates/updates catalog `lakehouse`, grants privileges, creates namespaces |
| `spark-fix-permissions` | `chown 185` on the event-log volume so Spark (UID 185) can write |
| `airflow-init` | `db migrate`, creates the admin, creates 5 Airflow connections |
| `kafka-setup` | Pre-creates 5 DataHub topics |
| `datahub-elasticsearch-setup` | Creates Elasticsearch indices |
| `datahub-postgres-setup` | Creates the DataHub schema in Postgres |
| `datahub-upgrade` | DataHub `SystemUpdate` |

## 4. Startup chain

Most `depends_on` entries use `service_healthy` or `service_completed_successfully`, so services
start in the right order without racing:

```
postgres ─▶ postgres-setup ─┬─▶ polaris-bootstrap ─▶ polaris ─┬─▶ polaris-setup (also needs minio-setup)
                            │                                 └─▶ trino
                            └─▶ airflow-init ─▶ airflow-webserver, airflow-scheduler
minio ─▶ minio-setup
spark-master ─▶ spark-worker, spark-history;  spark-fix-permissions + spark-master ─▶ spark-thrift
zookeeper ─▶ kafka ─┬─▶ kafka-setup ─────────────────┐
                    └─▶ schema-registry ─────────────┤
elasticsearch ─▶ datahub-elasticsearch-setup ────────┼─▶ datahub-upgrade ─▶ datahub-gms ─┬─▶ datahub-frontend-react
postgres-setup ─▶ datahub-postgres-setup ────────────┘                                  └─▶ datahub-actions
```

The Nth `docker compose up -d` still succeeds: every setup job is idempotent (verified).

## 5. Medallion data flow

| Layer | Code | Input → output | Main logic |
|---|---|---|---|
| **Ingest** | `airflow/plugins/ingest_ohlcv.py` | `vnstock` → `s3://lakehouse/bronze/_landing/stock_ohlcv/ingest_date=YYYY-MM-DD/data.parquet` | Pulls OHLCV per symbol (VCI source), adds `symbol`, `source`, `ingested_at`; writes parquet via `boto3`. Failing symbols are skipped; fails if no symbol has data. |
| **Bronze** | `spark/jobs/job_bronze.py` | landing parquet → `lakehouse.bronze.stock_ohlcv` | `time` → `trade_date`; dedup by `(symbol, trade_date)`, keeping the row with the latest `ingested_at`. Read schema declared explicitly. |
| **Silver** | `spark/jobs/job_silver.py` | bronze → `lakehouse.silver.stock_ohlcv` | Quality rules: non-null keys, prices > 0, volume ≥ 0, candle constraints (`high ≥ low`, `high ≥ max(open, close)`, `low ≤ min(open, close)`). Logs kept/dropped rows; fails if empty. |
| **Gold** | `spark/jobs/job_gold.py` | silver → `lakehouse.gold.stock_daily_metrics` | `prev_close`, `daily_return`, `ma5`, `ma20`, `range_pct`. Windows computed **within each symbol** by `trade_date`. Fails if empty. |
| **Query** | Trino | gold table | `SELECT … FROM lakehouse.gold.stock_daily_metrics` |

Common traits:

- **Idempotent.** Ingest overwrites the file per `ingest_date`, so re-running within a day
  leaves no junk. The Spark jobs use `createOrReplace()`.
- **Iceberg tables** use format v2, partitioned by `symbol`, located at
  `s3://lakehouse/<namespace>/<table>` (the catalog's default location).
- **Transform logic is factored into pure functions** (`transform_bronze`, `clean_silver`,
  `build_gold`), so it can be tested with hand-built DataFrames, without MinIO or Polaris.

## 6. Catalog: Apache Polaris

| Aspect | Configuration |
|---|---|
| Endpoint | `http://polaris:8181/api/catalog`, warehouse/prefix `lakehouse` |
| Auth | OAuth2 client-credentials `root` / `POLARIS_SECRET`, **scope `PRINCIPAL_ROLE:ALL` required** |
| Metadata persistence | Postgres via `POLARIS_PERSISTENCE_TYPE=relational-jdbc` (database `polaris`) |
| Catalog storage | S3: `endpoint` = MinIO, `pathStyleAccess=true`, `stsUnavailable=true`, `region=us-east-1`, `allowedLocations=s3://lakehouse/` |
| Privileges | `catalog_admin` is additionally granted `CATALOG_MANAGE_CONTENT` (needed for `DROP TABLE` purge and metrics reporting) |
| Catalog properties | `polaris.config.drop-with-purge.enabled=true` |
| Namespaces | `bronze`, `silver`, `gold`, created via the Iceberg REST API |
| Health | `/q/health` on port 8182, **container-internal only** |

Because `stsUnavailable=true`, Polaris **does not vend temporary credentials** to clients.
Each engine brings its own MinIO keys. Polaris itself reads/writes metadata files using the
`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION` environment variables.

## 7. How engines connect to the catalog

| | Spark (`spark/jobs/common.py`) | Trino (`trino/catalog/lakehouse.properties`) |
|---|---|---|
| Catalog | `SparkCatalog`, `type=rest`, `uri`, `warehouse=lakehouse` | `iceberg.catalog.type=rest`, `rest-catalog.uri`, `warehouse` |
| OAuth2 | `credential=root:<secret>`, `scope=PRINCIPAL_ROLE:ALL` | `oauth2.credential`, `oauth2.scope` (needs Trino ≥ 454) |
| Data files | `S3FileIO` + `s3.endpoint`, `path-style-access`, MinIO keys | `fs.native-s3.enabled`, `s3.endpoint`, `s3.region`, `s3.path-style-access`, keys |
| Landing reads | `fs.s3a.*` (hadoop-aws) to read raw parquet | — |
| Secrets | env `MINIO_*`, `POLARIS_SECRET` | `${ENV:MINIO_*}`, `${ENV:POLARIS_SECRET}` |

The `custom-spark:3.5.1` image (from `apache/spark:3.5.1`) adds the JARs `hadoop-aws 3.3.4`,
`iceberg-spark-runtime-3.5_2.12 1.5.2`, `iceberg-aws-bundle 1.5.2`, `postgresql 42.6.0`,
`acryl-spark-lineage 0.2.18`, `kafka-clients 3.6.0`; and Python `pandas`, `pyarrow`, `fastparquet`.
Python in the container is **3.8.10**. `spark-defaults.conf` only configures the event log and
timezone; the catalog config lives in `common.py`.

## 8. Orchestration: Airflow

- **DAG** `lakehouse_ohlcv` (`airflow/dags/lakehouse_ohlcv_dag.py`): `ingest_landing` →
  `build_bronze` → `build_silver` → `build_gold`. Schedule `0 18 * * 1-5` (18:00 Mon–Fri,
  `Asia/Ho_Chi_Minh` time), `catchup=False`, `retries=1`. New DAGs start **paused** by default.
- **Spark submission** via the **Spark Standalone REST API** `spark-master:6066`
  (`airflow/plugins/spark_rest.py`), deploy mode **cluster**: the driver runs on `spark-worker`,
  so Airflow needs neither Java nor `spark-submit`. The task waits for driver state `FINISHED`
  (30-minute timeout) and fails on `FAILED`, `ERROR` or `KILLED`.
- **Ingest** runs inside the scheduler (`PythonOperator`), reading `MINIO_*` from env.
- **Connections** are created in code by `airflow/scripts/init.sh` (delete, then recreate):
  `spark_rest_conn`, `minio_conn`, `postgres_conn`, `trino_conn`, `datahub_conn`.
- The `custom-airflow:2.9.2` image (from `apache/airflow:2.9.2-python3.11`) installs
  `dbt-core`/`dbt-spark` 1.8.0, the http/amazon/postgres/trino providers,
  `acryl-datahub[airflow]` 0.12.1.5, `pandas 2.1.4`, `pyarrow 14.0.2`, using the **official
  Airflow 2.9.2 constraints**.

## 9. Metadata & lineage: DataHub

DataHub **v0.12.1** (matches `acryl-datahub[airflow]==0.12.1.5`), ported from the MySQL
quickstart onto this repo's infrastructure:

- **Postgres** (database `datahub`), **Kafka** `kafka:29092`, **Elasticsearch** as the graph
  store (no Neo4j). `schema-registry` is added because DataHub requires one.
- `datahub-gms` runs the MAE/MCE/PE consumers embedded. UI on port 9002.
- Spark emits lineage via the `acryl-spark-lineage` JAR, Airflow via `datahub_conn`.
  *Lineage has not yet been verified in the UI.*

## 10. Configuration, secrets and storage

**All secrets and parameters live in `.env`** (gitignored, never pushed), injected into
containers via `environment:`. Variable groups:

| Group | Variables |
|---|---|
| Postgres | `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DB`, `POSTGRES_HOST`, `POSTGRES_DB_AIRFLOW`, `POSTGRES_DB_POLARIS`, `POSTGRES_DB_DATAHUB` |
| MinIO | `MINIO_ROOT_USER`, `MINIO_ROOT_PASSWORD`, `MINIO_ENDPOINT`, `MINIO_BUCKET` |
| Polaris | `POLARIS_SECRET`, `POLARIS_ADMIN_USERNAME`, `POLARIS_ADMIN_PASSWORD` |
| Airflow | `AIRFLOW_ADMIN_USER`, `AIRFLOW_ADMIN_USERNAME`, `AIRFLOW_ADMIN_PASSWORD`, `AIRFLOW_FERNET_KEY` |
| DataHub | `DATAHUB_ADMIN_USERNAME`, `DATAHUB_ADMIN_PASSWORD`, `DATAHUB_SECRET_KEY` |

**Named volumes** (prefixed `lakehouse-project_`): `postgres_data_vol`, `minio_data_vol`,
`spark_events_vol`, `airflow_logs_vol`, `kafka_data_vol`, `kafka_secret_vol`,
`zookeeper_data_vol`, `zookeeper_log_vol`, `zookeeper_secret_vol`, `elasticsearch_data_vol`.
`docker compose down` keeps volumes; add `-v` to wipe everything.

**MinIO layout** (bucket `lakehouse`):

```
s3://lakehouse/
├── bronze/
│   ├── _landing/stock_ohlcv/ingest_date=YYYY-MM-DD/data.parquet   ← written by ingest
│   └── stock_ohlcv/   (data/ + metadata/ of the Iceberg table)
├── silver/stock_ohlcv/
└── gold/stock_daily_metrics/
```

Conventions: every service uses the `Asia/Ho_Chi_Minh` timezone. Each `compose/*.yml` re-declares
the `lakehouse` network and its own `volumes:` so it can be merged independently. Code, comments,
shell scripts and docs are written in English.

## 11. Status & known limitations

**Tested (2026-10-27):**

- 27/27 services OK: 17 running (16 healthy; `datahub-actions` has no healthcheck) and 10 setup
  jobs exited with code 0. Re-running `up -d` still exits 0.
- All 10 host ports respond.
- Spark writes and reads an Iceberg table through Polaris onto MinIO.
- **Trino reads the Spark-written table**; Trino can create tables (CTAS) and `DROP TABLE`.
- Restarting Polaris loses neither catalog nor namespaces.
- Airflow loads the DAG with no import errors, has all 5 connections, and reaches Spark REST,
  Polaris, MinIO, Trino and DataHub.
- pytest: 4/4 on the host, 7/7 Spark tests in the container.

**Limitations:**

| Issue | Impact | Notes |
|---|---|---|
| `vnstock` (and `vnai`) **quarantined on PyPI** since 2026-09-24 | The `ingest_landing` task fails; the pipeline hasn't run end-to-end | `vnstock` is commented out of `airflow/requirements.txt`. Options: synthetic test data, or call the VCI/KBS endpoints directly |
| dbt has no models; `dbt_project.yml` is empty | dbt is unusable for now | `spark-thrift` also lacks the Polaris catalog config |
| Fixed `container_name`s | Only one copy of the stack per Docker host | |
| Python differs by place | Job code must run on Python 3.8 | Spark 3.8, Airflow 3.11, host venv 3.11 |
| `pyarrow` mismatch | Airflow 14.0.2 (required by constraints) vs `pyproject.toml` 15.0.2 | |
| Kafka/ES have no host ports | Can't connect from the host | Add `ports:` if needed |
| DataHub lineage | Not yet verified in the UI | |

## 12. Operations

Run **inside WSL**, from the repo root:

```bash
docker compose config --quiet          # check the 7 modules merge cleanly
docker compose build                   # build custom-spark, custom-airflow
docker compose up -d                   # start the whole stack
docker compose ps -a                   # status + health
docker compose logs -f <service>
docker compose down                    # stop (keeps volumes)
```

Quick checks:

```bash
docker exec trino trino --execute "SHOW SCHEMAS FROM lakehouse"   # bronze gold information_schema silver
curl -s -o /dev/null -w '%{http_code}\n' http://localhost:8181/api/catalog/v1/config  # 401 = Polaris is up
```

Run a Spark job by hand (note `spark-submit` is **not on the PATH**):

```bash
docker compose exec spark-master /opt/spark/bin/spark-submit \
  --py-files /opt/spark/jobs/common.py /opt/spark/jobs/job_bronze.py   # same for silver, gold
```

Tests:

```bash
venv/bin/python -m pytest -m "not spark"        # host (WSL), no Java needed
# Spark tests: run in spark-master (see CLAUDE.md, Common commands)
```

### Web UIs

Every UI below is published on the host and answers once the stack is up (checked 2026-10-27).
Credential variables refer to `.env`.

| UI | URL | What it's for | Login |
|---|---|---|---|
| MinIO console | <http://localhost:9001> | Browse bucket `lakehouse` (bronze/silver/gold files, landing parquet) | `MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD` |
| Airflow | <http://localhost:8085> | Unpause/trigger DAG `lakehouse_ohlcv`, task logs, connections | `AIRFLOW_ADMIN_USER` / `AIRFLOW_ADMIN_PASSWORD` |
| Trino | <http://localhost:8080> (redirects to `/ui/`) | Running/finished queries and cluster status (no SQL editor) | Any username, no password |
| Spark master | <http://localhost:8090> | Workers, running apps and drivers submitted via REST 6066 | None |
| Spark worker | <http://localhost:8081> | Executors/drivers on the worker, their stdout/stderr logs | None |
| Spark Thrift app | <http://localhost:4040> | Spark UI of the `spark-thrift` app (jobs, SQL tab) | None |
| Spark history | <http://localhost:18080> | UIs of finished Spark apps (from the event log) | None |
| DataHub | <http://localhost:9002> | Dataset search, lineage | `datahub` / `datahub` (image default; compose doesn't override it) |

Ports that answer but have **no web UI** (APIs only): MinIO S3 API `:9000`, Polaris `:8181`
(an unauthenticated `GET /api/catalog/v1/config` → 401), Spark REST submit `:6066`, DataHub GMS
`:8083`, Spark Thrift JDBC `:10000`, Postgres `:5432`.

## 13. Appendix: pitfalls hit while building

| Pitfall | Symptom | Fix |
|---|---|---|
| Polaris defaults to an **in-memory** metastore | Log says "intended for tests only"; database `polaris` has no tables | Enable `relational-jdbc` and add `polaris-bootstrap` |
| `admin-tool bootstrap` exits 3 on a 2nd run (docs say idempotent) | "already been bootstrapped" | Wrap in `sh`, treat that error as success |
| Re-adding an existing grant returns HTTP 500 | `duplicate key … grant_records_pkey` | `init.sh` lists grants first, then grants |
| Namespaces created via the Management API get 404 | `curl -s` still exits 0, so the script printed "created" | Use the Iceberg REST API and check `%{http_code}` |
| Trino 435 can't set the OAuth2 scope | `invalid_scope` | Upgrade to Trino 454, add `oauth2.scope` |
| Trino has no S3 credentials | `Unable to load AWS credentials` | Add `fs.native-s3.*` config |
| `pyarrow 15.0.2` conflicts with Airflow constraints | pip `ResolutionImpossible` | Downgrade to 14.0.2 |
| WSL shell opened before Docker integration was enabled | `permission denied … docker.sock` | Open a new shell or run `newgrp docker` |
| Docker Hub CDN drops the connection | `httpReadSeeker … EOF` | Re-run the command |
