# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A local "lakehouse" stack defined entirely as Docker Compose services — object storage, an Iceberg
REST catalog, Spark processing, dbt transforms, Trino query, and Airflow orchestration, all wired
together on a single Docker network. Everything is configured through `.env`, the YAML files under
`compose/`, and the shell init scripts.

**Product direction (chosen 2026-10-27):** a **simple end-to-end medallion pipeline** that
demonstrates every lakehouse phase on this stack — ingest → bronze → silver → gold → query. The
source is daily stock **OHLCV** from the Vietnamese-market library **`vnstock`**. Deliberately
small (a handful of symbols, full-rebuild each run) so the flow is easy to follow; swap in
MERGE/incremental later. The flow:

```
vnstock OHLCV ─(ingest, Python)→ s3://lakehouse/bronze/_landing/*.parquet
   → Spark job_bronze → lakehouse.bronze.stock_ohlcv        (raw, deduped by symbol+date)
   → Spark job_silver → lakehouse.silver.stock_ohlcv        (cleaned + DQ assertions)
   → Spark job_gold   → lakehouse.gold.stock_daily_metrics  (daily return, MA5/MA20, range)
   → query via Trino (catalog `lakehouse`)
Airflow DAG `lakehouse_ohlcv` chains: ingest → bronze → silver → gold.
```

Every medallion layer is a real Iceberg table in Polaris, so Trino can query all three. The
pipeline code lives in `spark/jobs/`, `airflow/plugins/` and `airflow/dags/`; see **Next steps**
for how to run it.

## Architecture

The entry point `docker-compose.yml` only `include`s the modular files under `compose/` and
declares the shared network. Every service joins the single bridge network **`lakehouse`** and
reaches the others by container/service name (e.g. Spark hits MinIO at `http://minio:9000`, Trino
hits the catalog at `http://polaris:8181`, dbt hits `spark-thrift:10000`). All credentials and
tunables live in `.env` and are injected via `environment:` — change config there, not by
hardcoding values in compose files.

Included modules (active set in `docker-compose.yml`): `infra.yml`, `storage.yml`, `catalog.yml`,
`spark.yml`, `trino.yml`, `airflow.yml`, `datahub.yml`.

Layers:

- **Storage** (`compose/storage.yml`): MinIO (S3-compatible object store, console on 9001). A
  one-shot `minio-setup` job runs `minio/scripts/init.sh` to create the `lakehouse` bucket with
  `bronze/`, `silver/`, `gold/` prefixes (idempotent — skips if `/data/lakehouse` already exists).
- **Catalog** (`compose/catalog.yml`): **Apache Polaris** — an Iceberg **REST** catalog backed by
  Postgres (not file/RocksDB), reached at `http://polaris:8181/api/catalog`, secured with
  **OAuth2 client-credentials** (`root` / `${POLARIS_SECRET}`). Health lives on the Quarkus
  management port **8182** (`/q/health`), not the main port 8181. A one-shot `polaris-setup` job
  runs `polaris/scripts/init.sh` to fetch a token, create the `lakehouse` catalog (S3 storage
  config pointed at MinIO, path-style access) and the `bronze`/`silver`/`gold` namespaces.
- **Processing** (`compose/spark.yml`): a Spark 3.5.1 cluster built from `spark/Dockerfile`
  (`custom-spark:3.5.1`) bundling the Hadoop-AWS, Iceberg (`iceberg-spark-runtime` +
  `iceberg-aws-bundle`, which carry the REST catalog client — no Nessie), Postgres, Kafka, and
  acryl DataHub-lineage JARs. Services: `spark-master` (RPC 7077, UI 8090→8080, REST 6066),
  `spark-worker` (UI 8081), `spark-thrift` (Hive Thrift JDBC on 10000 for dbt, app UI 4040),
  `spark-history` (18080). Job code lives in `./spark/jobs`, mounted at `/opt/spark/jobs` with
  `PYTHONPATH=/opt/spark/jobs`.
- **Transform** (`./dbt`): **dbt** with the `dbt-spark` adapter connecting over **thrift** to
  `spark-thrift:10000`, default schema `silver` (`dbt/profiles.yml`). The whole `./dbt` dir is
  mounted into the Airflow containers at `/opt/airflow/dbt`. `dbt_project.yml` is currently empty
  and there are no models yet.
- **Query** (`compose/trino.yml`): **Trino 435** with the Iceberg connector pointed at the Polaris
  REST catalog (`trino/catalog/lakehouse.properties`, OAuth2 with `${ENV:POLARIS_SECRET}`), web/
  JDBC on **8080**.
- **Orchestration** (`compose/airflow.yml`): Airflow 2.9.2 with `LocalExecutor`, custom-built from
  `airflow/Dockerfile` (`custom-airflow:2.9.2`, base `apache/airflow:2.9.2-python3.11` + extra pip
  deps incl. PyHive/thrift-sasl for the Spark Thrift connection). Web UI on **8085**→8080. DAGs in
  `airflow/dags`, plugins in `airflow/plugins`. **Airflow submits Spark jobs in cluster mode via
  the Spark Standalone REST API** (`spark-master:6066`), not via a `spark://` master connection.
- **Shared infra** (`compose/infra.yml`): one Postgres 15 instance backing multiple app DBs
  (`airflow`, `polaris`, `datahub` — see `POSTGRES_DB_*` in `.env`); a one-shot `postgres-setup`
  runs `postgres/scripts/init.sh` to idempotently create those roles/databases. Also Zookeeper,
  Kafka, and Elasticsearch 7.17 + a `kafka-setup` job that pre-creates DataHub's Kafka topics —
  these back the DataHub layer (`datahub.yml`).
- **Metadata/lineage** (`compose/datahub.yml`): **DataHub v0.12.1** (pinned to match
  `acryl-datahub[airflow]==0.12.1.5`), mapped from the official MySQL quickstart onto this repo's
  **Postgres** (`EBEAN_DATASOURCE_*` → `jdbc:postgresql://postgres:5432/datahub`), **Kafka**
  (`kafka:29092`), and **Elasticsearch** as the graph store (`GRAPH_SERVICE_IMPL=elasticsearch`,
  no Neo4j). Services: `datahub-gms` (API on host **8083**→8080, runs MAE/MCE/PE consumers
  embedded), `datahub-frontend-react` (UI on **9002**), `datahub-actions`, plus one-shot setup
  jobs `datahub-elasticsearch-setup`, `datahub-postgres-setup`, and `datahub-upgrade -u
  SystemUpdate`. A Confluent **`schema-registry`** (cp 7.6.0, internal only) is added here because
  DataHub requires one and `infra.yml` doesn't ship it. Spark/Airflow emit lineage to GMS via the
  acryl JAR / `datahub_conn`. **Env vars are taken verbatim from the `v0.12.1` tag** — don't
  hand-edit them from memory; re-derive from the upstream quickstart if bumping the version.

Medallion convention is consistent end-to-end: `bronze` / `silver` / `gold` exist both as MinIO
prefixes under `s3://lakehouse/` and as Polaris namespaces.

## Current state

- **Infrastructure wiring is complete.** The modular split under `compose/` is done and is the
  source of truth (no monolithic `docker-compose_bk.yml`). All seven modules — `infra`, `storage`,
  `catalog`, `spark`, `trino`, `airflow`, `datahub` — are active in `docker-compose.yml` and
  `docker compose config` merges cleanly (cross-file `depends_on` all resolve).
- **Validated only at config level — the stack has never been built or started at runtime.** No
  `docker compose build` / `up` has been smoke-tested here, so image pulls/builds, the long
  healthcheck dependency chain, and DataHub's first-run reindex are all unverified. Treat the first
  `up -d` as a debugging session, not a known-good boot.
- Build-context dirs `./spark` and `./airflow` exist (each with a `Dockerfile`). `./minio`,
  `./postgres`, `./polaris`, `./trino` exist as **stock images + mounted init scripts / config**
  (no Dockerfile in those).
- **DataHub** images are all pinned to explicit versions (no `head`/`latest`): server components at
  `v0.12.1`, `datahub-actions` at `v0.0.14` (the Actions release current when 0.12.1 shipped —
  Actions has its own release line; v0.0.15 didn't land until 2024-02).
- **The OHLCV medallion pipeline is written (but not runtime-tested):** `spark/jobs/common.py`,
  `job_bronze.py`, `job_silver.py`, `job_gold.py`; `airflow/plugins/ingest_ohlcv.py` (vnstock →
  parquet landing), `airflow/plugins/spark_rest.py` (submit to Spark REST 6066); and the DAG
  `airflow/dags/lakehouse_ohlcv_dag.py` (dag_id `lakehouse_ohlcv`). The jobs configure the
  Polaris/Iceberg catalog themselves in
  `common.py` (because `spark-defaults.conf` doesn't). Still **empty/not-written:** dbt models and
  `dbt/dbt_project.yml` (empty, so dbt cannot run as-is).
- **`.scratch_vnstock/`** is a throwaway `uv` venv used to verify the `vnstock` API; it is not part
  of the stack and can be deleted anytime.

## Next steps

The medallion pipeline is written; the remaining work is running it and then adding the
transform/analytics niceties.

1. **Smoke-test the stack** (prerequisite — never booted): `docker compose build` then
   `docker compose up -d`. Watch the healthcheck chain (postgres → setup jobs → polaris/airflow →
   spark). DataHub's first-run reindex is RAM-hungry; bring it up last if memory is tight.
2. **Run the pipeline.** Two ways, same jobs:
   - **By hand** (fastest to debug): ingest locally, then submit each Spark job in-container —
     ```bash
     # ingest: dùng venv có vnstock (vd .scratch_vnstock) — cần boto3 + pyarrow
     PYTHONUTF8=1 python airflow/plugins/ingest_ohlcv.py --symbols FPT,VNM,ACB \
       --endpoint-url http://localhost:9000   # access/secret lấy từ env MINIO_*
     docker compose exec spark-master spark-submit --py-files /opt/spark/jobs/common.py /opt/spark/jobs/job_bronze.py
     docker compose exec spark-master spark-submit --py-files /opt/spark/jobs/common.py /opt/spark/jobs/job_silver.py
     docker compose exec spark-master spark-submit --py-files /opt/spark/jobs/common.py /opt/spark/jobs/job_gold.py
     ```
   - **Via Airflow:** unpause the `lakehouse_ohlcv` DAG in the UI (8085) and trigger it; it chains
     ingest → bronze → silver → gold (Spark steps submit through the REST API on 6066).
3. **Query the result in Trino** (8080), catalog `lakehouse`: e.g.
   `SELECT symbol, trade_date, close, daily_return, ma20 FROM gold.stock_daily_metrics ORDER BY trade_date DESC LIMIT 20;`
4. **(Optional) Move silver/gold to dbt.** `dbt/dbt_project.yml` is still empty; port the two
   transforms to dbt-spark models over `spark-thrift:10000` once the thrift server also carries the
   Polaris catalog config (see note in `compose/spark.yml`).
5. **(Optional) Streaming + lineage.** Poll `vnstock` on an interval into **Kafka**, sink to
   `bronze`; confirm **DataHub** lineage in the UI (9002).

### `vnstock` API (verified 2026-10-27, v4.0.4)

Use the `vnstock.api` classes (the old `Vnstock()` facade is deprecated). The pipeline ingests daily
**OHLCV history**:

```python
from vnstock.api.quote import Quote
df = Quote(symbol="FPT", source="VCI").history(start="2024-01-01", end="2024-01-31", interval="1D")
# columns: time (datetime64), open, high, low, close (float), volume (int)
```

(The price board — `Trading(source="VCI").price_board([...])`, MultiIndex `match|match_price` etc. —
is still there for a future near-real-time quote feed.) `vnstock` prints a Vietnamese banner and
needs UTF-8 stdout on Windows (`PYTHONUTF8=1`); a gen5 (v5) banner shows but the v4 `Quote.history`
API still works, which is why `requirements.txt` pins `vnstock==4.0.4`.

## Common commands

All from the repo root:

```bash
docker compose config            # validate/merge the included compose files — good first sanity check
docker compose build             # build custom images (custom-spark, custom-airflow)
docker compose up -d             # start the full active stack
docker compose up -d <service>   # start one service and its dependencies
docker compose logs -f <service> # tail logs for one service
docker compose down              # stop and remove containers (named volumes persist)
```

There are no lint/test/format commands (no Python source, no Makefile, no pyproject/pytest config).

### Key ports (host)

MinIO 9000 / console 9001 · Polaris 8181 (health 8182) · Spark master UI 8090, RPC 7077, REST 6066
· Spark worker 8081 · Spark Thrift 10000, UI 4040 · Spark history 18080 · Trino 8080 · Airflow UI
8085 · DataHub UI 9002, GMS API 8083 · Postgres 5432 · Kafka 9092 · Elasticsearch 9200.

## Conventions already in the repo

- **Airflow connections are created in code, not the UI.** `airflow/scripts/init.sh` (run once by
  `airflow-init`) migrates the DB, ensures the admin user, and idempotently (delete-then-add)
  recreates `spark_rest_conn` (HTTP → Spark REST 6066), `minio_conn`, `postgres_conn`,
  `trino_conn`, and `datahub_conn` from `.env`. Edit that script when connection config changes.
- **Permission fix-up pattern**: containers run as different UIDs (Spark 185, …). A one-shot
  `spark-fix-permissions` (busybox) `chown`s the `spark_events_vol` to 185 before the Spark
  services start. If you add a service whose non-root container writes to a named volume, follow
  this pattern rather than fixing permissions ad hoc.
- **Healthcheck-gated startup**: most `depends_on` use `condition: service_healthy` or
  `service_completed_successfully` rather than plain ordering — preserve this so the dependency
  chain (postgres → postgres-setup → polaris/airflow-init → real services) doesn't race.
- Shell scripts and `.env` comments are written in **Vietnamese** — match that when editing them.
- Timezone is pinned to `Asia/Ho_Chi_Minh` (`TZ`) across every service.
- Each `compose/*.yml` re-declares the `lakehouse` network (and its own `volumes:`) so the file
  stays independently mergeable — keep that when adding modules.

## Editing notes

- When asked to add a service, put it in the matching `compose/*.yml` module (create the module and
  add it to the `include:` list in `docker-compose.yml` if needed), join the `lakehouse` network,
  pull config from `.env`, and gate startup with healthchecks.
- The catalog is **Polaris over Iceberg REST + OAuth2** — when configuring a new engine (Spark
  session, Trino catalog, dbt), point it at `http://polaris:8181/api/catalog` with the OAuth2
  credential, warehouse `lakehouse`; don't reintroduce Nessie/Hive-metastore config.
