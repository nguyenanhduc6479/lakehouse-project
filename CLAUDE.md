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
- **Catalog** (`compose/catalog.yml`): **Apache Polaris 1.5.0** (image pinned, not `latest`) — an
  Iceberg **REST** catalog reached at `http://polaris:8181/api/catalog`, secured with **OAuth2
  client-credentials** (`root` / `${POLARIS_SECRET}`, scope `PRINCIPAL_ROLE:ALL`). Persisted in
  Postgres via **`POLARIS_PERSISTENCE_TYPE=relational-jdbc`** — without it Polaris silently runs an
  **in-memory** metastore (log: "intended for tests only") and loses every catalog/table on
  restart. Chain: `postgres-setup` → **`polaris-bootstrap`** (`apache/polaris-admin-tool:1.5.0
  bootstrap`, creates schema + realm `POLARIS`) → `polaris` → `polaris-setup`. Health is on the
  Quarkus management port **8182** (`/q/health`, container-internal only — not published to the
  host). `polaris-setup` runs `polaris/scripts/init.sh` (idempotent, checks every HTTP code) to:
  create/update the `lakehouse` catalog (storage `endpoint`=MinIO, `pathStyleAccess`,
  `stsUnavailable=true`, `region`; property `polaris.config.drop-with-purge.enabled=true`), grant
  **`CATALOG_MANAGE_CONTENT`** to `catalog_admin`, and create the `bronze`/`silver`/`gold`
  namespaces through the **Iceberg REST** API (`POST /api/catalog/v1/lakehouse/namespaces`).
  Because `stsUnavailable=true` (no credential vending), Polaris reads/writes metadata with
  `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY`/`AWS_REGION` env, and each engine brings its own
  MinIO keys.
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
- **Query** (`compose/trino.yml`): **Trino 454** with the Iceberg connector pointed at the Polaris
  REST catalog (`trino/catalog/lakehouse.properties`), web/JDBC on **8080**. 454 is the minimum:
  `iceberg.rest-catalog.oauth2.scope` (required by Polaris) first exists there — on 435 every call
  fails with `invalid_scope`. The same file configures the native S3 filesystem
  (`fs.native-s3.enabled`, `s3.endpoint`/`region`/`path-style-access`/keys via `${ENV:MINIO_*}`)
  so Trino can read/write data files on MinIO.
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
- **Runtime-validated 2026-10-27 (WSL2 Ubuntu + Docker Desktop).** `docker compose build` and
  `up -d` succeed (also on re-runs); all 27 services come up: 17 running (16 healthy —
  `datahub-actions` has no healthcheck) + 10 one-shot jobs `Exited (0)`. Verified end-to-end:
  every host port answers; Spark (via `common.get_spark`) creates/writes/reads an Iceberg table
  through Polaris onto MinIO; **Trino reads the Spark-written table** and can CTAS + `DROP TABLE`;
  Polaris catalog/namespaces survive a `polaris` restart; Airflow loads `lakehouse_ohlcv` with no
  import errors, has all 5 connections, and reaches spark REST 6066 / polaris / minio / trino /
  datahub-gms; pytest 4/4 (host) + 7/7 Spark (container). The medallion **jobs themselves have not
  run end-to-end yet** — blocked on the ingest source (see `vnstock` below).
- **All `container_name`s are fixed**, so only one copy of this stack can exist per Docker host. An
  older copy (compose project `lakehouse` from `C:\Study_DE\lakehouse`) blocked `up` with name
  conflicts and was deleted (containers + its `lakehouse_*` volumes). This project's volumes are
  `lakehouse-project_*`.
- **Python versions differ by place:** the `custom-spark` image runs **Python 3.8.10** (code must
  stay 3.8-compatible at runtime — `from __future__ import annotations` is what makes `str | None`
  hints OK), Airflow is 3.11, the host `venv/` is 3.11.
- `airflow/requirements.txt` pins `pyarrow==14.0.2` because Airflow 2.9.2 constraints pin it
  (15.0.2 → pip `ResolutionImpossible`). `pyproject.toml` (host dev group) still has 15.0.2.
- Build-context dirs `./spark` and `./airflow` exist (each with a `Dockerfile`). `./minio`,
  `./postgres`, `./polaris`, `./trino` exist as **stock images + mounted init scripts / config**
  (no Dockerfile in those).
- **DataHub** images are all pinned to explicit versions (no `head`/`latest`): server components at
  `v0.12.1`, `datahub-actions` at `v0.0.14` (the Actions release current when 0.12.1 shipped —
  Actions has its own release line; v0.0.15 didn't land until 2024-02).
- **The OHLCV medallion pipeline is written (jobs not yet run end-to-end):** `spark/jobs/common.py`,
  `job_bronze.py`, `job_silver.py`, `job_gold.py`; `airflow/plugins/ingest_ohlcv.py` (vnstock →
  parquet landing), `airflow/plugins/spark_rest.py` (submit to Spark REST 6066); and the DAG
  `airflow/dags/lakehouse_ohlcv_dag.py` (dag_id `lakehouse_ohlcv`). The jobs configure the
  Polaris/Iceberg catalog themselves in
  `common.py` (because `spark-defaults.conf` doesn't). Transform logic is factored into pure
  functions (`transform_bronze` / `clean_silver` / `build_gold`) with `pytest` tests under `tests/`
  (ingest tests mock vnstock+boto3 and pass on the host; Spark tests run in the container). Still
  **empty/not-written:** dbt models and `dbt/dbt_project.yml` (empty, so dbt cannot run as-is).
- **Host dev env:** `venv/` at the repo root is a Linux (WSL) venv, Python 3.11 managed by `uv`,
  with the `dev` dependency group installed. WSL has **no Java**, so `@pytest.mark.spark` tests
  can't run there — run them in the container (see Common commands). The old `.scratch_vnstock/`
  venv no longer exists.
- **Under git now** (was not before). Remote `lakehouse` →
  `https://github.com/nguyenanhduc6479/lakehouse-project.git`; branches `main` and `dev` both track
  it (`dev` is the working branch). An older remote `origin` → `my-project.git` also exists. `.env`
  and `.claude/settings.local.json` are gitignored, so secrets are never pushed — keep it that way.

## Next steps

The stack is up and validated; the remaining work is an ingest source, running the jobs, and then
the transform/analytics niceties.

1. **Unblock ingest** (`vnstock` is uninstallable — see below). Options under consideration:
   (a) seed synthetic OHLCV parquet (same schema) into `bronze/_landing/` to exercise
   bronze → silver → gold → Trino now; (b) replace `vnstock` with direct calls to the public
   VCI/KBS price endpoints it wrapped. Not chosen: vendoring old `vnstock`/`vnai` wheels — the
   packages are quarantined pending a security review.
2. **Run the pipeline.** Two ways, same jobs:
   - **By hand** (fastest to debug): ingest, then submit each Spark job in-container —
     ```bash
     # ingest: cần venv có vnstock + boto3 + pyarrow (hiện vnstock không cài được — xem bước 1)
     PYTHONUTF8=1 python airflow/plugins/ingest_ohlcv.py --symbols FPT,VNM,ACB \
       --endpoint-url http://localhost:9000   # access/secret lấy từ env MINIO_*
     # spark-submit KHÔNG nằm trên PATH trong container -> dùng đường dẫn đầy đủ
     docker compose exec spark-master /opt/spark/bin/spark-submit --py-files /opt/spark/jobs/common.py /opt/spark/jobs/job_bronze.py
     docker compose exec spark-master /opt/spark/bin/spark-submit --py-files /opt/spark/jobs/common.py /opt/spark/jobs/job_silver.py
     docker compose exec spark-master /opt/spark/bin/spark-submit --py-files /opt/spark/jobs/common.py /opt/spark/jobs/job_gold.py
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

### `vnstock` API (v4.0.4) — currently NOT installable

**Status (checked 2026-10-27):** PyPI has **quarantined** `vnstock` (since 2026-09-24, pending a
security review) and its dependency `vnai`: simple index lists 0 files, JSON API returns 404.
`vnstock3` (3.2.1 / 3.1.0 / 0.3.0.9) is also unresolvable because every version requires `vnai`,
and its `core/__init__.py` hard-imports `vnai`. Hence `vnstock==4.0.4` is **commented out** in
`airflow/requirements.txt` (the image builds; `ingest_ohlcv.py` imports vnstock lazily so the DAG
still parses, only the `ingest_landing` task fails at run time). The API notes below are kept for
when a source is restored:

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
API still works, which is why `requirements.txt` pinned `vnstock==4.0.4`.

## Common commands

All from the repo root, **inside WSL** (Docker Desktop → Settings → Resources → WSL Integration
must be on for the distro; a shell opened before that was enabled lacks the `docker` group →
`permission denied ... docker.sock` — open a new shell or `newgrp docker`, don't `chmod` the socket):

```bash
docker compose config            # validate/merge the included compose files — good first sanity check
docker compose build             # build custom images (custom-spark, custom-airflow)
docker compose up -d             # start the full active stack
docker compose up -d <service>   # start one service and its dependencies
docker compose logs -f <service> # tail logs for one service
docker compose down              # stop and remove containers (named volumes persist)
```

Docker Hub's CDN intermittently drops large layer downloads (`httpReadSeeker ... EOF`), which aborts
`up` before any container is created — just retry. Don't run a blanket `docker compose pull`:
it also tries `custom-*` (local-only) and `minio/minio`, which Docker Hub now reports as
"repository does not exist"; the locally cached `minio/minio` image is what runs.

Python tooling (pipeline code — `pyproject.toml` at root, `uv` + `ruff` + `pytest`):

```bash
# venv/ (WSL, Python 3.11): deps nằm trong dependency-group `dev`, KHÔNG có requirements.txt ở root
uv pip install --python venv/bin/python --group dev
venv/bin/ruff format . && venv/bin/ruff check --fix .   # format + lint
venv/bin/python -m pytest -m "not spark"                 # unit tests trên host (ingest, mock)
```

Spark transform tests need a JVM (WSL has none) — run them in `spark-master`. The container has
no `pytest` and `tests/` is not mounted, so copy them in and install pytest to `/tmp` (pytest
8.3.x is the last line supporting the container's Python 3.8; `--user` fails on permissions):

```bash
docker exec spark-master python3 -m pip install -q --target /tmp/pylib 'pytest==8.3.5'
docker exec -u root spark-master rm -rf /tmp/tests && docker cp tests spark-master:/tmp/tests
docker exec -e PYTHONPATH=/tmp/pylib:/opt/spark/jobs:/opt/spark/python/lib/pyspark.zip:/opt/spark/python/lib/py4j-0.10.9.7-src.zip \
  spark-master python3 -m pytest -q -p no:cacheprovider /tmp/tests/test_bronze.py /tmp/tests/test_silver.py /tmp/tests/test_gold.py
```

Tests live in `tests/` (fixtures in `conftest.py`); `@pytest.mark.spark` = cần JVM. Pure-Python
ingest tests mock `vnstock` + `boto3` and pass on the host; the Spark transform tests
(`transform_bronze` / `clean_silver` / `build_gold`) run in the spark container.

Quick catalog check from Trino (proves OAuth2 scope + Polaris persistence + namespaces):
`docker exec trino trino --execute "SHOW SCHEMAS FROM lakehouse"` → `bronze gold information_schema silver`.

### Key ports (host)

MinIO 9000 / console 9001 · Polaris 8181 (health 8182 is container-only, not published; an
unauthenticated `GET :8181/api/catalog/v1/config` → 401 means it's up) · Spark master UI 8090, RPC 7077, REST 6066
· Spark worker 8081 · Spark Thrift 10000, UI 4040 · Spark history 18080 · Trino 8080 · Airflow UI
8085 · DataHub UI 9002, GMS API 8083 · Postgres 5432. **Not published** (container network only,
verified with `docker ps`): Kafka 9092/29092, Zookeeper 2181, Elasticsearch 9200, schema-registry
8081 — Kafka's `PLAINTEXT_HOST://localhost:9092` listener is unreachable from the host until a
`ports:` entry is added.

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
  credential **and scope `PRINCIPAL_ROLE:ALL`**, warehouse `lakehouse`, plus the engine's own
  MinIO S3 settings (Polaris does not vend credentials); don't reintroduce Nessie/Hive-metastore
  config.
- **Polaris 1.5.0 quirks verified here (docs disagree):** `admin-tool bootstrap` is *not*
  idempotent (2nd run → exit 3 "already been bootstrapped"; `polaris-bootstrap` wraps it in `sh`
  to treat that as success), and re-adding an existing grant returns **HTTP 500 duplicate key**
  (`init.sh` lists grants first). Namespaces are Iceberg-REST resources, not Management-API ones.
  Derive request/field names from the spec at tag `apache-polaris-1.5.0`
  (`spec/polaris-management-service.yml`), not memory.
- In setup scripts, never trust `curl -s` exit codes — it returns 0 on HTTP 4xx/5xx. The original
  `polaris/scripts/init.sh` printed "created" for every namespace while Polaris answered 404.
  Capture `%{http_code}` and fail loudly (see `req`/`die` in that script).
