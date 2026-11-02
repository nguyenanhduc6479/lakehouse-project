# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A **demo CDC data platform** on Docker Compose, built to learn how the pieces fit together — not a
production system. Full design: **`docs/dataplatform-design.md`** (read it before building a new
phase); build order and per-phase checks: **`docs/roadmap.md`** (keep its Status column current). The target flow:

```
generator.py → source-db (Postgres, wal_level=logical)
  → Debezium (Kafka Connect, Avro + Schema Registry) → Kafka topics src.shop.*
  → Iceberg sink → lakehouse.bronze.*_cdc   (append-only change events, Polaris catalog, MinIO)
  → PySpark SCD2 → Postgres `data-platform`.silver.*   (history; also the web app DB)
  → dbt-clickhouse → ClickHouse gold → Superset
  → batch sync → Elasticsearch → FastAPI → web page
Airflow DAG `platform_pipeline` (manual trigger): silver → (ES sync ‖ dbt run/test)
```

Query tool per layer (decided 2026-11-02): **Trino** reads bronze (inspection/debugging only, not on
the data path); silver is queried directly in Postgres; **ClickHouse** is the OLAP engine for gold
(kept deliberately for learning, even though Postgres could handle demo-sized gold).

**Scope rules (chosen 2026-11-02):** Docker Compose only (no Kubernetes); the source is one Python
generator writing `shop.users` / `shop.orders` / `shop.order_details`; DAGs use `schedule=None`
and are triggered by hand; silver is **eventually consistent across tables** (each table processed
independently, ordered by `source.lsn` per key, idempotent). Prefer the simplest thing that runs;
no HA, replicas, poolers or TLS.

## Current state

- **Clean slate (2026-11-02).** The previous vnstock OHLCV medallion pipeline, its tests, the old
  `docs/system-overview.*` and the whole DataHub layer were removed. What remains is reusable
  infra: Postgres, Zookeeper + Kafka, Schema Registry, Elasticsearch, MinIO, Polaris, Spark,
  Trino, Airflow, Grafana/Loki/Alloy. Roadmap phase 1 of the design doc is done; phase 2 has
  started: the `source-db` instance exists (no schema, CDC role/publication or generator yet).
- `docker compose config` merges cleanly (23 services). On 2026-11-02 the stack was torn down with
  `down -v --remove-orphans` (all containers and volumes, incl. DataHub leftovers, removed) and only
  `postgres` + `postgres-setup` + `source-db` were brought back up and verified; the rest of the
  stack has not been re-run since DataHub was removed.
- `spark/jobs/helper.py` (`get_spark`) builds a SparkSession wired to Polaris + MinIO — reuse it
  for every Spark job. `airflow/plugins/spark_rest.py` (`submit_and_wait`) submits a job via the
  Spark REST API. `airflow/dags/` is empty. `tests/` has only `conftest.py` (a local Spark fixture).
- `dbt/` still holds an empty `dbt_project.yml` and a dbt-spark profile; it becomes dbt-clickhouse
  in the gold phase.
- **Python versions differ by place:** the `custom-spark` image runs **Python 3.8.10** (code must
  stay 3.8-compatible at runtime — `from __future__ import annotations` is what makes `str | None`
  hints OK), Airflow is 3.11, the host `venv/` is 3.11.
- **All `container_name`s are fixed**, so only one copy of this stack can exist per Docker host.
  Volumes are `lakehouse-project_*`.
- **Under git.** Remote `lakehouse` → `https://github.com/nguyenanhduc6479/lakehouse-project.git`;
  `dev` is the working branch, `main` the default. An older remote `origin` → `my-project.git` also
  exists. `.env` and `.claude/settings.local.json` are gitignored — keep it that way.

## Architecture (what exists now)

The entry point `docker-compose.yml` only `include`s the modules under `compose/` (`infra.yml`, `source.yml`,
`storage.yml`, `catalog.yml`, `spark.yml`, `trino.yml`, `airflow.yml`, `monitoring.yml`) and
declares the shared network. Every service joins the bridge network **`lakehouse`** and reaches
the others by service name. All credentials and tunables live in `.env` and are injected via
`environment:` — don't hardcode them in compose files.

- **Postgres — two instances**, both on the same `postgres:16` image. `postgres` (`compose/infra.yml`, host 5432) is shared: its
  default DB `data-platform` (`POSTGRES_DB`, created by initdb) holds silver/staging, and a
  one-shot `postgres-setup` runs `postgres/scripts/init.sh` to idempotently create the `airflow`
  and `polaris` roles/DBs. `source-db` (`compose/source.yml`, host **5433**, DB `source-db`,
  `SOURCE_POSTGRES_*` in `.env`) is the CDC source, kept separate because `wal_level=logical` and
  slot WAL retention (`max_slot_wal_keep_size=1GB`) are instance-wide. `POSTGRES_*` only apply when
  a volume is first initialised — after changing them, recreate the volume (`down -v`).
- **Shared infra** (`compose/infra.yml`): Zookeeper + Kafka (cp 7.6.0,
  in-network listener `kafka:29092`, topic auto-create on). **Schema Registry** (cp 7.6.0,
  `http://schema-registry:8081`, internal only). **Elasticsearch 7.17** single node, security off
  (internal only) — the search layer.
- **Storage** (`compose/storage.yml`): MinIO (console 9001). One-shot `minio-setup` runs
  `minio/scripts/init.sh` to create bucket `lakehouse` with `bronze/`, `silver/`, `gold/` prefixes
  (skips if `/data/lakehouse` exists).
- **Catalog** (`compose/catalog.yml`): **Apache Polaris 1.5.0** — Iceberg **REST** catalog at
  `http://polaris:8181/api/catalog`, **OAuth2 client-credentials** (`root` / `${POLARIS_SECRET}`,
  scope `PRINCIPAL_ROLE:ALL`), persisted in Postgres via
  **`POLARIS_PERSISTENCE_TYPE=relational-jdbc`** (without it Polaris silently runs in-memory and
  loses everything on restart). Chain: `postgres-setup` → `polaris-bootstrap` → `polaris` →
  `polaris-setup`. Health on management port **8182** (container-only). `polaris/scripts/init.sh`
  creates the `lakehouse` catalog (MinIO endpoint, `pathStyleAccess`, `stsUnavailable=true`,
  `polaris.config.drop-with-purge.enabled=true`), grants `CATALOG_MANAGE_CONTENT` to
  `catalog_admin`, and creates namespaces `bronze`/`silver`/`gold` via the Iceberg REST API. No
  credential vending: every engine brings its own MinIO keys.
- **Processing** (`compose/spark.yml`): Spark 3.5.1 from `spark/Dockerfile` (`custom-spark:3.5.1`:
  hadoop-aws, `iceberg-spark-runtime` + `iceberg-aws-bundle` 1.5.2, Postgres JDBC, kafka-clients).
  `spark-master` (RPC 7077, UI 8090, REST 6066), `spark-worker` (UI 8081, size from
  `SPARK_WORKER_CORES`/`SPARK_WORKER_MEMORY`), `spark-thrift` (10000, UI 4040; capped at
  `spark.cores.max=1` + 512m — without the cap it takes every core and REST-submitted jobs wait
  forever). Jobs in `./spark/jobs` → `/opt/spark/jobs`, `PYTHONPATH=/opt/spark/jobs`. Event logs go
  to `spark_events_vol` (no history server; Grafana is used for logs).
- **Query** (`compose/trino.yml`): **Trino 454** (8080), Iceberg connector on Polaris
  (`trino/catalog/lakehouse.properties`) + native S3 to MinIO. 454 is the minimum: it is the first
  version with `iceberg.rest-catalog.oauth2.scope` (435 fails with `invalid_scope`).
- **Orchestration** (`compose/airflow.yml`): Airflow 2.9.2, LocalExecutor, `custom-airflow:2.9.2`
  from `airflow/Dockerfile` (+ `airflow/requirements.txt`, installed against the 2.9.2
  constraints). UI on **8085**. **Spark jobs are submitted in cluster mode through the Spark
  Standalone REST API** (`spark-master:6066`), not a `spark://` connection.
- **Logs** (`compose/monitoring.yml`, config in `./monitoring`): **Grafana 13.2.3** (3000) reading
  **Loki 3.7.8** (internal, 7-day retention), fed by **Grafana Alloy v1.20.1** tailing this
  project's containers via the Docker socket (labels `container`, `service`, `stream`). Loki is
  distroless, so `alloy`'s healthcheck probes both via bash `/dev/tcp` (`CMD bash -c`, not
  `CMD-SHELL`). Alloy only sees running containers.

To be added by the roadmap: the generator, Kafka Connect (Debezium + Iceberg sink,
`confluentinc/cp-kafka-connect` which already has the AvroConverter), ClickHouse, Superset,
FastAPI.

## Common commands

All from the repo root, **inside WSL** (Docker Desktop → WSL Integration must be on; a shell
opened before that lacks the `docker` group → `permission denied ... docker.sock` — open a new
shell or `newgrp docker`, don't `chmod` the socket):

```bash
docker compose config            # validate/merge the included compose files — first sanity check
docker compose build             # build custom images (custom-spark, custom-airflow)
docker compose up -d --remove-orphans
docker compose up -d <service>   # one service and its dependencies
docker compose logs -f <service>
docker compose down              # named volumes persist
```

Docker Hub's CDN intermittently drops large layer downloads (`httpReadSeeker ... EOF`) — retry.
Don't run a blanket `docker compose pull`: it also tries the local-only `custom-*` images and
`minio/minio` (Docker Hub reports "repository does not exist"; the locally cached image runs).

`spark-submit` is not on the container PATH — use `/opt/spark/bin/spark-submit`, e.g.
`docker compose exec spark-master /opt/spark/bin/spark-submit --py-files /opt/spark/jobs/helper.py /opt/spark/jobs/<job>.py`.

Python tooling (`pyproject.toml` at root, `uv` + `ruff` + `pytest`; deps in dependency-group `dev`,
no root `requirements.txt`):

```bash
uv pip install --python venv/bin/python --group dev
venv/bin/ruff format . && venv/bin/ruff check --fix .
venv/bin/python -m pytest -m "not spark"     # host tests (no JVM in WSL)
```

`@pytest.mark.spark` tests need a JVM — run them in `spark-master` (no pytest there and `tests/`
isn't mounted; pytest 8.3.x is the last line for the container's Python 3.8; `--user` fails):

```bash
docker exec spark-master python3 -m pip install -q --target /tmp/pylib 'pytest==8.3.5'
docker exec -u root spark-master rm -rf /tmp/tests && docker cp tests spark-master:/tmp/tests
docker exec -e PYTHONPATH=/tmp/pylib:/opt/spark/jobs:/opt/spark/python/lib/pyspark.zip:/opt/spark/python/lib/py4j-0.10.9.7-src.zip \
  spark-master python3 -m pytest -q -p no:cacheprovider /tmp/tests
```

Catalog check: `docker exec trino trino --execute "SHOW SCHEMAS FROM lakehouse"` →
`bronze gold information_schema silver`.

### Key ports (host)

MinIO 9000 / console 9001 · Polaris 8181 (unauthenticated `GET :8181/api/catalog/v1/config` → 401
means it's up) · Spark master UI 8090, RPC 7077, REST 6066 · Spark worker 8081 · Spark Thrift
10000, UI 4040 · Trino 8080 · Airflow 8085 · Grafana 3000 · Postgres 5432 · source-db 5433. **Not published**:
Loki 3100, Alloy 12345, Kafka 9092/29092, Zookeeper 2181, Elasticsearch 9200, Schema Registry 8081
— Kafka's `PLAINTEXT_HOST://localhost:9092` listener is unreachable from the host until a `ports:`
entry is added.

## Conventions

- **Airflow connections are created in code, not the UI.** `airflow/scripts/init.sh` (run by
  `airflow-init`) migrates the DB, ensures the admin user, and delete-then-adds `spark_rest_conn`,
  `minio_conn`, `postgres_conn`, `trino_conn` from `.env`. Add new connections there.
- **One-shot setup jobs** (`*-setup`) do idempotent initialisation (DBs, buckets, catalogs; later
  Debezium/sink connector registration). Never trust `curl -s` exit codes — it returns 0 on HTTP
  4xx/5xx; capture `%{http_code}` and fail loudly (see `req`/`die` in `polaris/scripts/init.sh`).
- **Permission fix-up pattern**: containers run as different UIDs (Spark 185, …). A busybox
  `spark-fix-permissions` job `chown`s `spark_events_vol` first. Follow it for any new non-root
  service writing to a named volume.
- **Healthcheck-gated startup**: use `condition: service_healthy` /
  `service_completed_successfully` in `depends_on`, so the chain doesn't race.
- Everything in the repo — code, comments, docstrings, logs, scripts, `.env` comments, docs — is in
  **English**. Only exception: `docs/roadmap.vi.md`, a Vietnamese copy of `docs/roadmap.md` the
  user asked for — update both together.
- Timezone `Asia/Ho_Chi_Minh` (`TZ`) everywhere.
- Each `compose/*.yml` re-declares the `lakehouse` network and its own `volumes:` so it stays
  independently mergeable.

## Editing notes

- New service → the matching `compose/*.yml` module (create one and add it to `include:` if
  needed), join `lakehouse`, read config from `.env`, gate startup with healthchecks.
- New engine on the catalog → `http://polaris:8181/api/catalog`, OAuth2 credential **and scope
  `PRINCIPAL_ROLE:ALL`**, warehouse `lakehouse`, plus the engine's own MinIO S3 settings. Don't
  introduce Nessie/Hive-metastore/JDBC-catalog config.
- **Polaris 1.5.0 quirks (docs disagree):** `admin-tool bootstrap` is not idempotent (2nd run →
  exit 3; `polaris-bootstrap` treats that as success); re-adding an existing grant → **HTTP 500
  duplicate key** (`init.sh` lists grants first); namespaces are Iceberg-REST resources, not
  Management-API ones. Derive request/field names from the spec at tag `apache-polaris-1.5.0`.
- Pin every new image to an explicit version (no `latest`), and check version compatibility
  (e.g. Debezium vs Kafka 3.6 in cp 7.6, Iceberg sink vs Polaris REST) before choosing.
