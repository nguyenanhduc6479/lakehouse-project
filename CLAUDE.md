# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A **demo CDC data platform** on Docker Compose, built to learn how the pieces fit together — not a
production system. Full design: **`docs/dataplatform-design.md`** (read it before building a new
phase); build order and per-phase checks: **`docs/roadmap.md`** (keep its Status column current). The target flow:

```
generator.py → source-db (Postgres, wal_level=logical)
  → Debezium (Kafka Connect, Avro + Schema Registry) → Kafka topics src.shop.*
  → Iceberg sink → lakehouse.bronze.*_cdc   (append-only change events, Polaris catalog, RustFS)
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
  infra: Postgres, Zookeeper + Kafka, Schema Registry, Elasticsearch, RustFS (S3), Polaris, Spark,
  Trino, Airflow, Grafana/Loki/Alloy. Roadmap phases 1 and 2 are **done**
  (2026-11-02): `source/schema.sql` (schema `shop`, 3 tables, triggers, publication
  `dbz_publication`, role `debezium`) is applied by the one-shot `source-db-setup` job (idempotent,
  CDC-ready), and `source/generator.py` fills it continuously. **Next: phase 3 (CDC — Kafka
  Connect + Debezium).**
- **Generator** (`source/generator.py` + `source/Dockerfile`, image `custom-generator:0.1.0`, Python 3.12, Faker 40.44, psycopg
  3.3): compose profile `generator`, so a plain `up` never starts it. Rate `GENERATOR_RATE` (`.env`).
  Pitfalls already fixed — keep them: the psycopg connection must be `autocommit=True` (otherwise
  the first statement opens an implicit transaction, every `conn.transaction()` becomes a savepoint
  and nothing commits until exit — invisible to Debezium); Faker 40.44 `vi_VN.first_name()` only
  returns "John"/"Jane", use `first_name_male()`/`first_name_female()`; Python as PID 1 ignores
  SIGTERM unless handled (it is). `faker` + `psycopg[binary]` are also in the host `dev` group for
  `tests/test_generator.py` — keep both pins in sync with `source/requirements.txt`.
- **Runtime-validated 2026-11-02 on native Docker Engine 29.9.2 in WSL** (from empty volumes,
  `build` + `up -d`): all services come up (16 running, all healthy except Loki which has no
  healthcheck; the one-shot jobs `Exited (0)` — 22 services incl. `source-db-setup`); every host port answers. A REST-submitted Spark job
  runs on a real executor and its driver/executor logs reach Loki. Verified: Trino and Spark (`helper.get_spark`) both create/read/drop
  an Iceberg table through Polaris into the `data-platform` bucket on RustFS (re-verified after the
  MinIO → RustFS swap; `rustfs-setup` is idempotent; Airflow `s3_conn` sees the bucket); Airflow connects to its DB as the
  `airflow` role; Kafka has transaction-log RF 1. Network verified (TCP to every service port +
  app-level checks from Airflow, Kafka, Schema Registry, Spark worker); recipe in `docs/services.md`.
- `spark/jobs/helper.py` (`get_spark`) builds a SparkSession wired to Polaris + RustFS (S3) — reuse it
  for every Spark job. `airflow/plugins/spark_rest.py` (`submit_and_wait`) submits a job via the
  Spark REST API. `airflow/dags/` is empty. `tests/` has only `conftest.py` (a local Spark fixture).
- `dbt/` holds only an empty `dbt_project.yml`; dbt-clickhouse (profile + models) arrives in the
  gold phase. `spark-thrift` and dbt-spark were removed (2026-11-02) — nothing uses Spark over JDBC.
- **Python versions differ by place:** the `custom-spark` image runs **Python 3.8.10** (code must
  stay 3.8-compatible at runtime — `from __future__ import annotations` is what makes `str | None`
  hints OK), Airflow is 3.11, the host `venv/` is 3.11.
- **All `container_name`s are fixed**, so only one copy of this stack can exist per Docker host.
  Volumes are `lakehouse-project_*`.
- **Under git.** One remote, `origin` → `git@github.com:nguyenanhduc6479/lakehouse-project.git`
  (SSH); `dev` is the working branch (tracks `origin/dev`), `main` the default. `.env` and
  `.claude/settings.local.json` are gitignored — keep it that way.

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
  in-network listener `kafka:29092`, topic auto-create on; offsets **and transaction-state** topics
  at RF 1 — the transaction log defaults to RF 3, which would break every transactional producer,
  e.g. the Iceberg sink's exactly-once commits). **Schema Registry** (cp 7.6.0,
  `http://schema-registry:8081`, internal only). **Elasticsearch 7.17** single node, security off
  (internal only) — the search layer.
- **Storage** (`compose/storage.yml`): **RustFS 1.0.4** (`rustfs/rustfs`, Apache 2.0, S3 API on
  9000, console on 9001 at `/rustfs/console/`). It replaced MinIO on 2026-11-02 because MinIO stopped
  publishing images. Config via `S3_*` in `.env` (`S3_ENDPOINT`, `S3_ACCESS_KEY`, `S3_SECRET_KEY`,
  `S3_BUCKET`, `S3_REGION`) — every engine reads these names, so swapping the store again only
  touches `storage.yml`. Path-style only (no `RUSTFS_SERVER_DOMAINS`); the region is ignored by
  RustFS but required by AWS SDK clients. `RUSTFS_OBS_LOG_DIRECTORY: ""` sends logs to stdout (the
  image default writes files under `/logs`, invisible to Alloy/Loki). Health: `GET :9000/health`.
  One-shot `rustfs-setup` (`amazon/aws-cli:2.39.3`) runs `rustfs/scripts/init.sh`: HEAD the bucket,
  create it only if missing. One bucket, `data-platform`; Iceberg tables land under
  `s3://data-platform/<namespace>/<table>`. Note: the bucket is `data-platform`, but the Polaris
  *catalog* (and Trino catalog / Spark `CATALOG`) is still named `lakehouse`.
- **Catalog** (`compose/catalog.yml`): **Apache Polaris 1.5.0** — Iceberg **REST** catalog at
  `http://polaris:8181/api/catalog`, **OAuth2 client-credentials** (`root` / `${POLARIS_SECRET}`,
  scope `PRINCIPAL_ROLE:ALL`), persisted in Postgres via
  **`POLARIS_PERSISTENCE_TYPE=relational-jdbc`** (without it Polaris silently runs in-memory and
  loses everything on restart). Chain: `postgres-setup` → `polaris-bootstrap` → `polaris` →
  `polaris-setup`. Health on management port **8182** (container-only). `polaris/scripts/init.sh`
  creates the `lakehouse` catalog (base location `s3://${S3_BUCKET}/`, RustFS endpoint,
  `pathStyleAccess`, `stsUnavailable=true`,
  `polaris.config.drop-with-purge.enabled=true`), grants `CATALOG_MANAGE_CONTENT` to
  `catalog_admin`, and creates namespaces `bronze`/`silver`/`gold` via the Iceberg REST API. No
  credential vending: every engine brings its own S3 keys.
- **Processing** (`compose/spark.yml`): Spark 3.5.1 from `spark/Dockerfile` (`custom-spark:3.5.1`:
  hadoop-aws, `iceberg-spark-runtime` + `iceberg-aws-bundle` 1.5.2, Postgres JDBC, kafka-clients).
  `spark-master` (RPC 7077, UI 8090, REST 6066), `spark-worker` (UI 8081, size from
  `SPARK_WORKER_CORES`/`SPARK_WORKER_MEMORY`). Jobs in `./spark/jobs` → `/opt/spark/jobs`, `PYTHONPATH=/opt/spark/jobs`. Event logs are
  **off** (nothing read them; no History Server). `spark-defaults.conf` sets
  **`spark.master=spark://spark-master:7077`** — without it every REST (6066) submit silently runs
  in `local[*]` inside the driver (the worker launches `SparkSubmit <app>` with no `--master` and
  SparkSubmit ignores the `-Dspark.master` the REST payload becomes; symptom: "Starting executor ID
  driver", no `app-*` dir). Verified 2026-11-02: a REST-submitted job now gets a real executor.
  Driver/executor stdout+stderr of cluster-mode jobs are files in the worker's `/opt/spark/work`
  (`driver-<id>/`, `app-<id>/<executor>/`), on volume `spark_work_vol`; the dir is created in the
  image owned by `spark`, so the fresh volume inherits it (no chown job). Worker cleanup deletes
  finished apps' dirs after 7 days (`SPARK_WORKER_OPTS`). **Config files are bind-mounted one file
  at a time**: after editing `spark-defaults.conf`, use `docker compose up -d --force-recreate
  spark-master spark-worker` — a plain `restart` fails ("error mounting ... no such file") because
  the editor replaced the file's inode.
- **Query** (`compose/trino.yml`): **Trino 454** (8080), Iceberg connector on Polaris
  (`trino/catalog/lakehouse.properties`) + native S3 to RustFS. 454 is the minimum: it is the first
  version with `iceberg.rest-catalog.oauth2.scope` (435 fails with `invalid_scope`).
- **Orchestration** (`compose/airflow.yml`): Airflow 2.9.2, LocalExecutor, `custom-airflow:2.9.2`
  from `airflow/Dockerfile` (+ `airflow/requirements.txt`, installed against the 2.9.2
  constraints). UI on **8085**. Its metadata DB is reached as the `airflow` role (owner of DB
  `airflow`), not the superuser; `AIRFLOW__WEBSERVER__SECRET_KEY` is shared by webserver and
  scheduler (otherwise the UI gets 403 reading task logs). **Spark jobs are submitted in cluster mode through the Spark
  Standalone REST API** (`spark-master:6066`), not a `spark://` connection.
- **Logs** (`compose/monitoring.yml`, config in `./monitoring`): **Grafana 13.2.3** (3000) reading
  **Loki 3.7.8** (internal, 7-day retention), fed by **Grafana Alloy v1.20.1**, which tails
  **Docker's json-file logs** (`/var/lib/docker/containers/*/*-json.log`, mounted read-only — no
  docker.sock) for `job="docker"` (labels `service`, `container` (= service), `stream`), filtered to
  compose project `lakehouse-project` via the compose labels Docker writes into each line (json-file
  option `labels`, set by `logging: *default-logging` on every service). File tailing catches even
  one-shot jobs that exit in <1 s (`postgres-setup` runs ~0.5 s) — Docker-API discovery lists running
  containers only and missed them. Verified 2026-11-02: Loki line counts equal `docker logs` for every
  container. Requires native Docker Engine (Desktop keeps those files in its VM). **Startup is
  gated on Alloy:** every root service (`postgres`, `source-db`, `zookeeper`, `elasticsearch`,
  `rustfs`, `spark-master`) `depends_on: alloy: service_healthy`, so everything starts after log
  shipping (Alloy's healthcheck also probes Loki; `start_interval: 2s` avoids a 30 s first-probe
  wait). Alloy also tails Spark job log files from `spark_work_vol` mounted read-only at `/spark-work`
  (`job="spark"`, `service="spark-job"`, labels `spark_id`, `role`=driver|executor, `executor`,
  `stream`; lines arrive within ~10 s). The "Container logs" dashboard queries `job=~"docker|spark"`.
  Loki is distroless, so `alloy`'s healthcheck probes both via bash `/dev/tcp` (`CMD bash -c`, not
  `CMD-SHELL`).

To be added by the roadmap: Kafka Connect (Debezium + Iceberg sink,
`confluentinc/cp-kafka-connect` which already has the AvroConverter), ClickHouse, Superset,
FastAPI.

## Common commands

All from the repo root (where `docker-compose.yml` and `.env` live — not from `compose/`),
**inside WSL**. Docker is the **native Docker Engine** in the Ubuntu WSL distro (docker-ce from
Docker's apt repo, started by systemd with WSL), **not Docker Desktop** — switched 2026-11-02;
Desktop's WSL integration is off and its `~/.docker` leftovers were moved to
`~/.docker-desktop-leftovers`. If a shell says `/mnt/c/Program Files/Docker/...: No such file`,
it cached the old Desktop binary: `hash -r` or open a new shell. Ports published by containers are
reachable from Windows on `localhost` (WSL2 forwarding), e.g. DataGrip on 5432/5433:

```bash
docker compose config            # validate/merge the included compose files — first sanity check
docker compose build             # build custom images (custom-spark, custom-airflow)
docker compose up -d --remove-orphans
docker compose up -d <service>   # one service and its dependencies
docker compose logs -f <service>
docker compose down              # named volumes persist
```

Docker Hub's CDN intermittently drops large layer downloads (`httpReadSeeker ... EOF`) — retry.
Don't run a blanket `docker compose pull`: it also tries the local-only `custom-*` images.

Generator (profile `generator`; `docker compose build` without the profile skips it):

```bash
docker compose --profile generator build generator
docker compose --profile generator up -d generator   # start writing into source-db
docker compose stop generator                        # stop (clean, ~1 s)
```

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

RustFS S3 9000 / console 9001 · Polaris 8181 (unauthenticated `GET :8181/api/catalog/v1/config` → 401
means it's up) · Spark master UI 8090, RPC 7077, REST 6066 · Spark worker 8081 · Trino 8080 · Airflow 8085 · Grafana 3000 · Postgres 5432 · source-db 5433. **Not published**:
Loki 3100, Alloy 12345, Kafka 9092/29092, Zookeeper 2181, Elasticsearch 9200, Schema Registry 8081
— Kafka's `PLAINTEXT_HOST://localhost:9092` listener is unreachable from the host until a `ports:`
entry is added.

## Conventions

- **Airflow connections are created in code, not the UI.** `airflow/scripts/init.sh` (run by
  `airflow-init`) migrates the DB, ensures the admin user, and delete-then-adds `spark_rest_conn`,
  `s3_conn` (RustFS), `postgres_conn` (→ `data-platform`), `trino_conn` (user `airflow`: Trino
  rejects requests without a user, HTTP 401) from `.env`. Add new connections there.
- **One-shot setup jobs** (`*-setup`) do idempotent initialisation (DBs, buckets, catalogs; later
  Debezium/sink connector registration). Never trust `curl -s` exit codes — it returns 0 on HTTP
  4xx/5xx; capture `%{http_code}` and fail loudly (see `req`/`die` in `polaris/scripts/init.sh`).
- **SQL setup scripts take secrets as psql variables**, never hardcoded: the job passes
  `.env` values with `psql -v ON_ERROR_STOP=1 -v name="$$ENV_VAR"` and the SQL reads `:'name'`.
  psql doesn't substitute variables inside `DO $$ … $$`, and `CREATE ROLE`/`CREATE PUBLICATION`
  have no `IF NOT EXISTS`, so create those idempotently with `SELECT format(...) WHERE NOT EXISTS
  (...) \gexec` (see `source/schema.sql`, run by `source-db-setup`). In tests, connect over the
  network (`-h source-db`), not `localhost` inside the container: the image trusts local logins,
  so a wrong password would still pass.
- **Volume ownership pattern**: containers run as different UIDs (Spark 185, RustFS 10001, …). A
  fresh named volume copies the owner of the mount-point directory *if that directory exists in
  the image* — so create it in the Dockerfile with the right owner (see `/opt/spark/work` in
  `spark/Dockerfile`) instead of adding a chown job. Only works for new/empty volumes.
- **Healthcheck-gated startup**: use `condition: service_healthy` /
  `service_completed_successfully` in `depends_on`, so the chain doesn't race.
- **Every new service** gets `logging: *default-logging` (each `compose/*.yml` defines the
  `x-logging` anchor; anchors don't cross `include`) — without it Alloy drops its logs — and must
  wait for `alloy` (directly if it has no other dependency, otherwise through its dependencies).
- Everything in the repo — code, comments, docstrings, logs, scripts, `.env` comments, docs — is in
  **English**. Only exception: `docs/roadmap.vi.md`, a Vietnamese copy of `docs/roadmap.md` the
  user asked for — update both together.
- Timezone: every long-running service gets `TZ: ${TZ}` (`.env`, `Asia/Ho_Chi_Minh`); for Postgres it
  also sets the server `timezone` at initdb. One-shot setup jobs don't need it.
- Each `compose/*.yml` re-declares the `lakehouse` network and its own `volumes:` so it stays
  independently mergeable.

## Editing notes

- New service → the matching `compose/*.yml` module (create one and add it to `include:` if
  needed), join `lakehouse`, read config from `.env`, gate startup with healthchecks.
- New engine on the catalog → `http://polaris:8181/api/catalog`, OAuth2 credential **and scope
  `PRINCIPAL_ROLE:ALL`**, warehouse `lakehouse`, plus the engine's own S3 settings (`S3_*`, path-style). Don't
  introduce Nessie/Hive-metastore/JDBC-catalog config.
- **Polaris 1.5.0 quirks (docs disagree):** `admin-tool bootstrap` is not idempotent (2nd run →
  exit 3; `polaris-bootstrap` treats that as success); re-adding an existing grant → **HTTP 500
  duplicate key** (`init.sh` lists grants first); namespaces are Iceberg-REST resources, not
  Management-API ones. Derive request/field names from the spec at tag `apache-polaris-1.5.0`.
- Pin every new image to an explicit version (no `latest`), and check version compatibility
  (e.g. Debezium vs Kafka 3.6 in cp 7.6, Iceberg sink vs Polaris REST) before choosing.
