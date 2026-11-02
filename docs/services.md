# Services, ports and links

Every service of the current stack, its ports, and how to reach it from the host. Ports are
defined in `compose/*.yml`; credentials live in `.env` (variable names below, never the values).

Inside the Docker network every service is reached by its service name (e.g. `kafka:29092`,
`http://polaris:8181`), not `localhost`. From Windows (DataGrip, browser) the host ports below are
reachable on `localhost` too: Docker Engine runs inside WSL and WSL2 forwards published ports.

The stack has 21 services: 16 long-running ones and 5 one-shot jobs (see the end of this page).

## Services with a UI

| Service | Host port(s) | Link | Login |
|---|---|---|---|
| Airflow webserver | 8085 → 8080 | http://localhost:8085 | `AIRFLOW_ADMIN_USER` / `AIRFLOW_ADMIN_PASSWORD` |
| Grafana | 3000 | http://localhost:3000 | `GRAFANA_ADMIN_USER` / `GRAFANA_ADMIN_PASSWORD` |
| RustFS console | 9001 | http://localhost:9001/rustfs/console/ | `S3_ACCESS_KEY` / `S3_SECRET_KEY` |
| Spark master UI | 8090 → 8080 | http://localhost:8090 | — |
| Spark worker UI | 8081 | http://localhost:8081 | — (links to each driver/executor's stdout/stderr; the same logs are in Grafana under `service=spark-job`) |
| Trino web UI | 8080 | http://localhost:8080/ui/ | any user name, no password |

## Services without a UI — exposed to the host

Backend services: no web UI, reached with a client (`psql`, S3 client, HTTP API, JDBC).

| Service | Host port(s) | How to connect | Notes |
|---|---|---|---|
| `postgres` (shared) | 5432 | `psql -h localhost -p 5432 -U $POSTGRES_USER data-platform` | Also holds the `airflow` and `polaris` DBs |
| `source-db` (CDC source) | 5433 → 5432 | `psql -h localhost -p 5433 -U $SOURCE_POSTGRES_USER source-db` | `wal_level=logical` |
| RustFS S3 API | 9000 | `http://localhost:9000` (S3 endpoint, path-style) | Same keys as the console; one bucket, `S3_BUCKET` (`data-platform`); region `S3_REGION` |
| Polaris (Iceberg REST catalog) | 8181 | `http://localhost:8181/api/catalog` | OAuth2 `root` / `POLARIS_SECRET`, scope `PRINCIPAL_ROLE:ALL`; an unauthenticated `GET /api/catalog/v1/config` → 401 means it is up |
| Spark master RPC | 7077 | `spark://localhost:7077` | |
| Spark master REST | 6066 | `http://localhost:6066` | Cluster-mode submit (used by Airflow) |
| Trino (client) | 8080 | `docker exec trino trino` | Same port as the web UI |

## Services without a UI — internal only

Backend services with no web UI and no published port; reachable only from inside the
`lakehouse` network (or with `docker exec`).

| Service | Internal address | Role |
|---|---|---|
| Kafka | `kafka:29092` | Event broker. The `PLAINTEXT_HOST://localhost:9092` listener exists but has no `ports:` entry, so it is unreachable from the host |
| Zookeeper | `zookeeper:2181` | Kafka coordination |
| Schema Registry | `http://schema-registry:8081` | Avro schemas |
| Elasticsearch 7.17 | `http://elasticsearch:9200` | Search index, security off |
| Loki | `http://loki:3100` | Log store, read through Grafana |
| Alloy | `alloy:12345` | Log collector (tails Docker json-file logs + Spark job logs); every other service waits for it to be healthy; its debug UI exists but the port is not published |
| Polaris management | `polaris:8182` | Health/metrics only |

## Airflow connections

Created by `airflow/scripts/init.sh` (run by `airflow-init`) from `.env`, never in the UI.

| Connection | Type | Points at | Notes |
|---|---|---|---|
| `spark_rest_conn` | HTTP | `spark-master:6066` | Cluster-mode submit via `airflow/plugins/spark_rest.py` |
| `s3_conn` | AWS | `http://rustfs:9000` | `S3_ACCESS_KEY` / `S3_SECRET_KEY`, region `S3_REGION` |
| `postgres_conn` | Postgres | `postgres:5432/data-platform` | Silver/staging database |
| `trino_conn` | Trino | `trino:8080`, catalog `lakehouse` | User `airflow`, no password (Trino rejects requests without a user) |

## Network

Every service joins one bridge network, `lakehouse` (Docker name `lakehouse-project_lakehouse`),
and resolves the others by service name. Verified 2026-11-02 at TCP level for every port in the
tables above and at application level (Airflow connections, Polaris OAuth2 token, Kafka
produce/consume, Schema Registry write, Spark worker → Postgres/Kafka/Polaris/RustFS). Quick
re-check from a throwaway container:

```bash
docker run --rm --network lakehouse-project_lakehouse alpine:3.19 sh -c \
  'for t in postgres:5432 source-db:5432 kafka:29092 schema-registry:8081 elasticsearch:9200 \
   rustfs:9000 polaris:8181 trino:8080 spark-master:6066 loki:3100; do
     nc -z -w 3 ${t%:*} ${t#*:} && echo "ok   $t" || echo "FAIL $t"; done'
```

## One-shot jobs

Run once at startup and exit; they have no port and no UI: `postgres-setup`, `rustfs-setup`,
`polaris-bootstrap`, `polaris-setup`, `airflow-init`.
`airflow-scheduler` is a long-running backend process, also without a UI or port.

## Not built yet

Planned by the [roadmap](roadmap.md), ports not assigned yet: generator, Kafka Connect
(Debezium + Iceberg sink), ClickHouse, Superset, FastAPI + web page. Add them here when they land.
