# Services, ports and links

Every service of the current stack, its ports, and how to reach it from the host. Ports are
defined in `compose/*.yml`; credentials live in `.env` (variable names below, never the values).

Inside the Docker network every service is reached by its service name (e.g. `kafka:29092`,
`http://polaris:8181`), not `localhost`. From Windows (DataGrip, browser) the host ports below are
reachable on `localhost` too: Docker Engine runs inside WSL and WSL2 forwards published ports.

The stack has 25 services: 18 long-running ones and 7 one-shot jobs (see the end of this page).

## Services with a UI

| Service | Host port(s) | Link | Login |
|---|---|---|---|
| Airflow webserver | 8085 → 8080 | http://localhost:8085 | `AIRFLOW_ADMIN_USER` / `AIRFLOW_ADMIN_PASSWORD` |
| Grafana | 3000 | http://localhost:3000 | `GRAFANA_ADMIN_USER` / `GRAFANA_ADMIN_PASSWORD` |
| Kafka UI | 8082 → 8080 | http://localhost:8082 | none (read-only: browse topics, Avro-decoded messages, Schema Registry, Kafka Connect) |
| RustFS console | 9001 | http://localhost:9001/rustfs/console/ | `S3_ACCESS_KEY` / `S3_SECRET_KEY` |
| Spark master UI | 8090 → 8080 | http://localhost:8090 | — |
| Spark worker UI | 8081 | http://localhost:8081 | — (links to each driver/executor's stdout/stderr; the same logs are in Grafana under `service=spark-job`) |
| Trino web UI | 8080 | http://localhost:8080/ui/ | any user name, no password |

## Services without a UI — exposed to the host

Backend services: no web UI, reached with a client (`psql`, S3 client, HTTP API, JDBC).

| Service | Host port(s) | How to connect | Notes |
|---|---|---|---|
| `postgres` (shared) | 5432 | `psql -h localhost -p 5432 -U $POSTGRES_USER data-platform` | Also holds the `airflow` and `polaris` DBs |
| `source-db` (CDC source) | 5433 → 5432 | `psql -h localhost -p 5433 -U $SOURCE_POSTGRES_USER source-db` | `wal_level=logical`; schema `shop` + publication `dbz_publication`; Debezium logs in as `debezium` / `SOURCE_POSTGRES_DB_DEBEZIUM_PASSWORD` |
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
| Schema Registry | `http://schema-registry:8081` | Avro schemas (subjects `src.shop.<table>-key/-value`, compatibility `BACKWARD`) |
| Kafka Connect | `http://kafka-connect:8083` | Runs the Debezium connector `source-shop` (source-db → topics `src.shop.*`); REST API for connector status. No host port: use `docker exec kafka-connect curl -s ...` |
| Elasticsearch 7.17 | `http://elasticsearch:9200` | Search index, security off |
| Loki | `http://loki:3100` | Log store, read through Grafana |
| Alloy | `alloy:12345` | Log collector (tails Docker json-file logs + Spark job logs); every other service waits for it to be healthy; its debug UI exists but the port is not published |
| Polaris management | `polaris:8182` | Health/metrics only |

## Airflow connections

Created by `airflow/scripts/init.sh` (run by `airflow-init`) from `.env`, never in the UI.

| Connection | Type | Points at | Notes |
|---|---|---|---|
| `spark_rest_conn` | HTTP | `spark-master:6066` | Cluster-mode submit via `airflow/dags/lib/spark_rest.py` |
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

Run once at startup and exit; they have no port and no UI: `postgres-setup`, `source-db-setup`
(applies `source/schema.sql`), `rustfs-setup`,
`polaris-bootstrap`, `polaris-setup`, `airflow-init`, `connect-setup` (registers
`connect/connectors/*.json` with Kafka Connect and waits until they are `RUNNING`).
`airflow-scheduler` is a long-running backend process, also without a UI or port.

## On demand (compose profile `generator`)

Not started by `docker compose up`; not counted in the 25 services above.

| Service | Role | Start / stop |
|---|---|---|
| `generator` | Writes shop activity into `source-db` (`GENERATOR_RATE` events/s, default 2) | `docker compose --profile generator up -d generator` / `docker compose stop generator` |

### Running the generator

The generator is one Python file, `source/generator.py`. Run it either way — the logic is the
same; don't run both at once (the write rates add up).

**In a container** (runs in the background, logs in Grafana under `service=generator`):

```bash
docker compose --profile generator build generator   # first time, or after editing source/generator.py
docker compose --profile generator up -d generator   # start
docker compose stop generator                        # stop (clean, ~1 s)
```

**Directly with Python** from the repo root in WSL (quick tests, debugging; logs only in the
terminal, not in Grafana — Alloy only collects container logs):

```bash
uv pip install --python venv/bin/python --group dev  # once: faker + psycopg are in the dev group

set -a && . ./.env && set +a                         # load .env into this shell
export PGHOST=localhost PGPORT=5433 \
       PGUSER="$SOURCE_POSTGRES_USER" PGPASSWORD="$SOURCE_POSTGRES_PASSWORD" \
       PGDATABASE="$SOURCE_POSTGRES_DB"              # source-db as seen from the host

venv/bin/python source/generator.py                  # 2 events/s until Ctrl+C (clean stop)
```

The `export`s only last for that terminal. Options (same for both ways; in the container they go
after the service name, e.g. `docker compose --profile generator run --rm generator --rate 10`):

| Option | Meaning | Default |
|---|---|---|
| `--rate` | Events per second | 2 |
| `--max-events` | Stop after N events (0 = run forever) | 0 |
| `--seed-users` | Create users until at least this many exist | 20 |
| `--seed` | Random seed, for a reproducible sequence of actions | random |
| `--log-level` | `DEBUG`, `INFO`, `WARNING` | `INFO` |

## Checking CDC

**In the browser — Kafka UI** (http://localhost:8082, cluster `lakehouse`):

- *Topics* → `src.shop.orders` → *Messages*: every change event, decoded from Avro (the
  `SchemaRegistry` serde is preselected for `src.*` topics). Expand one to see `before` / `after` /
  `source.lsn` / `source.txId` / `op`; set *Seek type* to *Newest* and hit refresh after a change.
- *Schema Registry*: the subjects Debezium registered, their versions and compatibility.
- *Kafka Connect* → `source-shop`: connector and task state, its config (the password stays
  `${env:...}`).
- *Consumers*: consumer groups and their lag. The Iceberg sink shows up as
  `connect-iceberg-bronze` (data topics; lag drops to 0 at each commit, ~every minute) plus
  `connect-iceberg-bronze-coord` and `cg-control-*` on its control topic `control-iceberg`.

It is read-only (`KAFKA_CLUSTERS_0_READONLY`): no producing, deleting topics or editing
connectors from the UI — those stay in code.

**From the terminal** — Kafka and Kafka Connect have no host port, so run the clients inside the
containers:

```bash
# connector + task state (both should be RUNNING)
docker exec kafka-connect curl -s http://localhost:8083/connectors/source-shop/status

# Avro subjects registered by Debezium
docker exec kafka-connect curl -s http://schema-registry:8081/subjects

# read change events as JSON, exits after 10 s without new messages (--timeout-ms, so `exec` is
# fine here); the tool also prints its own logs to stdout: keep lines starting with {
docker exec schema-registry kafka-avro-console-consumer --bootstrap-server kafka:29092 \
  --topic src.shop.orders --from-beginning --property schema.registry.url=http://localhost:8081 \
  --timeout-ms 10000 2>/dev/null | grep '^{'

# replication slot: active, and how much WAL it still holds
docker exec source-db sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "SELECT slot_name, active,
  pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)) AS lag FROM pg_replication_slots"'
```

**Watch CDC live** — one readable line per event (operation, table, key, transaction id, changed
columns), from the repo root:

```bash
docker run --rm --network lakehouse-project_lakehouse confluentinc/cp-schema-registry:7.6.0 \
  kafka-avro-console-consumer --bootstrap-server kafka:29092 --include 'src\.shop\..*' \
  --property schema.registry.url=http://schema-registry:8081 \
  2>/dev/null | python3 -u connect/scripts/watch_events.py      # Ctrl+C stops it
```

Use a throwaway `docker run --rm` container, not `docker exec`: Ctrl+C (or `timeout`) on a
`docker exec` only kills the client on the host — the consumer keeps running inside the container
and stays listed as a live group under *Consumers* in Kafka UI. `docker run` forwards the signal,
so the container stops and is removed.

Leave it running, then change a row in DataGrip (`UPDATE shop.users SET city = 'Huế' WHERE
user_id = 1;`) or start the generator: each change shows up within about a second, e.g.

```
UPDATE   users          user_id=1      tx=1389  city: Huế -> Đà Lạt, updated_at: ... -> ...
INSERT   orders         order_id=216    tx=1401
INSERT   order_details  order_detail_id=640    tx=1401
UPDATE   orders         order_id=216    tx=1401  total_amount: 0.00 -> 2,742,000.00
```

Without `--from-beginning` it shows only new events; add it to replay the snapshot too.

Avro decimals (`total_amount`, prices) show up as raw bytes in the raw console consumer; the value is
the big-endian integer divided by 10^scale (scale 2).

Re-register connectors after editing `connect/connectors/*.json`:
`docker compose up -d --force-recreate connect-setup`.

## Checking bronze

The Iceberg sink `iceberg-bronze` commits about once a minute; each commit is one Iceberg snapshot
per table. From Trino (`docker exec -it trino trino --catalog lakehouse --schema bronze`, or
DataGrip on `localhost:8080`, user any name, no password, catalog `lakehouse`). Long results open
in `less`: `q` leaves it, ← → scroll wide tables, `quit;` leaves the CLI; add `--pager=''` to print
straight to the terminal. Columns `before` / `after` / `source` are ROW types: filter with
`coalesce(after.user_id, before.user_id) = 1` (`after` is NULL on deletes, `before` on inserts).

```sql
SHOW TABLES FROM lakehouse.bronze;    -- users_cdc, orders_cdc, order_details_cdc

-- latest change events
SELECT op, after.order_id, after.status, after.total_amount, source.lsn, source.txid, _ingested_at
FROM lakehouse.bronze.orders_cdc ORDER BY source.lsn DESC LIMIT 20;

-- one row per commit
SELECT committed_at, operation, summary['added-records'] AS added, summary['total-records'] AS total
FROM lakehouse.bronze."orders_cdc$snapshots" ORDER BY committed_at;

-- events of one Vietnam-time day: partitions are UTC days (00:00-07:00 VN = previous UTC day),
-- so filter on _ingested_at with a zoned literal, not on the partition date
SELECT count(*) FROM lakehouse.bronze.orders_cdc
WHERE _ingested_at >= TIMESTAMP '2026-11-03 00:00 Asia/Ho_Chi_Minh'
  AND _ingested_at <  TIMESTAMP '2026-11-04 00:00 Asia/Ho_Chi_Minh';

-- data files on RustFS (one small Parquet file per table per commit)
SELECT file_path, record_count, file_size_in_bytes FROM lakehouse.bronze."orders_cdc$files";
```

Sink state: `docker exec kafka-connect curl -s localhost:8083/connectors/iceberg-bronze/status`.
Commits in the log (Grafana, service `kafka-connect`, search `Commit`): `Commit <id> complete,
committed to 3 table(s)` while data flows; `Commit timeout reached ... committed to 0 table(s)` when
the topics are idle is normal. Bronze is complete when its row count equals the topic's end offset
(Kafka UI → topic → *Number of messages*). The files themselves are browsable in the RustFS console
under `data-platform/bronze/`.

## Checking silver

Silver is filled by DAG **`platform_pipeline`** (Airflow, http://localhost:8085 → trigger; or
`docker exec airflow-scheduler airflow dags trigger platform_pipeline`). `silver_spark` stages new
bronze events, `silver_apply` applies them; its log has one line per table, e.g.
`silver.orders: snapshots (6610…, 4873…], 129 staged, 28 versions closed, 129 inserted`, or
`batch already applied ..., skipped` when nothing new arrived. The Spark side is in Grafana
(`service="spark-job"`).

Connect DataGrip (or `psql`) to `localhost:5432`, DB `data-platform`, user `POSTGRES_USER`:

```sql
-- where each table's next run starts (NULL = full read), and what the last Spark run staged
SELECT table_name, last_snapshot_id, updated_at FROM config.etl_checkpoint WHERE pipeline = 'silver';
SELECT * FROM staging.batch;

-- history of one order
SELECT order_id, status, total_amount, _valid_from, _valid_to, _is_current, _is_deleted, _lsn
FROM silver.orders WHERE order_id = 45 ORDER BY _lsn;

-- today's state (what the web app reads)
SELECT * FROM silver.orders WHERE _is_current AND NOT _is_deleted;

-- the state at a point in time
SELECT * FROM silver.orders
WHERE _valid_from <= TIMESTAMPTZ '2026-11-04 09:00+07'
  AND (_valid_to IS NULL OR _valid_to > TIMESTAMPTZ '2026-11-04 09:00+07')
  AND NOT _is_deleted;
```

Consistency checks (stop the generator, wait ~1 min for the sink's last commit, run the DAG):

| Check | Must equal |
|---|---|
| `count(*) WHERE _is_current AND NOT _is_deleted` | `count(*)` of the table in `source-db` |
| `count(*) WHERE _is_current` | distinct keys in bronze |
| `count(*)` | distinct (key, `source.txid`) in bronze (Trino, below) |
| `count(*) WHERE _valid_to = _valid_from` | 0 (a zero-length version = a transaction split across runs not merged) |

```sql
-- Trino: expected silver row count for orders
SELECT count(*) FROM (SELECT DISTINCT coalesce(after.order_id, before.order_id), source.txid
                      FROM lakehouse.bronze.orders_cdc) t;
```

Silver has fewer rows than bronze on purpose: Debezium duplicates are dropped, and an insert +
update of one row in one transaction is a single version.

**Rebuild a table from bronze** (e.g. after changing the transform): in one transaction
`TRUNCATE silver.orders;` and `UPDATE config.etl_checkpoint SET last_snapshot_id = NULL WHERE
pipeline = 'silver' AND table_name = 'silver.orders';`, then trigger the DAG. Resetting only the
checkpoint does nothing — every event is skipped by `_lsn`. Don't do it while a run is active.

## Polaris tokens: expiry and reset

Trino and the Iceberg sink talk to Polaris with an OAuth2 token. Polaris signs tokens with a fixed
HMAC key from `.env` (`POLARIS_TOKEN_SIGNING_KEY`, `token-broker.type=symmetric-key`) and issues
them for 30 days (`max-token-generation=P30D`), both in `compose/catalog.yml`. So:

- **Restarting Polaris changes nothing** for clients — tokens issued before stay valid (checked
  2026-11-03: Polaris restarted, Trino kept answering, the sink kept committing, 0 errors).
- A token stops working only when it **expires** — about 60 days after the client started (these
  clients refresh once: 30 days + 30) — or when `POLARIS_TOKEN_SIGNING_KEY` is changed.

Spark jobs and Debezium are never affected (a Spark job fetches a new token each run; Debezium
doesn't use Polaris).

**Symptoms**: Trino answers `Not authorized` / `Failed to load table`; the sink **task** goes
`FAILED` (`NotAuthorizedException`) at its next commit with data while the **connector** still says
`RUNNING`, and the lag of `connect-iceberg-bronze` keeps growing (Kafka UI → Consumers).

**Reset** — give both clients a new token:

```bash
docker exec kafka-connect curl -s -o /dev/null -w "%{http_code}\n" -X POST \
  "http://localhost:8083/connectors/iceberg-bronze/restart?includeTasks=true"   # 202; Debezium untouched
docker compose restart trino
```

**Check**: `docker exec kafka-connect curl -s localhost:8083/connectors/iceberg-bronze/status`
(connector + task `RUNNING`), a Trino query answers, and the consumer-group lag is back to 0
within about a minute. No data is lost: the sink moves its Kafka offsets only with a successful
Iceberg commit and re-reads from there (tested: bronze rows = Kafka messages, no duplicate
`_offset`) — as long as it happens within Kafka's 7-day retention.

## Adding a source table

Step-by-step commands (source table → connectors → snapshot of existing rows via the Debezium
signal table → bronze), and how to remove a table again: [add-source-table.md](add-source-table.md).

## Not built yet

Planned by the [roadmap](roadmap.md), ports not assigned yet: ClickHouse, Superset, FastAPI + web
page. Add them here when they land.
