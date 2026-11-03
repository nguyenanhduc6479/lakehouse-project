# Adding (and removing) a source table

Runbook for bringing a new table of `source-db` into the CDC pipeline, down to bronze, and for
taking one out again. Every command was run end to end on 2026-11-03 with a scratch table
`shop.test_products` (5 existing rows, then insert/update/delete), which was removed afterwards
with the removal steps below. Run everything from the repo root in WSL.

Example table name used below: `products`. Set it once per shell:

```bash
T=products                      # source table in schema shop -> topic src.shop.$T -> bronze.${T}_cdc
```

What a table touches:

| Layer | Where | What |
|---|---|---|
| Source | `source/schema.sql` | table, `updated_at` trigger, `REPLICA IDENTITY FULL`, publication, grant |
| CDC | `connect/connectors/source-shop.json` | `table.include.list` |
| Bronze | `connect/connectors/iceberg-bronze.json` | `topics`, `iceberg.tables`, one `route-regex` |
| Silver / gold | phase 5 / 6 | not built yet — add the table there when those exist |

Prerequisites (one-time, already in place since 2026-11-03): the signal table
`public.debezium_signal` (in `source/schema.sql`, in the publication) and
`"signal.data.collection": "public.debezium_signal"` in `source-shop.json`, plus a `Filter` SMT
that keeps signal-table events out of Kafka. Check:

```bash
docker exec source-db sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -Atc "SELECT tablename FROM pg_publication_tables WHERE pubname = '\''dbz_publication'\''"'
# -> users, orders, order_details, debezium_heartbeat, debezium_signal
```

---

## Adding a table

### 1. Source: declare the table in `source/schema.sql`

Append a block like this (keep it idempotent — the file runs on every `up`):

```sql
-- ---------------------------------------------------------
-- products
-- ---------------------------------------------------------
CREATE TABLE IF NOT EXISTS shop.products (
    product_id  bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    sku         varchar(50)   NOT NULL UNIQUE,
    name        varchar(200)  NOT NULL,
    price       numeric(18,2) NOT NULL,
    created_at  timestamptz   NOT NULL DEFAULT now(),
    updated_at  timestamptz   NOT NULL DEFAULT now()
);

CREATE OR REPLACE TRIGGER trg_products_updated_at
BEFORE UPDATE ON shop.products
FOR EACH ROW EXECUTE FUNCTION shop.set_updated_at();

-- full old row in UPDATE/DELETE events
ALTER TABLE shop.products REPLICA IDENTITY FULL;

-- into the publication (no ADD TABLE IF NOT EXISTS -> \gexec)
SELECT 'ALTER PUBLICATION dbz_publication ADD TABLE shop.products'
WHERE NOT EXISTS (SELECT 1 FROM pg_publication_tables
                  WHERE pubname = 'dbz_publication'
                    AND schemaname = 'shop' AND tablename = 'products')
\gexec

-- already covered by ALTER DEFAULT PRIVILEGES for new tables; explicit for tables that existed
GRANT SELECT ON shop.products TO debezium;
```

Rules for the table: a primary key (Debezium's Kafka key and the snapshot's chunking key), money
as `numeric(p,s)`, times as `timestamptz`, no generated columns (not sent by logical replication
before PostgreSQL 18).

Apply and check:

```bash
docker compose up -d --force-recreate source-db-setup && docker wait source-db-setup   # prints 0
docker logs source-db-setup 2>&1 | tail -1                                           # source-db schema applied

docker exec source-db sh -c "psql -U \"\$POSTGRES_USER\" -d \"\$POSTGRES_DB\" -Atc \"
  SELECT tablename FROM pg_publication_tables WHERE pubname = 'dbz_publication' AND tablename = '$T';
  SELECT relreplident FROM pg_class WHERE oid = 'shop.$T'::regclass\""
# -> products / f   (f = REPLICA IDENTITY FULL)
```

### 2. CDC + bronze: add the table to both connectors

`connect/connectors/source-shop.json` — append to the include list:

```json
"table.include.list": "shop.users,shop.orders,shop.order_details,shop.products",
```

`connect/connectors/iceberg-bronze.json` — three places:

```json
"topics": "src.shop.users,src.shop.orders,src.shop.order_details,src.shop.products",
"iceberg.tables": "bronze.users_cdc,bronze.orders_cdc,bronze.order_details_cdc,bronze.products_cdc",
"iceberg.table.bronze.products_cdc.route-regex": "^src\\.shop\\.products$",
```

Apply (re-registers both connectors; each restarts its task, Debezium keeps its offset):

```bash
for f in connect/connectors/*.json; do python3 -m json.tool "$f" >/dev/null || echo "INVALID $f"; done
docker compose up -d --force-recreate connect-setup && docker wait connect-setup && docker logs connect-setup
# -> Connector iceberg-bronze updated ... RUNNING / Connector source-shop updated ... RUNNING
```

From now on every change to `shop.$T` reaches Kafka. **Rows that already existed do not** —
Debezium snapshotted only once, when the connector was first created. The topic may already
exist at this point but stay empty: the sink subscribing to it makes Kafka create it.

### 3. Snapshot the existing rows (signal)

Ask Debezium for an incremental snapshot of the new table only. It runs while streaming goes on;
the other tables are not re-read. Existing rows arrive as `op = r` events.

```bash
docker exec source-db sh -c "psql -U \"\$POSTGRES_USER\" -d \"\$POSTGRES_DB\" -c \"
  INSERT INTO public.debezium_signal (id, type, data)
  VALUES ('snapshot-$T-' || to_char(now(), 'YYYYMMDDHH24MISS'), 'execute-snapshot',
          '{\\\"data-collections\\\": [\\\"shop.$T\\\"], \\\"type\\\": \\\"incremental\\\"}')\""
```

Check: the topic holds at least as many messages as the table has rows (more if rows changed
meanwhile), and Debezium logged the snapshot:

```bash
docker exec source-db sh -c "psql -U \"\$POSTGRES_USER\" -d \"\$POSTGRES_DB\" -Atc 'SELECT count(*) FROM shop.$T'"
docker exec kafka kafka-run-class kafka.tools.GetOffsetShell --bootstrap-server kafka:29092 \
  --topic src.shop.$T 2>/dev/null | awk -F: '{s+=$3} END{print s+0}'
docker logs --since 5m kafka-connect 2>&1 | grep -i "incremental snapshot"
# -> Requested 'INCREMENTAL' snapshot of data collections '[shop.products]' ...
#    ... incremental snapshotting of table 'shop.products' finished
```

Kafka UI (http://localhost:8082) → Topics → `src.shop.products` → Messages shows them decoded.
The signal and the snapshot's window markers stay as rows in `public.debezium_signal`; they can be
deleted once the snapshot finished (see "Removing a table", step 6).

### 4. Bronze

The sink creates `lakehouse.bronze.${T}_cdc` at its first record (same layout as the other
bronze tables: `before` / `after` / `source` rows, `_topic` / `_partition` / `_offset` /
`_ingested_at`, `day(_ingested_at)`, format v2) and commits about once a minute. Check — the
table is complete when its row count equals the topic's message count:

```bash
docker exec trino trino --execute "SELECT count(*) FROM lakehouse.bronze.${T}_cdc"
docker exec trino trino --output-format ALIGNED --execute "
  SELECT op, coalesce(after.product_id, before.product_id) AS id, after.name, source.lsn
  FROM lakehouse.bronze.${T}_cdc ORDER BY _offset"
```

Right after the `CREATE`, Trino may answer `Table ... does not exist` until the first commit
(≤ ~1 min); `Commit <id> complete, committed to 1 table(s)` in the `kafka-connect` log marks it.

### 5. Downstream

- **Silver** (phase 5) and **gold** (phase 6): add the table there once those layers exist.
- **Generator**: only if the demo should write the table continuously (`source/generator.py`).
- Keep the docs current: the source tables in `docs/dataplatform-design.md` §3.1 and the lists in
  `CLAUDE.md`.

---

## Removing a table

Order matters: first stop reading it, then delete what was derived from it, the source last.

```bash
T=products
set -a && . ./.env && set +a       # for the S3 cleanup in step 4
```

1. **Connectors** — remove the table from `table.include.list` (`source-shop.json`) and from
   `topics`, `iceberg.tables` and its `route-regex` line (`iceberg-bronze.json`), then:

   ```bash
   docker compose up -d --force-recreate connect-setup && docker wait connect-setup && docker logs connect-setup
   ```

2. **Bronze table** — `drop-with-purge` is enabled on the catalog, so its committed files go too:

   ```bash
   docker exec trino trino --execute "DROP TABLE lakehouse.bronze.${T}_cdc"
   ```

3. **Kafka topic and Avro schemas** (soft delete, then permanent):

   ```bash
   docker exec kafka kafka-topics --bootstrap-server kafka:29092 --delete --topic src.shop.$T
   for v in src.shop.$T-key src.shop.$T-value; do
     docker exec kafka-connect curl -s -o /dev/null -w "$v soft %{http_code}\n" -X DELETE http://schema-registry:8081/subjects/$v
     docker exec kafka-connect curl -s -o /dev/null -w "$v hard %{http_code}\n" -X DELETE "http://schema-registry:8081/subjects/$v?permanent=true"
   done
   ```

4. **Leftover files on RustFS** — a data file the sink wrote but never committed (e.g. Connect
   restarted between write and commit) is in no snapshot, so `DROP TABLE` doesn't see it. This
   happened in the test run. Remove the table's prefix:

   ```bash
   docker run --rm --network lakehouse-project_lakehouse \
     -e AWS_ACCESS_KEY_ID="$S3_ACCESS_KEY" -e AWS_SECRET_ACCESS_KEY="$S3_SECRET_KEY" -e AWS_DEFAULT_REGION="$S3_REGION" \
     amazon/aws-cli:2.39.3 --endpoint-url "$S3_ENDPOINT" s3 rm --recursive "s3://$S3_BUCKET/bronze/${T}_cdc/"
   ```

5. **Source** — out of the publication, drop the table, and delete its block from
   `source/schema.sql` (otherwise the next `up` recreates it):

   ```bash
   docker exec source-db sh -c "psql -U \"\$POSTGRES_USER\" -d \"\$POSTGRES_DB\" \
     -c 'ALTER PUBLICATION dbz_publication DROP TABLE shop.$T' -c 'DROP TABLE shop.$T'"
   ```

6. **Signal rows** (optional) — once no snapshot is running, processed signals can go:

   ```bash
   docker exec source-db sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "DELETE FROM public.debezium_signal"'
   ```

Check that nothing is left:

```bash
docker exec kafka kafka-topics --bootstrap-server kafka:29092 --list | grep "$T"           # nothing
docker exec kafka-connect curl -s http://schema-registry:8081/subjects                      # no src.shop.$T-*
docker exec trino trino --execute "SHOW TABLES FROM lakehouse.bronze"                      # no ${T}_cdc
```

The sink's consumer group keeps a committed offset for the deleted topic; it is harmless and
expires with the group's other stale offsets.

---

## Pitfalls seen while testing (2026-11-03)

- **Existing rows need the signal** (step 3). Without it the new table only has changes made after
  step 2 — silver would be missing every row that never changes again.
- **Signal events in Kafka**: Debezium always captures the signal table, even outside
  `table.include.list`, and would create topic `src.public.debezium_signal`. The `Filter` SMT
  (`dropSignals` in `source-shop.json`) drops those records.
- **Bronze lags by up to a commit interval** (~1 min): count rows against the topic, not
  immediately after the change.
- **Types in bronze**: `numeric(18,2)` becomes `decimal(38,2)`; `timestamptz` columns are ISO-8601
  strings inside `before` / `after` (design §4.3).
- Two platform bugs surfaced by this test and fixed the same day (details in `docs/roadmap.md`,
  phase 4): Polaris OAuth2 tokens expired after ~2 h because the clients refresh only once
  (token lifetime raised to 30 days), and the sink left records uncommitted when the topics went
  idle (`CONNECT_OFFSET_FLUSH_INTERVAL_MS=10000`).
