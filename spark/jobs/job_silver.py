"""Silver, step 1 of 2: bronze CDC events -> SCD2 version rows in Postgres staging (design §6.4).

For every active `pipeline = 'silver'` row of `config.etl_checkpoint`:
1. read only the bronze snapshots committed after `last_snapshot_id` (Iceberg incremental read,
   up to the table's current snapshot; NULL = read the whole table);
2. turn the events into version rows (`events_to_versions`, pure — unit-tested);
3. write them to `staging.<table>` (JDBC overwrite: the table is recreated, so new bronze
   columns show up there).
Finally `staging.batch` is rewritten with one row per active table — the snapshot range its
staging table holds: rows of the tables processed now are replaced, the others kept as they were
(`merge_batches`), so a one-table run doesn't lose another table's pending batch.

The job never touches silver or the checkpoint: Airflow's `silver_apply` (airflow/dags/
lib/silver_apply.py) applies staging and advances the checkpoint in one Postgres transaction, so a
failure anywhere leaves the checkpoint where it was and the next run re-reads the same range.

Generic: the tables and their keys come from `config.etl_checkpoint` (`source_table`,
`key_columns`); a new source table needs a checkpoint row, not code. `--table silver.<name>`
limits the run to one of them (default `all`). `--full-refresh` (needs one table) ignores its
checkpoint, stages the whole bronze table and marks the batch row `full_refresh = true`, which
makes silver_apply replace that silver table.

Runs on Python 3.8 (custom-spark image).
"""

from __future__ import annotations

import argparse
import logging
from datetime import datetime, timezone

from helper import get_spark, postgres_jdbc_options
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("job_silver")

# Validity bounds go to staging as text with an explicit UTC offset, so they mean the same
# instant whatever the session time zone; silver_apply casts them to timestamptz.
TS_FORMAT = "yyyy-MM-dd HH:mm:ss.SSSSSSXXX"

BATCH_SCHEMA = T.StructType(
    [
        T.StructField("table_name", T.StringType(), False),
        T.StructField("start_snapshot_id", T.LongType(), True),
        T.StructField("end_snapshot_id", T.LongType(), True),
        T.StructField("row_count", T.LongType(), False),
        # what silver_apply checks to replace the silver table (never inferred from a NULL start)
        T.StructField("full_refresh", T.BooleanType(), False),
        # text with an offset, cast by Postgres (stringtype=unspecified on the write): a kept row
        # must keep its original time, and JDBC timestamps would depend on the JVM time zone
        # (the JDBC write names its columns, so this order needn't match the table's)
        T.StructField("created_at", T.StringType(), False),
    ]
)


def events_to_versions(events: DataFrame, key_columns: list[str]) -> DataFrame:
    """Bronze CDC events -> one row per version of a source row (no I/O).

    `events` has the bronze columns `op`, `before`, `after` (structs of the source row) and
    `source` (with `lsn`, `txId`, `ts_us`). Rules (design §6.2):
    - the row is `after` for c/u/r and `before` for d (`_is_deleted = true`);
    - duplicates (Debezium is at-least-once) collapse on (key, lsn);
    - only the last event of a key within one source transaction becomes a version, otherwise an
      insert + update in one transaction would give a zero-length version;
    - versions of a key are ordered by LSN, never by time: `_valid_to` is the next version's
      `_valid_from` (commit time of the source transaction, `source.ts_us`), and the last version
      is current.
    The batch may contain events already in silver (re-runs): silver_apply skips them by `_lsn`.
    `_tx_id` is kept: the grouping by transaction only sees this batch, so silver_apply finishes it
    across runs (a transaction split by a sink commit).
    """
    is_delete = F.col("op") == "d"
    flat = events.select(
        F.when(is_delete, F.col("before")).otherwise(F.col("after")).alias("_row"),
        is_delete.alias("_is_deleted"),
        F.col("source.lsn").alias("_lsn"),
        F.col("source.txId").alias("_tx_id"),
        F.col("source.ts_us").alias("_ts_us"),
    ).select("_row.*", "_is_deleted", "_lsn", "_tx_id", "_ts_us")
    source_columns = [
        c for c in flat.columns if c not in ("_is_deleted", "_lsn", "_tx_id", "_ts_us")
    ]

    deduped = flat.dropDuplicates(key_columns + ["_lsn"])

    per_tx = Window.partitionBy(*key_columns, "_tx_id").orderBy(F.col("_lsn").desc())
    last_in_tx = (
        deduped.withColumn("_n", F.row_number().over(per_tx)).where(F.col("_n") == 1).drop("_n")
    )

    per_key = Window.partitionBy(*key_columns).orderBy("_lsn")
    versions = last_in_tx.withColumn("_valid_to_us", F.lead("_ts_us").over(per_key))

    def as_text(micros: str):
        return F.date_format(F.expr(f"timestamp_micros({micros})"), TS_FORMAT)

    return versions.select(
        *source_columns,
        as_text("_ts_us").alias("_valid_from"),
        as_text("_valid_to_us").alias("_valid_to"),
        F.col("_valid_to_us").isNull().alias("_is_current"),
        "_is_deleted",
        "_lsn",
        "_tx_id",
    )


def current_snapshot_id(spark: SparkSession, table: str) -> int | None:
    """Snapshot of the table's `main` branch, or None if the table has no snapshot yet."""
    rows = spark.sql(f"SELECT snapshot_id FROM {table}.refs WHERE name = 'main'").collect()
    return rows[0]["snapshot_id"] if rows else None


def read_bronze(spark: SparkSession, table: str, start: int | None, end: int) -> DataFrame:
    """Events committed after snapshot `start` (exclusive) up to `end`; all of `end` if no start.

    An incremental read walks the parent chain from `end` back to `start`, so `start` must still
    exist: never expire the checkpoint snapshot (design §9, DAG platform_maintenance).
    """
    reader = spark.read.format("iceberg")
    if start is None:
        reader = reader.option("snapshot-id", str(end))
    else:
        reader = reader.option("start-snapshot-id", str(start)).option("end-snapshot-id", str(end))
    return reader.load(table)


def process_table(
    spark: SparkSession, cp, jdbc: dict[str, str], full_refresh: bool = False
) -> tuple | None:
    """Stage one table; returns its staging.batch row without `created_at`
    (table, start, end, rows, full_refresh), or None if bronze has no such table.

    `full_refresh`: ignore the checkpoint and stage the whole bronze table (start = None); the
    batch row's `full_refresh = true` tells silver_apply to replace the silver table.
    """
    target, source, keys = cp["table_name"], cp["source_table"], list(cp["key_columns"])
    start = None if full_refresh else cp["last_snapshot_id"]
    if full_refresh:
        log.info("%s: full refresh, checkpoint %s ignored", target, cp["last_snapshot_id"])
    if not spark.catalog.tableExists(source):
        log.warning("%s: bronze table %s does not exist yet, skipped", target, source)
        return None

    end = current_snapshot_id(spark, source)
    if end is None or end == start:
        log.info("%s: no new bronze snapshot since %s, nothing to stage", target, start)
        return (target, start, start, 0, full_refresh)

    versions = events_to_versions(read_bronze(spark, source, start, end), keys).persist()
    rows = versions.count()
    if rows:
        staging = "staging." + target.split(".", 1)[1]
        versions.write.format("jdbc").options(**jdbc, dbtable=staging).mode("overwrite").save()
    log.info("%s: snapshots (%s, %s] -> %d version rows", target, start, end, rows)
    versions.unpersist()
    return (target, start, end, rows, full_refresh)


def select_checkpoints(checkpoints: list, table: str) -> list:
    """All active checkpoint rows for `table = 'all'`, else only that table's (or fail)."""
    if table == "all":
        return checkpoints
    selected = [cp for cp in checkpoints if cp["table_name"] == table]
    if not selected:
        known = ", ".join(sorted(cp["table_name"] for cp in checkpoints)) or "none"
        raise ValueError(f"{table} is not an active silver table (active: {known})")
    return selected


def merge_batches(existing: list[tuple], new: list[tuple], active: list[str]) -> list[tuple]:
    """staging.batch rows to write back: `new` replaces the rows of its tables, the existing rows
    of the other active tables stay unchanged, rows of tables no longer active are dropped.

    Rows are (table_name, start_snapshot_id, end_snapshot_id, row_count, full_refresh,
    created_at): a kept row keeps its flag, so a full refresh still waiting for silver_apply
    survives a run of another table.
    """
    merged = {row[0]: row for row in existing if row[0] in active}
    merged.update({row[0]: row for row in new})
    return [merged[name] for name in sorted(merged)]


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="bronze -> staging for the silver layer")
    parser.add_argument("--table", default="all", help="'all' or a silver table, e.g. silver.users")
    # a flag, not a value: a string "false" would be truthy (and BooleanOptionalAction is 3.9+)
    parser.add_argument(
        "--full-refresh",
        action="store_true",
        help="ignore the checkpoint: stage the whole bronze table, silver_apply rebuilds silver "
        "(one table only)",
    )
    args = parser.parse_args(argv)
    # same rule as the DAG: rebuilding every table at once is never what a by-hand run means
    if args.full_refresh and args.table == "all":
        parser.error("--full-refresh needs a specific --table, not 'all'")
    return args


def main() -> None:
    args = parse_args()
    table, full_refresh = args.table, args.full_refresh

    spark = get_spark("job_silver")
    jdbc = postgres_jdbc_options()
    checkpoints = (
        spark.read.format("jdbc")
        .options(**jdbc)
        .option(
            "query",
            "SELECT table_name, source_table, key_columns, last_snapshot_id "
            "FROM config.etl_checkpoint WHERE pipeline = 'silver' AND is_active",
        )
        .load()
        .collect()
    )
    active = [cp["table_name"] for cp in checkpoints]
    checkpoints = select_checkpoints(checkpoints, table)
    log.info(
        "tables: %s%s",
        ", ".join(cp["table_name"] for cp in checkpoints),
        " (full refresh)" if full_refresh else "",
    )
    now = datetime.now(timezone.utc).isoformat()
    batches = [
        b + (now,)
        for b in (process_table(spark, cp, jdbc, full_refresh) for cp in checkpoints)
        if b
    ]

    # Written last: until here staging.batch still describes the previous run, whose range is
    # never ahead of the checkpoint, so a failure above can make silver_apply lag, never skip.
    # Read fully (collected) before the write, then written back whole: truncate=true keeps the
    # table definition (primary key) from postgres/schema.sql.
    existing = [
        tuple(r)
        for r in spark.read.format("jdbc")
        .options(**jdbc)
        .option(
            "query",
            "SELECT table_name, start_snapshot_id, end_snapshot_id, row_count, "
            "full_refresh, created_at::text AS created_at FROM staging.batch",
        )
        .load()
        .collect()
    ]
    rows = merge_batches(existing, batches, active)
    for r in rows:
        log.info("staging.batch: %s (%s, %s] %d rows, full_refresh=%s, %s", *r)
    (
        spark.createDataFrame(rows, BATCH_SCHEMA)
        .write.format("jdbc")
        .options(**{**jdbc, "url": jdbc["url"] + "?stringtype=unspecified"})
        .options(dbtable="staging.batch", truncate="true")
        .mode("overwrite")
        .save()
    )
    spark.stop()


if __name__ == "__main__":
    main()
