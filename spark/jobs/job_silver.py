"""Silver, step 1 of 2: bronze CDC events -> SCD2 version rows in Postgres staging (design §6.4).

For every active `pipeline = 'silver'` row of `config.etl_checkpoint`:
1. read only the bronze snapshots committed after `last_snapshot_id` (Iceberg incremental read,
   up to the table's current snapshot; NULL = read the whole table);
2. turn the events into version rows (`events_to_versions`, pure — unit-tested);
3. write them to `staging.<table>` (JDBC overwrite: the table is recreated, so new bronze
   columns show up there).
Finally `staging.batch` is refilled with one row per table: the snapshot range staging holds.

The job never touches silver or the checkpoint: Airflow's `silver_apply` (airflow/plugins/
silver_apply.py) applies staging and advances the checkpoint in one Postgres transaction, so a
failure anywhere leaves the checkpoint where it was and the next run re-reads the same range.

Generic: the tables and their keys come from `config.etl_checkpoint` (`source_table`,
`key_columns`); a new source table needs a checkpoint row, not code.

Runs on Python 3.8 (custom-spark image).
"""

from __future__ import annotations

import logging

from helper import get_spark, postgres_jdbc_options
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("job_silver")

# Validity bounds go to staging as text with an explicit UTC offset, so they mean the same
# instant whatever the session time zone; silver_apply casts them to timestamptz.
TS_FORMAT = "yyyy-MM-dd HH:mm:ss.SSSSSSXXX"

TECHNICAL_COLUMNS = ["_valid_from", "_valid_to", "_is_current", "_is_deleted", "_lsn"]

BATCH_SCHEMA = T.StructType(
    [
        T.StructField("table_name", T.StringType(), False),
        T.StructField("start_snapshot_id", T.LongType(), True),
        T.StructField("end_snapshot_id", T.LongType(), True),
        T.StructField("row_count", T.LongType(), False),
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


def process_table(spark: SparkSession, cp, jdbc: dict[str, str]) -> tuple | None:
    """Stage one table; returns its staging.batch row, or None if bronze has no such table."""
    target, source, keys, start = (
        cp["table_name"],
        cp["source_table"],
        list(cp["key_columns"]),
        cp["last_snapshot_id"],
    )
    if not spark.catalog.tableExists(source):
        log.warning("%s: bronze table %s does not exist yet, skipped", target, source)
        return None

    end = current_snapshot_id(spark, source)
    if end is None or end == start:
        log.info("%s: no new bronze snapshot since %s, nothing to stage", target, start)
        return (target, start, start, 0)

    versions = events_to_versions(read_bronze(spark, source, start, end), keys).persist()
    rows = versions.count()
    if rows:
        staging = "staging." + target.split(".", 1)[1]
        versions.write.format("jdbc").options(**jdbc, dbtable=staging).mode("overwrite").save()
    log.info("%s: snapshots (%s, %s] -> %d version rows", target, start, end, rows)
    versions.unpersist()
    return (target, start, end, rows)


def main() -> None:
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
    batches = [b for b in (process_table(spark, cp, jdbc) for cp in checkpoints) if b]
    # Written last: until here staging.batch still describes the previous run, whose range is
    # never ahead of the checkpoint, so a failure above can make silver_apply lag, never skip.
    # truncate=true keeps the table definition (primary key) from postgres/schema.sql.
    (
        spark.createDataFrame(batches, BATCH_SCHEMA)
        .write.format("jdbc")
        .options(**jdbc, dbtable="staging.batch", truncate="true")
        .mode("overwrite")
        .save()
    )
    spark.stop()


if __name__ == "__main__":
    main()
