"""BRONZE: load raw landing parquet -> Iceberg table lakehouse.bronze.stock_ohlcv.

Bronze = a "nearly raw" copy of the source, with only minimal normalization:
- cast `time` -> `trade_date` as date (natural key by trading day),
- deduplicate by (symbol, trade_date), keeping the row with the latest `ingested_at`
  (each ingest re-pulls a whole date range -> runs on different days overlap).

Idempotent: createOrReplace() overwrites the whole table from the deduped landing union.
The demo dataset is small, so a full rebuild is enough and simplest; for incremental,
switch to MERGE INTO on (symbol, trade_date).

Logic lives in `transform_bronze()` (a pure function, DataFrame in/out) so it can be
unit-tested without MinIO/Polaris; `main()` handles the I/O.
"""

from __future__ import annotations

import logging

from common import CATALOG, get_spark, landing_s3a_uri
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

logger = logging.getLogger(__name__)
TABLE = f"{CATALOG}.bronze.stock_ohlcv"

# Explicit schema of the landing parquet (produced by ingest_ohlcv.py). Declared so
# Spark doesn't infer types — avoids type drift between runs / files.
LANDING_SCHEMA = StructType(
    [
        StructField("time", TimestampType(), True),
        StructField("open", DoubleType(), True),
        StructField("high", DoubleType(), True),
        StructField("low", DoubleType(), True),
        StructField("close", DoubleType(), True),
        StructField("volume", LongType(), True),
        StructField("symbol", StringType(), False),
        StructField("source", StringType(), True),
        StructField("ingested_at", TimestampType(), True),
    ]
)


def transform_bronze(raw: DataFrame) -> DataFrame:
    """Minimal normalization + dedup, keeping the latest row by `ingested_at`.

    Pure function: no I/O, so it can be tested with hand-built DataFrames.
    """
    dedup = Window.partitionBy("symbol", "trade_date").orderBy(F.col("ingested_at").desc())
    return (
        raw.withColumn("trade_date", F.to_date("time"))
        .withColumn("_rn", F.row_number().over(dedup))
        .where(F.col("_rn") == 1)
        .select(
            "symbol",
            "trade_date",
            F.col("open").cast("double").alias("open"),
            F.col("high").cast("double").alias("high"),
            F.col("low").cast("double").alias("low"),
            F.col("close").cast("double").alias("close"),
            F.col("volume").cast("bigint").alias("volume"),
            "source",
            "ingested_at",
        )
    )


def read_landing(spark: SparkSession) -> DataFrame:
    """Read all landing parquet with the explicit schema."""
    return spark.read.schema(LANDING_SCHEMA).parquet(landing_s3a_uri())


def main() -> None:
    spark = get_spark("job_bronze")
    raw = read_landing(spark)
    logger.warning("Landing: read %d raw rows.", raw.count())

    bronze = transform_bronze(raw)

    (
        bronze.writeTo(TABLE)
        .using("iceberg")
        .tableProperty("format-version", "2")
        .partitionedBy("symbol")
        .createOrReplace()
    )
    logger.warning("Wrote %s: %d rows.", TABLE, bronze.count())
    spark.stop()


if __name__ == "__main__":
    logging.basicConfig(level=logging.WARNING, format="%(asctime)s %(levelname)s %(message)s")
    main()
