"""BRONZE: nạp parquet thô ở landing -> bảng Iceberg lakehouse.bronze.stock_ohlcv.

Bronze = bản sao "gần như thô" của nguồn, chỉ chuẩn hoá tối thiểu:
- ép `time` -> `trade_date` kiểu date (khoá tự nhiên theo ngày giao dịch),
- khử trùng lặp theo (symbol, trade_date), giữ bản có `ingested_at` mới nhất
  (vì mỗi lần ingest kéo lại cả khoảng ngày -> các lần chạy khác ngày sẽ chồng nhau).

Idempotent: dùng createOrReplace() ghi đè toàn bộ bảng từ union landing đã dedupe.
Dataset demo nhỏ nên full-rebuild là đủ và đơn giản nhất; muốn incremental thì đổi
sang MERGE INTO theo (symbol, trade_date).

Logic đặt trong `transform_bronze()` (hàm thuần, chỉ nhận/trả DataFrame) để unit-test
được mà không cần MinIO/Polaris; `main()` lo phần I/O.
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

# Schema tường minh của parquet landing (do ingest_ohlcv.py sinh ra). Khai báo rõ để
# Spark không phải suy luận kiểu — tránh lệch kiểu giữa các lần chạy / file khác nhau.
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
    """Chuẩn hoá tối thiểu + khử trùng lặp, giữ bản mới nhất theo `ingested_at`.

    Hàm thuần: không đọc/ghi I/O, nên test được bằng DataFrame dựng tay.
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
    """Đọc toàn bộ parquet landing với schema tường minh."""
    return spark.read.schema(LANDING_SCHEMA).parquet(landing_s3a_uri())


def main() -> None:
    spark = get_spark("job_bronze")
    raw = read_landing(spark)
    logger.warning("Landing đọc được %d dòng thô.", raw.count())

    bronze = transform_bronze(raw)

    (
        bronze.writeTo(TABLE)
        .using("iceberg")
        .tableProperty("format-version", "2")
        .partitionedBy("symbol")
        .createOrReplace()
    )
    logger.warning("Ghi %s xong: %d dòng.", TABLE, bronze.count())
    spark.stop()


if __name__ == "__main__":
    logging.basicConfig(level=logging.WARNING, format="%(asctime)s %(levelname)s %(message)s")
    main()
