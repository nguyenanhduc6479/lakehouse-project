"""BRONZE: nạp parquet thô ở landing -> bảng Iceberg lakehouse.bronze.stock_ohlcv.

Bronze = bản sao "gần như thô" của nguồn, chỉ chuẩn hoá tối thiểu:
- ép `time` -> `trade_date` kiểu date (khoá tự nhiên theo ngày giao dịch),
- khử trùng lặp theo (symbol, trade_date), giữ bản có `ingested_at` mới nhất
  (vì mỗi lần ingest kéo lại cả khoảng ngày -> các lần chạy khác ngày sẽ chồng nhau).

Idempotent: dùng createOrReplace() ghi đè toàn bộ bảng từ union landing đã dedupe.
Dataset demo nhỏ nên full-rebuild là đủ và đơn giản nhất; muốn incremental thì đổi
sang MERGE INTO theo (symbol, trade_date).
"""
from __future__ import annotations

import logging

from pyspark.sql import Window
from pyspark.sql import functions as F

from common import CATALOG, get_spark, landing_s3a_uri

logger = logging.getLogger(__name__)
TABLE = f"{CATALOG}.bronze.stock_ohlcv"


def main() -> None:
    spark = get_spark("job_bronze")

    raw = spark.read.parquet(landing_s3a_uri())
    logger.warning("Landing đọc được %d dòng thô.", raw.count())

    # Chuẩn hoá tối thiểu + khử trùng lặp giữ bản mới nhất theo ingested_at.
    w = Window.partitionBy("symbol", "trade_date").orderBy(F.col("ingested_at").desc())
    bronze = (
        raw.withColumn("trade_date", F.to_date("time"))
        .withColumn("_rn", F.row_number().over(w))
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
