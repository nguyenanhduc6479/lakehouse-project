"""SILVER: làm sạch bronze -> bảng Iceberg lakehouse.silver.stock_ohlcv.

Luật làm sạch (data quality) cho OHLCV:
- bỏ dòng thiếu khoá (symbol/trade_date null),
- giữ giá hợp lệ: open/high/low/close > 0,
- bất biến nến: high >= low, high >= open/close, low <= open/close,
- volume >= 0.

Kèm assertion row-count (đúng convention repo: data quality check phải có row/null count).
Idempotent bằng createOrReplace().
"""
from __future__ import annotations

import logging

from pyspark.sql import functions as F

from common import CATALOG, get_spark

logger = logging.getLogger(__name__)
SRC = f"{CATALOG}.bronze.stock_ohlcv"
TABLE = f"{CATALOG}.silver.stock_ohlcv"


def main() -> None:
    spark = get_spark("job_silver")

    bronze = spark.table(SRC)
    total = bronze.count()

    # Đếm null khoá trước khi lọc (để log chất lượng nguồn).
    null_keys = bronze.where(F.col("symbol").isNull() | F.col("trade_date").isNull()).count()
    logger.warning("Bronze: %d dòng, %d dòng null khoá.", total, null_keys)

    clean = bronze.where(
        F.col("symbol").isNotNull()
        & F.col("trade_date").isNotNull()
        & (F.col("open") > 0)
        & (F.col("high") > 0)
        & (F.col("low") > 0)
        & (F.col("close") > 0)
        & (F.col("volume") >= 0)
        & (F.col("high") >= F.col("low"))
        & (F.col("high") >= F.greatest("open", "close"))
        & (F.col("low") <= F.least("open", "close"))
    ).select("symbol", "trade_date", "open", "high", "low", "close", "volume")

    kept = clean.count()
    logger.warning("Silver: giữ %d / %d dòng (loại %d).", kept, total, total - kept)

    # Assertion: silver rỗng gần như chắc chắn là lỗi cấu hình/nguồn -> fail job sớm.
    if kept == 0:
        raise RuntimeError("Silver rỗng sau khi làm sạch — kiểm tra bronze/luật DQ.")

    (
        clean.writeTo(TABLE)
        .using("iceberg")
        .tableProperty("format-version", "2")
        .partitionedBy("symbol")
        .createOrReplace()
    )
    logger.warning("Ghi %s xong.", TABLE)
    spark.stop()


if __name__ == "__main__":
    logging.basicConfig(level=logging.WARNING, format="%(asctime)s %(levelname)s %(message)s")
    main()
