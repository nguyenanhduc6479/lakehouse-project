"""SILVER: làm sạch bronze -> bảng Iceberg lakehouse.silver.stock_ohlcv.

Luật làm sạch (data quality) cho OHLCV:
- bỏ dòng thiếu khoá (symbol/trade_date null),
- giữ giá hợp lệ: open/high/low/close > 0,
- bất biến nến: high >= low, high >= max(open, close), low <= min(open, close),
- volume >= 0.

Logic lọc tách ra `clean_silver()` (hàm thuần) để unit-test; `main()` lo I/O và
in các chỉ số data-quality (row-count / null-count) đúng convention repo, rồi
assert silver không rỗng trước khi ghi.
"""

from __future__ import annotations

import logging

from common import CATALOG, get_spark
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)
SRC = f"{CATALOG}.bronze.stock_ohlcv"
TABLE = f"{CATALOG}.silver.stock_ohlcv"

# Cột đầu ra của silver (khai báo rõ để giữ hợp đồng schema ổn định cho gold/Trino).
SILVER_COLUMNS = ["symbol", "trade_date", "open", "high", "low", "close", "volume"]


def clean_silver(bronze: DataFrame) -> DataFrame:
    """Áp luật DQ, trả về DataFrame sạch với đúng SILVER_COLUMNS. Hàm thuần."""
    return bronze.where(
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
    ).select(*SILVER_COLUMNS)


def main() -> None:
    spark = get_spark("job_silver")

    bronze = spark.table(SRC)
    total = bronze.count()
    null_keys = bronze.where(F.col("symbol").isNull() | F.col("trade_date").isNull()).count()
    logger.warning("Bronze: %d dòng, %d dòng null khoá.", total, null_keys)

    clean = clean_silver(bronze)
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
