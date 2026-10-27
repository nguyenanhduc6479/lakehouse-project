"""SILVER: clean bronze -> Iceberg table lakehouse.silver.stock_ohlcv.

Cleaning (data quality) rules for OHLCV:
- drop rows missing a key (null symbol/trade_date),
- keep valid prices: open/high/low/close > 0,
- candle invariants: high >= low, high >= max(open, close), low <= min(open, close),
- volume >= 0.

Filtering lives in `clean_silver()` (a pure function) for unit tests; `main()` handles
I/O, logs data-quality metrics (row-count / null-count) per repo convention, then
asserts silver is non-empty before writing.
"""

from __future__ import annotations

import logging

from helper import CATALOG, get_spark
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)
SRC = f"{CATALOG}.bronze.stock_ohlcv"
TABLE = f"{CATALOG}.silver.stock_ohlcv"

# Silver output columns (declared explicitly to keep a stable schema contract for gold/Trino).
SILVER_COLUMNS = ["symbol", "trade_date", "open", "high", "low", "close", "volume"]


def clean_silver(bronze: DataFrame) -> DataFrame:
    """Apply DQ rules, return a clean DataFrame with exactly SILVER_COLUMNS. Pure function."""
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
    logger.warning("Bronze: %d rows, %d rows with null keys.", total, null_keys)

    clean = clean_silver(bronze)
    kept = clean.count()
    logger.warning("Silver: kept %d / %d rows (dropped %d).", kept, total, total - kept)

    # Assertion: an empty silver almost certainly means a config/source error -> fail the job early.
    if kept == 0:
        raise RuntimeError("Silver is empty after cleaning — check bronze/DQ rules.")

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
