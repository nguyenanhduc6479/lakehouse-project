"""GOLD: compute analytics metrics from silver -> lakehouse.gold.stock_daily_metrics.

The serving layer for queries/BI (queried with Trino). Each row = 1 symbol on 1 day with:
- prev_close: previous session's close (lag over time within the same symbol),
- daily_return: daily return = close/prev_close - 1,
- ma5 / ma20: 5- and 20-session moving averages of close,
- range_pct: daily range = (high - low)/close.

WHY window partitionBy(symbol) orderBy(trade_date): every metric is a time series
WITHIN EACH SYMBOL, never mixed across symbols. ma5/ma20 use rowsBetween by session count.

Logic lives in `build_gold()` (a pure function) for unit tests; `main()` handles I/O.
Idempotent via createOrReplace() (small table, a full rebuild is enough).
"""

from __future__ import annotations

import logging

from helper import CATALOG, get_spark
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)
SRC = f"{CATALOG}.silver.stock_ohlcv"
TABLE = f"{CATALOG}.gold.stock_daily_metrics"

GOLD_COLUMNS = [
    "symbol",
    "trade_date",
    "close",
    "prev_close",
    "daily_return",
    "ma5",
    "ma20",
    "range_pct",
    "volume",
]


def build_gold(silver: DataFrame) -> DataFrame:
    """Compute daily metrics per symbol. Pure function (needs high/low/close/volume columns)."""
    w = Window.partitionBy("symbol").orderBy("trade_date")
    w5 = w.rowsBetween(-4, 0)  # 5 sessions including the current one
    w20 = w.rowsBetween(-19, 0)  # 20 sessions

    return (
        silver.withColumn("prev_close", F.lag("close").over(w))
        .withColumn(
            "daily_return",
            F.when(
                F.col("prev_close").isNotNull() & (F.col("prev_close") != 0),
                F.col("close") / F.col("prev_close") - 1,
            ),
        )
        .withColumn("ma5", F.avg("close").over(w5))
        .withColumn("ma20", F.avg("close").over(w20))
        .withColumn("range_pct", (F.col("high") - F.col("low")) / F.col("close"))
        .select(*GOLD_COLUMNS)
    )


def main() -> None:
    spark = get_spark("job_gold")

    silver = spark.table(SRC)
    gold = build_gold(silver)

    rows = gold.count()
    if rows == 0:
        raise RuntimeError("Gold is empty — silver has no data.")
    logger.warning("Gold: %d metric rows.", rows)

    (
        gold.writeTo(TABLE)
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
