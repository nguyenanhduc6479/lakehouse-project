"""GOLD: từ silver tính chỉ số phân tích -> lakehouse.gold.stock_daily_metrics.

Đây là tầng phục vụ truy vấn/BI (query bằng Trino). Mỗi dòng = 1 mã 1 ngày kèm:
- prev_close: giá đóng cửa phiên trước (lag theo thời gian trong cùng mã),
- daily_return: lợi suất ngày = close/prev_close - 1,
- ma5 / ma20: trung bình động 5 và 20 phiên của close,
- range_pct: biên độ ngày = (high - low)/close.

WHY window partitionBy(symbol) orderBy(trade_date): mọi chỉ số đều theo chuỗi thời gian
TRONG TỪNG MÃ, không được trộn giữa các mã. ma5/ma20 dùng rowsBetween theo số phiên.
Idempotent bằng createOrReplace() (bảng nhỏ, rebuild toàn bộ là đủ).
"""
from __future__ import annotations

import logging

from pyspark.sql import Window
from pyspark.sql import functions as F

from common import CATALOG, get_spark

logger = logging.getLogger(__name__)
SRC = f"{CATALOG}.silver.stock_ohlcv"
TABLE = f"{CATALOG}.gold.stock_daily_metrics"


def main() -> None:
    spark = get_spark("job_gold")

    silver = spark.table(SRC)

    w = Window.partitionBy("symbol").orderBy("trade_date")
    w5 = w.rowsBetween(-4, 0)    # 5 phiên gồm phiên hiện tại
    w20 = w.rowsBetween(-19, 0)  # 20 phiên

    gold = (
        silver.withColumn("prev_close", F.lag("close").over(w))
        .withColumn(
            "daily_return",
            F.when(F.col("prev_close").isNotNull() & (F.col("prev_close") != 0),
                   F.col("close") / F.col("prev_close") - 1),
        )
        .withColumn("ma5", F.avg("close").over(w5))
        .withColumn("ma20", F.avg("close").over(w20))
        .withColumn("range_pct", (F.col("high") - F.col("low")) / F.col("close"))
        .select(
            "symbol", "trade_date", "close", "prev_close",
            "daily_return", "ma5", "ma20", "range_pct", "volume",
        )
    )

    rows = gold.count()
    if rows == 0:
        raise RuntimeError("Gold rỗng — silver không có dữ liệu.")
    logger.warning("Gold: %d dòng chỉ số.", rows)

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
