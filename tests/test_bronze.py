"""Test transform_bronze: khử trùng lặp giữ bản mới nhất theo ingested_at."""

from __future__ import annotations

from datetime import date, datetime

import pytest
from job_bronze import transform_bronze

# Cần SparkSession thật (JVM) -> chạy trong container Linux, bỏ qua trên host Windows bằng
#   uv run pytest -m "not spark"
pytestmark = pytest.mark.spark


def _row(ts: datetime, close: float, ingested_at: datetime, symbol: str = "FPT"):
    return (ts, 10.0, 11.0, 9.0, close, 1000, symbol, "VCI", ingested_at)


COLUMNS = ["time", "open", "high", "low", "close", "volume", "symbol", "source", "ingested_at"]


def test_dedup_keeps_latest_ingested_at(spark):
    # Cùng (FPT, 2024-01-02) xuất hiện 2 lần ở 2 lần ingest khác nhau -> giữ bản ingest sau.
    rows = [
        _row(datetime(2024, 1, 2), close=20.0, ingested_at=datetime(2024, 1, 2, 18, 0)),
        _row(datetime(2024, 1, 2), close=21.5, ingested_at=datetime(2024, 1, 3, 18, 0)),  # mới hơn
        _row(datetime(2024, 1, 3), close=22.0, ingested_at=datetime(2024, 1, 3, 18, 0)),
    ]
    df = spark.createDataFrame(rows, COLUMNS)

    out = {(r["symbol"], r["trade_date"]): r["close"] for r in transform_bronze(df).collect()}

    assert len(out) == 2  # 2 ngày phân biệt
    assert out[("FPT", date(2024, 1, 2))] == 21.5  # giữ close của bản ingest mới nhất
    assert out[("FPT", date(2024, 1, 3))] == 22.0


def test_output_has_trade_date_and_no_time(spark):
    df = spark.createDataFrame(
        [_row(datetime(2024, 1, 2), close=20.0, ingested_at=datetime(2024, 1, 2, 18, 0))],
        COLUMNS,
    )
    cols = transform_bronze(df).columns
    assert "trade_date" in cols
    assert "time" not in cols  # đã đổi sang trade_date
    assert "_rn" not in cols  # cột phụ dedupe không lọt ra ngoài
