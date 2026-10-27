"""Test build_gold: prev_close / daily_return / ma5 / ma20 / range_pct đúng theo từng mã."""

from __future__ import annotations

from datetime import date

import pytest
from job_gold import GOLD_COLUMNS, build_gold

# Cần JVM -> chạy trong container Linux; trên Windows bỏ qua bằng -m "not spark".
pytestmark = pytest.mark.spark

# Đầu vào = schema silver + high/low (build_gold cần high/low cho range_pct).
SILVER_COLUMNS_IN = ["symbol", "trade_date", "open", "high", "low", "close", "volume"]


def _silver(spark, rows):
    return spark.createDataFrame(rows, SILVER_COLUMNS_IN)


def test_metrics_single_symbol(spark):
    # 3 phiên liên tiếp của FPT, close = 10, 11, 12.
    rows = [
        ("FPT", date(2024, 1, 2), 10.0, 10.5, 9.5, 10.0, 100),
        ("FPT", date(2024, 1, 3), 11.0, 11.5, 10.5, 11.0, 100),
        ("FPT", date(2024, 1, 4), 12.0, 12.6, 11.4, 12.0, 100),
    ]
    out = {r["trade_date"]: r for r in build_gold(_silver(spark, rows)).collect()}

    d2, d3, d4 = date(2024, 1, 2), date(2024, 1, 3), date(2024, 1, 4)

    # prev_close: phiên đầu chưa có -> None
    assert out[d2]["prev_close"] is None
    assert out[d2]["daily_return"] is None
    assert out[d3]["prev_close"] == 10.0
    assert out[d3]["daily_return"] == pytest.approx(11.0 / 10.0 - 1)  # 0.1
    assert out[d4]["daily_return"] == pytest.approx(12.0 / 11.0 - 1)

    # ma5/ma20 khi ít hơn cửa sổ = trung bình luỹ kế tới phiên đó
    assert out[d2]["ma5"] == pytest.approx(10.0)
    assert out[d3]["ma5"] == pytest.approx((10.0 + 11.0) / 2)
    assert out[d4]["ma20"] == pytest.approx((10.0 + 11.0 + 12.0) / 3)

    # range_pct = (high - low)/close tại phiên d4 = (12.6-11.4)/12
    assert out[d4]["range_pct"] == pytest.approx((12.6 - 11.4) / 12.0)

    assert set(out[d2].asDict().keys()) == set(GOLD_COLUMNS)


def test_window_does_not_mix_symbols(spark):
    # prev_close của mã này KHÔNG được lấy từ mã khác.
    rows = [
        ("FPT", date(2024, 1, 2), 10.0, 10.5, 9.5, 10.0, 100),
        ("VNM", date(2024, 1, 3), 50.0, 50.5, 49.5, 50.0, 100),
    ]
    out = {r["symbol"]: r for r in build_gold(_silver(spark, rows)).collect()}
    # mỗi mã chỉ có 1 phiên -> prev_close đều None, không rò rỉ giữa 2 mã
    assert out["FPT"]["prev_close"] is None
    assert out["VNM"]["prev_close"] is None
