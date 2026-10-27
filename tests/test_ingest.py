"""Test run_ingest: mock vnstock (_fetch_symbol) + boto3 — không gọi mạng/MinIO thật."""

from __future__ import annotations

import io

import ingest_ohlcv
import pandas as pd
import pytest
from ingest_ohlcv import LANDING_PREFIX, run_ingest


def _fake_df(symbol: str, n: int = 3) -> pd.DataFrame:
    """Giả lập kết quả vnstock Quote.history cho 1 mã (sau khi _fetch_symbol gắn symbol/source)."""
    return pd.DataFrame(
        {
            "time": pd.date_range("2024-01-02", periods=n, freq="D"),
            "open": [10.0] * n,
            "high": [11.0] * n,
            "low": [9.0] * n,
            "close": [10.5] * n,
            "volume": [1000] * n,
            "symbol": [symbol] * n,
            "source": ["VCI"] * n,
        }
    )


def _run(mocker, fetch_side_effect, symbols):
    """Chạy run_ingest với _fetch_symbol và boto3.client đã bị mock. Trả về (key, s3_mock)."""
    mocker.patch.object(ingest_ohlcv, "_fetch_symbol", side_effect=fetch_side_effect)
    s3 = mocker.MagicMock()
    mocker.patch("boto3.client", return_value=s3)
    key = run_ingest(
        symbols=symbols,
        start="2024-01-01",
        end="2024-01-05",
        endpoint_url="http://minio:9000",
        access_key="key",
        secret_key="secret",
        bucket="lakehouse",
        ingest_date="2024-01-05",
    )
    return key, s3


def test_writes_one_parquet_to_expected_key(mocker):
    key, s3 = _run(mocker, lambda sym, start, end: _fake_df(sym), ["FPT", "VNM"])

    assert key == f"{LANDING_PREFIX}/ingest_date=2024-01-05/data.parquet"
    s3.put_object.assert_called_once()
    kwargs = s3.put_object.call_args.kwargs
    assert kwargs["Bucket"] == "lakehouse"
    assert kwargs["Key"] == key

    # Body là parquet hợp lệ, gồm cả 2 mã + cột ingested_at do run_ingest thêm.
    back = pd.read_parquet(io.BytesIO(kwargs["Body"]))
    assert set(back["symbol"]) == {"FPT", "VNM"}
    assert "ingested_at" in back.columns
    assert len(back) == 6  # 3 dòng/mã × 2 mã


def test_skips_empty_symbol(mocker):
    def fetch(sym, start, end):
        return pd.DataFrame() if sym == "VNM" else _fake_df(sym)

    _, s3 = _run(mocker, fetch, ["FPT", "VNM"])
    back = pd.read_parquet(io.BytesIO(s3.put_object.call_args.kwargs["Body"]))
    assert set(back["symbol"]) == {"FPT"}  # VNM rỗng bị bỏ qua


def test_symbol_error_is_isolated(mocker):
    def fetch(sym, start, end):
        if sym == "VNM":
            raise ValueError("API lỗi")
        return _fake_df(sym)

    _, s3 = _run(mocker, fetch, ["FPT", "VNM"])
    back = pd.read_parquet(io.BytesIO(s3.put_object.call_args.kwargs["Body"]))
    assert set(back["symbol"]) == {"FPT"}  # lỗi 1 mã không làm hỏng cả batch


def test_all_empty_raises(mocker):
    with pytest.raises(RuntimeError):
        _run(mocker, lambda sym, start, end: pd.DataFrame(), ["FPT", "VNM"])
