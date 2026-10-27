"""Test run_ingest: mocks vnstock (_fetch_symbol) + boto3 — no real network/MinIO calls."""

from __future__ import annotations

import io

import ingest_ohlcv
import pandas as pd
import pytest
from ingest_ohlcv import LANDING_PREFIX, run_ingest


def _fake_df(symbol: str, n: int = 3) -> pd.DataFrame:
    """Fake vnstock Quote.history result for 1 symbol (after _fetch_symbol adds symbol/source)."""
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
    """Run run_ingest with _fetch_symbol and boto3.client mocked. Returns (key, s3_mock)."""
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

    # Body is valid parquet with both symbols + the ingested_at column added by run_ingest.
    back = pd.read_parquet(io.BytesIO(kwargs["Body"]))
    assert set(back["symbol"]) == {"FPT", "VNM"}
    assert "ingested_at" in back.columns
    assert len(back) == 6  # 3 rows/symbol × 2 symbols


def test_skips_empty_symbol(mocker):
    def fetch(sym, start, end):
        return pd.DataFrame() if sym == "VNM" else _fake_df(sym)

    _, s3 = _run(mocker, fetch, ["FPT", "VNM"])
    back = pd.read_parquet(io.BytesIO(s3.put_object.call_args.kwargs["Body"]))
    assert set(back["symbol"]) == {"FPT"}  # empty VNM is skipped


def test_symbol_error_is_isolated(mocker):
    def fetch(sym, start, end):
        if sym == "VNM":
            raise ValueError("API error")
        return _fake_df(sym)

    _, s3 = _run(mocker, fetch, ["FPT", "VNM"])
    back = pd.read_parquet(io.BytesIO(s3.put_object.call_args.kwargs["Body"]))
    assert set(back["symbol"]) == {"FPT"}  # one symbol failing doesn't break the whole batch


def test_all_empty_raises(mocker):
    with pytest.raises(RuntimeError):
        _run(mocker, lambda sym, start, end: pd.DataFrame(), ["FPT", "VNM"])
