"""Test clean_silver: data-quality rules drop bad rows and keep valid ones."""

from __future__ import annotations

from datetime import date, datetime

import pytest
from job_silver import SILVER_COLUMNS, clean_silver
from pyspark.sql.types import (
    DateType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# Needs a JVM -> run in the Linux container; on Windows skip with -m "not spark".
pytestmark = pytest.mark.spark

# Explicit bronze schema -> createDataFrame works even when empty.
BRONZE_SCHEMA = StructType(
    [
        StructField("symbol", StringType(), True),
        StructField("trade_date", DateType(), True),
        StructField("open", DoubleType(), True),
        StructField("high", DoubleType(), True),
        StructField("low", DoubleType(), True),
        StructField("close", DoubleType(), True),
        StructField("volume", LongType(), True),
        StructField("source", StringType(), True),
        StructField("ingested_at", TimestampType(), True),
    ]
)
_TS = datetime(2024, 1, 2, 18, 0)


def _bronze(spark, rows):
    return spark.createDataFrame(rows, BRONZE_SCHEMA)


def test_keeps_valid_row(spark):
    rows = [("FPT", date(2024, 1, 2), 10.0, 11.0, 9.5, 10.5, 1000, "VCI", _TS)]
    out = clean_silver(_bronze(spark, rows))
    assert out.count() == 1
    assert out.columns == SILVER_COLUMNS  # matches the schema contract


def test_filters_bad_rows(spark):
    rows = [
        ("FPT", date(2024, 1, 2), 10.0, 11.0, 9.5, 10.5, 1000, "VCI", _TS),  # valid
        ("VNM", date(2024, 1, 2), 10.0, 11.0, 9.5, 0.0, 1000, "VCI", _TS),  # close = 0 -> dropped
        ("ACB", date(2024, 1, 2), 10.0, 9.0, 11.0, 10.0, 1000, "VCI", _TS),  # high < low -> dropped
        ("HPG", date(2024, 1, 2), 10.0, 11.0, 9.5, 10.5, -5, "VCI", _TS),  # volume < 0 -> dropped
        (None, date(2024, 1, 2), 10.0, 11.0, 9.5, 10.5, 1000, "VCI", _TS),  # null symbol -> dropped
        ("MWG", date(2024, 1, 2), 10.0, 10.2, 9.5, 10.5, 1000, "VCI", _TS),  # high < close -> drop
    ]
    out = clean_silver(_bronze(spark, rows))
    kept = {r["symbol"] for r in out.collect()}
    assert kept == {"FPT"}


def test_empty_input_gives_empty_output(spark):
    out = clean_silver(_bronze(spark, []))
    assert out.count() == 0
    assert out.columns == SILVER_COLUMNS
