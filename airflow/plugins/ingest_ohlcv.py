"""INGEST layer: pull daily OHLCV from vnstock and drop raw parquet onto MinIO (landing).

WHY in airflow/plugins: this directory is mounted into Airflow (see compose/airflow.yml),
so the DAG imports `run_ingest` directly. The file also has a `__main__` CLI to run by hand
from a local venv (e.g. .scratch_vnstock) while debugging — no Airflow needed.

WHY ingest is separate from Spark: the custom-spark image does NOT install vnstock (only
pandas/pyarrow). vnstock is a pure-Python client for the VCI API, so extraction runs in
Python and lands parquet; the Spark jobs then just read parquet -> Iceberg. Keeps the
Spark image lean.

WHY boto3 (not s3fs): Airflow already has boto3 via the amazon provider; adding s3fs easily
clashes on aiobotocore versions. Parquet is written to a buffer with pyarrow, then
put_object straight to MinIO.
"""

from __future__ import annotations

import argparse
import io
import logging
from datetime import UTC, date, datetime

logger = logging.getLogger(__name__)

# Matches DEFAULT_SYMBOLS in spark/jobs/common.py (keep both in sync by hand, since
# the Airflow-side and Spark-side Python can't share a module).
DEFAULT_SYMBOLS = ["FPT", "VNM", "ACB", "HPG", "MWG"]
LANDING_PREFIX = "bronze/_landing/stock_ohlcv"


def _fetch_symbol(symbol: str, start: str, end: str):
    """Fetch daily OHLCV for 1 symbol. Imports vnstock inside the function.

    WHY a lazy import: the Airflow scheduler parses plugin files when loading DAGs; importing
    vnstock at top level while the image lacks it would break the whole DAG import. Importing
    inside the function -> it only fails when the task actually runs, and the local CLI still works.
    """
    from vnstock.api.quote import Quote  # verified 2026-10-27: time/open/high/low/close/volume

    df = Quote(symbol=symbol, source="VCI").history(start=start, end=end, interval="1D")
    # vnstock returns column 'time' (datetime64), open/high/low/close (float), volume (int)
    df = df.assign(symbol=symbol, source="VCI")
    return df


def run_ingest(
    symbols: list[str] | None = None,
    start: str = "2024-01-01",
    end: str | None = None,
    *,
    endpoint_url: str,
    access_key: str,
    secret_key: str,
    bucket: str = "lakehouse",
    ingest_date: str | None = None,
) -> str:
    """Pull OHLCV for `symbols` over [start, end] and write 1 parquet file to landing.

    Idempotent: writes to a fixed key per `ingest_date` (default today), so a re-run on
    the same day OVERWRITES the old file, leaving no junk. Returns the S3 key written.
    """
    import boto3
    import pandas as pd

    symbols = symbols or DEFAULT_SYMBOLS
    end = end or date.today().isoformat()
    ingest_date = ingest_date or date.today().isoformat()
    ingested_at = datetime.now(UTC)

    frames = []
    for sym in symbols:
        try:
            df = _fetch_symbol(sym, start, end)
            if df is None or df.empty:
                logger.warning("Symbol %s has no data in [%s, %s], skipping.", sym, start, end)
                continue
            frames.append(df)
            logger.info("Fetched %s: %d rows.", sym, len(df))
        except Exception:  # noqa: BLE001 - log and continue with other symbols, don't fail the whole batch
            logger.exception("Error fetching symbol %s, skipping.", sym)

    if not frames:
        raise RuntimeError("No data fetched for any symbol — stopping ingest.")

    # Concatenate + stamp the ingest time so the bronze layer can dedupe by latest row.
    data = pd.concat(frames, ignore_index=True)
    data["ingested_at"] = ingested_at
    logger.info("Total %d rows for %d symbols.", len(data), len(frames))

    # Write parquet to a buffer (pyarrow) then upload — never touches local disk.
    buf = io.BytesIO()
    data.to_parquet(buf, engine="pyarrow", index=False)
    buf.seek(0)

    key = f"{LANDING_PREFIX}/ingest_date={ingest_date}/data.parquet"
    s3 = boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name="us-east-1",  # MinIO ignores region but boto3 needs a value
    )
    s3.put_object(Bucket=bucket, Key=key, Body=buf.getvalue())
    uri = f"s3://{bucket}/{key}"
    logger.info("Wrote landing: %s", uri)
    return key


def _main() -> None:
    """Manual CLI: reads config from command-line flags or env (MINIO_*)."""
    import os

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    p = argparse.ArgumentParser(description="Ingest vnstock OHLCV -> landing parquet on MinIO")
    p.add_argument(
        "--symbols", default=",".join(DEFAULT_SYMBOLS), help="Comma-separated list of symbols"
    )
    p.add_argument("--start", default="2024-01-01")
    p.add_argument("--end", default=None)
    p.add_argument(
        "--endpoint-url", default=os.environ.get("MINIO_ENDPOINT", "http://localhost:9000")
    )
    p.add_argument("--access-key", default=os.environ.get("MINIO_ROOT_USER"))
    p.add_argument("--secret-key", default=os.environ.get("MINIO_ROOT_PASSWORD"))
    p.add_argument("--bucket", default=os.environ.get("MINIO_BUCKET", "lakehouse"))
    args = p.parse_args()

    run_ingest(
        symbols=[s.strip() for s in args.symbols.split(",") if s.strip()],
        start=args.start,
        end=args.end,
        endpoint_url=args.endpoint_url,
        access_key=args.access_key,
        secret_key=args.secret_key,
        bucket=args.bucket,
    )


if __name__ == "__main__":
    _main()
