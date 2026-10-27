"""Tầng INGEST: kéo OHLCV ngày từ vnstock và đổ parquet thô xuống MinIO (landing).

WHY để ở airflow/plugins: thư mục này được mount vào Airflow (xem compose/airflow.yml),
nên DAG import trực tiếp `run_ingest`. Đồng thời file có CLI `__main__` để chạy tay bằng
venv local (ví dụ .scratch_vnstock) khi debug — không cần Airflow.

WHY tách ingest ra khỏi Spark: image custom-spark KHÔNG cài vnstock (chỉ có pandas/
pyarrow). vnstock là client Python thuần gọi API VCI, nên extract chạy phía Python rồi
land parquet; các job Spark sau đó chỉ việc đọc parquet -> Iceberg. Giữ Spark image gọn.

WHY dùng boto3 (không s3fs): Airflow đã có boto3 qua provider amazon, thêm s3fs dễ đụng
phiên bản aiobotocore. Ghi parquet ra buffer bằng pyarrow rồi put_object thẳng lên MinIO.
"""
from __future__ import annotations

import argparse
import io
import logging
from datetime import date, datetime, timezone

logger = logging.getLogger(__name__)

# Khớp DEFAULT_SYMBOLS ở spark/jobs/common.py (giữ 2 nơi đồng bộ bằng mắt vì
# Python phía Airflow và phía Spark không share được module).
DEFAULT_SYMBOLS = ["FPT", "VNM", "ACB", "HPG", "MWG"]
LANDING_PREFIX = "bronze/_landing/stock_ohlcv"


def _fetch_symbol(symbol: str, start: str, end: str):
    """Lấy OHLCV ngày của 1 mã. Import vnstock bên trong hàm.

    WHY import lazy: Airflow scheduler parse file plugin khi nạp DAG; nếu import vnstock
    ở top-level mà image chưa cài thì cả DAG lỗi import. Import trong hàm -> chỉ lỗi khi
    task thực sự chạy, và chạy CLI local vẫn ổn.
    """
    from vnstock.api.quote import Quote  # verified 2026-10-27: trả time/open/high/low/close/volume

    df = Quote(symbol=symbol, source="VCI").history(start=start, end=end, interval="1D")
    # vnstock trả cột 'time' (datetime64), open/high/low/close (float), volume (int)
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
    """Kéo OHLCV cho `symbols` trong [start, end] và ghi 1 file parquet lên landing.

    Idempotent: ghi vào key cố định theo `ingest_date` (mặc định hôm nay) nên chạy lại
    cùng ngày sẽ ĐÈ file cũ, không sinh rác. Trả về key S3 đã ghi.
    """
    import boto3
    import pandas as pd

    symbols = symbols or DEFAULT_SYMBOLS
    end = end or date.today().isoformat()
    ingest_date = ingest_date or date.today().isoformat()
    ingested_at = datetime.now(timezone.utc)

    frames = []
    for sym in symbols:
        try:
            df = _fetch_symbol(sym, start, end)
            if df is None or df.empty:
                logger.warning("Mã %s không có dữ liệu trong [%s, %s], bỏ qua.", sym, start, end)
                continue
            frames.append(df)
            logger.info("Kéo %s: %d dòng.", sym, len(df))
        except Exception:  # noqa: BLE001 - log và tiếp tục các mã khác, tránh fail cả batch
            logger.exception("Lỗi khi kéo mã %s, bỏ qua.", sym)

    if not frames:
        raise RuntimeError("Không kéo được dữ liệu cho bất kỳ mã nào — dừng ingest.")

    # Gộp + đóng dấu thời điểm ingest để tầng bronze dedupe theo bản mới nhất.
    data = pd.concat(frames, ignore_index=True)
    data["ingested_at"] = ingested_at
    logger.info("Tổng cộng %d dòng cho %d mã.", len(data), len(frames))

    # Ghi parquet ra buffer (pyarrow) rồi upload — không chạm đĩa cục bộ.
    buf = io.BytesIO()
    data.to_parquet(buf, engine="pyarrow", index=False)
    buf.seek(0)

    key = f"{LANDING_PREFIX}/ingest_date={ingest_date}/data.parquet"
    s3 = boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name="us-east-1",  # MinIO bỏ qua region nhưng boto3 cần có giá trị
    )
    s3.put_object(Bucket=bucket, Key=key, Body=buf.getvalue())
    uri = f"s3://{bucket}/{key}"
    logger.info("Đã ghi landing: %s", uri)
    return key


def _main() -> None:
    """CLI chạy tay: đọc config từ cờ dòng lệnh hoặc env (MINIO_*)."""
    import os

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    p = argparse.ArgumentParser(description="Ingest OHLCV vnstock -> parquet landing trên MinIO")
    p.add_argument("--symbols", default=",".join(DEFAULT_SYMBOLS), help="DS mã, phân tách bằng dấu phẩy")
    p.add_argument("--start", default="2024-01-01")
    p.add_argument("--end", default=None)
    p.add_argument("--endpoint-url", default=os.environ.get("MINIO_ENDPOINT", "http://localhost:9000"))
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
