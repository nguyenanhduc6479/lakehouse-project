"""DAG orchestrate pipeline lakehouse OHLCV: ingest -> bronze -> silver -> gold.

Chuỗi tuyến tính 4 bước, chạy đúng trên stack sẵn có:
1. ingest : PythonOperator gọi vnstock -> parquet landing trên MinIO (chạy trong Airflow).
2-4. bronze/silver/gold : submit Spark job qua REST 6066 (driver chạy trên spark-worker).

Config MinIO đọc từ env của container Airflow (inject qua compose/airflow.yml từ .env),
KHÔNG hardcode secret trong DAG.
"""
from __future__ import annotations

import os
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from ingest_ohlcv import DEFAULT_SYMBOLS, run_ingest
from spark_rest import submit_and_wait


def _ingest_callable(**_):
    """Wrapper đọc config MinIO từ env rồi gọi run_ingest."""
    run_ingest(
        symbols=DEFAULT_SYMBOLS,
        start="2024-01-01",
        endpoint_url=os.environ["MINIO_ENDPOINT"],
        access_key=os.environ["MINIO_ROOT_USER"],
        secret_key=os.environ["MINIO_ROOT_PASSWORD"],
        bucket=os.environ.get("MINIO_BUCKET", "lakehouse"),
    )


default_args = {"owner": "data-eng", "retries": 1}

with DAG(
    dag_id="lakehouse_ohlcv",
    description="vnstock OHLCV -> bronze -> silver -> gold (Iceberg/Polaris)",
    start_date=datetime(2024, 1, 1),
    schedule="0 18 * * 1-5",  # 18:00 các ngày trong tuần (sau phiên), giờ Asia/Ho_Chi_Minh
    catchup=False,
    default_args=default_args,
    tags=["lakehouse", "medallion", "vnstock"],
) as dag:
    ingest = PythonOperator(task_id="ingest_landing", python_callable=_ingest_callable)
    bronze = PythonOperator(
        task_id="build_bronze",
        python_callable=lambda **_: submit_and_wait("job_bronze.py"),
    )
    silver = PythonOperator(
        task_id="build_silver",
        python_callable=lambda **_: submit_and_wait("job_silver.py"),
    )
    gold = PythonOperator(
        task_id="build_gold",
        python_callable=lambda **_: submit_and_wait("job_gold.py"),
    )

    ingest >> bronze >> silver >> gold
