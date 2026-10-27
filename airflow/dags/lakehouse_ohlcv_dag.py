"""DAG orchestrating the lakehouse OHLCV pipeline: ingest -> bronze -> silver -> gold.

A linear 4-step chain that runs on the existing stack:
1. ingest : PythonOperator calls vnstock -> landing parquet on MinIO (runs inside Airflow).
2-4. bronze/silver/gold : submit Spark jobs via REST 6066 (driver runs on spark-worker).

MinIO config is read from the Airflow container's env (injected via compose/airflow.yml
from .env), NO secrets hardcoded in the DAG.
"""

from __future__ import annotations

import os
from datetime import datetime

from airflow.operators.python import PythonOperator
from ingest_ohlcv import DEFAULT_SYMBOLS, run_ingest
from spark_rest import submit_and_wait

from airflow import DAG


def _ingest_callable(**_):
    """Wrapper that reads MinIO config from env, then calls run_ingest."""
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
    schedule="0 18 * * 1-5",  # 18:00 on weekdays (after the session), Asia/Ho_Chi_Minh time
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
