"""DAG platform_pipeline (manual trigger): bronze -> silver (design §9).

silver_spark: submit spark/jobs/job_silver.py (bronze -> staging) through the Spark REST API.
silver_apply: apply staging to silver as SCD2 and advance the checkpoints (Postgres, one
transaction per table) — airflow/plugins/silver_apply.py.

Later phases add ES sync and dbt after silver_apply.
"""

from __future__ import annotations

import pendulum
from airflow.decorators import dag, task


@dag(
    dag_id="platform_pipeline",
    schedule=None,
    start_date=pendulum.datetime(2026, 11, 1, tz="Asia/Ho_Chi_Minh"),
    catchup=False,
    # two runs at once would race on staging and the checkpoints
    max_active_runs=1,
    tags=["silver"],
)
def platform_pipeline():
    @task
    def silver_spark() -> None:
        from spark_rest import submit_and_wait

        submit_and_wait("job_silver.py")

    @task
    def silver_apply() -> None:
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        from silver_apply import apply_all

        conn = PostgresHook(postgres_conn_id="postgres_conn").get_conn()
        try:
            apply_all(conn)
        finally:
            conn.close()

    silver_spark() >> silver_apply()


platform_pipeline()
