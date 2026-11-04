"""DAG platform_pipeline (manual trigger): bronze -> silver (design §9).

silver_spark: submit spark/jobs/job_silver.py (bronze -> staging) through the Spark REST API.
silver_apply: apply staging to silver as SCD2 and advance the checkpoints (Postgres, one
transaction per table) — airflow/dags/lib/silver_apply.py.
silver_verify: silver invariants + reconciliation with bronze at the checkpoint snapshot (Trino)
— airflow/dags/lib/silver_verify.py.

Param `table`: `all` (default) = every active table in config.etl_checkpoint; or one table,
`users` / `silver.users`. From the CLI:
    airflow dags trigger platform_pipeline -c '{"table": "users"}'

Later phases add ES sync and dbt after silver_apply.
"""

from __future__ import annotations

import pendulum
from airflow.decorators import dag, task
from airflow.models.param import Param


@dag(
    dag_id="platform_pipeline",
    schedule=None,
    start_date=pendulum.datetime(2026, 11, 1, tz="Asia/Ho_Chi_Minh"),
    catchup=False,
    # two runs at once would race on staging and the checkpoints
    max_active_runs=1,
    params={
        "table": Param(
            "all",
            type="string",
            pattern=r"^(all|(silver\.)?[a-z_][a-z0-9_]*)$",
            description="'all' = every active table in config.etl_checkpoint, "
            "or one table: users / silver.users",
        )
    },
    tags=["silver"],
)
def platform_pipeline():
    @task
    def silver_spark(table: str) -> None:
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        from lib.silver_apply import active_tables, resolve_table
        from lib.spark_rest import submit_and_wait

        table = resolve_table(table)
        # fail here with a readable error instead of a FAILED Spark driver a minute later
        conn = PostgresHook(postgres_conn_id="postgres_conn").get_conn()
        try:
            active_tables(conn, table)
        finally:
            conn.close()
        submit_and_wait("job_silver.py", app_args=["--table", table])

    @task
    def silver_apply(table: str) -> None:
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        from lib.silver_apply import apply_all, resolve_table

        conn = PostgresHook(postgres_conn_id="postgres_conn").get_conn()
        try:
            apply_all(conn, resolve_table(table))
        finally:
            conn.close()

    @task
    def silver_verify(table: str) -> None:
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        from airflow.providers.trino.hooks.trino import TrinoHook
        from lib.silver_apply import resolve_table
        from lib.silver_verify import verify_all

        trino = TrinoHook(trino_conn_id="trino_conn")
        conn = PostgresHook(postgres_conn_id="postgres_conn").get_conn()
        try:
            verify_all(conn, trino.get_records, resolve_table(table))
        finally:
            conn.close()

    table = "{{ params.table }}"
    silver_spark(table) >> silver_apply(table) >> silver_verify(table)


platform_pipeline()
