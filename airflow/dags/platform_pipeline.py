"""DAG platform_pipeline (manual trigger): bronze -> silver -> gold (design §9).

silver_spark: submit spark/jobs/job_silver.py (bronze -> staging) through the Spark REST API.
silver_apply: apply staging to silver as SCD2 and advance the checkpoints (Postgres, one
transaction per table) — airflow/dags/lib/silver_apply.py.
silver_verify: silver invariants + reconciliation with bronze at the checkpoint snapshot (Trino)
— airflow/dags/lib/silver_verify.py.
gold_dbt_run / gold_dbt_test: dbt (dbt/, own virtualenv) rebuilds the gold tables in ClickHouse
from silver, then runs the dbt tests (incl. gold count = silver count).

Param `table`: `all` (default) = every active table in config.etl_checkpoint; or one table,
`users` / `silver.users`. It limits the silver tasks only: gold is always rebuilt in full.
Param `full_refresh` (default false): rebuild ONE table's silver history from the whole bronze
table instead of applying only the new snapshots — after changing the silver transform, or to
repair silver. Needs a specific `table` (not `all`). silver_apply recognises the rebuilt staging
batch by itself and replaces the table in one transaction (lib/silver_apply.py).
From the CLI:
    airflow dags trigger platform_pipeline -c '{"table": "users"}'
    airflow dags trigger platform_pipeline -c '{"table": "users", "full_refresh": true}'

A later phase adds the ES sync next to the gold tasks.
"""

from __future__ import annotations

import pendulum
from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.operators.bash import BashOperator

# dbt runs from the bind-mounted project with the venv's binary; profile, target and log paths come
# from the environment (DBT_PROFILES_DIR, DBT_TARGET_PATH, DBT_LOG_PATH in compose/airflow.yml)
DBT = "cd /opt/airflow/dbt && /opt/airflow/dbt-venv/bin/dbt --no-use-colors"


@dag(
    dag_id="platform_pipeline",
    schedule=None,
    start_date=pendulum.datetime(2026, 11, 1, tz="Asia/Ho_Chi_Minh"),
    catchup=False,
    # two runs at once would race on staging and the checkpoints
    max_active_runs=1,
    # templates render to Python objects, not strings: "{{ params.full_refresh }}" gives the bool
    # False (a string "False" would be truthy). Every param reaches its task the same way, as a
    # task argument rendered from "{{ params.<name> }}"; the dbt bash_commands are plain strings
    render_template_as_native_obj=True,
    params={
        "table": Param(
            "all",
            type="string",
            pattern=r"^(all|(silver\.)?[a-z_][a-z0-9_]*)$",
            description="'all' = every active table in config.etl_checkpoint, "
            "or one table: users / silver.users",
        ),
        "full_refresh": Param(
            False,
            type="boolean",
            description="false = incremental (new bronze snapshots only); true = rebuild the "
            "table's silver history from the whole bronze table (needs a specific table)",
        ),
    },
    tags=["silver", "gold"],
)
def platform_pipeline():
    @task
    def silver_spark(table: str, full_refresh: bool) -> None:
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        from lib.silver_apply import active_tables, resolve_table
        from lib.spark_rest import submit_and_wait

        # guard against losing render_template_as_native_obj: "False" would mean a full refresh
        if not isinstance(full_refresh, bool):
            raise TypeError(f"full_refresh must be a bool, got {full_refresh!r}")
        table = resolve_table(table)
        if full_refresh and table == "all":
            raise ValueError("full_refresh needs a specific table, not 'all'")
        # fail here with a readable error instead of a FAILED Spark driver a minute later
        conn = PostgresHook(postgres_conn_id="postgres_conn").get_conn()
        try:
            active_tables(conn, table)
        finally:
            conn.close()
        # Spark's appArgs are strings: pass the flag only when it is on
        app_args = ["--table", table] + (["--full-refresh"] if full_refresh else [])
        submit_and_wait("job_silver.py", app_args=app_args)

    # no full_refresh here: silver_apply recognises a full-refresh batch from staging.batch
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

    gold_dbt_run = BashOperator(task_id="gold_dbt_run", bash_command=f"{DBT} run")
    gold_dbt_test = BashOperator(task_id="gold_dbt_test", bash_command=f"{DBT} test")

    table = "{{ params.table }}"
    full_refresh = "{{ params.full_refresh }}"
    (
        silver_spark(table, full_refresh)
        >> silver_apply(table)
        >> silver_verify(table)
        >> gold_dbt_run
    )
    gold_dbt_run >> gold_dbt_test


platform_pipeline()
