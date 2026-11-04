"""SQL built by airflow/dags/lib/silver_verify.py (no database needed)."""

from __future__ import annotations

from lib.silver_verify import build_bronze_counts_sql, build_silver_checks_sql


def test_bronze_counts_read_the_checkpoint_snapshot():
    sql = build_bronze_counts_sql("lakehouse.bronze.orders_cdc", ["order_id"], 42)
    assert "FROM lakehouse.bronze.orders_cdc FOR VERSION AS OF 42" in sql
    assert "count(DISTINCT (coalesce(after.order_id, before.order_id), source.txid))" in sql


def test_bronze_counts_composite_key():
    sql = build_bronze_counts_sql("b.t", ["a", "b"], 1)
    assert "(coalesce(after.a, before.a), coalesce(after.b, before.b), source.txid)" in sql


def test_silver_checks_group_and_order_by_key():
    sql = build_silver_checks_sql("silver.order_details", ["order_detail_id"])
    assert 'FROM "silver"."order_details" GROUP BY "order_detail_id"' in sql
    assert 'PARTITION BY "order_detail_id" ORDER BY _lsn' in sql
