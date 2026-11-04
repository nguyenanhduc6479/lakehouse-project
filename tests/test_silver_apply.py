"""SQL built by airflow/dags/lib/silver_apply.py (no database needed)."""

from __future__ import annotations

from lib.silver_apply import build_scd2_sql, quote_ident, quote_table, staging_table_of


def test_quoting():
    assert quote_ident('we"ird') == '"we""ird"'
    assert quote_table("silver.orders") == '"silver"."orders"'
    assert staging_table_of("silver.order_details") == "staging.order_details"


def test_scd2_sql_casts_to_silver_types_and_skips_applied_events():
    close, insert = build_scd2_sql(
        "silver.orders",
        "staging.orders",
        ["order_id"],
        ["order_id", "total_amount", "_valid_from", "_lsn"],
        {
            "order_id": "bigint",
            "total_amount": "numeric(18,2)",
            "_valid_from": "timestamp with time zone",
            "_lsn": "bigint",
        },
    )
    # close: only events newer than the current version, oldest first
    assert "WHERE st._lsn > s._lsn" in close
    assert 'ORDER BY st."order_id", st._lsn' in close
    assert 'UPDATE "silver"."orders" s' in close
    # insert: every value cast to the silver type, already-applied LSNs skipped
    assert 'st."total_amount"::numeric(18,2)' in insert
    assert 'st."_valid_from"::timestamp with time zone' in insert
    assert "AND s._lsn >= st._lsn" in insert


def test_composite_key():
    close, insert = build_scd2_sql(
        "silver.t",
        "staging.t",
        ["a", "b"],
        ["a", "b", "_lsn"],
        {"a": "int", "b": "int", "_lsn": "bigint"},
    )
    assert 's."a" = st."a" AND s."b" = st."b"' in insert
    assert 's."a" = nxt."a" AND s."b" = nxt."b"' in close
