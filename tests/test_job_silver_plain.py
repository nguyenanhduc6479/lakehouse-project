"""job_silver helpers that need no Spark session: table selection, staging.batch merge."""

from __future__ import annotations

import pytest
from job_silver import merge_batches, parse_args, select_checkpoints

ROWS = [{"table_name": "silver.orders"}, {"table_name": "silver.users"}]


def test_all_keeps_every_active_table():
    assert select_checkpoints(ROWS, "all") == ROWS


def test_one_table():
    assert select_checkpoints(ROWS, "silver.users") == [{"table_name": "silver.users"}]


def test_unknown_table_fails_and_lists_the_active_ones():
    with pytest.raises(ValueError, match=r"silver.nope .*silver.orders, silver.users"):
        select_checkpoints(ROWS, "silver.nope")


OLD = "2026-11-04 10:00:00+07"
NOW = "2026-11-04T05:00:00+00:00"


def test_merge_replaces_processed_tables_and_keeps_the_others():
    existing = [
        ("silver.order_details", 1, 2, 5, OLD),
        ("silver.orders", 3, 4, 7, OLD),
        ("silver.users", 5, 6, 0, OLD),
    ]
    new = [("silver.users", 6, 9, 3, NOW)]
    active = ["silver.order_details", "silver.orders", "silver.users"]
    assert merge_batches(existing, new, active) == [
        ("silver.order_details", 1, 2, 5, OLD),
        ("silver.orders", 3, 4, 7, OLD),
        ("silver.users", 6, 9, 3, NOW),
    ]


def test_merge_adds_new_tables_and_drops_inactive_ones():
    existing = [("silver.gone", 1, 2, 5, OLD), ("silver.orders", 3, 4, 7, OLD)]
    new = [("silver.users", None, 9, 3, NOW)]
    assert merge_batches(existing, new, ["silver.orders", "silver.users"]) == [
        ("silver.orders", 3, 4, 7, OLD),
        ("silver.users", None, 9, 3, NOW),
    ]


def test_full_refresh_is_off_by_default():
    args = parse_args(["--table", "silver.users"])
    assert (args.table, args.full_refresh) == ("silver.users", False)


def test_full_refresh_flag():
    assert parse_args(["--table", "silver.users", "--full-refresh"]).full_refresh is True


def test_full_refresh_takes_no_value():
    # a value like "false" would be truthy if it were accepted
    with pytest.raises(SystemExit):
        parse_args(["--full-refresh", "false"])
