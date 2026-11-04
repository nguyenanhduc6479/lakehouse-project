"""job_silver --table: which checkpoint rows a run processes (no Spark session needed)."""

from __future__ import annotations

import pytest
from job_silver import select_checkpoints

ROWS = [{"table_name": "silver.orders"}, {"table_name": "silver.users"}]


def test_all_keeps_every_active_table():
    assert select_checkpoints(ROWS, "all") == ROWS


def test_one_table():
    assert select_checkpoints(ROWS, "silver.users") == [{"table_name": "silver.users"}]


def test_unknown_table_fails_and_lists_the_active_ones():
    with pytest.raises(ValueError, match=r"silver.nope .*silver.orders, silver.users"):
        select_checkpoints(ROWS, "silver.nope")
