"""events_to_versions: bronze CDC events -> SCD2 version rows (spark/jobs/job_silver.py)."""

from __future__ import annotations

import pytest
from job_silver import events_to_versions

pytestmark = pytest.mark.spark

ROW = "STRUCT<order_id: BIGINT, status: STRING>"
SCHEMA = (
    f"op STRING, before {ROW}, after {ROW}, source STRUCT<lsn: BIGINT, txId: BIGINT, ts_us: BIGINT>"
)

# 2026-11-04 10:00:00 +07:00 in microseconds since the epoch
T0 = 1_793_761_200_000_000
SEC = 1_000_000


def event(op, order_id, status, lsn, tx, ts_us, before_status=None):
    after = None if op == "d" else (order_id, status)
    before = (order_id, before_status if op != "d" else status) if op in ("u", "d") else None
    return (op, before, after, (lsn, tx, ts_us))


def versions(spark, events):
    df = spark.createDataFrame(events, SCHEMA)
    return sorted(
        events_to_versions(df, ["order_id"]).collect(), key=lambda r: (r.order_id, r._lsn)
    )


def test_insert_then_update_in_one_transaction_is_one_version(spark):
    rows = versions(
        spark,
        [
            event("c", 1, "pending", 100, 7, T0),
            event("u", 1, "pending", 101, 7, T0, before_status="pending"),
        ],
    )
    assert [(r._lsn, r._is_current) for r in rows] == [(101, True)]


def test_versions_chain_by_lsn_and_last_is_current(spark):
    rows = versions(
        spark,
        [
            # listed out of order on purpose: ordering must come from the LSN
            event("u", 1, "shipped", 300, 9, T0 + 20 * SEC, before_status="paid"),
            event("c", 1, "pending", 100, 7, T0),
            event("u", 1, "paid", 200, 8, T0 + 10 * SEC, before_status="pending"),
        ],
    )
    assert [r.status for r in rows] == ["pending", "paid", "shipped"]
    assert [r._is_current for r in rows] == [False, False, True]
    assert rows[0]._valid_to == rows[1]._valid_from
    assert rows[1]._valid_to == rows[2]._valid_from
    assert rows[2]._valid_to is None
    assert not any(r._is_deleted for r in rows)


def test_duplicate_events_collapse(spark):
    e = event("c", 1, "pending", 100, 7, T0)
    rows = versions(spark, [e, e])
    assert len(rows) == 1


def test_delete_takes_before_values(spark):
    rows = versions(
        spark,
        [
            event("c", 1, "pending", 100, 7, T0),
            event("d", 1, "pending", 200, 8, T0 + SEC),
        ],
    )
    last = rows[-1]
    assert (last.order_id, last.status, last._is_deleted, last._is_current) == (
        1,
        "pending",
        True,
        True,
    )


def test_keys_are_independent(spark):
    rows = versions(
        spark,
        [event("r", 1, "paid", 50, 5, T0), event("r", 2, "pending", 50, 5, T0)],
    )
    assert [(r.order_id, r._is_current) for r in rows] == [(1, True), (2, True)]


def test_valid_from_is_utc_instant_with_offset(spark):
    (row,) = versions(spark, [event("c", 1, "pending", 100, 7, T0)])
    # session time zone of the fixture is Asia/Ho_Chi_Minh; the text carries the offset
    assert row._valid_from == "2026-11-04 10:00:00.000000+07:00"
