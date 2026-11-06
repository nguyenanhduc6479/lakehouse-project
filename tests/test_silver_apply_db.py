"""silver_apply and silver_verify on a real Postgres (airflow/dags/lib/silver_*.py).

Runs against a scratch database `silver_apply_test` created from postgres/schema.sql in the
`postgres` container (host port 5432) and dropped afterwards — the real `data-platform` DB is
never touched. Needs the stack up and the .env credentials in the environment:

    set -a && . ./.env && set +a && venv/bin/python -m pytest -m postgres

Skipped when Postgres is not reachable. Staging rows are written by hand in the shape
spark/jobs/job_silver.py produces (text validity bounds, numeric(38,2) money).
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest
from lib.silver_apply import apply_all, apply_table
from lib.silver_verify import INVARIANTS, verify_table

psycopg = pytest.importorskip("psycopg")

pytestmark = pytest.mark.postgres

TEST_DB = "silver_apply_test"
SCHEMA_SQL = Path(__file__).resolve().parents[1] / "postgres" / "schema.sql"
STAGING_DDL = """
    CREATE TABLE staging.orders (
        order_id bigint, status text, total_amount numeric(38,2){extra},
        _valid_from text, _valid_to text, _is_current boolean, _is_deleted boolean,
        _lsn bigint, _tx_id bigint
    )
"""


def T(second: int) -> str:
    return f"2026-11-04 10:00:{second:02d}.000000+07:00"


def row(order_id, status, lsn, tx, valid_from, valid_to=None, deleted=False, total="10.00"):
    """A staging version row; current when it has no _valid_to (as job_silver sets it)."""
    return (order_id, status, total, valid_from, valid_to, valid_to is None, deleted, lsn, tx)


def _connect(dbname: str, **kwargs):
    return psycopg.connect(
        host=os.environ.get("PGHOST", "localhost"),
        port=int(os.environ.get("PGPORT", "5432")),
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
        dbname=dbname,
        connect_timeout=3,
        **kwargs,
    )


@pytest.fixture(scope="module")
def db():
    if "POSTGRES_USER" not in os.environ or "POSTGRES_PASSWORD" not in os.environ:
        pytest.skip("POSTGRES_USER / POSTGRES_PASSWORD not set (source .env)")
    try:
        admin = _connect(os.environ.get("POSTGRES_DB", "data-platform"), autocommit=True)
    except psycopg.OperationalError as e:
        pytest.skip(f"Postgres not reachable: {e}")
    admin.execute(f"DROP DATABASE IF EXISTS {TEST_DB} WITH (FORCE)")
    admin.execute(f"CREATE DATABASE {TEST_DB}")
    conn = _connect(TEST_DB)
    conn.execute(SCHEMA_SQL.read_text())
    conn.commit()
    yield conn
    conn.close()
    admin.execute(f"DROP DATABASE {TEST_DB} WITH (FORCE)")
    admin.close()


@pytest.fixture
def conn(db):
    db.execute("TRUNCATE silver.orders, staging.batch")
    db.execute("DROP TABLE IF EXISTS staging.orders")
    db.execute(
        "UPDATE config.etl_checkpoint SET last_snapshot_id = NULL "
        "WHERE pipeline = 'silver' AND table_name = 'silver.orders'"
    )
    db.commit()
    return db


def stage(conn, rows, start, end, extra_columns=(), full_refresh=False):
    """What one job_silver run leaves behind: staging.orders + its staging.batch row
    (`full_refresh=True`: a `--full-refresh` run, which also stages from no start)."""
    extra = "".join(f", {name} {kind}" for name, kind, _ in extra_columns)
    conn.execute("DROP TABLE IF EXISTS staging.orders")
    conn.execute(STAGING_DDL.format(extra=extra))
    columns = ["order_id", "status", "total_amount"] + [name for name, _, _ in extra_columns]
    columns += ["_valid_from", "_valid_to", "_is_current", "_is_deleted", "_lsn", "_tx_id"]
    with conn.cursor() as cur:
        cur.executemany(
            f"INSERT INTO staging.orders ({', '.join(columns)}) "
            f"VALUES ({', '.join(['%s'] * len(columns))})",
            [r[:3] + tuple(v for _, _, v in extra_columns) + r[3:] for r in rows],
        )
        cur.execute(
            "INSERT INTO staging.batch "
            "(table_name, start_snapshot_id, end_snapshot_id, row_count, full_refresh) "
            "VALUES ('silver.orders', %s, %s, %s, %s) ON CONFLICT (table_name) DO UPDATE SET "
            "start_snapshot_id = EXCLUDED.start_snapshot_id, "
            "end_snapshot_id = EXCLUDED.end_snapshot_id, row_count = EXCLUDED.row_count, "
            "full_refresh = EXCLUDED.full_refresh",
            (start, end, len(rows), full_refresh),
        )
    conn.commit()


def apply(conn) -> str:
    try:
        with conn.cursor() as cur:
            summary = apply_table(cur, "silver.orders", ["order_id"])
        conn.commit()
        return summary
    except Exception:
        conn.rollback()
        raise


def versions(conn):
    return conn.execute(
        "SELECT order_id, status, to_char(_valid_from AT TIME ZONE 'Asia/Ho_Chi_Minh', 'SS'), "
        "to_char(_valid_to AT TIME ZONE 'Asia/Ho_Chi_Minh', 'SS'), _is_current, _is_deleted, _lsn "
        "FROM silver.orders ORDER BY order_id, _lsn"
    ).fetchall()


def checkpoint(conn):
    return conn.execute(
        "SELECT last_snapshot_id FROM config.etl_checkpoint WHERE table_name = 'silver.orders'"
    ).fetchone()[0]


def verify(conn, bronze=None):
    """silver_verify on silver.orders; `bronze` = what Trino would return (versions, keys)."""
    calls = []

    def trino_query(sql):
        calls.append(sql)
        return [bronze]

    with conn.cursor() as cur:
        counts, failed = verify_table(cur, trino_query, "silver.orders", ["order_id"])
    conn.commit()
    return counts, failed, calls


def assert_consistent(conn):
    """silver_verify's invariants (one current per key, no zero-length version, no gap)."""
    counts, _, _ = verify(conn, bronze=(0, 0))
    assert {name: counts[name] for name in INVARIANTS} == dict.fromkeys(INVARIANTS, 0)


def test_first_batch_inserts_the_chain(conn):
    stage(conn, [row(1, "pending", 100, 6, T(0), T(10)), row(1, "paid", 200, 7, T(10))], None, 1)
    assert apply(conn).endswith("0 superseded, 0 versions closed, 2 inserted")
    assert versions(conn) == [
        (1, "pending", "00", "10", False, False, 100),
        (1, "paid", "10", None, True, False, 200),
    ]
    assert checkpoint(conn) == 1
    assert_consistent(conn)


def test_values_are_cast_to_silver_types(conn):
    stage(conn, [row(1, "pending", 100, 6, T(0), total="12.34")], None, 1)
    apply(conn)
    total, kind = conn.execute(
        "SELECT total_amount, pg_typeof(total_amount)::text FROM silver.orders"
    ).fetchone()
    assert (str(total), kind) == ("12.34", "numeric")


def test_same_batch_again_is_skipped(conn):
    stage(conn, [row(1, "pending", 100, 6, T(0))], None, 1)
    apply(conn)
    stage(conn, [row(1, "paid", 200, 7, T(10))], 1, 2)
    apply(conn)
    assert "already applied" in apply(conn)
    assert len(versions(conn)) == 2


def test_first_batch_again_is_skipped(conn):
    # no start snapshot, like a full refresh, but not flagged as one: a plain applied batch
    stage(conn, [row(1, "pending", 100, 6, T(0), T(10)), row(1, "paid", 200, 7, T(10))], None, 1)
    apply(conn)
    stamps = conn.execute("SELECT _silver_updated_at FROM silver.orders ORDER BY _lsn").fetchall()
    assert "already applied" in apply(conn)
    assert (
        conn.execute("SELECT _silver_updated_at FROM silver.orders ORDER BY _lsn").fetchall()
        == stamps
    )


def test_rewound_checkpoint_replays_without_changes(conn):
    rows = [row(1, "pending", 100, 6, T(0), T(10)), row(1, "paid", 200, 7, T(10))]
    stage(conn, rows, None, 1)
    apply(conn)
    before = versions(conn)
    conn.execute("UPDATE config.etl_checkpoint SET last_snapshot_id = NULL")
    conn.commit()
    stage(conn, rows, None, 1)
    assert apply(conn).endswith("0 superseded, 0 versions closed, 0 inserted")
    assert versions(conn) == before


def test_overlapping_batch_closes_once(conn):
    stage(conn, [row(1, "pending", 100, 6, T(0))], None, 1)
    apply(conn)
    # the next run re-reads event 100 (e.g. a Debezium duplicate) next to a new one
    stage(conn, [row(1, "pending", 100, 6, T(0), T(10)), row(1, "paid", 200, 7, T(10))], 1, 2)
    assert apply(conn).endswith("0 superseded, 1 versions closed, 1 inserted")
    assert [(r[1], r[4]) for r in versions(conn)] == [("pending", False), ("paid", True)]
    assert checkpoint(conn) == 2
    assert_consistent(conn)


def test_delete_ends_with_a_deleted_current_version(conn):
    stage(conn, [row(1, "pending", 100, 6, T(0))], None, 1)
    apply(conn)
    stage(conn, [row(1, "pending", 200, 7, T(10), deleted=True)], 1, 2)
    apply(conn)
    assert versions(conn)[-1] == (1, "pending", "10", None, True, True, 200)
    assert_consistent(conn)


def test_transaction_split_across_two_runs_leaves_no_zero_length_version(conn):
    # tx 7 = two updates of order 1 at commit time T(10); the sink committed the first update in
    # one snapshot and the second in the next, and a silver run happened in between
    stage(conn, [row(1, "pending", 100, 6, T(0), T(10)), row(1, "paid", 200, 7, T(10))], None, 1)
    apply(conn)
    stage(conn, [row(1, "paid", 201, 7, T(10), total="99.00")], 1, 2)
    assert apply(conn).endswith("1 superseded, 0 versions closed, 1 inserted")
    assert versions(conn) == [
        (1, "pending", "00", "10", False, False, 100),
        (1, "paid", "10", None, True, False, 201),
    ]
    assert_consistent(conn)


def test_stale_batch_fails_and_changes_nothing(conn):
    stage(conn, [row(1, "pending", 100, 6, T(0))], None, 1)
    apply(conn)
    stage(conn, [row(1, "paid", 200, 7, T(10))], 5, 6)
    with pytest.raises(RuntimeError, match="checkpoint is 1"):
        apply(conn)
    assert [r[6] for r in versions(conn)] == [100]
    assert checkpoint(conn) == 1


def test_new_staging_column_is_added_to_silver(conn):
    stage(conn, [row(1, "pending", 100, 6, T(0))], None, 1, extra_columns=[("coupon", "text", "X")])
    apply(conn)
    assert conn.execute("SELECT coupon FROM silver.orders").fetchone() == ("X",)
    conn.execute("ALTER TABLE silver.orders DROP COLUMN coupon")
    conn.commit()


def test_apply_all_for_one_table(conn):
    stage(conn, [row(1, "pending", 100, 6, T(0))], None, 1)
    apply_all(conn, "silver.orders")
    assert checkpoint(conn) == 1
    with pytest.raises(ValueError, match="silver.nope is not an active silver table"):
        apply_all(conn, "silver.nope")


def test_full_refresh_rebuilds_a_damaged_history(conn):
    stage(conn, [row(1, "pending", 100, 6, T(0), T(10)), row(1, "paid", 200, 7, T(10))], None, 1)
    apply(conn)
    stage(conn, [row(2, "pending", 300, 8, T(20))], 1, 2)
    apply(conn)
    conn.execute("UPDATE silver.orders SET status = 'broken'")
    conn.execute("DELETE FROM silver.orders WHERE order_id = 2")
    conn.commit()
    # job_silver --full-refresh: the whole bronze table up to snapshot 3, no start
    rows = [
        row(1, "pending", 100, 6, T(0), T(10)),
        row(1, "paid", 200, 7, T(10)),
        row(2, "pending", 300, 8, T(20), T(30)),
        row(2, "shipped", 400, 9, T(30)),
    ]
    stage(conn, rows, None, 3, full_refresh=True)
    summary = apply(conn)
    assert "FULL REFRESH" in summary
    assert summary.endswith("2 old rows deleted, 4 staged, 4 inserted")
    assert versions(conn) == [
        (1, "pending", "00", "10", False, False, 100),
        (1, "paid", "10", None, True, False, 200),
        (2, "pending", "20", "30", False, False, 300),
        (2, "shipped", "30", None, True, False, 400),
    ]
    assert checkpoint(conn) == 3
    assert_consistent(conn)


def test_full_refresh_runs_without_a_new_bronze_snapshot(conn):
    rows = [row(1, "pending", 100, 6, T(0))]
    stage(conn, rows, None, 1)
    apply(conn)
    stage(conn, [row(1, "paid", 200, 7, T(10))], 1, 2)
    apply(conn)
    conn.execute("UPDATE silver.orders SET status = 'broken'")
    conn.commit()
    # bronze still at snapshot 2 = the checkpoint: must not be skipped as "already applied"
    stage(
        conn,
        [row(1, "pending", 100, 6, T(0), T(10)), row(1, "paid", 200, 7, T(10))],
        None,
        2,
        full_refresh=True,
    )
    assert "FULL REFRESH" in apply(conn)
    assert [r[1] for r in versions(conn)] == ["pending", "paid"]
    assert checkpoint(conn) == 2


def test_full_refresh_is_applied_once(conn):
    stage(conn, [row(1, "pending", 100, 6, T(0))], None, 1)
    apply(conn)
    stage(conn, [row(1, "pending", 100, 6, T(0))], None, 1, full_refresh=True)
    assert "FULL REFRESH" in apply(conn)
    flag = "SELECT full_refresh FROM staging.batch WHERE table_name = 'silver.orders'"
    assert conn.execute(flag).fetchone() == (False,)
    # a cleared silver_apply re-runs on the same batch row: nothing happens this time
    assert "already applied" in apply(conn)


def test_full_refresh_that_fails_leaves_silver_untouched(conn):
    stage(conn, [row(1, "pending", 100, 6, T(0))], None, 1)
    apply(conn)
    stage(conn, [row(1, "paid", 200, 7, T(10))], None, 2, full_refresh=True)
    conn.execute("DROP TABLE staging.orders")  # batch says 1 row, staging table is gone
    conn.commit()
    with pytest.raises(RuntimeError, match="staging.orders is missing"):
        apply(conn)
    assert [r[6] for r in versions(conn)] == [100]
    assert checkpoint(conn) == 1


def test_full_refresh_without_a_bronze_snapshot_is_refused(conn):
    stage(conn, [row(1, "pending", 100, 6, T(0))], None, 1)
    apply(conn)
    stage(conn, [], None, None, full_refresh=True)  # bronze table recreated empty
    with pytest.raises(RuntimeError, match="refusing to empty silver"):
        apply(conn)
    assert len(versions(conn)) == 1
    assert checkpoint(conn) == 1


def insert_silver(conn, rows):
    """Write versions straight into silver (bypassing the apply) to build broken histories."""
    with conn.cursor() as cur:
        cur.executemany(
            "INSERT INTO silver.orders (order_id, status, _valid_from, _valid_to, _is_current, "
            "_lsn, _tx_id) VALUES (%s, 'x', %s, %s, %s, %s, %s)",
            rows,
        )
    conn.execute("UPDATE config.etl_checkpoint SET last_snapshot_id = 1")
    conn.commit()


def test_verify_passes_after_a_normal_apply(conn):
    stage(conn, [row(1, "pending", 100, 6, T(0), T(10)), row(1, "paid", 200, 7, T(10))], None, 1)
    apply(conn)
    counts, failed, calls = verify(conn, bronze=(2, 1))
    assert failed == []
    assert (counts["rows"], counts["current_rows"], counts["bronze_versions"]) == (2, 1, 2)
    assert "FOR VERSION AS OF 1" in calls[0]


def test_verify_finds_broken_histories(conn):
    insert_silver(
        conn,
        [
            # key 1: zero-length version before the current one
            (1, T(10), T(10), False, 100, 6),
            (1, T(10), None, True, 101, 7),
            # key 2: no current version at all
            (2, T(0), T(5), False, 200, 8),
            # key 3: gap between 10 and 20
            (3, T(0), T(10), False, 300, 9),
            (3, T(20), None, True, 301, 10),
        ],
    )
    # bronze agrees with silver's counts, so only the history invariants can fail
    _, failed, _ = verify(conn, bronze=(5, 2))
    assert set(failed) == {"keys_without_one_current", "zero_length_versions", "broken_chains"}


def test_verify_finds_a_bronze_mismatch(conn):
    stage(conn, [row(1, "pending", 100, 6, T(0))], None, 1)
    apply(conn)
    _, failed, _ = verify(conn, bronze=(3, 2))
    assert failed == ["rows_vs_bronze_versions", "current_rows_vs_bronze_keys"]


def test_verify_before_the_first_apply_skips_bronze(conn):
    counts, failed, calls = verify(conn)
    assert (counts["rows"], failed, calls) == (0, [], [])
