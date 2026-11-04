"""silver_apply.apply_table on a real Postgres (airflow/dags/lib/silver_apply.py).

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


def stage(conn, rows, start, end, extra_columns=()):
    """What one job_silver run leaves behind: staging.orders + its staging.batch row."""
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
            "INSERT INTO staging.batch (table_name, start_snapshot_id, end_snapshot_id, row_count) "
            "VALUES ('silver.orders', %s, %s, %s) ON CONFLICT (table_name) DO UPDATE SET "
            "start_snapshot_id = EXCLUDED.start_snapshot_id, "
            "end_snapshot_id = EXCLUDED.end_snapshot_id, row_count = EXCLUDED.row_count",
            (start, end, len(rows)),
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


def assert_consistent(conn):
    """The invariants of docs/services.md -> "Checking silver"."""
    (zero_length,) = conn.execute(
        "SELECT count(*) FROM silver.orders WHERE _valid_to = _valid_from"
    ).fetchone()
    (gaps,) = conn.execute(
        "SELECT count(*) FROM (SELECT _valid_to, lead(_valid_from) OVER "
        "(PARTITION BY order_id ORDER BY _lsn) AS nxt FROM silver.orders) x "
        "WHERE _valid_to IS DISTINCT FROM nxt"
    ).fetchone()
    assert (zero_length, gaps) == (0, 0)


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
    rows = [row(1, "pending", 100, 6, T(0))]
    stage(conn, rows, None, 1)
    apply(conn)
    assert "already applied" in apply(conn)
    assert len(versions(conn)) == 1


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
