"""Silver, step 2 of 2: apply staging.<table> to silver.<table> as SCD2 (design §6.4).

Runs in Airflow (task `silver_apply` of DAG `platform_pipeline`) after spark/jobs/job_silver.py.
Per table, in ONE Postgres transaction:
1. check that staging.batch describes a range starting at the current checkpoint (otherwise the
   staging data is stale or belongs to another start point — fail instead of guessing);
2. add staging columns missing from silver (`ALTER TABLE ... ADD COLUMN`, staging's type);
3. close the current versions that have a newer staging event, then insert the staging versions
   silver doesn't have yet (both skip anything at or below silver's `_lsn` -> idempotent);
4. move the checkpoint to the snapshot staging was read up to.
Either all of it commits or nothing does. Tables are applied independently (silver is
eventually consistent across tables): one failing table does not stop the others, but the task
fails at the end.

Staging values are cast to the silver column types in SQL (`st.col::<type>`): staging holds what
Spark wrote (text timestamps, numeric(38,2) money), silver keeps the source types.

`apply_all` takes any DB-API connection (psycopg2 from PostgresHook in Airflow).
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# SQL
# ---------------------------------------------------------------------------

ACTIVE_TABLES_SQL = """
    SELECT table_name, key_columns
    FROM config.etl_checkpoint
    WHERE pipeline = 'silver' AND is_active
    ORDER BY table_name
"""

BATCH_SQL = """
    SELECT start_snapshot_id, end_snapshot_id, row_count
    FROM staging.batch
    WHERE table_name = %s
"""

# FOR UPDATE: a concurrent apply of the same table waits instead of racing on the checkpoint
CHECKPOINT_SQL = """
    SELECT last_snapshot_id
    FROM config.etl_checkpoint
    WHERE pipeline = 'silver' AND table_name = %s
    FOR UPDATE
"""

ADVANCE_CHECKPOINT_SQL = """
    UPDATE config.etl_checkpoint
    SET last_snapshot_id = %s, updated_at = now()
    WHERE pipeline = 'silver' AND table_name = %s
"""

# column name -> full type (e.g. numeric(18,2)) of a table, in column order
COLUMNS_SQL = """
    SELECT a.attname, format_type(a.atttypid, a.atttypmod)
    FROM pg_attribute a
    WHERE a.attrelid = to_regclass(%s) AND a.attnum > 0 AND NOT a.attisdropped
    ORDER BY a.attnum
"""

# 1) close the current version with the OLDEST staging event newer than it. Staging may also hold
#    already-applied events (re-runs, Debezium duplicates), hence the per-key `_lsn` comparison.
CLOSE_TEMPLATE = """
    WITH nxt AS (
        SELECT DISTINCT ON ({st_keys}) {st_keys}, st."_valid_from"::timestamptz AS _valid_from
        FROM {staging} st
        JOIN {silver} s ON {on_keys} AND s._is_current
        WHERE st._lsn > s._lsn
        ORDER BY {st_keys}, st._lsn
    )
    UPDATE {silver} s
    SET _valid_to = nxt._valid_from, _is_current = false, _silver_updated_at = now()
    FROM nxt
    WHERE {on_nxt_keys} AND s._is_current
"""

# 2) insert the staging versions silver doesn't have yet (anything at or below silver's `_lsn`
#    for that key is already applied or superseded -> idempotent)
INSERT_TEMPLATE = """
    INSERT INTO {silver} ({columns}, _silver_updated_at)
    SELECT {values}, now()
    FROM {staging} st
    WHERE NOT EXISTS (SELECT 1 FROM {silver} s WHERE {on_keys} AND s._lsn >= st._lsn)
"""


# ---------------------------------------------------------------------------
# SQL building (pure, unit-tested in tests/test_silver_apply.py)
# ---------------------------------------------------------------------------


def quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def quote_table(qualified: str) -> str:
    """'silver.orders' -> '"silver"."orders"'."""
    return ".".join(quote_ident(part) for part in qualified.split("."))


def staging_table_of(silver_table: str) -> str:
    return "staging." + silver_table.split(".", 1)[1]


def build_scd2_sql(
    silver_table: str,
    staging_table: str,
    key_columns: list[str],
    staging_columns: list[str],
    silver_types: dict[str, str],
) -> tuple[str, str]:
    """The two SCD2 statements (close, insert) for one table.

    `staging_columns`: staging's columns in order (source columns + technical ones);
    `silver_types`: silver column -> type, used to cast every staging value.
    """
    keys = [quote_ident(k) for k in key_columns]
    names = {
        "silver": quote_table(silver_table),
        "staging": quote_table(staging_table),
        "st_keys": ", ".join(f"st.{k}" for k in keys),
        "on_keys": " AND ".join(f"s.{k} = st.{k}" for k in keys),
        "on_nxt_keys": " AND ".join(f"s.{k} = nxt.{k}" for k in keys),
    }
    close = CLOSE_TEMPLATE.format(**names)
    insert = INSERT_TEMPLATE.format(
        **names,
        columns=", ".join(quote_ident(c) for c in staging_columns),
        values=", ".join(f"st.{quote_ident(c)}::{silver_types[c]}" for c in staging_columns),
    )
    return close, insert


# ---------------------------------------------------------------------------
# Apply
# ---------------------------------------------------------------------------


def _columns(cur, table: str) -> dict[str, str]:
    cur.execute(COLUMNS_SQL, (table,))
    return dict(cur.fetchall())


def _add_missing_columns(cur, silver_table: str, staging_types: dict[str, str]) -> dict[str, str]:
    """ALTER TABLE ... ADD COLUMN for staging columns silver lacks; returns silver's types."""
    silver_types = _columns(cur, silver_table)
    for column, column_type in staging_types.items():
        if column not in silver_types:
            logger.info("%s: new column %s %s", silver_table, column, column_type)
            cur.execute(
                f"ALTER TABLE {quote_table(silver_table)} "
                f"ADD COLUMN {quote_ident(column)} {column_type}"
            )
            silver_types[column] = column_type
    return silver_types


def apply_table(cur, silver_table: str, key_columns: list[str]) -> str:
    """Apply one table inside the caller's transaction; returns a one-line summary."""
    cur.execute(BATCH_SQL, (silver_table,))
    batch = cur.fetchone()
    if batch is None:
        return f"{silver_table}: no staging batch, skipped"
    start, end, rows = batch

    cur.execute(CHECKPOINT_SQL, (silver_table,))
    checkpoint = cur.fetchone()[0]
    if end == checkpoint:
        return f"{silver_table}: batch already applied (checkpoint {checkpoint}), skipped"
    if start != checkpoint:
        raise RuntimeError(
            f"{silver_table}: staging batch starts after snapshot {start} but the checkpoint is "
            f"{checkpoint}; re-run silver_spark to rebuild staging"
        )

    closed = inserted = 0
    if rows:
        staging_table = staging_table_of(silver_table)
        staging_types = _columns(cur, staging_table)
        if not staging_types:
            raise RuntimeError(
                f"{silver_table}: batch has {rows} rows but {staging_table} is missing"
            )
        silver_types = _add_missing_columns(cur, silver_table, staging_types)

        close, insert = build_scd2_sql(
            silver_table, staging_table, key_columns, list(staging_types), silver_types
        )
        cur.execute(close)
        closed = cur.rowcount
        cur.execute(insert)
        inserted = cur.rowcount

    cur.execute(ADVANCE_CHECKPOINT_SQL, (end, silver_table))
    return (
        f"{silver_table}: snapshots ({start}, {end}], {rows} staged, "
        f"{closed} versions closed, {inserted} inserted"
    )


def apply_all(conn) -> None:
    """Apply every active silver table, one transaction each; raise if any failed."""
    with conn.cursor() as cur:
        cur.execute(ACTIVE_TABLES_SQL)
        tables = cur.fetchall()
    conn.commit()

    failed = []
    for silver_table, key_columns in tables:
        try:
            with conn.cursor() as cur:
                summary = apply_table(cur, silver_table, list(key_columns))
            conn.commit()
            logger.info(summary)
        except Exception:
            conn.rollback()
            logger.exception("%s: apply failed, rolled back", silver_table)
            failed.append(silver_table)
    if failed:
        raise RuntimeError(f"silver_apply failed for: {', '.join(failed)}")
