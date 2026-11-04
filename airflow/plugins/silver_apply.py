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

COLUMNS_SQL = """
SELECT a.attname, format_type(a.atttypid, a.atttypmod)
FROM pg_attribute a
WHERE a.attrelid = to_regclass(%s) AND a.attnum > 0 AND NOT a.attisdropped
ORDER BY a.attnum
"""


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
    silver, staging = quote_table(silver_table), quote_table(staging_table)
    keys = [quote_ident(k) for k in key_columns]
    on_keys = " AND ".join(f"s.{k} = st.{k}" for k in keys)
    st_keys = ", ".join(f"st.{k}" for k in keys)

    close = f"""WITH nxt AS (
  SELECT DISTINCT ON ({st_keys}) {st_keys}, st."_valid_from"::timestamptz AS _valid_from
  FROM {staging} st
  JOIN {silver} s ON {on_keys} AND s._is_current
  WHERE st._lsn > s._lsn
  ORDER BY {st_keys}, st._lsn
)
UPDATE {silver} s
SET _valid_to = nxt._valid_from, _is_current = false, _silver_updated_at = now()
FROM nxt
WHERE {" AND ".join(f"s.{k} = nxt.{k}" for k in keys)} AND s._is_current"""

    columns = ", ".join(quote_ident(c) for c in staging_columns)
    values = ", ".join(f"st.{quote_ident(c)}::{silver_types[c]}" for c in staging_columns)
    insert = f"""INSERT INTO {silver} ({columns}, _silver_updated_at)
SELECT {values}, now()
FROM {staging} st
WHERE NOT EXISTS (SELECT 1 FROM {silver} s WHERE {on_keys} AND s._lsn >= st._lsn)"""
    return close, insert


def _columns(cur, table: str) -> dict[str, str]:
    cur.execute(COLUMNS_SQL, (table,))
    return dict(cur.fetchall())


def apply_table(cur, silver_table: str, key_columns: list[str]) -> str:
    """Apply one table inside the caller's transaction; returns a one-line summary."""
    cur.execute(
        "SELECT start_snapshot_id, end_snapshot_id, row_count FROM staging.batch "
        "WHERE table_name = %s",
        (silver_table,),
    )
    batch = cur.fetchone()
    if batch is None:
        return f"{silver_table}: no staging batch, skipped"
    start, end, rows = batch

    cur.execute(
        "SELECT last_snapshot_id FROM config.etl_checkpoint "
        "WHERE pipeline = 'silver' AND table_name = %s FOR UPDATE",
        (silver_table,),
    )
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
        silver_types = _columns(cur, silver_table)
        for column, column_type in staging_types.items():
            if column not in silver_types:
                logger.info("%s: new column %s %s", silver_table, column, column_type)
                cur.execute(
                    f"ALTER TABLE {quote_table(silver_table)} "
                    f"ADD COLUMN {quote_ident(column)} {column_type}"
                )
                silver_types[column] = column_type
        close, insert = build_scd2_sql(
            silver_table, staging_table, key_columns, list(staging_types), silver_types
        )
        cur.execute(close)
        closed = cur.rowcount
        cur.execute(insert)
        inserted = cur.rowcount

    cur.execute(
        "UPDATE config.etl_checkpoint SET last_snapshot_id = %s, updated_at = now() "
        "WHERE pipeline = 'silver' AND table_name = %s",
        (end, silver_table),
    )
    return (
        f"{silver_table}: snapshots ({start}, {end}], {rows} staged, "
        f"{closed} versions closed, {inserted} inserted"
    )


def apply_all(conn) -> None:
    """Apply every active silver table, one transaction each; raise if any failed."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT table_name, key_columns FROM config.etl_checkpoint "
            "WHERE pipeline = 'silver' AND is_active ORDER BY table_name"
        )
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
