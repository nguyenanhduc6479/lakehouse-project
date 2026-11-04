"""Silver, step 3: verify the tables silver_apply just wrote (task `silver_verify`).

Per active silver table (or only the DAG's `table`):
- invariants inside silver (Postgres), each must be 0:
  - `keys_without_one_current`: keys with no current version or with several;
  - `zero_length_versions`: `_valid_to = _valid_from` (e.g. a transaction split across runs that
    was not merged — design §6.2);
  - `broken_chains`: a version whose `_valid_to` is not the next version's `_valid_from`
    (ordered by `_lsn`), i.e. gaps or overlaps in a key's history;
- reconciliation with bronze (Trino) read **at the checkpoint snapshot** (`FOR VERSION AS OF`),
  so it is exact even while new events keep arriving:
  - silver rows = distinct (key, `source.txid`) in bronze (one version per key per transaction,
    duplicates dropped);
  - current versions = distinct keys in bronze.

Every table is checked, then the task fails if any check failed; the log has one line per table.
Not checked here: silver vs the source DB — only meaningful once the pipeline is drained (manual
check in docs/services.md -> "Checking silver"). The checks scan whole tables (fine at demo size).
"""

from __future__ import annotations

import logging
from collections.abc import Callable

from lib.silver_apply import active_tables, quote_ident, quote_table

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# SQL
# ---------------------------------------------------------------------------

SILVER_CHECKS_TEMPLATE = """
    SELECT
        (SELECT count(*) FROM {silver}) AS rows,
        (SELECT count(*) FROM {silver} WHERE _is_current) AS current_rows,
        (SELECT count(*) FROM {silver} WHERE _is_current AND NOT _is_deleted) AS live_rows,
        (SELECT count(*) FROM (
            SELECT 1 FROM {silver} GROUP BY {keys}
            HAVING count(*) FILTER (WHERE _is_current) <> 1
        ) x) AS keys_without_one_current,
        (SELECT count(*) FROM {silver} WHERE _valid_to = _valid_from) AS zero_length_versions,
        (SELECT count(*) FROM (
            SELECT _valid_to,
                   lead(_valid_from) OVER (PARTITION BY {keys} ORDER BY _lsn) AS next_from
            FROM {silver}
        ) x WHERE _valid_to IS DISTINCT FROM next_from) AS broken_chains
"""

CHECKPOINT_SQL = """
    SELECT last_snapshot_id, source_table
    FROM config.etl_checkpoint
    WHERE pipeline = 'silver' AND table_name = %s
"""

# Trino; snapshot IDs are bigint literals, inlined (Trino's DB-API has no %s parameters here)
BRONZE_COUNTS_TEMPLATE = """
    SELECT count(DISTINCT ({key_row}, source.txid)), count(DISTINCT ({key_row}))
    FROM {source} FOR VERSION AS OF {snapshot}
"""

INVARIANTS = ("keys_without_one_current", "zero_length_versions", "broken_chains")


# ---------------------------------------------------------------------------
# SQL building (pure, unit-tested in tests/test_silver_apply.py)
# ---------------------------------------------------------------------------


def build_silver_checks_sql(silver_table: str, key_columns: list[str]) -> str:
    return SILVER_CHECKS_TEMPLATE.format(
        silver=quote_table(silver_table),
        keys=", ".join(quote_ident(k) for k in key_columns),
    )


def build_bronze_counts_sql(source_table: str, key_columns: list[str], snapshot: int) -> str:
    """Distinct (key, txid) and distinct keys of a bronze table at one snapshot."""
    # Trino lowercases row field names; the key is in `after`, or `before` for deletes
    key_row = ", ".join(f"coalesce(after.{k}, before.{k})" for k in key_columns)
    return BRONZE_COUNTS_TEMPLATE.format(
        key_row=key_row, source=source_table, snapshot=int(snapshot)
    )


# ---------------------------------------------------------------------------
# Verify
# ---------------------------------------------------------------------------


def verify_table(
    cur, trino_query: Callable[[str], list], silver_table: str, key_columns: list[str]
) -> tuple[dict[str, int], list[str]]:
    """Run every check of one table; returns (all counts, names of the failed checks)."""
    cur.execute(build_silver_checks_sql(silver_table, key_columns))
    names = [d[0] for d in cur.description]
    counts = dict(zip(names, cur.fetchone(), strict=True))
    failed = [name for name in INVARIANTS if counts[name] != 0]

    cur.execute(CHECKPOINT_SQL, (silver_table,))
    snapshot, source_table = cur.fetchone()
    if snapshot is None:
        # never applied: silver must still be empty
        if counts["rows"]:
            failed.append("rows_without_checkpoint")
        return counts, failed

    ((bronze_versions, bronze_keys),) = trino_query(
        build_bronze_counts_sql(source_table, key_columns, snapshot)
    )
    counts["bronze_versions"], counts["bronze_keys"] = bronze_versions, bronze_keys
    if counts["rows"] != bronze_versions:
        failed.append("rows_vs_bronze_versions")
    if counts["current_rows"] != bronze_keys:
        failed.append("current_rows_vs_bronze_keys")
    return counts, failed


def verify_all(conn, trino_query: Callable[[str], list], table: str = "all") -> None:
    """Verify every active silver table (or only `table`); raise if any check failed."""
    problems = []
    for silver_table, key_columns in active_tables(conn, table):
        with conn.cursor() as cur:
            counts, failed = verify_table(cur, trino_query, silver_table, list(key_columns))
        conn.commit()
        summary = ", ".join(f"{k}={v}" for k, v in counts.items())
        if failed:
            logger.error("%s: FAILED %s (%s)", silver_table, ", ".join(failed), summary)
            problems.append(f"{silver_table}: {', '.join(failed)}")
        else:
            logger.info("%s: OK (%s)", silver_table, summary)
    if problems:
        raise RuntimeError("silver_verify failed — " + "; ".join(problems))
