"""Print Debezium change events one line each, to watch CDC live.

Reads the JSON lines of kafka-avro-console-consumer on stdin (other lines — the tool's own logs —
are skipped) and prints: operation, table, primary key, transaction id, and for an UPDATE the
columns that changed. Run from the repo root:

    docker run --rm --network lakehouse-project_lakehouse confluentinc/cp-schema-registry:7.6.0 \\
      kafka-avro-console-consumer --bootstrap-server kafka:29092 --include 'src\\.shop\\..*' \\
      --property schema.registry.url=http://schema-registry:8081 \\
      2>/dev/null | python3 -u connect/scripts/watch_events.py

`docker run --rm`, not `docker exec`: Ctrl+C on `docker exec` leaves the consumer running inside
the container.

Avro decimals (money columns) arrive as raw bytes and are decoded with scale 2.
"""

from __future__ import annotations

import json
import sys

OPS = {"c": "INSERT", "u": "UPDATE", "d": "DELETE", "r": "SNAPSHOT"}
DECIMAL_SCALE = 2  # numeric(18,2) in source/schema.sql


def unwrap(value):
    """Avro unions show up as {"type": value}; decimals as a latin-1 string of raw bytes."""
    if isinstance(value, dict) and len(value) == 1:
        value = next(iter(value.values()))
    return value


def row(image: dict | None) -> dict:
    if not image:
        return {}
    fields = next(iter(image.values()))  # {"src.shop.orders.Value": {...}}
    return {k: unwrap(v) for k, v in fields.items()}


def show(value):
    if isinstance(value, str) and value and not value.isprintable():
        raw = value.encode("latin-1")
        return f"{int.from_bytes(raw, 'big', signed=True) / 10**DECIMAL_SCALE:,.2f}"
    return value


def main() -> None:
    for line in sys.stdin:
        if not line.startswith("{"):
            continue
        event = json.loads(line)
        before, after = row(event["before"]), row(event["after"])
        source = event["source"]
        key, key_value = next(iter((after or before).items()))
        changed = ""
        if before and after:
            diffs = [
                f"{k}: {show(before.get(k))} -> {show(v)}"
                for k, v in after.items()
                if before.get(k) != v
            ]
            changed = "  " + ", ".join(diffs)
        print(
            f"{OPS.get(event['op'], event['op']):8} {source['table']:14} {key}={key_value:<6}"
            f" tx={unwrap(source['txId'])}{changed}",
            flush=True,
        )


if __name__ == "__main__":
    main()
