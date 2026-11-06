#!/bin/bash
# Runs at every start of the superset container, before the web server (compose/superset.yml):
# migrate the metadata DB (SQLite), ensure the admin user, sync roles/permissions and register the
# ClickHouse gold database (read-only user superset_reader). Idempotent: a re-run only re-checks.
set -euo pipefail

echo "migrating the metadata DB..."
superset db upgrade

echo "ensuring admin user '$SUPERSET_ADMIN_USER'..."
if superset fab list-users 2>/dev/null | grep -q "username:${SUPERSET_ADMIN_USER} "; then
    echo "  -> exists, resetting its password to the .env value"
    superset fab reset-password --username "$SUPERSET_ADMIN_USER" --password "$SUPERSET_ADMIN_PASSWORD"
else
    superset fab create-admin --username "$SUPERSET_ADMIN_USER" --firstname Admin --lastname Superset \
        --email "${SUPERSET_ADMIN_USER}@localhost.invalid" --password "$SUPERSET_ADMIN_PASSWORD"
fi

echo "syncing roles and permissions..."
superset init

# creates the database entry, or updates its URI if it exists
echo "registering database 'ClickHouse gold'..."
superset set-database-uri --database_name "ClickHouse gold" \
    --uri "clickhousedb://superset_reader:${SUPERSET_READER_PASSWORD}@clickhouse:8123/gold"

echo "superset init done."
