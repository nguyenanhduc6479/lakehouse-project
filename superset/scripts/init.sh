#!/bin/bash
# Runs at every start of the superset container, before the web server (compose/superset.yml):
# migrate the metadata DB (SQLite), ensure the admin user, sync roles/permissions, then
# bootstrap.py: register the ClickHouse gold database (read-only user superset_reader) and import
# the dashboards kept in git that are missing. Idempotent: a re-run only re-checks. If Superset
# doesn't come up, read this script's output first: `docker compose logs superset`.
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

# database "ClickHouse gold" (fixed uuid) + dashboards from superset/dashboards/: bootstrap.py
python /scripts/bootstrap.py

echo "superset init done."
