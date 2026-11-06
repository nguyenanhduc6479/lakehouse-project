"""Superset bootstrap, run by init.sh at every start (after `superset init`), inside the container.

1. Registers the database **ClickHouse gold** (read-only user `superset_reader`) with a FIXED
   uuid. Exported dashboards reference their database by uuid, with the password masked; on an
   empty volume the importer only accepts that if a database with the same uuid already exists
   (otherwise: "Must provide a password for the database"). `superset set-database-uri` would
   give a random uuid, so the database is created here instead. The password is URL-encoded, so
   any character is safe in `SUPERSET_READER_PASSWORD`.
2. Imports the dashboards kept in git: `superset/dashboards/` (mounted at /dashboards) holds an
   unzipped export bundle (`metadata.yaml`, `dashboards/`, `charts/`, `datasets/`, `databases/`;
   written by `superset/export-dashboards.sh`). Only dashboards **missing** from the metadata DB
   are imported (with their charts/datasets), so edits made in the UI survive a restart; to
   apply a changed bundle on purpose, delete the dashboard in the UI and restart, or see
   docs/services.md -> "Superset dashboards in git". Nothing to do if the folder has no bundle.
"""

from __future__ import annotations

import os
import uuid
from pathlib import Path
from urllib.parse import quote

import yaml
from superset.app import create_app

DB_NAME = "ClickHouse gold"
# never change: every exported dataset in superset/dashboards/ points at this uuid
DB_UUID = uuid.UUID("15620763-256f-4cbd-8b37-ab0f84dba298")
BUNDLE_DIR = Path("/dashboards")


def gold_uri() -> str:
    password = quote(os.environ["SUPERSET_READER_PASSWORD"], safe="")
    return f"clickhousedb://superset_reader:{password}@clickhouse:8123/gold"


def register_gold_db() -> None:
    from superset import db
    from superset.models.core import Database

    database = (
        db.session.query(Database).filter_by(uuid=DB_UUID).one_or_none()
        or db.session.query(Database).filter_by(database_name=DB_NAME).one_or_none()
    )
    if database is None:
        database = Database(database_name=DB_NAME, uuid=DB_UUID)
        db.session.add(database)
        print(f"  -> database '{DB_NAME}' created")
    else:
        print(f"  -> database '{DB_NAME}' exists, URI and password reset to .env")
    database.database_name = DB_NAME
    database.uuid = DB_UUID
    database.set_sqlalchemy_uri(gold_uri())
    db.session.commit()


def read_bundle(bundle_dir: Path) -> dict[str, str]:
    """{path relative to the bundle: YAML text}, the same shape the importer gets from a zip."""
    from superset.commands.importers.v1.utils import is_valid_config

    return {
        path.relative_to(bundle_dir).as_posix(): path.read_text()
        for path in sorted(bundle_dir.rglob("*"))
        if path.is_file() and is_valid_config(path.name)
    }


def import_missing_dashboards() -> None:
    from flask import g
    from superset import db, security_manager
    from superset.commands.dashboard.importers.dispatcher import ImportDashboardsCommand
    from superset.models.dashboard import Dashboard

    contents = read_bundle(BUNDLE_DIR)
    dashboards = {
        name: yaml.safe_load(text)["uuid"]
        for name, text in contents.items()
        if name.startswith("dashboards/")
    }
    if "metadata.yaml" not in contents or not dashboards:
        print("  -> no dashboard bundle in superset/dashboards/, nothing to import")
        return

    existing = {
        str(u)
        for (u,) in db.session.query(Dashboard.uuid).filter(
            Dashboard.uuid.in_([uuid.UUID(u) for u in dashboards.values()])
        )
    }
    missing = {name for name, u in dashboards.items() if u not in existing}
    if not missing:
        print(f"  -> all {len(dashboards)} dashboard(s) exist, kept as they are")
        return

    # drop the dashboards that exist: the importer only takes the charts/datasets/databases the
    # remaining dashboards reference; overwrite=True because those may be shared with others
    bundle = {
        name: text
        for name, text in contents.items()
        if not name.startswith("dashboards/") or name in missing
    }
    g.user = security_manager.find_user(username=os.environ["SUPERSET_ADMIN_USER"])
    ImportDashboardsCommand(bundle, overwrite=True).run()
    print(f"  -> imported {len(missing)} dashboard(s): {', '.join(sorted(missing))}")


def main() -> None:
    app = create_app()
    with app.app_context():
        print(f"registering database '{DB_NAME}'...")
        register_gold_db()
        print("importing dashboards from git...")
        import_missing_dashboards()


if __name__ == "__main__":
    main()
