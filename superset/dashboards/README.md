# Superset dashboards (as code)

Superset keeps its metadata in SQLite on the volume `superset_home_vol`, so `docker compose down
-v` would lose every dataset, chart and dashboard. This folder is their copy in git: an unzipped
Superset export bundle (`metadata.yaml`, `dashboards/`, `charts/`, `datasets/`, `databases/`),
plain YAML so changes show up in `git diff`. Database passwords are masked in the export.

- **Save:** build or edit dashboards in the UI, then from the repo root run
  `bash superset/export-dashboards.sh` (exports every dashboard, replacing this folder's YAML),
  review the diff and commit.
- **Restore:** automatic. At every start `superset/scripts/bootstrap.py` imports the dashboards
  that are missing from the metadata DB, so an empty volume gets everything back. Dashboards that
  already exist are left alone (edits made in the UI survive a restart).
- **Apply a changed bundle to an existing dashboard:** delete that dashboard in the UI and
  restart Superset (`docker compose restart superset`).

The database entry **ClickHouse gold** has a fixed uuid (`DB_UUID` in `bootstrap.py`); every
exported dataset points at it — never change it.
