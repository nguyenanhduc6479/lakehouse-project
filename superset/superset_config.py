"""Superset configuration (mounted into the container, SUPERSET_CONFIG_PATH).

Everything secret comes from the environment (compose/superset.yml <- .env).
No Redis / Celery: single gunicorn server, in-process cache, no async queries — demo scale.
"""

import os

SECRET_KEY = os.environ["SUPERSET_SECRET_KEY"]

# Metadata (users, database entries, charts, dashboards) in SQLite on the volume superset_home_vol.
# Superset's docs advise against SQLite in production (one writer at a time); this is a
# single-user local demo, so it is enough and keeps Superset independent of Postgres.
SQLALCHEMY_DATABASE_URI = "sqlite:////app/superset_home/superset.db"

CACHE_CONFIG = {"CACHE_TYPE": "SimpleCache", "CACHE_DEFAULT_TIMEOUT": 300}
DATA_CACHE_CONFIG = CACHE_CONFIG
