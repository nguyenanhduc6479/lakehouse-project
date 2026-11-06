"""Superset configuration (mounted into the container, SUPERSET_CONFIG_PATH).

Everything secret comes from the environment (compose/superset.yml <- .env).
No Redis / Celery: single gunicorn server, in-process cache, no async queries — demo scale.
"""

import os

SECRET_KEY = os.environ["SUPERSET_SECRET_KEY"]

# metadata DB: Postgres `superset`, owned by role `superset` (created by postgres-setup)
SQLALCHEMY_DATABASE_URI = "postgresql+psycopg2://{user}:{password}@postgres:5432/{db}".format(
    user=os.environ["SUPERSET_DB_USERNAME"],
    password=os.environ["SUPERSET_DB_PASSWORD"],
    db=os.environ["POSTGRES_DB_SUPERSET"],
)

CACHE_CONFIG = {"CACHE_TYPE": "SimpleCache", "CACHE_DEFAULT_TIMEOUT": 300}
DATA_CACHE_CONFIG = CACHE_CONFIG
