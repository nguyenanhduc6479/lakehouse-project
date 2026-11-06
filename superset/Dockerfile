# Superset with the drivers it needs: the stock image ships none since 5.0.
#   psycopg2-binary    -> metadata DB (Postgres `superset`)
#   clickhouse-connect -> the gold database (Superset's clickhouse extra wants >= 0.13, < 1.0)
FROM apache/superset:6.1.0

USER root
RUN uv pip install --no-cache --python /app/.venv/bin/python \
        clickhouse-connect==0.15.1 \
        psycopg2-binary==2.9.13
USER superset
