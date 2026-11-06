# Superset with the ClickHouse driver: the stock image ships none since 5.0. Baked into the image
# (not pip-installed at start): no download at every start, same versions every time.
# Superset's clickhouse extra wants clickhouse-connect >= 0.13, < 1.0.
FROM apache/superset:6.1.0

USER root
RUN uv pip install --no-cache --python /app/.venv/bin/python clickhouse-connect==0.15.1
USER superset
