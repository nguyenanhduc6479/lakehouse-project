# File: airflow/Dockerfile
FROM apache/airflow:2.9.2-python3.11

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.9.2/constraints-3.11.txt"

# dbt lives in its own virtualenv: dbt-core / dbt-clickhouse pin many of the same libraries as
# Airflow (jinja2, protobuf, click, ...) at other versions, and the Airflow 2.9.2 constraints would
# either block them or be broken by them. Tasks call /opt/airflow/dbt-venv/bin/dbt directly.
COPY dbt-requirements.txt /dbt-requirements.txt
RUN python -m venv /opt/airflow/dbt-venv \
    && /opt/airflow/dbt-venv/bin/pip install --no-cache-dir -r /dbt-requirements.txt
