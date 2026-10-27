#!/usr/bin/env bash
set -euo pipefail

airflow db migrate

# --- admin ---
: "${AIRFLOW_ADMIN_USER:?Missing AIRFLOW_ADMIN_USER}"
: "${AIRFLOW_ADMIN_PASSWORD:?Missing AIRFLOW_ADMIN_PASSWORD}"
airflow users create \
  --username "$AIRFLOW_ADMIN_USER" \
  --password "$AIRFLOW_ADMIN_PASSWORD" \
  --firstname "Admin" --lastname "User" \
  --role "Admin" \
  --email "admin@example.org" || true
echo "admin ensured"

# --- spark_rest_conn (Spark Standalone REST API, cluster mode) ---
# Airflow submit job qua HTTP → Spark Master port 6066
# The driver runs on the Spark Worker, not on Airflow
: "${SPARK_REST_HOST:?Missing SPARK_REST_HOST}"
: "${SPARK_REST_PORT:?Missing SPARK_REST_PORT}"

airflow connections delete spark_rest_conn 2>/dev/null || true
airflow connections add spark_rest_conn \
  --conn-type http \
  --conn-host "$SPARK_REST_HOST" \
  --conn-port "$SPARK_REST_PORT" \
  --conn-schema "http"
airflow connections get spark_rest_conn
echo "spark_rest_conn done."

# --- minio_conn ---
: "${MINIO_ROOT_USER:?Missing MINIO_ROOT_USER}"
: "${MINIO_ROOT_PASSWORD:?Missing MINIO_ROOT_PASSWORD}"
: "${MINIO_ENDPOINT:?Missing MINIO_ENDPOINT}"

airflow connections delete minio_conn 2>/dev/null || true
airflow connections add minio_conn \
  --conn-type aws \
  --conn-host "$MINIO_ENDPOINT" \
  --conn-login "$MINIO_ROOT_USER" \
  --conn-password "$MINIO_ROOT_PASSWORD" \
  --conn-extra "{\"endpoint_url\": \"$MINIO_ENDPOINT\"}"
airflow connections get minio_conn
echo "minio_conn done."

# --- postgres_conn ---
: "${POSTGRES_USER:?Missing POSTGRES_USER}"
: "${POSTGRES_PASSWORD:?Missing POSTGRES_PASSWORD}"
: "${POSTGRES_HOST:?Missing POSTGRES_HOST}"
: "${POSTGRES_PORT:?Missing POSTGRES_PORT}"
: "${POSTGRES_DB:?Missing POSTGRES_DB}"

airflow connections delete postgres_conn 2>/dev/null || true
airflow connections add postgres_conn \
  --conn-type postgres \
  --conn-host "$POSTGRES_HOST" \
  --conn-login "$POSTGRES_USER" \
  --conn-password "$POSTGRES_PASSWORD" \
  --conn-port "$POSTGRES_PORT" \
  --conn-schema "$POSTGRES_DB" \
  --conn-extra '{"options": "-c search_path=public"}'
airflow connections get postgres_conn
echo "postgres_conn done."

# --- trino_conn ---
: "${TRINO_HOST:?Missing TRINO_HOST}"
: "${TRINO_PORT:?Missing TRINO_PORT}"

airflow connections delete trino_conn 2>/dev/null || true
airflow connections add trino_conn \
  --conn-type trino \
  --conn-host "$TRINO_HOST" \
  --conn-port "$TRINO_PORT" \
  --conn-schema "lakehouse"
airflow connections get trino_conn
echo "trino_conn done."

# --- datahub_conn ---
: "${DATAHUB_GMS_HOST:?Missing DATAHUB_GMS_HOST}"
: "${DATAHUB_GMS_PORT:?Missing DATAHUB_GMS_PORT}"

airflow connections delete datahub_conn 2>/dev/null || true
airflow connections add datahub_conn \
  --conn-type http \
  --conn-host "$DATAHUB_GMS_HOST" \
  --conn-port "$DATAHUB_GMS_PORT"
airflow connections get datahub_conn
echo "datahub_conn done."

echo "Init done."