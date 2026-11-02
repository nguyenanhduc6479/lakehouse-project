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
# Airflow submits jobs over HTTP to Spark Master port 6066
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

# --- s3_conn (RustFS, S3-compatible) ---
: "${S3_ACCESS_KEY:?Missing S3_ACCESS_KEY}"
: "${S3_SECRET_KEY:?Missing S3_SECRET_KEY}"
: "${S3_ENDPOINT:?Missing S3_ENDPOINT}"
: "${S3_REGION:?Missing S3_REGION}"

airflow connections delete s3_conn 2>/dev/null || true
airflow connections add s3_conn \
  --conn-type aws \
  --conn-login "$S3_ACCESS_KEY" \
  --conn-password "$S3_SECRET_KEY" \
  --conn-extra "{\"endpoint_url\": \"$S3_ENDPOINT\", \"region_name\": \"$S3_REGION\"}"
airflow connections get s3_conn
echo "s3_conn done."

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
# Trino has no password auth here but rejects requests without a user name (HTTP 401)
airflow connections add trino_conn \
  --conn-type trino \
  --conn-host "$TRINO_HOST" \
  --conn-port "$TRINO_PORT" \
  --conn-login airflow \
  --conn-schema "lakehouse"
airflow connections get trino_conn
echo "trino_conn done."

echo "Init done."