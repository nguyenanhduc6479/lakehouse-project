#!/bin/bash
# set -e

echo "-----------------------------------------------------"
echo "waiting for postgres to be ready..."
echo "-----------------------------------------------------"

until pg_isready -h postgres -U "$POSTGRES_USER"; do
  echo "retrying..."
  sleep 2
done
echo "postgres is ready!"
echo "starting initialization for postgres"

# 1. functions
# 1.1. function to create a login
function create_login() {
    local username=$1
    local password=$2
    
    echo "processing login '$username'..."
    echo "checking existence for login '$username'..."
    
    exists=$(psql -h postgres -tAc "select 1 from pg_roles where rolname='$username'" 2>/dev/null)

    if [ "$exists" = "1" ]; then
        echo "  -> login '$username' already exists. skipping."
    else
        echo "  -> creating login '$username'..."

        output=$(psql -v on_error_stop=1 -h postgres -c "create role \"$username\" with login password '$password';" 2>&1)
        exit_code=$?

        if [ $exit_code -ne 0 ]; then
            echo "failed to create user '$username'"
            echo "error details: $output"
            exit 1
        else
            echo "  -> user '$username' created successfully."
        fi
    fi
}

# 1.2. function to create a database
function create_database() {
    local database=$1
    local username=$2
    
    echo "processing database '$database'..."
    echo "checking existence for database '$database'..."

    exists=$(psql -h postgres -tAc "select 1 from pg_database where datname='$database'" 2>/dev/null)

    if [ "$exists" = "1" ]; then
        echo "  -> database '$database' already exists. skipping."
    else
        echo "  -> creating database '$database' owned by '$username'..."

        output=$(psql -v on_error_stop=1 -h postgres -c "create database \"$database\" owner \"$username\";" 2>&1)
        exit_code=$?

        if [ $exit_code -ne 0 ]; then
            echo "failed to create database '$database'"
            echo "possible causes: user '$username' does not exist or permission denied."
            echo "error details: $output"
            exit 1
        else
            echo "  -> database '$database' created successfully."
        fi
    fi
}

# 2. create db, login and grant privileges for each service
# 2.1. airflow
if [ -n "$AIRFLOW_ADMIN_USERNAME" ] && [ -n "$POSTGRES_DB_AIRFLOW" ] && [ -n "$AIRFLOW_ADMIN_PASSWORD" ]; then
    create_login "$AIRFLOW_ADMIN_USERNAME" "$AIRFLOW_ADMIN_PASSWORD"
    create_database "$POSTGRES_DB_AIRFLOW" "$AIRFLOW_ADMIN_USERNAME"
else
    echo "warning: missing airflow vars."
fi

# 2.2. polaris
if [ -n "$POLARIS_ADMIN_USERNAME" ] && [ -n "$POSTGRES_DB_POLARIS" ] && [ -n "$POLARIS_ADMIN_PASSWORD" ]; then
    create_login "$POLARIS_ADMIN_USERNAME" "$POLARIS_ADMIN_PASSWORD"
    create_database "$POSTGRES_DB_POLARIS" "$POLARIS_ADMIN_USERNAME"
else
    echo "warning: missing polaris vars."
fi

# 2.3. superset (metadata DB)
if [ -n "$SUPERSET_DB_USERNAME" ] && [ -n "$POSTGRES_DB_SUPERSET" ] && [ -n "$SUPERSET_DB_PASSWORD" ]; then
    create_login "$SUPERSET_DB_USERNAME" "$SUPERSET_DB_PASSWORD"
    create_database "$POSTGRES_DB_SUPERSET" "$SUPERSET_DB_USERNAME"
else
    echo "warning: missing superset vars."
fi

# 3. data-platform DB: config + silver schemas (idempotent DDL)
echo "applying /postgres/schema.sql to database '$PGDATABASE'..."
output=$(psql -X -q -v ON_ERROR_STOP=1 -h postgres -f /postgres/schema.sql 2>&1)
exit_code=$?
if [ $exit_code -ne 0 ]; then
    echo "failed to apply /postgres/schema.sql"
    echo "error details: $output"
    exit 1
fi
echo "  -> schema applied."

# 4. read-only role ClickHouse reads silver with (named collection silver_pg, gold phase).
# Granted here rather than in schema.sql, which stays role-agnostic (the silver tests build a
# scratch DB from it). Default privileges cover silver tables created later by this user.
if [ -n "$CLICKHOUSE_READER_PASSWORD" ]; then
    create_login clickhouse_reader "$CLICKHOUSE_READER_PASSWORD"
    output=$(psql -X -q -v ON_ERROR_STOP=1 -h postgres \
        -c "GRANT USAGE ON SCHEMA silver TO clickhouse_reader" \
        -c "GRANT SELECT ON ALL TABLES IN SCHEMA silver TO clickhouse_reader" \
        -c "ALTER DEFAULT PRIVILEGES IN SCHEMA silver GRANT SELECT ON TABLES TO clickhouse_reader" 2>&1)
    if [ $? -ne 0 ]; then
        echo "failed to grant silver to clickhouse_reader"
        echo "error details: $output"
        exit 1
    fi
    echo "  -> clickhouse_reader can read schema silver."
else
    echo "warning: missing CLICKHOUSE_READER_PASSWORD."
fi

echo "-----------------------------------------------------"
echo "all initialization steps completed successfully!"
echo "-----------------------------------------------------"