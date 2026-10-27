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
# 1.1. hàm tạo login
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

        output=$(psql -v on_error_stop=1 -h postgres -c "create role $username with login password '$password';" 2>&1)
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

# 1.2. hàm tạo database
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

        output=$(psql -v on_error_stop=1 -h postgres -c "create database $database owner $username;" 2>&1)
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

# 2. tạo db, login và gắn quyền cho các service
# 2.1. xử lý cho airflow
if [ -n "$AIRFLOW_ADMIN_USERNAME" ] && [ -n "$POSTGRES_DB_AIRFLOW" ] && [ -n "$AIRFLOW_ADMIN_PASSWORD" ]; then
    create_login "$AIRFLOW_ADMIN_USERNAME" "$AIRFLOW_ADMIN_PASSWORD"
    create_database "$POSTGRES_DB_AIRFLOW" "$AIRFLOW_ADMIN_USERNAME"
else
    echo "warning: missing airflow vars."
fi

# 2.2. xử lý cho datahub
if [ -n "$DATAHUB_ADMIN_USERNAME" ] && [ -n "$POSTGRES_DB_DATAHUB" ] && [ -n "$DATAHUB_ADMIN_PASSWORD" ]; then
    create_login "$DATAHUB_ADMIN_USERNAME" "$DATAHUB_ADMIN_PASSWORD"
    create_database "$POSTGRES_DB_DATAHUB" "$DATAHUB_ADMIN_USERNAME"
else
    echo "warning: missing datahub vars."
fi

# 2.3. xử lý cho poleris
if [ -n "$POLARIS_ADMIN_USERNAME" ] && [ -n "$POSTGRES_DB_POLARIS" ] && [ -n "$POLARIS_ADMIN_PASSWORD" ]; then
    create_login "$POLARIS_ADMIN_USERNAME" "$POLARIS_ADMIN_PASSWORD"
    create_database "$POSTGRES_DB_POLARIS" "$POLARIS_ADMIN_USERNAME"
else
    echo "warning: missing polaris vars."
fi

echo "-----------------------------------------------------"
echo "all initialization steps completed successfully!"
echo "-----------------------------------------------------"