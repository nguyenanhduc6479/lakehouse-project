#!/bin/sh
set -e

BASE="http://${POLARIS_HOST}:${POLARIS_PORT}"

# --- lấy token ---
TOKEN=$(curl -s -X POST "${BASE}/api/catalog/v1/oauth/tokens" \
  -d "grant_type=client_credentials&client_id=${POLARIS_CLIENT_ID}&client_secret=${POLARIS_CLIENT_SECRET}&scope=PRINCIPAL_ROLE:ALL" \
  | sed -n 's/.*"access_token":"\([^"]*\)".*/\1/p')

if [ -z "$TOKEN" ]; then
  echo "ERROR: không lấy được token từ Polaris"
  exit 1
fi
echo "Token acquired."

# --- tạo catalog ---
curl -s -X POST "${BASE}/api/management/v1/catalogs" \
  -H "Authorization: Bearer ${TOKEN}" \
  -H "Content-Type: application/json" \
  -d "{
    \"name\": \"lakehouse\",
    \"type\": \"INTERNAL\",
    \"storageConfigInfo\": {
      \"storageType\": \"S3\",
      \"allowedLocations\": [\"s3://lakehouse/\"],
      \"s3.endpoint\": \"${MINIO_ENDPOINT}\",
      \"s3.access-key-id\": \"${MINIO_ROOT_USER}\",
      \"s3.secret-access-key\": \"${MINIO_ROOT_PASSWORD}\",
      \"s3.path-style-access\": \"true\"
    },
    \"properties\": {\"default-base-location\": \"s3://lakehouse/\"}
  }"
echo "Catalog 'lakehouse' created."

# --- tạo namespaces ---
for NS in bronze silver gold; do
  curl -s -X POST "${BASE}/api/management/v1/catalogs/lakehouse/namespaces" \
    -H "Authorization: Bearer ${TOKEN}" \
    -H "Content-Type: application/json" \
    -d "{\"namespace\": [\"${NS}\"]}"
  echo "Namespace '${NS}' created."
done

echo "Polaris setup done."