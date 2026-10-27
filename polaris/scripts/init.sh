#!/bin/sh
# Initialize Polaris: get token -> create/update catalog `lakehouse` -> create namespaces bronze/silver/gold.
# Idempotent: safe to re-run (409 = already exists -> update catalog / skip namespace).
# Checks the HTTP code at every step — `curl -s` doesn't fail on HTTP 4xx/5xx, which is why the
# script used to print "created" even when Polaris answered 404.
set -e

BASE="http://${POLARIS_HOST}:${POLARIS_PORT}"
MGMT="${BASE}/api/management/v1"
# Namespaces belong to the Iceberg REST API (prefix = catalog name), NOT the Management API.
REST="${BASE}/api/catalog/v1/lakehouse"
# MinIO ignores region, but the AWS SDK (Polaris + clients) requires one.
S3_REGION="${S3_REGION:-us-east-1}"

# req METHOD URL [JSON] -> body in /tmp/resp, HTTP code in $CODE
req() {
  if [ -n "${3:-}" ]; then
    CODE=$(curl -s -o /tmp/resp -w '%{http_code}' -X "$1" "$2" \
      -H "Authorization: Bearer ${TOKEN}" -H "Content-Type: application/json" -d "$3")
  else
    CODE=$(curl -s -o /tmp/resp -w '%{http_code}' -X "$1" "$2" \
      -H "Authorization: Bearer ${TOKEN}")
  fi
}

die() { echo "ERROR: $1 (HTTP ${CODE}): $(cat /tmp/resp 2>/dev/null)"; exit 1; }

# --- get token ---
TOKEN=$(curl -s -X POST "${BASE}/api/catalog/v1/oauth/tokens" \
  -d "grant_type=client_credentials&client_id=${POLARIS_CLIENT_ID}&client_secret=${POLARIS_CLIENT_SECRET}&scope=PRINCIPAL_ROLE:ALL" \
  | sed -n 's/.*"access_token":"\([^"]*\)".*/\1/p')

if [ -z "$TOKEN" ]; then
  echo "ERROR: could not get a token from Polaris"
  exit 1
fi
echo "Token acquired."

# --- catalog ---
# Fields per Polaris 1.5.0 AwsStorageConfigInfo (spec/polaris-management-service.yml):
# - endpoint + pathStyleAccess: point at MinIO, path-style is mandatory.
# - stsUnavailable=true: MinIO here has no STS -> Polaris vends no credentials, the server
#   reads/writes metadata with AWS_* env (see compose/catalog.yml), clients use their own keys.
# MinIO keys are NOT part of storageConfigInfo (the spec has no such field).
STORAGE="{
  \"storageType\": \"S3\",
  \"allowedLocations\": [\"s3://lakehouse/\"],
  \"endpoint\": \"${MINIO_ENDPOINT}\",
  \"pathStyleAccess\": true,
  \"stsUnavailable\": true,
  \"region\": \"${S3_REGION}\"
}"
# drop-with-purge: defaults to false -> Trino `DROP TABLE` (always sends purgeRequested=true) gets
# 403 DROP_TABLE_WITH_PURGE. Enabled at catalog level (FeatureConfiguration, Polaris 1.5.0).
PROPS='{"default-base-location": "s3://lakehouse/", "polaris.config.drop-with-purge.enabled": "true"}'

req POST "${MGMT}/catalogs" "{\"catalog\": {
  \"name\": \"lakehouse\",
  \"type\": \"INTERNAL\",
  \"properties\": ${PROPS},
  \"storageConfigInfo\": ${STORAGE}
}}"
case "$CODE" in
  2??) echo "Catalog 'lakehouse' created." ;;
  409)
    # Already exists (re-run / old postgres volume) -> update storage config to match this file.
    req GET "${MGMT}/catalogs/lakehouse"
    [ "$CODE" = 200 ] || die "read catalog lakehouse"
    VER=$(sed -n 's/.*"entityVersion":\([0-9]*\).*/\1/p' /tmp/resp)
    req PUT "${MGMT}/catalogs/lakehouse" \
      "{\"currentEntityVersion\": ${VER}, \"properties\": ${PROPS}, \"storageConfigInfo\": ${STORAGE}}"
    case "$CODE" in 2??) echo "Catalog 'lakehouse' already exists -> updated storage config." ;;
      *) die "update catalog lakehouse" ;; esac
    ;;
  *) die "create catalog lakehouse" ;;
esac

# --- privileges ---
# By default catalog_admin only has CATALOG_MANAGE_ACCESS + CATALOG_MANAGE_METADATA: enough to
# create/read/write tables but lacks data-level privileges -> 403 DROP_TABLE_WITH_PURGE (Trino
# DROP TABLE) and REPORT_*_METRICS (Iceberg client). CATALOG_MANAGE_CONTENT covers those.
# PUT grant is NOT idempotent (1.5.0: re-granting -> HTTP 500 duplicate key), so check first.
GRANTS_URL="${MGMT}/catalogs/lakehouse/catalog-roles/catalog_admin/grants"
req GET "$GRANTS_URL"
[ "$CODE" = 200 ] || die "read grants of catalog_admin"
if grep -q '"CATALOG_MANAGE_CONTENT"' /tmp/resp; then
  echo "catalog_admin already has CATALOG_MANAGE_CONTENT -> skipping."
else
  req PUT "$GRANTS_URL" '{"grant": {"type": "catalog", "privilege": "CATALOG_MANAGE_CONTENT"}}'
  case "$CODE" in 2??) echo "Granted CATALOG_MANAGE_CONTENT -> catalog_admin." ;;
    *) die "grant CATALOG_MANAGE_CONTENT" ;; esac
fi

# --- namespaces (Iceberg REST: POST /v1/{prefix}/namespaces) ---
for NS in bronze silver gold; do
  req POST "${REST}/namespaces" "{\"namespace\": [\"${NS}\"], \"properties\": {}}"
  case "$CODE" in
    2??) echo "Namespace '${NS}' created." ;;
    409) echo "Namespace '${NS}' already exists -> skipping." ;;
    *) die "create namespace ${NS}" ;;
  esac
done

echo "Polaris setup done."
