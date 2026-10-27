#!/bin/sh
# Khởi tạo Polaris: lấy token -> tạo/cập nhật catalog `lakehouse` -> tạo namespace bronze/silver/gold.
# Idempotent: chạy lại an toàn (409 = đã tồn tại -> cập nhật catalog / bỏ qua namespace).
# Kiểm tra HTTP code từng bước — `curl -s` không tự fail khi HTTP 4xx/5xx, nên trước đây
# script in "created" dù Polaris trả 404.
set -e

BASE="http://${POLARIS_HOST}:${POLARIS_PORT}"
MGMT="${BASE}/api/management/v1"
# Namespace thuộc Iceberg REST API (prefix = tên catalog), KHÔNG phải Management API.
REST="${BASE}/api/catalog/v1/lakehouse"
# MinIO bỏ qua region nhưng AWS SDK (Polaris + client) bắt buộc phải có.
S3_REGION="${S3_REGION:-us-east-1}"

# req METHOD URL [JSON] -> body ở /tmp/resp, HTTP code ở $CODE
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

# --- lấy token ---
TOKEN=$(curl -s -X POST "${BASE}/api/catalog/v1/oauth/tokens" \
  -d "grant_type=client_credentials&client_id=${POLARIS_CLIENT_ID}&client_secret=${POLARIS_CLIENT_SECRET}&scope=PRINCIPAL_ROLE:ALL" \
  | sed -n 's/.*"access_token":"\([^"]*\)".*/\1/p')

if [ -z "$TOKEN" ]; then
  echo "ERROR: không lấy được token từ Polaris"
  exit 1
fi
echo "Token acquired."

# --- catalog ---
# Field theo AwsStorageConfigInfo của Polaris 1.5.0 (spec/polaris-management-service.yml):
# - endpoint + pathStyleAccess: trỏ MinIO, bắt buộc path-style.
# - stsUnavailable=true: MinIO ở đây không dùng STS -> Polaris không vend credential, server
#   tự đọc/ghi metadata bằng AWS_* env (xem compose/catalog.yml), client dùng key của chính nó.
# Key MinIO KHÔNG nằm trong storageConfigInfo (spec không có field đó).
STORAGE="{
  \"storageType\": \"S3\",
  \"allowedLocations\": [\"s3://lakehouse/\"],
  \"endpoint\": \"${MINIO_ENDPOINT}\",
  \"pathStyleAccess\": true,
  \"stsUnavailable\": true,
  \"region\": \"${S3_REGION}\"
}"
# drop-with-purge: mặc định false -> Trino `DROP TABLE` (luôn gửi purgeRequested=true) bị
# 403 DROP_TABLE_WITH_PURGE. Bật ở cấp catalog (FeatureConfiguration, Polaris 1.5.0).
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
    # Đã có (chạy lại / volume postgres cũ) -> cập nhật storage config cho khớp file này.
    req GET "${MGMT}/catalogs/lakehouse"
    [ "$CODE" = 200 ] || die "đọc catalog lakehouse"
    VER=$(sed -n 's/.*"entityVersion":\([0-9]*\).*/\1/p' /tmp/resp)
    req PUT "${MGMT}/catalogs/lakehouse" \
      "{\"currentEntityVersion\": ${VER}, \"properties\": ${PROPS}, \"storageConfigInfo\": ${STORAGE}}"
    case "$CODE" in 2??) echo "Catalog 'lakehouse' đã tồn tại -> updated storage config." ;;
      *) die "cập nhật catalog lakehouse" ;; esac
    ;;
  *) die "tạo catalog lakehouse" ;;
esac

# --- quyền ---
# catalog_admin mặc định chỉ có CATALOG_MANAGE_ACCESS + CATALOG_MANAGE_METADATA: đủ tạo/đọc/ghi
# bảng nhưng thiếu quyền data-level -> 403 DROP_TABLE_WITH_PURGE (Trino DROP TABLE) và
# REPORT_*_METRICS (Iceberg client). CATALOG_MANAGE_CONTENT bao trùm các quyền đó.
# PUT grant KHÔNG idempotent (1.5.0: cấp lại -> HTTP 500 duplicate key) nên kiểm tra trước.
GRANTS_URL="${MGMT}/catalogs/lakehouse/catalog-roles/catalog_admin/grants"
req GET "$GRANTS_URL"
[ "$CODE" = 200 ] || die "đọc grants catalog_admin"
if grep -q '"CATALOG_MANAGE_CONTENT"' /tmp/resp; then
  echo "catalog_admin đã có CATALOG_MANAGE_CONTENT -> bỏ qua."
else
  req PUT "$GRANTS_URL" '{"grant": {"type": "catalog", "privilege": "CATALOG_MANAGE_CONTENT"}}'
  case "$CODE" in 2??) echo "Granted CATALOG_MANAGE_CONTENT -> catalog_admin." ;;
    *) die "cấp CATALOG_MANAGE_CONTENT" ;; esac
fi

# --- namespaces (Iceberg REST: POST /v1/{prefix}/namespaces) ---
for NS in bronze silver gold; do
  req POST "${REST}/namespaces" "{\"namespace\": [\"${NS}\"], \"properties\": {}}"
  case "$CODE" in
    2??) echo "Namespace '${NS}' created." ;;
    409) echo "Namespace '${NS}' đã tồn tại -> bỏ qua." ;;
    *) die "tạo namespace ${NS}" ;;
  esac
done

echo "Polaris setup done."
