#!/bin/sh
# Create the platform's single bucket on RustFS (idempotent): HEAD first, create only if missing.
# Runs in amazon/aws-cli; AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY / AWS_DEFAULT_REGION come from
# compose (mapped from S3_* in .env). Iceberg tables land under s3://<bucket>/<namespace>/<table>.

: "${S3_ENDPOINT:?Missing S3_ENDPOINT}"
: "${S3_BUCKET:?Missing S3_BUCKET}"

s3api() { aws --endpoint-url "$S3_ENDPOINT" s3api "$@"; }

echo "waiting for rustfs at ${S3_ENDPOINT}..."
tries=0
until s3api list-buckets >/dev/null 2>&1; do
  tries=$((tries + 1))
  if [ "$tries" -ge 30 ]; then
    echo "rustfs not reachable (or credentials rejected) after 30 tries"
    exit 1
  fi
  sleep 2
done

if s3api head-bucket --bucket "$S3_BUCKET" >/dev/null 2>&1; then
  echo "bucket ${S3_BUCKET} already exists. skipping."
  exit 0
fi

echo "creating bucket ${S3_BUCKET}..."
if ! s3api create-bucket --bucket "$S3_BUCKET" >/dev/null; then
  echo "failed to create bucket ${S3_BUCKET}"
  exit 1
fi
echo 'rustfs initialization completed!'
