#!/bin/sh

if [ -d '/data/lakehouse' ]; then 
    echo 'bucket lakehouse detected on disk. skipping initialization.';
    exit 0;
fi

echo 'bucket lakehouse is missing. connecting to minio...'
until mc alias set myminio http://minio:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD"; do
  echo '...waiting for minio connection...'
  sleep 1
done

echo 'creating buckets...'
mc mb --ignore-existing myminio/lakehouse/bronze
mc mb --ignore-existing myminio/lakehouse/silver
mc mb --ignore-existing myminio/lakehouse/gold

echo 'minio initialization completed!'
exit 0