"""Shared helpers for every Spark job in the lakehouse pipeline.

WHY separate: the repo's `spark-defaults.conf` does NOT declare the Iceberg/Polaris
catalog (only eventlog + timezone), so each job must load the REST catalog config
itself when building the SparkSession. Keeping it in one place lets the 3 jobs
bronze/silver/gold reuse it and avoids duplicated or drifting config between jobs.

Config is read from environment variables (injected via compose/spark.yml, from .env) —
NO secrets hardcoded in code, per repo convention.
"""

from __future__ import annotations

import os

from pyspark.sql import SparkSession

# Catalog name matches the catalog created in polaris/scripts/init.sh
CATALOG = "lakehouse"

# Default symbol list for the demo. Can be overridden when calling a job (see each job).
DEFAULT_SYMBOLS = ["FPT", "VNM", "ACB", "HPG", "MWG"]

# Landing path of the ingest layer (raw parquet) on MinIO.
# s3a:// for Spark (hadoop-aws), s3:// for Python clients (s3fs/boto3).
LANDING_PREFIX = "bronze/_landing/stock_ohlcv"


def _env(name: str, default: str | None = None) -> str:
    """Read a required env var; fail early if missing, instead of an obscure NPE deep in Spark."""
    val = os.environ.get(name, default)
    if val is None:
        raise RuntimeError(
            f"Missing environment variable '{name}'. "
            f"Check the environment: block of the Spark service in compose/spark.yml."
        )
    return val


def minio_bucket() -> str:
    return _env("MINIO_BUCKET", "lakehouse")


def get_spark(app_name: str) -> SparkSession:
    """Build a SparkSession wired to Polaris (Iceberg REST + OAuth2) and MinIO (S3).

    WHY each config:
    - catalog type=rest + uri=.../api/catalog: Polaris is an Iceberg REST catalog, NOT
      a Hive metastore or Nessie (see CLAUDE.md).
    - credential=root:<secret> + scope=PRINCIPAL_ROLE:ALL: the same client-credentials
      polaris/scripts/init.sh uses to get a token.
    - io-impl=S3FileIO + s3.*: Iceberg reads/writes data files directly on MinIO.
    - fs.s3a.*: lets Spark read the raw landing parquet (outside Iceberg).
    - path-style-access=true + ssl=false: required for MinIO (not real AWS).
    """
    minio_endpoint = _env("MINIO_ENDPOINT", "http://minio:9000")
    minio_user = _env("MINIO_ROOT_USER")
    minio_pass = _env("MINIO_ROOT_PASSWORD")
    polaris_secret = _env("POLARIS_SECRET")
    polaris_uri = _env("POLARIS_URI", "http://polaris:8181/api/catalog")

    builder = (
        SparkSession.builder.appName(app_name)
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        # --- Iceberg REST catalog pointing at Polaris ---
        .config(f"spark.sql.catalog.{CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{CATALOG}.type", "rest")
        .config(f"spark.sql.catalog.{CATALOG}.uri", polaris_uri)
        .config(f"spark.sql.catalog.{CATALOG}.warehouse", CATALOG)
        .config(f"spark.sql.catalog.{CATALOG}.credential", f"root:{polaris_secret}")
        .config(f"spark.sql.catalog.{CATALOG}.scope", "PRINCIPAL_ROLE:ALL")
        # --- Iceberg FileIO to MinIO ---
        .config(f"spark.sql.catalog.{CATALOG}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config(f"spark.sql.catalog.{CATALOG}.s3.endpoint", minio_endpoint)
        .config(f"spark.sql.catalog.{CATALOG}.s3.path-style-access", "true")
        .config(f"spark.sql.catalog.{CATALOG}.s3.access-key-id", minio_user)
        .config(f"spark.sql.catalog.{CATALOG}.s3.secret-access-key", minio_pass)
        .config("spark.sql.defaultCatalog", CATALOG)
        # --- s3a to read the raw landing parquet ---
        .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", minio_user)
        .config("spark.hadoop.fs.s3a.secret.key", minio_pass)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
    )
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def landing_s3a_uri() -> str:
    """s3a URI for Spark to read all parquet in the landing layer."""
    return f"s3a://{minio_bucket()}/{LANDING_PREFIX}"
