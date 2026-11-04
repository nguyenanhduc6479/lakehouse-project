"""Shared helpers for every Spark job on the platform.

WHY separate: the repo's `spark-defaults.conf` does NOT declare the Iceberg/Polaris
catalog (only the timezone), so each job must load the REST catalog config
itself when building the SparkSession. Keeping it in one place lets every job reuse
it and avoids duplicated or drifting config between jobs.

Config is read from environment variables (injected via compose/spark.yml, from .env) —
NO secrets hardcoded in code, per repo convention.
"""

from __future__ import annotations

import os

from pyspark.sql import SparkSession

# Catalog name matches the catalog created in polaris/scripts/init.sh
CATALOG = "lakehouse"


def _env(name: str, default: str | None = None) -> str:
    """Read a required env var; fail early if missing, instead of an obscure NPE deep in Spark."""
    val = os.environ.get(name, default)
    if val is None:
        raise RuntimeError(
            f"Missing environment variable '{name}'. "
            f"Check the environment: block of the Spark service in compose/spark.yml."
        )
    return val


def s3_bucket() -> str:
    return _env("S3_BUCKET", "data-platform")


def postgres_jdbc_options() -> dict[str, str]:
    """JDBC options for the `data-platform` Postgres DB (silver, staging, config schemas).

    Use as `spark.read.format("jdbc").options(**postgres_jdbc_options(), dbtable=...)`.
    """
    host = _env("POSTGRES_HOST", "postgres")
    return {
        "url": f"jdbc:postgresql://{host}:5432/{_env('POSTGRES_DB')}",
        "user": _env("POSTGRES_USER"),
        "password": _env("POSTGRES_PASSWORD"),
        "driver": "org.postgresql.Driver",
    }


def get_spark(app_name: str, *, extra_conf: dict[str, str] | None = None) -> SparkSession:
    """Build a SparkSession wired to Polaris (Iceberg REST + OAuth2) and RustFS (S3).

    `extra_conf`: job-specific Spark configs (e.g. {"spark.sql.shuffle.partitions": "8"}).
    Applied after the defaults below, so a job can also override one of them.

    WHY each config:
    - catalog type=rest + uri=.../api/catalog: Polaris is an Iceberg REST catalog, NOT
      a Hive metastore or Nessie (see CLAUDE.md).
    - credential=root:<secret> + scope=PRINCIPAL_ROLE:ALL: the same client-credentials
      polaris/scripts/init.sh uses to get a token.
    - io-impl=S3FileIO + s3.*: Iceberg reads/writes data files directly on RustFS.
    - fs.s3a.*: lets Spark read plain files on RustFS (outside Iceberg).
    - path-style-access=true + ssl=false: required for RustFS (not real AWS; path-style only).
    """
    s3_endpoint = _env("S3_ENDPOINT", "http://rustfs:9000")
    s3_access_key = _env("S3_ACCESS_KEY")
    s3_secret_key = _env("S3_SECRET_KEY")
    s3_region = _env("S3_REGION", "us-east-1")
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
        # --- Iceberg FileIO to RustFS ---
        .config(f"spark.sql.catalog.{CATALOG}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config(f"spark.sql.catalog.{CATALOG}.s3.endpoint", s3_endpoint)
        .config(f"spark.sql.catalog.{CATALOG}.s3.path-style-access", "true")
        .config(f"spark.sql.catalog.{CATALOG}.s3.access-key-id", s3_access_key)
        .config(f"spark.sql.catalog.{CATALOG}.s3.secret-access-key", s3_secret_key)
        .config(f"spark.sql.catalog.{CATALOG}.client.region", s3_region)
        .config("spark.sql.defaultCatalog", CATALOG)
        # --- s3a for plain files on RustFS ---
        .config("spark.hadoop.fs.s3a.endpoint", s3_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", s3_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", s3_secret_key)
        .config("spark.hadoop.fs.s3a.endpoint.region", s3_region)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
    )
    for key, value in (extra_conf or {}).items():
        builder = builder.config(key, value)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
