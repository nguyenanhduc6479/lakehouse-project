"""Helper dùng chung cho mọi Spark job của pipeline lakehouse.

WHY tách riêng: `spark-defaults.conf` trong repo CHƯA khai báo Iceberg/Polaris
catalog (chỉ có eventlog + timezone), nên từng job phải tự nạp cấu hình catalog
REST khi tạo SparkSession. Gom vào một chỗ để 3 job bronze/silver/gold dùng lại,
tránh lặp config và lệch cấu hình giữa các job.

Config đọc từ biến môi trường (inject qua compose/spark.yml, lấy từ .env) —
KHÔNG hardcode secret trong code, đúng convention repo.
"""

from __future__ import annotations

import os

from pyspark.sql import SparkSession

# Catalog name khớp với catalog đã tạo trong polaris/scripts/init.sh
CATALOG = "lakehouse"

# Danh sách mã mặc định cho demo. Có thể override khi gọi job (xem từng job).
DEFAULT_SYMBOLS = ["FPT", "VNM", "ACB", "HPG", "MWG"]

# Đường dẫn landing của tầng ingest (parquet thô) trên MinIO.
# Dùng s3a:// cho Spark (hadoop-aws), s3:// cho client Python (s3fs/boto3).
LANDING_PREFIX = "bronze/_landing/stock_ohlcv"


def _env(name: str, default: str | None = None) -> str:
    """Đọc env var bắt buộc; fail sớm nếu thiếu để lỗi dễ hiểu thay vì NPE sâu bên trong Spark."""
    val = os.environ.get(name, default)
    if val is None:
        raise RuntimeError(
            f"Thiếu biến môi trường '{name}'. "
            f"Kiểm tra block environment: của service Spark trong compose/spark.yml."
        )
    return val


def minio_bucket() -> str:
    return _env("MINIO_BUCKET", "lakehouse")


def get_spark(app_name: str) -> SparkSession:
    """Tạo SparkSession đã nối sẵn Polaris (Iceberg REST + OAuth2) và MinIO (S3).

    WHY từng config:
    - catalog type=rest + uri=.../api/catalog: Polaris là Iceberg REST catalog, KHÔNG
      phải Hive metastore hay Nessie (xem CLAUDE.md).
    - credential=root:<secret> + scope=PRINCIPAL_ROLE:ALL: client-credentials y hệt
      polaris/scripts/init.sh dùng để lấy token.
    - io-impl=S3FileIO + s3.*: Iceberg ghi/đọc data file trực tiếp xuống MinIO.
    - fs.s3a.*: để Spark đọc được parquet thô ở tầng landing (ngoài Iceberg).
    - path-style-access=true + ssl=false: bắt buộc cho MinIO (không phải AWS thật).
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
        # --- Iceberg REST catalog trỏ vào Polaris ---
        .config(f"spark.sql.catalog.{CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{CATALOG}.type", "rest")
        .config(f"spark.sql.catalog.{CATALOG}.uri", polaris_uri)
        .config(f"spark.sql.catalog.{CATALOG}.warehouse", CATALOG)
        .config(f"spark.sql.catalog.{CATALOG}.credential", f"root:{polaris_secret}")
        .config(f"spark.sql.catalog.{CATALOG}.scope", "PRINCIPAL_ROLE:ALL")
        # --- Iceberg FileIO xuống MinIO ---
        .config(f"spark.sql.catalog.{CATALOG}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config(f"spark.sql.catalog.{CATALOG}.s3.endpoint", minio_endpoint)
        .config(f"spark.sql.catalog.{CATALOG}.s3.path-style-access", "true")
        .config(f"spark.sql.catalog.{CATALOG}.s3.access-key-id", minio_user)
        .config(f"spark.sql.catalog.{CATALOG}.s3.secret-access-key", minio_pass)
        .config("spark.sql.defaultCatalog", CATALOG)
        # --- s3a để đọc parquet thô tầng landing ---
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
    """URI s3a cho Spark đọc toàn bộ parquet ở tầng landing."""
    return f"s3a://{minio_bucket()}/{LANDING_PREFIX}"
