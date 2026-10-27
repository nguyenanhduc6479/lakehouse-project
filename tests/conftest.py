"""Fixtures dùng chung cho test pipeline.

`spark`: SparkSession local[1] scope=session — nhẹ, không cần MinIO/Polaris/Iceberg vì
các test chỉ kiểm tra LOGIC transform thuần (clean_silver, build_gold, transform_bronze)
trên DataFrame dựng tay trong bộ nhớ.

Chạy test cần pyspark==3.5.1 + Java (JDK 8/11/17). Cách gọn nhất: chạy trong container
custom-spark (`docker compose exec spark-master python -m pytest /opt/...`) hoặc máy có
sẵn Java. `pythonpath` tới spark/jobs & airflow/plugins khai trong pyproject.toml.
"""

from __future__ import annotations

import pytest


@pytest.fixture(scope="session")
def spark():
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[1]")
        .appName("lakehouse-tests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "Asia/Ho_Chi_Minh")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
