"""Shared fixtures for the pipeline tests.

`spark`: SparkSession local[1] scope=session — lightweight, no MinIO/Polaris/Iceberg needed
since the tests only check pure transform LOGIC (clean_silver, build_gold, transform_bronze)
on hand-built in-memory DataFrames.

Running the tests needs pyspark==3.5.1 + Java (JDK 8/11/17). Simplest: run inside the
custom-spark container (`docker compose exec spark-master python -m pytest /opt/...`) or on
a machine with Java. The `pythonpath` to spark/jobs & airflow/plugins is set in pyproject.toml.
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
