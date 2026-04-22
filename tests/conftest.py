"""Shared pytest fixtures."""

from __future__ import annotations

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    """Local Spark session for unit tests."""
    spark = (
        SparkSession.builder
        .appName("tests")
        .master("local[2]")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    yield spark
    spark.stop()
