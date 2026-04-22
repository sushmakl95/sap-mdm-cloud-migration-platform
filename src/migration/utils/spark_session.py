"""Spark session factory with SAP + Databricks + Glue-compatible tuning."""

from __future__ import annotations

import os

from pyspark.sql import SparkSession


def get_spark_session(
    app_name: str = "sap-migration",
    master: str = "local[*]",
    extra_configs: dict | None = None,
) -> SparkSession:
    """Build a tuned Spark session for SAP migration workloads.

    Key configs:
      - AQE enabled (for joins during transform/validation)
      - Kryo serializer
      - Broadcast threshold 100MB
      - SAP HANA JDBC jar + Delta Lake jars pulled from Maven (when local)
      - High memory for driver (for collecting bucket checksums)
    """
    cores = os.cpu_count() or 4
    configs = {
        "spark.driver.memory": "4g",
        "spark.driver.bindAddress": "127.0.0.1",
        "spark.driver.host": "127.0.0.1",
        "spark.ui.enabled": "false",
        "spark.sql.shuffle.partitions": str(cores * 2),

        # AQE
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
        "spark.sql.adaptive.skewJoin.enabled": "true",

        # Serialization
        "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
        "spark.kryoserializer.buffer.max": "512m",

        # Broadcast threshold 100MB
        "spark.sql.autoBroadcastJoinThreshold": "104857600",

        # Delta Lake (for CDC silver tables)
        "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
        "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",

        # When running locally, pull SAP JDBC + Delta from Maven
        "spark.jars.packages": ",".join([
            "io.delta:delta-spark_2.12:3.2.0",
            "org.postgresql:postgresql:42.7.3",
            # SAP JDBC is proprietary — must be manually installed in Glue/Databricks
            # "com.sap.cloud.db.jdbc:ngdbc:2.21.10",
        ]),

        "spark.sql.warehouse.dir": os.environ.get("SPARK_WAREHOUSE", "/tmp/spark-warehouse"),
    }
    if extra_configs:
        configs.update(extra_configs)

    builder = SparkSession.builder.appName(app_name).master(master)
    for k, v in configs.items():
        builder = builder.config(k, str(v))

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
