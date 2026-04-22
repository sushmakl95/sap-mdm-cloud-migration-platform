"""Databricks Auto Loader consumer for DMS CDC events.

The CDC pipeline topology:

    DMS → S3 (CDC zone, Parquet) → Auto Loader → Delta Lake bronze →
    MERGE → Delta Lake silver → Lambda writer → Postgres + Redshift

This module implements the bronze + silver layers in Delta Lake.

Bronze: raw CDC events as delivered by DMS, including the magic `Op` column
        that says I (insert), U (update), or D (delete).

Silver: deduplicated target-shaped table with the LATEST version of each PK.
        Deletes are applied by tombstoning the row (we KEEP a record with
        _is_deleted=true rather than removing it — enables time-travel queries).

We use structured streaming with trigger-once semantics for a batch-like cadence
with streaming semantics (exactly-once via checkpoint).
"""

from __future__ import annotations

from dataclasses import dataclass

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from migration.schemas import TableContract
from migration.utils.logging_config import get_logger

log = get_logger(__name__, component="cdc_consumer")


@dataclass
class CdcConsumerConfig:
    """Configuration for one table's CDC consumer."""

    dms_s3_path: str
    """Where DMS delivers CDC events for this table."""

    bronze_path: str
    """Delta Lake bronze output path."""

    silver_path: str
    """Delta Lake silver output path (deduplicated, merged)."""

    checkpoint_path: str
    """Auto Loader checkpoint location."""

    trigger_interval: str = "5 minutes"
    """How often to process the stream."""


class CdcConsumer:
    """Streams DMS CDC events to Delta Lake with MERGE semantics."""

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def ingest_to_bronze(
        self, contract: TableContract, config: CdcConsumerConfig
    ) -> None:
        """Stage 1: Auto Loader reads DMS files → Delta Lake bronze."""
        log.info("cdc_bronze_ingest_started", table=contract.fqn)

        # Auto Loader configuration — incremental file discovery
        bronze_df = (
            self.spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "parquet")
            .option("cloudFiles.schemaLocation", f"{config.checkpoint_path}/schema")
            .option("cloudFiles.inferColumnTypes", "true")
            .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
            .load(config.dms_s3_path)
        )

        # DMS adds metadata columns — Op (operation: I/U/D), LSN, timestamp
        enriched = (
            bronze_df
            .withColumn("_cdc_ingested_at", F.current_timestamp())
            .withColumn("_cdc_source_file", F.col("_metadata.file_path"))
        )

        # Write to bronze Delta table
        (
            enriched.writeStream
            .format("delta")
            .outputMode("append")
            .option("checkpointLocation", f"{config.checkpoint_path}/bronze")
            .trigger(availableNow=True)
            .start(config.bronze_path)
            .awaitTermination()
        )

        log.info("cdc_bronze_ingest_complete", table=contract.fqn)

    def merge_to_silver(
        self, contract: TableContract, config: CdcConsumerConfig
    ) -> None:
        """Stage 2: MERGE bronze CDC events into silver (deduplicated target shape).

        This is where we apply the CDC event semantics:
          - Op='I' or 'U' → UPSERT row in silver
          - Op='D' → mark silver row as deleted (soft delete)
        """
        log.info("cdc_silver_merge_started", table=contract.fqn)

        # Read bronze (everything new since last merge, via streaming checkpoint)
        bronze = (
            self.spark.readStream.format("delta")
            .load(config.bronze_path)
        )

        # Keep only the latest event per PK within each micro-batch (dedupe)
        pk_cols = contract.primary_keys

        def _apply_merge_to_batch(batch_df: DataFrame, _batch_id: int) -> None:
            # Window within this batch to pick latest row per PK
            w = Window.partitionBy(*pk_cols).orderBy(F.col("_cdc_ingested_at").desc())
            latest = (
                batch_df
                .withColumn("_rn", F.row_number().over(w))
                .filter(F.col("_rn") == 1)
                .drop("_rn")
            )

            # Does silver exist? If not, initialize it from this batch
            if not DeltaTable.isDeltaTable(self.spark, config.silver_path):
                latest = latest.withColumn(
                    "_is_deleted",
                    F.when(F.col("Op") == "D", F.lit(True)).otherwise(F.lit(False)),
                )
                latest.write.format("delta").save(config.silver_path)
                return

            silver = DeltaTable.forPath(self.spark, config.silver_path)

            # MERGE condition: match on PK
            merge_condition = " AND ".join(
                f"target.{c} = source.{c}" for c in pk_cols
            )

            # Build the update-values dict (map source columns to target)
            update_columns = {
                m.target_name: f"source.{m.target_name}"
                for m in contract.columns
                if not m.is_audit and not m.is_pk
            }
            update_columns["_is_deleted"] = "CASE WHEN source.Op = 'D' THEN true ELSE false END"
            update_columns["_cdc_ingested_at"] = "source._cdc_ingested_at"

            insert_columns = {
                m.target_name: f"source.{m.target_name}"
                for m in contract.columns
                if not m.is_audit
            }
            insert_columns["_is_deleted"] = "CASE WHEN source.Op = 'D' THEN true ELSE false END"
            insert_columns["_cdc_ingested_at"] = "source._cdc_ingested_at"

            (
                silver.alias("target")
                .merge(latest.alias("source"), merge_condition)
                .whenMatchedUpdate(set=update_columns)
                .whenNotMatchedInsert(values=insert_columns)
                .execute()
            )

        # Stream with forEachBatch — lets us call Delta MERGE per micro-batch
        (
            bronze.writeStream
            .foreachBatch(_apply_merge_to_batch)
            .option("checkpointLocation", f"{config.checkpoint_path}/silver")
            .trigger(availableNow=True)
            .start()
            .awaitTermination()
        )

        log.info("cdc_silver_merge_complete", table=contract.fqn)
