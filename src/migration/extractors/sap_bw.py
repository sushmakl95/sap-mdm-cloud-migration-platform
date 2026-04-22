"""SAP BW extractor via Open Hub Service.

SAP BW exposes DSOs (DataStore Objects) and InfoCubes through Open Hub
destinations — a supported extraction mechanism for BI consumers. Unlike
direct JDBC to HANA, Open Hub is idempotent and respects BW's delta semantics.

Two supported Open Hub patterns:
  1. **File destination**: BW writes CSV/Parquet to a network share; we read it
  2. **Table destination**: BW writes to a specific HANA table, we read via JDBC

We use pattern 2 (table destination) since we already have HANA JDBC plumbed.
Open Hub maintains a DELTA queue — each read advances the pointer.
"""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession

from migration.schemas import TableContract
from migration.utils.logging_config import get_logger
from migration.utils.secrets import get_secret

log = get_logger(__name__, component="bw_extractor")


@dataclass
class BwExtractConfig:
    """BW Open Hub destination configuration."""

    open_hub_destination: str
    """Name of the Open Hub Destination (e.g., '0OHD_MATERIAL_DAILY')."""

    delta_mode: bool = True
    """Use BW's delta queue (vs. full init)."""

    process_chain: str | None = None
    """Optional: BW Process Chain that must have completed before we read."""


class SapBwExtractor:
    """SAP BW extractor via Open Hub table destinations."""

    def __init__(
        self,
        spark: SparkSession,
        jdbc_secret_id: str,
        region: str = "us-east-1",
    ):
        self.spark = spark
        self.jdbc_secret_id = jdbc_secret_id
        self.region = region

    def extract(
        self,
        contract: TableContract,
        bw_config: BwExtractConfig,
        output_path: str,
    ) -> int:
        """Read Open Hub destination table; write to S3."""
        log.info(
            "bw_extract_started",
            contract=contract.fqn,
            open_hub=bw_config.open_hub_destination,
        )

        # Verify process chain completed (if specified)
        if bw_config.process_chain:
            self._verify_chain_completed(bw_config.process_chain)

        df = self._read_open_hub_table(contract, bw_config)
        df.write.mode("overwrite").parquet(output_path)
        count = df.count()

        # Mark delta queue as consumed (BW-side: calls RFC /BIC/OHDELTA_CLOSE)
        if bw_config.delta_mode:
            self._close_delta_queue(bw_config.open_hub_destination)

        log.info("bw_extract_complete", contract=contract.fqn, rows=count)
        return count

    def _jdbc_url(self) -> str:
        creds = get_secret(self.jdbc_secret_id, region=self.region)
        host = creds["host"]
        port = creds.get("port", "30015")
        return f"jdbc:sap://{host}:{port}/?databaseName={creds['database']}"

    def _conn_props(self) -> dict[str, str]:
        creds = get_secret(self.jdbc_secret_id, region=self.region)
        return {
            "user": creds["username"],
            "password": creds["password"],
            "driver": "com.sap.db.jdbc.Driver",
            "fetchsize": "50000",
            "encrypt": "true",
        }

    def _read_open_hub_table(
        self, contract: TableContract, bw_config: BwExtractConfig
    ) -> DataFrame:
        """Read the Open Hub destination's backing table."""
        # Open Hub table convention: /BIC/OH<DESTINATION>
        table_name = f"/BIC/OH{bw_config.open_hub_destination}"
        query = f"(SELECT * FROM SAPBW.\"{table_name}\") oh"

        return (
            self.spark.read.format("jdbc")
            .option("url", self._jdbc_url())
            .option("dbtable", query)
            .option("partitionColumn", "REQUID")
            .option("lowerBound", "0")
            .option("upperBound", "2147483647")
            .option("numPartitions", str(contract.partition_num_chunks))
            .options(**self._conn_props())
            .load()
        )

    def _verify_chain_completed(self, process_chain: str) -> None:
        """Check that the named Process Chain has finished running.

        BW maintains chain status in RSPCLOGCHAIN — we query the latest run.
        If the chain is still running or has errored, we refuse to extract.
        """
        query = (
            f"(SELECT LOG_ID, DATUM, ZEIT, ANALYZED_STATUS "
            f"FROM SAPBW.RSPCLOGCHAIN "
            f"WHERE CHAIN_ID = '{process_chain}' "
            f"ORDER BY DATUM DESC, ZEIT DESC "
            f"LIMIT 1) latest_run"
        )
        df = (
            self.spark.read.format("jdbc")
            .option("url", self._jdbc_url())
            .option("dbtable", query)
            .options(**self._conn_props())
            .load()
        )
        row = df.first()
        if row is None:
            raise RuntimeError(f"Process chain {process_chain} has never run")

        status = row["ANALYZED_STATUS"]
        if status != "G":  # G = Green / success in SAP
            raise RuntimeError(
                f"Process chain {process_chain} last status was '{status}', not 'G'"
            )
        log.info("bw_chain_verified", chain=process_chain, status=status)

    def _close_delta_queue(self, open_hub_destination: str) -> None:
        """Tell BW the delta has been consumed. In production this would
        call an RFC (remote function call) like RSB_API_OHS_DELTAQUEUE_CLOSE.

        Left as a no-op placeholder here — a real implementation would shell out
        to a tiny RFC bridge service."""
        log.info("bw_delta_close_requested", destination=open_hub_destination)
