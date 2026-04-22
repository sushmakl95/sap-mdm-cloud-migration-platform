"""Main Glue job: SAP extract → S3 raw zone.

Glue runtime invocation:
    --JOB_NAME             (auto-provided)
    --contract_fqn         e.g., "HANA:SAPSR3.MARA"
    --batch_id             unique batch identifier
    --mode                 "full" | "incremental"
    --output_base          s3://bucket/raw/sap/
"""

from __future__ import annotations

import sys

try:
    from awsglue.context import GlueContext
    from awsglue.job import Job
    from awsglue.utils import getResolvedOptions
    _IN_GLUE = True
except ImportError:
    _IN_GLUE = False

from pyspark.context import SparkContext

from migration.extractors import SapHanaExtractor
from migration.schemas import TABLE_CONTRACTS
from migration.utils import (
    IdempotencyTracker,
    MetricsEmitter,
    configure_logging,
    get_logger,
)

REQUIRED_ARGS = ["JOB_NAME", "contract_fqn", "batch_id", "mode", "output_base"]


def main(args: dict) -> int:
    configure_logging(level="INFO", fmt="json")
    log = get_logger(__name__, job=args["JOB_NAME"], batch_id=args["batch_id"])

    contract_fqn = args["contract_fqn"]
    mode = args["mode"]
    output_base = args["output_base"].rstrip("/")

    # Look up contract
    sap_table = contract_fqn.split(".")[-1]
    contract = TABLE_CONTRACTS.get(sap_table)
    if contract is None:
        log.error("unknown_contract", fqn=contract_fqn)
        return 2

    log.info("extract_job_started", contract=contract.fqn, mode=mode)

    # Build Spark session via Glue
    spark = _build_spark(args["JOB_NAME"])

    # Initialize Secrets + idempotency
    region = args.get("region", "us-east-1")
    jdbc_secret = args.get("sap_secret_id", "sap-mdm/prod/hana-jdbc")
    idempotency_table = args.get("idempotency_table", "sap-migration-audit")

    tracker = IdempotencyTracker(idempotency_table, region=region)
    if tracker.is_processed(args["JOB_NAME"], args["batch_id"]):
        log.info("batch_already_processed")
        return 0

    tracker.mark_started(args["JOB_NAME"], args["batch_id"])

    metrics = MetricsEmitter("SAP/Migration/Extract", region=region)

    try:
        # Watermark for incremental loads
        watermark = None
        if mode == "incremental":
            watermark = tracker.get_watermark("extract", contract.fqn)
            log.info("incremental_watermark", fqn=contract.fqn, watermark=watermark)

        extractor = SapHanaExtractor(spark, jdbc_secret, region=region)

        output_path = (
            f"{output_base}/{contract.sap_table}/"
            f"{args['batch_id']}/"
        )

        result = extractor.extract(contract, output_path, incremental_watermark=watermark)

        # Metrics
        metrics.emit(
            "RowsExtracted",
            result.rows_extracted,
            dimensions={"Table": contract.sap_table, "Mode": mode},
        )

        # Advance watermark for next incremental run
        if result.incremental_watermark:
            tracker.set_watermark("extract", contract.fqn, result.incremental_watermark)

        tracker.mark_succeeded(
            args["JOB_NAME"], args["batch_id"], row_count=result.rows_extracted
        )
        log.info("extract_job_complete", rows=result.rows_extracted)
        return 0

    except Exception as exc:
        log.error("extract_job_failed", error=str(exc))
        tracker.mark_failed(args["JOB_NAME"], args["batch_id"], str(exc))
        metrics.emit(
            "JobFailed",
            1,
            dimensions={"Table": contract.sap_table},
        )
        return 1


def _build_spark(job_name: str):
    if _IN_GLUE:
        sc = SparkContext.getOrCreate()
        glue_context = GlueContext(sc)
        spark = glue_context.spark_session
        job = Job(glue_context)
        job.init(job_name, {})
        return spark
    from migration.utils.spark_session import get_spark_session
    return get_spark_session(app_name=job_name, master="local[*]")


if __name__ == "__main__":
    if _IN_GLUE:
        args = getResolvedOptions(sys.argv, REQUIRED_ARGS)
    else:
        import argparse
        parser = argparse.ArgumentParser()
        parser.add_argument("--JOB_NAME", default="sap-extract-local")
        parser.add_argument("--contract_fqn", required=True)
        parser.add_argument("--batch_id", required=True)
        parser.add_argument("--mode", default="incremental")
        parser.add_argument("--output_base", required=True)
        parser.add_argument("--region", default="us-east-1")
        parser.add_argument("--sap_secret_id", default="sap-mdm/prod/hana-jdbc")
        parser.add_argument("--idempotency_table", default="sap-migration-audit")
        parsed = parser.parse_args()
        args = vars(parsed)

    sys.exit(main(args))
