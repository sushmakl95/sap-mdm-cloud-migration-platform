"""Main Glue job: S3 raw → transform → Postgres + Redshift.

Glue invocation:
    --JOB_NAME
    --contract_fqn
    --batch_id
    --extract_path          s3://bucket/raw/sap/MARA/<batch>/
    --redshift_staging_path
    --redshift_iam_role
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

from migration.loaders import PostgresLoader, RedshiftLoader
from migration.schemas import TABLE_CONTRACTS
from migration.transformers import SapTypeTransformer
from migration.utils import (
    IdempotencyTracker,
    MetricsEmitter,
    configure_logging,
    get_logger,
)

REQUIRED_ARGS = [
    "JOB_NAME", "contract_fqn", "batch_id", "extract_path",
    "redshift_staging_path", "redshift_iam_role",
]


def main(args: dict) -> int:
    configure_logging(level="INFO", fmt="json")
    log = get_logger(__name__, job=args["JOB_NAME"], batch_id=args["batch_id"])

    sap_table = args["contract_fqn"].split(".")[-1]
    contract = TABLE_CONTRACTS.get(sap_table)
    if contract is None:
        log.error("unknown_contract", fqn=args["contract_fqn"])
        return 2

    region = args.get("region", "us-east-1")
    pg_secret = args.get("pg_secret_id", "sap-mdm/prod/postgres")
    rs_secret = args.get("rs_secret_id", "sap-mdm/prod/redshift")

    tracker = IdempotencyTracker(
        args.get("idempotency_table", "sap-migration-audit"), region=region
    )
    if tracker.is_processed(args["JOB_NAME"], args["batch_id"]):
        return 0

    tracker.mark_started(args["JOB_NAME"], args["batch_id"])
    metrics = MetricsEmitter("SAP/Migration/Load", region=region)
    spark = _build_spark(args["JOB_NAME"])

    try:
        # 1. Read extracted Parquet from S3
        raw_df = spark.read.parquet(args["extract_path"])
        log.info("loaded_raw", path=args["extract_path"], rows=raw_df.count())

        # 2. Transform
        transformer = SapTypeTransformer()
        transformed = transformer.transform(raw_df, contract)

        # 3. Apply PII masking for the analytical (Redshift) target only
        redshift_df = transformer.apply_pii_masking(transformed, contract)

        # 4. Load to Postgres
        pg_loader = PostgresLoader(pg_secret, region=region)
        pg_result = pg_loader.load(transformed, contract)
        log.info(
            "pg_load_result",
            success=pg_result.success,
            rows=pg_result.rows_loaded,
            duration_s=pg_result.duration_seconds,
        )
        metrics.emit(
            "PostgresRowsLoaded",
            pg_result.rows_loaded,
            dimensions={"Table": contract.target_postgres_table},
        )

        # 5. Load to Redshift
        rs_loader = RedshiftLoader(
            rs_secret,
            staging_s3_path=args["redshift_staging_path"],
            iam_role_arn=args["redshift_iam_role"],
            region=region,
        )
        rs_result = rs_loader.load(redshift_df, contract)
        log.info(
            "rs_load_result",
            success=rs_result.success,
            rows=rs_result.rows_loaded,
            duration_s=rs_result.duration_seconds,
        )
        metrics.emit(
            "RedshiftRowsLoaded",
            rs_result.rows_loaded,
            dimensions={"Table": contract.target_redshift_table},
        )

        # 6. Validate both targets succeeded
        if not pg_result.success or not rs_result.success:
            errors = []
            if not pg_result.success:
                errors.append(f"Postgres: {pg_result.error}")
            if not rs_result.success:
                errors.append(f"Redshift: {rs_result.error}")
            raise RuntimeError("; ".join(errors))

        tracker.mark_succeeded(
            args["JOB_NAME"],
            args["batch_id"],
            row_count=max(pg_result.rows_loaded, rs_result.rows_loaded),
        )
        return 0

    except Exception as exc:
        log.error("load_job_failed", error=str(exc))
        tracker.mark_failed(args["JOB_NAME"], args["batch_id"], str(exc))
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
        parser.add_argument("--JOB_NAME", default="sap-load-local")
        parser.add_argument("--contract_fqn", required=True)
        parser.add_argument("--batch_id", required=True)
        parser.add_argument("--extract_path", required=True)
        parser.add_argument("--redshift_staging_path", required=True)
        parser.add_argument("--redshift_iam_role", required=True)
        parser.add_argument("--region", default="us-east-1")
        parser.add_argument("--pg_secret_id", default="sap-mdm/prod/postgres")
        parser.add_argument("--rs_secret_id", default="sap-mdm/prod/redshift")
        parser.add_argument("--idempotency_table", default="sap-migration-audit")
        parsed = parser.parse_args()
        args = vars(parsed)

    sys.exit(main(args))
