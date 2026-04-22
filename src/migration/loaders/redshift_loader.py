"""Redshift loader — S3 COPY into staging, then MERGE into target.

Why S3 COPY?
  INSERT into Redshift is ~100-1000x slower than COPY. A 10M-row table that
  takes 2-3 hours via INSERT takes 2-3 minutes via COPY. The S3 round-trip
  adds marginal overhead that's dwarfed by the write speed gain.

Pattern:
  1. Write the batch as Parquet to S3 staging bucket
  2. CREATE TEMP TABLE with same schema as target
  3. COPY staging-S3 → temp table (parallel across slices)
  4. DELETE matching PKs from target
  5. INSERT temp → target
  6. Clean up temp

This pattern gives us ACID semantics (all-or-nothing transaction) with the
raw throughput of COPY.
"""

from __future__ import annotations

import time
import uuid
from dataclasses import dataclass

import redshift_connector
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from migration.schemas import TableContract
from migration.utils.logging_config import get_logger
from migration.utils.secrets import get_secret

log = get_logger(__name__, component="redshift_loader")


@dataclass
class RedshiftLoadResult:
    target: str
    rows_loaded: int
    s3_staging_path: str
    duration_seconds: float = 0.0
    error: str | None = None

    @property
    def success(self) -> bool:
        return self.error is None


class RedshiftLoader:
    """Redshift COPY-from-S3 loader with MERGE upsert pattern."""

    def __init__(
        self,
        secret_id: str,
        staging_s3_path: str,
        iam_role_arn: str,
        region: str = "us-east-1",
    ):
        self.secret_id = secret_id
        self.staging_s3_path = staging_s3_path.rstrip("/")
        self.iam_role_arn = iam_role_arn
        self.region = region

    def _connect(self) -> redshift_connector.Connection:
        creds = get_secret(self.secret_id, region=self.region)
        conn = redshift_connector.connect(
            host=creds["host"],
            port=int(creds.get("port", 5439)),
            database=creds["database"],
            user=creds["username"],
            password=creds["password"],
            ssl=True,
        )
        conn.autocommit = False
        return conn

    def load(self, df: DataFrame, contract: TableContract) -> RedshiftLoadResult:
        """Write DF to S3, then COPY + MERGE into Redshift."""
        start = time.time()

        batch_id = uuid.uuid4().hex[:12]
        target = contract.target_fqn_redshift
        s3_batch_path = f"{self.staging_s3_path}/{contract.target_redshift_table}/{batch_id}/"

        # Select target columns + audit columns
        columns = [m.target_name for m in contract.columns if not m.is_audit]
        ordered = df.select(*columns)
        ordered = (
            ordered
            .withColumn("_source_system", F.lit(contract.sap_system))
            .withColumn("_extracted_at", F.current_timestamp())
            .withColumn("_loaded_at", F.current_timestamp())
            .withColumn("_is_current", F.lit(True))
        )

        # 1. Write to S3 as Parquet
        ordered.write.mode("overwrite").parquet(s3_batch_path)
        row_count = ordered.count()
        log.info(
            "redshift_staging_written",
            target=target,
            s3_path=s3_batch_path,
            rows=row_count,
        )

        # 2. COPY + MERGE
        conn = self._connect()
        try:
            cur = conn.cursor()
            temp_table = f"tmp_{contract.target_redshift_table}_{batch_id}"

            # Create temp table with same structure as target
            cur.execute(f"CREATE TEMP TABLE {temp_table} (LIKE {target})")

            # COPY from S3
            cur.execute(
                f"COPY {temp_table} FROM '{s3_batch_path}' "
                f"IAM_ROLE '{self.iam_role_arn}' "
                f"FORMAT AS PARQUET"
            )

            # MERGE: delete-then-insert pattern (faster than native MERGE on small/medium batches)
            if contract.primary_keys:
                pk_predicate = " AND ".join(
                    f"t.{c} = s.{c}" for c in contract.primary_keys
                )
                cur.execute(
                    f"DELETE FROM {target} t USING {temp_table} s WHERE {pk_predicate}"
                )

            cur.execute(f"INSERT INTO {target} SELECT * FROM {temp_table}")

            conn.commit()
            duration = time.time() - start
            log.info(
                "redshift_load_complete",
                target=target,
                rows=row_count,
                duration_s=round(duration, 2),
            )
            return RedshiftLoadResult(
                target=target,
                rows_loaded=row_count,
                s3_staging_path=s3_batch_path,
                duration_seconds=duration,
            )
        except Exception as exc:
            conn.rollback()
            log.error("redshift_load_failed", target=target, error=str(exc))
            return RedshiftLoadResult(
                target=target,
                rows_loaded=0,
                s3_staging_path=s3_batch_path,
                error=str(exc),
            )
        finally:
            conn.close()


class RedshiftDdlExecutor:
    """Applies generated DDL to Redshift."""

    def __init__(self, secret_id: str, region: str = "us-east-1"):
        self.secret_id = secret_id
        self.region = region

    def apply(self, ddl: str) -> None:
        creds = get_secret(self.secret_id, region=self.region)
        conn = redshift_connector.connect(
            host=creds["host"],
            port=int(creds.get("port", 5439)),
            database=creds["database"],
            user=creds["username"],
            password=creds["password"],
            ssl=True,
        )
        conn.autocommit = False
        try:
            cur = conn.cursor()
            cur.execute(ddl)
            conn.commit()
            log.info("redshift_ddl_applied")
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()
