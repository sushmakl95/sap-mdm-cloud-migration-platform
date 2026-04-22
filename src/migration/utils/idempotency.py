"""DynamoDB-backed idempotency + watermark tracker.

Two responsibilities:
  1. **Idempotency**: prevent duplicate processing of the same batch. Before
     doing anything expensive, the pipeline checks 'has batch X already run?'
     If yes, it's a no-op. If no, it's atomically marked in-flight.

  2. **Watermarks**: for incremental loads, we need to remember 'what was the
     last successful timestamp loaded?' We store this per-(job, contract).

DynamoDB is the right choice:
  - Single-digit ms p99 reads
  - Conditional writes give true atomicity for the idempotency check
  - TTL auto-expires old idempotency records
  - Point-in-time recovery for disaster scenarios
"""

from __future__ import annotations

import time
from datetime import datetime, timedelta

import boto3
from botocore.exceptions import ClientError

from migration.utils.logging_config import get_logger

log = get_logger(__name__, component="idempotency")


class IdempotencyTracker:
    """Batch idempotency + watermark management via DynamoDB.

    Table schema (created by Terraform):
        pk (HASH):      {job_name}#{batch_id} OR watermark#{job}#{table}
        status:         STARTED | SUCCEEDED | FAILED
        started_at:     ISO8601
        completed_at:   ISO8601
        row_count:      int
        error_message:  string
        ttl:            epoch seconds (auto-delete after 30 days)
    """

    def __init__(self, table_name: str, region: str = "us-east-1"):
        self.table_name = table_name
        self.region = region
        self._table = boto3.resource("dynamodb", region_name=region).Table(table_name)

    # -------------------------------------------------------------------------
    # Idempotency
    # -------------------------------------------------------------------------
    def is_processed(self, job_name: str, batch_id: str) -> bool:
        """Return True iff this batch has completed successfully before."""
        pk = self._batch_pk(job_name, batch_id)
        try:
            response = self._table.get_item(Key={"pk": pk})
        except ClientError as exc:
            log.error("dynamodb_get_failed", pk=pk, error=str(exc))
            raise

        item = response.get("Item")
        return bool(item) and item.get("status") == "SUCCEEDED"

    def mark_started(self, job_name: str, batch_id: str) -> bool:
        """Atomically mark a batch as STARTED. Returns False if already started
        or already succeeded (caller should skip)."""
        pk = self._batch_pk(job_name, batch_id)
        ttl = int((datetime.utcnow() + timedelta(days=30)).timestamp())

        try:
            self._table.put_item(
                Item={
                    "pk": pk,
                    "status": "STARTED",
                    "started_at": datetime.utcnow().isoformat(),
                    "ttl": ttl,
                },
                ConditionExpression="attribute_not_exists(pk) OR #s = :failed",
                ExpressionAttributeNames={"#s": "status"},
                ExpressionAttributeValues={":failed": "FAILED"},
            )
            return True
        except self._table.meta.client.exceptions.ConditionalCheckFailedException:
            log.info("batch_already_in_flight_or_succeeded", pk=pk)
            return False

    def mark_succeeded(self, job_name: str, batch_id: str, row_count: int = 0) -> None:
        self._update_status(job_name, batch_id, "SUCCEEDED", row_count=row_count)

    def mark_failed(self, job_name: str, batch_id: str, error: str) -> None:
        self._update_status(job_name, batch_id, "FAILED", error_message=error)

    def _update_status(
        self,
        job_name: str,
        batch_id: str,
        status: str,
        row_count: int = 0,
        error_message: str | None = None,
    ) -> None:
        pk = self._batch_pk(job_name, batch_id)
        update_expr = "SET #s = :s, completed_at = :ct, row_count = :rc"
        values = {
            ":s": status,
            ":ct": datetime.utcnow().isoformat(),
            ":rc": row_count,
        }
        if error_message:
            update_expr += ", error_message = :err"
            values[":err"] = error_message[:1000]

        self._table.update_item(
            Key={"pk": pk},
            UpdateExpression=update_expr,
            ExpressionAttributeNames={"#s": "status"},
            ExpressionAttributeValues=values,
        )

    def _batch_pk(self, job_name: str, batch_id: str) -> str:
        return f"batch#{job_name}#{batch_id}"

    # -------------------------------------------------------------------------
    # Watermarks
    # -------------------------------------------------------------------------
    def get_watermark(self, job_name: str, contract_fqn: str) -> str | None:
        pk = self._watermark_pk(job_name, contract_fqn)
        response = self._table.get_item(Key={"pk": pk})
        item = response.get("Item")
        return item.get("watermark") if item else None

    def set_watermark(self, job_name: str, contract_fqn: str, watermark: str) -> None:
        pk = self._watermark_pk(job_name, contract_fqn)
        self._table.put_item(
            Item={
                "pk": pk,
                "watermark": watermark,
                "updated_at": datetime.utcnow().isoformat(),
                # No TTL — watermarks should persist
            }
        )

    def _watermark_pk(self, job_name: str, contract_fqn: str) -> str:
        return f"watermark#{job_name}#{contract_fqn}"


def exponential_backoff_sleep(attempt: int, base_seconds: float = 1.0, max_seconds: float = 60.0) -> None:
    """Simple exponential backoff for retries."""
    sleep_time = min(base_seconds * (2 ** attempt), max_seconds)
    time.sleep(sleep_time)
