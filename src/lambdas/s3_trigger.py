"""S3 trigger Lambda: fires Step Functions when DMS writes CDC files.

Invocation: S3 PUT event on the CDC zone.
Output: Starts the `migrate-table-sm` for the affected table.

Key safety: we DEBOUNCE — DMS writes hundreds of small files per minute during
heavy CDC load. We only kick off one state-machine run per (table, 5-min window)
via DynamoDB conditional writes.
"""

from __future__ import annotations

import json
import os
import uuid
from datetime import datetime, timedelta

import boto3

sfn = boto3.client("stepfunctions")
ddb = boto3.resource("dynamodb")

IDEMPOTENCY_TABLE = os.environ["IDEMPOTENCY_TABLE"]
STATE_MACHINE_ARN = os.environ["STATE_MACHINE_ARN"]
DEBOUNCE_WINDOW_MINUTES = int(os.environ.get("DEBOUNCE_WINDOW_MINUTES", "5"))


def handler(event: dict, context) -> dict:
    """AWS Lambda handler."""
    records = event.get("Records", [])
    tables_triggered = set()

    for record in records:
        s3_event = record.get("s3", {})
        key = s3_event.get("object", {}).get("key", "")

        # Parse key: cdc/sap/<TABLE>/<yyyy>/<mm>/<dd>/<hh>/<file>.parquet
        parts = key.split("/")
        if len(parts) < 3 or parts[0] != "cdc" or parts[1] != "sap":
            continue
        sap_table = parts[2]

        if _should_debounce(sap_table):
            continue

        batch_id = f"cdc-{sap_table}-{uuid.uuid4().hex[:12]}"
        sfn_input = {
            "contract_fqn": f"HANA:SAPSR3.{sap_table}",
            "batch_id": batch_id,
            "mode": "incremental",
            "trigger_source": "s3_cdc",
            "trigger_key": key,
        }

        sfn.start_execution(
            stateMachineArn=STATE_MACHINE_ARN,
            name=batch_id,
            input=json.dumps(sfn_input),
        )
        tables_triggered.add(sap_table)

    return {
        "statusCode": 200,
        "body": json.dumps({
            "tables_triggered": list(tables_triggered),
            "records_processed": len(records),
        }),
    }


def _should_debounce(sap_table: str) -> bool:
    """Return True if we've started an SFN run for this table recently."""
    table = ddb.Table(IDEMPOTENCY_TABLE)
    pk = f"debounce#{sap_table}"
    now = datetime.utcnow()
    ttl = int((now + timedelta(minutes=DEBOUNCE_WINDOW_MINUTES)).timestamp())

    try:
        table.put_item(
            Item={
                "pk": pk,
                "triggered_at": now.isoformat(),
                "ttl": ttl,
            },
            ConditionExpression="attribute_not_exists(pk)",
        )
        return False
    except table.meta.client.exceptions.ConditionalCheckFailedException:
        return True
