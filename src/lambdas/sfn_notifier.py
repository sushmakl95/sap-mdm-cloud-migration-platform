"""Step Functions state-change notifier Lambda.

Invocation: EventBridge rule on aws.states state-change events.
Output: formatted SNS message + Slack webhook (via SNS subscription).

Handles: SUCCEEDED, FAILED, TIMED_OUT, ABORTED.
"""

from __future__ import annotations

import json
import os
from datetime import datetime

import boto3

sns = boto3.client("sns")
SNS_TOPIC_ARN = os.environ["SNS_TOPIC_ARN"]


def handler(event: dict, context) -> dict:
    detail = event.get("detail", {})
    status = detail.get("status", "UNKNOWN")
    state_machine_arn = detail.get("stateMachineArn", "")
    execution_arn = detail.get("executionArn", "")

    # Parse execution input for context
    try:
        execution_input = json.loads(detail.get("input", "{}"))
    except (json.JSONDecodeError, TypeError):
        execution_input = {}

    contract_fqn = execution_input.get("contract_fqn", "unknown")
    batch_id = execution_input.get("batch_id", "unknown")
    mode = execution_input.get("mode", "unknown")

    # Determine severity emoji
    emoji = {
        "SUCCEEDED": "✅",
        "FAILED": "❌",
        "TIMED_OUT": "⏰",
        "ABORTED": "🛑",
    }.get(status, "⚠️")

    subject = f"{emoji} SAP Migration {status}: {contract_fqn}"

    body = {
        "status": status,
        "contract_fqn": contract_fqn,
        "batch_id": batch_id,
        "mode": mode,
        "execution_arn": execution_arn,
        "state_machine_arn": state_machine_arn,
        "started_at": detail.get("startDate"),
        "stopped_at": detail.get("stopDate"),
        "notified_at": datetime.utcnow().isoformat(),
        "region": event.get("region"),
    }

    if status == "FAILED":
        body["cause"] = detail.get("cause", "no cause")
        body["error"] = detail.get("error", "no error")

    sns.publish(
        TopicArn=SNS_TOPIC_ARN,
        Subject=subject[:100],
        Message=json.dumps(body, indent=2, default=str),
    )

    return {"statusCode": 200, "body": json.dumps({"notified": subject})}
