#!/usr/bin/env bash
# Initiate a rollback via the rollback Step Function.
# Usage: bash scripts/rollback.sh <batch_id> [strategy]

set -euo pipefail

BATCH_ID="${1:-}"
STRATEGY="${2:-restore_snapshot}"

if [[ -z "$BATCH_ID" ]]; then
    echo "Usage: $0 <batch_id> [restore_snapshot|delete_by_batch]"
    exit 1
fi

echo "⚠️  ROLLBACK REQUESTED"
echo "Batch ID: $BATCH_ID"
echo "Strategy: $STRATEGY"
echo

read -p "Confirm rollback (type ROLLBACK to proceed): " CONFIRM
if [[ "$CONFIRM" != "ROLLBACK" ]]; then
    echo "Aborted."
    exit 1
fi

ROLLBACK_SM_ARN=$(cd infra/terraform && terraform output -raw rollback_sm_arn)

aws stepfunctions start-execution \
    --state-machine-arn "$ROLLBACK_SM_ARN" \
    --name "rollback-${BATCH_ID}-$(date +%s)" \
    --input "{\"batch_id\": \"${BATCH_ID}\", \"rollback_strategy\": \"${STRATEGY}\"}"

echo "=== Rollback initiated ==="
echo "Check progress in Step Functions console"
echo "Notifications will go to SNS topic"
