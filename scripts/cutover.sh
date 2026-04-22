#!/usr/bin/env bash
# Cutover helper — executes a phase transition for the migration.
# Usage: bash scripts/cutover.sh <from_phase> <to_phase>

set -euo pipefail

FROM_PHASE="${1:-}"
TO_PHASE="${2:-}"

if [[ -z "$FROM_PHASE" || -z "$TO_PHASE" ]]; then
    echo "Usage: $0 <from_phase> <to_phase>"
    echo "Phases: 1 (shadow-read), 2 (dual-write), 3 (10%-read), 4 (primary-read), 5 (decommission)"
    exit 1
fi

echo "=== Pre-flight check ==="
sap-migrate validate --quick  # runs a sanity reconciliation

echo "=== Confirm cutover approval ==="
echo "From Phase: $FROM_PHASE"
echo "To Phase:   $TO_PHASE"
echo
echo "Required approvals for Phase $TO_PHASE:"
case $TO_PHASE in
    2) echo "  - Data Engineering tech lead" ;;
    3) echo "  - DE lead + consuming service owner" ;;
    4) echo "  - DE lead + service owners + DevOps" ;;
    5) echo "  - All above + Director of Engineering" ;;
esac

read -p "All approvals confirmed? (yes/NO): " CONFIRM
if [[ "$CONFIRM" != "yes" ]]; then
    echo "Aborted."
    exit 1
fi

echo "=== Recording cutover in audit log ==="
aws dynamodb put-item \
    --table-name "sap-mdm-prod-audit" \
    --item "{
        \"pk\": {\"S\": \"cutover#${FROM_PHASE}-to-${TO_PHASE}\"},
        \"initiated_at\": {\"S\": \"$(date -u +%Y-%m-%dT%H:%M:%S)\"},
        \"initiated_by\": {\"S\": \"${USER}\"},
        \"from_phase\": {\"S\": \"${FROM_PHASE}\"},
        \"to_phase\": {\"S\": \"${TO_PHASE}\"}
    }"

echo "=== Ramp instructions for Phase $TO_PHASE ==="
case $TO_PHASE in
    3)
        echo "Update feature flag: sap-mdm-read-target -> 10% new system"
        echo "Via: ldcli flag update sap-mdm-read-target --rules 'percentage:10'"
        ;;
    4)
        echo "Gradual ramp required:"
        echo "  Day 1: 10% -> 25%"
        echo "  Day 3: 25% -> 50%"
        echo "  Day 5: 50% -> 75%"
        echo "  Day 7: 75% -> 100%"
        echo "Each step: 48h bake time"
        ;;
    5)
        echo "Decommission prep:"
        echo "  1. Freeze old system writes (SAP SE06)"
        echo "  2. Take final SAP snapshot"
        echo "  3. Archive audit logs to S3 Object Lock bucket"
        ;;
esac

echo "=== Cutover initiated ==="
