# Rollback Playbook

## When to roll back

A rollback is a **recovery action** that reverts the platform to a known-good earlier state. It's not a fix — it's a pause button while you investigate.

**Triggers for rollback (automatic):**
- Phase 4 canary detects > 0.1% result diff between old and new
- Phase 5 critical service SLO breach attributed to new platform
- Active data corruption detected post-load (Layer 4 checksum failure after load succeeded)

**Triggers for rollback (human-initiated):**
- Executive call during incident
- External audit finding
- Major schema change landed incorrectly

**NOT triggers for rollback:**
- Individual query slowness (tune instead)
- Individual failed batch (re-run instead)
- Minor reconciliation warnings (investigate inline)

## Rollback strategies by phase

| Phase | Strategy | Cost | Duration |
|---|---|---|---|
| 1 (shadow read) | N/A — consumers untouched | $0 | N/A |
| 2 (dual write) | Disable new-system write path (feature flag) | ~$0 | 1 min |
| 3 (10% read) | Revert traffic split to 0% | ~$0 | 5 min |
| 4 (primary read) | Revert traffic split to 0%, old system re-authoritative | 24-48h manual reconciliation | 1 hour + reconciliation |
| 5 (decom) | Full re-migration in reverse | $$$ | 4-8 weeks |

## Phase 4 rollback (the most likely real rollback)

This is the specific playbook for the case we've seen most often in practice.

### Preconditions

- Old SAP system has been receiving writes the whole time (dual-write active)
- Last DMS checkpoint lag ≤ 5 minutes at start of rollback
- Service owners have been paged

### Procedure (automated — rollback SFN)

```bash
aws stepfunctions start-execution \
  --state-machine-arn $(terraform output -raw rollback_sm_arn) \
  --input '{"batch_id": "phase4-<timestamp>", "rollback_strategy": "restore_snapshot"}'
```

The rollback state machine:

1. **Validates rollback is allowed** — checks DynamoDB audit for phase state
2. **Cuts traffic split** — updates feature flag (service has pre-authorized this)
3. **Snapshots target** — takes RDS + Redshift snapshots for forensics
4. **Re-ingests any in-flight CDC** — drains DMS queue to the old system
5. **Notifies stakeholders** — SNS with rollback record

Estimated time: **5 minutes** for the technical cut. Follow-up reconciliation between old and new states may take 24-48h.

### Procedure (manual — if SFN is unavailable)

**Step 1: Cut traffic immediately.**
```bash
# Via your feature flag system (example: LaunchDarkly)
ldcli flag update sap-mdm-read-target --value "sap"
```

**Step 2: Verify cut took effect.**
```bash
# Watch the metrics — expect new-platform read traffic to drop to 0 within 30s
aws cloudwatch get-metric-statistics \
  --namespace SAP/Migration/Reads \
  --metric-name ReadsPerSecond \
  --start-time $(date -u -d '5 minutes ago' +%Y-%m-%dT%H:%M:%S) \
  --end-time $(date -u +%Y-%m-%dT%H:%M:%S) \
  --period 60 \
  --statistics Average \
  --dimensions Name=Target,Value=aws
```

**Step 3: Snapshot both target databases.**
```bash
# RDS
aws rds create-db-snapshot \
  --db-instance-identifier sap-mdm-prod-rds \
  --db-snapshot-identifier sap-mdm-prod-rollback-$(date +%Y%m%d-%H%M)

# Redshift
aws redshift create-cluster-snapshot \
  --cluster-identifier sap-mdm-prod-rs \
  --snapshot-identifier sap-mdm-prod-rollback-$(date +%Y%m%d-%H%M)
```

**Step 4: Page the incident commander.**

**Step 5: Post-rollback, run reconciliation between old system (now authoritative) and new system (now quarantined).**
```bash
sap-migrate validate \
  --mode rollback-delta \
  --from-phase 4 \
  --to-phase 3
```

### What NOT to do during rollback

- **Do not delete target data.** Snapshot it for forensics. The bug investigation needs it.
- **Do not kill in-flight batches forcefully.** Let them finish or fail gracefully; their idempotency will handle re-runs.
- **Do not change application code during rollback.** Use feature flags only. Code changes introduce new risk.
- **Do not try to "fix and roll forward"** during the incident. Roll back first, then diagnose.

## Post-rollback review

Within 5 business days of any rollback, hold a blameless incident review:

1. **Timeline reconstruction** — what happened, when, what was observed
2. **Root cause analysis** — 5 whys
3. **What got us here** — systemic issues that allowed the bug to reach production
4. **Action items** — validation gap fixes, monitoring additions
5. **Reconciliation plan** — how we'll re-attempt the phase transition

## Rollback testing

Rollback procedures MUST be tested in non-prod before Phase 4. Our repo includes a staging-only `test-rollback.sh` that:

1. Triggers a controlled rollback in staging
2. Measures cut-over time
3. Verifies all audit trail writes
4. Reports success/failure to the team

Run quarterly to prevent procedure drift.

## References

- [AWS Well-Architected Framework — Reliability Pillar](https://docs.aws.amazon.com/wellarchitected/latest/reliability-pillar/welcome.html)
- Google SRE Book Ch. 14 — Managing Incidents
