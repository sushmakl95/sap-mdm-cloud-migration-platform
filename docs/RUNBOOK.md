# Operations Runbook

## Common operational scenarios

### 1. Nightly batch migration didn't run

**Symptoms:** No SNS notification that batch migration started at 2am UTC.

**Diagnosis:**
```bash
aws scheduler get-schedule \
  --name sap-mdm-prod-nightly-migration \
  --group-name sap-mdm-prod-schedules

aws events list-rule-names-by-target \
  --target-arn $(terraform output -raw batch_migrate_sm_arn)
```

**Fixes (in order of likelihood):**

1. **Schedule is paused**: check `state` in the response — if `DISABLED`, re-enable:
   ```bash
   aws scheduler update-schedule --name sap-mdm-prod-nightly-migration ...
   ```

2. **Scheduler IAM role can't assume target**: check IAM role trust policy + SFN permissions.

3. **Bad cron expression**: `cron(0 2 * * ? *)` is correct. `cron(0 2 * * *)` (5 fields) is wrong — EventBridge wants 6.

4. **Manual trigger to get back on track**:
   ```bash
   aws stepfunctions start-execution \
     --state-machine-arn $(terraform output -raw batch_migrate_sm_arn) \
     --name "manual-recovery-$(date +%s)" \
     --input '{"contracts":["HANA:SAPSR3.MARA","HANA:SAPSR3.KNA1","HANA:SAPSR3.LFA1"],"mode":"incremental","batch_id":"recovery-manual"}'
   ```

### 2. One table's migration keeps failing

**Symptoms:** Same table (e.g., MARA) fails every night. Other tables fine.

**Diagnosis:**
```bash
# Latest failure details
aws stepfunctions list-executions \
  --state-machine-arn $(terraform output -raw migrate_table_sm_arn) \
  --status-filter FAILED \
  --max-items 5

# Pull logs for the failing execution
aws stepfunctions describe-execution \
  --execution-arn <arn-from-above>

# Glue job logs specifically
aws logs tail /aws-glue/jobs/sap-mdm-prod --filter-pattern ERROR --since 24h
```

**Common causes:**

| Error | Cause | Fix |
|---|---|---|
| `JDBC connection timeout` | SAP network blip or HANA restart | Re-run the batch manually; alert SAP basis if persistent |
| `Reconciliation FAILED: row_count` | Schema change in source | Update contract + redeploy |
| `Reconciliation FAILED: null_ratio` | Column renamed in source | Update contract + redeploy |
| `Redshift COPY failed` | IAM role lost S3 access | Check redshift-copy-role IAM policy |
| `Postgres: duplicate key value violates unique constraint` | Parallel batches ran concurrently | Check DynamoDB for stuck STARTED records; clean up |

### 3. DMS CDC lag is climbing

**Symptoms:** CloudWatch alarm "dms-lag" fires. Dashboard shows CDCLatencyTarget > 60s and rising.

**Diagnosis:**
```bash
aws dms describe-replication-tasks \
  --filters Name=replication-task-id,Values=sap-mdm-prod-cdc

aws dms describe-replication-instances \
  --filter Name=replication-instance-id,Values=sap-mdm-prod-dms-instance
```

Check:
- `Status` of task: should be RUNNING, not STOPPED or ERROR
- `ReplicationInstance` metrics: CPU, memory, network — CPU > 80% means resize
- SAP HANA logs: is the source log reader throttling?

**Fixes:**

1. **Resize replication instance** (most common):
   ```bash
   aws dms modify-replication-instance \
     --replication-instance-identifier sap-mdm-prod-dms-instance \
     --replication-instance-class dms.r5.xlarge \
     --apply-immediately
   ```

2. **Restart task** if it's stuck:
   ```bash
   aws dms stop-replication-task --replication-task-arn <arn>
   # wait for STOPPED
   aws dms start-replication-task --replication-task-arn <arn> --start-replication-task-type resume-processing
   ```

3. **Check target S3 throttling**: sometimes S3 throttles writes from a single DMS instance. Enable "Parallel apply" in task settings (already on by default in our config).

### 4. Target DB disk full (RDS / Redshift)

**Symptoms:** CloudWatch alarm "rds-disk-high" or "redshift-disk-high" fires.

**RDS:**

```bash
aws rds modify-db-instance \
  --db-instance-identifier sap-mdm-prod-rds \
  --allocated-storage 500 \
  --apply-immediately

# Enable storage autoscaling (one-time setup)
aws rds modify-db-instance \
  --db-instance-identifier sap-mdm-prod-rds \
  --max-allocated-storage 1000 \
  --apply-immediately
```

**Redshift:**

Options:
1. **VACUUM** — reclaim space from deleted rows: `VACUUM DELETE ONLY schemaname.tablename;`
2. **Resize cluster** (takes 30 min):
   ```bash
   aws redshift modify-cluster \
     --cluster-identifier sap-mdm-prod-rs \
     --number-of-nodes 3
   ```
3. **Drop old audit data** — the `_loaded_at` column makes this straightforward:
   ```sql
   DELETE FROM mdm.material_master WHERE _loaded_at < DATEADD(year, -2, CURRENT_DATE);
   ```

### 5. Idempotency table in bad state (stuck STARTED records)

**Symptoms:** Batches won't re-run because DynamoDB still shows STARTED.

**Diagnosis:**
```bash
aws dynamodb scan \
  --table-name sap-mdm-prod-audit \
  --filter-expression "#s = :started" \
  --expression-attribute-names '{"#s":"status"}' \
  --expression-attribute-values '{":started":{"S":"STARTED"}}' \
  --query 'Items[*].pk.S'
```

If any of these have `started_at` more than 24h ago, they're stuck.

**Fix (one-off):**
```bash
aws dynamodb update-item \
  --table-name sap-mdm-prod-audit \
  --key '{"pk":{"S":"batch#migrate-table#<stuck-batch-id>"}}' \
  --update-expression "SET #s = :failed" \
  --expression-attribute-names '{"#s":"status"}' \
  --expression-attribute-values '{":failed":{"S":"FAILED"}}'
```

This allows the batch to be re-triggered.

### 6. New SAP table to add

This should be a standard operation, not an exception:

```bash
# 1. Add contract to src/migration/schemas/mdm_contracts.py
# 2. Generate DDL
sap-migrate generate-ddl --output-dir ./sql/target

# 3. Apply DDL to prod (via your schema migration tool — e.g., Liquibase, sqitch, or manually)
psql $PGURL -f sql/target/postgres_<newtable>.sql
psql $RSURL -f sql/target/redshift_<newtable>.sql

# 4. Re-generate DMS task (adds table to include-list)
sap-migrate dms-config --output config/dms_task_config.json
# Update the DMS task in AWS (Terraform will pick up the config change)

# 5. Commit + PR + merge

# 6. Terraform apply
cd infra/terraform
terraform apply -var-file=envs/prod.tfvars

# 7. Trigger initial backfill
aws stepfunctions start-execution \
  --state-machine-arn $(terraform output -raw migrate_table_sm_arn) \
  --input '{"contract_fqn":"HANA:SAPSR3.NEWTABLE","mode":"full","batch_id":"initial-backfill"}'
```

### 7. Secrets rotation

Postgres + Redshift support automated rotation via Secrets Manager:

```bash
aws secretsmanager rotate-secret \
  --secret-id sap-mdm-prod/postgres \
  --rotation-lambda-arn <rotation-lambda-arn> \
  --rotation-rules AutomaticallyAfterDays=90
```

After rotation, restart any long-running jobs so they pick up the new secret (our Python `get_secret` uses `@lru_cache` — restart invalidates it).

SAP HANA credentials: **rotated manually by the SAP Basis team**, not automated by AWS. Our runbook:
1. Basis team creates new credentials in SAP
2. Basis team updates the secret: `aws secretsmanager put-secret-value ...`
3. Running Glue jobs finish current batch with old creds, next batch uses new
4. Validate nightly batch continues to succeed

## On-call paging

### Severity levels

**P0** — Revenue impact or data loss:
- 100% of batch migrations failing
- Data corruption detected in prod target
- DMS completely stopped (>1 hour lag growth)

**P1** — Degraded service:
- Single table consistently failing (≥3 consecutive runs)
- DMS lag between 5-60 minutes
- Reconciliation warnings across multiple tables

**P2** — No immediate impact:
- Single table failure (1 night)
- CloudWatch alarm noise
- Cost anomaly

### Paging contacts

(Placeholders — fill in per your organization)

| Severity | Channel | SLA |
|---|---|---|
| P0 | PagerDuty `sap-mdm-oncall` | 5 min response |
| P1 | Slack `#sap-mdm-alerts` | 30 min |
| P2 | Slack `#sap-mdm-alerts` | Next business day |

## Handy CLI snippets

```bash
# List recent batch executions
aws stepfunctions list-executions \
  --state-machine-arn $(terraform output -raw batch_migrate_sm_arn) \
  --max-items 10

# Get the latest SNS notification
aws sns list-subscriptions-by-topic \
  --topic-arn $(terraform output -raw notifications_topic_arn)

# Query today's audit trail
aws dynamodb scan \
  --table-name sap-mdm-prod-audit \
  --filter-expression "begins_with(pk, :today)" \
  --expression-attribute-values '{":today":{"S":"batch#migrate-table#'$(date +%Y%m%d)'"}}'

# Tail Glue job logs in real time
aws logs tail /aws-glue/jobs/sap-mdm-prod --follow
```
