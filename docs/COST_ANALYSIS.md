# Cost Analysis

## Summary

A production deployment of this platform costs approximately **$1,200/month** at moderate scale (10-50 GB daily extract volume, nightly batch + near-real-time CDC). This is a large expense — justified only if the SAP system being migrated is core enterprise infrastructure.

For evaluation / dev / demo, **use the local Docker Compose stack** (instructions in [LOCAL_DEVELOPMENT.md](LOCAL_DEVELOPMENT.md)). Zero cloud cost.

## Full breakdown (us-east-1, on-demand pricing, April 2026)

### Compute

| Resource | Configuration | Monthly cost |
|---|---|---|
| RDS Postgres (Multi-AZ) | db.r6g.large, 200GB gp3 | $180 |
| Redshift | dc2.large × 2 (or ra3.xlplus × 2) | $380 |
| Databricks | i3.xlarge × 2, 10h/day, spot-with-fallback | $220 |
| AWS DMS | dms.r5.large replication instance | $280 |
| AWS Glue | ~100 DPU-hours/month | $44 |
| Lambda | <1M invocations, minimal memory | $2 |
| Step Functions | <10K transitions | $0.25 |
| **Subtotal** | | **$1,106** |

### Network

| Resource | Configuration | Monthly cost |
|---|---|---|
| NAT Gateway | 1 AZ | $32 |
| Data transfer (SAP → AWS via Direct Connect) | 100 GB/day × 30 = 3TB | $60 |
| Data transfer out (to BI tools) | 500 GB/month | $45 |
| VPC Endpoints (S3 Gateway) | 1, regional | $0 (free) |
| **Subtotal** | | **$137** |

### Storage

| Resource | Configuration | Monthly cost |
|---|---|---|
| S3 Raw (Parquet, ~200 GB accumulated) | Standard + lifecycle to Glacier IR | $8 |
| S3 Staging (ephemeral, ~20 GB) | Standard | $1 |
| S3 Audit (WORM, slow growth) | Standard | $2 |
| S3 Glue Scripts | Standard | $0.50 |
| **Subtotal** | | **$12** |

### Observability + Management

| Resource | Configuration | Monthly cost |
|---|---|---|
| CloudWatch Logs (90-day retention) | ~100 GB ingested | $55 |
| CloudWatch Metrics (custom + default) | ~200 custom metrics | $60 |
| DynamoDB audit table (PAY_PER_REQUEST) | ~10K writes/month | $1.50 |
| Secrets Manager | 3 secrets | $1.20 |
| KMS (5 CMKs + rotation) | | $5 |
| SNS | <1K messages | $0.10 |
| **Subtotal** | | **$123** |

### Total: **~$1,378/month** (conservative; includes rounding + buffer)

Many organizations see **$1,000-1,600/month** in real-world deployments — within 15% of this estimate.

## Cost drivers (in order)

1. **Redshift** (28%) — keep it when BI access patterns justify it; otherwise skip for this use case
2. **DMS** (20%) — fixed cost while CDC is active; turn off during Phase 1 dry runs
3. **Databricks** (16%) — auto-scale down + spot instances are already applied
4. **RDS** (13%) — Multi-AZ doubles the base cost; use single-AZ in non-prod

## Optimization playbook

### Immediate wins (20-40% savings)

**1. Use Redshift Serverless for bursty workloads**
If your analytical queries are bursty (e.g., 2 hours/day of BI activity), Redshift Serverless at $0.50/RPU-hour often costs **less than half** of a provisioned cluster. Requires adaption for consistent-spend vs bursty-spend tradeoff.

**2. DMS Free Tier + scheduling**
In Phase 1 (shadow read), you don't need DMS active 24/7. Schedule the DMS task to run 2h/day for validation. Savings: ~$200/month.

**3. Databricks spot instances**
Already applied in our cluster policy. Savings: ~60% vs on-demand.

**4. Skip NAT Gateway in dev**
In dev, instead of a NAT Gateway ($32/month + data transfer), use public subnets + a bastion for egress. Savings: ~$40/month for dev environments.

**5. Lower RDS instance class in non-prod**
db.t4g.medium costs ~$25/month vs db.r6g.large's $180. Savings: ~$150/month for dev/staging combined.

### Medium-term wins (additional 10-20% savings)

**6. Reserved Instances**
1-year RI commits save ~40% on RDS + Redshift. If your deployment is >6 months committed, buy RIs.

**7. Glue job right-sizing**
Default: 10 workers × G.1X. Most SAP tables run happily at 5 workers. Measure first, then right-size per-job. Savings: ~30% on Glue bill.

**8. S3 Intelligent-Tiering**
Automatically moves cold audit data to cheaper tiers with no access penalty for reads. Enables ~50% savings on S3 with zero code change.

### Long-term wins (additional 10% savings)

**9. Consolidate to Aurora Serverless v2 instead of RDS**
If your operational write load is bursty, Aurora Serverless v2 provisions capacity per-second. Net savings depend on load pattern — typically 20-30%.

**10. Drop Redshift for ClickHouse on EC2 if your query pattern is simple**
Redshift shines at MPP aggregations. If 90% of your queries are "look up N rows by a filter", ClickHouse at $100/month handles the workload for far less than Redshift's $380. Significant engineering investment, only worth it if budget pressure is extreme.

## What you MUST pay for (don't skimp)

**1. Multi-AZ for RDS in prod.** Saving $90/month by going single-AZ exposes you to hours of downtime on any AZ blip. Don't.

**2. KMS CMKs.** They cost $1/month each. Don't share them across layers to "save" a few dollars — blast-radius containment is worth it.

**3. CloudWatch Logs retention of 90 days.** Shorter retention means you'll lack forensic data during incidents. Cheap insurance.

**4. S3 Object Lock for audit bucket.** Compliance mode prevents even root users from deleting audit trails. Required for SOX/GDPR.

## Monthly cost monitoring

Our Terraform creates:

- **AWS Budget** (set via `alarm_email_recipients` variable) — alerts at 80% and 100% of a monthly threshold
- **Cost allocation tags** — every resource tagged with `Project`, `Environment`, `CostCenter` for drill-down in Cost Explorer

Set your budget threshold to something realistic — we recommend **monthly estimate + 20%** so you're alerted on genuine anomalies, not routine noise.

## Cost comparison to alternatives

For context, what the typical alternatives cost at equivalent scale:

| Alternative | Monthly cost | Notes |
|---|---|---|
| This repo (DIY on AWS) | ~$1,200 | Full data ownership, custom logic |
| Fivetran + Snowflake | ~$3,500 | Turnkey but $$$$, less customizable |
| Stitch + Redshift | ~$800 | Cheaper but limited CDC options |
| Mulesoft + self-hosted targets | ~$6,000 | Enterprise middleware overhead |

For most enterprise SAP migrations we've done, this repo's approach at ~$1,200 is the sweet spot — affordable enough to run for months of migration, flexible enough to handle SAP's edge cases.
