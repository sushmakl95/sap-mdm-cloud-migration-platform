# Architecture

## End-to-end migration flow

```mermaid
flowchart LR
    subgraph SAP["🏢 SAP on-prem"]
        HANA[(SAP HANA)]
        BW[(SAP BW)]
    end

    subgraph Extract["📤 Extract"]
        DMS[AWS DMS CDC]
        GLUE[AWS Glue<br/>bulk PySpark]
        RAW[(S3 bronze<br/>by batch_id)]
    end

    subgraph Validate["✅ Validate DBR 15.4 LTS"]
        DBR[Databricks Runtime 15.4<br/>Asset Bundle]
        NORM[silver / normalize]
        RECON[gold / 5-layer<br/>reconciliation]
    end

    subgraph Publish["🎯 Publish"]
        PG[(RDS Postgres<br/>row lookup)]
        RS[(Redshift<br/>analytics MPP)]
        UC[Unity Catalog]
    end

    subgraph Gov["📒 Governance"]
        OL[OpenLineage Spark listener]
        MQ[Marquez / DataHub]
    end

    subgraph Ops["⚠️ Safety"]
        CUT[Multi-phase cutover<br/>Step Functions]
        ROLL[Automated rollback<br/>snapshot + replay]
    end

    HANA --> DMS --> RAW
    BW --> GLUE --> RAW
    RAW --> DBR --> NORM --> RECON
    RECON --> PG
    RECON --> RS
    RECON --> UC
    DBR -. spark.extraListeners .-> OL --> MQ
    CUT -. orchestrates .-> DBR
    CUT -. triggers .-> ROLL
```

## Why two target warehouses?

Most enterprise data platforms need **both** operational and analytical access patterns, which have opposing storage requirements:

| Pattern | Access type | Store |
|---|---|---|
| "Give me customer 12345's latest address" | Row lookup, microsecond latency | **RDS Postgres** — row-store, indexed on business keys |
| "What was the YoY sales change by country?" | Aggregation over millions of rows | **Redshift** — columnar, MPP, compressed |

Trying to force one store to do both results in either slow BI queries (if you pick Postgres) or unworkable microservice latency (if you pick Redshift). We solve this by writing to both in parallel — the extra cost is an ~8% overhead on the load phase, well worth it for the access flexibility.

## Why Databricks for validation instead of Glue?

Glue is optimized for ETL (one-way pipelines). Databricks is optimized for **iterative, interactive** work — which is exactly what reconciliation looks like:

1. Row count fails → pull up a sample → correlate with source
2. Find the pattern → tweak a check → re-run just that slice
3. Repeat until root cause is understood

Glue runs each Python script as a fresh JVM with no state. Databricks keeps the cluster warm across cells; a failed validation gives you a live notebook to drill into. For the migration use case — where 99% of runs PASS and 1% need hours of debugging — Databricks wins.

We keep Glue for the extract + load because those are linear pipelines that benefit from Glue's serverless cost model.

## Three-layer S3 organization

```
s3://sap-mdm-raw-<suffix>/
├── raw/sap/<TABLE>/<batch_id>/               ← Glue extract writes here
│   ├── part-*.parquet
│   ├── _schema.avsc                          ← source schema preserved
│   └── _manifest.json                        ← batch metadata
└── cdc/sap/<TABLE>/<yyyy>/<mm>/<dd>/<hh>/    ← DMS CDC writes here
    └── LOAD*.parquet

s3://sap-mdm-staging-<suffix>/
└── <TABLE>/<batch_id>/                       ← validated, ready-to-load
    └── part-*.parquet

s3://sap-mdm-audit-<suffix>/                  ← Object Lock, 7-year retention
└── validation-reports/<TABLE>/<batch_id>/
    └── report.json

s3://sap-mdm-glue-scripts-<suffix>/
├── jobs/                                     ← Python scripts
│   ├── glue_extract_job.py
│   ├── glue_load_job.py
│   └── glue_rollback_job.py
├── packages/migration.zip                    ← Python package
└── jars/                                     ← JDBC drivers
    ├── ngdbc-2.21.10.jar                     ← SAP HANA
    ├── postgresql-42.7.3.jar
    └── redshift-jdbc42-2.1.0.jar
```

Lifecycle rules:
- Raw: 90-day standard → 2555-day (7y) Glacier IR → delete
- CDC: 30 days standard → 180-day IA → delete
- Staging: 14-day delete
- Audit: 7-year Object Lock (COMPLIANCE mode — even root account can't delete)

## Migration phases — why 5 phases instead of 1

A big-bang cutover is tempting. It's also how you get paged on a Saturday at 3am with an irrecoverable data loss incident. The 5-phase approach looks slower but gives you 4 rollback windows where you can detect issues before they're catastrophic.

See [MIGRATION_LIFECYCLE.md](MIGRATION_LIFECYCLE.md) for the full playbook.

## Network topology

Three-tier VPC with strict ingress rules:

```
Public subnets (10.20.1-3.0/24)
├── Internet Gateway
├── NAT Gateway (AZ1 only — cost saver)
└── Bastion (optional, for emergency break-glass)

Private App subnets (10.20.11-13.0/24)
├── AWS Glue (via Glue Connection)
├── Lambda functions
├── DMS replication instances
└── → egress via NAT to SAP + internet APIs

Private Data subnets (10.20.21-23.0/24)
├── RDS Postgres (Multi-AZ in prod)
└── Redshift cluster
```

Security group rules:
- **App SG**: allows all egress, ingress from itself
- **Data SG**: ingress ONLY from App SG on 5432 (Postgres) and 5439 (Redshift)
- **No public IPs** on any data-plane resource

VPC endpoints (Gateway):
- **S3** — skips NAT for all S3 traffic (80%+ of our traffic), saves ~$200/month

## Data flow — batch path

```
SAP HANA
  │ JDBC (via Direct Connect / VPN, encrypted)
  ▼
AWS Glue (extract job)
  │ Parallel reads across partition_column
  ▼
S3 raw zone (Parquet + schema + manifest)
  │
  ▼
Databricks (reconciliation notebook — 5-layer validation)
  │ (validated? yes → continue; no → fail fast, alert)
  ▼
AWS Glue (load job)
  ├─► psycopg2 execute_values upsert → RDS Postgres
  └─► COPY from S3 → Redshift
  │
  ▼
Databricks (post-load reconciliation)
  │ (reads from both targets, re-validates parity)
  ▼
DynamoDB audit record + SNS success notification
```

## Data flow — CDC path

```
SAP HANA transaction log
  │ (log reader)
  ▼
AWS DMS (Replication instance)
  │ Parquet files, bucketed by hour
  ▼
S3 cdc/sap/<table>/<yyyy>/<mm>/<dd>/<hh>/
  │ S3 PUT event
  ▼
Lambda (s3_trigger.py)
  │ debounce within 5-min window (DynamoDB conditional write)
  │ invoke Step Function
  ▼
Databricks (Auto Loader + Delta MERGE)
  │ stream → bronze Delta
  │ silver Delta with tombstoning
  ▼
Lambda (per-table delta writer)
  ├─► Postgres (small upsert batches every 5 min)
  └─► Redshift (COPY from S3 every 15 min — optimize for batching)
```

## Idempotency & replay

Every batch is identified by a `batch_id` (typically a UUID prefix + timestamp). Before any expensive work, the pipeline:

1. Reads DynamoDB with PK `batch#migrate-table#<batch_id>`
2. If status = `SUCCEEDED` → returns immediately (no-op)
3. If status = `STARTED` or absent → atomically writes status = `STARTED` via conditional expression `attribute_not_exists(pk) OR #s = :failed`
4. Proceeds with extract/validate/load
5. On success: status = `SUCCEEDED` + `completed_at`
6. On failure: status = `FAILED` + `error_message`

This means:
- Re-triggering a failed batch is safe (marks as STARTED, retries from scratch)
- Re-triggering a succeeded batch is a no-op (instant success)
- Concurrent triggers for same batch_id race, only one wins, others skip

The audit table has TTL set to 30 days for batch records and no TTL for watermarks (which must persist indefinitely).

## What breaks, and how we handle it

| Failure mode | Detection | Response |
|---|---|---|
| SAP HANA unreachable | Glue job JDBC timeout | Auto-retry 2x, then alert + fail batch |
| S3 put failure | Glue job error | Auto-retry (boto3 built-in), then alert |
| Row-count drift > tolerance | Reconciliation layer 1 | Fail batch, DO NOT load, alert |
| Target DB full (RDS) | CloudWatch alarm (disk > 85%) | Auto-extend storage (RDS storage autoscaling) + alert |
| DMS replication lag > 60s | CloudWatch alarm | Alert; investigate (usually source-side) |
| Duplicate PKs in source | Reconciliation layer 3 | Fail batch, dump sample dups to audit bucket |
| Schema evolution in source | DMS task fails | Alert; operator updates contract + re-triggers |

## Why these specific choices over common alternatives

| Choice | Alternative we considered | Why we picked this |
|---|---|---|
| JDBC partitioned reads | SAP HANA CDS views via Open SQL | CDS requires SAP team cooperation; JDBC works anywhere |
| DMS + S3 CDC target | Fivetran / Stitch | Cost (~$3K/mo for equivalent) + data stays in our account |
| Databricks reconciliation | Glue notebook | Glue notebooks are stateless; debugging requires cluster re-start |
| Step Functions | Airflow | SFN is serverless, tightly integrated with Glue sync |
| DynamoDB audit | RDS audit table | DynamoDB conditional writes = cheap atomicity |
| Per-layer KMS keys | One shared CMK | Blast-radius containment on key compromise |
| Parquet everywhere | ORC / Avro / JSON | Parquet is the AWS + Databricks native format |

## References

- [AWS DMS best practices for SAP](https://docs.aws.amazon.com/dms/latest/userguide/CHAP_BestPractices.html)
- [Databricks MERGE INTO for CDC](https://docs.databricks.com/en/delta/merge.html)
- [Redshift COPY performance](https://docs.aws.amazon.com/redshift/latest/dg/c_loading-data-best-practices.html)
- [Glue JDBC connection tuning](https://docs.aws.amazon.com/glue/latest/dg/connection-JDBC-VPC.html)
