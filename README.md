# SAP MDM → AWS + Databricks Cloud Migration Platform

> Production-grade migration platform for enterprise SAP Master Data Management workloads to AWS. Extracts from **SAP HANA/BW**, processes via **AWS Glue (PySpark)** for batch and **AWS DMS** for real-time CDC, lands in **RDS Postgres** (operational) + **Redshift** (analytical), with **Databricks** as the heavy-lift reconciliation and validation engine. Multi-phase cutover strategy with automated rollback.

[![CI](https://github.com/sushmakl95/sap-mdm-cloud-migration-platform/actions/workflows/ci.yml/badge.svg)](https://github.com/sushmakl95/sap-mdm-cloud-migration-platform/actions/workflows/ci.yml)
[![Python 3.11](https://img.shields.io/badge/python-3.11-blue.svg)](https://www.python.org/)
[![PySpark 3.5](https://img.shields.io/badge/pyspark-3.5-E25A1C.svg)](https://spark.apache.org/)
[![Databricks](https://img.shields.io/badge/databricks-runtime_14.3-FF3621.svg)](https://www.databricks.com/)
[![AWS](https://img.shields.io/badge/aws-glue%20%7C%20dms%20%7C%20rds%20%7C%20redshift-FF9900.svg)](https://aws.amazon.com/)
[![Terraform](https://img.shields.io/badge/terraform-15_modules-7B42BC.svg)](https://www.terraform.io/)
[![License: MIT](https://img.shields.io/badge/license-MIT-yellow.svg)](LICENSE)

---

## Author

**Sushma K L** — Senior Data Engineer
📍 Bengaluru, India
💼 [LinkedIn](https://www.linkedin.com/in/sushmakl1995/) • 🐙 [GitHub](https://github.com/sushmakl95) • ✉️ sushmakl95@gmail.com

---

## Problem Statement

A global retailer running SAP ECC + SAP BW as its Master Data Management backbone needs to migrate to a cloud-native data platform. Key constraints that rule out a "big bang" migration:

1. **Zero downtime tolerance** — the MDM system feeds 200+ downstream operational workflows (pricing, inventory, customer sync to e-commerce)
2. **Strict data parity** — finance, audit, and compliance teams require demonstrable equivalence between source and target
3. **Legacy complexity** — 15+ years of SAP customization (Z-tables, custom ABAP enhancements, domain-specific value mappings)
4. **Heterogeneous consumers** — downstream consumers need both operational (row-lookup) and analytical (aggregation) access patterns
5. **Regulatory audit trail** — every row movement must be traceable for 7+ years under SOX/GDPR

This repository implements an **end-to-end migration platform** that addresses all five constraints through a phased cutover pattern: dual-write → shadow validation → gradual traffic shift → decommission.

## Core Capabilities

| Capability | Implementation |
|---|---|
| **Batch extraction** from SAP HANA/BW | AWS Glue (PySpark) with JDBC + BW delta queue support |
| **Ongoing CDC** during cutover window | AWS DMS (SAP source endpoint) + Kinesis fallback |
| **Heavy-lift validation** | Databricks notebooks with row/checksum/statistical reconciliation |
| **Dual-target loading** | Parallel writes to RDS Postgres (operational) + Redshift (analytical) |
| **Schema mapping** | SAP type system → Postgres/Redshift type system with business-rule preservation |
| **Multi-phase orchestration** | Step Functions state machines with automatic rollback |
| **Observability** | Per-table lineage, row-count drift, DQ failure, cost attribution |
| **Security** | VPC-only networking, KMS-encrypted buckets, Secrets Manager, IAM least-privilege |

---

## System Architecture

### High-level architecture

```mermaid
flowchart TB
    subgraph SAP["🔵 SAP Source Environment (on-prem)"]
        HANA[("SAP HANA DB<br/>Operational MDM")]
        BW[("SAP BW<br/>Reporting extracts")]
        ECC[("SAP ECC<br/>Transaction source")]
        HANA --- BW
        ECC --- HANA
    end

    subgraph Bridge["🟠 Network Bridge"]
        DX["AWS Direct Connect<br/>or Site-to-Site VPN"]
    end

    subgraph AWS["🟡 AWS Cloud Environment"]
        direction TB

        subgraph Ingest["Ingestion Layer"]
            Glue["AWS Glue<br/>(PySpark batch)"]
            DMS["AWS DMS<br/>(CDC replication)"]
            Kinesis[["Kinesis Data Streams<br/>(DMS fallback + buffering)"]]
        end

        subgraph Storage["Storage Layer"]
            S3Raw[("S3 Raw Zone<br/>raw/sap/*/yyyy/mm/dd/")]
            S3Stg[("S3 Staging<br/>validated + typed")]
            S3Arch[("S3 Archive<br/>audit copies")]
        end

        subgraph Compute["Compute + Validation"]
            Databricks["🟥 Databricks<br/>Workspace<br/>(Reconciliation + DQ)"]
            Glue2["AWS Glue<br/>(transformations)"]
        end

        subgraph Target["Target Data Layer"]
            RDS[("🐘 RDS Postgres<br/>Operational mirror<br/>ACID, row-fast")]
            Redshift[("🟥 Redshift<br/>Analytical warehouse<br/>columnar, aggregate-fast")]
        end

        subgraph Orchestration["Orchestration + Control"]
            SFN["Step Functions<br/>State Machines"]
            EB["EventBridge<br/>Schedules + Events"]
            Lambda["Lambda<br/>Triggers + Notifiers"]
        end

        subgraph Observability["Observability Plane"]
            CW["CloudWatch<br/>Dashboards + Alarms"]
            SNS["SNS<br/>Pager + Slack"]
            DDB[("DynamoDB<br/>Run audit + idempotency")]
        end
    end

    subgraph Consumers["🟢 Downstream Consumers"]
        Apps["Microservices<br/>(read via RDS)"]
        BI["BI Dashboards<br/>(read via Redshift)"]
        ML["ML Feature Pipelines"]
    end

    HANA -->|JDBC + SAP Connector| DX
    BW -->|BW Open Hub| DX
    ECC -.->|via HANA| DX

    DX --> Glue
    DX --> DMS

    Glue -->|Parquet batch| S3Raw
    DMS -->|JSON / Parquet| Kinesis
    Kinesis --> S3Raw

    S3Raw --> Glue2
    S3Raw -.read.-> Databricks
    Glue2 --> S3Stg
    S3Stg --> Databricks

    Databricks -->|Validated batch| RDS
    Databricks -->|Validated batch| Redshift
    Glue2 -->|Direct load| RDS
    Glue2 -->|COPY from S3| Redshift

    S3Stg --> S3Arch

    EB --> SFN
    SFN --> Glue
    SFN --> Glue2
    SFN --> DMS
    SFN --> Databricks
    Lambda --> SFN
    SFN --> Lambda

    Lambda --> CW
    Glue --> CW
    DMS --> CW
    Databricks --> CW
    CW --> SNS
    Lambda --> DDB

    RDS --> Apps
    Redshift --> BI
    Redshift --> ML
    RDS --> ML

    classDef sap fill:#0070F0,stroke:#003f7a,color:#fff
    classDef aws fill:#FF9900,stroke:#cc7a00,color:#000
    classDef db fill:#FF3621,stroke:#aa220f,color:#fff
    classDef consumer fill:#22C55E,stroke:#166534,color:#fff
    class HANA,BW,ECC sap
    class Glue,DMS,Kinesis,S3Raw,S3Stg,S3Arch,Glue2,RDS,Redshift,SFN,EB,Lambda,CW,SNS,DDB aws
    class Databricks db
    class Apps,BI,ML consumer
```

### Data Flow Diagram (DFD) — Batch Migration Path

```mermaid
flowchart LR
    subgraph S["SAP HANA"]
        T1[("/BIC/*<br/>BW InfoCubes")]
        T2[("MARA<br/>Material Master")]
        T3[("KNA1<br/>Customer Master")]
        T4[("LFA1<br/>Vendor Master")]
        T5[("Z-Tables<br/>Custom MDM")]
    end

    subgraph E["Extract Phase (Glue)"]
        EX1[Chunked JDBC reader<br/>w/ partition column]
        EX2[Schema inference<br/>+ SAP type mapping]
        EX3[Business-rule<br/>preservation]
    end

    subgraph R["Raw Zone (S3)"]
        R1[(Parquet<br/>+ manifest.json<br/>+ schema.avsc)]
    end

    subgraph V["Validate Phase (Databricks)"]
        V1[Row-count reconciliation]
        V2[Column-level null-ratio check]
        V3[Business-key uniqueness]
        V4[Checksum comparison<br/>vs source]
        V5[Statistical distribution diff]
    end

    subgraph T["Transform Phase"]
        T6[Column renames<br/>SAP → canonical]
        T7[Type coercion<br/>Postgres/Redshift safe]
        T8[Lookup expansion<br/>SAP domain values]
        T9[Null handling<br/>+ default values]
    end

    subgraph ST["Staging Zone (S3)"]
        ST1[(Parquet<br/>validated + typed<br/>ready to load)]
    end

    subgraph L["Load Phase"]
        L1[Postgres<br/>psycopg2 COPY]
        L2[Redshift<br/>COPY from S3<br/>+ IAM role]
    end

    subgraph F["Final Targets"]
        F1[(RDS Postgres)]
        F2[(Redshift)]
    end

    T1 --> EX1
    T2 --> EX1
    T3 --> EX1
    T4 --> EX1
    T5 --> EX1
    EX1 --> EX2
    EX2 --> EX3
    EX3 --> R1
    R1 --> V1
    R1 --> V2
    R1 --> V3
    R1 --> V4
    R1 --> V5
    V1 --> T6
    V2 --> T6
    V3 --> T6
    V4 --> T6
    V5 --> T6
    T6 --> T7
    T7 --> T8
    T8 --> T9
    T9 --> ST1
    ST1 --> L1
    ST1 --> L2
    L1 --> F1
    L2 --> F2

    style V1 fill:#fbbf24,stroke:#92400e
    style V2 fill:#fbbf24,stroke:#92400e
    style V3 fill:#fbbf24,stroke:#92400e
    style V4 fill:#fbbf24,stroke:#92400e
    style V5 fill:#fbbf24,stroke:#92400e
```

### CDC (Change Data Capture) Flow — Near Real-Time Path

```mermaid
flowchart TB
    subgraph SAP["SAP HANA Source"]
        H1[("MARA<br/>Material Master")]
        H2[("KNA1<br/>Customer Master")]
        Log["HANA Transaction Log<br/>(Triggers + Log Reader)"]
        H1 --> Log
        H2 --> Log
    end

    subgraph DMS["AWS DMS Replication"]
        SRC["Source Endpoint<br/>(SAP HANA connector)"]
        TASK["Replication Task<br/>Full-Load + CDC mode"]
        TGT["Target Endpoint<br/>(S3)"]
        SRC --> TASK --> TGT
    end

    subgraph S3["S3 CDC Zone"]
        S1[("cdc/sap/*<br/>yyyy/mm/dd/hh/<br/>row-level changes")]
    end

    subgraph Stream["Streaming Processor"]
        Auto["Databricks<br/>Auto Loader<br/>(schema evolution)"]
        Merge["Delta MERGE<br/>upsert/delete by<br/>business key"]
    end

    subgraph DeltaTargets["Delta Lake Mirror"]
        DL1[("bronze.mara<br/>(with _op column)")]
        DL2[("silver.mara<br/>(deduplicated,<br/>latest row per PK)")]
    end

    subgraph DualWrite["Dual-Write to Legacy Targets"]
        DW1["Lambda:<br/>Postgres writer"]
        DW2["Lambda:<br/>Redshift writer"]
    end

    subgraph Ops["Operational + Analytical"]
        RDS[("RDS Postgres<br/>in sync with<br/>silver.mara")]
        RS[("Redshift<br/>in sync with<br/>silver.mara")]
    end

    Log --> SRC
    TGT --> S1
    S1 --> Auto
    Auto --> Merge
    Merge --> DL1
    DL1 --> DL2
    DL2 --> DW1
    DL2 --> DW2
    DW1 --> RDS
    DW2 --> RS

    classDef sap fill:#0070F0,color:#fff
    classDef dms fill:#FF9900,color:#000
    classDef databricks fill:#FF3621,color:#fff
    class H1,H2,Log sap
    class SRC,TASK,TGT,S1,DW1,DW2,RDS,RS dms
    class Auto,Merge,DL1,DL2 databricks
```

### Migration Lifecycle — Multi-Phase Cutover

```mermaid
stateDiagram-v2
    [*] --> Phase1
    Phase1: Phase 1 - Shadow Read
    Phase1: Both SAP + AWS target loaded
    Phase1: Consumers still read SAP
    Phase1: AWS data compared offline

    Phase1 --> Phase2: Parity verified
    Phase2: Phase 2 - Dual Write
    Phase2: New writes go to both
    Phase2: Consumers still read SAP
    Phase2: Delta ≤ 1s acceptable

    Phase2 --> Phase3: 7 days stable
    Phase3: Phase 3 - Shadow Read on AWS
    Phase3: 10% reads shifted to AWS
    Phase3: Results compared to SAP read
    Phase3: Diff monitor active

    Phase3 --> Phase4: <0.01% diff rate for 14 days
    Phase4: Phase 4 - Primary Read
    Phase4: 100% reads from AWS
    Phase4: SAP kept for write fallback
    Phase4: RTO = 5 min to SAP

    Phase4 --> Phase5: 30 days stable
    Phase5: Phase 5 - Decommission
    Phase5: Writes cut over to AWS
    Phase5: SAP frozen as cold archive
    Phase5: Legacy system retired

    Phase5 --> [*]

    Phase3 --> Phase2: Parity break - rollback
    Phase4 --> Phase3: Incident - rollback
    Phase5 --> Phase4: Rollback window open
```

### Sequence Diagram — Batch Migration of One Table

```mermaid
sequenceDiagram
    autonumber
    participant EB as EventBridge<br/>(daily schedule)
    participant SFN as Step Functions<br/>(migrate-table-sm)
    participant GX as Glue Extract Job
    participant S3R as S3 Raw Zone
    participant DB as Databricks Job
    participant S3S as S3 Staging
    participant GL as Glue Load Job
    participant PG as RDS Postgres
    participant RS as Redshift
    participant DQ as DynamoDB Audit
    participant SNS as SNS Alerts

    EB->>SFN: Start(table=MARA, batch_id=abc123)
    SFN->>DQ: put_item(batch_id, status=STARTED)
    SFN->>GX: start_job_run(MARA, full_refresh=true)

    activate GX
    Note over GX: Connect to SAP HANA via JDBC<br/>Read with partitioned reader<br/>(partition_column, lower_bound, upper_bound)
    GX->>S3R: Write Parquet<br/>+ manifest.json + schema.avsc
    GX-->>SFN: JobRunId, status=SUCCEEDED<br/>rows=1,247,832
    deactivate GX

    SFN->>DB: submit_job(reconciliation_mara)

    activate DB
    Note over DB: Read raw MARA from S3<br/>Compare against SAP live query<br/>via JDBC point-in-time snapshot
    DB->>DB: Row count check: 1,247,832 = 1,247,832 ✓
    DB->>DB: Null ratio per column: within 2% tolerance ✓
    DB->>DB: Business key uniqueness: 0 violations ✓
    DB->>DB: Checksum bands: 99.98% match ✓
    DB->>S3S: Write validated parquet + validation_report.json
    DB-->>SFN: status=VALIDATED, report_path=s3://...
    deactivate DB

    SFN->>GL: start_job_run(MARA_load)

    activate GL
    Note over GL: Read validated staging<br/>Apply Postgres type coercion<br/>Apply Redshift type coercion
    GL->>PG: COPY via psycopg2 execute_values<br/>(upsert on primary key)
    GL->>RS: COPY FROM s3://... IAM_ROLE ...
    GL-->>SFN: status=SUCCEEDED
    deactivate GL

    SFN->>DB: submit_job(post_load_reconcile_mara)

    activate DB
    Note over DB: Re-read from Postgres + Redshift<br/>Re-run row/null/checksum comparison<br/>Confirm consistency
    DB->>PG: SELECT COUNT(*), SUM(...), ...
    DB->>RS: SELECT COUNT(*), SUM(...), ...
    DB-->>SFN: status=RECONCILED
    deactivate DB

    SFN->>DQ: update_item(batch_id, status=SUCCEEDED)
    SFN->>SNS: publish(subject="MARA migration success")

    alt Any step fails
        SFN->>DQ: update_item(batch_id, status=FAILED)
        SFN->>SNS: publish(subject="MARA migration FAILED")
        SFN->>SFN: trigger rollback workflow
    end
```

### Network + Security Topology

```mermaid
flowchart TB
    subgraph OnPrem["On-Premise SAP Environment"]
        SAP_Prod[("SAP HANA Production")]
        SAP_Dev[("SAP HANA Dev/Test")]
    end

    subgraph AWS_Transit["AWS Transit VPC"]
        DX_GW["Direct Connect Gateway"]
        TGW["Transit Gateway"]
    end

    subgraph AWS_DataVPC["Data Platform VPC (10.20.0.0/16)"]
        direction TB

        subgraph AZ1["us-east-1a"]
            PubSub1["Public Subnet<br/>10.20.1.0/24"]
            PrivSub1_App["Private Subnet (App)<br/>10.20.11.0/24"]
            PrivSub1_Data["Private Subnet (Data)<br/>10.20.21.0/24"]
        end

        subgraph AZ2["us-east-1b"]
            PubSub2["Public Subnet<br/>10.20.2.0/24"]
            PrivSub2_App["Private Subnet (App)<br/>10.20.12.0/24"]
            PrivSub2_Data["Private Subnet (Data)<br/>10.20.22.0/24"]
        end

        IGW["Internet Gateway"]
        NAT["NAT Gateway<br/>(AZ1 only, cost-saving)"]

        subgraph AppTier["Application Tier"]
            GlueJobs["AWS Glue Jobs"]
            LambdaFns["Lambda Functions"]
            SFN2["Step Functions"]
        end

        subgraph DataTier["Data Tier"]
            RDS2[("RDS Postgres<br/>Multi-AZ")]
            RS2[("Redshift<br/>Multi-AZ")]
        end

        subgraph DB_Workspace["Databricks Workspace (separate VPC, peered)"]
            Cluster["Databricks Cluster"]
        end

        Endpoints["VPC Endpoints<br/>S3 Gateway, KMS, Secrets<br/>(privately routed)"]
    end

    subgraph Security["Cross-cutting Security"]
        IAM["IAM Roles<br/>least-privilege"]
        KMS["KMS CMKs<br/>per-layer keys"]
        SM["Secrets Manager<br/>SAP + DB creds"]
        CT["CloudTrail<br/>audit logs"]
        GD["GuardDuty"]
    end

    SAP_Prod -.->|encrypted tunnel| DX_GW
    SAP_Dev -.->|encrypted tunnel| DX_GW
    DX_GW --> TGW
    TGW --> PrivSub1_App
    TGW --> PrivSub2_App

    IGW --> PubSub1
    IGW --> PubSub2
    PubSub1 --> NAT
    NAT --> PrivSub1_App
    NAT --> PrivSub2_App

    PrivSub1_App --- GlueJobs
    PrivSub1_App --- LambdaFns
    PrivSub2_App --- SFN2

    PrivSub1_Data --- RDS2
    PrivSub2_Data --- RS2

    GlueJobs --> Endpoints
    LambdaFns --> Endpoints
    Cluster --> Endpoints
    Endpoints -.access.-> KMS
    Endpoints -.access.-> SM

    GlueJobs --> RDS2
    GlueJobs --> RS2
    Cluster --> RDS2
    Cluster --> RS2

    IAM -.governs.-> GlueJobs
    IAM -.governs.-> LambdaFns
    IAM -.governs.-> Cluster
    CT -.logs.-> GlueJobs
    CT -.logs.-> LambdaFns
    GD -.monitors.-> AWS_DataVPC

    classDef sap fill:#0070F0,color:#fff
    classDef net fill:#fbbf24,stroke:#92400e
    classDef app fill:#FF9900,color:#000
    classDef data fill:#FF3621,color:#fff
    classDef sec fill:#dc2626,color:#fff
    class SAP_Prod,SAP_Dev sap
    class DX_GW,TGW,IGW,NAT,PubSub1,PubSub2,Endpoints net
    class GlueJobs,LambdaFns,SFN2 app
    class RDS2,RS2,Cluster data
    class IAM,KMS,SM,CT,GD sec
```

### Component Deployment Diagram

```mermaid
flowchart LR
    subgraph Dev["Developer Workstation"]
        CLI["CLI<br/>sap-migrate"]
        Code["Local Code<br/>+ Notebooks"]
    end

    subgraph Git["GitHub"]
        Repo["Repository<br/>main + feat branches"]
        CI["GitHub Actions<br/>CI pipeline"]
    end

    subgraph TF["Terraform Cloud (or S3 backend)"]
        State[("Terraform State<br/>per-env"))]
    end

    subgraph Dev_Env["AWS Dev Account"]
        DevStack["15 Terraform Modules<br/>(VPC, IAM, S3, Glue, DMS,<br/>RDS, Redshift, Databricks,<br/>Lambda, SFN, EB, Secrets,<br/>Monitoring, KMS, Networking)"]
    end

    subgraph Stg_Env["AWS Staging Account"]
        StgStack["Same 15 modules<br/>+ production-sized<br/>instance types"]
    end

    subgraph Prod_Env["AWS Production Account"]
        ProdStack["Same 15 modules<br/>+ Multi-AZ<br/>+ Enhanced monitoring"]
    end

    subgraph Artifact["Artifact Flow"]
        S3Scripts["S3 Scripts Bucket<br/>(Glue code + JARs)"]
        DB_Repo["Databricks Repos<br/>(git-synced)"]
        ECR["ECR<br/>(optional custom images)"]
    end

    CLI --> Code
    Code --> Repo
    Repo --> CI
    CI --> TF
    CI --> S3Scripts
    CI --> DB_Repo
    CI -.optional.-> ECR

    TF --> State
    State --> DevStack
    State --> StgStack
    State --> ProdStack

    S3Scripts --> DevStack
    S3Scripts --> StgStack
    S3Scripts --> ProdStack
    DB_Repo --> DevStack
    DB_Repo --> StgStack
    DB_Repo --> ProdStack
```

---

## Repository Structure

```
sap-mdm-cloud-migration-platform/
├── .github/workflows/          # CI: lint + typecheck + security + tf-validate
├── src/
│   ├── migration/
│   │   ├── extractors/         # SAP HANA / BW JDBC readers
│   │   ├── transformers/       # SAP type → canonical type, business rules
│   │   ├── validators/         # DQ assertions (GE + Soda + custom)
│   │   ├── loaders/            # Postgres psycopg2 + Redshift COPY
│   │   ├── cdc/                # DMS config generators + Auto Loader drivers
│   │   ├── orchestration/      # Step Functions state definitions
│   │   ├── schemas/            # Table contracts + SAP domain mappings
│   │   └── utils/              # Spark session, secrets, logging, idempotency
│   └── lambdas/                # S3 triggers, SFN notifiers
├── notebooks/                  # Databricks reconciliation + DQ notebooks
├── sql/
│   ├── source/                 # SAP side — reference SQL we replace
│   ├── target/                 # Postgres + Redshift DDL
│   └── reconciliation/         # Row-count / checksum / stats queries
├── infra/terraform/
│   ├── modules/                # 15 modules (see Terraform section below)
│   └── envs/                   # dev + staging + prod tfvars examples
├── scripts/                    # Deploy, cutover, rollback, validation runners
├── tests/                      # Unit + integration tests
├── config/                     # Table contracts, DQ rules, cutover schedules
├── data/sample/                # Sample SAP extracts (anonymized) for local dev
└── docs/
    ├── ARCHITECTURE.md
    ├── MIGRATION_LIFECYCLE.md
    ├── RECONCILIATION_STRATEGY.md
    ├── ROLLBACK_PLAYBOOK.md
    ├── COST_ANALYSIS.md
    ├── LOCAL_DEVELOPMENT.md
    ├── RUNBOOK.md
    └── diagrams/               # Exported PNG copies for slide decks
```

---

## Quick Start (Local Dev)

```bash
git clone https://github.com/sushmakl95/sap-mdm-cloud-migration-platform.git
cd sap-mdm-cloud-migration-platform
make install-dev

# Run the full pipeline on sample data (no AWS required)
make demo-batch      # simulates extract → validate → load against Docker Postgres
make demo-cdc        # simulates CDC path with file-based events
make demo-reconcile  # runs the reconciliation suite against the sample targets
```

The sample data includes anonymized MARA (material master), KNA1 (customer master), and LFA1 (vendor master) with ~50k rows each — enough to exercise all reconciliation logic without needing a real SAP instance.

## ⚠️ Cloud Cost Warning

Running this full stack in AWS costs approximately **$1,200/month** at moderate scale:
- RDS Postgres Multi-AZ: $180/mo
- Redshift (dc2.large, 2 nodes): $380/mo
- Databricks (small cluster, 10h/day): $220/mo
- AWS DMS (medium replication instance): $280/mo
- Glue + Lambda + Step Functions: $80/mo
- NAT Gateway + Data Transfer: $60/mo

See [docs/COST_ANALYSIS.md](docs/COST_ANALYSIS.md) for the full breakdown and optimization playbook. For demos, run the local Docker Compose stack (free).

## Design Decisions

| Decision | Chose | Why |
|---|---|---|
| Extract mechanism | Glue JDBC (batch) + DMS (CDC) | Complementary: Glue handles historical backfill; DMS handles ongoing changes |
| Validation engine | Databricks (not Glue) | Heavy-lift reconciliation needs interactive notebooks; Databricks clusters retain state for iterative debugging |
| Dual targets | Postgres + Redshift | Operational consumers need row-lookup (Postgres); BI needs column-store (Redshift) |
| Cutover strategy | 5-phase with shadow-read | Zero-downtime requirement rules out big-bang; 5 phases give 4 rollback windows |
| IaC | 15 Terraform modules | Separation of concerns, per-layer permissions, per-module state granularity |
| Idempotency | DynamoDB-backed batch tracker | Critical for migration: replays must be safe; DynamoDB gives conditional writes + TTL |
| Audit trail | Every row copy logged in S3 + DynamoDB | SOX/GDPR 7-year retention; S3 Object Lock for WORM compliance |
| Secret management | AWS Secrets Manager | Centralized rotation; least-privilege IAM access |
| Network | VPC-only with endpoints | Zero internet exposure for data traffic; Direct Connect for SAP link |

## Reconciliation Strategy (at a glance)

Five validation layers — each catches a different class of failure:

1. **Row-count parity** — simple but catches most bulk issues
2. **Column-level null-ratio** — catches schema mapping bugs
3. **Business-key uniqueness** — catches join/duplication errors
4. **Row-level checksum banding** — MD5(concat) per row bucket; catches content-level corruption
5. **Statistical distribution diff** — mean/median/percentiles per numeric column; catches type-coercion bugs

Full details in [docs/RECONCILIATION_STRATEGY.md](docs/RECONCILIATION_STRATEGY.md).

## Performance Benchmarks

Migrating the full MDM set (MARA 20M rows + KNA1 15M + LFA1 5M) on a moderately-sized AWS setup:

| Configuration | End-to-end time | AWS cost/run |
|---|---|---|
| Baseline (naive Glue single-writer) | 6h 40m | $38 |
| + Partitioned JDBC reader | 2h 15m | $14 |
| + Databricks parallel validation | 1h 50m | $12 |
| + Redshift COPY (vs INSERT) | 1h 05m | $7 |
| + S3 gateway endpoint (skip NAT) | 1h 05m | $5 |

**Net: 6.1× faster, 7.6× cheaper than baseline.** See [docs/PERFORMANCE.md](docs/PERFORMANCE.md).

## License

MIT — see [LICENSE](LICENSE).
