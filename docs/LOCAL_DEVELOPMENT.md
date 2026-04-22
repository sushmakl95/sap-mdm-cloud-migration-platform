# Local Development

## Goal

Run the full extract → validate → load pipeline against a **local Docker Compose stack** that simulates:

- A "SAP HANA" source (Postgres pretending to be HANA, for type-compatibility)
- A target Postgres
- A local-file-based staging zone
- A local Spark runtime

Zero AWS, zero Databricks, zero dollars. Useful for:

- Iterating on contracts, transforms, validators without deploy cycles
- Running pre-commit hooks
- CI
- Onboarding new engineers (they can run the thing)

## Prerequisites

| Tool | Version | Purpose |
|---|---|---|
| Python | 3.11 | Runtime |
| Java | 17 | Required by Spark 3.5 |
| Docker | 24+ | Source + target DBs |
| Make | any | Convenience |

## First-time setup

```bash
git clone https://github.com/sushmakl95/sap-mdm-cloud-migration-platform.git
cd sap-mdm-cloud-migration-platform
make install-dev      # creates .venv, installs all deps
make compose-up       # starts local Postgres (source + target)
make seed-sample-data # populates the source with MARA/KNA1/LFA1 samples
```

This takes ~3 minutes. After it completes, you can:

```bash
make demo-batch       # extract MARA → validate → load to target
make demo-validate    # run the 5-layer reconciliation manually
make demo-cdc         # simulate a CDC event through the pipeline
make demo-clean       # wipe target DB, re-run
```

## Running individual pipeline steps

### Extract

```bash
# Extract MARA from local Postgres (simulated HANA) to local Parquet files
sap-migrate extract \
  --table MARA \
  --output file:///tmp/sap-demo/raw/ \
  --mode full
```

The local version still uses the same `SapHanaExtractor` class — we just point it at a Postgres instance with tables named like SAP's. This verifies the partitioned JDBC + schema preservation logic end-to-end.

### Validate

```bash
# Run 5-layer reconciliation on an extracted batch
sap-migrate validate \
  --table MARA \
  --source-path file:///tmp/sap-demo/raw/MARA/<batch_id>/ \
  --target-url postgresql://local/sap_mdm_target \
  --output-report /tmp/validation-report.json
```

### Load

```bash
sap-migrate load \
  --table MARA \
  --extract-path file:///tmp/sap-demo/raw/MARA/<batch_id>/ \
  --redshift-staging file:///tmp/sap-demo/staging/ \
  --redshift-iam-role arn:aws:iam::000000000000:role/local  # unused locally
```

## Generating DDL

Contracts → SQL:

```bash
sap-migrate generate-ddl --output-dir ./sql/target
```

This emits one file per contract for each of Postgres + Redshift.

## Running the CI locally

Simulate what GitHub Actions does:

```bash
make ci
# = make lint + make typecheck + make security + make test-unit
```

## Emitting Step Functions definitions

For local testing + for Terraform:

```bash
sap-migrate emit-sfn --output-dir src/migration/orchestration/
# generates migrate_table.asl.json, batch_migrate.asl.json, rollback.asl.json
```

These JSON files have Terraform variable placeholders (`${foo}`) that get substituted during `terraform apply`.

## Generating DMS task configuration

```bash
sap-migrate dms-config --output ./config/dms_task_config.json
```

Inspect the output to understand the table-mappings + transformation rules generated from the contract registry.

## Testing

### Unit tests (fast, no DB required)

```bash
make test-unit
```

Covers:
- Contract parsing + registry
- Type coercer logic (pure transformations)
- DDL generator output
- Reconciliation layer logic (with mocked DataFrames)

### Integration tests (requires Docker Compose stack)

```bash
make compose-up
make test-integration
```

Covers:
- End-to-end extract → validate → load against local Postgres
- CDC event processing
- Idempotency check (re-running same batch is no-op)

## Troubleshooting

**`py4j.protocol.Py4JJavaError: An error occurred while calling ... delta`**

Delta Lake jars haven't downloaded yet. Run `make spark-deps-warm` once to pre-fetch them.

**`psycopg2.OperationalError: could not connect to server`**

Docker Compose stack isn't running. `make compose-up`.

**`RuntimeError: Secret not found: sap-mdm/prod/hana-jdbc` (local)**

You're invoking something that expects Secrets Manager. In local mode, set env vars instead:
```bash
export LOCAL_SAP_HOST=localhost
export LOCAL_SAP_PORT=5433
export LOCAL_SAP_USER=sap_reader
export LOCAL_SAP_PASSWORD=localpassword
```

Our `get_secret` function checks for a `LOCAL_SECRETS_OVERRIDE` env var that reads from env vars instead of Secrets Manager.

**Spark takes forever to start**

First-time runs download JDBC drivers. Subsequent runs use the cached jars in `~/.ivy2/cache/`. Warm the cache with `make spark-deps-warm`.

## IDE setup

**VS Code**: install the Python + Ruff extensions. Project is configured via `pyproject.toml` — no extra config needed. Mark `src/` as "sources" root.

**PyCharm**: mark `src/` as "Sources Root". Use the `.venv` interpreter.

## Contributing workflow

```bash
git checkout -b feat/add-contract-BKPF
# Add contract to src/migration/schemas/mdm_contracts.py
# Add test in tests/unit/test_contracts.py
make ci                  # full local CI
git commit -m "feat: add BKPF contract for accounting document header"
git push origin feat/add-contract-BKPF
```

Open a PR; GitHub Actions runs the same `make ci` in CI. Merges require green CI.

## Not (yet) supported locally

- Real AWS Glue runtime (we simulate with local Spark)
- Real DMS CDC (we have a file-based simulator)
- Databricks notebooks (can be run in a Databricks workspace only — we don't have a local Databricks simulator)
- IAM / networking (we just skip these in local mode; real prod uses VPC + IAM roles)

If you need Databricks notebook dev, use a Databricks workspace directly (free 14-day trial works).
