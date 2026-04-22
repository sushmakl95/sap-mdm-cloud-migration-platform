# Databricks notebook source
# MAGIC %md
# MAGIC # SAP Migration — 5-Layer Reconciliation Notebook
# MAGIC
# MAGIC This notebook is the "heavy-lift" reconciliation engine invoked by Step Functions
# MAGIC after the Glue extract or load phases. It:
# MAGIC
# MAGIC 1. Reads the extracted Parquet from S3 raw zone
# MAGIC 2. Runs a live point-in-time query against SAP HANA for comparison
# MAGIC 3. Executes the 5-layer validation
# MAGIC 4. Writes the validation report to S3 audit bucket
# MAGIC 5. Returns overall_status to Step Functions
# MAGIC
# MAGIC Parameters (passed by SFN via `NotebookParams`):
# MAGIC   - `contract_fqn`    e.g. `HANA:SAPSR3.MARA`
# MAGIC   - `batch_id`        unique batch identifier
# MAGIC   - `phase`           `post-extract` | `post-load`

# COMMAND ----------

# Parameters widget
dbutils.widgets.text("contract_fqn", "", "Contract FQN")
dbutils.widgets.text("batch_id", "", "Batch ID")
dbutils.widgets.text("phase", "post-extract", "Phase")
dbutils.widgets.text("raw_bucket", "", "Raw bucket name")
dbutils.widgets.text("audit_bucket", "", "Audit bucket name")

contract_fqn = dbutils.widgets.get("contract_fqn")
batch_id = dbutils.widgets.get("batch_id")
phase = dbutils.widgets.get("phase")
raw_bucket = dbutils.widgets.get("raw_bucket")
audit_bucket = dbutils.widgets.get("audit_bucket")

print(f"contract_fqn={contract_fqn}")
print(f"batch_id={batch_id}")
print(f"phase={phase}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load contract + extracted data

# COMMAND ----------

from migration.schemas import TABLE_CONTRACTS
from migration.validators import ReconciliationValidator

sap_table = contract_fqn.split(".")[-1]
contract = TABLE_CONTRACTS.get(sap_table)

if contract is None:
    raise ValueError(f"Unknown contract: {contract_fqn}")

# Path to the extracted Parquet
extract_path = f"s3://{raw_bucket}/raw/sap/{sap_table}/{batch_id}/"
extracted = spark.read.parquet(extract_path)
print(f"Extracted rows: {extracted.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load source-of-truth snapshot
# MAGIC
# MAGIC For post-extract validation, we query SAP HANA live (point-in-time snapshot).
# MAGIC For post-load validation, we query the target (Postgres + Redshift).

# COMMAND ----------

from migration.utils.secrets import get_secret

def load_sap_snapshot():
    """Read SAP table via JDBC for live comparison."""
    creds = get_secret("sap-mdm/prod/hana-jdbc")
    return (
        spark.read.format("jdbc")
        .option("url", f"jdbc:sap://{creds['host']}:{creds['port']}/?databaseName={creds['database']}")
        .option("dbtable", f"(SELECT * FROM {contract.sap_schema}.{contract.sap_table}) src")
        .option("user", creds["username"])
        .option("password", creds["password"])
        .option("driver", "com.sap.db.jdbc.Driver")
        .option("fetchsize", "50000")
        .load()
    )

def load_postgres_snapshot():
    """Read from target Postgres for post-load comparison."""
    creds = get_secret("sap-mdm/prod/postgres")
    return (
        spark.read.format("jdbc")
        .option("url", f"jdbc:postgresql://{creds['host']}:{creds['port']}/{creds['database']}")
        .option("dbtable", contract.target_fqn_postgres)
        .option("user", creds["username"])
        .option("password", creds["password"])
        .option("driver", "org.postgresql.Driver")
        .load()
    )

if phase == "post-extract":
    source = load_sap_snapshot()
    target = extracted
    print("Comparing SAP live ↔ extracted Parquet")
elif phase == "post-load":
    source = extracted
    target = load_postgres_snapshot()
    print("Comparing extracted Parquet ↔ Postgres target")
else:
    raise ValueError(f"Unknown phase: {phase}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run 5-layer reconciliation

# COMMAND ----------

validator = ReconciliationValidator()
report = validator.validate(
    source_df=source,
    target_df=target,
    contract=contract,
    batch_id=batch_id,
)

print(f"Overall status: {report.overall_status.value}")
for r in report.results:
    print(f"  [{r.status.value:8s}] {r.layer:28s}  {r.message}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write validation report to S3 audit

# COMMAND ----------

import json

audit_path = f"s3://{audit_bucket}/validation-reports/{sap_table}/{batch_id}/report.json"

report_json = report.to_json()
dbutils.fs.put(audit_path, report_json, overwrite=True)

print(f"Report written to {audit_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Return status to Step Functions

# COMMAND ----------

# Databricks Job API uses dbutils.notebook.exit() as the return value
dbutils.notebook.exit(json.dumps({
    "overall_status": report.overall_status.value,
    "source_row_count": report.source_row_count,
    "target_row_count": report.target_row_count,
    "report_path": audit_path,
    "failed_layers": [r.layer for r in report.results if r.status.value == "FAILED"],
}))
