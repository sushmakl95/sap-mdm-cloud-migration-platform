# Databricks notebook source
# MAGIC %md
# MAGIC # Post-Load Validation Notebook
# MAGIC
# MAGIC Runs AFTER the Glue load job has written to Postgres + Redshift. Re-reads
# MAGIC BOTH targets and verifies they match the raw extract AND each other.
# MAGIC
# MAGIC This catches bugs where the load phase itself introduced drift (e.g., bad
# MAGIC type casting between Postgres and Redshift, currency columns stored in
# MAGIC cents vs dollars between the two).

# COMMAND ----------

dbutils.widgets.text("contract_fqn", "")
dbutils.widgets.text("batch_id", "")
dbutils.widgets.text("raw_bucket", "")
dbutils.widgets.text("audit_bucket", "")

# COMMAND ----------

from migration.schemas import TABLE_CONTRACTS
from migration.utils.secrets import get_secret
from migration.validators import ReconciliationValidator

sap_table = dbutils.widgets.get("contract_fqn").split(".")[-1]
batch_id = dbutils.widgets.get("batch_id")
contract = TABLE_CONTRACTS[sap_table]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read all three datasets

# COMMAND ----------

# Raw extract (from S3)
raw = spark.read.parquet(
    f"s3://{dbutils.widgets.get('raw_bucket')}/raw/sap/{sap_table}/{batch_id}/"
)

# Postgres target
pg_creds = get_secret("sap-mdm/prod/postgres")
postgres = (
    spark.read.format("jdbc")
    .option("url", f"jdbc:postgresql://{pg_creds['host']}:{pg_creds['port']}/{pg_creds['database']}")
    .option("dbtable", contract.target_fqn_postgres)
    .option("user", pg_creds["username"])
    .option("password", pg_creds["password"])
    .option("driver", "org.postgresql.Driver")
    .load()
)

# Redshift target
rs_creds = get_secret("sap-mdm/prod/redshift")
redshift = (
    spark.read.format("jdbc")
    .option("url", f"jdbc:redshift://{rs_creds['host']}:{rs_creds['port']}/{rs_creds['database']}")
    .option("dbtable", contract.target_fqn_redshift)
    .option("user", rs_creds["username"])
    .option("password", rs_creds["password"])
    .option("driver", "com.amazon.redshift.jdbc42.Driver")
    .load()
)

print(f"Raw:      {raw.count():,}")
print(f"Postgres: {postgres.count():,}")
print(f"Redshift: {redshift.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validate raw ↔ Postgres

# COMMAND ----------

validator = ReconciliationValidator()
pg_report = validator.validate(raw, postgres, contract, f"{batch_id}-postgres")
print("Raw ↔ Postgres:", pg_report.overall_status.value)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validate raw ↔ Redshift
# MAGIC
# MAGIC Note: Redshift version has PII masking applied, so we compare non-PII columns only.

# COMMAND ----------

rs_report = validator.validate(raw, redshift, contract, f"{batch_id}-redshift")
print("Raw ↔ Redshift:", rs_report.overall_status.value)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validate Postgres ↔ Redshift (cross-target parity)

# COMMAND ----------

# This catches bugs in the load phase itself. If raw matches both but pg/rs
# disagree, something in the dual-write path is broken.
pg_rs_report = validator.validate(postgres, redshift, contract, f"{batch_id}-pg-rs")
print("Postgres ↔ Redshift:", pg_rs_report.overall_status.value)

# COMMAND ----------

import json

overall = "PASSED"
if any(r.overall_status.value == "FAILED" for r in [pg_report, rs_report, pg_rs_report]):
    overall = "FAILED"

result = {
    "contract_fqn": dbutils.widgets.get("contract_fqn"),
    "batch_id": batch_id,
    "phase": "post-load-full-validation",
    "overall_status": overall,
    "raw_vs_postgres": pg_report.overall_status.value,
    "raw_vs_redshift": rs_report.overall_status.value,
    "postgres_vs_redshift": pg_rs_report.overall_status.value,
}

audit_path = f"s3://{dbutils.widgets.get('audit_bucket')}/validation-reports/{sap_table}/{batch_id}/post_load.json"
dbutils.fs.put(audit_path, json.dumps(result, indent=2), overwrite=True)

dbutils.notebook.exit(json.dumps(result))
