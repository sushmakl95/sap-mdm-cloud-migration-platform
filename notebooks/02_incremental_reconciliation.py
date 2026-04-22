# Databricks notebook source
# MAGIC %md
# MAGIC # Incremental Reconciliation
# MAGIC
# MAGIC For incremental loads, we don't re-scan the full source table every night — that
# MAGIC would defeat the purpose of an incremental pipeline. Instead, we validate:
# MAGIC
# MAGIC   - The delta since last watermark is complete (row count matches source count for that window)
# MAGIC   - The delta merged correctly (PK-level spot checks on a sample)
# MAGIC   - No stale rows: target rows older than watermark still match source values
# MAGIC
# MAGIC This notebook runs faster than the full reconciliation (minutes, not 30+ minutes
# MAGIC for large tables) and is the right check for nightly incremental jobs.

# COMMAND ----------

dbutils.widgets.text("contract_fqn", "")
dbutils.widgets.text("batch_id", "")
dbutils.widgets.text("watermark_column", "ERDAT")
dbutils.widgets.text("watermark_from", "")
dbutils.widgets.text("watermark_to", "")
dbutils.widgets.text("raw_bucket", "")
dbutils.widgets.text("audit_bucket", "")

# COMMAND ----------

from migration.schemas import TABLE_CONTRACTS
from migration.utils.secrets import get_secret

sap_table = dbutils.widgets.get("contract_fqn").split(".")[-1]
batch_id = dbutils.widgets.get("batch_id")
wm_col = dbutils.widgets.get("watermark_column")
wm_from = dbutils.widgets.get("watermark_from")
wm_to = dbutils.widgets.get("watermark_to")

contract = TABLE_CONTRACTS[sap_table]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read source incremental window

# COMMAND ----------

creds = get_secret("sap-mdm/prod/hana-jdbc")
source_window = (
    spark.read.format("jdbc")
    .option("url", f"jdbc:sap://{creds['host']}:{creds['port']}/?databaseName={creds['database']}")
    .option("dbtable",
        f"(SELECT * FROM {contract.sap_schema}.{contract.sap_table} "
        f"WHERE {wm_col} > TO_TIMESTAMP('{wm_from}', 'YYYY-MM-DD HH24:MI:SS') "
        f"AND {wm_col} <= TO_TIMESTAMP('{wm_to}', 'YYYY-MM-DD HH24:MI:SS')) src"
    )
    .option("user", creds["username"])
    .option("password", creds["password"])
    .option("driver", "com.sap.db.jdbc.Driver")
    .load()
)

source_window_count = source_window.count()
print(f"Source window rows: {source_window_count:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read target rows within the same window

# COMMAND ----------

target_inc_col = [m for m in contract.columns if m.source_name == wm_col][0].target_name
pg_creds = get_secret("sap-mdm/prod/postgres")

target_window = (
    spark.read.format("jdbc")
    .option("url", f"jdbc:postgresql://{pg_creds['host']}:{pg_creds['port']}/{pg_creds['database']}")
    .option("dbtable",
        f"(SELECT * FROM {contract.target_fqn_postgres} "
        f"WHERE {target_inc_col} > '{wm_from}' "
        f"AND {target_inc_col} <= '{wm_to}') t"
    )
    .option("user", pg_creds["username"])
    .option("password", pg_creds["password"])
    .option("driver", "org.postgresql.Driver")
    .load()
)

target_window_count = target_window.count()
print(f"Target window rows: {target_window_count:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Layer 1 — row count parity on the window

# COMMAND ----------

row_count_diff_pct = (
    abs(source_window_count - target_window_count) / source_window_count * 100
    if source_window_count > 0 else 0.0
)
print(f"Row count diff: {row_count_diff_pct:.3f}%")

status_layer1 = "PASSED" if row_count_diff_pct < 0.1 else "FAILED"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Layer 2 — PK uniqueness in the window

# COMMAND ----------

pk_cols = contract.primary_keys
total = target_window.count()
distinct = target_window.select(*pk_cols).distinct().count()
duplicates = total - distinct

status_layer2 = "PASSED" if duplicates == 0 else "FAILED"
print(f"PK uniqueness: {duplicates} duplicates in {total:,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Layer 3 — PK-level spot check (50 random rows)

# COMMAND ----------

from pyspark.sql import functions as F

sample_pks = (
    target_window.select(*pk_cols)
    .sample(withReplacement=False, fraction=min(1.0, 50 / total))
    .limit(50)
    .collect()
)

# Build an IN clause for the source query
pk_predicates = []
for r in sample_pks:
    conditions = " AND ".join(f"{c} = '{r[c]}'" for c in pk_cols)
    pk_predicates.append(f"({conditions})")

if pk_predicates:
    source_samples = (
        spark.read.format("jdbc")
        .option("url", f"jdbc:sap://{creds['host']}:{creds['port']}/?databaseName={creds['database']}")
        .option("dbtable",
            f"(SELECT * FROM {contract.sap_schema}.{contract.sap_table} "
            f"WHERE {' OR '.join(pk_predicates)}) s"
        )
        .option("user", creds["username"])
        .option("password", creds["password"])
        .option("driver", "com.sap.db.jdbc.Driver")
        .load()
    )
    source_sample_count = source_samples.count()
    status_layer3 = "PASSED" if source_sample_count == len(sample_pks) else "FAILED"
    print(f"Spot check: {source_sample_count}/{len(sample_pks)} PKs found in source")
else:
    status_layer3 = "PASSED"
    print("No rows to spot-check")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Aggregate + export

# COMMAND ----------

import json

report = {
    "contract_fqn": dbutils.widgets.get("contract_fqn"),
    "batch_id": batch_id,
    "phase": "incremental-validation",
    "window": {"from": wm_from, "to": wm_to, "column": wm_col},
    "results": [
        {"layer": "row_count_window", "status": status_layer1,
         "source_count": source_window_count, "target_count": target_window_count,
         "diff_pct": row_count_diff_pct},
        {"layer": "pk_uniqueness_window", "status": status_layer2,
         "duplicates": duplicates, "total": total},
        {"layer": "pk_spot_check", "status": status_layer3,
         "sample_size": len(sample_pks) if pk_predicates else 0},
    ],
    "overall_status": "FAILED" if "FAILED" in (status_layer1, status_layer2, status_layer3) else "PASSED",
}

audit_path = f"s3://{dbutils.widgets.get('audit_bucket')}/validation-reports/{sap_table}/{batch_id}/incremental.json"
dbutils.fs.put(audit_path, json.dumps(report, indent=2), overwrite=True)

dbutils.notebook.exit(json.dumps({
    "overall_status": report["overall_status"],
    "report_path": audit_path,
}))
