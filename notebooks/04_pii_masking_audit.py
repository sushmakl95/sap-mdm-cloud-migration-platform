# Databricks notebook source
# MAGIC %md
# MAGIC # PII Masking Audit
# MAGIC
# MAGIC Runs weekly against the Redshift target. Verifies that PII-sensitive columns
# MAGIC (email, phone, names) are properly masked — i.e., they contain SHA256 hashes,
# MAGIC not raw values.
# MAGIC
# MAGIC If raw PII is found in Redshift, we have a compliance incident: the
# MAGIC transformer's PII masking logic failed somewhere. This notebook is
# MAGIC the safety net.

# COMMAND ----------

from migration.schemas import TABLE_CONTRACTS
from migration.utils.secrets import get_secret
from pyspark.sql import functions as F

# COMMAND ----------

rs_creds = get_secret("sap-mdm/prod/redshift")

# Columns that MUST be masked in analytical (Redshift) target
PII_PATTERNS = {
    "email": r"^[\w\.-]+@[\w\.-]+\.\w+$",  # raw email pattern
    "phone": r"^[\+\d\-\(\)\s]{7,}$",       # raw phone pattern
}

# SHA256 hashes are exactly 64 hex chars
SHA256_PATTERN = r"^[0-9a-f]{64}$"

# COMMAND ----------

def check_pii_column(schema: str, table: str, column: str):
    """Check one column for raw PII."""
    df = (
        spark.read.format("jdbc")
        .option("url", f"jdbc:redshift://{rs_creds['host']}:{rs_creds['port']}/{rs_creds['database']}")
        .option("dbtable", f"(SELECT {column} FROM {schema}.{table} WHERE {column} IS NOT NULL LIMIT 1000) sample")
        .option("user", rs_creds["username"])
        .option("password", rs_creds["password"])
        .option("driver", "com.amazon.redshift.jdbc42.Driver")
        .load()
    )

    total = df.count()
    if total == 0:
        return {"column": f"{schema}.{table}.{column}", "sampled": 0, "status": "NO_DATA"}

    hashed = df.filter(F.col(column).rlike(SHA256_PATTERN)).count()
    raw_email_like = df.filter(F.col(column).rlike(PII_PATTERNS["email"])).count()
    raw_phone_like = df.filter(F.col(column).rlike(PII_PATTERNS["phone"])).count()

    is_masked = hashed / total > 0.95

    return {
        "column": f"{schema}.{table}.{column}",
        "sampled": total,
        "hashed_count": hashed,
        "raw_email_like": raw_email_like,
        "raw_phone_like": raw_phone_like,
        "status": "MASKED" if is_masked else "FAIL_RAW_PII_FOUND",
    }

# COMMAND ----------

results = []
for contract_name, contract in TABLE_CONTRACTS.items():
    if not contract.enable_pii_masking:
        continue

    pii_cols = {"email", "phone_primary", "fax", "customer_name", "vendor_name"}
    for m in contract.columns:
        if m.target_name in pii_cols:
            result = check_pii_column(
                contract.target_redshift_schema,
                contract.target_redshift_table,
                m.target_name,
            )
            results.append(result)
            print(result)

# COMMAND ----------

import json

fails = [r for r in results if r["status"] == "FAIL_RAW_PII_FOUND"]
overall = "FAIL" if fails else "PASS"

audit = {
    "overall_status": overall,
    "columns_checked": len(results),
    "failed_columns": fails,
}

print(f"\nPII audit result: {overall}")

dbutils.notebook.exit(json.dumps(audit))
