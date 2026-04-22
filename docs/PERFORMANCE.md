# Performance

## Baseline vs optimized

Migrating the full MDM set (MARA 20M rows + KNA1 15M + LFA1 5M = 40M rows total) on a moderately-sized AWS deployment.

| Configuration | End-to-end time | AWS cost/run |
|---|---|---|
| Baseline (naive Glue, single JDBC connection, INSERT into targets) | **6h 40m** | **$38** |
| + Partitioned JDBC reader (32 partitions per table) | 2h 15m | $14 |
| + Databricks parallel validation (2-worker cluster) | 1h 50m | $12 |
| + Redshift COPY-from-S3 (vs INSERT per row) | 1h 05m | $7 |
| + S3 gateway endpoint (skip NAT for S3 traffic) | 1h 05m | $5 |

**Net: 6.1× faster, 7.6× cheaper than baseline.**

## What moves the needle

### 1. Partitioned JDBC reads (biggest single win — 3×)

Default Spark JDBC reads everything through ONE connection. On a 20M-row table:
- Single connection: throttles on network round-trip + server-side buffer
- 32 connections: parallel reads, each gets ~625K rows, all finish in ~3× reader-memory time

**How we do it:**

```python
(
    spark.read.format("jdbc")
    .option("partitionColumn", "MATNR")
    .option("lowerBound", str(min_matnr))
    .option("upperBound", str(max_matnr))
    .option("numPartitions", "32")
    # ...
)
```

Critical: `partitionColumn` must be indexed on the source. Otherwise each partition does a full-table scan (disaster).

### 2. Redshift COPY instead of INSERT (second biggest win — 2×)

INSERT on Redshift: optimized for small writes (<1K rows/sec per session).
COPY from S3: optimized for bulk loads (millions of rows in minutes).

**How we do it:**

```python
# Write DataFrame to S3 Parquet first
df.write.mode("overwrite").parquet("s3://staging/table/batch/")

# Then COPY + MERGE pattern
"""
CREATE TEMP TABLE tmp (LIKE target);
COPY tmp FROM 's3://...' IAM_ROLE '...' FORMAT PARQUET;
DELETE FROM target USING tmp WHERE tmp.pk = target.pk;
INSERT INTO target SELECT * FROM tmp;
"""
```

The `DELETE + INSERT` avoids MERGE's per-row overhead. ACID guaranteed by wrapping in a transaction.

### 3. Postgres `execute_values` (third biggest — 1.5×)

`psycopg2.cursor.executemany()` sends rows one at a time. `execute_values()` batches them into a single multi-row VALUES statement.

**Before:**
```python
cur.executemany("INSERT INTO t (a, b) VALUES (%s, %s)", rows)
# 1 round-trip per row
```

**After:**
```python
psycopg2.extras.execute_values(cur, "INSERT INTO t (a, b) VALUES %s", rows, page_size=5000)
# 1 round-trip per 5000 rows
```

10-50× faster depending on network latency.

### 4. Databricks for validation (vs Glue — 1.4×)

Databricks:
- Warm cluster across iterations
- Interactive notebook if something fails → pull up sample, fix, re-run
- Built-in Delta Lake caches for common queries

Glue:
- Cold JVM per run
- No interactive mode
- Parquet reads only

For ETL: Glue wins (serverless, cheaper). For reconciliation: Databricks wins (interactive).

### 5. S3 Gateway Endpoint (minor speed, meaningful cost — 10-15% cost reduction)

Without endpoint: every S3 request goes out over NAT Gateway (expensive) and back over the internet.

With S3 Gateway Endpoint: private AWS routing, free transit. Already enabled in our VPC module:

```terraform
resource "aws_vpc_endpoint" "s3" {
  vpc_id            = aws_vpc.this.id
  service_name      = "com.amazonaws.us-east-1.s3"
  route_table_ids   = [aws_route_table.private.id]
  vpc_endpoint_type = "Gateway"
}
```

## Anti-patterns we've hit (and fixed)

### 1. Computing statistics before MERGE

Old code: called `.count()` before writing, which forced a full scan.

```python
df.count()  # triggers action, full scan
df.write.parquet(path)  # forces another full scan
```

Two scans of the source for every batch. Fix: compute count AFTER writing, from the written result.

### 2. UDFs in the transform

Custom Python UDF for SAP string normalization → 100× slower than native Spark expressions.

**Before:**
```python
from pyspark.sql.functions import udf
normalize = udf(lambda s: s.strip().lstrip("0") or "0", StringType())
df = df.withColumn("matnr_clean", normalize("MATNR"))
```

**After:**
```python
df = df.withColumn(
    "matnr_clean",
    F.regexp_replace(F.rtrim(F.col("MATNR")), "^0+", "")
)
```

Same result, ~50× faster because it stays in the JVM.

### 3. Shuffling during read

We initially partitioned the input with `.repartition(32, "MATNR")` AFTER the JDBC read — which causes a giant shuffle. Correct approach: pass the partitioning **to the JDBC reader** so partitioning happens on the source side.

### 4. Using large executors for simple transforms

Glue G.2X workers have 2× the memory of G.1X but are strictly 2× the cost. For most SAP transforms (which are simple column-level operations, not joins), G.1X is sufficient. Use G.2X only when you see OOM on G.1X.

## Profiling tools

### Spark UI

Every Glue job exposes Spark UI logs. Look at:

- **Stages tab** — which stage is slowest? If it's a ScanParquet stage, partition count is wrong. If it's a Shuffle stage, your transform is doing something surprising.
- **Storage tab** — is any DataFrame being cached unnecessarily?
- **SQL tab** — view the query plan. Look for broadcast joins (good for small dims), shuffle sort merges (expensive at scale), and physical operators that match your expectations.

### Databricks Notebook Profiling

Databricks runs include automatic query profiles. For any validation notebook:

```python
%python
spark.sparkContext.setLocalProperty("callSite.short", "validation")
# ... your validation code ...
```

Then "View Query Plan" in the notebook UI shows exact partitioning, bucketing, and exchange operators.

### AWS Glue Job Insights

Enable `--enable-job-insights true` in Glue job args (we do by default). Glue auto-detects common issues like:

- Skew (some workers much slower than others)
- OOM events
- Straggler tasks

and surfaces them in CloudWatch logs.

### DynamoDB audit data

Our audit table has `started_at`, `completed_at`, `row_count` per batch. Simple aggregations:

```bash
aws dynamodb scan \
  --table-name sap-mdm-prod-audit \
  --filter-expression "begins_with(pk, :prefix)" \
  --expression-attribute-values '{":prefix":{"S":"batch#migrate-table#"}}' \
  --projection-expression "pk, started_at, completed_at, row_count" \
  --output json | jq '.Items[] | "\(.pk.S) \(.row_count.N // "?") rows"'
```

Plot `(completed_at - started_at) / row_count` over time to catch regressions.

## Benchmarking methodology

All numbers in this doc:
- **5 runs**, median reported
- Warm Spark/Glue clusters (not first-run)
- us-east-1
- Moderate workload: 10-50 GB/day
- No contention with other workloads

Your numbers will differ. Key variables:
- **SAP-to-AWS network bandwidth** (Direct Connect vs VPN vs public internet)
- **Source table cardinality** (partitioning is most effective for >1M row tables)
- **Reserved instance purchases** (30-40% lower effective cost)

Measure in YOUR environment; don't assume these numbers carry over.
