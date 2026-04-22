# Reconciliation Strategy — 5-Layer Validation

## The core problem

Given that source (SAP) and target (Postgres + Redshift) have different types, different storage layouts, different query semantics, and different performance characteristics — **how do we prove the data is equivalent without re-reading every row into a single process?**

Strict equality requires a Cartesian compare of every row. For a 20M-row table with 30 columns, that's 600M comparisons. Doable in batch but:
- Expensive (~15min of Databricks cluster time per table)
- Hard to pinpoint the cause when it fails ("some rows differ" doesn't tell you which or why)

Our approach: **5 independent layers, each catching a different failure class.** Each layer is O(N) and parallelizable. When something fails, we know which class of bug to investigate.

## The 5 layers

### Layer 1 — Row-count parity

**What it catches:** Gross data loss or duplication.

**How it works:** Run `SELECT COUNT(*)` on source and target. Compare. Fail if difference exceeds `row_count_tolerance_pct` from the contract.

**Why it's fast:** Both databases optimize COUNT(*). Single round-trip each.

**False positive risk:** Low. But watch out for:
- Source is still being written to during comparison window (use SAP point-in-time snapshot or freeze window)
- Target has residual rows from a failed prior batch (we use `_loaded_at > batch_start` predicate)

**Typical failure cause:** A partition of the source wasn't extracted (JDBC read silently dropped rows due to connection pool exhaustion).

### Layer 2 — Null-ratio drift

**What it catches:** Column-mapping bugs. The mapping might have a typo that silently maps all source values into NULL, or fails a type cast.

**How it works:** For each column, compute `NULL_COUNT / TOTAL` in source and target. Alert on columns where difference > 5 percentage points.

**Why 5%?** Small differences are expected:
- Some nulls become "0" (e.g., if `default_on_null` is set)
- Some nulls become "UNKNOWN" (string defaults)

Larger drifts are almost always bugs.

**Typical failure cause:** The target column's type was stricter than source; values that parsed-as-numeric in SAP came through as text, cast to NUMERIC failed → silent NULL.

### Layer 3 — Primary key uniqueness

**What it catches:** Duplicate rows introduced during load. This can happen when:
- A CDC event is delivered twice and not deduped
- A join during transform produced a Cartesian (1:many become many:many)
- Target load used INSERT instead of UPSERT

**How it works:** `COUNT(*) vs COUNT(DISTINCT pk_cols)` on target. If they differ, collect sample duplicate PKs.

**Cost:** O(N) with a distinct aggregation. Fast on Databricks with AQE.

### Layer 4 — Row-level checksum bands

**What it catches:** Content-level corruption (wrong values in fields that still look "valid"). This is the killer layer for migrations — it catches the subtle bugs:
- Unit conversions silently wrong (USD vs CENTS)
- Date formatting errors (Dec 1 vs Jan 12 — US vs EU parsing)
- Encoding issues (smart quotes → garbage)
- Rounding drift (SAP 13,3 → target 10,2)

**How it works:** Hash-bucket rows by `hash(pk) % 256`. For each bucket, compute checksum = md5(sorted_concatenation_of_row_md5s). Compare bucket-by-bucket.

```
Bucket 0:  source checksum = a1b2c3...  target checksum = a1b2c3...  ✓
Bucket 1:  source checksum = 4d5e6f...  target checksum = 4d5e6f...  ✓
Bucket 2:  source checksum = 789012...  target checksum = 789013...  ✗ (investigate)
```

**Why buckets?** Lets us parallelize and zoom in. If bucket 2 differs, we only need to pull 1/256th of the data to compare row-by-row.

**False positive risk:** Medium. Sensitive to:
- Column ordering differences (we sort)
- Trailing whitespace (we trim)
- NULL representations (we use sentinel "∅")

### Layer 5 — Statistical distribution

**What it catches:** Arithmetic drift. Even when row-level checksums match, aggregate statistics might differ subtly:
- Total revenue has drifted by 0.01%
- Mean weight has changed by 0.5%
- A column silently lost precision

**How it works:** For each numeric column, compute sum, mean on both sides. Compare. Alert if difference > 0.5%.

**Why 0.5% tolerance?** Legitimate differences come from:
- Decimal precision truncation (SAP DECIMAL(13,3) → Postgres NUMERIC(10,2))
- Currency conversion if we're hash-masking (shouldn't change sums though)

For financial / medical data, tighten to 0.01%. Configure per-contract.

## What a real validation report looks like

```json
{
  "contract_fqn": "HANA:SAPSR3.MARA",
  "batch_id": "batch-2026-04-21-a3f7c2",
  "validated_at": "2026-04-21T02:15:33Z",
  "overall_status": "PASSED",
  "source_row_count": 1247832,
  "target_row_count": 1247832,
  "results": [
    {"layer": "row_count", "status": "PASSED", "message": "source=1,247,832 target=1,247,832 diff=0.000%"},
    {"layer": "null_ratio", "status": "PASSED", "message": "All columns within null-ratio tolerance"},
    {"layer": "pk_uniqueness", "status": "PASSED", "message": "All 1,247,832 rows have unique PK"},
    {"layer": "checksum_bands", "status": "PASSED", "message": "All 256 buckets match between source and target"},
    {"layer": "statistical_distribution", "status": "PASSED", "message": "All 3 numeric columns within tolerance"}
  ]
}
```

Failure example:

```json
{
  "overall_status": "FAILED",
  "results": [
    {"layer": "row_count", "status": "PASSED", "message": "..."},
    {"layer": "null_ratio", "status": "FAILED",
     "message": "1 column(s) exceeded 5% null-ratio drift",
     "evidence": {
       "drifts": [{"column": "weight_unit", "source_null_pct": 0.12, "target_null_pct": 87.45, "drift_pct": 87.33}]
     }
    },
    {"layer": "pk_uniqueness", "status": "PASSED", "message": "..."}
  ]
}
```

This says: "weight_unit was 0.12% null in source, 87.45% null in target — investigate." The likely cause: a typo in the column mapping for weight_unit, sending source `GEWEI` values to the wrong target column.

## Running validation

```bash
sap-migrate validate --table MARA --batch-id <id>
# OR via the Databricks job (scheduled)
```

Reports are written to:
- **S3 audit bucket** (`s3://sap-mdm-audit-*/validation-reports/<TABLE>/<batch_id>/report.json`) for SOX
- **DynamoDB audit table** (summary row with status + report link)
- **CloudWatch** (metrics for dashboards)

## When to tune tolerances

Defaults (in code):
- Row count tolerance: 0.0% (strict)
- Null ratio drift: 5 percentage points
- Checksum: exact match required
- Statistical distribution: 0.5%

Tune per-contract when:
- **Source is live during extract**: row count tolerance → 0.1% (accommodates new writes)
- **Target has intentional defaults**: null ratio drift → 10% (accommodates `default_on_null`)
- **Aggressive precision reduction intentional**: statistical distribution → 2%

NEVER relax PK uniqueness. Duplicates = bug, always.

## What we don't validate

- **Semantic correctness** (e.g., "customer_id 123 refers to Alice" — nobody can verify this from data alone)
- **Referential integrity** to other tables (separate validation step, cross-table)
- **Business rules outside the schema** (e.g., "material XYZ should not exist for customer ABC"). Those are DQ rules, not reconciliation.

## Lessons learned

**Don't over-index on layer 1.** Row count matching is necessary but FAR from sufficient. We've seen perfect counts + completely wrong content.

**Layer 4 (checksum) is the workhorse.** It catches 80%+ of real bugs. The other layers are primarily diagnostic.

**Run validation BEFORE load, not after.** If validation fails, we don't want corrupted data in the target. Our pipeline runs extract → validate → load precisely because of this.

**Don't fail batches on warnings.** Reserve FAILED for hard violations. WARNING (e.g., "95% match but <99% target") lets humans decide.
