"""Tests for 5-layer reconciliation engine."""

from __future__ import annotations

import pytest
from pyspark.sql.types import StringType, StructField, StructType

from migration.schemas import MARA_CONTRACT
from migration.validators import ReconciliationValidator, ValidationStatus

pytestmark = pytest.mark.unit


def _build_mara_df(spark, rows):
    """Build a pseudo-MARA DataFrame with target-side column names."""
    columns = [m.target_name for m in MARA_CONTRACT.columns if not m.is_audit]
    schema = StructType([StructField(c, StringType(), True) for c in columns])
    return spark.createDataFrame(rows, schema)


def test_row_count_match_passes(spark):
    """Equal row counts pass layer 1."""
    source_rows = [("MAT1",) + ("X",) * 12] * 100
    target_rows = [("MAT1",) + ("X",) * 12] * 100

    source = _build_mara_df(spark, source_rows)
    target = _build_mara_df(spark, target_rows)

    validator = ReconciliationValidator()
    report = validator.validate(source, target, MARA_CONTRACT, "test-batch")

    row_count_result = next(r for r in report.results if r.layer == "row_count")
    assert row_count_result.status == ValidationStatus.PASSED


def test_row_count_mismatch_fails(spark):
    """Very different row counts fail layer 1."""
    source_rows = [(f"MAT{i}",) + ("X",) * 12 for i in range(100)]
    target_rows = [(f"MAT{i}",) + ("X",) * 12 for i in range(90)]  # 10 missing

    source = _build_mara_df(spark, source_rows)
    target = _build_mara_df(spark, target_rows)

    validator = ReconciliationValidator()
    report = validator.validate(source, target, MARA_CONTRACT, "test-batch")

    row_count_result = next(r for r in report.results if r.layer == "row_count")
    assert row_count_result.status == ValidationStatus.FAILED
    assert row_count_result.evidence["source_count"] == 100
    assert row_count_result.evidence["target_count"] == 90


def test_pk_uniqueness_catches_duplicates(spark):
    """Duplicate PKs in target fail layer 3."""
    source_rows = [(f"MAT{i}",) + ("X",) * 12 for i in range(10)]
    # Duplicate MAT1
    target_rows = [("MAT1",) + ("X",) * 12] * 2 + [
        (f"MAT{i}",) + ("X",) * 12 for i in range(2, 11)
    ]

    source = _build_mara_df(spark, source_rows)
    target = _build_mara_df(spark, target_rows)

    validator = ReconciliationValidator()
    report = validator.validate(source, target, MARA_CONTRACT, "test-batch")

    pk_result = next(r for r in report.results if r.layer == "pk_uniqueness")
    assert pk_result.status == ValidationStatus.FAILED
    assert pk_result.evidence["duplicate_count"] == 1


def test_overall_status_fails_on_any_layer_failure(spark):
    """Overall status = FAILED if any layer fails."""
    # Row count mismatch
    source_rows = [(f"MAT{i}",) + ("X",) * 12 for i in range(100)]
    target_rows = [(f"MAT{i}",) + ("X",) * 12 for i in range(50)]

    source = _build_mara_df(spark, source_rows)
    target = _build_mara_df(spark, target_rows)

    validator = ReconciliationValidator()
    report = validator.validate(source, target, MARA_CONTRACT, "test-batch")

    assert report.overall_status == ValidationStatus.FAILED


def test_validation_report_json_serializable(spark):
    source_rows = [(f"MAT{i}",) + ("X",) * 12 for i in range(3)]
    target_rows = source_rows

    source = _build_mara_df(spark, source_rows)
    target = _build_mara_df(spark, target_rows)

    validator = ReconciliationValidator()
    report = validator.validate(source, target, MARA_CONTRACT, "test-batch")

    # Should serialize without error
    json_str = report.to_json()
    assert "overall_status" in json_str
    assert "test-batch" in json_str
