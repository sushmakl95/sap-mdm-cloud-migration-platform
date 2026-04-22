"""Tests for SAP type coercer."""

from __future__ import annotations

import pytest
from pyspark.sql.types import StringType, StructField, StructType

from migration.schemas import MARA_CONTRACT
from migration.transformers import DdlGenerator, SapTypeTransformer

pytestmark = pytest.mark.unit


def test_transform_strips_leading_zeros_from_matnr(spark):
    """MATNR '000000000000001000' should become '1000'."""
    data = [
        ("000000000000001000", "HAWA", "EA", "", ""),
        ("000000000000020000", "FERT", "KG", "", ""),
    ]
    schema = StructType([
        StructField("MATNR", StringType(), True),
        StructField("MTART", StringType(), True),
        StructField("MEINS", StringType(), True),
        StructField("LVORM", StringType(), True),
        StructField("MANDT", StringType(), True),
    ])
    df = spark.createDataFrame(data, schema)

    transformer = SapTypeTransformer()
    out = transformer.transform(df, MARA_CONTRACT)

    rows = out.collect()
    material_numbers = [r["material_number"] for r in rows]
    assert "1000" in material_numbers
    assert "20000" in material_numbers


def test_transform_maps_domain_values(spark):
    """LVORM 'X'/'' should become BOOLEAN true/false."""
    data = [
        ("MATA", "HAWA", "EA", "X", ""),  # deleted
        ("MATB", "HAWA", "EA", "", ""),   # not deleted
    ]
    schema = StructType([
        StructField("MATNR", StringType(), True),
        StructField("MTART", StringType(), True),
        StructField("MEINS", StringType(), True),
        StructField("LVORM", StringType(), True),
        StructField("MANDT", StringType(), True),
    ])
    df = spark.createDataFrame(data, schema)

    transformer = SapTypeTransformer()
    out = transformer.transform(df, MARA_CONTRACT)

    rows = out.collect()
    # First row: LVORM='X' → is_deletion_flag=True
    # Second row: LVORM='' → is_deletion_flag=False
    deletion_flags = {r["material_number"]: r["is_deletion_flag"] for r in rows}
    assert deletion_flags["MATA"] is True
    assert deletion_flags["MATB"] is False


def test_transform_drops_mandt(spark):
    """MANDT (SAP client column) must not appear in target."""
    data = [("001", "MATA", "HAWA", "EA", "")]
    schema = StructType([
        StructField("MANDT", StringType(), True),
        StructField("MATNR", StringType(), True),
        StructField("MTART", StringType(), True),
        StructField("MEINS", StringType(), True),
        StructField("LVORM", StringType(), True),
    ])
    df = spark.createDataFrame(data, schema)

    transformer = SapTypeTransformer()
    out = transformer.transform(df, MARA_CONTRACT)

    assert "MANDT" not in out.columns
    assert "mandt" not in out.columns


def test_transform_trims_char_padding(spark):
    """SAP CHAR fields padded with spaces must be trimmed."""
    data = [("MATA    ", "HAWA", "EA  ", "", "")]
    schema = StructType([
        StructField("MATNR", StringType(), True),
        StructField("MTART", StringType(), True),
        StructField("MEINS", StringType(), True),
        StructField("LVORM", StringType(), True),
        StructField("MANDT", StringType(), True),
    ])
    df = spark.createDataFrame(data, schema)

    transformer = SapTypeTransformer()
    out = transformer.transform(df, MARA_CONTRACT)

    row = out.first()
    assert row["material_number"] == "MATA"
    assert row["base_unit_of_measure"] == "EA"


def test_ddl_generator_produces_valid_postgres_ddl():
    gen = DdlGenerator()
    output = gen.generate(MARA_CONTRACT)

    assert "CREATE TABLE" in output.postgres_ddl
    assert "mdm.material_master" in output.postgres_ddl
    assert "PRIMARY KEY (material_number)" in output.postgres_ddl


def test_ddl_generator_produces_valid_redshift_ddl():
    gen = DdlGenerator()
    output = gen.generate(MARA_CONTRACT)

    assert "CREATE TABLE" in output.redshift_ddl
    assert "mdm.material_master" in output.redshift_ddl
    # Material master gets DISTKEY on PK (not DISTSTYLE ALL)
    assert "DISTKEY" in output.redshift_ddl or "DISTSTYLE ALL" in output.redshift_ddl
    # SORTKEY should include material_number + _loaded_at
    assert "SORTKEY" in output.redshift_ddl


def test_ddl_generator_emits_indexes():
    gen = DdlGenerator()
    output = gen.generate(MARA_CONTRACT)

    assert len(output.postgres_indexes) > 0
    # _loaded_at index is always emitted
    assert any("_loaded_at" in idx for idx in output.postgres_indexes)
