"""Concrete contracts for the MDM core tables.

These are the three tables every SAP migration must handle:
  - MARA: Material Master — products, SKUs, item attributes
  - KNA1: Customer Master — customer accounts
  - LFA1: Vendor Master — supplier accounts

Plus one Z-table example showing how custom SAP enhancements are mapped:
  - ZPRICE_HISTORY: custom pricing history table
"""

from __future__ import annotations

from migration.schemas.contracts import ColumnMapping, TableContract

# -----------------------------------------------------------------------------
# MARA — Material Master
# -----------------------------------------------------------------------------
MARA_CONTRACT = TableContract(
    sap_system="HANA",
    sap_schema="SAPSR3",
    sap_table="MARA",
    target_postgres_schema="mdm",
    target_postgres_table="material_master",
    target_redshift_schema="mdm",
    target_redshift_table="material_master",
    partition_column="MATNR",
    partition_num_chunks=32,
    primary_keys=["material_number"],
    load_strategy="incremental_merge",
    incremental_column="changed_on",
    row_count_tolerance_pct=0.0,
    enable_cdc=True,
    columns=[
        ColumnMapping(
            source_name="MATNR", target_name="material_number",
            sap_type="CHAR", sap_length=18,
            postgres_type="VARCHAR(18)", redshift_type="VARCHAR(18)",
            nullable=False, is_pk=True,
            description="Unique material number (primary business key)",
        ),
        ColumnMapping(
            source_name="ERSDA", target_name="created_on",
            sap_type="DATE",
            postgres_type="DATE", redshift_type="DATE",
            description="Date when material was created",
        ),
        ColumnMapping(
            source_name="ERNAM", target_name="created_by",
            sap_type="CHAR", sap_length=12,
            postgres_type="VARCHAR(12)", redshift_type="VARCHAR(12)",
            description="SAP user who created the record",
        ),
        ColumnMapping(
            source_name="LAEDA", target_name="last_changed_on",
            sap_type="DATE",
            postgres_type="DATE", redshift_type="DATE",
        ),
        ColumnMapping(
            source_name="AENAM", target_name="last_changed_by",
            sap_type="CHAR", sap_length=12,
            postgres_type="VARCHAR(12)", redshift_type="VARCHAR(12)",
        ),
        ColumnMapping(
            source_name="MTART", target_name="material_type",
            sap_type="CHAR", sap_length=4,
            postgres_type="VARCHAR(4)", redshift_type="VARCHAR(4)",
            description="HAWA=Trading goods, FERT=Finished, ROH=Raw",
        ),
        ColumnMapping(
            source_name="MBRSH", target_name="industry_sector",
            sap_type="CHAR", sap_length=1,
            postgres_type="CHAR(1)", redshift_type="CHAR(1)",
        ),
        ColumnMapping(
            source_name="MATKL", target_name="material_group",
            sap_type="CHAR", sap_length=9,
            postgres_type="VARCHAR(9)", redshift_type="VARCHAR(9)",
        ),
        ColumnMapping(
            source_name="MEINS", target_name="base_unit_of_measure",
            sap_type="CHAR", sap_length=3,
            postgres_type="VARCHAR(3)", redshift_type="VARCHAR(3)",
            description="EA, KG, LB, M, etc.",
        ),
        ColumnMapping(
            source_name="BRGEW", target_name="gross_weight",
            sap_type="DECIMAL", sap_precision=13, sap_scale=3,
            postgres_type="NUMERIC(13,3)", redshift_type="DECIMAL(13,3)",
        ),
        ColumnMapping(
            source_name="NTGEW", target_name="net_weight",
            sap_type="DECIMAL", sap_precision=13, sap_scale=3,
            postgres_type="NUMERIC(13,3)", redshift_type="DECIMAL(13,3)",
        ),
        ColumnMapping(
            source_name="GEWEI", target_name="weight_unit",
            sap_type="CHAR", sap_length=3,
            postgres_type="VARCHAR(3)", redshift_type="VARCHAR(3)",
        ),
        ColumnMapping(
            source_name="LVORM", target_name="is_deletion_flag",
            sap_type="CHAR", sap_length=1,
            postgres_type="BOOLEAN", redshift_type="BOOLEAN",
            # SAP convention: 'X' = true, '' = false
            sap_domain_map={"X": "true", "": "false"},
            default_on_null="false",
            description="SAP deletion marker — 'X' means logically deleted",
        ),
    ],
    notes="Core material master; feeds all product-facing services.",
)


# -----------------------------------------------------------------------------
# KNA1 — Customer Master (General Data)
# -----------------------------------------------------------------------------
KNA1_CONTRACT = TableContract(
    sap_system="HANA",
    sap_schema="SAPSR3",
    sap_table="KNA1",
    target_postgres_schema="mdm",
    target_postgres_table="customer_master",
    target_redshift_schema="mdm",
    target_redshift_table="customer_master",
    partition_column="KUNNR",
    partition_num_chunks=32,
    primary_keys=["customer_number"],
    load_strategy="incremental_merge",
    incremental_column="changed_on",
    enable_pii_masking=True,
    enable_cdc=True,
    columns=[
        ColumnMapping(
            source_name="KUNNR", target_name="customer_number",
            sap_type="CHAR", sap_length=10,
            postgres_type="VARCHAR(10)", redshift_type="VARCHAR(10)",
            nullable=False, is_pk=True,
        ),
        ColumnMapping(
            source_name="NAME1", target_name="customer_name",
            sap_type="CHAR", sap_length=35,
            postgres_type="VARCHAR(35)", redshift_type="VARCHAR(35)",
        ),
        ColumnMapping(
            source_name="LAND1", target_name="country_code",
            sap_type="CHAR", sap_length=3,
            postgres_type="VARCHAR(3)", redshift_type="VARCHAR(3)",
        ),
        ColumnMapping(
            source_name="ORT01", target_name="city",
            sap_type="CHAR", sap_length=35,
            postgres_type="VARCHAR(35)", redshift_type="VARCHAR(35)",
        ),
        ColumnMapping(
            source_name="PSTLZ", target_name="postal_code",
            sap_type="CHAR", sap_length=10,
            postgres_type="VARCHAR(10)", redshift_type="VARCHAR(10)",
        ),
        ColumnMapping(
            source_name="STRAS", target_name="street_address",
            sap_type="CHAR", sap_length=35,
            postgres_type="VARCHAR(35)", redshift_type="VARCHAR(35)",
        ),
        ColumnMapping(
            source_name="TELF1", target_name="phone_primary",
            sap_type="CHAR", sap_length=16,
            postgres_type="VARCHAR(16)", redshift_type="VARCHAR(16)",
        ),
        ColumnMapping(
            source_name="TELFX", target_name="fax",
            sap_type="CHAR", sap_length=31,
            postgres_type="VARCHAR(31)", redshift_type="VARCHAR(31)",
        ),
        ColumnMapping(
            source_name="SMTP_ADDR", target_name="email",
            sap_type="CHAR", sap_length=241,
            postgres_type="VARCHAR(241)", redshift_type="VARCHAR(241)",
        ),
        ColumnMapping(
            source_name="ERDAT", target_name="created_on",
            sap_type="DATE",
            postgres_type="DATE", redshift_type="DATE",
        ),
        ColumnMapping(
            source_name="ERNAM", target_name="created_by",
            sap_type="CHAR", sap_length=12,
            postgres_type="VARCHAR(12)", redshift_type="VARCHAR(12)",
        ),
        ColumnMapping(
            source_name="LOEVM", target_name="is_deletion_flag",
            sap_type="CHAR", sap_length=1,
            postgres_type="BOOLEAN", redshift_type="BOOLEAN",
            sap_domain_map={"X": "true", "": "false"},
            default_on_null="false",
        ),
    ],
    notes="Core customer master — PII-sensitive, requires masking for analytical target.",
)


# -----------------------------------------------------------------------------
# LFA1 — Vendor Master (General Data)
# -----------------------------------------------------------------------------
LFA1_CONTRACT = TableContract(
    sap_system="HANA",
    sap_schema="SAPSR3",
    sap_table="LFA1",
    target_postgres_schema="mdm",
    target_postgres_table="vendor_master",
    target_redshift_schema="mdm",
    target_redshift_table="vendor_master",
    partition_column="LIFNR",
    partition_num_chunks=16,
    primary_keys=["vendor_number"],
    load_strategy="incremental_merge",
    incremental_column="changed_on",
    enable_cdc=True,
    columns=[
        ColumnMapping(
            source_name="LIFNR", target_name="vendor_number",
            sap_type="CHAR", sap_length=10,
            postgres_type="VARCHAR(10)", redshift_type="VARCHAR(10)",
            nullable=False, is_pk=True,
        ),
        ColumnMapping(
            source_name="NAME1", target_name="vendor_name",
            sap_type="CHAR", sap_length=35,
            postgres_type="VARCHAR(35)", redshift_type="VARCHAR(35)",
        ),
        ColumnMapping(
            source_name="LAND1", target_name="country_code",
            sap_type="CHAR", sap_length=3,
            postgres_type="VARCHAR(3)", redshift_type="VARCHAR(3)",
        ),
        ColumnMapping(
            source_name="STCD1", target_name="tax_number_1",
            sap_type="CHAR", sap_length=16,
            postgres_type="VARCHAR(16)", redshift_type="VARCHAR(16)",
        ),
        ColumnMapping(
            source_name="STCD2", target_name="tax_number_2",
            sap_type="CHAR", sap_length=11,
            postgres_type="VARCHAR(11)", redshift_type="VARCHAR(11)",
        ),
        ColumnMapping(
            source_name="ERDAT", target_name="created_on",
            sap_type="DATE",
            postgres_type="DATE", redshift_type="DATE",
        ),
        ColumnMapping(
            source_name="LOEVM", target_name="is_deletion_flag",
            sap_type="CHAR", sap_length=1,
            postgres_type="BOOLEAN", redshift_type="BOOLEAN",
            sap_domain_map={"X": "true", "": "false"},
            default_on_null="false",
        ),
    ],
    notes="Vendor master; limited cardinality (~50k rows typical).",
)


# -----------------------------------------------------------------------------
# ZPRICE_HISTORY — Custom Z-table example
# -----------------------------------------------------------------------------
ZPRICE_HISTORY_CONTRACT = TableContract(
    sap_system="HANA",
    sap_schema="SAPSR3",
    sap_table="ZPRICE_HISTORY",
    target_postgres_schema="mdm",
    target_postgres_table="price_history",
    target_redshift_schema="mdm",
    target_redshift_table="price_history",
    partition_column="VALID_FROM",
    partition_num_chunks=48,
    primary_keys=["material_number", "valid_from"],
    load_strategy="incremental_merge",
    incremental_column="valid_from",
    enable_cdc=False,
    columns=[
        ColumnMapping(
            source_name="MATNR", target_name="material_number",
            sap_type="CHAR", sap_length=18,
            postgres_type="VARCHAR(18)", redshift_type="VARCHAR(18)",
            nullable=False, is_pk=True,
        ),
        ColumnMapping(
            source_name="VALID_FROM", target_name="valid_from",
            sap_type="TIMESTAMP",
            postgres_type="TIMESTAMP", redshift_type="TIMESTAMP",
            nullable=False, is_pk=True,
        ),
        ColumnMapping(
            source_name="VALID_TO", target_name="valid_to",
            sap_type="TIMESTAMP",
            postgres_type="TIMESTAMP", redshift_type="TIMESTAMP",
        ),
        ColumnMapping(
            source_name="PRICE", target_name="price",
            sap_type="DECIMAL", sap_precision=12, sap_scale=4,
            postgres_type="NUMERIC(12,4)", redshift_type="DECIMAL(12,4)",
        ),
        ColumnMapping(
            source_name="CURRENCY", target_name="currency_code",
            sap_type="CHAR", sap_length=5,
            postgres_type="VARCHAR(5)", redshift_type="VARCHAR(5)",
        ),
        ColumnMapping(
            source_name="PRICE_TYPE", target_name="price_type",
            sap_type="CHAR", sap_length=2,
            postgres_type="VARCHAR(2)", redshift_type="VARCHAR(2)",
            description="LIST=list price, NET=net price, PROMO=promotional",
        ),
    ],
    notes="Custom Z-table for historical pricing. SCD2-like via valid_from/valid_to.",
)


# Global registry — pipelines iterate this
TABLE_CONTRACTS: dict[str, TableContract] = {
    "MARA": MARA_CONTRACT,
    "KNA1": KNA1_CONTRACT,
    "LFA1": LFA1_CONTRACT,
    "ZPRICE_HISTORY": ZPRICE_HISTORY_CONTRACT,
}


def get_contract(sap_table: str) -> TableContract | None:
    """Get a contract by SAP table name."""
    return TABLE_CONTRACTS.get(sap_table)


def contracts_requiring_cdc() -> list[TableContract]:
    """Subset of contracts for which DMS CDC should be enabled."""
    return [c for c in TABLE_CONTRACTS.values() if c.enable_cdc]
