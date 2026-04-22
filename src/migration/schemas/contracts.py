"""SAP table contracts and type mappings.

A "contract" describes one SAP source table:
  - Source identity (SAP system, schema, table name, SAP type)
  - Partition strategy (for JDBC partitioned reads)
  - Primary key(s)
  - Target naming (Postgres + Redshift)
  - Column-level type mapping (SAP type -> Postgres + Redshift type)
  - Business rules (SAP domain value mappings, calculated columns)

The extract/transform/load pipeline is driven entirely by contracts -- no
hard-coded table logic. Adding a new table is a matter of defining its contract.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal

# -----------------------------------------------------------------------------
# SAP type system -- HANA + BW native types we commonly encounter
# -----------------------------------------------------------------------------
SapType = Literal[
    # Character types
    "CHAR", "NVARCHAR", "VARCHAR", "STRING", "CLOB", "NCLOB",
    # Numeric types
    "TINYINT", "SMALLINT", "INTEGER", "BIGINT", "DECIMAL", "REAL", "DOUBLE",
    # Date/time
    "DATE", "TIME", "TIMESTAMP", "SECONDDATE",
    # Binary
    "VARBINARY", "BLOB",
    # Boolean (rare in SAP -- usually CHAR(1) with X/'' convention)
    "BOOLEAN",
]


@dataclass
class ColumnMapping:
    """One column's journey from SAP to Postgres + Redshift."""

    source_name: str
    """Column name as-is in SAP (often ALL_CAPS like MATNR, ERSDA)."""

    target_name: str
    """Column name in target (typically snake_case: material_number, created_on)."""

    sap_type: SapType
    sap_length: int | None = None
    sap_precision: int | None = None
    sap_scale: int | None = None

    postgres_type: str = ""
    """Target Postgres type (TEXT, NUMERIC(18,2), TIMESTAMP WITH TIME ZONE, ...)."""

    redshift_type: str = ""
    """Target Redshift type (VARCHAR(255), DECIMAL(18,2), TIMESTAMP, ...)."""

    nullable: bool = True
    is_pk: bool = False
    is_audit: bool = False
    """Audit columns (load_ts, source_system) are added by the pipeline, not sourced."""

    # Business rules
    sap_domain_map: dict[str, str] | None = None
    """If this column is a SAP domain (e.g., XFELD: 'X' -> true, '' -> false), map it here."""

    default_on_null: str | None = None
    """SQL expression used when source value is NULL (e.g., "'UNKNOWN'" or "CURRENT_TIMESTAMP")."""

    description: str = ""


@dataclass
class TableContract:
    """One SAP source table's full migration definition."""

    sap_system: Literal["HANA", "BW", "ECC"]
    sap_schema: str
    sap_table: str

    target_postgres_schema: str
    target_postgres_table: str
    target_redshift_schema: str
    target_redshift_table: str

    # JDBC partitioning -- absolutely critical for reading large tables in parallel
    partition_column: str | None = None
    partition_num_chunks: int = 16

    # Primary keys (composite supported)
    primary_keys: list[str] = field(default_factory=list)

    # Column mappings
    columns: list[ColumnMapping] = field(default_factory=list)

    # Load strategy
    load_strategy: Literal["full_refresh", "incremental_merge", "append_only"] = "full_refresh"
    incremental_column: str | None = None
    """For incremental_merge: timestamp/sequence column that marks "updated since last run"."""

    # Reconciliation tolerance
    row_count_tolerance_pct: float = 0.0
    """Acceptable difference between source and target row counts (0 = strict)."""

    # Business rules
    enable_cdc: bool = True
    """Whether DMS CDC should be enabled for this table."""

    enable_pii_masking: bool = False
    """Apply PII masking (emails, phone numbers, names) before loading to analytical."""

    notes: str = ""

    @property
    def fqn(self) -> str:
        """Fully-qualified source name for logs/metrics."""
        return f"{self.sap_system}:{self.sap_schema}.{self.sap_table}"

    @property
    def target_fqn_postgres(self) -> str:
        return f"{self.target_postgres_schema}.{self.target_postgres_table}"

    @property
    def target_fqn_redshift(self) -> str:
        return f"{self.target_redshift_schema}.{self.target_redshift_table}"
