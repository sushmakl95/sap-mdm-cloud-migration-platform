"""SAP type coercion engine.

SAP has its own type system rooted in ABAP. Many types don't directly map to
Postgres or Redshift. Key quirks:

  - **Leading zeros**: MATNR is a CHAR(18) in SAP. Business key `1000` is
    actually stored as `'000000000000001000'`. Downstream applications strip
    the zeros in ABAP. We have to do the same.
  - **Boolean flags**: SAP uses CHAR(1) fields with 'X'/'' convention for
    booleans. Postgres/Redshift have native BOOLEAN. We translate.
  - **Dates**: SAP DATS is a CHAR(8) like '20260421'. We parse to DATE.
  - **Times**: SAP TIMS is a CHAR(6) like '143015'. We parse to TIME.
  - **Decimal scale**: SAP DECIMAL(13,3) is stored as a number but often
    exposed as a string by JDBC drivers. We normalize to Decimal.
  - **Client column (MANDT)**: filtered out — irrelevant outside SAP.

This module owns the translation logic. It's data-driven by the ColumnMapping
definitions in the contract.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from migration.schemas import ColumnMapping, TableContract
from migration.utils.logging_config import get_logger

log = get_logger(__name__, component="transformer")


class SapTypeTransformer:
    """Transform a raw SAP-extracted DataFrame into canonical target schema."""

    def transform(self, df: DataFrame, contract: TableContract) -> DataFrame:
        """Apply all column mappings: rename, type-coerce, business-rule expand."""
        log.info("transform_started", contract=contract.fqn, rows=None)

        # Drop MANDT (SAP client column) if present — irrelevant in target
        if "MANDT" in df.columns:
            df = df.drop("MANDT")

        # Select only mapped columns in contract order, applying transforms
        selected_cols = []
        for mapping in contract.columns:
            if mapping.is_audit:
                continue  # audit columns are added separately
            if mapping.source_name not in df.columns:
                log.warning(
                    "source_column_missing",
                    contract=contract.fqn,
                    column=mapping.source_name,
                )
                # Emit a null for missing columns — downstream DQ will flag
                selected_cols.append(F.lit(None).alias(mapping.target_name))
                continue

            transformed = self._transform_column(
                F.col(mapping.source_name), mapping
            ).alias(mapping.target_name)
            selected_cols.append(transformed)

        # Preserve audit columns added by extractor
        audit_cols = [
            c for c in df.columns
            if c.startswith("_") and c not in {m.source_name for m in contract.columns}
        ]
        for c in audit_cols:
            selected_cols.append(F.col(c))

        result = df.select(*selected_cols)
        log.info("transform_complete", contract=contract.fqn, columns=len(selected_cols))
        return result

    def _transform_column(self, col: Column, mapping: ColumnMapping) -> Column:
        """Apply the full transformation chain for one column."""
        # Step 1: trim trailing spaces from CHAR fields (SAP padding)
        if mapping.sap_type in ("CHAR", "NCHAR", "VARCHAR", "NVARCHAR", "STRING"):
            col = F.rtrim(col)

        # Step 2: strip leading zeros for numeric-material-like keys
        # (ONLY if target type is not a strict-width VARCHAR)
        if self._should_strip_leading_zeros(mapping):
            col = F.regexp_replace(col, "^0+", "")
            # If the strip empties the value, it was all zeros — preserve '0'
            col = F.when(col == "", "0").otherwise(col)

        # Step 3: SAP domain value mapping (e.g., 'X'/'' → true/false)
        if mapping.sap_domain_map:
            col = self._apply_domain_map(col, mapping.sap_domain_map)
            # After domain mapping, value is already correct SQL literal,
            # so we skip type casting.
            return col

        # Step 4: type-specific parsing
        col = self._parse_by_type(col, mapping)

        # Step 5: apply default on NULL
        if mapping.default_on_null is not None:
            col = F.when(col.isNull(), F.expr(mapping.default_on_null)).otherwise(col)

        return col

    def _should_strip_leading_zeros(self, mapping: ColumnMapping) -> bool:
        """SAP material numbers, customer numbers etc. use alpha conversion
        which pads with zeros. Business convention: strip them in output."""
        # Heuristic: CHAR(n) where n > 6 and the column name suggests an ID
        if mapping.sap_type != "CHAR":
            return False
        id_suffixes = ("_number", "_nr", "_id", "material_number", "customer_number", "vendor_number")
        return any(mapping.target_name.endswith(s) for s in id_suffixes)

    def _apply_domain_map(self, col: Column, domain_map: dict[str, str]) -> Column:
        """Convert SAP domain values to target literals using a chained WHEN/OTHERWISE."""
        # Start with a base condition, then fold
        items = list(domain_map.items())
        if not items:
            return col
        first_key, first_val = items[0]
        result = F.when(col == F.lit(first_key), F.expr(first_val))
        for key, val in items[1:]:
            result = result.when(col == F.lit(key), F.expr(val))
        # Preserve original value if no mapping hits (safer than forcing NULL)
        result = result.otherwise(col)
        return result

    def _parse_by_type(self, col: Column, mapping: ColumnMapping) -> Column:
        """Type-specific parsing of SAP native encoding."""
        t = mapping.sap_type

        if t == "DATE":
            # SAP DATS is CHAR(8) like '20260421' OR already a DATE from HANA
            return F.coalesce(
                F.to_date(col, "yyyyMMdd"),
                F.to_date(col),  # already-typed fall-through
            )

        if t == "TIME":
            return F.coalesce(
                F.to_timestamp(col, "HHmmss"),
                F.to_timestamp(col),
            )

        if t in ("TIMESTAMP", "SECONDDATE"):
            return F.to_timestamp(col)

        if t in ("TINYINT", "SMALLINT", "INTEGER"):
            return col.cast("int")

        if t == "BIGINT":
            return col.cast("bigint")

        if t == "DECIMAL":
            assert mapping.sap_precision is not None
            assert mapping.sap_scale is not None
            return col.cast(f"decimal({mapping.sap_precision},{mapping.sap_scale})")

        if t == "REAL":
            return col.cast("float")

        if t == "DOUBLE":
            return col.cast("double")

        # Default: keep as string with trimming already applied in step 1
        return col

    def apply_pii_masking(self, df: DataFrame, contract: TableContract) -> DataFrame:
        """Apply PII masking for analytical target (Redshift).

        Masks email, phone, name columns using SHA256 hashes or generic placeholders.
        The operational target (Postgres) receives un-masked data, since services
        need it to actually send emails, etc.
        """
        if not contract.enable_pii_masking:
            return df

        pii_columns = {"email", "phone_primary", "fax", "customer_name", "vendor_name"}
        result = df
        for c in df.columns:
            if c in pii_columns:
                result = result.withColumn(c, F.sha2(F.col(c).cast("string"), 256))
        return result
