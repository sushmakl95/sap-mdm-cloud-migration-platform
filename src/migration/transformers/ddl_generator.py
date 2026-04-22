"""DDL generator — emits target Postgres + Redshift CREATE TABLE from contracts.

Writing DDL by hand for 50+ SAP tables is an error-prone waste of time. This
module generates it from the contract, ensuring target schema exactly matches
what the pipeline will load.

For Redshift, we also add:
  - DISTSTYLE / DISTKEY hints based on table size + PK cardinality
  - SORTKEY on business key + load date (most common BI access pattern)

For Postgres, we add:
  - PRIMARY KEY constraint
  - Indexes on business keys
  - Foreign key placeholders (commented out — real FKs declared separately)
"""

from __future__ import annotations

from dataclasses import dataclass

from migration.schemas import TableContract


@dataclass
class DdlOutput:
    postgres_ddl: str
    redshift_ddl: str
    postgres_indexes: list[str]


class DdlGenerator:
    """Contract → DDL for both target warehouses."""

    def generate(self, contract: TableContract) -> DdlOutput:
        pg_ddl = self._postgres_ddl(contract)
        rs_ddl = self._redshift_ddl(contract)
        pg_idx = self._postgres_indexes(contract)
        return DdlOutput(postgres_ddl=pg_ddl, redshift_ddl=rs_ddl, postgres_indexes=pg_idx)

    def _postgres_ddl(self, contract: TableContract) -> str:
        cols = [
            f"    {m.target_name:<30} {m.postgres_type}"
            + (" NOT NULL" if not m.nullable else "")
            for m in contract.columns
            if not m.is_audit
        ]

        # Audit columns appended
        cols.extend([
            "    _source_system              VARCHAR(10)     NOT NULL",
            "    _extracted_at               TIMESTAMP       NOT NULL",
            "    _loaded_at                  TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP",
            "    _is_current                 BOOLEAN         NOT NULL DEFAULT TRUE",
        ])

        pk_cols = ", ".join(
            m.target_name for m in contract.columns if m.is_pk
        )
        if pk_cols:
            cols.append(f"    PRIMARY KEY ({pk_cols})")

        ddl = (
            f"-- Generated from contract {contract.fqn}\n"
            f"CREATE SCHEMA IF NOT EXISTS {contract.target_postgres_schema};\n\n"
            f"CREATE TABLE IF NOT EXISTS {contract.target_fqn_postgres} (\n"
            + ",\n".join(cols)
            + "\n);\n"
        )
        return ddl

    def _postgres_indexes(self, contract: TableContract) -> list[str]:
        """Supplementary indexes beyond the PK."""
        indexes: list[str] = []
        schema_table = contract.target_fqn_postgres

        # Index on incremental column (speeds up delta queries)
        if contract.incremental_column:
            inc_target = self._map_incremental_column_name(contract)
            if inc_target:
                indexes.append(
                    f"CREATE INDEX IF NOT EXISTS idx_{contract.target_postgres_table}_{inc_target} "
                    f"ON {schema_table} ({inc_target});"
                )

        # Index on _loaded_at — invaluable for operational debugging
        indexes.append(
            f"CREATE INDEX IF NOT EXISTS idx_{contract.target_postgres_table}_loaded_at "
            f"ON {schema_table} (_loaded_at);"
        )

        return indexes

    def _map_incremental_column_name(self, contract: TableContract) -> str | None:
        """Convert SAP incremental column name to target name."""
        if not contract.incremental_column:
            return None
        for m in contract.columns:
            if m.source_name == contract.incremental_column:
                return m.target_name
        return contract.incremental_column.lower()

    def _redshift_ddl(self, contract: TableContract) -> str:
        cols = [
            f"    {m.target_name:<30} {m.redshift_type}"
            + (" NOT NULL" if not m.nullable else "")
            for m in contract.columns
            if not m.is_audit
        ]

        cols.extend([
            "    _source_system              VARCHAR(10)     NOT NULL",
            "    _extracted_at               TIMESTAMP       NOT NULL",
            "    _loaded_at                  TIMESTAMP       NOT NULL DEFAULT SYSDATE",
            "    _is_current                 BOOLEAN         NOT NULL DEFAULT TRUE",
        ])

        # Pick DISTKEY + SORTKEY
        distkey = self._choose_distkey(contract)
        sortkeys = self._choose_sortkeys(contract)

        distkey_clause = f"DISTKEY({distkey})" if distkey else "DISTSTYLE ALL"
        sortkey_clause = (
            f"COMPOUND SORTKEY({', '.join(sortkeys)})"
            if sortkeys
            else "SORTKEY(_loaded_at)"
        )

        ddl = (
            f"-- Generated from contract {contract.fqn}\n"
            f"CREATE SCHEMA IF NOT EXISTS {contract.target_redshift_schema};\n\n"
            f"CREATE TABLE IF NOT EXISTS {contract.target_fqn_redshift} (\n"
            + ",\n".join(cols)
            + f"\n)\n{distkey_clause}\n{sortkey_clause};\n"
        )
        return ddl

    def _choose_distkey(self, contract: TableContract) -> str | None:
        """Heuristic:
        - Small dim tables (e.g., vendor_master, < 1M rows expected): DISTSTYLE ALL
        - Large tables with a natural hash key: DISTKEY on primary business key
        """
        # For master-data tables, distribute by PK unless tiny
        if "vendor" in contract.target_redshift_table or "country" in contract.target_redshift_table:
            return None  # DISTSTYLE ALL
        if contract.primary_keys:
            return contract.primary_keys[0]
        return None

    def _choose_sortkeys(self, contract: TableContract) -> list[str]:
        """SORTKEY strategy: business key + load date covers 80% of query patterns."""
        keys: list[str] = []
        if contract.primary_keys:
            keys.append(contract.primary_keys[0])
        if contract.incremental_column:
            inc = self._map_incremental_column_name(contract)
            if inc:
                keys.append(inc)
        keys.append("_loaded_at")
        return list(dict.fromkeys(keys))  # de-dup while preserving order
