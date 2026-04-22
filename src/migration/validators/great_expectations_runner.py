"""Great Expectations integration for standardized DQ rules.

We use GE alongside our 5-layer reconciliation:
  - Reconciliation validates source ↔ target PARITY (structural equivalence)
  - GE validates TARGET-SIDE BUSINESS RULES (e.g., "status must be in {...}",
    "price must be non-negative", "email must be valid format")

Two layers of DQ complement each other: reconciliation catches migration bugs,
GE catches pre-existing source data quality issues that shouldn't flow through.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field

from pyspark.sql import DataFrame

from migration.schemas import TableContract
from migration.utils.logging_config import get_logger

log = get_logger(__name__, component="ge_validator")


@dataclass
class ExpectationResult:
    """One Great Expectations test result."""

    expectation_name: str
    column: str | None
    success: bool
    observed_value: dict | None = None
    unexpected_count: int = 0


@dataclass
class ExpectationSuite:
    """A collection of expectations for one table."""

    suite_name: str
    table_contract_fqn: str
    expectations: list[ExpectationResult] = field(default_factory=list)

    @property
    def success(self) -> bool:
        return all(e.success for e in self.expectations)

    def to_dict(self) -> dict:
        return {
            "suite_name": self.suite_name,
            "table": self.table_contract_fqn,
            "success": self.success,
            "expectations": [
                {
                    "name": e.expectation_name,
                    "column": e.column,
                    "success": e.success,
                    "observed": e.observed_value,
                    "unexpected_count": e.unexpected_count,
                }
                for e in self.expectations
            ],
        }


class GreatExpectationsRunner:
    """Runs GE-style expectations against the target DataFrame.

    We use a lightweight shim (instead of pulling full GE with all its deps)
    to keep pipelines fast. The expectation semantics are identical — if we
    ever migrate to full GE in the future, these translate 1:1.
    """

    def build_suite_for_contract(self, contract: TableContract) -> list[dict]:
        """Derive an expectation suite from contract metadata.

        Auto-generates:
          - expect_column_values_to_not_be_null for non-nullable columns
          - expect_column_values_to_be_unique for PK columns
          - expect_column_values_to_match_regex for email columns
          - expect_column_values_to_be_in_set for SAP domain columns
        """
        expectations: list[dict] = []

        for m in contract.columns:
            if m.is_audit:
                continue

            if not m.nullable:
                expectations.append({
                    "name": "expect_column_values_to_not_be_null",
                    "column": m.target_name,
                })

            if m.is_pk:
                expectations.append({
                    "name": "expect_column_values_to_be_unique",
                    "column": m.target_name,
                })

            if "email" in m.target_name:
                expectations.append({
                    "name": "expect_column_values_to_match_regex",
                    "column": m.target_name,
                    "regex": r"^[\w\.-]+@[\w\.-]+\.\w+$",
                    "allow_null": True,
                })

            if m.sap_domain_map:
                expectations.append({
                    "name": "expect_column_values_to_be_in_set",
                    "column": m.target_name,
                    "value_set": list(m.sap_domain_map.values()) + ["true", "false"],
                    "allow_null": True,
                })

        return expectations

    def run(self, df: DataFrame, contract: TableContract) -> ExpectationSuite:
        """Execute the auto-generated suite on the target DataFrame."""
        suite = ExpectationSuite(
            suite_name=f"{contract.target_redshift_table}_suite",
            table_contract_fqn=contract.fqn,
        )

        expectations = self.build_suite_for_contract(contract)
        for e in expectations:
            result = self._run_expectation(df, e)
            suite.expectations.append(result)

        log.info(
            "ge_suite_complete",
            contract=contract.fqn,
            total=len(expectations),
            passed=sum(1 for r in suite.expectations if r.success),
        )
        return suite

    def _run_expectation(self, df: DataFrame, spec: dict) -> ExpectationResult:
        name = spec["name"]
        column = spec.get("column")

        if name == "expect_column_values_to_not_be_null":
            null_count = df.filter(df[column].isNull()).count()
            return ExpectationResult(
                expectation_name=name,
                column=column,
                success=null_count == 0,
                observed_value={"null_count": null_count},
                unexpected_count=null_count,
            )

        if name == "expect_column_values_to_be_unique":
            total = df.count()
            distinct = df.select(column).distinct().count()
            dup_count = total - distinct
            return ExpectationResult(
                expectation_name=name,
                column=column,
                success=dup_count == 0,
                observed_value={"total": total, "distinct": distinct},
                unexpected_count=dup_count,
            )

        if name == "expect_column_values_to_match_regex":
            regex = spec["regex"]
            allow_null = spec.get("allow_null", True)
            check = df[column].rlike(regex)
            if allow_null:
                check = df[column].isNull() | check
            invalid_count = df.filter(~check).count()
            return ExpectationResult(
                expectation_name=name,
                column=column,
                success=invalid_count == 0,
                observed_value={"invalid_count": invalid_count},
                unexpected_count=invalid_count,
            )

        if name == "expect_column_values_to_be_in_set":
            value_set = spec["value_set"]
            allow_null = spec.get("allow_null", True)
            check = df[column].isin([str(v) for v in value_set])
            if allow_null:
                check = df[column].isNull() | check
            invalid_count = df.filter(~check).count()
            return ExpectationResult(
                expectation_name=name,
                column=column,
                success=invalid_count == 0,
                observed_value={"invalid_count": invalid_count, "allowed": value_set},
                unexpected_count=invalid_count,
            )

        # Unknown expectation
        return ExpectationResult(
            expectation_name=name,
            column=column,
            success=False,
            observed_value={"error": "unknown_expectation_name"},
        )

    def to_json(self, suite: ExpectationSuite) -> str:
        return json.dumps(suite.to_dict(), indent=2, default=str)
