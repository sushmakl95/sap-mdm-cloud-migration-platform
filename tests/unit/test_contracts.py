"""Tests for contract definitions + registry."""

from __future__ import annotations

import pytest

from migration.schemas import (
    KNA1_CONTRACT,
    LFA1_CONTRACT,
    MARA_CONTRACT,
    TABLE_CONTRACTS,
    contracts_requiring_cdc,
    get_contract,
)

pytestmark = pytest.mark.unit


def test_all_contracts_have_primary_keys():
    for name, contract in TABLE_CONTRACTS.items():
        assert contract.primary_keys, f"{name} has no primary keys"


def test_all_contracts_have_partition_column():
    for name, contract in TABLE_CONTRACTS.items():
        assert contract.partition_column, f"{name} has no partition column"


def test_all_contracts_map_primary_keys_to_columns():
    """Every PK must correspond to a declared column with is_pk=True."""
    for name, contract in TABLE_CONTRACTS.items():
        pk_col_names = {m.target_name for m in contract.columns if m.is_pk}
        assert set(contract.primary_keys) <= pk_col_names, (
            f"{name}: primary_keys={contract.primary_keys} not reflected "
            f"in column is_pk flags"
        )


def test_target_table_names_are_snake_case():
    for name, contract in TABLE_CONTRACTS.items():
        assert contract.target_postgres_table.islower()
        assert contract.target_redshift_table.islower()
        assert "_" in contract.target_postgres_table or contract.target_postgres_table.isalpha()


def test_get_contract_returns_none_for_unknown():
    assert get_contract("NONEXISTENT") is None


def test_get_contract_returns_expected():
    assert get_contract("MARA") == MARA_CONTRACT
    assert get_contract("KNA1") == KNA1_CONTRACT
    assert get_contract("LFA1") == LFA1_CONTRACT


def test_contracts_requiring_cdc_returns_only_cdc_enabled():
    cdc_contracts = contracts_requiring_cdc()
    for c in cdc_contracts:
        assert c.enable_cdc is True


def test_mara_has_expected_columns():
    assert any(m.target_name == "material_number" for m in MARA_CONTRACT.columns)
    assert any(m.target_name == "material_type" for m in MARA_CONTRACT.columns)


def test_column_type_mappings_are_consistent():
    """Every column must declare postgres_type + redshift_type."""
    for name, contract in TABLE_CONTRACTS.items():
        for m in contract.columns:
            if m.is_audit:
                continue
            assert m.postgres_type, f"{name}.{m.target_name}: missing postgres_type"
            assert m.redshift_type, f"{name}.{m.target_name}: missing redshift_type"


def test_fqn_format():
    assert MARA_CONTRACT.fqn == "HANA:SAPSR3.MARA"
    assert MARA_CONTRACT.target_fqn_postgres == "mdm.material_master"
    assert MARA_CONTRACT.target_fqn_redshift == "mdm.material_master"
