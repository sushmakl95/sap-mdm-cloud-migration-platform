"""Table contracts + SAP type mappings."""

from migration.schemas.contracts import ColumnMapping, SapType, TableContract
from migration.schemas.mdm_contracts import (
    KNA1_CONTRACT,
    LFA1_CONTRACT,
    MARA_CONTRACT,
    TABLE_CONTRACTS,
    ZPRICE_HISTORY_CONTRACT,
    contracts_requiring_cdc,
    get_contract,
)

__all__ = [
    "ColumnMapping",
    "KNA1_CONTRACT",
    "LFA1_CONTRACT",
    "MARA_CONTRACT",
    "SapType",
    "TABLE_CONTRACTS",
    "TableContract",
    "ZPRICE_HISTORY_CONTRACT",
    "contracts_requiring_cdc",
    "get_contract",
]
