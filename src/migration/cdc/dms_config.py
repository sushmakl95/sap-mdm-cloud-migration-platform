"""AWS DMS replication task configuration generator.

DMS handles the real-time CDC path from SAP HANA to S3 during the cutover
window. We generate task configurations programmatically from our contracts
to ensure they stay in sync with the batch pipeline.

DMS concepts used:
  - Source endpoint: SAP HANA (using DMS's native HANA connector)
  - Target endpoint: S3 (writes CDC events as Parquet with full before/after images)
  - Replication task: Full Load + CDC mode
  - Table mappings: include-list driven by our contract registry
  - Transformation rules: SAP uppercase → lowercase column names
"""

from __future__ import annotations

import json
from dataclasses import dataclass

from migration.schemas import TableContract, contracts_requiring_cdc


@dataclass
class DmsTaskConfig:
    """Complete DMS task spec: table mappings + replication settings."""

    task_identifier: str
    source_endpoint_arn: str
    target_endpoint_arn: str
    replication_instance_arn: str
    table_mappings: str  # JSON string
    replication_task_settings: str  # JSON string
    migration_type: str = "full-load-and-cdc"


class DmsConfigGenerator:
    """Generate DMS configs from our contract registry."""

    def generate_table_mappings(
        self, contracts: list[TableContract] | None = None
    ) -> dict:
        """Build the table-mappings document listing which SAP tables to replicate."""
        contracts = contracts or contracts_requiring_cdc()

        rules = []
        rule_id = 1

        # Include-selection rules
        for contract in contracts:
            rules.append({
                "rule-type": "selection",
                "rule-id": str(rule_id),
                "rule-name": f"include-{contract.sap_table.lower()}",
                "object-locator": {
                    "schema-name": contract.sap_schema,
                    "table-name": contract.sap_table,
                },
                "rule-action": "include",
                "filters": [],
            })
            rule_id += 1

        # Transformation rules: SAP ALLCAPS → snake_case for target
        for contract in contracts:
            # Table-level rename
            rules.append({
                "rule-type": "transformation",
                "rule-id": str(rule_id),
                "rule-name": f"rename-table-{contract.sap_table.lower()}",
                "rule-target": "table",
                "object-locator": {
                    "schema-name": contract.sap_schema,
                    "table-name": contract.sap_table,
                },
                "rule-action": "rename",
                "value": contract.target_postgres_table,
            })
            rule_id += 1

            # Schema rename
            rules.append({
                "rule-type": "transformation",
                "rule-id": str(rule_id),
                "rule-name": f"rename-schema-{contract.sap_table.lower()}",
                "rule-target": "schema",
                "object-locator": {
                    "schema-name": contract.sap_schema,
                    "table-name": contract.sap_table,
                },
                "rule-action": "rename",
                "value": contract.target_postgres_schema,
            })
            rule_id += 1

            # Column-level renames
            for column in contract.columns:
                if column.is_audit or column.source_name == column.target_name:
                    continue
                rules.append({
                    "rule-type": "transformation",
                    "rule-id": str(rule_id),
                    "rule-name": f"rename-{contract.sap_table}-{column.source_name}",
                    "rule-target": "column",
                    "object-locator": {
                        "schema-name": contract.sap_schema,
                        "table-name": contract.sap_table,
                        "column-name": column.source_name,
                    },
                    "rule-action": "rename",
                    "value": column.target_name,
                })
                rule_id += 1

        return {"rules": rules}

    def generate_task_settings(self) -> dict:
        """Replication task tuning — these are the knobs that matter."""
        return {
            "TargetMetadata": {
                "TargetSchema": "",
                "SupportLobs": True,
                "FullLobMode": False,
                "LobChunkSize": 64,
                "LimitedSizeLobMode": True,
                "LobMaxSize": 32,
                "InlineLobMaxSize": 0,
                "LoadMaxFileSize": 0,
                "ParallelLoadThreads": 8,
                "ParallelLoadBufferSize": 0,
                "BatchApplyEnabled": True,
                "TaskRecoveryTableEnabled": True,
            },
            "FullLoadSettings": {
                "TargetTablePrepMode": "TRUNCATE_BEFORE_LOAD",
                "CreatePkAfterFullLoad": False,
                "StopTaskCachedChangesApplied": False,
                "StopTaskCachedChangesNotApplied": False,
                "MaxFullLoadSubTasks": 8,
                "TransactionConsistencyTimeout": 600,
                "CommitRate": 50000,
            },
            "Logging": {
                "EnableLogging": True,
                "LogComponents": [
                    {"Id": "SOURCE_CAPTURE", "Severity": "LOGGER_SEVERITY_DEFAULT"},
                    {"Id": "SOURCE_UNLOAD", "Severity": "LOGGER_SEVERITY_DEFAULT"},
                    {"Id": "TARGET_LOAD", "Severity": "LOGGER_SEVERITY_DEFAULT"},
                    {"Id": "TARGET_APPLY", "Severity": "LOGGER_SEVERITY_DEFAULT"},
                ],
            },
            "ChangeProcessingTuning": {
                "BatchApplyPreserveTransaction": True,
                "BatchApplyTimeoutMin": 1,
                "BatchApplyTimeoutMax": 30,
                "BatchApplyMemoryLimit": 500,
                "MinTransactionSize": 1000,
                "CommitTimeout": 1,
                "MemoryLimitTotal": 1024,
                "MemoryKeepTime": 60,
                "StatementCacheSize": 50,
            },
            "ValidationSettings": {
                "EnableValidation": True,
                "ValidationMode": "ROW_LEVEL",
                "ThreadCount": 5,
                "PartitionSize": 10000,
                "FailureMaxCount": 10000,
                "TableFailureMaxCount": 1000,
                "HandleCollationDiff": True,
                "RecordFailureDelayLimitInMinutes": 0,
            },
            "ErrorBehavior": {
                "DataErrorPolicy": "LOG_ERROR",
                "DataTruncationErrorPolicy": "LOG_ERROR",
                "DataErrorEscalationPolicy": "SUSPEND_TABLE",
                "DataErrorEscalationCount": 0,
                "TableErrorPolicy": "SUSPEND_TABLE",
                "TableErrorEscalationPolicy": "STOP_TASK",
                "TableErrorEscalationCount": 0,
                "RecoverableErrorCount": -1,
                "RecoverableErrorInterval": 5,
                "RecoverableErrorThrottling": True,
                "RecoverableErrorThrottlingMax": 1800,
                "ApplyErrorDeletePolicy": "IGNORE_RECORD",
                "ApplyErrorInsertPolicy": "LOG_ERROR",
                "ApplyErrorUpdatePolicy": "LOG_ERROR",
                "ApplyErrorEscalationPolicy": "LOG_ERROR",
                "ApplyErrorEscalationCount": 0,
                "ApplyErrorFailOnTruncationDdl": False,
                "FullLoadIgnoreConflicts": True,
                "FailOnTransactionConsistencyBreached": False,
                "FailOnNoTablesCaptured": True,
            },
        }

    def build_config(
        self,
        task_identifier: str,
        source_endpoint_arn: str,
        target_endpoint_arn: str,
        replication_instance_arn: str,
        contracts: list[TableContract] | None = None,
    ) -> DmsTaskConfig:
        return DmsTaskConfig(
            task_identifier=task_identifier,
            source_endpoint_arn=source_endpoint_arn,
            target_endpoint_arn=target_endpoint_arn,
            replication_instance_arn=replication_instance_arn,
            table_mappings=json.dumps(self.generate_table_mappings(contracts)),
            replication_task_settings=json.dumps(self.generate_task_settings()),
        )
