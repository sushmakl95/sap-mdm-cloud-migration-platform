"""Step Functions state machine — orchestrates the full migration lifecycle.

Three state machines are defined here:

  1. **migrate-table-sm**: the workhorse — runs extract → validate → load for one table
  2. **batch-migrate-sm**: runs migrate-table-sm for ALL tables in parallel (up to
     configurable concurrency)
  3. **rollback-sm**: point-in-time rollback if a migration phase needs to revert

Each is emitted as ASL (Amazon States Language) JSON ready for Terraform.
"""

from __future__ import annotations

import json
import os
import sys


def migrate_single_table_state_machine(
    glue_extract_job: str,
    glue_load_job: str,
    databricks_reconciliation_job_id: str,
    idempotency_table: str,
    sns_topic_arn: str,
) -> dict:
    """ASL for migrating a single table end-to-end.

    Input shape:
        {"contract_fqn": "HANA:SAPSR3.MARA", "batch_id": "...", "mode": "full"}

    Flow:
        1. CheckIdempotency — skip if already succeeded
        2. RecordStart — DynamoDB put_item
        3. Extract — Glue job (SAP → S3 raw)
        4. Reconcile — Databricks job (source ↔ raw validation)
        5. Load — Glue job (raw → staging → Postgres + Redshift)
        6. PostLoadReconcile — Databricks job (raw ↔ target validation)
        7. RecordSuccess / RecordFailure
    """
    return {
        "Comment": "Migrate a single SAP table to Postgres + Redshift",
        "StartAt": "CheckIdempotency",
        "States": {
            "CheckIdempotency": {
                "Type": "Task",
                "Resource": "arn:aws:states:::dynamodb:getItem",
                "Parameters": {
                    "TableName": idempotency_table,
                    "Key": {
                        "pk": {"S.$": "States.Format('batch#migrate-table#{}', $.batch_id)"}
                    },
                },
                "ResultPath": "$.idempotency_check",
                "Next": "IsAlreadyDone",
            },
            "IsAlreadyDone": {
                "Type": "Choice",
                "Choices": [{
                    "Variable": "$.idempotency_check.Item.status.S",
                    "StringEquals": "SUCCEEDED",
                    "Next": "AlreadyDone",
                }],
                "Default": "RecordStart",
            },
            "AlreadyDone": {
                "Type": "Succeed",
                "Comment": "Batch already ran successfully — no-op",
            },
            "RecordStart": {
                "Type": "Task",
                "Resource": "arn:aws:states:::dynamodb:putItem",
                "Parameters": {
                    "TableName": idempotency_table,
                    "Item": {
                        "pk": {"S.$": "States.Format('batch#migrate-table#{}', $.batch_id)"},
                        "status": {"S": "STARTED"},
                        "started_at": {"S.$": "$$.State.EnteredTime"},
                        "contract_fqn": {"S.$": "$.contract_fqn"},
                    },
                },
                "ResultPath": None,
                "Next": "Extract",
            },
            "Extract": {
                "Type": "Task",
                "Resource": "arn:aws:states:::glue:startJobRun.sync",
                "Parameters": {
                    "JobName": glue_extract_job,
                    "Arguments": {
                        "--contract_fqn.$": "$.contract_fqn",
                        "--batch_id.$": "$.batch_id",
                        "--mode.$": "$.mode",
                    },
                },
                "ResultPath": "$.extract_result",
                "Retry": [{
                    "ErrorEquals": ["States.ALL"],
                    "IntervalSeconds": 30,
                    "MaxAttempts": 2,
                    "BackoffRate": 2.0,
                }],
                "Catch": [{
                    "ErrorEquals": ["States.ALL"],
                    "Next": "RecordFailure",
                    "ResultPath": "$.error",
                }],
                "Next": "ReconcileExtract",
            },
            "ReconcileExtract": {
                "Type": "Task",
                "Resource": "arn:aws:states:::databricks:runJob",
                "Parameters": {
                    "JobId": databricks_reconciliation_job_id,
                    "NotebookParams": {
                        "contract_fqn.$": "$.contract_fqn",
                        "batch_id.$": "$.batch_id",
                        "phase": "post-extract",
                    },
                },
                "ResultPath": "$.extract_reconcile_result",
                "Retry": [{
                    "ErrorEquals": ["States.ALL"],
                    "IntervalSeconds": 60,
                    "MaxAttempts": 1,
                }],
                "Catch": [{
                    "ErrorEquals": ["States.ALL"],
                    "Next": "RecordFailure",
                    "ResultPath": "$.error",
                }],
                "Next": "ValidateExtractStatus",
            },
            "ValidateExtractStatus": {
                "Type": "Choice",
                "Choices": [{
                    "Variable": "$.extract_reconcile_result.overall_status",
                    "StringEquals": "FAILED",
                    "Next": "RecordFailure",
                }],
                "Default": "Load",
            },
            "Load": {
                "Type": "Task",
                "Resource": "arn:aws:states:::glue:startJobRun.sync",
                "Parameters": {
                    "JobName": glue_load_job,
                    "Arguments": {
                        "--contract_fqn.$": "$.contract_fqn",
                        "--batch_id.$": "$.batch_id",
                        "--extract_path.$": "$.extract_result.output_path",
                    },
                },
                "ResultPath": "$.load_result",
                "Retry": [{
                    "ErrorEquals": ["States.ALL"],
                    "IntervalSeconds": 30,
                    "MaxAttempts": 1,
                }],
                "Catch": [{
                    "ErrorEquals": ["States.ALL"],
                    "Next": "RecordFailure",
                    "ResultPath": "$.error",
                }],
                "Next": "PostLoadReconcile",
            },
            "PostLoadReconcile": {
                "Type": "Task",
                "Resource": "arn:aws:states:::databricks:runJob",
                "Parameters": {
                    "JobId": databricks_reconciliation_job_id,
                    "NotebookParams": {
                        "contract_fqn.$": "$.contract_fqn",
                        "batch_id.$": "$.batch_id",
                        "phase": "post-load",
                    },
                },
                "ResultPath": "$.post_load_reconcile_result",
                "Catch": [{
                    "ErrorEquals": ["States.ALL"],
                    "Next": "RecordFailure",
                    "ResultPath": "$.error",
                }],
                "Next": "RecordSuccess",
            },
            "RecordSuccess": {
                "Type": "Task",
                "Resource": "arn:aws:states:::dynamodb:updateItem",
                "Parameters": {
                    "TableName": idempotency_table,
                    "Key": {
                        "pk": {"S.$": "States.Format('batch#migrate-table#{}', $.batch_id)"}
                    },
                    "UpdateExpression": "SET #s = :s, completed_at = :ct",
                    "ExpressionAttributeNames": {"#s": "status"},
                    "ExpressionAttributeValues": {
                        ":s": {"S": "SUCCEEDED"},
                        ":ct": {"S.$": "$$.State.EnteredTime"},
                    },
                },
                "ResultPath": None,
                "Next": "NotifySuccess",
            },
            "NotifySuccess": {
                "Type": "Task",
                "Resource": "arn:aws:states:::sns:publish",
                "Parameters": {
                    "TopicArn": sns_topic_arn,
                    "Subject.$": "States.Format('[SAP Migration] SUCCESS: {}', $.contract_fqn)",
                    "Message.$": "$",
                },
                "End": True,
            },
            "RecordFailure": {
                "Type": "Task",
                "Resource": "arn:aws:states:::dynamodb:updateItem",
                "Parameters": {
                    "TableName": idempotency_table,
                    "Key": {
                        "pk": {"S.$": "States.Format('batch#migrate-table#{}', $.batch_id)"}
                    },
                    "UpdateExpression": "SET #s = :s, completed_at = :ct, error = :err",
                    "ExpressionAttributeNames": {"#s": "status"},
                    "ExpressionAttributeValues": {
                        ":s": {"S": "FAILED"},
                        ":ct": {"S.$": "$$.State.EnteredTime"},
                        ":err": {"S.$": "States.JsonToString($.error)"},
                    },
                },
                "ResultPath": None,
                "Next": "NotifyFailure",
            },
            "NotifyFailure": {
                "Type": "Task",
                "Resource": "arn:aws:states:::sns:publish",
                "Parameters": {
                    "TopicArn": sns_topic_arn,
                    "Subject.$": "States.Format('[SAP Migration] FAILED: {}', $.contract_fqn)",
                    "Message.$": "$",
                },
                "Next": "FailState",
            },
            "FailState": {
                "Type": "Fail",
                "Error": "MigrationFailed",
                "Cause": "Check SNS notification + DynamoDB audit record",
            },
        },
    }


def batch_migrate_state_machine(
    migrate_table_state_machine_arn: str,
    max_concurrency: int = 4,
) -> dict:
    """ASL for migrating many tables in parallel.

    Input:
        {"contracts": ["HANA:SAPSR3.MARA", ...], "batch_id": "...", "mode": "full"}

    Uses a Map state with configurable concurrency — too high overwhelms SAP
    (which has JDBC connection limits), too low wastes time.
    """
    return {
        "Comment": "Migrate many SAP tables in parallel",
        "StartAt": "ParallelMigrate",
        "States": {
            "ParallelMigrate": {
                "Type": "Map",
                "MaxConcurrency": max_concurrency,
                "ItemsPath": "$.contracts",
                "ItemSelector": {
                    "contract_fqn.$": "$$.Map.Item.Value",
                    "batch_id.$": "States.Format('{}-{}', $.batch_id, $$.Map.Item.Index)",
                    "mode.$": "$.mode",
                },
                "ItemProcessor": {
                    "ProcessorConfig": {"Mode": "INLINE"},
                    "StartAt": "InvokeMigrateTable",
                    "States": {
                        "InvokeMigrateTable": {
                            "Type": "Task",
                            "Resource": "arn:aws:states:::states:startExecution.sync:2",
                            "Parameters": {
                                "StateMachineArn": migrate_table_state_machine_arn,
                                "Input.$": "$",
                            },
                            "End": True,
                        },
                    },
                },
                "End": True,
            },
        },
    }


def rollback_state_machine(
    idempotency_table: str,
    rollback_glue_job: str,
    sns_topic_arn: str,
) -> dict:
    """ASL for rolling back a migration batch.

    Input:
        {"batch_id": "...", "rollback_strategy": "restore_snapshot" | "delete_by_batch"}
    """
    return {
        "Comment": "Rollback a failed or problematic migration batch",
        "StartAt": "ConfirmRollbackRequested",
        "States": {
            "ConfirmRollbackRequested": {
                "Type": "Task",
                "Resource": "arn:aws:states:::dynamodb:getItem",
                "Parameters": {
                    "TableName": idempotency_table,
                    "Key": {
                        "pk": {"S.$": "States.Format('batch#migrate-table#{}', $.batch_id)"}
                    },
                },
                "ResultPath": "$.batch_record",
                "Next": "ExecuteRollback",
            },
            "ExecuteRollback": {
                "Type": "Task",
                "Resource": "arn:aws:states:::glue:startJobRun.sync",
                "Parameters": {
                    "JobName": rollback_glue_job,
                    "Arguments": {
                        "--batch_id.$": "$.batch_id",
                        "--strategy.$": "$.rollback_strategy",
                    },
                },
                "ResultPath": "$.rollback_result",
                "Next": "NotifyRollback",
            },
            "NotifyRollback": {
                "Type": "Task",
                "Resource": "arn:aws:states:::sns:publish",
                "Parameters": {
                    "TopicArn": sns_topic_arn,
                    "Subject.$": "States.Format('[SAP Migration] ROLLED BACK: {}', $.batch_id)",
                    "Message.$": "$",
                },
                "End": True,
            },
        },
    }


def emit_all_state_machines(output_dir: str) -> None:
    """Emit all three state machine definitions as JSON files (used by Terraform).

    NOTE: Terraform's `templatefile()` function treats `$$` as an escape sequence
    that outputs a literal `$`. Step Functions uses `$$.Foo` to reference the
    context object (e.g., `$$.State.EnteredTime`). Without escaping, templatefile
    would turn `$$.State.EnteredTime` into `$.State.EnteredTime` which is wrong.

    We emit `$$$$` in the JSON files so templatefile produces correct `$$`.
    """
    os.makedirs(output_dir, exist_ok=True)

    # Use placeholder values -- Terraform will substitute
    placeholders = {
        "glue_extract_job": "${glue_extract_job}",
        "glue_load_job": "${glue_load_job}",
        "databricks_reconciliation_job_id": "${databricks_reconciliation_job_id}",
        "idempotency_table": "${idempotency_table}",
        "sns_topic_arn": "${sns_topic_arn}",
        "rollback_glue_job": "${rollback_glue_job}",
        "migrate_table_state_machine_arn": "${migrate_table_state_machine_arn}",
    }

    def _escape_templatefile_literals(s: str) -> str:
        """Escape $$ to $$$$ so templatefile preserves them as $$."""
        return s.replace("$$", "$$$$")

    migrate_table_json = json.dumps(
        migrate_single_table_state_machine(
            placeholders["glue_extract_job"],
            placeholders["glue_load_job"],
            placeholders["databricks_reconciliation_job_id"],
            placeholders["idempotency_table"],
            placeholders["sns_topic_arn"],
        ),
        indent=2,
    )
    with open(f"{output_dir}/migrate_table.asl.json", "w") as f:
        f.write(_escape_templatefile_literals(migrate_table_json))

    batch_migrate_json = json.dumps(
        batch_migrate_state_machine(
            placeholders["migrate_table_state_machine_arn"]
        ),
        indent=2,
    )
    with open(f"{output_dir}/batch_migrate.asl.json", "w") as f:
        f.write(_escape_templatefile_literals(batch_migrate_json))

    rollback_json = json.dumps(
        rollback_state_machine(
            placeholders["idempotency_table"],
            placeholders["rollback_glue_job"],
            placeholders["sns_topic_arn"],
        ),
        indent=2,
    )
    with open(f"{output_dir}/rollback.asl.json", "w") as f:
        f.write(_escape_templatefile_literals(rollback_json))


if __name__ == "__main__":
    output = sys.argv[1] if len(sys.argv) > 1 else "./stepfunctions"
    emit_all_state_machines(output)
    print(f"State machines emitted to {output}/")
