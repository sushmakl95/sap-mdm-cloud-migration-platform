variable "name_prefix" { type = string }
variable "sfn_role_arn" { type = string }
variable "glue_extract_job" { type = string }
variable "glue_load_job" { type = string }
variable "idempotency_table" { type = string }
variable "sns_topic_arn" { type = string }
variable "databricks_job_id" { type = string }
variable "rollback_glue_job" { type = string }

# CloudWatch log group for SFN executions
resource "aws_cloudwatch_log_group" "sfn" {
  name              = "/aws/vendedlogs/states/${var.name_prefix}"
  retention_in_days = 30
}

# -----------------------------------------------------------------------------
# migrate-table-sm — one-table end-to-end
# -----------------------------------------------------------------------------
resource "aws_sfn_state_machine" "migrate_table" {
  name     = "${var.name_prefix}-migrate-table"
  role_arn = var.sfn_role_arn

  definition = templatefile("${path.module}/../../../../src/migration/orchestration/migrate_table.asl.json", {
    glue_extract_job                 = var.glue_extract_job
    glue_load_job                    = var.glue_load_job
    databricks_reconciliation_job_id = var.databricks_job_id
    idempotency_table                = var.idempotency_table
    sns_topic_arn                    = var.sns_topic_arn
  })

  logging_configuration {
    log_destination        = "${aws_cloudwatch_log_group.sfn.arn}:*"
    include_execution_data = true
    level                  = "ERROR"
  }

  tracing_configuration {
    enabled = true
  }
}

# -----------------------------------------------------------------------------
# batch-migrate-sm — orchestrates many tables in parallel
# -----------------------------------------------------------------------------
resource "aws_sfn_state_machine" "batch_migrate" {
  name     = "${var.name_prefix}-batch-migrate"
  role_arn = var.sfn_role_arn

  definition = templatefile("${path.module}/../../../../src/migration/orchestration/batch_migrate.asl.json", {
    migrate_table_state_machine_arn = aws_sfn_state_machine.migrate_table.arn
  })

  logging_configuration {
    log_destination        = "${aws_cloudwatch_log_group.sfn.arn}:*"
    include_execution_data = true
    level                  = "ERROR"
  }
}

# -----------------------------------------------------------------------------
# rollback-sm
# -----------------------------------------------------------------------------
resource "aws_sfn_state_machine" "rollback" {
  name     = "${var.name_prefix}-rollback"
  role_arn = var.sfn_role_arn

  definition = templatefile("${path.module}/../../../../src/migration/orchestration/rollback.asl.json", {
    idempotency_table = var.idempotency_table
    rollback_glue_job = var.rollback_glue_job
    sns_topic_arn     = var.sns_topic_arn
  })
}

output "migrate_table_sm_arn" { value = aws_sfn_state_machine.migrate_table.arn }
output "batch_migrate_sm_arn" { value = aws_sfn_state_machine.batch_migrate.arn }
output "rollback_sm_arn" { value = aws_sfn_state_machine.rollback.arn }
