variable "name_prefix" { type = string }
variable "batch_sm_arn" { type = string }
variable "sfn_notifier_lambda_arn" { type = string }
variable "schedule_expression" { type = string }

data "aws_caller_identity" "current" {}
data "aws_region" "current" {}

# -----------------------------------------------------------------------------
# Nightly batch migration schedule
# -----------------------------------------------------------------------------
resource "aws_scheduler_schedule_group" "migration" {
  name = "${var.name_prefix}-schedules"
}

resource "aws_iam_role" "scheduler" {
  name = "${var.name_prefix}-scheduler-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "scheduler.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "scheduler_sfn" {
  name = "${var.name_prefix}-scheduler-sfn"
  role = aws_iam_role.scheduler.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["states:StartExecution"]
      Resource = var.batch_sm_arn
    }]
  })
}

resource "aws_scheduler_schedule" "nightly_migration" {
  name       = "${var.name_prefix}-nightly-migration"
  group_name = aws_scheduler_schedule_group.migration.name

  flexible_time_window {
    mode                      = "FLEXIBLE"
    maximum_window_in_minutes = 15
  }

  schedule_expression          = var.schedule_expression
  schedule_expression_timezone = "UTC"

  target {
    arn      = var.batch_sm_arn
    role_arn = aws_iam_role.scheduler.arn

    input = jsonencode({
      contracts = ["HANA:SAPSR3.MARA", "HANA:SAPSR3.KNA1", "HANA:SAPSR3.LFA1", "HANA:SAPSR3.ZPRICE_HISTORY"]
      mode      = "incremental"
      batch_id  = "nightly-<aws.scheduler.scheduled-time>"
    })

    retry_policy {
      maximum_retry_attempts       = 2
      maximum_event_age_in_seconds = 3600
    }
  }
}

# -----------------------------------------------------------------------------
# SFN state-change event rule → notifier Lambda
# -----------------------------------------------------------------------------
resource "aws_cloudwatch_event_rule" "sfn_state_change" {
  name        = "${var.name_prefix}-sfn-state-change"
  description = "Capture all SFN state changes for SAP migration state machines"
  event_pattern = jsonencode({
    source      = ["aws.states"]
    detail-type = ["Step Functions Execution Status Change"]
    detail = {
      status          = ["SUCCEEDED", "FAILED", "TIMED_OUT", "ABORTED"]
      stateMachineArn = [{ prefix = "arn:aws:states:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:stateMachine:${var.name_prefix}" }]
    }
  })
}

resource "aws_cloudwatch_event_target" "sfn_notifier" {
  rule      = aws_cloudwatch_event_rule.sfn_state_change.name
  target_id = "SfnNotifierLambda"
  arn       = var.sfn_notifier_lambda_arn
}

resource "aws_lambda_permission" "allow_eventbridge_notifier" {
  statement_id  = "AllowEventBridgeInvoke"
  action        = "lambda:InvokeFunction"
  function_name = var.sfn_notifier_lambda_arn
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.sfn_state_change.arn
}

output "nightly_schedule_arn" { value = aws_scheduler_schedule.nightly_migration.arn }
output "sfn_event_rule_arn" { value = aws_cloudwatch_event_rule.sfn_state_change.arn }
