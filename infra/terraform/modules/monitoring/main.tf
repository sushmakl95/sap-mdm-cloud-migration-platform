variable "name_prefix" { type = string }
variable "kms_key_arn" { type = string }
variable "log_retention_days" { type = number }
variable "alarm_email_recipients" { type = list(string) }

# -----------------------------------------------------------------------------
# DynamoDB — idempotency + watermarks + audit
# -----------------------------------------------------------------------------
resource "aws_dynamodb_table" "audit" {
  name         = "${var.name_prefix}-audit"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "pk"

  attribute {
    name = "pk"
    type = "S"
  }

  ttl {
    attribute_name = "ttl"
    enabled        = true
  }

  server_side_encryption {
    enabled     = true
    kms_key_arn = var.kms_key_arn
  }

  point_in_time_recovery {
    enabled = true
  }

  tags = {
    Name = "${var.name_prefix}-audit"
    Purpose = "migration-idempotency-and-watermarks"
  }
}

# -----------------------------------------------------------------------------
# SNS topic for notifications (Slack webhook or email subscribers)
# -----------------------------------------------------------------------------
resource "aws_sns_topic" "notifications" {
  name              = "${var.name_prefix}-notifications"
  kms_master_key_id = "alias/aws/sns"
}

resource "aws_sns_topic_subscription" "email" {
  for_each  = toset(var.alarm_email_recipients)
  topic_arn = aws_sns_topic.notifications.arn
  protocol  = "email"
  endpoint  = each.value
}

# -----------------------------------------------------------------------------
# CloudWatch log groups
# -----------------------------------------------------------------------------
resource "aws_cloudwatch_log_group" "glue_jobs" {
  name              = "/aws-glue/jobs/${var.name_prefix}"
  retention_in_days = var.log_retention_days
}

resource "aws_cloudwatch_log_group" "dms_tasks" {
  name              = "dms-tasks-${var.name_prefix}"
  retention_in_days = var.log_retention_days
}

resource "aws_cloudwatch_log_group" "lambda" {
  name              = "/aws/lambda/${var.name_prefix}"
  retention_in_days = var.log_retention_days
}

# -----------------------------------------------------------------------------
# Alarms — the ones that matter most
# -----------------------------------------------------------------------------
resource "aws_cloudwatch_metric_alarm" "migration_failure" {
  alarm_name          = "${var.name_prefix}-migration-failure"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "JobFailed"
  namespace           = "SAP/Migration/Extract"
  period              = 300
  statistic           = "Sum"
  threshold           = 0
  treat_missing_data  = "notBreaching"

  alarm_description = "Any migration job failure triggers an alarm"
  alarm_actions     = [aws_sns_topic.notifications.arn]
}

resource "aws_cloudwatch_metric_alarm" "row_count_drift" {
  alarm_name          = "${var.name_prefix}-row-count-drift"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "RowCountDriftPct"
  namespace           = "SAP/Migration/Validation"
  period              = 300
  statistic           = "Maximum"
  threshold           = 1.0 # > 1% drift
  treat_missing_data  = "notBreaching"

  alarm_description = "Row count between source and target drifted more than 1%"
  alarm_actions     = [aws_sns_topic.notifications.arn]
}

resource "aws_cloudwatch_metric_alarm" "dms_replication_lag" {
  alarm_name          = "${var.name_prefix}-dms-lag"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 3
  metric_name         = "CDCLatencyTarget"
  namespace           = "AWS/DMS"
  period              = 60
  statistic           = "Average"
  threshold           = 60 # 60 second lag
  treat_missing_data  = "notBreaching"

  alarm_description = "DMS CDC replication lag exceeded 60 seconds"
  alarm_actions     = [aws_sns_topic.notifications.arn]
}

resource "aws_cloudwatch_metric_alarm" "rds_cpu" {
  alarm_name          = "${var.name_prefix}-rds-cpu-high"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 3
  metric_name         = "CPUUtilization"
  namespace           = "AWS/RDS"
  period              = 300
  statistic           = "Average"
  threshold           = 80

  alarm_description = "RDS Postgres CPU exceeded 80% for 15 minutes"
  alarm_actions     = [aws_sns_topic.notifications.arn]
}

resource "aws_cloudwatch_metric_alarm" "redshift_disk" {
  alarm_name          = "${var.name_prefix}-redshift-disk-high"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 3
  metric_name         = "PercentageDiskSpaceUsed"
  namespace           = "AWS/Redshift"
  period              = 300
  statistic           = "Maximum"
  threshold           = 85

  alarm_description = "Redshift cluster disk usage exceeded 85%"
  alarm_actions     = [aws_sns_topic.notifications.arn]
}

# -----------------------------------------------------------------------------
# CloudWatch dashboard — single pane of glass
# -----------------------------------------------------------------------------
resource "aws_cloudwatch_dashboard" "migration" {
  dashboard_name = "${var.name_prefix}-migration"

  dashboard_body = jsonencode({
    widgets = [
      {
        type = "metric"
        x    = 0
        y    = 0
        width  = 12
        height = 6
        properties = {
          metrics = [
            ["SAP/Migration/Extract", "RowsExtracted", "Table", "MARA"],
            [".", ".", ".", "KNA1"],
            [".", ".", ".", "LFA1"],
          ]
          period = 3600
          stat = "Sum"
          region = "us-east-1"
          title = "Rows Extracted per Table (hourly)"
        }
      },
      {
        type = "metric"
        x = 12
        y = 0
        width = 12
        height = 6
        properties = {
          metrics = [
            ["SAP/Migration/Load", "PostgresRowsLoaded", "Table", "material_master"],
            ["SAP/Migration/Load", "RedshiftRowsLoaded", "Table", "material_master"],
          ]
          period = 3600
          stat = "Sum"
          region = "us-east-1"
          title = "Rows Loaded (Postgres vs Redshift)"
        }
      },
      {
        type = "metric"
        x = 0
        y = 6
        width = 12
        height = 6
        properties = {
          metrics = [
            ["AWS/DMS", "CDCLatencyTarget", "ReplicationInstanceIdentifier", "${var.name_prefix}-dms-instance"],
            ["AWS/DMS", "CDCLatencySource", ".", "."],
          ]
          period = 60
          stat = "Average"
          region = "us-east-1"
          title = "DMS CDC Replication Lag (seconds)"
        }
      },
      {
        type = "metric"
        x = 12
        y = 6
        width = 12
        height = 6
        properties = {
          metrics = [
            ["AWS/RDS", "CPUUtilization", "DBInstanceIdentifier", "${var.name_prefix}-rds"],
            ["AWS/Redshift", "CPUUtilization", "ClusterIdentifier", "${var.name_prefix}-rs"],
          ]
          period = 300
          stat = "Average"
          region = "us-east-1"
          title = "Target DB CPU"
        }
      },
    ]
  })
}

output "idempotency_table_name" { value = aws_dynamodb_table.audit.name }
output "idempotency_table_arn" { value = aws_dynamodb_table.audit.arn }
output "notifications_topic_arn" { value = aws_sns_topic.notifications.arn }
output "dashboard_name" { value = aws_cloudwatch_dashboard.migration.dashboard_name }
