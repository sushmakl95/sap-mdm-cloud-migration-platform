variable "name_prefix" { type = string }
variable "databricks_workspace_url" { type = string }
variable "instance_profile_arn" { type = string }
variable "raw_bucket" { type = string }
variable "staging_bucket" { type = string }

# Register the AWS IAM instance profile with Databricks
# (so clusters can assume it and access our S3 + Secrets)
resource "databricks_instance_profile" "this" {
  instance_profile_arn = var.instance_profile_arn
}

# Cluster policy: limits what cluster configs users can create
# (prevents expensive r6gd.8xlarge instances by accident)
resource "databricks_cluster_policy" "migration" {
  name = "${var.name_prefix}-migration-policy"

  definition = jsonencode({
    "spark_version" = {
      "type"  = "fixed"
      "value" = "15.4.x-scala2.12"
    }
    "node_type_id" = {
      "type"         = "allowlist"
      "values"       = ["i3.xlarge", "i3.2xlarge", "r5d.xlarge", "r5d.2xlarge"]
      "defaultValue" = "i3.xlarge"
    }
    "autoscale.min_workers" = {
      "type"     = "range"
      "minValue" = 1
      "maxValue" = 4
    }
    "autoscale.max_workers" = {
      "type"     = "range"
      "minValue" = 2
      "maxValue" = 16
    }
    "aws_attributes.instance_profile_arn" = {
      "type"  = "fixed"
      "value" = var.instance_profile_arn
    }
    "aws_attributes.availability" = {
      "type"  = "fixed"
      "value" = "SPOT_WITH_FALLBACK"
    }
  })
}

# Reconciliation job cluster — short-lived, runs per-invocation
resource "databricks_job" "reconciliation" {
  name = "${var.name_prefix}-reconciliation"

  job_cluster {
    job_cluster_key = "reconcile"
    new_cluster {
      spark_version = "15.4.x-scala2.12"
      node_type_id  = "i3.xlarge"
      policy_id     = databricks_cluster_policy.migration.id
      num_workers   = 2
      aws_attributes {
        availability         = "SPOT_WITH_FALLBACK"
        instance_profile_arn = var.instance_profile_arn
      }
    }
  }

  task {
    task_key        = "run-reconciliation"
    job_cluster_key = "reconcile"

    notebook_task {
      notebook_path = "/Repos/data-eng/sap-mdm/notebooks/reconciliation"
      base_parameters = {
        raw_bucket     = var.raw_bucket
        staging_bucket = var.staging_bucket
      }
    }

    timeout_seconds           = 3600
    max_retries               = 1
    min_retry_interval_millis = 60000
  }

  email_notifications {
    on_failure = []
  }

  schedule {
    quartz_cron_expression = "0 0 3 * * ?"
    timezone_id            = "UTC"
    pause_status           = "PAUSED" # triggered by SFN instead
  }
}

output "cluster_policy_id" { value = databricks_cluster_policy.migration.id }
output "reconciliation_job_id" { value = databricks_job.reconciliation.id }
