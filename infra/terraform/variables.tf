variable "project_name" {
  description = "Project prefix for all resources"
  type        = string
  default     = "sap-mdm"
}

variable "environment" {
  description = "Environment: dev, staging, prod"
  type        = string
  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "Environment must be dev, staging, or prod."
  }
}

variable "aws_region" {
  description = "AWS region"
  type        = string
  default     = "us-east-1"
}

variable "cost_center" {
  description = "Cost center tag for billing"
  type        = string
  default     = "data-platform"
}

# -----------------------------------------------------------------------------
# Networking
# -----------------------------------------------------------------------------
variable "vpc_cidr_block" {
  description = "CIDR block for the data-platform VPC"
  type        = string
  default     = "10.20.0.0/16"
}

variable "availability_zones" {
  description = "AZs to span"
  type        = list(string)
  default     = ["us-east-1a", "us-east-1b", "us-east-1c"]
}

# -----------------------------------------------------------------------------
# Secrets (initial values only — rotate via Secrets Manager after creation)
# -----------------------------------------------------------------------------
variable "sap_hana_secret_value" {
  description = "Initial SAP HANA connection JSON (host, port, database, username, password)"
  type        = string
  sensitive   = true
}

variable "postgres_secret_value" {
  description = "Initial Postgres connection JSON"
  type        = string
  sensitive   = true
}

variable "redshift_secret_value" {
  description = "Initial Redshift connection JSON"
  type        = string
  sensitive   = true
}

# -----------------------------------------------------------------------------
# RDS + Redshift sizing
# -----------------------------------------------------------------------------
variable "postgres_master_username" {
  type    = string
  default = "sap_mdm_admin"
}

variable "rds_instance_class" {
  type    = string
  default = "db.r6g.large"
}

variable "rds_allocated_storage_gb" {
  type    = number
  default = 200
}

variable "redshift_master_username" {
  type    = string
  default = "sap_analytics"
}

variable "redshift_node_type" {
  type    = string
  default = "ra3.xlplus"
}

variable "redshift_number_of_nodes" {
  type    = number
  default = 2
}

# -----------------------------------------------------------------------------
# DMS
# -----------------------------------------------------------------------------
variable "dms_replication_instance_class" {
  type    = string
  default = "dms.r5.large"
}

# -----------------------------------------------------------------------------
# Databricks
# -----------------------------------------------------------------------------
variable "databricks_workspace_url" {
  description = "Databricks workspace URL (https://<org>.cloud.databricks.com)"
  type        = string
}

variable "databricks_token" {
  description = "Databricks PAT"
  type        = string
  sensitive   = true
}

# -----------------------------------------------------------------------------
# Schedule
# -----------------------------------------------------------------------------
variable "nightly_migration_schedule" {
  description = "EventBridge cron expression for nightly batch migration"
  type        = string
  default     = "cron(0 2 * * ? *)" # 2am UTC daily
}

# -----------------------------------------------------------------------------
# Observability
# -----------------------------------------------------------------------------
variable "log_retention_days" {
  type    = number
  default = 90
}

variable "alarm_email_recipients" {
  description = "Email addresses to subscribe to alarm SNS topic"
  type        = list(string)
  default     = []
}
