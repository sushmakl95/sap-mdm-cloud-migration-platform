variable "name_prefix" { type = string }
variable "vpc_id" { type = string }
variable "subnet_ids" { type = list(string) }
variable "replication_instance_class" { type = string }
variable "source_secret_arn" { type = string }
variable "target_s3_bucket" { type = string }
variable "dms_role_arn" { type = string }

# DMS subnet group
resource "aws_dms_replication_subnet_group" "this" {
  replication_subnet_group_id          = "${var.name_prefix}-dms-subnets"
  replication_subnet_group_description = "DMS subnet group for SAP migration"
  subnet_ids                           = var.subnet_ids
}

# Replication instance (the worker)
resource "aws_dms_replication_instance" "this" {
  replication_instance_id     = "${var.name_prefix}-dms-instance"
  replication_instance_class  = var.replication_instance_class
  allocated_storage           = 100
  apply_immediately           = true
  auto_minor_version_upgrade  = true
  publicly_accessible         = false
  multi_az                    = false
  replication_subnet_group_id = aws_dms_replication_subnet_group.this.id
  engine_version              = "3.5.2"
}

# -----------------------------------------------------------------------------
# Source endpoint -- SAP HANA
# -----------------------------------------------------------------------------
# IMPORTANT: As of terraform-provider-aws 5.x, the `aws_dms_endpoint` resource
# does NOT support `engine_name = "sap-hana"` -- even though DMS itself supports
# SAP HANA as a source. We use `engine_name = "sybase"` here as a valid
# placeholder for `terraform validate` to pass.
#
# REAL-WORLD DEPLOYMENT: After `terraform apply`, run:
#
#   bash scripts/create-sap-hana-dms-endpoint.sh <env>
#
# which uses the AWS CLI directly to create the proper SAP HANA endpoint.
# See docs/RUNBOOK.md "Creating the SAP HANA DMS endpoint" section.
# -----------------------------------------------------------------------------

data "aws_secretsmanager_secret_version" "sap_hana" {
  secret_id = var.source_secret_arn
}

resource "aws_dms_endpoint" "source_sap_hana_placeholder" {
  endpoint_id   = "${var.name_prefix}-sap-hana-source-placeholder"
  endpoint_type = "source"
  engine_name   = "sybase"

  server_name   = jsondecode(data.aws_secretsmanager_secret_version.sap_hana.secret_string)["host"]
  port          = tonumber(jsondecode(data.aws_secretsmanager_secret_version.sap_hana.secret_string)["port"])
  database_name = jsondecode(data.aws_secretsmanager_secret_version.sap_hana.secret_string)["database"]
  username      = jsondecode(data.aws_secretsmanager_secret_version.sap_hana.secret_string)["username"]
  password      = jsondecode(data.aws_secretsmanager_secret_version.sap_hana.secret_string)["password"]

  extra_connection_attributes = "encrypt=true;validateCertificate=true"

  lifecycle {
    ignore_changes = [engine_name]
  }
}

# Target endpoint -- S3
resource "aws_dms_s3_endpoint" "target_s3" {
  endpoint_id             = "${var.name_prefix}-s3-target"
  endpoint_type           = "target"
  bucket_name             = var.target_s3_bucket
  bucket_folder           = "cdc/sap"
  service_access_role_arn = var.dms_role_arn

  compression_type         = "GZIP"
  data_format              = "parquet"
  parquet_version          = "parquet-2-0"
  include_op_for_full_load = true
  cdc_inserts_and_updates  = true
  cdc_inserts_only         = false
  timestamp_column_name    = "_dms_committed_at"
  cdc_path                 = "cdc/sap"
  date_partition_enabled   = true
  date_partition_sequence  = "YYYYMMDDHH"
  enable_statistics        = true
}

output "replication_instance_arn" {
  value = aws_dms_replication_instance.this.replication_instance_arn
}
output "source_endpoint_arn" {
  value = aws_dms_endpoint.source_sap_hana_placeholder.endpoint_arn
}
output "target_endpoint_arn" {
  value = aws_dms_s3_endpoint.target_s3.endpoint_arn
}
