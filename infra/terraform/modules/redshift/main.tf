variable "name_prefix" { type = string }
variable "vpc_id" { type = string }
variable "subnet_ids" { type = list(string) }
variable "allowed_source_sg_ids" { type = list(string) }
variable "kms_key_arn" { type = string }
variable "master_username" { type = string }
variable "node_type" { type = string }
variable "number_of_nodes" { type = number }
variable "s3_iam_role_arn" { type = string }

resource "random_password" "master" {
  length  = 32
  special = false
}

resource "aws_security_group" "redshift" {
  name        = "${var.name_prefix}-redshift-sg"
  description = "Redshift — allow from app tier only"
  vpc_id      = var.vpc_id

  ingress {
    from_port       = 5439
    to_port         = 5439
    protocol        = "tcp"
    security_groups = var.allowed_source_sg_ids
    description     = "Redshift from app SGs"
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_redshift_subnet_group" "this" {
  name       = "${var.name_prefix}-redshift-subnets"
  subnet_ids = var.subnet_ids
}

resource "aws_redshift_parameter_group" "this" {
  name   = "${var.name_prefix}-rs-params"
  family = "redshift-1.0"

  parameter { name = "require_ssl"; value = "true" }
  parameter { name = "enable_user_activity_logging"; value = "true" }
  parameter { name = "max_concurrency_scaling_clusters"; value = "1" }
}

resource "aws_redshift_cluster" "this" {
  cluster_identifier           = "${var.name_prefix}-rs"
  database_name                = "sap_mdm"
  master_username              = var.master_username
  master_password              = random_password.master.result
  node_type                    = var.node_type
  number_of_nodes              = var.number_of_nodes
  cluster_type                 = var.number_of_nodes > 1 ? "multi-node" : "single-node"
  cluster_subnet_group_name    = aws_redshift_subnet_group.this.name
  vpc_security_group_ids       = [aws_security_group.redshift.id]
  parameter_group_name         = aws_redshift_parameter_group.this.name
  encrypted                    = true
  kms_key_id                   = var.kms_key_arn
  publicly_accessible          = false
  skip_final_snapshot          = false
  final_snapshot_identifier    = "${var.name_prefix}-rs-final-snapshot"
  automated_snapshot_retention_period = 7
  iam_roles                    = [var.s3_iam_role_arn]
  enhanced_vpc_routing         = true

  logging {
    enable = true
    log_destination_type = "cloudwatch"
    log_exports = ["userlog", "connectionlog", "useractivitylog"]
  }
}

output "endpoint" { value = aws_redshift_cluster.this.endpoint }
output "database_name" { value = aws_redshift_cluster.this.database_name }
output "id" { value = aws_redshift_cluster.this.id }
output "security_group_id" { value = aws_security_group.redshift.id }
