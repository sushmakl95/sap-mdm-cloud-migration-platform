variable "name_prefix" { type = string }
variable "vpc_id" { type = string }
variable "subnet_ids" { type = list(string) }
variable "allowed_source_sg_ids" { type = list(string) }
variable "kms_key_arn" { type = string }
variable "master_username" { type = string }
variable "master_password_secret" { type = string }
variable "instance_class" { type = string }
variable "allocated_storage_gb" { type = number }
variable "multi_az" { type = bool }

data "aws_secretsmanager_secret_version" "pg" {
  secret_id = var.master_password_secret
}

resource "aws_security_group" "rds" {
  name        = "${var.name_prefix}-rds-sg"
  description = "RDS Postgres — allow from app tier only"
  vpc_id      = var.vpc_id

  ingress {
    from_port       = 5432
    to_port         = 5432
    protocol        = "tcp"
    security_groups = var.allowed_source_sg_ids
    description     = "Postgres from app SGs"
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_db_subnet_group" "this" {
  name       = "${var.name_prefix}-rds-subnets"
  subnet_ids = var.subnet_ids
}

resource "aws_db_parameter_group" "pg16" {
  name   = "${var.name_prefix}-pg16"
  family = "postgres16"

  parameter {
    name  = "shared_preload_libraries"
    value = "pg_stat_statements"
  }
  parameter {
    name  = "log_min_duration_statement"
    value = "1000"
  }
  parameter {
    name  = "log_statement"
    value = "ddl"
  }
  parameter {
    name  = "track_io_timing"
    value = "on"
  }
}

resource "aws_db_instance" "this" {
  identifier                            = "${var.name_prefix}-rds"
  engine                                = "postgres"
  engine_version                        = "16.3"
  instance_class                        = var.instance_class
  allocated_storage                     = var.allocated_storage_gb
  max_allocated_storage                 = var.allocated_storage_gb * 2
  storage_type                          = "gp3"
  storage_encrypted                     = true
  kms_key_id                            = var.kms_key_arn
  username                              = var.master_username
  password                              = jsondecode(data.aws_secretsmanager_secret_version.pg.secret_string)["password"]
  db_subnet_group_name                  = aws_db_subnet_group.this.name
  vpc_security_group_ids                = [aws_security_group.rds.id]
  parameter_group_name                  = aws_db_parameter_group.pg16.name
  multi_az                              = var.multi_az
  publicly_accessible                   = false
  skip_final_snapshot                   = false
  final_snapshot_identifier             = "${var.name_prefix}-rds-final-snapshot"
  backup_retention_period               = 14
  backup_window                         = "03:00-04:00"
  maintenance_window                    = "sun:04:00-sun:05:00"
  performance_insights_enabled          = true
  performance_insights_kms_key_id       = var.kms_key_arn
  performance_insights_retention_period = 7
  deletion_protection                   = true
  enabled_cloudwatch_logs_exports       = ["postgresql"]
}

output "endpoint" { value = aws_db_instance.this.endpoint }
output "id" { value = aws_db_instance.this.id }
output "security_group_id" { value = aws_security_group.rds.id }
