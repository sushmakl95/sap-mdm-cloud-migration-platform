variable "name_prefix" { type = string }
variable "glue_role_arn" { type = string }
variable "scripts_bucket" { type = string }
variable "raw_bucket" { type = string }
variable "staging_bucket" { type = string }
variable "vpc_id" { type = string }
variable "subnet_ids" { type = list(string) }
variable "security_group_ids" { type = list(string) }

# -----------------------------------------------------------------------------
# Connection to enable Glue jobs to run inside the VPC (reach SAP HANA + RDS + Redshift)
# -----------------------------------------------------------------------------
resource "aws_glue_connection" "vpc" {
  name            = "${var.name_prefix}-vpc-connection"
  connection_type = "NETWORK"

  physical_connection_requirements {
    availability_zone      = "us-east-1a" # Glue needs one AZ per connection
    security_group_id_list = var.security_group_ids
    subnet_id              = var.subnet_ids[0]
  }
}

# -----------------------------------------------------------------------------
# Glue Database (Catalog) — registers raw/staging buckets as Hive tables
# -----------------------------------------------------------------------------
resource "aws_glue_catalog_database" "raw" {
  name        = "${replace(var.name_prefix, "-", "_")}_raw"
  description = "Catalog for SAP raw extracts"
}

resource "aws_glue_catalog_database" "staging" {
  name        = "${replace(var.name_prefix, "-", "_")}_staging"
  description = "Catalog for validated staging data"
}

# -----------------------------------------------------------------------------
# Extract job
# -----------------------------------------------------------------------------
resource "aws_glue_job" "extract" {
  name              = "${var.name_prefix}-extract"
  role_arn          = var.glue_role_arn
  glue_version      = "4.0"
  number_of_workers = 10
  worker_type       = "G.1X"
  timeout           = 480 # 8 hours
  max_retries       = 1

  connections = [aws_glue_connection.vpc.name]

  command {
    name            = "glueetl"
    script_location = "s3://${var.scripts_bucket}/jobs/glue_extract_job.py"
    python_version  = "3"
  }

  default_arguments = {
    "--enable-metrics"                   = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-job-insights"              = "true"
    "--enable-auto-scaling"              = "true"
    "--job-language"                     = "python"
    "--TempDir"                          = "s3://${var.scripts_bucket}/tmp/"
    "--extra-py-files"                   = "s3://${var.scripts_bucket}/packages/migration.zip"
    "--extra-jars"                       = "s3://${var.scripts_bucket}/jars/ngdbc-2.21.10.jar,s3://${var.scripts_bucket}/jars/postgresql-42.7.3.jar"
    "--additional-python-modules"        = "structlog==24.1.0,psycopg2-binary==2.9.9,redshift-connector==2.1.0"
  }
}

# -----------------------------------------------------------------------------
# Load job
# -----------------------------------------------------------------------------
resource "aws_glue_job" "load" {
  name              = "${var.name_prefix}-load"
  role_arn          = var.glue_role_arn
  glue_version      = "4.0"
  number_of_workers = 10
  worker_type       = "G.1X"
  timeout           = 480

  connections = [aws_glue_connection.vpc.name]

  command {
    name            = "glueetl"
    script_location = "s3://${var.scripts_bucket}/jobs/glue_load_job.py"
    python_version  = "3"
  }

  default_arguments = {
    "--enable-metrics"                   = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--job-language"                     = "python"
    "--TempDir"                          = "s3://${var.scripts_bucket}/tmp/"
    "--extra-py-files"                   = "s3://${var.scripts_bucket}/packages/migration.zip"
    "--extra-jars"                       = "s3://${var.scripts_bucket}/jars/postgresql-42.7.3.jar,s3://${var.scripts_bucket}/jars/redshift-jdbc42-2.1.0.jar"
    "--additional-python-modules"        = "structlog==24.1.0,psycopg2-binary==2.9.9,redshift-connector==2.1.0"
  }
}

# -----------------------------------------------------------------------------
# Rollback job (invoked by rollback SFN)
# -----------------------------------------------------------------------------
resource "aws_glue_job" "rollback" {
  name              = "${var.name_prefix}-rollback"
  role_arn          = var.glue_role_arn
  glue_version      = "4.0"
  number_of_workers = 5
  worker_type       = "G.1X"
  timeout           = 120

  command {
    name            = "glueetl"
    script_location = "s3://${var.scripts_bucket}/jobs/glue_rollback_job.py"
    python_version  = "3"
  }

  default_arguments = {
    "--enable-metrics"            = "true"
    "--job-language"              = "python"
    "--extra-py-files"            = "s3://${var.scripts_bucket}/packages/migration.zip"
    "--additional-python-modules" = "structlog==24.1.0,psycopg2-binary==2.9.9"
  }
}

output "extract_job_name" { value = aws_glue_job.extract.name }
output "load_job_name" { value = aws_glue_job.load.name }
output "rollback_job_name" { value = aws_glue_job.rollback.name }
output "raw_database" { value = aws_glue_catalog_database.raw.name }
output "staging_database" { value = aws_glue_catalog_database.staging.name }
