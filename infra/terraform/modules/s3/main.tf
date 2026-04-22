variable "name_prefix" { type = string }
variable "kms_key_arn" { type = string }

resource "random_id" "suffix" {
  byte_length = 4
}

locals {
  suffix = random_id.suffix.hex
}

# -----------------------------------------------------------------------------
# Raw zone — SAP extracts land here
# -----------------------------------------------------------------------------
resource "aws_s3_bucket" "raw" {
  bucket = "${var.name_prefix}-raw-${local.suffix}"
  tags = { Tier = "raw" }
}

resource "aws_s3_bucket_versioning" "raw" {
  bucket = aws_s3_bucket.raw.id
  versioning_configuration { status = "Enabled" }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "raw" {
  bucket = aws_s3_bucket.raw.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm     = "aws:kms"
      kms_master_key_id = var.kms_key_arn
    }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_public_access_block" "raw" {
  bucket                  = aws_s3_bucket.raw.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_lifecycle_configuration" "raw" {
  bucket = aws_s3_bucket.raw.id

  rule {
    id     = "archive-old-extracts"
    status = "Enabled"
    filter { prefix = "raw/sap/" }
    transition {
      days          = 90
      storage_class = "GLACIER_IR"
    }
    expiration {
      days = 2555 # 7 years for SOX
    }
  }
  rule {
    id     = "clean-up-cdc"
    status = "Enabled"
    filter { prefix = "cdc/sap/" }
    transition {
      days          = 30
      storage_class = "STANDARD_IA"
    }
    expiration {
      days = 180
    }
  }
}

# -----------------------------------------------------------------------------
# Staging zone — validated, ready-to-load Parquet
# -----------------------------------------------------------------------------
resource "aws_s3_bucket" "staging" {
  bucket = "${var.name_prefix}-staging-${local.suffix}"
  tags = { Tier = "staging" }
}

resource "aws_s3_bucket_versioning" "staging" {
  bucket = aws_s3_bucket.staging.id
  versioning_configuration { status = "Enabled" }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "staging" {
  bucket = aws_s3_bucket.staging.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm     = "aws:kms"
      kms_master_key_id = var.kms_key_arn
    }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "staging" {
  bucket = aws_s3_bucket.staging.id
  rule {
    id     = "expire-old-staging"
    status = "Enabled"
    filter { prefix = "" }
    expiration { days = 14 }
  }
}

# -----------------------------------------------------------------------------
# Scripts bucket — Glue scripts + Databricks JAR artifacts
# -----------------------------------------------------------------------------
resource "aws_s3_bucket" "glue_scripts" {
  bucket = "${var.name_prefix}-glue-scripts-${local.suffix}"
  tags = { Tier = "scripts" }
}

resource "aws_s3_bucket_versioning" "glue_scripts" {
  bucket = aws_s3_bucket.glue_scripts.id
  versioning_configuration { status = "Enabled" }
}

# -----------------------------------------------------------------------------
# Audit archive — WORM (Object Lock) for SOX/GDPR
# -----------------------------------------------------------------------------
resource "aws_s3_bucket" "audit" {
  bucket              = "${var.name_prefix}-audit-${local.suffix}"
  object_lock_enabled = true
  tags = { Tier = "audit" }
}

resource "aws_s3_bucket_object_lock_configuration" "audit" {
  bucket = aws_s3_bucket.audit.id
  rule {
    default_retention {
      mode = "COMPLIANCE"
      days = 2555 # 7 years
    }
  }
}

output "raw_bucket_name" { value = aws_s3_bucket.raw.id }
output "raw_bucket_arn" { value = aws_s3_bucket.raw.arn }
output "staging_bucket_name" { value = aws_s3_bucket.staging.id }
output "staging_bucket_arn" { value = aws_s3_bucket.staging.arn }
output "glue_scripts_bucket_name" { value = aws_s3_bucket.glue_scripts.id }
output "glue_scripts_bucket_arn" { value = aws_s3_bucket.glue_scripts.arn }
output "audit_bucket_name" { value = aws_s3_bucket.audit.id }
output "audit_bucket_arn" { value = aws_s3_bucket.audit.arn }
