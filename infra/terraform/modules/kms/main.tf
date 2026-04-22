variable "name_prefix" { type = string }

# Separate CMK per data layer — blast-radius containment.
# Compromise of one key never exposes cross-layer data.

resource "aws_kms_key" "s3" {
  description             = "${var.name_prefix}-s3 data-at-rest encryption"
  deletion_window_in_days = 30
  enable_key_rotation     = true
}

resource "aws_kms_alias" "s3" {
  name          = "alias/${var.name_prefix}-s3"
  target_key_id = aws_kms_key.s3.key_id
}

resource "aws_kms_key" "secrets" {
  description             = "${var.name_prefix}-secrets Secrets Manager encryption"
  deletion_window_in_days = 30
  enable_key_rotation     = true
}

resource "aws_kms_alias" "secrets" {
  name          = "alias/${var.name_prefix}-secrets"
  target_key_id = aws_kms_key.secrets.key_id
}

resource "aws_kms_key" "rds" {
  description             = "${var.name_prefix}-rds encryption"
  deletion_window_in_days = 30
  enable_key_rotation     = true
}

resource "aws_kms_alias" "rds" {
  name          = "alias/${var.name_prefix}-rds"
  target_key_id = aws_kms_key.rds.key_id
}

resource "aws_kms_key" "redshift" {
  description             = "${var.name_prefix}-redshift encryption"
  deletion_window_in_days = 30
  enable_key_rotation     = true
}

resource "aws_kms_alias" "redshift" {
  name          = "alias/${var.name_prefix}-redshift"
  target_key_id = aws_kms_key.redshift.key_id
}

resource "aws_kms_key" "dynamodb" {
  description             = "${var.name_prefix}-dynamodb encryption"
  deletion_window_in_days = 30
  enable_key_rotation     = true
}

output "s3_key_arn" { value = aws_kms_key.s3.arn }
output "secrets_key_arn" { value = aws_kms_key.secrets.arn }
output "rds_key_arn" { value = aws_kms_key.rds.arn }
output "redshift_key_arn" { value = aws_kms_key.redshift.arn }
output "dynamodb_key_arn" { value = aws_kms_key.dynamodb.arn }
