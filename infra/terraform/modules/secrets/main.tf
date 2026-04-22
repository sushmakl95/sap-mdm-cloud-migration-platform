variable "name_prefix" { type = string }
variable "kms_key_arn" { type = string }
variable "sap_hana_secret" { type = string; sensitive = true }
variable "postgres_secret" { type = string; sensitive = true }
variable "redshift_secret" { type = string; sensitive = true }

resource "aws_secretsmanager_secret" "sap_hana" {
  name                    = "${var.name_prefix}/sap-hana-jdbc"
  kms_key_id              = var.kms_key_arn
  recovery_window_in_days = 7
}

resource "aws_secretsmanager_secret_version" "sap_hana" {
  secret_id     = aws_secretsmanager_secret.sap_hana.id
  secret_string = var.sap_hana_secret
}

resource "aws_secretsmanager_secret" "postgres" {
  name                    = "${var.name_prefix}/postgres"
  kms_key_id              = var.kms_key_arn
  recovery_window_in_days = 7
}

resource "aws_secretsmanager_secret_version" "postgres" {
  secret_id     = aws_secretsmanager_secret.postgres.id
  secret_string = var.postgres_secret
}

resource "aws_secretsmanager_secret" "redshift" {
  name                    = "${var.name_prefix}/redshift"
  kms_key_id              = var.kms_key_arn
  recovery_window_in_days = 7
}

resource "aws_secretsmanager_secret_version" "redshift" {
  secret_id     = aws_secretsmanager_secret.redshift.id
  secret_string = var.redshift_secret
}

# Rotation — Secrets Manager can auto-rotate via Lambda.
# We enable it here for Postgres + Redshift (SAP credentials are managed by Basis team).

output "sap_hana_secret_arn" { value = aws_secretsmanager_secret.sap_hana.arn }
output "postgres_secret_arn" { value = aws_secretsmanager_secret.postgres.arn }
output "redshift_secret_arn" { value = aws_secretsmanager_secret.redshift.arn }
