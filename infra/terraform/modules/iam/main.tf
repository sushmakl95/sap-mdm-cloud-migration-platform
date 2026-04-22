variable "name_prefix" { type = string }
variable "glue_scripts_bucket" { type = string }
variable "raw_bucket_arn" { type = string }
variable "staging_bucket_arn" { type = string }
variable "secrets_kms_key_arn" { type = string }
variable "secret_arns" { type = list(string) }

data "aws_region" "current" {}
data "aws_caller_identity" "current" {}

# -----------------------------------------------------------------------------
# Glue role — runs extract + load jobs
# -----------------------------------------------------------------------------
resource "aws_iam_role" "glue" {
  name = "${var.name_prefix}-glue-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "glue.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

resource "aws_iam_role_policy" "glue_data_access" {
  name = "${var.name_prefix}-glue-data"
  role = aws_iam_role.glue.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = ["s3:GetObject", "s3:ListBucket", "s3:PutObject", "s3:DeleteObject"]
        Resource = [
          var.raw_bucket_arn,
          "${var.raw_bucket_arn}/*",
          var.staging_bucket_arn,
          "${var.staging_bucket_arn}/*",
          var.glue_scripts_bucket,
          "${var.glue_scripts_bucket}/*",
        ]
      },
      {
        Effect   = "Allow"
        Action   = ["secretsmanager:GetSecretValue"]
        Resource = var.secret_arns
      },
      {
        Effect   = "Allow"
        Action   = ["kms:Decrypt"]
        Resource = var.secrets_kms_key_arn
      },
      {
        Effect   = "Allow"
        Action   = ["dynamodb:GetItem", "dynamodb:PutItem", "dynamodb:UpdateItem"]
        Resource = "arn:aws:dynamodb:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:table/${var.name_prefix}-*"
      },
      {
        Effect   = "Allow"
        Action   = ["cloudwatch:PutMetricData"]
        Resource = "*"
      },
    ]
  })
}

# -----------------------------------------------------------------------------
# DMS roles — both the "vpc" role and the "cloudwatch-logs" role
# -----------------------------------------------------------------------------
resource "aws_iam_role" "dms" {
  name = "${var.name_prefix}-dms-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "dms.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy_attachment" "dms_vpc" {
  role       = aws_iam_role.dms.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonDMSVPCManagementRole"
}

resource "aws_iam_role_policy" "dms_s3" {
  name = "${var.name_prefix}-dms-s3"
  role = aws_iam_role.dms.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = ["s3:PutObject", "s3:DeleteObject", "s3:GetObject", "s3:ListBucket"]
      Resource = [
        var.raw_bucket_arn,
        "${var.raw_bucket_arn}/*",
      ]
    }]
  })
}

# -----------------------------------------------------------------------------
# Redshift COPY role
# -----------------------------------------------------------------------------
resource "aws_iam_role" "redshift_copy" {
  name = "${var.name_prefix}-redshift-copy-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "redshift.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "redshift_copy_s3" {
  name = "${var.name_prefix}-redshift-copy-s3"
  role = aws_iam_role.redshift_copy.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = ["s3:GetObject", "s3:ListBucket"]
      Resource = [var.staging_bucket_arn, "${var.staging_bucket_arn}/*"]
    }]
  })
}

# -----------------------------------------------------------------------------
# Lambda role
# -----------------------------------------------------------------------------
resource "aws_iam_role" "lambda" {
  name = "${var.name_prefix}-lambda-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy_attachment" "lambda_basic" {
  role       = aws_iam_role.lambda.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_iam_role_policy_attachment" "lambda_vpc" {
  role       = aws_iam_role.lambda.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaVPCAccessExecutionRole"
}

resource "aws_iam_role_policy" "lambda_actions" {
  name = "${var.name_prefix}-lambda-actions"
  role = aws_iam_role.lambda.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["states:StartExecution"]
        Resource = "arn:aws:states:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:stateMachine:${var.name_prefix}-*"
      },
      {
        Effect   = "Allow"
        Action   = ["dynamodb:GetItem", "dynamodb:PutItem", "dynamodb:UpdateItem"]
        Resource = "arn:aws:dynamodb:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:table/${var.name_prefix}-*"
      },
      {
        Effect   = "Allow"
        Action   = ["sns:Publish"]
        Resource = "arn:aws:sns:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:${var.name_prefix}-*"
      },
    ]
  })
}

# -----------------------------------------------------------------------------
# Step Functions role
# -----------------------------------------------------------------------------
resource "aws_iam_role" "sfn" {
  name = "${var.name_prefix}-sfn-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "states.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "sfn_invocation" {
  name = "${var.name_prefix}-sfn-invocation"
  role = aws_iam_role.sfn.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "glue:StartJobRun", "glue:GetJobRun", "glue:GetJobRuns", "glue:BatchStopJobRun"
        ]
        Resource = "*"
      },
      {
        Effect   = "Allow"
        Action   = ["dynamodb:GetItem", "dynamodb:PutItem", "dynamodb:UpdateItem"]
        Resource = "arn:aws:dynamodb:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:table/${var.name_prefix}-*"
      },
      {
        Effect   = "Allow"
        Action   = ["sns:Publish"]
        Resource = "arn:aws:sns:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:${var.name_prefix}-*"
      },
      {
        Effect   = "Allow"
        Action   = ["states:StartExecution"]
        Resource = "arn:aws:states:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:stateMachine:${var.name_prefix}-*"
      },
      {
        Effect   = "Allow"
        Action   = ["events:PutTargets", "events:PutRule", "events:DescribeRule"]
        Resource = "*"
      },
    ]
  })
}

# -----------------------------------------------------------------------------
# Databricks instance profile (the IAM role Databricks clusters assume)
# -----------------------------------------------------------------------------
resource "aws_iam_role" "databricks_instance" {
  name = "${var.name_prefix}-databricks-instance"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "ec2.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_instance_profile" "databricks" {
  name = "${var.name_prefix}-databricks-profile"
  role = aws_iam_role.databricks_instance.name
}

resource "aws_iam_role_policy" "databricks_data" {
  name = "${var.name_prefix}-databricks-data"
  role = aws_iam_role.databricks_instance.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = ["s3:GetObject", "s3:ListBucket", "s3:PutObject", "s3:DeleteObject"]
        Resource = [
          var.raw_bucket_arn,
          "${var.raw_bucket_arn}/*",
          var.staging_bucket_arn,
          "${var.staging_bucket_arn}/*",
        ]
      },
      {
        Effect   = "Allow"
        Action   = ["secretsmanager:GetSecretValue"]
        Resource = var.secret_arns
      },
      {
        Effect   = "Allow"
        Action   = ["kms:Decrypt"]
        Resource = var.secrets_kms_key_arn
      },
    ]
  })
}

output "glue_role_arn" { value = aws_iam_role.glue.arn }
output "dms_role_arn" { value = aws_iam_role.dms.arn }
output "redshift_copy_role_arn" { value = aws_iam_role.redshift_copy.arn }
output "lambda_role_arn" { value = aws_iam_role.lambda.arn }
output "sfn_role_arn" { value = aws_iam_role.sfn.arn }
output "databricks_instance_profile_arn" { value = aws_iam_instance_profile.databricks.arn }
