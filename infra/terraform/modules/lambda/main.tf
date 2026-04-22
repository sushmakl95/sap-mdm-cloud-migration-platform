variable "name_prefix" { type = string }
variable "lambda_role_arn" { type = string }
variable "raw_bucket_name" { type = string }
variable "idempotency_table" { type = string }
variable "sns_topic_arn" { type = string }
variable "state_machine_arn" { type = string }
variable "subnet_ids" { type = list(string) }
variable "security_group_ids" { type = list(string) }

# Lambda deployment artifacts — built by CI pipeline to these paths
data "archive_file" "s3_trigger" {
  type        = "zip"
  source_file = "${path.module}/../../../src/lambdas/s3_trigger.py"
  output_path = "${path.module}/s3_trigger.zip"
}

data "archive_file" "sfn_notifier" {
  type        = "zip"
  source_file = "${path.module}/../../../src/lambdas/sfn_notifier.py"
  output_path = "${path.module}/sfn_notifier.zip"
}

# -----------------------------------------------------------------------------
# S3 trigger — fires SFN when DMS writes CDC files
# -----------------------------------------------------------------------------
resource "aws_lambda_function" "s3_trigger" {
  function_name    = "${var.name_prefix}-s3-trigger"
  role             = var.lambda_role_arn
  handler          = "s3_trigger.handler"
  runtime          = "python3.11"
  filename         = data.archive_file.s3_trigger.output_path
  source_code_hash = data.archive_file.s3_trigger.output_base64sha256
  timeout          = 30
  memory_size      = 256

  environment {
    variables = {
      IDEMPOTENCY_TABLE        = var.idempotency_table
      STATE_MACHINE_ARN        = var.state_machine_arn
      DEBOUNCE_WINDOW_MINUTES  = "5"
    }
  }

  vpc_config {
    subnet_ids         = var.subnet_ids
    security_group_ids = var.security_group_ids
  }
}

resource "aws_lambda_permission" "s3_can_invoke" {
  statement_id  = "AllowS3Invoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.s3_trigger.function_name
  principal     = "s3.amazonaws.com"
  source_arn    = "arn:aws:s3:::${var.raw_bucket_name}"
}

resource "aws_s3_bucket_notification" "cdc_trigger" {
  bucket = var.raw_bucket_name

  lambda_function {
    lambda_function_arn = aws_lambda_function.s3_trigger.arn
    events              = ["s3:ObjectCreated:*"]
    filter_prefix       = "cdc/sap/"
    filter_suffix       = ".parquet"
  }

  depends_on = [aws_lambda_permission.s3_can_invoke]
}

# -----------------------------------------------------------------------------
# SFN notifier — state-change events → SNS
# -----------------------------------------------------------------------------
resource "aws_lambda_function" "sfn_notifier" {
  function_name    = "${var.name_prefix}-sfn-notifier"
  role             = var.lambda_role_arn
  handler          = "sfn_notifier.handler"
  runtime          = "python3.11"
  filename         = data.archive_file.sfn_notifier.output_path
  source_code_hash = data.archive_file.sfn_notifier.output_base64sha256
  timeout          = 15

  environment {
    variables = {
      SNS_TOPIC_ARN = var.sns_topic_arn
    }
  }
}

output "s3_trigger_function_arn" { value = aws_lambda_function.s3_trigger.arn }
output "sfn_notifier_function_arn" { value = aws_lambda_function.sfn_notifier.arn }
