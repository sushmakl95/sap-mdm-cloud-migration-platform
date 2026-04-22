terraform {
  required_version = ">= 1.7.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.40"
    }
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.40"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.6"
    }
  }
}

provider "aws" {
  region = var.aws_region
  default_tags {
    tags = {
      Project     = "sap-mdm-migration"
      Environment = var.environment
      ManagedBy   = "terraform"
      Owner       = "data-engineering"
      CostCenter  = var.cost_center
    }
  }
}

provider "databricks" {
  host  = var.databricks_workspace_url
  token = var.databricks_token
}

locals {
  name_prefix = "${var.project_name}-${var.environment}"

  common_tags = {
    Project     = "sap-mdm-migration"
    Environment = var.environment
    ManagedBy   = "terraform"
  }
}

# -----------------------------------------------------------------------------
# Layer 1: Networking (VPC + subnets + routing)
# -----------------------------------------------------------------------------
module "vpc" {
  source = "./modules/vpc"

  name_prefix        = local.name_prefix
  cidr_block         = var.vpc_cidr_block
  availability_zones = var.availability_zones
}

module "networking" {
  source = "./modules/networking"

  name_prefix      = local.name_prefix
  vpc_id           = module.vpc.vpc_id
  public_subnet_ids = module.vpc.public_subnet_ids
  private_app_subnet_ids  = module.vpc.private_app_subnet_ids
  private_data_subnet_ids = module.vpc.private_data_subnet_ids
}

# -----------------------------------------------------------------------------
# Layer 2: KMS + Secrets + IAM
# -----------------------------------------------------------------------------
module "kms" {
  source = "./modules/kms"

  name_prefix = local.name_prefix
}

module "secrets" {
  source = "./modules/secrets"

  name_prefix      = local.name_prefix
  kms_key_arn      = module.kms.secrets_key_arn
  sap_hana_secret  = var.sap_hana_secret_value
  postgres_secret  = var.postgres_secret_value
  redshift_secret  = var.redshift_secret_value
}

module "iam" {
  source = "./modules/iam"

  name_prefix         = local.name_prefix
  glue_scripts_bucket = module.s3.glue_scripts_bucket_arn
  raw_bucket_arn      = module.s3.raw_bucket_arn
  staging_bucket_arn  = module.s3.staging_bucket_arn
  secrets_kms_key_arn = module.kms.secrets_key_arn
  secret_arns = [
    module.secrets.sap_hana_secret_arn,
    module.secrets.postgres_secret_arn,
    module.secrets.redshift_secret_arn,
  ]
}

# -----------------------------------------------------------------------------
# Layer 3: S3 buckets
# -----------------------------------------------------------------------------
module "s3" {
  source = "./modules/s3"

  name_prefix = local.name_prefix
  kms_key_arn = module.kms.s3_key_arn
}

# -----------------------------------------------------------------------------
# Layer 4: Data stores (RDS, Redshift)
# -----------------------------------------------------------------------------
module "rds" {
  source = "./modules/rds"

  name_prefix            = local.name_prefix
  vpc_id                 = module.vpc.vpc_id
  subnet_ids             = module.vpc.private_data_subnet_ids
  allowed_source_sg_ids  = [module.networking.app_sg_id]
  kms_key_arn            = module.kms.rds_key_arn
  master_username        = var.postgres_master_username
  master_password_secret = module.secrets.postgres_secret_arn
  instance_class         = var.rds_instance_class
  allocated_storage_gb   = var.rds_allocated_storage_gb
  multi_az               = var.environment == "prod"
}

module "redshift" {
  source = "./modules/redshift"

  name_prefix           = local.name_prefix
  vpc_id                = module.vpc.vpc_id
  subnet_ids            = module.vpc.private_data_subnet_ids
  allowed_source_sg_ids = [module.networking.app_sg_id]
  kms_key_arn           = module.kms.redshift_key_arn
  master_username       = var.redshift_master_username
  node_type             = var.redshift_node_type
  number_of_nodes       = var.redshift_number_of_nodes
  s3_iam_role_arn       = module.iam.redshift_copy_role_arn
}

# -----------------------------------------------------------------------------
# Layer 5: Compute (Glue, DMS, Databricks, Lambda)
# -----------------------------------------------------------------------------
module "glue" {
  source = "./modules/glue"

  name_prefix         = local.name_prefix
  glue_role_arn       = module.iam.glue_role_arn
  scripts_bucket      = module.s3.glue_scripts_bucket_name
  raw_bucket          = module.s3.raw_bucket_name
  staging_bucket      = module.s3.staging_bucket_name
  vpc_id              = module.vpc.vpc_id
  subnet_ids          = module.vpc.private_app_subnet_ids
  security_group_ids  = [module.networking.app_sg_id]
}

module "dms" {
  source = "./modules/dms"

  name_prefix              = local.name_prefix
  vpc_id                   = module.vpc.vpc_id
  subnet_ids               = module.vpc.private_data_subnet_ids
  replication_instance_class = var.dms_replication_instance_class
  source_secret_arn        = module.secrets.sap_hana_secret_arn
  target_s3_bucket         = module.s3.raw_bucket_name
  dms_role_arn             = module.iam.dms_role_arn
}

module "databricks" {
  source = "./modules/databricks"

  name_prefix                = local.name_prefix
  databricks_workspace_url   = var.databricks_workspace_url
  instance_profile_arn       = module.iam.databricks_instance_profile_arn
  raw_bucket                 = module.s3.raw_bucket_name
  staging_bucket             = module.s3.staging_bucket_name
}

module "lambda" {
  source = "./modules/lambda"

  name_prefix          = local.name_prefix
  lambda_role_arn      = module.iam.lambda_role_arn
  raw_bucket_name      = module.s3.raw_bucket_name
  idempotency_table    = module.monitoring.idempotency_table_name
  sns_topic_arn        = module.monitoring.notifications_topic_arn
  state_machine_arn    = module.stepfunctions.migrate_table_sm_arn
  subnet_ids           = module.vpc.private_app_subnet_ids
  security_group_ids   = [module.networking.app_sg_id]
}

# -----------------------------------------------------------------------------
# Layer 6: Orchestration (Step Functions, EventBridge)
# -----------------------------------------------------------------------------
module "stepfunctions" {
  source = "./modules/stepfunctions"

  name_prefix         = local.name_prefix
  sfn_role_arn        = module.iam.sfn_role_arn
  glue_extract_job    = module.glue.extract_job_name
  glue_load_job       = module.glue.load_job_name
  idempotency_table   = module.monitoring.idempotency_table_name
  sns_topic_arn       = module.monitoring.notifications_topic_arn
  databricks_job_id   = module.databricks.reconciliation_job_id
  rollback_glue_job   = module.glue.rollback_job_name
}

module "eventbridge" {
  source = "./modules/eventbridge"

  name_prefix           = local.name_prefix
  batch_sm_arn          = module.stepfunctions.batch_migrate_sm_arn
  sfn_notifier_lambda_arn = module.lambda.sfn_notifier_function_arn
  schedule_expression   = var.nightly_migration_schedule
}

# -----------------------------------------------------------------------------
# Layer 7: Observability
# -----------------------------------------------------------------------------
module "monitoring" {
  source = "./modules/monitoring"

  name_prefix             = local.name_prefix
  kms_key_arn             = module.kms.dynamodb_key_arn
  log_retention_days      = var.log_retention_days
  alarm_email_recipients  = var.alarm_email_recipients
}
