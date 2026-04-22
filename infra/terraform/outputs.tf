output "vpc_id" {
  value = module.vpc.vpc_id
}

output "rds_endpoint" {
  value     = module.rds.endpoint
  sensitive = true
}

output "redshift_endpoint" {
  value     = module.redshift.endpoint
  sensitive = true
}

output "raw_bucket" {
  value = module.s3.raw_bucket_name
}

output "staging_bucket" {
  value = module.s3.staging_bucket_name
}

output "glue_extract_job" {
  value = module.glue.extract_job_name
}

output "glue_load_job" {
  value = module.glue.load_job_name
}

output "batch_migrate_sm_arn" {
  value = module.stepfunctions.batch_migrate_sm_arn
}

output "migrate_table_sm_arn" {
  value = module.stepfunctions.migrate_table_sm_arn
}

output "notifications_topic_arn" {
  value = module.monitoring.notifications_topic_arn
}

output "idempotency_table" {
  value = module.monitoring.idempotency_table_name
}
