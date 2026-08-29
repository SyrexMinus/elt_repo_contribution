locals {
  prefix = "${var.name_prefix}-${var.environment}"
}

module "s3" {
  source                = "./modules/s3"
  prefix                = local.prefix
  bronze_retention_days = var.bronze_retention_days
}

module "iam" {
  source      = "./modules/iam"
  prefix      = local.prefix
  bucket_arns = module.s3.bucket_arns
}

module "spark" {
  source             = "./modules/spark"
  prefix             = local.prefix
  execution_role_arn = module.iam.spark_execution_role_arn
  artifacts_bucket   = module.s3.artifacts_bucket_name
}

module "airflow" {
  source               = "./modules/airflow"
  prefix               = local.prefix
  instance_type        = var.airflow_instance_type
  ssh_key_name         = var.airflow_ssh_key_name
  allowed_cidr         = var.airflow_allowed_cidr
  instance_profile     = module.iam.airflow_instance_profile_name
  ingestion_cadence    = var.ingestion_cadence
  repositories         = var.repositories
  bronze_bucket        = module.s3.bronze_bucket_name
  artifacts_bucket     = module.s3.artifacts_bucket_name
  spark_application_id = module.spark.application_id
}

module "snowflake" {
  source         = "./modules/snowflake"
  prefix         = replace(upper(local.prefix), "-", "_")
  warehouse_size = var.snowflake_warehouse_size
}

module "cloudwatch" {
  source             = "./modules/cloudwatch"
  prefix             = local.prefix
  alert_email        = var.alert_email
  monthly_budget_usd = var.monthly_budget_usd
}
