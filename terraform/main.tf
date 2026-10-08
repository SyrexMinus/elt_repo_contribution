locals {
  prefix                  = "${var.name_prefix}-${var.environment}"
  snowflake_account_parts = regex("^([^-]+)-(.*)$", var.snowflake_account)
}

module "s3" {
  source                = "./modules/s3"
  prefix                = local.prefix
  bronze_retention_days = var.bronze_retention_days
}

module "iam" {
  source                           = "./modules/iam"
  prefix                           = local.prefix
  bucket_arns                      = module.s3.bucket_arns
  github_token_ssm_parameter       = var.github_token_ssm_parameter
  snowflake_password_ssm_parameter = var.snowflake_password_ssm_parameter
  secrets_kms_key_arn              = var.secrets_kms_key_arn
  spark_application_arn            = module.spark.application_arn
}

module "spark" {
  source            = "./modules/spark"
  prefix            = local.prefix
  artifacts_bucket  = module.s3.artifacts_bucket_name
  image             = var.silver_image
  subnet_id         = aws_subnet.silver.id
  security_group_id = aws_security_group.silver.id
}

module "airflow" {
  source                           = "./modules/airflow"
  prefix                           = local.prefix
  aws_region                       = var.aws_region
  image                            = var.airflow_image
  instance_type                    = var.airflow_instance_type
  ssh_key_name                     = var.airflow_ssh_key_name
  allowed_cidr                     = var.airflow_allowed_cidr
  instance_profile                 = module.iam.airflow_instance_profile_name
  ingestion_cadence                = var.ingestion_cadence
  repositories                     = var.repositories
  bronze_bucket                    = module.s3.bronze_bucket_name
  artifacts_bucket                 = module.s3.artifacts_bucket_name
  spark_application_id             = module.spark.application_id
  spark_execution_role_arn         = module.iam.spark_execution_role_arn
  silver_job_s3_uri                = module.spark.silver_job_s3_uri
  vpc_id                           = aws_vpc.pipeline.id
  subnet_id                        = aws_subnet.airflow.id
  silver_subnet_cidr               = aws_subnet.silver.cidr_block
  github_token_ssm_parameter       = var.github_token_ssm_parameter
  snowflake_password_ssm_parameter = var.snowflake_password_ssm_parameter
  snowflake_account                = var.snowflake_account
  snowflake_runtime_user           = var.snowflake_runtime_user
  snowflake_runtime_role           = module.snowflake.runtime_role_name
  snowflake_database               = module.snowflake.database_name
  snowflake_warehouse              = module.snowflake.warehouse_name

  depends_on = [module.iam, module.snowflake, aws_route_table_association.airflow]
}

module "snowflake" {
  source         = "./modules/snowflake"
  prefix         = replace(upper(local.prefix), "-", "_")
  warehouse_size = var.snowflake_warehouse_size
  runtime_user   = var.snowflake_runtime_user
  nat_public_ip  = aws_eip.nat.public_ip
}

module "cloudwatch" {
  source             = "./modules/cloudwatch"
  prefix             = local.prefix
  alert_email        = var.alert_email
  monthly_budget_usd = var.monthly_budget_usd
  metric_namespace   = "EltRepoContribution"
}
