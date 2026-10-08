aws_region  = "$AWS_REGION"
environment = "$ENVIRONMENT"
name_prefix = "$NAME_PREFIX"

repositories      = $REPOSITORIES_JSON
ingestion_cadence = "0 */6 * * *"
bronze_retention_days = 7
monthly_budget_usd    = 10
alert_email           = "$ALERT_EMAIL"

airflow_instance_type = "t3.medium"
airflow_image         = "$AIRFLOW_IMAGE"
airflow_ssh_key_name  = "$EC2_KEY_NAME"
airflow_allowed_cidr  = "$OPERATOR_PUBLIC_IP/32"

snowflake_account        = "$SNOWFLAKE_ACCOUNT"
snowflake_user           = "$SNOWFLAKE_TERRAFORM_USER"
snowflake_role           = "ACCOUNTADMIN"
snowflake_warehouse_size = "XSMALL"
snowflake_runtime_user   = "$SNOWFLAKE_RUNTIME_USER"
silver_image            = "$SILVER_IMAGE"
github_token_ssm_parameter       = "$GITHUB_PARAMETER"
snowflake_password_ssm_parameter = "$SNOWFLAKE_PARAMETER"
