variable "aws_region" {
  type = string
}

variable "environment" {
  type = string
}

variable "name_prefix" {
  type = string
}

variable "repositories" {
  type = list(string)
}

variable "ingestion_cadence" {
  type = string
}

variable "bronze_retention_days" {
  type = number
}

variable "monthly_budget_usd" {
  type = number
}

variable "alert_email" {
  type = string

  validation {
    condition     = can(regex("^[^@[:space:]]+@[^@[:space:]]+\\.[^@[:space:]]+$", var.alert_email))
    error_message = "alert_email must be a valid non-empty email address"
  }
}

variable "airflow_instance_type" {
  type = string
}

variable "airflow_image" {
  type = string
}

variable "airflow_ssh_key_name" {
  type = string
}

variable "airflow_allowed_cidr" {
  type = string
}

variable "snowflake_account" {
  type = string
}

variable "snowflake_user" {
  type = string
}

variable "snowflake_role" {
  type = string
}

variable "snowflake_warehouse_size" {
  type = string
}

variable "silver_image" {
  type = string
}

variable "github_token_ssm_parameter" {
  type = string
}

variable "snowflake_password_ssm_parameter" {
  type = string
}

variable "secrets_kms_key_arn" {
  type    = string
  default = null
}

variable "snowflake_runtime_user" {
  type    = string
  default = "ELT_PIPELINE"
}
