variable "aws_region" {
  type        = string
  default     = "eu-central-1"
}

variable "environment" {
  type        = string
  default     = "demo"
}

variable "name_prefix" {
  type        = string
  default     = "elt-repo-contribution"
}

variable "repositories" {
  type        = list(string)
  default     = ["apache/spark", "ClickHouse/ClickHouse"]
}

variable "ingestion_cadence" {
  type        = string
  default     = "0 */6 * * *"
}

variable "bronze_retention_days" {
  type        = number
  default     = 7
}

variable "monthly_budget_usd" {
  type        = number
  default     = 10
}

variable "alert_email" {
  type        = string
  default     = ""
}

variable "airflow_instance_type" {
  type        = string
  default     = "t3.micro"
}

variable "airflow_ssh_key_name" {
  type        = string
  default     = ""
}

variable "airflow_allowed_cidr" {
  type        = string
  default     = "0.0.0.0/0"
}

variable "snowflake_account" {
  type        = string
  default     = ""
}

variable "snowflake_user" {
  type        = string
  default     = ""
}

variable "snowflake_role" {
  type        = string
  default     = "SYSADMIN"
}

variable "snowflake_warehouse_size" {
  type        = string
  default     = "XSMALL"
}
