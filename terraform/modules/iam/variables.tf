variable "prefix" {
  type = string
}

variable "bucket_arns" {
  type = list(string)
}

variable "github_token_ssm_parameter" {
  type = string
}

variable "snowflake_password_ssm_parameter" {
  type = string
}

variable "secrets_kms_key_arn" {
  type = string
}

variable "spark_application_arn" {
  type = string
}
