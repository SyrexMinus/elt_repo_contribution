variable "prefix" {
  type        = string
}

variable "instance_type" {
  type        = string
}

variable "ssh_key_name" {
  type        = string
}

variable "allowed_cidr" {
  type        = string
}

variable "instance_profile" {
  type        = string
}

variable "ingestion_cadence" {
  type        = string
}

variable "repositories" {
  type        = list(string)
}

variable "bronze_bucket" {
  type        = string
}

variable "artifacts_bucket" {
  type        = string
}

variable "spark_application_id" {
  type        = string
}
