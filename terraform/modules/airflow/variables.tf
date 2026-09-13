variable "prefix" {
  type = string
}

variable "aws_region" {
  type = string
}

variable "image" {
  type = string

  validation {
    condition     = can(regex("^[0-9]{12}\\.dkr\\.ecr\\.[a-z0-9-]+\\.amazonaws\\.com/[a-z0-9._/-]+@sha256:[0-9a-f]{64}$", var.image))
    error_message = "image must be an immutable private ECR reference with a sha256 digest"
  }
}

variable "instance_type" {
  type = string
}

variable "ssh_key_name" {
  type = string
}

variable "allowed_cidr" {
  type = string
}

variable "instance_profile" {
  type = string
}

variable "ingestion_cadence" {
  type = string
}

variable "repositories" {
  type = list(string)
}

variable "bronze_bucket" {
  type = string
}

variable "artifacts_bucket" {
  type = string
}

variable "spark_application_id" {
  type = string
}
