variable "prefix" {
  type = string
}

variable "artifacts_bucket" {
  type = string
}

variable "image" {
  type = string

  validation {
    condition     = can(regex("^[0-9]{12}\\.dkr\\.ecr\\.[a-z0-9-]+\\.amazonaws\\.com/[a-z0-9._/-]+@sha256:[0-9a-f]{64}$", var.image))
    error_message = "image must be an immutable private ECR reference with a sha256 digest"
  }
}

variable "subnet_id" {
  type = string
}

variable "security_group_id" {
  type = string
}
