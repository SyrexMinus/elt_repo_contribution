variable "prefix" {
  type        = string
}

variable "alert_email" {
  type        = string
  default     = ""
}

variable "monthly_budget_usd" {
  type        = number
}

variable "metric_namespace" {
  type        = string
  default     = "EltRepoContribution"
}
