variable "prefix" {
  type = string
}

variable "alert_email" {
  type = string
}

variable "monthly_budget_usd" {
  type = number
}

variable "metric_namespace" {
  type = string
}

variable "repositories" {
  type = list(string)
}
