output "alerts_topic_arn" {
  value = aws_sns_topic.alerts.arn
}

output "metric_namespace" {
  value = var.metric_namespace
}
