resource "aws_sns_topic" "alerts" {
  name = "${var.prefix}-alerts"
}


data "aws_caller_identity" "current" {}

resource "aws_sns_topic_policy" "alerts" {
  arn = aws_sns_topic.alerts.arn
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = ["cloudwatch.amazonaws.com", "budgets.amazonaws.com"] }
      Action    = "sns:Publish"
      Resource  = aws_sns_topic.alerts.arn
      Condition = {
        StringEquals = { "aws:SourceAccount" = data.aws_caller_identity.current.account_id }
      }
    }]
  })
}


resource "aws_sns_topic_subscription" "email" {
  topic_arn = aws_sns_topic.alerts.arn
  protocol  = "email"
  endpoint  = var.alert_email
}


resource "aws_cloudwatch_metric_alarm" "corrupted" {
  for_each            = toset(["poller", "silver"])
  alarm_name          = "${var.prefix}-${each.key}-corrupted-records"
  namespace           = var.metric_namespace
  metric_name         = "corrupted_count"
  statistic           = "Sum"
  period              = 3600
  evaluation_periods  = 1
  comparison_operator = "GreaterThanThreshold"
  threshold           = 0
  alarm_actions       = [aws_sns_topic.alerts.arn]
  treat_missing_data  = "notBreaching"
  dimensions          = { stage = each.key }
}


resource "aws_cloudwatch_metric_alarm" "stage_duration" {
  for_each            = { poller = 3600, silver = 7200, gold = 1800 }
  alarm_name          = "${var.prefix}-${each.key}-stage-duration"
  namespace           = var.metric_namespace
  metric_name         = "job_duration"
  statistic           = "Maximum"
  period              = 300
  evaluation_periods  = 1
  comparison_operator = "GreaterThanThreshold"
  threshold           = each.value
  alarm_actions       = [aws_sns_topic.alerts.arn]
  treat_missing_data  = "notBreaching"
  dimensions          = { stage = each.key }
}


resource "aws_cloudwatch_metric_alarm" "job_failures" {
  for_each            = toset(["poller", "silver", "gold"])
  alarm_name          = "${var.prefix}-${each.key}-job-failures"
  namespace           = var.metric_namespace
  metric_name         = "job_finish_status"
  statistic           = "Sum"
  period              = 3600
  evaluation_periods  = 1
  comparison_operator = "GreaterThanThreshold"
  threshold           = 0
  alarm_actions       = [aws_sns_topic.alerts.arn]
  treat_missing_data  = "notBreaching"
  dimensions          = { stage = each.key, status = "fail" }
}


resource "aws_budgets_budget" "monthly" {
  name         = "${var.prefix}-monthly"
  budget_type  = "COST"
  limit_amount = tostring(var.monthly_budget_usd)
  limit_unit   = "USD"
  time_unit    = "MONTHLY"
  depends_on   = [aws_sns_topic_policy.alerts]

  notification {
    comparison_operator       = "GREATER_THAN"
    threshold                 = 80
    threshold_type            = "PERCENTAGE"
    notification_type         = "ACTUAL"
    subscriber_sns_topic_arns = [aws_sns_topic.alerts.arn]
  }
}
