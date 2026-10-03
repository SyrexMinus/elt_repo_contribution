output "application_id" {
  value = aws_emrserverless_application.silver.id
}

output "application_arn" {
  value = aws_emrserverless_application.silver.arn
}

output "silver_job_s3_uri" {
  value = "s3://${var.artifacts_bucket}/${aws_s3_object.silver_job.key}"
}
