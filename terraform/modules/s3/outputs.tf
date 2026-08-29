output "bronze_bucket_name" {
  value = aws_s3_bucket.bronze.bucket
}

output "artifacts_bucket_name" {
  value = aws_s3_bucket.artifacts.bucket
}

output "bucket_arns" {
  value       = [aws_s3_bucket.bronze.arn, aws_s3_bucket.artifacts.arn]
}
