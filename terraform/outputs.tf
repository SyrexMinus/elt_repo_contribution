output "bronze_bucket" {
  value = module.s3.bronze_bucket_name
}

output "artifacts_bucket" {
  value = module.s3.artifacts_bucket_name
}

output "spark_application_id" {
  value = module.spark.application_id
}

output "airflow_url" {
  value = "http://${aws_eip.nat.public_ip}:8080"
}

output "airflow_public_ip" {
  value = aws_eip.nat.public_ip
}

output "snowflake_database" {
  value = module.snowflake.database_name
}

output "snowflake_warehouse" {
  value = module.snowflake.warehouse_name
}
