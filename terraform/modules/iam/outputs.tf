output "airflow_instance_profile_name" {
  value = aws_iam_instance_profile.airflow.name
}

output "spark_execution_role_arn" {
  value = aws_iam_role.spark_execution.arn
}
