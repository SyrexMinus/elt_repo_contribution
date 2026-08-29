output "public_ip" {
  value = aws_instance.airflow.public_ip
}

output "webserver_url" {
  value = "http://${aws_instance.airflow.public_ip}:8080"
}
