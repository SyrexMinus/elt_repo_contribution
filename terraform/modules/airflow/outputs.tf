output "public_ip" {
  value = aws_instance.airflow.public_ip
}

output "webserver_url" {
  value = "http://${aws_instance.airflow.public_ip}:8080"
}

output "instance_id" {
  value = aws_instance.airflow.id
}

output "network_interface_id" {
  value = aws_instance.airflow.primary_network_interface_id
}
