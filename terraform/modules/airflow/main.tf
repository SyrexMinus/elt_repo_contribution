resource "aws_security_group" "airflow" {
  name   = "${var.prefix}-airflow"
  vpc_id = var.vpc_id

  ingress {
    description = "EMR HTTPS through NAT"
    from_port   = 443
    to_port     = 443
    protocol    = "tcp"
    cidr_blocks = [var.silver_subnet_cidr]
  }

  ingress {
    description = "Snowflake OCSP through NAT"
    from_port   = 80
    to_port     = 80
    protocol    = "tcp"
    cidr_blocks = [var.silver_subnet_cidr]
  }

  ingress {
    description = "Airflow webserver"
    from_port   = 8080
    to_port     = 8080
    protocol    = "tcp"
    cidr_blocks = [var.allowed_cidr]
  }

  ingress {
    description = "SSH"
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = [var.allowed_cidr]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}


locals {
  user_data = templatefile("${path.module}/templates/user_data.yaml.tftpl", {
    airflow_env = base64encode(templatefile("${path.module}/templates/airflow.env.tftpl", {
      ingestion_cadence                = var.ingestion_cadence
      repositories                     = join(",", var.repositories)
      bronze_bucket                    = var.bronze_bucket
      artifacts_bucket                 = var.artifacts_bucket
      spark_application_id             = var.spark_application_id
      spark_execution_role_arn         = var.spark_execution_role_arn
      silver_job_s3_uri                = var.silver_job_s3_uri
      aws_region                       = var.aws_region
      github_token_ssm_parameter       = var.github_token_ssm_parameter
      snowflake_password_ssm_parameter = var.snowflake_password_ssm_parameter
      snowflake_account                = var.snowflake_account
      snowflake_runtime_user           = var.snowflake_runtime_user
      snowflake_runtime_role           = var.snowflake_runtime_role
      snowflake_database               = var.snowflake_database
      snowflake_warehouse              = var.snowflake_warehouse
    }))
    bootstrap_airflow = filebase64("${path.module}/files/bootstrap-airflow")
    deploy_airflow    = filebase64("${path.module}/files/deploy-airflow")
    docker_compose    = filebase64("${path.module}/files/docker-compose.yml")
  })
}


data "aws_ami" "al2023" {
  most_recent = true
  owners      = ["amazon"]
  filter {
    name   = "name"
    values = ["al2023-ami-*-x86_64"]
  }
}

resource "aws_instance" "airflow" {
  ami                         = data.aws_ami.al2023.id
  instance_type               = var.instance_type
  subnet_id                   = var.subnet_id
  source_dest_check           = false
  key_name                    = var.ssh_key_name != "" ? var.ssh_key_name : null
  iam_instance_profile        = var.instance_profile
  vpc_security_group_ids      = [aws_security_group.airflow.id]
  user_data                   = local.user_data
  user_data_replace_on_change = true

  metadata_options {
    http_tokens                 = "required"
    http_put_response_hop_limit = 2
  }

  root_block_device {
    volume_size = 30
    volume_type = "gp3"
  }

  tags = {
    Name = "${var.prefix}-airflow"
  }
}


resource "aws_ssm_association" "airflow" {
  name = "AWS-RunShellScript"

  targets {
    key    = "InstanceIds"
    values = [aws_instance.airflow.id]
  }

  parameters = {
    commands = join("\n", [
      "for attempt in $(seq 1 60); do test -f /opt/airflow/.bootstrap-complete && break; sleep 5; done",
      "test -f /opt/airflow/.bootstrap-complete || { echo 'Airflow bootstrap did not complete within 300 seconds' >&2; exit 1; }",
      "/usr/local/bin/deploy-airflow '${var.image}' '${var.aws_region}'",
    ])
  }

  wait_for_success_timeout_seconds = 600
}
