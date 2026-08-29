data "aws_ami" "al2023" {
  most_recent = true
  owners      = ["amazon"]
  filter {
    name   = "name"
    values = ["al2023-ami-*-x86_64"]
  }
}

resource "aws_security_group" "airflow" {
  name        = "${var.prefix}-airflow"

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
  user_data = templatefile("${path.module}/templates/user_data.sh.tftpl", {
    ingestion_cadence    = var.ingestion_cadence
    repositories         = join(",", var.repositories)
    bronze_bucket        = var.bronze_bucket
    artifacts_bucket     = var.artifacts_bucket
    spark_application_id = var.spark_application_id
  })
}

resource "aws_instance" "airflow" {
  ami                    = data.aws_ami.al2023.id
  instance_type          = var.instance_type
  key_name               = var.ssh_key_name != "" ? var.ssh_key_name : null
  iam_instance_profile   = var.instance_profile
  vpc_security_group_ids = [aws_security_group.airflow.id]
  user_data              = local.user_data

  root_block_device {
    volume_size = 30
    volume_type = "gp3"
  }

  tags = {
    Name = "${var.prefix}-airflow"
  }
}
