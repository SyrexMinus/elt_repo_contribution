resource "aws_vpc" "pipeline" {
  cidr_block           = "10.42.0.0/16"
  enable_dns_support   = true
  enable_dns_hostnames = true
}


data "aws_availability_zones" "available" {
  state = "available"
}

resource "aws_subnet" "airflow" {
  vpc_id                  = aws_vpc.pipeline.id
  cidr_block              = "10.42.0.0/24"
  availability_zone       = data.aws_availability_zones.available.names[0]
  map_public_ip_on_launch = true
}


resource "aws_subnet" "silver" {
  vpc_id            = aws_vpc.pipeline.id
  cidr_block        = "10.42.1.0/24"
  availability_zone = aws_subnet.airflow.availability_zone
}


resource "aws_internet_gateway" "pipeline" {
  vpc_id = aws_vpc.pipeline.id
}


resource "aws_route_table" "airflow" {
  vpc_id = aws_vpc.pipeline.id
  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.pipeline.id
  }
}


resource "aws_route_table_association" "airflow" {
  subnet_id      = aws_subnet.airflow.id
  route_table_id = aws_route_table.airflow.id
}


resource "aws_route_table" "silver" {
  vpc_id = aws_vpc.pipeline.id
}


resource "aws_route" "silver_nat" {
  route_table_id         = aws_route_table.silver.id
  destination_cidr_block = "0.0.0.0/0"
  network_interface_id   = module.airflow.network_interface_id
}


resource "aws_route_table_association" "silver" {
  subnet_id      = aws_subnet.silver.id
  route_table_id = aws_route_table.silver.id
}


resource "aws_eip" "nat" {
  domain = "vpc"
}


resource "aws_eip_association" "nat" {
  allocation_id = aws_eip.nat.id
  instance_id   = module.airflow.instance_id
}


resource "aws_vpc_endpoint" "s3" {
  vpc_id          = aws_vpc.pipeline.id
  service_name    = "com.amazonaws.${var.aws_region}.s3"
  route_table_ids = [aws_route_table.airflow.id, aws_route_table.silver.id]
}


resource "aws_security_group" "silver" {
  name   = "${local.prefix}-silver"
  vpc_id = aws_vpc.pipeline.id

  ingress {
    from_port = 0
    to_port   = 0
    protocol  = "-1"
    self      = true
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}
