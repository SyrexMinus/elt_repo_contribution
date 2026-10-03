resource "aws_emrserverless_application" "silver" {
  name          = "${var.prefix}-silver"
  release_label = "emr-7.1.0"
  type          = "spark"

  image_configuration {
    image_uri = var.image
  }

  network_configuration {
    subnet_ids         = [var.subnet_id]
    security_group_ids = [var.security_group_id]
  }

  auto_start_configuration {
    enabled = true
  }

  auto_stop_configuration {
    enabled              = true
    idle_timeout_minutes = 1
  }

  maximum_capacity {
    cpu    = "8 vCPU"
    memory = "32 GB"
  }

  depends_on = [aws_ecr_repository_policy.silver]
}


data "aws_caller_identity" "current" {}

data "aws_region" "current" {}

resource "aws_ecr_repository_policy" "silver" {
  repository = split("@", split(".amazonaws.com/", var.image)[1])[0]
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "emr-serverless.amazonaws.com" }
      Action    = ["ecr:BatchGetImage", "ecr:DescribeImages", "ecr:GetDownloadUrlForLayer"]
      Condition = {
        ArnLike = {
          "aws:SourceArn" = "arn:aws:emr-serverless:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:/applications/*"
        }
      }
    }]
  })
}


resource "aws_s3_object" "silver_job" {
  bucket = var.artifacts_bucket
  key    = "jobs/spark/silver_job.py"
  source = "${path.module}/../../../stages/spark/silver_job.py"
  etag   = filemd5("${path.module}/../../../stages/spark/silver_job.py")
}
