resource "aws_emrserverless_application" "silver" {
  name          = "${var.prefix}-silver"
  release_label = "emr-7.1.0"
  type          = "spark"

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
}

resource "aws_s3_object" "silver_job" {
  bucket = var.artifacts_bucket
  key    = "jobs/spark/silver_job.py"
  source = "${path.module}/../../../stages/spark/silver_job.py"
  etag   = filemd5("${path.module}/../../../stages/spark/silver_job.py")
}
