data "aws_iam_policy_document" "s3_access" {
  statement {
    actions   = ["s3:ListBucket", "s3:GetBucketLocation"]
    resources = var.bucket_arns
  }
  statement {
    actions   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"]
    resources = [for arn in var.bucket_arns : "${arn}/*"]
  }
}

resource "aws_iam_policy" "s3_access" {
  name   = "${var.prefix}-s3-access"
  policy = data.aws_iam_policy_document.s3_access.json
}


data "aws_iam_policy_document" "ec2_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["ec2.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "airflow" {
  name               = "${var.prefix}-airflow"
  assume_role_policy = data.aws_iam_policy_document.ec2_assume.json
}


resource "aws_iam_role_policy_attachment" "airflow_s3" {
  role       = aws_iam_role.airflow.name
  policy_arn = aws_iam_policy.s3_access.arn
}


resource "aws_iam_policy" "cloudwatch_put_metrics" {
  name = "${var.prefix}-cw-metrics"
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["cloudwatch:PutMetricData", "logs:CreateLogStream", "logs:PutLogEvents"]
      Resource = "*"
    }]
  })
}


resource "aws_iam_role_policy_attachment" "airflow_cw" {
  role       = aws_iam_role.airflow.name
  policy_arn = aws_iam_policy.cloudwatch_put_metrics.arn
}


resource "aws_iam_policy" "emr_serverless_invoke" {
  name = "${var.prefix}-emr-invoke"
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["emr-serverless:StartJobRun", "emr-serverless:GetJobRun", "emr-serverless:CancelJobRun", "emr-serverless:StartApplication"]
      Resource = "*"
    }]
  })
}


resource "aws_iam_role_policy_attachment" "airflow_emr" {
  role       = aws_iam_role.airflow.name
  policy_arn = aws_iam_policy.emr_serverless_invoke.arn
}


resource "aws_iam_role_policy_attachment" "airflow_ecr" {
  role       = aws_iam_role.airflow.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonEC2ContainerRegistryReadOnly"
}


resource "aws_iam_role_policy_attachment" "airflow_ssm" {
  role       = aws_iam_role.airflow.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
}


resource "aws_iam_instance_profile" "airflow" {
  name = "${var.prefix}-airflow"
  role = aws_iam_role.airflow.name
}


data "aws_iam_policy_document" "emr_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["emr-serverless.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "spark_execution" {
  name               = "${var.prefix}-spark-exec"
  assume_role_policy = data.aws_iam_policy_document.emr_assume.json
}


resource "aws_iam_role_policy_attachment" "spark_s3" {
  role       = aws_iam_role.spark_execution.name
  policy_arn = aws_iam_policy.s3_access.arn
}


resource "aws_iam_role_policy_attachment" "spark_cw" {
  role       = aws_iam_role.spark_execution.name
  policy_arn = aws_iam_policy.cloudwatch_put_metrics.arn
}
