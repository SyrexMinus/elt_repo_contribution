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
      Resource = [var.spark_application_arn, "${var.spark_application_arn}/jobruns/*"]
    }]
  })
}


resource "aws_iam_role_policy_attachment" "airflow_emr" {
  role       = aws_iam_role.airflow.name
  policy_arn = aws_iam_policy.emr_serverless_invoke.arn
}


resource "aws_iam_role_policy" "airflow_pass_spark" {
  role = aws_iam_role.airflow.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = "iam:PassRole"
      Resource = aws_iam_role.spark_execution.arn
      Condition = {
        StringEquals = { "iam:PassedToService" = "emr-serverless.amazonaws.com" }
      }
    }]
  })
}


data "aws_region" "current" {}

data "aws_caller_identity" "current" {}

resource "aws_iam_role_policy" "airflow_secrets" {
  role = aws_iam_role.airflow.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = "ssm:GetParameter"
      Resource = [
        for name in [var.github_token_ssm_parameter, var.snowflake_password_ssm_parameter] :
        "arn:aws:ssm:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:parameter/${trimprefix(name, "/")}"
      ]
    }]
  })
}


resource "aws_iam_role_policy" "spark_secret" {
  role = aws_iam_role.spark_execution.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = "ssm:GetParameter"
      Resource = "arn:aws:ssm:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:parameter/${trimprefix(var.snowflake_password_ssm_parameter, "/")}"
    }]
  })
}


resource "aws_iam_role_policy" "secrets_kms" {
  for_each = var.secrets_kms_key_arn == null ? {} : {
    airflow = aws_iam_role.airflow.id
    silver  = aws_iam_role.spark_execution.id
  }
  role = each.value
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = "kms:Decrypt"
      Resource = var.secrets_kms_key_arn
      Condition = {
        StringEquals = { "kms:ViaService" = "ssm.${data.aws_region.current.name}.amazonaws.com" }
      }
    }]
  })
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
