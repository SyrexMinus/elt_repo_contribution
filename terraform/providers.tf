provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      Project     = var.name_prefix
      Environment = var.environment
      ManagedBy   = "terraform"
    }
  }
}

provider "snowflake" {
  organization_name = local.snowflake_account_parts[0]
  account_name      = local.snowflake_account_parts[1]
  user              = var.snowflake_user
  role              = var.snowflake_role
}
