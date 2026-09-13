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
  account = var.snowflake_account
  user    = var.snowflake_user
  role    = var.snowflake_role
}
