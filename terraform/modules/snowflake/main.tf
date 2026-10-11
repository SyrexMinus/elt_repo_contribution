resource "snowflake_database" "this" {
  name = "${var.prefix}_DB"
}

resource "snowflake_schema" "silver" {
  database = snowflake_database.this.name
  name     = "SILVER"
}

resource "snowflake_table" "pull" {
  database   = snowflake_database.this.name
  schema     = snowflake_schema.silver.name
  name       = "PULL"
  cluster_by = ["REPO", "STATE", "MERGED_AT"]

  column {
    name = "REPO"
    type = "STRING"
  }
  column {
    name = "PR_NUM"
    type = "NUMBER(38,0)"
  }
  column {
    name = "MERGED_AT"
    type = "TIMESTAMP_NTZ"
  }
  column {
    name = "USER_LOGIN"
    type = "STRING"
  }
  column {
    name = "STATE"
    type = "STRING"
  }
  column {
    name = "CREATED_AT"
    type = "TIMESTAMP_NTZ"
  }
  column {
    name = "UPDATED_AT"
    type = "TIMESTAMP_NTZ"
  }
  column {
    name = "TITLE"
    type = "STRING"
  }
  column {
    name = "PARSE_RUN_ID"
    type = "STRING"
  }
}

resource "snowflake_table" "file" {
  database   = snowflake_database.this.name
  schema     = snowflake_schema.silver.name
  name       = "FILE"
  cluster_by = ["REPO", "PR_NUM", "FILENAME"]

  column {
    name = "REPO"
    type = "STRING"
  }
  column {
    name = "PR_NUM"
    type = "NUMBER(38,0)"
  }
  column {
    name = "FILENAME"
    type = "STRING"
  }
  column {
    name = "ADDITIONS"
    type = "NUMBER(38,0)"
  }
  column {
    name = "DELETIONS"
    type = "NUMBER(38,0)"
  }
  column {
    name = "PARSE_RUN_ID"
    type = "STRING"
  }
}

resource "snowflake_table" "review" {
  database   = snowflake_database.this.name
  schema     = snowflake_schema.silver.name
  name       = "REVIEW"
  cluster_by = ["REPO", "PR_NUM", "STATE"]

  column {
    name = "REPO"
    type = "STRING"
  }
  column {
    name = "PR_NUM"
    type = "NUMBER(38,0)"
  }
  column {
    name = "USER_LOGIN"
    type = "STRING"
  }
  column {
    name = "STATE"
    type = "STRING"
  }
  column {
    name = "PARSE_RUN_ID"
    type = "STRING"
  }
}

resource "snowflake_schema" "gold" {
  database = snowflake_database.this.name
  name     = "GOLD"
}

resource "snowflake_warehouse" "this" {
  name                = "${var.prefix}_WH"
  warehouse_size      = var.warehouse_size
  auto_suspend        = 60
  auto_resume         = true
  initially_suspended = true
}

resource "snowflake_account_role" "runtime" {
  name = "${var.prefix}_PIPELINE"
}

resource "snowflake_grant_account_role" "runtime" {
  role_name = snowflake_account_role.runtime.name
  user_name = var.runtime_user
}

resource "snowflake_grant_privileges_to_account_role" "database" {
  account_role_name = snowflake_account_role.runtime.name
  privileges        = ["USAGE"]
  on_account_object {
    object_type = "DATABASE"
    object_name = snowflake_database.this.name
  }
}

resource "snowflake_grant_privileges_to_account_role" "warehouse" {
  account_role_name = snowflake_account_role.runtime.name
  privileges        = ["USAGE"]
  on_account_object {
    object_type = "WAREHOUSE"
    object_name = snowflake_warehouse.this.name
  }
}

resource "snowflake_grant_privileges_to_account_role" "silver_schema" {
  account_role_name = snowflake_account_role.runtime.name
  privileges        = ["USAGE", "CREATE TABLE", "CREATE STAGE", "CREATE FILE FORMAT"]
  on_schema {
    schema_name = "\"${snowflake_database.this.name}\".\"${snowflake_schema.silver.name}\""
  }
}

resource "snowflake_grant_privileges_to_account_role" "silver_tables" {
  for_each = {
    pull   = snowflake_table.pull.name
    file   = snowflake_table.file.name
    review = snowflake_table.review.name
  }
  account_role_name = snowflake_account_role.runtime.name
  privileges        = ["SELECT", "INSERT", "UPDATE", "DELETE"]
  on_schema_object {
    object_type = "TABLE"
    object_name = "\"${snowflake_database.this.name}\".\"${snowflake_schema.silver.name}\".\"${each.value}\""
  }
}

resource "snowflake_grant_privileges_to_account_role" "gold_schema" {
  account_role_name = snowflake_account_role.runtime.name
  privileges        = ["USAGE", "CREATE TABLE", "CREATE VIEW"]
  on_schema {
    schema_name = "\"${snowflake_database.this.name}\".\"${snowflake_schema.gold.name}\""
  }
}

resource "snowflake_network_policy" "runtime" {
  name            = "${var.prefix}_PIPELINE_EGRESS"
  allowed_ip_list = ["${var.nat_public_ip}/32"]
}

resource "snowflake_network_policy_attachment" "runtime" {
  network_policy_name = snowflake_network_policy.runtime.name
  set_for_account     = false
  users               = [var.runtime_user]
}
