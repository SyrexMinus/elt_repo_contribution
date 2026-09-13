resource "snowflake_database" "this" {
  name = "${var.prefix}_DB"
}

resource "snowflake_schema" "silver" {
  database = snowflake_database.this.name
  name     = "SILVER"
}

resource "snowflake_table" "pull" {
  database = snowflake_database.this.name
  schema   = snowflake_schema.silver.name
  name     = "PULL"

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
  database = snowflake_database.this.name
  schema   = snowflake_schema.silver.name
  name     = "FILE"

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
  database = snowflake_database.this.name
  schema   = snowflake_schema.silver.name
  name     = "REVIEW"

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
