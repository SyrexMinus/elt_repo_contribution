output "database_name" {
  value = snowflake_database.this.name
}

output "silver_schema" {
  value = snowflake_schema.silver.name
}

output "gold_schema" {
  value = snowflake_schema.gold.name
}

output "warehouse_name" {
  value = snowflake_warehouse.this.name
}
