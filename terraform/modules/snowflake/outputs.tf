output "database_name" {
  value = snowflake_database.this.name
}

output "warehouse_name" {
  value = snowflake_warehouse.this.name
}

output "runtime_role_name" {
  value = snowflake_account_role.runtime.name
}
