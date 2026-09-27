from __future__ import annotations

import os
from unittest import TestCase

import snowflake.connector


SILVER_TABLE_SCHEMAS = {
    "pull": [
        ("repo", "varchar"),
        ("pr_num", "number(38, 0)"),
        ("merged_at", "timestamp_ntz"),
        ("user_login", "varchar"),
        ("state", "varchar"),
        ("created_at", "timestamp_ntz"),
        ("updated_at", "timestamp_ntz"),
        ("title", "varchar"),
        ("parse_run_id", "varchar"),
    ],
    "file": [
        ("repo", "varchar"),
        ("pr_num", "number(38, 0)"),
        ("filename", "varchar"),
        ("additions", "number(38, 0)"),
        ("deletions", "number(38, 0)"),
        ("parse_run_id", "varchar"),
    ],
    "review": [
        ("repo", "varchar"),
        ("pr_num", "number(38, 0)"),
        ("user_login", "varchar"),
        ("state", "varchar"),
        ("parse_run_id", "varchar"),
    ],
}


def set_up_snowflake(
    test_case: type[TestCase],
    database: str,
    silver_schema: str,
    additional_schemas: tuple[str, ...] = (),
) -> None:
    test_case.database = database
    test_case.silver_schema = silver_schema
    test_case.snowflake = snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        role=os.environ["SNOWFLAKE_ROLE"],
        database=test_case.database,
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
    )
    test_case.addClassCleanup(test_case.snowflake.close)
    for schema in (test_case.silver_schema, *additional_schemas):
        execute(
            connection=test_case.snowflake,
            sql=f"CREATE TRANSIENT SCHEMA {test_case.database}.{schema}",
        )
        test_case.addClassCleanup(
            execute,
            connection=test_case.snowflake,
            sql=f"DROP SCHEMA IF EXISTS {test_case.database}.{schema} CASCADE",
        )
    create_silver_tables(
        connection=test_case.snowflake,
        database=test_case.database,
        silver_schema=test_case.silver_schema,
    )


def create_silver_tables(
    connection,
    database: str,
    silver_schema: str,
) -> None:
    for table, columns in SILVER_TABLE_SCHEMAS.items():
        column_definitions = ",\n".join(
            f"{column} {data_type}" for column, data_type in columns
        )
        execute(
            connection=connection,
            sql=(
                f"CREATE TABLE {database}.{silver_schema}.{table} "
                f"({column_definitions})"
            ),
        )


def execute(connection, sql: str) -> None:
    with connection.cursor() as cursor:
        cursor.execute(sql)
