import json
import os
from datetime import UTC, datetime, timedelta

import boto3
import snowflake.connector
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG


def _freshness():
    with (
        snowflake.connector.connect(
            account=os.environ["SNOWFLAKE_ACCOUNT"],
            user=os.environ["SNOWFLAKE_USER"],
            password=boto3.client("ssm").get_parameter(
                Name=os.environ["SNOWFLAKE_PASSWORD_SSM_PARAMETER"],
                WithDecryption=True,
            )["Parameter"]["Value"],
            role=os.environ["SNOWFLAKE_ROLE"],
            database=os.environ["SNOWFLAKE_DATABASE"],
            warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        ) as connection,
        connection.cursor() as cursor,
    ):
        silver_freshness = cursor.execute(
            """
            select
                configured_repositories.value::varchar as repo,
                coalesce(max(pull.updated_at) < dateadd(day, -7, sysdate()), true) as stale
            from table(flatten(input => parse_json(%s))) configured_repositories
            left join identifier(%s) pull
                on pull.repo = configured_repositories.value::varchar
            group by configured_repositories.value::varchar
            """,
            (
                json.dumps(os.environ["REPOSITORIES"].split(",")),
                f"{os.environ['SNOWFLAKE_DATABASE']}.{os.environ['DBT_SILVER_SCHEMA']}.PULL",
            ),
        ).fetchall()

    boto3.client("cloudwatch").put_metric_data(
        Namespace=os.environ["CLOUDWATCH_NAMESPACE"],
        MetricData=[
            {
                "MetricName": "data_freshness_stale",
                "Value": int(stale),
                "Unit": "Count",
                "Dimensions": [
                    {"Name": "stage", "Value": "silver"},
                    {"Name": "repo", "Value": repo},
                ],
            }
            for repo, stale in silver_freshness
        ],
    )


with DAG(
    dag_id="elt_repo_contribution_freshness",
    schedule="@daily",
    start_date=datetime(2026, 1, 1, tzinfo=UTC),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["elt", "repo-contribution"],
) as dag:
    PythonOperator(
        task_id="check_freshness",
        python_callable=_freshness,
        execution_timeout=timedelta(minutes=10),
        do_xcom_push=False,
        show_return_value_in_logs=False,
    )
