"""STUB: hourly ELT DAG (README ch.4).

Sequential hand-off: poller -> spark(silver) -> dbt(gold).
Artifacts pass between stages via XCom as S3 paths; data lives in S3 under
pipeline-artifacts/{run_id}/{stage}/output.parquet.

Config comes from env (set by Terraform user_data / .env):
  INGESTION_CADENCE, REPOSITORIES, BRONZE_BUCKET, ARTIFACTS_BUCKET,
  SPARK_APPLICATION_ID, SPARK_EXECUTION_ROLE_ARN, SILVER_JOB_S3_URI.

TODO: replace PythonOperator stubs with real poller invocation, EMR Serverless
      StartJobRun for Silver, and dbt run for Gold; wire freshness/empty-increment
      checks and CloudWatch metric emission.
"""

import os

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago


def _poll(**_):
    raise NotImplementedError("poller stage not implemented")


def _silver(**_):
    raise NotImplementedError("silver spark stage not implemented")


def _gold(**_):
    raise NotImplementedError("gold dbt stage not implemented")


with DAG(
    dag_id="elt_repo_contribution",
    schedule_interval=os.environ.get("INGESTION_CADENCE", "0 */6 * * *"),
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=["elt", "repo-contribution"],
) as dag:
    poll = PythonOperator(task_id="poller", python_callable=_poll)
    silver = PythonOperator(task_id="spark_silver", python_callable=_silver)
    gold = PythonOperator(task_id="dbt_gold", python_callable=_gold)

    poll >> silver >> gold
