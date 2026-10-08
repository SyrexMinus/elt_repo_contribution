import json
import os
import subprocess
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from time import monotonic

import boto3
import requests
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG, Param

from stages.poller.poller import poller


def _poll(**context):
    with requests.Session() as github:
        return poller(
            event={
                "repositories": os.environ["REPOSITORIES"].split(","),
                "github_token": _secret("GITHUB_TOKEN_SSM_PARAMETER"),
                "github_api_url": "https://api.github.com",
                "mode": context["params"]["mode"],
                "run_id": _run_id(context),
                "bronze_bucket": os.environ["BRONZE_BUCKET"],
                "artifacts_bucket": os.environ["ARTIFACTS_BUCKET"],
                "cloudwatch_namespace": os.environ["CLOUDWATCH_NAMESPACE"],
                "batch_max_pull_requests": 1000,
                "batch_max_bytes": 10 * 1024 * 1024,
                "request_timeout_seconds": 30,
            },
            s3=boto3.client("s3"),
            github=github,
            cloudwatch=boto3.client("cloudwatch"),
        )


def _secret(parameter_env):
    return boto3.client("ssm").get_parameter(
        Name=os.environ[parameter_env], WithDecryption=True
    )["Parameter"]["Value"]


def _run_id(context):
    return (
        context["dag_run"]
        .run_after.astimezone(timezone.utc)
        .strftime("%Y%m%dT%H%M%S%fZ")
    )


def _silver(**context):
    poller_result = context["ti"].xcom_pull(task_ids="poller")
    run_id = poller_result["run_id"]
    application_id = os.environ["SPARK_APPLICATION_ID"]
    log_uri = f"s3://{os.environ['ARTIFACTS_BUCKET']}/pipeline-logs/{run_id}/silver/"
    emr = boto3.client("emr-serverless")
    job_run_id = emr.start_job_run(
        applicationId=application_id,
        executionRoleArn=os.environ["SPARK_EXECUTION_ROLE_ARN"],
        name=f"silver-{run_id}",
        executionTimeoutMinutes=(
            120 if context["params"]["mode"] == "backfill" else 30
        ),
        jobDriver={
            "sparkSubmit": {
                "entryPoint": os.environ["SILVER_JOB_S3_URI"],
                "entryPointArguments": [
                    json.dumps(
                        {
                            "run_id": run_id,
                            "mode": context["params"]["mode"],
                            "stage1_artifact_uri": poller_result["artifact_uri"],
                            "artifacts_bucket": os.environ["ARTIFACTS_BUCKET"],
                            "cloudwatch_namespace": os.environ["CLOUDWATCH_NAMESPACE"],
                            "snowflake_password_ssm_parameter": os.environ[
                                "SNOWFLAKE_PASSWORD_SSM_PARAMETER"
                            ],
                            "snowflake_options": {
                                "sfURL": os.environ["SNOWFLAKE_URL"],
                                "sfUser": os.environ["SNOWFLAKE_USER"],
                                "sfDatabase": os.environ["SNOWFLAKE_DATABASE"],
                                "sfSchema": os.environ["DBT_SILVER_SCHEMA"],
                                "sfWarehouse": os.environ["SNOWFLAKE_WAREHOUSE"],
                                "sfRole": os.environ["SNOWFLAKE_ROLE"],
                            },
                        }
                    )
                ],
                "sparkSubmitParameters": (
                    "--conf spark.executor.instances=1 "
                    "--conf spark.executor.cores=2 "
                    "--conf spark.executor.memory=4g "
                    "--conf spark.driver.cores=2 "
                    "--conf spark.driver.memory=4g "
                    "--conf spark.sql.session.timeZone=UTC "
                    f"--conf spark.emr-serverless.driverEnv.AWS_DEFAULT_REGION={os.environ['AWS_REGION']} "
                    "--conf spark.emr-serverless.driverEnv.PYSPARK_DRIVER_PYTHON=/opt/silver-venv/bin/python3 "
                    "--conf spark.emr-serverless.driverEnv.PYSPARK_PYTHON=/opt/silver-venv/bin/python3 "
                    "--conf spark.executorEnv.PYSPARK_PYTHON=/opt/silver-venv/bin/python3 "
                    "--jars /usr/lib/spark/jars/spark-snowflake_2.12-3.2.2-spark_3.5.jar,"
                    "/usr/lib/spark/jars/snowflake-jdbc-4.1.0.jar"
                ),
            }
        },
        configurationOverrides={
            "monitoringConfiguration": {
                "s3MonitoringConfiguration": {"logUri": log_uri}
            }
        },
    )["jobRunId"]
    job = {"state": "SUBMITTED"}
    try:
        while True:
            job = emr.get_job_run(applicationId=application_id, jobRunId=job_run_id)[
                "jobRun"
            ]
            if job["state"] == "SUCCESS":
                return {
                    "run_id": run_id,
                    "status": poller_result["status"],
                    "artifact_uri": (
                        f"s3://{os.environ['ARTIFACTS_BUCKET']}/"
                        f"pipeline-artifacts/{run_id}/silver/output.parquet"
                    ),
                    "application_id": application_id,
                    "job_run_id": job_run_id,
                }
            if job["state"] in {"FAILED", "CANCELLED"}:
                raise RuntimeError(
                    f"EMR {application_id} job {job_run_id}: {job['state']}; "
                    f"{job.get('stateDetails', '')}; logs: {log_uri}"
                )
            time.sleep(30)
    except BaseException:
        if job["state"] not in {"SUCCESS", "FAILED", "CANCELLED"}:
            emr.cancel_job_run(applicationId=application_id, jobRunId=job_run_id)
        raise


def _gold(**context):
    silver_result = context["ti"].xcom_pull(task_ids="spark_silver")
    started_at = monotonic()
    status = "fail"
    cloudwatch = boto3.client("cloudwatch")
    _gold_metrics(
        cloudwatch,
        [
            {"MetricName": "job_start_count", "Value": 1, "Unit": "Count"},
            {"MetricName": "inflight_job_count", "Value": 1, "Unit": "Count"},
        ],
    )
    try:
        password = _secret("SNOWFLAKE_PASSWORD_SSM_PARAMETER")
        project = str(Path(__file__).resolve().parents[1] / "dbt")
        command = [
            "dbt",
            "build",
            "--project-dir",
            project,
            "--profiles-dir",
            project,
        ]
        if context["params"]["mode"] == "backfill":
            command.append("--full-refresh")
        try:
            result = subprocess.run(
                command,
                env={**os.environ, "SNOWFLAKE_PASSWORD": password},
                timeout=30 * 60,
                capture_output=True,
                text=True,
                check=False,
            )
        except subprocess.TimeoutExpired:
            raise RuntimeError("dbt Gold exceeded 30 minutes") from None
        if result.returncode:
            details = (result.stderr or result.stdout).replace(password, "***")
            raise RuntimeError(f"dbt Gold exited {result.returncode}: {details}")
        status = "success"
        return {"run_id": silver_result["run_id"], "status": status}
    finally:
        _gold_metrics(
            cloudwatch,
            [
                {"MetricName": "inflight_job_count", "Value": 0, "Unit": "Count"},
                {
                    "MetricName": "job_duration",
                    "Value": monotonic() - started_at,
                    "Unit": "Seconds",
                },
                {
                    "MetricName": "job_finish_status",
                    "Value": 1,
                    "Unit": "Count",
                    "Dimensions": [{"Name": "status", "Value": status}],
                },
            ],
        )


def _gold_metrics(cloudwatch, metrics):
    for metric in metrics:
        metric.setdefault("Dimensions", []).append({"Name": "stage", "Value": "gold"})
    cloudwatch.put_metric_data(
        Namespace=os.environ["CLOUDWATCH_NAMESPACE"], MetricData=metrics
    )


with DAG(
    dag_id="elt_repo_contribution",
    schedule=os.environ.get("INGESTION_CADENCE", "0 */6 * * *"),
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    params={
        "mode": Param("incremental", type="string", enum=["incremental", "backfill"])
    },
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "retry_exponential_backoff": True,
        "max_retry_delay": timedelta(minutes=30),
    },
    tags=["elt", "repo-contribution"],
) as dag:
    poll = PythonOperator(
        task_id="poller", python_callable=_poll, show_return_value_in_logs=False
    )
    silver = PythonOperator(
        task_id="spark_silver", python_callable=_silver, show_return_value_in_logs=False
    )
    gold = PythonOperator(
        task_id="dbt_gold",
        python_callable=_gold,
        execution_timeout=timedelta(minutes=30),
        show_return_value_in_logs=False,
    )
    poll >> silver >> gold
