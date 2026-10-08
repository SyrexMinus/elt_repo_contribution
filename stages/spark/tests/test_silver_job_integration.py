from __future__ import annotations

import io
import json
import os
import uuid
from collections import Counter
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from typing import Any
from unittest import TestCase

import boto3
import pyarrow as arrow
import requests
from pyarrow import parquet
from pyspark.sql import SparkSession

from stages.poller.poller import poller
from stages.spark import silver_job
from stages.spark.tests.snowflake_test_helpers import set_up_snowflake

REPOSITORY = "salute-developers/smart_app_framework"
BRONZE_BUCKET = "bronze-bucket"
ARTIFACTS_BUCKET = "artifacts-bucket"
CLOUDWATCH_NAMESPACE = "RepoContribution/IntegrationTest"
SNOWFLAKE_PASSWORD_SSM_PARAMETER = "/test/snowflake-password"


class SilverJobIntegrationTest(TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        set_up_snowflake(
            test_case=cls,
            database=os.environ["SNOWFLAKE_DATABASE"],
            silver_schema=f"SILVER_TEST_{uuid.uuid4().hex.upper()}",
        )

        endpoint = os.environ["LOCALSTACK_ENDPOINT"]
        client_options = {
            "endpoint_url": endpoint,
            "aws_access_key_id": "test",
            "aws_secret_access_key": "test",
            "region_name": "us-east-1",
        }
        cls.s3 = boto3.client("s3", **client_options)
        cls.cloudwatch_client = boto3.client("cloudwatch", **client_options)
        cls.ssm = _SSM(os.environ["SNOWFLAKE_PASSWORD"])
        cls.s3.create_bucket(Bucket=BRONZE_BUCKET)
        cls.s3.create_bucket(Bucket=ARTIFACTS_BUCKET)

        cls.spark = (
            SparkSession.builder.master("local[2]")
            .appName("silver-job-live-integration-test")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.endpoint", endpoint)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
            .config("spark.hadoop.fs.s3a.access.key", "test")
            .config("spark.hadoop.fs.s3a.secret.key", "test")
            .config(
                "spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
            )
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("ERROR")
        cls.addClassCleanup(cls.spark.stop)

    def test_silver_writes_live_backfill_to_snowflake(self) -> None:
        started_at = datetime.now(timezone.utc)
        initial_run_id = _run_id(started_at)
        snapshot_update_run_id = _run_id(started_at + timedelta(seconds=1))
        cloudwatch = _RecordingCloudWatch(self.cloudwatch_client)

        with requests.Session() as github:
            poller_result = poller(
                event={
                    "repositories": [REPOSITORY],
                    "github_token": os.environ["GITHUB_TOKEN"],
                    "github_api_url": "https://api.github.com",
                    "mode": "backfill",
                    "run_id": initial_run_id,
                    "bronze_bucket": BRONZE_BUCKET,
                    "artifacts_bucket": ARTIFACTS_BUCKET,
                    "cloudwatch_namespace": CLOUDWATCH_NAMESPACE,
                    "batch_max_pull_requests": 10,
                    "batch_max_bytes": 10 * 1024 * 1024,
                    "request_timeout_seconds": 30,
                },
                s3=self.s3,
                github=github,
                cloudwatch=cloudwatch,
            )

        bronze_recs = _bronze_records(s3=self.s3, run_id=initial_run_id)
        bronze_source_counts = Counter(record["entity"] for record in bronze_recs)
        deduplicated_counts = _deduplicated_counts(bronze_recs)
        initial_event = self._event(
            run_id=initial_run_id,
            artifact_uri=poller_result["artifact_uri"],
        )
        silver_metrics_offset = len(cloudwatch.calls)

        initial_result = silver_job.silver(
            event=initial_event,
            spark=self.spark,
            s3=self.s3,
            ssm=self.ssm,
            cloudwatch=cloudwatch,
        )
        snowflake_counts_after_insert = {
            table: _count(connection=self.snowflake, table=table)
            for table in ("PULL", "FILE", "REVIEW")
        }

        with self.subTest("Backfill writes deduplicated counts"):
            self.assertEqual(initial_result["status"], "success")
            self.assertGreaterEqual(snowflake_counts_after_insert["PULL"], 230)
            self.assertGreaterEqual(snowflake_counts_after_insert["FILE"], 230)
            self.assertEqual(
                {
                    table.lower(): count
                    for table, count in snowflake_counts_after_insert.items()
                },
                deduplicated_counts,
            )

        with self.subTest("Backfill writes stable GitHub rows"):
            self.assertEqual(
                _iso_row(
                    _query_one(
                        connection=self.snowflake,
                        sql="SELECT REPO, PR_NUM, STATE, TITLE, USER_LOGIN, "
                        "CREATED_AT, UPDATED_AT, MERGED_AT, PARSE_RUN_ID "
                        f"FROM {self.database}.{self.silver_schema}.PULL WHERE PR_NUM = 1",
                    )
                ),
                (
                    REPOSITORY,
                    1,
                    "closed",
                    "Feature/new metrics",
                    "intsyn",
                    "2022-04-20T13:15:37",
                    "2022-05-16T06:44:31",
                    "2022-05-16T06:44:31",
                    initial_run_id,
                ),
            )
            self.assertEqual(
                _query_one(
                    connection=self.snowflake,
                    sql="SELECT FILENAME, ADDITIONS, DELETIONS "
                    f"FROM {self.database}.{self.silver_schema}.FILE "
                    "WHERE PR_NUM = 210 "
                    "AND FILENAME = 'core/utils/stats_timer.py'",
                ),
                ("core/utils/stats_timer.py", 53, 1),
            )
            self.assertEqual(
                _query_one(
                    connection=self.snowflake,
                    sql="SELECT USER_LOGIN, STATE "
                    f"FROM {self.database}.{self.silver_schema}.REVIEW "
                    "WHERE PR_NUM = 210 AND USER_LOGIN = 'dangerink'",
                ),
                ("dangerink", "APPROVED"),
            )

        with self.subTest("Backfill writes artifact and metrics"):
            self.assertEqual(
                {
                    record["table"]: {
                        "source": record["source_row_count"],
                        "output": record["output_row_count"],
                        "corrupted": record["corrupted_row_count"],
                    }
                    for record in _artifact_records(
                        s3=self.s3,
                        run_id=initial_run_id,
                    )
                },
                {
                    table: {
                        "source": bronze_source_counts[table],
                        "output": deduplicated_counts[table],
                        "corrupted": 0,
                    }
                    for table in ("pull", "file", "review")
                },
            )
            _assert_success_metrics(
                test=self,
                calls=cloudwatch.calls[silver_metrics_offset:],
            )

        with self.subTest("Repeated event does not duplicate rows"):
            silver_job.silver(
                event=initial_event,
                spark=self.spark,
                s3=self.s3,
                ssm=self.ssm,
                cloudwatch=cloudwatch,
            )
            self.assertEqual(
                {
                    table: _count(connection=self.snowflake, table=table)
                    for table in ("PULL", "FILE", "REVIEW")
                },
                snowflake_counts_after_insert,
            )
        snapshot_pr_num = 210
        source_pull_rec = next(
            record
            for record in bronze_recs
            if record["entity"] == "pull" and record["pr_num"] == snapshot_pr_num
        )
        unrelated_child_rows = _child_rows(
            connection=self.snowflake,
            database=self.database,
            silver_schema=self.silver_schema,
            pr_num=1,
        )

        with self.subTest("Newer snapshot updates pull and replaces child rows"):
            snapshot_update_event = self._event(
                run_id=snapshot_update_run_id,
                artifact_uri=_store_snapshot(
                    s3=self.s3,
                    run_id=snapshot_update_run_id,
                    source_pull_rec=source_pull_rec,
                    title="Complete snapshot update",
                    updated_at="2026-09-27T11:00:00Z",
                    files=[("src/kept.py", 0, 7, "removed")],
                    reviews=[
                        ("kept-reviewer", "APPROVED", "2026-09-27T11:01:00Z")
                    ],
                ),
            )
            silver_job.silver(
                event=snapshot_update_event,
                spark=self.spark,
                s3=self.s3,
                ssm=self.ssm,
                cloudwatch=cloudwatch,
            )
            self.assertEqual(
                _pull_version(
                    connection=self.snowflake,
                    database=self.database,
                    silver_schema=self.silver_schema,
                    pr_num=snapshot_pr_num,
                ),
                ("Complete snapshot update", snapshot_update_run_id),
            )
            self.assertEqual(
                _child_rows(
                    connection=self.snowflake,
                    database=self.database,
                    silver_schema=self.silver_schema,
                    pr_num=snapshot_pr_num,
                ),
                {
                    "file": [("src/kept.py", 0, 7, snapshot_update_run_id)],
                    "review": [
                        ("kept-reviewer", "APPROVED", snapshot_update_run_id)
                    ],
                },
            )

        with self.subTest("Older snapshot does not restore replaced child rows"):
            silver_job.silver(
                event=initial_event,
                spark=self.spark,
                s3=self.s3,
                ssm=self.ssm,
                cloudwatch=cloudwatch,
            )
            self.assertEqual(
                _pull_version(
                    connection=self.snowflake,
                    database=self.database,
                    silver_schema=self.silver_schema,
                    pr_num=snapshot_pr_num,
                ),
                ("Complete snapshot update", snapshot_update_run_id),
            )
            self.assertEqual(
                _child_rows(
                    connection=self.snowflake,
                    database=self.database,
                    silver_schema=self.silver_schema,
                    pr_num=snapshot_pr_num,
                ),
                {
                    "file": [("src/kept.py", 0, 7, snapshot_update_run_id)],
                    "review": [
                        ("kept-reviewer", "APPROVED", snapshot_update_run_id)
                    ],
                },
            )
            self.assertEqual(
                _child_rows(
                    connection=self.snowflake,
                    database=self.database,
                    silver_schema=self.silver_schema,
                    pr_num=1,
                ),
                unrelated_child_rows,
            )

    def _event(self, run_id: str, artifact_uri: str) -> dict[str, Any]:
        return {
            "run_id": run_id,
            "stage1_artifact_uri": artifact_uri,
            "artifacts_bucket": ARTIFACTS_BUCKET,
            "cloudwatch_namespace": CLOUDWATCH_NAMESPACE,
            "snowflake_password_ssm_parameter": (
                SNOWFLAKE_PASSWORD_SSM_PARAMETER
            ),
            "snowflake_options": {
                "sfURL": os.environ["SNOWFLAKE_URL"],
                "sfUser": os.environ["SNOWFLAKE_USER"],
                "sfDatabase": self.database,
                "sfSchema": self.silver_schema,
                "sfWarehouse": os.environ["SNOWFLAKE_WAREHOUSE"],
                "sfRole": os.environ["SNOWFLAKE_ROLE"],
            },
        }

def _run_id(value: datetime) -> str:
    return value.strftime("%Y%m%dT%H%M%S%fZ")


def _bronze_records(s3, run_id: str) -> list[dict[str, Any]]:
    prefix = f"{REPOSITORY}/{run_id}/"
    bronze_recs = []
    reply = s3.list_objects_v2(Bucket=BRONZE_BUCKET, Prefix=prefix)
    for item in sorted(reply.get("Contents", []), key=lambda item: item["Key"]):
        body = s3.get_object(Bucket=BRONZE_BUCKET, Key=item["Key"])["Body"].read()
        bronze_recs.extend(json.loads(line) for line in body.splitlines())
    return bronze_recs


def _deduplicated_counts(bronze_recs: list[dict[str, Any]]) -> dict[str, int]:
    deduplicated_keys = {
        "pull": {
            (record["repo"], record["pr_num"])
            for record in bronze_recs
            if record["entity"] == "pull"
        },
        "file": {
            (
                record["repo"],
                record["pr_num"],
                record["payload"]["filename"],
            )
            for record in bronze_recs
            if record["entity"] == "file"
        },
        "review": {
            (
                record["repo"],
                record["pr_num"],
                record["payload"]["user"]["login"],
            )
            for record in bronze_recs
            if record["entity"] == "review"
        },
    }
    return {name: len(values) for name, values in deduplicated_keys.items()}


def _artifact_records(s3, run_id: str) -> list[dict[str, Any]]:
    key = f"pipeline-artifacts/{run_id}/silver/output.parquet"
    artifact_body = s3.get_object(Bucket=ARTIFACTS_BUCKET, Key=key)["Body"].read()
    return parquet.read_table(io.BytesIO(artifact_body)).to_pylist()


def _store_snapshot(
    s3,
    run_id: str,
    source_pull_rec: dict[str, Any],
    title: str,
    updated_at: str,
    files: list[tuple[str, int, int, str]],
    reviews: list[tuple[str, str, str]],
) -> str:
    pull_rec = deepcopy(source_pull_rec)
    pull_rec["run_id"] = run_id
    pull_rec["produced_new_snapshot"] = True
    pull_rec["payload"]["title"] = title
    pull_rec["payload"]["updated_at"] = updated_at
    bronze_recs = [pull_rec]
    bronze_recs.extend(
        {
            "run_id": run_id,
            "repo": REPOSITORY,
            "pr_num": pull_rec["pr_num"],
            "entity": "file",
            "payload": {
                "filename": filename,
                "additions": additions,
                "deletions": deletions,
                "status": status,
            },
        }
        for filename, additions, deletions, status in files
    )
    bronze_recs.extend(
        {
            "run_id": run_id,
            "repo": REPOSITORY,
            "pr_num": pull_rec["pr_num"],
            "entity": "review",
            "payload": {
                "user": {"login": user_login},
                "state": state,
                "submitted_at": submitted_at,
            },
        }
        for user_login, state, submitted_at in reviews
    )
    return _store_increment(s3=s3, run_id=run_id, bronze_recs=bronze_recs)


def _store_increment(
    s3,
    run_id: str,
    bronze_recs: list[dict[str, Any]],
) -> str:
    bronze_key = f"{REPOSITORY}/{run_id}/batch-00000.jsonl"
    s3.put_object(
        Bucket=BRONZE_BUCKET,
        Key=bronze_key,
        Body="".join(json.dumps(record) + "\n" for record in bronze_recs).encode(),
        ContentType="application/x-ndjson",
    )
    manifest_key = f"pipeline-artifacts/{run_id}/stage1/output.parquet"
    manifest_table = arrow.Table.from_pylist(
        [
            {
                "run_id": run_id,
                "repo": REPOSITORY,
                "s3_uri": f"s3://{BRONZE_BUCKET}/{bronze_key}",
                "pull_request_count": sum(
                    record["entity"] == "pull" for record in bronze_recs
                ),
                "record_count": len(bronze_recs),
            }
        ],
        schema=arrow.schema(
            [
                ("run_id", arrow.string()),
                ("repo", arrow.string()),
                ("s3_uri", arrow.string()),
                ("pull_request_count", arrow.int64()),
                ("record_count", arrow.int64()),
            ]
        ),
    )
    manifest_body = io.BytesIO()
    parquet.write_table(manifest_table, manifest_body)
    s3.put_object(
        Bucket=ARTIFACTS_BUCKET,
        Key=manifest_key,
        Body=manifest_body.getvalue(),
        ContentType="application/vnd.apache.parquet",
    )
    return f"s3://{ARTIFACTS_BUCKET}/{manifest_key}"


def _count(connection, table: str) -> int:
    return _query_one(
        connection=connection,
        sql=f"SELECT COUNT(*) FROM {SilverJobIntegrationTest.database}."
        f"{SilverJobIntegrationTest.silver_schema}.{table}",
    )[0]


def _query_one(connection, sql: str) -> tuple[Any, ...]:
    with connection.cursor() as cursor:
        cursor.execute(sql)
        row = cursor.fetchone()
    if row is None:
        raise AssertionError(f"query returned no rows: {sql}")
    return tuple(row)


def _child_rows(
    connection,
    database: str,
    silver_schema: str,
    pr_num: int,
) -> dict[str, list[tuple[Any, ...]]]:
    prefix = f"{database}.{silver_schema}"
    return {
        "file": _query_all(
            connection=connection,
            sql="SELECT FILENAME, ADDITIONS, DELETIONS, PARSE_RUN_ID "
            f"FROM {prefix}.FILE WHERE PR_NUM = {pr_num} ORDER BY FILENAME",
        ),
        "review": _query_all(
            connection=connection,
            sql="SELECT USER_LOGIN, STATE, PARSE_RUN_ID "
            f"FROM {prefix}.REVIEW WHERE PR_NUM = {pr_num} ORDER BY USER_LOGIN",
        ),
    }


def _query_all(connection, sql: str) -> list[tuple[Any, ...]]:
    with connection.cursor() as cursor:
        cursor.execute(sql)
        return [tuple(row) for row in cursor.fetchall()]


def _pull_version(
    connection,
    database: str,
    silver_schema: str,
    pr_num: int,
) -> tuple[Any, ...]:
    return _query_one(
        connection=connection,
        sql="SELECT TITLE, PARSE_RUN_ID "
        f"FROM {database}.{silver_schema}.PULL WHERE PR_NUM = {pr_num}",
    )


def _iso_row(row: tuple[Any, ...]) -> tuple[Any, ...]:
    return tuple(
        value.isoformat() if isinstance(value, datetime) else value for value in row
    )


def _assert_success_metrics(
    test: TestCase,
    calls: list[dict[str, Any]],
) -> None:
    metrics = [metric for call in calls for metric in call["MetricData"]]
    test.assertEqual(
        [
            metric["Value"]
            for metric in metrics
            if metric["MetricName"] == "job_start_count"
        ],
        [1],
    )
    test.assertEqual(
        [
            metric["Value"]
            for metric in metrics
            if metric["MetricName"] == "inflight_job_count"
        ],
        [1, 0],
    )
    finish = [
        metric for metric in metrics if metric["MetricName"] == "job_finish_status"
    ]
    test.assertEqual(len(finish), 1)
    for metric in metrics:
        test.assertIn(
            {"Name": "stage", "Value": "silver"},
            metric["Dimensions"],
        )
    test.assertIn(
        {"Name": "status", "Value": "success"},
        finish[0]["Dimensions"],
    )


class _RecordingCloudWatch:
    def __init__(self, client) -> None:
        self.client = client
        self.calls: list[dict[str, Any]] = []

    def put_metric_data(self, **kwargs: Any) -> Any:
        self.calls.append(kwargs)
        return self.client.put_metric_data(**kwargs)


class _SSM:
    def __init__(self, value: str) -> None:
        self.value = value

    def get_parameter(self, **_: Any) -> dict[str, dict[str, str]]:
        return {"Parameter": {"Value": self.value}}
