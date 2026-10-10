from __future__ import annotations

import io
import json
import os
import subprocess
from collections.abc import Iterator
from contextlib import ExitStack, contextmanager
from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Any, Self
from unittest import TestCase
from unittest.mock import patch
from urllib.parse import urlsplit

from pyarrow import parquet

from stages.airflow_dags import elt_dag

REPOSITORY = "salute-developers/smart_app_framework"
ARTIFACTS_BUCKET = "artifacts-bucket"
GITHUB_TOKEN = "github-secret"
SNOWFLAKE_PASSWORD = "snowflake-secret"
GITHUB_TOKEN_SSM_PARAMETER = "/test/github-token"
SNOWFLAKE_PASSWORD_SSM_PARAMETER = "/test/snowflake-password"
RUN_ID = "20260930T080706123456Z"


class EltDagTest(TestCase):
    def test_elt_repo_contribution_continues_empty_increment(self) -> None:
        state = {
            "last_successful_watermark": "2099-01-01T00:00:00Z",
            "pull_requests": {},
        }
        github = _GitHub(
            {
                _path("pulls"): [
                    _Reply(
                        [
                            _pull(
                                number=1,
                                updated_at="2026-09-29T00:00:00Z",
                            )
                        ]
                    )
                ]
            }
        )

        with _mock_runtime(
            github=github,
            state=state,
            emr_states=["PENDING", "RUNNING", "SUCCESS"],
        ) as runtime:
            self._assert_dag_contract()
            context = _context(mode="incremental")
            poller_result = elt_dag._poll(**context)
            context["ti"].values["poller"] = poller_result
            silver_result = elt_dag._silver(**context)
            context["ti"].values["spark_silver"] = silver_result
            gold_result = elt_dag._gold(**context)
            context["ti"].values["dbt_gold"] = gold_result

        self.assertEqual(poller_result["status"], "empty")
        self.assertEqual(poller_result["run_id"], RUN_ID)
        self.assertEqual(
            _artifact_records(
                runtime.s3,
                f"pipeline-artifacts/{RUN_ID}/bronze/output.parquet",
            ),
            [],
        )
        self.assertEqual(github.calls, [_path("pulls")])
        self.assertEqual(len(runtime.emr.start_calls), 1)
        emr_call = runtime.emr.start_calls[0]
        self.assertEqual(emr_call["executionTimeoutMinutes"], 30)
        silver_event = _silver_event(emr_call)
        self.assertEqual(silver_event["mode"], "incremental")
        self.assertEqual(silver_event["run_id"], RUN_ID)
        self.assertEqual(
            silver_event["stage1_artifact_uri"],
            poller_result["artifact_uri"],
        )
        self.assertEqual(
            silver_event["snowflake_password_ssm_parameter"],
            SNOWFLAKE_PASSWORD_SSM_PARAMETER,
        )
        self.assertNotIn(SNOWFLAKE_PASSWORD, json.dumps(emr_call))
        self.assertEqual(len(runtime.subprocess.calls), 1)
        command, options = runtime.subprocess.calls[0]
        self.assertEqual(options["timeout"], 30 * 60)
        self.assertEqual(
            options["env"]["SNOWFLAKE_PASSWORD"],
            SNOWFLAKE_PASSWORD,
        )
        self.assertNotIn(SNOWFLAKE_PASSWORD, command)
        self.assertNotIn(
            GITHUB_TOKEN,
            json.dumps(context["ti"].values, default=str),
        )
        self.assertNotIn(
            SNOWFLAKE_PASSWORD,
            json.dumps(context["ti"].values, default=str),
        )
        self.assertEqual(gold_result["status"], "success")
        self._assert_gold_metrics(runtime.cloudwatch.calls)

    def test_elt_repo_contribution_rebuilds_gold_on_backfill(self) -> None:
        github = _GitHub(
            {
                _path("pulls"): [
                    _Reply(
                        [
                            _pull(
                                number=210,
                                state="closed",
                                title="PNLP-12553: add stats",
                                created_at="2025-02-05T14:10:00Z",
                                updated_at="2025-02-06T11:56:01Z",
                                merged_at="2025-02-06T11:56:01Z",
                                user_login="SyrexMinus",
                            ),
                            _pull(
                                number=1,
                                state="closed",
                                title="Feature/new metrics",
                                created_at="2022-04-20T13:15:37Z",
                                updated_at="2022-05-16T06:44:31Z",
                                merged_at="2022-05-16T06:44:31Z",
                                user_login="intsyn",
                            ),
                        ]
                    )
                ],
                _path("pulls/1/files"): [
                    _Reply(
                        [
                            {
                                "filename": "README.md",
                                "additions": 4,
                                "deletions": 1,
                                "status": "modified",
                            }
                        ]
                    )
                ],
                _path("pulls/1/reviews"): [
                    _Reply(
                        [
                            {
                                "id": 111,
                                "user": {"login": "octocat"},
                                "state": "COMMENTED",
                                "submitted_at": "2022-05-15T18:00:00Z",
                                "commit_id": "ef10d889c11f8d39f541f02de62d6d4f1bd9c4b8",
                            }
                        ]
                    )
                ],
                _path("pulls/210/files"): [
                    _Reply(
                        [
                            {
                                "filename": "core/utils/stats_timer.py",
                                "additions": 53,
                                "deletions": 1,
                                "status": "modified",
                            }
                        ]
                    )
                ],
                _path("pulls/210/reviews"): [
                    _Reply(
                        [
                            {
                                "id": 2596185857,
                                "user": {"login": "dangerink"},
                                "state": "APPROVED",
                                "submitted_at": "2025-02-05T15:33:26Z",
                                "commit_id": "61689e3ecae92d23324eee2f01f64a1c4ef42095",
                            }
                        ]
                    )
                ],
            }
        )

        with _mock_runtime(
            github=github,
            emr_states=["SUCCESS"],
        ) as runtime:
            context = _context(mode="backfill")
            context["logical_date"] = datetime(2026, 9, 29, tzinfo=timezone.utc)
            context["dag_run"].logical_date = context["logical_date"]
            poller_result = elt_dag._poll(**context)
            context["ti"].values["poller"] = poller_result
            silver_result = elt_dag._silver(**context)
            context["ti"].values["spark_silver"] = silver_result
            gold_result = elt_dag._gold(**context)

        self.assertEqual(
            poller_result,
            {
                "run_id": RUN_ID,
                "status": "success",
                "artifact_uri": (
                    f"s3://{ARTIFACTS_BUCKET}/pipeline-artifacts/"
                    f"{RUN_ID}/bronze/output.parquet"
                ),
                "bronze_objects": 1,
                "pull_requests": 2,
                "corrupted_count": 0,
            },
        )
        self.assertEqual(
            _artifact_records(
                runtime.s3,
                f"pipeline-artifacts/{RUN_ID}/bronze/output.parquet",
            ),
            [
                {
                    "run_id": RUN_ID,
                    "repo": REPOSITORY,
                    "s3_uri": (
                        f"s3://bronze-bucket/{REPOSITORY}/{RUN_ID}/batch-00000.jsonl"
                    ),
                    "pull_request_count": 2,
                    "record_count": 6,
                }
            ],
        )
        self.assertEqual(
            [
                (
                    record["entity"],
                    record["pr_num"],
                    record["payload"].get("filename")
                    or record["payload"].get("user", {}).get("login"),
                )
                for record in _bronze_records(runtime.s3, poller_result)
            ],
            [
                ("pull", 210, "SyrexMinus"),
                ("file", 210, "core/utils/stats_timer.py"),
                ("review", 210, "dangerink"),
                ("pull", 1, "intsyn"),
                ("file", 1, "README.md"),
                ("review", 1, "octocat"),
            ],
        )
        emr_call = runtime.emr.start_calls[0]
        self.assertEqual(emr_call["executionTimeoutMinutes"], 120)
        silver_event = _silver_event(emr_call)
        self.assertEqual(silver_event["mode"], "backfill")
        self.assertEqual(silver_event["run_id"], RUN_ID)
        self.assertEqual(
            silver_result,
            {
                "run_id": RUN_ID,
                "status": "success",
                "artifact_uri": (
                    f"s3://{ARTIFACTS_BUCKET}/pipeline-artifacts/"
                    f"{RUN_ID}/silver/output.parquet"
                ),
                "application_id": "application-1",
                "job_run_id": "job-1",
            },
        )
        command, _ = runtime.subprocess.calls[0]
        self.assertIn("--full-refresh", command)
        self.assertEqual(
            gold_result,
            {"run_id": RUN_ID, "status": "success"},
        )
        self._assert_gold_metrics(runtime.cloudwatch.calls)

    def test_elt_repo_contribution_stops_gold_on_silver_failure(self) -> None:
        state = {
            "last_successful_watermark": "2099-01-01T00:00:00Z",
            "pull_requests": {},
        }
        github = _GitHub(
            {
                _path("pulls"): [
                    _Reply(
                        [
                            _pull(
                                number=1,
                                updated_at="2026-09-29T00:00:00Z",
                            )
                        ]
                    )
                ]
            }
        )

        with _mock_runtime(
            github=github,
            state=state,
            emr_states=["PENDING", "RUNNING", "FAILED"],
            emr_state_details="driver failed",
        ):
            context = _context(mode="incremental")
            context["ti"].values["poller"] = elt_dag._poll(**context)
            with self.assertRaisesRegex(
                RuntimeError,
                (
                    "application-1.*job-1.*FAILED.*driver failed.*"
                    f"s3://{ARTIFACTS_BUCKET}/pipeline-logs/{RUN_ID}/silver/"
                ),
            ) as raised:
                elt_dag._silver(**context)

        self.assertEqual(
            elt_dag.dag.get_task("dbt_gold").trigger_rule,
            "all_success",
        )
        self.assertEqual(
            elt_dag.dag.get_task("dbt_gold").upstream_task_ids,
            {"spark_silver"},
        )
        self.assertNotIn(GITHUB_TOKEN, str(raised.exception))
        self.assertNotIn(SNOWFLAKE_PASSWORD, str(raised.exception))

    def _assert_dag_contract(self) -> None:
        self.assertEqual(
            set(elt_dag.dag.task_dict),
            {"poller", "spark_silver", "dbt_gold"},
        )
        self.assertEqual(
            elt_dag.dag.get_task("poller").downstream_task_ids,
            {"spark_silver"},
        )
        self.assertEqual(
            elt_dag.dag.get_task("spark_silver").downstream_task_ids,
            {"dbt_gold"},
        )
        self.assertEqual(elt_dag.dag.params["mode"], "incremental")

    def _assert_gold_metrics(self, calls: list[dict[str, Any]]) -> None:
        metrics = [metric for call in calls for metric in call["MetricData"]]
        finish = [
            metric
            for metric in metrics
            if metric["MetricName"] == "job_finish_status"
            and {"Name": "stage", "Value": "gold"} in metric["Dimensions"]
        ]
        self.assertEqual(len(finish), 1)
        self.assertIn(
            {"Name": "status", "Value": "success"},
            finish[0]["Dimensions"],
        )


@contextmanager
def _mock_runtime(
    github: _GitHub,
    emr_states: list[str],
    state: dict[str, Any] | None = None,
    emr_state_details: str = "",
) -> Iterator[SimpleNamespace]:
    state_objects = (
        {
            (
                ARTIFACTS_BUCKET,
                f"poller-state/{REPOSITORY}.json",
            ): json.dumps(state).encode()
        }
        if state
        else {}
    )
    s3 = _S3(state_objects)
    ssm = _SSM(
        {
            GITHUB_TOKEN_SSM_PARAMETER: GITHUB_TOKEN,
            SNOWFLAKE_PASSWORD_SSM_PARAMETER: SNOWFLAKE_PASSWORD,
        }
    )
    cloudwatch = _CloudWatch()
    emr = _EMR(states=emr_states, state_details=emr_state_details)
    process = _Subprocess()
    clients = _Boto3(
        {
            "cloudwatch": cloudwatch,
            "emr-serverless": emr,
            "s3": s3,
            "ssm": ssm,
        }
    )
    runtime = SimpleNamespace(
        cloudwatch=cloudwatch,
        emr=emr,
        s3=s3,
        ssm=ssm,
        subprocess=process,
    )

    with ExitStack() as stack:
        stack.enter_context(patch.dict(os.environ, _environment(), clear=False))
        stack.enter_context(patch.object(elt_dag, "boto3", clients, create=True))
        stack.enter_context(
            patch.object(
                elt_dag,
                "requests",
                _Requests(github),
                create=True,
            )
        )
        stack.enter_context(patch.object(elt_dag, "subprocess", process, create=True))
        stack.enter_context(
            patch.object(
                elt_dag,
                "time",
                SimpleNamespace(sleep=lambda _: None),
                create=True,
            )
        )
        yield runtime


def _context(mode: str) -> dict[str, Any]:
    return {
        "dag_run": SimpleNamespace(
            run_after=datetime(2026, 9, 30, 8, 7, 6, 123456, tzinfo=timezone.utc),
            logical_date=None,
        ),
        "params": {"mode": mode},
        "ti": _TaskInstance(),
    }


def _environment() -> dict[str, str]:
    return {
        "ARTIFACTS_BUCKET": ARTIFACTS_BUCKET,
        "AWS_REGION": "eu-central-1",
        "BRONZE_BUCKET": "bronze-bucket",
        "CLOUDWATCH_NAMESPACE": "EltRepoContribution/Test",
        "DBT_GOLD_SCHEMA": "GOLD",
        "DBT_SILVER_SCHEMA": "SILVER",
        "GITHUB_TOKEN_SSM_PARAMETER": GITHUB_TOKEN_SSM_PARAMETER,
        "REPOSITORIES": REPOSITORY,
        "SILVER_JOB_S3_URI": (f"s3://{ARTIFACTS_BUCKET}/jobs/spark/silver_job.py"),
        "SNOWFLAKE_ACCOUNT": "test-account",
        "SNOWFLAKE_DATABASE": "TEST_DB",
        "SNOWFLAKE_PASSWORD_SSM_PARAMETER": (SNOWFLAKE_PASSWORD_SSM_PARAMETER),
        "SNOWFLAKE_ROLE": "ELT_PIPELINE_ROLE",
        "SNOWFLAKE_URL": "test-account.snowflakecomputing.com",
        "SNOWFLAKE_USER": "ELT_PIPELINE",
        "SNOWFLAKE_WAREHOUSE": "TEST_WH",
        "SPARK_APPLICATION_ID": "application-1",
        "SPARK_EXECUTION_ROLE_ARN": ("arn:aws:iam::123456789012:role/spark-execution"),
    }


def _silver_event(start_call: dict[str, Any]) -> dict[str, Any]:
    arguments = start_call["jobDriver"]["sparkSubmit"]["entryPointArguments"]
    return json.loads(arguments[0])


def _artifact_records(s3: _S3, key: str) -> list[dict[str, Any]]:
    return parquet.read_table(
        io.BytesIO(s3.objects[(ARTIFACTS_BUCKET, key)])
    ).to_pylist()


def _bronze_records(
    s3: _S3,
    poller_result: dict[str, Any],
) -> list[dict[str, Any]]:
    _, manifest_key = _s3_location(poller_result["artifact_uri"])
    records = []
    for artifact in _artifact_records(s3, manifest_key):
        bucket, key = _s3_location(artifact["s3_uri"])
        records.extend(
            json.loads(line) for line in s3.objects[(bucket, key)].splitlines()
        )
    return records


def _s3_location(uri: str) -> tuple[str, str]:
    location = urlsplit(uri)
    return location.netloc, location.path.lstrip("/")


def _path(endpoint: str) -> str:
    return f"/repos/{REPOSITORY}/{endpoint}"


def _pull(
    number: int,
    updated_at: str,
    state: str = "open",
    title: str | None = None,
    created_at: str = "2026-09-28T08:00:00Z",
    merged_at: str | None = None,
    user_login: str | None = None,
) -> dict[str, Any]:
    return {
        "number": number,
        "state": state,
        "created_at": created_at,
        "updated_at": updated_at,
        "merged_at": merged_at,
        "title": title or f"Pull request {number}",
        "user": {"login": user_login or f"author-{number}"},
    }


class _TaskInstance:
    def __init__(self) -> None:
        self.values: dict[str, Any] = {}

    def xcom_pull(self, task_ids: str, **_: Any) -> Any:
        return self.values[task_ids]


class _Reply:
    def __init__(self, payload: Any) -> None:
        self.status_code = 200
        self.headers: dict[str, str] = {}
        self.content = json.dumps(payload).encode()

    def json(self) -> Any:
        return json.loads(self.content)


class _GitHub:
    def __init__(self, routes: dict[str, list[_Reply]]) -> None:
        self.routes = {path: list(replies) for path, replies in routes.items()}
        self.calls: list[str] = []

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_: object) -> None:
        return None

    def get(self, url: str, **_: Any) -> _Reply:
        path = urlsplit(url).path
        self.calls.append(path)
        return self.routes[path].pop(0)


class _Requests:
    def __init__(self, github: _GitHub) -> None:
        self.github = github

    def Session(self) -> _GitHub:
        return self.github


class _S3Exceptions:
    NoSuchKey = KeyError


class _S3:
    exceptions = _S3Exceptions()

    def __init__(self, objects: dict[tuple[str, str], bytes]) -> None:
        self.objects = dict(objects)

    def get_object(self, Bucket: str, Key: str) -> dict[str, io.BytesIO]:
        return {"Body": io.BytesIO(self.objects[(Bucket, Key)])}

    def put_object(
        self,
        Bucket: str,
        Key: str,
        Body: bytes | io.BytesIO,
        **_: Any,
    ) -> None:
        try:
            value = Body.read()
        except AttributeError:
            value = bytes(Body)
        self.objects[(Bucket, Key)] = value


class _SSM:
    def __init__(self, values: dict[str, str]) -> None:
        self.values = values
        self.calls: list[dict[str, Any]] = []

    def get_parameter(self, **kwargs: Any) -> dict[str, dict[str, str]]:
        self.calls.append(kwargs)
        return {"Parameter": {"Value": self.values[kwargs["Name"]]}}


class _CloudWatch:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    def put_metric_data(self, **kwargs: Any) -> None:
        self.calls.append(kwargs)


class _EMR:
    def __init__(self, states: list[str], state_details: str) -> None:
        self.states = list(states)
        self.state_details = state_details
        self.start_calls: list[dict[str, Any]] = []
        self.cancel_calls: list[dict[str, Any]] = []

    def start_job_run(self, **kwargs: Any) -> dict[str, Any]:
        self.start_calls.append(kwargs)
        return {
            "jobRunId": "job-1",
            "ResponseMetadata": {"HTTPStatusCode": 200},
        }

    def get_job_run(self, **_: Any) -> dict[str, dict[str, str]]:
        return {
            "jobRun": {
                "state": self.states.pop(0),
                "stateDetails": self.state_details,
            }
        }

    def cancel_job_run(self, **kwargs: Any) -> dict[str, Any]:
        self.cancel_calls.append(kwargs)
        return {"ResponseMetadata": {"HTTPStatusCode": 200}}


class _Boto3:
    def __init__(self, clients: dict[str, Any]) -> None:
        self.clients = clients

    def client(self, service_name: str, **_: Any) -> Any:
        return self.clients[service_name]


class _Subprocess:
    CalledProcessError = subprocess.CalledProcessError
    TimeoutExpired = subprocess.TimeoutExpired

    def __init__(self) -> None:
        self.calls: list[tuple[list[str], dict[str, Any]]] = []

    def run(self, command: list[str], **kwargs: Any) -> subprocess.CompletedProcess:
        self.calls.append((command, kwargs))
        return subprocess.CompletedProcess(command, 0, stdout="", stderr="")
