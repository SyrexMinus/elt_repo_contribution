from __future__ import annotations

import io
import json
import tempfile
from pathlib import Path
from typing import Any, BinaryIO
from unittest import TestCase
from unittest.mock import patch

import pyarrow as arrow
from pyarrow import parquet
from pyspark.sql import DataFrame, SparkSession

from stages.spark import silver_job

ARTIFACTS_BUCKET = "artifacts-bucket"
CLOUDWATCH_NAMESPACE = "RepoContribution/Test"
REPOSITORY = "example/repository"
RUN_ID = "20260916T120000000000Z"


class SilverJobTest(TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.spark = (
            SparkSession.builder.master("local[2]")
            .appName("silver-job-unit-tests")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.session.timeZone", "UTC")
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls) -> None:
        cls.spark.stop()

    def test_parse_records_builds_rows_and_build_tables_deduplicates_reviews(
        self,
    ) -> None:
        bronze_recs = [
            _record(
                entity="pull",
                payload={
                    "number": 7,
                    "state": "closed",
                    "title": "Typed silver row",
                    "created_at": "2026-09-14T08:00:00Z",
                    "updated_at": "2026-09-15T09:30:00Z",
                    "merged_at": None,
                    "user": {"login": "author"},
                    "ignored": {"future": "field"},
                },
            ),
            _record(
                entity="file",
                payload={
                    "filename": "src/job.py",
                    "additions": 53,
                    "deletions": 1,
                    "status": "modified",
                },
            ),
            _record(
                entity="review",
                payload={
                    "id": 10,
                    "state": "CHANGES_REQUESTED",
                    "submitted_at": "2026-09-14T10:00:00Z",
                    "user": {"login": "reviewer"},
                },
            ),
            _record(
                entity="review",
                payload={
                    "id": 11,
                    "state": "APPROVED",
                    "submitted_at": "2026-09-15T11:00:00Z",
                    "user": {"login": "reviewer"},
                    "body": "Looks good",
                },
            ),
        ]
        bronze_lines = self.spark.createDataFrame(
            [(json.dumps(record),) for record in bronze_recs],
            "value string",
        )

        parsed = silver_job._parse_records(bronze_lines)
        silver_tables = silver_job._build_tables(parsed)

        self.assertEqual(
            {
                name: {
                    field.name: field.dataType.simpleString()
                    for field in frame.schema.fields
                }
                for name, frame in silver_tables.items()
            },
            {
                "pull": {
                    "repo": "string",
                    "pr_num": "bigint",
                    "merged_at": "timestamp",
                    "user_login": "string",
                    "state": "string",
                    "created_at": "timestamp",
                    "updated_at": "timestamp",
                    "title": "string",
                    "parse_run_id": "string",
                },
                "file": {
                    "repo": "string",
                    "pr_num": "bigint",
                    "filename": "string",
                    "additions": "bigint",
                    "deletions": "bigint",
                    "parse_run_id": "string",
                },
                "review": {
                    "repo": "string",
                    "pr_num": "bigint",
                    "user_login": "string",
                    "state": "string",
                    "parse_run_id": "string",
                },
            },
        )
        self.assertEqual(
            _rows(silver_tables["pull"]),
            [
                {
                    "repo": REPOSITORY,
                    "pr_num": 7,
                    "merged_at": None,
                    "user_login": "author",
                    "state": "closed",
                    "created_at": "2026-09-14T08:00:00",
                    "updated_at": "2026-09-15T09:30:00",
                    "title": "Typed silver row",
                    "parse_run_id": RUN_ID,
                }
            ],
        )
        self.assertEqual(
            _rows(silver_tables["file"]),
            [
                {
                    "repo": REPOSITORY,
                    "pr_num": 7,
                    "filename": "src/job.py",
                    "additions": 53,
                    "deletions": 1,
                    "parse_run_id": RUN_ID,
                }
            ],
        )
        self.assertEqual(
            _rows(silver_tables["review"]),
            [
                {
                    "repo": REPOSITORY,
                    "pr_num": 7,
                    "user_login": "reviewer",
                    "state": "APPROVED",
                    "parse_run_id": RUN_ID,
                }
            ],
        )
        self.assertEqual(
            {
                table: parsed.filter((parsed.table == table) & ~parsed.valid).count()
                for table in ("pull", "file", "review")
            },
            {"pull": 0, "file": 0, "review": 0},
        )

    def test_silver_merges_valid_rows(self) -> None:
        bronze_recs = [
            _record(entity="pull", payload=_pull_payload()),
            _record(
                entity="file",
                payload={"filename": "src/job.py", "additions": 53, "deletions": 1},
            ),
            _record(
                entity="review",
                payload={
                    "state": "APPROVED",
                    "submitted_at": "2026-09-15T11:00:00Z",
                    "user": {"login": "reviewer"},
                },
            ),
        ]

        with tempfile.TemporaryDirectory() as directory:
            bronze_path = Path(directory, "bronze.jsonl")
            bronze_path.write_text(
                "".join(json.dumps(record) + "\n" for record in bronze_recs)
            )
            manifest_key = f"pipeline-artifacts/{RUN_ID}/stage1/output.parquet"
            s3 = _S3(
                {
                    (ARTIFACTS_BUCKET, manifest_key): _manifest_bytes(
                        [bronze_path.as_uri()],
                        record_count=len(bronze_recs),
                    )
                }
            )
            event = _event(manifest_key)

            with patch.object(silver_job, "_merge_tables") as merge:
                result = silver_job.silver(
                    event=event,
                    spark=self.spark,
                    s3=s3,
                    cloudwatch=_CloudWatch(),
                )

        self.assertEqual(result["status"], "success")
        merge.assert_called_once()
        self.assertEqual(
            set(merge.call_args.kwargs["tables"]),
            {"pull", "file", "review"},
        )
        self.assertEqual(
            merge.call_args.kwargs["snowflake_options"],
            event["snowflake_options"],
        )

    def test_silver_quarantines_corrupted_rows_before_snowflake_write(self) -> None:
        bronze_recs = [
            _record(entity="pull", payload=_pull_payload()),
            _record(
                entity="pull",
                pr_num=8,
                payload={**_pull_payload(), "user": {}},
            ),
            _record(
                entity="file",
                pr_num=9,
                payload={"filename": "typed.py", "additions": "53", "deletions": 1},
            ),
            _record(
                entity="file",
                pr_num=10,
                payload={"filename": "range.py", "additions": 1, "deletions": -1},
            ),
            _record(
                entity="review",
                pr_num=11,
                payload={
                    "state": "APPROVED",
                    "submitted_at": "not-a-timestamp",
                    "user": {"login": "reviewer"},
                },
            ),
            _record(
                entity="review",
                pr_num=12,
                run_id="not-a-utc-run-id",
                payload={
                    "state": "APPROVED",
                    "submitted_at": "2026-09-15T11:00:00Z",
                    "user": {"login": "reviewer"},
                },
            ),
        ]

        with tempfile.TemporaryDirectory() as directory:
            bronze_path = Path(directory, "bronze.jsonl")
            bronze_path.write_text(
                "".join(json.dumps(record) + "\n" for record in bronze_recs)
            )
            manifest_key = f"pipeline-artifacts/{RUN_ID}/stage1/output.parquet"
            s3 = _S3(
                {
                    (ARTIFACTS_BUCKET, manifest_key): _manifest_bytes(
                        [bronze_path.as_uri()],
                        record_count=len(bronze_recs),
                    )
                }
            )
            cloudwatch = _CloudWatch()
            with (
                patch.object(silver_job, "_merge_tables") as merge,
                self.assertRaises(RuntimeError),
            ):
                silver_job.silver(
                    event=_event(manifest_key),
                    spark=self.spark,
                    s3=s3,
                    cloudwatch=cloudwatch,
                )

        merge.assert_not_called()
        quarantine = {
            key
            for bucket, key in s3.objects
            if bucket == ARTIFACTS_BUCKET
            and key.startswith(f"corrupted-artifacts/{RUN_ID}/silver/")
        }
        self.assertEqual(
            quarantine,
            {
                f"corrupted-artifacts/{RUN_ID}/silver/pull.parquet",
                f"corrupted-artifacts/{RUN_ID}/silver/file.parquet",
                f"corrupted-artifacts/{RUN_ID}/silver/review.parquet",
            },
        )
        self.assertEqual(
            {
                key.rsplit("/", 1)[-1]: len(
                    parquet.read_table(io.BytesIO(s3.objects[(ARTIFACTS_BUCKET, key)]))
                )
                for key in quarantine
            },
            {"pull.parquet": 1, "file.parquet": 2, "review.parquet": 2},
        )
        self.assertNotIn(
            (
                ARTIFACTS_BUCKET,
                f"pipeline-artifacts/{RUN_ID}/silver/output.parquet",
            ),
            s3.objects,
        )
        _assert_metrics(
            test=self,
            cloudwatch=cloudwatch,
            status="fail",
            corrupted_count=5,
        )

    def test_silver_writes_empty_artifact_when_stage1_manifest_is_empty(
        self,
    ) -> None:
        manifest_key = f"pipeline-artifacts/{RUN_ID}/stage1/output.parquet"
        s3 = _S3(
            {(ARTIFACTS_BUCKET, manifest_key): _manifest_bytes([], record_count=0)}
        )
        cloudwatch = _CloudWatch()

        with patch.object(silver_job, "_merge_tables") as merge:
            result = silver_job.silver(
                event=_event(manifest_key),
                spark=self.spark,
                s3=s3,
                cloudwatch=cloudwatch,
            )

        merge.assert_not_called()
        artifact_key = f"pipeline-artifacts/{RUN_ID}/silver/output.parquet"
        self.assertEqual(
            result,
            {
                "run_id": RUN_ID,
                "status": "empty",
                "artifact_uri": f"s3://{ARTIFACTS_BUCKET}/{artifact_key}",
                "source_row_count": 0,
                "output_row_count": 0,
                "corrupted_count": 0,
            },
        )
        self.assertEqual(
            sorted(
                parquet.read_table(
                    io.BytesIO(s3.objects[(ARTIFACTS_BUCKET, artifact_key)])
                ).to_pylist(),
                key=lambda record: record["table"],
            ),
            [
                {
                    "run_id": RUN_ID,
                    "table": "file",
                    "source_row_count": 0,
                    "output_row_count": 0,
                    "corrupted_row_count": 0,
                },
                {
                    "run_id": RUN_ID,
                    "table": "pull",
                    "source_row_count": 0,
                    "output_row_count": 0,
                    "corrupted_row_count": 0,
                },
                {
                    "run_id": RUN_ID,
                    "table": "review",
                    "source_row_count": 0,
                    "output_row_count": 0,
                    "corrupted_row_count": 0,
                },
            ],
        )
        self.assertFalse(
            any(
                key.startswith(f"corrupted-artifacts/{RUN_ID}/silver/")
                for bucket, key in s3.objects
                if bucket == ARTIFACTS_BUCKET
            )
        )
        _assert_metrics(
            test=self,
            cloudwatch=cloudwatch,
            status="empty",
            corrupted_count=0,
        )

def _record(
    entity: str,
    payload: dict[str, Any],
    pr_num: int = 7,
    run_id: str = RUN_ID,
) -> dict[str, Any]:
    return {
        "run_id": run_id,
        "repo": REPOSITORY,
        "pr_num": pr_num,
        "entity": entity,
        "payload": payload,
    }


def _pull_payload() -> dict[str, Any]:
    return {
        "number": 7,
        "state": "closed",
        "title": "Valid pull",
        "created_at": "2026-09-14T08:00:00Z",
        "updated_at": "2026-09-15T09:30:00Z",
        "merged_at": None,
        "user": {"login": "author"},
    }


def _event(manifest_key: str) -> dict[str, Any]:
    return {
        "run_id": RUN_ID,
        "stage1_artifact_uri": f"s3://{ARTIFACTS_BUCKET}/{manifest_key}",
        "artifacts_bucket": ARTIFACTS_BUCKET,
        "cloudwatch_namespace": CLOUDWATCH_NAMESPACE,
        "snowflake_options": {
            "sfURL": "unused",
            "sfUser": "unused",
            "sfPassword": "unused",
            "sfDatabase": "unused",
            "sfSchema": "unused",
            "sfWarehouse": "unused",
        },
    }


def _manifest_bytes(uris: list[str], record_count: int) -> bytes:
    schema = arrow.schema(
        [
            ("run_id", arrow.string()),
            ("repo", arrow.string()),
            ("s3_uri", arrow.string()),
            ("pull_request_count", arrow.int64()),
            ("record_count", arrow.int64()),
        ]
    )
    records = [
        {
            "run_id": RUN_ID,
            "repo": REPOSITORY,
            "s3_uri": uri,
            "pull_request_count": 1,
            "record_count": record_count,
        }
        for uri in uris
    ]
    body = io.BytesIO()
    parquet.write_table(arrow.Table.from_pylist(records, schema=schema), body)
    return body.getvalue()


def _rows(frame: DataFrame) -> list[dict[str, Any]]:
    rows = []
    for row in frame.collect():
        record = row.asDict(recursive=True)
        rows.append(
            {
                key: value.isoformat() if hasattr(value, "isoformat") else value
                for key, value in record.items()
            }
        )
    return rows


def _assert_metrics(
    test: TestCase,
    cloudwatch: _CloudWatch,
    status: str,
    corrupted_count: int,
) -> None:
    test.assertTrue(
        all(call["Namespace"] == CLOUDWATCH_NAMESPACE for call in cloudwatch.calls)
    )
    metrics = [metric for call in cloudwatch.calls for metric in call["MetricData"]]
    test.assertEqual(_metric_values(metrics, "job_start_count"), [1])
    test.assertEqual(_metric_values(metrics, "inflight_job_count"), [1, 0])
    test.assertEqual(_metric_values(metrics, "corrupted_count"), [corrupted_count])
    test.assertEqual(len(_metric_values(metrics, "job_duration")), 1)
    finish = [
        metric for metric in metrics if metric["MetricName"] == "job_finish_status"
    ]
    test.assertEqual(len(finish), 1)
    test.assertEqual(finish[0]["Value"], 1)
    test.assertIn(
        {"Name": "status", "Value": status},
        finish[0]["Dimensions"],
    )


def _metric_values(
    metrics: list[dict[str, Any]],
    name: str,
) -> list[int | float]:
    return [metric["Value"] for metric in metrics if metric["MetricName"] == name]


class _S3Exceptions:
    NoSuchKey = KeyError


class _S3:
    exceptions = _S3Exceptions()

    def __init__(
        self,
        objects: dict[tuple[str, str], bytes] | None = None,
    ) -> None:
        self.objects = dict(objects or {})

    def get_object(self, Bucket: str, Key: str) -> dict[str, io.BytesIO]:
        return {"Body": io.BytesIO(self.objects[(Bucket, Key)])}

    def put_object(
        self,
        Bucket: str,
        Key: str,
        Body: bytes | BinaryIO,
        **_: Any,
    ) -> None:
        try:
            body = Body.read()
        except AttributeError:
            body = bytes(Body)
        self.objects[(Bucket, Key)] = body


class _CloudWatch:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    def put_metric_data(self, **kwargs: Any) -> None:
        self.calls.append(kwargs)
