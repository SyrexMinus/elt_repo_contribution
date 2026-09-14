import io
import json
from pathlib import Path
from unittest import TestCase
from urllib.parse import parse_qs, urlencode, urlsplit

import pyarrow.parquet as parquet
import requests

from stages.poller.poller import poller


REPOSITORY = "salute-developers/smart_app_framework"
BRONZE_BUCKET = "bronze-bucket"
ARTIFACTS_BUCKET = "artifacts-bucket"
CLOUDWATCH_NAMESPACE = "RepoContribution/Test"
STATE_KEY = f"poller-state/{REPOSITORY}.json"


class PollerTest(TestCase):
    def test_poller_writes_incremental_artifacts(self):
        run_id = "incremental-run"
        event = _event(
            run_id=run_id,
            mode="incremental",
            batch_max_pull_requests=10,
        )
        watermark = "2025-02-06T11:56:01Z"
        state = {
            "last_successful_watermark": watermark,
            "pull_requests": {},
        }
        s3 = _S3({(ARTIFACTS_BUCKET, STATE_KEY): _json_bytes(state)})
        cloudwatch = _CloudWatch()

        with requests.Session() as github:
            result = poller(
                event=event,
                s3=s3,
                github=github,
                cloudwatch=cloudwatch,
            )

        bronze_records = _bronze_records(s3=s3, run_id=run_id)
        pull_records = [
            record for record in bronze_records if record["entity"] == "pull"
        ]
        file_records = [
            record for record in bronze_records if record["entity"] == "file"
        ]
        artifact_records = _artifact_records(
            s3=s3,
            key=f"pipeline-artifacts/{run_id}/stage1/output.parquet",
        )

        self.assertGreaterEqual(len(pull_records), 45)
        self.assertGreaterEqual(len(file_records), 45)
        self.assertNotIn(1, [record["pr_num"] for record in pull_records])
        self.assertIn(
            {
                "number": 210,
                "state": "closed",
                "title": "PNLP-12553: add stats",
                "updated_at": "2025-02-06T11:56:01Z",
                "merged_at": "2025-02-06T11:56:01Z",
                "user": "SyrexMinus",
            },
            [
                {
                    "number": record["payload"]["number"],
                    "state": record["payload"]["state"],
                    "title": record["payload"]["title"],
                    "updated_at": record["payload"]["updated_at"],
                    "merged_at": record["payload"]["merged_at"],
                    "user": record["payload"]["user"]["login"],
                }
                for record in pull_records
            ],
        )
        self.assertIn(
            {
                "filename": "core/utils/stats_timer.py",
                "additions": 53,
                "deletions": 1,
                "status": "modified",
            },
            [
                {
                    "filename": record["payload"]["filename"],
                    "additions": record["payload"]["additions"],
                    "deletions": record["payload"]["deletions"],
                    "status": record["payload"]["status"],
                }
                for record in bronze_records
                if record["entity"] == "file" and record["pr_num"] == 210
            ],
        )
        self.assertEqual(
            [
                {
                    "id": record["payload"]["id"],
                    "user": record["payload"]["user"]["login"],
                    "state": record["payload"]["state"],
                    "submitted_at": record["payload"]["submitted_at"],
                    "commit_id": record["payload"]["commit_id"],
                }
                for record in bronze_records
                if record["entity"] == "review" and record["pr_num"] == 210
            ],
            [
                {
                    "id": 2596185857,
                    "user": "dangerink",
                    "state": "APPROVED",
                    "submitted_at": "2025-02-05T15:33:26Z",
                    "commit_id": "61689e3ecae92d23324eee2f01f64a1c4ef42095",
                }
            ],
        )
        self.assertGreaterEqual(len(artifact_records), 5)
        self.assertEqual(
            {
                "run_id": artifact_records[0]["run_id"],
                "repo": artifact_records[0]["repo"],
                "s3_uri": artifact_records[0]["s3_uri"],
            },
            {
                "run_id": run_id,
                "repo": REPOSITORY,
                "s3_uri": (
                    f"s3://{BRONZE_BUCKET}/{REPOSITORY}/{run_id}/"
                    "batch-00000.jsonl"
                ),
            },
        )
        self.assertGreaterEqual(
            sum(
                record["pull_request_count"] for record in artifact_records
            ),
            45,
        )
        self.assertGreaterEqual(
            sum(record["record_count"] for record in artifact_records),
            90,
        )
        stored_state = _state_artifact(s3)
        self.assertGreater(stored_state["last_successful_watermark"], watermark)
        self.assertEqual(
            stored_state["pull_requests"]["210"],
            {
                "updated_at": "2025-02-06T11:56:01Z",
                "merged": True,
            },
        )
        self.assertEqual(
            {
                "run_id": result["run_id"],
                "status": result["status"],
                "artifact_uri": result["artifact_uri"],
                "corrupted_count": result["corrupted_count"],
            },
            {
                "run_id": run_id,
                "status": "success",
                "artifact_uri": (
                    f"s3://{ARTIFACTS_BUCKET}/pipeline-artifacts/"
                    f"{run_id}/stage1/output.parquet"
                ),
                "corrupted_count": 0,
            },
        )
        self.assertGreaterEqual(result["bronze_objects"], 5)
        self.assertGreaterEqual(result["pull_requests"], 45)
        _assert_metrics(
            test=self,
            cloudwatch=cloudwatch,
            status="success",
            corrupted_count=0,
        )

    def test_poller_writes_complete_history_for_backfill_mode(self):
        run_id = "backfill-run"
        event = _event(
            run_id=run_id,
            mode="backfill",
            batch_max_pull_requests=10,
        )
        s3 = _S3()
        cloudwatch = _CloudWatch()

        with requests.Session() as github:
            result = poller(
                event=event,
                s3=s3,
                github=github,
                cloudwatch=cloudwatch,
            )

        bronze_records = _bronze_records(s3=s3, run_id=run_id)
        pull_records = [
            record for record in bronze_records if record["entity"] == "pull"
        ]
        file_records = [
            record for record in bronze_records if record["entity"] == "file"
        ]
        artifact_records = _artifact_records(
            s3=s3,
            key=f"pipeline-artifacts/{run_id}/stage1/output.parquet",
        )

        self.assertGreaterEqual(len(pull_records), 230)
        self.assertGreaterEqual(len(file_records), 230)
        self.assertIn(
            {
                "number": 1,
                "state": "closed",
                "title": "Feature/new metrics",
                "created_at": "2022-04-20T13:15:37Z",
                "updated_at": "2022-05-16T06:44:31Z",
                "merged_at": "2022-05-16T06:44:31Z",
                "user": "intsyn",
            },
            [
                {
                    "number": record["payload"]["number"],
                    "state": record["payload"]["state"],
                    "title": record["payload"]["title"],
                    "created_at": record["payload"]["created_at"],
                    "updated_at": record["payload"]["updated_at"],
                    "merged_at": record["payload"]["merged_at"],
                    "user": record["payload"]["user"]["login"],
                }
                for record in pull_records
            ],
        )
        self.assertGreaterEqual(len(artifact_records), 23)
        self.assertEqual(
            {
                "run_id": artifact_records[0]["run_id"],
                "repo": artifact_records[0]["repo"],
                "s3_uri": artifact_records[0]["s3_uri"],
            },
            {
                "run_id": run_id,
                "repo": REPOSITORY,
                "s3_uri": (
                    f"s3://{BRONZE_BUCKET}/{REPOSITORY}/{run_id}/"
                    "batch-00000.jsonl"
                ),
            },
        )
        self.assertGreaterEqual(
            sum(
                record["pull_request_count"] for record in artifact_records
            ),
            230,
        )
        self.assertGreaterEqual(
            sum(record["record_count"] for record in artifact_records),
            460,
        )
        stored_state = _state_artifact(s3)
        self.assertEqual(
            stored_state["pull_requests"]["1"],
            {
                "updated_at": "2022-05-16T06:44:31Z",
                "merged": True,
            },
        )
        self.assertEqual(
            {
                "run_id": result["run_id"],
                "status": result["status"],
                "artifact_uri": result["artifact_uri"],
                "corrupted_count": result["corrupted_count"],
            },
            {
                "run_id": run_id,
                "status": "success",
                "artifact_uri": (
                    f"s3://{ARTIFACTS_BUCKET}/pipeline-artifacts/"
                    f"{run_id}/stage1/output.parquet"
                ),
                "corrupted_count": 0,
            },
        )
        self.assertGreaterEqual(result["bronze_objects"], 23)
        self.assertGreaterEqual(result["pull_requests"], 230)
        _assert_metrics(
            test=self,
            cloudwatch=cloudwatch,
            status="success",
            corrupted_count=0,
        )

    def test_poller_writes_empty_artifact_when_increment_has_no_changes(self):
        run_id = "empty-run"
        watermark = "2026-09-10T00:00:00Z"
        state = {
            "last_successful_watermark": watermark,
            "pull_requests": {
                "301": {"updated_at": watermark, "merged": True},
            },
        }
        state_body = _json_bytes(state)
        s3 = _S3({(ARTIFACTS_BUCKET, STATE_KEY): state_body})
        cloudwatch = _CloudWatch()
        github = _GitHub(
            {(_path("pulls"), 1): _Reply([])}
        )

        result = poller(
            event=_event(
                run_id=run_id,
                mode="incremental",
                batch_max_pull_requests=1000,
            ),
            s3=s3,
            github=github,
            cloudwatch=cloudwatch,
        )

        artifact_key = f"pipeline-artifacts/{run_id}/stage1/output.parquet"
        self.assertEqual(
            result,
            {
                "run_id": run_id,
                "status": "empty",
                "artifact_uri": f"s3://{ARTIFACTS_BUCKET}/{artifact_key}",
                "bronze_objects": 0,
                "pull_requests": 0,
                "corrupted_count": 0,
            },
        )
        self.assertEqual(
            _artifact_records(s3=s3, key=artifact_key),
            [],
        )
        self.assertEqual(
            _bronze_records(s3=s3, run_id=run_id),
            [],
        )
        self.assertEqual(
            s3.objects[(ARTIFACTS_BUCKET, STATE_KEY)],
            state_body,
        )
        _assert_metrics(
            test=self,
            cloudwatch=cloudwatch,
            status="empty",
            corrupted_count=0,
        )

    def test_poller_quarantines_corrupted_response(self):
        cases = {
            "non-200": _Reply(
                payload={"message": "server error"},
                status=500,
            ),
            "empty-body": _Reply(payload=None, empty_body=True),
        }

        for case, reply in cases.items():
            with self.subTest(case):
                run_id = f"corrupted-{case}"
                state = {
                    "last_successful_watermark": "2026-09-10T00:00:00Z",
                    "pull_requests": {},
                }
                state_body = _json_bytes(state)
                s3 = _S3({(ARTIFACTS_BUCKET, STATE_KEY): state_body})
                cloudwatch = _CloudWatch()
                github = _GitHub({(_path("pulls"), 1): reply})
                event = _event(
                    run_id=run_id,
                    mode="incremental",
                    batch_max_pull_requests=1000,
                )

                with self.assertRaises(RuntimeError):
                    poller(
                        event=event,
                        s3=s3,
                        github=github,
                        cloudwatch=cloudwatch,
                    )

                quarantine_key = (
                    f"corrupted-artifacts/{run_id}/stage1/output.parquet"
                )
                records = _artifact_records(
                    s3=s3,
                    key=quarantine_key,
                )
                self.assertEqual(len(records), 1)
                self.assertEqual(records[0]["repo"], REPOSITORY)
                self.assertEqual(records[0]["entity"], "pull")
                self.assertEqual(
                    records[0]["status"],
                    reply.status_code,
                )
                self.assertIn(
                    _path("pulls"),
                    records[0]["url"],
                )
                self.assertEqual(
                    records[0]["query"],
                    {
                        "direction": ["desc"],
                        "per_page": ["100"],
                        "sort": ["updated"],
                        "state": ["all"],
                    },
                )
                self.assertNotIn(
                    (
                        ARTIFACTS_BUCKET,
                        f"pipeline-artifacts/{run_id}/stage1/output.parquet",
                    ),
                    s3.objects,
                )
                self.assertEqual(
                    _bronze_records(s3=s3, run_id=run_id),
                    [],
                )
                self.assertEqual(
                    s3.objects[(ARTIFACTS_BUCKET, STATE_KEY)],
                    state_body,
                )
                self.assertNotIn(
                    event["github_token"].encode(),
                    b"".join(s3.objects.values()),
                )
                _assert_metrics(
                    test=self,
                    cloudwatch=cloudwatch,
                    status="fail",
                    corrupted_count=1,
                )

    def test_poller_preserves_state_when_artifact_upload_fails(self):
        run_id = "failed-upload-run"
        pull = _pull(number=401, updated_at="2026-09-12T00:00:00Z")
        state = {
            "last_successful_watermark": "2026-09-10T00:00:00Z",
            "pull_requests": {},
        }
        state_body = _json_bytes(state)
        artifact_key = f"pipeline-artifacts/{run_id}/stage1/output.parquet"
        s3 = _S3(
            objects={(ARTIFACTS_BUCKET, STATE_KEY): state_body},
            failed_puts={(ARTIFACTS_BUCKET, artifact_key)},
        )
        cloudwatch = _CloudWatch()
        github = _GitHub(
            {
                (_path("pulls"), 1): _Reply([pull]),
                (_path("pulls/401/files"), 1): _Reply([]),
                (_path("pulls/401/reviews"), 1): _Reply([]),
            }
        )

        with self.assertRaises(_UploadFailed):
            poller(
                event=_event(
                    run_id=run_id,
                    mode="incremental",
                    batch_max_pull_requests=1000,
                ),
                s3=s3,
                github=github,
                cloudwatch=cloudwatch,
            )

        self.assertEqual(
            s3.objects[(ARTIFACTS_BUCKET, STATE_KEY)],
            state_body,
        )
        self.assertNotIn(
            (ARTIFACTS_BUCKET, artifact_key),
            s3.objects,
        )
        self.assertEqual(
            [
                (record["entity"], record["pr_num"], record["payload"])
                for record in _bronze_records(s3=s3, run_id=run_id)
            ],
            [("pull", 401, pull)],
        )
        _assert_metrics(
            test=self,
            cloudwatch=cloudwatch,
            status="fail",
            corrupted_count=0,
        )


def _event(run_id, mode, batch_max_pull_requests):
    return {
        "repositories": [REPOSITORY],
        "github_token": _token(),
        "github_api_url": "https://api.github.com",
        "mode": mode,
        "run_id": run_id,
        "bronze_bucket": BRONZE_BUCKET,
        "artifacts_bucket": ARTIFACTS_BUCKET,
        "cloudwatch_namespace": CLOUDWATCH_NAMESPACE,
        "batch_max_pull_requests": batch_max_pull_requests,
        "batch_max_bytes": 10 * 1024 * 1024,
        "request_timeout_seconds": 30,
    }


def _token():
    return Path("~/.github/token").expanduser().read_text().strip()


def _pull(number, updated_at, state="open", merged_at=None):
    return {
        "number": number,
        "state": state,
        "updated_at": updated_at,
        "merged_at": merged_at,
        "title": f"Pull request {number}",
        "user": {"login": f"author-{number}"},
    }


def _path(endpoint):
    return f"/repos/{REPOSITORY}/{endpoint}"


def _json_bytes(value):
    return json.dumps(
        obj=value,
        separators=(",", ":"),
        sort_keys=True,
    ).encode()


def _artifact_records(s3, key):
    return parquet.read_table(
        io.BytesIO(s3.objects[(ARTIFACTS_BUCKET, key)])
    ).to_pylist()


def _state_artifact(s3):
    return json.loads(s3.objects[(ARTIFACTS_BUCKET, STATE_KEY)])


def _bronze_records(s3, run_id):
    prefix = f"{REPOSITORY}/{run_id}/"
    records = []
    for (bucket, key), body in sorted(s3.objects.items()):
        if bucket != BRONZE_BUCKET or not key.startswith(prefix):
            continue
        records.extend(json.loads(line) for line in body.splitlines())
    return records


def _assert_metrics(test, cloudwatch, status, corrupted_count):
    test.assertTrue(
        all(
            call["Namespace"] == CLOUDWATCH_NAMESPACE
            for call in cloudwatch.calls
        )
    )
    metrics = [
        metric
        for call in cloudwatch.calls
        for metric in call["MetricData"]
    ]
    test.assertEqual(
        _metric_values(metrics=metrics, name="job_start_count"),
        [1],
    )
    test.assertEqual(
        _metric_values(metrics=metrics, name="inflight_job_count"),
        [1, 0],
    )
    test.assertEqual(
        _metric_values(metrics=metrics, name="corrupted_count"),
        [corrupted_count],
    )
    test.assertEqual(
        len(_metric_values(metrics=metrics, name="job_duration")),
        1,
    )
    finish = [
        metric
        for metric in metrics
        if metric["MetricName"] == "job_finish_status"
    ]
    test.assertEqual(len(finish), 1)
    test.assertEqual(finish[0]["Value"], 1)
    test.assertIn(
        {"Name": "status", "Value": status},
        finish[0]["Dimensions"],
    )


def _metric_values(metrics, name):
    return [metric["Value"] for metric in metrics if metric["MetricName"] == name]


class _Reply:
    def __init__(self, payload, status=200, next_url=None, empty_body=False):
        self.status_code = status
        self.headers = {}
        self.content = b"" if empty_body else _json_bytes(payload)
        if next_url:
            self.headers["Link"] = f'<{next_url}>; rel="next"'

    def json(self):
        return json.loads(self.content)


class _GitHub:
    def __init__(self, routes):
        self.routes = dict(routes)

    def get(self, url, headers, params=None, timeout=None):
        if params:
            url = f"{url}?{urlencode(params)}"
        parsed = urlsplit(url)
        query = parse_qs(parsed.query)
        page = int(query["page"][0]) if "page" in query else 1
        return self.routes.pop((parsed.path, page))


class _S3:
    def __init__(self, objects=None, failed_puts=None):
        self.objects = dict(objects or {})
        self.failed_puts = set(failed_puts or set())

    def get_object(self, Bucket, Key):
        return {"Body": io.BytesIO(self.objects[(Bucket, Key)])}

    def put_object(self, Bucket, Key, Body, **kwargs):
        try:
            body = Body.read()
        except AttributeError:
            body = bytes(Body)
        if (Bucket, Key) in self.failed_puts:
            raise _UploadFailed(f"failed to upload s3://{Bucket}/{Key}")
        self.objects[(Bucket, Key)] = body


class _CloudWatch:
    def __init__(self):
        self.calls = []

    def put_metric_data(self, **kwargs):
        self.calls.append(kwargs)


class _UploadFailed(RuntimeError):
    pass
