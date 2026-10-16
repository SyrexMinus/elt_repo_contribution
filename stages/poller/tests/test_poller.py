from __future__ import annotations

import io
import json
from pathlib import Path
from typing import Any, BinaryIO, Literal
from unittest import TestCase
from unittest.mock import call, patch
from urllib.parse import parse_qs, urlencode, urlsplit

import requests
from pyarrow import parquet

from stages.poller import poller as poller_module
from stages.poller.poller import poller

REPOSITORY = "salute-developers/smart_app_framework"
BRONZE_BUCKET = "bronze-bucket"
ARTIFACTS_BUCKET = "artifacts-bucket"
CLOUDWATCH_NAMESPACE = "RepoContribution/Test"
STATE_KEY = f"poller-state/{REPOSITORY}.json"


class PollerTest(TestCase):
    def test_poller_writes_incremental_artifacts(self) -> None:
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
            key=f"pipeline-artifacts/{run_id}/bronze/output.parquet",
        )

        self.assertGreaterEqual(len(pull_records), 45)
        self.assertGreaterEqual(len(file_records), 45)
        self.assertEqual(
            {record["produced_new_snapshot"] for record in pull_records},
            {True},
        )
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
                    f"s3://{BRONZE_BUCKET}/{REPOSITORY}/{run_id}/batch-00000.jsonl"
                ),
            },
        )
        self.assertGreaterEqual(
            sum(record["pull_request_count"] for record in artifact_records),
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
                    f"{run_id}/bronze/output.parquet"
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

    def test_poller_writes_complete_history_for_backfill_mode(self) -> None:
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
            key=f"pipeline-artifacts/{run_id}/bronze/output.parquet",
        )

        self.assertGreaterEqual(len(pull_records), 230)
        self.assertGreaterEqual(len(file_records), 230)
        self.assertEqual(
            {record["produced_new_snapshot"] for record in pull_records},
            {True},
        )
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
                    f"s3://{BRONZE_BUCKET}/{REPOSITORY}/{run_id}/batch-00000.jsonl"
                ),
            },
        )
        self.assertGreaterEqual(
            sum(record["pull_request_count"] for record in artifact_records),
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
                    f"{run_id}/bronze/output.parquet"
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

    def test_poller_writes_empty_artifact_when_increment_has_no_changes(
        self,
    ) -> None:
        run_id = "empty-run"
        watermark = "2026-09-10T00:00:00Z"
        state = {
            "last_successful_watermark": watermark,
            "pull_requests": {
                "301": {"updated_at": watermark, "merged": True},
                "302": {"updated_at": watermark, "merged": False},
            },
        }
        state_body = _json_bytes(state)
        s3 = _S3({(ARTIFACTS_BUCKET, STATE_KEY): state_body})
        cloudwatch = _CloudWatch()
        github = _GitHub(
            {
                (_path("pulls"), 1): _Reply(
                    [
                        _pull(301, watermark, "closed", watermark),
                        _pull(302, watermark),
                        _pull(300, "2026-09-09T00:00:00Z"),
                    ]
                )
            }
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

        artifact_key = f"pipeline-artifacts/{run_id}/bronze/output.parquet"
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

    def test_poller_writes_unknown_pull_request_at_watermark(self) -> None:
        run_id = "watermark-run"
        watermark = "2026-09-10T00:00:00Z"
        s3 = _S3(
            {
                (ARTIFACTS_BUCKET, STATE_KEY): _json_bytes(
                    {
                        "last_successful_watermark": watermark,
                        "pull_requests": {
                            "301": {"updated_at": watermark, "merged": True},
                        },
                    }
                )
            }
        )
        github = _GitHub(
            {
                (_path("pulls"), 1): _Reply(
                    [
                        _pull(301, watermark, "closed", watermark),
                        _pull(302, watermark),
                    ]
                ),
                (_path("pulls/302/files"), 1): _Reply([]),
                (_path("pulls/302/reviews"), 1): _Reply([]),
            }
        )

        poller(_event(run_id, "incremental", 1000), s3, github, _CloudWatch())

        self.assertEqual(
            [record["pr_num"] for record in _bronze_records(s3, run_id)],
            [302],
        )

    def test_poller_retries_rate_limited_request(self) -> None:
        run_id = "rate-limited-run"
        pull = _pull(number=401, updated_at="2026-09-12T00:00:00Z")
        s3 = _S3()
        cloudwatch = _CloudWatch()
        github = _GitHub(
            {
                (_path("pulls"), 1): [
                    _Reply(
                        payload={"message": "secondary rate limit"},
                        status=429,
                        headers={"Retry-After": "2"},
                    ),
                    _Reply([pull]),
                ],
                (_path("pulls/401/files"), 1): [
                    _Reply(
                        payload={"message": "primary rate limit"},
                        status=403,
                        headers={
                            "X-RateLimit-Remaining": "0",
                            "X-RateLimit-Reset": "105",
                        },
                    ),
                    _Reply([]),
                ],
                (_path("pulls/401/reviews"), 1): _Reply([]),
            }
        )

        with (
            patch.object(poller_module.time, "time", return_value=100),
            patch.object(poller_module.time, "sleep") as sleep,
        ):
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

        self.assertEqual(sleep.call_args_list, [call(2), call(6)])
        self.assertEqual(
            [urlsplit(url).path for url in github.calls],
            [
                _path("pulls"),
                _path("pulls"),
                _path("pulls/401/files"),
                _path("pulls/401/files"),
                _path("pulls/401/reviews"),
            ],
        )
        self.assertEqual(result["status"], "success")
        self.assertEqual(result["pull_requests"], 1)
        self.assertEqual(
            [
                (record["entity"], record["pr_num"])
                for record in _bronze_records(s3=s3, run_id=run_id)
            ],
            [("pull", 401)],
        )
        self.assertFalse(
            any(
                bucket == ARTIFACTS_BUCKET
                and key.startswith(f"corrupted-artifacts/{run_id}/")
                for bucket, key in s3.objects
            )
        )
        self.assertEqual(
            _state_artifact(s3)["pull_requests"]["401"],
            {
                "updated_at": "2026-09-12T00:00:00Z",
                "merged": False,
            },
        )
        _assert_metrics(
            test=self,
            cloudwatch=cloudwatch,
            status="success",
            corrupted_count=0,
        )

    def test_poller_quarantines_corrupted_response(self) -> None:
        cases = {
            "non-200": _Reply(
                payload={"message": "server error"},
                status=500,
            ),
            "forbidden": _Reply(
                payload={"message": "forbidden"},
                status=403,
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
                github = _GitHub(
                    {(_path("pulls"), 1): [reply] * 5 if case == "non-200" else reply}
                )
                event = _event(
                    run_id=run_id,
                    mode="incremental",
                    batch_max_pull_requests=1000,
                )

                with (
                    patch.object(poller_module.time, "sleep") as sleep,
                    self.assertRaises(RuntimeError),
                ):
                    poller(
                        event=event,
                        s3=s3,
                        github=github,
                        cloudwatch=cloudwatch,
                    )

                self.assertEqual(
                    sleep.call_args_list,
                    [call(10), call(20), call(40), call(80)]
                    if case == "non-200"
                    else [],
                )
                quarantine_key = f"corrupted-artifacts/{run_id}/stage1/output.parquet"
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
                        f"pipeline-artifacts/{run_id}/bronze/output.parquet",
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

    def test_poller_retries_transient_responses_and_timeouts(self) -> None:
        event = _event("transient-run", "backfill", 1000)
        github = _GitHub(
            {
                (_path("pulls"), 1): [
                    _Reply({}, status=500),
                    requests.ConnectTimeout("connection timed out"),
                    _Reply({}, status=502),
                    requests.ReadTimeout("response timed out"),
                    _Reply([_pull(401, "2026-10-11T09:00:00Z")]),
                ],
                (_path("pulls/401/files"), 1): [
                    _Reply({}, status=503),
                    _Reply({}, status=504),
                    _Reply([{"filename": "src/app.py"}]),
                ],
                (_path("pulls/401/reviews"), 1): _Reply([{"state": "APPROVED"}]),
            }
        )
        s3 = _S3()
        with patch.object(poller_module.time, "sleep") as sleep:
            poller(event, s3, github, _CloudWatch())

        self.assertEqual(
            sleep.call_args_list,
            [call(10), call(20), call(40), call(80), call(10), call(20)],
        )
        self.assertEqual(
            [record["entity"] for record in _bronze_records(s3, event["run_id"])],
            ["pull", "file", "review"],
        )

    def test_poller_resumes_backfill_after_batch_checkpoint_upload_fails(self) -> None:
        event = _event("backfill-resume", "backfill", 1)
        event["repositories"] = [REPOSITORY, "hashicorp/terraform"]
        checkpoint_key = f"poller-checkpoints/{event['run_id']}/hashicorp/terraform.json"
        s3 = _S3(failed_put_numbers={(ARTIFACTS_BUCKET, checkpoint_key): 2})
        pulls_by_repo = {REPOSITORY: [400], "hashicorp/terraform": [401, 402, 403]}
        routes = {}
        for repo, numbers in pulls_by_repo.items():
            pull_path = f"/repos/{repo}/pulls"
            pulls = [_pull(number, "2026-10-11T09:00:00Z") for number in numbers]
            routes[(pull_path, 1)] = _Reply(
                pulls[:2],
                next_url=f"https://api.github.com{pull_path}?per_page=100&page=2"
                if len(pulls) > 2
                else None,
            )
            if len(pulls) > 2:
                routes[(pull_path, 2)] = _Reply(pulls[2:])
            for number in numbers:
                routes[(f"{pull_path}/{number}/files", 1)] = _Reply(
                    [{"filename": "src/app.py"}]
                )
                routes[(f"{pull_path}/{number}/reviews", 1)] = _Reply(
                    [{"state": "APPROVED"}]
                )
        with self.assertRaises(_UploadFailed):
            poller(event, s3, _GitHub(routes), _CloudWatch())

        github = _GitHub(
            {
                key: reply
                for key, reply in routes.items()
                if key[0].startswith("/repos/hashicorp/terraform/")
                and "/401/" not in key[0]
                and key != ("/repos/hashicorp/terraform/pulls", 1)
            }
        )
        poller(event, s3, github, _CloudWatch())

        manifest = _artifact_records(
            s3, f"pipeline-artifacts/{event['run_id']}/bronze/output.parquet"
        )
        self.assertEqual(len(manifest), 4)
        self.assertEqual(len({record["s3_uri"] for record in manifest}), 4)
        records = [
            json.loads(line)
            for (bucket, _), body in s3.objects.items()
            if bucket == BRONZE_BUCKET
            for line in body.splitlines()
        ]
        self.assertCountEqual(
            [
                (record["repo"], record["pr_num"], record["entity"])
                for record in records
            ],
            [
                (repo, number, entity)
                for repo, numbers in pulls_by_repo.items()
                for number in numbers
                for entity in ("pull", "file", "review")
            ],
        )
        self.assertEqual(
            [
                s3.put_calls.count((BRONZE_BUCKET, record["s3_uri"].split("/", 3)[3]))
                for record in manifest
            ],
            [1, 1, 2, 1],
        )

        github = _GitHub(routes)
        result = poller({**event, "run_id": "new-backfill"}, s3, github, _CloudWatch())
        self.assertEqual(result["pull_requests"], 4)
        self.assertEqual(len(github.calls), 11)

    def test_poller_resumes_incremental_with_original_watermark(self) -> None:
        event = _event("incremental-resume", "incremental", 1000)
        event["batch_max_bytes"] = 512
        state_body = _json_bytes({"last_successful_watermark": "2026-09-10T00:00:00Z"})
        s3 = _S3({(ARTIFACTS_BUCKET, STATE_KEY): state_body})
        routes = {
            (_path("pulls"), 1): _Reply(
                [
                    _pull(number, f"2026-09-{day:02d}T00:00:00Z")
                    for number, day in ((401, 12), (402, 11), (403, 10), (404, 9))
                ]
            )
        }
        for number in (401, 402, 403):
            routes[(_path(f"pulls/{number}/files"), 1)] = _Reply([])
            routes[(_path(f"pulls/{number}/reviews"), 1)] = _Reply([])
        github = _GitHub(
            {
                **routes,
                (_path("pulls/403/files"), 1): [requests.ReadTimeout("timed out")] * 5,
            }
        )
        with (
            patch.object(poller_module.time, "sleep") as sleep,
            self.assertRaises(requests.ReadTimeout),
        ):
            poller(event, s3, github, _CloudWatch())
        self.assertEqual(sleep.call_args_list, [call(10), call(20), call(40), call(80)])
        self.assertEqual(s3.objects[(ARTIFACTS_BUCKET, STATE_KEY)], state_body)
        self.assertEqual(
            [record["pr_num"] for record in _bronze_records(s3, event["run_id"])],
            [401],
        )

        github = _GitHub(
            {
                key: reply
                for key, reply in routes.items()
                if key[0] != _path("pulls") and "/401/" not in key[0]
            }
        )
        poller(event, s3, github, _CloudWatch())
        self.assertEqual(
            [record["pr_num"] for record in _bronze_records(s3, event["run_id"])],
            [401, 402, 403],
        )
        self.assertEqual(
            _state_artifact(s3)["last_successful_watermark"], "2026-09-12T00:00:00Z"
        )

    def test_poller_preserves_state_when_artifact_upload_fails(self) -> None:
        run_id = "failed-upload-run"
        pull = _pull(number=401, updated_at="2026-09-12T00:00:00Z")
        state = {
            "last_successful_watermark": "2026-09-10T00:00:00Z",
            "pull_requests": {},
        }
        state_body = _json_bytes(state)
        artifact_key = f"pipeline-artifacts/{run_id}/bronze/output.parquet"
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

    def test_poller_preserves_bronze_and_manifest_when_checkpoint_upload_fails(
        self,
    ) -> None:
        event = _event("20261011T100000000000Z", "incremental", 1000)
        event["repositories"] = [REPOSITORY, "hashicorp/terraform"]
        routes = {}
        for repo in event["repositories"]:
            pull_path = f"/repos/{repo}/pulls"
            routes[(pull_path, 1)] = _Reply([_pull(401, "2026-10-11T09:00:00Z")])
            routes[(f"{pull_path}/401/files", 1)] = _Reply(
                [{"filename": "src/app.py", "additions": 10, "deletions": 2}]
            )
            routes[(f"{pull_path}/401/reviews", 1)] = _Reply(
                [{"user": {"login": "reviewer"}, "state": "APPROVED"}]
            )
        s3 = _S3(failed_puts={(ARTIFACTS_BUCKET, "poller-state/hashicorp/terraform.json")})
        with self.assertRaises(_UploadFailed):
            poller(event, s3, _GitHub(routes), _CloudWatch())

        published_bronze_and_manifest = {
            key: body
            for key, body in s3.objects.items()
            if not key[1].startswith("poller-state/")
        }
        s3.failed_puts.clear()
        poller(event, s3, _GitHub(routes), _CloudWatch())
        self.assertEqual(
            {key: s3.objects[key] for key in published_bronze_and_manifest},
            published_bronze_and_manifest,
        )


def _event(
    run_id: str,
    mode: Literal["incremental", "backfill"],
    batch_max_pull_requests: int,
) -> dict[str, Any]:
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


def _token() -> str:
    return Path("~/.github/token").expanduser().read_text().strip()


def _pull(
    number: int,
    updated_at: str,
    state: str = "open",
    merged_at: str | None = None,
) -> dict[str, Any]:
    return {
        "number": number,
        "state": state,
        "updated_at": updated_at,
        "merged_at": merged_at,
        "title": f"Pull request {number}",
        "user": {"login": f"author-{number}"},
    }


def _path(endpoint: str) -> str:
    return f"/repos/{REPOSITORY}/{endpoint}"


def _json_bytes(value: Any) -> bytes:
    return json.dumps(
        obj=value,
        separators=(",", ":"),
        sort_keys=True,
    ).encode()


def _artifact_records(s3: _S3, key: str) -> list[dict[str, Any]]:
    return parquet.read_table(
        io.BytesIO(s3.objects[(ARTIFACTS_BUCKET, key)])
    ).to_pylist()


def _state_artifact(s3: _S3) -> dict[str, Any]:
    return json.loads(s3.objects[(ARTIFACTS_BUCKET, STATE_KEY)])


def _bronze_records(s3: _S3, run_id: str) -> list[dict[str, Any]]:
    prefix = f"{REPOSITORY}/{run_id}/"
    records = []
    for (bucket, key), body in sorted(s3.objects.items()):
        if bucket != BRONZE_BUCKET or not key.startswith(prefix):
            continue
        records.extend(json.loads(line) for line in body.splitlines())
    return records


def _assert_metrics(
    test: TestCase,
    cloudwatch: _CloudWatch,
    status: Literal["success", "empty", "fail"],
    corrupted_count: int,
) -> None:
    test.assertTrue(
        all(call["Namespace"] == CLOUDWATCH_NAMESPACE for call in cloudwatch.calls)
    )
    metrics = [metric for call in cloudwatch.calls for metric in call["MetricData"]]
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
        metric for metric in metrics if metric["MetricName"] == "job_finish_status"
    ]
    test.assertEqual(len(finish), 1)
    test.assertEqual(finish[0]["Value"], 1)
    for metric in metrics:
        test.assertIn(
            {"Name": "stage", "Value": "poller"},
            metric["Dimensions"],
        )
    test.assertIn(
        {"Name": "status", "Value": status},
        finish[0]["Dimensions"],
    )


def _metric_values(
    metrics: list[dict[str, Any]],
    name: str,
) -> list[int | float]:
    return [metric["Value"] for metric in metrics if metric["MetricName"] == name]


class _Reply:
    def __init__(
        self,
        payload: Any,
        status: int = 200,
        next_url: str | None = None,
        empty_body: bool = False,
        headers: dict[str, str] | None = None,
    ) -> None:
        self.status_code = status
        self.headers = dict(headers or {})
        self.content = b"" if empty_body else _json_bytes(payload)
        if next_url:
            self.headers["Link"] = f'<{next_url}>; rel="next"'

    def json(self) -> Any:
        return json.loads(self.content)


class _GitHub:
    def __init__(
        self,
        routes: dict[tuple[str, int], _Reply | Exception | list[_Reply | Exception]],
    ) -> None:
        self.routes = {
            key: replies if isinstance(replies, list) else [replies]
            for key, replies in routes.items()
        }
        self.calls: list[str] = []

    def get(
        self,
        url: str,
        headers: dict[str, str],
        params: dict[str, str | int] | None = None,
        timeout: float | None = None,
    ) -> _Reply:
        if params:
            url = f"{url}?{urlencode(params)}"
        self.calls.append(url)
        parsed = urlsplit(url)
        query = parse_qs(parsed.query)
        page = int(query["page"][0]) if "page" in query else 1
        reply = self.routes[(parsed.path, page)].pop(0)
        if isinstance(reply, Exception):
            raise reply
        return reply


class _S3:
    def __init__(
        self,
        objects: dict[tuple[str, str], bytes] | None = None,
        failed_puts: set[tuple[str, str]] | None = None,
        failed_put_numbers: dict[tuple[str, str], int] | None = None,
    ) -> None:
        self.objects = dict(objects or {})
        self.failed_puts = set(failed_puts or set())
        self.failed_put_numbers = dict(failed_put_numbers or {})
        self.put_calls: list[tuple[str, str]] = []

    def get_object(self, Bucket: str, Key: str) -> dict[str, io.BytesIO]:
        return {"Body": io.BytesIO(self.objects[(Bucket, Key)])}

    def put_object(
        self,
        Bucket: str,
        Key: str,
        Body: bytes | BinaryIO,
        **kwargs: Any,
    ) -> None:
        try:
            body = Body.read()
        except AttributeError:
            body = bytes(Body)
        self.put_calls.append((Bucket, Key))
        if (Bucket, Key) in self.failed_puts or self.put_calls.count(
            (Bucket, Key)
        ) == self.failed_put_numbers.get((Bucket, Key)):
            raise _UploadFailed(f"failed to upload s3://{Bucket}/{Key}")
        self.objects[(Bucket, Key)] = body


class _CloudWatch:
    def __init__(self) -> None:
        self.calls = []

    def put_metric_data(self, **kwargs: Any) -> None:
        self.calls.append(kwargs)


class _UploadFailed(RuntimeError):
    pass
