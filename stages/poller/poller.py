from __future__ import annotations

import io
import time
from collections.abc import Iterator, Mapping
from typing import Any, Literal, Protocol
from urllib.parse import parse_qs, urlencode, urlsplit

import pyarrow as arrow
from pyarrow import parquet
from pydantic import BaseModel, Field


def poller(
    event: dict[str, Any],
    s3: _S3Client,
    github: _GitHubClient,
    cloudwatch: _CloudWatchClient,
) -> dict[str, Any]:
    config = _PollerEvent.model_validate(event)
    started_at = time.monotonic()
    status = "fail"
    corrupted_count = 0
    _report_job_started(
        cloudwatch=cloudwatch,
        namespace=config.cloudwatch_namespace,
    )

    try:
        artifacts: list[_ArtifactRecord] = []
        states: list[tuple[str, _PollerState]] = []
        pull_count = 0

        for repo in config.repositories:
            state = _load_poller_state(
                s3=s3,
                bucket=config.artifacts_bucket,
                repository=repo,
            )
            try:
                repo_artifacts, pull_cnt, next_state = _poll_and_store_repo_events(
                    event=config,
                    repository=repo,
                    state=state,
                    s3=s3,
                    github=github,
                )
            except _CorruptedResponse as error:
                corrupted_count = 1
                _store_corrupted_artifact(
                    s3=s3,
                    bucket=config.artifacts_bucket,
                    key=(f"corrupted-artifacts/{config.run_id}/stage1/output.parquet"),
                    records=[error.record],
                )
                raise RuntimeError(
                    f"GitHub returned a corrupted response for {error.record.url}"
                ) from error

            artifacts.extend(repo_artifacts)
            pull_count += pull_cnt
            if pull_cnt:
                states.append((repo, next_state))

        artifact_key = f"pipeline-artifacts/{config.run_id}/stage1/output.parquet"
        _store_stage_output_manifest(
            s3=s3,
            bucket=config.artifacts_bucket,
            key=artifact_key,
            records=artifacts,
        )

        for repo, state in states:
            _store_poller_state(
                s3=s3,
                bucket=config.artifacts_bucket,
                repository=repo,
                state=state,
            )

        status = "success" if pull_count else "empty"
        return _PollerResult(
            run_id=config.run_id,
            status=status,
            artifact_uri=f"s3://{config.artifacts_bucket}/{artifact_key}",
            bronze_objects=len(artifacts),
            pull_requests=pull_count,
            corrupted_count=corrupted_count,
        ).model_dump()
    finally:
        _report_job_finished(
            cloudwatch=cloudwatch,
            namespace=config.cloudwatch_namespace,
            status=status,
            corrupted_count=corrupted_count,
            duration=time.monotonic() - started_at,
        )


class _PollerEvent(BaseModel):
    repositories: list[str]
    github_token: str
    github_api_url: str
    mode: Literal["incremental", "backfill"]
    run_id: str
    bronze_bucket: str
    artifacts_bucket: str
    cloudwatch_namespace: str
    batch_max_pull_requests: int
    batch_max_bytes: int
    request_timeout_seconds: float


class _PullRequestState(BaseModel):
    updated_at: str
    merged: bool


class _PollerState(BaseModel):
    last_successful_watermark: str | None = None
    pull_requests: dict[str, _PullRequestState] = Field(default_factory=dict)


class _BronzeRecord(BaseModel):
    run_id: str
    repo: str
    pr_num: int
    entity: Literal["pull", "file", "review"]
    payload: dict[str, Any]
    produced_new_snapshot: bool | None = None


class _ArtifactRecord(BaseModel):
    run_id: str
    repo: str
    s3_uri: str
    pull_request_count: int
    record_count: int


class _QuarantineRecord(BaseModel):
    repo: str
    entity: Literal["pull", "file", "review"]
    status: int
    url: str
    query: dict[str, list[str]]


class _PollerResult(BaseModel):
    run_id: str
    status: Literal["success", "empty"]
    artifact_uri: str
    bronze_objects: int
    pull_requests: int
    corrupted_count: int


class _CorruptedResponse(RuntimeError):
    def __init__(self, record: _QuarantineRecord) -> None:
        super().__init__(record.url)
        self.record = record


def _poll_and_store_repo_events(
    event: _PollerEvent,
    repository: str,
    state: _PollerState,
    s3: _S3Client,
    github: _GitHubClient,
) -> tuple[list[_ArtifactRecord], int, _PollerState]:
    watermark = state.last_successful_watermark
    next_state = state.model_copy(deep=True)
    artifacts: list[_ArtifactRecord] = []
    batch: list[bytes] = []
    batch_size = 0
    batch_pulls = 0
    pull_count = 0
    stop = False

    for pulls in _iter_github_pages(
        github_api_url=event.github_api_url,
        github_token=event.github_token,
        request_timeout_seconds=event.request_timeout_seconds,
        repository=repository,
        endpoint="pulls",
        entity="pull",
        github=github,
    ):
        for pull in pulls:
            if (
                event.mode == "incremental"
                and watermark
                and pull["updated_at"] < watermark
            ):
                stop = True
                break

            cached = next_state.pull_requests.get(str(pull["number"]))
            produced_new_snapshot = (
                event.mode == "backfill"
                or not cached
                or (not cached.merged and cached.updated_at != pull["updated_at"])
            )
            records: list[_BronzeRecord] = [
                _BronzeRecord(
                    run_id=event.run_id,
                    repo=repository,
                    pr_num=pull["number"],
                    entity="pull",
                    payload=pull,
                    produced_new_snapshot=produced_new_snapshot,
                )
            ]
            if produced_new_snapshot:
                for entity in ("file", "review"):
                    endpoint = "files" if entity == "file" else "reviews"
                    for items in _iter_github_pages(
                        github_api_url=event.github_api_url,
                        github_token=event.github_token,
                        request_timeout_seconds=event.request_timeout_seconds,
                        repository=repository,
                        endpoint=f"pulls/{pull['number']}/{endpoint}",
                        entity=entity,
                        github=github,
                    ):
                        records.extend(
                            _BronzeRecord(
                                run_id=event.run_id,
                                repo=repository,
                                pr_num=pull["number"],
                                entity=entity,
                                payload=payload,
                            )
                            for payload in items
                        )

            lines = [
                (
                    record.model_dump_json(
                        exclude={"produced_new_snapshot"}
                        if record.entity != "pull"
                        else None
                    )
                    + "\n"
                ).encode()
                for record in records
            ]
            size = sum(map(len, lines))
            if batch and (
                batch_pulls >= event.batch_max_pull_requests
                or batch_size + size > event.batch_max_bytes
            ):
                artifacts.append(
                    _store_bronze_batch(
                        run_id=event.run_id,
                        bronze_bucket=event.bronze_bucket,
                        repository=repository,
                        batch_number=len(artifacts),
                        lines=batch,
                        pull_request_count=batch_pulls,
                        s3=s3,
                    )
                )
                batch = []
                batch_size = 0
                batch_pulls = 0

            batch.extend(lines)
            batch_size += size
            batch_pulls += 1
            pull_count += 1
            next_state.pull_requests[str(pull["number"])] = _PullRequestState(
                updated_at=pull["updated_at"],
                merged=pull.get("merged_at") is not None,
            )
            if (
                next_state.last_successful_watermark is None
                or pull["updated_at"] > next_state.last_successful_watermark
            ):
                next_state.last_successful_watermark = pull["updated_at"]

        if stop:
            break

    if batch:
        artifacts.append(
            _store_bronze_batch(
                run_id=event.run_id,
                bronze_bucket=event.bronze_bucket,
                repository=repository,
                batch_number=len(artifacts),
                lines=batch,
                pull_request_count=batch_pulls,
                s3=s3,
            )
        )

    return artifacts, pull_count, next_state


def _iter_github_pages(
    github_api_url: str,
    github_token: str,
    request_timeout_seconds: float,
    repository: str,
    endpoint: str,
    entity: Literal["pull", "file", "review"],
    github: _GitHubClient,
) -> Iterator[list[dict[str, Any]]]:
    query = urlencode(
        {
            "state": "all",
            "sort": "updated",
            "direction": "desc",
            "per_page": 100,
        }
        if entity == "pull"
        else {"per_page": 100}
    )
    url = f"{github_api_url.rstrip('/')}/repos/{repository}/{endpoint}?{query}"
    headers = {
        "Accept": "application/vnd.github+json",
        "Authorization": f"Bearer {github_token}",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    while url:
        reply = github.get(
            url,
            headers=headers,
            timeout=request_timeout_seconds,
        )
        if reply.status_code != 200 or not reply.content:
            raise _CorruptedResponse(
                _QuarantineRecord(
                    repo=repository,
                    entity=entity,
                    status=reply.status_code,
                    url=url,
                    query=parse_qs(urlsplit(url).query),
                )
            )
        yield reply.json()
        url = _get_next_page_url(reply.headers.get("Link", ""))


def _get_next_page_url(link_header: str) -> str | None:
    for part in link_header.split(","):
        fields = part.split(";")
        if any(field.strip() == 'rel="next"' for field in fields[1:]):
            return fields[0].strip()[1:-1]
    return None


def _store_bronze_batch(
    run_id: str,
    bronze_bucket: str,
    repository: str,
    batch_number: int,
    lines: list[bytes],
    pull_request_count: int,
    s3: _S3Client,
) -> _ArtifactRecord:
    key = f"{repository}/{run_id}/batch-{batch_number:05d}.jsonl"
    s3.put_object(
        Bucket=bronze_bucket,
        Key=key,
        Body=b"".join(lines),
        ContentType="application/x-ndjson",
    )
    return _ArtifactRecord(
        run_id=run_id,
        repo=repository,
        s3_uri=f"s3://{bronze_bucket}/{key}",
        pull_request_count=pull_request_count,
        record_count=len(lines),
    )


def _load_poller_state(
    s3: _S3Client,
    bucket: str,
    repository: str,
) -> _PollerState:
    try:
        obj = s3.get_object(
            Bucket=bucket,
            Key=f"poller-state/{repository}.json",
        )
    except KeyError:
        return _PollerState()
    except s3.exceptions.NoSuchKey:
        return _PollerState()
    return _PollerState.model_validate_json(obj["Body"].read())


def _store_poller_state(
    s3: _S3Client,
    bucket: str,
    repository: str,
    state: _PollerState,
) -> None:
    s3.put_object(
        Bucket=bucket,
        Key=f"poller-state/{repository}.json",
        Body=state.model_dump_json().encode(),
        ContentType="application/json",
    )


def _store_stage_output_manifest(
    s3: _S3Client,
    bucket: str,
    key: str,
    records: list[_ArtifactRecord],
) -> None:
    schema = arrow.schema(
        [
            ("run_id", arrow.string()),
            ("repo", arrow.string()),
            ("s3_uri", arrow.string()),
            ("pull_request_count", arrow.int64()),
            ("record_count", arrow.int64()),
        ]
    )
    _store_parquet(
        s3=s3,
        bucket=bucket,
        key=key,
        table=arrow.Table.from_pylist(
            [record.model_dump() for record in records],
            schema=schema,
        ),
    )


def _store_corrupted_artifact(
    s3: _S3Client,
    bucket: str,
    key: str,
    records: list[_QuarantineRecord],
) -> None:
    _store_parquet(
        s3=s3,
        bucket=bucket,
        key=key,
        table=arrow.Table.from_pylist([record.model_dump() for record in records]),
    )


def _store_parquet(
    s3: _S3Client,
    bucket: str,
    key: str,
    table: arrow.Table,
) -> None:
    body = io.BytesIO()
    parquet.write_table(table, body)
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=body.getvalue(),
        ContentType="application/vnd.apache.parquet",
    )


def _report_job_started(
    cloudwatch: _CloudWatchClient,
    namespace: str,
) -> None:
    cloudwatch.put_metric_data(
        Namespace=namespace,
        MetricData=[
            {
                "MetricName": "job_start_count",
                "Value": 1,
                "Unit": "Count",
            },
            {
                "MetricName": "inflight_job_count",
                "Value": 1,
                "Unit": "Count",
            },
        ],
    )


def _report_job_finished(
    cloudwatch: _CloudWatchClient,
    namespace: str,
    status: str,
    corrupted_count: int,
    duration: float,
) -> None:
    cloudwatch.put_metric_data(
        Namespace=namespace,
        MetricData=[
            {
                "MetricName": "inflight_job_count",
                "Value": 0,
                "Unit": "Count",
            },
            {
                "MetricName": "corrupted_count",
                "Value": corrupted_count,
                "Unit": "Count",
            },
            {
                "MetricName": "job_duration",
                "Value": duration,
                "Unit": "Seconds",
            },
            {
                "MetricName": "job_finish_status",
                "Dimensions": [{"Name": "status", "Value": status}],
                "Value": 1,
                "Unit": "Count",
            },
        ],
    )


class _S3Exceptions(Protocol):
    NoSuchKey: type[Exception]


class _S3Client(Protocol):
    exceptions: _S3Exceptions

    def get_object(self, *, Bucket: str, Key: str) -> dict[str, Any]: ...

    def put_object(
        self,
        *,
        Bucket: str,
        Key: str,
        Body: bytes,
        **kwargs: Any,
    ) -> Any: ...


class _GitHubResponse(Protocol):
    status_code: int
    content: bytes
    headers: Mapping[str, str]

    def json(self) -> Any: ...


class _GitHubClient(Protocol):
    def get(
        self,
        url: str,
        *,
        headers: dict[str, str],
        timeout: float,
    ) -> _GitHubResponse: ...


class _CloudWatchClient(Protocol):
    def put_metric_data(self, **kwargs: Any) -> Any: ...
