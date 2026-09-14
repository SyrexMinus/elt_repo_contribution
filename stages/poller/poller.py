"""STUB: GitHub REST poller -> S3 Bronze (Stage 1).

Target behaviour (README ch.4/7):
- Incremental (default): GET /pulls?state=all&sort=updated&direction=desc,
  page down until updated_at < last_successful_watermark; fetch files/reviews
  on change (merged PRs cached, not re-fetched).
- Backfill (explicit): walk full history, PR-wise.
- Batch to ~1000 PR / ~10MB objects; preserve raw JSON down to 1 response.
- Quarantine status != 200 / empty bodies to corrupted-artifacts.
- Emit CloudWatch metrics: job_start_count, job_duration, job_finish_status.

Writes:
- Bronze:    s3://{BRONZE_BUCKET}/{repo}/{run_id}/...
- Artifacts: s3://{ARTIFACTS_BUCKET}/pipeline-artifacts/{run_id}/stage1/output.parquet
"""


def poller(event, s3, github, cloudwatch):
    # TODO: implement incremental/backfill polling, batching, quarantine, metrics.
    raise NotImplementedError("poller stage not implemented")
