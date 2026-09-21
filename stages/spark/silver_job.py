"""STUB: Spark Silver job (Stage 2).

Target behaviour (README ch.4/5):
- Read raw JSON from Bronze for a run_id.
- Flatten JSON -> rows, select fields, enforce schema (schema-drift check).
- Quarantine rows missing required fields / type violations / out-of-range.
- MERGE INTO the Snowflake pull/file/review tables (upsert), keeping the
  invariant 1 entity = 1 latest row:
    MERGE INTO <t> USING <s> ON <PK>
      WHEN MATCHED AND s.parse_run_id > t.parse_run_id THEN UPDATE ...
      WHEN NOT MATCHED THEN INSERT ...
- Emit CloudWatch metrics; write per-run artifact to
  s3://{ARTIFACTS_BUCKET}/pipeline-artifacts/{run_id}/silver/output.parquet
"""

import sys


def silver(event, spark, s3, cloudwatch):
    raise NotImplementedError("silver spark job not implemented")


def main(argv):
    raise NotImplementedError("silver spark job not implemented")


def _transform_records(lines):
    raise NotImplementedError("silver spark job not implemented")


def _merge_tables(tables, snowflake_options):
    raise NotImplementedError("silver spark job not implemented")


if __name__ == "__main__":
    main(sys.argv[1:])
