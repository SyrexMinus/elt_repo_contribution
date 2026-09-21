from __future__ import annotations

import io
import json
import re
import sys
import time
import uuid
from datetime import datetime, timezone
from typing import Any
from urllib.parse import unquote, urlsplit

import boto3
import pyarrow as arrow
from pyarrow import parquet
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as functions
from pyspark.sql import types


_TABLE_COLUMNS = {
    "pull": [
        "repo",
        "pr_num",
        "merged_at",
        "user_login",
        "state",
        "created_at",
        "updated_at",
        "title",
        "parse_run_id",
    ],
    "file": [
        "repo",
        "pr_num",
        "filename",
        "additions",
        "deletions",
        "parse_run_id",
    ],
    "review": [
        "repo",
        "pr_num",
        "user_login",
        "state",
        "parse_run_id",
    ],
}
_PRIMARY_KEYS = {
    "pull": ["repo", "pr_num"],
    "file": ["repo", "pr_num", "filename"],
    "review": ["repo", "pr_num", "user_login"],
}
_PARSED_SCHEMA = types.StructType(
    [
        types.StructField("table", types.StringType(), False),
        types.StructField("valid", types.BooleanType(), False),
        types.StructField("repo", types.StringType()),
        types.StructField("pr_num", types.LongType()),
        types.StructField("merged_at", types.TimestampType()),
        types.StructField("user_login", types.StringType()),
        types.StructField("state", types.StringType()),
        types.StructField("created_at", types.TimestampType()),
        types.StructField("updated_at", types.TimestampType()),
        types.StructField("title", types.StringType()),
        types.StructField("filename", types.StringType()),
        types.StructField("additions", types.LongType()),
        types.StructField("deletions", types.LongType()),
        types.StructField("parse_run_id", types.StringType()),
        types.StructField("submitted_at", types.TimestampType()),
    ]
)


# TODO optimize
def silver(
    event: dict[str, Any],
    spark: SparkSession,
    s3: Any,
    cloudwatch: Any,
) -> dict[str, Any]:
    started_at = time.monotonic()
    status = "fail"
    corrupted_count = 0
    cached_frames: list[DataFrame] = []
    _report_job_started(cloudwatch, event["cloudwatch_namespace"])

    try:
        manifest = _read_parquet(s3, event["stage1_artifact_uri"])
        bronze_uris = [record["s3_uri"] for record in manifest]
        if bronze_uris:
            lines = spark.read.text([_spark_uri(uri) for uri in bronze_uris])
        else:
            lines = spark.createDataFrame([], "value string")

        grouped_source_counts = {
            row["entity"]: row["count"]
            for row in lines.select(
                functions.get_json_object("value", "$.entity").alias("entity")
            )
            .groupBy("entity")
            .count()
            .collect()
        }
        source_row_count = sum(grouped_source_counts.values())
        source_counts = {
            table: grouped_source_counts.get(table, 0) for table in _TABLE_COLUMNS
        }

        if source_row_count:
            tables, corrupted_tables = _transform_records(lines)
            cached_frames = [
                frame.cache()
                for frame in [*tables.values(), *corrupted_tables.values()]
            ]
            output_counts = {
                table: frame.count() for table, frame in tables.items()
            }
            corrupted_counts = {
                table: frame.count()
                for table, frame in corrupted_tables.items()
            }
        else:
            tables = {}
            corrupted_tables = {}
            output_counts = {table: 0 for table in _TABLE_COLUMNS}
            corrupted_counts = {table: 0 for table in _TABLE_COLUMNS}

        corrupted_count = sum(corrupted_counts.values())
        for table, count in corrupted_counts.items():
            if count:
                _store_raw_rows(
                    s3=s3,
                    bucket=event["artifacts_bucket"],
                    key=(
                        f"corrupted-artifacts/{event['run_id']}/silver/"
                        f"{table}.parquet"
                    ),
                    rows=corrupted_tables[table],
                )
        if corrupted_count:
            raise RuntimeError(
                f"Silver input contains {corrupted_count} corrupted rows"
            )

        if source_row_count:
            _merge_tables(
                tables=tables,
                snowflake_options=event["snowflake_options"],
            )

        artifact_key = f"pipeline-artifacts/{event['run_id']}/silver/output.parquet"
        _store_artifact(
            s3=s3,
            bucket=event["artifacts_bucket"],
            key=artifact_key,
            run_id=event["run_id"],
            source_counts=source_counts,
            output_counts=output_counts,
            corrupted_counts=corrupted_counts,
        )
        status = "success" if source_row_count else "empty"
        return {
            "run_id": event["run_id"],
            "status": status,
            "artifact_uri": f"s3://{event['artifacts_bucket']}/{artifact_key}",
            "source_row_count": source_row_count,
            "output_row_count": sum(output_counts.values()),
            "corrupted_count": corrupted_count,
        }
    finally:
        for frame in cached_frames:
            frame.unpersist()
        _report_job_finished(
            cloudwatch=cloudwatch,
            namespace=event["cloudwatch_namespace"],
            status=status,
            corrupted_count=corrupted_count,
            duration=time.monotonic() - started_at,
        )


def main(argv: list[str]) -> dict[str, Any]:
    if len(argv) != 1:
        raise SystemExit("usage: silver_job.py '<event-json>'")
    spark = SparkSession.builder.appName("repo-contribution-silver").getOrCreate()
    return silver(
        event=json.loads(argv[0]),
        spark=spark,
        s3=boto3.client("s3"),
        cloudwatch=boto3.client("cloudwatch"),
    )


def _transform_records(
    lines: DataFrame,
) -> tuple[dict[str, DataFrame], dict[str, DataFrame]]:
    parse_record = functions.udf(_parse_record, _PARSED_SCHEMA)
    parsed = lines.select(
        functions.col("value").alias("raw"),
        parse_record("value").alias("record"),
    ).select("raw", "record.*")

    windows = {
        "pull": Window.partitionBy(*_PRIMARY_KEYS["pull"]).orderBy(
            functions.col("updated_at").desc(),
            functions.col("parse_run_id").desc(),
        ),
        "file": Window.partitionBy(*_PRIMARY_KEYS["file"]).orderBy(
            functions.col("parse_run_id").desc()
        ),
        "review": Window.partitionBy(*_PRIMARY_KEYS["review"]).orderBy(
            functions.col("submitted_at").desc(),
            functions.col("parse_run_id").desc(),
        ),
    }
    tables = {
        table: parsed.filter(
            (functions.col("table") == table) & functions.col("valid")
        )
        .withColumn("row_number", functions.row_number().over(windows[table]))
        .filter(functions.col("row_number") == 1)
        .select(*columns)
        for table, columns in _TABLE_COLUMNS.items()
    }
    corrupted_tables = {
        table: parsed.filter(
            (functions.col("table") == table) & ~functions.col("valid")
        ).select(functions.col("raw").alias("value"))
        for table in _TABLE_COLUMNS
    }
    return tables, corrupted_tables


def _merge_tables(
    tables: dict[str, DataFrame],
    snowflake_options: dict[str, str],
) -> None:
    for table, frame in tables.items():
        if frame.isEmpty():
            continue

        stage = f"SILVER_{table.upper()}_{uuid.uuid4().hex.upper()}"
        columns = _TABLE_COLUMNS[table]
        frame.select(
            *[
                functions.col(column).alias(column.upper())
                for column in columns
            ]
        ).write.format("net.snowflake.spark.snowflake").options(
            **snowflake_options
        ).option("dbtable", stage).mode("overwrite").save()

        assignments = ", ".join(
            f"{column.upper()} = s.{column.upper()}" for column in columns
        )
        names = ", ".join(column.upper() for column in columns)
        values = ", ".join(f"s.{column.upper()}" for column in columns)
        condition = " AND ".join(
            f"t.{column.upper()} = s.{column.upper()}"
            for column in _PRIMARY_KEYS[table]
        )
        merge = (
            f"MERGE INTO {table.upper()} t USING {stage} s ON {condition} "
            "WHEN MATCHED AND s.PARSE_RUN_ID > t.PARSE_RUN_ID THEN "
            f"UPDATE SET {assignments} "
            f"WHEN NOT MATCHED THEN INSERT ({names}) VALUES ({values})"
        )
        try:
            _run_snowflake_query(frame.sparkSession, snowflake_options, merge)
        finally:
            _run_snowflake_query(
                frame.sparkSession,
                snowflake_options,
                f"DROP TABLE IF EXISTS {stage}",
            )


def _parse_record(value: str) -> tuple[Any, ...]:
    table = "pull"
    try:
        record = json.loads(value)
        if not isinstance(record, dict):
            return _invalid_record(table)
        entity = record.get("entity")
        if entity in _TABLE_COLUMNS:
            table = entity
        if entity not in _TABLE_COLUMNS:
            return _invalid_record(table)

        run_id = record.get("run_id")
        repo = record.get("repo")
        pr_num = record.get("pr_num")
        payload = record.get("payload")
        if not _is_nonempty_string(run_id) or not re.fullmatch(
            r"\d{8}T\d{12}Z", run_id
        ):
            return _invalid_record(table)
        datetime.strptime(run_id, "%Y%m%dT%H%M%S%fZ")
        if (
            not _is_nonempty_string(repo)
            or not _is_int(pr_num)
            or pr_num <= 0
            or not isinstance(payload, dict)
        ):
            return _invalid_record(table)

        if table == "pull":
            user_login = _nested_login(payload)
            state = payload.get("state")
            title = payload.get("title")
            created_at = _timestamp(payload.get("created_at"))
            updated_at = _timestamp(payload.get("updated_at"))
            merged_value = payload.get("merged_at")
            merged_at = None if merged_value is None else _timestamp(merged_value)
            if not all(
                _is_nonempty_string(item)
                for item in (user_login, state, title)
            ):
                return _invalid_record(table)
            return (
                table,
                True,
                repo,
                pr_num,
                merged_at,
                user_login,
                state,
                created_at,
                updated_at,
                title,
                None,
                None,
                None,
                run_id,
                None,
            )

        if table == "file":
            filename = payload.get("filename")
            additions = payload.get("additions")
            deletions = payload.get("deletions")
            if (
                not _is_nonempty_string(filename)
                or not _is_int(additions)
                or additions < 0
                or not _is_int(deletions)
                or deletions < 0
            ):
                return _invalid_record(table)
            return (
                table,
                True,
                repo,
                pr_num,
                None,
                None,
                None,
                None,
                None,
                None,
                filename,
                additions,
                deletions,
                run_id,
                None,
            )

        user_login = _nested_login(payload)
        state = payload.get("state")
        submitted_at = _timestamp(payload.get("submitted_at"))
        if not all(_is_nonempty_string(item) for item in (user_login, state)):
            return _invalid_record(table)
        return (
            table,
            True,
            repo,
            pr_num,
            None,
            user_login,
            state,
            None,
            None,
            None,
            None,
            None,
            None,
            run_id,
            submitted_at,
        )
    except (TypeError, ValueError, json.JSONDecodeError):
        return _invalid_record(table)


def _invalid_record(table: str) -> tuple[Any, ...]:
    return (table, False, *([None] * 13))


def _nested_login(payload: dict[str, Any]) -> Any:
    user = payload.get("user")
    return user.get("login") if isinstance(user, dict) else None


def _is_nonempty_string(value: Any) -> bool:
    return isinstance(value, str) and bool(value.strip())


def _is_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _timestamp(value: Any) -> datetime:
    if not isinstance(value, str):
        raise ValueError("timestamp is not a string")
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(timezone.utc).replace(tzinfo=None)
    return parsed


def _read_parquet(s3: Any, uri: str) -> list[dict[str, Any]]:
    bucket, key = _s3_location(uri)
    body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
    return parquet.read_table(io.BytesIO(body)).to_pylist()


def _s3_location(uri: str) -> tuple[str, str]:
    parsed = urlsplit(uri)
    if parsed.scheme != "s3" or not parsed.netloc or not parsed.path:
        raise ValueError(f"invalid S3 URI: {uri}")
    return parsed.netloc, unquote(parsed.path.lstrip("/"))


def _spark_uri(uri: str) -> str:
    return f"s3a://{uri.removeprefix('s3://')}" if uri.startswith("s3://") else uri


def _store_raw_rows(
    s3: Any,
    bucket: str,
    key: str,
    rows: DataFrame,
) -> None:
    _store_parquet(
        s3=s3,
        bucket=bucket,
        key=key,
        table=arrow.Table.from_pylist(
            [{"value": row["value"]} for row in rows.collect()],
            schema=arrow.schema([("value", arrow.string())]),
        ),
    )


def _store_artifact(
    s3: Any,
    bucket: str,
    key: str,
    run_id: str,
    source_counts: dict[str, int],
    output_counts: dict[str, int],
    corrupted_counts: dict[str, int],
) -> None:
    schema = arrow.schema(
        [
            ("run_id", arrow.string()),
            ("table", arrow.string()),
            ("source_row_count", arrow.int64()),
            ("output_row_count", arrow.int64()),
            ("corrupted_row_count", arrow.int64()),
        ]
    )
    _store_parquet(
        s3=s3,
        bucket=bucket,
        key=key,
        table=arrow.Table.from_pylist(
            [
                {
                    "run_id": run_id,
                    "table": table,
                    "source_row_count": source_counts[table],
                    "output_row_count": output_counts[table],
                    "corrupted_row_count": corrupted_counts[table],
                }
                for table in _TABLE_COLUMNS
            ],
            schema=schema,
        ),
    )


def _store_parquet(
    s3: Any,
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


def _run_snowflake_query(
    spark: SparkSession,
    snowflake_options: dict[str, str],
    query: str,
) -> None:
    spark.sparkContext._jvm.net.snowflake.spark.snowflake.Utils.runQuery(
        snowflake_options,
        query,
    )


def _report_job_started(cloudwatch: Any, namespace: str) -> None:
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
    cloudwatch: Any,
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


if __name__ == "__main__":
    main(sys.argv[1:])
