from __future__ import annotations

import io
import json
import sys
import tempfile
import time
import uuid
from typing import Any
from urllib.parse import unquote, urlsplit

import boto3
import pyarrow as arrow
from pyarrow import parquet
from pydantic import BaseModel, ConfigDict
from pyspark import StorageLevel
from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as functions
from pyspark.sql import types


_TABLE_SCHEMAS = {
    "pull": types.StructType(
        [
            types.StructField("repo", types.StringType()),
            types.StructField("pr_num", types.LongType()),
            types.StructField("merged_at", types.TimestampType()),
            types.StructField("user_login", types.StringType()),
            types.StructField("state", types.StringType()),
            types.StructField("created_at", types.TimestampType()),
            types.StructField("updated_at", types.TimestampType()),
            types.StructField("title", types.StringType()),
            types.StructField("parse_run_id", types.StringType()),
        ]
    ),
    "file": types.StructType(
        [
            types.StructField("repo", types.StringType()),
            types.StructField("pr_num", types.LongType()),
            types.StructField("filename", types.StringType()),
            types.StructField("additions", types.LongType()),
            types.StructField("deletions", types.LongType()),
            types.StructField("parse_run_id", types.StringType()),
        ]
    ),
    "review": types.StructType(
        [
            types.StructField("repo", types.StringType()),
            types.StructField("pr_num", types.LongType()),
            types.StructField("user_login", types.StringType()),
            types.StructField("state", types.StringType()),
            types.StructField("parse_run_id", types.StringType()),
        ]
    ),
}
_TABLE_COLUMNS = {
    table: schema.fieldNames() for table, schema in _TABLE_SCHEMAS.items()
}
_PRIMARY_KEYS = {
    "pull": ["repo", "pr_num"],
    "file": ["repo", "pr_num", "filename"],
    "review": ["repo", "pr_num", "user_login"],
}
_PAYLOAD_SCHEMA = types.StructType(
    [
        types.StructField("merged_at", types.StringType()),
        types.StructField(
            "user",
            types.StructType([types.StructField("login", types.StringType())]),
        ),
        types.StructField("state", types.StringType()),
        types.StructField("created_at", types.StringType()),
        types.StructField("updated_at", types.StringType()),
        types.StructField("title", types.StringType()),
        types.StructField("filename", types.StringType()),
        types.StructField("additions", types.LongType()),
        types.StructField("deletions", types.LongType()),
        types.StructField("submitted_at", types.StringType()),
    ]
)
_SOURCE_SCHEMA = types.StructType(
    [
        types.StructField("run_id", types.StringType()),
        types.StructField("repo", types.StringType()),
        types.StructField("pr_num", types.LongType()),
        types.StructField("payload", types.StringType()),
    ]
)


def silver(
    event: dict[str, Any],
    spark: SparkSession,
    s3: Any,
    cloudwatch: Any,
) -> dict[str, Any]:
    config = _SilverEvent.model_validate(event)
    started_at = time.monotonic()
    status = "fail"
    corrupted_count = 0
    cached_frames: list[DataFrame] = []
    _report_job_started(
        cloudwatch=cloudwatch,
        namespace=config.cloudwatch_namespace,
    )

    try:
        manifest = _read_parquet(
            s3=s3,
            uri=config.stage1_artifact_uri,
        )
        source_row_count = sum(record["record_count"] for record in manifest)
        source_counts = {table: 0 for table in _TABLE_COLUMNS}
        output_counts = {table: 0 for table in _TABLE_COLUMNS}
        corrupted_counts = {table: 0 for table in _TABLE_COLUMNS}

        if source_row_count:
            lines = spark.read.text(
                [_spark_uri(record["s3_uri"]) for record in manifest]
            )
            parsed = _parse_records(lines).persist(StorageLevel.MEMORY_AND_DISK)
            cached_frames.append(parsed)
            source_counts, corrupted_counts = _count_parsed_records(parsed)

        corrupted_count = sum(corrupted_counts.values())
        if corrupted_count:
            for table, count in corrupted_counts.items():
                if not count:
                    continue
                _store_raw_rows(
                    s3=s3,
                    bucket=config.artifacts_bucket,
                    key=(
                        f"corrupted-artifacts/{config.run_id}/silver/"
                        f"{table}.parquet"
                    ),
                    rows=parsed.filter(
                        (functions.col("table") == table)
                        & ~functions.col("valid")
                    ).select(functions.col("raw").alias("value")),
                )
            raise RuntimeError(
                f"Silver input contains {corrupted_count} corrupted rows"
            )

        if source_row_count:
            tables = {
                table: frame.persist(StorageLevel.MEMORY_AND_DISK)
                for table, frame in _build_tables(parsed).items()
            }
            cached_frames.extend(tables.values())
            output_counts = _count_tables(tables)
            _merge_tables(
                tables={
                    table: frame
                    for table, frame in tables.items()
                    if output_counts[table]
                },
                snowflake_options=config.snowflake_options.model_dump(
                    exclude_none=True
                ),
            )

        artifact_key = f"pipeline-artifacts/{config.run_id}/silver/output.parquet"
        _store_artifact(
            s3=s3,
            bucket=config.artifacts_bucket,
            key=artifact_key,
            run_id=config.run_id,
            source_counts=source_counts,
            output_counts=output_counts,
            corrupted_counts=corrupted_counts,
        )
        status = "success" if source_row_count else "empty"
        return {
            "run_id": config.run_id,
            "status": status,
            "artifact_uri": f"s3://{config.artifacts_bucket}/{artifact_key}",
            "source_row_count": source_row_count,
            "output_row_count": sum(output_counts.values()),
            "corrupted_count": corrupted_count,
        }
    finally:
        for frame in cached_frames:
            frame.unpersist()
        _report_job_finished(
            cloudwatch=cloudwatch,
            namespace=config.cloudwatch_namespace,
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


class _SnowflakeOptions(BaseModel):
    model_config = ConfigDict(extra="allow")

    sfURL: str
    sfUser: str
    sfPassword: str
    sfDatabase: str
    sfSchema: str
    sfWarehouse: str
    sfRole: str | None = None


class _SilverEvent(BaseModel):
    model_config = ConfigDict(extra="allow")

    run_id: str
    stage1_artifact_uri: str
    artifacts_bucket: str
    cloudwatch_namespace: str
    snowflake_options: _SnowflakeOptions


def _parse_records(lines: DataFrame) -> DataFrame:
    source = lines.select(
        functions.col("value").alias("raw"),
        functions.get_json_object("value", "$.entity").alias("entity"),
        functions.from_json("value", _SOURCE_SCHEMA).alias("source"),
    ).select("raw", "entity", "source.*")
    source = source.select(
        "raw",
        "entity",
        "run_id",
        "repo",
        "pr_num",
        functions.col("payload").alias("_payload_source"),
        functions.from_json("payload", _PAYLOAD_SCHEMA).alias("payload"),
    )
    parsed = source.select(
        "raw",
        "entity",
        functions.when(
            functions.col("entity").isin(*_TABLE_COLUMNS),
            functions.col("entity"),
        )
        .otherwise(functions.lit("pull"))
        .alias("table"),
        "repo",
        "pr_num",
        "_payload_source",
        functions.col("payload").alias("_payload"),
        functions.col("payload.merged_at").alias("_merged_at_source"),
        functions.try_to_timestamp("payload.merged_at").alias("merged_at"),
        functions.col("payload.user.login").alias("user_login"),
        functions.col("payload.state").alias("state"),
        functions.try_to_timestamp("payload.created_at").alias("created_at"),
        functions.try_to_timestamp("payload.updated_at").alias("updated_at"),
        functions.col("payload.title").alias("title"),
        functions.col("payload.filename").alias("filename"),
        functions.col("payload.additions").alias("additions"),
        functions.col("payload.deletions").alias("deletions"),
        functions.col("run_id").alias("parse_run_id"),
        functions.try_to_timestamp(
            "run_id",
            functions.lit("yyyyMMdd'T'HHmmssSSSSSS'Z'"),
        ).alias("_parsed_run_id"),
        functions.try_to_timestamp("payload.submitted_at").alias("submitted_at"),
    )

    common_valid = (
        functions.col("entity").isin(*_TABLE_COLUMNS)
        & _is_nonempty_string("parse_run_id")
        & _is_json_string(column="raw", field="run_id")
        & functions.col("parse_run_id").rlike(r"^\d{8}T\d{12}Z$")
        & functions.col("_parsed_run_id").isNotNull()
        & _is_nonempty_string("repo")
        & _is_json_string(column="raw", field="repo")
        & functions.col("pr_num").isNotNull()
        & (functions.col("pr_num") > 0)
        & _is_json_object(column="raw", field="payload")
        & functions.col("_payload_source").isNotNull()
        & functions.col("_payload").isNotNull()
    )
    valid = (
        functions.when(
            functions.col("entity") == "pull",
            common_valid
            & _is_nonempty_string("user_login")
            & _is_json_string(column="_payload_source", field="login")
            & _is_nonempty_string("state")
            & _is_json_string(column="_payload_source", field="state")
            & _is_nonempty_string("title")
            & _is_json_string(column="_payload_source", field="title")
            & _is_json_string(column="_payload_source", field="created_at")
            & functions.col("created_at").isNotNull()
            & _is_json_string(column="_payload_source", field="updated_at")
            & functions.col("updated_at").isNotNull()
            & (
                functions.col("_merged_at_source").isNull()
                | (
                    _is_json_string(
                        column="_payload_source",
                        field="merged_at",
                    )
                    & functions.col("merged_at").isNotNull()
                )
            ),
        )
        .when(
            functions.col("entity") == "file",
            common_valid
            & _is_nonempty_string("filename")
            & _is_json_string(column="_payload_source", field="filename")
            & functions.col("additions").isNotNull()
            & (functions.col("additions") >= 0)
            & functions.col("deletions").isNotNull()
            & (functions.col("deletions") >= 0),
        )
        .when(
            functions.col("entity") == "review",
            common_valid
            & _is_nonempty_string("user_login")
            & _is_json_string(column="_payload_source", field="login")
            & _is_nonempty_string("state")
            & _is_json_string(column="_payload_source", field="state")
            & _is_json_string(column="_payload_source", field="submitted_at")
            & functions.col("submitted_at").isNotNull(),
        )
        .otherwise(functions.lit(False))
    )
    return parsed.withColumn("valid", valid).drop(
        "_payload",
        "_payload_source",
        "_merged_at_source",
        "_parsed_run_id",
    )


def _build_tables(parsed: DataFrame) -> dict[str, DataFrame]:
    ordering_columns = {
        "pull": ["updated_at", "parse_run_id"],
        "file": ["parse_run_id"],
        "review": ["submitted_at", "parse_run_id"],
    }
    tables = {}
    for table, columns in _TABLE_COLUMNS.items():
        primary_keys = _PRIMARY_KEYS[table]
        value_columns = [column for column in columns if column not in primary_keys]
        latest_columns = [
            *value_columns,
            *[
                column
                for column in ordering_columns[table]
                if column not in value_columns
            ],
        ]
        tables[table] = (
            parsed.filter(
                (functions.col("table") == table) & functions.col("valid")
            )
            .groupBy(*primary_keys)
            .agg(
                functions.max_by(
                    functions.struct(*latest_columns),
                    functions.struct(*ordering_columns[table]),
                ).alias("latest")
            )
            .select(*primary_keys, "latest.*")
            .select(*columns)
        )
    return tables


def _count_parsed_records(
    parsed: DataFrame,
) -> tuple[dict[str, int], dict[str, int]]:
    counts = parsed.agg(
        *[
            functions.count(
                functions.when(functions.col("entity") == table, 1)
            ).alias(f"source_{table}")
            for table in _TABLE_COLUMNS
        ],
        *[
            functions.count(
                functions.when(
                    (functions.col("table") == table)
                    & ~functions.col("valid"),
                    1,
                )
            ).alias(f"corrupted_{table}")
            for table in _TABLE_COLUMNS
        ],
    ).first()
    return (
        {table: counts[f"source_{table}"] for table in _TABLE_COLUMNS},
        {table: counts[f"corrupted_{table}"] for table in _TABLE_COLUMNS},
    )


def _count_tables(tables: dict[str, DataFrame]) -> dict[str, int]:
    count_frames = [
        frame.agg(functions.count(functions.lit(1)).alias("count")).select(
            functions.lit(table).alias("table"),
            "count",
        )
        for table, frame in tables.items()
    ]
    counts = count_frames[0]
    for count_frame in count_frames[1:]:
        counts = counts.unionByName(count_frame)
    return {row["table"]: row["count"] for row in counts.collect()}


def _merge_tables(
    tables: dict[str, DataFrame],
    snowflake_options: dict[str, str],
) -> None:
    for table, frame in tables.items():
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
            _run_snowflake_query(
                spark=frame.sparkSession,
                snowflake_options=snowflake_options,
                query=merge,
            )
        finally:
            _run_snowflake_query(
                spark=frame.sparkSession,
                snowflake_options=snowflake_options,
                query=f"DROP TABLE IF EXISTS {stage}",
            )


def _is_nonempty_string(column: str) -> Column:
    return functions.col(column).isNotNull() & (
        functions.length(functions.trim(functions.col(column))) > 0
    )


def _is_json_string(column: str, field: str) -> Column:
    # Spark coerces JSON scalars to StringType, so retain strict source-type checks.
    return functions.col(column).rlike(rf'(?s)"{field}"\s*:\s*"')


def _is_json_object(column: str, field: str) -> Column:
    return functions.col(column).rlike(rf'(?s)"{field}"\s*:\s*\{{')


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
    schema = arrow.schema([("value", arrow.string())])
    with tempfile.SpooledTemporaryFile(max_size=8 * 1024 * 1024) as body:
        with parquet.ParquetWriter(body, schema) as writer:
            batch = []
            for row in rows.toLocalIterator():
                batch.append({"value": row["value"]})
                if len(batch) == 10_000:
                    writer.write_table(
                        arrow.Table.from_pylist(batch, schema=schema)
                    )
                    batch.clear()
            if batch:
                writer.write_table(
                    arrow.Table.from_pylist(batch, schema=schema)
                )
        body.seek(0)
        s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=body,
            ContentType="application/vnd.apache.parquet",
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
