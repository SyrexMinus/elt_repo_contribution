from __future__ import annotations

import json
import os
import subprocess
import uuid
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any
from unittest import TestCase

from stages.spark.snowflake_test_helpers import execute, set_up_snowflake

REPOSITORY = "salute-developers/smart_app_framework"
INITIAL_RUN_ID = "20260926T100000000000Z"
INCREMENTAL_RUN_ID = "20260926T110000000000Z"
PROJECT_DIR = Path(__file__).parents[1]
PROFILES_DIR = Path(__file__).parent
SERVING_QUERY_DIR = PROJECT_DIR.parents[1] / "serving" / "queries"


class GoldBuildIntegrationTest(TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        suffix = uuid.uuid4().hex.upper()
        cls.gold_schema = f"DBT_GOLD_TEST_{suffix}"
        set_up_snowflake(
            test_case=cls,
            database=os.environ["SNOWFLAKE_DATABASE"],
            silver_schema=f"DBT_SILVER_TEST_{suffix}",
            additional_schemas=(cls.gold_schema,),
        )
        _insert_initial_fixture(
            connection=cls.snowflake,
            database=cls.database,
            silver_schema=cls.silver_schema,
        )
        cls.dbt_environment = {
            **os.environ,
            "DBT_SILVER_SCHEMA": cls.silver_schema,
            "DBT_GOLD_SCHEMA": cls.gold_schema,
        }

    def test_dbt_build_updates_facts_and_serving_queries(
        self,
    ) -> None:
        self._run_dbt(command="build", arguments=("--full-refresh",))
        self._assert_gold(incremental=False)
        self.assertEqual(
            _model_materializations(),
            {
                "fact_pr_directory": "incremental",
                "fact_review_interactions": "table",
                "fact_time_to_ship": "table",
            },
        )
        self._assert_serving_queries()

        _insert_stale_full_refresh_rows(
            connection=self.snowflake,
            database=self.database,
            gold_schema=self.gold_schema,
        )
        _apply_increment(
            connection=self.snowflake,
            database=self.database,
            silver_schema=self.silver_schema,
        )
        self._run_dbt(command="build")
        self._assert_gold(incremental=True)

        self._run_dbt(command="build")
        self._assert_gold(incremental=True)

    def _assert_gold(self, incremental: bool) -> None:
        gold = self._gold_relations()
        self.assertEqual(
            {relation: len(rows) for relation, rows in gold.items()},
            {
                "FACT_TIME_TO_SHIP": 3 if incremental else 2,
                "FACT_PR_DIRECTORY": 24 if incremental else 19,
                "FACT_REVIEW_INTERACTIONS": 4 if incremental else 3,
            },
        )

        if incremental:
            stable_rows = {
                "FACT_TIME_TO_SHIP": [
                    {
                        "repo": REPOSITORY,
                        "pr_num": 1,
                        "user_login": "SyrexMinus",
                        "merged_at": "2026-09-20T11:01:00",
                        "time_to_ship_minutes": 61,
                        "source_updated_at": "2026-09-20T11:02:00",
                        "source_parse_run_id": INITIAL_RUN_ID,
                    },
                    {
                        "repo": REPOSITORY,
                        "pr_num": 2,
                        "user_login": "dangerink",
                        "merged_at": "2026-09-20T12:30:00",
                        "time_to_ship_minutes": 30,
                        "source_updated_at": "2026-09-20T12:31:00",
                        "source_parse_run_id": INCREMENTAL_RUN_ID,
                    },
                ],
                "FACT_PR_DIRECTORY": [
                    {
                        "repo": REPOSITORY,
                        "pr_num": 1,
                        "nest_level": 2,
                        "dir": "src/core",
                        "user_login": "SyrexMinus",
                        "merged_at": "2026-09-20T11:01:00",
                        "directory_diff": 12,
                        "pr_diff": 29,
                        "source_updated_at": "2026-09-20T11:02:00",
                        "source_parse_run_id": INITIAL_RUN_ID,
                    },
                    {
                        "repo": REPOSITORY,
                        "pr_num": 2,
                        "nest_level": 1,
                        "dir": "src",
                        "user_login": "dangerink",
                        "merged_at": "2026-09-20T12:30:00",
                        "directory_diff": 1,
                        "pr_diff": 1,
                        "source_updated_at": "2026-09-20T12:31:00",
                        "source_parse_run_id": INCREMENTAL_RUN_ID,
                    }
                ],
                "FACT_REVIEW_INTERACTIONS": [
                    {
                        "repo": REPOSITORY,
                        "pr_num": 1,
                        "author": "SyrexMinus",
                        "reviewer": "dangerink",
                        "review_state": "APPROVED",
                        "merged_at": "2026-09-20T11:01:00",
                        "source_updated_at": "2026-09-20T11:02:00",
                        "source_parse_run_id": INITIAL_RUN_ID,
                    },
                    {
                        "repo": REPOSITORY,
                        "pr_num": 2,
                        "author": "dangerink",
                        "reviewer": "SyrexMinus",
                        "review_state": "APPROVED",
                        "merged_at": "2026-09-20T12:30:00",
                        "source_updated_at": "2026-09-20T12:31:00",
                        "source_parse_run_id": INCREMENTAL_RUN_ID,
                    }
                ],
            }
        else:
            stable_rows = {
                "FACT_TIME_TO_SHIP": [
                    {
                        "repo": REPOSITORY,
                        "pr_num": 1,
                        "user_login": "SyrexMinus",
                        "merged_at": "2026-09-20T11:01:00",
                        "time_to_ship_minutes": 61,
                        "source_updated_at": "2026-09-20T11:02:00",
                        "source_parse_run_id": INITIAL_RUN_ID,
                    }
                ],
                "FACT_PR_DIRECTORY": [
                    {
                        "repo": REPOSITORY,
                        "pr_num": 1,
                        "nest_level": 2,
                        "dir": "src/core",
                        "user_login": "SyrexMinus",
                        "merged_at": "2026-09-20T11:01:00",
                        "directory_diff": 12,
                        "pr_diff": 29,
                        "source_updated_at": "2026-09-20T11:02:00",
                        "source_parse_run_id": INITIAL_RUN_ID,
                    }
                ],
                "FACT_REVIEW_INTERACTIONS": [
                    {
                        "repo": REPOSITORY,
                        "pr_num": 1,
                        "author": "SyrexMinus",
                        "reviewer": "dangerink",
                        "review_state": "APPROVED",
                        "merged_at": "2026-09-20T11:01:00",
                        "source_updated_at": "2026-09-20T11:02:00",
                        "source_parse_run_id": INITIAL_RUN_ID,
                    }
                ],
            }

        for relation, rows in stable_rows.items():
            for row in rows:
                with self.subTest(relation=relation, row=row):
                    self.assertIn(row, gold[relation])

        for relation, rows in gold.items():
            with self.subTest(relation=relation):
                self.assertFalse(
                    any(row["repo"] == "stale/full-refresh" for row in rows)
                )
                self.assertFalse(
                    any(
                        row["repo"] == REPOSITORY
                        and row["pr_num"] in ({3} if incremental else {2, 3})
                        for row in rows
                    )
                )

        if incremental:
            self.assertEqual(
                {
                    (
                        row["nest_level"],
                        row["dir"],
                        row["directory_diff"],
                        row["pr_diff"],
                    )
                    for row in gold["FACT_PR_DIRECTORY"]
                    if row["repo"] == REPOSITORY and row["pr_num"] == 2
                },
                {(level, "src", 1, 1) for level in range(1, 6)},
            )

    def _assert_serving_queries(self) -> None:
        filters = {
            ":repo": f"'{REPOSITORY}'",
            "merged_at = :daterange": (
                "merged_at between '2026-09-20 00:00:00' " "and '2026-09-21 23:59:59'"
            ),
        }
        self.assertEqual(
            self._serving_rows(
                filename="time_to_ship_percentiles_per_author.sql",
                replacements=filters,
            ),
            [
                {
                    "user_login": "SyrexMinus",
                    "pr_count": 1,
                    "p50_minutes": 61,
                    "p75_minutes": 61,
                    "p90_minutes": 61,
                },
                {
                    "user_login": "dependabot[bot]",
                    "pr_count": 1,
                    "p50_minutes": 10,
                    "p75_minutes": 10,
                    "p90_minutes": 10,
                },
            ],
        )
        directory_filters = {**filters, ":nest_level": "1"}
        self.assertEqual(
            self._serving_rows(
                filename="pr_count_per_directory.sql",
                replacements=directory_filters,
            ),
            [
                {"dir": ".", "pr_count": 2},
                {"dir": "src", "pr_count": 1},
            ],
        )
        self.assertEqual(
            self._serving_rows(
                filename="approved_review_count_by_author_and_reviewer.sql",
                replacements=filters,
            ),
            [
                {
                    "author": "dependabot[bot]",
                    "reviewer": "robot-reviewer[bot]",
                    "approved_pr_count": 1,
                },
                {
                    "author": "SyrexMinus",
                    "reviewer": "dangerink",
                    "approved_pr_count": 1,
                },
            ],
        )
        author_share = {
            (row["dir"], row["user_login"]): row
            for row in self._serving_rows(
                filename="author_pr_and_diff_share_per_directory.sql",
                replacements={
                    **directory_filters,
                    ":pr_diff_range": "'0-99'",
                },
            )
        }
        self.assertEqual(
            {
                key: {
                    field: row[field]
                    for field in (
                        "user_pr_count",
                        "total_pr_count",
                        "user_diff",
                        "total_diff",
                    )
                }
                for key, row in author_share.items()
            },
            {
                (".", "SyrexMinus"): {
                    "user_pr_count": 1,
                    "total_pr_count": 2,
                    "user_diff": 5,
                    "total_diff": 7,
                },
                (".", "dependabot[bot]"): {
                    "user_pr_count": 1,
                    "total_pr_count": 2,
                    "user_diff": 2,
                    "total_diff": 7,
                },
                ("src", "SyrexMinus"): {
                    "user_pr_count": 1,
                    "total_pr_count": 1,
                    "user_diff": 24,
                    "total_diff": 24,
                },
            },
        )
        self.assertAlmostEqual(
            author_share[(".", "SyrexMinus")]["pr_count_share"],
            1 / 2,
            places=6,
        )
        self.assertAlmostEqual(
            author_share[(".", "SyrexMinus")]["diff_share"],
            5 / 7,
            places=6,
        )
        self.assertAlmostEqual(
            author_share[(".", "dependabot[bot]")]["diff_share"],
            2 / 7,
            places=6,
        )

    def _serving_rows(
        self,
        filename: str,
        replacements: dict[str, str],
    ) -> list[dict[str, Any]]:
        sql = Path(SERVING_QUERY_DIR, filename).read_text()
        sql = sql.replace("GOLD.", f"{self.database}.{self.gold_schema}.")
        for keyword, value in replacements.items():
            sql = sql.replace(keyword, value)
        return _query_rows(connection=self.snowflake, sql=sql)

    def _run_dbt(
        self,
        command: str,
        arguments: tuple[str, ...] = (),
    ) -> None:
        result = subprocess.run(
            [
                "dbt",
                command,
                "--project-dir",
                str(PROJECT_DIR),
                "--profiles-dir",
                str(PROFILES_DIR),
                "--target",
                "integration",
                "--no-use-colors",
                *arguments,
            ],
            cwd=PROJECT_DIR,
            env=self.dbt_environment,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            check=False,
        )
        self.assertEqual(
            result.returncode,
            0,
            result.stdout,
        )

    def _gold_relations(self) -> dict[str, list[dict[str, Any]]]:
        return {
            table: _rows(
                connection=self.snowflake,
                relation=f"{self.database}.{self.gold_schema}.{table}",
            )
            for table in (
                "FACT_TIME_TO_SHIP",
                "FACT_PR_DIRECTORY",
                "FACT_REVIEW_INTERACTIONS",
            )
        }


def _insert_initial_fixture(
    connection,
    database: str,
    silver_schema: str,
) -> None:
    prefix = f"{database}.{silver_schema}"
    _insert_rows(
        connection=connection,
        table=f"{prefix}.PULL",
        rows=[
            (
                REPOSITORY,
                1,
                "2026-09-20 11:01:00",
                "SyrexMinus",
                "closed",
                "2026-09-20 10:00:00",
                "2026-09-20 11:02:00",
                "Merged pull",
                INITIAL_RUN_ID,
            ),
            (
                REPOSITORY,
                2,
                None,
                "dangerink",
                "open",
                "2026-09-20 12:00:00",
                "2026-09-20 12:10:00",
                "Open pull",
                INITIAL_RUN_ID,
            ),
            (
                REPOSITORY,
                3,
                None,
                "SyrexMinus",
                "closed",
                "2026-09-20 13:00:00",
                "2026-09-20 13:10:00",
                "Closed unmerged pull",
                INITIAL_RUN_ID,
            ),
            (
                REPOSITORY,
                4,
                "2026-09-21 09:10:00",
                "dependabot[bot]",
                "closed",
                "2026-09-21 09:00:00",
                "2026-09-21 09:11:00",
                "Bot pull",
                INITIAL_RUN_ID,
            ),
        ],
    )
    _insert_rows(
        connection=connection,
        table=f"{prefix}.FILE",
        rows=[
            (REPOSITORY, 1, "README.md", 4, 1, INITIAL_RUN_ID),
            (REPOSITORY, 1, "src/main.py", 10, 2, INITIAL_RUN_ID),
            (REPOSITORY, 1, "src/core/a.py", 6, 1, INITIAL_RUN_ID),
            (REPOSITORY, 1, "src/core/b.py", 3, 2, INITIAL_RUN_ID),
            (REPOSITORY, 2, "src/open.py", 1, 0, INITIAL_RUN_ID),
            (REPOSITORY, 3, "src/unmerged.py", 1, 0, INITIAL_RUN_ID),
            (REPOSITORY, 4, "bot.py", 2, 0, INITIAL_RUN_ID),
        ],
    )
    _insert_rows(
        connection=connection,
        table=f"{prefix}.REVIEW",
        rows=[
            (REPOSITORY, 1, "dangerink", "APPROVED", INITIAL_RUN_ID),
            (REPOSITORY, 1, "octocat", "COMMENTED", INITIAL_RUN_ID),
            (REPOSITORY, 2, "SyrexMinus", "APPROVED", INITIAL_RUN_ID),
            (
                REPOSITORY,
                4,
                "robot-reviewer[bot]",
                "APPROVED",
                INITIAL_RUN_ID,
            ),
        ],
    )


def _apply_increment(
    connection,
    database: str,
    silver_schema: str,
) -> None:
    prefix = f"{database}.{silver_schema}"
    execute(
        connection=connection,
        sql=(
            f"UPDATE {prefix}.PULL SET MERGED_AT = '2026-09-20 12:30:00', "
            "STATE = 'closed', UPDATED_AT = '2026-09-20 12:31:00', "
            f"PARSE_RUN_ID = '{INCREMENTAL_RUN_ID}' "
            f"WHERE REPO = '{REPOSITORY}' AND PR_NUM = 2"
        ),
    )
    execute(
        connection=connection,
        sql=(
            f"UPDATE {prefix}.FILE SET PARSE_RUN_ID = '{INCREMENTAL_RUN_ID}' "
            f"WHERE REPO = '{REPOSITORY}' AND PR_NUM = 2"
        ),
    )
    execute(
        connection=connection,
        sql=(
            f"UPDATE {prefix}.REVIEW SET PARSE_RUN_ID = '{INCREMENTAL_RUN_ID}' "
            f"WHERE REPO = '{REPOSITORY}' AND PR_NUM = 2"
        ),
    )


def _insert_stale_full_refresh_rows(
    connection,
    database: str,
    gold_schema: str,
) -> None:
    prefix = f"{database}.{gold_schema}"
    execute(
        connection=connection,
        sql=(
            f"INSERT INTO {prefix}.FACT_TIME_TO_SHIP VALUES "
            "('stale/full-refresh', 999, 'stale-user', "
            "'2026-09-01 00:00:00', 1, '2026-09-01 00:00:00', "
            "'stale-full-refresh-run')"
        ),
    )
    execute(
        connection=connection,
        sql=(
            f"INSERT INTO {prefix}.FACT_REVIEW_INTERACTIONS VALUES "
            "('stale/full-refresh', 999, 'stale-author', 'stale-reviewer', "
            "'APPROVED', '2026-09-01 00:00:00', "
            "'2026-09-01 00:00:00', 'stale-full-refresh-run')"
        ),
    )


def _model_materializations() -> dict[str, str]:
    manifest = json.loads((PROJECT_DIR / "target/manifest.json").read_text())
    return {
        node["name"]: node["config"]["materialized"]
        for node in manifest["nodes"].values()
        if node["resource_type"] == "model"
    }


def _insert_rows(
    connection,
    table: str,
    rows: list[tuple[Any, ...]],
) -> None:
    placeholders = ", ".join("%s" for _ in rows[0])
    with connection.cursor() as cursor:
        cursor.executemany(
            f"INSERT INTO {table} VALUES ({placeholders})",
            rows,
        )


def _rows(connection, relation: str) -> list[dict[str, Any]]:
    return _sorted(
        _query_rows(connection=connection, sql=f"SELECT * FROM {relation}")
    )


def _query_rows(connection, sql: str) -> list[dict[str, Any]]:
    with connection.cursor() as cursor:
        cursor.execute(sql)
        columns = [column.name.lower() for column in cursor.description]
        rows = [
            {
                column: _normalize(value)
                for column, value in zip(columns, row, strict=True)
            }
            for row in cursor.fetchall()
        ]
    return rows


def _normalize(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Decimal):
        if value == value.to_integral_value():
            return int(value)
        return float(value)
    return value


def _sorted(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(rows, key=lambda row: json.dumps(row, sort_keys=True))


if __name__ == "__main__":
    import unittest

    unittest.main()
