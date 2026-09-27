from pathlib import Path
from unittest import TestCase

import yaml

from stages.spark.snowflake_test_helpers import SILVER_TABLE_SCHEMAS


class TableSchemasTest(TestCase):
    def test_table_schemas_match_dbt_sources(self) -> None:
        sources = yaml.safe_load(
            (Path(__file__).parents[1] / "models/sources.yml").read_text()
        )
        silver_source = next(
            source for source in sources["sources"] if source["name"] == "silver"
        )
        dbt_table_schemas = {
            table["name"]: [
                (column["name"], column["data_type"].lower())
                for column in table["columns"]
            ]
            for table in silver_source["tables"]
        }

        self.assertEqual(dbt_table_schemas, SILVER_TABLE_SCHEMAS)
