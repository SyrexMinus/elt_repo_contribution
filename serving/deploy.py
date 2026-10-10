import os
from pathlib import Path

import snowflake.connector


def main():
    source = Path(__file__).parent
    with (
        snowflake.connector.connect(
            account=os.environ["SNOWFLAKE_ACCOUNT"],
            user=os.environ["SNOWFLAKE_USER"],
            role=os.environ.get("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
            database=os.environ["SNOWFLAKE_DATABASE"],
            warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
            authenticator="SNOWFLAKE_JWT",
            private_key_file=os.environ["SNOWFLAKE_PRIVATE_KEY_FILE"],
        ) as connection,
        connection.cursor() as cursor,
    ):
        cursor.execute("CREATE STAGE IF NOT EXISTS PUBLIC.SERVING_CODE")
        for file in (source / "streamlit_app.py", source / "environment.yml"):
            cursor.execute(
                "PUT %s @PUBLIC.SERVING_CODE/app AUTO_COMPRESS=FALSE OVERWRITE=TRUE",
                (file.resolve().as_uri(),),
            )
        for file in sorted((source / "queries").glob("*.sql")):
            cursor.execute(
                "PUT %s @PUBLIC.SERVING_CODE/app/queries AUTO_COMPRESS=FALSE OVERWRITE=TRUE",
                (file.resolve().as_uri(),),
            )
        cursor.execute(
            "CREATE OR REPLACE STREAMLIT PUBLIC.REPO_CONTRIBUTION "
            "FROM '@PUBLIC.SERVING_CODE/app' MAIN_FILE='streamlit_app.py' "
            "RUNTIME_NAME='SYSTEM$WAREHOUSE_RUNTIME' "
            "QUERY_WAREHOUSE=%s",
            (os.environ["SNOWFLAKE_WAREHOUSE"],),
        )
        cursor.execute(
            "ALTER STREAMLIT PUBLIC.REPO_CONTRIBUTION ADD LIVE VERSION FROM LAST"
        )


if __name__ == "__main__":
    main()
