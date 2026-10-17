#!/usr/bin/env bash
set -euo pipefail

venv_dir="$PWD/stages/dbt/tests/.venv"
if [[ ! -x "$venv_dir/bin/python3.12" ]]; then
    python3.12 -m venv --clear "$venv_dir"
fi
"$venv_dir/bin/python" -m pip install \
    --quiet \
    --disable-pip-version-check \
    --requirement stages/dbt/tests/requirements.txt

export PATH="$venv_dir/bin:$PATH"
export SNOWFLAKE_ACCOUNT="${SNOWFLAKE_ACCOUNT:-QSLOGNN-IL52426}"
export SNOWFLAKE_USER="${SNOWFLAKE_USER:-SYREXMINUS}"
export SNOWFLAKE_ROLE="${SNOWFLAKE_ROLE:-SYSADMIN}"
export SNOWFLAKE_DATABASE="${SNOWFLAKE_DATABASE:-ELT_REPO_CONTRIBUTION_TEST}"
export SNOWFLAKE_WAREHOUSE="${SNOWFLAKE_WAREHOUSE:-ELT_TEST_WH}"
export SNOWFLAKE_PASSWORD="$(<"${SNOWFLAKE_TOKEN_FILE:-${HOME}/.snowflake/token}")"

exec python -m unittest -v \
    stages.dbt.tests.test_table_schemas \
    stages.dbt.tests.test_gold_integration
