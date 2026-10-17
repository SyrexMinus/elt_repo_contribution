#!/usr/bin/env bash
set -euo pipefail

venv_dir="stages/airflow_dags/tests/.venv"
if [[ ! -x "$venv_dir/bin/python3.12" ]]; then
    python3.12 -m venv --clear "$venv_dir"
fi
"$venv_dir/bin/python" -m pip install \
    --quiet \
    --disable-pip-version-check \
    --requirement stages/airflow_dags/tests/requirements-test.txt

export AIRFLOW__CORE__UNIT_TEST_MODE=True
export AIRFLOW_HOME="${TMPDIR:-/tmp}/elt-repo-contribution-airflow-tests"

exec "$venv_dir/bin/python" -m unittest -v \
    stages.airflow_dags.tests.test_elt_dag
