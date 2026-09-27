#!/usr/bin/env bash
set -euo pipefail

project_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
cd "$project_dir"

venv_dir="$project_dir/stages/poller/tests/.venv"
if [[ ! -x "$venv_dir/bin/python" ]]; then
    python3.11 -m venv "$venv_dir"
fi
"$venv_dir/bin/python" -m pip install \
    --quiet \
    --disable-pip-version-check \
    --requirement stages/poller/tests/requirements-test.txt

exec "$venv_dir/bin/python" -m unittest -v stages.poller.tests.test_poller
