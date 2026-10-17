#!/usr/bin/env bash
set -euo pipefail

venv_dir="stages/poller/tests/.venv"
if [[ ! -x "$venv_dir/bin/python3.12" ]]; then
    python3.12 -m venv --clear "$venv_dir"
fi
"$venv_dir/bin/python" -m pip install \
    --quiet \
    --disable-pip-version-check \
    --requirement stages/poller/tests/requirements-test.txt

exec "$venv_dir/bin/python" -m unittest -v stages.poller.tests.test_poller
