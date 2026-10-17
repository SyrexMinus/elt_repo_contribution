#!/usr/bin/env bash
set -euo pipefail

venv_dir="tools/.venv"
if [[ ! -x "$venv_dir/bin/python3.12" ]]; then
    python3.12 -m venv --clear "$venv_dir"
fi
"$venv_dir/bin/python" -m pip install \
    --quiet \
    --disable-pip-version-check \
    ruff

exec "$venv_dir/bin/ruff" check .
