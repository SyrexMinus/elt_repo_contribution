#!/usr/bin/env bash
set -euo pipefail

venv_dir="tools/.venv"
if [[ ! -x "$venv_dir/bin/python" ]]; then
    python3 -m venv "$venv_dir"
fi
"$venv_dir/bin/python" -m pip install \
    --quiet \
    --disable-pip-version-check \
    ruff

exec "$venv_dir/bin/ruff" check .
