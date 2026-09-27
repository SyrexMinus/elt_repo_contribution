#!/usr/bin/env bash
set -euo pipefail

project_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$project_dir"

stages/poller/run_tests.sh
stages/spark/tests/run_tests.sh all
stages/dbt/tests/run_tests.sh
