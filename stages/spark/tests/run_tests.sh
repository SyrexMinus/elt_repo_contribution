#!/usr/bin/env bash
set -euo pipefail

project_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
cd "$project_dir"

if docker compose version >/dev/null 2>&1; then
    compose=(docker compose -f stages/spark/tests/docker-compose.test.yml)
else
    compose=(docker-compose -f stages/spark/tests/docker-compose.test.yml)
fi

cleanup() {
    "${compose[@]}" down --volumes --remove-orphans >/dev/null 2>&1 || true
}
trap cleanup EXIT

mode="${1:-unit}"
if [[ "$mode" != "unit" && "$mode" != "all" ]]; then
    echo "usage: $0 [unit|all]" >&2
    exit 2
fi

"${compose[@]}" build spark-tests

tests=(stages.spark.tests.test_silver_job)
if [[ "$mode" == "all" ]]; then
    "${compose[@]}" up --detach --wait localstack
    tests+=(stages.spark.tests.test_silver_job_integration)
fi

"${compose[@]}" run --rm --no-deps spark-tests \
    python3 -m unittest -v "${tests[@]}"
