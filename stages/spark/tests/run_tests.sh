#!/usr/bin/env bash
set -euo pipefail

if docker compose version >/dev/null 2>&1; then
    compose=(docker compose -f stages/spark/tests/docker-compose.test.yml)
else
    compose=(docker-compose -f stages/spark/tests/docker-compose.test.yml)
fi

cleanup() {
    "${compose[@]}" down --volumes --remove-orphans >/dev/null 2>&1 || true
}
trap cleanup EXIT

mode="${1:-}"
if [[ -n "$mode" && "$mode" != "unit" ]]; then
    echo "usage: $0 [unit]" >&2
    exit 2
fi

"${compose[@]}" build spark-tests

tests=(stages.spark.tests.test_silver_job)
if [[ "$mode" != "unit" ]]; then
    "${compose[@]}" up --detach --wait localstack
    tests+=(stages.spark.tests.test_silver_job_integration)
fi

"${compose[@]}" run --rm --no-deps spark-tests \
    python3.12 -m unittest -v "${tests[@]}"
