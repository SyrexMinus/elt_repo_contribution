#!/usr/bin/env bash
set -euo pipefail

project_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
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

"${compose[@]}" build spark-tests
"${compose[@]}" run --rm --no-deps \
    --entrypoint python3 \
    --volume "${GITHUB_TOKEN_FILE:-${HOME}/.github/token}:/root/.github/token:ro" \
    spark-tests \
    -m unittest -v stages.poller.test_poller
