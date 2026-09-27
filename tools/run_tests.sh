#!/usr/bin/env bash
set -euo pipefail

stages/poller/tests/run_tests.sh
stages/spark/tests/run_tests.sh
stages/dbt/tests/run_tests.sh
