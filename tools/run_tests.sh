#!/usr/bin/env bash
set -euo pipefail

stages/poller/tests/run_tests.sh \
    > poller_test_output.log \
    2> poller_test_errors.log &
poller_pid=$!

stages/spark/tests/run_tests.sh \
    > spark_test_output.log \
    2> spark_test_errors.log &
spark_pid=$!

stages/dbt/tests/run_tests.sh \
    > dbt_test_output.log \
    2> dbt_test_errors.log &
dbt_pid=$!

status=0

if ! wait "$poller_pid"; then
    echo "Poller tests failed; see poller_test_errors.log" >&2
    status=1
fi

if ! wait "$spark_pid"; then
    echo "Spark tests failed; see spark_test_errors.log" >&2
    status=1
fi

if ! wait "$dbt_pid"; then
    echo "DBT tests failed; see dbt_test_errors.log" >&2
    status=1
fi

exit "$status"
