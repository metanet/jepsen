#!/usr/bin/env bash

if [ $# != 2 ]; then
	echo "how to use: ./test.sh test_count time_limit"
	exit 1
fi

test_count=$1
time_limit=$2

lein run test --workload cas-register --test-count "${test_count}" --time-limit "${time_limit}"