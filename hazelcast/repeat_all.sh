#!/usr/bin/env bash

if [ $# != 2 ]; then
	echo "usage: ./repeat_all.sh repeat test_duration"
	exit 1
fi

repeat=$1
test_duration=$2
round="1"

while [[ ${round} -le ${repeat} ]]; do

    echo "round: $round"

    echo "running cp-lock test"

    lein run test --workload cp-non-reentrant-lock --time-limit ${test_duration}

    if [[ $? != '0' ]]; then
        echo "cp-lock test failed"
        exit 1
    fi

    echo "running cp-reentrant-lock test"

    lein run test --workload cp-reentrant-lock --time-limit ${test_duration}

    if [[ $? != '0' ]]; then
        echo "cp-reentrant-lock test failed"
        exit 1
    fi

    echo "running fenced-lock test"

    lein run test --workload fenced-lock --time-limit ${test_duration}

    if [[ $? != '0' ]]; then
        echo "fenced-lock test failed"
        exit 1
    fi

    echo "running reentrant-fenced-lock test"

    lein run test --workload reentrant-fenced-lock --time-limit ${test_duration}

    if [[ $? != '0' ]]; then
        echo "reentrant-fenced-lock test failed"
        exit 1
    fi

    echo "running cp-semaphore test"

    lein run test --workload cp-semaphore --time-limit ${test_duration}

    if [[ $? != '0' ]]; then
        echo "cp-semaphore test failed"
        exit 1
    fi

    echo "running cp-cas-register test"

    lein run test --workload cp-cas-register --time-limit ${test_duration}

    if [[ $? != '0' ]]; then
        echo "cp-cas-register test failed"
        exit 1
    fi

    echo "running cp-atomic-long-ids test"

    lein run test --workload cp-atomic-long-ids --time-limit ${test_duration}

    if [[ $? != '0' ]]; then
        echo "cp-atomic-long-ids test failed"
        exit 1
    fi

    round=`expr $round \+ 1`

done
