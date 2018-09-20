#!/usr/bin/env bash

if [ $# != 1 ]; then
	echo "missing repeat parameter"
	exit 1
fi

repeat=$1
round="1"

while [ $round -le $repeat ]; do

    echo "round: $round"

    echo "running lock test"

    lein run test --workload raft-lock --time-limit 120

    if [ $? != '0' ]; then
        echo "lock test failed"
        exit 1
    fi

    echo "running reentrant lock test"

    lein run test --workload raft-reentrant-lock --time-limit 120

    if [ $? != '0' ]; then
        echo "reentrant lock test failed"
        exit 1
    fi

    echo "running fenced lock test"

    lein run test --workload raft-fenced-lock --time-limit 120

    if [ $? != '0' ]; then
        echo "fenced lock test failed"
        exit 1
    fi

    echo "running reentrant fenced lock test"

    lein run test --workload raft-reentrant-fenced-lock --time-limit 120

    if [ $? != '0' ]; then
        echo "reentrant fenced lock test failed"
        exit 1
    fi

    echo "running session aware semaphore test"

    lein run test --workload raft-session-aware-semaphore --time-limit 120

    if [ $? != '0' ]; then
        echo "session aware semaphore test failed"
        exit 1
    fi

    round=`expr $round \+ 1`

done
