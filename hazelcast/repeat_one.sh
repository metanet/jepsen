#!/usr/bin/env bash

if [ $# != 3 ]; then
	echo "usage: ./repeat_one.sh workload repeat test_duration"
	exit 1
fi

workload=$1
repeat=$2
test_duration=$3
round="1"

while [ ${round} -le ${repeat} ]; do

    echo "round: $round"

    echo "running $workload test"

    lein run test --workload ${workload} --time-limit ${test_duration}

    if [ $? != '0' ]; then
        echo "$workload test failed"
        exit 1
    fi

    round=`expr $round \+ 1`

done
