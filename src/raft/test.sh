#!/bin/bash

MAX_RUNS=100
export VERBOSE=0
TEST_CMD="go test -race"

for (( i=1; i<=$MAX_RUNS; i++ ))
do
    echo "Running test iteration $i"
    time $TEST_CMD > result.txt
    if [ $? -ne 0 ]; then
        echo "Test failed on iteration $i. Exiting."
        exit 1
    else
        echo "Test End $?"
    fi
done

echo "All tests passed for $MAX_RUNS iterations."
