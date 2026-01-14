#!/bin/bash

MAX_RUNS=100
export VERBOSE=0
TEST_CMD="VERBOSE=3 go test -run 2D -race"

for (( i=1; i<=$MAX_RUNS; i++ ))
do
    echo "Running test iteration $i"
    time $TEST_CMD > result_2D.txt
    if [ $? -ne 0 ]; then
        echo "Test failed on iteration $i. Exiting."
        exit 1
    else
        echo "Test End $?"
    fi
done

echo "All tests passed for $MAX_RUNS iterations."
