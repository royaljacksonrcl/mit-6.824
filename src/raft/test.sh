#!/bin/bash

MAX_RUNS=100
export VERBOSE=3
TEST_CMD="go test -run 2C -race"

for (( i=1; i<=$MAX_RUNS; i++ ))
do
    echo "Running test iteration $i"
    $TEST_CMD > result$i.txt
    if [ $? -ne 0 ]; then
        echo "Test failed on iteration $i. Exiting."
        exit 1
    else
        echo "Test End $?"
    fi
done

echo "All tests passed for $MAX_RUNS iterations."
