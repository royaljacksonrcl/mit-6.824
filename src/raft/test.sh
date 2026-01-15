#!/bin/bash

# ========================
# 默认测试次数
# ========================
declare -A ITER=(
  ["2A"]=100
  ["2B"]=100
  ["2C"]=100
  ["2D"]=100
)

# ========================
# 解析命令行参数：2A=5
# ========================
for arg in "$@"; do
  if [[ $arg =~ ^(2A|2B|2C|2D)=([0-9]+)$ ]]; then
    ITER[${BASH_REMATCH[1]}]=${BASH_REMATCH[2]}
  else
    echo "Invalid arg: $arg (use 2A=5 style)"
    exit 1
  fi
done

export VERBOSE=3
TEST_CMD="go test -race"

for part in 2A 2B 2C 2D; do
    MAX_RUNS=${ITER[$part]}
    [[ $MAX_RUNS -lt 1 ]] && continue

    echo "==========$part running $MAX_RUNS times =========="

    for (( i=1; i<=$MAX_RUNS; i++ ))
    do
        echo "Running test $part iteration $i"
        start=$(date +%s)
        $TEST_CMD  -run "$part"> result_$part.txt
        end=$(date +%s)
        if [ $? -ne 0 ]; then
            echo "Test failed on iteration $i. Exiting."
            exit 1
        else
            echo "Test End. Elapsed $((end - start))s"
        fi
    done
done

echo "All tests passed for $MAX_RUNS iterations."
