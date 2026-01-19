#!/usr/bin/env bash

set -o pipefail

TEST="$1"
MAX=1000

# 统计初始化
declare -a times
total_time=0
failed_runs=0

echo "Starting looped test for '$TEST' with $MAX runs..."
echo "====================Start====================="

for i in $(seq 1 $MAX); do
  LOG="test_run_${i}.log"
  echo "===== Run $i / $MAX ====="
  start_time=$(date +%s.%N)

  if go test -run "${TEST}" -race -v > "$LOG" 2>&1; then
    end_time=$(date +%s.%N)
    duration=$(echo "$end_time - $start_time" | bc -l 2>/dev/null || echo "0")

    # 记录时间
    times[$i]=$(printf "%.3f" "$duration")
    total_time=$(echo "$total_time + $duration" | bc -l)

    echo "✅ Passed at run $i in ${times[$i]} seconds"
    rm -f "$LOG"
  else
    echo "❌ Failed at run $i"
    echo "📄 Log saved to $LOG"
    exit 1
  fi
done

## 统计测试结果
echo "====================Summary====================="
average_time=$(echo "scale=3; $total_time / $MAX" | bc -l)

max_time=0
min_time=99999.999
for t in "${times[@]}"; do
  # 更新最大时间
  if (( $(echo "$t > $max_time" | bc -l) )); then
    max_time=$t
  fi
  # 更新最小时间
  if (( $(echo "$t < $min_time" | bc -l) )); then
    min_time=$t
  fi
done

echo "Total runs: $MAX"
echo "Total time: $(printf "%.3f" "$total_time") seconds"
echo "Average time per run: $(printf "%.3f" "$average_time") seconds"
echo "Fastest run: $(printf "%.3f" "$min_time") seconds"
echo "Slowest run: $(printf "%.3f" "$max_time") seconds"

# 趋势图
echo "trend of run times (seconds):"
echo "-------------------------------------"
max_bar_length=30
scale=$(echo "$max_bar_length * $max_time / $max_time" | bc -l)

for i in $(seq 1 $MAX); do
  t=${times[$i]}
  bar_length=$(echo "$t * $max_bar_length / $max_time" | bc -l)
  bar_length=${bar_length%.*} # 取整数部分
  bar=$(printf '%*s' "$bar_length" '' | tr ' ' '#')
  printf "Run %4d: %6.3f sec |%s\n" "$i" "$t" "$bar"
done
echo "-------------------------------------"

echo "✅ All $MAX runs passed"
