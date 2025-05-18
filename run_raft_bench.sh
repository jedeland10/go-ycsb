#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -ne 8 ]; then
  echo "Usage: $0 <output_dir> <num_runs> <keysize> <endpoint> <record_count> <operation_count> <thread_count> <max_exec>"
  exit 1
fi

# Parse the command-line arguments
output_dir="$1"
num_runs="$2"
keysize="$3"
endpoint="$4"
record_count="$5"
operation_count="$6"
thread_count="$7"
max_exec="$8"

# Ensure the output directory exists
mkdir -p "$output_dir"

# Loop over run indices
for run_index in $(seq 1 "$num_runs"); do
  output_file="${output_dir}/${keysize}_${run_index}.txt"
  : > "$output_file"   # truncate or create

  echo ">>> Run #${run_index}/${num_runs}, writing to ${output_file}"
  ./bin/go-ycsb run raft \
      -P workloads/workload_write \
      -p raft.address="$endpoint" \
      -p maxexecutiontime="$max_exec" \
      -p keysize="$keysize" \
      -p threadcount="$thread_count" \
      -p recordcount="$record_count" \
      -p operationcount="$operation_count" \
    2>&1 \
    | grep -E '^(UPDATE|TOTAL)' \
    | tee -a "$output_file"

  echo "---------------------------------------" | tee -a "$output_file"
done

echo "All ${num_runs} runs complete."
