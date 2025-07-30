#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -ne 9 ]; then
  echo "Usage: $0 <output_dir> <run_index> <keysize> <endpoint> <record_count> <operation_count> <thread_count> <max_exec> <leader_zone>"
  exit 1
fi

# Parse args
output_dir="$1"; run_index="$2"; keysize="$3"; endpoint="$4"
record_count="$5"; operation_count="$6"; thread_count="$7"; max_exec="$8"
leader_zone="$9"

mkdir -p "$output_dir"
output_file="$output_dir/${keysize}_${run_index}.txt"
> "$output_file"

# annotate the leader zone
echo "LEADER_ZONE=${leader_zone}" >> "$output_file"

# now do the run
./bin/go-ycsb run raft \
  -P workloads/workload_write \
  -p raft.address="$endpoint" \
  -p maxexecutiontime="$max_exec" \
  -p keysize="$keysize" \
  -p threadcount="$thread_count" \
  -p recordcount="$record_count" \
  -p operationcount="$operation_count" \
  -p warmuptime=10 \
  2>&1 \
  | grep -E '^(UPDATE|TOTAL)' \
  | tee -a "$output_file"

echo "Sleep 5 seconds before fetcing cache hits"
sleep 5

# fetch cache hits from raft server
echo "Fetching cache hits..." >> "$output_file"
go run get_cache_hits.go --addr "$endpoint" 2>&1 | tee -a "$output_file"
echo "---------------------------------------" >> "$output_file"
