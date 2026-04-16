#!/usr/bin/env bash
set -uo pipefail

if [ "$#" -lt 6 ] || [ "$#" -gt 8 ]; then
  echo "Usage: $0 <output_dir> <run_index> <endpoint> <thread_count> <max_exec> <zipfian_constant> [read_proportion] [trace_file]"
  echo ""
  echo "Arguments:"
  echo "  output_dir         - Directory to store output files"
  echo "  run_index          - Index for this run (used in output filename)"
  echo "  endpoint           - Raft server endpoint (e.g. 10.0.0.1:12380)"
  echo "  thread_count       - Number of concurrent threads"
  echo "  max_exec           - Max execution time in seconds"
  echo "  zipfian_constant   - Zipfian skew (0.0=uniform, 0.99=default, 1.2+=very skewed)"
  echo "  read_proportion    - Fraction of reads (default: 0.0 = all writes)"
  echo "  trace_file         - Trace file for key-size distribution (default: cluster37.sort.200k.zst)"
  exit 1
fi

# Parse args
output_dir="$1"; run_index="$2"; endpoint="$3"
thread_count="$4"; max_exec="$5"; zipfian_constant="$6"
read_proportion="${7:-0.0}"
trace_file="${8:-cluster37.sort.200k.zst}"

# Hard timeout: 2x max_exec + 60s buffer for startup/teardown
hard_timeout=$(( max_exec * 2 + 60 ))

mkdir -p "$output_dir"
output_file="$output_dir/tracedist_z${zipfian_constant}_r${read_proportion}_${run_index}.txt"
> "$output_file"

# Annotate run parameters
echo "TRACE_FILE=${trace_file}" >> "$output_file"
echo "ZIPFIAN_CONSTANT=${zipfian_constant}" >> "$output_file"
echo "READ_PROPORTION=${read_proportion}" >> "$output_file"
echo "THREAD_COUNT=${thread_count}" >> "$output_file"
echo "MAX_EXEC=${max_exec}" >> "$output_file"

timeout "${hard_timeout}" ./bin/go-ycsb run raft \
  -P workloads/workload_tracedist \
  -p raft.address="$endpoint" \
  -p tracedist.file="$trace_file" \
  -p tracedist.zipfianconstant="$zipfian_constant" \
  -p tracedist.readproportion="$read_proportion" \
  -p tracedist.maxkeysize=8192 \
  -p maxexecutiontime="$max_exec" \
  -p threadcount="$thread_count" \
  -p operationcount=999999999 \
  -p warmuptime=10 \
  2>&1 \
  | grep -E '^(INSERT|UPDATE|READ|DELETE|TOTAL|\[TRACEDIST DEBUG\])' \
  | tee -a "$output_file" \
  || echo "BENCH_EXIT_CODE=$?" >> "$output_file"

echo "Sleep 5 seconds before fetching cache hits"
sleep 5

# fetch cache hits from raft server
echo "Fetching cache hits..." >> "$output_file"
timeout 30 go run get_cache_hits.go --addr "$endpoint" 2>&1 | tee -a "$output_file" || true

# fetch restored count from raft server
echo "Fetching restored..." >> "$output_file"
timeout 30 go run get_restored.go --addr "$endpoint" 2>&1 | tee -a "$output_file" || true
echo "---------------------------------------" >> "$output_file"
