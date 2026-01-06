#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -lt 7 ] || [ "$#" -gt 9 ]; then
  echo "Usage: $0 <output_dir> <run_index> <trace_file> <endpoint> <thread_count> <max_exec> <leader_zone> [read_mode] [max_records]"
  echo ""
  echo "Arguments:"
  echo "  output_dir       - Directory to store output files"
  echo "  run_index        - Index for this run (used in output filename)"
  echo "  trace_file       - Path to the twemcache trace file (.zst or .csv)"
  echo "  endpoint         - Raft server endpoint"
  echo "  thread_count     - Number of concurrent threads"
  echo "  max_exec         - Max execution time in seconds"
  echo "  leader_zone      - Leader zone annotation"
  echo "  read_mode        - How to handle reads: 'skip', 'write', or 'read' (default: write)"
  echo "  max_records      - Max records to load from trace, 0=unlimited (default: 0)"
  exit 1
fi

# Parse args
output_dir="$1"; run_index="$2"; trace_file="$3"; endpoint="$4"
thread_count="$5"; max_exec="$6"; leader_zone="$7"
read_mode="${8:-write}"      # Default: convert reads to writes
max_records="${9:-0}"        # Default: load entire trace

# Extract trace filename for output naming
trace_basename=$(basename "$trace_file" .zst)
trace_basename=$(basename "$trace_basename" .csv)

mkdir -p "$output_dir"
output_file="$output_dir/${trace_basename}_${run_index}.txt"
> "$output_file"

# annotate the leader zone and trace file
echo "LEADER_ZONE=${leader_zone}" >> "$output_file"
echo "TRACE_FILE=${trace_file}" >> "$output_file"
echo "READ_MODE=${read_mode}" >> "$output_file"

# now do the run
# operationcount set very high - actual ops determined by trace length or max_exec
./bin/go-ycsb run raft \
  -P workloads/workload_trace \
  -p raft.address="$endpoint" \
  -p trace.file="$trace_file" \
  -p trace.maxrecords="$max_records" \
  -p trace.readmode="$read_mode" \
  -p maxexecutiontime="$max_exec" \
  -p threadcount="$thread_count" \
  -p operationcount=999999999 \
  -p warmuptime=10 \
  2>&1 \
  | grep -E '^(INSERT|UPDATE|READ|DELETE|TOTAL)' \
  | tee -a "$output_file"

echo "Sleep 5 seconds before fetching cache hits"
sleep 5

# fetch cache hits from raft server
echo "Fetching cache hits..." >> "$output_file"
go run get_cache_hits.go --addr "$endpoint" 2>&1 | tee -a "$output_file"
echo "---------------------------------------" >> "$output_file"

