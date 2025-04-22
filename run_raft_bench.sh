if [ "$#" -ne 8 ]; then
  echo "Usage: $0 <output_file_path> <run_index> <keysize> <endpoint> <record_count> <operation_count> <thread_count> <max-exec>"
  exit 1
fi

# Parse the command-line arguments
output_file_path="$1"   # e.g. "../ycsb_bench/cache/once"
run_index="$2"          # e.g. "1"
keysize="$3"      # e.g. "4096"
endpoint="$4"
record_count="$5"
operation_count="$6"
thread_count="$7"
max_exec="$8"

# Build the output file name: output_file_path/{keyprefixsize}_{run_index}.txt
output_file="${output_file_path}/${keysize}_${run_index}.txt"

# Clear the output file
> "$output_file"


./bin/go-ycsb run raft -P workloads/workload_write \
    -p raft.address="$endpoint" \
    -p maxexecutiontime="$max_exec" \
    -p keysize="$keysize" \
    -p threadcount="$thread_count" \
    -p recordcount="$record_count" -p operationcount="$operation_count" 2>&1 \
    | grep -E '^(UPDATE|TOTAL)' | tee -a "$output_file"

echo "---------------------------------------" | tee -a "$output_file"
