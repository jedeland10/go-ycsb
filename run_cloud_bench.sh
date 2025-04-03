# Ensure we have exactly four parameters
if [ "$#" -ne 7 ]; then
  echo "Usage: $0 <output_file_path> <run_index> <keysize> <record_count> <operation_count> <thread_count> <endpoint>"
  exit 1
fi

# Parse the command-line arguments
output_file_path="$1"   # e.g. "../ycsb_bench/cache/once"
run_index="$2"          # e.g. "1"
keysize="$3"      # e.g. "4096"
record_count="$4"
operation_count="$5"
thread_count="$6"
endpoint="$7"

# Build the output file name: output_file_path/{keyprefixsize}_{run_index}.txt
output_file="${output_file_path}/${keysize}_${run_index}.txt"

# Clear the output file
> "$output_file"


./bin/go-ycsb run etcd -P workloads/workload_write \
    -p etcd.endpoints="$endpoint" \
    -p keypsize="$keysize" \
    -p threadcount="$thread_count" \
    -p recordcount="$record_count" -p operationcount="$operation_count" 2>&1 \
    | grep -E '^(INSERT|TOTAL)' | tee -a "$output_file"

echo "---------------------------------------" | tee -a "$output_file"
