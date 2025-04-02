if [ "$#" -ne 5 ]; then
  echo "Usage: $0  <keysize> <endpoint> <record_count> <operation_count> <thread_count>"
  exit 1
fi

# Parse the command-line arguments
keysize="$1"      # e.g. "4096"
endpoint="$2"         # e.g. "http://127.0.0.1:12380/"
record_count="$3"
operation_count="$4"
thread_count="$5"


echo "TEST Performing load phase..." 

./bin/ycsb load etcd -P workloads/workload_write \
    -p etcd.endpoints="$endpoint" \
    -p recordcount="$record_count" -p operationcount="$operation_count" \
    -p threadcount="$thread_count" \
    -p keysize="$keysize" \
    -p insertproportion=1 -p updateproportion=0 2>&1 \
    | grep -E '^\[OVERALL\]|^\[INSERT\]' 

