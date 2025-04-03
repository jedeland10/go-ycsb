if [ "$#" -ne 4 ]; then
  echo "Usage: $0  <keysize> <endpoint> <record_count> <thread_count>"
  exit 1
fi

# Parse the command-line arguments
keysize="$1"      # e.g. "4096"
endpoint="$2"         # e.g. "http://127.0.0.1:12380/"
record_count="$3"
thread_count="$4"


echo "TEST Performing load phase..." 

./bin/go-ycsb load etcd -P workloads/workload_write \
    -p etcd.endpoints="$endpoint" \
    -p recordcount="$record_count" \
    -p threadcount="$thread_count" \
    -p keysize="$keysize" \
    -p insertproportion=1 -p updateproportion=0 | grep -E '^(INSERT|TOTAL)'

