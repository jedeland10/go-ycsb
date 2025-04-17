if [ "$#" -ne 4 ]; then
  echo "Usage: $0  <keysize> <endpoint> <record_count> <thread_count>"
  exit 1
fi

# Parse the command-line arguments
keysize="$1"
endpoint="$2"
record_count="$3"
thread_count="$4"


echo "TEST Performing load phase..." 

./bin/go-ycsb load raft -P workloads/workload_write \
    -p raft.address="$endpoint" \
    -p recordcount="$record_count" \
    -p threadcount="$thread_count" \
    -p keysize="$keysize" \
    -p insertproportion=1 -p updateproportion=0 -p fieldcount=1 # | grep -E '^(INSERT|TOTAL)'

