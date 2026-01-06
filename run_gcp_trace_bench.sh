#!/usr/bin/env bash

set -euo pipefail

REMOTE_USER="johanedeland"

# GCP project/zone settings
PROJECT_ID="jedeland"
ZONE1="us-central1-b"
ZONE2="europe-west1-b"
ZONE3="asia-northeast1-b"
ZONE4="australia-southeast1-b"
ZONE5="southamerica-east1-a"
MACHINE_TYPE="e2-standard-4"

# Images
CUSTOM_IMAGE_NAME="etcd-image-mac"
YCSB_IMAGE="go-ycsb-image"

# VM names
RAFT_NODE_PREFIX="raft-node"
YCSB_CLIENT="ycsb-client-1"

# Benchmark settings
NUM_NODES=5
THREAD_COUNT=4
NUM_RUNS=5
MAX_EXEC=60
ETCD_BRANCHES=("leaderEncode")
YCSB_BRANCH="master"
CLIENT_TYPE="e2-standard-4"

# Trace settings
TRACE_FILE="cluster26.sort.zst"
GCS_BUCKET="gs://jedeland-thesis-traces"
READ_MODE="skip"  # skip, write, or read

echo ">>> [1/5] Creating ${NUM_NODES} raft-node VMs from image '${CUSTOM_IMAGE_NAME}'..."

COMMON_RAFT_ARGS="--project=${PROJECT_ID} \
  --machine-type=${MACHINE_TYPE} \
  --image=${CUSTOM_IMAGE_NAME} \
  --tags=raft-node \
  --provisioning-model=SPOT \
  --maintenance-policy=TERMINATE \
  --quiet"

COMMON_YCSB_ARGS="--project=${PROJECT_ID} \
  --machine-type=${CLIENT_TYPE} \
  --image=${YCSB_IMAGE} \
  --provisioning-model=SPOT \
  --maintenance-policy=TERMINATE \
  --quiet"

echo ">>> Launching raft-node creates in parallel…"
gcloud compute instances create "${RAFT_NODE_PREFIX}-1" --zone="${ZONE1}" ${COMMON_RAFT_ARGS} &
gcloud compute instances create "${RAFT_NODE_PREFIX}-2" --zone="${ZONE2}" ${COMMON_RAFT_ARGS} &
gcloud compute instances create "${RAFT_NODE_PREFIX}-3" --zone="${ZONE3}" ${COMMON_RAFT_ARGS} &
gcloud compute instances create "${RAFT_NODE_PREFIX}-4" --zone="${ZONE4}" ${COMMON_RAFT_ARGS} &
gcloud compute instances create "${RAFT_NODE_PREFIX}-5" --zone="${ZONE5}" ${COMMON_RAFT_ARGS} &

# Create single YCSB client in EU (ZONE2) - will only run when leader is in EU
echo ">>> Creating YCSB client in ${ZONE2} (EU)"
gcloud compute instances create "${YCSB_CLIENT}" --zone="${ZONE2}" ${COMMON_YCSB_ARGS} --tags="${YCSB_CLIENT}" &

wait

echo ">>> [2/5] Waiting for SSH and bootstrapping YCSB client..."

# Wait for SSH
until gcloud compute ssh "${YCSB_CLIENT}" --zone="${ZONE2}" --ssh-flag="-o ControlMaster=no" --quiet --command="echo ready" &>/dev/null; do
  sleep 5
done

# Bootstrap YCSB client with trace file download
echo ">>> Bootstrapping ${YCSB_CLIENT} and downloading trace..."
gcloud compute ssh "${REMOTE_USER}@${YCSB_CLIENT}" \
  --zone="${ZONE2}" \
  --ssh-flag="-o ControlMaster=no" \
  --ssh-flag="-o ServerAliveInterval=30" \
  --ssh-flag="-o ServerAliveCountMax=5" \
  --quiet \
  --command="bash -l -c '
    cd ~/go-ycsb;
    git fetch;
    git checkout ${YCSB_BRANCH};
    git pull;
    make;
    
    # Download trace file if not present
    if [ ! -f ./${TRACE_FILE} ]; then
      echo \"Downloading trace file from GCS...\"
      gsutil cp ${GCS_BUCKET}/${TRACE_FILE} ./
    fi
    echo \"Trace file ready: \$(ls -lh ${TRACE_FILE})\"
  '"

# Get internal IPs for each node
IP_NODE_1=$(gcloud compute instances describe "${RAFT_NODE_PREFIX}-1" --project="${PROJECT_ID}" --zone="${ZONE1}" --format='get(networkInterfaces[0].networkIP)')
IP_NODE_2=$(gcloud compute instances describe "${RAFT_NODE_PREFIX}-2" --project="${PROJECT_ID}" --zone="${ZONE2}" --format='get(networkInterfaces[0].networkIP)')
IP_NODE_3=$(gcloud compute instances describe "${RAFT_NODE_PREFIX}-3" --project="${PROJECT_ID}" --zone="${ZONE3}" --format='get(networkInterfaces[0].networkIP)')
IP_NODE_4=$(gcloud compute instances describe "${RAFT_NODE_PREFIX}-4" --project="${PROJECT_ID}" --zone="${ZONE4}" --format='get(networkInterfaces[0].networkIP)')
IP_NODE_5=$(gcloud compute instances describe "${RAFT_NODE_PREFIX}-5" --project="${PROJECT_ID}" --zone="${ZONE5}" --format='get(networkInterfaces[0].networkIP)')

echo ">>> Node IPs: ${IP_NODE_1}, ${IP_NODE_2}, ${IP_NODE_3}, ${IP_NODE_4}, ${IP_NODE_5}"

for branch in "${ETCD_BRANCHES[@]}"; do
  echo ">>> [3/5] Building etcd branch '${branch}' on all nodes..."
  
  gcloud compute ssh "${REMOTE_USER}@${RAFT_NODE_PREFIX}-1" --zone="${ZONE1}" --quiet --command "bash -l -c '
    cd ~/etcd; git fetch; git checkout ${branch}; git pull origin ${branch};
    cd ~/etcd/contrib/raftexample; rm -rf raftexample*; go build -o raftexample;
  '" &
  gcloud compute ssh "${REMOTE_USER}@${RAFT_NODE_PREFIX}-2" --zone="${ZONE2}" --quiet --command "bash -l -c '
    cd ~/etcd; git fetch; git checkout ${branch}; git pull origin ${branch};
    cd ~/etcd/contrib/raftexample; rm -rf raftexample*; go build -o raftexample;
  '" &
  gcloud compute ssh "${REMOTE_USER}@${RAFT_NODE_PREFIX}-3" --zone="${ZONE3}" --quiet --command "bash -l -c '
    cd ~/etcd; git fetch; git checkout ${branch}; git pull origin ${branch};
    cd ~/etcd/contrib/raftexample; rm -rf raftexample*; go build -o raftexample;
  '" &
  gcloud compute ssh "${REMOTE_USER}@${RAFT_NODE_PREFIX}-4" --zone="${ZONE4}" --quiet --command "bash -l -c '
    cd ~/etcd; git fetch; git checkout ${branch}; git pull origin ${branch};
    cd ~/etcd/contrib/raftexample; rm -rf raftexample*; go build -o raftexample;
  '" &
  gcloud compute ssh "${REMOTE_USER}@${RAFT_NODE_PREFIX}-5" --zone="${ZONE5}" --quiet --command "bash -l -c '
    cd ~/etcd; git fetch; git checkout ${branch}; git pull origin ${branch};
    cd ~/etcd/contrib/raftexample; rm -rf raftexample*; go build -o raftexample;
  '" &
  wait

  for runIndex in $(seq 1 ${NUM_RUNS}); do
    echo ">>> [4/5] Starting Raft cluster (run ${runIndex}/${NUM_RUNS})..."
    
    # Start all nodes
    gcloud compute ssh "${REMOTE_USER}@${RAFT_NODE_PREFIX}-1" --zone="${ZONE1}" --quiet --command "bash -l -c '
      cd ~/etcd/contrib/raftexample; rm -f raft*.log;
      nohup ./raftexample --id 1 \
        --cluster http://${IP_NODE_1}:12379,http://${IP_NODE_2}:22379,http://${IP_NODE_3}:32379,http://${IP_NODE_4}:42379,http://${IP_NODE_5}:52379 \
        --port 12380 > raft1.log 2>&1 &
    '" &
    gcloud compute ssh "${REMOTE_USER}@${RAFT_NODE_PREFIX}-2" --zone="${ZONE2}" --quiet --command "bash -l -c '
      cd ~/etcd/contrib/raftexample; rm -f raft*.log;
      nohup ./raftexample --id 2 \
        --cluster http://${IP_NODE_1}:12379,http://${IP_NODE_2}:22379,http://${IP_NODE_3}:32379,http://${IP_NODE_4}:42379,http://${IP_NODE_5}:52379 \
        --port 12380 > raft2.log 2>&1 &
    '" &
    gcloud compute ssh "${REMOTE_USER}@${RAFT_NODE_PREFIX}-3" --zone="${ZONE3}" --quiet --command "bash -l -c '
      cd ~/etcd/contrib/raftexample; rm -f raft*.log;
      nohup ./raftexample --id 3 \
        --cluster http://${IP_NODE_1}:12379,http://${IP_NODE_2}:22379,http://${IP_NODE_3}:32379,http://${IP_NODE_4}:42379,http://${IP_NODE_5}:52379 \
        --port 12380 > raft3.log 2>&1 &
    '" &
    gcloud compute ssh "${REMOTE_USER}@${RAFT_NODE_PREFIX}-4" --zone="${ZONE4}" --quiet --command "bash -l -c '
      cd ~/etcd/contrib/raftexample; rm -f raft*.log;
      nohup ./raftexample --id 4 \
        --cluster http://${IP_NODE_1}:12379,http://${IP_NODE_2}:22379,http://${IP_NODE_3}:32379,http://${IP_NODE_4}:42379,http://${IP_NODE_5}:52379 \
        --port 12380 > raft4.log 2>&1 &
    '" &
    gcloud compute ssh "${REMOTE_USER}@${RAFT_NODE_PREFIX}-5" --zone="${ZONE5}" --quiet --command "bash -l -c '
      cd ~/etcd/contrib/raftexample; rm -f raft*.log;
      nohup ./raftexample --id 5 \
        --cluster http://${IP_NODE_1}:12379,http://${IP_NODE_2}:22379,http://${IP_NODE_3}:32379,http://${IP_NODE_4}:42379,http://${IP_NODE_5}:52379 \
        --port 12380 > raft5.log 2>&1 &
    '" &
    wait

    # Wait for leader election
    echo ">>> Waiting for Raft leader election..."
    while true; do
      LINE=$(gcloud compute ssh "${REMOTE_USER}@${RAFT_NODE_PREFIX}-1" \
        --zone="${ZONE1}" --quiet \
        --command "grep -m1 'elected leader' ~/etcd/contrib/raftexample/raft1.log" || true)
      if [ -n "$LINE" ]; then
        LEADER_ID=$(echo "$LINE" | sed -n 's/.*elected leader \([0-9][0-9]*\).*/\1/p')
        if [ -n "$LEADER_ID" ]; then
          echo ">>> Leader elected: node ${LEADER_ID}"
          break
        fi
      fi
      sleep 2
    done

    # Determine leader zone and IP
    case "$LEADER_ID" in
      1) LEADER_ZONE="${ZONE1}"; LEADER_IP="${IP_NODE_1}" ;;
      2) LEADER_ZONE="${ZONE2}"; LEADER_IP="${IP_NODE_2}" ;;
      3) LEADER_ZONE="${ZONE3}"; LEADER_IP="${IP_NODE_3}" ;;
      4) LEADER_ZONE="${ZONE4}"; LEADER_IP="${IP_NODE_4}" ;;
      5) LEADER_ZONE="${ZONE5}"; LEADER_IP="${IP_NODE_5}" ;;
    esac
    echo ">>> Leader is node ${LEADER_ID} in ${LEADER_ZONE} (IP=${LEADER_IP})"

    # Only run benchmark when leader is node 2 (EU)
    if [ "${LEADER_ID}" -ne 2 ]; then
      echo ">>> Leader is not node 2 (EU); skipping this run and restarting cluster..."
    else
      # Run trace workload from YCSB client (targeting leader in EU)
      echo ">>> Running trace workload against leader ${LEADER_IP}:12380..."
      gcloud compute ssh "${REMOTE_USER}@${YCSB_CLIENT}" \
        --zone="${ZONE2}" \
        --ssh-flag="-o ControlMaster=no" \
        --ssh-flag="-o ServerAliveInterval=30" \
        --ssh-flag="-o ServerAliveCountMax=5" \
        --quiet \
        --command="bash -l -c '
          cd ~/go-ycsb;
          mkdir -p results/trace/${branch};
          ./run_trace_bench.sh results/trace/${branch} \
            ${runIndex} ./${TRACE_FILE} ${LEADER_IP}:12380 \
            ${THREAD_COUNT} ${MAX_EXEC} ${LEADER_ZONE} ${READ_MODE}
        '"
      
      echo ">>> Run ${runIndex} completed."
    fi

    # Stop raft nodes
    echo ">>> Stopping raftexample on all nodes..."
    gcloud compute ssh "${REMOTE_USER}@raft-node-1" --zone="${ZONE1}" --ssh-flag="-o ControlMaster=no" --quiet --command="pkill -f raftexample" &
    gcloud compute ssh "${REMOTE_USER}@raft-node-2" --zone="${ZONE2}" --ssh-flag="-o ControlMaster=no" --quiet --command="pkill -f raftexample" &
    gcloud compute ssh "${REMOTE_USER}@raft-node-3" --zone="${ZONE3}" --ssh-flag="-o ControlMaster=no" --quiet --command="pkill -f raftexample" &
    gcloud compute ssh "${REMOTE_USER}@raft-node-4" --zone="${ZONE4}" --ssh-flag="-o ControlMaster=no" --quiet --command="pkill -f raftexample" &
    gcloud compute ssh "${REMOTE_USER}@raft-node-5" --zone="${ZONE5}" --ssh-flag="-o ControlMaster=no" --quiet --command="pkill -f raftexample" &
    wait
    sleep 3
  done

  # Fetch results
  echo ">>> [5/5] Fetching results..."
  mkdir -p "./results/trace/${branch}"
  gcloud compute scp \
    --zone="${ZONE2}" \
    "${REMOTE_USER}@${YCSB_CLIENT}:~/go-ycsb/results/trace/${branch}" \
    "./results/trace/" \
    --recurse
  echo ">>> Results fetched to ./results/trace/${branch}"
done

# Cleanup
echo ">>> Deleting VMs..."
gcloud compute instances delete "${RAFT_NODE_PREFIX}-1" --zone="${ZONE1}" --quiet &
gcloud compute instances delete "${RAFT_NODE_PREFIX}-2" --zone="${ZONE2}" --quiet &
gcloud compute instances delete "${RAFT_NODE_PREFIX}-3" --zone="${ZONE3}" --quiet &
gcloud compute instances delete "${RAFT_NODE_PREFIX}-4" --zone="${ZONE4}" --quiet &
gcloud compute instances delete "${RAFT_NODE_PREFIX}-5" --zone="${ZONE5}" --quiet &
gcloud compute instances delete "${YCSB_CLIENT}" --zone="${ZONE2}" --quiet &
wait

echo ">>> Done!"

