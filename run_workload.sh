#!/usr/bin/env bash
set -euo pipefail

# =========================
# Config (edit if needed)
# =========================
CLIENT_PORTS=(12380 22380 32380 42380 52380)

THREADS=${THREADS:-10}
OP_PER_CLIENT=${OP_PER_CLIENT:-100000}
KEYSIZE=${KEYSIZE:-4096}
RECORDCOUNT=${RECORDCOUNT:-1}
MAX_EXEC=${MAX_EXEC:-200}
ZONE=${ZONE:-local}
YCSB=${YCSB:-"./run_raft_bench.sh"}

MODE="spread"        # spread | leaderonly
LEADER_ID=1

# =========================
# Helpers
# =========================
check_listeners() {
  local ok=true
  for p in "${CLIENT_PORTS[@]}"; do
    ss -ltn | awk '{print $4}' | grep -q ":$p$" || { echo "WARN: no LISTEN on client :$p" >&2; ok=false; }
  done
  $ok || echo "Continuing anyway…"
}

run_clients() {
  echo ">>> Starting 5 YCSB clients | ops/client=${OP_PER_CLIENT} threads=${THREADS} mode=${MODE}"
  set -x
  if [[ "$MODE" == "leaderonly" ]]; then
    leader_cport="${CLIENT_PORTS[$((LEADER_ID-1))]}"
    "$YCSB" results 1 "$KEYSIZE" "localhost:${leader_cport}" "$RECORDCOUNT" "$OP_PER_CLIENT" "$THREADS" "$MAX_EXEC" "$ZONE" & P1=$!
    "$YCSB" results 2 "$KEYSIZE" "localhost:${leader_cport}" "$RECORDCOUNT" "$OP_PER_CLIENT" "$THREADS" "$MAX_EXEC" "$ZONE" & P2=$!
    "$YCSB" results 3 "$KEYSIZE" "localhost:${leader_cport}" "$RECORDCOUNT" "$OP_PER_CLIENT" "$THREADS" "$MAX_EXEC" "$ZONE" & P3=$!
    "$YCSB" results 4 "$KEYSIZE" "localhost:${leader_cport}" "$RECORDCOUNT" "$OP_PER_CLIENT" "$THREADS" "$MAX_EXEC" "$ZONE" & P4=$!
    "$YCSB" results 5 "$KEYSIZE" "localhost:${leader_cport}" "$RECORDCOUNT" "$OP_PER_CLIENT" "$THREADS" "$MAX_EXEC" "$ZONE" & P5=$!
  else
    "$YCSB" results 1 "$KEYSIZE" "localhost:${CLIENT_PORTS[0]}" "$RECORDCOUNT" "$OP_PER_CLIENT" "$THREADS" "$MAX_EXEC" "$ZONE" & P1=$!
    "$YCSB" results 2 "$KEYSIZE" "localhost:${CLIENT_PORTS[1]}" "$RECORDCOUNT" "$OP_PER_CLIENT" "$THREADS" "$MAX_EXEC" "$ZONE" & P2=$!
    "$YCSB" results 3 "$KEYSIZE" "localhost:${CLIENT_PORTS[2]}" "$RECORDCOUNT" "$OP_PER_CLIENT" "$THREADS" "$MAX_EXEC" "$ZONE" & P3=$!
    "$YCSB" results 4 "$KEYSIZE" "localhost:${CLIENT_PORTS[3]}" "$RECORDCOUNT" "$OP_PER_CLIENT" "$THREADS" "$MAX_EXEC" "$ZONE" & P4=$!
    "$YCSB" results 5 "$KEYSIZE" "localhost:${CLIENT_PORTS[4]}" "$RECORDCOUNT" "$OP_PER_CLIENT" "$THREADS" "$MAX_EXEC" "$ZONE" & P5=$!
  fi
  set +x
  wait $P1; S1=$?
  wait $P2; S2=$?
  wait $P3; S3=$?
  wait $P4; S4=$?
  wait $P5; S5=$?

  echo "Exit codes: c1=$S1 c2=$S2 c3=$S3 c4=$S4 c5=$S5"
}

usage() {
  cat <<EOF
Usage: $(basename "$0") [options]
  -t, --threads N           threads per client (default ${THREADS})
  -o, --op-per-client N     ops per client (default ${OP_PER_CLIENT})
  -R, --recordcount N       record count (default ${RECORDCOUNT})
  -k, --keysize N           key/value size (default ${KEYSIZE})
  -m, --max-exec N          max exec seconds (default ${MAX_EXEC})
  --mode MODE               spread | leaderonly   (default ${MODE})
  --leader-id N             1-5 (used if mode=leaderonly; default ${LEADER_ID})
  -h, --help                show help
EOF
}

# =========================
# CLI
# =========================
while [[ $# -gt 0 ]]; do
  case "$1" in
    -t|--threads) THREADS="$2"; shift 2;;
    -o|--op-per-client) OP_PER_CLIENT="$2"; shift 2;;
    -R|--recordcount) RECORDCOUNT="$2"; shift 2;;
    -k|--keysize) KEYSIZE="$2"; shift 2;;
    -m|--max-exec) MAX_EXEC="$2"; shift 2;;
    --mode) MODE="$2"; shift 2;;
    --leader-id) LEADER_ID="$2"; shift 2;;
    -h|--help) usage; exit 0;;
    *) echo "Unknown arg: $1"; usage; exit 1;;
  esac
done

# =========================
# Main
# =========================
echo ">>> Checking listeners"
check_listeners

if [[ "$MODE" == "leaderonly" ]]; then
  echo ">>> Running clients (mode=$MODE, leader_id=$LEADER_ID)"
else
  echo ">>> Running clients (mode=$MODE)"
fi
run_clients

echo ">>> Done."
