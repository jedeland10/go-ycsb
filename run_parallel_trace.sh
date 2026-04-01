#!/usr/bin/env bash
set -euo pipefail

# =========================
# Config (edit if needed)
# =========================
PEER_PORTS=(12379 22379 32379 42379 52379)     # Raft replication ports (node1/2/3)
CLIENT_PORTS=(12380 22380 32380 42380 52380)   # Client API ports (node1/2/3)

THREADS=${THREADS:-50}
TRACE_FILE=${TRACE_FILE:-""}
READ_MODE=${READ_MODE:-"write"}
MAX_RECORDS=${MAX_RECORDS:-0}
MAX_EXEC=${MAX_EXEC:-200}
ZONE=${ZONE:-local}
YCSB=${YCSB:-"./run_trace_bench.sh"}
OUTPUT_DIR=${OUTPUT_DIR:-"results"}

# Defaults; can be overridden by flags
INCLUDE_CLIENT=false
MODE="spread"        # spread | leaderonly
LEADER_ID=1          # used only when MODE=leaderonly

OUT_DIR="results/_summary"
CSV_PER_NODE="${OUT_DIR}/nft_bytes_per_node.csv"
CSV_TO_PEER="${OUT_DIR}/nft_to_peer_out.csv"

# =========================
# Helpers
# =========================
need(){ command -v "$1" >/dev/null 2>&1 || { echo "Missing: $1"; exit 1; }; }
port_to_node() {
  case "$1" in
    12379|12380) echo 1 ;;
    22379|22380) echo 2 ;;
    32379|32380) echo 3 ;;
    42379|42380) echo 4 ;;
    52379|52380) echo 5 ;;
    *) echo 0 ;;
  esac
}

sudo -v || true   # cache sudo creds upfront

# Create table/chains + all rules (idempotent, safe under -euo pipefail)
ensure_nft() {
  # table + chains
  if ! sudo nft list table inet raftmon >/dev/null 2>&1; then
    sudo nft add table inet raftmon
  fi
  if ! sudo nft list chain inet raftmon out >/dev/null 2>&1; then
    sudo nft add chain inet raftmon out '{ type filter hook output priority 0; policy accept; }'
  fi
  if ! sudo nft list chain inet raftmon in  >/dev/null 2>&1; then
    sudo nft add chain inet raftmon in  '{ type filter hook input  priority 0; policy accept; }'
  fi

  # peer rules
  for p in "${PEER_PORTS[@]}"; do
    if ! sudo nft list chain inet raftmon out 2>/dev/null | grep -q 'comment "peer_'"$p"'_sent"'; then
      sudo nft add rule inet raftmon out tcp sport "$p" counter comment "peer_${p}_sent"
    fi
    if ! sudo nft list chain inet raftmon in 2>/dev/null | grep -q 'comment "peer_'"$p"'_recv"'; then
      sudo nft add rule inet raftmon in  tcp dport "$p" counter comment "peer_${p}_recv"
    fi
    if ! sudo nft list chain inet raftmon out 2>/dev/null | grep -q 'comment "to_peer_'"$p"'_out"'; then
      sudo nft add rule inet raftmon out tcp dport "$p" counter comment "to_peer_${p}_out"
    fi
    # Catch leader->followers even if followers dial the leader (source is leader peer port)
    if ! sudo nft list chain inet raftmon in 2>/dev/null | grep -q 'comment "from_peer_'"$p"'_in"'; then
      sudo nft add rule inet raftmon in  tcp sport "$p" counter comment "from_peer_${p}_in"
    fi
  done

  # client rules (optional)
  if $INCLUDE_CLIENT; then
    for p in "${CLIENT_PORTS[@]}"; do
      if ! sudo nft list chain inet raftmon out 2>/dev/null | grep -q 'comment "client_'"$p"'_sent"'; then
        sudo nft add rule inet raftmon out tcp sport "$p" counter comment "client_${p}_sent"
      fi
      if ! sudo nft list chain inet raftmon in 2>/dev/null | grep -q 'comment "client_'"$p"'_recv"'; then
        sudo nft add rule inet raftmon in  tcp dport "$p" counter comment "client_${p}_recv"
      fi
    done
  fi
}

# Delete table, recreate rules, then zero counters
reset_nft() {
  sudo nft delete table inet raftmon >/dev/null 2>&1 || true
  ensure_nft
  sudo nft reset counters table inet raftmon
}

check_listeners() {
  local ok=true
  for p in "${PEER_PORTS[@]}"; do
    ss -ltn | awk '{print $4}' | grep -q ":$p$" || { echo "WARN: no LISTEN on peer :$p" >&2; ok=false; }
  done
  for p in "${CLIENT_PORTS[@]}"; do
    ss -ltn | awk '{print $4}' | grep -q ":$p$" || { echo "WARN: no LISTEN on client :$p" >&2; ok=false; }
  done
  $ok || echo "Continuing anyway…"
}

run_clients() {
  echo ">>> Starting 5 YCSB trace clients | threads=${THREADS} mode=${MODE} trace=${TRACE_FILE} read_mode=${READ_MODE}"
  set -x
  if [[ "$MODE" == "leaderonly" ]]; then
    leader_cport="${CLIENT_PORTS[$((LEADER_ID-1))]}"
    "$YCSB" "$OUTPUT_DIR" 1 "$TRACE_FILE" "localhost:${leader_cport}" "$THREADS" "$MAX_EXEC" "$ZONE" "$READ_MODE" "$MAX_RECORDS" & P1=$!
    "$YCSB" "$OUTPUT_DIR" 2 "$TRACE_FILE" "localhost:${leader_cport}" "$THREADS" "$MAX_EXEC" "$ZONE" "$READ_MODE" "$MAX_RECORDS" & P2=$!
    "$YCSB" "$OUTPUT_DIR" 3 "$TRACE_FILE" "localhost:${leader_cport}" "$THREADS" "$MAX_EXEC" "$ZONE" "$READ_MODE" "$MAX_RECORDS" & P3=$!
    "$YCSB" "$OUTPUT_DIR" 4 "$TRACE_FILE" "localhost:${leader_cport}" "$THREADS" "$MAX_EXEC" "$ZONE" "$READ_MODE" "$MAX_RECORDS" & P4=$!
    "$YCSB" "$OUTPUT_DIR" 5 "$TRACE_FILE" "localhost:${leader_cport}" "$THREADS" "$MAX_EXEC" "$ZONE" "$READ_MODE" "$MAX_RECORDS" & P5=$!
  else
    "$YCSB" "$OUTPUT_DIR" 1 "$TRACE_FILE" "localhost:${CLIENT_PORTS[0]}" "$THREADS" "$MAX_EXEC" "$ZONE" "$READ_MODE" "$MAX_RECORDS" & P1=$!
    "$YCSB" "$OUTPUT_DIR" 2 "$TRACE_FILE" "localhost:${CLIENT_PORTS[1]}" "$THREADS" "$MAX_EXEC" "$ZONE" "$READ_MODE" "$MAX_RECORDS" & P2=$!
    "$YCSB" "$OUTPUT_DIR" 3 "$TRACE_FILE" "localhost:${CLIENT_PORTS[2]}" "$THREADS" "$MAX_EXEC" "$ZONE" "$READ_MODE" "$MAX_RECORDS" & P3=$!
    "$YCSB" "$OUTPUT_DIR" 4 "$TRACE_FILE" "localhost:${CLIENT_PORTS[3]}" "$THREADS" "$MAX_EXEC" "$ZONE" "$READ_MODE" "$MAX_RECORDS" & P4=$!
    "$YCSB" "$OUTPUT_DIR" 5 "$TRACE_FILE" "localhost:${CLIENT_PORTS[4]}" "$THREADS" "$MAX_EXEC" "$ZONE" "$READ_MODE" "$MAX_RECORDS" & P5=$!
  fi
  set +x
  wait $P1; S1=$?;
  wait $P2; S2=$?;
  wait $P3; S3=$?;
  wait $P4; S4=$?;
  wait $P5; S5=$?;


  echo "Exit codes: c1=$S1 c2=$S2 c3=$S3 c4=$S4 c5=$S5"
}

# Parse nft: emit CSV lines role,port,dir,bytes (JSON is robust to layout)
dump_rules_csv() {
  sudo nft -j list table inet raftmon | jq -r '
    .nftables[] | select(.rule) | .rule as $r
    | ($r.comment // "") as $c
    | ([$r.expr[]? | select(.counter?).counter.bytes] | first) as $b
    | select($c | test("^(peer|client)_[0-9]+_(sent|recv)$|^to_peer_[0-9]+_out$|^from_peer_[0-9]+_in$"))
    | if ($c|startswith("to_peer_")) then
        "to_peer,\(( $c|capture("to_peer_(?<p>[0-9]+)_out").p )),out,\($b//0)"
      elif ($c|startswith("from_peer_")) then
        "from_peer,\(( $c|capture("from_peer_(?<p>[0-9]+)_in").p )),in,\($b//0)"
      else
        ($c|capture("(?<role>peer|client)_(?<p>[0-9]+)_(?<dir>sent|recv)")
         | "\(.role),\(.p),\(.dir),\($b//0)")
      end'
}

report() {
  mkdir -p "$OUT_DIR"

  # Build maps from nft JSON dump
  declare -A SENT RECV TOPO FROM
  while IFS=, read -r role port dir bytes; do
    key="${role}:${port}"
    case "$role" in
      to_peer)     TOPO["$key"]="$bytes" ;;
      from_peer)   FROM["$key"]="$bytes" ;;
      peer)
        if [[ "$dir" == "sent" ]]; then SENT["$key"]="$bytes"; else RECV["$key"]="$bytes"; fi
        ;;
      client)
        if $INCLUDE_CLIENT; then
          if [[ "$dir" == "sent" ]]; then SENT["$key"]="$bytes"; else RECV["$key"]="$bytes"; fi
        fi
        ;;
    esac
  done < <(dump_rules_csv)

  # Per-node rows (peer)
  if [ ! -f "$CSV_PER_NODE" ]; then
    echo "node_id,role,threads,trace_file,read_mode,max_records,max_exec,sent_bytes,recv_bytes,sum_bytes,timestamp" > "$CSV_PER_NODE"
  fi
  echo "=== Bytes per node ==="
  echo "node_id,role,port,sent_bytes,recv_bytes,sum_bytes"
  for p in "${PEER_PORTS[@]}"; do
    nid=$(port_to_node "$p")
    s=${SENT["peer:${p}"]:-0}
    r=${RECV["peer:${p}"]:-0}
    sum=$((s + r))
    echo "${nid},peer,${p},${s},${r},${sum}"
    echo "${nid},peer,${THREADS},${TRACE_FILE},${READ_MODE},${MAX_RECORDS},${MAX_EXEC},${s},${r},${sum},$(date -Iseconds)" >> "$CSV_PER_NODE"
  done

  # Optional client rows per node
  if $INCLUDE_CLIENT; then
    for p in "${CLIENT_PORTS[@]}"; do
      nid=$(port_to_node "$p")
      s=${SENT["client:${p}"]:-0}
      r=${RECV["client:${p}"]:-0}
      sum=$((s + r))
      echo "${nid},client,${p},${s},${r},${sum}"
      echo "${nid},client,${THREADS},${TRACE_FILE},${READ_MODE},${MAX_RECORDS},${MAX_EXEC},${s},${r},${sum},$(date -Iseconds)" >> "$CSV_PER_NODE"
    done
  fi
  echo "CSV(per-node): $CSV_PER_NODE"

  # Egress to peer ports (any sender -> destination peer)
  if [ ! -f "$CSV_TO_PEER" ]; then
    echo "dest_node_id,peer_port,bytes_out_to_peer,threads,trace_file,read_mode,max_records,timestamp" > "$CSV_TO_PEER"
  fi
  echo "=== Egress to peer ports (any sender -> dest peer) ==="
  echo "dest_node,peer_port,bytes_out_to_peer"
  for p in "${PEER_PORTS[@]}"; do
    nid=$(port_to_node "$p")
    b=${TOPO["to_peer:${p}"]:-0}
    echo "${nid},${p},${b}"
    echo "${nid},${p},${b},${THREADS},${TRACE_FILE},${READ_MODE},${MAX_RECORDS},$(date -Iseconds)" >> "$CSV_TO_PEER"
  done
  echo "CSV(to-peer): $CSV_TO_PEER"

  # --- Replication summary (leader -> followers) ---
  local leader_port=""
  local leader_in=0
  for p in "${PEER_PORTS[@]}"; do
    b=${TOPO["to_peer:${p}"]:-0}
    if (( b > leader_in )); then leader_in=$b; leader_port=$p; fi
  done

  local rep_bytes=0
  for p in "${PEER_PORTS[@]}"; do
    [[ "$p" == "$leader_port" ]] || rep_bytes=$((rep_bytes + ${TOPO["to_peer:${p}"]:-0}))
  done

  local rep_bytes_from=${FROM["from_peer:${leader_port}"]:-0}

  echo "=== Replication summary ==="
  echo "leader_port=${leader_port}  replication_bytes(to_peer_followers)=${rep_bytes}"
  echo "cross_check(from_leader_sport_in)=${rep_bytes_from}"
}

usage() {
  cat <<EOF
Usage: $(basename "$0") [options]
  -t, --threads N           threads per client (default ${THREADS})
  -f, --trace-file PATH     path to trace file (.zst or .csv) [REQUIRED]
  -r, --read-mode MODE      skip | write | read (default ${READ_MODE})
  -n, --max-records N       max records from trace, 0=unlimited (default ${MAX_RECORDS})
  -m, --max-exec N          max exec seconds (default ${MAX_EXEC})
  -o, --output-dir DIR      output directory (default ${OUTPUT_DIR})
  -c, --include-client      also count client ports ${CLIENT_PORTS[*]}
  --mode MODE               spread | leaderonly   (default ${MODE})
  --leader-id N             1-5 (used if mode=leaderonly; default ${LEADER_ID})
  --reset-nft               delete & recreate nft table before run
  -h, --help                show help
EOF
}

# =========================
# CLI
# =========================
RESET=false
while [[ $# -gt 0 ]]; do
  case "$1" in
    -t|--threads) THREADS="$2"; shift 2;;
    -f|--trace-file) TRACE_FILE="$2"; shift 2;;
    -r|--read-mode) READ_MODE="$2"; shift 2;;
    -n|--max-records) MAX_RECORDS="$2"; shift 2;;
    -m|--max-exec) MAX_EXEC="$2"; shift 2;;
    -o|--output-dir) OUTPUT_DIR="$2"; shift 2;;
    -c|--include-client) INCLUDE_CLIENT=true; shift;;
    --mode) MODE="$2"; shift 2;;
    --leader-id) LEADER_ID="$2"; shift 2;;
    --reset-nft) RESET=true; shift;;
    -h|--help) usage; exit 0;;
    *) echo "Unknown arg: $1"; usage; exit 1;;
  esac
done

# =========================
# Main
# =========================
if [[ -z "$TRACE_FILE" ]]; then
  echo "ERROR: --trace-file is required"
  usage
  exit 1
fi

need sudo; need nft; need ss; need awk; need jq

if $RESET; then
  reset_nft
else
  ensure_nft
  sudo nft reset counters table inet raftmon
fi

echo ">>> Checking listeners"
check_listeners

if [[ "$MODE" == "leaderonly" ]]; then
  echo ">>> Running trace clients (mode=$MODE, leader_id=$LEADER_ID)"
else
  echo ">>> Running trace clients (mode=$MODE)"
fi
run_clients

echo ">>> Reading counters"
report

echo ">>> Done."
