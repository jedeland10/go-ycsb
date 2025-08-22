#!/usr/bin/env bash
set -euo pipefail

# ---------- Config ----------
PEER_PORTS=(12379 22379 32379)     # Raft peer (replication) ports
CLIENT_PORTS=(12380 22380 32380)   # Client API ports (optional)
INCLUDE_CLIENT=${INCLUDE_CLIENT:-false}

THREADS=${THREADS:-1000}
OP_PER_CLIENT=${OP_PER_CLIENT:-500000}   # 166667 if ~500k total across 3
KEYSIZE=${KEYSIZE:-4096}
RECORDCOUNT=${RECORDCOUNT:-10000}
MAX_EXEC=${MAX_EXEC:-60}
RUN_ID=${RUN_ID:-r1}
ZONE=${ZONE:-local}

YCSB=${YCSB:-"./run_raft_bench.sh"}      # your launcher

OUT_DIR="results/_summary"
CSV_PATH="${OUT_DIR}/nft_bytes_${RUN_ID}.csv"

# ---------- Helpers ----------
need_cmd(){ command -v "$1" >/dev/null 2>&1 || { echo "Missing dependency: $1"; exit 1; }; }

ensure_nft_setup() {
  sudo nft list table inet raftmon >/dev/null 2>&1 || sudo nft add table inet raftmon
  sudo nft list chain inet raftmon out >/dev/null 2>&1 || \
    sudo nft add chain inet raftmon out '{ type filter hook output priority 0; }'
  sudo nft list chain inet raftmon in  >/dev/null 2>&1 || \
    sudo nft add chain inet raftmon in  '{ type filter hook input  priority 0; }'

  local ports=("${PEER_PORTS[@]}")
  $INCLUDE_CLIENT && ports+=("${CLIENT_PORTS[@]}")

  # For each port:
  # - count BYTES SENT BY peer (OUTPUT sport == port)   -> comment "peer:<port>:sent"
  # - count BYTES RECEIVED BY peer (INPUT  dport == port)-> comment "peer:<port>:recv"
  for p in "${ports[@]}"; do
    # OUTPUT sport (sent by peer p)
    if ! sudo nft list chain inet raftmon out | grep -q "tcp sport $p .* comment \"peer:$p:sent\""; then
      sudo nft add rule inet raftmon out tcp sport $p counter comment "peer:$p:sent"
    fi
    # INPUT dport (received by peer p)
    if ! sudo nft list chain inet raftmon in  | grep -q "tcp dport $p .* comment \"peer:$p:recv\""; then
      sudo nft add rule inet raftmon in  tcp dport $p counter comment "peer:$p:recv"
    fi
  done
}

zero_counters(){ sudo nft reset counters table inet raftmon; }

check_listening() {
  local ok=true
  for p in "${CLIENT_PORTS[@]}"; do
    ss -ltn | awk '{print $4}' | grep -q ":$p$" || { echo "WARN: no listener on client $p" >&2; ok=false; }
  done
  for p in "${PEER_PORTS[@]}"; do
    ss -ltn | awk '{print $4}' | grep -q ":$p$" || { echo "WARN: no listener on peer $p" >&2; ok=false; }
  done
  $ok || echo "Continuing anyway…"
}

run_three_clients() {
  echo ">>> Starting 3 concurrent YCSB clients: ${OP_PER_CLIENT} ops/client, ${THREADS} threads/client"
  "$YCSB" results "${RUN_ID}" "${KEYSIZE}" "localhost:${CLIENT_PORTS[0]}" "${RECORDCOUNT}" "${OP_PER_CLIENT}" "${THREADS}" "${MAX_EXEC}" "${ZONE}" &
  "$YCSB" results "${RUN_ID}" "${KEYSIZE}" "localhost:${CLIENT_PORTS[1]}" "${RECORDCOUNT}" "${OP_PER_CLIENT}" "${THREADS}" "${MAX_EXEC}" "${ZONE}" &
  "$YCSB" results "${RUN_ID}" "${KEYSIZE}" "localhost:${CLIENT_PORTS[2]}" "${RECORDCOUNT}" "${OP_PER_CLIENT}" "${THREADS}" "${MAX_EXEC}" "${ZONE}" &
  wait
}

# parse nft output to "peer,dir,bytes"
dump_per_peer() {
  sudo nft list table inet raftmon \
  | awk '
      /comment "peer:/ {
        # Example rule line contains the comment; next line has "counter packets X bytes Y"
        if (match($0,/comment "peer:([0-9]+):(sent|recv)"/,M)) { port=M[1]; dir=M[2]; getline;
          for (i=1;i<=NF;i++) if ($i=="bytes") { bytes=$(i+1); break }
          printf "%s,%s,%s\n", port, dir, bytes
        }
      }'
}

report_bytes() {
  echo "=== Per-peer bytes (loopback accurate) ==="
  local TMP; TMP=$(mktemp)
  dump_per_peer | sort -t, -k1,1 -k2,2 > "$TMP"
  column -s, -t "$TMP" | sed '1i port,dir,bytes' | column -s, -t

  # totals
  local SENT_TOTAL RECV_TOTAL SUM_TOTAL
  SENT_TOTAL=$(awk -F, '$2=="sent"{s+=$3} END{print s+0}' "$TMP")
  RECV_TOTAL=$(awk -F, '$2=="recv"{s+=$3} END{print s+0}' "$TMP")
  SUM_TOTAL=$((SENT_TOTAL + RECV_TOTAL))

  echo "=== Totals ==="
  echo "bytes_sent_by_peers (sum of OUTPUT sport): $SENT_TOTAL"
  echo "bytes_received_by_peers (sum of INPUT dport): $RECV_TOTAL"
  echo "sum (sent+recv): $SUM_TOTAL"

  mkdir -p "$OUT_DIR"
  if [ ! -f "$CSV_PATH" ]; then
    echo "run_id,include_client,threads,op_per_client,total_ops,recordcount,keysize,max_exec,sent_bytes,recv_bytes,sum_bytes,timestamp" > "$CSV_PATH"
  fi
  local TOTAL_OPS=$((OP_PER_CLIENT * 3))
  echo "${RUN_ID},${INCLUDE_CLIENT},${THREADS},${OP_PER_CLIENT},${TOTAL_OPS},${RECORDCOUNT},${KEYSIZE},${MAX_EXEC},${SENT_TOTAL},${RECV_TOTAL},${SUM_TOTAL},$(date -Iseconds)" >> "$CSV_PATH"
  echo "CSV appended to: $CSV_PATH"
  rm -f "$TMP"
}

usage() {
  cat <<EOF
Usage: $(basename "$0") [options]
  -r, --run-id ID           Run id (default: ${RUN_ID})
  -o, --op-per-client N     Ops per client (default: ${OP_PER_CLIENT})
  -t, --threads N           Threads per client (default: ${THREADS})
  -R, --recordcount N       Record count (default: ${RECORDCOUNT})
  -k, --keysize N           Key/value size arg (default: ${KEYSIZE})
  -m, --max-exec N          Max exec (default: ${MAX_EXEC})
  -c, --include-client      Also count client ports ${CLIENT_PORTS[*]}
EOF
}

# ---------- CLI ----------
while [[ $# -gt 0 ]]; do
  case "$1" in
    -r|--run-id) RUN_ID="$2"; shift 2;;
    -o|--op-per-client) OP_PER_CLIENT="$2"; shift 2;;
    -t|--threads) THREADS="$2"; shift 2;;
    -R|--recordcount) RECORDCOUNT="$2"; shift 2;;
    -k|--keysize) KEYSIZE="$2"; shift 2;;
    -m|--max-exec) MAX_EXEC="$2"; shift 2;;
    -c|--include-client) INCLUDE_CLIENT=true; shift;;
    -h|--help) usage; exit 0;;
    *) echo "Unknown arg: $1"; usage; exit 1;;
  esac
done

# ---------- Main ----------
need_cmd sudo; need_cmd nft; need_cmd awk; need_cmd ss

echo ">>> Ensuring nftables rules exist"
ensure_nft_setup

echo ">>> Zeroing counters (T0)"
zero_counters

echo ">>> Checking listeners"
check_listening

run_three_clients

echo ">>> Reading counters (T1)"
report_bytes

echo ">>> Done."
