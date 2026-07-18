#!/usr/bin/env bash
# Active-expire live benchmark: dreads (active-off / active-on) vs Valkey.
#
# Unlike `SET EX 100` (100s TTL never expires during the run → insert path only),
# this uses a SHORT TTL over a LARGE keyspace so keys constantly expire DURING the
# run and the background active-expire cycle actually reaps them — the reaping
# competes with request handling on the single event loop. Metric: sustained
# SET-with-PX throughput under that expiry pressure.
#
# Methodology (per bench/README + memory): each server pinned to ONE core (2),
# client pinned to 9 cores (3-11) so it never starves the server; ONE server at a
# time; the three configs are INTERLEAVED round-robin per round so drift cancels;
# performance governor; NO monitor client attached.
set -u
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="$ROOT/bin/dreads"
SRV_CORE="${SRV_CORE:-2}"; CLI_CORES="${CLI_CORES:-3-11}"
P="${P:-16}"; C="${C:-50}"; N="${N:-1000000}"; R="${R:-2000000}"; TTL="${TTL:-200}"
RUNS="${RUNS:-5}"
TMP=/tmp/ae-bench
DPORT=7101; VPORT=7102

cleanup() { fuser -k -KILL $DPORT/tcp $VPORT/tcp 2>/dev/null; pkill -9 -x valkey-server 2>/dev/null; rm -rf "$TMP"; }
trap cleanup EXIT
cleanup; mkdir -p "$TMP"; sleep 0.3

wait_ping() { local t=0; until [ "$(redis-cli -p "$1" PING 2>/dev/null)" = PONG ]; do sleep 0.15; t=$((t+1)); [ $t -gt 60 ] && return 1; done; }

# one SET-with-PX run → rps (keys expire mid-run at this TTL/keyspace)
run_rps() { taskset -c "$CLI_CORES" redis-benchmark -p "$1" -P "$P" -c "$C" -n "$N" -r "$R" -q \
    SET "key:__rand_int__" v PX "$TTL" 2>/dev/null | tr '\r' '\n' \
    | grep -oE '[0-9]+\.[0-9]+ requests per second' | tail -1 | grep -oE '^[0-9]+'; }

start_dreads() { # <active-expire yes|no>
  rm -rf "$TMP/d"; mkdir -p "$TMP/d"
  printf 'port %s\ndir %s\nactive-expire %s\n' "$DPORT" "$TMP/d" "$1" > "$TMP/d/n.conf"
  taskset -c "$SRV_CORE" "$BIN" "$TMP/d/n.conf" >"$TMP/d/log" 2>&1 & wait_ping "$DPORT"; }
start_valkey() {
  taskset -c "$SRV_CORE" valkey-server --port "$VPORT" --save '' --appendonly no \
    --daemonize no --logfile "$TMP/vlog" >/dev/null 2>&1 & wait_ping "$VPORT"; }

declare -A vals
add() { vals[$1]+="$2 "; }
report() { local lbl="$1" p; p=$(printf '%s\n' ${vals[$1]}); \
  printf '%-22s min=%-8s med=%-8s max=%s\n' "$lbl" \
    "$(echo "$p"|sort -n|head -1)" "$(echo "$p"|sort -n|awk '{a[NR]=$1}END{print a[int((NR+1)/2)]}')" "$(echo "$p"|sort -n|tail -1)"; }

echo "=== active-expire live: SET PX $TTL, keyspace=$R, P=$P C=$C N=$N, $RUNS runs interleaved ==="
for run in $(seq "$RUNS"); do
  # dreads active-OFF
  start_dreads no;  add off "$(run_rps $DPORT)"; fuser -k -KILL $DPORT/tcp 2>/dev/null; sleep 0.5
  # dreads active-ON
  start_dreads yes; r=$(run_rps $DPORT)
  exp=$(redis-cli -p $DPORT info stats 2>/dev/null | grep -a expired_keys | grep -oE '[0-9]+')
  add on "$r"; echo "  [run $run] dreads active-on expired_keys=$exp"
  fuser -k -KILL $DPORT/tcp 2>/dev/null; sleep 0.5
  # valkey
  start_valkey; r=$(run_rps $VPORT)
  vexp=$(redis-cli -p $VPORT info stats 2>/dev/null | grep -a expired_keys | grep -oE '[0-9]+')
  vsz=$(redis-cli -p $VPORT dbsize 2>/dev/null)
  add vk "$r"; echo "  [run $run] valkey expired_keys=$vexp dbsize=$vsz"
  fuser -k -KILL $VPORT/tcp 2>/dev/null; pkill -9 -x valkey-server 2>/dev/null; sleep 0.5
done
echo "--- results (rps) ---"
report off; report on; report vk
