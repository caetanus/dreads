#!/usr/bin/env bash
# Reproducible write-throughput benchmark: dreads vs Valkey (and real Redis via
# docker, optional). SET, localhost, pipelined. See ../BENCHMARKS.md for the
# results and interpretation. Numbers are machine-specific — re-run here.
#
#   bench/run.sh                 # dreads + Valkey tiers
#   REDIS=1 bench/run.sh         # also real Redis 7 (docker) solo + cluster
#   N=300000 C=50 P=16 bench/run.sh
set -u
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="$ROOT/bin/dreads"
N=${N:-300000}; C=${C:-50}; P=${P:-16}       # ops, connections, pipeline depth
TMP=/tmp/dreads-bench
CLI="redis-cli"

cleanup() {
    pkill -9 -x dreads 2>/dev/null
    pkill -9 -x valkey-server 2>/dev/null
    [ "${REDIS:-0}" = 1 ] && docker rm -f rbsolo rbc1 rbc2 rbc3 2>/dev/null >/dev/null
    rm -rf "$TMP"
}
trap cleanup EXIT
cleanup; mkdir -p "$TMP"

# --- build dreads (release, LDC) if needed ---
if [ ! -x "$BIN" ]; then
    echo "building dreads (release, ldc2)..."
    (cd "$ROOT" && dub build -b release --compiler=ldc2) >/dev/null 2>&1
fi

# redis-benchmark SET rps for a port
setbench() { redis-benchmark -p "$1" -t set -n "$N" -c "$C" -P "$P" -q 2>&1 \
    | grep -aoE 'SET: [0-9.]+ requests per' | grep -oE '[0-9.]+' | head -1; }

wait_ping() { local t=0; until [ "$($CLI -p "$1" PING 2>/dev/null)" = PONG ]; do
    sleep 0.2; t=$((t+1)); [ $t -gt 50 ] && return 1; done; }

wait_leader() { local t=0; until [ "$($CLI -p "$1" RAFT STATUS 2>/dev/null | sed -n '2p')" = leader ]; do
    sleep 0.2; t=$((t+1)); [ $t -gt 60 ] && return 1; done; }

start_dreads_solo() { local port="$1" extra="${2:-}"; rm -rf "$TMP/d"; mkdir -p "$TMP/d"
    { printf 'port %s\ndir %s\n' "$port" "$TMP/d"; [ -n "$extra" ] && printf '%s\n' "$extra"; } > "$TMP/d/n.conf"
    "$BIN" "$TMP/d/n.conf" >"$TMP/d/log" 2>&1 & wait_ping "$port"; }

start_dreads_raft() { rm -rf "$TMP/r"; local id peers p
    for id in 1 2 3; do local d="$TMP/r/n$id"; mkdir -p "$d"; peers=""
        for p in 1 2 3; do [ $p = $id ] && continue; peers+="${peers:+,}$p@127.0.0.1:1700$p"; done
        printf 'port 700%s\ndir %s\nraft-node-id %s\nraft-peers %s\nraft-port 1700%s\nsynchronous off\n' \
            $id "$d" $id "$peers" $id > "$d/n.conf"
        "$BIN" "$d/n.conf" >"$d/out.log" 2>&1 &
    done; sleep 3
    for id in 1 2 3; do wait_leader 700$id && { echo 700$id; return; }; done; }

hr() { printf '  %-42s %s rps\n' "$1" "$2"; }

echo "=== dreads (single node) ==="
start_dreads_solo 7071
hr "solo (in-memory)" "$(setbench 7071)"; pkill -9 -x dreads 2>/dev/null; sleep 1
start_dreads_solo 7072 $'appendonly yes\nsynchronous normal'
hr "persistent AOF (group-commit fsync)" "$(setbench 7072)"; pkill -9 -x dreads 2>/dev/null; sleep 1

echo "=== dreads 3-node raft (synchronous majority, linearizable) ==="
L=$(start_dreads_raft)
hr "raft cluster (majority 2/3)" "$(setbench "$L")"
pkill -9 -x dreads 2>/dev/null; sleep 1

echo "=== Valkey (single node) ==="
valkey-server --port 8101 --appendonly no --save '' --dir "$TMP" >"$TMP/vk.out" 2>&1 & wait_ping 8101
hr "solo (in-memory)" "$(setbench 8101)"; pkill -9 -x valkey-server 2>/dev/null; sleep 1

echo "=== Valkey replication (primary + 2 replicas = 3 copies) ==="
valkey-server --port 8101 --appendonly no --save '' --dir "$TMP" >"$TMP/p.out" 2>&1 &
valkey-server --port 8102 --replicaof 127.0.0.1 8101 --save '' --dir "$TMP" >"$TMP/r1.out" 2>&1 &
valkey-server --port 8103 --replicaof 127.0.0.1 8101 --save '' --dir "$TMP" >"$TMP/r2.out" 2>&1 &
sleep 3
hr "async (weak: primary-only ack)" "$(setbench 8101)"
hr "WAIT 1 (majority 2/3, sync)" "$(python3 "$ROOT/bench/wait_bench.py" 8101 1 "$C" "$P")"
hr "WAIT 2 (all 3 copies, sync)" "$(python3 "$ROOT/bench/wait_bench.py" 8101 2 "$C" "$P")"
pkill -9 -x valkey-server 2>/dev/null; sleep 1

if [ "${REDIS:-0}" = 1 ]; then
    echo "=== real Redis 7 (docker, --net=host) ==="
    docker run -d --net=host --name rbsolo redis:7-alpine \
        redis-server --save '' --appendonly no --port 9001 >/dev/null && sleep 2
    hr "solo (in-memory)" "$(setbench 9001)"; docker rm -f rbsolo >/dev/null 2>&1
    for i in 1 2 3; do docker run -d --net=host --name rbc$i redis:7-alpine \
        redis-server --port 910$i --cluster-enabled yes --cluster-config-file "$TMP/n910$i.conf" \
        --cluster-node-timeout 5000 --appendonly no --save '' >/dev/null; done; sleep 2
    redis-cli --cluster create 127.0.0.1:9101 127.0.0.1:9102 127.0.0.1:9103 --cluster-yes >/dev/null 2>&1
    until [ "$(redis-cli -p 9101 cluster info 2>/dev/null | grep -ao cluster_state:ok)" ]; do sleep 1; done
    hr "cluster 3 masters (SHARDED, no replication)" \
        "$(redis-benchmark --cluster -p 9101 -t set -n "$N" -c "$C" -P "$P" -q 2>&1 | grep -aoE 'SET: [0-9.]+ requests per' | grep -oE '[0-9.]+' | head -1)"
    docker rm -f rbc1 rbc2 rbc3 >/dev/null 2>&1
fi
echo "(-c$C -P$P, N=$N, localhost)"
