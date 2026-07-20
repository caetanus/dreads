#!/usr/bin/env bash
# A/B: does raft-secret (BLAKE2b-128 per-frame MAC) cost SET throughput?
# Starts a real 3-node raft cluster, benchmarks SET against the leader, then
# repeats with `raft-secret` set on all three. Reports both + the delta.
#
# Real disk (/var/tmp, not tmpfs) so the durable path is exercised. Anti-zombie:
# pkill -9 -x dreads and verify count 0 between runs.
set -u
BIN=./bin/dreads
ROOT=/var/tmp/dreads_authab
N=${1:-200000}        # SET ops per run
CLIENTS=${2:-32}
SYNC=${3:-full}       # full (fsync) | off (isolate the MAC from fsync)
PIPE=${4:-1}          # redis-benchmark -P pipeline depth
TRIALS=${5:-3}        # trials per config; report the median

kill_all() { pkill -9 -x dreads 2>/dev/null; for _ in $(seq 1 40); do [ "$(pgrep -x dreads | wc -l)" -eq 0 ] && break; sleep 0.1; done; }
cli() { redis-cli -p "$1" "${@:2}"; }

write_conf() { # $1=id $2=secret(0/1)
  local id=$1 sec=$2 cport=$((7000+id)) rport=$((17000+id)) dir="$ROOT/$id"
  mkdir -p "$dir"
  local peers=""
  for j in 1 2 3; do [ "$j" != "$id" ] && peers="${peers:+$peers,}$j@127.0.0.1:$((17000+j))"; done
  {
    echo "port $cport"
    echo "dir $dir"
    echo "raft-node-id $id"
    echo "raft-port $rport"
    echo "raft-peers $peers"
    echo "synchronous $SYNC"
    [ "$sec" = "1" ] && echo 'raft-secret "benchmark-cluster-shared-secret-xyz"'
  } > "$ROOT/$id.conf"
}

start_cluster() { # $1=secret(0/1)
  rm -rf "$ROOT"; mkdir -p "$ROOT"
  for id in 1 2 3; do write_conf "$id" "$1"; $BIN "$ROOT/$id.conf" >"$ROOT/$id.log" 2>&1 & done
}

leader_port() { # wait for a leader, echo its client port
  for _ in $(seq 1 100); do
    for id in 1 2 3; do
      local L; L=$(cli $((7000+id)) RAFT LEADER 2>/dev/null)
      if [ -n "$L" ] && [ "$L" != "0" ] && [ "$L" -ge 1 ] 2>/dev/null; then echo $((7000+L)); return 0; fi
    done
    sleep 0.2
  done
  return 1
}

median() { printf '%s\n' "$@" | sort -n | awk '{a[NR]=$1} END{print (NR%2)?a[(NR+1)/2]:(a[NR/2]+a[NR/2+1])/2}'; }

run() { # $1=label $2=secret(0/1) -> echoes median qps
  local vals=()
  for _ in $(seq 1 "$TRIALS"); do
    start_cluster "$2"
    local lp; lp=$(leader_port) || { kill_all; continue; }
    cli "$lp" SET __warm ok >/dev/null 2>&1
    local q
    q=$(redis-benchmark -p "$lp" -t set -n "$N" -c "$CLIENTS" -P "$PIPE" -q 2>/dev/null \
        | sed -n 's/.*SET[: ]*\([0-9.]*\) requests per second.*/\1/p')
    [ -n "$q" ] && vals+=("$q")
    kill_all; sleep 0.5
  done
  echo "$1 trials=[${vals[*]}] median=$(median "${vals[@]}")"
}

echo "=== raft-secret A/B  (N=$N clients=$CLIENTS pipeline=$PIPE synchronous=$SYNC trials=$TRIALS) ==="
kill_all
A=$(run "no-auth " 0)
B=$(run "auth-MAC" 1)
echo "$A"
echo "$B"
awk -v a="$A" -v b="$B" 'BEGIN{
  na=a; nb=b; sub(/.*median=/,"",na); sub(/.*median=/,"",nb);
  if (na>0 && nb>0) printf "delta: %+.1f%% (%.0f -> %.0f SET/s)\n", (nb-na)/na*100, na, nb;
}'
kill_all
