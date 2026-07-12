#!/usr/bin/env bash
# Reusable RESP-server benchmark (dreads / valkey / redis).
#
# Usage:  bench/redis-bench.sh <port> [label]
#   The server must already be running and pinned to its own core; this script
#   pins the client to CLIENT_CORES (default 4-11) and subscribers to SUB_CORES
#   (8-11) so the client never steals the server's core. Run ONE server at a
#   time (a co-resident idle server perturbs the numbers).
#
# Env knobs: P (pipeline, 16) N (requests, 800000) R (keyspace, 100000)
#            RUNS (repeats for min/med/max, 7) CLIENT_CORES SUB_CORES
#
# Sections: data-ops (min/med/max) · SET EX · pattern pub/sub scaling
#           (simple + aa*bb + aa*c?e*bb) · fan-out. NOTE: run the AOF section by
#           starting the server with persistence on (see bench/README).
set -u
PORT="${1:-6379}"; LABEL="${2:-server}"
CLIENT_CORES="${CLIENT_CORES:-4-11}"; SUB_CORES="${SUB_CORES:-8-11}"
P="${P:-16}"; N="${N:-800000}"; R="${R:-100000}"; RUNS="${RUNS:-7}"
CLI="redis-cli -p $PORT"
bench() { taskset -c "$CLIENT_CORES" redis-benchmark -p "$PORT" -P "$P" -c 50 -n "$N" -r "$R" -q "$@" 2>/dev/null; }
rate() { bench "$@" | tr '\r' '\n' | grep -oE '[0-9]+\.[0-9]+ requests per second' | tail -1 | grep -oE '^[0-9]+'; }
median() { sort -n | awk '{a[NR]=$1} END{print a[int((NR+1)/2)]}'; }
stat() { # <label> <benchmark args...>  -> "label min= med= max="
  local lbl="$1"; shift; local v=() i r
  for i in $(seq "$RUNS"); do r=$(rate "$@"); [ -n "$r" ] && v+=("$r"); done
  local p; p=$(printf '%s\n' "${v[@]}")
  printf '%-24s min=%-8s med=%-8s max=%s\n' "$lbl" \
    "$(echo "$p" | sort -n | head -1)" "$(echo "$p" | median)" "$(echo "$p" | sort -n | tail -1)"
}
peak() { # <label> <args...>  -> single best-of-RUNS (peak)
  local lbl="$1"; shift; local m=0 i r
  for i in $(seq "$RUNS"); do r=$(rate "$@"); [ -n "$r" ] && [ "$r" -gt "$m" ] && m=$r; done
  printf '%-24s %s\n' "$lbl" "$m"
}

echo "=== $LABEL (port $PORT)  P=$P N=$N R=$R runs=$RUNS ==="

echo "--- data ops (min/median/max rps) ---"
for t in set get incr lpush rpush lpop sadd hset spop zadd zpopmin mset; do stat "$t" -t "$t"; done

echo "--- SET with EX (TTL write path) ---"
stat "SET EX 100" SET "key:__rand_int__" v EX 100

echo "--- pattern pub/sub: publish to a NON-matching channel while N patterns are subscribed ---"
pat() { # <count> <template with NN> <publish-channel>
  local npat="$1" tmpl="$2" ch="$3"; set -f
  local pats=() i; for i in $(seq 0 $((npat - 1))); do pats+=("${tmpl//NN/$i}"); done
  $CLI psubscribe "${pats[@]}" >/dev/null 2>&1 & local sp=$!
  disown 2>/dev/null || true
  sleep 1.2; local np; np=$($CLI pubsub numpat 2>/dev/null)
  peak "  $tmpl x$npat (np=$np)" PUBLISH "$ch" hi
  kill "$sp" 2>/dev/null; sleep 0.3; set +f
}
for n in 1 100 1000; do pat "$n" 'pNN:*' 'zzz:x'; done
pat 1000 'aaNN*bb' 'zzz'
pat 1000 'aaNN*c?e*bb' 'zzz'

echo "--- fan-out: publish to N plain subscribers ---"
fan() { # <nsubs>
  local nsub="$1" pids=() i
  for i in $(seq "$nsub"); do taskset -c "$SUB_CORES" $CLI subscribe ch >/dev/null 2>&1 & pids+=("$!"); disown 2>/dev/null || true; done
  sleep 1.5; local ns; ns=$($CLI pubsub numsub ch | tail -1)
  peak "  PUBLISH x${nsub}subs (n=$ns)" PUBLISH ch hi
  for i in "${pids[@]}"; do kill -9 "$i" 2>/dev/null; done; sleep 0.5
}
peak "  PUBLISH 0 subs" PUBLISH ch hi
fan 10
fan 50
