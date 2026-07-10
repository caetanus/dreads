#!/usr/bin/env bash
# PubSub matching-cost benchmark: PUBLISH rate to a NON-matching channel while P
# patterns are registered. Isolates pattern-match cost (no delivery). Three
# pattern shapes expose where dreads' header index helps and where it does NOT:
#   prefix     -> dreads flat (header probe),  Valkey O(P)          [dreads wins]
#   suffix     -> dreads fallback O(P) == Valkey O(P)               [tie]
#   sameheader -> all P share a header bucket -> both O(P)          [neither good]
set -u
cd "$(dirname "$0")/.."
DREADS=./bin/dreads
SUB=bench/pubsub_sub.py
PS=(0 1000 10000)
N=30000
rate() { grep -aoE '[0-9]+\.[0-9]+ requests per second' | grep -oE '^[0-9.]+' | head -1; }

wait_ready() { for _ in $(seq 1 300); do grep -q READY "$1" 2>/dev/null && return 0; sleep 0.1; done; return 1; }

run_case() { # $1 server-name  $2 port  $3 mode  $4 publish-channel
  local name=$1 port=$2 mode=$3 chan=$4 P sf spid rf
  printf "  %-7s %-10s" "$name" "$mode"
  for P in "${PS[@]}"; do
    spid=""
    if [ "$P" -gt 0 ]; then
      sf=/tmp/pm_$port.out; : > "$sf"
      python3 "$SUB" 127.0.0.1 "$port" "$P" "$mode" > "$sf" 2>&1 &
      spid=$!
      wait_ready "$sf" || { printf " P%s=NOTREADY" "$P"; kill -9 $spid 2>/dev/null; continue; }
    fi
    rf=$(timeout 30 redis-benchmark -n "$N" -c 50 -P 16 -q -p "$port" publish "$chan" hi 2>&1 | rate)
    printf "  P%s=%s" "$P" "${rf:-ERR}"
    [ -n "$spid" ] && { kill -9 $spid 2>/dev/null; wait $spid 2>/dev/null; }
  done
  echo
}

# Kill by PORT, not by process name: SO_REUSEPORT lets stale/renamed instances
# (e.g. a leftover dreads-v3) silently share the port and split connections,
# which quietly breaks per-instance state like pub/sub. fuser -k is the only
# reliable way to guarantee a single server per port.
fuser -k 7001/tcp 7101/tcp 2>/dev/null; sleep 1
for p in 7001 7101; do
  [ "$(ss -ltn 2>/dev/null | grep -c :$p)" -ne 0 ] && { echo "ABORT: port $p still has a listener"; exit 1; }
done
rm -rf /tmp/pmd; mkdir -p /tmp/pmd
printf 'port 7001\ndir /tmp/pmd\n' > /tmp/pmd/c.conf
$DREADS /tmp/pmd/c.conf >/tmp/pmd/d.log 2>&1 &
valkey-server --port 7101 --save '' --appendonly no --logfile /tmp/pmd/v.log &
sleep 2

echo "PUBLISH rps (columns = pattern count P):"
for m in prefix suffix sameheader; do
  case $m in
    prefix)     ch=aaa ;;
    suffix)     ch=x:nomatch ;;
    sameheader) ch=shared:x:nomatch ;;
  esac
  run_case dreads 7001 "$m" "$ch"
  run_case valkey 7101 "$m" "$ch"
done

fuser -k 7001/tcp 7101/tcp 2>/dev/null
echo DONE
