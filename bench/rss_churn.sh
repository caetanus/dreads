#!/usr/bin/env bash
# RSS-under-churn: the metric that actually separates allocators (throughput does
# not — SET/SADD are network-bound, allocator <1% of CPU). Fills the keyspace with
# values of VARYING sizes (different allocator buckets), then churns in waves
# (delete a stripe, refill with a different size) to fragment. Reports RSS (real
# process resident memory) and used_memory (=keyspaceBytesUsed) so RSS/used is a
# fragmentation proxy — a size-segregated freelist should reclaim holes a plain
# malloc leaves stranded.
#
# Usage: rss_churn.sh <binary> [label]   (server pinned to core 2)
set -u
BIN="${1:?binary}"; LABEL="${2:-server}"
PORT="${PORT:-7912}"; ROUNDS="${ROUNDS:-40}"; BATCH="${BATCH:-20000}"
SIZES=(16 64 200 500 1200 4000)   # straddle the segregator thresholds
CLI="redis-cli -p $PORT"

fuser -k -KILL "$PORT"/tcp >/dev/null 2>&1; sleep 0.3
rm -rf /tmp/rssd; mkdir -p /tmp/rssd
printf 'port %s\ndir /tmp/rssd\nappendonly no\n' "$PORT" > /tmp/rssd/n.conf
taskset -c 2 "$BIN" /tmp/rssd/n.conf >/tmp/rssd/log 2>&1 &
SRV=$!
for i in $(seq 50); do $CLI ping >/dev/null 2>&1 && break; sleep 0.1; done

rss() { awk '/VmRSS/{print $2}' /proc/$SRV/status 2>/dev/null; }   # KiB
used() { $CLI info memory 2>/dev/null | tr -d '\r' | awk -F: '/^used_memory:/{print $2}'; }

# one wave: write BATCH keys on `stripe`, each value cycling through SIZES.
# awk builds one value string per size once, then emits the whole SET stream.
SZLIST="$(IFS=,; echo "${SIZES[*]}")"
wave() { # <stripe>
  awk -v stripe="$1" -v batch="$BATCH" -v szlist="$SZLIST" 'BEGIN{
      n=split(szlist,S,","); for(j=1;j<=n;j++){v="";for(c=0;c<S[j];c++)v=v"x";V[j]=v}
      for(i=0;i<batch;i++){ j=((i+stripe)%n)+1; printf "SET k:%d:%d %s\n",stripe,i,V[j] }
    }' | $CLI --pipe >/dev/null 2>&1
}
delstripe() { # <stripe>
  awk -v stripe="$1" -v batch="$BATCH" 'BEGIN{
      for(i=0;i<batch;i++) printf "DEL k:%d:%d\n",stripe,i }' | $CLI --pipe >/dev/null 2>&1
}

echo "=== $LABEL (port $PORT) rounds=$ROUNDS batch=$BATCH ==="
printf '%-6s %-12s %-12s %-8s\n' round RSS_KiB used_bytes RSS/used
peakrss=0
for r in $(seq "$ROUNDS"); do
  wave "$r"
  # churn: once we have a backlog, delete a stripe 8 rounds old (fragments)
  (( r > 8 )) && delstripe $(( r - 8 ))
  R=$(rss); U=$(used)
  (( R > peakrss )) && peakrss=$R
  ratio=$(awk -v r="$R" -v u="$U" 'BEGIN{ if(u>0) printf "%.2f", (r*1024)/u; else print "-" }')
  printf '%-6s %-12s %-12s %-8s\n' "$r" "$R" "$U" "$ratio"
done
echo "peak RSS: ${peakrss} KiB   final used_memory: $(used) bytes"
fuser -k -KILL "$PORT"/tcp >/dev/null 2>&1
