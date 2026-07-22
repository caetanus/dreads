#!/bin/bash
# Run the Valkey unit suite against a SHARDED dreads (multi-router) to surface
# the cross-shard/broadcast gaps: a suite that touches KEYS/SCAN/DBSIZE/FLUSH*,
# multi-key ops, or cross-connection pub/sub will FAIL under shards>1 until the
# broadcast primitive + CROSSSLOT land. Usage: sweep-shards.sh <shards> <outdir> [file ...]
set -u
DREADS=/home/caetano/lab/dreads/bin/dreads
CONF=/home/caetano/lab/dreads/blackbox/dreads-suite.conf
SKIP=/home/caetano/lab/dreads/blackbox/valkey-sync.skip
PORT=7777
SHARDS=${1:?shards}; OUT=${2:?outdir}; shift 2
# the broadcast/cross-shard-sensitive suites (the rest are single-key = already fine)
FILES=${@:-"unit/keyspace unit/scan unit/other unit/pubsub unit/pubsubshard unit/type/incr unit/type/string"}
mkdir -p "$OUT"
cd /tmp/valkey || exit 1
for f in $FILES; do
  fuser -k $PORT/tcp 2>/dev/null
  while ss -tlnH | grep -q ":$PORT "; do sleep 0.2; done
  (cd /tmp && $DREADS $CONF $PORT --shards $SHARDS >"$OUT/server-$(echo $f | tr '/' '_').log" 2>&1 &)
  for i in $(seq 50); do redis-cli -p $PORT ping >/dev/null 2>&1 && break; sleep 0.1; done
  name=$(echo $f | tr '/' '_')
  timeout 300 ./runtest --host 127.0.0.1 --port $PORT --single $f --singledb --skipfile $SKIP > "$OUT/$name.log" 2>&1
  rc=$?
  # strip ANSI, then read the reliable signals
  clean=$(sed 's/\x1b\[[0-9;]*m//g' "$OUT/$name.log")
  ok=$(printf '%s' "$clean" | grep -c '\[ok\]')
  err=$(printf '%s' "$clean" | grep -cE '\[err\]|\[exception\]')
  if printf '%s' "$clean" | grep -q 'All tests passed'; then verdict=PASS
  elif [ "$err" -gt 0 ]; then verdict=FAIL
  elif [ "$rc" -ne 0 ]; then verdict=ERROR/timeout
  else verdict='?'; fi
  printf 'shards=%s  %-22s %-12s ok=%s err=%s\n' "$SHARDS" "$f" "$verdict" "$ok" "$err"
done
fuser -k $PORT/tcp 2>/dev/null
