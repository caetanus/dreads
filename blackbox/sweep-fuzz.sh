#!/bin/bash
# Loop the Valkey blackbox sweep N times with a SHORT per-file timeout to catch
# INTERMITTENT hangs/races (a clean single pass is not proof — the blocking/
# unblock/pause paths especially need repeated runs). Flags any file that times
# out (exit=124) in ANY iteration and leaves that server up for inspection.
# Usage: sweep-fuzz.sh <iterations> [per-file-timeout-secs] [file ...]
set -u
DREADS=/home/caetano/lab/dreads/bin/dreads
CONF=/home/caetano/lab/dreads/blackbox/dreads-suite.conf
SKIP=/home/caetano/lab/dreads/blackbox/valkey-sync.skip
PORT=7799
ITERS=${1:?iterations}; shift
TMO=${1:-120}; shift 2>/dev/null || true
FILES=${@:-"unit/type/incr unit/type/string unit/type/list unit/type/hash unit/type/set unit/type/zset unit/type/stream unit/type/stream-cgroups unit/expire unit/keyspace unit/scan unit/bitops unit/bitfield unit/other unit/sort unit/pause unit/scripting"}
cd /tmp/valkey
hangs=0
for it in $(seq $ITERS); do
  echo "==================== ITERATION $it/$ITERS ===================="
  for f in $FILES; do
    fuser -k -KILL $PORT/tcp 2>/dev/null
    while ss -tlnH | grep -q ":$PORT "; do sleep 0.2; done
    (cd /tmp && $DREADS $CONF $PORT >/tmp/sweepfuzz-server.log 2>&1 &)
    for i in $(seq 50); do redis-cli -p $PORT ping >/dev/null 2>&1 && break; sleep 0.1; done
    name=$(echo $f | tr '/' '_')
    timeout $TMO ./runtest --host 127.0.0.1 --port $PORT --single $f --skipfile $SKIP > /tmp/sweepfuzz-$name.log 2>&1
    rc=$?
    if [ $rc -eq 124 ]; then
      echo "!!! HANG it$it $f (timeout ${TMO}s) — server left on $PORT"
      hangs=$((hangs+1))
      redis-cli -p $PORT info clients 2>/dev/null | grep blocked_clients
      exit 124
    fi
    echo "  it$it $f exit=$rc"
  done
done
fuser -k -KILL $PORT/tcp 2>/dev/null
echo "=== $ITERS iterations, hangs=$hangs ==="
