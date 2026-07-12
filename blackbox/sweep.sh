#!/bin/bash
# Valkey blackbox sweep: fresh dreads per file (config leakage between files
# is real — an aborted file leaves encoding thresholds flipped).
# Usage: sweep.sh <outdir> [file ...]
set -u
DREADS=/home/caetano/lab/dreads/bin/dreads
CONF=/home/caetano/lab/dreads/blackbox/dreads-suite.conf
SKIP=/home/caetano/lab/dreads/blackbox/valkey-sync.skip
PORT=7777
OUT=${1:?outdir}; shift
FILES=${@:-"unit/type/incr unit/type/string unit/type/list unit/type/hash unit/type/set unit/type/zset unit/expire unit/keyspace unit/scan unit/bitops unit/other unit/sort"}
mkdir -p "$OUT"
cd /tmp/valkey
for f in $FILES; do
  pkill -9 -x dreads 2>/dev/null
  while ss -tlnH | grep -q ":$PORT "; do sleep 0.2; done
  (cd /tmp && $DREADS $CONF $PORT >"$OUT/server-$(echo $f | tr '/' '_').log" 2>&1 &)
  for i in $(seq 50); do redis-cli -p $PORT ping >/dev/null 2>&1 && break; sleep 0.1; done
  name=$(echo $f | tr '/' '_')
  timeout 900 ./runtest --host 127.0.0.1 --port $PORT --single $f --skipfile $SKIP > "$OUT/$name.log" 2>&1
  echo "$f exit=$?"
done
pkill -9 -x dreads 2>/dev/null
