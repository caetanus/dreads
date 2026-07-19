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
# The applicable Valkey unit suite run against dreads (Valkey as read-only oracle).
# Original 12 + the 2026-07-19 expansion (all confirmed 0-failed against dreads):
# scripting 548/0, hashexpire 230/0, stream 71/0, stream-cgroups 53/0, pubsub 35/0,
# pause 19/0, bitfield 18/0, list-3 11/0, quit 3/0, list-2 2/0, auth/wait/pubsubshard 0/0.
# Files still failing/hanging (multi, dump, hyperloglog, protocol, info-command,
# introspection[-2], functions, geo, slowlog) are catalogued in BLACKBOX-TODO.md and
# stay OUT of the default until fixed or skip-listed.
FILES=${@:-"unit/type/incr unit/type/string unit/type/list unit/type/list-2 unit/type/list-3 unit/type/hash unit/type/set unit/type/zset unit/type/stream unit/type/stream-cgroups unit/expire unit/hashexpire unit/keyspace unit/scan unit/bitops unit/bitfield unit/other unit/sort unit/pubsub unit/pubsubshard unit/pause unit/quit unit/auth unit/wait unit/scripting unit/multi"}
mkdir -p "$OUT"
cd /tmp/valkey
for f in $FILES; do
  # kill by PORT, never by process name: -x would take down dreads
  # instances that belong to other sessions/sweeps
  fuser -k $PORT/tcp 2>/dev/null
  while ss -tlnH | grep -q ":$PORT "; do sleep 0.2; done
  (cd /tmp && $DREADS $CONF $PORT >"$OUT/server-$(echo $f | tr '/' '_').log" 2>&1 &)
  for i in $(seq 50); do redis-cli -p $PORT ping >/dev/null 2>&1 && break; sleep 0.1; done
  name=$(echo $f | tr '/' '_')
  timeout 900 ./runtest --host 127.0.0.1 --port $PORT --single $f --skipfile $SKIP > "$OUT/$name.log" 2>&1
  echo "$f exit=$?"
done
fuser -k $PORT/tcp 2>/dev/null
