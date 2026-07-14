#!/bin/bash
# Expire durability + TOUCH regression harness.
#
# THE RULE (per the user): for EVERY expire mechanism —
#   1. reload the AOF, verify the key expires at the RIGHT (absolute) date;
#   2. reload the AOF, TOUCH the key, verify TOUCH does NOT renew the expire.
#
# What is persisted is ABSOLUTE time (never relative — a relative log would
# resurrect an expired key and reset a live one across a restart). This proves it
# end-to-end through the real AOF file + reload path.
#
# Usage: expire_loadaof_test.sh [port]
set -u
PORT=${1:-7803}
AOF=/tmp/expire_loadaof_$PORT.aof
DR="$(dirname "$0")/../bin/dreads"
R(){ redis-cli -p $PORT "$@"; }
start(){ (cd /tmp && "$DR" $PORT --appendonly=$AOF >/tmp/ea_$PORT.log 2>&1 &)
  for i in $(seq 40); do redis-cli -p $PORT ping >/dev/null 2>&1 && return; sleep 0.1; done
  echo "server did not start"; cat /tmp/ea_$PORT.log; exit 1; }
stop(){ fuser -k $PORT/tcp 2>/dev/null; sleep 0.5; }

pkill -x dreads 2>/dev/null; fuser -k $PORT/tcp 2>/dev/null; sleep 0.4; rm -f $AOF
start
NOW=$(R TIME | head -1); FUT_S=$((NOW+1000)); FUT_MS=$(( (NOW+1000)*1000 ))
# every relative + absolute + write-with-ttl mechanism, all ~1000s in the future
R SET k_expire v    >/dev/null; R EXPIRE    k_expire   1000    >/dev/null
R SET k_pexpire v   >/dev/null; R PEXPIRE   k_pexpire  1000000 >/dev/null
R SET k_expireat v  >/dev/null; R EXPIREAT  k_expireat $FUT_S  >/dev/null
R SET k_pexpireat v >/dev/null; R PEXPIREAT k_pexpireat $FUT_MS >/dev/null
R SETEX  k_setex 1000 v    >/dev/null; R PSETEX k_psetex 1000000 v >/dev/null
R SET k_setex2 v EX 1000   >/dev/null; R SET k_setpx  v PX 1000000 >/dev/null
R SET k_setexat v EXAT $FUT_S >/dev/null; R SET k_setpxat v PXAT $FUT_MS >/dev/null
R SET k_getex v >/dev/null; R GETEX k_getex EX 1000 >/dev/null
KEYS=(k_expire k_pexpire k_expireat k_pexpireat k_setex k_psetex k_setex2 k_setpx k_setexat k_setpxat k_getex)
R SET k_short v >/dev/null; R PEXPIRE k_short 1500 >/dev/null   # must be gone after downtime
declare -A BEFORE; for k in "${KEYS[@]}"; do BEFORE[$k]=$(R PTTL $k); done
sleep 1; stop
fail=0
echo "=== RELOAD #1: expires at the right (absolute) date? [~2s downtime] ==="; sleep 2; start
for k in "${KEYS[@]}"; do
  now=$(R PTTL $k); bef=${BEFORE[$k]}
  if [ "$now" -lt "$bef" ] && [ "$now" -gt $((bef-6000)) ]; then st=OK; else st="!!!BAD"; fail=1; fi
  printf "  %-12s %s -> %s  %s\n" "$k" "$bef" "$now" "$st"
done
sv=$(R GET k_short); [ -z "$sv" ] && echo "  k_short expired in downtime  OK" || { echo "  k_short RESURRECTED !!!"; fail=1; }
echo "=== RELOAD #2: does TOUCH renew the expire? ==="; stop; sleep 1; start
for k in "${KEYS[@]}"; do
  b=$(R PTTL $k); R TOUCH $k >/dev/null; sleep 0.03; a=$(R PTTL $k)
  if [ "$a" -le "$b" ]; then st="OK no-renew"; else st="!!!RENEWED"; fail=1; fi
  printf "  %-12s %s -> %s  %s\n" "$k" "$b" "$a" "$st"
done
stop
echo "RESULT: $([ $fail -eq 0 ] && echo 'ALL PASS' || echo 'FAILURES')"
exit $fail
