#!/bin/bash
# Per-test isolated driver for the librdkafka suite vs dreads: each test gets
# its own runner process (a timeout/abort can't kill the batch), 5-way parallel.
SP=/tmp/claude-1000/-home-caetano-lab-dreads/12e4a0a7-03f4-42f9-93c0-38224267b4d5/scratchpad
T=$SP/librdkafka/tests
export LD_LIBRARY_PATH=$SP/librdkafka/src:$SP/librdkafka/src-cpp
cd $T
mkdir -p $SP/rdk_results
# the compiled-in test list: numbered .c files in the 0000-series
ls 0*.c* 2>/dev/null | sed -E 's/^([0-9]{4})-.*/\1/' | sort -u > $SP/rdk_tests.txt
# Tests with built-in wall-clock phases (mock reconnect waits, idle-close,
# sockem slow-connect simulation) that the speed multiplier 0.5 in test.conf
# falsely times out. They run SERIALLY at the end under multiplier 2 — all of
# them PASS against dreads at real-time pacing (verified 2026-08-24).
SLOW="0075 0088 0104 0121 0123 0131 0149"
is_slow() { case " $SLOW " in *" $1 "*) return 0;; esac; return 1; }
run_one() {
  local n=$1
  timeout ${2:-90} env TESTS=$n ./test-runner -Q >$SP/rdk_results/$n.log 2>&1
  local rc=$?
  if grep -qa "ALL TESTS PASSED" $SP/rdk_results/$n.log; then echo "PASS $n"
  elif grep -qaE "_SKIP|skipping|SKIPPED" $SP/rdk_results/$n.log && ! grep -qa "FAILED" $SP/rdk_results/$n.log; then echo "SKIP $n"
  else echo "FAIL $n (rc=$rc)"
  fi >> $SP/rdk_tally.txt
}
: > $SP/rdk_tally.txt
N=0
for t in $(cat $SP/rdk_tests.txt); do
  is_slow $t && continue
  run_one $t &
  N=$((N+1))
  [ $((N % 5)) -eq 0 ] && wait
done
wait
# slow tail: real-time multiplier, serial (no test.conf races)
sed -i 's/^test.timeout.multiplier=0.5/test.timeout.multiplier=2/' test.conf
for t in $SLOW; do run_one $t 300; done
sed -i 's/^test.timeout.multiplier=2/test.timeout.multiplier=0.5/' test.conf
sort $SP/rdk_tally.txt -k2 > $SP/rdk_tally_sorted.txt
awk '{c[$1]++} END{for(k in c) printf "%s=%d ", k, c[k]; print ""}' $SP/rdk_tally.txt
echo DRIVERDONE
