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
run_one() {
  local n=$1
  timeout 90 env TESTS=$n ./test-runner -Q >$SP/rdk_results/$n.log 2>&1
  local rc=$?
  if grep -qa "ALL TESTS PASSED" $SP/rdk_results/$n.log; then echo "PASS $n"
  elif grep -qaE "_SKIP|skipping|SKIPPED" $SP/rdk_results/$n.log && ! grep -qa "FAILED" $SP/rdk_results/$n.log; then echo "SKIP $n"
  else echo "FAIL $n (rc=$rc)"
  fi >> $SP/rdk_tally.txt
}
: > $SP/rdk_tally.txt
N=0
for t in $(cat $SP/rdk_tests.txt); do
  run_one $t &
  N=$((N+1))
  [ $((N % 5)) -eq 0 ] && wait
done
wait
sort $SP/rdk_tally.txt -k2 > $SP/rdk_tally_sorted.txt
awk '{c[$1]++} END{for(k in c) printf "%s=%d ", k, c[k]; print ""}' $SP/rdk_tally.txt
echo DRIVERDONE
