#!/usr/bin/env bash
# Run the full conformance battery: dub unit tests + every skin's observable
# suite against ONE live dreads. A suite whose Python client isn't installed is
# SKIPPED, not failed, so this runs in a bare environment and degrades cleanly.
#
#   test/run-conformance.sh            # build + unit tests + all skin suites
#   test/run-conformance.sh --no-build # skip the dub build
#   test/run-conformance.sh --no-unit  # skip dub test (unit)
#   test/run-conformance.sh --quick    # skip both build and unit tests
#
# Exit non-zero if any suite (or the unit tests) failed.
set -u
cd "$(dirname "$0")/.." || exit 2

DO_BUILD=1; DO_UNIT=1
for a in "$@"; do case "$a" in
  --no-build) DO_BUILD=0 ;;
  --no-unit)  DO_UNIT=0 ;;
  --quick)    DO_BUILD=0; DO_UNIT=0 ;;
esac; done

RESP=7300; AMQP=5672; MQTT=1883; KAFKA=9092; SQS=9324
DIR="$(mktemp -d)"
CONF="test/conformance"
declare -A RESULT   # suite -> PASS/FAIL/SKIP
FAILED=0

say() { printf '\n\033[1;36m== %s\033[0m\n' "$*"; }
have() { python3 -c "import $1" >/dev/null 2>&1; }

cleanup() { pkill -9 -x dreads 2>/dev/null; rm -rf "$DIR"; }
trap cleanup EXIT

pkill -9 -x dreads 2>/dev/null; sleep 0.5

# --- build ------------------------------------------------------------------
if [ "$DO_BUILD" = 1 ]; then
  say "dub build"
  dub build 2>&1 | tail -3 || { echo "BUILD FAILED"; exit 2; }
fi
[ -x ./bin/dreads ] || { echo "no ./bin/dreads — build first"; exit 2; }

# --- unit tests -------------------------------------------------------------
if [ "$DO_UNIT" = 1 ]; then
  say "dub test (unit, DMD)"
  if dub test 2>&1 | tail -3 | grep -q "0 failed"; then
    RESULT[unit]=PASS
  else
    RESULT[unit]=FAIL; FAILED=1
  fi
fi

# --- boot one dreads with every skin ----------------------------------------
say "booting dreads (shards=4, all skins) at $DIR"
./bin/dreads --port=$RESP --shards=4 --appendonly=yes \
  --amqp-port=$AMQP --mqtt-port=$MQTT --kafka-port=$KAFKA --sqs-port=$SQS \
  --dir="$DIR" >"$DIR/dreads.log" 2>&1 &
SRV=$!
for i in $(seq 1 20); do
  redis-cli -p $RESP ping >/dev/null 2>&1 && break; sleep 0.3
done
if ! redis-cli -p $RESP ping >/dev/null 2>&1; then
  echo "dreads did not come up"; tail -5 "$DIR/dreads.log"; exit 2
fi

# --- run one Python suite, gated on its client lib --------------------------
# args: suite-name  import-module  script  port
run_suite() {
  local name=$1 mod=$2 script=$3 port=$4
  say "$name conformance ($script)"
  if [ -n "$mod" ] && ! have "$mod"; then
    echo "  (python '$mod' not installed — SKIP)"; RESULT[$name]=SKIP; return
  fi
  if timeout 120 python3 "$CONF/$script" "$port"; then
    RESULT[$name]=PASS
  else
    RESULT[$name]=FAIL; FAILED=1
  fi
}

run_suite SQS   ""                    sqs_conformance.py        $SQS
run_suite AMQP  pika                  amqp_conformance.py       $AMQP
run_suite MQTT  paho.mqtt.client      mqtt_conformance.py       $MQTT
run_suite Kafka kafka                 kafka_golib_conformance.py $KAFKA

kill -0 $SRV 2>/dev/null && [ "$(grep -c FATAL "$DIR/dreads.log")" = 0 ] \
  && SRVOK=PASS || SRVOK=FAIL
[ "$SRVOK" = FAIL ] && FAILED=1

# --- summary ----------------------------------------------------------------
say "SUMMARY"
for k in unit SQS AMQP MQTT Kafka; do
  [ -n "${RESULT[$k]:-}" ] && printf '  %-6s %s\n' "$k" "${RESULT[$k]}"
done
printf '  %-6s %s (broker alive + no FATAL after all suites)\n' "server" "$SRVOK"
echo
if [ "$FAILED" = 0 ]; then
  echo "ALL CONFORMANCE PASSED"
else
  echo "CONFORMANCE FAILURES ABOVE"
fi
exit $FAILED
