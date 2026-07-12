#!/usr/bin/env bash
# MQTT fan-out + wildcard benchmark against a broker, for cross-protocol
# comparison with the RESP pub/sub numbers. Uses the eclipse-mosquitto C tools
# (mosquitto_pub/sub) so the client is C — comparable to redis-benchmark.
#
# Assumes a mosquitto broker container named "mosq" pinned to BROKER_CORE:
#   docker run -d --name mosq --cpuset-cpus 1 -p 1883:1883 \
#     -v /tmp/mosq/mosquitto.conf:/mosquitto/config/mosquitto.conf eclipse-mosquitto
#   (mosquitto.conf: "listener 1883\nallow_anonymous true\nmax_queued_messages 0")
#
# Usage: bench/mqtt-bench.sh [nmsgs] [nsubs]
set -u
NMSGS="${1:-1000000}"; NSUBS="${2:-50}"
IMG=eclipse-mosquitto
run() { docker run --rm --network host "$@"; }  # clients on host network -> localhost:1883

echo "=== MQTT (mosquitto) — QoS 0, $NSUBS subs, $NMSGS msgs ==="

# fan-out: NSUBS subscribers on one topic, timed publish
docker rm -f mqsubs >/dev/null 2>&1
docker run -d --name mqsubs --cpuset-cpus 8-11 --network host "$IMG" \
  sh -c "for i in \$(seq $NSUBS); do mosquitto_sub -h localhost -t ch >/dev/null 2>&1 & done; wait" >/dev/null
sleep 3
base0=$(date +%s.%N); run --cpuset-cpus 4-7 "$IMG" true; base1=$(date +%s.%N)
base=$(echo "$base1 - $base0" | bc)
p0=$(date +%s.%N)
run --cpuset-cpus 4-7 "$IMG" sh -c "yes hi | head -$NMSGS | mosquitto_pub -l -h localhost -t ch"
p1=$(date +%s.%N)
pub=$(echo "($p1 - $p0) - $base" | bc)
echo "PUBLISH x${NSUBS}subs = $(echo "scale=0; $NMSGS / $pub" | bc)/s  (${pub}s minus ${base}s startup)"
docker rm -f mqsubs >/dev/null 2>&1

# wildcard scaling: NPAT distinct '+'-wildcard subscriptions, publish to a non-match
for npat in 100 1000; do
  docker rm -f mqpat >/dev/null 2>&1
  docker run -d --name mqpat --cpuset-cpus 8-11 --network host "$IMG" \
    sh -c "for i in \$(seq 0 $((npat-1))); do mosquitto_sub -h localhost -t \"p\$i/+\" >/dev/null 2>&1 & done; wait" >/dev/null
  sleep 3
  q0=$(date +%s.%N)
  run --cpuset-cpus 4-7 "$IMG" sh -c "yes hi | head -$NMSGS | mosquitto_pub -l -h localhost -t zzz/x"
  q1=$(date +%s.%N)
  qt=$(echo "($q1 - $q0) - $base" | bc)
  echo "wildcard pN/+ x$npat (nomatch) = $(echo "scale=0; $NMSGS / $qt" | bc)/s"
  docker rm -f mqpat >/dev/null 2>&1
done
