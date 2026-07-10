#!/usr/bin/env python3
# MQTT matching-cost curiosity: N non-matching wildcard subscribers registered,
# then publish QoS-1 to a non-matching topic and measure the rate. QoS 1 gives
# broker backpressure (each PUBLISH is acked), so the rate reflects the broker's
# per-message processing (topic matching), analogous to the Redis PUBLISH test.
# mosquitto uses a topic tree, so it should stay ~flat as N grows.
#   mqtt_bench.py <N>
import sys, time
import paho.mqtt.client as mqtt

N = int(sys.argv[1])
HOST, PORT = "127.0.0.1", 1883

def client():
    try:
        return mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    except Exception:
        return mqtt.Client()

# Subscriber: N non-matching single-level-wildcard filters zzz/<i>/+
sub = client()
sub.connect(HOST, PORT)
sub.loop_start()
if N > 0:
    filters = [(f"zzz/{i}/+", 0) for i in range(N)]
    for j in range(0, N, 500):
        sub.subscribe(filters[j:j + 500])
    time.sleep(1.5)  # let subscriptions register on the broker

# Publisher: QoS 1, bounded inflight so paho blocks -> rate == broker ack rate
pub = client()
pub.max_inflight_messages_set(1000)
pub.connect(HOST, PORT)
pub.loop_start()

M = 100000
t0 = time.time()
for i in range(M):
    info = pub.publish("aaa/b", "x", qos=1)
    info.wait_for_publish(timeout=5)  # blocks on the inflight window
dt = time.time() - t0
pub.loop_stop(); sub.loop_stop()
print(f"N={N:<7} {M/dt:,.0f} pub/s")
