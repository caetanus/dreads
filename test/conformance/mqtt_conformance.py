#!/usr/bin/env python3
# Broker-observable MQTT conformance for the dreads MQTT skin, via paho.
# Drives the pub/sub delivery contract: QoS 0/1/2, retained, wildcards.
#
#   mqtt_conformance.py <mqtt_port>   e.g. ... 1883
#   (start dreads first: ./bin/dreads --port 7300 --mqtt-port 1883 --shards 4)
import sys, time, threading
import paho.mqtt.client as mqtt

PORT = int(sys.argv[1]) if len(sys.argv) > 1 else 1883
passed = 0
failed = 0
fails = []


def check(name, cond, detail=""):
    global passed, failed
    if cond:
        print("  PASS  %-52s %s" % (name, detail)); passed += 1
    else:
        print("  FAIL  %-52s %s" % (name, detail)); failed += 1; fails.append(name)


def new_client(cid):
    # paho 2.x requires an explicit callback API version; 1.x doesn't have it.
    try:
        return mqtt.Client(mqtt.CallbackAPIVersion.VERSION1, client_id=cid)
    except (AttributeError, TypeError):
        return mqtt.Client(client_id=cid)


class Sink:
    def __init__(self):
        self.msgs = []
        self.lock = threading.Lock()

    def on_message(self, client, userdata, msg):
        with self.lock:
            self.msgs.append((msg.topic, msg.payload, msg.qos, msg.retain))

    def wait(self, n, timeout=5.0):
        end = time.time() + timeout
        while time.time() < end:
            with self.lock:
                if len(self.msgs) >= n:
                    return True
            time.sleep(0.02)
        return False


def connect(cid):
    c = new_client(cid)
    sink = Sink()
    c.on_message = sink.on_message
    c.connect("127.0.0.1", PORT, keepalive=30)
    c.loop_start()
    return c, sink


# --- [PUBSUB QoS] one subscriber, each QoS level ---------------------------
print("\n[PUBSUB QoS]")
sub, sink = connect("conf-sub")
pub, _ = connect("conf-pub")
for qos in (0, 1, 2):
    sink.msgs.clear()
    sub.subscribe("conf/q%d" % qos, qos=qos)
    time.sleep(0.3)  # let SUBSCRIBE settle
    pub.publish("conf/q%d" % qos, b"payload", qos=qos)
    ok = sink.wait(1)
    got = sink.msgs[0] if sink.msgs else (None, None, None, None)
    check("QoS %d publish is delivered" % qos, ok and got[1] == b"payload",
          "qos=%s" % (got[2],))

# --- [WILDCARD] # and + filters --------------------------------------------
print("\n[WILDCARD]")
sink.msgs.clear()
sub.subscribe("sensors/#", qos=1)
time.sleep(0.3)
pub.publish("sensors/room1/temp", b"21.5", qos=1)
pub.publish("sensors/room2/hum", b"55", qos=1)
check("multi-level '#' wildcard matches", sink.wait(2) and len(sink.msgs) >= 2,
      "n=%d" % len(sink.msgs))
sink.msgs.clear()
sub.subscribe("plus/+/x", qos=0)
time.sleep(0.3)
pub.publish("plus/a/x", b"hit", qos=0)
pub.publish("plus/a/b/x", b"miss", qos=0)  # too deep for a single '+'
time.sleep(0.6)
topics = [m[0] for m in sink.msgs]
check("single-level '+' wildcard matches one level only",
      "plus/a/x" in topics and "plus/a/b/x" not in topics, "topics=%s" % topics)

# --- [RETAINED] a late subscriber still gets the last retained message ------
print("\n[RETAINED]")
pub.publish("conf/retained", b"kept", qos=1, retain=True)
time.sleep(0.3)
late, lsink = connect("conf-late")
late.subscribe("conf/retained", qos=1)
ok = lsink.wait(1)
check("retained message replayed to a new subscriber",
      ok and lsink.msgs and lsink.msgs[0][1] == b"kept" and lsink.msgs[0][3],
      "retainflag=%s" % (lsink.msgs[0][3] if lsink.msgs else None))
# clear it
pub.publish("conf/retained", b"", qos=1, retain=True)
time.sleep(0.3)
late2, lsink2 = connect("conf-late2")
late2.subscribe("conf/retained", qos=1)
lsink2.wait(1, timeout=1.0)
check("zero-length retained publish clears the retention", not lsink2.msgs)

for c in (sub, pub, late, late2):
    c.loop_stop(); c.disconnect()

print("\n" + "=" * 60)
print("MQTT conformance: %d passed, %d failed" % (passed, failed))
if failed:
    print("FAILED:", ", ".join(fails)); sys.exit(1)
print("PASS: dreads satisfies the observable MQTT contract")
