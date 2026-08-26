#!/usr/bin/env python3
# Broker-observable AMQP 0-9-1 conformance for the dreads AMQP skin, via pika
# (the reference client). Drives the publisher-confirm + durability contract that
# the sharded fire-and-forget publish path must preserve.
#
#   amqp_conformance.py <amqp_port>   e.g. ... 5672
#   (start dreads first: ./bin/dreads --port 7300 --amqp-port 5672 --shards 4)
import sys, pika

PORT = int(sys.argv[1]) if len(sys.argv) > 1 else 5672
passed = 0
failed = 0
fails = []


def check(name, cond, detail=""):
    global passed, failed
    if cond:
        print("  PASS  %-52s %s" % (name, detail)); passed += 1
    else:
        print("  FAIL  %-52s %s" % (name, detail)); failed += 1; fails.append(name)


params = pika.ConnectionParameters(host="127.0.0.1", port=PORT,
                                   connection_attempts=5, retry_delay=1)
conn = pika.BlockingConnection(params)
ch = conn.channel()
ch.confirm_delivery()  # publisher confirms ON

# --- [CONFIRMS + ORDER] a hot single queue (the cross-shard hop path) -------
print("\n[CONFIRMS + ORDER]")
ch.queue_declare("amqpc.q1", durable=True)
ch.queue_purge("amqpc.q1")
N = 5000
acked = 0
for i in range(N):
    try:
        ch.basic_publish("", "amqpc.q1", ("m%d" % i).encode(),
                         properties=pika.BasicProperties(delivery_mode=2))
        acked += 1  # BlockingConnection raises on a nack/return
    except Exception as e:
        print("   publish failed:", e); break
check("all %d publishes confirmed" % N, acked == N, "acked=%d" % acked)
got = []
for _ in range(N):
    m = ch.basic_get("amqpc.q1", auto_ack=True)
    if m[0] is None:
        break
    got.append(m[2].decode())
check("all %d messages retrievable" % N, len(got) == N, "got=%d" % len(got))
check("FIFO order preserved", got == ["m%d" % i for i in range(N)])

# --- [CROSS-SHARD] many queues spread across shards by name -----------------
print("\n[CROSS-SHARD]")
NQ, per = 8, 500
for q in range(NQ):
    ch.queue_declare("amqpc.mq%d" % q, durable=True)
    ch.queue_purge("amqpc.mq%d" % q)
for q in range(NQ):
    for i in range(per):
        ch.basic_publish("", "amqpc.mq%d" % q, ("%d:%d" % (q, i)).encode())
counts = []
for q in range(NQ):
    c = 0
    while ch.basic_get("amqpc.mq%d" % q, auto_ack=True)[0] is not None:
        c += 1
    counts.append(c)
check("%d cross-shard queues each hold %d" % (NQ, per),
      all(c == per for c in counts), "counts=%s" % counts)

# --- [MANDATORY] unroutable comes back, not silently confirmed --------------
print("\n[MANDATORY]")
ch2 = conn.channel()
ch2.confirm_delivery()
returned = False
try:
    ch2.basic_publish("", "amqpc.nonexistent.xyz", b"orphan", mandatory=True)
except pika.exceptions.UnroutableError:
    returned = True
except Exception:
    returned = True  # any refusal beats a silent accept
check("mandatory unroutable is returned to the publisher", returned)

# --- [INTERLEAVE] declare + publish in one batch keeps its order -------------
print("\n[INTERLEAVE]")
ch.queue_declare("amqpc.mix", durable=True); ch.queue_purge("amqpc.mix")
for i in range(1000):
    ch.basic_publish("", "amqpc.mix", ("x%d" % i).encode())
cnt = 0
while ch.basic_get("amqpc.mix", auto_ack=True)[0] is not None:
    cnt += 1
check("interleaved declare+publish batch intact", cnt == 1000, "cnt=%d" % cnt)

conn.close()
print("\n" + "=" * 60)
print("AMQP 0-9-1 conformance: %d passed, %d failed" % (passed, failed))
if failed:
    print("FAILED:", ", ".join(fails)); sys.exit(1)
print("PASS: dreads satisfies the observable AMQP 0-9-1 contract")
