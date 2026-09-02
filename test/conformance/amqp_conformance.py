#!/usr/bin/env python3
# Broker-observable AMQP 0-9-1 conformance for the dreads AMQP skin, via pika
# (the reference client). Drives the publisher-confirm + durability contract that
# the sharded fire-and-forget publish path must preserve.
#
#   amqp_conformance.py <amqp_port>   e.g. ... 5672
#   (start dreads first: ./bin/dreads --port 7300 --amqp-port 5672 --shards 4)
import sys, time, pika

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

# --- [QOS DELIVERY] a consumer that sets basic.qos still gets its messages ---
# Regression: batching the consumer pop made the burst break on a full prefetch
# window with burst > 0, which skipped the queue-dry flush. Staged deliveries sat
# in the write buffer until it reached its byte cap -- which it never did,
# because the window only reopens when the client acks what is still buffered.
# Every consumer that set basic.qos received NOTHING. PerfTest leaves prefetch
# unlimited, so no existing suite covered it.
print("\n[QOS DELIVERY]")
for pf in (1, 5, 50):
    qn = "amqpc.qos%d" % pf
    c2 = pika.BlockingConnection(params); k2 = c2.channel()
    k2.queue_delete(queue=qn); k2.queue_declare(queue=qn, durable=True)
    for i in range(120):
        k2.basic_publish("", qn, b"q%03d" % i)
    c2.close()
    c2 = pika.BlockingConnection(params); k2 = c2.channel()
    k2.basic_qos(prefetch_count=pf)
    got = 0
    for m, _p, _b in k2.consume(qn, inactivity_timeout=4):
        if m is None:
            break
        k2.basic_ack(m.delivery_tag); got += 1
        if got >= 120:
            break
    k2.cancel(); c2.close()
    check("basic.qos prefetch=%d delivers (no buffered-delivery stall)" % pf,
          got == 120, "received=%d/120" % got)

# --- [BATCH TAIL] a consumer that stops mid-batch keeps nothing --------------
# The consumer prefetches a batch off the list, so between the pop and the
# delivery its fiber is the only holder. Cancelling with a tight window leaves
# the rest of that batch held by nobody once the fiber ends: off the queue,
# never delivered, never requeued.
print("\n[BATCH TAIL]")
qn = "amqpc.batchtail"
c2 = pika.BlockingConnection(params); k2 = c2.channel()
k2.queue_delete(queue=qn); k2.queue_declare(queue=qn, durable=True)
for i in range(200):
    k2.basic_publish("", qn, b"b%03d" % i)
c2.close()
c2 = pika.BlockingConnection(params); k2 = c2.channel()
k2.basic_qos(prefetch_count=1)
took = 0
for m, _p, _b in k2.consume(qn, inactivity_timeout=4):
    if m is None:
        break
    k2.basic_ack(m.delivery_tag); took += 1
    break
k2.cancel(); c2.close()
c2 = pika.BlockingConnection(params); k2 = c2.channel()
seen = []
while True:
    m, _p, b = k2.basic_get(qn, auto_ack=True)
    if m is None:
        break
    seen.append(b.decode())
c2.close()
check("cancelling mid-batch loses no message",
      len(seen) == 200 - took, "left=%d expected=%d" % (len(seen), 200 - took))
check("the returned batch tail keeps queue order",
      seen == sorted(seen), "first=%s" % seen[:4])

# --- [X-OVERFLOW] a full queue can refuse instead of dropping its head -------
# RabbitMQ 4 and LavinMQ both enforce x-overflow; the AMQP default (drop-head)
# silently evicts, which is the wrong answer for a client that would rather be
# told. reject-publish refuses the message AND nacks a confirming publisher.
print("\n[X-OVERFLOW]")
qn = "amqpc.ovf"
c2 = pika.BlockingConnection(params); k2 = c2.channel()
try: k2.queue_delete(queue=qn)
except Exception: c2 = pika.BlockingConnection(params); k2 = c2.channel()
k2.queue_declare(queue=qn, durable=True,
                 arguments={"x-max-length": 2, "x-overflow": "reject-publish"})
k2.confirm_delivery()
nacked = False
try:
    for i in range(8):
        k2.basic_publish("", qn, b"o%d" % i)
except Exception:
    nacked = True
check("x-overflow reject-publish nacks past the bound", nacked)
c2 = pika.BlockingConnection(params); k2 = c2.channel()
d = k2.queue_declare(queue=qn, durable=True, passive=True).method.message_count
check("reject-publish keeps the HEAD, not the tail", d <= 2, "depth=%d (bound 2)" % d)
first = k2.basic_get(qn, auto_ack=True)[2]
check("the retained head is the FIRST message", first == b"o0", "first=%s" % first)
c2.close()

# drop-head (the default) still evicts rather than refusing
qn = "amqpc.ovfdh"
c2 = pika.BlockingConnection(params); k2 = c2.channel()
try: k2.queue_delete(queue=qn)
except Exception: c2 = pika.BlockingConnection(params); k2 = c2.channel()
k2.queue_declare(queue=qn, durable=True, arguments={"x-max-length": 2})
k2.confirm_delivery()
dropped_ok = True
try:
    for i in range(8):
        k2.basic_publish("", qn, b"d%d" % i)
except Exception:
    dropped_ok = False
check("default overflow still drop-head (accepts, evicts)", dropped_ok)
c2 = pika.BlockingConnection(params); k2 = c2.channel()
last = k2.basic_get("amqpc.ovfdh", auto_ack=True)[2]
check("drop-head keeps the TAIL", last == b"d6", "first-out=%s" % last)
c2.close()

# --- [X-DELIVERY-LIMIT] a poison message stops being requeued ---------------
# Without a limit a consumer that keeps nacking spins the same record forever.
# LavinMQ enforces this; measured there, a limit of 2 yields 3 deliveries and
# then the message is dead-lettered.
print("\n[X-DELIVERY-LIMIT]")
c2 = pika.BlockingConnection(params); k2 = c2.channel()
for q in ("amqpc.dlsrc", "amqpc.dldead"):
    try: k2.queue_delete(queue=q)
    except Exception:
        c2 = pika.BlockingConnection(params); k2 = c2.channel()
k2.queue_declare(queue="amqpc.dldead", durable=True)
k2.queue_declare(queue="amqpc.dlsrc", durable=True,
                 arguments={"x-delivery-limit": 2, "x-dead-letter-exchange": "",
                            "x-dead-letter-routing-key": "amqpc.dldead"})
k2.basic_publish("", "amqpc.dlsrc", b"poison")
n = 0
for _ in range(10):
    m, _p, _b = k2.basic_get("amqpc.dlsrc", auto_ack=False)
    if m is None:
        break
    n += 1
    k2.basic_nack(m.delivery_tag, requeue=True)
left = k2.queue_declare(queue="amqpc.dlsrc", durable=True, passive=True).method.message_count
dead = k2.queue_declare(queue="amqpc.dldead", durable=True, passive=True).method.message_count
check("x-delivery-limit bounds redelivery", n <= 4, "deliveries=%d (limit 2)" % n)
check("the message leaves the source queue", left == 0, "left=%d" % left)
check("and is dead-lettered, not dropped", dead == 1, "dead=%d" % dead)

# a queue WITHOUT the limit still requeues without bound
try: k2.queue_delete(queue="amqpc.dlnone")
except Exception:
    c2 = pika.BlockingConnection(params); k2 = c2.channel()
k2.queue_declare(queue="amqpc.dlnone", durable=True)
k2.basic_publish("", "amqpc.dlnone", b"forever")
n2 = 0
for _ in range(6):
    m, _p, _b = k2.basic_get("amqpc.dlnone", auto_ack=False)
    if m is None:
        break
    n2 += 1
    k2.basic_nack(m.delivery_tag, requeue=True)
check("no limit still requeues unbounded", n2 == 6, "deliveries=%d" % n2)
c2.close()

# --- [X-MAX-PRIORITY] higher priority is served first ------------------------
# Backed by one list per level (level 0 is the queue's own key, so a plain FIFO
# queue is byte-identical to what it was). Every read, requeue, purge, delete
# and depth has to span the levels or a priority queue silently loses messages.
print("\n[X-MAX-PRIORITY]")
c2 = pika.BlockingConnection(params); k2 = c2.channel()
def _fresh(q, args=None):
    global c2, k2
    try: k2.queue_delete(queue=q)
    except Exception:
        c2 = pika.BlockingConnection(params); k2 = c2.channel()
    k2.queue_declare(queue=q, durable=True, arguments=args or {})
PRIOS = [(1, b"p1"), (5, b"p5"), (0, b"p0"), (3, b"p3"), (5, b"p5b")]
_fresh("amqpc.prio", {"x-max-priority": 5})
for pr, body in PRIOS:
    k2.basic_publish("", "amqpc.prio", body, properties=pika.BasicProperties(priority=pr))
time.sleep(0.4)
d = k2.queue_declare(queue="amqpc.prio", durable=True, passive=True).method.message_count
check("depth counts every priority level", d == 5, "depth=%d of 5" % d)
got = []
while True:
    m, _p, b = k2.basic_get("amqpc.prio", auto_ack=True)
    if m is None:
        break
    got.append(b.decode())
check("basic.get serves highest priority first", got == ["p5", "p5b", "p3", "p1", "p0"],
      "order=%s" % got)

_fresh("amqpc.prio2", {"x-max-priority": 5})
for pr, body in PRIOS:
    k2.basic_publish("", "amqpc.prio2", body, properties=pika.BasicProperties(priority=pr))
time.sleep(0.4)
seen = []
for m, _p, b in k2.consume("amqpc.prio2", inactivity_timeout=4):
    if m is None:
        break
    seen.append(b.decode()); k2.basic_ack(m.delivery_tag)
    if len(seen) >= 5:
        break
k2.cancel()
check("a consumer serves highest priority first", seen == ["p5", "p5b", "p3", "p1", "p0"],
      "order=%s" % seen)

_fresh("amqpc.prio3", {"x-max-priority": 5})
k2.basic_publish("", "amqpc.prio3", b"hi", properties=pika.BasicProperties(priority=4))
k2.basic_publish("", "amqpc.prio3", b"lo", properties=pika.BasicProperties(priority=0))
time.sleep(0.3)
m, _p, b = k2.basic_get("amqpc.prio3", auto_ack=False)
k2.basic_nack(m.delivery_tag, requeue=True); time.sleep(0.3)
m2, _p2, b2 = k2.basic_get("amqpc.prio3", auto_ack=True)
check("requeue puts a message back at ITS level", b == b"hi" and b2 == b"hi",
      "first=%s after-requeue=%s" % (b, b2))

_fresh("amqpc.prio4", {"x-max-priority": 5})
for pr, body in PRIOS:
    k2.basic_publish("", "amqpc.prio4", body, properties=pika.BasicProperties(priority=pr))
time.sleep(0.3)
pk = k2.queue_purge("amqpc.prio4")
left = k2.queue_declare(queue="amqpc.prio4", durable=True, passive=True).method.message_count
check("purge empties every level", left == 0, "left=%d" % left)
c2.close()

conn.close()
print("\n" + "=" * 60)
print("AMQP 0-9-1 conformance: %d passed, %d failed" % (passed, failed))
if failed:
    print("FAILED:", ", ".join(fails)); sys.exit(1)
print("PASS: dreads satisfies the observable AMQP 0-9-1 contract")
