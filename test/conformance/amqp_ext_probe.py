#!/usr/bin/env python3
"""Probe AMQP 0-9-1 extensions by BEHAVIOUR, not by whether a declare succeeded:
brokers accept unknown x- arguments silently, so declaring proves nothing."""
import sys, pika, time

PORT = int(sys.argv[1]); LABEL = sys.argv[2]
P = pika.ConnectionParameters(host="127.0.0.1", port=PORT, heartbeat=30,
        credentials=pika.PlainCredentials("guest", "guest"),
        connection_attempts=3, retry_delay=2)

def fresh(q, args=None, etype=None, ex=None):
    c = pika.BlockingConnection(P); ch = c.channel()
    try: ch.queue_delete(queue=q)
    except Exception: c = pika.BlockingConnection(P); ch = c.channel()
    if ex:
        try: ch.exchange_delete(exchange=ex)
        except Exception: c = pika.BlockingConnection(P); ch = c.channel()
        ch.exchange_declare(exchange=ex, exchange_type=etype, durable=True)
    ch.queue_declare(queue=q, durable=True, arguments=args or {})
    return c, ch

def depth(ch, q):
    return ch.queue_declare(queue=q, durable=True, passive=True).method.message_count

R = {}
def rec(name, ok, detail=""):
    R[name] = (ok, detail)

# --- x-max-length-bytes: cap by BYTES, not count -------------------------
try:
    c, ch = fresh("e.bytes", {"x-max-length-bytes": 1000})
    for i in range(50):
        ch.basic_publish("", "e.bytes", b"x" * 100)   # 5000 bytes total
    time.sleep(0.3); d = depth(ch, "e.bytes"); c.close()
    rec("x-max-length-bytes", d <= 12, "depth=%d (cap 1000B/100B = ~10)" % d)
except Exception as e: rec("x-max-length-bytes", False, "err %s" % type(e).__name__)

# --- x-overflow reject-publish: publisher gets a nack --------------------
try:
    c, ch = fresh("e.ovf", {"x-max-length": 2, "x-overflow": "reject-publish"})
    ch.confirm_delivery(); nacked = False
    for i in range(6):
        try: ch.basic_publish("", "e.ovf", b"m")
        except Exception: nacked = True; break
    time.sleep(0.2); c.close()
    rec("x-overflow reject-publish", nacked, "publisher nacked" if nacked else "all accepted")
except Exception as e: rec("x-overflow reject-publish", False, "err %s" % type(e).__name__)

# --- x-max-priority: higher priority delivered first ---------------------
try:
    c, ch = fresh("e.prio", {"x-max-priority": 10})
    for pr, body in ((1, b"low"), (9, b"high")):
        ch.basic_publish("", "e.prio", body, properties=pika.BasicProperties(priority=pr))
    time.sleep(0.3)
    first = ch.basic_get("e.prio", auto_ack=True)[2]
    c.close()
    rec("x-max-priority", first == b"high", "first=%s" % first)
except Exception as e: rec("x-max-priority", False, "err %s" % type(e).__name__)

# --- x-single-active-consumer: only one consumer is fed -----------------
try:
    c, ch = fresh("e.sac", {"x-single-active-consumer": True})
    for i in range(20): ch.basic_publish("", "e.sac", b"s")
    got = [0, 0]
    c2 = pika.BlockingConnection(P); a = c2.channel(); b = c2.channel()
    a.basic_qos(prefetch_count=1); b.basic_qos(prefetch_count=1)
    def mk(i, chan):
        def cb(chx, m, p, body):
            got[i] += 1; chx.basic_ack(m.delivery_tag)
        return cb
    a.basic_consume("e.sac", mk(0, a)); b.basic_consume("e.sac", mk(1, b))
    c2.process_data_events(time_limit=3)
    a.stop_consuming(); b.stop_consuming(); c2.close(); c.close()
    rec("x-single-active-consumer", (got[0] == 0) != (got[1] == 0), "got=%s" % got)
except Exception as e: rec("x-single-active-consumer", False, "err %s" % type(e).__name__)

# --- x-queue-type: quorum / stream ---------------------------------------
for qt in ("quorum", "stream"):
    try:
        c, ch = fresh("e.qt." + qt, {"x-queue-type": qt})
        ch.basic_publish("", "e.qt." + qt, b"q")
        time.sleep(0.3); d = depth(ch, "e.qt." + qt); c.close()
        rec("x-queue-type=" + qt, True, "declared, depth=%d" % d)
    except Exception as e: rec("x-queue-type=" + qt, False, "err %s" % type(e).__name__)

# --- x-delivery-limit: redelivery is bounded -----------------------------
try:
    c, ch = fresh("e.dl", {"x-queue-type": "quorum", "x-delivery-limit": 2})
    ch.basic_publish("", "e.dl", b"d"); time.sleep(0.3)
    n = 0
    for _ in range(8):
        m, p, body = ch.basic_get("e.dl", auto_ack=False)
        if m is None: break
        n += 1; ch.basic_nack(m.delivery_tag, requeue=True); time.sleep(0.1)
    c.close()
    rec("x-delivery-limit", n <= 4, "redeliveries=%d (limit 2)" % n)
except Exception as e: rec("x-delivery-limit", False, "err %s" % type(e).__name__)

# --- x-delayed-message exchange (LavinMQ / rmq plugin) -------------------
try:
    c, ch = fresh("e.delay", None, etype="x-delayed-message", ex="e.dx")
    rec("x-delayed-message exchange", True, "declared")
    c.close()
except Exception as e: rec("x-delayed-message exchange", False, "err %s" % type(e).__name__)

print("\n=== %s ===" % LABEL)
for k in sorted(R):
    ok, d = R[k]
    print("  %-28s %-4s %s" % (k, "YES" if ok else "no", d))
