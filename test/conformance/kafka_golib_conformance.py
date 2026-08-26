#!/usr/bin/env python3
# Broker-observable Kafka conformance suite for dreads, derived from the
# faustbrian/golib Kafka conformance decision register (KAFKA-DEC-002/003/006):
#   https://github.com/faustbrian/golib/blob/main/pkg/kafka/docs/specification-decisions.md
#
# golib's suite tests a CLIENT library's contract; most of its rules are client
# policy (idempotence config, Retain byte-ownership, cooperative rebalance,
# consumer-group commit contiguity) that a broker cannot be tested for. This
# suite extracts ONLY the broker-observable invariants — the ones dreads must
# satisfy for a golib-style client to behave correctly — and asserts them
# directly against a running dreads, standalone (no reference broker needed).
#
#   kafka_golib_conformance.py <kafka_port>   e.g. ... 19092
#   (start dreads first: ./bin/dreads --port 7300 --kafka-port 19092)
#
# NOT covered here (require unimplemented feature milestones — see user rule:
# report, do not implement): consumer groups (assignment epoch, contiguous
# OffsetCommit/OffsetFetch, rebalance ownership), transactions, idempotent
# producer dedup, all-ISR acks. These are single-node-stateless / group features.
import sys, time
from kafka import KafkaProducer, KafkaConsumer
from kafka.structs import TopicPartition

AV = (0, 10, 1)
HOST = "127.0.0.1"

def producer(bs): return KafkaProducer(bootstrap_servers=bs, api_version=AV)
def consumer(bs): return KafkaConsumer(bootstrap_servers=bs, api_version=AV,
                                       consumer_timeout_ms=4000, enable_auto_commit=False)

def drain_partition(bs, topic, part, start=None):
    tp = TopicPartition(topic, part)
    c = consumer(bs); c.assign([tp])
    if start is None: c.seek_to_beginning(tp)
    else: c.seek(tp, start)
    out = [(m.offset, m.value.decode()) for m in c]
    c.close()
    return out

CHECKS = []
def check(dec, name):
    def deco(fn): CHECKS.append((dec, name, fn)); return fn
    return deco

# ---- KAFKA-DEC-002: producer delivery, partitioning, ordering ----
@check("KAFKA-DEC-002", "offset-assignment: produce returns the real fetch offset")
def c_offset_assign(bs, topic):
    p = producer(bs)
    sent = [p.send(topic, ("a%02d" % i).encode(), partition=0).get(timeout=10).offset for i in range(10)]
    p.flush(); p.close()
    got = [o for o, _ in drain_partition(bs, topic, 0)]
    assert sent == list(range(sent[0], sent[0] + 10)), "produce offsets not contiguous: %s" % sent
    assert got[:10] == sent, "fetch offsets %s != produce-returned %s" % (got[:10], sent)
    return "produce offsets %s == fetch offsets" % (sent,)

@check("KAFKA-DEC-002", "partition-local ordering: fetch order == produce order")
def c_order(bs, topic):
    p = producer(bs)
    vals = ["ord%03d" % i for i in range(30)]
    for v in vals: p.send(topic, v.encode(), partition=0).get(timeout=10)
    p.flush(); p.close()
    got = [v for _, v in drain_partition(bs, topic, 0)]
    assert got == vals, "order broken: %s..." % got[:5]
    return "30 records fetched in exact produce order"

@check("KAFKA-DEC-002", "monotonic contiguous offsets per partition (base=newLen-count)")
def c_monotonic(bs, topic):
    p = producer(bs)
    for i in range(25): p.send(topic, ("m%02d" % i).encode(), partition=0).get(timeout=10)
    p.flush(); p.close()
    offs = [o for o, _ in drain_partition(bs, topic, 0)]
    assert offs == list(range(offs[0], offs[0] + len(offs))), "gaps/non-monotonic: %s" % offs
    return "offsets %d..%d strictly contiguous ascending" % (offs[0], offs[-1])

@check("KAFKA-DEC-002", "explicit-partition production lands on the named partition only")
def c_explicit_part(bs, topic):
    p = producer(bs)
    # distinct payload per partition 0..3
    for part in range(4):
        for i in range(5): p.send(topic, ("p%d-%d" % (part, i)).encode(), partition=part).get(timeout=10)
    p.flush(); p.close()
    for part in range(4):
        vals = [v for _, v in drain_partition(bs, topic, part)]
        assert all(v.startswith("p%d-" % part) for v in vals), \
            "partition %d leaked foreign records: %s" % (part, vals)
        assert len(vals) == 5, "partition %d expected 5 got %d" % (part, len(vals))
    return "partitions 0-3 each hold only their own 5 records (no cross-partition leak)"

# ---- KAFKA-DEC-003: consumer fetch — broker-observable subset ----
@check("KAFKA-DEC-003", "partial fetch: one partition does not block another (multi-part fetch)")
def c_partial_fetch(bs, topic):
    p = producer(bs)
    for i in range(8): p.send(topic, ("q0-%d" % i).encode(), partition=0).get(timeout=10)
    for i in range(8): p.send(topic, ("q1-%d" % i).encode(), partition=1).get(timeout=10)
    p.flush(); p.close()
    tp0, tp1 = TopicPartition(topic, 0), TopicPartition(topic, 1)
    c = consumer(bs); c.assign([tp0, tp1]); c.seek_to_beginning(tp0); c.seek_to_beginning(tp1)
    by_part = {0: [], 1: []}
    for m in c: by_part[m.partition].append((m.offset, m.value.decode()))
    c.close()
    assert len(by_part[0]) == 8 and len(by_part[1]) == 8, \
        "multi-part fetch incomplete: p0=%d p1=%d" % (len(by_part[0]), len(by_part[1]))
    # per-partition sequential order preserved
    for part in (0, 1):
        offs = [o for o, _ in by_part[part]]
        assert offs == sorted(offs), "partition %d out of order in multi-fetch" % part
    return "both partitions fully + independently returned in one fetch, per-partition ordered"

# ---- KAFKA-DEC-006: exact direct-partition replay ----
@check("KAFKA-DEC-006", "replay inclusive-start: seek(K) returns first record AT offset K")
def c_replay_start(bs, topic):
    p = producer(bs)
    for i in range(20): p.send(topic, ("r%02d" % i).encode(), partition=0).get(timeout=10)
    p.flush(); p.close()
    got = drain_partition(bs, topic, 0, start=7)
    assert got and got[0][0] == 7, "inclusive-start broken: first offset %s != 7" % (got[0][0] if got else None)
    assert got[0][1] == "r07", "wrong record at offset 7: %s" % got[0][1]
    return "seek(7) -> first fetched offset is exactly 7 (r07)"

@check("KAFKA-DEC-006", "replay fail-closed: fetch past end returns empty (no wrap/skip)")
def c_replay_pastend(bs, topic):
    p = producer(bs)
    for i in range(20): p.send(topic, ("s%02d" % i).encode(), partition=0).get(timeout=10)
    p.flush(); p.close()
    tp = TopicPartition(topic, 0)
    c = consumer(bs); c.assign([tp]); end = c.end_offsets([tp])[tp]
    c.seek(tp, end + 5); got = c.poll(timeout_ms=1500); c.close()
    assert not got, "fetch past end (%d+5) returned data: %s" % (end, got)
    return "seek(end+5) -> empty (fail-closed, no wrap-around)"

@check("KAFKA-DEC-006", "replay partition-local ascending order from a mid-offset")
def c_replay_ascending(bs, topic):
    p = producer(bs)
    for i in range(20): p.send(topic, ("t%02d" % i).encode(), partition=0).get(timeout=10)
    p.flush(); p.close()
    got = drain_partition(bs, topic, 0, start=5)
    offs = [o for o, _ in got]
    assert offs == sorted(offs) and offs[0] == 5, "not ascending-from-5: %s" % offs[:6]
    return "replay from offset 5 strictly ascending %d..%d" % (offs[0], offs[-1])

@check("KAFKA-DEC-006", "replay is repeatable & non-mutating (assign/seek, no group)")
def c_replay_idempotent(bs, topic):
    p = producer(bs)
    for i in range(15): p.send(topic, ("u%02d" % i).encode(), partition=0).get(timeout=10)
    p.flush(); p.close()
    tp = TopicPartition(topic, 0)
    c = consumer(bs); begin0 = c.beginning_offsets([tp])[tp]; end0 = c.end_offsets([tp])[tp]; c.close()
    first = drain_partition(bs, topic, 0, start=0)
    second = drain_partition(bs, topic, 0, start=0)
    c2 = consumer(bs); begin1 = c2.beginning_offsets([tp])[tp]; end1 = c2.end_offsets([tp])[tp]; c2.close()
    assert first == second, "replay not repeatable"
    assert (begin0, end0) == (begin1, end1), "replay mutated log bounds: %s -> %s" % ((begin0, end0), (begin1, end1))
    return "two replays identical; log bounds unchanged (%d,%d) — no group mutation" % (begin1, end1)

# ---- INSPECTION (inspection.md): broker-observable Metadata/ListOffsets ----
# Uses RAW named Metadata v1 + ListOffsets v1 requests, not kafka-python's
# cluster cache: kafka-python's partitions_for_topic triggers an ALL-topics
# metadata refresh (empty topics array), which dreads answers empty BY DESIGN
# ("we answer only named topics" — kafka.d handleMetadata). Naming the topic is
# what a real produce/consume client does, and is what this suite must exercise.
import socket, struct

def _recv_frame(s):
    (n,) = struct.unpack(">i", s.recv(4)); b = b""
    while len(b) < n:
        c = s.recv(n - len(b))
        if not c: break
        b += c
    return b

class _P:
    def __init__(self, b): self.b = b; self.i = 0
    def i16(self): v = struct.unpack(">h", self.b[self.i:self.i+2])[0]; self.i += 2; return v
    def i32(self): v = struct.unpack(">i", self.b[self.i:self.i+4])[0]; self.i += 4; return v
    def i64(self): v = struct.unpack(">q", self.b[self.i:self.i+8])[0]; self.i += 8; return v
    def i8(self):  v = self.b[self.i]; self.i += 1; return v
    def st(self):
        n = self.i16()
        if n < 0: return None            # nullable string: advance 0 bytes
        v = self.b[self.i:self.i+n]; self.i += n; return v.decode()

def _client_hdr(api_key, api_ver, corr):
    cb = b"insp"
    return struct.pack(">hhi", api_key, api_ver, corr) + struct.pack(">h", len(cb)) + cb

def raw_metadata(bs, topic):
    host, port = bs.split(":"); port = int(port)
    body = _client_hdr(3, 1, 1)
    tb = topic.encode()
    body += struct.pack(">i", 1) + struct.pack(">h", len(tb)) + tb   # 1 NAMED topic
    s = socket.socket(); s.settimeout(3); s.connect((host, port))
    s.sendall(struct.pack(">i", len(body)) + body)
    p = _P(_recv_frame(s)); s.close()
    p.i32()                                                          # correlation id
    brokers = [(p.i32(), p.st(), p.i32(), p.st()) for _ in range(p.i32())]  # id,host,port,rack
    ctrl = p.i32()
    topics = []
    for _ in range(p.i32()):
        err = p.i16(); name = p.st(); p.i8()                        # err, name, is_internal
        parts = []
        for _ in range(p.i32()):
            pe = p.i16(); pid = p.i32(); leader = p.i32()
            [p.i32() for _ in range(p.i32())]                       # replicas
            [p.i32() for _ in range(p.i32())]                       # isr
            parts.append((pid, leader, pe))
        topics.append((err, name, parts))
    return brokers, ctrl, topics

def raw_listoffsets(bs, topic, part, ts):
    # ts: -2 = earliest, -1 = latest
    host, port = bs.split(":"); port = int(port)
    body = _client_hdr(2, 1, 1) + struct.pack(">i", -1)             # replica_id
    tb = topic.encode()
    body += struct.pack(">i", 1) + struct.pack(">h", len(tb)) + tb  # 1 topic
    body += struct.pack(">i", 1) + struct.pack(">i", part) + struct.pack(">q", ts)  # 1 partition
    s = socket.socket(); s.settimeout(3); s.connect((host, port))
    s.sendall(struct.pack(">i", len(body)) + body)
    p = _P(_recv_frame(s)); s.close()
    p.i32()                                                         # correlation id
    for _ in range(p.i32()):
        p.st()                                                      # topic name
        for _ in range(p.i32()):
            p.i32(); err = p.i16(); p.i64(); off = p.i64()         # partition, err, ts, offset
            return err, off
    return None, None

@check("INSPECTION", "topic existence: NAMED metadata for an uncreated topic exists (stateless)")
def c_topic_exists(bs, topic):
    _, _, topics = raw_metadata(bs, topic)  # topic never produced to
    assert topics, "uncreated topic absent from NAMED metadata (stateless model broken)"
    err, name, parts = topics[0]
    assert err == 0 and name == topic, "topic err=%s name=%s" % (err, name)
    assert len(parts) >= 1, "topic advertises 0 partitions"
    return "uncreated topic '%s' exists, err=0, %d partitions" % (name, len(parts))

@check("INSPECTION", "partition count stable + ids 0..n-1 across NAMED metadata requests")
def c_part_count_stable(bs, topic):
    _, _, t1 = raw_metadata(bs, topic)
    _, _, t2 = raw_metadata(bs, topic)
    ids1 = sorted(pid for pid, _, _ in t1[0][2])
    ids2 = sorted(pid for pid, _, _ in t2[0][2])
    assert ids1 == ids2, "partition ids unstable: %s != %s" % (ids1, ids2)
    assert ids1 == list(range(len(ids1))), "partition ids not 0..n-1: %s" % ids1
    return "%d partitions, ids 0..%d, stable across requests" % (len(ids1), len(ids1) - 1)

@check("INSPECTION", "every partition has a valid leader in the broker set")
def c_leaders_all(bs, topic):
    brokers, _, topics = raw_metadata(bs, topic)
    bids = {bid for bid, _, _, _ in brokers}
    parts = topics[0][2]
    missing = [pid for pid, leader, pe in parts if leader in (-1, None) or pe != 0]
    assert not missing, "partitions with no leader / error: %s (=> incomplete broker state)" % missing
    foreign = [(pid, leader) for pid, leader, _ in parts if leader not in bids]
    assert not foreign, "partitions whose leader is not an advertised broker: %s" % foreign
    return "all %d partitions err=0 with a leader in broker set %s" % (len(parts), sorted(bids))

@check("INSPECTION", "broker list present and self-consistent (host/port, no dup ids)")
def c_broker_list(bs, topic):
    brokers, ctrl, _ = raw_metadata(bs, topic)
    assert brokers, "empty broker list in metadata"
    ids = [bid for bid, _, _, _ in brokers]
    assert len(ids) == len(set(ids)), "duplicate broker node ids: %s" % ids
    bad = [(bid, h, pt) for bid, h, pt, _ in brokers if not h or not (0 < pt < 65536)]
    assert not bad, "brokers with invalid host/port: %s" % bad
    assert ctrl in set(ids), "controller_id %s not an advertised broker %s" % (ctrl, ids)
    return "%d broker(s), unique ids %s, controller=%s, valid host:port" % (len(brokers), sorted(ids), ctrl)

@check("INSPECTION", "offset queryability: every partition answers ListOffsets earliest+latest")
def c_offset_queryable(bs, topic):
    _, _, topics = raw_metadata(bs, topic)
    parts = [pid for pid, _, _ in topics[0][2]]
    for pid in parts:
        e_err, earliest = raw_listoffsets(bs, topic, pid, -2)
        l_err, latest = raw_listoffsets(bs, topic, pid, -1)
        assert e_err == 0 and l_err == 0, "partition %d ListOffsets err (earliest=%s latest=%s)" % (pid, e_err, l_err)
        assert earliest is not None and latest is not None and earliest <= latest, \
            "partition %d bad offsets earliest=%s latest=%s" % (pid, earliest, latest)
    return "all %d partitions answer ListOffsets earliest<=latest" % len(parts)

def main():
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 19092
    bs = "%s:%d" % (HOST, port)
    ts = int(time.time())
    passed = failed = 0
    last_dec = None
    for i, (dec, name, fn) in enumerate(CHECKS):
        if dec != last_dec:
            print("\n[%s]" % dec); last_dec = dec
        topic = "golib_%d_%d" % (ts, i)
        try:
            detail = fn(bs, topic)
            print("  PASS  %-58s %s" % (name, detail)); passed += 1
        except AssertionError as e:
            print("  FAIL  %-58s %s" % (name, e)); failed += 1
        except Exception as e:
            print("  ERROR %-58s %s: %s" % (name, type(e).__name__, e)); failed += 1
    print("\n" + "=" * 60)
    print("golib-derived conformance: %d passed, %d failed" % (passed, failed))
    print("PASS: dreads satisfies all broker-observable golib invariants" if failed == 0
          else "*** FAIL: broker-observable conformance divergence ***")
    return 0 if failed == 0 else 1

if __name__ == "__main__":
    sys.exit(main())
