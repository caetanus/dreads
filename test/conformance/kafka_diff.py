#!/usr/bin/env python3
# Differential (oracle) conformance test for the dreads Kafka skin: run identical
# kafka-python operations against BOTH dreads and a reference Apache Kafka, and
# diff the client-observable results. Apache Kafka is the oracle — any mismatch
# on the core per-partition protocol semantics (offset assignment, ordering,
# content, ListOffsets, fetch-past-end) is a real conformance bug.
#
#   kafka_diff.py <dreads_bootstrap> <apache_bootstrap>
#   e.g. kafka_diff.py 127.0.0.1:19092 127.0.0.1:9092
#
# SETUP TRAP: the apache/kafka:3.7.0 container binds BOTH 9092 AND 9093 on
# 127.0.0.1 (specific). The kernel routes loopback to the more-specific listen,
# so a dreads bound to *:9092 is shadowed and clients silently hit Apache. Run
# dreads on a port Apache does NOT use (e.g. --kafka-port 19092) for this diff.
#
# We produce all messages to partition 0 explicitly: dreads is stateless and
# advertises KAFKA_PARTITIONS=4 per topic while Apache auto-creates with 1, so
# the default partitioner spreads messages differently (NOT a bug, a documented
# design drift). Pinning partition 0 isolates the offset/ordering semantics that
# MUST match. See DRIFT: dreads 4 partitions vs Apache default num.partitions.
import sys, time
from kafka import KafkaProducer, KafkaConsumer
from kafka.structs import TopicPartition

AV = (0, 10, 1)  # the pre-flexible dialect dreads speaks (0.10.x wire)
N = 20


def run(bs, topic):
    r = {}
    p = KafkaProducer(bootstrap_servers=bs, api_version=AV)
    r['base_offsets'] = [p.send(topic, ('msg-%02d' % i).encode(), partition=0).get(timeout=10).offset
                         for i in range(N)]
    p.flush(); p.close()
    tp = TopicPartition(topic, 0)
    c = KafkaConsumer(bootstrap_servers=bs, api_version=AV, consumer_timeout_ms=4000,
                      enable_auto_commit=False)
    c.assign([tp]); c.seek_to_beginning(tp)
    r['consumed'] = [(m.offset, m.value.decode()) for m in c]
    c.close()
    c2 = KafkaConsumer(bootstrap_servers=bs, api_version=AV)
    c2.assign([tp])
    r['beginning'] = c2.beginning_offsets([tp])[tp]
    r['end'] = c2.end_offsets([tp])[tp]
    c2.seek(tp, r['end'] + 5)                       # past the end
    r['past_end_empty'] = not c2.poll(timeout_ms=1500)
    c2.close()
    return r


def main():
    if len(sys.argv) != 3:
        print("usage: kafka_diff.py <dreads_bootstrap> <apache_bootstrap>")
        return 2
    dreads_bs, apache_bs = sys.argv[1], sys.argv[2]
    topic = 'conf_%d' % int(time.time())
    A = run(apache_bs, topic)
    D = run(dreads_bs, topic)
    checks = [
        ("base_offsets", A['base_offsets'], D['base_offsets']),
        ("consumed content+order", [v for _, v in A['consumed']], [v for _, v in D['consumed']]),
        ("consumed offsets", [o for o, _ in A['consumed']], [o for o, _ in D['consumed']]),
        ("listoffsets (beginning,end)", (A['beginning'], A['end']), (D['beginning'], D['end'])),
        ("fetch-past-end empty", A['past_end_empty'], D['past_end_empty']),
    ]
    ok = True
    for name, a, d in checks:
        match = a == d
        ok = ok and match
        print("  %-28s %s" % (name, "MATCH" if match else "*** DIVERGE ***  apache=%r dreads=%r" % (a, d)))
    print("PASS: dreads Kafka conforms to Apache on per-partition semantics" if ok
          else "FAIL: conformance divergence (see above)")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
