# dreads vs the incumbents — four protocols (2026-09-02, Ryzen 3950X)

Every protocol is driven by ONE client binary against every broker, so what is
being compared is the server and nothing else. 16-byte messages throughout.

Two rules this file follows, both learned the hard way in the campaign that
produced it:

- **Equal core budget.** Where a competitor has a threading knob (valkey's
  io-threads) it gets the same cores dreads gets, and dreads is measured in the
  configuration it is meant to run in — not crippled to make a number.
- **Check the client is not the ceiling.** A saturated load generator cannot
  show the presence OR the absence of a broker-side difference. Read `cpu/wall`
  from `/proc/<pid>/stat`: below ~1.0 core per shard thread, the broker is not
  the limit. Every wrong conclusion in this campaign came from skipping this.

---

## RESP — redis-benchmark -P16 -c32 --threads 8, 2M ops, servers on cores 0-7

| | SET | GET |
|---|---:|---:|
| dreads s1 | 1 140 250 | 1 141 552 |
| **dreads s8** | **2 652 519** (range 2.65–3.97M) | **3 992 016** |
| valkey 9.1, io-threads 8 | 1 140 901 | 1 597 444 |
| valkey 9.1, default | 725 953 | 887 705 |
| dragonfly, 8 cores | 1 329 787 | 1 331 558 |

Median of three. dreads s8 is **2.3x valkey** at the same core budget and **2.0x
dragonfly** on SET. One shard ties valkey with eight io-threads.

s8 is bimodal across runs (2.65M and 3.97M on SET) because redis-benchmark is at
its own ceiling there — 8 and 16 client threads land at the same place — so the
median is reported with its range rather than the best sample.

## AMQP 0-9-1 — bench/amqpload, 8 queues, consume-only drain of a pre-filled backlog

| | manual ack | delivery only |
|---|---:|---:|
| **dreads s1** | **3 081 423** | 5 124 042 |
| dreads s2 | 2 765 047 | 5 985 359 |
| dreads s4 | 2 024 008 | 7 234 724 |
| dreads s8 | 2 170 278 | **8 352 099** |
| LavinMQ 2.9 | 1 490 336 | 2 221 933 |
| RabbitMQ 3.13 | 262 391 | 375 498 |

Best dreads figure against each: **2.1x LavinMQ** and **11.7x RabbitMQ** with
per-message acks; **3.8x** and **22x** on delivery alone.

The two columns scale in OPPOSITE directions — delivery improves with shards,
the ack path degrades past one. That is the open item in
AMQP-CONSUME-2026-09-01.md; **one shard is the right configuration for an
ack-heavy AMQP workload.**

RabbitMQ was cross-checked with its OWN reference client to make sure ours did
not handicap it: PerfTest reaches 128 842 msg/s (8 queues, 20s, mixed) — lower
than the 262 391 it reaches with bench/amqpload. AMQP has no client-side
accumulation to lose.

## MQTT — bench/mqttload, 8 topics, QoS-1 publishers, sustained-rate subscribers

| | msg/s |
|---|---:|
| **dreads s1** | **3 679 606 / 3 725 817 / 4 480 447** |
| dreads s2 | 477 913 / 173 955 |
| dreads s4 | 439 646 |
| dreads s8 | 135 849 |
| mosquitto 2 | 118 038 |

dreads at one shard is **~31x mosquitto**. Past one shard the fan-out collapses
by an order of magnitude, and it reproduces. Delivery stays CORRECT there — a
paho probe at s1 and s2 receives 200/200 on all 8 topics — so this is a
throughput pathology in the cross-shard fan-out, not lost messages. It is the
largest open performance item in the tree.

## Kafka — kafka-producer-perf-test.sh (the reference client), equal core budget

Kafka is designed around a SMART CLIENT that batches on the producer side and
routes each partition to the broker that leads it. Both halves matter:

- Benchmarking it with a hand-rolled producer measures the tool. bench/kafkaload
  sends 32 records per request where the Java producer batches ~1000, and an
  earlier revision of this file claimed 8x on that basis. Retracted.
- Hiding the topology from the client wastes the other half. dreads used to
  advertise a single broker leading every partition, so the client could not
  route and the broker paid a cross-shard hop for every partition a connection's
  shard did not own. `kafka-shard-ports yes` advertises one broker per shard with
  each partition led by its owning shard.

Both servers pinned to physical cores 0-3, six producers on 4-15 + 20-31 (three
times the server budget, avoiding the servers' SMT siblings), 1.5M x 16-byte
records each, acks=1:

| | records/sec |
|---|---:|
| dreads s4, routing off | 3 231 031 |
| dreads s4, routing on | 4 089 190 |
| **dreads s4, routing + tcfg cache** | **4 255 082** |
| Apache Kafka 3.7 (KRaft) | 2 726 648 |

**1.56x Apache Kafka** at the same core budget under the same client. Median of
three where repeated; no group overlapped another.

### The hop measured directly

Cross-shard keyspace executions over a 1M-record produce run at --shards 4,
bucketed by key prefix for the WHOLE run:

| | remote total | partition logs | topic config | other |
|---|---:|---:|---:|---:|
| routing off | 8 061 | 3 478 | 4 580 | 3 |
| routing on | 3 296 | **0** | 3 295 | 1 |
| routing on + tcfg cache | **6** | 0 | 3 | 3 |

With routing on the partition log NEVER crosses a shard: the client's routing is
exact. Everything left was `kafka.tcfg.<topic>` — a process-global key read by
the compaction gate on every Produce — and caching it per shard takes the total
from 3 296 to 6.

Shard scaling with both changes: s1 2.93M, s2 4.08M (1.39x), s4 4.15M (1.41x),
against the FLAT s1 3.15M / s4 3.03M / s8 3.25M that preceded them. Sublinear,
but what caps it now is no longer cross-shard traffic.

---

## Reading these honestly

- **Shards do not help every protocol.** One shard is the best configuration for
  ack-heavy AMQP and for MQTT. RESP, AMQP delivery and Kafka produce scale up.
  That is a real limitation, stated here rather than buried.
- **Use the vendor's own client where the protocol has one**, and ask the
  question per protocol rather than assuming. It turned an 8x Kafka claim into a
  1.56x one, and the RabbitMQ cross-check showed the same suspicion did not
  apply there.
- Competitors run stock images with default tuning apart from valkey's
  io-threads. A tuned RabbitMQ or Kafka would do better than shown.
- Kafka here is a single broker with acks=1 and no replication. Its usual
  deployment shape is different and its durability guarantees are not dreads'.
- RESP s8 and the Kafka rows are near their client's ceiling; read them as lower
  bounds.

## Reproducing

    ldc2 -O2 -release bench/amqpload.d -of amqpload      # AMQP
    ldc2 -O2 -release bench/mqttload.d -of mqttload      # MQTT
    ldc2 -O2 -release bench/kafkaload.d -of kafkaload    # Kafka (small-batch shape)

RESP uses redis-benchmark; Kafka's headline numbers use
kafka-producer-perf-test.sh from the apache/kafka image. PerfTest is fine for
AMQP CONFORMANCE but caps near 0.9M msg/s and must not be used for throughput.
