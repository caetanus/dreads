# dreads vs the incumbents — four protocols (2026-09-02, Ryzen 3950X)

Every protocol is driven by ONE client binary against every broker, so the
comparison is the server and nothing else. Server pinned to physical cores 0-7,
clients to 8-15 + their siblings 24-31. 16-byte messages throughout.

`--shards N` is a dreads knob; the incumbents have no equivalent, so they appear
once. Where a competitor has a threading knob (valkey's io-threads) it is given
the same core budget dreads gets.

## RESP — redis-benchmark -P16 -c32 --threads 4, 2M ops

| | SET | GET |
|---|---:|---:|
| dreads s1 | 1 140 901 | 1 141 552 |
| dreads s2 | 1 996 008 | 1 996 008 |
| dreads s4 | 2 656 042 | 3 992 016 |
| **dreads s8** | **3 984 064** | **3 992 016** |
| valkey 9.1 (io-threads 8) | 1 141 552 | — |
| valkey 9.1 (default) | 725 689 | 887 705 |
| dragonfly (8 cores) | 1 330 672 | 1 598 721 |

dreads s8 is 3.5x valkey at the same core budget and 3.0x dragonfly. Note the
ceiling: 8 and 16 client threads both land at ~3.96M, so s8 may be reading
redis-benchmark rather than the server.

## AMQP 0-9-1 — bench/amqpload, 8 queues, consume-only drain

| | manual ack | delivery only |
|---|---:|---:|
| **dreads s1** | **2 986 706** | 5 125 670 |
| dreads s2 | 2 014 520 | 5 406 838 |
| dreads s4 | 1 640 434 | **9 001 618** |
| dreads s8 | 1 245 158 | 7 981 720 |
| LavinMQ 2.9 | 1 524 606 | 2 030 083 |
| RabbitMQ 3.13 | 263 189 | 386 908 |

Best dreads figure against each: 2.0x LavinMQ and 11.3x RabbitMQ with acks,
4.4x and 23x on delivery. The two columns scale in OPPOSITE directions —
delivery improves with shards, the ack path degrades past one — which is the
open item documented in AMQP-CONSUME-2026-09-01.md.

LavinMQ reads 1.52M here against the 550k an earlier campaign measured with
RabbitMQ PerfTest. PerfTest was the bottleneck in those runs, not the broker.

## MQTT — bench/mqttload, 8 topics, QoS-1 publishers, sustained-rate subscribers

| | msg/s |
|---:|---:|
| **dreads s1** | **3 679 606 / 3 725 817 / 4 480 447** |
| dreads s2 | 477 913 / 173 955 |
| dreads s4 | 439 646 |
| dreads s8 | 135 849 |
| mosquitto 2 | 118 038 |

dreads at one shard is ~31x mosquitto. **Past one shard, fan-out collapses by
roughly an order of magnitude, and it reproduces.** Delivery stays CORRECT — a
paho probe at s1 and s2 receives 200/200 on all 8 topics — so this is a
throughput pathology in the cross-shard fan-out, not lost messages. Unexplained;
the largest open perf item in the tree.

## Kafka — measured with KAFKA'S OWN producer

Kafka is designed around a SMART CLIENT: the producer accumulates records
asynchronously and ships large batches (batch.size/linger.ms), and the broker is
built to expect that. A hand-rolled producer measures the tool, not the broker.
bench/kafkaload sends 32 records per Produce request; the Java producer with
stock settings batches roughly 1000 records of this size. Benchmarking Kafka
with ours understated it by about the batching ratio.

kafka-producer-perf-test.sh (the reference client), 5M records of 16 bytes,
acks=1, one producer:

| | records/sec |
|---|---:|
| dreads s1 | 2 292 526 |
| dreads s2 | 2 248 201 |
| dreads s4 | 2 352 941 |
| dreads s8 | 2 275 831 |
| Apache Kafka 3.7 (KRaft) | 2 134 016 |

Both land in the same band, and dreads is flat across shard counts — the
signature of a client ceiling. Three concurrent producers confirm it:

| | records/sec, 3 producers |
|---|---:|
| dreads s4 | 3 647 741 |
| Apache Kafka 3.7 | 3 536 544 |

**A 3% difference. Kafka and dreads are a tie on produce**, and the measurement
is still client-bound for both. The earlier "8.2x Apache Kafka" in this file was
an artifact of the naive producer and is retracted.

For the record, the same suspicion was checked against RabbitMQ, and there it
does NOT hold: RabbitMQ driven by its own PerfTest reaches 128 842 msg/s (8
queues, 20s, mixed) — LOWER than the 263 189 it reached with bench/amqpload. Our
client did not handicap it.

## Reading these honestly

- **One shard is the best configuration for AMQP acks and MQTT.** Only RESP and
  AMQP delivery-only scale up with shards; Kafka produce is flat because the
  client saturates first. That is a real limitation, not a footnote.
- **Use the vendor's own client where the protocol has one.** Kafka's throughput
  lives in client-side batching, and measuring it with a hand-rolled producer
  produced an 8x claim that a fair test turned into a 3% tie. AMQP and MQTT have
  no equivalent client-side accumulation, and the RabbitMQ cross-check above
  confirms our client did not disadvantage it — but the question has to be asked
  per protocol, not assumed.
- Competitors run their own default tuning apart from valkey's io-threads. A
  tuned RabbitMQ or Kafka would do better than shown; these are stock images.
- Kafka here is a single broker with acks=1 and no replication — its usual
  deployment shape is different, and its durability guarantees are not dreads'.
- The RESP s8 figure is at the client's ceiling and should be read as a lower
  bound.

## Kafka partition routing (kafka-shard-ports) — the fair comparison

The Kafka table above is UNFAIR TO DREADS and is superseded here. It gave Apache
Kafka a routing client and all of its internal parallelism while measuring dreads
at one shard (one core) and at four shards WITHOUT partition routing, where every
partition a shard does not own costs a cross-shard hop. Neither is dreads doing
the thing the Kafka protocol was designed for.

Re-measured with an EQUAL CORE BUDGET and a load generator that is not the
ceiling: both servers pinned to physical cores 0-3, six kafka-producer-perf-test
producers on 4-15 + 20-31 (three times the server's budget, and avoiding the
servers' own SMT siblings), 1.5M x 16-byte records each, acks=1.

| | records/sec (3 runs) | median |
|---|---|---:|
| dreads s4, routing OFF | 3 332 908 / 3 231 031 / 3 142 617 | 3 231 031 |
| **dreads s4, routing ON** | 4 089 190 / 4 120 030 / 3 993 021 | **4 089 190** |
| Apache Kafka 3.7 (KRaft) | 2 585 997 / 2 687 868 / 2 496 995 | 2 585 997 |

**Routing is worth +27%**, and with it dreads is **1.58x Apache Kafka** at the
same core budget under the same smart client. No run of any group overlaps
another.

And it makes Kafka produce SCALE with shards for the first time (4 cores,
routing on):

| shards | records/sec | speedup |
|---|---:|---:|
| s1 | 2 924 247 | 1.00 |
| s2 | 3 592 647 | 1.23 |
| s4 | 4 086 940 | 1.40 |

Sublinear — 35% efficiency at four shards, not the linear the design aims for —
but monotonic and positive, against the FLAT curve that preceded it (s1 3.15M,
s4 3.03M, s8 3.25M). The likely remaining brake is the metadata that still
crosses shards: `kafka.tcfg.<topic>`, `kafka.topics` and `kafka.acls` are
process-global keys that hash to one shard, so every other shard reaches across
for them. The AMQP skin solved that class with a thread-local,
broadcast-replicated control plane; Kafka has no equivalent yet.

### What was wrong before, and why

An earlier note in this file said the feature "did not materialise". That was
measured with six JVM producers against a server on EIGHT cores, where the
client saturates near 3.8M and Apache Kafka itself tops out at 3.54M — a test
that cannot show the presence or the absence of a broker-side gain. Shrinking
both servers to four cores, so the load generator has room, is what made the
27% visible. Same error as the PerfTest episode earlier in this campaign:
concluding from a saturated client.
