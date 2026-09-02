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

## Kafka — bench/kafkaload, 4 topics, produce acks=1

| | produce msg/s |
|---:|---:|
| **dreads s1** | **4 033 964** |
| dreads s2 | 2 673 941 |
| dreads s4 | 3 109 904 |
| dreads s8 | 2 417 916 |
| Apache Kafka 3.7 (KRaft) | 494 586 |

dreads s1 is 8.2x Apache Kafka. Shard scaling is flat-to-negative here too.

## Reading these honestly

- **One shard is the best configuration for AMQP acks, MQTT and Kafka.** Only
  RESP and AMQP delivery-only scale up with shards. That is a real limitation,
  not a footnote.
- Competitors run their own default tuning apart from valkey's io-threads. A
  tuned RabbitMQ or Kafka would do better than shown; these are stock images.
- Kafka's 494k is a single broker with acks=1 and no replication — its usual
  deployment shape is different, and its durability guarantees are not dreads'.
- The RESP s8 figure is at the client's ceiling and should be read as a lower
  bound.
