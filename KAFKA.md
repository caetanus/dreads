# Kafka

dreads speaks the **Kafka wire protocol** as a native face over the sharded core.
It is the skin whose model maps onto ours most naturally:

> A Kafka **partition** is the Redis list at `kafka.t.<topic>.<p>`, on the shard
> that owns its slot. **Produce is an `RPUSH`** (the offset is the list position,
> assigned atomically by `RPUSH`'s length reply on the owner); **Fetch is an
> `LRANGE`**; the high watermark is `LLEN`; earliest/latest come from the list
> bounds. Durability is the per-shard AOF — no Kafka-specific persistence code.

So partitions spread across shards exactly like keys spread across a Redis
Cluster, and a topic's log survives `kill -9` and replays on boot for free.

> Stated honestly. This page tells you what works, what the conformance suites
> cover, and where it differs from a real Kafka cluster. **When in doubt, the
> tests are the source of truth** — `test/conformance/kafka_golib_conformance.py`
> and the librdkafka harness, not the prose.

## Enabling it

| directive | flag | default | meaning |
|---|---|---|---|
| `kafka-port N` | `--kafka-port=N` | `0` (off) | Kafka listener, e.g. `9092` |
| `kafka-tls-port N` | `--kafka-tls-port=N` | `0` (off) | Kafka over TLS |
| `kafka-db N` | `--kafka-db=N` | `18` | logical DB the partition keyspace lives in |
| `kafka-require-sasl yes` | `--kafka-require-sasl=yes` | `no` | every data/admin API needs SASL first |
| `kafka-super-users ...` | `--kafka-super-users=...` | — | `User:admin,...` — bypass ACL enforcement |
| `appendonly yes` | `--appendonly=yes` | `no` | persist the log to the AOF |

```sh
dreads --port=6379 --kafka-port=9092 --appendonly=yes --dir=/var/lib/dreads
```

```python
from kafka import KafkaProducer, KafkaConsumer
KafkaProducer(bootstrap_servers="localhost:9092").send("orders", b"hello")
for msg in KafkaConsumer("orders", bootstrap_servers="localhost:9092",
                         group_id="g1", auto_offset_reset="earliest"):
    print(msg.offset, msg.value)
```

## What is supported

**Core data plane** — `Produce` (acks 0/1 — the write is applied on the owner
before the response), `Fetch` (inclusive-start, fail-closed past the end — no
wrap), `ListOffsets` (earliest / latest / by-timestamp), `Metadata`,
`ApiVersions`. Records use the v2 `RecordBatch` format.

**Consumer groups** — the classic rebalance protocol: `FindCoordinator`,
`JoinGroup`, `SyncGroup`, `Heartbeat`, `LeaveGroup`, `OffsetCommit`,
`OffsetFetch`, `DescribeGroups` / `ListGroups` / `DeleteGroups`, `OffsetDelete`.
Group state (members, generation, assignments, committed offsets) lives on the
coordinator shard. This drives the golib **Consumer** conformance.

**Idempotent producer (KIP-98)** — `InitProducerId` assigns a producer id +
epoch; per-`(producer, partition)` sequence numbers are admitted in order
(`OUT_OF_ORDER_SEQUENCE` on a gap, duplicate-batch detection on a retry).

**Transactions (KIP-98)** — a single-node transaction coordinator:
`AddPartitionsToTxn`, `AddOffsetsToTxn`, `TxnOffsetCommit`, `EndTxn`
(commit/abort). This drives the golib **Transaction** conformance.

**Admin** — `CreateTopics`, `DeleteTopics`, `CreatePartitions`, `DeleteRecords`,
`DescribeConfigs` / `AlterConfigs` / `IncrementalAlterConfigs`, and the ACL admin
trio `DescribeAcls` / `CreateAcls` / `DeleteAcls`.

**Security** — SASL `PLAIN` and `SCRAM-SHA-256` / `SCRAM-SHA-512`
(`SaslHandshake` + `SaslAuthenticate`) against the ACL user set;
`kafka-require-sasl` makes auth mandatory; ACL enforcement (deny > allow >
default-deny) gates every API, with `kafka-super-users` as the bypass. TLS via
`kafka-tls-port`.

**Topics** are stateless by default: a Metadata request for an un-created topic
reports it as existing with `KAFKA_PARTITIONS` (4) partitions, so a producer
never has to pre-create. `CreateTopics` pins an explicit partition count.

**Conformance:** the golib broker-observable suite passes **14/14**; the
librdkafka integration harness passes **160/166** (the six are structural — see
below).

## Where it differs from a real Kafka cluster

The wire protocol — request/response framing, error codes, record batches, the
group and transaction state machines — targets Apache Kafka, with librdkafka and
golib as the yardsticks. The differences are architectural.

### Divergent by design — these will stay

- **One broker is advertised.** Metadata returns a single broker (node 0) and
  every partition's leader is that broker. A smart client therefore sends every
  partition's traffic to the one node, and the cross-**shard** routing happens
  **inside** the node over the SPSC hop — invisibly, the way Redis Cluster
  internally redirects. (Multi-**node** distribution is the RESP cluster proxy;
  exposing per-partition leaders across nodes in Kafka Metadata is a separate,
  unshipped step.)
- **The client picks the partition; we only set the count.** Kafka's partitioner
  lives in the client (Java `murmur2`, librdkafka `crc32` — they disagree), so a
  broker cannot and does not dictate the key→partition hash. dreads controls the
  partition **count** and the partition→shard placement; the message key never
  enters *our* routing (see the partitioning note in the smart-client design).
- **Replication is Raft, not ISR/KRaft.** Redundancy comes from dreads' Raft log
  (`--raft`), not Kafka's replication factor / in-sync-replica set. `acks=all`,
  `min.insync.replicas`, and KRaft controller metadata have no Kafka-native
  equivalent here — the committed log is the durability story, and confirm
  durability is everysec (a hard crash forfeits the sub-second AOF tail; a clean
  shutdown loses nothing).
- **No log compaction, no time/size retention, no truncation.** A partition is an
  append-only list: earliest offset stays 0 unless you `DeleteRecords`; there is
  no `cleanup.policy=compact` and no automatic segment aging.

### Not done yet — gaps we will close

- **Classic consumer groups only — no KIP-848.** The next-generation
  (broker-side-assignment) consumer group protocol is out of scope; clients use
  the classic client-side-assignment rebalance.
- **The six librdkafka structural failures** — cluster-shaped expectations
  (multi-broker metadata, controller/coordinator topology details) that follow
  from the single-broker model above, not data-plane bugs.

## See also

- [SHARDING.md](SHARDING.md) — the slot model that places partitions on shards.
- [AMQP.md](AMQP.md) — the other queue-shaped skin, same list-is-the-log idea.
- `source/dreads/kafka.d` + `source/dreads/kafkagroup.d` — the implementation;
  `test/conformance/kafka_golib_conformance.py` — the observable-behavior suite.
