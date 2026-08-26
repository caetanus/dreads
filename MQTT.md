# MQTT

dreads speaks **MQTT 3.1.1 and MQTT 5.0** as a native protocol face over the same
sharded core that serves RESP — the first of the non-RESP skins, and the plainest
statement of the "one ring" thesis: same process, same threads, same
share-nothing fabric, a different wire out front.

MQTT is **publish/subscribe**, not queuing — so unlike [AMQP](AMQP.md) it does not
map a topic to a keyspace list. A subscription lives in a per-shard topic trie; a
`PUBLISH` fans out to matching subscribers across shards over the SPSC fabric.
What *is* persisted lands in the keyspace: retained messages and persistent-session
state.

> Stated honestly, like the rest of dreads. This page tells you what works, what
> is an extension, and where it differs from a classic broker (mosquitto / EMQX /
> HiveMQ).

## Enabling it

| directive | flag | default | meaning |
|---|---|---|---|
| `mqtt-port N` | `--mqtt-port=N` | `0` (off) | plaintext MQTT listener, e.g. `1883` |
| `mqtt-tls-port N` | `--mqtt-tls-port=N` | `0` (off) | MQTT over TLS (`mqtts`), e.g. `8883` |
| `mqtt-ws-port N` | `--mqtt-ws-port=N` | `0` (off) | MQTT over WebSocket, e.g. `8083` |
| `mqtt-wss-port N` | `--mqtt-wss-port=N` | `0` (off) | MQTT over WebSocket over TLS (`wss`), e.g. `8084` |
| `mqtt-db N` | `--mqtt-db=N` | `17` | logical DB for retained + session state |
| `mqtt-sys-bytes yes` | `--mqtt-sys-bytes=yes` | `no` | publish `$SYS/broker/bytes-{received,sent}` counters |

```sh
dreads --port=6379 --mqtt-port=1883 --mqtt-ws-port=8083 --dir=/var/lib/dreads
```

Any MQTT client connects unchanged — `mosquitto_pub`/`_sub`, Paho (the reference
suite dreads is tested against), MQTT.js over WebSocket:

```sh
mosquitto_sub -h localhost -p 1883 -t 'sensors/#' -q 1 &
mosquitto_pub -h localhost -p 1883 -t sensors/room1/temp -q 1 -m '21.5'
```

## What is supported

**Protocol** — both **3.1.1** (protocol level 4) and **5.0** (level 5) on the same
port; the CONNECT protocol level selects the dialect (v5 packets carry a property
block and use reason codes on CONNACK/SUBACK/etc.).

**QoS 0 / 1 / 2**, both directions:

- **Outbound** delivery at `min(publishQoS, subscriptionGrant)`. QoS 1 tracks an
  unacked packet id; QoS 2 runs the full PUBLISH → PUBREC → PUBREL → PUBCOMP
  handshake with packet-id dedup.
- **Inbound** QoS 1 (PUBACK on receipt) and QoS 2 (PUBREC/PUBREL/PUBCOMP with
  dedup).

**Retained messages** — published with the retain flag, replayed to a new
subscriber at SUBSCRIBE time, cleared by a zero-length retained publish. Broadcast-
replicated to every shard (rare writes, local reads).

**Wildcards** — `+` (single level) and `#` (multi-level) topic filters.

**Last Will and Testament** — published when a connection drops abnormally,
cleared by a clean DISCONNECT; v5 **will-delay-interval** is honored (the will
fires after the delay, and a reconnect within the window cancels it) and v5 will
properties (content-type, user-properties, ...) survive onto the will PUBLISH.

**Keepalive** — enforced at 1.5× the negotiated interval; a dead TCP peer is also
caught by the read loop.

**Sessions** — clean-start (`clean_session` / v5 `clean-start`) **and persistent
sessions**: with a non-zero session-expiry the session is held offline on
disconnect, and a reconnect resumes it (**CONNACK session-present = 1**) —
subscriptions are restored and the unacked QoS 1/2 window is retransmitted.
Client-id **takeover** ([MQTT-3.1.4-2]) is coordinated across shards.

**MQTT 5 features:**

- **Properties** on every packet, **reason codes** on CONNACK / SUBACK / PUBACK /
  DISCONNECT.
- **Shared subscriptions** — `$share/<group>/<filter>`: one member of the group
  receives each message (round-robin). *(Cross-shard caveat below.)*
- **Subscription identifiers** — echoed on every matching PUBLISH.
- **Topic aliases** — inbound and outbound.
- **Flow control** — v5 **receive-maximum** in both directions; QoS 1/2 deliveries
  that exceed the peer's window are held in FIFO order (not dropped).
- **Maximum-packet-size** — the client's advertised limit is respected.
- **Subscription options** — no-local, retain-as-published, retain-handling.
- **Session-expiry-interval** and **message-expiry-interval**.

**Transports** — TCP, TLS (`mqtt-tls-port`), and MQTT-over-WebSocket / `wss`
(`mqtt-ws-port` / `mqtt-wss-port`), with the RFC 6455 framing handled in-process.

**`$SYS`** — opt-in `$SYS/broker/bytes-received` and `bytes-sent` counters
(`mqtt-sys-bytes yes`).

## Sharding and performance

A connection is served on the shard that accepted it; a `PUBLISH` delivers to that
shard's local subscriber trie and fans out to the other shards over the SPSC
fabric (`ShardMsg.mqttPub`), gated by a global subscriber count so an idle skin
costs nothing.

Delivery never blocks the loop: each connection owns a **writer fiber** draining a
double-buffered outbox; publisher fibers and the cross-shard fan-in only append +
signal, so a slow subscriber blocks nothing but itself (its QoS 0 overflow is
dropped, spec-legal; QoS 1/2 flow-controls).

Throughput is **fan-out-bound, not connection-bound** — a single busy publisher
saturates the path, so MQTT does not scale with connection count the way RESP
does (measured ~4.3M msg/s at `--shards 8`, roughly flat from 1 connection
upward). Against mosquitto on the same box (~74K msg/s, `tcp_nodelay` on) that is
~58×.

## Where it differs from a classic broker

On the wire — CONNECT/CONNACK, the QoS handshakes, property tables, reason codes —
dreads targets the spec, with the **Paho** conformance suite as the yardstick. The
differences below are the exceptions.

### Divergent by design — these will stay

- **Fan-out is fire-and-forget, share-nothing.** A message is delivered by fanning
  out across shards, not stored in a per-topic log. Only *retained* messages and
  *persistent-session* unacked windows are persisted; a QoS 0 message to a slow or
  absent subscriber is dropped (spec-legal), not spooled.
- **Overlapping subscriptions may deliver duplicates.** A client subscribed to two
  filters that both match a topic can receive the message twice — the 3.1.1 spec
  explicitly permits this.

### Not done yet — gaps we will close

- **Cross-shard shared subscriptions can double-deliver.** ⚠️ The one real
  correctness gap: a `$share/<group>` whose members are spread across **different
  shards** gets **one delivery per shard-with-members** (a duplicate, never a
  loss). A group whose members all live on **one shard** is exact round-robin.
  Exactly-once across shards needs replicated per-(group, filter, shard) member
  counts + a global sequence — a deliberate, un-shipped design. **If you rely on
  shared-subscription load-balancing, keep a group's members on one shard (or one
  `--shards 1` node) until this lands.**
- **No enhanced authentication (`AUTH` packet).** A v5 client that starts an
  `AUTH` exchange is closed; only username/password (against the ACL) is honored.
- **`PUBACK`/`PUBREC` never report `0x10` (no matching subscribers).** An inbound
  publish is always acked with the success short form — the fire-and-forget
  cross-shard fan-out can't cheaply prove a *global* zero-match.
- **A 3.1.1 client's QoS 1/2 overflow degrades to QoS 0.** Without v5 flow
  control, once a 3.1.1 client's bounded in-flight window is full, further QoS 1/2
  deliveries drop to QoS 0 rather than blocking. (v5 clients get real
  receive-maximum flow control instead.)

## See also

- [PUBSUB.md](PUBSUB.md) — the RESP pub/sub engine MQTT's fan-out shares DNA with.
- [SHARDING.md](SHARDING.md) — the per-shard tries and the SPSC fan-out fabric.
- `source/dreads/mqtt.d` — the implementation and the authoritative scope.
