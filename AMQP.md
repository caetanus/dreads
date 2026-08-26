# AMQP 0-9-1

dreads speaks **AMQP 0-9-1** — the RabbitMQ dialect — as a native protocol face
over the same sharded core that serves RESP. It is not a bridge or a shim: a
queue **is** a Redis list, an exchange is routing metadata, and every published
message flows through the exact data plane a `RPUSH` uses. One engine, many
faces.

> **Stated honestly, like the rest of dreads.** This page tells you what works,
> what is an extension, and what is deliberately not here yet. The v1 scope line
> lives in the source (`source/dreads/amqp.d`); this document expands it.

## The model: a queue is a list

The structural bet is simple and it pays for everything else:

> **An AMQP queue named `q` is the Redis list at key `amq.q.<q>`, on the shard
> that owns `keyToSlot("amq.q.<q>")`.**

Consequences you get for free:

- **`basic.publish` is an `RPUSH`**; `basic.get` / `basic.consume` are `LPOP` /
  `LRANGE`. The queue depth is `LLEN`.
- **Durability is the per-shard AOF** — the same log that persists your RESP
  writes persists your queue. Survives `kill -9`; replayed on boot; re-sharded
  automatically when `--shards N` changes.
- **Same storage engine, dedicated DB.** The queue is a real list object built
  and persisted by the exact keyspace machinery a RESP `LPUSH` uses — it lives in
  logical DB **16** (`amq-db`), one of three skin slots that sit **above** the
  RESP-addressable range (`databases` caps `SELECT` at 0–15), so skin state never
  collides with — or is reachable by — a user keyspace. It is the same engine, not
  a Redis-visible key.
- **Distributes like Redis Cluster.** The queue lives at its CRC16 slot, so
  queues spread across shards by name — identical to how a key spreads. A
  producer served by another shard reaches the owner over the internal SPSC hop
  (see [Sharding](#sharding-and-performance)).

Exchange and binding metadata (declarations, bindings, dead-letter/TTL/max-length
policy) is **thread-local per shard, replicated by broadcast** over the SPSC
fabric — declares are control-plane-rare, so every shard holds a full copy and
routing is a local lookup with no hop.

## Enabling it

Config directives (redis.conf-style) or `--flags`:

| directive | flag | default | meaning |
|---|---|---|---|
| `amqp-port N` | `--amqp-port=N` | `0` (off) | plaintext AMQP listener, e.g. `5672` |
| `amqp-tls-port N` | `--amqp-tls-port=N` | `0` (off) | AMQP-over-TLS (`amqps`) |
| `amqp-db N` | `--amqp-db=N` | `16` | logical DB the queue keyspace lives in |
| `appendonly yes` | `--appendonly=yes` | `no` | persist queues to the AOF |
| `management-port N` | `--management-port=N` | `0` (off) | RabbitMQ management HTTP API (RMQ uses `15672`) |

```sh
dreads --port=6379 --amqp-port=5672 --appendonly=yes --dir=/var/lib/dreads
```

A standard client connects with no special options:

```python
import pika
conn = pika.BlockingConnection(pika.ConnectionParameters("localhost", 5672))
ch = conn.channel()
ch.queue_declare("orders", durable=True)
ch.basic_publish(exchange="", routing_key="orders", body=b"hello")
```

## What is supported

**Exchanges** — `direct`, `fanout`, `topic` (AMQP topic wildcards `*` and `#` on
dot-segments), plus the default (`""`) exchange (route by queue name) and the
seeded `amq.*` exchanges. Alternate-exchange (`alternate-exchange` arg) cascades
an unroutable message to a fallback exchange.

**Methods** — `connection.*` / `channel.*` open+close, `exchange.declare`,
`queue.declare` (durable / exclusive / auto-delete / passive), `queue.bind`,
`basic.publish`, `basic.get`, `basic.consume` / `basic.cancel`, `basic.ack` /
`basic.nack` / `basic.reject`, `confirm.select` (publisher confirms), `tx.select`
/ `tx.commit` / `tx.rollback`.

**Publisher confirms** (`confirm.select`) — every publish is acked with a
`basic.ack` (or returned via `basic.return` for a mandatory unroutable one). See
[Durability](#durability-and-publisher-confirms) for exactly what the ack
promises.

**Consumers** — `basic.consume` and `basic.get` with **auto-ack** semantics. A
consumer receives `basic.deliver` frames as messages arrive. Explicit
`basic.ack`/`nack`/`reject` are accepted; a `nack`/`reject` with `requeue=true`
puts the message back at the **front** of the queue.

**RabbitMQ extensions that work:**

- **Dead-lettering** — `x-dead-letter-exchange` / `x-dead-letter-routing-key`. A
  message that expires (TTL), is rejected, or overflows `x-max-length` is
  re-published to the DLX. An `x-death` hop counter bounds pathological
  A→X→A dead-letter loops.
- **Message TTL** — `x-message-ttl` (per-queue), expiring messages to the DLX (or
  dropping them if none).
- **Queue length limit** — `x-max-length` (drop-head overflow to the DLX).
- **Mandatory publish** — an unroutable `mandatory` message comes back as
  `basic.return` (`312 NO_ROUTE`) instead of vanishing; in confirm mode it is
  still acked (the broker took responsibility).
- **Sender-selected routing** — `CC` / `BCC` header arrays add routing keys; `BCC`
  is stripped from the delivered headers.
- **Direct reply-to** — publishing with `reply-to = amq.rabbitmq.reply-to` routes
  the reply straight to the requesting channel's live fast-reply consumer (the
  dominant RPC pattern).
- **Exclusive** and **passive** queues, **durable** / **auto-delete** flags.

**Authentication** — SASL `PLAIN` / `AMQPLAIN` against the ACL user set; a refused
login gets `ACCESS_REFUSED`. TLS via `amqp-tls-port` (`amqps://`).

**Management** — the RabbitMQ management HTTP API (overview, queues with live
message rates, users) when `management-port` is set.

## Durability and publisher confirms

A confirmed publish is durable at the **`appendonly=everysec`** level — the same
guarantee Redis gives with `appendfsync everysec`. Concretely:

- **Same-shard (local) publish** — the message is applied and its AOF record
  `fwrite`+`fflush`'d to the OS **before** the `basic.ack` ships. Not a promise,
  a fact.
- **Cross-shard publish** — the message is **fire-and-forget** enqueued into the
  owner shard's in-process SPSC ring; the ack ships as soon as it is enqueued.
  The ring is drained FIFO in the same process, so "enqueued" is a binding
  promise the write will be applied — and the owner `fflush`es its AOF after
  every drain pass. The only extra loss window (enqueued, not-yet-drained) is
  microseconds, strictly inside the everysec window a crash already exposes.

**Graceful shutdown keeps the promise.** On `SIGTERM` dreads denies new work,
drains every shard's ring — applying confirmed-but-not-yet-applied publishes —
and flushes every AOF before exiting (~0.1s). A clean shutdown loses nothing that
was acked. (A hard `kill -9` still forfeits the sub-second everysec tail, exactly
like Redis.)

For a message to survive a *restart* the queue keyspace must be persisted:
`appendonly yes`. Without it, queues are in-memory only.

## Sharding and performance

Because a queue lives at its slot, publishers served by other shards hop the
write to the owner. That hop is the whole performance story:

- The hop carries `[db][key][record]` **raw** — no RESP synthesis, no reply
  round-trip. It applies the `RPUSH` directly on the owner and confirms on
  enqueue (fire-and-forget). This is what stops per-shard throughput from
  collapsing as shards grow.
- A **single hot queue** cannot be parallelized — it is one list on one shard, so
  all its producers converge there. Spread load across **many queues** (or use
  same-slot `{hashtag}` queue names deliberately) and the shards run independent.

Measured on a 3950X, 16 producers over 16 queues, aggregate publish rate: ~5.0M
msg/s at 1 shard, staying near that through 4 shards; a single hop-bound stream
runs ~3–4× faster than the naive synchronous hop it replaced. RabbitMQ's peak on
the same box is ~264K msg/s.

## Deliberately not here yet

Honest scope — these are known gaps, not hidden ones:

- **No heartbeat enforcement.** Heartbeats are negotiated but a dead peer is
  detected by read timeout, not by missed heartbeats.
- **No `basic.qos` prefetch windows.** Consumers poll; a global/per-consumer
  prefetch cap is accepted but not used to throttle delivery.
- **Auto-ack is the redelivery model.** Explicit acks are accepted and honored
  for requeue, but crash-time redelivery of un-acked in-flight messages (a
  per-consumer PEL) is the stream-backed v2, not here.
- **Consumer-count aggregation is shard-local.** `queue.declare-ok`'s
  consumer_count reflects this shard's consumers, not a cross-shard sum.

Everything else on the wire — method framing, error codes, property tables,
content headers — targets RabbitMQ byte-for-byte, with the `pika` client suite as
the yardstick.

## Interop

Tested against **pika** (the reference Python client) — publisher confirms, FIFO
ordering, cross-shard queues, dead-lettering, and mandatory returns. Any AMQP
0-9-1 client that negotiates down to the pre-flexible method set (kombu, the Java
client, Bunny, amqplib) speaks to dreads.

## See also

- [SHARDING.md](SHARDING.md) — the slot model and the SPSC hop fabric.
- [AMQP 1.0](AMQP10.md) — the separate, newer protocol (not a superset of 0-9-1).
- `source/dreads/amqp.d` — the implementation and the authoritative scope line.
