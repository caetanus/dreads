# AMQP 1.0

dreads also speaks **AMQP 1.0** — the OASIS/ISO standard, the protocol behind
RabbitMQ 4.x's native AMQP, Azure Service Bus, and Qpid. It is **a different
protocol from [AMQP 0-9-1](AMQP.md)**, not a newer revision of it: symmetric
peer-to-peer links, credit-based flow, explicit settlement, a self-describing
type system. The two share **nothing on the wire beyond the TCP port** — the
8-byte protocol header decides which conversation you get.

Under the hood they share the thing that matters: **the same queue keyspace**. A
message published over 1.0 lands in the same `amq.q.<name>` list a 0-9-1 client
reads, so the two protocols interoperate — publish on one face, consume on the
other.

> Stated honestly, like the rest of dreads. This page tells you what works, what
> is an extension, and what is deliberately not here yet.

## One port, header dispatch

There is **no separate config for 1.0.** The same `amqp-port` (and
`amqp-tls-port`) serves both dialects; the client announces itself with the
protocol header and dreads routes accordingly:

- `AMQP\x00\x00\x09\x01` → the 0-9-1 conversation.
- `AMQP\x00\x01\x00\x00` → AMQP 1.0 (bare).
- `AMQP\x03\x01\x00\x00` → AMQP 1.0 with the SASL security layer.

The TLS leg negotiated on `amqp-tls-port` is handed straight to whichever dialect
the header selects, so 1.0-over-TLS works with no extra setup.

```sh
dreads --amqp-port=5672 --amqp-tls-port=5671 --appendonly=yes --dir=/var/lib/dreads
```

```java
// RabbitMQ AMQP 1.0 Java client
Environment env = new AmqpEnvironmentBuilder()
    .connectionSettings().uri("amqp://localhost:5672").environmentBuilder().build();
Connection conn = env.connectionBuilder().build();
Publisher pub = conn.publisherBuilder().queue("orders").build();
pub.publish(pub.message("hello".getBytes()), ctx -> {});
```

## The conversation

**SASL** — the security layer advertises `PLAIN` and `ANONYMOUS`; `PLAIN`
authenticates against the ACL user set, `ANONYMOUS` is refused once an ACL is
configured. `open`/`close` carry the container id and negotiate the idle timeout
(heartbeats at half the interval, like the 0-9-1 face).

**Sessions and links** — `begin`/`end` open a session; `attach`/`detach` open a
link. Roles are symmetric:

- A **sender link** from the client (role = sender) means the client publishes to
  us — its `target` resolves to an exchange (or the default exchange by queue
  name). An **anonymous sender** (empty target) routes each message by its own
  `properties.to` address.
- A **receiver link** from the client (role = receiver) means we send to the
  client — a broker consumer fiber drives `transfer` frames as messages arrive.

**Flow control is link credit, not prefetch.** A receiver grants credit with
`flow`; the broker sends at most that many `transfer`s and stops until more
credit arrives. Incoming/outgoing windows are honored.

**Settlement is explicit, via `disposition`** — and it maps onto the queue's
lifecycle exactly as RabbitMQ does:

| 1.0 disposition | queue effect |
|---|---|
| `accepted` | ack (message consumed) |
| `released` | requeue (back to the front) |
| `rejected` | dead-letter (or drop if no DLX) |
| `modified` | requeue / dead-letter, with delivery annotations |

**Management** — queue and exchange topology is declared through the standard
`$management` node (the AMQP 1.0 management extension); those declares replicate
across shards through the same control-plane ops the 0-9-1 face uses, so a queue
declared over 1.0 is visible to a 0-9-1 consumer and vice-versa.

## Streams and filters

dreads implements RabbitMQ's **stream** consumption and **filter expressions**
over 1.0 (validated against RabbitMQ's own `SourceFiltersTest`):

- **Stream queues** — a queue declared `type=stream` is read **non-destructively
  by position**. A receiver's `source` filter-set carries an offset spec —
  `rabbitmq:stream-offset-spec` as `first` / `last` / `next` / an absolute
  `long` / a `timestamp` — and each delivered message is annotated with its
  `x-stream-offset`. Multiple consumers read the same stream independently; a
  disposition on a stream delivery is a no-op (the log is not consumed).
- **Bloom stream filters** — `rabbitmq:stream-filter` (a list of filter values)
  plus `rabbitmq:stream-match-unfiltered`. A message whose value doesn't match is
  skipped **without burning link credit** (the flow-control contract the RabbitMQ
  test enforces).
- **Filter expressions (RabbitMQ 4.1)** — `amqp:properties-filter` (match on the
  standard message properties) and `amqp:application-properties-filter` (match on
  application key/value pairs), with the `$p:` / `$s:` prefix and suffix
  modifiers.

A stream consumer's `attach` is refused if its `source` would drop the filters it
asked for — so a client always knows whether its filter took effect.

## Deliberately not here yet

Honest scope — known gaps, not hidden ones:

- **No transactions** (the 1.0 transactional-messaging feature).
- **No link resume** — a detached link's unsettled map is not preserved for
  re-attach; a reconnecting client starts fresh.
- **No link-routing / multi-hop** — links terminate at a queue/exchange on this
  broker, not routed onward.

Everything on the wire below that — the type-system codec, performative framing,
SASL, flow/credit accounting, settlement, stream offsets and filters — targets
the spec and RabbitMQ's behavior, with the **rabbitmq-amqp-java-client** suite as
the yardstick.

## Interop with 0-9-1

Because both faces share the `amq.q.<name>` keyspace, they mix freely:

- Publish over 1.0 (a sender link to `orders`), consume over 0-9-1 (`basic.get
  orders`) — same message, same FIFO order.
- Declare a queue over either face; the other sees it (declares replicate through
  one shared control plane).
- The same dead-letter / TTL / max-length policy (set at declare time) governs a
  queue regardless of which protocol writes it.

## See also

- [AMQP 0-9-1](AMQP.md) — the RabbitMQ classic dialect and the queue-is-a-list
  model both faces share.
- [SHARDING.md](SHARDING.md) — the slot model and cross-shard hop that carry
  queue traffic.
- `source/dreads/amqp10.d` — the implementation and the authoritative scope; the
  milestone history is in `AMQP10-PLAN.md`.
