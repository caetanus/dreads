# PubSub performance plan — match at subscribe, not at publish

## The problem

Redis `PUBLISH channel msg`:
- **Exact subscribers:** `dict[channel]` → O(1), iterate. Already fast.
- **Pattern subscribers:** walk the *entire* pattern list and run `stringmatchlen(pattern, channel)` on each. Cost = **O(P) × O(glob)** per publish, where the glob is a recursive matcher that backtracks on `*`.

Both costs bite: the O(P) linear scan *and* the per-pattern matcher. The worst
case is not synthetic — **keyspace notifications** are exactly this shape: every
key mutation publishes to `__keyspace@0__:<key>` and clients
`PSUBSCRIBE __keyspace@0__:user.*`, so the glob-per-publish becomes the whole
server's bottleneck.

## The idea: invert the question

Redis asks, per publish: *"for each pattern, does it match this channel?"* —
pattern-centric, O(P).

We invert it: *"given this channel, which patterns match?"* — channel-centric,
against an index built **over the subscriptions**. The organizing work happens
**at SUBSCRIBE time** (index the pattern once); at publish the channel just
probes the index and touches only patterns whose literal anchor already fits.

## Anchor taxonomy (single-star patterns)

A pattern with one `*` and literal text around it splits by which side is
anchored. Let `C` be the channel:

| Pattern | Class | Confirm test | Cost |
|---|---|---|---|
| `A*`  | **left-bound**  | `C.startsWith(A)` | O(len A) |
| `*B`  | **right-bound** | `C.endsWith(B)`   | O(len B) |
| `A*B` | **both-bound**  | `C.startsWith(A) && C.endsWith(B) && len(C) >= len(A)+len(B)` | O(len A + len B), **no middle search** |

The key insight: `A*B` needs **no substring search** — the middle `*` is free,
so two anchored comparisons decide it. `len(C) >= len(A)+len(B)` prevents the
prefix and suffix from overlapping (`*` matches zero-or-more, so `A*B` matches
`AB`).

Residual general patterns — `?`, `[...]`, or multiple interior stars
(`A*B*C`) — are rare. They are pre-tokenized once (split on `*` into anchored
segments) and matched iteratively (no recursive backtracking); they still index
by their leading literal `A`, so they are only *candidates* when their prefix
fits the channel — never a blind scan.

## Data structures

1. **`exact`** — `hashmap<channel, subscriberSet>`. Exact `SUBSCRIBE` and
   metachar-free patterns. O(1) lookup.
2. **`leftIndex`** — radix (compressed prefix) tree keyed by the literal prefix
   `A`. Holds `A*` (confirm = true), `A*B` (store `B`, confirm = endsWith + len),
   and general `A*...` (store the compiled tail matcher). Also the home of
   general patterns' leading literal.
3. **`rightIndex`** — radix tree keyed by `reverse(B)` for pure `*B`.
4. **`matchAll`** — patterns that are exactly `*` (empty prefix). Delivered on
   every publish; normally a tiny set.

A pattern lives in exactly one index. `A*B` picks the **more selective** anchor
(the longer literal) for its index and stores the other end for the confirm
step.

## Publish path

```
publish(C, msg):
    deliver to exact[C]                             # O(1) + O(subs)
    cand  = leftIndex.walkPrefixes(C)               # O(len C): all A that prefix C
          ∪ rightIndex.walkPrefixes(reverse(C))     # all B that suffix C
          ∪ matchAll
    for p in cand: if confirm(p, C): mark p          # endsWith / tail matcher
    frame = encodeOnce(C, msg)                       # refcounted, one allocation
    for s in subscribers(exact[C] ∪ marked): s.enqueue(frame)
```

`walkPrefixes(S)` descends the radix tree following `S`'s bytes; every node that
terminates one or more patterns yields candidates. O(len S), independent of P.

**Complexity:** from O(P × glob) to **O(len(C) + #patterns-that-actually-match)**.
Publish throughput stays **flat as P grows** — precisely where Redis degrades
linearly.

## Delivery (fan-out)

- **Encode once, refcount, share.** Serialize the RESP `message` frame a single
  time; hand the same malloc-backed refcounted buffer to every matched
  subscriber's out queue. No per-subscriber re-serialization. Fits zero-GC.
- **Bounded per-subscriber buffer, drop on overflow.** PubSub is fire-and-forget
  / at-most-once, so a slow consumer is dropped (or disconnected, à la
  `client-output-buffer-limit pubsub`), never allowed to stall the publisher.
- **Publisher never awaits a socket.** It enqueues and moves on; a blocked write
  yields its own fiber and cannot stall consensus or other channels.
- **PubSub bypasses Raft.** No log, no fsync, node-local — durability zero is the
  correct semantic (matches Redis). Routing it through Raft would be insane.

## Sharding (parallel fan-out)

Channel → shard by `CRC16(channel) & mask`, same as the keyspace. `PUBLISH`
routes to the owning shard's thread; matching and fan-out run on that shard's
core, in parallel with other channels' publishes. Patterns can match channels on
any shard, so the (small, rarely-changing) pattern index is replicated to every
shard; each shard matches locally against its own channels. This is what lets us
support patterns under sharded pubsub — which Redis 7's `SSUBSCRIBE` gave up on.

## Phases

- **P1 — indexed local matching:** anchor classification, `leftIndex` radix,
  encode-once + drop backpressure. Kills the O(P) scan for the dominant `A*`
  case on one node.
- **P2 — full anchors + sharding:** `rightIndex`, `A*B` both-bound, general
  tail matcher; channel→shard routing, `SSUBSCRIBE`/`SPUBLISH`, parallel fan-out.
- **P3 — cross-node + keyspace notifications:** lightweight inter-node broadcast
  (non-Raft) for classic pubsub; wire keyspace-event notifications through the
  same index.

## Measurement

`redis-benchmark` does not exercise pubsub matching. A dedicated harness:
N pattern subscribers with realistic `prefix.*` patterns (keyspace-notify shape),
M publishers, measure publish throughput + delivery latency while **sweeping P**.
The proof is the curve: flat for dreads, linear degradation for Redis.
