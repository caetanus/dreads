# PubSub performance plan — pre-match at subscribe, fast-match at publish

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
against an index built **over the subscriptions**.

The match still happens at publish — the channel only exists then. What moves to
SUBSCRIBE time is a **pre-match**: each pattern is indexed once by its literal
anchor. At publish this pre-work does more than skip the O(P) scan — it
*accelerates the match itself*. Descending the channel through the prefix radix
compares each shared prefix **once**, and every pattern anchored on it falls out
together. So the per-publish cost drops below even a *single* glob, not just
below P globs: matching patterns share their anchor comparison instead of each
paying for it. Publish touches only patterns whose anchor already fits, and pays
the anchor test once per shared prefix.

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

The match happens at publish, but the pattern is **pre-matched** into a tree at
subscribe. A pattern is split on its `*` stars into literal runs; those runs
become the tree's nodes, with a wildcard link between them:

```
psubscribe websockets:*:system1
    literals = ["websockets:", ":system1"], one star

root
 └─ "websockets:"        header node        (the discriminator)
      └─ *               wildcard link      (consumes any run, no compare)
           └─ ":system1" tail node  → { (compiled pattern, subscriber) ... }
```

1. **`exact`** — `hashmap<channel, subscriberSet>`. Exact `SUBSCRIBE` and
   metachar-free patterns. O(1) lookup.
2. **`patternTree`** — the segment/topic tree. Edges are literal runs; a `*`
   is a wildcard node linking one literal run to the next. A pattern's terminal
   node holds `{compiled pattern, subscriber}` entries. The **header** (first
   literal run) is the primary discriminator: a publish whose channel does not
   start with a header is rejected on the first, short comparison.
   - `A*`   → header `A`, wildcard, terminal (matches any tail).
   - `*B`   → empty header (root wildcard), tail `B`.
   - `A*B`  → header `A`, wildcard, tail `B` — exactly the example above.
   - `A*B*C`→ header `A`, wildcard, `B`, wildcard, tail `C` (deeper path).
3. **`general`** — patterns using `?` or `[...]` (positional/class wildcards that
   don't align to run boundaries). Small list, pre-compiled, matched iteratively.
   Still bucketed under their leading literal so they are candidates only when the
   header fits.
4. **`matchAll`** — the bare `*` pattern; delivered on every publish. Tiny set.

**Semantics stay exactly Redis's.** A single-star `A*B` reduces to
`C.startsWith(A) && C.endsWith(B) && len(C) >= len(A)+len(B)` — the star spans
anything, colons included, so this is byte-for-byte the glob result, just reached
by tree walk + two anchored compares instead of a backtracking matcher.

## Publish path

```
publish(C, msg):
    deliver to exact[C]                       # O(1) + O(subs)
    node = patternTree.root
    walk C run-by-run:                         # descend by the channel's bytes
        match literal edges against C          # header first — cheap reject
        at a wildcard link, absorb the run up to the next literal (no compare)
        collect entries at every terminal node reached
    for p in general: if globMatch(p, C): collect
    for p in matchAll: collect
    frame = encodeOnce(C, msg)                 # one refcounted allocation
    for s in collected ∪ exact[C]: s.enqueue(frame)
```

Descending the tree compares each **shared header once**: every pattern under a
header is tested by that single comparison, and non-matching publishes die at the
header. Cost is O(len C + entries actually reached) — independent of P.

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

- **P1 — segment-tree matching:** compile patterns into literal runs, insert
  into the pattern tree keyed by header, walk the channel to collect matches.
  Replaces the O(P × glob) publish loop with a header-discriminated tree walk.
  Keeps exact glob semantics (general `?`/`[...]` fall back to `globMatch`).
- **P2 — fan-out + sharding:** encode-once refcounted frame, bounded per-sub
  buffer + drop; channel→shard routing, `SSUBSCRIBE`/`SPUBLISH`, parallel
  fan-out with the pattern tree replicated per shard.
- **P3 — cross-node + keyspace notifications:** lightweight inter-node broadcast
  (non-Raft) for classic pubsub; wire keyspace-event notifications through the
  same tree.

## Measurement

`redis-benchmark` does not exercise pubsub matching. A dedicated harness:
N pattern subscribers with realistic `prefix.*` patterns (keyspace-notify shape),
M publishers, measure publish throughput + delivery latency while **sweeping P**.
The proof is the curve: flat for dreads, linear degradation for Redis.
