# Sharding plan (phase 2)

## Thesis

dreads already has **strong single-group replication** (Raft). Sharding adds
**horizontal scale** on top of it. The differentiator vs Redis Cluster: Redis
Cluster shards give you scale but each shard replicates **asynchronously**
(weak, can lose acked writes on failover). dreads shards are each a **Raft
group** — so the cluster is *both* sharded (scales across cores/machines) *and*
linearizable per key (no acked-write loss). Benchmarks already show dreads' one
Raft group beats Valkey `WAIT` ~4×; N of them in parallel is the scale story.

Two things ship together because they share the machinery:
1. **Scale-up** — thread-per-shard, shared-nothing, on one machine (across cores).
2. **Scale-out** — shards' Raft replicas spread across machines.

## Model

- **16384 hash slots**, Redis-compatible: `slot = CRC16(key) & 16383`, with
  `{hashtag}` extraction (only the substring between the first `{` and the next
  `}` is hashed) so related keys co-locate. This is exactly Redis Cluster's
  keyspace map, so existing cluster-aware clients work unchanged.
- **Shard** = a contiguous-or-not set of slots owned by **one Raft group**.
- **Replica set** = the Raft group's nodes (e.g. 3), on distinct machines.
- Every write for slot *s* goes through shard(*s*)'s Raft leader → majority
  commit → apply. Reads: leader-local (or ReadIndex later; followers serve
  applied state, Redis async-replica semantics).

## Components

### 1. Meta group (authoritative cluster config)
A dedicated Raft group (its own log) owns the **slot map**: `slot -> shardId`,
`shardId -> {members, leader hint, epoch}`, and migration state. It is the one
linearizable source of truth for topology; every node caches it and follows its
change stream. Slot moves and membership changes are **transactions on the meta
group** (so two shards can never both think they own a slot). This replaces
Redis Cluster's gossip+epoch with consensus — simpler to reason about, no
split-brain slot ownership. (Reuses `vendor/raft` + `replicator.d` as-is; the
state machine is the slot map instead of a keyspace.)

### 2. Data shards (Raft group per shard)
Each shard is an independent instance of today's replicated keyspace: its own
Raft node, log, keyspace, dedicated event-loop thread. **Shared-nothing** — a
shard touches only its own state, so no locks between shards. This is the
current dedicated-raft-loop generalized from 1 to N.

### 3. Router / front end
The client-facing listener parses a command, extracts the key(s) → slot →
shard, and:
- **owned locally** → hand off to that shard's thread via a lock-free
  `CrossQueue` (reuse `raftq.d`), reap the reply in order (reuse the write
  pipelining), write it back.
- **owned elsewhere** → `-MOVED <slot> <host:port>` (or `-ASK` mid-migration).
- The router only needs the cached slot map; it does not touch shard state.

### 4. CLUSTER command surface
`CLUSTER SLOTS | SHARDS | NODES | INFO | KEYSLOT | MYID | COUNTKEYSINSLOT |
GETKEYSINSLOT`, plus `ASKING`, `READONLY`/`READWRITE`. Served from the cached
meta state. This is what makes smart clients (that cache the slot map and route
directly) work — most traffic then arrives already at the right node.

### 5. Cross-slot / multi-key
`MSET/MGET/SINTERSTORE/…` and `MULTI`/`EVAL` touching multiple slots must be
same-slot: if keys hash to different slots → `-CROSSSLOT`. `{hashtag}` lets
clients force co-location. Single-key ops are the fast path.

## Threading (the scale-up win)

```
                accept + parse (IO threads)
                      | route by slot
   ┌──────────┬───────┴───────┬──────────┐    lock-free CrossQueue per shard
 shard0     shard1          shard2     shard3   (proposals in, replies out)
 (thread)   (thread)        (thread)   (thread)
  raft+ks    raft+ks         raft+ks    raft+ks   shared-NOTHING: no locks
```

Each shard thread is a full replicated keyspace on one core. A workload spread
across slots runs on all cores in parallel — the same lock-free queue + wake-
when-parked + write-pipelining we built for the single raft loop, instantiated
per shard. Cross-thread hop per command is amortized by pipelining, and paid
only for the parallelism it buys.

## Resharding (live slot migration)

Redis-Cluster-shaped, but ownership flips atomically via the meta group:
1. Meta txn marks slot *s*: source `MIGRATING`, target `IMPORTING`, bump epoch.
2. Keys copied source→target (as replicated writes on the target shard's Raft
   group; source serves reads meanwhile). A key present on source but requested
   on target during migration → `-ASK` one-shot redirect (client sends
   `ASKING` then the command).
3. When drained, a meta txn sets `slot s -> target`, bumps epoch. Nodes see the
   new map; subsequent misroutes get `-MOVED`.
Because the map is consensus-owned, there is never a window where two shards
both accept writes for *s* (unlike gossip races).

## Phasing

- **2a — single-machine thread-per-shard (scale-up, no cross-machine repl).**
  N shard threads in one process, each a keyspace (Raft group of 1, i.e. no
  replication yet — just partitioning). Router hands off by slot. Report all
  slots local via `CLUSTER SLOTS` so cluster clients work. Deliverable: writes
  scale across cores on one box; `CLUSTER KEYSLOT`, hashtags, `CROSSSLOT`.
- **2b — replicated shards across machines (scale-out).** Each shard's Raft
  group spans machines (reuse `raft-peers` per shard). Meta group owns the slot
  map; `-MOVED` redirects; `CLUSTER SLOTS/SHARDS/NODES` report the real map.
  Deliverable: a fault-tolerant sharded cluster stronger than Redis Cluster.
- **2c — live resharding.** `MIGRATING`/`IMPORTING`, `-ASK`, `ASKING`, the
  migrate path, epoch bumps. Deliverable: rebalance without downtime.

## Reuse of existing machinery

- `vendor/raft` + `replicator.d`: one instance per shard **and** for the meta
  group — no new consensus code.
- `raftq.d` `CrossQueue`: router→shard proposal queue and shard→router reply
  queue (same SPSC lock-free ring + wake-when-parked).
- Write pipelining, median-commit, memcpy encode, non-blocking compaction, the
  leader-churn truncation fix: all per-shard, unchanged.
- `commands.d`: dispatch is already keyspace-scoped; a shard just holds its own
  `Keyspace`.

## Open decisions

- **Slot→shard granularity:** contiguous ranges (simple, Redis-shard-like) vs
  arbitrary slot sets (flexible rebalancing). Start contiguous.
- **Shard count on one machine:** fixed at boot (= cores−reserved) vs dynamic.
  Start fixed; dynamic split/merge is later.
- **Meta group placement:** its own 3-node Raft group vs co-located with shard
  0's group. Start dedicated for clarity.
- **Reads:** leader-local first (matches today); ReadIndex/lease for
  linearizable follower reads later.
- **Client hop cost:** measure the router→shard hop under pipelining; if it
  dominates for single-shard-heavy workloads, add a same-thread fast path when
  the connection's traffic is slot-affine.
