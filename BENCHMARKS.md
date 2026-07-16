# Benchmarks

Write throughput of **dreads** vs **Valkey 9.1** (the optimized Redis fork) and
**real Redis 7.4** (via docker). `SET`, localhost, pipelined.

```sh
bench/run.sh              # dreads + Valkey tiers (native)
REDIS=1 bench/run.sh      # also real Redis 7 solo + cluster (docker, --net=host)
N=300000 C=50 P=16 bench/run.sh   # override ops / connections / pipeline depth
```

> Numbers are **machine-specific** — always re-run `bench/run.sh` on your box.
> Manjaro Linux (kernel 6.1), AVX2 CPU, LDC / LLVM, jemalloc, dreads release
> build, Valkey 9.1.0, Redis 7.4.9. `-c50 -P16`, loopback, pinned single-thread.
> The **solo in-memory** row was re-measured on the current build (SET, `N=1M`,
> median of 3); the AOF and replication/raft rows are from the earlier run and
> not yet re-measured.

## Results (`-c50 -P16`, writes/sec)

### Single node

| system | mode | rps | durability |
|---|---|---|---|
| **dreads** | solo (in-memory) | **~1.48M** | none |
| **dreads** | persistent AOF | **~0.91M** | group-commit fsync (≈`everysec`) |
| Valkey 9.1 | solo (in-memory) | ~1.01M | none |
| Redis 7 | solo (in-memory) | ~0.84M | none |
| Redis 7 | AOF `everysec` | ~0.59M | batched fsync |
| Redis 7 | AOF `always` | ~0.22M | fsync per write |

### Replication — 3 copies of every key (the real apples-to-apples for raft)

| system | mode | rps | consistency |
|---|---|---|---|
| Valkey | async (primary-only ack) | ~0.72M | **weak** — primary crash loses un-propagated writes |
| Valkey | `WAIT 1` (majority 2/3, sync) | ~0.12M | synchronous (bolt-on) |
| Valkey | `WAIT 2` (all 3, sync) | ~0.26M | synchronous (bolt-on) |
| **dreads** | **raft** (majority 2/3, sync) | **~0.55M** | **linearizable** (native consensus) |

### Sharding (not replication)

| system | mode | rps | note |
|---|---|---|---|
| Redis 7 | Cluster, 3 masters | ~1.19M | keys **partitioned**, **no** replication/consensus |

## Interpretation

- **Engine.** dreads standalone (~1.48M SET) is ~1.5× Valkey 9.1 standalone
  (~1.01M) and leads on every command type (GET/SET/INCR/LPUSH/SADD/HSET/ZADD,
  1.1–1.5×; see the README table) — the D + zero-GC + arena engine is simply fast.

- **Synchronous replication is the headline.** For the guarantee raft actually
  provides — every write durable on a majority before the client is told OK —
  **dreads is ~4× Valkey's `WAIT 1`** (same majority) and still beats Valkey
  waiting on *all three* copies. Valkey's `WAIT` is a retrofit on top of async
  replication: it needs a separate offset-poll round-trip and replicas ack on a
  timer. dreads' raft *is* the replication path — AppendEntries + acks are
  native, pipelined, and group-committed — so the thing raft is built for it
  does far better than a bolt-on.

  *(The `WAIT 1` < `WAIT 2` inversion is a Valkey replica-ack-cadence quirk, not
  a dreads artifact; both are far below dreads regardless.)*

- **Where the baselines "win" is a different guarantee, not a faster one.**
  - Valkey **async** (~0.72M) beats dreads raft — but it can *lose your write*
    on a primary crash; dreads never acks a write that isn't on a majority.
  - Redis **Cluster** (~1.19M) beats everything — but it *shards* (1 copy per
    key across 3 masters) with no replication or consensus; a master death loses
    its shard. dreads keeps 3 consistent copies.

  So dreads' fully-replicated, linearizable cluster **out-throughputs a durable
  single Redis** (~0.55M vs 0.59M `everysec` / 0.22M `always`), and its
  synchronously-replicated writes are multiples of Valkey's.

## Method notes

- `redis-benchmark -t set -n N -c C -P P -q` for the standard tiers.
- Valkey `WAIT` cannot be driven by `redis-benchmark`, so `bench/wait_bench.py`
  pipelines `P` `SET`s + one `WAIT <n> 1000` per connection across `C`
  connections — each batch durable on `n` replicas before it counts.
- dreads raft runs `synchronous off` (OS-buffered log, like Valkey/Redis default
  and `appendfsync no`); durability comes from majority *replication*, not
  per-node fsync. Use `synchronous full` for fsync-before-ack on every node.
- All processes on loopback; Redis containers use `--net=host` for parity.
