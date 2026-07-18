# dreads vs Valkey 9.1 (and MQTT) — benchmark

**Date:** 2026-07-18 · **dreads** `2d5892c` (LDC release, `-O3 -release
-mcpu=x86-64-v3`; keyspace + connection buffers on the swappable composed
allocator) vs **master** `a820285` (raw malloc, pre-allocator refactor) vs
**Valkey** 9.1.0 (jemalloc, `--io-threads 1`). Scripts: [`redis-bench.sh`](redis-bench.sh),
[`mqtt-bench.sh`](mqtt-bench.sh) — see [README](README.md).

## Method

12-core Linux box, **performance** governor. Each server **single-threaded, pinned
to one core** (core 2); client pinned to **9 separate cores** (3–11) — a 2-core
client saturates before the server and hides its real throughput. **One server at
a time** (never co-resident), but the three are **interleaved round-robin per
round** so thermal/load drift cancels instead of skewing whoever runs last.
`-P 16 -c 50`, N=1M, min/median/max over 5 runs (median is the honest figure).

## Data operations — min · **median** · max Mrps

| Command | dreads (composed) | master (raw malloc) | Valkey 9.1 | dreads/valkey |
|---|---|---|---|---:|
| GET | 1.44 · **1.54** · 1.62 | 1.47 · **1.56** · 1.63 | 1.13 · **1.46** · 1.48 | 1.05× |
| SET | 1.35 · **1.39** · 1.48 | 1.31 · **1.42** · 1.47 | 0.91 · **0.94** · 1.00 | 1.48× |
| INCR | 1.34 · **1.37** · 1.41 | 1.21 · **1.39** · 1.41 | 1.15 · **1.19** · 1.24 | 1.15× |
| LPUSH | 1.21 · **1.26** · 1.30 | 1.22 · **1.28** · 1.29 | 0.89 · **1.03** · 1.05 | 1.22× |
| RPUSH | 1.22 · **1.27** · 1.31 | 1.27 · **1.32** · 1.34 | 1.01 · **1.08** · 1.11 | 1.18× |
| LPOP | 1.37 · **1.41** · 1.45 | 1.02 · **1.37** · 1.44 | 0.86 · **0.94** · 0.98 | 1.50× |
| SADD | 1.37 · **1.39** · 1.42 | 1.35 · **1.37** · 1.41 | 1.06 · **1.16** · 1.18 | 1.20× |
| HSET | 1.32 · **1.32** · 1.35 | 1.25 · **1.33** · 1.41 | 0.89 · **0.99** · 1.03 | 1.33× |
| SPOP | 1.45 · **1.55** · 1.65 | 1.40 · **1.57** · 1.62 | 1.30 · **1.36** · 1.38 | 1.14× |
| ZADD | 1.20 · **1.28** · 1.31 | 1.20 · **1.29** · 1.34 | 0.85 · **0.95** · 1.00 | 1.35× |
| ZPOPMIN | 1.42 · **1.44** · 1.47 | 1.38 · **1.44** · 1.47 | 1.21 · **1.35** · 1.38 | 1.07× |
| MSET(10)| 0.54 · **0.57** · 0.58 | 0.55 · **0.56** · 0.59 | 0.23 · **0.24** · 0.25 | 2.38× |

dreads wins **every** op vs Valkey (GET +5% to MSET +138%).

**Did the composed allocator move throughput? No — it's a tie.** `dreads (composed)`
≈ `master (raw malloc)` on all 12 ops, within run-to-run noise. The allocator is
<1% of CPU (the data path is network-bound), so it cannot move ops/s; its payoff is
**RSS/fragmentation under churn** (~3–7.5% lower peak, `bench/rss_churn.sh`) plus
build-swappability / real portable OOM accounting — not throughput.

**INCR flipped since the old table.** The 2026-07-11 row had INCR as Valkey's
(0.82×) because values were string-backed and every INCR re-parsed the string. The
`StrVal` tagged union stores int-encoded values (a native `long` add; bytes
materialised only on read), so INCR is now dreads-favourable (1.15×).

## SET with EX (TTL write path)

| | dreads active-off (default) | dreads active-on | Valkey |
|---|---:|---:|---:|
| SET EX 100 (median) | **~824k** | ~556k | ~700k |

Active expiry is opt-in. **Off by default the TTL write path beats Valkey** (~824k
vs ~700k) — the deadline is just stamped on the value, lazily checked on access.
**On, it drops below Valkey** (~556k) because dreads keeps expires in an ordered
**RB-tree** (`deadline → keys`, O(log n) insert) for a deterministic in-order drain,
whereas Valkey keeps a **hash** (`key → deadline`, O(1) insert) and drains by random
sampling. The tree costs the SET-EX hot path to make the active-expire cycle and
backlog drains (import-release) cheap; the `removeRight`/`rb_first_cached` work makes
that drain O(1)-amortised. Tree-vs-hash is a real tradeoff, paid only when active
expiry is on.

## Transactions (TPS)

`MULTI / SET / INCR / EXEC` blocks, RESP, pipelined, verified applied.

| | dreads | Valkey |
|---|---:|---:|
| transactions/s | **877,530** | 429,074 | (1 txn = 4 commands) |

dreads 2.05×. As expected, pipelining/transactions only amortize the network
fraction (~50–60% of per-op cost), so the ceiling of the gain is bounded.

## Persistence — AOF (median rps, `appendfsync everysec` ≈ dreads `synchronous normal`)

| | dreads + AOF | Valkey + AOF | ratio |
|---|---:|---:|---:|
| SET | **892k** | 587k | 1.52× |
| LPUSH | **1.06M** | 624k | 1.70× |
| SADD | **912k** | 795k | 1.15× |
| HSET | **872k** | 503k | 1.73× |

dreads' deterministic, batched AOF (time/rand-resolved commands, group flush)
leads Valkey's on the write path.

## Pub/Sub

### Pattern matching — **where dreads shines**

Publish to a channel matching **none** of N subscribed patterns — pure matcher
cost. Valkey scans every pattern O(N); dreads' segment-tree stays flat.

| patterns (publish non-match) | dreads | Valkey | dreads ÷ |
|---|---:|---:|---:|
| `pN:*` × 1 | 1.38M | 1.04M | 1.3× |
| `pN:*` × 100 | **1.39M** | 379k | 3.7× |
| `pN:*` × 1000 | **1.36M** | 48k | **28×** |
| `aaN*bb` × 1000 | **1.34M** | 48k | 28× |
| `aaN*c?e*bb` × 1000 | **1.41M** | 50k | 28× |
| `aaN*bb*cc*dd` × 1000 (worst) | **1.40M** | 51k | **27×** |

At 1000 patterns dreads is **~28× Valkey** and barely degrades from 1 pattern;
Valkey collapses 21×. Complex/worst-case globs (`*`+`?`, many segments) don't
change it — the matcher indexes by fixed segments.

### Fan-out — publish to N plain subscribers

| PUBLISH | dreads | Valkey | Mosquitto (MQTT) |
|---|---:|---:|---:|
| 0 subscribers | **1.39M** | 1.12M | — |
| × 10 subs | **683k** | 282k | — |
| × 50 subs | **454k** | 68k | 330k |

dreads leads Valkey **6.7×** on 50-sub fan-out and edges the dedicated MQTT
broker — after fixing a per-message-write bug (commit `b062f27`: batch the whole
outbound ring into one syscall per wakeup; 5.4k → 472k, ~87×).

### Known gap — many subscribers on the *same* pattern

`PSUBSCRIBE '*'` × 50 (all match, fan-out to all): dreads **38k** vs Valkey
**51k** — dreads is *behind* here. Distinct patterns are its strength; N
subscribers on one identical pattern re-check that pattern per subscriber. Open
optimization (dedupe identical patterns), analogous to the fan-out fix.

## Bottom line

Data ops ~1.25× median (up to 1.6×). Transactions 2×. AOF writes 1.15–1.73×.
**Pattern pub/sub ~28×** (the segment-tree matcher). Plain fan-out 6.7× (after the
batched-write fix). One known gap: same-pattern fan-out. Same architecture as
Valkey — single thread, jemalloc — so these are per-op-efficiency wins.
