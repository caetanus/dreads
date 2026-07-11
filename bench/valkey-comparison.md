# dreads vs Valkey 9.1 (and MQTT) — benchmark

**Date:** 2026-07-11 · **dreads** `b062f27` (LDC release, `-O3 -release
-mcpu=x86-64-v3`, jemalloc) · **Valkey** 9.1.0 (jemalloc, `--io-threads 1`) ·
**Mosquitto** (eclipse-mosquitto, MQTT QoS 0). Scripts: [`redis-bench.sh`](redis-bench.sh),
[`mqtt-bench.sh`](mqtt-bench.sh) — see [README](README.md).

## Method

12-core Linux box. Each server **single-threaded, pinned to one core**, client on
separate cores, **one server at a time** (a co-resident idle server perturbs the
numbers). Persistence off unless the AOF section. `-P 16 -c 50`, min/median/max
over 5–7 runs (median is the honest figure).

## Data operations — min · **median** · max rps

| Command | dreads | Valkey 9.1 | med ratio |
|---|---|---|---:|
| GET | 0.92M · **1.07M** · 1.12M | 0.94M · **1.05M** · 1.10M | 1.02× |
| SET | 0.68M · **1.03M** · 1.13M | 0.68M · **0.83M** · 0.90M | 1.24× |
| INCR | 0.74M · **0.86M** · 0.96M | 0.86M · **1.05M** · 1.09M | 0.82× |
| LPUSH | 1.10M · **1.16M** · 1.27M | 0.79M · **0.91M** · 1.01M | 1.27× |
| RPUSH | 0.98M · **1.20M** · 1.26M | 0.86M · **0.94M** · 1.01M | 1.28× |
| LPOP | 1.15M · **1.28M** · 1.38M | 0.75M · **0.80M** · 0.82M | 1.61× |
| SADD | 0.84M · **1.03M** · 1.08M | 0.68M · **0.83M** · 0.90M | 1.24× |
| HSET | 0.86M · **1.01M** · 1.05M | 0.67M · **0.70M** · 0.76M | 1.44× |
| SPOP | — · **1.35M** · 1.37M | — · **1.17M** · 1.25M | 1.15× |
| ZADD | 0.32M · **0.36M** · 0.39M | 0.33M · **0.34M** · 0.35M | 1.05× |
| ZPOPMIN | 1.01M · **1.27M** · 1.32M | 1.10M · **1.15M** · 1.20M | 1.10× |
| MSET(10)| 0.27M · **0.28M** · 0.29M | 0.17M · **0.18M** · 0.18M | 1.56× |

Peak (single server, `-P 32`, best-of-5): **GET 2.34M vs 1.73M (1.35×), SET 1.33M
vs 0.93M (1.43×)**. dreads wins ~everywhere; INCR is Valkey's.

## SET with EX (TTL write path)

| | dreads active-off (default) | dreads active-on | Valkey |
|---|---:|---:|---:|
| SET EX 100 (median) | ~660k | 510k | 558k |

Active expiry is opt-in; off by default the TTL write path stays at Valkey-class
throughput. (Active-on maintains the drop-soon index — see the expiry commits.)

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
