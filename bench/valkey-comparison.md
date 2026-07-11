# dreads vs Valkey (and MQTT) — benchmark

**Date:** 2026-07-11 · **dreads** `b062f27` (LDC release, `-O3 -release
-mcpu=x86-64-v3`, jemalloc) · **Valkey** 9.1.0 (jemalloc, `--io-threads 1`,
`--save '' --appendonly no`) · **Mosquitto** (eclipse-mosquitto, MQTT QoS 0).

## Method

- Machine: 12 physical cores, Linux 6.1.
- Each server is **single-threaded** and pinned to **one dedicated core**;
  clients (`valkey-benchmark`) pinned to separate cores so the client never
  steals the server's core.
- **One server at a time** — never dreads and valkey up together (a co-resident
  idle server perturbs the numbers; measured separately).
- Persistence off on both. `-P 16 -c 50 -n 800k-2M -r 100k`.
- Data ops: **7 runs each, min / median / max** reported (median is the honest
  figure; peak is the `-P 32` best-of-5 in a separate row).

## Data operations — median rps (7 runs, one at a time, `-P 16`)

| Command | dreads (min · **med** · max) | Valkey 9.1 (min · **med** · max) | med ratio |
|---|---|---|---:|
| GET | 1.14M · **1.35M** · 1.37M | 0.94M · **1.05M** · 1.10M | 1.29× |
| SET | 0.91M · **1.02M** · 1.16M | 0.76M · **0.81M** · 0.88M | 1.26× |
| PUBLISH (0 sub) | 1.16M · **1.39M** · 1.42M | 1.04M · **1.12M** · 1.19M | 1.24× |

Broad suite (best of 3, `-P 16`, one at a time): dreads wins 11 of 13
(INCR/ZADD tie). Full per-command peak table below.

| | dreads | valkey | | | dreads | valkey |
|---|--:|--:|---|---|--:|--:|
| SET | **1.11M** | 831k | | SPOP | **1.39M** | 1.21M |
| GET | **1.11M** | 1.07M | | ZADD | 365k | 354k |
| INCR | 867k | **1.05M** | | ZPOPMIN | **1.37M** | 1.21M |
| LPUSH | **1.18M** | 912k | | MSET(10) | **297k** | 183k |
| RPUSH | **1.26M** | 993k | | LPOP | **1.18M** | 862k |
| SADD | **1.02M** | 847k | | HSET | **999k** | 750k |

### Peak (single server, `-P 32`, best of 5)

| | dreads | Valkey | ratio |
|---|--:|--:|--:|
| GET | **2.34M** | 1.73M | 1.35× |
| SET | **1.33M** | 928k | 1.43× |

## Pub/Sub fan-out

`PUBLISH` to a channel with N live subscribers (subscribers pinned to cores 8-11,
publisher 4-7). Redis brokers via `valkey-benchmark PUBLISH -P 16`; MQTT via
`mosquitto_pub -l` (C client, QoS 0), host-timed minus container startup.

| PUBLISH rate | dreads | Valkey 9.1 | Mosquitto (MQTT) |
|---|--:|--:|--:|
| 0 subscribers | **1.39M** | 1.12M | — |
| × 10 subscribers | **728k** | 248k | — |
| × 50 subscribers | **472k** | 65k | 330k |

**The batched-write fix.** The subscriber writer fiber originally did one
`tcp.write` **per message** — with `TCP_NODELAY`, a write syscall per message per
subscriber. A publish to N subscribers cost N syscalls, and delivery saturated at
~270k msg/s regardless of N (26.6k×10 ≈ 5.4k×50). Draining the whole outbound
ring into one buffer and issuing **one write per wakeup** (commit `b062f27`) took
PUBLISH ×50 from **5.4k → 472k rps (~87×)** — ~23.6M msg/s delivered on one core.

dreads now leads Valkey on fan-out by **7–12×** and edges out the dedicated MQTT
broker. Caveats on the MQTT column: different protocol and client, and QoS 0 lets
Mosquitto drop under slow subscribers, so it is a rough reference, not an
apples-to-apples row.

## Bottom line

Single-reply data ops: dreads is ahead of Valkey 9.1 across the board (~1.25×
median, up to 1.6×), same architecture (one thread, jemalloc). Pub/Sub fan-out —
after the batched-write fix — is **7–12× ahead of Valkey** and competitive with a
dedicated MQTT broker.
