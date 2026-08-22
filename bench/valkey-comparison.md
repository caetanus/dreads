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

## SET with EX (TTL write path) — insert only (EX 100 never expires mid-run)

| | dreads active-off (default) | dreads active-on | Valkey |
|---|---:|---:|---:|
| SET EX 100 (median) | **~824k** | ~556k | ~700k |

Active expiry is opt-in. With a 100s TTL nothing expires during the run, so this is
the pure INSERT path. **Off by default the TTL write path beats Valkey** (~824k vs
~700k) — the deadline is just stamped on the value, lazily checked on access. **On,
it drops** (~556k) because dreads keeps expires in an ordered **RB-tree**
(`deadline → keys`, O(log n) insert) vs Valkey's **hash** (`key → deadline`, O(1)).
Tree-vs-hash is a real tradeoff paid only when active expiry is on.

## Active expiry LIVE — short TTL, keys actually expiring mid-run

`bench/active-expire-bench.sh`: SET with PX 100–200 over a 2M keyspace, so keys
expire DURING the run and the background cycle reaps them (competing with request
handling on the one loop). Interleaved, server core 2, client cores 3–11.

| | dreads active-off | dreads active-on | Valkey |
|---|---:|---:|---:|
| SET PX (median, ~300k reaped/run) | ~553–556k | **~553–557k** | ~562–572k |

**Under real expiry churn, active-on ties Valkey (0.97–0.99×) and matches dreads'
own active-off — active expiry is ~free on throughput.** The RB-tree insert cost
(above) is hidden here because the drain keeps the table small, offsetting it.
Caveat: under this *synthetic* ~300k/s expiry pressure dreads reaps less
aggressively (`ACTIVE_EXPIRE_BUDGET=20k` / 200ms = 100k/s cap → keys linger, dbsize
645k post-run draining to 483k idle); Valkey's adaptive cycle reaps more. A
memory-timeliness gap, tunable via budget/interval (trades loop-time), irrelevant at
realistic expiry rates. The **split/lazyfree** exploration confirmed offloading the
drain teardown does NOT help here — d.del dominates on-loop (bench/expire_bench.d);
lazyfree's win is async free of one giant value (bench/lazyfree_bench.d, 7.6×).

## Allocator composition — fragmentation (`bench/rss_churn.sh`)

The keyspace runs on a swappable composed allocator. Its point is **not** throughput
(allocator is <1% of CPU — see the tie above) but **jemalloc-independence**: a real
portable byte count for OOM, reclaim policy we own, and swap-by-build. Churn 265 MB
of live data (varied sizes) over 40 delete/refill rounds, RSS vs used (lower = less
fragmentation):

| composition (build) | RSS | RSS/used |
|---|---:|---:|
| **composed** — freelist + bucketizer + bitmapped mid (default) | **363 MB** | **1.40** |
| Mallocator (jemalloc), `DreadsDataMalloc` | 382 MB | 1.47 |
| bitmap tiers, `DreadsDataBitmap` | 465 MB | 1.79 |
| bump/region (LIFO reclaim only), `DreadsDataBump` | 916 MB | 3.53 |

The composed layout is the best of the tested compositions: it beats raw jemalloc by
~5% (hands it a coarse big-block pattern, pools fine-grained by size itself), beats a
bitmap-heavy layout by ~22% (BitmappedBlock rounds to its block size ⇒ internal
waste), and a bump/region 2.5× (barely reclaims). Freed blocks do **not** return to
the OS under jemalloc either (dirty-page decay), so "our freelist vs jemalloc" is
about *precision of reuse*, not OS return.

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

## Bench box (all 2026-08-21 sections below)

AMD Ryzen 9 3950X (Zen 2, 16C/32T, 4× 4-core CCX, 16 MB L3 per CCX, HT
siblings N↔N+16), 62 GiB DDR4, Linux 7.1.8-1-MANJARO, performance governor,
default mitigations ON, SMT unused for benching (physical cores only: server
on 0..N-1, clients on 8..15). Clients: `redis-benchmark -c 25 -P 64
-r 200000`, 4 pinned processes summed; results above ~7M rps need 5 client
processes (cores 8/9/10/12/14 — leave 3 cores free for loopback softirq; 6+
client procs REGRESS). dreads: LDC release (verify it — a debug binary costs
~40%!). Valkey 9.1.1: distro jemalloc build, `--save '' --appendonly no
--io-threads 1`. Dragonfly v1.40.1: official release binary, io_uring,
`--proactor_threads N`.

## Single-threaded head-to-head (2026-08-21, unified methodology)

Both pinned to one core, scripts: `scratchpad/bench/h2h.sh` pattern (server
core 0, clients 8/10/12/14, 2M ops each):

| Command | dreads | Valkey 9.1.1 | ratio |
|---|---:|---:|---:|
| SET | 1.86M | 0.83M | 2.2× |
| LPUSH | 2.11M | 1.06M | 2.0× |
| HSET | 1.62M | 0.81M | 2.0× |
| SADD | 1.65M | 1.08M | 1.5× |
| GET | 1.70M | 1.22M | 1.4× |
| INCR | 1.58M | 1.26M | 1.3× |
| ZADD (single growing zset) | 0.42M | 0.35M | 1.2× |

## Dragonfly head-to-head (2026-08-21, dreads v0.5.0 vs dragonfly v1.40.1)

Same box (Ryzen 3950X), same methodology both sides: server pinned to cores
0..N-1, 4× redis-benchmark clients pinned to cores 8/10/12/14, `-c 25 -P 64
-r 200000`, 3M ops per client, performance governor, best of 2-3. Dragonfly
run as shipped (io_uring, `--proactor_threads N`, no persistence); dreads
`--shards N` (epoll), AOF off. Both are dumb-client setups (single conn per
client → internal cross-thread hops on both sides — the honest comparison;
neither side gets a cluster-aware client).

| op@threads | dreads    | dragonfly | dreads/dfly |
|------------|-----------|-----------|-------------|
| SET @1     | 1.78M     | 0.63M     | **2.8×**    |
| SET @2     | 2.99M     | 1.29M     | 2.3×        |
| SET @4     | 5.23M     | 2.53M     | 2.1×        |
| SET @8     | **6.84M** | 4.26M     | 1.6×        |
| GET @1     | 1.75M     | 0.72M     | 2.4×        |
| GET @8     | **9.81M** | 4.71M     | **2.1×**    |
| INCR @1    | 1.78M     | 0.66M     | 2.7×        |
| INCR @8    | **8.06M** | 4.62M     | 1.7×        |

Reference on the same setup: valkey 9.1 solo = SET 768K / GET 948K.

(An earlier revision of this table reported INCR@8 as a tie at 4.58M and
GET@8 as 6.82M — BOTH were measurement errors, kept here as a warning: the
INCR/GET@8 dreads runs had silently picked up a DEBUG build of bin/dreads,
and GET/INCR@8 were additionally CLIENT-bound — 4 benchmark procs cap out
around 7-8M; anything above needs 5 client procs (cores 8/9/10/12/14, three
spare for the loopback softirq — 6+ clients REGRESS from softirq
starvation). SET@8 is genuinely server-bound (4 and 5 clients agree).
Dragonfly's numbers sit well under the 4-client cap and were unaffected.)

Reading: Dragonfly's per-thread scaling percentage is excellent (~85% at 8,
ours 48-70% depending on op) but its per-thread baseline is ~2.8× lower, so
dreads wins EVERY point, 1.5-2.8×. The honest scaling metric — marginal
throughput per added core — also favors dreads (SET: +0.72M/core vs +0.52M).
Their flat ~4.3-4.7M across ALL ops at 8 threads suggests a coordination/IO
ceiling; ours still varies by op (6.8-9.8M; per-shard efficiency SET 48% / INCR 57% / GET 70%), i.e. bound by per-op cost, not
by the fabric. Remaining upside: the per-hop tax (bytecode-IR) and the
hashtable; at the 85-90%/shard target our 8-thread ceiling is ~12M.

## Full sweep — every protocol, one session (2026-08-22)

All numbers re-measured back-to-back on the bench box (see the spec section
above), release build `e12572e`+, performance governor, one server core
unless stated, pinned clients, port-ownership verified with `ss` before
every run (see the 9092 trap note). Best of the runs shown; same load
generator binary for both sides in every protocol.

### RESP, single thread (4 pinned clients, -c 25 -P 64)

| op | dreads | valkey 9.1.1 | Dragonfly (1 thread) |
|---|---:|---:|---:|
| SET  | **1.86M** | 958K (1.9×) | 631K (2.9×) |
| GET  | **1.82M** | 1.33M (1.4×) | 736K (2.5×) |
| INCR | **1.91M** | 1.30M (1.5×) | 680K (2.8×) |

### RESP, sharded ladder (SET; 8-shard points use 5 clients)

| threads | dreads | Dragonfly |
|---:|---:|---:|
| 1 | 1.80M | 631K |
| 2 | 2.81M | 1.23M |
| 4 | 5.04M | 2.44M |
| 8 | **8.03M** | 4.04M |

At 8 threads: GET **10.4M** vs 4.59M (2.3×), INCR **8.18M** vs 4.13M (2.0×).
GET@8 is up from 9.8M at the last campaign checkpoint.

### MQTT (QoS1 acked, window 256, 16B; 1 core each)

| metric | dreads | mosquitto 2 (tuned) |
|---|---:|---:|
| acked pub, no subscribers | **8.37M msg/s** | 76.2K (110×) |
| acked pub, 1 live subscriber | **6.13M msg/s** | 74.3K (82×) |
| end-to-end, sustained 3s window | **4.37M msg/s** | subscriber dropped |

(Post-hardening numbers — full 3.1.1 validation on every packet costs ~5%
on the no-sub path and the per-connection writer-fiber redesign GAINED 28%
on the live-subscriber path. The end-to-end number uses the honest metric:
deliveries counted over a sustained window starting at the first delivery,
under a continuous flood; the earlier 2.12M silently included the idle gap
before the publisher started, and burst-drain runs can report absurd rates.
Slow subscribers now shed QoS-0 load at a 64MB outbox cap instead of
stalling anything — mosquitto's answer to the same overload is dropping the
subscriber entirely.)

### AMQP 0-9-1 (publisher confirms, window 256; 1 core each)

| metric | dreads | RabbitMQ 3 |
|---|---:|---:|
| confirmed publish | **1.11M msg/s** | 32.4K (34×) |
| end-to-end (concurrent pub+consume) | **708K msg/s** | 40.6K (17×) |

### Kafka (6M msgs, acks=1, 32 msgs/request; dreads 1 core, Apache 2 cores)

| metric | dreads | Apache Kafka 3.7 |
|---|---:|---:|
| acked produce (CRC-validated) | **3.70M msg/s** | 515K (7.2×) |
| fetch (drain from 0) | **21.5M msg/s** | 2.57M (8.4×) |

Durability footnote from this sweep: Apache acked 6,000,000 produces but
its log retained 5,998,816 (acks=1 semantics — 1,184 acked messages gone
after a clean container stop); the load client now reads the high
watermark from fetch responses and stops at the real end instead of
spinning. dreads retained all 6,000,128 acked records (count is the 32-per
-request rounding), replayable through the same per-shard AOF that backs
RESP/MQTT/AMQP.

## MQTT skin vs Eclipse Mosquitto (2026-08-21)

Same box (see the bench-box section), both brokers pinned to core 0, driven
by the SAME load generator (`bench/mqttload.d` — a pipelined QoS-1 publisher
with a 256-message inflight window counting PUBACKs, plus a delivery-counting
subscriber; identical binary against both brokers). Mosquitto 2.x (official
docker image) tuned for fairness: `max_inflight_messages 0`,
`max_queued_messages 0`, `set_tcp_nodelay true`. dreads: `--mqtt-port 1883`,
single shard.

| metric (1 core, QoS1 window 256, 16B payload) | dreads MQTT skin | mosquitto | |
|---|---:|---:|---:|
| acked publish rate, 1 subscriber attached | **4.16M msg/s** | 77.6K | **54×** |
| end-to-end deliveries to that subscriber   | **2.04M msg/s** | (subscriber dropped under load) | — |
| acked publish rate, no subscribers          | **8.3M msg/s**  | 76.8K | 108× |

Notes: mosquitto disconnects the QoS-0 subscriber under this load (a legal
QoS-0 overload response), so its end-to-end number does not exist at this
rate. The dreads skin delivers 2M msg/s to a live subscriber on ONE core —
the same engine, threads and share-nothing fabric that serve RESP (`the
Redis tables above`): MQTT here is a protocol face, not a separate broker.
Correctness: wildcard (`+`/`#`) overlap, retained-message delivery to late
subscribers on other shard threads, and QoS1 acks verified with paho-mqtt
at shards=4.

## AMQP skin vs RabbitMQ 3 (2026-08-22)

Same box, both brokers pinned to core 0, driven by the SAME load generator
(`bench/amqpload.d` — publisher-confirm mode with a 256-message inflight
window counting basic.ack, and a no-ack consumer counting basic.deliver;
identical binary against both). RabbitMQ 3 official docker image, default
config; dreads `--amqp-port 5672`, single shard, AOF off.

| metric (1 core, confirms on, 16B payload) | dreads AMQP skin | RabbitMQ 3 | |
|---|---:|---:|---:|
| confirmed publish rate            | **1.13M msg/s** | 33.4K | **34×** |
| end-to-end (pub+consume, no-ack)  | **797K msg/s**  | 39.5K | **20×** |

Every dreads publish traverses the REAL data plane: the queue is a list in
the shard's keyspace (`LRANGE amq.q.<name>` shows it from the RESP side),
which is why queues survive kill -9 via the per-shard AOF with zero
AMQP-specific persistence code — verified: 10 queued messages with
properties recovered in order after a SIGKILL. Feature surface validated
with pika at shards=4: topic/direct/fanout/HEADERS exchanges, property
passthrough, publisher confirms, ack/nack/reject with requeue,
dead-lettering via x-dead-letter-exchange, consumer cancel, heartbeats.

## Kafka skin vs Apache Kafka 3.7 (2026-08-21)

Same box, driven by the SAME load generator (`bench/kafkaload.d` — a
correlation-pipelined producer with a 64-request inflight window, 32
messages per Produce request, acks=1, counting broker acks; and a fetch
loop counting fetched messages; identical binary against both brokers,
speaking the pre-flexible dialect both accept: Produce v2 / Fetch v3 /
MessageSet v1). Apache Kafka 3.7 official docker image, KRaft single
broker, pinned to cores 0-1 (TWO cores); dreads `--kafka-port 9092`,
single shard, pinned to core 0 (ONE core). 16B payloads.

| metric | dreads Kafka skin (1 core) | Apache Kafka 3.7 (2 cores) | |
|---|---:|---:|---:|
| acked produce rate (acks=1)   | **4.44M msg/s** | 645K | **6.9×** |
| fetch (consume from offset 0) | **23.4M msg/s** | 2.56M | **9.1×** |

A produce request's whole message set lands as ONE atomic variadic RPUSH on
the owner shard — the batch's base offset is `new length - count`, so offset
assignment is atomic with the append and every skin (RESP, MQTT, AMQP,
Kafka) shares the exact same write tail, AOF included: a partition IS a
list (`LRANGE kafka.t.<topic>.<p>` shows the records from the RESP side),
and produced messages survive kill -9 with zero Kafka-specific persistence
code. Fetch initially LOST to the incumbent (1.94M vs 2.56M: Kafka serves
contiguous log segments via sendfile, while a naive skin re-walked the list
per fetch — O(offset) seek per request, quadratic over a partition drain).
Two structural fixes closed it and then some: (1) a direct owner-shard read
path (walk the packed list segment and append `[offset][stored record]`
straight into the response buffer — no synthesized-RESP LRANGE round trip,
no reply parse, no per-record re-copy; offsets are implicit list indices,
stamped on the fly), and (2) a resume cursor (`ListSeekHint`): a sequential
consumer's next fetch starts walking at the byte position where the last
one stopped, epoch-validated against the segment's buffer address and the
list's lifetime push/pop counters, so ANY mutation that could shift bytes
or indices degrades the cursor to a plain head walk — never to wrong
bytes (verified: LPOP from the RESP side mid-consumption, then reseek).
12M-record drain: 512ms. One core, ~4 of 5 runs land 22.4-23.4M; the
occasional ~4.6M run is the known loopback-softirq placement lottery.

Bench-trap note (recorded so nobody repeats it): the Apache container binds
`127.0.0.1:9092` (specific address) while dreads binds `*:9092` — BOTH
listens coexist, and the kernel hands every loopback connect to the more
specific one. A benchmark "against dreads" with the container still up is
silently a benchmark against Apache Kafka. `docker stop kafka` first;
verify with `ss -ltnp | grep 9092`.
