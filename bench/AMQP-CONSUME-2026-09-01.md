# AMQP consume: 2.8x less CPU per message (2026-09-01, 3950X)

Replacing the unacked hash map with a dense sliding window keyed by delivery tag
cut the consume path from **3.44 to 1.23 µs/msg** — past LavinMQ's 1.6 µs.

## Method — and two traps that invalidated earlier numbers

Deterministic drain: prefill a fixed 800k-message backlog, then time a
consume-only run. A mixed producer+consumer run never reaches steady state and
spreads 2x; this shape is comparable across builds.

Server pinned to physical cores 0-7, PerfTest (docker) to 8-15 + siblings 24-31.
16-byte persistent messages, manual ack. Throughput is PerfTest's `receiving rate
avg`; CPU is utime+stime read off `/proc/<pid>/stat` across the drain, so the
per-message cost is measured on the broker and never inferred from the client.

Two traps, both of which produced wrong conclusions before they were caught:

- **The client is often the limiter, not the broker.** An 8-consumer drain
  plateaus at ~590k msg/s for *every* shard count, which reads as "sharding buys
  nothing". Widening the client alone (16 queues/consumers over 16 CPUs, server
  untouched) moved the same s1 build to ~700k. Always confirm the broker is
  CPU-saturated before reporting a throughput ceiling as the broker's.
- **PerfTest's `-D` is a PER-CONSUMER limit.** With more consumers than queues
  the backlog does not split evenly, at least one consumer never reaches its
  quota, and the run hangs forever. Keep one queue per consumer.

Run-to-run spread on this harness is about ±10%, so single samples decide
nothing: every figure below is the median of three interleaved A/B runs.

## Result — interleaved A/B, same client, s1, 16 queues/consumers

| build | µs/msg (3 runs) | median | msg/s (3 runs) | median |
|---|---|---:|---|---:|
| hash map (before) | 3.44 / 3.36 / 3.58 | **3.44** | 682k / 711k / 673k | **682k** |
| dense window (after) | 1.40 / 1.19 / 1.23 | **1.23** | 656k / 783k / 734k | **734k** |
| LavinMQ 2.9.2 (~1 core) | — | *1.6* | — | *550k* |

**2.8x less CPU per message. Throughput only +8%** — because at 1.23 µs × 734k/s
the broker now occupies **0.90 of one core**: it is no longer the bottleneck in
this configuration, so the remaining ceiling is the client and the loopback, not
dreads. The efficiency win is real and the throughput number understates it.

Against LavinMQ: 1.3x more efficient per message, +33% throughput.

## What made the difference

`perf record --call-graph` on the drain put ~35% of all cycles in one place:

| symbol | share |
|---|---:|
| GC mark (`ScanRange`) | 11.0% |
| `_aaGetX!(ulong, Unacked)` — the delivery-side insert | 7.9% |
| `settleTagUnknown` — one lookup on the ack path | 7.0% |
| `GCBits.setLocked` | 3.4% |
| `bytesHashUnaligned` / `_d_aaInH` / hash mix / AA resize | 3.9% |
| GC `smallAlloc` | 1.2% |

`settleTagUnknown` costing 7% for a *single* lookup is the signature of a cache
miss — a pointer chase into a large hash. Delivery tags are `nextTag++`, strictly
sequential, and the outstanding set is almost always a compact run, so the map
was paying hashing and indirection for a key that is already a dense index. The
GC share follows from the same structure: its buckets are scanned every cycle.

`UnackedMap` holds that run in a ring indexed by `tag - base`, with an AA spill
for the pathological case (a client that acks a high tag and sits on a low one).
It keeps the built-in AA's surface — `in`, `[]=`, `remove`, `length`, `clear`,
`foreach` — so all ~17 call sites are unchanged. Iteration order becomes
ascending instead of hash order; every foreach site already sorted explicitly,
so nothing depended on it.

## Not regressed

- RESP hotpath, interleaved A/B, 2M ops, `-P16 -c32 --threads 4`: SET/GET
  1.141M both builds, within 0.06%. Untouched.
- 599 unit tests (up from 598: `UnackedMap` covers out-of-order settle, base
  advance, growth, spill, spill re-migration, iteration order, clear).
- Full 5-skin conformance battery with all skins live at once: ALL PASSED.

## Still open

At s2 with 16 consumers the same build only reaches 2.16-2.30 µs/msg — the
remaining cost there is the cross-shard hop, not the unacked store. The
per-message `pay.data.idup` (a GC allocation that copies the payload) also
survives this change and is the next candidate.
