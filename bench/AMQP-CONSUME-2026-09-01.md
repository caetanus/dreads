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

---

# Why Amdahl does not work for AMQP (measured 2026-09-01)

Amdahl's law describes a ceiling: speedup flattens, it never descends. AMQP
consume DESCENDS with shard count, which is the USL crosstalk term, not Amdahl.
Here is what that term actually is, measured rather than modelled.

## The measurements

Drain of 800k messages, 16 queues/consumers, server pinned to cores 0-7.

| shards | rate | CPU | busiest thread | vol. ctx switches | per msg |
|---|---:|---:|---:|---:|---:|
| s1 | 692k | 1.11 s | 0.50 core | 611 | 0.001 |
| s2 | 688k | 1.78 s | 0.45 core | 9 747 | 0.012 |
| s4 | 668k | 2.67 s | 0.42 core | 78 831 | **0.099** |

With 32 consumers the throughput curve is unambiguous: **853k (s1) -> 617k (s2)
-> 560k (s4)**. Adding shards makes it slower.

## What it is not

- **Not CPU saturation.** No shard thread exceeds 0.50 of a core at ANY shard
  count. The consume path is latency-bound, not compute-bound — more threads
  cannot buy throughput from a system that is not using the threads it has.
- **Not the idle spin.** That was real and large (7.96 cores at s8 with zero
  messages) and is fixed in 331a673, but the retrograde curve survives it: with
  the spin compiled out entirely, s4 still costs 2.86 s against s1's 0.98 s.
- **Not per-message broadcast.** With ONE queue and ONE consumer at s4, only two
  threads burn anything (4.08 s + 0.12 s) and the total matches s1 (4.20 vs
  4.03 s). Nothing fans out to every shard.
- **Not hop round-trip latency that batching could amortise.** Raising the
  consumer batch from 64 to 256 made every configuration slightly WORSE
  (s1 1.52 -> 1.65, s4 4.28 -> 4.91 us/msg).
- **Not the client.** A single shard reaches 924k msg/s with 32 consumers, well
  past the ~780k that earlier runs mistook for a ceiling.

## What it is

**Adding a shard does not split the work; it converts an in-process call into a
cross-thread park/wake pair.** At s4 the broker performs 129x more voluntary
context switches per message than at s1, and `perf` puts the extra time exactly
there: the kernel's share of samples goes from 11.8% to 38.5% while total
samples rise 3.5x — about 11x more kernel time. At ~15 us per switch, 0.099
switches/msg is ~1.5 us, and the measured excess is 1.95 us/msg (2.67 s vs
1.11 s over 800k). The overhead accounts for essentially all of it.

The consumer fiber runs on the CONNECTION's thread; the queue's owner is chosen
by key hash. They coincide with probability 1/N, so the fraction of deliveries
paying a cross-thread handoff is (N-1)/N — it GROWS with N. That is why the
curve descends instead of flattening.

Spin and park are two halves of one bill: the drain spin budget hides the
handoff by burning a core, parking pays for it in syscalls. Neither removes it.

## What is still unexplained

The arithmetic does not close. 78 831 switches over 12 500 bursts is 6.3 per
burst — 8.4 per REMOTE burst — where one pop round-trip should cost one. Other
hop sites in the consume path are unaccounted for. Finding them needs a hop
counter, not another curve; building the fix (pipelining the pop hop the way
gAmqpPushStage already pipelines publishes) on the present model would be
building on a model that does not add up.

The structural answer, if locality is confirmed as the cause, is to remove the
handoff rather than pay for it: run the delivery loop on the thread that owns
the queue, or place the queue on the consumer's shard. The second changes shard
placement and must not be done without asking.
