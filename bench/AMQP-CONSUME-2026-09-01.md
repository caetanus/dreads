# CORRECTION (2026-09-02): AMQP consume DOES scale with shards

Everything below this section about retrograde scaling was measured through
RabbitMQ PerfTest, and **PerfTest was the bottleneck, not the broker.** The repo
already ships a native load generator, `bench/amqpload.d` — the redis-benchmark
of the AMQP skin — and it was not used. It should have been.

    single subscriber, one shard:   amqpload 4.88M msg/s     PerfTest ~0.9M

Re-measured with the native client (8-32 queues, one subscriber each, 16-byte
messages, consume-only drain of a pre-filled backlog, server pinned to cores
0-7, clients to 8-15+24-31):

| shards | rate | speedup | cores |
|---|---:|---:|---:|
| s1 | 4.77M | 1.00 | 0.98 |
| s2 | 6.80M | 1.43 | 1.71 |
| s4 | 8.9-12.9M | 1.9-2.7 | 2.5-3.3 |
| s8 | **13.8M** | **2.91** | 4.4 |

That is an Amdahl curve — rising, then flattening — not a retrograde one. It
implies a serial fraction around 28%. A single shard saturates its one thread at
4.74M msg/s (0.97 cores, 0.5% run-to-run spread), and eight shards reach 13.8M.
The earlier conclusion, "--shards 1 is the fastest configuration for a
consume-heavy AMQP workload", is WRONG and is retracted.

## What the old numbers actually measured

PerfTest capped near 900k msg/s, about 20% of what one shard can deliver. Every
figure in the sections below was taken with the broker mostly idle, being
trickled by a slow client. In that regime the analysis is still literally true —
more shards do mean more threads waking for a trickle, the context-switch
counting is correct, 5.1 switches per hop is correct — but it describes the cost
of *coordinating a trickle*, not how the broker scales under load. The
retrograde throughput curve was the client saturating, and nothing else.

Caveat, stated because it matters: `amqpload sub` consumes with `no_ack=true`,
so it exercises delivery WITHOUT the ack and unacked-window path that PerfTest's
manual-ack drain exercises. The two are not the same workload. What is
established is that delivery scales positively with shards; whether the
manual-ack path does too is not yet measured, and needs an acking mode in
amqpload.

## The spin gate, re-judged under real load

`331a673` gates the drain spin on how much the previous pass drained. It was
tuned in the trickle regime. Re-measured with the native client at 16 queues:

| --shards | full spin (4096) | gated (shipped) |
|---|---:|---:|
| 4 | 11.63M / 2.05 s | 10.32M / 1.83 s |
| 8 | 13.73M / 2.92 s | 12.40M / 2.32 s |

The gate costs ~10% of throughput under load and saves the idle burn (7.96 cores
at s8 with zero messages, which is real and independent of client speed). Both
alternatives tried — hysteresis on the batch signal, and gating on delivered
RATE instead — were measured and neither beat the shipped gate in both regimes,
so neither was kept. Making the policy a config directive, so an operator with
dedicated cores can choose the full spin, is the open item.

## Method note for whoever reads this next

Use `bench/amqpload.d`, built with `ldc2 -O2 -release`. PerfTest is fine for
CONFORMANCE and for ack-path behaviour; it cannot measure this broker's
throughput. Before reporting any ceiling as the broker's, check `cpu/wall` from
`/proc/<pid>/stat`: under ~1.0 core per shard thread the broker is not the
limit.

---

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

## The arithmetic, closed with a hop counter

A first pass at this could not make the numbers add up: 78 831 switches over an
assumed 12 500 bursts is 6.3 per burst, where one pop round-trip should cost
one. The assumption was wrong, not the model. Counting AMQP keyspace ops
directly (local vs hopped, bucketed by command) gives, for one 800k drain at
--shards 4:

    lpop  local=3 273  remote=13 606      (80.6% remote)
    llen  local=6      remote=26

16 879 pops for 800k messages is 47 messages per pop — the 64-deep batch with
partial fills — and **69 677 context switches over 13 606 remote hops is 5.1 per
hop**. There are no hidden hop sites. One round trip costs ~5 switches: the
asking fiber parks, the owner's drain loop wakes and re-parks, the asker's drain
loop wakes and re-parks. 13 606 x 5.1 = 69 400 against 69 677 measured.

## Spin and park are one bill, paid two ways

| --shards 4 | rate | CPU | ctx switches/msg |
|---|---:|---:|---:|
| fixed spin 4096 | 659k | 4.05 s | 0.001 |
| batch-gated spin (shipped) | 723k | 2.59 s | 0.090 |
| *s1 for reference* | *755k* | *1.00 s* | *0.001* |

Spinning removes the switches completely — 0.001/msg, the same as a single
shard — and costs more CPU than the switches did. Sweeping the ceiling
(0/32/128/512/4096 with the gate) lands everything between 2.7 and 3.1 s: there
is no tuning that gets both. Neither policy comes near s1's 1.00 s, because both
are ways of PAYING for the handoff rather than removing it.

## A fix that was tried and rejected

Pipelining the pop — fire the next batch's hop before delivering the current
one, the way gAmqpPushStage already pipelines publishes — was implemented and
measured. Median of three, --shards 4: 644k -> 677k msg/s with 16 consumers,
542k -> 568k with 32. About 5%, which is INSIDE this harness's +/-10% run-to-run
spread: three samples cannot demonstrate it works.

It was reverted. The cost was a second batch held off the queue that every one
of the consumer fiber's exits must give back — the exact defect class that lost
63 messages earlier the same day — and the tight-qos guard means the new
give-back path is not even exercised by the regression test that covers the
first batch. Not a trade worth making for an effect smaller than the noise.

## The ceiling is not measurable on this box

The obvious way to price the structural fix is to emulate perfect locality: run
N separate dreads processes, one shard and one core each, each owning its own
queues, and see whether N of them deliver N times one. That experiment cannot be
run here. RabbitMQ PerfTest — a JVM client — tops out around 900k msg/s on this
machine (the highest figure observed across the whole campaign is 924k, from a
SINGLE shard with 32 consumers). Four locality-perfect brokers would have to be
driven past 3M msg/s to show 4x, so the client would cap the result long before
the broker did, and any number it produced would be the client's.

Pricing the fix needs a C-level AMQP load generator, the way redis-benchmark is
for RESP. None is installed here (no rabbitmq-c tools), and paho's Python client
is two orders of magnitude too slow (measured 14.8k msg/s). Until one exists,
the size of the prize is unmeasured — the CAUSE above is proven, its price is
not.

## What would actually work

Remove the handoff instead of paying for it: run the delivery loop on the thread
that OWNS the queue, or place the queue on the consumer's shard. The second
changes shard placement and must not be done without asking. Until one of those
happens, AMQP consume does not scale with shards, and --shards 1 is the fastest
configuration for a consume-heavy AMQP workload.
