# Sharding — session handoff (continue on the fast machine)

Branch **`sharding`**, tip **`a4ea4f1`**. Everything below is committed and green:
DMD `dub test` 576/0, LDC release builds, sharded is correct + crash-free under a
c=16 hammer. This PC is too noisy to measure the last stretch — pick up on the fast box.

## Where we are

The thread-per-shard hop (the day-1 architecture — single-thread was the interim) is
**built and scaling**. A dumb-client cross-shard hop first COLLAPSED below single-thread
(4 shards ≈ 0.85×). Rebuilt share-nothing → **per-shard efficiency 40% → ~75%**, and it's
now roughly CONSTANT across N (not decaying), which is the whole game:

| shards | SET (server-bound, HT-pinned) | vs 1× | per-shard |
|--------|-------------------------------|-------|-----------|
| 1 | ~1.04–1.16M | 1.00× | — |
| 2 | ~1.60M | 1.54× | **77%** |
| 3 | ~2.32M | 2.22× | **74%** |

Target (USER, Amdahl + multi-router, no central bottleneck): **85–90% per shard**,
constant, then scale N until the hardware jam (memory bandwidth). Beating Dragonfly =
constant degradation × max shards until the jam.

## What killed the "re-enqueue" (commit a4ea4f1), each measured

1. **Share-nothing SPSC transport** (`shard.d`). Was ONE MPSC inbound queue + per-shard
   **mutex** + cross-thread ManualEvent wake (perf: **~12% futex**). Now: per-pair SPSC
   ring lanes — `gInbound[dst].lanes[src]`, only thread `src` writes it, only `dst` reads
   it → no lock, no CAS on a shared cursor. `ShardInbound` is a **class** with `waitData`/
   `wake` **methods** (waitUninterruptible on `this`, like `raftq.CrossQueue`). The only
   cross-thread signal is one per-shard wake event, emitted **batched** (once per pipeline
   batch, and only if the consumer actually parked — under load it never parks).
2. **Share-nothing allocators** (`alloc.d`). `gDataAlloc`/`gConnAlloc` were ONE `__gshared`
   instance hammered by all shard threads → freelist corruption → **SIGSEGV**. Now a
   `__gshared` ARRAY indexed by a TLS shard id (`gAllocShard`, set in `shardThreadEntry`):
   each shard its own freelists + counters. Array (not a TLS *instance*) on purpose — see
   the DMD trap below.
3. **No blocking wait — "é task"** (`server.d`). A keyed command is FIRED at its owner
   without blocking; its `ShardPending` is recorded in command order and reaped at the next
   flush point (batch end / `PIPELINE_CAP` / before any inline-reply command). All shards
   run their slice concurrently; the connection only ever blocks at the batch boundary.
4. **Self-queue fast-path**. A key owned by the connection's own shard skips the hop: runs
   inline (full ACL/stats if nothing is queued ahead, else an in-order ready slot).
5. **O(1) routing** (`acl.d` `commandRouteKey`). `shardOwnerOf` used `getCommandKeys`' linear
   keyspec scan (**~7%**). Now a compile-time first-key-position table indexed by
   `aclCmdIndex`; dynamic/keyword commands fall back. Routes multi-key by first key.
6. **Zero-copy drain** (`server.d shardDrainLoop` + `shard.d shardDrainOnce!fn`). The owner
   parses/dispatches straight from the ring slice and the reply is written directly into the
   requester's pending — no `ring→buf→pending→o` intermediates (halved the parse cost).

## The DMD dip1000 trap (READ before touching serve-path attributes)

`dub test` builds with `-preview=dip1000` (from the unit-threaded/fluent-asserts deps); the
main build does not. Under dip1000, vibe-core 2.14.0's `waitForDataEx`/`ManualEvent.wait`
fail `@safe`/scope inference **if a serve-path helper is @safe-inferred and forced inline**.
Fix that worked: **removed `pragma(inline,true)` from `sharded()` and `myKeyspace()`**
(LDC still inlines them — objdump confirms 0 call sites, so NO single-thread regression).
Do NOT "fix" this by scattering `@system` — USER rule: **@pure > @safe > @trusted > @system,
strongest possible, every @system justified** (the optimizer works better with stronger
guarantees). If a new serve-path helper re-trips it, drop its `pragma(inline)`, don't downgrade
safety.

## Benchmark methodology (CRITICAL — the noise that fooled this PC)

- **Run `lscpu` first on the fast box** and map HT siblings. On this i7-9750H: 6 physical
  cores, `cpu N ↔ cpu N+6` are siblings. Server pinned to 0-3 and clients to 6-9 were
  FIGHTING for the same physical cores → garbage numbers. **Server and clients MUST be on
  disjoint physical cores.**
- **`redis-benchmark` is single-threaded per process** (~1.0–1.16M/core ceiling). One client
  cannot saturate a multi-shard server. Use **N PARALLEL `redis-benchmark` procs**, each
  pinned to its own client core, and SUM the rps.
- **`-P 16`** (pipeline 16) is the methodology, `-c 25`-ish per client, `-r 200000` keyspace
  (must spread keys across shards — a single key routes to ONE shard and measures nothing).
- Warmup one run, take median/best of a few. `pkill -9 -x dreads` between runs; watch for
  "port already held" (a stale instance serves your requests and lies).
- The arbiter for zero-cost/regression is **perf instructions/op** (deterministic), not noisy
  throughput. `perf record -g --pid <srv>` during load to find hot spots.
- shards=1 must equal the pre-sharding baseline (measured zero-cost rule). USER expects
  single-thread ≈ **1.55M** on a good box; this PC only showed ~1.16M (client-bound + noise).

## Next levers (to close 75% → 85-90%)

Current flat profile (shards=3): `Keyspace.lookup` 9.4% (real work), memcpy 6.8%,
`parseValue` 6.2% (half is the **owner re-parse**), **`shardOwnerOf` 4.25%**, syscalls ~6%,
drain 3%, dispatch 3%, fnv1a 2.6%. No single big bottleneck — it's a grind of 2-3% items:

- **owner re-parse (~3%)** — the command is parsed on the requester AND re-parsed on the owner.
- **`shardOwnerOf` (~4%)** — lowercase + table lookup, per command.

### The big structural idea (USER, "just thinking" — not yet greenlit): a RESP **bytecode IR**
Compile the wire command to a compact bytecode: **db + key FIRST (fixed offset), command as a
byte (opcode), then args.** This kills all three residual taxes at once:
- db+key at a fixed offset → routing is a read, no `getCommandKeys`/classification (kills `shardOwnerOf`);
- opcode → jump-table dispatch, no string-switch/`aclCmdIndex`/uppercase loop;
- owner executes bytecode → **no re-parse** (compile once on the requester, execute on the owner = 1 parse total).
- Bonus: it's the **IR every frontend targets** (RESP/MQTT/AMQP/Kafka compile to the same
  bytecode; the shard is skin-agnostic — the "RESP is one skin" vision made concrete). May
  also lift single-thread (opcode dispatch > string switch) toward the 1.55M.
- Watch: variadic commands (MSET/ZADD N), keyless, forward-compat opcodes, and its relation to
  the AOF/Raft log (could store bytecode directly — "AOF is ours" — but then it's a persistence
  contract to version). Compile runs on the requester = distributed across the N routers (Amdahl-fine).

Compilation happens in the parse loop; forward the compact bytecode straight into the SPSC lane
(keeps the zero-copy drain). This is judged the RIGHT next structural step — more leverage than
any remaining micro-opt.

### Also on the list
- **ACL executes on the SHARD** (USER directive). Today hopped commands do a bare `dispatch`
  (no ACL/stats on the owner — a v1 gap; the requester's `executeCommand` enforces ACL before
  `shardFire`). Proper design: carry the requester's user (id/ptr) through the hop, enforce ACL
  on the owner's drain → distributes ACL work (Amdahl) and lets restricted users use the fast path.
- **thread-affinity per shard** (USER directive): pin shard `i`'s worker thread to core `i`
  (`pthread_setaffinity_np`), shard 0 = main thread on core 0. Not yet done.
- **early-shard was TRIED and REVERTED** — routing before `executeCommand`'s preamble measured
  NEUTRAL-to-slightly-negative (the preamble isn't the bottleneck; the guard + double-route for
  self/keyless cost as much as it saved). The bytecode IR is the version of this idea that wins.

### Out of scope in v1 (correctness gaps, documented)
MULTI/EXEC, WATCH, blocking commands (BLPOP family), and Lua scripts across shards. They run on
the connection's own shard today (can be wrong if the key lives elsewhere). Windows has no
SO_REUSEPORT → sharding is forced off there (single-thread), by design.

## Files
`source/dreads/shard.d` (SPSC transport, ShardInbound class, routing math),
`source/dreads/server.d` (shardDrainLoop / shardFire / flushShardPending / serve-loop hooks /
shardThreadEntry / startShards), `source/dreads/acl.d` (`commandRouteKey` + CTFE table),
`source/dreads/alloc.d` (per-shard allocator array), `config.d` (`shards N`, default 1).
Design doc: `SHARDING.md`. Run: `./bin/dreads <port> --shards N`.
