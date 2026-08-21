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

## 2b routing — session end 2026-07-22 (next: phase 2.5b)
Landed today on `sharding` (576/0, sweep @ shards=4 all green: keyspace 46/0, scan 21/0,
string 104/0, incr 31/0, other 26/0):
- `aa2cc77` — OBJECT ENCODING/FREQ/IDLETIME/REFCOUNT + MEMORY USAGE route by arg[2]
  (container block in `acl.d commandRouteSlot`); RANDOMKEY merge fixed firstNonNil→**randomNonNil**
  (was biased to the lowest-indexed shard); `blackbox/valkey-shard.skip` = the multishard skip list.
- `ee4d92c` — **Phase 2.5 stream routing**: XGROUP/XINFO route by arg[2]; XREAD/XREADGROUP were
  already routed (buildMultiKey + forEachCommandKey STREAMS-token). Stale "Phase 2.5 TODO" comment
  in `aclkeys.d` corrected.

**Skip-file gotcha (cost a cycle):** regex form is `/pattern` — leading `/` ONLY; a trailing `/`
is taken literally (`search_pattern_list` strips index 0). Names carry a `{$type}` prefix
(`{standalone} SCAN ...`) so use the regex form to match through it.

**[DONE 2026-08-19, `921825c`] phase 2.5b — BLOCKING commands under sharding.**
Design that landed: hop the blocking command to the key-owner shard and park it THERE —
the owner's drain spawns a fiber per blocking hop (`HOP_BLOCKING` bit 15) that runs the
EXISTING block machinery (gWaiters FIFO / gKeyActivity / blockWait) against a synthetic
`Conn` (`remoteBlock=true`); the requester waits synchronously, cancelling via the atomic
`ShardPending.cancel` handshake (+ `ShardMsg.blockKick` for instant XREAD cancel). KEY
LESSON: a remote park must wait the FULL slice on gKeyActivity — re-waking on a poll tick
re-registers and shuffles the event's FIFO waiter order (fairness break, caught by the
"reprocessing command" test). Pre-existing bugs fixed en route: owner drain now runs the
write-tail (gKeyActivity/signalReadyKeys — without it even local blockers never woke on
hopped writes); `HOP_RESP3` bit 14 carries the requester's protocol (RESP3 nil/double
forms were wrong for EVERY hopped command) + per-conn gRespProto restore on block wakes
(latent single-shard bug); xreadBlock's static-TLS buffers made per-call; gBlockedClients
is a shared atomic; CLIENT UNBLOCK broadcasts (sumInt) since conn registries are per-router.
Sweep @ shards=4 ALL green incl. list 269/0, zset 319/0, stream 71/0, cgroups 50/0;
shards=8 spot green; shards=1 parity intact; 576/0.
WATCH: one 1-in-10 hang of unit/type/list at "Unblock fairness is kept while pipelining"
(shards=4, full-matrix run only; 9 subsequent runs + a 200-iteration direct repro all
clean — if it re-appears, start there). Skip file gained (D) stats-not-aggregated,
(E) MULTI/EXEC/WATCH-under-sharding, (F) valkey-9.1.0 drift categories.
NOTE: /tmp/valkey battery setup = valkey-9.1.0 tarball + `make MALLOC=libc valkey-server`,
symlinked to /tmp/valkey (it lives in a session scratchpad — re-create after reboot).

## 2.5c/2.5d — session 2026-08-19 (SAME session continued): ALL of (c), (d), (e) LANDED

- `5534643` **(c) cross-shard pub/sub + per-shard maintenance**: PUBLISH/SPUBLISH broadcast
  (sumInt, gated by the shared atomic `gSubTotal` — zero subscribers ⇒ :0 with no hop);
  keyspace notifications / script publishes fan out via `ShardMsg.pub`; PUBSUB introspection
  aggregates (unionArr/sumPairs/unionCount merges — NUMPAT ships pattern NAMES, a count
  cannot be deduped). Pre-existing fixed: the drain never flushed pending notifications
  (losses + stale-backlog dupes); active expire/eviction swept gDbs from the main thread
  ONLY (nothing was reaped on any shard under sharding!) → per-shard 200ms/1s maintenance
  timers over `myDbSlice()`; `gNotifyDb` was __gshared (cross-shard db-name races) → TLS.
- `b7a4a5e` **(d) INFO/stats aggregation**: INFO broadcasts, `infoMerge` sums per-shard TLS
  scalars, unions cmdstat_/errorstat_/dbN lines with fields summed (policy list in
  `mergeInfoTexts`). The drain now counts commandstats/errorstats on the OWNER (hopped
  commands were counted NOWHERE). CONFIG RESETSTAT broadcasts. CLIENT UNBLOCK honours the
  Redis contract (unblock has HAPPENED at :1): direct cancel write + kick + DEFERRED :1 via
  a watcher fiber (`ShardPending.genq` detects slot reuse). KEY LESSON: cross-LANE ordering
  in the SPSC fabric is NOT FIFO — two shards' messages to a third can drain in either
  order; any "A must be visible before B" contract needs the reply chain, not lane luck.
- `1913718` blocking-in-MULTI/EXEC serves its one-shot form ON THE OWNER (`HOP_NOBLOCK`).
- `6fda4b9` **(e) same-slot MULTI/EXEC ships as ONE atomic unit** (`ShardMsg.execBatch`):
  owner drain executes sections back-to-back with no yield, write-tail + notify flush once
  at the end (the "reprocessing" contract); DEBUG SLEEP allowed (sleeps the owner thread);
  falls back to per-command replay for mixed owners / server-layer / restricted-ACL.

**Sweep: ALL TEN suites green** @ shards=4 AND shards=1 (keyspace 46, scan 24, other 26,
pubsub 34, incr 31, string 105, list 276, zset 321, stream 71, cgroups 51 — full baseline
counts), spot-green @ shards=8. 576/0, LDC release ok.

**Remaining correctness edges (all documented in blackbox/valkey-shard.skip):**
(E) WATCH visibility (per-shard gWriteEpoch — a shared atomic would put a contended line
on the per-write hot path; needs a per-slot/per-shard epoch check design), CLIENT LIST
aggregation (1 test, upstream harness bug in its fail path), (F) valkey-9.1.0 drift
(2 list-encoding tests + background-expire notification — fail at shards=1 too).
Also still open globally: AOF under sharding (hopped writes never AOF — pre-existing),
cross-shard client tracking, scripts/MIGRATE across shards, the ACL-on-owner hop contract.

**[DONE 2026-08-20, `7986b90`] perf re-adjudication after the correctness campaign** (on
the 3950X fastbox, governor was POWERSAVE — official re-baseline still pending): instr/op
P=64 SET: shards=1 2834→**2738 (−3.4%, now −6.5% vs master!)** — the local write-tail
emitted gKeyActivity on EVERY write; a TLS `tKeyWaiters` gate (bracketed by
waitForActivity; safe on the cooperative loop) wakes only live waiters. shards=2
3246→3327: was 3473 — fixed a seq-cst cancel store in acquireShardPending (implicit XCHG
per hop → relaxed) and isBlockingHopForm's per-keyed-command string switch (→ CTFE
`gCmdBlockingHop` opcode table); the remaining +81/op is the owner-side stats/write-tail/
notify work the drain now legitimately does. Powersave ladder: 1→1.61M · 2→2.30M(71%) ·
4→4.20M(65%) · 6→5.03M(52%) · 8→5.72M(44%).

**NEXT — the structural perf levers, on a performance-governor session:** (1) re-baseline
the ladder officially; (2) the bytecode IR (kills commandRouteSlot 7-8% + owner re-parse +
string dispatch — see "The big structural idea"); (3) the hashtable (Keyspace.lookup ~23%
at s2); (4) vibe-core logTrace ~2-3% sits in the read path of BOTH builds — a vibe build
with compiled-out trace logging is a cheap candidate. The old plan below for context:

Original 2.5b→2.5c note kept below for context:
OLD — phase 2.5b = BLOCKING commands under sharding. The stream suites (baseline shards=1:
71/0 + 51/0) fail under shards>1 ONLY on `XREAD`/`XREADGROUP` with `BLOCK`: the client parks on the
ROUTER shard, but the wake (XADD) fires on the KEY-OWNER shard and never reaches the parked client →
NOGROUP / timeout. Same root cause as the v1-scope "blocking commands (BLPOP family)" gap below —
now the concrete workstream. Design sketch: hop the blocking command to the owner and park there
(the connection's reply path must reach the owner), OR make the block registry shard-aware.
Also still open: (c) **pub/sub cross-shard** (broker-critical — subscriber on router shard never
sees a PUBLISH/keyspace-notification fired on the owner shard; keyspace-notif tests HANG, skipped),
(d) **INFO keyspace aggregation** (each router reports only its shard's key count).

## Files
`source/dreads/shard.d` (SPSC transport, ShardInbound class, routing math),
`source/dreads/server.d` (shardDrainLoop / shardFire / flushShardPending / serve-loop hooks /
shardThreadEntry / startShards), `source/dreads/acl.d` (`commandRouteKey` + CTFE table),
`source/dreads/alloc.d` (per-shard allocator array), `config.d` (`shards N`, default 1).
Design doc: `SHARDING.md`. Run: `./bin/dreads <port> --shards N`.
