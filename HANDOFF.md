# Handoff — read me first (assistant orientation)

You are continuing **dreads**, a GC-free, Redis/Valkey-compatible in-memory data
store in D. This file re-orients you after a `/clear`. Your persistent memory in
`~/.claude/projects/-home-caetano-lab-dreads/memory/` is the source of truth —
**read `MEMORY.md` (the index) and the linked files before asserting anything
about the project.** You keep losing project knowledge across handoffs; the user
has called this out repeatedly. Don't restate facts from training; verify.

## What dreads is (origin — memory `project-origin-handoff`)

The user built dreads solo for months (stopped at RESP3 + 3 datatypes, no
server) then handed it to you. Design: **vibe.d/vibe-core fibers, one fiber per
connection; all DBs on ONE thread** (thread-per-DB is a future design needing a
Lamport-ring lock-free IPC — the single-thread choice is DELIBERATE, not drift).
Data plane is `@nogc nothrow`, GC disabled, malloc/jemalloc + per-connection
arena. Replication is **Raft** (the AOF command log IS the raft log), so
**SYNC/PSYNC is a NOOP** — dreads is NOT "all in-memory", it HAS AOF persistence.

## Working rules (memory `dreads-code-style`, `no-parity-overclaims`, others)

- **Idiomatic D, never C-with-D-syntax.** `union` IS idiomatic (don't force
  SumType onto hot value types); but kill unnecessary C-isms: no `snprintf`
  (use `fmtLong` / a divide loop), no `memcpy` (slice copy `dst[]=src[]`; only
  `memmove` for overlapping). @safe surface, @trusted confined to pointer ops.
- **Tests are SEPARATE UT files** in `source/tests/*_tests.d` (unit-threaded
  named `@("group.name") unittest` + fluent-asserts), NOT inline in the module.
- **No moving forward without a benchmark.** Prove the hot path didn't regress;
  use `perf` for microbenchmarks. Benchmark methodology in memory
  `dreads-benchmark-method` + `small-collections-llvm` (hot-single-key is
  artificial; measure COLD random access; "O(n) in cache > O(1) in memory").
- **Before starting any test/bench server, wait for the TCP port to be free**
  (SO_REUSEPORT lets a stale dreads keep serving → flaky). `pkill -9 -x <comm>`
  (NOT `-f`, it self-matches the shell) then `while ss -tlnH|grep -q ":$PORT ";
  do sleep 0.2; done`.
- **Chat in pt-BR; code/comments/commits in English.** Valkey is a READ-ONLY
  reference (error strings from BSD-3 src, not CC-BY-SA docs).
- **Never overclaim parity** — DRIFT.md / BLACKBOX-TODO.md are the truth. Parity
  = real Redis USAGE, not internals. When the user states a design decision,
  write it to memory immediately (it's the only thing that survives handoff).
- Commit/push only when asked; never leave a dead (merged) branch — delete it.

## Where things are

- `source/dreads/`: server.d (vibe front-end), commands.d (dispatch, ~90 cmds),
  obj.d (RObj tagged union + Keyspace + `gDbs[16]`), dict.d (StrVal typed union
  + Dict alias to emplace.HashMap), smallset.d/smallhash.d/smallzset.d (the
  LLVM-style small containers), zset.d, resp.d, aof.d, replicator.d (raft),
  config.d, miscops.d, zsetops.d, bitmap.d, hll.d, scripting.d.
- `vendor/emplace` (submodule): GC-free C++-style containers + smartptrs
  (Vector has `~this`; HashMap uses `free()` — see the smartptr TODO).
- `vendor/raft` (submodule, = the `draft` package).
- Tests: `source/tests/*_tests.d`. Build: `dub build -b release --compiler=ldc2`
  (or reggae+ninja). Test: `dub test --compiler=ldc2` (currently 197, 0 failed;
  2 flaky TTL/notify tests pass on re-run).
- Docs: `TODO.md` (open work), `BLACKBOX-TODO.md` (Valkey suite gap map),
  `DRIFT.md` (parity truth), `bench/` (benchmark scripts + results).

## Recently done (this session, on master)

Multi-DB (16 dbs, per-conn `Conn.dbp`); RESP-typed `StrVal` union (int/embstr/
raw, native INCR, +1.2× vs Valkey on INCR); config encoding thresholds; DEBUG
subcommands; ZADD flags / COPY DB / SCAN TYPE; raft routes writes to their db;
**LLVM-style small containers** for set/hash/zset (contiguous blob + spill,
~-51% memory, real intset/listpack/hashtable/skiplist encoding, rock-solid
promotion); crash + ZINCRBY/ZRANK fixes.

## What to do next

Read `TODO.md`. The big items: (1) emplace smartptr/RAII + RObj-off-union
refactor, (2) finish the RESP3 value union (float/null/bigint), (3) blackbox
long-tail (RESP3 doubles, RANDFIELD randomness, HGETDEL, SYNC skip). A dedicated
**`blackbox` branch** exists for the long-tail. Ask the user which to pursue.
