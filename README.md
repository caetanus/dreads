<p align="center">
  <img src="assets/dreads-logo.jpg" alt="dreads — a fast, reliable, in-memory data store, written in DLANG" width="360">
</p>

<h1 align="center">dreads ⚡</h1>

<p align="center"><b>A fast, reliable, in-memory data store — written in D.</b></p>

Redis-compatible, built around three commitments: zero GC in the data plane,
arena memory, and one purpose — speed. It speaks RESP2/RESP3 and tracks
Redis/Valkey on the supported command surface.

```
⟜ Ultra-light. 16 logical DBs on one event-loop thread.
⟜ Arena memory. Zero-GC data plane.
⟜ Raft-replicated log. Custom types. One purpose: Speed.
```

## Compatibility, stated honestly

**The goal is to be as close to 100% Redis-compatible as possible — the only
permanent exceptions are deliberate architectural divergences.** Everything a
client can observe on the wire is meant to converge on Redis/Valkey behaviour,
byte for byte; where dreads differs it is because a *design decision* makes it
differ, not because a command was left half-done. The line between "not done
yet" (a bug we will close) and "divergent by design" (an exception we own) is
tracked mechanically in **[DRIFT.md](DRIFT.md)** — that file, not this README,
is the source of truth, and we never claim 1:1 parity without citing it.

The architectural exceptions — the things that will *stay* different:

- **All DBs live on one event-loop thread** (Redis's single-writer model, by
  choice), with a shared-nothing per-shard future rather than locks.
- **Replication is Raft consensus**, not the legacy async wire. `SYNC`/`PSYNC`/
  `REPLICAOF`/`min-replicas-*` are therefore no-ops or unsupported by design;
  durability comes from the committed log instead.
- **Scripts replicate their *effects*** (like Redis 7+): the `EVAL` never
  enters the log — each write it performs is logged as itself. Scripts also run
  on a **dedicated Lua thread** off the event loop, so a long/looping script
  keeps the loop responsive (a client gets `PONG`, never `-BUSY`).
- **The append log is dreads' own format.** Redis parity means the
  client-visible surface; the log only owes deterministic replay + compaction.
- **Persistence is the AOF/Raft log, not RDB** — so `DUMP`/`RESTORE`/`MIGRATE`
  and an RDB file format are out of scope until a documented serialization
  lands.

Everything else — commands, replies, error strings, encodings, RESP3 framing —
is a convergence target, and the live Valkey blackbox suite is the yardstick.

## Origin

dreads started as an experiment with a single question: **would D's fibers give
a real edge for a Redis-like server?** Redis is single-threaded and bound on
multiplexing many connections; D's lightweight fibers on a single-threaded
event loop (vibe-core) promised a fiber-per-connection model without the weight
of OS threads. The zero-GC data plane, arena memory, and the Raft log all came
from taking that first result seriously once the answer looked like *yes*.

## Why — the numbers

Competitive with real Redis, measured with `redis-benchmark` against the
official Redis 8.8 image (Docker, host network, persistence off), LDC release
build with jemalloc:

| Command (`-P 16`, 50 conns) | dreads | Redis 8.8 | |
|---|---|---|---|
| SET | **1.20M rps** (p50 0.31 ms) | 862k (0.67 ms) | 1.4× |
| GET | **1.32M** (0.31 ms) | 1.16M (0.52 ms) | 1.1× |
| LPUSH | **1.20M** (0.40 ms) | 1.04M (0.61 ms) | 1.2× |
| ZADD | **1.40M** (0.38 ms) | 905k (0.71 ms) | 1.5× |

One core does ~1.39M ops/s; because the model is single-threaded-per-shard,
scaling is horizontal (two shards → ~2.51M measured with parallel plain
clients). Unpipelined throughput is round-trip bound on both sides (~95–100k
rps); the pipelined numbers show the real per-command cost.

## How it's fast

- **Zero-GC data plane.** The RESP parser, every data structure, command
  dispatch and the AOF are `@nogc nothrow`, compiler-enforced. Memory is
  malloc/jemalloc plus a per-connection **arena** reset after each command. The
  D GC is disabled at startup and nothing in the request path allocates on it.
- **Zero-copy parsing.** Commands are parsed as slices into the connection
  buffer; incomplete input is a status, not an exception.
- **Real data structures.** Open-addressing hash tables (FNV-1a, tombstones),
  intrusive doubly-linked lists, a skiplist with per-level spans for O(log n)
  ZRANK, plus **LLVM-style small containers** (contiguous array + linear scan
  that promotes to the full structure past a threshold) so small sets/hashes/
  zsets cost far less memory than a full dict.
- **vibe-core front-end.** Fiber per connection on a single-threaded event
  loop; fibers and connections are recycled, so steady state allocates nothing.

## Features

- **222 of Redis's 241 core commands** — see [DRIFT.md](DRIFT.md) for the
  honest gap list and every semantic difference. All data types including
  **streams** with consumer groups; **GEO** (geohash-scored zsets, Redis-exact
  outputs); **bitmaps** with `BITFIELD`; **HyperLogLog**; TTL/expiration with
  the full `SET` option set and opt-in active expiry; the `SCAN` family;
  `SORT`, `LCS`, `OBJECT ENCODING`; **16 logical databases** (`SELECT`,
  `SWAPDB`, `MOVE`, per-connection keyspace); transactions
  (`MULTI`/`EXEC`/`WATCH`); **blocking commands** (`BLPOP`, `BLMOVE`,
  `BZPOPMIN`, `BLMPOP`, `XREAD BLOCK`, ...) on fiber wakeups; `MONITOR`;
  `maxmemory` with sampled LRU eviction (jemalloc-exact accounting);
  redis.conf-style config with live `CONFIG GET/SET`; and Redis's exact error
  strings (`WRONGTYPE`, arity, `NOSCRIPT`, `BUSYGROUP`, ...), audited against
  Valkey source.
- **RESP2 and RESP3.** `HELLO 2`/`HELLO 3` negotiate per connection; the RESP3
  types (null, boolean, double, big number, verbatim, map, set, push) are
  emitted through a proto-aware reply oracle, and pub/sub confirmations/messages
  are framed as Push under RESP3. RESP2 clients are byte-unchanged.
- **Pub/Sub**: `SUBSCRIBE`/`PSUBSCRIBE` (glob) / `PUBLISH`/`PUBSUB`, shard
  pub/sub, subscribe-mode command gating, and **keyspace notifications**
  (`__keyspace@N__` / `__keyevent@N__`).
- **Lua scripting** — system Lua 5.4 with a malloc-backed allocator (its GC
  never touches the D GC), running on a **dedicated thread** off the event loop:
  - `EVAL`/`EVALSHA`(`_RO`), `SCRIPT LOAD|EXISTS|FLUSH|SHOW`, and Redis
    **Functions** (`FUNCTION LOAD|...`, `FCALL`/`FCALL_RO`);
  - `redis.call`/`pcall`, `cjson`, `cmsgpack`, `bit`, `redis.sha1hex`,
    `redis.setresp`, `redis.set_repl`, a Lua 5.1 compat layer, and RESP3
    replies inside scripts;
  - a real sandbox — curated libraries, `_G` protected against global
    creation/undefined reads, throwaway `_ENV` per run, `lua-time-limit` and
    `lua-memory-limit` (with working `SCRIPT KILL`);
  - the `#!lua flags=...` shebang (`no-writes`, `allow-oom`, ...), the deny-oom
    contract enforced inside scripts, `SELECT` inside a script, a frozen
    per-EVAL clock, and **effects replication** (each `redis.call` write reaches
    the log in its propagation form).
- **Persistence (AOF) + Raft**: `--appendonly[=path]` logs writes as RESP,
  `fflush` per batch, `fsync` every second, replay on boot. Time/randomness are
  logged **resolved** — `EXPIRE`→absolute `PEXPIREAT`, `XADD *`→generated ID,
  `SPOP`→`SREM` — so replay is deterministic. That same log is the substrate for
  **Raft replication** (`vendor/raft`): leader election, log replication,
  deterministic apply, failover, dynamic membership (joint consensus) and
  `InstallSnapshot` compaction, all verified on a live cluster.

## Build & run

Requirements: a D compiler (LDC recommended for release), dub, liblua 5.4, and
on Linux jemalloc (linked automatically).

```sh
dub build -b release --compiler=ldc2
./bin/dreads                       # port 6379
./bin/dreads 6390 --appendonly     # custom port + AOF persistence
redis-cli -p 6390 PING
```

Day-to-day builds go through [reggae](https://github.com/atilaneves/reggae)
(faster incremental builds):

```sh
reggae -b ninja && ninja
```

## Tests

```sh
dub test
```

Unit tests via [unit-threaded](https://github.com/atilaneves/unit-threaded)
with [fluent-asserts](https://github.com/gedaiu/fluent-asserts), including a
storage-recovery suite that replays the AOF into a fresh keyspace and requires
byte-identical replies, crash-recovery verified live with `kill -9`, and Raft
verified on a live multi-node cluster. Compatibility is measured against a live
Valkey server with the upstream **blackbox** suite (`blackbox/sweep.sh`); every
suite failure fixed gets its own internal unit test, and the by-design skips are
catalogued in `blackbox/valkey-sync.skip`.

## Architecture

```
source/dreads/
  mem.d        ByteBuffer (malloc) + Arena (region allocator)   @nogc
  resp.d       RESP2/RESP3 zero-copy parser + encoder           @nogc
  respvariant  RValue tree + lazy, zero-alloc reply oracle      @nogc
  dict.d       open-addressing Dict!V + StrVal tagged union     @nogc
  smallset.d   LLVM-style small containers (array → dict/skip)  @nogc
  list.d       doubly-linked list, inline payload               @nogc
  zset.d       skiplist with spans + score dict                 @nogc
  stream.d     stream entries, binary-searched ranges           @nogc
  obj.d        RObj tagged union + typed Keyspace (TTL-aware)   @nogc
  commands.d   dispatch, 222 commands, deterministic propagation @nogc
  det.d        the single injectable "now" (frozen per command)  @nogc
  notify.d     keyspace notifications (deferred publish)
  pubsub.d     channel/pattern registry, sink-based delivery
  scripting.d  Lua bridge on a dedicated thread; Functions; sandbox
  config.d     redis.conf parsing + live CONFIG GET/SET
  cluster.d    command flags / CLUSTER surface
  aof.d        append-only log: write, fsync policy, replay
  replicator.d Raft integration (cross-thread queues, apply loop)
  server.d     vibe-core TCP front-end (fiber per connection)
vendor/raft/   Raft consensus for D (git submodule)
vendor/emplace/ non-GC containers + RAII smart pointers (submodule)
```

## Roadmap

- **Sharding** — slot ranges (CRC16/16384) each owned by a Raft group: the
  single-machine shared-nothing thread-per-shard model, and where the
  `CLUSTER`/`MOVED`/`ASK` surface lands.
- **Closing the blackbox tail** — error-stats telemetry (`INFO Errorstats`/
  `Commandstats`), an ACL engine (`ACL SETUSER`/`AUTH`/`acl_check_cmd`), and the
  remaining semantic drift listed in [DRIFT.md](DRIFT.md).
- **Value serialization** — a documented `DUMP`/`RESTORE`/`MIGRATE` format.

## License

MIT © Marcelo Aires Caetano
```