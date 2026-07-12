# dreads ⚡

**A performance-focused, Redis-compatible alternative, in D.** An in-memory data
store built around three commitments: zero GC in the data plane, arena memory,
and one purpose — speed. It speaks RESP2/RESP3 and matches Redis/Valkey on the
supported command surface (see [DRIFT.md](DRIFT.md)) — it is *not yet* a verified
drop-in replacement; full blackbox compatibility against a live Valkey oracle is
still in progress.

```
⟜ Ultra-light. Thread-isolated DBs.
⟜ Arena memory. Zero-GC overhead.
⟜ Geo indexing. Custom types. One purpose: Speed.
```

## Origin

dreads started as an experiment with a single question: **would D's fibers give
a real edge for a Redis-like server?** Redis is single-threaded and bound on
multiplexing many connections; D's lightweight fibers on a single-threaded
event loop (vibe-core) promised a fiber-per-connection model without the weight
of OS threads. The numbers below are what that experiment grew into once the
answer looked like *yes* — the zero-GC data plane, arena memory, and the Raft
log all came from taking that first result seriously.

## Why

Competitive with real Redis, measured with `redis-benchmark` against the
official Redis 8.8 image (Docker, host network, persistence off), LDC release
build with jemalloc:

| Command (`-P 16`, 50 conns) | dreads | Redis 8.8 | |
|---|---|---|---|
| SET | **1.20M rps** (p50 0.31 ms) | 862k (0.67 ms) | 1.4× |
| GET | **1.32M** (0.31 ms) | 1.16M (0.52 ms) | 1.1× |
| LPUSH | **1.20M** (0.40 ms) | 1.04M (0.61 ms) | 1.2× |
| ZADD | **1.40M** (0.38 ms) | 905k (0.71 ms) | 1.5× |

Unpipelined throughput is on par (~95–100k rps — both sides are round-trip
bound); the pipelined numbers show the real per-command cost.

## How it's fast

- **Zero-GC data plane.** The RESP parser, every data structure, the command
  dispatch and the AOF are `@nogc nothrow`, compiler-enforced. Memory is
  malloc/jemalloc plus a per-connection region **arena** that is reset after
  each command. The D GC is disabled at startup and nothing in the request
  path can allocate on it.
- **Zero-copy parsing.** Commands are parsed as slices into the connection
  buffer; incomplete input is a status, not an exception.
- **Real data structures.** Open-addressing hash tables (FNV-1a, tombstones),
  intrusive doubly-linked lists, and a skiplist with per-level spans for
  O(log n) ZRANK — the same layout Redis uses.
- **vibe-core front-end.** Fiber per connection on a single-threaded event
  loop; fibers and connections are recycled, so steady state allocates
  nothing.

## Features

- **222 of Redis's 241 core commands** (see [DRIFT.md](DRIFT.md) for the
  honest gap list and semantic differences — the 19 missing are mostly
  cluster/replication, which Raft will replace). All data types including
  streams with consumer groups; GEO (geohash-scored zsets, Redis-exact
  outputs); bitmaps with `BITFIELD`; HyperLogLog; TTL/expiration with the
  full `SET` option set; `SCAN` family; `SORT`, `LCS`, `OBJECT`;
  transactions (`MULTI`/`EXEC`/`WATCH`); **blocking commands** (`BLPOP`,
  `BLMOVE`, `BZPOPMIN`, `XREAD BLOCK`, ...) on fiber wakeups; `MONITOR`;
  `maxmemory` with sampled LRU eviction (jemalloc-exact accounting);
  redis.conf-style configuration with live `CONFIG GET/SET`; and Redis's
  exact error messages (`WRONGTYPE`, arity, `NOSCRIPT`, `BUSYGROUP`, ...).
- **Pub/Sub**: `SUBSCRIBE`/`UNSUBSCRIBE`/`PSUBSCRIBE` (glob patterns)/
  `PUBLISH`/`PUBSUB`, with subscribe-mode command gating.
- **Lua scripting**: `EVAL`/`EVALSHA`/`SCRIPT LOAD|EXISTS|FLUSH` on the
  system Lua 5.4 with a malloc-backed allocator (Lua's GC never touches the
  D GC). `redis.call`/`pcall`/`status_reply`/`error_reply`, `KEYS`/`ARGV`,
  Redis conversion rules.
- **Persistence (AOF)**: `--appendonly[=path]` logs write commands as raw
  RESP, `fflush` per batch, `fsync` every second, replay on boot. Commands
  that depend on time or randomness are logged **resolved** — `EXPIRE`
  becomes an absolute `PEXPIREAT`, `XADD *` carries the generated ID,
  `EVALSHA` becomes `EVAL`, `SPOP` becomes `SREM` — so replay is
  deterministic. This same command log is the substrate for Raft
  replication (see `vendor/raft`).

## Build & run

Requirements: a D compiler (LDC recommended for release), dub, liblua 5.4,
and on Linux jemalloc (linked automatically).

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

100 tests via [unit-threaded](https://github.com/atilaneves/unit-threaded)
with [fluent-asserts](https://github.com/gedaiu/fluent-asserts), including a
storage-recovery suite that replays the AOF into a fresh keyspace and
requires byte-identical replies, and crash-recovery verified live with
`kill -9`.

## Architecture

```
source/dreads/
  mem.d        ByteBuffer (malloc) + Arena (region allocator)   @nogc
  resp.d       RESP2 zero-copy parser + encoder                 @nogc
  dict.d       open-addressing Dict!V (keyspace, hash, set)     @nogc
  list.d       doubly-linked list, inline payload               @nogc
  zset.d       skiplist with spans + score dict                 @nogc
  stream.d     stream entries, binary-searched ranges           @nogc
  obj.d        RObj tagged union + typed Keyspace (TTL-aware)   @nogc
  commands.d   dispatch, ~90 commands, deterministic propagation @nogc
  pubsub.d     channel/pattern registry, sink-based delivery
  scripting.d  Lua bridge (EVAL/EVALSHA/SCRIPT), SHA1 cache
  aof.d        append-only file: log, fsync policy, replay
  server.d     vibe-core TCP front-end (fiber per connection)
vendor/raft/   Raft consensus for D (git submodule; in progress)
```

## Roadmap

- **Raft replication** — the deterministic command log is designed to be the
  replicated log; `vendor/raft` holds the library skeleton (vibe-core based).
- **GEO commands** (geohash over the existing zset), MULTI/EXEC
  transactions, zset algebra, bitmaps, blocking ops (`BLPOP`,
  `XREAD BLOCK`), stream consumer groups, AOF rewrite/compaction,
  thread-isolated DB shards. Full prioritized gap list: [DRIFT.md](DRIFT.md).

## License

MIT © Marcelo Aires Caetano
