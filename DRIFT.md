# Drift: dreads vs Redis

Honest, mechanically-derived gap analysis. Method: the canonical core command
list from the official docs repo (`redis-doc/commands.json`, 241 base
commands / 370 including subcommands) diffed against dreads' dispatch tables,
plus a hand-audit of semantic differences in commands we *do* implement.
Regenerate the diff by re-running the extraction (see git history of this
file) whenever the dispatch grows.

**Status: 225 of 241 base commands implemented; 16 missing.** Module
families (RedisJSON, RediSearch, Bloom, TimeSeries — bundled in the Redis 8
image) are out of scope entirely.

## Missing commands

| Family | Missing | Why |
|---|---|---|
| **cluster (4)** | cluster asking readonly readwrite | Excluded by owner decision — to be discussed (intersects Raft) |
| **replication (7)** | replicaof slaveof failover psync sync replconf restore-asking | Same: replication is the Raft roadmap (`vendor/raft`), not the legacy wire protocol |
| **serialization (3)** | dump restore migrate | Need an RDB-compatible (or documented custom) value serialization format |
| **hll debug (2)** | pfdebug pfselftest | Internal debug commands |

Also missing: hash-field TTLs (`HEXPIRE`/`HPEXPIRE`/`HTTL`/..., Redis 7.4+).

## Semantic drift in implemented commands

These exist but do not match Redis exactly:

- **SET**: full option set implemented (`EX/PX/EXAT/PXAT/NX/XX/KEEPTTL/GET`).
- **SRANDMEMBER/ZRANDMEMBER/HRANDFIELD**: real uniform draws (xorshift64*,
  reservoir sampling for the distinct-count form). **SPOP/RANDOMKEY** remain
  deterministic (first live slot). SPOP propagates as `SREM`, so
  persistence/replication are unaffected.
- **SCAN family**: cursor is a slot index; a concurrent rehash can miss or
  duplicate elements (Redis's reverse-binary cursor guarantees stability).
  ZSCAN is rank-based (ordered).
- **WATCH**: global write epoch — *any* write since WATCH aborts EXEC
  (stricter than Redis's per-key tracking). MULTI queues without validating
  commands, so unknown commands fail inside EXEC instead of at queue time;
  EXEC is not wrapped in a MULTI marker in the AOF (each write logs
  individually). Blocking commands inside MULTI degrade to one immediate
  attempt (Redis behaves the same).
- **Streams**: consumer-group PELs are volatile (not persisted by the AOF
  rewrite; `XGROUP CREATE` + last-delivered-id are). `XREADGROUP` skips
  deleted entries on re-delivery instead of returning nil fields; no inline
  `XADD ... MAXLEN`, no `XRANGE` exclusive `(` bounds; `XTRIM` `~` trims
  exactly; no `MINID`/`LIMIT`.
- **Expiration**: lazy expiry is always enforced on access; active expiry is
  available as an opt-in `active-expire yes` drop-soon index swept by the 1s
  timer. Expired keys do not propagate explicit `DEL`s to the AOF (absolute
  timestamps make replay converge, but the window Redis closes with propagated
  deletes exists here). With active expiry off, memory reclamation and `expired`
  event timing remain access-driven.
- **Lua**: system Lua 5.4, not Redis's patched 5.1. **Sandboxed**: only
  base/string/table/math are loaded (no io/os/package/debug),
  `dofile`/`loadfile`/`load`/`print` pruned, `_G` protected against global
  creation/reads of undefined globals, `math.random` reseeded per invocation
  from a distinct seed (effects replication removes the need for a deterministic
  RNG, matching Redis 7+; an in-script `math.randomseed` still overrides it), and
  resource limits enforced —
  `lua-time-limit` (instruction-count hook, default 5000ms) and
  `lua-memory-limit` (allocator cap, default unlimited), both settable at
  runtime via CONFIG SET. Scripts also get a **throwaway `_ENV` per run**
  (globals they create die with the execution — fresh-interpreter isolation
  at shared-state cost) and the state is recycled past 32MB of heap.
  Helper libraries: `cjson` and `cmsgpack` (in-project D implementations),
  Redis marks `_G` and the library tables read-only at the Lua VM level (a
  patch we don't have on stock 5.4), so protection is emulated: library tables
  are read-only proxies, and `setmetatable`/`getmetatable` are guarded so a
  script can't wipe `_G`'s protective metatable (`setmetatable(_G, {})`) or
  reach through it. `cmsgpack` packs a self-referential table to a fixed depth
  (16) exactly like Redis, but the byte order of the packed map depends on Lua's
  hash iteration and 5.4 differs from 5.1 (semantics/unpack identical; only the
  hex dump differs). Deep-table reply conversion reserves Lua stack via
  `lua_checkstack` and caps at "reached lua stack limit" rather than overflowing.
  `redis.sha1hex`, `bit`, `redis.log` (accepted, dropped), `redis.setresp`,
  `redis.set_repl` and a **Lua 5.1 compat layer** (`unpack`, `table.getn`,
  `math.pow`, `math.log10`, `math.ldexp` — Redis embeds 5.1, we run 5.4).
  Redis **Functions** (FUNCTION LOAD/DELETE/FLUSH/LIST/STATS, FCALL/FCALL_RO)
  are implemented. Still missing: `struct`. **Threading model diverges on
  purpose**: scripts run on a dedicated Lua thread (one, off the event loop),
  every `redis.call` round-trips to the main thread (the single keyspace
  writer). So a long/looping script does NOT stall the loop — other clients
  keep getting real replies (PONG), where single-threaded Redis returns
  `-BUSY`. **SCRIPT KILL works** (a shared flag the worker's instruction hook
  polls, like the time limit), but the BUSY-state tests are N/A since there
  is no BUSY state.
  **Replication model: EFFECTS** (like Redis 7+, where it is also the only
  mode): the EVAL itself never enters the raft/AOF log — each write the
  script performs via redis.call is logged as itself, in its propagation
  form (SETEX → `SET k v PXAT`, SPOP → SREM, XADD * → resolved id). Under
  raft the bridge PROPOSES each inner write, so the leader's state only
  changes through the log; standalone, a server-installed sink appends each
  effect to the AOF. Random-reply commands need no write guard — replicas
  replay what happened. A script that fails halfway keeps its earlier
  writes in the log, exactly like it keeps them in the dataset (dreads does
  not wrap script effects in MULTI/EXEC, same as its unwrapped EXEC).
  `redis.replicate_commands()` answers **true**. The clock is frozen for
  the whole EVAL (in-script `TIME` is deterministic; relative TTLs resolve
  like their logged effects).
- **Protocol**: RESP2 **and RESP3** (branch `resp3-oracle`). `HELLO 2`/`HELLO 3`
  negotiate per-connection (`Conn.resp3`); `HELLO 3` replies as a map. RESP3
  types implemented: null (`_`), boolean (`#`), double (`,`), big number (`(`),
  verbatim (`=`), map (`%`), set (`~`), push (`>`). Pub/sub (subscribe
  confirmations + message/pmessage/smessage) framed as Push under RESP3.
  Divergent replies routed through the proto-aware oracle: HGETALL/CONFIG GET
  (map), SMEMBERS/SINTER/SDIFF/SUNION (set), ZSCORE/ZINCRBY (double),
  INFO/LOLWUT (verbatim), all nils (`_`). RESP2 clients are byte-unchanged.
  Still to migrate: WITHSCORES array-of-pairs restructuring (ZRANGE family),
  XPENDING/XINFO maps, client-side tracking (not supported). No real `AUTH` /
  ACL enforcement yet (`requirepass`, `HELLO AUTH`, `ACL SETUSER` are roadmap).
- **Keyspace notifications**: `notify-keyspace-events` flags (K/E/g/$/l/s/h/z/x/
  e/t/m/n/d/A) and the `__keyspace@0__` / `__keyevent@0__` channels work. All
  common write commands fire events — string (SET/SETNX/SETEX/GETSET/GETDEL/
  APPEND/SETRANGE/INCR*/DECR*/INCRBYFLOAT/MSET), generic (DEL/EXPIRE/PERSIST/
  RENAME/COPY, plus `del` when a container empties), list (L/RPUSH, L/RPUSHX,
  L/RPOP, LSET, LREM, LINSERT, LTRIM, LMOVE/RPOPLPUSH), hash (HSET/HSETNX/HDEL/
  HINCRBY/HINCRBYFLOAT), set (SADD/SREM/SPOP/SMOVE, S{INTER,UNION,DIFF}STORE),
  zset (ZADD/ZREM/ZINCRBY/ZPOPMIN/ZPOPMAX, Z{UNION,INTER,DIFF}STORE, ZRANGESTORE,
  ZREMRANGEBY{RANK,SCORE,LEX}), stream (XADD/XDEL/XTRIM/XSETID), and `expired`.
  Store/trim/remrange variants fire only when they actually mutate (empty result
  emits `del` on the destination instead). Still NOT covered: `evicted`/`e`
  (maxmemory eviction), `keymiss`/`m`, `new`/`n` key events, and stream
  consumer-group events (XGROUP/XCLAIM). Events fire only on the standalone path
  (not the Raft apply path), notification channel names still use db 0 even
  when the client selected another DB, and `CONFIG SET notify-keyspace-events`
  is wired for standalone runtime changes.
- **maxmemory/LRU**: accounting via jemalloc `stats.allocated` (Linux only —
  inert elsewhere); approximate LRU samples 5 keys per eviction;
  `allkeys-lfu`/`volatile-ttl` policies not implemented. Scripts honour the
  deny-oom contract: over the limit, a legacy (no-shebang) script's write
  commands fail with `OOM command not allowed`, a `#!lua`-shebang script without
  `allow-oom` is refused outright, and `allow-oom`/`no-writes`/read-only runs
  proceed. The script-side check is read-only (it does not run the eviction
  cycle, which is main-thread-only), so a script write is not retried after
  eviction the way a direct client write is.
- **PUBSUB NUMPAT** counts total pattern subscriptions, not unique patterns.
  Shard pub/sub is a separate namespace on the same single node.
- **MEMORY USAGE** is a rough structural estimate, not allocator-exact.
- **INFO** exposes the sections needed by the current blackbox tail (clients,
  memory, stats, persistence, multi-DB keyspace), but is not a full Redis INFO
  mirror yet (notably Errorstats/Commandstats are still roadmap). `SAVE`/
  `BGSAVE`/`LASTSAVE` are fsync-backed (no RDB files); `BGREWRITEAOF` runs
  synchronously.
- **Memory model**: small hashes/sets/zsets use dreads' own contiguous
  listpack/intset-style encodings and spill to the full structures past the
  configured thresholds. The encodings are Redis-shaped for `OBJECT ENCODING`,
  but the in-memory layout is dreads-specific.

## Raft replication (phase 1 — implemented, verified live)

`vendor/raft` + `dreads/replicator.d` replace the legacy replication wire with
a consensus-replicated log. Opt-in via `raft-node-id`/`raft-peers`/`raft-port`;
standalone dreads (no config) instantiates none of it and the write path is
byte-identical. Verified on a live 3-node cluster: leader election,
replication of writes to followers, deterministic apply (EXPIRE resolves to
the same absolute TTL on every replica via the injectable determinism clock),
and leader-failover with committed data surviving. The log entry is
`[u64 clock][raw RESP command]`; the leader logs without executing (Raft only
mutates state on commit) and every replica applies with the injected clock.

**Remaining Raft / Redis-surface gaps:**

- **No ReadIndex.** Leader reads are served locally; a just-partitioned leader
  could serve slightly stale data until it steps down. Followers serve their
  local applied state (Redis async-replica read semantics).
- **`ROLE`/`WAIT`/`REPLICAOF` still expose Redis-standalone semantics rather
  than a full Raft-aware operational surface.**
- Dynamic membership is supported (joint consensus §6): `RAFT ADDNODE
  id@host:port` / `REMOVENODE id` / `STATUS`, join-mode learner
  (`raft-join yes`). InstallSnapshot (§7) IS supported: `RAFT COMPACT` (and
  auto-compaction past a log threshold) collapses dead history into a snapshot
  and discards the log; a joining node whose needed entries were compacted
  catches up via snapshot transfer. Remaining membership gaps: no PreVote
  (a partitioned server rejoining can still bump terms), single-shot snapshot
  (not chunked — a multi-GB snapshot is one message), and the disruptive-
  removed-server edge is only handled by the not-in-config election guard.

## Roadmap beyond parity

Phase-2: **sharding** — slot ranges (CRC16/16384) each owned by a Raft group,
which is the single-machine shared-nothing threading model (thread-per-shard).
This is also where dynamic membership and the `CLUSTER`/`MOVED`/`ASK` command
surface land. `DUMP`/`RESTORE`/`MIGRATE`, ACL/AUTH enforcement, INFO
Errorstats/Commandstats, and the blackbox long tail remain parity work.
