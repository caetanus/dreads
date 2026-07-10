# Drift: dreads vs Redis

Honest, mechanically-derived gap analysis. Method: the canonical core command
list from the official docs repo (`redis-doc/commands.json`, 241 base
commands / 370 including subcommands) diffed against dreads' dispatch tables,
plus a hand-audit of semantic differences in commands we *do* implement.
Regenerate the diff by re-running the extraction (see git history of this
file) whenever the dispatch grows.

**Status: 222 of 241 base commands implemented; 19 missing.** Module
families (RedisJSON, RediSearch, Bloom, TimeSeries — bundled in the Redis 8
image) are out of scope entirely.

## Missing commands

| Family | Missing | Why |
|---|---|---|
| **cluster (4)** | cluster asking readonly readwrite | Excluded by owner decision — to be discussed (intersects Raft) |
| **replication (7)** | replicaof slaveof failover psync sync replconf restore-asking | Same: replication is the Raft roadmap (`vendor/raft`), not the legacy wire protocol |
| **serialization (3)** | dump restore migrate | Need an RDB-compatible (or documented custom) value serialization format |
| **functions (3)** | function fcall fcall_ro | Redis Functions API; `EVAL`/`EVALSHA` (+`_RO`) cover scripting |
| **hll debug (2)** | pfdebug pfselftest | Internal debug commands |

Also missing: hash-field TTLs (`HEXPIRE`/`HPEXPIRE`/`HTTL`/..., Redis 7.4+).

## Semantic drift in implemented commands

These exist but do not match Redis exactly:

- **EXPIRE family**: no `NX/XX/GT/LT` flags. **ZADD**: no
  `NX/XX/GT/LT/CH/INCR` flags (GEOADD does implement NX/XX/CH).
- **SET**: full option set implemented (`EX/PX/EXAT/PXAT/NX/XX/KEEPTTL/GET`).
- **SPOP/SRANDMEMBER/ZRANDMEMBER/HRANDFIELD/RANDOMKEY**: deterministic
  (first live slots/ranks), not random. SPOP propagates as `SREM`, so
  persistence/replication are unaffected.
- **SCAN family**: cursor is a slot index; a concurrent rehash can miss or
  duplicate elements (Redis's reverse-binary cursor guarantees stability).
  ZSCAN is rank-based (ordered).
- **SORT**: no `BY`/`GET` patterns (explicit error).
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
- **Expiration**: lazy only (checked on access); no active expiry cycle, and
  expired keys do not propagate explicit `DEL`s to the AOF (absolute
  timestamps make replay converge, but the window Redis closes with
  propagated deletes exists here).
- **Lua**: system Lua 5.4, not Redis's patched 5.1. **Sandboxed**: only
  base/string/table/math are loaded (no io/os/package/debug),
  `dofile`/`loadfile`/`load`/`print` pruned, `_G` protected against global
  creation/reads of undefined globals, `math.random` reseeded
  deterministically per invocation, and resource limits enforced —
  `lua-time-limit` (instruction-count hook, default 5000ms) and
  `lua-memory-limit` (allocator cap, default unlimited), both settable at
  runtime via CONFIG SET. Scripts also get a **throwaway `_ENV` per run**
  (globals they create die with the execution — fresh-interpreter isolation
  at shared-state cost) and the state is recycled past 32MB of heap.
  Helper libraries: `cjson` and `cmsgpack` (in-project D implementations),
  `redis.sha1hex` and `bit` are provided; still missing: `struct`, and
  `SCRIPT KILL` (the time limit hard-aborts instead — with a single-threaded
  event loop no other command can arrive mid-script anyway); scripts log
  verbatim, so time-dependent commands *inside* scripts (relative `EXPIRE`,
  `XADD *`) can drift on replay.
- **Protocol**: RESP2 only — `HELLO 3` answers `NOPROTO`. No `AUTH`
  passwords (no `requirepass` yet).
- **Keyspace notifications**: `notify-keyspace-events` flags (K/E/g/$/l/s/h/z/x/
  e/t/m/n/d/A) and the `__keyspace@0__` / `__keyevent@0__` channels work. Most
  common write commands fire events — string (SET/SETNX/SETEX/GETSET/GETDEL/
  APPEND/SETRANGE/INCR*/DECR*/INCRBYFLOAT/MSET), generic (DEL/EXPIRE/PERSIST/
  RENAME, plus `del` when a container empties), list (L/RPUSH, L/RPOP, LSET,
  LREM), hash (HSET/HSETNX/HDEL/HINCRBY), set (SADD/SREM/SPOP/SMOVE), zset
  (ZADD/ZREM/ZINCRBY/ZPOPMIN/ZPOPMAX), and `expired`. Still NOT instrumented:
  LINSERT/LTRIM/RPOPLPUSH/LMOVE/LPUSHX/RPUSHX, HINCRBYFLOAT, the `*STORE` family
  (S/Z inter/union/diff, ZRANGESTORE), ZREMRANGEBY*, COPY/MOVE, and stream ops.
  Events fire only on the standalone path (not the Raft apply path), db is always
  0, and `CONFIG SET notify-keyspace-events` is not wired (config-file only).
- **maxmemory/LRU**: accounting via jemalloc `stats.allocated` (Linux only —
  inert elsewhere); approximate LRU samples 5 keys per eviction;
  `allkeys-lfu`/`volatile-ttl` policies not implemented.
- **PUBSUB NUMPAT** counts total pattern subscriptions, not unique patterns.
  Shard pub/sub is a separate namespace on the same single node.
- **MEMORY USAGE** is a rough structural estimate, not allocator-exact.
- **INFO** is a stub with a few fields; `SELECT` accepts only db 0 (single
  keyspace); `SAVE`/`BGSAVE`/`LASTSAVE` are fsync-backed (no RDB files);
  `BGREWRITEAOF` runs synchronously (single-threaded event loop).
- **Memory model**: no small-value encodings (listpack/intset) — per-key
  memory is higher than Redis for small containers; `OBJECT ENCODING`
  reports our actual encodings.

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

**Phase-1 gaps (deliberate — static-membership-first was an explicit
decision to avoid debugging consensus and reconfiguration at once):**

- **No dynamic membership — no runtime join/leave.** Peers are fixed at boot
  by `raft-peers`. Adding a 4th node to a running 3-node cluster, or formally
  removing a node so the majority is recomputed, is NOT supported (needs joint
  consensus / §6 single-server changes + AddServer/RemoveServer RPCs). A dead
  node is *tolerated* (cluster runs on the majority) and an existing node can
  *restart* and re-sync from its durable raftlog (tested), but that is "the
  same member returning," not a new member. Cluster size is set at boot.
- **No ReadIndex.** Leader reads are served locally; a just-partitioned leader
  could serve slightly stale data until it steps down. Followers serve their
  local applied state (Redis async-replica read semantics).
- **`EVAL`-with-writes and `MULTI`/`EXEC` are not raft-routed yet** — they run
  locally on the leader (not replicated through the log).
- **`ROLE`/`WAIT`/`REPLICAOF` still report standalone values** (not raft-aware).
- Dynamic membership IS supported now (joint consensus §6): `RAFT ADDNODE
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
surface land. `DUMP`/`RESTORE`/`MIGRATE` and Redis Functions when parity work
resumes.
