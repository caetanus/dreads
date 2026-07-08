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
  passwords (no `requirepass` yet); no keyspace notifications.
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

## Roadmap beyond parity

Raft replication (vendor/raft) replaces the legacy replication wire; the
deterministic AOF (with the propagation-override rules) is the replicated
log and `BGREWRITEAOF` output is the snapshot format. Cluster commands wait
on that discussion. `DUMP`/`RESTORE` and Redis Functions are next when
parity work resumes.
