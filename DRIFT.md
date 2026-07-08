# Drift: dreads vs Redis

Honest, mechanically-derived gap analysis. Method: the canonical core command
list from the official docs repo (`redis-doc/commands.json`, 370 commands)
diffed against dreads' dispatch tables, plus a hand-audit of semantic
differences in commands we *do* implement. Regenerate the diff by re-running
the extraction (see git history of this file) whenever the dispatch grows.

**Status: 120 core commands implemented, 121 base commands missing.**
Roughly half the core surface — this is not 1:1 yet. Module families
(RedisJSON, RediSearch, Bloom, TimeSeries — bundled in the Redis 8 image)
are out of scope entirely.

## Missing commands, by family

| Family | Missing |
|---|---|
| **geo (10)** | geoadd geodist geohash geopos georadius georadius_ro georadiusbymember georadiusbymember_ro geosearch geosearchstore |
| **transactions (5)** | multi exec discard watch unwatch |
| **bitmap (7)** | setbit getbit bitcount bitpos bitop bitfield bitfield_ro |
| **sorted-set (18)** | zunionstore zinterstore zdiff zdiffstore zunion zinter zintercard zrangestore zrangebylex zrevrangebylex zremrangebylex zrevrangebyscore zlexcount zrandmember zmpop bzmpop bzpopmin bzpopmax |
| **list (9)** | lpushx rpushx lpos lmpop blpop brpop blmove blmpop brpoplpush |
| **stream (9)** | xrevrange xsetid xinfo xgroup xreadgroup xack xpending xclaim xautoclaim |
| **generic (15)** | unlink touch randomkey copy sort sort_ro object dump restore migrate move expiretime pexpiretime wait waitaof |
| **hash (3)** | hincrbyfloat hmset hrandfield |
| **hyperloglog (5)** | pfadd pfcount pfmerge pfdebug pfselftest |
| **string (2)** | lcs substr |
| **connection (4)** | hello auth client reset |
| **scripting (5)** | eval_ro evalsha_ro function fcall fcall_ro |
| **pubsub (3)** | ssubscribe sunsubscribe spublish (shard pub/sub) |
| **server (22)** | info-adjacent and ops surface: bgsave bgrewriteaof save lastsave shutdown monitor slowlog latency memory debug acl module replicaof slaveof role failover psync sync replconf swapdb lolwut restore-asking |
| **cluster (4)** | cluster asking readonly readwrite |

Also missing hash-field TTLs (`HEXPIRE`/`HPEXPIRE`/`HTTL`/... , Redis 7.4+).

## Semantic drift in implemented commands

These exist but do not match Redis exactly:

- **EXPIRE family**: no `NX/XX/GT/LT` flags.
- **ZADD**: no `NX/XX/GT/LT/CH/INCR` flags.
- **ZRANGE**: only the classic index form (+`WITHSCORES`); no unified
  `REV/BYSCORE/BYLEX/LIMIT` (Redis 6.2+). `ZRANGEBYSCORE` lacks `LIMIT`.
- **XADD**: no `NOMKSTREAM`, no inline `MAXLEN/MINID` trimming. **XRANGE**:
  no exclusive `(` bounds. **XREAD**: no `BLOCK`. **XTRIM**: `MAXLEN` only,
  `~` accepted but trims exactly; no `MINID`/`LIMIT`.
- **SPOP/SRANDMEMBER**: deterministic (first live slots), not random.
  SPOP is propagated as `SREM`, so persistence/replication is unaffected.
- **SCAN family**: cursor is a slot index; a concurrent rehash can miss or
  duplicate elements. Redis's reverse-binary cursor guarantees elements
  present for the whole scan are returned. ZSCAN is rank-based (ordered).
- **LPOP/RPOP** with `count` on a missing key reply `*-1`; **SETRANGE** has
  no proportional-padding limit; **GETDEL/GETEX** implemented, `GETEX`
  without options is treated as a pure read (not logged).
- **Expiration**: lazy only (checked on access). No active expiry cycle, and
  expired keys do not propagate explicit `DEL`s to the AOF — replay converges
  because expiries are stored absolute, but the divergence window Redis
  closes with propagated deletes exists here.
- **Lua**: system Lua 5.4, not Redis's patched 5.1/LuaJIT; full stdlib is
  open (Redis sandboxes); no `cjson`/`cmsgpack`/`struct`/`bit`/
  `redis.sha1hex`; no script timeout/`SCRIPT KILL`; scripts are logged
  verbatim (command-replication), so time-dependent commands *inside*
  scripts (relative `EXPIRE`, `XADD *`) can drift on replay — Redis solves
  this with effect replication.
- **Protocol**: RESP2 only; no `HELLO`/RESP3, no `AUTH`, no inline commands.
- **Server surface**: `INFO` is a stub (a few fields), `CONFIG GET` returns
  empty, `COMMAND` returns an empty array, `SELECT` accepts only db 0
  (single keyspace), `PUBSUB NUMPAT` not implemented.
- **Memory model**: no small-value encodings (listpack/intset), so per-key
  memory is higher than Redis for small containers; no `maxmemory`/eviction
  policies; no keyspace notifications.
- **Persistence**: AOF only — no RDB, no AOF rewrite/compaction yet, fsync
  policy fixed at everysec.

## Priorities (as of 2026-07-08)

1. **GEO family** — explicitly requested; the logo promises it. Sorted-set
   backed geohash encoding, `GEOADD/GEOPOS/GEODIST/GEOSEARCH`.
2. **Transactions** (`MULTI/EXEC/DISCARD/WATCH`) — broad client dependency.
3. **zset algebra** (`ZUNIONSTORE/ZINTERSTORE/...`) and easy generics
   (`UNLINK/TOUCH/RANDOMKEY/COPY/LPUSHX/RPUSHX/HMSET/HINCRBYFLOAT`).
4. **Bitmaps** (`SETBIT/GETBIT/BITCOUNT/BITOP/BITPOS`).
5. **Blocking ops** (`BLPOP/BRPOP/BLMOVE`, `XREAD BLOCK`) — needs fiber
   wakeup plumbing shared with pub/sub.
6. **Stream consumer groups**; **HELLO/RESP3**; hash-field TTLs.
