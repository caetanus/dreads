# Cross-shard & broadcast command map (phase 2b routing)

The routing taxonomy for every command under thread-per-shard + multi-router
(SO_REUSEPORT). Each connection lands on one shard-thread (its router); a command
either runs **local**, **hops** to one owner, spans **multiple owners**, or must
reach **all shards** (broadcast). This maps what v1 does today vs what 2b needs.

Source of truth: `source/dreads/aclkeys.d` (key specs), `source/dreads/aclcat.d`
(command table), `source/dreads/server.d` (routing: `shardOwnerOf`, the hop).

Legend for **v1 status**: ✅ correct · ⚠️ silently wrong under shards>1 · 🚫 gated.

---

## 1. Single-key — route to one owner shard  ✅

The fast path, fully working: `shardOwnerOf` extracts the first key → slot → owner;
same-shard runs inline, else hops (bytecode descriptor, coalesced). ~150 commands:
GET/SET/INCR/APPEND/GETDEL/GETEX, all H*/L*/S*/Z*/X* single-key ops, EXPIRE/TTL/
PERSIST/TYPE/DUMP/RESTORE, PF* single-key, GEO* (single key), OBJECT, SORT (no BY/GET
crossing), etc. **No work needed** — this is the hop we already have.

---

## 2. Multi-key — keys may span shards  ⚠️ (v1: routes by FIRST key only)

v1 routes by the first key and runs the whole command on that owner — **wrong if the
other keys live elsewhere** (reads stale/missing data, writes to the wrong shard).
Redis Cluster's contract: same-slot or `-CROSSSLOT`; `{hashtag}` forces co-location.
**2b: emit `-CROSSSLOT` when keys hash to different slots; single-slot runs as a
normal hop.** Scatter-gather (running pieces on each owner) is a later, harder option
for the read-only aggregators.

### 2a. Variadic (N keys to end)
`del · unlink · exists · touch · mget · mset · msetnx · watch · pfcount ·
sdiff · sinter · sunion · sdiffstore · sinterstore · sunionstore · pfmerge · bitop ·
blpop · brpop · bzpopmax · bzpopmin`

### 2b. Two-position (src + dst — CROSSSLOT unless same hashtag)
`rename · renamenx · copy · smove · lmove · blmove · rpoplpush · brpoplpush ·
geosearchstore · zrangestore · zdiffstore · zinterstore · zunionstore · sdiffstore ·
sinterstore · sunionstore · pfmerge · bitop`

### 2c. numkeys form (`… numkeys N k1..kN …`)
`zunion · zinter · zdiff · zunionstore · zinterstore · zdiffstore · zintercard ·
sintercard · lmpop · zmpop · blmpop · bzmpop · msetex`

> Note: `MSET k1 v1 k2 v2` with keys on different shards is the common legitimately
> cross-shard case; v1 mis-executes it. It is the first `-CROSSSLOT` to add.

---

## 3. Broadcast — keyspace-wide (must touch ALL shards)  ⚠️ (v1: local shard only)

Keyless, so `shardOwnerOf` returns −1 → v1 runs them on the **router's own shard
only**. Under shards>1 they are silently partial:

| command | v1 (partial) | 2b (broadcast) |
|---|---|---|
| `KEYS pattern` | this shard's matches | fan-out to all shards, concat |
| `SCAN cursor` | this shard's slice | cursor must encode (shard, inner-cursor); walk shards in turn |
| `DBSIZE` | this shard's count | sum across shards |
| `RANDOMKEY` | from this shard | pick a shard, then a key |
| `FLUSHALL` / `FLUSHDB` | this shard | broadcast to all shards |
| `SWAPDB` | this shard | broadcast (or refuse in cluster mode, like Redis) |
| `DBSIZE`/`WAIT`/`WAITAOF` | local | aggregate/coordinate across shards |

**2b strategy:** a broadcast primitive — the router fans the command to every shard
via the hop transport, then aggregates the replies (sum for DBSIZE, concat for KEYS,
`+OK` gate for FLUSH*). SCAN needs a composite cursor. This is the same fan-out the
pub/sub broadcast (§4) needs — build it once.

---

## 4. Pub/Sub — broadcast vs sharded  ⚠️ (v1: per-shard-local, THE gap you flagged)

Two distinct pub/sub planes (Redis 7+):

### 4a. GLOBAL pub/sub — BROADCAST across all shards
`publish · subscribe · unsubscribe · psubscribe · punsubscribe · pubsub`
- A `SUBSCRIBE`/`PSUBSCRIBE` registers on the **connection's** shard (local, fine).
- But a `PUBLISH` may land on ANY shard's router, and its subscribers — **especially
  PATTERN subscribers (`PSUBSCRIBE ch*`), which match by glob and can sit on any
  shard** — must all receive it. So **PUBLISH must broadcast to every shard**, each
  matching its own local channel + pattern subscribers.
- **This is exactly "pubsub is broadcast when there's a glob":** a plain-channel
  PUBLISH could in principle be routed by the channel's slot, but a pattern like
  `news.*` has no single slot — a publish to `news.sports` must be checked against
  every shard's pattern table. Redis solves it by making ALL global pub/sub a cluster
  broadcast (no slot routing for `PUBLISH`/`SUBSCRIBE`), which is what 2b must do.
- **v1 status:** `gPubSub` was made THREAD-LOCAL in the share-nothing sweep, so a
  subscriber on shard 3 does NOT see a PUBLISH that lands on shard 5. Correct only
  when subscriber and publisher share a shard. The 2b fix: PUBLISH hops the message
  to all shards; each delivers to its own subscribers same-thread (no cross-thread
  buffer write — the delivery stays share-nothing).

### 4b. SHARDED pub/sub — route by channel slot  ⚠️ (single-shard, like a key)
`spublish · ssubscribe · sunsubscribe`
- Introduced precisely to AVOID the broadcast: the channel has a slot, so `SSUBSCRIBE`
  and `SPUBLISH` route to the **one** shard owning `CRC16(channel)`. No fan-out.
- **v1:** `gShardPubSub` is thread-local too; correct only when the subscriber's
  connection and the publisher land on the shard owning the channel's slot. 2b routes
  both by channel slot (treat the channel like a key) → single-shard, no broadcast.

---

## 5. Broadcast — config / definition apply-to-ALL  ⚠️/🚫

State that must be identical on every shard, so a mutation broadcasts:
- `config set …` — every shard holds its own `gConfig` mirror; a runtime SET must
  apply on all (v1: applies on the router's shard only).
- `acl setuser · acl deluser · acl load` — the user registry must match on all shards
  (v1 gap, documented in SHARDING-HANDOFF; currently unguarded).
- `script load · function load · function delete` — scripts must be loadable on the
  shard that ends up executing them (any shard, since a hopped EVAL runs on the owner).
- `flushall` (also §3), `swapdb`.

**2b:** the meta-group / broadcast path applies these on every shard (or refuses in
cluster mode where Redis does).

---

## 6. Aggregate — READ from all shards, merge  ⚠️ (v1: local shard's view)

Introspection that must sum/union across shards:
- `info` — clients/keyspace/stats are per-shard TLS (the share-nothing sweep);
  a full INFO must aggregate (v1 reports the serving shard's numbers).
- `dbsize` (also §3), `client list` / `client no-evict` (conns are per-shard),
  `command count`, `memory usage`/`memory stats`, `slowlog get`, `commandlog`,
  `pubsub channels`/`numsub`/`numpat` (subscribers spread across shards),
  `acl list`/`acl getuser` (once ACL is shared).

**2b:** fan-out read + merge (same primitive as §3).

---

## 7. Connection-local — per-connection state, no keyspace  ✅

Run on the connection's own shard, no routing needed. Already correct:
`ping · echo · select · hello · auth · quit · reset · lolwut · readonly · readwrite ·
asking · client (setname/getname/id/info/no-evict/reply/unpause) · command (docs/info/
getkeys) · object help`.

---

## 8. Transactions / blocking / scripts — SAME-SHARD scope in v1  🚫 (out of scope, documented)

- `multi · exec · discard · watch · unwatch` — a transaction touching keys on
  different shards is genuinely cross-shard (needs the meta-group / 2PC). **v1: runs
  on the connection's own shard — wrong if a key lives elsewhere.**
- Blocking: `blpop · brpop · blmove · brpoplpush · blmpop · bzpopmin · bzpopmax ·
  bzmpop · wait · xread [block] · xreadgroup [block]` — the wait/wake is same-thread;
  cross-shard blocking needs the hop to carry the block registration. v1 same-shard.
- Scripts: `eval · evalsha · eval_ro · evalsha_ro · fcall · fcall_ro` — keys come from
  the `numkeys` prefix; a script whose keys span shards is cross-shard. v1 runs on the
  connection's shard (the Lua pool round-trips to the main thread).

These are the v1 correctness gaps already listed in SHARDING-HANDOFF; the doc here
just places them in the full taxonomy.

---

## 9. Server-global admin / cluster / replication — once, or meta-served  ✅/2b

- Once (leader or any single shard): `save · bgsave · bgrewriteaof · shutdown ·
  lastsave · time · role · failover · replicaof · slaveof · debug · pfselftest ·
  latency · reset · module`.
- Cluster surface (served from the cached slot map): `cluster (slots/shards/nodes/
  info/keyslot/myid/countkeysinslot/getkeysinslot)`, `asking`, `readonly`/`readwrite`.
- Replication wire: `sync · psync · replconf · migrate · restore-asking · clusterscan`.

---

## Build order for 2b (what the map implies)

1. **The broadcast primitive** — fan a command to all shards over the hop transport
   and aggregate replies. Unblocks §3 (KEYS/SCAN/DBSIZE/FLUSH*), §4a (PUBLISH), §5
   (CONFIG/ACL/SCRIPT apply), §6 (INFO/aggregates). One mechanism, many callers.
2. **`-CROSSSLOT` for multi-key** (§2) — cheap correctness: reject different-slot
   multi-key; single-slot is a normal hop. Makes MSET/MGET/DEL/… safe.
3. **Sharded pub/sub routing** (§4b) — route SPUBLISH/SSUBSCRIBE by channel slot.
4. Later/harder: cross-shard MULTI/EXEC, blocking across shards, cross-shard scripts,
   scatter-gather for read aggregators (SINTER/SUNION across slots).
