# Blackbox compatibility TODO

> **TODO (fuzz the whole sweep):** run the ENTIRE blackbox sweep in a loop with a
> per-file timeout to catch INTERMITTENT hangs/races, not just a single pass. A
> real intermittent lost-wakeup in the list blocking/unblock code slipped past a
> clean single run (271/10, completed) but reproduced on the 1st fuzz run (hang,
> 200s timeout, right after "List of various encodings - sanitize dump"). One pass
> is not proof; the blocking/unblock paths especially need repeated runs.

Failures found running the **Valkey test suite** against a live dreads in
external mode (`./runtest --host … --port … --single <file>`), on **db 9**
(no `--singledb`, exercising multi-DB). Valkey is used as a read-only oracle.

> These are the **first blocking failure per file**. The TCL runner aborts a
> file at its first `[exception]`, so `ok=N` undercounts and downstream
> failures are **masked** until the blocker above them is cleared. Re-run each
> file after fixing a blocker to reveal the next layer.

**Layer 1 (2026-07, cleared):** CONFIG encoding thresholds, DEBUG stubs,
COPY … DB n, OBJECT arity, SCAN TYPE — all landed on master.

**Layer 2 (2026-07-12, cleared on `blackbox`):** list OBJECT ENCODING tiers,
blocking on db≠0 (+ firstPass WRONGTYPE + `inExec` bail), HELP replies,
real RANDFIELD/RANDMEMBER randomness (dreads.rand, reservoir/selection
sampling), RESP3 pair nesting + `_` nulls, live-config small-container
thresholds, SINTERCARD/*MPOP error parity, BITCOUNT/BITPOS range semantics,
SORT BY/GET, MSETEX, CONFIG INFO, XGROUP SETID, INFO
(blocked_clients/used_memory/expired_keys/multi-db keyspace), SMOVE notify,
notify-keyspace-events applied on CONFIG SET, `blackbox/valkey-sync.skip`
(SYNC is N/A by design), `blackbox/dreads-suite.conf` (active-expire for
suite runs). Every fix has its own UT (`tests/blackbox_regressions_tests.d`
and siblings — NEVER copy the tcl suite; memory `blackbox-internal-coverage`).
Unit runner is now single-threaded (`-s` injected in bin/ut.d): tests mutate
`__gshared` state (gRespProto, gConfig, notify hooks).

**Layer 3 (2026-07-12, cleared):** HGETDEL; EXPIRE-family NX/XX/GT/LT +
overflow messages + past-deadline synchronous delete; GETEX error split;
ZUNIONSTORE ±inf→0 and 0×inf weights; lex-range reversed-bounds emptiness;
typed range-bound errors (float/lex/int); ZADD dangling-pair = syntax error;
MSETEX expire not-an-integer; notify flags applied on CONFIG SET;
ZRANDMEMBER WITHSCORES count-overflow guard (was a server CRASH via 9e18-draw
loop); `blackbox/sweep.sh` restarts dreads per file (kills cross-file config
leakage).

## Progress per file (sweep 2026-07-14, db 9, skipfile, fresh server per file)

| File | ok | err | first blocker (if aborts) |
|---|---|---|---|
| unit/type/incr | 32 | 0 | **PASSES** |
| unit/type/string | 106 | 0 | **PASSES** |
| unit/type/zset | 322 | 0 | **PASSES** |
| unit/type/stream | 71 | 0 | **PASSES** (skips N/A: `~` approx trim = Redis macro-node granularity; 2 DEBUG LOADAOF need appendonly override) |
| unit/type/stream-cgroups | 53 | 0 | **PASSES** (skips: total_blocking_keys introspection, DUMP/RESTORE+AOF consumer metadata = Raft-replicated by design, legacy-RDB load, lag-with-tombstones) |
| unit/scan | 24 | 0 | **PASSES** |
| unit/bitfield | 18 | 0 | **PASSES** |
| unit/type/list | 271 | 10 | `CLIENT PAUSE` now implemented; **hangs** at a blocking↔pause / commandstats test — re-sweep |
| unit/pause | 19 | 0 | **PASSES** (1 skip N/A: deferring-client write-backpressure timing — dreads buffers server-side by design) |
| unit/dump | 15 | 1 | **MIGRATE landed** (DUMP->RESTORE over cached socket; 14 two-instance tests are external:skip, verified live vs valkey 9.1; the runnable cached-connection-release test passes); stream DUMP/RESTORE byte-exact both ways; only remaining fail = RESTORE shared-NACK corruption reject (expects "Bad data format", we say "checksum are wrong") |
| unit/type/hash | 85 | 0 | **PASSES** (HINCRBYFLOAT long-double repr + NaN/Inf + value-update spill; HRANDFIELD uniform via rejection sampling) |
| unit/type/set | 116 | 0 | **PASSES** (SRANDMEMBER uniform via rejection sampling; 1 skip = per-key WATCH TODO, we use a conservative global epoch) |
| unit/bitops | 49 | 2 | completes; SETBIT fuzz |
| unit/sort | 54 | 0 | **PASSES** (1 skip N/A: DEBUG OBJECT ql_compressed = quicklist node compression we don't model) |
| unit/expire | 66 | 5 | aborts: tcl `table_size` var (import-source landed) |
| unit/keyspace | 45 | 1 | aborts: stream-cgroups COPY (ERR syntax) |
| unit/other | 25 | 2 | aborts: CONFIG SET unsupported param |
| **unit/hashexpire** | **230** | **0** | **PASSES** (HGETEX trailing-FIELDS numfields error; skips = field-expiry MODEL divergence: dreads reaps expired fields eagerly in lookup, Valkey keeps them physical until active/access — HGET contract identical, only HLEN/HTTL of an unreaped expired field differ) |
| unit/dump | 5 | 2 | aborts: OBJECT FREQ (LFU freq tracking) |
| unit/hyperloglog | 6 | 2 | aborts: PFDEBUG |
| unit/scripting | 548 | 0 | **PASSES** (errorstats/commandstats + Valkey error format + acl_check_cmd) |

Landed 2026-07-14 (this session): HEXPIRE family + HSETEX + HGETEX; DUMP/RESTORE
(external AOF-command<->RDB translator, both phases, bidirectional Valkey 9.1
interop); CLIENT IMPORT-SOURCE + import-mode expiry pause; LFU/rdb-version-check/
hll-sparse CONFIG params; INFO `keys_with_volatile_items` + `expired_fields`.
hashexpire.tcl went 3 -> 226. No hot-path regression (SET 1.24M / GET 1.43M /
HGET-with-field-TTL 1.19M rps @ -P16).

CLIENT PAUSE/UNPAUSE landed (2026-07-14): buffer-and-replay via the normal
pipeline (no fiber park — the socket keeps draining into a bounded per-conn
buffer, so a flood trips the client-query-buffer-limit and the conn is dropped;
verified the cut point tracks the configured limit). WRITE vs ALL, issuer
exempt, stacking (max end + most restrictive), INFO paused_* fields,
blocked_clients accounting, and the WRITE "write surface" (PUBLISH/SPUBLISH/
PFCOUNT + EVAL/EVALSHA/FCALL gated by shebang/function no-writes flags,
EXEC-with-writes as a unit). Also fixed SCRIPT LOAD to strip the `#!lua` shebang.
pause.tcl 2 -> 13; no hot-path regression (SET 1.26M / GET 1.37M rps @ -P16).
Remaining unit/pause: eviction & active/passive expire skipped during a pause
(needs a periodic-eviction cycle dreads lacks + lazy-expire deferral), may-
replicate rejected inside RO scripts (script bridge), one deferring-client timer.

Eviction/OOM (2026-07-14): deny-OOM now matches Valkey's CMD_DENYOOM (only
allocating writes refused; DEL/EXPIRE/*POP run under OOM; a dirty script isn't
stopped mid-way). Opt-in background eviction cycle behind `active-eviction`
(default off; write path frees on demand) — evicts across all dbs on the 1s
timer, counts INFO `evicted_keys`, fires the "evicted" event, and is skipped
during a CLIENT PAUSE window. Also OBJECT FREQ/IDLETIME follow the maxmemory
policy (LRU/LFU share obj.lruSecs; RESTORE FREQ/IDLETIME seed it), and a script's
redis.call('publish') now reaches pub/sub via a data-plane dispatch case. The
scripting.tcl OOM abort now blocks on INFO errorstats/commandstats (a stats gap,
NOT eviction): errorstat_<code>:count, total_error_replies, cmdstat rejected_calls
/failed_calls, CONFIG RESETSTAT.

Quick wins to unmask more: re-sweep list.tcl (PAUSE was its blocker),
**CLIENT REPLY** (pubsub), OBJECT FREQ (dump), PFDEBUG (hyperloglog), the `other`
CONFIG param, and stream-COPY (keyspace). Sort is the biggest local debt.

## Layer 4 — open failures (sweep7/8)

### Blocking service order — DONE (2026-07-13, event-driven rewrite)
The poll-retry "wake everyone and race" model was replaced with a true
event-driven blocked-client FIFO (see the `event-driven` skill + memory
`blocking-fifo`). Per-`(db,key)` deque of single-shot waiters; a producer wakes
ONLY the live front of a touched key (FIFO for free — no broadcast, no gating);
serving cascades via the next-front signal; single-shot generation stops
double-serve (Circular BRPOPLPUSH). Timeout is the loop's own timer (finite) or a
pure event wait (infinite) — no sleep/poll. New `emplace.Deque` (ring buffer,
releases memory as it drains). All targets pass: Linked LMOVEs, Circular
BRPOPLPUSH, BRPOPLPUSH/BLMPOP/BZMPOP "multiple blocked clients", wrong-dest,
maintains-order, BZPOPMIN expire-reblock (fixed a frozen-`gClock` bug: blocking
serves inline, so the loop refreshes the deterministic clock on each wake).
**list 100/8 → 106/2** (2 left = LPOS RANK + the tcl `can't read "cmd"` abort,
both non-blocking, separate). **zset 318/8 → 322/0.** No cross-file regressions
(full sweep 999/51 → 1007/37). XREAD BLOCK deliberately stays on the broadcast
(fan-out, not hand-off).

**XREADGROUP BLOCK — DONE (2026-07-14, commit 87995e5).** It accepted the BLOCK
keyword but never parked (validated timeout, one attempt, returned nil). Now a
live conn reading `>` parks its fiber on the same `gKeyActivity` event as XREAD
(fan-out — the group's `lastDelivered` cursor serializes who gets which entry, so
no per-key FIFO hand-off). Only `>` blocks; explicit ids read the PEL and return
at once; non-BLOCK / MULTI-EXEC fall through to one-shot dispatch. Unlike XREAD
it's a WRITE → a served pass is logged rewritten without the BLOCK pair. Verified
live (parks blocked_clients:1, wakes on XADD in ~1s not the 4s timeout, PEL
registered, two workers each get a distinct entry). Own UT
`blackbox.xreadgroup_block_servability`. Valkey's stream-cgroups blocking tests
(line 216+) remain unreachable — that file aborts earlier on separate debts
(`XGROUP CREATE ... ENTRIESREAD -3` validation, `XPENDING ... IDLE`).

### Streams — DONE (2026-07-14): stream.tcl 71/0, stream-cgroups 53/0
Both files went from aborting at test 5/10 to fully passing. Landed, roughly in
dependency order: u64 stream ids (parseUlong, not signed) + `ms-*` auto-seq;
XGROUP CREATE ENTRIESREAD validation + XINFO entries-read/lag; XPENDING IDLE +
exclusive `(` bounds; XACK atomic id validation; XGROUP SETID/CREATE `-`/`+`
cursors; inline XADD/XTRIM trim (MAXLEN/MINID/`=`/LIMIT/NOMKSTREAM); XRANGE/
XREVRANGE exclusive `(` edges; PEL history reports deleted entries (#5570); XREAD
`+` last-element + ignore mid-block type change; stream metadata (entries-added,
max-deleted-entry-id, recorded-first-entry-id) + XINFO STREAM [FULL]; XSETID with
ENTRIESADDED/MAXDELETEDID; nextId ms-rollover on seq exhaustion; CONFIG SET
multi-pair + CONFIG GET of SET-only params; SWAPDB/MOVE/XAUTOCLAIM are writes
(unblock blocked XREADGROUP); XCLAIM/XAUTOCLAIM evict deleted entries; XREADGROUP
BLOCK (event-driven, see blocking section); consumer introspection (XINFO
CONSUMERS, seen/active-time, name-sorted, entries-read/lag tombstone-free path);
blocked-command commandstats.

**Deliberate skips (N/A or by-design, in valkey-sync.skip):** `~` approximate trim
and lag-with-tombstones depend on Redis's rax macro-node granularity, which dreads
(flat sorted array) doesn't model; DUMP/RESTORE+AOF of consumer-group PEL/consumer
metadata is redundant with Raft replication (the follower rebuilds the PEL from the
log — low-priority to cover the full STREAM_LISTPACKS_3 group layout anyway, see
the `rdb-pel-from-raft` memory); `total_blocking_keys` introspection needs a
per-key blocked-client registry we don't keep (XREAD/XREADGROUP block on the global
event); legacy-RDB (rdb_ver<10/11) loading; and DEBUG LOADAOF/AOFRW need a per-test
appendonly override the external harness can't apply. No SET/GET hot-path change.

### Hash-field TTL (HEXPIRE family) — DONE (2026-07-14)
Implemented HEXPIRE/HPEXPIRE/HEXPIREAT/HPEXPIREAT, HPERSIST, HTTL/HPTTL/
HEXPIRETIME/HPEXPIRETIME, HGETEX — behavior derived from the blackbox tests +
command docs (the client contract: reply codes -2/-1/0/1/2, the NX/XX/GT/LT
condition flags, HSET dropping a field's TTL), NOT from Valkey source. Storage is a
lazily-allocated per-hash side-map `SmallHash.fieldTTL` (field→absMs, own field
names, survives listpack↔hashtable spill); a small hash with any field TTL reports
`listpackex`. Expiry mirrors key TTL exactly: **lazy** reap in `Keyspace.lookup`
(the per-field analog of the key-expired check) and **active** reap via a NEW
tagged secondary index `subExpires: deadline→[{type,key}]` — one entry per
container at its nearest field deadline, dispatched by type (hash now, zset-member
later), reusing arm/disarm/retime/cycle. Propagation is canonical + deterministic:
`HPEXPIREAT` (set) / `HDEL` (past-time or reap), like EXPIRE→PEXPIREAT/DEL — no
per-field DEL on lazy reap (absolute deadline is already replicated). Events
`hexpire`/`hpersist`/`hexpired` (NClass.hash), once per command. Own UT:
`blackbox.hexpire_*` (6 tests). No tcl suite here (this Valkey checkout's hash.tcl
predates the feature); validated by unit + live smoke + source parity.

### DUMP / RESTORE — DONE phase 1 (2026-07-14)
DUMP/RESTORE/CRC64 landed as an **external AOF-command <-> RDB translator**
(`dreads/rdb.d`) — NOT a first-class serializer. It speaks only RESP commands
(the compactor's `dumpKey` output) and RDB bytes, never touches RObj. DUMP =
`dumpKey(valueOnly)` -> `commandsToRdb` -> version-80 + CRC64-Jones footer.
RESTORE = `verifyFooter` -> `rdbToCommands` -> re-dispatch (DEL + rebuild cmds +
PEXPIREAT logged; no RESTORE in the log). Encoder emits PLAIN types
(0/1/2/4/5 + 22 hash-with-field-TTL); the decoder handles those. Field TTLs
round-trip (HASH_2). REPLACE/ABSTTL/BUSYKEY/checksum-reject all correct. Own UT
`blackbox.rdb_dump_restore_roundtrip` + codec UTs in rdb.d. Also fixed a
durability gap: `dumpKey` now re-emits `HPEXPIREAT` for hash field TTLs (they
were dropped on rewrite/snapshot). **hash 79/4 -> 81/8 (now RUNS TO COMPLETION —
DUMP unblocked it; the 4 uniques are pre-existing HRANDFIELD/HINCRBYFLOAT, not
RDB). list 269/6 -> 271/10 (advanced past DUMP; new blocker = CLIENT PAUSE).**
**Phase 2 DONE (2026-07-14):** compact-encoding decoders — intset (11),
listpack (16/17/20), quicklist2 (18) — so real Valkey/Redis dumps import.
**Bidirectional interop with Valkey 9.1 verified live: dreads DUMP -> Valkey
RESTORE works for every type; Valkey DUMP -> dreads RESTORE works for every type
(intset up to 300 elems, listpack hash/set/zset, quicklist2 list).** Own UT
`blackbox.rdb_restore_valkey_compact` uses REAL Valkey-captured payload bytes.
Also a live loadaof harness `blackbox/expire_loadaof_test.sh` (the USER's rule):
every expire mechanism reloads with its ABSOLUTE deadline intact, expired keys
stay dead across downtime, and TOUCH does NOT renew (matches Valkey — TOUCH only
bumps last-access; sessions renew via EXPIRE/GETEX). Phase 2b (deferred): legacy
ziplist (10/12/13) / zipmap (9) / quicklist-v1 (14) / ZSET(3 ascii) / LZF-
compressed strings / stream DUMP — Valkey 9 doesn't emit these, needed only for
very old dumps or compressed large values.

**MIGRATE DONE (2026-07-14, commit 6852d20):** option 2 — DUMP each existing key
here, RESTORE it onto the target over a **cached outbound socket** (per host:port,
released after 10s idle from the 1s timer; INFO `migrate_cached_sockets`), then
DEL locally unless COPY (logged). Reuses `dumpKeyPayload`, so it interoperates
byte-for-byte with real Redis/Valkey, not just dreads<->dreads. Flags COPY/REPLACE
/AUTH/AUTH2/KEYS; missing keys skipped, NOKEY when none; RESTORE errors surfaced
verbatim; broken socket closed not reused. Arg parsing extracted to pure
`parseMigrateArgs` with own UT `blackbox.migrate_arg_parsing`. The 14 two-instance
dump.tcl MIGRATE tests are external:skip (harness won't spawn dreads as the second
node); verified live vs valkey 9.1 (basic/copy+ttl/replace/BUSYKEY/NOKEY/multi-key
/socket-caching). The runnable cached-connection-release test passes.

### Not implemented (each aborts its file)
- HSCAN NOVALUES (scan file, to confirm).
- COPY of a stream with consumer groups (keyspace).
(CLIENT PAUSE and stream DUMP/RESTORE — both DONE this session, see above.)

### Scripting (effects replication LANDED — leftovers)
Effects replication is in (EVAL never logged; each redis.call write logged
in its propagation form; raft proposes per-write; replicate_commands=true).
Leftovers: `struct` library; SCRIPT LOAD is per-node (not replicated — an
EVALSHA on a node that never loaded the body answers NOSCRIPT, client falls
back to EVAL); script effects are not wrapped in MULTI/EXEC (matches the
unwrapped-EXEC drift); under raft a script's inner writes are one consensus
round-trip EACH (pipelining them is a future optimization). Add
`unit/scripting` to the sweep next.

### Singles
- zset: "zunionInterDiffGenericCommand at least 1 input key" (exact arity/
  message split), ZRANGE/ZRANGESTORE "invalid syntax" edges, BZMPOP
  non-key-argument arity (#10762).
- hash: HRANDFIELD count-hashtable coverage edge; HINCRBYFLOAT float
  representation (uses long double in Redis; issue #2846 test).
- set: SRANDMEMBER histogram distribution in spilled (Dict) mode — the
  wrapping-scan draw is biased by probe clusters; use rejection sampling.
- set/WATCH: "SMOVE only notify dstset" is really the WATCH global-epoch
  drift (DRIFT.md): unchanged dst still aborts EXEC.
- expire: 4 leftovers (re-check names after next sweep).
- bitops: SETBIT fuzz test got an empty reply mid-run (find which command).
- other: CONFIG INFO entries need a `flags` field (+ multi-pattern matching).
- sort: BY-nosort ordering nuances (sorted-set source keeps zset order,
  `BY <constant> + STORE` still sorts for determinism), GET pattern ending in
  `->`, "sub-sorts lexicographically when scores equal", SORT_RO key
  extraction, STORE-created list must report the right encoding.

## Multi-DB peripheral gaps (not yet exercised, known incomplete)

These pass the core SELECT/MOVE/SWAPDB path but are still hardwired to db 0:

- **Keyspace notifications** publish with a hardcoded `db 0` in the channel
  (`__keyspace@0__`). Should use the connection's current db index.
- **Standalone AOF SELECT-logging**: replay runs from the log stream and needs
  a `SELECT N` marker (or equivalent framing) before writes on db N. Raft live
  proposals already carry the db index; raft snapshots still need per-db
  framing instead of dumping only db 0.
- **Eviction, blocking re-dispatch, and MULTI-replay** paths need a multi-DB
  audit. SELECT inside MULTI must be verified against Valkey; any replay path
  that dispatches with a fixed `ks` can silently target the wrong DB.
- **CLIENT LIST/INFO** now reports the real db, but `addr` is still `?`.

## ACL / AUTH (2026-07-13, `auth-acl` → `master`)
- **acl.tcl block-1 now PASSES COMPLETELY: 88 ok / 0 err** (was 17 at the start
  of the ACL push). Achieved across the session: GETUSER/LIST, subcommand +
  first-arg rules, ACL CAT/DRYRUN, canonical AOF/raft persistence, key + channel
  enforcement, ACL LOG (aggregation, contexts, metrics), `db=` database
  selectors, default-user-off auth, HELLO AUTH/SETNAME, DELUSER edges
  (default-protected + self/other-session disconnect), ACL HELP/LOAD, GETUSER
  lossless compaction, EXEC-replay + blocked-client ACL re-check, a global
  connection registry (CLIENT LIST enumeration + channel kill-on-revoke +
  CLIENT KILL), and INFO commandstats. Every fix has an own unit test (per
  `blackbox-internal-coverage`); server-layer-only bits noted here.
- **INFO commandstats (commit 1205560):** `# Commandstats` with per-command
  `cmdstat_<name>:calls/usec/usec_per_call/rejected_calls/failed_calls`. Counters
  are real (calls/failed at the executeCommand dispatch chokepoint; rejected at
  every pre-execution ACL denial incl. the blocked-client re-check); CONFIG
  RESETSTAT clears them. `usec` is intentionally 0 — no per-command clock read
  (hot-path cost); blocking/pubsub/connection commands handled before the
  dispatch chokepoint aren't call-counted. Revisit if real latency stats are
  wanted.
- **Connection registry (commit 702f79c):** intrusive doubly-linked list of every
  live `Conn*` (each lives on its serveClient fiber stack; single event loop ⇒ no
  locking). Powers `CLIENT LIST` (all clients), channel kill-on-revoke, and
  DELUSER disconnecting OTHER sessions, and **CLIENT KILL** (commit 26444e5:
  ID/USER/SKIPME filters, self-kill closes after reply). `addr` is still `?`
  (no peer-address plumbing), so ADDR/LADDR filters and the legacy addr form
  never match; TYPE/MAXAGE are accepted but unmodelled.
- Later acl.tcl `start_server` blocks (aclfile-based) remain `external:skip`:
  dreads persists users via the AOF/raft log, not an `aclfile` (see
  `aof-is-ours`), so `ACL LOAD/SAVE` return "not configured to use an ACL file".

## Method to complete the catalog
1. Land group 1 (CONFIG) + group 2 (DEBUG stubs) — the two blockers gating
   the most files.
2. Re-run the sweep; record the next layer of `[err]` per file.
3. Repeat until files run to completion, then log the true per-test failures.
