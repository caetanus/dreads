# Blackbox compatibility TODO

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

## Progress per file (fresh sweep 2026-07-13, sweep-27, db 9, skipfile, fresh server per file)

| File | sweep8 ok | now ok | err | first blocker (if aborts) |
|---|---|---|---|---|
| unit/type/incr | 32 | 32 | 0 | **PASSES** |
| unit/type/string | 82 | 106 | 0 | **PASSES** (was 2 err) |
| unit/type/list | 100 | 100 | 8 | aborts: tcl `can't read "cmd"` (framework var; investigate) |
| unit/type/hash | 79 | 79 | 3 | aborts: `DUMP` unknown command |
| unit/type/set | 115 | 115 | 4 | yes |
| unit/type/zset | 305 | 318 | 8 | yes (ZRANDMEMBER crash guard landed) |
| unit/expire | 62 | 62 | 3 | aborts: `CLIENT IMPORT-SOURCE` unknown subcommand |
| unit/keyspace | 45 | 45 | 0 | aborts: stream-cgroups COPY (ERR syntax) |
| unit/scan | 10 | 24 | 0 | yes (was aborting) |
| unit/bitops | 46 | 49 | 2 | yes (was aborting) |
| unit/other | 8 | 25 | 1 | aborts: CONFIG SET "Unsupported CONFIG parameter" |
| unit/sort | 45 | 44 | 22 | yes |
| **total** | **929** | **999** | **51** | |

Quick wins to unmask downstream tests: **CLIENT IMPORT-SOURCE** stub (expire),
the unsupported **CONFIG parameter** in `other`, and the `list` tcl `cmd` var
(may be a dreads reply shape the framework doesn't expect). DUMP/RESTORE (hash)
and stream-COPY (keyspace) are the bigger blockers.

## Layer 4 — open failures (sweep7/8)

### Blocking service order (list 9, zset ~5)
Multiple blocked clients must be served FIFO and chained wakes must cascade
(Linked LMOVEs, Circular BRPOPLPUSH, BLMPOP/BZMPOP "multiple blocked
clients", BZPOPMIN re-block after expired). dreads' poll-retry model wakes
all waiters and races them. BRPOPLPUSH with a wrong-typed *destination* must
surface the error on wake (currently swallowed by the firstPass rule shared
with B*MPOP). Needs a real blocked-client queue (FIFO per key) instead of
the retry loop — design work, not a patch.

### Not implemented (each aborts its file)
- DUMP/RESTORE (hash file; RDB payload + CRC — sizeable).
- HSCAN NOVALUES (scan file, to confirm).
- COPY of a stream with consumer groups (keyspace).

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
