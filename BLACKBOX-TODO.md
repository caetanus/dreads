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

## Progress per file (session start → sweep8, db 9, skipfile, fresh server per file)

| File | ok before | ok now | err | runs to end? |
|---|---|---|---|---|
| unit/type/incr | 32 | 32 | 0 | **PASSES** |
| unit/type/string | 21† | 82 | 2 | yes |
| unit/type/list | 32†‡ | 100 | 9 | yes |
| unit/type/hash | 31† | 79 | 4 | aborts at DUMP (not implemented) |
| unit/type/set | 90 | 115 | 2 | yes |
| unit/type/zset | 110† | 305 | 9 | crashed at ZRANDMEMBER overflow (guard landed after sweep8) |
| unit/expire | 30 | 62 | 4 | yes |
| unit/keyspace | 42 | 45 | 1 | aborts: stream-cgroups COPY (syntax) |
| unit/scan | 10 | 10 | 3 | aborts (HSCAN NOVALUES?) |
| unit/bitops | 11† | 46 | 1 | aborts: SETBIT fuzz empty reply |
| unit/other | 0† | 8 | 3 | aborts: CONFIG INFO `flags` field |
| unit/sort | 0† | 45 | 10 | yes |
| **total** | **409** | **929** | **48** | |

† aborted at first exception; ‡ plus a 600 s hang.

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
- **AOF / raft SELECT-logging**: replay and the command log run on `gDbs[0]`
  (`gKeys`). A write on db N must log a `SELECT N` marker so replay targets
  the right dataspace ("alterar o log = dizer qual dataspace foi commitado").
- **Eviction, blocking re-dispatch, and MULTI-replay** paths still use db 0
  (`gKeys`) rather than the originating connection's db. SELECT inside MULTI
  is queued but not honored on EXEC replay (dispatch `ks` is fixed at EXEC).
- **CLIENT LIST/INFO** now reports the real db, but `addr` is still `?`.

## Method to complete the catalog
1. Land group 1 (CONFIG) + group 2 (DEBUG stubs) — the two blockers gating
   the most files.
2. Re-run the sweep; record the next layer of `[err]` per file.
3. Repeat until files run to completion, then log the true per-test failures.
