# Blackbox compatibility TODO

Failures found running the **Valkey test suite** against a live dreads in
external mode (`./runtest --host … --port … --single <file>`), on **db 9**
(no `--singledb`, exercising multi-DB). Valkey is used as a read-only oracle.

> These are the **first blocking failure per file**. The TCL runner aborts a
> file at its first `[exception]`, so `ok=N` undercounts and downstream
> failures are **masked** until the blocker above them is cleared. Re-run each
> file after fixing a blocker to reveal the next layer.

## First blocker per file (db 9)

| File | ok before block | First blocker |
|---|---|---|
| unit/type/incr | 14 | `ERR DEBUG subcommand not supported` |
| unit/type/string | 21 | `ERR Unsupported CONFIG parameter` |
| unit/type/list | 0 | `ERR Unsupported CONFIG parameter` |
| unit/type/hash | 1 | `ERR Unsupported CONFIG parameter` |
| unit/type/set | 0 | `ERR Unsupported CONFIG parameter` |
| unit/type/zset | 0 | `ERR Unsupported CONFIG parameter` |
| unit/expire | 30 | `ERR Unsupported CONFIG parameter` |
| unit/keyspace | 28 | `ERR wrong number of arguments for 'copy'` (COPY … DB n) |
| unit/scan | 3 | `ERR syntax error` (SCAN … TYPE) |
| unit/bitops | 11 | `ERR syntax error` |
| unit/other | 0 | `ERR wrong number of arguments for 'object'` |
| unit/sort | 0 | `ERR Unsupported CONFIG parameter` |

## Blockers, grouped

### 1. CONFIG SET encoding thresholds — biggest unblocker
Type tests flip encoding thresholds to force listpack↔hashtable/skiplist
transitions, then `assert_encoding`. dreads rejects the params with
`ERR Unsupported CONFIG parameter`, aborting the file immediately. Accept
(store, even as a no-op where dreads has a single encoding) at least:

- `list-max-listpack-size` / `list-max-ziplist-size`, `list-compress-depth`
- `hash-max-listpack-entries` / `-value` (+ `-ziplist-` aliases)
- `set-max-listpack-entries`, `set-max-intset-entries`
- `zset-max-listpack-entries` / `-value` (+ `-ziplist-` aliases)
- `stream-node-max-entries`
- `proto-max-bulk-len`, `client-query-buffer-limit`
- `lazyfree-lazy-server-del`, `notify-keyspace-events`

Unblocks: string, list, hash, set, zset, expire, sort (and more).

### 2. DEBUG subcommands
`ERR DEBUG subcommand not supported`. Tests lean on:
`DEBUG JMAP`, `DEBUG OBJECT`, `DEBUG RELOAD`, `DEBUG SET-ACTIVE-EXPIRE`,
`DEBUG STRINGMATCH-LEN`, `DEBUG QUICKLIST-PACKED-THRESHOLD`,
`DEBUG SLEEP`, `DEBUG JMAP`. Stub the safe no-ops (`JMAP`, `SLEEP`,
`SET-ACTIVE-EXPIRE`, `QUICKLIST-PACKED-THRESHOLD`) → `+OK`; implement
`DEBUG OBJECT` (encoding/refcount) and `DEBUG RELOAD` (AOF round-trip) where
feasible. Unblocks: incr + most type files past encoding asserts.

### 3. COPY … DB n [REPLACE] — multi-DB gap
`copy src dst DB 10` and `copy src dst DB 10 REPLACE`. dreads' COPY only
takes 2 args. Extend to cross-db (write into `gDbs[n]`), honoring `REPLACE`
and the same-key/same-db guard, mirroring MOVE.

### 4. OBJECT arity/subcommands
`ERR wrong number of arguments for 'object'` in unit/other. Audit
`OBJECT ENCODING|REFCOUNT|IDLETIME|FREQ|HELP` arity + outputs.

### 5. SCAN TYPE option + error parity
`scan 0 type string` must filter by type and, for an unknown type, error
`*unknown type name*`. dreads returns a generic `ERR syntax error`.

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
