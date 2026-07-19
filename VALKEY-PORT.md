# Valkey suite → native UT port ("contrabando")

Bringing Valkey's `tests/unit/**/*.tcl` COVERAGE into `dub test` as native
in-process unittests (harness: `ks.run("CMD", args...)` → RESP reply, drives real
dispatch, no server). Server-only scenarios (protocol framing, multi-client,
keyspace push over the wire, replication, DEBUG-internals, config-limits) stay in
the blackbox sweep. Credit: THIRD_PARTY_NOTICES (Valkey BSD-3). One file per turn;
each real behaviour diff found becomes a fix, not a skip.

## Ported to UT
- [x] `type/incr` → `valkey_incr_tests.d` (4 tests: int paths, errors, INCRBYFLOAT ±)
- [x] `type/string` → `valkey_string_tests.d` (5 tests: set/get/mget/getset, mset/msetnx, extended SET NX/XX/GET, append/strlen/setrange/getrange, LCS)
- [x] `bitfield` → `valkey_bitfield_tests.d` (4 tests: set/get, #idx, overflow wrap/sat, RO)
- [x] `keyspace` → `valkey_keyspace_tests.d` (4 tests: del/exists/dbsize, rename edges, copy same-db, type)

## Pending — UT-portable (pure command logic)
type/string, type/list(+list-2,list-3), type/hash, type/set, type/zset,
type/stream(+cgroups), keyspace, scan, sort, expire, hashexpire, bitops, dump,
geo, hyperloglog, multi (MULTI/EXEC/DISCARD/WATCH logic), pubsub (channel bookkeeping)

## Blackbox-only (server / protocol / multi-client / replication / config)
protocol, networking, obuf-limits, querybuf, replybufsize, io-threads, tls, mptcp,
oom-score-adj, client-eviction, maxmemory, memefficiency, pause, wait, tracking
(invalidation push over wire), introspection(-2) (CLIENT KILL/bgsave, access-time),
info(-command), latency-monitor, slowlog, commandlog, shutdown, quit, auth, acl,
acl-v2, functions, scripting (mostly), aofrw, limits, violations, lazyfree

Notes: many "pending" files also have server-only sub-cases (DEBUG RELOAD, refcount,
wait_for_condition, deferring clients) — those specific cases go to blackbox; the
rest port to UT. The blackbox sweep (blackbox/sweep.sh) already validates the whole
files against a live dreads with Valkey as oracle.
