# dreads — leftovers / open work

Snapshot 2026-07-12 (after the small-containers + value-union work merged to
master). Ordered roughly by size. Blackbox = the Valkey test suite run in
external mode (`/tmp/valkey/runtest --host … --port … --single <file>`), on db 9.

## Big architectural

- **emplace → 100% smart-pointer compatible, then move RObj off the raw union.**
  emplace containers must use **destructors (`~this`)**, not the manual `free()`
  convention — `Uniq!HashMap` today leaks (calls a `~this` that doesn't exist).
  A D `union` may not hold a destructor type, so once containers have `~this`,
  `RObj` can no longer be a raw by-value tagged union: move it to a `SumType` or
  a tagged owning-pointer. Then `SmallSet/SmallHash/SmallZSet` revert their raw
  malloc arrays to `emplace.Vector`. Core refactor, own branch. See memory
  `emplace-smartptr-raii`.
- **Finish the RESP3 value union.** `StrVal` is a tagged union but only `str`
  (embstr/raw) + `i64` are live; `f64`/`nul`/`big` are reserved enum slots. The
  original design is a union over int|string|float|null|bigint. See memory
  `string-value-storage`.
- **Patch Lua for VM-level readonly tables (the `_G` protection).** We run stock
  Lua 5.4, so `_G`/library protection is *emulated* with metatables — which
  cannot stop `rawset(_G, …)` or protect the shared *type* metatables
  (`getmetatable('').__index = …`). That's why `Try trick readonly table on
  basic types metatable` is skipped and why the guard is fragile. Redis/Valkey
  ship a patched Lua with `lua_enablereadonlytable`/`lua_isreadonlytable`
  (an out-of-band readonly flag on the VM's `Table`, enforced in `lvm.c`'s
  set-path so *every* write — raw or not — raises). Options: (a) vendor Lua and
  apply the same VM patch (matches Redis exactly, closes the skip, drops the
  metatable hack in scripting.d); (b) the userdata-`_G` route (make `_ENV`/`_G`
  a D-backed userdata so `rawset`/`setmetatable` simply don't apply — no patch,
  but `_G` stops being the real globals table). (a) is the faithful fix. Own
  branch; re-run unit/scripting after. See DRIFT.md "readonly emulation".

## Blackbox long-tail (type files run to completion; these are edge cases)

- **RESP3 double formatting.** ZPOPMIN/scores emit a RESP3 double `,-1` where
  the harness/Redis expects the bulk/int form for whole numbers — verify the
  `,` vs `$` reply choice per command. Likely clears a chunk of zset err.
- **Random-command distribution tail.** SRANDMEMBER/ZRANDMEMBER/HRANDFIELD now
  use real sampling; re-run the Valkey histogram tests, especially spilled Dict
  mode, to catch bias from probe clusters.
- **HGETEX** (Redis 7.4) not implemented. HGETDEL landed and has regression
  coverage.
- **SYNC/PSYNC** — decision recorded: dreads replicates via Raft so SYNC is a
  NOOP; the `attach_to_replication_stream` tests are N/A. Either reply something
  the helper tolerates or SKIP those tests via a versioned `--skipfile`. See
  memory `sync-is-noop-raft`. (User once said "SYNC/PSYNC responde OK, já está
  syncado" — but the helper needs a `$<len>` bulk, not `+OK`; revisit.)
- Smaller: SUNION hashtable+listpack mix, SPOP with count edge cases,
  ZRANGEBYSCORE/ZRANGEBYLEX bad-range specifiers, ZADD variadic partial-parse
  atomicity, ZLEXCOUNT, and any remaining exact arity/message splits surfaced by
  the next sweep.
- **CONFIG params / INFO metadata**: more encoding/misc directives and
  `CONFIG INFO` fields the suite flips or inspects (accept as advisory no-ops
  as needed). See `BLACKBOX-TODO.md` for the current first-blocker map.

## Small-containers polish

- **Single-allocation listpack**: fold the offset index into a length-prefixed
  blob (one alloc, not blob+index). Removes the second cache line → tips the
  cold-lookup benchmark from tie to a WIN. Cost: `keyAt` O(n); small SSCAN would
  return all at once (Redis listpack behaviour). See `bench/small-containers.md`.

## Multi-DB peripheral (still hardwired to db 0)

- Keyspace notifications publish `__keyspace@0__` (should use the conn's db).
- Standalone-AOF SELECT-logging still needs a per-db marker. Raft live log
  entries now carry the db index; raft **snapshot** dumping still needs per-db
  framing.
- Eviction / blocking re-dispatch / MULTI-replay paths need a multi-DB audit.
  SELECT-in-MULTI in particular must be verified against Valkey. See memory
  `multi-db`.
- `DEBUG RELOAD`/`LOADAOF` are stubbed no-ops — do a real in-process AOF
  round-trip (dreads HAS persistence; see memory `dreads-is-not-in-memory-only`).

## Misc

- #47: drop git submodules → dub registry deps for emplace/draft (blocked on
  registry ingestion).
- `dreads.map` (RB-tree) still ships C-style (raw Node*, delegate opApply) —
  D-ify per memory `dreads-code-style`.
