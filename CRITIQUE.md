# dreads: critical review toward drop-in Redis/Valkey compatibility

This is a sober review of the repository as it stands on the current working
tree. It assumes the explicit product goal is not just "a fast Redis-like
engine", but a practical drop-in replacement for Redis/Valkey on the supported
surface.

## Executive take

dreads is technically serious. The core architecture is coherent: malloc-backed
data structures, arena scratch allocation, a mostly `@nogc` command path,
deterministic propagation for persistence/replication, AOF rewrite, Lua
sandboxing, Pub/Sub work, and Raft integration are all meaningful engineering
choices. The performance claims are plausible because several wins are
structural, not cosmetic: lower hot-path allocation cost, deterministic/batched
AOF logging, and indexed Pub/Sub pattern matching.

The main risk is not that the project is shallow. The risk is that "drop-in
Redis/Valkey" is an extremely high bar. Performance can be ahead while still
leaving compatibility, operational behavior, and failure semantics short of what
existing clients assume.

## Current health

The working tree is not green at the time of this review.

`dub test` ran 169 tests and failed 1:

- `dreads.notify.unittest_L150`
- failure: `ArraySliceError` in `source/dreads/notify.d:121`
- location: `flushPendingNotify()`

This matters because the current local diff expands keyspace notification
coverage in `commands.d` and `zsetops.d`. The failing area is therefore directly
related to active work, not an unrelated corner.

At the time the failing test was observed, the working tree had local changes
in notification-related command files. Treat the exact dirty-file list as
ephemeral; re-run `git status --short` before using this section as a release
gate.

> **Resolved (2026-07-11).** `flushPendingNotify()` is fixed (bounds-checked) and
> the suite is green — **176 tests, 0 failed**. The inline-protocol gap surfaced
> during benchmarking is also fixed (see #5/#10). Performance was measured
> head-to-head against Valkey 9.1 — see [`bench/valkey-comparison.md`](bench/valkey-comparison.md).

## What looks strong

- The project is not a toy clone. It has real implementations for the data
  structures and protocol path instead of wrapping an existing Redis server.
- The zero-GC/arena story is credible in the code structure, especially around
  RESP parsing, command dispatch, reply construction, and keyspace objects.
- The AOF design is more thoughtful than "append raw commands and hope":
  commands with time-derived state are resolved to deterministic forms such as
  `PEXPIREAT`.
- AOF rewrite / snapshot dumping skips expired keys and emits canonical rebuild
  commands.
- `lookup()` removes expired keys and emits `expired` notifications, so lazy
  expiry is not as weak as a naive lazy-only implementation.
- `SCAN` and `RANDOMKEY` already filter expired keys.
- The documentation is unusually honest about semantic drift.
- The test suite covers a broad range of behavior: recovery, sandboxing,
  protocol errors, zsets, streams, bitmap, raftlog, and robustness cases.
- The Pub/Sub performance direction is strong because it changes the asymptotic
  cost of pattern publish matching.

## Highest priority drop-in risks

### 1. Blackbox compatibility must become the source of truth

The planned reverse-blackbox workflow against Valkey is the right direction.
For drop-in compatibility, docs are not enough and manual command audits will
miss edge cases.

The harness should compare:

- exact RESP shape, including nulls, arrays, maps, attributes, doubles, and
  integer formatting;
- exact error classes and messages where client behavior depends on them;
- final state after command sequences, not only individual replies;
- TTL/PTTL with tolerance windows;
- key type transitions and empty-container deletion;
- behavior after syntax/type errors;
- script return conversion and script error behavior;
- Pub/Sub and keyspace notification side channels;
- persistence/restart state;
- replica/raft behavior where it intentionally replaces Redis replication.

Each mismatch should be classified as:

- must fix;
- accepted product drift;
- nondeterministic / tolerance-based;
- oracle gap;
- dreads crash/assert/protocol violation.

> **Response (2026-07-11).** The *performance* blackbox exists —
> [`bench/redis-bench.sh`](bench/redis-bench.sh) + [`bench/mqtt-bench.sh`](bench/mqtt-bench.sh)
> compare data ops (min/med/max), transactions, AOF, and pub/sub (fan-out +
> pattern scaling) against Valkey 9.1 and Mosquitto, one-server-at-a-time, pinned.
> It already caught real bugs: a per-message write syscall in fan-out (fixed,
> ~87×) and the missing inline protocol. The *correctness* blackbox (diffing RESP
> shape / error class / final state against a live Valkey oracle) is still the
> single highest-value open item.

### 2. Documentation drift is already visible

Some docs appear stale or internally inconsistent relative to the current
implementation and project claims.

Examples:

- `DRIFT.md` still says RESP2-only / `HELLO 3` returns `NOPROTO`, while the
  project owner states RESP3 exists.
- The Raft section lists "phase-1 gaps" and then later says dynamic membership
  and snapshots are supported.
- The expiration section is too pessimistic. The code already handles expired
  keys correctly in several paths: `lookup()`, notifications-on-access,
  `SCAN`, `RANDOMKEY`, AOF rewrite, and snapshots.

For a drop-in goal, stale drift docs are dangerous because they blur whether a
difference is deliberate, fixed, or unknown.

> **Partly done.** RESP3 is implemented (handshake, encoders, pub/sub push) and
> benchmarked; the expiry and RESP3 lines in `DRIFT.md` are updated. Remaining:
> reconcile the Raft "phase-1 gaps" wording against the current membership/snapshot
> support.

### 3. Expiry semantics: remaining visible gaps

Lazy expiry is not inherently a blocker. For normal reads, a key expired by
deadline is dead when read. The current implementation is stronger than a
generic lazy-only design because `lookup()` deletes and emits `expired`, and
AOF/snapshot dumping skips expired keys.

Remaining likely drift:

- `DBSIZE` uses raw keyspace length, so it can count untouched expired keys.
- `KEYS` iterates raw keyspace and can list untouched expired keys.
- `INFO` reports raw key count and `expires=0`, which is not Redis-compatible.
- expired keys can retain memory until touched or compacted;
- active expiry event timing differs from Redis's active cycle;
- maxmemory eviction samples raw slots and may evict an already-expired key via
  the eviction path rather than treating it as expiry first.

For drop-in behavior, `DBSIZE`, `KEYS`, `INFO`, and eviction interaction deserve
explicit tests against Valkey.

> **Fixed (commit `be4952b`).** Active expiration is now implemented as an opt-in
> **drop-soon index** (deadline → keys, swept by a 1s timer), gated by the
> `active-expire` config (default **off** — measured ~40% SET-EX cost, so lazy
> stays the fast default; on = bounded memory). Command semantics are now
> Redis-shaped regardless of the setting: **`KEYS` skips logically-expired keys**,
> **`INFO` reports real `db0:keys=N,expires=M`**, and **`DBSIZE` stays raw** (Redis
> counts unreaped-expired too). Unit tests added (`ext.expiry_visibility`,
> `ext.info_keyspace`, drop-soon Keyspace test). Still open: maxmemory-eviction ↔
> expiry interaction, and a Valkey blackbox for these.

### 4. Keyspace notifications need hardening

Keyspace notifications are important because they are both user-visible behavior
and part of the Pub/Sub performance story.

Current concerns:

- there is an active failing unit test in notification flushing;
- docs say events do not fire through the Raft apply path;
- `CONFIG SET notify-keyspace-events` is documented as not wired;
- event coverage is still narrower than Redis/Valkey for some classes;
- event timing for expiration is access-driven, not active-cycle-driven.

For drop-in compatibility, notification behavior should be blackbox-tested as a
first-class output stream, not only inferred from command replies.

> **Improved.** The failing flush test is fixed. Active-cycle expiry now exists:
> when `active-expire` is on, the 1s drop-soon sweep fires `expired` events (it
> reuses the same deferred-publish queue and flushes them). Still open: events
> through the Raft apply path, wider event-class coverage, and a pub/sub
> side-channel blackbox test.

### 5. RESP3 must be treated as a compatibility matrix, not a checkbox

If RESP3 is implemented, the next risk is surface completeness:

- `HELLO 3` negotiation;
- push messages;
- maps/sets/attributes;
- null bulk vs null array vs RESP3 null;
- client tracking if supported or rejected;
- Pub/Sub framing under RESP3;
- error formatting and client library expectations.

Drop-in means common Redis clients can negotiate and continue normally. A small
RESP3 subset may work for hand tests but still break real clients.

> **Advancing.** RESP3 handshake, the map/set/push/double/null encoders, and the
> lazy reply oracle are implemented; pub/sub push framing is per-subscriber. A
> real gap was just closed: the parser rejected **inline commands** (any request
> not starting with a RESP marker) with a hard protocol error, so `redis-cli
> --pipe` and inline clients failed — now supported (commit `76df819`). A full
> client-library compatibility matrix + client-tracking decision remain TODO.

### 6. Lua compatibility remains high-risk

The implementation uses system Lua 5.4 with a custom sandbox, not Redis's
patched Lua 5.1 environment. That can be a reasonable engineering choice, but
it is a drop-in risk.

Areas to blackbox heavily:

- numeric conversion and integer/float edge cases;
- table-to-RESP conversion;
- error text and stack traces;
- `redis.call` vs `redis.pcall`;
- deterministic `math.random`;
- script cache behavior;
- time-dependent commands inside scripts;
- availability and behavior of helper libraries.

If dreads intentionally diverges here, it should be documented as product drift,
not accidental compatibility.

> **Unchanged.** Still system Lua 5.4 + custom sandbox — accepted product drift,
> not yet audited against Redis's Lua 5.1 semantics.

### 7. Transactions and watch semantics are intentionally stricter

`WATCH` is documented as using a global write epoch, so any write can abort
`EXEC`, not only writes to watched keys. That is safe but not Redis-compatible.

This can break real clients that use `WATCH` under concurrent unrelated writes.
For drop-in behavior, this is one of the more important semantic gaps to close
or clearly mark as accepted drift.

> **Open.** Global-epoch WATCH is unchanged — still safe-but-stricter than Redis.
> Not yet decided/closed; the most important remaining *semantic* gap.

### 8. SCAN behavior is filtered but cursor semantics differ

`SCAN` filters expired keys, which is good. The remaining drift is cursor
semantics: the cursor is a table slot index. Redis uses cursor logic designed to
handle incremental hash table traversal across resizing.

Potential effects:

- misses or duplicates during rehash/mutation;
- different cursor progression;
- surprising behavior for clients that assume Redis-like scan stability.

This may be acceptable in practice, but it should be blackbox-tested under
mutation and resizing.

> **Unchanged.** Cursor is still a slot index; not yet blackbox-tested under
> rehash/mutation.

### 9. "Random" commands are deterministic

Commands such as `RANDOMKEY`, `SPOP`, `SRANDMEMBER`, `ZRANDMEMBER`, and
`HRANDFIELD` are documented as deterministic in places.

This is often fine for persistence and testing, but it is visible behavior.
Some clients use these commands expecting sampling behavior. For drop-in
compatibility, deterministic selection is a real semantic difference.

> **Accepted drift.** These stay deterministic — required by the deterministic
> AOF/Raft propagation model (the resolved-command log). Documented, not a bug.

### 10. INFO, ROLE, WAIT, and operational command surfaces matter

Drop-in clients and operators often probe server identity and capabilities.
Partial or stubbed server commands can break otherwise-compatible workloads.

High-value surfaces:

- `INFO`;
- `ROLE`;
- `WAIT`;
- `COMMAND`;
- `CLIENT`;
- `CONFIG GET/SET`;
- `HELLO`;
- `ACL` / `AUTH` behavior;
- persistence commands such as `SAVE`, `BGSAVE`, `BGREWRITEAOF`, `LASTSAVE`.

Even when a feature is intentionally unsupported, the exact Redis/Valkey-style
failure mode matters for client compatibility.

> **Partial.** `INFO`'s keyspace line is fixed; `CONFIG GET/SET` gained
> `active-expire`; the inline protocol is fixed (affects any tool that probes over
> inline). `ROLE`/`WAIT`/`COMMAND`/`CLIENT`/`ACL`/`SAVE`/`BGSAVE`/`LASTSAVE` still
> need the exact Redis failure-mode audit against a live oracle.

### 11. Raft is a product differentiator but not Redis replication

Raft replication can be better than Redis async replication for durability and
linearizability. But "better guarantee" is still not the same wire/operational
surface.

Drop-in risks:

- clients expecting Redis replication commands;
- `ROLE`/`WAIT` semantics;
- reads served by stale or partitioned leaders/followers;
- script and transaction routing through Raft;
- membership changes and snapshot edge cases;
- behavior during leader churn.

The right message is not "same replication as Redis"; it is "Redis-compatible
single-node command surface with a stronger, different replication model".

> **Unchanged framing.** Still positioned exactly that way. This cycle's
> performance work was single-node; the Raft-surface drop-in audit (ROLE/WAIT
> semantics, stale-read policy, txn/script routing) is future work.

## Suggested near-term fixes

1. ~~Fix the current `flushPendingNotify()` test failure~~ — **done** (suite green, 176 tests).
2. Update `DRIFT.md` for RESP3/expiry/Raft/notifications — **partly done** (RESP3 + expiry updated; Raft wording pending).
3. Add Valkey blackbox for `DBSIZE`/`KEYS`/`SCAN`/`INFO`/untouched-expired — **behavior fixed + unit tests**; the live-oracle blackbox is still TODO.
4. Notification blackbox (pub/sub captured with replies) — **TODO**.
5. Decide global-epoch `WATCH` (temporary vs accepted drift) — **open**.
6. Compatibility matrix by command family (compatible / drift / unknown) — **TODO**.
7. Run sequences through restart/AOF rewrite — AOF write path is **benchmarked**; correctness-through-restart blackbox is **TODO**.

**Delivered this cycle (2026-07):** RESP3 + lazy reply oracle; inline protocol;
active expiration (opt-in drop-soon index) + `KEYS`/`INFO`/`DBSIZE` semantics;
keyspace-notification flush fix; pub/sub fan-out batching (~87×) and shared `*`
pmessage; INCR buffer reuse; and a reusable performance blackbox (`bench/`) with a
full Valkey 9.1 + MQTT comparison. **The dominant open item remains the
*correctness* blackbox against a live Valkey oracle** — items 3, 4, 6, 7.

## Bottom line

dreads looks like a strong engine with several legitimate performance wins over
Redis/Valkey in important workloads. The codebase has enough substance that the
drop-in goal is worth taking seriously.

The remaining work is less about raw speed and more about boring precision:
blackbox compatibility, exact protocol behavior, operational command surfaces,
expiration edge cases, notification side effects, and documentation discipline.
Those are the areas most likely to decide whether this becomes a fast Redis-like
database or a true drop-in replacement.
