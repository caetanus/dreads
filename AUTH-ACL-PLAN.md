# AUTH + ACL — implementation plan

Branch: `auth-acl`. Goal: real authentication and a real ACL engine, closing
`unit/auth` (13 tests), `unit/acl` (145), `unit/acl-v2` (164), and the scripting
suite's `Script ACL check` (`redis.acl_check_cmd`). Today AUTH is a stub error,
HELLO AUTH is accepted-and-ignored, and `ACL` answers canned values.

Constraints (house rules): no C-isms (use `Dict`/jah containers + RAII, not
hand-rolled malloc lists), no gambi, and **benchmark after** — enforcement sits
on the command hot path, so the unrestricted default user must stay ~free.

---

## 1. Data model

A global user registry (single keyspace writer thread owns it, like `gDbs`):

```
struct AclUser {
    const(char)[] name;              // malloc'd
    bool enabled;                    // `on` / `off`
    bool nopass;                     // accepts any password
    Vector!(ubyte[32]) passwords;    // SHA-256 hashes (jah.Vector)
    AclPerm root;                    // the base permission set
    Vector!AclPerm selectors;        // ACL v2 (…) extra sets
}

struct AclPerm {
    bool allCommands;                // +@all seen with nothing removed after
    CmdBits allowed;                 // per-command allow bitset (see §4)
    CmdBits denied;                  // explicit -cmd after a +@cat
    bool allKeys;
    Vector!KeyPat keyPats;           // ~pat with R/W flags (%RW ~pat)
    bool allChannels;
    Vector!(const(char)[]) chanPats; // &pat
}
struct KeyPat { const(char)[] pat; bool read, write; }
```

- The **default** user always exists: `on nopass ~* &* +@all` (or password-
  protected once `requirepass` is set — see §6). Registry seeded at boot.
- Registry: `Dict!(AclUser)` keyed by name, or a small purpose struct in a new
  `source/dreads/acl.d`. Non-GC; parsing/formatting is control-plane so a scoped
  GC alloc there is acceptable if it keeps the code clean (the hot path never
  allocates).

## 2. Per-connection state (`Conn`, server.d)

Add `AclUser* user;` (current authenticated user, default = the `default` user)
and drop the "no auth" shortcut in HELLO. AUTH/HELLO switch `c.user`.

## 3. Commands

- **AUTH** `[username] password` — one-arg form authenticates the `default`
  user; two-arg picks a user. Verify password (SHA-256 vs stored hashes, or
  nopass), user `enabled`. On success set `c.user`, reply `+OK`; else
  `-WRONGPASS`. The current stub message stays for the no-password-configured
  one-arg case only.
- **HELLO … AUTH u p** — same path (currently ignored).
- **ACL** subcommands (by test frequency): `SETUSER` (136), `GETUSER` (62),
  `LOG` (45), `LOAD`/`SAVE` (23/7, aclfile), `CAT` (11), `DELUSER` (9),
  `WHOAMI` (6), `LIST` (5), `GENPASS` (5), `USERS`, `DRYRUN`.

## 4. Command categories + the permission bitset

Enforcement needs a command → categories map and a per-command index. Redis
tags every command with categories (`@read @write @keyspace @dangerous
@admin @scripting @fast @slow …`). dreads has the dispatch table but no
metadata, so build one table in `acl.d`:

```
struct CmdInfo { string name; uint cats; } // cats = bitmask of AclCat
```

`CmdBits` is a bitset over the command index (fixed count, ~240). `+@cat`
sets the bits of every command in `cat`; `+cmd` sets one bit; `-…` clears.
`+cmd|subcmd` (container subcommands) needs a sub-rule map on the parent —
Phase 2 (a `Vector!(subname)` per allowed/denied container command).

Building the category table is the bulk of the mechanical work; derive it from
Valkey's `commands/*.json` `acl_categories` to stay honest (script it, don't
guess), and keep it beside the dispatch as the single source.

## 5. Enforcement (the hot-path integration)

In `handleCommand`/`dispatch`, before executing, for `c.user` that is **not**
the unrestricted `+@all ~* &*` user:

1. command allowed? (root or any selector allows the command bit)
2. every key argument matches an allowed key pattern with the right R/W flag
   (needs the command's key-spec — dreads already extracts keys for
   COPY/cluster; reuse/extend that);
3. pub/sub channels match an allowed channel pattern (for SUBSCRIBE/PUBLISH…).

Deny → `-NOPERM …` (Redis's exact strings: `this user has no permissions to
run the '<cmd>' command`, `… to access one of the keys …`, `… channels …`) and
an ACL LOG entry (§7).

**Perf:** gate the whole block behind `if (c.user is gDefaultUser &&
gDefaultUnrestricted)` (a cached bool) so the no-ACL common case is a single
pointer compare — then benchmark SET/GET to confirm no regression.

## 6. requirepass ↔ default user

`CONFIG SET requirepass X` sets the default user's password (and `resetpass`
when empty), matching Redis. `requirepass` GET returns it (already `sensitive`
in CONFIG INFO).

## 7. ACL LOG, GENPASS, DRYRUN

- **LOG**: a ring buffer of denied attempts (reason, object, username, cmd,
  client-info, count, timestamps). `ACL LOG [n]` / `ACL LOG RESET`.
- **GENPASS [bits]**: hex from the deterministic RNG (dreads.rand) — 256 bits
  default.
- **DRYRUN user cmd [args]**: evaluate without running; reply `OK` or the
  NOPERM reason.

## 8. Scripting: `redis.acl_check_cmd`

Wire the connection's `AclUser*` into the script bridge context (it already
carries db/clock via the pool round-trip — add the user). `luaAclCheckCmd`
evaluates the real rules; unknown command → the `Invalid command passed to
server.acl_check_cmd()` error. Closes `Script ACL check`.

## 9. Password storage + brute-force resistance

Two separate concerns — conflating them is the mistake.

**Stored hash = SHA-256 (parity, non-negotiable).** Valkey's ACL wire format is
SHA-256 hex: `>pass` hashes then stores, `#<sha256hex>` sets a hash directly,
`<pass`/`!hash` remove, and **`ACL GETUSER` returns the SHA-256 hash** — the
acl.tcl tests assert this. A slow KDF (Argon2/scrypt/bcrypt) in the *stored*
hash would break `#hash` and GETUSER, so it is out. Add SHA-256 (`std.digest.
sha : SHA256`, or a small `@nogc` impl for the zero-GC bar; hashing is
control-plane). **Compare in constant time** (no early-out on the first
differing byte) to deny timing oracles.

**Brute-force resistance lives in AUTH, not the hash.** A fast hash is only safe
if you cap attempt throughput — so dreads adds a slow-auth layer Valkey does not
have, on top of the parity-correct storage:

- **Non-blocking exponential backoff on failure**, keyed by source (and/or
  username): each failed AUTH grows a per-key delay before the reply. The delay
  is a **vibe fiber `sleep`** (yields the fiber) — *never* a `Thread.sleep`,
  which would stall the single event-loop thread for every other client. Only
  the attacking connection waits; the server stays responsive. Reset on success.
- **Optional lockout** after N consecutive failures (config
  `acl-auth-max-failures`), with a cooldown.
- **Optional per-AUTH work factor** (`acl-auth-min-delay-ms`): a floor delay on
  *every* attempt (even the first), capping global attempts/sec regardless of
  source spoofing.
- Failures also feed **ACL LOG** (§7), so the operator sees the brute-force.

This keeps the hash Valkey-compatible while making online brute-force
impractical — a real security edge over stock Valkey, at zero parity cost.
(An *offline* attack on a leaked SHA-256 hash is only mitigated by password
strength; that limitation is inherited from the Valkey format and documented,
not hidden.)

## 10. Replication / persistence

ACL is node-local operational config (like Redis before the ACL-in-repl work).
Decision to confirm with the owner: keep ACL out of the Raft/AOF log (managed
per node via `aclfile` / CONFIG), OR replicate SETUSER/DELUSER as their own log
entries. Default plan: **out of the log**, `ACL LOAD`/`SAVE` against an
`aclfile` — consistent with "the log is ours; ACL is control plane." Record in
DRIFT.md either way.

---

## Phasing

- **Phase 1 — core (closes auth.tcl + most of acl.tcl + scripting ACL):**
  user registry + `default` user, AUTH, HELLO AUTH, requirepass wiring,
  SETUSER/GETUSER/DELUSER/LIST/USERS/WHOAMI/CAT, rule parser (on/off, passwords,
  +cmd/-cmd/+@cat, ~key/allkeys, &chan/allchannels, reset), command-category
  table, command+key+channel enforcement with NOPERM strings, `acl_check_cmd`.
- **Phase 2 — ACL v2 (acl-v2.tcl):** selectors `(...)`, `%R~`/`%W~`/`%RW~` key
  read/write flags, `+cmd|subcmd` container subcommand rules, DRYRUN.
- **Phase 3 — ops:** ACL LOG, GENPASS, LOAD/SAVE (aclfile), GETUSER/LIST exact
  formatting parity, `ACL CAT <category>` listing.

## New / touched files

- `source/dreads/acl.d` — **new**: AclUser/AclPerm, registry, rule parse/format,
  category table, enforcement predicate, ACL LOG.
- `source/dreads/server.d` — Conn.user, AUTH/HELLO wiring (with the
  non-blocking failure backoff + constant-time compare), enforcement call in
  the command loop, `ACL`/`AUTH` real handlers, requirepass hook. New config:
  `acl-auth-min-delay-ms`, `acl-auth-max-failures`.
- `source/dreads/commands.d` — remove AUTH stub; expose key-spec extraction for
  enforcement.
- `source/dreads/scripting.d` — carry `AclUser*` into the bridge; real
  `luaAclCheckCmd`.
- `DRIFT.md` — ACL semantics + the replication decision.
- Internal unit tests per behavior; then run `unit/auth`, `unit/acl`,
  `unit/acl-v2` in the blackbox sweep, and **re-run the benchmark** to confirm
  the enforcement gate is free for the default user.

## Risks / open questions

- **Command-category table accuracy** — derive from Valkey `commands/*.json`,
  don't hand-wave; it's the correctness backbone.
- **Key extraction** for arbitrary commands (getkeys) — dreads has partial key
  handling; some commands need proper key-spec (first/last/step or MOVABLEKEYS).
- **Perf** of enforcement — must be a single compare for the unrestricted user.
- **Replication of ACL** — owner decision (default: out of the log).
