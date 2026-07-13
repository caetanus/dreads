/**
 * dreads.acl — ACL data model, rule parsing, and permission checks.
 *
 * Users live in a global registry (single-writer thread, like the keyspace).
 * Command permissions are a per-command bitset over the generated category
 * table (dreads.aclcat); key/channel permissions are glob patterns. Passwords
 * are Argon2id (dreads.authpw); a bare SHA-256 hex is accepted for Valkey
 * interop. See AUTH-ACL-PLAN.md. Control-plane: not on the command hot path
 * except the enforcement predicates (aclCanRunCmd/Key/Channel), which the
 * caller must gate behind "is this the unrestricted default user?".
 */
module dreads.acl;

import dreads.aclcat : AclCat, gCmdCats, aclCatBit;
import dreads.mem : mallocDup, freeSlice;
import emplace.vector : Vector;

/// All ACL category names (for `ACL CAT`), derived from the generated enum.
public immutable string[] aclCatNames = [__traits(allMembers, AclCat)];

/// True once ACL is in use (any SETUSER, or requirepass). While false — the
/// default deployment with only the unrestricted nopass `default` user — the
/// command loop skips enforcement entirely (one global bool test, zero cost).
public __gshared bool gAclActive;

enum size_t NCMD = gCmdCats.length;
enum size_t NW = (NCMD + 63) / 64;

/// A key pattern with its read/write applicability (%R~ / %W~ / %RW~ / ~).
struct KeyPat
{
    const(char)[] pat; // malloc'd
    bool read = true, write = true;
}

/// One permission set (a user's root, or later an ACL v2 selector).
struct AclPerm
{
    ulong[NW] allowed; // per-command allow bits (index = position in gCmdCats)
    bool allKeys;
    bool allChannels;
    Vector!KeyPat keyPats;
    Vector!(const(char)[]) chanPats; // malloc'd

    // command-rule text, kept verbatim so ACL GETUSER/LIST can echo the rules
    // instead of reverse-engineering the bitset (Valkey's approach — a category
    // grant like "+@read" prints as "+@read", not 40 expanded "+cmd"s).
    bool cmdBaseAll; // base is "+@all" (true) or "-@all" (false, the fresh default)
    Vector!(const(char)[]) cmdRules; // malloc'd delta tokens since the last base

    ~this() @nogc nothrow @trusted
    {
        foreach (i; 0 .. keyPats.length)
            freeSlice(keyPats[i].pat);
        foreach (i; 0 .. chanPats.length)
            freeSlice(chanPats[i]);
        foreach (i; 0 .. cmdRules.length)
            freeSlice(cmdRules[i]);
    }

    void reset() @nogc nothrow @trusted
    {
        allowed[] = 0;
        allKeys = allChannels = false;
        foreach (i; 0 .. keyPats.length)
            freeSlice(keyPats[i].pat);
        keyPats.clear();
        foreach (i; 0 .. chanPats.length)
            freeSlice(chanPats[i]);
        chanPats.clear();
        cmdBaseAll = false;
        clearCmdRules();
    }

    // reset the command-rule text to a base (does NOT touch the bitset)
    void setCmdBase(bool all) @nogc nothrow @trusted
    {
        cmdBaseAll = all;
        clearCmdRules();
    }

    void addCmdRule(scope const(char)[] tok) @nogc nothrow @trusted
    {
        cmdRules.put(cast(const(char)[]) mallocDup(tok));
    }

    private void clearCmdRules() @nogc nothrow @trusted
    {
        foreach (i; 0 .. cmdRules.length)
            freeSlice(cmdRules[i]);
        cmdRules.clear();
    }
}

/// An ACL user.
struct AclUser
{
    ulong id; // stable, monotonic — lets a script carry "which user" as a scalar
    const(char)[] name; // malloc'd
    bool enabled;
    bool nopass;
    Vector!(const(char)[]) passwords; // malloc'd hashes (Argon2id PHC or sha256 hex)
    AclPerm root;

    ~this() @nogc nothrow @trusted
    {
        freeSlice(name);
        foreach (i; 0 .. passwords.length)
            freeSlice(passwords[i]);
    }
}

// --- bitset helpers ----------------------------------------------------------

private void bitSet(ref ulong[NW] bits, size_t i, bool on) @nogc nothrow @safe
{
    if (on)
        bits[i >> 6] |= 1UL << (i & 63);
    else
        bits[i >> 6] &= ~(1UL << (i & 63));
}

private bool bitGet(ref const ulong[NW] bits, size_t i) @nogc nothrow @safe
{
    return (bits[i >> 6] & (1UL << (i & 63))) != 0;
}

import dreads.dict : Dict;

/// Command name (lowercase) -> its index in gCmdCats, or -1.
///
/// A compile-time `static foreach` string switch, NOT a runtime hash table: the
/// command names are known at build time, so LDC lowers this to a length-bucketed
/// comparison tree that lives in the instruction stream (stays hot in i-cache).
/// A Dict here cost ~11% on the enforcement hot path — its bucket array is touched
/// ONLY by ACL, so it fell cold between commands and every lookup ate a data-cache
/// miss. The switch has no cold data to miss.
int aclCmdIndex(scope const(char)[] lower) @trusted nothrow @nogc
{
    switch (lower)
    {
        static foreach (i, c; gCmdCats)
        {
    case c.name:
            return cast(int) i;
        }
    default:
        return -1;
    }
}

private void allowCategory(ref AclPerm p, uint catBit, bool allow) @nogc nothrow @safe
{
    foreach (i, ref c; gCmdCats)
        if (c.cats & catBit)
            bitSet(p.allowed, i, allow);
}

private void allowAll(ref AclPerm p, bool allow) @nogc nothrow @safe
{
    p.allowed[] = allow ? ulong.max : 0;
}

// --- rule parsing ------------------------------------------------------------

/// Apply one ACL rule token to a user. Returns false (and leaves an error in
/// `err`) on a malformed rule. Argon2id hashing of `>pass` happens here (a
/// control-plane cost, ~tens of ms — SETUSER is an admin command).
bool aclApplyRule(AclUser* u, scope const(char)[] tok, ref const(char)[] err) @trusted
{
    import dreads.authpw : hashPassword, verifyPassword;

    if (tok.length == 0)
        return true;
    // lowercase small keywords into a stack buffer for the switch
    if (tok == "on") { u.enabled = true; return true; }
    if (tok == "off") { u.enabled = false; return true; }
    if (tok == "nopass") { u.nopass = true; clearPasswords(u); return true; }
    if (tok == "resetpass") { u.nopass = false; clearPasswords(u); return true; }
    if (tok == "reset") { aclResetUser(u); return true; }
    if (tok == "allkeys" || tok == "~*") { u.root.allKeys = true; return true; }
    if (tok == "resetkeys") { u.root.allKeys = false; freeKeyPats(u); return true; }
    if (tok == "allchannels" || tok == "&*") { u.root.allChannels = true; return true; }
    if (tok == "resetchannels" || tok == "nochannels")
    {
        u.root.allChannels = false;
        freeChanPats(u);
        return true;
    }
    if (tok == "allcommands" || tok == "+@all")
    {
        allowAll(u.root, true);
        u.root.setCmdBase(true);
        return true;
    }
    if (tok == "nocommands" || tok == "-@all")
    {
        allowAll(u.root, false);
        u.root.setCmdBase(false);
        return true;
    }
    // accepted no-ops: sanitize-payload is a DUMP-safety flag (dreads has no
    // DUMP), clearselectors clears ACL v2 selectors (Phase 2 — not stored yet)
    if (tok == "sanitize-payload" || tok == "nosanitize-payload"
            || tok == "skip-sanitize-payload" || tok == "clearselectors")
        return true;

    switch (tok[0])
    {
    case '(':
        // ACL v2 selector `(rules)` — accepted but not yet enforced (Phase 2)
        return true;
    case '>':
        u.nopass = false;
        u.passwords.put(cast(const(char)[]) mallocDup(hashPassword(tok[1 .. $])));
        return true;
    case '#':
        {
            auto h = tok[1 .. $];
            // A bare SHA-256 hex (Valkey interop) OR a dreads Argon2id PHC string.
            // The latter is what the canonical propagation form carries so the
            // AOF/raft log stores the already-hashed password verbatim (no
            // re-hash on replay — deterministic, and Argon2 is too slow to redo).
            if (!isSha256Hex(h) && !(h.length >= 7 && h[0 .. 7] == "$argon2"))
            {
                err = "ERR Error in ACL SETUSER modifier '#': Invalid password hash"
                    ~ " provided. It must be exactly 64 characters and contain"
                    ~ " only lowercase hexadecimal characters";
                return false;
            }
            u.nopass = false;
            u.passwords.put(cast(const(char)[]) mallocDup(h));
            return true;
        }
    case '<':
        // remove a password by plaintext (verify against each stored hash)
        foreach (i; 0 .. u.passwords.length)
            if (verifyPassword(tok[1 .. $], u.passwords[i]))
            {
                removePasswordAt(u, i);
                break;
            }
        return true;
    case '!':
        {
            // remove a password by its (SHA-256 hex) hash
            auto h = tok[1 .. $];
            if (!isSha256Hex(h))
            {
                err = "ERR Error in ACL SETUSER modifier '!': Invalid password hash"
                    ~ " provided. It must be exactly 64 characters and contain"
                    ~ " only lowercase hexadecimal characters";
                return false;
            }
            foreach (i; 0 .. u.passwords.length)
                if (u.passwords[i] == h)
                {
                    removePasswordAt(u, i);
                    break;
                }
            return true;
        }
    case '+':
        if (!applyCmdRule(u, tok[1 .. $], true, err))
            return false;
        u.root.addCmdRule(tok);
        return true;
    case '-':
        if (!applyCmdRule(u, tok[1 .. $], false, err))
            return false;
        u.root.addCmdRule(tok);
        return true;
    case '~':
        u.root.keyPats.put(KeyPat(cast(const(char)[]) mallocDup(tok[1 .. $])));
        return true;
    case '%':
        return applyKeyPatWithFlags(u, tok[1 .. $], err);
    case '&':
        u.root.chanPats.put(cast(const(char)[]) mallocDup(tok[1 .. $]));
        return true;
    default:
        err = "ERR Error in ACL SETUSER modifier: Syntax error";
        return false;
    }
}

private bool applyCmdRule(AclUser* u, scope const(char)[] spec, bool allow,
        ref const(char)[] err) @trusted
{
    if (spec.length == 0)
        return false;
    if (spec[0] == '@')
    {
        auto bit = aclCatBit(spec[1 .. $]);
        if (bit == 0)
        {
            err = "ERR Error in ACL SETUSER modifier: Unknown command or category name in ACL";
            return false;
        }
        allowCategory(u.root, bit, allow);
        return true;
    }
    // strip a |subcommand suffix for now (phase 2 tracks per-subcommand rules)
    auto name = spec;
    foreach (i, ch; spec)
        if (ch == '|')
        {
            name = spec[0 .. i];
            break;
        }
    char[64] lb = void;
    if (name.length > lb.length)
        return false;
    foreach (i, ch; name)
        lb[i] = (ch >= 'A' && ch <= 'Z') ? cast(char)(ch + 32) : ch;
    auto idx = aclCmdIndex(lb[0 .. name.length]);
    if (idx < 0)
    {
        err = "ERR Error in ACL SETUSER modifier: Unknown command or category name in ACL";
        return false;
    }
    bitSet(u.root.allowed, idx, allow);
    return true;
}

private bool applyKeyPatWithFlags(AclUser* u, scope const(char)[] spec,
        ref const(char)[] err) @trusted
{
    // %[RW]~pattern
    bool r, w;
    size_t i = 0;
    for (; i < spec.length && spec[i] != '~'; i++)
    {
        if (spec[i] == 'R' || spec[i] == 'r')
            r = true;
        else if (spec[i] == 'W' || spec[i] == 'w')
            w = true;
        else
        {
            err = "ERR Error in ACL SETUSER modifier: Syntax error";
            return false;
        }
    }
    if (i >= spec.length || spec[i] != '~' || (!r && !w))
    {
        err = "ERR Error in ACL SETUSER modifier: Syntax error";
        return false;
    }
    u.root.keyPats.put(KeyPat(cast(const(char)[]) mallocDup(spec[i + 1 .. $]), r, w));
    return true;
}

private void clearPasswords(AclUser* u) @nogc nothrow @trusted
{
    foreach (i; 0 .. u.passwords.length)
        freeSlice(u.passwords[i]);
    u.passwords.clear();
}

/// Remove the password hash at `idx` (order-agnostic swap-remove).
private void removePasswordAt(AclUser* u, size_t idx) @nogc nothrow @trusted
{
    freeSlice(u.passwords[idx]);
    immutable last = u.passwords.length - 1;
    if (idx != last)
        u.passwords[idx] = u.passwords[last];
    u.passwords.popBack();
}

private void freeKeyPats(AclUser* u) @nogc nothrow @trusted
{
    foreach (i; 0 .. u.root.keyPats.length)
        freeSlice(u.root.keyPats[i].pat);
    u.root.keyPats.clear();
}

private void freeChanPats(AclUser* u) @nogc nothrow @trusted
{
    foreach (i; 0 .. u.root.chanPats.length)
        freeSlice(u.root.chanPats[i]);
    u.root.chanPats.clear();
}

/// `reset`: back to a fresh, disabled, no-permission user (keeping the name).
void aclResetUser(AclUser* u) @nogc nothrow @trusted
{
    u.enabled = false;
    u.nopass = false;
    clearPasswords(u);
    u.root.reset();
}

private bool isSha256Hex(scope const(char)[] s) @nogc nothrow @safe
{
    if (s.length != 64)
        return false;
    foreach (c; s)
        if (!((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')))
            return false;
    return true;
}

// --- permission checks (enforcement) ----------------------------------------

/// Can this user run the command at `cmdIdx` (aclCmdIndex)? A negative index
/// (command not in the table) is treated as allowed — unknown/new commands are
/// not gated until they are catalogued.
bool aclCanRunCmd(const(AclUser)* u, int cmdIdx) @nogc nothrow @safe
{
    if (!u.enabled)
        return false;
    if (cmdIdx < 0)
        return true;
    return bitGet(u.root.allowed, cast(size_t) cmdIdx);
}

/// Can this user touch `key` for the required access (read and/or write)?
bool aclCanAccessKey(const(AclUser)* u, scope const(char)[] key, bool needRead,
        bool needWrite) @nogc nothrow @trusted
{
    import dreads.commands : globMatch;

    if (u.root.allKeys)
        return true;
    foreach (i; 0 .. u.root.keyPats.length)
    {
        auto kp = u.root.keyPats[i];
        if ((needRead && !kp.read) || (needWrite && !kp.write))
            continue;
        if (globMatch(kp.pat, key))
            return true;
    }
    return false;
}

/// Does the user lack access to any key this command touches? Locates key args
/// via the generated spec tables (dreads.aclkeys): static index+range specs, and
/// numkeys-based ones (ZUNIONSTORE, LMPOP, …). EVAL/FCALL are NOT here — their
/// keys are checked transparently as each redis.call round-trips. A few
/// keyword-positioned keys (GEORADIUS STORE, XREAD STREAMS, SORT BY/GET, MIGRATE
/// KEYS) are still Phase 2.5. `arr` is the full command array (arr[0] = name).
bool aclKeyDenied(const(AclUser)* u, scope const(char)[] name, scope const(RVal)[] arr) @trusted nothrow @nogc
{
    import dreads.aclkeys : gCmdKeySpecs, gCmdKeyNumSpecs;

    if (u.root.allKeys)
        return false;
    immutable argc = cast(int) arr.length;
    // static index+range specs
    foreach (ref ks; gCmdKeySpecs)
    {
        if (ks.name != name)
            continue;
        immutable last = ks.last >= 0 ? ks.first + ks.last : argc + ks.last;
        for (int idx = ks.first; idx <= last && idx < argc; idx += ks.step)
        {
            if (idx < 1)
                continue;
            if (!aclCanAccessKey(u, arr[idx].str, ks.needR, ks.needW))
                return true;
        }
    }
    // numkeys-based specs: read N from arr[pos+keynumidx], keys from pos+firstkey
    foreach (ref ks; gCmdKeyNumSpecs)
    {
        if (ks.name != name)
            continue;
        immutable numAt = ks.pos + ks.keynumidx;
        if (numAt < 1 || numAt >= argc)
            continue;
        long nk;
        if (!parseKeyCount(arr[numAt].str, nk) || nk <= 0)
            continue;
        immutable first = ks.pos + ks.firstkey;
        for (long i = 0; i < nk; i++)
        {
            immutable idx = first + cast(int) i * ks.step;
            if (idx < 1 || idx >= argc)
                break;
            if (!aclCanAccessKey(u, arr[idx].str, ks.needR, ks.needW))
                return true;
        }
    }
    return false;
}

// small @nogc decimal parse for the numkeys arg (avoids importing the dispatch
// layer into acl.d); accepts a plain non-negative integer, rejects anything else.
private bool parseKeyCount(scope const(char)[] s, out long v) @nogc nothrow @safe
{
    if (s.length == 0 || s.length > 18)
        return false;
    long acc = 0;
    foreach (c; s)
    {
        if (c < '0' || c > '9')
            return false;
        acc = acc * 10 + (c - '0');
    }
    v = acc;
    return true;
}

/// Can this user use pub/sub `channel`? A plain channel (PUBLISH/SUBSCRIBE) is
/// glob-matched against the allowed `&patterns`; a subscribe PATTERN (PSUBSCRIBE)
/// is matched LITERALLY (a client may only subscribe to a pattern the ACL grants
/// verbatim) — mirrors Valkey's ACLCheckChannelAgainstList.
bool aclCanAccessChannel(const(AclUser)* u, scope const(char)[] channel,
        bool isPattern = false) @nogc nothrow @trusted
{
    import dreads.commands : globMatch;

    if (u.root.allChannels)
        return true;
    foreach (i; 0 .. u.root.chanPats.length)
    {
        auto pat = u.root.chanPats[i];
        if (isPattern ? (pat == channel) : globMatch(pat, channel))
            return true;
    }
    return false;
}

/// Snapshot the user's password hashes as isolated immutable strings, so the
/// slow Argon2 verify can run on a worker thread without touching the registry
/// (which the event-loop thread may mutate meanwhile). Empty when nopass.
immutable(string)[] aclPasswordHashes(const(AclUser)* u) @trusted
{
    auto n = u.passwords.length;
    auto arr = new string[n];
    foreach (i; 0 .. n)
        arr[i] = u.passwords[i].idup;
    return cast(immutable(string)[]) arr;
}

// --- rule description (ACL GETUSER / LIST) -----------------------------------

import dreads.mem : ByteBuffer;

/// Append the command-rule string ("-@all +get +set", "+@all", ...) — the base
/// plus the verbatim delta tokens kept at apply time, matching Valkey's output.
void aclDescribeCommands(const(AclUser)* u, ref ByteBuffer o) @trusted nothrow @nogc
{
    o.append(u.root.cmdBaseAll ? "+@all" : "-@all");
    foreach (i; 0 .. u.root.cmdRules.length)
    {
        o.append(" ");
        o.append(u.root.cmdRules[i]);
    }
}

/// Append the key-pattern string: "~*" if allkeys, else space-joined patterns
/// with their read/write qualifier (`~p`, `%R~p`, `%W~p`) as Valkey formats them.
void aclDescribeKeys(const(AclUser)* u, ref ByteBuffer o) @trusted nothrow @nogc
{
    if (u.root.allKeys)
    {
        o.append("~*");
        return;
    }
    foreach (i; 0 .. u.root.keyPats.length)
    {
        if (i)
            o.append(" ");
        auto p = u.root.keyPats[i];
        if (p.read && p.write)
            o.append("~");
        else
        {
            o.append("%");
            if (p.read)
                o.append("R");
            if (p.write)
                o.append("W");
            o.append("~");
        }
        o.append(p.pat);
    }
}

/// Append the channel-pattern string: "&*" if allchannels, else "&p" joined.
/// `withReset` prepends the `resetchannels` token (the ACL LIST / config-file
/// form uses it; the ACL GETUSER `channels` field does not).
void aclDescribeChannels(const(AclUser)* u, ref ByteBuffer o, bool withReset = false) @trusted nothrow @nogc
{
    if (u.root.allChannels)
    {
        o.append("&*");
        return;
    }
    if (withReset)
        o.append("resetchannels");
    foreach (i; 0 .. u.root.chanPats.length)
    {
        if (withReset || i)
            o.append(" ");
        o.append("&");
        o.append(u.root.chanPats[i]);
    }
}

// --- canonical propagation form (AOF / raft log) ----------------------------

import dreads.resp : repArrayHeader, repBulk, RVal;

/// RESP-encode the canonical `ACL SETUSER <name> reset …` that fully rebuilds
/// this user from scratch — the deterministic, idempotent propagation form for
/// the AOF/raft log. Passwords go in verbatim as `#<stored-hash>` (already
/// Argon2id-hashed), so replay never re-hashes and every node converges. Because
/// it is a full-state `reset`, compaction is trivial: the newest one wins.
void aclEncodeCanonicalSetuser(const(AclUser)* u, ref ByteBuffer o) @trusted nothrow @nogc
{
    size_t n = 5; // ACL SETUSER <name> reset <on|off>
    if (u.nopass)
        n++;
    n += u.passwords.length;
    n += u.root.allKeys ? 1 : u.root.keyPats.length;
    n += u.root.allChannels ? 1 : u.root.chanPats.length;
    n += 1 + u.root.cmdRules.length; // command base + delta tokens

    repArrayHeader(o, n);
    repBulk(o, "ACL");
    repBulk(o, "SETUSER");
    repBulk(o, u.name);
    repBulk(o, "reset");
    repBulk(o, u.enabled ? "on" : "off");
    if (u.nopass)
        repBulk(o, "nopass");

    static ByteBuffer tb; // TLS scratch to assemble prefixed tokens
    foreach (i; 0 .. u.passwords.length)
    {
        tb.clear();
        tb.append("#");
        tb.append(u.passwords[i]);
        repBulk(o, cast(const(char)[]) tb.data);
    }
    if (u.root.allKeys)
        repBulk(o, "~*");
    else
        foreach (i; 0 .. u.root.keyPats.length)
        {
            tb.clear();
            auto p = u.root.keyPats[i];
            if (p.read && p.write)
                tb.append("~");
            else
            {
                tb.append("%");
                if (p.read)
                    tb.append("R");
                if (p.write)
                    tb.append("W");
                tb.append("~");
            }
            tb.append(p.pat);
            repBulk(o, cast(const(char)[]) tb.data);
        }
    if (u.root.allChannels)
        repBulk(o, "&*");
    else
        foreach (i; 0 .. u.root.chanPats.length)
        {
            tb.clear();
            tb.append("&");
            tb.append(u.root.chanPats[i]);
            repBulk(o, cast(const(char)[]) tb.data);
        }
    repBulk(o, u.root.cmdBaseAll ? "+@all" : "-@all");
    foreach (i; 0 .. u.root.cmdRules.length)
        repBulk(o, u.root.cmdRules[i]);
}

/// Apply a canonical ACL command on the REPLAY / raft-commit path (never a live
/// client — the server layer handles those). `args` is the command minus "ACL":
/// `[SETUSER, name, reset, …]` or `[DELUSER, name, …]`.
void aclApplyCanonical(const(RVal)[] args) @trusted nothrow
{
    if (args.length < 2)
        return;
    static bool eqIC(scope const(char)[] a, scope const(char)[] b) @nogc nothrow @safe
    {
        if (a.length != b.length)
            return false;
        foreach (i, ch; a)
        {
            auto x = (ch >= 'A' && ch <= 'Z') ? cast(char)(ch + 32) : ch;
            auto y = (b[i] >= 'A' && b[i] <= 'Z') ? cast(char)(b[i] + 32) : b[i];
            if (x != y)
                return false;
        }
        return true;
    }

    if (eqIC(args[0].str, "setuser"))
    {
        auto u = aclGetOrCreate(args[1].str);
        if (u is null)
            return;
        const(char)[] err;
        foreach (ref r; args[2 .. $])
        {
            try
                aclApplyRule(u, r.str, err); // canonical never uses `>` (no hashing)
            catch (Exception)
            {
            }
        }
        gAclActive = true;
    }
    else if (eqIC(args[0].str, "deluser"))
    {
        foreach (ref a; args[1 .. $])
            aclDelUser(a.str);
    }
}

/// Append a canonical `ACL SETUSER … reset …` for EVERY user — the ACL half of
/// AOF rewrite / raft snapshot, so a compacted log still rebuilds the registry.
/// (default included: its line re-affirms the seeded state, or carries changes.)
void aclDumpUsers(ref ByteBuffer buf) @trusted nothrow @nogc
{
    aclEachUser((AclUser* u) @nogc nothrow {
        aclEncodeCanonicalSetuser(u, buf);
        return 0;
    });
}

/// Verify a plaintext password against this user (nopass or any stored hash).
/// Runs Argon2 — call OFF the event loop.
bool aclCheckPassword(const(AclUser)* u, scope const(char)[] pass) @trusted nothrow
{
    import dreads.authpw : verifyPassword;

    if (u.nopass)
        return true;
    foreach (i; 0 .. u.passwords.length)
        if (verifyPassword(pass, u.passwords[i]))
            return true;
    return false;
}

// --- global registry ---------------------------------------------------------

import dreads.dict : Dict;

private __gshared Dict!(AclUser*) gUsers;
private __gshared bool gAclInited;
private __gshared ulong gUserSeq; // monotonic user-id source (0 = "no user")

/// Malloc + construct a fresh, disabled, no-permission user with `name`.
AclUser* aclNewUser(scope const(char)[] name) @trusted nothrow
{
    import core.lifetime : emplace;
    import core.stdc.stdlib : malloc;

    auto u = cast(AclUser*) malloc(AclUser.sizeof);
    emplace(u);
    u.id = ++gUserSeq;
    u.name = cast(const(char)[]) mallocDup(name);
    return u;
}

/// The user with this stable id, or null (deleted). Linear over the registry —
/// used off the hot path (script redis.call enforcement), few users.
AclUser* aclUserById(ulong id) @nogc nothrow
{
    if (id == 0)
        return null;
    AclUser* found;
    aclEachUser((AclUser* u) @nogc nothrow {
        if (u.id == id)
        {
            found = u;
            return 1;
        }
        return 0;
    });
    return found;
}

/// Run ~this (frees name/passwords/patterns) and free the user.
void aclFreeUser(AclUser* u) @trusted nothrow
{
    import core.stdc.stdlib : free;

    destroy(*u);
    free(u);
}

/// Seed the always-present `default` user (`on nopass +@all ~* &*`). Idempotent.
void aclInit() @trusted
{
    if (gAclInited)
        return;
    gAclInited = true;
    auto def = aclNewUser("default");
    def.enabled = true;
    def.nopass = true;
    allowAll(def.root, true);
    def.root.setCmdBase(true); // rule text: "+@all" (matches the full bitset)
    def.root.allKeys = true;
    def.root.allChannels = true;
    gUsers.set("default", def);
}

/// The user by name, or null.
AclUser* aclUser(scope const(char)[] name) @nogc nothrow
{
    auto pp = gUsers.get(name);
    return pp ? *pp : null;
}

/// The user, creating a fresh disabled one if absent (ACL SETUSER semantics).
AclUser* aclGetOrCreate(scope const(char)[] name) @trusted nothrow
{
    if (auto u = aclUser(name))
        return u;
    auto u = aclNewUser(name);
    gUsers.set(name, u);
    return u;
}

/// Remove a user (never `default`). Returns true if one was deleted.
bool aclDelUser(scope const(char)[] name) @trusted nothrow
{
    if (name == "default")
        return false;
    auto pp = gUsers.get(name);
    if (pp is null)
        return false;
    auto u = *pp;
    gUsers.remove(name);
    aclFreeUser(u);
    return true;
}

/// Iterate every registered user (ACL LIST/USERS). Delegate returns non-zero to
/// stop early.
int aclEachUser(scope int delegate(AclUser* u) @nogc nothrow dg) @nogc nothrow
{
    return gUsers.opApply((const(char)[] k, ref AclUser* u) => dg(u));
}

/// Number of registered users.
size_t aclUserCount() @nogc nothrow
{
    return gUsers.length;
}

/// True when `u` is unrestricted (enabled ∧ every command ∧ all keys/channels)
/// — the enforcement fast case; such a user passes any `command ∈ cap_set` test.
bool aclUnrestricted(const(AclUser)* u) @nogc nothrow @safe
{
    if (!u.enabled || !u.root.allKeys || !u.root.allChannels)
        return false;
    foreach (w; u.root.allowed)
        if (w != ulong.max)
            return false;
    return true;
}

// --- tests -------------------------------------------------------------------

version (unittest) private alias freshUser = aclNewUser;
version (unittest) private alias freeUser = aclFreeUser;

unittest // the scripting-ACL scenario: bob = on >123 +@scripting +set ~x*
{
    import dreads.authpw : initAuthPw, configureArgon;

    initAuthPw();
    configureArgon(1, 8192); // fast for the test

    auto bob = freshUser("bob");
    scope (exit)
        freeUser(bob);
    const(char)[] err;
    foreach (rule; ["on", ">123", "+@scripting", "+set", "~x*"])
        assert(aclApplyRule(bob, rule, err), rule ~ " => " ~ err);

    // password
    assert(aclCheckPassword(bob, "123"));
    assert(!aclCheckPassword(bob, "456"));

    auto iSet = aclCmdIndex("set"), iHset = aclCmdIndex("hset"), iEval = aclCmdIndex("eval");
    assert(iSet >= 0 && iHset >= 0 && iEval >= 0);
    // +set and +@scripting granted; hset was not
    assert(aclCanRunCmd(bob, iSet));
    assert(aclCanRunCmd(bob, iEval)); // via @scripting
    assert(!aclCanRunCmd(bob, iHset));
    // keys: ~x* matches xx, not yy
    assert(aclCanAccessKey(bob, "xx", true, true));
    assert(!aclCanAccessKey(bob, "yy", true, true));

    configureArgon(2, 16 * 1024 * 1024);
}

unittest // reset + allkeys/allcommands + %RW flags
{
    auto u = freshUser("u");
    scope (exit)
        freeUser(u);
    const(char)[] err;
    foreach (r; ["on", "+@all", "allkeys", "allchannels"])
        assert(aclApplyRule(u, r, err), err);
    assert(aclCanRunCmd(u, aclCmdIndex("hset")));
    assert(aclCanAccessKey(u, "anything", true, true));
    assert(aclCanAccessChannel(u, "news"));

    assert(aclApplyRule(u, "reset", err));
    assert(!u.enabled);
    assert(!aclCanRunCmd(u, aclCmdIndex("hset")));
    assert(!aclCanAccessKey(u, "anything", true, true));

    // read-only key pattern
    foreach (r; ["on", "+@all", "%R~ro:*"])
        assert(aclApplyRule(u, r, err), err);
    assert(aclCanAccessKey(u, "ro:1", true, false));   // read ok
    assert(!aclCanAccessKey(u, "ro:1", false, true));  // write denied

    // channel access: glob for plain channels, LITERAL for subscribe patterns
    auto cu = freshUser("cu");
    scope (exit)
        freeUser(cu);
    foreach (r; ["on", "+@all", "resetchannels", "&news:*", "&chat"])
        assert(aclApplyRule(cu, r, err), err);
    assert(aclCanAccessChannel(cu, "news:1"));           // glob &news:*
    assert(aclCanAccessChannel(cu, "chat"));
    assert(!aclCanAccessChannel(cu, "secret"));
    assert(aclCanAccessChannel(cu, "news:*", true));     // PSUBSCRIBE: literal == &news:*
    assert(!aclCanAccessChannel(cu, "news:1", true));    // literal: "news:1" != any pattern
    assert(!aclCanAccessChannel(cu, "chat:*", true));

    // key ACL via the static key-spec table (dreads.aclkeys)
    import dreads.resp : RVal, RType;

    static RVal[] mk(string[] toks...)
    {
        auto a = new RVal[toks.length];
        foreach (i, t; toks)
        {
            a[i].type = RType.BulkString;
            a[i].str = t;
        }
        return a;
    }

    auto ku = freshUser("ku");
    scope (exit)
        freeUser(ku);
    foreach (r; ["on", "+@all", "~foo:*", "%R~ro:*"])
        assert(aclApplyRule(ku, r, err), err);
    assert(!aclKeyDenied(ku, "set", mk("SET", "foo:1", "v")));   // write ~foo:*
    assert(aclKeyDenied(ku, "set", mk("SET", "bar:1", "v")));    // key not covered
    assert(aclKeyDenied(ku, "set", mk("SET", "ro:1", "v")));     // ro:* is read-only
    assert(!aclKeyDenied(ku, "get", mk("GET", "ro:1")));         // read ok
    assert(!aclKeyDenied(ku, "mget", mk("MGET", "foo:1", "foo:2")));
    assert(aclKeyDenied(ku, "mset", mk("MSET", "foo:1", "v", "bar:2", "w"))); // 2nd key denied
    assert(!aclKeyDenied(ku, "ping", mk("PING"))); // keyless

    // numkeys-based specs: ZUNIONSTORE dest (static) + srcs (keynum), LMPOP
    assert(!aclKeyDenied(ku, "zunionstore", mk("ZUNIONSTORE", "foo:d", "2", "foo:a", "foo:b")));
    assert(aclKeyDenied(ku, "zunionstore", mk("ZUNIONSTORE", "bar:d", "2", "foo:a", "foo:b"))); // dest
    assert(aclKeyDenied(ku, "zunionstore", mk("ZUNIONSTORE", "foo:d", "2", "foo:a", "bar:b"))); // src
    assert(!aclKeyDenied(ku, "lmpop", mk("LMPOP", "2", "foo:1", "foo:2", "LEFT")));
    assert(aclKeyDenied(ku, "lmpop", mk("LMPOP", "2", "foo:1", "bar:1", "LEFT")));
    assert(aclKeyDenied(ku, "sintercard", mk("SINTERCARD", "2", "foo:a", "bar:b")));
}

unittest // registry: default user, get/create/del, unrestricted predicate
{
    aclInit();
    auto def = aclUser("default");
    assert(def !is null && aclUnrestricted(def)); // on nopass +@all ~* &*

    const(char)[] err;
    auto alice = aclGetOrCreate("alice");
    assert(alice !is null && aclUser("alice") is alice);
    assert(!aclUnrestricted(alice)); // fresh user is disabled + empty
    foreach (r; ["on", "+@all", "allkeys", "allchannels"])
        aclApplyRule(alice, r, err);
    assert(aclUnrestricted(alice)); // now full

    assert(aclDelUser("alice") && aclUser("alice") is null);
    assert(!aclDelUser("default")); // default can't be deleted
}

unittest // long command names (>16 chars) are catalogued and gated
{
    // Regression: handleCommand uppercased into a char[16] buffer and took a
    // raw-dispatch path for longer names, SKIPPING ACL enforcement. The command
    // table must know these names so the (now char[32]) enforcement path gates
    // them. GEORADIUSBYMEMBER_RO (20 chars) is the longest command token.
    assert(aclCmdIndex("georadiusbymember") >= 0);
    assert(aclCmdIndex("georadiusbymember_ro") >= 0);

    aclInit();
    const(char)[] err;
    auto u = aclGetOrCreate("geotest");
    scope (exit)
        aclDelUser("geotest");
    foreach (r; ["on", "~*", "+get", "+geoadd"]) // no geo-read perms
        assert(aclApplyRule(u, r, err), cast(string) err);
    assert(aclCanRunCmd(u, aclCmdIndex("geoadd")));
    assert(!aclCanRunCmd(u, aclCmdIndex("georadiusbymember")));
    assert(!aclCanRunCmd(u, aclCmdIndex("georadiusbymember_ro")));
}

unittest // ACL GETUSER rule description echoes the applied rules (Valkey format)
{
    import dreads.mem : ByteBuffer;

    static string describe(void function(const(AclUser)*, ref ByteBuffer) f, const(AclUser)* u)
    {
        ByteBuffer b;
        f(u, b);
        return (cast(const(char)[]) b.data).idup;
    }
    static string describeCh(const(AclUser)* u, bool reset = false)
    {
        ByteBuffer b;
        aclDescribeChannels(u, b, reset);
        return (cast(const(char)[]) b.data).idup;
    }

    aclInit();
    // default is the seeded unrestricted user: "+@all ~* &*"
    auto def = aclUser("default");
    assert(describe(&aclDescribeCommands, def) == "+@all");
    assert(describe(&aclDescribeKeys, def) == "~*");
    assert(describeCh(def) == "&*");

    const(char)[] err;
    auto u = aclGetOrCreate("descr");
    scope (exit)
        aclDelUser("descr");
    foreach (r; ["on", "+@list", "+get", "-lpush", "~k*", "%R~ro:*", "&news"])
        assert(aclApplyRule(u, r, err), cast(string) err);
    // base "-@all" + verbatim delta tokens, in order
    assert(describe(&aclDescribeCommands, u) == "-@all +@list +get -lpush");
    assert(describe(&aclDescribeKeys, u) == "~k* %R~ro:*");
    assert(describeCh(u) == "&news"); // GETUSER form: no resetchannels prefix
    assert(describeCh(u, true) == "resetchannels &news"); // LIST form

    // a later +@all resets the command-rule base and drops earlier deltas
    assert(aclApplyRule(u, "+@all", err));
    assert(describe(&aclDescribeCommands, u) == "+@all");
    assert(aclApplyRule(u, "-del", err));
    assert(describe(&aclDescribeCommands, u) == "+@all -del");
}

unittest // canonical propagation: encode a user, apply it back, state matches
{
    import dreads.mem : ByteBuffer, Arena;
    import dreads.resp : parseValue, ParseStatus, RVal, RType;

    aclInit();
    const(char)[] err;
    auto src = aclGetOrCreate("canon_src");
    scope (exit)
        aclDelUser("canon_src");
    // give it a password by hash (no Argon2 in the test), keys, channels, commands
    enum h64 = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
    foreach (r; ["on", "#" ~ h64, "~app:*", "%R~ro:*", "&news", "+@read", "+set", "-get"])
        assert(aclApplyRule(src, r, err), cast(string) err);

    // encode the canonical ACL SETUSER … and parse it back into an RVal array
    ByteBuffer enc;
    aclEncodeCanonicalSetuser(src, enc);
    Arena arena;
    RVal cmd;
    size_t pos = 0;
    assert(parseValue(enc.data, pos, arena, cmd) == ParseStatus.ok);
    assert(cmd.type == RType.Array && cmd.arr[0].str == "ACL");

    // apply it to a DIFFERENT registry name and confirm the rebuilt user matches
    // (rename the target in the parsed command)
    aclDelUser("canon_dst");
    cmd.arr[2].str = "canon_dst";
    aclApplyCanonical(cmd.arr[1 .. $]);
    scope (exit)
        aclDelUser("canon_dst");
    auto dst = aclUser("canon_dst");
    assert(dst !is null && dst.enabled);
    assert(aclCanRunCmd(dst, aclCmdIndex("set")) && aclCanRunCmd(dst, aclCmdIndex("mget")));
    assert(!aclCanRunCmd(dst, aclCmdIndex("get"))); // -get after +@read
    assert(!dst.nopass && dst.passwords.length == 1 && dst.passwords[0] == h64);
    assert(dst.root.allKeys == false && dst.root.keyPats.length == 2);
    assert(dst.root.chanPats.length == 1 && dst.root.chanPats[0] == "news");
}
