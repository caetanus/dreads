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

/// A per-subcommand override (`+client|id`, `-config|set`). Overrides the base
/// command bit for that one subcommand; later rules for the same pair win.
struct SubRule
{
    int cmd; // aclCmdIndex of the container command
    const(char)[] sub; // malloc'd, lowercase subcommand name
    bool allow;
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
    Vector!SubRule subRules; // per-subcommand overrides (+client|id, …)

    ~this() @nogc nothrow @trusted
    {
        foreach (i; 0 .. keyPats.length)
            freeSlice(keyPats[i].pat);
        foreach (i; 0 .. chanPats.length)
            freeSlice(chanPats[i]);
        foreach (i; 0 .. cmdRules.length)
            freeSlice(cmdRules[i]);
        foreach (i; 0 .. subRules.length)
            freeSlice(subRules[i].sub);
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
        clearSubRules();
    }

    // reset the command-rule text to a base (does NOT touch the bitset)
    void setCmdBase(bool all) @nogc nothrow @trusted
    {
        cmdBaseAll = all;
        clearCmdRules();
        clearSubRules(); // +@all/-@all wipe per-subcommand overrides too
    }

    void addCmdRule(scope const(char)[] tok) @nogc nothrow @trusted
    {
        cmdRules.put(cast(const(char)[]) mallocDup(tok));
    }

    // Record a +/- command/category rule token, normalized to lowercase (command,
    // category and subcommand names are case-insensitive; GETUSER echoes them
    // lowercased). For categories, drop any prior rule for the same category so a
    // re-added `-@hash` moves to the end (lossless compaction, Valkey semantics).
    // Command/subcommand dedup already happened in applyCmdRule via dropCmdRuleFor.
    void addCmdRuleNorm(scope const(char)[] tok) @nogc nothrow @trusted
    {
        char[256] lb = void;
        if (tok.length < 2 || tok.length > lb.length)
        {
            addCmdRule(tok);
            return;
        }
        lb[0] = tok[0]; // sign (+/-) kept verbatim
        foreach (i; 1 .. tok.length)
        {
            auto ch = tok[i];
            lb[i] = (ch >= 'A' && ch <= 'Z') ? cast(char)(ch + 32) : ch;
        }
        auto ntok = cast(const(char)[]) lb[0 .. tok.length];
        if (tok[1] == '@')
            dropCatRule(ntok[2 .. $]); // dedup by category name (any sign)
        addCmdRule(ntok);
    }

    // Drop any cmdRule that is a category (+@x / -@x) whose name equals catLower.
    void dropCatRule(scope const(char)[] catLower) @nogc nothrow @trusted
    {
        size_t w = 0;
        foreach (r; 0 .. cmdRules.length)
        {
            auto t = cmdRules[r];
            bool isCat = t.length >= 2 && (t[0] == '+' || t[0] == '-') && t[1] == '@';
            if (isCat && eqLower(t[2 .. $], catLower))
                freeSlice(cmdRules[r]);
            else
            {
                cmdRules[w] = cmdRules[r];
                w++;
            }
        }
        while (cmdRules.length > w)
            cmdRules.popBack();
    }

    // Engulf: adding `+memory` collapses a prior `+memory|doctor` so GETUSER
    // shows the whole command, not stale subcommand tokens (Valkey semantics).
    // `whole` drops every rule for the command; else only the exact cmd|sub.
    // Order of surviving tokens is preserved (GETUSER echoes them in order).
    void dropCmdRuleFor(scope const(char)[] cmdLower, scope const(char)[] subLower,
            bool whole) @nogc nothrow @trusted
    {
        size_t w = 0;
        foreach (r; 0 .. cmdRules.length)
        {
            if (cmdRuleMatches(cmdRules[r], cmdLower, subLower, whole))
                freeSlice(cmdRules[r]);
            else
            {
                cmdRules[w] = cmdRules[r];
                w++;
            }
        }
        while (cmdRules.length > w)
            cmdRules.popBack();
    }

    // does rule token `tok` (+cmd / -cmd|sub) target this command (+ sub)?
    private static bool cmdRuleMatches(scope const(char)[] tok, scope const(char)[] cmdLower,
            scope const(char)[] subLower, bool whole) @nogc nothrow @safe
    {
        if (tok.length < 2 || (tok[0] != '+' && tok[0] != '-') || tok[1] == '@')
            return false;
        auto rest = tok[1 .. $];
        size_t bar = rest.length;
        foreach (i, ch; rest)
            if (ch == '|')
            {
                bar = i;
                break;
            }
        if (!eqLower(rest[0 .. bar], cmdLower))
            return false;
        if (whole)
            return true; // whole-command rule engulfs every sub of this command
        auto tsub = bar < rest.length ? rest[bar + 1 .. $] : null;
        return eqLower(tsub, subLower);
    }

    private static bool eqLower(scope const(char)[] a, scope const(char)[] b) @nogc nothrow @safe
    {
        if (a.length != b.length)
            return false;
        foreach (i, ch; a)
        {
            auto x = (ch >= 'A' && ch <= 'Z') ? cast(char)(ch + 32) : ch;
            if (x != b[i])
                return false;
        }
        return true;
    }

    // record a per-subcommand override; drop any earlier one for the same pair
    void setSubRule(int cmd, scope const(char)[] sub, bool allow) @nogc nothrow @trusted
    {
        foreach (i; 0 .. subRules.length)
            if (subRules[i].cmd == cmd && subRules[i].sub == sub)
            {
                subRules[i].allow = allow;
                return;
            }
        subRules.put(SubRule(cmd, cast(const(char)[]) mallocDup(sub), allow));
    }

    // a whole-command rule (+client / -client) supersedes its subcommand rules
    void dropSubRules(int cmd) @nogc nothrow @trusted
    {
        for (size_t i = 0; i < subRules.length;)
            if (subRules[i].cmd == cmd)
            {
                freeSlice(subRules[i].sub);
                subRules[i] = subRules[subRules.length - 1];
                subRules.popBack();
            }
            else
                i++;
    }

    private void clearCmdRules() @nogc nothrow @trusted
    {
        foreach (i; 0 .. cmdRules.length)
            freeSlice(cmdRules[i]);
        cmdRules.clear();
    }

    private void clearSubRules() @nogc nothrow @trusted
    {
        foreach (i; 0 .. subRules.length)
            freeSlice(subRules[i].sub);
        subRules.clear();
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
        u.root.addCmdRuleNorm(tok);
        return true;
    case '-':
        if (!applyCmdRule(u, tok[1 .. $], false, err))
            return false;
        u.root.addCmdRuleNorm(tok);
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
        // category names are case-insensitive (@HASH == @hash)
        char[64] cb = void;
        auto cat = spec[1 .. $];
        uint bit;
        if (cat.length && cat.length <= cb.length)
        {
            foreach (i, ch; cat)
                cb[i] = (ch >= 'A' && ch <= 'Z') ? cast(char)(ch + 32) : ch;
            bit = aclCatBit(cast(const(char)[]) cb[0 .. cat.length]);
        }
        if (bit == 0)
        {
            err = "ERR Error in ACL SETUSER modifier: Unknown command or category name in ACL";
            return false;
        }
        allowCategory(u.root, bit, allow);
        return true;
    }
    // split an optional |subcommand suffix (+client|id → cmd "client", sub "id")
    auto name = spec;
    const(char)[] sub;
    foreach (i, ch; spec)
        if (ch == '|')
        {
            name = spec[0 .. i];
            sub = spec[i + 1 .. $];
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
    if (sub.length)
    {
        // sub may hold a further '|'. First segment = the subcommand/first-arg.
        size_t seg = sub.length;
        foreach (i, ch; sub)
            if (ch == '|')
            {
                seg = i;
                break;
            }
        char[64] sb = void;
        if (seg > sb.length)
            return false;
        foreach (i; 0 .. seg)
            sb[i] = (sub[i] >= 'A' && sub[i] <= 'Z') ? cast(char)(sub[i] + 32) : sub[i];
        auto slow = sb[0 .. seg];
        if (aclIsContainer(lb[0 .. name.length]))
        {
            // container: the segment must be a real subcommand; a further '|'
            // (config|get|appendonly) is an unsupported first-arg of a subcommand
            if (!aclSubExists(lb[0 .. name.length], slow))
            {
                err = "ERR Error in ACL SETUSER modifier: Unknown command or category name in ACL";
                return false;
            }
            if (seg < sub.length)
            {
                err = "ERR Error in ACL SETUSER modifier: Allowing first-arg of a"
                    ~ " subcommand is not supported";
                return false;
            }
        }
        else
        {
            // non-container: `|arg` is a first-arg rule (e.g. select|0). A second
            // '|' would be two first-args — unsupported → "Unknown command…".
            if (seg < sub.length)
            {
                err = "ERR Error in ACL SETUSER modifier: Unknown command or category name in ACL";
                return false;
            }
        }
        // both subcommands and first-args are enforced via the same SubRule
        // (aclCanRunCmdSub matches arg[1]); the rule text is echoed verbatim.
        u.root.dropCmdRuleFor(lb[0 .. name.length], slow, false); // replace same key
        u.root.setSubRule(idx, slow, allow);
        return true;
    }
    // a whole-command rule supersedes (engulfs) its subcommand overrides
    u.root.dropCmdRuleFor(lb[0 .. name.length], null, true);
    u.root.dropSubRules(idx);
    bitSet(u.root.allowed, idx, allow);
    return true;
}

/// Is `sub` a real subcommand of container `cmd` (both lowercase)?
bool aclSubExists(scope const(char)[] cmd, scope const(char)[] sub) @trusted nothrow @nogc
{
    import dreads.aclsub : gSubCmds;

    foreach (ref sc; gSubCmds)
        if (sc.container == cmd && sc.sub == sub)
            return true;
    return false;
}

/// Does `cmd` (lowercase) have subcommands (CONFIG, CLIENT, …)? If not, a
/// `cmd|arg` rule is a first-arg restriction (select|0), not a subcommand.
bool aclIsContainer(scope const(char)[] cmd) @trusted nothrow @nogc
{
    import dreads.aclsub : gSubCmds;

    foreach (ref sc; gSubCmds)
        if (sc.container == cmd)
            return true;
    return false;
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
    // NOTE: `off` (disabled) is an AUTH-time concern only — it blocks new logins
    // (checked in the AUTH path), but an already-authenticated connection retains
    // its command permissions, and ACL DRYRUN ignores on/off entirely. So the
    // enablement flag must NOT gate command checks here.
    if (cmdIdx < 0)
        return true;
    return bitGet(u.root.allowed, cast(size_t) cmdIdx);
}

/// Like aclCanRunCmd but honours per-subcommand overrides (`+client|id`): a
/// matching subrule wins over the base command bit. `sub` is the (lowercase)
/// subcommand arg, empty for a bare command. Fast path when the user has none.
bool aclCanRunCmdSub(const(AclUser)* u, int cmdIdx, scope const(char)[] sub) @nogc nothrow @safe
{
    if (sub.length && u.root.subRules.length)
        foreach (i; 0 .. u.root.subRules.length)
        {
            auto sr = u.root.subRules[i];
            if (sr.cmd == cmdIdx && sr.sub == sub)
                return sr.allow;
        }
    return aclCanRunCmd(u, cmdIdx);
}

/// True if the user has any per-subcommand rule for this command — the denial
/// message then names the `command|subcommand` (as Valkey does for containers).
bool aclCmdHasSubRule(const(AclUser)* u, int cmdIdx) @nogc nothrow @safe
{
    foreach (i; 0 .. u.root.subRules.length)
        if (u.root.subRules[i].cmd == cmdIdx)
            return true;
    return false;
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
    return aclDeniedKey(u, name, arr) !is null;
}

/// The first key this command touches that the user CANNOT access, or null if
/// all are allowed (used both for enforcement and for the ACL LOG object name).
const(char)[] aclDeniedKey(const(AclUser)* u, scope const(char)[] name, scope const(RVal)[] arr) @trusted nothrow @nogc
{
    import dreads.aclkeys : gCmdKeySpecs, gCmdKeyNumSpecs;

    if (u.root.allKeys)
        return null;
    immutable argc = cast(int) arr.length;
    foreach (ref ks; gCmdKeySpecs) // static index+range specs
    {
        if (ks.name != name)
            continue;
        immutable last = ks.last >= 0 ? ks.first + ks.last : argc + ks.last;
        for (int idx = ks.first; idx <= last && idx < argc; idx += ks.step)
        {
            if (idx < 1)
                continue;
            if (!aclCanAccessKey(u, arr[idx].str, ks.needR, ks.needW))
                return arr[idx].str;
        }
    }
    foreach (ref ks; gCmdKeyNumSpecs) // numkeys-based specs
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
                return arr[idx].str;
        }
    }
    // keyword-positioned keys (STORE dests, XREAD STREAMS keys, MIGRATE KEYS)
    return aclKeywordDeniedKey(u, name, arr, argc);
}

private const(char)[] aclKeywordDeniedKey(const(AclUser)* u, scope const(char)[] name,
        scope const(RVal)[] arr, int argc) @trusted nothrow @nogc
{
    static bool kw(scope const(char)[] a, scope const(char)[] lit) @nogc nothrow @safe
    {
        if (a.length != lit.length)
            return false;
        foreach (i, c; a)
            if (((c >= 'A' && c <= 'Z') ? cast(char)(c + 32) : c) != lit[i])
                return false;
        return true;
    }

    switch (name)
    {
    case "georadius", "georadiusbymember", "sort", "sort_ro":
        for (int i = 1; i + 1 < argc; i++)
            if ((kw(arr[i].str, "store") || kw(arr[i].str, "storedist"))
                    && !aclCanAccessKey(u, arr[i + 1].str, false, true))
                return arr[i + 1].str;
        return null;
    case "xread", "xreadgroup":
        for (int i = 1; i < argc; i++)
            if (kw(arr[i].str, "streams"))
            {
                immutable rest = argc - (i + 1);
                immutable nkeys = rest / 2;
                foreach (j; 0 .. nkeys)
                    if (!aclCanAccessKey(u, arr[i + 1 + j].str, true, false))
                        return arr[i + 1 + j].str;
                break;
            }
        return null;
    case "migrate":
        for (int i = 1; i < argc; i++)
            if (kw(arr[i].str, "keys"))
            {
                foreach (j; i + 1 .. argc)
                    if (!aclCanAccessKey(u, arr[j].str, true, true))
                        return arr[j].str;
                break;
            }
        return null;
    default:
        return null;
    }
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

/// True when `u` is unrestricted (every command ∧ all keys/channels) — the
/// enforcement fast case; such a user passes any `command ∈ cap_set` test.
/// on/off is deliberately NOT consulted (it gates AUTH, not authed perms).
bool aclUnrestricted(const(AclUser)* u) @nogc nothrow @safe
{
    if (!u.root.allKeys || !u.root.allChannels)
        return false;
    foreach (w; u.root.allowed)
        if (w != ulong.max)
            return false;
    return true;
}

// --- ACL LOG (denied-attempt log) --------------------------------------------

/// One ACL LOG entry. Similar violations aggregate (count++), keeping the
/// entry-id and created time, refreshing updated. A pure VALUE type — the
/// object/user/client-info live in INLINE fixed buffers, so there is no
/// per-string malloc and nothing to free: the containing `Vector!AclLogEntry`
/// owns the single backing allocation (RAII), and shifting/eviction is plain
/// value assignment. reason/ctx are interned literals. Stored newest-LAST.
struct AclLogEntry
{
    ulong id;
    ulong count;
    long created; // ms
    long updated; // ms
    string reason; // "command" | "key" | "channel" | "auth" (literal)
    string ctx; // "toplevel" | "multi" | "lua" (literal)
    private char[128] objBuf = void;
    private char[64] userBuf = void;
    private char[192] ciBuf = void;
    private ushort objN, userN, ciN;

    void setObj(scope const(char)[] s) @nogc nothrow @trusted
    {
        objN = cast(ushort)(s.length > objBuf.length ? objBuf.length : s.length);
        objBuf[0 .. objN] = s[0 .. objN];
    }

    void setUser(scope const(char)[] s) @nogc nothrow @trusted
    {
        userN = cast(ushort)(s.length > userBuf.length ? userBuf.length : s.length);
        userBuf[0 .. userN] = s[0 .. userN];
    }

    void setCinfo(scope const(char)[] s) @nogc nothrow @trusted
    {
        ciN = cast(ushort)(s.length > ciBuf.length ? ciBuf.length : s.length);
        ciBuf[0 .. ciN] = s[0 .. ciN];
    }

    const(char)[] obj() const @nogc nothrow @trusted return => objBuf[0 .. objN];
    const(char)[] user() const @nogc nothrow @trusted return => userBuf[0 .. userN];
    const(char)[] cinfo() const @nogc nothrow @trusted return => ciBuf[0 .. ciN];
}

private __gshared Vector!AclLogEntry gAclLog; // index 0 = oldest, last = newest
private __gshared ulong gAclLogSeq;
public __gshared long gAclLogMaxLen = 128; // CONFIG acllog-max-len

/// Record (or aggregate) a denied attempt. reason/ctx are interned literals.
void aclLogAdd(string reason, string ctx, scope const(char)[] obj,
        scope const(char)[] user, scope const(char)[] cinfo, long nowMs) @trusted nothrow @nogc
{
    if (gAclLogMaxLen <= 0)
        return;
    // aggregate a matching recent entry (scan newest-first)
    foreach_reverse (i; 0 .. gAclLog.length)
    {
        auto e = &gAclLog[i];
        if (e.reason == reason && e.ctx == ctx && e.obj == obj && e.user == user)
        {
            e.count++;
            e.updated = nowMs;
            if (i != gAclLog.length - 1) // bubble to the newest slot (the end)
            {
                auto tmp = gAclLog[i];
                foreach (j; i .. gAclLog.length - 1)
                    gAclLog[j] = gAclLog[j + 1];
                gAclLog[gAclLog.length - 1] = tmp;
            }
            return;
        }
    }
    AclLogEntry ne;
    ne.id = ++gAclLogSeq;
    ne.count = 1;
    ne.created = ne.updated = nowMs;
    ne.reason = reason;
    ne.ctx = ctx;
    ne.setObj(obj);
    ne.setUser(user);
    ne.setCinfo(cinfo);
    gAclLog.put(ne);
    if (gAclLog.length > gAclLogMaxLen) // drop the oldest (front), no free needed
    {
        foreach (j; 0 .. gAclLog.length - 1)
            gAclLog[j] = gAclLog[j + 1];
        gAclLog.popBack();
    }
}

void aclLogReset() @trusted nothrow @nogc
{
    gAclLog.clear(); // value entries — nothing to free
}

/// Number of entries currently held.
size_t aclLogCount() @trusted nothrow @nogc
{
    return gAclLog.length;
}

/// The entry at reverse index `ri` (0 = newest), or null if out of range.
const(AclLogEntry)* aclLogAt(size_t ri) @trusted nothrow @nogc
{
    if (ri >= gAclLog.length)
        return null;
    return &gAclLog[gAclLog.length - 1 - ri];
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

    // keyword-positioned keys: STORE dest, XREAD STREAMS, MIGRATE KEYS
    assert(!aclKeyDenied(ku, "georadius", mk("GEORADIUS", "foo:g", "13", "38", "1", "km", "STORE", "foo:d")));
    assert(aclKeyDenied(ku, "georadius", mk("GEORADIUS", "foo:g", "13", "38", "1", "km", "STORE", "bar:d")));
    assert(!aclKeyDenied(ku, "xread", mk("XREAD", "STREAMS", "foo:a", "foo:b", "0", "0")));
    assert(aclKeyDenied(ku, "xread", mk("XREAD", "STREAMS", "foo:a", "bar:b", "0", "0")));
    assert(!aclKeyDenied(ku, "sort", mk("SORT", "foo:l", "STORE", "foo:d")));
    assert(aclKeyDenied(ku, "sort", mk("SORT", "foo:l", "STORE", "bar:d")));
    assert(aclKeyDenied(ku, "migrate", mk("MIGRATE", "h", "0", "", "0", "5000", "KEYS", "foo:1", "bar:2")));
}

// `off` (disabled) is an AUTH-time concern only — an already-authed connection
// keeps its command permissions, and ACL DRYRUN ignores on/off. Regression for
// the blackbox "ACL GETUSER provides correct results" / self-reset abort where a
// user that reset itself (→ off) was wrongly denied every command.
unittest
{
    auto u = freshUser("u");
    scope (exit)
        freeUser(u);
    const(char)[] err;
    // reset without `on` leaves the user disabled but +@all still grants commands
    foreach (r; ["reset", "+@all", "~*", "-@string", "+incr", "-debug", "+debug|digest"])
        assert(aclApplyRule(u, r, err), err);
    assert(!u.enabled);                                  // reset disabled it
    assert(aclCanRunCmd(u, aclCmdIndex("acl")));         // acl ∈ +@all — allowed
    assert(aclCanRunCmd(u, aclCmdIndex("incr")));        // +incr
    assert(!aclCanRunCmd(u, aclCmdIndex("get")));        // -@string
    assert(aclUnrestricted(u) == false);                // -@string ⇒ not unrestricted
    // a disabled +@all ~* &* user still counts as unrestricted (on/off ignored)
    auto a = freshUser("a");
    scope (exit)
        freeUser(a);
    foreach (r; ["+@all", "allkeys", "allchannels"])    // no `on` → stays off
        assert(aclApplyRule(a, r, err), err);
    assert(!a.enabled);
    assert(aclUnrestricted(a));
}

// ACL GETUSER lossless compaction: categories are case-insensitive and a
// re-added rule (any sign) moves to the end. Regression for the blackbox
// "ACL GETUSER provides correct results" reorder/dedup + `+@HASH` case aborts.
unittest
{
    import dreads.mem : ByteBuffer;

    auto u = freshUser("u");
    scope (exit)
        freeUser(u);
    const(char)[] err;

    static string cmds(const(AclUser)* u) @trusted
    {
        ByteBuffer b;
        aclDescribeCommands(u, b);
        return (cast(const(char)[]) b.data).idup;
    }

    foreach (r; ["+@all", "-@hash", "-@slow", "+hget"])
        assert(aclApplyRule(u, r, err), err);
    assert(cmds(u) == "+@all -@hash -@slow +hget", cmds(u));

    // re-adding -@hash moves it to the end (no duplicate)
    assert(aclApplyRule(u, "-@hash", err), err);
    assert(cmds(u) == "+@all -@slow +hget -@hash", cmds(u));

    // inverting a category replaces the prior one in place-at-end order
    assert(aclApplyRule(u, "+@hash", err), err);
    assert(cmds(u) == "+@all -@slow +hget +@hash", cmds(u));

    // categories are case-insensitive and collapse to one lowercase token
    assert(aclApplyRule(u, "-@all", err), err);
    foreach (r; ["+@HASH", "+@hash", "+@HaSh"])
        assert(aclApplyRule(u, r, err), err);
    assert(cmds(u) == "-@all +@hash", cmds(u));

    // commands are case-insensitive too
    assert(aclApplyRule(u, "-@all", err), err);
    foreach (r; ["+HGET", "+hget", "+hGeT"])
        assert(aclApplyRule(u, r, err), err);
    assert(cmds(u) == "-@all +hget", cmds(u));

    // whole-command rule engulfs subcommand tokens
    assert(aclApplyRule(u, "-@all", err), err);
    foreach (r; ["+config|get", "+config", "-config|set"])
        assert(aclApplyRule(u, r, err), err);
    assert(cmds(u) == "-@all +config -config|set", cmds(u));
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

unittest // per-subcommand ACL: +@all -client +client|id
{
    const(char)[] err;
    auto u = aclGetOrCreate("subtest");
    scope (exit)
        aclDelUser("subtest");
    foreach (r; ["on", "+@all", "-client", "+client|id", "+client|setname"])
        assert(aclApplyRule(u, r, err), cast(string) err);

    immutable ci = aclCmdIndex("client");
    assert(!aclCanRunCmd(u, ci)); // base client denied by -client
    assert(aclCanRunCmdSub(u, ci, "id")); // re-allowed subcommand
    assert(aclCanRunCmdSub(u, ci, "setname"));
    assert(!aclCanRunCmdSub(u, ci, "kill")); // no subrule → base (denied)
    assert(aclCmdHasSubRule(u, ci)); // denial message names client|<sub>
    // a bare command still works via the base bit (config allowed by +@all)
    assert(aclCanRunCmdSub(u, aclCmdIndex("config"), "get"));

    // a later whole-command rule supersedes the subrules
    assert(aclApplyRule(u, "+client", err));
    assert(aclCanRunCmd(u, ci) && !aclCmdHasSubRule(u, ci));
    assert(aclCanRunCmdSub(u, ci, "kill"));
}

unittest // subcommand catalog: validation + first-arg rules + container check
{
    assert(aclIsContainer("config") && aclIsContainer("client"));
    assert(!aclIsContainer("get") && !aclIsContainer("select"));
    assert(aclSubExists("config", "get") && aclSubExists("function", "list"));
    assert(!aclSubExists("config", "asdf") && !aclSubExists("get", "foo"));

    const(char)[] err;
    auto u = aclGetOrCreate("subval");
    scope (exit)
        aclDelUser("subval");
    assert(aclApplyRule(u, "on", err));
    // container: valid subcommand ok; unknown sub / first-arg-of-sub rejected
    assert(aclApplyRule(u, "+config|get", err));
    assert(!aclApplyRule(u, "+config|asdf", err));
    assert(!aclApplyRule(u, "+config|get|appendonly", err)); // first-arg of a sub
    // non-container: `|arg` is a first-arg rule (select|0), enforced via SubRule
    assert(aclApplyRule(u, "-@all", err));
    assert(aclApplyRule(u, "+select|0", err));
    immutable si = aclCmdIndex("select");
    assert(aclCanRunCmdSub(u, si, "0")); // SELECT 0 ok
    assert(!aclCanRunCmdSub(u, si, "1")); // SELECT 1 denied
    assert(!aclApplyRule(u, "+get|k1|k2", err)); // two first-args → rejected
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
