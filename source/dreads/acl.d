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

    ~this() @nogc nothrow @trusted
    {
        foreach (i; 0 .. keyPats.length)
            freeSlice(keyPats[i].pat);
        foreach (i; 0 .. chanPats.length)
            freeSlice(chanPats[i]);
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
    }
}

/// An ACL user.
struct AclUser
{
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

/// Command name (lowercase) -> its index in gCmdCats, or -1.
int aclCmdIndex(scope const(char)[] lower) @nogc nothrow @safe
{
    foreach (i, ref c; gCmdCats)
        if (c.name == lower)
            return cast(int) i;
    return -1;
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
    import dreads.authpw : hashPassword;

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
    if (tok == "allcommands" || tok == "+@all") { allowAll(u.root, true); return true; }
    if (tok == "nocommands" || tok == "-@all") { allowAll(u.root, false); return true; }

    switch (tok[0])
    {
    case '>':
        u.nopass = false;
        u.passwords.put(cast(const(char)[]) mallocDup(hashPassword(tok[1 .. $])));
        return true;
    case '#':
        {
            auto h = tok[1 .. $];
            if (!isSha256Hex(h))
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
    case '+':
        return applyCmdRule(u, tok[1 .. $], true, err);
    case '-':
        return applyCmdRule(u, tok[1 .. $], false, err);
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

/// Can this user use pub/sub `channel`?
bool aclCanAccessChannel(const(AclUser)* u, scope const(char)[] channel) @nogc nothrow @trusted
{
    import dreads.commands : globMatch;

    if (u.root.allChannels)
        return true;
    foreach (i; 0 .. u.root.chanPats.length)
        if (globMatch(u.root.chanPats[i], channel))
            return true;
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

/// Malloc + construct a fresh, disabled, no-permission user with `name`.
AclUser* aclNewUser(scope const(char)[] name) @trusted nothrow
{
    import core.lifetime : emplace;
    import core.stdc.stdlib : malloc;

    auto u = cast(AclUser*) malloc(AclUser.sizeof);
    emplace(u);
    u.name = cast(const(char)[]) mallocDup(name);
    return u;
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
