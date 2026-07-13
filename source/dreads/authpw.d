/**
 * dreads.authpw — password hashing / verification for ACL.
 *
 * New passwords use Argon2id (libsodium `crypto_pwhash`), the OWASP-recommended
 * memory-hard KDF: each verify costs ~15-30 ms + tens of MiB, so online
 * brute-force is expensive by construction. `verifyPassword` also accepts a
 * bare SHA-256 hex hash — Valkey's ACL wire format (`#<sha256hex>`) — so a
 * password imported from a Valkey `aclfile` or set directly still authenticates
 * (see AUTH-ACL-PLAN.md §9).
 *
 * This is control-plane code: it is NOT `@nogc` (Argon2 + PHC strings), and the
 * ~15-30 ms verify is CPU-bound, so callers MUST run it OFF the event loop (a
 * dedicated auth worker thread), never inline on the single vibe loop.
 *
 * Call `initAuthPw()` once at startup before any hash/verify.
 */
module dreads.authpw;

import libsodium.core : sodium_init;
import libsodium.utils : sodium_memcmp;
import libsodium.crypto_pwhash : crypto_pwhash_str_alg, crypto_pwhash_str_verify,
    crypto_pwhash_STRBYTES, crypto_pwhash_ALG_ARGON2ID13;
import libsodium.randombytes : randombytes_uniform;

// Argon2id tuning (overridable via CONFIG later). m=16 MiB, t=2 verifies in
// ~15-30 ms on a modern core — a strong margin without a punishing latency.
private __gshared ulong gArgonOps = 2;
private __gshared size_t gArgonMem = 16 * 1024 * 1024;

/// Initialize libsodium. Idempotent; must run once before hash/verify.
void initAuthPw() @trusted
{
    if (sodium_init() < 0)
        throw new Exception("libsodium: sodium_init() failed");
}

/// Override the Argon2id cost parameters (CONFIG acl-argon2-mem/-ops).
void configureArgon(ulong opslimit, size_t memlimit) @trusted nothrow @nogc
{
    if (opslimit)
        gArgonOps = opslimit;
    if (memlimit)
        gArgonMem = memlimit;
}

/// Argon2id PHC string for a plaintext password ("$argon2id$v=19$m=…$salt$hash").
string hashPassword(scope const(char)[] plaintext) @trusted
{
    char[crypto_pwhash_STRBYTES] out_ = void;
    immutable rc = crypto_pwhash_str_alg(out_, plaintext.ptr, plaintext.length,
            gArgonOps, gArgonMem, crypto_pwhash_ALG_ARGON2ID13);
    if (rc != 0)
        throw new Exception("hashPassword: argon2id hashing failed");
    import core.stdc.string : strlen;

    return out_[0 .. strlen(out_.ptr)].idup;
}

/// Verify a plaintext against a stored hash (Argon2id PHC, or a Valkey SHA-256
/// hex). A small CSPRNG jitter after every attempt dilutes timing analysis.
/// Runs the memory-hard KDF — call OFF the event loop.
bool verifyPassword(scope const(char)[] plaintext, scope const(char)[] storedHash) @trusted nothrow
{
    scope (exit)
        addTimingJitter();
    if (storedHash.length >= 7 && storedHash[0 .. 7] == "$argon2")
        return verifyArgon2(plaintext, storedHash);
    if (isSha256Hex(storedHash))
        return verifySha256(plaintext, storedHash);
    return false;
}

/// The lowercase SHA-256 hex of a password — the Valkey `>pass`/`#hash` form,
/// used only for interop (GETUSER of an imported hash, DRYRUN).
string sha256Hex(scope const(char)[] plaintext) @trusted
{
    import std.digest.sha : sha256Of;

    ubyte[32] d = sha256Of(cast(const(ubyte)[]) plaintext);
    auto s = new char[64];
    toHexLower(d, s);
    return cast(string) s;
}

// --- internals ---------------------------------------------------------------

private bool verifyArgon2(scope const(char)[] plaintext, scope const(char)[] storedHash) @trusted nothrow
{
    if (storedHash.length >= crypto_pwhash_STRBYTES)
        return false;
    char[crypto_pwhash_STRBYTES] buf = 0; // NUL-padded fixed buffer libsodium wants
    buf[0 .. storedHash.length] = storedHash[];
    return crypto_pwhash_str_verify(buf, plaintext.ptr, plaintext.length) == 0;
}

private bool verifySha256(scope const(char)[] plaintext, scope const(char)[] storedHex) @trusted nothrow
{
    ubyte[32] want = void;
    if (!hexDecode(storedHex, want))
        return false;
    import std.digest.sha : sha256Of;

    ubyte[32] got = sha256Of(cast(const(ubyte)[]) plaintext);
    return sodium_memcmp(got.ptr, want.ptr, 32) == 0; // constant-time
}

private bool isSha256Hex(scope const(char)[] s) @safe nothrow @nogc
{
    if (s.length != 64)
        return false;
    foreach (c; s)
        if (!((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')))
            return false;
    return true;
}

private bool hexDecode(scope const(char)[] hex, ref ubyte[32] outb) @safe nothrow @nogc
{
    if (hex.length != 64)
        return false;
    static int nib(char c) @safe nothrow @nogc
    {
        if (c >= '0' && c <= '9')
            return c - '0';
        if (c >= 'a' && c <= 'f')
            return c - 'a' + 10;
        if (c >= 'A' && c <= 'F')
            return c - 'A' + 10;
        return -1;
    }

    foreach (i; 0 .. 32)
    {
        auto hi = nib(hex[i * 2]), lo = nib(hex[i * 2 + 1]);
        if (hi < 0 || lo < 0)
            return false;
        outb[i] = cast(ubyte)((hi << 4) | lo);
    }
    return true;
}

private void toHexLower(ref const ubyte[32] b, char[] outc) @safe nothrow @nogc
{
    static immutable hexd = "0123456789abcdef";
    foreach (i; 0 .. 32)
    {
        outc[i * 2] = hexd[b[i] >> 4];
        outc[i * 2 + 1] = hexd[b[i] & 0xf];
    }
}

/// Sleep a uniformly-random 0-4 ms (CSPRNG) so an attacker can't average out
/// timing side-channels. Best-effort; on the auth worker thread a plain sleep
/// is fine (never call verifyPassword on the event-loop thread).
private void addTimingJitter() @trusted nothrow
{
    import core.thread : Thread;
    import core.time : usecs;

    try
        Thread.sleep(usecs(randombytes_uniform(4000)));
    catch (Exception)
    {
    }
}

// --- tests -------------------------------------------------------------------

unittest
{
    import std.algorithm : startsWith;

    initAuthPw();
    // fast tuning so the test isn't slow; still exercises the real KDF
    configureArgon(1, 8192);
    auto h = hashPassword("hunter2");
    assert(h.startsWith("$argon2id$"), h);
    assert(verifyPassword("hunter2", h));
    assert(!verifyPassword("wrong", h));
    configureArgon(2, 16 * 1024 * 1024); // restore defaults

    // Valkey SHA-256 interop: a bare hex hash verifies the same password
    auto hex = sha256Hex("s3cr3t");
    assert(hex.length == 64);
    assert(verifyPassword("s3cr3t", hex));
    assert(!verifyPassword("nope", hex));
    // an unknown format never authenticates
    assert(!verifyPassword("x", "not-a-hash"));
}
