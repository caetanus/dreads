/**
 * dreads.authpw — password hashing / verification for ACL.
 *
 * Default (no external dependency): **salted PBKDF2-HMAC-SHA256**, stored as
 *   $pbkdf2-sha256$<iters>$<b64url-salt>$<b64url-hash>
 * — a slow, salted hash (iteration-hardened) with a CSPRNG salt.
 *
 * Optional (`dub -c argon2`, defines `WithArgon2`, links libsodium): new hashes
 * use **Argon2id** (memory-hard). `verifyPassword` still reads PBKDF2 hashes, so
 * enabling/disabling Argon2 is transparent for existing users.
 *
 * A bare 64-char SHA-256 hex is also accepted on verify — Valkey's ACL wire
 * form (`#<sha256hex>`) — so an imported `aclfile` hash still authenticates.
 *
 * Control-plane code (NOT `@nogc`; the KDF is deliberately slow) — callers run
 * it OFF the event loop (a dedicated auth worker thread), never inline.
 * See AUTH-ACL-PLAN.md §9.
 */
module dreads.authpw;

import std.base64 : Base64URL;
import std.digest.hmac : hmac;
import std.digest.sha : SHA256, sha256Of;
import std.string : representation;

// PBKDF2 work factor (OWASP-class; CONFIG-overridable). Argon2 params below.
private __gshared uint gPbkdf2Iters = 210_000;

version (WithArgon2)
{
    import libsodium.core : sodium_init;
    import libsodium.crypto_pwhash : crypto_pwhash_str_alg, crypto_pwhash_str_verify,
        crypto_pwhash_STRBYTES, crypto_pwhash_ALG_ARGON2ID13;

    private __gshared ulong gArgonOps = 2;
    private __gshared size_t gArgonMem = 16 * 1024 * 1024;
}

/// Idempotent startup init (initializes libsodium when Argon2 is compiled in).
void initAuthPw() @trusted
{
    version (WithArgon2)
        if (sodium_init() < 0)
            throw new Exception("libsodium: sodium_init() failed");
}

/// Tune the default KDF cost (CONFIG). `iters` sets PBKDF2 iterations; the
/// Argon2 ops/mem overloads apply only when compiled with WithArgon2.
void configurePbkdf2(uint iters) @trusted nothrow @nogc
{
    if (iters)
        gPbkdf2Iters = iters;
}

version (WithArgon2) void configureArgon(ulong ops, size_t mem) @trusted nothrow @nogc
{
    if (ops)
        gArgonOps = ops;
    if (mem)
        gArgonMem = mem;
}

/// Hash a plaintext password with the default KDF (Argon2id if compiled in,
/// else salted PBKDF2-HMAC-SHA256). Runs the slow KDF — call OFF the event loop.
string hashPassword(scope const(char)[] plaintext) @trusted
{
    version (WithArgon2)
        return hashArgon2(plaintext);
    else
        return hashPbkdf2(plaintext);
}

/// Verify a plaintext against a stored hash, dispatching on its format. A small
/// jitter after every attempt dilutes timing analysis. Runs the slow KDF.
bool verifyPassword(scope const(char)[] plaintext, scope const(char)[] storedHash) @trusted nothrow
{
    scope (exit)
        addTimingJitter();
    if (storedHash.length >= 7 && storedHash[0 .. 7] == "$argon2")
    {
        version (WithArgon2)
            return verifyArgon2(plaintext, storedHash);
        else
            return false; // argon2 hash but no libsodium — cannot verify
    }
    if (storedHash.length >= 15 && storedHash[0 .. 15] == "$pbkdf2-sha256$")
        return verifyPbkdf2(plaintext, storedHash);
    if (isSha256Hex(storedHash))
        return verifySha256(plaintext, storedHash);
    return false;
}

/// Lowercase SHA-256 hex of a password — the Valkey `#hash` wire form (interop).
string sha256Hex(scope const(char)[] plaintext) @trusted
{
    ubyte[32] d = sha256Of(cast(const(ubyte)[]) plaintext);
    auto s = new char[64];
    toHexLower(d, s);
    return cast(string) s;
}

// --- PBKDF2-HMAC-SHA256 (default) -------------------------------------------

private enum DK_LEN = 32;

private string hashPbkdf2(scope const(char)[] pw) @trusted
{
    import std.conv : text;

    ubyte[16] salt = void;
    cryptoRandom(salt[]);
    auto dk = pbkdf2(pw.representation, salt[], gPbkdf2Iters);
    return ("$pbkdf2-sha256$" ~ text(gPbkdf2Iters) ~ "$"
            ~ Base64URL.encode(salt[]) ~ "$" ~ Base64URL.encode(dk[])).idup;
}

private bool verifyPbkdf2(scope const(char)[] pw, scope const(char)[] stored) @trusted nothrow
{
    try
    {
        import std.string : split;
        import std.conv : to;

        auto p = (cast(string) stored).split('$');
        if (p.length != 5 || p[1] != "pbkdf2-sha256")
            return false;
        immutable iters = p[2].to!uint;
        auto salt = Base64URL.decode(p[3]);
        auto expected = Base64URL.decode(p[4]);
        auto actual = pbkdf2(pw.representation, salt, iters);
        return constantTimeEqual(actual[], expected);
    }
    catch (Exception)
        return false;
}

private ubyte[DK_LEN] pbkdf2(scope const(ubyte)[] pw, scope const(ubyte)[] salt, uint iters) @trusted nothrow
{
    import std.bitmanip : nativeToBigEndian;

    ubyte[4] blk = nativeToBigEndian!uint(1u); // single output block (dkLen=32)
    ubyte[32] u = void;
    {
        auto h = hmac!SHA256(pw);
        h.put(salt);
        h.put(blk[]);
        u = h.finish();
    }
    ubyte[32] t = u;
    foreach (_; 1 .. iters)
    {
        auto h = hmac!SHA256(pw);
        h.put(u[]);
        u = h.finish();
        foreach (i; 0 .. 32)
            t[i] ^= u[i];
    }
    ubyte[DK_LEN] dk = t[0 .. DK_LEN];
    return dk;
}

// --- Argon2id (optional) -----------------------------------------------------

version (WithArgon2)
{
    private string hashArgon2(scope const(char)[] plaintext) @trusted
    {
        char[crypto_pwhash_STRBYTES] out_ = void;
        immutable rc = crypto_pwhash_str_alg(out_, plaintext.ptr, plaintext.length,
                gArgonOps, gArgonMem, crypto_pwhash_ALG_ARGON2ID13);
        if (rc != 0)
            throw new Exception("hashPassword: argon2id hashing failed");
        import core.stdc.string : strlen;

        return out_[0 .. strlen(out_.ptr)].idup;
    }

    private bool verifyArgon2(scope const(char)[] plaintext, scope const(char)[] stored) @trusted nothrow
    {
        if (stored.length >= crypto_pwhash_STRBYTES)
            return false;
        char[crypto_pwhash_STRBYTES] buf = 0;
        buf[0 .. stored.length] = stored[];
        return crypto_pwhash_str_verify(buf, plaintext.ptr, plaintext.length) == 0;
    }
}

// --- SHA-256 interop (Valkey #hash) -----------------------------------------

private bool verifySha256(scope const(char)[] plaintext, scope const(char)[] storedHex) @trusted nothrow
{
    ubyte[32] want = void;
    if (!hexDecode(storedHex, want))
        return false;
    ubyte[32] got = sha256Of(cast(const(ubyte)[]) plaintext);
    return constantTimeEqual(got[], want[]);
}

// --- helpers -----------------------------------------------------------------

private bool constantTimeEqual(scope const(ubyte)[] a, scope const(ubyte)[] b) @safe pure nothrow @nogc
{
    if (a.length != b.length)
        return false;
    ubyte diff = 0;
    foreach (i; 0 .. a.length)
        diff |= a[i] ^ b[i];
    return diff == 0;
}

/// Cryptographically-random bytes from the OS (/dev/urandom).
private void cryptoRandom(scope ubyte[] buf) @trusted
{
    import std.stdio : File;

    auto f = File("/dev/urandom", "rb");
    auto got = f.rawRead(buf);
    if (got.length != buf.length)
        throw new Exception("cryptoRandom: short read from /dev/urandom");
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

/// Non-blocking on the worker thread: 0-4 ms jitter (never call on the loop).
private void addTimingJitter() @trusted nothrow
{
    import core.thread : Thread;
    import core.time : usecs;
    import dreads.rand : randBelow;

    try
        Thread.sleep(usecs(randBelow(4000)));
    catch (Exception)
    {
    }
}

// --- tests -------------------------------------------------------------------

unittest
{
    import std.algorithm : startsWith;

    initAuthPw();
    configurePbkdf2(1000); // fast for the test; still salted + iterated

    auto h = hashPassword("hunter2");
    version (WithArgon2)
        assert(h.startsWith("$argon2id$"), h);
    else
        assert(h.startsWith("$pbkdf2-sha256$"), h);
    assert(verifyPassword("hunter2", h));
    assert(!verifyPassword("wrong", h));
    // two hashes of the same password differ (random salt)
    assert(hashPassword("hunter2") != h);

    // Valkey SHA-256 interop: a bare hex hash verifies the same password
    auto hex = sha256Hex("s3cr3t");
    assert(hex.length == 64 && verifyPassword("s3cr3t", hex) && !verifyPassword("nope", hex));
    assert(!verifyPassword("x", "not-a-hash"));

    configurePbkdf2(210_000);
}
