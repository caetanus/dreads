module dreads.mac;

// Optional authentication of the Raft replication wire. Raft assumes non-
// Byzantine participants; on an unauthenticated transport a single crafted
// frame (e.g. a RequestVote with term=ulong.max) can isolate a node or a
// forged "leader" can install arbitrary state. A keyed MAC over every frame
// makes the transport reject anything not signed with the shared cluster
// secret, which closes that whole class at the transport layer without
// touching the consensus algorithm.
//
// Primitive: BLAKE2b-128 keyed (libsodium crypto_generichash). Chosen after a
// microbench of the candidates (bench/raft_auth_bench.d): HMAC-SHA512-256 (the
// obvious crypto_auth) was the SLOWEST on every frame size (~1.6us fixed cost,
// 0.43 GB/s); BLAKE2b is ~5x cheaper fixed cost and ~3x the bandwidth, is
// DETERMINISTIC (no per-frame nonce to manage — AES-GMAC is faster on large
// frames but nonce reuse across restarts silently breaks it), and a 128-bit tag
// is strong. The cost is off the default path anyway: this is opt-in via
// `raft-secret` and lives on the dedicated raft thread, amortized over each
// group-commit batch (one MAC per frame, not per write).

// libsodium: keyed hash + constant-time 16-byte compare. Already linked (libs).
extern (C) @nogc nothrow @system
{
    int sodium_init(); // idempotent; >=0 on success
    int crypto_generichash(ubyte* o, size_t olen, const(ubyte)* i, ulong ilen,
            const(ubyte)* key, size_t keylen);
    int crypto_verify_16(const(ubyte)* a, const(ubyte)* b); // 0 iff equal, constant-time
}

/// Tag length on the wire (BLAKE2b-128). The transport appends this many bytes
/// after every frame when auth is enabled.
enum size_t RAFT_TAG_LEN = 16;

// The 32-byte frame key, derived once from the cluster secret. Written before
// the transport starts (publication happens-before via the raft thread start),
// read on the raft thread only.
private __gshared ubyte[32] gKey;
private __gshared bool gReady;

/// Derive the frame key from the cluster secret (any passphrase). Call once at
/// boot before wiring the transport. Returns false if libsodium init fails.
bool raftAuthInit(scope const(char)[] secret) @system
{
    if (sodium_init() < 0)
        return false;
    // 32-byte key = keyed-less BLAKE2b of the passphrase (stable KDF).
    cast(void) crypto_generichash(gKey.ptr, gKey.length,
            cast(const(ubyte)*) secret.ptr, secret.length, null, 0);
    gReady = true;
    return true;
}

/// Write the 16-byte tag of `frame` into `tagOut` (>= RAFT_TAG_LEN). SignFn.
void raftSign(scope const(ubyte)[] frame, ubyte[] tagOut) nothrow @system
{
    cast(void) crypto_generichash(tagOut.ptr, RAFT_TAG_LEN,
            frame.ptr, frame.length, gKey.ptr, gKey.length);
}

/// True iff `tag` is the valid MAC of `frame` (constant-time). VerifyFn.
bool raftVerify(scope const(ubyte)[] frame, scope const(ubyte)[] tag) nothrow @system
{
    if (tag.length != RAFT_TAG_LEN)
        return false;
    ubyte[RAFT_TAG_LEN] expect = void;
    cast(void) crypto_generichash(expect.ptr, RAFT_TAG_LEN,
            frame.ptr, frame.length, gKey.ptr, gKey.length);
    return crypto_verify_16(expect.ptr, tag.ptr) == 0;
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

version (unittest)
{
    import fluent.asserts;

    @("mac.sign_verify_roundtrip")
    unittest
    {
        raftAuthInit("correct horse battery staple").expect.to.equal(true);
        auto frame = cast(const(ubyte)[]) "\x00\x01a raft frame with \xff bytes\x00";
        ubyte[RAFT_TAG_LEN] tag = void;
        raftSign(frame, tag[]);
        raftVerify(frame, tag[]).expect.to.equal(true);
    }

    @("mac.tamper_is_rejected")
    unittest
    {
        raftAuthInit("secret-key").expect.to.equal(true);
        auto frame = cast(const(ubyte)[]) "appendentries-payload-1234567890";
        ubyte[RAFT_TAG_LEN] tag = void;
        raftSign(frame, tag[]);

        // flip one frame byte -> tag no longer matches
        auto bad = frame.dup;
        bad[5] ^= 0x01;
        raftVerify(bad, tag[]).expect.to.equal(false);

        // flip one tag byte -> reject
        auto badTag = tag.dup;
        badTag[0] ^= 0x80;
        raftVerify(frame, badTag[]).expect.to.equal(false);

        // wrong length tag -> reject (no OOB)
        raftVerify(frame, tag[0 .. 8]).expect.to.equal(false);
    }

    @("mac.different_secret_different_key")
    unittest
    {
        raftAuthInit("cluster-A").expect.to.equal(true);
        auto frame = cast(const(ubyte)[]) "shared frame bytes here.........";
        ubyte[RAFT_TAG_LEN] tagA = void;
        raftSign(frame, tagA[]);

        // a node in a DIFFERENT cluster (different secret) can't forge a tag we accept
        raftAuthInit("cluster-B").expect.to.equal(true);
        raftVerify(frame, tagA[]).expect.to.equal(false);

        // and its own tag verifies under its own key
        ubyte[RAFT_TAG_LEN] tagB = void;
        raftSign(frame, tagB[]);
        raftVerify(frame, tagB[]).expect.to.equal(true);
        (tagA[] != tagB[]).expect.to.equal(true);
    }
}
