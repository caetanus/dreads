module dreads.rand;

// Tiny non-cryptographic PRNG (xorshift64*) for the random-pick commands
// (RANDOMKEY, SRANDMEMBER, HRANDFIELD, ZRANDMEMBER, SPOP sampling). @nogc,
// one mul per draw. NOT for anything security-sensitive.

private __gshared ulong gRandState = 0x9E37_79B9_7F4A_7C15;

/// Mix boot-time entropy in (the wall clock is plenty for shuffling replies).
public void seedRand(ulong seed) @nogc nothrow
{
    gRandState = seed | 1; // state must never be zero
}

/// Next 64 random bits.
public ulong nextRand() @nogc nothrow
{
    auto x = gRandState;
    x ^= x >> 12;
    x ^= x << 25;
    x ^= x >> 27;
    gRandState = x;
    return x * 0x2545_F491_4F6C_DD1D;
}

/// Uniform-ish draw in [0, n). n must be > 0 (modulo bias is fine here).
public size_t randBelow(size_t n) @nogc nothrow
{
    return cast(size_t)(nextRand() % n);
}
