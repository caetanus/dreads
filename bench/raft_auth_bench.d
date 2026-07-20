module bench.raft_auth_bench;

// Microbenchmark for candidate raft-frame authenticators (auth-only — we need
// integrity/origin, not confidentiality, on an internal network). Measures each
// keyed MAC over realistic raft frame sizes so we can pick the primitive BEFORE
// wiring it into the transport:
//   40 B    — heartbeat / vote (the amortized-over-batch fixed cost matters most)
//   256 B   — small AppendEntries batch
//   1 KiB   — typical batched writes
//   64 KiB  — large batch
//   4 MiB   — InstallSnapshot chunk (the bandwidth-bound case)
//
// Reports MB/s (bandwidth) and ns/frame (fixed cost). Verify ≈ tag + a 16/32-byte
// compare, so tag timing is representative of both directions.
//
// Run: dub run --build=release --config=raft-auth-bench --compiler=ldc2

import core.time : MonoTime;
import std.stdio : writef, writefln, writeln;

extern (C) @nogc nothrow
{
    int sodium_init();

    // HMAC-SHA512-256: 32-byte key, 32-byte tag.
    int crypto_auth(ubyte* o, const(ubyte)* i, ulong ilen, const(ubyte)* k);

    // BLAKE2b keyed: variable out/key length.
    int crypto_generichash(ubyte* o, size_t olen, const(ubyte)* i, ulong ilen,
            const(ubyte)* key, size_t keylen);

    // SipHash-2-4: 16-byte key, 8-byte tag.
    int crypto_shorthash(ubyte* o, const(ubyte)* i, ulong ilen, const(ubyte)* k);

    // AES-256-GCM (hardware where available). GMAC = empty message, frame as AAD.
    int crypto_aead_aes256gcm_is_available();
    int crypto_aead_aes256gcm_encrypt(ubyte* c, ulong* clen, const(ubyte)* m, ulong mlen,
            const(ubyte)* ad, ulong adlen, const(ubyte)* nsec, const(ubyte)* npub,
            const(ubyte)* k);
}

struct Rng
{
    ulong s = 0x243F6A8885A308D3;
    ulong next() @nogc nothrow
    {
        s ^= s << 13;
        s ^= s >> 7;
        s ^= s << 17;
        return s;
    }
}

// Each candidate authenticates `frame`, writing its tag; returns the tag length.
alias MacFn = size_t function(scope const(ubyte)[] frame) @nogc nothrow @system;

__gshared ubyte[32] key32;
__gshared ubyte[16] key16;
__gshared ubyte[12] nonce;
__gshared ubyte[64] tagBuf;

size_t macHmacSha512(scope const(ubyte)[] f) @nogc nothrow @system
{
    cast(void) crypto_auth(tagBuf.ptr, f.ptr, f.length, key32.ptr);
    return 32;
}

size_t macBlake2b(scope const(ubyte)[] f) @nogc nothrow @system
{
    cast(void) crypto_generichash(tagBuf.ptr, 32, f.ptr, f.length, key32.ptr, 32);
    return 32;
}

size_t macBlake2b16(scope const(ubyte)[] f) @nogc nothrow @system
{
    cast(void) crypto_generichash(tagBuf.ptr, 16, f.ptr, f.length, key32.ptr, 32);
    return 16;
}

size_t macSipHash(scope const(ubyte)[] f) @nogc nothrow @system
{
    cast(void) crypto_shorthash(tagBuf.ptr, f.ptr, f.length, key16.ptr);
    return 8;
}

size_t macAesGmac(scope const(ubyte)[] f) @nogc nothrow @system
{
    ulong clen;
    // empty message, frame as AAD -> c holds only the 16-byte GMAC tag
    cast(void) crypto_aead_aes256gcm_encrypt(tagBuf.ptr, &clen, null, 0, f.ptr, f.length,
            null, nonce.ptr, key32.ptr);
    return cast(size_t) clen;
}

void benchOne(string name, MacFn mac, const(ubyte)[] frame) @system
{
    // iterations scaled to hash a fixed ~total volume per (size), clamped so
    // tiny frames still measure fixed cost and huge frames don't run for minutes.
    ulong iters = 400_000_000UL / (frame.length + 64);
    if (iters < 300)
        iters = 300; // 300 * 4 MiB ~ 1.2 GB -> ~1s, measurable
    if (iters > 3_000_000)
        iters = 3_000_000;

    size_t tlen;
    // warm up
    foreach (_; 0 .. 1000)
        tlen = mac(frame);

    auto t0 = MonoTime.currTime;
    foreach (_; 0 .. iters)
        tlen = mac(frame);
    auto t1 = MonoTime.currTime;

    immutable ns = (t1 - t0).total!"nsecs";
    immutable nsPer = cast(double) ns / iters;
    immutable mbs = (cast(double) frame.length * iters) / (cast(double) ns) * 1000.0; // bytes/ns -> GB/s*...
    // bytes/ns * 1e9 = bytes/s; /1e6 = MB/s  => bytes/ns * 1000 = MB/s
    writefln("  %-16s tag=%2dB  %8.1f ns/frame  %8.0f MB/s", name, tlen, nsPer, mbs);
}

void main() @system
{
    if (sodium_init() < 0)
    {
        writeln("sodium_init failed");
        return;
    }
    foreach (i; 0 .. key32.length)
        key32[i] = cast(ubyte)(i * 7 + 1);
    foreach (i; 0 .. key16.length)
        key16[i] = cast(ubyte)(i * 11 + 3);

    immutable aesOk = crypto_aead_aes256gcm_is_available() == 1;

    writeln("Raft-frame authenticator microbench (auth-only, keyed MACs)");
    writeln("  primitive           tag        fixed cost      throughput");
    writefln("  AES-256-GMAC hardware available: %s", aesOk ? "yes" : "NO (skipped)");

    Rng rng;
    foreach (sz; [40UL, 256, 1024, 65_536, 4 * 1024 * 1024])
    {
        auto frame = new ubyte[sz];
        foreach (ref b; frame)
            b = cast(ubyte)(rng.next() & 0xFF);
        writefln("\n frame = %d B", sz);
        benchOne("HMAC-SHA512-256", &macHmacSha512, frame);
        benchOne("BLAKE2b-256", &macBlake2b, frame);
        benchOne("BLAKE2b-128", &macBlake2b16, frame);
        benchOne("SipHash-2-4", &macSipHash, frame);
        if (aesOk)
            benchOne("AES-256-GMAC", &macAesGmac, frame);
    }
    writeln("\nNote: tag+verify on the raft thread; small frames are amortized over a");
    writeln("group-commit batch (1 MAC per frame, not per write). Fixed cost (ns/frame)");
    writeln("drives the small-frame case; MB/s drives snapshot chunks / large batches.");
}
