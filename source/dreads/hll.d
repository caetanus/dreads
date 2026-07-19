module dreads.hll;

// HyperLogLog (PFADD/PFCOUNT/PFMERGE): dense encoding only — 16384 six-bit
// registers (12KB) behind a small "HYLL" header, stored as a plain string
// value so persistence and type checks come for free. Classic bias-corrected
// estimator with linear counting for the low range (~0.81% error, same
// promise as Redis).

import core.stdc.math : log;
import core.stdc.string : memcmp, memcpy, memset;

import dreads.mem : ByteBuffer;
import dreads.obj : Keyspace, ObjType, RObj;
import dreads.resp;

private enum REGISTERS = 16384; // 2^14
private enum HDR = 16;
private enum DENSE_BYTES = HDR + (REGISTERS * 6 + 7) / 8;

private bool isHll(scope const(char)[] s) @nogc nothrow
{
    return s.length == DENSE_BYTES && s[0 .. 4] == "HYLL";
}

private ubyte getReg(scope const(char)[] s, uint idx) @nogc nothrow
{
    auto bit = cast(size_t) idx * 6;
    auto byteIdx = HDR + bit / 8;
    auto shift = bit % 8;
    auto lo = cast(ubyte) s[byteIdx] >> shift;
    auto hi = shift > 2 ? cast(uint) cast(ubyte) s[byteIdx + 1] << (8 - shift) : 0;
    return (lo | hi) & 0x3F;
}

private void setReg(char[] s, uint idx, ubyte val) @nogc nothrow
{
    auto bit = cast(size_t) idx * 6;
    auto byteIdx = HDR + bit / 8;
    auto shift = bit % 8;
    auto mask = cast(uint)(0x3F << shift);
    auto cur = cast(ubyte) s[byteIdx] | (shift > 2 ? cast(uint) cast(ubyte) s[byteIdx + 1] << 8 : 0);
    cur = (cur & ~mask) | (cast(uint) val << shift);
    s[byteIdx] = cast(char)(cur & 0xFF);
    if (shift > 2)
        s[byteIdx + 1] = cast(char)((cur >> 8) & 0xFF);
}

private ulong hash64(scope const(char)[] s) @nogc nothrow
{
    // FNV-1a 64 + splitmix finalizer: good dispersion for HLL purposes
    ulong h = 0xcbf2_9ce4_8422_2325;
    foreach (c; s)
    {
        h ^= c;
        h *= 0x100_0000_01b3;
    }
    h ^= h >> 30;
    h *= 0xbf58_476d_1ce4_e5b9;
    h ^= h >> 27;
    h *= 0x94d0_49bb_1331_11eb;
    h ^= h >> 31;
    return h;
}

/// Register index (low 14 bits) and rank (position of first 1 in the rest).
private void hashToReg(ulong h, out uint idx, out ubyte rank) @nogc nothrow
{
    idx = cast(uint)(h & (REGISTERS - 1));
    auto rest = h >> 14;
    ubyte r = 1;
    while ((rest & 1) == 0 && r < 51)
    {
        rest >>= 1;
        r++;
    }
    rank = r;
}

private double estimate(scope const(char)[] s) @nogc nothrow
{
    enum double m = REGISTERS;
    double sum = 0;
    uint zeros = 0;
    foreach (i; 0 .. REGISTERS)
    {
        auto r = getReg(s, cast(uint) i);
        if (r == 0)
            zeros++;
        sum += 1.0 / cast(double)(1UL << r);
    }
    enum alpha = 0.7213 / (1.0 + 1.079 / m);
    auto e = alpha * m * m / sum;
    if (e <= 2.5 * m && zeros > 0)
        e = m * log(m / cast(double) zeros); // linear counting
    return e;
}

/// Fresh dense HLL owned by the caller.
private char[] newHll() @nogc nothrow @trusted
{
    import dreads.mem : allocZeroed;
    import dreads.alloc : KeyspaceAllocator;

    auto p = allocZeroed!KeyspaceAllocator(DENSE_BYTES);
    p[0 .. 4] = "HYLL";
    return p;
}

/// The key's HLL bytes for writing, creating if missing.
/// null with err=true on type errors.
private char[] hllFor(ref Keyspace ks, scope const(char)[] key, bool create,
        ref ByteBuffer o, out bool err) @nogc nothrow
{
    bool wrong;
    auto obj = ks.lookupTyped(key, ObjType.str, wrong);
    if (wrong)
    {
        err = true;
        repError(o, "WRONGTYPE Operation against a key holding the wrong kind of value");
        return null;
    }
    if (obj is null)
    {
        if (!create)
            return null;
        RObj no;
        no.type = ObjType.str;
        no.str.setRaw(newHll());
        ks.d.set(key, no);
        return ks.lookup(key).str.rawMut();
    }
    char[24] sb = void;
    if (!isHll(obj.str.bytes(sb))) // an int-encoded value materializes then fails the magic
    {
        err = true;
        repError(o, "WRONGTYPE Key is not a valid HyperLogLog string value.");
        return null;
    }
    return obj.str.rawMut(); // a valid HLL is already raw bytes (no conversion)
}

/// PFADD key [element ...]
public void pfadd(ref Keyspace ks, const(RVal)[] args, ref ByteBuffer o) @nogc nothrow
{
    if (args.length < 1)
    {
        repError(o, "ERR wrong number of arguments for 'pfadd' command");
        return;
    }
    bool err;
    // Creating the HLL for a missing key IS a change (Valkey: `PFADD key` with no
    // elements on a new key returns 1 and materialises an empty HLL).
    immutable existed = ks.exists(args[0].str);
    auto hll = hllFor(ks, args[0].str, true, o, err);
    if (err)
        return;
    bool changed = !existed;
    foreach (ref a; args[1 .. $])
    {
        uint idx;
        ubyte rank;
        hashToReg(hash64(a.str), idx, rank);
        if (getReg(hll, idx) < rank)
        {
            setReg(hll, idx, rank);
            changed = true;
        }
    }
    repInt(o, changed ? 1 : 0);
}

/// PFCOUNT key [key ...]
public void pfcount(ref Keyspace ks, const(RVal)[] args, ref ByteBuffer o) @nogc nothrow
{
    if (args.length < 1)
    {
        repError(o, "ERR wrong number of arguments for 'pfcount' command");
        return;
    }
    if (args.length == 1)
    {
        bool err;
        auto hll = hllFor(ks, args[0].str, false, o, err);
        if (err)
            return;
        repInt(o, hll is null ? 0 : cast(long)(estimate(hll) + 0.5));
        return;
    }
    // multiple keys: merge into a stack-temporary
    char[DENSE_BYTES] tmp = 0;
    tmp[0 .. 4] = "HYLL";
    bool any = false;
    foreach (ref a; args)
    {
        bool err;
        auto hll = hllFor(ks, a.str, false, o, err);
        if (err)
            return;
        if (hll is null)
            continue;
        any = true;
        foreach (i; 0 .. REGISTERS)
        {
            auto r = getReg(hll, cast(uint) i);
            if (r > getReg(tmp[], cast(uint) i))
                setReg(tmp[], cast(uint) i, r);
        }
    }
    repInt(o, any ? cast(long)(estimate(tmp[]) + 0.5) : 0);
}

/// PFMERGE dest [src ...]
public void pfmerge(ref Keyspace ks, const(RVal)[] args, ref ByteBuffer o) @nogc nothrow
{
    if (args.length < 1)
    {
        repError(o, "ERR wrong number of arguments for 'pfmerge' command");
        return;
    }
    // validate sources first (dest creation must not survive a source error)
    foreach (ref a; args[1 .. $])
    {
        bool err;
        hllFor(ks, a.str, false, o, err);
        if (err)
            return;
    }
    bool err;
    hllFor(ks, args[0].str, true, o, err); // ensure dest exists
    if (err)
        return;
    foreach (ref a; args[1 .. $])
    {
        bool err2;
        auto src = hllFor(ks, a.str, false, o, err2);
        // re-resolve dest each round: source lookups may have rehashed
        auto dst2 = ks.lookup(args[0].str).str.rawMut();
        if (src is null)
            continue;
        foreach (i; 0 .. REGISTERS)
        {
            auto r = getReg(src, cast(uint) i);
            if (r > getReg(dst2, cast(uint) i))
                setReg(dst2, cast(uint) i, r);
        }
    }
    repSimple(o, "OK");
}
