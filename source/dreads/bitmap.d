module dreads.bitmap;

// Bitmap commands over string values. Redis bit addressing: bit 0 is the
// most significant bit of byte 0. All @nogc; strings grow zero-filled up to
// the 512MB proto-max-bulk-len cap.

import core.bitop : popcnt;
import core.stdc.string : memcpy, memset;

import dreads.commands : eqICKeyword, normalizeRange, parseLong, gWriteNoOp;
import dreads.alloc : KeyspaceAllocator;
import dreads.mem : Arena, ByteBuffer, allocZeroed;
import dreads.obj : Keyspace, ObjType, RObj;
import dreads.resp;

private enum MAX_BITMAP_BYTES = 512UL * 1024 * 1024;
private enum MAX_BIT_OFFSET = 4UL * 1024 * 1024 * 1024 - 1; // 4G bits

private void repWrongTypeB(ref ByteBuffer o) @nogc nothrow
{
    repError(o, "WRONGTYPE Operation against a key holding the wrong kind of value");
}

private ubyte bitAt(scope const(char)[] s, ulong bit) @nogc nothrow
{
    auto byteIdx = bit >> 3;
    if (byteIdx >= s.length)
        return 0;
    return (s[byteIdx] >> (7 - (bit & 7))) & 1;
}

/// Grows (never shrinks) the string value of key to at least nbytes,
/// zero-filling. Returns the mutable bytes, or null on WRONGTYPE.
private char[] ensureBytes(ref Keyspace ks, scope const(char)[] key, size_t nbytes,
        out bool wrongType) @nogc nothrow
{
    auto obj = ks.lookupTyped(key, ObjType.str, wrongType);
    if (wrongType)
        return null;
    if (obj is null)
    {
        RObj no;
        no.type = ObjType.str;
        no.str.setRaw(allocZeroed!KeyspaceAllocator(nbytes));
        ks.d.set(key, no);
        return ks.lookup(key).str.rawMut();
    }
    auto cur = obj.str.rawMut(); // materialize an int-encoded value to raw bytes
    if (cur.length >= nbytes)
        return cur; // we own the keyspace-allocated buffer
    auto grown = allocZeroed!KeyspaceAllocator(nbytes);
    grown[0 .. cur.length] = cur[]; // slice copy, not memcpy
    obj.str.setRaw(grown); // frees the old buffer
    return grown;
}

/// SETBIT key offset 0|1
public void setbit(ref Keyspace ks, const(RVal)[] args, ref ByteBuffer o) @nogc nothrow
{
    if (args.length != 3)
    {
        repError(o, "ERR wrong number of arguments for 'setbit' command");
        return;
    }
    long off;
    if (!parseLong(args[1].str, off) || off < 0 || cast(ulong) off > MAX_BIT_OFFSET)
    {
        repError(o, "ERR bit offset is not an integer or out of range");
        return;
    }
    if (args[2].str != "0" && args[2].str != "1")
    {
        repError(o, "ERR bit is not an integer or out of range");
        return;
    }
    bool wrong;
    immutable neededLen = cast(size_t)(off >> 3) + 1;
    // Capture the current length BEFORE we possibly grow the string: SETBIT only
    // dirties (rdb_changes / WATCH / propagation) when it changes the bit OR extends
    // the string (a new key or a higher offset both extend). See gWriteNoOp.
    size_t oldLen = 0;
    {
        auto cur = ks.lookupTyped(args[0].str, ObjType.str, wrong);
        if (wrong)
        {
            repWrongTypeB(o);
            return;
        }
        if (cur !is null)
            oldLen = cur.str.len();
    }
    auto bytes = ensureBytes(ks, args[0].str, neededLen, wrong);
    if (wrong)
    {
        repWrongTypeB(o);
        return;
    }
    auto mask = cast(ubyte)(1 << (7 - (off & 7)));
    auto old = (bytes[cast(size_t)(off >> 3)] & mask) ? 1 : 0;
    immutable newBit = args[2].str[0] == '1' ? 1 : 0;
    if (newBit)
        bytes[cast(size_t)(off >> 3)] |= mask;
    else
        bytes[cast(size_t)(off >> 3)] &= ~cast(int) mask;
    repInt(o, old);
    if (old == newBit && neededLen <= oldLen)
        gWriteNoOp = true; // bit unchanged and no length growth
}

/// GETBIT key offset
public void getbit(ref Keyspace ks, const(RVal)[] args, ref ByteBuffer o) @nogc nothrow
{
    if (args.length != 2)
    {
        repError(o, "ERR wrong number of arguments for 'getbit' command");
        return;
    }
    long off;
    if (!parseLong(args[1].str, off) || off < 0 || cast(ulong) off > MAX_BIT_OFFSET)
    {
        repError(o, "ERR bit offset is not an integer or out of range");
        return;
    }
    bool wrong;
    auto obj = ks.lookupTyped(args[0].str, ObjType.str, wrong);
    if (wrong)
    {
        repWrongTypeB(o);
        return;
    }
    char[24] sb = void;
    repInt(o, obj is null ? 0 : bitAt(obj.str.bytes(sb), cast(ulong) off));
}

/// Resolves [start end [BYTE|BIT]] into an inclusive bit range.
/// `badInt` distinguishes a non-integer start/end (Valkey: "not an integer or
/// out of range") from a syntax error (extra/unknown trailing words).
private bool bitRange(const(RVal)[] opts, size_t bitLen, out ulong fromBit,
        out ulong toBit, out bool empty, out bool badInt) @nogc nothrow
{
    long start = 0, stop = -1;
    bool bitMode;
    if (opts.length >= 2)
    {
        if (opts.length > 3)
            return false;
        if (opts.length == 3)
        {
            if (eqICKeyword(opts[2].str, "BIT"))
                bitMode = true;
            else if (!eqICKeyword(opts[2].str, "BYTE"))
                return false;
        }
        if (!parseLong(opts[0].str, start) || !parseLong(opts[1].str, stop))
        {
            badInt = true;
            return false;
        }
    }
    else if (opts.length == 1)
    {
        // start without end: end defaults to -1 (byte mode)
        if (!parseLong(opts[0].str, start))
        {
            badInt = true;
            return false;
        }
    }
    auto units = bitMode ? cast(long) bitLen : cast(long)((bitLen + 7) / 8);
    normalizeRange(start, stop, units);
    if (start > stop || units == 0)
    {
        empty = true;
        return true;
    }
    if (bitMode)
    {
        fromBit = cast(ulong) start;
        toBit = cast(ulong) stop;
    }
    else
    {
        fromBit = cast(ulong) start * 8;
        toBit = cast(ulong) stop * 8 + 7;
    }
    if (toBit >= bitLen)
        toBit = bitLen - 1;
    return true;
}

/// BITCOUNT key [start end [BYTE|BIT]]
public void bitcount(ref Keyspace ks, const(RVal)[] args, ref ByteBuffer o) @nogc nothrow
{
    if (args.length < 1)
    {
        repError(o, "ERR wrong number of arguments for 'bitcount' command");
        return;
    }
    ulong fromBit, toBit;
    bool empty, badInt;
    // validate the range args before touching the key (Valkey errors on bad
    // args even for a missing or wrong-typed key)
    if (!bitRange(args[1 .. $], 0, fromBit, toBit, empty, badInt))
    {
        repError(o, badInt ? "ERR value is not an integer or out of range" : "ERR syntax error");
        return;
    }
    bool wrong;
    auto obj = ks.lookupTyped(args[0].str, ObjType.str, wrong);
    if (wrong)
    {
        repWrongTypeB(o);
        return;
    }
    if (obj is null)
    {
        repInt(o, 0);
        return;
    }
    char[24] sb = void;
    auto s = obj.str.bytes(sb);
    cast(void) bitRange(args[1 .. $], s.length * 8, fromBit, toBit, empty, badInt);
    if (empty || s.length == 0)
    {
        repInt(o, 0);
        return;
    }
    long n = 0;
    ulong bit = fromBit;
    while (bit <= toBit && (bit & 7) != 0) // leading partial byte
        n += bitAt(s, bit++);
    while (bit + 8 <= toBit + 1) // whole bytes
    {
        n += popcnt(cast(uint) cast(ubyte) s[cast(size_t)(bit >> 3)]);
        bit += 8;
    }
    while (bit <= toBit) // trailing bits
        n += bitAt(s, bit++);
    repInt(o, n);
}

/// BITPOS key bit [start [end [BYTE|BIT]]]
public void bitpos(ref Keyspace ks, const(RVal)[] args, ref ByteBuffer o) @nogc nothrow
{
    if (args.length < 2)
    {
        repError(o, "ERR wrong number of arguments for 'bitpos' command");
        return;
    }
    long bitArg;
    if (!parseLong(args[1].str, bitArg))
    {
        repError(o, "ERR value is not an integer or out of range");
        return;
    }
    if (bitArg != 0 && bitArg != 1)
    {
        repError(o, "ERR The bit argument must be 1 or 0.");
        return;
    }
    bool wantOne = bitArg == 1;
    // pad "start without end" to end=-1, then validate the range args before
    // touching the key (Valkey errors on bad args even for a missing key)
    bool rangeGiven = args.length > 2;
    bool endGiven = args.length > 3;
    RVal[3] padded;
    size_t nOpts = 0;
    if (rangeGiven)
    {
        if (args.length > 5)
        {
            repError(o, "ERR syntax error");
            return;
        }
        padded[0] = args[2];
        if (endGiven)
            padded[1] = args[3];
        else
        {
            padded[1].type = RType.BulkString;
            padded[1].str = "-1";
        }
        nOpts = 2;
        if (args.length == 5)
        {
            padded[2] = args[4];
            nOpts = 3;
        }
    }
    ulong fromBit = 0, toBit = 0;
    bool empty, badInt;
    if (!bitRange(padded[0 .. nOpts], 0, fromBit, toBit, empty, badInt))
    {
        repError(o, badInt ? "ERR value is not an integer or out of range" : "ERR syntax error");
        return;
    }
    bool wrong;
    auto obj = ks.lookupTyped(args[0].str, ObjType.str, wrong);
    if (wrong)
    {
        repWrongTypeB(o);
        return;
    }
    char[24] sb = void;
    auto s = obj is null ? null : obj.str.bytes(sb);
    // missing/empty key is all virtual zeros: 0 is found at 0, 1 is absent
    if (s.length == 0)
    {
        repInt(o, wantOne ? -1 : 0);
        return;
    }
    toBit = s.length * 8 - 1;
    if (rangeGiven)
    {
        cast(void) bitRange(padded[0 .. nOpts], s.length * 8, fromBit, toBit, empty, badInt);
        if (empty)
        {
            repInt(o, -1);
            return;
        }
    }
    foreach (bit; fromBit .. toBit + 1)
    {
        if (bitAt(s, bit) == (wantOne ? 1 : 0))
        {
            repInt(o, cast(long) bit);
            return;
        }
    }
    // searching for 0 without an explicit end: virtual zeros past the end
    if (!wantOne && !endGiven)
        repInt(o, cast(long)(s.length * 8));
    else
        repInt(o, -1);
}

/// BITOP AND|OR|XOR|NOT dest src [src ...]
public void bitop(ref Keyspace ks, const(RVal)[] args, ref ByteBuffer o, ref Arena arena) @nogc nothrow
{
    if (args.length < 3)
    {
        repError(o, "ERR wrong number of arguments for 'bitop' command");
        return;
    }
    char op;
    if (eqICKeyword(args[0].str, "AND"))
        op = '&';
    else if (eqICKeyword(args[0].str, "OR"))
        op = '|';
    else if (eqICKeyword(args[0].str, "XOR"))
        op = '^';
    else if (eqICKeyword(args[0].str, "NOT"))
        op = '~';
    else
    {
        repError(o, "ERR syntax error");
        return;
    }
    auto srcs = args[2 .. $];
    if (op == '~' && srcs.length != 1)
    {
        repError(o, "ERR BITOP NOT must be called with a single source key.");
        return;
    }
    size_t maxLen = 0;
    auto views = arena.allocArray!(const(char)[])(srcs.length);
    foreach (i, ref k; srcs)
    {
        bool wrong;
        auto obj = ks.lookupTyped(k.str, ObjType.str, wrong);
        if (wrong)
        {
            repWrongTypeB(o);
            return;
        }
        // each operand needs its own scratch (they coexist); str values ignore it
        views[i] = obj is null ? null : obj.str.bytes(arena.allocArray!char(24));
        if (views[i].length > maxLen)
            maxLen = views[i].length;
    }
    if (maxLen == 0)
    {
        ks.del(args[1].str);
        repInt(o, 0);
        return;
    }
    auto outBuf = arena.allocArray!char(maxLen);
    foreach (i; 0 .. maxLen)
    {
        ubyte acc = op == '&' ? 0xFF : 0;
        foreach (vi, v; views)
        {
            ubyte b = i < v.length ? cast(ubyte) v[i] : 0;
            final switch (op)
            {
            case '&':
                acc &= b;
                break;
            case '|':
                acc |= b;
                break;
            case '^':
                acc = vi == 0 ? b : cast(ubyte)(acc ^ b);
                break;
            case '~':
                acc = cast(ubyte) ~cast(int) b;
                break;
            }
        }
        outBuf[i] = acc;
    }
    ks.setStr(args[1].str, outBuf);
    repInt(o, cast(long) maxLen);
}

// ---------------------------------------------------------------------------
// BITFIELD
// ---------------------------------------------------------------------------

private bool parseFieldType(scope const(char)[] s, out bool signed, out uint width) @nogc nothrow
{
    if (s.length < 2 || (s[0] != 'u' && s[0] != 'U' && s[0] != 'i' && s[0] != 'I'))
        return false;
    signed = s[0] == 'i' || s[0] == 'I';
    long w;
    if (!parseLong(s[1 .. $], w) || w < 1)
        return false;
    if (signed ? w > 64 : w > 63)
        return false;
    width = cast(uint) w;
    return true;
}

private bool parseFieldOffset(scope const(char)[] s, uint width, out ulong bitOff) @nogc nothrow
{
    long v;
    bool scaled = s.length > 1 && s[0] == '#';
    if (!parseLong(scaled ? s[1 .. $] : s, v) || v < 0)
        return false;
    auto off = cast(ulong) v * (scaled ? width : 1);
    if (off + width > MAX_BIT_OFFSET + 1)
        return false;
    bitOff = off;
    return true;
}

private ulong readField(scope const(char)[] s, ulong off, uint width) @nogc nothrow
{
    ulong v = 0;
    foreach (i; 0 .. width)
        v = (v << 1) | bitAt(s, off + i);
    return v;
}

private void writeField(char[] s, ulong off, uint width, ulong v) @nogc nothrow
{
    foreach (i; 0 .. width)
    {
        auto bit = (v >> (width - 1 - i)) & 1;
        auto byteIdx = cast(size_t)((off + i) >> 3);
        auto mask = cast(ubyte)(1 << (7 - ((off + i) & 7)));
        if (bit)
            s[byteIdx] |= mask;
        else
            s[byteIdx] &= ~cast(int) mask;
    }
}

private long signExtend(ulong raw, uint width) @nogc nothrow
{
    if (width == 64)
        return cast(long) raw;
    if (raw & (1UL << (width - 1)))
        return cast(long)(raw | (~0UL << width));
    return cast(long) raw;
}

/// BITFIELD key [GET ty off | SET ty off val | INCRBY ty off incr | OVERFLOW WRAP|SAT|FAIL]...
public void bitfield(ref Keyspace ks, const(RVal)[] args, ref ByteBuffer o,
        ref Arena arena, bool readOnly) @nogc nothrow
{
    if (args.length < 1)
    {
        repError(o, "ERR wrong number of arguments for 'bitfield' command");
        return;
    }
    enum Ovf
    {
        wrap,
        sat,
        fail
    }

    // first pass: validate and find the largest touched offset
    Ovf ovf = Ovf.wrap;
    size_t i = 1;
    ulong maxBit = 0;
    size_t nops = 0;
    bool mutates = false;
    while (i < args.length)
    {
        auto w = args[i].str;
        if (eqICKeyword(w, "OVERFLOW") && i + 1 < args.length)
        {
            if (eqICKeyword(args[i + 1].str, "WRAP"))
                ovf = Ovf.wrap;
            else if (eqICKeyword(args[i + 1].str, "SAT"))
                ovf = Ovf.sat;
            else if (eqICKeyword(args[i + 1].str, "FAIL"))
                ovf = Ovf.fail;
            else
            {
                repError(o, "ERR Invalid OVERFLOW type specified");
                return;
            }
            i += 2;
            continue;
        }
        bool isGet = eqICKeyword(w, "GET");
        bool isSet = eqICKeyword(w, "SET");
        bool isIncr = eqICKeyword(w, "INCRBY");
        auto need = isGet ? 3 : 4;
        if ((!isGet && !isSet && !isIncr) || i + need > args.length)
        {
            repError(o, "ERR syntax error");
            return;
        }
        if ((isSet || isIncr) && readOnly)
        {
            repError(o, "ERR BITFIELD_RO only supports the GET subcommand");
            return;
        }
        bool signed;
        uint width;
        ulong off;
        if (!parseFieldType(args[i + 1].str, signed, width))
        {
            repError(o, "ERR Invalid bitfield type. Use something like i16 u8." ~
                    " Note that u64 is not supported but i64 is.");
            return;
        }
        if (!parseFieldOffset(args[i + 2].str, width, off))
        {
            repError(o, "ERR bit offset is not an integer or out of range");
            return;
        }
        long val;
        if (!isGet && !parseLong(args[i + 3].str, val))
        {
            repError(o, "ERR value is not an integer or out of range");
            return;
        }
        if (!isGet)
            mutates = true;
        if (off + width > maxBit)
            maxBit = off + width;
        nops++;
        i += need;
    }
    if (nops == 0)
    {
        repArrayHeader(o, 0);
        return;
    }

    bool wrong;
    char[] bytes;
    char[24] sb = void; // scratch for an int-encoded read value; must outlive the pass
    bool anyChange = false; // dirty only if a SET changed a field, an INCRBY ran, or grew
    if (mutates)
    {
        immutable neededLen = cast(size_t)((maxBit + 7) / 8);
        size_t oldLen0 = 0; // pre-growth length: a new key or a higher offset extends
        {
            auto cur0 = ks.lookupTyped(args[0].str, ObjType.str, wrong);
            if (wrong)
            {
                repWrongTypeB(o);
                return;
            }
            if (cur0 !is null)
                oldLen0 = cur0.str.len();
        }
        bytes = ensureBytes(ks, args[0].str, neededLen, wrong);
        if (wrong)
        {
            repWrongTypeB(o);
            return;
        }
        if (neededLen > oldLen0)
            anyChange = true;
    }
    else
    {
        auto obj = ks.lookupTyped(args[0].str, ObjType.str, wrong);
        if (wrong)
        {
            repWrongTypeB(o);
            return;
        }
        bytes = obj is null ? null : cast(char[]) obj.str.bytes(sb);
    }

    // second pass: execute
    repArrayHeader(o, nops);
    ovf = Ovf.wrap;
    i = 1;
    while (i < args.length)
    {
        auto w = args[i].str;
        if (eqICKeyword(w, "OVERFLOW"))
        {
            if (eqICKeyword(args[i + 1].str, "SAT"))
                ovf = Ovf.sat;
            else if (eqICKeyword(args[i + 1].str, "FAIL"))
                ovf = Ovf.fail;
            else
                ovf = Ovf.wrap;
            i += 2;
            continue;
        }
        bool isGet = eqICKeyword(w, "GET");
        bool isSet = eqICKeyword(w, "SET");
        bool signed;
        uint width;
        ulong off;
        cast(void) parseFieldType(args[i + 1].str, signed, width);
        cast(void) parseFieldOffset(args[i + 2].str, width, off);

        auto raw = readField(bytes, off, width);
        auto cur = signed ? signExtend(raw, width) : cast(long) raw;

        if (isGet)
        {
            repInt(o, cur);
            i += 3;
            continue;
        }
        long operand;
        parseLong(args[i + 3].str, operand);
        long result;
        bool overflowed = false;
        if (isSet)
        {
            result = operand;
            // range check against the field width
            if (signed)
            {
                auto minV = width == 64 ? long.min : -(1L << (width - 1));
                auto maxV = width == 64 ? long.max : (1L << (width - 1)) - 1;
                if (result < minV || result > maxV)
                {
                    overflowed = true;
                    result = result < minV ? minV : maxV;
                }
            }
            else
            {
                auto maxV = (1UL << width) - 1;
                if (operand < 0 || cast(ulong) operand > maxV)
                {
                    overflowed = true;
                    result = operand < 0 ? 0 : cast(long) maxV;
                }
            }
        }
        else // INCRBY
        {
            import core.checkedint : adds;

            bool nativeOvf;
            result = adds(cur, operand, nativeOvf);
            if (signed)
            {
                auto minV = width == 64 ? long.min : -(1L << (width - 1));
                auto maxV = width == 64 ? long.max : (1L << (width - 1)) - 1;
                if (nativeOvf || result < minV || result > maxV)
                {
                    overflowed = true;
                    result = (operand > 0) ? maxV : minV;
                }
            }
            else
            {
                auto maxV = cast(long)((1UL << width) - 1);
                if (nativeOvf || result < 0 || result > maxV)
                {
                    overflowed = true;
                    result = (operand > 0) ? maxV : 0;
                }
            }
        }

        if (overflowed && ovf == Ovf.fail)
        {
            repNullBulk(o);
            i += 4;
            continue;
        }
        ulong toWrite;
        if (overflowed && ovf == Ovf.wrap)
        {
            // wrap = plain modular arithmetic in the field width
            auto wide = isSet ? cast(ulong) operand : raw + cast(ulong) operand;
            toWrite = width == 64 ? wide : wide & ((1UL << width) - 1);
        }
        else
            toWrite = width == 64 ? cast(ulong) result : cast(ulong) result & ((1UL << width) - 1);
        // dirty tracking: INCRBY always counts; SET only when the field changes
        if (!isSet)
            anyChange = true;
        else if (toWrite != (width == 64 ? raw : raw & ((1UL << width) - 1)))
            anyChange = true;
        writeField(bytes, off, width, toWrite);
        // GET-style reply: SET returns the OLD value, INCRBY the new one
        if (isSet)
            repInt(o, cur);
        else
        {
            auto nr = readField(bytes, off, width);
            repInt(o, signed ? signExtend(nr, width) : cast(long) nr);
        }
        i += 4;
    }
    if (!anyChange)
        gWriteNoOp = true; // no field changed / all-GET ⇒ not dirty, don't propagate
}
