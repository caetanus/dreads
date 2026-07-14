module dreads.rdb;

// RDB translator — DUMP/RESTORE's contact surface is the AOF, never the RObj.
//
// The engine's canonical persistence is the command log (AOF/raft). RDB is not a
// first-class serializer of in-memory structures; it is an EXTERNAL translation
// layer that speaks only two languages: RESP write commands (the compactor's
// output — dumpKey/dumpKeyspace in aof.d) and RDB bytes. It never imports
// dreads.obj or a container:
//
//   * DUMP    = dumpKey(valueOnly) -> RESP commands -> commandsToRdb -> + footer.
//   * RESTORE = verify footer -> rdbToCommands(key) -> dispatch (applies + logs).
//
// This file holds the byte-level RDB primitives + CRC64; the command<->RDB
// accumulators live alongside (added incrementally). Format mirrored from Valkey
// src/rdb.h + rdb.c (parity rule): RDB_VERSION 80, reflected CRC64 poly
// 0x95ac9329ac4bc9b5, length varint 6/14/32/64-bit + special int/LZF, ints LE,
// millisecond time 8B LE, binary double 8B LE.

import dreads.mem : ByteBuffer, Arena;
import dreads.dict : fmtLong;
import dreads.resp : RVal, RType, ParseStatus, parseValue, repArrayHeader, repBulk;
import dreads.commands : parseLong, parseDouble, fmtDouble;
import emplace.vector : Vector;

// ---------------------------------------------------------------------------
// Constants (Valkey src/rdb.h)
// ---------------------------------------------------------------------------

enum ushort RDB_VERSION = 80;

// value type bytes (the first byte of a payload)
enum ubyte RDB_TYPE_STRING = 0;
enum ubyte RDB_TYPE_LIST = 1; // plain: len + N strings
enum ubyte RDB_TYPE_SET = 2; // plain: len + N strings
enum ubyte RDB_TYPE_ZSET = 3; // plain: len + N (string, ascii-double)
enum ubyte RDB_TYPE_HASH = 4; // plain: len + N (field, value)
enum ubyte RDB_TYPE_ZSET_2 = 5; // len + N (string, binary-double 8B)
enum ubyte RDB_TYPE_HASH_ZIPMAP = 9;
enum ubyte RDB_TYPE_LIST_ZIPLIST = 10;
enum ubyte RDB_TYPE_SET_INTSET = 11;
enum ubyte RDB_TYPE_ZSET_ZIPLIST = 12;
enum ubyte RDB_TYPE_HASH_ZIPLIST = 13;
enum ubyte RDB_TYPE_LIST_QUICKLIST = 14;
enum ubyte RDB_TYPE_HASH_LISTPACK = 16;
enum ubyte RDB_TYPE_ZSET_LISTPACK = 17;
enum ubyte RDB_TYPE_LIST_QUICKLIST_2 = 18;
enum ubyte RDB_TYPE_SET_LISTPACK = 20;
enum ubyte RDB_TYPE_HASH_2 = 22; // len + N (field, value, expiry-ms 8B); 0 = no TTL

// length-encoding prefixes (top 2 bits of the first byte)
private enum ubyte RDB_6BITLEN = 0;
private enum ubyte RDB_14BITLEN = 1;
private enum ubyte RDB_32BITLEN = 0x80;
private enum ubyte RDB_64BITLEN = 0x81;
private enum ubyte RDB_ENCVAL = 3;
private enum ubyte RDB_ENC_INT8 = 0;
private enum ubyte RDB_ENC_INT16 = 1;
private enum ubyte RDB_ENC_INT32 = 2;
private enum ubyte RDB_ENC_LZF = 3;

// ---------------------------------------------------------------------------
// CRC64 (Jones, reflected) — the DUMP footer checksum. Table built once at start.
// ---------------------------------------------------------------------------

// Computed at compile time (CTFE) so this module has no `shared static this` —
// a runtime ctor would create a cyclic module-init dependency with scripting.
private static immutable ulong[256] crcTable = () {
    ulong[256] t;
    enum ulong REV_POLY = 0x95ac9329ac4bc9b5; // reflected Jones poly (Valkey crcspeed.c)
    foreach (i; 0 .. 256)
    {
        ulong crc = i;
        foreach (_; 0 .. 8)
            crc = (crc & 1) ? (crc >> 1) ^ REV_POLY : crc >> 1;
        t[i] = crc;
    }
    return t;
}();

/// Redis/Valkey CRC64 (seed 0), used for the DUMP payload footer.
ulong crc64(ulong crc, scope const(ubyte)[] s) @nogc nothrow
{
    foreach (b; s)
        crc = crcTable[(crc ^ b) & 0xff] ^ (crc >> 8);
    return crc;
}

// ---------------------------------------------------------------------------
// Writer primitives — append RDB bytes to a ByteBuffer.
// ---------------------------------------------------------------------------

struct RdbWriter
{
    ByteBuffer* o;

    void u8(ubyte b) @nogc nothrow
    {
        o.appendByte(b);
    }

    void raw(scope const(void)[] bytes) @nogc nothrow
    {
        o.append(bytes);
    }

    /// Length-prefix varint (Valkey rdbSaveLen): 6/14/32/64-bit, big-endian tails.
    void saveLen(ulong n) @nogc nothrow
    {
        if (n < (1 << 6))
            u8(cast(ubyte)((RDB_6BITLEN << 6) | n));
        else if (n < (1 << 14))
        {
            u8(cast(ubyte)((RDB_14BITLEN << 6) | ((n >> 8) & 0x3F)));
            u8(cast(ubyte)(n & 0xFF));
        }
        else if (n <= uint.max)
        {
            u8(RDB_32BITLEN);
            foreach_reverse (i; 0 .. 4)
                u8(cast(ubyte)((n >> (i * 8)) & 0xFF)); // big-endian
        }
        else
        {
            u8(RDB_64BITLEN);
            foreach_reverse (i; 0 .. 8)
                u8(cast(ubyte)((n >> (i * 8)) & 0xFF)); // big-endian
        }
    }

    /// A raw string: length prefix then bytes. We never int-encode or LZF on
    /// write (RESTORE re-derives the int encoding); both are valid RDB.
    void saveRawString(scope const(char)[] s) @nogc nothrow
    {
        saveLen(s.length);
        o.append(s);
    }

    /// Absolute millisecond time — 8 bytes little-endian (rdbSaveMillisecondTime).
    void saveMillis(long t) @nogc nothrow
    {
        foreach (i; 0 .. 8)
            u8(cast(ubyte)((cast(ulong) t >> (i * 8)) & 0xFF));
    }

    /// A binary double — the IEEE-754 bit pattern, 8 bytes little-endian (ZSET_2).
    void saveBinaryDouble(double d) @nogc nothrow @trusted
    {
        immutable ulong bits = *cast(ulong*)&d;
        foreach (i; 0 .. 8)
            u8(cast(ubyte)((bits >> (i * 8)) & 0xFF));
    }
}

// ---------------------------------------------------------------------------
// Reader primitives — consume RDB bytes from a slice with a cursor. Every reader
// method returns false on truncation/format error so the caller can reply
// "Bad data format" instead of trusting a partial parse.
// ---------------------------------------------------------------------------

struct RdbReader
{
    const(ubyte)[] data;
    size_t pos;
    private char[24] intScratch; // int-encoded strings materialize here

    bool u8(out ubyte b) @nogc nothrow
    {
        if (pos >= data.length)
            return false;
        b = data[pos++];
        return true;
    }

    /// Read a length varint. `encoded` is set when the byte was a special
    /// (11xxxxxx) encoding — the low 6 bits are the sub-type, handled by loadString.
    bool loadLen(out ulong len, out bool encoded) @nogc nothrow
    {
        ubyte b;
        if (!u8(b))
            return false;
        immutable type = (b & 0xC0) >> 6;
        if (type == RDB_6BITLEN)
        {
            len = b & 0x3F;
            return true;
        }
        else if (type == RDB_14BITLEN)
        {
            ubyte b2;
            if (!u8(b2))
                return false;
            len = ((cast(ulong)(b & 0x3F)) << 8) | b2;
            return true;
        }
        else if (type == RDB_ENCVAL)
        {
            len = b & 0x3F;
            encoded = true;
            return true;
        }
        else if (b == RDB_32BITLEN)
        {
            if (pos + 4 > data.length)
                return false;
            len = 0;
            foreach (i; 0 .. 4)
                len = (len << 8) | data[pos++]; // big-endian
            return true;
        }
        else if (b == RDB_64BITLEN)
        {
            if (pos + 8 > data.length)
                return false;
            len = 0;
            foreach (i; 0 .. 8)
                len = (len << 8) | data[pos++]; // big-endian
            return true;
        }
        return false;
    }

    /// Read a string object. Plain strings return a zero-copy slice into `data`;
    /// int-encoded ones materialize into the reader's scratch. LZF is phase 2.
    bool loadString(out const(char)[] s) @nogc nothrow @trusted
    {
        ulong len;
        bool enc;
        if (!loadLen(len, enc))
            return false;
        if (enc)
        {
            if (len == RDB_ENC_INT8)
            {
                ubyte b;
                if (!u8(b))
                    return false;
                s = fmtLong(cast(byte) b, intScratch);
                return true;
            }
            else if (len == RDB_ENC_INT16)
            {
                if (pos + 2 > data.length)
                    return false;
                short v = cast(short)(data[pos] | (data[pos + 1] << 8)); // LE
                pos += 2;
                s = fmtLong(v, intScratch);
                return true;
            }
            else if (len == RDB_ENC_INT32)
            {
                if (pos + 4 > data.length)
                    return false;
                int v = data[pos] | (data[pos + 1] << 8) | (data[pos + 2] << 16) | (
                    data[pos + 3] << 24); // LE
                pos += 4;
                s = fmtLong(v, intScratch);
                return true;
            }
            else // RDB_ENC_LZF
                return false; // phase 2
        }
        if (pos + len > data.length)
            return false;
        s = cast(const(char)[]) data[pos .. pos + cast(size_t) len];
        pos += cast(size_t) len;
        return true;
    }

    /// 8-byte little-endian millisecond time.
    bool loadMillis(out long t) @nogc nothrow
    {
        if (pos + 8 > data.length)
            return false;
        ulong v = 0;
        foreach (i; 0 .. 8)
            v |= (cast(ulong) data[pos + i]) << (i * 8);
        pos += 8;
        t = cast(long) v;
        return true;
    }

    /// 8-byte little-endian binary double (ZSET_2).
    bool loadBinaryDouble(out double d) @nogc nothrow @trusted
    {
        if (pos + 8 > data.length)
            return false;
        ulong bits = 0;
        foreach (i; 0 .. 8)
            bits |= (cast(ulong) data[pos + i]) << (i * 8);
        pos += 8;
        d = *cast(double*)&bits;
        return true;
    }
}

// ---------------------------------------------------------------------------
// Footer: DUMP payload = <type + value> + RDB version (2B LE) + CRC64 (8B LE).
// ---------------------------------------------------------------------------

/// Append the version + CRC64 footer over everything already in `o` (the value).
void appendFooter(ref ByteBuffer o) @nogc nothrow
{
    o.appendByte(RDB_VERSION & 0xFF);
    o.appendByte((RDB_VERSION >> 8) & 0xFF);
    immutable crc = crc64(0, cast(const(ubyte)[]) o.data);
    foreach (i; 0 .. 8)
        o.appendByte(cast(ubyte)((crc >> (i * 8)) & 0xFF));
}

/// Validate a RESTORE payload's footer: version accepted and CRC matches. On
/// success `body_` is the value bytes (type + value, footer stripped).
bool verifyFooter(scope const(ubyte)[] payload, out const(ubyte)[] body_) @nogc nothrow
{
    if (payload.length < 10)
        return false;
    immutable n = payload.length - 10;
    immutable ushort ver = cast(ushort)(payload[n] | (payload[n + 1] << 8));
    if (ver > RDB_VERSION)
        return false;
    // CRC64 covers the value bytes + the 2 version bytes; 0 means "not checked".
    ulong stored = 0;
    foreach (i; 0 .. 8)
        stored |= (cast(ulong) payload[n + 2 + i]) << (i * 8);
    if (stored != 0)
    {
        immutable calc = crc64(0, payload[0 .. n + 2]);
        if (calc != stored)
            return false;
    }
    body_ = payload[0 .. n];
    return true;
}

// ---------------------------------------------------------------------------
// Encoder: one key's compactor commands  ->  RDB value (type + serialized).
// The commands come from dumpKey(valueOnly): SET / RPUSH / SADD / HSET (+
// HPEXPIREAT for field TTLs) / ZADD. Anything else (XADD — stream) => false so
// DUMP can reply that the type is unsupported. Never touches RObj.
// ---------------------------------------------------------------------------

/// Append the RDB value (type byte + body) for the key whose rebuild `commands`
/// are given. Returns false on an unsupported command or malformed input.
bool commandsToRdb(scope const(ubyte)[] commands, ref Arena arena, ref ByteBuffer sink) @nogc nothrow
{
    enum Kind
    {
        none,
        str,
        list,
        set,
        hash,
        zset
    }

    Kind kind = Kind.none;
    const(char)[] strVal;
    Vector!(const(char)[]) members; // list or set elements (in order)
    Vector!(const(char)[]) hf, hv; // hash fields / values (parallel)
    Vector!ulong httl; // parallel field TTLs (0 = none)
    Vector!(const(char)[]) zm; // zset members
    Vector!double zs; // parallel zset scores

    size_t pos = 0;
    RVal cmd;
    while (pos < commands.length)
    {
        if (parseValue(commands, pos, arena, cmd) != ParseStatus.ok
            || cmd.type != RType.Array || cmd.arr.length < 2)
            return false;
        auto verb = cmd.arr[0].str;
        if (verb == "SET")
        {
            if (cmd.arr.length < 3)
                return false;
            kind = Kind.str;
            strVal = cmd.arr[2].str;
        }
        else if (verb == "RPUSH")
        {
            kind = Kind.list;
            foreach (ref a; cmd.arr[2 .. $])
                members.put(a.str);
        }
        else if (verb == "SADD")
        {
            kind = Kind.set;
            foreach (ref a; cmd.arr[2 .. $])
                members.put(a.str);
        }
        else if (verb == "HSET")
        {
            kind = Kind.hash;
            for (size_t i = 2; i + 1 < cmd.arr.length; i += 2)
            {
                hf.put(cmd.arr[i].str);
                hv.put(cmd.arr[i + 1].str);
                httl.put(0);
            }
        }
        else if (verb == "HPEXPIREAT")
        {
            // HPEXPIREAT key ms FIELDS 1 field
            if (cmd.arr.length < 6)
                return false;
            long ms;
            if (!parseLong(cmd.arr[2].str, ms))
                return false;
            auto field = cmd.arr[5].str;
            foreach (j; 0 .. hf.length)
                if (hf[j] == field)
                {
                    httl[j] = cast(ulong) ms;
                    break;
                }
        }
        else if (verb == "ZADD")
        {
            kind = Kind.zset;
            for (size_t i = 2; i + 1 < cmd.arr.length; i += 2)
            {
                double sc;
                if (!parseDouble(cmd.arr[i].str, sc))
                    return false;
                zs.put(sc);
                zm.put(cmd.arr[i + 1].str);
            }
        }
        else
            return false; // XADD / anything else — stream deferred
    }

    auto w = RdbWriter(&sink);
    final switch (kind)
    {
    case Kind.none:
        return false;
    case Kind.str:
        w.u8(RDB_TYPE_STRING);
        w.saveRawString(strVal);
        break;
    case Kind.list:
        w.u8(RDB_TYPE_LIST);
        w.saveLen(members.length);
        foreach (m; members[])
            w.saveRawString(m);
        break;
    case Kind.set:
        w.u8(RDB_TYPE_SET);
        w.saveLen(members.length);
        foreach (m; members[])
            w.saveRawString(m);
        break;
    case Kind.hash:
        bool anyTTL = false;
        foreach (t; httl[])
            if (t != 0)
            {
                anyTTL = true;
                break;
            }
        w.u8(anyTTL ? RDB_TYPE_HASH_2 : RDB_TYPE_HASH);
        w.saveLen(hf.length);
        foreach (j; 0 .. hf.length)
        {
            w.saveRawString(hf[j]);
            w.saveRawString(hv[j]);
            if (anyTTL)
                w.saveMillis(cast(long) httl[j]);
        }
        break;
    case Kind.zset:
        w.u8(RDB_TYPE_ZSET_2);
        w.saveLen(zm.length);
        foreach (j; 0 .. zm.length)
        {
            w.saveRawString(zm[j]);
            w.saveBinaryDouble(zs[j]);
        }
        break;
    }
    return true;
}

// ---------------------------------------------------------------------------
// Decoder: RDB value  ->  the RESP commands that rebuild `key`. Dispatch applies
// them (and logs them — the "vice-versa"). Plain encodings only for now; compact
// encodings (listpack/ziplist/intset/quicklist) + LZF + stream are phase 2. On
// `false` the caller must discard `sink` (it may hold a partial command).
// ---------------------------------------------------------------------------

bool rdbToCommands(scope const(char)[] key, scope const(ubyte)[] body_, ref ByteBuffer sink) @nogc nothrow
{
    auto r = RdbReader(body_, 0);
    ubyte type;
    if (!r.u8(type))
        return false;
    switch (type)
    {
    case RDB_TYPE_STRING:
        const(char)[] s;
        if (!r.loadString(s))
            return false;
        repArrayHeader(sink, 3);
        repBulk(sink, "SET");
        repBulk(sink, key);
        repBulk(sink, s);
        return true;
    case RDB_TYPE_LIST:
        return decodeStrings(r, key, "RPUSH", sink);
    case RDB_TYPE_SET:
        return decodeStrings(r, key, "SADD", sink);
    case RDB_TYPE_HASH:
    case RDB_TYPE_HASH_2:
        return decodeHash(r, type, key, sink);
    case RDB_TYPE_ZSET_2:
        return decodeZsetBinary(r, key, sink);
    default:
        return false; // compact encodings / ZSET(3 ascii) / stream: phase 2
    }
}

// A flat "len + N strings" body -> one RPUSH/SADD command.
private bool decodeStrings(ref RdbReader r, scope const(char)[] key,
    scope const(char)[] verb, ref ByteBuffer sink) @nogc nothrow
{
    ulong n;
    bool e;
    if (!r.loadLen(n, e) || e)
        return false;
    if (n == 0)
        return true;
    repArrayHeader(sink, cast(size_t)(2 + n));
    repBulk(sink, verb);
    repBulk(sink, key);
    foreach (_; 0 .. n)
    {
        const(char)[] s;
        if (!r.loadString(s))
            return false;
        repBulk(sink, s);
    }
    return true;
}

// HASH / HASH_2 -> HSET, then one HPEXPIREAT per TTL'd field (HASH_2 only).
private bool decodeHash(ref RdbReader r, ubyte type, scope const(char)[] key,
    ref ByteBuffer sink) @nogc nothrow
{
    ulong n;
    bool e;
    if (!r.loadLen(n, e) || e)
        return false;
    if (n == 0)
        return true;
    Vector!(const(char)[]) ttlFields; // TTL'd fields, stashed for HPEXPIREAT
    Vector!long ttlVals;
    repArrayHeader(sink, cast(size_t)(2 + n * 2));
    repBulk(sink, "HSET");
    repBulk(sink, key);
    foreach (_; 0 .. n)
    {
        const(char)[] f, v;
        if (!r.loadString(f) || !r.loadString(v))
            return false;
        repBulk(sink, f);
        repBulk(sink, v);
        if (type == RDB_TYPE_HASH_2)
        {
            long t;
            if (!r.loadMillis(t))
                return false;
            if (t != 0)
            {
                ttlFields.put(f);
                ttlVals.put(t);
            }
        }
    }
    foreach (j; 0 .. ttlFields.length)
    {
        repArrayHeader(sink, 6);
        repBulk(sink, "HPEXPIREAT");
        repBulk(sink, key);
        char[24] tb = void;
        repBulk(sink, fmtLong(ttlVals[j], tb));
        repBulk(sink, "FIELDS");
        repBulk(sink, "1");
        repBulk(sink, ttlFields[j]);
    }
    return true;
}

// ZSET_2 (member, binary-double) -> ZADD key score member ... (score first).
private bool decodeZsetBinary(ref RdbReader r, scope const(char)[] key, ref ByteBuffer sink) @nogc nothrow
{
    ulong n;
    bool e;
    if (!r.loadLen(n, e) || e)
        return false;
    if (n == 0)
        return true;
    repArrayHeader(sink, cast(size_t)(2 + n * 2));
    repBulk(sink, "ZADD");
    repBulk(sink, key);
    foreach (_; 0 .. n)
    {
        const(char)[] m;
        double sc;
        if (!r.loadString(m) || !r.loadBinaryDouble(sc))
            return false;
        char[40] fb = void;
        repBulk(sink, fmtDouble(fb, sc));
        repBulk(sink, m);
    }
    return true;
}

@nogc nothrow unittest // CRC64 matches the canonical Redis/Valkey check vector
{
    assert(crc64(0, cast(const(ubyte)[]) "123456789") == 0xe9c6d914c4b8d9caUL);
    assert(crc64(0, cast(const(ubyte)[]) "") == 0);
}

@system unittest // length varint + string + double round-trip through the primitives
{
    ByteBuffer o;
    auto w = RdbWriter(&o);
    w.saveLen(5);
    w.saveLen(300); // 14-bit
    w.saveLen(100_000); // 32-bit
    w.saveRawString("hello");
    w.saveBinaryDouble(3.5);
    w.saveMillis(1_000_100_000);

    auto r = RdbReader(cast(const(ubyte)[]) o.data, 0);
    ulong len;
    bool enc;
    assert(r.loadLen(len, enc) && len == 5 && !enc);
    assert(r.loadLen(len, enc) && len == 300);
    assert(r.loadLen(len, enc) && len == 100_000);
    const(char)[] s;
    assert(r.loadString(s) && s == "hello");
    double d;
    assert(r.loadBinaryDouble(d) && d == 3.5);
    long t;
    assert(r.loadMillis(t) && t == 1_000_100_000);
}

version (unittest) private string respArr(string[] args...)
{
    import std.conv : to;

    string r = "*" ~ args.length.to!string ~ "\r\n";
    foreach (a; args)
        r ~= "$" ~ a.length.to!string ~ "\r\n" ~ a ~ "\r\n";
    return r;
}

@system unittest // command <-> RDB round-trip per type (pure codec, no RObj)
{
    static string roundtrip(string cmds, string key)
    {
        Arena arena;
        ByteBuffer rdb;
        assert(commandsToRdb(cast(const(ubyte)[]) cmds, arena, rdb), "encode failed");
        appendFooter(rdb);
        const(ubyte)[] body_;
        assert(verifyFooter(cast(const(ubyte)[]) rdb.data, body_), "footer failed");
        ByteBuffer back;
        assert(rdbToCommands(key, body_, back), "decode failed");
        return (cast(string) back.data).idup;
    }

    assert(roundtrip(respArr("SET", "k", "hello"), "k") == respArr("SET", "k", "hello"));
    assert(roundtrip(respArr("RPUSH", "k", "a", "b", "c"), "k") == respArr("RPUSH", "k", "a", "b", "c"));
    assert(roundtrip(respArr("SADD", "k", "x", "y"), "k") == respArr("SADD", "k", "x", "y"));
    assert(roundtrip(respArr("HSET", "k", "f1", "v1", "f2", "v2"), "k")
            == respArr("HSET", "k", "f1", "v1", "f2", "v2"));
    assert(roundtrip(respArr("ZADD", "k", "1.5", "m1", "2.5", "m2"), "k")
            == respArr("ZADD", "k", "1.5", "m1", "2.5", "m2"));

    // hash with a field TTL: HSET then HPEXPIREAT survives the round-trip (HASH_2)
    auto hcmds = respArr("HSET", "k", "f1", "v1", "f2", "v2")
        ~ respArr("HPEXPIREAT", "k", "5000", "FIELDS", "1", "f1");
    assert(roundtrip(hcmds, "k") == hcmds);
}

@system unittest // a corrupted CRC is rejected
{
    Arena arena;
    ByteBuffer rdb;
    assert(commandsToRdb(cast(const(ubyte)[]) respArr("SET", "k", "v"), arena, rdb));
    appendFooter(rdb);
    auto bytes = (cast(ubyte[]) rdb.data).dup;
    bytes[$ - 1] ^= 0xFF; // flip a CRC byte
    const(ubyte)[] body_;
    assert(!verifyFooter(bytes, body_));
}
