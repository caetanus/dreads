module dreads.dict;

// The dataspace's hash table lives in the `emplace` package now (it WAS this
// file — extracted, generalized to own any resource-bearing value, and shared
// as a reusable library). This module keeps the dreads-specific value types and
// aliases the container as `Dict` so the existing call sites are unchanged.

import dreads.mem : mallocAppend, mallocDup, freeSlice;

public import emplace.hashmap : HashMap;

/// The dataspace hash table: malloc'd string keys -> owned values.
alias Dict(V) = HashMap!V;

/// Owned string value (hash fields, plain string objects).
/// long -> decimal into `buf` (needs >= 20 bytes). Zero-overhead: a divide loop,
/// no runtime format-string parse. Handles long.min via unsigned magnitude.
package const(char)[] fmtLong(long v, return scope char[] buf) @nogc nothrow @trusted
{
    ulong u = v < 0 ? -cast(ulong) v : cast(ulong) v;
    size_t p = buf.length;
    do
    {
        buf[--p] = cast(char)('0' + u % 10);
        u /= 10;
    }
    while (u);
    if (v < 0)
        buf[--p] = '-';
    return buf[p .. $];
}

/// Scalar value kind. String values split into `embstr` (short, <= 44 bytes,
/// never mutated) and `raw` (long OR mutated) to match Redis's OBJECT ENCODING —
/// APPEND/SETRANGE always yield raw regardless of length. `i64` is int-encoded;
/// `f64`/`nul`/`big` are reserved for later.
public enum ValKind : ubyte
{
    embstr,
    raw,
    i64,
    f64,
    nul,
    big
}

/// A scalar value: an idiomatic D tagged union over the RESP3 scalars. The union
/// arms are PRIVATE — callers go through the methods, so nobody reads the wrong
/// arm. An integer value keeps ONLY the long (Redis OBJ_ENCODING_INT parity) and
/// materializes its bytes on demand, so INCR is a native add and GET formats lazily.
public struct StrVal
{
    /// The Redis embstr/raw cutoff: a fresh string of at most this many bytes is
    /// embstr-encoded, longer ones raw.
    enum size_t EMBSTR_MAX = 44;

    private ValKind kind = ValKind.embstr;
    private union
    {
        const(char)[] str_; // kind == embstr | raw: owned malloc'd bytes
        long i_; // kind == i64
        double d_; // kind == f64 (reserved)
    }

    private bool isStrKind() const @nogc nothrow
    {
        return kind == ValKind.embstr || kind == ValKind.raw;
    }

    /// From client bytes, with Redis canonical-int detection: an exact-round-trip
    /// integer is stored int-encoded, everything else as a string.
    static StrVal of(scope const(char)[] bytes) @nogc nothrow
    {
        long iv;
        return canonicalInt(bytes, iv) ? StrVal.ofInt(iv) : StrVal.ofStr(bytes);
    }

    private static StrVal ofStr(scope const(char)[] bytes) @nogc nothrow
    {
        StrVal r;
        r.kind = bytes.length <= EMBSTR_MAX ? ValKind.embstr : ValKind.raw;
        r.str_ = mallocDup(bytes);
        return r;
    }

    /// Construct directly int-encoded (INCR result, etc.).
    static StrVal ofInt(long i) @nogc nothrow
    {
        StrVal r;
        r.kind = ValKind.i64;
        r.i_ = i;
        return r;
    }

    /// Construct as a raw string WITHOUT int-detection — for values that must
    /// round-trip byte-exact and keep a stable pointer (e.g. Lua script bodies).
    static StrVal ofRaw(scope const(char)[] bytes) @nogc nothrow
    {
        StrVal r;
        r.kind = ValKind.raw;
        r.str_ = mallocDup(bytes);
        return r;
    }

    /// A deep copy preserving the encoding (int copies trivially; a string
    /// re-dups its bytes and keeps its embstr/raw kind).
    StrVal dup() const @nogc nothrow @trusted
    {
        if (isStrKind)
        {
            StrVal r;
            r.kind = kind;
            r.str_ = mallocDup(str_);
            return r;
        }
        StrVal r = this; // int/reserved arms own nothing — a bitwise copy is safe
        return r;
    }

    /// The stable owned byte slice, valid ONLY for a string value (asserts).
    /// Use where a materialized-int scratch would dangle (persistent references).
    const(char)[] rawView() const @nogc nothrow
    {
        assert(isStrKind, "rawView on a non-string value");
        return str_;
    }

    ValKind encoding() const @nogc nothrow
    {
        return kind;
    }

    bool isInt() const @nogc nothrow
    {
        return kind == ValKind.i64;
    }

    /// The integer value when int-encoded.
    bool asInt(out long value) const @nogc nothrow
    {
        if (kind == ValKind.i64)
        {
            value = i_;
            return true;
        }
        return false;
    }

    /// Byte view: a string returns its slice (zero cost); an int formats into the
    /// caller's `scratch` (>= 20 bytes) and returns that slice.
    const(char)[] bytes(return scope char[] scratch) const @nogc nothrow @trusted
    {
        final switch (kind)
        {
        case ValKind.embstr:
        case ValKind.raw:
            return str_;
        case ValKind.i64:
            return fmtLong(i_, scratch);
        case ValKind.f64:
        case ValKind.nul:
        case ValKind.big:
            return null; // reserved kinds not stored yet
        }
    }

    /// Byte length without materializing.
    size_t len() const @nogc nothrow @trusted
    {
        if (isStrKind)
            return str_.length;
        if (kind == ValKind.i64)
        {
            char[24] b = void;
            return fmtLong(i_, b).length;
        }
        return 0;
    }

    /// Materialize into an owned RAW string in place for a mutating op
    /// (APPEND/SETRANGE/SETBIT); returns the slice. Any mutation yields raw, so
    /// this always leaves the value raw-encoded (like Redis).
    const(char)[] toStr() @nogc nothrow @trusted
    {
        if (kind == ValKind.i64)
        {
            char[24] b = void;
            str_ = mallocDup(fmtLong(i_, b)); // reads i_ before overwriting the arm
        }
        kind = ValKind.raw;
        return str_;
    }

    /// Overwrite with a fresh string value (INCRBYFLOAT/GETSET result), reusing
    /// the current allocation when it fits; embstr/raw by length.
    void assign(scope const(char)[] nv) @nogc nothrow @trusted
    {
        if (isStrKind && str_.ptr !is null && nv.length <= str_.length)
        {
            (cast(char*) str_.ptr)[0 .. nv.length] = nv[]; // slice copy, not memcpy
            str_ = str_.ptr[0 .. nv.length];
        }
        else
        {
            if (isStrKind)
                freeSlice(str_);
            str_ = mallocDup(nv);
        }
        kind = nv.length <= EMBSTR_MAX ? ValKind.embstr : ValKind.raw;
    }

    /// Overwrite with an int (INCR result); frees any prior bytes.
    void assignInt(long value) @nogc nothrow @trusted
    {
        if (isStrKind)
            freeSlice(str_);
        kind = ValKind.i64;
        i_ = value;
    }

    /// Append raw bytes (APPEND), materializing an int first. mallocAppend frees
    /// the old buffer and returns the grown one. The result is raw (via toStr).
    void append(scope const(char)[] add) @nogc nothrow @trusted
    {
        toStr();
        str_ = mallocAppend(str_, add);
    }

    /// The raw mutable byte buffer, materializing an int first. Binary-blob
    /// commands (bitmap, HyperLogLog) own and edit/grow this buffer in place.
    char[] rawMut() @nogc nothrow @trusted
    {
        toStr();
        return cast(char[]) str_;
    }

    /// Adopt an already-owned raw buffer, freeing the prior one. Ownership of
    /// `owned` transfers to this value.
    void setRaw(const(char)[] owned) @nogc nothrow @trusted
    {
        if (isStrKind)
            freeSlice(str_);
        kind = ValKind.raw;
        str_ = owned;
    }

    void free() @nogc nothrow @trusted
    {
        if (isStrKind)
            freeSlice(str_);
        str_ = null;
        kind = ValKind.embstr;
    }
}

/// Redis's canonical-integer test: `v` must be exactly the decimal form of the
/// returned long (so "007", "+5", "-0", " 5" and overflow all fail). Round-trip:
/// parse, then require the reformat to equal the input.
public bool canonicalInt(scope const(char)[] v, out long iv) @nogc nothrow @trusted
{
    if (v.length == 0 || v.length > 20)
        return false;
    if (v[0] != '-' && (v[0] < '0' || v[0] > '9')) // optional '-' then digits
        return false;
    long acc = 0;
    immutable neg = v[0] == '-';
    size_t start = neg ? 1 : 0;
    if (start == v.length)
        return false;
    foreach (c; v[start .. $])
    {
        if (c < '0' || c > '9')
            return false;
        if (acc > (long.max - (c - '0')) / 10)
            return false; // magnitude overflow (long.min still caught by round-trip)
        acc = acc * 10 + (c - '0');
    }
    iv = neg ? -acc : acc;
    char[24] b = void;
    return fmtLong(iv, b) == v; // canonical iff it reformats byte-identical
}

/// Empty value for set-like dicts.
public struct Unit
{
    void free() @nogc nothrow
    {
    }
}

/// Score value for sorted sets.
public struct DoubleVal
{
    double d = 0;

    void free() @nogc nothrow
    {
    }
}

unittest // Dict (== emplace.HashMap) with dreads' owned string values
{
    Dict!StrVal d;
    scope (exit)
        d.free();
    char[24] sb = void;
    assert(d.get("missing") is null);
    assert(d.set("foo", StrVal.of("bar")));
    assert(d.get("foo").bytes(sb) == "bar");
    assert(!d.set("foo", StrVal.of("baz"))); // overwrite frees the old value
    assert(d.get("foo").bytes(sb) == "baz");
    assert(d.exists("foo") && d.length == 1);
    assert(d.del("foo") && !d.del("foo") && !d.exists("foo"));
}

unittest // set-like and score dicts
{
    Dict!Unit s;
    scope (exit)
        s.free();
    assert(s.set("a", Unit()) && !s.set("a", Unit()));
    assert(s.exists("a") && !s.exists("b"));

    Dict!DoubleVal z;
    scope (exit)
        z.free();
    z.set("m", DoubleVal(1.5));
    z.get("m").d += 1;
    assert(z.get("m").d == 2.5);
}
