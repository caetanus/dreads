module dreads.respvariant;

// The RESP reply oracle. A reply is built as a typed value (an RVariant tree),
// and ONE encoder — `encode(o, proto)` — resolves it to RESP2 or RESP3 bytes.
// The protocol lives here and nowhere else: command handlers declare *what* the
// reply is (a map, a set, a double, a null), never *how* it is framed on the
// wire. Zero-overhead abstraction in D: a SumType tag (jump table, no vtable),
// match! for the encoder, and Unique children — a reply tree has single
// ownership (each child belongs to exactly one parent, no sharing), so
// unique_ptr is the right tool; freeing the root cascades, no manual dispose.
//
// automem.Vector can't hold move-only elements (its internal shift copies), so
// the child list is a tiny move-aware vector (moveEmplace, never copies). The
// value automem gives here is the smart pointer, not the container.

import std.sumtype : SumType, match;
import core.lifetime : forward, move, moveEmplace;
import core.stdc.stdio : snprintf;
import core.stdc.stdlib : malloc, realloc, free;

import automem.unique : Unique;
import std.experimental.allocator.mallocator : Mallocator;

import dreads.mem : ByteBuffer;

/// A reply-tree node, uniquely owned by its parent.
alias RV = Unique!(RVariant, Mallocator);

/// Minimal move-only vector: append + index + iterate, never copies an element
/// (moveEmplace on insert, bitwise realloc-move on grow — valid for Unique,
/// which is a pointer+allocator with no self-references). Frees its elements.
struct MVec(T)
{
    private T* p;
    private size_t len, cap;

    @disable this(this);

    void put()(auto ref T e) @nogc nothrow @trusted
    {
        if (len == cap)
        {
            cap = cap ? cap * 2 : 4;
            p = cast(T*) realloc(p, cap * T.sizeof);
            assert(p !is null, "out of memory");
        }
        moveEmplace(e, p[len]);
        len++;
    }

    inout(T)[] opSlice() inout @nogc nothrow @trusted return
    {
        return p[0 .. len];
    }

    @property size_t length() const @nogc nothrow
    {
        return len;
    }

    ~this() @nogc nothrow @trusted
    {
        foreach (i; 0 .. len)
            destroy!false(p[i]); // run element dtor (releases the Unique); no re-init
        if (p !is null)
            free(p);
    }
}

// Each RESP type is a distinct payload struct — SumType disambiguates by type,
// and distinct wrappers also dodge implicit conversions (e.g. bool<->long).
struct Nil
{
}

struct Simple
{
    const(char)[] s;
}

struct Err
{
    const(char)[] s;
}

struct Bulk
{
    const(char)[] s;
}

struct Verbatim
{
    char[3] fmt = "txt";
    const(char)[] s;
}

struct BigNum
{
    const(char)[] digits;
}

struct Num
{
    long v;
}

struct Dbl
{
    double v;
}

struct Bool
{
    bool v;
}

struct Arr
{
    MVec!RV items;
    void add(P)(auto ref P payload) @nogc nothrow
    {
        items.put(node(forward!payload));
    }
}

struct SetT
{
    MVec!RV items;
    void add(P)(auto ref P payload) @nogc nothrow
    {
        items.put(node(forward!payload));
    }
}

struct Push
{
    MVec!RV items;
    void add(P)(auto ref P payload) @nogc nothrow
    {
        items.put(node(forward!payload));
    }
}

/// One key/value edge of a map; insertion order is preserved (RESP map replies
/// keep source order, not sorted order).
struct Pair
{
    RV key;
    RV val;
}

struct MapT
{
    MVec!Pair pairs;
    void add(KP, VP)(auto ref KP keyPayload, auto ref VP valPayload) @nogc nothrow
    {
        pairs.put(Pair(node(forward!keyPayload), node(forward!valPayload)));
    }
}

struct RVariant
{
    SumType!(Nil, Simple, Err, Bulk, Verbatim, BigNum, Num, Dbl, Bool, Arr, SetT, Push, MapT) v;

    this(P)(auto ref P payload) @nogc nothrow
    {
        v = move(payload);
    }
}

/// A reply value (root). `rv(Bulk("hi"))`, `rv(Nil())`, `rv(move(arr))`.
RVariant rv(P)(auto ref P payload) @nogc nothrow
{
    return RVariant(forward!payload);
}

/// A heap child node. Built in place from its payload — never by copying a
/// RVariant.
RV node(P)(auto ref P payload) @nogc nothrow
{
    return RV(forward!payload);
}

// --- the oracle: one place that knows RESP2 vs RESP3 framing ---------------

private void appendLong(ref ByteBuffer o, long v) @nogc nothrow
{
    char[24] tmp = void;
    immutable n = snprintf(tmp.ptr, tmp.length, "%lld", v);
    o.append(tmp[0 .. n]);
}

private void aggHeader(ref ByteBuffer o, char resp3Byte, size_t count,
        size_t resp2Count, int proto) @nogc nothrow
{
    o.appendByte(proto >= 3 ? resp3Byte : '*');
    appendLong(o, cast(long)(proto >= 3 ? count : resp2Count));
    o.append("\r\n");
}

private void bulkBytes(ref ByteBuffer o, const(char)[] s) @nogc nothrow
{
    o.appendByte('$');
    appendLong(o, cast(long) s.length);
    o.append("\r\n");
    o.append(s);
    o.append("\r\n");
}

// Non-capturing type probe: the match lambdas reference only the payload, so
// no closure is allocated (unlike capturing o/proto in the encoder itself).
private T* peek(T)(ref RVariant r) @nogc nothrow @trusted
{
    return r.v.match!((ref T x) => &x, (ref _) => cast(T*) null);
}

// Per-type serialization, chosen at compile time — a plain templated function,
// so it captures nothing and stays @nogc. Aggregates recurse through encode().
private void emit(T)(ref T x, ref ByteBuffer o, int proto) @nogc nothrow @trusted
{
    static if (is(T == Nil))
        o.append(proto >= 3 ? "_\r\n" : "$-1\r\n");
    else static if (is(T == Simple))
    {
        o.appendByte('+');
        o.append(x.s);
        o.append("\r\n");
    }
    else static if (is(T == Err))
    {
        o.appendByte('-');
        o.append(x.s);
        o.append("\r\n");
    }
    else static if (is(T == Bulk))
        bulkBytes(o, x.s);
    else static if (is(T == Verbatim))
    {
        if (proto >= 3)
        {
            o.appendByte('=');
            appendLong(o, cast(long)(x.s.length + 4));
            o.append("\r\n");
            o.append(x.fmt[]);
            o.appendByte(':');
            o.append(x.s);
            o.append("\r\n");
        }
        else
            bulkBytes(o, x.s);
    }
    else static if (is(T == BigNum))
    {
        if (proto >= 3)
        {
            o.appendByte('(');
            o.append(x.digits);
            o.append("\r\n");
        }
        else
            bulkBytes(o, x.digits);
    }
    else static if (is(T == Num))
    {
        o.appendByte(':');
        appendLong(o, x.v);
        o.append("\r\n");
    }
    else static if (is(T == Dbl))
    {
        char[40] b = void;
        immutable n = snprintf(b.ptr, b.length, "%.17g", x.v);
        if (proto >= 3)
        {
            o.appendByte(',');
            o.append(b[0 .. n]);
            o.append("\r\n");
        }
        else
            bulkBytes(o, b[0 .. n]);
    }
    else static if (is(T == Bool))
    {
        if (proto >= 3)
            o.append(x.v ? "#t\r\n" : "#f\r\n");
        else
            o.append(x.v ? ":1\r\n" : ":0\r\n");
    }
    else static if (is(T == Arr) || is(T == SetT) || is(T == Push))
    {
        static if (is(T == SetT))
            enum char h3 = '~';
        else static if (is(T == Push))
            enum char h3 = '>';
        else
            enum char h3 = '*';
        aggHeader(o, h3, x.items.length, x.items.length, proto);
        foreach (ref c; x.items[])
            encode(*c.borrow, o, proto);
    }
    else static if (is(T == MapT))
    {
        // RESP3 map = %N pairs; RESP2 flat array of 2N elements.
        aggHeader(o, '%', x.pairs.length, x.pairs.length * 2, proto);
        foreach (ref pr; x.pairs[])
        {
            encode(*pr.key.borrow, o, proto);
            encode(*pr.val.borrow, o, proto);
        }
    }
    else
        static assert(false, "unhandled RESP payload " ~ T.stringof);
}

/// Serialize the reply tree to `o` in the negotiated protocol (2 or 3). One
/// place owns the RESP2-vs-RESP3 framing; handlers never see the protocol.
void encode(ref RVariant r, ref ByteBuffer o, int proto) @nogc nothrow @trusted
{
    static foreach (T; typeof(RVariant.v).Types)
    {
        if (auto p = peek!T(r))
            return emit(*p, o, proto);
    }
}

version (unittest) private string enc(ref RVariant r, int proto)
{
    ByteBuffer o;
    encode(r, o, proto);
    return (cast(char[]) o.data).idup;
}

unittest // scalars: identical in RESP2/RESP3, plus the divergent ones
{
    auto s = rv(Simple("OK"));
    assert(enc(s, 2) == "+OK\r\n" && enc(s, 3) == "+OK\r\n");
    auto b = rv(Bulk("hi"));
    assert(enc(b, 2) == "$2\r\nhi\r\n" && enc(b, 3) == "$2\r\nhi\r\n");
    auto i = rv(Num(-3));
    assert(enc(i, 2) == ":-3\r\n" && enc(i, 3) == ":-3\r\n");

    auto n = rv(Nil());
    assert(enc(n, 2) == "$-1\r\n" && enc(n, 3) == "_\r\n");
    auto bo = rv(Bool(true));
    assert(enc(bo, 2) == ":1\r\n" && enc(bo, 3) == "#t\r\n");
    auto bn = rv(BigNum("123456789012345678901234567890"));
    assert(enc(bn, 3) == "(123456789012345678901234567890\r\n");
    assert(enc(bn, 2) == "$30\r\n123456789012345678901234567890\r\n");
    auto vb = rv(Verbatim("txt", "hello"));
    assert(enc(vb, 3) == "=9\r\ntxt:hello\r\n" && enc(vb, 2) == "$5\r\nhello\r\n");
}

unittest // aggregates: array / set / push / map degrade correctly
{
    Arr a;
    a.add(Num(1));
    a.add(Bulk("x"));
    auto ra = rv(move(a));
    assert(enc(ra, 2) == "*2\r\n:1\r\n$1\r\nx\r\n");
    assert(enc(ra, 3) == "*2\r\n:1\r\n$1\r\nx\r\n"); // arrays same in both

    SetT st;
    st.add(Num(1));
    auto rs = rv(move(st));
    assert(enc(rs, 2) == "*1\r\n:1\r\n" && enc(rs, 3) == "~1\r\n:1\r\n");

    MapT m;
    m.add(Bulk("a"), Num(1));
    auto rm = rv(move(m));
    assert(enc(rm, 2) == "*2\r\n$1\r\na\r\n:1\r\n");
    assert(enc(rm, 3) == "%1\r\n$1\r\na\r\n:1\r\n");
}

unittest // nested: a map whose value is an array of doubles
{
    Arr scores;
    scores.add(Dbl(1.5));
    MapT m;
    m.add(Bulk("k"), move(scores));
    auto r = rv(move(m));
    assert(enc(r, 3) == "%1\r\n$1\r\nk\r\n*1\r\n,1.5\r\n");
    assert(enc(r, 2) == "*2\r\n$1\r\nk\r\n*1\r\n$3\r\n1.5\r\n");
}
