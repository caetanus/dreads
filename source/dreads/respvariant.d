module dreads.respvariant;

// The RESP reply oracle (dreads domain). A reply is built as a typed RVariant
// tree, and ONE encoder — encode(o, proto) — resolves it to RESP2 or RESP3
// bytes. The protocol lives here alone; handlers declare *what* the reply is
// (map/set/double/null/push), never *how* it frames.
//
// RVariant is a hand-rolled tagged union (enum Kind + union), not std.sumtype:
// std.sumtype breaks on recursive move-only members (a tree of unique_ptr
// children) under forward references — reproduced with both our Uniq and
// automem's Unique. The manual union is zero-overhead (a `final switch` is a
// jump table, no vtable), C++/Rust in spirit, and fully under our control.
//
// Children are Uniq (unique_ptr): a reply tree is single-ownership, freeing the
// root cascades. The child list is a small move-aware vector (MVec) — automem's
// Vector can't hold move-only elements. Both are slated to move into the
// vendored dlang-non-gc-data-structures library; RVariant stays here (dreads).

import core.lifetime : forward, move, moveEmplace;
import core.stdc.stdio : snprintf;
import core.stdc.stdlib : realloc, free;

import std.experimental.allocator.mallocator : Mallocator;

import dreads.smartptr : Uniq;
import dreads.mem : ByteBuffer;

/// A pool for reply-tree nodes: an intrusive free list of fixed-size blocks
/// (every RVariant node is the same size). Freeing a node returns it to the
/// list instead of the OS, so the next reply reuses it — the benchmark showed
/// this is ~10-16x cheaper per node than malloc, taking the oracle from ~1.5x
/// to ~1.25x of direct emit. Single event-loop thread, so a plain __gshared
/// list is race-free (same model as the deferred notify queue).
struct NodePool
{
    private __gshared void* freeHead;
    __gshared NodePool instance;

    void[] allocate(size_t n) @nogc nothrow @trusted
    {
        if (freeHead !is null)
        {
            auto p = freeHead;
            freeHead = *cast(void**) p; // next pointer stored in the free block
            return p[0 .. n];
        }
        return Mallocator.instance.allocate(n);
    }

    bool deallocate(void[] b) @nogc nothrow @trusted
    {
        *cast(void**) b.ptr = freeHead; // push onto the free list
        freeHead = b.ptr;
        return true;
    }
}

/// A reply-tree node, uniquely owned by its parent, drawn from the node pool.
alias RV = Uniq!(RVariant, NodePool);

/// Minimal move-only vector (moveEmplace on insert, realloc-move on grow, never
/// copies an element). Holds Uniq children / RVariant pairs.
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

    void opAssign(MVec rhs) @nogc nothrow @trusted
    {
        // move-assign: swap buffers, rhs's destructor frees ours
        auto tp = p;
        auto tl = len;
        auto tc = cap;
        p = rhs.p;
        len = rhs.len;
        cap = rhs.cap;
        rhs.p = tp;
        rhs.len = tl;
        rhs.cap = tc;
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
            destroy!false(p[i]);
        if (p !is null)
            free(p);
    }
}

// Payload wrapper structs — the builder vocabulary (rv(Bulk("x")), node(Num(1))).
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

/// Aggregate builders: append children, then wrap with rv()/node() (moved in).
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

/// A map builder — children are stored flattened (k0, v0, k1, v1, ...), which
/// preserves insertion order and needs no Pair type.
struct MapT
{
    MVec!RV kids;
    void add(K, V)(auto ref K keyPayload, auto ref V valPayload) @nogc nothrow
    {
        kids.put(node(forward!keyPayload));
        kids.put(node(forward!valPayload));
    }
}

enum Kind : ubyte
{
    nil,
    simple,
    err,
    bulk,
    verbatim,
    bignum,
    num,
    dbl,
    boolean,
    arr,
    set,
    push,
    map
}

/// Tagged union. Move-only (owns its children through `kids`). The union holds
/// only POD (slices/scalars), so it needs no destructor; `kids` carries the
/// children and its own destructor cascades the free.
struct RVariant
{
    Kind kind = Kind.nil;
    union
    {
        const(char)[] str; // simple / err / bulk / bignum
        Verbatim vb; // verbatim
        long i; // num
        double d; // dbl
        bool b; // boolean
    }

    MVec!RV kids; // arr/set/push children, or flattened map pairs

    @disable this(this);

    this(P)(auto ref P payload) @nogc nothrow @trusted
    {
        static if (is(P == Nil))
            kind = Kind.nil;
        else static if (is(P == Simple))
        {
            kind = Kind.simple;
            str = payload.s;
        }
        else static if (is(P == Err))
        {
            kind = Kind.err;
            str = payload.s;
        }
        else static if (is(P == Bulk))
        {
            kind = Kind.bulk;
            str = payload.s;
        }
        else static if (is(P == BigNum))
        {
            kind = Kind.bignum;
            str = payload.digits;
        }
        else static if (is(P == Verbatim))
        {
            kind = Kind.verbatim;
            vb = payload;
        }
        else static if (is(P == Num))
        {
            kind = Kind.num;
            i = payload.v;
        }
        else static if (is(P == Dbl))
        {
            kind = Kind.dbl;
            d = payload.v;
        }
        else static if (is(P == Bool))
        {
            kind = Kind.boolean;
            b = payload.v;
        }
        else static if (is(P == Arr))
        {
            kind = Kind.arr;
            kids = move(payload.items);
        }
        else static if (is(P == SetT))
        {
            kind = Kind.set;
            kids = move(payload.items);
        }
        else static if (is(P == Push))
        {
            kind = Kind.push;
            kids = move(payload.items);
        }
        else static if (is(P == MapT))
        {
            kind = Kind.map;
            kids = move(payload.kids);
        }
        else
            static assert(false, "not a RESP payload: " ~ P.stringof);
    }
}

/// A reply value (root). `rv(Bulk("hi"))`, `rv(Nil())`, `rv(move(arr))`.
RVariant rv(P)(auto ref P payload) @nogc nothrow
{
    return RVariant(forward!payload);
}

/// A heap child node, built in place from its payload.
RV node(P)(auto ref P payload) @nogc nothrow
{
    return RV.make(forward!payload);
}

// --- the oracle: one place that knows RESP2 vs RESP3 framing ---------------

private void appendLong(ref ByteBuffer o, long v) @nogc nothrow
{
    char[24] tmp = void;
    immutable n = snprintf(tmp.ptr, tmp.length, "%lld", v);
    o.append(tmp[0 .. n]);
}

private void bulkBytes(ref ByteBuffer o, const(char)[] s) @nogc nothrow
{
    o.appendByte('$');
    appendLong(o, cast(long) s.length);
    o.append("\r\n");
    o.append(s);
    o.append("\r\n");
}

private void aggHeader(ref ByteBuffer o, char resp3Byte, size_t count,
        size_t resp2Count, int proto) @nogc nothrow
{
    o.appendByte(proto >= 3 ? resp3Byte : '*');
    appendLong(o, cast(long)(proto >= 3 ? count : resp2Count));
    o.append("\r\n");
}

/// Serialize the reply tree to `o` in the negotiated protocol (2 or 3).
void encode(ref RVariant r, ref ByteBuffer o, int proto) @nogc nothrow @trusted
{
    final switch (r.kind)
    {
    case Kind.nil:
        o.append(proto >= 3 ? "_\r\n" : "$-1\r\n");
        break;
    case Kind.simple:
        o.appendByte('+');
        o.append(r.str);
        o.append("\r\n");
        break;
    case Kind.err:
        o.appendByte('-');
        o.append(r.str);
        o.append("\r\n");
        break;
    case Kind.bulk:
        bulkBytes(o, r.str);
        break;
    case Kind.bignum:
        if (proto >= 3)
        {
            o.appendByte('(');
            o.append(r.str);
            o.append("\r\n");
        }
        else
            bulkBytes(o, r.str);
        break;
    case Kind.verbatim:
        if (proto >= 3)
        {
            o.appendByte('=');
            appendLong(o, cast(long)(r.vb.s.length + 4));
            o.append("\r\n");
            o.append(r.vb.fmt[]);
            o.appendByte(':');
            o.append(r.vb.s);
            o.append("\r\n");
        }
        else
            bulkBytes(o, r.vb.s);
        break;
    case Kind.num:
        o.appendByte(':');
        appendLong(o, r.i);
        o.append("\r\n");
        break;
    case Kind.dbl:
        char[40] db = void;
        immutable n = snprintf(db.ptr, db.length, "%.17g", r.d);
        if (proto >= 3)
        {
            o.appendByte(',');
            o.append(db[0 .. n]);
            o.append("\r\n");
        }
        else
            bulkBytes(o, db[0 .. n]);
        break;
    case Kind.boolean:
        if (proto >= 3)
            o.append(r.b ? "#t\r\n" : "#f\r\n");
        else
            o.append(r.b ? ":1\r\n" : ":0\r\n");
        break;
    case Kind.arr:
        aggHeader(o, '*', r.kids.length, r.kids.length, proto);
        foreach (ref c; r.kids[])
            encode(c.get, o, proto);
        break;
    case Kind.set:
        aggHeader(o, '~', r.kids.length, r.kids.length, proto);
        foreach (ref c; r.kids[])
            encode(c.get, o, proto);
        break;
    case Kind.push:
        aggHeader(o, '>', r.kids.length, r.kids.length, proto);
        foreach (ref c; r.kids[])
            encode(c.get, o, proto);
        break;
    case Kind.map:
        // RESP3 %N pairs; RESP2 flat *2N. kids is flattened k0,v0,k1,v1,...
        aggHeader(o, '%', r.kids.length / 2, r.kids.length, proto);
        foreach (ref c; r.kids[])
            encode(c.get, o, proto);
        break;
    }
}

// ---------------------------------------------------------------------------
// Lazy oracle: the reply is streamed straight from its source (a hash, a set, a
// range) instead of materialized into a tree. The oracle emits the proto-aware
// header, then a scope delegate streams the children — the delegate never
// escapes, so it lives on the stack and this stays @nogc and allocation-free
// (benchmark: within noise of hand-written direct emit, vs ~1.2-1.5x for the
// materialized path). This is the default for replies backed by a container.
// ---------------------------------------------------------------------------

alias Emit = void delegate(ref ByteBuffer o, int proto) @nogc nothrow;

private void lazyAgg(ref ByteBuffer o, char resp3Byte, size_t count,
        size_t resp2Count, int proto, scope Emit emit) @nogc nothrow
{
    aggHeader(o, resp3Byte, count, resp2Count, proto);
    emit(o, proto);
}

/// Array of `n` elements (identical framing in both protocols).
void lazyArray(ref ByteBuffer o, int proto, size_t n, scope Emit emit) @nogc nothrow
{
    lazyAgg(o, '*', n, n, proto, emit);
}

/// Set of `n` elements — `~n` in RESP3, `*n` in RESP2.
void lazySet(ref ByteBuffer o, int proto, size_t n, scope Emit emit) @nogc nothrow
{
    lazyAgg(o, '~', n, n, proto, emit);
}

/// Push of `n` elements — `>n` in RESP3, `*n` in RESP2.
void lazyPush(ref ByteBuffer o, int proto, size_t n, scope Emit emit) @nogc nothrow
{
    lazyAgg(o, '>', n, n, proto, emit);
}

/// Map of `pairs` key/value pairs — `%pairs` in RESP3, flat `*2*pairs` in RESP2.
/// `emit` must stream exactly `2*pairs` elements (k0, v0, k1, v1, ...).
void lazyMap(ref ByteBuffer o, int proto, size_t pairs, scope Emit emit) @nogc nothrow
{
    lazyAgg(o, '%', pairs, pairs * 2, proto, emit);
}

// Proto-aware scalar emitters for use inside a lazy stream (the ones whose wire
// shape differs between protocols; bulk/simple/int are identical, use resp.d).
void emitNull(ref ByteBuffer o, int proto) @nogc nothrow
{
    o.append(proto >= 3 ? "_\r\n" : "$-1\r\n");
}

void emitBool(ref ByteBuffer o, bool v, int proto) @nogc nothrow
{
    if (proto >= 3)
        o.append(v ? "#t\r\n" : "#f\r\n");
    else
        o.append(v ? ":1\r\n" : ":0\r\n");
}

void emitDouble(ref ByteBuffer o, double v, int proto) @nogc nothrow
{
    char[40] b = void;
    immutable n = snprintf(b.ptr, b.length, "%.17g", v);
    if (proto >= 3)
    {
        o.appendByte(',');
        o.append(b[0 .. n]);
        o.append("\r\n");
    }
    else
        bulkBytes(o, b[0 .. n]);
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
    assert(enc(ra, 3) == "*2\r\n:1\r\n$1\r\nx\r\n");

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
