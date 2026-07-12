module dreads.respvariant;

// The RESP reply oracle (dreads domain), lazy by design: a reply is streamed
// straight from its source (a hash, a set, a range), never materialized into a
// tree. `lazyMap/lazySet/lazyArray/lazyPush` emit the proto-aware aggregate
// header (%/~/> in RESP3 vs * in RESP2) and then run a `scope` delegate that
// streams the children. The delegate never escapes, so it lives on the stack —
// the @nogc command path compiles, which proves no GC closure is allocated.
// Benchmark: within noise of hand-written direct emit (the materialized-tree
// approach it replaced ran ~1.2-1.5x and needed a node allocator; dropped).
//
// The protocol lives here and in the proto-aware scalar helpers below (plus the
// invariant ones in dreads.resp — bulk/int/simple are byte-identical across
// versions); command handlers declare *what* the reply is, never *how* it frames.

import core.stdc.stdio : snprintf;
import std.json : JSONValue;

import dreads.mem : ByteBuffer;

/// Streams an aggregate's children into `o` for the negotiated `proto`.
alias Emit = void delegate(ref ByteBuffer o, int proto) @nogc nothrow;

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

private void lazyAgg(ref ByteBuffer o, char resp3Byte, size_t count,
        size_t resp2Count, int proto, scope Emit emit) @nogc nothrow
{
    o.appendByte(proto >= 3 ? resp3Byte : '*');
    appendLong(o, cast(long)(proto >= 3 ? count : resp2Count));
    o.append("\r\n");
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

// Proto-aware scalar emitters for the leaf types whose wire shape differs
// between protocols (bulk/int/simple are identical — use dreads.resp).
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

// ---------------------------------------------------------------------------
// Structured reply value (debug / introspection). Unlike the lazy oracle above
// — which streams straight to the wire and is the production hot path — RValue
// materializes a reply as a plain GC-backed tree so it can be inspected. It can
// `encode()` to RESP (either protocol) AND `toJson()` for debugging ("what did
// the server build?"). GC is fine here: this is never on the @nogc data path,
// only in tools and tests. (This is why RVariant existed originally; the hot
// path is lazy, the debug view is this.)
// ---------------------------------------------------------------------------

struct RValue
{
    enum K
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
        array,
        set,
        push,
        map
    }

    K kind;
    string s; // simple/err/bulk/verbatim/bignum (verbatim: 3-char fmt then payload)
    long i;
    double d;
    bool b;
    RValue[] items; // array/set/push children
    RValue[] pairs; // map: flattened [k0, v0, k1, v1, ...]

    static RValue nul()
    {
        return RValue(K.nil);
    }

    static RValue simple(string x)
    {
        auto r = RValue(K.simple);
        r.s = x;
        return r;
    }

    static RValue err(string x)
    {
        auto r = RValue(K.err);
        r.s = x;
        return r;
    }

    static RValue bulk(string x)
    {
        auto r = RValue(K.bulk);
        r.s = x;
        return r;
    }

    static RValue num(long x)
    {
        auto r = RValue(K.num);
        r.i = x;
        return r;
    }

    static RValue dbl(double x)
    {
        auto r = RValue(K.dbl);
        r.d = x;
        return r;
    }

    static RValue boolean(bool x)
    {
        auto r = RValue(K.boolean);
        r.b = x;
        return r;
    }

    static RValue array(RValue[] xs)
    {
        auto r = RValue(K.array);
        r.items = xs;
        return r;
    }

    static RValue set(RValue[] xs)
    {
        auto r = RValue(K.set);
        r.items = xs;
        return r;
    }

    static RValue push(RValue[] xs)
    {
        auto r = RValue(K.push);
        r.items = xs;
        return r;
    }

    static RValue map(RValue[] flatPairs)
    {
        auto r = RValue(K.map);
        r.pairs = flatPairs;
        return r;
    }

    /// Encode to RESP in the given protocol (mirrors the lazy oracle's framing).
    void encode(ref ByteBuffer o, int proto) const
    {
        final switch (kind)
        {
        case K.nil:
            emitNull(o, proto);
            break;
        case K.simple:
            o.appendByte('+');
            o.append(s);
            o.append("\r\n");
            break;
        case K.err:
            o.appendByte('-');
            o.append(s);
            o.append("\r\n");
            break;
        case K.bulk:
        case K.verbatim:
        case K.bignum:
            bulkBytes(o, s);
            break;
        case K.num:
            o.appendByte(':');
            appendLong(o, i);
            o.append("\r\n");
            break;
        case K.dbl:
            emitDouble(o, d, proto);
            break;
        case K.boolean:
            emitBool(o, b, proto);
            break;
        case K.array:
        case K.set:
        case K.push:
            o.appendByte(proto >= 3 ? (kind == K.set ? '~' : (kind == K.push ? '>' : '*')) : '*');
            appendLong(o, cast(long) items.length);
            o.append("\r\n");
            foreach (ref c; items)
                c.encode(o, proto);
            break;
        case K.map:
            o.appendByte(proto >= 3 ? '%' : '*');
            appendLong(o, cast(long)(proto >= 3 ? pairs.length / 2 : pairs.length));
            o.append("\r\n");
            foreach (ref c; pairs)
                c.encode(o, proto);
            break;
        }
    }

    /// A RESP-typed debug view (picked up by writeln/format): shows the wire
    /// type of each node, unlike toJson which is a plain value view.
    string toString() const
    {
        import std.format : format;
        import std.array : appender;

        auto a = appender!string();
        void walk(const ref RValue v)
        {
            final switch (v.kind)
            {
            case K.nil:
                a.put("(nil)");
                break;
            case K.simple:
                a.put(format!"+%s"(v.s));
                break;
            case K.err:
                a.put(format!"-%s"(v.s));
                break;
            case K.bulk:
                a.put(format!`"%s"`(v.s));
                break;
            case K.verbatim:
                a.put(format!"=%s"(v.s));
                break;
            case K.bignum:
                a.put(format!"(%s"(v.s));
                break;
            case K.num:
                a.put(format!":%d"(v.i));
                break;
            case K.dbl:
                a.put(format!",%g"(v.d));
                break;
            case K.boolean:
                a.put(v.b ? "#t" : "#f");
                break;
            case K.array:
            case K.set:
            case K.push:
                a.put(v.kind == K.set ? "~[" : (v.kind == K.push ? ">[" : "*["));
                foreach (idx, ref c; v.items)
                {
                    if (idx)
                        a.put(", ");
                    walk(c);
                }
                a.put(']');
                break;
            case K.map:
                a.put("%{");
                for (size_t k = 0; k + 1 < v.pairs.length; k += 2)
                {
                    if (k)
                        a.put(", ");
                    walk(v.pairs[k]);
                    a.put(": ");
                    walk(v.pairs[k + 1]);
                }
                a.put('}');
                break;
            }
        }

        walk(this);
        return a.data;
    }

    /// JSON for debugging — "what reply did we build?". Returns a JSONValue
    /// (call `.toString`/`.toPrettyString` for text).
    JSONValue toJson() const
    {
        final switch (kind)
        {
        case K.nil:
            return JSONValue(null);
        case K.simple:
        case K.err:
        case K.bulk:
        case K.verbatim:
        case K.bignum:
            return JSONValue(s);
        case K.num:
            return JSONValue(i);
        case K.dbl:
            return JSONValue(d);
        case K.boolean:
            return JSONValue(b);
        case K.array:
        case K.set:
        case K.push:
            JSONValue[] arr;
            foreach (ref c; items)
                arr ~= c.toJson();
            return JSONValue(arr);
        case K.map:
            JSONValue[string] obj;
            for (size_t k = 0; k + 1 < pairs.length; k += 2)
                obj[pairs[k].s] = pairs[k + 1].toJson();
            return JSONValue(obj);
        }
    }
}

unittest // RValue: encode + JSON debug view
{
    auto reply = RValue.map([
        RValue.bulk("name"), RValue.bulk("dreads"),
        RValue.bulk("scores"), RValue.array([RValue.dbl(1.5), RValue.num(2)]),
        RValue.bulk("live"), RValue.boolean(true),
    ]);
    auto j = reply.toJson; // a JSONValue (order-independent field checks)
    assert(j["name"].str == "dreads");
    assert(j["scores"].array.length == 2 && j["scores"][0].floating == 1.5 && j["scores"][1].integer == 2);
    assert(j["live"].boolean == true);
    assert(reply.toString == `%{"name": "dreads", "scores": *[,1.5, :2], "live": #t}`);

    ByteBuffer o3;
    reply.encode(o3, 3);
    // %3 map, native double, native bool
    assert((cast(char[]) o3.data).idup ==
            "%3\r\n$4\r\nname\r\n$6\r\ndreads\r\n$6\r\nscores\r\n*2\r\n,1.5\r\n:2\r\n$4\r\nlive\r\n#t\r\n");
}

version (unittest) private string enc(scope void delegate(ref ByteBuffer) @nogc nothrow build)
{
    ByteBuffer o;
    build(o);
    return (cast(char[]) o.data).idup;
}

unittest // lazy aggregates degrade RESP3 -> RESP2 correctly
{
    import dreads.resp : repBulk, repInt;

    // map: %1 vs flat *2
    assert(enc((ref o) => lazyMap(o, 3, 1, (ref oo, p) {
                repBulk(oo, "k");
                repBulk(oo, "v");
            })) == "%1\r\n$1\r\nk\r\n$1\r\nv\r\n");
    assert(enc((ref o) => lazyMap(o, 2, 1, (ref oo, p) {
                repBulk(oo, "k");
                repBulk(oo, "v");
            })) == "*2\r\n$1\r\nk\r\n$1\r\nv\r\n");

    // set: ~2 vs *2
    assert(enc((ref o) => lazySet(o, 3, 2, (ref oo, p) {
                repInt(oo, 1);
                repInt(oo, 2);
            })) == "~2\r\n:1\r\n:2\r\n");
    assert(enc((ref o) => lazySet(o, 2, 2, (ref oo, p) {
                repInt(oo, 1);
                repInt(oo, 2);
            })) == "*2\r\n:1\r\n:2\r\n");

    // push: >1 vs *1
    assert(enc((ref o) => lazyPush(o, 3, 1, (ref oo, p) { repBulk(oo, "m"); })) == ">1\r\n$1\r\nm\r\n");
}

unittest // proto-aware scalar leaves
{
    assert(enc((ref o) => emitNull(o, 3)) == "_\r\n" && enc((ref o) => emitNull(o, 2)) == "$-1\r\n");
    assert(enc((ref o) => emitBool(o, true, 3)) == "#t\r\n" && enc((ref o) => emitBool(o, true, 2)) == ":1\r\n");
    assert(enc((ref o) => emitDouble(o, 1.5, 3)) == ",1.5\r\n" && enc((ref o) => emitDouble(o,
            1.5, 2)) == "$3\r\n1.5\r\n");
}
