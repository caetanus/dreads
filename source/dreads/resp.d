module dreads.resp;

// RESP2 parser + encoder, fully @nogc.
// The parser is zero-copy: RVal strings are slices into the input buffer,
// valid until the buffer is consumed. Arrays are allocated from an Arena.
// Incomplete input is a status, not an error — the caller waits for more bytes.

import core.stdc.stdio : snprintf;

import dreads.mem;

public enum RType : ubyte
{
    Null,
    SimpleString,
    Error,
    Integer,
    BulkString,
    Array
}

public struct RVal
{
    RType type;
    union
    {
        long integer;
        const(char)[] str;
        RVal[] arr;
    }
}

public enum ParseStatus : ubyte
{
    ok,
    incomplete,
    protocolError
}

// Redis's own limits: 512MB per bulk string, 1M elements per multibulk.
private enum MAX_BULK = 512L * 1024 * 1024;
private enum MAX_ARRAY = 1024L * 1024;

private ptrdiff_t findCRLF(scope const(ubyte)[] buf, size_t from) @nogc nothrow
{
    if (buf.length < 2 || from + 1 >= buf.length)
        return -1;
    foreach (i; from .. buf.length - 1)
    {
        if (buf[i] == '\r' && buf[i + 1] == '\n')
            return i;
    }
    return -1;
}

private ParseStatus parseLineInt(scope const(ubyte)[] buf, ref size_t pos, out long value) @nogc nothrow
{
    auto end = findCRLF(buf, pos);
    if (end < 0)
        return ParseStatus.incomplete;
    size_t i = pos;
    bool neg = false;
    if (i < end && (buf[i] == '-' || buf[i] == '+'))
    {
        neg = buf[i] == '-';
        i++;
    }
    if (i == cast(size_t) end)
        return ParseStatus.protocolError;
    long v = 0;
    for (; i < cast(size_t) end; i++)
    {
        ubyte c = buf[i];
        if (c < '0' || c > '9')
            return ParseStatus.protocolError;
        // far above any valid length header; prevents silent wrap-around
        if (v > 100_000_000_000_000_000)
            return ParseStatus.protocolError;
        v = v * 10 + (c - '0');
    }
    value = neg ? -v : v;
    pos = end + 2;
    return ParseStatus.ok;
}

/// Parses one RESP value starting at buf[pos]. On ok, advances pos past it.
/// On incomplete/protocolError, pos is left untouched.
public ParseStatus parseValue(scope const(ubyte)[] buf, ref size_t pos,
        ref Arena arena, out RVal val) @nogc nothrow
{
    if (pos >= buf.length)
        return ParseStatus.incomplete;

    char t = cast(char) buf[pos];
    switch (t)
    {
    case '+':
    case '-':
        {
            auto end = findCRLF(buf, pos + 1);
            if (end < 0)
                return ParseStatus.incomplete;
            val.type = t == '+' ? RType.SimpleString : RType.Error;
            val.str = cast(const(char)[]) buf[pos + 1 .. end];
            pos = end + 2;
            return ParseStatus.ok;
        }
    case ':':
        {
            size_t p = pos + 1;
            long v;
            auto st = parseLineInt(buf, p, v);
            if (st != ParseStatus.ok)
                return st;
            val.type = RType.Integer;
            val.integer = v;
            pos = p;
            return ParseStatus.ok;
        }
    case '$':
        {
            size_t p = pos + 1;
            long n;
            auto st = parseLineInt(buf, p, n);
            if (st != ParseStatus.ok)
                return st;
            if (n == -1)
            {
                val.type = RType.Null;
                pos = p;
                return ParseStatus.ok;
            }
            if (n < 0 || n > MAX_BULK)
                return ParseStatus.protocolError;
            if (p + n + 2 > buf.length)
                return ParseStatus.incomplete;
            if (buf[p + n] != '\r' || buf[p + n + 1] != '\n')
                return ParseStatus.protocolError;
            val.type = RType.BulkString;
            val.str = cast(const(char)[]) buf[p .. p + n];
            pos = p + n + 2;
            return ParseStatus.ok;
        }
    case '*':
        {
            size_t p = pos + 1;
            long n;
            auto st = parseLineInt(buf, p, n);
            if (st != ParseStatus.ok)
                return st;
            if (n == -1)
            {
                val.type = RType.Null;
                pos = p;
                return ParseStatus.ok;
            }
            if (n < 0 || n > MAX_ARRAY)
                return ParseStatus.protocolError;
            auto items = arena.allocArray!RVal(cast(size_t) n);
            foreach (i; 0 .. cast(size_t) n)
            {
                st = parseValue(buf, p, arena, items[i]);
                if (st != ParseStatus.ok)
                    return st;
            }
            val.type = RType.Array;
            val.arr = items;
            pos = p;
            return ParseStatus.ok;
        }
    default:
        return parseInline(buf, pos, arena, val);
    }
}

// Redis inline commands: a request line that does not start with a RESP marker
// is a plain, whitespace-separated command (e.g. "PING\r\n", "SET k v\r\n").
// Terminated by \n; a preceding \r is trimmed. Zero-copy — each token is a slice
// into the buffer. Capped like Redis to reject an unbounded line with no newline.
private enum MAX_INLINE = 64 * 1024;

private ParseStatus parseInline(scope const(ubyte)[] buf, ref size_t pos,
        ref Arena arena, out RVal val) @nogc nothrow
{
    size_t nl = pos;
    while (nl < buf.length && buf[nl] != '\n')
        nl++;
    if (nl >= buf.length) // no line terminator yet
        return buf.length - pos > MAX_INLINE ? ParseStatus.protocolError : ParseStatus.incomplete;
    size_t stop = nl;
    if (stop > pos && buf[stop - 1] == '\r')
        stop--; // trim the CR of CRLF

    static bool isSpace(ubyte c) @nogc nothrow
    {
        return c == ' ' || c == '\t';
    }

    size_t count = 0, i = pos;
    while (i < stop) // count whitespace-separated tokens
    {
        while (i < stop && isSpace(buf[i]))
            i++;
        if (i >= stop)
            break;
        count++;
        while (i < stop && !isSpace(buf[i]))
            i++;
    }

    auto items = arena.allocArray!RVal(count);
    size_t idx = 0;
    i = pos;
    while (i < stop && idx < count)
    {
        while (i < stop && isSpace(buf[i]))
            i++;
        if (i >= stop)
            break;
        immutable s = i;
        while (i < stop && !isSpace(buf[i]))
            i++;
        items[idx].type = RType.BulkString;
        items[idx].str = cast(const(char)[]) buf[s .. i];
        idx++;
    }
    val.type = RType.Array;
    val.arr = items;
    pos = nl + 1; // consume through the newline
    return ParseStatus.ok;
}

// ---------------------------------------------------------------------------
// Encoder: replies are appended to a ByteBuffer.
// ---------------------------------------------------------------------------

private void appendInt(ref ByteBuffer o, long v) @nogc nothrow
{
    char[24] tmp = void;
    auto n = snprintf(tmp.ptr, tmp.length, "%lld", v);
    o.append(tmp[0 .. n]);
}

public void repSimple(ref ByteBuffer o, scope const(char)[] s) @nogc nothrow
{
    o.appendByte('+');
    o.append(s);
    o.append("\r\n");
}

public void repError(ref ByteBuffer o, scope const(char)[] s) @nogc nothrow
{
    o.appendByte('-');
    o.append(s);
    o.append("\r\n");
}

public void repInt(ref ByteBuffer o, long v) @nogc nothrow
{
    o.appendByte(':');
    appendInt(o, v);
    o.append("\r\n");
}

public void repBulk(ref ByteBuffer o, scope const(char)[] s) @nogc nothrow
{
    o.appendByte('$');
    appendInt(o, cast(long) s.length);
    o.append("\r\n");
    o.append(s);
    o.append("\r\n");
}

// Negotiated protocol version of the connection currently being served. The
// event loop is single-threaded, so a global set once per command dispatch is
// race-free (same pattern as the deferred keyspace-notify queue). 2 = RESP2
// (default), 3 = RESP3. Reply builders whose *shape* differs between versions
// consult this; pub/sub delivery to OTHER connections cannot use it (it frames
// per-subscriber — see repPushHeader / connSink).
public __gshared int gRespProto = 2;

public void repNullBulk(ref ByteBuffer o) @nogc nothrow
{
    o.append(gRespProto >= 3 ? "_\r\n" : "$-1\r\n");
}

/// Null in an array position. RESP2 uses the null *array* `*-1`; RESP3 unifies
/// all nulls to `_`.
public void repNullArray(ref ByteBuffer o) @nogc nothrow
{
    o.append(gRespProto >= 3 ? "_\r\n" : "*-1\r\n");
}

/// Map header: N field/value pairs follow (2N elements). RESP3 `%N`; RESP2
/// degrades to a flat array of 2N elements `*2N`.
public void repMapHeader(ref ByteBuffer o, size_t pairs) @nogc nothrow
{
    o.appendByte(gRespProto >= 3 ? '%' : '*');
    appendInt(o, cast(long)(gRespProto >= 3 ? pairs : pairs * 2));
    o.append("\r\n");
}

/// Set header: N unordered elements. RESP3 `~N`; RESP2 degrades to array `*N`.
public void repSetHeader(ref ByteBuffer o, size_t n) @nogc nothrow
{
    o.appendByte(gRespProto >= 3 ? '~' : '*');
    appendInt(o, cast(long) n);
    o.append("\r\n");
}

/// Push header: out-of-band message (pub/sub, invalidation). RESP3 `>N`; RESP2
/// degrades to array `*N`. NOTE: this consults the global proto, so it is only
/// correct for the connection being served synchronously — pub/sub fan-out to
/// other connections frames per-subscriber in connSink, not here.
public void repPushHeader(ref ByteBuffer o, size_t n) @nogc nothrow
{
    o.appendByte(gRespProto >= 3 ? '>' : '*');
    appendInt(o, cast(long) n);
    o.append("\r\n");
}

/// Boolean. RESP3 `#t`/`#f`; RESP2 degrades to integer `:1`/`:0`.
public void repBool(ref ByteBuffer o, bool b) @nogc nothrow
{
    if (gRespProto >= 3)
        o.append(b ? "#t\r\n" : "#f\r\n");
    else
        o.append(b ? ":1\r\n" : ":0\r\n");
}

/// Big number (arbitrary-precision integer as decimal text). RESP3 `(digits`;
/// RESP2 degrades to a bulk string. `s` must be a valid signed decimal.
public void repBigNumber(ref ByteBuffer o, scope const(char)[] s) @nogc nothrow
{
    if (gRespProto >= 3)
    {
        o.appendByte('(');
        o.append(s);
        o.append("\r\n");
    }
    else
        repBulk(o, s);
}

/// Verbatim string with a 3-char format hint (e.g. "txt", "mkd"). RESP3
/// `=len\r\nfmt:payload`; RESP2 degrades to a plain bulk string of the payload.
public void repVerbatim(ref ByteBuffer o, scope const(char)[] fmt3, scope const(char)[] s) @nogc nothrow
{
    if (gRespProto >= 3)
    {
        o.appendByte('=');
        appendInt(o, cast(long)(s.length + 4)); // "fmt:" + payload
        o.append("\r\n");
        o.append(fmt3);
        o.appendByte(':');
        o.append(s);
        o.append("\r\n");
    }
    else
        repBulk(o, s);
}

/// Valkey's canonical container-command error: names the offending subcommand
/// and points at HELP. `parent` is the UPPERCASE command name (e.g. "CLIENT"),
/// `sub` is the subcommand token as the client typed it (truncated to 128, as
/// Valkey does with %.128s). See THIRD_PARTY_NOTICES.md.
public void repUnknownSubcommand(ref ByteBuffer o, scope const(char)[] parent,
        scope const(char)[] sub) @nogc nothrow
{
    o.append("-ERR unknown subcommand or wrong number of arguments for '");
    o.append(sub.length > 128 ? sub[0 .. 128] : sub);
    o.append("'. Try ");
    o.append(parent);
    o.append(" HELP.\r\n");
}

public void repArrayHeader(ref ByteBuffer o, size_t n) @nogc nothrow
{
    o.appendByte('*');
    appendInt(o, cast(long) n);
    o.append("\r\n");
}

/// Generic encoder (round-trips parseValue output).
public void encode(ref ByteBuffer o, const ref RVal v) @nogc nothrow
{
    final switch (v.type)
    {
    case RType.Null:
        repNullBulk(o);
        break;
    case RType.SimpleString:
        repSimple(o, v.str);
        break;
    case RType.Error:
        repError(o, v.str);
        break;
    case RType.Integer:
        repInt(o, v.integer);
        break;
    case RType.BulkString:
        repBulk(o, v.str);
        break;
    case RType.Array:
        repArrayHeader(o, v.arr.length);
        foreach (ref e; v.arr)
            encode(o, e);
        break;
    }
}

// ---------------------------------------------------------------------------
// Tests (GC is fine inside unittest blocks; the API under test is @nogc).
// ---------------------------------------------------------------------------

version (unittest)
{
    private RVal parseOne(string input, ref Arena arena, ParseStatus expected = ParseStatus.ok,
            size_t expectedConsumed = size_t.max)
    {
        RVal v;
        size_t pos = 0;
        auto st = parseValue(cast(const(ubyte)[]) input, pos, arena, v);
        assert(st == expected);
        if (expected == ParseStatus.ok)
            assert(pos == (expectedConsumed == size_t.max ? input.length : expectedConsumed));
        else
            assert(pos == 0);
        return v;
    }
}

unittest // scalars
{
    Arena a;
    auto v = parseOne("+OK\r\n", a);
    assert(v.type == RType.SimpleString && v.str == "OK");

    v = parseOne("-ERR something\r\n", a);
    assert(v.type == RType.Error && v.str == "ERR something");

    v = parseOne(":-12345\r\n", a);
    assert(v.type == RType.Integer && v.integer == -12_345);

    v = parseOne("$5\r\nhello\r\n", a);
    assert(v.type == RType.BulkString && v.str == "hello");

    v = parseOne("$0\r\n\r\n", a);
    assert(v.type == RType.BulkString && v.str == "");

    v = parseOne("$-1\r\n", a);
    assert(v.type == RType.Null);

    // bulk strings are binary-safe: CRLF and NUL inside are data
    v = parseOne("$12\r\nhello\r\nwor\0d\r\n", a);
    assert(v.type == RType.BulkString && v.str == "hello\r\nwor\0d");
}

unittest // arrays
{
    Arena a;
    auto v = parseOne("*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n", a);
    assert(v.type == RType.Array && v.arr.length == 2);
    assert(v.arr[0].str == "foo" && v.arr[1].str == "bar");

    v = parseOne("*0\r\n", a);
    assert(v.type == RType.Array && v.arr.length == 0);

    v = parseOne("*-1\r\n", a);
    assert(v.type == RType.Null);

    // nested with mixed types
    v = parseOne("*3\r\n:1\r\n*2\r\n+a\r\n-e\r\n$-1\r\n", a);
    assert(v.type == RType.Array && v.arr.length == 3);
    assert(v.arr[0].integer == 1);
    assert(v.arr[1].type == RType.Array && v.arr[1].arr[1].type == RType.Error);
    assert(v.arr[2].type == RType.Null);

    // deep nesting (the old "insane" case, with valid CRLF terminators)
    v = parseOne("*1\r\n*1\r\n*1\r\n*1\r\n*2\r\n$6\r\nhello\n\r\n:-1\r\n", a);
    foreach (_; 0 .. 4)
    {
        assert(v.type == RType.Array && v.arr.length == 1);
        v = v.arr[0];
    }
    assert(v.arr.length == 2 && v.arr[0].str == "hello\n" && v.arr[1].integer == -1);
}

unittest // inline commands (Redis inline protocol): non-'*' line -> whitespace split
{
    Arena a;
    auto v = parseOne("PING\r\n", a);
    assert(v.type == RType.Array && v.arr.length == 1 && v.arr[0].str == "PING");
    assert(v.arr[0].type == RType.BulkString);

    // args + runs of whitespace collapse
    v = parseOne("  SET   k   v  \r\n", a);
    assert(v.arr.length == 3 && v.arr[0].str == "SET" && v.arr[1].str == "k" && v.arr[2].str == "v");

    // \n-only terminator (no \r), and tab as a separator
    v = parseOne("PING\n", a);
    assert(v.arr.length == 1 && v.arr[0].str == "PING");
    v = parseOne("GET\tfoo\r\n", a);
    assert(v.arr.length == 2 && v.arr[0].str == "GET" && v.arr[1].str == "foo");

    // blank line -> empty array (the server loop skips it, like Redis)
    v = parseOne("\r\n", a);
    assert(v.type == RType.Array && v.arr.length == 0);

    // no terminator yet -> incomplete, not a protocol error
    parseOne("SET k v", a, ParseStatus.incomplete);
}

unittest // pipelined inline commands parse one at a time; inline + RESP interleave
{
    Arena a;
    RVal v;
    auto buf = cast(const(ubyte)[]) "MULTI\r\nSET k v\r\nINCR c\r\nEXEC\r\n";
    size_t pos = 0;
    foreach (w; ["MULTI", "SET", "INCR", "EXEC"])
    {
        assert(parseValue(buf, pos, a, v) == ParseStatus.ok);
        assert(v.type == RType.Array && v.arr[0].str == w);
    }
    assert(pos == buf.length);

    buf = cast(const(ubyte)[]) "PING\r\n*1\r\n$4\r\nPING\r\nPING\r\n";
    pos = 0;
    foreach (_; 0 .. 3)
        assert(parseValue(buf, pos, a, v) == ParseStatus.ok && v.arr[0].str == "PING");
    assert(pos == buf.length);
}

unittest // every proper prefix of a valid message is incomplete, never an error
{
    Arena a;
    string[] vectors = [
        "+OK\r\n", ":-42\r\n", "$5\r\nhello\r\n", "$-1\r\n",
        "*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n",
        "*2\r\n*1\r\n:1\r\n$4\r\nab\r\n\r\n",
    ];
    foreach (full; vectors)
    {
        foreach (cut; 0 .. full.length)
            parseOne(full[0 .. cut], a, ParseStatus.incomplete);
        a.reset();
    }
}

unittest // protocol errors
{
    Arena a;
    // a bare non-'*' token with no newline is now an incomplete INLINE command
    // (wait for the line terminator), not a protocol error
    parseOne("x", a, ParseStatus.incomplete);
    auto x = parseOne("x\r\n", a);
    assert(x.type == RType.Array && x.arr.length == 1 && x.arr[0].str == "x");
    parseOne("$abc\r\n", a, ParseStatus.protocolError);
    parseOne(":\r\n", a, ParseStatus.protocolError);
    parseOne(":12x\r\n", a, ParseStatus.protocolError);
    parseOne("$-2\r\n", a, ParseStatus.protocolError);
    // bulk payload not terminated by CRLF
    parseOne("$3\r\nfooxy", a, ParseStatus.protocolError);
}

unittest // pos advances past one value and leaves the rest (pipelining)
{
    Arena a;
    string two = "+OK\r\n:7\r\n";
    RVal v;
    size_t pos = 0;
    assert(parseValue(cast(const(ubyte)[]) two, pos, a, v) == ParseStatus.ok);
    assert(v.str == "OK" && pos == 5);
    assert(parseValue(cast(const(ubyte)[]) two, pos, a, v) == ParseStatus.ok);
    assert(v.integer == 7 && pos == two.length);
}

unittest // encode round-trip
{
    Arena a;
    string[] vectors = [
        "+OK\r\n", "-ERR bad\r\n", ":-42\r\n", ":0\r\n",
        "$5\r\nhello\r\n", "$0\r\n\r\n", "$-1\r\n",
        "*0\r\n", "*3\r\n:1\r\n*2\r\n+a\r\n-e\r\n$12\r\nhello\r\nwor\0d\r\n",
    ];
    foreach (input; vectors)
    {
        auto v = parseOne(input, a);
        ByteBuffer o;
        encode(o, v);
        assert(cast(string) o.data == input);
        a.reset();
    }
}

unittest // reply builders
{
    gRespProto = 2; // repNullBulk reads the global; pin it (parallel test runner)
    scope (exit)
        gRespProto = 2;
    ByteBuffer o;
    repSimple(o, "PONG");
    repInt(o, -3);
    repBulk(o, "hi");
    repNullBulk(o);
    repArrayHeader(o, 2);
    assert(cast(string) o.data == "+PONG\r\n:-3\r\n$2\r\nhi\r\n$-1\r\n*2\r\n");
}

unittest // RESP2 vs RESP3 shape-dependent encoders
{
    scope (exit)
        gRespProto = 2;

    // RESP2 degradations
    gRespProto = 2;
    {
        ByteBuffer o;
        repNullBulk(o);
        repNullArray(o);
        repMapHeader(o, 2); // 2 pairs -> flat *4
        repSetHeader(o, 3);
        repPushHeader(o, 3);
        repBool(o, true);
        repBool(o, false);
        repBigNumber(o, "123");
        repVerbatim(o, "txt", "hello");
        assert(cast(string) o.data ==
                "$-1\r\n" ~ "*-1\r\n" ~ "*4\r\n" ~ "*3\r\n" ~ "*3\r\n" ~
                ":1\r\n" ~ ":0\r\n" ~ "$3\r\n123\r\n" ~ "$5\r\nhello\r\n");
    }

    // RESP3 native types
    gRespProto = 3;
    {
        ByteBuffer o;
        repNullBulk(o);
        repNullArray(o);
        repMapHeader(o, 2);
        repSetHeader(o, 3);
        repPushHeader(o, 3);
        repBool(o, true);
        repBool(o, false);
        repBigNumber(o, "123");
        repVerbatim(o, "txt", "hello");
        assert(cast(string) o.data ==
                "_\r\n" ~ "_\r\n" ~ "%2\r\n" ~ "~3\r\n" ~ ">3\r\n" ~
                "#t\r\n" ~ "#f\r\n" ~ "(123\r\n" ~ "=9\r\ntxt:hello\r\n");
    }
}
