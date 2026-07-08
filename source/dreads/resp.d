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
        return ParseStatus.protocolError;
    }
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

public void repNullBulk(ref ByteBuffer o) @nogc nothrow
{
    o.append("$-1\r\n");
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
    parseOne("x", a, ParseStatus.protocolError);
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
    ByteBuffer o;
    repSimple(o, "PONG");
    repInt(o, -3);
    repBulk(o, "hi");
    repNullBulk(o);
    repArrayHeader(o, 2);
    assert(cast(string) o.data == "+PONG\r\n:-3\r\n$2\r\nhi\r\n$-1\r\n*2\r\n");
}
