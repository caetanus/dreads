module dreads.cjson;

// JSON for Lua scripts (cjson.encode / cjson.decode / cjson.null), written
// in-project against the Lua C API — the library is small, so it is ours
// instead of a vendored lua-cjson. Semantics follow lua-cjson: arrays are
// tables with integer keys 1..n, null decodes to the cjson.null lightuserdata
// sentinel, NaN/Inf refuse to encode, nesting is depth-capped.

import core.stdc.stdio : snprintf;
import core.stdc.stdlib : strtod;

import dreads.lua;
import dreads.mem : ByteBuffer;

// lua-cjson runtime configuration (the Redis suite flips these)
// Default nesting limit. Lowered from 1000: the decoder/encoder recurse one
// native stack frame per level, and scripts run on a small vibe FIBER stack
// (gLuaPool.runTaskH), which overflows (real SIGSEGV) after a few hundred
// frames — a crash DoS reachable from a plain cjson.decode of deep JSON, no
// config change needed. 64 is well beyond any real document and safe on the
// fiber stack with wide margin.
private __gshared long gEncodeMaxDepth = 64;
private __gshared long gDecodeMaxDepth = 64;
private __gshared bool gEncodeInvalidNumbers = false;

public void registerCjson(lua_State* L) nothrow @nogc
{
    lua_createtable(L, 0, 3);
    lua_pushcclosure(L, &jsonEncode, 0);
    lua_setfield(L, -2, "encode");
    lua_pushcclosure(L, &jsonDecode, 0);
    lua_setfield(L, -2, "decode");
    lua_pushlightuserdata(L, null); // cjson.null sentinel
    lua_setfield(L, -2, "null");
    // lua-cjson runtime configuration: depth limits and invalid-number
    // tolerance really apply; the rest are accepted no-ops
    lua_pushcclosure(L, &jsonEncodeMaxDepth, 0);
    lua_setfield(L, -2, "encode_max_depth");
    lua_pushcclosure(L, &jsonDecodeMaxDepth, 0);
    lua_setfield(L, -2, "decode_max_depth");
    lua_pushcclosure(L, &jsonEncodeInvalidNumbers, 0);
    lua_setfield(L, -2, "encode_invalid_numbers");
    foreach (nm; ["encode_keep_buffer\0", "decode_invalid_numbers\0",
            "encode_sparse_array\0", "encode_number_precision\0"])
    {
        lua_pushcclosure(L, &jsonConfigStub, 0);
        lua_setfield(L, -2, nm.ptr);
    }
    lua_setglobal(L, "cjson");
}

/// Config calls with no effect on this implementation; echo the args.
extern (C) private int jsonConfigStub(lua_State* L) nothrow @nogc
{
    return lua_gettop(L);
}

// Hard ceiling on the configurable JSON nesting depth. encode/decode recurse
// one native (D/C) stack frame per level, so an unclamped depth lets a script
// set a huge limit and blow the worker-thread stack with a deeply-nested input
// (e.g. cjson.decode(string.rep("[", 2e6))) — a crash DoS. This bounds it.
private enum JSON_MAX_DEPTH_CEIL = 64;

extern (C) private int jsonEncodeMaxDepth(lua_State* L) nothrow @nogc
{
    int isnum;
    auto v = lua_tointegerx(L, 1, &isnum);
    if (isnum == 0 || v < 1)
        return luaL_error(L, "expected positive integer");
    if (v > JSON_MAX_DEPTH_CEIL)
        v = JSON_MAX_DEPTH_CEIL; // clamp: never honor a stack-overflowing depth
    gEncodeMaxDepth = v;
    lua_pushinteger(L, v);
    return 1;
}

extern (C) private int jsonDecodeMaxDepth(lua_State* L) nothrow @nogc
{
    int isnum;
    auto v = lua_tointegerx(L, 1, &isnum);
    if (isnum == 0 || v < 1)
        return luaL_error(L, "expected positive integer");
    if (v > JSON_MAX_DEPTH_CEIL)
        v = JSON_MAX_DEPTH_CEIL; // clamp: never honor a stack-overflowing depth
    gDecodeMaxDepth = v;
    lua_pushinteger(L, v);
    return 1;
}

extern (C) private int jsonEncodeInvalidNumbers(lua_State* L) nothrow @nogc
{
    gEncodeInvalidNumbers = lua_toboolean(L, 1) != 0;
    lua_pushboolean(L, gEncodeInvalidNumbers ? 1 : 0);
    return 1;
}

// ---------------------------------------------------------------------------
// encode
// ---------------------------------------------------------------------------

private void escapeInto(ref ByteBuffer o, scope const(char)[] s) nothrow @nogc
{
    o.appendByte('"');
    foreach (c; s)
    {
        switch (c)
        {
        case '"':
            o.append(`\"`);
            break;
        case '\\':
            o.append(`\\`);
            break;
        case '\n':
            o.append(`\n`);
            break;
        case '\r':
            o.append(`\r`);
            break;
        case '\t':
            o.append(`\t`);
            break;
        case '\b':
            o.append(`\b`);
            break;
        case '\f':
            o.append(`\f`);
            break;
        default:
            if (cast(ubyte) c < 0x20)
            {
                char[8] b = void;
                auto n = snprintf(b.ptr, b.length, `\u%04x`, cast(uint) c);
                o.append(b[0 .. n]);
            }
            else
                o.appendByte(c);
        }
    }
    o.appendByte('"');
}

/// Encodes the value at absolute index idx. Errors longjmp via luaL_error.
private void encodeValue(lua_State* L, int idx, ref ByteBuffer o, int depth) nothrow @nogc
{
    if (depth > gEncodeMaxDepth)
        luaL_error(L, "Cannot serialise: excessive nesting");
    switch (lua_type(L, idx))
    {
    case LUA_TNIL:
        o.append("null");
        break;
    case LUA_TBOOLEAN:
        o.append(lua_toboolean(L, idx) ? "true" : "false");
        break;
    case LUA_TLIGHTUSERDATA:
        if (lua_touserdata(L, idx) is null)
        {
            o.append("null"); // cjson.null
            break;
        }
        luaL_error(L, "Cannot serialise userdata");
        break;
    case LUA_TNUMBER:
        {
            char[40] b = void;
            int n;
            if (lua_isinteger(L, idx))
            {
                int isnum;
                n = snprintf(b.ptr, b.length, "%lld", lua_tointegerx(L, idx, &isnum));
            }
            else
            {
                int isnum;
                auto d = lua_tonumberx(L, idx, &isnum);
                if (d != d || d == double.infinity || d == -double.infinity)
                {
                    if (!gEncodeInvalidNumbers)
                        luaL_error(L,
                                "Cannot serialise number: must not be NaN or Infinity");
                    n = snprintf(b.ptr, b.length, d != d ? "nan"
                            : (d > 0 ? "inf" : "-inf"));
                }
                else
                    n = snprintf(b.ptr, b.length, "%.14g", d);
            }
            o.append(b[0 .. n]);
            break;
        }
    case LUA_TSTRING:
        {
            size_t len;
            auto p = lua_tolstring(L, idx, &len);
            escapeInto(o, p[0 .. len]);
            break;
        }
    case LUA_TTABLE:
        encodeTable(L, idx, o, depth);
        break;
    default:
        luaL_error(L, "Cannot serialise this Lua type");
    }
}

private void encodeTable(lua_State* L, int idx, ref ByteBuffer o, int depth) nothrow @nogc
{
    // array iff every key is an integer in 1..rawlen and the count matches
    auto n = cast(long) lua_rawlen(L, idx);
    long count = 0;
    bool allInt = true;
    lua_pushnil(L);
    while (lua_next(L, idx) != 0)
    {
        count++;
        if (allInt && lua_isinteger(L, -2))
        {
            int isnum;
            auto k = lua_tointegerx(L, -2, &isnum);
            if (k < 1 || k > n)
                allInt = false;
        }
        else
            allInt = false;
        lua_settop(L, lua_gettop(L) - 1); // pop value, keep key for lua_next
    }

    if (count == 0)
    {
        o.append("{}");
        return;
    }
    if (allInt && count == n)
    {
        o.appendByte('[');
        foreach (i; 1 .. n + 1)
        {
            if (i > 1)
                o.appendByte(',');
            lua_rawgeti(L, idx, i);
            encodeValue(L, lua_gettop(L), o, depth + 1);
            lua_settop(L, lua_gettop(L) - 1);
        }
        o.appendByte(']');
        return;
    }
    // object: string keys, or number keys converted to strings
    o.appendByte('{');
    bool first = true;
    lua_pushnil(L);
    while (lua_next(L, idx) != 0)
    {
        auto kt = lua_type(L, -2);
        if (kt != LUA_TSTRING && kt != LUA_TNUMBER)
            luaL_error(L, "Cannot serialise table key of this type");
        if (!first)
            o.appendByte(',');
        first = false;
        // copy the key before tolstring: converting in place breaks lua_next
        lua_pushvalue(L, -2);
        size_t klen;
        auto kp = lua_tolstring(L, -1, &klen);
        escapeInto(o, kp[0 .. klen]);
        lua_settop(L, lua_gettop(L) - 1);
        o.appendByte(':');
        encodeValue(L, lua_gettop(L), o, depth + 1);
        lua_settop(L, lua_gettop(L) - 1); // pop value, keep key
    }
    o.appendByte('}');
}

extern (C) private int jsonEncode(lua_State* L) nothrow @nogc
{
    if (lua_gettop(L) != 1)
        return luaL_error(L, "cjson.encode expects exactly one argument");
    static ByteBuffer buf; // TLS; single-threaded event loop
    buf.clear();
    encodeValue(L, 1, buf, 0);
    lua_pushlstring(L, cast(const(char)*) buf.data.ptr, buf.length);
    return 1;
}

// ---------------------------------------------------------------------------
// decode
// ---------------------------------------------------------------------------

private struct Parser
{
    lua_State* L;
    const(char)[] s;
    size_t i;

    void fail() nothrow @nogc
    {
        luaL_error(L, "Expected value but found invalid token at character %d",
                cast(int)(i + 1));
    }

    void skipWs() nothrow @nogc
    {
        while (i < s.length && (s[i] == ' ' || s[i] == '\t' || s[i] == '\n' || s[i] == '\r'))
            i++;
    }

    bool lit(scope const(char)[] word) nothrow @nogc
    {
        if (i + word.length > s.length || s[i .. i + word.length] != word)
            return false;
        i += word.length;
        return true;
    }

    /// Parses one value and leaves it on the Lua stack.
    void value(int depth) nothrow @nogc
    {
        if (depth > gDecodeMaxDepth)
            luaL_error(L, "Found too many nested data structures");
        skipWs();
        if (i >= s.length)
            fail();
        switch (s[i])
        {
        case 'n':
            if (!lit("null"))
                fail();
            lua_pushlightuserdata(L, null); // cjson.null
            break;
        case 't':
            if (!lit("true"))
                fail();
            lua_pushboolean(L, 1);
            break;
        case 'f':
            if (!lit("false"))
                fail();
            lua_pushboolean(L, 0);
            break;
        case '"':
            str();
            break;
        case '[':
            {
                i++;
                lua_createtable(L, 4, 0);
                skipWs();
                if (i < s.length && s[i] == ']')
                {
                    i++;
                    break;
                }
                long n = 0;
                for (;;)
                {
                    value(depth + 1);
                    lua_rawseti(L, -2, ++n);
                    skipWs();
                    if (i >= s.length)
                        fail();
                    if (s[i] == ',')
                    {
                        i++;
                        continue;
                    }
                    if (s[i] == ']')
                    {
                        i++;
                        break;
                    }
                    fail();
                }
                break;
            }
        case '{':
            {
                i++;
                lua_createtable(L, 0, 4);
                skipWs();
                if (i < s.length && s[i] == '}')
                {
                    i++;
                    break;
                }
                for (;;)
                {
                    skipWs();
                    if (i >= s.length || s[i] != '"')
                        fail();
                    str(); // key
                    skipWs();
                    if (i >= s.length || s[i] != ':')
                        fail();
                    i++;
                    value(depth + 1);
                    lua_rawset(L, -3);
                    skipWs();
                    if (i >= s.length)
                        fail();
                    if (s[i] == ',')
                    {
                        i++;
                        continue;
                    }
                    if (s[i] == '}')
                    {
                        i++;
                        break;
                    }
                    fail();
                }
                break;
            }
        default:
            number();
        }
    }

    void number() nothrow @nogc
    {
        auto start = i;
        bool isInt = true;
        if (i < s.length && s[i] == '-')
            i++;
        while (i < s.length && s[i] >= '0' && s[i] <= '9')
            i++;
        if (i < s.length && (s[i] == '.' || s[i] == 'e' || s[i] == 'E'))
        {
            isInt = false;
            while (i < s.length && (s[i] == '.' || s[i] == 'e' || s[i] == 'E'
                    || s[i] == '+' || s[i] == '-' || (s[i] >= '0' && s[i] <= '9')))
                i++;
        }
        if (i == start || (i == start + 1 && s[start] == '-'))
            fail();
        char[48] b = void;
        auto len = i - start;
        if (len >= b.length)
            fail();
        b[0 .. len] = s[start .. i];
        b[len] = 0;
        char* endp;
        auto d = strtod(b.ptr, &endp);
        if (endp !is b.ptr + len)
            fail();
        // integer BY VALUE, not just by syntax: Redis's Lua 5.1 prints whole
        // doubles as "0"/"-5000" (%.14g); on 5.4 only an integer does that
        if ((isInt || d == cast(double) cast(long) d)
                && d >= -9.007199254740992e15 && d <= 9.007199254740992e15)
            lua_pushinteger(L, cast(long) d);
        else
            lua_pushnumber(L, d);
    }

    private uint hex4() nothrow @nogc
    {
        if (i + 4 > s.length)
            fail();
        uint v = 0;
        foreach (_; 0 .. 4)
        {
            auto c = s[i++];
            v <<= 4;
            if (c >= '0' && c <= '9')
                v |= c - '0';
            else if (c >= 'a' && c <= 'f')
                v |= c - 'a' + 10;
            else if (c >= 'A' && c <= 'F')
                v |= c - 'A' + 10;
            else
                fail();
        }
        return v;
    }

    private void utf8(ref ByteBuffer o, uint cp) nothrow @nogc
    {
        if (cp < 0x80)
            o.appendByte(cast(ubyte) cp);
        else if (cp < 0x800)
        {
            o.appendByte(cast(ubyte)(0xC0 | (cp >> 6)));
            o.appendByte(cast(ubyte)(0x80 | (cp & 0x3F)));
        }
        else if (cp < 0x10000)
        {
            o.appendByte(cast(ubyte)(0xE0 | (cp >> 12)));
            o.appendByte(cast(ubyte)(0x80 | ((cp >> 6) & 0x3F)));
            o.appendByte(cast(ubyte)(0x80 | (cp & 0x3F)));
        }
        else
        {
            o.appendByte(cast(ubyte)(0xF0 | (cp >> 18)));
            o.appendByte(cast(ubyte)(0x80 | ((cp >> 12) & 0x3F)));
            o.appendByte(cast(ubyte)(0x80 | ((cp >> 6) & 0x3F)));
            o.appendByte(cast(ubyte)(0x80 | (cp & 0x3F)));
        }
    }

    void str() nothrow @nogc
    {
        // TLS scratch: safe because a string always finishes parsing before
        // any sibling string starts
        static ByteBuffer sb;
        i++; // opening quote
        auto chunkStart = i;
        sb.clear();
        for (;;)
        {
            if (i >= s.length)
                fail();
            auto c = s[i];
            if (c == '"')
            {
                sb.append(s[chunkStart .. i]);
                i++;
                break;
            }
            if (c != '\\')
            {
                i++;
                continue;
            }
            sb.append(s[chunkStart .. i]);
            i++;
            if (i >= s.length)
                fail();
            switch (s[i])
            {
            case '"':
                sb.appendByte('"');
                i++;
                break;
            case '\\':
                sb.appendByte('\\');
                i++;
                break;
            case '/':
                sb.appendByte('/');
                i++;
                break;
            case 'b':
                sb.appendByte('\b');
                i++;
                break;
            case 'f':
                sb.appendByte('\f');
                i++;
                break;
            case 'n':
                sb.appendByte('\n');
                i++;
                break;
            case 'r':
                sb.appendByte('\r');
                i++;
                break;
            case 't':
                sb.appendByte('\t');
                i++;
                break;
            case 'u':
                {
                    i++;
                    auto cp = hex4();
                    if (cp >= 0xD800 && cp <= 0xDBFF) // surrogate pair
                    {
                        if (i + 1 < s.length && s[i] == '\\' && s[i + 1] == 'u')
                        {
                            i += 2;
                            auto lo = hex4();
                            if (lo >= 0xDC00 && lo <= 0xDFFF)
                                cp = 0x10000 + ((cp - 0xD800) << 10) + (lo - 0xDC00);
                            else
                                fail();
                        }
                        else
                            fail();
                    }
                    utf8(sb, cp);
                    break;
                }
            default:
                fail();
            }
            chunkStart = i;
        }
        lua_pushlstring(L, cast(const(char)*) sb.data.ptr, sb.length);
    }
}

extern (C) private int jsonDecode(lua_State* L) nothrow @nogc
{
    size_t len;
    auto p = lua_tolstring(L, 1, &len);
    if (p is null)
        return luaL_error(L, "cjson.decode expects a string");
    Parser ps;
    ps.L = L;
    ps.s = p[0 .. len];
    ps.value(0);
    ps.skipWs();
    if (ps.i != len)
        return luaL_error(L, "Expected the end but found invalid token at character %d",
                cast(int)(ps.i + 1));
    return 1;
}
