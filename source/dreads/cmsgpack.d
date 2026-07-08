module dreads.cmsgpack;

// MessagePack for Lua scripts (cmsgpack.pack / cmsgpack.unpack), in-project
// like cjson: the wire format is a page of spec, so it is ours instead of a
// GC-based dub dependency. Semantics follow lua-cmsgpack: tables with
// integer keys 1..n become arrays, everything else a map; pack() encodes
// each argument in sequence; unpack() returns every object in the blob as
// multiple values; unsupported Lua types encode as nil.

import dreads.lua;
import dreads.mem : ByteBuffer;

private enum MAX_DEPTH = 100;

public void registerCmsgpack(lua_State* L) nothrow @nogc
{
    lua_createtable(L, 0, 2);
    lua_pushcclosure(L, &msgpackPack, 0);
    lua_setfield(L, -2, "pack");
    lua_pushcclosure(L, &msgpackUnpack, 0);
    lua_setfield(L, -2, "unpack");
    lua_setglobal(L, "cmsgpack");
}

// ---------------------------------------------------------------------------
// pack
// ---------------------------------------------------------------------------

private void putBE(ref ByteBuffer o, ulong v, uint bytes) nothrow @nogc
{
    foreach_reverse (i; 0 .. bytes)
        o.appendByte(cast(ubyte)(v >> (8 * i)));
}

private void packInt(ref ByteBuffer o, long v) nothrow @nogc
{
    if (v >= 0)
    {
        if (v < 0x80)
            o.appendByte(cast(ubyte) v); // positive fixint
        else if (v <= ubyte.max)
        {
            o.appendByte(0xCC);
            o.appendByte(cast(ubyte) v);
        }
        else if (v <= ushort.max)
        {
            o.appendByte(0xCD);
            putBE(o, v, 2);
        }
        else if (v <= uint.max)
        {
            o.appendByte(0xCE);
            putBE(o, v, 4);
        }
        else
        {
            o.appendByte(0xCF);
            putBE(o, v, 8);
        }
    }
    else
    {
        if (v >= -32)
            o.appendByte(cast(ubyte) v); // negative fixint (0xE0..0xFF)
        else if (v >= byte.min)
        {
            o.appendByte(0xD0);
            o.appendByte(cast(ubyte) v);
        }
        else if (v >= short.min)
        {
            o.appendByte(0xD1);
            putBE(o, cast(ulong) v, 2);
        }
        else if (v >= int.min)
        {
            o.appendByte(0xD2);
            putBE(o, cast(ulong) v, 4);
        }
        else
        {
            o.appendByte(0xD3);
            putBE(o, cast(ulong) v, 8);
        }
    }
}

private void packStr(ref ByteBuffer o, scope const(char)[] s) nothrow @nogc
{
    if (s.length <= 31)
        o.appendByte(cast(ubyte)(0xA0 | s.length)); // fixstr
    else if (s.length <= ubyte.max)
    {
        o.appendByte(0xD9);
        o.appendByte(cast(ubyte) s.length);
    }
    else if (s.length <= ushort.max)
    {
        o.appendByte(0xDA);
        putBE(o, s.length, 2);
    }
    else
    {
        o.appendByte(0xDB);
        putBE(o, s.length, 4);
    }
    o.append(s);
}

private void packValue(lua_State* L, int idx, ref ByteBuffer o, int depth) nothrow @nogc
{
    if (depth > MAX_DEPTH)
        luaL_error(L, "Cannot pack: excessive nesting");
    switch (lua_type(L, idx))
    {
    case LUA_TNIL:
        o.appendByte(0xC0);
        break;
    case LUA_TBOOLEAN:
        o.appendByte(lua_toboolean(L, idx) ? 0xC3 : 0xC2);
        break;
    case LUA_TNUMBER:
        {
            int isnum;
            if (lua_isinteger(L, idx))
                packInt(o, lua_tointegerx(L, idx, &isnum));
            else
            {
                union F
                {
                    double d;
                    ulong u;
                }

                F f;
                f.d = lua_tonumberx(L, idx, &isnum);
                o.appendByte(0xCB);
                putBE(o, f.u, 8);
            }
            break;
        }
    case LUA_TSTRING:
        {
            size_t len;
            auto p = lua_tolstring(L, idx, &len);
            packStr(o, p[0 .. len]);
            break;
        }
    case LUA_TTABLE:
        {
            // array iff every key is an integer in 1..rawlen (like cjson)
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
                lua_settop(L, lua_gettop(L) - 1);
            }
            if (allInt && count == n)
            {
                if (n <= 15)
                    o.appendByte(cast(ubyte)(0x90 | n)); // fixarray
                else if (n <= ushort.max)
                {
                    o.appendByte(0xDC);
                    putBE(o, n, 2);
                }
                else
                {
                    o.appendByte(0xDD);
                    putBE(o, n, 4);
                }
                foreach (i; 1 .. n + 1)
                {
                    lua_rawgeti(L, idx, i);
                    packValue(L, lua_gettop(L), o, depth + 1);
                    lua_settop(L, lua_gettop(L) - 1);
                }
            }
            else
            {
                if (count <= 15)
                    o.appendByte(cast(ubyte)(0x80 | count)); // fixmap
                else if (count <= ushort.max)
                {
                    o.appendByte(0xDE);
                    putBE(o, count, 2);
                }
                else
                {
                    o.appendByte(0xDF);
                    putBE(o, count, 4);
                }
                lua_pushnil(L);
                while (lua_next(L, idx) != 0)
                {
                    // key: use a copy so lua_next keeps working
                    lua_pushvalue(L, -2);
                    packValue(L, lua_gettop(L), o, depth + 1);
                    lua_settop(L, lua_gettop(L) - 1);
                    packValue(L, lua_gettop(L), o, depth + 1); // value
                    lua_settop(L, lua_gettop(L) - 1);
                }
            }
            break;
        }
    default:
        o.appendByte(0xC0); // lua-cmsgpack packs unsupported types as nil
    }
}

extern (C) private int msgpackPack(lua_State* L) nothrow @nogc
{
    auto n = lua_gettop(L);
    if (n == 0)
        return luaL_error(L, "cmsgpack.pack expects at least one argument");
    static ByteBuffer buf; // TLS; single-threaded event loop
    buf.clear();
    foreach (i; 1 .. n + 1)
        packValue(L, i, buf, 0);
    lua_pushlstring(L, cast(const(char)*) buf.data.ptr, buf.length);
    return 1;
}

// ---------------------------------------------------------------------------
// unpack
// ---------------------------------------------------------------------------

private struct Decoder
{
    lua_State* L;
    const(ubyte)[] s;
    size_t i;

    void need(size_t n) nothrow @nogc
    {
        if (i + n > s.length)
            luaL_error(L, "Missing bytes in input.");
    }

    ulong readBE(uint bytes) nothrow @nogc
    {
        need(bytes);
        ulong v = 0;
        foreach (_; 0 .. bytes)
            v = (v << 8) | s[i++];
        return v;
    }

    void pushStr(size_t len) nothrow @nogc
    {
        need(len);
        lua_pushlstring(L, cast(const(char)*)&s[i], len);
        i += len;
    }

    void array(long n, int depth) nothrow @nogc
    {
        lua_createtable(L, cast(int)(n < int.max ? n : 0), 0);
        foreach (k; 1 .. n + 1)
        {
            value(depth + 1);
            lua_rawseti(L, -2, k);
        }
    }

    void map(long n, int depth) nothrow @nogc
    {
        lua_createtable(L, 0, cast(int)(n < int.max ? n : 0));
        foreach (_; 0 .. n)
        {
            value(depth + 1); // key
            value(depth + 1); // value
            lua_rawset(L, -3);
        }
    }

    /// Decodes one object and leaves it on the Lua stack.
    void value(int depth) nothrow @nogc
    {
        if (depth > MAX_DEPTH)
            luaL_error(L, "Found too many nested data structures");
        if (!lua_checkstack(L, 4))
            luaL_error(L, "Lua stack overflow while unpacking");
        need(1);
        auto tag = s[i++];
        if (tag < 0x80) // positive fixint
        {
            lua_pushinteger(L, tag);
            return;
        }
        if (tag >= 0xE0) // negative fixint
        {
            lua_pushinteger(L, cast(byte) tag);
            return;
        }
        if (tag >= 0xA0 && tag <= 0xBF) // fixstr
        {
            pushStr(tag & 0x1F);
            return;
        }
        if (tag >= 0x90 && tag <= 0x9F) // fixarray
        {
            array(tag & 0x0F, depth);
            return;
        }
        if (tag >= 0x80 && tag <= 0x8F) // fixmap
        {
            map(tag & 0x0F, depth);
            return;
        }
        switch (tag)
        {
        case 0xC0:
            lua_pushnil(L);
            break;
        case 0xC2:
            lua_pushboolean(L, 0);
            break;
        case 0xC3:
            lua_pushboolean(L, 1);
            break;
        case 0xC4: // bin8/16/32 read as strings, like lua-cmsgpack
            pushStr(cast(size_t) readBE(1));
            break;
        case 0xC5:
            pushStr(cast(size_t) readBE(2));
            break;
        case 0xC6:
            pushStr(cast(size_t) readBE(4));
            break;
        case 0xCA:
            {
                union F32
                {
                    float f;
                    uint u;
                }

                F32 f;
                f.u = cast(uint) readBE(4);
                lua_pushnumber(L, f.f);
                break;
            }
        case 0xCB:
            {
                union F64
                {
                    double d;
                    ulong u;
                }

                F64 f;
                f.u = readBE(8);
                lua_pushnumber(L, f.d);
                break;
            }
        case 0xCC:
            lua_pushinteger(L, cast(long) readBE(1));
            break;
        case 0xCD:
            lua_pushinteger(L, cast(long) readBE(2));
            break;
        case 0xCE:
            lua_pushinteger(L, cast(long) readBE(4));
            break;
        case 0xCF:
            lua_pushinteger(L, cast(long) readBE(8)); // u64 > long.max wraps
            break;
        case 0xD0:
            lua_pushinteger(L, cast(byte) readBE(1));
            break;
        case 0xD1:
            lua_pushinteger(L, cast(short) readBE(2));
            break;
        case 0xD2:
            lua_pushinteger(L, cast(int) readBE(4));
            break;
        case 0xD3:
            lua_pushinteger(L, cast(long) readBE(8));
            break;
        case 0xD9:
            pushStr(cast(size_t) readBE(1));
            break;
        case 0xDA:
            pushStr(cast(size_t) readBE(2));
            break;
        case 0xDB:
            pushStr(cast(size_t) readBE(4));
            break;
        case 0xDC:
            array(cast(long) readBE(2), depth);
            break;
        case 0xDD:
            array(cast(long) readBE(4), depth);
            break;
        case 0xDE:
            map(cast(long) readBE(2), depth);
            break;
        case 0xDF:
            map(cast(long) readBE(4), depth);
            break;
        default: // 0xC1 (never used) and the ext family
            luaL_error(L, "Unsupported msgpack type 0x%02x", cast(uint) tag);
        }
    }
}

extern (C) private int msgpackUnpack(lua_State* L) nothrow @nogc
{
    size_t len;
    auto p = lua_tolstring(L, 1, &len);
    if (p is null || len == 0)
        return luaL_error(L, "cmsgpack.unpack expects a non-empty string");
    Decoder d;
    d.L = L;
    d.s = cast(const(ubyte)[]) p[0 .. len];
    int count = 0;
    while (d.i < d.s.length)
    {
        d.value(0);
        count++;
    }
    return count;
}
