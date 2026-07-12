module dreads.scripting;

// Lua scripting (EVAL / EVALSHA / SCRIPT). One long-lived lua_State runs on a
// malloc-backed lua_Alloc, so Lua's internal GC never touches the D GC. The
// redis.call/pcall bridge builds an RVal command straight from the Lua stack,
// runs it through the @nogc dispatch, and converts the RESP reply back to Lua
// values using Redis's conversion rules.

import core.stdc.stdio : snprintf;
import core.stdc.stdlib : crealloc = realloc, cfree = free;
import core.sync.mutex : Mutex;

import std.digest.sha : SHA1;

import dreads.commands : dispatch, parseLong;
import dreads.dict : Dict, StrVal;
import dreads.lua;
import dreads.mem : Arena, ByteBuffer;
import dreads.obj : Keyspace;
import dreads.resp;

private __gshared lua_State* gL;
private __gshared Dict!StrVal gScripts; // sha1 (lowercase hex) -> script body

// The event loop is single-threaded, but the unit-threaded test runner is
// not; serialize every use of the shared Lua state. druntime's Mutex is
// portable (pthreads on POSIX, SRWLOCK on Windows); allocated once at startup.
private __gshared Mutex gLuaLock;

shared static this()
{
    gLuaLock = new Mutex;
}

// Bridge context for the duration of one EVAL: which keyspace/arena to use.
private struct BridgeCtx
{
    Keyspace* ks;
    Arena* arena;
    bool readOnly; // EVAL_RO / EVALSHA_RO
    bool sawRandom; // a non-deterministic command ran; writes must be refused
    ByteBuffer replyBuf; // staging for bridge replies, reused across calls
}

private __gshared BridgeCtx gCtx;

// --- sandbox: memory accounting and script deadline ---
private __gshared ulong gLuaBytes; // bytes currently allocated by the state
private __gshared long gLuaDeadlineMsecs; // MonoTime as msecs; 0 = no limit

extern (C) private void* luaAllocFn(void* ud, void* ptr, size_t osize, size_t nsize) nothrow @nogc
{
    import dreads.config : gConfig;

    // Lua 5.4: when ptr is null, osize is a type tag, not a size
    auto oldSize = ptr is null ? 0 : osize;
    if (nsize == 0)
    {
        cfree(ptr);
        gLuaBytes -= oldSize;
        return null;
    }
    if (nsize > oldSize && gConfig.luaMemoryLimit != 0
            && gLuaBytes + (nsize - oldSize) > gConfig.luaMemoryLimit)
        return null; // Lua turns this into a clean memory error
    auto p = crealloc(ptr, nsize);
    if (p !is null)
        gLuaBytes += nsize - oldSize;
    return p;
}

private long monoMsecs() nothrow @nogc
{
    import core.time : MonoTime;

    return MonoTime.currTime.ticks / (MonoTime.ticksPerSecond / 1000);
}

extern (C) private void luaTimeoutHook(lua_State* L, void* ar) nothrow @nogc
{
    if (gLuaDeadlineMsecs != 0 && monoMsecs() > gLuaDeadlineMsecs)
        luaL_error(L, "script exceeded the lua-time-limit");
}

// Scripts run in a curated environment: only base/string/table/math, no
// io/os/package/debug, escape hatches pruned, and _G protected against
// global creation (scripts share one state; globals would leak across them).
private static immutable protectGlobalsChunk = q{
    local mt = {}
    mt.__newindex = function(t, n, v)
        error("Script attempted to create global variable '" .. tostring(n) .. "'", 2)
    end
    mt.__index = function(t, n)
        error("Script attempted to access nonexistent global variable '" .. tostring(n) .. "'", 2)
    end
    setmetatable(_G, mt)
};

private bool ensureState() nothrow
{
    if (gL !is null)
        return true;
    gL = lua_newstate(&luaAllocFn, null);
    if (gL is null)
        return false;
    // sandbox layer 1: selective libraries
    luaL_requiref(gL, "_G", &luaopen_base, 1);
    luaL_requiref(gL, "string", &luaopen_string, 1);
    luaL_requiref(gL, "table", &luaopen_table, 1);
    luaL_requiref(gL, "math", &luaopen_math, 1);
    lua_settop(gL, 0);
    // sandbox layer 2: prune the escape hatches the base library ships
    foreach (name; ["dofile\0", "loadfile\0", "load\0", "print\0"])
    {
        lua_pushnil(gL);
        lua_setglobal(gL, name.ptr);
    }
    // global `redis` table with the call bridge (before _G gets protected)
    lua_createtable(gL, 0, 4);
    lua_pushcclosure(gL, &luaRedisCall, 0);
    lua_setfield(gL, -2, "call");
    lua_pushcclosure(gL, &luaRedisPcall, 0);
    lua_setfield(gL, -2, "pcall");
    lua_pushcclosure(gL, &luaStatusReply, 0);
    lua_setfield(gL, -2, "status_reply");
    lua_pushcclosure(gL, &luaErrorReply, 0);
    lua_setfield(gL, -2, "error_reply");
    lua_pushcclosure(gL, &luaSha1Hex, 0);
    lua_setfield(gL, -2, "sha1hex");
    lua_setglobal(gL, "redis");
    // helper libraries scripts expect from Redis
    {
        import dreads.cjson : registerCjson;
        import dreads.cmsgpack : registerCmsgpack;

        registerCjson(gL);
        registerCmsgpack(gL);
    }
    registerBitLib(gL);
    // pre-create KEYS/ARGV so reading them never trips the protection
    lua_createtable(gL, 0, 0);
    lua_setglobal(gL, "KEYS");
    lua_createtable(gL, 0, 0);
    lua_setglobal(gL, "ARGV");
    // sandbox layer 3: protect _G
    if (luaL_loadbuffer(gL, protectGlobalsChunk.ptr, protectGlobalsChunk.length,
            "@sandbox") != LUA_OK || lua_pcall(gL, 0, 0, 0) != LUA_OK)
    {
        lua_settop(gL, 0);
        return false;
    }
    // sandbox layer 4: instruction-count hook enforcing lua-time-limit
    lua_sethook(gL, &luaTimeoutHook, LUA_MASKCOUNT, 100_000);
    // compiled-chunk cache (sha -> function): compiling dominates EVAL cost,
    // so scripts compile once per state lifetime, like Redis
    lua_createtable(gL, 0, 16);
    lua_setfield(gL, LUA_REGISTRYINDEX, "dreads_scripts");
    return true;
}

// ---------------------------------------------------------------------------
// helper libraries: redis.sha1hex and the LuaJIT-style bit library
// ---------------------------------------------------------------------------

extern (C) private int luaSha1Hex(lua_State* L) nothrow @nogc
{
    size_t len;
    auto p = lua_tolstring(L, 1, &len);
    if (p is null)
        return luaL_error(L, "wrong number or type of arguments");
    char[40] hex = void;
    sha1Hex(p[0 .. len], hex);
    lua_pushlstring(L, hex.ptr, 40);
    return 1;
}

private int bitArg(lua_State* L, int idx) nothrow @nogc
{
    int isnum;
    auto d = lua_tonumberx(L, idx, &isnum);
    if (isnum == 0)
        luaL_error(L, "bad argument to bit operation (number expected)");
    return cast(int)(cast(long) d & 0xFFFF_FFFF);
}

extern (C) private int bitBand(lua_State* L) nothrow @nogc
{
    auto acc = bitArg(L, 1);
    foreach (i; 2 .. lua_gettop(L) + 1)
        acc &= bitArg(L, i);
    lua_pushinteger(L, acc);
    return 1;
}

extern (C) private int bitBor(lua_State* L) nothrow @nogc
{
    auto acc = bitArg(L, 1);
    foreach (i; 2 .. lua_gettop(L) + 1)
        acc |= bitArg(L, i);
    lua_pushinteger(L, acc);
    return 1;
}

extern (C) private int bitBxor(lua_State* L) nothrow @nogc
{
    auto acc = bitArg(L, 1);
    foreach (i; 2 .. lua_gettop(L) + 1)
        acc ^= bitArg(L, i);
    lua_pushinteger(L, acc);
    return 1;
}

extern (C) private int bitBnot(lua_State* L) nothrow @nogc
{
    lua_pushinteger(L, ~bitArg(L, 1));
    return 1;
}

extern (C) private int bitLshift(lua_State* L) nothrow @nogc
{
    lua_pushinteger(L, cast(int)(bitArg(L, 1) << (bitArg(L, 2) & 31)));
    return 1;
}

extern (C) private int bitRshift(lua_State* L) nothrow @nogc
{
    lua_pushinteger(L, cast(int)(cast(uint) bitArg(L, 1) >> (bitArg(L, 2) & 31)));
    return 1;
}

extern (C) private int bitArshift(lua_State* L) nothrow @nogc
{
    lua_pushinteger(L, bitArg(L, 1) >> (bitArg(L, 2) & 31));
    return 1;
}

extern (C) private int bitTobit(lua_State* L) nothrow @nogc
{
    lua_pushinteger(L, bitArg(L, 1));
    return 1;
}

extern (C) private int bitTohex(lua_State* L) nothrow @nogc
{
    char[16] b = void;
    auto n = snprintf(b.ptr, b.length, "%08x", cast(uint) bitArg(L, 1));
    lua_pushlstring(L, b.ptr, n);
    return 1;
}

private void registerBitLib(lua_State* L) nothrow @nogc
{
    lua_createtable(L, 0, 9);
    lua_pushcclosure(L, &bitBand, 0);
    lua_setfield(L, -2, "band");
    lua_pushcclosure(L, &bitBor, 0);
    lua_setfield(L, -2, "bor");
    lua_pushcclosure(L, &bitBxor, 0);
    lua_setfield(L, -2, "bxor");
    lua_pushcclosure(L, &bitBnot, 0);
    lua_setfield(L, -2, "bnot");
    lua_pushcclosure(L, &bitLshift, 0);
    lua_setfield(L, -2, "lshift");
    lua_pushcclosure(L, &bitRshift, 0);
    lua_setfield(L, -2, "rshift");
    lua_pushcclosure(L, &bitArshift, 0);
    lua_setfield(L, -2, "arshift");
    lua_pushcclosure(L, &bitTobit, 0);
    lua_setfield(L, -2, "tobit");
    lua_pushcclosure(L, &bitTohex, 0);
    lua_setfield(L, -2, "tohex");
    lua_setglobal(L, "bit");
}

// ---------------------------------------------------------------------------
// redis.call / redis.pcall
// ---------------------------------------------------------------------------

extern (C) private int luaRedisCall(lua_State* L) nothrow @nogc
{
    return redisCallImpl(L, true);
}

extern (C) private int luaRedisPcall(lua_State* L) nothrow @nogc
{
    return redisCallImpl(L, false);
}

/// {ok = <msg>} / {err = <msg>} constructors
extern (C) private int luaStatusReply(lua_State* L) nothrow @nogc
{
    return wrapReply(L, "ok");
}

extern (C) private int luaErrorReply(lua_State* L) nothrow @nogc
{
    return wrapReply(L, "err");
}

private int wrapReply(lua_State* L, const(char)* field) nothrow @nogc
{
    size_t len;
    auto p = lua_tolstring(L, 1, &len);
    if (p is null)
    {
        lua_pushlstring(L, "wrong number or type of arguments".ptr, 33);
        return lua_error(L);
    }
    lua_createtable(L, 0, 1);
    lua_pushlstring(L, p, len);
    lua_setfield(L, -2, field);
    return 1;
}

private int redisCallImpl(lua_State* L, bool raise) nothrow @nogc
{
    auto argc = lua_gettop(L);
    if (argc == 0 || gCtx.ks is null)
    {
        lua_pushlstring(L, "Please specify at least one argument for this redis lib call".ptr, 61);
        return lua_error(L);
    }
    auto arr = gCtx.arena.allocArray!RVal(argc);
    foreach (i; 0 .. argc)
    {
        auto t = lua_type(L, i + 1);
        if (t != LUA_TSTRING && t != LUA_TNUMBER)
        {
            lua_pushlstring(L, "Lua redis lib command arguments must be strings or integers".ptr, 60);
            return lua_error(L);
        }
        size_t len;
        auto p = lua_tolstring(L, i + 1, &len);
        arr[i].type = RType.BulkString;
        arr[i].str = p[0 .. len];
    }
    RVal cmd;
    cmd.type = RType.Array;
    cmd.arr = arr;

    {
        import dreads.commands : isWriteCommand;

        char[24] up = void;
        auto cname = arr[0].str;
        if (cname.length <= up.length)
        {
            foreach (ci, ch; cname)
                up[ci] = ch >= 'a' && ch <= 'z' ? cast(char)(ch - 32) : ch;
            auto uname = up[0 .. cname.length];
            if (gCtx.readOnly && isWriteCommand(uname))
            {
                lua_pushlstring(L,
                        "Write commands are not allowed from read-only scripts.".ptr, 55);
                return lua_error(L);
            }
            // Scripts replicate verbatim (the EVAL itself is the AOF/raft
            // entry), so a replay must retrace every step: once a random-
            // reply command ran, further writes would diverge — refuse them,
            // like pre-effects Redis. The clock is frozen per EVAL, so TIME
            // stays deterministic and needs no flag.
            if (gCtx.sawRandom && isWriteCommand(uname))
            {
                enum msg = "Write commands not allowed after non deterministic commands";
                lua_pushlstring(L, msg.ptr, msg.length);
                return lua_error(L);
            }
            switch (uname)
            {
            case "RANDOMKEY", "SRANDMEMBER", "HRANDFIELD", "ZRANDMEMBER":
                gCtx.sawRandom = true;
                break;
            default:
                break;
            }
        }
    }

    gCtx.replyBuf.clear();
    // keep the EVAL entry's frozen clock: inner dispatch must NOT re-freeze
    // to the wall clock, or TTL commands inside a replayed script drift
    {
        import dreads.det : detNow = now;

        dispatch(cmd, *gCtx.ks, gCtx.replyBuf, *gCtx.arena, detNow());
    }

    RVal reply;
    size_t pos = 0;
    if (parseValue(gCtx.replyBuf.data, pos, *gCtx.arena, reply) != ParseStatus.ok)
    {
        lua_pushlstring(L, "internal error decoding command reply".ptr, 37);
        return lua_error(L);
    }
    if (reply.type == RType.Error)
    {
        lua_createtable(L, 0, 1);
        lua_pushlstring(L, reply.str.ptr, reply.str.length);
        lua_setfield(L, -2, "err");
        if (raise)
            return lua_error(L); // longjmp; no D destructors live on this frame
        return 1;
    }
    pushRespToLua(L, reply);
    return 1;
}

/// Redis reply -> Lua value conversion rules.
private void pushRespToLua(lua_State* L, const ref RVal v) nothrow @nogc
{
    final switch (v.type)
    {
    case RType.Null:
        lua_pushboolean(L, 0);
        break;
    case RType.SimpleString:
        lua_createtable(L, 0, 1);
        lua_pushlstring(L, v.str.ptr, v.str.length);
        lua_setfield(L, -2, "ok");
        break;
    case RType.Error:
        lua_createtable(L, 0, 1);
        lua_pushlstring(L, v.str.ptr, v.str.length);
        lua_setfield(L, -2, "err");
        break;
    case RType.Integer:
        lua_pushinteger(L, v.integer);
        break;
    case RType.BulkString:
        lua_pushlstring(L, v.str.ptr, v.str.length);
        break;
    case RType.Array:
        lua_createtable(L, cast(int) v.arr.length, 0);
        foreach (i, ref e; v.arr)
        {
            pushRespToLua(L, e);
            lua_rawseti(L, -2, cast(long) i + 1);
        }
        break;
    }
}

/// Lua return value -> RESP reply conversion rules.
private void luaToResp(lua_State* L, int idx, ref ByteBuffer o) nothrow @nogc
{
    switch (lua_type(L, idx))
    {
    case LUA_TNIL:
        repNullBulk(o);
        break;
    case LUA_TBOOLEAN:
        if (lua_toboolean(L, idx))
            repInt(o, 1);
        else
            repNullBulk(o);
        break;
    case LUA_TNUMBER:
        {
            int isnum;
            repInt(o, cast(long) lua_tonumberx(L, idx, &isnum)); // Redis truncates
            break;
        }
    case LUA_TSTRING:
        {
            size_t len;
            auto p = lua_tolstring(L, idx, &len);
            repBulk(o, p[0 .. len]);
            break;
        }
    case LUA_TTABLE:
        {
            // {err=...} / {ok=...} win over the array part
            if (lua_getfield(L, idx, "err") == LUA_TSTRING)
            {
                size_t len;
                auto p = lua_tolstring(L, -1, &len);
                repError(o, p[0 .. len]);
                lua_settop(L, lua_gettop(L) - 1);
                break;
            }
            lua_settop(L, lua_gettop(L) - 1);
            if (lua_getfield(L, idx, "ok") == LUA_TSTRING)
            {
                size_t len;
                auto p = lua_tolstring(L, -1, &len);
                repSimple(o, p[0 .. len]);
                lua_settop(L, lua_gettop(L) - 1);
                break;
            }
            lua_settop(L, lua_gettop(L) - 1);
            // array part, stopping at the first nil like Redis
            auto n = cast(long) lua_rawlen(L, idx);
            long count = 0;
            while (count < n)
            {
                if (lua_rawgeti(L, idx, count + 1) == LUA_TNIL)
                {
                    lua_settop(L, lua_gettop(L) - 1);
                    break;
                }
                lua_settop(L, lua_gettop(L) - 1);
                count++;
            }
            repArrayHeader(o, cast(size_t) count);
            foreach (i; 0 .. count)
            {
                lua_rawgeti(L, idx, i + 1);
                luaToResp(L, lua_gettop(L), o);
                lua_settop(L, lua_gettop(L) - 1);
            }
            break;
        }
    default:
        repNullBulk(o); // functions/userdata read as nil
    }
}

// ---------------------------------------------------------------------------
// Commands: EVAL / EVALSHA / SCRIPT
// ---------------------------------------------------------------------------

private void sha1Hex(scope const(char)[] body_, ref char[40] outHex) nothrow @nogc
{
    static immutable hexDigits = "0123456789abcdef";
    SHA1 sha;
    sha.start();
    sha.put(cast(const(ubyte)[]) body_);
    auto digest = sha.finish();
    foreach (i, b; digest)
    {
        outHex[i * 2] = hexDigits[b >> 4];
        outHex[i * 2 + 1] = hexDigits[b & 0xF];
    }
}

/// EVAL script numkeys key [key ...] arg [arg ...]  (bySha: EVALSHA)
public void evalCommand(const(RVal)[] args, ref Keyspace ks, ref ByteBuffer o,
        ref Arena arena, bool bySha, bool readOnly = false) nothrow
{
    if (args.length < 2)
    {
        repError(o, bySha ? "ERR wrong number of arguments for 'evalsha' command"
                : "ERR wrong number of arguments for 'eval' command");
        return;
    }
    long numkeys;
    if (!parseLong(args[1].str, numkeys) || numkeys < 0)
    {
        repError(o, "ERR value is not an integer or out of range");
        return;
    }
    if (cast(size_t) numkeys > args.length - 2)
    {
        repError(o, "ERR Number of keys can't be greater than number of args");
        return;
    }

    gLuaLock.lock_nothrow();
    scope (exit)
        gLuaLock.unlock_nothrow();

    const(char)[] body_;
    char[40] sha = void;
    if (bySha)
    {
        if (args[0].str.length != 40)
        {
            repError(o, "NOSCRIPT No matching script.");
            return;
        }
        foreach (i, c; args[0].str)
            sha[i] = c >= 'A' && c <= 'Z' ? cast(char)(c + 32) : c;
        auto cached = gScripts.get(sha[]);
        if (cached is null)
        {
            repError(o, "NOSCRIPT No matching script.");
            return;
        }
        body_ = cached.rawView();
    }
    else
    {
        body_ = args[0].str;
        sha1Hex(body_, sha);
        if (gScripts.get(sha[]) is null)
            gScripts.set(sha[], StrVal.ofRaw(body_)); // EVAL populates the cache too
    }

    if (!ensureState())
    {
        repError(o, "ERR failed to initialize Lua");
        return;
    }

    // KEYS / ARGV globals
    auto keys = args[2 .. 2 + cast(size_t) numkeys];
    auto argv = args[2 + cast(size_t) numkeys .. $];
    lua_createtable(gL, cast(int) keys.length, 0);
    foreach (i, ref k; keys)
    {
        lua_pushlstring(gL, k.str.ptr, k.str.length);
        lua_rawseti(gL, -2, cast(long) i + 1);
    }
    lua_setglobal(gL, "KEYS");
    lua_createtable(gL, cast(int) argv.length, 0);
    foreach (i, ref a; argv)
    {
        lua_pushlstring(gL, a.str.ptr, a.str.length);
        lua_rawseti(gL, -2, cast(long) i + 1);
    }
    lua_setglobal(gL, "ARGV");

    gCtx.ks = &ks;
    gCtx.arena = &arena;
    gCtx.readOnly = readOnly;
    gCtx.sawRandom = false;
    // deterministic per-invocation RNG (replicas/replay must agree)
    lua_getglobal(gL, "math");
    lua_getfield(gL, -1, "randomseed");
    lua_pushinteger(gL, 0);
    lua_pcall(gL, 1, 0, 0);
    lua_settop(gL, 0);
    // arm the script deadline for the timeout hook
    import dreads.config : gConfig;

    gLuaDeadlineMsecs = gConfig.luaTimeLimitMs > 0 ? monoMsecs() + gConfig.luaTimeLimitMs : 0;
    scope (exit)
    {
        gCtx.ks = null;
        gCtx.arena = null;
        gCtx.readOnly = false;
        gLuaDeadlineMsecs = 0;
        lua_settop(gL, 0);
        // recycle: bound long-term growth by rebuilding the state once its
        // heap passes the threshold (cost amortizes to ~zero per script)
        enum RECYCLE_BYTES = 32UL * 1024 * 1024;
        if (gLuaBytes > RECYCLE_BYTES)
        {
            lua_close(gL);
            gL = null;
            gLuaBytes = 0;
        }
    }

    // cached compiled chunk, or compile and cache (compiling dominates EVAL)
    lua_getfield(gL, LUA_REGISTRYINDEX, "dreads_scripts");
    lua_pushlstring(gL, sha.ptr, 40);
    lua_rawget(gL, -2);
    if (lua_type(gL, -1) != LUA_TFUNCTION)
    {
        lua_settop(gL, lua_gettop(gL) - 1); // drop the miss
        if (luaL_loadbuffer(gL, body_.ptr, body_.length, "@user_script") != LUA_OK)
        {
            luaErrToResp(o, "ERR Error compiling script: ");
            return;
        }
        lua_pushlstring(gL, sha.ptr, 40);
        lua_pushvalue(gL, -2);
        lua_rawset(gL, -4); // cache[sha] = fn (fn stays on top)
    }
    // per-run _ENV: the script's globals live in a throwaway table chained to
    // the shared base — the cheap equivalent of a fresh interpreter per run
    // (the cache table sitting below the function is harmless to pcall)
    lua_createtable(gL, 0, 8); // env
    lua_createtable(gL, 0, 1); // env metatable
    lua_rawgeti(gL, LUA_REGISTRYINDEX, LUA_RIDX_GLOBALS);
    lua_setfield(gL, -2, "__index");
    lua_setmetatable(gL, -2);
    lua_setupvalue(gL, -2, 1);
    {
        import dreads.commands : gInScript;

        gInScript = true;
        scope (exit)
            gInScript = false;
        if (lua_pcall(gL, 0, 1, 0) != LUA_OK)
        {
            luaErrToResp(o, "ERR Error running script: ");
            return;
        }
    }
    luaToResp(gL, lua_gettop(gL), o);
}

/// -<prefix><lua error message>, CRLF-sanitized
private void luaErrToResp(ref ByteBuffer o, scope const(char)[] prefix) nothrow
{
    size_t len;
    auto p = lua_tolstring(gL, -1, &len);
    o.appendByte('-');
    o.append(prefix);
    if (p !is null)
    {
        foreach (c; p[0 .. len])
            o.appendByte(c == '\r' || c == '\n' ? ' ' : c);
    }
    o.append("\r\n");
}

/// Cached script body for a 40-char sha (any case), or null. The slice stays
/// valid until SCRIPT FLUSH; callers must use it immediately.
public const(char)[] cachedScript(scope const(char)[] sha) nothrow @nogc
{
    if (sha.length != 40)
        return null;
    char[40] lower = void;
    foreach (i, c; sha)
        lower[i] = c >= 'A' && c <= 'Z' ? cast(char)(c + 32) : c;
    gLuaLock.lock_nothrow();
    scope (exit)
        gLuaLock.unlock_nothrow();
    auto v = gScripts.get(lower[]);
    return v is null ? null : v.rawView();
}

/// SCRIPT LOAD/EXISTS/FLUSH
public void scriptCommand(const(RVal)[] args, ref ByteBuffer o) nothrow
{
    if (args.length == 0)
    {
        repError(o, "ERR wrong number of arguments for 'script' command");
        return;
    }
    gLuaLock.lock_nothrow();
    scope (exit)
        gLuaLock.unlock_nothrow();

    auto sub = args[0].str;
    char[8] sbuf = void;
    if (sub.length > sbuf.length)
    {
        repUnknownSubcommand(o, "SCRIPT", sub);
        return;
    }
    foreach (i, c; sub)
        sbuf[i] = c >= 'a' && c <= 'z' ? cast(char)(c - 32) : c;

    switch (cast(string) sbuf[0 .. sub.length])
    {
    case "LOAD":
        {
            if (args.length != 2)
            {
                repError(o, "ERR wrong number of arguments for 'script load' command");
                return;
            }
            // reject scripts that do not compile, like Redis
            if (!ensureState())
            {
                repError(o, "ERR failed to initialize Lua");
                return;
            }
            if (luaL_loadbuffer(gL, args[1].str.ptr, args[1].str.length, "@user_script") != LUA_OK)
            {
                luaErrToResp(o, "ERR Error compiling script: ");
                lua_settop(gL, 0);
                return;
            }
            lua_settop(gL, 0);
            char[40] sha = void;
            sha1Hex(args[1].str, sha);
            if (gScripts.get(sha[]) is null)
                gScripts.set(sha[], StrVal.ofRaw(args[1].str));
            repBulk(o, sha[]);
            return;
        }
    case "EXISTS":
        {
            repArrayHeader(o, args.length - 1);
            foreach (ref a; args[1 .. $])
            {
                char[40] sha = void;
                bool valid = a.str.length == 40;
                if (valid)
                {
                    foreach (i, c; a.str)
                        sha[i] = c >= 'A' && c <= 'Z' ? cast(char)(c + 32) : c;
                }
                repInt(o, valid && gScripts.get(sha[]) !is null ? 1 : 0);
            }
            return;
        }
    case "FLUSH":
        {
            gScripts.clear();
            if (gL !is null) // drop the compiled-chunk cache too
            {
                lua_createtable(gL, 0, 16);
                lua_setfield(gL, LUA_REGISTRYINDEX, "dreads_scripts");
            }
            repSimple(o, "OK");
            return;
        }
    default:
        repUnknownSubcommand(o, "SCRIPT", sub);
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

version (unittest)
{
    /// rest = [numkeys, keys..., args...]; numkeys defaults to "0".
    private string evalRun(ref Keyspace ks, string script, string[] rest...)
    {
        Arena arena;
        ByteBuffer o;
        auto all = [script] ~ (rest.length ? rest : ["0"]);
        auto vals = new RVal[all.length];
        foreach (i, s; all)
        {
            vals[i].type = RType.BulkString;
            vals[i].str = s;
        }
        evalCommand(vals, ks, o, arena, false);
        return (cast(string) o.data).idup;
    }
}

unittest // return type conversions
{
    Keyspace ks;
    scope (exit)
        ks.d.free();
    assert(evalRun(ks, "return 1") == ":1\r\n");
    assert(evalRun(ks, "return 3.9") == ":3\r\n"); // truncated like Redis
    assert(evalRun(ks, "return 'hi'") == "$2\r\nhi\r\n");
    assert(evalRun(ks, "return nil") == "$-1\r\n");
    assert(evalRun(ks, "return true") == ":1\r\n");
    assert(evalRun(ks, "return false") == "$-1\r\n");
    assert(evalRun(ks, "return {1, 'two', 3}") == "*3\r\n:1\r\n$3\r\ntwo\r\n:3\r\n");
    assert(evalRun(ks, "return {1, nil, 3}") == "*1\r\n:1\r\n"); // stops at nil
    assert(evalRun(ks, "return redis.status_reply('GOOD')") == "+GOOD\r\n");
    assert(evalRun(ks, "return redis.error_reply('bad thing')") == "-bad thing\r\n");
    assert(evalRun(ks, "return {KEYS[1], ARGV[1]}", "1", "k1", "a1") == "*2\r\n$2\r\nk1\r\n$2\r\na1\r\n");
}

unittest // redis.call bridge
{
    Keyspace ks;
    scope (exit)
        ks.d.free();
    assert(evalRun(ks, "return redis.call('SET', KEYS[1], ARGV[1])", "1", "k", "v") == "+OK\r\n");
    assert(evalRun(ks, "return redis.call('GET', KEYS[1])", "1", "k") == "$1\r\nv\r\n");
    assert(evalRun(ks, "return redis.call('GET', 'ghost')") == "$-1\r\n"); // nil -> false -> null bulk
    assert(evalRun(ks, "return redis.call('INCR', 'cnt') + 10") == ":11\r\n");
    assert(evalRun(ks, "redis.call('RPUSH','l','a','b'); return redis.call('LRANGE','l',0,-1)")
            == "*2\r\n$1\r\na\r\n$1\r\nb\r\n");
    // redis.call raises on error; pcall returns the error table
    auto raised = evalRun(ks, "return redis.call('INCR', 'k')");
    assert(raised[0] == '-');
    import std.algorithm : canFind;

    auto caught = evalRun(ks, "local e = redis.pcall('INCR', 'k'); return e.err");
    assert(caught[0] == '$' && caught.canFind("not an integer"));
}

unittest // errors
{
    Keyspace ks;
    scope (exit)
        ks.d.free();
    auto compile = evalRun(ks, "this is not lua");
    assert(compile[0 .. 28] == "-ERR Error compiling script:");
    auto runtime = evalRun(ks, "error('boom')");
    assert(runtime[0 .. 26] == "-ERR Error running script:");
}

unittest // SCRIPT LOAD/EXISTS/FLUSH + EVALSHA
{
    import std.format : format;

    Keyspace ks;
    scope (exit)
        ks.d.free();
    Arena arena;
    ByteBuffer o;

    RVal[3] loadArgs;
    loadArgs[0].type = RType.BulkString;
    loadArgs[0].str = "LOAD";
    loadArgs[1].type = RType.BulkString;
    loadArgs[1].str = "return 42";
    scriptCommand(loadArgs[0 .. 2], o);
    auto reply = (cast(string) o.data).idup;
    assert(reply[0 .. 4] == "$40\r"[0 .. 4]);
    auto sha = reply[5 .. 45]; // skip "$40\r\n"
    o.clear();

    // EXISTS: known and unknown
    RVal[3] exArgs;
    exArgs[0].type = RType.BulkString;
    exArgs[0].str = "EXISTS";
    exArgs[1].type = RType.BulkString;
    exArgs[1].str = sha;
    exArgs[2].type = RType.BulkString;
    exArgs[2].str = "0000000000000000000000000000000000000000";
    scriptCommand(exArgs[0 .. 3], o);
    assert(cast(string) o.data == "*2\r\n:1\r\n:0\r\n");
    o.clear();

    // EVALSHA runs the cached script
    RVal[2] evArgs;
    evArgs[0].type = RType.BulkString;
    evArgs[0].str = sha;
    evArgs[1].type = RType.BulkString;
    evArgs[1].str = "0";
    evalCommand(evArgs[0 .. 2], ks, o, arena, true);
    assert(cast(string) o.data == ":42\r\n");
    o.clear();

    // FLUSH drops the cache
    RVal[1] flArgs;
    flArgs[0].type = RType.BulkString;
    flArgs[0].str = "FLUSH";
    scriptCommand(flArgs[0 .. 1], o);
    assert(cast(string) o.data == "+OK\r\n");
    o.clear();
    evalCommand(evArgs[0 .. 2], ks, o, arena, true);
    assert((cast(string) o.data)[0 .. 9] == "-NOSCRIPT");
}
