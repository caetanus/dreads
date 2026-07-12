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
    // dispatch reaches Redis Functions through these (no import cycle)
    import dreads.commands : gFcallHook, gFunctionHook;

    gFunctionHook = &functionCommand;
    gFcallHook = &fcallCommand;
}

// Bridge context for the duration of one EVAL. In INLINE mode (unit tests,
// standalone-without-pool) the bridge dispatches directly against `ks`. In
// POOL mode the script runs on the dedicated Lua thread and the bridge
// round-trips each redis.call to the main thread (single keyspace writer),
// carrying `db`/`clock` instead of a keyspace pointer.
private struct BridgeCtx
{
    Keyspace* ks; // inline mode only
    Arena* arena;
    bool readOnly; // EVAL_RO / EVALSHA_RO
    bool viaPool; // true on the Lua worker: round-trip commands to main
    ushort db; // pool mode: which gDbs slot the round-trip targets
    ulong clock; // frozen per-script clock threaded through every round-trip
    ByteBuffer replyBuf; // staging for bridge replies, reused across calls
    ByteBuffer effectBuf; // re-encoded inner command (effect capture / propose)
}

// Bridge context lives wherever the VM runs: __gshared for inline (main), and
// the same variable on the single Lua thread in pool mode (only one thread
// touches the VM either way, so no sharing hazard).
private __gshared BridgeCtx gCtx;

// --- effects replication (the EVAL itself never enters the log) ---
// Every write a script performs via redis.call is what gets logged: its
// propagation-override form when the command set one (SETEX -> absolute
// time, SPOP -> SREM, XADD * -> resolved id), else the command verbatim.
// Standalone, the server installs a sink that appends to the AOF; under
// raft the bridge PROPOSES each write and the apply loop runs it, so the
// leader's state only ever changes through the log. Random-reply commands
// need no write guard under this model: replicas replay what happened.

/// Set by the server at boot: receives one RESP-encoded effect to log.
public __gshared void function(scope const(ubyte)[]) @nogc nothrow gScriptEffectSink;

/// True when the last EVAL performed at least one effective write.
public __gshared bool gScriptWrote;

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

// SCRIPT KILL: the main loop bumps this when a `SCRIPT KILL` arrives; the
// per-worker hook (which already polls the time limit) sees it and aborts the
// running script. Thread-shared, checked cheaply between instruction batches.
package shared bool gScriptKillRequested;

// True while the Lua thread is executing a script (SCRIPT KILL -> NOTBUSY when
// clear). Set/cleared by the worker around each run.
package shared bool gScriptRunning;

extern (C) private void luaTimeoutHook(lua_State* L, void* ar) nothrow @nogc
{
    import core.atomic : atomicLoad;

    if (atomicLoad(gScriptKillRequested))
        luaL_error(L, "Script killed by user with SCRIPT KILL...");
    if (gLuaDeadlineMsecs != 0 && monoMsecs() > gLuaDeadlineMsecs)
        luaL_error(L, "script exceeded the lua-time-limit");
}

// Lua 5.1 compatibility: Redis embeds 5.1 and every script in the wild
// targets it; restore the names 5.2+ moved or dropped. Runs before _G is
// protected (these ARE global writes).
private static immutable lua51CompatChunk = "unpack = table.unpack\n"
    ~ "table.getn = function(t) return #t end\n"
    ~ "math.pow = function(x, y) return x ^ y end\n"
    ~ "math.log10 = function(x) return math.log(x, 10) end\n"
    ~ "math.ldexp = function(m, e) return m * 2.0 ^ e end\n";

// Scripts run in a curated environment: only base/string/table/math, no
// io/os/package/debug, escape hatches pruned, and _G protected against
// global creation (scripts share one state; globals would leak across them).
private static immutable protectGlobalsChunk = q{
    local mt = {}
    mt.__newindex = function(t, n, v)
        error("Attempt to modify a readonly table", 2)
    end
    mt.__index = function(t, n)
        error("Script attempted to access nonexistent global variable '" .. tostring(n) .. "'", 2)
    end
    setmetatable(_G, mt)
};

/// Make the table on top of the stack read-only: writes raise "Attempt to
/// modify a readonly table" and the metatable is protected. Leaves the table
/// on the stack. Used for the library tables scripts must not tamper with.
private void installReadonlyProxy(lua_State* L, const(char)* name) nothrow @nogc
{
    lua_createtable(L, 0, 0); // proxy
    lua_createtable(L, 0, 3); // metatable
    lua_pushvalue(L, -3); // real table
    lua_setfield(L, -2, "__index");
    lua_pushcclosure(L, &luaReadonlyNewIndex, 0);
    lua_setfield(L, -2, "__newindex");
    lua_pushboolean(L, 0);
    lua_setfield(L, -2, "__metatable"); // hide it from get/setmetatable
    lua_setmetatable(L, -2); // proxy gets the metatable
    lua_setglobal(L, name); // global = proxy
    lua_settop(L, lua_gettop(L) - 1); // pop the real table
}

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
    lua_pushcclosure(gL, &luaRedisLog, 0);
    lua_setfield(gL, -2, "log");
    lua_pushcclosure(gL, &luaReplicateCommands, 0);
    lua_setfield(gL, -2, "replicate_commands");
    lua_pushcclosure(gL, &luaSetResp, 0);
    lua_setfield(gL, -2, "setresp");
    lua_pushcclosure(gL, &luaSetRepl, 0);
    lua_setfield(gL, -2, "set_repl");
    foreach (i, r; ["REPL_NONE\0", "REPL_AOF\0", "REPL_SLAVE\0",
            "REPL_REPLICA\0", "REPL_ALL\0"])
    {
        lua_pushinteger(gL, cast(long) i);
        lua_setfield(gL, -2, r.ptr);
    }
    lua_pushcclosure(gL, &luaRegisterFunction, 0);
    lua_setfield(gL, -2, "register_function");
    // redis.log severity constants (scripts pass them as the first argument)
    foreach (i, lvl; ["LOG_DEBUG\0", "LOG_VERBOSE\0", "LOG_NOTICE\0", "LOG_WARNING\0"])
    {
        lua_pushinteger(gL, cast(long) i);
        lua_setfield(gL, -2, lvl.ptr);
    }
    lua_pushvalue(gL, -1); // Valkey aliases the whole API: server == redis
    lua_setglobal(gL, "server");
    lua_setglobal(gL, "redis");
    // helper libraries scripts expect from Redis
    {
        import dreads.cjson : registerCjson;
        import dreads.cmsgpack : registerCmsgpack;

        registerCjson(gL);
        registerCmsgpack(gL);
    }
    registerBitLib(gL);
    // restricted os: the real table has only clock, but a metatable __index
    // hands back an error-raising stub for any other field, so os.execute()
    // fails with Valkey's exact "attempt to call field 'execute'" while
    // pairs(os) still enumerates just clock
    lua_createtable(gL, 0, 1);
    lua_pushcclosure(gL, &luaOsClock, 0);
    lua_setfield(gL, -2, "clock");
    lua_createtable(gL, 0, 1);
    lua_pushcclosure(gL, &luaOsForbiddenIndex, 0);
    lua_setfield(gL, -2, "__index");
    lua_setmetatable(gL, -2);
    lua_setglobal(gL, "os");
    // library tables are read-only: scripts may not shadow redis.call, etc.
    foreach (lib; ["redis\0", "cjson\0", "cmsgpack\0", "bit\0"])
    {
        lua_getglobal(gL, lib.ptr);
        installReadonlyProxy(gL, lib.ptr);
    }
    lua_getglobal(gL, "redis"); // server aliases the read-only redis proxy
    lua_setglobal(gL, "server");
    // pre-create KEYS/ARGV so reading them never trips the protection
    lua_createtable(gL, 0, 0);
    lua_setglobal(gL, "KEYS");
    lua_createtable(gL, 0, 0);
    lua_setglobal(gL, "ARGV");
    // Lua 5.1 compat aliases, then sandbox layer 3: protect _G
    if (luaL_loadbuffer(gL, lua51CompatChunk.ptr, lua51CompatChunk.length,
            "@compat51") != LUA_OK || lua_pcall(gL, 0, 0, 0) != LUA_OK)
    {
        lua_settop(gL, 0);
        return false;
    }
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
    // Redis Functions: fname -> callback (rebuilt lazily after a recycle
    // from gLibCode, the D-side source of truth)
    lua_createtable(gL, 0, 8);
    lua_setfield(gL, LUA_REGISTRYINDEX, "dreads_functions");
    return true;
}

// ---------------------------------------------------------------------------
// helper libraries: redis.sha1hex and the LuaJIT-style bit library
// ---------------------------------------------------------------------------

/// _ENV.__newindex: scripts run against a throwaway environment but may not
/// create globals (Valkey forbids it); every assignment to an undeclared
/// global lands here.
extern (C) private int luaReadonlyNewIndex(lua_State* L) nothrow @nogc
{
    return luaL_error(L, "Attempt to modify a readonly table");
}

/// Raise a plain-string error carrying its own code — no "chunk:line:"
/// location (unlike luaL_error), so pcall(err) is exactly msg and a client
/// switching on "-ERR ..." keeps working (issue #3663).
private int raiseErr(lua_State* L, string msg) nothrow @nogc
{
    lua_pushlstring(L, msg.ptr, msg.length);
    return lua_error(L);
}

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

/// os.<missing>: returns a closure that raises "attempt to call field '<f>'"
/// (upvalue 1 = the field name), matching Valkey's sandboxed os.
extern (C) private int luaOsForbiddenCall(lua_State* L) nothrow @nogc
{
    size_t len;
    auto name = lua_tolstring(L, lua_upvalueindex(1), &len);
    static ByteBuffer mb; // TLS
    mb.clear();
    mb.append("attempt to call field '");
    if (name !is null)
        mb.append(cast(const(ubyte)[]) name[0 .. len]);
    mb.append("'");
    lua_pushlstring(L, cast(const(char)*) mb.data.ptr, mb.data.length);
    return lua_error(L);
}

extern (C) private int luaOsForbiddenIndex(lua_State* L) nothrow @nogc
{
    // args: (table, key); return a raiser closure carrying the key name
    lua_pushvalue(L, 2);
    lua_pushcclosure(L, &luaOsForbiddenCall, 1);
    return 1;
}

/// os.clock(): elapsed CPU-ish seconds; scripts use it to measure spans.
extern (C) private int luaOsClock(lua_State* L) nothrow @nogc
{
    lua_pushnumber(L, cast(double) monoMsecs() / 1000.0);
    return 1;
}

/// redis.log(level, message...): accepted and dropped — dreads has no server
/// log file; the call must not error out of a script.
extern (C) private int luaRedisLog(lua_State* L) nothrow @nogc
{
    if (lua_gettop(L) < 2)
        return raiseErr(L, "ERR server.log() requires two arguments or more.");
    int isnum;
    auto lvl = lua_tointegerx(L, 1, &isnum);
    if (isnum == 0 || lvl < 0 || lvl > 3)
        return raiseErr(L, "ERR Invalid log level.");
    return 0;
}

/// redis.set_repl(flags): effects replication is the only mode, so the
/// replication target can't be narrowed; accept a valid arg, ignore it.
extern (C) private int luaSetRepl(lua_State* L) nothrow @nogc
{
    if (lua_gettop(L) < 1)
        return raiseErr(L, "ERR server.set_repl() requires one argument.");
    return 0;
}

/// redis.replicate_commands(): effects replication IS dreads' only mode
/// (each redis.call write is the log entry), so this is truthfully a yes —
/// like Redis 7+, where it also became the only mode.
extern (C) private int luaReplicateCommands(lua_State* L) nothrow @nogc
{
    lua_pushboolean(L, 1);
    return 1;
}

/// redis.setresp(2|3): scripts see RESP2-shaped conversions either way today.
extern (C) private int luaSetResp(lua_State* L) nothrow @nogc
{
    auto v = lua_tointegerx(L, 1, null);
    if (v != 2 && v != 3)
        return raiseErr(L, "ERR RESP version must be 2 or 3.");
    return 0;
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
    // LuaBitOp: optional width (default 8); negative width => uppercase, and
    // |width| digits capped at 8
    long width = 8;
    if (lua_gettop(L) >= 2)
    {
        int isnum;
        auto w = lua_tonumberx(L, 2, &isnum);
        if (isnum)
            width = cast(long) w;
    }
    bool upper = width < 0;
    long digits = width < 0 ? -width : width;
    if (digits > 8)
        digits = 8;
    char[16] b = void;
    auto fmt = upper ? "%0*X".ptr : "%0*x".ptr;
    auto n = snprintf(b.ptr, b.length, fmt, cast(int) digits, cast(uint) bitArg(L, 1));
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
    if (gLoadingLib)
    {
        enum msg = "attempt to call a redis command from a function loading context";
        lua_pushlstring(L, msg.ptr, msg.length);
        return lua_error(L);
    }
    auto argc = lua_gettop(L);
    // inline mode needs a bound keyspace; pool mode round-trips (ks is null)
    if (argc == 0 || (!gCtx.viaPool && gCtx.ks is null))
        return raiseErr(L, "ERR Please specify at least one argument for this redis lib call");
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

    bool isWrite = false;
    {
        import dreads.commands : isWriteCommand;

        char[24] up = void;
        auto cname = arr[0].str;
        if (cname.length <= up.length)
        {
            foreach (ci, ch; cname)
                up[ci] = ch >= 'a' && ch <= 'z' ? cast(char)(ch - 32) : ch;
            auto uc = up[0 .. cname.length];
            isWrite = isWriteCommand(uc);
            // commands that manage the server/connection make no sense from a
            // script and Redis flags them CMD_NOSCRIPT
            switch (uc)
            {
            case "CLUSTER", "SUBSCRIBE", "UNSUBSCRIBE", "PSUBSCRIBE",
                    "PUNSUBSCRIBE", "MULTI", "EXEC", "DISCARD", "WATCH",
                    "SCRIPT", "FUNCTION", "FCALL", "FCALL_RO", "EVAL",
                    "EVALSHA", "EVAL_RO", "EVALSHA_RO", "MONITOR", "SYNC",
                    "PSYNC", "RESET", "AUTH", "HELLO":
                enum ns = "ERR This Redis command is not allowed from script";
                lua_createtable(L, 0, 1);
                lua_pushlstring(L, ns.ptr, ns.length);
                lua_setfield(L, -2, "err");
                return lua_error(L);
            default:
                break;
            }
            if (gCtx.readOnly && isWrite)
            {
                // raise as an {err=...} object so it surfaces verbatim, the
                // way Redis reports bridge-level refusals
                enum ro = "ERR Write commands are not allowed from read-only scripts.";
                lua_createtable(L, 0, 1);
                lua_pushlstring(L, ro.ptr, ro.length);
                lua_setfield(L, -2, "err");
                return lua_error(L);
            }
        }
    }

    gCtx.replyBuf.clear();
    int st;
    if (gCtx.viaPool)
    {
        // POOL mode: hand the command to the main thread (single keyspace
        // writer), which executes it, captures the effect and replies. The
        // round-trip parks this Lua-thread fiber on a cross-thread event;
        // the @nogc cast is the same control-plane escape the server uses.
        alias RtFn = int function(scope const(RVal)[], bool, ref ByteBuffer) @nogc nothrow;
        st = (cast(RtFn)&poolRoundTrip)(arr, isWrite, gCtx.replyBuf);
    }
    else
    {
        // INLINE mode: run it right here against the bound keyspace.
        alias ExFn = int function(ref Keyspace, const ref RVal, scope const(RVal)[],
                bool, ulong, ref Arena, ref ByteBuffer, ref ByteBuffer) @nogc nothrow;
        st = (cast(ExFn)&executeScriptCommand)(*gCtx.ks, cmd, arr, isWrite,
                gCtx.clock, *gCtx.arena, gCtx.replyBuf, gCtx.effectBuf);
    }
    if (st == 1)
        return raiseErr(L, "READONLY You can't write against a read only replica.");
    if (st == 2)
        return raiseErr(L, "ERR replication error");

    RVal reply;
    size_t pos = 0;
    if (parseValue(gCtx.replyBuf.data, pos, *gCtx.arena, reply) != ParseStatus.ok)
    {
        lua_pushlstring(L, "internal error decoding command reply".ptr, 37);
        return lua_error(L);
    }
    if (reply.type == RType.Error)
    {
        // two error classes get the BRIDGE's wording, like Redis (scripts
        // and the suite match on these exact phrases)
        auto emsg = reply.str;
        if (emsg.length >= 19 && emsg[0 .. 19] == "ERR unknown command")
            emsg = "Unknown command called from script";
        else
        {
            enum arity = "wrong number of arguments";
            foreach (i; 0 .. emsg.length >= arity.length ? emsg.length - arity.length + 1 : 0)
            {
                if (emsg[i .. i + arity.length] == arity)
                {
                    emsg = "Wrong number of args calling command from script";
                    break;
                }
            }
        }
        static ByteBuffer eb; // TLS: ERR-prefixed error text
        emsg = ensureErrCode(eb, emsg);
        lua_createtable(L, 0, 1);
        lua_pushlstring(L, emsg.ptr, emsg.length);
        lua_setfield(L, -2, "err");
        if (raise)
            return lua_error(L); // longjmp; no D destructors live on this frame
        return 1;
    }
    pushRespToLua(L, reply);
    return 1;
}

/// Issue #3663: every error a script surfaces must carry an error code, so
/// clients switching on `-ERR ...` keep working. A message already starting
/// with an uppercase CODE + space is left alone (WRONGTYPE, READONLY, ...);
/// anything else is prefixed "ERR ". Result is staged in `buf`.
private const(char)[] ensureErrCode(ref ByteBuffer buf, const(char)[] msg) nothrow @nogc
{
    size_t i = 0;
    while (i < msg.length && msg[i] >= 'A' && msg[i] <= 'Z')
        i++;
    if (i > 0 && i < msg.length && msg[i] == ' ')
        return msg; // already coded
    buf.clear();
    buf.append("ERR ");
    buf.append(cast(const(ubyte)[]) msg);
    return cast(const(char)[]) buf.data;
}

/// Re-encodes a command as a RESP array into `dst` (an effect to log verbatim,
/// or a raft entry to propose).
private void encodeCmd(ref ByteBuffer dst, scope const(RVal)[] arr) nothrow @nogc
{
    dst.clear();
    repArrayHeader(dst, arr.length);
    foreach (ref a; arr)
        repBulk(dst, a.str);
}

private void encodeEffect(scope const(RVal)[] arr) nothrow @nogc
{
    encodeCmd(gCtx.effectBuf, arr);
}

/// Executes one script bridge command against `ks` at the frozen `clock`:
/// standalone dispatches and captures the effect (AOF sink); under raft
/// PROPOSES the effect through consensus so the leader's state only changes
/// via the log. Reply RESP lands in `reply`. Returns 0 ok, 1 = not the
/// leader, 2 = replication error. Runs on the MAIN thread (the single
/// keyspace writer) — inline for tests/standalone, or the cmd-drain in pool
/// mode. NOT @nogc: the raft path parks the fiber awaiting consensus.
package int executeScriptCommand(ref Keyspace ks, const ref RVal cmd,
        scope const(RVal)[] arr, bool isWrite, ulong clock, ref Arena arena,
        ref ByteBuffer reply, ref ByteBuffer effScratch) nothrow
{
    import dreads.det : freezeClock;

    reply.clear();
    if (!isWrite)
    {
        dispatch(cmd, ks, reply, arena, clock);
        return 0;
    }
    import dreads.commands : propagationOverride;
    import dreads.obj : gDbs, NUM_DBS;
    import dreads.replicator : gReplicator;
    import dreads.stream : nowMs;

    if (gReplicator !is null)
    {
        encodeCmd(effScratch, arr);
        auto dbIdx = cast(size_t)(&ks - &gDbs[0]);
        if (dbIdx >= NUM_DBS)
            dbIdx = 0;
        try
        {
            if (!gReplicator.isLeader)
                return 1;
            gReplicator.proposeWrite(effScratch.data, nowMs(), cast(ushort) dbIdx, reply);
        }
        catch (Exception)
            return 2;
        gScriptWrote = true;
        return 0;
    }
    // standalone: dispatch with the frozen clock, then log the effect
    dispatch(cmd, ks, reply, arena, clock);
    auto rd = reply.data;
    if (rd.length > 0 && rd[0] != '-')
    {
        if (gScriptEffectSink !is null)
        {
            if (!propagationOverride.empty)
                gScriptEffectSink(propagationOverride.data);
            else
            {
                encodeCmd(effScratch, arr);
                gScriptEffectSink(effScratch.data);
            }
        }
        gScriptWrote = true;
    }
    propagationOverride.clear();
    return 0;
}

// ---------------------------------------------------------------------------
// Lua worker thread — scripts run OFF the event loop on one dedicated thread
// (SPSC queues both ways), so a busy script can't stall the main loop and a
// SCRIPT KILL delivered on the main loop reaches the worker's hook. The
// keyspace stays single-writer: every redis.call round-trips to the main
// thread, which executes it and replies. Falls back to INLINE execution when
// the pool isn't started (unit tests, standalone-without-pool).
// ---------------------------------------------------------------------------

import vibe.core.core : runTask;
import vibe.core.sync : createSharedManualEvent, ManualEvent;
import vibe.core.taskpool : TaskPool;

import dreads.raftq : CrossQueue;

public enum LuaReqKind : ubyte
{
    eval,
    fcall,
    function_,
    script
}

private struct ReqSlot
{
    LuaReqKind kind;
    bool bySha, readOnly;
    ushort db;
    ulong clock;
    ByteBuffer args; // RESP-encoded arg array (stable across the hand-off)
    ByteBuffer reply; // filled by the Lua thread
    shared(ManualEvent) done;
    bool ready;
    ReqSlot* nextFree;
}

private struct CmdSlot
{
    ByteBuffer bytes; // RESP command from the bridge
    ushort db;
    ulong clock;
    bool isWrite;
    ByteBuffer reply;
    int status; // 0 ok / 1 readonly-replica / 2 repl error
    shared(ManualEvent) done;
    bool ready;
}

private shared TaskPool gLuaPool;
package __gshared bool gLuaPoolUp; // pool started? (server sets it)
private __gshared CrossQueue gReqQ; // main -> lua: execution requests
private __gshared CrossQueue gCmdQ; // lua -> main: redis.call round-trips
private __gshared CmdSlot gCmdSlot; // reused: the Lua thread runs one call at a time
private __gshared ReqSlot* gReqFree; // main-side freelist

// pool-mode signal: THREAD-LOCAL — true only on the Lua worker thread while a
// script runs. Must NOT be __gshared: the main thread has to see false so its
// SCRIPT KILL / routing guards work while the worker is busy.
private bool gPoolMode;
private ushort gPoolDb;
private ulong gPoolClock;

/// Binds gCtx for the run: inline uses the passed keyspace and the wall clock;
/// pool mode carries the request's db/clock and round-trips instead.
private void bindBridgeContext(Keyspace* ks) nothrow @nogc
{
    import dreads.det : detNow = now;

    if (gPoolMode)
    {
        gCtx.ks = null;
        gCtx.viaPool = true;
        gCtx.db = gPoolDb;
        gCtx.clock = gPoolClock;
    }
    else
    {
        gCtx.ks = ks;
        gCtx.viaPool = false;
        gCtx.db = 0;
        gCtx.clock = detNow();
    }
}

/// Bridge (Lua thread): post one command to the main thread and await its
/// reply. Returns the execute status; the RESP reply lands in `reply`.
private int poolRoundTrip(scope const(RVal)[] arr, bool isWrite, ref ByteBuffer reply) nothrow
{
    encodeCmd(gCmdSlot.bytes, arr);
    gCmdSlot.db = gCtx.db;
    gCmdSlot.clock = gCtx.clock;
    gCmdSlot.isWrite = isWrite;
    gCmdSlot.ready = false;
    gCmdSlot.status = 0;
    try
    {
        auto ec = gCmdSlot.done.emitCount;
        gCmdQ.put(gCmdSlot.bytes.data, cast(void*)&gCmdSlot, 0);
        while (!gCmdSlot.ready)
        {
            ec = gCmdSlot.done.emitCount;
            if (gCmdSlot.ready)
                break;
            gCmdSlot.done.waitUninterruptible(ec);
        }
    }
    catch (Exception)
    {
        reply.clear();
        repError(reply, "ERR script command dispatch failed");
        return 0;
    }
    reply.clear();
    reply.append(gCmdSlot.reply.data);
    return gCmdSlot.status; // gScriptWrote is set on the main thread by the drain
}

/// Start the Lua worker thread and the main-side command drain. Called once at
/// server boot. After this, EVAL/FCALL/FUNCTION/SCRIPT route through the pool.
public void startLuaScriptPool() nothrow
{
    try
    {
        gReqQ = new CrossQueue(1024);
        gCmdQ = new CrossQueue(1024);
        gCmdSlot.done = createSharedManualEvent();
        gLuaPool = new shared TaskPool(1, "lua");
        gLuaPool.runTaskH(&luaThreadEntry);
        runTask(() nothrow { cmdDrainLoop(); });
        gLuaPoolUp = true;
    }
    catch (Exception)
    {
    }
}

/// Main-side: hand a script request to the Lua thread and await its reply.
public void luaExecOnPool(LuaReqKind kind, scope const(RVal)[] args, bool bySha,
        bool readOnly, ushort db, ulong clock, ref ByteBuffer o) nothrow
{
    auto slot = acquireReqSlot();
    slot.kind = kind;
    slot.bySha = bySha;
    slot.readOnly = readOnly;
    slot.db = db;
    slot.clock = clock;
    encodeCmd(slot.args, args);
    slot.ready = false;
    try
    {
        auto ec = slot.done.emitCount;
        gReqQ.put(slot.args.data, cast(void*) slot, 0);
        while (!slot.ready)
        {
            ec = slot.done.emitCount;
            if (slot.ready)
                break;
            slot.done.waitUninterruptible(ec);
        }
    }
    catch (Exception)
    {
        releaseReqSlot(slot);
        repError(o, "ERR script execution failed");
        return;
    }
    o.append(slot.reply.data);
    releaseReqSlot(slot);
}

private ReqSlot* acquireReqSlot() nothrow
{
    if (gReqFree !is null)
    {
        auto s = gReqFree;
        gReqFree = s.nextFree;
        s.nextFree = null;
        return s;
    }
    auto s = new ReqSlot; // one-time GC as the pool of in-flight EVALs grows
    try
        s.done = createSharedManualEvent();
    catch (Exception)
    {
    }
    return s;
}

private void releaseReqSlot(ReqSlot* s) nothrow
{
    s.nextFree = gReqFree;
    gReqFree = s;
}

/// Routes a script entry through the pool when it's up and we're on the main
/// thread; true = handled (caller returns). On the Lua thread (gPoolMode) it
/// returns false so the real body runs.
private bool routeToPool(LuaReqKind kind, scope const(RVal)[] args, ref Keyspace ks,
        ref ByteBuffer o, bool bySha, bool readOnly) nothrow
{
    if (!gLuaPoolUp || gPoolMode)
        return false;
    import dreads.det : detNow = now;
    import dreads.obj : gDbs, NUM_DBS;

    auto dbi = cast(size_t)(&ks - &gDbs[0]);
    if (dbi >= NUM_DBS)
        dbi = 0;
    luaExecOnPool(kind, args, bySha, readOnly, cast(ushort) dbi, detNow(), o);
    return true;
}

/// The Lua worker thread: drain requests, run each script, reply. One request
/// at a time — the VM and its lua_State are affine to this thread.
private static void luaThreadEntry() nothrow
{
    static ByteBuffer payload;
    static Arena arena;
    while (true)
    {
        try
        {
            gReqQ.waitData();
            void* tag;
            ulong meta;
            uint kind;
            while (gReqQ.take(payload, tag, meta, kind))
            {
                auto slot = cast(ReqSlot*) tag;
                arena.reset();
                import core.atomic : atomicStore;

                atomicStore(gScriptRunning, true);
                scope (exit)
                {
                    atomicStore(gScriptRunning, false);
                    atomicStore(gScriptKillRequested, false); // consume any pending kill
                }
                // reconstruct the arg array from the RESP payload
                RVal cmd;
                size_t pos = 0;
                slot.reply.clear();
                if (parseValue(slot.args.data, pos, arena, cmd) != ParseStatus.ok
                        || cmd.type != RType.Array)
                {
                    repError(slot.reply, "ERR internal script marshalling error");
                }
                else
                {
                    gPoolMode = true;
                    gPoolDb = slot.db;
                    gPoolClock = slot.clock;
                    scope (exit)
                        gPoolMode = false;
                    auto a = cmd.arr;
                    // gDbs[0] is a placeholder; pool mode never dereferences ks
                    import dreads.obj : gDbs;

                    final switch (slot.kind)
                    {
                    case LuaReqKind.eval:
                        evalCommand(a, gDbs[0], slot.reply, arena, slot.bySha, slot.readOnly);
                        break;
                    case LuaReqKind.fcall:
                        fcallCommand(a, gDbs[0], slot.reply, arena, slot.readOnly);
                        break;
                    case LuaReqKind.function_:
                        functionCommand(a, gDbs[0], slot.reply, arena);
                        break;
                    case LuaReqKind.script:
                        scriptCommand(a, slot.reply);
                        break;
                    }
                }
                slot.ready = true;
                slot.done.emit();
            }
        }
        catch (Exception)
        {
        }
    }
}

/// Main-side command drain: execute each round-tripped redis.call against the
/// keyspace (single writer here), capture the effect, reply to the Lua thread.
private void cmdDrainLoop() nothrow
{
    static ByteBuffer payload;
    static Arena arena;
    static ByteBuffer eff;
    import dreads.commands : isWriteCommand;
    import dreads.obj : gDbs, NUM_DBS;

    while (true)
    {
        try
        {
            gCmdQ.waitData();
            void* tag;
            ulong meta;
            uint kind;
            while (gCmdQ.take(payload, tag, meta, kind))
            {
                auto slot = cast(CmdSlot*) tag;
                arena.reset();
                RVal cmd;
                size_t pos = 0;
                slot.reply.clear();
                slot.status = 0;
                if (parseValue(slot.bytes.data, pos, arena, cmd) == ParseStatus.ok
                        && cmd.type == RType.Array && cmd.arr.length > 0)
                {
                    auto dbi = slot.db < NUM_DBS ? slot.db : 0;
                    slot.status = executeScriptCommand(gDbs[dbi], cmd, cmd.arr,
                            slot.isWrite, slot.clock, arena, slot.reply, eff);
                }
                else
                    repError(slot.reply, "ERR internal script command error");
                slot.ready = true;
                slot.done.emit();
            }
        }
        catch (Exception)
        {
        }
    }
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
private void luaToResp(lua_State* L, int idx, ref ByteBuffer o, int depth = 0) nothrow @nogc
{
    // deep/recursive tables would overflow the C stack; Redis caps the
    // conversion and emits this sentinel (the reply so far is already framed)
    if (depth > 1000)
    {
        repError(o, "reached lua stack limit");
        return;
    }
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
            // {err=...} / {ok=...} win over the array part. RAW gets: this
            // runs outside any pcall, and a table with a throwing __index
            // metamethod (the protected _G, a user metatable) would longjmp
            // straight through the D frames — a hard crash, not an error.
            lua_pushlstring(L, "err".ptr, 3);
            if (lua_rawget(L, idx) == LUA_TSTRING)
            {
                size_t len;
                auto p = lua_tolstring(L, -1, &len);
                repError(o, p[0 .. len]);
                lua_settop(L, lua_gettop(L) - 1);
                break;
            }
            lua_settop(L, lua_gettop(L) - 1);
            lua_pushlstring(L, "ok".ptr, 2);
            if (lua_rawget(L, idx) == LUA_TSTRING)
            {
                size_t len;
                auto p = lua_tolstring(L, -1, &len);
                repSimple(o, p[0 .. len]);
                lua_settop(L, lua_gettop(L) - 1);
                break;
            }
            lua_settop(L, lua_gettop(L) - 1);
            // {double=n} -> RESP3 double / RESP2 bulk string
            lua_pushlstring(L, "double".ptr, 6);
            if (lua_rawget(L, idx) == LUA_TNUMBER)
            {
                import dreads.commands : repDouble;

                int isnum;
                repDouble(o, lua_tonumberx(L, -1, &isnum));
                lua_settop(L, lua_gettop(L) - 1);
                break;
            }
            lua_settop(L, lua_gettop(L) - 1);
            // {big_number=str} -> RESP3 big number / RESP2 bulk string
            lua_pushlstring(L, "big_number".ptr, 10);
            if (lua_rawget(L, idx) == LUA_TSTRING)
            {
                size_t bl;
                auto bp = lua_tolstring(L, -1, &bl);
                repBigNumber(o, bp[0 .. bl]);
                lua_settop(L, lua_gettop(L) - 1);
                break;
            }
            lua_settop(L, lua_gettop(L) - 1);
            // {map={k=v,...}} -> RESP3 map / RESP2 flat array
            lua_pushlstring(L, "map".ptr, 3);
            if (lua_rawget(L, idx) == LUA_TTABLE)
            {
                int mapIdx = lua_gettop(L);
                size_t pairs = 0;
                lua_pushnil(L);
                while (lua_next(L, mapIdx) != 0)
                {
                    pairs++;
                    lua_settop(L, lua_gettop(L) - 1); // drop value, keep key
                }
                repMapHeader(o, pairs);
                lua_pushnil(L);
                while (lua_next(L, mapIdx) != 0)
                {
                    // key at -2, value at -1; emit both, keep key for next
                    luaToResp(L, lua_gettop(L) - 1, o, depth + 1);
                    luaToResp(L, lua_gettop(L), o, depth + 1);
                    lua_settop(L, lua_gettop(L) - 1);
                }
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
                luaToResp(L, lua_gettop(L), o, depth + 1);
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
    if (routeToPool(LuaReqKind.eval, args, ks, o, bySha, readOnly))
        return;
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

    gCtx.arena = &arena;
    gCtx.readOnly = readOnly;
    bindBridgeContext(&ks); // sets ks/viaPool/db/clock for inline or pool mode
    gScriptWrote = false; // per-EVAL: the server signals WATCH/blocked wakes
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
    lua_pushcclosure(gL, &luaReadonlyNewIndex, 0);
    lua_setfield(gL, -2, "__newindex"); // scripts may not create globals
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

/// -<prefix><lua error message>, CRLF-sanitized. A {err=...} error object
/// (a failing redis.call raised inside the script) surfaces VERBATIM, like
/// Redis: the caller sees the original command error, not a wrapper.
private void luaErrToResp(ref ByteBuffer o, scope const(char)[] prefix) nothrow
{
    size_t len;
    if (lua_type(gL, -1) == LUA_TTABLE)
    {
        lua_pushlstring(gL, "err".ptr, 3);
        if (lua_rawget(gL, -2) == LUA_TSTRING)
        {
            auto ep = lua_tolstring(gL, -1, &len);
            o.appendByte('-');
            foreach (c; ep[0 .. len])
                o.appendByte(c == '\r' || c == '\n' ? ' ' : c);
            o.append("\r\n");
            lua_settop(gL, lua_gettop(gL) - 1);
            return;
        }
        lua_settop(gL, lua_gettop(gL) - 1);
    }
    auto p = lua_tolstring(gL, -1, &len);
    o.appendByte('-');
    // an error already carrying a CODE (our raiseErr strings: "ERR ...",
    // "WRONGTYPE ...") surfaces verbatim; a raw Lua error gets the wrapper
    if (p !is null && !startsWithCode(p[0 .. len]))
        o.append(prefix);
    if (p !is null)
    {
        foreach (c; p[0 .. len])
            o.appendByte(c == '\r' || c == '\n' ? ' ' : c);
    }
    o.append("\r\n");
}

/// True when the message opens with an uppercase CODE followed by a space.
private bool startsWithCode(scope const(char)[] m) nothrow @nogc
{
    size_t i = 0;
    while (i < m.length && m[i] >= 'A' && m[i] <= 'Z')
        i++;
    return i > 0 && i < m.length && m[i] == ' ';
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
    // SCRIPT KILL runs on the MAIN thread (never the pool — the Lua thread is
    // busy with the very script we're killing): flag it for the worker's hook.
    if (!gPoolMode && args[0].str.length == 4)
    {
        char[4] k = void;
        foreach (i, c; args[0].str)
            k[i] = c >= 'a' && c <= 'z' ? cast(char)(c - 32) : c;
        if (k == "KILL")
        {
            import core.atomic : atomicLoad, atomicStore;

            if (!atomicLoad(gScriptRunning))
            {
                repError(o, "NOTBUSY No scripts in execution right now.");
                return;
            }
            atomicStore(gScriptKillRequested, true);
            repSimple(o, "OK");
            return;
        }
    }
    // everything else runs on the Lua thread when the pool is up
    if (gLuaPoolUp && !gPoolMode)
    {
        import dreads.det : detNow = now;

        luaExecOnPool(LuaReqKind.script, args, false, false, 0, detNow(), o);
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
// Redis Functions: FUNCTION LOAD/DELETE/FLUSH/LIST/STATS + FCALL/FCALL_RO.
// Named callbacks registered by a library script, sharing the EVAL sandbox,
// bridge and effects replication. D side owns the durable state (function ->
// library + flags, library -> source); the Lua-side callback table is a
// cache rebuilt lazily from the source after a state recycle. FUNCTION
// LOAD/DELETE/FLUSH set propagationOverride = themselves, so they reach the
// AOF and (via the server's raft gate) the followers; FCALL replicates by
// its effects like EVAL.
// ---------------------------------------------------------------------------

private __gshared Dict!StrVal gFns; // fname -> "<lib>\0" ~ ('W'|'R')
private __gshared Dict!StrVal gLibCode; // libname -> full library source
private __gshared bool gLoadingLib; // register_function allowed; redis.call not
private __gshared const(char)[] gLoadingLibName; // valid during one LOAD
private __gshared ByteBuffer gLoadNames; // "\0"-joined fnames this LOAD added

/// redis.register_function('name', callback) or
/// redis.register_function{function_name=, callback=, flags={...}}
extern (C) private int luaRegisterFunction(lua_State* L) nothrow @nogc
{
    if (!gLoadingLib)
    {
        enum msg = "redis.register_function can only be called inside a library load";
        lua_pushlstring(L, msg.ptr, msg.length);
        return lua_error(L);
    }
    const(char)[] fname;
    bool noWrites = false;
    size_t len;
    if (lua_type(L, 1) == LUA_TTABLE)
    {
        lua_getfield(L, 1, "function_name");
        auto p = lua_tolstring(L, -1, &len);
        if (p is null)
        {
            enum m1 = "missing function_name";
            lua_pushlstring(L, m1.ptr, m1.length);
            return lua_error(L);
        }
        fname = p[0 .. len];
        // fname stays valid: the string lives in the argument table
        lua_settop(L, 1);
        lua_getfield(L, 1, "flags");
        if (lua_type(L, -1) == LUA_TTABLE)
        {
            foreach (i; 1 .. 17) // flags are few; bounded scan
            {
                lua_rawgeti(L, -1, i);
                auto fp = lua_tolstring(L, -1, &len);
                if (fp is null)
                {
                    lua_settop(L, lua_gettop(L) - 1);
                    break;
                }
                if (fp[0 .. len] == "no-writes")
                    noWrites = true;
                lua_settop(L, lua_gettop(L) - 1);
            }
        }
        lua_settop(L, 1);
        lua_getfield(L, 1, "callback");
    }
    else
    {
        auto p = lua_tolstring(L, 1, &len);
        if (p is null)
        {
            enum m2 = "wrong arguments to register_function";
            lua_pushlstring(L, m2.ptr, m2.length);
            return lua_error(L);
        }
        fname = p[0 .. len];
        lua_pushvalue(L, 2);
    }
    if (lua_type(L, -1) != LUA_TFUNCTION)
    {
        enum m3 = "callback must be a function";
        lua_pushlstring(L, m3.ptr, m3.length);
        return lua_error(L);
    }
    // collision with another library is an error; re-registering within the
    // same library (a lazy reload) just overwrites
    auto meta = gFns.get(fname);
    if (meta !is null)
    {
        auto raw = meta.rawView();
        auto lib = raw[0 .. $ - 2];
        if (lib != gLoadingLibName)
        {
            enum m4 = "Function already exists in another library";
            lua_pushlstring(L, m4.ptr, m4.length);
            return lua_error(L);
        }
    }
    // registry cache: dreads_functions[fname] = callback
    lua_getfield(L, LUA_REGISTRYINDEX, "dreads_functions");
    lua_pushlstring(L, fname.ptr, fname.length);
    lua_pushvalue(L, -3);
    lua_rawset(L, -3);
    lua_settop(L, 0);
    // durable D-side metadata
    static ByteBuffer mv; // TLS scratch: "<lib>\0" ~ flag
    mv.clear();
    mv.append(gLoadingLibName);
    mv.appendByte(0);
    mv.appendByte(noWrites ? 'R' : 'W');
    gFns.set(fname, StrVal.ofRaw(cast(const(char)[]) mv.data));
    gLoadNames.append(fname);
    gLoadNames.appendByte(0);
    return 0;
}

/// Runs a library body in loading mode; false = Lua error (reply written).
private bool runLibraryBody(scope const(char)[] lib, scope const(char)[] body_,
        ref ByteBuffer o, bool emitError) nothrow
{
    gLoadingLib = true;
    gLoadingLibName = lib;
    gLoadNames.clear();
    scope (exit)
    {
        gLoadingLib = false;
        gLoadingLibName = null;
    }
    // the "#!lua name=..." shebang is metadata, not Lua: compile past it
    if (body_.length >= 2 && body_[0] == '#' && body_[1] == '!')
    {
        size_t nl = 0;
        while (nl < body_.length && body_[nl] != '\n')
            nl++;
        body_ = nl < body_.length ? body_[nl + 1 .. $] : null;
    }
    if (luaL_loadbuffer(gL, body_.ptr, body_.length, "@user_function") != LUA_OK)
    {
        if (emitError)
            luaErrToResp(o, "ERR Error compiling function: ");
        lua_settop(gL, 0);
        return false;
    }
    // same throwaway _ENV as EVAL, chained to the protected base
    lua_createtable(gL, 0, 8);
    lua_createtable(gL, 0, 1);
    lua_rawgeti(gL, LUA_REGISTRYINDEX, LUA_RIDX_GLOBALS);
    lua_setfield(gL, -2, "__index");
    lua_pushcclosure(gL, &luaReadonlyNewIndex, 0);
    lua_setfield(gL, -2, "__newindex"); // scripts may not create globals
    lua_setmetatable(gL, -2);
    lua_setupvalue(gL, -2, 1);
    if (lua_pcall(gL, 0, 0, 0) != LUA_OK)
    {
        // roll back whatever this load registered
        size_t start = 0;
        auto names = cast(const(char)[]) gLoadNames.data;
        foreach (i, ch; names)
        {
            if (ch == 0)
            {
                gFns.del(names[start .. i]);
                start = i + 1;
            }
        }
        if (emitError)
            luaErrToResp(o, "ERR Error registering functions: ");
        lua_settop(gL, 0);
        return false;
    }
    lua_settop(gL, 0);
    return true;
}

/// Parses "#!lua name=<lib>" and returns the library name (null = bad).
private const(char)[] parseShebang(scope const(char)[] code) nothrow @nogc
{
    if (code.length < 2 || code[0] != '#' || code[1] != '!')
        return null;
    size_t eol = 0;
    while (eol < code.length && code[eol] != '\n')
        eol++;
    auto line = code[0 .. eol];
    if (line.length < 5 || line[2 .. 5] != "lua")
        return null;
    enum tag = "name=";
    foreach (i; 0 .. line.length)
    {
        if (i + tag.length <= line.length && line[i .. i + tag.length] == tag)
        {
            auto rest = line[i + tag.length .. $];
            size_t end = 0;
            while (end < rest.length && rest[end] != ' ' && rest[end] != '\t'
                    && rest[end] != '\r')
                end++;
            return end == 0 ? null : rest[0 .. end];
        }
    }
    return null;
}

/// Deletes every function belonging to lib; returns how many were dropped.
private size_t dropLibFunctions(scope const(char)[] lib) nothrow
{
    // collect first (no deleting while iterating), then remove dict + cache
    static ByteBuffer victims; // TLS
    victims.clear();
    foreach (i; 0 .. gFns.capacity)
    {
        if (!gFns.slotLive(i))
            continue;
        auto raw = gFns.valAt(i).rawView();
        if (raw[0 .. $ - 2] == lib)
        {
            victims.append(gFns.keyAt(i));
            victims.appendByte(0);
        }
    }
    size_t n = 0, start = 0;
    auto names = cast(const(char)[]) victims.data;
    foreach (i, ch; names)
    {
        if (ch != 0)
            continue;
        auto fname = names[start .. i];
        gFns.del(fname);
        if (gL !is null)
        {
            lua_getfield(gL, LUA_REGISTRYINDEX, "dreads_functions");
            lua_pushlstring(gL, fname.ptr, fname.length);
            lua_pushnil(gL);
            lua_rawset(gL, -3);
            lua_settop(gL, 0);
        }
        n++;
        start = i + 1;
    }
    return n;
}

/// Leaves the raw FUNCTION subcommand in propagationOverride: registry
/// mutations must reach the AOF and the raft log like any write.
private void propagateFunctionCmd(const(RVal)[] args) nothrow @nogc
{
    import dreads.commands : propagationOverride;

    propagationOverride.clear();
    repArrayHeader(propagationOverride, 1 + args.length);
    repBulk(propagationOverride, "FUNCTION");
    foreach (ref a; args)
        repBulk(propagationOverride, a.str);
}

/// FUNCTION LOAD [REPLACE] code | DELETE lib | FLUSH | LIST | STATS | HELP
public void functionCommand(const(RVal)[] args, ref Keyspace ks, ref ByteBuffer o,
        ref Arena arena) nothrow
{
    if (routeToPool(LuaReqKind.function_, args, ks, o, false, false))
        return;
    import dreads.commands : eqICKeyword;

    if (args.length == 0)
    {
        repError(o, "ERR wrong number of arguments for 'function' command");
        return;
    }
    gLuaLock.lock_nothrow();
    scope (exit)
        gLuaLock.unlock_nothrow();
    auto sub = args[0].str;
    if (eqICKeyword(sub, "HELP"))
    {
        repHelp!"FUNCTION"(o);
        return;
    }
    if (eqICKeyword(sub, "LOAD"))
    {
        bool replace = args.length >= 2 && eqICKeyword(args[1].str, "REPLACE");
        auto codeIdx = replace ? 2 : 1;
        if (args.length != codeIdx + 1)
        {
            repError(o, "ERR wrong number of arguments for 'function|load' command");
            return;
        }
        auto code = args[cast(size_t) codeIdx].str;
        auto lib = parseShebang(code);
        if (lib is null)
        {
            repError(o, "ERR Missing library metadata");
            return;
        }
        if (gLibCode.get(lib) !is null)
        {
            if (!replace)
            {
                o.append("-ERR Library '");
                o.append(lib);
                o.append("' already exists\r\n");
                return;
            }
            dropLibFunctions(lib);
        }
        if (!ensureState())
        {
            repError(o, "ERR failed to initialize Lua");
            return;
        }
        if (!runLibraryBody(lib, code, o, true))
            return;
        if (gLoadNames.empty)
        {
            gLibCode.del(lib);
            repError(o, "ERR No functions registered");
            return;
        }
        gLibCode.set(lib, StrVal.ofRaw(code));
        propagateFunctionCmd(args);
        repBulk(o, lib);
        return;
    }
    if (eqICKeyword(sub, "DELETE"))
    {
        if (args.length != 2)
        {
            repError(o, "ERR wrong number of arguments for 'function|delete' command");
            return;
        }
        if (gLibCode.get(args[1].str) is null)
        {
            repError(o, "ERR Library not found");
            return;
        }
        dropLibFunctions(args[1].str);
        gLibCode.del(args[1].str);
        propagateFunctionCmd(args);
        repSimple(o, "OK");
        return;
    }
    if (eqICKeyword(sub, "FLUSH"))
    {
        gFns.clear();
        gLibCode.clear();
        if (gL !is null)
        {
            lua_createtable(gL, 0, 8);
            lua_setfield(gL, LUA_REGISTRYINDEX, "dreads_functions");
        }
        propagateFunctionCmd(args);
        repSimple(o, "OK");
        return;
    }
    if (eqICKeyword(sub, "LIST"))
    {
        // array of {library_name, engine, functions:[{name, description, flags}]}
        size_t nlibs = 0;
        foreach (i; 0 .. gLibCode.capacity)
            if (gLibCode.slotLive(i))
                nlibs++;
        repArrayHeader(o, nlibs);
        foreach (i; 0 .. gLibCode.capacity)
        {
            if (!gLibCode.slotLive(i))
                continue;
            auto lib = gLibCode.keyAt(i);
            repMapHeader(o, 3);
            repBulk(o, "library_name");
            repBulk(o, lib);
            repBulk(o, "engine");
            repBulk(o, "LUA");
            repBulk(o, "functions");
            size_t nf = 0;
            foreach (j; 0 .. gFns.capacity)
                if (gFns.slotLive(j) && gFns.valAt(j).rawView()[0 .. $ - 2] == lib)
                    nf++;
            repArrayHeader(o, nf);
            foreach (j; 0 .. gFns.capacity)
            {
                if (!gFns.slotLive(j))
                    continue;
                auto raw = gFns.valAt(j).rawView();
                if (raw[0 .. $ - 2] != lib)
                    continue;
                repMapHeader(o, 3);
                repBulk(o, "name");
                repBulk(o, gFns.keyAt(j));
                repBulk(o, "description");
                repNullBulk(o);
                repBulk(o, "flags");
                repSetHeader(o, raw[$ - 1] == 'R' ? 1 : 0);
                if (raw[$ - 1] == 'R')
                    repBulk(o, "no-writes");
            }
        }
        return;
    }
    if (eqICKeyword(sub, "STATS"))
    {
        size_t nlibs = 0, nfns = 0;
        foreach (i; 0 .. gLibCode.capacity)
            if (gLibCode.slotLive(i))
                nlibs++;
        foreach (i; 0 .. gFns.capacity)
            if (gFns.slotLive(i))
                nfns++;
        repMapHeader(o, 2);
        repBulk(o, "running_script");
        repNullBulk(o);
        repBulk(o, "engines");
        repMapHeader(o, 1);
        repBulk(o, "LUA");
        repMapHeader(o, 2);
        repBulk(o, "libraries_count");
        repInt(o, cast(long) nlibs);
        repBulk(o, "functions_count");
        repInt(o, cast(long) nfns);
        return;
    }
    repUnknownSubcommand(o, "FUNCTION", sub);
}

/// FCALL/FCALL_RO name numkeys key [key ...] arg [arg ...]
public void fcallCommand(const(RVal)[] args, ref Keyspace ks, ref ByteBuffer o,
        ref Arena arena, bool readOnly) nothrow
{
    if (routeToPool(LuaReqKind.fcall, args, ks, o, false, readOnly))
        return;
    if (args.length < 2)
    {
        repError(o, readOnly ? "ERR wrong number of arguments for 'fcall_ro' command"
                : "ERR wrong number of arguments for 'fcall' command");
        return;
    }
    long numkeys;
    if (!parseLong(args[1].str, numkeys))
    {
        repError(o, "ERR value is not an integer or out of range");
        return;
    }
    if (numkeys < 0)
    {
        repError(o, "ERR Number of keys can't be negative");
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
    auto meta = gFns.get(args[0].str);
    if (meta is null)
    {
        repError(o, "ERR Function not found");
        return;
    }
    auto raw = meta.rawView();
    if (readOnly && raw[$ - 1] != 'R')
    {
        repError(o, "ERR Can not execute a script with write flag using *_ro command.");
        return;
    }
    if (!ensureState())
    {
        repError(o, "ERR failed to initialize Lua");
        return;
    }
    // fetch the callback; a recycled state rebuilds it from the library source
    bool fetchCallback() nothrow @nogc
    {
        // stash the callback under a fixed registry key so it survives the
        // stack cleanup without lua_remove (not bound)
        lua_getfield(gL, LUA_REGISTRYINDEX, "dreads_functions");
        lua_pushlstring(gL, args[0].str.ptr, args[0].str.length);
        lua_rawget(gL, -2);
        if (lua_type(gL, -1) != LUA_TFUNCTION)
        {
            lua_settop(gL, 0);
            return false;
        }
        lua_setfield(gL, LUA_REGISTRYINDEX, "dreads_current_fn");
        lua_settop(gL, 0);
        return true;
    }

    if (!fetchCallback())
    {
        auto lib = raw[0 .. $ - 2];
        auto src = gLibCode.get(lib);
        if (src is null || !runLibraryBody(lib, src.rawView(), o, false) || !fetchCallback())
        {
            repError(o, "ERR Function not found");
            return;
        }
    }

    gCtx.arena = &arena;
    gCtx.readOnly = readOnly;
    bindBridgeContext(&ks);
    gScriptWrote = false;
    // deterministic per-invocation RNG, script deadline, state recycling —
    // the same run discipline as EVAL
    lua_getglobal(gL, "math");
    lua_getfield(gL, -1, "randomseed");
    lua_pushinteger(gL, 0);
    lua_pcall(gL, 1, 0, 0);
    lua_settop(gL, 0);
    lua_getfield(gL, LUA_REGISTRYINDEX, "dreads_current_fn"); // callback
    import dreads.config : gConfig;

    gLuaDeadlineMsecs = gConfig.luaTimeLimitMs > 0 ? monoMsecs() + gConfig.luaTimeLimitMs : 0;
    scope (exit)
    {
        gCtx.ks = null;
        gCtx.arena = null;
        gCtx.readOnly = false;
        gLuaDeadlineMsecs = 0;
        lua_settop(gL, 0);
        enum RECYCLE_BYTES = 32UL * 1024 * 1024;
        if (gLuaBytes > RECYCLE_BYTES)
        {
            lua_close(gL);
            gL = null;
            gLuaBytes = 0;
        }
    }
    // functions receive KEYS/ARGV as ARGUMENTS (two tables), not globals
    auto keys = args[2 .. 2 + cast(size_t) numkeys];
    auto argv = args[2 + cast(size_t) numkeys .. $];
    lua_createtable(gL, cast(int) keys.length, 0);
    foreach (i, ref k; keys)
    {
        lua_pushlstring(gL, k.str.ptr, k.str.length);
        lua_rawseti(gL, -2, cast(long) i + 1);
    }
    lua_createtable(gL, cast(int) argv.length, 0);
    foreach (i, ref a; argv)
    {
        lua_pushlstring(gL, a.str.ptr, a.str.length);
        lua_rawseti(gL, -2, cast(long) i + 1);
    }
    {
        import dreads.commands : gInScript;

        gInScript = true;
        scope (exit)
            gInScript = false;
        if (lua_pcall(gL, 2, 1, 0) != LUA_OK)
        {
            luaErrToResp(o, "ERR Error running function: ");
            return;
        }
    }
    luaToResp(gL, lua_gettop(gL), o);
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
