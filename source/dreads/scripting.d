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
private __gshared Dict!StrVal gScripts; // sha1 (lowercase hex) -> script SOURCE
// sha1 -> compiled BYTECODE (lua_dump of the source). A local fast-reload cache:
// after a state recycle, chunks reload from bytecode (no reparse) instead of
// recompiling the source. Rebuilt from source, never exported — the source is the
// portable/version-independent form for export/import (RDB/replication); Lua
// bytecode is bound to this exact interpreter build.
private __gshared Dict!StrVal gScriptBc;

// The event loop is single-threaded, but the unit-threaded test runner is
// not; serialize every use of the shared Lua state. druntime's Mutex is
// portable (pthreads on POSIX, SRWLOCK on Windows); allocated once at startup.
private __gshared Mutex gLuaLock;

shared static this()
{
    gLuaLock = new Mutex;
    // dispatch reaches Redis Functions (and the script-cache count for INFO)
    // through these hooks (no import cycle)
    import dreads.commands : gFcallHook, gFunctionHook, gScriptCountHook,
        gScriptWritesHook;

    gFunctionHook = &functionCommand;
    gFcallHook = &fcallCommand;
    gScriptCountHook = &cachedScriptCount;
    gScriptWritesHook = &scriptWrites; // WRITE-pause: hold writing scripts only
}

/// WRITE-mode CLIENT PAUSE hook: does this EVAL/EVALSHA/FCALL invocation possibly
/// write? A no-writes shebang (EVAL/EVALSHA) or a function registered `no-writes`
/// (FCALL) is read-only and passes the barrier; a legacy or unflagged script may
/// write and is held. arg[1] is the inline body (EVAL), sha (EVALSHA) or function
/// name (FCALL). Runs on the event-loop thread; only briefly locks for lookups.
private bool scriptWrites(scope const(char)[] uname, const ref RVal cmd) @nogc nothrow
{
    if (cmd.type != RType.Array || cmd.arr.length < 2)
        return true; // malformed — be safe (it will error on run anyway)
    auto a1 = cmd.arr[1].str;
    if (uname == "EVAL")
        return bodyMayWrite(a1);
    if (uname == "EVALSHA")
    {
        auto body_ = cachedScript(a1); // takes its own lock
        return body_ is null ? true : bodyMayWrite(body_); // uncached => NOSCRIPT later
    }
    // FCALL: the registry stores 'R' (no-writes) or 'W' as the last meta byte.
    gLuaLock.lock_nothrow();
    scope (exit)
        gLuaLock.unlock_nothrow();
    auto meta = gFns.get(a1);
    if (meta is null)
        return true; // unknown function => let it through the gate to error
    auto raw = meta.rawView();
    return raw.length == 0 || raw[$ - 1] != 'R';
}

/// A shebang script is read-only only when its first line carries `no-writes`;
/// a legacy (no-shebang) script may always write.
private bool bodyMayWrite(scope const(char)[] body_) @nogc nothrow
{
    if (body_.length < 2 || body_[0] != '#' || body_[1] != '!')
        return true; // legacy script — may write
    size_t e = 0;
    while (e < body_.length && body_[e] != '\n')
        e++;
    auto line = body_[0 .. e]; // the shebang line
    if (line.length >= 9)
        foreach (i; 0 .. line.length - 8)
            if (line[i .. i + 9] == "no-writes")
                return false;
    return true;
}

/// Per-invocation math.random seed. dreads replicates script EFFECTS, not the
/// RNG, so successive scripts must NOT share a fixed seed (Redis 7+ dropped the
/// deterministic reseed for the same reason) — otherwise every math.random()
/// returns the same value forever. An ever-increasing counter mixed with the
/// wall clock gives each run a distinct seed; an in-script math.randomseed()
/// still overrides it, so explicitly-seeded scripts stay reproducible.
private shared ulong gRandSeedCtr;
private ulong nextScriptRandSeed() nothrow @nogc
{
    import core.atomic : atomicOp;
    import dreads.stream : nowMs;

    return atomicOp!"+="(gRandSeedCtr, 1) ^ nowMs();
}

/// Deny-oom policy for the script in flight (THREAD-LOCAL, like gPoolMode): the
/// bridge lets writes through only when this is true. Set per run in
/// evalCommand/fcallCommand; read by the deny-OOM gate in executeScriptCommand,
/// which runs on the MAIN thread (a redis.call round-trips there). __gshared and
/// safe because exactly one script runs at a time and the round-trip's cross-
/// thread event orders the write-before-read.
public __gshared bool gScriptAllowOom;

/// The current script is read-only (EVAL_RO/EVALSHA_RO or a no-writes shebang /
/// function). Read by the RO-write gate in executeScriptCommand (main thread),
/// same one-script-at-a-time __gshared reasoning as gScriptAllowOom.
public __gshared bool gScriptReadOnly;

/// Source id of the running script (its sha, or a function name) — appended to a
/// client-facing script error as ` script: <src>, on @user_script:1.`, the way
/// Valkey tags where the error came from. Set per run on the Lua worker.
private __gshared char[64] gScriptSource;
private __gshared size_t gScriptSourceLen;

/// True when the server is over its maxmemory limit (a read-only check; script
/// writes don't run the eviction cycle — see DRIFT.md on approximate LRU).
private bool scriptOverMaxmemory() nothrow @nogc
{
    import dreads.config : gConfig;
    import dreads.mem : usedMemory;

    return gConfig.maxmemory != 0 && usedMemory() > gConfig.maxmemory;
}

/// Number of scripts in the EVAL cache (INFO Memory: number_of_cached_scripts).
public size_t cachedScriptCount() nothrow @nogc
{
    gLuaLock.lock_nothrow();
    scope (exit)
        gLuaLock.unlock_nothrow();
    return gScripts.length;
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
    ulong userId; // the calling ACL user (0 = unrestricted/no ACL); rides each
    // redis.call to the main thread, which resolves + enforces it
}

// The calling ACL user id for the script about to run — set by the server on
// the main thread right before EVAL/FCALL and captured SYNCHRONOUSLY into the
// pool request (no yield in between), so concurrent scripts never race on it.
// 0 = the common case (unrestricted user / no ACL) => no enforcement.
package ulong gPendingScriptUser;
private ulong gPoolScriptUser; // Lua-thread-local copy, taken from the request

/// Server hook: which user is about to run a script (0 to disable enforcement).
public void scriptSetPendingUser(ulong userId) nothrow @nogc
{
    gPendingScriptUser = userId;
}

// Pointer to the EVAL/FCALL caller's tot-cmds counter. Each redis.call a script
// makes is a processed command on the caller (Redis counts it in the client's
// command stats), so executeScriptCommand — which runs on the main thread, where
// this Conn field lives — bumps it. Set right before the script runs, cleared
// right after; single-script-at-a-time makes the __gshared pointer safe (same
// reasoning as gPendingScriptUser).
package ulong* gScriptCallerCmds;

/// Server hook: the caller's tot-cmds counter for the script about to run (null off).
public void scriptSetCallerCmds(ulong* p) nothrow @nogc
{
    gScriptCallerCmds = p;
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

// The script's current RESP level (redis.setresp). Thread-local: only the VM
// thread touches it. Controls how redis.call replies convert to Lua AND the
// gRespProto the round-trip runs the command at.
private int gScriptResp = 2;

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
// READ shim only: reading an undeclared global raises Valkey's exact error. WRITE
// protection is NOT here anymore — it comes from the VM-level read-only flag
// (lua_enablereadonlytable, applied in D right after this runs), which no Lua-level
// trick (rawset, setmetatable, getmetatable("").__index, ...) can bypass. So this
// chunk only installs the nonexistent-global read behavior on _G.
private static immutable protectGlobalsChunk = q{
    setmetatable(_G, {
        __index = function(t, n)
            error("Script attempted to access nonexistent global variable '" .. tostring(n) .. "'", 2)
        end
    })
};

/// Seal a table (by top-of-stack) read-only via the VM flag, plus its metatable if
/// it has one (the _G/os read shims live in a metatable that a script could reach
/// through getmetatable()). Pops nothing it wasn't given; leaves the stack balanced.
private void sealTable(lua_State* L) nothrow @nogc
{
    if (lua_getmetatable(L, -1)) // pushes the metatable if present
    {
        lua_enablereadonlytable(L, -1, 1);
        lua_pop(L, 1);
    }
    lua_enablereadonlytable(L, -1, 1);
}

/// Mark every shared sandbox table read-only at the VM level. Reads/calls keep
/// working; every write path (assignment, rawset, setmetatable, and the
/// getmetatable("").__index reach into the real string table) is rejected by the
/// interpreter — no Lua-level metatable trick can bypass it. Called once, last in
/// ensureState, after the compat shims and read-shim have done their writes.
private void sealReadonlyTables(lua_State* L) nothrow @nogc
{
    static immutable names = ["_G\0", "string\0", "table\0", "math\0", "redis\0",
        "server\0", "cjson\0", "cmsgpack\0", "bit\0", "os\0"];
    foreach (n; names)
    {
        lua_getglobal(L, n.ptr);
        sealTable(L);
        lua_pop(L, 1);
    }
    // string VALUES share a metatable whose __index is the real string table
    // (already sealed via "string"); seal that metatable itself too.
    lua_pushlstring(L, "".ptr, 0);
    if (lua_getmetatable(L, -1))
    {
        lua_enablereadonlytable(L, -1, 1);
        lua_pop(L, 1);
    }
    lua_pop(L, 1);
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
    // string.dump serializes a function to bytecode — a bytecode-escape primitive
    // (Redis strips it too), pointless once load/loadstring reject bytecode. Remove
    // it here, before the string table is sealed read-only.
    lua_getglobal(gL, "string");
    lua_pushnil(gL);
    lua_setfield(gL, -2, "dump");
    lua_pop(gL, 1);
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
    lua_pushcclosure(gL, &luaAclCheckCmd, 0);
    lua_setfield(gL, -2, "acl_check_cmd");
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
    // loadstring exists but rejects everything (nil): loading dumped bytecode
    // then calling the nil result is Valkey's "attempt to call a nil value"
    lua_pushcclosure(gL, &luaLoadstringStub, 0);
    lua_setglobal(gL, "loadstring");
    // pre-create KEYS/ARGV so reading them never trips the protection
    lua_createtable(gL, 0, 0);
    lua_setglobal(gL, "KEYS");
    lua_createtable(gL, 0, 0);
    lua_setglobal(gL, "ARGV");
    // Lua 5.1 compat shims (unpack/table.getn/math.pow/...) — scripts target 5.1.
    // MUST run before the tables are sealed read-only below: it writes into them.
    if (luaL_loadbuffer(gL, lua51CompatChunk.ptr, lua51CompatChunk.length,
            "@compat51") != LUA_OK || lua_pcall(gL, 0, 0, 0) != LUA_OK)
    {
        lua_settop(gL, 0);
        return false;
    }
    // install the _G nonexistent-global READ shim
    if (luaL_loadbuffer(gL, protectGlobalsChunk.ptr, protectGlobalsChunk.length,
            "@sandbox") != LUA_OK || lua_pcall(gL, 0, 0, 0) != LUA_OK)
    {
        lua_settop(gL, 0);
        return false;
    }
    // Seal every shared table with the VM read-only flag: reads/calls still work,
    // but ALL writes (=, rawset, setmetatable, getmetatable("").__index, ...) are
    // rejected by the interpreter itself. This replaces the old metatable-proxy +
    // rawset/setmetatable gates. Runs last, after every setup write is done.
    sealReadonlyTables(gL);
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
// compiled-bytecode cache (sha -> lua_dump): source is the canonical, exportable
// form; bytecode is a local fast-reload cache so a chunk skips the parser after a
// state recycle.
// ---------------------------------------------------------------------------

extern (C) private int luaBcWriter(lua_State* L, const(void)* p, size_t sz, void* ud) nothrow @nogc
{
    (cast(ByteBuffer*) ud).append((cast(const(ubyte)*) p)[0 .. sz]);
    return 0;
}

/// Dump the compiled function on top of the stack into gScriptBc[sha]; the function
/// stays on the stack. Keeps debug info (strip=0) so runtime errors still carry line
/// numbers. Best-effort — a dump failure just leaves the entry unpopulated.
private void captureBytecode(lua_State* L, const(char)[] sha) nothrow
{
    ByteBuffer buf;
    if (lua_dump(L, &luaBcWriter, &buf, 0) == 0)
        gScriptBc.set(sha, StrVal.ofRaw(cast(const(char)[]) buf.data));
}

/// Leave the compiled function for (sha, source) on the stack, or return false on a
/// compile error (nothing pushed). Reloads from the bytecode cache when present (no
/// reparse); otherwise compiles the source as TEXT ONLY — a body that is actually
/// Lua bytecode is refused, closing the untrusted-bytecode load vector — and captures
/// its bytecode for next time.
private bool loadUserChunk(lua_State* L, const(char)[] sha, const(char)[] source,
        const(char)* chunkname) nothrow
{
    if (auto bc = gScriptBc.get(sha))
    {
        auto b = bc.rawView();
        if (luaL_loadbufferx(L, b.ptr, b.length, chunkname, "b") == LUA_OK)
            return true;
        lua_pop(L, 1); // drop the load error; fall back to the source
    }
    if (luaL_loadbufferx(L, source.ptr, source.length, chunkname, "t") != LUA_OK)
        return false;
    if (gScriptBc.get(sha) is null)
        captureBytecode(L, sha);
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

/// redis.acl_check_cmd(cmd, arg...): true if the CALLER's ACL user may run `cmd`
/// with those args (command + key permissions), false if denied. An unknown
/// command errors. Unrestricted / no-ACL callers always pass.
extern (C) private int luaAclCheckCmd(lua_State* L) nothrow @nogc
{
    import dreads.acl : aclUserById, aclUnrestricted, aclCanRunCmd, aclCmdIndex,
        aclDeniedKey;

    immutable argc = lua_gettop(L);
    if (argc < 1)
        return raiseErr(L, "ERR Please specify at least one argument for this redis lib call");
    size_t clen;
    auto cp = lua_tolstring(L, 1, &clen);
    char[64] lc = void;
    int cidx = -1;
    if (cp !is null && clen && clen <= lc.length)
    {
        foreach (i; 0 .. clen)
            lc[i] = cp[i] >= 'A' && cp[i] <= 'Z' ? cast(char)(cp[i] + 32) : cp[i];
        cidx = aclCmdIndex(cast(const(char)[]) lc[0 .. clen]);
    }
    if (cidx < 0)
        return raiseErr(L, "ERR Invalid command passed to server.acl_check_cmd()");
    auto u = aclUserById(gCtx.userId);
    if (gCtx.userId == 0 || u is null || aclUnrestricted(u))
    {
        lua_pushboolean(L, 1); // no ACL in force for this caller
        return 1;
    }
    bool allowed = aclCanRunCmd(u, cidx);
    if (allowed) // command permitted — now the key patterns
    {
        RVal[32] argbuf = void;
        size_t n = 0;
        foreach (li; 1 .. argc + 1)
        {
            if (n >= argbuf.length)
                break;
            size_t sl;
            auto sp = lua_tolstring(L, cast(int) li, &sl);
            argbuf[n].type = RType.BulkString;
            argbuf[n].str = sp is null ? "" : cast(string) sp[0 .. sl];
            n++;
        }
        if (aclDeniedKey(u, cast(const(char)[]) lc[0 .. clen], argbuf[0 .. n]) !is null)
            allowed = false;
    }
    lua_pushboolean(L, allowed ? 1 : 0);
    return 1;
}

/// loadstring stub: always nil (see the setglobal note).
extern (C) private int luaLoadstringStub(lua_State* L) nothrow @nogc
{
    lua_pushnil(L);
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

/// redis.setresp(2|3): sets the RESP level redis.call replies convert at.
extern (C) private int luaSetResp(lua_State* L) nothrow @nogc
{
    auto v = lua_tointegerx(L, 1, null);
    if (v != 2 && v != 3)
        return raiseErr(L, "ERR RESP version must be 2 or 3.");
    gScriptResp = cast(int) v;
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
            enum m = "ERR Command arguments must be strings or integers";
            lua_pushlstring(L, m.ptr, m.length);
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
        import dreads.commands : isWriteCommand, isPausedByWrite, isDenyOomCommand;

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
            // The RO-write refusal (a write / may-replicate command from a
            // read-only script) is enforced at the pipeline top too — see the gate
            // in executeScriptCommand — so it is counted like any command.
            // deny-OOM is enforced at the pipeline top (executeScriptCommand, on
            // the main thread) so the refusal is counted there like any command —
            // see the gate + statRejected in executeScriptCommand.
            // SELECT inside a script switches the db for the rest of THIS script
            // (subsequent redis.call target the new db) without touching the
            // caller's connection db. Update the bridge's db before the command
            // runs; an out-of-range index falls through and the dispatch of
            // SELECT returns the error, leaving the context unchanged.
            if (uc == "SELECT" && argc >= 2)
            {
                import dreads.commands : parseLong;
                import dreads.obj : gDbs, NUM_DBS;

                long dbn;
                if (parseLong(arr[1].str, dbn) && dbn >= 0 && dbn < NUM_DBS)
                {
                    gCtx.db = cast(ushort) dbn;
                    if (!gCtx.viaPool)
                        gCtx.ks = &gDbs[cast(size_t) dbn];
                }
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
                bool, ulong, ulong, int, ref Arena, ref ByteBuffer, ref ByteBuffer) @nogc nothrow;
        st = (cast(ExFn)&executeScriptCommand)(*gCtx.ks, cmd, arr, isWrite,
                gCtx.clock, gCtx.userId, gScriptResp, *gCtx.arena, gCtx.replyBuf, gCtx.effectBuf);
    }
    if (st == 1)
        return raiseErr(L, "READONLY You can't write against a read only replica.");
    if (st == 2)
        return raiseErr(L, "ERR replication error");

    auto rbytes = cast(const(char)[]) gCtx.replyBuf.data;
    if (rbytes.length == 0)
    {
        lua_pushlstring(L, "internal error decoding command reply".ptr, 37);
        return lua_error(L);
    }
    // top-level error: apply the bridge's wording (scripts/suite match these)
    if (rbytes[0] == '-')
    {
        size_t e = 1;
        while (e < rbytes.length && rbytes[e] != '\r')
            e++;
        auto emsg = rbytes[1 .. e];
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
    // non-error: parse the RESP (RESP2 or RESP3, per the script's setresp)
    // straight into a Lua value
    size_t pos = 0;
    respBytesToLua(L, rbytes, pos);
    return 1;
}

/// Parses one RESP value from `b` at `pos` (advancing it) and pushes the Lua
/// equivalent, using Redis's conversion rules AND handling RESP3 types the way
/// a script at `setresp(3)` sees them: map -> {map={...}}, set -> {set={...}},
/// double -> {double=n}, big number -> {big_number="..."}, bool -> true/false,
/// verbatim -> the string, null -> false.
private void respBytesToLua(lua_State* L, scope const(char)[] b, ref size_t pos) nothrow @nogc
{
    if (pos >= b.length)
    {
        lua_pushboolean(L, 0);
        return;
    }
    char lead = b[pos++];
    auto line = readLine(b, pos); // content up to (not incl) CRLF; advances pos
    switch (lead)
    {
    case '+': // simple string -> {ok=...}
        lua_createtable(L, 0, 1);
        lua_pushlstring(L, line.ptr, line.length);
        lua_setfield(L, -2, "ok");
        break;
    case '-': // error (nested) -> {err=...}
        lua_createtable(L, 0, 1);
        lua_pushlstring(L, line.ptr, line.length);
        lua_setfield(L, -2, "err");
        break;
    case ':': // integer
        lua_pushinteger(L, parseI(line));
        break;
    case ',': // RESP3 double -> {double=n}
        lua_createtable(L, 0, 1);
        lua_pushnumber(L, parseF(line));
        lua_setfield(L, -2, "double");
        break;
    case '(': // RESP3 big number -> {big_number="..."}
        lua_createtable(L, 0, 1);
        lua_pushlstring(L, line.ptr, line.length);
        lua_setfield(L, -2, "big_number");
        break;
    case '#': // RESP3 boolean
        lua_pushboolean(L, line.length && line[0] == 't');
        break;
    case '_': // RESP3 null -> Lua nil (re-emits as the client's null, unlike
        lua_pushnil(L); // a #f boolean which round-trips as #f)
        break;
    case '$': // bulk string ($-1 -> false)
        {
            auto n = parseI(line);
            if (n < 0)
            {
                lua_pushboolean(L, 0);
                break;
            }
            auto s = b[pos .. pos + cast(size_t) n];
            pos += cast(size_t) n + 2; // content + CRLF
            lua_pushlstring(L, s.ptr, s.length);
            break;
        }
    case '=': // RESP3 verbatim -> {format="txt", string="..."} (re-emittable)
        {
            auto n = parseI(line);
            auto s = b[pos .. pos + cast(size_t) n];
            pos += cast(size_t) n + 2;
            const(char)[] fmt = "txt", payload = s;
            if (s.length >= 4 && s[3] == ':')
            {
                fmt = s[0 .. 3];
                payload = s[4 .. $];
            }
            lua_createtable(L, 0, 2);
            lua_pushlstring(L, fmt.ptr, fmt.length);
            lua_setfield(L, -2, "format");
            lua_pushlstring(L, payload.ptr, payload.length);
            lua_setfield(L, -2, "string");
            break;
        }
    case '*': // array (*-1 -> false)
    case '>': // RESP3 push -> array
    case '~': // RESP3 set -> {set={member=true,...}}
        {
            auto n = parseI(line);
            if (n < 0)
            {
                lua_pushboolean(L, 0);
                break;
            }
            if (lead == '~')
            {
                lua_createtable(L, 0, 1);
                lua_createtable(L, 0, cast(int) n); // the set members table
                foreach (_; 0 .. n)
                {
                    respBytesToLua(L, b, pos); // member (key)
                    lua_pushboolean(L, 1);
                    lua_rawset(L, -3);
                }
                lua_setfield(L, -2, "set");
            }
            else
            {
                lua_createtable(L, cast(int) n, 0);
                foreach (i; 0 .. n)
                {
                    respBytesToLua(L, b, pos);
                    lua_rawseti(L, -2, cast(long) i + 1);
                }
            }
            break;
        }
    case '|': // RESP3 attribute -> skipped (not exposed to scripts); the real
        {         // reply follows, so parse and return THAT
            auto n = parseI(line); // pair count
            foreach (_; 0 .. n * 2)
            {
                respBytesToLua(L, b, pos); // parse each attr child...
                lua_settop(L, lua_gettop(L) - 1); // ...and discard it
            }
            respBytesToLua(L, b, pos); // the actual reply
            break;
        }
    case '%': // RESP3 map -> {map={k=v,...}}
        {
            auto n = parseI(line); // pair count
            lua_createtable(L, 0, 1);
            lua_createtable(L, 0, cast(int) n);
            foreach (_; 0 .. n)
            {
                respBytesToLua(L, b, pos); // key
                respBytesToLua(L, b, pos); // value
                lua_rawset(L, -3);
            }
            lua_setfield(L, -2, "map");
            break;
        }
    default:
        lua_pushboolean(L, 0);
        break;
    }
}

private const(char)[] readLine(scope const(char)[] b, ref size_t pos) nothrow @nogc
{
    size_t start = pos;
    while (pos < b.length && b[pos] != '\r')
        pos++;
    auto s = b[start .. pos];
    pos += 2; // skip CRLF
    return s;
}

private long parseI(scope const(char)[] s) nothrow @nogc
{
    long v = 0;
    bool neg = s.length && s[0] == '-';
    foreach (c; s[neg ? 1 : 0 .. $])
        if (c >= '0' && c <= '9')
            v = v * 10 + (c - '0');
    return neg ? -v : v;
}

private double parseF(scope const(char)[] s) nothrow @nogc
{
    import core.stdc.stdlib : strtod;

    char[64] buf = void;
    if (s.length >= buf.length)
        return 0;
    buf[0 .. s.length] = s[];
    buf[s.length] = 0;
    return strtod(buf.ptr, null);
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
        scope const(RVal)[] arr, bool isWrite, ulong clock, ulong userId, int respLevel,
        ref Arena arena, ref ByteBuffer reply, ref ByteBuffer effScratch) nothrow
{
    import dreads.resp : gRespProto;

    // Each redis.call is a processed command on the caller's connection (Redis
    // reflects this in the client's tot-cmds / command stats).
    if (gScriptCallerCmds !is null)
        (*gScriptCallerCmds)++;

    // ACL: a redis.call must obey the CALLER's permissions, not the fact that
    // the caller was allowed to run EVAL. `userId` rides in from the connection
    // (invisible to Lua) — resolve it here on the writer thread and enforce
    // command/key/channel exactly like the top-level. This is why EVAL/FCALL need
    // no key-spec of their own: each redis.call round-trips here as a real command
    // (SET, GET, …) and is checked transparently, keys and all.
    if (userId != 0 && arr.length > 0)
    {
        import dreads.acl : aclUserById, aclUnrestricted, aclCanRunCmd, aclCmdIndex,
            aclDeniedKey, aclCanAccessChannel, aclLogAdd;
        import dreads.stream : nowMs;

        auto u = aclUserById(userId);
        if (u !is null && !aclUnrestricted(u))
        {
            // an ACL denial inside a script is logged with the "lua" context and
            // the calling command (eval); the object is the offending command/
            // key/channel from the redis.call.
            static void logDenial(string reason, const(char)[] obj,
                    const(char)[] uname) @trusted nothrow @nogc
            {
                static ByteBuffer ci; // TLS
                ci.clear();
                ci.append("id=0 addr=? name= cmd=eval");
                aclLogAdd(reason, "lua", obj, uname, cast(const(char)[]) ci.data, nowMs());
            }

            auto nm = arr[0].str;
            char[32] lbuf = void;
            if (nm.length <= lbuf.length)
            {
                foreach (i, ch; nm)
                    lbuf[i] = (ch >= 'A' && ch <= 'Z') ? cast(char)(ch + 32) : ch;
                auto lname = cast(const(char)[]) lbuf[0 .. nm.length];
                if (!aclCanRunCmd(u, aclCmdIndex(lname)))
                {
                    logDenial("command", lname, u.name);
                    reply.clear();
                    static ByteBuffer eb; // TLS scratch for the message text
                    eb.clear();
                    eb.append("NOPERM User ");
                    eb.append(u.name);
                    eb.append(" has no permissions to run the '");
                    eb.append(lname);
                    eb.append("' command");
                    repError(reply, cast(const(char)[]) eb.data);
                    return 0;
                }
                if (!u.root.allKeys)
                {
                    auto dk = aclDeniedKey(u, lname, arr);
                    if (dk)
                    {
                        logDenial("key", dk, u.name);
                        reply.clear();
                        repError(reply, "NOPERM No permissions to access a key");
                        return 0;
                    }
                }
                if (!u.root.allChannels && arr.length >= 2
                        && (lname == "publish" || lname == "spublish")
                        && !aclCanAccessChannel(u, arr[1].str))
                {
                    logDenial("channel", arr[1].str, u.name);
                    reply.clear();
                    repError(reply, "NOPERM No permissions to access a channel");
                    return 0;
                }
            }
        }
    }

    // run the command at the script's RESP level so its reply (e.g. DEBUG
    // PROTOCOL, HGETALL) is framed the way redis.call should see it; restore
    // the client's level right after (dispatch of reads doesn't yield)
    auto savedProto = gRespProto;
    gRespProto = respLevel;
    scope (exit)
        gRespProto = savedProto;

    reply.clear();

    // Pipeline-top stats + deny-OOM for the sub-command, on the MAIN thread (a
    // redis.call round-trips here), so both are counted like any top-level command
    // and the errorstat HashMap is never touched from the Lua worker.
    import dreads.stats : statCall, statRejected, statErrorReply, gTotalErrorReplies;
    import dreads.acl : aclCmdIndex;
    import dreads.commands : isDenyOomCommand, isPausedByWrite;

    int cidx = -1;
    {
        char[16] up = void, lo = void;
        auto nm = arr.length ? arr[0].str : null;
        if (nm.length && nm.length <= up.length)
        {
            foreach (i, ch; nm)
            {
                up[i] = ch >= 'a' && ch <= 'z' ? cast(char)(ch - 32) : ch;
                lo[i] = ch >= 'A' && ch <= 'Z' ? cast(char)(ch + 32) : ch;
            }
            cidx = aclCmdIndex(cast(const(char)[]) lo[0 .. nm.length]);
            auto ucName = cast(const(char)[]) up[0 .. nm.length];
            // RO-write: a read-only script (EVAL_RO / no-writes) can't run a write
            // or a may-replicate command (PUBLISH/PFCOUNT). Covers reads too, so it
            // sits before the isWrite-gated deny-OOM check.
            if (gScriptReadOnly && isPausedByWrite(ucName))
            {
                enum ro = "ERR Write commands are not allowed from read-only scripts.";
                repError(reply, ro);
                statRejected(cidx);
                statErrorReply(ro);
                return 0;
            }
            if (isWrite && isDenyOomCommand(ucName)
                && !gScriptAllowOom && !gScriptWrote && scriptOverMaxmemory())
            {
                enum oom = "OOM command not allowed when used memory > 'maxmemory'.";
                repError(reply, oom);
                statRejected(cidx); // refused before running
                statErrorReply(oom);
                return 0;
            }
        }
    }
    // Count the executed sub-command (a leaf: its error, if any, is the real one).
    immutable subPrev = gTotalErrorReplies;
    scope (exit)
        if (cidx >= 0)
        {
            auto rd = reply.data;
            immutable errored = rd.length > 0 && rd[0] == '-';
            if (errored && gTotalErrorReplies == subPrev)
                statErrorReply(cast(const(char)[]) rd);
            statCall(cidx, errored);
        }

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
        auto dbIdx = cast(size_t) ks.db;
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
    ulong userId; // the calling ACL user (0 = no enforcement)
    int clientResp; // the client's RESP level (final reply framing on worker)
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
    ulong userId; // calling ACL user (0 = no enforcement), rides the round-trip
    bool isWrite;
    int resp; // gRespProto to run the command at (script's setresp)
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
        gCtx.userId = gPoolScriptUser;
    }
    else
    {
        gCtx.ks = ks;
        gCtx.viaPool = false;
        gCtx.db = 0;
        gCtx.clock = detNow();
        gCtx.userId = gPendingScriptUser;
    }
}

/// Bridge (Lua thread): post one command to the main thread and await its
/// reply. Returns the execute status; the RESP reply lands in `reply`.
private int poolRoundTrip(scope const(RVal)[] arr, bool isWrite, ref ByteBuffer reply) nothrow
{
    encodeCmd(gCmdSlot.bytes, arr);
    gCmdSlot.db = gCtx.db;
    gCmdSlot.clock = gCtx.clock;
    gCmdSlot.userId = gCtx.userId;
    gCmdSlot.isWrite = isWrite;
    gCmdSlot.resp = gScriptResp;
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
            cast(void) gCmdSlot.done.waitUninterruptible(ec);
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

/// Stop and join the Lua worker thread. The pool's worker is a NON-daemon thread
/// running an infinite drain loop, so without this druntime blocks on it when
/// main() returns — the server appears to hang for seconds on SIGTERM. Call once
/// at shutdown, after the event loop has returned.
public void shutdownLuaScriptPool() nothrow
{
    if (!gLuaPoolUp || gLuaPool is null)
        return;
    gLuaPoolUp = false;
    try
        (cast(TaskPool) gLuaPool).terminate(); // stops the pool loop and joins the thread
    catch (Exception)
    {
    }
}

/// Main-side: hand a script request to the Lua thread and await its reply.
public void luaExecOnPool(LuaReqKind kind, scope const(RVal)[] args, bool bySha,
        bool readOnly, ushort db, ulong clock, ulong userId, ref ByteBuffer o) nothrow
{
    auto slot = acquireReqSlot();
    slot.kind = kind;
    slot.bySha = bySha;
    slot.readOnly = readOnly;
    slot.db = db;
    slot.clock = clock;
    slot.userId = userId; // captured here (main thread) before the hand-off
    slot.clientResp = gRespProto; // main-thread TLS: this connection's level
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

    auto dbi = cast(size_t) ks.db;
    if (dbi >= NUM_DBS)
        dbi = 0;
    luaExecOnPool(kind, args, bySha, readOnly, cast(ushort) dbi, detNow(), gPendingScriptUser, o);
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
                    gPoolScriptUser = slot.userId;
                    gRespProto = slot.clientResp; // frame the final reply here
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
                            slot.isWrite, slot.clock, slot.userId, slot.resp, arena, slot.reply, eff);
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
    // conversion and emits this sentinel (the reply so far is already framed).
    // Each level also pushes onto the Lua stack, which the C API does NOT grow
    // on its own: past the reserved slots lua_rawgeti would write out of bounds
    // and silently corrupt the shared state, so reserve room before descending
    // and treat exhaustion as the same limit.
    if (depth > 1000 || lua_checkstack(L, 4) == 0)
    {
        repError(o, "ERR reached lua stack limit");
        return;
    }
    switch (lua_type(L, idx))
    {
    case LUA_TNIL:
        repNullBulk(o);
        break;
    case LUA_TBOOLEAN:
        // at setresp(3) booleans are real RESP3 booleans (#t/#f, or :1/:0
        // to a RESP2 client); the legacy setresp(2) rule is true->:1, false->nil
        if (gScriptResp >= 3)
            repBool(o, lua_toboolean(L, idx) != 0);
        else if (lua_toboolean(L, idx))
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
                auto msg = p[0 .. len];
                // a returned {err=...} without a CODE gets the default ERR
                // (redis.error_reply("") -> "ERR"); a coded one surfaces verbatim
                if (!startsWithCode(msg))
                {
                    static ByteBuffer eb; // TLS scratch
                    eb.clear();
                    eb.append("ERR");
                    if (msg.length)
                    {
                        eb.appendByte(' ');
                        eb.append(msg);
                    }
                    repError(o, cast(const(char)[]) eb.data);
                }
                else
                    repError(o, msg);
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
            // {format="txt", string="..."} -> RESP3 verbatim / RESP2 bulk
            lua_pushlstring(L, "format".ptr, 6);
            if (lua_rawget(L, idx) == LUA_TSTRING)
            {
                size_t fl;
                auto fp = lua_tolstring(L, -1, &fl);
                char[3] fmt = ['t', 'x', 't'];
                if (fl >= 3)
                    fmt[] = fp[0 .. 3];
                lua_settop(L, lua_gettop(L) - 1);
                lua_pushlstring(L, "string".ptr, 6);
                if (lua_rawget(L, idx) == LUA_TSTRING)
                {
                    size_t sl;
                    auto sp = lua_tolstring(L, -1, &sl);
                    repVerbatim(o, fmt[], sp[0 .. sl]);
                    lua_settop(L, lua_gettop(L) - 1);
                    break;
                }
                lua_settop(L, lua_gettop(L) - 1);
            }
            else
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
                // a big number is a single logical line: CR/LF in the value
                // (malformed input) is sanitized to spaces, like Redis, so it
                // can't desync the reply stream
                static ByteBuffer bnb; // TLS
                bnb.clear();
                foreach (ch; bp[0 .. bl])
                    bnb.appendByte(ch == '\r' || ch == '\n' ? ' ' : ch);
                repBigNumber(o, cast(const(char)[]) bnb.data);
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
            // {set={member=true,...}} -> RESP3 set / RESP2 array
            lua_pushlstring(L, "set".ptr, 3);
            if (lua_rawget(L, idx) == LUA_TTABLE)
            {
                int setIdx = lua_gettop(L);
                size_t members = 0;
                lua_pushnil(L);
                while (lua_next(L, setIdx) != 0)
                {
                    members++;
                    lua_settop(L, lua_gettop(L) - 1);
                }
                repSetHeader(o, members);
                lua_pushnil(L);
                while (lua_next(L, setIdx) != 0)
                {
                    luaToResp(L, lua_gettop(L) - 1, o, depth + 1); // the member key
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
    // Freeze the clock to wall time at script start: the whole EVAL runs against
    // this instant (in-script TIME is deterministic, relative TTLs resolve to
    // it). EVAL is handled at the server layer, which — unlike dispatch — does
    // not freeze per command, so without this the script would inherit a stale
    // gClock from the previous command and misjudge already-expired keys.
    import dreads.det : freezeClock;

    freezeClock(0);
    if (routeToPool(LuaReqKind.eval, args, ks, o, bySha, readOnly))
        return;
    if (args.length < 2)
    {
        repError(o, bySha ? "ERR wrong number of arguments for 'evalsha' command"
                : "ERR wrong number of arguments for 'eval' command");
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
    gScriptSource[0 .. 40] = sha[]; // tag client-facing errors with this source
    gScriptSourceLen = 40;

    // optional `#!lua [flags=...]` shebang: validate + strip before compiling,
    // and honour no-writes (the other flags are accepted for compatibility)
    bool shebangNoWrites, shebangAllowOom, hasShebang;
    if (!parseEvalShebang(body_, o, shebangNoWrites, shebangAllowOom, hasShebang))
        return;
    if (shebangNoWrites)
        readOnly = true;
    // OOM policy for this run: read-only and allow-oom (or no-writes, which
    // implies it) scripts bypass the deny-oom check; the per-command bridge
    // check uses gScriptAllowOom. A script that declares a shebang WITHOUT
    // allow-oom is refused outright while over maxmemory, regardless of its
    // body (Redis's "default flags in OOM" contract); legacy no-shebang
    // scripts still run and only their deny-oom writes fail.
    gScriptAllowOom = shebangAllowOom || shebangNoWrites || readOnly;
    if (hasShebang && !gScriptAllowOom && scriptOverMaxmemory())
    {
        repError(o, "OOM command not allowed when used memory > 'maxmemory'.");
        return;
    }

    if (!ensureState())
    {
        repError(o, "ERR failed to initialize Lua");
        return;
    }

    // KEYS / ARGV globals. _G is sealed read-only, so lift the seal for exactly
    // these two writes (Redis toggles readonly around its KEYS/ARGV setup the same
    // way). The KEYS/ARGV tables themselves are fresh and writable here.
    lua_getglobal(gL, "_G");
    lua_enablereadonlytable(gL, -1, 0);
    lua_pop(gL, 1);
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
    lua_getglobal(gL, "_G");
    lua_enablereadonlytable(gL, -1, 1);
    lua_pop(gL, 1);

    gCtx.arena = &arena;
    gCtx.readOnly = readOnly;
    gScriptReadOnly = readOnly; // pipeline-top RO-write gate reads this on main
    bindBridgeContext(&ks); // sets ks/viaPool/db/clock for inline or pool mode
    gScriptResp = 2; // redis.setresp default per script
    gScriptWrote = false; // per-EVAL: the server signals WATCH/blocked wakes
    // fresh RNG seed per run: effects replication means the RNG need not be
    // deterministic across runs (see nextScriptRandSeed)
    lua_getglobal(gL, "math");
    lua_getfield(gL, -1, "randomseed");
    lua_pushinteger(gL, cast(long) nextScriptRandSeed());
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
        // reload from cached bytecode (fast) or compile the source, caching bytecode
        if (!loadUserChunk(gL, sha[], body_, "@user_script"))
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
            luaErrToResp(o, "ERR ", true); // client-facing: tag with ` script: <src>`
            return;
        }
    }
    luaToResp(gL, lua_gettop(gL), o);
}

/// -<prefix><lua error message>, CRLF-sanitized. A {err=...} error object
/// (a failing redis.call raised inside the script) surfaces VERBATIM, like
/// Redis: the caller sees the original command error, not a wrapper.
private void luaErrToResp(ref ByteBuffer o, scope const(char)[] prefix,
    bool withSource = false) nothrow
{
    size_t len;
    o.appendByte('-');
    bool wrote = false;
    if (lua_type(gL, -1) == LUA_TTABLE)
    {
        lua_pushlstring(gL, "err".ptr, 3);
        immutable isStr = lua_rawget(gL, -2) == LUA_TSTRING;
        if (isStr)
        {
            auto ep = lua_tolstring(gL, -1, &len);
            auto msg = ep[0 .. len];
            // a value already carrying a CODE ("WRONGTYPE ...", "MY_ERR x")
            // surfaces verbatim; a bare/empty message gets the default ERR code
            if (!startsWithCode(msg))
                o.append("ERR ");
            foreach (c; msg)
                o.appendByte(c == '\r' || c == '\n' ? ' ' : c);
            wrote = true;
        }
        lua_settop(gL, lua_gettop(gL) - 1);
        if (!isStr) // error({}) or an error object without an 'err' string
        {
            o.append("ERR unknown error");
            wrote = true;
        }
    }
    else
    {
        auto p = lua_tolstring(gL, -1, &len);
        if (p !is null && len)
        {
            // an error already carrying a CODE surfaces verbatim; a raw Lua error
            // (e.g. `error('msg')` -> "user_script:1: msg") gets the prefix
            if (!startsWithCode(p[0 .. len]))
                o.append(prefix);
            foreach (c; p[0 .. len])
                o.appendByte(c == '\r' || c == '\n' ? ' ' : c);
            wrote = true;
        }
    }
    if (!wrote)
        o.append("ERR unknown error");
    // tag where a client-facing script error came from (Valkey's ` script: ...`)
    if (withSource && gScriptSourceLen)
    {
        o.append(" script: ");
        o.append(gScriptSource[0 .. gScriptSourceLen]);
        o.append(", on @user_script:1.");
    }
    o.append("\r\n");
}

/// True when the message opens with an uppercase CODE followed by a space.
private bool startsWithCode(scope const(char)[] m) nothrow @nogc
{
    // the code is the first word if it is UPPERCASE (letters/digits/_) and
    // followed by a space, e.g. "WRONGTYPE ...", "MY_ERR_CODE custom msg"
    size_t i = 0;
    while (i < m.length && ((m[i] >= 'A' && m[i] <= 'Z') || m[i] == '_'
            || (m[i] >= '0' && m[i] <= '9')))
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

        luaExecOnPool(LuaReqKind.script, args, false, false, 0, detNow(), gPendingScriptUser, o);
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
            // Strip + validate the optional `#!lua` shebang before compiling, exactly
            // as EVAL does. We CACHE the original body (with shebang) so EVALSHA
            // re-parses it identically; only the compile check sees the stripped body.
            const(char)[] body_ = args[1].str;
            bool nw, ao, hs;
            if (!parseEvalShebang(body_, o, nw, ao, hs))
                return; // parseEvalShebang wrote the error reply
            char[40] sha = void;
            sha1Hex(args[1].str, sha);
            // compile (text only) to validate + capture the bytecode for fast reload
            if (!loadUserChunk(gL, sha[], body_, "@user_script"))
            {
                luaErrToResp(o, "ERR Error compiling script: ");
                lua_settop(gL, 0);
                return;
            }
            lua_settop(gL, 0);
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
            // optional ASYNC/SYNC mode: dreads flushes synchronously either way
            gScripts.clear();
            gScriptBc.clear(); // drop the cached bytecode alongside the source
            if (gL !is null) // drop the compiled-chunk cache too
            {
                lua_createtable(gL, 0, 16);
                lua_setfield(gL, LUA_REGISTRYINDEX, "dreads_scripts");
            }
            repSimple(o, "OK");
            return;
        }
    case "SHOW":
        {
            // SCRIPT SHOW <sha>: return the cached body, or NOSCRIPT when the
            // sha is malformed (not 40 hex chars) or not in the cache
            char[40] sha = void;
            bool valid = args.length == 2 && args[1].str.length == 40;
            if (valid)
            {
                foreach (i, c; args[1].str)
                {
                    char lc = c >= 'A' && c <= 'Z' ? cast(char)(c + 32) : c;
                    if (!((lc >= '0' && lc <= '9') || (lc >= 'a' && lc <= 'f')))
                    {
                        valid = false;
                        break;
                    }
                    sha[i] = lc;
                }
            }
            auto v = valid ? gScripts.get(sha[]) : null;
            if (v is null)
            {
                repError(o, "NOSCRIPT No matching script. Please use EVAL.");
                return;
            }
            repBulk(o, v.rawView());
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
    // text only: a FUNCTION library body that is actually Lua bytecode is refused
    if (luaL_loadbufferx(gL, body_.ptr, body_.length, "@user_function", "t") != LUA_OK)
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

/// EVAL shebang (`#!lua [flags=...]`): validates the engine and flags, strips
/// the line off `body_`, and reports which run flags were set. Returns false and
/// frames the error reply when the shebang is malformed. No shebang => true, no
/// change. Recognized flags: no-writes, allow-oom, allow-stale,
/// allow-cross-slot-keys (only no-writes changes behaviour here; the others are
/// accepted for compatibility).
private bool parseEvalShebang(ref const(char)[] body_, ref ByteBuffer o,
        out bool noWrites, out bool allowOom, out bool hasShebang) nothrow @nogc
{
    noWrites = false;
    allowOom = false;
    hasShebang = false;
    if (body_.length < 2 || body_[0] != '#' || body_[1] != '!')
        return true;
    hasShebang = true;
    size_t eol = 0;
    while (eol < body_.length && body_[eol] != '\n')
        eol++;
    auto line = body_[2 .. eol];
    if (line.length && line[$ - 1] == '\r')
        line = line[0 .. $ - 1];

    size_t p = 0;
    const(char)[] nextTok() nothrow @nogc
    {
        while (p < line.length && (line[p] == ' ' || line[p] == '\t'))
            p++;
        size_t s = p;
        while (p < line.length && line[p] != ' ' && line[p] != '\t')
            p++;
        return line[s .. p];
    }

    static ByteBuffer eb; // TLS: error message scratch
    void fail(string prefix, const(char)[] what, string suffix = "") nothrow @nogc
    {
        eb.clear();
        eb.append("ERR ");
        eb.append(prefix);
        eb.append(what);
        eb.append(suffix);
        repError(o, cast(const(char)[]) eb.data);
    }

    auto engine = nextTok();
    if (engine != "lua")
    {
        fail("Could not find scripting engine named '", engine, "'");
        return false;
    }
    for (;;)
    {
        auto opt = nextTok();
        if (opt.length == 0)
            break;
        size_t eq = 0;
        while (eq < opt.length && opt[eq] != '=')
            eq++;
        if (opt[0 .. eq] != "flags" || eq >= opt.length)
        {
            fail("Unknown lua shebang option: ", opt[0 .. eq]);
            return false;
        }
        auto flags = opt[eq + 1 .. $];
        size_t fs = 0;
        while (fs <= flags.length)
        {
            size_t fe = fs;
            while (fe < flags.length && flags[fe] != ',')
                fe++;
            auto f = flags[fs .. fe];
            if (f.length)
            {
                if (f == "no-writes")
                    noWrites = true;
                else if (f == "allow-oom")
                    allowOom = true;
                else if (f == "allow-stale" || f == "allow-cross-slot-keys")
                {
                    // accepted, no behavioural change here
                }
                else
                {
                    fail("Unexpected flag in script shebang: ", f);
                    return false;
                }
            }
            if (fe >= flags.length)
                break;
            fs = fe + 1;
        }
    }
    body_ = eol < body_.length ? body_[eol + 1 .. $] : body_[$ .. $];
    return true;
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
    // freeze the clock to wall time at call start (see evalCommand)
    import dreads.det : freezeClock;

    freezeClock(0);
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
    gScriptReadOnly = readOnly; // pipeline-top RO-write gate reads this on main
    bindBridgeContext(&ks);
    gScriptResp = 2;
    gScriptWrote = false;
    gScriptAllowOom = readOnly; // no-writes/RO functions bypass the deny-oom gate
    // fresh RNG seed per run, script deadline, state recycling — the same run
    // discipline as EVAL
    lua_getglobal(gL, "math");
    lua_getfield(gL, -1, "randomseed");
    lua_pushinteger(gL, cast(long) nextScriptRandSeed());
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
    // a returned error without a CODE gets the default ERR prefix (Valkey 7)
    assert(evalRun(ks, "return redis.error_reply('bad thing')") == "-ERR bad thing\r\n");
    assert(evalRun(ks, "return redis.error_reply('WRONGTYPE nope')") == "-WRONGTYPE nope\r\n");
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

unittest // ACL: redis.call obeys the caller's permissions, not just +eval
{
    import dreads.acl : aclInit, aclGetOrCreate, aclApplyRule, aclDelUser;
    import std.algorithm : canFind;

    Keyspace ks;
    scope (exit)
        ks.d.free();

    aclInit();
    auto u = aclGetOrCreate("scripttest");
    const(char)[] err;
    foreach (rule; ["on", "~*", "+eval", "+get"]) // note: NO +set
        assert(aclApplyRule(u, rule, err), rule ~ " => " ~ cast(string) err);

    // inline path reads the pending user; set it for the duration of the calls
    scriptSetPendingUser(u.id);
    scope (exit)
    {
        scriptSetPendingUser(0);
        aclDelUser("scripttest");
    }

    // a permitted command inside the script still works...
    assert(evalRun(ks, "return redis.call('GET', 'k')") == "$-1\r\n");
    // ...but SET is refused even though the user could run EVAL (no leak)
    auto blocked = evalRun(ks, "return redis.call('SET', 'k', 'v')");
    assert(blocked[0] == '-' && blocked.canFind("NOPERM") && blocked.canFind("'set'"), blocked);
    // redis.pcall surfaces the same NOPERM as a Lua error table
    auto viaPcall = evalRun(ks, "local e = redis.pcall('SET','k','v'); return e.err");
    assert(viaPcall.canFind("NOPERM"), viaPcall);
}

unittest // errors
{
    Keyspace ks;
    scope (exit)
        ks.d.free();
    import std.algorithm : canFind;

    auto compile = evalRun(ks, "this is not lua");
    assert(compile[0 .. 28] == "-ERR Error compiling script:");
    // a runtime error surfaces as `-ERR <lua msg> script: <src>` (Valkey-style)
    auto runtime = evalRun(ks, "error('boom')");
    assert(runtime[0 .. 5] == "-ERR " && runtime.canFind("boom")
            && runtime.canFind(" script: "), runtime);
}

unittest // sandbox: _G protection, pruned globals, recursion guard
{
    import std.algorithm : canFind;

    Keyspace ks;
    scope (exit)
        ks.d.free();

    // _G is read-only: creating a global, replacing its metatable, or reaching
    // through getmetatable(_G) all raise "Attempt to modify a readonly table".
    // Stock Lua lets setmetatable(_G,{}) succeed, which would also silently
    // wipe the protection for every later script sharing the state — guard it.
    foreach (trick; [
            "x = 1", "_G = {}", "redis = function() return 1 end",
            "setmetatable(_G, {})", "local g = getmetatable(_G); g.__index = {}"
        ])
    {
        auto r = evalRun(ks, trick);
        assert(r[0] == '-' && r.canFind("readonly table"), trick ~ " => " ~ r);
    }
    // the guard must not have broken legitimate setmetatable on a script table
    assert(evalRun(ks,
            "local t = setmetatable({}, {__index=function() return 5 end}); return t.foo")
            == ":5\r\n");
    // and the wipe attempt above must not have poisoned undefined-global reads
    foreach (g; ["loadfile", "dofile", "print", "load"])
    {
        auto r = evalRun(ks, g ~ "('x')");
        assert(r.canFind("nonexistent global variable '" ~ g ~ "'"), g ~ " => " ~ r);
    }

    // deep self-referential tables are capped, not a C-stack overflow, and the
    // capped conversion must leave the shared state intact for the next script
    auto rec = evalRun(ks, "local a = {}; local b = {a}; a[1] = b; return a");
    assert(rec.canFind("ERR reached lua stack limit"), rec);
    assert(evalRun(ks, "return string.upper('ok')") == "$2\r\nOK\r\n");
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

unittest // SCRIPT SHOW, cached-script count, EVAL numkeys errors
{
    import std.algorithm : canFind;

    Keyspace ks;
    scope (exit)
        ks.d.free();
    ByteBuffer o;

    static RVal bs(string s)
    {
        RVal v;
        v.type = RType.BulkString;
        v.str = s;
        return v;
    }

    // flush, then a known body -> known sha (matches the blackbox fixture)
    scriptCommand([bs("FLUSH")], o);
    o.clear();
    assert(cachedScriptCount() == 0);
    scriptCommand([bs("LOAD"), bs("return 'dump'")], o);
    auto sha = (cast(string) o.data)[5 .. 45].idup; // skip "$40\r\n"
    o.clear();
    assert(cachedScriptCount() == 1);

    // SHOW returns the body for a cached sha
    scriptCommand([bs("SHOW"), bs(sha)], o);
    assert(cast(string) o.data == "$13\r\nreturn 'dump'\r\n", cast(string) o.data);
    o.clear();
    // NOSCRIPT for a wrong-length sha, an invalid-char sha, and an unknown sha
    foreach (bad; [
            "b534286061d4b06c06015ae8", "ZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZ",
            "0000000000000000000000000000000000000000"
        ])
    {
        scriptCommand([bs("SHOW"), bs(bad)], o);
        assert((cast(string) o.data)[0 .. 9] == "-NOSCRIPT", cast(string) o.data);
        o.clear();
    }

    // numkeys errors: non-integer vs negative are distinct messages
    Arena arena;
    evalCommand([bs("return 1"), bs("notanint")], ks, o, arena, false);
    assert(cast(string) o.data == "-ERR value is not an integer or out of range\r\n");
    o.clear();
    evalCommand([bs("return 1"), bs("-1")], ks, o, arena, false);
    assert(cast(string) o.data == "-ERR Number of keys can't be negative\r\n");
}

unittest // EVAL shebang: engine + flag validation, no-writes enforcement
{
    import std.algorithm : canFind;

    Keyspace ks;
    scope (exit)
        ks.d.free();
    ByteBuffer o;
    Arena arena;

    static RVal bs(string s)
    {
        RVal v;
        v.type = RType.BulkString;
        v.str = s;
        return v;
    }

    // a valid #!lua shebang is stripped and the body runs
    evalCommand([bs("#!lua\nreturn 1"), bs("0")], ks, o, arena, false);
    assert(cast(string) o.data == ":1\r\n", cast(string) o.data);
    o.clear();
    // wrong engine / unknown option / unknown flag each get their own message
    evalCommand([bs("#!not-lua\nreturn 1"), bs("0")], ks, o, arena, false);
    assert((cast(string) o.data).canFind("Could not find scripting engine"), cast(string) o.data);
    o.clear();
    evalCommand([bs("#!lua badger=data\nreturn 1"), bs("0")], ks, o, arena, false);
    assert((cast(string) o.data).canFind("Unknown lua shebang option"), cast(string) o.data);
    o.clear();
    evalCommand([bs("#!lua flags=allow-oom,what?\nreturn 1"), bs("0")], ks, o, arena, false);
    assert((cast(string) o.data).canFind("Unexpected flag in script shebang"), cast(string) o.data);
    o.clear();
    // no-writes turns the run read-only: a write raises, a read is fine
    evalCommand([bs("#!lua flags=no-writes\nreturn redis.call('set','k','v')"), bs("1"), bs("k")],
            ks, o, arena, false);
    assert((cast(string) o.data).canFind("read-only script"), cast(string) o.data);
    o.clear();
    evalCommand([bs("#!lua flags=no-writes\nreturn 7"), bs("0")], ks, o, arena, false);
    assert(cast(string) o.data == ":7\r\n", cast(string) o.data);
}

unittest // deny-oom enforcement in scripts
{
    import std.algorithm : canFind;
    import dreads.config : gConfig;

    Keyspace ks;
    scope (exit)
        ks.d.free();
    ByteBuffer o;
    Arena arena;

    static RVal bs(string s)
    {
        RVal v;
        v.type = RType.BulkString;
        v.str = s;
        return v;
    }

    auto savedMax = gConfig.maxmemory;
    gConfig.maxmemory = 1; // used_memory is always over 1 byte
    scope (exit)
        gConfig.maxmemory = savedMax;

    // legacy (no shebang): the deny-oom write fails, a read still runs
    evalCommand([bs("return redis.call('set','k','v')"), bs("1"), bs("k")], ks, o, arena, false);
    assert((cast(string) o.data).canFind("OOM command not allowed"), cast(string) o.data);
    o.clear();
    evalCommand([bs("redis.call('get','k'); return 5"), bs("1"), bs("k")], ks, o, arena, false);
    assert(cast(string) o.data == ":5\r\n", cast(string) o.data);
    o.clear();
    // a shebang WITHOUT allow-oom is rejected outright, regardless of body
    evalCommand([bs("#!lua flags=\nreturn 1"), bs("0")], ks, o, arena, false);
    assert((cast(string) o.data).canFind("OOM command not allowed"), cast(string) o.data);
    o.clear();
    // allow-oom lets the write through
    evalCommand([bs("#!lua flags=allow-oom\nreturn redis.call('set','k','v')"), bs("1"), bs("k")],
            ks, o, arena, false);
    assert(cast(string) o.data == "+OK\r\n", cast(string) o.data);
}
