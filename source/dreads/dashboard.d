module dreads.dashboard;

// Compile-time optional: everything below is under `version(DreadsDashboard)` (on by
// default; the `no-dashboard` dub config leaves this module EMPTY — no code, and the
// embedded UI bundle is never imported). server.d's dashboard calls are gated by the
// same version, so a no-dashboard build has zero dashboard bytes.
version (DreadsDashboard):

// Built-in web dashboard — OPT-IN (`dashboard yes`, off by default). Runs on its
// OWN thread with an isolated vibe event loop, so it never shares fibers with the
// data-plane loop. Because the server runs under GC.disable (a collection would
// stop-the-world every thread, incl. the data plane), everything here is written
// @nogc: static/stack buffers, no per-request GC allocation. HTTP + WebSocket are
// hand-rolled on listenTCP (vibe-core has no HTTP server, and vibe-d's GC use is
// exactly what we must avoid).
//
// Phase 1 (this file, first increment): thread lifecycle + a minimal HTTP handler
// serving a placeholder page. Live metrics over WebSocket + the embedded React UI
// come next; the "no client, no watch" rule means the main loop only snapshots
// metrics while at least one dashboard client is connected.

import core.atomic : atomicOp, atomicLoad, atomicFence;
import core.time : seconds, msecs;

import vibe.core.net : listenTCP, TCPConnection, TCPListener;
import vibe.core.stream : IOMode;
import vibe.core.sync : createSharedManualEvent, ManualEvent;
import vibe.core.taskpool : TaskPool;

import std.conv : to; // CTFE only (Content-Length of the embedded bundle)

import dreads.config : gConfig;
import dreads.stream : nowMs;
import dreads.resp : RVal;
import dreads.obj : Keyspace;
import dreads.mem : Arena;

private shared TaskPool gDashPool;
private __gshared bool gDashUp;
private shared ManualEvent gDashStop; // emitted at shutdown to end the listener task

// Count of connected dashboard WebSocket clients. The main loop reads this to
// honour "no client, no watch": it only snapshots + pushes metrics while > 0.
package shared int gDashClients;

// Resolved dashboard listen port (dashboard-port, or the RESP port + 1 by default).
private __gshared ushort gDashPort;

// ---- metrics seqlock: single writer (main-loop timer) -> many readers (WS conns) ----
// A latest-value snapshot. The writer bumps gMSeq odd before writing and even after;
// a reader copies the buffer and retries if the seq moved (torn read). No ManualEvent
// (its emit isn't @nogc — an alloc under GC.disable would never be collected).
private __gshared ubyte[8192] gMBuf;
private __gshared size_t gMLen;
private shared uint gMSeq;

// Command-group call counts, summed from gCmdStats (indexed by aclCmdIndex). Grouped
// so the dashboard can chart strings/lists/streams/pubsub rates, not just SET/GET.
private immutable string[] STR_CMDS = ["set", "get", "setex", "psetex", "getset", "append",
        "incr", "decr", "incrby", "decrby", "mset", "mget", "setnx", "getdel", "getex"];
private immutable string[] LIST_CMDS = ["lpush", "rpush", "lpop", "rpop", "lset", "lrem",
        "linsert", "ltrim", "lpushx", "rpushx", "rpoplpush", "lmove", "blpop", "brpop"];
private immutable string[] STREAM_CMDS = ["xadd", "xdel", "xread", "xrange", "xrevrange",
        "xlen", "xtrim", "xreadgroup", "xack", "xclaim", "xautoclaim"];

private ulong sumCalls(const(string)[] names) @nogc nothrow
{
    import dreads.stats : gCmdStats;
    import dreads.acl : aclCmdIndex;

    ulong t = 0;
    foreach (n; names)
    {
        immutable i = aclCmdIndex(n);
        if (i >= 0 && i < cast(int) gCmdStats.length)
            t += gCmdStats[i].calls;
    }
    return t;
}

// Build the compact metrics JSON into `dst`; returns bytes written (0 on error).
private size_t buildMetricsJson(scope char[] dst, size_t channels, size_t patterns) @nogc nothrow
{
    import core.stdc.stdio : snprintf;
    import dreads.stats : gCmdStats;
    import dreads.obj : gDbs, NUM_DBS, gExpiredKeys, gEvictedKeys, gConnectedClients,
        gBlockedClients;
    import dreads.mem : usedMemory;
    import dreads.pubsub : gPubMessages;

    ulong total = gPubMessages; // publishes bypass gCmdStats; count them too
    foreach (ref s; gCmdStats)
        total += s.calls;
    ulong keys = 0;
    foreach (ref db; gDbs)
        keys += db.length;

    int n = snprintf(dst.ptr, dst.length,
        `{"t":%llu,"mem":%llu,"maxmem":%llu,"clients":%lld,"blocked":%lld,`
        ~ `"expired":%llu,"evicted":%llu,"cmds":%llu,"keys":%llu,"str":%llu,`
        ~ `"list":%llu,"stream":%llu,"pub":%llu,"channels":%zu,"patterns":%zu,"db":[`,
        cast(ulong) nowMs(), cast(ulong) usedMemory(), cast(ulong) gConfig.maxmemory,
        cast(long) gConnectedClients, cast(long) gBlockedClients, cast(ulong) gExpiredKeys,
        cast(ulong) gEvictedKeys, total, keys, sumCalls(STR_CMDS), sumCalls(LIST_CMDS),
        sumCalls(STREAM_CMDS), cast(ulong) gPubMessages, channels, patterns);
    if (n < 0)
        return 0;
    size_t p = n;
    foreach (i; 0 .. NUM_DBS)
    {
        int m = snprintf(dst.ptr + p, dst.length - p, i == 0 ? "%zu" : ",%zu", gDbs[i].length);
        if (m < 0 || p + m >= dst.length)
            break;
        p += m;
    }
    int e = snprintf(dst.ptr + p, dst.length - p, "]}");
    if (e > 0)
        p += e;
    return p;
}

// ---- command bridge: run a command on the MAIN (writer) thread ----
// The dashboard thread can't touch the keyspace directly (single-writer model), so
// admin/write actions round-trip to the main loop, exactly like a script's redis.call:
// post the RESP command to gDashCmdQ, the main-side drain executes it via
// executeScriptCommand (ACL bypassed — userId 0 — because the dashboard is already
// gated by opt-in + password + dashboard-write/-admin) and signals the reply back.
// A TaskMutex serializes the dashboard's single reused slot (admin ops are rare).
import dreads.mem : ByteBuffer, Arena;
import vibe.core.sync : TaskMutex;

private struct DashCmdSlot
{
    ByteBuffer bytes; // RESP command in
    ushort db;
    ByteBuffer reply; // RESP reply out
    int status;
    shared(ManualEvent) done;
    bool ready;
}

private __gshared void* gDashCmdQ; // dreads.raftq.CrossQueue (opaque here to avoid a cycle)
private __gshared DashCmdSlot gDashSlot;
private __gshared TaskMutex gDashCmdMutex;
private __gshared bool gDashBridgeUp;

/// Start the command bridge (called on the MAIN thread at boot when the dashboard
/// is enabled): creates the queue and runs the drain fiber on the main event loop.
public void startDashCmdBridge() nothrow
{
    import dreads.raftq : CrossQueue;
    import vibe.core.core : runTask;

    if (!gConfig.dashboard)
        return;
    try
    {
        gDashCmdQ = cast(void*) new CrossQueue(256);
        gDashSlot.done = createSharedManualEvent();
        gDashCmdMutex = new TaskMutex;
        gDashBridgeUp = true;
        runTask(() nothrow { dashCmdDrainLoop(); });
    }
    catch (Exception)
    {
    }
}

// The dashboard gate. Reads are always allowed (inspectors). Writes need
// dashboard-write; ACL admin and CONFIG SET need the higher dashboard-admin.
// Returns null to allow, else the RESP-less error message to reply with.
private const(char)[] dashDeny(scope const(RVal)[] arr) @nogc nothrow
{
    import dreads.commands : isWriteCommand;

    static char[32] ub = void;
    auto name = arr[0].str;
    if (name.length == 0 || name.length > ub.length)
        return "ERR dashboard: bad command";
    foreach (i, c; name)
        ub[i] = (c >= 'a' && c <= 'z') ? cast(char)(c - 32) : c;
    auto U = ub[0 .. name.length];

    if (U == "ACL")
        return gConfig.dashboardAdmin ? null
            : "ERR dashboard: ACL admin disabled (set dashboard-admin yes)";
    if (U == "CONFIG" && arr.length >= 2)
    {
        auto sub = arr[1].str;
        immutable isSet = sub.length == 3
            && (sub[0] | 32) == 's' && (sub[1] | 32) == 'e' && (sub[2] | 32) == 't';
        if (isSet && !gConfig.dashboardAdmin)
            return "ERR dashboard: CONFIG SET needs dashboard-admin yes";
    }
    if (isWriteCommand(U) && !gConfig.dashboardWrite)
        return "ERR dashboard: writes disabled (set dashboard-write yes)";
    return null;
}

// CONFIG SET is control-plane (not in the keyspace dispatch), so handle the one
// admin knob the dashboard needs — the maxmemory bump — directly. Value is bytes.
// Returns true if it consumed the command (reply written).
// PUBSUB is a server-layer command (not in the commands.d dispatch the bridge uses),
// so serve its read-only introspection here — on the main thread, over the same
// gPubSub the server does. Mirrors dashApplyConfig's intercept-before-dispatch pattern.
private bool dashPubsub(scope const(RVal)[] arr, ref ByteBuffer reply) nothrow
{
    import dreads.server : pubsubIntrospect;

    if (arr.length == 0 || !ciEq(arr[0].str, "pubsub"))
        return false;
    pubsubIntrospect(arr[1 .. $], reply);
    return true;
}

// EVAL/EVALSHA are server-layer scripting commands (not in the commands.d dispatch
// the bridge uses), so the Lua playground's EVAL returned "unknown command" over
// /api/exec. Run it in-place via evalCommand on the main thread (same call the normal
// EVAL path makes). A writing script needs dashboard-write; the _RO forms are always
// allowed. Effects propagate through the redis.call sink inside the script, as usual.
private bool dashEval(scope const(RVal)[] arr, ref Keyspace ks, ref ByteBuffer reply, ref Arena arena) nothrow
{
    import dreads.scripting : evalCommand;
    import dreads.resp : repError;

    if (arr.length == 0)
        return false;
    auto n = arr[0].str;
    bool bySha, readOnly, isEval;
    if (ciEq(n, "eval"))
        isEval = true;
    else if (ciEq(n, "evalsha"))
    {
        isEval = true;
        bySha = true;
    }
    else if (ciEq(n, "eval_ro"))
    {
        isEval = true;
        readOnly = true;
    }
    else if (ciEq(n, "evalsha_ro"))
    {
        isEval = true;
        bySha = true;
        readOnly = true;
    }
    if (!isEval)
        return false;
    if (!readOnly && !gConfig.dashboardWrite)
    {
        repError(reply, "ERR dashboard: writes disabled (set dashboard-write yes)");
        return true;
    }
    evalCommand(arr[1 .. $], ks, reply, arena, bySha, readOnly);
    return true;
}

// ACL is a server-layer command (not in commands.d dispatch), so route it to the
// focused dashboard handler. Gating (dashboard-admin) already happened in dashDeny.
private bool dashAcl(scope const(RVal)[] arr, scope const(ubyte)[] rawCmd, ref ByteBuffer reply) nothrow
{
    import dreads.server : aclDashboardCommand;

    if (arr.length == 0 || !ciEq(arr[0].str, "acl"))
        return false;
    aclDashboardCommand(arr, rawCmd, reply);
    return true;
}

private bool dashApplyConfig(scope const(RVal)[] arr, ref ByteBuffer reply) @nogc nothrow
{
    import dreads.resp : repError;

    if (arr.length < 4 || !ciEq(arr[0].str, "config") || !ciEq(arr[1].str, "set"))
        return false;
    if (!ciEq(arr[2].str, "maxmemory"))
        return false; // other params fall through to the normal path
    ulong v = 0;
    bool ok = arr[3].str.length > 0;
    foreach (c; arr[3].str)
    {
        if (c < '0' || c > '9')
        {
            ok = false;
            break;
        }
        v = v * 10 + (c - '0');
    }
    if (!ok)
    {
        repError(reply, "ERR dashboard: maxmemory bump takes a byte count");
        return true;
    }
    gConfig.maxmemory = v;
    reply.append("+OK\r\n");
    return true;
}

// Main-thread drain: execute each queued command on its db and reply. Mirrors the
// Lua bridge's cmdDrainLoop.
private void dashCmdDrainLoop() nothrow
{
    import dreads.raftq : CrossQueue;
    import dreads.resp : RVal, RType, parseValue, ParseStatus, repError, gRespProto;
    import dreads.obj : gDbs, NUM_DBS;
    import dreads.scripting : executeScriptCommand, appendScriptsResp, scriptRemove, scriptCommand;

    auto q = cast(CrossQueue) gDashCmdQ;
    static ByteBuffer payload;
    static Arena arena;
    static ByteBuffer eff;
    while (true)
    {
        try
        {
            q.waitData();
            void* tag;
            ulong meta;
            uint kind;
            while (q.take(payload, tag, meta, kind))
            {
                auto slot = cast(DashCmdSlot*) tag;
                arena.reset();
                slot.reply.clear();
                slot.status = 0;
                RVal cmd;
                size_t pos = 0;
                if (parseValue(slot.bytes.data, pos, arena, cmd) == ParseStatus.ok
                        && cmd.type == RType.Array && cmd.arr.length > 0)
                {
                    if (cmd.arr.length >= 2 && ciEq(cmd.arr[0].str, "script")
                            && ciEq(cmd.arr[1].str, "list"))
                        appendScriptsResp(slot.reply); // dashboard: list all cached scripts
                    else if (cmd.arr.length >= 3 && ciEq(cmd.arr[0].str, "script")
                            && ciEq(cmd.arr[1].str, "remove"))
                    {
                        if (!gConfig.dashboardWrite)
                            repError(slot.reply, "ERR dashboard: writes disabled (set dashboard-write yes)");
                        else
                        {
                            char[64] lo = void;
                            auto s = cmd.arr[2].str;
                            if (s.length > lo.length)
                                repError(slot.reply, "ERR bad sha");
                            else
                            {
                                foreach (i, c; s)
                                    lo[i] = (c >= 'A' && c <= 'Z') ? cast(char)(c + 32) : c;
                                if (scriptRemove(lo[0 .. s.length]))
                                    slot.reply.append("+OK\r\n");
                                else
                                    repError(slot.reply, "NOSCRIPT No matching script.");
                            }
                        }
                    }
                    else if (cmd.arr.length >= 2 && ciEq(cmd.arr[0].str, "script"))
                    {
                        // other SCRIPT subcommands (LOAD to create/cache a script,
                        // EXISTS, FLUSH) — server-layer, run in-place. They touch the
                        // script cache, so gate behind dashboard-write.
                        if (!gConfig.dashboardWrite)
                            repError(slot.reply, "ERR dashboard: writes disabled (set dashboard-write yes)");
                        else
                            scriptCommand(cmd.arr[1 .. $], slot.reply);
                    }
                    else if (auto reason = dashDeny(cmd.arr))
                        repError(slot.reply, reason);
                    else if (dashApplyConfig(cmd.arr, slot.reply))
                    {
                        // handled in-place (maxmemory bump)
                    }
                    else if (dashPubsub(cmd.arr, slot.reply))
                    {
                        // PUBSUB introspection (server-layer command, served in-place)
                    }
                    else if (dashEval(cmd.arr, gDbs[slot.db < NUM_DBS ? slot.db : 0], slot.reply, arena))
                    {
                        // EVAL/EVALSHA (server-layer scripting command, run in-place)
                    }
                    else if (dashAcl(cmd.arr, slot.bytes.data, slot.reply))
                    {
                        // ACL (server-layer command, served in-place — dashboard-admin gated)
                    }
                    else
                    {
                        auto dbi = slot.db < NUM_DBS ? slot.db : 0;
                        slot.status = executeScriptCommand(gDbs[dbi], cmd, cmd.arr, true,
                            cast(ulong) nowMs(), 0 /*userId: admin*/, gRespProto, arena, slot.reply, eff);
                    }
                }
                else
                    repError(slot.reply, "ERR malformed dashboard command");
                slot.ready = true;
                slot.done.emit();
            }
        }
        catch (Exception)
        {
        }
    }
}

/// Run a RESP command on the writer thread and copy its RESP reply into `reply`.
/// Called from the dashboard thread (HTTP handlers). Blocking round-trip, serialized.
package bool runCommand(scope const(ubyte)[] respCmd, ushort db, ref ByteBuffer reply) nothrow
{
    import dreads.raftq : CrossQueue;

    if (!gDashBridgeUp || gDashCmdQ is null)
        return false;
    try
    {
        gDashCmdMutex.lock();
        scope (exit)
            gDashCmdMutex.unlock();
        gDashSlot.bytes.clear();
        gDashSlot.bytes.append(respCmd);
        gDashSlot.db = db;
        gDashSlot.ready = false;
        (cast(CrossQueue) gDashCmdQ).put(gDashSlot.bytes.data, cast(void*)&gDashSlot, 0);
        while (!gDashSlot.ready)
            gDashSlot.done.wait();
        reply.clear();
        reply.append(gDashSlot.reply.data);
        return true;
    }
    catch (Exception)
        return false;
}

/// Publish a metrics snapshot (called by the main-loop timer, so gConnectedClients
/// and the keyspace are read on the writer thread). Skips entirely when nobody is
/// watching. Pub/sub counts are passed in (gPubSub is private to the server module).
package void snapshotMetrics(size_t channels, size_t patterns) @nogc nothrow
{
    if (atomicLoad(gDashClients) <= 0)
        return; // no client, no watch
    char[8192] tmp = void;
    immutable n = buildMetricsJson(tmp[], channels, patterns);
    if (n == 0 || n > gMBuf.length)
        return;
    atomicOp!"+="(gMSeq, 1); // -> odd: writing
    atomicFence();
    gMBuf[0 .. n] = cast(const(ubyte)[]) tmp[0 .. n];
    gMLen = n;
    atomicFence();
    atomicOp!"+="(gMSeq, 1); // -> even: done
}

// The dashboard UI: a Preact + uPlot app built to ONE self-contained index.html
// (vendor/dashboard, via its build.sh preBuildCommand) and embedded at compile time
// with a string import — no external files, served as one static HTTP response.
private immutable string DASH_BODY = import("index.html.gz"); // gzipped bundle
private immutable string DASH_PLACEHOLDER =
    "HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=utf-8\r\n"
    ~ "Content-Encoding: gzip\r\nContent-Length: "
    ~ DASH_BODY.length.to!string ~ "\r\nConnection: close\r\n\r\n" ~ DASH_BODY;

/// Start the dashboard thread — a no-op unless `dashboard yes` is configured.
/// Called once at server boot, right after the Lua pool. `respPort` is the RESP
/// listen port; the dashboard defaults to `respPort + 1` (override: dashboard-port).
public void startDashboard(ushort respPort) nothrow
{
    if (!gConfig.dashboard)
        return; // OPT-IN: nothing runs, no port bound, no thread, when off
    gDashPort = gConfig.dashboardPort != 0
        ? gConfig.dashboardPort : cast(ushort)(respPort + 1);
    try
    {
        gDashStop = createSharedManualEvent();
        gDashPool = new shared TaskPool(1, "dash");
        gDashPool.runTaskH(&dashThreadEntry);
        gDashUp = true;
    }
    catch (Exception)
    {
    }
}

/// The resolved dashboard port (0 if the dashboard is disabled) — for logging.
public ushort dashboardPort() @nogc nothrow
{
    return gDashUp ? gDashPort : 0;
}

// Runs on the dashboard worker thread: bind the listener on THIS thread's event
// loop, then park until shutdown. Accepted connections are handled as fibers on the
// same worker loop, fully isolated from the data-plane loop.
private void dashThreadEntry() nothrow
{
    try
    {
        listenTCP(gDashPort,
            delegate(TCPConnection conn) @trusted nothrow { onDashConn(conn); },
            gConfig.dashboardBind);
        gDashStop.wait(); // keep the task alive; yields so the loop handles accepts
    }
    catch (Exception)
    {
    }
}

private size_t parseContentLength(scope const(char)[] req) @safe @nogc nothrow
{
    auto v = httpHeader(req, "Content-Length");
    size_t r = 0;
    foreach (c; v)
    {
        if (c < '0' || c > '9')
            break;
        r = r * 10 + (c - '0');
    }
    return r;
}

// POST /api/exec: the body is a RESP command (the client builds it, so it is
// binary-safe — values/scripts with spaces or newlines just work). Optional
// password gate (X-Dashboard-Auth). Runs it through the writer bridge (which
// applies the read/write/admin gate) and returns the raw RESP reply.
private void handleExec(TCPConnection conn, scope ubyte[] buf, size_t n, scope const(char)[] req) @trusted
{
    import core.stdc.stdio : snprintf;

    enum R401 = "HTTP/1.1 401 Unauthorized\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
    enum R400 = "HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";

    if (gConfig.dashboardPassword.length)
    {
        if (httpHeader(req, "X-Dashboard-Auth") != gConfig.dashboardPassword)
        {
            conn.write(cast(const(ubyte)[]) R401);
            return;
        }
    }

    // locate the body (after CRLF CRLF) and its declared length
    size_t hend = 0;
    if (n >= 4)
        foreach (i; 0 .. n - 3)
            if (buf[i] == '\r' && buf[i + 1] == '\n' && buf[i + 2] == '\r' && buf[i + 3] == '\n')
            {
                hend = i + 4;
                break;
            }
    immutable clen = parseContentLength(req);
    if (hend == 0 || clen == 0 || clen > 256 * 1024)
    {
        conn.write(cast(const(ubyte)[]) R400);
        return;
    }

    ByteBuffer body_;
    size_t have = n - hend;
    if (have > clen)
        have = clen;
    body_.append(buf[hend .. hend + have]);
    ubyte[4096] tmp = void;
    while (body_.data.length < clen)
    {
        if (!conn.waitForData(5.seconds))
            break;
        immutable r = conn.read(tmp[], IOMode.once);
        if (r <= 0)
            break;
        immutable need = clen - body_.data.length;
        body_.append(tmp[0 .. (r > need ? need : r)]);
    }

    ByteBuffer reply;
    immutable ok = runCommand(body_.data, 0, reply);
    auto payload = ok ? reply.data : cast(const(ubyte)[]) "-ERR dashboard bridge unavailable\r\n";
    char[128] hdr = void;
    immutable hn = snprintf(hdr.ptr, hdr.length,
        "HTTP/1.1 200 OK\r\nContent-Type: application/octet-stream\r\nContent-Length: %zu\r\nConnection: close\r\n\r\n",
        payload.length);
    if (hn > 0)
        conn.write(cast(const(ubyte)[]) hdr[0 .. hn]);
    conn.write(payload);
}

private void onDashConn(TCPConnection conn) @trusted nothrow
{
    try
    {
        ubyte[4096] buf = void;
        size_t n;
        if (conn.waitForData(5.seconds))
            n = conn.read(buf[], IOMode.once);
        auto req = cast(const(char)[]) buf[0 .. n];

        auto target = httpPath(req);
        size_t qpos = 0;
        while (qpos < target.length && target[qpos] != '?')
            qpos++;
        auto path = target[0 .. qpos];
        auto query = qpos < target.length ? target[qpos + 1 .. $] : null;
        if (path == "/ws")
        {
            // Live metrics are gated behind the password too. Browsers can't set
            // headers on a WebSocket, so the frontend passes ?auth=<password>.
            enum WS401 = "HTTP/1.1 401 Unauthorized\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
            if (gConfig.dashboardPassword.length
                    && queryParam(query, "auth") != gConfig.dashboardPassword)
                conn.write(cast(const(ubyte)[]) WS401);
            else
            {
                auto key = httpHeader(req, "Sec-WebSocket-Key");
                if (key.length && wsHandshake(conn, key))
                    wsLoop(conn);
            }
        }
        else if (path == "/api/exec")
        {
            handleExec(conn, buf[], n, req);
        }
        else
        {
            conn.write(cast(const(ubyte)[]) DASH_PLACEHOLDER);
        }
        conn.close();
    }
    catch (Exception)
    {
    }
}

// ---- minimal HTTP request parsing (over the already-read head, @nogc) ----

// Value of a query-string parameter (raw, undecoded — dashboard passwords are simple
// tokens with no &/space/? so no percent-decoding is needed). Null if absent.
private const(char)[] queryParam(return scope const(char)[] query, scope const(char)[] name) @safe @nogc nothrow
{
    size_t i = 0;
    while (i < query.length)
    {
        size_t amp = i;
        while (amp < query.length && query[amp] != '&')
            amp++;
        auto pair = query[i .. amp];
        size_t eq = 0;
        while (eq < pair.length && pair[eq] != '=')
            eq++;
        if (pair[0 .. eq] == name)
            return eq < pair.length ? pair[eq + 1 .. $] : null;
        i = amp + 1;
    }
    return null;
}

// The request-target from the first line: "GET <path> HTTP/1.1".
private const(char)[] httpPath(return scope const(char)[] req) @safe @nogc nothrow
{
    size_t s = 0;
    while (s < req.length && req[s] != ' ')
        s++; // past the method
    s++;
    size_t e = s;
    while (e < req.length && req[e] != ' ')
        e++;
    return s <= req.length && e <= req.length && s <= e ? req[s .. e] : null;
}

// Value of a header by (case-insensitive) name, trimmed. Empty if absent.
private const(char)[] httpHeader(return scope const(char)[] req, scope const(char)[] name) @safe @nogc nothrow
{
    size_t i = 0;
    while (i < req.length)
    {
        // start of a line
        size_t ls = i;
        size_t le = ls;
        while (le < req.length && req[le] != '\r' && req[le] != '\n')
            le++;
        auto line = req[ls .. le];
        if (line.length > name.length + 1 && ciEq(line[0 .. name.length], name) && line[name.length] == ':')
        {
            size_t vs = name.length + 1;
            while (vs < line.length && (line[vs] == ' ' || line[vs] == '\t'))
                vs++;
            return line[vs .. $];
        }
        // advance past CRLF
        i = le;
        while (i < req.length && (req[i] == '\r' || req[i] == '\n'))
            i++;
        if (i == le)
            break;
    }
    return null;
}

private bool ciEq(scope const(char)[] a, scope const(char)[] b) @safe @nogc nothrow
{
    if (a.length != b.length)
        return false;
    foreach (i; 0 .. a.length)
    {
        char x = a[i], y = b[i];
        if (x >= 'A' && x <= 'Z')
            x += 32;
        if (y >= 'A' && y <= 'Z')
            y += 32;
        if (x != y)
            return false;
    }
    return true;
}

// ---- WebSocket (RFC 6455): handshake + server->client text frames ----

private bool wsHandshake(TCPConnection conn, scope const(char)[] key) @trusted nothrow
{
    import std.digest.sha : sha1Of;
    import std.base64 : Base64;

    try
    {
        enum GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";
        char[128] cat = void;
        if (key.length + GUID.length > cat.length)
            return false;
        cat[0 .. key.length] = key;
        cat[key.length .. key.length + GUID.length] = GUID;
        auto sha = sha1Of(cast(const(ubyte)[]) cat[0 .. key.length + GUID.length]); // ubyte[20]
        char[32] accBuf = void;
        auto acc = Base64.encode(sha[], accBuf[]); // 28 chars

        // Assemble the whole 101 response and write ONCE (three small writes could
        // fragment across TCP segments and trip strict clients).
        enum PRE = "HTTP/1.1 101 Switching Protocols\r\n"
            ~ "Upgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ";
        enum SUF = "\r\n\r\n";
        char[256] resp = void;
        size_t p = 0;
        resp[p .. p + PRE.length] = PRE;
        p += PRE.length;
        resp[p .. p + acc.length] = acc[];
        p += acc.length;
        resp[p .. p + SUF.length] = SUF;
        p += SUF.length;
        conn.write(cast(const(ubyte)[]) resp[0 .. p]);
        return true;
    }
    catch (Exception)
        return false;
}

// Send one unmasked text frame (server->client never masks).
private bool wsSendText(TCPConnection conn, scope const(char)[] payload) @trusted nothrow
{
    try
    {
        ubyte[10] h = void;
        size_t hn;
        h[0] = 0x81; // FIN + opcode=text
        if (payload.length < 126)
        {
            h[1] = cast(ubyte) payload.length;
            hn = 2;
        }
        else if (payload.length < 65_536)
        {
            h[1] = 126;
            h[2] = cast(ubyte)(payload.length >> 8);
            h[3] = cast(ubyte)(payload.length);
            hn = 4;
        }
        else
        {
            h[1] = 127;
            ulong L = payload.length;
            foreach (i; 0 .. 8)
                h[2 + i] = cast(ubyte)(L >> (8 * (7 - i)));
            hn = 10;
        }
        conn.write(h[0 .. hn]);
        conn.write(cast(const(ubyte)[]) payload);
        return true;
    }
    catch (Exception)
        return false;
}

// Phase-1 heartbeat loop: while connected, push a tiny JSON every refresh
// interval. Phase 2b replaces the heartbeat body with the real metrics snapshot
// drained from the main loop's CrossQueue. Tracks gDashClients for "no watch".
private void wsLoop(TCPConnection conn) @trusted nothrow
{
    atomicOp!"+="(gDashClients, 1);
    scope (exit)
        atomicOp!"-="(gDashClients, 1);

    auto ivl = gConfig.dashboardIntervalMs.msecs;
    ubyte[256] scratch = void;
    ubyte[8192] local = void;
    while (true)
    {
        // read the latest published snapshot (seqlock: retry on a torn read)
        size_t ln = 0;
        foreach (_; 0 .. 16)
        {
            immutable s1 = atomicLoad(gMSeq);
            if (s1 & 1)
                continue; // writer mid-update
            auto L = gMLen;
            if (L > local.length)
                L = local.length;
            local[0 .. L] = gMBuf[0 .. L];
            if (atomicLoad(gMSeq) == s1)
            {
                ln = L;
                break;
            }
        }
        const(char)[] payload = ln > 0 ? cast(const(char)[]) local[0 .. ln] : "{}";
        if (!wsSendText(conn, payload))
            break;
        try
        {
            // wait up to one interval; a client frame (incl. close) wakes us early
            if (conn.waitForData(ivl))
            {
                immutable r = conn.read(scratch[], IOMode.once);
                if (r >= 1 && (scratch[0] & 0x0F) == 0x8) // close frame
                    break;
            }
            if (!conn.connected)
                break;
        }
        catch (Exception)
            break;
    }
}

/// Stop and join the dashboard thread. The pool worker is non-daemon, so (like the
/// Lua pool) druntime would block on it at exit without this.
public void shutdownDashboard() nothrow
{
    if (!gDashUp || gDashPool is null)
        return;
    gDashUp = false;
    try
    {
        gDashStop.emit(); // ends dashThreadEntry's wait()
        (cast(TaskPool) gDashPool).terminate();
    }
    catch (Exception)
    {
    }
}
