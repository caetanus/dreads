module dreads.dashboard;

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

import core.atomic : atomicOp, atomicLoad;
import core.time : seconds, msecs;

import vibe.core.net : listenTCP, TCPConnection, TCPListener;
import vibe.core.stream : IOMode;
import vibe.core.sync : createSharedManualEvent, ManualEvent;
import vibe.core.taskpool : TaskPool;

import dreads.config : gConfig;
import dreads.stream : nowMs;

private shared TaskPool gDashPool;
private __gshared bool gDashUp;
private shared ManualEvent gDashStop; // emitted at shutdown to end the listener task

// Count of connected dashboard WebSocket clients. The main loop reads this to
// honour "no client, no watch": it only snapshots + pushes metrics while > 0.
package shared int gDashClients;

// A complete, static HTTP/1.0 response (headers + body). Being a CTFE-folded string
// literal it is read-only program data — writing it allocates nothing.
private immutable string DASH_PLACEHOLDER =
    "HTTP/1.1 200 OK\r\n"
    ~ "Content-Type: text/html; charset=utf-8\r\n"
    ~ "Connection: close\r\n"
    ~ "\r\n"
    ~ "<!doctype html><html><head><meta charset=\"utf-8\"><title>dreads dashboard</title></head>"
    ~ "<body style=\"font-family:system-ui,sans-serif;background:#0b0e14;color:#c9d1d9;margin:2rem\">"
    ~ "<h1>dreads &#9889; dashboard</h1>"
    ~ "<p>Phase&nbsp;1 scaffold &mdash; the isolated dashboard event loop is live. "
    ~ "Live metrics (WebSocket) and the React UI are next.</p>"
    ~ "</body></html>";

/// Start the dashboard thread — a no-op unless `dashboard yes` is configured.
/// Called once at server boot, right after the Lua pool.
public void startDashboard() nothrow
{
    if (!gConfig.dashboard)
        return; // OPT-IN: nothing runs, no port bound, no thread, when off
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

// Runs on the dashboard worker thread: bind the listener on THIS thread's event
// loop, then park until shutdown. Accepted connections are handled as fibers on the
// same worker loop, fully isolated from the data-plane loop.
private void dashThreadEntry() nothrow
{
    try
    {
        listenTCP(gConfig.dashboardPort,
            delegate(TCPConnection conn) @trusted nothrow { onDashConn(conn); },
            gConfig.dashboardBind);
        gDashStop.wait(); // keep the task alive; yields so the loop handles accepts
    }
    catch (Exception)
    {
    }
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

        if (httpPath(req) == "/ws")
        {
            auto key = httpHeader(req, "Sec-WebSocket-Key");
            if (key.length && wsHandshake(conn, key))
                wsLoop(conn);
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

// The request-target from the first line: "GET <path> HTTP/1.1".
private const(char)[] httpPath(scope const(char)[] req) @safe @nogc nothrow
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
private const(char)[] httpHeader(scope const(char)[] req, scope const(char)[] name) @safe @nogc nothrow
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
    import core.stdc.stdio : snprintf;

    atomicOp!"+="(gDashClients, 1);
    scope (exit)
        atomicOp!"-="(gDashClients, 1);

    auto ivl = gConfig.dashboardIntervalMs.msecs;
    ubyte[256] scratch = void;
    while (true)
    {
        char[96] j = void;
        immutable n = snprintf(j.ptr, j.length,
            `{"t":%llu,"clients":%d}`, cast(ulong) nowMs(), atomicLoad(gDashClients));
        if (n <= 0 || !wsSendText(conn, j[0 .. n]))
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
