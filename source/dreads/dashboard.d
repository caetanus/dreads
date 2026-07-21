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

import core.time : seconds;

import vibe.core.net : listenTCP, TCPConnection, TCPListener;
import vibe.core.stream : IOMode;
import vibe.core.sync : createSharedManualEvent, ManualEvent;
import vibe.core.taskpool : TaskPool;

import dreads.config : gConfig;

private shared TaskPool gDashPool;
private __gshared bool gDashUp;
private shared ManualEvent gDashStop; // emitted at shutdown to end the listener task

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
        ubyte[2048] buf = void;
        if (conn.waitForData(3.seconds))
            conn.read(buf[], IOMode.once); // consume the request head (not routed yet)
        conn.write(cast(const(ubyte)[]) DASH_PLACEHOLDER);
        conn.close();
    }
    catch (Exception)
    {
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
