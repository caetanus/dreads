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

import core.atomic : atomicOp, atomicLoad, atomicFence;
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
private immutable string[] PUB_CMDS = ["publish", "spublish"];

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

    ulong total = 0;
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
        sumCalls(STREAM_CMDS), sumCalls(PUB_CMDS), channels, patterns);
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

// A complete, static HTTP response (headers + body). CTFE-folded => read-only
// program data, writing it allocates nothing. Phase-1 interim UI: a self-contained
// page that opens the /ws stream and shows live counters + rates + a memory bar —
// so the dashboard is usable NOW, before the embedded Preact build (Phase 2).
private immutable string DASH_PLACEHOLDER =
    "HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=utf-8\r\nConnection: close\r\n\r\n"
    ~ DASH_HTML;

private immutable string DASH_HTML = `<!doctype html><html><head><meta charset="utf-8">
<title>dreads dashboard</title><style>
*{box-sizing:border-box}body{font-family:system-ui,sans-serif;background:#0b0e14;color:#c9d1d9;margin:0;padding:1.4rem}
h1{font-weight:600;margin:0 0 .3rem;font-size:1.3rem}.sub{color:#5c6b82;font-size:.8rem;margin-bottom:1.1rem}
.grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(150px,1fr));gap:.7rem}
.card{background:#141b26;border:1px solid #1f2a3a;border-radius:10px;padding:.7rem .9rem}
.k{font-size:.68rem;color:#7d8aa0;text-transform:uppercase;letter-spacing:.05em}
.v{font-size:1.5rem;font-weight:600;margin-top:.15rem;font-variant-numeric:tabular-nums}
.r{font-size:.72rem;color:#3fb950;margin-top:.1rem;font-variant-numeric:tabular-nums}
.bar{height:9px;border-radius:5px;background:#1f2a3a;overflow:hidden;margin-top:.5rem}
.fill{height:100%;background:linear-gradient(90deg,#2f81f7,#3fb950)}
.off{color:#f85149}#st{float:right;font-size:.75rem;color:#7d8aa0}</style></head><body>
<h1>dreads &#9889; dashboard <span id=st>connecting&hellip;</span></h1>
<div class=sub>live metrics &mdash; rates are per second</div>
<div class=grid>
<div class=card><div class=k>commands/s</div><div class=v id=cmds>0</div><div class=r id=cmdsR>&nbsp;</div></div>
<div class=card><div class=k>strings/s</div><div class=v id=str>0</div><div class=r id=strR>&nbsp;</div></div>
<div class=card><div class=k>lists/s</div><div class=v id=list>0</div><div class=r id=listR>&nbsp;</div></div>
<div class=card><div class=k>streams/s</div><div class=v id=stream>0</div><div class=r id=streamR>&nbsp;</div></div>
<div class=card><div class=k>publish/s</div><div class=v id=pub>0</div><div class=r id=pubR>&nbsp;</div></div>
<div class=card><div class=k>keys</div><div class=v id=keys>0</div><div class=r id=keysR>&nbsp;</div></div>
<div class=card><div class=k>clients</div><div class=v id=clients>0</div></div>
<div class=card><div class=k>pubsub ch / pat</div><div class=v id=chpat>0 / 0</div></div>
<div class=card><div class=k>expired / evicted</div><div class=v id=exev>0 / 0</div></div>
<div class=card style="grid-column:1/-1"><div class=k>memory used / max</div>
<div class=v id=mem>0</div><div class=bar><div class=fill id=memfill style="width:0%"></div></div></div>
</div>
<script>
var $=function(i){return document.getElementById(i)},prev=null;
function fmt(n){if(n>=1e9)return(n/1e9).toFixed(1)+'G';if(n>=1e6)return(n/1e6).toFixed(1)+'M';if(n>=1e3)return(n/1e3).toFixed(1)+'k';return''+n}
function bytes(n){var u=['B','KB','MB','GB','TB'],i=0;while(n>=1024&&i<4){n/=1024;i++}return n.toFixed(i?1:0)+u[i]}
function conn(){
 var ws=new WebSocket((location.protocol=='https:'?'wss':'ws')+'://'+location.host+'/ws');
 ws.onopen=function(){$('st').textContent='live';$('st').className=''};
 ws.onclose=function(){$('st').textContent='disconnected';$('st').className='off';setTimeout(conn,1000)};
 ws.onmessage=function(e){
  var m=JSON.parse(e.data);if(!m.t)return;
  var dt=prev?(m.t-prev.t)/1000:0;
  function rate(id,cur,pv){var el=$(id);el.textContent=fmt(cur);var rEl=$(id+'R');if(rEl&&dt>0){var rr=Math.max(0,Math.round((cur-pv)/dt));rEl.textContent='+'+fmt(rr)+'/s'}}
  rate('cmds',m.cmds,prev?prev.cmds:0);rate('str',m.str,prev?prev.str:0);
  rate('list',m.list,prev?prev.list:0);rate('stream',m.stream,prev?prev.stream:0);
  rate('pub',m.pub,prev?prev.pub:0);rate('keys',m.keys,prev?prev.keys:0);
  $('clients').textContent=m.clients;$('chpat').textContent=m.channels+' / '+m.patterns;
  $('exev').textContent=fmt(m.expired)+' / '+fmt(m.evicted);
  var mm=m.maxmem>0?m.maxmem:0,pc=mm?Math.min(100,m.mem/mm*100):0;
  $('mem').textContent=bytes(m.mem)+(mm?' / '+bytes(mm):' / ∞');
  $('memfill').style.width=(mm?pc:6)+'%';
  prev=m;
 };
}
conn();
</script></body></html>`;

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
