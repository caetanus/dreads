module dreads.cluster;

// Phase-2a sharding: a static slot-range topology. Each node owns a contiguous
// slot range and serves only keys in it; a key outside its range gets a
// `-MOVED <slot> <host:port>` so a cluster-aware client re-routes (and caches
// the map from CLUSTER SLOTS). The topology is fixed at boot by `cluster-nodes`
// ("lo-hi@host:port,..."). Shards are shared-nothing — each node/thread owns
// its keyspace, no cross-shard locks — so throughput adds across nodes.
//
// Key routing here is the common case: the routed key is the command's first
// argument (arg[1]). Keyless/admin/connection commands run locally on any node.
// Multi-key CROSSSLOT enforcement and non-first-arg key specs are a later
// refinement (tracked in SHARDING.md).

import dreads.config : gConfig;
import dreads.mem : ByteBuffer;
import dreads.resp;
import dreads.slots : keyToSlot, SLOTS;

struct ClusterNode
{
    ushort lo, hi; // inclusive slot range
    string host;
    ushort port;
}

__gshared ClusterNode[] gNodes; // full topology (all shards)
__gshared int gSelfIndex = -1; // this node's entry in gNodes (-1 = not clustered)

/// Parse `cluster-nodes` and locate self by port. Called at boot when
/// cluster-enabled. Malformed entries are skipped.
void initCluster()
{
    import std.array : split;
    import std.conv : to;

    if (!gConfig.clusterEnabled || gConfig.clusterNodes.length == 0)
        return;
    foreach (spec; gConfig.clusterNodes.split(","))
    {
        if (spec.length == 0)
            continue;
        try
        {
            auto at = spec.split("@"); // "lo-hi" @ "host:port"
            auto rng = at[0].split("-");
            auto hp = at[1].split(":");
            gNodes ~= ClusterNode(rng[0].to!ushort, rng[1].to!ushort, hp[0].idup, hp[1].to!ushort);
        }
        catch (Exception)
        {
        }
    }
    foreach (i, ref n; gNodes)
        if (n.port == gConfig.port)
            gSelfIndex = cast(int) i;
}

@property bool active() @nogc nothrow
{
    return gSelfIndex >= 0;
}

bool ownsSlot(ushort slot) @nogc nothrow
{
    if (gSelfIndex < 0)
        return true;
    auto n = gNodes[gSelfIndex];
    return slot >= n.lo && slot <= n.hi;
}

private const(ClusterNode)* ownerOf(ushort slot) @nogc nothrow
{
    foreach (ref n; gNodes)
        if (slot >= n.lo && slot <= n.hi)
            return &n;
    return null;
}

// Commands that never carry a routable key — connection/admin/pubsub/txn/
// keyspace-wide — run on whatever node receives them.
private bool keyless(scope const(char)[] u) @nogc nothrow
{
    switch (u)
    {
    case "PING", "ECHO", "QUIT", "SELECT", "HELLO", "AUTH", "RESET", "COMMAND",
        "CLUSTER", "CONFIG", "INFO", "CLIENT", "DBSIZE", "FLUSHALL", "FLUSHDB",
        "KEYS", "SCAN", "RANDOMKEY", "SWAPDB", "TIME", "LOLWUT", "SAVE", "BGSAVE",
        "LASTSAVE", "BGREWRITEAOF", "SHUTDOWN", "WAIT", "FAILOVER", "ROLE",
        "REPLICAOF", "SLAVEOF", "RAFT", "SUBSCRIBE", "UNSUBSCRIBE", "PSUBSCRIBE",
        "PUNSUBSCRIBE", "SSUBSCRIBE", "SUNSUBSCRIBE", "PUBLISH", "SPUBLISH",
        "PUBSUB", "MULTI", "EXEC", "DISCARD", "WATCH", "UNWATCH", "MONITOR",
        "SLOWLOG", "MEMORY", "LATENCY", "ACL", "SCRIPT", "FUNCTION", "DEBUG",
        "OBJECT", "SORT", "SCRIPT_LOAD":
        return true;
    default:
        return false;
    }
}

/// If this command must be handled elsewhere, append a -MOVED and return true
/// (the caller stops). Otherwise return false (handle locally). Only meaningful
/// when clustered; a no-op otherwise.
bool redirectIfForeign(scope const(char)[] uname, scope const(RVal)[] arr, ref ByteBuffer o) nothrow
{
    if (gSelfIndex < 0 || arr.length < 2 || keyless(uname))
        return false;
    auto slot = keyToSlot(arr[1].str);
    if (ownsSlot(slot))
        return false;
    auto n = ownerOf(slot);
    if (n is null)
        return false;
    o.append("-MOVED ");
    appendUint(o, slot);
    o.appendByte(' ');
    o.append(n.host);
    o.appendByte(':');
    appendUint(o, n.port);
    o.append("\r\n");
    return true;
}

private void appendUint(ref ByteBuffer o, ulong v) nothrow
{
    char[20] b = void;
    size_t i = b.length;
    do
    {
        b[--i] = cast(char)('0' + v % 10);
        v /= 10;
    }
    while (v);
    o.append(b[i .. $]);
}

/// CLUSTER subcommand handler. Reports the static topology so smart clients can
/// build their slot map. Returns true (always handled).
bool clusterCommand(scope const(RVal)[] args, ref ByteBuffer o) nothrow
{
    if (args.length < 2)
    {
        o.append("-ERR wrong number of arguments for 'cluster' command\r\n");
        return true;
    }
    char[16] ub = void;
    auto sub = args[1].str;
    if (sub.length > ub.length)
    {
        repUnknownSubcommand(o, "CLUSTER", sub);
        return true;
    }
    foreach (i, c; sub)
        ub[i] = c >= 'a' && c <= 'z' ? cast(char)(c - 32) : c;
    auto u = cast(string) ub[0 .. sub.length];

    switch (u)
    {
    case "KEYSLOT":
        if (args.length < 3)
        {
            o.append("-ERR wrong number of arguments\r\n");
            return true;
        }
        o.appendByte(':');
        appendUint(o, keyToSlot(args[2].str));
        o.append("\r\n");
        return true;
    case "MYID":
        // no gossip node ids yet: a stable synthetic id from host:port
        o.append("$40\r\n");
        auto n = gSelfIndex >= 0 ? gNodes[gSelfIndex] : ClusterNode(0, 0, "127.0.0.1", gConfig.port);
        char[40] id = '0';
        auto tag = n.host ~ ":";
        size_t k;
        foreach (c; tag)
            if (k < 40)
                id[k++] = c;
        appendPortHex(id[], k, n.port);
        o.append(cast(const(char)[]) id[]);
        o.append("\r\n");
        return true;
    case "INFO":
        auto sz = gNodes.length ? gNodes.length : 1;
        ByteBuffer body_;
        body_.append("cluster_enabled:");
        appendUint(body_, gConfig.clusterEnabled ? 1 : 0);
        body_.append("\r\ncluster_state:ok");
        body_.append("\r\ncluster_slots_assigned:16384\r\ncluster_known_nodes:");
        appendUint(body_, sz);
        body_.append("\r\ncluster_size:");
        appendUint(body_, sz);
        body_.append("\r\n");
        o.clear();
        o.appendByte('$');
        appendUint(o, body_.length);
        o.append("\r\n");
        o.append(body_.data);
        o.append("\r\n");
        return true;
    case "SLOTS":
        appendSlots(o);
        return true;
    case "SHARDS":
        // minimal: reuse SLOTS shape is not identical; report empty for now
        o.append("*0\r\n");
        return true;
    case "NODES":
        appendNodesText(o);
        return true;
    case "RESET", "BUMPEPOCH", "SET-CONFIG-EPOCH", "SETSLOT", "FLUSHSLOTS", "FORGET":
        o.append("+OK\r\n");
        return true;
    default:
        repUnknownSubcommand(o, "CLUSTER", sub);
        return true;
    }
}

private void appendPortHex(char[] id, size_t at, ushort port) @nogc nothrow
{
    static immutable hx = "0123456789abcdef";
    ushort p = port;
    foreach_reverse (j; 0 .. 4)
        if (at + j < id.length)
            id[at + j] = hx[(p >> (4 * (3 - j))) & 0xF];
}

// CLUSTER SLOTS: array of [startSlot, endSlot, [host, port, nodeid]] per shard.
private void appendSlots(ref ByteBuffer o) nothrow
{
    auto nodes = gNodes.length ? gNodes : [ClusterNode(0, cast(ushort)(SLOTS - 1),
            "127.0.0.1", gConfig.port)];
    o.appendByte('*');
    appendUint(o, nodes.length);
    o.append("\r\n");
    foreach (ref n; nodes)
    {
        o.append("*3\r\n:");
        appendUint(o, n.lo);
        o.append("\r\n:");
        appendUint(o, n.hi);
        o.append("\r\n*3\r\n$");
        appendUint(o, n.host.length);
        o.append("\r\n");
        o.append(n.host);
        o.append("\r\n:");
        appendUint(o, n.port);
        o.append("\r\n$40\r\n");
        char[40] id = '0';
        size_t k;
        foreach (c; n.host)
            if (k < 36)
                id[k++] = c;
        appendPortHex(id[], k, n.port);
        o.append(cast(const(char)[]) id[]);
        o.append("\r\n");
    }
}

private void appendNodesText(ref ByteBuffer o) nothrow
{
    ByteBuffer t;
    auto nodes = gNodes.length ? gNodes : [ClusterNode(0, cast(ushort)(SLOTS - 1),
            "127.0.0.1", gConfig.port)];
    foreach (i, ref n; nodes)
    {
        // <id> <ip:port@cport> <flags> - 0 0 <epoch> connected <lo-hi>
        char[40] id = '0';
        size_t k;
        foreach (c; n.host)
            if (k < 36)
                id[k++] = c;
        appendPortHex(id[], k, n.port);
        t.append(cast(const(char)[]) id[]);
        t.appendByte(' ');
        t.append(n.host);
        t.appendByte(':');
        appendUint(t, n.port);
        t.appendByte('@');
        appendUint(t, cast(ushort)(n.port + 10_000));
        t.append(cast(int) i == gSelfIndex ? " myself,master - 0 0 0 connected " : " master - 0 0 0 connected ");
        appendUint(t, n.lo);
        t.appendByte('-');
        appendUint(t, n.hi);
        t.append("\n");
    }
    o.appendByte('$');
    appendUint(o, t.length);
    o.append("\r\n");
    o.append(t.data);
    o.append("\r\n");
}

// ---------------------------------------------------------------------------
// PROXY (cluster-proxy): forward a foreign key's raw command to its owner node
// and relay the reply, transparent to any client (incl. the skins). This is the
// "shard is a Redis instance" model — routing to a remote shard = being its
// RESP client. RESP is already in-order request/reply per connection, so no
// wire protocol, no correlation, no pending pointer. The default (MOVED) path
// above is unchanged; proxy is opt-in for non-cluster-aware clients.
// ---------------------------------------------------------------------------

import vibe.core.net : TCPConnection, connectTCP;
import vibe.core.stream : IOMode;
import core.time : seconds;

/// Which peer node owns this command's key, or -1 if local/keyless (handle
/// here). The caller flushes staged local hops before forwarding, so the proxy
/// reply keeps pipeline order.
int proxyForeignNode(scope const(char)[] uname, scope const(RVal)[] arr) nothrow @trusted
{
    if (gSelfIndex < 0 || arr.length < 2 || keyless(uname))
        return -1;
    immutable slot = keyToSlot(arr[1].str);
    if (ownsSlot(slot))
        return -1;
    foreach (i, ref n; gNodes)
        if (slot >= n.lo && slot <= n.hi)
            return cast(int) i;
    return -1;
}

/// Per-CLIENT-CONNECTION upstream sockets to peer nodes. A client connection is
/// a serial stream (even pipelined, processed in order by ONE fiber), so a
/// dedicated upstream socket per (client, peer) is naturally serialized — no
/// lock, no cross-fiber interleave (the shared-per-thread socket deadlocked
/// under concurrency+pipelining). GC class: freed with the Conn, no malloc
/// cross-thread hazard.
final class ClientProxy
{
    TCPConnection[] up; // per gNodes index
    bool[] alive;
    int[] db;

    private void ensure(int n) nothrow
    {
        if (up.length < gNodes.length)
        {
            up.length = gNodes.length;
            alive.length = gNodes.length;
            db.length = gNodes.length;
            foreach (ref d; db)
                d = -1;
        }
    }
}

/// Forward `rawCmd` to peer `node` on `db`, over THIS client's dedicated
/// upstream socket, relaying the reply into `o`. `cp` is lazily created by the
/// caller (one per client Conn).
void proxyForward(ClientProxy cp, int node, int db, scope const(ubyte)[] rawCmd,
        ref ByteBuffer o) nothrow @trusted
{
    cp.ensure(node);
    if (!cp.alive[node])
    {
        try
        {
            cp.up[node] = connectTCP(gNodes[node].host, gNodes[node].port);
            cp.up[node].tcpNoDelay = true;
            cp.alive[node] = true;
            cp.db[node] = -1;
        }
        catch (Exception)
        {
            cp.alive[node] = false;
            o.append("-CLUSTERDOWN can't reach the slot's owner\r\n");
            return;
        }
    }
    try
    {
        if (db != cp.db[node])
        {
            static ByteBuffer sel;
            sel.clear();
            pxSelect(sel, db);
            cp.up[node].write(sel.data);
            static ByteBuffer sr;
            if (!pxReadReply(cp.up[node], sr))
            {
                proxyDrop(cp, node);
                o.append("-CLUSTERDOWN owner link failed\r\n");
                return;
            }
            cp.db[node] = db;
        }
        cp.up[node].write(rawCmd);
        static ByteBuffer reply;
        if (!pxReadReply(cp.up[node], reply))
        {
            proxyDrop(cp, node);
            o.append("-CLUSTERDOWN owner link failed\r\n");
            return;
        }
        o.append(reply.data);
    }
    catch (Exception)
    {
        proxyDrop(cp, node);
        o.append("-CLUSTERDOWN owner link failed\r\n");
    }
}

/// Close this client's upstream sockets (called when the client disconnects).
void proxyClose(ClientProxy cp) nothrow @trusted
{
    if (cp is null)
        return;
    foreach (i; 0 .. cp.up.length)
        if (cp.alive[i])
        {
            try
                cp.up[i].close();
            catch (Exception)
            {
            }
            cp.alive[i] = false;
        }
}

private void proxyDrop(ClientProxy cp, int node) nothrow @trusted
{
    try
        cp.up[node].close();
    catch (Exception)
    {
    }
    cp.alive[node] = false;
    cp.db[node] = -1;
}

private void pxSelect(ref ByteBuffer o, int db) nothrow @trusted
{
    import core.stdc.stdio : snprintf;

    char[24] n = void;
    immutable dn = snprintf(n.ptr, n.length, "%d", db);
    char[8] hd = void;
    immutable hn = snprintf(hd.ptr, hd.length, "$%d\r\n", dn);
    o.append("*2\r\n$6\r\nSELECT\r\n");
    o.append(hd[0 .. hn]);
    o.append(n[0 .. dn]);
    o.append("\r\n");
}

private bool pxReadReply(TCPConnection c, ref ByteBuffer dst) @trusted
{
    static ByteBuffer buf;
    buf.clear();
    ubyte[16384] tmp = void;
    for (;;)
    {
        immutable n = respReplyComplete(cast(const(ubyte)[]) buf.data);
        if (n > 0)
        {
            dst.clear();
            dst.append((cast(const(ubyte)[]) buf.data)[0 .. n]);
            return true;
        }
        if (!c.waitForData(30.seconds))
            return false;
        immutable r = c.read(tmp[], IOMode.once);
        if (r <= 0)
            return false;
        buf.append(tmp[0 .. cast(size_t) r]);
    }
}

/// Length of one complete RESP reply at the front of `d`, or 0 if incomplete.
private size_t respReplyComplete(scope const(ubyte)[] d) @trusted @nogc nothrow
{
    size_t pos = 0;
    return pxScanOne(d, pos) ? pos : 0;
}

private bool pxScanOne(scope const(ubyte)[] d, ref size_t pos) @trusted @nogc nothrow
{
    if (pos >= d.length)
        return false;
    immutable t = d[pos];
    if (t == '$')
    {
        size_t p = pos + 1;
        long n;
        if (!pxReadLong(d, p, n))
            return false;
        if (n < 0)
        {
            pos = p;
            return true;
        }
        if (p + cast(size_t) n + 2 > d.length)
            return false;
        pos = p + cast(size_t) n + 2;
        return true;
    }
    if (t == '*' || t == '~' || t == '>' || t == '%')
    {
        size_t p = pos + 1;
        long n;
        if (!pxReadLong(d, p, n))
            return false;
        if (n < 0)
        {
            pos = p;
            return true;
        }
        immutable elems = (t == '%') ? cast(size_t)(n * 2) : cast(size_t) n;
        foreach (_; 0 .. elems)
            if (!pxScanOne(d, p))
                return false;
        pos = p;
        return true;
    }
    // +simple, -error, :int, and RESP3 _,#,,,( — all line-terminated
    return pxScanLine(d, pos);
}

private bool pxScanLine(scope const(ubyte)[] d, ref size_t pos) @trusted @nogc nothrow
{
    size_t i = pos;
    while (i + 1 < d.length && !(d[i] == '\r' && d[i + 1] == '\n'))
        i++;
    if (i + 1 >= d.length)
        return false;
    pos = i + 2;
    return true;
}

private bool pxReadLong(scope const(ubyte)[] d, ref size_t pos, out long v) @trusted @nogc nothrow
{
    bool neg = false;
    size_t i = pos;
    if (i < d.length && d[i] == '-')
    {
        neg = true;
        i++;
    }
    bool any = false;
    long acc = 0;
    while (i < d.length && d[i] >= '0' && d[i] <= '9')
    {
        acc = acc * 10 + (d[i] - '0');
        i++;
        any = true;
    }
    if (!any || i + 1 >= d.length || d[i] != '\r' || d[i + 1] != '\n')
        return false;
    v = neg ? -acc : acc;
    pos = i + 2;
    return true;
}

@trusted unittest // RESP reply framing across types + partial input
{
    static size_t frame(string s)
    {
        return respReplyComplete(cast(const(ubyte)[]) s);
    }

    assert(frame("+OK\r\n") == 5);
    assert(frame("-ERR x\r\n") == 8);
    assert(frame(":123\r\n") == 6);
    assert(frame("$5\r\nhello\r\n") == 11);
    assert(frame("$-1\r\n") == 5);
    assert(frame("*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n") == 22);
    assert(frame("*-1\r\n") == 5);
    assert(frame("*2\r\n*1\r\n:9\r\n$1\r\nx\r\n") == 19);
    assert(frame("$5\r\nhel") == 0);
    assert(frame("*2\r\n$3\r\nfoo\r\n") == 0);
    assert(frame("+OK\r") == 0);
    assert(frame("+OK\r\n:1\r\n") == 5);
}
