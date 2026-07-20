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
