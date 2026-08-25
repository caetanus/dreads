// RabbitMQ management HTTP API (production drop-in M4). A hand-rolled HTTP/1.1
// server (like dashboard.d — vibe-core has no HTTP, vibe-d's GC is what we
// avoid) on its own thread, port 15672. It reports the AMQP broker state that
// rabbitmqadmin, the management UI, monitoring probes, and k8s operators poll.
//
// v1 scope: the read plane (overview/queues/exchanges/bindings/vhosts/nodes/
// users/whoami/aliveness) backed by the RESP command bridge (queues ARE
// `amq.q.<name>` lists) and an AMQP-thread exchange snapshot, HTTP basic auth
// against the SAME ACL users as every skin, and queue declare/delete. Deferred
// to v2 (documented): connection listing + DELETE (cross-shard conn registry),
// per-message-rate stats.
module dreads.mgmt;

import vibe.core.net : listenTCP, TCPConnection;
import vibe.core.stream : IOMode;
import core.time : seconds;
import dreads.mem : ByteBuffer;
import dreads.config : gConfig, dreadsVersion;

// The RESP command bridge (dashboard.d owns the shard round-trip machinery).
import dreads.dashboard : runCommand, runCommandAdmin;

private __gshared bool gMgmtUp;

/// Exchange snapshot hook: an AMQP shard thread serializes its (replicated)
/// exchange set into `o` as `name\ttype\n` lines. Installed by the server when
/// the AMQP skin is up; null = report only the default exchanges.
public __gshared void function(ref ByteBuffer o) nothrow gMgmtExchanges;

/// Connection registry hooks (M4 v2): list connections as JSON, and
/// request-close by name (empty = all). Installed by the server with the AMQP
/// skin's cross-shard registry.
public __gshared void function(ref ByteBuffer o) nothrow gMgmtConnections;
public __gshared size_t function(scope const(char)[] name) nothrow gMgmtKillConn;

public void startManagement() nothrow
{
    import vibe.core.core : runTask;
    import core.stdc.stdio : printf;

    if (gConfig.mgmtPort == 0)
        return;
    try
    {
        listenTCP(gConfig.mgmtPort,
            delegate(TCPConnection conn) @trusted nothrow { onConn(conn); },
            gConfig.mgmtBind);
        gMgmtUp = true;
        printf("dreads management API on port %u\n", cast(uint) gConfig.mgmtPort);
    }
    catch (Exception)
    {
    }
}

// ---------------------------------------------------------------------------
// Connection handling
// ---------------------------------------------------------------------------

private void onConn(TCPConnection conn) @trusted nothrow
{
    try
    {
        ubyte[8192] buf = void;
        size_t n;
        if (conn.waitForData(10.seconds))
            n = conn.read(buf[], IOMode.once);
        if (n == 0)
        {
            conn.close();
            return;
        }
        auto req = cast(const(char)[]) buf[0 .. n];
        auto method = httpMethod(req);
        auto target = httpPath(req);
        // strip query string
        size_t qp = 0;
        while (qp < target.length && target[qp] != '?')
            qp++;
        auto path = target[0 .. qp];

        // basic auth against ACL (a real deployment always sets it; when only
        // the seeded default user exists, auth is accept-any like the skins)
        if (!authOk(req))
        {
            enum R401 = "HTTP/1.1 401 Unauthorized\r\n"
                ~ "WWW-Authenticate: Basic realm=\"dreads\"\r\n"
                ~ "Content-Length: 0\r\nConnection: close\r\n\r\n";
            conn.write(cast(const(ubyte)[]) R401);
            conn.close();
            return;
        }

        ByteBuffer body_;
        route(method, path, body_);
        if (body_.empty)
            writeStatus(conn, 404, "Not Found", "{\"error\":\"not_found\"}");
        else
            writeJson(conn, cast(const(char)[]) body_.data);
        conn.close();
    }
    catch (Exception)
    {
    }
}

private void route(scope const(char)[] method, scope const(char)[] path, ref ByteBuffer o) @trusted
{
    if (method == "GET")
    {
        if (path == "/api/overview")
            overview(o);
        else if (path == "/api/whoami")
            o.append(`{"name":"guest","tags":["administrator"]}`);
        else if (path == "/api/vhosts")
            o.append(`[{"name":"/","tracing":false}]`);
        else if (path == "/api/nodes")
            nodes(o);
        else if (path == "/api/connections")
        {
            if (gMgmtConnections !is null)
                gMgmtConnections(o);
            else
                o.append("[]");
        }
        else if (path == "/api/channels")
            o.append("[]");
        else if (path == "/api/users")
            users(o);
        else if (path == "/api/exchanges" || path == "/api/exchanges/%2F"
                || path == "/api/exchanges/%2f")
            exchanges(o);
        else if (path == "/api/bindings")
            o.append("[]"); // v2: binding enumeration snapshot
        else if (path == "/api/queues" || path == "/api/queues/%2F"
                || path == "/api/queues/%2f")
            queues(o, null);
        else if (path.length > 15 && (path[0 .. 15] == "/api/queues/%2F"
                || path[0 .. 15] == "/api/queues/%2f") && path[15] == '/')
            queues(o, path[16 .. $]); // one queue
        else if (path.length >= 20 && path[0 .. 20] == "/api/aliveness-test/")
            o.append(`{"status":"ok"}`);
    }
    else if (method == "PUT")
    {
        // PUT /api/queues/%2F/<name>: queues are lazy (created on first use),
        // so a declare just confirms — RabbitMQ returns 201/204
        if (isQueuePath(path))
            o.append(`{"ok":true}`);
    }
    else if (method == "DELETE")
    {
        if (path.length > 21 && path[0 .. 21] == "/api/connections/name" && path[21] == '/')
        {
            // rabbitmqctl close_all_connections shims to DELETE here
            if (gMgmtKillConn !is null)
                cast(void) gMgmtKillConn(urlDecode(path[22 .. $]));
            o.append(`{"ok":true}`);
        }
        else if (path.length > 16 && path[0 .. 16] == "/api/connections" && path[16] == '/')
        {
            if (gMgmtKillConn !is null)
                cast(void) gMgmtKillConn(urlDecode(path[17 .. $]));
            o.append(`{"ok":true}`);
        }
        else if (isQueuePath(path))
        {
            auto name = queueName(path);
            if (name.length)
            {
                static ByteBuffer key, cmd, reply;
                key.clear();
                key.append("amq.q.");
                key.append(name);
                cmd.clear();
                respCmd(cmd, "DEL", cast(const(char)[]) key.data);
                cast(void) runCommandAdmin(cmd.data, cast(ushort) gConfig.amqpDb, reply);
                o.append(`{"ok":true}`);
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Endpoint bodies
// ---------------------------------------------------------------------------

private void overview(ref ByteBuffer o) @trusted
{
    immutable nq = queueCount();
    o.append(`{"management_version":"3.13.0","product_name":"dreads",`);
    o.append(`"product_version":"`);
    o.append(dreadsVersion);
    o.append(`","rabbitmq_version":"3.13.0","erlang_version":"26.0",`);
    o.append(`"cluster_name":"dreads","node":"dreads@localhost",`);
    o.append(`"object_totals":{"connections":0,"channels":0,"exchanges":7,"queues":`);
    appendUint(o, nq);
    o.append(`,"consumers":0},`);
    o.append(`"listeners":[{"node":"dreads@localhost","protocol":"amqp","port":`);
    appendUint(o, gConfig.amqpPort);
    o.append(`}],"queue_totals":{"messages":0,"messages_ready":0,`);
    o.append(`"messages_unacknowledged":0}}`);
}

private void nodes(ref ByteBuffer o) @trusted
{
    o.append(`[{"name":"dreads@localhost","type":"disc","running":true,`);
    o.append(`"mem_used":0,"fd_used":0,"sockets_used":0,"proc_used":0,`);
    o.append(`"uptime":0,"run_queue":0,"processors":1}]`);
}

private void users(ref ByteBuffer o) @trusted
{
    import dreads.acl : aclEachUser, AclUser;

    o.append("[");
    bool first = true;
    cast(void) aclEachUser((AclUser* u) @trusted @nogc nothrow {
        if (!first)
            o.append(",");
        first = false;
        o.append(`{"name":"`);
        appendJsonStr(o, u.name);
        o.append(`","tags":["administrator"]}`);
        return 0;
    });
    o.append("]");
}

// The 7 default AMQP exchanges every broker has, plus any the snapshot hook
// reports. RabbitMQ's fixed set: "", amq.direct, amq.fanout, amq.topic,
// amq.headers, amq.match, amq.rabbitmq.trace.
private void exchanges(ref ByteBuffer o) @trusted
{
    static immutable string[7][2] def = [
        ["", "amq.direct", "amq.fanout", "amq.topic", "amq.headers", "amq.match",
            "amq.rabbitmq.trace"],
        ["direct", "direct", "fanout", "topic", "headers", "headers", "topic"],
    ];
    o.append("[");
    foreach (i; 0 .. 7)
    {
        if (i)
            o.append(",");
        exchangeObj(o, def[0][i], def[1][i]);
    }
    // user-declared exchanges from the AMQP-thread snapshot (name\ttype\n lines)
    if (gMgmtExchanges !is null)
    {
        static ByteBuffer snap;
        snap.clear();
        gMgmtExchanges(snap);
        auto d = cast(const(char)[]) snap.data;
        size_t i = 0;
        while (i < d.length)
        {
            size_t nl = i;
            while (nl < d.length && d[nl] != '\n')
                nl++;
            auto line = d[i .. nl];
            size_t tab = 0;
            while (tab < line.length && line[tab] != '\t')
                tab++;
            if (tab < line.length && line[0 .. tab].length)
            {
                o.append(",");
                exchangeObj(o, line[0 .. tab], line[tab + 1 .. $]);
            }
            i = nl + 1;
        }
    }
    o.append("]");
}

private void exchangeObj(ref ByteBuffer o, scope const(char)[] name, scope const(char)[] type) @trusted
{
    o.append(`{"name":"`);
    appendJsonStr(o, name);
    o.append(`","vhost":"/","type":"`);
    o.append(type);
    o.append(`","durable":true,"auto_delete":false,"internal":false,"arguments":{}}`);
}

// GET queues: enumerate `amq.q.*` via the command bridge, LLEN each for depth.
// `only` non-null => a single named queue object (not an array).
private void queues(ref ByteBuffer o, scope const(char)[] only) @trusted
{
    static ByteBuffer cmd, reply;
    if (only !is null)
    {
        // one queue: report it (queues are lazy, so "exists" = has messages or
        // was declared; we report depth via LLEN, 0 if absent)
        immutable depth = queueDepth(only);
        queueObj(o, only, depth);
        return;
    }
    // KEYS amq.q.* — the management API is not a hot path; KEYS is acceptable
    cmd.clear();
    respCmd(cmd, "KEYS", "amq.q.*");
    if (!runCommand(cmd.data, cast(ushort) gConfig.amqpDb, reply))
    {
        o.append("[]");
        return;
    }
    o.append("[");
    bool first = true;
    // parse the RESP array of bulk strings
    auto d = cast(const(char)[]) reply.data;
    size_t i = 0;
    if (d.length && d[0] == '*')
    {
        i = 1;
        long cnt = 0;
        while (i < d.length && d[i] != '\r')
            cnt = cnt * 10 + (d[i++] - '0');
        i += 2;
        foreach (_; 0 .. cnt)
        {
            if (i >= d.length || d[i] != '$')
                break;
            i++;
            long bl = 0;
            while (i < d.length && d[i] != '\r')
                bl = bl * 10 + (d[i++] - '0');
            i += 2;
            if (bl < 0 || i + bl + 2 > d.length)
                break;
            auto key = d[i .. i + cast(size_t) bl];
            i += cast(size_t) bl + 2;
            if (key.length <= 6 || key[0 .. 6] != "amq.q.")
                continue;
            auto name = key[6 .. $];
            if (!first)
                o.append(",");
            first = false;
            queueObj(o, name, queueDepth(name));
        }
    }
    o.append("]");
}

private void queueObj(ref ByteBuffer o, scope const(char)[] name, long depth) @trusted
{
    o.append(`{"name":"`);
    appendJsonStr(o, name);
    o.append(`","vhost":"/","durable":true,"auto_delete":false,"exclusive":false,`);
    o.append(`"arguments":{},"node":"dreads@localhost","state":"running",`);
    o.append(`"messages":`);
    appendUint(o, depth < 0 ? 0 : cast(size_t) depth);
    o.append(`,"messages_ready":`);
    appendUint(o, depth < 0 ? 0 : cast(size_t) depth);
    o.append(`,"messages_unacknowledged":0,"consumers":0}`);
}

private long queueDepth(scope const(char)[] name) @trusted
{
    static ByteBuffer key, cmd, reply;
    key.clear();
    key.append("amq.q.");
    key.append(name);
    cmd.clear();
    respCmd(cmd, "LLEN", cast(const(char)[]) key.data);
    if (!runCommand(cmd.data, cast(ushort) gConfig.amqpDb, reply))
        return -1;
    auto d = cast(const(char)[]) reply.data;
    if (d.length < 2 || d[0] != ':')
        return -1;
    long v = 0;
    foreach (c; d[1 .. $])
    {
        if (c == '\r')
            break;
        v = v * 10 + (c - '0');
    }
    return v;
}

private size_t queueCount() @trusted
{
    static ByteBuffer cmd, reply;
    cmd.clear();
    respCmd(cmd, "KEYS", "amq.q.*");
    if (!runCommand(cmd.data, cast(ushort) gConfig.amqpDb, reply))
        return 0;
    auto d = cast(const(char)[]) reply.data;
    if (d.length == 0 || d[0] != '*')
        return 0;
    size_t i = 1, v = 0;
    while (i < d.length && d[i] != '\r')
        v = v * 10 + (d[i++] - '0');
    return v;
}

// ---------------------------------------------------------------------------
// HTTP + auth helpers
// ---------------------------------------------------------------------------

private bool authOk(scope const(char)[] req) @trusted
{
    import dreads.acl : aclUser, aclCheckPassword, aclUserCount;
    import dreads.tls : b64dec;

    // only the seeded default user => accept-any (mirrors the skins' gate)
    if (aclUserCount() <= 1)
        return true;
    auto h = httpHeader(req, "Authorization");
    if (h.length < 6 || h[0 .. 6] != "Basic ")
        return false;
    ubyte[256] db = void;
    auto dec = b64dec(h[6 .. $], db);
    if (dec is null)
        return false;
    auto s = cast(const(char)[]) dec;
    size_t c = 0;
    while (c < s.length && s[c] != ':')
        c++;
    if (c >= s.length)
        return false;
    auto user = s[0 .. c];
    auto pass = s[c + 1 .. $];
    auto u = aclUser(user);
    if (u is null || !u.enabled)
        return false;
    try
        return aclCheckPassword(u, pass);
    catch (Exception)
        return false;
}

private bool isQueuePath(scope const(char)[] path) @safe @nogc nothrow
{
    return path.length > 16 && (path[0 .. 15] == "/api/queues/%2F"
            || path[0 .. 15] == "/api/queues/%2f") && path[15] == '/';
}

private const(char)[] queueName(return scope const(char)[] path) @safe @nogc nothrow
{
    return isQueuePath(path) ? path[16 .. $] : null;
}

private void writeJson(TCPConnection conn, scope const(char)[] json) @trusted
{
    import core.stdc.stdio : snprintf;

    char[160] hdr = void;
    immutable hn = snprintf(hdr.ptr, hdr.length,
        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: %zu\r\nConnection: close\r\n\r\n",
        json.length);
    if (hn > 0)
    {
        try
        {
            conn.write(cast(const(ubyte)[]) hdr[0 .. hn]);
            conn.write(cast(const(ubyte)[]) json);
        }
        catch (Exception)
        {
        }
    }
}

private void writeStatus(TCPConnection conn, int code, scope const(char)[] msg,
        scope const(char)[] json) @trusted
{
    import core.stdc.stdio : snprintf;

    char[192] hdr = void;
    immutable hn = snprintf(hdr.ptr, hdr.length,
        "HTTP/1.1 %d %.*s\r\nContent-Type: application/json\r\nContent-Length: %zu\r\nConnection: close\r\n\r\n",
        code, cast(int) msg.length, msg.ptr, json.length);
    if (hn > 0)
    {
        try
        {
            conn.write(cast(const(ubyte)[]) hdr[0 .. hn]);
            conn.write(cast(const(ubyte)[]) json);
        }
        catch (Exception)
        {
        }
    }
}

private const(char)[] httpMethod(return scope const(char)[] req) @safe @nogc nothrow
{
    size_t e = 0;
    while (e < req.length && req[e] != ' ')
        e++;
    return req[0 .. e];
}

private const(char)[] httpPath(return scope const(char)[] req) @safe @nogc nothrow
{
    size_t s = 0;
    while (s < req.length && req[s] != ' ')
        s++;
    s++;
    size_t e = s;
    while (e < req.length && req[e] != ' ')
        e++;
    return s <= req.length && e <= req.length && s <= e ? req[s .. e] : null;
}

private const(char)[] httpHeader(return scope const(char)[] req, scope const(char)[] name) @safe @nogc nothrow
{
    size_t i = 0;
    while (i < req.length)
    {
        size_t ls = i;
        size_t le = ls;
        while (le < req.length && req[le] != '\r' && req[le] != '\n')
            le++;
        auto line = req[ls .. le];
        size_t colon = 0;
        while (colon < line.length && line[colon] != ':')
            colon++;
        if (colon < line.length && ciEq(line[0 .. colon], name))
        {
            size_t vs = colon + 1;
            while (vs < line.length && (line[vs] == ' ' || line[vs] == '\t'))
                vs++;
            return line[vs .. $];
        }
        i = le;
        while (i < req.length && (req[i] == '\r' || req[i] == '\n'))
            i++;
        if (le == ls) // blank line = end of headers
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

private void appendUint(ref ByteBuffer o, size_t v) @trusted @nogc nothrow
{
    char[20] t = void;
    size_t n;
    if (v == 0)
        t[n++] = '0';
    else
        while (v)
        {
            t[n++] = cast(char)('0' + v % 10);
            v /= 10;
        }
    char[20] r = void;
    foreach (i; 0 .. n)
        r[i] = t[n - 1 - i];
    o.append(r[0 .. n]);
}

private void appendJsonStr(ref ByteBuffer o, scope const(char)[] s) @trusted @nogc nothrow
{
    foreach (c; s)
    {
        if (c == '"' || c == '\\')
        {
            o.appendByte('\\');
            o.appendByte(c);
        }
        else if (c == '\n')
            o.append("\\n");
        else if (c == '\r')
            o.append("\\r");
        else if (c == '\t')
            o.append("\\t");
        else if (cast(ubyte) c < 0x20)
        {
            // control char -> \u00XX
            static immutable hex = "0123456789abcdef";
            o.append("\\u00");
            o.appendByte(hex[(c >> 4) & 0xF]);
            o.appendByte(hex[c & 0xF]);
        }
        else
            o.appendByte(c);
    }
}

// Percent-decode into a TLS scratch (connection names carry %3A for ':').
private const(char)[] urlDecode(scope const(char)[] s) @trusted nothrow
{
    static ByteBuffer db;
    db.clear();
    size_t i = 0;
    while (i < s.length)
    {
        if (s[i] == '%' && i + 2 < s.length)
        {
            int hi = hexv(s[i + 1]), lo = hexv(s[i + 2]);
            if (hi >= 0 && lo >= 0)
            {
                db.appendByte(cast(char)((hi << 4) | lo));
                i += 3;
                continue;
            }
        }
        db.appendByte(s[i]);
        i++;
    }
    return cast(const(char)[]) db.data;
}

private int hexv(char c) @safe @nogc nothrow
{
    if (c >= '0' && c <= '9')
        return c - '0';
    if (c >= 'a' && c <= 'f')
        return c - 'a' + 10;
    if (c >= 'A' && c <= 'F')
        return c - 'A' + 10;
    return -1;
}

private void respCmd(ref ByteBuffer o, scope const(char)[][] args...) @trusted
{
    import core.stdc.stdio : snprintf;

    char[24] t = void;
    int n = snprintf(t.ptr, t.length, "*%zu\r\n", args.length);
    o.append(t[0 .. n]);
    foreach (a; args)
    {
        n = snprintf(t.ptr, t.length, "$%zu\r\n", a.length);
        o.append(t[0 .. n]);
        o.append(a);
        o.append("\r\n");
    }
}
