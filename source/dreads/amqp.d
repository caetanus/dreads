module dreads.amqp;

// AMQP 0-9-1 frontend — the SECOND non-RESP skin over the sharded core.
//
// The structural bet: an AMQP QUEUE IS A LIST in the keyspace of the shard
// that owns keyToSlot(queueName). basic.publish RPUSHes through the same data
// plane every RESP write uses — which means queues are DURABLE via the
// per-shard AOF for free, survive kill -9, and are inspectable from the RESP
// side (`LRANGE amq.q.<name> 0 -1`). One engine, protocol faces.
//
// Architecture (mirrors the MQTT skin):
//   - per-shard SO_REUSEPORT listeners on --amqp-port, a fiber per connection;
//   - exchange/binding metadata is THREAD-LOCAL, replicated by broadcast over
//     the SPSC fabric (declares/binds are control-plane-rare);
//   - queue data lives on the owner shard, reached via hooks installed by
//     server.d (self-shard direct dispatch or a cross-shard hop).
//
// v1 scope (documented): PLAIN auth accepted (any), one vhost, exchanges
// direct/fanout/topic (AMQP topic wildcards * and # on dot-segments),
// queue.declare/bind, basic.publish, basic.get, basic.consume with
// AUTO-ACK semantics (explicit acks accepted and ignored — redelivery on
// crash needs a PEL and comes with the stream-backed v2), publisher confirms
// (confirm.select), channel/connection close. No heartbeat enforcement, no
// exclusive/passive semantics, no basic.qos windows (consumers poll).

import core.atomic : atomicLoad, atomicOp, MemoryOrder;

import vibe.core.core : runTask, sleep;
import vibe.core.net : TCPConnection;
import vibe.core.sync : TaskMutex;
import core.time : msecs;

import dreads.mem : ByteBuffer;

// ---------------------------------------------------------------------------
// Hooks installed by server.d (avoid an import cycle): queue data-plane ops
// and control-plane replication.
public __gshared void delegate(scope const(char)[] key, scope const(char)[] payload) nothrow gAmqpPush;
public __gshared void delegate(scope const(char)[] key, scope const(char)[] payload) nothrow gAmqpPushFront;
public __gshared bool delegate(scope const(char)[] key, ref ByteBuffer outPayload) nothrow gAmqpPop;
public __gshared long delegate(scope const(char)[] key) nothrow gAmqpLen;
public __gshared void delegate(scope const(ubyte)[] ctl) nothrow gAmqpCtlFanout;

public shared long gAmqpConsumers; // gate: publish-side wake fan-out etc (future)

/// Queue key namespace: visible from RESP on purpose (cross-protocol is a
/// feature — `LRANGE amq.q.tasks 0 -1` shows the queue).
public void queueKey(scope const(char)[] q, ref ByteBuffer o) @nogc nothrow
{
    o.clear();
    o.append("amq.q.");
    o.append(q);
}

// ---------------------------------------------------------------------------
// Control plane: exchanges + bindings (THREAD-LOCAL, broadcast-replicated).

private enum ExType : ubyte
{
    direct = 0,
    fanout = 1,
    topic = 2,
    headers = 3,
}

private struct Binding
{
    string queue;
    string key; // binding/routing key
    immutable(ubyte)[] args; // raw binding-arguments table (headers exchanges)
}

private struct QueueMeta
{
    string dlx; // x-dead-letter-exchange ("" = none)
    string dlrk; // x-dead-letter-routing-key ("" = original queue name)
}

private QueueMeta[string] gQueueMeta; // TLS, broadcast-replicated

private ExType[string] gExchanges; // TLS
private Binding[][string] gBindings; // TLS: exchange -> bindings

/// AMQP topic match: dot-separined; `*` = exactly one word, `#` = zero+ words.
package bool amqpTopicMatches(scope const(char)[] pattern, scope const(char)[] key) @nogc nothrow
{
    size_t pi = 0, ki = 0;
    for (;;)
    {
        size_t pe = pi;
        while (pe < pattern.length && pattern[pe] != '.')
            pe++;
        auto pseg = pattern[pi .. pe];
        if (pseg == "#")
        {
            if (pe >= pattern.length)
                return true; // trailing # swallows the rest (incl. zero words)
            // '#' mid-pattern: try to match the remainder at every position
            auto rest = pattern[pe + 1 .. $];
            size_t k2 = ki;
            for (;;)
            {
                if (amqpTopicMatches(rest, key[k2 .. $]))
                    return true;
                while (k2 < key.length && key[k2] != '.')
                    k2++;
                if (k2 >= key.length)
                    return false;
                k2++;
            }
        }
        size_t ke = ki;
        while (ke < key.length && key[ke] != '.')
            ke++;
        auto kseg = key[ki .. ke];
        if (ki > key.length)
            return false;
        if (pseg != "*" && pseg != kseg)
            return false;
        immutable pDone = pe >= pattern.length;
        immutable kDone = ke >= key.length;
        if (pDone || kDone)
        {
            if (kDone && !pDone && pattern[pe + 1 .. $] == "#")
                return true;
            return pDone && kDone;
        }
        pi = pe + 1;
        ki = ke + 1;
    }
}

// Apply a control op locally. Wire: [op u8][len u16][exchange][len u16][a][len u16][b]
//   op 1 = exchange.declare (a = type name)
//   op 2 = queue.bind       (a = queue, b = routing key)
public void amqpApplyCtl(scope const(ubyte)[] p) nothrow @trusted
{
    try
    {
        size_t i = 0;
        if (p.length < 1)
            return;
        immutable op = p[i++];
        const(char)[] rd()
        {
            if (i + 2 > p.length)
                return null;
            immutable n = (cast(size_t) p[i] << 8) | p[i + 1];
            i += 2;
            if (i + n > p.length)
                return null;
            auto s = cast(const(char)[]) p[i .. i + n];
            i += n;
            return s;
        }

        auto ex = rd().idup;
        auto a = rd().idup;
        if (op == 1)
        {
            ExType t = a == "fanout" ? ExType.fanout : a == "topic" ? ExType.topic
                : a == "headers" ? ExType.headers : ExType.direct;
            gExchanges[ex] = t;
        }
        else if (op == 2)
        {
            auto b = rd().idup;
            auto extra = rd(); // raw binding args table (may be empty)
            gBindings[ex] ~= Binding(a, b, extra is null ? null : cast(immutable(ubyte)[]) extra.idup);
        }
        else if (op == 3) // queue metadata: ex=queue, a=dlx, b=dlrk
        {
            auto b = rd().idup;
            gQueueMeta[ex] = QueueMeta(a, b);
        }
    }
    catch (Exception)
    {
    }
}

private void ctlBroadcast(ubyte op, scope const(char)[] ex, scope const(char)[] a,
        scope const(char)[] b, scope const(ubyte)[] extra = null) nothrow @trusted
{
    static ByteBuffer cb; // TLS
    cb.clear();
    cb.appendByte(cast(char) op);
    void put(scope const(char)[] s)
    {
        cb.appendByte(cast(char)(s.length >> 8));
        cb.appendByte(cast(char)(s.length & 0xFF));
        cb.append(s);
    }

    put(ex);
    put(a);
    put(b);
    put(cast(const(char)[]) extra);
    amqpApplyCtl(cb.data); // local first
    if (gAmqpCtlFanout !is null)
        gAmqpCtlFanout(cb.data);
}

/// Headers-exchange match: binding args carry x-match (all|any, default all)
/// plus wanted key/values; string values compared, other types by presence.
private bool headersMatch(scope const(ubyte)[] bindArgs,
        scope const(ubyte)[] msgHeaders) @nogc nothrow
{
    if (bindArgs is null)
        return false;
    bool any = false;
    {
        auto xm = tableGetStr(bindArgs, "x-match");
        any = xm == "any";
    }
    bool allOk = true;
    bool anyOk = false;
    bool sawWant = false;
    cast(void) tableWalk(bindArgs, (scope const(char)[] k, char ty, scope const(ubyte)[] v) @nogc nothrow {
        if (k.length >= 2 && k[0] == 'x' && k[1] == '-')
            return true; // x-match etc are directives, not header wants
        sawWant = true;
        bool hit = false;
        if (msgHeaders !is null)
        {
            if (ty == 'S' || ty == 's')
            {
                auto mv = tableGetStr(msgHeaders, k);
                hit = mv !is null && mv == cast(const(char)[]) v;
            }
            else
            {
                // non-string want: match on key presence
                cast(void) tableWalk(msgHeaders, (scope const(char)[] mk, char mt,
                        scope const(ubyte)[] mv2) @nogc nothrow {
                    if (mk == k)
                    {
                        hit = true;
                        return false;
                    }
                    return true;
                });
            }
        }
        if (hit)
            anyOk = true;
        else
            allOk = false;
        return true;
    });
    if (!sawWant)
        return false;
    return any ? anyOk : allOk;
}

/// Route (exchange, routingKey) -> queue names, calling sink for each.
private void routeTo(scope const(char)[] ex, scope const(char)[] rkey,
        scope const(ubyte)[] msgHeaders,
        scope void delegate(string q) nothrow sink) nothrow @trusted
{
    try
    {
        if (ex.length == 0)
        {
            // default exchange: routing key IS the queue name
            sink(rkey.idup);
            return;
        }
        auto t = (cast(string) ex) in gExchanges;
        auto bl = (cast(string) ex) in gBindings;
        if (t is null || bl is null)
            return;
        final switch (*t)
        {
        case ExType.fanout:
            foreach (ref bd; *bl)
                sink(bd.queue);
            break;
        case ExType.direct:
            foreach (ref bd; *bl)
                if (bd.key == rkey)
                    sink(bd.queue);
            break;
        case ExType.topic:
            foreach (ref bd; *bl)
                if (amqpTopicMatches(bd.key, rkey))
                    sink(bd.queue);
            break;
        case ExType.headers:
            foreach (ref bd; *bl)
                if (headersMatch(bd.args, msgHeaders))
                    sink(bd.queue);
            break;
        }
    }
    catch (Exception)
    {
    }
}

// ---------------------------------------------------------------------------
// Frame codec

private enum ubyte FRAME_METHOD = 1, FRAME_HEADER = 2, FRAME_BODY = 3,
        FRAME_HEARTBEAT = 8, FRAME_END = 0xCE;

private void frameStart(ref ByteBuffer o, ubyte type, ushort chan, out size_t sizeAt) @nogc nothrow
{
    o.appendByte(cast(char) type);
    o.appendByte(cast(char)(chan >> 8));
    o.appendByte(cast(char)(chan & 0xFF));
    sizeAt = o.length;
    o.append("\0\0\0\0"); // patched by frameFinish
}

private void frameFinish(ref ByteBuffer o, size_t sizeAt) @nogc nothrow @trusted
{
    immutable size = o.length - sizeAt - 4;
    auto d = cast(ubyte[]) o.data;
    d[sizeAt] = cast(ubyte)(size >> 24);
    d[sizeAt + 1] = cast(ubyte)(size >> 16);
    d[sizeAt + 2] = cast(ubyte)(size >> 8);
    d[sizeAt + 3] = cast(ubyte)(size & 0xFF);
    o.appendByte(cast(char) FRAME_END);
}

private void putU16(ref ByteBuffer o, ushort v) @nogc nothrow
{
    o.appendByte(cast(char)(v >> 8));
    o.appendByte(cast(char)(v & 0xFF));
}

private void putU32(ref ByteBuffer o, uint v) @nogc nothrow
{
    o.appendByte(cast(char)(v >> 24));
    o.appendByte(cast(char)(v >> 16));
    o.appendByte(cast(char)(v >> 8));
    o.appendByte(cast(char)(v & 0xFF));
}

private void putU64(ref ByteBuffer o, ulong v) @nogc nothrow
{
    putU32(o, cast(uint)(v >> 32));
    putU32(o, cast(uint) v);
}

private void putShortStr(ref ByteBuffer o, scope const(char)[] s) @nogc nothrow
{
    o.appendByte(cast(char) s.length);
    o.append(s);
}

private void putLongStr(ref ByteBuffer o, scope const(char)[] s) @nogc nothrow
{
    putU32(o, cast(uint) s.length);
    o.append(s);
}

private struct Rd
{
    const(ubyte)[] p;
    size_t i;
    bool ok = true;

    ubyte u8() @nogc nothrow
    {
        if (i + 1 > p.length)
        {
            ok = false;
            return 0;
        }
        return p[i++];
    }

    ushort u16() @nogc nothrow
    {
        if (i + 2 > p.length)
        {
            ok = false;
            return 0;
        }
        auto v = cast(ushort)((p[i] << 8) | p[i + 1]);
        i += 2;
        return v;
    }

    uint u32() @nogc nothrow
    {
        if (i + 4 > p.length)
        {
            ok = false;
            return 0;
        }
        uint v = (cast(uint) p[i] << 24) | (cast(uint) p[i + 1] << 16)
            | (cast(uint) p[i + 2] << 8) | p[i + 3];
        i += 4;
        return v;
    }

    ulong u64() @nogc nothrow
    {
        ulong hi = u32();
        return (hi << 32) | u32();
    }

    const(char)[] shortStr() @nogc nothrow
    {
        immutable n = u8();
        if (!ok || i + n > p.length)
        {
            ok = false;
            return null;
        }
        auto s = cast(const(char)[]) p[i .. i + n];
        i += n;
        return s;
    }

    const(char)[] longStr() @nogc nothrow
    {
        immutable n = u32();
        if (!ok || i + n > p.length)
        {
            ok = false;
            return null;
        }
        auto s = cast(const(char)[]) p[i .. i + n];
        i += n;
        return s;
    }

    void skipTable() @nogc nothrow
    {
        immutable n = u32();
        if (!ok || i + n > p.length)
        {
            ok = false;
            return;
        }
        i += n;
    }

    /// The raw bytes of the table at the cursor (content only), cursor advanced.
    const(ubyte)[] tableRaw() @nogc nothrow
    {
        immutable n = u32();
        if (!ok || i + n > p.length)
        {
            ok = false;
            return null;
        }
        auto t = p[i .. i + n];
        i += n;
        return t;
    }
}

/// Walk a field-table's CONTENT, calling dg(key, type, valueSlice) per entry
/// (valueSlice = the value bytes for string types, raw bytes otherwise).
/// Returns false on malformed input.
package bool tableWalk(scope const(ubyte)[] t,
        scope bool delegate(scope const(char)[] key, char type,
            scope const(ubyte)[] val) @nogc nothrow dg) @nogc nothrow
{
    size_t i = 0;
    while (i < t.length)
    {
        if (i + 1 > t.length)
            return false;
        immutable kn = t[i++];
        if (i + kn + 1 > t.length)
            return false;
        auto key = cast(const(char)[]) t[i .. i + kn];
        i += kn;
        immutable char ty = cast(char) t[i++];
        size_t vlen;
        size_t voff = i;
        switch (ty)
        {
        case 'S', 'x', 'A', 'F':
            if (i + 4 > t.length)
                return false;
            vlen = (cast(size_t) t[i] << 24) | (cast(size_t) t[i + 1] << 16)
                | (cast(size_t) t[i + 2] << 8) | t[i + 3];
            voff = i + 4;
            i = voff + vlen;
            break;
        case 's':
            if (i + 1 > t.length)
                return false;
            vlen = t[i];
            voff = i + 1;
            i = voff + vlen;
            break;
        case 't', 'b', 'B':
            vlen = 1;
            i += 1;
            break;
        case 'U', 'u':
            vlen = 2;
            i += 2;
            break;
        case 'I', 'i', 'f':
            vlen = 4;
            i += 4;
            break;
        case 'l', 'd', 'T':
            vlen = 8;
            i += 8;
            break;
        case 'D':
            vlen = 5;
            i += 5;
            break;
        case 'V':
            vlen = 0;
            break;
        default:
            return false; // unknown type tag
        }
        if (i > t.length)
            return false;
        if (!dg(key, ty, t[voff .. voff + vlen]))
            return true; // early stop by consumer
    }
    return true;
}

/// Fetch a string-typed value ('S'/'s') by key from a table's content.
package const(char)[] tableGetStr(return scope const(ubyte)[] t, scope const(char)[] key) @nogc nothrow
{
    const(char)[] found = null;
    cast(void) tableWalk(t, (scope const(char)[] k, char ty, scope const(ubyte)[] v) @nogc nothrow {
        if (k == key && (ty == 'S' || ty == 's'))
        {
            found = cast(const(char)[]) v;
            return false;
        }
        return true;
    });
    return found;
}

/// Extract the headers table content from a basic-properties block
/// (property-flags u16; headers is flag bit 13, preceded by content-type
/// bit 15 and content-encoding bit 14 when present).
package const(ubyte)[] propsHeaders(return scope const(ubyte)[] props) @nogc nothrow
{
    if (props.length < 2)
        return null;
    immutable flags = (cast(ushort) props[0] << 8) | props[1];
    size_t i = 2;
    static bool skipShort(scope const(ubyte)[] p, ref size_t i) @nogc nothrow
    {
        if (i + 1 > p.length)
            return false;
        immutable n = p[i];
        i += 1 + n;
        return i <= p.length;
    }

    if (flags & 0x8000) // content-type
        if (!skipShort(props, i))
            return null;
    if (flags & 0x4000) // content-encoding
        if (!skipShort(props, i))
            return null;
    if (!(flags & 0x2000)) // no headers
        return null;
    if (i + 4 > props.length)
        return null;
    immutable n = (cast(size_t) props[i] << 24) | (cast(size_t) props[i + 1] << 16)
        | (cast(size_t) props[i + 2] << 8) | props[i + 3];
    i += 4;
    if (i + n > props.length)
        return null;
    return props[i .. i + n];
}

// ---------------------------------------------------------------------------
// Connection / channel state

private struct PendingPub
{
    bool active;
    string exchange;
    string rkey;
    ulong bodySize;
    ByteBuffer payload;
    ByteBuffer props; // property-flags + property-list from the content header
}

private struct Channel
{
    bool open;
    bool confirmMode;
    ulong confirmSeq; // next publish seq (delivery-tag for basic.ack confirms)
    PendingPub pub;
}

private struct Unacked
{
    string queue;
    const(ubyte)[] blob; // stored record (props+body), for requeue/dead-letter
}

private final class AmqpConn
{
    TCPConnection tcp;
    TaskMutex wlock;
    Channel[ushort] chans;
    bool closing;
    bool[string] cancelledTags; // basic.cancel'ed consumer tags
    Unacked[ulong] unacked; // delivery-tag -> record (no_ack=false consumers)
    ulong nextTag = 1;

    this(TCPConnection c) nothrow
    {
        tcp = c;
        try
            wlock = new TaskMutex;
        catch (Exception)
            assert(false);
    }
}

private void sendTo(AmqpConn c, scope const(ubyte)[] bytes) nothrow
{
    try
    {
        c.wlock.lock();
        scope (exit)
            c.wlock.unlock();
        c.tcp.write(bytes);
    }
    catch (Exception)
    {
    }
}

// method frame helpers
private void method(ref ByteBuffer o, ushort chan, ushort cls, ushort mth,
        scope void delegate(ref ByteBuffer) @nogc nothrow args = null) nothrow
{
    size_t at;
    frameStart(o, FRAME_METHOD, chan, at);
    putU16(o, cls);
    putU16(o, mth);
    if (args !is null)
        args(o);
    frameFinish(o, at);
}

// ---------------------------------------------------------------------------
// Serve loop

public void serveAmqpClient(TCPConnection tcp) nothrow
{
    try
        tcp.tcpNoDelay = true;
    catch (Exception)
    {
    }
    auto c = new AmqpConn(tcp);
    static void closeQuiet(AmqpConn cc) nothrow
    {
        try
            cc.tcp.close();
        catch (Exception)
        {
        }
    }

    scope (exit)
    {
        c.closing = true;
        requeueAllUnacked(c);
        closeQuiet(c);
    }

    ByteBuffer inb;
    ByteBuffer outb;

    // protocol header: AMQP\0\x00\x09\x01
    {
        ubyte[8] hdr;
        size_t got = 0;
        while (got < 8)
        {
            try
            {
                if (!tcp.waitForData())
                    return;
                auto n = tcp.leastSize;
                if (n == 0)
                    return;
                auto want = 8 - got;
                auto take = n < want ? cast(size_t) n : want;
                tcp.read(hdr[got .. got + take]);
                got += take;
            }
            catch (Exception)
                return;
        }
        if (hdr[0 .. 4] != "AMQP")
            return;
        // Connection.Start — the server-properties table must ANNOUNCE the
        // capabilities we implement (pika refuses confirm.select client-side
        // unless `publisher_confirms` is advertised here).
        method(outb, 0, 10, 10, (ref ByteBuffer o) @nogc nothrow {
            o.appendByte(0); // version major
            o.appendByte(9); // minor
            // server-properties field table
            size_t tblAt = o.length;
            o.append("\0\0\0\0"); // table byte-length, patched below
            putShortStr(o, "product");
            o.appendByte('S');
            putLongStr(o, "dreads");
            putShortStr(o, "capabilities");
            o.appendByte('F');
            size_t capAt = o.length;
            o.append("\0\0\0\0"); // inner table length, patched below
            putShortStr(o, "publisher_confirms");
            o.appendByte('t');
            o.appendByte(1);
            putShortStr(o, "basic.nack"); // pika gates confirms on BOTH caps
            o.appendByte('t');
            o.appendByte(1);
            // patch inner then outer lengths
            auto d = cast(ubyte[]) o.data;
            immutable capLen = o.length - capAt - 4;
            d[capAt] = cast(ubyte)(capLen >> 24);
            d[capAt + 1] = cast(ubyte)(capLen >> 16);
            d[capAt + 2] = cast(ubyte)(capLen >> 8);
            d[capAt + 3] = cast(ubyte)(capLen & 0xFF);
            immutable tblLen = o.length - tblAt - 4;
            d[tblAt] = cast(ubyte)(tblLen >> 24);
            d[tblAt + 1] = cast(ubyte)(tblLen >> 16);
            d[tblAt + 2] = cast(ubyte)(tblLen >> 8);
            d[tblAt + 3] = cast(ubyte)(tblLen & 0xFF);
            putLongStr(o, "PLAIN");
            putLongStr(o, "en_US");
        });
        sendTo(c, outb.data);
        outb.clear();
    }

    for (;;)
    {
        bool alive;
        try
            alive = tcp.waitForData();
        catch (Exception)
            alive = false;
        if (!alive)
            return;
        ulong avail;
        try
            avail = tcp.leastSize;
        catch (Exception)
            return;
        if (avail == 0)
            return;
        auto space = inb.freeSpace(cast(size_t) avail);
        try
            tcp.read(space[0 .. cast(size_t) avail]);
        catch (Exception)
            return;
        inb.grow(cast(size_t) avail);

        size_t pos = 0;
        for (;;)
        {
            auto d = inb.data;
            if (d.length - pos < 7)
                break;
            immutable ftype = d[pos];
            immutable chan = cast(ushort)((d[pos + 1] << 8) | d[pos + 2]);
            immutable fsize = (cast(uint) d[pos + 3] << 24) | (cast(uint) d[pos + 4] << 16)
                | (cast(uint) d[pos + 5] << 8) | d[pos + 6];
            if (d.length - pos < 7 + fsize + 1)
                break;
            auto payload = d[pos + 7 .. pos + 7 + fsize];
            if (d[pos + 7 + fsize] != FRAME_END)
                return; // framing error
            if (!handleFrame(c, ftype, chan, payload, outb))
            {
                if (!outb.empty)
                    sendTo(c, outb.data);
                return;
            }
            pos += 7 + fsize + 1;
        }
        if (!outb.empty)
        {
            sendTo(c, outb.data);
            outb.clear();
        }
        inb.consume(pos);
    }
}

private bool handleFrame(AmqpConn c, ubyte ftype, ushort chan,
        scope const(ubyte)[] p, ref ByteBuffer o) nothrow @trusted
{
    if (ftype == FRAME_HEARTBEAT)
        return true;
    if (ftype == FRAME_HEADER)
    {
        auto ch = chan in c.chans;
        if (ch is null || !ch.pub.active)
            return true;
        Rd r = Rd(p);
        cast(void) r.u16(); // class
        cast(void) r.u16(); // weight
        ch.pub.bodySize = r.u64();
        ch.pub.props.clear();
        if (r.i < p.length)
            ch.pub.props.append(p[r.i .. $]); // property flags + list, verbatim
        if (ch.pub.bodySize == 0)
            finishPublish(c, chan, *ch, o);
        return r.ok;
    }
    if (ftype == FRAME_BODY)
    {
        auto ch = chan in c.chans;
        if (ch is null || !ch.pub.active)
            return true;
        ch.pub.payload.append(p);
        if (ch.pub.payload.length >= ch.pub.bodySize)
            finishPublish(c, chan, *ch, o);
        return true;
    }
    if (ftype != FRAME_METHOD)
        return true;

    Rd r = Rd(p);
    immutable cls = r.u16();
    immutable mth = r.u16();
    switch (cls)
    {
    case 10: // connection
        switch (mth)
        {
        case 11: // start-ok
            r.skipTable();
            cast(void) r.shortStr();
            cast(void) r.longStr();
            cast(void) r.shortStr();
            method(o, 0, 10, 30, (ref ByteBuffer b) @nogc nothrow {
                putU16(b, 2047); // channel-max
                putU32(b, 131072); // frame-max
                putU16(b, 30); // heartbeat: we SEND every ~15s (see the sender
            });                //  fiber); client heartbeats are accepted
            startHeartbeat(c);
            return true;
        case 31: // tune-ok
            return true;
        case 40: // open
            method(o, 0, 10, 41, (ref ByteBuffer b) @nogc nothrow {
                putShortStr(b, "");
            });
            return true;
        case 50: // close
            method(o, 0, 10, 51);
            return false;
        case 51: // close-ok
            return false;
        default:
            return true;
        }
    case 20: // channel
        switch (mth)
        {
        case 10: // open
            try
                c.chans[chan] = Channel(true);
            catch (Exception)
            {
            }
            method(o, chan, 20, 11, (ref ByteBuffer b) @nogc nothrow {
                putLongStr(b, "");
            });
            return true;
        case 40: // close
            try
                c.chans.remove(chan);
            catch (Exception)
            {
            }
            method(o, chan, 20, 41);
            return true;
        default:
            return true;
        }
    case 40: // exchange
        if (mth == 10) // declare
        {
            cast(void) r.u16();
            auto ex = r.shortStr();
            auto typ = r.shortStr();
            ctlBroadcast(1, ex, typ, "");
            method(o, chan, 40, 11);
            return true;
        }
        return true;
    case 50: // queue
        switch (mth)
        {
        case 10: // declare
            {
                cast(void) r.u16();
                auto q = r.shortStr();
                cast(void) r.u8(); // passive/durable/exclusive/auto-delete/no-wait bits
                auto argsTbl = r.tableRaw();
                if (argsTbl !is null && argsTbl.length)
                {
                    auto dlx = tableGetStr(argsTbl, "x-dead-letter-exchange");
                    auto dlrk = tableGetStr(argsTbl, "x-dead-letter-routing-key");
                    if (dlx !is null)
                        ctlBroadcast(3, q, dlx, dlrk is null ? "" : dlrk);
                }
                static ByteBuffer kb; // TLS
                queueKey(q, kb);
                immutable cnt = gAmqpLen !is null ? gAmqpLen(kb.data.asChars) : 0;
                // copy q before building the reply (r points into inb)
                static char[256] qcopy = void;
                auto qn = q.length <= qcopy.length ? q.length : qcopy.length;
                qcopy[0 .. qn] = q[0 .. qn];
                auto qq = cast(const(char)[]) qcopy[0 .. qn];
                method(o, chan, 50, 11, (ref ByteBuffer b) @nogc nothrow {
                    putShortStr(b, qq);
                    putU32(b, cast(uint)(cnt < 0 ? 0 : cnt));
                    putU32(b, 0);
                });
                return true;
            }
        case 20: // bind
            {
                cast(void) r.u16();
                auto q = r.shortStr();
                auto ex = r.shortStr();
                auto rk = r.shortStr();
                cast(void) r.u8(); // no-wait
                auto bindArgs = r.tableRaw();
                ctlBroadcast(2, ex, q, rk, bindArgs);
                method(o, chan, 50, 21);
                return true;
            }
        default:
            return true;
        }
    case 60: // basic
        switch (mth)
        {
        case 40: // publish
            {
                auto ch = chan in c.chans;
                if (ch is null)
                    return true;
                cast(void) r.u16();
                auto ex = r.shortStr();
                auto rk = r.shortStr();
                try
                {
                    ch.pub.active = true;
                    ch.pub.exchange = ex.idup;
                    ch.pub.rkey = rk.idup;
                    ch.pub.payload.clear();
                    ch.pub.bodySize = 0;
                }
                catch (Exception)
                {
                }
                return true;
            }
        case 70: // get
            {
                cast(void) r.u16();
                auto q = r.shortStr();
                static ByteBuffer kb2; // TLS
                queueKey(q, kb2);
                static ByteBuffer pay; // TLS
                pay.clear();
                if (gAmqpPop !is null && gAmqpPop(kb2.data.asChars, pay))
                {
                    immutable remaining = gAmqpLen !is null ? gAmqpLen(kb2.data.asChars) : 0;
                    method(o, chan, 60, 71, (ref ByteBuffer b) @nogc nothrow {
                        putU64(b, 1); // delivery-tag (auto-ack semantics)
                        b.appendByte(0); // redelivered
                        putShortStr(b, "");
                        putShortStr(b, "");
                        putU32(b, cast(uint)(remaining < 0 ? 0 : remaining));
                    });
                    emitContent(o, chan, pay.data);
                }
                else
                    method(o, chan, 60, 72, (ref ByteBuffer b) @nogc nothrow {
                        putShortStr(b, "");
                    });
                return true;
            }
        case 20: // consume
            {
                auto ch = chan in c.chans;
                cast(void) r.u16();
                auto q = r.shortStr();
                auto tag = r.shortStr();
                immutable bits = r.u8();
                immutable noAck = (bits & 2) != 0;
                static char[128] tagbuf = void;
                const(char)[] tg;
                if (tag.length == 0)
                    tg = "ctag-1";
                else
                {
                    auto tn = tag.length <= tagbuf.length ? tag.length : tagbuf.length;
                    tagbuf[0 .. tn] = tag[0 .. tn];
                    tg = cast(const(char)[]) tagbuf[0 .. tn];
                }
                method(o, chan, 60, 21, (ref ByteBuffer b) @nogc nothrow {
                    putShortStr(b, tg);
                });
                startConsumer(c, chan, q, tg, noAck);
                return true;
            }
        case 80: // ack: delivery-tag u64, multiple bit
            {
                immutable tag = r.u64();
                immutable multiple = r.ok && r.i < p.length ? (p[r.i] & 1) != 0 : false;
                try
                {
                    if (multiple)
                    {
                        ulong[] drop;
                        foreach (t, ref u; c.unacked)
                            if (t <= tag)
                                drop ~= t;
                        foreach (t; drop)
                            c.unacked.remove(t);
                    }
                    else
                        c.unacked.remove(tag);
                }
                catch (Exception)
                {
                }
                return true;
            }
        case 90: // reject: delivery-tag u64, requeue bit
            {
                immutable tag = r.u64();
                immutable requeue = r.ok && r.i < p.length ? (p[r.i] & 1) != 0 : false;
                settleNegative(c, tag, requeue);
                return true;
            }
        case 120: // nack: delivery-tag u64, multiple, requeue
            {
                immutable tag = r.u64();
                immutable bits2 = r.ok && r.i < p.length ? p[r.i] : 0;
                immutable multiple = (bits2 & 1) != 0;
                immutable requeue = (bits2 & 2) != 0;
                try
                {
                    if (multiple)
                    {
                        ulong[] all;
                        foreach (t, ref u; c.unacked)
                            if (t <= tag)
                                all ~= t;
                        foreach (t; all)
                            settleNegative(c, t, requeue);
                    }
                    else
                        settleNegative(c, tag, requeue);
                }
                catch (Exception)
                {
                }
                return true;
            }
        case 10: // qos — no windowing v1 (consumers poll); acknowledge it
            method(o, chan, 60, 11);
            return true;
        case 30: // cancel — stop the consumer fiber, reply CancelOk
            {
                auto tag = r.shortStr();
                immutable noWait = r.ok && r.i < p.length ? (p[r.i] & 1) != 0 : false;
                try
                    c.cancelledTags[tag.idup] = true;
                catch (Exception)
                {
                }
                if (!noWait)
                {
                    static char[128] tb = void;
                    auto tn = tag.length <= tb.length ? tag.length : tb.length;
                    tb[0 .. tn] = tag[0 .. tn];
                    auto tg2 = cast(const(char)[]) tb[0 .. tn];
                    method(o, chan, 60, 31, (ref ByteBuffer b) @nogc nothrow {
                        putShortStr(b, tg2);
                    });
                }
                return true;
            }
        default:
            return true;
        }
    case 85: // confirm
        if (mth == 10) // select
        {
            auto ch = chan in c.chans;
            if (ch !is null)
            {
                ch.confirmMode = true;
                ch.confirmSeq = 1;
            }
            method(o, chan, 85, 11);
            return true;
        }
        return true;
    default:
        return true;
    }
}

private auto asChars(const(ubyte)[] b) @nogc nothrow
{
    return cast(const(char)[]) b;
}

// Queue record framing: [\x01 'A' 'M' 'Q'][u32 propLen][props][body].
// A record WITHOUT the magic (e.g. LPUSHed from the RESP side — cross-protocol
// ingest is a feature) is treated as a bare body with empty properties.
private void buildRecord(ref ByteBuffer o, scope const(ubyte)[] props,
        scope const(ubyte)[] body_) @nogc nothrow
{
    o.append("\x01AMQ");
    putU32(o, cast(uint) props.length);
    o.append(props);
    o.append(body_);
}

package void splitRecord(scope const(ubyte)[] blob, out const(ubyte)[] props,
        out const(ubyte)[] body_) @nogc nothrow
{
    if (blob.length >= 8 && blob[0] == 0x01 && blob[1] == 'A' && blob[2] == 'M'
            && blob[3] == 'Q')
    {
        immutable pl = (cast(size_t) blob[4] << 24) | (cast(size_t) blob[5] << 16)
            | (cast(size_t) blob[6] << 8) | blob[7];
        if (8 + pl <= blob.length)
        {
            props = blob[8 .. 8 + pl];
            body_ = blob[8 + pl .. $];
            return;
        }
    }
    props = null;
    body_ = blob;
}

private void finishPublish(AmqpConn c, ushort chan, ref Channel ch, ref ByteBuffer o) nothrow @trusted
{
    ch.pub.active = false;
    // route to queues and RPUSH the framed record through the data plane
    static ByteBuffer rec; // TLS
    rec.clear();
    buildRecord(rec, ch.pub.props.data, ch.pub.payload.data);
    auto payload = rec.data.asChars;
    auto hdrs = propsHeaders(ch.pub.props.data);
    routeTo(ch.pub.exchange, ch.pub.rkey, hdrs, (string q) nothrow {
        static ByteBuffer kb3; // TLS
        queueKey(q, kb3);
        if (gAmqpPush !is null)
            gAmqpPush(kb3.data.asChars, payload);
    });
    if (ch.confirmMode)
    {
        immutable tag = ch.confirmSeq++;
        method(o, chan, 60, 80, (ref ByteBuffer b) @nogc nothrow {
            putU64(b, tag);
            b.appendByte(0); // multiple=false
        });
    }
}

private void emitContent(ref ByteBuffer o, ushort chan, scope const(ubyte)[] blob) nothrow
{
    const(ubyte)[] props, body_;
    splitRecord(blob, props, body_);
    // content HEADER frame — the publisher's property block replays VERBATIM
    // (content-type, headers, correlation-id ... survive the queue)
    size_t at;
    frameStart(o, FRAME_HEADER, chan, at);
    putU16(o, 60); // class basic
    putU16(o, 0); // weight
    putU64(o, body_.length);
    if (props.length >= 2)
        o.append(props);
    else
        putU16(o, 0); // no properties
    frameFinish(o, at);
    // BODY frame (single — frame-max is honored implicitly for bench payloads)
    frameStart(o, FRAME_BODY, chan, at);
    o.append(body_);
    frameFinish(o, at);
}

/// A dying connection returns everything unacked to the FRONT of its queue —
/// the at-least-once contract for no_ack=false consumers.
private void requeueAllUnacked(AmqpConn c) nothrow @trusted
{
    try
    {
        foreach (t, ref u; c.unacked)
        {
            static ByteBuffer kb6; // TLS
            queueKey(u.queue, kb6);
            if (gAmqpPushFront !is null)
                gAmqpPushFront(kb6.data.asChars, u.blob.asChars);
        }
        c.unacked.clear();
    }
    catch (Exception)
    {
    }
}

/// Negative settle: requeue=true puts the record back at the queue FRONT;
/// requeue=false dead-letters via the queue's x-dead-letter-exchange (declared
/// metadata) or drops when none is configured.
private void settleNegative(AmqpConn c, ulong tag, bool requeue) nothrow @trusted
{
    Unacked u;
    bool found = false;
    try
    {
        if (auto p = tag in c.unacked)
        {
            u = *p;
            found = true;
            c.unacked.remove(tag);
        }
    }
    catch (Exception)
    {
    }
    if (!found)
        return;
    static ByteBuffer kb4; // TLS
    if (requeue)
    {
        queueKey(u.queue, kb4);
        if (gAmqpPushFront !is null)
            gAmqpPushFront(kb4.data.asChars, u.blob.asChars);
        return;
    }
    QueueMeta meta;
    try
        if (auto m = u.queue in gQueueMeta)
            meta = *m;
    catch (Exception)
    {
    }
    if (meta.dlx.length == 0)
        return; // no dead-letter exchange: drop
    const(ubyte)[] props, body_;
    splitRecord(u.blob, props, body_);
    auto rk = meta.dlrk.length ? meta.dlrk : u.queue;
    auto blob = u.blob.asChars;
    routeTo(meta.dlx, rk, propsHeaders(props), (string q) nothrow {
        static ByteBuffer kb5; // TLS
        queueKey(q, kb5);
        if (gAmqpPush !is null)
            gAmqpPush(kb5.data.asChars, blob);
    });
}

// Consumer: a fiber that drains the queue to this connection. v1 POLLS the
// data plane (1ms backoff when empty) — under load it never sleeps; a parked
// wake integration (the BLPOP machinery) is the v2 upgrade.
private void startConsumer(AmqpConn c, ushort chan, scope const(char)[] q,
        scope const(char)[] tag, bool noAck) nothrow
{
    string qs, ts;
    try
    {
        qs = q.idup;
        ts = tag.idup;
    }
    catch (Exception)
        return;
    atomicOp!"+="(gAmqpConsumers, 1);
    try
        cast(void) runTask((AmqpConn cc, ushort chn, string qq, string tt, bool na) nothrow {
            scope (exit)
                atomicOp!"-="(gAmqpConsumers, 1);
            ByteBuffer kb;
            kb.append("amq.q.");
            kb.append(qq);
            ByteBuffer pay;
            ByteBuffer ob;
            while (!cc.closing)
            {
                try
                {
                    if (tt in cc.cancelledTags)
                        return;
                }
                catch (Exception)
                {
                }
                // BURST drain: up to 64 messages per socket write — a delivery
                // per write capped the consumer at ~137k msg/s (measured)
                ob.clear();
                int burst = 0;
                while (burst < 64)
                {
                    pay.clear();
                    if (!(gAmqpPop !is null && gAmqpPop(kb.data.asChars, pay)))
                        break;
                    immutable tg = cc.nextTag++;
                    if (!na)
                        try
                            cc.unacked[tg] = Unacked(qq, pay.data.idup);
                        catch (Exception)
                        {
                        }
                    method(ob, chn, 60, 60, (ref ByteBuffer b) @nogc nothrow {
                        putShortStr(b, tt);
                        putU64(b, tg);
                        b.appendByte(0);
                        putShortStr(b, "");
                        putShortStr(b, qq);
                    });
                    emitContent(ob, chn, pay.data);
                    burst++;
                }
                if (burst == 0)
                {
                    try
                        sleep(1.msecs);
                    catch (Exception)
                        return;
                    continue;
                }
                sendTo(cc, ob.data);
            }
        }, c, chan, qs, ts, noAck);
    catch (Exception)
        atomicOp!"-="(gAmqpConsumers, 1);
}

// Heartbeat sender: a fiber emitting a heartbeat frame every 15s while the
// connection lives (half the negotiated 30s interval). Read-side liveness
// stays TCP-level in v1 (the serve loop notices the close).
private void startHeartbeat(AmqpConn c) nothrow
{
    try
        cast(void) runTask((AmqpConn cc) nothrow {
            static immutable ubyte[8] hb = [8, 0, 0, 0, 0, 0, 0, 0xCE];
            while (!cc.closing)
            {
                try
                    sleep(15_000.msecs);
                catch (Exception)
                    return;
                if (cc.closing)
                    return;
                sendTo(cc, hb[]);
            }
        }, c);
    catch (Exception)
    {
    }
}

// ---------------------------------------------------------------------------
// Tests

unittest // topic matching
{
    assert(amqpTopicMatches("a.b.c", "a.b.c"));
    assert(!amqpTopicMatches("a.b.c", "a.b"));
    assert(amqpTopicMatches("a.*.c", "a.b.c"));
    assert(!amqpTopicMatches("a.*", "a.b.c"));
    assert(amqpTopicMatches("a.#", "a.b.c"));
    assert(amqpTopicMatches("a.#", "a"));
    assert(amqpTopicMatches("#", "x.y"));
    assert(amqpTopicMatches("#.c", "a.b.c"));
    assert(amqpTopicMatches("a.#.c", "a.c"));
    assert(amqpTopicMatches("a.#.c", "a.x.y.c"));
    assert(!amqpTopicMatches("a.#.c", "a.x.y"));
}
