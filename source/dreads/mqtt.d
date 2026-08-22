module dreads.mqtt;

// MQTT 3.1.1 frontend — the FIRST non-RESP skin over the sharded core (the
// "one ring" thesis made concrete: same process, same threads, same
// share-nothing fabric; RESP is just one protocol face among several).
//
// Architecture mirrors the RESP side exactly:
//   - every shard thread opens an SO_REUSEPORT listener on the MQTT port; a
//     connection is served by a fiber on the thread that accepted it;
//   - subscriptions live in a THREAD-LOCAL topic trie (share-nothing — the
//     same rule as gPubSub);
//   - a PUBLISH delivers to the local trie and fans out to the other shards
//     over the SPSC fabric (ShardMsg.mqttPub), gated by a global atomic
//     subscriber count so an idle skin costs nothing;
//   - retained messages are broadcast-replicated to every thread's local map
//     (rare writes, local reads at SUBSCRIBE time).
//
// v1 scope (documented): QoS 0 delivery, QoS 1 publishes (PUBACK on receipt),
// clean sessions only (CONNACK session-present=0), retained messages, no
// keepalive enforcement (TCP death is detected by the read loop), overlapping
// subscriptions may deliver duplicates (the 3.1.1 spec permits this).

import core.atomic : atomicLoad, atomicOp, MemoryOrder;

import vibe.core.net : TCPConnection;
import vibe.core.sync : TaskMutex;

import dreads.mem : ByteBuffer;

// ---------------------------------------------------------------------------
// Global gate: total MQTT subscriptions across every thread's trie. A publish
// with zero subscribers anywhere skips both matching and the cross-shard
// fan-out (same trick as pubsub.gSubTotal).
public shared long gMqttSubTotal;

/// One MQTT connection (fiber-owned). The write mutex serializes deliveries
/// from publisher fibers on the same thread with the conn's own replies.
public final class MqttConn
{
    TCPConnection tcp;
    TaskMutex wlock;
    bool connected; // CONNECT seen and CONNACKed
    uint gen; // bumped on disconnect: stale trie entries self-invalidate
    // Delivery outbox: publishes matched to this conn accumulate here and are
    // flushed ONCE per publish batch (mqttFlushDirty) — a write syscall per
    // DELIVERY throttled the E2E rate to ~135k msg/s. Same-thread only (every
    // deliverer — local publisher fibers and the drain's fan-in — runs on this
    // conn's own shard thread), so no lock guards the buffer; only the socket
    // write takes wlock.
    ByteBuffer obox;
    bool dirty;

    this(TCPConnection c) nothrow
    {
        tcp = c;
        try
            wlock = new TaskMutex;
        catch (Exception)
            assert(false, "mqtt: mutex alloc failed");
    }
}

// ---------------------------------------------------------------------------
// Topic trie (THREAD-LOCAL). Segment-split on '/'; `+` matches exactly one
// segment, `#` matches the rest (including zero segments). Standard MQTT
// matching semantics, one trie per shard thread.

private struct SubEntry
{
    MqttConn c;
    uint gen; // conn's gen at subscribe time; != c.gen ⇒ stale (lazily skipped)
    ubyte qos; // granted qos (v1: always 0 on delivery)
}

private final class TrieNode
{
    TrieNode[string] kids; // segment -> child (GC AA: control plane, not hot path)
    TrieNode plus; // the `+` child
    SubEntry[] subs; // subscribers terminating at THIS node
    SubEntry[] hash; // `#` subscribers rooted at this node
}

private TrieNode gTrieRoot; // TLS: this thread's subscription trie
private const(char)[][string] gRetained; // TLS: topic -> retained payload (replicated)

private TrieNode trieRoot() nothrow @trusted
{
    if (gTrieRoot is null)
    {
        try
            gTrieRoot = new TrieNode;
        catch (Exception)
            assert(false);
    }
    return gTrieRoot;
}

// Split-walk `filter` creating nodes, then append the entry at the terminal.
private void trieSubscribe(scope const(char)[] filter, MqttConn c, ubyte qos) @trusted nothrow
{
    try
    {
        auto n = trieRoot();
        size_t i = 0;
        while (i <= filter.length)
        {
            // next segment [i .. e)
            size_t e = i;
            while (e < filter.length && filter[e] != '/')
                e++;
            auto seg = filter[i .. e];
            if (seg == "#")
            {
                n.hash ~= SubEntry(c, c.gen, qos);
                atomicOp!"+="(gMqttSubTotal, 1);
                return;
            }
            TrieNode next;
            if (seg == "+")
            {
                if (n.plus is null)
                    n.plus = new TrieNode;
                next = n.plus;
            }
            else
            {
                auto k = cast(string) seg.idup;
                if (auto p = k in n.kids)
                    next = *p;
                else
                {
                    next = new TrieNode;
                    n.kids[k] = next;
                }
            }
            if (e >= filter.length)
            {
                next.subs ~= SubEntry(c, c.gen, qos);
                atomicOp!"+="(gMqttSubTotal, 1);
                return;
            }
            n = next;
            i = e + 1;
        }
    }
    catch (Exception)
    {
    }
}

// Remove every entry of `c` under `filter` (exact filter match).
private void trieUnsubscribe(scope const(char)[] filter, MqttConn c) @trusted nothrow
{
    try
    {
        auto n = trieRoot();
        size_t i = 0;
        while (i <= filter.length)
        {
            size_t e = i;
            while (e < filter.length && filter[e] != '/')
                e++;
            auto seg = filter[i .. e];
            if (seg == "#")
            {
                dropConn(n.hash, c);
                return;
            }
            TrieNode next;
            if (seg == "+")
                next = n.plus;
            else if (auto p = (cast(string) seg) in n.kids)
                next = *p;
            if (next is null)
                return;
            if (e >= filter.length)
            {
                dropConn(next.subs, c);
                return;
            }
            n = next;
            i = e + 1;
        }
    }
    catch (Exception)
    {
    }
}

private void dropConn(ref SubEntry[] a, MqttConn c) @trusted nothrow
{
    size_t w = 0;
    foreach (ref e; a)
        if (e.c !is c)
            a[w++] = e;
    if (w != a.length)
    {
        atomicOp!"-="(gMqttSubTotal, cast(long)(a.length - w));
        a.length = w;
    }
}

/// Collect the live subscribers matching `topic` into `outSubs`. The walk
/// branches on the exact segment AND the `+` child at every level; `#` piles
/// entries from any level. Stale entries (dead conns) are skipped lazily.
private void trieMatch(TrieNode n, scope const(char)[] topic, size_t i,
        ref MqttConn[64] outSubs, ref size_t outN) @trusted nothrow
{
    if (n is null || outN >= outSubs.length)
        return;
    foreach (ref e; n.hash)
        addLive(e, outSubs, outN);
    if (i > topic.length)
    {
        foreach (ref e; n.subs)
            addLive(e, outSubs, outN);
        return;
    }
    size_t e2 = i;
    while (e2 < topic.length && topic[e2] != '/')
        e2++;
    auto seg = topic[i .. e2];
    immutable size_t next = e2 >= topic.length ? topic.length + 1 : e2 + 1;
    try
        if (auto p = (cast(string) seg) in n.kids)
            trieMatch(*p, topic, next, outSubs, outN);
    catch (Exception)
    {
    }
    trieMatch(n.plus, topic, next, outSubs, outN);
}

private void addLive(ref SubEntry e, ref MqttConn[64] outSubs, ref size_t outN) @trusted nothrow
{
    if (outN >= outSubs.length)
        return;
    if (e.c is null || e.gen != e.c.gen)
        return; // stale (unsubscribed/disconnected)
    try
        if (!e.c.tcp.connected)
            return;
    catch (Exception)
        return;
    outSubs[outN++] = e.c;
}

/// Does `filter` match `topic`? (retained-message delivery at SUBSCRIBE time.)
package bool mqttFilterMatches(scope const(char)[] filter, scope const(char)[] topic) @nogc nothrow
{
    size_t fi = 0, ti = 0;
    for (;;)
    {
        size_t fe = fi;
        while (fe < filter.length && filter[fe] != '/')
            fe++;
        auto fseg = filter[fi .. fe];
        if (fseg == "#")
            return true;
        size_t te = ti;
        while (te < topic.length && topic[te] != '/')
            te++;
        auto tseg = topic[ti .. te];
        if (fseg != "+" && fseg != tseg)
            return false;
        immutable fDone = fe >= filter.length;
        immutable tDone = te >= topic.length;
        if (fDone || tDone)
        {
            // "a/#" matches "a": a trailing "/#" covers ZERO remaining levels
            if (tDone && !fDone && filter[fe + 1 .. $] == "#")
                return true;
            return fDone && tDone;
        }
        fi = fe + 1;
        ti = te + 1;
    }
}

// ---------------------------------------------------------------------------
// Wire codec (3.1.1)

private enum ubyte PT_CONNECT = 1, PT_CONNACK = 2, PT_PUBLISH = 3, PT_PUBACK = 4,
        PT_SUBSCRIBE = 8, PT_SUBACK = 9, PT_UNSUBSCRIBE = 10, PT_UNSUBACK = 11,
        PT_PINGREQ = 12, PT_PINGRESP = 13, PT_DISCONNECT = 14;

/// Decode the remaining-length varint at buf[pos..]; false when incomplete.
package bool decodeVarint(scope const(ubyte)[] buf, ref size_t pos, out uint val) @nogc nothrow
{
    uint mul = 1;
    val = 0;
    foreach (k; 0 .. 4)
    {
        if (pos >= buf.length)
            return false;
        immutable b = buf[pos++];
        val += (b & 0x7F) * mul;
        if ((b & 0x80) == 0)
            return true;
        mul *= 128;
    }
    return false; // malformed (>4 bytes)
}

package void encodeVarint(ref ByteBuffer o, uint v) @nogc nothrow
{
    do
    {
        ubyte b = v & 0x7F;
        v >>= 7;
        if (v)
            b |= 0x80;
        o.appendByte(cast(char) b);
    }
    while (v);
}

private bool rdStr(scope const(ubyte)[] p, ref size_t i, out const(char)[] s) @nogc nothrow
{
    if (i + 2 > p.length)
        return false;
    immutable n = (cast(size_t) p[i] << 8) | p[i + 1];
    i += 2;
    if (i + n > p.length)
        return false;
    s = cast(const(char)[]) p[i .. i + n];
    i += n;
    return true;
}

// ---------------------------------------------------------------------------
// Delivery + cross-shard fan-out

// Hooks installed by server.d (avoids a server import cycle): fan a publish
// out to the other shards / count stats.
public __gshared void delegate(scope const(char)[] topic,
        scope const(char)[] payload, bool retain) nothrow gMqttFanout;
public __gshared ulong gMqttMessages; // total publishes routed (INFO/debug)

/// Deliver `topic`/`payload` to THIS thread's matching subscribers, and update
/// this thread's retained map when asked. Called for local publishes AND for
/// fan-in from other shards (the drain's mqttPub case).
public void mqttDeliverLocal(scope const(char)[] topic, scope const(char)[] payload,
        bool retain) nothrow @trusted
{
    if (retain)
    {
        try
        {
            if (payload.length == 0)
                gRetained.remove(cast(string) topic);
            else
                gRetained[topic.idup] = payload.idup;
        }
        catch (Exception)
        {
        }
    }
    if (atomicLoad!(MemoryOrder.raw)(gMqttSubTotal) == 0)
        return;
    MqttConn[64] subs = void;
    size_t n = 0;
    trieMatch(gTrieRoot, topic, 0, subs, n);
    if (n == 0)
        return;
    static ByteBuffer pkt; // TLS: built once per publish, appended to each sub
    pkt.clear();
    buildPublish(pkt, topic, payload, false);
    foreach (s; subs[0 .. n])
    {
        s.obox.append(pkt.data);
        if (!s.dirty)
        {
            s.dirty = true;
            try
                tDirty ~= s;
            catch (Exception)
            {
            }
        }
    }
}

private MqttConn[] tDirty; // TLS: conns with pending deliveries this batch

/// Flush every accumulated delivery outbox — called once per publish batch
/// (the publisher's serve loop after its parse pass, and the drain after a
/// fan-in pass). One write per touched subscriber per batch.
public void mqttFlushDirty() nothrow @trusted
{
    if (tDirty.length == 0)
        return;
    foreach (c; tDirty)
    {
        if (!c.obox.empty)
        {
            sendTo(c, c.obox.data);
            c.obox.clear();
        }
        c.dirty = false;
    }
    tDirty.length = 0;
    try
        (cast(MqttConn[]) tDirty).assumeSafeAppend;
    catch (Exception)
    {
    }
}

private void buildPublish(ref ByteBuffer o, scope const(char)[] topic,
        scope const(char)[] payload, bool retain) @nogc nothrow
{
    o.appendByte(cast(char)((PT_PUBLISH << 4) | (retain ? 1 : 0))); // QoS 0 out
    encodeVarint(o, cast(uint)(2 + topic.length + payload.length));
    o.appendByte(cast(char)(topic.length >> 8));
    o.appendByte(cast(char)(topic.length & 0xFF));
    o.append(topic);
    o.append(payload);
}

private void sendTo(MqttConn c, scope const(ubyte)[] bytes) nothrow
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

// ---------------------------------------------------------------------------
// The per-connection serve loop (a fiber per accepted MQTT connection).

public void serveMqttClient(TCPConnection tcp) nothrow
{
    try
        tcp.tcpNoDelay = true; // PUBACK/deliveries are tiny — Nagle throttles
    catch (Exception)          // the QoS1 window to ~6k msg/s (measured)
    {
    }
    auto c = new MqttConn(tcp);
    static void closeQuiet(MqttConn cc) nothrow
    {
        try
            cc.tcp.close();
        catch (Exception)
        {
        }
    }

    scope (exit)
    {
        c.gen++; // invalidate every trie entry of this conn (lazily skipped)
        closeQuiet(c);
    }
    ByteBuffer inb;
    ByteBuffer outb;
    for (;;)
    {
        // read at least one byte (blocks in the fiber)
        bool alive;
        try
            alive = tcp.waitForData();
        catch (Exception)
            alive = false;
        if (!alive)
            return;
        auto avail = () @trusted {
            try
                return tcp.leastSize;
            catch (Exception)
                return cast(ulong) 0;
        }();
        if (avail == 0)
            return;
        auto space = inb.freeSpace(cast(size_t) avail);
        try
            tcp.read(space[0 .. cast(size_t) avail]);
        catch (Exception)
            return;
        inb.grow(cast(size_t) avail);

        // parse complete packets off the front
        size_t pos = 0;
        for (;;)
        {
            auto d = inb.data;
            if (pos >= d.length)
                break;
            size_t hp = pos + 1;
            uint rem;
            if (!decodeVarint(d, hp, rem))
                break; // incomplete header
            if (hp + rem > d.length)
                break; // incomplete body
            immutable ubyte h = d[pos];
            auto body_ = d[hp .. hp + rem];
            if (!handlePacket(c, h, body_, outb))
            {
                if (!outb.empty) // flush any acks built before the close
                    sendTo(c, outb.data);
                return; // protocol error or DISCONNECT
            }
            pos = hp + rem;
        }
        // ONE write per read batch (RESP-style pipelining): a windowed QoS1
        // publisher's acks coalesce instead of paying a syscall per packet —
        // and this batch's DELIVERIES flush once per subscriber, not per msg
        mqttFlushDirty();
        if (!outb.empty)
        {
            sendTo(c, outb.data);
            outb.clear();
        }
        inb.consume(pos);
    }
}

// Returns false to close the connection.
private bool handlePacket(MqttConn c, ubyte h, scope const(ubyte)[] p,
        ref ByteBuffer o) nothrow @trusted
{
    immutable type = h >> 4;
    final switch (type)
    {
    case PT_CONNECT:
        {
            size_t i = 0;
            const(char)[] proto;
            if (!rdStr(p, i, proto) || i >= p.length)
                return false;
            immutable level = p[i++];
            if (i >= p.length)
                return false;
            immutable flags = p[i++];
            i += 2; // keepalive (unenforced v1)
            const(char)[] clientId;
            if (!rdStr(p, i, clientId))
                return false;
            // skip will topic/message, username, password per flags
            if (flags & 0x04)
            {
                const(char)[] wt, wm;
                if (!rdStr(p, i, wt) || !rdStr(p, i, wm))
                    return false;
            }
            if (flags & 0x80)
            {
                const(char)[] u;
                if (!rdStr(p, i, u))
                    return false;
            }
            if (flags & 0x40)
            {
                const(char)[] pw;
                if (!rdStr(p, i, pw))
                    return false;
            }
            c.connected = true;
            // CONNACK: session-present=0, rc=0 (3.1 level 3 and 3.1.1 level 4 both accepted)
            o.appendByte(cast(char)(PT_CONNACK << 4));
            o.appendByte(cast(char) 2);
            o.appendByte(cast(char) 0);
            o.appendByte(cast(char)(level == 3 || level == 4 ? 0 : 1));
            return level == 3 || level == 4;
        }
    case PT_PUBLISH:
        {
            if (!c.connected)
                return false;
            immutable qos = (h >> 1) & 0x3;
            immutable retain = (h & 1) != 0;
            size_t i = 0;
            const(char)[] topic;
            if (!rdStr(p, i, topic))
                return false;
            ushort pid = 0;
            if (qos > 0)
            {
                if (i + 2 > p.length)
                    return false;
                pid = cast(ushort)((p[i] << 8) | p[i + 1]);
                i += 2;
            }
            auto payload = cast(const(char)[]) p[i .. $];
            gMqttMessages++;
            mqttDeliverLocal(topic, payload, retain);
            if (gMqttFanout !is null)
                gMqttFanout(topic, payload, retain);
            if (qos >= 1) // QoS1: ack on receipt; QoS2 (unsupported) acked as 1
            {
                o.appendByte(cast(char)(PT_PUBACK << 4));
                o.appendByte(cast(char) 2);
                o.appendByte(cast(char)(pid >> 8));
                o.appendByte(cast(char)(pid & 0xFF));
            }
            return true;
        }
    case PT_SUBSCRIBE:
        {
            if (!c.connected || p.length < 2)
                return false;
            immutable pid = cast(ushort)((p[0] << 8) | p[1]);
            size_t i = 2;
            static ubyte[64] granted = void;
            size_t ng = 0;
            static const(char)[][64] filters;
            while (i < p.length && ng < granted.length)
            {
                const(char)[] filter;
                if (!rdStr(p, i, filter) || i >= p.length)
                    return false;
                immutable reqQos = p[i++];
                cast(void) reqQos;
                trieSubscribe(filter, c, 0);
                filters[ng] = filter;
                granted[ng++] = 0; // v1: everything granted at QoS 0
            }
            o.appendByte(cast(char)(PT_SUBACK << 4));
            encodeVarint(o, cast(uint)(2 + ng));
            o.appendByte(cast(char)(pid >> 8));
            o.appendByte(cast(char)(pid & 0xFF));
            foreach (g; granted[0 .. ng])
                o.appendByte(cast(char) g);
            // retained messages matching the new filters, delivered after SUBACK
            try
                foreach (topic, payload; gRetained)
                    foreach (f; filters[0 .. ng])
                        if (mqttFilterMatches(f, topic))
                        {
                            buildPublish(o, topic, payload, true);
                            break;
                        }
            catch (Exception)
            {
            }
            return true;
        }
    case PT_UNSUBSCRIBE:
        {
            if (!c.connected || p.length < 2)
                return false;
            immutable pid = cast(ushort)((p[0] << 8) | p[1]);
            size_t i = 2;
            while (i < p.length)
            {
                const(char)[] filter;
                if (!rdStr(p, i, filter))
                    return false;
                trieUnsubscribe(filter, c);
            }
            o.appendByte(cast(char)(PT_UNSUBACK << 4));
            o.appendByte(cast(char) 2);
            o.appendByte(cast(char)(pid >> 8));
            o.appendByte(cast(char)(pid & 0xFF));
            return true;
        }
    case PT_PINGREQ:
        o.appendByte(cast(char)(PT_PINGRESP << 4));
        o.appendByte(cast(char) 0);
        return true;
    case PT_DISCONNECT:
        return false;
    case PT_CONNACK:
    case PT_PUBACK:
    case PT_SUBACK:
    case PT_UNSUBACK:
    case PT_PINGRESP:
        return true; // client->server echoes of server packets: tolerate
    }
}

// ---------------------------------------------------------------------------
// Tests

unittest // varint round-trip
{
    ByteBuffer b;
    foreach (v; [0u, 1, 127, 128, 16383, 16384, 268435455])
    {
        b.clear();
        encodeVarint(b, v);
        size_t pos = 0;
        uint back;
        assert(decodeVarint(b.data, pos, back));
        assert(back == v);
    }
}

unittest // filter matching semantics
{
    assert(mqttFilterMatches("a/b/c", "a/b/c"));
    assert(!mqttFilterMatches("a/b/c", "a/b"));
    assert(mqttFilterMatches("a/+/c", "a/b/c"));
    assert(!mqttFilterMatches("a/+/c", "a/b/d"));
    assert(!mqttFilterMatches("a/+", "a/b/c"));
    assert(mqttFilterMatches("a/#", "a/b/c"));
    assert(mqttFilterMatches("a/#", "a"));
    assert(mqttFilterMatches("#", "anything/at/all"));
    assert(mqttFilterMatches("+/b", "a/b"));
    assert(!mqttFilterMatches("+/b", "a/c"));
    assert(!mqttFilterMatches("b/+", "a/b"));
}
