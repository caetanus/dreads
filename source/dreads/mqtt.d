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
// Scope: QoS 0 delivery, QoS 1 publishes (PUBACK on receipt), QoS 2 publishes
// (full PUBREC/PUBREL/PUBCOMP receive handshake with packet-id dedup;
// deliveries themselves go out at QoS 0), clean sessions only (CONNACK
// session-present=0), retained messages, no keepalive enforcement (TCP death
// is detected by the read loop), overlapping subscriptions may deliver
// duplicates (the 3.1.1 spec permits this).
//
// Delivery writes: each connection owns a WRITER FIBER draining a
// double-buffered outbox. Deliverers (local publisher fibers and the shard
// drain's fan-in) only append + signal — they NEVER touch the socket, so a
// slow subscriber blocks nothing but itself, and no fiber yields while
// holding a slice of a buffer another fiber can grow (the classic
// append-during-write use-after-free).

import core.atomic : atomicLoad, atomicOp, MemoryOrder;

import core.time : Duration, msecs, seconds;

import vibe.core.core : runTask, Task;
import vibe.core.net : TCPConnection;
import vibe.core.sync : LocalManualEvent, TaskMutex, createManualEvent;

import dreads.mem : ByteBuffer;

// ---------------------------------------------------------------------------
// Global gate: total MQTT subscriptions across every thread's trie. A publish
// with zero subscribers anywhere skips both matching and the cross-shard
// fan-out (same trick as pubsub.gSubTotal).
public shared long gMqttSubTotal;

/// Deadline for a freshly-accepted socket to complete CONNECT. Without it a
/// client that opens TCP and never speaks pins a serve fiber + writer fiber +
/// MqttConn forever (unauthenticated pre-handshake slowloris).
private enum Duration MQTT_CONNECT_TIMEOUT = 30.seconds;

/// One MQTT connection (fiber-owned). The write mutex serializes deliveries
/// from publisher fibers on the same thread with the conn's own replies.
public final class MqttConn
{
    TCPConnection tcp;
    TaskMutex wlock;
    bool connected; // CONNECT seen and CONNACKed
    uint gen; // bumped on disconnect: stale trie entries self-invalidate
    // Delivery outbox pair: deliverers append to `obox` and signal `flushEvt`;
    // the conn's writer fiber swaps obox<->wbox (no yield in between) and
    // writes wbox. All deliverers run on this conn's own shard thread, so the
    // buffers need no lock — and because the writer only ever writes from
    // wbox, an append during a blocked write grows a DIFFERENT buffer.
    // A write syscall per DELIVERY throttled E2E to ~135k msg/s; batching
    // happens naturally here (everything accumulated during one write goes
    // out in the next).
    ByteBuffer obox;
    ByteBuffer wbox;
    LocalManualEvent flushEvt;
    bool dirty;
    bool closed; // serve fiber sets on exit; writer fiber then drains + stops
    // Read deadline: a BOUNDED default until CONNECT arrives (a client that
    // opens TCP and never sends CONNECT must be reaped — pre-handshake
    // slowloris), then re-armed to 1.5x the CONNECT keepalive (0 kA = max).
    Duration readDeadline = MQTT_CONNECT_TIMEOUT;
    // QoS2 receive state: packet ids between PUBLISH(qos2) and PUBREL. Dedup:
    // a retransmitted qos2 PUBLISH with an id already here is PUBRECed but
    // NOT redelivered (exactly-once toward the subscriber side).
    ushort[] q2pids;
    // Filters this conn subscribed (idup'd): torn down on disconnect so trie
    // entries and gMqttSubTotal don't leak under connect/subscribe churn.
    const(char)[][] filters;

    this(TCPConnection c) nothrow
    {
        tcp = c;
        try
        {
            wlock = new TaskMutex;
            flushEvt = createManualEvent();
        }
        catch (Exception)
            assert(false, "mqtt: conn alloc failed");
    }
}

/// Per-subscriber outbox cap: past this, further QoS-0 deliveries to the slow
/// subscriber are DROPPED (spec-legal for QoS 0) instead of growing memory
/// without bound. Counted in gMqttDropped. Sized to absorb a real burst
/// (mosquitto's answer to the same burst is dropping the whole subscriber);
/// a persistently slow reader still converges to bounded memory.
private enum size_t MQTT_OBOX_CAP = 64 << 20;
/// Largest accepted remaining-length. The varint allows 256MB; accepting that
/// from an unauthenticated socket is an invitation. Oversized frame = close.
private enum uint MQTT_MAX_PACKET = 16 << 20;
public shared ulong gMqttDropped; // deliveries dropped at full outboxes
public shared ulong gMqttRetainedDropped; // retained stores refused at the caps
/// Retained-store caps, PER THREAD (the store is broadcast-replicated): an
/// unauthenticated publisher must not be able to grow every shard's heap
/// without bound. Refusals count in gMqttRetainedDropped.
private enum size_t MQTT_MAX_RETAINED_TOPICS = 65536;
private enum size_t MQTT_MAX_RETAINED_BYTES = 256 << 20;
private size_t tRetainedBytes; // TLS: payload bytes currently in gRetained
/// Per-connection subscription cap: past this SUBSCRIBE gets SUBACK 0x80
/// (unlimited re-subscribe was a memory + delivery-amplification DoS).
private enum size_t MQTT_MAX_SUBS = 4096;

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

// [MQTT-3.8.4-3] an identical re-subscribe REPLACES the existing entry (same
// conn, same terminal): update qos in place, no second entry, no recount —
// appending instead was both a spec violation (duplicate deliveries) and an
// amplification DoS. Returns true when a NEW entry was created.
private bool upsertEntry(ref SubEntry[] a, MqttConn c, ubyte qos) @trusted nothrow
{
    foreach (ref e; a)
        if (e.c is c && e.gen == c.gen)
        {
            e.qos = qos;
            return false;
        }
    try
        a ~= SubEntry(c, c.gen, qos);
    catch (Exception)
        return false;
    atomicOp!"+="(gMqttSubTotal, 1);
    return true;
}

// Split-walk `filter` creating nodes, then upsert the entry at the terminal.
// Returns true when a NEW subscription was created (false = replaced/failed).
private bool trieSubscribe(scope const(char)[] filter, MqttConn c, ubyte qos) @trusted nothrow
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
                return upsertEntry(n.hash, c, qos);
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
                return upsertEntry(next.subs, c, qos);
            n = next;
            i = e + 1;
        }
    }
    catch (Exception)
    {
    }
    return false;
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

// TLS match scratch: grows to the largest fan-out seen on this thread. A
// fixed [64] here silently starved every subscriber past the 64th — forever,
// per publish, with no counter.
private MqttConn[] tMatchBuf;
private size_t tMatchLen;

/// Collect the live subscribers matching `topic` into tMatchBuf[0..tMatchLen].
/// The walk branches on the exact segment AND the `+` child at every level;
/// `#` piles entries from any level. Stale entries are skipped lazily.
private void trieMatch(TrieNode n, scope const(char)[] topic, size_t i) @trusted nothrow
{
    if (n is null)
        return;
    // [MQTT-4.7.2-1] a topic starting with '$' must not match a wildcard at
    // the FIRST level: at the root, skip both the '#' pile and the '+' child.
    immutable dollarRoot = i == 0 && topic.length != 0 && topic[0] == '$';
    if (!dollarRoot)
        foreach (ref e; n.hash)
            addLive(e);
    if (i > topic.length)
    {
        foreach (ref e; n.subs)
            addLive(e);
        return;
    }
    size_t e2 = i;
    while (e2 < topic.length && topic[e2] != '/')
        e2++;
    auto seg = topic[i .. e2];
    immutable size_t next = e2 >= topic.length ? topic.length + 1 : e2 + 1;
    try
        if (auto p = (cast(string) seg) in n.kids)
            trieMatch(*p, topic, next);
    catch (Exception)
    {
    }
    if (!dollarRoot)
        trieMatch(n.plus, topic, next);
}

private void addLive(ref SubEntry e) @trusted nothrow
{
    if (e.c is null || e.gen != e.c.gen)
        return; // stale (unsubscribed/disconnected)
    try
        if (!e.c.tcp.connected)
            return;
    catch (Exception)
        return;
    if (tMatchLen >= tMatchBuf.length)
    {
        try
            tMatchBuf.length = tMatchBuf.length ? tMatchBuf.length * 2 : 64;
        catch (Exception)
            return;
    }
    tMatchBuf[tMatchLen++] = e.c;
}

/// Does `filter` match `topic`? (retained-message delivery at SUBSCRIBE time.)
package bool mqttFilterMatches(scope const(char)[] filter, scope const(char)[] topic) @nogc nothrow
{
    // [MQTT-4.7.2-1]: wildcards never match the first level of a '$' topic
    if (topic.length != 0 && topic[0] == '$')
    {
        size_t fe0 = 0;
        while (fe0 < filter.length && filter[fe0] != '/')
            fe0++;
        auto f0 = filter[0 .. fe0];
        if (f0 == "#" || f0 == "+")
            return false;
    }
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

/// [MQTT-3.3.2-2] a PUBLISH topic NAME: non-empty, no wildcards, no NUL.
package bool mqttValidTopicName(scope const(char)[] t) @nogc nothrow
{
    if (t.length == 0)
        return false;
    foreach (ch; t)
        if (ch == '+' || ch == '#' || ch == '\0')
            return false;
    return true;
}

/// [MQTT-4.7.1] a topic FILTER: non-empty, no NUL; '+' and '#' only as whole
/// segments; '#' only as the last segment. ("a//b" — empty levels — is legal.)
package bool mqttValidFilter(scope const(char)[] f) @nogc nothrow
{
    if (f.length == 0)
        return false;
    size_t i = 0;
    for (;;)
    {
        size_t e = i;
        while (e < f.length && f[e] != '/')
            e++;
        auto seg = f[i .. e];
        foreach (ch; seg)
        {
            if (ch == '\0')
                return false;
            if ((ch == '+' || ch == '#') && seg.length != 1)
                return false;
        }
        if (seg == "#" && e < f.length)
            return false;
        if (e >= f.length)
            return true;
        i = e + 1;
    }
}

// ---------------------------------------------------------------------------
// Wire codec (3.1.1)

private enum ubyte PT_CONNECT = 1, PT_CONNACK = 2, PT_PUBLISH = 3, PT_PUBACK = 4,
        PT_PUBREC = 5, PT_PUBREL = 6, PT_PUBCOMP = 7,
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
public shared ulong gMqttMessages; // total publishes routed (INFO/debug)

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
            if (auto old = topic in gRetained)
            {
                immutable oldLen = (*old).length;
                if (payload.length == 0)
                {
                    gRetained.remove(cast(string) topic);
                    tRetainedBytes -= oldLen;
                }
                else if (tRetainedBytes - oldLen + payload.length <= MQTT_MAX_RETAINED_BYTES)
                {
                    gRetained[cast(string) topic] = payload.idup;
                    tRetainedBytes += payload.length - oldLen;
                }
                else
                    atomicOp!"+="(gMqttRetainedDropped, 1);
            }
            else if (payload.length != 0)
            {
                // new retained topic: both caps gate the store (per thread —
                // the store is replicated, so this bounds every shard's heap)
                if (gRetained.length < MQTT_MAX_RETAINED_TOPICS
                        && tRetainedBytes + payload.length <= MQTT_MAX_RETAINED_BYTES)
                {
                    gRetained[topic.idup] = payload.idup;
                    tRetainedBytes += payload.length;
                }
                else
                    atomicOp!"+="(gMqttRetainedDropped, 1);
            }
        }
        catch (Exception)
        {
        }
    }
    if (atomicLoad!(MemoryOrder.raw)(gMqttSubTotal) == 0)
        return;
    tMatchLen = 0;
    trieMatch(gTrieRoot, topic, 0);
    if (tMatchLen == 0)
        return;
    static ByteBuffer pkt; // TLS: built once per publish, appended to each sub
    pkt.clear();
    buildPublish(pkt, topic, payload, false);
    foreach (s; tMatchBuf[0 .. tMatchLen])
    {
        if (s.closed)
            continue;
        if (s.obox.length + pkt.length > MQTT_OBOX_CAP)
        {
            atomicOp!"+="(gMqttDropped, 1); // QoS0 drop at a full outbox is spec-legal
            continue;
        }
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

/// Wake the writer fiber of every conn touched this batch — called once per
/// publish batch (the publisher's serve loop after its parse pass, and the
/// drain after a fan-in pass). CONTAINS NO YIELD: emit only schedules, so
/// tDirty cannot be mutated under this loop and the drain fiber can never be
/// parked on a subscriber's socket (one slow MQTT client used to stall the
/// whole shard's cross-shard traffic from here).
public void mqttFlushDirty() nothrow @trusted
{
    if (tDirty.length == 0)
        return;
    foreach (c; tDirty)
    {
        c.dirty = false;
        try
            c.flushEvt.emit();
        catch (Exception)
        {
        }
    }
    tDirty.length = 0;
    try
        (cast(MqttConn[]) tDirty).assumeSafeAppend;
    catch (Exception)
    {
    }
}

/// Bitwise buffer swap: ByteBuffer disables copying; a raw byte swap is
/// correct here (no interior self-pointers) and guaranteed yield-free.
private void swapBufs(ref ByteBuffer a, ref ByteBuffer b) @nogc nothrow @trusted
{
    import core.stdc.string : memcpy;

    ubyte[ByteBuffer.sizeof] t = void;
    memcpy(t.ptr, &a, ByteBuffer.sizeof);
    memcpy(&a, &b, ByteBuffer.sizeof);
    memcpy(&b, t.ptr, ByteBuffer.sizeof);
}

/// Idle outboxes shrink back to this; a one-time fan-out burst must not pin
/// ~64MB per connection for the life of an otherwise-idle subscriber. Sized
/// ABOVE the steady-state working set (a windowed consumer accumulates a few
/// hundred KB between writes) so the hot path never churns the allocator —
/// only a genuine multi-MB spike gets released.
private enum size_t MQTT_OBOX_KEEP = 4 << 20;

/// The per-connection writer fiber: drains the outbox for as long as the conn
/// lives. Only THIS fiber writes deliveries, so a blocked write blocks only
/// this subscriber; deliverers append to `obox` while a write is in flight on
/// `wbox`. It releases both buffers deterministically on the owning thread at
/// exit — GC finalization would free these malloc-plane blocks from whatever
/// thread ran the collection, poisoning another shard's freelist. The SERVE
/// fiber owns the socket close (and joins this fiber), so a writer parked in a
/// stalled tcp.write is unblocked when the serve side closes the fd — the
/// stuck-writer-can-never-be-reaped hole the previous design left.
private void mqttWriter(MqttConn c) nothrow
{
    scope (exit)
    {
        c.obox.release();
        c.wbox.release();
    }
    for (;;)
    {
        while (c.obox.empty && !c.closed)
        {
            immutable ec = () @trusted {
                try
                    return c.flushEvt.emitCount;
                catch (Exception)
                    return 0;
            }();
            if (!c.obox.empty || c.closed)
                break;
            try
                c.flushEvt.wait(ec);
            catch (Exception)
            {
            }
        }
        if (c.obox.empty)
        {
            if (c.closed)
                return;
            continue;
        }
        swapBufs(c.obox, c.wbox); // no yield between check and swap
        if (!sendTo(c, c.wbox.data))
        {
            c.closed = true; // dead socket: stop delivering
            return;
        }
        c.wbox.trim(MQTT_OBOX_KEEP); // release a burst-grown block back down
    }
}

private void closeTcp(MqttConn c) nothrow
{
    try
        c.tcp.close();
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

private bool sendTo(MqttConn c, scope const(ubyte)[] bytes) nothrow
{
    try
    {
        c.wlock.lock();
        scope (exit)
            c.wlock.unlock();
        c.tcp.write(bytes);
        return true;
    }
    catch (Exception)
        return false;
}

// ---------------------------------------------------------------------------
// The per-connection serve loop (a fiber per accepted MQTT connection).

// Connection teardown (a named nothrow helper: scope(exit) may not hold a
// try/catch). Order matters: real subscription teardown, wake+close so a
// stalled writer unblocks, then join so buffer release runs on THIS thread.
private void mqttTeardown(MqttConn c, Task writer) nothrow
{
    // tear down subscriptions for real (not just lazily): under connect/
    // subscribe churn the lazy-gen scheme leaked trie entries and pinned
    // gMqttSubTotal above zero forever, keeping the idle-skin gate open.
    foreach (f; c.filters)
        trieUnsubscribe(f, c);
    c.filters = null;
    c.gen++; // invalidate any remaining trie entries (lazily skipped)
    // wake OTHER subscribers whose outboxes this connection's last batch filled
    // (PUBLISH+DISCONNECT coalesced in one read batch is the standard
    // fire-and-forget pattern; their writers are alive and deliver). This
    // connection's OWN obox is best-effort dropped — closeTcp below runs before
    // the join, so its writer's final send finds a closed socket (spec-legal
    // QoS-0 drop on disconnect).
    mqttFlushDirty();
    c.closed = true;
    try
        c.flushEvt.emit(); // wake the writer to drain and exit
    catch (Exception)
    {
    }
    // The SERVE fiber closes the socket — this unblocks a writer parked in a
    // stalled tcp.write (its write throws, sendTo returns false). Then join so
    // buffer release runs (this thread) before the MqttConn is dropped — no
    // reliance on the GC finalizer.
    closeTcp(c);
    try
        writer.join();
    catch (Exception)
    {
    }
}

public void serveMqttClient(TCPConnection tcp) nothrow
{
    try
        tcp.tcpNoDelay = true; // PUBACK/deliveries are tiny — Nagle throttles
    catch (Exception)          // the QoS1 window to ~6k msg/s (measured)
    {
    }
    auto c = new MqttConn(tcp);
    Task writer;
    try
        writer = runTask(&mqttWriter, c);
    catch (Exception)
    {
        closeTcp(c); // no writer fiber exists: close here or the fd leaks
        c.obox.release();
        c.wbox.release();
        return;
    }

    scope (exit)
        mqttTeardown(c, writer);
    ByteBuffer inb;
    ByteBuffer outb;
    for (;;)
    {
        // read at least one byte; a keepalive-exceeded silence closes the conn
        // (waitForData returns false on BOTH the deadline and a real close —
        // both mean "drop it", exactly the MQTT keepalive contract)
        bool alive;
        try
            alive = tcp.waitForData(c.readDeadline);
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
            {
                // 4 continuation bytes present = malformed varint, not merely
                // an incomplete one: close instead of waiting forever
                if (d.length - (pos + 1) >= 4)
                    return;
                break; // incomplete header
            }
            if (rem > MQTT_MAX_PACKET)
                return; // oversized frame: refuse to buffer it
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
            outb.trim(MQTT_OBOX_KEEP); // a retained-# replay can spike outb
        }
        inb.consume(pos);
        if (inb.empty && inb.capacity > MQTT_OBOX_KEEP)
            inb.trim(MQTT_OBOX_KEEP); // drop a max-packet reservation once drained
    }
}

// Returns false to close the connection. Validation posture: anything the
// 3.1.1 spec marks "MUST close the Network Connection" closes; malformed
// SUBSCRIBE filters that are merely unusable get SUBACK failure 0x80.
private bool handlePacket(MqttConn c, ubyte h, scope const(ubyte)[] p,
        ref ByteBuffer o) nothrow @trusted
{
    immutable type = h >> 4;
    immutable fl = h & 0x0F;
    // [MQTT-3.1.0-1] the first packet MUST be CONNECT
    if (!c.connected && type != PT_CONNECT)
        return false;
    switch (type)
    {
    case PT_CONNECT:
        {
            if (c.connected)
                return false; // [MQTT-3.1.0-2] second CONNECT: protocol error
            if (fl != 0)
                return false;
            size_t i = 0;
            const(char)[] proto;
            if (!rdStr(p, i, proto) || i >= p.length)
                return false;
            immutable level = p[i++];
            // [MQTT-3.1.2-1] unknown protocol name: close WITHOUT a CONNACK;
            // known name with wrong level: CONNACK rc=1, then close
            if (proto != "MQTT" && proto != "MQIsdp")
                return false;
            immutable okPair = (proto == "MQTT" && level == 4)
                || (proto == "MQIsdp" && level == 3);
            if (i >= p.length)
                return false;
            immutable flags = p[i++];
            if (flags & 0x01)
                return false; // [MQTT-3.1.2-3] reserved flag MUST be 0
            immutable willQos = (flags >> 3) & 0x3;
            if (willQos == 3)
                return false; // [MQTT-3.1.2-14]
            if (!(flags & 0x04) && (flags & 0x38))
                return false; // [MQTT-3.1.2-11/13/15] will qos/retain w/o will
            // keepalive seconds: enforce 1.5x as the read deadline [MQTT-3.1.2-24]
            immutable ka = cast(ushort)((p[i] << 8) | p[i + 1]);
            i += 2;
            c.readDeadline = ka == 0 ? Duration.max : (ka * 1500).msecs;
            const(char)[] clientId;
            if (!rdStr(p, i, clientId))
                return false;
            // [MQTT-3.1.3-8] empty ClientId REQUIRES CleanSession=1: refuse
            // rc=0x02 (we run clean-only, but the refusal is still mandated)
            if (clientId.length == 0 && !(flags & 0x02))
            {
                o.appendByte(cast(char)(PT_CONNACK << 4));
                o.appendByte(cast(char) 2);
                o.appendByte(cast(char) 0);
                o.appendByte(cast(char) 2);
                return false;
            }
            // skip will topic/message, username, password per flags
            if (flags & 0x04)
            {
                const(char)[] wt, wm;
                if (!rdStr(p, i, wt) || !rdStr(p, i, wm))
                    return false;
                if (!mqttValidTopicName(wt))
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
            c.connected = okPair;
            // CONNACK: session-present=0
            o.appendByte(cast(char)(PT_CONNACK << 4));
            o.appendByte(cast(char) 2);
            o.appendByte(cast(char) 0);
            o.appendByte(cast(char)(okPair ? 0 : 1));
            return okPair;
        }
    case PT_PUBLISH:
        {
            immutable qos = (h >> 1) & 0x3;
            if (qos == 3)
                return false; // [MQTT-3.3.1-4] both QoS bits set: close
            immutable retain = (h & 1) != 0;
            size_t i = 0;
            const(char)[] topic;
            if (!rdStr(p, i, topic))
                return false;
            if (!mqttValidTopicName(topic))
                return false; // [MQTT-3.3.2-2] wildcard/empty/NUL topic: close
            ushort pid = 0;
            if (qos > 0)
            {
                if (i + 2 > p.length)
                    return false;
                pid = cast(ushort)((p[i] << 8) | p[i + 1]);
                i += 2;
                if (pid == 0)
                    return false; // [MQTT-2.3.1-1]
            }
            auto payload = cast(const(char)[]) p[i .. $];
            if (qos == 2)
            {
                // dedup: a retransmit of an in-flight qos2 id is acked again
                // but NOT redelivered
                bool dup = false;
                foreach (q; c.q2pids)
                    if (q == pid)
                    {
                        dup = true;
                        break;
                    }
                if (!dup)
                {
                    if (c.q2pids.length >= 1024)
                        return false; // receive-window abuse
                    atomicOp!"+="(gMqttMessages, 1);
                    mqttDeliverLocal(topic, payload, retain);
                    if (gMqttFanout !is null)
                        gMqttFanout(topic, payload, retain);
                    try
                        c.q2pids ~= pid;
                    catch (Exception)
                        return false;
                }
                o.appendByte(cast(char)(PT_PUBREC << 4));
                o.appendByte(cast(char) 2);
                o.appendByte(cast(char)(pid >> 8));
                o.appendByte(cast(char)(pid & 0xFF));
                return true;
            }
            atomicOp!"+="(gMqttMessages, 1);
            mqttDeliverLocal(topic, payload, retain);
            if (gMqttFanout !is null)
                gMqttFanout(topic, payload, retain);
            if (qos == 1)
            {
                o.appendByte(cast(char)(PT_PUBACK << 4));
                o.appendByte(cast(char) 2);
                o.appendByte(cast(char)(pid >> 8));
                o.appendByte(cast(char)(pid & 0xFF));
            }
            return true;
        }
    case PT_PUBREL:
        {
            // completes the qos2 receive handshake
            if (fl != 0x02 || p.length < 2)
                return false; // [MQTT-3.6.1-1]
            immutable pid = cast(ushort)((p[0] << 8) | p[1]);
            size_t w = 0;
            foreach (q; c.q2pids)
                if (q != pid)
                    c.q2pids[w++] = q;
            c.q2pids.length = w;
            o.appendByte(cast(char)(PT_PUBCOMP << 4));
            o.appendByte(cast(char) 2);
            o.appendByte(cast(char)(pid >> 8));
            o.appendByte(cast(char)(pid & 0xFF));
            return true;
        }
    case PT_SUBSCRIBE:
        {
            if (fl != 0x02)
                return false; // [MQTT-3.8.1-1]
            if (p.length < 2)
                return false;
            immutable pid = cast(ushort)((p[0] << 8) | p[1]);
            if (pid == 0)
                return false; // [MQTT-2.3.1-1]
            size_t i = 2;
            // NO-YIELD window: these TLS scratch arrays are filled and
            // consumed without any suspension point in between
            static ubyte[64] granted = void;
            size_t ng = 0;
            static const(char)[][64] filters;
            while (i < p.length)
            {
                if (ng >= granted.length)
                    return false; // >64 filters in one packet: refuse
                const(char)[] filter;
                if (!rdStr(p, i, filter) || i >= p.length)
                    return false;
                immutable reqQos = p[i++];
                if (reqQos > 2)
                    return false; // [MQTT-3.8.3-4]
                if (!mqttValidFilter(filter) || c.filters.length >= MQTT_MAX_SUBS)
                {
                    granted[ng] = 0x80; // unusable filter / sub-cap: failure
                    filters[ng++] = null;
                    continue;
                }
                // teardown-list entry FIRST: if this allocation fails we
                // refuse the filter instead of creating a trie entry with no
                // teardown record (that combination leaked gMqttSubTotal)
                const(char)[] fcopy;
                try
                {
                    fcopy = filter.idup;
                    c.filters ~= fcopy;
                }
                catch (Exception)
                {
                    granted[ng] = 0x80;
                    filters[ng++] = null;
                    continue;
                }
                if (!trieSubscribe(filter, c, 0))
                    c.filters.length = c.filters.length - 1; // replaced: no new entry
                filters[ng] = filter;
                granted[ng++] = 0; // v1: everything granted at QoS 0
            }
            if (ng == 0)
                return false; // [MQTT-3.8.3-3] at least one filter required
            o.appendByte(cast(char)(PT_SUBACK << 4));
            encodeVarint(o, cast(uint)(2 + ng));
            o.appendByte(cast(char)(pid >> 8));
            o.appendByte(cast(char)(pid & 0xFF));
            foreach (g; granted[0 .. ng])
                o.appendByte(cast(char) g);
            // retained messages matching the new filters, delivered after SUBACK
            try
                foreach (topic, payload; gRetained)
                {
                    if (o.length > MQTT_OBOX_CAP)
                        break; // bounded replay burst (retained is QoS0/best-effort)
                    foreach (f; filters[0 .. ng])
                        if (f !is null && mqttFilterMatches(f, topic))
                        {
                            buildPublish(o, topic, payload, true);
                            break;
                        }
                }
            catch (Exception)
            {
            }
            return true;
        }
    case PT_UNSUBSCRIBE:
        {
            if (fl != 0x02)
                return false; // [MQTT-3.10.1-1]
            if (p.length < 2)
                return false;
            immutable pid = cast(ushort)((p[0] << 8) | p[1]);
            if (pid == 0)
                return false;
            size_t i = 2;
            bool any = false;
            while (i < p.length)
            {
                const(char)[] filter;
                if (!rdStr(p, i, filter))
                    return false;
                any = true;
                if (!mqttValidFilter(filter))
                    continue; // ack it, but never trie-walk a malformed filter
                trieUnsubscribe(filter, c);
                size_t w = 0;
                foreach (f; c.filters)
                    if (f != filter)
                        c.filters[w++] = f;
                c.filters.length = w;
            }
            if (!any)
                return false; // [MQTT-3.10.3-2]
            o.appendByte(cast(char)(PT_UNSUBACK << 4));
            o.appendByte(cast(char) 2);
            o.appendByte(cast(char)(pid >> 8));
            o.appendByte(cast(char)(pid & 0xFF));
            return true;
        }
    case PT_PINGREQ:
        if (fl != 0)
            return false;
        o.appendByte(cast(char)(PT_PINGRESP << 4));
        o.appendByte(cast(char) 0);
        return true;
    case PT_DISCONNECT:
        return false;
    default:
        // types 0 and 15 are reserved; CONNACK/PUBACK/PUBREC/PUBCOMP/SUBACK/
        // UNSUBACK/PINGRESP are server->client only (we never send QoS>0 out,
        // so none is ever legitimate inbound). This default also replaces a
        // final-switch that turned any of them into a runtime SwitchError —
        // one PUBREL used to crash the whole server.
        return false;
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

unittest // [MQTT-4.7.2-1] '$' topics never match root-level wildcards
{
    assert(!mqttFilterMatches("#", "$SYS/broker/load"));
    assert(!mqttFilterMatches("+/broker/load", "$SYS/broker/load"));
    assert(mqttFilterMatches("$SYS/#", "$SYS/broker/load"));
    assert(mqttFilterMatches("$SYS/+/load", "$SYS/broker/load"));
}

unittest // topic-name and filter validation
{
    assert(mqttValidTopicName("a/b/c"));
    assert(mqttValidTopicName("/"));
    assert(!mqttValidTopicName(""));
    assert(!mqttValidTopicName("a/+/c"));
    assert(!mqttValidTopicName("a/#"));
    assert(!mqttValidTopicName("a\0b"));
    assert(mqttValidFilter("a/+/c"));
    assert(mqttValidFilter("a/#"));
    assert(mqttValidFilter("#"));
    assert(mqttValidFilter("a//b"));
    assert(!mqttValidFilter(""));
    assert(!mqttValidFilter("a/#/b"));
    assert(!mqttValidFilter("a+"));
    assert(!mqttValidFilter("+a"));
    assert(!mqttValidFilter("a/b#"));
}
