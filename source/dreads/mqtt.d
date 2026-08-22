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
// Scope: QoS 0/1/2 delivery (outbound PUBLISH at min(publishQoS, subGrant): QoS1
// tracks an unacked id, QoS2 runs the PUBLISH/PUBREC/PUBREL/PUBCOMP handshake, a
// bounded per-conn in-flight window degrades to QoS0), QoS 1 receive (PUBACK on
// receipt), QoS 2 receive (full PUBREC/PUBREL/PUBCOMP handshake with
// packet-id dedup), clean sessions only (CONNACK session-present=0), retained
// messages, keepalive enforced at 1.5x [MQTT-3.1.2-24] (0 = none; TCP death is
// also detected by the read loop), overlapping subscriptions may deliver
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

/// Monotonic per-CONNECT stamp: the newest session with a given clientId wins
/// the takeover race across shards (convergence, like gMqttRetainSeq).
public shared ulong gMqttConnGen;
/// THIS shard's live clientId -> conn map (share-nothing).
private MqttConn[string] gLocalClients; // TLS
/// Broadcast a (clientId, connGen) takeover to every OTHER shard (installed by
/// server.d). Null when unsharded.
public __gshared void delegate(scope const(char)[] clientId, ulong gen) nothrow gMqttConnBcast;

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
    ubyte protoVer = 4; // MQTT protocol level: 4 = 3.1.1, 5 = 5.0 (v5 packets
    // carry a property block; v5 CONNACK/SUBACK use reason codes)
    // v5 inbound topic aliases: a client maps a small int -> topic to save bytes;
    // first PUBLISH with an alias carries the topic + alias (registers it), later
    // ones carry an empty topic + the alias (resolved here). Bounded by the
    // topic-alias-maximum we advertise in CONNACK.
    string[ushort] inAlias;
    size_t inAliasBytes; // running size of the aliased topics (byte cap)
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
    // Client identity for [MQTT-3.1.4-2] takeover: a new CONNECT with the same
    // (non-empty) clientId must disconnect the existing session. connGen is a
    // global monotonic stamp so the NEWEST connection wins regardless of the
    // order the cross-shard takeover broadcasts arrive (the retained-seq lesson).
    string clientId;
    ulong connGen;
    // Last Will and Testament ([MQTT-3.1.2-8]): published if the connection
    // drops abnormally (TCP death, takeover, protocol error) but NOT on a clean
    // DISCONNECT, which clears it. willTopic empty = no will.
    string willTopic;
    const(ubyte)[] willPayload;
    bool willRetain;
    // QoS1/2 OUTBOUND delivery: a per-conn packet-id (1..65535, wraps), the set
    // of QoS1 ids delivered but not yet PUBACKed, and the QoS2 ids mid-handshake
    // (outQos2[pid]: 1 = awaiting PUBREC, 2 = PUBREL sent, awaiting PUBCOMP). One
    // shared pid space; the combined window bounds RAM -> a slow consumer degrades
    // the delivery to QoS0 rather than growing memory. In-session only: no
    // cross-reconnect retransmit (needs persistent sessions).
    ushort nextPid = 1;
    bool[ushort] inflight;
    ubyte[ushort] outQos2;

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
/// Per-connection outbound in-flight window (QoS1 inflight + QoS2 handshakes,
/// combined): past this, a QoS1/2 delivery is sent as QoS0 (graceful
/// degradation, no unbounded in-flight growth for a slow acker).
private enum size_t MQTT_QOS1_WINDOW = 1024;
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
/// v5 topic-alias-maximum we advertise: the largest alias a client may use when
/// publishing to us (also bounds the per-conn inbound alias table by COUNT).
private enum ushort MQTT_TOPIC_ALIAS_MAX = 1024;
/// ...and by BYTES: an aliased topic is idup'd (up to ~64KB), so the count cap
/// alone would let one conn pin MAX*64KB; this bounds the whole table.
private enum size_t MQTT_MAX_ALIAS_BYTES = 1 << 20; // 1MB of aliased topics/conn

// ---------------------------------------------------------------------------
// Topic trie (THREAD-LOCAL). Segment-split on '/'; `+` matches exactly one
// segment, `#` matches the rest (including zero segments). Standard MQTT
// matching semantics, one trie per shard thread.

private struct SubEntry
{
    MqttConn c;
    uint gen; // conn's gen at subscribe time; != c.gen ⇒ stale (lazily skipped)
    ubyte qos; // granted qos (v1: always 0 on delivery)
    ubyte opts; // v5 subscription options: bit2 no-local, bit3 retain-as-published,
    // bits4-5 retain-handling (v3 subs: 0 = deliver-all, clear-retain, send-retained)
    string shareGroup; // v5 shared subscription ($share/<group>/...): one member
    // of the group gets each message (round-robin). Empty = a normal subscription.
}

private final class TrieNode
{
    TrieNode[string] kids; // segment -> child (GC AA: control plane, not hot path)
    TrieNode plus; // the `+` child
    SubEntry[] subs; // subscribers terminating at THIS node
    SubEntry[] hash; // `#` subscribers rooted at this node
}

private TrieNode gTrieRoot; // TLS: this thread's subscription trie
/// A retained entry carries a global monotonic SEQUENCE so cross-shard
/// replication converges: shards apply an incoming retained op only when its
/// seq beats the stored one. An empty payload is a TOMBSTONE (a retained
/// DELETE) that still holds a seq, so a late lower-seq SET can't resurrect it.
private struct Retained
{
    const(char)[] payload;
    ulong seq;
}

private Retained[string] gRetained; // TLS: topic -> retained value (replicated)

/// Global monotonic sequence stamped on every retained publish (origin-agnostic
/// total order). Only retained publishes touch it — off the delivery hot path.
public shared ulong gMqttRetainSeq;

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
private bool upsertEntry(ref SubEntry[] a, MqttConn c, ubyte qos, ubyte opts,
        string shareGroup = null) @trusted nothrow
{
    // identity is (conn, gen, shareGroup): a normal sub and a shared sub (or two
    // different groups) to the same filter are DISTINCT subscriptions.
    foreach (ref e; a)
        if (e.c is c && e.gen == c.gen && e.shareGroup == shareGroup)
        {
            e.qos = qos;
            e.opts = opts;
            return false;
        }
    try
        a ~= SubEntry(c, c.gen, qos, opts, shareGroup);
    catch (Exception)
        return false;
    atomicOp!"+="(gMqttSubTotal, 1);
    return true;
}

// Split-walk `filter` creating nodes, then upsert the entry at the terminal.
// Returns true when a NEW subscription was created (false = replaced/failed).
private bool trieSubscribe(scope const(char)[] filter, MqttConn c, ubyte qos,
        ubyte opts = 0, string shareGroup = null) @trusted nothrow
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
                return upsertEntry(n.hash, c, qos, opts, shareGroup);
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
                return upsertEntry(next.subs, c, qos, opts, shareGroup);
            n = next;
            i = e + 1;
        }
    }
    catch (Exception)
    {
    }
    return false;
}

// Remove entries of `c` under `filter`. matchGroup=false removes ALL of the
// conn's entries there (disconnect teardown); matchGroup=true removes only the
// one whose shareGroup == group (an explicit UNSUBSCRIBE of one subscription).
private void trieUnsubscribe(scope const(char)[] filter, MqttConn c,
        bool matchGroup = false, string group = null) @trusted nothrow
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
                dropConn(n.hash, c, matchGroup, group);
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
                dropConn(next.subs, c, matchGroup, group);
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

private void dropConn(ref SubEntry[] a, MqttConn c, bool matchGroup = false,
        string group = null) @trusted nothrow
{
    size_t w = 0;
    foreach (ref e; a)
        if (e.c !is c || (matchGroup && e.shareGroup != group))
            a[w++] = e; // keep
    if (w != a.length)
    {
        atomicOp!"-="(gMqttSubTotal, cast(long)(a.length - w));
        a.length = w;
    }
}

// TLS match scratch: grows to the largest fan-out seen on this thread. A
// fixed [64] here silently starved every subscriber past the 64th — forever,
// per publish, with no counter.
private struct Match // one matched subscriber + its granted qos (fused so the
{                    // conn and its qos can never desync — no parallel arrays)
    MqttConn c;
    ubyte qos;
    bool noLocal; // v5 no-local: don't echo the publisher's own message back
    bool rap; // v5 retain-as-published: keep the publisher's retain flag
    string shareGroup; // v5 shared sub group ("" = normal, deliver to all)
}

private Match[] tMatchBuf;
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
    tMatchBuf[tMatchLen++] = Match(e.c, e.qos, (e.opts & 0x04) != 0,
            (e.opts & 0x08) != 0, e.shareGroup);
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
public __gshared void delegate(scope const(char)[] topic, scope const(char)[] payload,
        bool retain, ulong seq, ubyte pubQos, scope const(char)[] props) nothrow gMqttFanout;
public shared ulong gMqttMessages; // total publishes routed (INFO/debug)
/// Broker start time (ms) for $SYS/broker/uptime; stamped on the first $SYS tick.
private shared ulong gMqttStartMs;

/// Publish the $SYS/broker/* broker-monitoring topics to THIS shard's local
/// subscribers (a de-facto MQTT standard; mosquitto-compatible). Called ~every
/// 10s from the maintenance tick. Values are the GLOBAL atomic counters, so a
/// client subscribed to $SYS on any shard sees broker-wide stats. Non-retained
/// (a fresh subscriber gets the next tick); the $-guard already lets an explicit
/// `$SYS/#` match while keeping `#`/`+` from matching $-topics.
public void mqttPublishSys() nothrow @trusted
{
    import core.atomic : atomicLoad, atomicStore, MemoryOrder;
    import dreads.stream : nowMs;
    import core.stdc.stdio : snprintf;

    if (atomicLoad!(MemoryOrder.raw)(gMqttSubTotal) == 0)
        return; // nobody subscribed to anything -> skip (idle-skin cost = 0)
    immutable now = nowMs();
    if (atomicLoad!(MemoryOrder.raw)(gMqttStartMs) == 0)
        atomicStore!(MemoryOrder.raw)(gMqttStartMs, now);
    static char[32] nb = void;
    void pub(scope const(char)[] topic, scope const(char)[] payload) nothrow
    {
        mqttDeliverLocal(topic, payload, false, 0);
    }
    static void num(ref char[32] b, ulong v, ref const(char)[] outp) nothrow
    {
        immutable n = snprintf(b.ptr, b.length, "%llu", v);
        outp = n > 0 ? cast(const(char)[]) b[0 .. n] : "0";
    }

    const(char)[] v;
    pub("$SYS/broker/version", "dreads MQTT 3.1.1");
    num(nb, (now - atomicLoad!(MemoryOrder.raw)(gMqttStartMs)) / 1000, v);
    pub("$SYS/broker/uptime", v);
    num(nb, cast(ulong) atomicLoad!(MemoryOrder.raw)(gMqttMessages), v);
    pub("$SYS/broker/messages/received", v);
    immutable st = atomicLoad!(MemoryOrder.raw)(gMqttSubTotal);
    num(nb, st < 0 ? 0 : cast(ulong) st, v);
    pub("$SYS/broker/subscriptions/count", v);
    num(nb, cast(ulong) atomicLoad!(MemoryOrder.raw)(gMqttDropped), v);
    pub("$SYS/broker/messages/dropped", v);
    // deliver the $SYS batch (mqttDeliverLocal queued into subscriber outboxes)
    mqttFlushDirty();
}

/// Close a local session a NEWER CONNECT (gen) superseded. Same-thread, so the
/// map access is unlocked. Setting closed + waking the socket makes the victim's
/// serve fiber observe the close on its next read and run its normal teardown.
private void takeoverLocal(scope const(char)[] clientId, ulong gen) nothrow @trusted
{
    try
    {
        if (auto pc = clientId in gLocalClients)
        {
            auto victim = *pc;
            if (victim !is null && victim.connGen < gen)
            {
                victim.closed = true;
                try
                    victim.flushEvt.emit();
                catch (Exception)
                {
                }
                try
                    victim.tcp.close(); // wakes its serve fiber -> teardown
                catch (Exception)
                {
                }
            }
        }
    }
    catch (Exception)
    {
    }
}

/// A CONNECT on another shard took over `clientId` (drain's mqttConnect fan-in).
public void mqttTakeover(scope const(char)[] clientId, ulong gen) nothrow @trusted
{
    takeoverLocal(clientId, gen);
}

/// Deliver `topic`/`payload` to THIS thread's matching subscribers, and update
/// this thread's retained map when asked. Called for local publishes AND for
/// fan-in from other shards (the drain's mqttPub case).
public void mqttDeliverLocal(scope const(char)[] topic, scope const(char)[] payload,
        bool retain, ulong seq, ubyte pubQos = 0, MqttConn publisher = null,
        scope const(char)[] props = null) nothrow @trusted
{
    if (retain)
    {
        try
        {
            auto old = topic in gRetained;
            // seq-gated: apply only a strictly newer op (converges every shard
            // regardless of the order the SPSC lanes deliver same-topic writes
            // from different origins). A tombstone (empty payload) keeps its seq
            // so a late lower-seq SET is rejected, not resurrected.
            if (old !is null && seq <= old.seq)
            {
                // stale replica write: ignore
            }
            else if (old !is null)
            {
                immutable oldLen = old.payload.length;
                if (payload.length == 0)
                {
                    gRetained[cast(string) topic] = Retained(null, seq); // tombstone
                    tRetainedBytes -= oldLen;
                }
                else if (tRetainedBytes - oldLen + payload.length <= MQTT_MAX_RETAINED_BYTES)
                {
                    gRetained[cast(string) topic] = Retained(payload.idup, seq);
                    tRetainedBytes += payload.length - oldLen;
                }
                else
                    atomicOp!"+="(gMqttRetainedDropped, 1);
            }
            else // new topic
            {
                if (gRetained.length < MQTT_MAX_RETAINED_TOPICS
                        && tRetainedBytes + payload.length <= MQTT_MAX_RETAINED_BYTES)
                {
                    gRetained[topic.idup] = Retained(payload.length ? payload.idup : null, seq);
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
    // QoS0 packet built ONCE and shared (the hot path); QoS1 subscribers get a
    // per-conn packet with their own packet-id (a separate, slower branch that
    // does NOT touch the QoS0 fast path).
    static ByteBuffer pkt; // TLS: the shared QoS0 publish (v3)
    static ByteBuffer pktV5; // TLS: the shared QoS0 publish (v5, empty props)
    static ByteBuffer q1; // TLS: a per-subscriber QoS1 publish
    pkt.clear();
    buildPublish(pkt, topic, payload, false);
    bool pktV5built = false; // build the v5 variant lazily (only if a v5 sub matches)
    // Deliver one message to one matched subscriber (QoS/no-local/RAP-aware).
    void deliverTo(ref Match m) @trusted nothrow
    {
        auto s = m.c;
        if (s.closed)
            return;
        if (m.noLocal && s is publisher)
            return; // v5 no-local: don't echo the publisher's own message back
        immutable v5 = s.protoVer == 5;
        // v5 retain-as-published keeps the publisher's retain flag; otherwise a
        // forwarded delivery clears retain [MQTT-3.3.1-9].
        immutable delRetain = m.rap && retain;
        immutable effQos = pubQos < m.qos ? pubQos : m.qos;
        if (effQos >= 1)
        {
            // QoS1/2: assign a packet-id and track it in flight (a saturated
            // window degrades this delivery to QoS0)
            ushort pid = 0;
            if (s.inflight.length + s.outQos2.length < MQTT_QOS1_WINDOW)
                pid = nextDeliveryPid(s);
            if (pid != 0)
            {
                q1.clear();
                if (effQos == 2)
                    buildPublishQos2(q1, topic, payload, delRetain, pid, v5, props);
                else
                    buildPublishQos1(q1, topic, payload, delRetain, pid, v5, props);
                if (s.obox.length + q1.length > MQTT_OBOX_CAP)
                {
                    atomicOp!"+="(gMqttDropped, 1);
                    return;
                }
                s.obox.append(q1.data);
                try
                {
                    if (effQos == 2)
                        s.outQos2[pid] = 1; // awaiting PUBREC
                    else
                        s.inflight[pid] = true;
                }
                catch (Exception)
                {
                }
                if (!s.dirty)
                {
                    s.dirty = true;
                    try
                        tDirty ~= s;
                    catch (Exception)
                    {
                    }
                }
                return;
            }
            // window saturated -> deliver at QoS0
        }
        if (delRetain)
        {
            // rare (retain-as-published, QoS0): the shared packet has retain=0,
            // so build a one-off with the retain bit set
            q1.clear();
            buildPublish(q1, topic, payload, true, v5, props);
            if (s.obox.length + q1.length > MQTT_OBOX_CAP)
            {
                atomicOp!"+="(gMqttDropped, 1);
                return;
            }
            s.obox.append(q1.data);
        }
        else
        {
            if (v5 && !pktV5built)
            {
                pktV5.clear();
                buildPublish(pktV5, topic, payload, false, true, props);
                pktV5built = true;
            }
            immutable plen = v5 ? pktV5.length : pkt.length;
            if (s.obox.length + plen > MQTT_OBOX_CAP)
            {
                atomicOp!"+="(gMqttDropped, 1); // QoS0 drop at a full outbox is spec-legal
                return;
            }
            s.obox.append(v5 ? pktV5.data : pkt.data);
        }
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

    // Pass 1: deliver to every NORMAL subscriber; tally shared subs per group.
    static size_t[string] groupCount; // TLS, reused
    bool anyShared = false;
    foreach (ref m; tMatchBuf[0 .. tMatchLen])
    {
        if (m.shareGroup.length == 0)
            deliverTo(m);
        else
        {
            anyShared = true;
            try
                groupCount[m.shareGroup] = groupCount.get(m.shareGroup, 0) + 1;
            catch (Exception)
            {
            }
        }
    }
    // Pass 2: shared subscriptions — ONE member of each group receives the
    // message, chosen round-robin across deliveries (load balancing).
    if (anyShared)
    {
        static size_t[string] groupPos; // TLS
        foreach (ref m; tMatchBuf[0 .. tMatchLen])
        {
            if (m.shareGroup.length == 0)
                continue;
            size_t cnt, pos, rr;
            try
            {
                cnt = groupCount.get(m.shareGroup, 0);
                pos = groupPos.get(m.shareGroup, 0);
                rr = gShareRR.get(m.shareGroup, 0);
            }
            catch (Exception)
            {
            }
            if (cnt != 0 && pos == rr % cnt)
                deliverTo(m);
            try
                groupPos[m.shareGroup] = pos + 1;
            catch (Exception)
            {
            }
        }
        try
        {
            foreach (g, _; groupCount)
                gShareRR[g] = gShareRR.get(g, 0) + 1; // advance for next message
            groupCount.clear();
            groupPos.clear();
        }
        catch (Exception)
        {
        }
    }
}

// Round-robin cursor per shared-subscription group (TLS; persists across
// deliveries so successive messages rotate through the group's members).
private size_t[string] gShareRR;

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

/// Byte length of `v` encoded as a remaining-length varint (1-4 bytes).
private uint varintSize(uint v) @nogc nothrow pure
{
    if (v < 128)
        return 1;
    if (v < 16384)
        return 2;
    if (v < 2097152)
        return 3;
    return 4;
}

private void buildPublish(ref ByteBuffer o, scope const(char)[] topic,
        scope const(char)[] payload, bool retain, bool v5 = false,
        scope const(char)[] props = null) @nogc nothrow
{
    o.appendByte(cast(char)((PT_PUBLISH << 4) | (retain ? 1 : 0))); // QoS 0 out
    encodeVarint(o, cast(uint)(2 + topic.length
            + (v5 ? varintSize(cast(uint) props.length) + props.length : 0)
            + payload.length));
    o.appendByte(cast(char)(topic.length >> 8));
    o.appendByte(cast(char)(topic.length & 0xFF));
    o.append(topic);
    if (v5) // v5 property block: length varint + the forwarded properties
    {
        encodeVarint(o, cast(uint) props.length);
        o.append(props);
    }
    o.append(payload);
}

/// QoS1 PUBLISH (DUP=0): fixed header 0x32|retain, then topic, packet-id, payload.
private void buildPublishQos1(ref ByteBuffer o, scope const(char)[] topic,
        scope const(char)[] payload, bool retain, ushort pid, bool v5 = false,
        scope const(char)[] props = null) @nogc nothrow
{
    o.appendByte(cast(char)((PT_PUBLISH << 4) | 0x02 | (retain ? 1 : 0)));
    encodeVarint(o, cast(uint)(2 + topic.length + 2
            + (v5 ? varintSize(cast(uint) props.length) + props.length : 0)
            + payload.length));
    o.appendByte(cast(char)(topic.length >> 8));
    o.appendByte(cast(char)(topic.length & 0xFF));
    o.append(topic);
    o.appendByte(cast(char)(pid >> 8));
    o.appendByte(cast(char)(pid & 0xFF));
    if (v5)
    {
        encodeVarint(o, cast(uint) props.length);
        o.append(props);
    }
    o.append(payload);
}

/// QoS2 PUBLISH (DUP=0): fixed header 0x34|retain, then topic, packet-id, payload.
private void buildPublishQos2(ref ByteBuffer o, scope const(char)[] topic,
        scope const(char)[] payload, bool retain, ushort pid, bool v5 = false,
        scope const(char)[] props = null) @nogc nothrow
{
    o.appendByte(cast(char)((PT_PUBLISH << 4) | 0x04 | (retain ? 1 : 0)));
    encodeVarint(o, cast(uint)(2 + topic.length + 2
            + (v5 ? varintSize(cast(uint) props.length) + props.length : 0)
            + payload.length));
    o.appendByte(cast(char)(topic.length >> 8));
    o.appendByte(cast(char)(topic.length & 0xFF));
    o.append(topic);
    o.appendByte(cast(char)(pid >> 8));
    o.appendByte(cast(char)(pid & 0xFF));
    if (v5)
    {
        encodeVarint(o, cast(uint) props.length);
        o.append(props);
    }
    o.append(payload);
}

/// Next outbound packet-id for `c` not already in flight on EITHER the QoS1 or
/// the QoS2 handshake (one shared id space; skips 0).
private ushort nextDeliveryPid(MqttConn c) @trusted nothrow
{
    foreach (_; 0 .. 65535)
    {
        immutable pid = c.nextPid;
        c.nextPid = c.nextPid == 0xFFFF ? 1 : cast(ushort)(c.nextPid + 1);
        bool used = false;
        try
            used = (pid in c.inflight) !is null || (pid in c.outQos2) !is null;
        catch (Exception)
        {
        }
        if (pid != 0 && !used)
            return pid;
    }
    return 0; // window saturated (all ids in flight)
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
    // drop our clientId registration, but only if a NEWER session hasn't
    // already replaced us in the map (identity check)
    if (c.clientId.length != 0)
        try
        {
            if (auto pc = c.clientId in gLocalClients)
                if (*pc is c)
                    gLocalClients.remove(c.clientId);
        }
        catch (Exception)
        {
        }
    c.gen++; // invalidate any remaining trie entries (lazily skipped)
    // Last Will: an abnormal disconnect (TCP death / takeover / protocol error)
    // left willTopic set (a clean DISCONNECT cleared it) -> publish it now, the
    // same path a live PUBLISH takes (local delivery + cross-shard fan-out +
    // retained if the will-retain flag was set).
    if (c.connected && c.willTopic.length != 0)
    {
        immutable rseq = c.willRetain ? atomicOp!"+="(gMqttRetainSeq, 1) : 0;
        mqttDeliverLocal(c.willTopic, cast(const(char)[]) c.willPayload, c.willRetain, rseq);
        if (gMqttFanout !is null)
            gMqttFanout(c.willTopic, cast(const(char)[]) c.willPayload, c.willRetain, rseq, 0, null);
        c.willTopic = null;
    }
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

// Skip an MQTT 5 property block at p[i]: a varint length then that many bytes.
// Used where we don't (yet) consume any property (CONNECT/will/SUBSCRIBE/
// UNSUBSCRIBE). Returns false on a malformed/truncated block.
private bool mqttSkipProps(scope const(ubyte)[] p, ref size_t i) @nogc nothrow
{
    uint plen;
    if (!decodeVarint(p, i, plen))
        return false;
    if (i + plen > p.length)
        return false;
    i += plen;
    return true;
}

// The v5 PUBLISH properties we consume (the rest are correctly skipped by type).
private struct PubProps
{
    bool hasAlias;
    ushort topicAlias;
    bool hasExpiry;
    uint msgExpiry; // seconds
}

// Parse an MQTT 5 PUBLISH property block at p[i], extracting topic-alias and
// message-expiry, and building into `fwd` the FORWARDABLE properties (everything
// EXCEPT topic-alias, which is per-hop, and subscription-identifier, which the
// broker assigns) so a delivery can replay content-type / response-topic /
// correlation-data / user-property / payload-format to the subscriber. Advances
// i to the end of the block; every read is bounded by `end` (itself <= p.length)
// so a malformed block can't read OOB.
private bool mqttParsePubProps(scope const(ubyte)[] p, ref size_t i, out PubProps pp,
        ref ByteBuffer fwd) @nogc nothrow
{
    fwd.clear();
    uint plen;
    if (!decodeVarint(p, i, plen))
        return false;
    immutable end = i + plen;
    if (end > p.length)
        return false;
    while (i < end)
    {
        immutable propStart = i; // the id byte, for a verbatim forward copy
        immutable id = p[i++];
        bool forward = true;
        switch (id)
        {
        case 0x01: // payload-format-indicator: 1 byte
            if (i + 1 > end)
                return false;
            i += 1;
            break;
        case 0x02: // message-expiry-interval: u32 seconds
            if (i + 4 > end)
                return false;
            pp.msgExpiry = (cast(uint) p[i] << 24) | (cast(uint) p[i + 1] << 16)
                | (cast(uint) p[i + 2] << 8) | p[i + 3];
            pp.hasExpiry = true;
            i += 4;
            break;
        case 0x23: // topic-alias: u16 — per-hop, NOT forwarded
            if (i + 2 > end)
                return false;
            pp.topicAlias = cast(ushort)((p[i] << 8) | p[i + 1]);
            pp.hasAlias = true;
            i += 2;
            forward = false;
            break;
        case 0x03, 0x08, 0x09: // content-type / response-topic / correlation-data:
            // a 2-byte length prefix then that many bytes
            if (i + 2 > end)
                return false;
            immutable n = (cast(size_t) p[i] << 8) | p[i + 1];
            i += 2;
            if (i + n > end)
                return false;
            i += n;
            break;
        case 0x0B: // subscription-identifier: varint — broker-assigned, NOT forwarded
            uint sv;
            if (!decodeVarint(p, i, sv) || i > end)
                return false;
            forward = false;
            break;
        case 0x26: // user-property: two length-prefixed strings
            foreach (_; 0 .. 2)
            {
                if (i + 2 > end)
                    return false;
                immutable n = (cast(size_t) p[i] << 8) | p[i + 1];
                i += 2;
                if (i + n > end)
                    return false;
                i += n;
            }
            break;
        default:
            return false; // unknown property id in a PUBLISH: malformed
        }
        if (forward)
            fwd.append(cast(const(char)[]) p[propStart .. i]);
    }
    i = end;
    return true;
}

// CONNACK for either protocol version. v5 adds a reason code (0x00 = Success,
// 0x80+ = failure) and an (empty, for now) property block; v3 uses the legacy
// return code. `code` is already in the caller's version's encoding.
private void mqttConnack(ref ByteBuffer o, ubyte protoVer, bool sessionPresent,
        ubyte code) @nogc nothrow
{
    o.appendByte(cast(char)(PT_CONNACK << 4));
    if (protoVer == 5)
    {
        // properties: topic-alias-maximum (0x22, u16) so the client may alias
        o.appendByte(cast(char) 6); // ack-flags + reason + prop-len(1) + prop(3)
        o.appendByte(cast(char)(sessionPresent ? 1 : 0));
        o.appendByte(cast(char) code);
        o.appendByte(cast(char) 3); // property length
        o.appendByte(cast(char) 0x22); // topic-alias-maximum
        o.appendByte(cast(char)(MQTT_TOPIC_ALIAS_MAX >> 8));
        o.appendByte(cast(char)(MQTT_TOPIC_ALIAS_MAX & 0xFF));
    }
    else
    {
        o.appendByte(cast(char) 2);
        o.appendByte(cast(char)(sessionPresent ? 1 : 0));
        o.appendByte(cast(char) code);
    }
}

// Parse a v5 shared-subscription filter "$share/<group>/<filter>". Returns true
// if the filter has the $share/ prefix (fills group+actual, or sets malformed on
// an empty/wildcard group or empty actual filter); false if it's a normal filter.
private bool mqttParseShare(scope const(char)[] f, out const(char)[] group,
        out const(char)[] actual, out bool malformed) @nogc nothrow
{
    malformed = false;
    enum string pfx = "$share/";
    if (f.length < pfx.length || f[0 .. pfx.length] != pfx)
        return false; // not a shared subscription
    auto rest = f[pfx.length .. $];
    size_t s = 0;
    while (s < rest.length && rest[s] != '/')
        s++;
    if (s == 0 || s >= rest.length)
    {
        malformed = true; // empty group, or no "/<filter>" after the group
        return true;
    }
    group = rest[0 .. s];
    actual = rest[s + 1 .. $];
    foreach (ch; group)
        if (ch == '+' || ch == '#')
        {
            malformed = true; // [MQTT-4.8.2] a share group has no wildcards
            return true;
        }
    if (actual.length == 0)
        malformed = true;
    return true;
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
            immutable okPair = (proto == "MQTT" && (level == 4 || level == 5))
                || (proto == "MQIsdp" && level == 3);
            if (okPair && level == 5)
                c.protoVer = 5;
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
            if ((flags & 0x40) && !(flags & 0x80))
                return false; // [MQTT-3.1.2-22] password flag requires username flag
            // keepalive seconds: enforce 1.5x as the read deadline [MQTT-3.1.2-24]
            if (i + 2 > p.length)
                return false; // truncated CONNECT: no keepalive field (a body
            // ending at/after the flags byte would otherwise read 1-2 bytes
            // past the packet — an unchecked OOB in a -release build, i.e. a
            // shard crash from one unauthenticated malformed CONNECT)
            immutable ka = cast(ushort)((p[i] << 8) | p[i + 1]);
            i += 2;
            c.readDeadline = ka == 0 ? Duration.max : (ka * 1500).msecs;
            // v5 CONNECT properties (session-expiry, receive-max, ...) follow the
            // keepalive; parsed-and-skipped for now.
            if (c.protoVer == 5 && !mqttSkipProps(p, i))
                return false;
            const(char)[] clientId;
            if (!rdStr(p, i, clientId))
                return false;
            // [MQTT-3.1.3-4/-9] the server MAY reject an unacceptable ClientId
            // with CONNACK 0x02. Cap the length (spec suggests 1-23): a huge
            // ClientId would otherwise be idup'd AND broadcast to every shard on
            // the takeover path — a per-CONNECT cross-shard amplification DoS.
            if (clientId.length > 256)
            {
                // v3 rc=2 identifier-rejected; v5 reason 0x85 client-id-not-valid
                mqttConnack(o, c.protoVer, false, c.protoVer == 5 ? 0x85 : 2);
                return false;
            }
            // [MQTT-3.1.3-8] empty ClientId REQUIRES CleanSession=1: refuse
            // (we run clean-only, but the refusal is still mandated)
            if (clientId.length == 0 && !(flags & 0x02))
            {
                mqttConnack(o, c.protoVer, false, c.protoVer == 5 ? 0x85 : 2);
                return false;
            }
            // will topic/message, then username/password per flags
            if (flags & 0x04)
            {
                // v5: will properties precede the will topic in the payload
                if (c.protoVer == 5 && !mqttSkipProps(p, i))
                    return false;
                const(char)[] wt, wm;
                if (!rdStr(p, i, wt) || !rdStr(p, i, wm))
                    return false;
                if (!mqttValidTopicName(wt))
                    return false;
                if (wt.length != 0 && wt[0] == '$')
                    return false; // [MQTT reserved] a client will can't target $SYS/*
                // stored for publish on abnormal disconnect; will-QoS (bits
                // 3-4) is delivered at QoS 0 like every other delivery, will
                // -retain (bit 5) is honored
                try
                {
                    c.willTopic = wt.idup;
                    c.willPayload = cast(const(ubyte)[]) wm.idup;
                }
                catch (Exception)
                {
                    c.willTopic = null;
                }
                c.willRetain = (flags & 0x20) != 0;
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
            // [MQTT-3.1.4-2] takeover: a non-empty clientId displaces any
            // existing session with the same id — locally now, and on the other
            // shards via a broadcast. connGen (global monotonic) makes the
            // newest win regardless of broadcast arrival order.
            if (okPair && clientId.length != 0)
            {
                immutable g = atomicOp!"+="(gMqttConnGen, 1);
                c.connGen = g;
                try
                    c.clientId = clientId.idup;
                catch (Exception)
                    c.clientId = null;
                if (c.clientId.length != 0)
                {
                    takeoverLocal(c.clientId, g);
                    try
                        gLocalClients[c.clientId] = c;
                    catch (Exception)
                    {
                    }
                    if (gMqttConnBcast !is null)
                        gMqttConnBcast(c.clientId, g);
                }
            }
            // CONNACK: session-present=0 (clean sessions only); reason/rc 0 on
            // success, else unacceptable-protocol-version
            mqttConnack(o, c.protoVer, false, okPair ? 0 : 1);
            return okPair;
        }
    case PT_PUBLISH:
        {
            immutable qos = (h >> 1) & 0x3;
            if (qos == 3)
                return false; // [MQTT-3.3.1-4] both QoS bits set: close
            immutable retain = (h & 1) != 0;
            immutable v5 = c.protoVer == 5;
            size_t i = 0;
            const(char)[] topic;
            if (!rdStr(p, i, topic))
                return false;
            // v3 validates the topic now; v5 defers — an aliased PUBLISH may
            // carry an EMPTY topic (resolved from the alias below).
            if (!v5 && !mqttValidTopicName(topic))
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
            const(char)[] props; // v5 forwardable properties (round-tripped to subs)
            if (v5)
            {
                PubProps pp;
                static ByteBuffer fwdProps; // TLS: consumed by deliver+fanout (no yield)
                if (!mqttParsePubProps(p, i, pp, fwdProps))
                    return false;
                props = cast(const(char)[]) fwdProps.data;
                if (pp.hasAlias)
                {
                    if (pp.topicAlias == 0 || pp.topicAlias > MQTT_TOPIC_ALIAS_MAX)
                        return false; // [MQTT-3.3.2-4] alias out of range
                    if (topic.length != 0)
                    {
                        // first use: register alias -> topic (validate the topic).
                        // Byte-capped: an idup'd topic is up to ~64KB; without a
                        // byte bound a client could pin MAX*64KB with distinct
                        // aliases (a per-conn RAM DoS). On overwrite, discount the
                        // old topic's bytes first.
                        if (!mqttValidTopicName(topic))
                            return false;
                        try
                        {
                            size_t oldLen = 0;
                            if (auto oa = pp.topicAlias in c.inAlias)
                                oldLen = (*oa).length;
                            if (c.inAliasBytes + topic.length - oldLen <= MQTT_MAX_ALIAS_BYTES)
                            {
                                c.inAlias[pp.topicAlias] = topic.idup;
                                c.inAliasBytes = c.inAliasBytes + topic.length - oldLen;
                            }
                            // else over budget: skip the mapping (a later resolve
                            // of this alias then closes the connection)
                        }
                        catch (Exception)
                        {
                        }
                    }
                    else
                    {
                        // later use: empty topic, resolve from the alias table
                        bool found = false;
                        try
                            if (auto t = pp.topicAlias in c.inAlias)
                            {
                                topic = *t;
                                found = true;
                            }
                        catch (Exception)
                        {
                        }
                        if (!found)
                            return false; // unknown alias with an empty topic
                    }
                }
                if (!mqttValidTopicName(topic))
                    return false; // the (resolved) topic must be valid
            }
            auto payload = cast(const(char)[]) p[i .. $];
            // [MQTT reserved] $-topics belong to the broker: a CLIENT publish to
            // one (e.g. spoofing $SYS/broker/messages) is dropped — not delivered,
            // fanned, or retained — but still acked so the client isn't confused.
            immutable clientReserved = topic.length != 0 && topic[0] == '$';
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
                    if (!clientReserved)
                    {
                        atomicOp!"+="(gMqttMessages, 1);
                        immutable rseq = retain ? atomicOp!"+="(gMqttRetainSeq, 1) : 0;
                        mqttDeliverLocal(topic, payload, retain, rseq, qos, c, props);
                        if (gMqttFanout !is null)
                            gMqttFanout(topic, payload, retain, rseq, qos, props);
                    }
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
            if (!clientReserved)
            {
                atomicOp!"+="(gMqttMessages, 1);
                immutable rseq = retain ? atomicOp!"+="(gMqttRetainSeq, 1) : 0;
                mqttDeliverLocal(topic, payload, retain, rseq, qos, c, props);
                if (gMqttFanout !is null)
                    gMqttFanout(topic, payload, retain, rseq, qos, props);
            }
            if (qos == 1)
            {
                o.appendByte(cast(char)(PT_PUBACK << 4));
                o.appendByte(cast(char) 2);
                o.appendByte(cast(char)(pid >> 8));
                o.appendByte(cast(char)(pid & 0xFF));
            }
            return true;
        }
    case PT_PUBACK:
        // the subscriber acking one of OUR QoS1 deliveries -> release the id
        if (p.length >= 2)
        {
            immutable pid = cast(ushort)((p[0] << 8) | p[1]);
            try
                c.inflight.remove(pid);
            catch (Exception)
            {
            }
        }
        return true;
    case PT_PUBREC:
        // phase 1 ack of one of OUR QoS2 deliveries -> advance the handshake and
        // send PUBREL [MQTT-4.3.3]. Lenient on an unknown pid (still PUBREL, so a
        // lost outQos2 entry can't strand the peer) but never close.
        if (p.length >= 2)
        {
            immutable pid = cast(ushort)((p[0] << 8) | p[1]);
            try
            {
                if ((pid in c.outQos2) !is null)
                    c.outQos2[pid] = 2; // PUBREL sent, awaiting PUBCOMP
            }
            catch (Exception)
            {
            }
            o.appendByte(cast(char)((PT_PUBREL << 4) | 0x02));
            o.appendByte(cast(char) 2);
            o.appendByte(cast(char)(pid >> 8));
            o.appendByte(cast(char)(pid & 0xFF));
        }
        return true;
    case PT_PUBCOMP:
        // the subscriber completing one of OUR QoS2 deliveries -> release the id
        if (p.length >= 2)
        {
            immutable pid = cast(ushort)((p[0] << 8) | p[1]);
            try
                c.outQos2.remove(pid);
            catch (Exception)
            {
            }
        }
        return true;
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
            immutable v5 = c.protoVer == 5;
            // v5 SUBSCRIBE properties (subscription-identifier, user-props) sit
            // after the packet-id; skipped now.
            if (v5 && !mqttSkipProps(p, i))
                return false;
            // NO-YIELD window: these TLS scratch arrays are filled and
            // consumed without any suspension point in between
            static ubyte[64] granted = void;
            size_t ng = 0;
            static const(char)[][64] filters;
            static bool[64] retainOk; // per-filter: send retained on subscribe?
            while (i < p.length)
            {
                if (ng >= granted.length)
                    return false; // >64 filters in one packet: refuse
                const(char)[] filter;
                if (!rdStr(p, i, filter) || i >= p.length)
                    return false;
                immutable optByte = p[i++];
                // v5 subscription options: bits 0-1 QoS, 2 no-local, 3 retain-as-
                // published, 4-5 retain-handling, 6-7 reserved (MUST be 0). v3:
                // the whole byte is the requested QoS. (no-local/RAP/retain-
                // handling honored incrementally.)
                if (v5 && (optByte & 0xC0))
                    return false; // [MQTT-3.8.3-4] reserved bits set
                immutable reqQos = cast(ubyte)(v5 ? (optByte & 0x03) : optByte);
                if (reqQos > 2)
                    return false; // [MQTT-3.8.3-4]
                // v5 shared subscription: $share/<group>/<actual-filter> — the
                // trie stores the ACTUAL filter, the entry carries the group.
                const(char)[] actualFilter = filter;
                const(char)[] shareGroup;
                if (v5)
                {
                    const(char)[] g, a;
                    bool malformed;
                    if (mqttParseShare(filter, g, a, malformed))
                    {
                        if (malformed)
                        {
                            granted[ng] = 0x80;
                            retainOk[ng] = false;
                            filters[ng++] = null;
                            continue;
                        }
                        actualFilter = a;
                        shareGroup = g;
                    }
                }
                if (!mqttValidFilter(actualFilter) || c.filters.length >= MQTT_MAX_SUBS)
                {
                    granted[ng] = 0x80; // unusable filter / sub-cap: failure
                    retainOk[ng] = false;
                    filters[ng++] = null;
                    continue;
                }
                // teardown-list entry FIRST (the ACTUAL filter, so disconnect
                // walks the right trie path): if this alloc fails we refuse the
                // filter rather than leak gMqttSubTotal on a trie-only entry
                const(char)[] fcopy;
                try
                {
                    fcopy = actualFilter.idup;
                    c.filters ~= fcopy;
                }
                catch (Exception)
                {
                    granted[ng] = 0x80;
                    retainOk[ng] = false;
                    filters[ng++] = null;
                    continue;
                }
                immutable grant = cast(ubyte) reqQos; // QoS0/1/2 out (reqQos already <=2)
                string sg;
                if (shareGroup.length)
                    try
                        sg = shareGroup.idup;
                    catch (Exception)
                    {
                    }
                immutable isNew = trieSubscribe(actualFilter, c, grant,
                        cast(ubyte)(v5 ? optByte : 0), sg);
                if (!isNew)
                    c.filters.length = c.filters.length - 1; // replaced: no new entry
                // retain-handling (opts bits 4-5): 0 = always send retained on
                // subscribe, 1 = only if new, 2 = never. A SHARED subscription
                // never gets retained-on-subscribe [MQTT-4.8.2].
                immutable rh = v5 ? ((optByte >> 4) & 0x03) : 0;
                retainOk[ng] = shareGroup.length == 0 && (rh == 0 || (rh == 1 && isNew));
                filters[ng] = actualFilter;
                granted[ng++] = grant;
            }
            if (ng == 0)
                return false; // [MQTT-3.8.3-3] at least one filter required
            o.appendByte(cast(char)(PT_SUBACK << 4));
            encodeVarint(o, cast(uint)(2 + (v5 ? 1 : 0) + ng));
            o.appendByte(cast(char)(pid >> 8));
            o.appendByte(cast(char)(pid & 0xFF));
            if (v5)
                o.appendByte(0); // v5 property length 0 (before the reason codes)
            foreach (g; granted[0 .. ng])
                o.appendByte(cast(char) g); // grant/reason: QoS 0/1/2 or 0x80 failure
            // retained messages matching the new filters, delivered after SUBACK
            try
                foreach (topic, r; gRetained)
                {
                    if (r.payload.length == 0)
                        continue; // tombstone (deleted retained): never delivered
                    if (o.length > MQTT_OBOX_CAP)
                        break; // bounded replay burst (retained is QoS0/best-effort)
                    foreach (fi, f; filters[0 .. ng])
                        if (f !is null && retainOk[fi] && mqttFilterMatches(f, topic))
                        {
                            buildPublish(o, topic, r.payload, true, v5);
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
            immutable v5 = c.protoVer == 5;
            if (v5 && !mqttSkipProps(p, i)) // v5 UNSUBSCRIBE properties (user-props)
                return false;
            size_t nf = 0;
            while (i < p.length)
            {
                const(char)[] filter;
                if (!rdStr(p, i, filter))
                    return false;
                nf++;
                // a $share/<group>/<filter> unsubscribe removes only THAT shared
                // subscription; a normal filter removes only the normal one
                // (group ""). Both are group-specific (matchGroup=true).
                const(char)[] actualFilter = filter;
                string group;
                if (v5)
                {
                    const(char)[] g, a;
                    bool malformed;
                    if (mqttParseShare(filter, g, a, malformed))
                    {
                        if (malformed)
                            continue; // ack it, ignore
                        actualFilter = a;
                        group = cast(string) g;
                    }
                }
                if (!mqttValidFilter(actualFilter))
                    continue; // ack it, but never trie-walk a malformed filter
                trieUnsubscribe(actualFilter, c, true, group);
                // remove ONE matching teardown record (the conn may still hold
                // other subscriptions — different group — at the same filter)
                foreach (idx, f; c.filters)
                    if (f == actualFilter)
                    {
                        c.filters[idx] = c.filters[$ - 1];
                        c.filters.length = c.filters.length - 1;
                        break;
                    }
            }
            if (nf == 0)
                return false; // [MQTT-3.10.3-2]
            o.appendByte(cast(char)(PT_UNSUBACK << 4));
            if (v5)
            {
                // v5 UNSUBACK: property block + one reason code per filter
                encodeVarint(o, cast(uint)(2 + 1 + nf));
                o.appendByte(cast(char)(pid >> 8));
                o.appendByte(cast(char)(pid & 0xFF));
                o.appendByte(0); // property length 0
                foreach (_; 0 .. nf)
                    o.appendByte(0); // 0x00 = success
            }
            else
            {
                o.appendByte(cast(char) 2);
                o.appendByte(cast(char)(pid >> 8));
                o.appendByte(cast(char)(pid & 0xFF));
            }
            return true;
        }
    case PT_PINGREQ:
        if (fl != 0)
            return false;
        o.appendByte(cast(char)(PT_PINGRESP << 4));
        o.appendByte(cast(char) 0);
        return true;
    case PT_DISCONNECT:
        // [MQTT-3.14.4-3] a clean DISCONNECT drops the will. v5 adds a reason
        // code: 0x04 = "disconnect with will message" KEEPS it (teardown fires
        // it); any other reason (and all of v3) clears it.
        if (!(c.protoVer == 5 && p.length >= 1 && p[0] == 0x04))
        {
            c.willTopic = null;
            c.willPayload = null;
        }
        return false;
    default:
        // types 0 and 15 are reserved; CONNACK/SUBACK/UNSUBACK/PINGRESP are
        // server->client only and never legitimate inbound. PUBACK/PUBREC/
        // PUBCOMP ARE legitimate now that we deliver QoS1/2 out (own cases
        // above). This default also replaces a final-switch that turned any of
        // them into a runtime SwitchError — one PUBREL used to crash the server.
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
