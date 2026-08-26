module dreads.mqtt;

import dreads.tls;
import dreads.ws;
import vibe.core.stream : IOMode;

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

import core.atomic : atomicLoad, atomicStore, atomicOp, MemoryOrder;

import core.time : Duration, msecs, seconds, MonoTime, dur;

import vibe.core.core : runTask, Task;
import vibe.core.net : TCPConnection;
import vibe.core.sync : LocalManualEvent, TaskMutex, createManualEvent;

import dreads.mem : ByteBuffer;
import dreads.acl : AclUser, aclUser, aclCheckPassword, aclCanAccessChannel;
import dreads.shard : tShard, ShardMsg;

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
/// Cross-shard parked-session pool. A persistent session parked offline on its
/// shard is ALSO registered here (keyed by clientId) so a reconnect that
/// SO_REUSEPORT hashed onto a DIFFERENT shard can still find and resume it. The
/// per-shard gLocalClients stays the fast same-shard path; this global is the
/// only shared-mutable MQTT state, guarded by a SPINLOCK (not a mutex, which would
/// sleep a thread and break the thread-per-core no-blocking model). The critical
/// section is a single AA op — a reconnect grabs the lock only to CLAIM the parked
/// session (microseconds), never during the state copy, so contention is nil.
private __gshared MqttConn[string] gParkedPool;
private shared bool gParkedLock;

private void cpuPause() @trusted nothrow @nogc
{
    version (D_InlineAsm_X86_64)
        asm nothrow @nogc { rep; nop; } // PAUSE — spin-wait hint
    else version (D_InlineAsm_X86)
        asm nothrow @nogc { rep; nop; }
}

private void parkedLock() @trusted nothrow @nogc
{
    import core.atomic : cas;

    while (!cas(&gParkedLock, false, true))
        cpuPause();
}

private void parkedUnlock() @trusted nothrow @nogc
{
    import core.atomic : atomicStore;

    atomicStore!(MemoryOrder.rel)(gParkedLock, false);
}

/// Register a parked session in the cross-shard pool (so a reconnect on another
/// shard can find it). Locks gParkedMutex briefly; the key is the idup'd clientId.
private void parkedPoolPut(MqttConn c) nothrow @trusted
{
    if (c.clientId.length == 0)
        return;
    parkedLock();
    scope (exit)
        parkedUnlock();
    try
        gParkedPool[c.clientId] = c;
    catch (Exception)
    {
    }
}

/// Remove a parked session from the cross-shard pool — but only if this exact conn
/// is still the registered one (a newer session for the same id may have replaced
/// it). Called when the parked session ends or is resumed.
private void parkedPoolDel(MqttConn c) nothrow @trusted
{
    if (c.clientId.length == 0)
        return;
    parkedLock();
    scope (exit)
        parkedUnlock();
    try
        if (auto pc = c.clientId in gParkedPool)
            if (*pc is c)
                gParkedPool.remove(c.clientId);
    catch (Exception)
    {
    }
}
/// Broadcast a (clientId, connGen) takeover to every OTHER shard (installed by
/// server.d). Null when unsharded.
public __gshared void delegate(scope const(char)[] clientId, ulong gen) nothrow gMqttConnBcast;
/// Ask shard `dstShard` to FREEZE the parked session for `clientId` (cross-shard
/// reconnect handshake). Installed by server.d as a ShardMsg.mqttResume enqueue;
/// null when unsharded. See mqttResumeSignal (the receiving side).
public __gshared void delegate(uint dstShard, scope const(char)[] clientId) nothrow gMqttResume;

/// Owner-shard side of the cross-shard reconnect handshake: the drain calls this
/// when a ShardMsg.mqttResume arrives. If we hold the parked session for this id,
/// flag it to freeze and wake its parked fiber (same thread => LocalManualEvent
/// works). The fiber then sets `redirect`+`frozen` so the reconnecting shard can
/// adopt the now-stable state.
public void mqttResumeSignal(scope const(char)[] clientId) nothrow @trusted
{
    try
        if (auto pc = clientId in gLocalClients)
        {
            auto s = *pc;
            if (s !is null && s.offline)
            {
                s.freezeReq = true;
                try
                    s.reconnectEvt.emit();
                catch (Exception)
                {
                }
            }
        }
    catch (Exception)
    {
    }
}

/// Deadline for a freshly-accepted socket to complete CONNECT. Without it a
/// client that opens TCP and never speaks pins a serve fiber + writer fiber +
/// MqttConn forever (unauthenticated pre-handshake slowloris).
private enum Duration MQTT_CONNECT_TIMEOUT = 30.seconds;

/// One MQTT connection (fiber-owned). The write mutex serializes deliveries
/// from publisher fibers on the same thread with the conn's own replies.
/// Full subscription record kept per-connection (parallel to MqttConn.filters)
/// so a persistent-session resume can restore the exact subscriptions.
private struct SubInfo
{
    ubyte qos;
    ubyte opts;
    string shareGroup;
    uint subId;
}

public final class MqttConn
{
    dreads.tls.TlsLeg* tlsLeg; // null = plaintext (every existing path untouched)
    dreads.ws.WsCodec* wsCodec; // null = raw TCP; non-null = MQTT-over-WebSocket
    TCPConnection tcp;
    TaskMutex wlock;
    bool connected; // CONNECT seen and CONNACKed
    uint gen; // bumped on disconnect: stale trie entries self-invalidate
    // ACL: the authenticated user (CONNECT username -> ACL user, `default` when
    // absent). SUBSCRIBE/PUBLISH topics are authorized as ACL CHANNELS against it
    // (a denied topic -> SUBACK failure / dropped publish) so MQTT clients obey
    // the same channel ACL as Redis pub/sub — including `&!pattern` topic denies.
    const(AclUser)* aclUser;
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
    // Persistent-session offline-hold: on a persistent client's disconnect the
    // MqttConn is NOT torn down — it stays in gLocalClients + the trie, marked
    // offline, its writer stopped, and deliveries accumulate in obox (capped) as
    // a fixed offline queue. A same-thread reconnect migrates the session; the
    // maintenance sweep reaps it at offlineDeadline. (SO_REUSEPORT cross-thread
    // reconnect is the deliberately-unhandled ostrich case.)
    bool offline; // wrapper state: socket dead/disconnected, session held ALIVE
    MonoTime offlineDeadline; // (legacy; park computes its own deadlines)
    // Transparent-strategy: on socket death the serve fiber does NOT return — it
    // parks (mqttParkOrEnd), holding the whole session ALIVE (subs in the trie,
    // obox as the offline queue), and vibe never learns the socket died. A
    // same-shard reconnect MIGRATES this session onto the new connection and wakes
    // reconnectEvt to end the parked fiber (the socket is never moved between
    // conns — vibe forbids that under a pending read). Expiry is a timed wait (no
    // separate Timer); will-delay is orchestrated in the same wait loop.
    LocalManualEvent reconnectEvt;
    bool discard; // set to make a parked session END now (migrated away / clean_start)
    // Cross-shard reconnect handshake: a reconnect on ANOTHER shard asks this
    // session's owner shard (shardId) to FREEZE it (drop subs so obox stops
    // growing) + become a redirect, then adopts the frozen state. `redirect` is
    // the atomic hot-path flag (a delivery to a redirect conn is re-published so
    // it reaches the session's new shard, instead of piling in a dead obox).
    uint shardId; // the shard that owns this conn (set at accept)
    bool freezeReq; // owner-shard fiber: a cross-shard reconnect asked us to freeze
    shared bool frozen; // set by the owner once frozen — the reconnecting shard waits on it
    shared bool redirect; // hot-path flag: this parked conn is now a short-lived redirect
    ubyte discReason = 0x82; // v5 server-DISCONNECT reason on a protocol-error close
    // (default 0x82 Protocol Error; a handler may set e.g. 0x93 Receive Maximum)
    uint willDelay; // v5 will-delay-interval (seconds): 0 = fire will immediately on drop
    // Publication-expiry: while offline, a queued PUBLISH carrying a message-
    // expiry-interval records {its start offset in obox, absolute deadline}. On
    // reconnect the flush drops the expired ones and rewrites the survivors'
    // interval to the time remaining [MQTT-3.3.2-5].
    OExpiry[] oExprQ;
    // Outbound flow control: QoS1/2 deliveries that don't fit the send window
    // (receive-maximum) are HELD here in FIFO order instead of degrading to QoS0,
    // and released as PUBACK/PUBCOMP free window slots. Bounded by MQTT_OBOX_CAP
    // (heldBytes) so a stalled consumer can't grow memory without limit.
    HeldPub[] heldQ;
    size_t heldBytes;
    ubyte protoVer = 4; // MQTT protocol level: 4 = 3.1.1, 5 = 5.0 (v5 packets
    // carry a property block; v5 CONNACK/SUBACK use reason codes)
    // v5 inbound topic aliases: a client maps a small int -> topic to save bytes;
    // first PUBLISH with an alias carries the topic + alias (registers it), later
    // ones carry an empty topic + the alias (resolved here). Bounded by the
    // topic-alias-maximum we advertise in CONNACK.
    string[ushort] inAlias;
    size_t inAliasBytes; // running size of the aliased topics (byte cap)
    // v5 OUTBOUND topic aliases (broker -> client): if the client advertised a
    // topic-alias-maximum, we map a topic to a short alias so repeat deliveries
    // ship an empty topic + the alias. outAliasMax = how many we may use (capped);
    // outAliasNext = the next alias to hand out (1-based); outAlias = topic->alias
    // registered ON THE CLIENT; outAliasBytes = byte cap on the table. An alias is
    // recorded ONLY after the registering PUBLISH is actually queued, so a dropped
    // registration never leaves the client unable to resolve a later reuse.
    ushort outAliasMax;
    ushort outAliasNext = 1;
    ushort[string] outAlias;
    size_t outAliasBytes;
    uint maxPktSize; // v5 maximum-packet-size the CLIENT advertised: we MUST NOT
    // send a PUBLISH larger than this to it [MQTT-3.1.2-24 area]. 0 = no limit.
    ushort sendMax = 0xFFFF; // v5 receive-maximum the CLIENT advertised: the max
    // QoS1/2 messages we may have in flight toward it (flow control). Default =
    // no limit beyond our own window.
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
    SubInfo[] subInfo; // parallel to `filters`: the granted qos/opts/share/subId,
    // so a persistent-session resume can re-subscribe them on the new connection
    // Client identity for [MQTT-3.1.4-2] takeover: a new CONNECT with the same
    // (non-empty) clientId must disconnect the existing session. connGen is a
    // global monotonic stamp so the NEWEST connection wins regardless of the
    // order the cross-shard takeover broadcasts arrive (the retained-seq lesson).
    string clientId;
    ulong connGen;
    bool cleanStart; // CONNECT clean-start/clean-session flag (flags & 0x02)
    uint sessionExpiry; // v5 session-expiry-interval (seconds; uint.max = never)
    // Last Will and Testament ([MQTT-3.1.2-8]): published if the connection
    // drops abnormally (TCP death, takeover, protocol error) but NOT on a clean
    // DISCONNECT, which clears it. willTopic empty = no will.
    string willTopic;
    const(ubyte)[] willPayload;
    bool willRetain;
    const(char)[] willProps; // v5 forwardable will properties (will-delay-interval
    // stripped): emitted on the will PUBLISH so content-type/user-props survive.
    // QoS1/2 OUTBOUND delivery: a per-conn packet-id (1..65535, wraps), the set
    // of QoS1 ids delivered but not yet PUBACKed, and the QoS2 ids mid-handshake
    // (outQos2[pid]: 1 = awaiting PUBREC, 2 = PUBREL sent, awaiting PUBCOMP). One
    // shared pid space; the combined window bounds RAM -> a slow consumer degrades
    // the delivery to QoS0 rather than growing memory. In-session only: no
    // cross-reconnect retransmit (needs persistent sessions).
    ushort nextPid = 1;
    bool[ushort] inflight;
    ubyte[ushort] outQos2;
    // Persistent sessions (session-expiry > 0) keep the full unacked QoS1/2
    // PUBLISH bytes per packet-id, so a reconnect redelivers them with DUP=1
    // [MQTT-4.4.0-1]. Cleared on PUBACK/PUBCOMP; empty for clean sessions.
    const(char)[][ushort] inflightMsg;

    this(TCPConnection c) nothrow
    {
        tcp = c;
        try
        {
            wlock = new TaskMutex;
            flushEvt = createManualEvent();
            reconnectEvt = createManualEvent();
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
/// Pre-CONNECT cap: a CONNECT is small, so refuse to buffer megabytes for an
/// unauthenticated peer (16MB/conn held for CONNECT_TIMEOUT with no aggregate
/// budget is a cheap amplification). Raised to MQTT_MAX_PACKET once connected.
private enum uint MQTT_PRECONNECT_MAX = 256 << 10;
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
// A retained TOMBSTONE (empty-payload) exists only to gate a concurrent stale SET
// that a reordered SPSC lane delivers AFTER the delete — a sub-second window. It
// gets a generous TTL so mqttExpireRetained reaps it: without one, an empty-retain
// flood to distinct never-set topics permanently fills the topic-count cap and the
// broker refuses all future retained SETs, never self-healing. 60s >> lane latency.
private enum long MQTT_TOMBSTONE_TTL_S = 60;
private enum size_t MQTT_MAX_RETAINED_BYTES = 256 << 20;
private size_t tRetainedBytes; // TLS: payload bytes currently in gRetained
/// Per-connection subscription cap: past this SUBSCRIBE gets SUBACK 0x80
/// (unlimited re-subscribe was a memory + delivery-amplification DoS).
private enum size_t MQTT_MAX_SUBS = 4096;
/// v5 topic-alias-maximum we advertise: the largest alias a client may use when
/// publishing to us (also bounds the per-conn inbound alias table by COUNT).
private enum ushort MQTT_TOPIC_ALIAS_MAX = 1024;
private enum ushort MQTT_SERVER_KEEPALIVE = 60; // cap the client's keep-alive at
// 60s and tell it via CONNACK ServerKeepAlive (0x13) so idle clients keep pinging
/// v5 receive-maximum WE advertise (CONNACK 0x21): the max concurrent unacked
/// QoS2 the client may have in flight TOWARD us. Exceeding it is a Receive
/// Maximum exceeded protocol error -> DISCONNECT 0x93 [MQTT-4.9]. 20 matches the
/// common broker default (mosquitto), keeping legitimate bursts comfortable.
private enum ushort MQTT_SERVER_RECEIVE_MAX = 20;
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
    uint subId; // v5 subscription-identifier (0x0B): echoed on every PUBLISH this
    // subscription delivers, so the client can correlate. 0 = none.
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
    const(char)[] props; // v5 forwardable props with message-expiry (0x02) STRIPPED
    // out (re-emitted decremented on replay per [MQTT-3.3.2-5]); null for v3/no-props
    MonoTime deadline; // when this retained message expires; valid iff hasExpiry
    bool hasExpiry; // v5 message-expiry-interval was present: evict + decrement
    ubyte qos; // original publish QoS: retained-on-subscribe delivers at
    // min(this, the subscription's granted QoS) [MQTT-3.3.1-9 / 3.8.4]
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
        string shareGroup = null, uint subId = 0) @trusted nothrow
{
    // identity is (conn, gen, shareGroup): a normal sub and a shared sub (or two
    // different groups) to the same filter are DISTINCT subscriptions.
    foreach (ref e; a)
        if (e.c is c && e.gen == c.gen && e.shareGroup == shareGroup)
        {
            e.qos = qos;
            e.opts = opts;
            e.subId = subId; // a re-subscribe updates the identifier
            return false;
        }
    try
        a ~= SubEntry(c, c.gen, qos, opts, shareGroup, subId);
    catch (Exception)
        return false;
    atomicOp!"+="(gMqttSubTotal, 1);
    return true;
}

// Split-walk `filter` creating nodes, then upsert the entry at the terminal.
// Returns true when a NEW subscription was created (false = replaced/failed).
private bool trieSubscribe(scope const(char)[] filter, MqttConn c, ubyte qos,
        ubyte opts = 0, string shareGroup = null, uint subId = 0) @trusted nothrow
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
                return upsertEntry(n.hash, c, qos, opts, shareGroup, subId);
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
                return upsertEntry(next.subs, c, qos, opts, shareGroup, subId);
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
    uint subId; // v5 subscription-identifier to echo on delivery (0 = none)
}

private struct OExpiry // a queued offline PUBLISH with a message-expiry-interval
{
    size_t start; // byte offset of the PUBLISH within the conn's obox
    MonoTime deadline; // absolute time the message expires (queue time + interval)
}

private struct HeldPub // a QoS1/2 delivery held behind a full send-window (flow control)
{
    const(char)[] topic;
    const(char)[] payload;
    const(char)[] props; // the forwarded v5 property block (no alias/subid prefix)
    ubyte qos;
    bool retain;
}

private Match[] tMatchBuf;
private size_t tMatchLen;
// TLS scratch: the subscription-identifiers of ALL overlapping subscriptions a
// single client has for one delivery, coalesced into ONE PUBLISH [MQTT-3.3.4-4].
private uint[] tCoSubIds;

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
    // An OFFLINE persistent session has a closed socket but a live obox queue:
    // include it (deliver into the queue). Otherwise require a live socket.
    if (!e.c.offline)
    {
        try
            if (!e.c.tcp.connected)
                return;
        catch (Exception)
            return;
    }
    if (tMatchLen >= tMatchBuf.length)
    {
        try
            tMatchBuf.length = tMatchBuf.length ? tMatchBuf.length * 2 : 64;
        catch (Exception)
            return;
    }
    tMatchBuf[tMatchLen++] = Match(e.c, e.qos, (e.opts & 0x04) != 0,
            (e.opts & 0x08) != 0, e.shareGroup, e.subId);
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
/// True if the topic filter contains an MQTT wildcard ('+' or '#'). Used to pick
/// the ACL channel-match mode: a wildcard filter matches an ACL pattern exactly,
/// a literal one is glob-matched (so `&!literal` topic denies apply).
private bool mqttHasWildcard(scope const(char)[] f) @nogc nothrow @safe
{
    foreach (ch; f)
        if (ch == '+' || ch == '#')
            return true;
    return false;
}

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

/// Generic RESP-over-data-plane exec (installed by server.d = amqpDataExec):
/// hops to the owning shard, so persistent-session state survives in the Redis
/// keyspace and is reachable from whichever thread a client reconnects on
/// (SO_REUSEPORT spreads reconnects across shards). Cross-shard hops YIELD.
public __gshared void delegate(scope const(char)[][] args, ref ByteBuffer reply) nothrow gMqttExec;

// A persistent MQTT session lives in the key `mqtt.sess.<clientId>` with a Redis
// TTL = session-expiry-interval, so expiry is handled by the keyspace itself. A
// clean_start=1 connect discards it; a clean_start=0 connect that finds it live
// resumes (CONNACK session-present=1). [MQTT-3.1.2-4..6 / 3.1.2-23]
private enum string MQTT_SESS_PREFIX = "mqtt.sess.";

private void sessKey(scope const(char)[] clientId, ref ByteBuffer kb) nothrow @trusted
{
    kb.clear();
    kb.append(MQTT_SESS_PREFIX);
    kb.append(clientId);
}

/// Does a live persistent session exist for this client id? (EXISTS -> :1/:0)
private bool mqttSessionExists(scope const(char)[] clientId) nothrow @trusted
{
    if (gMqttExec is null || clientId.length == 0)
        return false;
    static ByteBuffer kb, rb;
    sessKey(clientId, kb);
    const(char)[][2] a = ["exists", cast(const(char)[]) kb.data];
    gMqttExec(a[], rb);
    auto d = rb.data;
    return d.length >= 2 && d[0] == ':' && d[1] == '1';
}

/// Persist the session with a TTL (seconds); expiry==uint.max means "never
/// expire" -> no TTL. expiry==0 means discard (handled by the caller via del).
private void mqttSessionPut(scope const(char)[] clientId, uint expiry) nothrow @trusted
{
    import core.stdc.stdio : snprintf;

    if (gMqttExec is null || clientId.length == 0)
        return;
    static ByteBuffer kb, rb;
    sessKey(clientId, kb);
    if (expiry == uint.max)
    {
        const(char)[][3] a = ["set", cast(const(char)[]) kb.data, "1"];
        gMqttExec(a[], rb);
    }
    else
    {
        char[16] eb = void;
        immutable n = snprintf(eb.ptr, eb.length, "%u", expiry);
        const(char)[][5] a = ["set", cast(const(char)[]) kb.data, "1", "EX",
            cast(const(char)[]) eb[0 .. (n > 0 ? n : 0)]];
        gMqttExec(a[], rb);
    }
}

/// Discard the persistent session (clean_start, or session-expiry-interval 0).
private void mqttSessionDel(scope const(char)[] clientId) nothrow @trusted
{
    if (gMqttExec is null || clientId.length == 0)
        return;
    static ByteBuffer kb, rb;
    sessKey(clientId, kb);
    const(char)[][2] a = ["del", cast(const(char)[]) kb.data];
    gMqttExec(a[], rb);
}
public shared ulong gMqttMessages; // total publishes routed (INFO/debug)
public shared long gMqttClientsConnected; // currently-connected clients ($SYS)
public shared ulong gMqttSent; // messages delivered to subscribers ($SYS)
public shared ulong gMqttBytesRx; // MQTT bytes received ($SYS, opt-in)
public shared ulong gMqttBytesTx; // MQTT bytes sent ($SYS, opt-in)
// Byte accounting is OFF by default: it adds an atomic per packet in/out on the
// hot path. Enable with `mqtt-sys-bytes yes` to get $SYS/broker/bytes/*.
public __gshared bool gMqttSysBytes;
/// Broker start time (ms) for $SYS/broker/uptime; stamped on the first $SYS tick.
private shared ulong gMqttStartMs;

/// Seed the broker start clock at boot so $SYS uptime is accurate from the
/// first tick (else the first publish shows 0). Idempotent.
public void mqttSeedStart() nothrow @trusted
{
    import core.atomic : atomicLoad, atomicStore, MemoryOrder;
    import dreads.stream : nowMs;

    if (atomicLoad!(MemoryOrder.raw)(gMqttStartMs) == 0)
        atomicStore!(MemoryOrder.raw)(gMqttStartMs, nowMs());
}

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
    immutable up = (now - atomicLoad!(MemoryOrder.raw)(gMqttStartMs)) / 1000;
    static char[40] ub2 = void;
    immutable un = snprintf(ub2.ptr, ub2.length, "%llu seconds", up);
    pub("$SYS/broker/uptime", un > 0 ? cast(const(char)[]) ub2[0 .. un] : "0 seconds");
    immutable cc = atomicLoad!(MemoryOrder.raw)(gMqttClientsConnected);
    num(nb, cc < 0 ? 0 : cast(ulong) cc, v);
    pub("$SYS/broker/clients/connected", v);
    pub("$SYS/broker/clients/total", v); // no offline-session registry: == connected
    num(nb, cast(ulong) atomicLoad!(MemoryOrder.raw)(gMqttMessages), v);
    pub("$SYS/broker/messages/received", v);
    num(nb, cast(ulong) atomicLoad!(MemoryOrder.raw)(gMqttSent), v);
    pub("$SYS/broker/messages/sent", v);
    immutable st = atomicLoad!(MemoryOrder.raw)(gMqttSubTotal);
    num(nb, st < 0 ? 0 : cast(ulong) st, v);
    pub("$SYS/broker/subscriptions/count", v);
    num(nb, cast(ulong) atomicLoad!(MemoryOrder.raw)(gMqttDropped), v);
    pub("$SYS/broker/messages/dropped", v);
    if (gMqttSysBytes)
    {
        num(nb, cast(ulong) atomicLoad!(MemoryOrder.raw)(gMqttBytesRx), v);
        pub("$SYS/broker/bytes/received", v);
        num(nb, cast(ulong) atomicLoad!(MemoryOrder.raw)(gMqttBytesTx), v);
        pub("$SYS/broker/bytes/sent", v);
    }
    // deliver the $SYS batch (mqttDeliverLocal queued into subscriber outboxes)
    mqttFlushDirty();
}

/// Maintenance sweep (per-shard tick): drop every retained message past its v5
/// message-expiry deadline, so expired-but-unscanned entries can't pin the
/// retained byte/topic caps between SUBSCRIBEs (the SUBSCRIBE-time eviction is
/// opportunistic and capped; this is the hard bound). gRetained is TLS, so each
/// shard reaps its own replica. Allocates only when something is actually
/// expired (the `expired` list stays null on the common no-op path).
public void mqttExpireRetained() @trusted nothrow
{
    if (gRetained.length == 0)
        return;
    immutable now = MonoTime.currTime;
    const(char)[][] expired; // local: no static buffer to pin idup'd keys
    try
    {
        foreach (topic, ref r; gRetained)
            if (r.hasExpiry && now >= r.deadline)
                expired ~= topic; // collect; never mutate the AA mid-foreach
        foreach (t; expired)
            if (auto rr = t in gRetained)
            {
                tRetainedBytes -= rr.payload.length + rr.props.length;
                gRetained.remove(cast(string) t);
            }
    }
    catch (Exception)
    {
    }
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
                // v5: tell the displaced client WHY (Session taken over, 0x8E)
                // before we close its socket, instead of a bare TCP reset. Only for
                // an ONLINE v5 session (an offline/parked one has no live socket).
                if (victim.connected && victim.protoVer == 5 && !victim.offline)
                {
                    static ByteBuffer db; // TLS: consumed synchronously by sendTo
                    db.clear();
                    mqttServerDisconnect(db, 0x8E); // Session taken over
                    sendTo(victim, db.data);
                }
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
                // both payload AND stored props count against the byte cap (the
                // byte-cap lesson: a topic-count cap over variable props is no RAM
                // bound); the old entry's props are discounted on replace.
                immutable oldLen = old.payload.length + old.props.length;
                if (payload.length == 0)
                {
                    // tombstone (delete): TTL'd so the sweep reaps it (see above)
                    gRetained[cast(string) topic] = Retained(null, seq, null,
                            MonoTime.currTime + dur!"seconds"(MQTT_TOMBSTONE_TTL_S), true);
                    tRetainedBytes -= oldLen;
                }
                else
                {
                    Retained r = makeRetained(payload, seq, props, pubQos);
                    immutable newLen = r.payload.length + r.props.length;
                    if (tRetainedBytes - oldLen + newLen <= MQTT_MAX_RETAINED_BYTES)
                    {
                        gRetained[cast(string) topic] = r;
                        tRetainedBytes += newLen - oldLen;
                    }
                    else
                        atomicOp!"+="(gMqttRetainedDropped, 1);
                }
            }
            else // new topic
            {
                if (payload.length == 0)
                {
                    // tombstone on an absent topic: seq-stamped, topic-count-capped,
                    // and TTL'd so an empty-retain flood to distinct topics can't
                    // permanently pin the cap (the sweep reaps it after the TTL)
                    if (gRetained.length < MQTT_MAX_RETAINED_TOPICS)
                        gRetained[topic.idup] = Retained(null, seq, null,
                                MonoTime.currTime + dur!"seconds"(MQTT_TOMBSTONE_TTL_S), true);
                    else
                        atomicOp!"+="(gMqttRetainedDropped, 1);
                }
                else
                {
                    Retained r = makeRetained(payload, seq, props, pubQos);
                    immutable newLen = r.payload.length + r.props.length;
                    if (gRetained.length < MQTT_MAX_RETAINED_TOPICS
                            && tRetainedBytes + newLen <= MQTT_MAX_RETAINED_BYTES)
                    {
                        gRetained[topic.idup] = r;
                        tRetainedBytes += newLen;
                    }
                    else
                        atomicOp!"+="(gMqttRetainedDropped, 1);
                }
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
    {
        import core.atomic : atomicOp;

        atomicOp!"+="(gMqttSent, cast(ulong) tMatchLen); // $SYS messages/sent
    }
    // QoS0 packet built ONCE and shared (the hot path); QoS1 subscribers get a
    // per-conn packet with their own packet-id (a separate, slower branch that
    // does NOT touch the QoS0 fast path).
    static ByteBuffer pkt; // TLS: the shared QoS0 publish (v3)
    static ByteBuffer pktV5; // TLS: the shared QoS0 publish (v5, empty props)
    static ByteBuffer q1; // TLS: a per-subscriber QoS1 publish
    pkt.clear();
    buildPublish(pkt, topic, payload, false);
    bool pktV5built = false; // build the v5 variant lazily (only if a v5 sub matches)
    // The coalesced subscription-identifiers for the CURRENT delivery: when a
    // client has several overlapping subscriptions matching this topic, all of
    // their ids ride ONE PUBLISH (deliverTo reads this; empty => use m.subId).
    const(uint)[] coSubIds = null;
    // Deliver one message to one matched subscriber (QoS/no-local/RAP-aware).
    // Returns TRUE iff the message was actually queued to the subscriber — a
    // shared-subscription round-robin uses this to FALL THROUGH to the next group
    // member when the chosen one is ineligible (no-local-publisher, obox full, or
    // over maxPktSize), so a message is never lost for the whole group.
    bool deliverTo(ref Match m) @trusted nothrow
    {
        import core.builtins : expect;

        auto s = m.c;
        if (s.closed)
            return false;
        // Cross-shard resume: a session FROZEN for a reconnect on another shard
        // is a short-lived redirect — its obox is being adopted there, so don't
        // pile into it. `expect(..., false)`: never true on the delivery hot path
        // except during the ~1s redirect window of a rare cross-shard reconnect.
        if (expect(atomicLoad!(MemoryOrder.acq)(s.redirect), false))
            return true; // dropped here; it reaches the session via its new shard
        if (m.noLocal && s is publisher)
            return false; // v5 no-local: don't echo the publisher's own message back
        immutable v5 = s.protoVer == 5;
        // Publication-expiry: a message queued for an OFFLINE session that carries
        // a message-expiry-interval is tracked (start offset + deadline) so the
        // reconnect flush can drop it if expired / decrement it [MQTT-3.3.2-5].
        immutable size_t oStart = s.obox.length;
        immutable uint expE = (s.offline && v5) ? msgExpiryFromProps(props) : 0;
        void recordExpiry() nothrow
        {
            if (expE != 0 && s.obox.length > oStart)
                try
                    s.oExprQ ~= OExpiry(oStart, MonoTime.currTime + dur!"seconds"(expE));
                catch (Exception)
                {
                }
        }
        scope (exit)
            recordExpiry();
        // v5 outbound topic alias: map this topic to a short alias for a client
        // that advertised topic-alias-maximum. First delivery of a topic REGISTERS
        // it (full topic + alias); later ones REUSE (empty topic + alias). The
        // mapping is recorded only after a successful queue (recordAlias), so a
        // dropped registration never strands the client on an unknown alias.
        const(char)[] outTopic = topic;
        ushort aliasVal = 0;
        bool aliasRegister = false;
        if (v5 && s.outAliasMax != 0)
        {
            try
            {
                if (auto pa = topic in s.outAlias)
                {
                    aliasVal = *pa;
                    outTopic = null; // reuse: the alias resolves the empty topic
                }
                else if (s.outAliasNext <= s.outAliasMax
                        && s.outAliasBytes + topic.length <= MQTT_MAX_ALIAS_BYTES)
                {
                    aliasVal = s.outAliasNext; // register a new alias for this topic
                    aliasRegister = true;
                }
            }
            catch (Exception)
            {
            }
        }
        void recordAlias()
        {
            if (!aliasRegister)
                return;
            try
            {
                s.outAlias[topic.idup] = aliasVal;
                s.outAliasBytes += topic.length;
            }
            catch (Exception)
            {
            }
            if (s.outAliasNext < ushort.max)
                s.outAliasNext++;
        }
        // v5 property prefix for THIS subscriber: topic-alias (0x23) then
        // subscription-identifier (0x0B), ahead of the forwarded props. Any prefix
        // forces a per-conn one-off packet (can't reuse the pooled v5 packet). The
        // common no-alias/no-subId case leaves `props` untouched — zero extra work.
        static ByteBuffer sidBuf;
        const(char)[] dprops = props;
        // emit the coalesced overlapping-subscription ids, or this match's single
        // id when there was no coalescing (shared subs / non-overlapping)
        immutable bool anySid = coSubIds.length != 0 || m.subId != 0;
        if (v5 && (aliasVal != 0 || anySid))
        {
            sidBuf.clear();
            if (aliasVal != 0)
            {
                sidBuf.appendByte(cast(char) 0x23);
                sidBuf.appendByte(cast(char)(aliasVal >> 8));
                sidBuf.appendByte(cast(char)(aliasVal & 0xFF));
            }
            if (coSubIds.length != 0)
            {
                foreach (sid; coSubIds)
                    if (sid != 0)
                    {
                        sidBuf.appendByte(cast(char) 0x0B); // subscription-identifier
                        encodeVarint(sidBuf, sid);
                    }
            }
            else if (m.subId != 0)
            {
                sidBuf.appendByte(cast(char) 0x0B);
                encodeVarint(sidBuf, m.subId);
            }
            sidBuf.append(props);
            dprops = cast(const(char)[]) sidBuf.data;
        }
        // v5 retain-as-published keeps the publisher's retain flag; otherwise a
        // forwarded delivery clears retain [MQTT-3.3.1-9].
        immutable delRetain = m.rap && retain;
        immutable effQos = pubQos < m.qos ? pubQos : m.qos;
        if (effQos >= 1)
        {
            // QoS1/2: assign a packet-id and track it in flight (a saturated
            // window degrades this delivery to QoS0). The window is our own cap
            // tightened by the client's v5 receive-maximum (flow control): we must
            // not hold more unacked QoS1/2 toward it than it advertised.
            immutable size_t win = s.sendMax < MQTT_QOS1_WINDOW ? s.sendMax : MQTT_QOS1_WINDOW;
            // ONLINE flow control: deliver immediately only if the window has room
            // AND nothing is already held (preserve FIFO order); otherwise HOLD and
            // release on a PUBACK/PUBCOMP. OFFLINE sessions keep the prior behavior
            // (queue in obox now, or degrade to QoS0 when the window is full).
            immutable bool holdPath = !s.offline;
            ushort pid = 0;
            if ((!holdPath || s.heldQ.length == 0)
                && s.inflight.length + s.outQos2.length < win)
                pid = nextDeliveryPid(s);
            if (pid != 0)
            {
                q1.clear();
                if (effQos == 2)
                    buildPublishQos2(q1, outTopic, payload, delRetain, pid, v5, dprops);
                else
                    buildPublishQos1(q1, outTopic, payload, delRetain, pid, v5, dprops);
                if ((s.maxPktSize != 0 && q1.length > s.maxPktSize)
                    || s.obox.length + q1.length > MQTT_OBOX_CAP)
                {
                    atomicOp!"+="(gMqttDropped, 1);
                    return false;
                }
                s.obox.append(q1.data);
                recordAlias(); // registration is now queued: safe to remember it
                try
                {
                    if (effQos == 2)
                        s.outQos2[pid] = 1; // awaiting PUBREC
                    else
                        s.inflight[pid] = true;
                    // Keep for redelivery only when actually SENT to a live client
                    // (persistent session). An offline conn just QUEUES it in obox,
                    // so recording it as inflight would double-deliver on resume.
                    if (s.sessionExpiry > 0 && !s.offline)
                        s.inflightMsg[pid] = (cast(const(char)[]) q1.data).idup;
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
                return true;
            }
            // window saturated (or messages already held): HOLD as QoS1/2 and
            // release on the next PUBACK/PUBCOMP, rather than degrading to QoS0.
            // Bounded by MQTT_OBOX_CAP; only ONLINE sessions hold (offline queues
            // in obox), and a full hold-queue falls through to the QoS0 path.
            if (holdPath)
            {
                immutable msz = topic.length + payload.length + props.length;
                if (s.heldBytes + msz <= MQTT_OBOX_CAP)
                {
                    try
                    {
                        s.heldQ ~= HeldPub(topic.idup, payload.idup,
                                props.length ? props.idup : null,
                                cast(ubyte) effQos, delRetain);
                        s.heldBytes += msz;
                        return true;
                    }
                    catch (Exception)
                    {
                    }
                }
                // hold-queue full -> fall through to the QoS0 degradation below
            }
        }
        if (delRetain)
        {
            // rare (retain-as-published, QoS0): the shared packet has retain=0,
            // so build a one-off with the retain bit set
            q1.clear();
            buildPublish(q1, outTopic, payload, true, v5, dprops);
            if ((s.maxPktSize != 0 && q1.length > s.maxPktSize)
                    || s.obox.length + q1.length > MQTT_OBOX_CAP)
            {
                atomicOp!"+="(gMqttDropped, 1);
                return false;
            }
            s.obox.append(q1.data);
            recordAlias();
        }
        else if (v5 && (anySid || aliasVal != 0))
        {
            // per-sub property prefix (subscription-identifier and/or topic-alias):
            // distinct packet, so build a one-off instead of the pooled v5 packet
            q1.clear();
            buildPublish(q1, outTopic, payload, false, true, dprops);
            if ((s.maxPktSize != 0 && q1.length > s.maxPktSize)
                    || s.obox.length + q1.length > MQTT_OBOX_CAP)
            {
                atomicOp!"+="(gMqttDropped, 1);
                return false;
            }
            s.obox.append(q1.data);
            recordAlias();
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
            if ((s.maxPktSize != 0 && plen > s.maxPktSize)
                    || s.obox.length + plen > MQTT_OBOX_CAP)
            {
                atomicOp!"+="(gMqttDropped, 1); // QoS0 drop at a full outbox is spec-legal
                return false;
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
        return true; // queued to the subscriber
    }

    // Pass 1: deliver to every NORMAL subscriber; collect shared members per group
    // (their indices into tMatchBuf, in match order) for the round-robin below.
    static size_t[][string] groupIdx; // TLS, reused: group -> member indices
    bool anyShared = false;
    try
        foreach (ref lst; groupIdx.byValue)
            lst.length = 0; // reset lengths, keep capacity (reuse across messages)
    catch (Exception)
    {
    }
    foreach (i, ref m; tMatchBuf[0 .. tMatchLen])
    {
        if (m.shareGroup.length != 0)
        {
            anyShared = true;
            try
                groupIdx.require(m.shareGroup, null) ~= i;
            catch (Exception)
            {
            }
            continue;
        }
        // Non-shared: COALESCE a client's overlapping subscriptions into ONE
        // PUBLISH carrying ALL their subscription-identifiers [MQTT-3.3.4-4] at
        // the HIGHEST matching QoS [MQTT-3.3.4-2]. Deliver once, on the FIRST
        // match for this client; later matches for it are skipped.
        bool firstForClient = true;
        foreach (j; 0 .. i)
            if (tMatchBuf[j].shareGroup.length == 0 && tMatchBuf[j].c is m.c)
            {
                firstForClient = false;
                break;
            }
        if (!firstForClient)
            continue;
        tCoSubIds.length = 0;
        ubyte maxQos = 0;
        bool rp = false, anyDeliverable = false;
        foreach (j; i .. tMatchLen)
        {
            auto pm = tMatchBuf[j];
            if (pm.shareGroup.length != 0 || pm.c !is m.c)
                continue;
            if (pm.noLocal && pm.c is publisher)
                continue; // this subscription is no-local-suppressed for its owner
            anyDeliverable = true;
            if (pm.qos > maxQos)
                maxQos = pm.qos;
            rp = rp || pm.rap;
            if (pm.subId != 0)
                try
                    tCoSubIds ~= pm.subId;
                catch (Exception)
                {
                }
        }
        if (!anyDeliverable)
            continue; // every matching subscription was no-local-suppressed
        // noLocal already applied above => agg.noLocal = false (don't re-suppress)
        auto agg = Match(m.c, maxQos, false, rp, null, 0);
        coSubIds = tCoSubIds; // read by deliverTo (multiple 0x0B properties)
        cast(void) deliverTo(agg);
        coSubIds = null;
    }
    // Pass 2: shared subscriptions — ONE member of each group receives the message,
    // chosen round-robin. FALL THROUGH to the next member (in RR order, wrapping)
    // when the chosen one is ineligible (no-local-publisher / obox full / over
    // maxPktSize) so a message is never lost for the whole group.
    if (anyShared)
    {
        try
            foreach (g, ref idxs; groupIdx)
            {
                immutable cnt = idxs.length;
                if (cnt == 0)
                    continue;
                immutable rr = gShareRR.get(g, 0);
                foreach (k; 0 .. cnt)
                    if (deliverTo(tMatchBuf[idxs[(rr + k) % cnt]]))
                        break; // delivered to an eligible member — stop
                gShareRR[g] = rr + 1; // rotate the start for the next message
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
    // Release only wbox here; obox is released by mqttTeardown, so a session held
    // offline (model A) keeps its queued obox while the writer is parked.
    scope (exit)
        c.wbox.release();
    for (;;)
    {
        // Park while there's nothing to send OR while offline — a dead socket
        // must NOT be written (model A: the serve fiber owns the offline/expiry;
        // deliveries accumulate in obox as the offline queue meanwhile).
        while (!c.closed && (c.obox.empty || c.offline))
        {
            immutable ec = () @trusted {
                try
                    return c.flushEvt.emitCount;
                catch (Exception)
                    return 0;
            }();
            if (c.closed || (!c.obox.empty && !c.offline))
                break;
            try
                c.flushEvt.wait(ec);
            catch (Exception)
            {
            }
        }
        if (c.closed)
            return;
        if (c.obox.empty || c.offline)
            continue;
        swapBufs(c.obox, c.wbox); // no yield between check and swap
        if (!sendTo(c, c.wbox.data))
        {
            // Socket died mid-write: do NOT kill the writer (a reconnect needs
            // it). Mark offline and park; the serve fiber drives the will/expiry
            // and wakes us on rebind. The in-flight wbox is dropped — QoS0 loss
            // is spec-legal, QoS1/2 are redelivered (inflightMsg) on reconnect.
            c.offline = true;
            continue;
        }
        c.wbox.trim(MQTT_OBOX_KEEP); // release a burst-grown block back down
    }
}

/// After emitting a server DISCONNECT: keep the READ side open for a short window,
/// draining (and discarding) whatever the peer still sends, so the peer can READ
/// the DISCONNECT before we close. A full close() while the peer is mid-write
/// makes its next send RST (BrokenPipe) — the peer would never see the reason.
/// Returns as soon as the peer closes (waitForData false) or the window elapses.
private void mqttLingerClose(MqttConn c) nothrow @trusted
{
    ubyte[512] scratch = void;
    immutable deadline = MonoTime.currTime + dur!"msecs"(500);
    for (;;)
    {
        immutable left = deadline - MonoTime.currTime;
        if (left <= Duration.zero)
            break;
        bool alive;
        try
            alive = c.tcp.waitForData(left);
        catch (Exception)
            alive = false;
        if (!alive)
            break; // peer closed, or the linger window elapsed
        immutable avail = () @trusted {
            try
                return c.tcp.leastSize;
            catch (Exception)
                return cast(ulong) 0;
        }();
        if (avail == 0)
            break;
        immutable n = avail > scratch.length ? scratch.length : cast(size_t) avail;
        try
            c.tcp.read(scratch[0 .. n]); // drain + discard
        catch (Exception)
            break;
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

/// Release held QoS1/2 deliveries (flow control) into freed send-window slots, in
/// FIFO order, until the window is full again or the hold-queue drains. Called
/// after a PUBACK/PUBCOMP frees a slot, and after a reconnect resumes the window.
/// Each released message is assigned a fresh packet-id, queued to obox, and
/// tracked in flight exactly like an immediate QoS1/2 delivery.
private void mqttReleaseHeld(MqttConn s) nothrow @trusted
{
    if (s.heldQ.length == 0)
        return;
    immutable v5 = s.protoVer == 5;
    immutable size_t win = s.sendMax < MQTT_QOS1_WINDOW ? s.sendMax : MQTT_QOS1_WINDOW;
    static ByteBuffer q1; // TLS
    while (s.heldQ.length != 0 && s.inflight.length + s.outQos2.length < win)
    {
        auto h = s.heldQ[0];
        immutable pid = nextDeliveryPid(s);
        if (pid == 0)
            break; // no free id despite window room (defensive)
        q1.clear();
        if (h.qos == 2)
            buildPublishQos2(q1, h.topic, h.payload, h.retain, pid, v5, h.props);
        else
            buildPublishQos1(q1, h.topic, h.payload, h.retain, pid, v5, h.props);
        if ((s.maxPktSize != 0 && q1.length > s.maxPktSize)
            || s.obox.length + q1.length > MQTT_OBOX_CAP)
            atomicOp!"+="(gMqttDropped, 1); // can't queue now: drop (obox full/too big)
        else
        {
            s.obox.append(q1.data);
            try
            {
                if (h.qos == 2)
                    s.outQos2[pid] = 1; // awaiting PUBREC
                else
                    s.inflight[pid] = true;
                if (s.sessionExpiry > 0 && !s.offline)
                    s.inflightMsg[pid] = (cast(const(char)[]) q1.data).idup;
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
        }
        immutable msz = h.topic.length + h.payload.length + h.props.length;
        s.heldBytes = s.heldBytes > msz ? s.heldBytes - msz : 0;
        foreach (i; 1 .. s.heldQ.length)
            s.heldQ[i - 1] = s.heldQ[i];
        s.heldQ.length = s.heldQ.length - 1;
    }
}

private bool sendTo(MqttConn c, scope const(ubyte)[] bytes) nothrow
{
    import dreads.tls : legSend;

    if (gMqttSysBytes && bytes.length)
    {
        import core.atomic : atomicOp;

        atomicOp!"+="(gMqttBytesTx, cast(ulong) bytes.length);
    }
    try
    {
        c.wlock.lock();
        scope (exit)
            c.wlock.unlock();
        if (c.wsCodec !is null)
        {
            // flush any control frames (pong/close) the decoder queued, then
            // the MQTT payload as a binary frame — all under the wlock so
            // frames keep order on the wire
            static ByteBuffer wb; // TLS scratch, consumed synchronously
            wb.clear();
            if (!c.wsCodec.ctlOut.empty)
            {
                wb.append(c.wsCodec.ctlOut.data);
                c.wsCodec.ctlOut.clear();
            }
            if (bytes.length)
                wsEncodeBinary(wb, bytes);
            if (!wb.empty)
            {
                if (c.tlsLeg !is null)
                    return legSend(c.tlsLeg, c.tcp, cast(const(ubyte)[]) wb.data);
                c.tcp.write(wb.data);
            }
            return true;
        }
        if (c.tlsLeg !is null)
            return legSend(c.tlsLeg, c.tcp, bytes);
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
// Remove an offline placeholder from the per-shard list (swap-remove).
/// Publish the Last Will now (local delivery + cross-shard fan-out + retained if
/// the will-retain flag is set), then clear willTopic so it fires exactly once.
private void fireWill(MqttConn c) nothrow @trusted
{
    if (c.willTopic.length == 0)
        return;
    // defensive: never publish a will the authenticated user can't access (the
    // CONNECT path already drops it; this guards any other fire site). No-op
    // without ACL (c.aclUser is null).
    if (c.aclUser !is null && !aclCanAccessChannel(c.aclUser, c.willTopic))
    {
        c.willTopic = null;
        return;
    }
    immutable rseq = c.willRetain ? atomicOp!"+="(gMqttRetainSeq, 1) : 0;
    mqttDeliverLocal(c.willTopic, cast(const(char)[]) c.willPayload, c.willRetain,
            rseq, 0, null, c.willProps);
    if (gMqttFanout !is null)
        gMqttFanout(c.willTopic, cast(const(char)[]) c.willPayload, c.willRetain,
                rseq, 0, c.willProps);
    c.willTopic = null;
}

/// The message-expiry-interval (0x02) value in a v5 PUBLISH property block, or 0
/// if absent. Walks the (broker-built, well-formed) block; bounded by its length.
private uint msgExpiryFromProps(scope const(char)[] props) @nogc nothrow @trusted
{
    auto p = cast(const(ubyte)[]) props;
    size_t i = 0;
    while (i < p.length)
    {
        immutable id = p[i++];
        switch (id)
        {
        case 0x01: // payload-format-indicator
            i += 1;
            break;
        case 0x02: // message-expiry-interval: u32
            if (i + 4 > p.length)
                return 0;
            return (cast(uint) p[i] << 24) | (cast(uint) p[i + 1] << 16)
                | (cast(uint) p[i + 2] << 8) | p[i + 3];
        case 0x23: // topic-alias: u16
            i += 2;
            break;
        case 0x03, 0x08, 0x09: // content-type / response-topic / correlation-data
            if (i + 2 > p.length)
                return 0;
            i += 2 + ((cast(size_t) p[i] << 8) | p[i + 1]);
            break;
        case 0x0B: // subscription-identifier: varint
            uint sv;
            if (!decodeVarint(p, i, sv))
                return 0;
            break;
        case 0x26: // user-property: two length-prefixed strings
            foreach (_; 0 .. 2)
            {
                if (i + 2 > p.length)
                    return 0;
                i += 2 + ((cast(size_t) p[i] << 8) | p[i + 1]);
            }
            break;
        default:
            return 0; // unknown id in our own block: stop
        }
        if (i > p.length)
            return 0;
    }
    return 0;
}

/// Offset of the 4 message-expiry-interval value bytes within a v5 PUBLISH packet
/// (`[hdr][remlen][topic]([pid])[proplen][props][payload]`), or -1 if absent.
private long expiryValueOffsetInPacket(scope const(ubyte)[] pkt) @nogc nothrow @trusted
{
    if (pkt.length < 2)
        return -1;
    immutable qos = (pkt[0] >> 1) & 3;
    size_t i = 1;
    uint rem;
    if (!decodeVarint(pkt, i, rem))
        return -1;
    if (i + 2 > pkt.length)
        return -1;
    i += 2 + ((cast(size_t) pkt[i] << 8) | pkt[i + 1]); // skip topic
    if (qos >= 1)
        i += 2; // skip packet-id
    if (i > pkt.length)
        return -1;
    uint plen;
    if (!decodeVarint(pkt, i, plen))
        return -1;
    immutable propsEnd = i + plen;
    if (propsEnd > pkt.length)
        return -1;
    size_t j = i;
    while (j < propsEnd)
    {
        immutable id = pkt[j++];
        switch (id)
        {
        case 0x01:
            j += 1;
            break;
        case 0x02:
            return (j + 4 <= propsEnd) ? cast(long) j : -1;
        case 0x23:
            j += 2;
            break;
        case 0x03, 0x08, 0x09:
            if (j + 2 > propsEnd)
                return -1;
            j += 2 + ((cast(size_t) pkt[j] << 8) | pkt[j + 1]);
            break;
        case 0x0B:
            uint sv;
            if (!decodeVarint(pkt, j, sv))
                return -1;
            break;
        case 0x26:
            foreach (_; 0 .. 2)
            {
                if (j + 2 > propsEnd)
                    return -1;
                j += 2 + ((cast(size_t) pkt[j] << 8) | pkt[j + 1]);
            }
            break;
        default:
            return -1;
        }
        if (j > propsEnd)
            return -1;
    }
    return -1;
}

/// Process a resuming session's offline queue for message-expiry [MQTT-3.3.2-5]:
/// DROP the PUBLISHes whose deadline has passed, and rewrite the survivors'
/// message-expiry-interval to the seconds remaining. Rebuilds obox in place;
/// untracked packets (no expiry / redelivered PUBREL) are copied verbatim.
private void mqttExpireOfflineQueue(MqttConn c) nothrow @trusted
{
    if (c.oExprQ.length == 0)
        return;
    immutable now = MonoTime.currTime;
    auto d = c.obox.data;
    static ByteBuffer filtered; // TLS: the surviving, decremented queue
    filtered.clear();
    size_t pos = 0;
    while (pos < d.length)
    {
        immutable ubyte hdr = d[pos];
        size_t p = pos + 1;
        uint rem;
        if (!decodeVarint(d, p, rem) || p + rem > d.length)
        {
            filtered.append(d[pos .. $]); // malformed tail: copy verbatim, stop
            pos = d.length;
            break;
        }
        immutable pktEnd = p + rem;
        long di = -1;
        foreach (k, ref e; c.oExprQ)
            if (e.start == pos)
            {
                di = k;
                break;
            }
        if (di >= 0 && (hdr >> 4) == PT_PUBLISH)
        {
            if (now >= c.oExprQ[di].deadline)
            {
                pos = pktEnd; // expired: drop the whole packet
                continue;
            }
            immutable uint remain = cast(uint)((c.oExprQ[di].deadline - now).total!"seconds");
            immutable fStart = filtered.length;
            filtered.append(d[pos .. pktEnd]);
            immutable eoff = expiryValueOffsetInPacket(d[pos .. pktEnd]);
            if (eoff >= 0)
            {
                auto fb = filtered.data;
                immutable o2 = fStart + cast(size_t) eoff;
                if (o2 + 4 <= fb.length)
                {
                    fb[o2] = cast(ubyte)(remain >> 24);
                    fb[o2 + 1] = cast(ubyte)(remain >> 16);
                    fb[o2 + 2] = cast(ubyte)(remain >> 8);
                    fb[o2 + 3] = cast(ubyte)(remain & 0xFF);
                }
            }
        }
        else
            filtered.append(d[pos .. pktEnd]);
        pos = pktEnd;
    }
    c.obox.clear();
    c.obox.append(filtered.data);
    c.oExprQ = null;
}

/// A clean_start=0 CONNECT: MIGRATE the parked session `parked` (same shard) onto
/// the reconnecting connection `newc`, which keeps serving on its OWN socket. We
/// do NOT move the TCPConnection between conns — vibe forbids reassigning a
/// socket while a read is in-flight (`assert(sock == m_socket)`, net.d) and doing
/// so crashes under multiple shards. Instead newc adopts the parked session's
/// subscriptions (re-pointed in the trie), offline queue, in-flight QoS1/2,
/// publication-expiry tracking and flow-control hold-queue; then the parked fiber
/// is signalled to end. Same-thread cooperative scheduling: every mutation here
/// completes before this fiber yields, and the parked fiber is suspended.
/// Returns true (a session was resumed).
/// Adopt a parked session's state onto the reconnecting connection `newc`:
/// re-point its subscriptions to newc's trie, take its offline queue + in-flight
/// QoS1/2 + publication-expiry + flow-control-hold state, run the expiry sweep,
/// and redeliver the unacked (DUP) + PUBRELs. Reads only from `parked` (a frozen
/// or same-thread session — no concurrent writer). Does NOT end `parked`.
private void mqttAdoptState(MqttConn parked, MqttConn newc) nothrow @trusted
{
    foreach (i, f; parked.filters)
    {
        if (i >= parked.subInfo.length)
            break;
        auto si = parked.subInfo[i];
        cast(void) trieSubscribe(f, newc, si.qos, si.opts, si.shareGroup, si.subId);
        try
        {
            newc.filters ~= f;
            newc.subInfo ~= si;
        }
        catch (Exception)
        {
        }
    }
    try
        if (!parked.obox.empty)
            newc.obox.append(parked.obox.data);
    catch (Exception)
    {
    }
    newc.inflight = parked.inflight;
    newc.outQos2 = parked.outQos2;
    newc.inflightMsg = parked.inflightMsg;
    newc.oExprQ = parked.oExprQ;
    newc.heldQ = parked.heldQ;
    newc.heldBytes = parked.heldBytes;
    mqttExpireOfflineQueue(newc); // drop expired, decrement survivors
    try
        foreach (pid, msg; newc.inflightMsg)
        {
            auto dup = msg.dup;
            if (dup.length)
                dup[0] = cast(char)(dup[0] | 0x08); // set the DUP bit [MQTT-4.4.0-1]
            newc.obox.append(dup);
        }
    catch (Exception)
    {
    }
    try
        foreach (pid, st; newc.outQos2)
            if (st == 2)
            {
                newc.obox.appendByte(cast(char)((PT_PUBREL << 4) | 0x02));
                newc.obox.appendByte(cast(char) 2);
                newc.obox.appendByte(cast(char)(pid >> 8));
                newc.obox.appendByte(cast(char)(pid & 0xFF));
            }
    catch (Exception)
    {
    }
    mqttReleaseHeld(newc); // flow any messages held behind the window pre-drop
}

/// SAME-shard reconnect: adopt the parked session onto newc, then signal the
/// parked fiber to discard + tear down (it no longer owns the id, and its will
/// must not fire). Synchronous — same thread, no freeze handshake needed.
private bool mqttMigrateParked(MqttConn parked, MqttConn newc) nothrow @trusted
{
    mqttAdoptState(parked, newc);
    parked.willTopic = null;
    parked.discard = true;
    try
        parked.reconnectEvt.emit();
    catch (Exception)
    {
    }
    return true;
}

/// CROSS-shard reconnect: the parked session lives on ANOTHER shard. Claim it from
/// the shared pool, ask its owner to FREEZE it (ShardMsg.mqttResume), wait for the
/// freeze (bounded), then adopt its now-stable state. The owner's freeze fiber
/// ends `parked` itself after its redirect window, so we never write to it. Falls
/// back to a fresh session (returns false) if there is no cross-shard path or the
/// owner does not freeze in time.
private bool mqttResumeXShard(MqttConn newc) nothrow @trusted
{
    import vibe.core.core : sleep;

    if (gMqttResume is null || newc.clientId.length == 0)
        return false;
    // claim the parked session from the cross-shard pool (spinlock — yield-free)
    MqttConn parked;
    parkedLock();
    try
        if (auto pc = newc.clientId in gParkedPool)
        {
            parked = *pc;
            gParkedPool.remove(newc.clientId);
        }
    catch (Exception)
    {
    }
    parkedUnlock();
    if (parked is null || parked.shardId == tShard)
        return false; // nothing parked cross-shard for this id
    // ask the owner shard to freeze it, then wait (bounded) for the frozen flag
    gMqttResume(parked.shardId, newc.clientId);
    bool ok = false;
    immutable deadline = MonoTime.currTime + dur!"msecs"(500);
    while (MonoTime.currTime < deadline)
    {
        if (atomicLoad!(MemoryOrder.acq)(parked.frozen))
        {
            ok = true;
            break;
        }
        try
            sleep(2.msecs); // yield-loop: the owner freezes within a drain tick
        catch (Exception)
        {
        }
    }
    if (!ok)
        return false; // owner didn't freeze in time -> start a fresh session
    mqttAdoptState(parked, newc); // parked is frozen (redirect=drop) -> safe to read
    return true;
}

/// clean_start=1 CONNECT for a client id whose session is parked offline on this
/// shard: signal that parked session to END now (its fiber unsubscribes + drops
/// its keyspace record), so the fresh session takes over cleanly.
private void mqttDiscardParked(MqttConn old) nothrow @trusted
{
    old.discard = true;
    try
        old.reconnectEvt.emit();
    catch (Exception)
    {
    }
}

/// The transparent-strategy park (model A). Called by the serve fiber when the
/// socket dies / a keepalive lapses / a clean DISCONNECT arrives. If the session
/// is persistent it is held ALIVE here — the fiber parks on reconnectEvt with a
/// timed wait that also drives will-delay and session-expiry — and vibe never
/// learns the socket died. Returns true when a reconnect rebound a new socket
/// (the serve loop resumes reading c.tcp); false when the session ended (expiry,
/// or non-persistent) and the caller must fall through to a real teardown.
/// `cleanDisc` = a clean DISCONNECT (no will fires); otherwise an abnormal drop.
private bool mqttParkOrEnd(MqttConn c, bool cleanDisc) nothrow @trusted
{
    immutable expirySecs0 = c.sessionExpiry == uint.max ? 0x7FFF_FFFF : c.sessionExpiry;
    immutable wdEff = c.willDelay > expirySecs0 ? expirySecs0 : c.willDelay;
    // Park when EITHER: (a) a persistent session (expiry > 0) with live subs must
    // hold its offline queue for a reconnect, OR (b) an abnormal drop must DELAY
    // the will by will-delay-interval (capped by session-expiry) [MQTT-3.1.3-9] —
    // this holds even for a subscriber-less publisher. Must still be the
    // registered owner of its client id.
    immutable queueHold = c.sessionExpiry > 0 && c.filters.length > 0;
    immutable willDelayHold = !cleanDisc && c.willTopic.length != 0 && wdEff > 0;
    bool eligible = c.connected && c.clientId.length != 0
        && (queueHold || willDelayHold);
    if (eligible)
        try
            eligible = (c.clientId in gLocalClients) !is null
                && *(c.clientId in gLocalClients) is c;
        catch (Exception)
            eligible = false;
    if (!eligible)
        return false; // real teardown (fires the will there if one is set)

    closeTcp(c); // release the dead/disconnected fd; the SESSION lives on in us
    c.offline = true;
    mqttFlushDirty();
    if (queueHold)
    {
        mqttSessionPut(c.clientId, c.sessionExpiry); // advertise session-present
        parkedPoolPut(c); // and make it resumable from ANY shard (cross-shard reconnect)
    }

    immutable now = MonoTime.currTime;
    immutable expirySecs = expirySecs0;
    immutable expiryAt = now + dur!"seconds"(expirySecs);
    // will-delay is capped by session-expiry [MQTT-3.1.3-9]; cleanDisc => no will
    bool willPending = !cleanDisc && c.willTopic.length != 0;
    MonoTime willAt = now + dur!"seconds"(wdEff);

    for (;;)
    {
        immutable t = MonoTime.currTime;
        // next deadline = the sooner of an unfired will and the session expiry
        MonoTime next = expiryAt;
        if (willPending && willAt < next)
            next = willAt;
        Duration timeout = next > t ? (next - t) : Duration.zero;

        immutable ec = () @trusted {
            try
                return c.reconnectEvt.emitCount;
            catch (Exception)
                return 0;
        }();
        immutable newEc = () @trusted {
            try
                return c.reconnectEvt.waitUninterruptible(timeout, ec);
            catch (Exception)
                return ec;
        }();
        if (newEc != ec) // woken: freeze-for-cross-shard-resume, migration, or discard
        {
            if (c.freezeReq)
            {
                // A reconnect on ANOTHER shard is adopting this session. FREEZE:
                // set `redirect` (deliverTo now drops into us — obox stops growing)
                // then publish `frozen` (release) so the reconnecting shard reads a
                // stable obox+state. Same-thread cooperative scheduling => no other
                // fiber runs between these stores, so no delivery races the freeze.
                c.freezeReq = false;
                c.willTopic = null; // session lives on the new shard: our will must NOT fire
                atomicStore!(MemoryOrder.rel)(c.redirect, true);
                atomicStore!(MemoryOrder.rel)(c.frozen, true);
                parkedPoolDel(c); // claimed by the reconnecting shard; leave the pool
                // hold as a redirect for a bounded window (covers the reconnecting
                // shard's adopt + routing convergence), then end (teardown drops our
                // subs). ~1s time-to-die, per the design.
                immutable rdl = MonoTime.currTime + dur!"msecs"(1000);
                for (;;)
                {
                    immutable left = rdl - MonoTime.currTime;
                    if (left <= Duration.zero)
                        break;
                    immutable e2 = () @trusted {
                        try
                            return c.reconnectEvt.emitCount;
                        catch (Exception)
                            return 0;
                    }();
                    cast(void) () @trusted {
                        try
                            return c.reconnectEvt.waitUninterruptible(left, e2);
                        catch (Exception)
                            return e2;
                    }();
                    if (MonoTime.currTime >= rdl)
                        break;
                }
                c.sessionExpiry = 0; // teardown discards the record
                return false;
            }
            // migration (same-shard) or clean_start discard -> end
            c.discard = false;
            c.sessionExpiry = 0; // teardown discards the record; the new conn owns it
            return false;
        }
        // timed out: fire a due will, end at expiry
        immutable t2 = MonoTime.currTime;
        if (willPending && t2 >= willAt)
        {
            fireWill(c);
            mqttFlushDirty(); // wake the will subscribers' writers to send it NOW
            willPending = false;
        }
        if (t2 >= expiryAt)
        {
            if (willPending)
                fireWill(c);
            c.sessionExpiry = 0; // tell teardown to DISCARD the session record
            return false;
        }
    }
}

/// Maintenance sweep hook (retained for server.d wiring). Model A holds offline
/// sessions in their own parked serve fibers with per-session timed-wait expiry,
/// so there is no placeholder list to reap here.
public void mqttReapOfflineConns() nothrow @trusted
{
}

private void mqttTeardown(MqttConn c, Task writer) nothrow
{
    immutable wasConnected = c.connected;
    if (c.connected)
    {
        import core.atomic : atomicOp;

        atomicOp!"-="(gMqttClientsConnected, 1); // $SYS clients/connected
        c.connected = false;
    }
    scope (exit)
    {
        if (c.tlsLeg !is null)
        {
            c.tlsLeg.free(); // owning thread; after the writer joined
            c.tlsLeg = null;
        }
        if (c.wsCodec !is null)
        {
            c.wsCodec.free();
            c.wsCodec = null;
        }
    }
    // Model A: a persistent session that dropped its socket is held ALIVE by its
    // parked serve fiber (mqttParkOrEnd), NOT here. Teardown is the REAL end —
    // reached only when the session ended (expiry, park set sessionExpiry=0) or
    // was never persistent (protocol error / clean session / bad CONNECT).
    parkedPoolDel(c); // drop the cross-shard pool entry if this conn owned it
    // tear down subscriptions for real (not just lazily): under connect/
    // subscribe churn the lazy-gen scheme leaked trie entries and pinned
    // gMqttSubTotal above zero forever, keeping the idle-skin gate open.
    foreach (f; c.filters)
        trieUnsubscribe(f, c);
    c.filters = null;
    // drop our clientId registration, but only if a NEWER session hasn't
    // already replaced us in the map (identity check)
    bool stillMine = false;
    if (c.clientId.length != 0)
        try
        {
            if (auto pc = c.clientId in gLocalClients)
                if (*pc is c)
                {
                    stillMine = true;
                    gLocalClients.remove(c.clientId);
                }
        }
        catch (Exception)
        {
        }
    // Persistent session: on our own close (not a takeover), keep the session
    // record for session-expiry-interval seconds (Redis TTL) so a later
    // clean_start=0 reconnect resumes it; expiry 0 discards it. A takeover skips
    // this — the newer connection already owns the client id's session.
    if (stillMine)
    {
        if (c.sessionExpiry == 0)
            mqttSessionDel(c.clientId);
        else
            mqttSessionPut(c.clientId, c.sessionExpiry);
    }
    c.gen++; // invalidate any remaining trie entries (lazily skipped)
    // Last Will: an abnormal disconnect (TCP death / takeover / protocol error)
    // left willTopic set (a clean DISCONNECT cleared it) -> publish it now, the
    // same path a live PUBLISH takes (local delivery + cross-shard fan-out +
    // retained if the will-retain flag was set).
    if (wasConnected)
        fireWill(c); // no-op if a clean DISCONNECT or the park already fired it
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
    c.obox.release(); // the writer no longer releases obox (offline-hold keeps it)
}

public void serveMqttClient(TCPConnection tcp, bool tls = false, bool ws = false) nothrow
{
    try
        tcp.tcpNoDelay = true; // PUBACK/deliveries are tiny — Nagle throttles
    catch (Exception)          // the QoS1 window to ~6k msg/s (measured)
    {
    }
    auto c = new MqttConn(tcp);
    if (tls)
    {
        c.tlsLeg = TlsLeg.create(true);
        if (c.tlsLeg is null)
        {
            closeTcp(c);
            return;
        }
    }
    if (ws)
    {
        // Gather the HTTP upgrade request (decrypted through the TLS leg for
        // wss), do the RFC 6455 handshake, then run the normal MQTT loop with a
        // WS codec framing every read/write. The response is routed over the
        // same transport (raw, or TLS-encrypted).
        static ByteBuffer reqbuf, respbuf;
        reqbuf.clear();
        bool gotHeaders = false;
        foreach (_; 0 .. 64) // bounded: handshake completes in a few round-trips
        {
            if (c.tlsLeg !is null)
            {
                if (!legPump(c.tlsLeg, tcp))
                    break;
                legDrainInto(c.tlsLeg, reqbuf);
                cast(void) legSend(c.tlsLeg, tcp, null); // flush TLS handshake cipher
            }
            else
            {
                ubyte[8192] hb = void;
                size_t hn;
                try
                {
                    if (!tcp.waitForData(30.seconds))
                        break;
                    hn = tcp.read(hb[], IOMode.once);
                }
                catch (Exception)
                {
                    break;
                }
                if (hn == 0)
                    break;
                reqbuf.append(hb[0 .. hn]);
            }
            if (wsHeadersComplete(cast(const(ubyte)[]) reqbuf.data))
            {
                gotHeaders = true;
                break;
            }
        }
        if (!gotHeaders
                || !wsHandshakeResponse(cast(const(char)[]) reqbuf.data, "mqtt", respbuf))
        {
            closeTcp(c);
            return;
        }
        c.wsCodec = WsCodec.create();
        if (c.wsCodec is null)
        {
            closeTcp(c);
            return;
        }
        // write the 101 response over the right transport
        bool sent;
        if (c.tlsLeg !is null)
            sent = legSend(c.tlsLeg, tcp, cast(const(ubyte)[]) respbuf.data);
        else
        {
            try
            {
                tcp.write(cast(const(ubyte)[]) respbuf.data);
                sent = true;
            }
            catch (Exception)
            {
            }
        }
        if (!sent)
        {
            closeTcp(c);
            return;
        }
        // a client that pipelined its CONNECT right after the upgrade
        auto tail = wsBodyAfterHandshake(cast(const(ubyte)[]) reqbuf.data);
        if (tail.length)
            cast(void) c.wsCodec.feed(tail);
    }
    c.shardId = tShard; // the shard that accepted this socket (for cross-shard resume)
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
    readloop: for (;;)
    {
        // MQTT-over-WS: decoded bytes from a prior read (or the handshake tail)
        // may already hold a full packet — process them WITHOUT blocking on the
        // socket, or a client awaiting CONNACK after a pipelined CONNECT hangs.
        immutable bool wsHavePre = c.wsCodec !is null && !c.wsCodec.pin.empty;
        if (wsHavePre)
            c.wsCodec.drainInto(inb);
        else
        {
        // read at least one byte; a keepalive-exceeded silence closes the conn
        // (waitForData returns false on BOTH the deadline and a real close —
        // both mean "drop it", exactly the MQTT keepalive contract). c.tcp (not
        // the captured `tcp`) is read each time so a reconnect's rebind is picked
        // up transparently.
        bool alive;
        try
            alive = c.tcp.waitForData(c.readDeadline);
        catch (Exception)
            alive = false;
        if (!alive)
        {
            // socket death / keepalive lapse: park the session (model A). A
            // reconnect rebinds a fresh socket and we resume; expiry ends us.
            if (mqttParkOrEnd(c, false))
            {
                inb.clear();
                outb.clear();
                continue readloop;
            }
            return;
        }
        auto avail = () @trusted {
            try
                return c.tcp.leastSize;
            catch (Exception)
                return cast(ulong) 0;
        }();
        if (avail == 0)
        {
            if (mqttParkOrEnd(c, false))
            {
                inb.clear();
                outb.clear();
                continue readloop;
            }
            return;
        }
        if (c.wsCodec !is null)
        {
            // read raw socket bytes (optionally through TLS for wss), decode WS
            // frames into MQTT bytes. A pong/close the decoder queued goes out
            // via sendTo(null). Any already-buffered decoded bytes (from the
            // handshake tail) also drain here.
            static ubyte[65536] wsread = void;
            long rn;
            if (c.tlsLeg !is null)
            {
                if (!legPump(c.tlsLeg, c.tcp))
                {
                    if (mqttParkOrEnd(c, false)) { inb.clear(); outb.clear(); continue readloop; }
                    return;
                }
                static ByteBuffer plain;
                plain.clear();
                legDrainInto(c.tlsLeg, plain);
                if (!c.wsCodec.feed(cast(const(ubyte)[]) plain.data))
                {
                    sendTo(c, null); // flush the close echo
                    return;
                }
            }
            else
            {
                try
                    rn = c.tcp.read(wsread[0 .. (avail < wsread.length ? cast(size_t) avail : wsread.length)], IOMode.once);
                catch (Exception)
                {
                    if (mqttParkOrEnd(c, false)) { inb.clear(); outb.clear(); continue readloop; }
                    return;
                }
                if (rn <= 0)
                    return;
                if (!c.wsCodec.feed(wsread[0 .. cast(size_t) rn]))
                {
                    sendTo(c, null); // flush the close echo, then end
                    return;
                }
            }
            if (!c.wsCodec.ctlOut.empty)
                sendTo(c, null); // flush pong(s)
            c.wsCodec.drainInto(inb);
        }
        else if (c.tlsLeg !is null)
        {
            // cipher -> engine -> plaintext into inb; packet loop unchanged.
            // sendTo(null) flushes handshake cipher under the wlock.
            if (!legPump(c.tlsLeg, c.tcp) || !sendTo(c, null))
            {
                if (mqttParkOrEnd(c, false))
                {
                    inb.clear();
                    outb.clear();
                    continue readloop;
                }
                return;
            }
            legDrainInto(c.tlsLeg, inb);
        }
        else
        {
        auto space = inb.freeSpace(cast(size_t) avail);
        if (space.length < cast(size_t) avail)
            return; // OOM growing the input buffer: drop THIS client, not the broker
        try
            c.tcp.read(space[0 .. cast(size_t) avail]);
        catch (Exception)
        {
            if (mqttParkOrEnd(c, false))
            {
                inb.clear();
                outb.clear();
                continue readloop;
            }
            return;
        }
        inb.grow(cast(size_t) avail);
        }
        } // end: else (blocking read when no pre-buffered WS bytes)

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
            immutable pktCap = c.connected ? MQTT_MAX_PACKET : MQTT_PRECONNECT_MAX;
            if (rem > pktCap)
            {
                // oversized frame: refuse to buffer it. Tell a connected v5
                // client why (Packet Too Large) before dropping the socket.
                if (c.connected && c.protoVer == 5)
                    mqttServerDisconnect(outb, 0x95);
                if (!outb.empty)
                    sendTo(c, outb.data);
                return;
            }
            if (hp + rem > d.length)
                break; // incomplete body
            immutable ubyte h = d[pos];
            auto body_ = d[hp .. hp + rem];
            if (gMqttSysBytes)
            {
                import core.atomic : atomicOp;

                atomicOp!"+="(gMqttBytesRx, cast(ulong)(hp + rem - pos));
            }
            if (!handlePacket(c, h, body_, outb))
            {
                // v5: on a protocol-error close of an ESTABLISHED session, send a
                // server DISCONNECT with a reason so the client isn't left to
                // infer a bare TCP reset. A clean client DISCONNECT (which also
                // returns false) needs none; a failed/absent CONNECT is answered
                // by CONNACK (c.connected is still false there), not DISCONNECT.
                immutable cleanDisc = (h >> 4) == PT_DISCONNECT;
                immutable sentDisc = c.connected && c.protoVer == 5 && !cleanDisc;
                if (sentDisc)
                    mqttServerDisconnect(outb, c.discReason); // 0x82, or a handler's reason
                if (!outb.empty) // flush any acks built before the close
                    sendTo(c, outb.data);
                // After a server DISCONNECT, LINGER: keep the read side open briefly
                // so the peer can read the DISCONNECT (and its in-flight writes are
                // drained, not RST) before we close [flow_control2 / Receive Maximum].
                if (sentDisc)
                    mqttLingerClose(c);
                // Park the session (model A): a persistent session is held ALIVE
                // for a reconnect; a clean DISCONNECT fires no will, an abnormal
                // one fires it after will-delay. Non-persistent => real teardown.
                if (mqttParkOrEnd(c, cleanDisc))
                {
                    inb.clear();
                    outb.clear();
                    continue readloop;
                }
                return; // session ended (expiry / non-persistent)
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

// Extract the session-expiry-interval (0x11, u32) from a v5 DISCONNECT payload,
// which may OVERRIDE the CONNECT value [MQTT-3.1.2-23]. p = [reason?][proplen]
// [props]. Returns false on malformation; hasSei=true when 0x11 was present.
private bool mqttDisconnectSEI(scope const(ubyte)[] p, out uint sei, out bool hasSei) @nogc nothrow
{
    hasSei = false;
    if (p.length <= 1)
        return true; // no reason+props (or reason only): SEI unchanged
    size_t i = 1; // skip reason code
    uint plen;
    if (!decodeVarint(p, i, plen))
        return false;
    immutable end = i + plen;
    if (end > p.length)
        return false;
    while (i < end)
    {
        immutable id = p[i++];
        switch (id)
        {
        case 0x11: // session-expiry-interval u32
            if (i + 4 > end)
                return false;
            sei = (cast(uint) p[i] << 24) | (cast(uint) p[i + 1] << 16)
                | (cast(uint) p[i + 2] << 8) | p[i + 3];
            hasSei = true;
            i += 4;
            break;
        case 0x1F, 0x1C: // reason-string / server-reference: utf8 string
            if (i + 2 > end)
                return false;
            immutable n = (cast(size_t) p[i] << 8) | p[i + 1];
            i += 2;
            if (i + n > end)
                return false;
            i += n;
            break;
        case 0x26: // user-property: two utf8 strings
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
            return false; // unknown DISCONNECT property
        }
    }
    return true;
}

// Parse a v5 CONNECT property block at p[i], extracting receive-maximum (0x21)
// and correctly skipping every other CONNECT property by type. Advances i to the
// end; every read is bounded by `end` (<= p.length) so a malformed block can't
// read OOB. recvMax stays 0 if absent (caller treats 0 as "no client limit").
private bool mqttParseConnectProps(scope const(ubyte)[] p, ref size_t i,
        out ushort recvMax, out uint maxPkt, out ushort aliasMax,
        out uint sessionExpiry) @nogc nothrow
{
    uint plen;
    if (!decodeVarint(p, i, plen))
        return false;
    immutable end = i + plen;
    if (end > p.length)
        return false;
    while (i < end)
    {
        immutable id = p[i++];
        switch (id)
        {
        case 0x11, 0x27: // session-expiry-interval u32, maximum-packet-size u32
            if (i + 4 > end)
                return false;
            immutable u32 = (cast(uint) p[i] << 24) | (cast(uint) p[i + 1] << 16)
                | (cast(uint) p[i + 2] << 8) | p[i + 3];
            if (id == 0x27) // maximum-packet-size: cap our outbound to this client
                maxPkt = u32;
            else // 0x11 session-expiry-interval (0xFFFFFFFF = never expire)
                sessionExpiry = u32;
            i += 4;
            break;
        case 0x21, 0x22: // receive-maximum u16, topic-alias-maximum u16
            if (i + 2 > end)
                return false;
            if (id == 0x21)
                recvMax = cast(ushort)((p[i] << 8) | p[i + 1]);
            else // 0x22: how many outbound aliases this client will accept
                aliasMax = cast(ushort)((p[i] << 8) | p[i + 1]);
            i += 2;
            break;
        case 0x17, 0x19: // request-problem-info, request-response-info: 1 byte
            if (i + 1 > end)
                return false;
            i += 1;
            break;
        case 0x15, 0x16: // authentication-method string, authentication-data binary
            if (i + 2 > end)
                return false;
            immutable n = (cast(size_t) p[i] << 8) | p[i + 1];
            i += 2;
            if (i + n > end)
                return false;
            i += n;
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
            return false; // unknown CONNECT property id: malformed
        }
    }
    i = end;
    return true;
}

// Parse a v5 SUBSCRIBE property block at p[i], extracting the subscription-
// identifier (0x0B) and skipping user-property (0x26) — the only two properties
// valid on SUBSCRIBE. A subscription-identifier of 0 is a protocol error
// [MQTT-3.8.2.1-2]; any other property id is malformed. Bounded by `end`.
private bool mqttParseSubProps(scope const(ubyte)[] p, ref size_t i, out uint subId) @nogc nothrow
{
    subId = 0;
    uint plen;
    if (!decodeVarint(p, i, plen))
        return false;
    immutable end = i + plen;
    if (end > p.length)
        return false;
    while (i < end)
    {
        immutable id = p[i++];
        switch (id)
        {
        case 0x0B: // subscription-identifier: varint 1..268435455
            if (subId != 0)
                return false; // [MQTT-3.8.2.1-2] more than one is a protocol error
            uint v;
            if (!decodeVarint(p, i, v) || i > end)
                return false;
            if (v == 0)
                return false; // [MQTT-3.8.2.1-2] a 0 identifier is a protocol error
            subId = v;
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
            return false; // only subscription-identifier + user-property are legal
        }
    }
    i = end;
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

// Parse a v5 will property block at p[i], building `fwd` = the FORWARDABLE will
// properties (everything EXCEPT will-delay-interval 0x18, which is will-specific
// and never delivered) so the will PUBLISH carries content-type / user-property /
// response-topic / correlation-data / payload-format / message-expiry to
// subscribers. Advances i to the block end; every read is bounded by `end`
// (<= p.length), so a malformed CONNECT can't read OOB.
private bool mqttParseWillProps(scope const(ubyte)[] p, ref size_t i,
        ref ByteBuffer fwd, out uint willDelay) @nogc nothrow
{
    fwd.clear();
    willDelay = 0;
    uint plen;
    if (!decodeVarint(p, i, plen))
        return false;
    immutable end = i + plen;
    if (end > p.length)
        return false;
    while (i < end)
    {
        immutable propStart = i;
        immutable id = p[i++];
        bool forward = true;
        switch (id)
        {
        case 0x01: // payload-format-indicator: 1 byte
            if (i + 1 > end)
                return false;
            i += 1;
            break;
        case 0x02, 0x18: // message-expiry-interval (fwd) / will-delay-interval (not): u32
            if (i + 4 > end)
                return false;
            if (id == 0x18)
            {
                forward = false; // will-delay is consumed by the broker, not sent
                willDelay = (cast(uint) p[i] << 24) | (cast(uint) p[i + 1] << 16)
                    | (cast(uint) p[i + 2] << 8) | p[i + 3];
            }
            i += 4;
            break;
        case 0x03, 0x08, 0x09: // content-type / response-topic / correlation-data
            if (i + 2 > end)
                return false;
            immutable n = (cast(size_t) p[i] << 8) | p[i + 1];
            i += 2;
            if (i + n > end)
                return false;
            i += n;
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
            return false; // unknown will property id: malformed
        }
        if (forward)
            fwd.append(cast(const(char)[]) p[propStart .. i]);
    }
    i = end;
    return true;
}

// Copy a (broker-built, well-formed) PUBLISH property block into `out_` omitting
// any message-expiry-interval (0x02), and return the expiry seconds via `expiry`
// (0 = none). Retained storage keeps the expiry as an absolute deadline instead,
// re-emitting a DECREMENTED 0x02 on replay per [MQTT-3.3.2-5]. Bounded like the
// parser (defensive: this block is our own, but never read past its end).
private void stripExpiryProp(scope const(char)[] props, ref ByteBuffer out_,
        out uint expiry) @nogc nothrow @trusted
{
    out_.clear();
    expiry = 0;
    auto p = cast(const(ubyte)[]) props;
    size_t i = 0;
    immutable end = p.length;
    while (i < end)
    {
        immutable propStart = i;
        immutable id = p[i++];
        switch (id)
        {
        case 0x01: // payload-format-indicator: 1 byte
            if (i + 1 > end)
                return;
            i += 1;
            break;
        case 0x02: // message-expiry-interval: u32 — extracted, NOT copied
            if (i + 4 > end)
                return;
            expiry = (cast(uint) p[i] << 24) | (cast(uint) p[i + 1] << 16)
                | (cast(uint) p[i + 2] << 8) | p[i + 3];
            i += 4;
            continue; // skip the append below: 0x02 is stored as a deadline
        case 0x23: // topic-alias: u16 (should not appear in a forwardable block)
            if (i + 2 > end)
                return;
            i += 2;
            break;
        case 0x03, 0x08, 0x09: // content-type / response-topic / correlation-data
            if (i + 2 > end)
                return;
            immutable n = (cast(size_t) p[i] << 8) | p[i + 1];
            i += 2;
            if (i + n > end)
                return;
            i += n;
            break;
        case 0x0B: // subscription-identifier: varint
            uint sv;
            if (!decodeVarint(p, i, sv) || i > end)
                return;
            break;
        case 0x26: // user-property: two length-prefixed strings
            foreach (_; 0 .. 2)
            {
                if (i + 2 > end)
                    return;
                immutable n = (cast(size_t) p[i] << 8) | p[i + 1];
                i += 2;
                if (i + n > end)
                    return;
                i += n;
            }
            break;
        default:
            return; // unknown id (shouldn't happen — we built this block)
        }
        out_.append(props[propStart .. i]);
    }
}

// Build a Retained value for a SET: idup the payload, strip+store the forwardable
// props (message-expiry becomes an absolute deadline). Called only on the cold
// retained-publish path, inside the caller's try/catch.
private Retained makeRetained(scope const(char)[] payload, ulong seq,
        scope const(char)[] props, ubyte qos = 0) @trusted
{
    Retained r;
    r.payload = payload.length ? payload.idup : null;
    r.seq = seq;
    r.qos = qos;
    if (props.length)
    {
        static ByteBuffer sb;
        uint exp;
        stripExpiryProp(props, sb, exp);
        if (sb.length)
            r.props = (cast(const(char)[]) sb.data).idup;
        if (exp != 0)
        {
            r.deadline = MonoTime.currTime + dur!"seconds"(exp);
            r.hasExpiry = true;
        }
    }
    return r;
}

// CONNACK for either protocol version. v5 adds a reason code (0x00 = Success,
// 0x80+ = failure) and an (empty, for now) property block; v3 uses the legacy
// return code. `code` is already in the caller's version's encoding.
private void mqttConnack(ref ByteBuffer o, ubyte protoVer, bool sessionPresent,
        ubyte code, scope const(char)[] assignedId = null, ushort serverKeepAlive = 0) @nogc nothrow
{
    o.appendByte(cast(char)(PT_CONNACK << 4));
    if (protoVer == 5)
    {
        // properties: topic-alias-maximum (0x22, u16) always; assigned-client-
        // identifier (0x12, utf8) when the server assigned an id to an empty
        // ClientId; server-keep-alive (0x13, u16) when the server capped it.
        // assignedId is broker-generated and short (<32), so every length here
        // stays in one varint byte.
        immutable size_t idLen = assignedId.length;
        // topic-alias-maximum (0x22) + receive-maximum (0x21), each 3 bytes, always
        immutable size_t propLen = 3 + 3 + (idLen ? 3 + idLen : 0) + (serverKeepAlive ? 3 : 0);
        o.appendByte(cast(char)(2 + 1 + propLen)); // remaining length (< 127)
        o.appendByte(cast(char)(sessionPresent ? 1 : 0));
        o.appendByte(cast(char) code);
        o.appendByte(cast(char) propLen); // property length (< 127)
        o.appendByte(cast(char) 0x22); // topic-alias-maximum
        o.appendByte(cast(char)(MQTT_TOPIC_ALIAS_MAX >> 8));
        o.appendByte(cast(char)(MQTT_TOPIC_ALIAS_MAX & 0xFF));
        o.appendByte(cast(char) 0x21); // receive-maximum
        o.appendByte(cast(char)(MQTT_SERVER_RECEIVE_MAX >> 8));
        o.appendByte(cast(char)(MQTT_SERVER_RECEIVE_MAX & 0xFF));
        if (serverKeepAlive)
        {
            o.appendByte(cast(char) 0x13); // server-keep-alive
            o.appendByte(cast(char)(serverKeepAlive >> 8));
            o.appendByte(cast(char)(serverKeepAlive & 0xFF));
        }
        if (idLen)
        {
            o.appendByte(cast(char) 0x12); // assigned-client-identifier
            o.appendByte(cast(char)(idLen >> 8));
            o.appendByte(cast(char)(idLen & 0xFF));
            o.append(assignedId[0 .. idLen]);
        }
    }
    else
    {
        o.appendByte(cast(char) 2);
        o.appendByte(cast(char)(sessionPresent ? 1 : 0));
        o.appendByte(cast(char) code);
    }
}

// A v5 server-initiated DISCONNECT (type 14): tell the client WHY we are closing
// before we drop the socket, instead of a bare TCP reset. Fixed header 0xE0,
// remaining length 2 = [reason-code][property-length 0]. v3.1.1 has no server
// DISCONNECT, so callers must gate this on protoVer == 5. Common reasons:
// 0x82 Protocol Error, 0x81 Malformed Packet, 0x95 Packet Too Large.
private void mqttServerDisconnect(ref ByteBuffer o, ubyte reason) @nogc nothrow
{
    o.appendByte(cast(char)(PT_DISCONNECT << 4));
    o.appendByte(cast(char) 2); // remaining length
    o.appendByte(cast(char) reason);
    o.appendByte(cast(char) 0); // property length 0
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
            if (c.protoVer < 5 && (flags & 0x40) && !(flags & 0x80))
                return false; // [MQTT-3.1.2-22] v3.1.1 only: password requires username (v5 removed this)
            // keepalive seconds: enforce 1.5x as the read deadline [MQTT-3.1.2-24]
            if (i + 2 > p.length)
                return false; // truncated CONNECT: no keepalive field (a body
            // ending at/after the flags byte would otherwise read 1-2 bytes
            // past the packet — an unchecked OOB in a -release build, i.e. a
            // shard crash from one unauthenticated malformed CONNECT)
            immutable ka = cast(ushort)((p[i] << 8) | p[i + 1]);
            i += 2;
            // Cap the keep-alive at 60s; a client asking for more is told the
            // server's value via CONNACK ServerKeepAlive (0x13) [MQTT-3.1.2-21].
            ushort serverKa = 0;
            ushort effKa = ka;
            if (ka > MQTT_SERVER_KEEPALIVE)
            {
                effKa = MQTT_SERVER_KEEPALIVE;
                serverKa = MQTT_SERVER_KEEPALIVE;
            }
            c.readDeadline = effKa == 0 ? Duration.max : (effKa * 1500).msecs;
            // clean-start/clean-session (flags bit 1) + session-expiry govern the
            // persistent session. v3 has no expiry property: clean_session=1 means
            // discard-on-disconnect (SEI 0), clean_session=0 means never-expire.
            c.cleanStart = (flags & 0x02) != 0;
            c.sessionExpiry = c.cleanStart ? 0 : uint.max; // v3 default; v5 overrides
            // v5 CONNECT properties (session-expiry, receive-max, ...) follow the
            // keepalive. We extract receive-maximum (flow control on how many
            // QoS1/2 we may hold in flight toward this client) and correctly skip
            // the rest by type. A present receive-maximum of 0 is a protocol error
            // per [MQTT-3.3.4-9]; we treat it leniently as "no client limit".
            if (c.protoVer == 5)
            {
                ushort recvMax, aliasMax;
                uint maxPkt, sessExp;
                if (!mqttParseConnectProps(p, i, recvMax, maxPkt, aliasMax, sessExp))
                    return false;
                c.sessionExpiry = sessExp; // v5: 0 = discard on disconnect
                if (recvMax != 0)
                    c.sendMax = recvMax;
                c.maxPktSize = maxPkt; // 0 = no limit
                // cap our outbound-alias usage at what the client accepts AND our
                // own table bound (the client may advertise up to 65535)
                c.outAliasMax = aliasMax < MQTT_TOPIC_ALIAS_MAX
                    ? aliasMax : MQTT_TOPIC_ALIAS_MAX;
            }
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
                // v5: will properties precede the will topic in the payload.
                // Round-trip the forwardable ones (will-delay-interval stripped)
                // so the will PUBLISH carries content-type/user-props/etc.
                static ByteBuffer wpBuf;
                uint wDelay = 0;
                if (c.protoVer == 5 && !mqttParseWillProps(p, i, wpBuf, wDelay))
                    return false;
                c.willDelay = wDelay;
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
                    c.willProps = (c.protoVer == 5 && wpBuf.length)
                        ? (cast(const(char)[]) wpBuf.data).idup : null;
                }
                catch (Exception)
                {
                    c.willTopic = null;
                }
                c.willRetain = (flags & 0x20) != 0;
            }
            const(char)[] username;
            if (flags & 0x80)
            {
                if (!rdStr(p, i, username))
                    return false;
            }
            const(char)[] password;
            if (flags & 0x40)
            {
                if (!rdStr(p, i, password))
                    return false;
            }
            // ACL authentication: resolve the user (the `default` user when the
            // CONNECT carries no username) and verify the password. On failure,
            // CONNACK bad-user-name-or-password (v5 0x86 / v3 0x04) and drop —
            // no session is set up. A null default user means the ACL subsystem
            // isn't initialised: fall back to legacy unauthenticated behaviour.
            if (okPair)
            {
                immutable hasUser = (flags & 0x80) != 0;
                auto au = aclUser(hasUser ? username : "default");
                if (au is null && !hasUser)
                    c.aclUser = null; // ACL not initialised -> allow (legacy)
                else if (au is null || !au.enabled
                        || !aclCheckPassword(au, (flags & 0x40) ? password : ""))
                {
                    mqttConnack(o, c.protoVer, false, c.protoVer == 5 ? 0x86 : 4);
                    return false; // bad user name or password
                }
                else
                    c.aclUser = au;
            }
            // Last Will topic ACL: the will PUBLISHes on the client's behalf on an
            // abnormal disconnect, so it must clear the SAME channel ACL a live
            // PT_PUBLISH does. An unauthorized will topic is dropped (no-op when no
            // ACL is configured: c.aclUser is null -> allowed).
            if (c.willTopic.length != 0 && c.aclUser !is null
                    && !aclCanAccessChannel(c.aclUser, c.willTopic))
            {
                c.willTopic = null; // never fire an unauthorized will
                c.willPayload = null;
                c.willProps = null;
            }
            c.connected = okPair;
            if (okPair)
            {
                import core.atomic : atomicOp;

                atomicOp!"+="(gMqttClientsConnected, 1);
            }
            // [MQTT-3.1.3-6] empty ClientId on v5: the server assigns a unique one
            // and MUST return it (Assigned Client Identifier in the CONNACK). The
            // global gen counter makes it unique across shards; registered locally
            // for INFO/teardown, but with NO takeover broadcast — a fresh unique id
            // can never collide with an existing session.
            const(char)[] assignedId;
            if (c.protoVer == 5 && okPair && clientId.length == 0)
            {
                immutable g = atomicOp!"+="(gMqttConnGen, 1);
                c.connGen = g;
                char[21] buf = void;
                static immutable string hexd = "0123456789abcdef";
                buf[0 .. 5] = "auto-";
                foreach (k; 0 .. 16)
                    buf[5 + k] = hexd[(g >> ((15 - k) * 4)) & 0xF];
                try
                    c.clientId = buf[0 .. 21].idup;
                catch (Exception)
                    c.clientId = null;
                if (c.clientId.length)
                {
                    assignedId = c.clientId;
                    try
                        gLocalClients[c.clientId] = c;
                    catch (Exception)
                    {
                    }
                }
            }
            // [MQTT-3.1.4-2] takeover: a non-empty clientId displaces any
            // existing session with the same id — locally now, and on the other
            // shards via a broadcast. connGen (global monotonic) makes the
            // newest win regardless of broadcast arrival order.
            bool resumedOffline = false;
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
                    // Reconnect: a parked OFFLINE session for this id. Same shard =
                    // MIGRATE synchronously (no socket move — see mqttMigrateParked).
                    // clean_start=1 discards it. A parked session on ANOTHER shard is
                    // resumed via the freeze handshake (mqttResumeXShard).
                    MqttConn parked;
                    try
                        if (auto pc = c.clientId in gLocalClients)
                            if (*pc !is c && (*pc).offline)
                                parked = *pc;
                    catch (Exception)
                    {
                    }
                    if (parked !is null && !c.cleanStart)
                        resumedOffline = mqttMigrateParked(parked, c);
                    else if (parked !is null && c.cleanStart)
                        mqttDiscardParked(parked); // fresh session takes over
                    else if (parked is null && !c.cleanStart)
                        resumedOffline = mqttResumeXShard(c); // cross-shard resume
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
            // Persistent session: a same-shard offline session was MIGRATED above
            // (resumedOffline). Otherwise the keyspace record (mqtt.sess.<id>, Redis
            // TTL) gives cross-shard session-present. These ops hop cross-shard and
            // YIELD; o/clientId are fiber-local.
            bool sessPresent = resumedOffline;
            if (okPair && c.clientId.length != 0 && !resumedOffline)
            {
                if (c.cleanStart)
                    mqttSessionDel(c.clientId);
                else
                    sessPresent = mqttSessionExists(c.clientId);
            }
            // CONNACK: reason/rc 0 on success, else unacceptable-protocol-version.
            // assignedId is non-empty only for an empty-ClientId v5 client.
            mqttConnack(o, c.protoVer, sessPresent, okPair ? 0 : 1, assignedId, serverKa);
            // a migrated session's queued messages go out right after the CONNACK,
            // in the same response buffer (ordering guaranteed, no writer race)
            if (resumedOffline && !c.obox.empty)
            {
                o.append(c.obox.data);
                c.obox.clear();
            }
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
            // ACL: a publish to a channel the user can't access is dropped the same
            // way (drop-but-ack) so an unauthorized device can't inject messages.
            immutable clientReserved = (topic.length != 0 && topic[0] == '$')
                || (c.aclUser !is null && !aclCanAccessChannel(c.aclUser, topic));
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
                    // Receive Maximum [MQTT-4.9]: at most MQTT_SERVER_RECEIVE_MAX
                    // concurrent unacked inbound QoS2. The (max+1)th is a protocol
                    // error -> server DISCONNECT 0x93 (set the reason; the serve
                    // loop emits it + lingers on the false return).
                    if (c.q2pids.length >= MQTT_SERVER_RECEIVE_MAX)
                    {
                        c.discReason = 0x93; // Receive Maximum exceeded
                        return false;
                    }
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
            {
                c.inflight.remove(pid);
                c.inflightMsg.remove(pid);
            }
            catch (Exception)
            {
            }
            mqttReleaseHeld(c); // a QoS1 slot freed -> release a held delivery
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
                c.inflightMsg.remove(pid); // PUBLISH done; redeliver PUBREL instead
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
            mqttReleaseHeld(c); // a QoS2 slot freed -> release a held delivery
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
            // after the packet-id. The subscription-identifier is echoed on every
            // PUBLISH this SUBSCRIBE's filters deliver, so the client can tell
            // which subscription matched.
            uint subId = 0;
            if (v5 && !mqttParseSubProps(p, i, subId))
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
                        // [MQTT-3.8.3-4] no-local (bit 2) on a SHARED subscription
                        // is a Protocol Error — reject this filter (the round-robin
                        // also falls through an ineligible member as a safety net).
                        if (optByte & 0x04)
                        {
                            granted[ng] = 0x80;
                            retainOk[ng] = false;
                            filters[ng++] = null;
                            continue;
                        }
                    }
                }
                if (!mqttValidFilter(actualFilter) || c.filters.length >= MQTT_MAX_SUBS)
                {
                    granted[ng] = 0x80; // unusable filter / sub-cap: failure
                    retainOk[ng] = false;
                    filters[ng++] = null;
                    continue;
                }
                // ACL: authorize the filter as a CHANNEL. A denied filter gets a
                // SUBACK failure (0x80) and is NOT subscribed — the reason code the
                // MQTT conformance suite expects for an unauthorized subscription
                // in both v3.1.1 and v5. A wildcard filter matches ACL patterns
                // exactly; a literal one is glob-matched (so `&!pattern` applies).
                if (c.aclUser !is null
                    && !aclCanAccessChannel(c.aclUser, actualFilter,
                            mqttHasWildcard(actualFilter)))
                {
                    granted[ng] = 0x80;
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
                        cast(ubyte)(v5 ? optByte : 0), sg, subId);
                if (!isNew)
                    c.filters.length = c.filters.length - 1; // replaced: no new entry
                else // keep subInfo index-aligned with filters (for session resume)
                    try
                        c.subInfo ~= SubInfo(grant, cast(ubyte)(v5 ? optByte : 0), sg, subId);
                    catch (Exception)
                        c.filters.length = c.filters.length - 1; // drop to stay aligned
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
            immutable nowRt = MonoTime.currTime; // one clock read for the scan
            const(char)[][32] expiredBuf; // expired topics to evict after the loop
            size_t nExpired = 0;
            try
                foreach (topic, r; gRetained)
                {
                    if (r.payload.length == 0)
                        continue; // tombstone (deleted retained): never delivered
                    // v5 message-expiry: a retained message past its deadline is
                    // dropped, not delivered [MQTT-3.3.2-5], and evicted below.
                    if (r.hasExpiry && nowRt >= r.deadline)
                    {
                        if (nExpired < expiredBuf.length)
                            expiredBuf[nExpired++] = topic;
                        continue;
                    }
                    if (o.length > MQTT_OBOX_CAP)
                        break; // bounded replay burst (retained is QoS0/best-effort)
                    foreach (fi, f; filters[0 .. ng])
                        if (f !is null && retainOk[fi] && mqttFilterMatches(f, topic))
                        {
                            // v5 outgoing props: the subscription-identifier
                            // (retained delivered at SUBSCRIBE time also carries
                            // it) then a DECREMENTED message-expiry, ahead of the
                            // stored (expiry-stripped) props.
                            const(char)[] outProps = r.props;
                            if (v5 && (subId != 0 || r.hasExpiry))
                            {
                                static ByteBuffer pb;
                                pb.clear();
                                if (subId != 0)
                                {
                                    pb.appendByte(cast(char) 0x0B);
                                    encodeVarint(pb, subId);
                                }
                                if (r.hasExpiry)
                                {
                                    immutable long remS = (r.deadline - nowRt).total!"seconds";
                                    immutable uint rem = remS <= 0 ? 0
                                        : (remS > uint.max ? uint.max : cast(uint) remS);
                                    pb.appendByte(cast(char) 0x02);
                                    pb.appendByte(cast(char)(rem >> 24));
                                    pb.appendByte(cast(char)(rem >> 16));
                                    pb.appendByte(cast(char)(rem >> 8));
                                    pb.appendByte(cast(char) rem);
                                }
                                pb.append(r.props);
                                outProps = cast(const(char)[]) pb.data;
                            }
                            // retained delivered at min(msg QoS, granted QoS)
                            // [MQTT-3.8.4]. QoS1/2 needs a packet-id tracked in
                            // flight (PUBACK/PUBREC clears it); a saturated
                            // receive-maximum window degrades to QoS0.
                            immutable ubyte effQos = r.qos < granted[fi] ? r.qos : granted[fi];
                            ushort rpid = 0;
                            if (effQos >= 1)
                            {
                                immutable size_t win = c.sendMax < MQTT_QOS1_WINDOW
                                    ? c.sendMax : MQTT_QOS1_WINDOW;
                                if (c.inflight.length + c.outQos2.length < win)
                                    rpid = nextDeliveryPid(c);
                            }
                            if (rpid == 0)
                                buildPublish(o, topic, r.payload, true, v5, outProps);
                            else if (effQos == 2)
                            {
                                buildPublishQos2(o, topic, r.payload, true, rpid, v5, outProps);
                                try
                                    c.outQos2[rpid] = 1; // awaiting PUBREC
                                catch (Exception)
                                {
                                }
                            }
                            else
                            {
                                buildPublishQos1(o, topic, r.payload, true, rpid, v5, outProps);
                                try
                                    c.inflight[rpid] = true;
                                catch (Exception)
                                {
                                }
                            }
                            break;
                        }
                }
            catch (Exception)
            {
            }
            // evict the expired retained topics discovered during the scan (done
            // after iterating so we don't mutate the AA mid-foreach)
            foreach (t; expiredBuf[0 .. nExpired])
            {
                try
                {
                    if (auto rr = t in gRetained)
                    {
                        tRetainedBytes -= rr.payload.length + rr.props.length;
                        gRetained.remove(cast(string) t);
                    }
                }
                catch (Exception)
                {
                }
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
                        if (idx < c.subInfo.length) // keep subInfo index-aligned
                        {
                            c.subInfo[idx] = c.subInfo[$ - 1];
                            c.subInfo.length = c.subInfo.length - 1;
                        }
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
        // A v5 DISCONNECT may override the session-expiry-interval, changing how
        // long the persistent session is kept [MQTT-3.1.2-23]. The cleanup path
        // reads c.sessionExpiry to persist/discard the session record.
        if (c.protoVer == 5)
        {
            uint sei;
            bool hasSei;
            if (mqttDisconnectSEI(p, sei, hasSei) && hasSei)
                c.sessionExpiry = sei;
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
