module dreads.amqp;

import dreads.tls : TlsLeg, legPump, legTake, legDrainInto, legSend;

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
import dreads.stream : nowMs;
import dreads.alloc : MAX_SHARDS;

// ---------------------------------------------------------------------------
// Hooks installed by server.d (avoid an import cycle): queue data-plane ops
// and control-plane replication.
public __gshared void delegate(scope const(char)[] key, scope const(char)[] payload) nothrow gAmqpPush;
/// FIRE-AND-FORGET live publish: a local key applies inline; a remote key is
/// enqueued into the owner shard's SPSC ring and we return immediately. No ack,
/// no wait — the ring guarantees FIFO delivery in-process, so "enqueued" is a
/// binding promise the RPUSH will be applied, as durable as the everysec-AOF
/// confirm already is. This is the cheap replacement for the generic RESP hop:
/// one cross-thread wake, forward only, no return trip.
public __gshared void delegate(scope const(char)[] key, scope const(char)[] payload) nothrow gAmqpPushStage;
public __gshared void delegate(scope const(char)[] key, scope const(char)[] payload) nothrow gAmqpPushFront;

/// Return the UNDELIVERED tail of a prefetched batch to the head of its queue.
///
/// `gAmqpPopN` takes records OFF the list, so between the pop and the delivery
/// the consumer fiber is the only holder. Every way that fiber can end —
/// basic.cancel, channel close, channel reopened under a new generation, the
/// queue being deleted, the connection closing — must put the tail back or those
/// records are gone: off the queue, never delivered, never requeued. (The
/// single-pop burst this replaced held at most one record, and pushed it back.)
///
/// Not marked redelivered: these were never put on the wire.
private void returnBatchRemainder(scope const(char)[] key, ref ByteBuffer batch,
        ref size_t batchPos, ref int batchLeft) nothrow @trusted
{
    if (batchLeft <= 0)
    {
        batchLeft = 0;
        return;
    }
    immutable(ubyte)[][] rest;
    auto bd = batch.data;
    size_t p = batchPos;
    while (batchLeft > 0)
    {
        batchLeft--;
        if (p + 2 > bd.length || bd[p] != '$')
            break;
        size_t i = p + 1;
        immutable neg = i < bd.length && bd[i] == '-';
        size_t blen = 0;
        if (neg)
            i++;
        while (i < bd.length && bd[i] != '\r')
        {
            blen = blen * 10 + (bd[i] - '0');
            i++;
        }
        i += 2; // \r\n
        if (neg)
        {
            p = i; // nil element: nothing to give back
            continue;
        }
        if (i + blen > bd.length)
            break;
        try
            rest ~= bd[i .. i + blen].idup; // copy: the pushes below yield
        catch (Exception)
            break;
        p = i + blen + 2;
    }
    batchLeft = 0;
    batchPos = 0;
    if (gAmqpPushFront is null)
        return;
    // REVERSE: each push lands at the head, so pushing last-first restores the
    // original order at the front of the queue.
    foreach_reverse (rec; rest)
        gAmqpPushFront(key, cast(const(char)[]) rec);
}
public __gshared bool delegate(scope const(char)[] key, ref ByteBuffer outPayload) nothrow gAmqpPop;
/// BATCHED pop: takes up to `count` messages in ONE core exec (`LPOP key n`).
/// The consumer burst used to call gAmqpPop per message — each of those is a
/// full cross-shard round-trip when the queue is owned elsewhere, so a 64-deep
/// burst cost 64 hops. That per-message round-trip is the USL crosstalk term
/// that made s4 SLOWER than s1 (retrograde scaling). One hop per burst instead.
/// Fills `outRaw` with the raw RESP multi-bulk reply, which the CALLER owns —
/// it must survive the yields inside the burst (deadLetter hops), so it cannot
/// be a shared TLS buffer. Returns the element count (0 = queue empty).
public __gshared int delegate(scope const(char)[] key, uint count,
        ref ByteBuffer outRaw) nothrow gAmqpPopN;
public __gshared long delegate(scope const(char)[] key) nothrow gAmqpLen;
public __gshared void delegate(scope const(char)[] key) nothrow gAmqpDelKey;
/// Non-destructive head read (LINDEX key 0) for the active-TTL reaper; false =
/// empty queue. Installed by server.d.
public __gshared bool delegate(scope const(char)[] key, ref ByteBuffer outHead) nothrow gAmqpPeekHead;
/// Positional LINDEX peek (AMQP 1.0 stream consumers read non-destructively).
public __gshared bool delegate(scope const(char)[] key, long index, ref ByteBuffer outPayload) nothrow gAmqpPeekAt;
/// Does THIS shard thread own `key`? Only the owner reaps a queue (so peek+pop
/// stay self-shard and yield-free, and no queue is swept N times). Installed by
/// server.d.
public __gshared bool delegate(scope const(char)[] key) nothrow gAmqpOwns;
/// Flush THIS shard-thread's AOF pending buffer to the OS (installed by
/// server.d). Skins call it once per network batch so a confirmed publish is
/// durable before the ack/confirm reaches the client — matching how the RESP
/// serve loop flushes per batch. Without it, skin writes sat in the in-memory
/// pending buffer until the everysec tick, so a kill -9 lost up to a second of
/// ALREADY-ACKED messages.
public __gshared void delegate() nothrow gAmqpAofFlush;
public __gshared void delegate(scope const(ubyte)[] ctl) nothrow gAmqpCtlFanout;

public shared long gAmqpConsumers; // gate: publish-side wake fan-out etc (future)

// PER-SHARD publish/return counters. A single `shared ulong` incremented on every
// basic.publish bounced its cache line across every core — a write-contended
// global that is an Amdahl serial fraction (it flattened per-shard AMQP scaling).
// Each shard now bumps ONLY its own slot (plain ++, no atomic), cache-line padded
// so slots never false-share; the dashboard/mgmt readers (off the hot path) sum
// the slots. Total msgs published = sum(gAmqpMsgShard[*].v).
private struct PadU64
{
    ulong v;
    ulong[7] _pad; // fill the 64-byte line so neighbours don't false-share
}

private __gshared PadU64[MAX_SHARDS] gAmqpMsgShard;
private __gshared PadU64[MAX_SHARDS] gAmqpRetShard;

pragma(inline, true)
private void amqpCountPub() @trusted nothrow @nogc
{
    import dreads.shard : tShard;

    ++gAmqpMsgShard[tShard].v; // single writer per slot: no atomic needed
}

pragma(inline, true)
private void amqpCountRet() @trusted nothrow @nogc
{
    import dreads.shard : tShard;

    ++gAmqpRetShard[tShard].v;
}

/// Cumulative basic.publish records routed, summed across shards (off hot path).
public ulong amqpPubTotal() @trusted nothrow @nogc
{
    ulong s = 0;
    foreach (ref c; gAmqpMsgShard)
        s += c.v;
    return s;
}

/// Cumulative mandatory publishes returned (no route), summed across shards.
public ulong amqpRetTotal() @trusted nothrow @nogc
{
    ulong s = 0;
    foreach (ref c; gAmqpRetShard)
        s += c.v;
    return s;
}

// --- Management API message-rate sampler (M4) -------------------------------
// The counters above are cumulative; the RMQ management API also wants an
// instantaneous publish rate (msgs/sec). A timer (server boot, when the mgmt
// API is on) calls amqpSampleRates() every few seconds; mgmt reads the total
// plus the last-interval rate. Cheap: two sums + a subtraction per tick.
private __gshared ulong gAmqpRatePrevPub;
private __gshared ulong gAmqpRatePrevRet;
private __gshared long gAmqpRatePrevMs;
private __gshared double gAmqpPubRate = 0;
private __gshared double gAmqpRetRate = 0;

public void amqpSampleRates() nothrow @trusted
{
    immutable now = nowMs();
    immutable pub = amqpPubTotal();
    immutable ret = amqpRetTotal();
    if (gAmqpRatePrevMs != 0)
    {
        immutable dt = now - gAmqpRatePrevMs;
        if (dt > 0)
        {
            immutable secs = cast(double) dt / 1000.0;
            gAmqpPubRate = cast(double)(pub - gAmqpRatePrevPub) / secs;
            gAmqpRetRate = cast(double)(ret - gAmqpRatePrevRet) / secs;
        }
    }
    gAmqpRatePrevPub = pub;
    gAmqpRatePrevRet = ret;
    gAmqpRatePrevMs = now;
}

/// Management API: cumulative publish/return totals + their last-interval rate.
public void amqpMessageStats(out ulong pubTotal, out double pubRate,
        out ulong retTotal, out double retRate) nothrow @trusted
{
    pubTotal = amqpPubTotal();
    retTotal = amqpRetTotal();
    pubRate = gAmqpPubRate;
    retRate = gAmqpRetRate;
}
public shared ulong gAmqpBindingDrops; // duplicate/over-cap bindings refused
private shared ulong gAmqpQueueGen; // counter for server-generated queue names

/// Advertised frame-max (connection.tune). A frame larger than this is a
/// framing error: refuse instead of buffering an attacker-chosen u32 of bytes.
enum uint AMQP_FRAME_MAX = 131072;
/// Hard cap on a single message body (content-header bodySize is a u64).
enum ulong AMQP_MAX_BODY = 128UL << 20;
/// Default per-connection unacked window when the client never sets basic.qos:
/// bounds the RAM a consumer that never acks can pin (prefetch DoS).
enum size_t AMQP_DEFAULT_PREFETCH = 20000;
/// Bindings per exchange, per shard (each is replicated to every thread).
enum size_t AMQP_MAX_BINDINGS = 4096;
/// Per-shard caps on control-plane cardinality (each is replicated to every
/// thread, so an uncapped declare is a 1->N memory amplification DoS).
enum size_t AMQP_MAX_EXCHANGES = 65536;
enum size_t AMQP_MAX_QUEUEMETA = 65536;
/// Per-connection caps.
enum size_t AMQP_MAX_CHANNELS = 2047;   // matches the advertised channel-max
/// Per-connection cap on the SUM of in-progress content-body assemblies across
/// all channels. Without it, 2047 channels each mid-publish with a 128MB
/// bodySize whose final body frame is withheld pin ~256GB. 256MB clears a legit
/// single max body (AMQP_MAX_BODY=128MB) with headroom while bounding the flood.
enum size_t AMQP_MAX_PENDING_BYTES = 256UL << 20;
enum size_t AMQP_MAX_CONSUMERS = 4096;
/// Bytes of stacked deliveries that force a socket write while the queue is
/// still hot. The consumer otherwise flushes only when the queue runs dry, so
/// this is a memory bound on a deep backlog, not a batch size.
enum size_t AMQP_CONSUMER_FLUSH_BYTES = 256 * 1024;
/// A dead-letter is dropped once it has been dead-lettered this many times
/// (the x-death hop count) — bounds an A->X->A dead-letter cycle.
// Backstop only: PURE-automatic dead-letter loops (TTL->DLX->...->same queue,
// no client rejection anywhere in the history) are detected and dropped in
// deadLetter() itself, RabbitMQ-style. Rejection-driven cycles legitimately
// live for many hops (rabbit loops them indefinitely), so the cap is generous.
enum int AMQP_MAX_DEATHS = 4096;
public shared ulong gAmqpCtlDrops; // control-plane declares refused at a cap

/// Queue key namespace: visible from RESP on purpose (cross-protocol is a
/// feature — `LRANGE amq.q.tasks 0 -1` shows the queue).
public void queueKey(scope const(char)[] q, ref ByteBuffer o) @nogc nothrow
{
    o.clear();
    o.append("amq.q.");
    o.append(q);
}

/// The backing list for one priority LEVEL of a queue. Level 0 IS the queue's
/// own key, so a queue without x-max-priority is byte-identical to what it was
/// and no other call site has to know about levels.
private void queueKeyPrio(scope const(char)[] q, uint lvl, ref ByteBuffer o) @nogc nothrow
{
    queueKey(q, o);
    if (lvl == 0)
        return;
    // FIXED THREE DIGITS. x-max-priority goes to 255, so ".p1" and ".p10" would
    // order lexicographically as p1 < p10 < p2 — wrong for anything that scans
    // or lists these keys (a KEYS sweep, the dashboard, a human reading a dump).
    // Zero-padding makes lexicographic order match numeric order across the
    // whole legal range, and keeps every level key the same width.
    o.append(".p");
    char[3] nb = void;
    nb[0] = cast(char)('0' + (lvl / 100) % 10);
    nb[1] = cast(char)('0' + (lvl / 10) % 10);
    nb[2] = cast(char)('0' + lvl % 10);
    o.append(nb[]);
}

/// Build the key of the HIGHEST non-empty priority level of `q` into `o`, and
/// return that level. Falls back to level 0 (the queue's own key) when every
/// level is empty, so a caller can always just read from what it gets back.
/// Costs one LLEN per level above 0 that is empty; a plain FIFO queue (mp == 0)
/// pays nothing and takes the same path it always did.
private uint queueKeyRead(scope const(char)[] q, ref ByteBuffer o) nothrow @trusted
{
    immutable mp = queueMaxPrio(q);
    if (mp == 0 || gAmqpLen is null)
    {
        queueKey(q, o);
        return 0;
    }
    for (uint lvl = mp; lvl > 0; lvl--)
    {
        queueKeyPrio(q, lvl, o);
        if (gAmqpLen(o.data.asChars) > 0)
            return lvl;
    }
    queueKey(q, o);
    return 0;
}

/// Total depth across every priority level (the number a client must see).
private long queueDepth(scope const(char)[] q) nothrow @trusted
{
    if (gAmqpLen is null)
        return 0;
    static ByteBuffer kdq; // TLS: consumed immediately, no yield
    immutable mp = queueMaxPrio(q);
    long tot = 0;
    foreach (lvl; 0 .. mp + 1)
    {
        queueKeyPrio(q, lvl, kdq);
        immutable n = gAmqpLen(kdq.data.asChars);
        if (n > 0)
            tot += n;
    }
    return tot;
}

/// Apply `fn` to every backing list of `q` (just its own key when it is a plain
/// FIFO queue). Used by purge and delete: a priority queue that dropped only
/// level 0 would leak every higher-priority message it still held.
private void queueEachLevel(scope const(char)[] q,
        scope void delegate(scope const(char)[] key) nothrow fn) nothrow @trusted
{
    static ByteBuffer kel; // TLS
    immutable mp = queueMaxPrio(q);
    foreach (lvl; 0 .. mp + 1)
    {
        queueKeyPrio(q, lvl, kel);
        fn(kel.data.asChars);
    }
}

/// Which level of `q` a stored record belongs to (0 for a plain FIFO queue).
private uint recordPrio(scope const(char)[] q, scope const(ubyte)[] blob) nothrow @trusted
{
    immutable mp = queueMaxPrio(q);
    if (mp == 0)
        return 0;
    long pm;
    int dth;
    const(char)[] rk;
    const(ubyte)[] pr, bd;
    splitRecord(blob, pm, dth, rk, pr, bd);
    return propsPriority(pr, mp);
}

/// x-max-priority for a queue, 0 when it is a plain FIFO queue.
private uint queueMaxPrio(scope const(char)[] q) nothrow @trusted
{
    try
        if (auto m = cast(string) q in gQueueMeta)
            return m.maxPrio;
    catch (Exception)
    {
    }
    return 0;
}

/// The `priority` basic property, clamped to the queue's ceiling. Walks the
/// properties that precede it in flags order, like propsExpiration does:
/// content-type, content-encoding, headers (field table), delivery-mode.
package uint propsPriority(scope const(ubyte)[] props, uint cap) @nogc nothrow
{
    if (props.length < 2)
        return 0;
    immutable flags = (cast(ushort) props[0] << 8) | props[1];
    size_t i = 2;
    static bool skipShort(scope const(ubyte)[] p, ref size_t j) @nogc nothrow
    {
        if (j + 1 > p.length)
            return false;
        immutable n = p[j];
        j += 1 + n;
        return j <= p.length;
    }

    if (flags & 0x8000) // content-type
        if (!skipShort(props, i))
            return 0;
    if (flags & 0x4000) // content-encoding
        if (!skipShort(props, i))
            return 0;
    if (flags & 0x2000) // headers: long field table
    {
        if (i + 4 > props.length)
            return 0;
        immutable tl = (cast(size_t) props[i] << 24) | (cast(size_t) props[i + 1] << 16)
            | (cast(size_t) props[i + 2] << 8) | props[i + 3];
        i += 4 + tl;
        if (i > props.length)
            return 0;
    }
    if (flags & 0x1000) // delivery-mode: octet
        i += 1;
    if (!(flags & 0x0800)) // no priority property
        return 0;
    if (i + 1 > props.length)
        return 0;
    immutable pr = props[i];
    return pr > cap ? cap : pr;
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

/// Global monotonic sequence stamped on every control-plane op. The ctl maps
/// are broadcast-replicated across shards over INDEPENDENT SPSC lanes, so a
/// bind racing an unbind (or a declare racing a delete) from two origin shards
/// arrives in different orders at different shards. An LWW-element-set
/// (per-element seq + tombstone) converges regardless of arrival order: apply
/// an op only when its seq beats the element's stored seq.
public shared ulong gAmqpCtlSeq;

private struct Binding
{
    string queue; // destination: a queue name, or an exchange name if toExchange
    string key; // binding/routing key
    immutable(ubyte)[] args; // raw binding-arguments table (headers exchanges)
    ulong seq; // ctl seq of the last bind/unbind on this element
    bool alive = true; // false = tombstone (unbound), retained for seq-gating
    bool toExchange; // exchange-to-exchange binding: route recurses into `queue`
}

private struct QueueMeta
{
    string dlx; // x-dead-letter-exchange (meaningful only when dlxSet; "" = the DEFAULT exchange)
    string dlrk; // x-dead-letter-routing-key ("" = original queue name)
    long ttlMs; // x-message-ttl (meaningful only when ttlSet; 0 = expire immediately)
    long maxLen; // x-max-length ENCODED +1 (0 = unset, N+1 = bound N); overflow drops the HEAD
    bool dlxSet; // x-dead-letter-exchange arg present ("" alone can't tell: it names the default exchange)
    bool ttlSet; // x-message-ttl arg present (0 is a VALID immediate-expiry TTL)
    long expMs; // x-expires: the queue's unused-lease in ms (meaningful when expSet)
    bool expSet;
    /// x-overflow: 0 = drop-head (the AMQP default and what this broker always
    /// did), 1 = reject-publish. RabbitMQ 4 and LavinMQ both enforce it; a full
    /// queue then refuses the message instead of evicting its head, and a
    /// publisher in confirm mode gets basic.nack rather than basic.ack.
    ubyte overflow;
    /// x-delivery-limit ENCODED +1 (0 = unset, N+1 = limit N). A message that
    /// has been delivered and requeued N times is dead-lettered instead of
    /// requeued again — the poison-message brake LavinMQ has and this broker
    /// did not, which without it loops such a message forever.
    long delLimit;
    /// x-max-priority: 0 = a plain FIFO queue (and every key stays exactly where
    /// it was); N > 0 = a priority queue backed by N+1 lists, level 0 at the
    /// queue's own key and levels 1..N at `<key>.p<level>`. That is how RabbitMQ
    /// implements them too, and it keeps non-priority queues untouched.
    ubyte maxPrio;
}

/// x-expires lease deadlines (nowMs-based), shard-local. Stamped by the op-3
/// meta apply, the op-11 touch (declares/gets), and a consumer count hitting
/// zero; only the shard OWNING the queue's key acts on an expired lease.
private long[string] gQueueLease; // TLS
/// Replicated live-consumer counts (op 12 inc / op 13 dec): the x-expires
/// sweep must see consumers on EVERY shard, unlike the shard-local
/// gQueueConsumers that feeds declare-ok.
private int[string] gQueueConsGlobal; // TLS

private QueueMeta[string] gQueueMeta; // TLS, broadcast-replicated

private ExType[string] gExchanges; // TLS

/// Management API hook (M4): serialize this thread's replicated exchange set as
/// `name\ttype\n` lines (skips the default amq.* names the API adds itself).
/// Runs ON an AMQP shard thread (gExchanges is TLS) — installed at boot.
public void amqpExchangeSnapshot(ref ByteBuffer o) nothrow @trusted
{
    static immutable string[4] tn = ["direct", "fanout", "topic", "headers"];
    foreach (name, t; gExchanges)
    {
        if (name.length == 0)
            continue;
        if (name.length >= 4 && name[0 .. 4] == "amq.")
            continue; // defaults are emitted by the API
        o.append(name);
        o.appendByte('\t');
        o.append(tn[cast(ubyte) t]);
        o.appendByte('\n');
    }
}

/// Management API hook (M4): serialize the replicated binding set as
/// `source\tdestination\tdest_kind\trouting_key\n` lines (dest_kind = q|e),
/// skipping tombstones (unbound elements kept for seq-gating). Runs ON an AMQP
/// shard thread (gBindings is TLS+replicated) — installed at boot.
public void amqpBindingsSnapshot(ref ByteBuffer o) nothrow @trusted
{
    foreach (exch, ref lst; gBindings)
        foreach (ref b; lst)
        {
            if (!b.alive)
                continue; // tombstone
            o.append(exch);
            o.appendByte('\t');
            o.append(b.queue);
            o.appendByte('\t');
            o.appendByte(b.toExchange ? 'e' : 'q');
            o.appendByte('\t');
            o.append(b.key);
            o.appendByte('\n');
        }
}
private string[string] gExchangeAE; // TLS: exchange -> alternate-exchange (replicated via op-1)
/// Last op seq per exchange NAME. A deleted exchange is removed from gExchanges
/// but keeps its seq here (tombstone) so a stale lower-seq declare is rejected.
private ulong[string] gExchangeSeq; // TLS
// Declared-queue existence set (LWW-element-set), mirroring the exchange
// registry: op 8 declares a queue name, op 9 tombstones it. gQueueSeq keeps a
// per-name seq so a stale declare can't resurrect a deleted queue. This is what
// backs the passive-declare / basic.consume NOT_FOUND (404) checks that
// RabbitMQ clients rely on to probe existence.
private bool[string] gQueues; // TLS: present => queue currently exists
// Exclusive-queue ownership: queue -> owning connection id (ctl op 10 claims,
// op 9 clears with the queue). An access from any OTHER connection is a 405
// RESOURCE_LOCKED channel error; the owner's teardown deletes the queue.
private ulong[string] gQueueOwner; // TLS, broadcast-replicated
// Declare flags (durable=2 | exclusive=4 | auto-delete=8), for redeclare
// equivalence (406 on mismatch) and auto-delete-on-last-cancel. Replicated
// with the existence set (op 8 carries them; op 9 clears).
private ubyte[string] gQueueFlags; // TLS
private ubyte[string] gExchFlags; // TLS (durable=2 | auto-delete=4 | internal=8)
private shared ulong gAmqpConnGen; // atomic source for AmqpConn.connId
private ulong[string] gQueueSeq; // TLS: per-name LWW seq (tombstones survive delete)
// Live consumer count per queue, shard-local (a consumer and the queue.declare
// that reports it share a connection, hence a shard, in the common case). Feeds
// the consumer_count field of queue.declare-ok. Cross-shard consumers are not
// summed here — an accepted best-effort, matching how RabbitMQ treats the field.
private uint[string] gQueueConsumers; // TLS
// x-priority per live consumer (shard-local): only consumers at the queue's
// MAX live priority pop; lower ones idle until the higher cancel/exit.
private int[][string] gQueuePrios; // TLS

private void qPrioAdd(string q, int p) nothrow @trusted
{
    try
        gQueuePrios[q] ~= p;
    catch (Exception)
    {
    }
}

private void qPrioRemove(string q, int p) nothrow @trusted
{
    try
        if (auto pl = q in gQueuePrios)
            foreach (i2, v; *pl)
                if (v == p)
                {
                    *pl = (*pl)[0 .. i2] ~ (*pl)[i2 + 1 .. $];
                    if ((*pl).length == 0)
                        gQueuePrios.remove(q);
                    return;
                }
    catch (Exception)
    {
    }
}

private int qPrioMax(string q) nothrow @trusted
{
    int mx = int.min;
    try
        if (auto pl = q in gQueuePrios)
            foreach (v; *pl)
                if (v > mx)
                    mx = v;
    catch (Exception)
    {
    }
    return mx;
}
private Binding[][string] gBindings; // TLS: exchange -> bindings

/// The AMQP 0-9-1 default exchanges exist on every vhost with NO explicit
/// declare (the nameless "" exchange is handled specially in routeTo). Without
/// this, publishing to amq.topic/amq.direct/... found no gExchanges entry and
/// silently dropped every message. gExchanges is TLS, so seed each shard thread
/// once; routeTo only runs on a thread that has an AMQP connection (which ran
/// this), so per-connection-thread seeding covers every routing path. Not
/// broadcast — every shard seeds the SAME fixed types, so no divergence.
private void seedWellKnownExchanges() nothrow
{
    static bool seeded;
    if (seeded)
        return;
    seeded = true;
    // Seed a default only if it has NO control-plane history: a name present in
    // gExchangeSeq was either declared or DELETED (op 1/5), and those ops are
    // broadcast+seq-gated across shards. Seeding a name that a broadcast delete
    // tombstoned (on a shard that seeds AFTER receiving the delete — the ctl apply
    // runs without any AMQP connection) would resurrect it on that shard only =
    // cross-shard split-brain. Deferring to gExchangeSeq keeps every shard in sync.
    void seed(string name, ExType t) nothrow
    {
        try
            if (name !in gExchanges && name !in gExchangeSeq)
                gExchanges[name] = t;
        catch (Exception)
        {
        }
    }

    seed("amq.direct", ExType.direct);
    seed("amq.fanout", ExType.fanout);
    seed("amq.topic", ExType.topic);
    seed("amq.headers", ExType.headers);
    seed("amq.match", ExType.headers); // amq.match is the headers-exchange alias
}

/// AMQP topic match: dot-separined; `*` = exactly one word, `#` = zero+ words.
package bool amqpTopicMatches(scope const(char)[] pattern, scope const(char)[] key) @nogc nothrow
{
    // Segment both keys on '.', then match with the CLASSIC linear wildcard
    // algorithm using a single backtrack pointer: '#' behaves like glob '*'
    // (matches zero-or-more words), '*' matches exactly one word. This is
    // O(nPat * nKey) — the previous recursive matcher backtracked EXPONENTIALLY
    // on a multi-'#' binding key (e.g. "#.#.#.#…"), letting one publish freeze
    // the shard event loop. Segment counts are capped (real routing/binding keys
    // have a handful of words); an absurd one simply does not match.
    enum size_t MAXSEG = 128;
    size_t[MAXSEG + 1] pOff = void, kOff = void; // seg start offsets (+ end sentinel)
    size_t nPat = 0, nKey = 0;
    {
        size_t s = 0;
        foreach (i, ch; pattern)
            if (ch == '.')
            {
                if (nPat >= MAXSEG)
                    return false;
                pOff[nPat++] = s;
                s = i + 1;
            }
        if (nPat >= MAXSEG)
            return false;
        pOff[nPat++] = s;
        pOff[nPat] = pattern.length + 1; // sentinel: last seg end = len
    }
    {
        size_t s = 0;
        foreach (i, ch; key)
            if (ch == '.')
            {
                if (nKey >= MAXSEG)
                    return false;
                kOff[nKey++] = s;
                s = i + 1;
            }
        if (nKey >= MAXSEG)
            return false;
        kOff[nKey++] = s;
        kOff[nKey] = key.length + 1;
    }
    size_t pp = 0, kk = 0;
    long starPat = -1; // pattern index of the most recent '#'
    size_t starKey = 0; // key word to resume from on backtrack
    while (kk < nKey)
    {
        auto kseg = key[kOff[kk] .. kOff[kk + 1] - 1];
        if (pp < nPat)
        {
            auto pseg = pattern[pOff[pp] .. pOff[pp + 1] - 1];
            if (pseg == "*" || pseg == kseg)
            {
                pp++;
                kk++;
                continue;
            }
            if (pseg == "#")
            {
                starPat = cast(long) pp; // record '#'; try it matching zero words
                starKey = kk;
                pp++;
                continue;
            }
        }
        if (starPat >= 0)
        {
            pp = cast(size_t) starPat + 1; // let the last '#' absorb one more word
            starKey++;
            kk = starKey;
            continue;
        }
        return false;
    }
    while (pp < nPat && pattern[pOff[pp] .. pOff[pp + 1] - 1] == "#")
        pp++; // trailing '#'s match zero words
    return pp == nPat;
}

// Apply a control op locally. Wire: [op u8][len u16][exchange][len u16][a][len u16][b]
//   op 1 = exchange.declare (a = type name)
//   op 2 = queue.bind       (a = queue, b = routing key)
/// Topology epoch: bumped on EVERY applied ctl op. The publish hot path
/// memoizes its per-queue lookups (existence + max-length) against it, so a
/// run of publishes to the same queue pays ONE relaxed atomic load instead of
/// two AA probes per message.
package shared ulong gAmqpTopoEpoch;

public void amqpApplyCtl(scope const(ubyte)[] p) nothrow @trusted
{
    {
        import core.atomic : atomicOp;

        atomicOp!"+="(gAmqpTopoEpoch, 1);
    }
    try
    {
        size_t i = 0;
        if (p.length < 9)
            return;
        immutable op = p[i++];
        ulong seq = 0; // global ctl order (LWW-element-set)
        foreach (k; 0 .. 8)
            seq = (seq << 8) | p[i + k];
        i += 8;
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
            if (auto sp = (cast(string) ex) in gExchangeSeq)
                if (seq <= *sp)
                    return; // stale vs a newer declare/delete on this name
            if ((cast(string) ex) !in gExchanges && gExchanges.length >= AMQP_MAX_EXCHANGES)
            {
                atomicOp!"+="(gAmqpCtlDrops, 1);
                return;
            }
            gExchanges[ex] = t;
            {
                auto b0 = rd(); // declare flags (1 byte; may be empty from old peers)
                if (b0.length >= 1 && gExchFlags.length < AMQP_MAX_EXCHANGES)
                    gExchFlags[ex] = cast(ubyte) b0[0];
                auto ae0 = rd(); // alternate-exchange name (may be empty)
                if (ae0.length && gExchangeAE.length < AMQP_MAX_EXCHANGES)
                    gExchangeAE[ex] = cast(string) ae0.idup;
                else if (ae0 !is null)
                    gExchangeAE.remove(cast(string) ex); // redeclare without AE clears it
            }
            // bound the seq map: gExchanges shrinks on delete but gExchangeSeq
            // KEEPS a tombstone (rejects a stale declare), so declare+delete of
            // unique names would grow it without bound (per-shard, replicated).
            // Cap NEW keys like gQueueMeta self-caps; existing keys still update.
            if ((cast(string) ex) in gExchangeSeq || gExchangeSeq.length < AMQP_MAX_EXCHANGES)
                gExchangeSeq[ex] = seq;
        }
        else if (op == 2)
        {
            auto b = rd().idup;
            auto extra = rd(); // raw binding args table (may be empty)
            auto exb = extra is null ? null : cast(immutable(ubyte)[]) extra.idup;
            auto lst = ex in gBindings;
            if (lst !is null)
            {
                // existing element (live or tombstone): revive/update if newer.
                // !toExchange so a queue-bind can't collide with an exchange-bind
                // of the same name.
                foreach (ref bd; *lst)
                    if (bd.queue == a && bd.key == b && bd.args == exb && !bd.toExchange)
                    {
                        if (seq > bd.seq)
                        {
                            bd.seq = seq;
                            bd.alive = true;
                        }
                        return;
                    }
                if ((*lst).length >= AMQP_MAX_BINDINGS)
                {
                    atomicOp!"+="(gAmqpBindingDrops, 1);
                    return;
                }
            }
            else if (gBindings.length >= AMQP_MAX_EXCHANGES)
            {
                // cap the NUMBER of bound-exchange keys, not just the per-exchange
                // list length: queue.bind does NOT require the exchange to exist,
                // so a flood of binds to unique (undeclared) exchange names would
                // otherwise grow gBindings' key set without bound — and it's
                // replicated to EVERY shard via ctlBroadcast (an amplified,
                // per-shard RAM DoS with attacker-chosen keys). AMQP_MAX_EXCHANGES
                // is the right bound: legit binds only target declared exchanges,
                // which are already capped there.
                atomicOp!"+="(gAmqpBindingDrops, 1);
                return;
            }
            gBindings[ex] ~= Binding(a, b, exb, seq, true);
        }
        else if (op == 3) // queue metadata: ex=queue, a=dlx, b=dlrk, ttl+maxlen(i64 BE each)
        {
            auto b = rd().idup;
            auto tb = rd(); // 8B ttl [+ 8B x-max-length] big-endian (may be empty)
            long ttl = 0;
            long mxl = 0;
            if (tb.length >= 8)
                foreach (k; 0 .. 8)
                    ttl = (ttl << 8) | tb[k];
            if (tb.length >= 16)
                foreach (k; 8 .. 16)
                    mxl = (mxl << 8) | tb[k];
            if ((cast(string) ex) !in gQueueMeta && gQueueMeta.length >= AMQP_MAX_QUEUEMETA)
            {
                atomicOp!"+="(gAmqpCtlDrops, 1);
                return;
            }
            // presence flags (17th byte): bit0 dlx arg present, bit2 ttl arg
            // present — "" is a real dlx (the default exchange) and 0 a real
            // ttl (expire immediately), so value alone can't signal absence.
            ubyte fl = 0;
            if (tb.length >= 17)
                fl = tb[16];
            else // legacy encode: infer presence from a non-empty/non-zero value
                fl = cast(ubyte)((a.length ? 1 : 0) | (ttl > 0 ? 4 : 0));
            long expw = 0;
            if (tb.length >= 25)
                foreach (k; 17 .. 25)
                    expw = (expw << 8) | tb[k];
            // merge, don't clobber: a redeclare that sets only ttl must not
            // erase a previously-configured DLX (and vice-versa)
            ubyte ovfw = 0;
            if (tb.length >= 26)
                ovfw = tb[25]; // absent on a shorter (older) encode: drop-head
            long dlw = 0;
            if (tb.length >= 34)
                foreach (k; 26 .. 34)
                    dlw = (dlw << 8) | tb[k];
            ubyte mpw = 0;
            if (tb.length >= 35)
                mpw = tb[34];
            QueueMeta qm = QueueMeta(a, b, ttl, mxl, (fl & 1) != 0, (fl & 4) != 0,
                    expw, (fl & 8) != 0, ovfw, dlw, mpw);
            if (auto ex0 = (cast(string) ex) in gQueueMeta)
            {
                if (!(fl & 1))
                {
                    qm.dlx = ex0.dlx;
                    qm.dlxSet = ex0.dlxSet;
                }
                if (b.length == 0)
                    qm.dlrk = ex0.dlrk;
                if (!(fl & 4))
                {
                    qm.ttlMs = ex0.ttlMs;
                    qm.ttlSet = ex0.ttlSet;
                }
                if (mxl == 0)
                    qm.maxLen = ex0.maxLen;
                if (ovfw == 0)
                    qm.overflow = ex0.overflow; // merge, like every other arg
                if (dlw == 0)
                    qm.delLimit = ex0.delLimit;
                if (mpw == 0)
                    qm.maxPrio = ex0.maxPrio;
                if (!(fl & 8))
                {
                    qm.expMs = ex0.expMs;
                    qm.expSet = ex0.expSet;
                }
            }
            gQueueMeta[ex] = qm;
            if (qm.expSet) // (re)declare re-arms the unused-lease on every shard
                gQueueLease[ex] = cast(long) nowMs() + qm.expMs;
        }
        else if (op == 4) // queue.unbind: ex=exchange, a=queue, b=routing-key
        {
            auto b = rd().idup;
            auto lst = ex in gBindings;
            if (lst !is null)
            {
                bool found = false;
                foreach (ref bd; *lst)
                    if (bd.queue == a && bd.key == b && !bd.toExchange)
                    {
                        found = true;
                        if (seq > bd.seq)
                        {
                            bd.seq = seq; // tombstone: a stale re-bind can't revive it
                            bd.alive = false;
                        }
                    }
                // an unbind that ARRIVES BEFORE the bind it cancels (independent
                // SPSC lanes reorder across shards) must still leave a seq-stamped
                // tombstone; else the later LOWER-seq bind is appended live and the
                // shards permanently split-brain. Append one (capped, args=null —
                // unbind matches by queue+key, same as the existing-element case).
                if (!found && (*lst).length < AMQP_MAX_BINDINGS)
                    *lst ~= Binding(a, b, null, seq, false);
            }
            else if (gBindings.length < AMQP_MAX_EXCHANGES)
                gBindings[ex] = [Binding(a, b, null, seq, false)];
        }
        else if (op == 6) // exchange.bind: ex=source, a=dest exchange, b=rk
        {
            auto b = rd().idup;
            auto extra = rd();
            auto exb = extra is null ? null : cast(immutable(ubyte)[]) extra.idup;
            auto lst = ex in gBindings;
            if (lst !is null)
            {
                foreach (ref bd; *lst)
                    if (bd.queue == a && bd.key == b && bd.args == exb && bd.toExchange)
                    {
                        if (seq > bd.seq)
                        {
                            bd.seq = seq;
                            bd.alive = true;
                        }
                        return;
                    }
                if ((*lst).length >= AMQP_MAX_BINDINGS)
                {
                    atomicOp!"+="(gAmqpBindingDrops, 1);
                    return;
                }
            }
            else if (gBindings.length >= AMQP_MAX_EXCHANGES)
            {
                atomicOp!"+="(gAmqpBindingDrops, 1);
                return;
            }
            gBindings[ex] ~= Binding(a, b, exb, seq, true, true); // toExchange
        }
        else if (op == 7) // exchange.unbind: ex=source, a=dest exchange, b=rk
        {
            auto b = rd().idup;
            auto lst = ex in gBindings;
            if (lst !is null)
            {
                bool found = false;
                foreach (ref bd; *lst)
                    if (bd.queue == a && bd.key == b && bd.toExchange)
                    {
                        found = true;
                        if (seq > bd.seq)
                        {
                            bd.seq = seq;
                            bd.alive = false;
                        }
                    }
                // same reorder hazard as queue.unbind: tombstone an unseen e2e bind
                if (!found && (*lst).length < AMQP_MAX_BINDINGS)
                    *lst ~= Binding(a, b, null, seq, false, true); // toExchange
            }
            else if (gBindings.length < AMQP_MAX_EXCHANGES)
                gBindings[ex] = [Binding(a, b, null, seq, false, true)];
        }
        else if (op == 5) // exchange.delete: ex=exchange
        {
            try
            {
                if (auto sp = (cast(string) ex) in gExchangeSeq)
                    if (seq <= *sp)
                        return; // stale vs a newer declare/delete
                gExchanges.remove(cast(string) ex);
                gExchFlags.remove(cast(string) ex);
                gExchangeAE.remove(cast(string) ex);
                // tombstone (usually an update — declare recorded the key); the
                // cap guard mirrors the declare so a delete can't grow it either
                if ((cast(string) ex) in gExchangeSeq || gExchangeSeq.length < AMQP_MAX_EXCHANGES)
                    gExchangeSeq[ex] = seq; // rejects a stale later declare
                // the exchange's bindings die with it (RabbitMQ): both its own
                // (as source) and every e2e binding pointing AT it (as
                // destination). Leaving them "inert" split-brained a REDECLARE
                // of the same name, which revived routing through stale
                // bindings. Seq-guarded tombstones keep concurrent-bind LWW.
                if (auto bl = (cast(string) ex) in gBindings)
                    foreach (ref bd; *bl)
                        if (bd.alive && bd.seq < seq)
                        {
                            bd.alive = false;
                            bd.seq = seq;
                        }
                foreach (bex, ref blist; gBindings)
                    foreach (ref bd; blist)
                        if (bd.alive && bd.toExchange && bd.queue == ex && bd.seq < seq)
                        {
                            bd.alive = false;
                            bd.seq = seq;
                        }
            }
            catch (Exception)
            {
            }
        }
        else if (op == 8) // queue.declare: ex=queue name, a=declare flags (1 byte)
        {
            if (auto sp = (cast(string) ex) in gQueueSeq)
                if (seq <= *sp)
                    return; // stale vs a newer declare/delete on this name
            if ((cast(string) ex) !in gQueueSeq && gQueueSeq.length >= AMQP_MAX_QUEUEMETA)
            {
                atomicOp!"+="(gAmqpCtlDrops, 1);
                return;
            }
            gQueues[ex] = true;
            gQueueSeq[ex] = seq;
            if (a.length >= 1)
                gQueueFlags[ex] = cast(ubyte) a[0];
        }
        else if (op == 9) // queue.delete: ex=queue name (tombstone)
        {
            if (auto sp = (cast(string) ex) in gQueueSeq)
                if (seq <= *sp)
                    return; // stale vs a newer declare/delete
            gQueues.remove(cast(string) ex);
            gQueueOwner.remove(cast(string) ex); // exclusivity dies with the queue
            gQueueMeta.remove(cast(string) ex); // dlx/ttl die with the queue too
            gQueueFlags.remove(cast(string) ex);
            gQueueLease.remove(cast(string) ex); // x-expires state dies too
            gQueueConsGlobal.remove(cast(string) ex);
            // ... and so do its BINDINGS (RabbitMQ removes them with the queue).
            // A stale binding would keep routing dead-letters/publishes into a
            // ghost list under the deleted name — a queue redeclared later
            // inherits those strays. Tombstoned with THIS delete's seq: a later
            // re-bind carries a higher seq and revives the element (LWW).
            foreach (bex, ref blist; gBindings)
                foreach (ref bd; blist)
                    if (bd.alive && !bd.toExchange && bd.queue == ex && bd.seq < seq)
                    {
                        bd.alive = false;
                        bd.seq = seq;
                    }
            if ((cast(string) ex) in gQueueSeq || gQueueSeq.length < AMQP_MAX_QUEUEMETA)
                gQueueSeq[ex] = seq; // tombstone: rejects a stale later declare
        }
        else if (op == 10) // exclusive claim: ex=queue, a=owner conn id (decimal)
        {
            ulong oid = 0;
            foreach (ch3; a)
                if (ch3 >= '0' && ch3 <= '9')
                    oid = oid * 10 + (ch3 - '0');
            if (oid != 0 && gQueueOwner.length < AMQP_MAX_QUEUEMETA)
                gQueueOwner[ex] = oid;
        }
        else if (op == 11) // queue touch: re-arm the x-expires unused-lease
        {
            if (auto qm = (cast(string) ex) in gQueueMeta)
                if (qm.expSet)
                    gQueueLease[ex] = cast(long) nowMs() + qm.expMs;
        }
        else if (op == 12 || op == 13) // consumer count inc/dec (replicated sum)
        {
            auto pcg = (cast(string) ex) in gQueueConsGlobal;
            if (op == 12)
            {
                if (pcg !is null)
                    ++*pcg;
                else if (gQueueConsGlobal.length < AMQP_MAX_QUEUEMETA)
                    gQueueConsGlobal[ex] = 1;
            }
            else if (pcg !is null)
            {
                if (*pcg > 1)
                    --*pcg;
                else
                {
                    gQueueConsGlobal.remove(cast(string) ex);
                    // the queue just became UNUSED: the x-expires clock starts now
                    if (auto qm = (cast(string) ex) in gQueueMeta)
                        if (qm.expSet)
                            gQueueLease[ex] = cast(long) nowMs() + qm.expMs;
                }
            }
        }
    }
    catch (Exception)
    {
    }
}

/// Exclusive-queue gate: true (and sends the 405) when `q` is another
/// connection's exclusive queue. RabbitMQ answers RESOURCE_LOCKED for ANY
/// access — declare, consume, bind, delete — from a non-owner.
private bool exclusiveDenied(AmqpConn c, ushort chan, ref ByteBuffer o,
        scope const(char)[] q, ushort cls, ushort mth) nothrow @trusted
{
    if (auto po = (cast(string) q) in gQueueOwner)
        if (*po != c.connId)
        {
            channelClose(o, chan, 405, "RESOURCE_LOCKED - exclusive queue", cls, mth);
            c.chans.remove(chan);
            return true;
        }
    return false;
}

// publish-path memo (see gAmqpTopoEpoch)
private char[256] tPubMemoQBuf = void;
private size_t tPubMemoQLen;
private bool tPubMemoExists;
private long tPubMemoMaxLen; // gQueueMeta.maxLen (bound+1 encoding; 0 = unset)
private ubyte tPubMemoOverflow; // gQueueMeta.overflow, memoised with maxLen
private ulong tPubMemoEpoch = ulong.max;

/// True iff `q` names a queue that is currently declared (op 8, not yet op 9)
/// on this shard. The registry is broadcast-replicated with LWW seq ordering,
/// so a declare on the owning shard is visible here after fan-out; a same-shard
/// declare→consume is synchronous (ctlBroadcast applies locally first).
private bool queueExists(scope const(char)[] q) nothrow @trusted
{
    return (cast(string) q in gQueues) !is null;
}

private void ctlBroadcast(ubyte op, scope const(char)[] ex, scope const(char)[] a,
        scope const(char)[] b, scope const(ubyte)[] extra = null) nothrow @trusted
{
    // gAmqpCtlFanout -> shardEnqueue YIELDS under ring backpressure; a shared
    // static staging buffer would be rewritten by a concurrent declare/bind on
    // this thread during that yield, replicating a corrupted op to peers. Fast
    // path keeps the reused TLS buffer; reentrant callers get a fresh local.
    static ByteBuffer cbStatic; // TLS
    static bool cbBusy;
    ByteBuffer cbLocal;
    ByteBuffer* cbp = &cbLocal;
    if (!cbBusy)
    {
        cbBusy = true;
        cbp = &cbStatic;
    }
    scope (exit)
        if (cbp is &cbStatic)
            cbBusy = false;
    cbp.clear();
    cbp.appendByte(cast(char) op);
    immutable ulong seq = atomicOp!"+="(gAmqpCtlSeq, 1);
    foreach (k; 0 .. 8)
        cbp.appendByte(cast(char)(seq >> ((7 - k) * 8)));
    void put(scope const(char)[] s)
    {
        cbp.appendByte(cast(char)(s.length >> 8));
        cbp.appendByte(cast(char)(s.length & 0xFF));
        cbp.append(s);
    }

    put(ex);
    put(a);
    put(b);
    put(cast(const(char)[]) extra);
    amqpApplyCtl(cbp.data); // local first
    if (gAmqpCtlFanout !is null)
        gAmqpCtlFanout(cbp.data);
}

/// Does exchange `x` still have any LIVE binding as SOURCE (queue-binds and
/// e2e binds both live under gBindings[x])?
private bool exchangeHasLiveBindings(string x) nothrow @trusted
{
    try
        if (auto bl = x in gBindings)
            foreach (ref bd; *bl)
                if (bd.alive)
                    return true;
    catch (Exception)
    {
    }
    return false;
}

/// Exchanges holding a LIVE binding whose destination is `dest` (a queue name
/// by default; e2e destinations when `e2e`). Callers snapshot BEFORE the
/// removal broadcast so the auto-delete sweep knows who just lost a binding.
private string[] bindingSourcesTo(scope const(char)[] dest, bool e2e) nothrow @trusted
{
    string[] outv;
    try
        foreach (bex, ref blist; gBindings)
            foreach (ref bd; blist)
                if (bd.alive && bd.toExchange == e2e && bd.queue == dest)
                {
                    outv ~= bex;
                    break;
                }
    catch (Exception)
    {
    }
    return outv;
}

/// RabbitMQ auto-delete exchanges: an auto-delete exchange dies when a binding
/// removal leaves it with ZERO live bindings as source (it necessarily had at
/// least one — the sweep only runs on removals). Its death tombstones the e2e
/// bindings pointing AT it, which can cascade into THEIR sources. Runs on the
/// shard that originated the removal, AFTER that broadcast applied locally;
/// each death broadcasts its own op-5, so every shard replays the same
/// deterministic delete sequence (no per-shard decisions).
private void autoDeleteExchangeSweep(string[] seeds) nothrow @trusted
{
    try
    {
        string[] cand = seeds;
        int guard = 0;
        while (cand.length && ++guard <= AMQP_MAX_EXCHANGES)
        {
            auto x = cand[$ - 1];
            cand.length = cand.length - 1;
            if (x !in gExchanges)
                continue; // already gone (or never declared)
            auto fp = x in gExchFlags;
            if (fp is null || !(*fp & 4))
                continue; // not auto-delete
            if (exchangeHasLiveBindings(x))
                continue;
            // who loses a binding when x dies (collect BEFORE the broadcast
            // tombstones them; the broadcast's local apply is synchronous)
            auto next = bindingSourcesTo(x, true);
            ctlBroadcast(5, x, "", "");
            cand ~= next;
        }
    }
    catch (Exception)
    {
    }
}

/// Compare a header WANT (type/value from the binding args) against message
/// headers by key. Void ('V') or empty want = presence-only (any value);
/// every other type must match by value bytes AND compatible type. This is
/// what a headers binding of {count: 5} means — a message with count=7 must
/// NOT route (the old code matched on key presence alone).
private bool headerWantMatches(scope const(ubyte)[] msgHeaders,
        scope const(char)[] key, char wty, scope const(ubyte)[] wval) @nogc nothrow
{
    immutable presenceOnly = wty == 'V' || wval.length == 0;
    bool hit = false;
    cast(void) tableWalk(msgHeaders, (scope const(char)[] mk, char mt,
            scope const(ubyte)[] mv) @nogc nothrow {
        if (mk != key)
            return true;
        if (presenceOnly)
            hit = true;
        else if ((wty == 'S' || wty == 's') && (mt == 'S' || mt == 's'))
            hit = wval == mv; // string equality regardless of short/long tag
        else
            hit = wty == mt && wval == mv; // same type, same bytes
        return false; // first occurrence of the key decides
    });
    return hit;
}

/// Headers-exchange match: binding args carry x-match (all|any, default all)
/// plus wanted key/values compared by value (void/empty = presence-only).
private bool headersMatch(scope const(ubyte)[] bindArgs,
        scope const(ubyte)[] msgHeaders) @nogc nothrow
{
    if (bindArgs is null || bindArgs.length == 0)
        return true; // no args = x-match "all" with zero criteria: vacuously ALL
    bool any = false;
    bool withX = false; // "all-with-x"/"any-with-x": x-* keys ARE wants too
    {
        auto xm = tableGetStr(bindArgs, "x-match");
        any = xm == "any" || xm == "any-with-x";
        withX = xm == "any-with-x" || xm == "all-with-x";
    }
    bool allOk = true;
    bool anyOk = false;
    bool sawWant = false;
    cast(void) tableWalk(bindArgs, (scope const(char)[] k, char ty, scope const(ubyte)[] v) @nogc nothrow {
        if (k == "x-match")
            return true; // always a directive, never a want
        if (!withX && k.length >= 2 && k[0] == 'x' && k[1] == '-')
            return true; // x-* are directives unless the -with-x variants ask
        sawWant = true;
        immutable hit = msgHeaders !is null && headerWantMatches(msgHeaders, k, ty, v);
        if (hit)
            anyOk = true;
        else
            allOk = false;
        return true;
    });
    if (!sawWant)
        return !any; // vacuous "all" matches everything; vacuous "any" nothing
    return any ? anyOk : allOk;
}

/// Route (exchange, routingKey) -> queue names, calling sink for each.
private void routeTo(scope const(char)[] ex, scope const(char)[] rkey,
        scope const(ubyte)[] msgHeaders,
        scope void delegate(scope const(char)[] q) nothrow sink,
        scope const(const(char)[])[] altKeys = null) nothrow @trusted
{
    try
    {
        if (ex.length == 0)
        {
            // default exchange: routing key IS the queue name (CC/BCC keys too).
            // Pass the rkey SLICE straight through — NO idup. The sink consumes
            // it synchronously (byte-copy into its dedup arena + the RPUSH key,
            // AA lookups via cast(string)) before any yield and never retains it,
            // so a GC string is pure waste here — and being ONE GC alloc PER
            // MESSAGE, that alloc took the global GC lock and serialised every
            // shard (the Amdahl killer that flattened AMQP's per-shard scaling).
            sink(rkey);
            foreach (ak; altKeys)
                sink(ak);
            return;
        }
        auto t = (cast(string) ex) in gExchanges;
        auto bl = (cast(string) ex) in gBindings;
        if (t is null || bl is null)
            return;
        // Collect DEDUPED destination queues: a queue with several matching
        // bindings (overlapping topic patterns like a.# + a.b, or a fanout queue
        // bound twice with different routing keys) must receive the message ONCE,
        // not once per binding — RabbitMQ deduplicates the destination set. The
        // matching phase yields nowhere, so a reused TLS buffer is safe to fill;
        // a reentrant routeTo (the sink's cross-shard RPUSH YIELDS) takes a
        // stack-local so the parked outer loop keeps reading its own list.
        static string[] destStatic; // TLS
        static bool destBusy;
        string[] destLocal;
        string[]* dests = &destLocal;
        if (!destBusy)
        {
            destBusy = true;
            dests = &destStatic;
        }
        scope (exit)
            if (dests is &destStatic)
                destBusy = false;
        (*dests).length = 0;
        void add(string q) nothrow
        {
            foreach (d; *dests)
                if (d == q)
                    return; // this queue already matched another binding/path
            try
                *dests ~= q;
            catch (Exception)
            {
            }
        }
        // Exchange visited-set: expand each exchange AT MOST ONCE per publish so a
        // binding graph (a cycle A->B->A, or a diamond) can't blow up to N^depth
        // work — the depth cap alone bounds path LENGTH, not total work, and a
        // yield-free `collect` monopolizing the shard on one publish is a freeze
        // DoS. Slices are stable for this yield-free walk (gBindings keys / the
        // idup'd dest-exchange names), so no idup. Reused TLS: seeded per publish;
        // a reentrant routeTo (during sink's yield) refills it after we're done
        // with it, which is harmless (collect never reads it across a yield).
        // Best-depth memo (parallel arrays: name -> shallowest depth expanded). A
        // DFS that first reaches an exchange DEEP would truncate its subtree one
        // hop short (the depth cap) and drop an in-cap destination; and a plain
        // boolean visited would then block the later SHALLOWER path. So we re-expand
        // an exchange whenever it's reached at a STRICTLY shallower depth (more hop
        // budget). Bounded: depth strictly decreases, so <= cap+1 re-expansions per
        // exchange; add() dedups the queues. Reused TLS, seeded per publish.
        static const(char)[][] visited;
        static int[] visitedDepth;
        size_t vindex(scope const(char)[] cx) nothrow
        {
            foreach (k, v; visited)
                if (v == cx)
                    return k;
            return size_t.max;
        }
        // Collect matching destinations, recursing through exchange-to-exchange
        // bindings (bd.toExchange). Memo-guarded (above) AND depth-bounded;
        // add() dedups queues reached via multiple paths.
        void collect(scope const(char)[] cx, int depth) nothrow
        {
            if (depth > AMQP_MAX_EXCHANGE_HOPS)
                return;
            auto ct = (cast(string) cx) in gExchanges;
            auto cbl = (cast(string) cx) in gBindings;
            if (ct is null || cbl is null)
                return;
            foreach (ref bd; *cbl)
            {
                if (!bd.alive)
                    continue;
                bool m;
                final switch (*ct)
                {
                case ExType.fanout:
                    m = true;
                    break;
                case ExType.direct:
                    m = bd.key == rkey;
                    if (!m)
                        foreach (ak; altKeys)
                            if (bd.key == ak)
                            {
                                m = true;
                                break;
                            }
                    break;
                case ExType.topic:
                    m = amqpTopicMatches(bd.key, rkey);
                    if (!m)
                        foreach (ak; altKeys)
                            if (amqpTopicMatches(bd.key, ak))
                            {
                                m = true;
                                break;
                            }
                    break;
                case ExType.headers:
                    m = headersMatch(bd.args, msgHeaders);
                    break;
                }
                if (!m)
                    continue;
                if (bd.toExchange)
                {
                    immutable nd = depth + 1;
                    if (nd > AMQP_MAX_EXCHANGE_HOPS)
                        continue; // beyond the hop budget: expands nothing
                    immutable idx = vindex(bd.queue);
                    if (idx == size_t.max)
                    {
                        try
                        {
                            visited ~= bd.queue;
                            visitedDepth ~= nd;
                        }
                        catch (Exception)
                        {
                            // keep the parallel arrays the same length (a desync
                            // would OOB visitedDepth[idx] under -release)
                            if (visited.length > visitedDepth.length)
                                visited.length = visitedDepth.length;
                            continue;
                        }
                    }
                    else if (nd < visitedDepth[idx])
                        visitedDepth[idx] = nd; // reached shallower: re-expand, more budget
                    else
                        continue; // already expanded at an equal-or-shallower depth
                    collect(bd.queue, nd); // route into the destination exchange
                }
                else
                    add(bd.queue);
            }
        }

        visited.length = 0;
        visitedDepth.length = 0;
        collect(ex, 0);
        foreach (q; *dests)
            sink(q); // yields per queue; dests is stable (reentrants use destLocal)
    }
    catch (Exception)
    {
    }
}

// ---------------------------------------------------------------------------
// Frame codec

private enum ubyte FRAME_METHOD = 1, FRAME_HEADER = 2, FRAME_BODY = 3,
        FRAME_HEARTBEAT = 8, FRAME_END = 0xCE;

// Frame writers reserve ONCE and store, instead of appendByte per byte. Every
// appendByte is its own reserve + capacity check, so a u32 cost four of each and
// a u64 eight; profiling the settle path put 23% of all cycles in this handful
// of functions. freeSpace/grow is the buffer's own API for exactly this, and it
// keeps the OOM contract: freeSpace returns short, and the writer drops the
// bytes rather than overrunning the block.
private void frameStart(ref ByteBuffer o, ubyte type, ushort chan, out size_t sizeAt) @nogc nothrow
{
    auto d = o.freeSpace(7);
    if (d.length < 7)
    {
        sizeAt = o.length;
        return; // OOM: the buffer flags it and every writer below no-ops
    }
    d[0] = type;
    d[1] = cast(ubyte)(chan >> 8);
    d[2] = cast(ubyte)(chan & 0xFF);
    d[3] = 0;
    d[4] = 0;
    d[5] = 0;
    d[6] = 0; // size placeholder, patched by frameFinish
    o.grow(7);
    sizeAt = o.length - 4;
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
    auto d = o.freeSpace(2);
    if (d.length < 2)
        return;
    d[0] = cast(ubyte)(v >> 8);
    d[1] = cast(ubyte)(v & 0xFF);
    o.grow(2);
}

private void putU32(ref ByteBuffer o, uint v) @nogc nothrow
{
    auto d = o.freeSpace(4);
    if (d.length < 4)
        return;
    d[0] = cast(ubyte)(v >> 24);
    d[1] = cast(ubyte)(v >> 16);
    d[2] = cast(ubyte)(v >> 8);
    d[3] = cast(ubyte)(v & 0xFF);
    o.grow(4);
}

private void putU64(ref ByteBuffer o, ulong v) @nogc nothrow
{
    auto d = o.freeSpace(8);
    if (d.length < 8)
        return;
    foreach (i; 0 .. 8)
        d[i] = cast(ubyte)(v >> (56 - 8 * i));
    o.grow(8);
}

private void putShortStr(ref ByteBuffer o, scope const(char)[] s) @nogc nothrow
{
    // shortstr carries a ONE-byte length prefix (AMQP max 255). A >255-byte value
    // would truncate the length byte while the full bytes still follow, desyncing
    // a spec-strict consumer's frame stream. Emit at most 255 with a matching len.
    immutable n = s.length > 255 ? 255 : s.length;
    auto d = o.freeSpace(n + 1);
    if (d.length < n + 1)
        return;
    d[0] = cast(ubyte) n;
    d[1 .. n + 1] = cast(const(ubyte)[]) s[0 .. n];
    o.grow(n + 1);
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
            // RabbitMQ field-table dialect: 's' is a SIGNED 16-BIT SHORT (the
            // java client writes Short this way), NOT the 0-9-1 shortstr —
            // shortstr does not occur in rabbit tables.
            vlen = 2;
            voff = i;
            i += 2;
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
/// Fetch an integer-typed value by key from a field table (x-message-ttl is
/// commonly 'I' i32 but clients also send 'l'/'i'/'b'/'B'/'U'/'u'). Big-endian,
/// signed. Returns 0 when absent or non-integer.
package long tableGetInt(scope const(ubyte)[] t, scope const(char)[] key) @nogc nothrow
{
    long found = 0;
    cast(void) tableWalk(t, (scope const(char)[] k, char ty, scope const(ubyte)[] v) @nogc nothrow {
        if (k != key)
            return true;
        switch (ty)
        {
        case 'b': // signed octet
            if (v.length >= 1) found = cast(byte) v[0];
            break;
        case 'B': // unsigned octet
            if (v.length >= 1) found = cast(long) v[0];
            break;
        case 'U', 's': // signed short ('s' = rabbit-dialect short, not shortstr)
            if (v.length >= 2) found = cast(short)((v[0] << 8) | v[1]);
            break;
        case 'u': // unsigned short
            if (v.length >= 2) found = (cast(long) v[0] << 8) | v[1];
            break;
        case 'I': // signed int
            if (v.length >= 4)
                found = cast(int)((cast(uint) v[0] << 24) | (cast(uint) v[1] << 16)
                    | (cast(uint) v[2] << 8) | v[3]);
            break;
        case 'i': // unsigned int
            if (v.length >= 4)
                found = (cast(long) v[0] << 24) | (cast(long) v[1] << 16)
                    | (cast(long) v[2] << 8) | v[3];
            break;
        case 'l', 'L': // long (signed)
            if (v.length >= 8)
                foreach (n; 0 .. 8)
                    found = (found << 8) | v[n];
            break;
        default:
            break;
        }
        return false;
    });
    return found;
}

/// Presence+type check for an integer table arg: 0 = absent, 1 = an integer
/// field type, -1 = present with a NON-integer type (string "foobar",
/// "10000foobar", bool, ...) — the inequivalent-arg 406 case.
package int tableIntKind(scope const(ubyte)[] t, scope const(char)[] key,
        out long val) @nogc nothrow
{
    int kind = 0;
    long got = 0;
    cast(void) tableWalk(t, (scope const(char)[] k, char ty, scope const(ubyte)[] v) @nogc nothrow {
        if (k != key)
            return true;
        switch (ty)
        {
        case 'b', 'B', 's', 'U', 'u', 'I', 'i', 'l', 'L':
            kind = 1;
            break;
        default:
            kind = -1;
            break;
        }
        return false;
    });
    if (kind == 1)
        got = tableGetInt(t, key);
    val = got;
    return kind;
}

/// Presence+type check for a STRING table arg: 0 = absent, 1 = long-string
/// ('S'), -1 = present with any other type — the inequivalent-arg 406 case.
package int tableStrKind(scope const(ubyte)[] t, scope const(char)[] key) @nogc nothrow
{
    int kind = 0;
    cast(void) tableWalk(t, (scope const(char)[] k, char ty, scope const(ubyte)[] v) @nogc nothrow {
        if (k != key)
            return true;
        kind = ty == 'S' ? 1 : -1;
        return false;
    });
    return kind;
}

package const(char)[] tableGetStr(return scope const(ubyte)[] t, scope const(char)[] key) @nogc nothrow
{
    const(char)[] found = null;
    cast(void) tableWalk(t, (scope const(char)[] k, char ty, scope const(ubyte)[] v) @nogc nothrow {
        if (k == key && ty == 'S') // 's' is a rabbit-dialect short, not a string
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

/// The `reply-to` basic property (bit 0x0200 shortstr), or null when absent.
package const(char)[] propsReplyTo(return scope const(ubyte)[] props) @nogc nothrow
{
    if (props.length < 2)
        return null;
    immutable flags = (cast(ushort) props[0] << 8) | props[1];
    if (!(flags & 0x0200))
        return null;
    size_t i = 2;
    static bool skipShort(scope const(ubyte)[] pp, ref size_t j) @nogc nothrow
    {
        if (j + 1 > pp.length)
            return false;
        j += 1 + pp[j];
        return j <= pp.length;
    }

    if (flags & 0x8000)
        if (!skipShort(props, i))
            return null;
    if (flags & 0x4000)
        if (!skipShort(props, i))
            return null;
    if (flags & 0x2000)
    {
        if (i + 4 > props.length)
            return null;
        immutable n = (cast(size_t) props[i] << 24) | (cast(size_t) props[i + 1] << 16)
            | (cast(size_t) props[i + 2] << 8) | props[i + 3];
        i += 4 + n;
        if (i > props.length)
            return null;
    }
    if (flags & 0x1000)
        i += 1;
    if (flags & 0x0800)
        i += 1;
    if (flags & 0x0400)
        if (!skipShort(props, i))
            return null;
    if (i + 1 > props.length || i + 1 + props[i] > props.length)
        return null;
    return cast(const(char)[]) props[i + 1 .. i + 1 + props[i]];
}

/// Rebuild `props` with the reply-to property REPLACED by `newRt` (set when
/// absent). Everything else survives verbatim.
private void replaceReplyTo(ref ByteBuffer dst, scope const(ubyte)[] props,
        scope const(char)[] newRt) @nogc nothrow
{
    dst.clear();
    ushort flags = 0;
    size_t i = 2;
    if (props.length >= 2)
        flags = cast(ushort)((props[0] << 8) | props[1]);
    immutable nf = cast(ushort)(flags | 0x0200);
    dst.appendByte(cast(char)(nf >> 8));
    dst.appendByte(cast(char)(nf & 0xFF));
    static bool copyShort(ref ByteBuffer d2, scope const(ubyte)[] pp, ref size_t j) @nogc nothrow
    {
        if (j >= pp.length || j + 1 + pp[j] > pp.length)
            return false;
        immutable seg = 1 + pp[j];
        d2.append(cast(const(char)[]) pp[j .. j + seg]);
        j += seg;
        return true;
    }

    if (flags & 0x8000)
        if (!copyShort(dst, props, i))
            return;
    if (flags & 0x4000)
        if (!copyShort(dst, props, i))
            return;
    if (flags & 0x2000)
    {
        if (i + 4 > props.length)
            return;
        immutable n = (cast(size_t) props[i] << 24) | (cast(size_t) props[i + 1] << 16)
            | (cast(size_t) props[i + 2] << 8) | props[i + 3];
        if (i + 4 + n > props.length)
            return;
        dst.append(cast(const(char)[]) props[i .. i + 4 + n]);
        i += 4 + n;
    }
    if (flags & 0x1000)
        if (i < props.length)
            dst.appendByte(cast(char) props[i++]);
    if (flags & 0x0800)
        if (i < props.length)
            dst.appendByte(cast(char) props[i++]);
    if (flags & 0x0400)
        if (!copyShort(dst, props, i))
            return;
    // the NEW reply-to
    dst.appendByte(cast(char)(newRt.length > 255 ? 255 : newRt.length));
    dst.append(newRt[0 .. newRt.length > 255 ? 255 : newRt.length]);
    if (flags & 0x0200) // skip the OLD one
        if (i < props.length)
            i += 1 + props[i];
    if (i <= props.length)
        dst.append(cast(const(char)[]) props[i .. $]); // expiration onward verbatim
}

/// Parse the `expiration` basic property (a shortstr of decimal milliseconds;
/// property-flags bit 8). 0 = absent/invalid. Walks the properties that precede
/// it in the flags order: content-type, content-encoding, headers (field
/// table), delivery-mode + priority (one octet each), correlation-id, reply-to.
/// The expiration BasicProperty, ms: -1 = absent, -2 = present but INVALID
/// (non-numeric — "foobar", "10000foobar", "-1"), >= 0 = the value ("0" means
/// expire immediately).
package long propsExpiration(scope const(ubyte)[] props) @nogc nothrow
{
    if (props.length < 2)
        return -1;
    immutable flags = (cast(ushort) props[0] << 8) | props[1];
    size_t i = 2;
    static bool skipShort(scope const(ubyte)[] p, ref size_t j) @nogc nothrow
    {
        if (j + 1 > p.length)
            return false;
        j += 1 + p[j];
        return j <= p.length;
    }

    if (flags & 0x8000) // content-type
        if (!skipShort(props, i))
            return -1;
    if (flags & 0x4000) // content-encoding
        if (!skipShort(props, i))
            return -1;
    if (flags & 0x2000) // headers: u32 length + table
    {
        if (i + 4 > props.length)
            return -1;
        immutable n = (cast(size_t) props[i] << 24) | (cast(size_t) props[i + 1] << 16)
            | (cast(size_t) props[i + 2] << 8) | props[i + 3];
        i += 4 + n;
        if (i > props.length)
            return -1;
    }
    if (flags & 0x1000) // delivery-mode: octet
        i += 1;
    if (flags & 0x0800) // priority: octet
        i += 1;
    if (flags & 0x0400) // correlation-id
        if (!skipShort(props, i))
            return -1;
    if (flags & 0x0200) // reply-to
        if (!skipShort(props, i))
            return -1;
    if (!(flags & 0x0100)) // no expiration property
        return -1;
    if (i + 1 > props.length)
        return -1;
    immutable len = props[i];
    i += 1;
    if (i + len > props.length)
        return -1;
    long v = 0;
    foreach (k; 0 .. len)
    {
        immutable ch = props[i + k];
        if (ch < '0' || ch > '9')
            return -2; // present but non-numeric: the publish-time 406 case
        v = v * 10 + (ch - '0');
    }
    return v;
}

// ---------------------------------------------------------------------------
// Connection / channel state

private struct PendingPub
{
    bool active;
    bool mandatory; // basic.publish mandatory bit: unroutable -> basic.return
    string exchange;
    string rkey;
    ulong bodySize;
    ByteBuffer payload;
    ByteBuffer props; // property-flags + property-list from the content header
}

// A transaction (tx.select) buffers publishes and settles on the channel and
// applies them atomically on tx.commit (or drops them on tx.rollback).
private struct TxPub
{
    string exchange;
    string rkey;
    bool mandatory;
    immutable(ubyte)[] record; // the framed record, ready to route on commit
}

private struct TxSettle
{
    ulong tag;
    ubyte kind; // 0 = ack, 1 = nack, 2 = reject
    bool multiple;
    bool requeue;
}

/// [bug 21846] ack/nack/reject with an unknown delivery-tag is an IMMEDIATE
/// channel 406 PRECONDITION_FAILED — even on a transacted channel. "Known" =
/// an outstanding unacked entry of THIS channel that the open tx hasn't
/// already settled; the multiple form (tag = upper bound, 0 = all) is only
/// checked for "was this tag ever issued" (tags are conn-monotonic).
/// `pu`, when given, is the slot the caller has ALREADY looked up for this tag:
/// the single-settle path would otherwise hash/index `unacked` here and again in
/// dropUnacked, two touches of the same entry per ack. Ignored for a multiple
/// settle, which does not resolve one tag.
private bool settleTagUnknown(AmqpConn c, ushort chan, ulong tag, bool multiple,
        Unacked* pu = null) nothrow @trusted
{
    try
    {
        if (multiple)
        {
            if (tag == 0)
                return false; // "all outstanding" is always known
            // beyond the highest tag ever ISSUED ON THIS CHANNEL = unknown —
            // per-channel, so another channel's higher tag doesn't launder it
            if (auto mch = chan in c.chans)
                return tag > mch.lastTag;
            return tag >= c.nextTag;
        }
        auto u = pu !is null ? pu : (tag in c.unacked);
        if (u is null || u.chan != chan)
            return true;
        if (auto tch = chan in c.chans)
            if (tch.txMode)
                foreach (ref tsx; tch.txSettles)
                    if (tsx.tag == tag && !tsx.multiple)
                        return true; // second settle of the same tag inside the tx
        return false;
    }
    catch (Exception)
    {
        return false;
    }
}

/// Cap the buffered work of one open transaction (unbounded would be a RAM DoS:
/// a client that tx.selects and never commits). The BYTE cap is the real bound —
/// a message body reaches AMQP_MAX_BODY (128MB), so 100k of them would be
/// terabytes; the count cap only guards the tiny settle structs.
private enum size_t AMQP_MAX_TX = 100_000;
/// Max exchange-to-exchange hops a message routes through (cycle/depth bound).
private enum int AMQP_MAX_EXCHANGE_HOPS = 10;
private enum size_t AMQP_MAX_TX_BYTES = 256UL << 20; // 256MB of buffered pubs/tx
// The unacked/prefetch window is byte-bounded too (not just count-bounded): a
// no-ack=false consumer that stops acking large messages would otherwise pin
// count*bodysize RAM (the count cap alone is not a RAM bound — the byte-cap
// lesson). Per connection, like the tx buffer.
private enum size_t AMQP_MAX_UNACKED_BYTES = 256UL << 20; // 256MB

private struct Channel
{
    bool open;
    uint openGen; // per-conn generation stamped on channel.open. Consumer fibers
    // capture it and exit when the channel is gone OR reopened under a NEW gen —
    // channel NUMBERS are reused (pika reuses the lowest freed one), so a plain
    // per-number closed-flag would kill a fresh consumer on a reopened number.
    bool confirmMode;
    ulong confirmSeq; // next publish seq (delivery-tag for basic.ack confirms)
    PendingPub pub;
    bool txMode; // tx.select: buffer pubs/settles until tx.commit
    TxPub[] txPubs;
    TxSettle[] txSettles;
    bool prefetchGlobal; // basic.qos global bit: true = the prefetch window is
    // per-CHANNEL (shared); false (the default) = per-CONSUMER
    string lastServed; // (kept for context) tag of the last global-window delivery
    string[] rrOrder; // live consumer tags in consume order: under a GLOBAL
    // window the designated (rrNext) consumer gets first crack at each freed
    // slot; others wait a bounded grace (the QosTests fairness pair)
    size_t rrNext;
    string lastQueue; // "current queue": last queue DECLARED on this channel —
    // the spec default for an empty queue field in queue.bind/unbind/purge/
    // delete and basic.consume/get
    ulong lastTag; // highest delivery-tag ISSUED on this channel (tags are
    // conn-monotonic): a multiple-form settle beyond it is "unknown" (406)
    bool drConsumer; // active direct-reply consumer (amq.rabbitmq.reply-to)
    string drCtag; // its consumer tag (basic.cancel clears the flag)
    size_t txBytes; // running size of buffered tx publish records (byte cap)
    // basic.qos is CHANNEL-scoped in 0-9-1 (RabbitMQ: per-channel window).
    // 0 = unset -> conn-level fallback -> AMQP_DEFAULT_PREFETCH. unackedN is
    // this channel's live unacked count (maintained by the deliver/settle
    // paths); the connection keeps the global byte cap as the DoS backstop.
    ushort prefetch;
    uint unackedN;
}

private struct Unacked
{
    string queue;
    const(ubyte)[] blob; // stored record (rk+props+body), for requeue/dead-letter
    ushort chan; // owning channel: channel.close requeues just this channel's
    int deaths; // reserved for a future x-death header hop count (loop bound)
    bool fromGet; // basic.get delivery: OUTSIDE the consumer qos window
    // (RabbitMQ: qos governs consumers only), so its settle must not
    // decrement the channel's consumer-window counter.
    CuCell* cu; // delivering consumer's unacked counter (null for basic.get):
    // released by the UnackedMap when this record leaves it, so no settle path
    // has to remember to.
    string ctag; // delivering consumer's tag ("" for basic.get): qos with
    // global=false is a PER-CONSUMER window (the 0-9-1 java default), so
    // settles must credit the right consumer's counter.
}

/// Max slots the dense unacked window will span before falling back to the
/// hash spill. Sized so the fast path covers every sane prefetch window
/// (65536 outstanding deliveries) while bounding the per-connection cost of a
/// client that acks a high tag and sits on a low one forever.
private enum size_t UNACKED_WINDOW_MAX = 1 << 16;

/// Unacked store: a DENSE SLIDING WINDOW keyed by delivery tag, with an AA
/// spill for the pathological case.
///
/// Delivery tags are `c.nextTag++` — strictly sequential per connection — and
/// settles arrive in roughly the order they were issued, so the live set is
/// almost always a compact run. Holding that run in a hash map made the ack
/// path a pointer chase: profiling the consume drain (s2, 16 queues) put 18.8%
/// of all cycles in the AA (`_aaGetX` 7.9%, `settleTagUnknown` 7.0% — a single
/// lookup, i.e. cache misses — plus resize/hash/mix) and fed the 16.4% the GC
/// spent scanning its buckets. A ring indexed by (tag - base) turns both the
/// insert and the settle into contiguous array access.
///
/// The interface mirrors the built-in AA it replaced (`in`, `[]=`, `remove`,
/// `length`, `clear`, `foreach`) so the ~17 call sites are untouched. The one
/// visible difference is ITERATION ORDER: ascending by tag instead of hash
/// order. Every foreach site collects tags and then sorts them explicitly
/// (requeue needs highest-first, dead-letter lowest-first), so ordering is not
/// load-bearing — but do not add a site that relies on it.
/// Per-consumer unacked counter, held by POINTER instead of looked up by tag.
///
/// The count was a `uint[string]` keyed by the consumer tag, hashed once on
/// every delivery and once on every settle — 6.2% of cycles on the settle path
/// (hash mix, _d_aaInH, bytesHash) for a value both sides can simply hold.
///
/// A dense index would be wrong here: a consumer can be CANCELLED while its
/// deliveries are still unacked, so an index could be recycled under records
/// that still point at it. The cell instead frees itself when both halves are
/// done — the consumer has exited AND its last delivery has settled — so no
/// owner has to outlive the other. Everything below runs on the connection's
/// own thread (the consumer fiber delivers, the read fiber settles), so plain
/// increments are correct; no atomics.
private struct CuCell
{
    uint count;
    bool gone; // the consumer fiber has exited
}

private CuCell* cuNew() @nogc nothrow @trusted
{
    import core.stdc.stdlib : calloc;

    return cast(CuCell*) calloc(1, CuCell.sizeof);
}

/// One settle: drop the count, and reap the cell if the consumer already left.
private void cuRelease(CuCell* cu) @nogc nothrow @trusted
{
    import core.stdc.stdlib : free;

    if (cu is null)
        return;
    if (cu.count > 0)
        cu.count--;
    if (cu.count == 0 && cu.gone)
        free(cu);
}

/// The consumer fiber is leaving: reap now if nothing is still outstanding,
/// otherwise let the last settle do it.
private void cuGone(CuCell* cu) @nogc nothrow @trusted
{
    import core.stdc.stdlib : free;

    if (cu is null)
        return;
    cu.gone = true;
    if (cu.count == 0)
        free(cu);
}

/// Copy a delivery's record into memory the UnackedMap OWNS. Deliberately not
/// the GC: this is one allocation per delivered message, it holds no pointers,
/// and profiling the settle path put ~4.7% of cycles in GC mark and GCBits
/// scanning exactly these blobs. Every free is inside UnackedMap, so no caller
/// has to remember one.
private const(ubyte)[] dupBlob(scope const(ubyte)[] src) @nogc nothrow @trusted
{
    import core.stdc.stdlib : malloc;
    import core.stdc.string : memcpy;

    if (src.length == 0)
        return null;
    auto p = cast(ubyte*) malloc(src.length);
    if (p is null)
        return null; // caller degrades: an unacked with no body still settles
    memcpy(p, src.ptr, src.length);
    return cast(const(ubyte)[]) p[0 .. src.length];
}

private void freeBlob(ref Unacked u) @nogc nothrow @trusted
{
    import core.stdc.stdlib : free;

    if (u.blob.ptr !is null)
        free(cast(void*) u.blob.ptr);
    u.blob = null;
    // the record's other owned resource: its consumer's window credit
    cuRelease(u.cu);
    u.cu = null;
}

private struct UnackedMap
{
    private Unacked[] buf; // ring, length is a power of two (0 = unallocated)
    private bool[] live;
    private ulong base_; // tag held at window offset 0
    private size_t head; // ring index of window offset 0
    private size_t span; // window width; slots [head, head+span) may be live
    private size_t n; // live entries inside the window
    private Unacked[ulong] spill; // tags too far past base_ to fit the window

    private size_t idx(ulong tag) const @nogc nothrow @safe
    {
        return (head + cast(size_t)(tag - base_)) & (buf.length - 1);
    }

    private bool inWindow(ulong tag) const @nogc nothrow @safe
    {
        return span != 0 && tag >= base_ && tag - base_ < span;
    }

    /// Grow to hold `need` slots, re-laying the window at offset 0.
    private void grow(size_t need) nothrow @trusted
    {
        size_t cap = buf.length ? buf.length : 64;
        while (cap < need)
            cap <<= 1;
        auto nb = new Unacked[cap];
        auto nl = new bool[cap];
        foreach (i; 0 .. span)
        {
            immutable src = (head + i) & (buf.length - 1);
            if (live[src])
            {
                nb[i] = buf[src];
                nl[i] = true;
            }
        }
        buf = nb;
        live = nl;
        head = 0;
    }

    /// Drop dead slots off the front so `base_` tracks the lowest live tag.
    private void advance() nothrow @trusted
    {
        while (span != 0 && !live[head])
        {
            head = (head + 1) & (buf.length - 1);
            base_++;
            span--;
        }
        if (span == 0)
        {
            head = 0;
            // window is empty: adopt the spill's lowest tag as the new base so
            // a client that stranded one low tag still gets the fast path back.
            if (spill.length)
                try
                {
                    ulong lo = ulong.max;
                    foreach (t, ref _u; spill)
                        if (t < lo)
                            lo = t;
                    base_ = lo;
                    ulong[] moved;
                    foreach (t, ref u; spill)
                        if (t - lo < UNACKED_WINDOW_MAX)
                            moved ~= t;
                    foreach (t; moved)
                    {
                        auto u = spill[t];
                        spill.remove(t);
                        opIndexAssign(u, t);
                    }
                }
                catch (Exception)
                {
                }
        }
    }

    void opIndexAssign(Unacked v, ulong tag) nothrow @trusted
    {
        if (span == 0)
        {
            if (buf.length == 0)
                grow(64);
            base_ = tag;
            head = 0;
            span = 1;
            live[0] = true;
            buf[0] = v;
            n++;
            return;
        }
        if (tag < base_ || tag - base_ >= UNACKED_WINDOW_MAX)
        {
            try
                spill[tag] = v;
            catch (Exception)
            {
            }
            return;
        }
        immutable need = cast(size_t)(tag - base_) + 1;
        if (need > buf.length)
            grow(need);
        if (need > span)
            span = need;
        immutable i = idx(tag);
        if (!live[i])
            n++;
        else
            freeBlob(buf[i]); // reused slot: its record goes with it
        live[i] = true;
        buf[i] = v;
    }

    inout(Unacked)* opBinaryRight(string op : "in")(ulong tag) inout @nogc nothrow @trusted
    {
        if (inWindow(tag))
        {
            immutable i = idx(tag);
            return live[i] ? &buf[i] : null;
        }
        if (spill.length)
            return tag in spill;
        return null;
    }

    bool remove(ulong tag) nothrow @trusted
    {
        if (inWindow(tag))
        {
            immutable i = idx(tag);
            if (!live[i])
                return false;
            live[i] = false;
            freeBlob(buf[i]); // this map owns the record's memory
            buf[i] = Unacked.init;
            n--;
            advance();
            return true;
        }
        if (spill.length)
            try
            {
                if (auto sp = tag in spill)
                    freeBlob(*sp);
                return spill.remove(tag);
            }
            catch (Exception)
            {
            }
        return false;
    }

    size_t length() const @nogc nothrow @safe
    {
        return n + spill.length;
    }

    void clear() nothrow @trusted
    {
        foreach (i; 0 .. span)
        {
            immutable j = (head + i) & (buf.length - 1);
            if (live[j])
                freeBlob(buf[j]);
        }
        try
            foreach (t, ref u; spill)
                freeBlob(u);
        catch (Exception)
        {
        }
        buf = null;
        live = null;
        base_ = 0;
        head = 0;
        span = 0;
        n = 0;
        try
            spill.clear();
        catch (Exception)
        {
        }
    }

    /// Ascending by tag over the window, then the spill. The delegate is typed
    /// `nothrow` because every call site sits inside a nothrow frame handler;
    /// a template opApply cannot be used here (foreach cannot infer the loop
    /// variable types from one).
    int opApply(scope int delegate(ulong, ref Unacked) nothrow dg) nothrow @trusted
    {
        foreach (i; 0 .. span)
        {
            immutable j = (head + i) & (buf.length - 1);
            if (!live[j])
                continue;
            if (auto r = dg(base_ + i, buf[j]))
                return r;
        }
        if (spill.length)
            try
            {
                foreach (t, ref u; spill)
                    if (auto r = dg(t, u))
                        return r;
            }
            catch (Exception)
            {
            }
        return 0;
    }
}

unittest // UnackedMap: AA-compatible surface over the dense sliding window
{
    // dupBlob, not a literal: the map FREES what it is given, and handing it
    // static storage would abort.
    static Unacked mk(ushort ch, string ct = "")
    {
        return Unacked("q", dupBlob(cast(const(ubyte)[]) "body"), ch, 0, false, null, ct);
    }

    UnackedMap m;
    assert(m.length == 0);
    assert((1UL in m) is null);
    assert(m.remove(1) == false); // removing from an empty map is not a crash

    // insert / lookup / length
    foreach (t; 1 .. 6)
        m[t] = mk(cast(ushort)(t * 10));
    assert(m.length == 5);
    assert((3UL in m) !is null && (3UL in m).chan == 30);
    assert((6UL in m) is null);

    // OUT-OF-ORDER settle: freeing a middle tag must not slide the base, and
    // must not disturb its neighbours (the bug a ring gets wrong).
    assert(m.remove(3));
    assert((3UL in m) is null);
    assert((2UL in m) !is null && (4UL in m) !is null);
    assert(m.length == 4);
    assert(m.remove(3) == false); // double settle

    // freeing the FRONT slides the base past every dead slot at once
    assert(m.remove(1));
    assert(m.remove(2));
    assert(m.base_ == 4 && m.length == 2); // 3 was already dead: skipped too

    // ascending iteration, and every live tag yielded exactly once
    ulong[] seen;
    foreach (t, ref u; m)
        seen ~= t;
    assert(seen == [4UL, 5UL]);

    // growth past the initial capacity, with the window still correct
    UnackedMap g;
    foreach (t; 1 .. 500)
        g[t] = mk(cast(ushort)(t % 60000));
    assert(g.length == 499);
    assert((499UL in g) !is null && (499UL in g).chan == cast(ushort)(499 % 60000));
    foreach (t; 1 .. 500)
        assert(g.remove(t));
    assert(g.length == 0);
    assert((250UL in g) is null);

    // SPILL: a tag too far past the base for the window falls back to the AA
    // and stays fully visible through the same interface.
    UnackedMap sp;
    sp[1] = mk(1);
    immutable far = 1UL + UNACKED_WINDOW_MAX + 7;
    sp[far] = mk(2);
    assert(sp.length == 2);
    assert((far in sp) !is null && (far in sp).chan == 2);
    ulong[] both;
    foreach (t, ref u; sp)
        both ~= t;
    assert(both.length == 2 && both[0] == 1 && both[1] == far);
    // draining the window re-bases onto the spill so the fast path comes back
    assert(sp.remove(1));
    assert(sp.length == 1);
    assert((far in sp) !is null);
    assert(sp.base_ == far); // migrated out of the spill, back into the window
    assert(sp.remove(far));
    assert(sp.length == 0);

    // clear drops everything, window and spill alike
    UnackedMap c;
    c[1] = mk(1);
    c[1UL + UNACKED_WINDOW_MAX + 1] = mk(2);
    c.clear();
    assert(c.length == 0);
    assert((1UL in c) is null);
    c[9] = mk(3); // usable again after clear
    assert(c.length == 1 && (9UL in c) !is null);
}

private final class AmqpConn
{
    TlsLeg* tlsLeg; // null = plaintext
    TCPConnection tcp;
    TaskMutex wlock;
    Channel[ushort] chans;
    bool closing;
    // Management API (M4 v2): a cross-thread kill request. The mgmt thread sets
    // it under the registry mutex; THIS connection's own thread notices at its
    // next read-wait timeout and closes the socket (a cross-thread tcp.close is
    // unsafe in vibe-core — only the owning event loop may close).
    shared bool killReq;
    char[80] peerName = void; // "host:port" captured at connect (registry list)
    ubyte peerNameLen;
    char[64] loginUser = void; // authenticated user (registry list)
    ubyte loginUserLen;
    long connectedMs; // wall time at connect (registry age)
    bool[string] cancelledTags; // basic.cancel'ed consumer tags
    UnackedMap unacked; // delivery-tag -> record (no_ack=false consumers)
    size_t unackedBytes; // running sum of unacked blob bytes (byte-cap the window)
    size_t pendingBytes; // running sum of in-progress publish body bytes across channels (DoS cap)
    ulong nextTag = 1;
    ulong nextCtag = 1; // server-assigned consumer tags (unique per connection)
    ulong connId; // process-unique id (exclusive-queue ownership token)
    string[] exclQueues; // exclusive queues THIS connection declared (owns)
    size_t prefetch;    // basic.qos prefetch-count (0 = AMQP_DEFAULT_PREFETCH)
    uint chanGenCtr; // monotonic per-conn source for Channel.openGen (channel-reuse safe)
    size_t consumerCount; // live basic.consume fibers (per-conn cap)
    // live consumer fibers per (channel, gen) — key chan<<32|gen. channel.close
    // waits on this before its close-ok so no staged delivery trails it
    // ("Unsolicited delivery" kills the java client). Gen-keyed so a reopened
    // channel's fresh consumers don't block the OLD close.
    uint[ulong] chanConsumers;
    ulong flushSeq; // bumps after each serve-loop reply flush: a consumer's
    // FIRST delivery must wait for the flush carrying its consume-ok (a long
    // pipelined batch outlives the old fixed 1ms park -> "Unsolicited delivery")
    uint hbSendMs; // heartbeat SEND interval in MS (0 = disabled): the
    // negotiated interval HALVED, like RabbitMQ — a full-interval cadence sits
    // exactly on the client's reader-idle boundary and gets dropped (hb=1)
    uint hbSecs; // NEGOTIATED heartbeat (seconds): reads stalling past 2x this close the conn
    long lastReadMs; // MONOTONIC ms of the last bytes read (MonoTime — the
    // frozen per-command gClock behind nowMs() would leave stale stamps)
    bool hbStarted;  // the sender fiber is spawned exactly once
    const(void)* aclAuth; // authenticated ACL user (AclUser*); null = legacy accept-any
    bool opened; // connection.open-ok sent; data-plane classes gated behind this when ACL configured
    // NEGOTIATED max frame size. Starts at the spec's frame-min-size (4096):
    // until tune-ok lands, a peer frame beyond that is a negotiation-phase
    // frame error (rejectLargeFramesDuringConnectionNegotiation pins it).
    uint frameMax = 4096; // (from tune-ok): we
    // MUST NOT emit a frame larger than this, so a client that negotiated a smaller
    // frame-max doesn't see an over-size body frame (a fatal framing error to a
    // spec-strict receiver). Chunk size for bodies = frameMax - 8.

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
        if (c.tlsLeg !is null)
            cast(void) legSend(c.tlsLeg, c.tcp, bytes);
        else
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

/// The four AMQP 0-9-1 core exchange types dreads implements. RabbitMQ also
/// admits plugin types (x-*), but with no plugins loaded an unknown type is a
/// hard error — see exchangeTypeError below.
private bool validExType(scope const(char)[] t) @nogc nothrow pure @safe
{
    return t == "direct" || t == "fanout" || t == "topic" || t == "headers";
}

/// Send a connection-level close (class 10, method 50) on channel 0 — the HARD
/// error path: the client kills every channel and answers close-ok, which the
/// serve loop turns into the socket close (case 51). The caller returns TRUE
/// (keep reading for that close-ok); frames a pipelining client already sent
/// after the offending one are still processed — acceptable for now, RabbitMQ
/// discards them.
private void connectionClose(ref ByteBuffer o, ushort code, scope const(char)[] text,
        ushort cls, ushort mth) nothrow @trusted
{
    method(o, 0, 10, 50, (ref ByteBuffer b) @nogc nothrow {
        putU16(b, code);
        putShortStr(b, text);
        putU16(b, cls);
        putU16(b, mth);
    });
}

/// Send a soft channel-level close (class 20, method 40) and drop the channel;
/// the connection survives (the client opens a fresh channel). `cls`/`mth` name
/// the offending method. The caller must also `c.chans.remove(chan)` — see the
/// NOT_FOUND sites — so the dead channel's state (consumers, tx) is released.
private void channelClose(ref ByteBuffer o, ushort chan, ushort code,
        scope const(char)[] text, ushort cls, ushort mth) nothrow @trusted
{
    method(o, chan, 20, 40, (ref ByteBuffer b) @nogc nothrow {
        putU16(b, code);
        putShortStr(b, text);
        putU16(b, cls);
        putU16(b, mth);
    });
}

// ---------------------------------------------------------------------------
// Serve loop

public void serveAmqpClient(TCPConnection tcp, bool tls = false) nothrow
{
    TlsLeg* leg;
    if (tls)
    {
        leg = TlsLeg.create(true);
        if (leg is null)
        {
            try
                tcp.close();
            catch (Exception)
            {
            }
            return;
        }
    }
    scope (exit)
        if (leg !is null)
            leg.free(); // nulled below once a conn (or the 1.0 skin) owns it

    seedWellKnownExchanges(); // once per shard thread: the mandated amq.* defaults
    try
        tcp.tcpNoDelay = true;
    catch (Exception)
    {
    }
    auto c = new AmqpConn(tcp);
    c.tlsLeg = leg; // conn owns it for send/recv; serve's scope(exit) still frees
    static void closeQuiet(AmqpConn cc) nothrow
    {
        try
            cc.tcp.close();
        catch (Exception)
        {
        }
    }

    import core.atomic : atomicOp;

    c.connId = atomicOp!"+="(gAmqpConnGen, 1);
    // Reply-to anti-forgery secret: seed ONCE from a CSPRNG. The old
    // monoMs()^pointer seed was guessable — an attacker could forge another
    // client's direct-reply token and inject into its reply consumer. CAS so
    // concurrent first-connects converge on a single value; the weak scheme
    // survives only as a last-resort fallback if the CSPRNG is unavailable.
    {
        import core.atomic : cas, atomicLoad;
        import dreads.tls : tlsRandBytes;

        if (atomicLoad(gDrSecret) == 0)
        {
            ubyte[8] rb = void;
            ulong seed = 0;
            if (tlsRandBytes(rb[]))
                seed = (cast(ulong) rb[0]) | (cast(ulong) rb[1] << 8)
                    | (cast(ulong) rb[2] << 16) | (cast(ulong) rb[3] << 24)
                    | (cast(ulong) rb[4] << 32) | (cast(ulong) rb[5] << 40)
                    | (cast(ulong) rb[6] << 48) | (cast(ulong) rb[7] << 56);
            if (seed == 0)
                seed = monoMs() * 0x9E3779B97F4A7C15 + cast(ulong) cast(void*) c;
            cast(void) cas(&gDrSecret, 0UL, seed);
        }
    }
    c.connectedMs = nowMs();
    try
    {
        auto ra = tcp.remoteAddress.toString();
        immutable rl = ra.length < c.peerName.length ? ra.length : c.peerName.length;
        c.peerName[0 .. rl] = ra[0 .. rl];
        c.peerNameLen = cast(ubyte) rl;
    }
    catch (Exception)
    {
    }
    try
        gConnsById[c.connId] = c; // direct-reply routing (this shard's conns)
    catch (Exception)
    {
    }
    {
        import dreads.shard : tShard;

        amqpRegAdd(c, tShard); // cross-shard mgmt registry
    }
    startKillWatcher(c); // notices a mgmt-API kill request, closes on THIS thread
    scope (exit)
    {
        gConnsById.remove(c.connId);
        amqpRegRemove(c.connId);
        c.closing = true;
        requeueAllUnacked(c);
        // exclusive queues die with their owning connection (RabbitMQ):
        // tombstone the existence set (live consumers elsewhere get their
        // Consumer Cancel Notification via the queueExists poll) and DEL the
        // backing list.
        foreach (q; c.exclQueues)
        {
            if (auto po = q in gQueueOwner)
                if (*po == c.connId)
                {
                    auto adSeeds = bindingSourcesTo(q, false);
                    ctlBroadcast(9, q, "", "");
                    autoDeleteExchangeSweep(adSeeds);
                    static ByteBuffer xk; // TLS: teardown runs serially per conn
                    queueKey(q, xk);
                    if (gAmqpDelKey !is null)
                        gAmqpDelKey(xk.data.asChars);
                }
        }
        releaseChannels(c); // free malloc-plane bufs on THIS (owning) thread
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
            if (leg !is null)
            {
                got += legTake(leg, hdr[got .. 8]);
                if (got == 8)
                    break;
                if (!legPump(leg, tcp) || !legSend(leg, tcp, null))
                    return; // handshake flush is single-fiber here (no conn yet)
                continue;
            }
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
        if (hdr[0 .. 4] != "AMQP" || hdr[4] != 0 || hdr[5] != 0 || hdr[6] != 9 || hdr[7] != 1)
        {
            // AMQP 1.0 speaks on the SAME port with its own headers: protocol
            // id 3 = SASL layer, 0 = bare 1.0 (both versioned 1.0.0). Hand the
            // connection to the 1.0 skin (dreads.amqp10).
            if (hdr[0 .. 4] == "AMQP" && hdr[5] == 1 && hdr[6] == 0 && hdr[7] == 0
                    && (hdr[4] == 3 || hdr[4] == 0))
            {
                import dreads.amqp10 : amqp10Serve;

                auto legHand = leg;
                leg = null; // the 1.0 skin owns it now (frees on its teardown)
                amqp10Serve(tcp, hdr[4] == 3, legHand);
                return;
            }
            // bad/unsupported protocol header: reply with the header we DO
            // support, then close (the 0-9-1 spec's negotiation-failure path —
            // the java suite's crazyProtocolHeader reads it back)
            if (leg !is null)
                cast(void) legSend(leg, tcp, cast(const(ubyte)[]) "AMQP\x00\x00\x09\x01");
            else
            {
                try
                    tcp.write(cast(const(ubyte)[]) "AMQP\x00\x00\x09\x01");
                catch (Exception)
                {
                }
            }
            return;
        }
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
            // RabbitMQ-compatible dialect version, same pattern as the RESP
            // INFO redis_version: clients (and the rabbitmq-java-client test
            // harness) parse server-properties["version"] to gate features —
            // its absence NPEs every BrokerTestCase setUp. 3.13 is the RabbitMQ
            // our error semantics are modeled on.
            putShortStr(o, "version");
            o.appendByte('S');
            putLongStr(o, "3.13.0");
            putShortStr(o, "platform");
            o.appendByte('S');
            putLongStr(o, "D/dreads");
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
            putShortStr(o, "consumer_cancel_notify"); // we push basic.cancel on queue delete
            o.appendByte('t');
            o.appendByte(1);
            putShortStr(o, "exchange_exchange_bindings"); // op 6/7 e2e bindings
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
            putLongStr(o, "PLAIN AMQPLAIN");
            putLongStr(o, "en_US");
        });
        sendTo(c, outb.data);
        outb.clear();
    }

    // TLS: plaintext decrypted PAST the 8-byte header (e.g. a pipelined
    // StartOk) is already in pin — parse it BEFORE blocking for fresh bytes,
    // or a client that awaits our reply deadlocks against our waitForData.
    bool preDrained = false;
    if (leg !is null && !leg.pin.empty)
    {
        legDrainInto(leg, inb);
        preDrained = true;
    }
    for (;;)
    {
        if (preDrained)
            preDrained = false; // one shot: parse what we already hold
        else
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
            if (leg !is null)
            {
                if (!legPump(leg, tcp))
                    return;
                legDrainInto(leg, inb);
                sendTo(c, null); // flush handshake cipher under the wlock
                c.lastReadMs = monoMs();
            }
            else
            {
                auto space = inb.freeSpace(cast(size_t) avail);
                if (space.length < cast(size_t) avail)
                    return; // OOM growing the input buffer: drop THIS client, not the broker
                try
                    tcp.read(space[0 .. cast(size_t) avail]);
                catch (Exception)
                    return;
                c.lastReadMs = monoMs(); // heartbeat dead-peer clock
                inb.grow(cast(size_t) avail);
            }
        }

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
            if (cast(ulong) fsize + 8 > c.frameMax)
            {
                // exceeds the NEGOTIATED frame-max (frame-max counts header +
                // payload + end octet): connection-level 501 FRAME_ERROR, like
                // RabbitMQ (rejectExceedingFrameMax pins the close code)
                connectionClose(outb, 501, "FRAME_ERROR - frame too large", 0, 0);
                sendTo(c, outb.data);
                try
                    sleep(100.msecs); // let the peer READ the close before the RST
                catch (Exception)
                {
                }
                return;
            }
            if (d.length - pos < 7 + cast(size_t) fsize + 1) // size_t: no u32 wrap
                break;
            auto payload = d[pos + 7 .. pos + 7 + fsize];
            if (d[pos + 7 + fsize] != FRAME_END)
                return; // framing error
            if (!handleFrame(c, ftype, chan, payload, outb))
            {
                if (gAmqpAofFlush !is null)
                    gAmqpAofFlush();
                if (!outb.empty)
                    sendTo(c, outb.data);
                return;
            }
            pos += 7 + fsize + 1;
        }
        if (!outb.empty)
        {
            if (gAmqpAofFlush !is null)
                gAmqpAofFlush(); // AOF durable BEFORE the confirm/reply is sent
            sendTo(c, outb.data);
            outb.clear();
        }
        c.flushSeq++; // parked first-delivery fibers may proceed
        inb.consume(pos);
    }
}

private bool handleFrame(AmqpConn c, ubyte ftype, ushort chan,
        scope const(ubyte)[] p, ref ByteBuffer o) nothrow @trusted
{
    if (ftype == FRAME_HEARTBEAT)
    {
        if (chan != 0)
        {
            connectionClose(o, 505, "UNEXPECTED_FRAME - heartbeat on non-zero channel", 0, 0);
            return true;
        }
        return true;
    }
    if (ftype == FRAME_HEADER)
    {
        auto ch = chan in c.chans;
        if (ch is null)
            return true; // closed-channel content is discarded (spec)
        if (!ch.pub.active)
        {
            connectionClose(o, 505, "UNEXPECTED_FRAME - content header without method", 60, 0);
            return true;
        }
        Rd r = Rd(p);
        immutable hcls = r.u16(); // class: must echo the method's (basic = 60)
        if (hcls != 60)
        {
            connectionClose(o, 505, "UNEXPECTED_FRAME - header class mismatch", 60, 0);
            ch.pub.active = false;
            return true;
        }
        cast(void) r.u16(); // weight
        ch.pub.bodySize = r.u64();
        if (ch.pub.bodySize > AMQP_MAX_BODY)
            return false; // oversized message body: close
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
        if (ch is null)
            return true; // closed-channel content: discard
        if (!ch.pub.active || ch.pub.bodySize == 0)
        {
            connectionClose(o, 505, "UNEXPECTED_FRAME - unexpected content body", 60, 0);
            ch.pub.active = false;
            return true;
        }
        ch.pub.payload.append(p);
        // Per-connection cap on the SUM of in-progress body assemblies across all
        // channels: a client that opens many channels and withholds each final
        // body frame would otherwise pin unbounded memory. Completion
        // (finishPublish) and channel drop (requeueAndDropChannel) decrement it.
        c.pendingBytes += p.length;
        if (c.pendingBytes > AMQP_MAX_PENDING_BYTES)
        {
            connectionClose(o, 501, "FRAME_ERROR - staged publish bodies exceed the connection limit", 60, 40);
            return false; // over budget: close the connection
        }
        if (ch.pub.payload.length >= ch.pub.bodySize)
            finishPublish(c, chan, *ch, o);
        return true;
    }
    if (ftype != FRAME_METHOD)
    {
        connectionClose(o, 501, "FRAME_ERROR - unknown frame type", 0, 0);
        return true;
    }

    Rd r = Rd(p);
    immutable cls = r.u16();
    if (cls != 10 && cls != 20)
        if (auto mch = chan in c.chans)
            if (mch.pub.active && mch.pub.bodySize > 0
                    && mch.pub.payload.length < mch.pub.bodySize)
            {
                connectionClose(o, 505, "UNEXPECTED_FRAME - incomplete content", cast(ushort) cls, 0);
                mch.pub.active = false;
                return true;
            }
    immutable mth = r.u16();
    // HANDSHAKE GATE: with an ACL configured (aclUserCount()>1), no class other
    // than connection (10) may be dispatched until connection.open-ok has been
    // sent. Legacy accept-any (aclUserCount()<=1) leaves the gate open (unchanged).
    if (cls != 10 && !c.opened)
    {
        import dreads.acl : aclUserCount;

        if (aclUserCount() > 1)
        {
            connectionClose(o, 503, "COMMAND_INVALID - expected connection.open", cast(ushort) cls, 0);
            return true;
        }
    }
    switch (cls)
    {
    case 10: // connection
        switch (mth)
        {
        case 11: // start-ok
            auto clientProps = r.tableRaw(); // client-properties (capabilities live here)
            auto saslMech = r.shortStr(); // PLAIN or AMQPLAIN
            auto saslResp = r.longStr(); // PLAIN: authzid NUL authcid NUL password
            cast(void) r.shortStr();
            {
                // Validate against the ACL exactly like the MQTT skin: an
                // EMPTY registry (no `default` user defined) keeps the legacy
                // accept-any behavior; once ACL users exist, an unknown user,
                // a disabled one, or a wrong password is a 403 ACCESS_REFUSED
                // connection close (RabbitMQ's authentication_failure_close).
                import dreads.acl : aclUser, aclCheckPassword, aclUserCount;

                const(char)[] auser, apass;
                if (saslMech == "AMQPLAIN")
                {
                    // AMQPLAIN: the response is a field TABLE (no length
                    // prefix) with LOGIN and PASSWORD longstrs
                    cast(void) tableWalk(cast(const(ubyte)[]) saslResp, (scope const(char)[] k, char ty,
                            scope const(ubyte)[] v) @nogc nothrow {
                        if (ty == 'S' && k == "LOGIN")
                            auser = cast(const(char)[]) v;
                        else if (ty == 'S' && k == "PASSWORD")
                            apass = cast(const(char)[]) v;
                        return true;
                    });
                }
                else
                {
                    auto sr = cast(const(char)[]) saslResp;
                    size_t z1 = sr.length, z2 = sr.length;
                    foreach (i, ch2; sr)
                        if (ch2 == '\0')
                        {
                            if (z1 == sr.length)
                                z1 = i;
                            else
                            {
                                z2 = i;
                                break;
                            }
                        }
                    if (z1 < sr.length && z2 < sr.length)
                    {
                        auser = sr[z1 + 1 .. z2];
                        apass = sr[z2 + 1 .. $];
                    }
                }
                auto au = aclUser(auser.length ? auser : "default");
                // Legacy accept-any until the operator actually CONFIGURES
                // users: a fresh dreads seeds only the nopass `default` user,
                // and virtually every AMQP client sends guest/guest — refusing
                // them on an unconfigured broker would break existing deploys.
                immutable aclOff = aclUserCount() <= 1;
                if (!aclOff && (au is null || !au.enabled || !aclCheckPassword(au, apass)))
                {
                    // the polite 403-in-handshake is OPT-IN: only clients
                    // advertising the authentication_failure_close capability
                    // get it (RabbitMQ). Everyone else gets the historical
                    // abrupt hangup (the java suite pins BOTH behaviors).
                    bool authFailClose = false;
                    cast(void) tableWalk(cast(const(ubyte)[]) clientProps,
                            (scope const(char)[] k, char ty, scope const(ubyte)[] v) @nogc nothrow {
                        if (k == "capabilities" && ty == 'F')
                            cast(void) tableWalk(v, (scope const(char)[] k2, char t2,
                                    scope const(ubyte)[] v2) @nogc nothrow {
                                if (k2 == "authentication_failure_close"
                                        && t2 == 't' && v2.length && v2[0])
                                    authFailClose = true;
                                return true;
                            });
                        return true;
                    });
                    if (!authFailClose)
                        return false; // abrupt close: no capability, no close-method
                    connectionClose(o, 403,
                            "ACCESS_REFUSED - Login was refused using authentication mechanism PLAIN. For details see the broker logfile.",
                            0, 0);
                    return true; // stay open for the client's close-ok
                }
                c.aclAuth = au; // null under legacy accept-any
                if (auser.length && auser.length <= c.loginUser.length)
                {
                    c.loginUser[0 .. auser.length] = auser;
                    c.loginUserLen = cast(ubyte) auser.length;
                }
            }
            method(o, 0, 10, 30, (ref ByteBuffer b) @nogc nothrow {
                putU16(b, 2047); // channel-max
                putU32(b, 131072); // frame-max
                putU16(b, 30); // heartbeat we PROPOSE; the client's tune-ok
            });                //  (below) is what we actually honor
            return true;
        case 31: // tune-ok: channel-max u16, frame-max u32, heartbeat u16
            {
                cast(void) r.u16(); // channel-max (we accept the client's)
                immutable fm = r.u32(); // frame-max (u32): the client's negotiated max
                // a frame-max below the spec's frame-min-size (4096) is a
                // negotiation failure: RabbitMQ hangs up (the java suite's
                // frameMaxLessThanFrameMinSize pins the close)
                if (fm != 0 && fm < 4096)
                    return false;
                // honor it (bounded to [4096, our proposal]); 0 = "no limit" per
                // spec, so fall back to our own max. Prevents emitting a body frame
                // larger than a down-negotiating client's frame-max.
                c.frameMax = fm == 0 ? AMQP_FRAME_MAX
                    : (fm > AMQP_FRAME_MAX ? AMQP_FRAME_MAX : fm);
                if (c.frameMax < 4096)
                    c.frameMax = 4096;
                immutable hb = r.u16(); // NEGOTIATED heartbeat interval, seconds
                // Send at half the negotiated interval so the client always
                // sees a frame within its dead-peer window (2× interval).
                // 0 = heartbeats disabled: don't start the sender at all.
                c.hbSecs = hb;
                c.lastReadMs = monoMs();
                c.hbSendMs = hb == 0 ? 0 : (cast(uint) hb * 1000) / 2;
                if (c.hbSendMs != 0)
                    startHeartbeat(c);
                return true;
            }
        case 40: // open
            {
                auto vhost = r.shortStr();
                // dreads has exactly ONE vhost, "/": opening any other is a 530
                // NOT_ALLOWED connection close, like RabbitMQ's vhost-not-found.
                if (vhost.length && vhost != "/")
                {
                    connectionClose(o, 530, "NOT_ALLOWED - vhost not found", 10, 40);
                    return true; // stay open for close-ok
                }
                // vhost ACCESS: an authenticated user with no key grant and no
                // channel grant at all can touch nothing — RabbitMQ's
                // no-vhost-permission maps to a 530 at open.
                {
                    import dreads.acl : AclUser;

                    auto aup = cast(const(AclUser)*) c.aclAuth;
                    if (aup !is null && !aup.root.allKeys && aup.root.keyPats.length == 0
                            && !aup.root.allChannels && aup.root.chanPats.length == 0)
                    {
                        connectionClose(o, 530, "NOT_ALLOWED - access to vhost '/' refused", 10, 40);
                        return true;
                    }
                }
                method(o, 0, 10, 41, (ref ByteBuffer b) @nogc nothrow {
                    putShortStr(b, "");
                });
                c.opened = true; // handshake complete — data-plane classes now allowed
                return true;
            }
        case 50: // close
            method(o, 0, 10, 51);
            return false;
        case 51: // close-ok
            return false;
        case 70: // update-secret: new-secret (longstr), reason (shortstr)
            // dreads carries no per-connection secret to rotate (auth is
            // accept-any PLAIN), so just acknowledge and keep the connection
            // open — a client's periodic credential refresh must not drop it.
            method(o, 0, 10, 71); // update-secret-ok
            return true;
        default:
            return true;
        }
    case 20: // channel
        switch (mth)
        {
        case 10: // open
            if (chan !in c.chans && c.chans.length >= AMQP_MAX_CHANNELS)
                return false; // channel-max exceeded: close the connection
            try
            {
                c.chans[chan] = Channel(true);
                c.chans[chan].openGen = ++c.chanGenCtr; // fresh gen: reopens are distinct
            }
            catch (Exception)
            {
            }
            method(o, chan, 20, 11, (ref ByteBuffer b) @nogc nothrow {
                putLongStr(b, "");
            });
            return true;
        case 40: // close
            {
                // requeue this channel's in-flight (unacked) records and stop its
                // consumer fibers (they observe the gen/existence mismatch and exit)
                // before dropping the channel — else those popped messages are lost.
                uint cgen = 0;
                if (auto pcc = chan in c.chans)
                    cgen = pcc.openGen;
                requeueAndDropChannel(c, chan);
                // HOLD the close-ok until this channel's consumer fibers exit: a
                // delivery burst staged during the close would hit the wire AFTER
                // the close-ok — the java client reads a delivery on a closed
                // channel as "Unsolicited delivery" and kills the connection.
                try
                {
                    immutable ckey = (cast(ulong) chan) << 32 | cgen;
                    int spins = 0;
                    while ((ckey in c.chanConsumers) !is null && spins++ < 200)
                        sleep(1.msecs);
                }
                catch (Exception)
                {
                }
                method(o, chan, 20, 41);
                return true;
            }
        case 20: // flow: we never throttle -> acknowledge, echoing the state
            {
                immutable active = r.u8() & 1;
                method(o, chan, 20, 21, (ref ByteBuffer b) @nogc nothrow {
                    b.appendByte(cast(char) active); // flow-ok
                });
                return true;
            }
        default:
            return true;
        }
    case 40: // exchange
        if (mth == 10) // declare
        {
            cast(void) r.u16();
            auto exRaw = r.shortStr();
            // RabbitMQ strips CR/LF from declared exchange names (the java
            // suite pins it): "e\nxc\rhange" declares "exchange"
            char[256] exbuf = void;
            size_t exn = 0;
            foreach (xc; exRaw)
                if (xc != '\n' && xc != '\r' && exn < exbuf.length)
                    exbuf[exn++] = xc;
            auto ex = cast(const(char)[]) exbuf[0 .. exn];
            auto typ = r.shortStr();
            immutable flags = r.u8(); // passive/durable/auto-delete/internal/no-wait
            immutable passive = (flags & 0x01) != 0;
            if (passive)
            {
                // existence probe: an unknown exchange is a channel 404
                // NOT_FOUND. The default exchange ("") and the seeded amq.*
                // always exist; a passive declare never creates or type-checks.
                if (ex.length && (cast(string) ex !in gExchanges))
                {
                    channelClose(o, chan, 404, "NOT_FOUND - no exchange", 40, 10);
                    c.chans.remove(chan);
                    return true;
                }
                method(o, chan, 40, 11);
                return true;
            }
            // [bug 22101] the default exchange cannot be (re)declared: 403
            if (ex.length == 0)
            {
                channelClose(o, chan, 403,
                        "ACCESS_REFUSED - operation not permitted on the default exchange",
                        40, 10);
                c.chans.remove(chan);
                return true;
            }
            // An unknown exchange type is a channel 406 PRECONDITION_FAILED (the
            // connection survives), matching what RabbitMQ returns for a declare
            // with a type no plugin provides. An empty type keeps the historical
            // default-to-direct.
            if (typ.length && !validExType(typ))
            {
                channelClose(o, chan, 406, "PRECONDITION_FAILED - invalid exchange type", 40, 10);
                c.chans.remove(chan);
                return true;
            }
            // equivalence: redeclaring an EXISTING exchange with a different
            // type OR different durable/auto-delete/internal flags is a 406
            // PRECONDITION_FAILED (RabbitMQ inequivalent-args)
            if (typ.length)
                if (auto pt = (cast(string) ex) in gExchanges)
                {
                    immutable ExType want = typ == "fanout" ? ExType.fanout
                        : typ == "topic" ? ExType.topic
                        : typ == "headers" ? ExType.headers : ExType.direct;
                    if (*pt != want)
                    {
                        channelClose(o, chan, 406,
                                "PRECONDITION_FAILED - inequivalent arg 'type'", 40, 10);
                        c.chans.remove(chan);
                        return true;
                    }
                    if (auto pfx = (cast(string) ex) in gExchFlags)
                        if (*pfx != (flags & 0x0E))
                        {
                            channelClose(o, chan, 406,
                                    "PRECONDITION_FAILED - inequivalent flags", 40, 10);
                            c.chans.remove(chan);
                            return true;
                        }
                }
            char[1] xfb = [cast(char)(flags & 0x0E)];
            auto exArgsTbl = r.tableRaw();
            auto aeName = tableGetStr(exArgsTbl, "alternate-exchange");
            ctlBroadcast(1, ex, typ, xfb[], cast(const(ubyte)[]) aeName);
            if (!(flags & 16)) // nowait: the client forbade declare-ok — an
                method(o, chan, 40, 11); // extra reply desyncs its RPC stream
            return true;
        }
        if (mth == 20) // delete
        {
            cast(void) r.u16();
            auto ex = r.shortStr();
            immutable dbits = r.ok && r.i < p.length ? p[r.i] : 0; // if-unused|nowait
            if (ex.length == 0) // [bug 22101] no delete on the default exchange
            {
                channelClose(o, chan, 403,
                        "ACCESS_REFUSED - operation not permitted on the default exchange",
                        40, 20);
                c.chans.remove(chan);
                return true;
            }
            {
                import dreads.acl : aclUserCount, aclCanAccessKey, AclUser;

                if (aclUserCount() > 1) // per-op ACL on the exchange
                {
                    auto au = cast(const(AclUser)*) c.aclAuth;
                    if (au !is null && !aclCanAccessKey(au, ex, false, true))
                    {
                        channelClose(o, chan, 403, "ACCESS_REFUSED - access to exchange refused", 40, 20);
                        c.chans.remove(chan);
                        return true;
                    }
                }
            }
            if ((dbits & 1) && exchangeHasLiveBindings(cast(string) ex))
            {
                // if-unused: an exchange with live bindings is "in use" — the
                // delete is a channel 406 PRECONDITION_FAILED (RabbitMQ)
                channelClose(o, chan, 406, "PRECONDITION_FAILED - in use", 40, 20);
                c.chans.remove(chan);
                return true;
            }
            // e2e sources pointing AT this exchange lose a binding when it dies
            auto adSeeds = bindingSourcesTo(ex, true);
            ctlBroadcast(5, ex, "", ""); // op 5: drop the exchange + its bindings
            autoDeleteExchangeSweep(adSeeds);
            if (!(dbits & 2))
                method(o, chan, 40, 21); // delete-ok (suppressed by nowait)
            return true;
        }
        if (mth == 30) // bind (exchange-to-exchange): dest, source, rk, args
        {
            cast(void) r.u16();
            auto dest = r.shortStr();
            auto source = r.shortStr();
            auto rk = r.shortStr();
            immutable ebnw = r.u8() & 1; // no-wait
            auto bindArgs = r.tableRaw();
            ctlBroadcast(6, source, dest, rk, bindArgs); // op 6: source -> dest exch
            if (!ebnw)
                method(o, chan, 40, 31); // bind-ok (suppressed by nowait)
            return true;
        }
        if (mth == 40) // unbind (exchange-to-exchange)
        {
            cast(void) r.u16();
            auto dest = r.shortStr();
            auto source = r.shortStr();
            auto rk = r.shortStr();
            immutable eunw = r.u8() & 1; // no-wait
            cast(void) r.tableRaw();
            ctlBroadcast(7, source, dest, rk); // op 7: drop the e2e binding
            try
                autoDeleteExchangeSweep([cast(string) source.idup]);
            catch (Exception)
            {
            }
            if (!eunw)
                method(o, chan, 40, 51); // unbind-ok (suppressed by nowait)
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
                immutable qflags = r.u8(); // passive/durable/exclusive/auto-delete/no-wait
                immutable passive = (qflags & 0x01) != 0;
                auto argsTbl = r.tableRaw();
                // [queue.declare] an EMPTY name means the server assigns a unique
                // one and returns it (the RPC reply-queue / temporary-queue
                // pattern). STACK-local (not TLS) so the name survives the
                // ctlBroadcast yield below. The queue itself needs no explicit
                // creation — it's a keyspace list, usable as soon as it's named.
                char[256] qbuf = void;
                const(char)[] qq;
                if (q.length == 0)
                {
                    import core.stdc.stdio : snprintf;

                    immutable n = snprintf(qbuf.ptr, qbuf.length, "amq.gen-%llu",
                            cast(ulong) atomicOp!"+="(gAmqpQueueGen, 1));
                    qq = cast(const(char)[]) qbuf[0 .. n];
                }
                else
                {
                    // RabbitMQ strips CR/LF from declared queue names (bug
                    // 21846-era behavior the java suite pins): "a\nb\r" = "ab"
                    size_t qn = 0;
                    foreach (qc; q)
                        if (qc != '\n' && qc != '\r' && qn < qbuf.length)
                            qbuf[qn++] = qc;
                    qq = cast(const(char)[]) qbuf[0 .. qn];
                }
                if (argsTbl !is null && argsTbl.length)
                {
                    // arg validation (RabbitMQ equivalence rules): x-message-ttl
                    // must be an integer >= 0; x-expires an integer > 0. A wrong
                    // type or range is a channel 406 PRECONDITION_FAILED.
                    long ttlV, expV;
                    immutable tk = tableIntKind(argsTbl, "x-message-ttl", ttlV);
                    immutable ek = tableIntKind(argsTbl, "x-expires", expV);
                    // x-dead-letter-*: the exchange/routing-key must be STRINGS,
                    // and a routing-key without its exchange is meaningless —
                    // all inequivalent-arg 406s, like RabbitMQ.
                    immutable dk = tableStrKind(argsTbl, "x-dead-letter-exchange");
                    immutable rk2 = tableStrKind(argsTbl, "x-dead-letter-routing-key");
                    long mlV;
                    immutable mk = tableIntKind(argsTbl, "x-max-length", mlV);
                    if (tk < 0 || (tk > 0 && ttlV < 0) || ek < 0 || (ek > 0 && expV <= 0)
                            || mk < 0 || (mk > 0 && mlV < 0)
                            || dk < 0 || rk2 < 0 || (rk2 > 0 && dk == 0))
                    {
                        channelClose(o, chan, 406,
                                "PRECONDITION_FAILED - invalid arg", 50, 10);
                        c.chans.remove(chan);
                        return true;
                    }
                    // EQUIVALENCE on redeclare: an existing queue's x-args must
                    // match the stored ones (406 otherwise). The old MERGE let a
                    // previous declaration's ttl/dlx leak into a redeclare that
                    // omitted them — messages then expired under a TTL the
                    // client never asked for.
                    if (!passive && queueExists(qq))
                    {
                        // (plain-queue + new args intentionally NOT 406'd: the
                        // TTLHandling harness re-binds the same queue name with
                        // fresh args per case after deletes that may still be
                        // replicating — only a STORED-vs-REQUESTED mismatch is
                        // an unambiguous inequivalence.)
                        if (auto m0 = (cast(string) qq) in gQueueMeta)
                        {
                            auto rdlx = tableGetStr(argsTbl, "x-dead-letter-exchange");
                            auto rdrk = tableGetStr(argsTbl, "x-dead-letter-routing-key");
                            immutable rml = mk > 0 ? mlV + 1 : 0; // +1-encoded like the meta
                            immutable mm = (m0.ttlSet != (tk > 0))
                                || (m0.ttlSet && m0.ttlMs != ttlV)
                                || (m0.expSet != (ek > 0))
                                || (m0.expSet && m0.expMs != expV)
                                || (m0.maxLen != rml)
                                || (m0.dlxSet != (rdlx !is null))
                                || (m0.dlx != (rdlx is null ? "" : rdlx))
                                || (m0.dlrk != (rdrk is null ? "" : rdrk));
                            if (mm)
                            {
                                channelClose(o, chan, 406,
                                        "PRECONDITION_FAILED - inequivalent arg", 50, 10);
                                c.chans.remove(chan);
                                return true;
                            }
                        }
                    }
                    auto dlx = tableGetStr(argsTbl, "x-dead-letter-exchange");
                    auto dlrk = tableGetStr(argsTbl, "x-dead-letter-routing-key");
                    immutable ttl = tk > 0 ? ttlV : 0;
                    long mxl2;
                    immutable mlPresent = tableIntKind(argsTbl, "x-max-length", mxl2) > 0;
                    // x-overflow: "drop-head" (default) or "reject-publish".
                    // reject-publish-dlx is NOT claimed — silently treating it
                    // as plain reject would drop messages a client expects to
                    // find in its dead-letter queue.
                    ubyte ovfEnc = 0;
                    {
                        auto ovs = tableGetStr(argsTbl, "x-overflow");
                        if (ovs !is null)
                        {
                            if (ovs == "reject-publish")
                                ovfEnc = 1;
                            else if (ovs != "drop-head")
                            {
                                channelClose(o, chan, 406,
                                        "PRECONDITION_FAILED - invalid arg 'x-overflow'", 50, 10);
                                c.chans.remove(chan);
                                return true;
                            }
                        }
                    }
                    // ENCODED as value+1 on the wire and in the meta: maxlen 0
                    // is a VALID bound (the queue holds nothing) and must be
                    // distinguishable from unset.
                    immutable mlEnc = mlPresent ? mxl2 + 1 : 0;
                    long mpV;
                    immutable mpPresent = tableIntKind(argsTbl, "x-max-priority", mpV) > 0;
                    if (mpPresent && (mpV < 0 || mpV > 255))
                    {
                        channelClose(o, chan, 406,
                                "PRECONDITION_FAILED - invalid arg 'x-max-priority'", 50, 10);
                        c.chans.remove(chan);
                        return true;
                    }
                    immutable mpEnc = mpPresent ? cast(ubyte) mpV : 0;
                    long dlV;
                    immutable dlPresent = tableIntKind(argsTbl, "x-delivery-limit", dlV) > 0;
                    if (dlPresent && dlV < 0)
                    {
                        channelClose(o, chan, 406,
                                "PRECONDITION_FAILED - invalid arg 'x-delivery-limit'", 50, 10);
                        c.chans.remove(chan);
                        return true;
                    }
                    immutable dlEnc = dlPresent ? dlV + 1 : 0;
                    if (dlx !is null || dlrk !is null || tk > 0 || mlEnc > 0 || ek > 0
                            || ovfEnc != 0 || dlEnc > 0 || mpEnc != 0)
                    {
                        ubyte[35] tb = void;
                        foreach (k; 0 .. 8)
                            tb[k] = cast(ubyte)(ttl >> ((7 - k) * 8));
                        foreach (k; 0 .. 8)
                            tb[8 + k] = cast(ubyte)(mlEnc >> ((7 - k) * 8));
                        // presence flags: "" dlx (default exchange) and ttl 0
                        // (expire immediately) are real values, not absence
                        tb[16] = cast(ubyte)((dlx !is null ? 1 : 0)
                                | (dlrk !is null ? 2 : 0) | (tk > 0 ? 4 : 0)
                                | (ek > 0 ? 8 : 0));
                        foreach (k; 0 .. 8)
                            tb[17 + k] = cast(ubyte)((ek > 0 ? expV : 0) >> ((7 - k) * 8));
                        tb[25] = ovfEnc; // appended: older peers stop at 25 and default to drop-head
                        foreach (k; 0 .. 8)
                            tb[26 + k] = cast(ubyte)(dlEnc >> ((7 - k) * 8));
                        tb[34] = mpEnc;
                        ctlBroadcast(3, qq, dlx is null ? "" : dlx,
                                dlrk is null ? "" : dlrk, tb[]);
                    }
                }
                // passive declare probes existence only: an unknown NAMED queue
                // is a channel-level 404 NOT_FOUND (the connection lives, the
                // client opens a fresh channel), exactly like RabbitMQ. An empty
                // name is never passive-checked (it always names a fresh server
                // queue). A non-passive declare records the name in the
                // existence set so later passive/consume probes resolve it.
                if (exclusiveDenied(c, chan, o, qq, 50, 10))
                    return true; // another connection's exclusive queue: 405
                // redeclare with different durable/exclusive/auto-delete is a
                // 406 PRECONDITION_FAILED (RabbitMQ inequivalence). Passive
                // declares never flag-check.
                if (!passive)
                    if (auto pf = (cast(string) qq) in gQueueFlags)
                        if (*pf != (qflags & 0x0E))
                        {
                            // redeclaring an EXISTING non-exclusive queue as
                            // exclusive is a 405 RESOURCE_LOCKED (RabbitMQ);
                            // every other flag mismatch stays a 406
                            if ((qflags & 0x04) && !(*pf & 0x04))
                                channelClose(o, chan, 405,
                                        "RESOURCE_LOCKED - not exclusive", 50, 10);
                            else
                                channelClose(o, chan, 406,
                                        "PRECONDITION_FAILED - inequivalent flags", 50, 10);
                            c.chans.remove(chan);
                            return true;
                        }
                if (passive)
                {
                    bool drOk = false;
                    if (qq == DIRECT_REPLY_Q)
                        drOk = true; // the pseudo-queue always "exists"
                    else if (qq.length > DIRECT_REPLY_Q.length
                            && qq[0 .. DIRECT_REPLY_Q.length] == DIRECT_REPLY_Q)
                    {
                        ulong dcid;
                        uint dch, dgen;
                        // a VALID reply token "exists"; a tampered one 404s
                        drOk = drParse(qq, dcid, dch, dgen);
                        if (!drOk)
                        {
                            channelClose(o, chan, 404, "NOT_FOUND - no queue", 50, 10);
                            c.chans.remove(chan);
                            return true;
                        }
                    }
                    if (!drOk && q.length && !queueExists(qq))
                    {
                        channelClose(o, chan, 404, "NOT_FOUND - no queue", 50, 10);
                        c.chans.remove(chan);
                        return true;
                    }
                }
                else if (!queueExists(qq))
                {
                    // broadcast is a stream — clients redeclare a queue before
                    // every use, so announce only the FIRST time this shard sees
                    // a name (or the first after a tombstone). LWW seq still
                    // orders the genuine announce and a post-delete resurrection.
                    char[1] fb = [cast(char)(qflags & 0x0E)];
                    ctlBroadcast(8, qq, fb[], "");
                    if (qflags & 4) // exclusive: claim it for THIS connection
                    {
                        char[24] idb = void;
                        size_t idl = 0;
                        ulong v = c.connId;
                        char[24] tmp = void;
                        size_t tn = 0;
                        do
                        {
                            tmp[tn++] = cast(char)('0' + v % 10);
                            v /= 10;
                        }
                        while (v);
                        while (tn)
                            idb[idl++] = tmp[--tn];
                        ctlBroadcast(10, qq, idb[0 .. idl], "");
                        try
                            c.exclQueues ~= qq.idup;
                        catch (Exception)
                        {
                        }
                    }
                }
                try
                    if (auto lch = chan in c.chans)
                        lch.lastQueue = qq.idup; // the channel's "current queue"
                catch (Exception)
                {
                }
                // ANY declare (active or passive) extends an x-expires lease
                try
                    if (auto qm0 = (cast(string) qq) in gQueueMeta)
                        if (qm0.expSet)
                            ctlBroadcast(11, qq, "", "");
                catch (Exception)
                {
                }
                immutable cnt = queueDepth(qq); // sums every priority level
                immutable ccnt = (cast(string) qq in gQueueConsumers)
                    ? gQueueConsumers[cast(string) qq] : 0u;
                if (!(qflags & 16)) // nowait: no declare-ok
                    method(o, chan, 50, 11, (ref ByteBuffer b) @nogc nothrow {
                        putShortStr(b, qq);
                        putU32(b, cast(uint)(cnt < 0 ? 0 : cnt));
                        putU32(b, ccnt); // live consumers on this queue (shard-local)
                    });
                return true;
            }
        case 20: // bind
            {
                cast(void) r.u16();
                auto q = r.shortStr();
                auto ex = r.shortStr();
                auto rk = r.shortStr();
                immutable bnw = r.u8() & 1; // no-wait
                auto bindArgs = r.tableRaw();
                // spec "current queue" defaults: an empty queue field names the
                // last queue DECLARED on this channel; an empty routing key,
                // when the queue field was ALSO empty, is that queue's name
                immutable qWasEmpty = q.length == 0;
                if (qWasEmpty)
                    if (auto bch = chan in c.chans)
                        q = bch.lastQueue;
                if (rk.length == 0 && qWasEmpty)
                    rk = q;
                // [bug 22101] publish and declare are the ONLY operations
                // permitted on the default exchange: bind -> channel 403
                if (ex.length == 0)
                {
                    channelClose(o, chan, 403,
                            "ACCESS_REFUSED - operation not permitted on the default exchange",
                            50, 20);
                    c.chans.remove(chan);
                    return true;
                }
                // Binding an unknown queue — or a named unknown exchange — is a
                // channel 404 NOT_FOUND, like RabbitMQ: bindings must reference
                // objects that exist. An empty queue name keeps the historical
                // pass-through (dreads does not track last-declared-per-channel).
                if (q.length && !queueExists(q))
                {
                    channelClose(o, chan, 404, "NOT_FOUND - no queue", 50, 20);
                    c.chans.remove(chan);
                    return true;
                }
                if (ex.length && (cast(string) ex !in gExchanges))
                {
                    channelClose(o, chan, 404, "NOT_FOUND - no exchange", 50, 20);
                    c.chans.remove(chan);
                    return true;
                }
                if (exclusiveDenied(c, chan, o, q, 50, 20))
                    return true;
                {
                    import dreads.acl : aclUserCount, aclCanAccessKey, AclUser;

                    if (q.length && aclUserCount() > 1) // per-op ACL on the queue
                    {
                        auto au = cast(const(AclUser)*) c.aclAuth;
                        if (au !is null && !aclCanAccessKey(au, q, false, true))
                        {
                            channelClose(o, chan, 403, "ACCESS_REFUSED - access to queue refused", 50, 20);
                            c.chans.remove(chan);
                            return true;
                        }
                    }
                }
                // headers exchange: a present x-match must be one of the four
                // valid strings — anything else is a bind-time 406 (RabbitMQ)
                try
                    if (auto pxt = (cast(string) ex) in gExchanges)
                        if (*pxt == ExType.headers && bindArgs !is null)
                        {
                            immutable xmk = tableStrKind(bindArgs, "x-match");
                            auto xmv = tableGetStr(bindArgs, "x-match");
                            if (xmk < 0 || (xmk > 0 && xmv != "all" && xmv != "any"
                                    && xmv != "all-with-x" && xmv != "any-with-x"))
                            {
                                channelClose(o, chan, 406,
                                        "PRECONDITION_FAILED - invalid x-match", 50, 20);
                                c.chans.remove(chan);
                                return true;
                            }
                        }
                catch (Exception)
                {
                }
                ctlBroadcast(2, ex, q, rk, bindArgs);
                if (!bnw)
                    method(o, chan, 50, 21); // bind-ok (suppressed by nowait)
                return true;
            }
        case 50: // unbind
            {
                cast(void) r.u16();
                auto q = r.shortStr();
                auto ex = r.shortStr();
                auto rk = r.shortStr();
                cast(void) r.tableRaw(); // arguments (ignored on unbind)
                if (ex.length == 0) // [bug 22101] no unbind on the default exchange
                {
                    channelClose(o, chan, 403,
                            "ACCESS_REFUSED - operation not permitted on the default exchange",
                            50, 50);
                    c.chans.remove(chan);
                    return true;
                }
                if (exclusiveDenied(c, chan, o, q, 50, 50))
                    return true; // another connection's exclusive queue: 405
                {
                    import dreads.acl : aclUserCount, aclCanAccessKey, AclUser;

                    if (q.length && aclUserCount() > 1) // per-op ACL on the queue
                    {
                        auto au = cast(const(AclUser)*) c.aclAuth;
                        if (au !is null && !aclCanAccessKey(au, q, false, true))
                        {
                            channelClose(o, chan, 403, "ACCESS_REFUSED - access to queue refused", 50, 50);
                            c.chans.remove(chan);
                            return true;
                        }
                    }
                }
                ctlBroadcast(4, ex, q, rk); // op 4: drop the matching binding
                try
                    autoDeleteExchangeSweep([cast(string) ex.idup]);
                catch (Exception)
                {
                }
                method(o, chan, 50, 51); // unbind-ok
                return true;
            }
        case 30: // purge: empty the queue, reply purge-ok with the count
            {
                cast(void) r.u16();
                auto q = r.shortStr();
                cast(void) r.u8(); // no-wait
                if (exclusiveDenied(c, chan, o, q, 50, 30))
                    return true; // another connection's exclusive queue: 405
                {
                    import dreads.acl : aclUserCount, aclCanAccessKey, AclUser;

                    if (q.length && aclUserCount() > 1) // per-op ACL on the queue
                    {
                        auto au = cast(const(AclUser)*) c.aclAuth;
                        if (au !is null && !aclCanAccessKey(au, q, false, true))
                        {
                            channelClose(o, chan, 403, "ACCESS_REFUSED - access to queue refused", 50, 30);
                            c.chans.remove(chan);
                            return true;
                        }
                    }
                }
                static ByteBuffer pk; // TLS
                queueKey(q, pk);
                // stack-copy the key across gAmqpLen's yield (same hazard as
                // delete below): a concurrent queueKey would clobber TLS `pk`
                char[8 + 256 + 4] purgeKeyStore = void;
                immutable pklen = pk.length <= purgeKeyStore.length ? pk.length : purgeKeyStore.length;
                purgeKeyStore[0 .. pklen] = cast(const(char)[]) pk.data[0 .. pklen];
                auto purgeKey = cast(const(char)[]) purgeKeyStore[0 .. pklen];
                immutable n = queueDepth(q); // every priority level
                if (gAmqpDelKey !is null)
                {
                    gAmqpDelKey(purgeKey); // DEL empties the list; queue-meta kept
                    if (queueMaxPrio(q) > 0)
                        queueEachLevel(q, (scope const(char)[] key) nothrow {
                            gAmqpDelKey(key); // ... and the upper levels with it
                        });
                }
                method(o, chan, 50, 31, (ref ByteBuffer b) @nogc nothrow {
                    putU32(b, cast(uint)(n < 0 ? 0 : n)); // purged message_count
                });
                return true;
            }
        case 40: // delete
            {
                cast(void) r.u16();
                auto q = r.shortStr();
                immutable qdel = r.u8(); // if-unused(1)/if-empty(2)/no-wait(4)
                if (exclusiveDenied(c, chan, o, q, 50, 40))
                    return true;
                {
                    import dreads.acl : aclUserCount, aclCanAccessKey, AclUser;

                    if (q.length && aclUserCount() > 1) // per-op ACL on the queue
                    {
                        auto au = cast(const(AclUser)*) c.aclAuth;
                        if (au !is null && !aclCanAccessKey(au, q, false, true))
                        {
                            channelClose(o, chan, 403, "ACCESS_REFUSED - access to queue refused", 50, 40);
                            c.chans.remove(chan);
                            return true;
                        }
                    }
                }
                static ByteBuffer dk; // TLS
                queueKey(q, dk);
                // stack-copy: gAmqpLen's cross-shard hop YIELDS, and a
                // concurrent queueKey would clobber the TLS `dk` used by the
                // gAmqpDelKey below -> DELETING THE WRONG QUEUE's list.
                char[8 + 256 + 4] delKeyStore = void;
                immutable dklen = dk.length <= delKeyStore.length ? dk.length : delKeyStore.length;
                delKeyStore[0 .. dklen] = cast(const(char)[]) dk.data[0 .. dklen];
                auto delKey = cast(const(char)[]) delKeyStore[0 .. dklen];
                immutable n = gAmqpLen !is null ? gAmqpLen(delKey) : 0;
                if (gAmqpDelKey !is null)
                    gAmqpDelKey(delKey); // DEL the backing list
                if (queueExists(q)) // dedupe: only stream a delete for a known queue
                {
                    auto adSeeds = bindingSourcesTo(q, false);
                    ctlBroadcast(9, q, "", ""); // tombstone in the existence set
                    autoDeleteExchangeSweep(adSeeds); // sources that lost this queue's bindings
                }
                if (!(qdel & 4))
                    method(o, chan, 50, 41, (ref ByteBuffer b) @nogc nothrow {
                        putU32(b, cast(uint)(n < 0 ? 0 : n)); // message_count
                    });
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
                immutable pubBits = r.u8(); // mandatory bit 0, immediate bit 1
                try
                    if (auto pfx = (cast(string) ex) in gExchFlags)
                        if (*pfx & 8) // internal: only e2e routing may reach it
                        {
                            channelClose(o, chan, 403,
                                    "ACCESS_REFUSED - cannot publish to internal exchange", 60, 40);
                            c.chans.remove(chan);
                            return true;
                        }
                catch (Exception)
                {
                }
                if (pubBits & 2)
                {
                    // immediate=true was REMOVED in RabbitMQ 3.0: a hard
                    // connection-level 540 NOT_IMPLEMENTED, exactly like rabbit
                    connectionClose(o, 540, "NOT_IMPLEMENTED - immediate=true", 60, 40);
                    return true;
                }
                try
                {
                    ch.pub.active = true;
                    ch.pub.mandatory = (pubBits & 1) != 0;
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
                immutable getNoAck = r.ok && r.i < p.length ? (p[r.i] & 1) != 0 : false;
                if (exclusiveDenied(c, chan, o, q, 60, 70))
                    return true; // another connection's exclusive queue: 405
                {
                    import dreads.acl : aclUserCount, aclCanAccessKey, AclUser;

                    if (aclUserCount() > 1) // per-op ACL: authorize READ on the queue
                    {
                        auto au = cast(const(AclUser)*) c.aclAuth;
                        if (au !is null && !aclCanAccessKey(au, q, true, false))
                        {
                            channelClose(o, chan, 403, "ACCESS_REFUSED - read access to queue refused", 60, 70);
                            c.chans.remove(chan);
                            return true;
                        }
                    }
                }
                // a basic.get counts as "use": extend an x-expires lease
                try
                    if (auto qm0 = (cast(string) q) in gQueueMeta)
                        if (qm0.expSet)
                            ctlBroadcast(11, q, "", "");
                catch (Exception)
                {
                }
                // a no-ack=false get also consumes prefetch: don't let millions
                // of un-acked gets pin RAM (the consumer path already caps this)
                // basic.qos does NOT govern basic.get (RabbitMQ: prefetch is a
                // consumer contract — the java suite gets WITH the consumer
                // window deliberately full). Only the RAM backstop applies.
                bool getFull = false;
                try
                    getFull = !getNoAck && (c.unacked.length >= AMQP_DEFAULT_PREFETCH
                            || c.unackedBytes >= AMQP_MAX_UNACKED_BYTES);
                catch (Exception)
                {
                }
                // live consumers first: RabbitMQ dispatches a published message
                // to consumers before processing the channel's next RPC, so a
                // publish-then-get with an attached consumer deterministically
                // gets get-empty. Our consumers POLL (1ms backoff) — without
                // this yield the get races them and steals the delivery.
                try
                    if ((cast(string) q) in gQueueConsumers)
                        sleep(2.msecs);
                catch (Exception)
                {
                }
                static ByteBuffer kb2; // TLS
                // x-max-priority: read the highest non-empty level (level 0 IS
                // the queue's key, so a plain FIFO queue is unaffected)
                cast(void) queueKeyRead(q, kb2);
                // stack-copy the key: the expired-head drain below calls
                // deadLetter, whose cross-shard DLX push YIELDS, and a
                // concurrent fiber's queueKey would clobber the TLS `kb2` used
                // by the next pop -> popping the WRONG queue (same hazard the
                // active-TTL reaper had).
                char[8 + 256 + 4] getKeyStore = void;
                immutable gklen = kb2.length <= getKeyStore.length ? kb2.length : getKeyStore.length;
                getKeyStore[0 .. gklen] = cast(const(char)[]) kb2.data[0 .. gklen];
                auto getKey = cast(const(char)[]) getKeyStore[0 .. gklen];
                static ByteBuffer pay; // TLS
                pay.clear();
                // drain expired heads (dead-letter or drop) before delivering
                immutable getTtl = queueTtl(q);
                bool getHit = false;
                if (!getFull && gAmqpPop !is null)
                {
                    int drained = 0;
                    // bound the expired-head drain so a queue full of expired
                    // messages can't stall the serve fiber in one get
                    while (drained < 4096 && gAmqpPop(getKey, pay))
                    {
                        if (isExpired(pay.data, getTtl))
                        {
                            deadLetter(q, pay.data, "expired");
                            pay.clear();
                            drained++;
                            continue;
                        }
                        getHit = true;
                        break;
                    }
                }
                if (getHit)
                {
                    // Copy the record out of the shared TLS `pay` BEFORE the
                    // gAmqpLen hop below parks: during that park a sibling
                    // basic.get on this thread refills `pay`, and we then
                    // idup/slice/emit it — cross-client disclosure + a wrong
                    // unacked record. recCopy is per-call (stack).
                    ByteBuffer recCopy;
                    recCopy.append(cast(const(char)[]) pay.data);
                    immutable remaining = gAmqpLen !is null ? gAmqpLen(getKey) : 0;
                    immutable gtag = c.nextTag++;
                    try
                        if (auto gch = chan in c.chans)
                            gch.lastTag = gtag; // multiple-settle window bound
                    catch (Exception)
                    {
                    }
                    // no-ack=false: record for later ack/requeue; the old code
                    // hardcoded tag 1 and never recorded it, so a get+ack
                    // workflow could neither ack nor requeue (message lost)
                    if (!getNoAck)
                        try
                        {
                            c.unacked[gtag] = Unacked(q.idup, dupBlob(recCopy.data), chan, 0, true);
                            c.unackedBytes += recCopy.data.length;
                            // NOT counted in the channel's consumer window:
                            // qos is a consumer contract (fromGet above).
                        }
                        catch (Exception)
                        {
                        }
                    immutable redlv = recordRedelivered(recCopy.data);
                    auto grk = recordRoutingKey(recCopy.data);
                    auto gex = recordExchange(recCopy.data);
                    method(o, chan, 60, 71, (ref ByteBuffer b) @nogc nothrow {
                        putU64(b, gtag);
                        b.appendByte(redlv ? 1 : 0); // redelivered
                        putShortStr(b, gex); // original exchange
                        putShortStr(b, grk); // original routing key
                        putU32(b, cast(uint)(remaining < 0 ? 0 : remaining));
                    });
                    emitContent(o, chan, recCopy.data, c.frameMax);
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
                if (ch is null)
                    return true; // consume on an unopened channel: ignore
                cast(void) r.u16();
                auto q = r.shortStr();
                {
                    import dreads.acl : aclUserCount, aclCanAccessKey, AclUser;

                    // per-op ACL: authorize READ on the queue ONCE at consume setup
                    // (never per delivery). The direct reply-to pseudo-queue is not
                    // a real queue and is exempt.
                    if (q != DIRECT_REPLY_Q && aclUserCount() > 1)
                    {
                        auto au = cast(const(AclUser)*) c.aclAuth;
                        if (au !is null && !aclCanAccessKey(au, q, true, false))
                        {
                            channelClose(o, chan, 403, "ACCESS_REFUSED - read access to queue refused", 60, 20);
                            c.chans.remove(chan);
                            return true;
                        }
                    }
                }
                if (q == DIRECT_REPLY_Q)
                {
                    // direct reply-to pseudo-queue: no-ack ONLY, at most one
                    // consumer per channel; no fiber — replies are delivered
                    // straight to this channel by the token publish path
                    auto dtag = r.shortStr();
                    immutable dbits2 = r.u8();
                    if (!(dbits2 & 2)) // no-ack REQUIRED
                    {
                        channelClose(o, chan, 406,
                                "PRECONDITION_FAILED - reply consumer cannot acknowledge", 60, 20);
                        c.chans.remove(chan);
                        return true;
                    }
                    if (ch.drConsumer)
                    {
                        channelClose(o, chan, 406,
                                "PRECONDITION_FAILED - reply consumer already set", 60, 20);
                        c.chans.remove(chan);
                        return true;
                    }
                    char[64] drtb = void;
                    const(char)[] drt;
                    if (dtag.length)
                    {
                        auto dn = dtag.length <= drtb.length ? dtag.length : drtb.length;
                        drtb[0 .. dn] = dtag[0 .. dn];
                        drt = drtb[0 .. dn];
                    }
                    else
                    {
                        import core.stdc.stdio : snprintf;

                        immutable dn = snprintf(drtb.ptr, drtb.length,
                                "amq.ctag-%llu", cast(ulong) c.nextCtag++);
                        drt = drtb[0 .. dn];
                    }
                    try
                    {
                        ch.drConsumer = true;
                        ch.drCtag = drt.idup;
                    }
                    catch (Exception)
                    {
                    }
                    if (!(dbits2 & 8)) // no-wait
                        method(o, chan, 60, 21, (ref ByteBuffer b) @nogc nothrow {
                            putShortStr(b, drt);
                        });
                    return true;
                }
                // Consuming from a queue that was never declared is a channel
                // 404 NOT_FOUND, like RabbitMQ. The connection survives.
                if (q.length && !queueExists(q))
                {
                    channelClose(o, chan, 404, "NOT_FOUND - no queue", 60, 20);
                    c.chans.remove(chan);
                    return true;
                }
                if (exclusiveDenied(c, chan, o, q, 60, 20))
                    return true;
                auto tag = r.shortStr();
                immutable bits = r.u8();
                immutable noAck = (bits & 2) != 0;
                immutable subNoWait = (bits & 8) != 0;
                int consumerPrio = 0;
                {
                    // consume-arguments: a present x-priority must be an
                    // INTEGER (ConsumerPriorities.validation pins the 406)
                    auto cargs = r.tableRaw();
                    if (cargs !is null && cargs.length)
                    {
                        long xprio;
                        immutable xpk = tableIntKind(cargs, "x-priority", xprio);
                        if (xpk < 0)
                        {
                            channelClose(o, chan, 406,
                                    "PRECONDITION_FAILED - invalid x-priority", 60, 20);
                            c.chans.remove(chan);
                            return true;
                        }
                        if (xpk > 0)
                            consumerPrio = cast(int) xprio;
                    }
                }
                // STACK buffer, not TLS: the cancel-race wait below YIELDS, and
                // a concurrent consume on this thread would clobber a shared
                // static (the delKeyStore hazard all over this file).
                char[128] tagbuf = void;
                const(char)[] tg;
                if (tag.length == 0)
                {
                    // server-assigned tag MUST be unique per connection — the
                    // old shared literal "ctag-1" made basic.cancel stop every
                    // default-tagged consumer at once
                    import core.stdc.stdio : snprintf;

                    immutable n = snprintf(tagbuf.ptr, tagbuf.length,
                            "ctag-%llu", cast(ulong) c.nextCtag++);
                    tg = cast(const(char)[]) tagbuf[0 .. n];
                }
                else
                {
                    auto tn = tag.length <= tagbuf.length ? tag.length : tagbuf.length;
                    tagbuf[0 .. tn] = tag[0 .. tn];
                    tg = cast(const(char)[]) tagbuf[0 .. tn];
                }
                if (c.consumerCount >= AMQP_MAX_CONSUMERS)
                    return false; // consumer flood: close the connection
                // cancel(nowait) + re-consume with the SAME tag races the old
                // fiber: until it exits, the tag sits in cancelledTags and a
                // fresh fiber would see its own tag marked and die instantly.
                // Wait (bounded) for the old consumer to clear its mark.
                try
                {
                    int spins = 0;
                    while ((tg in c.cancelledTags) !is null && spins++ < 200)
                        sleep(1.msecs);
                }
                catch (Exception)
                {
                }
                if (!subNoWait) // nowait: the client forbade consume-ok
                    method(o, chan, 60, 21, (ref ByteBuffer b) @nogc nothrow {
                        putShortStr(b, tg);
                    });
                startConsumer(c, chan, q, tg, noAck, consumerPrio);
                return true;
            }
        case 80: // ack: delivery-tag u64, multiple bit
            {
                immutable tag = r.u64();
                immutable multiple = r.ok && r.i < p.length ? (p[r.i] & 1) != 0 : false;
                // ONE lookup for the whole settle: the check and the drop both
                // take the slot instead of finding it again.
                auto ackSlot = multiple ? null : (tag in c.unacked);
                if (settleTagUnknown(c, chan, tag, multiple, ackSlot))
                {
                    channelClose(o, chan, 406,
                            "PRECONDITION_FAILED - unknown delivery tag", 60, 80);
                    c.chans.remove(chan);
                    return true;
                }
                if (auto tch = chan in c.chans)
                    if (tch.txMode)
                    {
                        if (tch.txSettles.length < AMQP_MAX_TX)
                            try
                                tch.txSettles ~= TxSettle(tag, 0, multiple, false);
                            catch (Exception)
                            {
                            }
                        return true; // applied on tx.commit
                    }
                try
                {
                    if (multiple)
                    {
                        // [basic.ack] delivery tags are channel-specific: a
                        // multiple-ack on THIS channel must ack only THIS
                        // channel's deliveries <= tag. Our tags are per-conn
                        // (monotonic across channels), so without the u.chn filter
                        // a multiple-ack would over-ack sibling channels' lower
                        // tags -> their messages silently dropped from unacked.
                        // [basic.ack] delivery-tag 0 + multiple=1 acks ALL
                        // outstanding (on this channel); otherwise up-to-and-
                        // including tag. tags start at 1, so tag==0 must not be
                        // compared with t<=tag (that matches nothing).
                        ulong[] drop;
                        foreach (t, ref u; c.unacked)
                            if ((tag == 0 || t <= tag) && u.chan == chan)
                                drop ~= t;
                        foreach (t; drop)
                            dropUnacked(c, t);
                    }
                    else
                        dropUnacked(c, tag, ackSlot);
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
                if (settleTagUnknown(c, chan, tag, false))
                {
                    channelClose(o, chan, 406,
                            "PRECONDITION_FAILED - unknown delivery tag", 60, 90);
                    c.chans.remove(chan);
                    return true;
                }
                if (auto tch = chan in c.chans)
                    if (tch.txMode)
                    {
                        if (tch.txSettles.length < AMQP_MAX_TX)
                            try
                                tch.txSettles ~= TxSettle(tag, 2, false, requeue);
                            catch (Exception)
                            {
                            }
                        return true;
                    }
                settleNegative(c, tag, requeue);
                return true;
            }
        case 120: // nack: delivery-tag u64, multiple, requeue
            {
                immutable tag = r.u64();
                immutable bits2 = r.ok && r.i < p.length ? p[r.i] : 0;
                immutable multiple = (bits2 & 1) != 0;
                immutable requeue = (bits2 & 2) != 0;
                if (settleTagUnknown(c, chan, tag, multiple))
                {
                    channelClose(o, chan, 406,
                            "PRECONDITION_FAILED - unknown delivery tag", 60, 120);
                    c.chans.remove(chan);
                    return true;
                }
                if (auto tch = chan in c.chans)
                    if (tch.txMode)
                    {
                        if (tch.txSettles.length < AMQP_MAX_TX)
                            try
                                tch.txSettles ~= TxSettle(tag, 1, multiple, requeue);
                            catch (Exception)
                            {
                            }
                        return true;
                    }
                try
                {
                    if (multiple)
                    {
                        // channel-scoped like basic.ack: nack-multiple settles
                        // only THIS channel's deliveries <= tag (tags are per-conn
                        // so the u.chn filter stops cross-channel over-nack)
                        ulong[] all;
                        foreach (t, ref u; c.unacked)
                            if ((tag == 0 || t <= tag) && u.chan == chan)
                                all ~= t; // tag 0 + multiple = ALL outstanding
                        // preserve delivery order on redelivery: requeue pushes
                        // to the FRONT, so settle highest-tag-first (lowest ends
                        // up frontmost = FIFO); dead-letter RPUSHes the DLX tail,
                        // so settle lowest-first. Sort explicitly rather than
                        // leaning on the store's iteration order.
                        import std.algorithm.sorting : sort;

                        if (requeue)
                            sort!"a > b"(all);
                        else
                            sort!"a < b"(all);
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
        case 100: // recover-async (deprecated): requeue unacked, NO reply
        case 110: // recover: requeue this channel's unacked, reply recover-ok
            {
                immutable recRequeue = (r.u8() & 1) != 0;
                // recover with requeue=false ("redeliver to the ORIGINAL
                // consumer") was never implemented by RabbitMQ: hard 540
                if (mth == 110 && !recRequeue)
                {
                    connectionClose(o, 540, "NOT_IMPLEMENTED - requeue=false", 60, 110);
                    return true;
                }
                try
                {
                    // requeue every unacked delivery on THIS channel, FIFO-
                    // preserving (descending tag -> pushFront leaves ascending at
                    // the head), each marked redelivered by settleNegative. A
                    // client stuck without recover-ok used to hang forever.
                    import std.algorithm.sorting : sort;

                    // [bug 21845] a tag with a PENDING (uncommitted) tx ack is
                    // NOT redelivered by recover — the ack stands unless the tx
                    // rolls back.
                    bool txAcked(ulong t) nothrow
                    {
                        if (auto rch = chan in c.chans)
                            if (rch.txMode)
                                foreach (ref tsx; rch.txSettles)
                                    if (tsx.kind == 0 && (tsx.multiple
                                            ? (tsx.tag == 0 || t <= tsx.tag) : tsx.tag == t))
                                        return true;
                        return false;
                    }

                    ulong[] all;
                    foreach (t, ref u; c.unacked)
                        if (u.chan == chan && !txAcked(t))
                            all ~= t;
                    sort!"a > b"(all);
                    foreach (t; all)
                        settleNegative(c, t, true);
                }
                catch (Exception)
                {
                }
                if (mth == 110)
                    method(o, chan, 60, 111); // recover-ok
                return true;
            }
        case 10: // qos: prefetch-size u32, prefetch-count u16, global bit
            {
                immutable psize = r.u32();
                // The byte-window prefetch (prefetch-size != 0) is not
                // implemented; RabbitMQ answers with a HARD error — a
                // connection-level 540 NOT_IMPLEMENTED that kills every channel.
                if (psize != 0)
                {
                    connectionClose(o, 540, "NOT_IMPLEMENTED - prefetch_size!=0", 60, 10);
                    return true; // stay open for the client's close-ok
                }
                immutable pc = r.u16();
                immutable qosGlobal = r.ok && r.i < p.length ? (p[r.i] & 1) != 0 : false;
                // global=true: the window is CHANNEL-shared; global=false (the
                // default): PER-CONSUMER, applied to consumers started after
                // this qos. The conn-level copy remains as the fallback for
                // channels that never issued qos themselves.
                if (auto qch = chan in c.chans)
                {
                    qch.prefetch = pc;
                    qch.prefetchGlobal = qosGlobal;
                }
                c.prefetch = pc; // 0 = "no specific limit" -> default cap applies
                method(o, chan, 60, 11);
                return true;
            }
        case 30: // cancel — stop the consumer fiber, reply CancelOk
            {
                auto tag = r.shortStr();
                immutable noWait = r.ok && r.i < p.length ? (p[r.i] & 1) != 0 : false;
                // direct-reply consumer: no fiber to wait for — clear + ok
                if (auto drch = chan in c.chans)
                    if (drch.drConsumer && drch.drCtag == tag)
                    {
                        drch.drConsumer = false;
                        drch.drCtag = null;
                        if (!noWait)
                        {
                            char[64] drtb2 = void;
                            auto dn2 = tag.length <= drtb2.length ? tag.length : drtb2.length;
                            drtb2[0 .. dn2] = tag[0 .. dn2];
                            auto drt2 = cast(const(char)[]) drtb2[0 .. dn2];
                            method(o, chan, 60, 31, (ref ByteBuffer b) @nogc nothrow {
                                putShortStr(b, drt2);
                            });
                        }
                        return true;
                    }
                // cap the map: basic.cancel needs neither an open channel nor a
                // completed handshake, so a flood of unique bogus tags would
                // otherwise grow this per-conn AA without bound (RAM DoS). Legit
                // pending cancels never exceed the live-consumer count (both
                // capped at AMQP_MAX_CONSUMERS) and each is removed on the
                // consumer's exit, so a healthy connection never hits the cap.
                // stack copy FIRST: the wait below yields, and `tag` slices the
                // conn read buffer another fiber could refill during the park
                char[128] tb = void;
                auto tn = tag.length <= tb.length ? tag.length : tb.length;
                tb[0 .. tn] = tag[0 .. tn];
                auto tg2 = cast(const(char)[]) tb[0 .. tn];
                try
                    if (c.cancelledTags.length < AMQP_MAX_CONSUMERS)
                        c.cancelledTags[tag.idup] = true;
                catch (Exception)
                {
                }
                // HOLD the cancel-ok until the consumer fiber exits (it removes
                // its marker on exit): a burst staged during the cancel would
                // otherwise hit the wire AFTER the cancel-ok — the java client
                // treats a post-cancel-ok delivery as "Unsolicited delivery"
                // and kills the whole connection. Bounded: an unknown tag has
                // no fiber and just costs the full spin.
                try
                {
                    int spins = 0;
                    while (((cast(string) tg2) in c.cancelledTags) !is null && spins++ < 200)
                        sleep(1.msecs);
                }
                catch (Exception)
                {
                }
                if (!noWait)
                    method(o, chan, 60, 31, (ref ByteBuffer b) @nogc nothrow {
                        putShortStr(b, tg2);
                    });
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
                if (ch.txMode)
                    return txConfirmConflict(c, chan, o);
                ch.confirmMode = true;
                ch.confirmSeq = 1;
            }
            method(o, chan, 85, 11);
            return true;
        }
        return true;
    case 90: // tx (transactions)
        {
            auto ch = chan in c.chans;
            if (ch is null)
                return true;
            if (mth == 10) // select
            {
                if (ch.confirmMode)
                    return txConfirmConflict(c, chan, o);
                ch.txMode = true;
                method(o, chan, 90, 11); // select-ok
            }
            else if (mth == 20) // commit: apply buffered pubs + settles atomically
            {
                if (!ch.txMode)
                {
                    // commit without tx.select: channel 406 (RabbitMQ)
                    channelClose(o, chan, 406,
                            "PRECONDITION_FAILED - channel is not transactional", 90, 20);
                    c.chans.remove(chan);
                    return true;
                }
                commitTx(c, chan, *ch, o);
                method(o, chan, 90, 21); // commit-ok
            }
            else if (mth == 30) // rollback: drop the buffers (acks stay un-applied
            {                    //  -> messages remain unacked / redeliverable)
                if (!ch.txMode)
                {
                    channelClose(o, chan, 406,
                            "PRECONDITION_FAILED - channel is not transactional", 90, 30);
                    c.chans.remove(chan);
                    return true;
                }
                ch.txPubs = null;
                ch.txSettles = null;
                ch.txBytes = 0;
                method(o, chan, 90, 31); // rollback-ok
            }
            return true;
        }
    default:
        return true;
    }
}

/// confirm.select and tx.select are mutually exclusive (a tx buffers publishes
/// and sends no confirm, so a confirm client would hang). Reject the second with
/// a channel.close(406) and kill the channel — RabbitMQ's PRECONDITION_FAILED.
private bool txConfirmConflict(AmqpConn c, ushort chan, ref ByteBuffer o) nothrow @trusted
{
    method(o, chan, 20, 40, (ref ByteBuffer b) @nogc nothrow {
        putU16(b, 406); // PRECONDITION_FAILED
        putShortStr(b, "confirm and tx are mutually exclusive");
        putU16(b, 0); // class-id
        putU16(b, 0); // method-id
    });
    try
        c.chans.remove(chan); // channel is dead; its (tiny, just-selected) state goes
    catch (Exception)
    {
    }
    return true;
}

/// Apply a channel's buffered transaction (tx.commit): route the buffered
/// publishes then apply the buffered acks/nacks/rejects, then clear the buffers.
private void commitTx(AmqpConn c, ushort chan, ref Channel ch, ref ByteBuffer o) nothrow @trusted
{
    foreach (ref tp; ch.txPubs)
    {
        long pm;
        int d;
        const(char)[] rk;
        const(ubyte)[] props, body_;
        splitRecord(tp.record, pm, d, rk, props, body_);
        // the TTL clock starts at COMMIT, not at the buffered publish: an
        // uncommitted message isn't in the queue yet (transactionalPublishWithGet
        // pins this). Re-stamp publishMs (record bytes 4..12, after the magic).
        auto stamped = cast(ubyte[]) tp.record.dup;
        if (stamped.length >= 12 && (stamped[0] == 0x04 || stamped[0] == 0x05))
        {
            immutable nowc = cast(ulong) nowMs();
            foreach (k; 0 .. 8)
                stamped[4 + k] = cast(ubyte)(nowc >> ((7 - k) * 8));
        }
        auto payload = (cast(const(ubyte)[]) stamped).asChars;
        int routed = 0;
        scope void delegate(scope const(char)[]) nothrow txSink = (scope const(char)[] q) nothrow {
            if (!queueExists(q))
                return; // same existing-queues-only rule as the live publish path
            static ByteBuffer kbT; // TLS
            queueKey(q, kbT);
            if (gAmqpPush !is null)
                gAmqpPush(kbT.data.asChars, payload);
            routed++;
            enforceMaxLen(q);
        };
        routeTo(tp.exchange, tp.rkey, propsHeaders(props), txSink);
        if (routed == 0) // alternate-exchange cascade, as in finishPublish
        {
            string[8] seenAE;
            size_t nAE = 0;
            seenAE[nAE++] = tp.exchange;
            auto cur = tp.exchange;
            while (routed == 0 && nAE < seenAE.length)
            {
                string ae;
                if (auto pae = cur in gExchangeAE)
                    ae = *pae;
                if (ae.length == 0)
                    break;
                bool revisit = false;
                foreach (sx; seenAE[0 .. nAE])
                    if (sx == ae)
                    {
                        revisit = true;
                        break;
                    }
                if (revisit)
                    break;
                seenAE[nAE++] = ae;
                routeTo(ae, tp.rkey, propsHeaders(props), txSink);
                cur = ae;
            }
        }
        amqpCountPub();
        if (tp.mandatory && routed == 0)
        {
            auto exn = tp.exchange;
            auto rkn = tp.rkey;
            method(o, chan, 60, 50, (ref ByteBuffer b) @nogc nothrow {
                putU16(b, 312);
                putShortStr(b, "NO_ROUTE");
                putShortStr(b, exn);
                putShortStr(b, rkn);
            });
            emitContent(o, chan, tp.record, c.frameMax);
            amqpCountRet();
        }
    }
    foreach (ref ts; ch.txSettles)
    {
        if (ts.kind == 0) // ack
        {
            try
            {
                if (ts.multiple)
                {
                    ulong[] drop;
                    foreach (t, ref u; c.unacked)
                        if ((ts.tag == 0 || t <= ts.tag) && u.chan == chan)
                            drop ~= t;
                    foreach (t; drop)
                        dropUnacked(c, t);
                }
                else
                    dropUnacked(c, ts.tag);
            }
            catch (Exception)
            {
            }
        }
        else // nack (1) / reject (2)
        {
            if (ts.multiple)
            {
                ulong[] all;
                try
                    foreach (t, ref u; c.unacked)
                        if ((ts.tag == 0 || t <= ts.tag) && u.chan == chan)
                            all ~= t;
                catch (Exception)
                {
                }
                import std.algorithm.sorting : sort;

                if (ts.requeue)
                    sort!"a > b"(all);
                else
                    sort!"a < b"(all);
                foreach (t; all)
                    settleNegative(c, t, ts.requeue);
            }
            else
                settleNegative(c, ts.tag, ts.requeue);
        }
    }
    ch.txPubs = null;
    ch.txSettles = null;
    ch.txBytes = 0;
}

private auto asChars(const(ubyte)[] b) @nogc nothrow
{
    return cast(const(char)[]) b;
}

// Queue record framing: [\x01 'A' 'M' 'Q'][u32 propLen][props][body].
// A record WITHOUT the magic (e.g. LPUSHed from the RESP side — cross-protocol
// ingest is a feature) is treated as a bare body with empty properties.
// Record v2 ("\x02AMQ"): [magic 4][u16 rkLen][rk][u32 propLen][props][body].
// v2 carries the ORIGINAL routing key so dead-lettering can re-route by it
// (RabbitMQ's default), which neither a consumer nor basic.get can otherwise
// recover from the stored bytes. splitRecord still reads legacy v1
// ("\x01AMQ", no rk) and bare (magic-less) records.
private void buildRecord(ref ByteBuffer o, long publishMs, int deaths,
        scope const(char)[] rkey, scope const(ubyte)[] props, scope const(ubyte)[] body_,
        scope const(char)[] exchange = "") @nogc nothrow
{
    // v4 record: bytes 0..12 are byte-identical to v3 (magic, publishMs, deaths
    // with the redelivered flag in bit 7), then the ORIGINAL exchange (u16 len)
    // ahead of the routing key. basic.deliver/basic.get-ok must carry the real
    // exchange the message was published to (recordExchange reads it back).
    // v5 record: v4 plus a DELIVERY COUNT byte at 13 (everything after it shifts
    // by one). x-delivery-limit needs to know how many times a message has been
    // delivered and requeued, and there was nowhere to keep it: the deaths byte
    // is x-death hop count in bits 0..6 with the redelivered FLAG in bit 7, and
    // a flag cannot answer "how many". Readers for v1..v4 stay, so existing
    // records and AOF replay are unaffected; only an OLDER build reading a
    // record written by this one would not see the new field.
    o.append("\x05AMQ");
    putU64(o, cast(ulong) publishMs); // wall-clock ms at publish (0 = unknown)
    o.appendByte(cast(char)(deaths > 255 ? 255 : (deaths < 0 ? 0 : deaths))); // x-death hop count
    o.appendByte(0); // delivery count: a fresh publish has never been delivered
    immutable el = exchange.length > 0xFFFF ? 0xFFFF : exchange.length;
    o.appendByte(cast(char)(el >> 8));
    o.appendByte(cast(char)(el & 0xFF));
    o.append(exchange[0 .. el]);
    immutable rl = rkey.length > 0xFFFF ? 0xFFFF : rkey.length;
    o.appendByte(cast(char)(rl >> 8));
    o.appendByte(cast(char)(rl & 0xFF));
    o.append(rkey[0 .. rl]);
    putU32(o, cast(uint) props.length);
    o.append(props);
    o.append(body_);
}

package void splitRecord(scope const(ubyte)[] blob, out long publishMs,
        out int deaths, out const(char)[] rkey, out const(ubyte)[] props,
        out const(ubyte)[] body_) @nogc nothrow
{
    if (blob.length >= 18 && blob[0] == 0x05 && blob[1] == 'A' && blob[2] == 'M'
            && blob[3] == 'Q')
    {
        long pm = 0;
        foreach (k; 0 .. 8)
            pm = (pm << 8) | blob[4 + k];
        publishMs = pm;
        deaths = blob[12] & 0x7F; // bit 7 is the redelivered flag, not a death
        // blob[13] is the delivery count (recordDeliveryCount), then v4's layout
        immutable el = (cast(size_t) blob[14] << 8) | blob[15];
        immutable ro = 16 + el; // routing-key length offset
        if (ro + 2 <= blob.length)
        {
            immutable rl = (cast(size_t) blob[ro] << 8) | blob[ro + 1];
            immutable po = ro + 2 + rl;
            if (po + 4 <= blob.length)
            {
                rkey = cast(const(char)[]) blob[ro + 2 .. po];
                immutable pl = (cast(size_t) blob[po] << 24) | (cast(size_t) blob[po + 1] << 16)
                    | (cast(size_t) blob[po + 2] << 8) | blob[po + 3];
                if (po + 4 + pl <= blob.length)
                {
                    props = blob[po + 4 .. po + 4 + pl];
                    body_ = blob[po + 4 + pl .. $];
                    return;
                }
            }
        }
    }
    if (blob.length >= 17 && blob[0] == 0x04 && blob[1] == 'A' && blob[2] == 'M'
            && blob[3] == 'Q')
    {
        long pm = 0;
        foreach (k; 0 .. 8)
            pm = (pm << 8) | blob[4 + k];
        publishMs = pm;
        deaths = blob[12] & 0x7F; // bit 7 is the redelivered flag, not a death
        immutable el = (cast(size_t) blob[13] << 8) | blob[14]; // exchange, skipped here
        immutable ro = 15 + el; // routing-key length offset
        if (ro + 2 <= blob.length)
        {
            immutable rl = (cast(size_t) blob[ro] << 8) | blob[ro + 1];
            immutable po = ro + 2 + rl;
            if (po + 4 <= blob.length)
            {
                rkey = cast(const(char)[]) blob[ro + 2 .. po];
                immutable pl = (cast(size_t) blob[po] << 24) | (cast(size_t) blob[po + 1] << 16)
                    | (cast(size_t) blob[po + 2] << 8) | blob[po + 3];
                if (po + 4 + pl <= blob.length)
                {
                    props = blob[po + 4 .. po + 4 + pl];
                    body_ = blob[po + 4 + pl .. $];
                    return;
                }
            }
        }
    }
    if (blob.length >= 15 && blob[0] == 0x03 && blob[1] == 'A' && blob[2] == 'M'
            && blob[3] == 'Q')
    {
        long pm = 0;
        foreach (k; 0 .. 8)
            pm = (pm << 8) | blob[4 + k];
        publishMs = pm;
        deaths = blob[12] & 0x7F; // bit 7 is the redelivered flag, not a death
        immutable rl = (cast(size_t) blob[13] << 8) | blob[14];
        if (15 + rl + 4 <= blob.length)
        {
            rkey = cast(const(char)[]) blob[15 .. 15 + rl];
            immutable po = 15 + rl;
            immutable pl = (cast(size_t) blob[po] << 24) | (cast(size_t) blob[po + 1] << 16)
                | (cast(size_t) blob[po + 2] << 8) | blob[po + 3];
            if (po + 4 + pl <= blob.length)
            {
                props = blob[po + 4 .. po + 4 + pl];
                body_ = blob[po + 4 + pl .. $];
                return;
            }
        }
    }
    if (blob.length >= 6 && blob[0] == 0x02 && blob[1] == 'A' && blob[2] == 'M'
            && blob[3] == 'Q')
    {
        immutable rl = (cast(size_t) blob[4] << 8) | blob[5];
        if (6 + rl + 4 <= blob.length)
        {
            rkey = cast(const(char)[]) blob[6 .. 6 + rl];
            immutable po = 6 + rl;
            immutable pl = (cast(size_t) blob[po] << 24) | (cast(size_t) blob[po + 1] << 16)
                | (cast(size_t) blob[po + 2] << 8) | blob[po + 3];
            if (po + 4 + pl <= blob.length)
            {
                props = blob[po + 4 .. po + 4 + pl];
                body_ = blob[po + 4 + pl .. $];
                return;
            }
        }
    }
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

/// The redelivered flag ([basic.deliver]/[basic.get-ok]: message was delivered
/// before) lives in bit 7 of the v3 deaths byte — deaths caps at 16 so the high
/// bits are free, and splitRecord masks it off the count. Set on requeue only;
/// a fresh publish and a dead-lettered copy (rebuilt by buildRecord, no bit 7)
/// both read false, matching RabbitMQ (dead-letter starts a fresh delivery).
package bool recordRedelivered(scope const(ubyte)[] blob) @nogc nothrow
{
    return blob.length >= 15 && (blob[0] == 0x03 || blob[0] == 0x04 || blob[0] == 0x05)
        && blob[1] == 'A' && blob[2] == 'M' && blob[3] == 'Q' && (blob[12] & 0x80) != 0;
}

/// How many times this message has been delivered and put back (v5 records
/// only; anything older has no counter and reads 0, so a queue that gains an
/// x-delivery-limit never retro-kills messages published before it).
package uint recordDeliveryCount(scope const(ubyte)[] blob) @nogc nothrow
{
    return blob.length >= 18 && blob[0] == 0x05 && blob[1] == 'A' && blob[2] == 'M'
        && blob[3] == 'Q' ? blob[13] : 0;
}

/// The ORIGINAL exchange a message was published to ([basic.deliver]/
/// [basic.get-ok] carry it). Only the v4 record stores it; earlier records (and
/// bare RESP-side values) predate the field and yield "" — which is also the
/// correct value for a default-exchange publish.
private const(char)[] recordExchange(return scope const(ubyte)[] blob) @nogc nothrow
{
    if (blob.length >= 18 && blob[0] == 0x05 && blob[1] == 'A' && blob[2] == 'M'
            && blob[3] == 'Q')
    {
        immutable el = (cast(size_t) blob[14] << 8) | blob[15];
        if (16 + el <= blob.length)
            return cast(const(char)[]) blob[16 .. 16 + el];
    }
    if (blob.length >= 17 && blob[0] == 0x04 && blob[1] == 'A' && blob[2] == 'M'
            && blob[3] == 'Q')
    {
        immutable el = (cast(size_t) blob[13] << 8) | blob[14];
        if (15 + el <= blob.length)
            return cast(const(char)[]) blob[15 .. 15 + el];
    }
    return "";
}

/// The ORIGINAL routing key a message was published with ([basic.deliver] /
/// [basic.get-ok] must carry it — topic consumers parse it). Stored in the v3
/// record; for a default-exchange publish it already equals the queue name, so
/// this is correct in both cases (a bare RESP-side value yields "").
private const(char)[] recordRoutingKey(return scope const(ubyte)[] blob) @nogc nothrow
{
    long pm;
    int d;
    const(char)[] rk;
    const(ubyte)[] props, body_;
    splitRecord(blob, pm, d, rk, props, body_);
    return rk;
}

/// Copy `blob` into `dst` with the v3 redelivered bit set (for requeue). A
/// non-v3 record (bare RESP-side value) has no flag slot and passes through.
private void markRedelivered(ref ByteBuffer dst, scope const(ubyte)[] blob) @nogc nothrow @trusted
{
    dst.clear();
    dst.append(cast(const(char)[]) blob);
    if (blob.length >= 15 && (blob[0] == 0x03 || blob[0] == 0x04 || blob[0] == 0x05)
            && blob[1] == 'A' && blob[2] == 'M' && blob[3] == 'Q')
    {
        auto d = cast(ubyte[]) dst.data;
        d[12] |= 0x80;
        // v5 also COUNTS the redelivery, saturating: x-delivery-limit needs the
        // number, and a wrap would hand a poison message a fresh budget.
        if (blob[0] == 0x05 && dst.length >= 18 && d[13] < 255)
            d[13]++;
    }
}

/// Requeue a channel's in-flight (unacked) records to their queue fronts, then
/// drop the channel so its consumer fibers observe the gen/existence mismatch
/// and exit. Shared by channel.close and the publish-to-unknown-exchange 404.
private void requeueAndDropChannel(AmqpConn c, ushort chan) nothrow @trusted
{
    try
    {
        // collect first, settle after — settleNegative mutates c.unacked, so it
        // must not run inside the foreach over it. Descending tag + pushFront
        // leaves the queue head in ascending (FIFO) order.
        ulong[] mine;
        foreach (t, ref u; c.unacked)
            if (u.chan == chan)
                mine ~= t;
        import std.algorithm.sorting : sort;

        sort!"a > b"(mine);
        foreach (t; mine)
            settleNegative(c, t, true);
        // an incomplete publish assembly on this channel still counts against the
        // per-connection in-progress cap; release it before dropping the channel.
        // finishPublish already cleared active for completed bodies, so this fires
        // only for a channel dropped mid-assembly (e.g. channel.close).
        if (auto dch = chan in c.chans)
            if (dch.pub.active && dch.pub.payload.length < dch.pub.bodySize)
            {
                if (c.pendingBytes >= dch.pub.payload.length)
                    c.pendingBytes -= dch.pub.payload.length;
                else
                    c.pendingBytes = 0;
            }
        c.chans.remove(chan);
    }
    catch (Exception)
    {
    }
}

/// x-max-length: after a push, evict heads beyond the bound — dead-lettered
/// (reason "maxlen") when the queue has a DLX, dropped otherwise. Runs inside
/// the publish sink: every buffer here is stack-local because both the LLEN
/// and the pops can hop cross-shard and YIELD (the delKeyStore hazard).
private void enforceMaxLen(scope const(char)[] q) nothrow @trusted
{
    long ml = 0;
    try
        if (auto m = cast(string) q in gQueueMeta) // reinterpret: read-only probe
            ml = m.maxLen;
    catch (Exception)
    {
    }
    if (ml <= 0 || gAmqpLen is null || gAmqpPop is null)
        return;
    ml -= 1; // decode: stored as bound+1 so a 0 bound stays distinct from unset
    static ByteBuffer mkq; // consumed into the stack copy before any yield
    queueKey(q, mkq);
    char[8 + 256 + 4] ks = void;
    immutable kl = mkq.length <= ks.length ? mkq.length : ks.length;
    ks[0 .. kl] = cast(const(char)[]) mkq.data[0 .. kl];
    auto key = cast(const(char)[]) ks[0 .. kl];
    int guard = 0;
    while (guard++ < 1024)
    {
        immutable n = gAmqpLen(key);
        if (n <= ml)
            break; // ml may be 0: a zero bound evicts everything
        ByteBuffer pay; // local: the pop yields
        if (!gAmqpPop(key, pay))
            break;
        deadLetter(q, cast(const(ubyte)[]) pay.data, "maxlen"); // drops if no DLX
    }
}

private void finishPublish(AmqpConn c, ushort chan, ref Channel ch, ref ByteBuffer o) nothrow @trusted
{
    // this assembly is complete: release its bytes from the in-progress cap
    // (accumulated frame-by-frame in the FRAME_BODY path). active goes false
    // just below, so a later requeueAndDropChannel won't double-decrement.
    if (c.pendingBytes >= ch.pub.payload.length)
        c.pendingBytes -= ch.pub.payload.length;
    else
        c.pendingBytes = 0;
    ch.pub.active = false;
    // basic.publish to a non-existent exchange is a channel 404 NOT_FOUND, like
    // RabbitMQ ("no exchange 'x'"). The default exchange ("") and the seeded
    // amq.* always exist; a named exchange must have been declared. Validate
    // before routing; requeueAndDropChannel removes chans[chan], so `ch` must
    // not be touched afterwards (the callers return immediately — safe).
    if (ch.pub.exchange.length && (cast(string) ch.pub.exchange !in gExchanges))
    {
        channelClose(o, chan, 404, "NOT_FOUND - no exchange", 60, 40);
        requeueAndDropChannel(c, chan);
        return;
    }
    // expiration property must be a decimal-ms string: anything else is a 406
    // PRECONDITION_FAILED at publish (RabbitMQ), never stored.
    if (propsExpiration(ch.pub.props.data) == -2)
    {
        channelClose(o, chan, 406, "PRECONDITION_FAILED - invalid expiration", 60, 40);
        requeueAndDropChannel(c, chan);
        return;
    }
    // Per-operation ACL (no-op unless an ACL is configured — fast global load +
    // predicted branch, identical in shape to the Kafka skin's gate). Authorize
    // WRITE on the exchange being published to; for the default exchange ("")
    // authorize the routing key (= destination queue). Checked ONCE here on the
    // channel-resident operands, never per destination queue inside routeTo.
    {
        import dreads.acl : aclUserCount, aclCanAccessKey, AclUser;

        if (aclUserCount() > 1)
        {
            auto au = cast(const(AclUser)*) c.aclAuth;
            auto target = ch.pub.exchange.length ? ch.pub.exchange : ch.pub.rkey;
            if (au !is null && !aclCanAccessKey(au, target, false, true))
            {
                channelClose(o, chan, 403, "ACCESS_REFUSED - write access refused", 60, 40);
                requeueAndDropChannel(c, chan);
                return;
            }
        }
    }
    immutable mandatory = ch.pub.mandatory;
    // route to queues and RPUSH the framed record through the data plane. rec
    // is normally a reused TLS static, but the sink's cross-shard RPUSH YIELDS
    // and `payload` slices rec — a concurrent publish on this thread would
    // clobber it. Reentrant callers take a fresh local so the parked fan-out
    // keeps writing its own bytes.
    static ByteBuffer recStatic; // TLS
    static bool recBusy;
    ByteBuffer recLocal;
    ByteBuffer* rec = &recLocal;
    if (!recBusy)
    {
        recBusy = true;
        rec = &recStatic;
    }
    scope (exit)
        if (rec is &recStatic)
            recBusy = false;
    // --- CC/BCC sender-selected routing: extra routing keys from the "CC"/
    // "BCC" header string-arrays; BCC is stripped from the delivered props.
    const(ubyte)[] effProps = ch.pub.props.data;
    // per-call (NOT TLS): sp/drp hold the BCC-stripped / reply-to-rewritten
    // property block that `effProps` slices. buildRecord reads effProps AFTER the
    // direct-reply sendTo() yield below, so a TLS static here would be clobbered
    // by a concurrent same-shard publish — another client's props spliced in.
    ByteBuffer sp, drp;
    const(char)[][32] ccKeys;
    size_t nCc = 0;
    {
        auto h0 = propsHeaders(effProps);
        bool hasBcc = false;
        if (h0 !is null && h0.length)
        {
            static immutable string[2] ccNames = ["CC", "BCC"];
            bool badCc = false;
            foreach (cn; ccNames)
            {
                cast(void) tableWalk(h0, (scope const(char)[] k, char ty,
                        scope const(ubyte)[] v) @nogc nothrow {
                    if (k != cn)
                        return true;
                    if (cn == "BCC")
                        hasBcc = true; // stripped regardless of value type
                    if (ty != 'A')
                    {
                        badCc = true; // CC/BCC MUST be an array: channel 406
                        return false;
                    }
                    size_t i2 = 0;
                    while (i2 + 5 <= v.length && nCc < ccKeys.length)
                    {
                        immutable et = v[i2];
                        immutable ln = (cast(size_t) v[i2 + 1] << 24)
                            | (cast(size_t) v[i2 + 2] << 16)
                            | (cast(size_t) v[i2 + 3] << 8) | v[i2 + 4];
                        i2 += 5;
                        if (et != 'S' || i2 + ln > v.length)
                            break; // only longstr members route
                        if (ln > 0)
                            ccKeys[nCc++] = cast(const(char)[]) v[i2 .. i2 + ln];
                        i2 += ln;
                    }
                    return false;
                });
            }
            if (badCc)
            {
                channelClose(o, chan, 406,
                        "PRECONDITION_FAILED - CC/BCC header must be an array", 60, 40);
                try
                    c.chans.remove(chan);
                catch (Exception)
                {
                }
                return;
            }
            if (hasBcc)
            {
                // splice the headers table minus BCC back into the props.
                // STACK buffer (not TLS): consumed by buildRecord below, but a
                // clean lifetime regardless of later yields.
                immutable off = cast(size_t)(h0.ptr - effProps.ptr);
                sp.clear(); // hoisted per-call buffer (declared at function scope)
                sp.append(cast(const(char)[]) effProps[0 .. off - 4]);
                immutable lenAt = sp.length;
                putU32(sp, 0);
                immutable tblStart2 = sp.length;
                appendHeadersExcept(sp, h0, "BCC");
                patchU32(sp, lenAt, cast(uint)(sp.length - tblStart2));
                sp.append(cast(const(char)[]) effProps[off + h0.length .. $]);
                effProps = cast(const(ubyte)[]) sp.data;
            }
        }
    }
    // CC/BCC add extra routing destinations. On the DEFAULT exchange each extra
    // key IS a destination queue, so it must pass the same per-op WRITE ACL as
    // the primary routing key — else a restricted user reaches queues it cannot
    // name directly. (Named exchanges authorize on the exchange itself, checked
    // above; CC/BCC route through that same exchange.)
    if (nCc && ch.pub.exchange.length == 0)
    {
        import dreads.acl : aclUserCount, aclCanAccessKey, AclUser;

        if (aclUserCount() > 1)
        {
            auto au = cast(const(AclUser)*) c.aclAuth;
            if (au !is null)
                foreach (ck; ccKeys[0 .. nCc])
                    if (!aclCanAccessKey(au, ck, false, true))
                    {
                        channelClose(o, chan, 403, "ACCESS_REFUSED - write access refused", 60, 40);
                        requeueAndDropChannel(c, chan);
                        return;
                    }
        }
    }
    // direct reply-to: a request published with reply-to=amq.rabbitmq.reply-to
    // gets the property REWRITTEN to this channel's live reply token; without
    // an active reply consumer it is a channel 406 (RabbitMQ's fast-reply rule)
    if (propsReplyTo(effProps) == DIRECT_REPLY_Q)
    {
        bool drOk = false;
        uint drGen;
        try
            if (auto pch2 = chan in c.chans)
                if (pch2.drConsumer)
                {
                    drOk = true;
                    drGen = pch2.openGen;
                }
        catch (Exception)
        {
        }
        if (!drOk)
        {
            channelClose(o, chan, 406,
                    "PRECONDITION_FAILED - fast reply consumer does not exist", 60, 40);
            try
                c.chans.remove(chan);
            catch (Exception)
            {
            }
            return;
        }
        char[160] drtok = void;
        immutable drtl = drToken(drtok, c.connId, chan, drGen);
        // drp: hoisted per-call buffer (declared at function scope) — effProps is
        // read after the reply sendTo() yield, so it must not be a TLS static.
        replaceReplyTo(drp, effProps, drtok[0 .. drtl]);
        effProps = cast(const(ubyte)[]) drp.data;
    }
    // publish TO a reply token (default exchange): deliver STRAIGHT to the
    // consumer channel it names — same-shard connections only (the tests and
    // the dominant RPC pattern use one connection). Invalid/tampered tokens
    // and gone consumers route nowhere (mandatory returns, otherwise drop).
    bool drDirect = false;
    int drRouted = 0;
    if (ch.pub.exchange.length == 0 && ch.pub.rkey.length > DIRECT_REPLY_Q.length
            && ch.pub.rkey[0 .. DIRECT_REPLY_Q.length] == DIRECT_REPLY_Q)
    {
        drDirect = true;
        ulong dcid;
        uint dch, dgen;
        if (drParse(ch.pub.rkey, dcid, dch, dgen))
        {
            AmqpConn tc;
            try
                if (auto ptc = dcid in gConnsById)
                    tc = *ptc;
            catch (Exception)
            {
            }
            if (tc !is null && !tc.closing)
            {
                try
                    if (auto tch3 = cast(ushort) dch in tc.chans)
                        if (tch3.openGen == dgen && tch3.drConsumer)
                        {
                            ByteBuffer drec;
                            buildRecord(drec, cast(long) nowMs(), 0, ch.pub.rkey,
                                    effProps, ch.pub.payload.data, "");
                            ByteBuffer dout;
                            immutable dtag2 = tc.nextTag++;
                            tch3.lastTag = dtag2;
                            auto drct = tch3.drCtag;
                            method(dout, cast(ushort) dch, 60, 60, (ref ByteBuffer b) @nogc nothrow {
                                putShortStr(b, drct);
                                putU64(b, dtag2);
                                b.appendByte(0); // never redelivered (no-ack)
                                putShortStr(b, ""); // default exchange
                                putShortStr(b, ""); // routing key elided, like rabbit
                            });
                            emitContent(dout, cast(ushort) dch, drec.data, tc.frameMax);
                            sendTo(tc, dout.data);
                            drRouted = 1;
                        }
                catch (Exception)
                {
                }
            }
        }
    }
    rec.clear();
    buildRecord(*rec, cast(long) nowMs(), 0, ch.pub.rkey, effProps,
            ch.pub.payload.data, ch.pub.exchange);
    if (ch.txMode)
    {
        // transaction: buffer the framed publish; it routes on tx.commit and is
        // discarded on tx.rollback. exchange/rkey are already idup'd strings.
        // Bound by BOTH count and bytes (a 128MB body × 100k count = terabytes).
        // Over budget: drop this publish (the tx will be incomplete, but the
        // shard survives) rather than OOM.
        if (ch.txPubs.length < AMQP_MAX_TX && ch.txBytes + rec.data.length <= AMQP_MAX_TX_BYTES)
            try
            {
                ch.txPubs ~= TxPub(ch.pub.exchange, ch.pub.rkey, mandatory, rec.data.idup);
                ch.txBytes += rec.data.length;
            }
            catch (Exception)
            {
            }
        return;
    }
    auto payload = rec.data.asChars;
    auto hdrs = propsHeaders(effProps);
    amqpCountPub(); // per-shard counter: no cross-shard atomic on the hot path
    int routed = drRouted;
    // the push stays IN-WALK (the collect-then-push restructure broke the
    // Erlang serialization cases — reverted in b743fa9); CC/BCC dedup happens
    // inside the sink via `seen`, whose entries are COPIES (the walk's names
    // live in reused TLS a reentrant routeTo refills during our push yields).
    // dedup names are copied into a STACK arena (not GC idup — that was one
    // allocation PER MESSAGE on the hot path; the copies must still be real
    // because the walk's names live in reused TLS that a reentrant routeTo
    // refills during our push yields)
    char[2048] seenArena = void;
    size_t seenUsed = 0;
    uint[64] seenOff = void;
    uint[64] seenLen = void;
    size_t ns = 0;
    bool pubRejected = false; // x-overflow reject-publish refused a target queue
    scope void delegate(scope const(char)[]) nothrow pushSink = (scope const(char)[] q) nothrow {
        foreach (di; 0 .. ns)
            if (seenArena[seenOff[di] .. seenOff[di] + seenLen[di]] == q)
                return; // already delivered for this publish (noDuplicates)
        if (ns < seenOff.length && seenUsed + q.length <= seenArena.length)
        {
            seenArena[seenUsed .. seenUsed + q.length] = q[];
            seenOff[ns] = cast(uint) seenUsed;
            seenLen[ns] = cast(uint) q.length;
            seenUsed += q.length;
            ns++;
        }
        // route only to queues that EXIST (RabbitMQ): the default exchange
        // routes by name, and a push to an undeclared name would create a
        // ghost list — and defeat the mandatory basic.return (312 NO_ROUTE).
        // Existence + max-length come from the epoch-gated memo: a run of
        // publishes to the same queue skips the AA probes entirely.
        bool exists;
        long mlP1;
        ubyte ovf;
        {
            import core.atomic : MemoryOrder, atomicLoad;

            immutable ep = atomicLoad!(MemoryOrder.raw)(gAmqpTopoEpoch);
            if (ep == tPubMemoEpoch && q.length == tPubMemoQLen
                    && tPubMemoQBuf[0 .. tPubMemoQLen] == q)
            {
                exists = tPubMemoExists;
                mlP1 = tPubMemoMaxLen;
                ovf = tPubMemoOverflow;
            }
            else
            {
                exists = queueExists(q);
                mlP1 = 0;
                ovf = 0;
                if (exists)
                    try
                        if (auto m = cast(string) q in gQueueMeta) // reinterpret: read-only AA probe, no alloc
                        {
                            mlP1 = m.maxLen;
                            ovf = m.overflow;
                        }
                    catch (Exception)
                    {
                    }
                if (q.length <= tPubMemoQBuf.length)
                {
                    tPubMemoQBuf[0 .. q.length] = q[];
                    tPubMemoQLen = q.length;
                    tPubMemoExists = exists;
                    tPubMemoMaxLen = mlP1;
                    tPubMemoOverflow = ovf;
                    tPubMemoEpoch = ep;
                }
            }
        }
        if (!exists)
            return;
        static ByteBuffer kb3; // TLS
        queueKey(q, kb3);
        // x-max-priority: the message lands in the list for ITS priority level.
        // Level 0 is the queue's own key, so a plain FIFO queue never takes this
        // branch and its key is unchanged.
        {
            immutable mp = queueMaxPrio(q);
            if (mp > 0)
            {
                long pmP;
                int dthP;
                const(char)[] rkP;
                const(ubyte)[] prP, bdP;
                splitRecord(cast(const(ubyte)[]) payload, pmP, dthP, rkP, prP, bdP);
                queueKeyPrio(q, propsPriority(prP, mp), kb3);
            }
        }
        // x-overflow: reject-publish. The default (drop-head) evicts the queue's
        // HEAD after the push; reject-publish refuses the message instead, and a
        // publisher in confirm mode is nacked rather than acked. The length probe
        // costs a keyspace read, so it runs ONLY for a queue configured this way.
        if (ovf == 1 && mlP1 > 0 && gAmqpLen !is null)
        {
            if (gAmqpLen(kb3.data.asChars) >= mlP1 - 1)
            {
                pubRejected = true;
                return; // not enqueued: routed stays 0 for this queue
            }
        }
        // FIRE the RPUSH: a local key applies inline, a remote one is enqueued into
        // the owner's ring and forgotten (the ring guarantees delivery — see
        // gAmqpPushStage). The confirm ships at the batch flush, already promised.
        if (gAmqpPushStage !is null)
            gAmqpPushStage(kb3.data.asChars, payload);
        else if (gAmqpPush !is null)
            gAmqpPush(kb3.data.asChars, payload);
        routed++;
        if (mlP1 > 0)
            enforceMaxLen(q);
    };
    if (!drDirect)
        routeTo(ch.pub.exchange, ch.pub.rkey, hdrs, pushSink, ccKeys[0 .. nCc]);
    // alternate-exchange: an UNROUTED message cascades through the AE chain
    // (same routing key + CC); a revisit or depth cap breaks x->u->v->x cycles
    if (routed == 0)
    {
        string[8] seenAE;
        size_t nAE = 0;
        try
            seenAE[nAE++] = ch.pub.exchange;
        catch (Exception)
        {
        }
        auto cur = cast(string) ch.pub.exchange;
        while (routed == 0 && nAE < seenAE.length)
        {
            string ae;
            try
                if (auto pae = cur in gExchangeAE)
                    ae = *pae;
            catch (Exception)
            {
            }
            if (ae.length == 0)
                break;
            bool revisit = false;
            foreach (sx; seenAE[0 .. nAE])
                if (sx == ae)
                {
                    revisit = true;
                    break;
                }
            if (revisit)
                break;
            seenAE[nAE++] = ae;
            routeTo(ae, ch.pub.rkey, hdrs, pushSink, ccKeys[0 .. nCc]);
            cur = ae;
        }
    }
    // [basic.return] a mandatory publish that matched no queue must come BACK
    // to the publisher (312 NO_ROUTE) instead of vanishing while confirmed
    if (mandatory && routed == 0)
    {
        auto exn = ch.pub.exchange;
        auto rkn = ch.pub.rkey;
        method(o, chan, 60, 50, (ref ByteBuffer b) @nogc nothrow {
            putU16(b, 312); // NO_ROUTE
            putShortStr(b, "NO_ROUTE");
            putShortStr(b, exn);
            putShortStr(b, rkn);
        });
        emitContent(o, chan, cast(const(ubyte)[]) payload, c.frameMax);
        amqpCountRet();
    }
    if (ch.confirmMode)
    {
        // RabbitMQ confirms a returned mandatory message too: the return is the
        // routing signal, the ack is the broker-took-responsibility signal.
        // x-overflow reject-publish is the one case where the broker refuses
        // responsibility, and that is a basic.nack, not a basic.ack.
        immutable tag = ch.confirmSeq++;
        method(o, chan, 60, pubRejected ? 120 : 80, (ref ByteBuffer b) @nogc nothrow {
            putU64(b, tag);
            b.appendByte(0); // multiple=false (nack's requeue bit is unused here)
        });
    }
}

private void emitContent(ref ByteBuffer o, ushort chan, scope const(ubyte)[] blob,
        uint frameMax = AMQP_FRAME_MAX) nothrow
{
    long pm0;
    int dths0;
    const(char)[] rkey0;
    const(ubyte)[] props, body_;
    splitRecord(blob, pm0, dths0, rkey0, props, body_);
    // content HEADER frame — the publisher's property block replays VERBATIM
    // (content-type, headers, correlation-id ... survive the queue)
    size_t at;
    frameStart(o, FRAME_HEADER, chan, at);
    putU16(o, 60); // class basic
    putU16(o, 0); // weight
    putU64(o, body_.length);
    // The content-header frame carries the property block in ONE frame (there is
    // no header continuation), so it must fit the peer's negotiated frame-max. A
    // consumer with a smaller frame-max than the publisher — or a cross-protocol
    // record with an oversized headers table — would otherwise get a header frame
    // exceeding frame-max and desync/drop. Frame = 7 hdr + 12 fixed + props + 1
    // end; emit props only when they fit, else send an empty property block.
    if (props.length >= 2 && cast(ulong) props.length + 20 <= frameMax)
        o.append(props);
    else
        putU16(o, 0); // no properties (absent, or too large for this frame-max)
    frameFinish(o, at);
    // BODY frames: an EMPTY body emits NO body frame (a spurious zero-length
    // body frame after body-size=0 desyncs a strict client's parser — pika
    // drops the connection with a NoneType body_size error). A body larger than
    // the advertised frame-max (131072) is split so no single frame exceeds it
    // (each frame = 8 bytes of framing + payload, so payload <= frame-max - 8).
    immutable size_t bodyChunk = frameMax - 8; // frameMax is clamped >= 4096
    size_t off = 0;
    while (off < body_.length)
    {
        immutable end = body_.length - off > bodyChunk ? off + bodyChunk : body_.length;
        frameStart(o, FRAME_BODY, chan, at);
        o.append(body_[off .. end]);
        frameFinish(o, at);
        off = end;
    }
}

/// A dying connection returns everything unacked to the FRONT of its queue —
/// the at-least-once contract for no_ack=false consumers.
// Release each channel's malloc-plane ByteBuffers on the OWNING shard thread.
// Left to the GC, ByteBuffer.~this would run on whatever thread triggered the
// collection and free these blocks into another shard's per-thread freelist =
// heap corruption (the shard-registry bug class). (scope(exit) can't hold a
// try/catch, so this lives in a named nothrow helper.)
private void releaseChannels(AmqpConn c) nothrow @trusted
{
    try
        foreach (ref ch; c.chans)
        {
            ch.pub.payload.release();
            ch.pub.props.release();
        }
    catch (Exception)
    {
    }
}

private void requeueAllUnacked(AmqpConn c) nothrow @trusted
{
    try
    {
        // snapshot the tags, then requeue in DESCENDING order so the pushFront
        // leaves them in ascending (delivery/FIFO) order at the queue front. The
        // snapshot also decouples the iteration from gAmqpPushFront's cross-shard
        // yield (no live-AA iterator held across the hop).
        import std.algorithm.sorting : sort;

        ulong[] tags;
        foreach (t, ref u; c.unacked)
            tags ~= t;
        sort!"a > b"(tags);
        foreach (t; tags)
            if (auto u = t in c.unacked)
            {
                if (!queueExists(u.queue))
                    continue; // dead queue: no ghost list on teardown either
                static ByteBuffer kb6; // TLS
                queueKeyPrio(u.queue, recordPrio(u.queue, u.blob), kb6);
                static ByteBuffer rq6; // TLS: redelivered-marked copy
                markRedelivered(rq6, u.blob);
                if (gAmqpPushFront !is null)
                    gAmqpPushFront(kb6.data.asChars, rq6.data.asChars);
                enforceMaxLen(u.queue); // x-max-length holds across requeues
            }
    }
    catch (Exception)
    {
    }
    // OUTSIDE the try: the map owns every record's memory, so a throw in the
    // requeue loop above must not skip the free.
    try
    {
        c.unacked.clear(); // releases every record's blob AND its window credit
        c.unackedBytes = 0;
    }
    catch (Exception)
    {
    }
}

/// Negative settle: requeue=true puts the record back at the queue FRONT;
/// Remove one unacked record (positive ack path), discounting its bytes from the
/// window accumulator. Call AFTER a foreach over unacked has collected the tags,
/// never during it. Underflow-guarded.
/// Decrement the owning channel's live-unacked counter (no-op if the channel
/// is already gone — its counter died with it).
/// Drop `tag` from the channel's global-window round-robin order (nothrow-safe
/// for scope(exit) use — array concat can allocate).
private void rrOrderRemove(AmqpConn c, ushort chan, string tag) nothrow @trusted
{
    try
        if (auto rchx = chan in c.chans)
            foreach (i3, v3; rchx.rrOrder)
                if (v3 == tag)
                {
                    rchx.rrOrder = rchx.rrOrder[0 .. i3] ~ rchx.rrOrder[i3 + 1 .. $];
                    break;
                }
    catch (Exception)
    {
    }
}

private void chanUnackedDec(AmqpConn c, ushort chan) nothrow @trusted
{
    if (auto ch = chan in c.chans)
        if (ch.unackedN > 0)
            ch.unackedN--;
}

private void dropUnacked(AmqpConn c, ulong tag, Unacked* pu = null) nothrow @trusted
{
    try
        if (auto p = pu !is null ? pu : (tag in c.unacked))
        {
            immutable n = p.blob.length;
            c.unackedBytes = c.unackedBytes >= n ? c.unackedBytes - n : 0;
            if (!p.fromGet)
            {
                chanUnackedDec(c, p.chan);
            }
            c.unacked.remove(tag);
        }
    catch (Exception)
    {
    }
}

/// requeue=false dead-letters via the queue's x-dead-letter-exchange (declared
/// metadata) or drops when none is configured.
private void settleNegative(AmqpConn c, ulong tag, bool requeue) nothrow @trusted
{
    Unacked u;
    ByteBuffer keep; // stack-local: outlives the map entry, not shared across yields
    bool found = false;
    try
    {
        if (auto p = tag in c.unacked)
        {
            u = *p;
            found = true;
            immutable n = u.blob.length;
            c.unackedBytes = c.unackedBytes >= n ? c.unackedBytes - n : 0;
            if (!u.fromGet)
            {
                chanUnackedDec(c, u.chan);
            }
            // The map OWNS u.blob and frees it on remove, but everything below
            // (markRedelivered, deadLetter) still needs the record — and it
            // cannot run before the remove, because both yield and a sibling
            // ack of this same tag during that yield would settle it twice.
            // So take a copy, then remove.
            keep.append(u.blob);
            u.blob = keep.data;
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
        if (!queueExists(u.queue))
            return; // the queue died: its messages die with it (no ghost list)
        // x-delivery-limit: a message that has already been handed out this many
        // times is not put back again — it is dead-lettered, so a consumer that
        // keeps nacking cannot spin the same record forever.
        immutable dlim = queueDeliveryLimit(u.queue);
        if (dlim >= 0 && recordDeliveryCount(u.blob) >= dlim)
        {
            deadLetter(u.queue, u.blob, "delivery_limit");
            return;
        }
        queueKeyPrio(u.queue, recordPrio(u.queue, u.blob), kb4);
        // mark the requeued copy redelivered so the next delivery sets the flag
        // (both TLS buffers are consumed by gAmqpPushFront before its yield)
        static ByteBuffer rq4; // TLS
        markRedelivered(rq4, u.blob);
        if (gAmqpPushFront !is null)
            gAmqpPushFront(kb4.data.asChars, rq4.data.asChars);
        // x-max-length holds across REQUEUES too (RabbitMQ): an over-cap
        // queue head-drops (dead-lettering via DLX when configured)
        enforceMaxLen(u.queue);
        return;
    }
    deadLetter(u.queue, u.blob, "rejected");
}

/// The queue's x-delivery-limit, or -1 when unset. A record whose delivery
/// count has REACHED it is dead-lettered instead of requeued.
private long queueDeliveryLimit(scope const(char)[] q) nothrow @trusted
{
    try
        if (auto m = cast(string) q in gQueueMeta)
            return m.delLimit > 0 ? m.delLimit - 1 : -1;
    catch (Exception)
    {
    }
    return -1;
}

/// The queue's x-message-ttl in ms (-1 = none; 0 = expire immediately).
/// Looked up per delivery.
private long queueTtl(scope const(char)[] q) nothrow @trusted
{
    try
        if (auto m = q in gQueueMeta)
            return m.ttlSet ? m.ttlMs : -1;
    catch (Exception)
    {
    }
    return -1;
}

/// The record's EFFECTIVE TTL: the smaller of the queue x-message-ttl and the
/// per-message expiration, -1 when neither applies. Mirrors isExpired's merge.
private long effectiveTtl(scope const(ubyte)[] blob, long ttlMs) nothrow @trusted
{
    long pm;
    int dths;
    const(char)[] rk;
    const(ubyte)[] props, body_;
    splitRecord(blob, pm, dths, rk, props, body_);
    immutable msgTtl = propsExpiration(props);
    long ttl = ttlMs >= 0 ? ttlMs : -1;
    if (msgTtl >= 0 && (ttl < 0 || msgTtl < ttl))
        ttl = msgTtl;
    return ttl;
}

/// Has this record outlived `ttlMs` since it was published? (v3 records carry
/// the publish time; older records report 0 = never expire lazily.)
/// `ttlMs` contract: -1 = the queue has no TTL, 0 = expire immediately.
private bool isExpired(scope const(ubyte)[] blob, long ttlMs) nothrow @trusted
{
    long pm;
    int dths;
    const(char)[] rk;
    const(ubyte)[] props, body_;
    splitRecord(blob, pm, dths, rk, props, body_);
    // effective TTL = the SMALLER of the queue x-message-ttl and the message's
    // own `expiration` property (RabbitMQ semantics), each side ignored when
    // absent (-1; -2 = invalid expiration). A per-message expiration expires
    // even on a queue with no x-message-ttl (lazily at delivery; the active
    // reaper only sweeps queues that have a queue-level TTL).
    immutable msgTtl = propsExpiration(props);
    long ttl = ttlMs >= 0 ? ttlMs : -1;
    if (msgTtl >= 0 && (ttl < 0 || msgTtl < ttl))
        ttl = msgTtl;
    if (ttl < 0)
        return false;
    if (ttl == 0)
        return pm > 0; // TTL 0: expired the moment it was stored
    // compare as `published <= now - ttl` (NOT `now > published + ttl`): the
    // sum form overflows i64 for a client-supplied huge x-message-ttl and
    // wraps negative, falsely expiring fresh messages. `now - ttl` underflows
    // to negative for such a ttl -> pm(>0) <= negative is false -> never
    // expires, the correct reading of a ~290-million-year TTL.
    return pm > 0 && pm <= cast(long) nowMs() - ttl;
}

/// Route an (already-stored) record to `queue`'s dead-letter exchange, keeping
/// the original routing key unless x-dead-letter-routing-key overrides it.
/// Shared by nack/reject (settleNegative) and TTL expiry at delivery.
// --- x-death header (RabbitMQ dead-letter provenance a DLX consumer reads for
// poison-message handling: count, reason, queue, ...) ---
/// x-death bookkeeping for a fresh death at (queue, reason): returns the PRIOR
/// count for this (queue, reason) and appends every OTHER existing entry
/// verbatim into `others` ('F' + u32 + table each) — RabbitMQ keeps ONE entry
/// per (queue, reason), incrementing its count, and preserves the rest.
private long xDeathOthers(scope const(ubyte)[] props, scope const(char)[] queue,
        scope const(char)[] reason, ref ByteBuffer others,
        ref bool sawQueue, ref bool sawRejected) @nogc nothrow @trusted
{
    long prior = 0;
    sawQueue = false;
    sawRejected = false;
    auto h = propsHeaders(props);
    if (h is null || h.length == 0)
        return 0;
    cast(void) tableWalk(h, (scope const(char)[] k, char ty, scope const(ubyte)[] v) @nogc nothrow {
        if (k != "x-death" || ty != 'A')
            return true;
        size_t i = 0;
        while (i + 5 <= v.length)
        {
            immutable et = v[i];
            immutable ln = (cast(size_t) v[i + 1] << 24) | (cast(size_t) v[i + 2] << 16)
                | (cast(size_t) v[i + 3] << 8) | v[i + 4];
            if (et != 'F' || i + 5 + ln > v.length)
                break;
            auto elem = v[i .. i + 5 + ln];
            auto tbl = v[i + 5 .. i + 5 + ln];
            const(char)[] eq, er;
            long ec = 0;
            cast(void) tableWalk(tbl, (scope const(char)[] k2, char t2,
                    scope const(ubyte)[] v2) @nogc nothrow {
                if (k2 == "queue" && t2 == 'S')
                    eq = cast(const(char)[]) v2;
                else if (k2 == "reason" && t2 == 'S')
                    er = cast(const(char)[]) v2;
                else if (k2 == "count" && t2 == 'l' && v2.length == 8)
                {
                    ec = 0;
                    foreach (b2; v2)
                        ec = (ec << 8) | b2;
                }
                return true;
            });
            if (eq == queue)
                sawQueue = true; // any reason: cycle-detection input
            if (er == "rejected")
                sawRejected = true; // a client action breaks a pure-expiry cycle
            if (eq == queue && er == reason)
                prior = ec;
            else
                others.append(cast(const(char)[]) elem);
            i += 5 + ln;
        }
        return false;
    });
    return prior;
}

// --- Direct Reply-To (amq.rabbitmq.reply-to pseudo-queue, rabbit extension) ---
enum DIRECT_REPLY_Q = "amq.rabbitmq.reply-to";
/// Connections served by THIS shard, keyed by connId (direct-reply routing).
private AmqpConn[ulong] gConnsById; // TLS

// --- Cross-shard connection registry (management API M4 v2) -----------------
// A process-global list so the mgmt thread can enumerate/kill AMQP connections
// across every shard. Each entry copies the display fields plus a pointer to
// the conn's `killReq` flag (a GC class field: non-moving, stable address while
// the conn lives; unregistered before the conn is freed). All access under the
// mutex — the mgmt thread and N shard threads touch it.
struct AmqpConnEntry
{
    ulong connId;
    shared(bool)* killPtr;
    char[80] name = void;
    ubyte nameLen;
    char[64] user = void;
    ubyte userLen;
    long connectedMs;
    uint shardId;
}

import core.sync.mutex : Mutex;

private __gshared Mutex gAmqpRegMutex;
private __gshared AmqpConnEntry[] gAmqpReg;
private shared bool gAmqpRegInit;

private void amqpRegEnsure() nothrow @trusted
{
    import core.atomic : cas;

    if (cas(&gAmqpRegInit, false, true))
    {
        try
            gAmqpRegMutex = new Mutex;
        catch (Exception)
        {
        }
    }
    // a losing racer must wait for the winner to publish the Mutex
    while (gAmqpRegMutex is null)
    {
    }
}

private void amqpRegAdd(AmqpConn c, uint shardId) nothrow @trusted
{
    amqpRegEnsure();
    try
    {
        gAmqpRegMutex.lock();
        scope (exit)
            gAmqpRegMutex.unlock();
        AmqpConnEntry e;
        e.connId = c.connId;
        e.killPtr = &c.killReq;
        immutable nl = c.peerNameLen < e.name.length ? c.peerNameLen : cast(ubyte) e.name.length;
        e.name[0 .. nl] = c.peerName[0 .. nl];
        e.nameLen = nl;
        immutable ul = c.loginUserLen < e.user.length ? c.loginUserLen : cast(ubyte) e.user.length;
        e.user[0 .. ul] = c.loginUser[0 .. ul];
        e.userLen = ul;
        e.connectedMs = c.connectedMs;
        e.shardId = shardId;
        gAmqpReg ~= e;
    }
    catch (Exception)
    {
    }
}

private void amqpRegRemove(ulong connId) nothrow @trusted
{
    if (gAmqpRegMutex is null)
        return;
    try
    {
        gAmqpRegMutex.lock();
        scope (exit)
            gAmqpRegMutex.unlock();
        foreach (i; 0 .. gAmqpReg.length)
            if (gAmqpReg[i].connId == connId)
            {
                gAmqpReg[i] = gAmqpReg[$ - 1];
                gAmqpReg.length = gAmqpReg.length - 1;
                break;
            }
    }
    catch (Exception)
    {
    }
}

/// Management API: serialize the live AMQP connections as JSON. Runs on the
/// mgmt thread — reads the shared registry under the mutex (a pure copy, no
/// conn touched). name = "host:port", the RabbitMQ connection identifier.
public void amqpConnectionsJson(ref ByteBuffer o) nothrow @trusted
{
    o.append("[");
    if (gAmqpRegMutex !is null)
    {
        try
        {
            gAmqpRegMutex.lock();
            scope (exit)
                gAmqpRegMutex.unlock();
            foreach (i, ref e; gAmqpReg)
            {
                if (i)
                    o.append(",");
                o.append(`{"name":"`);
                amqpJsonStr(o, e.name[0 .. e.nameLen]);
                o.append(`","user":"`);
                amqpJsonStr(o, e.userLen ? e.user[0 .. e.userLen] : cast(char[]) "guest");
                o.append(`","vhost":"/","protocol":"AMQP 0-9-1","state":"running",`);
                o.append(`"channels":0,"node":"dreads@localhost"}`);
            }
        }
        catch (Exception)
        {
        }
    }
    o.append("]");
}

/// Management API: request-close every connection whose name == `name` (or ALL
/// when name is empty). Sets the cross-thread kill flag; the owning shard
/// thread closes the socket at its next read-wait tick. Returns how many were
/// flagged.
public size_t amqpKillConnection(scope const(char)[] name) nothrow @trusted
{
    import core.atomic : atomicStore;

    if (gAmqpRegMutex is null)
        return 0;
    size_t n;
    try
    {
        gAmqpRegMutex.lock();
        scope (exit)
            gAmqpRegMutex.unlock();
        foreach (ref e; gAmqpReg)
            if (name.length == 0 || e.name[0 .. e.nameLen] == name)
            {
                if (e.killPtr !is null)
                    atomicStore(*e.killPtr, true);
                n++;
            }
    }
    catch (Exception)
    {
    }
    return n;
}

private void amqpJsonStr(ref ByteBuffer o, scope const(char)[] s) nothrow @trusted
{
    foreach (ch; s)
    {
        if (ch == '"' || ch == '\\')
        {
            o.appendByte('\\');
            o.appendByte(ch);
        }
        else if (cast(ubyte) ch >= 0x20)
            o.appendByte(ch);
    }
}
private __gshared ulong gDrSecret; // keyed into the reply token (anti-forgery)

/// Weak keyed hash for the reply token (anti-tamper, not crypto: the java
/// suite's `hack` test only proves a corrupted token cannot publish).
private ulong drSig(ulong connId, uint chan, uint gen) @nogc nothrow
{
    ulong h = gDrSecret ^ 0x9E3779B97F4A7C15;
    static void mix(ref ulong hh, ulong v) @nogc nothrow
    {
        hh ^= v + 0x9E3779B97F4A7C15 + (hh << 6) + (hh >> 2);
        hh *= 0xFF51AFD7ED558CCD;
    }

    mix(h, connId);
    mix(h, chan);
    mix(h, gen);
    return h;
}

/// "amq.rabbitmq.reply-to.<connId>-<chan>-<gen>-<sig>" (decimal fields).
private size_t drToken(char[] buf, ulong connId, uint chan, uint gen) @nogc nothrow
{
    import core.stdc.stdio : snprintf;

    return cast(size_t) snprintf(buf.ptr, buf.length, "%s.%llu-%u-%u-%llx",
            DIRECT_REPLY_Q.ptr, connId, chan, gen, drSig(connId, chan, gen));
}

/// Parse+verify a reply token. Returns false on any malformation or bad sig.
private bool drParse(scope const(char)[] name, out ulong connId, out uint chan, out uint gen) @nogc nothrow
{
    enum pfx = DIRECT_REPLY_Q ~ ".";
    if (name.length <= pfx.length || name[0 .. pfx.length] != pfx)
        return false;
    auto t = name[pfx.length .. $];
    ulong[4] f;
    size_t fi = 0, i = 0;
    while (i < t.length && fi < 4)
    {
        ulong v = 0;
        bool any = false;
        immutable hex = fi == 3;
        while (i < t.length && t[i] != '-')
        {
            immutable ch2 = t[i];
            int d2 = -1;
            if (ch2 >= '0' && ch2 <= '9')
                d2 = ch2 - '0';
            else if (hex && ch2 >= 'a' && ch2 <= 'f')
                d2 = ch2 - 'a' + 10;
            else
                return false;
            v = v * (hex ? 16 : 10) + cast(ulong) d2;
            any = true;
            i++;
        }
        if (!any)
            return false;
        f[fi++] = v;
        if (i < t.length && t[i] == '-')
            i++;
    }
    if (fi != 4 || i != t.length)
        return false;
    connId = f[0];
    chan = cast(uint) f[1];
    gen = cast(uint) f[2];
    return f[3] == drSig(connId, cast(uint) f[1], cast(uint) f[2]);
}

/// First 'S' (longstr) value under `key` in the props' headers table, or null.
private const(char)[] headerStr(scope const(ubyte)[] props, scope const(char)[] key) @nogc nothrow @trusted
{
    const(char)[] val;
    auto h = propsHeaders(props);
    if (h is null || h.length == 0)
        return null;
    cast(void) tableWalk(h, (scope const(char)[] k, char ty, scope const(ubyte)[] v) @nogc nothrow {
        if (k != key || ty != 'S')
            return true;
        val = cast(const(char)[]) v;
        return false;
    });
    return val;
}

/// Raw contents of the first field-array ('A') value under `key` in the props'
/// headers table, or null.
private const(ubyte)[] headerArr(scope const(ubyte)[] props, scope const(char)[] key) @nogc nothrow @trusted
{
    const(ubyte)[] val;
    auto h = propsHeaders(props);
    if (h is null || h.length == 0)
        return null;
    cast(void) tableWalk(h, (scope const(char)[] k, char ty, scope const(ubyte)[] v) @nogc nothrow {
        if (k != key || ty != 'A')
            return true;
        val = v;
        return false;
    });
    return val;
}

private void patchU32(ref ByteBuffer o, size_t at, uint v) @nogc nothrow @trusted
{
    auto d = cast(ubyte[]) o.data;
    if (at + 4 <= d.length)
    {
        d[at] = cast(ubyte)(v >> 24);
        d[at + 1] = cast(ubyte)(v >> 16);
        d[at + 2] = cast(ubyte)(v >> 8);
        d[at + 3] = cast(ubyte)(v & 0xFF);
    }
}

private void xtStr(ref ByteBuffer o, scope const(char)[] key, scope const(char)[] val) @nogc nothrow
{
    o.appendByte(cast(char) key.length);
    o.append(key);
    o.appendByte('S');
    putU32(o, cast(uint) val.length);
    o.append(val);
}

/// Encode the `x-death` headers entry: [keyLen]["x-death"]['A'] array of one
/// table {count 'l', reason/queue/exchange 'S', routing-keys 'A', }.
/// `ccRaw` is the raw contents of a CC header field-array ('S' elements): the
/// death's routing-keys are the keys the message was PUBLISHED with — original
/// rk + CC, BCC excluded (already stripped at publish). An x-dead-letter-
/// routing-key override is NOT recorded here (it only names the re-publish key;
/// the java suite's deadLetterNewRK asserts exactly [rk, CC...]).
/// `origExp` non-empty = the per-message `expiration` being removed on this
/// death, preserved as original-expiration (RabbitMQ 3.x).
private void buildXDeathEntry(ref ByteBuffer o, long count, scope const(char)[] reason,
        scope const(char)[] queue, scope const(char)[] rk,
        scope const(char)[] origEx, scope const(ubyte)[] ccRaw,
        scope const(char)[] origExp, scope const(ubyte)[] othersRaw) @nogc nothrow
{
    o.appendByte(cast(char) 7);
    o.append("x-death");
    o.appendByte('A'); // array
    immutable arrAt = o.length;
    putU32(o, 0);
    immutable arrStart = o.length;
    o.appendByte('F'); // one table element
    immutable tblAt = o.length;
    putU32(o, 0);
    immutable tblStart = o.length;
    o.appendByte(cast(char) 5);
    o.append("count");
    o.appendByte('l');
    putU64(o, cast(ulong) count);
    xtStr(o, "reason", reason);
    xtStr(o, "queue", queue);
    xtStr(o, "exchange", origEx); // the v4 record carries the original exchange
    if (origExp.length)
        xtStr(o, "original-expiration", origExp);
    o.appendByte(cast(char) 12);
    o.append("routing-keys");
    o.appendByte('A');
    immutable rkAt = o.length;
    putU32(o, 0);
    immutable rkStart = o.length;
    o.appendByte('S');
    putU32(o, cast(uint) rk.length);
    o.append(rk);
    // CC keys ride along verbatim ('S' elements only, deduped vs the rk)
    {
        size_t ci = 0;
        while (ci + 5 <= ccRaw.length)
        {
            immutable ct = ccRaw[ci];
            immutable cl = (cast(size_t) ccRaw[ci + 1] << 24) | (cast(size_t) ccRaw[ci + 2] << 16)
                | (cast(size_t) ccRaw[ci + 3] << 8) | ccRaw[ci + 4];
            if (ct != 'S' || ci + 5 + cl > ccRaw.length)
                break;
            auto cv = cast(const(char)[]) ccRaw[ci + 5 .. ci + 5 + cl];
            if (cv != rk)
                o.append(cast(const(char)[]) ccRaw[ci .. ci + 5 + cl]);
            ci += 5 + cl;
        }
    }
    patchU32(o, rkAt, cast(uint)(o.length - rkStart));
    patchU32(o, tblAt, cast(uint)(o.length - tblStart));
    // OTHER (queue, reason) entries survive verbatim after ours
    o.append(cast(const(char)[]) othersRaw);
    patchU32(o, arrAt, cast(uint)(o.length - arrStart));
}

/// Append every entry of headers-table `t` to `dst` EXCEPT the one keyed
/// `skipKey`. Used to drop a prior x-death before adding the current one, so a
/// message dead-lettered N times carries ONE x-death (with the live count), not
/// N stale duplicates. Bails (copying nothing further) on a malformed entry.
private void appendHeadersExcept(ref ByteBuffer dst, scope const(ubyte)[] t,
        scope const(char)[] skipKey, bool dropDeathMeta = false) @nogc nothrow
{
    static bool deathMetaKey(scope const(char)[] k) @nogc nothrow
    {
        // the six 3.7+/3.10+ death-tracking headers (re-emitted fresh on merge)
        enum fp = "x-first-death-", lp = "x-last-death-";
        return (k.length > fp.length && k[0 .. fp.length] == fp)
            || (k.length > lp.length && k[0 .. lp.length] == lp);
    }

    size_t i = 0;
    while (i < t.length)
    {
        immutable entryStart = i;
        immutable kn = t[i];
        i += 1;
        if (i + kn + 1 > t.length)
            return;
        auto key = cast(const(char)[]) t[i .. i + kn];
        i += kn;
        immutable ty = cast(char) t[i];
        i += 1;
        switch (ty)
        {
        case 'S', 'x', 'A', 'F':
            if (i + 4 > t.length)
                return;
            immutable vlen = (cast(size_t) t[i] << 24) | (cast(size_t) t[i + 1] << 16)
                | (cast(size_t) t[i + 2] << 8) | t[i + 3];
            i += 4 + vlen;
            break;
        case 's':
            i += 2; // rabbit-dialect short, not shortstr
            break;
        case 't', 'b', 'B':
            i += 1;
            break;
        case 'U', 'u':
            i += 2;
            break;
        case 'I', 'i', 'f':
            i += 4;
            break;
        case 'l', 'd', 'T':
            i += 8;
            break;
        case 'D':
            i += 5;
            break;
        case 'V':
            break;
        default:
            return; // unknown type: can't size the value -> stop
        }
        if (i > t.length)
            return;
        if (key != skipKey && !(dropDeathMeta && deathMetaKey(key)))
            dst.append(cast(const(char)[]) t[entryStart .. i]);
    }
}

/// Rebuild `props` with `xentry` prepended to the headers table (creating the
/// headers property if absent) and any PRIOR x-death dropped. Every other
/// property survives verbatim. On any malformed length the tail is copied as-is
/// (best-effort, never OOB).
private void mergeXDeath(ref ByteBuffer dst, scope const(ubyte)[] props,
        scope const(ubyte)[] xentry) @nogc nothrow @trusted
{
    dst.clear();
    if (props.length < 2)
    {
        dst.appendByte(cast(char) 0x20); // flags 0x2000 (headers only)
        dst.appendByte(cast(char) 0x00);
        putU32(dst, cast(uint) xentry.length);
        dst.append(cast(const(char)[]) xentry);
        return;
    }
    immutable flags = (cast(ushort) props[0] << 8) | props[1];
    size_t i = 2;
    // + headers (the x-death lives there), - expiration (removed on
    // dead-lettering so the message can't re-expire downstream; preserved as
    // original-expiration inside the x-death entry)
    immutable nf = cast(ushort)((flags | 0x2000) & ~0x0100);
    dst.appendByte(cast(char)(nf >> 8));
    dst.appendByte(cast(char)(nf & 0xFF));
    // content-type, then content-encoding: copy each shortstr verbatim
    if (flags & 0x8000)
    {
        if (i >= props.length || i + 1 + props[i] > props.length)
        {
            dst.append(cast(const(char)[]) props[i .. $]);
            return;
        }
        immutable seg = 1 + props[i];
        dst.append(cast(const(char)[]) props[i .. i + seg]);
        i += seg;
    }
    if (flags & 0x4000)
    {
        if (i >= props.length || i + 1 + props[i] > props.length)
        {
            dst.append(cast(const(char)[]) props[i .. $]);
            return;
        }
        immutable seg = 1 + props[i];
        dst.append(cast(const(char)[]) props[i .. i + seg]);
        i += seg;
    }
    const(ubyte)[] existing;
    if (flags & 0x2000)
    {
        if (i + 4 > props.length)
        {
            dst.append(cast(const(char)[]) props[i .. $]);
            return;
        }
        immutable hl = (cast(size_t) props[i] << 24) | (cast(size_t) props[i + 1] << 16)
            | (cast(size_t) props[i + 2] << 8) | props[i + 3];
        i += 4;
        if (i + hl > props.length)
        {
            dst.append(cast(const(char)[]) props[i - 4 .. $]);
            return;
        }
        existing = props[i .. i + hl];
        i += hl;
    }
    // drop any prior x-death from the existing headers so the message carries a
    // single x-death with the live count, not one stale duplicate per hop
    static ByteBuffer hstrip; // TLS (consumed synchronously, no yield in here)
    hstrip.clear();
    appendHeadersExcept(hstrip, existing, "x-death", true);
    putU32(dst, cast(uint)(xentry.length + hstrip.length));
    dst.append(cast(const(char)[]) xentry);
    dst.append(cast(const(char)[]) hstrip.data);
    // delivery-mode onward: verbatim EXCEPT the expiration shortstr (its flag
    // bit is already cleared in nf). Walk the fixed order: delivery-mode(1B),
    // priority(1B), correlation-id(ss), reply-to(ss), expiration(ss).
    if (flags & 0x1000) // delivery-mode
        if (i < props.length)
            dst.appendByte(cast(char) props[i++]);
    if (flags & 0x0800) // priority
        if (i < props.length)
            dst.appendByte(cast(char) props[i++]);
    static immutable int[3] ssBits = [0x0400, 0x0200, 0x0100]; // correlation-id, reply-to, expiration
    foreach (bit; ssBits)
    {
        if (!(flags & bit))
            continue;
        if (i >= props.length || i + 1 + props[i] > props.length)
            return; // malformed: stop (never OOB)
        immutable seg = 1 + props[i];
        if (bit != 0x0100)
            dst.append(cast(const(char)[]) props[i .. i + seg]);
        i += seg;
    }
    dst.append(cast(const(char)[]) props[i .. $]); // message-id onward verbatim
}

private void deadLetter(scope const(char)[] queue, scope const(ubyte)[] blob,
        scope const(char)[] reason) nothrow @trusted
{
    QueueMeta meta;
    try
        if (auto m = queue in gQueueMeta)
            meta = *m;
    catch (Exception)
    {
    }
    if (!meta.dlxSet)
        return; // no dead-letter exchange: drop ("" is VALID: the default exchange)
    long pm;
    int deaths;
    const(char)[] origRk;
    const(ubyte)[] props, body_;
    splitRecord(blob, pm, deaths, origRk, props, body_);
    // x-death loop bound: A(ttl)->DLX->A(ttl)->... would livelock (TTL re-fires
    // automatically, no client action). Persist the hop count in the record and
    // drop once it saturates.
    if (deaths >= AMQP_MAX_DEATHS)
        return;
    auto rk = meta.dlrk.length ? meta.dlrk : (origRk.length ? origRk : queue);
    // rebuild the record with deaths+1 (keeps publishMs so TTL still measures
    // from the ORIGINAL publish, RabbitMQ-style). The DLX fan-out's cross-shard
    // RPUSH YIELDS and blobc slices this buffer, so a concurrent deadLetter on
    // this thread (reaper vs consumer-burst/basic.get/nack) would clobber it and
    // every fan-out target AFTER the first would ship the wrong record. Mirror
    // finishPublish: the reentrant caller takes a fresh stack-local so the parked
    // fan-out keeps reading its own bytes.
    static ByteBuffer dlrecStatic; // TLS
    static bool dlrecBusy;
    ByteBuffer dlrecLocal;
    ByteBuffer* dlrec = &dlrecLocal;
    if (!dlrecBusy)
    {
        dlrecBusy = true;
        dlrec = &dlrecStatic;
    }
    scope (exit)
        if (dlrec is &dlrecStatic)
            dlrecBusy = false;
    // augment the props with an x-death header (count = deaths+1, this queue,
    // the original routing key). TLS buffers, consumed by buildRecord before the
    // routeTo yield below (like the queue-key buffers).
    static ByteBuffer xbuf; // TLS: the x-death header entry
    static ByteBuffer paug; // TLS: props + x-death
    xbuf.clear();
    static ByteBuffer xoth; // TLS: prior x-death entries for OTHER (queue,reason)
    xoth.clear();
    bool sawQueue, sawRejected;
    immutable prior = xDeathOthers(props, queue, reason, xoth, sawQueue, sawRejected);
    // pure-automatic cycle drop (RabbitMQ): a message dead-lettered AGAIN from
    // a queue it already died in, with no client rejection anywhere in its
    // history, is in a fully-automatic loop (TTL->DLX->...->same queue) and is
    // dropped. One "rejected" death anywhere (a client acted) keeps it alive.
    if (reason != "rejected" && sawQueue && !sawRejected)
        return;
    // a per-message `expiration` is REMOVED on dead-lettering (it must not
    // re-expire downstream) and preserved as original-expiration in the entry
    char[24] expBuf = void;
    const(char)[] origExp;
    immutable expMs = propsExpiration(props);
    if (expMs >= 0)
    {
        size_t ep = expBuf.length;
        long ev = expMs;
        do
        {
            expBuf[--ep] = cast(char)('0' + (ev % 10));
            ev /= 10;
        }
        while (ev > 0);
        origExp = expBuf[ep .. $];
    }
    buildXDeathEntry(xbuf, prior + 1, reason, queue, origRk, recordExchange(blob),
            headerArr(props, "CC"), origExp, xoth.data);
    // RabbitMQ 3.7+/3.10+ death-tracking headers, appended as sibling entries
    // (mergeXDeath splices xbuf verbatim into the table front and strips the
    // stale copies): x-first-death-* records the FIRST death and survives
    // every later hop; x-last-death-* always reflects THIS one.
    auto fdq = headerStr(props, "x-first-death-queue");
    auto fdr = headerStr(props, "x-first-death-reason");
    auto fdx = headerStr(props, "x-first-death-exchange");
    if (fdq.length == 0)
    {
        fdq = queue;
        fdr = reason;
        fdx = recordExchange(blob);
    }
    xtStr(xbuf, "x-first-death-queue", fdq);
    xtStr(xbuf, "x-first-death-reason", fdr);
    xtStr(xbuf, "x-first-death-exchange", fdx);
    xtStr(xbuf, "x-last-death-queue", queue);
    xtStr(xbuf, "x-last-death-reason", reason);
    xtStr(xbuf, "x-last-death-exchange", recordExchange(blob));
    mergeXDeath(paug, props, xbuf.data);
    dlrec.clear();
    buildRecord(*dlrec, pm, deaths + 1, origRk, paug.data, body_, meta.dlx);
    auto blobc = dlrec.data.asChars;
    routeTo(meta.dlx, rk, propsHeaders(paug.data), (scope const(char)[] q) nothrow {
        static ByteBuffer kb5; // TLS
        queueKeyPrio(q, recordPrio(q, cast(const(ubyte)[]) blobc), kb5);
        if (gAmqpPush !is null)
            gAmqpPush(kb5.data.asChars, blobc);
    });
}

// ---------------------------------------------------------------------------
// AMQP 1.0 skin shims (dreads.amqp10): the 1.0 module maps onto the SAME
// queue keyspace and routing walk — these package helpers expose exactly the
// two operations it needs without opening the module's internals.

/// Ensure `q` exists in the replicated existence set (1.0 attach-time
/// declare, equivalent to a 0-9-1 non-exclusive non-auto-delete declare).
package void a10EnsureQueue(scope const(char)[] q) nothrow @trusted
{
    try
        if (q.length && !queueExists(q))
        {
            char[1] fb = [cast(char) 0];
            ctlBroadcast(8, q, fb[], "");
        }
    catch (Exception)
    {
    }
}

/// Publish one already-encoded (0-9-1 props + body) message through the
/// routing walk. Returns the number of queues routed to.
package int a10Publish(scope const(char)[] exchange, scope const(char)[] rkey,
        scope const(ubyte)[] props, scope const(ubyte)[] body_) nothrow @trusted
{
    // `payload` slices `rec` and is re-read by the sink on EACH routed queue —
    // across gAmqpPush's cross-shard park. A bare TLS static would be clobbered by
    // a REENTRANT a10Publish (another 1.0 transfer on this thread) that runs during
    // the park and rewrites it, splicing its bytes into this publish's later
    // destinations. Mirror finishPublish: the first (non-reentrant) caller uses the
    // TLS static (zero-alloc); a reentrant caller takes a stack-local, leaving the
    // outer call's buffer — and its live `payload` slice — intact.
    static ByteBuffer recStatic; // TLS
    static bool recBusy;
    ByteBuffer recLocal;
    ByteBuffer* recp = &recLocal;
    if (!recBusy)
    {
        recBusy = true;
        recp = &recStatic;
    }
    scope (exit)
        if (recp is &recStatic)
            recBusy = false;
    recp.clear();
    buildRecord(*recp, cast(long) nowMs(), 0, rkey, props, body_, exchange);
    auto payload = recp.data.asChars;
    amqpCountPub();
    int routed = 0;
    string[16] seen;
    size_t ns = 0;
    scope void delegate(scope const(char)[]) nothrow sink = (scope const(char)[] q) nothrow {
        foreach (d; seen[0 .. ns])
            if (d == q)
                return;
        if (ns < seen.length)
            try
                seen[ns++] = q.idup; // RETAINED for dedup across the fanout: genuine copy
            catch (Exception)
            {
            }
        if (!queueExists(q))
            return;
        static ByteBuffer kb10; // TLS
        queueKeyPrio(q, recordPrio(q, cast(const(ubyte)[]) payload), kb10);
        if (gAmqpPush !is null)
            gAmqpPush(kb10.data.asChars, payload);
        routed++;
        enforceMaxLen(q);
    };
    routeTo(exchange, rkey, propsHeaders(props), sink);
    return routed;
}

/// 1.0 management shims: queue/exchange/binding topology ops for the
/// $management node (dreads.amqp10). All replicate through the SAME ctl ops
/// as 0-9-1 declares — one topology, two protocols.
package bool a10QueueExists(scope const(char)[] q) nothrow @trusted
{
    return queueExists(q);
}

package long a10QueueLen(scope const(char)[] q) nothrow @trusted
{
    if (gAmqpLen is null)
        return 0;
    immutable n = queueDepth(q); // sums every priority level
    return n < 0 ? 0 : n;
}

/// Non-destructive positional read for STREAM consumers (1.0 skin).
package bool a10PeekAt(scope const(char)[] q, long index, ref ByteBuffer outPayload) nothrow @trusted
{
    if (gAmqpPeekAt is null)
        return false;
    static ByteBuffer kbx; // TLS
    queueKey(q, kbx);
    char[8 + 256 + 4] ks = void;
    immutable kl = kbx.length <= ks.length ? kbx.length : ks.length;
    ks[0 .. kl] = cast(const(char)[]) kbx.data[0 .. kl];
    outPayload.clear();
    return gAmqpPeekAt(cast(const(char)[]) ks[0 .. kl], index, outPayload);
}

package ubyte a10QueueFlags(scope const(char)[] q) nothrow @trusted
{
    try
        if (auto pf = (cast(string) q) in gQueueFlags)
            return *pf;
    catch (Exception)
    {
    }
    return 0;
}

/// Declare with 0-9-1-style flags (durable=2|exclusive=4|auto-delete=8) and
/// the known x-args. Returns false when the name already existed.
package bool a10DeclareQueue(scope const(char)[] q, ubyte flags,
        bool ttlSet, long ttlMs, bool expSet, long expMs,
        bool mlSet, long maxLen, scope const(char)[] dlx, bool dlxSet,
        scope const(char)[] dlrk) nothrow @trusted
{
    immutable existed = queueExists(q);
    try
    {
        if (!existed)
        {
            char[1] fb = [cast(char)(flags & 0x0E)];
            ctlBroadcast(8, q, fb[], "");
        }
        if (dlxSet || dlrk.length || ttlSet || mlSet || expSet)
        {
            ubyte[25] tb = void;
            foreach (k; 0 .. 8)
                tb[k] = cast(ubyte)(ttlMs >> ((7 - k) * 8));
            immutable mlEnc = mlSet ? maxLen + 1 : 0;
            foreach (k; 0 .. 8)
                tb[8 + k] = cast(ubyte)(mlEnc >> ((7 - k) * 8));
            tb[16] = cast(ubyte)((dlxSet ? 1 : 0) | (dlrk.length ? 2 : 0)
                    | (ttlSet ? 4 : 0) | (expSet ? 8 : 0));
            foreach (k; 0 .. 8)
                tb[17 + k] = cast(ubyte)((expSet ? expMs : 0) >> ((7 - k) * 8));
            ctlBroadcast(3, q, dlxSet ? dlx : "", dlrk, tb[]);
        }
    }
    catch (Exception)
    {
    }
    return !existed;
}

package void a10DeleteQueue(scope const(char)[] q) nothrow @trusted
{
    try
    {
        auto adSeeds = bindingSourcesTo(q, false);
        ctlBroadcast(9, q, "", "");
        if (gAmqpDelKey !is null)
            queueEachLevel(q, (scope const(char)[] key) nothrow {
                gAmqpDelKey(key); // every priority level, not just level 0
            });
        autoDeleteExchangeSweep(adSeeds);
    }
    catch (Exception)
    {
    }
}

package void a10PurgeQueue(scope const(char)[] q) nothrow @trusted
{
    if (gAmqpDelKey !is null)
        queueEachLevel(q, (scope const(char)[] key) nothrow {
            gAmqpDelKey(key); // every priority level
        });
}

/// Snapshot the stored queue meta for 1.0 redeclare-equivalence (409).
package void a10QueueMetaGet(scope const(char)[] q, out bool ttlSet,
        out long ttlMs, out bool expSet, out long expMs, out long maxLenEnc,
        out bool dlxSet, out const(char)[] dlx, out const(char)[] dlrk) nothrow @trusted
{
    try
        if (auto m = (cast(string) q) in gQueueMeta)
        {
            ttlSet = m.ttlSet;
            ttlMs = m.ttlMs;
            expSet = m.expSet;
            expMs = m.expMs;
            maxLenEnc = m.maxLen;
            dlxSet = m.dlxSet;
            dlx = m.dlx;
            dlrk = m.dlrk;
        }
    catch (Exception)
    {
    }
}

/// The stored exchange type name ("" when unknown).
package const(char)[] a10ExchangeType(scope const(char)[] x) nothrow @trusted
{
    try
        if (auto t = (cast(string) x) in gExchanges)
            final switch (*t)
            {
            case ExType.direct:
                return "direct";
            case ExType.fanout:
                return "fanout";
            case ExType.topic:
                return "topic";
            case ExType.headers:
                return "headers";
            }
    catch (Exception)
    {
    }
    return "";
}

/// Replicated live-consumer count for a queue (0-9-1 + 1.0 consumers both
/// feed op-12/13).
package uint a10ConsumerCount(scope const(char)[] q) nothrow @trusted
{
    try
        if (auto p2 = (cast(string) q) in gQueueConsGlobal)
            return cast(uint)(*p2 < 0 ? 0 : *p2);
    catch (Exception)
    {
    }
    return 0;
}

/// 1.0 receiver links register as consumers (op-12/13): x-expires "in use"
/// and queue-info consumer_count both see them.
package void a10ConsumerInc(scope const(char)[] q) nothrow @trusted
{
    try
        ctlBroadcast(12, q, "", "");
    catch (Exception)
    {
    }
}

package void a10ConsumerDec(scope const(char)[] q) nothrow @trusted
{
    try
        ctlBroadcast(13, q, "", "");
    catch (Exception)
    {
    }
}

/// 1.0 exclusivity: mint a conn id, claim a queue, test ownership.
package ulong a10NewConnId() nothrow @trusted
{
    return atomicOp!"+="(gAmqpConnGen, 1);
}

package void a10ClaimExclusive(scope const(char)[] q, ulong connId) nothrow @trusted
{
    try
    {
        char[24] idb = void;
        size_t idl = 0;
        ulong v = connId;
        char[24] tmp = void;
        size_t tn = 0;
        do
        {
            tmp[tn++] = cast(char)('0' + v % 10);
            v /= 10;
        }
        while (v);
        while (tn)
            idb[idl++] = tmp[--tn];
        ctlBroadcast(10, q, idb[0 .. idl], "");
    }
    catch (Exception)
    {
    }
}

/// 0 = unowned; otherwise the owning conn id.
package ulong a10ExclusiveOwner(scope const(char)[] q) nothrow @trusted
{
    try
        if (auto po = (cast(string) q) in gQueueOwner)
            return *po;
    catch (Exception)
    {
    }
    return 0;
}

/// Is the queue at (or beyond) its x-max-length bound?
package bool a10QueueFull(scope const(char)[] q) nothrow @trusted
{
    try
        if (auto m = (cast(string) q) in gQueueMeta)
            if (m.maxLen > 0) // +1-encoded: bound = maxLen-1
                return a10QueueLen(q) >= m.maxLen - 1;
    catch (Exception)
    {
    }
    return false;
}

/// 1.0 consumer x-priority registration (shared registry with 0-9-1).
package void a10PrioAdd(scope const(char)[] q, int prio) nothrow @trusted
{
    try
        qPrioAdd(cast(string) q.idup, prio);
    catch (Exception)
    {
    }
}

package void a10PrioRemove(scope const(char)[] q, int prio) nothrow @trusted
{
    try
        qPrioRemove(cast(string) q.idup, prio);
    catch (Exception)
    {
    }
}

package int a10PrioMax(scope const(char)[] q) nothrow @trusted
{
    try
        return qPrioMax(cast(string) q.idup);
    catch (Exception)
        return int.min;
}

package bool a10ExchangeExists(scope const(char)[] x) nothrow @trusted
{
    try
        return ((cast(string) x) in gExchanges) !is null;
    catch (Exception)
        return false;
}

package void a10DeclareExchange(scope const(char)[] x, scope const(char)[] typ,
        ubyte flags, scope const(char)[] ae) nothrow @trusted
{
    try
    {
        char[1] xfb = [cast(char)(flags & 0x0E)];
        ctlBroadcast(1, x, typ, xfb[], cast(const(ubyte)[]) ae);
    }
    catch (Exception)
    {
    }
}

package void a10DeleteExchange(scope const(char)[] x) nothrow @trusted
{
    try
    {
        auto adSeeds = bindingSourcesTo(x, true);
        ctlBroadcast(5, x, "", "");
        autoDeleteExchangeSweep(adSeeds);
    }
    catch (Exception)
    {
    }
}

package void a10Bind(scope const(char)[] source, scope const(char)[] dest,
        scope const(char)[] key, bool destIsExchange,
        scope const(ubyte)[] args = null) nothrow @trusted
{
    try
        if (destIsExchange)
            ctlBroadcast(6, source, dest, key, args);
        else
            ctlBroadcast(2, source, dest, key, args);
    catch (Exception)
    {
    }
}

/// Enumerate LIVE bindings from `source` to `dest` (queue or exchange) —
/// the 1.0 management GET /bindings listing. Sink gets (key, rawArgsTable).
package void a10ListBindings(scope const(char)[] source, scope const(char)[] dest,
        bool destIsExchange,
        scope void delegate(scope const(char)[] key, scope const(ubyte)[] args) nothrow sink) nothrow @trusted
{
    try
        if (auto bl = (cast(string) source) in gBindings)
            foreach (ref bd; *bl)
                if (bd.alive && bd.toExchange == destIsExchange && bd.queue == dest)
                    sink(bd.key, bd.args);
    catch (Exception)
    {
    }
}

package void a10Unbind(scope const(char)[] source, scope const(char)[] dest,
        scope const(char)[] key, bool destIsExchange) nothrow @trusted
{
    try
    {
        if (destIsExchange)
            ctlBroadcast(7, source, dest, key);
        else
            ctlBroadcast(4, source, dest, key);
        autoDeleteExchangeSweep([cast(string) source.idup]);
    }
    catch (Exception)
    {
    }
}

/// Pop one deliverable record from `q` (expired heads are dead-lettered and
/// skipped, bounded). False = empty.
package bool a10Pop(scope const(char)[] q, ref ByteBuffer outPayload) nothrow @trusted
{
    if (gAmqpPop is null)
        return false;
    static ByteBuffer kbp; // TLS
    cast(void) queueKeyRead(q, kbp); // highest non-empty priority level
    char[8 + 256 + 4] ks = void;
    immutable kl = kbp.length <= ks.length ? kbp.length : ks.length;
    ks[0 .. kl] = cast(const(char)[]) kbp.data[0 .. kl];
    auto key = cast(const(char)[]) ks[0 .. kl];
    int guard = 0;
    while (guard++ < 64)
    {
        outPayload.clear();
        if (!gAmqpPop(key, outPayload))
            return false;
        immutable qttl = queueTtl(q);
        if (isExpired(outPayload.data, qttl)
                && !(effectiveTtl(outPayload.data, qttl) == 0
                    && !recordRedelivered(outPayload.data)))
        {
            deadLetter(q, outPayload.data, "expired");
            continue;
        }
        return true;
    }
    return false;
}

/// Requeue a popped record to the queue FRONT (marked redelivered); dropped
/// when the queue no longer exists. x-max-length holds.
package void a10Requeue(scope const(char)[] q, scope const(ubyte)[] blob) nothrow @trusted
{
    if (!queueExists(q))
        return;
    static ByteBuffer kbr; // TLS
    queueKeyPrio(q, recordPrio(q, blob), kbr); // back to its OWN level
    static ByteBuffer rqr; // TLS
    markRedelivered(rqr, blob);
    if (gAmqpPushFront !is null)
        gAmqpPushFront(kbr.data.asChars, rqr.data.asChars);
    try
        enforceMaxLen(q.idup);
    catch (Exception)
    {
    }
}

/// Requeue with EXTRA header-table entries spliced in (1.0 modified-state
/// annotations land in the 0-9-1 headers table; x-* keys re-emit as
/// annotations on the next 1.0 delivery). Prepend-wins on key collision.
package void a10RequeueAnn(scope const(char)[] q, scope const(ubyte)[] blob,
        scope const(ubyte)[] extraTbl, bool requeue = true) nothrow @trusted
{
    if (requeue && !queueExists(q))
        return;
    long pm;
    int deaths;
    const(char)[] rk;
    const(ubyte)[] props, body_;
    splitRecord(blob, pm, deaths, rk, props, body_);
    // rebuild props with headers = extra + existing (mergeXDeath-style)
    static ByteBuffer np; // TLS: consumed by buildRecord below (no yield)
    np.clear();
    ushort flags = 0;
    size_t i = 2;
    if (props.length >= 2)
        flags = cast(ushort)((props[0] << 8) | props[1]);
    immutable nf2 = cast(ushort)(flags | 0x2000);
    np.appendByte(cast(char)(nf2 >> 8));
    np.appendByte(cast(char)(nf2 & 0xFF));
    static bool cpShort(ref ByteBuffer d2, scope const(ubyte)[] pp, ref size_t j) @nogc nothrow
    {
        if (j >= pp.length || j + 1 + pp[j] > pp.length)
            return false;
        immutable seg = 1 + pp[j];
        d2.append(cast(const(char)[]) pp[j .. j + seg]);
        j += seg;
        return true;
    }

    if (flags & 0x8000)
        if (!cpShort(np, props, i))
            return;
    if (flags & 0x4000)
        if (!cpShort(np, props, i))
            return;
    const(ubyte)[] existing;
    if (flags & 0x2000)
    {
        if (i + 4 > props.length)
            return;
        immutable hl = (cast(size_t) props[i] << 24) | (cast(size_t) props[i + 1] << 16)
            | (cast(size_t) props[i + 2] << 8) | props[i + 3];
        i += 4;
        if (i + hl > props.length)
            return;
        existing = props[i .. i + hl];
        i += hl;
    }
    putU32(np, cast(uint)(extraTbl.length + existing.length));
    np.append(cast(const(char)[]) extraTbl);
    np.append(cast(const(char)[]) existing);
    np.append(cast(const(char)[]) props[i .. $]);
    static ByteBuffer nrec; // TLS
    nrec.clear();
    buildRecord(nrec, pm, deaths, rk, cast(const(ubyte)[]) np.data, body_, recordExchange(blob));
    if (!requeue)
    {
        // modified + undeliverable-here: dead-letter WITH the annotations
        deadLetter(q, cast(const(ubyte)[]) nrec.data, "rejected");
        return;
    }
    static ByteBuffer kra; // TLS
    queueKeyPrio(q, recordPrio(q, cast(const(ubyte)[]) nrec.data), kra);
    static ByteBuffer rqa; // TLS
    markRedelivered(rqa, cast(const(ubyte)[]) nrec.data);
    if (gAmqpPushFront !is null)
        gAmqpPushFront(kra.data.asChars, rqa.data.asChars);
    try
        enforceMaxLen(q.idup);
    catch (Exception)
    {
    }
}

/// Dead-letter (or drop) a client-rejected record.
package void a10Reject(scope const(char)[] q, scope const(ubyte)[] blob) nothrow @trusted
{
    deadLetter(q, blob, "rejected");
}

/// ACTIVE x-message-ttl expiry (called ~1/s per shard from the maintenance
/// tick). Lazy expiry only fires when a queue is READ; a message that outlives
/// its TTL while sitting in an unconsumed queue would linger forever and never
/// dead-letter (a real gap vs RabbitMQ). This proactively expires the heads of
/// every TTL queue THIS shard owns: peek the head, and while it is expired,
/// pop + dead-letter it (bounded per queue per tick). Ownership-gated so peek
/// and pop are self-shard and yield-free between them.
public void amqpTtlSweep() nothrow @trusted
{
    if (gAmqpPeekHead is null || gAmqpOwns is null || gAmqpPop is null)
        return;
    try
    {
        static ByteBuffer kb; // TLS
        static ByteBuffer head; // TLS
        static ByteBuffer popped; // TLS
        // SNAPSHOT the TTL queue names first (yield-free): the processing loop
        // below calls deadLetter, whose cross-shard DLX push YIELDS, and
        // gQueueMeta is mutated by every AMQP fiber's queue.declare on this
        // thread — holding its foreach iterator across that yield would be a
        // use-after-invalidation (a concurrent declare rehashes the AA).
        static string[] ttlQ; // TLS scratch
        size_t nq = 0;
        foreach (q, ref meta; gQueueMeta)
        {
            if (!meta.ttlSet && !meta.expSet)
                continue;
            if (ttlQ.length <= nq)
                ttlQ.length = nq + 8;
            ttlQ[nq++] = q;
        }
        foreach (qi; 0 .. nq)
        {
            auto q = ttlQ[qi];
            // re-read the meta (a declare during a prior queue's yield may have
            // changed it; a delete leaves it absent -> skip)
            auto mp = q in gQueueMeta;
            if (mp is null || (!mp.ttlSet && !mp.expSet))
                continue;
            immutable ttl = mp.ttlMs;
            immutable hasTtl = mp.ttlSet;
            immutable hasExp = mp.expSet;
            immutable expLease = mp.expMs;
            // COPY the key to the stack: deadLetter() below yields (cross-shard
            // DLX push), and the TLS `kb` would be clobbered by a concurrent
            // fiber's queueKey during that park -> the next peek/pop would hit a
            // DIFFERENT queue. The stack copy survives the yield.
            cast(void) queueKeyRead(q, kb); // highest non-empty priority level
            char[8 + 256 + 4] keyStore = void; // "amq.q." + queue name
            immutable klen = kb.length <= keyStore.length ? kb.length : keyStore.length;
            keyStore[0 .. klen] = cast(const(char)[]) kb.data[0 .. klen];
            auto key = cast(const(char)[]) keyStore[0 .. klen];
            if (!gAmqpOwns(key))
                continue; // only the list's owner reaps it
            // x-expires: the owner deletes an UNUSED queue whose lease lapsed.
            // "Used" = any live consumer anywhere (replicated op-12/13 counts);
            // declares and basic.gets re-arm the lease via op-3/op-11.
            if (hasExp)
            {
                long deadline;
                if (auto pl = q in gQueueLease)
                    deadline = *pl;
                else
                    gQueueLease[q] = deadline = cast(long) nowMs() + expLease;
                if ((q in gQueueConsGlobal) is null && cast(long) nowMs() > deadline)
                {
                    auto adSeeds = bindingSourcesTo(q, false);
                    ctlBroadcast(9, q, "", ""); // exactly like queue.delete
                    if (gAmqpDelKey !is null)
                        gAmqpDelKey(key);
                    autoDeleteExchangeSweep(adSeeds);
                    continue;
                }
            }
            if (!hasTtl)
                continue;
            int reaped = 0;
            while (reaped < 4096) // bound the work per queue per tick
            {
                head.clear();
                if (!gAmqpPeekHead(key, head))
                    break; // empty queue
                if (!isExpired(head.data, ttl))
                    break; // head is fresh -> everything behind it is younger
                if (effectiveTtl(head.data, ttl) == 0 && !recordRedelivered(head.data)
                        && (q in gQueueConsGlobal) !is null)
                    break; // 0-TTL head with an ACTIVE consumer: it gets delivered
                popped.clear();
                if (!gAmqpPop(key, popped))
                    break;
                // defensive: if a consumer raced between peek and pop and the
                // popped record is NOT actually expired, put it back and stop
                // (peek+pop are yield-free self-shard, so this should not fire)
                if (!isExpired(popped.data, ttl))
                {
                    if (gAmqpPushFront !is null)
                        gAmqpPushFront(key, popped.data.asChars);
                    break;
                }
                deadLetter(q, popped.data, "expired"); // DLX route or drop (no DLX)
                reaped++;
            }
        }
    }
    catch (Exception)
    {
    }
}

// Consumer: a fiber that drains the queue to this connection. v1 POLLS the
// data plane (1ms backoff when empty) — under load it never sleeps; a parked
// wake integration (the BLPOP machinery) is the v2 upgrade.
/// Per-queue live-consumer count (shard-local TLS), used only by the
/// queue.declare-ok consumer_count field. Both run on the shard thread.
private void qConsumerInc(string q) nothrow @trusted
{
    try
    {
        gQueueConsumers[q] = (q in gQueueConsumers ? gQueueConsumers[q] : 0u) + 1;
        ctlBroadcast(12, q, "", ""); // replicated count (x-expires "in use")
    }
    catch (Exception)
    {
    }
}

private void qConsumerDec(string q) nothrow @trusted
{
    try
        ctlBroadcast(13, q, "", ""); // replicated count (a zero re-arms x-expires)
    catch (Exception)
    {
    }
    bool last = false;
    if (auto p = q in gQueueConsumers)
    {
        if (*p > 0)
            --*p;
        if (*p == 0)
        {
            gQueueConsumers.remove(q);
            last = true;
        }
    }
    if (!last)
        return;
    // auto-delete: the queue dies when its LAST consumer goes away (it had
    // at least one — this decrement proves it). Runs on the consumer fiber,
    // where the tombstone broadcast + backing DEL may yield freely.
    if (auto pf = q in gQueueFlags)
        if (*pf & 0x08)
        {
            auto adSeeds = bindingSourcesTo(q, false);
            ctlBroadcast(9, q, "", "");
            static ByteBuffer adk; // TLS: consumed by the DEL before any yield
            queueKey(q, adk);
            if (gAmqpDelKey !is null)
                gAmqpDelKey(adk.data.asChars);
            autoDeleteExchangeSweep(adSeeds); // sources that lost this queue's bindings
        }
}

private void startConsumer(AmqpConn c, ushort chan, scope const(char)[] q,
        scope const(char)[] tag, bool noAck, int prio = 0) nothrow
{
    string qs, ts;
    try
    {
        qs = q.idup;
        ts = tag.idup;
    }
    catch (Exception)
        return;
    uint myGen; // the channel generation this consumer belongs to (channel exists
    if (auto pch = chan in c.chans) // now — basic.consume is on an open channel)
        myGen = pch.openGen;
    atomicOp!"+="(gAmqpConsumers, 1);
    c.consumerCount++;
    qConsumerInc(qs); // live consumer_count for queue.declare-ok
    qPrioAdd(qs, prio); // x-priority dispatch preference
    try
        if (auto rch0 = chan in c.chans)
            rch0.rrOrder ~= ts; // global-window round-robin order
    catch (Exception)
    {
    }
    try
    {
        immutable ck0 = (cast(ulong) chan) << 32 | myGen;
        c.chanConsumers[ck0] = (ck0 in c.chanConsumers ? c.chanConsumers[ck0] : 0u) + 1;
    }
    catch (Exception)
    {
    }
    try
        cast(void) runTask((AmqpConn cc, ushort chn, string qq, string tt, bool na, uint mg, int myPrio) nothrow {
            auto cu = cuNew(); // this consumer's unacked counter, by pointer
            scope (exit)
            {
                // hand the cell over: it is reaped here if nothing is still
                // outstanding, otherwise by whichever settle finishes last.
                cuGone(cu);
                atomicOp!"-="(gAmqpConsumers, 1);
                if (cc.consumerCount > 0)
                    cc.consumerCount--;
                qPrioRemove(qq, myPrio);
                qConsumerDec(qq);
                // drop our own cancel marker so a healthy connection's
                // cancelledTags doesn't accumulate one dead entry per
                // consume/cancel cycle (it's only needed until we've seen it).
                // AA.remove on a string key is nothrow — no try/catch (which a
                // scope(exit) can't contain anyway).
                cc.cancelledTags.remove(tt);
                immutable ckx = (cast(ulong) chn) << 32 | mg;
                if (auto pcn = ckx in cc.chanConsumers)
                {
                    if (*pcn > 1)
                        --*pcn;
                    else
                        cc.chanConsumers.remove(ckx);
                }
                rrOrderRemove(cc, chn, tt);
            }
            ByteBuffer kb;
            kb.append("amq.q.");
            kb.append(qq);
            immutable cMaxPrio = queueMaxPrio(qq); // 0 = plain FIFO, unchanged path
            ByteBuffer pay;
            ByteBuffer ob;
            // BATCH prefetch: one `LPOP key 64` per burst instead of 64 pops.
            // Each gAmqpPop is a cross-shard round-trip when the queue lives on
            // another shard, so the old burst paid 64 hops — the crosstalk that
            // inverted the scaling curve. FIBER-LOCAL (not TLS): the burst
            // yields inside (deadLetter), and a shared static would be
            // clobbered by a sibling consumer mid-drain.
            ByteBuffer batch;
            size_t batchPos = 0;
            int batchLeft = 0;
            // The fiber has five exits (cancel, channel closed, channel
            // reopened, queue deleted, conn closing) plus the loop falling
            // through; none of them can be allowed to drop a prefetched tail.
            scope (exit)
                returnBatchRemainder(kb.data.asChars, batch, batchPos, batchLeft);
            // runTask runs this fiber SYNCHRONOUSLY up to its first yield — a
            // same-shard pop + sendTo here would put the first delivery on the
            // wire BEFORE the consume-ok still staged in the handler's reply
            // buffer ("Unsolicited delivery": the client kills the connection
            // on a delivery for a tag it hasn't confirmed). Park until the
            // serve loop FLUSHES the batch that staged our consume-ok — a
            // fixed 1ms was outlived by long pipelined batches (multi-threaded
            // clients + cross-shard publishes). Bounded: a nowait consume may
            // never flush, so give up parking after ~50 ticks.
            {
                immutable wantFlush = cc.flushSeq + 1;
                int parked = 0;
                while (cc.flushSeq < wantFlush && parked++ < 50)
                {
                    try
                        sleep(1.msecs);
                    catch (Exception)
                        return;
                }
            }
            int graceTicks = 0; // bounded fairness deferral (global qos)
            while (!cc.closing)
            {
                try
                {
                    if (tt in cc.cancelledTags)
                        return;
                    // channel gone (closed) or REOPENED under a new generation
                    // (reused number): this fiber is the OLD instance -> exit. A
                    // fresh consumer on the reused number has the new gen and stays.
                    auto pch = chn in cc.chans;
                    if (pch is null || pch.openGen != mg)
                        return;
                    // Consumer Cancel Notification: our queue was deleted (the
                    // op-9 tombstone dropped it from the existence set on every
                    // shard). Tell the client its consumer is gone (server-sent
                    // basic.cancel) and exit, exactly like RabbitMQ. The queue
                    // was present when the consumer started (basic.consume 404s
                    // an unknown queue), so this fires only on a real delete.
                    if (!queueExists(qq))
                    {
                        ByteBuffer cbuf;
                        method(cbuf, chn, 60, 30, (ref ByteBuffer b) @nogc nothrow {
                            putShortStr(b, tt);
                            // no-wait = 1, like RabbitMQ: a server-initiated
                            // cancel expects NO cancel-ok, and the Erlang
                            // client's selective consumer CRASHES on nowait=0
                            // (pika merely tolerated it).
                            b.appendByte(1);
                        });
                        sendTo(cc, cbuf.data);
                        return;
                    }
                }
                catch (Exception)
                {
                }
                // prefetch window: a no-ack=false consumer that stops acking
                // must not drain the whole queue into `unacked` (RAM DoS). Once
                // the window is full, back off until acks drain it. The window
                // is CHANNEL-scoped (basic.qos semantics, iso RabbitMQ); the
                // conn-level prefetch is the fallback, bytes stay conn-global.
                uint limit = AMQP_DEFAULT_PREFETCH;
                uint chanN = 0;
                bool windowFull = false;
                bool perConsumer = false;
                try
                {
                    auto wch = chn in cc.chans; // refetched: prior loop yielded
                    if (wch !is null)
                        chanN = wch.unackedN;
                    immutable pf = wch !is null && wch.prefetch ? wch.prefetch : cc.prefetch;
                    if (pf)
                        limit = cast(uint) pf; // qos values are u16; never truncates
                    // qos global=false (the wire default) is a PER-CONSUMER
                    // window: count only THIS consumer's unacked deliveries
                    perConsumer = wch !is null && wch.prefetch && !wch.prefetchGlobal;
                    if (perConsumer)
                        chanN = cu !is null ? cu.count : 0u; // no lookup at all
                    // a GLOBAL window gates no-ack consumers too: their
                    // deliveries don't ADD to the window, but they must wait
                    // while it is full (noAckObeysLimit pins this)
                    immutable gatesNoAck = wch !is null && wch.prefetch && wch.prefetchGlobal;
                    windowFull = (!na || gatesNoAck) && (chanN >= limit
                            || cc.unackedBytes >= AMQP_MAX_UNACKED_BYTES);
                    // fairness: under a global window the DESIGNATED consumer
                    // (round-robin over the channel's live consumers) gets
                    // first crack at each freed slot; everyone else waits a
                    // bounded grace so an empty designated queue can't stall
                    // the channel.
                    if (!windowFull && gatesNoAck && wch.rrOrder.length > 1)
                    {
                        auto designated = wch.rrOrder[wch.rrNext % wch.rrOrder.length];
                        if (designated != tt && graceTicks < 2)
                        {
                            graceTicks++;
                            windowFull = true; // one 1ms back-off tick
                        }
                    }
                }
                catch (Exception)
                {
                }
                if (windowFull)
                {
                    // FLUSH BEFORE PARKING. The burst breaks on a full window
                    // with burst > 0, so the queue-dry flush below is skipped
                    // and `ob` would sit here until it reached the byte cap --
                    // which it never does, because the window only reopens when
                    // the client acks what is still in `ob`. That deadlocks
                    // every consumer that sets basic.qos.
                    if (ob.length)
                    {
                        sendTo(cc, ob.data);
                        ob.clear();
                    }
                    try
                        sleep(1.msecs);
                    catch (Exception)
                        return;
                    continue;
                }
                // x-priority: while a HIGHER-priority consumer is live on this
                // queue, lower ones idle (RabbitMQ dispatch preference)
                if (myPrio < qPrioMax(qq))
                {
                    if (ob.length) // same rule: never park holding deliveries
                    {
                        sendTo(cc, ob.data);
                        ob.clear();
                    }
                    try
                        sleep(1.msecs);
                    catch (Exception)
                        return;
                    continue;
                }
                // BURST drain. `ob` is NOT cleared per burst any more: we keep
                // stacking frames across consecutive non-empty bursts and only
                // hit the socket when the queue runs dry (or the buffer hits the
                // cap). That is LavinMQ's shape — it flushes right before it
                // blocks, never per batch — and it turns a deep backlog into a
                // few large writes instead of one write per 64 messages.
                int burst = 0;
                while (burst < 64)
                {
                    if (!na && (chanN >= limit
                            || cc.unackedBytes >= AMQP_MAX_UNACKED_BYTES))
                        break; // window filled mid-burst (count OR bytes)
                    pay.clear();
                    // refill from ONE hop when the local batch runs dry
                    if (batchLeft == 0)
                    {
                        batchPos = 0;
                        // x-max-priority: serve the highest non-empty level.
                        // Re-picked per refill, not once per fiber: a level that
                        // was empty when this consumer started may be the one to
                        // drain now.
                        if (cMaxPrio > 0)
                            cast(void) queueKeyRead(qq, kb);
                        if (gAmqpPopN !is null)
                            batchLeft = gAmqpPopN(kb.data.asChars, 64, batch);
                        else if (gAmqpPop !is null && gAmqpPop(kb.data.asChars, pay))
                        {
                            batchLeft = -1; // legacy single-pop already in `pay`
                        }
                        if (batchLeft == 0)
                            break; // queue empty
                    }
                    if (batchLeft > 0)
                    {
                        // next `$len\r\n<payload>\r\n` element of the batch
                        auto bd = batch.data;
                        if (batchPos + 2 > bd.length || bd[batchPos] != '$')
                        {
                            batchLeft = 0;
                            break;
                        }
                        size_t bi = batchPos + 1;
                        bool neg = bi < bd.length && bd[bi] == '-';
                        size_t blen = 0;
                        if (neg)
                            bi++;
                        while (bi < bd.length && bd[bi] != '\r')
                        {
                            blen = blen * 10 + (bd[bi] - '0');
                            bi++;
                        }
                        bi += 2; // skip \r\n
                        if (neg)
                        {
                            batchPos = bi;
                            batchLeft--;
                            continue; // nil element: skip, don't deliver
                        }
                        if (bi + blen > bd.length)
                        {
                            batchLeft = 0;
                            break;
                        }
                        pay.append(bd[bi .. bi + blen]);
                        batchPos = bi + blen + 2; // payload + \r\n
                        batchLeft--;
                    }
                    else
                        batchLeft = 0; // legacy path: `pay` already filled
                    // x-message-ttl: an expired head is dead-lettered (or
                    // dropped), never delivered. Count it toward the burst so a
                    // backlog of expired heads can't drain unboundedly in one
                    // pass (it yields between bursts). EXCEPT: TTL 0 means
                    // "expire unless deliverable IMMEDIATELY" — a fresh (never
                    // redelivered) message reaching an active consumer is
                    // delivered; only its requeued copy expires
                    // (zeroTTLDelivery pins both halves).
                    immutable qttl = queueTtl(qq);
                    if (isExpired(pay.data, qttl)
                            && !(effectiveTtl(pay.data, qttl) == 0
                                && !recordRedelivered(pay.data)))
                    {
                        deadLetter(qq, pay.data, "expired");
                        burst++;
                        continue;
                    }
                    // the pop's cross-shard hop yielded; if the channel/conn was
                    // torn down during that park, put the record back at the
                    // FRONT and stop rather than deliver on a dead channel
                    bool gone = cc.closing;
                    try
                    {
                        auto pc2 = chn in cc.chans;
                        gone = gone || pc2 is null || pc2.openGen != mg;
                        // basic.cancel landed during the pop's yield: stop NOW.
                        // The cancel handler holds its cancel-ok until this
                        // fiber exits, so no delivery can trail the cancel-ok
                        // ("Unsolicited delivery" kills the java client).
                        gone = gone || (tt in cc.cancelledTags) !is null;
                    }
                    catch (Exception)
                    {
                    }
                    if (gone)
                    {
                        // tail first, then THIS record on top of it: pushes go
                        // to the head, so the last push ends up frontmost.
                        returnBatchRemainder(kb.data.asChars, batch, batchPos, batchLeft);
                        if (gAmqpPushFront !is null)
                            gAmqpPushFront(kb.data.asChars, pay.data.asChars);
                        break;
                    }
                    immutable tg = cc.nextTag++;
                    try
                        if (auto tch2 = chn in cc.chans)
                            tch2.lastTag = tg; // multiple-settle window bound
                    catch (Exception)
                    {
                    }
                    if (!na)
                        try
                        {
                            cc.unacked[tg] = Unacked(qq, dupBlob(pay.data), chn, 0, false, cu, tt);
                            cc.unackedBytes += pay.data.length;
                            // fresh lookup (the pop yielded; AA may have moved)
                            if (auto uch = chn in cc.chans)
                                uch.unackedN++;
                            if (cu !is null)
                                cu.count++; // the record released it on settle
                            chanN++; // keep the burst-local window in step
                        }
                        catch (Exception)
                        {
                        }
                    immutable redlv = recordRedelivered(pay.data);
                    auto drk = recordRoutingKey(pay.data);
                    auto dex = recordExchange(pay.data);
                    method(ob, chn, 60, 60, (ref ByteBuffer b) @nogc nothrow {
                        putShortStr(b, tt);
                        putU64(b, tg);
                        b.appendByte(redlv ? 1 : 0); // redelivered
                        putShortStr(b, dex); // original exchange
                        putShortStr(b, drk); // original routing key, not the queue name
                    });
                    emitContent(ob, chn, pay.data, cc.frameMax);
                    burst++;
                }
                if (burst == 0)
                {
                    // queue is dry: THIS is the moment to hit the socket, then
                    // idle. Everything stacked since the last flush goes out in
                    // one write.
                    if (ob.length)
                    {
                        sendTo(cc, ob.data);
                        ob.clear();
                    }
                    try
                        sleep(1.msecs);
                    catch (Exception)
                        return;
                    continue;
                }
                try
                    if (auto sch = chn in cc.chans)
                        if (sch.prefetch && sch.prefetchGlobal)
                        {
                            sch.lastServed = tt;
                            // WE delivered: if we were the designated consumer,
                            // pass the turn on; either way our grace resets
                            if (sch.rrOrder.length
                                    && sch.rrOrder[sch.rrNext % sch.rrOrder.length] == tt)
                                sch.rrNext++;
                            graceTicks = 0;
                        }
                catch (Exception)
                {
                }
                // Still messages flowing: keep stacking. Flush only to bound
                // the buffer (and so a very deep backlog still streams out
                // instead of ballooning in RAM). Yield on BYTES delivered, not
                // per burst, so the loop stays tight while the queue is hot.
                if (ob.length >= AMQP_CONSUMER_FLUSH_BYTES)
                {
                    sendTo(cc, ob.data);
                    ob.clear();
                }
            }
        }, c, chan, qs, ts, noAck, myGen, prio);
    catch (Exception)
    {
        atomicOp!"-="(gAmqpConsumers, 1);
        if (c.consumerCount > 0)
            c.consumerCount--;
        try
        {
            immutable cky = (cast(ulong) chan) << 32 | myGen;
            if (auto pcn = cky in c.chanConsumers)
            {
                if (*pcn > 1)
                    --*pcn;
                else
                    c.chanConsumers.remove(cky);
            }
        }
        catch (Exception)
        {
        }
        qConsumerDec(qs); // the fiber never ran; undo the pre-increment
    }
}

// Heartbeat sender: a fiber emitting a heartbeat frame every 15s while the
// connection lives (half the negotiated 30s interval). Read-side liveness
// stays TCP-level in v1 (the serve loop notices the close).
/// Monotonic milliseconds (never the frozen per-command gClock).
private long monoMs() nothrow @trusted
{
    import core.time : MonoTime;

    try
        return MonoTime.currTime.ticks / (MonoTime.ticksPerSecond / 1000);
    catch (Exception)
        return 0;
}

// Management API (M4 v2): a per-connection fiber on the conn's OWN thread that
// polls the cross-thread kill flag and closes the socket when set — a
// cross-thread tcp.close is unsafe in vibe-core, so the mgmt thread only flags
// and this fiber (co-located with the serve loop) performs the close. Idle
// cost: one 1s sleep; ends as soon as the connection closes for any reason.
private void startKillWatcher(AmqpConn c) nothrow
{
    import core.time : seconds;
    import core.atomic : atomicLoad;

    try
        cast(void) runTask((AmqpConn cc) nothrow {
            while (!cc.closing)
            {
                try
                    sleep(1.seconds);
                catch (Exception)
                    return;
                if (cc.closing)
                    return;
                if (atomicLoad(cc.killReq))
                {
                    try
                        cc.tcp.close(); // same thread as the serve loop: safe
                    catch (Exception)
                    {
                    }
                    return;
                }
            }
        }, c);
    catch (Exception)
    {
    }
}

private void startHeartbeat(AmqpConn c) nothrow
{
    if (c.hbStarted || c.hbSendMs == 0)
        return;
    c.hbStarted = true;
    try
        cast(void) runTask((AmqpConn cc) nothrow {
            static immutable ubyte[8] hb = [8, 0, 0, 0, 0, 0, 0, 0xCE];
            immutable dur = cc.hbSendMs;
            while (!cc.closing)
            {
                try
                    sleep(dur.msecs);
                catch (Exception)
                    return;
                if (cc.closing)
                    return;
                // dead-peer detection, RabbitMQ-style: no bytes read for 2x
                // the negotiated interval means the peer is gone — close the
                // socket (the java Heartbeat test mutes its side and expects
                // the server to hang up).
                if (cc.hbSecs != 0 && cc.lastReadMs != 0
                        && monoMs() - cc.lastReadMs > cast(long) cc.hbSecs * 2000 + 500)
                {
                    try
                        cc.tcp.close();
                    catch (Exception)
                    {
                    }
                    return;
                }
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
