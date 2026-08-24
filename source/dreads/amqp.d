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
import dreads.stream : nowMs;

// ---------------------------------------------------------------------------
// Hooks installed by server.d (avoid an import cycle): queue data-plane ops
// and control-plane replication.
public __gshared void delegate(scope const(char)[] key, scope const(char)[] payload) nothrow gAmqpPush;
public __gshared void delegate(scope const(char)[] key, scope const(char)[] payload) nothrow gAmqpPushFront;
public __gshared bool delegate(scope const(char)[] key, ref ByteBuffer outPayload) nothrow gAmqpPop;
public __gshared long delegate(scope const(char)[] key) nothrow gAmqpLen;
public __gshared void delegate(scope const(char)[] key) nothrow gAmqpDelKey;
/// Non-destructive head read (LINDEX key 0) for the active-TTL reaper; false =
/// empty queue. Installed by server.d.
public __gshared bool delegate(scope const(char)[] key, ref ByteBuffer outHead) nothrow gAmqpPeekHead;
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
public shared ulong gAmqpMessages; // total basic.publish records routed (dashboard)
public shared ulong gAmqpReturned; // mandatory publishes returned (no route)
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
enum size_t AMQP_MAX_CONSUMERS = 4096;
/// A dead-letter is dropped once it has been dead-lettered this many times
/// (the x-death hop count) — bounds an A->X->A dead-letter cycle.
enum int AMQP_MAX_DEATHS = 16;
public shared ulong gAmqpCtlDrops; // control-plane declares refused at a cap

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
    string dlx; // x-dead-letter-exchange ("" = none)
    string dlrk; // x-dead-letter-routing-key ("" = original queue name)
    long ttlMs; // x-message-ttl (0 = no expiry); lazily dead-lettered/dropped
}

private QueueMeta[string] gQueueMeta; // TLS, broadcast-replicated

private ExType[string] gExchanges; // TLS
/// Last op seq per exchange NAME. A deleted exchange is removed from gExchanges
/// but keeps its seq here (tombstone) so a stale lower-seq declare is rejected.
private ulong[string] gExchangeSeq; // TLS
// Declared-queue existence set (LWW-element-set), mirroring the exchange
// registry: op 8 declares a queue name, op 9 tombstones it. gQueueSeq keeps a
// per-name seq so a stale declare can't resurrect a deleted queue. This is what
// backs the passive-declare / basic.consume NOT_FOUND (404) checks that
// RabbitMQ clients rely on to probe existence.
private bool[string] gQueues; // TLS: present => queue currently exists
private ulong[string] gQueueSeq; // TLS: per-name LWW seq (tombstones survive delete)
// Live consumer count per queue, shard-local (a consumer and the queue.declare
// that reports it share a connection, hence a shard, in the common case). Feeds
// the consumer_count field of queue.declare-ok. Cross-shard consumers are not
// summed here — an accepted best-effort, matching how RabbitMQ treats the field.
private uint[string] gQueueConsumers; // TLS
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
    // Bound the '#'-backtracking: routing keys are attacker-controlled and a
    // key with thousands of dot-words against a multi-'#' pattern is
    // exponential recursion + fiber-stack growth. Real routing keys have a
    // handful of segments; refuse to match absurd ones on the hot path.
    {
        size_t words = 1;
        foreach (ch; key)
            if (ch == '.')
                words++;
        if (words > 128)
            return false;
    }
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
        else if (op == 3) // queue metadata: ex=queue, a=dlx, b=dlrk, ttl(i64 BE)
        {
            auto b = rd().idup;
            auto tb = rd(); // 8-byte big-endian x-message-ttl (may be empty)
            long ttl = 0;
            if (tb.length == 8)
                foreach (k; 0 .. 8)
                    ttl = (ttl << 8) | tb[k];
            if ((cast(string) ex) !in gQueueMeta && gQueueMeta.length >= AMQP_MAX_QUEUEMETA)
            {
                atomicOp!"+="(gAmqpCtlDrops, 1);
                return;
            }
            // merge, don't clobber: a redeclare that sets only ttl must not
            // erase a previously-configured DLX (and vice-versa)
            QueueMeta qm = QueueMeta(a, b, ttl);
            if (auto ex0 = (cast(string) ex) in gQueueMeta)
            {
                if (a.length == 0)
                    qm.dlx = ex0.dlx;
                if (b.length == 0)
                    qm.dlrk = ex0.dlrk;
                if (ttl == 0)
                    qm.ttlMs = ex0.ttlMs;
            }
            gQueueMeta[ex] = qm;
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
                // tombstone (usually an update — declare recorded the key); the
                // cap guard mirrors the declare so a delete can't grow it either
                if ((cast(string) ex) in gExchangeSeq || gExchangeSeq.length < AMQP_MAX_EXCHANGES)
                    gExchangeSeq[ex] = seq; // rejects a stale later declare
                // bindings under a deleted exchange are inert (routeTo needs the
                // exchange); leave them so a concurrent bind's LWW state is kept
            }
            catch (Exception)
            {
            }
        }
        else if (op == 8) // queue.declare: ex=queue name (existence set)
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
        }
        else if (op == 9) // queue.delete: ex=queue name (tombstone)
        {
            if (auto sp = (cast(string) ex) in gQueueSeq)
                if (seq <= *sp)
                    return; // stale vs a newer declare/delete
            gQueues.remove(cast(string) ex);
            if ((cast(string) ex) in gQueueSeq || gQueueSeq.length < AMQP_MAX_QUEUEMETA)
                gQueueSeq[ex] = seq; // tombstone: rejects a stale later declare
        }
    }
    catch (Exception)
    {
    }
}

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
        immutable hit = msgHeaders !is null && headerWantMatches(msgHeaders, k, ty, v);
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
                    break;
                case ExType.topic:
                    m = amqpTopicMatches(bd.key, rkey);
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
        case 'U': // signed short
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

/// Parse the `expiration` basic property (a shortstr of decimal milliseconds;
/// property-flags bit 8). 0 = absent/invalid. Walks the properties that precede
/// it in the flags order: content-type, content-encoding, headers (field
/// table), delivery-mode + priority (one octet each), correlation-id, reply-to.
package long propsExpiration(scope const(ubyte)[] props) @nogc nothrow
{
    if (props.length < 2)
        return 0;
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
            return 0;
    if (flags & 0x4000) // content-encoding
        if (!skipShort(props, i))
            return 0;
    if (flags & 0x2000) // headers: u32 length + table
    {
        if (i + 4 > props.length)
            return 0;
        immutable n = (cast(size_t) props[i] << 24) | (cast(size_t) props[i + 1] << 16)
            | (cast(size_t) props[i + 2] << 8) | props[i + 3];
        i += 4 + n;
        if (i > props.length)
            return 0;
    }
    if (flags & 0x1000) // delivery-mode: octet
        i += 1;
    if (flags & 0x0800) // priority: octet
        i += 1;
    if (flags & 0x0400) // correlation-id
        if (!skipShort(props, i))
            return 0;
    if (flags & 0x0200) // reply-to
        if (!skipShort(props, i))
            return 0;
    if (!(flags & 0x0100)) // no expiration property
        return 0;
    if (i + 1 > props.length)
        return 0;
    immutable len = props[i];
    i += 1;
    if (i + len > props.length)
        return 0;
    long v = 0;
    foreach (k; 0 .. len)
    {
        immutable ch = props[i + k];
        if (ch < '0' || ch > '9')
            return 0; // non-numeric expiration -> treat as unset
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
    size_t txBytes; // running size of buffered tx publish records (byte cap)
}

private struct Unacked
{
    string queue;
    const(ubyte)[] blob; // stored record (rk+props+body), for requeue/dead-letter
    ushort chan; // owning channel: channel.close requeues just this channel's
    int deaths; // reserved for a future x-death header hop count (loop bound)
}

private final class AmqpConn
{
    TCPConnection tcp;
    TaskMutex wlock;
    Channel[ushort] chans;
    bool closing;
    bool[string] cancelledTags; // basic.cancel'ed consumer tags
    Unacked[ulong] unacked; // delivery-tag -> record (no_ack=false consumers)
    size_t unackedBytes; // running sum of unacked blob bytes (byte-cap the window)
    ulong nextTag = 1;
    ulong nextCtag = 1; // server-assigned consumer tags (unique per connection)
    size_t prefetch;    // basic.qos prefetch-count (0 = AMQP_DEFAULT_PREFETCH)
    uint chanGenCtr; // monotonic per-conn source for Channel.openGen (channel-reuse safe)
    size_t consumerCount; // live basic.consume fibers (per-conn cap)
    uint hbSendSecs; // heartbeat SEND interval (0 = disabled); set from tune-ok
    bool hbStarted;  // the sender fiber is spawned exactly once
    uint frameMax = AMQP_FRAME_MAX; // NEGOTIATED max frame size (from tune-ok): we
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

public void serveAmqpClient(TCPConnection tcp) nothrow
{
    seedWellKnownExchanges(); // once per shard thread: the mandated amq.* defaults
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
        if (space.length < cast(size_t) avail)
            return; // OOM growing the input buffer: drop THIS client, not the broker
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
            if (fsize > AMQP_FRAME_MAX)
                return; // exceeds advertised frame-max: framing error, close
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
                putU16(b, 30); // heartbeat we PROPOSE; the client's tune-ok
            });                //  (below) is what we actually honor
            return true;
        case 31: // tune-ok: channel-max u16, frame-max u32, heartbeat u16
            {
                cast(void) r.u16(); // channel-max (we accept the client's)
                immutable fm = r.u32(); // frame-max (u32): the client's negotiated max
                // honor it (bounded to [4096, our proposal]); 0 = "no limit" per
                // spec, so fall back to our own max. Prevents emitting a body frame
                // larger than a down-negotiating client's frame-max.
                c.frameMax = fm == 0 ? AMQP_FRAME_MAX
                    : (fm < 4096 ? 4096 : (fm > AMQP_FRAME_MAX ? AMQP_FRAME_MAX : fm));
                immutable hb = r.u16(); // NEGOTIATED heartbeat interval, seconds
                // Send at half the negotiated interval so the client always
                // sees a frame within its dead-peer window (2× interval).
                // 0 = heartbeats disabled: don't start the sender at all.
                c.hbSendSecs = hb == 0 ? 0 : (hb + 1) / 2;
                if (c.hbSendSecs != 0)
                    startHeartbeat(c);
                return true;
            }
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
            // requeue this channel's in-flight (unacked) records and stop its
            // consumer fibers (they observe the gen/existence mismatch and exit)
            // before dropping the channel — else those popped messages are lost.
            requeueAndDropChannel(c, chan);
            method(o, chan, 20, 41);
            return true;
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
            auto ex = r.shortStr();
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
            ctlBroadcast(1, ex, typ, "");
            method(o, chan, 40, 11);
            return true;
        }
        if (mth == 20) // delete
        {
            cast(void) r.u16();
            auto ex = r.shortStr();
            ctlBroadcast(5, ex, "", ""); // op 5: drop the exchange + its bindings
            method(o, chan, 40, 21); // delete-ok
            return true;
        }
        if (mth == 30) // bind (exchange-to-exchange): dest, source, rk, args
        {
            cast(void) r.u16();
            auto dest = r.shortStr();
            auto source = r.shortStr();
            auto rk = r.shortStr();
            cast(void) r.u8(); // no-wait
            auto bindArgs = r.tableRaw();
            ctlBroadcast(6, source, dest, rk, bindArgs); // op 6: source -> dest exch
            method(o, chan, 40, 31); // bind-ok
            return true;
        }
        if (mth == 40) // unbind (exchange-to-exchange)
        {
            cast(void) r.u16();
            auto dest = r.shortStr();
            auto source = r.shortStr();
            auto rk = r.shortStr();
            cast(void) r.u8(); // no-wait
            cast(void) r.tableRaw();
            ctlBroadcast(7, source, dest, rk); // op 7: drop the e2e binding
            method(o, chan, 40, 51); // unbind-ok
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
                    immutable qn = q.length <= qbuf.length ? q.length : qbuf.length;
                    qbuf[0 .. qn] = q[0 .. qn];
                    qq = cast(const(char)[]) qbuf[0 .. qn];
                }
                if (argsTbl !is null && argsTbl.length)
                {
                    auto dlx = tableGetStr(argsTbl, "x-dead-letter-exchange");
                    auto dlrk = tableGetStr(argsTbl, "x-dead-letter-routing-key");
                    immutable ttl = tableGetInt(argsTbl, "x-message-ttl");
                    if (dlx !is null || ttl > 0)
                    {
                        ubyte[8] tb = void;
                        foreach (k; 0 .. 8)
                            tb[k] = cast(ubyte)(ttl >> ((7 - k) * 8));
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
                if (passive)
                {
                    if (q.length && !queueExists(qq))
                    {
                        channelClose(o, chan, 404, "NOT_FOUND - no queue", 50, 10);
                        c.chans.remove(chan);
                        return true;
                    }
                }
                else if (!queueExists(qq))
                    // broadcast is a stream — clients redeclare a queue before
                    // every use, so announce only the FIRST time this shard sees
                    // a name (or the first after a tombstone). LWW seq still
                    // orders the genuine announce and a post-delete resurrection.
                    ctlBroadcast(8, qq, "", "");
                static ByteBuffer kb; // TLS
                queueKey(qq, kb);
                immutable cnt = gAmqpLen !is null ? gAmqpLen(kb.data.asChars) : 0;
                immutable ccnt = (cast(string) qq in gQueueConsumers)
                    ? gQueueConsumers[cast(string) qq] : 0u;
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
                cast(void) r.u8(); // no-wait
                auto bindArgs = r.tableRaw();
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
                ctlBroadcast(2, ex, q, rk, bindArgs);
                method(o, chan, 50, 21);
                return true;
            }
        case 50: // unbind
            {
                cast(void) r.u16();
                auto q = r.shortStr();
                auto ex = r.shortStr();
                auto rk = r.shortStr();
                cast(void) r.tableRaw(); // arguments (ignored on unbind)
                ctlBroadcast(4, ex, q, rk); // op 4: drop the matching binding
                method(o, chan, 50, 51); // unbind-ok
                return true;
            }
        case 30: // purge: empty the queue, reply purge-ok with the count
            {
                cast(void) r.u16();
                auto q = r.shortStr();
                cast(void) r.u8(); // no-wait
                static ByteBuffer pk; // TLS
                queueKey(q, pk);
                // stack-copy the key across gAmqpLen's yield (same hazard as
                // delete below): a concurrent queueKey would clobber TLS `pk`
                char[8 + 256 + 4] purgeKeyStore = void;
                immutable pklen = pk.length <= purgeKeyStore.length ? pk.length : purgeKeyStore.length;
                purgeKeyStore[0 .. pklen] = cast(const(char)[]) pk.data[0 .. pklen];
                auto purgeKey = cast(const(char)[]) purgeKeyStore[0 .. pklen];
                immutable n = gAmqpLen !is null ? gAmqpLen(purgeKey) : 0;
                if (gAmqpDelKey !is null)
                    gAmqpDelKey(purgeKey); // DEL empties the list; queue-meta kept
                method(o, chan, 50, 31, (ref ByteBuffer b) @nogc nothrow {
                    putU32(b, cast(uint)(n < 0 ? 0 : n)); // purged message_count
                });
                return true;
            }
        case 40: // delete
            {
                cast(void) r.u16();
                auto q = r.shortStr();
                cast(void) r.u8(); // if-unused/if-empty/no-wait bits
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
                    ctlBroadcast(9, q, "", ""); // tombstone in the existence set
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
                // a no-ack=false get also consumes prefetch: don't let millions
                // of un-acked gets pin RAM (the consumer path already caps this)
                immutable getLimit = c.prefetch ? c.prefetch : AMQP_DEFAULT_PREFETCH;
                bool getFull = false;
                try
                    getFull = !getNoAck && (c.unacked.length >= getLimit
                            || c.unackedBytes >= AMQP_MAX_UNACKED_BYTES);
                catch (Exception)
                {
                }
                static ByteBuffer kb2; // TLS
                queueKey(q, kb2);
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
                    immutable remaining = gAmqpLen !is null ? gAmqpLen(getKey) : 0;
                    immutable gtag = c.nextTag++;
                    // no-ack=false: record for later ack/requeue; the old code
                    // hardcoded tag 1 and never recorded it, so a get+ack
                    // workflow could neither ack nor requeue (message lost)
                    if (!getNoAck)
                        try
                        {
                            c.unacked[gtag] = Unacked(q.idup, pay.data.idup, chan, 0);
                            c.unackedBytes += pay.data.length;
                        }
                        catch (Exception)
                        {
                        }
                    immutable redlv = recordRedelivered(pay.data);
                    auto grk = recordRoutingKey(pay.data);
                    auto gex = recordExchange(pay.data);
                    method(o, chan, 60, 71, (ref ByteBuffer b) @nogc nothrow {
                        putU64(b, gtag);
                        b.appendByte(redlv ? 1 : 0); // redelivered
                        putShortStr(b, gex); // original exchange
                        putShortStr(b, grk); // original routing key
                        putU32(b, cast(uint)(remaining < 0 ? 0 : remaining));
                    });
                    emitContent(o, chan, pay.data, c.frameMax);
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
                // Consuming from a queue that was never declared is a channel
                // 404 NOT_FOUND, like RabbitMQ. The connection survives.
                if (q.length && !queueExists(q))
                {
                    channelClose(o, chan, 404, "NOT_FOUND - no queue", 60, 20);
                    c.chans.remove(chan);
                    return true;
                }
                auto tag = r.shortStr();
                immutable bits = r.u8();
                immutable noAck = (bits & 2) != 0;
                static char[128] tagbuf = void;
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
                        dropUnacked(c, tag);
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
                        // preserve delivery order on redelivery: tags are
                        // monotonic (delivery order), but the AA yields them in
                        // hash order. requeue pushes to the FRONT, so settle
                        // highest-tag-first (lowest ends up frontmost = FIFO);
                        // dead-letter RPUSHes the DLX tail, so settle lowest-first.
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
                cast(void) r.u8(); // requeue bit — we always requeue to the front
                try
                {
                    // requeue every unacked delivery on THIS channel, FIFO-
                    // preserving (descending tag -> pushFront leaves ascending at
                    // the head), each marked redelivered by settleNegative. A
                    // client stuck without recover-ok used to hang forever.
                    import std.algorithm.sorting : sort;

                    ulong[] all;
                    foreach (t, ref u; c.unacked)
                        if (u.chan == chan)
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
                c.prefetch = pc; // 0 = "no specific limit" -> default cap applies
                method(o, chan, 60, 11);
                return true;
            }
        case 30: // cancel — stop the consumer fiber, reply CancelOk
            {
                auto tag = r.shortStr();
                immutable noWait = r.ok && r.i < p.length ? (p[r.i] & 1) != 0 : false;
                // cap the map: basic.cancel needs neither an open channel nor a
                // completed handshake, so a flood of unique bogus tags would
                // otherwise grow this per-conn AA without bound (RAM DoS). Legit
                // pending cancels never exceed the live-consumer count (both
                // capped at AMQP_MAX_CONSUMERS) and each is removed on the
                // consumer's exit, so a healthy connection never hits the cap.
                try
                    if (c.cancelledTags.length < AMQP_MAX_CONSUMERS)
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
                commitTx(c, chan, *ch, o);
                method(o, chan, 90, 21); // commit-ok
            }
            else if (mth == 30) // rollback: drop the buffers (acks stay un-applied
            {                    //  -> messages remain unacked / redeliverable)
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
        auto payload = tp.record.asChars;
        int routed = 0;
        routeTo(tp.exchange, tp.rkey, propsHeaders(props), (string q) nothrow {
            static ByteBuffer kbT; // TLS
            queueKey(q, kbT);
            if (gAmqpPush !is null)
                gAmqpPush(kbT.data.asChars, payload);
            routed++;
        });
        atomicOp!"+="(gAmqpMessages, 1);
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
            atomicOp!"+="(gAmqpReturned, 1);
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
    o.append("\x04AMQ");
    putU64(o, cast(ulong) publishMs); // wall-clock ms at publish (0 = unknown)
    o.appendByte(cast(char)(deaths > 255 ? 255 : (deaths < 0 ? 0 : deaths))); // x-death hop count
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
private bool recordRedelivered(scope const(ubyte)[] blob) @nogc nothrow
{
    return blob.length >= 15 && (blob[0] == 0x03 || blob[0] == 0x04) && blob[1] == 'A'
        && blob[2] == 'M' && blob[3] == 'Q' && (blob[12] & 0x80) != 0;
}

/// The ORIGINAL exchange a message was published to ([basic.deliver]/
/// [basic.get-ok] carry it). Only the v4 record stores it; earlier records (and
/// bare RESP-side values) predate the field and yield "" — which is also the
/// correct value for a default-exchange publish.
private const(char)[] recordExchange(return scope const(ubyte)[] blob) @nogc nothrow
{
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
    if (blob.length >= 15 && (blob[0] == 0x03 || blob[0] == 0x04) && blob[1] == 'A'
            && blob[2] == 'M' && blob[3] == 'Q')
    {
        auto d = cast(ubyte[]) dst.data;
        d[12] |= 0x80;
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
        c.chans.remove(chan);
    }
    catch (Exception)
    {
    }
}

private void finishPublish(AmqpConn c, ushort chan, ref Channel ch, ref ByteBuffer o) nothrow @trusted
{
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
    rec.clear();
    buildRecord(*rec, cast(long) nowMs(), 0, ch.pub.rkey, ch.pub.props.data,
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
    auto hdrs = propsHeaders(ch.pub.props.data);
    atomicOp!"+="(gAmqpMessages, 1);
    int routed = 0;
    routeTo(ch.pub.exchange, ch.pub.rkey, hdrs, (string q) nothrow {
        static ByteBuffer kb3; // TLS
        queueKey(q, kb3);
        if (gAmqpPush !is null)
            gAmqpPush(kb3.data.asChars, payload);
        routed++;
    });
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
        atomicOp!"+="(gAmqpReturned, 1);
    }
    if (ch.confirmMode)
    {
        // RabbitMQ confirms a returned mandatory message too: the return is the
        // routing signal, the ack is the broker-took-responsibility signal
        immutable tag = ch.confirmSeq++;
        method(o, chan, 60, 80, (ref ByteBuffer b) @nogc nothrow {
            putU64(b, tag);
            b.appendByte(0); // multiple=false
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
    if (props.length >= 2)
        o.append(props);
    else
        putU16(o, 0); // no properties
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
                static ByteBuffer kb6; // TLS
                queueKey(u.queue, kb6);
                static ByteBuffer rq6; // TLS: redelivered-marked copy
                markRedelivered(rq6, u.blob);
                if (gAmqpPushFront !is null)
                    gAmqpPushFront(kb6.data.asChars, rq6.data.asChars);
            }
        c.unacked.clear();
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
private void dropUnacked(AmqpConn c, ulong tag) nothrow @trusted
{
    try
        if (auto p = tag in c.unacked)
        {
            immutable n = p.blob.length;
            c.unackedBytes = c.unackedBytes >= n ? c.unackedBytes - n : 0;
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
    bool found = false;
    try
    {
        if (auto p = tag in c.unacked)
        {
            u = *p;
            found = true;
            immutable n = u.blob.length;
            c.unackedBytes = c.unackedBytes >= n ? c.unackedBytes - n : 0;
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
        // mark the requeued copy redelivered so the next delivery sets the flag
        // (both TLS buffers are consumed by gAmqpPushFront before its yield)
        static ByteBuffer rq4; // TLS
        markRedelivered(rq4, u.blob);
        if (gAmqpPushFront !is null)
            gAmqpPushFront(kb4.data.asChars, rq4.data.asChars);
        return;
    }
    deadLetter(u.queue, u.blob, "rejected");
}

/// The queue's x-message-ttl in ms (0 = none). Looked up per delivery.
private long queueTtl(scope const(char)[] q) nothrow @trusted
{
    try
        if (auto m = q in gQueueMeta)
            return m.ttlMs;
    catch (Exception)
    {
    }
    return 0;
}

/// Has this record outlived `ttlMs` since it was published? (v3 records carry
/// the publish time; older records report 0 = never expire lazily.)
private bool isExpired(scope const(ubyte)[] blob, long ttlMs) nothrow @trusted
{
    long pm;
    int dths;
    const(char)[] rk;
    const(ubyte)[] props, body_;
    splitRecord(blob, pm, dths, rk, props, body_);
    // effective TTL = the SMALLER of the queue x-message-ttl and the message's
    // own `expiration` property (RabbitMQ semantics), ignoring a 0 (unset) on
    // either side. A per-message expiration expires even on a queue with no
    // x-message-ttl (lazily at delivery; the active reaper only sweeps queues
    // that have a queue-level TTL).
    immutable msgTtl = propsExpiration(props);
    long ttl;
    if (ttlMs > 0 && msgTtl > 0)
        ttl = ttlMs < msgTtl ? ttlMs : msgTtl;
    else
        ttl = ttlMs > 0 ? ttlMs : msgTtl;
    if (ttl <= 0)
        return false;
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
private void buildXDeathEntry(ref ByteBuffer o, long count, scope const(char)[] reason,
        scope const(char)[] queue, scope const(char)[] rk) @nogc nothrow
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
    xtStr(o, "exchange", ""); // original exchange isn't stored in the record
    o.appendByte(cast(char) 12);
    o.append("routing-keys");
    o.appendByte('A');
    immutable rkAt = o.length;
    putU32(o, 0);
    immutable rkStart = o.length;
    o.appendByte('S');
    putU32(o, cast(uint) rk.length);
    o.append(rk);
    patchU32(o, rkAt, cast(uint)(o.length - rkStart));
    patchU32(o, tblAt, cast(uint)(o.length - tblStart));
    patchU32(o, arrAt, cast(uint)(o.length - arrStart));
}

/// Append every entry of headers-table `t` to `dst` EXCEPT the one keyed
/// `skipKey`. Used to drop a prior x-death before adding the current one, so a
/// message dead-lettered N times carries ONE x-death (with the live count), not
/// N stale duplicates. Bails (copying nothing further) on a malformed entry.
private void appendHeadersExcept(ref ByteBuffer dst, scope const(ubyte)[] t,
        scope const(char)[] skipKey) @nogc nothrow
{
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
            if (i + 1 > t.length)
                return;
            i += 1 + t[i];
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
        if (key != skipKey)
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
    immutable nf = cast(ushort)(flags | 0x2000);
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
    appendHeadersExcept(hstrip, existing, "x-death");
    putU32(dst, cast(uint)(xentry.length + hstrip.length));
    dst.append(cast(const(char)[]) xentry);
    dst.append(cast(const(char)[]) hstrip.data);
    dst.append(cast(const(char)[]) props[i .. $]); // delivery-mode onward verbatim
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
    if (meta.dlx.length == 0)
        return; // no dead-letter exchange: drop
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
    buildXDeathEntry(xbuf, deaths + 1, reason, queue, origRk);
    mergeXDeath(paug, props, xbuf.data);
    dlrec.clear();
    buildRecord(*dlrec, pm, deaths + 1, origRk, paug.data, body_, meta.dlx);
    auto blobc = dlrec.data.asChars;
    routeTo(meta.dlx, rk, propsHeaders(paug.data), (string q) nothrow {
        static ByteBuffer kb5; // TLS
        queueKey(q, kb5);
        if (gAmqpPush !is null)
            gAmqpPush(kb5.data.asChars, blobc);
    });
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
            if (meta.ttlMs <= 0)
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
            if (mp is null || mp.ttlMs <= 0)
                continue;
            immutable ttl = mp.ttlMs;
            // COPY the key to the stack: deadLetter() below yields (cross-shard
            // DLX push), and the TLS `kb` would be clobbered by a concurrent
            // fiber's queueKey during that park -> the next peek/pop would hit a
            // DIFFERENT queue. The stack copy survives the yield.
            queueKey(q, kb);
            char[8 + 256 + 4] keyStore = void; // "amq.q." + queue name
            immutable klen = kb.length <= keyStore.length ? kb.length : keyStore.length;
            keyStore[0 .. klen] = cast(const(char)[]) kb.data[0 .. klen];
            auto key = cast(const(char)[]) keyStore[0 .. klen];
            if (!gAmqpOwns(key))
                continue; // only the list's owner reaps it
            int reaped = 0;
            while (reaped < 4096) // bound the work per queue per tick
            {
                head.clear();
                if (!gAmqpPeekHead(key, head))
                    break; // empty queue
                if (!isExpired(head.data, ttl))
                    break; // head is fresh -> everything behind it is younger
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
        gQueueConsumers[q] = (q in gQueueConsumers ? gQueueConsumers[q] : 0u) + 1;
    catch (Exception)
    {
    }
}

private void qConsumerDec(string q) nothrow @trusted
{
    if (auto p = q in gQueueConsumers)
    {
        if (*p > 0)
            --*p;
        if (*p == 0)
            gQueueConsumers.remove(q);
    }
}

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
    uint myGen; // the channel generation this consumer belongs to (channel exists
    if (auto pch = chan in c.chans) // now — basic.consume is on an open channel)
        myGen = pch.openGen;
    atomicOp!"+="(gAmqpConsumers, 1);
    c.consumerCount++;
    qConsumerInc(qs); // live consumer_count for queue.declare-ok
    try
        cast(void) runTask((AmqpConn cc, ushort chn, string qq, string tt, bool na, uint mg) nothrow {
            scope (exit)
            {
                atomicOp!"-="(gAmqpConsumers, 1);
                if (cc.consumerCount > 0)
                    cc.consumerCount--;
                qConsumerDec(qq);
                // drop our own cancel marker so a healthy connection's
                // cancelledTags doesn't accumulate one dead entry per
                // consume/cancel cycle (it's only needed until we've seen it).
                // AA.remove on a string key is nothrow — no try/catch (which a
                // scope(exit) can't contain anyway).
                cc.cancelledTags.remove(tt);
            }
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
                            b.appendByte(0); // no-wait = 0
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
                // the window is full, back off until acks drain it.
                immutable limit = cc.prefetch ? cc.prefetch : AMQP_DEFAULT_PREFETCH;
                bool windowFull = false;
                try
                    windowFull = !na && (cc.unacked.length >= limit
                            || cc.unackedBytes >= AMQP_MAX_UNACKED_BYTES);
                catch (Exception)
                {
                }
                if (windowFull)
                {
                    try
                        sleep(1.msecs);
                    catch (Exception)
                        return;
                    continue;
                }
                // BURST drain: up to 64 messages per socket write — a delivery
                // per write capped the consumer at ~137k msg/s (measured)
                ob.clear();
                int burst = 0;
                while (burst < 64)
                {
                    if (!na && (cc.unacked.length >= limit
                            || cc.unackedBytes >= AMQP_MAX_UNACKED_BYTES))
                        break; // window filled mid-burst (count OR bytes)
                    pay.clear();
                    if (!(gAmqpPop !is null && gAmqpPop(kb.data.asChars, pay)))
                        break;
                    // x-message-ttl: an expired head is dead-lettered (or
                    // dropped), never delivered. Count it toward the burst so a
                    // backlog of expired heads can't drain unboundedly in one
                    // pass (it yields between bursts).
                    if (isExpired(pay.data, queueTtl(qq)))
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
                    }
                    catch (Exception)
                    {
                    }
                    if (gone)
                    {
                        if (gAmqpPushFront !is null)
                            gAmqpPushFront(kb.data.asChars, pay.data.asChars);
                        break;
                    }
                    immutable tg = cc.nextTag++;
                    if (!na)
                        try
                        {
                            cc.unacked[tg] = Unacked(qq, pay.data.idup, chn, 0);
                            cc.unackedBytes += pay.data.length;
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
                    try
                        sleep(1.msecs);
                    catch (Exception)
                        return;
                    continue;
                }
                sendTo(cc, ob.data);
            }
        }, c, chan, qs, ts, noAck, myGen);
    catch (Exception)
    {
        atomicOp!"-="(gAmqpConsumers, 1);
        if (c.consumerCount > 0)
            c.consumerCount--;
        qConsumerDec(qs); // the fiber never ran; undo the pre-increment
    }
}

// Heartbeat sender: a fiber emitting a heartbeat frame every 15s while the
// connection lives (half the negotiated 30s interval). Read-side liveness
// stays TCP-level in v1 (the serve loop notices the close).
private void startHeartbeat(AmqpConn c) nothrow
{
    if (c.hbStarted || c.hbSendSecs == 0)
        return;
    c.hbStarted = true;
    try
        cast(void) runTask((AmqpConn cc) nothrow {
            static immutable ubyte[8] hb = [8, 0, 0, 0, 0, 0, 0, 0xCE];
            immutable dur = cc.hbSendSecs * 1000;
            while (!cc.closing)
            {
                try
                    sleep(dur.msecs);
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
