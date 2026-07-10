module dreads.pubsub;

// Pub/Sub registry. Subscribers are opaque (ctx, sink) pairs so the registry
// is testable without sockets: in production the sink locks the connection's
// write mutex and writes to the TCPConnection; in tests it appends to a
// buffer. No GC allocations here — subscriber lists are malloc'd arrays and
// messages are staged in a scratch ByteBuffer.
//
// Pattern matching is pre-matched at SUBSCRIBE time, not scanned at publish
// (see PUBSUB.md). Each pattern is classified by its single `*` anchor:
//   A*   prefix   — channel.startsWith(A)
//   *B   suffix   — channel.endsWith(B)
//   A*B  both     — startsWith(A) && endsWith(B) && len >= |A|+|B|  (no middle scan)
//   A    exactPat — channel == A (a metachar-free PSUBSCRIBE)
//   *    all      — matches everything
//   ?/** general  — falls back to globMatch (exact glob semantics preserved)
// Patterns with a non-empty header (exactPat/prefix/both) live in `headerIndex`
// keyed by that header. A publish probes only the channel's own prefixes, so the
// header is the discriminator: cost is O(len(channel)), independent of P.

import core.stdc.stdlib : malloc, realloc, cfree = free;
import core.stdc.string : memcpy;

import dreads.commands : globMatch;
import dreads.dict : Dict, Unit;
import dreads.mem : ByteBuffer;
import dreads.resp : repArrayHeader, repBulk;

/// Refcounted message frame: encode once, share across every matched subscriber
/// (no per-subscriber copy). The event loop is single-threaded, so `refs` is a
/// plain counter. The frame bytes are stored inline after the header, so the
/// whole message is one allocation.
///
/// NOTE — there are now two refcount mechanisms in the tree: std.typecons
/// SafeRefCounted (containers.d) and this RcMsg. This is deliberate, not an
/// oversight. RcMsg is NOT reusing automem/SafeRefCounted because the lifecycle
/// here is manual and cross-fiber — the publisher fiber stashes the pointer in a
/// raw malloc'd ring and a different (writer) fiber releases it later. RAII
/// value-refcounts (which retain on copy / release on scope exit) fight that
/// pattern: they can't live in a raw ring without emplace/destroy gymnastics,
/// and RefCounted!(Vector!ubyte) would be two allocations (control block + the
/// vector's buffer) instead of the one inline allocation here. Manual
/// retain/release maps 1:1 onto enqueue/drain. If a general refcount ever gets
/// standardised for the malloc data plane, fold this into it.
public struct RcMsg
{
    uint refs;
    uint len;
}

/// Build a refcounted frame from staged bytes (refs starts at 1: the caller's).
public RcMsg* rcFromBytes(scope const(ubyte)[] bytes) @nogc nothrow
{
    auto m = cast(RcMsg*) malloc(RcMsg.sizeof + (bytes.length ? bytes.length : 1));
    assert(m !is null, "out of memory");
    m.refs = 1;
    m.len = cast(uint) bytes.length;
    if (bytes.length)
        memcpy(cast(ubyte*)(m + 1), bytes.ptr, bytes.length);
    return m;
}

public const(ubyte)[] rcData(const(RcMsg)* m) @nogc nothrow
{
    return (cast(const(ubyte)*)(m + 1))[0 .. m.len];
}

public void rcRetain(RcMsg* m) @nogc nothrow
{
    m.refs++;
}

public void rcRelease(RcMsg* m) @nogc nothrow
{
    if (--m.refs == 0)
        cfree(m);
}

/// One connected client from the registry's point of view.
public struct Subscriber
{
    void* ctx;
    void function(void* ctx, RcMsg* msg) nothrow sink;
    Dict!Unit channels;
    Dict!Unit patterns;

    @property size_t subCount() const @nogc nothrow
    {
        return channels.length + patterns.length;
    }

    void free() @nogc nothrow
    {
        channels.free();
        patterns.free();
    }
}

/// Growable malloc'd array of subscriber pointers.
private struct SubList
{
    Subscriber** items;
    size_t len;
    size_t cap;

    void add(Subscriber* s) @nogc nothrow
    {
        if (len == cap)
        {
            cap = cap ? cap * 2 : 4;
            items = cast(Subscriber**) realloc(items, cap * (Subscriber*).sizeof);
            assert(items !is null, "out of memory");
        }
        items[len++] = s;
    }

    /// Returns true when the subscriber was present.
    bool remove(Subscriber* s) @nogc nothrow
    {
        foreach (i; 0 .. len)
        {
            if (items[i] is s)
            {
                items[i] = items[len - 1];
                len--;
                return true;
            }
        }
        return false;
    }

    void free() @nogc nothrow
    {
        if (items !is null)
            cfree(items);
        items = null;
        len = cap = 0;
    }
}

// --- pattern classification -------------------------------------------------

private enum PatKind : ubyte
{
    exactPat, // no metachar: matches channel == pattern
    prefix, // A*
    both, // A*B
    suffix, // *B
    general, // ? or two-plus stars: globMatch fallback
    all // bare "*"
}

/// A compiled pattern subscription. `raw` is an owned malloc copy of the pattern
/// text (used for the pmessage reply and for equality on removal); `a`/`b` slice
/// into it.
private struct PatEntry
{
    Subscriber* sub;
    char* raw;
    uint rawLen;
    PatKind kind;
    uint aLen; // prefix literal = raw[0 .. aLen]
    uint bOff; // suffix literal = raw[bOff .. bOff + bLen]
    uint bLen;

    const(char)[] pattern() const @nogc nothrow
    {
        return raw[0 .. rawLen];
    }

    const(char)[] b() const @nogc nothrow
    {
        return raw[bOff .. bOff + bLen];
    }

    void freeRaw() @nogc nothrow
    {
        if (raw !is null)
            cfree(raw);
        raw = null;
    }
}

/// Classify without copying (used for both compile and removal lookup).
private PatKind classify(scope const(char)[] p, out uint aLen, out uint bOff, out uint bLen) @nogc nothrow
{
    size_t starPos = size_t.max;
    int stars = 0;
    bool q = false;
    foreach (i, c; p)
    {
        if (c == '*')
        {
            stars++;
            if (starPos == size_t.max)
                starPos = i;
        }
        else if (c == '?')
            q = true;
    }
    if (stars == 0 && !q)
    {
        aLen = cast(uint) p.length;
        return PatKind.exactPat;
    }
    if (q || stars >= 2)
        return PatKind.general;
    // exactly one star, no '?'
    if (p.length == 1)
        return PatKind.all; // "*"
    if (starPos == 0)
    {
        bOff = 1;
        bLen = cast(uint) p[1 .. $].length;
        return PatKind.suffix;
    }
    if (starPos + 1 == p.length)
    {
        aLen = cast(uint) starPos;
        return PatKind.prefix;
    }
    aLen = cast(uint) starPos;
    bOff = cast(uint)(starPos + 1);
    bLen = cast(uint) p[starPos + 1 .. $].length;
    return PatKind.both;
}

private bool endsWith(scope const(char)[] s, scope const(char)[] suf) @nogc nothrow pure
{
    return s.length >= suf.length && s[$ - suf.length .. $] == suf;
}

/// A non-empty header qualifies a pattern for the header index (probed by the
/// channel's prefixes); everything else goes to the linear fallback.
private bool headed(PatKind k, uint aLen) @nogc nothrow
{
    return aLen > 0 && (k == PatKind.exactPat || k == PatKind.prefix || k == PatKind.both);
}

/// Growable malloc'd array of PatEntry (value type; owns each entry's raw copy).
private struct PatBucket
{
    PatEntry* items;
    size_t len;
    size_t cap;

    void add(PatEntry e) @nogc nothrow
    {
        if (len == cap)
        {
            cap = cap ? cap * 2 : 4;
            items = cast(PatEntry*) realloc(items, cap * PatEntry.sizeof);
            assert(items !is null, "out of memory");
        }
        items[len++] = e;
    }

    /// Remove the entry for (sub, pattern); frees its raw copy. Returns true if found.
    bool removeMatching(Subscriber* s, scope const(char)[] pat) @nogc nothrow
    {
        foreach (i; 0 .. len)
        {
            if (items[i].sub is s && items[i].pattern == pat)
            {
                items[i].freeRaw();
                items[i] = items[len - 1];
                len--;
                return true;
            }
        }
        return false;
    }

    void free() @nogc nothrow
    {
        foreach (i; 0 .. len)
            items[i].freeRaw();
        if (items !is null)
            cfree(items);
        items = null;
        len = cap = 0;
    }
}

public struct PubSub
{
    private Dict!SubList channels; // channel -> subscribers
    private Dict!PatBucket headerIndex; // header literal -> pattern entries (exactPat/prefix/both)
    private PatBucket fallback; // suffix/general/all (empty-header patterns)
    private size_t maxHeaderLen; // longest header present (bounds the prefix probe)
    private size_t patternEntries; // total (subscriber, pattern) pairs
    private ByteBuffer scratch; // message staging, reused across publishes

    /// True when the channel subscription is new for this subscriber.
    bool subscribe(Subscriber* s, scope const(char)[] channel) @nogc nothrow
    {
        if (!s.channels.set(channel, Unit()))
            return false;
        auto list = channels.get(channel);
        if (list is null)
        {
            channels.set(channel, SubList());
            list = channels.get(channel);
        }
        list.add(s);
        return true;
    }

    bool unsubscribe(Subscriber* s, scope const(char)[] channel) @nogc nothrow
    {
        if (!s.channels.del(channel))
            return false;
        auto list = channels.get(channel);
        if (list !is null)
        {
            list.remove(s);
            if (list.len == 0)
                channels.del(channel);
        }
        return true;
    }

    bool psubscribe(Subscriber* s, scope const(char)[] pattern) @nogc nothrow
    {
        if (!s.patterns.set(pattern, Unit()))
            return false;
        insertPattern(s, pattern);
        return true;
    }

    bool punsubscribe(Subscriber* s, scope const(char)[] pattern) @nogc nothrow
    {
        if (!s.patterns.del(pattern))
            return false;
        removePattern(s, pattern);
        return true;
    }

    /// Removes every subscription (connection teardown).
    void dropAll(Subscriber* s) @nogc nothrow
    {
        foreach (ch, ref u; s.channels)
        {
            auto list = channels.get(ch);
            if (list !is null)
            {
                list.remove(s);
                if (list.len == 0)
                    channels.del(ch);
            }
        }
        s.channels.clear();
        foreach (pat, ref u; s.patterns)
            removePattern(s, pat);
        s.patterns.clear();
    }

    // --- pattern index maintenance ---

    private void insertPattern(Subscriber* s, scope const(char)[] p) @nogc nothrow
    {
        uint aLen, bOff, bLen;
        auto kind = classify(p, aLen, bOff, bLen);
        PatEntry e;
        e.sub = s;
        e.rawLen = cast(uint) p.length;
        e.raw = cast(char*) malloc(p.length ? p.length : 1);
        assert(e.raw !is null, "out of memory");
        if (p.length)
            memcpy(e.raw, p.ptr, p.length);
        e.kind = kind;
        e.aLen = aLen;
        e.bOff = bOff;
        e.bLen = bLen;

        if (headed(kind, aLen))
        {
            auto hdr = p[0 .. aLen];
            auto b = headerIndex.get(hdr);
            if (b is null)
            {
                headerIndex.set(hdr, PatBucket());
                b = headerIndex.get(hdr);
            }
            b.add(e);
            if (aLen > maxHeaderLen)
                maxHeaderLen = aLen;
        }
        else
            fallback.add(e);
        patternEntries++;
    }

    private void removePattern(Subscriber* s, scope const(char)[] p) @nogc nothrow
    {
        uint aLen, bOff, bLen;
        auto kind = classify(p, aLen, bOff, bLen);
        bool removed;
        if (headed(kind, aLen))
        {
            auto hdr = p[0 .. aLen];
            auto b = headerIndex.get(hdr);
            if (b !is null && b.removeMatching(s, p))
            {
                removed = true;
                if (b.len == 0)
                    headerIndex.del(hdr);
            }
        }
        else
            removed = fallback.removeMatching(s, p);
        if (removed)
            patternEntries--;
    }

    private void emitPmessage(Subscriber* s, scope const(char)[] pat,
            scope const(char)[] channel, scope const(char)[] payload) nothrow
    {
        scratch.clear();
        repArrayHeader(scratch, 4);
        repBulk(scratch, "pmessage");
        repBulk(scratch, pat);
        repBulk(scratch, channel);
        repBulk(scratch, payload);
        auto m = rcFromBytes(scratch.data);
        s.sink(s.ctx, m);
        rcRelease(m);
    }

    /// Delivers to channel and pattern subscribers; returns the receiver count.
    /// verb is "message" for regular pub/sub, "smessage" for shard channels.
    long publish(scope const(char)[] channel, scope const(char)[] payload,
            scope const(char)[] verb = "message") nothrow
    {
        long receivers = 0;
        auto list = channels.get(channel);
        if (list !is null && list.len > 0)
        {
            scratch.clear();
            repArrayHeader(scratch, 3);
            repBulk(scratch, verb);
            repBulk(scratch, channel);
            repBulk(scratch, payload);
            auto m = rcFromBytes(scratch.data); // encode once, share across subscribers
            foreach (i; 0 .. list.len)
            {
                auto s = list.items[i];
                s.sink(s.ctx, m);
                receivers++;
            }
            rcRelease(m); // drop the publisher's reference; sinks retained their own
        }

        // Header-indexed patterns: probe only the channel's own prefixes.
        if (headerIndex.length)
        {
            immutable L = channel.length;
            immutable kmax = L < maxHeaderLen ? L : maxHeaderLen;
            for (size_t k = 1; k <= kmax; k++)
            {
                auto b = headerIndex.get(channel[0 .. k]);
                if (b is null)
                    continue;
                foreach (i; 0 .. b.len)
                {
                    auto e = &b.items[i];
                    bool m;
                    final switch (e.kind)
                    {
                    case PatKind.exactPat:
                        m = L == k; // header matched full channel
                        break;
                    case PatKind.prefix:
                        m = true; // header is a prefix of the channel
                        break;
                    case PatKind.both:
                        m = L >= e.aLen + e.bLen && endsWith(channel, e.b);
                        break;
                    case PatKind.suffix:
                    case PatKind.general:
                    case PatKind.all:
                        m = false; // never live in the header index
                        break;
                    }
                    if (m)
                    {
                        emitPmessage(e.sub, e.pattern, channel, payload);
                        receivers++;
                    }
                }
            }
        }

        // Empty-header patterns (suffix/general/all): exact glob, linear.
        foreach (i; 0 .. fallback.len)
        {
            auto e = &fallback.items[i];
            if (globMatch(e.pattern, channel))
            {
                emitPmessage(e.sub, e.pattern, channel, payload);
                receivers++;
            }
        }
        return receivers;
    }

    /// PUBSUB CHANNELS [pattern]
    int eachChannel(scope const(char)[] pattern,
            scope int delegate(const(char)[] channel, size_t nsubs) @nogc nothrow dg) @nogc nothrow
    {
        foreach (ch, ref list; channels)
        {
            if (pattern.length && !globMatch(pattern, ch))
                continue;
            auto r = dg(ch, list.len);
            if (r)
                return r;
        }
        return 0;
    }

    size_t channelSubCount(scope const(char)[] channel) @nogc nothrow
    {
        auto list = channels.get(channel);
        return list is null ? 0 : list.len;
    }

    /// Total pattern subscriptions (Redis counts unique patterns; see DRIFT).
    size_t patternCount() const @nogc nothrow
    {
        return patternEntries;
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

version (unittest)
{
    private struct FakeClient
    {
        Subscriber sub;
        ByteBuffer received;

        static void sinkFn(void* ctx, RcMsg* m) nothrow
        {
            (cast(FakeClient*) ctx).received.append(rcData(m));
        }

        void init_()
        {
            sub.ctx = &this;
            sub.sink = &sinkFn;
        }

        string got()
        {
            return (cast(string) received.data).idup;
        }
    }
}

unittest // subscribe/publish/unsubscribe flow
{
    PubSub ps;
    FakeClient a, b;
    a.init_();
    b.init_();
    scope (exit)
    {
        a.sub.free();
        b.sub.free();
    }

    assert(ps.subscribe(&a.sub, "news"));
    assert(!ps.subscribe(&a.sub, "news")); // duplicate
    assert(ps.subscribe(&b.sub, "news"));
    assert(a.sub.subCount == 1);
    assert(ps.channelSubCount("news") == 2);

    assert(ps.publish("news", "hello") == 2);
    enum msg = "*3\r\n$7\r\nmessage\r\n$4\r\nnews\r\n$5\r\nhello\r\n";
    assert(a.got == msg && b.got == msg);

    assert(ps.publish("empty", "x") == 0);

    assert(ps.unsubscribe(&a.sub, "news"));
    assert(!ps.unsubscribe(&a.sub, "news"));
    assert(ps.channelSubCount("news") == 1);
    a.received.clear();
    assert(ps.publish("news", "again") == 1);
    assert(a.got == "");
}

unittest // pattern subscriptions (prefix)
{
    PubSub ps;
    FakeClient p;
    p.init_();
    scope (exit)
        p.sub.free();

    assert(ps.psubscribe(&p.sub, "user:*"));
    assert(!ps.psubscribe(&p.sub, "user:*"));
    assert(ps.publish("user:42", "hi") == 1);
    assert(p.got == "*4\r\n$8\r\npmessage\r\n$6\r\nuser:*\r\n$7\r\nuser:42\r\n$2\r\nhi\r\n");
    assert(ps.publish("other", "no") == 0);

    // both channel and pattern deliveries count
    FakeClient c;
    c.init_();
    scope (exit)
        c.sub.free();
    ps.subscribe(&c.sub, "user:42");
    p.received.clear();
    assert(ps.publish("user:42", "x") == 2);

    assert(ps.punsubscribe(&p.sub, "user:*"));
    assert(ps.publish("user:42", "y") == 1); // only the channel subscriber now
}

unittest // both-bound A*B — the websockets:*:system1 case
{
    PubSub ps;
    FakeClient p;
    p.init_();
    scope (exit)
        p.sub.free();

    assert(ps.psubscribe(&p.sub, "websockets:*:system1"));
    // header + tail match, wildcard run is free
    assert(ps.publish("websockets:connect:system1", "x") == 1);
    p.received.clear();
    // header matches but tail differs -> no delivery
    assert(ps.publish("websockets:connect:system2", "x") == 0);
    // tail matches but header differs -> no delivery
    assert(ps.publish("http:connect:system1", "x") == 0);
    // the star spans colons too (exact glob semantics)
    assert(ps.publish("websockets:a:b:c:system1", "x") == 1);
}

unittest // header discriminates: nested and sibling headers each fire once
{
    PubSub ps;
    FakeClient a, b, c;
    a.init_();
    b.init_();
    c.init_();
    scope (exit)
    {
        a.sub.free();
        b.sub.free();
        c.sub.free();
    }
    ps.psubscribe(&a.sub, "foo*"); // prefix, header "foo"
    ps.psubscribe(&b.sub, "fo*"); // prefix, header "fo"
    ps.psubscribe(&c.sub, "bar*"); // prefix, header "bar"

    // "food" matches foo* and fo* but not bar*
    assert(ps.publish("food", "x") == 2);
    assert(a.got.length > 0 && b.got.length > 0 && c.got.length == 0);
    // "bard" matches only bar*
    a.received.clear();
    b.received.clear();
    assert(ps.publish("bard", "x") == 1);
}

unittest // suffix, general and match-all live in the fallback
{
    PubSub ps;
    FakeClient s, g, all;
    s.init_();
    g.init_();
    all.init_();
    scope (exit)
    {
        s.sub.free();
        g.sub.free();
        all.sub.free();
    }
    ps.psubscribe(&s.sub, "*:done"); // suffix
    ps.psubscribe(&g.sub, "job.?"); // general (has '?')
    ps.psubscribe(&all.sub, "*"); // match-all

    assert(ps.publish("task:done", "x") == 2); // suffix + all
    assert(s.got.length > 0 && all.got.length > 0);
    s.received.clear();
    all.received.clear();
    assert(ps.publish("job.7", "x") == 2); // general + all
    assert(g.got.length > 0);

    assert(ps.punsubscribe(&all.sub, "*"));
    assert(ps.publish("job.7", "y") == 1); // only general now
}

unittest // exact metachar-free pattern matches only the identical channel
{
    PubSub ps;
    FakeClient p;
    p.init_();
    scope (exit)
        p.sub.free();
    ps.psubscribe(&p.sub, "abc"); // exactPat, header "abc"
    assert(ps.publish("abc", "x") == 1);
    p.received.clear();
    assert(ps.publish("abcd", "x") == 0); // header is a prefix but lengths differ
    assert(ps.publish("ab", "x") == 0);
}

unittest // dropAll cleans every registration
{
    PubSub ps;
    FakeClient a;
    a.init_();
    scope (exit)
        a.sub.free();
    ps.subscribe(&a.sub, "c1");
    ps.subscribe(&a.sub, "c2");
    ps.psubscribe(&a.sub, "p*");
    ps.psubscribe(&a.sub, "x*y"); // both
    ps.psubscribe(&a.sub, "*z"); // suffix (fallback)
    assert(a.sub.subCount == 5);
    assert(ps.patternCount == 3);
    ps.dropAll(&a.sub);
    assert(a.sub.subCount == 0);
    assert(ps.patternCount == 0);
    assert(ps.publish("c1", "x") == 0);
    assert(ps.publish("pq", "x") == 0);
    assert(ps.publish("xANYy", "x") == 0);
    assert(ps.publish("endz", "x") == 0);
    size_t n;
    ps.eachChannel(null, (ch, cnt) { n++; return 0; });
    assert(n == 0);
}
