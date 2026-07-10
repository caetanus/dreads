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
import dreads.zset : ZSet;

/// Refcounted message frame: encode once, share across every matched subscriber
/// (no per-subscriber copy). The event loop is single-threaded, so `refs` is a
/// plain counter. The frame bytes are stored inline after the header, so the
/// whole message is one allocation.
///
/// NOTE — this coexists with std.typecons SafeRefCounted (containers.d), and
/// that is deliberate: automem/SafeRefCounted does not provide what this needs.
/// Two grounded reasons (verified against automem's ref_counted.d):
///   1. One allocation. RcMsg stores the frame bytes inline after the header,
///      so a frame is a single malloc. automem RefCounted's Impl holds the
///      payload inline too, but a variable-length payload (Vector!ubyte) keeps
///      its buffer separately -> two allocations per frame. The frame alloc is
///      ~3% of a publish (bench/rcalloc_bench.d: 7.4 ns malloc/free), so halving
///      the alloc count matters more than swapping the allocator (a FreeList was
///      measured 72% faster per op but only ~2% of a publish -> not worth it).
///   2. Manual, cross-fiber lifetime. The publisher fiber stashes the pointer in
///      a raw ring; a different writer fiber releases it later. RAII value-
///      refcounts (retain-on-copy / release-on-scope) can't live in a raw ring
///      without emplace/destroy gymnastics; manual retain/release maps 1:1 onto
///      enqueue/drain. The refcount itself is three trivial lines.
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

/// RESP3 subscribers receive pub/sub frames as a Push (`>`) rather than an
/// Array (`*`); the frame is otherwise byte-identical, so clone and patch the
/// leading type byte. Returns a fresh frame (refs=1) owned by the caller. Only
/// the (typically fewer) RESP3 subscribers pay this copy — RESP2 subscribers
/// keep sharing the one encode-once frame.
public RcMsg* rcAsPush(const(RcMsg)* m) @nogc nothrow
{
    auto n = rcFromBytes(rcData(m));
    if (n.len > 0)
        (cast(ubyte*)(n + 1))[0] = '>';
    return n;
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

// A pattern is split at its OUTER metacharacters into a literal prefix A (up to
// the first '*'/'?') and a literal suffix B (after the last). It is indexed by
// whichever of A/B is non-empty, and a publish intersects the A-hits and B-hits.
// "scan" kinds carry extra middle complexity ('?' or a second '*'), so after the
// anchors narrow the candidate subgroup, globMatch verifies the middle — but
// only over that small subgroup, never over all patterns.
private enum PatKind : ubyte
{
    exact, // no metachar: matches iff channel == pattern (left-only, len check)
    prefix, // A* : left-only, left match is sufficient
    suffix, // *B : right-only, right match is sufficient
    both, // A*B (single '*'): both-anchored, anchors + length are exact
    leftScan, // left-anchored, needs globMatch (e.g. A?x, A*x*)
    rightScan, // right-anchored, needs globMatch (e.g. x?B, *x*B)
    bothScan, // both-anchored + middle: A*x*B, A?B — intersect then globMatch
    matchAll, // bare "*"
    noAnchor // no outer literal on either side: *x*, ?x? — global glob scan
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
    uint aLen; // left literal  = raw[0 .. aLen]           (A in A*B)
    uint bOff; // right literal = raw[bOff .. bOff + bLen]  (B in A*B)
    uint bLen;

    const(char)[] pattern() const @nogc nothrow
    {
        return raw[0 .. rawLen];
    }

    const(char)[] a() const @nogc nothrow
    {
        return raw[0 .. aLen];
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

/// Classify without copying. Splits at the OUTER metacharacters: aLen is the
/// literal prefix length (up to the first '*'/'?'), bOff/bLen the literal suffix
/// (after the last). The kind records how much middle work remains.
private PatKind classify(scope const(char)[] p, out uint aLen, out uint bOff, out uint bLen) @nogc nothrow
{
    size_t firstMeta = size_t.max, lastMeta = size_t.max;
    int stars = 0;
    bool q = false;
    foreach (i, c; p)
    {
        if (c == '*' || c == '?')
        {
            if (firstMeta == size_t.max)
                firstMeta = i;
            lastMeta = i;
            if (c == '*')
                stars++;
            else
                q = true;
        }
    }
    if (firstMeta == size_t.max) // no metachar
    {
        aLen = cast(uint) p.length;
        return PatKind.exact;
    }
    if (p.length == 1) // "*" or "?"
        return stars == 1 ? PatKind.matchAll : PatKind.noAnchor;

    aLen = cast(uint) firstMeta; // literal prefix p[0 .. firstMeta]
    bOff = cast(uint)(lastMeta + 1); // literal suffix p[lastMeta+1 .. $]
    bLen = cast(uint) p[lastMeta + 1 .. $].length;
    immutable simple = stars == 1 && !q; // single '*', no '?' -> anchors are exact

    if (aLen > 0 && bLen > 0)
        return simple ? PatKind.both : PatKind.bothScan;
    if (aLen > 0)
        return simple ? PatKind.prefix : PatKind.leftScan;
    if (bLen > 0)
        return simple ? PatKind.suffix : PatKind.rightScan;
    return PatKind.noAnchor; // *x*, ?x? : nothing to anchor on
}

private bool startsWith(scope const(char)[] s, scope const(char)[] pre) @nogc nothrow pure
{
    return s.length >= pre.length && s[0 .. pre.length] == pre;
}

private bool endsWith(scope const(char)[] s, scope const(char)[] suf) @nogc nothrow pure
{
    return s.length >= suf.length && s[$ - suf.length .. $] == suf;
}

/// Left-anchored kinds carry a literal prefix A (indexed by A).
private bool leftKeyed(PatKind k) @nogc nothrow
{
    return k == PatKind.exact || k == PatKind.prefix || k == PatKind.leftScan
        || k == PatKind.both || k == PatKind.bothScan;
}

/// Right-anchored kinds carry a literal suffix B (indexed by reverse(B)).
private bool rightKeyed(PatKind k) @nogc nothrow
{
    return k == PatKind.suffix || k == PatKind.rightScan
        || k == PatKind.both || k == PatKind.bothScan;
}

/// Both-anchored kinds go through the intersection (in bothLeft AND bothRight).
private bool bothAnchored(PatKind k) @nogc nothrow
{
    return k == PatKind.both || k == PatKind.bothScan;
}

/// Reverse `s` into `buf` (cleared first); `buf.data` is then the reversed bytes.
private void revInto(ref ByteBuffer buf, scope const(char)[] s) @nogc nothrow
{
    buf.clear();
    foreach_reverse (c; s)
        buf.appendByte(c);
}

/// Growable malloc'd array of PatEntry POINTERS. Entries are owned once (malloc'd
/// in insertPattern, freed in removePattern); a both-anchored pattern is
/// referenced from two buckets, so buckets store pointers and free() releases
/// only the array, never the entries.
private struct PtrBucket
{
    PatEntry** items;
    size_t len;
    size_t cap;

    void add(PatEntry* e) @nogc nothrow
    {
        if (len == cap)
        {
            cap = cap ? cap * 2 : 4;
            items = cast(PatEntry**) realloc(items, cap * (PatEntry*).sizeof);
            assert(items !is null, "out of memory");
        }
        items[len++] = e;
    }

    /// Remove by pointer identity (does not free the entry).
    bool remove(PatEntry* e) @nogc nothrow
    {
        foreach (i; 0 .. len)
        {
            if (items[i] is e)
            {
                items[i] = items[len - 1];
                len--;
                return true;
            }
        }
        return false;
    }

    /// Find the entry for (sub, pattern) by identity of its text; null if absent.
    PatEntry* find(Subscriber* s, scope const(char)[] pat) @nogc nothrow
    {
        foreach (i; 0 .. len)
            if (items[i].sub is s && items[i].pattern == pat)
                return items[i];
        return null;
    }

    void free() @nogc nothrow
    {
        if (items !is null)
            cfree(items);
        items = null;
        len = cap = 0;
    }
}

public struct PubSub
{
    private Dict!SubList channels; // channel -> subscribers
    // Anchor-indexed pattern matcher (see PUBSUB.md). Every pattern is split at
    // its outer metachars into a literal prefix A and suffix B. Single-anchor
    // patterns (A* / *B) live in leftOnly / rightOnly and deliver on one probe.
    // Both-anchored patterns (A*B, A*x*B, A?B) live in BOTH bothLeft (keyed by A)
    // and bothRight (keyed by reverse(B)); a publish probes each over the channel
    // prefixes / reversed-channel prefixes, then INTERSECTS by iterating the
    // SMALLER hit set and verifying the opposite anchor — so a shared anchor
    // never forces an O(P) walk. "scan" kinds then run globMatch over just that
    // small subgroup to check the middle. Only anchorless *x* patterns fall to a
    // global glob scan.
    private Dict!PtrBucket leftOnly; // A -> exact / prefix / leftScan
    private Dict!PtrBucket rightOnly; // reverse(B) -> suffix / rightScan
    private Dict!PtrBucket bothLeft; // A -> both / bothScan (also in bothRight)
    private Dict!PtrBucket bothRight; // reverse(B) -> both / bothScan
    private PtrBucket matchAll; // the bare "*"
    private PtrBucket fallback; // noAnchor: *x*, ?x?
    private size_t maxLeftOnlyLen, maxRightOnlyLen; // probe bounds per index
    private size_t maxBothLeftLen, maxBothRightLen;
    private size_t patternEntries; // total (subscriber, pattern) pairs
    // Active-channel index for PUBSUB CHANNELS <pat>: ordered (skiplist) sets of
    // channel names, normal and reversed, so a pattern's prefix/suffix becomes a
    // lexicographic range (find-left / find-right) instead of an O(channels) scan.
    private ZSet chanByName; // channel -> ordered by name
    private ZSet chanByRev; // reverse(channel) -> ordered by suffix
    private ByteBuffer scratch; // message staging, reused across publishes
    private ByteBuffer revScratch; // reversed channel staging for the right probe
    private ByteBuffer boundBuf; // upper-bound (prefix successor) staging

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
            activateChannel(channel); // first subscriber -> index the channel
        }
        list.add(s);
        return true;
    }

    private void activateChannel(scope const(char)[] ch) @nogc nothrow
    {
        chanByName.add(0, ch);
        revInto(revScratch, ch);
        chanByRev.add(0, cast(const(char)[]) revScratch.data);
    }

    private void deactivateChannel(scope const(char)[] ch) @nogc nothrow
    {
        chanByName.remove(ch);
        revInto(revScratch, ch);
        chanByRev.remove(cast(const(char)[]) revScratch.data);
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
            {
                channels.del(channel);
                deactivateChannel(channel);
            }
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
                {
                    channels.del(ch);
                    deactivateChannel(ch);
                }
            }
        }
        s.channels.clear();
        foreach (pat, ref u; s.patterns)
            removePattern(s, pat);
        s.patterns.clear();
    }

    // --- pattern index maintenance ---

    private PtrBucket* bucketFor(ref Dict!PtrBucket idx, scope const(char)[] key) @nogc nothrow
    {
        auto b = idx.get(key);
        if (b is null)
        {
            idx.set(key, PtrBucket());
            b = idx.get(key);
        }
        return b;
    }

    private void removeFrom(ref Dict!PtrBucket idx, scope const(char)[] key, PatEntry* e) @nogc nothrow
    {
        auto b = idx.get(key);
        if (b !is null)
        {
            b.remove(e);
            if (b.len == 0)
                idx.del(key); // frees the (now empty) bucket array only
        }
    }

    private void insertPattern(Subscriber* s, scope const(char)[] p) @nogc nothrow
    {
        uint aLen, bOff, bLen;
        auto kind = classify(p, aLen, bOff, bLen);
        auto e = cast(PatEntry*) malloc(PatEntry.sizeof);
        assert(e !is null, "out of memory");
        *e = PatEntry.init;
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

        if (bothAnchored(kind))
        {
            bucketFor(bothLeft, e.a).add(e);
            if (aLen > maxBothLeftLen)
                maxBothLeftLen = aLen;
            revInto(revScratch, e.b);
            bucketFor(bothRight, cast(const(char)[]) revScratch.data).add(e);
            if (bLen > maxBothRightLen)
                maxBothRightLen = bLen;
        }
        else if (leftKeyed(kind)) // exact / prefix / leftScan (bLen == 0)
        {
            bucketFor(leftOnly, e.a).add(e);
            if (aLen > maxLeftOnlyLen)
                maxLeftOnlyLen = aLen;
        }
        else if (rightKeyed(kind)) // suffix / rightScan (aLen == 0)
        {
            revInto(revScratch, e.b);
            bucketFor(rightOnly, cast(const(char)[]) revScratch.data).add(e);
            if (bLen > maxRightOnlyLen)
                maxRightOnlyLen = bLen;
        }
        else
            (kind == PatKind.matchAll ? matchAll : fallback).add(e);
        patternEntries++;
    }

    private void removePattern(Subscriber* s, scope const(char)[] p) @nogc nothrow
    {
        uint aLen, bOff, bLen;
        auto kind = classify(p, aLen, bOff, bLen);

        // Locate the shared entry via whichever index holds it.
        PatEntry* e;
        if (bothAnchored(kind))
        {
            if (auto b = bothLeft.get(p[0 .. aLen]))
                e = b.find(s, p);
        }
        else if (leftKeyed(kind))
        {
            if (auto b = leftOnly.get(p[0 .. aLen]))
                e = b.find(s, p);
        }
        else if (rightKeyed(kind))
        {
            revInto(revScratch, p[bOff .. bOff + bLen]);
            if (auto b = rightOnly.get(cast(const(char)[]) revScratch.data))
                e = b.find(s, p);
        }
        else
            e = (kind == PatKind.matchAll ? matchAll : fallback).find(s, p);
        if (e is null)
            return;

        // Unlink from every index referencing it, then free the entry once.
        if (bothAnchored(kind))
        {
            removeFrom(bothLeft, e.a, e);
            revInto(revScratch, e.b);
            removeFrom(bothRight, cast(const(char)[]) revScratch.data, e);
        }
        else if (leftKeyed(kind))
            removeFrom(leftOnly, e.a, e);
        else if (rightKeyed(kind))
        {
            revInto(revScratch, e.b);
            removeFrom(rightOnly, cast(const(char)[]) revScratch.data, e);
        }
        else
            (kind == PatKind.matchAll ? matchAll : fallback).remove(e);
        e.freeRaw();
        cfree(e);
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

    /// Total entries across `idx` buckets whose key is a prefix of `probe`.
    /// Used to pick the smaller side of the both-anchored intersection.
    private size_t anchorCount(ref Dict!PtrBucket idx, scope const(char)[] probe,
            size_t maxLen) @nogc nothrow
    {
        size_t n = 0;
        immutable kmax = probe.length < maxLen ? probe.length : maxLen;
        for (size_t k = 1; k <= kmax; k++)
            if (auto b = idx.get(probe[0 .. k]))
                n += b.len;
        return n;
    }

    /// Drive the both-anchored intersection from `driveIdx` (probed by
    /// `driveProbe`): verify the opposite anchor, run the middle glob for
    /// bothScan, deliver. fromLeft = driven by A (verify B is a suffix), else by B.
    private long bothDeliver(scope const(char)[] channel, scope const(char)[] payload,
            ref Dict!PtrBucket driveIdx, scope const(char)[] driveProbe, size_t driveMax,
            bool fromLeft) nothrow
    {
        long n = 0;
        immutable L = channel.length;
        immutable kmax = driveProbe.length < driveMax ? driveProbe.length : driveMax;
        for (size_t k = 1; k <= kmax; k++)
        {
            auto b = driveIdx.get(driveProbe[0 .. k]);
            if (b is null)
                continue;
            foreach (i; 0 .. b.len)
            {
                auto e = b.items[i];
                if (L < e.aLen + e.bLen) // anchors would overlap
                    continue;
                if (fromLeft ? !endsWith(channel, e.b) : !startsWith(channel, e.a))
                    continue; // opposite anchor fails
                if (e.kind == PatKind.bothScan && !globMatch(e.pattern, channel))
                    continue; // anchors pass, the middle does not
                emitPmessage(e.sub, e.pattern, channel, payload);
                n++;
            }
        }
        return n;
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

        // --- pattern delivery: anchor-indexed matching ---
        immutable L = channel.length;
        revInto(revScratch, channel); // reversed channel for every right-side probe
        auto cr = cast(const(char)[]) revScratch.data;

        // Left-only (exact / prefix / leftScan): probe the channel's prefixes.
        if (leftOnly.length)
        {
            immutable kmax = L < maxLeftOnlyLen ? L : maxLeftOnlyLen;
            for (size_t k = 1; k <= kmax; k++)
            {
                auto b = leftOnly.get(channel[0 .. k]);
                if (b is null)
                    continue;
                foreach (i; 0 .. b.len)
                {
                    auto e = b.items[i];
                    bool ok;
                    final switch (e.kind)
                    {
                    case PatKind.exact:
                        ok = L == k; // A == whole channel
                        break;
                    case PatKind.prefix:
                        ok = true;
                        break;
                    case PatKind.leftScan:
                        ok = globMatch(e.pattern, channel);
                        break;
                    case PatKind.suffix:
                    case PatKind.rightScan:
                    case PatKind.both:
                    case PatKind.bothScan:
                    case PatKind.matchAll:
                    case PatKind.noAnchor:
                        break; // never in leftOnly
                    }
                    if (ok)
                    {
                        emitPmessage(e.sub, e.pattern, channel, payload);
                        receivers++;
                    }
                }
            }
        }

        // Right-only (suffix / rightScan): probe the reversed channel's prefixes.
        if (rightOnly.length)
        {
            immutable kmax = L < maxRightOnlyLen ? L : maxRightOnlyLen;
            for (size_t k = 1; k <= kmax; k++)
            {
                auto b = rightOnly.get(cr[0 .. k]);
                if (b is null)
                    continue;
                foreach (i; 0 .. b.len)
                {
                    auto e = b.items[i];
                    bool ok;
                    final switch (e.kind)
                    {
                    case PatKind.suffix:
                        ok = true;
                        break;
                    case PatKind.rightScan:
                        ok = globMatch(e.pattern, channel);
                        break;
                    case PatKind.exact:
                    case PatKind.prefix:
                    case PatKind.leftScan:
                    case PatKind.both:
                    case PatKind.bothScan:
                    case PatKind.matchAll:
                    case PatKind.noAnchor:
                        break; // never in rightOnly
                    }
                    if (ok)
                    {
                        emitPmessage(e.sub, e.pattern, channel, payload);
                        receivers++;
                    }
                }
            }
        }

        // Both-anchored (A*B, A*x*B, A?B): intersect the A-hits and B-hits,
        // driven by the SMALLER side so a shared anchor is never an O(P) walk.
        if (bothLeft.length)
        {
            immutable leftN = anchorCount(bothLeft, channel, maxBothLeftLen);
            immutable rightN = anchorCount(bothRight, cr, maxBothRightLen);
            if (leftN != 0 && rightN != 0)
                receivers += (leftN <= rightN)
                    ? bothDeliver(channel, payload, bothLeft, channel, maxBothLeftLen, true)
                    : bothDeliver(channel, payload, bothRight, cr, maxBothRightLen, false);
        }

        // bare "*" matches everything
        foreach (i; 0 .. matchAll.len)
        {
            auto e = matchAll.items[i];
            emitPmessage(e.sub, e.pattern, channel, payload);
            receivers++;
        }

        // anchorless (*x*, ?x?): global glob scan (rare)
        foreach (i; 0 .. fallback.len)
        {
            auto e = fallback.items[i];
            if (globMatch(e.pattern, channel))
            {
                emitPmessage(e.sub, e.pattern, channel, payload);
                receivers++;
            }
        }
        return receivers;
    }

    // Smallest string strictly greater than every string with prefix `p`: p with
    // its last non-0xFF byte incremented. false if p is all 0xFF (no upper bound).
    private bool prefixSucc(ref ByteBuffer buf, scope const(char)[] p) @nogc nothrow
    {
        size_t cut = p.length;
        while (cut > 0 && cast(ubyte) p[cut - 1] == 0xFF)
            cut--;
        if (cut == 0)
            return false;
        buf.clear();
        foreach (i; 0 .. cut - 1)
            buf.appendByte(p[i]);
        buf.appendByte(cast(char)(cast(ubyte) p[cut - 1] + 1));
        return true;
    }

    /// PUBSUB CHANNELS [pattern]. Uses the ordered channel index: the pattern's
    /// literal prefix (or suffix) becomes a lexicographic range (find-left /
    /// find-right), then globMatch verifies the middle. Falls back to a full
    /// ordered walk only for anchorless patterns ("*", *x*).
    int eachChannel(scope const(char)[] pattern,
            scope int delegate(const(char)[] channel, size_t nsubs) @nogc nothrow dg) @nogc nothrow
    {
        if (pattern.length)
        {
            uint aLen, bOff, bLen;
            cast(void) classify(pattern, aLen, bOff, bLen);
            if (aLen > 0) // find-left: channels in [A, prefixSucc(A))
            {
                auto a = pattern[0 .. aLen];
                immutable hasHi = prefixSucc(boundBuf, a);
                auto hi = cast(const(char)[]) boundBuf.data;
                return chanByName.walkLexFrom(a, false, false, hi, true, !hasHi, (m, s) {
                    return globMatch(pattern, m) ? dg(m, channelSubCount(m)) : 0;
                });
            }
            if (bLen > 0) // find-right: reversed channels in [revB, prefixSucc(revB))
            {
                revInto(revScratch, pattern[bOff .. bOff + bLen]);
                auto rb = cast(const(char)[]) revScratch.data;
                immutable hasHi = prefixSucc(boundBuf, rb);
                auto hi = cast(const(char)[]) boundBuf.data;
                return chanByRev.walkLexFrom(rb, false, false, hi, true, !hasHi, (m, s) {
                    revInto(scratch, m); // un-reverse to the real channel name
                    auto ch = cast(const(char)[]) scratch.data;
                    return globMatch(pattern, ch) ? dg(ch, channelSubCount(ch)) : 0;
                });
            }
        }
        // no pattern, or anchorless: ordered full walk
        return chanByName.walkLexFrom(null, false, true, null, false, true, (m, s) {
            return (pattern.length == 0 || globMatch(pattern, m)) ? dg(m, channelSubCount(m)) : 0;
        });
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

unittest // multiple distinct-header patterns on ONE subscriber all match
{
    PubSub ps;
    FakeClient p;
    p.init_();
    scope (exit)
        p.sub.free();
    assert(ps.psubscribe(&p.sub, "a:*"));
    assert(ps.psubscribe(&p.sub, "b:*"));
    assert(ps.psubscribe(&p.sub, "c:*"));
    assert(ps.patternCount == 3);
    assert(ps.publish("a:1", "x") == 1); // each header must still be found
    assert(ps.publish("b:1", "x") == 1);
    assert(ps.publish("c:1", "x") == 1);
}

unittest // suffix (rightIndex), general (fallback) and match-all
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
    ps.psubscribe(&s.sub, "*:done"); // suffix -> rightIndex
    ps.psubscribe(&g.sub, "job.?"); // general (has '?') -> fallback
    ps.psubscribe(&all.sub, "*"); // match-all

    assert(ps.publish("task:done", "x") == 2); // suffix + all
    assert(s.got.length > 0 && all.got.length > 0);
    s.received.clear();
    all.received.clear();
    assert(ps.publish("job.7", "x") == 2); // general + all
    assert(g.got.length > 0);
    // suffix must NOT match a non-suffix channel
    assert(ps.publish("done:task", "x") == 1); // only match-all

    assert(ps.punsubscribe(&all.sub, "*"));
    assert(ps.publish("job.7", "y") == 1); // only general now
}

unittest // dual-anchor A*B intersection: BOTH anchors required, no overlap
{
    PubSub ps;
    FakeClient p;
    p.init_();
    scope (exit)
        p.sub.free();
    ps.psubscribe(&p.sub, "a*z"); // both: A="a", B="z"
    assert(ps.publish("abcz", "x") == 1); // prefix a + suffix z
    assert(ps.publish("az", "x") == 1); // star matches empty (len 2 >= 1+1)
    assert(ps.publish("abc", "x") == 0); // prefix a, but no z suffix
    assert(ps.publish("xyz", "x") == 0); // z suffix, but no a prefix
    assert(ps.publish("z", "x") == 0); // suffix only, len 1 < |a|+|z|
    assert(ps.publish("a", "x") == 0); // prefix only
}

unittest // sameheader: many A*B share header A; only the right tail fires
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
    ps.psubscribe(&a.sub, "sh:*:t1"); // all three share header "sh:"
    ps.psubscribe(&b.sub, "sh:*:t2");
    ps.psubscribe(&c.sub, "sh:*:t3");
    assert(ps.publish("sh:x:t2", "y") == 1); // only b (tail :t2), not a/c
    assert(b.got.length > 0 && a.got.length == 0 && c.got.length == 0);
    assert(ps.publish("sh:x:t9", "y") == 0); // header matches, no tail matches
    assert(ps.publish("other:t2", "y") == 0); // tail matches, header doesn't
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

unittest // matching edge cases: anchor overlap, star-eats-empty, multi-star, '?'
{
    static size_t hits(scope const(char)[] pat, scope const(char)[] chan)
    {
        PubSub ps;
        FakeClient p;
        p.init_();
        scope (exit)
            p.sub.free();
        ps.psubscribe(&p.sub, pat);
        return cast(size_t) ps.publish(chan, "x");
    }

    // A*B: the star matches zero chars, but the anchors must not overlap.
    assert(hits("a*z", "az") == 1); // star = "" (len 2 == |a|+|z|)
    assert(hits("a*z", "z") == 0); // len 1 < 2: 'a' has nowhere to sit
    assert(hits("a*z", "a") == 0); // no 'z' suffix
    assert(hits("ab*yz", "abyz") == 1); // touching anchors, star empty
    assert(hits("ab*yz", "abz") == 0); // len 3 < 4
    assert(hits("ab*ab", "abab") == 1); // A==B, overlap check keeps it honest
    assert(hits("ab*ab", "aba") == 0);

    // prefix/suffix stars eating the whole channel
    assert(hits("foo*", "foo") == 1); // trailing star = ""
    assert(hits("*foo", "foo") == 1); // leading star = ""
    assert(hits("foo*", "fo") == 0);
    assert(hits("*foo", "oo") == 0);

    // multi-star (bothScan): anchors gate, then globMatch checks the middle order
    assert(hits("foo*bar*baz", "fooXbarYbaz") == 1);
    assert(hits("foo*bar*baz", "foobarbaz") == 1); // both stars empty
    assert(hits("foo*bar*baz", "fooYbaz") == 0); // no "bar" in the middle
    assert(hits("foo*bar*baz", "foo:baz") == 0);
    assert(hits("a*b*c", "axbxc") == 1);
    assert(hits("a*b*c", "axc") == 0); // missing b

    // '?' matches exactly one char (bothScan / leftScan / rightScan)
    assert(hits("a?c", "abc") == 1);
    assert(hits("a?c", "ac") == 0); // ? needs one char
    assert(hits("a?c", "abbc") == 0); // ? is exactly one
    assert(hits("ab?", "abc") == 1); // leftScan
    assert(hits("ab?", "ab") == 0);
    assert(hits("?bc", "abc") == 1); // rightScan
    assert(hits("?bc", "bc") == 0);

    // channels containing literal metacharacters
    assert(hits("a?c", "a*c") == 1); // ? matches the literal '*'
    assert(hits("a*c", "a*c") == 1); // pattern star matches the literal '*'
    assert(hits("x*", "x*y") == 1);
}

unittest // exact-channel SUBSCRIBE: matched by equality; metachars are literal
{
    PubSub ps;
    FakeClient a, b, p;
    a.init_();
    b.init_();
    p.init_();
    scope (exit)
    {
        a.sub.free();
        b.sub.free();
        p.sub.free();
    }
    ps.subscribe(&a.sub, "foo:*"); // a channel literally NAMED "foo:*"
    ps.subscribe(&b.sub, "foo:bar");

    // exact channels match by equality — the name is never globbed
    assert(ps.publish("foo:*", "x") == 1); // only a
    assert(ps.publish("foo:bar", "x") == 1); // only b
    assert(ps.publish("foo:baz", "x") == 0); // neither
    assert(!ps.subscribe(&a.sub, "foo:*")); // dup on same sub

    // an exact and a pattern subscriber both fire on one publish
    ps.psubscribe(&p.sub, "foo:*"); // a real pattern
    a.received.clear();
    b.received.clear();
    p.received.clear();
    assert(ps.publish("foo:bar", "x") == 2); // b (exact) + p (pattern)
    assert(b.got.length > 0 && p.got.length > 0);
    assert(ps.publish("foo:*", "x") == 2); // a (exact "foo:*") + p (pattern matches)

    // '?' in an exact channel name is literal too
    ps.subscribe(&a.sub, "a?c");
    assert(ps.publish("a?c", "y") == 1); // exact
    assert(ps.publish("abc", "y") == 0); // not globbed

    // PUBSUB CHANNELS lists exact-subscribed channels; pattern-only never appears
    FakeClient q;
    q.init_();
    scope (exit)
        q.sub.free();
    ps.psubscribe(&q.sub, "ghost:*"); // pattern only -> no channel entry
    size_t g;
    ps.eachChannel("ghost:*", (ch, cnt) { g++; return 0; });
    assert(g == 0);

    // a channel leaves the index when its last exact subscriber unsubscribes
    ps.unsubscribe(&a.sub, "foo:*");
    size_t n;
    ps.eachChannel("foo:*", (ch, cnt) { n++; return 0; }); // glob -> foo:bar only
    assert(n == 1);
}

unittest // channel SubList dropped when the last subscriber leaves; shared stays
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
    ps.subscribe(&a.sub, "room");
    ps.subscribe(&b.sub, "room"); // two subscribers, one channel
    ps.subscribe(&a.sub, "aonly");
    assert(ps.channelSubCount("room") == 2);

    // one of two leaves: the list survives, channel stays indexed
    ps.unsubscribe(&a.sub, "room");
    assert(ps.channelSubCount("room") == 1);
    assert(ps.publish("room", "x") == 1);
    size_t n;
    ps.eachChannel("room", (ch, cnt) { n++; return 0; });
    assert(n == 1);

    // the last one leaves: list is dropped, channel gone from publish and index
    ps.unsubscribe(&b.sub, "room");
    assert(ps.channelSubCount("room") == 0);
    assert(ps.publish("room", "x") == 0);
    n = 0;
    ps.eachChannel("room", (ch, cnt) { n++; return 0; });
    assert(n == 0);
}

unittest // disconnect (dropAll): removes this sub everywhere; shared channels survive
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
    ps.subscribe(&a.sub, "shared");
    ps.subscribe(&b.sub, "shared"); // b is also on "shared"
    ps.subscribe(&a.sub, "aonly");
    ps.psubscribe(&a.sub, "a:*");
    ps.psubscribe(&b.sub, "b:*");

    ps.dropAll(&a.sub); // a disconnects

    assert(a.sub.subCount == 0);
    assert(ps.channelSubCount("aonly") == 0); // a's exclusive channel is gone
    assert(ps.channelSubCount("shared") == 1); // shared survives (b remains)
    assert(ps.publish("shared", "x") == 1); // delivered to b
    size_t n;
    ps.eachChannel(null, (ch, cnt) { n++; return 0; });
    assert(n == 1); // only "shared" is still indexed
    assert(ps.publish("a:x", "x") == 0); // a's pattern gone
    assert(ps.publish("b:x", "x") == 1); // b's pattern remains
    assert(ps.patternCount == 1);
}

unittest // complex interleaved metachars: foo:*:bar:?:num:*:baz
{
    static size_t hits(scope const(char)[] pat, scope const(char)[] chan)
    {
        PubSub ps;
        FakeClient p;
        p.init_();
        scope (exit)
            p.sub.free();
        ps.psubscribe(&p.sub, pat);
        return cast(size_t) ps.publish(chan, "x");
    }
    // A="foo:", B=":baz"; middle "*:bar:?:num:*" is verified by globMatch.
    enum pat = "foo:*:bar:?:num:*:baz";
    assert(hits(pat, "foo:aa:bar:c:num:dd:baz") == 1); // *=aa, ?=c, *=dd
    assert(hits(pat, "foo::bar:c:num::baz") == 1); // both stars eat empty
    assert(hits(pat, "foo:x:y:bar:z:num:w:baz") == 1); // first * eats "x:y"
    assert(hits(pat, "foo:aa:bar:cc:num:dd:baz") == 0); // '?' needs exactly one char
    assert(hits(pat, "foo:aa:bar::num:dd:baz") == 0); // '?' got zero chars
    assert(hits(pat, "foo:aa:XXX:c:num:dd:baz") == 0); // no ":bar:" segment
    assert(hits(pat, "xoo:aa:bar:c:num:dd:baz") == 0); // wrong prefix (anchor A)
    assert(hits(pat, "foo:aa:bar:c:num:dd:qux") == 0); // wrong suffix (anchor B)
    assert(hits(pat, "foo:aa:bar:c:num:dd:baz:x") == 0); // suffix must END the channel
    assert(hits(pat, "foo:bar:c:num:baz") == 0); // missing the literal ":bar:"/":num:" run
}

unittest // multiple subscribers, same pattern, all receive; dedup per subscriber
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
    assert(ps.psubscribe(&a.sub, "ev:*:hot"));
    assert(ps.psubscribe(&b.sub, "ev:*:hot")); // same pattern, different sub
    assert(!ps.psubscribe(&a.sub, "ev:*:hot")); // dup on the same sub
    assert(ps.publish("ev:x:hot", "y") == 2); // both a and b, once each
    assert(a.got.length > 0 && b.got.length > 0);
    assert(ps.punsubscribe(&a.sub, "ev:*:hot"));
    assert(ps.publish("ev:x:hot", "y") == 1); // only b remains
}

unittest // PUBSUB CHANNELS: find-left / find-right over the ordered channel index
{
    PubSub ps;
    FakeClient c;
    c.init_();
    scope (exit)
        c.sub.free();
    foreach (ch; ["news:sport", "news:tech", "chat:room1", "other:tech"])
        ps.subscribe(&c.sub, ch);

    static size_t count(ref PubSub p, const(char)[] pat)
    {
        size_t n;
        p.eachChannel(pat, (ch, cnt) { n++; return 0; });
        return n;
    }

    assert(count(ps, null) == 4); // all
    assert(count(ps, "news:*") == 2); // find-left prefix "news:"
    assert(count(ps, "*:tech") == 2); // find-right suffix ":tech" -> news:tech, other:tech
    assert(count(ps, "news:*ch") == 1); // find-left + glob middle -> news:tech only
    assert(count(ps, "*room1") == 1); // find-right -> chat:room1
    assert(count(ps, "nomatch:*") == 0); // empty prefix range
    assert(count(ps, "chat:room1") == 1); // exact
    // channel goes away when its last subscriber leaves
    ps.unsubscribe(&c.sub, "news:tech");
    assert(count(ps, "news:*") == 1); // only news:sport left
}

unittest // PUBSUB CHANNELS edge cases: prefix boundary, suffix vs delimiter, nesting
{
    PubSub ps;
    FakeClient c;
    c.init_();
    scope (exit)
        c.sub.free();
    foreach (ch; ["news", "news:", "news:x", "newsX", "a:tech", "tech", "z"])
        ps.subscribe(&c.sub, ch);

    static size_t count(ref PubSub p, const(char)[] pat)
    {
        size_t n;
        p.eachChannel(pat, (ch, cnt) { n++; return 0; });
        return n;
    }

    // find-left range must not leak past the prefix boundary. "news:*" covers
    // "news:" and "news:x" but NOT "news" (shorter) or "newsX" (past ':').
    assert(count(ps, "news:*") == 2); // news: , news:x
    assert(count(ps, "news*") == 4); // news, news:, news:x, newsX
    assert(count(ps, "news") == 1); // exact only

    // find-right: suffix with the delimiter is stricter than the bare word.
    assert(count(ps, "*:tech") == 1); // a:tech (has ":tech"), not "tech"
    assert(count(ps, "*tech") == 2); // a:tech and tech

    // "*" and null are all channels
    assert(count(ps, null) == 7);
    assert(count(ps, "*") == 7);

    // single-char channel at a range boundary
    assert(count(ps, "z*") == 1);
    assert(count(ps, "z") == 1);
    assert(count(ps, "zz*") == 0);
}
