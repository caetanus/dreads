module dreads.stream;

// Redis STREAM type, phase 1 (no consumer groups). Entries are kept in a
// sorted malloc'd array — IDs are monotonically increasing, so appends keep
// order and range queries binary-search the lower bound. Fully @nogc.

import core.stdc.stdlib : crealloc = realloc, cfree = free, malloc;
import core.stdc.string : memmove;

import dreads.dict : Dict, Unit;
import dreads.mem : freeSlice, mallocDup;

public struct StreamID
{
    ulong ms;
    ulong seq;

    int opCmp(const StreamID o) const @nogc nothrow
    {
        if (ms != o.ms)
            return ms < o.ms ? -1 : 1;
        if (seq != o.seq)
            return seq < o.seq ? -1 : 1;
        return 0;
    }

    static StreamID minId() @nogc nothrow
    {
        return StreamID(0, 0);
    }

    static StreamID maxId() @nogc nothrow
    {
        return StreamID(ulong.max, ulong.max);
    }
}

/// Wall-clock milliseconds for auto-generated IDs. Only XADD * consumes this;
/// the AOF/replication log always records the resolved ID, so replay stays
/// deterministic.
public ulong nowMs() @nogc nothrow
{
    version (Posix)
    {
        import core.sys.posix.time : CLOCK_REALTIME, clock_gettime, timespec;

        timespec ts;
        clock_gettime(CLOCK_REALTIME, &ts);
        return cast(ulong) ts.tv_sec * 1000 + cast(ulong) ts.tv_nsec / 1_000_000;
    }
    else
    {
        import core.stdc.time : time;

        return cast(ulong) time(null) * 1000;
    }
}

public struct FieldPair
{
    const(char)[] field;
    const(char)[] value;
}

private struct SEntry
{
    StreamID id;
    FieldPair* pairs; // malloc'd; field/value strings are malloc'd copies
    size_t npairs;
}

/// One pending (delivered, unacknowledged) entry of a consumer group.
public struct PelEntry
{
    StreamID id;
    const(char)[] consumer; // slice of the group's consumers dict key: stable
    ulong deliveryTimeMs;
    ulong deliveryCount;
}

/// Per-consumer activity clocks (XINFO CONSUMERS idle/inactive).
public struct ConsumerInfo
{
    ulong seenTime; // last attempted interaction (any XREADGROUP/XCLAIM)
    ulong activeTime; // last successful read/claim of real data; 0 = never
}

/// Consumer group: last-delivered cursor + pending entries list (sorted).
public struct Group
{
    StreamID lastDelivered;
    long entriesRead; // XGROUP CREATE ENTRIESREAD; -1 = unknown, feeds XINFO lag
    PelEntry* pel;
    size_t plen;
    size_t pcap;
    Dict!ConsumerInfo consumers;

    void free() @nogc nothrow
    {
        if (pel !is null)
            cfree(pel);
        pel = null;
        plen = pcap = 0;
        consumers.free();
    }

    /// Independent deep copy — cursor, entries-read, consumers, and the PEL, with
    /// every copied PEL entry's `consumer` slice re-pointed into the COPY's own
    /// consumers dict (dict keys are stable). The result owns nothing of `this`,
    /// so it outlives the source group (COPY/MOVE then DEL of the source key).
    Group dup() const @nogc nothrow @trusted
    {
        auto src = cast(Group*)&this;
        Group g;
        g.lastDelivered = lastDelivered;
        g.entriesRead = entriesRead;
        foreach (ci; 0 .. src.consumers.capacity) // consumers first
            if (src.consumers.slotLive(ci))
                g.consumers.set(src.consumers.keyAt(ci), *src.consumers.valAt(ci));
        foreach (pe; src.pending) // then the PEL, pointing into the copy's dict
        {
            const(char)[] stable;
            foreach (ci; 0 .. g.consumers.capacity)
                if (g.consumers.slotLive(ci) && g.consumers.keyAt(ci) == pe.consumer)
                {
                    stable = g.consumers.keyAt(ci);
                    break;
                }
            g.pelSet(pe.id, stable, pe.deliveryTimeMs, pe.deliveryCount);
        }
        return g;
    }

    @property const(PelEntry)[] pending() const @nogc nothrow
    {
        return pel[0 .. plen];
    }

    /// Registers the consumer (if new) and bumps its seen-time; returns the
    /// dict-owned (stable) name slice.
    const(char)[] ensureConsumer(scope const(char)[] name, ulong now) @nogc nothrow
    {
        bool created;
        return ensureConsumer(name, now, created);
    }

    /// As above, reporting via `created` whether a new consumer was registered
    /// (so the caller can fire the `xgroup-createconsumer` keyspace event).
    const(char)[] ensureConsumer(scope const(char)[] name, ulong now, out bool created) @nogc nothrow
    {
        if (auto ci = consumers.get(name))
            ci.seenTime = now;
        else
        {
            consumers.set(name, ConsumerInfo(now, 0));
            created = true;
        }
        foreach (i; 0 .. consumers.capacity)
        {
            if (consumers.slotLive(i) && consumers.keyAt(i) == name)
                return consumers.keyAt(i);
        }
        return null; // unreachable
    }

    /// Marks a consumer as having read/claimed real data (active-time).
    void markActive(scope const(char)[] name, ulong now) @nogc nothrow
    {
        if (auto ci = consumers.get(name))
            ci.activeTime = now;
    }

    private size_t pelLowerBound(StreamID id) const @nogc nothrow
    {
        size_t lo = 0, hi = plen;
        while (lo < hi)
        {
            auto mid = lo + (hi - lo) / 2;
            if (pel[mid].id < id)
                lo = mid + 1;
            else
                hi = mid;
        }
        return lo;
    }

    /// Adds or reassigns a pending entry.
    void pelSet(StreamID id, scope const(char)[] consumer, ulong nowMillis,
            ulong deliveryCount) @nogc nothrow
    {
        auto i = pelLowerBound(id);
        if (i < plen && pel[i].id == id)
        {
            pel[i].consumer = consumer;
            pel[i].deliveryTimeMs = nowMillis;
            pel[i].deliveryCount = deliveryCount;
            return;
        }
        if (plen == pcap)
        {
            pcap = pcap ? pcap * 2 : 8;
            pel = cast(PelEntry*) crealloc(pel, pcap * PelEntry.sizeof);
            assert(pel !is null, "out of memory");
        }
        if (i < plen)
            memmove(&pel[i + 1], &pel[i], (plen - i) * PelEntry.sizeof);
        pel[i] = PelEntry(id, consumer, nowMillis, deliveryCount);
        plen++;
    }

    ptrdiff_t pelFind(StreamID id) const @nogc nothrow
    {
        auto i = pelLowerBound(id);
        return i < plen && pel[i].id == id ? cast(ptrdiff_t) i : -1;
    }

    bool pelRemove(StreamID id) @nogc nothrow
    {
        auto i = pelFind(id);
        if (i < 0)
            return false;
        if (cast(size_t) i + 1 < plen)
            memmove(&pel[i], &pel[i + 1], (plen - i - 1) * PelEntry.sizeof);
        plen--;
        return true;
    }
}

public struct Stream
{
    private SEntry* entries;
    private size_t len;
    private size_t cap;
    StreamID lastId; // survives even when all entries are gone
    ulong entriesAdded; // total ever XADDed (monotonic; XINFO entries-added)
    StreamID maxDeletedId; // greatest id ever XDELeted (XINFO max-deleted-entry-id)
    Dict!Group groups; // consumer groups by name

    /// XINFO recorded-first-entry-id: the id of the oldest live entry, or 0-0.
    @property StreamID recordedFirstId() const @nogc nothrow
    {
        return len ? entries[0].id : StreamID(0, 0);
    }

    @property size_t length() const @nogc nothrow
    {
        return len;
    }

    void free() @nogc nothrow
    {
        foreach (i; 0 .. len)
        {
            foreach (j; 0 .. entries[i].npairs)
            {
                freeSlice(entries[i].pairs[j].field);
                freeSlice(entries[i].pairs[j].value);
            }
            cfree(entries[i].pairs);
        }
        if (entries !is null)
            cfree(entries);
        entries = null;
        len = cap = 0;
        lastId = StreamID(0, 0);
        entriesAdded = 0;
        maxDeletedId = StreamID(0, 0);
        groups.free();
    }

    /// Independent deep copy — entries, id/counter metadata, and every consumer
    /// group (each via `Group.dup`). Used by `RObj.deepDup` for COPY/MOVE. The
    /// result shares no memory with `this`.
    Stream dup() const @nogc nothrow @trusted
    {
        auto src = cast(Stream*)&this;
        Stream s;
        src.walkRange(StreamID.minId, StreamID.maxId, 0, (id, pairs) {
            s.add(id, pairs);
            return 0;
        });
        s.lastId = lastId; // survives an empty stream
        s.entriesAdded = entriesAdded;
        s.maxDeletedId = maxDeletedId;
        foreach (gi; 0 .. src.groups.capacity)
            if (src.groups.slotLive(gi))
                s.groups.set(src.groups.keyAt(gi), src.groups.valAt(gi).dup());
        return s;
    }

    /// Fetches one entry's fields; returns dg's result, or -1 when absent.
    int getEntry(StreamID id,
            scope int delegate(const(FieldPair)[] pairs) @nogc nothrow dg) const @nogc nothrow
    {
        auto i = lowerBound(id);
        if (i >= len || entries[i].id != id)
            return -1;
        return dg(entries[i].pairs[0 .. entries[i].npairs]);
    }

    /// Newest live entry (XINFO last-entry); returns dg's result, or -1 if empty.
    int getLast(scope int delegate(StreamID id, const(FieldPair)[] pairs) @nogc nothrow dg) const @nogc nothrow
    {
        if (len == 0)
            return -1;
        auto e = &entries[len - 1];
        return dg(e.id, e.pairs[0 .. e.npairs]);
    }

    /// First existing entry id strictly greater than after; ok=false at end.
    StreamID nextAfter(StreamID after, out bool ok) const @nogc nothrow
    {
        auto start = after.seq == ulong.max ? StreamID(after.ms + 1, 0) : StreamID(after.ms,
                after.seq + 1);
        auto i = lowerBound(start);
        if (i >= len)
            return StreamID(0, 0);
        ok = true;
        return entries[i].id;
    }

    /// Count of live entries with id strictly greater than `after` — a consumer
    /// group's lag (how many entries it has not yet been delivered). Sorted array
    /// ⇒ one binary search.
    size_t countAfter(StreamID after) const @nogc nothrow
    {
        auto start = after.seq == ulong.max ? StreamID(after.ms + 1, 0) : StreamID(after.ms,
                after.seq + 1);
        return len - lowerBound(start);
    }

    /// ID for XADD *: current ms, or lastId.ms with a bumped sequence when the
    /// clock has not advanced (or went backwards). When the sequence is exhausted
    /// at lastId.ms (e.g. a future timestamp with seq=u64max), roll over into the
    /// next millisecond rather than overflowing the sequence.
    StreamID nextId(ulong wallMs) const @nogc nothrow
    {
        if (wallMs > lastId.ms)
            return StreamID(wallMs, 0);
        if (lastId.seq == ulong.max)
            return StreamID(lastId.ms + 1, 0);
        return StreamID(lastId.ms, lastId.seq + 1);
    }

    /// ID for XADD `ms-*` (explicit ms, auto sequence). seq=0 when ms is ahead of
    /// lastId.ms; the next sequence when equal (fails on u64 exhaustion); fails
    /// when ms < lastId.ms (can't produce an increasing id). Empty stream keeps
    /// lastId=0-0, so `0-*` on a fresh stream yields 0-1.
    bool nextSeqForMs(ulong ms, out StreamID id) @nogc nothrow
    {
        if (ms > lastId.ms)
        {
            id = StreamID(ms, 0);
            return true;
        }
        if (ms == lastId.ms)
        {
            if (lastId.seq == ulong.max)
                return false; // sequence exhausted for this ms
            id = StreamID(ms, lastId.seq + 1);
            return true;
        }
        return false; // ms < lastId.ms
    }

    /// Appends one entry, copying the pairs. Fails unless id > lastId.
    bool add(StreamID id, scope const(FieldPair)[] src) @nogc nothrow
    {
        if (id <= lastId)
            return false;
        if (len == cap)
        {
            cap = cap ? cap * 2 : 8;
            entries = cast(SEntry*) crealloc(entries, cap * SEntry.sizeof);
            assert(entries !is null, "out of memory");
        }
        auto e = &entries[len];
        e.id = id;
        e.npairs = src.length;
        e.pairs = cast(FieldPair*) malloc(src.length * FieldPair.sizeof);
        assert(e.pairs !is null, "out of memory");
        foreach (i, ref p; src)
        {
            e.pairs[i].field = mallocDup(p.field);
            e.pairs[i].value = mallocDup(p.value);
        }
        len++;
        lastId = id;
        entriesAdded++;
        return true;
    }

    /// XDEL: removes one exact id, compacting the array. lastId is untouched.
    bool removeId(StreamID id) @nogc nothrow
    {
        import core.stdc.string : memmove;

        auto i = lowerBound(id);
        if (i >= len || entries[i].id != id)
            return false;
        freeEntry(i);
        if (i + 1 < len)
            memmove(&entries[i], &entries[i + 1], (len - i - 1) * SEntry.sizeof);
        len--;
        if (id > maxDeletedId) // XINFO max-deleted-entry-id (XDEL only, not trim)
            maxDeletedId = id;
        return true;
    }

    /// XTRIM MAXLEN: keeps only the newest maxlen entries. `limit` (0 = unlimited)
    /// caps how many are dropped in one call (the ~ ... LIMIT form). Returns the
    /// dropped count.
    size_t trimMaxLen(size_t maxlen, size_t limit = 0) @nogc nothrow
    {
        if (len <= maxlen)
            return 0;
        auto drop = len - maxlen;
        if (limit && drop > limit)
            drop = limit;
        return dropOldest(drop);
    }

    /// XTRIM MINID: drops entries with id < minid (oldest first). `limit`
    /// (0 = unlimited) caps how many are dropped. Returns the dropped count.
    size_t trimMinId(StreamID minid, size_t limit = 0) @nogc nothrow
    {
        auto drop = lowerBound(minid); // entries with id strictly below minid
        if (limit && drop > limit)
            drop = limit;
        return dropOldest(drop);
    }

    /// Drop the `drop` oldest entries, compacting the array. Shared by the trims.
    private size_t dropOldest(size_t drop) @nogc nothrow
    {
        import core.stdc.string : memmove;

        if (drop == 0)
            return 0;
        foreach (i; 0 .. drop)
            freeEntry(i);
        if (drop < len)
            memmove(entries, entries + drop, (len - drop) * SEntry.sizeof);
        len -= drop;
        return drop;
    }

    private void freeEntry(size_t i) @nogc nothrow
    {
        foreach (j; 0 .. entries[i].npairs)
        {
            entries[i].pairs[j].field.freeSlice;
            entries[i].pairs[j].value.freeSlice;
        }
        cfree(entries[i].pairs);
    }

    /// First index with id >= target.
    private size_t lowerBound(StreamID target) const @nogc nothrow
    {
        size_t lo = 0, hi = len;
        while (lo < hi)
        {
            auto mid = lo + (hi - lo) / 2;
            if (entries[mid].id < target)
                lo = mid + 1;
            else
                hi = mid;
        }
        return lo;
    }

    /// Iterates entries with start <= id <= end, at most count (0 = no limit).
    int walkRange(StreamID start, StreamID end, size_t count,
            scope int delegate(StreamID id, const(FieldPair)[] pairs) @nogc nothrow dg) const @nogc nothrow
    {
        size_t emitted = 0;
        for (auto i = lowerBound(start); i < len; i++)
        {
            if (entries[i].id > end)
                break;
            if (count && emitted == count)
                break;
            auto r = dg(entries[i].id, entries[i].pairs[0 .. entries[i].npairs]);
            if (r)
                return r;
            emitted++;
        }
        return 0;
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

unittest // ordering, lastId, rejection of non-increasing IDs
{
    Stream s;
    scope (exit)
        s.free();
    FieldPair[1] p = [FieldPair("k", "v")];
    assert(s.add(StreamID(5, 0), p[]));
    assert(s.add(StreamID(5, 1), p[]));
    assert(s.add(StreamID(9, 0), p[]));
    assert(!s.add(StreamID(9, 0), p[])); // equal
    assert(!s.add(StreamID(5, 2), p[])); // smaller ms
    assert(s.length == 3);
    assert(s.lastId == StreamID(9, 0));
}

unittest // nextId semantics
{
    Stream s;
    scope (exit)
        s.free();
    assert(s.nextId(100) == StreamID(100, 0));
    FieldPair[1] p = [FieldPair("a", "b")];
    s.add(StreamID(100, 0), p[]);
    assert(s.nextId(100) == StreamID(100, 1)); // same ms bumps seq
    assert(s.nextId(50) == StreamID(100, 1)); // clock went backwards
    assert(s.nextId(101) == StreamID(101, 0));
}

unittest // range queries
{
    Stream s;
    scope (exit)
        s.free();
    foreach (i; 1 .. 6) // ids 1-0 .. 5-0
    {
        FieldPair[1] p = [FieldPair("n", "x")];
        s.add(StreamID(i, 0), p[]);
    }
    size_t n;
    ulong firstMs, lastMs;
    s.walkRange(StreamID(2, 0), StreamID(4, ulong.max), 0, (id, pairs) {
        if (n == 0)
            firstMs = id.ms;
        lastMs = id.ms;
        n++;
        return 0;
    });
    assert(n == 3 && firstMs == 2 && lastMs == 4);

    n = 0;
    s.walkRange(StreamID.minId, StreamID.maxId, 2, (id, pairs) { n++; return 0; });
    assert(n == 2); // COUNT honored

    n = 0;
    s.walkRange(StreamID(5, 1), StreamID.maxId, 0, (id, pairs) { n++; return 0; });
    assert(n == 0); // past the end

    // exclusive read pattern used by XREAD: everything > (3,0)
    n = 0;
    s.walkRange(StreamID(3, 1), StreamID.maxId, 0, (id, pairs) { n++; return 0; });
    assert(n == 2);
}

unittest // multiple field pairs are copied and retrievable
{
    Stream s;
    scope (exit)
        s.free();
    FieldPair[2] p = [FieldPair("name", "alice"), FieldPair("age", "30")];
    s.add(StreamID(1, 0), p[]);
    bool checked;
    s.walkRange(StreamID.minId, StreamID.maxId, 0, (id, pairs) {
        assert(pairs.length == 2);
        assert(pairs[0].field == "name" && pairs[0].value == "alice");
        assert(pairs[1].field == "age" && pairs[1].value == "30");
        checked = true;
        return 0;
    });
    assert(checked);
}
