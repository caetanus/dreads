module dreads.stream;

// Redis STREAM type, phase 1 (no consumer groups). Entries are kept in a
// sorted malloc'd array — IDs are monotonically increasing, so appends keep
// order and range queries binary-search the lower bound. Fully @nogc.

import core.stdc.stdlib : crealloc = realloc, cfree = free, malloc;

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

public struct Stream
{
    private SEntry* entries;
    private size_t len;
    private size_t cap;
    StreamID lastId; // survives even when all entries are gone

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
    }

    /// ID for XADD *: current ms, or lastId.ms with a bumped sequence when the
    /// clock has not advanced (or went backwards).
    StreamID nextId(ulong wallMs) const @nogc nothrow
    {
        if (wallMs > lastId.ms)
            return StreamID(wallMs, 0);
        return StreamID(lastId.ms, lastId.seq + 1);
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
        return true;
    }

    /// XTRIM MAXLEN: keeps only the newest maxlen entries; returns dropped count.
    size_t trimMaxLen(size_t maxlen) @nogc nothrow
    {
        import core.stdc.string : memmove;

        if (len <= maxlen)
            return 0;
        auto drop = len - maxlen;
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
