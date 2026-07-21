module dreads.list;

// Redis LIST backed by a listpack-lite packed Segment (dreads.packedlist): the
// element BYTES live INLINE in one contiguous block — no per-element node/malloc and
// no pointer-to-heap per value. This is the Phase-2 win the cache-miss A/B pinned
// (see list-deque-refactor-plan): the old linked list and the Deque!StrVal both paid
// one value-cache-miss per element because the value sat OUTSIDE the container;
// packing it inline makes LRANGE stream in cache-line order and drops the per-element
// allocation. Push/pop stay O(1) amortized at both ends (a head byte-cursor makes
// popFront allocation-free; a front gap feeds pushFront). LINSERT/LREM/LSET-resize
// rebuild (O(n), rare — accepted). free()/gatherBlocks touch ONE block, not N.
//
// PHASE 2a: a single Segment is the whole list (small/medium — the 90% case). PHASE
// 2b (follow-up) promotes huge lists to a Deque!Segment quicklist so a giant list
// isn't one giant contiguous block; the Segment is reused as the node there.

import std.algorithm.mutation : swap;
import dreads.packedlist : Segment;

public struct DList
{
    private Segment seg;
    // Lifetime queue counters (native queue metrics; free — DList still fits the
    // RObj union's SmallZSet-sized slack). `seg.length` is the live depth.
    private ulong enq_;
    private ulong deq_;

    @property size_t length() const @nogc nothrow @trusted
    {
        return seg.length;
    }

    @property ulong enqueued() const @nogc nothrow
    {
        return enq_;
    }

    @property ulong dequeued() const @nogc nothrow
    {
        return deq_;
    }

    /// Release the packed block. The RObj union free() dispatch calls this (the
    /// Segment's ~this is never auto-run for a union member). Leaves seg empty +
    /// reusable (the next push re-allocates).
    void free() @nogc nothrow @trusted
    {
        seg.free();
    }

    /// Off-loop lazyfree: record the one backing block, freeing NOTHING. The RObj
    /// shell is discarded by the caller, so ~this never runs.
    void gatherBlocks(scope void delegate(void*, size_t) @nogc nothrow add) @nogc nothrow @trusted
    {
        seg.gatherBlocks(add);
    }

    void pushFront(scope const(char)[] v) @nogc nothrow @trusted
    {
        seg.pushFront(v);
        enq_++;
    }

    void pushBack(scope const(char)[] v) @nogc nothrow @trusted
    {
        seg.pushBack(v);
        enq_++;
    }

    /// Valid only while the element stays in the list (until the next mutation).
    const(char)[] front() const @nogc nothrow @trusted
    {
        assert(seg.length > 0);
        return seg.front;
    }

    const(char)[] back() const @nogc nothrow @trusted
    {
        assert(seg.length > 0);
        return seg.back;
    }

    void popFront() @nogc nothrow @trusted
    {
        assert(seg.length > 0);
        seg.popFront();
        deq_++;
    }

    void popBack() @nogc nothrow @trusted
    {
        assert(seg.length > 0);
        seg.popBack();
        deq_++;
    }

    // Redis index (negative counts from the tail) -> logical [0, len); -1 if OOB.
    private long logical(long idx) const @nogc nothrow @trusted
    {
        immutable n = cast(long) seg.length;
        if (idx < 0)
            idx += n;
        return (idx < 0 || idx >= n) ? -1 : idx;
    }

    const(char)[] at(long idx, out bool ok) const @nogc nothrow @trusted
    {
        immutable i = logical(idx);
        if (i < 0)
            return null;
        ok = true;
        return seg[cast(size_t) i];
    }

    /// LSET: same-length values overwrite in place; a resize rebuilds (O(n), rare).
    bool setAt(long idx, scope const(char)[] v) @nogc nothrow @trusted
    {
        immutable i = logical(idx);
        if (i < 0)
            return false;
        immutable ii = cast(size_t) i;
        Segment ns;
        foreach (j; 0 .. seg.length)
            ns.pushBack(j == ii ? v : seg[j]);
        swap(seg, ns); // ns (old block) freed at scope exit
        return true;
    }

    /// LINSERT: insert v before/after the first element equal to pivot. Packed rep
    /// has no middle insert -> rebuild (O(n), rare). Returns the new length, or -1.
    long insertAround(scope const(char)[] pivot, scope const(char)[] v, bool before) @nogc nothrow @trusted
    {
        immutable n = seg.length;
        long idx = -1;
        foreach (i; 0 .. n)
            if (seg[i] == pivot)
            {
                idx = cast(long) i;
                break;
            }
        if (idx < 0)
            return -1;
        immutable size_t insertAt = before ? cast(size_t) idx : cast(size_t)(idx + 1);
        Segment ns;
        foreach (i; 0 .. n)
        {
            if (i == insertAt)
                ns.pushBack(v);
            ns.pushBack(seg[i]);
        }
        if (insertAt == n)
            ns.pushBack(v);
        swap(seg, ns);
        return cast(long)(n + 1);
    }

    /// LREM: rcount > 0 removes from head, < 0 from tail, 0 all. Rebuild keeping the
    /// survivors; a from-tail pass builds reversed then the swap restores order.
    long remove(long rcount, scope const(char)[] v) @nogc nothrow @trusted
    {
        immutable n = seg.length;
        immutable long limit = rcount == 0 ? long.max : (rcount > 0 ? rcount : -rcount);
        immutable fromTail = rcount < 0;
        long removed = 0;
        Segment ns;
        if (fromTail)
        {
            foreach_reverse (i; 0 .. n)
            {
                if (removed < limit && seg[i] == v)
                    removed++; // drop
                else
                    ns.pushFront(seg[i]); // pushFront preserves original order
            }
        }
        else
        {
            foreach (i; 0 .. n)
            {
                if (removed < limit && seg[i] == v)
                    removed++;
                else
                    ns.pushBack(seg[i]);
            }
        }
        swap(seg, ns);
        return removed;
    }

    /// Iterates elements [start .. start+cnt) (start may be negative, Redis-style).
    int walkRange(long start, size_t cnt,
            scope int delegate(const(char)[] v) @nogc nothrow dg) const @nogc nothrow @trusted
    {
        immutable n = cast(long) seg.length;
        long s = start < 0 ? start + n : start;
        if (s < 0 || s >= n)
            return 0;
        return seg.walkRange(cast(size_t) s, cnt, dg);
    }

    int opApply(scope int delegate(const(char)[] v) @nogc nothrow dg) const @nogc nothrow @trusted
    {
        return seg.opApply(dg);
    }
}

unittest // push/pop both ends
{
    DList l;
    scope (exit)
        l.free();
    l.pushBack("b");
    l.pushFront("a");
    l.pushBack("c");
    assert(l.length == 3);
    assert(l.front == "a" && l.back == "c");
    l.popFront();
    assert(l.front == "b");
    l.popBack();
    assert(l.back == "b" && l.length == 1);
    l.popBack();
    assert(l.length == 0);
}

unittest // indexing, negative indexes, setAt
{
    DList l;
    scope (exit)
        l.free();
    foreach (v; ["zero", "one", "two", "three"])
        l.pushBack(v);
    bool ok;
    assert(l.at(0, ok) == "zero" && ok);
    assert(l.at(3, ok) == "three");
    assert(l.at(-1, ok) == "three");
    assert(l.at(-4, ok) == "zero");
    ok = false;
    l.at(4, ok);
    assert(!ok);
    l.at(-5, ok);
    assert(!ok);
    assert(l.setAt(1, "ONE"));
    assert(l.at(1, ok) == "ONE");
    assert(l.setAt(-1, "THREE"));
    assert(l.back == "THREE");
    assert(!l.setAt(10, "nope"));
}

unittest // LSET resize (new value a different length) keeps order + bytes
{
    DList l;
    scope (exit)
        l.free();
    foreach (v; ["a", "bb", "ccc"])
        l.pushBack(v);
    assert(l.setAt(1, "LONGER-VALUE"));
    bool ok;
    assert(l.at(0, ok) == "a");
    assert(l.at(1, ok) == "LONGER-VALUE");
    assert(l.at(2, ok) == "ccc");
    assert(l.length == 3);
}

unittest // LREM semantics
{
    DList l;
    scope (exit)
        l.free();
    foreach (v; ["x", "a", "x", "b", "x", "c"])
        l.pushBack(v);
    assert(l.remove(1, "x") == 1); // first from head
    bool ok;
    assert(l.at(0, ok) == "a");
    assert(l.remove(-1, "x") == 1); // first from tail
    assert(l.length == 4);
    assert(l.remove(0, "x") == 1); // all remaining
    assert(l.length == 3);
    char[8] all;
    size_t n;
    foreach (v; l)
        all[n++] = v[0];
    assert(all[0 .. n] == "abc");
}

unittest // LINSERT
{
    DList l;
    scope (exit)
        l.free();
    foreach (v; ["a", "b", "d"])
        l.pushBack(v);
    assert(l.insertAround("b", "c", false) == 4); // after b
    assert(l.insertAround("a", "A", true) == 5); // before a
    char[8] got;
    size_t n;
    foreach (v; l)
        got[n++] = v[0];
    assert(got[0 .. n] == "Aabcd");
    assert(l.insertAround("zz", "x", true) == -1); // pivot absent
}

unittest // walkRange
{
    DList l;
    scope (exit)
        l.free();
    foreach (v; ["0", "1", "2", "3", "4"])
        l.pushBack(v);
    char[8] got;
    size_t n;
    l.walkRange(1, 3, (v) { got[n++] = v[0]; return 0; });
    assert(got[0 .. n] == "123");
    // negative start + over-long count clamps
    n = 0;
    l.walkRange(-2, 10, (v) { got[n++] = v[0]; return 0; });
    assert(got[0 .. n] == "34");
}

unittest // queue counters + reuse after free
{
    DList l;
    scope (exit)
        l.free();
    l.pushBack("a");
    l.pushBack("b");
    l.pushFront("c");
    l.popFront();
    assert(l.enqueued == 3 && l.dequeued == 1 && l.length == 2);
}

unittest // large round-trip preserves order and drains clean
{
    DList l;
    scope (exit)
        l.free();
    foreach (i; 0 .. 1000)
    {
        char[8] b;
        import core.stdc.stdio : snprintf;

        auto n = snprintf(b.ptr, b.length, "%d", i);
        l.pushBack(cast(const(char)[]) b[0 .. n]);
    }
    assert(l.length == 1000);
    size_t k;
    foreach (v; l)
    {
        char[8] b;
        import core.stdc.stdio : snprintf;

        auto n = snprintf(b.ptr, b.length, "%d", cast(int) k);
        assert(v == cast(const(char)[]) b[0 .. n]);
        k++;
    }
    foreach (i; 0 .. 1000)
        l.popFront();
    assert(l.length == 0);
}
