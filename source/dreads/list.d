module dreads.list;

// Redis LIST backed by an emplace ring Deque (no hand-rolled node malloc/free).
// The element is StrVal (owns its bytes via .free() convention, KeyspaceAllocator);
// the Deque disposes each element with .free() and manages its ONE ring block with
// RAII — so there is NO per-node manual free. Copy semantics MATCH SmallHash exactly
// (shallow value copy, safe because an RObj is never copied, only moved); free() is
// the union's manual dispatch. LINSERT/LREM rebuild (Deque has no middle ops) — rare,
// non-hot — using MOVES (no shallow-copy hazard). See list-deque-refactor-plan.

import std.algorithm.mutation : swap;
import dreads.alloc : KeyspaceAllocator;
import dreads.dict : StrVal;
import emplace.deque : Deque;

private alias Ring = Deque!(StrVal, KeyspaceAllocator);

// Transfer ownership of a StrVal out of a slot: hand back its bytes and NULL the
// source so it is never disposed twice. core.lifetime.move can't do this — StrVal
// is __traits(isPOD) (no dtor) yet owns memory via .free(), so `move` shallow-copies
// WITHOUT nulling the source, aliasing the pointer ⇒ use-after-free on the rebuild.
private StrVal takeOut(ref StrVal s) @nogc nothrow
{
    StrVal r = s; // bitwise copy (shares the pointer for an instant)
    s = StrVal.init; // null the source: it now owns nothing, disposes to a no-op
    return r; // the caller (the new ring) is the sole owner
}

public struct DList
{
    private Ring q;
    // Lifetime queue counters (native queue metrics; free — DList still fits the
    // RObj union's SmallZSet-sized slack). `q.length` is the live depth.
    private ulong enq_;
    private ulong deq_;

    @property size_t length() const @nogc nothrow @trusted
    {
        return q.length;
    }

    @property ulong enqueued() const @nogc nothrow
    {
        return enq_;
    }

    @property ulong dequeued() const @nogc nothrow
    {
        return deq_;
    }

    /// Release the ring + every element. The RObj union free() dispatch calls this
    /// (the Deque's ~this is never auto-run for a union member). clearShrink disposes
    /// each StrVal (.free()) then frees the ring block; leaves `q` empty + reusable.
    void free() @nogc nothrow @trusted
    {
        q.clearShrink();
    }

    /// Off-loop lazyfree: record the ring block + each element's bytes, freeing
    /// NOTHING. The RObj shell is discarded by the caller, so ~this never runs.
    void gatherBlocks(scope void delegate(void*, size_t) @nogc nothrow add) @nogc nothrow @trusted
    {
        q.gatherBlocks(add);
    }

    void pushFront(scope const(char)[] v) @nogc nothrow @trusted
    {
        q.pushFront(StrVal.ofRaw(v)); // ofRaw: byte-exact + stable rawView (no int-encode)
        enq_++;
    }

    void pushBack(scope const(char)[] v) @nogc nothrow @trusted
    {
        q.pushBack(StrVal.ofRaw(v));
        enq_++;
    }

    /// Valid only while the element stays in the list.
    const(char)[] front() const @nogc nothrow @trusted
    {
        assert(q.length > 0);
        return q.front.rawView();
    }

    const(char)[] back() const @nogc nothrow @trusted
    {
        assert(q.length > 0);
        return q.back.rawView();
    }

    void popFront() @nogc nothrow @trusted
    {
        assert(q.length > 0);
        q.popFront(); // disposeElem -> StrVal.free()
        deq_++;
    }

    void popBack() @nogc nothrow @trusted
    {
        assert(q.length > 0);
        q.popBack();
        deq_++;
    }

    // Redis index (negative counts from the tail) -> logical [0, len); -1 if OOB.
    private long logical(long idx) const @nogc nothrow @trusted
    {
        immutable n = cast(long) q.length;
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
        return q[cast(size_t) i].rawView();
    }

    bool setAt(long idx, scope const(char)[] v) @nogc nothrow @trusted
    {
        immutable i = logical(idx);
        if (i < 0)
            return false;
        q[cast(size_t) i].free(); // release the old bytes
        q[cast(size_t) i] = StrVal.ofRaw(v); // bitwise-assign the new (owns fresh bytes)
        return true;
    }

    /// LINSERT: insert v before/after the first element equal to pivot. Deque has no
    /// middle insert -> rebuild (O(n), rare). Returns the new length, or -1 if absent.
    long insertAround(scope const(char)[] pivot, scope const(char)[] v, bool before) @nogc nothrow @trusted
    {
        immutable n = q.length;
        long idx = -1;
        foreach (i; 0 .. n)
            if (q[i].rawView() == pivot)
            {
                idx = cast(long) i;
                break;
            }
        if (idx < 0)
            return -1;
        immutable size_t insertAt = before ? cast(size_t) idx : cast(size_t)(idx + 1);
        Ring nq;
        foreach (i; 0 .. n)
        {
            if (i == insertAt)
                nq.pushBack(StrVal.ofRaw(v));
            nq.pushBack(takeOut(q[i])); // transfer ownership; q[i] becomes StrVal.init
        }
        if (insertAt == n)
            nq.pushBack(StrVal.ofRaw(v));
        swap(q, nq); // q gets the new ring; nq (old content: dropped elems + ring) dies at scope exit
        return cast(long)(n + 1);
    }

    /// LREM: rcount > 0 removes from head, < 0 from tail, 0 all. Rebuild keeping the
    /// survivors (moved); the DROPPED elements stay in `q` and are freed by q.~this
    /// during the swap — so no explicit per-element free.
    long remove(long rcount, scope const(char)[] v) @nogc nothrow @trusted
    {
        immutable n = q.length;
        immutable long limit = rcount == 0 ? long.max : (rcount > 0 ? rcount : -rcount);
        immutable fromTail = rcount < 0;
        long removed = 0;
        Ring nq;
        if (fromTail)
        {
            foreach_reverse (i; 0 .. n)
            {
                if (removed < limit && q[i].rawView() == v)
                    removed++; // drop: leave in q (freed by q.~this at swap)
                else
                    nq.pushFront(takeOut(q[i])); // pushFront preserves original order
            }
        }
        else
        {
            foreach (i; 0 .. n)
            {
                if (removed < limit && q[i].rawView() == v)
                    removed++;
                else
                    nq.pushBack(takeOut(q[i]));
            }
        }
        swap(q, nq);
        return removed;
    }

    /// Iterates elements [start .. start+cnt) (start may be negative, Redis-style).
    int walkRange(long start, size_t cnt,
            scope int delegate(const(char)[] v) @nogc nothrow dg) const @nogc nothrow @trusted
    {
        immutable n = cast(long) q.length;
        long s = start < 0 ? start + n : start;
        if (s < 0 || s >= n)
            return 0;
        foreach (k; 0 .. cnt)
        {
            immutable size_t i = cast(size_t) s + k;
            if (i >= q.length)
                break;
            auto r = dg(q[i].rawView());
            if (r)
                return r;
        }
        return 0;
    }

    int opApply(scope int delegate(const(char)[] v) @nogc nothrow dg) const @nogc nothrow @trusted
    {
        foreach (i; 0 .. q.length)
        {
            auto r = dg(q[i].rawView());
            if (r)
                return r;
        }
        return 0;
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
