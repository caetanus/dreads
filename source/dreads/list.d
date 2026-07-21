module dreads.list;

// @nogc doubly-linked list with the payload inline in the node (one malloc
// per element). Backs the Redis LIST type. Plain data — call free() when done.

import core.stdc.string : memcpy;
import dreads.alloc : KeyspaceAllocator;

private struct Node
{
    Node* prev;
    Node* next;
    size_t len;
    // payload bytes follow the header

    inout(char)[] data() inout @nogc nothrow return
    {
        auto p = cast(inout(char)*)(cast(inout(ubyte)*)&this + Node.sizeof);
        return p[0 .. len];
    }
}

private Node* mkNode(scope const(char)[] v) @nogc nothrow @trusted
{
    auto n = cast(Node*) KeyspaceAllocator.instance.allocate(Node.sizeof + v.length).ptr;
    assert(n !is null, "out of memory");
    n.prev = n.next = null;
    n.len = v.length;
    if (v.length)
        memcpy(cast(ubyte*) n + Node.sizeof, v.ptr, v.length);
    return n;
}

// Node is a header + inline payload; its block size is Node.sizeof + len, which
// the node itself records — so a size-aware allocator gets the right bucket back.
private void freeNode(Node* n) @nogc nothrow @trusted
{
    KeyspaceAllocator.instance.deallocate((cast(void*) n)[0 .. Node.sizeof + n.len]);
}

public struct DList
{
    private Node* head;
    private Node* tail;
    private size_t count;
    // Lifetime queue counters (native queue metrics for the dashboard / a future
    // AMQP frontend): total items ever pushed / popped on THIS list. Free memory —
    // they fit the RObj union's existing slack (DList is 40B; SmallZSet sizes the
    // union at 128B). `count` is the live depth. Reset when the key is recreated.
    private ulong enq_;
    private ulong deq_;

    @property size_t length() const @nogc nothrow
    {
        return count;
    }

    @property ulong enqueued() const @nogc nothrow
    {
        return enq_;
    }

    @property ulong dequeued() const @nogc nothrow
    {
        return deq_;
    }

    void free() @nogc nothrow
    {
        auto n = head;
        while (n !is null)
        {
            auto next = n.next;
            freeNode(n);
            n = next;
        }
        head = tail = null;
        count = 0;
    }

    // Off-loop free support (lazyfree): record every backing block via `add`,
    // freeing NOTHING. The scattered next-pointer chase — the cache-miss-bound cost
    // of freeing a big list — runs on the free-thread; the loop later deallocates
    // the recorded blocks. Read-only: the list is left intact (its owner, a detached
    // RObj wrapper, is discarded after this returns). Same block size as freeNode.
    void gatherBlocks(scope void delegate(void*, size_t) @nogc nothrow add) @nogc nothrow @trusted
    {
        auto n = head;
        while (n !is null)
        {
            auto next = n.next;
            add(cast(void*) n, Node.sizeof + n.len);
            n = next;
        }
    }

    void pushFront(scope const(char)[] v) @nogc nothrow
    {
        auto n = mkNode(v);
        n.next = head;
        if (head !is null)
            head.prev = n;
        head = n;
        if (tail is null)
            tail = n;
        count++;
        enq_++;
    }

    void pushBack(scope const(char)[] v) @nogc nothrow
    {
        auto n = mkNode(v);
        n.prev = tail;
        if (tail !is null)
            tail.next = n;
        tail = n;
        if (head is null)
            head = n;
        count++;
        enq_++;
    }

    /// Valid only while the element stays in the list.
    const(char)[] front() const @nogc nothrow
    {
        assert(head !is null);
        return head.data;
    }

    const(char)[] back() const @nogc nothrow
    {
        assert(tail !is null);
        return tail.data;
    }

    void popFront() @nogc nothrow
    {
        assert(head !is null);
        auto n = head;
        head = n.next;
        if (head !is null)
            head.prev = null;
        else
            tail = null;
        freeNode(n);
        count--;
        deq_++;
    }

    void popBack() @nogc nothrow
    {
        assert(tail !is null);
        auto n = tail;
        tail = n.prev;
        if (tail !is null)
            tail.next = null;
        else
            head = null;
        freeNode(n);
        count--;
        deq_++;
    }

    /// Redis index: negative counts from the tail. Null when out of range.
    private inout(Node)* nodeAt(long idx) inout @nogc nothrow
    {
        if (idx < 0)
            idx += cast(long) count;
        if (idx < 0 || idx >= cast(long) count)
            return null;
        // inout locals cannot be re-seated; walk with a mutable pointer
        if (idx <= cast(long)(count / 2))
        {
            auto n = cast(Node*) head;
            foreach (_; 0 .. idx)
                n = n.next;
            return cast(inout(Node)*) n;
        }
        auto n = cast(Node*) tail;
        foreach (_; 0 .. cast(long) count - 1 - idx)
            n = n.prev;
        return cast(inout(Node)*) n;
    }

    const(char)[] at(long idx, out bool ok) const @nogc nothrow
    {
        auto n = nodeAt(idx);
        if (n is null)
            return null;
        ok = true;
        return n.data;
    }

    bool setAt(long idx, scope const(char)[] v) @nogc nothrow
    {
        auto n = cast(Node*) nodeAt(idx);
        if (n is null)
            return false;
        auto nn = mkNode(v);
        nn.prev = n.prev;
        nn.next = n.next;
        if (n.prev !is null)
            n.prev.next = nn;
        else
            head = nn;
        if (n.next !is null)
            n.next.prev = nn;
        else
            tail = nn;
        freeNode(n);
        return true;
    }

    /// LINSERT: inserts v before/after the first node equal to pivot.
    /// Returns the new length, or -1 when the pivot is absent.
    long insertAround(scope const(char)[] pivot, scope const(char)[] v, bool before) @nogc nothrow
    {
        for (auto n = head; n !is null; n = n.next)
        {
            if (n.data != pivot)
                continue;
            auto nn = mkNode(v);
            if (before)
            {
                nn.prev = n.prev;
                nn.next = n;
                if (n.prev !is null)
                    n.prev.next = nn;
                else
                    head = nn;
                n.prev = nn;
            }
            else
            {
                nn.prev = n;
                nn.next = n.next;
                if (n.next !is null)
                    n.next.prev = nn;
                else
                    tail = nn;
                n.next = nn;
            }
            count++;
            return cast(long) count;
        }
        return -1;
    }

    /// LREM semantics: rcount > 0 removes from head, < 0 from tail, 0 all.
    long remove(long rcount, scope const(char)[] v) @nogc nothrow
    {
        long removed = 0;
        long limit = rcount == 0 ? long.max : (rcount > 0 ? rcount : -rcount);
        bool fromTail = rcount < 0;
        auto n = fromTail ? tail : head;
        while (n !is null && removed < limit)
        {
            auto adjacent = fromTail ? n.prev : n.next;
            if (n.data == v)
            {
                if (n.prev !is null)
                    n.prev.next = n.next;
                else
                    head = n.next;
                if (n.next !is null)
                    n.next.prev = n.prev;
                else
                    tail = n.prev;
                freeNode(n);
                count--;
                removed++;
            }
            n = adjacent;
        }
        return removed;
    }

    /// Iterates elements [start .. start+n) of the already-normalized range.
    int walkRange(long start, size_t n,
            scope int delegate(const(char)[] v) @nogc nothrow dg) const @nogc nothrow
    {
        const(Node)* node = nodeAt(start);
        foreach (_; 0 .. n)
        {
            if (node is null)
                break;
            auto r = dg(node.data);
            if (r)
                return r;
            node = node.next;
        }
        return 0;
    }

    int opApply(scope int delegate(const(char)[] v) @nogc nothrow dg) const @nogc nothrow
    {
        for (const(Node)* n = head; n !is null; n = n.next)
        {
            auto r = dg(n.data);
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
    assert(l.length == 0 && l.head is null && l.tail is null);
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
