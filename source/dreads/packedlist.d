module dreads.packedlist;

// Listpack-lite packed segment for the Redis LIST type. The whole point (proven
// by the Phase-1 cache-miss A/B — see list-deque-refactor-plan): store the element
// BYTES INLINE in one contiguous block, NOT a container of pointers-to-heap. Then
// LRANGE streams values in cache-line order (≈0 value-miss) and there is NO
// per-element malloc — one block per segment. A Segment is the whole list while it
// is small; PackedList (below, Phase 2b) chains Segments into a quicklist so both
// ends stay O(1) for arbitrarily large lists.
//
// Entry layout, packed back-to-back between [head, tail):
//     [len : u32] [bytes : len] [len : u32]
// The length is stored at BOTH ends (prefix + trailer) so a forward walk reads the
// prefix and a reverse walk (popBack/back) reads the trailer — O(1) at both ends.
// `head` is a byte cursor: popFront just advances it (no memmove); the dead front
// gap is reclaimed by compaction when it grows past half the block.

import core.stdc.string : memmove, memcpy;
import dreads.alloc : KeyspaceAllocator;

private enum uint HDR = 4; // one u32 length field (there are two per entry)
private enum uint OVH = 2 * HDR; // per-entry overhead (prefix + trailer)

private uint rd32(scope const(ubyte)* p) @nogc nothrow @trusted
{
    return *cast(const(uint)*) p; // amd64 + arm64 (our only targets) allow unaligned loads
}

private void wr32(scope ubyte* p, uint v) @nogc nothrow @trusted
{
    *cast(uint*) p = v;
}

struct Segment
{
    private ubyte* buf;
    private uint cap; // bytes allocated
    private uint head; // byte offset of the first entry
    private uint tail; // one past the last entry's trailer (== used end)
    private uint cnt; // number of entries

    // Copyable like emplace.Deque (so DList/RObj stay copyable — the RObj union is
    // never actually copied, only moved, but the type must remain copyable). A copy
    // DEEP-copies the block, so even an accidental copy can't double-free.
    this(this) @nogc nothrow @trusted
    {
        if (buf)
        {
            auto nblk = KeyspaceAllocator.instance.allocate(cap);
            assert(nblk.ptr !is null, "packed segment copy failed");
            memcpy(nblk.ptr, buf, cap);
            buf = cast(ubyte*) nblk.ptr;
        }
    }

    @property size_t length() const @nogc nothrow { return cnt; }
    @property bool empty() const @nogc nothrow { return cnt == 0; }
    /// Live payload+overhead bytes (what the quicklist caps a node by).
    @property uint usedBytes() const @nogc nothrow { return tail - head; }

    /// Bytes a value of length `n` will occupy (payload + the two length fields).
    static uint entryCost(size_t n) @nogc nothrow { return cast(uint)(OVH + n); }

    private void ensureTail(uint need) @nogc nothrow @trusted
    {
        if (tail + need <= cap)
            return;
        // Reclaiming the dead front gap may be enough — cheaper than growing.
        immutable live = tail - head;
        if (head > 0 && live + need <= cap)
        {
            if (live)
                memmove(buf, buf + head, live); // overlapping: shift live bytes to front
            tail = live;
            head = 0;
            return;
        }
        // Grow via allocate + copy + deallocate — NOT the allocator's reallocate:
        // on the composed KeyspaceAllocator, reallocate of a large region-tier block
        // crashes in its internal deallocate (region reclaim is not realloc-safe).
        // allocate/deallocate are the paths every other large value uses. Copying to
        // the front of the fresh block also closes any dead front gap for free.
        uint nc = cap ? cap * 2 : 64;
        while (nc < live + need)
            nc *= 2;
        auto nblk = KeyspaceAllocator.instance.allocate(nc);
        assert(nblk.ptr !is null, "packed segment grow failed");
        auto nb = cast(ubyte*) nblk.ptr;
        if (live)
            memcpy(nb, buf + head, live); // disjoint fresh block
        if (buf)
            KeyspaceAllocator.instance.deallocate((cast(void*) buf)[0 .. cap]);
        buf = nb;
        cap = nc;
        tail = live;
        head = 0;
    }

    void pushBack(scope const(char)[] v) @nogc nothrow @trusted
    {
        immutable len = cast(uint) v.length;
        ensureTail(OVH + len);
        wr32(buf + tail, len);
        if (len)
            (cast(ubyte*) buf)[tail + HDR .. tail + HDR + len] = cast(const(ubyte)[]) v;
        wr32(buf + tail + HDR + len, len);
        tail += OVH + len;
        cnt++;
    }

    void pushFront(scope const(char)[] v) @nogc nothrow @trusted
    {
        immutable len = cast(uint) v.length;
        immutable need = OVH + len;
        if (head < need)
        {
            // No front slack. Make room: compact toward the far end if the block is
            // big enough, else grow (the new block gets front headroom == need).
            immutable live = tail - head;
            if (cap >= live + need)
            {
                // slide live bytes to the top of the block, opening a front gap
                immutable ntail = cap;
                immutable nhead = cap - live;
                if (live)
                    memmove(buf + nhead, buf + head, live); // overlapping: slide to top
                head = nhead;
                tail = ntail;
            }
            else
            {
                uint nc = cap ? cap * 2 : 64;
                while (nc < live + need)
                    nc *= 2;
                auto nblk = KeyspaceAllocator.instance.allocate(nc);
                assert(nblk.ptr !is null, "packed segment alloc failed");
                auto nb = cast(ubyte*) nblk.ptr;
                immutable nhead = nc - live;
                if (live)
                    memcpy(nb + nhead, buf + head, live); // disjoint fresh block
                if (buf)
                    KeyspaceAllocator.instance.deallocate((cast(void*) buf)[0 .. cap]);
                buf = nb;
                cap = nc;
                head = nhead;
                tail = nc;
            }
        }
        head -= need;
        wr32(buf + head, len);
        if (len)
            (cast(ubyte*) buf)[head + HDR .. head + HDR + len] = cast(const(ubyte)[]) v;
        wr32(buf + head + HDR + len, len);
        cnt++;
    }

    /// Valid only until the next mutation.
    const(char)[] front() const @nogc nothrow @trusted
    {
        assert(cnt > 0);
        immutable len = rd32(buf + head);
        return cast(const(char)[]) buf[head + HDR .. head + HDR + len];
    }

    const(char)[] back() const @nogc nothrow @trusted
    {
        assert(cnt > 0);
        immutable len = rd32(buf + tail - HDR);
        return cast(const(char)[]) buf[tail - HDR - len .. tail - HDR];
    }

    void popFront() @nogc nothrow @trusted
    {
        assert(cnt > 0);
        immutable len = rd32(buf + head);
        head += OVH + len;
        cnt--;
        if (cnt == 0)
            head = tail = 0; // fully drained: reset cursors so the block is reused clean
    }

    void popBack() @nogc nothrow @trusted
    {
        assert(cnt > 0);
        immutable len = rd32(buf + tail - HDR);
        tail -= OVH + len;
        cnt--;
        if (cnt == 0)
            head = tail = 0;
    }

    /// Sequential forward walk — O(1) per step (read prefix, advance). This is the
    /// cache-streaming path LRANGE/opApply use; do NOT random-index in a loop.
    int opApply(scope int delegate(const(char)[] v) @nogc nothrow dg) const @nogc nothrow @trusted
    {
        uint off = head;
        foreach (_; 0 .. cnt)
        {
            immutable len = rd32(buf + off);
            auto r = dg(cast(const(char)[]) buf[off + HDR .. off + HDR + len]);
            if (r)
                return r;
            off += OVH + len;
        }
        return 0;
    }

    /// Sequential walk of [start .. start+cnt) in ONE forward pass — O(start+cnt),
    /// NOT quadratic. Backs LRANGE. `start` is a validated logical index.
    int walkRange(size_t start, size_t cnt,
            scope int delegate(const(char)[] v) @nogc nothrow dg) const @nogc nothrow @trusted
    {
        uint off = head;
        foreach (_; 0 .. start)
            off += OVH + rd32(buf + off);
        immutable end = start + cnt <= this.cnt ? start + cnt : this.cnt;
        foreach (_; start .. end)
        {
            immutable len = rd32(buf + off);
            auto r = dg(cast(const(char)[]) buf[off + HDR .. off + HDR + len]);
            if (r)
                return r;
            off += OVH + len;
        }
        return 0;
    }

    /// Random access (LINDEX/LSET) — O(i) forward walk. Bounded per segment.
    const(char)[] opIndex(size_t i) const @nogc nothrow @trusted
    {
        assert(i < cnt);
        uint off = head;
        foreach (_; 0 .. i)
            off += OVH + rd32(buf + off);
        immutable len = rd32(buf + off);
        return cast(const(char)[]) buf[off + HDR .. off + HDR + len];
    }

    void free() @nogc nothrow @trusted
    {
        if (buf)
            KeyspaceAllocator.instance.deallocate((cast(void*) buf)[0 .. cap]);
        buf = null;
        cap = head = tail = cnt = 0;
    }

    ~this() @nogc nothrow @trusted { free(); }

    /// Off-loop lazyfree: report the single backing block, freeing nothing.
    void gatherBlocks(scope void delegate(void*, size_t) @nogc nothrow add) const @nogc nothrow @trusted
    {
        if (buf)
            add(cast(void*) buf, cap);
    }
}

// --- tests ----------------------------------------------------------------------

unittest // push/pop both ends, ordering
{
    Segment s;
    s.pushBack("b");
    s.pushFront("a");
    s.pushBack("c"); // a b c
    assert(s.length == 3);
    assert(s.front == "a" && s.back == "c");
    s.popFront();
    assert(s.front == "b"); // b c
    s.popBack();
    assert(s.back == "b" && s.length == 1);
    s.popFront();
    assert(s.empty && s.length == 0);
}

unittest // index + opApply order after mixed ends
{
    Segment s;
    foreach (v; ["3", "2", "1"])
        s.pushFront(v); // 1 2 3
    foreach (v; ["4", "5"])
        s.pushBack(v); // 1 2 3 4 5
    assert(s.length == 5);
    foreach (i, exp; ["1", "2", "3", "4", "5"])
        assert(s[i] == exp);
    char[8] got;
    size_t n;
    foreach (v; s)
        got[n++] = v[0];
    assert(got[0 .. n] == "12345");
}

unittest // empty values and varied lengths
{
    Segment s;
    s.pushBack("");
    s.pushBack("hello world this is a longer value");
    s.pushFront("");
    assert(s.length == 3);
    assert(s[0] == "" && s[2] == "hello world this is a longer value");
    assert(s.front == "" && s.back == "hello world this is a longer value");
}

unittest // drain-to-empty resets and the block is reusable
{
    Segment s;
    foreach (i; 0 .. 100)
        s.pushBack("xxxxxxxx");
    foreach (i; 0 .. 100)
        s.popFront();
    assert(s.empty);
    // reuse after drain (head/tail reset to 0)
    s.pushBack("y");
    assert(s.front == "y" && s.length == 1);
}

unittest // heavy front-cursor churn (popFront advances head, compaction reclaims)
{
    Segment s;
    // queue pattern: push many, pop from front, keep pushing — exercises the head
    // cursor + compaction path without unbounded growth
    foreach (round; 0 .. 50)
    {
        foreach (i; 0 .. 20)
            s.pushBack("payload16bytes!!");
        foreach (i; 0 .. 20)
            s.popFront();
    }
    assert(s.empty);
    s.pushBack("z");
    assert(s.back == "z");
}

unittest // pushFront-heavy (LPUSH) grows the front gap correctly
{
    Segment s;
    foreach (i; 0 .. 200)
        s.pushFront("abc"); // all "abc", reverse insertion => still all "abc"
    assert(s.length == 200);
    foreach (i; 0 .. 200)
        assert(s[i] == "abc");
    // and reverse via popBack
    foreach (i; 0 .. 200)
    {
        assert(s.back == "abc");
        s.popBack();
    }
    assert(s.empty);
}

unittest // gatherBlocks reports exactly the one live block
{
    Segment s;
    foreach (i; 0 .. 10)
        s.pushBack("data");
    size_t blocks;
    s.gatherBlocks((p, n) { blocks++; });
    assert(blocks == 1);
}
