module dreads.zset;

// Sorted set: skiplist ordered by (score, member) with per-level spans for
// O(log n) rank queries — the same layout as Redis's zskiplist — plus a
// member->score dict for O(1) ZSCORE. Fully @nogc, plain data, call free().

import core.stdc.stdlib : malloc, cfree = free, rand;
import core.stdc.string : memcmp, memcpy;

import dreads.dict : Dict, DoubleVal;

private enum MAX_LEVEL = 32;

private struct Level
{
    ZNode* forward;
    size_t span;
}

private struct ZNode
{
    double score;
    ZNode* backward;
    size_t memberLen;
    ubyte level;
    // Level[level] follows, then member bytes; nodes never move, so the
    // computed slices below stay valid for the node's lifetime.

    inout(Level)[] levels() inout @nogc nothrow return
    {
        auto p = cast(inout(Level)*)(cast(inout(ubyte)*)&this + ZNode.sizeof);
        return p[0 .. level];
    }

    inout(char)[] member() inout @nogc nothrow return
    {
        auto p = cast(inout(char)*)(cast(inout(ubyte)*)&this + ZNode.sizeof + Level.sizeof * level);
        return p[0 .. memberLen];
    }
}

private ZNode* mkNode(ubyte lvl, double score, scope const(char)[] member) @nogc nothrow
{
    auto n = cast(ZNode*) malloc(ZNode.sizeof + Level.sizeof * lvl + member.length);
    assert(n !is null, "out of memory");
    n.score = score;
    n.backward = null;
    n.memberLen = member.length;
    n.level = lvl;
    foreach (ref l; n.levels)
    {
        l.forward = null;
        l.span = 0;
    }
    if (member.length)
        memcpy(cast(void*) n.member.ptr, member.ptr, member.length);
    return n;
}

private int cmpMember(scope const(char)[] a, scope const(char)[] b) @nogc nothrow
{
    auto minl = a.length < b.length ? a.length : b.length;
    if (minl)
    {
        auto c = memcmp(a.ptr, b.ptr, minl);
        if (c)
            return c < 0 ? -1 : 1;
    }
    if (a.length == b.length)
        return 0;
    return a.length < b.length ? -1 : 1;
}

/// True when (s1, m1) orders before (s2, m2).
private bool nodeLess(double s1, scope const(char)[] m1, double s2, scope const(char)[] m2) @nogc nothrow
{
    if (s1 != s2)
        return s1 < s2;
    return cmpMember(m1, m2) < 0;
}

private ubyte randomLevel() @nogc nothrow
{
    ubyte lvl = 1;
    while (lvl < MAX_LEVEL && (rand() & 0xFFFF) < 0.25 * 0xFFFF)
        lvl++;
    return lvl;
}

public struct ZSet
{
    private ZNode* header; // sentinel with MAX_LEVEL levels, created lazily
    private ZNode* tailN;
    private int levelCount = 0; // levels in use (0 until first insert)
    private size_t count;
    private Dict!DoubleVal scores;

    @property size_t length() const @nogc nothrow
    {
        return count;
    }

    void free() @nogc nothrow
    {
        auto n = header;
        while (n !is null)
        {
            auto next = n.levels[0].forward;
            cfree(n);
            n = next;
        }
        header = tailN = null;
        levelCount = 0;
        count = 0;
        scores.free();
    }

    bool score(scope const(char)[] member, out double s) const @nogc nothrow
    {
        auto v = scores.get(member);
        if (v is null)
            return false;
        s = v.d;
        return true;
    }

    /// Adds member or updates its score. Returns true when newly added.
    bool add(double s, scope const(char)[] member) @nogc nothrow
    {
        auto existing = scores.get(member);
        if (existing !is null)
        {
            if (existing.d != s)
            {
                sklDelete(existing.d, member);
                sklInsert(s, member);
                existing.d = s;
            }
            return false;
        }
        sklInsert(s, member);
        scores.set(member, DoubleVal(s));
        return true;
    }

    bool remove(scope const(char)[] member) @nogc nothrow
    {
        auto existing = scores.get(member);
        if (existing is null)
            return false;
        sklDelete(existing.d, member);
        scores.del(member);
        return true;
    }

    /// 0-based rank in ascending order; ok=false when absent.
    size_t rank(scope const(char)[] member, out bool ok) const @nogc nothrow
    {
        auto existing = scores.get(member);
        if (existing is null)
            return 0;
        auto s = existing.d;
        size_t traversed = 0;
        auto x = cast(ZNode*) header;
        for (int i = levelCount - 1; i >= 0; i--)
        {
            while (x.levels[i].forward !is null
                    && !nodeLess(s, member, x.levels[i].forward.score, x.levels[i].forward.member))
            {
                traversed += x.levels[i].span;
                x = x.levels[i].forward;
                if (x.memberLen == member.length && cmpMember(x.member, member) == 0)
                {
                    ok = true;
                    return traversed - 1; // spans count from the header sentinel
                }
            }
        }
        return 0; // unreachable when the dict and skiplist agree
    }

    /// Node at 0-based ascending rank, or null.
    private inout(ZNode)* nodeByRank(size_t r) inout @nogc nothrow
    {
        if (r >= count)
            return null;
        size_t target = r + 1; // header is rank 0 internally
        size_t traversed = 0;
        auto x = cast(ZNode*) header;
        for (int i = levelCount - 1; i >= 0; i--)
        {
            while (x.levels[i].forward !is null && traversed + x.levels[i].span <= target)
            {
                traversed += x.levels[i].span;
                x = x.levels[i].forward;
            }
            if (traversed == target)
                return cast(inout(ZNode)*) x;
        }
        return null;
    }

    /// Iterates ranks [start .. start+n), ascending (rev=false) or descending
    /// over the reversed view (rev=true: rank 0 is the highest element).
    int walkRange(size_t start, size_t n, bool rev,
            scope int delegate(const(char)[] member, double score) @nogc nothrow dg) const @nogc nothrow
    {
        if (start >= count)
            return 0;
        auto node = nodeByRank(rev ? count - 1 - start : start);
        foreach (_; 0 .. n)
        {
            if (node is null)
                break;
            auto r = dg(node.member, node.score);
            if (r)
                return r;
            node = rev ? node.backward : node.levels[0].forward;
            if (node is header)
                break;
        }
        return 0;
    }

    /// Iterates members with min <= score <= max (bounds optionally exclusive).
    int walkScoreRange(double min, bool minExcl, double max, bool maxExcl,
            scope int delegate(const(char)[] member, double score) @nogc nothrow dg) const @nogc nothrow
    {
        if (count == 0)
            return 0;
        // descend to the last node below the lower bound
        auto x = cast(ZNode*) header;
        for (int i = levelCount - 1; i >= 0; i--)
        {
            while (x.levels[i].forward !is null
                    && (minExcl ? x.levels[i].forward.score <= min : x.levels[i].forward.score < min))
                x = x.levels[i].forward;
        }
        auto node = x.levels[0].forward;
        while (node !is null)
        {
            if (maxExcl ? node.score >= max : node.score > max)
                break;
            auto r = dg(node.member, node.score);
            if (r)
                return r;
            node = node.levels[0].forward;
        }
        return 0;
    }

    private void sklInsert(double s, scope const(char)[] member) @nogc nothrow
    {
        if (header is null)
        {
            header = mkNode(MAX_LEVEL, 0, null);
            levelCount = 1;
        }
        ZNode*[MAX_LEVEL] update = void;
        size_t[MAX_LEVEL] rankAt = void;
        auto x = header;
        for (int i = levelCount - 1; i >= 0; i--)
        {
            rankAt[i] = i == levelCount - 1 ? 0 : rankAt[i + 1];
            while (x.levels[i].forward !is null
                    && nodeLess(x.levels[i].forward.score, x.levels[i].forward.member, s, member))
            {
                rankAt[i] += x.levels[i].span;
                x = x.levels[i].forward;
            }
            update[i] = x;
        }
        auto lvl = randomLevel();
        if (lvl > levelCount)
        {
            foreach (i; levelCount .. lvl)
            {
                rankAt[i] = 0;
                update[i] = header;
                update[i].levels[i].span = count;
            }
            levelCount = lvl;
        }
        auto n = mkNode(lvl, s, member);
        foreach (i; 0 .. lvl)
        {
            n.levels[i].forward = update[i].levels[i].forward;
            update[i].levels[i].forward = n;
            n.levels[i].span = update[i].levels[i].span - (rankAt[0] - rankAt[i]);
            update[i].levels[i].span = rankAt[0] - rankAt[i] + 1;
        }
        foreach (i; lvl .. levelCount)
            update[i].levels[i].span++;
        n.backward = update[0] is header ? null : update[0];
        if (n.levels[0].forward !is null)
            n.levels[0].forward.backward = n;
        else
            tailN = n;
        count++;
    }

    private void sklDelete(double s, scope const(char)[] member) @nogc nothrow
    {
        ZNode*[MAX_LEVEL] update = void;
        auto x = header;
        for (int i = levelCount - 1; i >= 0; i--)
        {
            while (x.levels[i].forward !is null
                    && nodeLess(x.levels[i].forward.score, x.levels[i].forward.member, s, member))
                x = x.levels[i].forward;
            update[i] = x;
        }
        auto n = x.levels[0].forward;
        if (n is null || n.score != s || cmpMember(n.member, member) != 0)
            return; // not found; dict and skiplist out of sync would be a bug
        foreach (i; 0 .. levelCount)
        {
            if (update[i].levels[i].forward is n)
            {
                update[i].levels[i].span += n.levels[i].span - 1;
                update[i].levels[i].forward = n.levels[i].forward;
            }
            else
                update[i].levels[i].span--;
        }
        if (n.levels[0].forward !is null)
            n.levels[0].forward.backward = n.backward;
        else
            tailN = n.backward;
        while (levelCount > 1 && header.levels[levelCount - 1].forward is null)
            levelCount--;
        cfree(n);
        count--;
    }
}

unittest // add, score, update, remove
{
    ZSet z;
    scope (exit)
        z.free();
    assert(z.add(2, "b"));
    assert(z.add(1, "a"));
    assert(z.add(3, "c"));
    assert(!z.add(10, "b")); // update, not add
    assert(z.length == 3);
    double s;
    assert(z.score("b", s) && s == 10);
    assert(!z.score("nope", s));
    assert(z.remove("b"));
    assert(!z.remove("b"));
    assert(z.length == 2);
}

unittest // ordering by (score, member) and ranks
{
    ZSet z;
    scope (exit)
        z.free();
    z.add(1, "b");
    z.add(1, "a"); // same score: lexicographic
    z.add(0.5, "c");
    z.add(2, "d");
    // expected ascending order: c(0.5) a(1) b(1) d(2)
    bool ok;
    assert(z.rank("c", ok) == 0 && ok);
    assert(z.rank("a", ok) == 1);
    assert(z.rank("b", ok) == 2);
    assert(z.rank("d", ok) == 3);
    ok = false;
    z.rank("x", ok);
    assert(!ok);

    char[8] got;
    size_t n;
    z.walkRange(0, 4, false, (m, s) { got[n++] = m[0]; return 0; });
    assert(got[0 .. n] == "cabd");
    n = 0;
    z.walkRange(0, 4, true, (m, s) { got[n++] = m[0]; return 0; });
    assert(got[0 .. n] == "dbac");
    n = 0;
    z.walkRange(1, 2, false, (m, s) { got[n++] = m[0]; return 0; });
    assert(got[0 .. n] == "ab");
}

unittest // score ranges with inclusive/exclusive bounds
{
    ZSet z;
    scope (exit)
        z.free();
    foreach (i; 0 .. 10)
    {
        char[2] m = [cast(char)('a' + i), 0];
        z.add(i, m[0 .. 1]);
    }
    char[16] got;
    size_t n;
    z.walkScoreRange(2, false, 5, false, (m, s) { got[n++] = m[0]; return 0; });
    assert(got[0 .. n] == "cdef"); // scores 2..5 inclusive
    n = 0;
    z.walkScoreRange(2, true, 5, true, (m, s) { got[n++] = m[0]; return 0; });
    assert(got[0 .. n] == "de"); // (2..5) exclusive
    n = 0;
    z.walkScoreRange(-double.infinity, false, double.infinity, false, (m, s) {
        n++;
        return 0;
    });
    assert(n == 10);
}

unittest // rank stays correct across many inserts and deletes
{
    import std.conv : to;
    import std.format : format;

    ZSet z;
    scope (exit)
        z.free();
    foreach (i; 0 .. 500)
        z.add(i, format("m%03d", i));
    bool ok;
    foreach (i; 0 .. 500)
        assert(z.rank(format("m%03d", i), ok) == i && ok);
    // remove every even member; odd ranks collapse
    foreach (i; 0 .. 500)
        if (i % 2 == 0)
            assert(z.remove(format("m%03d", i)));
    assert(z.length == 250);
    foreach (i; 0 .. 500)
        if (i % 2 == 1)
            assert(z.rank(format("m%03d", i), ok) == i / 2 && ok);
}
