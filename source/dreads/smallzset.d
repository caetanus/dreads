module dreads.smallzset;

// A sorted set with the same small-size optimization as SmallSet/SmallHash:
// below the Redis zset-max-listpack thresholds the member bytes live packed in
// a contiguous blob, with a parallel index of (offset, score) kept SORTED by
// (score, member) — so rank/range walks are a slice of the sorted index and a
// membership/score lookup is a cache-resident linear scan. Past the threshold
// it spills one-way to the skiplist-backed ZSet. API mirrors ZSet's used
// surface. free()/RAII, no ~this (union field). See [[small-collections-llvm]].

import dreads.zset : ZSet;
import dreads.alloc : KeyspaceAllocator;
import dreads.mem : allocZeroed;
import std.experimental.allocator : reallocate;

struct SmallZSet
{
    enum size_t MAX_ENTRIES = 128; // zset-max-listpack-entries
    enum size_t MAX_VALUE = 64; // zset-max-listpack-value

    private struct Ent
    {
        uint pos, len;
    } // member = blob[pos .. pos+len]

    private bool big;
    private ubyte* blob; // packed member bytes (append + memmove-close, tight)
    private size_t blen, bcap;
    private Ent* ents; // index, SORTED by (score, member)
    private double* scores; // parallel to ents
    private size_t count, cap;
    private ZSet large;

    const(char)[] encoding() const @nogc nothrow
    {
        return big ? "skiplist" : "listpack";
    }

    private const(char)[] memberAt(size_t i) const @nogc nothrow @trusted
    {
        return cast(const(char)[]) blob[ents[i].pos .. ents[i].pos + ents[i].len];
    }

    // compare entry i against (s, m) in (score, member-lex) order
    private int cmpEnt(size_t i, double s, scope const(char)[] m) const @nogc nothrow
    {
        if (scores[i] < s)
            return -1;
        if (scores[i] > s)
            return 1;
        return cmpLex(memberAt(i), m);
    }

    private static int cmpLex(scope const(char)[] a, scope const(char)[] b) @nogc nothrow
    {
        immutable n = a.length < b.length ? a.length : b.length;
        foreach (k; 0 .. n)
            if (a[k] != b[k])
                return a[k] < b[k] ? -1 : 1;
        return a.length < b.length ? -1 : (a.length > b.length ? 1 : 0);
    }

    private size_t indexOf(scope const(char)[] m) const @nogc nothrow
    {
        foreach (i; 0 .. count)
            if (memberAt(i) == m)
                return i;
        return size_t.max;
    }

    // ----- queries -----

    @property size_t length() const @nogc nothrow
    {
        return big ? large.length : count;
    }

    bool score(scope const(char)[] member, out double s) const @nogc nothrow
    {
        if (big)
            return large.score(member, s);
        immutable i = indexOf(member);
        if (i == size_t.max)
            return false;
        s = scores[i];
        return true;
    }

    size_t rank(scope const(char)[] member, out bool ok) const @nogc nothrow
    {
        if (big)
            return large.rank(member, ok);
        immutable i = indexOf(member); // the sorted index IS the rank
        ok = i != size_t.max;
        return ok ? i : 0;
    }

    // ----- mutations -----

    /// Add/update member with score s. Returns true when newly added.
    bool add(double s, scope const(char)[] member) @nogc nothrow
    {
        if (big)
            return large.add(s, member);
        immutable i = indexOf(member);
        if (i != size_t.max)
        {
            if (scores[i] == s)
                return false; // unchanged
            removeAt(i); // re-sort: drop then re-insert at the new score
            insert(s, member);
            return false;
        }
        import dreads.config : gConfig;

        // live thresholds — the suite flips them via CONFIG SET
        immutable limit = gConfig.zsetMaxListpackEntries < 0 ? 0
            : cast(size_t) gConfig.zsetMaxListpackEntries;
        immutable maxVal = gConfig.zsetMaxListpackValue < 0 ? 0
            : cast(size_t) gConfig.zsetMaxListpackValue;
        if (count >= limit || member.length > maxVal)
        {
            spill();
            return large.add(s, member);
        }
        insert(s, member);
        return true;
    }

    bool remove(scope const(char)[] member) @nogc nothrow
    {
        if (big)
            return large.remove(member);
        immutable i = indexOf(member);
        if (i == size_t.max)
            return false;
        removeAt(i);
        return true;
    }

    // ----- ordered walks -----

    int walkRange(size_t start, size_t n, bool rev,
            scope int delegate(const(char)[] member, double score) @nogc nothrow dg) const @nogc nothrow
    {
        if (big)
            return large.walkRange(start, n, rev, dg);
        foreach (k; 0 .. n)
        {
            immutable idx = rev ? count - 1 - start - k : start + k;
            if (idx >= count) // rev underflow wraps to a huge value; also guards start+k
                break;
            if (auto r = dg(memberAt(idx), scores[idx]))
                return r;
        }
        return 0;
    }

    int walkScoreRange(double min, bool minExcl, double max, bool maxExcl,
            scope int delegate(const(char)[] member, double score) @nogc nothrow dg) const @nogc nothrow
    {
        if (big)
            return large.walkScoreRange(min, minExcl, max, maxExcl, dg);
        foreach (i; 0 .. count) // sorted by score: emit the in-range run
        {
            immutable sc = scores[i];
            if (sc < min || (minExcl && sc == min))
                continue;
            if (sc > max || (maxExcl && sc == max))
                break;
            if (auto r = dg(memberAt(i), sc))
                return r;
        }
        return 0;
    }

    int walkLexRange(scope const(char)[] min, bool minExcl, bool minNegInf,
            scope const(char)[] max, bool maxExcl, bool maxPosInf,
            scope int delegate(const(char)[] m, double s) @nogc nothrow dg) const @nogc nothrow
    {
        if (big)
            return large.walkLexRange(min, minExcl, minNegInf, max, maxExcl, maxPosInf, dg);
        // ZRANGEBYLEX assumes a single score; the index is member-sorted within it
        foreach (i; 0 .. count)
        {
            auto m = memberAt(i);
            if (!minNegInf)
            {
                immutable c = cmpLex(m, min);
                if (c < 0 || (minExcl && c == 0))
                    continue;
            }
            if (!maxPosInf)
            {
                immutable c = cmpLex(m, max);
                if (c > 0 || (maxExcl && c == 0))
                    break;
            }
            if (auto r = dg(m, scores[i]))
                return r;
        }
        return 0;
    }

    // ----- lifecycle -----

    SmallZSet dup() const @nogc nothrow @trusted
    {
        SmallZSet c;
        if (big)
        {
            large.walkRange(0, large.length, false, (const(char)[] m, double s) @nogc nothrow{
                c.add(s, m);
                return 0;
            });
            return c;
        }
        if (count)
        {
            c.blob = cast(ubyte*) KeyspaceAllocator.instance.allocate(blen).ptr;
            c.blob[0 .. blen] = blob[0 .. blen];
            c.bcap = c.blen = blen;
            c.ents = cast(Ent*) KeyspaceAllocator.instance.allocate(count * Ent.sizeof).ptr;
            c.ents[0 .. count] = ents[0 .. count];
            c.scores = cast(double*) KeyspaceAllocator.instance.allocate(count * double.sizeof).ptr;
            c.scores[0 .. count] = scores[0 .. count];
            c.cap = c.count = count;
        }
        return c;
    }

    void clear() @nogc nothrow
    {
        if (big)
            large.free();
        count = blen = 0;
        big = false;
    }

    void free() @nogc nothrow @trusted
    {
        clear();
        if (blob !is null)
        {
            KeyspaceAllocator.instance.deallocate((cast(void*) blob)[0 .. bcap]);
            blob = null;
            bcap = 0;
        }
        if (ents !is null)
        {
            KeyspaceAllocator.instance.deallocate((cast(void*) ents)[0 .. cap * Ent.sizeof]);
            KeyspaceAllocator.instance.deallocate((cast(void*) scores)[0 .. cap * double.sizeof]);
            ents = null;
            scores = null;
            cap = 0;
        }
    }

    // Off-loop free support (lazyfree): record every backing block via `add`,
    // freeing NOTHING. Mirrors free() EXACTLY: the spilled ZSet (its skiplist is the
    // offloadable chase) when big, PLUS the small-mode blob/ents/scores arrays —
    // which spill() leaves allocated (it only zeroes count/blen), so free() releases
    // them regardless of `big`. Read-only (the owner is discarded after this).
    void gatherBlocks(scope void delegate(void*, size_t) @nogc nothrow add) @nogc nothrow @trusted
    {
        if (big)
            large.gatherBlocks(add);
        if (blob !is null)
            add(cast(void*) blob, bcap);
        if (ents !is null)
        {
            add(cast(void*) ents, cap * Ent.sizeof);
            add(cast(void*) scores, cap * double.sizeof);
        }
    }

    // ----- internals -----

    private void spill() @nogc nothrow
    {
        foreach (i; 0 .. count)
            large.add(scores[i], memberAt(i));
        count = blen = 0;
        big = true;
    }

    // insert (s, m) at its sorted position; member bytes appended to the blob
    private void insert(double s, scope const(char)[] m) @nogc nothrow @trusted
    {
        // grow blob + index
        if (blen + m.length > bcap)
        {
            auto nc = bcap ? bcap * 2 : 16;
            while (nc < blen + m.length)
                nc *= 2;
            void[] blk = blob is null ? null : (cast(void*) blob)[0 .. bcap];
            immutable ok = reallocate(KeyspaceAllocator.instance, blk, nc);
            assert(ok, "out of memory");
            blob = cast(ubyte*) blk.ptr;
            bcap = nc;
        }
        if (count == cap)
        {
            immutable nc = cap ? cap * 2 : 4;
            void[] eblk = ents is null ? null : (cast(void*) ents)[0 .. cap * Ent.sizeof];
            immutable okE = reallocate(KeyspaceAllocator.instance, eblk, nc * Ent.sizeof);
            assert(okE, "out of memory");
            ents = cast(Ent*) eblk.ptr;
            void[] sblk = scores is null ? null : (cast(void*) scores)[0 .. cap * double.sizeof];
            immutable okS = reallocate(KeyspaceAllocator.instance, sblk, nc * double.sizeof);
            assert(okS, "out of memory");
            scores = cast(double*) sblk.ptr;
            cap = nc;
        }
        // append member bytes at the end of the blob
        immutable pos = cast(uint) blen;
        blob[blen .. blen + m.length] = cast(const(ubyte)[]) m[];
        blen += m.length;
        // find the sorted insert slot, shift the index up, place the entry
        size_t at = count;
        foreach (i; 0 .. count)
            if (cmpEnt(i, s, m) > 0)
            {
                at = i;
                break;
            }
        foreach_reverse (k; at .. count)
        {
            ents[k + 1] = ents[k];
            scores[k + 1] = scores[k];
        }
        ents[at] = Ent(pos, cast(uint) m.length);
        scores[at] = s;
        count++;
    }

    private void removeAt(size_t i) @nogc nothrow @trusted
    {
        import core.stdc.string : memmove;

        immutable pos = ents[i].pos, mlen = ents[i].len;
        memmove(blob + pos, blob + pos + mlen, blen - pos - mlen);
        blen -= mlen;
        foreach (k; 0 .. count)
            if (ents[k].pos > pos)
                ents[k].pos -= mlen;
        foreach (k; i .. count - 1)
        {
            ents[k] = ents[k + 1];
            scores[k] = scores[k + 1];
        }
        count--;
    }
}
