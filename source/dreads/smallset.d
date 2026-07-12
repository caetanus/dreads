module dreads.smallset;

// A byte-string set with an LLVM-SmallSet / Redis-listpack-style small-size
// optimization: below a cache-fitting threshold ALL member bytes live packed in
// one contiguous blob (with a parallel offset index for O(1) keyAt), so a
// membership scan walks a single cache-resident buffer instead of chasing a
// pointer to a separately-malloc'd member per element — that pointer-chase is
// what perf showed dominates; O(n) inside the cache beats O(n) wandering RAM.
// Past the threshold it spills one-way to a Dict. API mirrors `Dict!Unit` so
// RObj call sites are unchanged.
//
// Design notes: see `small-collections-llvm` memory. Starts in dreads; may move
// to emplace. RObj holds this by value in its union, so it uses free()/RAII-by-
// convention, NOT a destructor (a union field may not have one).

import dreads.dict : canonicalInt, Dict, Unit;

struct SmallSet
{
    // Redis thresholds. An all-integer set stays compact (intset) up to
    // MAX_INTSET; any non-int member drops it to the listpack limits
    // (MAX_ENTRIES / MAX_MEMBER). Either way the blob fits L1. Then it spills.
    enum size_t MAX_ENTRIES = 128; // set-max-listpack-entries
    enum size_t MAX_MEMBER = 64; // set-max-listpack-value
    enum size_t MAX_INTSET = 512; // set-max-intset-entries

    private struct Ent
    {
        uint pos, len;
    } // member = blob[pos .. pos+len]

    private bool big; // false = packed blob, true = Dict
    private bool hasNonInt; // a non-int member was added (intset -> listpack).
    // NOTE: inverted (not `allInt`) on purpose — RObj holds this in a zero-init
    // union, so a field's `= true` default would NOT apply; zero must mean intset.
    private ubyte* blob; // packed member bytes, back to back
    private size_t blen, bcap;
    private Ent* ents; // one entry per member, into the blob
    private size_t count, ecap;
    private Dict!Unit large; // large mode

    /// The Redis-equivalent encoding, backed by the real small/large state.
    const(char)[] encoding() const @nogc nothrow
    {
        if (big)
            return "hashtable";
        return hasNonInt ? "listpack" : "intset";
    }

    private const(char)[] at(size_t i) const @nogc nothrow @trusted
    {
        return cast(const(char)[]) blob[ents[i].pos .. ents[i].pos + ents[i].len];
    }

    // ----- queries -----

    @property size_t length() const @nogc nothrow
    {
        return big ? large.length : count;
    }

    bool contains(scope const(char)[] m) const @nogc nothrow
    {
        if (big)
            return large.contains(m);
        // scan the contiguous blob — cache-resident, no per-member pointer chase
        foreach (i; 0 .. count)
            if (at(i) == m)
                return true;
        return false;
    }

    alias exists = contains;

    // ----- slot view (SCAN / SRANDMEMBER / SPOP cursor); keyAt is O(1) -----

    @property size_t capacity() const @nogc nothrow
    {
        return big ? large.capacity : count;
    }

    bool slotLive(size_t i) const @nogc nothrow
    {
        return big ? large.slotLive(i) : i < count;
    }

    const(char)[] keyAt(size_t i) const @nogc nothrow
    {
        return big ? large.keyAt(i) : at(i);
    }

    // ----- mutations -----

    /// Mirrors `Dict!Unit.set`: the Unit value is ignored. Returns true when the
    /// member was newly added.
    bool set(scope const(char)[] m, Unit) @nogc nothrow
    {
        if (big)
            return large.set(m, Unit());
        foreach (i; 0 .. count)
            if (at(i) == m)
                return false;
        import dreads.config : gConfig;

        long iv;
        immutable mIsInt = canonicalInt(m, iv);
        immutable staysInt = !hasNonInt && mIsInt;
        // live thresholds — the suite flips them via CONFIG SET (enums above
        // document the Redis defaults)
        immutable cfgLimit = staysInt ? gConfig.setMaxIntsetEntries : gConfig.setMaxListpackEntries;
        immutable limit = cfgLimit < 0 ? 0 : cast(size_t) cfgLimit;
        immutable maxMember = gConfig.setMaxListpackValue < 0 ? 0
            : cast(size_t) gConfig.setMaxListpackValue;
        if (count >= limit || (!mIsInt && m.length > maxMember))
        {
            spill();
            return large.set(m, Unit());
        }
        append(m);
        if (!mIsInt)
            hasNonInt = true;
        return true;
    }

    bool remove(scope const(char)[] m) @nogc nothrow @trusted
    {
        if (big)
            return large.remove(m);
        import core.stdc.string : memmove;

        foreach (i; 0 .. count)
            if (at(i) == m)
            {
                immutable pos = ents[i].pos, mlen = ents[i].len;
                // close the blob hole, then fix up every offset past it
                memmove(blob + pos, blob + pos + mlen, blen - pos - mlen);
                blen -= mlen;
                foreach (j; 0 .. count)
                    if (ents[j].pos > pos)
                        ents[j].pos -= mlen;
                foreach (j; i .. count - 1) // drop entry i, keep order
                    ents[j] = ents[j + 1];
                count--;
                return true;
            }
        return false;
    }

    alias del = remove;

    // ----- iteration -----

    int opApply(scope int delegate(const(char)[]) @nogc nothrow dg) @nogc nothrow
    {
        if (big)
            return large.opApply((const(char)[] k, ref Unit) => dg(k));
        foreach (i; 0 .. count)
            if (auto r = dg(at(i)))
                return r;
        return 0;
    }

    int opApply(scope int delegate(const(char)[], ref Unit) @nogc nothrow dg) @nogc nothrow
    {
        if (big)
            return large.opApply(dg);
        Unit u;
        foreach (i; 0 .. count)
            if (auto r = dg(at(i), u))
                return r;
        return 0;
    }

    // ----- lifecycle -----

    /// Independent deep copy. Used by RObj.deepDup / COPY.
    SmallSet dup() const @nogc nothrow @trusted
    {
        SmallSet c;
        if (big)
        {
            foreach (i; 0 .. large.capacity)
                if (large.slotLive(i))
                    c.set(large.keyAt(i), Unit());
            return c;
        }
        // fast path: one blob copy + one entry-array copy
        import core.stdc.stdlib : malloc;

        if (count)
        {
            c.blob = cast(ubyte*) malloc(blen);
            c.blob[0 .. blen] = blob[0 .. blen]; // slice copy
            c.bcap = c.blen = blen;
            c.ents = cast(Ent*) malloc(count * Ent.sizeof);
            c.ents[0 .. count] = ents[0 .. count];
            c.ecap = c.count = count;
        }
        c.hasNonInt = hasNonInt;
        return c;
    }

    void clear() @nogc nothrow
    {
        if (big)
            large.free();
        count = blen = 0;
        big = false;
        hasNonInt = false;
    }

    void free() @nogc nothrow @trusted
    {
        import core.stdc.stdlib : cfree = free;

        clear();
        if (blob !is null)
        {
            cfree(blob);
            blob = null;
            bcap = 0;
        }
        if (ents !is null)
        {
            cfree(ents);
            ents = null;
            ecap = 0;
        }
    }

    // one-way small -> large: hand every member to the Dict (which dups the key)
    private void spill() @nogc nothrow
    {
        foreach (i; 0 .. count)
            large.set(at(i), Unit());
        count = blen = 0;
        big = true;
    }

    // append member bytes to the blob + an entry, growing both 2x as needed
    private void append(scope const(char)[] m) @nogc nothrow @trusted
    {
        import core.stdc.stdlib : realloc;

        if (blen + m.length > bcap)
        {
            auto nc = bcap ? bcap * 2 : 16; // small first cap: tiny sets stay tiny
            while (nc < blen + m.length)
                nc *= 2;
            blob = cast(ubyte*) realloc(blob, nc);
            assert(blob !is null, "out of memory");
            bcap = nc;
        }
        if (count == ecap)
        {
            immutable nc = ecap ? ecap * 2 : 4;
            ents = cast(Ent*) realloc(ents, nc * Ent.sizeof);
            assert(ents !is null, "out of memory");
            ecap = nc;
        }
        blob[blen .. blen + m.length] = cast(const(ubyte)[]) m[]; // slice copy
        ents[count++] = Ent(cast(uint) blen, cast(uint) m.length);
        blen += m.length;
    }
}

