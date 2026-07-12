module dreads.smallset;

// A byte-string set with an LLVM-SmallSet-style small-size optimization: below a
// cache-fitting threshold it is a contiguous array of members with linear scan
// (one allocation, no per-element hash nodes — cache-friendly, prefetchable);
// above it, it spills one-way to a Dict. The memory win is on the many-tiny-sets
// case; the API mirrors `Dict!Unit` exactly so RObj call sites are unchanged.
//
// Design notes: see the `small-collections-llvm` memory. Starts in dreads; may
// move to emplace once proven. RObj holds this by value in its union, so — like
// the emplace containers — it uses free()/RAII-by-convention, NOT a destructor
// (a union field may not have one). That rules out emplace.Vector (it has a
// ~this), so the small array is a raw malloc'd buffer managed here.

import dreads.dict : Dict, Unit;
import dreads.mem : mallocDup, freeSlice;

struct SmallSet
{
    // Redis set-max-listpack thresholds: stay small while the count is within
    // MAX_ENTRIES and every member is at most MAX_MEMBER bytes. That bounds the
    // contiguous array to a few KB (fits L1), so the linear scan stays hot.
    enum size_t MAX_ENTRIES = 128;
    enum size_t MAX_MEMBER = 64;

    private bool big; // false = small array, true = Dict
    private const(char)[]* items; // malloc'd array of owned member slices (small)
    private size_t len, cap;
    private Dict!Unit large; // large mode

    // ----- queries -----

    @property size_t length() const @nogc nothrow
    {
        return big ? large.length : len;
    }

    bool contains(scope const(char)[] m) const @nogc nothrow
    {
        if (big)
            return large.contains(m);
        foreach (i; 0 .. len)
            if (items[i] == m)
                return true;
        return false;
    }

    alias exists = contains;

    // ----- slot view (SCAN / SRANDMEMBER / SPOP cursor) -----

    @property size_t capacity() const @nogc nothrow
    {
        return big ? large.capacity : len;
    }

    bool slotLive(size_t i) const @nogc nothrow
    {
        return big ? large.slotLive(i) : i < len;
    }

    const(char)[] keyAt(size_t i) const @nogc nothrow
    {
        return big ? large.keyAt(i) : items[i];
    }

    // ----- mutations -----

    /// Mirrors `Dict!Unit.set`: the Unit value is ignored. Returns true when the
    /// member was newly added.
    bool set(scope const(char)[] m, Unit) @nogc nothrow
    {
        if (big)
            return large.set(m, Unit());
        foreach (i; 0 .. len)
            if (items[i] == m)
                return false;
        // spill before growing past the cache-fitting threshold
        if (len >= MAX_ENTRIES || m.length > MAX_MEMBER)
        {
            spill();
            return large.set(m, Unit());
        }
        pushSmall(mallocDup(m)); // own the member bytes
        return true;
    }

    bool remove(scope const(char)[] m) @nogc nothrow
    {
        if (big)
            return large.remove(m);
        foreach (i; 0 .. len)
            if (items[i] == m)
            {
                freeSlice(items[i]);
                items[i] = items[len - 1]; // swap-remove
                len--;
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
        foreach (i; 0 .. len)
            if (auto r = dg(items[i]))
                return r;
        return 0;
    }

    int opApply(scope int delegate(const(char)[], ref Unit) @nogc nothrow dg) @nogc nothrow
    {
        if (big)
            return large.opApply(dg);
        Unit u;
        foreach (i; 0 .. len)
            if (auto r = dg(items[i], u))
                return r;
        return 0;
    }

    // ----- lifecycle -----

    /// Independent deep copy (dups member bytes). Used by RObj.deepDup / COPY.
    SmallSet dup() const @nogc nothrow
    {
        SmallSet c;
        if (big)
        {
            foreach (i; 0 .. large.capacity)
                if (large.slotLive(i))
                    c.set(large.keyAt(i), Unit());
        }
        else
            foreach (i; 0 .. len)
                c.set(items[i], Unit());
        return c;
    }

    void clear() @nogc nothrow
    {
        if (big)
            large.free();
        else
            foreach (i; 0 .. len)
                freeSlice(items[i]);
        len = 0;
        big = false;
    }

    void free() @nogc nothrow @trusted
    {
        clear();
        if (items !is null)
        {
            import core.stdc.stdlib : cfree = free;

            cfree(items);
            items = null;
            cap = 0;
        }
    }

    // one-way small -> large: hand every member to the Dict (which dups the key),
    // then free the array's owned copies.
    private void spill() @nogc nothrow
    {
        foreach (i; 0 .. len)
        {
            large.set(items[i], Unit());
            freeSlice(items[i]);
        }
        len = 0;
        big = true;
    }

    // append to the small array, growing the buffer 2x as needed
    private void pushSmall(const(char)[] owned) @nogc nothrow @trusted
    {
        import core.stdc.stdlib : realloc;

        if (len == cap)
        {
            immutable nc = cap ? cap * 2 : 8;
            items = cast(const(char)[]*) realloc(items, nc * (const(char)[]).sizeof);
            assert(items !is null, "out of memory");
            cap = nc;
        }
        items[len++] = owned;
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

version (unittest)
{
    import fluent.asserts;

    @("smallset.small_mode_and_spill")
    unittest
    {
        import dreads.dict : fmtLong;

        SmallSet s;
        scope (exit)
            s.free();

        s.set("a", Unit()).expect.to.equal(true);
        s.set("b", Unit()).expect.to.equal(true);
        s.set("a", Unit()).expect.to.equal(false); // duplicate
        s.length.expect.to.equal(2);
        s.contains("a").expect.to.equal(true);
        s.contains("z").expect.to.equal(false);

        char[24] b = void;
        foreach (i; 0 .. 100)
            s.set(fmtLong(i, b), Unit());
        s.length.expect.to.be.greaterThan(50);

        s.remove("a").expect.to.equal(true);
        s.remove("a").expect.to.equal(false);
        s.contains("b").expect.to.equal(true);
    }

    @("smallset.spills_past_threshold")
    unittest
    {
        import dreads.dict : fmtLong;

        SmallSet s;
        scope (exit)
            s.free();
        char[24] b = void;
        foreach (i; 0 .. cast(long)(SmallSet.MAX_ENTRIES + 50))
            s.set(fmtLong(i, b), Unit());
        s.length.expect.to.equal(SmallSet.MAX_ENTRIES + 50);
        // every member survives the spill and stays iterable
        size_t counted;
        s.opApply((const(char)[]) @nogc nothrow{ counted++; return 0; });
        counted.expect.to.equal(s.length);
    }
}
