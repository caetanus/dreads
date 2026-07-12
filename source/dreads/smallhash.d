module dreads.smallhash;

// A field->value hash with the same small-size optimization as SmallSet: below
// the Redis hash-max-listpack thresholds the FIELD bytes live packed in one
// contiguous blob (with an offset index for O(1) keyAt), and the VALUES live in
// a parallel StrVal array — so `valAt` still returns a `StrVal*` and the H*
// command sites are unchanged. Past the threshold it spills one-way to a Dict.
// API mirrors `Dict!StrVal`. See [[small-collections-llvm]]; free()/RAII, no
// ~this (union field).

import dreads.dict : Dict, StrVal;

struct SmallHash
{
    enum size_t MAX_ENTRIES = 128; // hash-max-listpack-entries
    enum size_t MAX_VALUE = 64; // hash-max-listpack-value (applies to field AND value)

    private struct Ent
    {
        uint pos, len;
    } // field = blob[pos .. pos+len]

    private bool big;
    private ubyte* blob; // packed field bytes
    private size_t blen, bcap;
    private Ent* ents; // one per field, into the blob
    private StrVal* vals; // parallel value array (vals[i] belongs to ents[i])
    private size_t count, cap;
    private Dict!StrVal large;

    /// hash has no intset tier: small = listpack, large = hashtable.
    const(char)[] encoding() const @nogc nothrow
    {
        return big ? "hashtable" : "listpack";
    }

    private const(char)[] fieldAt(size_t i) const @nogc nothrow @trusted
    {
        return cast(const(char)[]) blob[ents[i].pos .. ents[i].pos + ents[i].len];
    }

    // ----- queries -----

    @property size_t length() const @nogc nothrow
    {
        return big ? large.length : count;
    }

    inout(StrVal)* get(scope const(char)[] k) inout @nogc nothrow @trusted
    {
        if (big)
            return large.get(k);
        foreach (i; 0 .. count)
            if (fieldAt(i) == k)
                return &vals[i];
        return null;
    }

    bool contains(scope const(char)[] k) const @nogc nothrow @trusted
    {
        return (cast() this).get(k) !is null;
    }

    alias exists = contains;

    // ----- slot view (HSCAN / HRANDFIELD cursor); O(1) -----

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
        return big ? large.keyAt(i) : fieldAt(i);
    }

    inout(StrVal)* valAt(size_t i) inout @nogc nothrow @trusted
    {
        return big ? large.valAt(i) : &vals[i];
    }

    // ----- mutations -----

    /// Set field k to value v (ownership of v transfers here). Returns true when
    /// the field is new. Mirrors `Dict!StrVal.set`.
    bool set(scope const(char)[] k, StrVal v) @nogc nothrow
    {
        if (big)
            return large.set(k, v);
        foreach (i; 0 .. count)
            if (fieldAt(i) == k)
            {
                vals[i].free(); // overwrite frees the old value
                vals[i] = v;
                return false;
            }
        if (count >= MAX_ENTRIES || k.length > MAX_VALUE || v.len() > MAX_VALUE)
        {
            spill();
            return large.set(k, v);
        }
        append(k, v);
        return true;
    }

    bool remove(scope const(char)[] k) @nogc nothrow @trusted
    {
        if (big)
            return large.remove(k);
        import core.stdc.string : memmove;

        foreach (i; 0 .. count)
            if (fieldAt(i) == k)
            {
                vals[i].free();
                immutable pos = ents[i].pos, flen = ents[i].len;
                memmove(blob + pos, blob + pos + flen, blen - pos - flen);
                blen -= flen;
                foreach (j; 0 .. count)
                    if (ents[j].pos > pos)
                        ents[j].pos -= flen;
                foreach (j; i .. count - 1) // keep field/value arrays parallel + in order
                {
                    ents[j] = ents[j + 1];
                    vals[j] = vals[j + 1];
                }
                count--;
                return true;
            }
        return false;
    }

    alias del = remove;

    // ----- iteration -----

    int opApply(scope int delegate(const(char)[] key, ref StrVal val) @nogc nothrow dg) @nogc nothrow
    {
        if (big)
            return large.opApply(dg);
        foreach (i; 0 .. count)
            if (auto r = dg(fieldAt(i), vals[i]))
                return r;
        return 0;
    }

    // ----- lifecycle -----

    /// Deep copy (dups fields + each value). Used by RObj.deepDup / COPY.
    SmallHash dup() const @nogc nothrow @trusted
    {
        SmallHash c;
        if (big)
        {
            foreach (i; 0 .. large.capacity)
                if (large.slotLive(i))
                    c.set(large.keyAt(i), large.valAt(i).dup());
            return c;
        }
        foreach (i; 0 .. count)
            c.append(fieldAt(i), vals[i].dup());
        return c;
    }

    void clear() @nogc nothrow
    {
        if (big)
            large.free();
        else
            foreach (i; 0 .. count)
                vals[i].free();
        count = blen = 0;
        big = false;
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
            cfree(vals);
            ents = null;
            vals = null;
            cap = 0;
        }
    }

    // one-way small -> large: move every field/value into the Dict
    private void spill() @nogc nothrow @trusted
    {
        foreach (i; 0 .. count)
            large.set(fieldAt(i), vals[i]); // value ownership transfers to the Dict
        count = blen = 0;
        big = true;
    }

    private void append(scope const(char)[] k, StrVal v) @nogc nothrow @trusted
    {
        import core.stdc.stdlib : realloc;

        if (blen + k.length > bcap)
        {
            auto nc = bcap ? bcap * 2 : 16;
            while (nc < blen + k.length)
                nc *= 2;
            blob = cast(ubyte*) realloc(blob, nc);
            assert(blob !is null, "out of memory");
            bcap = nc;
        }
        if (count == cap)
        {
            immutable nc = cap ? cap * 2 : 4;
            ents = cast(Ent*) realloc(ents, nc * Ent.sizeof);
            vals = cast(StrVal*) realloc(vals, nc * StrVal.sizeof);
            assert(ents !is null && vals !is null, "out of memory");
            cap = nc;
        }
        blob[blen .. blen + k.length] = cast(const(ubyte)[]) k[]; // slice copy
        ents[count] = Ent(cast(uint) blen, cast(uint) k.length);
        vals[count] = v;
        count++;
        blen += k.length;
    }
}

