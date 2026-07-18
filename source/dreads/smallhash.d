module dreads.smallhash;

// A field->value hash with the same small-size optimization as SmallSet: below
// the Redis hash-max-listpack thresholds the FIELD bytes live packed in one
// contiguous blob (with an offset index for O(1) keyAt), and the VALUES live in
// a parallel StrVal array — so `valAt` still returns a `StrVal*` and the H*
// command sites are unchanged. Past the threshold it spills one-way to a Dict.
// API mirrors `Dict!StrVal`. See [[small-collections-llvm]]; free()/RAII, no
// ~this (union field).

import dreads.dict : Dict, StrVal;
import dreads.alloc : KeyspaceAllocator;
import emplace.vector : Vector;
import emplace.hashmap : HashMap;
import std.experimental.allocator : reallocate;

// Field-TTL side-map, keyed by field name -> absolute deadline (ms). Keyspace
// data, so it routes through KeyspaceAllocator like the rest of the hash.
private alias FieldTTLMap = HashMap!(ulong, KeyspaceAllocator);

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
    private HashMap!(StrVal, KeyspaceAllocator) large;

    // Per-field TTL side-map (HEXPIRE family). NULL until the first field TTL is
    // set — a hash without field expiry pays nothing (one null pointer). Keyed by
    // field name -> absolute deadline (ms); the Dict owns its own copies of the
    // field names, so it survives blob realloc / small->large spill (which key by
    // name too). See [[keyspace]] field-TTL design: this is the per-hash analog of
    // RObj.expireAtMs; the *active* reap is driven by a separate tagged index.
    private FieldTTLMap* fieldTTL;
    // A cached LOWER BOUND on the field deadlines — the O(1) fast path for lazy
    // reap: when `minDeadline > now` nothing can be due, so a hot read skips the
    // O(n) scan entirely. Kept exact by `setFieldTTL`; a `clearFieldTTL` that drops
    // the current min leaves it stale-small (still a valid lower bound), and the
    // next scan self-heals it via `minFieldTTL`. 0 = no field TTLs.
    private ulong minDeadline;

    /// hash has no intset tier: small = listpack, large = hashtable. A small hash
    /// that carries any field TTL reports "listpackex" (Valkey's listpack-with-
    /// expiry encoding), matching OBJECT ENCODING after HEXPIRE.
    const(char)[] encoding() const @nogc nothrow
    {
        if (big)
            return "hashtable";
        return hasFieldTTL ? "listpackex" : "listpack";
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
        // cast the POINTER to mutable (get is non-const), NOT `(cast() this)` —
        // copying `this` would deep-copy `large` via HashMap's postblit (an alloc
        // on a read path) and then free it, dangling get's returned pointer.
        return (cast(SmallHash*)&this).get(k) !is null;
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
        // HSET-family write drops any TTL the field had (Valkey: entryUpdate with
        // EXPIRY_NONE). No-op for a new field or a hash with no field TTLs.
        if (fieldTTL !is null)
            fieldTTL.remove(k);
        if (big)
            return large.set(k, v);
        import dreads.config : gConfig;

        // live thresholds — the suite flips them via CONFIG SET
        immutable limit = gConfig.hashMaxListpackEntries < 0 ? 0
            : cast(size_t) gConfig.hashMaxListpackEntries;
        immutable maxVal = gConfig.hashMaxListpackValue < 0 ? 0
            : cast(size_t) gConfig.hashMaxListpackValue;
        foreach (i; 0 .. count)
            if (fieldAt(i) == k)
            {
                // an update whose new value exceeds the listpack limit must spill
                if (v.len() > maxVal)
                {
                    spill();
                    return large.set(k, v);
                }
                vals[i].free(); // overwrite frees the old value
                vals[i] = v;
                return false;
            }
        if (count >= limit || k.length > maxVal || v.len() > maxVal)
        {
            spill();
            return large.set(k, v);
        }
        append(k, v);
        return true;
    }

    bool remove(scope const(char)[] k) @nogc nothrow @trusted
    {
        // Remove the field FIRST (this reads `k`), then clear its TTL entry — the
        // reap path passes `k` as a slice INTO the fieldTTL key memory, and
        // `fieldTTL.remove` frees that memory. Clearing the TTL first would dangle
        // `k` before the field lookup below (a UAF the composed allocator turns
        // into a missed removal; malloc merely left the freed bytes readable).
        bool found;
        if (big)
            found = large.remove(k);
        else
        {
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
                    found = true;
                    break;
                }
        }
        if (fieldTTL !is null)
            fieldTTL.remove(k); // drop the field's TTL entry, if any
        return found;
    }

    alias del = remove;

    // ----- field TTL (HEXPIRE family) -----

    /// Does any field carry a TTL? Cheap guard for the lazy-reap fast path.
    bool hasFieldTTL() const @nogc nothrow
    {
        return fieldTTL !is null && fieldTTL.length > 0;
    }

    /// Set (or update) `field`'s absolute deadline (ms). The caller guarantees the
    /// field exists. Lazily allocates the side-map on first use.
    void setFieldTTL(scope const(char)[] field, ulong at) @nogc nothrow @trusted
    {
        if (fieldTTL is null)
        {
            import dreads.mem : allocZeroed;

            fieldTTL = cast(FieldTTLMap*) allocZeroed!KeyspaceAllocator(FieldTTLMap.sizeof).ptr;
            assert(fieldTTL !is null, "out of memory");
        }
        fieldTTL.set(field, at); // Dict owns a stable copy of the field name
        if (minDeadline == 0 || at < minDeadline)
            minDeadline = at; // keep the fast-path bound exact on set
    }

    /// `field`'s absolute deadline (ms), or 0 when it has no TTL.
    ulong getFieldTTL(scope const(char)[] field) const @nogc nothrow @trusted
    {
        if (fieldTTL is null)
            return 0;
        // deref a mutable-cast pointer (lvalue ref, no copy). `(cast() *fieldTTL)`
        // would copy the map via postblit and free it at end of statement — then
        // `*p` below reads freed memory (a UAF the composed allocator exposes).
        auto p = (cast(FieldTTLMap*) fieldTTL).get(field);
        return p is null ? 0 : *p;
    }

    /// Drop `field`'s TTL (HPERSIST). Returns true when a TTL was actually removed.
    bool clearFieldTTL(scope const(char)[] field) @nogc nothrow
    {
        if (fieldTTL is null)
            return false;
        return fieldTTL.remove(field);
    }

    /// The smallest field deadline in the hash (0 = no field TTLs). Drives the
    /// active index: a hash registers itself at its *nearest* field deadline.
    ulong minFieldTTL() const @nogc nothrow @trusted
    {
        if (fieldTTL is null)
            return 0;
        ulong m = 0;
        (cast(FieldTTLMap*) fieldTTL).opApply((const(char)[] _f, ref ulong at) @nogc nothrow {
            if (m == 0 || at < m)
                m = at;
            return 0;
        });
        return m;
    }

    /// Lazy reap: remove every field whose deadline is <= `nowMs` (field and value
    /// gone, TTL entry gone). Returns how many fields were reaped. The @nogc analog
    /// of key-level lazy expiry in `Keyspace.lookup`.
    size_t reapExpired(ulong nowMs) @nogc nothrow @trusted
    {
        if (fieldTTL is null || fieldTTL.length == 0 || minDeadline > nowMs)
            return 0; // O(1) fast path: nothing can be due yet
        // Collect the due field names first (slices into the side-map's own stable
        // key memory), then mutate — never delete while iterating the Dict.
        Vector!(const(char)[]) due;
        fieldTTL.opApply((const(char)[] f, ref ulong at) @nogc nothrow {
            if (at <= nowMs)
                due.put(f);
            return 0;
        });
        foreach (f; due[])
            remove(f); // removes field+value AND its TTL entry (remove() clears it)
        minDeadline = minFieldTTL(); // refresh the bound (self-heals a stale-small min)
        return due.length;
    }

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
        // clone the field-TTL side-map (keyed by name, independent of tier)
        if (fieldTTL !is null)
            (cast(FieldTTLMap*) fieldTTL).opApply((const(char)[] f, ref ulong at) @nogc nothrow {
                c.setFieldTTL(f, at);
                return 0;
            });
        return c;
    }

    void clear() @nogc nothrow
    {
        if (big)
            large.free();
        else
            foreach (i; 0 .. count)
                vals[i].free();
        if (fieldTTL !is null)
            fieldTTL.clear(); // drop every field TTL; keep the map alloc for reuse
        minDeadline = 0;
        count = blen = 0;
        big = false;
    }

    void free() @nogc nothrow @trusted
    {
        clear();
        if (fieldTTL !is null)
        {
            fieldTTL.free();
            KeyspaceAllocator.instance.deallocate((cast(void*) fieldTTL)[0 .. FieldTTLMap.sizeof]);
            fieldTTL = null;
        }
        if (blob !is null)
        {
            KeyspaceAllocator.instance.deallocate((cast(void*) blob)[0 .. bcap]);
            blob = null;
            bcap = 0;
        }
        if (ents !is null)
        {
            KeyspaceAllocator.instance.deallocate((cast(void*) ents)[0 .. cap * Ent.sizeof]);
            KeyspaceAllocator.instance.deallocate((cast(void*) vals)[0 .. cap * StrVal.sizeof]);
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
        if (blen + k.length > bcap)
        {
            auto nc = bcap ? bcap * 2 : 16;
            while (nc < blen + k.length)
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
            void[] vblk = vals is null ? null : (cast(void*) vals)[0 .. cap * StrVal.sizeof];
            immutable okV = reallocate(KeyspaceAllocator.instance, vblk, nc * StrVal.sizeof);
            assert(okV, "out of memory");
            vals = cast(StrVal*) vblk.ptr;
            cap = nc;
        }
        blob[blen .. blen + k.length] = cast(const(ubyte)[]) k[]; // slice copy
        ents[count] = Ent(cast(uint) blen, cast(uint) k.length);
        vals[count] = v;
        count++;
        blen += k.length;
    }
}

@nogc nothrow unittest // reapExpired regression: it collects due field names as
{                       // slices INTO the fieldTTL key memory, then remove()s each.
    // remove() must drop the field BEFORE clearing the TTL (which frees that key
    // slice) — else `k` dangles and the composed allocator (freelist next-ptr
    // overwrite) leaves the field un-removed. malloc hid this; this test catches it.
    SmallHash h;
    scope (exit)
        h.free();
    h.set("f1", StrVal.of("v1"));
    h.set("f2", StrVal.of("v2"));
    h.setFieldTTL("f1", 100); // deadline 100, reap at now=200
    assert(h.getFieldTTL("f1") == 100);
    immutable reaped = h.reapExpired(200);
    assert(reaped == 1, "expired field not reaped");
    assert(h.get("f1") is null, "reaped field still present (remove UAF)");
    assert(h.get("f2") !is null, "live field wrongly dropped");
    assert(h.length == 1);
}

