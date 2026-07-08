module dreads.dict;

// Generic @nogc open-addressing hash table: malloc'd string keys mapped to
// owned POD values. V must provide `void free() @nogc nothrow` to release
// whatever it owns; the dict calls it on overwrite, del, clear and free.
// Instances are plain data (no destructor, no copy hooks) so they can live
// inside unions (see dreads.obj) — call free() explicitly when done.

import core.stdc.stdlib : calloc, cfree = free;

import dreads.mem : mallocDup, freeSlice;

package ulong fnv1a(scope const(char)[] s) @nogc nothrow
{
    ulong h = 0xcbf2_9ce4_8422_2325;
    foreach (c; s)
    {
        h ^= c;
        h *= 0x100_0000_01b3;
    }
    return h;
}

/// Owned string value (hash fields, plain string objects).
public struct StrVal
{
    const(char)[] s;

    static StrVal of(scope const(char)[] v) @nogc nothrow
    {
        return StrVal(mallocDup(v));
    }

    void free() @nogc nothrow
    {
        freeSlice(s);
        s = null;
    }
}

/// Empty value for set-like dicts.
public struct Unit
{
    void free() @nogc nothrow
    {
    }
}

/// Score value for sorted sets.
public struct DoubleVal
{
    double d = 0;

    void free() @nogc nothrow
    {
    }
}

private enum SlotState : ubyte
{
    empty,
    used,
    tomb
}

public struct Dict(V) if (__traits(compiles, V.init.free()))
{
    private static struct Slot
    {
        SlotState state;
        ulong hash;
        const(char)[] key;
        V val;
    }

    private Slot* slots;
    private size_t cap; // power of two; 0 until first insert
    private size_t used; // live entries
    private size_t fill; // live + tombstones

    @property size_t length() const @nogc nothrow
    {
        return used;
    }

    /// Releases every entry and the table itself.
    void free() @nogc nothrow
    {
        clear();
        if (slots !is null)
        {
            cfree(slots);
            slots = null;
            cap = 0;
        }
    }

    /// Removes every entry, keeping the allocated table.
    void clear() @nogc nothrow
    {
        foreach (i; 0 .. cap)
        {
            if (slots[i].state == SlotState.used)
            {
                freeSlice(slots[i].key);
                slots[i].val.free();
            }
            slots[i] = Slot.init;
        }
        used = fill = 0;
    }

    private size_t findSlot(scope const(char)[] k, ulong h, out bool found) const @nogc nothrow
    {
        size_t mask = cap - 1;
        size_t i = h & mask;
        size_t firstTomb = size_t.max;
        while (true)
        {
            final switch (slots[i].state)
            {
            case SlotState.empty:
                found = false;
                return firstTomb != size_t.max ? firstTomb : i;
            case SlotState.tomb:
                if (firstTomb == size_t.max)
                    firstTomb = i;
                break;
            case SlotState.used:
                if (slots[i].hash == h && slots[i].key == k)
                {
                    found = true;
                    return i;
                }
                break;
            }
            i = (i + 1) & mask;
        }
    }

    private void rehash(size_t ncap) @nogc nothrow
    {
        auto nslots = cast(Slot*) calloc(ncap, Slot.sizeof);
        assert(nslots !is null, "out of memory");
        size_t mask = ncap - 1;
        foreach (i; 0 .. cap)
        {
            if (slots[i].state != SlotState.used)
                continue;
            size_t j = slots[i].hash & mask;
            while (nslots[j].state == SlotState.used)
                j = (j + 1) & mask;
            nslots[j] = slots[i];
        }
        if (slots !is null)
            cfree(slots);
        slots = nslots;
        cap = ncap;
        fill = used;
    }

    private void maybeGrow() @nogc nothrow
    {
        if (fill * 4 < cap * 3)
            return;
        // double only under real growth; otherwise rebuild in place to purge tombs
        size_t ncap = cap == 0 ? 16 : (used * 2 >= cap ? cap * 2 : cap);
        rehash(ncap);
    }

    /// Inserts or overwrites, taking ownership of val. Returns true if new.
    /// Invalidates pointers previously returned by get().
    bool set(scope const(char)[] k, V val) @nogc nothrow
    {
        maybeGrow();
        auto h = fnv1a(k);
        bool found;
        auto i = findSlot(k, h, found);
        if (found)
        {
            slots[i].val.free();
            slots[i].val = val;
            return false;
        }
        if (slots[i].state == SlotState.empty)
            fill++;
        slots[i].state = SlotState.used;
        slots[i].hash = h;
        slots[i].key = mallocDup(k);
        slots[i].val = val;
        used++;
        return true;
    }

    /// Pointer to the live value, or null. Valid until the next set/del.
    inout(V)* get(scope const(char)[] k) inout @nogc nothrow
    {
        if (used == 0)
            return null;
        bool found;
        auto i = findSlot(k, fnv1a(k), found);
        return found ? &slots[i].val : null;
    }

    bool exists(scope const(char)[] k) const @nogc nothrow
    {
        return get(k) !is null;
    }

    bool del(scope const(char)[] k) @nogc nothrow
    {
        if (used == 0)
            return false;
        bool found;
        auto i = findSlot(k, fnv1a(k), found);
        if (!found)
            return false;
        freeSlice(slots[i].key);
        slots[i].val.free();
        slots[i] = Slot.init;
        slots[i].state = SlotState.tomb;
        used--;
        return true;
    }

    /// Removes k without freeing its value; the caller takes ownership.
    bool steal(scope const(char)[] k, out V val) @nogc nothrow
    {
        if (used == 0)
            return false;
        bool found;
        auto i = findSlot(k, fnv1a(k), found);
        if (!found)
            return false;
        freeSlice(slots[i].key);
        val = slots[i].val;
        slots[i] = Slot.init;
        slots[i].state = SlotState.tomb;
        used--;
        return true;
    }

    // Index-based iteration for @nogc callers that cannot afford closures.
    @property size_t capacity() const @nogc nothrow
    {
        return cap;
    }

    bool slotLive(size_t i) const @nogc nothrow
    {
        return slots[i].state == SlotState.used;
    }

    const(char)[] keyAt(size_t i) const @nogc nothrow
    {
        return slots[i].key;
    }

    inout(V)* valAt(size_t i) inout @nogc nothrow
    {
        return &slots[i].val;
    }

    int opApply(scope int delegate(const(char)[] key, ref V val) @nogc nothrow dg) @nogc nothrow
    {
        foreach (i; 0 .. cap)
        {
            if (slots[i].state != SlotState.used)
                continue;
            auto r = dg(slots[i].key, slots[i].val);
            if (r)
                return r;
        }
        return 0;
    }
}

unittest // basic set/get/del/exists with owned string values
{
    Dict!StrVal d;
    scope (exit)
        d.free();
    assert(d.get("missing") is null);
    assert(d.set("foo", StrVal.of("bar")));
    assert(d.get("foo").s == "bar");
    assert(!d.set("foo", StrVal.of("baz"))); // overwrite frees the old value
    assert(d.get("foo").s == "baz");
    assert(d.exists("foo"));
    assert(d.length == 1);
    assert(d.del("foo"));
    assert(!d.del("foo"));
    assert(!d.exists("foo"));
    assert(d.length == 0);
}

unittest // rehash under load, deletion churn, clear
{
    import std.conv : to;

    Dict!StrVal d;
    scope (exit)
        d.free();
    foreach (i; 0 .. 1000)
        d.set(i.to!string, StrVal.of("v" ~ i.to!string));
    assert(d.length == 1000);
    assert(d.get("999").s == "v999");
    foreach (i; 0 .. 500)
        assert(d.del(i.to!string));
    assert(d.length == 500);
    assert(!d.exists("42"));
    assert(d.exists("542"));
    foreach (i; 0 .. 500)
        d.set(i.to!string, StrVal.of("again"));
    assert(d.length == 1000);
    assert(d.get("42").s == "again");
    size_t n;
    foreach (key, ref val; d)
        n++;
    assert(n == 1000);
    d.clear();
    assert(d.length == 0 && !d.exists("999"));
    d.set("x", StrVal.of("y"));
    assert(d.get("x").s == "y");
}

unittest // set-like dict and score dict
{
    Dict!Unit s;
    scope (exit)
        s.free();
    assert(s.set("a", Unit()));
    assert(!s.set("a", Unit()));
    assert(s.exists("a") && !s.exists("b"));

    Dict!DoubleVal z;
    scope (exit)
        z.free();
    z.set("m", DoubleVal(1.5));
    assert(z.get("m").d == 1.5);
    z.get("m").d += 1;
    assert(z.get("m").d == 2.5);
}

unittest // empty keys and values are valid
{
    Dict!StrVal d;
    scope (exit)
        d.free();
    d.set("", StrVal.of("empty-key"));
    d.set("empty-val", StrVal.of(""));
    assert(d.get("").s == "empty-key");
    assert(d.get("empty-val").s == "");
}
