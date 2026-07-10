module dreads.map;

// automem ships only Vector — no associative container. This is a small,
// insertion-ordered map built on top of automem.Vector, @nogc and allocation-
// clean (the backing Vector owns its buffer and frees it on destruction).
//
// Lookup is a linear scan: it is meant for SMALL, order-sensitive collections
// — RESP map/attribute replies (HGETALL, CONFIG GET, XINFO ...) where the
// field order must be preserved and N is tiny. For the large keyspace hash
// tables use dreads.dict.Dict instead (open-addressed, O(1)); this trades that
// for insertion-order iteration, which Dict does not provide.

import automem.vector : Vector;
import std.experimental.allocator.mallocator : Mallocator;

struct Map(K, V)
{
    private struct Entry
    {
        K key;
        V val;
    }

    private Vector!(Entry, Mallocator) entries;

    /// Insert or overwrite. Preserves first-insertion position on overwrite.
    // @trusted: automem's opIndex bounds-checks (throwing, so not nothrow); the
    // raw @system slice does not, and our loop bounds are proven correct.
    void set(K key, V val) @nogc nothrow @trusted
    {
        foreach (ref e; entries[])
        {
            if (e.key == key)
            {
                e.val = val;
                return;
            }
        }
        entries.put(Entry(key, val));
    }

    /// Pointer to the stored value, or null if absent.
    V* get(K key) @nogc nothrow @trusted return
    {
        foreach (ref e; entries[])
            if (e.key == key)
                return &e.val;
        return null;
    }

    bool contains(K key) @nogc nothrow
    {
        return get(key) !is null;
    }

    @property size_t length() const @nogc nothrow
    {
        return cast(size_t) entries.length;
    }

    @property bool empty() const @nogc nothrow
    {
        return entries.length == 0;
    }

    void clear() @nogc nothrow
    {
        entries.clear();
    }

    /// Iterate key/value pairs in insertion order.
    int opApply(scope int delegate(ref K, ref V) @nogc nothrow dg) @nogc nothrow @trusted
    {
        foreach (ref e; entries[])
        {
            if (auto r = dg(e.key, e.val))
                return r;
        }
        return 0;
    }
}

@nogc nothrow unittest // basic ordered-map behaviour
{
    Map!(const(char)[], int) m;
    m.set("b", 2);
    m.set("a", 1);
    m.set("b", 20); // overwrite keeps position
    assert(m.length == 2);
    assert(*m.get("a") == 1);
    assert(*m.get("b") == 20);
    assert(m.get("z") is null);
    assert(m.contains("a") && !m.contains("z"));

    // insertion order: b then a
    const(char)[][2] seen;
    size_t n = 0;
    foreach (ref k, ref v; m)
        seen[n++] = k;
    assert(seen[0] == "b" && seen[1] == "a");
}

@nogc nothrow unittest // pointer values (the RVariant use case) don't recurse-fault
{
    static struct Node
    {
        int x;
    }

    Node n1 = {7};
    Node n2 = {9};
    Map!(const(char)[], Node*) m;
    m.set("first", &n1);
    m.set("second", &n2);
    assert((*m.get("first")).x == 7);
    assert((*m.get("second")).x == 9);
    assert(m.length == 2);
}
