module dreads.dict;

// The dataspace's hash table lives in the `emplace` package now (it WAS this
// file — extracted, generalized to own any resource-bearing value, and shared
// as a reusable library). This module keeps the dreads-specific value types and
// aliases the container as `Dict` so the existing call sites are unchanged.

import dreads.mem : mallocDup, freeSlice;

public import emplace.hashmap : HashMap;

/// The dataspace hash table: malloc'd string keys -> owned values.
alias Dict(V) = HashMap!V;

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

unittest // Dict (== emplace.HashMap) with dreads' owned string values
{
    Dict!StrVal d;
    scope (exit)
        d.free();
    assert(d.get("missing") is null);
    assert(d.set("foo", StrVal.of("bar")));
    assert(d.get("foo").s == "bar");
    assert(!d.set("foo", StrVal.of("baz"))); // overwrite frees the old value
    assert(d.get("foo").s == "baz");
    assert(d.exists("foo") && d.length == 1);
    assert(d.del("foo") && !d.del("foo") && !d.exists("foo"));
}

unittest // set-like and score dicts
{
    Dict!Unit s;
    scope (exit)
        s.free();
    assert(s.set("a", Unit()) && !s.set("a", Unit()));
    assert(s.exists("a") && !s.exists("b"));

    Dict!DoubleVal z;
    scope (exit)
        z.free();
    z.set("m", DoubleVal(1.5));
    z.get("m").d += 1;
    assert(z.get("m").d == 2.5);
}
