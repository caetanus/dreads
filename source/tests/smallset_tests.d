module tests.smallset_tests;

// SmallSet: contiguous-blob small mode + one-way spill to a Dict. UT-style named
// tests + fluent asserts.

version (unittest)
{
    import fluent.asserts;
    import std.conv : to;

    import dreads.smallset : SmallSet;
    import dreads.dict : Unit;

    @("smallset.small_mode_and_spill")
    unittest
    {
        SmallSet s;
        scope (exit)
            s.free();

        s.set("a", Unit()).expect.to.equal(true);
        s.set("b", Unit()).expect.to.equal(true);
        s.set("a", Unit()).expect.to.equal(false); // duplicate
        s.length.expect.to.equal(2);
        s.contains("a").expect.to.equal(true);
        s.contains("z").expect.to.equal(false);

        foreach (i; 0 .. 100)
            s.set(i.to!string, Unit());
        s.length.expect.to.be.greaterThan(50);
        s.encoding.expect.to.equal("listpack"); // "a"/"b" above make it non-int, still small

        s.remove("a").expect.to.equal(true); // swap-remove keeps the rest
        s.remove("a").expect.to.equal(false);
        s.contains("b").expect.to.equal(true);
    }

    @("smallset.spills_past_threshold")
    unittest
    {
        SmallSet s;
        scope (exit)
            s.free();
        foreach (i; 0 .. cast(long)(SmallSet.MAX_ENTRIES + 50))
            s.set("m" ~ i.to!string, Unit()); // non-int members -> listpack limit
        s.encoding.expect.to.equal("hashtable"); // past 128 non-int -> spilled
        // every member survives the spill and stays iterable
        size_t counted;
        s.opApply((const(char)[]) @nogc nothrow{ counted++; return 0; });
        counted.expect.to.equal(s.length);
    }

    @("smallset.intset_vs_listpack_encoding")
    unittest
    {
        SmallSet s;
        scope (exit)
            s.free();
        s.set("1", Unit());
        s.set("2", Unit());
        s.encoding.expect.to.equal("intset");
        s.set("x", Unit()); // a non-int drops it to listpack
        s.encoding.expect.to.equal("listpack");
    }
}
