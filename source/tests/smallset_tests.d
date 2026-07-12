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

    // --- promotion (small -> big) must be rock solid ---

    @("smallset.spill_at_exact_boundary_keeps_all_members")
    unittest
    {
        SmallSet s;
        scope (exit)
            s.free();
        // non-int members: listpack limit is MAX_ENTRIES
        foreach (i; 0 .. cast(long) SmallSet.MAX_ENTRIES)
            s.set("m" ~ i.to!string, Unit());
        s.encoding.expect.to.equal("listpack");
        s.set("mEXTRA", Unit()); // crosses the boundary
        s.encoding.expect.to.equal("hashtable");
        s.length.expect.to.equal(SmallSet.MAX_ENTRIES + 1);
        foreach (i; 0 .. cast(long) SmallSet.MAX_ENTRIES)
            s.contains("m" ~ i.to!string).expect.to.equal(true);
        s.contains("mEXTRA").expect.to.equal(true);
    }

    @("smallset.intset_spills_at_512_not_128")
    unittest
    {
        SmallSet s;
        scope (exit)
            s.free();
        // pure ints: stays intset up to MAX_INTSET (512), not the 128 listpack cap
        foreach (i; 0 .. 200)
            s.set(i.to!string, Unit());
        s.encoding.expect.to.equal("intset");
        s.length.expect.to.equal(200);
    }

    @("smallset.spill_triggered_by_long_member")
    unittest
    {
        SmallSet s;
        scope (exit)
            s.free();
        s.set("short", Unit());
        s.encoding.expect.to.equal("listpack");
        char[100] big = 'x';
        s.set(big[], Unit()); // member > MAX_MEMBER forces spill
        s.encoding.expect.to.equal("hashtable");
        s.contains("short").expect.to.equal(true);
        s.contains(big[]).expect.to.equal(true);
    }

    @("smallset.boundary_churn_then_spill")
    unittest
    {
        SmallSet s;
        scope (exit)
            s.free();
        foreach (i; 0 .. cast(long) SmallSet.MAX_ENTRIES)
            s.set("k" ~ i.to!string, Unit());
        s.remove("k0");
        s.remove("k1");
        s.encoding.expect.to.equal("listpack"); // back under threshold
        foreach (i; 0 .. 5)
            s.set("n" ~ i.to!string, Unit());
        s.encoding.expect.to.equal("hashtable"); // crossed again
        s.contains("k0").expect.to.equal(false);
        s.contains("k50").expect.to.equal(true);
        s.contains("n3").expect.to.equal(true);
    }

    @("smallset.dup_after_spill_is_independent")
    unittest
    {
        SmallSet s;
        scope (exit)
            s.free();
        foreach (i; 0 .. cast(long)(SmallSet.MAX_ENTRIES + 10))
            s.set("m" ~ i.to!string, Unit());
        auto c = s.dup();
        scope (exit)
            c.free();
        c.length.expect.to.equal(s.length);
        s.remove("m5");
        s.contains("m5").expect.to.equal(false);
        c.contains("m5").expect.to.equal(true); // copy untouched
    }
}
