module tests.smallhash_tests;

// SmallHash: field blob + parallel StrVal value array, spilling to a Dict.
// UT-style named tests + fluent asserts.

version (unittest)
{
    import fluent.asserts;
    import std.conv : to;

    import dreads.smallhash : SmallHash;
    import dreads.dict : StrVal;

    private string val(ref SmallHash h, string field)
    {
        char[24] sb = void;
        auto p = h.get(field);
        return p is null ? null : p.bytes(sb).idup;
    }

    @("smallhash.small_mode_get_set_del")
    unittest
    {
        SmallHash h;
        scope (exit)
            h.free();

        h.set("a", StrVal.of("1")).expect.to.equal(true);
        h.set("b", StrVal.of("hello")).expect.to.equal(true);
        h.set("a", StrVal.of("2")).expect.to.equal(false); // overwrite
        h.length.expect.to.equal(2);
        h.val("a").expect.to.equal("2");
        h.val("b").expect.to.equal("hello");
        (h.get("missing") is null).expect.to.equal(true);
        h.encoding.expect.to.equal("listpack");

        h.remove("a").expect.to.equal(true);
        h.remove("a").expect.to.equal(false);
        h.val("b").expect.to.equal("hello");
        h.length.expect.to.equal(1);
    }

    @("smallhash.spills_past_threshold")
    unittest
    {
        SmallHash h;
        scope (exit)
            h.free();
        foreach (i; 0 .. cast(long)(SmallHash.MAX_ENTRIES + 20))
            h.set(i.to!string, StrVal.ofInt(i));
        h.length.expect.to.equal(SmallHash.MAX_ENTRIES + 20);
        h.encoding.expect.to.equal("hashtable");
        h.val("5").expect.to.equal("5"); // values survive the spill
    }

    // --- promotion (small -> big) must be rock solid ---

    @("smallhash.spill_at_exact_boundary_keeps_all_fields_and_values")
    unittest
    {
        SmallHash h;
        scope (exit)
            h.free();
        // fill to exactly the threshold (still small), then one more forces spill
        foreach (i; 0 .. cast(long) SmallHash.MAX_ENTRIES)
            h.set("f" ~ i.to!string, StrVal.of("v" ~ i.to!string));
        h.encoding.expect.to.equal("listpack");
        h.length.expect.to.equal(SmallHash.MAX_ENTRIES);
        h.set("fEXTRA", StrVal.of("vEXTRA")); // crosses the boundary
        h.encoding.expect.to.equal("hashtable");
        h.length.expect.to.equal(SmallHash.MAX_ENTRIES + 1);
        // every field's value survived the promotion intact
        foreach (i; 0 .. cast(long) SmallHash.MAX_ENTRIES)
            h.val("f" ~ i.to!string).expect.to.equal("v" ~ i.to!string);
        h.val("fEXTRA").expect.to.equal("vEXTRA");
    }

    @("smallhash.spill_triggered_by_long_value")
    unittest
    {
        SmallHash h;
        scope (exit)
            h.free();
        h.set("a", StrVal.of("short"));
        h.encoding.expect.to.equal("listpack");
        char[100] big = 'x';
        h.set("b", StrVal.of(big[])); // value > MAX_VALUE forces spill
        h.encoding.expect.to.equal("hashtable");
        h.val("a").expect.to.equal("short"); // small value survived
        h.val("b").expect.to.equal(big[].idup);
    }

    @("smallhash.boundary_churn_then_spill")
    unittest
    {
        SmallHash h;
        scope (exit)
            h.free();
        // hover at the boundary with add/overwrite/remove, then cross it
        foreach (i; 0 .. cast(long) SmallHash.MAX_ENTRIES)
            h.set("k" ~ i.to!string, StrVal.ofInt(i));
        h.remove("k0").expect.to.equal(true);
        h.remove("k1").expect.to.equal(true);
        h.encoding.expect.to.equal("listpack"); // back under threshold
        foreach (i; 0 .. 5)
            h.set("n" ~ i.to!string, StrVal.ofInt(1000 + i));
        h.encoding.expect.to.equal("hashtable"); // crossed again -> spilled
        (h.get("k0") is null).expect.to.equal(true); // removed stays removed
        h.val("k50").expect.to.equal("50");
        h.val("n3").expect.to.equal("1003");
    }

    @("smallhash.dup_after_spill_is_independent")
    unittest
    {
        SmallHash h;
        scope (exit)
            h.free();
        foreach (i; 0 .. cast(long)(SmallHash.MAX_ENTRIES + 10))
            h.set("f" ~ i.to!string, StrVal.ofInt(i));
        auto c = h.dup();
        scope (exit)
            c.free();
        c.encoding.expect.to.equal("hashtable");
        c.length.expect.to.equal(h.length);
        h.remove("f5"); // mutating the original must not touch the copy
        (h.get("f5") is null).expect.to.equal(true);
        c.val("f5").expect.to.equal("5");
    }
}
