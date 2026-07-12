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
}
