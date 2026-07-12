module tests.smallzset_tests;

// SmallZSet: sorted-index small mode over a member blob, spilling to a skiplist
// ZSet. UT-style named tests + fluent asserts.

version (unittest)
{
    import fluent.asserts;
    import std.conv : to;

    import dreads.smallzset : SmallZSet;

    @("smallzset.sorted_add_score_rank")
    unittest
    {
        SmallZSet z;
        scope (exit)
            z.free();

        z.add(3, "c").expect.to.equal(true);
        z.add(1, "a").expect.to.equal(true);
        z.add(2, "b").expect.to.equal(true);
        z.add(1, "a").expect.to.equal(false); // unchanged
        z.length.expect.to.equal(3);
        z.encoding.expect.to.equal("listpack");

        double s;
        z.score("b", s).expect.to.equal(true);
        s.expect.to.equal(2.0);
        z.score("z", s).expect.to.equal(false);

        // sorted by score: a(1) b(2) c(3) -> ranks 0,1,2
        bool ok;
        z.rank("a", ok).expect.to.equal(cast(size_t) 0);
        z.rank("b", ok).expect.to.equal(cast(size_t) 1);
        z.rank("c", ok).expect.to.equal(cast(size_t) 2);
    }

    @("smallzset.reorders_on_score_update")
    unittest
    {
        SmallZSet z;
        scope (exit)
            z.free();
        z.add(1, "a");
        z.add(2, "b");
        z.add(3, "c");
        z.add(10, "a"); // a jumps to the end
        bool ok;
        z.rank("a", ok).expect.to.equal(cast(size_t) 2); // now last
        z.rank("b", ok).expect.to.equal(cast(size_t) 0);
        z.length.expect.to.equal(3);
    }

    @("smallzset.score_range_walk")
    unittest
    {
        SmallZSet z;
        scope (exit)
            z.free();
        foreach (i; 0 .. 10)
            z.add(cast(double) i, "m" ~ i.to!string);
        size_t n;
        z.walkScoreRange(3, false, 6, false, (const(char)[] m, double s) @nogc nothrow{
            n++;
            return 0;
        });
        n.expect.to.equal(cast(size_t) 4); // scores 3,4,5,6 inclusive
    }

    @("smallzset.spills_past_threshold")
    unittest
    {
        SmallZSet z;
        scope (exit)
            z.free();
        foreach (i; 0 .. cast(long)(SmallZSet.MAX_ENTRIES + 20))
            z.add(cast(double) i, "m" ~ i.to!string);
        z.length.expect.to.equal(SmallZSet.MAX_ENTRIES + 20);
        z.encoding.expect.to.equal("skiplist");
        double s;
        z.score("m5", s).expect.to.equal(true);
        s.expect.to.equal(5.0);
    }

    // --- promotion (small -> big) must be rock solid ---

    @("smallzset.spill_preserves_scores_and_order")
    unittest
    {
        SmallZSet z;
        scope (exit)
            z.free();
        foreach (i; 0 .. cast(long) SmallZSet.MAX_ENTRIES)
            z.add(cast(double) i, "m" ~ i.to!string);
        z.encoding.expect.to.equal("listpack");
        z.add(9999, "mEXTRA"); // crosses the boundary
        z.encoding.expect.to.equal("skiplist");
        // scores survive
        double s;
        z.score("m0", s);
        s.expect.to.equal(0.0);
        z.score("m127", s);
        s.expect.to.equal(127.0);
        // rank order survives (m0 first, mEXTRA last)
        bool ok;
        z.rank("m0", ok).expect.to.equal(cast(size_t) 0);
        z.rank("mEXTRA", ok).expect.to.equal(z.length - 1);
    }

    @("smallzset.spill_triggered_by_long_member")
    unittest
    {
        SmallZSet z;
        scope (exit)
            z.free();
        z.add(1, "short");
        z.encoding.expect.to.equal("listpack");
        char[100] big = 'x';
        z.add(2, big[]); // member > MAX_VALUE forces spill
        z.encoding.expect.to.equal("skiplist");
        double s;
        z.score("short", s).expect.to.equal(true);
        s.expect.to.equal(1.0);
        z.score(big[], s).expect.to.equal(true);
        s.expect.to.equal(2.0);
    }

    @("smallzset.dup_after_spill_is_independent")
    unittest
    {
        SmallZSet z;
        scope (exit)
            z.free();
        foreach (i; 0 .. cast(long)(SmallZSet.MAX_ENTRIES + 10))
            z.add(cast(double) i, "m" ~ i.to!string);
        auto c = z.dup();
        scope (exit)
            c.free();
        c.length.expect.to.equal(z.length);
        z.remove("m5");
        double s;
        z.score("m5", s).expect.to.equal(false);
        c.score("m5", s).expect.to.equal(true); // copy untouched
        s.expect.to.equal(5.0);
    }
}
