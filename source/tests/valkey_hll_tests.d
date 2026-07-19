module tests.valkey_hll_tests;

// Valkey unit/hyperloglog.tcl core ported to native in-process UT (see
// valkey_incr_tests.d for rationale + THIRD_PARTY_NOTICES credit). HLL counts are
// approximate, but for small cardinalities the sparse representation is EXACT, so
// these use small sets. The accuracy/fuzz (0.81% error over millions) + DEBUG
// self-test cases stay in the blackbox sweep.

version (unittest)
{
    import fluent.asserts;
    import std.conv : to;

    import dreads.commands;
    import dreads.mem : Arena, ByteBuffer;
    import dreads.obj : Keyspace;
    import dreads.resp;

    private string respCmd(string[] args...)
    {
        string r = "*" ~ args.length.to!string ~ "\r\n";
        foreach (a; args)
            r ~= "$" ~ a.length.to!string ~ "\r\n" ~ a ~ "\r\n";
        return r;
    }

    private string run(ref Keyspace ks, string[] cmdArgs...)
    {
        Arena arena;
        ByteBuffer o;
        RVal v;
        size_t pos = 0;
        propagationOverride.clear();
        auto encoded = cmdArgs.respCmd;
        parseValue(cast(const(ubyte)[]) encoded, pos, arena, v).expect.to.equal(ParseStatus.ok);
        v.dispatch(ks, o, arena);
        return (cast(string) o.data).idup;
    }

    @("valkey.hll.add_count_merge")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        // PFADD returns 1 when the estimate changed, 0 when it did not
        ks.run("PFADD", "hll", "a", "b", "c").expect.to.equal(":1\r\n");
        ks.run("PFADD", "hll", "a", "b", "c").expect.to.equal(":0\r\n"); // nothing new
        ks.run("PFADD", "hll", "d").expect.to.equal(":1\r\n");
        // small cardinality is exact
        ks.run("PFCOUNT", "hll").expect.to.equal(":4\r\n");

        // PFADD with no elements on a missing key still creates it (returns 1)
        ks.run("PFADD", "empty").expect.to.equal(":1\r\n");
        ks.run("PFCOUNT", "empty").expect.to.equal(":0\r\n");
        // PFCOUNT of a missing key is 0
        ks.run("PFCOUNT", "nope").expect.to.equal(":0\r\n");

        // PFCOUNT over multiple keys = cardinality of the union
        ks.run("PFADD", "h1", "a", "b", "c");
        ks.run("PFADD", "h2", "c", "d", "e");
        ks.run("PFCOUNT", "h1", "h2").expect.to.equal(":5\r\n"); // {a,b,c,d,e}

        // PFMERGE combines sources into dest
        ks.run("PFMERGE", "dst", "h1", "h2").expect.to.equal("+OK\r\n");
        ks.run("PFCOUNT", "dst").expect.to.equal(":5\r\n");
        // merging into an existing dest unions with it
        ks.run("PFADD", "h3", "e", "f");
        ks.run("PFMERGE", "dst", "h3").expect.to.equal("+OK\r\n");
        ks.run("PFCOUNT", "dst").expect.to.equal(":6\r\n"); // + f
    }
}
