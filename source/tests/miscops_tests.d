module tests.miscops_tests;

// LPOS/LMPOP/SORT/LCS/HRANDFIELD/HLL — happy paths plus the mandatory
// invalid/out-of-window cases.

version (unittest)
{
    import fluent.asserts;

    import dreads.commands;
    import dreads.mem : Arena, ByteBuffer;
    import dreads.obj : Keyspace;
    import dreads.resp;

    private string respCmd(string[] args...)
    {
        import std.conv : to;

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

    @("misc.lpos_lmpop")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();
        ks.run("RPUSH", "l", "a", "b", "c", "b", "b");
        ks.run("LPOS", "l", "b").expect.to.equal(":1\r\n");
        ks.run("LPOS", "l", "b", "RANK", "2").expect.to.equal(":3\r\n");
        ks.run("LPOS", "l", "b", "RANK", "-1").expect.to.equal(":4\r\n");
        ks.run("LPOS", "l", "b", "COUNT", "0").expect.to.equal("*3\r\n:1\r\n:3\r\n:4\r\n");
        ks.run("LPOS", "l", "b", "COUNT", "2").expect.to.equal("*2\r\n:1\r\n:3\r\n");
        ks.run("LPOS", "l", "zz").expect.to.equal("$-1\r\n");
        ks.run("LPOS", "l", "b", "RANK", "0")[0].expect.to.equal('-');
        ks.run("LPOS", "l", "b", "COUNT", "-1")[0].expect.to.equal('-');
        ks.run("LPOS", "ghost", "x").expect.to.equal("$-1\r\n");

        ks.run("LMPOP", "2", "nope", "l", "LEFT")
            .expect.to.equal("*2\r\n$1\r\nl\r\n*1\r\n$1\r\na\r\n");
        ks.run("LMPOP", "1", "l", "RIGHT", "COUNT", "10")[0 .. 4].expect.to.equal("*2\r\n");
        ks.run("EXISTS", "l").expect.to.equal(":0\r\n");
        ks.run("LMPOP", "1", "l", "LEFT").expect.to.equal("*-1\r\n");
        ks.run("LMPOP", "1", "l", "UP")[0].expect.to.equal('-');
        ks.run("LMPOP", "0", "LEFT")[0].expect.to.equal('-');
    }

    @("misc.sort")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();
        ks.run("RPUSH", "l", "3", "1", "2", "10");
        ks.run("SORT", "l").expect.to.equal("*4\r\n$1\r\n1\r\n$1\r\n2\r\n$1\r\n3\r\n$2\r\n10\r\n");
        ks.run("SORT", "l", "DESC")[0 .. 4].expect.to.equal("*4\r\n");
        ks.run("SORT", "l", "LIMIT", "1", "2").expect.to.equal("*2\r\n$1\r\n2\r\n$1\r\n3\r\n");
        ks.run("RPUSH", "w", "banana", "apple");
        ks.run("SORT", "w")[0].expect.to.equal('-'); // not numbers
        ks.run("SORT", "w", "ALPHA")
            .expect.to.equal("*2\r\n$5\r\napple\r\n$6\r\nbanana\r\n");
        ks.run("SADD", "s", "5", "3");
        ks.run("SORT", "s").expect.to.equal("*2\r\n$1\r\n3\r\n$1\r\n5\r\n");
        ks.run("SORT", "l", "STORE", "dst").expect.to.equal(":4\r\n");
        ks.run("TYPE", "dst").expect.to.equal("+list\r\n");
        ks.run("SORT_RO", "l", "STORE", "dst")[0].expect.to.equal('-'); // no STORE on _RO
        ks.run("SORT", "ghost").expect.to.equal("*0\r\n");
        ks.run("SET", "str", "v");
        ks.run("SORT", "str")[0].expect.to.equal('-');
    }

    @("misc.sort_by_get")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();
        ks.run("RPUSH", "l", "a", "b", "c");
        ks.run("MSET", "w_a", "3", "w_b", "1", "w_c", "2");
        ks.run("SORT", "l", "BY", "w_*", "ALPHA")
            .expect.to.equal("*3\r\n$1\r\nb\r\n$1\r\nc\r\n$1\r\na\r\n");
        ks.run("SORT", "l", "BY", "w_*") // numeric weights
            .expect.to.equal("*3\r\n$1\r\nb\r\n$1\r\nc\r\n$1\r\na\r\n");
        // GET projects through a pattern; '#' is the element itself
        ks.run("MSET", "d_a", "A", "d_b", "B", "d_c", "C");
        ks.run("SORT", "l", "BY", "w_*", "GET", "d_*", "GET", "#")
            .expect.to.equal("*6\r\n$1\r\nB\r\n$1\r\nb\r\n$1\r\nC\r\n$1\r\nc\r\n$1\r\nA\r\n$1\r\na\r\n");
        // missing GET key -> nil
        ks.run("SORT", "l", "BY", "w_*", "GET", "nope_*")
            .expect.to.equal("*3\r\n$-1\r\n$-1\r\n$-1\r\n");
        // BY without '*' skips sorting (container order)
        ks.run("SORT", "l", "BY", "nosort")
            .expect.to.equal("*3\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n");
        // hash-field weights and projections
        ks.run("HSET", "h_a", "w", "2", "v", "va");
        ks.run("HSET", "h_b", "w", "1", "v", "vb");
        ks.run("RPUSH", "l2", "a", "b");
        ks.run("SORT", "l2", "BY", "h_*->w", "GET", "h_*->v")
            .expect.to.equal("*2\r\n$2\r\nvb\r\n$2\r\nva\r\n");
        // STORE flattens GET projections; nil stores as empty string
        ks.run("SORT", "l2", "BY", "h_*->w", "GET", "ghost_*", "STORE", "dst2")
            .expect.to.equal(":2\r\n");
        ks.run("LRANGE", "dst2", "0", "-1").expect.to.equal("*2\r\n$0\r\n\r\n$0\r\n\r\n");
        // non-numeric weights without ALPHA -> error
        ks.run("MSET", "w_a", "x");
        ks.run("SORT", "l", "BY", "w_*")[0].expect.to.equal('-');
    }

    @("misc.lcs")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();
        // Redis doc fixture
        ks.run("MSET", "key1", "ohmytext", "key2", "mynewtext");
        ks.run("LCS", "key1", "key2").expect.to.equal("$6\r\nmytext\r\n");
        ks.run("LCS", "key1", "key2", "LEN").expect.to.equal(":6\r\n");
        auto idx = ks.run("LCS", "key1", "key2", "IDX", "MINMATCHLEN", "4");
        idx.expect.to.contain("matches");
        idx.expect.to.contain(":4\r\n:7\r\n"); // "text" in key1
        auto wml = ks.run("LCS", "key1", "key2", "IDX", "WITHMATCHLEN");
        wml.expect.to.contain(":6\r\n"); // total len
        ks.run("LCS", "key1", "key2", "LEN", "IDX")[0].expect.to.equal('-');
        ks.run("LCS", "ghost1", "ghost2").expect.to.equal("$0\r\n\r\n");
        ks.run("RPUSH", "l", "x");
        ks.run("LCS", "key1", "l")[0].expect.to.equal('-');
    }

    @("misc.hrandfield_hll")
    unittest
    {
        import std.conv : to;

        Keyspace ks;
        scope (exit)
            ks.d.free();
        ks.run("HSET", "h", "f1", "v1", "f2", "v2");
        ks.run("HRANDFIELD", "h")[0 .. 4].expect.to.equal("$2\r\n");
        ks.run("HRANDFIELD", "h", "5")[0 .. 4].expect.to.equal("*2\r\n");
        ks.run("HRANDFIELD", "h", "-4")[0 .. 4].expect.to.equal("*4\r\n");
        ks.run("HRANDFIELD", "h", "2", "WITHVALUES")[0 .. 4].expect.to.equal("*4\r\n");
        ks.run("HRANDFIELD", "ghost").expect.to.equal("$-1\r\n");

        // HLL: 1000 distinct elements must estimate within ~3%
        foreach (i; 0 .. 1000)
            ks.run("PFADD", "hll", "user" ~ i.to!string);
        auto reply = ks.run("PFCOUNT", "hll");
        reply[0].expect.to.equal(':');
        auto est = reply[1 .. $ - 2].to!long;
        (est > 970 && est < 1030).expect.to.equal(true);
        // adding existing elements reports 0
        ks.run("PFADD", "hll", "user1").expect.to.equal(":0\r\n");
        ks.run("PFADD", "hll", "brand-new").expect.to.equal(":1\r\n");
        // merge into a fresh dest and count multiple keys
        foreach (i; 0 .. 500)
            ks.run("PFADD", "hll2", "other" ~ i.to!string);
        ks.run("PFMERGE", "merged", "hll", "hll2").expect.to.equal("+OK\r\n");
        auto m = ks.run("PFCOUNT", "merged")[1 .. $ - 2].to!long;
        (m > 1420 && m < 1590).expect.to.equal(true);
        auto multi = ks.run("PFCOUNT", "hll", "hll2")[1 .. $ - 2].to!long;
        (multi > 1420 && multi < 1590).expect.to.equal(true);
        ks.run("PFCOUNT", "ghost").expect.to.equal(":0\r\n");
        // a non-HLL string is rejected
        ks.run("SET", "plain", "not-an-hll");
        ks.run("PFADD", "plain", "x")[0].expect.to.equal('-');
        ks.run("PFCOUNT", "plain")[0].expect.to.equal('-');
    }
}
