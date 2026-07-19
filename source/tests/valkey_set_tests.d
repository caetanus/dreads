module tests.valkey_set_tests;

// Valkey unit/type/set.tcl core ops ported to native in-process UT (see
// valkey_incr_tests.d for rationale + THIRD_PARTY_NOTICES credit). Set replies are
// UNORDERED, so array results are compared as sorted multisets. intset/listpack
// encoding variants are the small-container UTs; DEBUG/fuzz cases stay in the sweep.

version (unittest)
{
    import fluent.asserts;
    import std.conv : to;
    import std.string : indexOf, startsWith;
    import std.algorithm : sort;

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

    // parse a RESP array of bulk strings into string[]
    private string[] parseArr(string s)
    {
        string[] r;
        if (s.length == 0 || s[0] != '*')
            return r;
        size_t i = 0;
        immutable nl = s.indexOf("\r\n");
        immutable n = s[1 .. nl].to!int;
        i = nl + 2;
        foreach (_; 0 .. n)
        {
            assert(s[i] == '$', "expected bulk in array");
            immutable e = s[i .. $].indexOf("\r\n") + i;
            immutable len = s[i + 1 .. e].to!int;
            i = e + 2;
            r ~= s[i .. i + len];
            i += len + 2;
        }
        return r;
    }

    // assert a RESP array equals `expected` as an UNORDERED set (sorted compare)
    private void sameSet(string reply, string[] expected...)
    {
        auto got = parseArr(reply);
        sort(got);
        auto exp = expected.dup;
        sort(exp);
        got.expect.to.equal(exp);
    }

    @("valkey.set.add_card_ismember_rem")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        // SADD returns the count of NEW members
        ks.run("SADD", "s", "a", "b", "c").expect.to.equal(":3\r\n");
        ks.run("SADD", "s", "a", "d").expect.to.equal(":1\r\n"); // only d is new
        ks.run("SCARD", "s").expect.to.equal(":4\r\n");

        // SISMEMBER / SMISMEMBER
        ks.run("SISMEMBER", "s", "a").expect.to.equal(":1\r\n");
        ks.run("SISMEMBER", "s", "zzz").expect.to.equal(":0\r\n");
        ks.run("SMISMEMBER", "s", "a", "zzz", "d").expect.to.equal("*3\r\n:1\r\n:0\r\n:1\r\n");

        // SMEMBERS (unordered)
        sameSet(ks.run("SMEMBERS", "s"), "a", "b", "c", "d");

        // SREM returns count removed; last removal drops the key
        ks.run("SREM", "s", "a", "zzz").expect.to.equal(":1\r\n");
        ks.run("SREM", "s", "b", "c", "d").expect.to.equal(":3\r\n");
        ks.run("EXISTS", "s").expect.to.equal(":0\r\n");
    }

    @("valkey.set.pop_move")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        // SPOP with count == size returns all members (unordered), empties the set
        ks.run("SADD", "s", "a", "b", "c");
        sameSet(ks.run("SPOP", "s", "10"), "a", "b", "c");
        ks.run("EXISTS", "s").expect.to.equal(":0\r\n");
        // SPOP on missing: nil bulk (no count) / empty array (count)
        ks.run("SPOP", "s").expect.to.equal("$-1\r\n");
        ks.run("SPOP", "s", "3").expect.to.equal("*0\r\n");

        // SMOVE moves a member between sets
        ks.run("SADD", "src", "a", "b");
        ks.run("SADD", "dst", "x");
        ks.run("SMOVE", "src", "dst", "a").expect.to.equal(":1\r\n");
        ks.run("SMOVE", "src", "dst", "zzz").expect.to.equal(":0\r\n"); // not a member
        ks.run("SISMEMBER", "dst", "a").expect.to.equal(":1\r\n");
        ks.run("SISMEMBER", "src", "a").expect.to.equal(":0\r\n");
    }

    @("valkey.set.inter_union_diff")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        ks.run("SADD", "s1", "a", "b", "c", "d");
        ks.run("SADD", "s2", "b", "c", "e");
        ks.run("SADD", "s3", "c", "f");

        sameSet(ks.run("SINTER", "s1", "s2"), "b", "c");
        sameSet(ks.run("SUNION", "s1", "s2"), "a", "b", "c", "d", "e");
        sameSet(ks.run("SDIFF", "s1", "s2"), "a", "d");
        // three-way
        sameSet(ks.run("SINTER", "s1", "s2", "s3"), "c");
        // SINTERCARD (Valkey/Redis 7): cardinality of the intersection, with LIMIT
        ks.run("SINTERCARD", "2", "s1", "s2").expect.to.equal(":2\r\n");
        ks.run("SINTERCARD", "2", "s1", "s2", "LIMIT", "1").expect.to.equal(":1\r\n");

        // *STORE variants return the stored cardinality and materialise the set
        ks.run("SINTERSTORE", "d1", "s1", "s2").expect.to.equal(":2\r\n");
        sameSet(ks.run("SMEMBERS", "d1"), "b", "c");
        ks.run("SUNIONSTORE", "d2", "s1", "s2").expect.to.equal(":5\r\n");
        sameSet(ks.run("SMEMBERS", "d2"), "a", "b", "c", "d", "e");
        ks.run("SDIFFSTORE", "d3", "s1", "s2").expect.to.equal(":2\r\n");
        sameSet(ks.run("SMEMBERS", "d3"), "a", "d");
    }
}
