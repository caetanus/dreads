module tests.zsetops_tests;

// Sorted-set algebra + full range family (unified ZRANGE, lex, store forms).

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

    @("zops.unified_zrange")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();
        ks.run("ZADD", "z", "1", "a", "2", "b", "3", "c", "4", "d");

        // classic index form still works
        ks.run("ZRANGE", "z", "0", "1").expect.to.equal("*2\r\n$1\r\na\r\n$1\r\nb\r\n");
        // REV
        ks.run("ZRANGE", "z", "0", "1", "REV").expect.to.equal("*2\r\n$1\r\nd\r\n$1\r\nc\r\n");
        // BYSCORE with LIMIT
        ks.run("ZRANGE", "z", "(1", "+inf", "BYSCORE", "LIMIT", "1", "2")
            .expect.to.equal("*2\r\n$1\r\nc\r\n$1\r\nd\r\n");
        // BYSCORE REV: bounds come as (max, min)
        ks.run("ZRANGE", "z", "+inf", "-inf", "BYSCORE", "REV", "LIMIT", "0", "2")
            .expect.to.equal("*2\r\n$1\r\nd\r\n$1\r\nc\r\n");
        // BYLEX (uniform scores needed: rebuild)
        ks.run("ZADD", "lex", "0", "a", "0", "b", "0", "c", "0", "d");
        ks.run("ZRANGE", "lex", "[b", "[c", "BYLEX")
            .expect.to.equal("*2\r\n$1\r\nb\r\n$1\r\nc\r\n");
        ks.run("ZRANGE", "lex", "(a", "+", "BYLEX", "LIMIT", "0", "2")
            .expect.to.equal("*2\r\n$1\r\nb\r\n$1\r\nc\r\n");
        // LIMIT without BY* errors
        ks.run("ZRANGE", "z", "0", "1", "LIMIT", "0", "1")
            .expect.to.contain("LIMIT is only supported");
        // invalid option combinations are a syntax error, checked before the
        // bounds are parsed (so BYLEX+WITHSCORES doesn't fail on "0" as a lex
        // bound), and options a legacy form doesn't own are rejected
        ks.run("ZRANGE", "z", "0", "-1", "BYLEX", "WITHSCORES")
            .expect.to.equal("-ERR syntax error\r\n");
        ks.run("ZREVRANGE", "z", "0", "-1", "BYSCORE").expect.to.equal("-ERR syntax error\r\n");
        ks.run("ZRANGEBYSCORE", "z", "0", "-1", "REV").expect.to.equal("-ERR syntax error\r\n");
        // ZRANGESTORE rejects WITHSCORES and LIMIT-without-BY (was silently stored)
        ks.run("ZRANGESTORE", "d", "z", "0", "-1", "WITHSCORES").expect.to.equal("-ERR syntax error\r\n");
        ks.run("ZRANGESTORE", "d", "z", "0", "-1", "LIMIT", "1", "2")
            .expect.to.contain("LIMIT is only supported");
        // ZUNION with no keys names the command in the error
        ks.run("ZUNION", "0", "k").expect.to.contain("for 'zunion' command");
        ks.run("ZDIFFSTORE", "d", "0", "k").expect.to.contain("for 'zdiffstore' command");
        // legacy variants route through the same core
        ks.run("ZREVRANGEBYSCORE", "z", "3", "1")
            .expect.to.equal("*3\r\n$1\r\nc\r\n$1\r\nb\r\n$1\r\na\r\n");
        ks.run("ZRANGEBYLEX", "lex", "-", "(c")
            .expect.to.equal("*2\r\n$1\r\na\r\n$1\r\nb\r\n");
        ks.run("ZREVRANGEBYLEX", "lex", "+", "[c")
            .expect.to.equal("*2\r\n$1\r\nd\r\n$1\r\nc\r\n");
        ks.run("ZRANGEBYLEX", "lex", "b", "c")[0].expect.to.equal('-'); // needs [ or (
    }

    @("zops.lexcount_remrange_store")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();
        ks.run("ZADD", "lex", "0", "a", "0", "b", "0", "c", "0", "d");
        ks.run("ZLEXCOUNT", "lex", "-", "+").expect.to.equal(":4\r\n");
        ks.run("ZLEXCOUNT", "lex", "[b", "(d").expect.to.equal(":2\r\n");
        ks.run("ZLEXCOUNT", "ghost", "-", "+").expect.to.equal(":0\r\n");

        ks.run("ZADD", "z", "1", "a", "2", "b", "3", "c");
        ks.run("ZRANGESTORE", "dst", "z", "1", "2").expect.to.equal(":2\r\n");
        ks.run("ZRANGE", "dst", "0", "-1", "WITHSCORES")
            .expect.to.equal("*4\r\n$1\r\nb\r\n$1\r\n2\r\n$1\r\nc\r\n$1\r\n3\r\n");
        // empty result deletes destination
        ks.run("ZRANGESTORE", "dst", "z", "5", "9").expect.to.equal(":0\r\n");
        ks.run("EXISTS", "dst").expect.to.equal(":0\r\n");

        ks.run("ZREMRANGEBYLEX", "lex", "[b", "[c").expect.to.equal(":2\r\n");
        ks.run("ZRANGE", "lex", "0", "-1").expect.to.equal("*2\r\n$1\r\na\r\n$1\r\nd\r\n");
    }

    @("zops.randmember_mpop")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();
        ks.run("ZADD", "z", "1", "a", "2", "b");
        ks.run("ZRANDMEMBER", "z")[0 .. 4].expect.to.equal("$1\r\n"); // a or b
        ks.run("ZRANDMEMBER", "z", "5")[0 .. 4].expect.to.equal("*2\r\n");
        ks.run("ZRANDMEMBER", "z", "-3")[0 .. 4].expect.to.equal("*3\r\n"); // repeats
        ks.run("ZRANDMEMBER", "z", "2", "WITHSCORES")[0 .. 4].expect.to.equal("*4\r\n");
        ks.run("ZRANDMEMBER", "ghost").expect.to.equal("$-1\r\n");

        ks.run("ZMPOP", "2", "nope", "z", "MIN")
            .expect.to.equal("*2\r\n$1\r\nz\r\n*1\r\n*2\r\n$1\r\na\r\n$1\r\n1\r\n");
        ks.run("ZMPOP", "1", "z", "MAX", "COUNT", "5")
            .expect.to.equal("*2\r\n$1\r\nz\r\n*1\r\n*2\r\n$1\r\nb\r\n$1\r\n2\r\n");
        ks.run("EXISTS", "z").expect.to.equal(":0\r\n");
        ks.run("ZMPOP", "1", "z", "MIN").expect.to.equal("*-1\r\n");
        ks.run("ZMPOP", "1", "z", "SIDEWAYS")[0].expect.to.equal('-');
    }

    @("zops.union_inter_diff")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();
        ks.run("ZADD", "z1", "1", "a", "2", "b");
        ks.run("ZADD", "z2", "10", "b", "20", "c");
        ks.run("SADD", "s1", "b", "d"); // plain sets participate with score 1

        ks.run("ZUNIONSTORE", "u", "2", "z1", "z2").expect.to.equal(":3\r\n");
        ks.run("ZSCORE", "u", "b").expect.to.equal("$2\r\n12\r\n"); // SUM default
        ks.run("ZUNIONSTORE", "u", "2", "z1", "z2", "WEIGHTS", "1", "2")
            .expect.to.equal(":3\r\n");
        ks.run("ZSCORE", "u", "c").expect.to.equal("$2\r\n40\r\n");
        ks.run("ZUNIONSTORE", "u", "2", "z1", "z2", "AGGREGATE", "MAX")
            .expect.to.equal(":3\r\n");
        ks.run("ZSCORE", "u", "b").expect.to.equal("$2\r\n10\r\n");

        ks.run("ZINTERSTORE", "i", "2", "z1", "z2").expect.to.equal(":1\r\n");
        ks.run("ZSCORE", "i", "b").expect.to.equal("$2\r\n12\r\n");
        ks.run("ZINTERSTORE", "i", "2", "z1", "s1").expect.to.equal(":1\r\n");
        ks.run("ZSCORE", "i", "b").expect.to.equal("$1\r\n3\r\n"); // 2 + 1

        ks.run("ZDIFFSTORE", "d", "2", "z1", "z2").expect.to.equal(":1\r\n");
        ks.run("ZRANGE", "d", "0", "-1").expect.to.equal("*1\r\n$1\r\na\r\n");
        // empty store result deletes dst
        ks.run("ZDIFFSTORE", "d", "2", "z1", "z1").expect.to.equal(":0\r\n");
        ks.run("EXISTS", "d").expect.to.equal(":0\r\n");

        // non-store forms
        ks.run("ZUNION", "2", "z1", "z2", "WITHSCORES")[0 .. 4].expect.to.equal("*6\r\n");
        ks.run("ZINTER", "2", "z1", "z2").expect.to.equal("*1\r\n$1\r\nb\r\n");
        ks.run("ZDIFF", "2", "z1", "z2", "WITHSCORES")
            .expect.to.equal("*2\r\n$1\r\na\r\n$1\r\n1\r\n");
        ks.run("ZINTERCARD", "2", "z1", "z2").expect.to.equal(":1\r\n");
        ks.run("ZINTERCARD", "2", "z1", "z2", "LIMIT", "1").expect.to.equal(":1\r\n");

        // wrong type source
        ks.run("SET", "str", "x");
        ks.run("ZUNIONSTORE", "u", "2", "z1", "str")[0].expect.to.equal('-');
        ks.run("ZUNION", "0")[0].expect.to.equal('-'); // numkeys must be >= 1
    }
}
