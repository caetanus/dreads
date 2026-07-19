module tests.valkey_zset_tests;

// Valkey unit/type/zset.tcl core ops ported to native in-process UT (see
// valkey_incr_tests.d for rationale + THIRD_PARTY_NOTICES credit). zset replies are
// score-ORDERED, so array results compare exactly. listpack/skiplist encodings are
// the small-container UTs; fuzz/DEBUG cases stay in the blackbox sweep.

version (unittest)
{
    import fluent.asserts;
    import std.conv : to;
    import std.string : startsWith;

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

    private string bulk(string p)
    {
        return "$" ~ p.length.to!string ~ "\r\n" ~ p ~ "\r\n";
    }

    private string arrB(string[] items...)
    {
        string r = "*" ~ items.length.to!string ~ "\r\n";
        foreach (it; items)
            r ~= bulk(it);
        return r;
    }

    enum NIL = "$-1\r\n";

    @("valkey.zset.add_score_rank")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        // ZADD returns count of NEW members; order is by (score, member)
        ks.run("ZADD", "z", "10", "a", "20", "b", "30", "c").expect.to.equal(":3\r\n");
        ks.run("ZRANGE", "z", "0", "-1", "WITHSCORES").expect.to.equal(
                arrB("a", "10", "b", "20", "c", "30"));

        // update an existing score (returns 0 new); CH counts changed
        ks.run("ZADD", "z", "5", "a").expect.to.equal(":0\r\n");
        ks.run("ZADD", "z", "CH", "1", "a", "20", "b").expect.to.equal(":1\r\n"); // only a changed
        ks.run("ZSCORE", "z", "a").expect.to.equal(bulk("1"));
        ks.run("ZSCORE", "z", "missing").expect.to.equal(NIL);
        ks.run("ZMSCORE", "z", "a", "missing", "c").expect.to.equal(
                "*3\r\n" ~ bulk("1") ~ NIL ~ bulk("30"));

        // ZRANK / ZREVRANK (order after a=1,b=20,c=30)
        ks.run("ZRANK", "z", "a").expect.to.equal(":0\r\n");
        ks.run("ZRANK", "z", "c").expect.to.equal(":2\r\n");
        ks.run("ZREVRANK", "z", "a").expect.to.equal(":2\r\n");
        ks.run("ZRANK", "z", "missing").expect.to.equal(NIL);

        // ZCARD / ZCOUNT
        ks.run("ZCARD", "z").expect.to.equal(":3\r\n");
        ks.run("ZCOUNT", "z", "0", "20").expect.to.equal(":2\r\n"); // a=1, b=20
        ks.run("ZCOUNT", "z", "(1", "+inf").expect.to.equal(":2\r\n"); // exclusive of 1
    }

    @("valkey.zset.incr_rem_pop")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        ks.run("ZADD", "z", "1", "a", "2", "b", "3", "c");
        // ZINCRBY returns the new score (bulk)
        ks.run("ZINCRBY", "z", "5", "a").expect.to.equal(bulk("6"));
        ks.run("ZINCRBY", "z", "2.5", "new").expect.to.equal(bulk("2.5")); // creates member
        // ZADD INCR is the same as ZINCRBY, returns new score
        ks.run("ZADD", "z", "INCR", "1", "b").expect.to.equal(bulk("3"));

        // ZREM
        ks.run("ZREM", "z", "a", "zzz").expect.to.equal(":1\r\n");

        // ZPOPMIN / ZPOPMAX (member+score pair; count -> flat array)
        ks.run("DEL", "p");
        ks.run("ZADD", "p", "1", "a", "2", "b", "3", "c");
        ks.run("ZPOPMIN", "p").expect.to.equal(arrB("a", "1"));
        ks.run("ZPOPMAX", "p").expect.to.equal(arrB("c", "3"));
        ks.run("ZADD", "p", "1", "a", "3", "c");
        ks.run("ZPOPMIN", "p", "2").expect.to.equal(arrB("a", "1", "b", "2"));
    }

    @("valkey.zset.ranges")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        ks.run("ZADD", "z", "1", "a", "2", "b", "3", "c", "4", "d", "5", "e");

        // ZRANGE by index, REV, WITHSCORES
        ks.run("ZRANGE", "z", "0", "2").expect.to.equal(arrB("a", "b", "c"));
        ks.run("ZRANGE", "z", "0", "-1", "REV").expect.to.equal(arrB("e", "d", "c", "b", "a"));
        ks.run("ZREVRANGE", "z", "0", "1").expect.to.equal(arrB("e", "d"));

        // ZRANGEBYSCORE: inclusive, exclusive, -inf/+inf, LIMIT
        ks.run("ZRANGEBYSCORE", "z", "2", "4").expect.to.equal(arrB("b", "c", "d"));
        ks.run("ZRANGEBYSCORE", "z", "(2", "4").expect.to.equal(arrB("c", "d"));
        ks.run("ZRANGEBYSCORE", "z", "-inf", "+inf", "WITHSCORES").expect.to.equal(
                arrB("a", "1", "b", "2", "c", "3", "d", "4", "e", "5"));
        ks.run("ZRANGEBYSCORE", "z", "2", "5", "LIMIT", "1", "2").expect.to.equal(arrB("c", "d"));

        // ZRANGEBYLEX (all same score would be needed for pure lex; here scores differ
        // but lex range still applies on members via the ordered structure)
        ks.run("DEL", "lex");
        ks.run("ZADD", "lex", "0", "a", "0", "b", "0", "c", "0", "d");
        ks.run("ZRANGEBYLEX", "lex", "-", "+").expect.to.equal(arrB("a", "b", "c", "d"));
        ks.run("ZRANGEBYLEX", "lex", "[b", "(d").expect.to.equal(arrB("b", "c"));
        ks.run("ZLEXCOUNT", "lex", "-", "+").expect.to.equal(":4\r\n");
    }
}
