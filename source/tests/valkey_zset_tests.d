module tests.valkey_zset_tests;

// Valkey unit/type/zset.tcl ported to native in-process UT (see valkey_incr_tests.d
// for rationale + THIRD_PARTY_NOTICES credit). zset replies are score-ORDERED so
// array results compare exactly; unordered union/inter/diff spots use sameSet.
// SKIPPED: listpack/skiplist encoding-conversion duplicates (semantics ported once),
// fuzz loops, DEBUG RELOAD/htstats, blocking BZPOP/BZMPOP, replication streams,
// MULTI/EXEC, RESP3 map shapes, OBJECT ENCODING.

version (unittest)
{
    import fluent.asserts;
    import std.conv : to;
    import std.string : startsWith, indexOf;
    import std.algorithm : sort;

    import dreads.commands;
    import dreads.mem : Arena, ByteBuffer;
    import dreads.obj : Keyspace;
    import dreads.resp;

    private string respCmd(string[] a...)
    {
        string r = "*" ~ a.length.to!string ~ "\r\n";
        foreach (x; a)
            r ~= "$" ~ x.length.to!string ~ "\r\n" ~ x ~ "\r\n";
        return r;
    }

    private string run(ref Keyspace ks, string[] c...)
    {
        Arena arena;
        ByteBuffer o;
        RVal v;
        size_t p = 0;
        propagationOverride.clear();
        auto e = c.respCmd;
        parseValue(cast(const(ubyte)[]) e, p, arena, v).expect.to.equal(ParseStatus.ok);
        v.dispatch(ks, o, arena, 1_700_000_000_000UL);
        return (cast(string) o.data).idup;
    }

    private string bulk(string p)
    {
        return "$" ~ p.length.to!string ~ "\r\n" ~ p ~ "\r\n";
    }

    private string arrB(string[] it...)
    {
        string r = "*" ~ it.length.to!string ~ "\r\n";
        foreach (x; it)
            r ~= bulk(x);
        return r;
    }

    private string[] parseArr(string s)
    {
        string[] r;
        if (s.length == 0 || s[0] != '*')
            return r;
        immutable nl = s.indexOf("\r\n");
        immutable n = s[1 .. nl].to!int;
        size_t i = nl + 2;
        foreach (_; 0 .. n)
        {
            if (s[i] != '$')
                break;
            immutable ee = s[i .. $].indexOf("\r\n") + i;
            immutable ln = s[i + 1 .. ee].to!int;
            i = ee + 2;
            if (ln < 0)
            {
                r ~= null;
                continue;
            }
            r ~= s[i .. i + ln];
            i += ln + 2;
        }
        return r;
    }

    private void sameSet(string reply, string[] exp...)
    {
        auto g = parseArr(reply);
        sort(g);
        auto e = exp.dup;
        sort(e);
        g.expect.to.equal(e);
    }

    enum NIL = "$-1\r\n";
    enum NILARR = "*-1\r\n";
    enum EMPTY = "*0\r\n";

    // ---- ZADD basics: add, update, order, CH ----
    @("valkey.zset.zadd_basic")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        ks.run("ZADD", "z", "10", "x").expect.to.equal(":1\r\n");
        ks.run("ZADD", "z", "20", "y").expect.to.equal(":1\r\n");
        ks.run("ZADD", "z", "30", "z").expect.to.equal(":1\r\n");
        ks.run("ZRANGE", "z", "0", "-1").expect.to.equal(arrB("x", "y", "z"));
        // score update reorders
        ks.run("ZADD", "z", "1", "y").expect.to.equal(":0\r\n");
        ks.run("ZRANGE", "z", "0", "-1").expect.to.equal(arrB("y", "x", "z"));

        // Variadic base case + return value semantics
        ks.run("DEL", "m");
        ks.run("ZADD", "m", "10", "a", "20", "b", "30", "c").expect.to.equal(":3\r\n");
        ks.run("ZRANGE", "m", "0", "-1", "WITHSCORES").expect.to.equal(
                arrB("a", "10", "b", "20", "c", "30"));
        // only x is new
        ks.run("ZADD", "m", "5", "x", "20", "b", "30", "c").expect.to.equal(":1\r\n");
        ks.run("ZRANGE", "m", "0", "-1", "WITHSCORES").expect.to.equal(
                arrB("x", "5", "a", "10", "b", "20", "c", "30"));

        // CH: number of changed (added OR updated) elements
        ks.run("DEL", "c");
        ks.run("ZADD", "c", "10", "x", "20", "y", "30", "z");
        ks.run("ZADD", "c", "11", "x", "21", "y", "30", "z").expect.to.equal(":0\r\n");
        ks.run("ZADD", "c", "CH", "12", "x", "22", "y", "30", "z").expect.to.equal(":2\r\n");
    }

    // ---- ZADD flags: XX / NX / GT / LT / INCR ----
    @("valkey.zset.zadd_flags")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        // XX without existing key -> 0, key not created
        ks.run("ZADD", "z", "XX", "10", "x").expect.to.equal(":0\r\n");
        ks.run("TYPE", "z").expect.to.equal("+none\r\n");

        // XX existing -> only updates
        ks.run("ZADD", "z", "10", "x");
        ks.run("ZADD", "z", "XX", "20", "y").expect.to.equal(":0\r\n");
        ks.run("ZCARD", "z").expect.to.equal(":1\r\n");

        // XX returns number actually added (0 with XX)
        ks.run("DEL", "z");
        ks.run("ZADD", "z", "10", "x");
        ks.run("ZADD", "z", "10", "x", "20", "y", "30", "z").expect.to.equal(":2\r\n");

        // XX updates existing scores, skips new
        ks.run("DEL", "z");
        ks.run("ZADD", "z", "10", "x", "20", "y", "30", "z");
        ks.run("ZADD", "z", "XX", "5", "foo", "11", "x", "21", "y", "40", "zap");
        ks.run("ZCARD", "z").expect.to.equal(":3\r\n");
        ks.run("ZSCORE", "z", "x").expect.to.equal(bulk("11"));
        ks.run("ZSCORE", "z", "y").expect.to.equal(bulk("21"));

        // GT: only update when new score greater; CH counts
        ks.run("DEL", "z");
        ks.run("ZADD", "z", "10", "x", "20", "y", "30", "z");
        ks.run("ZADD", "z", "GT", "CH", "5", "foo", "11", "x", "21", "y", "29", "z")
            .expect.to.equal(":3\r\n"); // foo added, x&y up, z unchanged
        ks.run("ZCARD", "z").expect.to.equal(":4\r\n");
        ks.run("ZSCORE", "z", "x").expect.to.equal(bulk("11"));
        ks.run("ZSCORE", "z", "z").expect.to.equal(bulk("30"));

        // LT: only update when new score lower
        ks.run("DEL", "z");
        ks.run("ZADD", "z", "10", "x", "20", "y", "30", "z");
        ks.run("ZADD", "z", "LT", "CH", "5", "foo", "11", "x", "21", "y", "29", "z")
            .expect.to.equal(":2\r\n"); // foo added, z down; x,y not lower
        ks.run("ZSCORE", "z", "x").expect.to.equal(bulk("10"));
        ks.run("ZSCORE", "z", "z").expect.to.equal(bulk("29"));

        // GT XX: skip new members
        ks.run("DEL", "z");
        ks.run("ZADD", "z", "10", "x", "20", "y", "30", "z");
        ks.run("ZADD", "z", "GT", "XX", "CH", "5", "foo", "11", "x", "21", "y", "29", "z")
            .expect.to.equal(":2\r\n");
        ks.run("ZCARD", "z").expect.to.equal(":3\r\n");

        // LT XX
        ks.run("DEL", "z");
        ks.run("ZADD", "z", "10", "x", "20", "y", "30", "z");
        ks.run("ZADD", "z", "LT", "XX", "CH", "5", "foo", "11", "x", "21", "y", "29", "z")
            .expect.to.equal(":1\r\n");
        ks.run("ZCARD", "z").expect.to.equal(":3\r\n");

        // NX non-existing key adds all
        ks.run("DEL", "z");
        ks.run("ZADD", "z", "NX", "10", "x", "20", "y", "30", "z");
        ks.run("ZCARD", "z").expect.to.equal(":3\r\n");

        // NX only adds new, never updates
        ks.run("DEL", "z");
        ks.run("ZADD", "z", "10", "x", "20", "y", "30", "z");
        ks.run("ZADD", "z", "NX", "11", "x", "21", "y", "100", "a", "200", "b")
            .expect.to.equal(":2\r\n");
        ks.run("ZSCORE", "z", "x").expect.to.equal(bulk("10"));
        ks.run("ZSCORE", "z", "a").expect.to.equal(bulk("100"));

        // INCR works like ZINCRBY (returns new score)
        ks.run("DEL", "z");
        ks.run("ZADD", "z", "10", "x", "20", "y", "30", "z");
        ks.run("ZADD", "z", "INCR", "15", "x").expect.to.equal(bulk("25"));

        // INCR LT/GT returns nil (nil bulk) when the update is skipped
        ks.run("DEL", "z");
        ks.run("ZADD", "z", "28", "x");
        ks.run("ZADD", "z", "LT", "INCR", "1", "x").expect.to.equal(NIL);
        ks.run("ZSCORE", "z", "x").expect.to.equal(bulk("28"));
        ks.run("ZADD", "z", "GT", "INCR", "-1", "x").expect.to.equal(NIL);
        ks.run("ZSCORE", "z", "x").expect.to.equal(bulk("28"));
    }

    // ---- ZADD error / syntax cases ----
    @("valkey.zset.zadd_errors")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        // NaN not allowed
        ks.run("ZADD", "z", "nan", "abc").startsWith("-ERR").should.equal(true);
        // empty string not a valid score
        ks.run("ZADD", "z", "", "abc").startsWith("-ERR").should.equal(true);
        // incomplete score-member pair
        ks.run("ZADD", "z", "XX", "10", "x", "20").startsWith("-ERR").should.equal(true);
        // missing arg (odd tail)
        ks.run("ZADD", "z", "10", "a", "20", "b", "30", "c", "40").startsWith("-ERR").should.equal(true);
        // XX + NX incompatible
        ks.run("ZADD", "z", "XX", "NX", "10", "x").startsWith("-ERR").should.equal(true);
        // GT + NX, LT + NX, LT + GT incompatible
        ks.run("ZADD", "z", "GT", "NX", "10", "x").startsWith("-ERR").should.equal(true);
        ks.run("ZADD", "z", "LT", "NX", "10", "x").startsWith("-ERR").should.equal(true);
        ks.run("ZADD", "z", "LT", "GT", "10", "x").startsWith("-ERR").should.equal(true);
        // INCR only supports a single pair
        ks.run("ZADD", "z", "10", "x", "20", "y", "30", "z");
        ks.run("ZADD", "z", "INCR", "15", "x", "10", "y").startsWith("-ERR").should.equal(true);

        // Variadic parse error mid-way does not create/mutate key
        ks.run("DEL", "m");
        ks.run("ZADD", "m", "10", "a", "20", "b", "30.badscore", "c").startsWith("-ERR").should.equal(true);
        ks.run("EXISTS", "m").expect.to.equal(":0\r\n");
    }

    // ---- ZINCRBY ----
    @("valkey.zset.zincrby")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        // creates a new sorted set
        ks.run("ZINCRBY", "z", "1", "foo").expect.to.equal(bulk("1"));
        ks.run("ZRANGE", "z", "0", "-1").expect.to.equal(arrB("foo"));
        ks.run("ZSCORE", "z", "foo").expect.to.equal(bulk("1"));

        // increment and decrement
        ks.run("ZINCRBY", "z", "2", "foo").expect.to.equal(bulk("3"));
        ks.run("ZINCRBY", "z", "1", "bar").expect.to.equal(bulk("1"));
        ks.run("ZRANGE", "z", "0", "-1").expect.to.equal(arrB("bar", "foo"));
        ks.run("ZINCRBY", "z", "10", "bar");
        ks.run("ZINCRBY", "z", "-5", "foo");
        ks.run("ZINCRBY", "z", "-5", "bar");
        ks.run("ZRANGE", "z", "0", "-1").expect.to.equal(arrB("foo", "bar"));
        ks.run("ZSCORE", "z", "foo").expect.to.equal(bulk("-2"));
        ks.run("ZSCORE", "z", "bar").expect.to.equal(bulk("6"));

        // NaN result -> error
        ks.run("DEL", "n");
        ks.run("ZINCRBY", "n", "+inf", "abc").expect.to.equal(bulk("inf"));
        ks.run("ZINCRBY", "n", "-inf", "abc").startsWith("-ERR").should.equal(true);

        // NaN increment
        ks.run("ZINCRBY", "n", "nan", "abc").startsWith("-ERR").should.equal(true);

        // invalid incr value
        ks.run("DEL", "zi");
        ks.run("ZADD", "zi", "1", "one");
        ks.run("ZINCRBY", "zi", "v", "one").startsWith("-ERR").should.equal(true);
    }

    // ---- ZCARD / ZREM ----
    @("valkey.zset.card_rem")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        ks.run("ZADD", "z", "10", "a", "20", "b", "30", "c");
        ks.run("ZCARD", "z").expect.to.equal(":3\r\n");
        ks.run("ZCARD", "missing").expect.to.equal(":0\r\n");

        // ZREM removes key after last element
        ks.run("DEL", "z");
        ks.run("ZADD", "z", "10", "x", "20", "y");
        ks.run("EXISTS", "z").expect.to.equal(":1\r\n");
        ks.run("ZREM", "z", "zz").expect.to.equal(":0\r\n");
        ks.run("ZREM", "z", "y").expect.to.equal(":1\r\n");
        ks.run("ZREM", "z", "x").expect.to.equal(":1\r\n");
        ks.run("EXISTS", "z").expect.to.equal(":0\r\n");

        // variadic ZREM counts only actually-removed members
        ks.run("ZADD", "z", "10", "a", "20", "b", "30", "c");
        ks.run("ZREM", "z", "x", "y", "a", "b", "k").expect.to.equal(":2\r\n");
        ks.run("ZREM", "z", "foo", "bar").expect.to.equal(":0\r\n");
        ks.run("ZREM", "z", "c").expect.to.equal(":1\r\n");
        ks.run("EXISTS", "z").expect.to.equal(":0\r\n");
    }

    // ---- ZSCORE / ZMSCORE ----
    @("valkey.zset.score")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        ks.run("ZADD", "z", "10", "x", "20", "y");
        ks.run("ZSCORE", "z", "x").expect.to.equal(bulk("10"));
        ks.run("ZSCORE", "z", "missing").expect.to.equal(NIL);

        // ZMSCORE mixed present/missing -> nil bulks interleaved
        ks.run("ZMSCORE", "z", "x", "y").expect.to.equal("*2\r\n" ~ bulk("10") ~ bulk("20"));
        ks.run("ZMSCORE", "z", "x", "miss").expect.to.equal("*2\r\n" ~ bulk("10") ~ NIL);
        ks.run("ZMSCORE", "z", "x").expect.to.equal("*1\r\n" ~ bulk("10"));

        // empty set -> all nil
        ks.run("DEL", "e");
        ks.run("ZMSCORE", "e", "x", "y").expect.to.equal("*2\r\n" ~ NIL ~ NIL);

        // float formatting
        ks.run("DEL", "f");
        ks.run("ZADD", "f", "3.5", "a", "1.0", "b");
        ks.run("ZSCORE", "f", "a").expect.to.equal(bulk("3.5"));
        ks.run("ZSCORE", "f", "b").expect.to.equal(bulk("1"));
        // inf
        ks.run("DEL", "i");
        ks.run("ZADD", "i", "+inf", "a", "-inf", "b");
        ks.run("ZSCORE", "i", "a").expect.to.equal(bulk("inf"));
        ks.run("ZSCORE", "i", "b").expect.to.equal(bulk("-inf"));
        // double max round-trips to scientific notation
        ks.run("DEL", "dm");
        ks.run("ZADD", "dm", "1.7976931348623157e+308", "dblmax");
        ks.run("ZSCORE", "dm", "dblmax").expect.to.equal(bulk("1.7976931348623157e+308"));
    }

    // ---- ZRANK / ZREVRANK (with and without WITHSCORE) ----
    @("valkey.zset.rank")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        ks.run("ZADD", "z", "10", "x", "20", "y", "30", "z");
        ks.run("ZRANK", "z", "x").expect.to.equal(":0\r\n");
        ks.run("ZRANK", "z", "y").expect.to.equal(":1\r\n");
        ks.run("ZRANK", "z", "z").expect.to.equal(":2\r\n");
        ks.run("ZREVRANK", "z", "x").expect.to.equal(":2\r\n");
        ks.run("ZREVRANK", "z", "z").expect.to.equal(":0\r\n");
        // missing member -> nil bulk
        ks.run("ZRANK", "z", "foo").expect.to.equal(NIL);
        ks.run("ZREVRANK", "z", "foo").expect.to.equal(NIL);

        // WITHSCORE -> [rank, score] pair
        ks.run("ZRANK", "z", "x", "WITHSCORE").expect.to.equal("*2\r\n" ~ ":0\r\n" ~ bulk("10"));
        ks.run("ZRANK", "z", "z", "WITHSCORE").expect.to.equal("*2\r\n" ~ ":2\r\n" ~ bulk("30"));
        ks.run("ZREVRANK", "z", "x", "WITHSCORE").expect.to.equal("*2\r\n" ~ ":2\r\n" ~ bulk("10"));
        ks.run("ZREVRANK", "z", "z", "WITHSCORE").expect.to.equal("*2\r\n" ~ ":0\r\n" ~ bulk("30"));
        // missing member with WITHSCORE -> nil ARRAY
        ks.run("ZRANK", "z", "foo", "WITHSCORE").expect.to.equal(NILARR);
        ks.run("ZREVRANK", "z", "foo", "WITHSCORE").expect.to.equal(NILARR);

        // after deletion ranks compact
        ks.run("ZREM", "z", "y");
        ks.run("ZRANK", "z", "x").expect.to.equal(":0\r\n");
        ks.run("ZRANK", "z", "z").expect.to.equal(":1\r\n");
        ks.run("ZRANK", "z", "z", "WITHSCORE").expect.to.equal("*2\r\n" ~ ":1\r\n" ~ bulk("30"));
    }

    // ---- ZRANGE / ZREVRANGE by index ----
    @("valkey.zset.range_index")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        ks.run("ZADD", "z", "1", "a", "2", "b", "3", "c", "4", "d");

        ks.run("ZRANGE", "z", "0", "-1").expect.to.equal(arrB("a", "b", "c", "d"));
        ks.run("ZRANGE", "z", "0", "-2").expect.to.equal(arrB("a", "b", "c"));
        ks.run("ZRANGE", "z", "1", "-1").expect.to.equal(arrB("b", "c", "d"));
        ks.run("ZRANGE", "z", "-2", "-1").expect.to.equal(arrB("c", "d"));
        ks.run("ZRANGE", "z", "-2", "-2").expect.to.equal(arrB("c"));
        // out of range start
        ks.run("ZRANGE", "z", "-5", "2").expect.to.equal(arrB("a", "b", "c"));
        ks.run("ZRANGE", "z", "5", "-1").expect.to.equal(EMPTY);
        // out of range end
        ks.run("ZRANGE", "z", "0", "5").expect.to.equal(arrB("a", "b", "c", "d"));
        ks.run("ZRANGE", "z", "0", "-5").expect.to.equal(EMPTY);
        // withscores
        ks.run("ZRANGE", "z", "0", "-1", "WITHSCORES").expect.to.equal(
                arrB("a", "1", "b", "2", "c", "3", "d", "4"));
        // REV modifier
        ks.run("ZRANGE", "z", "0", "-1", "REV").expect.to.equal(arrB("d", "c", "b", "a"));

        // ZREVRANGE
        ks.run("ZREVRANGE", "z", "0", "-1").expect.to.equal(arrB("d", "c", "b", "a"));
        ks.run("ZREVRANGE", "z", "0", "-2").expect.to.equal(arrB("d", "c", "b"));
        ks.run("ZREVRANGE", "z", "1", "-1").expect.to.equal(arrB("c", "b", "a"));
        ks.run("ZREVRANGE", "z", "-2", "-1").expect.to.equal(arrB("b", "a"));
        ks.run("ZREVRANGE", "z", "5", "-1").expect.to.equal(EMPTY);
        ks.run("ZREVRANGE", "z", "0", "-5").expect.to.equal(EMPTY);
        ks.run("ZREVRANGE", "z", "0", "-1", "WITHSCORES").expect.to.equal(
                arrB("d", "4", "c", "3", "b", "2", "a", "1"));
    }

    // ---- ZRANGEBYSCORE / ZREVRANGEBYSCORE / ZCOUNT ----
    @("valkey.zset.range_byscore")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        // -inf a, 1 b, 2 c, 3 d, 4 e, 5 f, +inf g
        ks.run("ZADD", "z", "-inf", "a", "1", "b", "2", "c", "3", "d",
                "4", "e", "5", "f", "+inf", "g");

        // inclusive
        ks.run("ZRANGEBYSCORE", "z", "-inf", "2").expect.to.equal(arrB("a", "b", "c"));
        ks.run("ZRANGEBYSCORE", "z", "0", "3").expect.to.equal(arrB("b", "c", "d"));
        ks.run("ZRANGEBYSCORE", "z", "4", "+inf").expect.to.equal(arrB("e", "f", "g"));
        ks.run("ZREVRANGEBYSCORE", "z", "3", "0").expect.to.equal(arrB("d", "c", "b"));
        ks.run("ZREVRANGEBYSCORE", "z", "+inf", "4").expect.to.equal(arrB("g", "f", "e"));
        ks.run("ZCOUNT", "z", "0", "3").expect.to.equal(":3\r\n");

        // exclusive
        ks.run("ZRANGEBYSCORE", "z", "(-inf", "(2").expect.to.equal(arrB("b"));
        ks.run("ZRANGEBYSCORE", "z", "(0", "(3").expect.to.equal(arrB("b", "c"));
        ks.run("ZREVRANGEBYSCORE", "z", "(3", "(0").expect.to.equal(arrB("c", "b"));
        ks.run("ZCOUNT", "z", "(0", "(3").expect.to.equal(":2\r\n");

        // withscores
        ks.run("ZRANGEBYSCORE", "z", "0", "3", "WITHSCORES").expect.to.equal(
                arrB("b", "1", "c", "2", "d", "3"));
        ks.run("ZREVRANGEBYSCORE", "z", "3", "0", "WITHSCORES").expect.to.equal(
                arrB("d", "3", "c", "2", "b", "1"));

        // LIMIT
        ks.run("ZRANGEBYSCORE", "z", "0", "10", "LIMIT", "0", "2").expect.to.equal(arrB("b", "c"));
        ks.run("ZRANGEBYSCORE", "z", "0", "10", "LIMIT", "2", "3").expect.to.equal(
                arrB("d", "e", "f"));
        ks.run("ZRANGEBYSCORE", "z", "0", "10", "LIMIT", "20", "10").expect.to.equal(EMPTY);
        ks.run("ZREVRANGEBYSCORE", "z", "10", "0", "LIMIT", "0", "2").expect.to.equal(arrB("f", "e"));
        ks.run("ZREVRANGEBYSCORE", "z", "10", "0", "LIMIT", "2", "3").expect.to.equal(
                arrB("d", "c", "b"));
        ks.run("ZRANGEBYSCORE", "z", "2", "5", "LIMIT", "2", "3", "WITHSCORES").expect.to.equal(
                arrB("e", "4", "f", "5"));

        // empty ranges
        ks.run("ZRANGEBYSCORE", "z", "4", "2").expect.to.equal(EMPTY);
        ks.run("ZRANGEBYSCORE", "z", "(2", "2").expect.to.equal(EMPTY);
        ks.run("ZRANGEBYSCORE", "z", "2.4", "2.6").expect.to.equal(EMPTY);
        ks.run("ZRANGEBYSCORE", "z", "(2.4", "(2.6").expect.to.equal(EMPTY);

        // non-float min/max -> error
        ks.run("ZRANGEBYSCORE", "z", "str", "1").startsWith("-ERR").should.equal(true);
        ks.run("ZRANGEBYSCORE", "z", "1", "str").startsWith("-ERR").should.equal(true);
        ks.run("ZRANGEBYSCORE", "z", "1", "NaN").startsWith("-ERR").should.equal(true);
    }

    // ---- ZRANGEBYLEX / ZREVRANGEBYLEX / ZLEXCOUNT ----
    @("valkey.zset.range_bylex")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        ks.run("ZADD", "z", "0", "alpha", "0", "bar", "0", "cool", "0", "down",
                "0", "elephant", "0", "foo", "0", "great", "0", "hill", "0", "omega");

        // inclusive range
        ks.run("ZRANGEBYLEX", "z", "-", "[cool").expect.to.equal(arrB("alpha", "bar", "cool"));
        ks.run("ZRANGEBYLEX", "z", "[bar", "[down").expect.to.equal(arrB("bar", "cool", "down"));
        ks.run("ZRANGEBYLEX", "z", "[g", "+").expect.to.equal(arrB("great", "hill", "omega"));
        ks.run("ZREVRANGEBYLEX", "z", "[cool", "-").expect.to.equal(arrB("cool", "bar", "alpha"));
        ks.run("ZREVRANGEBYLEX", "z", "+", "[great").expect.to.equal(arrB("omega", "hill", "great"));
        ks.run("ZLEXCOUNT", "z", "[ele", "[h").expect.to.equal(":3\r\n");

        // exclusive range
        ks.run("ZRANGEBYLEX", "z", "-", "(cool").expect.to.equal(arrB("alpha", "bar"));
        ks.run("ZRANGEBYLEX", "z", "(bar", "(down").expect.to.equal(arrB("cool"));
        ks.run("ZRANGEBYLEX", "z", "(great", "+").expect.to.equal(arrB("hill", "omega"));
        ks.run("ZREVRANGEBYLEX", "z", "(cool", "-").expect.to.equal(arrB("bar", "alpha"));
        ks.run("ZLEXCOUNT", "z", "(ele", "(great").expect.to.equal(":2\r\n");

        // empty / inverted
        ks.run("ZRANGEBYLEX", "z", "(az", "(b").expect.to.equal(EMPTY);
        ks.run("ZRANGEBYLEX", "z", "(z", "+").expect.to.equal(EMPTY);
        ks.run("ZRANGEBYLEX", "z", "-", "[aaaa").expect.to.equal(EMPTY);
        ks.run("ZREVRANGEBYLEX", "z", "(hill", "(omega").expect.to.equal(EMPTY);

        // ZLEXCOUNT advanced
        ks.run("ZLEXCOUNT", "z", "-", "+").expect.to.equal(":9\r\n");
        ks.run("ZLEXCOUNT", "z", "+", "-").expect.to.equal(":0\r\n");
        ks.run("ZLEXCOUNT", "z", "+", "[c").expect.to.equal(":0\r\n");
        ks.run("ZLEXCOUNT", "z", "[bar", "+").expect.to.equal(":8\r\n");
        ks.run("ZLEXCOUNT", "z", "[bar", "[foo").expect.to.equal(":5\r\n");
        ks.run("ZLEXCOUNT", "z", "[bar", "(foo").expect.to.equal(":4\r\n");
        ks.run("ZLEXCOUNT", "z", "(bar", "(foo").expect.to.equal(":3\r\n");
        ks.run("ZLEXCOUNT", "z", "-", "(foo").expect.to.equal(":5\r\n");
        ks.run("ZLEXCOUNT", "z", "(maxstring", "+").expect.to.equal(":1\r\n");

        // ZRANGEBYLEX LIMIT
        ks.run("ZRANGEBYLEX", "z", "-", "[cool", "LIMIT", "0", "2").expect.to.equal(arrB("alpha", "bar"));
        ks.run("ZRANGEBYLEX", "z", "-", "[cool", "LIMIT", "1", "2").expect.to.equal(arrB("bar", "cool"));
        ks.run("ZRANGEBYLEX", "z", "[bar", "[down", "LIMIT", "0", "0").expect.to.equal(EMPTY);
        ks.run("ZRANGEBYLEX", "z", "[bar", "[down", "LIMIT", "0", "1").expect.to.equal(arrB("bar"));
        ks.run("ZRANGEBYLEX", "z", "[bar", "[down", "LIMIT", "1", "1").expect.to.equal(arrB("cool"));

        // invalid lex range specifiers
        ks.run("ZRANGEBYLEX", "z", "foo", "bar").startsWith("-ERR").should.equal(true);
        ks.run("ZRANGEBYLEX", "z", "[foo", "bar").startsWith("-ERR").should.equal(true);
        ks.run("ZRANGEBYLEX", "z", "+x", "[bar").startsWith("-ERR").should.equal(true);
        ks.run("ZRANGEBYLEX", "z", "-x", "[bar").startsWith("-ERR").should.equal(true);
    }

    // ---- ZREMRANGEBYSCORE / BYRANK / BYLEX ----
    @("valkey.zset.remrange")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        // ZREMRANGEBYSCORE
        ks.run("ZADD", "z", "1", "a", "2", "b", "3", "c", "4", "d", "5", "e");
        ks.run("ZREMRANGEBYSCORE", "z", "2", "4").expect.to.equal(":3\r\n");
        ks.run("ZRANGE", "z", "0", "-1").expect.to.equal(arrB("a", "e"));
        // switch min/max removes nothing
        ks.run("DEL", "z");
        ks.run("ZADD", "z", "1", "a", "2", "b", "3", "c", "4", "d", "5", "e");
        ks.run("ZREMRANGEBYSCORE", "z", "4", "2").expect.to.equal(":0\r\n");
        // exclusive
        ks.run("ZREMRANGEBYSCORE", "z", "(1", "5").expect.to.equal(":4\r\n");
        ks.run("ZRANGE", "z", "0", "-1").expect.to.equal(arrB("a"));
        // destroy when empty
        ks.run("ZREMRANGEBYSCORE", "z", "1", "5").expect.to.equal(":1\r\n");
        ks.run("EXISTS", "z").expect.to.equal(":0\r\n");
        // non-value min/max
        ks.run("ZREMRANGEBYSCORE", "z", "str", "1").startsWith("-ERR").should.equal(true);
        ks.run("ZREMRANGEBYSCORE", "z", "1", "NaN").startsWith("-ERR").should.equal(true);

        // ZREMRANGEBYRANK
        ks.run("DEL", "r");
        ks.run("ZADD", "r", "1", "a", "2", "b", "3", "c", "4", "d", "5", "e");
        ks.run("ZREMRANGEBYRANK", "r", "1", "3").expect.to.equal(":3\r\n");
        ks.run("ZRANGE", "r", "0", "-1").expect.to.equal(arrB("a", "e"));
        // start overflow / underflow removes nothing
        ks.run("DEL", "r");
        ks.run("ZADD", "r", "1", "a", "2", "b", "3", "c", "4", "d", "5", "e");
        ks.run("ZREMRANGEBYRANK", "r", "10", "-1").expect.to.equal(":0\r\n");
        ks.run("ZREMRANGEBYRANK", "r", "0", "-10").expect.to.equal(":0\r\n");
        // end overflow removes all and destroys
        ks.run("ZREMRANGEBYRANK", "r", "0", "10").expect.to.equal(":5\r\n");
        ks.run("EXISTS", "r").expect.to.equal(":0\r\n");

        // ZREMRANGEBYLEX
        ks.run("DEL", "l");
        ks.run("ZADD", "l", "0", "alpha", "0", "bar", "0", "cool", "0", "down", "0", "elephant");
        ks.run("ZREMRANGEBYLEX", "l", "-", "[cool").expect.to.equal(":3\r\n");
        ks.run("ZRANGE", "l", "0", "-1").expect.to.equal(arrB("down", "elephant"));
        // inverted lex removes nothing
        ks.run("DEL", "l");
        ks.run("ZADD", "l", "0", "alpha", "0", "bar", "0", "cool");
        ks.run("ZREMRANGEBYLEX", "l", "(z", "+").expect.to.equal(":0\r\n");
        // destroy when empty (whole range)
        ks.run("ZREMRANGEBYLEX", "l", "-", "+").expect.to.equal(":3\r\n");
        ks.run("EXISTS", "l").expect.to.equal(":0\r\n");
    }

    // ---- ZPOPMIN / ZPOPMAX ----
    @("valkey.zset.pop")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        // basic single-key: pop returns [member, score]; deletes key when empty
        ks.run("ZADD", "z", "-1", "a", "1", "b", "2", "c", "3", "d", "4", "e");
        ks.run("ZPOPMIN", "z").expect.to.equal(arrB("a", "-1"));
        ks.run("ZPOPMIN", "z").expect.to.equal(arrB("b", "1"));
        ks.run("ZPOPMAX", "z").expect.to.equal(arrB("e", "4"));
        ks.run("ZPOPMAX", "z").expect.to.equal(arrB("d", "3"));
        ks.run("ZPOPMIN", "z").expect.to.equal(arrB("c", "2"));
        ks.run("EXISTS", "z").expect.to.equal(":0\r\n");

        // with count -> flat member/score array
        ks.run("ZADD", "z1", "0", "a", "1", "b", "2", "c", "3", "d");
        ks.run("ZPOPMIN", "z1", "2").expect.to.equal(arrB("a", "0", "b", "1"));
        ks.run("ZPOPMAX", "z1", "2").expect.to.equal(arrB("d", "3", "c", "2"));

        // count 0 -> empty array; key unchanged
        ks.run("DEL", "z0");
        ks.run("ZADD", "z0", "1", "a", "2", "b", "3", "c");
        ks.run("ZPOPMIN", "z0", "0").expect.to.equal(EMPTY);
        ks.run("ZPOPMAX", "z0", "0").expect.to.equal(EMPTY);
        ks.run("ZCARD", "z0").expect.to.equal(":3\r\n");

        // non-existing key -> empty array (with and without count)
        ks.run("DEL", "ne");
        ks.run("ZPOPMIN", "ne").expect.to.equal(EMPTY);
        ks.run("ZPOPMIN", "ne", "1").expect.to.equal(EMPTY);
        ks.run("ZPOPMAX", "ne").expect.to.equal(EMPTY);

        // negative count -> error
        ks.run("ZPOPMIN", "z0", "-1").startsWith("-ERR").should.equal(true);
        ks.run("ZPOPMAX", "z0", "-2").startsWith("-ERR").should.equal(true);

        // wrong type
        ks.run("SET", "s", "bar");
        ks.run("ZPOPMIN", "s").startsWith("-WRONGTYPE").should.equal(true);
        ks.run("ZPOPMIN", "s", "0").startsWith("-WRONGTYPE").should.equal(true);
        ks.run("ZPOPMAX", "s").startsWith("-WRONGTYPE").should.equal(true);
        ks.run("ZPOPMAX", "s", "2").startsWith("-WRONGTYPE").should.equal(true);
    }

    // ---- ZMPOP ----
    @("valkey.zset.zmpop")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        // shape: *2 -> [ key , [ [member,score], ... ] ]
        ks.run("ZADD", "z", "1", "a", "2", "b", "3", "c");
        ks.run("ZMPOP", "1", "z", "MIN").expect.to.equal(
                "*2\r\n" ~ bulk("z") ~ "*1\r\n" ~ "*2\r\n" ~ bulk("a") ~ bulk("1"));
        // with count -> multiple inner pairs (MAX order: c then b)
        ks.run("ZMPOP", "1", "z", "MAX", "COUNT", "2").expect.to.equal(
                "*2\r\n" ~ bulk("z") ~ "*2\r\n"
                ~ "*2\r\n" ~ bulk("c") ~ bulk("3")
                ~ "*2\r\n" ~ bulk("b") ~ bulk("2"));

        // non-existing keys -> nil array
        ks.run("DEL", "ne");
        ks.run("ZMPOP", "1", "ne", "MIN").expect.to.equal(NILARR);
        ks.run("ZMPOP", "2", "ne", "ne2", "MAX", "COUNT", "1").expect.to.equal(NILARR);

        // first non-empty key wins among many
        ks.run("DEL", "k1", "k2");
        ks.run("ZADD", "k2", "5", "x", "6", "y");
        ks.run("ZMPOP", "2", "k1", "k2", "MIN").expect.to.equal(
                "*2\r\n" ~ bulk("k2") ~ "*1\r\n" ~ "*2\r\n" ~ bulk("x") ~ bulk("5"));

        // wrong type
        ks.run("SET", "s", "bar");
        ks.run("ZMPOP", "1", "s", "MIN").startsWith("-WRONGTYPE").should.equal(true);
        ks.run("ZMPOP", "1", "s", "MAX", "COUNT", "200").startsWith("-WRONGTYPE").should.equal(true);

        // illegal arguments
        ks.run("ZMPOP").startsWith("-ERR").should.equal(true);
        ks.run("ZMPOP", "1").startsWith("-ERR").should.equal(true);
        ks.run("ZMPOP", "1", "z").startsWith("-ERR").should.equal(true);
        ks.run("ZMPOP", "0", "z", "MIN").startsWith("-ERR").should.equal(true);
        ks.run("ZMPOP", "a", "z", "MIN").startsWith("-ERR").should.equal(true);
        ks.run("ZMPOP", "-1", "z", "MAX").startsWith("-ERR").should.equal(true);
        ks.run("ZMPOP", "1", "z", "bad_where").startsWith("-ERR").should.equal(true);
        ks.run("ZMPOP", "1", "z", "MIN", "bar_arg").startsWith("-ERR").should.equal(true);
        ks.run("ZMPOP", "1", "z", "MAX", "MIN").startsWith("-ERR").should.equal(true);
        ks.run("ZMPOP", "1", "z", "COUNT").startsWith("-ERR").should.equal(true);
        ks.run("ZMPOP", "1", "z", "MAX", "COUNT", "1", "COUNT", "2").startsWith("-ERR").should.equal(true);
        ks.run("ZMPOP", "1", "z", "MIN", "COUNT", "0").startsWith("-ERR").should.equal(true);
        ks.run("ZMPOP", "1", "z", "MAX", "COUNT", "a").startsWith("-ERR").should.equal(true);
        ks.run("ZMPOP", "1", "z", "MIN", "COUNT", "-1").startsWith("-ERR").should.equal(true);
    }

    // ---- ZUNIONSTORE / ZUNION ----
    @("valkey.zset.union")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        // union against non-existing key doesn't set destination
        ks.run("ZUNIONSTORE", "dst", "1", "empty").expect.to.equal(":0\r\n");
        ks.run("EXISTS", "dst").expect.to.equal(":0\r\n");

        ks.run("ZADD", "a", "1", "a", "2", "b", "3", "c");
        ks.run("ZADD", "b", "1", "b", "2", "c", "3", "d");

        // basics: scores summed, result sorted by (score, member)
        ks.run("ZUNIONSTORE", "c", "2", "a", "b").expect.to.equal(":4\r\n");
        ks.run("ZRANGE", "c", "0", "-1", "WITHSCORES").expect.to.equal(
                arrB("a", "1", "b", "3", "d", "3", "c", "5"));

        // ZUNION direct with WITHSCORES
        ks.run("ZUNION", "2", "a", "b", "WITHSCORES").expect.to.equal(
                arrB("a", "1", "b", "3", "d", "3", "c", "5"));

        // union with an empty set = just the non-empty one
        ks.run("DEL", "sb");
        ks.run("ZUNION", "2", "a", "sb", "WITHSCORES").expect.to.equal(
                arrB("a", "1", "b", "2", "c", "3"));

        // WEIGHTS
        ks.run("ZUNIONSTORE", "c", "2", "a", "b", "WEIGHTS", "2", "3").expect.to.equal(":4\r\n");
        ks.run("ZRANGE", "c", "0", "-1", "WITHSCORES").expect.to.equal(
                arrB("a", "2", "b", "7", "d", "9", "c", "12"));

        // AGGREGATE MIN / MAX
        ks.run("ZUNIONSTORE", "c", "2", "a", "b", "AGGREGATE", "MIN");
        ks.run("ZRANGE", "c", "0", "-1", "WITHSCORES").expect.to.equal(
                arrB("a", "1", "b", "1", "c", "2", "d", "3"));
        ks.run("ZUNIONSTORE", "c", "2", "a", "b", "AGGREGATE", "MAX");
        ks.run("ZRANGE", "c", "0", "-1", "WITHSCORES").expect.to.equal(
                arrB("a", "1", "b", "2", "c", "3", "d", "3"));

        // union with a regular set (set members counted as score 1)
        ks.run("DEL", "seta");
        ks.run("SADD", "seta", "a", "b", "c");
        ks.run("ZUNIONSTORE", "c", "2", "seta", "b", "WEIGHTS", "2", "3").expect.to.equal(":4\r\n");
        ks.run("ZRANGE", "c", "0", "-1", "WITHSCORES").expect.to.equal(
                arrB("a", "2", "b", "5", "c", "8", "d", "9"));

        // regression: weights 0 must not create NaN from -inf
        ks.run("DEL", "zi");
        ks.run("ZADD", "zi", "-inf", "neginf");
        ks.run("ZUNIONSTORE", "o", "1", "zi", "WEIGHTS", "0");
        ks.run("ZRANGE", "o", "0", "-1", "WITHSCORES").expect.to.equal(arrB("neginf", "0"));

        // NaN weights -> error
        ks.run("DEL", "i1", "i2");
        ks.run("ZADD", "i1", "1", "k");
        ks.run("ZADD", "i2", "1", "k");
        ks.run("ZUNIONSTORE", "i3", "2", "i1", "i2", "WEIGHTS", "nan", "nan").startsWith("-ERR").should.equal(true);
    }

    // ---- ZINTERSTORE / ZINTER / ZINTERCARD ----
    @("valkey.zset.inter")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        ks.run("ZADD", "a", "1", "a", "2", "b", "3", "c");
        ks.run("ZADD", "b", "1", "b", "2", "c", "3", "d");

        // basics: intersection scores summed
        ks.run("ZINTERSTORE", "c", "2", "a", "b").expect.to.equal(":2\r\n");
        ks.run("ZRANGE", "c", "0", "-1", "WITHSCORES").expect.to.equal(arrB("b", "3", "c", "5"));
        ks.run("ZINTER", "2", "a", "b", "WITHSCORES").expect.to.equal(arrB("b", "3", "c", "5"));

        // WEIGHTS / AGGREGATE
        ks.run("ZINTER", "2", "a", "b", "WEIGHTS", "2", "3", "WITHSCORES").expect.to.equal(
                arrB("b", "7", "c", "12"));
        ks.run("ZINTER", "2", "a", "b", "AGGREGATE", "MIN", "WITHSCORES").expect.to.equal(
                arrB("b", "1", "c", "2"));
        ks.run("ZINTER", "2", "a", "b", "AGGREGATE", "MAX", "WITHSCORES").expect.to.equal(
                arrB("b", "2", "c", "3"));

        // intersection with a regular set
        ks.run("DEL", "seta");
        ks.run("SADD", "seta", "a", "b", "c");
        ks.run("ZINTERSTORE", "c", "2", "seta", "b", "WEIGHTS", "2", "3").expect.to.equal(":2\r\n");
        ks.run("ZRANGE", "c", "0", "-1", "WITHSCORES").expect.to.equal(arrB("b", "5", "c", "8"));

        // empty intersection with an empty set
        ks.run("DEL", "sb");
        ks.run("ZINTER", "2", "a", "sb", "WITHSCORES").expect.to.equal(EMPTY);

        // ZINTERCARD
        ks.run("ZINTERCARD", "2", "a", "b").expect.to.equal(":2\r\n");
        ks.run("ZINTERCARD", "2", "a", "b", "LIMIT", "0").expect.to.equal(":2\r\n");
        ks.run("ZINTERCARD", "2", "a", "b", "LIMIT", "1").expect.to.equal(":1\r\n");
        ks.run("ZINTERCARD", "2", "a", "b", "LIMIT", "10").expect.to.equal(":2\r\n");

        // ZINTERCARD illegal arguments
        ks.run("ZINTERCARD", "1", "a", "bar_arg").startsWith("-ERR").should.equal(true);
        ks.run("ZINTERCARD", "1", "a", "LIMIT").startsWith("-ERR").should.equal(true);
        ks.run("ZINTERCARD", "1", "a", "LIMIT", "-1").startsWith("-ERR").should.equal(true);
        ks.run("ZINTERCARD", "1", "a", "LIMIT", "a").startsWith("-ERR").should.equal(true);

        // regression: intset+hashtable intersection is empty
        ks.run("DEL", "s1", "s2");
        ks.run("SADD", "s1", "a");
        ks.run("SADD", "s2", "10");
        ks.run("ZINTERSTORE", "s3", "2", "s1", "s2").expect.to.equal(":0\r\n");

        // #516 regression: mixed sets and zsets with weights 0 0 1
        ks.run("DEL", "one", "two", "three");
        ks.run("SADD", "one", "100", "101", "102", "103");
        ks.run("SADD", "two", "100", "200", "201", "202");
        ks.run("ZADD", "three", "1", "500", "1", "501", "1", "502", "1", "503", "1", "100");
        ks.run("ZINTERSTORE", "th", "3", "one", "two", "three", "WEIGHTS", "0", "0", "1")
            .expect.to.equal(":1\r\n");
        ks.run("ZRANGE", "th", "0", "-1").expect.to.equal(arrB("100"));
    }

    // ---- ZDIFF / ZDIFFSTORE ----
    @("valkey.zset.diff")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        ks.run("ZADD", "a", "1", "a", "2", "b", "3", "c");
        ks.run("ZADD", "b", "1", "b", "2", "c", "3", "d");

        ks.run("ZDIFFSTORE", "c", "2", "a", "b").expect.to.equal(":1\r\n");
        ks.run("ZRANGE", "c", "0", "-1", "WITHSCORES").expect.to.equal(arrB("a", "1"));
        ks.run("ZDIFF", "2", "a", "b", "WITHSCORES").expect.to.equal(arrB("a", "1"));

        // diff with a regular set
        ks.run("DEL", "seta");
        ks.run("SADD", "seta", "a", "b", "c");
        ks.run("ZDIFFSTORE", "c", "2", "seta", "b").expect.to.equal(":1\r\n");
        ks.run("ZRANGE", "c", "0", "-1", "WITHSCORES").expect.to.equal(arrB("a", "1"));

        // subtracting a set from itself -> empty
        ks.run("ZDIFFSTORE", "c", "2", "a", "a").expect.to.equal(":0\r\n");
        ks.run("ZRANGE", "c", "0", "-1").expect.to.equal(EMPTY);

        // multi-key algorithm 2
        ks.run("DEL", "za", "zb", "zc", "zd", "ze");
        ks.run("ZADD", "za", "1", "a", "2", "b", "3", "c", "5", "e");
        ks.run("ZADD", "zb", "1", "b");
        ks.run("ZADD", "zc", "1", "c");
        ks.run("ZADD", "zd", "1", "d");
        ks.run("ZDIFFSTORE", "ze", "4", "za", "zb", "zc", "zd").expect.to.equal(":2\r\n");
        ks.run("ZRANGE", "ze", "0", "-1", "WITHSCORES").expect.to.equal(arrB("a", "1", "e", "5"));

        // empty result early
        ks.run("DEL", "x", "y", "z");
        ks.run("ZADD", "x", "1", "a", "2", "b");
        ks.run("ZADD", "y", "1", "a", "2", "b");
        ks.run("ZDIFFSTORE", "z", "2", "x", "y").expect.to.equal(":0\r\n");
        ks.run("ZRANGE", "z", "0", "-1").expect.to.equal(EMPTY);
    }

    // ---- Union/Inter/Diff error and empty-key cases ----
    @("valkey.zset.setop_errors")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        // against non-existing keys
        ks.run("DEL", "ne");
        ks.run("ZUNION", "1", "ne").expect.to.equal(EMPTY);
        ks.run("ZINTER", "1", "ne").expect.to.equal(EMPTY);
        ks.run("ZINTERCARD", "1", "ne").expect.to.equal(":0\r\n");
        ks.run("ZINTERCARD", "1", "ne", "LIMIT", "0").expect.to.equal(":0\r\n");
        ks.run("ZDIFF", "1", "ne").expect.to.equal(EMPTY);

        // WITHSCORES not valid for the *STORE variants
        ks.run("ZADD", "d", "1", "a");
        ks.run("ZADD", "f", "1", "b");
        ks.run("ZUNIONSTORE", "foo", "2", "d", "f", "WITHSCORES").startsWith("-ERR").should.equal(true);
        ks.run("ZINTERSTORE", "foo", "2", "d", "f", "WITHSCORES").startsWith("-ERR").should.equal(true);
        ks.run("ZDIFFSTORE", "foo", "2", "d", "f", "WITHSCORES").startsWith("-ERR").should.equal(true);

        // at least 1 input key needed (numkeys 0)
        ks.run("ZUNION", "0", "k").startsWith("-ERR").should.equal(true);
        ks.run("ZUNIONSTORE", "dst", "0", "k").startsWith("-ERR").should.equal(true);
        ks.run("ZINTER", "0", "k").startsWith("-ERR").should.equal(true);
        ks.run("ZINTERSTORE", "dst", "0", "k").startsWith("-ERR").should.equal(true);
        ks.run("ZDIFF", "0", "k").startsWith("-ERR").should.equal(true);
        ks.run("ZDIFFSTORE", "dst", "0", "k").startsWith("-ERR").should.equal(true);
        ks.run("ZINTERCARD", "0", "k").startsWith("-ERR").should.equal(true);
    }

    // ---- ZRANGESTORE ----
    @("valkey.zset.rangestore")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        ks.run("ZADD", "z1", "1", "a", "2", "b", "3", "c", "4", "d");

        // basic full range
        ks.run("ZRANGESTORE", "z2", "z1", "0", "-1").expect.to.equal(":4\r\n");
        ks.run("ZRANGE", "z2", "0", "-1", "WITHSCORES").expect.to.equal(
                arrB("a", "1", "b", "2", "c", "3", "d", "4"));

        // index range
        ks.run("ZRANGESTORE", "z2", "z1", "1", "2").expect.to.equal(":2\r\n");
        ks.run("ZRANGE", "z2", "0", "-1", "WITHSCORES").expect.to.equal(arrB("b", "2", "c", "3"));

        // BYLEX
        ks.run("ZRANGESTORE", "z2", "z1", "[b", "[c", "BYLEX").expect.to.equal(":2\r\n");
        ks.run("ZRANGE", "z2", "0", "-1", "WITHSCORES").expect.to.equal(arrB("b", "2", "c", "3"));

        // BYSCORE
        ks.run("ZRANGESTORE", "z2", "z1", "1", "2", "BYSCORE").expect.to.equal(":2\r\n");
        ks.run("ZRANGE", "z2", "0", "-1", "WITHSCORES").expect.to.equal(arrB("a", "1", "b", "2"));

        // BYSCORE LIMIT
        ks.run("ZRANGESTORE", "z2", "z1", "0", "5", "BYSCORE", "LIMIT", "0", "2").expect.to.equal(":2\r\n");
        ks.run("ZRANGE", "z2", "0", "-1", "WITHSCORES").expect.to.equal(arrB("a", "1", "b", "2"));

        // BYSCORE REV LIMIT
        ks.run("ZRANGESTORE", "z2", "z1", "5", "0", "BYSCORE", "REV", "LIMIT", "0", "2")
            .expect.to.equal(":2\r\n");
        ks.run("ZRANGE", "z2", "0", "-1", "WITHSCORES").expect.to.equal(arrB("c", "3", "d", "4"));

        // src key missing -> 0, dest not created
        ks.run("DEL", "z2");
        ks.run("ZRANGESTORE", "z2", "missing", "0", "-1").expect.to.equal(":0\r\n");
        ks.run("EXISTS", "z2").expect.to.equal(":0\r\n");

        // src wrong type -> WRONGTYPE, existing dest untouched
        ks.run("ZADD", "z2", "1", "a");
        ks.run("SET", "s", "bar");
        ks.run("ZRANGESTORE", "z2", "s", "0", "-1").startsWith("-WRONGTYPE").should.equal(true);
        ks.run("ZRANGE", "z2", "0", "-1").expect.to.equal(arrB("a"));

        // empty range -> 0, dest deleted
        ks.run("ZRANGESTORE", "z2", "z1", "5", "6").expect.to.equal(":0\r\n");
        ks.run("EXISTS", "z2").expect.to.equal(":0\r\n");
        ks.run("ZADD", "z2", "1", "a");
        ks.run("ZRANGESTORE", "z2", "z1", "[f", "[g", "BYLEX").expect.to.equal(":0\r\n");
        ks.run("EXISTS", "z2").expect.to.equal(":0\r\n");
        ks.run("ZADD", "z2", "1", "a");
        ks.run("ZRANGESTORE", "z2", "z1", "5", "6", "BYSCORE").expect.to.equal(":0\r\n");
        ks.run("EXISTS", "z2").expect.to.equal(":0\r\n");

        // invalid syntax
        ks.run("ZRANGESTORE", "z2", "z1", "0", "-1", "LIMIT", "1", "2").startsWith("-ERR").should.equal(true);
        ks.run("ZRANGESTORE", "z2", "z1", "0", "-1", "WITHSCORES").startsWith("-ERR").should.equal(true);
    }

    // ---- ZRANGE modern options (BYSCORE / BYLEX / REV) ----
    @("valkey.zset.range_modern")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        ks.run("ZADD", "z1", "1", "a", "2", "b", "3", "c", "4", "d");

        // BYSCORE REV LIMIT WITHSCORES
        ks.run("ZRANGE", "z1", "5", "0", "BYSCORE", "REV", "LIMIT", "0", "2", "WITHSCORES")
            .expect.to.equal(arrB("d", "4", "c", "3"));
        // BYLEX
        ks.run("ZRANGE", "z1", "[b", "[c", "BYLEX").expect.to.equal(arrB("b", "c"));

        // invalid syntax combinations
        ks.run("ZRANGE", "z1", "0", "-1", "LIMIT", "1", "2").startsWith("-ERR").should.equal(true);
        ks.run("ZRANGE", "z1", "0", "-1", "BYLEX", "WITHSCORES").startsWith("-ERR").should.equal(true);
        ks.run("ZREVRANGE", "z1", "0", "-1", "BYSCORE").startsWith("-ERR").should.equal(true);
        ks.run("ZRANGEBYSCORE", "z1", "0", "-1", "REV").startsWith("-ERR").should.equal(true);
    }

    // ---- ZRANDMEMBER ----
    @("valkey.zset.randmember")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        ks.run("ZADD", "z", "1", "a", "2", "b", "3", "c");

        // single member (no count) returns a bare bulk that must belong to the set
        auto m = ks.run("ZRANDMEMBER", "z");
        (m == bulk("a") || m == bulk("b") || m == bulk("c")).expect.to.equal(true);

        // count of 0 -> empty array
        ks.run("ZRANDMEMBER", "z", "0").expect.to.equal(EMPTY);
        // non-existing key with count -> empty array
        ks.run("ZRANDMEMBER", "nonexisting", "100").expect.to.equal(EMPTY);

        // positive count >= set size returns exactly the distinct members
        sameSet(ks.run("ZRANDMEMBER", "z", "10"), "a", "b", "c");

        // negative count -> repetition allowed, exact requested length
        parseArr(ks.run("ZRANDMEMBER", "z", "-5")).length.expect.to.equal(5);
        // negative count WITHSCORES -> 2x length
        parseArr(ks.run("ZRANDMEMBER", "z", "-3", "WITHSCORES")).length.expect.to.equal(6);

        // count overflow -> out of range error
        ks.run("ZRANDMEMBER", "z", "-9223372036854775808", "WITHSCORES").startsWith("-ERR").should.equal(true);
        ks.run("ZRANDMEMBER", "z", "-9223372036854775808").startsWith("-ERR").should.equal(true);
    }
}
