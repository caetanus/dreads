module tests.commands_ext_tests;

// Extended command surface (phase B of the 1:1 effort): keyspace extras,
// string/list/hash/set/zset tails. UT-style named tests + fluent asserts.

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

    @("ext.rename")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();
        ks.run("SET", "a", "va");
        ks.run("EXPIRE", "a", "1000");
        ks.run("RENAME", "a", "b").expect.to.equal("+OK\r\n");
        ks.run("EXISTS", "a").expect.to.equal(":0\r\n");
        ks.run("GET", "b").expect.to.equal("$2\r\nva\r\n");
        ks.run("TTL", "b").expect.to.equal(":1000\r\n"); // TTL moves with the key
        ks.run("RENAME", "ghost", "x").expect.to.equal("-ERR no such key\r\n");
        ks.run("SET", "c", "vc");
        ks.run("RENAMENX", "c", "b").expect.to.equal(":0\r\n"); // dest exists
        ks.run("RENAMENX", "c", "d").expect.to.equal(":1\r\n");
        ks.run("RENAMENX", "ghost", "x").expect.to.equal("-ERR no such key\r\n");
        // RENAME over a different type frees the old destination
        ks.run("RPUSH", "lst", "x");
        ks.run("RENAME", "b", "lst").expect.to.equal("+OK\r\n");
        ks.run("TYPE", "lst").expect.to.equal("+string\r\n");
    }

    @("ext.server_misc")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();
        auto time = ks.run("TIME");
        time[0 .. 4].expect.to.equal("*2\r\n");
        ks.run("SELECT", "0").expect.to.equal("+OK\r\n");
        ks.run("SELECT", "3").expect.to.equal("-ERR DB index is out of range\r\n");
        ks.run("CONFIG", "GET", "maxmemory").expect.to.equal("*0\r\n");
        ks.run("CONFIG", "SET", "x", "y").expect.to.equal("+OK\r\n");
        ks.run("INFO").expect.to.contain("redis_version");
        ks.run("SET", "k", "v");
        ks.run("FLUSHDB").expect.to.equal("+OK\r\n");
        ks.run("DBSIZE").expect.to.equal(":0\r\n");
    }

    @("ext.string_ranges_and_floats")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();
        ks.run("SET", "s", "Hello World");
        ks.run("GETRANGE", "s", "0", "4").expect.to.equal("$5\r\nHello\r\n");
        ks.run("GETRANGE", "s", "-5", "-1").expect.to.equal("$5\r\nWorld\r\n");
        ks.run("GETRANGE", "s", "50", "60").expect.to.equal("$0\r\n\r\n");
        ks.run("GETRANGE", "ghost", "0", "1").expect.to.equal("$0\r\n\r\n");

        ks.run("SETRANGE", "s", "6", "Redis").expect.to.equal(":11\r\n");
        ks.run("GET", "s").expect.to.equal("$11\r\nHello Redis\r\n");
        ks.run("SETRANGE", "pad", "3", "x").expect.to.equal(":4\r\n");
        ks.run("GET", "pad").expect.to.equal("$4\r\n\0\0\0x\r\n");
        ks.run("SETRANGE", "s", "-1", "x")[0].expect.to.equal('-');

        ks.run("INCRBYFLOAT", "f", "10.5").expect.to.equal("$4\r\n10.5\r\n");
        ks.run("INCRBYFLOAT", "f", "0.1").expect.to.contain("10.6");
        ks.run("INCRBYFLOAT", "s", "1")[0].expect.to.equal('-'); // not a float
        // float results are propagated as SET (never re-derived)
        ks.run("INCRBYFLOAT", "f2", "2.5");
        (cast(string) propagationOverride.data).expect.to.contain("SET");

        ks.run("MSETNX", "n1", "a", "n2", "b").expect.to.equal(":1\r\n");
        ks.run("MSETNX", "n2", "x", "n3", "y").expect.to.equal(":0\r\n");
        ks.run("EXISTS", "n3").expect.to.equal(":0\r\n"); // all-or-nothing
    }

    @("ext.list_tail")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();
        ks.run("RPUSH", "l", "a", "b", "c", "d", "e");
        ks.run("LTRIM", "l", "1", "3").expect.to.equal("+OK\r\n");
        ks.run("LRANGE", "l", "0", "-1").expect.to.equal("*3\r\n$1\r\nb\r\n$1\r\nc\r\n$1\r\nd\r\n");
        ks.run("LTRIM", "l", "5", "10").expect.to.equal("+OK\r\n"); // empty range deletes
        ks.run("EXISTS", "l").expect.to.equal(":0\r\n");
        ks.run("LTRIM", "ghost", "0", "1").expect.to.equal("+OK\r\n");

        ks.run("RPUSH", "l2", "a", "c");
        ks.run("LINSERT", "l2", "BEFORE", "c", "b").expect.to.equal(":3\r\n");
        ks.run("LINSERT", "l2", "AFTER", "c", "d").expect.to.equal(":4\r\n");
        ks.run("LRANGE", "l2", "0", "-1")
            .expect.to.equal("*4\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n$1\r\nd\r\n");
        ks.run("LINSERT", "l2", "BEFORE", "zz", "x").expect.to.equal(":-1\r\n");
        ks.run("LINSERT", "ghost", "BEFORE", "a", "x").expect.to.equal(":0\r\n");
        ks.run("LINSERT", "l2", "SIDEWAYS", "a", "x").expect.to.equal("-ERR syntax error\r\n");

        ks.run("RPUSH", "src", "1", "2", "3");
        ks.run("RPOPLPUSH", "src", "dst").expect.to.equal("$1\r\n3\r\n");
        ks.run("LMOVE", "src", "dst", "LEFT", "RIGHT").expect.to.equal("$1\r\n1\r\n");
        ks.run("LRANGE", "dst", "0", "-1").expect.to.equal("*2\r\n$1\r\n3\r\n$1\r\n1\r\n");
        ks.run("LRANGE", "src", "0", "-1").expect.to.equal("*1\r\n$1\r\n2\r\n");
        // rotation on the same key
        ks.run("RPUSH", "rot", "x", "y");
        ks.run("LMOVE", "rot", "rot", "RIGHT", "LEFT").expect.to.equal("$1\r\ny\r\n");
        ks.run("LRANGE", "rot", "0", "-1").expect.to.equal("*2\r\n$1\r\ny\r\n$1\r\nx\r\n");
        // source emptied by the move disappears
        ks.run("LMOVE", "src", "dst", "LEFT", "RIGHT");
        ks.run("EXISTS", "src").expect.to.equal(":0\r\n");
        ks.run("LMOVE", "ghost", "dst", "LEFT", "LEFT").expect.to.equal("$-1\r\n");
    }

    @("ext.hash_tail")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();
        ks.run("HSETNX", "h", "f", "v1").expect.to.equal(":1\r\n");
        ks.run("HSETNX", "h", "f", "v2").expect.to.equal(":0\r\n");
        ks.run("HGET", "h", "f").expect.to.equal("$2\r\nv1\r\n");
        ks.run("HSTRLEN", "h", "f").expect.to.equal(":2\r\n");
        ks.run("HSTRLEN", "h", "nope").expect.to.equal(":0\r\n");
        ks.run("HSTRLEN", "ghost", "f").expect.to.equal(":0\r\n");
    }

    @("ext.set_tail")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();
        ks.run("SADD", "s", "a", "b", "c");
        ks.run("SPOP", "s").expect.to.startWith("$1\r\n");
        // SPOP propagates as SREM of the popped member
        (cast(string) propagationOverride.data).expect.to.contain("SREM");
        ks.run("SCARD", "s").expect.to.equal(":2\r\n");
        ks.run("SPOP", "s", "10")[0 .. 4].expect.to.equal("*2\r\n");
        ks.run("EXISTS", "s").expect.to.equal(":0\r\n"); // emptied set vanishes
        ks.run("SPOP", "ghost").expect.to.equal("$-1\r\n");
        ks.run("SPOP", "ghost", "2").expect.to.equal("*0\r\n");

        ks.run("SADD", "r", "m1", "m2");
        ks.run("SRANDMEMBER", "r").expect.to.startWith("$2\r\nm");
        ks.run("SRANDMEMBER", "r", "5")[0 .. 4].expect.to.equal("*2\r\n");
        ks.run("SRANDMEMBER", "r", "-5")[0 .. 4].expect.to.equal("*5\r\n"); // repeats
        ks.run("SCARD", "r").expect.to.equal(":2\r\n"); // read-only

        ks.run("SADD", "m src".dup.idup, "x"); // key with space, why not
        ks.run("SADD", "msrc", "keep", "move");
        ks.run("SMOVE", "msrc", "mdst", "move").expect.to.equal(":1\r\n");
        ks.run("SMOVE", "msrc", "mdst", "ghost").expect.to.equal(":0\r\n");
        ks.run("SISMEMBER", "mdst", "move").expect.to.equal(":1\r\n");
        ks.run("SMISMEMBER", "msrc", "keep", "move")
            .expect.to.equal("*2\r\n:1\r\n:0\r\n");

        ks.run("SADD", "i1", "a", "b", "c");
        ks.run("SADD", "i2", "b", "c", "d");
        ks.run("SINTERCARD", "2", "i1", "i2").expect.to.equal(":2\r\n");
        ks.run("SINTERCARD", "2", "i1", "i2", "LIMIT", "1").expect.to.equal(":1\r\n");
        ks.run("SINTERCARD", "2", "i1", "ghost").expect.to.equal(":0\r\n");

        ks.run("SINTERSTORE", "outI", "i1", "i2").expect.to.equal(":2\r\n");
        ks.run("SMEMBERS", "outI")[0 .. 4].expect.to.equal("*2\r\n");
        ks.run("SUNIONSTORE", "outU", "i1", "i2").expect.to.equal(":4\r\n");
        ks.run("SDIFFSTORE", "outD", "i1", "i2").expect.to.equal(":1\r\n");
        ks.run("SMEMBERS", "outD").expect.to.equal("*1\r\n$1\r\na\r\n");
        // storing an empty result deletes the destination
        ks.run("SDIFFSTORE", "outD", "i1", "i1").expect.to.equal(":0\r\n");
        ks.run("EXISTS", "outD").expect.to.equal(":0\r\n");
    }

    @("ext.scan_family")
    unittest
    {
        import std.algorithm : canFind;
        import std.conv : to;

        Keyspace ks;
        scope (exit)
            ks.d.free();
        foreach (i; 0 .. 30)
            ks.run("SET", "key:" ~ i.to!string, "v");
        ks.run("SET", "other", "v");

        // full iteration through cursors must visit every key exactly once
        size_t seen = 0;
        string cursor = "0";
        do
        {
            auto reply = ks.run("SCAN", cursor, "COUNT", "7");
            reply[0 .. 4].expect.to.equal("*2\r\n");
            // cursor is the first bulk; items counted by "key:"/"other" hits
            auto payload = reply;
            size_t at = 4;
            // parse cursor bulk: $n\r\n<digits>\r\n
            size_t lineEnd = at;
            while (payload[lineEnd] != '\n')
                lineEnd++;
            auto clen = payload[at + 1 .. lineEnd - 1].to!size_t;
            cursor = payload[lineEnd + 1 .. lineEnd + 1 + clen].idup;
            foreach (i; 0 .. payload.length - 1)
                if (payload[i .. $].length >= 4 && payload[i .. i + 4] == "key:")
                    seen++;
            if (payload.canFind("other"))
                seen++;
        }
        while (cursor != "0");
        seen.expect.to.equal(31);

        // MATCH filters
        auto m = ks.run("SCAN", "0", "MATCH", "key:1?", "COUNT", "1000");
        m.expect.to.contain("key:15");
        m.expect.to.not.contain("other");

        ks.run("HSET", "h", "f1", "v1", "f2", "v2");
        auto h = ks.run("HSCAN", "h", "0", "COUNT", "100");
        h.expect.to.contain("f1");
        h.expect.to.contain("v1");
        auto hn = ks.run("HSCAN", "h", "0", "COUNT", "100", "NOVALUES");
        hn.expect.to.contain("f1");
        hn.expect.to.not.contain("v1");

        ks.run("SADD", "s", "m1", "m2");
        ks.run("SSCAN", "s", "0").expect.to.contain("m1");

        ks.run("ZADD", "z", "1", "a", "2", "b", "3", "c");
        auto z1 = ks.run("ZSCAN", "z", "0", "COUNT", "2");
        z1.expect.to.contain("a");
        z1.expect.to.not.contain("c");
        z1.expect.to.contain("$1\r\n2\r\n"); // cursor advanced to rank 2
        auto z2 = ks.run("ZSCAN", "z", "2", "COUNT", "2");
        z2.expect.to.contain("c");
        z2[0 .. 10].expect.to.equal("*2\r\n$1\r\n0\r"); // wrapped
        ks.run("SCAN", "0", "BOGUS").expect.to.equal("-ERR syntax error\r\n");
    }

    @("ext.stream_tail")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();
        ks.run("XADD", "s", "1-1", "a", "1");
        ks.run("XADD", "s", "2-1", "b", "2");
        ks.run("XADD", "s", "3-1", "c", "3");
        ks.run("XADD", "s", "4-1", "d", "4");

        ks.run("XDEL", "s", "2-1", "9-9").expect.to.equal(":1\r\n");
        ks.run("XLEN", "s").expect.to.equal(":3\r\n");
        ks.run("XRANGE", "s", "-", "+").expect.to.not.contain("2-1");
        ks.run("XDEL", "ghost", "1-1").expect.to.equal(":0\r\n");

        ks.run("XTRIM", "s", "MAXLEN", "2").expect.to.equal(":1\r\n");
        ks.run("XRANGE", "s", "-", "+").expect.to.not.contain("1-1");
        ks.run("XTRIM", "s", "MAXLEN", "~", "1").expect.to.equal(":1\r\n");
        ks.run("XLEN", "s").expect.to.equal(":1\r\n");
        ks.run("XTRIM", "s", "MAXLEN", "0").expect.to.equal(":1\r\n");
        // an emptied stream still exists and remembers lastId
        ks.run("EXISTS", "s").expect.to.equal(":1\r\n");
        ks.run("XADD", "s", "1-1", "x", "y")[0].expect.to.equal('-'); // id <= lastId
        ks.run("XTRIM", "s", "WRONG", "1").expect.to.equal("-ERR syntax error\r\n");
    }

    @("ext.zset_tail")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();
        ks.run("ZADD", "z", "1", "a", "2", "b", "3", "c", "4", "d");
        ks.run("ZCOUNT", "z", "2", "3").expect.to.equal(":2\r\n");
        ks.run("ZCOUNT", "z", "(2", "+inf").expect.to.equal(":2\r\n");
        ks.run("ZCOUNT", "ghost", "-inf", "+inf").expect.to.equal(":0\r\n");

        ks.run("ZMSCORE", "z", "a", "nope", "d")
            .expect.to.equal("*3\r\n$1\r\n1\r\n$-1\r\n$1\r\n4\r\n");

        ks.run("ZPOPMIN", "z").expect.to.equal("*2\r\n$1\r\na\r\n$1\r\n1\r\n");
        ks.run("ZPOPMAX", "z").expect.to.equal("*2\r\n$1\r\nd\r\n$1\r\n4\r\n");
        ks.run("ZPOPMIN", "z", "5")[0 .. 4].expect.to.equal("*4\r\n");
        ks.run("EXISTS", "z").expect.to.equal(":0\r\n"); // emptied zset vanishes
        ks.run("ZPOPMIN", "ghost").expect.to.equal("*0\r\n");

        ks.run("ZADD", "zr", "1", "a", "2", "b", "3", "c", "4", "d");
        ks.run("ZREMRANGEBYRANK", "zr", "0", "1").expect.to.equal(":2\r\n");
        ks.run("ZRANGE", "zr", "0", "-1").expect.to.equal("*2\r\n$1\r\nc\r\n$1\r\nd\r\n");
        ks.run("ZREMRANGEBYSCORE", "zr", "(3", "+inf").expect.to.equal(":1\r\n");
        ks.run("ZRANGE", "zr", "0", "-1").expect.to.equal("*1\r\n$1\r\nc\r\n");
        ks.run("ZREMRANGEBYSCORE", "zr", "-inf", "+inf").expect.to.equal(":1\r\n");
        ks.run("EXISTS", "zr").expect.to.equal(":0\r\n");
    }
}
