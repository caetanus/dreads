module tests.robustness_tests;

// Invalid commands, malformed arguments, boundary numbers and out-of-window
// inputs — the server must reply with an error (or a sane empty result) and
// never crash, hang or corrupt state.

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

    private void expectError(ref Keyspace ks, string[] cmdArgs...)
    {
        auto reply = run(ks, cmdArgs);
        reply[0].expect.to.equal('-');
    }

    @("robust.protocol_garbage")
    unittest
    {
        Arena a;
        RVal v;
        // claimed lengths that would overflow or exceed limits must be
        // protocol errors, never silent wraps that stall the connection
        foreach (bad; [
            "$99999999999999999999\r\n", "*99999999999999999999\r\n",
            "$18446744073709551617\r\n", ":999999999999999999999999\r\n",
            "$-2\r\n", "*-2\r\n",
        ])
        {
            size_t pos = 0;
            parseValue(cast(const(ubyte)[]) bad, pos, a, v)
                .expect.to.equal(ParseStatus.protocolError);
        }
        // a line not starting with a RESP marker is the inline protocol, not an
        // error: "!oops" is a (bogus) inline command; binary with no newline waits
        size_t ip = 0;
        parseValue(cast(const(ubyte)[]) "!oops\r\n", ip, a, v).expect.to.equal(ParseStatus.ok);
        ip = 0;
        parseValue(cast(const(ubyte)[]) "\x00\x01\x02", ip, a, v).expect.to.equal(ParseStatus.incomplete);
        // huge-but-valid bulk header: incomplete (waits for data), not error
        size_t pos = 0;
        parseValue(cast(const(ubyte)[]) "$1048576\r\nabc", pos, a, v)
            .expect.to.equal(ParseStatus.incomplete);
    }

    @("robust.numeric_boundaries")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();
        // long.min/max survive INCR/DECR edges with proper errors
        ks.run("SET", "n", "9223372036854775807");
        expectError(ks, "INCR", "n");
        ks.run("SET", "n", "-9223372036854775808");
        expectError(ks, "DECR", "n");
        ks.run("GET", "n").expect.to.contain("-9223372036854775808"); // unchanged
        expectError(ks, "INCRBY", "n", "9223372036854775808"); // > long.max
        expectError(ks, "INCRBY", "n", "not-a-number");
        expectError(ks, "EXPIRE", "n", "99999999999999999999"); // overflow
        // giant-but-parseable expire overflows the ms conversion
        expectError(ks, "EXPIRE", "n", "9223372036854775807");
    }

    @("robust.out_of_window")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();
        ks.run("RPUSH", "l", "a", "b");
        // indices far outside the list
        ks.run("LRANGE", "l", "100", "200").expect.to.equal("*0\r\n");
        ks.run("LRANGE", "l", "-999", "999")[0 .. 4].expect.to.equal("*2\r\n");
        ks.run("LINDEX", "l", "9223372036854775807").expect.to.equal("$-1\r\n");
        ks.run("LINDEX", "l", "-9223372036854775808").expect.to.equal("$-1\r\n");
        expectError(ks, "LSET", "l", "999", "x");
        // string windows
        ks.run("SET", "s", "abc");
        ks.run("GETRANGE", "s", "999", "1000").expect.to.equal("$0\r\n\r\n");
        ks.run("GETRANGE", "s", "-999", "999").expect.to.equal("$3\r\nabc\r\n");
        // SETRANGE beyond the 512MB cap must refuse, not allocate
        expectError(ks, "SETRANGE", "s", "536870912", "x");
        expectError(ks, "SETRANGE", "s", "-1", "x");
        // APPEND cannot push past the cap either (simulated small here: the
        // check is on combined length; can't build 512MB in a test, so just
        // assert the normal path still works)
        ks.run("APPEND", "s", "d").expect.to.equal(":4\r\n");
        // scan cursors far past capacity terminate with cursor 0
        ks.run("SCAN", "999999999")[0 .. 10].expect.to.equal("*2\r\n$1\r\n0\r");
        ks.run("ZADD", "z", "1", "a");
        ks.run("ZSCAN", "z", "999999")[0 .. 10].expect.to.equal("*2\r\n$1\r\n0\r");
        // zset ranks out of window
        ks.run("ZRANGE", "z", "5", "10").expect.to.equal("*0\r\n");
        ks.run("ZREMRANGEBYRANK", "z", "5", "10").expect.to.equal(":0\r\n");
        // stream ranges beyond content
        ks.run("XADD", "st", "1-1", "f", "v");
        ks.run("XRANGE", "st", "999", "+").expect.to.equal("*0\r\n");
        // u64-max is a VALID stream id (ms-seq are two u64s), so this parses and
        // reads nothing past it — a nil array, not a parse error (see parseUlong)
        ks.run("XREAD", "STREAMS", "st", "18446744073709551615").expect.to.equal("*-1\r\n");
    }

    @("robust.bad_arity_and_types_everywhere")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();
        // a sweep of wrong arities: each must error, never assert
        foreach (cmd; [
            ["SET"], ["SET", "k"], ["GET"], ["GET", "a", "b"], ["EXPIRE", "k"],
            ["LPUSH", "l"], ["LRANGE", "l", "0"], ["LSET", "l", "0"],
            ["HSET", "h", "f"], ["HGET", "h"], ["SADD", "s"],
            ["ZADD", "z", "1"], ["ZADD", "z", "notnum", "m"],
            ["ZRANGEBYSCORE", "z", "bad", "1"], ["ZINCRBY", "z", "x", "m"],
            ["XADD", "st"], ["XADD", "st", "1-1", "f"], ["GETEX", "k", "EX"],
            ["SETEX", "k", "x", "v"], ["COPY", "a"], ["GETRANGE", "s", "a", "b"],
            ["SINTERCARD", "0"], ["ZMPOP", "1", "z"], ["LMOVE", "a", "b", "LEFT"],
            ["GEOADD", "g", "1", "2"], ["GEODIST", "g", "a"],
            ["GEOSEARCH", "g", "FROMLONLAT", "x", "y", "BYRADIUS", "1", "km"],
        ])
            expectError(ks, cmd);

        // wrong-type across families with one probe key per type
        ks.run("SET", "str", "v");
        ks.run("RPUSH", "lst", "v");
        foreach (cmd; [
            ["LPUSH", "str", "x"], ["HSET", "str", "f", "v"], ["SADD", "str", "m"],
            ["ZADD", "str", "1", "m"], ["XADD", "str", "1-1", "f", "v"],
            ["GET", "lst"], ["INCR", "lst"], ["GETRANGE", "lst", "0", "1"],
            ["ZUNIONSTORE", "u", "1", "str"], ["GEOPOS", "str", "m"],
        ])
            expectError(ks, cmd);

        // unknown and empty commands
        expectError(ks, "TOTALLYUNKNOWN");
        expectError(ks, "X");
        // command name too long for the dispatch buffer
        expectError(ks, "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA");
    }

    @("robust.state_survives_error_storm")
    unittest
    {
        import std.conv : to;

        Keyspace ks;
        scope (exit)
            ks.d.free();
        ks.run("SET", "anchor", "value");
        // hammer errors, then verify the keyspace is intact and usable
        foreach (i; 0 .. 200)
        {
            run(ks, "INCR", "anchor");
            run(ks, "LPUSH", "anchor", "x");
            run(ks, "EXPIRE", "anchor", "notanumber");
            run(ks, "ZADD", "anchor", "1", "m");
            run(ks, "NOSUCHCMD" ~ (i % 10).to!string);
        }
        ks.run("GET", "anchor").expect.to.equal("$5\r\nvalue\r\n");
        ks.run("TTL", "anchor").expect.to.equal(":-1\r\n");
        ks.run("DBSIZE").expect.to.equal(":1\r\n");
    }
}
