module tests.valkey_incr_tests;

// Valkey unit/type/incr.tcl ported to native in-process UT (the "contrabando":
// bring Valkey's test COVERAGE into `dub test`, re-expressed against dreads'
// dispatch — not a copy of the tcl, and no server needed). Valkey is BSD-3; the
// scenarios are credited in THIRD_PARTY_NOTICES. Server-only cases (DEBUG OBJECT
// refcount, valgrind guards) are noted where dropped.

version (unittest)
{
    import fluent.asserts;
    import std.string : startsWith;

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

    // bulk-string reply for `payload`, e.g. bulk("1.25") == "$4\r\n1.25\r\n"
    private string bulk(string payload)
    {
        import std.conv : to;

        return "$" ~ payload.length.to!string ~ "\r\n" ~ payload ~ "\r\n";
    }

    @("valkey.incr.integer_paths")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        // INCR against non existing key -> 1, then GET -> "1"
        ks.run("INCR", "novar").expect.to.equal(":1\r\n");
        ks.run("GET", "novar").expect.to.equal(bulk("1"));
        // INCR against key created by incr itself -> 2
        ks.run("INCR", "novar").expect.to.equal(":2\r\n");
        // DECR -> 1
        ks.run("DECR", "novar").expect.to.equal(":1\r\n");

        // DECR against non-existing key -> -1; then INCR the same key -> 0
        ks.run("DEL", "nx");
        ks.run("DECR", "nx").expect.to.equal(":-1\r\n");
        ks.run("INCR", "nx").expect.to.equal(":0\r\n");

        // INCR against key originally SET
        ks.run("SET", "novar", "100");
        ks.run("INCR", "novar").expect.to.equal(":101\r\n");

        // INCR / INCRBY over a 32-bit value
        ks.run("SET", "novar", "17179869184");
        ks.run("INCR", "novar").expect.to.equal(":17179869185\r\n");
        ks.run("SET", "novar", "17179869184");
        ks.run("INCRBY", "novar", "17179869184").expect.to.equal(":34359738368\r\n");

        // DECRBY over 32-bit, negative result -> -1
        ks.run("SET", "novar", "17179869184");
        ks.run("DECRBY", "novar", "17179869185").expect.to.equal(":-1\r\n");

        // DECRBY against non-existing key -> -1
        ks.run("DEL", "kne");
        ks.run("DECRBY", "kne", "1").expect.to.equal(":-1\r\n");

        // INCR modifies the value in place (correctness, sans DEBUG OBJECT refcount)
        ks.run("SET", "foo", "20000");
        ks.run("INCR", "foo").expect.to.equal(":20001\r\n");
        ks.run("INCR", "foo").expect.to.equal(":20002\r\n");
    }

    @("valkey.incr.error_paths")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        // INCR fails against a key with spaces (left / right / both) -> not an integer
        foreach (v; ["    11", "11    ", "    11    "])
        {
            ks.run("SET", "novar", v);
            ks.run("INCR", "novar").startsWith("-ERR").should.equal(true);
        }

        // DECRBY negation overflow (-INT64_MIN cannot be represented)
        ks.run("SET", "x", "0");
        ks.run("DECRBY", "x", "-9223372036854775808").startsWith("-ERR").should.equal(true);

        // INCR against a key holding a list -> WRONGTYPE
        ks.run("RPUSH", "mylist", "1");
        ks.run("INCR", "mylist").startsWith("-WRONGTYPE").should.equal(true);

        // arity: INCR with an extra arg
        ks.run("INCR", "mk", "v").startsWith("-ERR").should.equal(true);
    }

    @("valkey.incr.incrbyfloat")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        // against a non-existing key: 1, then +0.25 -> 1.25 (and GET agrees)
        ks.run("DEL", "novar");
        ks.run("INCRBYFLOAT", "novar", "1").expect.to.equal(bulk("1"));
        ks.run("GET", "novar").expect.to.equal(bulk("1"));
        ks.run("INCRBYFLOAT", "novar", "0.25").expect.to.equal(bulk("1.25"));
        ks.run("GET", "novar").expect.to.equal(bulk("1.25"));

        // originally SET; over 32-bit; over-32-bit increment
        ks.run("SET", "novar", "1.5");
        ks.run("INCRBYFLOAT", "novar", "1.5").expect.to.equal(bulk("3"));
        ks.run("SET", "novar", "17179869184");
        ks.run("INCRBYFLOAT", "novar", "1.5").expect.to.equal(bulk("17179869185.5"));
        ks.run("SET", "novar", "17179869184");
        ks.run("INCRBYFLOAT", "novar", "17179869184").expect.to.equal(bulk("34359738368"));

        // decrement -> -0.1
        ks.run("SET", "foo", "1");
        ks.run("INCRBYFLOAT", "foo", "-1.1").expect.to.equal(bulk("-0.1"));

        // no negative zero: +1/41 then -1/41 -> 0
        ks.run("DEL", "foo");
        ks.run("INCRBYFLOAT", "foo", "0.024390243902439");
        ks.run("INCRBYFLOAT", "foo", "-0.024390243902439");
        ks.run("GET", "foo").expect.to.equal(bulk("0"));
    }

    @("valkey.incr.incrbyfloat_errors")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        // spaces (left / right / both) -> not a valid float
        foreach (v; ["    11", "11    ", " 11 "])
        {
            ks.run("SET", "novar", v);
            ks.run("INCRBYFLOAT", "novar", "1.0").startsWith("-ERR").should.equal(true);
        }

        // against a list -> WRONGTYPE
        ks.run("DEL", "mylist");
        ks.run("RPUSH", "mylist", "1");
        ks.run("INCRBYFLOAT", "mylist", "1.0").startsWith("-WRONGTYPE").should.equal(true);

        // Inf/NaN rejected ("would produce NaN or Infinity")
        ks.run("SET", "foo", "0");
        ks.run("INCRBYFLOAT", "foo", "+inf").startsWith("-ERR").should.equal(true);
    }
}
