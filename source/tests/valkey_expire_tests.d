module tests.valkey_expire_tests;

// Valkey unit/expire.tcl ported to native in-process UT (see valkey_incr_tests.d for
// rationale + THIRD_PARTY_NOTICES credit). Cases that need REAL wall-time to elapse
// ("after 2.1s the key is gone", active-expire cycle) are server-only -> blackbox
// sweep. The in-process clock is frozen per command, so relative TTLs are exact.

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

    @("valkey.expire.ttl_persist")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        // EXPIRE sets a TTL; TTL/PTTL read it (frozen clock -> exact)
        ks.run("SET", "x", "foo");
        ks.run("EXPIRE", "x", "100").expect.to.equal(":1\r\n");
        ks.run("TTL", "x").expect.to.equal(":100\r\n");
        ks.run("PTTL", "x").expect.to.equal(":100000\r\n");
        ks.run("GET", "x").expect.to.equal(bulk("foo")); // still readable

        // PERSIST undoes the EXPIRE
        ks.run("PERSIST", "x").expect.to.equal(":1\r\n");
        ks.run("TTL", "x").expect.to.equal(":-1\r\n");
        ks.run("PERSIST", "x").expect.to.equal(":0\r\n"); // no TTL to remove

        // -1 = no expire, -2 = key does not exist
        ks.run("TTL", "x").expect.to.equal(":-1\r\n");
        ks.run("PTTL", "x").expect.to.equal(":-1\r\n");
        ks.run("TTL", "missing").expect.to.equal(":-2\r\n");
        ks.run("PTTL", "missing").expect.to.equal(":-2\r\n");
        ks.run("PERSIST", "missing").expect.to.equal(":0\r\n");
        ks.run("EXPIRE", "missing", "100").expect.to.equal(":0\r\n");
    }

    @("valkey.expire.setex_and_errors")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        // SETEX: value + TTL together
        ks.run("SETEX", "k", "100", "val").expect.to.equal("+OK\r\n");
        ks.run("TTL", "k").expect.to.equal(":100\r\n");
        ks.run("GET", "k").expect.to.equal(bulk("val"));
        // PSETEX (milliseconds)
        ks.run("PSETEX", "k2", "100000", "v").expect.to.equal("+OK\r\n");
        ks.run("TTL", "k2").expect.to.equal(":100\r\n");

        // SETEX with a non-positive / non-integer time -> error
        ks.run("SETEX", "k", "0", "v").startsWith("-ERR").should.equal(true);
        ks.run("SETEX", "k", "-1", "v").startsWith("-ERR").should.equal(true);
        ks.run("SETEX", "k", "abc", "v").startsWith("-ERR").should.equal(true);
        // EXPIRE with empty-string TTL -> error
        ks.run("SET", "e", "v");
        ks.run("EXPIRE", "e", "").startsWith("-ERR").should.equal(true);
    }

    @("valkey.expire.past_deletes")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        // an already-past EXPIRE deletes the key immediately (returns 1)
        ks.run("SET", "x", "foo");
        ks.run("EXPIRE", "x", "-1").expect.to.equal(":1\r\n");
        ks.run("EXISTS", "x").expect.to.equal(":0\r\n");

        // EXPIREAT with a past absolute time deletes too
        ks.run("SET", "y", "bar");
        ks.run("EXPIREAT", "y", "1").expect.to.equal(":1\r\n"); // epoch 1s = long past
        ks.run("EXISTS", "y").expect.to.equal(":0\r\n");
    }

    @("valkey.expire.options")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        // EXPIRE NX/XX/GT/LT (Redis/Valkey 7.0)
        ks.run("SET", "x", "foo");
        ks.run("EXPIRE", "x", "100", "NX").expect.to.equal(":1\r\n"); // no TTL yet
        ks.run("EXPIRE", "x", "200", "NX").expect.to.equal(":0\r\n"); // already has one
        ks.run("EXPIRE", "x", "200", "XX").expect.to.equal(":1\r\n"); // has one -> ok
        ks.run("TTL", "x").expect.to.equal(":200\r\n");

        // GT: only if the new TTL is greater than the current
        ks.run("EXPIRE", "x", "100", "GT").expect.to.equal(":0\r\n"); // 100 < 200
        ks.run("EXPIRE", "x", "300", "GT").expect.to.equal(":1\r\n"); // 300 > 200
        ks.run("TTL", "x").expect.to.equal(":300\r\n");
        // LT: only if less
        ks.run("EXPIRE", "x", "400", "LT").expect.to.equal(":0\r\n"); // 400 > 300
        ks.run("EXPIRE", "x", "100", "LT").expect.to.equal(":1\r\n"); // 100 < 300
        ks.run("TTL", "x").expect.to.equal(":100\r\n");
    }
}
