module tests.valkey_dump_tests;

// Valkey unit/dump.tcl core ported to native in-process UT (see valkey_incr_tests.d
// for rationale + THIRD_PARTY_NOTICES credit). DUMP produces an opaque binary blob;
// rather than assert exact bytes, these round-trip DUMP->DEL->RESTORE for every type
// and check the value survives. Cross-version / corrupt-payload / IDLETIME-FREQ cases
// stay in the blackbox sweep.

version (unittest)
{
    import fluent.asserts;
    import std.conv : to;
    import std.string : startsWith;

    import dreads.commands;
    import dreads.mem : Arena, ByteBuffer;
    import dreads.obj : Keyspace;
    import dreads.resp;

    private string respCmd(scope const(char)[][] args...)
    {
        string r = "*" ~ args.length.to!string ~ "\r\n";
        foreach (a; args)
            r ~= "$" ~ a.length.to!string ~ "\r\n" ~ cast(string) a ~ "\r\n";
        return r;
    }

    private string run(ref Keyspace ks, scope const(char)[][] cmdArgs...)
    {
        Arena arena;
        ByteBuffer o;
        RVal v;
        size_t pos = 0;
        propagationOverride.clear();
        auto encoded = respCmd(cmdArgs);
        parseValue(cast(const(ubyte)[]) encoded, pos, arena, v).expect.to.equal(ParseStatus.ok);
        v.dispatch(ks, o, arena);
        return (cast(string) o.data).idup;
    }

    // extract the payload of a `$<len>\r\n<bytes>\r\n` bulk reply (binary-safe)
    private const(char)[] bulkPayload(string reply)
    {
        assert(reply.length && reply[0] == '$', "not a bulk reply");
        size_t i = 1;
        while (reply[i] != '\r')
            i++;
        immutable len = reply[1 .. i].to!long;
        assert(len >= 0, "nil bulk has no payload");
        i += 2; // skip \r\n
        return reply[i .. i + cast(size_t) len];
    }

    private string bulk(string p)
    {
        return "$" ~ p.length.to!string ~ "\r\n" ~ p ~ "\r\n";
    }

    @("valkey.dump.restore_roundtrip")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        // string
        ks.run("SET", "k", "hello world");
        auto blob = bulkPayload(ks.run("DUMP", "k")).idup;
        ks.run("DEL", "k");
        ks.run("RESTORE", "k", "0", blob).expect.to.equal("+OK\r\n");
        ks.run("GET", "k").expect.to.equal(bulk("hello world"));

        // RESTORE onto an existing key without REPLACE -> BUSYKEY error
        ks.run("RESTORE", "k", "0", blob).startsWith("-BUSYKEY").should.equal(true);
        // with REPLACE it overwrites
        ks.run("RESTORE", "k", "0", blob, "REPLACE").expect.to.equal("+OK\r\n");

        // list
        ks.run("RPUSH", "l", "a", "b", "c");
        auto lblob = bulkPayload(ks.run("DUMP", "l")).idup;
        ks.run("DEL", "l");
        ks.run("RESTORE", "l", "0", lblob).expect.to.equal("+OK\r\n");
        ks.run("LRANGE", "l", "0", "-1").expect.to.equal(
                "*3\r\n" ~ bulk("a") ~ bulk("b") ~ bulk("c"));

        // hash
        ks.run("HSET", "h", "f1", "v1", "f2", "v2");
        auto hblob = bulkPayload(ks.run("DUMP", "h")).idup;
        ks.run("DEL", "h");
        ks.run("RESTORE", "h", "0", hblob).expect.to.equal("+OK\r\n");
        ks.run("HGET", "h", "f1").expect.to.equal(bulk("v1"));
        ks.run("HLEN", "h").expect.to.equal(":2\r\n");

        // zset
        ks.run("ZADD", "z", "1", "a", "2", "b");
        auto zblob = bulkPayload(ks.run("DUMP", "z")).idup;
        ks.run("DEL", "z");
        ks.run("RESTORE", "z", "0", zblob).expect.to.equal("+OK\r\n");
        ks.run("ZRANGE", "z", "0", "-1", "WITHSCORES").expect.to.equal(
                "*4\r\n" ~ bulk("a") ~ bulk("1") ~ bulk("b") ~ bulk("2"));

        // DUMP of a missing key -> nil
        ks.run("DUMP", "missing").expect.to.equal("$-1\r\n");
    }
}
