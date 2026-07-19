module tests.valkey_stream_cgroups_tests;

// Valkey unit/type/stream-cgroups.tcl core ported to native in-process UT (see
// valkey_incr_tests.d for rationale + THIRD_PARTY_NOTICES credit). Consumer groups
// live in the stream RObj (not connection state), so XGROUP/XREADGROUP/XACK/XPENDING
// are unit-testable with explicit IDs + explicit consumer names. Blocking XREADGROUP
// and cross-connection cases stay in the blackbox sweep.

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

    private string entry(string id, string[] fv...)
    {
        string fields = "*" ~ fv.length.to!string ~ "\r\n";
        foreach (x; fv)
            fields ~= bulk(x);
        return "*2\r\n" ~ bulk(id) ~ fields;
    }

    @("valkey.streamcg.group_read_ack_pending")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        ks.run("XADD", "s", "1-1", "f", "a").expect.to.equal(bulk("1-1"));
        ks.run("XADD", "s", "2-1", "f", "b").expect.to.equal(bulk("2-1"));

        // create a group reading from the start
        ks.run("XGROUP", "CREATE", "s", "g", "0").expect.to.equal("+OK\r\n");
        // duplicate group -> BUSYGROUP
        ks.run("XGROUP", "CREATE", "s", "g", "0").startsWith("-BUSYGROUP").should.equal(true);

        // XREADGROUP delivers the new entries and puts them in the PEL:
        // *1 [ *2 [ "s", [entry(1-1), entry(2-1)] ] ]
        ks.run("XREADGROUP", "GROUP", "g", "c1", "COUNT", "10", "STREAMS", "s", ">")
            .expect.to.equal("*1\r\n*2\r\n" ~ bulk("s") ~ "*2\r\n"
                ~ entry("1-1", "f", "a") ~ entry("2-1", "f", "b"));

        // ack one; the other stays pending
        ks.run("XACK", "s", "g", "1-1").expect.to.equal(":1\r\n");
        ks.run("XACK", "s", "g", "1-1").expect.to.equal(":0\r\n"); // already acked

        // XPENDING summary: [count, min-id, max-id, [[consumer, count]]]
        ks.run("XPENDING", "s", "g").expect.to.equal(
                "*4\r\n:1\r\n" ~ bulk("2-1") ~ bulk("2-1") ~ "*1\r\n*2\r\n" ~ bulk("c1") ~ bulk("1"));

        // a fresh ">" read returns nothing new (all delivered)
        ks.run("XREADGROUP", "GROUP", "g", "c1", "STREAMS", "s", ">").expect.to.equal("*-1\r\n");
        // reading "0" replays this consumer's still-pending entries
        ks.run("XREADGROUP", "GROUP", "g", "c1", "STREAMS", "s", "0")
            .expect.to.equal("*1\r\n*2\r\n" ~ bulk("s") ~ "*1\r\n" ~ entry("2-1", "f", "b"));

        // XGROUP CREATECONSUMER / DELCONSUMER
        ks.run("XGROUP", "CREATECONSUMER", "s", "g", "c2").expect.to.equal(":1\r\n");
        ks.run("XGROUP", "CREATECONSUMER", "s", "g", "c2").expect.to.equal(":0\r\n"); // exists
        ks.run("XGROUP", "DELCONSUMER", "s", "g", "c1").expect.to.equal(":1\r\n"); // 1 pending removed
    }
}
