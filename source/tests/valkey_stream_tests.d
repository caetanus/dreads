module tests.valkey_stream_tests;

// Valkey unit/type/stream.tcl core ported to native in-process UT (see
// valkey_incr_tests.d for rationale + THIRD_PARTY_NOTICES credit). Explicit stream
// IDs keep replies deterministic; auto-`*` IDs, blocking XREAD, consumer-group PEL
// and DEBUG cases stay in the blackbox sweep (stream-cgroups has its own coverage).

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

    // one XRANGE entry: *2 [ id, [f, v, ...] ]
    private string entry(string id, string[] fv...)
    {
        string fields = "*" ~ fv.length.to!string ~ "\r\n";
        foreach (x; fv)
            fields ~= bulk(x);
        return "*2\r\n" ~ bulk(id) ~ fields;
    }

    @("valkey.stream.add_len_range")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        // XADD with explicit IDs returns the ID
        ks.run("XADD", "s", "1-1", "field1", "value1").expect.to.equal(bulk("1-1"));
        ks.run("XADD", "s", "2-2", "field2", "value2").expect.to.equal(bulk("2-2"));
        ks.run("XADD", "s", "3-3", "a", "1", "b", "2").expect.to.equal(bulk("3-3"));
        ks.run("XLEN", "s").expect.to.equal(":3\r\n");

        // an ID <= the last one is rejected
        ks.run("XADD", "s", "2-2", "x", "y").startsWith("-ERR").should.equal(true);

        // XRANGE - + returns every entry, id + flat field/value list, in order
        ks.run("XRANGE", "s", "-", "+").expect.to.equal(
                "*3\r\n" ~ entry("1-1", "field1", "value1") ~ entry("2-2", "field2",
                "value2") ~ entry("3-3", "a", "1", "b", "2"));
        // COUNT limits, exclusive range with (
        ks.run("XRANGE", "s", "-", "+", "COUNT", "2").expect.to.equal(
                "*2\r\n" ~ entry("1-1", "field1", "value1") ~ entry("2-2", "field2", "value2"));
        ks.run("XRANGE", "s", "(1-1", "+").expect.to.equal(
                "*2\r\n" ~ entry("2-2", "field2", "value2") ~ entry("3-3", "a", "1", "b", "2"));

        // XREVRANGE reverses
        ks.run("XREVRANGE", "s", "+", "-", "COUNT", "1").expect.to.equal(
                "*1\r\n" ~ entry("3-3", "a", "1", "b", "2"));
    }

    @("valkey.stream.del_trim")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        ks.run("XADD", "s", "1-1", "f", "a");
        ks.run("XADD", "s", "2-1", "f", "b");
        ks.run("XADD", "s", "3-1", "f", "c");

        // XDEL removes by id, returns count deleted
        ks.run("XDEL", "s", "2-1", "9-9").expect.to.equal(":1\r\n");
        ks.run("XLEN", "s").expect.to.equal(":2\r\n");
        ks.run("XRANGE", "s", "-", "+").expect.to.equal(
                "*2\r\n" ~ entry("1-1", "f", "a") ~ entry("3-1", "f", "c"));

        // XTRIM MAXLEN keeps the newest N
        ks.run("XADD", "s", "4-1", "f", "d");
        ks.run("XADD", "s", "5-1", "f", "e");
        ks.run("XTRIM", "s", "MAXLEN", "2").expect.to.equal(":2\r\n"); // 2 removed
        ks.run("XLEN", "s").expect.to.equal(":2\r\n");
        ks.run("XRANGE", "s", "-", "+").expect.to.equal(
                "*2\r\n" ~ entry("4-1", "f", "d") ~ entry("5-1", "f", "e"));
    }
}
