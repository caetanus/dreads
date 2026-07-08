module tests.streamops_tests;

// XREVRANGE/XSETID/XINFO and consumer groups.

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

    @("streamops.xrevrange_xsetid_xinfo")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();
        ks.run("XADD", "s", "1-1", "a", "1");
        ks.run("XADD", "s", "2-1", "b", "2");
        ks.run("XADD", "s", "3-1", "c", "3");

        auto rev = ks.run("XREVRANGE", "s", "+", "-");
        rev[0 .. 4].expect.to.equal("*3\r\n");
        rev.expect.to.startWith("*3\r\n*2\r\n$3\r\n3-1"); // newest first
        ks.run("XREVRANGE", "s", "+", "-", "COUNT", "1")[0 .. 4].expect.to.equal("*1\r\n");
        ks.run("XREVRANGE", "s", "2", "1")[0 .. 4].expect.to.equal("*2\r\n");
        ks.run("XREVRANGE", "ghost", "+", "-").expect.to.equal("*0\r\n");

        ks.run("XSETID", "s", "50-0").expect.to.equal("+OK\r\n");
        auto id = ks.run("XADD", "s", "*", "d", "4");
        id.expect.to.startWith("$"); // auto id now >= 50-0
        ks.run("XSETID", "s", "1-0")[0].expect.to.equal('-'); // below top entry
        ks.run("XSETID", "ghost", "5-5")[0].expect.to.equal('-');

        auto info = ks.run("XINFO", "STREAM", "s");
        info.expect.to.contain("length");
        info.expect.to.contain("last-generated-id");
        ks.run("XINFO", "STREAM", "ghost")[0].expect.to.equal('-');
    }

    @("streamops.consumer_groups")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();
        ks.run("XADD", "s", "1-1", "n", "one");
        ks.run("XADD", "s", "2-1", "n", "two");
        ks.run("XADD", "s", "3-1", "n", "three");

        ks.run("XGROUP", "CREATE", "s", "g", "0").expect.to.equal("+OK\r\n");
        ks.run("XGROUP", "CREATE", "s", "g", "0")
            .expect.to.equal("-BUSYGROUP Consumer Group name already exists\r\n");
        ks.run("XGROUP", "CREATE", "nostream", "g", "0")[0].expect.to.equal('-');
        ks.run("XGROUP", "CREATE", "mk", "g", "$", "MKSTREAM").expect.to.equal("+OK\r\n");
        ks.run("TYPE", "mk").expect.to.equal("+stream\r\n");

        // consumer alice reads two new entries
        auto r1 = ks.run("XREADGROUP", "GROUP", "g", "alice", "COUNT", "2", "STREAMS", "s", ">");
        r1.expect.to.contain("1-1");
        r1.expect.to.contain("2-1");
        r1.expect.to.not.contain("3-1");
        // bob gets the remaining one
        auto r2 = ks.run("XREADGROUP", "GROUP", "g", "bob", "STREAMS", "s", ">");
        r2.expect.to.contain("3-1");
        // no more new entries
        ks.run("XREADGROUP", "GROUP", "g", "alice", "STREAMS", "s", ">")
            .expect.to.equal("*-1\r\n");
        // pending summary: 3 entries, alice 2, bob 1
        auto p = ks.run("XPENDING", "s", "g");
        p.expect.to.contain(":3\r\n");
        p.expect.to.contain("alice");
        p.expect.to.contain("bob");
        // re-delivery of alice's own history (id 0 = everything)
        auto hist = ks.run("XREADGROUP", "GROUP", "g", "alice", "STREAMS", "s", "0");
        hist.expect.to.contain("1-1");
        hist.expect.to.not.contain("3-1"); // bob's
        // ack one
        ks.run("XACK", "s", "g", "1-1").expect.to.equal(":1\r\n");
        ks.run("XACK", "s", "g", "1-1").expect.to.equal(":0\r\n");
        // extended pending: only alice, range
        auto px = ks.run("XPENDING", "s", "g", "-", "+", "10", "alice");
        px.expect.to.contain("2-1");
        px.expect.to.not.contain("3-1");
        // claim bob's entry for alice (idle 0 threshold)
        auto cl = ks.run("XCLAIM", "s", "g", "alice", "0", "3-1");
        cl.expect.to.contain("3-1");
        auto px2 = ks.run("XPENDING", "s", "g", "-", "+", "10", "alice");
        px2.expect.to.contain("3-1");
        // JUSTID form
        ks.run("XCLAIM", "s", "g", "bob", "0", "3-1", "JUSTID")
            .expect.to.equal("*1\r\n$3\r\n3-1\r\n");
        // delconsumer drops pending
        auto del = ks.run("XGROUP", "DELCONSUMER", "s", "g", "bob");
        del.expect.to.equal(":1\r\n"); // 3-1 was bob's again
        // group introspection
        auto gi = ks.run("XINFO", "GROUPS", "s");
        gi.expect.to.contain("last-delivered-id");
        // destroy
        ks.run("XGROUP", "DESTROY", "s", "g").expect.to.equal(":1\r\n");
        ks.run("XREADGROUP", "GROUP", "g", "alice", "STREAMS", "s", ">")[0]
            .expect.to.equal('-'); // NOGROUP
        // robustness
        ks.run("XREADGROUP", "GROUP", "g", "c", "STREAMS", "s")[0].expect.to.equal('-');
        ks.run("XACK", "s", "nogroup", "1-1").expect.to.equal(":0\r\n");
        ks.run("XPENDING", "s", "nogroup")[0].expect.to.equal('-');
    }

    @("streamops.noack_and_setid_groups")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();
        ks.run("XADD", "s", "1-1", "n", "one");
        ks.run("XGROUP", "CREATE", "s", "g", "0");
        ks.run("XREADGROUP", "GROUP", "g", "c", "NOACK", "STREAMS", "s", ">")
            .expect.to.contain("1-1");
        ks.run("XPENDING", "s", "g").expect.to.contain(":0\r\n"); // NOACK skips PEL
    }
}
