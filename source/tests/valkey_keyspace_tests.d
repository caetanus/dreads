module tests.valkey_keyspace_tests;

// Valkey unit/keyspace.tcl ported to native in-process UT (see valkey_incr_tests.d
// for rationale + THIRD_PARTY_NOTICES credit). KEYS (unordered array), cross-DB COPY
// (needs SELECT/multi-db over a connection) and DEBUG cases stay in the blackbox
// sweep. RENAME/RENAMENX basics already live in commands_ext_tests.d; here are the
// edge cases (same src/dest, volatile-TTL) + DEL/EXISTS/DBSIZE/COPY/TYPE.

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

    @("valkey.keyspace.del_exists_dbsize")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        // DEL a single item
        ks.run("SET", "x", "foo");
        ks.run("DEL", "x").expect.to.equal(":1\r\n");
        ks.run("GET", "x").expect.to.equal("$-1\r\n");

        // Vararg DEL: 3 of 4 exist -> :3
        ks.run("MSET", "f1", "a", "f2", "b", "f3", "c");
        ks.run("DEL", "f1", "f2", "f3", "f4").expect.to.equal(":3\r\n");

        // EXISTS: present/absent, and multi-key counts duplicates
        ks.run("SET", "nk", "test");
        ks.run("EXISTS", "nk").expect.to.equal(":1\r\n");
        ks.run("DEL", "nk");
        ks.run("EXISTS", "nk").expect.to.equal(":0\r\n");
        ks.run("SET", "a", "1");
        ks.run("EXISTS", "a", "a", "b").expect.to.equal(":2\r\n"); // a counted twice

        // DBSIZE reflects live keys
        ks.run("FLUSHDB");
        ks.run("MSET", "k1", "1", "k2", "2", "k3", "3");
        ks.run("DBSIZE").expect.to.equal(":3\r\n");
    }

    @("valkey.keyspace.rename_edges")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        // RENAME / RENAMENX where source and dest are the same key (existing)
        ks.run("SET", "mk", "foo");
        ks.run("RENAME", "mk", "mk").expect.to.equal("+OK\r\n");
        ks.run("RENAMENX", "mk", "mk").expect.to.equal(":0\r\n");
        // same key, non-existing source -> no such key
        ks.run("DEL", "gone");
        ks.run("RENAME", "gone", "gone").startsWith("-ERR").should.equal(true);

        // RENAME moves the TTL with the key
        ks.run("DEL", "s", "d");
        ks.run("SET", "s", "foo");
        ks.run("EXPIRE", "s", "100");
        ks.run("TTL", "s").expect.to.equal(":100\r\n");
        ks.run("RENAME", "s", "d").expect.to.equal("+OK\r\n");
        ks.run("TTL", "d").expect.to.equal(":100\r\n");

        // a volatile source overwriting a volatile target does NOT inherit target's
        // TTL — the source had none here, so the result is persistent (-1)
        ks.run("DEL", "s", "d");
        ks.run("SET", "s", "foo");
        ks.run("SET", "d", "bar");
        ks.run("EXPIRE", "d", "100");
        ks.run("TTL", "s").expect.to.equal(":-1\r\n");
        ks.run("RENAME", "s", "d");
        ks.run("TTL", "d").expect.to.equal(":-1\r\n"); // source's (no) TTL wins
    }

    @("valkey.keyspace.copy_same_db")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        // COPY basic (same DB): copies the value, returns :1
        ks.run("SET", "src", "foobar");
        ks.run("COPY", "src", "dst").expect.to.equal(":1\r\n");
        ks.run("GET", "dst").expect.to.equal(bulk("foobar"));

        // without REPLACE, COPY onto an existing key fails -> :0
        ks.run("COPY", "src", "dst").expect.to.equal(":0\r\n");
        // with REPLACE it overwrites -> :1
        ks.run("SET", "src", "changed");
        ks.run("COPY", "src", "dst", "REPLACE").expect.to.equal(":1\r\n");
        ks.run("GET", "dst").expect.to.equal(bulk("changed"));

        // copied data is independent: mutating the copy doesn't touch the source
        ks.run("SET", "dst", "hoge");
        ks.run("GET", "src").expect.to.equal(bulk("changed"));
    }

    @("valkey.keyspace.type")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        ks.run("SET", "s", "v");
        ks.run("TYPE", "s").expect.to.equal("+string\r\n");
        ks.run("RPUSH", "l", "a");
        ks.run("TYPE", "l").expect.to.equal("+list\r\n");
        ks.run("SADD", "st", "m");
        ks.run("TYPE", "st").expect.to.equal("+set\r\n");
        ks.run("HSET", "h", "f", "v");
        ks.run("TYPE", "h").expect.to.equal("+hash\r\n");
        ks.run("ZADD", "z", "1", "m");
        ks.run("TYPE", "z").expect.to.equal("+zset\r\n");
        ks.run("TYPE", "missing").expect.to.equal("+none\r\n");
    }
}
