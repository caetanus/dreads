module tests.valkey_keyspace_tests;

// Valkey unit/keyspace.tcl ported to native in-process UT (see valkey_incr_tests.d
// for rationale + THIRD_PARTY_NOTICES credit). The Valkey tcl is a read-only spec +
// reply oracle; nothing tcl lands in the repo. This is a WIDE daily-sweep port:
// every deterministic, in-process-runnable case — DEL/EXISTS/DBSIZE, KEYS (glob +
// hashtag + long/nested patterns), RENAME/RENAMENX edges (same key, volatile TTL),
// COPY (same-db value/REPLACE/independence + DB-arg errors), TYPE, RANDOMKEY,
// MOVE/SWAPDB error surfaces. Cross-DB data verification (needs SELECT over a
// connection) and DEBUG/notification cases stay in the blackbox sweep.

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

    // ---------------------------------------------------------------- DEL / EXISTS
    @("valkey.keyspace.del")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        // DEL against a single item
        ks.run("SET", "x", "foo").expect.to.equal("+OK\r\n");
        ks.run("GET", "x").expect.to.equal(bulk("foo"));
        ks.run("DEL", "x").expect.to.equal(":1\r\n");
        ks.run("GET", "x").expect.to.equal(NIL);

        // Vararg DEL: 3 of 4 exist -> :3, and MGET now sees them all nil
        ks.run("SET", "foo1", "a");
        ks.run("SET", "foo2", "b");
        ks.run("SET", "foo3", "c");
        ks.run("DEL", "foo1", "foo2", "foo3", "foo4").expect.to.equal(":3\r\n");
        ks.run("MGET", "foo1", "foo2", "foo3").expect.to.equal("*3\r\n" ~ NIL ~ NIL ~ NIL);

        // DEL of a purely missing key -> :0
        ks.run("DEL", "nope").expect.to.equal(":0\r\n");
    }

    @("valkey.keyspace.untagged_multikey")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        ks.run("MSET", "foo1", "a", "foo2", "b", "foo3", "c").expect.to.equal("+OK\r\n");
        ks.run("MGET", "foo1", "foo2", "foo3", "foo4")
            .expect.to.equal("*4\r\n" ~ bulk("a") ~ bulk("b") ~ bulk("c") ~ NIL);
        ks.run("DEL", "foo1", "foo2", "foo3", "foo4").expect.to.equal(":3\r\n");
    }

    @("valkey.keyspace.exists")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        // present -> 1, deleted -> 0
        ks.run("SET", "newkey", "test");
        ks.run("EXISTS", "newkey").expect.to.equal(":1\r\n");
        ks.run("DEL", "newkey");
        ks.run("EXISTS", "newkey").expect.to.equal(":0\r\n");

        // multi-key EXISTS counts duplicates
        ks.run("SET", "a", "1");
        ks.run("EXISTS", "a", "a", "b").expect.to.equal(":2\r\n");

        // Zero-length value: SET/GET/EXISTS all behave normally
        ks.run("SET", "emptykey", "");
        ks.run("GET", "emptykey").expect.to.equal(bulk(""));
        ks.run("EXISTS", "emptykey").expect.to.equal(":1\r\n");
        ks.run("DEL", "emptykey");
        ks.run("EXISTS", "emptykey").expect.to.equal(":0\r\n");
    }

    // ---------------------------------------------------------------- KEYS / DBSIZE
    @("valkey.keyspace.keys_glob")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        foreach (k; ["key_x", "key_y", "key_z", "foo_a", "foo_b", "foo_c"])
            ks.run("SET", k, "hello");

        // KEYS with a prefix pattern
        ks.run("KEYS", "foo*").sameSet("foo_a", "foo_b", "foo_c");
        // KEYS to get all keys
        ks.run("KEYS", "*").sameSet("key_x", "key_y", "key_z", "foo_a", "foo_b", "foo_c");
        // DBSIZE reflects the live count
        ks.run("DBSIZE").expect.to.equal(":6\r\n");
        // no-match pattern -> empty array
        ks.run("KEYS", "zzz*").expect.to.equal("*0\r\n");
    }

    @("valkey.keyspace.keys_hashtag")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        // braces are literal glob characters
        foreach (k; ["{a}x", "{a}y", "{a}z", "{b}a", "{b}b", "{b}c"])
            ks.run("SET", k, "hello");
        ks.run("KEYS", "{a}*").sameSet("{a}x", "{a}y", "{a}z");
        ks.run("KEYS", "*{b}*").sameSet("{b}a", "{b}b", "{b}c");
    }

    @("valkey.keyspace.keys_empty_db")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        // KEYS with an empty DB -> empty array
        ks.run("KEYS", "*").expect.to.equal("*0\r\n");

        // DEL all keys leaves DBSIZE 0
        ks.run("SET", "a", "1");
        ks.run("SET", "b", "2");
        ks.run("DEL", "a", "b");
        ks.run("DBSIZE").expect.to.equal(":0\r\n");
    }

    @("valkey.keyspace.keys_long_and_nested")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        // KEYS * twice with a long key (github issue #1208) — deterministic single key
        immutable longk = "dlskeriewrioeuwqoirueioqwrueoqwrueqw";
        ks.run("SET", longk, "test");
        ks.run("KEYS", "*").expect.to.equal(arrB(longk));
        ks.run("KEYS", "*").expect.to.equal(arrB(longk));

        // Regression: pattern-matching long nested loops must terminate with no match
        ks.run("FLUSHDB");
        ks.run("SET", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "1");
        ks.run("KEYS", "a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*b")
            .expect.to.equal("*0\r\n");
    }

    // ---------------------------------------------------------------- RENAME
    @("valkey.keyspace.rename_basic")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        // chained RENAME carries the value; source no longer exists after
        ks.run("SET", "mykey", "hello");
        ks.run("RENAME", "mykey", "mykey1").expect.to.equal("+OK\r\n");
        ks.run("RENAME", "mykey1", "mykey2").expect.to.equal("+OK\r\n");
        ks.run("GET", "mykey2").expect.to.equal(bulk("hello"));
        ks.run("EXISTS", "mykey").expect.to.equal(":0\r\n");
        ks.run("EXISTS", "mykey1").expect.to.equal(":0\r\n");

        // RENAME against an already-existing destination overwrites it
        ks.run("SET", "mk", "a");
        ks.run("SET", "mk2", "b");
        ks.run("RENAME", "mk2", "mk").expect.to.equal("+OK\r\n");
        ks.run("GET", "mk").expect.to.equal(bulk("b"));
        ks.run("EXISTS", "mk2").expect.to.equal(":0\r\n");

        // RENAME against a non-existing source key -> error
        ks.run("RENAME", "nokey", "foobar").startsWith("-ERR").should.equal(true);
    }

    @("valkey.keyspace.renamenx")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        // RENAMENX basic: succeeds when dest is free
        ks.run("SET", "mk", "foobar");
        ks.run("RENAMENX", "mk", "mk2").expect.to.equal(":1\r\n");
        ks.run("GET", "mk2").expect.to.equal(bulk("foobar"));
        ks.run("EXISTS", "mk").expect.to.equal(":0\r\n");

        // RENAMENX against an already-existing destination -> :0, both untouched
        ks.run("SET", "a", "foo");
        ks.run("SET", "b", "bar");
        ks.run("RENAMENX", "a", "b").expect.to.equal(":0\r\n");
        ks.run("GET", "a").expect.to.equal(bulk("foo"));
        ks.run("GET", "b").expect.to.equal(bulk("bar"));

        // RENAMENX against a non-existing source key -> error
        ks.run("RENAMENX", "nokey", "dst").startsWith("-ERR").should.equal(true);
    }

    @("valkey.keyspace.rename_same_key")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        // src == dst, existing: RENAME -> OK, RENAMENX -> :0
        ks.run("SET", "mykey", "foo");
        ks.run("RENAME", "mykey", "mykey").expect.to.equal("+OK\r\n");
        ks.run("RENAMENX", "mykey", "mykey").expect.to.equal(":0\r\n");

        // src == dst, non-existing: RENAME -> error
        ks.run("DEL", "gone");
        ks.run("RENAME", "gone", "gone").startsWith("-ERR").should.equal(true);
    }

    @("valkey.keyspace.rename_ttl")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        // RENAME with a volatile key moves the TTL as well
        ks.run("SET", "s", "foo");
        ks.run("EXPIRE", "s", "100").expect.to.equal(":1\r\n");
        ks.run("TTL", "s").expect.to.equal(":100\r\n");
        ks.run("RENAME", "s", "d").expect.to.equal("+OK\r\n");
        ks.run("TTL", "d").expect.to.equal(":100\r\n");

        // RENAME does not inherit the TTL of the target key: a persistent source
        // overwriting a volatile target leaves the dest persistent (-1)
        ks.run("DEL", "s", "d");
        ks.run("SET", "s", "foo");
        ks.run("SET", "d", "bar");
        ks.run("EXPIRE", "d", "100");
        ks.run("TTL", "s").expect.to.equal(":-1\r\n");
        ks.run("RENAME", "s", "d").expect.to.equal("+OK\r\n");
        ks.run("TTL", "d").expect.to.equal(":-1\r\n"); // source's (no) TTL wins
    }

    // ---------------------------------------------------------------- COPY
    @("valkey.keyspace.copy_basic")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        // COPY basic (same DB): copies the value, returns :1, DBSIZE grows
        ks.run("SET", "src", "foobar");
        ks.run("COPY", "src", "dst").expect.to.equal(":1\r\n");
        ks.run("GET", "dst").expect.to.equal(bulk("foobar"));
        ks.run("DBSIZE").expect.to.equal(":2\r\n");

        // COPY onto an existing key without REPLACE fails -> :0
        ks.run("COPY", "src", "dst").expect.to.equal(":0\r\n");

        // COPY of a non-existing source -> :0
        ks.run("COPY", "noexist", "noexist2").expect.to.equal(":0\r\n");
    }

    @("valkey.keyspace.copy_replace_and_independence")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        ks.run("SET", "src", "foobar");
        ks.run("COPY", "src", "dst").expect.to.equal(":1\r\n");

        // REPLACE overwrites -> :1
        ks.run("SET", "src", "changed");
        ks.run("COPY", "src", "dst", "REPLACE").expect.to.equal(":1\r\n");
        ks.run("GET", "dst").expect.to.equal(bulk("changed"));

        // copied data is independent: mutating the copy doesn't touch the source
        ks.run("SET", "dst", "hoge");
        ks.run("GET", "src").expect.to.equal(bulk("changed"));
    }

    @("valkey.keyspace.copy_expire_metadata")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        // COPY copies the key's expire metadata as well
        ks.run("SET", "src", "foobar", "EX", "100");
        ks.run("COPY", "src", "dst", "REPLACE").expect.to.equal(":1\r\n");
        ks.run("TTL", "dst").expect.to.equal(":100\r\n");
        ks.run("GET", "dst").expect.to.equal(bulk("foobar"));

        // COPY does not create an expire if the source has none
        ks.run("SET", "src2", "foobar");
        ks.run("TTL", "src2").expect.to.equal(":-1\r\n");
        ks.run("COPY", "src2", "dst2", "REPLACE").expect.to.equal(":1\r\n");
        ks.run("TTL", "dst2").expect.to.equal(":-1\r\n");
        ks.run("GET", "dst2").expect.to.equal(bulk("foobar"));
    }

    @("valkey.keyspace.copy_errors")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        ks.run("SET", "mk", "foobar");

        // same source and destination -> error
        ks.run("COPY", "mk", "mk").startsWith("-ERR source and destination")
            .should.equal(true);

        // non-integer DB argument -> not-an-integer error
        ks.run("COPY", "mk", "mnk", "DB", "notanumber")
            .startsWith("-ERR value is not an integer or out of range")
            .should.equal(true);

        // out-of-range DB argument -> DB index out of range
        ks.run("COPY", "mk", "mnk", "DB", "999")
            .startsWith("-ERR DB index is out of range").should.equal(true);
        ks.run("COPY", "mk", "mnk", "DB", "-1")
            .startsWith("-ERR DB index is out of range").should.equal(true);

        // unknown trailing token -> syntax error
        ks.run("COPY", "mk", "mnk", "BOGUS").startsWith("-ERR syntax error")
            .should.equal(true);
    }

    // ---------------------------------------------------------------- TYPE
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
        ks.run("XADD", "x", "*", "f", "v");
        ks.run("TYPE", "x").expect.to.equal("+stream\r\n");
        ks.run("TYPE", "missing").expect.to.equal("+none\r\n");
    }

    // ---------------------------------------------------------------- RANDOMKEY
    @("valkey.keyspace.randomkey")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        // against an empty DB -> nil bulk
        ks.run("RANDOMKEY").expect.to.equal(NIL);

        // regression: SET then DEL leaves the DB empty -> nil bulk
        ks.run("SET", "x", "10");
        ks.run("DEL", "x");
        ks.run("RANDOMKEY").expect.to.equal(NIL);

        // with a single key, RANDOMKEY must return that key
        ks.run("SET", "only", "1");
        ks.run("RANDOMKEY").expect.to.equal(bulk("only"));

        // with two keys, RANDOMKEY returns one of them
        ks.run("SET", "bar", "y");
        auto rk = ks.run("RANDOMKEY");
        (rk == bulk("only") || rk == bulk("bar")).should.equal(true);
    }

    // ---------------------------------------------------------------- MOVE / SWAPDB
    // The in-process keyspace is detached (not in gDbs), so cross-DB data moves are
    // not observable here; the error surfaces are. Cross-DB verification lives in the
    // blackbox sweep (needs SELECT over a connection).
    @("valkey.keyspace.move_errors")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        ks.run("SET", "mykey", "hello");

        // non-integer target DB -> not-an-integer error
        ks.run("MOVE", "mykey", "notanumber")
            .startsWith("-ERR value is not an integer or out of range")
            .should.equal(true);

        // out-of-range target DB -> DB index out of range
        ks.run("MOVE", "mykey", "999")
            .startsWith("-ERR DB index is out of range").should.equal(true);

        // extra token past `key db [REPLACE]` -> syntax error
        ks.run("MOVE", "mykey", "1", "notreplace")
            .startsWith("-ERR syntax error").should.equal(true);
        ks.run("MOVE", "mykey", "1", "REPLACE", "extra")
            .startsWith("-ERR syntax error").should.equal(true);
    }

    @("valkey.keyspace.swapdb_errors")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        // out-of-range indices
        ks.run("SWAPDB", "44", "55")
            .startsWith("-ERR DB index is out of range").should.equal(true);
        // non-integer indices -> per-position invalid-index errors
        ks.run("SWAPDB", "44", "a")
            .startsWith("-ERR invalid second DB index").should.equal(true);
        ks.run("SWAPDB", "a", "55")
            .startsWith("-ERR invalid first DB index").should.equal(true);
        ks.run("SWAPDB", "a", "b")
            .startsWith("-ERR invalid first DB index").should.equal(true);
        // swapping a db with itself is a well-formed no-op -> OK
        ks.run("SWAPDB", "0", "0").expect.to.equal("+OK\r\n");
    }
}
