module tests.valkey_string_tests;

// Valkey unit/type/string.tcl ported to native in-process UT (see valkey_incr_tests.d
// for the "contrabando" rationale + THIRD_PARTY_NOTICES credit). Cluster hash-tags
// ({t}) dropped (single-node UT). Valkey-specific extensions (MSETEX, SET IFEQ,
// DELIFEQ) and server-only cases (replica propagation, keyspace notifications, DEBUG
// RELOAD, memory usage) are left to the blackbox sweep / catalogued as gaps.

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

    enum NIL = "$-1\r\n"; // RESP2 null bulk

    @("valkey.string.set_get_mget_getset")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        // SET/GET, empty item
        ks.run("SET", "x", "foobar").expect.to.equal("+OK\r\n");
        ks.run("GET", "x").expect.to.equal(bulk("foobar"));
        ks.run("SET", "e", "").expect.to.equal("+OK\r\n");
        ks.run("GET", "e").expect.to.equal(bulk(""));

        // MGET: existing, non-existing (nil), non-string (nil)
        ks.run("SET", "foo", "BAR");
        ks.run("SET", "bar", "FOO");
        ks.run("MGET", "foo", "bar").expect.to.equal("*2\r\n" ~ bulk("BAR") ~ bulk("FOO"));
        ks.run("MGET", "foo", "baazz", "bar").expect.to.equal("*3\r\n" ~ bulk("BAR") ~ NIL ~ bulk("FOO"));
        ks.run("SADD", "myset", "ciao");
        ks.run("MGET", "foo", "baazz", "bar", "myset").expect.to.equal(
                "*4\r\n" ~ bulk("BAR") ~ NIL ~ bulk("FOO") ~ NIL);

        // GETSET: new value (nil old), replace (old value)
        ks.run("DEL", "g");
        ks.run("GETSET", "g", "xyz").expect.to.equal(NIL);
        ks.run("GET", "g").expect.to.equal(bulk("xyz"));
        ks.run("GETSET", "g", "abc").expect.to.equal(bulk("xyz"));
        ks.run("GET", "g").expect.to.equal(bulk("abc"));
    }

    @("valkey.string.mset_msetnx")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        // MSET base + wrong-arg-count
        ks.run("MSET", "x", "10", "y", "foo bar").expect.to.equal("+OK\r\n");
        ks.run("MGET", "x", "y").expect.to.equal("*2\r\n" ~ bulk("10") ~ bulk("foo bar"));
        ks.run("MSET", "a", "1", "b").startsWith("-ERR").should.equal(true); // odd args
        ks.run("MSETNX", "a", "1", "b").startsWith("-ERR").should.equal(true);

        // MSET same key twice — last wins
        ks.run("SET", "k", "x");
        ks.run("MSET", "k", "xxx", "k", "yyy").expect.to.equal("+OK\r\n");
        ks.run("GET", "k").expect.to.equal(bulk("yyy"));

        // MSETNX: fails (0) if ANY key exists; all-or-nothing
        ks.run("DEL", "x1", "y2");
        ks.run("SET", "ex", "v");
        ks.run("MSETNX", "x1", "xxx", "y2", "yyy", "ex", "20").expect.to.equal(":0\r\n");
        ks.run("EXISTS", "x1").expect.to.equal(":0\r\n"); // nothing set
        ks.run("MSETNX", "x1", "xxx", "y2", "yyy").expect.to.equal(":1\r\n");
        ks.run("GET", "x1").expect.to.equal(bulk("xxx"));
        ks.run("GET", "y2").expect.to.equal(bulk("yyy"));
    }

    @("valkey.string.extended_set")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        // syntax error on a bogus option
        ks.run("SET", "foo", "bar", "non-existing-option").startsWith("-ERR").should.equal(true);

        // NX: set only if absent
        ks.run("DEL", "foo");
        ks.run("SET", "foo", "1", "NX").expect.to.equal("+OK\r\n");
        ks.run("SET", "foo", "2", "NX").expect.to.equal(NIL); // exists -> nil
        ks.run("GET", "foo").expect.to.equal(bulk("1"));

        // XX: set only if present
        ks.run("DEL", "foo");
        ks.run("SET", "foo", "1", "XX").expect.to.equal(NIL); // absent -> nil
        ks.run("SET", "foo", "bar");
        ks.run("SET", "foo", "2", "XX").expect.to.equal("+OK\r\n");
        ks.run("GET", "foo").expect.to.equal(bulk("2"));

        // GET: return old value alongside the set
        ks.run("DEL", "foo");
        ks.run("SET", "foo", "bar");
        ks.run("SET", "foo", "bar2", "GET").expect.to.equal(bulk("bar")); // old
        ks.run("GET", "foo").expect.to.equal(bulk("bar2"));
        ks.run("DEL", "foo");
        ks.run("SET", "foo", "bar", "GET").expect.to.equal(NIL); // no previous
        // GET + XX (no previous -> nil, not set)
        ks.run("DEL", "foo");
        ks.run("SET", "foo", "bar", "GET", "XX").expect.to.equal(NIL);
        ks.run("GET", "foo").expect.to.equal(NIL);
        // GET + NX (previous exists -> returns old, does NOT overwrite)
        ks.run("SET", "foo", "bar");
        ks.run("SET", "foo", "baz", "GET", "NX").expect.to.equal(bulk("bar"));
        ks.run("GET", "foo").expect.to.equal(bulk("bar"));

        // GET against a wrong type -> WRONGTYPE
        ks.run("DEL", "foo");
        ks.run("RPUSH", "foo", "waffle");
        ks.run("SET", "foo", "bar", "GET").startsWith("-WRONGTYPE").should.equal(true);
    }

    @("valkey.string.append_strlen_range")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        // APPEND: create then extend, returns new length
        ks.run("DEL", "k");
        ks.run("APPEND", "k", "Hello ").expect.to.equal(":6\r\n");
        ks.run("APPEND", "k", "World").expect.to.equal(":11\r\n");
        ks.run("GET", "k").expect.to.equal(bulk("Hello World"));

        // STRLEN
        ks.run("STRLEN", "k").expect.to.equal(":11\r\n");
        ks.run("STRLEN", "missing").expect.to.equal(":0\r\n");

        // SETRANGE: overwrite at offset, zero-pad the gap, returns new length
        ks.run("SET", "sr", "Hello World");
        ks.run("SETRANGE", "sr", "6", "Redis").expect.to.equal(":11\r\n");
        ks.run("GET", "sr").expect.to.equal(bulk("Hello Redis"));
        ks.run("DEL", "sr2");
        ks.run("SETRANGE", "sr2", "5", "Hello").expect.to.equal(":10\r\n");
        ks.run("GET", "sr2").expect.to.equal(bulk("\0\0\0\0\0Hello"));

        // GETRANGE: inclusive [start,end], negatives from the end
        ks.run("SET", "gr", "This is a string");
        ks.run("GETRANGE", "gr", "0", "3").expect.to.equal(bulk("This"));
        ks.run("GETRANGE", "gr", "-3", "-1").expect.to.equal(bulk("ing"));
        ks.run("GETRANGE", "gr", "0", "-1").expect.to.equal(bulk("This is a string"));
        ks.run("GETRANGE", "gr", "10", "100").expect.to.equal(bulk("string"));
    }

    @("valkey.string.lcs")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        // classic Redis LCS example
        ks.run("MSET", "key1", "ohmytext", "key2", "mynewtext");
        ks.run("LCS", "key1", "key2").expect.to.equal(bulk("mytext"));
        ks.run("LCS", "key1", "key2", "LEN").expect.to.equal(":6\r\n");
    }
}
