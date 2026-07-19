module tests.valkey_string_tests;

// Valkey unit/type/string.tcl ported to native in-process UT. The tcl is a
// read-only spec + oracle (expected reply bytes captured from valkey-server);
// nothing tcl lands in the repo. Cluster hash-tags ({t}) are dropped (single-node
// UT). Server-only cases (replica propagation, keyspace notifications, DEBUG
// RELOAD/OBJECT/set-active-expire, memory usage, OBJECT ENCODING, real wall-clock
// 'after N') and fuzz loops are skipped; every deterministic reply is ported.
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

    // ------------------------------------------------------------------
    @("valkey.string.set_get_basic")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        // SET and GET an item
        ks.run("SET", "x", "foobar").expect.to.equal("+OK\r\n");
        ks.run("GET", "x").expect.to.equal(bulk("foobar"));

        // SET and GET an empty item
        ks.run("SET", "x", "").expect.to.equal("+OK\r\n");
        ks.run("GET", "x").expect.to.equal(bulk(""));

        // GET on a missing key -> nil bulk
        ks.run("GET", "nope").expect.to.equal(NIL);

        // Big payload round-trips
        string buf;
        foreach (_; 0 .. 4000)
            buf ~= "abcd";
        ks.run("SET", "foo", buf).expect.to.equal("+OK\r\n");
        ks.run("GET", "foo").expect.to.equal(bulk(buf));

        // SET wrong number of args
        ks.run("SET", "k").startsWith("-ERR").should.equal(true);

        // GET against wrong type -> WRONGTYPE
        ks.run("DEL", "lk");
        ks.run("LPUSH", "lk", "x");
        ks.run("GET", "lk").startsWith("-WRONGTYPE").should.equal(true);
    }

    // ------------------------------------------------------------------
    @("valkey.string.setnx")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        // SETNX target key missing -> 1, sets value
        ks.run("DEL", "novar");
        ks.run("SETNX", "novar", "foobared").expect.to.equal(":1\r\n");
        ks.run("GET", "novar").expect.to.equal(bulk("foobared"));

        // SETNX target key exists -> 0, value unchanged
        ks.run("SET", "novar", "foobared");
        ks.run("SETNX", "novar", "blabla").expect.to.equal(":0\r\n");
        ks.run("GET", "novar").expect.to.equal(bulk("foobared"));

        // SETNX against not-expired volatile key -> 0
        ks.run("SET", "vx", "10");
        ks.run("EXPIRE", "vx", "10000");
        ks.run("SETNX", "vx", "20").expect.to.equal(":0\r\n");
        ks.run("GET", "vx").expect.to.equal(bulk("10"));
    }

    // ------------------------------------------------------------------
    @("valkey.string.getset")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        // GETSET (set new value) -> nil old, then value visible
        ks.run("DEL", "foo");
        ks.run("GETSET", "foo", "xyz").expect.to.equal(NIL);
        ks.run("GET", "foo").expect.to.equal(bulk("xyz"));

        // GETSET (replace old value) -> old value returned
        ks.run("SET", "foo", "bar");
        ks.run("GETSET", "foo", "xyz").expect.to.equal(bulk("bar"));
        ks.run("GET", "foo").expect.to.equal(bulk("xyz"));
    }

    // ------------------------------------------------------------------
    @("valkey.string.getdel_getex")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        // GETDEL: returns value then deletes; second call nil
        ks.run("DEL", "foo");
        ks.run("SET", "foo", "bar");
        ks.run("GETDEL", "foo").expect.to.equal(bulk("bar"));
        ks.run("GETDEL", "foo").expect.to.equal(NIL);

        // GETEX EX option (frozen clock -> exact TTL)
        ks.run("DEL", "foo");
        ks.run("SET", "foo", "bar");
        ks.run("GETEX", "foo", "EX", "10").expect.to.equal(bulk("bar"));
        ks.run("TTL", "foo").expect.to.equal(":10\r\n");

        // GETEX PX option
        ks.run("SET", "foo", "bar");
        ks.run("GETEX", "foo", "PX", "10000").expect.to.equal(bulk("bar"));
        ks.run("PTTL", "foo").expect.to.equal(":10000\r\n");

        // GETEX PERSIST removes the TTL
        ks.run("SET", "foo", "bar", "EX", "10");
        ks.run("TTL", "foo").expect.to.equal(":10\r\n");
        ks.run("GETEX", "foo", "PERSIST").expect.to.equal(bulk("bar"));
        ks.run("TTL", "foo").expect.to.equal(":-1\r\n");

        // GETEX no option -> value, no TTL change
        ks.run("DEL", "foo");
        ks.run("SET", "foo", "bar");
        ks.run("GETEX", "foo").expect.to.equal(bulk("bar"));
        ks.run("TTL", "foo").expect.to.equal(":-1\r\n");

        // GETEX on a missing key -> nil
        ks.run("DEL", "gx");
        ks.run("GETEX", "gx").expect.to.equal(NIL);

        // GETEX syntax errors / wrong number of arguments
        ks.run("GETEX", "foo", "non-existent-option").startsWith("-ERR").should.equal(true);
        ks.run("GETEX").startsWith("-ERR").should.equal(true);
    }

    // ------------------------------------------------------------------
    @("valkey.string.mget")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        ks.run("SET", "foo", "BAR");
        ks.run("SET", "bar", "FOO");
        ks.run("MGET", "foo", "bar").expect.to.equal(arrB("BAR", "FOO"));

        // Against a non existing key -> nil in the middle
        ks.run("MGET", "foo", "baazz", "bar")
            .expect.to.equal("*3\r\n" ~ bulk("BAR") ~ NIL ~ bulk("FOO"));

        // Against a non-string key -> nil
        ks.run("SADD", "myset", "ciao");
        ks.run("SADD", "myset", "bau");
        ks.run("MGET", "foo", "baazz", "bar", "myset")
            .expect.to.equal("*4\r\n" ~ bulk("BAR") ~ NIL ~ bulk("FOO") ~ NIL);
    }

    // ------------------------------------------------------------------
    @("valkey.string.mset_msetnx")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        // MSET base case (including binary-ish values)
        ks.run("MSET", "x", "10", "y", "foo bar", "z", "x x x x x x x\n\n\r\n")
            .expect.to.equal("+OK\r\n");
        ks.run("MGET", "x", "y", "z")
            .expect.to.equal(arrB("10", "foo bar", "x x x x x x x\n\n\r\n"));

        // MSET / MSETNX wrong number of args (odd)
        ks.run("MSET", "x", "10", "y", "foo bar", "z").startsWith("-ERR").should.equal(true);
        ks.run("MSETNX", "x", "20", "y", "foo bar", "z").startsWith("-ERR").should.equal(true);

        // MSET with same key twice -> last value wins
        ks.run("SET", "x", "x");
        ks.run("MSET", "x", "xxx", "x", "yyy").expect.to.equal("+OK\r\n");
        ks.run("GET", "x").expect.to.equal(bulk("yyy"));

        // MSETNX with an already existent key -> 0, nothing set (atomic)
        ks.run("MSETNX", "x1", "xxx", "y2", "yyy", "x", "20").expect.to.equal(":0\r\n");
        ks.run("EXISTS", "x1").expect.to.equal(":0\r\n");
        ks.run("EXISTS", "y2").expect.to.equal(":0\r\n");

        // MSETNX with not existing keys -> 1
        ks.run("MSETNX", "x1", "xxx", "y2", "yyy").expect.to.equal(":1\r\n");
        ks.run("GET", "x1").expect.to.equal(bulk("xxx"));
        ks.run("GET", "y2").expect.to.equal(bulk("yyy"));

        // MSETNX not existing keys - same key twice -> 1, last wins
        ks.run("DEL", "x1");
        ks.run("MSETNX", "x1", "xxx", "x1", "yyy").expect.to.equal(":1\r\n");
        ks.run("GET", "x1").expect.to.equal(bulk("yyy"));

        // MSETNX already existing - same key twice -> 0
        ks.run("MSETNX", "x1", "xxx", "x1", "zzz").expect.to.equal(":0\r\n");
        ks.run("GET", "x1").expect.to.equal(bulk("yyy"));
    }

    // ------------------------------------------------------------------
    @("valkey.string.extended_set_flags")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        // Bogus option -> syntax error
        ks.run("SET", "foo", "bar", "non-existing-option").startsWith("-ERR").should.equal(true);

        // NX: set only when absent
        ks.run("DEL", "foo");
        ks.run("SET", "foo", "1", "NX").expect.to.equal("+OK\r\n");
        ks.run("SET", "foo", "2", "NX").expect.to.equal(NIL);
        ks.run("GET", "foo").expect.to.equal(bulk("1"));

        // XX: set only when present
        ks.run("DEL", "foo");
        ks.run("SET", "foo", "1", "XX").expect.to.equal(NIL);
        ks.run("SET", "foo", "bar");
        ks.run("SET", "foo", "2", "XX").expect.to.equal("+OK\r\n");
        ks.run("GET", "foo").expect.to.equal(bulk("2"));

        // EX / PX set an exact TTL (frozen clock)
        ks.run("DEL", "foo");
        ks.run("SET", "foo", "bar", "EX", "10").expect.to.equal("+OK\r\n");
        ks.run("TTL", "foo").expect.to.equal(":10\r\n");
        ks.run("SET", "foo", "bar", "PX", "10000").expect.to.equal("+OK\r\n");
        ks.run("PTTL", "foo").expect.to.equal(":10000\r\n");

        // Multiple options at once: XX PX
        ks.run("SET", "foo", "val");
        ks.run("SET", "foo", "bar", "XX", "PX", "10000").expect.to.equal("+OK\r\n");
        ks.run("PTTL", "foo").expect.to.equal(":10000\r\n");
    }

    // ------------------------------------------------------------------
    @("valkey.string.extended_set_get")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        // GET returns the old value
        ks.run("DEL", "foo");
        ks.run("SET", "foo", "bar");
        ks.run("SET", "foo", "bar2", "GET").expect.to.equal(bulk("bar"));
        ks.run("GET", "foo").expect.to.equal(bulk("bar2"));

        // GET with no previous value -> nil, but still sets
        ks.run("DEL", "foo");
        ks.run("SET", "foo", "bar", "GET").expect.to.equal(NIL);
        ks.run("GET", "foo").expect.to.equal(bulk("bar"));

        // GET + XX with a previous value
        ks.run("DEL", "foo");
        ks.run("SET", "foo", "bar");
        ks.run("SET", "foo", "baz", "GET", "XX").expect.to.equal(bulk("bar"));
        ks.run("GET", "foo").expect.to.equal(bulk("baz"));

        // GET + XX with no previous value -> nil, not set
        ks.run("DEL", "foo");
        ks.run("SET", "foo", "bar", "GET", "XX").expect.to.equal(NIL);
        ks.run("GET", "foo").expect.to.equal(NIL);

        // GET + NX with no previous value -> nil, sets it
        ks.run("DEL", "foo");
        ks.run("SET", "foo", "bar", "GET", "NX").expect.to.equal(NIL);
        ks.run("GET", "foo").expect.to.equal(bulk("bar"));

        // GET + NX with a previous value -> returns old, does NOT overwrite
        ks.run("SET", "foo", "bar");
        ks.run("SET", "foo", "baz", "GET", "NX").expect.to.equal(bulk("bar"));
        ks.run("GET", "foo").expect.to.equal(bulk("bar"));

        // GET against a wrong type -> WRONGTYPE, value preserved
        ks.run("DEL", "foo");
        ks.run("RPUSH", "foo", "waffle");
        ks.run("SET", "foo", "bar", "GET").startsWith("-WRONGTYPE").should.equal(true);
        ks.run("RPOP", "foo").expect.to.equal(bulk("waffle"));
    }

    // ------------------------------------------------------------------
    @("valkey.string.set_ifeq")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        // IFEQ: only set when current value matches
        ks.run("DEL", "foo");
        ks.run("SET", "foo", "initial_value");
        ks.run("SET", "foo", "new_value", "IFEQ", "initial_value").expect.to.equal("+OK\r\n");
        ks.run("GET", "foo").expect.to.equal(bulk("new_value"));
        ks.run("SET", "foo", "should_not_set", "IFEQ", "wrong_value").expect.to.equal(NIL);
        ks.run("GET", "foo").expect.to.equal(bulk("new_value"));

        // IFEQ against a non-string current value -> WRONGTYPE
        ks.run("DEL", "foo");
        ks.run("SADD", "foo", "some_set_value");
        ks.run("SET", "foo", "new_value", "IFEQ", "some_set_value")
            .startsWith("-WRONGTYPE").should.equal(true);

        // IFEQ with GET: no matching key -> nil, not set
        ks.run("DEL", "foo");
        ks.run("SET", "foo", "new_value", "IFEQ", "initial_value", "GET").expect.to.equal(NIL);
        ks.run("GET", "foo").expect.to.equal(NIL);

        // IFEQ with GET: match -> returns old value, sets new
        ks.run("SET", "foo", "initial_value");
        ks.run("SET", "foo", "new_value", "IFEQ", "initial_value", "GET")
            .expect.to.equal(bulk("initial_value"));
        ks.run("GET", "foo").expect.to.equal(bulk("new_value"));

        // IFEQ combined with XX / NX -> syntax error
        ks.run("DEL", "foo");
        ks.run("SET", "foo", "new_value", "IFEQ", "initial_value", "XX")
            .startsWith("-ERR").should.equal(true);
        ks.run("SET", "foo", "new_value", "IFEQ", "initial_value", "NX")
            .startsWith("-ERR").should.equal(true);
    }

    // ------------------------------------------------------------------
    @("valkey.string.delifeq")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        // Non-existing key -> 0
        ks.run("DEL", "foo");
        ks.run("DELIFEQ", "foo", "test").expect.to.equal(":0\r\n");

        // Existing key, matching value -> 1 (deleted)
        ks.run("SET", "foo", "test");
        ks.run("DELIFEQ", "foo", "test").expect.to.equal(":1\r\n");
        ks.run("EXISTS", "foo").expect.to.equal(":0\r\n");

        // Existing key, non-matching value -> 0
        ks.run("SET", "foo", "nope");
        ks.run("DELIFEQ", "foo", "test").expect.to.equal(":0\r\n");
        ks.run("EXISTS", "foo").expect.to.equal(":1\r\n");

        // Non-string value -> WRONGTYPE
        ks.run("DEL", "foo");
        ks.run("SADD", "foo", "test");
        ks.run("DELIFEQ", "foo", "test").startsWith("-WRONGTYPE").should.equal(true);
    }

    // ------------------------------------------------------------------
    @("valkey.string.strlen_append")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        // STRLEN against non-existing key
        ks.run("STRLEN", "notakey").expect.to.equal(":0\r\n");

        // STRLEN against integer-encoded value
        ks.run("SET", "myinteger", "-555");
        ks.run("STRLEN", "myinteger").expect.to.equal(":4\r\n");

        // STRLEN against plain string
        ks.run("SET", "mystring", "foozzz0123456789 baz");
        ks.run("STRLEN", "mystring").expect.to.equal(":20\r\n");

        // STRLEN wrong type
        ks.run("DEL", "l");
        ks.run("LPUSH", "l", "a");
        ks.run("STRLEN", "l").startsWith("-WRONGTYPE").should.equal(true);

        // APPEND creates then extends, returning the new length
        ks.run("DEL", "k");
        ks.run("APPEND", "k", "Hello ").expect.to.equal(":6\r\n");
        ks.run("APPEND", "k", "World").expect.to.equal(":11\r\n");
        ks.run("GET", "k").expect.to.equal(bulk("Hello World"));

        // APPEND onto an int-encoded value (returns full length, value concatenated)
        ks.run("DEL", "foo");
        ks.run("SET", "foo", "1");
        ks.run("APPEND", "foo", "2").expect.to.equal(":2\r\n");
        ks.run("GET", "foo").expect.to.equal(bulk("12"));
    }

    // ------------------------------------------------------------------
    @("valkey.string.setbit")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        // SETBIT against non-existing key: bit 1 -> 0x40 == "@"
        ks.run("DEL", "mykey");
        ks.run("SETBIT", "mykey", "1", "1").expect.to.equal(":0\r\n");
        ks.run("GET", "mykey").expect.to.equal(bulk("@"));

        // SETBIT against string-encoded key "@" (0x40 = 01000000)
        ks.run("SET", "mykey", "@");
        ks.run("SETBIT", "mykey", "2", "1").expect.to.equal(":0\r\n"); // -> 01100000 = "`"
        ks.run("GET", "mykey").expect.to.equal(bulk("`"));
        ks.run("SETBIT", "mykey", "1", "0").expect.to.equal(":1\r\n"); // -> 00100000 = " "
        ks.run("GET", "mykey").expect.to.equal(bulk(" "));

        // SETBIT against wrong type
        ks.run("DEL", "mykey");
        ks.run("LPUSH", "mykey", "foo");
        ks.run("SETBIT", "mykey", "0", "1").startsWith("-WRONGTYPE").should.equal(true);

        // Out-of-range bit offset
        ks.run("DEL", "mykey");
        ks.run("SETBIT", "mykey", "4294967296", "1").startsWith("-ERR").should.equal(true);
        ks.run("SETBIT", "mykey", "-1", "1").startsWith("-ERR").should.equal(true);

        // Non-bit argument
        ks.run("SETBIT", "mykey", "0", "-1").startsWith("-ERR").should.equal(true);
        ks.run("SETBIT", "mykey", "0", "2").startsWith("-ERR").should.equal(true);
        ks.run("SETBIT", "mykey", "0", "10").startsWith("-ERR").should.equal(true);
    }

    // ------------------------------------------------------------------
    @("valkey.string.getbit")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        // GETBIT against non-existing key -> 0
        ks.run("DEL", "mykey");
        ks.run("GETBIT", "mykey", "0").expect.to.equal(":0\r\n");

        // GETBIT against string-encoded key "`" (0x60 = 01100000)
        ks.run("SET", "mykey", "`");
        ks.run("GETBIT", "mykey", "0").expect.to.equal(":0\r\n");
        ks.run("GETBIT", "mykey", "1").expect.to.equal(":1\r\n");
        ks.run("GETBIT", "mykey", "2").expect.to.equal(":1\r\n");
        ks.run("GETBIT", "mykey", "3").expect.to.equal(":0\r\n");
        // Out-of-range bits read as 0
        ks.run("GETBIT", "mykey", "8").expect.to.equal(":0\r\n");
        ks.run("GETBIT", "mykey", "100").expect.to.equal(":0\r\n");
        ks.run("GETBIT", "mykey", "10000").expect.to.equal(":0\r\n");
    }

    // ------------------------------------------------------------------
    @("valkey.string.setrange")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        // Against non-existing key
        ks.run("DEL", "mykey");
        ks.run("SETRANGE", "mykey", "0", "foo").expect.to.equal(":3\r\n");
        ks.run("GET", "mykey").expect.to.equal(bulk("foo"));

        // Empty value on a missing key -> 0, key not created
        ks.run("DEL", "mykey");
        ks.run("SETRANGE", "mykey", "0", "").expect.to.equal(":0\r\n");
        ks.run("EXISTS", "mykey").expect.to.equal(":0\r\n");

        // Offset > 0 on missing key -> zero-pad
        ks.run("DEL", "mykey");
        ks.run("SETRANGE", "mykey", "1", "foo").expect.to.equal(":4\r\n");
        ks.run("GET", "mykey").expect.to.equal(bulk("\0foo"));

        // Against a string value: overwrite at various offsets
        ks.run("SET", "mykey", "foo");
        ks.run("SETRANGE", "mykey", "0", "b").expect.to.equal(":3\r\n");
        ks.run("GET", "mykey").expect.to.equal(bulk("boo"));

        // Empty value leaves the string unchanged
        ks.run("SET", "mykey", "foo");
        ks.run("SETRANGE", "mykey", "0", "").expect.to.equal(":3\r\n");
        ks.run("GET", "mykey").expect.to.equal(bulk("foo"));

        ks.run("SET", "mykey", "foo");
        ks.run("SETRANGE", "mykey", "1", "b").expect.to.equal(":3\r\n");
        ks.run("GET", "mykey").expect.to.equal(bulk("fbo"));

        // Extend past the end with zero padding
        ks.run("SET", "mykey", "foo");
        ks.run("SETRANGE", "mykey", "4", "bar").expect.to.equal(":7\r\n");
        ks.run("GET", "mykey").expect.to.equal(bulk("foo\0bar"));

        // Against int-encoded value
        ks.run("SET", "mykey", "1234");
        ks.run("SETRANGE", "mykey", "0", "2").expect.to.equal(":4\r\n");
        ks.run("GET", "mykey").expect.to.equal(bulk("2234"));

        // Wrong type
        ks.run("DEL", "mykey");
        ks.run("LPUSH", "mykey", "foo");
        ks.run("SETRANGE", "mykey", "0", "bar").startsWith("-WRONGTYPE").should.equal(true);

        // Negative offset -> out of range
        ks.run("SET", "mykey", "hello");
        ks.run("SETRANGE", "mykey", "-1", "world").startsWith("-ERR").should.equal(true);

        // Offset beyond maximum allowed size
        ks.run("SETRANGE", "mykey", "536870908", "world").startsWith("-ERR").should.equal(true);
    }

    // ------------------------------------------------------------------
    @("valkey.string.getrange")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        // Against non-existing key -> empty
        ks.run("DEL", "mykey");
        ks.run("GETRANGE", "mykey", "0", "-1").expect.to.equal(bulk(""));

        // Against string value
        ks.run("SET", "mykey", "Hello World");
        ks.run("GETRANGE", "mykey", "0", "3").expect.to.equal(bulk("Hell"));
        ks.run("GETRANGE", "mykey", "0", "-1").expect.to.equal(bulk("Hello World"));
        ks.run("GETRANGE", "mykey", "-4", "-1").expect.to.equal(bulk("orld"));
        ks.run("GETRANGE", "mykey", "5", "3").expect.to.equal(bulk("")); // start>end
        ks.run("GETRANGE", "mykey", "5", "5000").expect.to.equal(bulk(" World"));
        ks.run("GETRANGE", "mykey", "-5000", "10000").expect.to.equal(bulk("Hello World"));

        // Against integer-encoded value
        ks.run("SET", "mykey", "1234");
        ks.run("GETRANGE", "mykey", "0", "2").expect.to.equal(bulk("123"));
        ks.run("GETRANGE", "mykey", "0", "-1").expect.to.equal(bulk("1234"));
        ks.run("GETRANGE", "mykey", "-3", "-1").expect.to.equal(bulk("234"));
        ks.run("GETRANGE", "mykey", "5", "3").expect.to.equal(bulk(""));
        ks.run("GETRANGE", "mykey", "3", "5000").expect.to.equal(bulk("4"));
        ks.run("GETRANGE", "mykey", "-5000", "10000").expect.to.equal(bulk("1234"));

        // Huge end range gets clamped (Github issue #1844)
        ks.run("SET", "foo", "bar");
        ks.run("GETRANGE", "foo", "0", "4294967297").expect.to.equal(bulk("bar"));

        // Wrong key type
        ks.run("DEL", "lkey1");
        ks.run("LPUSH", "lkey1", "list");
        ks.run("GETRANGE", "lkey1", "0", "-1").startsWith("-WRONGTYPE").should.equal(true);
    }

    // ------------------------------------------------------------------
    @("valkey.string.substr")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        ks.run("SET", "key", "abcde");
        ks.run("SUBSTR", "key", "0", "0").expect.to.equal(bulk("a"));
        ks.run("SUBSTR", "key", "0", "3").expect.to.equal(bulk("abcd"));
        ks.run("SUBSTR", "key", "-4", "-1").expect.to.equal(bulk("bcde"));
        ks.run("SUBSTR", "key", "-1", "-3").expect.to.equal(bulk(""));
        ks.run("SUBSTR", "key", "7", "8").expect.to.equal(bulk(""));
        ks.run("SUBSTR", "nokey", "0", "1").expect.to.equal(bulk(""));
    }

    // ------------------------------------------------------------------
    @("valkey.string.lcs")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        ks.run("SET", "key1", "ohmytext");
        ks.run("SET", "key2", "mynewtext");

        // LCS basic + LEN
        ks.run("LCS", "key1", "key2").expect.to.equal(bulk("mytext"));
        ks.run("LCS", "key1", "key2", "LEN").expect.to.equal(":6\r\n");

        // No common subsequence -> empty bulk
        ks.run("SET", "a", "abc");
        ks.run("SET", "b", "xyz");
        ks.run("LCS", "a", "b").expect.to.equal(bulk(""));

        // Missing keys treated as empty -> empty bulk
        ks.run("LCS", "nope1", "nope2").expect.to.equal(bulk(""));

        // Wrong type -> ERR
        ks.run("DEL", "s");
        ks.run("SADD", "s", "x");
        ks.run("LCS", "key1", "s").startsWith("-ERR").should.equal(true);

        // LCS IDX (matches array + len). Two matches: [4,7]<->[5,8] and [2,3]<->[0,1]
        immutable idx = "*4\r\n" ~ bulk("matches")
            ~ "*2\r\n"
            ~ "*2\r\n" ~ "*2\r\n:4\r\n:7\r\n" ~ "*2\r\n:5\r\n:8\r\n"
            ~ "*2\r\n" ~ "*2\r\n:2\r\n:3\r\n" ~ "*2\r\n:0\r\n:1\r\n"
            ~ bulk("len") ~ ":6\r\n";
        ks.run("LCS", "key1", "key2", "IDX").expect.to.equal(idx);

        // LCS IDX WITHMATCHLEN (adds the match length to each pair)
        immutable idxwml = "*4\r\n" ~ bulk("matches")
            ~ "*2\r\n"
            ~ "*3\r\n" ~ "*2\r\n:4\r\n:7\r\n" ~ "*2\r\n:5\r\n:8\r\n" ~ ":4\r\n"
            ~ "*3\r\n" ~ "*2\r\n:2\r\n:3\r\n" ~ "*2\r\n:0\r\n:1\r\n" ~ ":2\r\n"
            ~ bulk("len") ~ ":6\r\n";
        ks.run("LCS", "key1", "key2", "IDX", "WITHMATCHLEN").expect.to.equal(idxwml);

        // MINMATCHLEN filters out the short match, keeping only the len-4 one
        immutable idxmin = "*4\r\n" ~ bulk("matches")
            ~ "*1\r\n"
            ~ "*3\r\n" ~ "*2\r\n:4\r\n:7\r\n" ~ "*2\r\n:5\r\n:8\r\n" ~ ":4\r\n"
            ~ bulk("len") ~ ":6\r\n";
        ks.run("LCS", "key1", "key2", "IDX", "WITHMATCHLEN", "MINMATCHLEN", "4")
            .expect.to.equal(idxmin);
    }

    // ------------------------------------------------------------------
    @("valkey.string.msetex_basic")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        // MSETEX with EX option -> success (1), TTL exact
        ks.run("DEL", "key1", "key2");
        ks.run("MSETEX", "2", "key1", "val1", "key2", "val2", "EX", "10").expect.to.equal(":1\r\n");
        ks.run("MGET", "key1", "key2").expect.to.equal(arrB("val1", "val2"));
        ks.run("TTL", "key1").expect.to.equal(":10\r\n");
        ks.run("TTL", "key2").expect.to.equal(":10\r\n");

        // PX option
        ks.run("DEL", "key1", "key2");
        ks.run("MSETEX", "2", "key1", "val1", "key2", "val2", "PX", "10000").expect.to.equal(":1\r\n");
        ks.run("PTTL", "key1").expect.to.equal(":10000\r\n");

        // KEEPTTL retains TTL; without options TTL is removed
        ks.run("DEL", "key1", "key2");
        ks.run("MSETEX", "2", "key1", "val1", "key2", "val2", "EX", "100");
        ks.run("MSETEX", "2", "key1", "n1", "key2", "n2", "KEEPTTL").expect.to.equal(":1\r\n");
        ks.run("MGET", "key1", "key2").expect.to.equal(arrB("n1", "n2"));
        ks.run("TTL", "key1").expect.to.equal(":100\r\n");

        // Without options -> new key has no TTL; overwriting drops TTL
        ks.run("DEL", "key1", "key2");
        ks.run("MSETEX", "2", "key1", "val1", "key2", "val2").expect.to.equal(":1\r\n");
        ks.run("TTL", "key1").expect.to.equal(":-1\r\n");
        ks.run("SET", "key1", "old1", "EX", "100");
        ks.run("MSETEX", "1", "key1", "new1").expect.to.equal(":1\r\n");
        ks.run("TTL", "key1").expect.to.equal(":-1\r\n");

        // Same key twice -> last value wins
        ks.run("DEL", "key1");
        ks.run("MSETEX", "2", "key1", "val1", "key1", "val2").expect.to.equal(":1\r\n");
        ks.run("GET", "key1").expect.to.equal(bulk("val2"));
    }

    // ------------------------------------------------------------------
    @("valkey.string.msetex_nx_xx")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        // NX: only set if all keys absent
        ks.run("DEL", "key1", "key2");
        ks.run("MSETEX", "2", "key1", "val1", "key2", "val2", "NX").expect.to.equal(":1\r\n");
        ks.run("GET", "key1").expect.to.equal(bulk("val1"));
        ks.run("GET", "key2").expect.to.equal(bulk("val2"));

        // NX fails (0) when any key exists; nothing changed / nothing set
        ks.run("DEL", "key1", "key2");
        ks.run("SET", "key1", "existing");
        ks.run("MSETEX", "2", "key1", "val1", "key2", "val2", "NX").expect.to.equal(":0\r\n");
        ks.run("GET", "key1").expect.to.equal(bulk("existing"));
        ks.run("GET", "key2").expect.to.equal(NIL);

        // XX: only set if all keys exist
        ks.run("MSET", "key1", "existing", "key2", "existing");
        ks.run("MSETEX", "2", "key1", "val1", "key2", "val2", "XX").expect.to.equal(":1\r\n");
        ks.run("GET", "key1").expect.to.equal(bulk("val1"));

        // XX fails when a key is missing
        ks.run("SET", "key1", "existing");
        ks.run("DEL", "key2");
        ks.run("MSETEX", "2", "key1", "val1", "key2", "val2", "XX").expect.to.equal(":0\r\n");
        ks.run("GET", "key1").expect.to.equal(bulk("existing"));
        ks.run("EXISTS", "key2").expect.to.equal(":0\r\n");

        // NX + EX combined
        ks.run("DEL", "key1", "key2");
        ks.run("MSETEX", "2", "key1", "val1", "key2", "val2", "NX", "EX", "10")
            .expect.to.equal(":1\r\n");
        ks.run("TTL", "key1").expect.to.equal(":10\r\n");

        // XX + PX combined
        ks.run("MSET", "key1", "old1", "key2", "old2");
        ks.run("MSETEX", "2", "key1", "val1", "key2", "val2", "XX", "PX", "10000")
            .expect.to.equal(":1\r\n");
        ks.run("PTTL", "key1").expect.to.equal(":10000\r\n");
    }

    // ------------------------------------------------------------------
    @("valkey.string.msetex_errors")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        // Wrong number of arguments
        ks.run("MSETEX").startsWith("-ERR").should.equal(true);
        ks.run("MSETEX", "key", "value").startsWith("-ERR").should.equal(true);

        // Invalid numkeys
        ks.run("MSETEX", "0", "key1", "value").startsWith("-ERR").should.equal(true);
        ks.run("MSETEX", "-1", "key1", "value").startsWith("-ERR").should.equal(true);
        ks.run("MSETEX", "1.5", "key1", "value").startsWith("-ERR").should.equal(true);
        ks.run("MSETEX", "numkeys", "key1", "value").startsWith("-ERR").should.equal(true);

        // numkeys mismatch -> syntax error
        ks.run("MSETEX", "2", "key1", "value").startsWith("-ERR").should.equal(true);

        // NX and XX together -> syntax error
        ks.run("MSETEX", "1", "key1", "value", "NX", "XX").startsWith("-ERR").should.equal(true);
        ks.run("MSETEX", "1", "key1", "value", "XX", "NX").startsWith("-ERR").should.equal(true);

        // Expire-flag argument errors
        ks.run("MSETEX", "1", "key1", "value", "EX").startsWith("-ERR").should.equal(true);
        ks.run("MSETEX", "1", "key1", "value", "EX", "0").startsWith("-ERR").should.equal(true);
        ks.run("MSETEX", "1", "key1", "value", "EX", "-1").startsWith("-ERR").should.equal(true);
        ks.run("MSETEX", "1", "key1", "value", "EX", "1.5").startsWith("-ERR").should.equal(true);
        ks.run("MSETEX", "1", "key1", "value", "EX", "foo").startsWith("-ERR").should.equal(true);

        // Mutually exclusive expire options
        ks.run("MSETEX", "1", "key1", "value", "EX", "1", "PX", "1")
            .startsWith("-ERR").should.equal(true);
        ks.run("MSETEX", "1", "key1", "value", "EX", "1", "EXAT", "1")
            .startsWith("-ERR").should.equal(true);
        ks.run("MSETEX", "1", "key1", "value", "EX", "1", "KEEPTTL")
            .startsWith("-ERR").should.equal(true);

        // Unknown trailing option
        ks.run("MSETEX", "1", "key1", "value", "EX", "1", "wrong_option")
            .startsWith("-ERR").should.equal(true);
    }
}
