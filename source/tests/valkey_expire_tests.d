module tests.valkey_expire_tests;

// Valkey unit/expire.tcl ported to native in-process UT (see valkey_incr_tests.d for
// rationale + THIRD_PARTY_NOTICES credit). Cases that need REAL wall-time to elapse
// ("after 2.1s the key is gone", active-expire cycle, lazy-expire), server-only DEBUG
// (set-active-expire/loadaof), replication/AOF propagation, import-mode and CLIENT
// state are server-only -> blackbox sweep. The in-process clock is frozen per command,
// so relative TTLs are exact (EXPIRE 100 -> TTL 100).

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

    // ---- EXPIRE basic behavior + PERSIST -------------------------------------
    @("valkey.expire.basic")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        // EXPIRE - set timeouts multiple times (frozen clock -> exact TTL)
        ks.run("SET", "x", "foobar").expect.to.equal("+OK\r\n");
        ks.run("EXPIRE", "x", "5").expect.to.equal(":1\r\n");
        ks.run("TTL", "x").expect.to.equal(":5\r\n");
        ks.run("EXPIRE", "x", "10").expect.to.equal(":1\r\n");
        ks.run("TTL", "x").expect.to.equal(":10\r\n");

        // It should still be possible to read 'x'
        ks.run("GET", "x").expect.to.equal(bulk("foobar"));

        // EXPIRE - write on expire should work (LPUSH after EXPIRE)
        ks.run("DEL", "x").expect.to.equal(":1\r\n");
        ks.run("LPUSH", "x", "foo").expect.to.equal(":1\r\n");
        ks.run("EXPIRE", "x", "1000").expect.to.equal(":1\r\n");
        ks.run("LPUSH", "x", "bar").expect.to.equal(":2\r\n");
        ks.run("LRANGE", "x", "0", "-1").expect.to.equal(arrB("bar", "foo"));

        // EXPIREAT - EXPIRE-alike behavior; a far-future absolute time yields a TTL
        ks.run("DEL", "x");
        ks.run("SET", "x", "foo");
        // 32503680000 = year 3000 in epoch seconds -> huge positive TTL, key survives
        ks.run("EXPIREAT", "x", "32503680000").expect.to.equal(":1\r\n");
        ks.run("EXISTS", "x").expect.to.equal(":1\r\n");
        // and PTTL is positive (not -1/-2)
        ks.run("PTTL", "x").startsWith(":-").should.equal(false);

        // PERSIST can undo an EXPIRE
        ks.run("SET", "x", "foo");
        ks.run("EXPIRE", "x", "50").expect.to.equal(":1\r\n");
        ks.run("TTL", "x").expect.to.equal(":50\r\n");
        ks.run("PERSIST", "x").expect.to.equal(":1\r\n");
        ks.run("TTL", "x").expect.to.equal(":-1\r\n");
        ks.run("GET", "x").expect.to.equal(bulk("foo"));

        // PERSIST returns 0 against non-volatile or non-existing keys
        ks.run("SET", "nv", "foo");
        ks.run("PERSIST", "nv").expect.to.equal(":0\r\n"); // no TTL
        ks.run("PERSIST", "nokeyatall").expect.to.equal(":0\r\n"); // missing
    }

    // ---- SETEX / PSETEX -------------------------------------------------------
    @("valkey.expire.setex")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        // SETEX: value + TTL together
        ks.run("SETEX", "x", "12", "test").expect.to.equal("+OK\r\n");
        ks.run("TTL", "x").expect.to.equal(":12\r\n");
        ks.run("GET", "x").expect.to.equal(bulk("test"));

        // SETEX overwrite existing key
        ks.run("SET", "y", "old");
        ks.run("SETEX", "y", "1", "foo").expect.to.equal("+OK\r\n");
        ks.run("GET", "y").expect.to.equal(bulk("foo"));
        ks.run("TTL", "y").expect.to.equal(":1\r\n");

        // SETEX wrong time parameter -> invalid expire error
        ks.run("SETEX", "z", "-10", "foo").startsWith("-ERR invalid expire").should.equal(true);
        ks.run("SETEX", "z", "0", "foo").startsWith("-ERR invalid expire").should.equal(true);
        ks.run("SETEX", "z", "abc", "foo").startsWith("-ERR").should.equal(true);

        // PSETEX can set sub-second expires (100ms -> PTTL 100, TTL rounds to 0)
        ks.run("PSETEX", "x", "100", "somevalue").expect.to.equal("+OK\r\n");
        ks.run("PTTL", "x").expect.to.equal(":100\r\n");
        ks.run("GET", "x").expect.to.equal(bulk("somevalue"));

        // PSETEX millisecond larger value
        ks.run("PSETEX", "m", "100000", "v").expect.to.equal("+OK\r\n");
        ks.run("TTL", "m").expect.to.equal(":100\r\n");
        ks.run("PTTL", "m").expect.to.equal(":100000\r\n");
    }

    // ---- PEXPIRE / PEXPIREAT sub-second + already-expired ---------------------
    @("valkey.expire.pexpire")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        // PEXPIRE can set sub-second expires
        ks.run("SET", "x", "somevalue");
        ks.run("PEXPIRE", "x", "100").expect.to.equal(":1\r\n");
        ks.run("PTTL", "x").expect.to.equal(":100\r\n");
        ks.run("GET", "x").expect.to.equal(bulk("somevalue"));

        // EXPIRE / EXPIREAT / PEXPIRE / PEXPIREAT with already-expired time delete
        // the key immediately (return 1).
        ks.run("SET", "x", "somevalue");
        ks.run("EXPIRE", "x", "-1").expect.to.equal(":1\r\n");
        ks.run("EXISTS", "x").expect.to.equal(":0\r\n");

        ks.run("SET", "x", "somevalue");
        ks.run("EXPIREAT", "x", "1").expect.to.equal(":1\r\n"); // epoch 1s = long past
        ks.run("EXISTS", "x").expect.to.equal(":0\r\n");

        ks.run("SET", "x", "somevalue");
        ks.run("PEXPIRE", "x", "-1000").expect.to.equal(":1\r\n");
        ks.run("EXISTS", "x").expect.to.equal(":0\r\n");

        ks.run("SET", "x", "somevalue");
        ks.run("PEXPIREAT", "x", "1").expect.to.equal(":1\r\n"); // 1ms epoch = long past
        ks.run("EXISTS", "x").expect.to.equal(":0\r\n");
    }

    // ---- TTL / PTTL / EXPIRETIME / PEXPIRETIME sentinels ----------------------
    @("valkey.expire.ttl_sentinels")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        // return -1 if key has no expire
        ks.run("SET", "x", "hello");
        ks.run("TTL", "x").expect.to.equal(":-1\r\n");
        ks.run("PTTL", "x").expect.to.equal(":-1\r\n");
        ks.run("EXPIRETIME", "x").expect.to.equal(":-1\r\n");
        ks.run("PEXPIRETIME", "x").expect.to.equal(":-1\r\n");

        // return -2 if key does not exist
        ks.run("DEL", "x");
        ks.run("TTL", "x").expect.to.equal(":-2\r\n");
        ks.run("PTTL", "x").expect.to.equal(":-2\r\n");
        ks.run("EXPIRETIME", "x").expect.to.equal(":-2\r\n");
        ks.run("PEXPIRETIME", "x").expect.to.equal(":-2\r\n");
    }

    // ---- EXPIRETIME / PEXPIRETIME return the absolute set-time ----------------
    @("valkey.expire.expiretime_absolute")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        // EXPIRETIME returns the absolute expiration time in seconds (as set via EXAT)
        immutable absSec = "40000000000"; // far-future epoch seconds
        ks.run("SET", "x", "somevalue", "EXAT", absSec).expect.to.equal("+OK\r\n");
        ks.run("EXPIRETIME", "x").expect.to.equal(":" ~ absSec ~ "\r\n");

        // PEXPIRETIME returns the absolute expiration time in ms (as set via PXAT)
        immutable absMs = "40000000000000"; // far-future epoch ms
        ks.run("SET", "y", "somevalue", "PXAT", absMs).expect.to.equal("+OK\r\n");
        ks.run("PEXPIRETIME", "y").expect.to.equal(":" ~ absMs ~ "\r\n");
    }

    // ---- KEYS sees volatile keys until they actually expire -------------------
    @("valkey.expire.keys_visible")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        // 5 keys in, 5 keys out (a has a future TTL, still listed by KEYS)
        ks.run("SET", "a", "c");
        ks.run("EXPIRE", "a", "5").expect.to.equal(":1\r\n");
        ks.run("SET", "t", "c");
        ks.run("SET", "e", "c");
        ks.run("SET", "s", "c");
        ks.run("SET", "foo", "b");
        sameSet(ks.run("KEYS", "*"), "a", "e", "foo", "s", "t");
        ks.run("DBSIZE").expect.to.equal(":5\r\n");
    }

    // ---- Big-integer overflow error cases -------------------------------------
    @("valkey.expire.overflow_errors")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        ks.run("SET", "foo", "bar");

        // NOTE: the harness freezes a fixed clock (1.7e12 ms) so ms-precision TTLs
        // are deterministic. The first case below overflows only because basetime
        // (that clock) exceeds ~1.638e12 ms — don't lower the harness clock past it.
        // EXPIRE with big integer overflows when converted to milliseconds
        ks.run("EXPIRE", "foo", "9223370399119966")
            .startsWith("-ERR invalid expire time in 'expire' command").should.equal(true);
        ks.run("EXPIRE", "foo", "9223372036854776")
            .startsWith("-ERR invalid expire time in 'expire' command").should.equal(true);
        ks.run("EXPIRE", "foo", "10000000000000000")
            .startsWith("-ERR invalid expire time in 'expire' command").should.equal(true);
        ks.run("EXPIRE", "foo", "18446744073709561")
            .startsWith("-ERR invalid expire time in 'expire' command").should.equal(true);

        // EXPIRE with big negative integer
        ks.run("EXPIRE", "foo", "-9223372036854776")
            .startsWith("-ERR invalid expire time in 'expire' command").should.equal(true);
        ks.run("EXPIRE", "foo", "-9999999999999999")
            .startsWith("-ERR invalid expire time in 'expire' command").should.equal(true);

        // all the above must have left foo unchanged (no TTL)
        ks.run("TTL", "foo").expect.to.equal(":-1\r\n");

        // PEXPIRE with big integer overflow when basetime is added
        ks.run("PEXPIRE", "foo", "9223372036854770000")
            .startsWith("-ERR invalid expire time in 'pexpire' command").should.equal(true);

        // PEXPIREAT with big integer works (absolute time, no basetime add)
        ks.run("SET", "foo", "bar");
        ks.run("PEXPIREAT", "foo", "9223372036854770000").expect.to.equal(":1\r\n");

        // PEXPIREAT with big negative integer works (deletes key -> TTL -2)
        ks.run("SET", "foo", "bar");
        ks.run("PEXPIREAT", "foo", "-9223372036854770000").expect.to.equal(":1\r\n");
        ks.run("TTL", "foo").expect.to.equal(":-2\r\n");
    }

    // ---- EXPIRE / empty-string / SET EX big-int errors ------------------------
    @("valkey.expire.set_getex_errors")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        // EXPIRE with empty string as TTL -> not an integer error
        ks.run("SET", "foo", "bar");
        ks.run("EXPIRE", "foo", "")
            .startsWith("-ERR value is not an integer or out of range").should.equal(true);

        // SET with EX / big & smallest integer -> invalid expire in 'set'
        ks.run("SET", "foo", "bar", "EX", "10000000000000000")
            .startsWith("-ERR invalid expire time in 'set' command").should.equal(true);
        ks.run("SET", "foo", "bar", "EX", "-9999999999999999")
            .startsWith("-ERR invalid expire time in 'set' command").should.equal(true);

        // GETEX with big & smallest integer -> invalid expire in 'getex'
        ks.run("SET", "foo", "bar");
        ks.run("GETEX", "foo", "EX", "10000000000000000")
            .startsWith("-ERR invalid expire time in 'getex' command").should.equal(true);
        ks.run("GETEX", "foo", "EX", "-9999999999999999")
            .startsWith("-ERR invalid expire time in 'getex' command").should.equal(true);

        // GETEX with non-integer expiration time -> value is not an integer
        ks.run("DEL", "foo");
        ks.run("GETEX", "foo", "ex", "abcd")
            .startsWith("-ERR value is not an integer or out of range").should.equal(true);
        ks.run("SET", "foo", "bar");
        ks.run("GETEX", "foo", "px", "-abcd")
            .startsWith("-ERR value is not an integer or out of range").should.equal(true);
        ks.run("DEL", "foo");
        ks.run("HSET", "foo", "f", "v");
        ks.run("GETEX", "foo", "exat", "-abcd")
            .startsWith("-ERR value is not an integer or out of range").should.equal(true);
    }

    // ---- SET removes/keeps TTL; GETEX PERSIST/no-opt --------------------------
    @("valkey.expire.set_keepttl_getex")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        // SET command removes expire
        ks.run("SET", "foo", "bar", "EX", "100");
        ks.run("TTL", "foo").expect.to.equal(":100\r\n");
        ks.run("SET", "foo", "bar");
        ks.run("TTL", "foo").expect.to.equal(":-1\r\n");

        // SET KEEPTTL keeps the TTL
        ks.run("SET", "foo", "bar", "EX", "100");
        ks.run("SET", "foo", "bar2", "KEEPTTL");
        ks.run("TTL", "foo").expect.to.equal(":100\r\n");
        ks.run("GET", "foo").expect.to.equal(bulk("bar2"));

        // GETEX PERSIST removes the TTL (and returns the value)
        ks.run("SET", "foo", "bar", "EX", "100");
        ks.run("GETEX", "foo", "PERSIST").expect.to.equal(bulk("bar"));
        ks.run("TTL", "foo").expect.to.equal(":-1\r\n");

        // GETEX with no option leaves the TTL untouched
        ks.run("SET", "foo", "bar", "EX", "100");
        ks.run("GETEX", "foo").expect.to.equal(bulk("bar"));
        ks.run("TTL", "foo").expect.to.equal(":100\r\n");

        // GETEX EX sets a new TTL
        ks.run("SET", "foo", "bar");
        ks.run("GETEX", "foo", "EX", "100").expect.to.equal(bulk("bar"));
        ks.run("TTL", "foo").expect.to.equal(":100\r\n");

        // GETEX EXAT in the past deletes the key (returns the value then removes it)
        ks.run("SET", "foo", "bar");
        ks.run("GETEX", "foo", "exat", "1").expect.to.equal(bulk("bar"));
        ks.run("EXISTS", "foo").expect.to.equal(":0\r\n");

        // GETEX on a missing key returns nil
        ks.run("DEL", "gone");
        ks.run("GETEX", "gone").expect.to.equal(NIL);
    }

    // ---- EXPIRE NX / XX / GT / LT combinations --------------------------------
    @("valkey.expire.flags")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        // NX on a key with a TTL -> 0 (unchanged)
        ks.run("SET", "foo", "bar", "EX", "100");
        ks.run("EXPIRE", "foo", "200", "NX").expect.to.equal(":0\r\n");
        ks.run("TTL", "foo").expect.to.equal(":100\r\n");

        // NX on a key without a TTL -> 1
        ks.run("SET", "foo", "bar");
        ks.run("EXPIRE", "foo", "200", "NX").expect.to.equal(":1\r\n");
        ks.run("TTL", "foo").expect.to.equal(":200\r\n");

        // XX on a key with a TTL -> 1
        ks.run("SET", "foo", "bar", "EX", "100");
        ks.run("EXPIRE", "foo", "200", "XX").expect.to.equal(":1\r\n");
        ks.run("TTL", "foo").expect.to.equal(":200\r\n");

        // XX on a key without a TTL -> 0
        ks.run("SET", "foo", "bar");
        ks.run("EXPIRE", "foo", "200", "XX").expect.to.equal(":0\r\n");
        ks.run("TTL", "foo").expect.to.equal(":-1\r\n");

        // GT on a key with a lower ttl -> 1 (new > current)
        ks.run("SET", "foo", "bar", "EX", "100");
        ks.run("EXPIRE", "foo", "200", "GT").expect.to.equal(":1\r\n");
        ks.run("TTL", "foo").expect.to.equal(":200\r\n");

        // GT on a key with a higher ttl -> 0 (new < current)
        ks.run("SET", "foo", "bar", "EX", "200");
        ks.run("EXPIRE", "foo", "100", "GT").expect.to.equal(":0\r\n");
        ks.run("TTL", "foo").expect.to.equal(":200\r\n");

        // GT on a key without a ttl -> 0 (no-ttl counts as +inf)
        ks.run("SET", "foo", "bar");
        ks.run("EXPIRE", "foo", "200", "GT").expect.to.equal(":0\r\n");
        ks.run("TTL", "foo").expect.to.equal(":-1\r\n");

        // LT on a key with a higher ttl -> 0 (new > current)
        ks.run("SET", "foo", "bar", "EX", "100");
        ks.run("EXPIRE", "foo", "200", "LT").expect.to.equal(":0\r\n");
        ks.run("TTL", "foo").expect.to.equal(":100\r\n");

        // LT on a key with a lower ttl -> 1
        ks.run("SET", "foo", "bar", "EX", "200");
        ks.run("EXPIRE", "foo", "100", "LT").expect.to.equal(":1\r\n");
        ks.run("TTL", "foo").expect.to.equal(":100\r\n");

        // LT on a key without a ttl -> 1 (no-ttl = +inf, any finite is lower)
        ks.run("SET", "foo", "bar");
        ks.run("EXPIRE", "foo", "100", "LT").expect.to.equal(":1\r\n");
        ks.run("TTL", "foo").expect.to.equal(":100\r\n");

        // LT + XX on a key with a ttl -> 1
        ks.run("SET", "foo", "bar", "EX", "200");
        ks.run("EXPIRE", "foo", "100", "LT", "XX").expect.to.equal(":1\r\n");
        ks.run("TTL", "foo").expect.to.equal(":100\r\n");

        // LT + XX on a key without a ttl -> 0 (XX requires an existing ttl)
        ks.run("SET", "foo", "bar");
        ks.run("EXPIRE", "foo", "200", "LT", "XX").expect.to.equal(":0\r\n");
        ks.run("TTL", "foo").expect.to.equal(":-1\r\n");
    }

    // ---- EXPIRE flag conflict / unsupported-option errors ---------------------
    @("valkey.expire.flag_errors")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        ks.run("SET", "foo", "bar");

        ks.run("EXPIRE", "foo", "200", "LT", "GT")
            .startsWith("-ERR GT and LT options at the same time are not compatible")
            .should.equal(true);
        ks.run("EXPIRE", "foo", "200", "NX", "GT")
            .startsWith("-ERR NX and XX, GT or LT options at the same time are not compatible")
            .should.equal(true);
        ks.run("EXPIRE", "foo", "200", "NX", "LT")
            .startsWith("-ERR NX and XX, GT or LT options at the same time are not compatible")
            .should.equal(true);
        ks.run("EXPIRE", "foo", "200", "NX", "XX")
            .startsWith("-ERR NX and XX, GT or LT options at the same time are not compatible")
            .should.equal(true);

        // unsupported option
        ks.run("EXPIRE", "foo", "200", "AB")
            .startsWith("-ERR Unsupported option AB").should.equal(true);
        ks.run("EXPIRE", "foo", "200", "XX", "AB")
            .startsWith("-ERR Unsupported option AB").should.equal(true);
    }

    // ---- EXPIRE with negative expiry + non-existent-key with options ----------
    @("valkey.expire.negative_and_missing")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        // EXPIRE with negative expiry (via LT) deletes the volatile key -> 1, TTL -2
        ks.run("SET", "foo", "bar", "EX", "100");
        ks.run("EXPIRE", "foo", "-10", "LT").expect.to.equal(":1\r\n");
        ks.run("TTL", "foo").expect.to.equal(":-2\r\n");

        // same on a non-volatile key (LT vs +inf passes -> negative time deletes)
        ks.run("SET", "foo", "bar");
        ks.run("EXPIRE", "foo", "-10", "LT").expect.to.equal(":1\r\n");
        ks.run("TTL", "foo").expect.to.equal(":-2\r\n");

        // EXPIRE with a non-existent key + any option -> 0
        ks.run("DEL", "none");
        ks.run("EXPIRE", "none", "100", "NX").expect.to.equal(":0\r\n");
        ks.run("EXPIRE", "none", "100", "XX").expect.to.equal(":0\r\n");
        ks.run("EXPIRE", "none", "100", "GT").expect.to.equal(":0\r\n");
        ks.run("EXPIRE", "none", "100", "LT").expect.to.equal(":0\r\n");
    }
}
