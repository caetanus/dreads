module tests.valkey_scan_tests;

// Valkey unit/scan.tcl ported to native in-process UT. SCAN is cursor-based:
// every helper loops until the cursor returns to "0" and compares the collected
// elements as an unordered set (Valkey guarantees full coverage, never order).
//
// Ported: basic/COUNT/MATCH/TYPE full-coverage sweeps, TYPE + MATCH + COUNT
// together, unknown-type-name error, lazy expiry drop during SCAN (and the TYPE
// filter gating that expiry), SSCAN/HSCAN/ZSCAN element/field/score shapes,
// HSCAN NOVALUES, ZSCAN NOSCORES, MATCH patterns on each variant, the int-encoded
// SSCAN #1345 case, the tiny-denormal ZSCAN score #2175 regression, wrong-type and
// arity / syntax error surfaces.
//
// SKIPPED (per porting rules): the `foreach encoding` intset/listpack/hashtable/
// skiplist re-runs (same semantics — ported once), the random SCAN-under-write and
// SREM-rehash fuzz loops, DEBUG SET-ACTIVE-EXPIRE / INFO keyspace introspection,
// and the cluster-slot MATCH optimization (cluster-only). Nothing tcl lands here.
// See valkey_incr_tests.d for the THIRD_PARTY_NOTICES credit rationale.

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

    // Parse a SCAN reply `*2\r\n<cursor bulk><elements array>` -> (cursor, elements).
    private void parseScan(string s, out string cursor, out string[] elems)
    {
        assert(s.length && s[0] == '*'); // *2
        size_t i = s.indexOf("\r\n") + 2;
        assert(s[i] == '$');
        immutable e = s[i .. $].indexOf("\r\n") + i;
        immutable cl = s[i + 1 .. e].to!int;
        i = e + 2;
        cursor = s[i .. i + cl];
        i += cl + 2;
        elems = parseArr(s[i .. $]);
    }

    // Loop `pre ~ [cursor] ~ post` until the cursor returns to "0"; collect elements.
    private string[] scanLoop(ref Keyspace ks, string[] pre, string[] post = [])
    {
        string[] all;
        string cursor = "0";
        do
        {
            auto reply = run(ks, pre ~ cursor ~ post);
            string[] el;
            parseScan(reply, cursor, el);
            all ~= el;
        }
        while (cursor != "0");
        return all;
    }

    private void collectMatches(string[] got, string[] exp...)
    {
        sort(got);
        auto e = exp.dup;
        sort(e);
        got.expect.to.equal(e);
    }

    // ---- SCAN keyspace: basic / COUNT / MATCH / TYPE full coverage -----------
    @("valkey.scan.keyspace_basic")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        // Populate a moderate keyspace across types.
        foreach (i; 0 .. 200)
            ks.run("SET", "key:" ~ i.to!string, "v");
        ks.run("RPUSH", "mylist", "x");
        ks.run("SADD", "myset", "m");

        // {type} SCAN basic — every key reported, dedup, exact count.
        auto all = scanLoop(ks, ["SCAN"]);
        sort(all);
        all.length.expect.to.equal(202);

        // {type} SCAN COUNT — COUNT is a hint; full coverage regardless.
        auto c5 = scanLoop(ks, ["SCAN"], ["COUNT", "5"]);
        sort(c5);
        c5.length.expect.to.equal(202);

        // COUNT 1 (smallest legal) still returns everything.
        auto c1 = scanLoop(ks, ["SCAN"], ["COUNT", "1"]);
        sort(c1);
        c1.length.expect.to.equal(202);

        // A COUNT larger than the keyspace: single pass, cursor 0.
        auto big = scanLoop(ks, ["SCAN"], ["COUNT", "100000"]);
        sort(big);
        big.length.expect.to.equal(202);
    }

    @("valkey.scan.keyspace_match")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        // {type} SCAN MATCH "key:1??" — three-digit 1xx keys only.
        // Populate 0..199 so key:1?? matches key:100..key:199 (100 keys).
        foreach (i; 0 .. 200)
            ks.run("SET", "key:" ~ i.to!string, "v");

        auto m = scanLoop(ks, ["SCAN"], ["MATCH", "key:1??"]);
        sort(m);
        m.length.expect.to.equal(100);
        m[0].expect.to.equal("key:100");
        m[$ - 1].expect.to.equal("key:199");

        // A prefix glob.
        auto pre = scanLoop(ks, ["SCAN"], ["MATCH", "key:19*"]);
        sort(pre);
        // key:19, key:190..key:199 => 11 keys.
        pre.length.expect.to.equal(11);

        // A pattern matching nothing -> empty.
        scanLoop(ks, ["SCAN"], ["MATCH", "nope:*"]).length.expect.to.equal(0);
    }

    @("valkey.scan.keyspace_type")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        // {type} SCAN TYPE — only strings from populate; non-strings excluded.
        foreach (i; 0 .. 50)
            ks.run("SET", "key:" ~ i.to!string, "v");
        ks.run("RPUSH", "l", "a");
        ks.run("SADD", "st", "a");
        ks.run("HSET", "h", "f", "v");
        ks.run("ZADD", "z", "1", "a");

        // TYPE list -> only the one list.
        collectMatches(scanLoop(ks, ["SCAN"], ["TYPE", "list"]), "l");
        // TYPE set -> only the set.
        collectMatches(scanLoop(ks, ["SCAN"], ["TYPE", "set"]), "st");
        // TYPE hash / zset.
        collectMatches(scanLoop(ks, ["SCAN"], ["TYPE", "hash"]), "h");
        collectMatches(scanLoop(ks, ["SCAN"], ["TYPE", "zset"]), "z");

        // TYPE string -> all 50 strings, none of the containers.
        auto strs = scanLoop(ks, ["SCAN"], ["TYPE", "string"]);
        sort(strs);
        strs.length.expect.to.equal(50);

        // All three args together: TYPE string MATCH key:* COUNT 10.
        auto three = scanLoop(ks, ["SCAN"], ["TYPE", "string", "MATCH", "key:*", "COUNT", "10"]);
        sort(three);
        three.length.expect.to.equal(50);

        // TYPE with a MATCH that excludes the strings -> empty.
        scanLoop(ks, ["SCAN"], ["TYPE", "string", "MATCH", "zzz*"]).length.expect.to.equal(0);
    }

    // ---- SCAN error surfaces --------------------------------------------------
    @("valkey.scan.errors")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        ks.run("SET", "foo", "bar");

        // {type} SCAN unknown type — "*unknown type name*".
        ks.run("SCAN", "0", "TYPE", "string1").startsWith("-ERR").should.equal(true);
        ks.run("SCAN", "0", "TYPE", "string1")[0 .. 4].expect.to.equal("-ERR");

        // Arity: SCAN with no cursor.
        ks.run("SCAN").startsWith("-ERR").should.equal(true);

        // Non-numeric cursor -> error.
        ks.run("SCAN", "notacursor").startsWith("-ERR").should.equal(true);

        // Negative cursor -> error.
        ks.run("SCAN", "-1").startsWith("-ERR").should.equal(true);

        // COUNT 0 and negative COUNT are rejected.
        ks.run("SCAN", "0", "COUNT", "0").startsWith("-ERR").should.equal(true);
        ks.run("SCAN", "0", "COUNT", "-1").startsWith("-ERR").should.equal(true);
        ks.run("SCAN", "0", "COUNT", "abc").startsWith("-ERR").should.equal(true);

        // MATCH without an argument -> syntax error.
        ks.run("SCAN", "0", "MATCH").startsWith("-ERR").should.equal(true);

        // A bare unknown option -> syntax error.
        ks.run("SCAN", "0", "bogus").startsWith("-ERR").should.equal(true);
    }

    // ---- SCAN lazy expiry: expired keys are dropped during the scan ----------
    @("valkey.scan.expired_keys")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        // {type} SCAN with expired keys — passive expiration is triggered by SCAN.
        foreach (i; 0 .. 10)
            ks.run("SET", "key:" ~ i.to!string, "v");
        ks.run("SET", "foo", "bar");
        ks.run("PEXPIREAT", "foo", "1"); // absolute past -> already expired
        ks.run("HSET", "hash", "f", "v");
        ks.run("PEXPIREAT", "hash", "1");

        // The scan must not surface the expired keys.
        auto seen = scanLoop(ks, ["SCAN"], ["COUNT", "10"]);
        sort(seen);
        seen.length.expect.to.equal(10);
        foreach (k; seen)
        {
            (k == "foo").expect.to.equal(false);
            (k == "hash").expect.to.equal(false);
        }

        // And they are removed: DBSIZE reflects the 10 survivors.
        ks.run("DBSIZE").expect.to.equal(":10\r\n");
    }

    @("valkey.scan.expired_keys_type_filter")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        // {type} SCAN with expired keys with TYPE filter — the type-check lookup
        // during filtering triggers lazy expiry for EVERY candidate key it inspects,
        // so both the expired string and the expired hash are reaped (oracle-verified).
        foreach (i; 0 .. 10)
            ks.run("SET", "key:" ~ i.to!string, "v");
        ks.run("SET", "foo", "bar");
        ks.run("PEXPIREAT", "foo", "1"); // string, expired -> reaped by TYPE string scan
        ks.run("HSET", "hash", "f", "v");
        ks.run("PEXPIREAT", "hash", "1"); // hash, expired -> NOT touched by TYPE string scan

        auto seen = scanLoop(ks, ["SCAN"], ["TYPE", "string", "COUNT", "10"]);
        sort(seen);
        seen.length.expect.to.equal(10); // the 10 live strings, not expired foo

        // only the 10 live strings remain; both expired keys were reaped by the scan.
        ks.run("DBSIZE").expect.to.equal(":10\r\n");
    }

    // ---- SSCAN ----------------------------------------------------------------
    @("valkey.scan.sscan")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        // {type} SSCAN with encoding (semantics ported once): all members reported.
        foreach (i; 0 .. 100)
            ks.run("SADD", "set", "ele:" ~ i.to!string);
        auto all = scanLoop(ks, ["SSCAN", "set"]);
        sort(all);
        all.length.expect.to.equal(100);

        // {type} SSCAN with PATTERN — foo* over a mixed set.
        ks.run("DEL", "mykey");
        ks.run("SADD", "mykey", "foo", "fab", "fiz", "foobar", "1", "2", "3", "4");
        collectMatches(scanLoop(ks, ["SSCAN", "mykey"], ["MATCH", "foo*", "COUNT", "10000"]),
                "foo", "foobar");

        // {type} SSCAN with integer encoded object (issue #1345).
        ks.run("DEL", "iset");
        ks.run("SADD", "iset", "1", "a");
        collectMatches(scanLoop(ks, ["SSCAN", "iset"], ["MATCH", "*a*", "COUNT", "100"]), "a");
        collectMatches(scanLoop(ks, ["SSCAN", "iset"], ["MATCH", "*1*", "COUNT", "100"]), "1");

        // Missing key -> empty cursor-0 reply, no error.
        ks.run("SSCAN", "nope", "0").expect.to.equal("*2\r\n$1\r\n0\r\n*0\r\n");

        // Wrong type.
        ks.run("SET", "str", "v");
        ks.run("SSCAN", "str", "0").startsWith("-WRONGTYPE").should.equal(true);

        // Arity.
        ks.run("SSCAN").startsWith("-ERR").should.equal(true);
        ks.run("SSCAN", "onlykey").startsWith("-ERR").should.equal(true);
    }

    // ---- HSCAN ----------------------------------------------------------------
    @("valkey.scan.hscan")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        // {type} HSCAN with encoding (semantics ported once): flat [f,v,...] and
        // each field maps to its own value.
        foreach (i; 0 .. 30)
            ks.run("HSET", "hash", "key:" ~ i.to!string, i.to!string);
        auto flat = scanLoop(ks, ["HSCAN", "hash"]);
        flat.length.expect.to.equal(60); // 30 field/value pairs
        // Verify each key:N maps to value N.
        for (size_t i = 0; i + 1 < flat.length; i += 2)
        {
            auto f = flat[i], v = flat[i + 1];
            ("key:" ~ v).expect.to.equal(f);
        }

        // {type} HSCAN NOVALUES — fields only.
        auto fields = scanLoop(ks, ["HSCAN", "hash"], ["NOVALUES", "COUNT", "1000"]);
        sort(fields);
        fields.length.expect.to.equal(30);

        // {type} HSCAN with PATTERN — foo* returns field+value pairs whose field matches.
        ks.run("DEL", "mykey");
        ks.run("HSET", "mykey", "foo", "1", "fab", "2", "fiz", "3",
                "foobar", "10", "1", "a", "2", "b", "3", "c", "4", "d");
        collectMatches(scanLoop(ks, ["HSCAN", "mykey"], ["MATCH", "foo*", "COUNT", "10000"]),
                "foo", "1", "foobar", "10");

        // {type} HSCAN with NOVALUES over the mixed hash: every field, no values.
        collectMatches(scanLoop(ks, ["HSCAN", "mykey"], ["NOVALUES"]),
                "1", "2", "3", "4", "fab", "fiz", "foo", "foobar");

        // Missing key -> empty.
        ks.run("HSCAN", "nope", "0").expect.to.equal("*2\r\n$1\r\n0\r\n*0\r\n");

        // Wrong type.
        ks.run("SET", "str", "v");
        ks.run("HSCAN", "str", "0").startsWith("-WRONGTYPE").should.equal(true);

        // NOSCORES is not an HSCAN option -> error.
        ks.run("HSCAN", "mykey", "0", "NOSCORES").startsWith("-ERR").should.equal(true);

        // Arity.
        ks.run("HSCAN").startsWith("-ERR").should.equal(true);
        ks.run("HSCAN", "onlykey").startsWith("-ERR").should.equal(true);
    }

    // ---- ZSCAN ----------------------------------------------------------------
    @("valkey.scan.zscan")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        // {type} ZSCAN with encoding (semantics ported once): flat [member,score,...]
        // and each member key:N carries score N.
        foreach (i; 0 .. 30)
            ks.run("ZADD", "zset", i.to!string, "key:" ~ i.to!string);
        auto flat = scanLoop(ks, ["ZSCAN", "zset"]);
        flat.length.expect.to.equal(60);
        for (size_t i = 0; i + 1 < flat.length; i += 2)
        {
            auto m = flat[i], s = flat[i + 1];
            ("key:" ~ s).expect.to.equal(m);
        }

        // {type} ZSCAN with NOSCORES — members only.
        auto members = scanLoop(ks, ["ZSCAN", "zset"], ["NOSCORES", "COUNT", "1000"]);
        sort(members);
        members.length.expect.to.equal(30);

        // {type} ZSCAN with PATTERN — foo* member glob.
        ks.run("DEL", "mykey");
        ks.run("ZADD", "mykey", "1", "foo", "2", "fab", "3", "fiz", "10", "foobar");
        collectMatches(scanLoop(ks, ["ZSCAN", "mykey"], ["MATCH", "foo*", "COUNT", "10000"]),
                "foo", "1", "foobar", "10");

        // {type} ZSCAN with NOSCORES over the mixed zset: every member, no score.
        collectMatches(scanLoop(ks, ["ZSCAN", "mykey"], ["NOSCORES"]),
                "fab", "fiz", "foo", "foobar");

        // Score formatting: integral score prints without a decimal point.
        ks.run("DEL", "sc");
        ks.run("ZADD", "sc", "1", "a", "2.5", "b");
        collectMatches(scanLoop(ks, ["ZSCAN", "sc"]), "a", "1", "b", "2.5");

        // Missing key -> empty.
        ks.run("ZSCAN", "nope", "0").expect.to.equal("*2\r\n$1\r\n0\r\n*0\r\n");

        // Wrong type.
        ks.run("SET", "str", "v");
        ks.run("ZSCAN", "str", "0").startsWith("-WRONGTYPE").should.equal(true);

        // NOVALUES is not a ZSCAN option -> error.
        ks.run("ZSCAN", "mykey", "0", "NOVALUES").startsWith("-ERR").should.equal(true);

        // Arity.
        ks.run("ZSCAN").startsWith("-ERR").should.equal(true);
        ks.run("ZSCAN", "onlykey").startsWith("-ERR").should.equal(true);
    }

    // ---- ZSCAN tiny-denormal score regression (issue #2175) ------------------
    @("valkey.scan.zscan_tiny_score")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        // {type} ZSCAN scores: regression test for issue #2175 — a denormalised
        // score must round-trip as a non-zero number, not collapse to "0".
        ks.run("ZADD", "mykey", "9.8813129168249309e-323", "m0");
        auto flat = scanLoop(ks, ["ZSCAN", "mykey"]);
        flat.length.expect.to.equal(2);
        flat[0].expect.to.equal("m0");
        // The score text parses back to a strictly-positive double.
        (flat[1].to!double > 0.0).expect.to.equal(true);
        (flat[1] != "0").expect.to.equal(true);
    }
}
