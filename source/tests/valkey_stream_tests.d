module tests.valkey_stream_tests;

// Valkey unit/type/stream.tcl ported to native in-process UT (see valkey_incr_tests.d
// for rationale + THIRD_PARTY_NOTICES credit). The tcl is read-only spec/oracle; NOTHING
// tcl lands here. Explicit IDs keep replies deterministic; auto-`*` timestamp cases port
// only the parts whose reply is fixed (seq increment, 0-0 rejection). Blocking XREAD,
// consumer groups, DEBUG/AOF-reload, fuzzing and MULTI/EXEC stay out of scope (the
// stream-cgroups sweep + blackbox cover the server layer).

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

    // one XRANGE-style entry: *2 [ id, [f, v, ...] ]
    private string entry(string id, string[] fv...)
    {
        string fields = "*" ~ fv.length.to!string ~ "\r\n";
        foreach (x; fv)
            fields ~= bulk(x);
        return "*2\r\n" ~ bulk(id) ~ fields;
    }

    // XINFO STREAM (non-FULL) replies a flat map: [k1, v1, k2, v2, ...] where keys are
    // bulk strings and values alternate between integers (:N) and bulk strings ($..),
    // ending with the nested first-entry/last-entry arrays. This walks the top-level
    // map, decoding each value as either an integer or a bulk, and returns the value
    // (as a string) for the requested scalar field. Nested-array values terminate the
    // scan (we only query the leading scalar fields).
    private string infoField(ref Keyspace ks, string key, string field)
    {
        auto s = ks.run("XINFO", "STREAM", key);
        if (s.length == 0 || s[0] != '*')
            return "";
        immutable nl = s.indexOf("\r\n");
        immutable n = s[1 .. nl].to!int;
        size_t i = nl + 2;
        bool wantValue = false;
        foreach (elem; 0 .. n)
        {
            if (i >= s.length)
                break;
            immutable tag = s[i];
            immutable ee = s[i .. $].indexOf("\r\n") + i;
            if (tag == '*')
                break; // nested first-entry/last-entry value
            if (tag == ':')
            {
                immutable val = s[i + 1 .. ee];
                i = ee + 2;
                if (wantValue)
                    return val.idup;
                wantValue = false;
                continue;
            }
            // '$' bulk
            immutable ln = s[i + 1 .. ee].to!int;
            i = ee + 2;
            immutable bodyStr = s[i .. i + ln];
            i += ln + 2;
            if (wantValue)
                return bodyStr.idup;
            if (bodyStr == field)
                wantValue = true;
        }
        return "";
    }

    enum NIL = "$-1\r\n";
    enum WRONGTYPE = "-WRONGTYPE";

    // ---- XADD basic add + XLEN + XRANGE fetch ----
    @("valkey.stream.xadd_xrange_xlen")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        ks.run("XADD", "mystream", "1-1", "item", "1", "value", "a").expect.to.equal(bulk("1-1"));
        ks.run("XADD", "mystream", "2-2", "item", "2", "value", "b").expect.to.equal(bulk("2-2"));
        ks.run("XLEN", "mystream").expect.to.equal(":2\r\n");

        ks.run("XRANGE", "mystream", "-", "+").expect.to.equal(
            "*2\r\n" ~ entry("1-1", "item", "1", "value", "a")
                ~ entry("2-2", "item", "2", "value", "b"));
    }

    // ---- XADD wrong number of args ----
    @("valkey.stream.xadd_argcount")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        ks.run("XADD", "mystream").startsWith("-ERR").should.equal(true);
        ks.run("XADD", "mystream", "*").startsWith("-ERR").should.equal(true);
        ks.run("XADD", "mystream", "*", "field").startsWith("-ERR").should.equal(true);
    }

    // ---- XADD ID edge cases: overflow, smaller-than-last, seq overflow, 0-0, 0-* ----
    @("valkey.stream.xadd_id_edges")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        // full-max ID accepted; the next auto id has nowhere to go -> error
        ks.run("XADD", "a", "18446744073709551615-18446744073709551615", "a", "b")
            .expect.to.equal(bulk("18446744073709551615-18446744073709551615"));
        ks.run("XADD", "a", "*", "c", "d").startsWith("-ERR").should.equal(true);

        // auto-seq for a fixed ms is last-seq+1
        ks.run("XADD", "b", "123-456", "item", "1").expect.to.equal(bulk("123-456"));
        ks.run("XADD", "b", "123-*", "item", "2").expect.to.equal(bulk("123-457"));

        // auto-seq is 0 for a strictly-greater ms
        ks.run("XADD", "c", "123-456", "i", "a").expect.to.equal(bulk("123-456"));
        ks.run("XADD", "c", "789-*", "i", "b").expect.to.equal(bulk("789-0"));

        // auto-seq below the last ID's ms is rejected
        ks.run("XADD", "d", "123-456", "i", "a");
        ks.run("XADD", "d", "42-*", "i", "b").startsWith("-ERR").should.equal(true);

        // seq at max then auto-seq -> overflow -> rejected
        ks.run("XADD", "e", "1-18446744073709551615", "a", "b");
        ks.run("XADD", "e", "1-*", "c", "d").startsWith("-ERR").should.equal(true);

        // 0-* first entry gets seq 1
        ks.run("XADD", "f", "0-*", "a", "b").expect.to.equal(bulk("0-1"));

        // 0-0 is not a legal ID; nothing is created
        ks.run("XADD", "g", "0-0", "k", "v").startsWith("-ERR").should.equal(true);
        ks.run("EXISTS", "g").expect.to.equal(":0\r\n");

        // explicit id <= top is rejected
        ks.run("XADD", "h", "5-0", "a", "b");
        ks.run("XADD", "h", "5-0", "a", "b").startsWith("-ERR").should.equal(true);
        ks.run("XADD", "h", "4-0", "a", "b").startsWith("-ERR").should.equal(true);

        // partial ID with maximal seq then partial-auto is rejected
        ks.run("XADD", "i", "1-18446744073709551615", "f1", "v1");
        ks.run("XADD", "i", "1-*", "f2", "v2").startsWith("-ERR").should.equal(true);
    }

    // ---- XADD streamID edge: future ms with max seq, then auto rolls ms ----
    @("valkey.stream.xadd_streamid_edge")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        ks.run("XADD", "x", "2577343934890-18446744073709551615", "f", "v");
        // auto id: ms bumps by one, seq resets to 0
        ks.run("XADD", "x", "*", "f2", "v2").expect.to.equal(bulk("2577343934891-0"));
        ks.run("XRANGE", "x", "-", "+").expect.to.equal(
            "*2\r\n" ~ entry("2577343934890-18446744073709551615", "f", "v")
                ~ entry("2577343934891-0", "f2", "v2"));
    }

    // ---- XADD NOMKSTREAM ----
    @("valkey.stream.xadd_nomkstream")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        // NOMKSTREAM on a missing key returns nil bulk and creates nothing
        ks.run("XADD", "mystream", "NOMKSTREAM", "*", "item", "1").expect.to.equal(NIL);
        ks.run("EXISTS", "mystream").expect.to.equal(":0\r\n");

        // once the stream exists, NOMKSTREAM appends normally
        ks.run("XADD", "mystream", "1-1", "item", "1", "value", "a");
        ks.run("XADD", "mystream", "NOMKSTREAM", "2-2", "item", "2", "value", "b")
            .expect.to.equal(bulk("2-2"));
        ks.run("XLEN", "mystream").expect.to.equal(":2\r\n");
    }

    // ---- XADD MAXLEN 0 creates an empty stream ----
    @("valkey.stream.xadd_maxlen_zero")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        ks.run("XADD", "mystream", "MAXLEN", "0", "*", "a", "b");
        ks.run("XLEN", "mystream").expect.to.equal(":0\r\n");
        ks.infoField("mystream", "length").expect.to.equal("0");
    }

    // ---- XADD MAXLEN keeps newest N ----
    @("valkey.stream.xadd_maxlen_trims")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        foreach (i; 1 .. 11)
            ks.run("XADD", "s", "MAXLEN", "5", i.to!string ~ "-0", "n", i.to!string);
        ks.run("XLEN", "s").expect.to.equal(":5\r\n");
        // the surviving entries are 6..10
        ks.run("XRANGE", "s", "-", "+").expect.to.equal(
            "*5\r\n" ~ entry("6-0", "n", "6") ~ entry("7-0", "n", "7")
                ~ entry("8-0", "n", "8") ~ entry("9-0", "n", "9") ~ entry("10-0", "n", "10"));

        // '=' argument is exact and equivalent
        foreach (i; 1 .. 11)
            ks.run("XADD", "s2", "MAXLEN", "=", "5", i.to!string ~ "-0", "n", i.to!string);
        ks.run("XLEN", "s2").expect.to.equal(":5\r\n");
    }

    // ---- XADD MINID trims entries below the given id ----
    @("valkey.stream.xadd_minid_trims")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        foreach (i; 1 .. 6)
            ks.run("XADD", "s", i.to!string ~ "-0", "f", "v");
        // MINID 3 drops 1-0 and 2-0, then appends 6-0
        ks.run("XADD", "s", "MINID", "3", "6-0", "f", "v").expect.to.equal(bulk("6-0"));
        ks.run("XLEN", "s").expect.to.equal(":4\r\n");
        ks.run("XRANGE", "s", "-", "+").expect.to.equal(
            "*4\r\n" ~ entry("3-0", "f", "v") ~ entry("4-0", "f", "v")
                ~ entry("5-0", "f", "v") ~ entry("6-0", "f", "v"));
    }

    // ---- XADD LIMIT requires the ~ option ----
    @("valkey.stream.xadd_limit_requires_approx")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        // LIMIT without ~ is a syntax error
        ks.run("XADD", "s", "MAXLEN", "5", "LIMIT", "1", "*", "a", "b")
            .startsWith("-ERR").should.equal(true);

        // MAXLEN ~ 0 LIMIT bounds the number of drops. dreads uses an exact count
        // capped by LIMIT (min(len-maxlen, limit)) rather than Valkey's radix-node
        // approximation, so on a 3-entry stream LIMIT 1 drops exactly 1. After the
        // 4th append the stream holds 3.
        foreach (i; 1 .. 4)
            ks.run("XADD", "y", i.to!string ~ "-0", "xitem", "v");
        ks.run("XADD", "y", "MAXLEN", "~", "0", "LIMIT", "1", "4-0", "xitem", "v");
        ks.run("XLEN", "y").expect.to.equal(":3\r\n");
    }

    // ---- XADD wrong type ----
    @("valkey.stream.xadd_wrongtype")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        ks.run("SET", "str", "hello");
        ks.run("XADD", "str", "*", "f", "v").startsWith(WRONGTYPE).should.equal(true);
        ks.run("XLEN", "str").startsWith(WRONGTYPE).should.equal(true);
        ks.run("XRANGE", "str", "-", "+").startsWith(WRONGTYPE).should.equal(true);
    }

    // ---- XLEN / XRANGE / XDEL / XTRIM / XINFO on missing keys ----
    @("valkey.stream.missing_key")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        ks.run("XLEN", "nope").expect.to.equal(":0\r\n");
        ks.run("XRANGE", "nope", "-", "+").expect.to.equal("*0\r\n");
        ks.run("XREVRANGE", "nope", "+", "-").expect.to.equal("*0\r\n");
        ks.run("XDEL", "nope", "1-1").expect.to.equal(":0\r\n");
        ks.run("XTRIM", "nope", "MAXLEN", "5").expect.to.equal(":0\r\n");
        ks.run("XINFO", "STREAM", "nope").startsWith("-ERR").should.equal(true);
    }

    // ---- XRANGE COUNT + exclusive ranges + XREVRANGE ----
    @("valkey.stream.xrange_count_exclusive")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        ks.run("XADD", "s", "1-1", "field1", "value1");
        ks.run("XADD", "s", "2-2", "field2", "value2");
        ks.run("XADD", "s", "3-3", "a", "1", "b", "2");

        // COUNT limits the returned entries
        ks.run("XRANGE", "s", "-", "+", "COUNT", "2").expect.to.equal(
            "*2\r\n" ~ entry("1-1", "field1", "value1")
                ~ entry("2-2", "field2", "value2"));

        // exclusive start '(' skips the boundary entry
        ks.run("XRANGE", "s", "(1-1", "+").expect.to.equal(
            "*2\r\n" ~ entry("2-2", "field2", "value2")
                ~ entry("3-3", "a", "1", "b", "2"));

        // exclusive end
        ks.run("XRANGE", "s", "-", "(3-3").expect.to.equal(
            "*2\r\n" ~ entry("1-1", "field1", "value1")
                ~ entry("2-2", "field2", "value2"));

        // XREVRANGE returns entries in reverse, COUNT applies
        ks.run("XREVRANGE", "s", "+", "-", "COUNT", "1").expect.to.equal(
            "*1\r\n" ~ entry("3-3", "a", "1", "b", "2"));
        ks.run("XREVRANGE", "s", "+", "-").expect.to.equal(
            "*3\r\n" ~ entry("3-3", "a", "1", "b", "2")
                ~ entry("2-2", "field2", "value2") ~ entry("1-1", "field1", "value1"));
    }

    // ---- XRANGE exclusive-boundary edge cases + error cases ----
    @("valkey.stream.xrange_exclusive_edges")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        immutable string[] ids = [
            "0-1", "0-18446744073709551615", "1-0", "42-0", "42-42",
            "18446744073709551615-18446744073709551614",
            "18446744073709551615-18446744073709551615"
        ];
        foreach (id; ids)
            ks.run("XADD", "vip", id, "foo", "bar");

        // all present
        parseCount(ks.run("XRANGE", "vip", "-", "+")).expect.to.equal(7);
        // excluding the first id
        parseCount(ks.run("XRANGE", "vip", "(0-1", "+")).expect.to.equal(6);
        // excluding the last id
        parseCount(ks.run("XRANGE", "vip", "-", "(18446744073709551615-18446744073709551615"))
            .expect.to.equal(6);
        // a single element between two exclusive bounds
        parseCount(ks.run("XRANGE", "vip", "(0-1", "(1-0")).expect.to.equal(1);
        parseCount(ks.run("XRANGE", "vip", "(1-0", "(42-42")).expect.to.equal(1);

        // '(-' and '(+' are invalid stream IDs
        ks.run("XRANGE", "vip", "(-", "+").startsWith("-ERR").should.equal(true);
        ks.run("XRANGE", "vip", "-", "(+").startsWith("-ERR").should.equal(true);
        // excluding beyond the max possible seq
        ks.run("XRANGE", "vip", "(18446744073709551615-18446744073709551615", "+")
            .startsWith("-ERR").should.equal(true);
        // excluding below 0-0
        ks.run("XRANGE", "vip", "-", "(0-0").startsWith("-ERR").should.equal(true);
    }

    private int parseCount(string s)
    {
        if (s.length == 0 || s[0] != '*')
            return -1;
        immutable nl = s.indexOf("\r\n");
        return s[1 .. nl].to!int;
    }

    // ---- XREVRANGE regression: mixed and same-field entries ----
    @("valkey.stream.xrevrange_regression")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        ks.run("XADD", "t", "1234567891230", "key1", "value1");
        ks.run("XADD", "t", "1234567891240", "key2", "value2");
        ks.run("XADD", "t", "1234567891250", "key3", "value3");
        ks.run("XREVRANGE", "t", "1234567891245", "-").expect.to.equal(
            "*2\r\n" ~ entry("1234567891240-0", "key2", "value2")
                ~ entry("1234567891230-0", "key1", "value1"));

        // same-field entries
        ks.run("XADD", "t2", "1234567891230", "key1", "value1");
        ks.run("XADD", "t2", "1234567891240", "key1", "value2");
        ks.run("XADD", "t2", "1234567891250", "key1", "value3");
        ks.run("XREVRANGE", "t2", "1234567891245", "-").expect.to.equal(
            "*2\r\n" ~ entry("1234567891240-0", "key1", "value2")
                ~ entry("1234567891230-0", "key1", "value1"));
    }

    // ---- XDEL basic + multi ----
    @("valkey.stream.xdel")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        ks.run("XADD", "s", "1-0", "foo", "value0");
        ks.run("XADD", "s", "2-0", "foo", "value1");
        ks.run("XADD", "s", "3-0", "foo", "value2");
        // delete the middle one
        ks.run("XDEL", "s", "2-0").expect.to.equal(":1\r\n");
        ks.run("XLEN", "s").expect.to.equal(":2\r\n");
        ks.run("XRANGE", "s", "-", "+").expect.to.equal(
            "*2\r\n" ~ entry("1-0", "foo", "value0") ~ entry("3-0", "foo", "value2"));

        // multiple ids at once, only existing ones count
        ks.run("XADD", "m", "1-1", "a", "1");
        ks.run("XADD", "m", "1-2", "b", "2");
        ks.run("XADD", "m", "1-3", "c", "3");
        ks.run("XADD", "m", "1-4", "d", "4");
        ks.run("XADD", "m", "1-5", "e", "5");
        ks.run("XDEL", "m", "1-1", "1-4", "1-5", "2-1").expect.to.equal(":3\r\n");
        ks.run("XLEN", "m").expect.to.equal(":2\r\n");
        ks.run("XRANGE", "m", "-", "+").expect.to.equal(
            "*2\r\n" ~ entry("1-2", "b", "2") ~ entry("1-3", "c", "3"));
    }

    // ---- XTRIM MAXLEN + MINID ----
    @("valkey.stream.xtrim")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        foreach (i; 1 .. 6)
            ks.run("XADD", "s", i.to!string ~ "-0", "f", "v");

        // MINID = 3-0 removes 1-0 and 2-0 (returns 2)
        ks.run("XTRIM", "s", "MINID", "=", "3-0").expect.to.equal(":2\r\n");
        ks.run("XRANGE", "s", "-", "+").expect.to.equal(
            "*3\r\n" ~ entry("3-0", "f", "v") ~ entry("4-0", "f", "v")
                ~ entry("5-0", "f", "v"));

        // MINID without = also trims (big gap)
        ks.run("XADD", "b", "1-0", "f", "v");
        ks.run("XADD", "b", "1641544570597-0", "f", "v");
        ks.run("XADD", "b", "1641544570597-1", "f", "v");
        ks.run("XTRIM", "b", "MINID", "1641544570597-0").expect.to.equal(":1\r\n");
        ks.run("XRANGE", "b", "-", "+").expect.to.equal(
            "*2\r\n" ~ entry("1641544570597-0", "f", "v")
                ~ entry("1641544570597-1", "f", "v"));

        // MAXLEN keeps the newest N
        foreach (i; 1 .. 11)
            ks.run("XADD", "c", i.to!string ~ "-0", "n", i.to!string);
        ks.run("XTRIM", "c", "MAXLEN", "6").expect.to.equal(":4\r\n");
        ks.run("XLEN", "c").expect.to.equal(":6\r\n");
        ks.run("XTRIM", "c", "MAXLEN", "=", "5").expect.to.equal(":1\r\n");
        ks.run("XLEN", "c").expect.to.equal(":5\r\n");
    }

    // ---- XTRIM LIMIT requires ~ / delete no more than limit ----
    @("valkey.stream.xtrim_limit")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        foreach (i; 1 .. 4)
            ks.run("XADD", "s", i.to!string ~ "-0", "xitem", "v");

        // LIMIT without ~ is a syntax error
        ks.run("XTRIM", "s", "MAXLEN", "1", "LIMIT", "30").startsWith("-ERR").should.equal(true);

        // MAXLEN ~ 0 LIMIT bounds how many get removed. dreads drops
        // min(len-maxlen, limit) exactly: LIMIT 1 drops 1, then LIMIT 3 drops the
        // remaining 2 (not Valkey's node-approximated 0-then-2).
        ks.run("XTRIM", "s", "MAXLEN", "~", "0", "LIMIT", "1").expect.to.equal(":1\r\n");
        ks.run("XTRIM", "s", "MAXLEN", "~", "0", "LIMIT", "3").expect.to.equal(":2\r\n");
    }

    // ---- XREAD ----
    @("valkey.stream.xread")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        ks.run("XADD", "les", "1-0", "k1", "v1");
        ks.run("XADD", "les", "2-0", "k2", "v2");
        ks.run("XADD", "les", "3-0", "k3", "v3");

        // XREAD COUNT 1 from 0-0 returns the first entry, nested one level deeper
        // *1 [ *2 [ key, *1 [ *2 [ id, [f,v] ] ] ] ]
        immutable e10 = "*2\r\n" ~ bulk("1-0") ~ arrB("k1", "v1");
        ks.run("XREAD", "COUNT", "1", "STREAMS", "les", "0-0").expect.to.equal(
            "*1\r\n*2\r\n" ~ bulk("les") ~ "*1\r\n" ~ e10);

        // XREAD STREAMS les + returns only the last entry
        immutable e30 = "*2\r\n" ~ bulk("3-0") ~ arrB("k3", "v3");
        ks.run("XREAD", "STREAMS", "les", "+").expect.to.equal(
            "*1\r\n*2\r\n" ~ bulk("les") ~ "*1\r\n" ~ e30);

        // COUNT > 1 with + still returns only the last element
        ks.run("XREAD", "COUNT", "3", "STREAMS", "les", "+").expect.to.equal(
            "*1\r\n*2\r\n" ~ bulk("les") ~ "*1\r\n" ~ e30);

        // an ID equal to the last returns nothing (nil array in RESP2)
        ks.run("XREAD", "STREAMS", "les", "3-0").expect.to.equal("*-1\r\n");
    }

    // ---- XREAD nil / empty cases ----
    @("valkey.stream.xread_empty")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        // reading a missing stream from 0-0 -> nil array
        ks.run("XREAD", "STREAMS", "nope", "0-0").expect.to.equal("*-1\r\n");
        // + on a missing stream -> nil array
        ks.run("XREAD", "STREAMS", "emp", "+").expect.to.equal("*-1\r\n");

        // add then delete -> still nil for + (last id known but no entries)
        ks.run("XADD", "emp", "1-0", "k1", "v1");
        ks.run("XDEL", "emp", "1-0");
        ks.run("XREAD", "STREAMS", "emp", "+").expect.to.equal("*-1\r\n");
    }

    // ---- XREAD multiple streams (last element) ----
    @("valkey.stream.xread_multi_last")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        ks.run("XADD", "a", "1-0", "k1", "v1");
        ks.run("XADD", "a", "2-0", "k2", "v2");
        ks.run("XADD", "a", "3-0", "k3", "v3");
        ks.run("XADD", "b", "1-0", "k1", "v4");
        ks.run("XADD", "b", "2-0", "k2", "v5");
        ks.run("XADD", "b", "3-0", "k3", "v6");

        // + across two existing streams + one non-existent: only the two return
        immutable ea = "*2\r\n" ~ bulk("3-0") ~ arrB("k3", "v3");
        immutable eb = "*2\r\n" ~ bulk("3-0") ~ arrB("k3", "v6");
        ks.run("XREAD", "STREAMS", "a", "b", "nope", "+", "+", "+").expect.to.equal(
            "*2\r\n"
                ~ "*2\r\n" ~ bulk("a") ~ "*1\r\n" ~ ea
                ~ "*2\r\n" ~ bulk("b") ~ "*1\r\n" ~ eb);
    }

    // ---- XREAD streamID edge (max seq boundary) ----
    @("valkey.stream.xread_streamid_edge")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        ks.run("XADD", "x", "1-1", "f", "v");
        ks.run("XADD", "x", "1-18446744073709551615", "f", "v");
        ks.run("XADD", "x", "2-1", "f", "v");
        // reading after the max-seq id yields only 2-1
        immutable e = "*2\r\n" ~ bulk("2-1") ~ arrB("f", "v");
        ks.run("XREAD", "STREAMS", "x", "1-18446744073709551615").expect.to.equal(
            "*1\r\n*2\r\n" ~ bulk("x") ~ "*1\r\n" ~ e);
    }

    // ---- XSETID ----
    @("valkey.stream.xsetid")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        ks.run("XADD", "m", "MAXLEN", "0", "*", "a", "b");
        // set a specific last-generated-id
        ks.run("XSETID", "m", "200-0").expect.to.equal("+OK\r\n");
        ks.infoField("m", "last-generated-id").expect.to.equal("200-0");

        // cannot set to a smaller ID than the current top item
        ks.run("XADD", "x", "5-0", "a", "b");
        ks.run("XSETID", "x", "1-1").startsWith("-ERR").should.equal(true);

        // no such key
        ks.run("XSETID", "zz", "1-1").startsWith("-ERR").should.equal(true);
    }

    // ---- XSETID with offset + tombstone form + syntax/validation errors ----
    @("valkey.stream.xsetid_full")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        ks.run("XADD", "x", "1-0", "a", "b");

        // offset without a tombstone is a syntax error (old positional form)
        ks.run("XSETID", "x", "1-1", "0").startsWith("-ERR").should.equal(true);
        // a lone tombstone positional is also a syntax error
        ks.run("XSETID", "x", "1-1", "0-0").startsWith("-ERR").should.equal(true);
        // negative entries-added rejected
        ks.run("XSETID", "x", "1-1", "ENTRIESADDED", "-1", "MAXDELETEDID", "0-0")
            .startsWith("-ERR").should.equal(true);
        // entries-added below the stream length is rejected
        ks.run("XSETID", "x", "1-0", "ENTRIESADDED", "0", "MAXDELETEDID", "0-0")
            .startsWith("-ERR").should.equal(true);
        // max-deleted-id larger than the id being set is rejected
        ks.run("XSETID", "x", "1-0", "ENTRIESADDED", "1", "MAXDELETEDID", "2-0")
            .startsWith("-ERR").should.equal(true);

        // valid full form
        ks.run("XSETID", "x", "5-0", "ENTRIESADDED", "10", "MAXDELETEDID", "3-0")
            .expect.to.equal("+OK\r\n");
        ks.infoField("x", "last-generated-id").expect.to.equal("5-0");
        ks.infoField("x", "max-deleted-entry-id").expect.to.equal("3-0");
        ks.infoField("x", "entries-added").expect.to.equal("10");
    }

    // ---- XINFO STREAM: entries-added, recorded-first-entry-id, max-deleted ----
    @("valkey.stream.xinfo_counters")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        ks.run("XADD", "x", "1-0", "data", "a");
        ks.infoField("x", "entries-added").expect.to.equal("1");
        ks.infoField("x", "recorded-first-entry-id").expect.to.equal("1-0");
        ks.infoField("x", "max-deleted-entry-id").expect.to.equal("0-0");

        ks.run("XADD", "x", "2-0", "data", "a");
        ks.infoField("x", "entries-added").expect.to.equal("2");
        // recorded first stays at the true first entry
        ks.infoField("x", "recorded-first-entry-id").expect.to.equal("1-0");

        // deleting a middle entry updates max-deleted but not recorded-first
        ks.run("XADD", "x", "3-0", "data", "c");
        ks.run("XDEL", "x", "2-0");
        ks.infoField("x", "max-deleted-entry-id").expect.to.equal("2-0");
        ks.infoField("x", "recorded-first-entry-id").expect.to.equal("1-0");
        // deleting a lower id keeps max-deleted at the higher deleted id
        ks.run("XDEL", "x", "1-0");
        ks.infoField("x", "max-deleted-entry-id").expect.to.equal("2-0");
        // recorded-first now advances to the surviving first entry
        ks.infoField("x", "recorded-first-entry-id").expect.to.equal("3-0");
    }

    // ---- XINFO STREAM reflects XTRIM on recorded-first ----
    @("valkey.stream.xinfo_recorded_first_after_trim")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        foreach (i; 1 .. 6)
            ks.run("XADD", "x", i.to!string ~ "-0", "data", "a");
        ks.infoField("x", "entries-added").expect.to.equal("5");
        ks.infoField("x", "recorded-first-entry-id").expect.to.equal("1-0");

        // trimming to the newest 2 pushes recorded-first to 4-0
        ks.run("XTRIM", "x", "MAXLEN", "=", "2");
        ks.infoField("x", "recorded-first-entry-id").expect.to.equal("4-0");
    }

    // ---- XSETID cannot set smaller ID than current max-deleted ----
    @("valkey.stream.xsetid_below_maxdeleted")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        ks.run("XADD", "x", "1-1", "a", "1");
        ks.run("XADD", "x", "1-2", "b", "2");
        ks.run("XADD", "x", "1-3", "c", "3");
        ks.run("XDEL", "x", "1-2");
        ks.run("XDEL", "x", "1-3");
        ks.infoField("x", "max-deleted-entry-id").expect.to.equal("1-3");
        // trying to set the last-id below the max-deleted tombstone is rejected
        ks.run("XSETID", "x", "1-2").startsWith("-ERR").should.equal(true);
    }

    // ---- XGROUP / XINFO HELP arg errors ----
    @("valkey.stream.help_argerrors")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        ks.run("XGROUP", "help", "xxx").startsWith("-ERR").should.equal(true);
        ks.run("XINFO", "help", "xxx").startsWith("-ERR").should.equal(true);
    }
}
