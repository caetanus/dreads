module tests.valkey_list_tests;

// Valkey unit/type/list.tcl ported to native in-process unittests (see
// valkey_incr_tests.d for rationale + THIRD_PARTY_NOTICES credit). The tcl is a
// read-only spec (which scenarios exist) + oracle (expected reply bytes, grounded
// against valkey-server). Nothing tcl ends up here.
//
// Ported: every deterministic, in-process-runnable command-behavior case — LPUSH/
// RPUSH/LPUSHX/RPUSHX, LPOP/RPOP (+count), LMPOP, LLEN, LINDEX, LSET, LRANGE,
// LINSERT, LREM, LTRIM, LPOS (RANK/COUNT/MAXLEN combos), RPOPLPUSH, LMOVE, and every
// error/edge (missing key -> nil, WRONGTYPE, out-of-range, count/syntax errors).
//
// Skipped (per porting rules): encoding-conversion duplicates (listpack<->quicklist
// via config — semantic ported once); fuzz/random loops; server-only DEBUG
// RELOAD/quicklist-packed-threshold/set-active-expire/memory-usage/OBJECT ENCODING;
// blocking (BLPOP/BRPOP/BLMOVE/BLMPOP/BRPOPLPUSH — its own UT + event-driven suite);
// MULTI/EXEC, WATCH, CLIENT, replication-stream, SWAPDB-wake, keyspace notifications,
// RESP2/3 hello handshake, DUMP/RESTORE round-trips.

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
    enum NILARR = "*-1\r\n";

    // ---- LPUSH, RPUSH, LLEN, LINDEX, LPOP/RPOP basics ---------------------
    @("valkey.list.push_llen_lindex_pop")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        // first lpush then rpush; head..tail order, returns new length
        ks.run("LPUSH", "mylist1", "a").expect.to.equal(":1\r\n");
        ks.run("RPUSH", "mylist1", "b").expect.to.equal(":2\r\n");
        ks.run("RPUSH", "mylist1", "c").expect.to.equal(":3\r\n");
        ks.run("LLEN", "mylist1").expect.to.equal(":3\r\n");
        ks.run("LINDEX", "mylist1", "0").expect.to.equal(bulk("a"));
        ks.run("LINDEX", "mylist1", "1").expect.to.equal(bulk("b"));
        ks.run("LINDEX", "mylist1", "2").expect.to.equal(bulk("c"));
        ks.run("LINDEX", "mylist1", "3").expect.to.equal(NIL); // out of range -> nil
        ks.run("RPOP", "mylist1").expect.to.equal(bulk("c"));
        ks.run("LPOP", "mylist1").expect.to.equal(bulk("a"));

        // first rpush then lpush
        ks.run("RPUSH", "mylist2", "a").expect.to.equal(":1\r\n");
        ks.run("LPUSH", "mylist2", "b").expect.to.equal(":2\r\n");
        ks.run("LPUSH", "mylist2", "c").expect.to.equal(":3\r\n");
        ks.run("LINDEX", "mylist2", "0").expect.to.equal(bulk("c"));
        ks.run("LINDEX", "mylist2", "1").expect.to.equal(bulk("b"));
        ks.run("LINDEX", "mylist2", "2").expect.to.equal(bulk("a"));
        ks.run("LINDEX", "mylist2", "-1").expect.to.equal(bulk("a"));
        ks.run("RPOP", "mylist2").expect.to.equal(bulk("a"));
        ks.run("LPOP", "mylist2").expect.to.equal(bulk("c"));
    }

    // ---- Variadic RPUSH/LPUSH ---------------------------------------------
    @("valkey.list.variadic_push")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        ks.run("LPUSH", "mylist", "a", "b", "c", "d").expect.to.equal(":4\r\n");
        ks.run("RPUSH", "mylist", "0", "1", "2", "3").expect.to.equal(":8\r\n");
        ks.run("LRANGE", "mylist", "0", "-1").expect.to.equal(
            arrB("d", "c", "b", "a", "0", "1", "2", "3"));
    }

    // ---- DEL a list -------------------------------------------------------
    @("valkey.list.del")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        ks.run("RPUSH", "mylist2", "a");
        ks.run("DEL", "mylist2").expect.to.equal(":1\r\n");
        ks.run("EXISTS", "mylist2").expect.to.equal(":0\r\n");
        ks.run("LLEN", "mylist2").expect.to.equal(":0\r\n");
    }

    // ---- LPOP/RPOP with the optional count argument -----------------------
    @("valkey.list.pop_count")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        // aa bb cc dd ee ff gg after: lpush listcount aa bb cc dd ee ff gg
        ks.run("LPUSH", "listcount", "aa", "bb", "cc", "dd", "ee", "ff", "gg")
            .expect.to.equal(":7\r\n");
        // head is gg
        ks.run("LPOP", "listcount", "1").expect.to.equal(arrB("gg"));
        ks.run("LPOP", "listcount", "2").expect.to.equal(arrB("ff", "ee"));
        ks.run("RPOP", "listcount", "2").expect.to.equal(arrB("aa", "bb"));
        ks.run("RPOP", "listcount", "1").expect.to.equal(arrB("cc"));
        // count larger than remaining returns what's left
        ks.run("RPOP", "listcount", "123").expect.to.equal(arrB("dd"));
        // negative count -> error
        ks.run("LPOP", "forbarqaz", "-123").startsWith("-ERR").should.equal(true);
    }

    // ---- LPOP/RPOP wrong number of arguments ------------------------------
    @("valkey.list.pop_wrong_args")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        auto e1 = ks.run("LPOP", "key", "1", "1");
        e1.startsWith("-ERR").should.equal(true);
        (e1.indexOf("wrong number of arguments") >= 0).should.equal(true);
        auto e2 = ks.run("RPOP", "key", "2", "2");
        e2.startsWith("-ERR").should.equal(true);
        (e2.indexOf("wrong number of arguments") >= 0).should.equal(true);
    }

    // ---- LPOP/RPOP count 0 -> empty array; nonexistent -> nil -------------
    @("valkey.list.pop_count_zero_and_missing")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        ks.run("LPUSH", "listcount", "zero");
        ks.run("LPOP", "listcount", "0").expect.to.equal("*0\r\n");
        ks.run("RPOP", "listcount", "0").expect.to.equal("*0\r\n");

        // non existing key: no-count -> nil bulk; with count -> nil array
        ks.run("LPOP", "non_existing_key").expect.to.equal(NIL);
        ks.run("RPOP", "non_existing_key").expect.to.equal(NIL);
        ks.run("LPOP", "non_existing_key", "0").expect.to.equal(NILARR);
        ks.run("LPOP", "non_existing_key", "1").expect.to.equal(NILARR);
        ks.run("RPOP", "non_existing_key", "0").expect.to.equal(NILARR);
        ks.run("RPOP", "non_existing_key", "1").expect.to.equal(NILARR);
    }

    // ---- LPUSHX / RPUSHX --------------------------------------------------
    @("valkey.list.pushx")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        // generic: on non-existing key -> 0, no key created
        ks.run("LPUSHX", "xlist", "a").expect.to.equal(":0\r\n");
        ks.run("LLEN", "xlist").expect.to.equal(":0\r\n");
        ks.run("RPUSHX", "xlist", "a").expect.to.equal(":0\r\n");
        ks.run("LLEN", "xlist").expect.to.equal(":0\r\n");
        ks.run("EXISTS", "xlist").expect.to.equal(":0\r\n");

        // once it exists, PUSHX appends/prepends variadically
        ks.run("RPUSH", "xlist", "b", "c").expect.to.equal(":2\r\n");
        ks.run("RPUSHX", "xlist", "d").expect.to.equal(":3\r\n");
        ks.run("LPUSHX", "xlist", "a").expect.to.equal(":4\r\n");
        ks.run("RPUSHX", "xlist", "42", "x").expect.to.equal(":6\r\n");
        ks.run("LPUSHX", "xlist", "y3", "y2", "y1").expect.to.equal(":9\r\n");
        ks.run("LRANGE", "xlist", "0", "-1").expect.to.equal(
            arrB("y1", "y2", "y3", "a", "b", "c", "d", "42", "x"));
    }

    // ---- LINSERT ----------------------------------------------------------
    @("valkey.list.linsert")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        ks.run("RPUSH", "xlist", "a", "c", "d");
        ks.run("LINSERT", "xlist", "before", "c", "zz").expect.to.equal(":4\r\n");
        ks.run("LRANGE", "xlist", "0", "10").expect.to.equal(arrB("a", "zz", "c", "d"));
        ks.run("LINSERT", "xlist", "after", "c", "yy").expect.to.equal(":5\r\n");
        ks.run("LRANGE", "xlist", "0", "10").expect.to.equal(arrB("a", "zz", "c", "yy", "d"));
        ks.run("LINSERT", "xlist", "after", "d", "dd").expect.to.equal(":6\r\n");
        // pivot not found -> -1
        ks.run("LINSERT", "xlist", "after", "bad", "ddd").expect.to.equal(":-1\r\n");
        ks.run("LRANGE", "xlist", "0", "10").expect.to.equal(arrB("a", "zz", "c", "yy", "d", "dd"));
        ks.run("LINSERT", "xlist", "before", "a", "aa").expect.to.equal(":7\r\n");
        ks.run("LINSERT", "xlist", "before", "bad", "aaa").expect.to.equal(":-1\r\n");
        ks.run("LRANGE", "xlist", "0", "10").expect.to.equal(
            arrB("aa", "a", "zz", "c", "yy", "d", "dd"));
        // integer-encoded value inserts fine
        ks.run("LINSERT", "xlist", "before", "aa", "42").expect.to.equal(":8\r\n");
        ks.run("LRANGE", "xlist", "0", "0").expect.to.equal(arrB("42"));
    }

    @("valkey.list.linsert_errors")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        ks.run("RPUSH", "xlist", "a", "b");
        // bad where keyword -> syntax error
        auto e = ks.run("LINSERT", "xlist", "aft3r", "aa", "42");
        e.startsWith("-ERR").should.equal(true);
        (e.indexOf("syntax error") >= 0).should.equal(true);

        // against non-list value -> WRONGTYPE
        ks.run("SET", "k1", "v1");
        ks.run("LINSERT", "k1", "after", "0", "0").startsWith("-WRONGTYPE").should.equal(true);

        // against non existing key -> 0
        ks.run("LINSERT", "not-a-key", "before", "0", "0").expect.to.equal(":0\r\n");
    }

    // ---- LLEN / LINDEX / LPUSH / RPUSH wrong-type + missing ---------------
    @("valkey.list.type_errors")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        ks.run("SET", "mylist", "foobar");
        ks.run("LLEN", "mylist").startsWith("-WRONGTYPE").should.equal(true);
        ks.run("LINDEX", "mylist", "0").startsWith("-WRONGTYPE").should.equal(true);
        ks.run("LPUSH", "mylist", "0").startsWith("-WRONGTYPE").should.equal(true);
        ks.run("RPUSH", "mylist", "0").startsWith("-WRONGTYPE").should.equal(true);

        // non existing key: LLEN -> 0, LINDEX -> nil
        ks.run("LLEN", "not-a-key").expect.to.equal(":0\r\n");
        ks.run("LINDEX", "not-a-key", "10").expect.to.equal(NIL);
    }

    // ---- LPOS -------------------------------------------------------------
    @("valkey.list.lpos_basic_rank_count")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        // a b c LARGE 2 3 c c  (index:  0 1 2 3   4 5 6 7)
        ks.run("RPUSH", "mylist", "a", "b", "c", "LARGE", "2", "3", "c", "c");
        ks.run("LPOS", "mylist", "a").expect.to.equal(":0\r\n");
        ks.run("LPOS", "mylist", "c").expect.to.equal(":2\r\n");

        // RANK positive / negative / beyond matches
        ks.run("LPOS", "mylist", "c", "RANK", "1").expect.to.equal(":2\r\n");
        ks.run("LPOS", "mylist", "c", "RANK", "2").expect.to.equal(":6\r\n");
        ks.run("LPOS", "mylist", "c", "RANK", "4").expect.to.equal(NIL); // beyond matches
        ks.run("LPOS", "mylist", "c", "RANK", "-1").expect.to.equal(":7\r\n");
        ks.run("LPOS", "mylist", "c", "RANK", "-2").expect.to.equal(":6\r\n");

        // RANK 0 -> error
        auto e0 = ks.run("LPOS", "mylist", "c", "RANK", "0");
        e0.startsWith("-ERR").should.equal(true);
        (e0.indexOf("RANK can't be zero") >= 0).should.equal(true);
        // RANK out of range (LLONG_MIN) -> error
        auto er = ks.run("LPOS", "mylist", "c", "RANK", "-9223372036854775808");
        er.startsWith("-ERR").should.equal(true);
        (er.indexOf("out of range") >= 0).should.equal(true);

        // COUNT
        ks.run("LPOS", "mylist", "c", "COUNT", "0").expect.to.equal("*3\r\n:2\r\n:6\r\n:7\r\n");
        ks.run("LPOS", "mylist", "c", "COUNT", "1").expect.to.equal("*1\r\n:2\r\n");
        ks.run("LPOS", "mylist", "c", "COUNT", "2").expect.to.equal("*2\r\n:2\r\n:6\r\n");
        ks.run("LPOS", "mylist", "c", "COUNT", "100").expect.to.equal("*3\r\n:2\r\n:6\r\n:7\r\n");

        // COUNT + RANK
        ks.run("LPOS", "mylist", "c", "COUNT", "0", "RANK", "2").expect.to.equal(
            "*2\r\n:6\r\n:7\r\n");
        ks.run("LPOS", "mylist", "c", "COUNT", "2", "RANK", "-1").expect.to.equal(
            "*2\r\n:7\r\n:6\r\n");
    }

    @("valkey.list.lpos_missing_nomatch_maxlen")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        ks.run("RPUSH", "mylist", "a", "b", "c", "LARGE", "2", "3", "c", "c");

        // non existing key with COUNT -> empty array
        ks.run("LPOS", "mylistxxx", "c", "COUNT", "0", "RANK", "2").expect.to.equal("*0\r\n");

        // no match: with COUNT -> empty array; without COUNT -> nil
        ks.run("LPOS", "mylist", "x", "COUNT", "2", "RANK", "-1").expect.to.equal("*0\r\n");
        ks.run("LPOS", "mylist", "x", "RANK", "-1").expect.to.equal(NIL);

        // MAXLEN limits the scan window
        ks.run("LPOS", "mylist", "a", "COUNT", "0", "MAXLEN", "1").expect.to.equal("*1\r\n:0\r\n");
        ks.run("LPOS", "mylist", "c", "COUNT", "0", "MAXLEN", "1").expect.to.equal("*0\r\n");
        ks.run("LPOS", "mylist", "c", "COUNT", "0", "MAXLEN", "3").expect.to.equal("*1\r\n:2\r\n");
        ks.run("LPOS", "mylist", "c", "COUNT", "0", "MAXLEN", "3", "RANK", "-1")
            .expect.to.equal("*2\r\n:7\r\n:6\r\n");
        ks.run("LPOS", "mylist", "c", "COUNT", "0", "MAXLEN", "7", "RANK", "2")
            .expect.to.equal("*1\r\n:6\r\n");

        // RANK greater than matches -> empty array (with COUNT)
        ks.run("DEL", "mylist");
        ks.run("LPUSH", "mylist", "a");
        ks.run("LPOS", "mylist", "b", "COUNT", "10", "RANK", "5").expect.to.equal("*0\r\n");
    }

    // ---- LMPOP ------------------------------------------------------------
    @("valkey.list.lmpop_single")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        // reply shape: *2\r\n<key>\r\n*<N>\r\n<elems...>
        ks.run("RPUSH", "mylist", "a", "b", "LARGE", "d", "e", "f");
        // same key multiple times; left count 2
        ks.run("LMPOP", "2", "mylist", "mylist", "left", "count", "2")
            .expect.to.equal("*2\r\n" ~ bulk("mylist") ~ arrB("a", "b"));
        ks.run("LMPOP", "2", "mylist", "mylist", "right", "count", "2")
            .expect.to.equal("*2\r\n" ~ bulk("mylist") ~ arrB("f", "e"));
        ks.run("LLEN", "mylist").expect.to.equal(":2\r\n");

        // first exists, second missing
        ks.run("DEL", "mylist");
        ks.run("RPUSH", "mylist", "a", "b", "LARGE", "d", "e");
        ks.run("DEL", "mylist2");
        ks.run("LMPOP", "2", "mylist", "mylist2", "left", "count", "1")
            .expect.to.equal("*2\r\n" ~ bulk("mylist") ~ arrB("a"));
        ks.run("LLEN", "mylist").expect.to.equal(":4\r\n");
        ks.run("LMPOP", "2", "mylist", "mylist2", "right", "count", "10")
            .expect.to.equal("*2\r\n" ~ bulk("mylist") ~ arrB("e", "d", "LARGE", "b"));
        // now both empty -> nil array
        ks.run("LMPOP", "2", "mylist", "mylist2", "right", "count", "1").expect.to.equal(NILARR);

        // first missing, second exists
        ks.run("DEL", "mylist");
        ks.run("RPUSH", "mylist2", "1", "2", "LARGE", "4", "5");
        ks.run("LMPOP", "2", "mylist", "mylist2", "right", "count", "1")
            .expect.to.equal("*2\r\n" ~ bulk("mylist2") ~ arrB("5"));
        ks.run("LLEN", "mylist2").expect.to.equal(":4\r\n");
        ks.run("LMPOP", "2", "mylist", "mylist2", "left", "count", "10")
            .expect.to.equal("*2\r\n" ~ bulk("mylist2") ~ arrB("1", "2", "LARGE", "4"));
        ks.run("EXISTS", "mylist", "mylist2").expect.to.equal(":0\r\n");
    }

    @("valkey.list.lmpop_multi_and_empty")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        ks.run("RPUSH", "mylist", "a", "b", "LARGE", "d", "e");
        ks.run("RPUSH", "mylist2", "1", "2", "LARGE", "4", "5");

        // pops from the FIRST non-empty key
        ks.run("LMPOP", "2", "mylist", "mylist2", "left", "count", "2")
            .expect.to.equal("*2\r\n" ~ bulk("mylist") ~ arrB("a", "b"));
        ks.run("LLEN", "mylist").expect.to.equal(":3\r\n");
        ks.run("LMPOP", "2", "mylist", "mylist2", "right", "count", "3")
            .expect.to.equal("*2\r\n" ~ bulk("mylist") ~ arrB("e", "d", "LARGE"));
        ks.run("EXISTS", "mylist").expect.to.equal(":0\r\n");
        // first now empty -> pop from second
        ks.run("LMPOP", "2", "mylist", "mylist2", "left", "count", "3")
            .expect.to.equal("*2\r\n" ~ bulk("mylist2") ~ arrB("1", "2", "LARGE"));
        ks.run("LLEN", "mylist2").expect.to.equal(":2\r\n");
        ks.run("LMPOP", "2", "mylist", "mylist2", "right", "count", "2")
            .expect.to.equal("*2\r\n" ~ bulk("mylist2") ~ arrB("5", "4"));

        // LMPOP against all-empty / missing keys -> nil array
        ks.run("DEL", "ne1", "ne2");
        ks.run("LMPOP", "1", "ne1", "left", "count", "1").expect.to.equal(NILARR);
        ks.run("LMPOP", "1", "ne1", "left", "count", "10").expect.to.equal(NILARR);
        ks.run("LMPOP", "2", "ne1", "ne2", "right", "count", "1").expect.to.equal(NILARR);
        ks.run("LMPOP", "2", "ne1", "ne2", "right", "count", "10").expect.to.equal(NILARR);
    }

    @("valkey.list.lmpop_errors")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        // wrong number of arguments
        ks.run("LMPOP").startsWith("-ERR").should.equal(true);
        ks.run("LMPOP", "1").startsWith("-ERR").should.equal(true);
        ks.run("LMPOP", "1", "mylist").startsWith("-ERR").should.equal(true);

        // numkeys errors
        auto n0 = ks.run("LMPOP", "0", "mylist", "LEFT");
        n0.startsWith("-ERR").should.equal(true);
        (n0.indexOf("numkeys") >= 0).should.equal(true);
        ks.run("LMPOP", "a", "mylist", "LEFT").startsWith("-ERR").should.equal(true);
        ks.run("LMPOP", "-1", "mylist", "RIGHT").startsWith("-ERR").should.equal(true);

        // syntax errors
        ks.run("LMPOP", "1", "mylist", "bad_where").startsWith("-ERR").should.equal(true);
        ks.run("LMPOP", "1", "mylist", "LEFT", "bar_arg").startsWith("-ERR").should.equal(true);
        ks.run("LMPOP", "1", "mylist", "RIGHT", "LEFT").startsWith("-ERR").should.equal(true);
        ks.run("LMPOP", "1", "mylist", "COUNT").startsWith("-ERR").should.equal(true);
        ks.run("LMPOP", "1", "mylist", "LEFT", "COUNT", "1", "COUNT", "2")
            .startsWith("-ERR").should.equal(true);
        ks.run("LMPOP", "2", "mylist", "mylist2", "bad_arg").startsWith("-ERR").should.equal(true);

        // count errors
        auto c0 = ks.run("LMPOP", "1", "mylist", "LEFT", "COUNT", "0");
        c0.startsWith("-ERR").should.equal(true);
        (c0.indexOf("count") >= 0).should.equal(true);
        ks.run("LMPOP", "1", "mylist", "RIGHT", "COUNT", "a").startsWith("-ERR").should.equal(true);
        ks.run("LMPOP", "1", "mylist", "LEFT", "COUNT", "-1").startsWith("-ERR").should.equal(true);
        ks.run("LMPOP", "2", "mylist", "mylist2", "RIGHT", "COUNT", "-1")
            .startsWith("-ERR").should.equal(true);
    }

    // ---- LPOP/RPOP/LMPOP against wrong type / empty -----------------------
    @("valkey.list.pop_empty_and_wrongtype")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        // against empty list -> nil bulk / nil array
        ks.run("LPOP", "non-existing-list").expect.to.equal(NIL);
        ks.run("RPOP", "non-existing-list2").expect.to.equal(NIL);
        ks.run("LMPOP", "1", "non-existing-list", "left", "count", "1").expect.to.equal(NILARR);
        ks.run("LMPOP", "1", "non-existing-list", "left", "count", "10").expect.to.equal(NILARR);
        ks.run("LMPOP", "2", "non-existing-list", "non-existing-list2", "right", "count", "1")
            .expect.to.equal(NILARR);

        // against a non-list value -> WRONGTYPE
        ks.run("SET", "notalist", "foo");
        ks.run("LPOP", "notalist").startsWith("-WRONGTYPE").should.equal(true);
        ks.run("RPOP", "notalist").startsWith("-WRONGTYPE").should.equal(true);

        ks.run("SET", "notalist2", "nolist");
        ks.run("LMPOP", "2", "notalist", "notalist2", "left", "count", "1")
            .startsWith("-WRONGTYPE").should.equal(true);
        ks.run("LMPOP", "2", "notalist", "notalist2", "right", "count", "10")
            .startsWith("-WRONGTYPE").should.equal(true);
    }

    // ---- Basic LPOP/RPOP/LMPOP mixed --------------------------------------
    @("valkey.list.basic_pop_lmpop")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        ks.run("RPUSH", "mylist", "LARGE", "1", "2");
        ks.run("LPOP", "mylist").expect.to.equal(bulk("LARGE"));
        ks.run("RPOP", "mylist").expect.to.equal(bulk("2"));
        ks.run("LPOP", "mylist").expect.to.equal(bulk("1"));
        ks.run("LLEN", "mylist").expect.to.equal(":0\r\n");

        ks.run("RPUSH", "mylist", "LARGE", "1", "2");
        ks.run("LMPOP", "1", "mylist", "left", "count", "1")
            .expect.to.equal("*2\r\n" ~ bulk("mylist") ~ arrB("LARGE"));
        ks.run("LMPOP", "2", "mylist", "mylist", "right", "count", "2")
            .expect.to.equal("*2\r\n" ~ bulk("mylist") ~ arrB("2", "1"));
    }

    // ---- LRANGE -----------------------------------------------------------
    @("valkey.list.lrange")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        // LARGE 1 2 3 4 5 6 7 8 9
        ks.run("RPUSH", "mylist", "LARGE", "1", "2", "3", "4", "5", "6", "7", "8", "9");
        ks.run("LRANGE", "mylist", "1", "-2").expect.to.equal(
            arrB("1", "2", "3", "4", "5", "6", "7", "8"));
        ks.run("LRANGE", "mylist", "-3", "-1").expect.to.equal(arrB("7", "8", "9"));
        ks.run("LRANGE", "mylist", "4", "4").expect.to.equal(arrB("4"));
        // inverted indexes -> empty
        ks.run("LRANGE", "mylist", "6", "2").expect.to.equal("*0\r\n");

        // out of range including full list
        ks.run("DEL", "mylist");
        ks.run("RPUSH", "mylist", "LARGE", "1", "2", "3");
        ks.run("LRANGE", "mylist", "-1000", "1000").expect.to.equal(arrB("LARGE", "1", "2", "3"));
        // out of range negative end
        ks.run("LRANGE", "mylist", "0", "-4").expect.to.equal(arrB("LARGE"));
        ks.run("LRANGE", "mylist", "0", "-5").expect.to.equal("*0\r\n");

        // non existing key
        ks.run("LRANGE", "nosuchkey", "0", "1").expect.to.equal("*0\r\n");

        // start > end -> empty (backward compat)
        ks.run("DEL", "mylist");
        ks.run("RPUSH", "mylist", "1", "LARGE", "3");
        ks.run("LRANGE", "mylist", "1", "0").expect.to.equal("*0\r\n");
        ks.run("LRANGE", "mylist", "-1", "-2").expect.to.equal("*0\r\n");
    }

    // ---- LTRIM ------------------------------------------------------------
    @("valkey.list.ltrim")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        // helper via re-create each assert: 1 2 3 4 LARGE
        void mk()
        {
            ks.run("DEL", "mylist");
            ks.run("RPUSH", "mylist", "1", "2", "3", "4", "LARGE");
        }

        mk();
        ks.run("LTRIM", "mylist", "0", "0").expect.to.equal("+OK\r\n");
        ks.run("LRANGE", "mylist", "0", "-1").expect.to.equal(arrB("1"));
        mk();
        ks.run("LTRIM", "mylist", "0", "1");
        ks.run("LRANGE", "mylist", "0", "-1").expect.to.equal(arrB("1", "2"));
        mk();
        ks.run("LTRIM", "mylist", "0", "2");
        ks.run("LRANGE", "mylist", "0", "-1").expect.to.equal(arrB("1", "2", "3"));
        mk();
        ks.run("LTRIM", "mylist", "1", "2");
        ks.run("LRANGE", "mylist", "0", "-1").expect.to.equal(arrB("2", "3"));
        mk();
        ks.run("LTRIM", "mylist", "1", "-1");
        ks.run("LRANGE", "mylist", "0", "-1").expect.to.equal(arrB("2", "3", "4", "LARGE"));
        mk();
        ks.run("LTRIM", "mylist", "1", "-2");
        ks.run("LRANGE", "mylist", "0", "-1").expect.to.equal(arrB("2", "3", "4"));
        mk();
        ks.run("LTRIM", "mylist", "-2", "-1");
        ks.run("LRANGE", "mylist", "0", "-1").expect.to.equal(arrB("4", "LARGE"));
        mk();
        ks.run("LTRIM", "mylist", "-1", "-1");
        ks.run("LRANGE", "mylist", "0", "-1").expect.to.equal(arrB("LARGE"));
        mk();
        ks.run("LTRIM", "mylist", "-5", "-1");
        ks.run("LRANGE", "mylist", "0", "-1").expect.to.equal(arrB("1", "2", "3", "4", "LARGE"));
        mk();
        ks.run("LTRIM", "mylist", "-10", "10");
        ks.run("LRANGE", "mylist", "0", "-1").expect.to.equal(arrB("1", "2", "3", "4", "LARGE"));
        mk();
        ks.run("LTRIM", "mylist", "0", "5");
        ks.run("LRANGE", "mylist", "0", "-1").expect.to.equal(arrB("1", "2", "3", "4", "LARGE"));

        // out of range negative end
        mk();
        ks.run("LTRIM", "mylist", "0", "-5");
        ks.run("LRANGE", "mylist", "0", "-1").expect.to.equal(arrB("1"));
        mk();
        ks.run("LTRIM", "mylist", "0", "-6");
        ks.run("LRANGE", "mylist", "0", "-1").expect.to.equal("*0\r\n"); // whole list emptied
    }

    // ---- LSET -------------------------------------------------------------
    @("valkey.list.lset")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        // 99 98 LARGE 96 95
        ks.run("RPUSH", "mylist", "99", "98", "LARGE", "96", "95");
        ks.run("LSET", "mylist", "1", "foo").expect.to.equal("+OK\r\n");
        ks.run("LSET", "mylist", "-1", "bar").expect.to.equal("+OK\r\n");
        ks.run("LRANGE", "mylist", "0", "-1").expect.to.equal(
            arrB("99", "foo", "LARGE", "96", "bar"));

        // out of range index -> error
        ks.run("LSET", "mylist", "10", "foo").startsWith("-ERR").should.equal(true);

        // against non existing key -> error (no such key)
        auto nk = ks.run("LSET", "nosuchkey", "10", "foo");
        nk.startsWith("-ERR").should.equal(true);
        (nk.indexOf("no such key") >= 0).should.equal(true);

        // against non list value -> WRONGTYPE
        ks.run("SET", "nolist", "foobar");
        ks.run("LSET", "nolist", "0", "foo").startsWith("-WRONGTYPE").should.equal(true);
    }

    // ---- LREM -------------------------------------------------------------
    @("valkey.list.lrem")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        // remove all occurrences (count 0)
        ks.run("RPUSH", "mylist", "E", "foo", "bar", "foobar", "foobared", "zap", "bar", "test", "foo");
        ks.run("LREM", "mylist", "0", "bar").expect.to.equal(":2\r\n");
        ks.run("LRANGE", "mylist", "0", "-1").expect.to.equal(
            arrB("E", "foo", "foobar", "foobared", "zap", "test", "foo"));

        // remove first occurrence (count 1)
        ks.run("LREM", "mylist", "1", "foo").expect.to.equal(":1\r\n");
        ks.run("LRANGE", "mylist", "0", "-1").expect.to.equal(
            arrB("E", "foobar", "foobared", "zap", "test", "foo"));

        // remove non existing element -> 0, list unchanged
        ks.run("LREM", "mylist", "1", "nosuchelement").expect.to.equal(":0\r\n");
        ks.run("LRANGE", "mylist", "0", "-1").expect.to.equal(
            arrB("E", "foobar", "foobared", "zap", "test", "foo"));

        // from tail with negative count
        ks.run("DEL", "mylist");
        ks.run("RPUSH", "mylist", "E", "foo", "bar", "foobar", "foobared", "zap", "bar", "test", "foo", "foo");
        ks.run("LREM", "mylist", "-1", "bar").expect.to.equal(":1\r\n");
        ks.run("LRANGE", "mylist", "0", "-1").expect.to.equal(
            arrB("E", "foo", "bar", "foobar", "foobared", "zap", "test", "foo", "foo"));
        ks.run("LREM", "mylist", "-2", "foo").expect.to.equal(":2\r\n");
        ks.run("LRANGE", "mylist", "0", "-1").expect.to.equal(
            arrB("E", "foo", "bar", "foobar", "foobared", "zap", "test"));

        // deleting int-encoded objects
        ks.run("DEL", "myotherlist");
        ks.run("RPUSH", "myotherlist", "E", "1", "2", "3");
        ks.run("LREM", "myotherlist", "1", "2").expect.to.equal(":1\r\n");
        ks.run("LLEN", "myotherlist").expect.to.equal(":3\r\n");
    }

    // ---- RPOPLPUSH --------------------------------------------------------
    @("valkey.list.rpoplpush")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        // base case: tail of src -> head of dst
        ks.run("RPUSH", "mylist1", "a", "LARGE", "c", "d");
        ks.run("RPOPLPUSH", "mylist1", "mylist2").expect.to.equal(bulk("d"));
        ks.run("RPOPLPUSH", "mylist1", "mylist2").expect.to.equal(bulk("c"));
        ks.run("RPOPLPUSH", "mylist1", "mylist2").expect.to.equal(bulk("LARGE"));
        ks.run("LRANGE", "mylist1", "0", "-1").expect.to.equal(arrB("a"));
        ks.run("LRANGE", "mylist2", "0", "-1").expect.to.equal(arrB("LARGE", "c", "d"));

        // same list as src and dst (rotate tail to head)
        ks.run("DEL", "mylist");
        ks.run("RPUSH", "mylist", "a", "LARGE", "c");
        ks.run("RPOPLPUSH", "mylist", "mylist").expect.to.equal(bulk("c"));
        ks.run("LRANGE", "mylist", "0", "-1").expect.to.equal(arrB("c", "a", "LARGE"));

        // against non existing key -> nil, no keys created
        ks.run("DEL", "srclist", "dstlist");
        ks.run("RPOPLPUSH", "srclist", "dstlist").expect.to.equal(NIL);
        ks.run("EXISTS", "srclist").expect.to.equal(":0\r\n");
        ks.run("EXISTS", "dstlist").expect.to.equal(":0\r\n");

        // non list src -> WRONGTYPE, src stays string
        ks.run("SET", "srclist", "x");
        ks.run("RPOPLPUSH", "srclist", "dstlist").startsWith("-WRONGTYPE").should.equal(true);
        ks.run("EXISTS", "newlist").expect.to.equal(":0\r\n");

        // non list dst -> WRONGTYPE, src unchanged
        ks.run("DEL", "srclist2");
        ks.run("RPUSH", "srclist2", "a", "LARGE", "c", "d");
        ks.run("SET", "dstlist2", "x");
        ks.run("RPOPLPUSH", "srclist2", "dstlist2").startsWith("-WRONGTYPE").should.equal(true);
        ks.run("LRANGE", "srclist2", "0", "-1").expect.to.equal(arrB("a", "LARGE", "c", "d"));
    }

    // ---- LMOVE ------------------------------------------------------------
    @("valkey.list.lmove")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        // LEFT RIGHT: pop head of src, push tail of dst
        ks.run("RPUSH", "mylist1", "a", "LARGE", "c", "d");
        ks.run("LMOVE", "mylist1", "mylist2", "LEFT", "RIGHT").expect.to.equal(bulk("a"));
        ks.run("LMOVE", "mylist1", "mylist2", "LEFT", "RIGHT").expect.to.equal(bulk("LARGE"));
        ks.run("LRANGE", "mylist1", "0", "-1").expect.to.equal(arrB("c", "d"));
        ks.run("LRANGE", "mylist2", "0", "-1").expect.to.equal(arrB("a", "LARGE"));

        // RIGHT LEFT: pop tail of src, push head of dst
        ks.run("DEL", "s", "d");
        ks.run("RPUSH", "s", "a", "b", "c");
        ks.run("LMOVE", "s", "d", "RIGHT", "LEFT").expect.to.equal(bulk("c"));
        ks.run("LMOVE", "s", "d", "RIGHT", "LEFT").expect.to.equal(bulk("b"));
        ks.run("LRANGE", "d", "0", "-1").expect.to.equal(arrB("b", "c"));

        // same list as src and dst; RIGHT LEFT rotates tail to head
        ks.run("DEL", "m");
        ks.run("RPUSH", "m", "a", "LARGE", "c");
        ks.run("LMOVE", "m", "m", "RIGHT", "LEFT").expect.to.equal(bulk("c"));
        ks.run("LRANGE", "m", "0", "-1").expect.to.equal(arrB("c", "a", "LARGE"));
        // RIGHT RIGHT is a no-op on the same list
        ks.run("DEL", "m");
        ks.run("RPUSH", "m", "a", "LARGE", "c");
        ks.run("LMOVE", "m", "m", "RIGHT", "RIGHT").expect.to.equal(bulk("c"));
        ks.run("LRANGE", "m", "0", "-1").expect.to.equal(arrB("a", "LARGE", "c"));

        // LMOVE with non-existent source -> nil
        ks.run("DEL", "nosrc", "somedst");
        ks.run("LMOVE", "nosrc", "somedst", "LEFT", "RIGHT").expect.to.equal(NIL);
    }
}
