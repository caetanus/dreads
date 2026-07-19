module tests.valkey_sort_tests;

// Valkey unit/sort.tcl ported to native in-process UT (see valkey_incr_tests.d
// for rationale + THIRD_PARTY_NOTICES credit for the read-only reference). The
// tcl is a spec/oracle only; nothing tcl lands in the repo. Reply bytes below
// were ground-truthed against valkey-server 7521.
//
// Covers: numeric/ALPHA/DESC/LIMIT, BY external weight keys, BY hash field
// (k*->f), BY nosort (list insertion order + zset native order), GET # / GET
// external / GET hash / GET const-nil, GET-pattern-ending-with-`->` (stays a
// key), STORE (list creation, aliasing, empty/out-of-range removal, GET
// projections), sub-lex tiebreak, not-a-double errors, wrong-type, missing key,
// SORT_RO STORE rejection + read path, huge LIMIT offset, and COMMAND GETKEYS.
//
// Skipped per orchestrator rules: encoding-conversion duplicates (foreach
// encoding — semantics ported once), fuzzing/create_random_dataset, DEBUG
// OBJECT/RELOAD, MULTI/EXEC + eval (per-connection/script), cluster-mode BY/GET
// denial (server/cluster layer), and the config-driven quicklist STORE case.

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

    // -----------------------------------------------------------------------
    // Numeric (default), ALPHA, DESC, LIMIT
    // -----------------------------------------------------------------------
    @("valkey.sort.numeric_alpha_desc_limit")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        // default numeric ascending
        ks.run("RPUSH", "l", "3", "1", "2", "5", "4");
        ks.run("SORT", "l").expect.to.equal(arrB("1", "2", "3", "4", "5"));
        // DESC (numeric)
        ks.run("SORT", "l", "DESC").expect.to.equal(arrB("5", "4", "3", "2", "1"));
        // LIMIT off count
        ks.run("SORT", "l", "LIMIT", "1", "2").expect.to.equal(arrB("2", "3"));
        // LIMIT with a negative offset clamps to 0
        ks.run("SORT", "l", "LIMIT", "-10", "100").expect.to.equal(
                arrB("1", "2", "3", "4", "5"));
        // LIMIT beyond the end -> empty
        ks.run("SORT", "l", "LIMIT", "5", "10").expect.to.equal("*0\r\n");

        // ALPHA against integer-encoded strings (tcl: {1 10 2 3})
        ks.run("DEL", "mylist");
        ks.run("RPUSH", "mylist", "2", "1", "3", "10");
        ks.run("SORT", "mylist", "ALPHA").expect.to.equal(arrB("1", "10", "2", "3"));

        // ALPHA of plain strings
        ks.run("RPUSH", "al", "banana", "apple", "cherry");
        ks.run("SORT", "al", "ALPHA").expect.to.equal(arrB("apple", "banana", "cherry"));
        // without ALPHA a non-number is an error
        ks.run("SORT", "al").startsWith(
                "-ERR One or more scores can't be converted into double").should.equal(true);
    }

    // -----------------------------------------------------------------------
    // Floating point sort (tcl issue #19)
    // -----------------------------------------------------------------------
    @("valkey.sort.floats")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        ks.run("RPUSH", "mylist", "1.1", "5.10", "3.10", "7.44", "2.1",
                "5.75", "6.12", "0.25", "1.15");
        // sorted by real value; the original textual forms are preserved
        ks.run("SORT", "mylist").expect.to.equal(
                arrB("0.25", "1.1", "1.15", "2.1", "3.10", "5.10", "5.75", "6.12", "7.44"));
    }

    // -----------------------------------------------------------------------
    // BY external weight keys, BY hash field, GET projections
    // -----------------------------------------------------------------------
    @("valkey.sort.by_get")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        ks.run("RPUSH", "ids", "1", "2", "3");
        // weight_1=30, weight_2=10, weight_3=20  -> order 2,3,1
        ks.run("MSET", "weight_1", "30", "weight_2", "10", "weight_3", "20");
        ks.run("SORT", "ids", "BY", "weight_*").expect.to.equal(arrB("2", "3", "1"));
        // BY key with LIMIT (tcl "SORT BY key with limit")
        ks.run("SORT", "ids", "BY", "weight_*", "LIMIT", "1", "2").expect.to.equal(
                arrB("3", "1"));

        // BY hash field pattern k*->f (same weights)
        ks.run("HSET", "wobj_1", "weight", "30");
        ks.run("HSET", "wobj_2", "weight", "10");
        ks.run("HSET", "wobj_3", "weight", "20");
        ks.run("SORT", "ids", "BY", "wobj_*->weight").expect.to.equal(arrB("2", "3", "1"));
        ks.run("SORT", "ids", "BY", "wobj_*->weight", "LIMIT", "1", "2").expect.to.equal(
                arrB("3", "1"));

        // GET # returns the element itself, in sorted order
        ks.run("SORT", "ids", "BY", "weight_*", "GET", "#").expect.to.equal(
                arrB("2", "3", "1"));
        // GET external pattern pulls the weight values, in the sorted order
        ks.run("SORT", "ids", "BY", "weight_*", "GET", "weight_*").expect.to.equal(
                arrB("10", "20", "30"));
        // GET # then GET weight_* interleaves element + external per row
        ks.run("SORT", "ids", "BY", "weight_*", "GET", "#", "GET", "weight_*").expect.to.equal(
                arrB("2", "10", "3", "20", "1", "30"));
        // GET hash field projection
        ks.run("SORT", "ids", "BY", "weight_*", "GET", "wobj_*->weight").expect.to.equal(
                arrB("10", "20", "30"));
    }

    // -----------------------------------------------------------------------
    // GET # numeric sort; GET <const> yields all-nil array; GET missing -> nil
    // -----------------------------------------------------------------------
    @("valkey.sort.get_hash_const")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        // GET # with numeric sort
        ks.run("RPUSH", "tosort", "3", "1", "2", "10");
        ks.run("SORT", "tosort", "GET", "#").expect.to.equal(arrB("1", "2", "3", "10"));

        // GET <const> (no '*') never matches -> one nil per element (tcl: 16 nils)
        ks.run("DEL", "foo");
        ks.run("SORT", "tosort", "GET", "foo").expect.to.equal("*4\r\n" ~ NIL ~ NIL ~ NIL ~ NIL);

        // GET missing external key -> nil for that element
        ks.run("MSET", "d_1", "one", "d_3", "three"); // d_2, d_10 missing
        ks.run("SORT", "tosort", "GET", "d_*").expect.to.equal(
                "*4\r\n" ~ bulk("one") ~ NIL ~ bulk("three") ~ NIL);
    }

    // -----------------------------------------------------------------------
    // GET pattern ending with just `->` stays a plain key (no hash field)
    // -----------------------------------------------------------------------
    @("valkey.sort.get_trailing_arrow")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        ks.run("RPUSH", "mylist", "a");
        // key literally named "x:a->"
        ks.run("SET", "x:a->", "100");
        // pattern "x:*->" substitutes to "x:a->" and is used as the whole key
        ks.run("SORT", "mylist", "BY", "num", "GET", "x:*->").expect.to.equal(arrB("100"));

        // conversely, a plain "x:a" key does NOT satisfy the trailing-arrow pattern
        ks.run("DEL", "m2");
        ks.run("RPUSH", "m2", "a");
        ks.run("SET", "x2:a", "100");
        ks.run("SORT", "m2", "BY", "nosort", "GET", "x2:*->").expect.to.equal("*1\r\n" ~ NIL);
    }

    // -----------------------------------------------------------------------
    // BY nosort on lists (retains native / insertion order) + LIMIT + STORE
    // -----------------------------------------------------------------------
    @("valkey.sort.nosort_list")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        // lpush 2 1 4 3 5  -> stored head-first: 5 3 4 1 2
        ks.run("LPUSH", "testa", "2", "1", "4", "3", "5");
        ks.run("SORT", "testa", "BY", "nosort").expect.to.equal(arrB("5", "3", "4", "1", "2"));

        // nosort + STORE retains native order
        ks.run("SORT", "testa", "BY", "nosort", "STORE", "testb").expect.to.equal(":5\r\n");
        ks.run("LRANGE", "testb", "0", "-1").expect.to.equal(arrB("5", "3", "4", "1", "2"));

        // nosort + LIMIT + STORE keeps native order of the window
        ks.run("SORT", "testa", "BY", "nosort", "LIMIT", "0", "3", "STORE", "testb").expect.to.equal(
                ":3\r\n");
        ks.run("LRANGE", "testb", "0", "-1").expect.to.equal(arrB("5", "3", "4"));
    }

    // -----------------------------------------------------------------------
    // Sorted-set source: ALPHA DESC, BY nosort native order + LIMIT variants
    // -----------------------------------------------------------------------
    @("valkey.sort.zset")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        ks.run("ZADD", "zset", "1", "a", "5", "b", "2", "c", "10", "d", "3", "e");
        // native score order is a c e b d
        ks.run("SORT", "zset", "ALPHA", "DESC").expect.to.equal(arrB("e", "d", "c", "b", "a"));

        // BY nosort retains the zset (score) ordering; DESC reverses it
        ks.run("SORT", "zset", "BY", "nosort", "ASC").expect.to.equal(
                arrB("a", "c", "e", "b", "d"));
        ks.run("SORT", "zset", "BY", "nosort", "DESC").expect.to.equal(
                arrB("d", "b", "e", "c", "a"));

        // BY nosort + LIMIT
        ks.run("SORT", "zset", "BY", "nosort", "ASC", "LIMIT", "0", "1").expect.to.equal(
                arrB("a"));
        ks.run("SORT", "zset", "BY", "nosort", "DESC", "LIMIT", "0", "1").expect.to.equal(
                arrB("d"));
        ks.run("SORT", "zset", "BY", "nosort", "ASC", "LIMIT", "0", "2").expect.to.equal(
                arrB("a", "c"));
        ks.run("SORT", "zset", "BY", "nosort", "DESC", "LIMIT", "0", "2").expect.to.equal(
                arrB("d", "b"));
        // window past the end -> empty
        ks.run("SORT", "zset", "BY", "nosort", "LIMIT", "5", "10").expect.to.equal("*0\r\n");
        // negative offset clamps to start
        ks.run("SORT", "zset", "BY", "nosort", "LIMIT", "-10", "100").expect.to.equal(
                arrB("a", "c", "e", "b", "d"));
    }

    // -----------------------------------------------------------------------
    // Set source: BY <const> STORE forces a deterministic alpha sort; BY
    // <const> alpha ordering; sub-lex tiebreak when scores tie
    // -----------------------------------------------------------------------
    @("valkey.sort.set_by_const_sublex")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        immutable string[] members = [
            "a", "b", "c", "d", "e", "f", "g", "h", "i", "l", "m", "n", "o",
            "p", "q", "r", "s", "t", "u", "v", "z", "aa", "aaa", "azz"
        ];
        immutable string[] alphaOrder = [
            "a", "aa", "aaa", "azz", "b", "c", "d", "e", "f", "g", "h", "i",
            "l", "m", "n", "o", "p", "q", "r", "s", "t", "u", "v", "z"
        ];

        string[] sadd = ["SADD", "myset"];
        foreach (m; members)
            sadd ~= m;
        ks.run(sadd);

        // BY <const> ALPHA STORE: dreads forces alpha (replay-stable) for sets
        ks.run("SORT", "myset", "ALPHA", "BY", "_", "STORE", "mylist").expect.to.equal(":24\r\n");
        string[] wantLrange = ["LRANGE", "mylist", "0", "-1"];
        ks.run(wantLrange).expect.to.equal(arrB(cast(string[]) alphaOrder));

        // BY score:* where every score is equal -> lexicographic tiebreak
        foreach (m; members)
            ks.run("SET", "score:" ~ m, "100");
        ks.run("SORT", "myset", "BY", "score:*").expect.to.equal(arrB(cast(string[]) alphaOrder));
    }

    // -----------------------------------------------------------------------
    // STORE behaviors: type list, length reply, empty/out-of-range removal
    // -----------------------------------------------------------------------
    @("valkey.sort.store")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        // basic STORE returns the length, writes a list
        ks.run("RPUSH", "ids", "3", "1", "2");
        ks.run("SORT", "ids", "STORE", "dst").expect.to.equal(":3\r\n");
        ks.run("LRANGE", "dst", "0", "-1").expect.to.equal(arrB("1", "2", "3"));
        ks.run("TYPE", "dst").expect.to.equal("+list\r\n");

        // STORE of an empty source returns 0 (tcl github issue 224)
        ks.run("DEL", "foo", "bar");
        ks.run("SORT", "foo", "STORE", "bar").expect.to.equal(":0\r\n");

        // STORE does not create an empty list when LIMIT skips everything (issue 224)
        ks.run("LPUSH", "src", "bar");
        ks.run("SORT", "src", "ALPHA", "LIMIT", "10", "10", "STORE", "zap").expect.to.equal(
                ":0\r\n");
        ks.run("EXISTS", "zap").expect.to.equal(":0\r\n");

        // STORE removes an existing dst when the result is empty (issue 227)
        ks.run("LPUSH", "existing", "x"); // pre-populate the dst key
        ks.run("SORT", "emptylist", "STORE", "existing").expect.to.equal(":0\r\n");
        ks.run("EXISTS", "existing").expect.to.equal(":0\r\n");

        // STORE with GET projections stores each projection (nil -> "")
        ks.run("DEL", "ids2");
        ks.run("RPUSH", "ids2", "1", "2");
        ks.run("MSET", "g_1", "one"); // g_2 missing -> ""
        ks.run("SORT", "ids2", "GET", "g_*", "STORE", "gdst").expect.to.equal(":2\r\n");
        ks.run("LRANGE", "gdst", "0", "-1").expect.to.equal(arrB("one", ""));
    }

    // -----------------------------------------------------------------------
    // Error / edge cases
    // -----------------------------------------------------------------------
    @("valkey.sort.errors_edges")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        // not-a-double, plain numeric sort (tcl "bad doubles (1)")
        ks.run("SADD", "myset", "1", "2", "3", "4", "not-a-double");
        ks.run("SORT", "myset").startsWith(
                "-ERR One or more scores can't be converted into double").should.equal(true);

        // not-a-double via BY weight key (tcl "bad doubles (2)")
        ks.run("SADD", "ns", "1", "2", "3", "4");
        ks.run("MSET", "score:1", "10", "score:2", "20", "score:3", "30", "score:4",
                "not-a-double");
        ks.run("SORT", "ns", "BY", "score:*").startsWith(
                "-ERR One or more scores can't be converted into double").should.equal(true);

        // wrong type
        ks.run("SET", "strk", "val");
        ks.run("SORT", "strk").startsWith("-WRONGTYPE").should.equal(true);

        // missing key -> empty array
        ks.run("SORT", "nokeyhere").expect.to.equal("*0\r\n");
        // empty list (after all elements removed) -> empty array
        ks.run("RPUSH", "el", "x");
        ks.run("LPOP", "el");
        ks.run("SORT", "el").expect.to.equal("*0\r\n");

        // SORT_RO cannot take STORE (tcl)
        ks.run("SORT_RO", "foolist", "STORE", "bar").expect.to.equal("-ERR syntax error\r\n");
    }

    // -----------------------------------------------------------------------
    // SORT_RO read path + huge LIMIT offset (tcl "SETRANGE with huge offset")
    // -----------------------------------------------------------------------
    @("valkey.sort.sort_ro")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        // SORT_RO with BY nosort GET (trailing-arrow key does not resolve x:a)
        ks.run("LPUSH", "mylist", "a");
        ks.run("SET", "x:a", "100");
        ks.run("SORT_RO", "mylist", "BY", "nosort", "GET", "x:*->").expect.to.equal("*1\r\n" ~ NIL);

        // huge LIMIT offset+count -> a valid (possibly clamped) result, not a crash
        ks.run("DEL", "L");
        ks.run("LPUSH", "L", "2", "1", "0");
        // BY 'a' (no '*') => nosort; offset 2 keeps the last native element only
        ks.run("SORT_RO", "L", "BY", "a", "LIMIT", "2", "9223372036854775807").expect.to.equal(
                arrB("2"));
        ks.run("SORT_RO", "L", "BY", "a", "LIMIT", "2", "2147483647").expect.to.equal(
                arrB("2"));
    }

    // -----------------------------------------------------------------------
    // COMMAND GETKEYS for SORT / SORT_RO (source + last STORE dst)
    // -----------------------------------------------------------------------
    @("valkey.sort.getkeys")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        // SORT abc STORE def -> {abc def}
        ks.run("COMMAND", "GETKEYS", "sort", "abc", "store", "def").expect.to.equal(
                arrB("abc", "def"));
        // SORT_RO abc -> {abc}
        ks.run("COMMAND", "GETKEYS", "sort_ro", "abc").expect.to.equal(arrB("abc"));
        // last STORE wins
        ks.run("COMMAND", "GETKEYS", "sort", "abc", "store", "invalid", "store",
                "stillbad", "store", "def").expect.to.equal(arrB("abc", "def"));
    }
}
