module tests.valkey_sort_tests;

// Valkey unit/sort.tcl core ported to native in-process UT (see valkey_incr_tests.d
// for rationale + THIRD_PARTY_NOTICES credit). Covers numeric/ALPHA/DESC/LIMIT,
// BY (external weight patterns + nosort), GET (# and external), STORE, and the
// not-a-number error. Fuzz/config/DEBUG cases stay in the blackbox sweep.

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

    private string arrB(string[] items...)
    {
        string r = "*" ~ items.length.to!string ~ "\r\n";
        foreach (it; items)
            r ~= bulk(it);
        return r;
    }

    @("valkey.sort.numeric_alpha_limit")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        // numeric sort (default), DESC, LIMIT
        ks.run("RPUSH", "l", "3", "1", "2", "5", "4");
        ks.run("SORT", "l").expect.to.equal(arrB("1", "2", "3", "4", "5"));
        ks.run("SORT", "l", "DESC").expect.to.equal(arrB("5", "4", "3", "2", "1"));
        ks.run("SORT", "l", "LIMIT", "1", "2").expect.to.equal(arrB("2", "3"));

        // ALPHA sort of strings; without ALPHA a non-number is an error
        ks.run("RPUSH", "al", "banana", "apple", "cherry");
        ks.run("SORT", "al", "ALPHA").expect.to.equal(arrB("apple", "banana", "cherry"));
        ks.run("SORT", "al").startsWith("-ERR").should.equal(true); // not a double

        // BY nosort preserves insertion order (+ LIMIT)
        ks.run("SORT", "l", "BY", "nosort").expect.to.equal(arrB("3", "1", "2", "5", "4"));
        ks.run("SORT", "l", "BY", "nosort", "LIMIT", "0", "2").expect.to.equal(arrB("3", "1"));
    }

    @("valkey.sort.by_get_store")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        // BY external weight patterns: sort ids by weight_<id>
        ks.run("RPUSH", "ids", "1", "2", "3");
        ks.run("MSET", "weight_1", "10", "weight_2", "30", "weight_3", "20");
        ks.run("SORT", "ids", "BY", "weight_*").expect.to.equal(arrB("1", "3", "2")); // 10,20,30

        // GET # returns the sorted elements; GET pattern pulls external values
        ks.run("MSET", "data_1", "one", "data_2", "two", "data_3", "three");
        ks.run("SORT", "ids", "BY", "weight_*", "GET", "#").expect.to.equal(arrB("1", "3", "2"));
        ks.run("SORT", "ids", "BY", "weight_*", "GET", "data_*").expect.to.equal(
                arrB("one", "three", "two"));

        // hash field access via pattern->field
        ks.run("HSET", "h_1", "w", "5");
        ks.run("HSET", "h_2", "w", "1");
        ks.run("HSET", "h_3", "w", "9");
        ks.run("SORT", "ids", "BY", "h_*->w").expect.to.equal(arrB("2", "1", "3")); // 1,5,9

        // STORE writes the result as a list, returns its length
        ks.run("SORT", "ids", "STORE", "dst").expect.to.equal(":3\r\n");
        ks.run("LRANGE", "dst", "0", "-1").expect.to.equal(arrB("1", "2", "3"));
        ks.run("TYPE", "dst").expect.to.equal("+list\r\n");
    }
}
