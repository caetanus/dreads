module tests.valkey_hash_tests;

// Valkey unit/type/hash.tcl core ops ported to native in-process UT (see
// valkey_incr_tests.d for rationale + THIRD_PARTY_NOTICES credit). HGETALL/HKEYS/
// HVALS are UNORDERED -> compared as sorted multisets. Field-TTL (HEXPIRE family) has
// its own coverage; listpack/hashtable encodings are the small-container UTs.

version (unittest)
{
    import fluent.asserts;
    import std.conv : to;
    import std.string : indexOf, startsWith;
    import std.algorithm : sort;

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
            assert(s[i] == '$');
            immutable e = s[i .. $].indexOf("\r\n") + i;
            immutable len = s[i + 1 .. e].to!int;
            i = e + 2;
            r ~= s[i .. i + len];
            i += len + 2;
        }
        return r;
    }

    private void sameSet(string reply, string[] expected...)
    {
        auto got = parseArr(reply);
        sort(got);
        auto exp = expected.dup;
        sort(exp);
        got.expect.to.equal(exp);
    }

    // HGETALL flat array [f1,v1,f2,v2,...] compared as an unordered set of "f=v"
    private void sameHash(string reply, string[] fv...)
    {
        auto flat = parseArr(reply);
        string[] got;
        for (size_t i = 0; i + 1 < flat.length; i += 2)
            got ~= flat[i] ~ "=" ~ flat[i + 1];
        sort(got);
        string[] exp;
        for (size_t i = 0; i + 1 < fv.length; i += 2)
            exp ~= fv[i] ~ "=" ~ fv[i + 1];
        sort(exp);
        got.expect.to.equal(exp);
    }

    @("valkey.hash.set_get_del")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        // HSET returns count of NEW fields
        ks.run("HSET", "h", "f1", "a", "f2", "b").expect.to.equal(":2\r\n");
        ks.run("HSET", "h", "f1", "x", "f3", "c").expect.to.equal(":1\r\n"); // only f3 new
        ks.run("HGET", "h", "f1").expect.to.equal(bulk("x"));
        ks.run("HGET", "h", "missing").expect.to.equal("$-1\r\n");
        ks.run("HLEN", "h").expect.to.equal(":3\r\n");

        // HMGET: values in the requested order, nil for absent
        ks.run("HMGET", "h", "f1", "missing", "f2").expect.to.equal(
                "*3\r\n" ~ bulk("x") ~ "$-1\r\n" ~ bulk("b"));

        // HEXISTS / HSTRLEN
        ks.run("HEXISTS", "h", "f2").expect.to.equal(":1\r\n");
        ks.run("HEXISTS", "h", "zzz").expect.to.equal(":0\r\n");
        ks.run("HSTRLEN", "h", "f3").expect.to.equal(":1\r\n");
        ks.run("HSTRLEN", "h", "zzz").expect.to.equal(":0\r\n");

        // HSETNX: only when absent
        ks.run("HSETNX", "h", "f1", "nope").expect.to.equal(":0\r\n");
        ks.run("HSETNX", "h", "f4", "d").expect.to.equal(":1\r\n");
        ks.run("HGET", "h", "f1").expect.to.equal(bulk("x")); // unchanged

        // HDEL returns count removed; last field drops the key
        ks.run("HDEL", "h", "f1", "zzz").expect.to.equal(":1\r\n");
        ks.run("HDEL", "h", "f2", "f3", "f4").expect.to.equal(":3\r\n");
        ks.run("EXISTS", "h").expect.to.equal(":0\r\n");
    }

    @("valkey.hash.getall_keys_vals")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        ks.run("HSET", "h", "a", "1", "b", "2", "c", "3");
        sameHash(ks.run("HGETALL", "h"), "a", "1", "b", "2", "c", "3");
        sameSet(ks.run("HKEYS", "h"), "a", "b", "c");
        sameSet(ks.run("HVALS", "h"), "1", "2", "3");
        // empty on missing
        ks.run("HGETALL", "missing").expect.to.equal("*0\r\n");
    }

    @("valkey.hash.incr")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        // HINCRBY creates + increments; returns the new integer
        ks.run("HINCRBY", "h", "n", "5").expect.to.equal(":5\r\n");
        ks.run("HINCRBY", "h", "n", "-2").expect.to.equal(":3\r\n");
        // non-integer field -> error
        ks.run("HSET", "h", "s", "abc");
        ks.run("HINCRBY", "h", "s", "1").startsWith("-ERR").should.equal(true);

        // HINCRBYFLOAT
        ks.run("HINCRBYFLOAT", "h", "f", "1.5").expect.to.equal(bulk("1.5"));
        ks.run("HINCRBYFLOAT", "h", "f", "2.0").expect.to.equal(bulk("3.5"));
    }
}
