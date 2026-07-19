module tests.valkey_scan_tests;

// Valkey unit/scan.tcl core ported to native in-process UT (see valkey_incr_tests.d
// for rationale + THIRD_PARTY_NOTICES credit). SCAN is cursor-based: each helper
// loops until the cursor returns to "0" and compares the collected elements as an
// unordered set. Fuzz/encoding/DEBUG cases stay in the blackbox sweep.

version (unittest)
{
    import fluent.asserts;
    import std.conv : to;
    import std.string : indexOf;
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

    // parse a SCAN reply `*2\r\n<cursor bulk><elements array>` -> (cursor, elements)
    private void parseScan(string s, out string cursor, out string[] elems)
    {
        assert(s[0] == '*'); // *2
        size_t i = s.indexOf("\r\n") + 2;
        assert(s[i] == '$');
        immutable e = s[i .. $].indexOf("\r\n") + i;
        immutable cl = s[i + 1 .. e].to!int;
        i = e + 2;
        cursor = s[i .. i + cl];
        i += cl + 2;
        elems = parseArr(s[i .. $]);
    }

    // loop `pre ~ [cursor] ~ post` until the cursor returns to "0"; collect elements
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

    private void sameSet(string[] got, string[] expected...)
    {
        sort(got);
        auto exp = expected.dup;
        sort(exp);
        got.expect.to.equal(exp);
    }

    @("valkey.scan.keyspace")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        ks.run("MSET", "a", "1", "b", "2", "c", "3");
        ks.run("RPUSH", "mylist", "x");
        ks.run("SADD", "myset", "m");

        // SCAN collects every key
        sameSet(scanLoop(ks, ["SCAN"]), "a", "b", "c", "mylist", "myset");
        // MATCH filters
        sameSet(scanLoop(ks, ["SCAN"], ["MATCH", "my*"]), "mylist", "myset");
        // TYPE filters
        sameSet(scanLoop(ks, ["SCAN"], ["TYPE", "string"]), "a", "b", "c");
        // COUNT is a hint, still returns everything
        sameSet(scanLoop(ks, ["SCAN"], ["COUNT", "1"]), "a", "b", "c", "mylist", "myset");
    }

    @("valkey.scan.hscan_sscan_zscan")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        // HSCAN returns flat [f,v,f,v,...]
        ks.run("HSET", "h", "f1", "a", "f2", "b", "f3", "c");
        sameSet(scanLoop(ks, ["HSCAN", "h"]), "f1", "a", "f2", "b", "f3", "c");
        // HSCAN NOVALUES (7.4): fields only
        sameSet(scanLoop(ks, ["HSCAN", "h"], ["NOVALUES"]), "f1", "f2", "f3");

        // SSCAN returns the members
        ks.run("SADD", "s", "m1", "m2", "m3");
        sameSet(scanLoop(ks, ["SSCAN", "s"]), "m1", "m2", "m3");
        sameSet(scanLoop(ks, ["SSCAN", "s"], ["MATCH", "m1"]), "m1");

        // ZSCAN returns flat [member, score, ...]
        ks.run("ZADD", "z", "1", "a", "2", "b");
        sameSet(scanLoop(ks, ["ZSCAN", "z"]), "a", "1", "b", "2");
    }
}
