module tests.valkey_list_tests;

// Valkey unit/type/list.tcl core ops ported to native in-process UT (see
// valkey_incr_tests.d for rationale + THIRD_PARTY_NOTICES credit). Encoding-conversion
// variants (listpack<->quicklist via config), blocking (BLPOP/BRPOP) and fuzz cases
// are server-only / covered elsewhere (blocking has its own UT; encodings are the
// small-container UTs). Here: the deterministic command semantics.

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

    private string arrB(string[] items...) // array of bulk strings
    {
        string r = "*" ~ items.length.to!string ~ "\r\n";
        foreach (it; items)
            r ~= bulk(it);
        return r;
    }

    enum NIL = "$-1\r\n";

    @("valkey.list.push_range_len_index")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        // RPUSH / LPUSH return the new length; order is head..tail
        ks.run("RPUSH", "l", "a", "b", "c").expect.to.equal(":3\r\n");
        ks.run("LPUSH", "l", "z").expect.to.equal(":4\r\n");
        ks.run("LRANGE", "l", "0", "-1").expect.to.equal(arrB("z", "a", "b", "c"));
        ks.run("LLEN", "l").expect.to.equal(":4\r\n");

        // LINDEX: positive, negative, out of range -> nil
        ks.run("LINDEX", "l", "0").expect.to.equal(bulk("z"));
        ks.run("LINDEX", "l", "-1").expect.to.equal(bulk("c"));
        ks.run("LINDEX", "l", "99").expect.to.equal(NIL);

        // LSET valid + out of range
        ks.run("LSET", "l", "0", "Z").expect.to.equal("+OK\r\n");
        ks.run("LINDEX", "l", "0").expect.to.equal(bulk("Z"));
        ks.run("LSET", "l", "99", "x").startsWith("-ERR").should.equal(true);

        // LRANGE sub-range + out-of-bounds clamp
        ks.run("LRANGE", "l", "1", "2").expect.to.equal(arrB("a", "b"));
        ks.run("LRANGE", "l", "5", "10").expect.to.equal("*0\r\n");
        ks.run("LRANGE", "missing", "0", "-1").expect.to.equal("*0\r\n");

        // PUSHX only when the key exists
        ks.run("RPUSHX", "nope", "v").expect.to.equal(":0\r\n");
        ks.run("LPUSHX", "nope", "v").expect.to.equal(":0\r\n");
        ks.run("EXISTS", "nope").expect.to.equal(":0\r\n");
    }

    @("valkey.list.pop_with_count")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        ks.run("RPUSH", "l", "a", "b", "c", "d", "e");
        // single element pop -> bulk
        ks.run("LPOP", "l").expect.to.equal(bulk("a"));
        ks.run("RPOP", "l").expect.to.equal(bulk("e"));
        // count -> array (LPOP 2 = [b c])
        ks.run("LPOP", "l", "2").expect.to.equal(arrB("b", "c"));
        // remaining: [d]; count larger than list returns what's left
        ks.run("RPOP", "l", "5").expect.to.equal(arrB("d"));
        // now empty -> key gone
        ks.run("EXISTS", "l").expect.to.equal(":0\r\n");
        // pop on missing: no-count -> nil bulk; with count -> nil array
        ks.run("LPOP", "l").expect.to.equal(NIL);
        ks.run("LPOP", "l", "2").expect.to.equal("*-1\r\n");
    }

    @("valkey.list.insert_rem_trim")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        // LINSERT before/after a pivot; missing pivot -> -1; missing key -> 0
        ks.run("RPUSH", "l", "a", "b", "d");
        ks.run("LINSERT", "l", "BEFORE", "d", "c").expect.to.equal(":4\r\n");
        ks.run("LRANGE", "l", "0", "-1").expect.to.equal(arrB("a", "b", "c", "d"));
        ks.run("LINSERT", "l", "AFTER", "d", "e").expect.to.equal(":5\r\n");
        ks.run("LINSERT", "l", "BEFORE", "zzz", "x").expect.to.equal(":-1\r\n");
        ks.run("LINSERT", "missing", "BEFORE", "a", "x").expect.to.equal(":0\r\n");

        // LREM: count>0 head-first, count<0 tail-first, count=0 all
        ks.run("DEL", "r");
        ks.run("RPUSH", "r", "a", "b", "a", "c", "a");
        ks.run("LREM", "r", "2", "a").expect.to.equal(":2\r\n"); // first two 'a'
        ks.run("LRANGE", "r", "0", "-1").expect.to.equal(arrB("b", "c", "a"));
        ks.run("DEL", "r");
        ks.run("RPUSH", "r", "a", "b", "a", "c", "a");
        ks.run("LREM", "r", "-1", "a").expect.to.equal(":1\r\n"); // last 'a'
        ks.run("LRANGE", "r", "0", "-1").expect.to.equal(arrB("a", "b", "a", "c"));
        ks.run("LREM", "r", "0", "a").expect.to.equal(":2\r\n"); // all 'a'
        ks.run("LRANGE", "r", "0", "-1").expect.to.equal(arrB("b", "c"));

        // LTRIM keeps [start,end]
        ks.run("DEL", "t");
        ks.run("RPUSH", "t", "a", "b", "c", "d", "e");
        ks.run("LTRIM", "t", "1", "3").expect.to.equal("+OK\r\n");
        ks.run("LRANGE", "t", "0", "-1").expect.to.equal(arrB("b", "c", "d"));
    }

    @("valkey.list.move_and_pos")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        // RPOPLPUSH moves the tail of src to the head of dst, returns the element
        ks.run("RPUSH", "src", "a", "b", "c");
        ks.run("RPOPLPUSH", "src", "dst").expect.to.equal(bulk("c"));
        ks.run("LRANGE", "src", "0", "-1").expect.to.equal(arrB("a", "b"));
        ks.run("LRANGE", "dst", "0", "-1").expect.to.equal(arrB("c"));
        // LMOVE with explicit directions
        ks.run("LMOVE", "src", "dst", "LEFT", "RIGHT").expect.to.equal(bulk("a"));
        ks.run("LRANGE", "dst", "0", "-1").expect.to.equal(arrB("c", "a"));

        // LPOS: index of first match, RANK for the Nth, COUNT for all, nil if absent
        ks.run("DEL", "p");
        ks.run("RPUSH", "p", "a", "b", "c", "a", "b", "c", "a");
        ks.run("LPOS", "p", "a").expect.to.equal(":0\r\n");
        ks.run("LPOS", "p", "a", "RANK", "2").expect.to.equal(":3\r\n");
        ks.run("LPOS", "p", "a", "COUNT", "0").expect.to.equal("*3\r\n:0\r\n:3\r\n:6\r\n");
        ks.run("LPOS", "p", "zzz").expect.to.equal(NIL);
    }
}
