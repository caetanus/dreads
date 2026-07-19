module tests.valkey_bitfield_tests;

// Valkey unit/bitfield.tcl ported to native in-process UT (see valkey_incr_tests.d
// for the "contrabando" rationale + THIRD_PARTY_NOTICES credit). Replication tests
// (master/slave) are server-only → left to the blackbox sweep.

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

    private string arr(long[] vals...) // "*N\r\n:v1\r\n..."
    {
        string r = "*" ~ vals.length.to!string ~ "\r\n";
        foreach (v; vals)
            r ~= ":" ~ v.to!string ~ "\r\n";
        return r;
    }

    private string bulk(string p)
    {
        return "$" ~ p.length.to!string ~ "\r\n" ~ p ~ "\r\n";
    }

    @("valkey.bitfield.set_get_basics")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        // signed SET/GET basics (each call returns an array of 1)
        ks.run("BITFIELD", "bits", "set", "i8", "0", "-100").expect.to.equal(arr(0));
        ks.run("BITFIELD", "bits", "set", "i8", "0", "101").expect.to.equal(arr(-100));
        ks.run("BITFIELD", "bits", "get", "i8", "0").expect.to.equal(arr(101));

        // unsigned SET/GET basics
        ks.run("DEL", "bits");
        ks.run("BITFIELD", "bits", "set", "u8", "0", "255").expect.to.equal(arr(0));
        ks.run("BITFIELD", "bits", "set", "u8", "0", "100").expect.to.equal(arr(255));
        ks.run("BITFIELD", "bits", "get", "u8", "0").expect.to.equal(arr(100));

        // signed SET/GET together in one call: 255 wraps to -1 in i8
        ks.run("DEL", "bits");
        ks.run("BITFIELD", "bits", "set", "i8", "0", "255", "set", "i8", "0", "100",
                "get", "i8", "0").expect.to.equal(arr(0, -1, 100));

        // unsigned SET, INCRBY, GET: 255+100 = 355 wraps mod 256 -> 99
        ks.run("DEL", "bits");
        ks.run("BITFIELD", "bits", "set", "u8", "0", "255", "incrby", "u8", "0", "100",
                "get", "u8", "0").expect.to.equal(arr(0, 99, 99));

        // only the key -> empty array
        ks.run("DEL", "bits");
        ks.run("BITFIELD", "bits").expect.to.equal("*0\r\n");
    }

    @("valkey.bitfield.idx_and_incrby")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        // #<idx> form: three u8 slots become the bytes "ABC"
        ks.run("BITFIELD", "bits", "set", "u8", "#0", "65");
        ks.run("BITFIELD", "bits", "set", "u8", "#1", "66");
        ks.run("BITFIELD", "bits", "set", "u8", "#2", "67");
        ks.run("GET", "bits").expect.to.equal(bulk("ABC"));

        // basic INCRBY form
        ks.run("DEL", "bits");
        ks.run("BITFIELD", "bits", "set", "u8", "#0", "10");
        ks.run("BITFIELD", "bits", "incrby", "u8", "#0", "100").expect.to.equal(arr(110));
        ks.run("BITFIELD", "bits", "incrby", "u8", "#0", "100").expect.to.equal(arr(210));

        // chaining two INCRBY in one call
        ks.run("DEL", "bits");
        ks.run("BITFIELD", "bits", "set", "u8", "#0", "10");
        ks.run("BITFIELD", "bits", "incrby", "u8", "#0", "100", "incrby", "u8", "#0", "100")
            .expect.to.equal(arr(110, 210));
    }

    @("valkey.bitfield.overflow")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        // unsigned WRAP
        ks.run("BITFIELD", "bits", "set", "u8", "#0", "100");
        ks.run("BITFIELD", "bits", "overflow", "wrap", "incrby", "u8", "#0", "257").expect.to.equal(arr(101));
        ks.run("BITFIELD", "bits", "get", "u8", "#0").expect.to.equal(arr(101));
        ks.run("BITFIELD", "bits", "overflow", "wrap", "incrby", "u8", "#0", "255").expect.to.equal(arr(100));
        ks.run("BITFIELD", "bits", "get", "u8", "#0").expect.to.equal(arr(100));

        // unsigned SAT: saturate high to 255, low to 0
        ks.run("DEL", "bits");
        ks.run("BITFIELD", "bits", "set", "u8", "#0", "100");
        ks.run("BITFIELD", "bits", "overflow", "sat", "incrby", "u8", "#0", "257").expect.to.equal(arr(255));
        ks.run("BITFIELD", "bits", "get", "u8", "#0").expect.to.equal(arr(255));
        ks.run("BITFIELD", "bits", "overflow", "sat", "incrby", "u8", "#0", "-255").expect.to.equal(arr(0));
        ks.run("BITFIELD", "bits", "get", "u8", "#0").expect.to.equal(arr(0));

        // signed WRAP
        ks.run("DEL", "bits");
        ks.run("BITFIELD", "bits", "set", "i8", "#0", "100");
        ks.run("BITFIELD", "bits", "overflow", "wrap", "incrby", "i8", "#0", "257").expect.to.equal(arr(101));
        ks.run("BITFIELD", "bits", "get", "i8", "#0").expect.to.equal(arr(101));
        ks.run("BITFIELD", "bits", "overflow", "wrap", "incrby", "i8", "#0", "255").expect.to.equal(arr(100));
        ks.run("BITFIELD", "bits", "get", "i8", "#0").expect.to.equal(arr(100));

        // signed SAT: saturate high to 127, low to -128
        ks.run("DEL", "bits");
        ks.run("BITFIELD", "bits", "set", "u8", "#0", "100");
        ks.run("BITFIELD", "bits", "overflow", "sat", "incrby", "i8", "#0", "257").expect.to.equal(arr(127));
        ks.run("BITFIELD", "bits", "get", "i8", "#0").expect.to.equal(arr(127));
        ks.run("BITFIELD", "bits", "overflow", "sat", "incrby", "i8", "#0", "-255").expect.to.equal(arr(-128));
        ks.run("BITFIELD", "bits", "get", "i8", "#0").expect.to.equal(arr(-128));
    }

    @("valkey.bitfield.readonly")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        // BITFIELD_RO with only the key -> empty array
        ks.run("BITFIELD_RO", "bits").expect.to.equal("*0\r\n");
        // BITFIELD_RO rejects any write subcommand
        ks.run("BITFIELD_RO", "bits", "set", "u8", "0", "100", "get", "u8", "0")
            .startsWith("-ERR").should.equal(true);
        // BITFIELD_RO GET works
        ks.run("BITFIELD", "bits", "set", "u8", "0", "42");
        ks.run("BITFIELD_RO", "bits", "get", "u8", "0").expect.to.equal(arr(42));
    }
}
