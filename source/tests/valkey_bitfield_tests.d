module tests.valkey_bitfield_tests;

// Valkey unit/bitfield.tcl ported to native in-process UT. The tcl is a read-only
// spec + oracle; nothing tcl lands here. Expected reply bytes were grounded against
// a live valkey-server (redis-cli --no-raw / raw RESP socket). Replication tests
// (master/slave), fuzzing loops and DEBUG RELOAD are server-only / non-deterministic
// -> left to the blackbox sweep. See THIRD_PARTY_NOTICES for the Valkey credit.

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
        v.dispatch(ks, o, arena, 1_700_000_000_000UL);
        return (cast(string) o.data).idup;
    }

    // "*N\r\n:v1\r\n..." — every element an integer.
    private string arr(long[] vals...)
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

    enum NIL = "$-1\r\n";

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

        // INCRBY on a missing key starts from 0 (default WRAP)
        ks.run("DEL", "fresh");
        ks.run("BITFIELD", "fresh", "incrby", "u8", "0", "10").expect.to.equal(arr(10));
    }

    @("valkey.bitfield.overflow_wrap")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        // unsigned WRAP: 100+257 = 357 mod 256 = 101
        ks.run("BITFIELD", "bits", "set", "u8", "#0", "100");
        ks.run("BITFIELD", "bits", "overflow", "wrap", "incrby", "u8", "#0", "257").expect.to.equal(arr(101));
        ks.run("BITFIELD", "bits", "get", "u8", "#0").expect.to.equal(arr(101));
        ks.run("BITFIELD", "bits", "overflow", "wrap", "incrby", "u8", "#0", "255").expect.to.equal(arr(100));
        ks.run("BITFIELD", "bits", "get", "u8", "#0").expect.to.equal(arr(100));

        // signed WRAP behaves the same way on the raw bits
        ks.run("DEL", "bits");
        ks.run("BITFIELD", "bits", "set", "i8", "#0", "100");
        ks.run("BITFIELD", "bits", "overflow", "wrap", "incrby", "i8", "#0", "257").expect.to.equal(arr(101));
        ks.run("BITFIELD", "bits", "get", "i8", "#0").expect.to.equal(arr(101));
        ks.run("BITFIELD", "bits", "overflow", "wrap", "incrby", "i8", "#0", "255").expect.to.equal(arr(100));
        ks.run("BITFIELD", "bits", "get", "i8", "#0").expect.to.equal(arr(100));
    }

    @("valkey.bitfield.overflow_sat")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        // unsigned SAT: saturate high to 255, low to 0
        ks.run("BITFIELD", "bits", "set", "u8", "#0", "100");
        ks.run("BITFIELD", "bits", "overflow", "sat", "incrby", "u8", "#0", "257").expect.to.equal(arr(255));
        ks.run("BITFIELD", "bits", "get", "u8", "#0").expect.to.equal(arr(255));
        ks.run("BITFIELD", "bits", "overflow", "sat", "incrby", "u8", "#0", "-255").expect.to.equal(arr(0));
        ks.run("BITFIELD", "bits", "get", "u8", "#0").expect.to.equal(arr(0));

        // signed SAT: saturate high to 127, low to -128 (i8 read from a u8-set key)
        ks.run("DEL", "bits");
        ks.run("BITFIELD", "bits", "set", "u8", "#0", "100");
        ks.run("BITFIELD", "bits", "overflow", "sat", "incrby", "i8", "#0", "257").expect.to.equal(arr(127));
        ks.run("BITFIELD", "bits", "get", "i8", "#0").expect.to.equal(arr(127));
        ks.run("BITFIELD", "bits", "overflow", "sat", "incrby", "i8", "#0", "-255").expect.to.equal(arr(-128));
        ks.run("BITFIELD", "bits", "get", "i8", "#0").expect.to.equal(arr(-128));

        // SET beyond range under SAT clamps to the boundary
        ks.run("DEL", "bits");
        ks.run("BITFIELD", "bits", "overflow", "sat", "set", "u8", "0", "300").expect.to.equal(arr(0));
        ks.run("BITFIELD", "bits", "get", "u8", "0").expect.to.equal(arr(255));
    }

    @("valkey.bitfield.overflow_fail")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        // FAIL returns a nil element for the operation that would overflow
        ks.run("BITFIELD", "bits", "set", "u8", "0", "255").expect.to.equal(arr(0));
        ks.run("BITFIELD", "bits", "overflow", "fail", "incrby", "u8", "0", "10")
            .expect.to.equal("*1\r\n" ~ NIL);

        // mixed chain: FAIL nil element followed by a plain GET int -> "*2\r\n$-1\r\n:200\r\n"
        ks.run("DEL", "bits");
        ks.run("BITFIELD", "bits", "set", "u8", "0", "200").expect.to.equal(arr(0));
        ks.run("BITFIELD", "bits", "overflow", "fail", "incrby", "u8", "0", "100",
                "get", "u8", "0").expect.to.equal("*2\r\n" ~ NIL ~ ":200\r\n");

        // OVERFLOW keyword switches mid-chain: SAT clamps, then FAIL nils
        ks.run("DEL", "bits");
        ks.run("BITFIELD", "bits", "set", "u8", "0", "200", "overflow", "sat",
                "incrby", "u8", "0", "100", "overflow", "fail", "incrby", "u8", "0", "100")
            .expect.to.equal("*3\r\n:0\r\n:255\r\n" ~ NIL);
    }

    @("valkey.bitfield.widths_and_boundaries")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        // i1 min: set 0 then incrby 1 -> -1 (single sign bit)
        ks.run("BITFIELD", "b1", "set", "i1", "0", "0", "incrby", "i1", "0", "1")
            .expect.to.equal(arr(0, -1));

        // i64 full negative extreme round-trips
        ks.run("DEL", "b64");
        ks.run("BITFIELD", "b64", "set", "i64", "0", "-9223372036854775808",
                "get", "i64", "0").expect.to.equal(arr(0, long.min));

        // u63 max round-trips (u64 is not supported, i64/u63 are the ceilings)
        ks.run("DEL", "u");
        ks.run("BITFIELD", "u", "set", "u63", "0", "9223372036854775807",
                "get", "u63", "0").expect.to.equal(arr(0, 9_223_372_036_854_775_807L));
    }

    @("valkey.bitfield.regressions")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        // #3221: GET u1 on the string "1" reads the high bit of '1' (0x31) -> 0
        ks.run("SET", "bits", "1");
        ks.run("BITFIELD", "bits", "get", "u1", "0").expect.to.equal(arr(0));

        // #3564: SET at bit 0 and bit 64 then INCRBY the byte at bit 10 (a fresh byte)
        ks.run("DEL", "mystring");
        ks.run("BITFIELD", "mystring", "SET", "i8", "0", "10", "SET", "i8", "64", "10",
                "INCRBY", "i8", "10", "99900").expect.to.equal(arr(0, 0, 60));
    }

    @("valkey.bitfield.errors")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        // arity: BITFIELD needs at least a key
        ks.run("BITFIELD").startsWith("-ERR").should.equal(true);

        // wrong type
        ks.run("RPUSH", "l", "a");
        ks.run("BITFIELD", "l", "get", "u8", "0").startsWith("-WRONGTYPE").should.equal(true);

        // invalid bitfield type: u64 unsupported, i65 too wide
        ks.run("BITFIELD", "bits", "get", "u64", "0")
            .startsWith("-ERR Invalid bitfield type").should.equal(true);
        ks.run("BITFIELD", "bits", "get", "i65", "0")
            .startsWith("-ERR Invalid bitfield type").should.equal(true);

        // negative / bad bit offset
        ks.run("BITFIELD", "bits", "get", "u8", "-1")
            .startsWith("-ERR bit offset is not an integer or out of range").should.equal(true);

        // syntax error for an unknown subcommand
        ks.run("BITFIELD", "bits", "foo", "u8", "0")
            .startsWith("-ERR syntax error").should.equal(true);

        // invalid OVERFLOW type
        ks.run("BITFIELD", "bits", "overflow", "bad", "get", "u8", "0")
            .startsWith("-ERR Invalid OVERFLOW type specified").should.equal(true);

        // non-integer value
        ks.run("BITFIELD", "bits", "set", "u8", "0", "abc")
            .startsWith("-ERR value is not an integer or out of range").should.equal(true);
    }

    @("valkey.bitfield.readonly")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        // BITFIELD_RO with only the key -> empty array
        ks.run("BITFIELD_RO", "bits").expect.to.equal("*0\r\n");

        // BITFIELD_RO rejects any write subcommand with the exact error
        ks.run("BITFIELD_RO", "bits", "set", "u8", "0", "100", "get", "u8", "0")
            .startsWith("-ERR BITFIELD_RO only supports the GET subcommand").should.equal(true);

        // INCRBY is a write too -> rejected
        ks.run("BITFIELD_RO", "bits", "incrby", "u8", "0", "1")
            .startsWith("-ERR BITFIELD_RO only supports the GET subcommand").should.equal(true);

        // BITFIELD_RO GET works
        ks.run("BITFIELD", "bits", "set", "u8", "0", "42");
        ks.run("BITFIELD_RO", "bits", "get", "u8", "0").expect.to.equal(arr(42));

        // GET on a missing key returns 0
        ks.run("DEL", "gone");
        ks.run("BITFIELD_RO", "gone", "get", "u8", "0").expect.to.equal(arr(0));
    }
}
