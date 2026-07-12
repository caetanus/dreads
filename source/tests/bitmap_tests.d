module tests.bitmap_tests;

version (unittest)
{
    import fluent.asserts;

    import dreads.commands;
    import dreads.mem : Arena, ByteBuffer;
    import dreads.obj : Keyspace;
    import dreads.resp;

    private string respCmd(string[] args...)
    {
        import std.conv : to;

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

    @("bitmap.setbit_getbit")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();
        ks.run("SETBIT", "b", "7", "1").expect.to.equal(":0\r\n");
        ks.run("GETBIT", "b", "7").expect.to.equal(":1\r\n");
        ks.run("GETBIT", "b", "6").expect.to.equal(":0\r\n");
        ks.run("GETBIT", "b", "9999").expect.to.equal(":0\r\n"); // past end reads 0
        ks.run("SETBIT", "b", "7", "0").expect.to.equal(":1\r\n");
        ks.run("GET", "b").expect.to.equal("$1\r\n\0\r\n");
        // bit 0 is the MSB of byte 0 (Redis convention)
        ks.run("SETBIT", "b", "0", "1");
        ks.run("GET", "b").expect.to.equal("$1\r\n\x80\r\n");
        // growth zero-fills
        ks.run("SETBIT", "b", "23", "1");
        ks.run("STRLEN", "b").expect.to.equal(":3\r\n");
        // robustness
        ks.run("SETBIT", "b", "-1", "1")[0].expect.to.equal('-');
        ks.run("SETBIT", "b", "0", "2")[0].expect.to.equal('-');
        ks.run("SETBIT", "b", "99999999999999", "1")[0].expect.to.equal('-');
        ks.run("RPUSH", "l", "x");
        ks.run("SETBIT", "l", "0", "1")[0].expect.to.equal('-'); // WRONGTYPE
    }

    @("bitmap.bitcount_bitpos")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();
        ks.run("SET", "s", "foobar");
        ks.run("BITCOUNT", "s").expect.to.equal(":26\r\n");
        ks.run("BITCOUNT", "s", "0", "0").expect.to.equal(":4\r\n");
        ks.run("BITCOUNT", "s", "1", "1").expect.to.equal(":6\r\n");
        ks.run("BITCOUNT", "s", "0", "5", "BIT").expect.to.equal(":3\r\n"); // 'f'=01100110, bits 0..5
        ks.run("BITCOUNT", "s", "5", "30", "BIT").expect.to.equal(":17\r\n");
        ks.run("BITCOUNT", "ghost").expect.to.equal(":0\r\n");
        ks.run("BITCOUNT", "s", "50", "60").expect.to.equal(":0\r\n"); // out of window
        // start without end: end defaults to -1
        ks.run("BITCOUNT", "s", "0").expect.to.equal(":26\r\n");
        ks.run("BITCOUNT", "s", "1").expect.to.equal(":22\r\n"); // "oobar"
        ks.run("BITCOUNT", "s", "-1").expect.to.equal(":4\r\n"); // "r"
        ks.run("BITCOUNT", "s", "1000").expect.to.equal(":0\r\n");
        ks.run("BITCOUNT", "s", "x")
            .expect.to.equal("-ERR value is not an integer or out of range\r\n");
        ks.run("BITCOUNT", "s", "0", "1", "junk").expect.to.equal("-ERR syntax error\r\n");
        // args are validated before the key is looked at (Valkey parity)
        ks.run("BITCOUNT", "ghost", "a", "b")
            .expect.to.equal("-ERR value is not an integer or out of range\r\n");

        ks.run("SET", "z", "\xff\xf0\x00");
        ks.run("BITPOS", "z", "0").expect.to.equal(":12\r\n");
        ks.run("BITPOS", "z", "1").expect.to.equal(":0\r\n");
        ks.run("SET", "ones", "\xff\xff");
        ks.run("BITPOS", "ones", "0").expect.to.equal(":16\r\n"); // virtual zero past end
        ks.run("BITPOS", "ones", "0", "0", "-1").expect.to.equal(":-1\r\n"); // explicit end
        ks.run("BITPOS", "ones", "0", "0").expect.to.equal(":16\r\n"); // start only: virtual zero
        ks.run("BITPOS", "ghost", "1").expect.to.equal(":-1\r\n");
        // missing key is all zeros no matter the range
        ks.run("BITPOS", "ghost", "0", "0", "-1", "BIT").expect.to.equal(":0\r\n");
        ks.run("BITPOS", "z", "2")[0].expect.to.equal('-');
        ks.run("BITPOS", "z", "x")
            .expect.to.equal("-ERR value is not an integer or out of range\r\n");
        // args are validated before the key is looked at
        ks.run("BITPOS", "ghost", "0", "a", "b")
            .expect.to.equal("-ERR value is not an integer or out of range\r\n");
        ks.run("BITPOS", "ghost", "0", "0", "-1", "JUNK").expect.to.equal("-ERR syntax error\r\n");
        ks.run("BITPOS", "ghost", "0", "0", "-1", "BIT", "X").expect.to.equal("-ERR syntax error\r\n");
    }

    @("bitmap.bitop")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();
        ks.run("SET", "a", "abc");
        ks.run("SET", "b", "ab");
        ks.run("BITOP", "AND", "dst", "a", "b").expect.to.equal(":3\r\n");
        ks.run("GET", "dst").expect.to.equal("$3\r\nab\0\r\n"); // shorter src zero-padded
        ks.run("BITOP", "OR", "dst", "a", "b").expect.to.equal(":3\r\n");
        ks.run("GET", "dst").expect.to.equal("$3\r\nabc\r\n");
        ks.run("BITOP", "XOR", "dst", "a", "a").expect.to.equal(":3\r\n");
        ks.run("GET", "dst").expect.to.equal("$3\r\n\0\0\0\r\n");
        ks.run("BITOP", "NOT", "dst", "a").expect.to.equal(":3\r\n");
        ks.run("BITOP", "NOT", "dst", "a", "b")[0].expect.to.equal('-'); // NOT is unary
        ks.run("BITOP", "NAND", "dst", "a")[0].expect.to.equal('-');
        ks.run("BITOP", "AND", "dst", "ghost1", "ghost2").expect.to.equal(":0\r\n");
        ks.run("EXISTS", "dst").expect.to.equal(":0\r\n"); // empty result deletes
    }

    @("bitmap.bitfield")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();
        // Redis doc example: INCRBY then GET in one call
        ks.run("BITFIELD", "bf", "INCRBY", "i5", "100", "1", "GET", "u4", "0")
            .expect.to.equal("*2\r\n:1\r\n:0\r\n");
        // SET returns the old value
        ks.run("BITFIELD", "bf2", "SET", "u8", "0", "255").expect.to.equal("*1\r\n:0\r\n");
        ks.run("BITFIELD", "bf2", "GET", "u8", "0").expect.to.equal("*1\r\n:255\r\n");
        // signed read of the same bits
        ks.run("BITFIELD", "bf2", "GET", "i8", "0").expect.to.equal("*1\r\n:-1\r\n");
        // '#' scaled offsets
        ks.run("BITFIELD", "bf3", "SET", "u8", "#1", "7", "GET", "u8", "8")
            .expect.to.equal("*2\r\n:0\r\n:7\r\n");
        // overflow semantics on u2 (max 3)
        ks.run("BITFIELD", "ov", "SET", "u2", "0", "3").expect.to.equal("*1\r\n:0\r\n");
        ks.run("BITFIELD", "ov", "OVERFLOW", "SAT", "INCRBY", "u2", "0", "5")
            .expect.to.equal("*1\r\n:3\r\n"); // clamped
        ks.run("BITFIELD", "ov", "OVERFLOW", "FAIL", "INCRBY", "u2", "0", "1")
            .expect.to.equal("*1\r\n$-1\r\n"); // nil on overflow
        ks.run("BITFIELD", "ov", "OVERFLOW", "WRAP", "INCRBY", "u2", "0", "1")
            .expect.to.equal("*1\r\n:0\r\n"); // 3+1 wraps to 0
        // robustness
        ks.run("BITFIELD", "bf", "GET", "u64", "0")[0].expect.to.equal('-'); // u64 invalid
        ks.run("BITFIELD", "bf", "GET", "x8", "0")[0].expect.to.equal('-');
        ks.run("BITFIELD", "bf", "SET", "u8", "0")[0].expect.to.equal('-');
        ks.run("BITFIELD_RO", "bf", "SET", "u8", "0", "1")[0].expect.to.equal('-');
        ks.run("BITFIELD_RO", "bf2", "GET", "u8", "0").expect.to.equal("*1\r\n:255\r\n");
        ks.run("BITFIELD", "bf").expect.to.equal("*0\r\n");
    }
}
