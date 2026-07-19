module tests.valkey_bitops_tests;

// Valkey unit/bitops.tcl ported to native in-process UT. The .tcl is a
// read-only spec/oracle (see THIRD_PARTY_NOTICES for the BSD-3 credit); no tcl
// code lands here. Fuzzing loops, DEBUG RELOAD, large-memory (proto-max-bulk-len)
// and dirty-counter (rdb_changes_since_last_save) cases are server-only -> skipped.

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

    private string i(long n)
    {
        return ":" ~ n.to!string ~ "\r\n";
    }

    enum NIL = "$-1\r\n";

    // ---------------------------------------------------------------- BITCOUNT

    @("valkey.bitops.bitcount_wrongtype")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        ks.run("DEL", "mylist");
        ks.run("LPUSH", "mylist", "a", "b", "c");
        ks.run("BITCOUNT", "mylist").startsWith("-WRONGTYPE").should.equal(true);
        ks.run("BITCOUNT", "mylist", "0", "100").startsWith("-WRONGTYPE").should.equal(true);
        // negative indexes where start > end still WRONGTYPE (type checked)
        ks.run("BITCOUNT", "mylist", "-6", "-7").startsWith("-WRONGTYPE").should.equal(true);
        ks.run("BITCOUNT", "mylist", "-6", "-15", "bit").startsWith("-WRONGTYPE").should.equal(true);
    }

    @("valkey.bitops.bitcount_missing_and_oob")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        // 0 against non existing key
        ks.run("DEL", "no-key");
        ks.run("BITCOUNT", "no-key").expect.to.equal(i(0));
        ks.run("BITCOUNT", "no-key", "0", "1000", "bit").expect.to.equal(i(0));

        // 0 with out of range indexes
        ks.run("SET", "str", "xxxx");
        ks.run("BITCOUNT", "str", "4", "10").expect.to.equal(i(0));
        ks.run("BITCOUNT", "str", "32", "87", "bit").expect.to.equal(i(0));

        // 0 with negative indexes where start > end (existing key)
        ks.run("BITCOUNT", "str", "-6", "-7").expect.to.equal(i(0));
        ks.run("BITCOUNT", "str", "-6", "-15", "bit").expect.to.equal(i(0));

        // same, against non existing key
        ks.run("DEL", "str");
        ks.run("BITCOUNT", "str", "-6", "-7").expect.to.equal(i(0));
        ks.run("BITCOUNT", "str", "-6", "-15", "bit").expect.to.equal(i(0));
    }

    @("valkey.bitops.bitcount_test_vectors")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        // count_bits(vec) and full-range bit-mode both equal the popcount
        // "" -> 0
        ks.run("SET", "str", "");
        ks.run("BITCOUNT", "str").expect.to.equal(i(0));
        ks.run("BITCOUNT", "str", "0", "-1", "bit").expect.to.equal(i(0));
        // "\xaa" -> 4
        ks.run("SET", "str", "\xaa");
        ks.run("BITCOUNT", "str").expect.to.equal(i(4));
        ks.run("BITCOUNT", "str", "0", "-1", "bit").expect.to.equal(i(4));
        // "\x00\x00\xff" -> 8
        ks.run("SET", "str", "\x00\x00\xff");
        ks.run("BITCOUNT", "str").expect.to.equal(i(8));
        ks.run("BITCOUNT", "str", "0", "-1", "bit").expect.to.equal(i(8));
        // "foobar" -> 26
        ks.run("SET", "str", "foobar");
        ks.run("BITCOUNT", "str").expect.to.equal(i(26));
        ks.run("BITCOUNT", "str", "0", "-1", "bit").expect.to.equal(i(26));
        // "123" -> 10
        ks.run("SET", "str", "123");
        ks.run("BITCOUNT", "str").expect.to.equal(i(10));
        ks.run("BITCOUNT", "str", "0", "-1", "bit").expect.to.equal(i(10));
    }

    @("valkey.bitops.bitcount_just_start")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        ks.run("SET", "s", "foobar");
        ks.run("BITCOUNT", "s", "0").expect.to.equal(i(26)); // "foobar"
        ks.run("BITCOUNT", "s", "1").expect.to.equal(i(22)); // "oobar"
        ks.run("BITCOUNT", "s", "1000").expect.to.equal(i(0));
        ks.run("BITCOUNT", "s", "-1").expect.to.equal(i(4)); // "r"
        ks.run("BITCOUNT", "s", "-2").expect.to.equal(i(7)); // "ar"
        ks.run("BITCOUNT", "s", "-1000").expect.to.equal(i(26)); // "foobar"
    }

    @("valkey.bitops.bitcount_start_end")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        ks.run("SET", "s", "foobar");
        // byte ranges
        ks.run("BITCOUNT", "s", "0", "-1").expect.to.equal(i(26)); // foobar
        ks.run("BITCOUNT", "s", "1", "-2").expect.to.equal(i(18)); // ooba
        ks.run("BITCOUNT", "s", "-2", "1").expect.to.equal(i(0));
        ks.run("BITCOUNT", "s", "-1000", "0").expect.to.equal(i(4)); // f
        ks.run("BITCOUNT", "s", "0", "1000").expect.to.equal(i(26)); // foobar
        ks.run("BITCOUNT", "s", "-1000", "1000").expect.to.equal(i(26));

        // bit ranges (all resolve to the same sub-window as the byte equivalents)
        ks.run("BITCOUNT", "s", "0", "-1", "bit").expect.to.equal(i(26));
        ks.run("BITCOUNT", "s", "10", "14", "bit").expect.to.equal(i(4));
        ks.run("BITCOUNT", "s", "3", "14", "bit").expect.to.equal(i(7));
        ks.run("BITCOUNT", "s", "3", "29", "bit").expect.to.equal(i(16));
        ks.run("BITCOUNT", "s", "10", "-34", "bit").expect.to.equal(i(4)); // == 10..14
        ks.run("BITCOUNT", "s", "3", "-34", "bit").expect.to.equal(i(7)); // == 3..14
        ks.run("BITCOUNT", "s", "3", "-19", "bit").expect.to.equal(i(16)); // == 3..29
        ks.run("BITCOUNT", "s", "-2", "1", "bit").expect.to.equal(i(0));
        ks.run("BITCOUNT", "s", "-1000", "14", "bit").expect.to.equal(i(9)); // == 0..14
        ks.run("BITCOUNT", "s", "0", "1000", "bit").expect.to.equal(i(26));
        ks.run("BITCOUNT", "s", "-1000", "1000", "bit").expect.to.equal(i(26));
    }

    @("valkey.bitops.bitcount_illegal_args")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        // syntax error even for a non-existing key (args validated first)
        ks.run("DEL", "s");
        ks.run("BITCOUNT", "s", "0", "1", "hello").expect.to.equal("-ERR syntax error\r\n");
        ks.run("BITCOUNT", "s", "0", "1", "hello", "hello2").expect.to.equal("-ERR syntax error\r\n");
        ks.run("SET", "s", "1");
        ks.run("BITCOUNT", "s", "0", "1", "hello").expect.to.equal("-ERR syntax error\r\n");
        ks.run("BITCOUNT", "s", "0", "1", "hello", "hello2").expect.to.equal("-ERR syntax error\r\n");
    }

    @("valkey.bitops.bitcount_non_integer")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        // against existing key
        ks.run("SET", "s", "1");
        ks.run("BITCOUNT", "s", "a", "b")
            .expect.to.equal("-ERR value is not an integer or out of range\r\n");
        // against non existing key (args validated before key lookup)
        ks.run("DEL", "s");
        ks.run("BITCOUNT", "s", "a", "b")
            .expect.to.equal("-ERR value is not an integer or out of range\r\n");
        // against wrong type -> still the arg error (validated first)
        ks.run("LPUSH", "s", "a", "b", "c");
        ks.run("BITCOUNT", "s", "a", "b")
            .expect.to.equal("-ERR value is not an integer or out of range\r\n");
    }

    @("valkey.bitops.bitcount_misaligned")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        // github issue #582: huge byte end clamps, returns 1 (no error on this build)
        ks.run("DEL", "foo");
        ks.run("SETBIT", "foo", "0", "1");
        ks.run("BITCOUNT", "foo", "0", "4294967296").expect.to.equal(i(1));

        // misaligned prefix
        ks.run("DEL", "str");
        ks.run("SET", "str", "ab");
        ks.run("BITCOUNT", "str", "1", "-1").expect.to.equal(i(3));

        // misaligned prefix + full words + remainder
        ks.run("DEL", "str");
        ks.run("SET", "str", "__PPxxxxxxxxxxxxxxxxRR__");
        ks.run("BITCOUNT", "str", "2", "-3").expect.to.equal(i(74));
    }

    // ------------------------------------------------------------------- BITOP

    @("valkey.bitops.bitop_not")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        // NOT of empty string -> 0-length result deletes dest (oracle: GET dest = nil)
        ks.run("SET", "s", "");
        ks.run("BITOP", "not", "dest", "s").expect.to.equal(i(0));
        ks.run("GET", "dest").expect.to.equal(NIL);

        // NOT of a known string
        ks.run("SET", "s", "\xaa\x00\xff\x55");
        ks.run("BITOP", "not", "dest", "s").expect.to.equal(i(4));
        ks.run("GET", "dest").expect.to.equal(bulk("\x55\xff\x00\xaa"));

        // dest and target are the same key
        ks.run("SET", "same", "\xaa\x00\xff\x55");
        ks.run("BITOP", "not", "same", "same").expect.to.equal(i(4));
        ks.run("GET", "same").expect.to.equal(bulk("\x55\xff\x00\xaa"));
    }

    @("valkey.bitops.bitop_single_input")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        // AND|OR|XOR don't change the string with a single input key
        ks.run("SET", "a", "\x01\x02\xff");
        ks.run("BITOP", "and", "res1", "a").expect.to.equal(i(3));
        ks.run("BITOP", "or", "res2", "a").expect.to.equal(i(3));
        ks.run("BITOP", "xor", "res3", "a").expect.to.equal(i(3));
        ks.run("GET", "res1").expect.to.equal(bulk("\x01\x02\xff"));
        ks.run("GET", "res2").expect.to.equal(bulk("\x01\x02\xff"));
        ks.run("GET", "res3").expect.to.equal(bulk("\x01\x02\xff"));
    }

    @("valkey.bitops.bitop_missing_key_is_zero")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        // a missing key is treated as a stream of zero bytes
        ks.run("SET", "a", "\x01\x02\xff");
        ks.run("BITOP", "and", "res1", "no-such-key", "a").expect.to.equal(i(3));
        ks.run("BITOP", "or", "res2", "no-such-key", "a", "no-such-key2").expect.to.equal(i(3));
        ks.run("BITOP", "xor", "res3", "no-such-key", "a").expect.to.equal(i(3));
        ks.run("GET", "res1").expect.to.equal(bulk("\x00\x00\x00"));
        ks.run("GET", "res2").expect.to.equal(bulk("\x01\x02\xff"));
        ks.run("GET", "res3").expect.to.equal(bulk("\x01\x02\xff"));
    }

    @("valkey.bitops.bitop_zero_padding")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        // shorter keys are zero-padded to the max length key
        ks.run("SET", "a", "\x01\x02\xff\xff");
        ks.run("SET", "b", "\x01\x02\xff");
        ks.run("BITOP", "and", "res1", "a", "b").expect.to.equal(i(4));
        ks.run("BITOP", "or", "res2", "a", "b").expect.to.equal(i(4));
        ks.run("BITOP", "xor", "res3", "a", "b").expect.to.equal(i(4));
        ks.run("GET", "res1").expect.to.equal(bulk("\x01\x02\xff\x00"));
        ks.run("GET", "res2").expect.to.equal(bulk("\x01\x02\xff\xff"));
        ks.run("GET", "res3").expect.to.equal(bulk("\x00\x00\x00\xff"));
    }

    @("valkey.bitops.bitop_int_encoded")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        // integer-encoded source objects: "1" xor "2" xor "1" = "2"
        ks.run("SET", "a", "1");
        ks.run("SET", "b", "2");
        ks.run("BITOP", "xor", "dest", "a", "b", "a").expect.to.equal(i(1));
        ks.run("GET", "dest").expect.to.equal(bulk("2"));
    }

    @("valkey.bitops.bitop_wrongtype")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        // non-string source key -> WRONGTYPE
        ks.run("DEL", "c");
        ks.run("SET", "a", "1");
        ks.run("SET", "b", "2");
        ks.run("LPUSH", "c", "foo");
        ks.run("BITOP", "xor", "dest", "a", "b", "c", "d").startsWith("-WRONGTYPE").should.equal(true);
    }

    @("valkey.bitops.bitop_empty_after_nonempty")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        // issue #529: empty (missing) source key after a non-empty one -> length 32
        ks.run("FLUSHDB");
        ks.run("SET", "a",
                "\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"
                ~ "\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00");
        ks.run("BITOP", "or", "x", "a", "b").expect.to.equal(i(32));
    }

    // ------------------------------------------------------------------ BITPOS

    @("valkey.bitops.bitpos_wrongtype")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        ks.run("DEL", "mylist");
        ks.run("LPUSH", "mylist", "a", "b", "c");
        ks.run("BITPOS", "mylist", "0").startsWith("-WRONGTYPE").should.equal(true);
        ks.run("BITPOS", "mylist", "1", "10", "100").startsWith("-WRONGTYPE").should.equal(true);
    }

    @("valkey.bitops.bitpos_illegal_args")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        ks.run("DEL", "s");
        ks.run("BITPOS", "s", "0", "1", "hello", "hello2").expect.to.equal("-ERR syntax error\r\n");
        ks.run("BITPOS", "s", "0", "0", "1", "hello").expect.to.equal("-ERR syntax error\r\n");
        ks.run("SET", "s", "1");
        ks.run("BITPOS", "s", "0", "1", "hello", "hello2").expect.to.equal("-ERR syntax error\r\n");
        ks.run("BITPOS", "s", "0", "0", "1", "hello").expect.to.equal("-ERR syntax error\r\n");
    }

    @("valkey.bitops.bitpos_non_integer")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        // against existing key
        ks.run("SET", "s", "1");
        ks.run("BITPOS", "s", "a").expect.to.equal("-ERR value is not an integer or out of range\r\n");
        ks.run("BITPOS", "s", "0", "a", "b").expect.to.equal("-ERR value is not an integer or out of range\r\n");
        // against non existing key
        ks.run("DEL", "s");
        ks.run("BITPOS", "s", "b").expect.to.equal("-ERR value is not an integer or out of range\r\n");
        ks.run("BITPOS", "s", "0", "a", "b").expect.to.equal("-ERR value is not an integer or out of range\r\n");
        // against wrong type -> still the arg error (validated first)
        ks.run("LPUSH", "s", "a", "b", "c");
        ks.run("BITPOS", "s", "a").expect.to.equal("-ERR value is not an integer or out of range\r\n");
        ks.run("BITPOS", "s", "1", "a", "b").expect.to.equal("-ERR value is not an integer or out of range\r\n");
    }

    @("valkey.bitops.bitpos_empty_key")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        // bit=0 with empty key -> 0
        ks.run("DEL", "str");
        ks.run("BITPOS", "str", "0").expect.to.equal(i(0));
        ks.run("BITPOS", "str", "0", "0", "-1", "bit").expect.to.equal(i(0));
        // bit=1 with empty key -> -1
        ks.run("BITPOS", "str", "1").expect.to.equal(i(-1));
        ks.run("BITPOS", "str", "1", "0", "-1").expect.to.equal(i(-1));
    }

    @("valkey.bitops.bitpos_less_than_one_word")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        // bit=0 in a sub-word string
        ks.run("SET", "str", "\xff\xf0\x00");
        ks.run("BITPOS", "str", "0").expect.to.equal(i(12));
        ks.run("BITPOS", "str", "0", "0", "-1", "bit").expect.to.equal(i(12));
        // bit=0 starting at unaligned address
        ks.run("BITPOS", "str", "0", "1").expect.to.equal(i(12));
        ks.run("BITPOS", "str", "0", "1", "-1", "bit").expect.to.equal(i(12));

        // bit=1 in a sub-word string
        ks.run("SET", "str", "\x00\x0f\x00");
        ks.run("BITPOS", "str", "1").expect.to.equal(i(12));
        ks.run("BITPOS", "str", "1", "0", "-1", "bit").expect.to.equal(i(12));

        // bit=1 starting at unaligned address
        ks.run("SET", "str", "\x00\x0f\xff");
        ks.run("BITPOS", "str", "1", "1").expect.to.equal(i(12));
        ks.run("BITPOS", "str", "1", "1", "-1", "bit").expect.to.equal(i(12));
    }

    @("valkey.bitops.bitpos_unaligned_full_word_remainder")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        // 3-byte prefix + three 8-byte words of 0xff + a 0x0f byte: first zero @216
        ks.run("DEL", "str");
        ks.run("SET", "str", "\xff\xff\xff");
        ks.run("APPEND", "str", "\xff\xff\xff\xff\xff\xff\xff\xff");
        ks.run("APPEND", "str", "\xff\xff\xff\xff\xff\xff\xff\xff");
        ks.run("APPEND", "str", "\xff\xff\xff\xff\xff\xff\xff\xff");
        ks.run("APPEND", "str", "\x0f");
        foreach (start; 0 .. 9)
            ks.run("BITPOS", "str", "0", start.to!string).expect.to.equal(i(216));
        foreach (start; [1, 9, 17, 25, 33, 41, 49, 57, 65])
            ks.run("BITPOS", "str", "0", start.to!string, "-1", "bit").expect.to.equal(i(216));

        // mirror image: zeros + 0xf0 -> first one bit @216
        ks.run("DEL", "str");
        ks.run("SET", "str", "\x00\x00\x00");
        ks.run("APPEND", "str", "\x00\x00\x00\x00\x00\x00\x00\x00");
        ks.run("APPEND", "str", "\x00\x00\x00\x00\x00\x00\x00\x00");
        ks.run("APPEND", "str", "\x00\x00\x00\x00\x00\x00\x00\x00");
        ks.run("APPEND", "str", "\xf0");
        foreach (start; 0 .. 9)
            ks.run("BITPOS", "str", "1", start.to!string).expect.to.equal(i(216));
        foreach (start; [1, 9, 17, 25, 33, 41, 49, 57, 65])
            ks.run("BITPOS", "str", "1", start.to!string, "-1", "bit").expect.to.equal(i(216));
    }

    @("valkey.bitops.bitpos_all_zero")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        // bit=1 returns -1 if the string is all 0 bits (of any length)
        ks.run("SET", "str", "");
        foreach (n; 0 .. 20)
        {
            ks.run("BITPOS", "str", "1").expect.to.equal(i(-1));
            ks.run("BITPOS", "str", "1", "0", "-1", "bit").expect.to.equal(i(-1));
            ks.run("APPEND", "str", "\x00");
        }
    }

    @("valkey.bitops.bitpos_intervals")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        // bit=0 with intervals over "\x00\xff\x00"
        ks.run("SET", "str", "\x00\xff\x00");
        ks.run("BITPOS", "str", "0", "0", "-1").expect.to.equal(i(0));
        ks.run("BITPOS", "str", "0", "1", "-1").expect.to.equal(i(16));
        ks.run("BITPOS", "str", "0", "2", "-1").expect.to.equal(i(16));
        ks.run("BITPOS", "str", "0", "2", "200").expect.to.equal(i(16));
        ks.run("BITPOS", "str", "0", "1", "1").expect.to.equal(i(-1));

        ks.run("BITPOS", "str", "0", "0", "-1", "bit").expect.to.equal(i(0));
        ks.run("BITPOS", "str", "0", "8", "-1", "bit").expect.to.equal(i(16));
        ks.run("BITPOS", "str", "0", "16", "-1", "bit").expect.to.equal(i(16));
        ks.run("BITPOS", "str", "0", "16", "200", "bit").expect.to.equal(i(16));
        ks.run("BITPOS", "str", "0", "8", "8", "bit").expect.to.equal(i(-1));

        // bit=1 with intervals over the same string
        ks.run("BITPOS", "str", "1", "0", "-1").expect.to.equal(i(8));
        ks.run("BITPOS", "str", "1", "1", "-1").expect.to.equal(i(8));
        ks.run("BITPOS", "str", "1", "2", "-1").expect.to.equal(i(-1));
        ks.run("BITPOS", "str", "1", "2", "200").expect.to.equal(i(-1));
        ks.run("BITPOS", "str", "1", "1", "1").expect.to.equal(i(8));

        ks.run("BITPOS", "str", "1", "0", "-1", "bit").expect.to.equal(i(8));
        ks.run("BITPOS", "str", "1", "8", "-1", "bit").expect.to.equal(i(8));
        ks.run("BITPOS", "str", "1", "16", "-1", "bit").expect.to.equal(i(-1));
        ks.run("BITPOS", "str", "1", "16", "200", "bit").expect.to.equal(i(-1));
        ks.run("BITPOS", "str", "1", "8", "8", "bit").expect.to.equal(i(8));
    }

    @("valkey.bitops.bitpos_end_given")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        // bit=0 changes behavior once an explicit end is given (all-ones string)
        ks.run("SET", "str", "\xff\xff\xff");
        ks.run("BITPOS", "str", "0").expect.to.equal(i(24)); // virtual zero past end
        ks.run("BITPOS", "str", "0", "0").expect.to.equal(i(24)); // start only: still virtual zero
        ks.run("BITPOS", "str", "0", "0", "-1").expect.to.equal(i(-1)); // explicit end: no zero
        ks.run("BITPOS", "str", "0", "0", "-1", "bit").expect.to.equal(i(-1));
    }
}
