module tests.valkey_hash_tests;

// Valkey unit/type/hash.tcl ported to native in-process UT (the "contrabando":
// bring Valkey's test COVERAGE into `dub test`, re-expressed against dreads'
// dispatch — NOT a copy of the tcl, and no server needed). Valkey is BSD-3; the
// scenarios are credited in THIRD_PARTY_NOTICES. Expected reply bytes were
// grounded against a live valkey-server oracle.
//
// SKIPPED (out of scope for an in-process semantic sweep): encoding-conversion
// duplicates (listpack<->hashtable via config / assert_encoding / DEBUG OBJECT —
// the small-container UTs cover those); fuzzing / random loops; DUMP/RESTORE
// roundtrips; DEBUG *, memory_usage, OBJECT ENCODING; RESP3 HELLO shape (harness
// dispatches RESP2). HGETALL/HKEYS/HVALS and HRANDFIELD (unordered) are compared
// as sorted multisets.

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
            immutable e = s[i .. $].indexOf("\r\n") + i;
            immutable len = s[i + 1 .. e].to!int;
            i = e + 2;
            if (len < 0)
            {
                r ~= null;
                continue;
            }
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

    private size_t arrLen(string reply)
    {
        return parseArr(reply).length;
    }

    enum NIL = "$-1\r\n";

    // -------------------------------------------------------------------------

    @("valkey.hash.set_get")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        // HSET returns the count of NEW fields (update vs insert)
        ks.run("HSET", "h", "f1", "a", "f2", "b").expect.to.equal(":2\r\n");
        ks.run("HSET", "h", "f1", "x", "f3", "c").expect.to.equal(":1\r\n"); // only f3 new
        ks.run("HLEN", "h").expect.to.equal(":3\r\n");

        // HGET update reflected; missing field & missing key -> nil bulk
        ks.run("HGET", "h", "f1").expect.to.equal(bulk("x"));
        ks.run("HGET", "h", "missing").expect.to.equal(NIL);
        ks.run("HGET", "nokey", "f").expect.to.equal(NIL);

        // HSET single-field update returns 0, insert returns 1
        ks.run("HSET", "h", "f1", "newval1").expect.to.equal(":0\r\n");
        ks.run("HGET", "h", "f1").expect.to.equal(bulk("newval1"));
        ks.run("HSET", "h", "__foobar123__", "newval").expect.to.equal(":1\r\n");
        ks.run("HDEL", "h", "__foobar123__").expect.to.equal(":1\r\n");
    }

    @("valkey.hash.mget_mset")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        ks.run("HMSET", "h", "f1", "v1", "f2", "v2", "f3", "v3").expect.to.equal("+OK\r\n");

        // HMGET returns values in requested order, nil for absent fields
        ks.run("HMGET", "h", "f1", "missing", "f2").expect.to.equal(
                "*3\r\n" ~ bulk("v1") ~ NIL ~ bulk("v2"));

        // HMGET against a non-existing key -> all nils
        ks.run("HMGET", "doesntexist", "__1__", "__2__").expect.to.equal(
                "*2\r\n" ~ NIL ~ NIL);
        // HMGET against existing key, non-existing fields -> all nils
        ks.run("HMGET", "h", "__1__", "__2__").expect.to.equal("*2\r\n" ~ NIL ~ NIL);
    }

    @("valkey.hash.setnx")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        // target key missing -> set, returns 1
        ks.run("HSETNX", "h", "__123__", "foo").expect.to.equal(":1\r\n");
        ks.run("HGET", "h", "__123__").expect.to.equal(bulk("foo"));
        // target key exists -> unchanged, returns 0
        ks.run("HSETNX", "h", "__123__", "bar").expect.to.equal(":0\r\n");
        ks.run("HGET", "h", "__123__").expect.to.equal(bulk("foo"));
    }

    @("valkey.hash.exists_strlen")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        ks.run("HSET", "h", "a", "1", "b", "22", "c", "333");
        ks.run("HEXISTS", "h", "a").expect.to.equal(":1\r\n");
        ks.run("HEXISTS", "h", "nokey").expect.to.equal(":0\r\n");

        ks.run("HSTRLEN", "h", "a").expect.to.equal(":1\r\n");
        ks.run("HSTRLEN", "h", "b").expect.to.equal(":2\r\n");
        ks.run("HSTRLEN", "h", "c").expect.to.equal(":3\r\n");
        // non-existing field -> 0
        ks.run("HSTRLEN", "h", "__123__").expect.to.equal(":0\r\n");

        // HSTRLEN corner cases: length of stored representation
        ks.run("HSET", "h", "f", "-9223372036854775808");
        ks.run("HSTRLEN", "h", "f").expect.to.equal(":20\r\n");
        ks.run("HSET", "h", "f", "9223372036854775807");
        ks.run("HSTRLEN", "h", "f").expect.to.equal(":19\r\n");
        ks.run("HSET", "h", "f", "9223372036854775808");
        ks.run("HSTRLEN", "h", "f").expect.to.equal(":19\r\n");
        ks.run("HSET", "h", "f", "");
        ks.run("HSTRLEN", "h", "f").expect.to.equal(":0\r\n");
        ks.run("HSET", "h", "f", "0");
        ks.run("HSTRLEN", "h", "f").expect.to.equal(":1\r\n");
        ks.run("HSET", "h", "f", "-1");
        ks.run("HSTRLEN", "h", "f").expect.to.equal(":2\r\n");
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
        // against a non-existing key -> empty array
        ks.run("HGETALL", "htest").expect.to.equal("*0\r\n");
        ks.run("HKEYS", "htest").expect.to.equal("*0\r\n");
        ks.run("HVALS", "htest").expect.to.equal("*0\r\n");
    }

    @("valkey.hash.hdel")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        // HDEL return value: no-key field -> 0; existing -> 1; already-gone -> 0
        ks.run("HSET", "sh", "a", "1", "b", "2");
        ks.run("HDEL", "sh", "nokey").expect.to.equal(":0\r\n");
        ks.run("HDEL", "sh", "a").expect.to.equal(":1\r\n");
        ks.run("HDEL", "sh", "a").expect.to.equal(":0\r\n");
        ks.run("HGET", "sh", "a").expect.to.equal(NIL);

        // HDEL of more than a single value: count only the ones that existed
        ks.run("DEL", "myhash");
        ks.run("HMSET", "myhash", "a", "1", "b", "2", "c", "3");
        ks.run("HDEL", "myhash", "x", "y").expect.to.equal(":0\r\n");
        ks.run("HDEL", "myhash", "a", "c", "f").expect.to.equal(":2\r\n");
        sameHash(ks.run("HGETALL", "myhash"), "b", "2");

        // hash becomes empty before deleting all specified fields -> key removed
        ks.run("DEL", "myhash");
        ks.run("HMSET", "myhash", "a", "1", "b", "2", "c", "3");
        ks.run("HDEL", "myhash", "a", "b", "c", "d", "e").expect.to.equal(":3\r\n");
        ks.run("EXISTS", "myhash").expect.to.equal(":0\r\n");
    }

    @("valkey.hash.getdel")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        // single field: returns the value, removes it, key drops when empty
        ks.run("HSET", "myhash", "field1", "value1");
        ks.run("HGETDEL", "myhash", "FIELDS", "1", "field1").expect.to.equal(arrB("value1"));
        ks.run("HEXISTS", "myhash", "field1").expect.to.equal(":0\r\n");
        ks.run("EXISTS", "myhash").expect.to.equal(":0\r\n");

        // multiple fields: values in requested order
        ks.run("HMSET", "myhash", "f1", "v1", "f2", "v2", "f3", "v3");
        ks.run("HGETDEL", "myhash", "FIELDS", "2", "f1", "f3").expect.to.equal(arrB("v1", "v3"));
        ks.run("HEXISTS", "myhash", "f1").expect.to.equal(":0\r\n");
        ks.run("HEXISTS", "myhash", "f2").expect.to.equal(":1\r\n");
        ks.run("HEXISTS", "myhash", "f3").expect.to.equal(":0\r\n");

        // non-existing field -> nil element, field kept
        ks.run("DEL", "myhash");
        ks.run("HSET", "myhash", "field1", "value1");
        ks.run("HGETDEL", "myhash", "FIELDS", "1", "nonexisting").expect.to.equal("*1\r\n" ~ NIL);
        ks.run("HEXISTS", "myhash", "field1").expect.to.equal(":1\r\n");

        // existing + non-existing field mix on a nonempty result
        ks.run("HGETDEL", "myhash", "FIELDS", "2", "field1", "field2").expect.to.equal(
                "*2\r\n" ~ bulk("value1") ~ NIL);

        // non-existing key -> single nil
        ks.run("DEL", "myhash");
        ks.run("HGETDEL", "myhash", "FIELDS", "1", "field1").expect.to.equal("*1\r\n" ~ NIL);

        // mix of existing and non-existing fields
        ks.run("DEL", "myhash");
        ks.run("HMSET", "myhash", "a", "1", "b", "2", "c", "3");
        ks.run("HGETDEL", "myhash", "FIELDS", "3", "a", "nonexist", "b").expect.to.equal(
                "*3\r\n" ~ bulk("1") ~ NIL ~ bulk("2"));
        ks.run("HEXISTS", "myhash", "a").expect.to.equal(":0\r\n");
        ks.run("HEXISTS", "myhash", "b").expect.to.equal(":0\r\n");
        ks.run("HEXISTS", "myhash", "c").expect.to.equal(":1\r\n");

        // hash becomes empty after deletion -> key removed
        ks.run("DEL", "myhash");
        ks.run("HMSET", "myhash", "a", "1", "b", "2");
        ks.run("HGETDEL", "myhash", "FIELDS", "2", "a", "b").expect.to.equal(arrB("1", "2"));
        ks.run("EXISTS", "myhash").expect.to.equal(":0\r\n");
    }

    @("valkey.hash.getdel_errors")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        ks.run("HSET", "myhash", "a", "1");
        // numfields not an integer
        ks.run("HGETDEL", "myhash", "FIELDS", "a", "b", "c").startsWith("-ERR").should.equal(true);
        // numfields does not match the provided number of fields
        ks.run("HGETDEL", "myhash", "FIELDS", "2", "a", "b", "c").startsWith("-ERR").should.equal(
                true);
        ks.run("HGETDEL", "myhash", "FIELDS", "4", "a", "b", "c").startsWith("-ERR").should.equal(
                true);
        // wrong number of arguments
        ks.run("HGETDEL", "myhash").startsWith("-ERR").should.equal(true);

        // wrong type
        ks.run("SET", "wrongtype", "somevalue");
        ks.run("HGETDEL", "wrongtype", "FIELDS", "1", "field1").startsWith("-WRONGTYPE")
            .should.equal(true);
    }

    @("valkey.hash.incrby")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        // against a non-existing database key -> creates hash, returns increment
        ks.run("HINCRBY", "htest", "foo", "2").expect.to.equal(":2\r\n");

        // against a non-existing hash field -> starts from 0
        ks.run("HDEL", "sh", "tmp");
        ks.run("HINCRBY", "sh", "tmp", "2").expect.to.equal(":2\r\n");
        ks.run("HGET", "sh", "tmp").expect.to.equal(bulk("2"));
        // against field created by hincrby itself
        ks.run("HINCRBY", "sh", "tmp", "3").expect.to.equal(":5\r\n");
        ks.run("HGET", "sh", "tmp").expect.to.equal(bulk("5"));

        // against field originally set with HSET
        ks.run("HSET", "sh", "tmp", "100");
        ks.run("HINCRBY", "sh", "tmp", "2").expect.to.equal(":102\r\n");

        // over a 32-bit value, and with an over-32-bit increment
        ks.run("HSET", "sh", "tmp", "17179869184");
        ks.run("HINCRBY", "sh", "tmp", "1").expect.to.equal(":17179869185\r\n");
        ks.run("HSET", "sh", "tmp", "17179869184");
        ks.run("HINCRBY", "sh", "tmp", "17179869184").expect.to.equal(":34359738368\r\n");

        // detect overflow
        ks.run("HSET", "hash", "n", "-9223372036854775484");
        ks.run("HINCRBY", "hash", "n", "-1").expect.to.equal(":-9223372036854775485\r\n");
        ks.run("HINCRBY", "hash", "n", "-10000").startsWith("-ERR").should.equal(true);
    }

    @("valkey.hash.incrby_errors")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        // non-integer increment value
        ks.run("HSET", "incrhash", "field", "5");
        ks.run("HINCRBY", "incrhash", "field", "v").startsWith("-ERR").should.equal(true);
        ks.run("HINCRBYFLOAT", "incrhash", "field", "v").startsWith("-ERR").should.equal(true);

        // fails against a hash value with spaces (left / right)
        ks.run("HSET", "sh", "str", " 11");
        ks.run("HINCRBY", "sh", "str", "1").startsWith("-ERR").should.equal(true);
        ks.run("HSET", "sh", "str", "11 ");
        ks.run("HINCRBY", "sh", "str", "1").startsWith("-ERR").should.equal(true);
    }

    @("valkey.hash.incrbyfloat")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        // against a non-existing database key
        ks.run("HINCRBYFLOAT", "htest", "foo", "2.5").expect.to.equal(bulk("2.5"));

        // against a non-existing hash field -> starts from 0
        ks.run("HDEL", "sh", "tmp");
        ks.run("HINCRBYFLOAT", "sh", "tmp", "2.5").expect.to.equal(bulk("2.5"));
        ks.run("HGET", "sh", "tmp").expect.to.equal(bulk("2.5"));
        // field created by hincrbyfloat itself
        ks.run("HINCRBYFLOAT", "sh", "tmp", "3.5").expect.to.equal(bulk("6"));

        // field originally set with HSET
        ks.run("HSET", "sh", "tmp", "100");
        ks.run("HINCRBYFLOAT", "sh", "tmp", "2.5").expect.to.equal(bulk("102.5"));

        // over 32-bit value / over-32-bit increment
        ks.run("HSET", "sh", "tmp", "17179869184");
        ks.run("HINCRBYFLOAT", "sh", "tmp", "1").expect.to.equal(bulk("17179869185"));
        ks.run("HSET", "sh", "tmp", "17179869184");
        ks.run("HINCRBYFLOAT", "sh", "tmp", "17179869184").expect.to.equal(bulk("34359738368"));

        // correct float representation (issue #2846)
        ks.run("DEL", "myhash");
        ks.run("HINCRBYFLOAT", "myhash", "float", "1.23").expect.to.equal(bulk("1.23"));
        ks.run("HINCRBYFLOAT", "myhash", "float", "0.77").expect.to.equal(bulk("2"));
        ks.run("HINCRBYFLOAT", "myhash", "float", "-0.1").expect.to.equal(bulk("1.9"));
    }

    @("valkey.hash.incrbyfloat_errors")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        // fails against a hash value with spaces (left / right)
        ks.run("HSET", "sh", "str", " 11");
        ks.run("HINCRBYFLOAT", "sh", "str", "1").startsWith("-ERR").should.equal(true);
        ks.run("HSET", "sh", "str", "11 ");
        ks.run("HINCRBYFLOAT", "sh", "str", "1").startsWith("-ERR").should.equal(true);

        // fails against a non-float hash value
        ks.run("HSET", "h2", "field", "abc");
        ks.run("HINCRBYFLOAT", "h2", "field", "1").startsWith("-ERR").should.equal(true);

        // does not allow NaN or Infinity; key is not created
        ks.run("HINCRBYFLOAT", "hfoo", "field", "+inf").startsWith("-ERR").should.equal(true);
        ks.run("EXISTS", "hfoo").expect.to.equal(":0\r\n");
    }

    @("valkey.hash.randfield")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        ks.run("HSET", "myhash", "a", "1", "b", "2", "c", "3");

        // no-count form: a single existing field (bulk string)
        auto one = ks.run("HRANDFIELD", "myhash");
        one.startsWith("$").should.equal(true);

        // count == 0 -> empty array
        ks.run("HRANDFIELD", "myhash", "0").expect.to.equal("*0\r\n");

        // count against a non-existing key -> empty array
        ks.run("HRANDFIELD", "nonexisting_key", "100").expect.to.equal("*0\r\n");
        ks.run("HRANDFIELD", "nonexisting_key", "100", "WITHVALUES").expect.to.equal("*0\r\n");

        // positive count >= hash size -> all fields exactly once (unique)
        sameSet(ks.run("HRANDFIELD", "myhash", "3"), "a", "b", "c");
        sameSet(ks.run("HRANDFIELD", "myhash", "5"), "a", "b", "c");
        // WITHVALUES at >= size -> the whole hash (flat [f,v,...])
        sameHash(ks.run("HRANDFIELD", "myhash", "5", "WITHVALUES"), "a", "1", "b", "2", "c", "3");

        // positive count < hash size -> exactly `count` unique fields
        arrLen(ks.run("HRANDFIELD", "myhash", "2")).expect.to.equal(2);
        arrLen(ks.run("HRANDFIELD", "myhash", "2", "WITHVALUES")).expect.to.equal(4);

        // negative count -> exactly |count| elements (may repeat)
        arrLen(ks.run("HRANDFIELD", "myhash", "-20")).expect.to.equal(20);
        arrLen(ks.run("HRANDFIELD", "myhash", "-20", "WITHVALUES")).expect.to.equal(40);
        // every returned field belongs to the hash
        foreach (f; parseArr(ks.run("HRANDFIELD", "myhash", "-10")))
            ((f == "a") || (f == "b") || (f == "c")).should.equal(true);
    }

    @("valkey.hash.randfield_errors")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        ks.run("HSET", "myhash", "a", "1");
        // count overflow -> value is out of range
        ks.run("HRANDFIELD", "myhash", "-9223372036854775808", "WITHVALUES")
            .startsWith("-ERR").should.equal(true);
        ks.run("HRANDFIELD", "myhash", "-9223372036854775808").startsWith("-ERR").should.equal(true);
    }

    @("valkey.hash.arity")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        // HSET / HMSET with a dangling field (no value) -> wrong number of arguments
        ks.run("HSET", "smallhash", "key1", "val1", "key2").startsWith("-ERR").should.equal(true);
        ks.run("HMSET", "smallhash", "key1", "val1", "key2").startsWith("-ERR").should.equal(true);
    }

    @("valkey.hash.wrongtype")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        ks.run("SET", "wrongtype", "somevalue");
        // every hash command against a string key -> WRONGTYPE
        ks.run("HMGET", "wrongtype", "f1", "f2").startsWith("-WRONGTYPE").should.equal(true);
        ks.run("HRANDFIELD", "wrongtype").startsWith("-WRONGTYPE").should.equal(true);
        ks.run("HGET", "wrongtype", "f1").startsWith("-WRONGTYPE").should.equal(true);
        ks.run("HGETALL", "wrongtype").startsWith("-WRONGTYPE").should.equal(true);
        ks.run("HDEL", "wrongtype", "f1").startsWith("-WRONGTYPE").should.equal(true);
        ks.run("HINCRBY", "wrongtype", "f1", "2").startsWith("-WRONGTYPE").should.equal(true);
        ks.run("HINCRBYFLOAT", "wrongtype", "f1", "2.5").startsWith("-WRONGTYPE").should.equal(true);
        ks.run("HSTRLEN", "wrongtype", "f1").startsWith("-WRONGTYPE").should.equal(true);
        ks.run("HVALS", "wrongtype").startsWith("-WRONGTYPE").should.equal(true);
        ks.run("HKEYS", "wrongtype").startsWith("-WRONGTYPE").should.equal(true);
        ks.run("HEXISTS", "wrongtype", "f1").startsWith("-WRONGTYPE").should.equal(true);
        ks.run("HSET", "wrongtype", "f1", "val1").startsWith("-WRONGTYPE").should.equal(true);
        ks.run("HMSET", "wrongtype", "f1", "v1", "f2", "v2").startsWith("-WRONGTYPE").should.equal(
                true);
        ks.run("HSETNX", "wrongtype", "f1", "val1").startsWith("-WRONGTYPE").should.equal(true);
        ks.run("HLEN", "wrongtype").startsWith("-WRONGTYPE").should.equal(true);
        ks.run("HGETDEL", "wrongtype", "FIELDS", "1", "f1").startsWith("-WRONGTYPE").should.equal(
                true);
    }

    @("valkey.hash.large_keys")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        // regression for large field names: distinct large keys resolve independently
        string k1;
        foreach (_; 0 .. 300)
            k1 ~= "k";
        string k2 = k1 ~ "X"; // differs in length
        ks.run("HSET", "hash", k1, "a");
        ks.run("HSET", "hash", k2, "b");
        ks.run("HGET", "hash", k1).expect.to.equal(bulk("a"));
        ks.run("HGET", "hash", k2).expect.to.equal(bulk("b"));
    }
}
