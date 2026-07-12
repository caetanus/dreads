module tests.blackbox_regressions_tests;

// Internal coverage for behaviors first caught by the Valkey blackbox suite.
// Scenarios are OURS (never copied from the tcl suite); each named test cites
// the failure class it pins down. Server-layer behaviors the dispatch harness
// can't reach (blocking wake, CONFIG SET side effects, CONFIG INFO) stay
// blackbox-only — see BLACKBOX-TODO.md.

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

    @("blackbox.help_replies")
    unittest
    {
        // "Coverage: HELP commands" class: every container command answers
        // HELP with the "<CMD> <subcommand> ..." header line
        Keyspace ks;
        scope (exit)
            ks.d.free();
        foreach (cmd; ["OBJECT", "MEMORY", "SLOWLOG", "COMMANDLOG", "COMMAND",
                "FUNCTION", "MODULE"])
            ks.run(cmd, "HELP").expect.to.contain(cmd ~ " <subcommand> ");
    }

    @("blackbox.rand_commands_cover_all_members")
    unittest
    {
        // RANDFIELD/RANDMEMBER randomness class: repeated single draws must
        // reach every member (the old code always returned the first slot).
        // P(miss after 60 draws of 3) ~ (2/3)^60: effectively impossible.
        Keyspace ks;
        scope (exit)
            ks.d.free();
        ks.run("SADD", "s", "a", "b", "c");
        ks.run("HSET", "h", "f1", "1", "f2", "2", "f3", "3");
        ks.run("ZADD", "z", "1", "m1", "2", "m2", "3", "m3");
        bool[string] seenS, seenH, seenZ;
        foreach (_; 0 .. 60)
        {
            seenS[ks.run("SRANDMEMBER", "s")[4 .. 5].idup] = true;
            seenH[ks.run("HRANDFIELD", "h")[4 .. 6].idup] = true;
            seenZ[ks.run("ZRANDMEMBER", "z")[4 .. 6].idup] = true;
        }
        seenS.length.expect.to.equal(3);
        seenH.length.expect.to.equal(3);
        seenZ.length.expect.to.equal(3);
        // distinct-count draws never repeat and never exceed the cardinality
        ks.run("SRANDMEMBER", "s", "2")[0 .. 4].expect.to.equal("*2\r\n");
        ks.run("SRANDMEMBER", "s", "9")[0 .. 4].expect.to.equal("*3\r\n");
        // WITHSCORES/WITHVALUES double the reply: count*2 must not overflow
        // (this crashed the server before the guard)
        ks.run("ZRANDMEMBER", "z", "-9223372036854770000", "WITHSCORES")
            .expect.to.equal("-ERR value is out of range\r\n");
        ks.run("HRANDFIELD", "h", "-9223372036854770000", "WITHVALUES")
            .expect.to.equal("-ERR value is out of range\r\n");
    }

    @("blackbox.resp3_pair_nesting")
    unittest
    {
        // RESP3 desync class: WITHSCORES/WITHVALUES and counted zset pops
        // nest [member, score] pairs under RESP3 (flat 2n under RESP2), and
        // scores go out as native doubles (",1")
        Keyspace ks;
        scope (exit)
        {
            gRespProto = 2;
            ks.d.free();
        }
        ks.run("ZADD", "z", "1", "a", "2", "b");
        ks.run("HSET", "h", "f", "v");

        gRespProto = 3;
        ks.run("ZRANGE", "z", "0", "-1", "WITHSCORES")
            .expect.to.equal("*2\r\n*2\r\n$1\r\na\r\n,1\r\n*2\r\n$1\r\nb\r\n,2\r\n");
        ks.run("ZUNION", "1", "z", "WITHSCORES")[0 .. 8].expect.to.equal("*2\r\n*2\r\n");
        ks.run("HRANDFIELD", "h", "1", "WITHVALUES")
            .expect.to.equal("*1\r\n*2\r\n$1\r\nf\r\n$1\r\nv\r\n");
        ks.run("ZRANDMEMBER", "z", "2", "WITHSCORES")[0 .. 8].expect.to.equal("*2\r\n*2\r\n");
        // counted pop nests; the plain single pop stays flat
        ks.run("ZPOPMIN", "z", "1").expect.to.equal("*1\r\n*2\r\n$1\r\na\r\n,1\r\n");
        ks.run("ZPOPMIN", "z").expect.to.equal("*2\r\n$1\r\nb\r\n,2\r\n");

        gRespProto = 2;
        ks.run("ZADD", "z", "1", "a", "2", "b");
        ks.run("ZRANGE", "z", "0", "-1", "WITHSCORES")
            .expect.to.equal("*4\r\n$1\r\na\r\n$1\r\n1\r\n$1\r\nb\r\n$1\r\n2\r\n");
        ks.run("ZPOPMIN", "z", "1").expect.to.equal("*2\r\n$1\r\na\r\n$1\r\n1\r\n");
    }

    @("blackbox.resp3_null_arrays")
    unittest
    {
        // RESP3 null class: "no data" replies are `_` under RESP3, `*-1`/`$-1`
        // under RESP2 (LPOP count, LMPOP, ZMPOP, GEOPOS holes)
        Keyspace ks;
        scope (exit)
        {
            gRespProto = 2;
            ks.d.free();
        }
        gRespProto = 3;
        ks.run("LPOP", "ghost", "2").expect.to.equal("_\r\n");
        ks.run("LMPOP", "1", "ghost", "LEFT").expect.to.equal("_\r\n");
        ks.run("ZMPOP", "1", "ghost", "MIN").expect.to.equal("_\r\n");
        gRespProto = 2;
        ks.run("LPOP", "ghost", "2").expect.to.equal("*-1\r\n");
        ks.run("LMPOP", "1", "ghost", "LEFT").expect.to.equal("*-1\r\n");
        ks.run("ZMPOP", "1", "ghost", "MIN").expect.to.equal("*-1\r\n");
    }

    @("blackbox.container_thresholds_follow_config")
    unittest
    {
        // encoding-threshold class: the small containers must obey the LIVE
        // config (the suite flips them via CONFIG SET), not baked-in defaults
        import dreads.config : gConfig;

        Keyspace ks;
        auto saved = gConfig;
        scope (exit)
        {
            gConfig = saved;
            ks.d.free();
        }
        gConfig.setMaxListpackValue = 8;
        ks.run("SADD", "s", "short", "muchlongerthan8");
        ks.run("OBJECT", "ENCODING", "s").expect.to.equal("$9\r\nhashtable\r\n");
        gConfig.setMaxListpackEntries = 0;
        ks.run("SADD", "s2", "x");
        ks.run("OBJECT", "ENCODING", "s2").expect.to.equal("$9\r\nhashtable\r\n");
        gConfig.hashMaxListpackEntries = 1;
        ks.run("HSET", "h", "a", "1", "b", "2");
        ks.run("OBJECT", "ENCODING", "h").expect.to.equal("$9\r\nhashtable\r\n");
        gConfig.zsetMaxListpackEntries = 1;
        ks.run("ZADD", "z", "1", "a", "2", "b");
        ks.run("OBJECT", "ENCODING", "z").expect.to.equal("$8\r\nskiplist\r\n");
    }

    @("blackbox.hgetdel")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();
        ks.run("HSET", "h", "f1", "v1", "f2", "v2");
        ks.run("HGETDEL", "h", "FIELDS", "2", "f1", "nope")
            .expect.to.equal("*2\r\n$2\r\nv1\r\n$-1\r\n");
        ks.run("HGET", "h", "f1").expect.to.equal("$-1\r\n"); // deleted
        ks.run("HGET", "h", "f2").expect.to.equal("$2\r\nv2\r\n"); // untouched
        // last field removed deletes the key
        ks.run("HGETDEL", "h", "FIELDS", "1", "f2").expect.to.equal("*1\r\n$2\r\nv2\r\n");
        ks.run("EXISTS", "h").expect.to.equal(":0\r\n");
        // missing key answers nulls
        ks.run("HGETDEL", "ghost", "FIELDS", "1", "f").expect.to.equal("*1\r\n$-1\r\n");
        // errors
        ks.run("HGETDEL", "h", "NOPE", "1", "f").expect.to.equal("-ERR syntax error\r\n");
        ks.run("HGETDEL", "h", "FIELDS", "2", "f")
            .expect.to.contain("numfields should be greater than 0");
        ks.run("HGETDEL", "h", "FIELDS", "x", "f")
            .expect.to.equal("-ERR value is not an integer or out of range\r\n");
    }

    @("blackbox.zset_range_error_parity")
    unittest
    {
        // bound errors carry Valkey's wording and fire before the key lookup
        Keyspace ks;
        scope (exit)
            ks.d.free();
        ks.run("ZRANGEBYSCORE", "ghost", "str", "1")
            .expect.to.equal("-ERR min or max is not a float\r\n");
        ks.run("ZREMRANGEBYSCORE", "ghost", "str", "1")
            .expect.to.equal("-ERR min or max is not a float\r\n");
        ks.run("ZRANGEBYLEX", "ghost", "bogus", "[c")
            .expect.to.equal("-ERR min or max not valid string range item\r\n");
        // reversed lex range matches nothing
        ks.run("ZADD", "z", "0", "a", "0", "b", "0", "c");
        ks.run("ZLEXCOUNT", "z", "+", "[c").expect.to.equal(":0\r\n");
        ks.run("ZRANGEBYLEX", "z", "(c", "[a").expect.to.equal("*0\r\n");
        ks.run("ZLEXCOUNT", "z", "-", "+").expect.to.equal(":3\r\n");
        // dangling score without member is a syntax error, not arity
        ks.run("ZADD", "z", "1", "x", "2").expect.to.equal("-ERR syntax error\r\n");
        // +inf + -inf aggregates to 0 (never NaN)
        ks.run("ZADD", "zi1", "inf", "k");
        ks.run("ZADD", "zi2", "-inf", "k");
        ks.run("ZUNIONSTORE", "zi3", "2", "zi1", "zi2").expect.to.equal(":1\r\n");
        ks.run("ZSCORE", "zi3", "k").expect.to.equal("$1\r\n0\r\n");
        // MSETEX: non-integer expire value is a not-an-integer error
        ks.run("MSETEX", "1", "k", "v", "EX", "abc")
            .expect.to.equal("-ERR value is not an integer or out of range\r\n");
        // a non-positive time is invalid for every option, absolute included
        ks.run("MSETEX", "1", "k", "v", "EX", "0")
            .expect.to.equal("-ERR invalid expire time in 'msetex' command\r\n");
        ks.run("MSETEX", "1", "k", "v", "EXAT", "0")
            .expect.to.equal("-ERR invalid expire time in 'msetex' command\r\n");
        ks.run("MSETEX", "1", "k", "v", "PXAT", "-1")
            .expect.to.equal("-ERR invalid expire time in 'msetex' command\r\n");
    }

    @("blackbox.compare_and_set")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();
        // SET ... IFEQ: write only when the current value matches
        ks.run("SET", "foo", "initial");
        ks.run("SET", "foo", "newv", "IFEQ", "initial").expect.to.equal("+OK\r\n");
        ks.run("GET", "foo").expect.to.equal("$4\r\nnewv\r\n");
        ks.run("SET", "foo", "nope", "IFEQ", "wrong").expect.to.equal("$-1\r\n"); // no match
        ks.run("GET", "foo").expect.to.equal("$4\r\nnewv\r\n");
        // IFEQ + GET returns the old value on a match
        ks.run("SET", "foo", "x2", "IFEQ", "newv", "GET").expect.to.equal("$4\r\nnewv\r\n");
        // IFEQ is incompatible with NX/XX; a non-string current value is WRONGTYPE
        ks.run("SET", "foo", "v", "IFEQ", "x2", "XX").expect.to.equal("-ERR syntax error\r\n");
        ks.run("SADD", "s", "m");
        ks.run("SET", "s", "v", "IFEQ", "m")
            .expect.to.equal("-WRONGTYPE Operation against a key holding the wrong kind of value\r\n");

        // DELIFEQ: delete only when the current value matches
        ks.run("DELIFEQ", "ghost", "v").expect.to.equal(":0\r\n");
        ks.run("SET", "k", "v");
        ks.run("DELIFEQ", "k", "nope").expect.to.equal(":0\r\n");
        ks.run("EXISTS", "k").expect.to.equal(":1\r\n");
        ks.run("DELIFEQ", "k", "v").expect.to.equal(":1\r\n");
        ks.run("EXISTS", "k").expect.to.equal(":0\r\n");
        ks.run("DELIFEQ", "s", "m")
            .expect.to.equal("-WRONGTYPE Operation against a key holding the wrong kind of value\r\n");
    }

    @("blackbox.expire_flags_and_overflow")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();
        ks.run("SET", "k", "v");
        // NX only when no TTL; XX only when one exists
        ks.run("EXPIRE", "k", "100", "NX").expect.to.equal(":1\r\n");
        ks.run("EXPIRE", "k", "200", "NX").expect.to.equal(":0\r\n");
        ks.run("EXPIRE", "k", "200", "XX").expect.to.equal(":1\r\n");
        // GT/LT against the current deadline; no-TTL key is +infinity
        ks.run("EXPIRE", "k", "100", "GT").expect.to.equal(":0\r\n");
        ks.run("EXPIRE", "k", "300", "GT").expect.to.equal(":1\r\n");
        ks.run("EXPIRE", "k", "400", "LT").expect.to.equal(":0\r\n");
        ks.run("EXPIRE", "k", "100", "LT").expect.to.equal(":1\r\n");
        ks.run("PERSIST", "k");
        ks.run("EXPIRE", "k", "100", "GT").expect.to.equal(":0\r\n"); // vs infinity
        ks.run("EXPIRE", "k", "100", "LT").expect.to.equal(":1\r\n");
        // conflicts and unknown options
        ks.run("EXPIRE", "k", "1", "NX", "XX")
            .expect.to.contain("NX and XX, GT or LT options");
        ks.run("EXPIRE", "k", "1", "GT", "LT")
            .expect.to.contain("GT and LT options");
        ks.run("EXPIRE", "k", "1", "BOGUS").expect.to.contain("Unsupported option BOGUS");
        // overflow carries the command name
        ks.run("EXPIRE", "k", "9223372036854775")
            .expect.to.equal("-ERR invalid expire time in 'expire' command\r\n");
        ks.run("EXPIRE", "k", "-9223372036854775807") // seconds→ms conversion overflows
            .expect.to.equal("-ERR invalid expire time in 'expire' command\r\n");
        ks.run("SET", "k", "v");
        ks.run("PEXPIRE", "k", "9223372036854775807") // base-time addition overflows
            .expect.to.equal("-ERR invalid expire time in 'pexpire' command\r\n");
        // a past deadline deletes the key on the spot
        ks.run("SET", "gone", "v");
        ks.run("EXPIREAT", "gone", "1").expect.to.equal(":1\r\n");
        ks.run("EXISTS", "gone").expect.to.equal(":0\r\n");
        // GETEX error split: bad option / bad int / bad time
        ks.run("SET", "g", "v");
        ks.run("GETEX", "g", "BOGUS", "1").expect.to.equal("-ERR syntax error\r\n");
        ks.run("GETEX", "g", "EX", "x")
            .expect.to.equal("-ERR value is not an integer or out of range\r\n");
        ks.run("GETEX", "g", "EX", "0")
            .expect.to.equal("-ERR invalid expire time in 'getex' command\r\n");
        ks.run("GETEX", "g", "EX", "9223372036854775")
            .expect.to.equal("-ERR invalid expire time in 'getex' command\r\n");
    }

    @("blackbox.replication_rewrites")
    unittest
    {
        // what reaches the AOF/raft log must replay to the same state
        Keyspace ks;
        scope (exit)
            ks.d.free();
        // MSETEX with a relative expire propagates the ABSOLUTE deadline
        ks.run("MSETEX", "2", "a", "1", "b", "2", "EX", "100").expect.to.equal(":1\r\n");
        auto prop = (cast(string) propagationOverride.data).idup;
        prop.expect.to.contain("PXAT");
        prop.expect.to.contain("MSETEX");
        // GETEX rewrites to PEXPIREAT / PERSIST
        ks.run("GETEX", "a", "EX", "50");
        (cast(string) propagationOverride.data).expect.to.contain("PEXPIREAT");
        ks.run("GETEX", "a", "PERSIST");
        (cast(string) propagationOverride.data).expect.to.contain("PERSIST");
        // SORT BY nosort with STORE forces a deterministic (alphabetical)
        // order: set slot order is not replay-stable
        ks.run("SADD", "s", "c", "a", "b");
        ks.run("SORT", "s", "BY", "nosort", "ALPHA", "STORE", "dst").expect.to.equal(":3\r\n");
        ks.run("LRANGE", "dst", "0", "-1")
            .expect.to.equal("*3\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n");
    }

    @("blackbox.functions")
    unittest
    {
        // minimal Redis Functions: LOAD registers named callbacks, FCALL runs
        // them with KEYS/ARGV as arguments, flags gate FCALL_RO
        Keyspace ks;
        scope (exit)
        {
            ks.run("FUNCTION", "FLUSH");
            ks.d.free();
        }
        enum lib = "#!lua name=mylib\n"
            ~ "redis.register_function('f1', function(KEYS, ARGV) "
            ~ "return redis.call('SET', KEYS[1], ARGV[1]) end)\n"
            ~ "redis.register_function{function_name='f2', callback="
            ~ "function(KEYS, ARGV) return redis.call('GET', KEYS[1]) end, "
            ~ "flags={'no-writes'}}";
        ks.run("FUNCTION", "LOAD", lib).expect.to.equal("$5\r\nmylib\r\n");
        // duplicate load fails without REPLACE, succeeds with it
        ks.run("FUNCTION", "LOAD", lib).expect.to.contain("already exists");
        ks.run("FUNCTION", "LOAD", "REPLACE", lib).expect.to.equal("$5\r\nmylib\r\n");
        ks.run("FCALL", "f1", "1", "k", "v").expect.to.equal("+OK\r\n");
        ks.run("FCALL", "f2", "1", "k").expect.to.equal("$1\r\nv\r\n");
        // FCALL_RO: only no-writes functions
        ks.run("FCALL_RO", "f2", "1", "k").expect.to.equal("$1\r\nv\r\n");
        ks.run("FCALL_RO", "f1", "1", "k", "x")
            .expect.to.contain("with write flag using *_ro");
        // and a no-writes function is refused a write at the bridge
        enum badLib = "#!lua name=badlib\n"
            ~ "redis.register_function{function_name='bad', callback="
            ~ "function(KEYS, ARGV) return redis.call('SET', KEYS[1], 'x') end, "
            ~ "flags={'no-writes'}}";
        ks.run("FUNCTION", "LOAD", badLib);
        ks.run("FCALL_RO", "bad", "1", "kk")[0].expect.to.equal('-');
        // errors
        ks.run("FCALL", "nope", "0").expect.to.equal("-ERR Function not found\r\n");
        ks.run("FUNCTION", "LOAD", "no shebang here")
            .expect.to.equal("-ERR Missing library metadata\r\n");
        ks.run("FUNCTION", "LOAD", "#!lua name=empty\nlocal x = 1")
            .expect.to.equal("-ERR No functions registered\r\n");
        // redis.call is fenced off during library load
        ks.run("FUNCTION", "LOAD", "#!lua name=fence\nredis.call('SET', 'a', 'b')")
            .expect.to.contain("function loading context");
        // the server API alias registers too
        enum srvLib = "#!lua name=srvlib\n"
            ~ "server.register_function('sf', function(KEYS, ARGV) return 7 end)";
        ks.run("FUNCTION", "LOAD", srvLib).expect.to.equal("$6\r\nsrvlib\r\n");
        ks.run("FCALL", "sf", "0").expect.to.equal(":7\r\n");
        // DELETE drops the lib's functions
        ks.run("FUNCTION", "DELETE", "srvlib").expect.to.equal("+OK\r\n");
        // LOAD/DELETE/FLUSH propagate themselves (registry must replicate)
        (cast(string) propagationOverride.data).expect.to.contain("FUNCTION");
        ks.run("FCALL", "sf", "0").expect.to.equal("-ERR Function not found\r\n");
        ks.run("FUNCTION", "DELETE", "srvlib").expect.to.equal("-ERR Library not found\r\n");
        // effects: FCALL writes replicate as the inner command, never FCALL
        {
            import dreads.scripting : gScriptEffectSink;

            static ByteBuffer captured;
            captured.clear();
            auto saved = gScriptEffectSink;
            scope (exit)
                gScriptEffectSink = saved;
            gScriptEffectSink = (scope const(ubyte)[] fx) @nogc nothrow {
                captured.append(fx);
            };
            ks.run("FCALL", "f1", "1", "fxk", "fxv").expect.to.equal("+OK\r\n");
            auto fx = (cast(string) captured.data).idup;
            fx.expect.to.contain("SET");
            fx.expect.to.not.contain("FCALL");
        }
    }

    @("blackbox.lpos_rank_zero_message")
    unittest
    {
        // error-parity class: LPOS RANK 0 uses Valkey's exact wording
        Keyspace ks;
        scope (exit)
            ks.d.free();
        ks.run("RPUSH", "l", "a");
        ks.run("LPOS", "l", "a", "RANK", "0")
            .expect.to.contain("RANK can't be zero: use 1 to start from the first match");
    }

    @("blackbox.smove_notifies_dst_only_on_addition")
    unittest
    {
        // notification class: SMOVE emits "sadd" only when the member was
        // actually added to dst (already-present members move silently)
        import dreads.notify : flushPendingNotify, gNotifyFlags, gNotifyPublish, NClass;

        Keyspace ks;
        static int sadds; // TLS capture
        sadds = 0;
        auto savedFlags = gNotifyFlags;
        auto savedPub = gNotifyPublish;
        scope (exit)
        {
            gNotifyFlags = savedFlags;
            gNotifyPublish = savedPub;
            ks.d.free();
        }
        gNotifyFlags = NClass.keyevent | NClass.set;
        gNotifyPublish = (scope const(char)[] chan, scope const(char)[] msg) nothrow{
            if (msg == "sadd" || (chan.length >= 4 && chan[$ - 4 .. $] == "sadd"))
                sadds++;
        };
        ks.run("SADD", "src", "a", "b");
        ks.run("SADD", "dst", "a");
        flushPendingNotify();
        sadds = 0;
        ks.run("SMOVE", "src", "dst", "a").expect.to.equal(":1\r\n"); // dup: no sadd
        flushPendingNotify();
        sadds.expect.to.equal(0);
        ks.run("SMOVE", "src", "dst", "b").expect.to.equal(":1\r\n"); // real addition
        flushPendingNotify();
        sadds.expect.to.equal(1);
    }
}
