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

    // Like `run`, but freezes the command clock at `atMs` (the dispatch applyTime,
    // as a replayed raft entry would) — deterministic time for TTL tests.
    private string runAt(ref Keyspace ks, ulong atMs, string[] cmdArgs...)
    {
        Arena arena;
        ByteBuffer o;
        RVal v;
        size_t pos = 0;
        propagationOverride.clear();
        auto encoded = cmdArgs.respCmd;
        parseValue(cast(const(ubyte)[]) encoded, pos, arena, v).expect.to.equal(ParseStatus.ok);
        v.dispatch(ks, o, arena, atMs);
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
        // a score spelled out to DBL_MAX's full ~325-digit decimal still parses
        // (strtod handles any length; our copy buffer must not cap it)
        enum dblmax = "179769313486231570814527423731704356798070567525844996"
            ~ "598917476803157260780028538760589558632766878171540458953514"
            ~ "382464234321326889464182768467546703537516986049910576551282"
            ~ "076245490090389328944075868508455133942304583236903222948165"
            ~ "808559332123348274797826204144723168738177180919299881250404"
            ~ "026184124858368.00000000000000000";
        ks.run("ZADD", "zbig", dblmax, "m").expect.to.equal(":1\r\n");
        ks.run("ZSCORE", "zbig", "m").expect.to.equal("$23\r\n1.7976931348623157e+308\r\n");
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

    @("blackbox.deque_fifo_and_shrink")
    unittest
    {
        // emplace.Deque backs the per-key blocked-client FIFO (BLPOP family). It
        // must keep arrival order across ring wrap AND release memory as it drains
        // (long-running: a key that saw a burst of waiters must not keep the block).
        import emplace.deque : Deque;

        Deque!int d;
        foreach (i; 0 .. 6)
            d.pushBack(i);
        d.popFront();
        d.popFront(); // head advances; next pushes wrap the ring
        d.pushBack(6);
        d.pushBack(7);
        // logical order preserved: 2 3 4 5 6 7
        foreach (i, want; [2, 3, 4, 5, 6, 7])
            d[i].expect.to.equal(want);
        d.front.expect.to.equal(2);
        d.back.expect.to.equal(7);

        // FIFO drain across a grow
        Deque!int q;
        foreach (i; 0 .. 500)
            q.pushBack(i);
        foreach (i; 0 .. 500)
        {
            q.front.expect.to.equal(i);
            q.popFront();
        }
        q.empty.expect.to.equal(true);
    }

    @("blackbox.hexpire_httl_codes")
    unittest
    {
        // HEXPIRE family (Valkey 7.4): per-field reply codes and the four TTL
        // read commands. Codes: -2 no field, -1 no TTL, 1 set, value for reads.
        enum ulong T0 = 1_000_000_000; // frozen now, 1e9 ms

        Keyspace ks;
        scope (exit)
            ks.d.free();

        ks.runAt(T0, "HSET", "h", "f1", "v1", "f2", "v2", "f3", "v3");
        // set TTL on f1,f2 (100s); nope is missing
        ks.runAt(T0, "HEXPIRE", "h", "100", "FIELDS", "2", "f1", "f2")
            .expect.to.equal("*2\r\n:1\r\n:1\r\n");
        ks.runAt(T0, "HEXPIRE", "h", "100", "FIELDS", "1", "nope")
            .expect.to.equal("*1\r\n:-2\r\n");
        // reads: f1 has 100s, f3 has none (-1), nope missing (-2)
        ks.runAt(T0, "HTTL", "h", "FIELDS", "3", "f1", "f3", "nope")
            .expect.to.equal("*3\r\n:100\r\n:-1\r\n:-2\r\n");
        ks.runAt(T0, "HPTTL", "h", "FIELDS", "1", "f1").expect.to.equal("*1\r\n:100000\r\n");
        // absolute: now(1e9ms)+100000ms = 1_000_100_000ms -> 1_000_100 s
        ks.runAt(T0, "HEXPIRETIME", "h", "FIELDS", "1", "f1").expect.to.equal("*1\r\n:1000100\r\n");
        ks.runAt(T0, "HPEXPIRETIME", "h", "FIELDS", "1", "f1")
            .expect.to.equal("*1\r\n:1000100000\r\n");
        // propagation is canonical HPEXPIREAT with the absolute ms
        ks.runAt(T0, "HEXPIRE", "h", "100", "FIELDS", "1", "f1");
        (cast(string) propagationOverride.data).idup.expect.to.equal(
            "*6\r\n$10\r\nHPEXPIREAT\r\n$1\r\nh\r\n$10\r\n1000100000\r\n$6\r\nFIELDS\r\n$1\r\n1\r\n$2\r\nf1\r\n");
    }

    @("blackbox.hexpire_lazy_reap_and_past")
    unittest
    {
        // A field whose deadline passed is invisible (lazy reap on access), and a
        // past-time HEXPIRE deletes the field now (code 2) and propagates HDEL.
        enum ulong T0 = 1_000_000_000;
        enum ulong T1 = 1_000_200_000; // 200s later

        Keyspace ks;
        scope (exit)
            ks.d.free();

        ks.runAt(T0, "HSET", "h", "f1", "v1", "f2", "v2");
        ks.runAt(T0, "HEXPIRE", "h", "100", "FIELDS", "1", "f1");
        ks.runAt(T1, "HGET", "h", "f1").expect.to.equal("$-1\r\n"); // reaped, invisible
        ks.runAt(T1, "HGET", "h", "f2").expect.to.equal("$2\r\nv2\r\n"); // survives
        ks.runAt(T1, "HLEN", "h").expect.to.equal(":1\r\n");

        // past-time set: code 2, field deleted, propagation is HDEL
        ks.runAt(T0, "HSET", "h2", "a", "1", "b", "2");
        ks.runAt(T0, "HPEXPIREAT", "h2", "1", "FIELDS", "1", "a").expect.to.equal("*1\r\n:2\r\n");
        (cast(string) propagationOverride.data).idup.expect.to.equal(
            "*3\r\n$4\r\nHDEL\r\n$2\r\nh2\r\n$1\r\na\r\n");
        ks.runAt(T0, "HGET", "h2", "a").expect.to.equal("$-1\r\n");
    }

    @("blackbox.hexpire_conditions_and_persist")
    unittest
    {
        // NX/XX/GT/LT per-field conditions (a field with no TTL is +infinity for
        // GT/LT, like key EXPIRE) and HPERSIST codes (-2/-1/1).
        import dreads.det : gClock;

        Keyspace ks;
        scope (exit)
        {
            ks.d.free();
            gClock = 0;
        }
        gClock = 1_000_000_000;

        ks.run("HSET", "h", "f", "1");
        ks.run("HEXPIRE", "h", "100", "NX", "FIELDS", "1", "f").expect.to.equal("*1\r\n:1\r\n");
        ks.run("HEXPIRE", "h", "200", "NX", "FIELDS", "1", "f").expect.to.equal("*1\r\n:0\r\n");
        ks.run("HEXPIRE", "h", "200", "GT", "FIELDS", "1", "f").expect.to.equal("*1\r\n:1\r\n");
        ks.run("HEXPIRE", "h", "100", "GT", "FIELDS", "1", "f").expect.to.equal("*1\r\n:0\r\n");
        ks.run("HEXPIRE", "h", "50", "LT", "FIELDS", "1", "f").expect.to.equal("*1\r\n:1\r\n");
        ks.run("HEXPIRE", "h", "999", "XX", "FIELDS", "1", "f").expect.to.equal("*1\r\n:1\r\n");
        // incompatible flags
        ks.run("HEXPIRE", "h", "1", "NX", "GT", "FIELDS", "1", "f").expect.to.contain("not compatible");

        // HPERSIST: f has TTL (->1), g has none (->-1), nope missing (->-2)
        ks.run("HSET", "h", "g", "2");
        ks.run("HPERSIST", "h", "FIELDS", "3", "f", "g", "nope")
            .expect.to.equal("*3\r\n:1\r\n:-1\r\n:-2\r\n");
        ks.run("HTTL", "h", "FIELDS", "1", "f").expect.to.equal("*1\r\n:-1\r\n"); // gone
    }

    @("blackbox.hexpire_hset_clears_and_encoding")
    unittest
    {
        // A plain HSET overwrite discards the field's TTL (Valkey: EXPIRY_NONE),
        // and a small hash carrying any field TTL reports "listpackex".
        import dreads.det : gClock;

        Keyspace ks;
        scope (exit)
        {
            ks.d.free();
            gClock = 0;
        }
        gClock = 1_000_000_000;

        ks.run("HSET", "h", "f", "1");
        ks.run("OBJECT", "ENCODING", "h").expect.to.equal("$8\r\nlistpack\r\n");
        ks.run("HEXPIRE", "h", "100", "FIELDS", "1", "f");
        ks.run("OBJECT", "ENCODING", "h").expect.to.equal("$10\r\nlistpackex\r\n");
        ks.run("HSET", "h", "f", "2"); // overwrite drops the TTL
        ks.run("HTTL", "h", "FIELDS", "1", "f").expect.to.equal("*1\r\n:-1\r\n");
    }

    @("blackbox.hgetex_ttl_ops")
    unittest
    {
        // HGETEX returns the values (HMGET-shaped) and applies its TTL op as a
        // side effect: EX sets, PERSIST clears, no-op leaves the TTL untouched.
        import dreads.det : gClock;

        Keyspace ks;
        scope (exit)
        {
            ks.d.free();
            gClock = 0;
        }
        gClock = 1_000_000_000;

        ks.run("HSET", "h", "f", "v1", "g", "v2");
        ks.run("HGETEX", "h", "EX", "100", "FIELDS", "2", "f", "g")
            .expect.to.equal("*2\r\n$2\r\nv1\r\n$2\r\nv2\r\n");
        ks.run("HTTL", "h", "FIELDS", "2", "f", "g").expect.to.equal("*2\r\n:100\r\n:100\r\n");
        ks.run("HGETEX", "h", "PERSIST", "FIELDS", "1", "f")
            .expect.to.equal("*1\r\n$2\r\nv1\r\n");
        // f persisted, g untouched by the plain (no-op) read below
        ks.run("HGETEX", "h", "FIELDS", "1", "g").expect.to.equal("*1\r\n$2\r\nv2\r\n");
        ks.run("HTTL", "h", "FIELDS", "2", "f", "g").expect.to.equal("*2\r\n:-1\r\n:100\r\n");
    }

    // Extract a RESP bulk-string payload ($<len>\r\n<bytes>\r\n) by length — the
    // DUMP payload is binary and may contain \r\n, so never split on the delimiter.
    private string extractBulk(string reply)
    {
        assert(reply.length > 0 && reply[0] == '$', "not a bulk reply");
        size_t i = 1, len = 0;
        while (reply[i] != '\r')
        {
            len = len * 10 + (reply[i] - '0');
            i++;
        }
        i += 2; // skip \r\n
        return reply[i .. i + len];
    }

    @("blackbox.rdb_dump_restore_roundtrip")
    unittest
    {
        // DUMP -> RESTORE round-trips every value type through the AOF-command <->
        // RDB translator (the compactor feeds DUMP; RESTORE decodes to commands and
        // dispatches). Field TTLs survive as HASH_2.
        import dreads.det : gClock;

        Keyspace ks;
        scope (exit)
        {
            ks.d.free();
            gClock = 0;
        }
        enum ulong T0 = 1_000_000_000;

        ks.runAt(T0, "SET", "str", "hello");
        ks.runAt(T0, "RPUSH", "l", "a", "b", "c");
        ks.runAt(T0, "SADD", "s", "x", "y", "z");
        ks.runAt(T0, "HSET", "h", "f1", "v1", "f2", "v2");
        ks.runAt(T0, "HEXPIRE", "h", "500", "FIELDS", "1", "f1"); // -> HASH_2
        ks.runAt(T0, "ZADD", "z", "1.5", "m1", "2.5", "m2");

        foreach (k; ["str", "l", "s", "h", "z"])
        {
            auto payload = extractBulk(ks.runAt(T0, "DUMP", k));
            ks.runAt(T0, "DEL", k);
            ks.runAt(T0, "RESTORE", k, "0", payload).expect.to.equal("+OK\r\n");
        }

        // contents survived
        ks.runAt(T0, "GET", "str").expect.to.equal("$5\r\nhello\r\n");
        ks.runAt(T0, "LRANGE", "l", "0", "-1")
            .expect.to.equal("*3\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n");
        ks.runAt(T0, "SCARD", "s").expect.to.equal(":3\r\n");
        ks.runAt(T0, "HGET", "h", "f2").expect.to.equal("$2\r\nv2\r\n");
        ks.runAt(T0, "HTTL", "h", "FIELDS", "1", "f1").expect.to.equal("*1\r\n:500\r\n"); // TTL survived
        ks.runAt(T0, "ZSCORE", "z", "m2").expect.to.equal("$3\r\n2.5\r\n");

        // BUSYKEY without REPLACE, ok with REPLACE
        auto p = extractBulk(ks.runAt(T0, "DUMP", "str"));
        ks.runAt(T0, "RESTORE", "str", "0", p).expect.to.contain("BUSYKEY");
        ks.runAt(T0, "RESTORE", "str", "0", p, "REPLACE").expect.to.equal("+OK\r\n");
        // corrupted payload rejected
        auto bad = p.dup;
        bad[$ - 1] = cast(char)(bad[$ - 1] ^ 0xFF);
        ks.runAt(T0, "RESTORE", "x", "0", cast(string) bad).expect.to.contain("checksum");
    }

    @("blackbox.rdb_stream_dump_restore")
    unittest
    {
        // DUMP -> RESTORE round-trips a STREAM through the codec: entries (fields
        // and values, incl. multi-field), the last-id, and a consumer group. The
        // stream RDB is encoded one-listpack-per-entry (master field template +
        // one SAMEFIELDS entry) and decoded back to XADD/XSETID/XGROUP. Verified
        // byte-exact against Valkey 9.1 both ways (live).
        import dreads.det : gClock;

        Keyspace ks;
        scope (exit)
        {
            ks.d.free();
            gClock = 0;
        }
        enum ulong T0 = 1_000_000_000;

        ks.runAt(T0, "XADD", "st", "5-1", "name", "alice", "age", "30");
        ks.runAt(T0, "XADD", "st", "6-1", "name", "bob");
        ks.runAt(T0, "XADD", "st", "7-3", "x", "y");
        ks.runAt(T0, "XGROUP", "CREATE", "st", "g1", "0");

        auto payload = extractBulk(ks.runAt(T0, "DUMP", "st"));
        ks.runAt(T0, "DEL", "st");
        ks.runAt(T0, "RESTORE", "st", "0", payload).expect.to.equal("+OK\r\n");

        ks.runAt(T0, "XLEN", "st").expect.to.equal(":3\r\n");
        ks.runAt(T0, "XRANGE", "st", "5-1", "5-1")
            .expect.to.equal("*1\r\n*2\r\n$3\r\n5-1\r\n*4\r\n$4\r\nname\r\n$5\r\nalice\r\n$3\r\nage\r\n$2\r\n30\r\n");
        // last-id preserved (a further XADD * must land after 7-3)
        ks.runAt(T0, "XADD", "st", "7-4", "z", "w").expect.to.equal("$3\r\n7-4\r\n");
        // the consumer group survived
        ks.runAt(T0, "XINFO", "GROUPS", "st").expect.to.contain("g1");
    }

    private string hexToStr(string h)
    {
        import std.conv : to;

        auto o = new char[h.length / 2];
        foreach (i; 0 .. o.length)
            o[i] = cast(char) h[2 * i .. 2 * i + 2].to!ubyte(16);
        return cast(string) o;
    }

    @("blackbox.rdb_restore_valkey_compact")
    unittest
    {
        // RESTORE real Valkey 9.1 DUMP payloads (captured live): intset,
        // listpack set/hash/zset, quicklist2 list. These are the compact encodings
        // dreads must decode to import external Redis/Valkey dumps (phase 2).
        Keyspace ks;
        scope (exit)
            ks.d.free();

        // intset {10,20,30}
        ks.run("RESTORE", "iset", "0",
            hexToStr("0b0e02000000030000000a0014001e005000cdeac7c8f30be87c"))
            .expect.to.equal("+OK\r\n");
        ks.run("SCARD", "iset").expect.to.equal(":3\r\n");
        ks.run("SISMEMBER", "iset", "20").expect.to.equal(":1\r\n");

        // listpack set {alpha,beta}
        ks.run("RESTORE", "slp", "0",
            hexToStr("141414000000020085616c70686106846265746105ff5000570d82be5bd5477b"))
            .expect.to.equal("+OK\r\n");
        ks.run("SISMEMBER", "slp", "alpha").expect.to.equal(":1\r\n");

        // listpack hash {f1:v1, count:42}
        ks.run("RESTORE", "hlp", "0",
            hexToStr("1018180000000400826631038276310385636f756e74062a01ff5000a71468eb3bccba4b"))
            .expect.to.equal("+OK\r\n");
        ks.run("HGET", "hlp", "f1").expect.to.equal("$2\r\nv1\r\n");
        ks.run("HGET", "hlp", "count").expect.to.equal("$2\r\n42\r\n");

        // listpack zset {m1:1.5, m2:-2}
        ks.run("RESTORE", "zlp", "0",
            hexToStr("1117170000000400826d3203dffe02826d310383312e3504ff50002cb016337541ecc2"))
            .expect.to.equal("+OK\r\n");
        ks.run("ZSCORE", "zlp", "m1").expect.to.equal("$3\r\n1.5\r\n");
        ks.run("ZSCORE", "zlp", "m2").expect.to.equal("$2\r\n-2\r\n");

        // quicklist2 list [a,b,7]
        ks.run("RESTORE", "lst", "0",
            hexToStr("1201020f0f00000003008161028162020701ff50006bffb482e0c12468"))
            .expect.to.equal("+OK\r\n");
        ks.run("LRANGE", "lst", "0", "-1")
            .expect.to.equal("*3\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\n7\r\n");
    }

    @("blackbox.hsetex_flags")
    unittest
    {
        // HSETEX key [FNX|FXX] [EX|PX|EXAT|PXAT|KEEPTTL] FIELDS n field value...
        import dreads.det : gClock;

        Keyspace ks;
        scope (exit)
        {
            ks.d.free();
            gClock = 0;
        }
        enum ulong T0 = 1_000_000_000;

        // set value + TTL atomically
        ks.runAt(T0, "HSETEX", "h", "EX", "100", "FIELDS", "2", "f1", "v1", "f2", "v2")
            .expect.to.equal(":1\r\n");
        ks.runAt(T0, "HGET", "h", "f1").expect.to.equal("$2\r\nv1\r\n");
        ks.runAt(T0, "HTTL", "h", "FIELDS", "1", "f1").expect.to.equal("*1\r\n:100\r\n");

        // FNX fails when any field exists; FXX succeeds when all exist
        ks.runAt(T0, "HSETEX", "h", "FNX", "EX", "50", "FIELDS", "1", "f1", "x")
            .expect.to.equal(":0\r\n");
        ks.runAt(T0, "HSETEX", "h", "FXX", "EX", "50", "FIELDS", "1", "f1", "vNEW")
            .expect.to.equal(":1\r\n");
        ks.runAt(T0, "HGET", "h", "f1").expect.to.equal("$4\r\nvNEW\r\n");

        // KEEPTTL changes the value but keeps the field's TTL
        ks.runAt(T0, "HSETEX", "h", "KEEPTTL", "FIELDS", "1", "f1", "vKeep")
            .expect.to.equal(":1\r\n");
        ks.runAt(T0, "HTTL", "h", "FIELDS", "1", "f1").expect.to.equal("*1\r\n:50\r\n");

        // no TTL flag => field becomes persistent (clears any TTL)
        ks.runAt(T0, "HSETEX", "h", "FIELDS", "1", "f2", "vP").expect.to.equal(":1\r\n");
        ks.runAt(T0, "HTTL", "h", "FIELDS", "1", "f2").expect.to.equal("*1\r\n:-1\r\n");
    }

    @("blackbox.import_mode_pauses_expiry")
    unittest
    {
        // CLIENT IMPORT-SOURCE / import-mode: a bulk-load window where expiry is
        // paused so a stream of RESTOREs with absolute (possibly past) TTLs loads
        // consistently instead of racing the expiry cycle.
        import dreads.obj : gImportMode;
        import dreads.det : gClock;

        Keyspace ks;
        scope (exit)
        {
            ks.d.free();
            gImportMode = false;
            gClock = 0;
        }
        enum ulong T0 = 1_000_000_000;

        ks.runAt(T0, "SET", "k", "v");
        ks.runAt(T0, "PEXPIRE", "k", "10"); // deadline T0+10

        gImportMode = true; // import window: past-deadline key stays live
        ks.runAt(T0 + 100, "GET", "k").expect.to.equal("$1\r\nv\r\n");
        ks.runAt(T0 + 100, "EXISTS", "k").expect.to.equal(":1\r\n");

        gImportMode = false; // window closed: lazy expiry resumes
        ks.runAt(T0 + 100, "GET", "k").expect.to.equal("$-1\r\n");
    }

    @("blackbox.hexpire_spill_and_active_reap")
    unittest
    {
        // Field TTLs survive the small->large (listpack->hashtable) spill (they
        // key by field name, not slot), and the active cycle reaps due fields of
        // an untouched hash via the tagged secondary index.
        import dreads.det : gClock;
        import dreads.obj : gActiveExpire;
        import std.conv : to;

        enum ulong T0 = 1_000_000_000;
        enum ulong T1 = 1_000_200_000; // 200s later

        Keyspace ks;
        scope (exit)
        {
            ks.d.free();
            gClock = 0;
            gActiveExpire = false;
        }
        gActiveExpire = true; // arm the secondary index

        // one TTL'd field (500s), then push past 128 entries to force the spill
        ks.runAt(T0, "HSET", "big", "keep", "v");
        ks.runAt(T0, "HEXPIRE", "big", "500", "FIELDS", "1", "keep");
        foreach (i; 0 .. 200)
            ks.runAt(T0, "HSET", "big", "x" ~ i.to!string, "y");
        ks.runAt(T0, "OBJECT", "ENCODING", "big").expect.to.equal("$9\r\nhashtable\r\n");
        ks.runAt(T0, "HTTL", "big", "FIELDS", "1", "keep").expect.to.equal("*1\r\n:500\r\n"); // survived spill

        // active reap of an untouched hash: f (100s) is due at T1, g survives
        ks.runAt(T0, "HSET", "h", "f", "1", "g", "2");
        ks.runAt(T0, "HEXPIRE", "h", "100", "FIELDS", "1", "f");
        gClock = T1; // advance for the (non-dispatch) active cycle
        ks.activeSubExpireCycle();
        ks.runAt(T1, "HGET", "h", "f").expect.to.equal("$-1\r\n"); // reaped by the cycle
        ks.runAt(T1, "HGET", "h", "g").expect.to.equal("$1\r\n2\r\n"); // survives
    }

    // CLIENT PAUSE write surface: the classifier the WRITE-mode barrier consults.
    // The socket/fiber buffering + replay is server-layer (stays blackbox-only —
    // see BLACKBOX-TODO.md); this pins the pure predicate, Valkey's WRITE-pause
    // mask CMD_WRITE | CMD_MAY_REPLICATE.
    @("blackbox.pause_write_surface_classifier")
    unittest
    {
        // plain writes are held
        isPausedByWrite("SET").should.equal(true);
        isPausedByWrite("HSET").should.equal(true);
        isPausedByWrite("ZADD").should.equal(true);
        // may-replicate: propagate / rewrite a cache, so a WRITE pause holds them
        isPausedByWrite("PUBLISH").should.equal(true);
        isPausedByWrite("SPUBLISH").should.equal(true);
        isPausedByWrite("PFCOUNT").should.equal(true);
        // pure reads pass the barrier
        isPausedByWrite("GET").should.equal(false);
        isPausedByWrite("HGET").should.equal(false);
        isPausedByWrite("PING").should.equal(false);
        // scripts are NOT decided here — write-ness depends on shebang / function
        // flags, resolved separately via gScriptWritesHook
        isPausedByWrite("EVAL").should.equal(false);
        isPausedByWrite("FCALL").should.equal(false);
    }

    // deny-OOM classification (Valkey CMD_DENYOOM): only allocating writes are
    // refused over maxmemory; freeing/neutral writes run so a script can DEL to
    // make room. First caught by unit/scripting (dirty-scripts / allow-oom).
    @("blackbox.deny_oom_classifier")
    unittest
    {
        // allocating writes are denied under OOM
        isDenyOomCommand("SET").should.equal(true);
        isDenyOomCommand("APPEND").should.equal(true);
        isDenyOomCommand("LPUSH").should.equal(true);
        isDenyOomCommand("HSET").should.equal(true);
        isDenyOomCommand("ZADD").should.equal(true);
        isDenyOomCommand("COPY").should.equal(true);
        isDenyOomCommand("RESTORE").should.equal(true);
        // freeing / neutral writes are exempt (run even under OOM)
        isDenyOomCommand("DEL").should.equal(false);
        isDenyOomCommand("UNLINK").should.equal(false);
        isDenyOomCommand("GETDEL").should.equal(false);
        isDenyOomCommand("EXPIRE").should.equal(false);
        isDenyOomCommand("PERSIST").should.equal(false);
        isDenyOomCommand("LPOP").should.equal(false);
        isDenyOomCommand("SREM").should.equal(false);
        isDenyOomCommand("ZPOPMIN").should.equal(false);
        isDenyOomCommand("RENAME").should.equal(false);
        // reads are never deny-oom
        isDenyOomCommand("GET").should.equal(false);
        isDenyOomCommand("PING").should.equal(false);
    }

    // INFO errorstats: statErrorReply extracts the error code (first token, capped
    // at 32 chars, default ERR) and bumps total_error_replies. The pipeline wiring
    // is server-layer (blackbox-only); this pins the code extraction.
    @("blackbox.errorstats_code_extraction")
    unittest
    {
        import dreads.stats : gErrorStats, gTotalErrorReplies, statErrorReply,
            resetErrorStats;

        resetErrorStats();
        statErrorReply("-WRONGTYPE Operation against a key holding the wrong kind");
        statErrorReply("-ERR bad");
        statErrorReply("ERR no leading dash"); // caller passed the bare message
        statErrorReply("-OOM command not allowed");
        gTotalErrorReplies.should.equal(4);
        (*gErrorStats.get("WRONGTYPE")).should.equal(1);
        (*gErrorStats.get("ERR")).should.equal(2);
        (*gErrorStats.get("OOM")).should.equal(1);
        resetErrorStats();
        gTotalErrorReplies.should.equal(0);
        assert(gErrorStats.get("OOM") is null);
    }

    // OBJECT FREQ/IDLETIME follow the maxmemory policy (LRU and LFU share one
    // counter, like Redis's obj->lru), and RESTORE FREQ/IDLETIME seed it. First
    // caught by unit/dump (OBJECT FREQ aborted the file).
    @("blackbox.object_freq_idletime_lfu")
    unittest
    {
        import dreads.config : gConfig;
        import dreads.obj : lruClock;

        Keyspace ks;
        auto savedPolicy = gConfig.maxmemoryPolicy;
        scope (exit)
        {
            ks.d.free();
            gConfig.maxmemoryPolicy = savedPolicy;
        }

        ks.run("SET", "k", "v");

        // non-LFU: IDLETIME is an int, FREQ errors
        gConfig.maxmemoryPolicy = "noeviction";
        ks.run("OBJECT", "IDLETIME", "k").should.startWith(":");
        ks.run("OBJECT", "FREQ", "k").should.startWith("-ERR An LFU maxmemory policy is not selected");

        // LFU: FREQ is an int, IDLETIME errors
        gConfig.maxmemoryPolicy = "allkeys-lfu";
        ks.run("OBJECT", "FREQ", "k").should.startWith(":");
        ks.run("OBJECT", "IDLETIME", "k").should.startWith("-ERR An LFU maxmemory policy is selected");

        // RESTORE FREQ seeds the counter; OBJECT FREQ reads it back (and the
        // introspection lookup must not bump it)
        auto dumped = extractBulk(ks.run("DUMP", "k"));
        ks.run("DEL", "k");
        ks.run("RESTORE", "k", "0", dumped, "FREQ", "100").should.equal("+OK\r\n");
        ks.run("OBJECT", "FREQ", "k").should.equal(":100\r\n");

        // RESTORE IDLETIME backdates last-access so OBJECT IDLETIME reports it
        gConfig.maxmemoryPolicy = "allkeys-lru";
        ks.run("DEL", "k");
        ks.run("RESTORE", "k", "0", dumped, "IDLETIME", "1000").should.equal("+OK\r\n");
        immutable reply = ks.run("OBJECT", "IDLETIME", "k"); // ":<idle>\r\n", idle ~ 1000
        reply.should.startWith(":");
    }

    // MIGRATE argument parsing (option 2: DUMP here -> RESTORE onto the target
    // over a cached socket). The socket round-trip is server-layer and validated
    // live against Valkey (dump.tcl MIGRATE tests are external:skip); this pins
    // the pure arg parser: host/port/destdb/timeout + COPY/REPLACE/AUTH/AUTH2/KEYS
    // and the single-key vs KEYS-form validation.
    @("blackbox.migrate_arg_parsing")
    unittest
    {
        static RVal[] cmd(const(char)[][] parts...)
        {
            auto a = new RVal[parts.length];
            foreach (i, p; parts)
            {
                a[i].type = RType.BulkString;
                a[i].str = p;
            }
            return a;
        }

        MigrateArgs m;

        // basic single-key form
        parseMigrateArgs(cmd("MIGRATE", "127.0.0.1", "6380", "k", "0", "1000"), m)
            .should.equal(true);
        m.host.should.equal("127.0.0.1");
        m.port.should.equal(6380);
        m.destdb.should.equal(0);
        m.timeout.should.equal(1000);
        m.singleKey.should.equal("k");
        m.copy.should.equal(false);
        m.replace.should.equal(false);
        m.hasAuth.should.equal(false);
        m.keyList.length.should.equal(0);

        // COPY + REPLACE flags
        parseMigrateArgs(cmd("MIGRATE", "h", "1", "k", "2", "5", "COPY", "REPLACE"), m)
            .should.equal(true);
        m.copy.should.equal(true);
        m.replace.should.equal(true);
        m.destdb.should.equal(2);

        // AUTH pw / AUTH2 user pw
        parseMigrateArgs(cmd("MIGRATE", "h", "1", "k", "0", "5", "AUTH", "secret"), m)
            .should.equal(true);
        m.hasAuth.should.equal(true);
        m.authUser.length.should.equal(0);
        m.authPw.should.equal("secret");
        parseMigrateArgs(cmd("MIGRATE", "h", "1", "k", "0", "5", "AUTH2", "u", "p"), m)
            .should.equal(true);
        m.authUser.should.equal("u");
        m.authPw.should.equal("p");

        // KEYS form: key argument must be the empty string, all keys collected
        parseMigrateArgs(cmd("MIGRATE", "h", "1", "", "0", "5", "KEYS", "a", "b", "c"), m)
            .should.equal(true);
        m.keyList.length.should.equal(3);
        m.keyList[0].str.should.equal("a");
        m.keyList[2].str.should.equal("c");
        m.singleKey.length.should.equal(0);

        // KEYS with a non-empty key argument is an error
        parseMigrateArgs(cmd("MIGRATE", "h", "1", "notempty", "0", "5", "KEYS", "a"), m)
            .should.equal(false);
        m.err.should.startWith("ERR When using MIGRATE KEYS option");

        // empty single key (no KEYS) -> wrong number of args
        parseMigrateArgs(cmd("MIGRATE", "h", "1", "", "0", "5"), m).should.equal(false);
        m.err.should.startWith("ERR wrong number of arguments");

        // too few args
        parseMigrateArgs(cmd("MIGRATE", "h", "1", "k", "0"), m).should.equal(false);
        m.err.should.startWith("ERR wrong number of arguments");

        // non-integer port / destdb / timeout
        parseMigrateArgs(cmd("MIGRATE", "h", "notaport", "k", "0", "5"), m).should.equal(false);
        m.err.should.startWith("ERR value is not an integer");

        // unknown option -> syntax error
        parseMigrateArgs(cmd("MIGRATE", "h", "1", "k", "0", "5", "BOGUS"), m).should.equal(false);
        m.err.should.equal("ERR syntax error");
    }

    // XREADGROUP BLOCK servability: the parking/wake is server-layer (fan-out on
    // gKeyActivity, blackbox-only like the FIFO) — this pins the per-wake predicate
    // the block loop consults. A `>` read on an empty group returns the nil array
    // (=> keep blocking); a delivery advances the group cursor so the *next* `>`
    // read is nil again; an explicit id reads the PEL (=> never blocks).
    @("blackbox.xreadgroup_block_servability")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        ks.run("XADD", "s", "1-1", "f", "v0"); // group starts at $ = 1-1
        ks.run("XGROUP", "CREATE", "s", "g", "$");

        // `>` with no new messages -> nil array: this is what makes the loop wait
        ks.run("XREADGROUP", "GROUP", "g", "w1", "STREAMS", "s", ">")
            .should.equal("*-1\r\n");

        // a new entry -> `>` now delivers it (and advances the group cursor)
        ks.run("XADD", "s", "2-1", "job", "42");
        auto served = ks.run("XREADGROUP", "GROUP", "g", "w1", "STREAMS", "s", ">");
        served.should.contain("2-1");
        served.should.contain("job");
        served.should.not.equal("*-1\r\n");

        // cursor advanced: a second `>` read is nil again (would re-block)
        ks.run("XREADGROUP", "GROUP", "g", "w1", "STREAMS", "s", ">")
            .should.equal("*-1\r\n");

        // explicit id reads the consumer's PEL (the delivered-but-unacked 2-1) —
        // a non-nil reply, so the server layer never parks for an explicit id
        auto hist = ks.run("XREADGROUP", "GROUP", "g", "w1", "STREAMS", "s", "0");
        hist.should.contain("2-1");
        hist.should.not.equal("*-1\r\n");
    }
}
