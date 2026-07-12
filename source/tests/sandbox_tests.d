module tests.sandbox_tests;

// Lua sandbox: curated libraries, pruned escape hatches, protected globals,
// deterministic RNG, and the memory/time resource limits.

version (unittest)
{
    import fluent.asserts;

    import dreads.commands : propagationOverride;
    import dreads.config : gConfig;
    import dreads.mem : Arena, ByteBuffer;
    import dreads.obj : Keyspace;
    import dreads.resp;
    import dreads.scripting : evalCommand;

    private string evalRun(ref Keyspace ks, string script, string[] rest...)
    {
        Arena arena;
        ByteBuffer o;
        auto all = [script] ~ (rest.length ? rest : ["0"]);
        auto vals = new RVal[all.length];
        foreach (i, s; all)
        {
            vals[i].type = RType.BulkString;
            vals[i].str = s;
        }
        evalCommand(vals, ks, o, arena, false);
        return (cast(string) o.data).idup;
    }

    @("sandbox.no_dangerous_libraries")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();
        // io/package/debug are not loaded; reading them trips _G protection
        foreach (lib; ["io", "debug", "package", "require", "dofile",
                "loadfile", "load", "print"])
        {
            auto reply = ks.run2(lib);
            reply[0].expect.to.equal('-');
            reply.expect.to.contain("global");
        }
        // os is a two-function whitelist: clock/time yes, execute & co. no
        ks.evalRun("return type(os.clock())").expect.to.equal("$6\r\nnumber\r\n");
        // os is clock-only when enumerated, but any other field is a stub
        // that errors on call (Valkey's exact "attempt to call field 'X'")
        ks.evalRun("local n=0 for k in pairs(os) do n=n+1 end return n").expect.to.equal(":1\r\n");
        ks.evalRun("os.execute()").expect.to.contain("attempt to call field 'execute'");
        ks.evalRun("os.getenv()").expect.to.contain("attempt to call field 'getenv'");
        ks.evalRun("os.time()").expect.to.contain("attempt to call field 'time'");
        // the allowed libraries work
        ks.evalRun("return string.upper('ok')").expect.to.equal("$2\r\nOK\r\n");
        ks.evalRun("return table.concat({'a','b'}, '-')").expect.to.equal("$3\r\na-b\r\n");
        ks.evalRun("return math.floor(3.7)").expect.to.equal(":3\r\n");
        ks.evalRun("return tostring(1) .. type({})").expect.to.equal("$6\r\n1table\r\n");
    }

    // helper: probe a global by name
    private string run2(ref Keyspace ks, string global)
    {
        return ks.evalRun("return tostring(" ~ global ~ ")");
    }

    @("sandbox.per_run_environment")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();
        // creating a global is forbidden (Valkey parity): the throwaway _ENV
        // is read-only for undeclared names
        auto wr = ks.evalRun("leaked = 42 return 1");
        wr[0].expect.to.equal('-');
        wr.expect.to.contain("readonly table");
        // reading an undeclared global errors through the protected base
        auto next = ks.evalRun("return tostring(leaked)");
        next[0].expect.to.equal('-');
        next.expect.to.contain("nonexistent global");
        // writing the shared base directly is blocked too
        auto direct = ks.evalRun("_G.leaked = 42 return 1");
        direct[0].expect.to.equal('-');
        direct.expect.to.contain("readonly table");
        // locals are fine, and KEYS/ARGV still arrive
        ks.evalRun("local x = 42 return x").expect.to.equal(":42\r\n");
        ks.evalRun("return {KEYS[1], ARGV[1]}", "1", "k", "a")
            .expect.to.equal("*2\r\n$1\r\nk\r\n$1\r\na\r\n");
        // redis.call still works under the sandbox
        ks.evalRun("redis.call('SET', KEYS[1], 'v') return redis.call('GET', KEYS[1])",
                "1", "sb").expect.to.equal("$1\r\nv\r\n");
    }

    @("sandbox.random")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();
        // effects replication: no need for a deterministic RNG, so successive
        // scripts draw different values (Redis 7+ behaviour)
        auto a = ks.evalRun("return tostring(math.random(1000000))");
        auto b = ks.evalRun("return tostring(math.random(1000000))");
        a.expect.to.not.equal(b);
        // an explicit seed is still honoured: same seed -> same draw
        auto s1 = ks.evalRun("math.randomseed(42); return tostring(math.random(1000000))");
        auto s2 = ks.evalRun("math.randomseed(42); return tostring(math.random(1000000))");
        s1.expect.to.equal(s2);
    }

    @("sandbox.cjson")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();
        // encode: scalars, arrays, objects, escapes
        ks.evalRun("return cjson.encode({1, 2, 'three'})")
            .expect.to.equal("$13\r\n[1,2,\"three\"]\r\n");
        ks.evalRun("return cjson.encode({nome = 'zé'})")
            .expect.to.contain(`{"nome":"z`);
        ks.evalRun("return cjson.encode({})").expect.to.equal("$2\r\n{}\r\n");
        ks.evalRun("return cjson.encode('a\"b')").expect.to.contain(`\"`);
        ks.evalRun("return cjson.encode(3.5)").expect.to.equal("$3\r\n3.5\r\n");
        ks.evalRun("return cjson.encode(cjson.null)").expect.to.equal("$4\r\nnull\r\n");
        ks.evalRun("return cjson.encode(0/0)")[0].expect.to.equal('-'); // NaN
        // decode: round trip through Lua
        ks.evalRun(`local t = cjson.decode('{"a": [1, 2.5, "x", null, true]}')
                    return {t.a[1], t.a[2] * 2, t.a[3], tostring(t.a[4] == cjson.null), tostring(t.a[5])}`)
            .expect.to.equal("*5\r\n:1\r\n:5\r\n$1\r\nx\r\n$4\r\ntrue\r\n$4\r\ntrue\r\n");
        // unicode escapes decode to UTF-8
        ks.evalRun(`return cjson.decode('"café"')`).expect.to.equal("$5\r\ncaf\xc3\xa9\r\n");
        // full round trip
        ks.evalRun(`return cjson.encode(cjson.decode('{"k":[1,{"n":2}]}'))`)
            .expect.to.equal("$17\r\n{\"k\":[1,{\"n\":2}]}\r\n");
        // invalid json errors cleanly
        ks.evalRun(`return cjson.decode('{oops')`)[0].expect.to.equal('-');
        ks.evalRun(`return cjson.decode('[1,')`)[0].expect.to.equal('-');
        ks.evalRun(`return cjson.decode('1 trailing')`)[0].expect.to.equal('-');
    }

    @("sandbox.cmsgpack")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();
        // byte-exact wire format checks against the msgpack spec
        ks.evalRun("return cmsgpack.pack(1)").expect.to.equal("$1\r\n\x01\r\n");
        ks.evalRun("return cmsgpack.pack(true)").expect.to.equal("$1\r\n\xc3\r\n");
        ks.evalRun("return cmsgpack.pack({1,2,3})")
            .expect.to.equal("$4\r\n\x93\x01\x02\x03\r\n");
        ks.evalRun("return cmsgpack.pack('')").expect.to.equal("$1\r\n\xa0\r\n");
        ks.evalRun("return cmsgpack.pack(-1)").expect.to.equal("$1\r\n\xff\r\n");
        ks.evalRun("return cmsgpack.pack(200)").expect.to.equal("$2\r\n\xcc\xc8\r\n");
        // round trips: scalars, nesting, maps, doubles, negatives
        ks.evalRun(`local b = cmsgpack.pack({a = {1, -300, 2.5, 'x'}, n = 70000})
                    local t = cmsgpack.unpack(b)
                    return {t.a[1], t.a[2], tostring(t.a[3]), t.a[4], t.n}`)
            .expect.to.equal("*5\r\n:1\r\n:-300\r\n$3\r\n2.5\r\n$1\r\nx\r\n:70000\r\n");
        // multiple values: pack(a, b) -> unpack returns both
        ks.evalRun(`local a, b = cmsgpack.unpack(cmsgpack.pack(7, 'oi'))
                    return {a, b}`).expect.to.equal("*2\r\n:7\r\n$2\r\noi\r\n");
        // big string crosses the fixstr boundary and survives
        ks.evalRun(`local s = string.rep('z', 100)
                    return cmsgpack.unpack(cmsgpack.pack(s)) == s and 1 or 0`)
            .expect.to.equal(":1\r\n");
        // robustness: truncated input, unsupported tag, empty
        ks.evalRun(`return cmsgpack.unpack(string.char(0xdc, 0x00))`)[0]
            .expect.to.equal('-'); // truncated array16 header
        ks.evalRun(`return cmsgpack.unpack(string.char(0xc1))`)[0].expect.to.equal('-');
        ks.evalRun(`return cmsgpack.unpack('')`)[0].expect.to.equal('-');
        ks.evalRun(`return cmsgpack.pack()`)[0].expect.to.equal('-');
    }

    @("sandbox.sha1hex_and_bit")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();
        // known SHA1 vectors
        ks.evalRun("return redis.sha1hex('')")
            .expect.to.equal("$40\r\nda39a3ee5e6b4b0d3255bfef95601890afd80709\r\n");
        ks.evalRun("return redis.sha1hex('abc')")
            .expect.to.equal("$40\r\na9993e364706816aba3e25717850c26c9cd0d89d\r\n");
        // bit library, LuaJIT semantics
        ks.evalRun("return bit.band(0xff, 0x0f)").expect.to.equal(":15\r\n");
        ks.evalRun("return bit.bor(1, 2, 4)").expect.to.equal(":7\r\n");
        ks.evalRun("return bit.bxor(5, 3)").expect.to.equal(":6\r\n");
        ks.evalRun("return bit.bnot(0)").expect.to.equal(":-1\r\n");
        ks.evalRun("return bit.lshift(1, 8)").expect.to.equal(":256\r\n");
        ks.evalRun("return bit.rshift(-1, 28)").expect.to.equal(":15\r\n");
        ks.evalRun("return bit.arshift(-16, 2)").expect.to.equal(":-4\r\n");
        ks.evalRun("return bit.tohex(255)").expect.to.equal("$8\r\n000000ff\r\n");
        ks.evalRun("return bit.band('x')")[0].expect.to.equal('-');
    }

    @("sandbox.resource_limits")
    unittest
    {
        import core.time : MonoTime, msecs;

        Keyspace ks;
        scope (exit)
            ks.d.free();

        // memory: cap at 8MB, try to build far more
        auto savedMem = gConfig.luaMemoryLimit;
        gConfig.luaMemoryLimit = 8 * 1024 * 1024;
        scope (exit)
            gConfig.luaMemoryLimit = savedMem;
        auto oom = ks.evalRun(
                "local t = {} for i = 1, 10000000 do t[i] = 'xxxxxxxxxxxxxxxx' end return #t");
        oom[0].expect.to.equal('-'); // clean error, no crash

        // time: 100ms budget against an infinite loop
        auto savedTime = gConfig.luaTimeLimitMs;
        gConfig.luaTimeLimitMs = 100;
        scope (exit)
            gConfig.luaTimeLimitMs = savedTime;
        auto before = MonoTime.currTime;
        auto timedOut = ks.evalRun("while true do end");
        auto took = MonoTime.currTime - before;
        timedOut[0].expect.to.equal('-');
        timedOut.expect.to.contain("lua-time-limit");
        (took < msecs(2000)).expect.to.equal(true); // aborted promptly

        // the state remains usable afterwards
        ks.evalRun("return 7").expect.to.equal(":7\r\n");
    }

    @("sandbox.lua51_compat")
    unittest
    {
        // Redis embeds Lua 5.1; scripts in the wild use the names 5.2+
        // moved or dropped — the compat chunk restores them on our 5.4
        Keyspace ks;
        scope (exit)
            ks.d.free();
        ks.evalRun("return unpack({7})").expect.to.equal(":7\r\n");
        ks.evalRun("return redis.call('RPUSH', KEYS[1], unpack(ARGV))",
                "1", "l", "a", "b", "c").expect.to.equal(":3\r\n");
        ks.evalRun("return math.pow(2, 10)").expect.to.equal(":1024\r\n");
        ks.evalRun("return math.log10(1000)").expect.to.equal(":3\r\n");
        ks.evalRun("return table.getn({1,2,3})").expect.to.equal(":3\r\n");
    }

    @("sandbox.redis_api_stubs")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();
        // effects replication is the only mode, so this is truthfully a yes
        ks.evalRun("if redis.replicate_commands() then return 1 else return 0 end")
            .expect.to.equal(":1\r\n");
        ks.evalRun("redis.log(redis.LOG_WARNING, 'x'); return 1").expect.to.equal(":1\r\n");
        ks.evalRun("return redis.log()")[0].expect.to.equal('-'); // needs 2+ args
        ks.evalRun("redis.setresp(3); return 1").expect.to.equal(":1\r\n");
        ks.evalRun("return redis.setresp(4)")[0].expect.to.equal('-');
    }

    @("sandbox.blocking_commands_one_shot")
    unittest
    {
        // inside a script the blocking family degrades to one immediate
        // attempt (scripts, like replay, can never wait) — and script
        // command names arrive lowercase, which once picked the wrong verb
        Keyspace ks;
        scope (exit)
            ks.d.free();
        ks.evalRun("local r = redis.pcall('blpop', KEYS[1], 0); "
                ~ "return r == false and 1 or 0", "1", "nolist")
            .expect.to.equal(":1\r\n");
        ks.evalRun("redis.call('RPUSH', KEYS[1], 'a', 'b'); "
                ~ "local r = redis.call('blpop', KEYS[1], 0); return r[2]", "1", "bl")
            .expect.to.equal("$1\r\na\r\n");
        ks.evalRun("redis.call('RPUSH', KEYS[1], 'x'); "
                ~ "return redis.call('brpoplpush', KEYS[1], KEYS[2], 0)", "2", "bs", "bd")
            .expect.to.equal("$1\r\nx\r\n");
        ks.evalRun("redis.call('ZADD', KEYS[1], 1, 'lo', 2, 'hi'); "
                ~ "local r = redis.call('bzpopmax', KEYS[1], 0); return r[2]", "1", "bz")
            .expect.to.equal("$2\r\nhi\r\n");
        ks.evalRun("local r = redis.call('bzmpop', 0, 1, KEYS[1], 'MIN'); "
                ~ "return r[1]", "1", "bz").expect.to.equal("$2\r\nbz\r\n");
    }

    @("sandbox.effects_replication")
    unittest
    {
        // the EVAL never enters the log — each redis.call write does, in its
        // propagation form. Capture the sink and check.
        import dreads.scripting : gScriptEffectSink;

        static ByteBuffer captured;
        captured.clear();
        auto savedSink = gScriptEffectSink;
        scope (exit)
            gScriptEffectSink = savedSink;
        gScriptEffectSink = (scope const(ubyte)[] fx) @nogc nothrow {
            captured.append(fx);
        };

        Keyspace ks;
        scope (exit)
            ks.d.free();
        ks.evalRun("redis.call('SETEX', KEYS[1], 100, 'v'); "
                ~ "redis.call('RPUSH', KEYS[2], 'a'); "
                ~ "redis.call('GET', KEYS[1]); return 1", "2", "k", "l")
            .expect.to.equal(":1\r\n");
        auto fx = (cast(string) captured.data).idup;
        fx.expect.to.not.contain("EVAL"); // the script itself is never logged
        fx.expect.to.contain("RPUSH"); // plain writes log verbatim
        fx.expect.to.not.contain("GET\r\n"); // reads leave no effect
        fx.expect.to.not.contain("SETEX"); // logged as its absolute-time form
        fx.expect.to.contain("PXAT"); // ...i.e. SET k v PXAT <deadline>

        // random-then-write is legal under effects replication: the replica
        // replays what happened, not what was rolled
        captured.clear();
        ks.evalRun("redis.call('SADD', KEYS[1], 'a', 'b', 'c'); "
                ~ "local m = redis.call('SRANDMEMBER', KEYS[1]); "
                ~ "return redis.call('SET', KEYS[2], m)", "2", "s", "pick")
            .expect.to.equal("+OK\r\n");
        (cast(string) captured.data).expect.to.contain("SET");

        // a script that fails halfway keeps its earlier writes in the log,
        // exactly like it keeps them in the dataset
        captured.clear();
        auto err = ks.evalRun("redis.call('SET', KEYS[1], 'kept'); "
                ~ "redis.call('INCR', KEYS[1]); return 1", "1", "half");
        err[0].expect.to.equal('-');
        ks.evalRun("return redis.call('GET', KEYS[1])", "1", "half")
            .expect.to.equal("$4\r\nkept\r\n");
        (cast(string) captured.data).expect.to.contain("kept");

        // the clock is frozen for the whole EVAL: TIME is deterministic and
        // in-script relative TTLs match the absolute effects that got logged
        ks.evalRun("local a = redis.call('TIME'); local b = redis.call('TIME'); "
                ~ "return (a[1] == b[1] and a[2] == b[2]) and 1 or 0")
            .expect.to.equal(":1\r\n");
    }
}
