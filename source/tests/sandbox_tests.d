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
        // io/os/package/debug are not loaded; reading them trips _G protection
        foreach (lib; ["os", "io", "debug", "package", "require", "dofile",
                "loadfile", "load", "print"])
        {
            auto reply = ks.run2(lib);
            reply[0].expect.to.equal('-');
            reply.expect.to.contain("global");
        }
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
        // globals live in a throwaway per-run _ENV: usable within the script...
        ks.evalRun("leaked = 42 return leaked").expect.to.equal(":42\r\n");
        // ...but gone on the next run (reads chain to the protected base)
        auto next = ks.evalRun("return tostring(leaked)");
        next[0].expect.to.equal('-');
        next.expect.to.contain("nonexistent global");
        // writing the shared base directly is still blocked
        auto direct = ks.evalRun("_G.leaked = 42 return 1");
        direct[0].expect.to.equal('-');
        direct.expect.to.contain("create global");
        // locals are fine, and KEYS/ARGV still arrive
        ks.evalRun("local x = 42 return x").expect.to.equal(":42\r\n");
        ks.evalRun("return {KEYS[1], ARGV[1]}", "1", "k", "a")
            .expect.to.equal("*2\r\n$1\r\nk\r\n$1\r\na\r\n");
        // redis.call still works under the sandbox
        ks.evalRun("redis.call('SET', KEYS[1], 'v') return redis.call('GET', KEYS[1])",
                "1", "sb").expect.to.equal("$1\r\nv\r\n");
    }

    @("sandbox.deterministic_random")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();
        auto a = ks.evalRun("return tostring(math.random(1000000))");
        auto b = ks.evalRun("return tostring(math.random(1000000))");
        a.expect.to.equal(b); // reseeded per invocation
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
}
