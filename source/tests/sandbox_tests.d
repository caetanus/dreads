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
