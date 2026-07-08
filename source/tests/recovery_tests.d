module tests.recovery_tests;

// Storage recovery suite: drives commands through dispatch plus the REAL
// logging policy (logAfterDispatch), then reloads the AOF into a fresh
// keyspace and requires byte-identical replies from both. Replay runs the
// same command sequence, so even dict iteration order must match.

version (unittest)
{
    import core.stdc.stdio : remove;

    import fluent.asserts;

    import dreads.aof;
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

    /// dispatch + the server's real AOF logging policy
    private string runLogged(ref Keyspace ks, ref Aof aof, string[] cmdArgs...)
    {
        Arena arena;
        ByteBuffer o;
        RVal v;
        size_t pos = 0;
        propagationOverride.clear(); // never inherit a previous test's override
        auto encoded = cmdArgs.respCmd;
        parseValue(cast(const(ubyte)[]) encoded, pos, arena, v).expect.to.equal(ParseStatus.ok);
        v.dispatch(ks, o, arena);
        char[24] up;
        foreach (i, c; cmdArgs[0])
            up[i] = c >= 'a' && c <= 'z' ? cast(char)(c - 32) : c;
        aof.logAfterDispatch(cast(const(ubyte)[]) encoded, up[0 .. cmdArgs[0].length], o.data);
        return (cast(string) o.data).idup;
    }

    /// dispatch only — used to probe state without touching the log
    private string probe(ref Keyspace ks, string[] cmdArgs...)
    {
        Arena arena;
        ByteBuffer o;
        RVal v;
        size_t pos = 0;
        propagationOverride.clear();
        auto encoded = cmdArgs.respCmd;
        parseValue(cast(const(ubyte)[]) encoded, pos, arena, v).expect.to.equal(ParseStatus.ok);
        v.dispatch(ks, o, arena);
        propagationOverride.clear();
        return (cast(string) o.data).idup;
    }

    private void rmPath(string path)
    {
        (path ~ "\0").ptr.remove;
    }

    private void expectSameReplies(ref Keyspace a, ref Keyspace b, string[][] probes)
    {
        foreach (p; probes)
            b.probe(p).expect.to.equal(a.probe(p));
    }

    @("recovery.full_state_roundtrip")
    unittest
    {
        enum path = "/tmp/dreads_recovery_full.aof";
        path.rmPath;
        scope (exit)
            path.rmPath;

        Keyspace ks;
        scope (exit)
            ks.d.free();
        Aof aof;
        aof.open(path).expect.to.equal(true);

        // strings
        ks.runLogged(aof, "SET", "greeting", "hello");
        ks.runLogged(aof, "APPEND", "greeting", " world");
        ks.runLogged(aof, "INCR", "hits");
        ks.runLogged(aof, "INCRBY", "hits", "9");
        ks.runLogged(aof, "MSET", "m1", "a", "m2", "b");
        // lists (including pops so recovery must replay removals)
        ks.runLogged(aof, "RPUSH", "queue", "j1", "j2", "j3", "j4");
        ks.runLogged(aof, "LPOP", "queue");
        ks.runLogged(aof, "LSET", "queue", "0", "J2");
        ks.runLogged(aof, "LREM", "queue", "1", "j3");
        // hash
        ks.runLogged(aof, "HSET", "user", "name", "alice", "age", "30");
        ks.runLogged(aof, "HINCRBY", "user", "age", "1");
        ks.runLogged(aof, "HDEL", "user", "name");
        // set
        ks.runLogged(aof, "SADD", "tags", "a", "b", "c");
        ks.runLogged(aof, "SREM", "tags", "b");
        // zset
        ks.runLogged(aof, "ZADD", "board", "10", "p1", "20", "p2");
        ks.runLogged(aof, "ZINCRBY", "board", "5", "p1");
        ks.runLogged(aof, "ZREM", "board", "p2");
        // stream with auto id: the log must carry the resolved id
        ks.runLogged(aof, "XADD", "events", "*", "kind", "login");
        ks.runLogged(aof, "XADD", "events", "*", "kind", "logout");
        // deletions and conditional writes
        ks.runLogged(aof, "SET", "gone", "x");
        ks.runLogged(aof, "GETDEL", "gone");
        ks.runLogged(aof, "SETNX", "greeting", "ignored"); // no-op, still deterministic

        aof.close();

        Keyspace ks2;
        scope (exit)
            ks2.d.free();
        path.aofLoad(ks2).expect.to.be.greaterThan(0);

        expectSameReplies(ks, ks2, [
            ["GET", "greeting"], ["GET", "hits"], ["MGET", "m1", "m2"],
            ["LRANGE", "queue", "0", "-1"], ["LLEN", "queue"],
            ["HGETALL", "user"], ["HGET", "user", "age"],
            ["SMEMBERS", "tags"], ["SCARD", "tags"],
            ["ZRANGE", "board", "0", "-1", "WITHSCORES"],
            ["XRANGE", "events", "-", "+"], ["XLEN", "events"],
            ["EXISTS", "gone"], ["DBSIZE"],
            ["TYPE", "greeting"], ["TYPE", "queue"], ["TYPE", "user"],
            ["TYPE", "tags"], ["TYPE", "board"], ["TYPE", "events"],
        ]);
    }

    @("recovery.multi_generation")
    unittest
    {
        enum path = "/tmp/dreads_recovery_gen.aof";
        path.rmPath;
        scope (exit)
            path.rmPath;

        // generation 1
        {
            Keyspace ks;
            scope (exit)
                ks.d.free();
            Aof aof;
            aof.open(path).expect.to.equal(true);
            ks.runLogged(aof, "SET", "g1", "one");
            ks.runLogged(aof, "RPUSH", "l", "a");
            aof.close();
        }
        // generation 2: recover, keep appending to the same file
        Keyspace ks2;
        scope (exit)
            ks2.d.free();
        path.aofLoad(ks2).expect.to.equal(2);
        {
            Aof aof;
            aof.open(path).expect.to.equal(true); // "ab" appends
            ks2.runLogged(aof, "SET", "g2", "two");
            ks2.runLogged(aof, "RPUSH", "l", "b");
            ks2.runLogged(aof, "DEL", "g1");
            aof.close();
        }
        // generation 3 sees the cumulative history
        Keyspace ks3;
        scope (exit)
            ks3.d.free();
        path.aofLoad(ks3).expect.to.equal(5);
        expectSameReplies(ks2, ks3, [
            ["GET", "g1"], ["GET", "g2"], ["LRANGE", "l", "0", "-1"], ["DBSIZE"],
        ]);
        ks3.probe("GET", "g2").expect.to.equal("$3\r\ntwo\r\n");
        ks3.probe("EXISTS", "g1").expect.to.equal(":0\r\n");
    }

    @("recovery.ttl_survives_with_absolute_time")
    unittest
    {
        enum path = "/tmp/dreads_recovery_ttl.aof";
        path.rmPath;
        scope (exit)
            path.rmPath;

        Keyspace ks;
        scope (exit)
            ks.d.free();
        Aof aof;
        aof.open(path).expect.to.equal(true);
        ks.runLogged(aof, "SET", "rel", "v");
        ks.runLogged(aof, "EXPIRE", "rel", "3600"); // relative, logged absolute
        ks.runLogged(aof, "SET", "abs", "v", "PXAT", "4102444800000"); // year 2100
        ks.runLogged(aof, "SET", "dead", "v");
        ks.runLogged(aof, "PEXPIREAT", "dead", "1"); // 1970: expired
        aof.close();

        Keyspace ks2;
        scope (exit)
            ks2.d.free();
        path.aofLoad(ks2).expect.to.be.greaterThan(0);

        // identical absolute expiry, not re-derived from "now"
        (ks2.lookup("rel") !is null).expect.to.equal(true);
        ks2.lookup("rel").expireAtMs.expect.to.equal(ks.lookup("rel").expireAtMs);
        ks2.lookup("abs").expireAtMs.expect.to.equal(4_102_444_800_000UL);
        // an expired key must not resurrect
        ks2.probe("EXISTS", "dead").expect.to.equal(":0\r\n");
    }

    @("recovery.reads_and_errors_are_not_logged")
    unittest
    {
        enum path = "/tmp/dreads_recovery_ro.aof";
        path.rmPath;
        scope (exit)
            path.rmPath;

        Keyspace ks;
        scope (exit)
            ks.d.free();
        Aof aof;
        aof.open(path).expect.to.equal(true);
        ks.runLogged(aof, "SET", "k", "text"); // 1st write
        ks.runLogged(aof, "GET", "k"); // read
        ks.runLogged(aof, "TTL", "k"); // read
        ks.runLogged(aof, "GETEX", "k"); // read (no ttl change)
        ks.runLogged(aof, "INCR", "k")[0].expect.to.equal('-'); // error: not logged
        ks.runLogged(aof, "RPUSH", "l", "x"); // 2nd write
        ks.runLogged(aof, "LRANGE", "l", "0", "-1"); // read
        aof.close();

        Keyspace ks2;
        scope (exit)
            ks2.d.free();
        path.aofLoad(ks2).expect.to.equal(2); // only the two effective writes
    }

    @("recovery.xadd_auto_ids_are_stable")
    unittest
    {
        enum path = "/tmp/dreads_recovery_xadd.aof";
        path.rmPath;
        scope (exit)
            path.rmPath;

        Keyspace ks;
        scope (exit)
            ks.d.free();
        Aof aof;
        aof.open(path).expect.to.equal(true);
        ks.runLogged(aof, "XADD", "s", "*", "n", "1");
        ks.runLogged(aof, "XADD", "s", "*", "n", "2");
        ks.runLogged(aof, "XADD", "s", "*", "n", "3");
        aof.close();

        auto before = ks.probe("XRANGE", "s", "-", "+");

        Keyspace ks2;
        scope (exit)
            ks2.d.free();
        path.aofLoad(ks2).expect.to.equal(3);
        ks2.probe("XRANGE", "s", "-", "+").expect.to.equal(before);
    }
}
