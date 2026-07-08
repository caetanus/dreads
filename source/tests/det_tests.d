module tests.det_tests;

// Determinism: applying the same command with the same injected clock must
// produce identical state on every replica — the property that lets the Raft
// log resolve time-dependent commands deterministically.

version (unittest)
{
    import fluent.asserts;

    import dreads.commands : dispatch, propagationOverride;
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

    private string apply(ref Keyspace ks, ulong clock, string[] cmdArgs...)
    {
        Arena arena;
        ByteBuffer o;
        RVal v;
        size_t pos = 0;
        propagationOverride.clear();
        auto encoded = cmdArgs.respCmd;
        parseValue(cast(const(ubyte)[]) encoded, pos, arena, v).expect.to.equal(ParseStatus.ok);
        v.dispatch(ks, o, arena, clock); // inject the frozen clock
        return (cast(string) o.data).idup;
    }

    @("det.expire_resolves_against_injected_clock")
    unittest
    {
        // two "replicas" apply EXPIRE with the SAME injected clock
        enum ulong T = 1_000_000_000_000; // fixed logical time
        Keyspace a, b;
        scope (exit)
        {
            a.d.free();
            b.d.free();
        }
        a.apply(T, "SET", "k", "v");
        b.apply(T, "SET", "k", "v");
        a.apply(T, "EXPIRE", "k", "100");
        b.apply(T + 5000, "EXPIRE", "k", "100"); // different wall time...
        // ...but each stamps its OWN clock; to converge, replicas share the
        // leader's clock. Here we prove the absolute expiry equals clock+100s:
        a.lookup("k").expireAtMs.expect.to.equal(T + 100_000);
        b.lookup("k").expireAtMs.expect.to.equal(T + 5000 + 100_000);
        // same injected clock => identical expiry (the replication invariant)
        Keyspace c;
        scope (exit)
            c.d.free();
        c.apply(T, "SET", "k", "v");
        c.apply(T, "EXPIRE", "k", "100");
        c.lookup("k").expireAtMs.expect.to.equal(a.lookup("k").expireAtMs);
    }

    @("det.xadd_star_id_is_clock_derived")
    unittest
    {
        enum ulong T = 1_700_000_000_000;
        Keyspace a, b;
        scope (exit)
        {
            a.d.free();
            b.d.free();
        }
        // same injected clock + same (empty) stream state => identical ID
        auto ra = a.apply(T, "XADD", "s", "*", "f", "v");
        auto rb = b.apply(T, "XADD", "s", "*", "f", "v");
        ra.expect.to.equal(rb);
        ra.expect.to.contain("1700000000000-0");
        // a later logged clock advances the ms part deterministically
        auto ra2 = a.apply(T + 1, "XADD", "s", "*", "f", "v");
        auto rb2 = b.apply(T + 1, "XADD", "s", "*", "f", "v");
        ra2.expect.to.equal(rb2);
        ra2.expect.to.contain("1700000000001-0");
    }

    @("det.lazy_expiry_uses_frozen_clock")
    unittest
    {
        enum ulong T = 500_000_000_000;
        Keyspace ks;
        scope (exit)
            ks.d.free();
        ks.apply(T, "SET", "k", "v");
        ks.apply(T, "PEXPIREAT", "k", (T + 1000).to_str);
        // a read at a frozen clock before expiry sees the key...
        ks.apply(T + 500, "EXISTS", "k").expect.to.equal(":1\r\n");
        // ...and after expiry it is gone, decided by the injected clock
        ks.apply(T + 2000, "EXISTS", "k").expect.to.equal(":0\r\n");
    }

    private string to_str(ulong v)
    {
        import std.conv : to;

        return v.to!string;
    }
}
