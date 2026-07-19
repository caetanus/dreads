module tests.valkey_hll_tests;

// Native in-process port of Valkey unit/hyperloglog.tcl. The .tcl is a
// read-only spec/oracle only — nothing tcl lands here (see THIRD_PARTY_NOTICES
// for the BSD-3 credit). Grounded against valkey-server on :7524.
//
// dreads HLL specifics that shape this port (dreads.hll):
//   * DENSE ENCODING ONLY. There is no sparse rep, no PFDEBUG, no PFSELFTEST.
//     A fresh HLL is always the full 12304-byte dense object.
//     => the `foreach encoding` / sparse<->dense promotion / hll-sparse-max-bytes
//        / PFDEBUG GETREG/ENCODING/TODENSE/SIMD / PFSELFTEST cases are SKIPPED
//        (server/DEBUG-only or encoding-duplicate).
//   * Small cardinalities are EXACT: linear counting keys off the count of
//     non-zero registers, and <=~few-hundred distinct elements land in distinct
//     registers, so PFCOUNT of N distinct elements == N. All exact asserts here
//     stay well inside that regime.
//   * Corruption detection is length+magic only: isHll() requires
//     length==DENSE_BYTES && bytes[0..4]=="HYLL". So a corrupted TAIL (APPEND) or
//     a broken MAGIC both surface as WRONGTYPE ("not a valid HyperLogLog string
//     value."), NOT the INVALIDOBJ that Valkey's structural walker raises.
//     dreads does NOT validate the encoding byte (offset 4), so SETRANGE at
//     offset 4 stays a valid HLL — that Valkey case is a documented divergence
//     and is NOT asserted as an error here.
//   * No cached cardinality: estimate() always recomputes, header byte 15 stays
//     0x00 forever (Valkey flips it to 0x80 on modify). We assert dreads' 0x00;
//     the 0x80 half of the Valkey cache-invalidation case is a divergence.

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
            immutable ee = s[i .. $].indexOf("\r\n") + i;
            immutable ln = s[i + 1 .. ee].to!int;
            i = ee + 2;
            if (ln < 0)
            {
                r ~= null;
                continue;
            }
            r ~= s[i .. i + ln];
            i += ln + 2;
        }
        return r;
    }

    private void sameSet(string reply, string[] exp...)
    {
        auto g = parseArr(reply);
        sort(g);
        auto e = exp.dup;
        sort(e);
        g.expect.to.equal(e);
    }

    enum NIL = "$-1\r\n";

    // Fresh dense HLL object size in dreads (16-byte header + 16384 six-bit regs).
    enum DENSE_BYTES = 16 + (16384 * 6 + 7) / 8; // == 12304

    // ---- PFADD create / return semantics ---------------------------------
    @("valkey.hll.pfadd_create")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        // PFADD with no elements creates an HLL value; EXISTS => 1.
        ks.run("PFADD", "hll").expect.to.equal(":1\r\n"); // creation is a change
        ks.run("EXISTS", "hll").expect.to.equal(":1\r\n");
        // Approximated cardinality right after creation is zero.
        ks.run("PFCOUNT", "hll").expect.to.equal(":0\r\n");

        // PFADD returns 1 when at least one register was modified.
        ks.run("PFADD", "hll", "a", "b", "c").expect.to.equal(":1\r\n");
        // PFADD returns 0 when no register was modified.
        ks.run("PFADD", "hll", "a", "b", "c").expect.to.equal(":0\r\n");

        // PFADD with an empty-string element (regression) — still valid,
        // and since "" is a brand-new element it modifies a register => 1.
        ks.run("PFADD", "hll", "").expect.to.equal(":1\r\n");
        // adding "" again changes nothing.
        ks.run("PFADD", "hll", "").expect.to.equal(":0\r\n");

        // A fresh HLL is a dense object of the fixed size.
        ks.run("STRLEN", "hll").expect.to.equal(":" ~ DENSE_BYTES.to!string ~ "\r\n");
    }

    // ---- PFCOUNT exact small cardinalities -------------------------------
    @("valkey.hll.pfcount_exact")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        // PFCOUNT of a missing key is 0 (no creation).
        ks.run("PFCOUNT", "nope").expect.to.equal(":0\r\n");
        ks.run("EXISTS", "nope").expect.to.equal(":0\r\n");

        // Distinct-element counts are exact in the linear-counting range.
        ks.run("PFADD", "c1", "a").expect.to.equal(":1\r\n");
        ks.run("PFCOUNT", "c1").expect.to.equal(":1\r\n");

        ks.run("PFADD", "c2", "a", "b").expect.to.equal(":1\r\n");
        ks.run("PFCOUNT", "c2").expect.to.equal(":2\r\n");

        ks.run("PFADD", "c3", "a", "b", "c").expect.to.equal(":1\r\n");
        ks.run("PFCOUNT", "c3").expect.to.equal(":3\r\n");

        ks.run("PFADD", "c5", "a", "b", "c", "d", "e").expect.to.equal(":1\r\n");
        ks.run("PFCOUNT", "c5").expect.to.equal(":5\r\n");

        // Duplicate elements in one PFADD collapse.
        ks.run("PFADD", "dup", "a", "a", "a", "b", "b", "c").expect.to.equal(":1\r\n");
        ks.run("PFCOUNT", "dup").expect.to.equal(":3\r\n");

        // Cardinality grows across successive adds and cache stays consistent.
        ks.run("PFADD", "grow", "1", "2", "3", "4", "5");
        ks.run("PFCOUNT", "grow").expect.to.equal(":5\r\n");
        ks.run("PFADD", "grow", "6", "7", "8", "8", "9", "10"); // 8 duplicated
        ks.run("PFCOUNT", "grow").expect.to.equal(":10\r\n");
    }

    // ---- PFCOUNT over multiple keys = union ------------------------------
    @("valkey.hll.pfcount_union")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        ks.run("PFADD", "h1", "a", "b", "c");
        ks.run("PFADD", "h2", "b", "c", "d");
        ks.run("PFADD", "h3", "c", "d", "e");

        // union {a,b,c,d,e} == 5, sources unchanged.
        ks.run("PFCOUNT", "h1", "h2", "h3").expect.to.equal(":5\r\n");
        ks.run("PFCOUNT", "h1").expect.to.equal(":3\r\n");

        // A missing key in the multi-key set contributes nothing.
        ks.run("PFCOUNT", "h1", "missing").expect.to.equal(":3\r\n");
        // All-missing multi-key set => 0.
        ks.run("PFCOUNT", "no1", "no2").expect.to.equal(":0\r\n");
    }

    // ---- PFMERGE union semantics -----------------------------------------
    @("valkey.hll.pfmerge_union")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        ks.run("PFADD", "hll1", "a", "b", "c");
        ks.run("PFADD", "hll2", "b", "c", "d");
        ks.run("PFADD", "hll3", "c", "d", "e");

        // merge the three into a fresh dest.
        ks.run("PFMERGE", "hll", "hll1", "hll2", "hll3").expect.to.equal("+OK\r\n");
        ks.run("PFCOUNT", "hll").expect.to.equal(":5\r\n"); // {a..e}

        // Merging into an existing non-empty dest unions with its own contents.
        ks.run("PFADD", "acc", "a", "b");
        ks.run("PFADD", "src", "b", "c");
        ks.run("PFMERGE", "acc", "src").expect.to.equal("+OK\r\n");
        ks.run("PFCOUNT", "acc").expect.to.equal(":3\r\n"); // {a,b,c}
    }

    // ---- PFMERGE edge cases: missing sources, self-merge -----------------
    @("valkey.hll.pfmerge_edges")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        // Merge with one missing source key creates an empty dest.
        ks.run("PFMERGE", "dst", "srcmissing").expect.to.equal("+OK\r\n");
        ks.run("EXISTS", "dst").expect.to.equal(":1\r\n");
        ks.run("PFCOUNT", "dst").expect.to.equal(":0\r\n");

        // Merge with several missing sources still creates an empty dest.
        ks.run("PFMERGE", "dst2", "m1", "m2").expect.to.equal("+OK\r\n");
        ks.run("EXISTS", "dst2").expect.to.equal(":1\r\n");
        ks.run("PFCOUNT", "dst2").expect.to.equal(":0\r\n");

        // PFMERGE with only a dest (no sources) creates an empty dest.
        ks.run("PFMERGE", "solo").expect.to.equal("+OK\r\n");
        ks.run("EXISTS", "solo").expect.to.equal(":1\r\n");
        ks.run("PFCOUNT", "solo").expect.to.equal(":0\r\n");

        // PFMERGE with only a non-empty dest (no sources) preserves it.
        ks.run("PFADD", "keep", "a", "b", "c").expect.to.equal(":1\r\n");
        ks.run("PFMERGE", "keep").expect.to.equal("+OK\r\n");
        ks.run("EXISTS", "keep").expect.to.equal(":1\r\n");
        ks.run("PFCOUNT", "keep").expect.to.equal(":3\r\n");
    }

    // ---- Type checking: WRONGTYPE on non-HLL string / other types --------
    @("valkey.hll.wrongtype")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        // A plain string is not a valid HLL.
        ks.run("SET", "foo", "bar").expect.to.equal("+OK\r\n");
        ks.run("PFADD", "foo", "1").startsWith("-WRONGTYPE").should.equal(true);
        ks.run("PFCOUNT", "foo").startsWith("-WRONGTYPE").should.equal(true);

        // A valid HLL for the other side of the merge.
        ks.run("PFADD", "good", "a").expect.to.equal(":1\r\n");
        // dest is the bad string.
        ks.run("PFMERGE", "foo", "good").startsWith("-WRONGTYPE").should.equal(true);
        // source is the bad string.
        ks.run("PFMERGE", "dst", "foo").startsWith("-WRONGTYPE").should.equal(true);

        // Multi-key PFCOUNT that hits a non-HLL key errors.
        ks.run("PFCOUNT", "good", "foo").startsWith("-WRONGTYPE").should.equal(true);
    }

    // ---- Corruption detection --------------------------------------------
    @("valkey.hll.corruption")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        // Additional bytes at the tail break the fixed length => WRONGTYPE.
        // (Valkey raises INVALIDOBJ here via its structural walker; dreads is
        // dense-only and validates by length+magic, so it is WRONGTYPE.)
        ks.run("PFADD", "t1", "a", "b", "c");
        ks.run("APPEND", "t1", "hello");
        ks.run("PFCOUNT", "t1").startsWith("-WRONGTYPE").should.equal(true);
        ks.run("PFADD", "t1", "x").startsWith("-WRONGTYPE").should.equal(true);

        // Broken magic (overwrite the "HYLL" header) => WRONGTYPE.
        ks.run("PFADD", "t2", "a", "b", "c");
        ks.run("SETRANGE", "t2", "0", "0123");
        ks.run("PFCOUNT", "t2").startsWith("-WRONGTYPE").should.equal(true);
    }

    // ---- Arity errors -----------------------------------------------------
    @("valkey.hll.arity")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        ks.run("PFADD").startsWith("-ERR").should.equal(true);
        ks.run("PFCOUNT").startsWith("-ERR").should.equal(true);
        ks.run("PFMERGE").startsWith("-ERR").should.equal(true);
    }

    // ---- Header cache byte stays 0x00 (dreads recomputes; no cache) ------
    @("valkey.hll.no_cache_byte")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        // Valkey flips header byte 15 to 0x80 when the HLL is modified after a
        // PFCOUNT (cache-invalidation marker). dreads has no cached cardinality
        // — estimate() always recomputes — so byte 15 stays 0x00 throughout.
        ks.run("PFADD", "hll", "a", "b", "c");
        ks.run("PFCOUNT", "hll"); // would set the cache in Valkey
        ks.run("GETRANGE", "hll", "15", "15").expect.to.equal(bulk("\x00"));
        ks.run("PFADD", "hll", "1", "2", "3"); // real modification
        ks.run("GETRANGE", "hll", "15", "15").expect.to.equal(bulk("\x00"));
    }
}
