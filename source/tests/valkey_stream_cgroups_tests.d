module tests.valkey_stream_cgroups_tests;

// Valkey unit/type/stream-cgroups.tcl ported to native in-process unittests (see
// valkey_incr_tests.d for rationale + THIRD_PARTY_NOTICES credit). Consumer groups
// live in the stream RObj (not connection state), so XGROUP/XREADGROUP/XACK/XPENDING/
// XCLAIM/XAUTOCLAIM/XINFO are unit-testable with explicit IDs + explicit consumer
// names. The tcl is a read-only oracle: expected reply bytes were captured from a
// live valkey-server. Blocking XREADGROUP (BLOCK), deferring/multiple clients,
// replication/slaveof, DEBUG LOADAOF/RELOAD, RESTORE-of-legacy-RDB, wall-clock IDLE
// timing, and XINFO CONSUMERS idle/inactive (non-deterministic clock) stay in the
// blackbox sweep. The extended XPENDING form's field-3 idle time is likewise a wall
// clock, so those cases parse the reply and assert on id/consumer/delivery only.

version (unittest)
{
    import fluent.asserts;
    import std.conv : to;
    import std.string : startsWith;
    import std.algorithm : canFind;

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

    // one XRANGE-style entry: *2 [ id, [f, v, ...] ]
    private string entry(string id, string[] fv...)
    {
        string fields = "*" ~ fv.length.to!string ~ "\r\n";
        foreach (x; fv)
            fields ~= bulk(x);
        return "*2\r\n" ~ bulk(id) ~ fields;
    }

    // one XREADGROUP history entry with deleted body: *2 [ id, nil-ARRAY ].
    // Valkey emits the deleted field-values as a nil *array* (*-1), not a nil bulk.
    private string tombstone(string id)
    {
        return "*2\r\n" ~ bulk(id) ~ "*-1\r\n";
    }

    // XREADGROUP top wrapper: *1 [ *2 [ key, [entries...] ] ]
    private string streamReply(string key, string entries)
    {
        return "*1\r\n*2\r\n" ~ bulk(key) ~ entries;
    }

    enum NIL = "$-1\r\n";
    enum NILARR = "*-1\r\n";

    @("valkey.streamcg.create_and_dup")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        // XGROUP CREATE on a missing key without MKSTREAM -> ERR
        ks.run("XGROUP", "CREATE", "s", "g", "$").startsWith(
            "-ERR The XGROUP subcommand requires the key to exist").should.equal(true);

        // MKSTREAM creates the empty stream and the group
        ks.run("XGROUP", "CREATE", "s", "g", "$", "MKSTREAM").expect.to.equal("+OK\r\n");
        ks.run("XLEN", "s").expect.to.equal(":0\r\n");

        // duplicate group name -> BUSYGROUP
        ks.run("XGROUP", "CREATE", "s", "g", "$").startsWith("-BUSYGROUP").should.equal(true);
    }

    @("valkey.streamcg.create_entriesread")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        ks.run("XADD", "s", "1-1", "a", "1");
        ks.run("XADD", "s", "1-2", "b", "2");
        ks.run("XADD", "s", "1-3", "c", "3");
        ks.run("XADD", "s", "1-4", "d", "4");

        // negative ENTRIESREAD (other than -1) -> error
        ks.run("XGROUP", "CREATE", "s", "g", "$", "ENTRIESREAD", "-3")
            .startsWith("-ERR value for ENTRIESREAD must be positive or -1").should.equal(true);

        ks.run("XGROUP", "CREATE", "s", "mygroup1", "$", "ENTRIESREAD", "0").expect.to.equal(
            "+OK\r\n");
        ks.run("XGROUP", "CREATE", "s", "mygroup2", "$", "ENTRIESREAD", "3").expect.to.equal(
            "+OK\r\n");

        // XINFO GROUPS reflects entries-read exactly as given (group iteration order
        // is hash-order, so assert order-independently). Both groups are created at
        // the tail ("$" -> last-delivered-id 1-4). mygroup1: entries-read 0, lag 4;
        // mygroup2: entries-read 3, lag 1.
        auto info = ks.run("XINFO", "GROUPS", "s");
        info.startsWith("*2\r\n").should.equal(true); // two groups
        info.canFind(bulk("mygroup1")).should.equal(true);
        info.canFind(bulk("mygroup2")).should.equal(true);
        info.canFind(bulk("entries-read") ~ ":0\r\n").should.equal(true);
        info.canFind(bulk("entries-read") ~ ":3\r\n").should.equal(true);
        info.canFind(bulk("lag") ~ ":4\r\n").should.equal(true);
        info.canFind(bulk("lag") ~ ":1\r\n").should.equal(true);
    }

    @("valkey.streamcg.readgroup_new_and_history")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        ks.run("XADD", "mystream", "1-1", "a", "1");
        ks.run("XADD", "mystream", "2-1", "b", "2");
        ks.run("XGROUP", "CREATE", "mystream", "mygroup", "0");

        // ">" reads only never-delivered entries and puts them in the PEL
        ks.run("XREADGROUP", "GROUP", "mygroup", "consumer-1", "STREAMS", "mystream", ">")
            .expect.to.equal(streamReply("mystream",
                "*2\r\n" ~ entry("1-1", "a", "1") ~ entry("2-1", "b", "2")));

        ks.run("XADD", "mystream", "3-1", "c", "3");
        ks.run("XADD", "mystream", "4-1", "d", "4");

        // a different consumer reads the next new entries
        ks.run("XREADGROUP", "GROUP", "mygroup", "consumer-2", "STREAMS", "mystream", ">")
            .expect.to.equal(streamReply("mystream",
                "*2\r\n" ~ entry("3-1", "c", "3") ~ entry("4-1", "d", "4")));

        // reading "0" replays each consumer's own still-pending history
        ks.run("XREADGROUP", "GROUP", "mygroup", "consumer-1", "COUNT", "10", "STREAMS",
            "mystream", "0").expect.to.equal(streamReply("mystream",
                "*2\r\n" ~ entry("1-1", "a", "1") ~ entry("2-1", "b", "2")));
        ks.run("XREADGROUP", "GROUP", "mygroup", "consumer-2", "COUNT", "10", "STREAMS",
            "mystream", "0").expect.to.equal(streamReply("mystream",
                "*2\r\n" ~ entry("3-1", "c", "3") ~ entry("4-1", "d", "4")));

        // a fresh ">" read with nothing new returns a nil array
        ks.run("XREADGROUP", "GROUP", "mygroup", "consumer-1", "STREAMS", "mystream", ">")
            .expect.to.equal(NILARR);
    }

    @("valkey.streamcg.xpending_forms")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        ks.run("XADD", "mystream", "1-1", "a", "1");
        ks.run("XADD", "mystream", "2-1", "b", "2");
        ks.run("XADD", "mystream", "3-1", "c", "3");
        ks.run("XADD", "mystream", "4-1", "d", "4");
        ks.run("XGROUP", "CREATE", "mystream", "mygroup", "0");
        // consumer-1 gets first 2, consumer-2 gets next 2
        ks.run("XREADGROUP", "GROUP", "mygroup", "consumer-1", "COUNT", "2", "STREAMS",
            "mystream", ">");
        ks.run("XREADGROUP", "GROUP", "mygroup", "consumer-2", "COUNT", "2", "STREAMS",
            "mystream", ">");

        // summary: [count, min-id, max-id, [[consumer,count]...]]
        ks.run("XPENDING", "mystream", "mygroup").expect.to.equal(
            "*4\r\n:4\r\n" ~ bulk("1-1") ~ bulk("4-1") ~ "*2\r\n"
            ~ "*2\r\n" ~ bulk("consumer-1") ~ bulk("2")
            ~ "*2\r\n" ~ bulk("consumer-2") ~ bulk("2"));

        // extended full range: 4 entries (idle field is a wall clock -> structural asserts)
        auto ext = ks.run("XPENDING", "mystream", "mygroup", "-", "+", "10");
        ext.startsWith("*4\r\n").should.equal(true); // 4 pending rows
        ext.canFind(bulk("1-1")).should.equal(true);
        ext.canFind(bulk("2-1")).should.equal(true);
        ext.canFind(bulk("3-1")).should.equal(true);
        ext.canFind(bulk("4-1")).should.equal(true);
        ext.canFind(bulk("consumer-1")).should.equal(true);
        ext.canFind(bulk("consumer-2")).should.equal(true);

        // single-consumer filter: 2 rows
        auto c1 = ks.run("XPENDING", "mystream", "mygroup", "-", "+", "10", "consumer-1");
        c1.startsWith("*2\r\n").should.equal(true);
        c1.canFind(bulk("1-1")).should.equal(true);
        c1.canFind(bulk("2-1")).should.equal(true);
        c1.canFind(bulk("3-1")).should.equal(false);

        // exclusive range ( .. ( drops the endpoints -> 2 rows (2-1, 3-1)
        auto exr = ks.run("XPENDING", "mystream", "mygroup", "(1-1", "(4-1", "10");
        exr.startsWith("*2\r\n").should.equal(true);
        exr.canFind(bulk("1-1")).should.equal(false);
        exr.canFind(bulk("4-1")).should.equal(false);
        exr.canFind(bulk("2-1")).should.equal(true);
        exr.canFind(bulk("3-1")).should.equal(true);
    }

    @("valkey.streamcg.xpending_empty")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        ks.run("XGROUP", "CREATE", "e", "g", "0", "MKSTREAM");
        // empty summary: count 0, nil min, nil max, nil consumer array
        ks.run("XPENDING", "e", "g").expect.to.equal("*4\r\n:0\r\n" ~ NIL ~ NIL ~ NILARR);
        // empty extended range -> empty array
        ks.run("XPENDING", "e", "g", "-", "+", "10").expect.to.equal("*0\r\n");
    }

    @("valkey.streamcg.xack")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        ks.run("XADD", "s", "1-1", "a", "1");
        ks.run("XADD", "s", "2-1", "b", "2");
        ks.run("XGROUP", "CREATE", "s", "g", "0");
        ks.run("XREADGROUP", "GROUP", "g", "c1", "STREAMS", "s", ">");

        // XACK removes an entry from the PEL
        ks.run("XACK", "s", "g", "1-1").expect.to.equal(":1\r\n");
        // can't ack the same entry twice
        ks.run("XACK", "s", "g", "1-1").expect.to.equal(":0\r\n");
        // multiple ids -> counts only the ones actually removed
        ks.run("XACK", "s", "g", "1-1", "2-1").expect.to.equal(":1\r\n");
        // group PEL now empty
        ks.run("XPENDING", "s", "g").expect.to.equal("*4\r\n:0\r\n" ~ NIL ~ NIL ~ NILARR);
    }

    @("valkey.streamcg.xack_invalid_id")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        ks.run("XGROUP", "CREATE", "s", "g", "$", "MKSTREAM");
        ks.run("XADD", "s", "1-1", "f1", "v1");
        ks.run("XREADGROUP", "GROUP", "g", "c", "STREAMS", "s", ">");
        // at least one invalid ID -> error, nothing acked
        ks.run("XACK", "s", "g", "1-1", "invalid-id").startsWith(
            "-ERR Invalid stream ID specified").should.equal(true);
        // the valid ack still works
        ks.run("XACK", "s", "g", "1-1").expect.to.equal(":1\r\n");
    }

    @("valkey.streamcg.setid_reassign")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        ks.run("XADD", "events", "1-0", "f1", "v1");
        ks.run("XADD", "events", "2-0", "f1", "v1");
        ks.run("XADD", "events", "3-0", "f1", "v1");
        ks.run("XADD", "events", "4-0", "f1", "v1");
        ks.run("XGROUP", "CREATE", "events", "g1", "$");
        ks.run("XADD", "events", "5-0", "f1", "v1");

        // only the entry added after group-creation is new
        ks.run("XREADGROUP", "GROUP", "g1", "c1", "STREAMS", "events", ">")
            .expect.to.equal(streamReply("events", "*1\r\n" ~ entry("5-0", "f1", "v1")));

        // rewind the group and a new consumer sees all 5
        ks.run("XGROUP", "SETID", "events", "g1", "-").expect.to.equal("+OK\r\n");
        ks.run("XREADGROUP", "GROUP", "g1", "c2", "STREAMS", "events", ">")
            .expect.to.equal(streamReply("events", "*5\r\n"
                ~ entry("1-0", "f1", "v1") ~ entry("2-0", "f1", "v1")
                ~ entry("3-0", "f1", "v1") ~ entry("4-0", "f1", "v1")
                ~ entry("5-0", "f1", "v1")));
    }

    @("valkey.streamcg.empty_history_bug5577")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        ks.run("XADD", "events", "1-0", "a", "1");
        ks.run("XADD", "events", "2-0", "b", "2");
        ks.run("XADD", "events", "3-0", "c", "3");
        ks.run("XGROUP", "CREATE", "events", "mygroup", "0");

        // local PEL empty -> reading "0" returns the stream with an EMPTY list
        ks.run("XREADGROUP", "GROUP", "mygroup", "myc", "COUNT", "3", "STREAMS", "events", "0")
            .expect.to.equal("*1\r\n*2\r\n" ~ bulk("events") ~ "*0\r\n");

        // ">" fetches all three
        ks.run("XREADGROUP", "GROUP", "mygroup", "myc", "COUNT", "3", "STREAMS", "events", ">")
            .expect.to.equal(streamReply("events", "*3\r\n"
                ~ entry("1-0", "a", "1") ~ entry("2-0", "b", "2") ~ entry("3-0", "c", "3")));

        // now history is populated with three not-acked entries
        ks.run("XREADGROUP", "GROUP", "mygroup", "myc", "COUNT", "3", "STREAMS", "events", "0")
            .expect.to.equal(streamReply("events", "*3\r\n"
                ~ entry("1-0", "a", "1") ~ entry("2-0", "b", "2") ~ entry("3-0", "c", "3")));
    }

    @("valkey.streamcg.history_deleted_bug5570")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        ks.run("XGROUP", "CREATE", "mystream", "mygroup", "$", "MKSTREAM");
        ks.run("XADD", "mystream", "1", "field1", "A");
        ks.run("XREADGROUP", "GROUP", "mygroup", "myc", "STREAMS", "mystream", ">");
        // MAXLEN 1 trims 1-0 out of the stream while it's still pending
        ks.run("XADD", "mystream", "MAXLEN", "1", "2", "field1", "B");
        ks.run("XREADGROUP", "GROUP", "mygroup", "myc", "STREAMS", "mystream", ">");

        // history read: 1-0 body is gone (nil), 2-0 still present
        ks.run("XREADGROUP", "GROUP", "mygroup", "myc", "STREAMS", "mystream", "0-1")
            .expect.to.equal(streamReply("mystream",
                "*2\r\n" ~ tombstone("1-0") ~ entry("2-0", "field1", "B")));
    }

    @("valkey.streamcg.readgroup_id_not_gt")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        // stream ran dry: last_id 666 but no live entries
        ks.run("XGROUP", "CREATE", "mystream", "mygroup", "$", "MKSTREAM");
        ks.run("XADD", "mystream", "666", "key", "value");
        ks.run("XDEL", "mystream", "666");

        // any explicit (non-">") id returns the stream with an empty list (PEL empty)
        foreach (id; ["0", "600", "666", "700"])
            ks.run("XREADGROUP", "GROUP", "mygroup", "myc", "STREAMS", "mystream", id)
                .expect.to.equal("*1\r\n*2\r\n" ~ bulk("mystream") ~ "*0\r\n");

        // add a new entry; ">" delivers it into the PEL
        ks.run("XADD", "mystream", "667", "key", "value");
        ks.run("XREADGROUP", "GROUP", "mygroup", "myc", "STREAMS", "mystream", ">")
            .expect.to.equal(streamReply("mystream", "*1\r\n" ~ entry("667-0", "key", "value")));

        // now explicit id < 667 replays the pending entry
        foreach (id; ["0", "600", "666"])
            ks.run("XREADGROUP", "GROUP", "mygroup", "myc", "STREAMS", "mystream", id)
                .expect.to.equal(streamReply("mystream", "*1\r\n" ~ entry("667-0", "key", "value")));

        // explicit id >= 667 -> empty list
        foreach (id; ["667", "700"])
            ks.run("XREADGROUP", "GROUP", "mygroup", "myc", "STREAMS", "mystream", id)
                .expect.to.equal("*1\r\n*2\r\n" ~ bulk("mystream") ~ "*0\r\n");

        // after ACK, all explicit ids return empty list
        ks.run("XACK", "mystream", "mygroup", "667");
        foreach (id; ["0", "600", "666", "667", "700"])
            ks.run("XREADGROUP", "GROUP", "mygroup", "myc", "STREAMS", "mystream", id)
                .expect.to.equal("*1\r\n*2\r\n" ~ bulk("mystream") ~ "*0\r\n");
    }

    @("valkey.streamcg.readgroup_unbalanced")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        ks.run("XADD", "mystream", "666", "f", "v");
        ks.run("XGROUP", "CREATE", "mystream", "mygroup", "$");

        ks.run("XREADGROUP", "GROUP", "mygroup", "Alice", "COUNT", "1", "STREAMS", "mystream")
            .startsWith(
                "-ERR Unbalanced 'xreadgroup' list of streams").should.equal(true);
        ks.run("XREAD", "COUNT", "1", "STREAMS", "mystream")
            .startsWith("-ERR Unbalanced 'xread' list of streams").should.equal(true);
    }

    @("valkey.streamcg.xclaim")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        ks.run("XADD", "mystream", "1-0", "a", "1");
        ks.run("XADD", "mystream", "2-0", "b", "2");
        ks.run("XADD", "mystream", "3-0", "c", "3");
        ks.run("XGROUP", "CREATE", "mystream", "mygroup", "0");

        // consumer1 reads item 1
        ks.run("XREADGROUP", "GROUP", "mygroup", "consumer1", "COUNT", "1", "STREAMS",
            "mystream", ">");

        // it's pending under consumer1 only
        ks.run("XPENDING", "mystream", "mygroup", "-", "+", "10", "consumer1")
            .startsWith("*1\r\n").should.equal(true);
        ks.run("XPENDING", "mystream", "mygroup", "-", "+", "10", "consumer2")
            .expect.to.equal("*0\r\n");

        // consumer2 claims item 1 (min-idle-time 0 so it always qualifies)
        ks.run("XCLAIM", "mystream", "mygroup", "consumer2", "0", "1-0")
            .expect.to.equal("*1\r\n" ~ entry("1-0", "a", "1"));

        // ownership moved to consumer2
        ks.run("XPENDING", "mystream", "mygroup", "-", "+", "10", "consumer1")
            .expect.to.equal("*0\r\n");
        ks.run("XPENDING", "mystream", "mygroup", "-", "+", "10", "consumer2")
            .startsWith("*1\r\n").should.equal(true);

        // JUSTID returns just the id list
        ks.run("XCLAIM", "mystream", "mygroup", "consumer3", "0", "1-0", "JUSTID")
            .expect.to.equal("*1\r\n" ~ bulk("1-0"));

        // claiming a deleted entry is a NOP -> empty reply
        ks.run("XADD", "mystream", "4-0", "d", "4");
        ks.run("XREADGROUP", "GROUP", "mygroup", "consumer1", "COUNT", "1", "STREAMS",
            "mystream", ">");
        ks.run("XDEL", "mystream", "4-0");
        ks.run("XCLAIM", "mystream", "mygroup", "consumer2", "0", "4-0").expect.to.equal("*0\r\n");
    }

    @("valkey.streamcg.xclaim_same_consumer")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        ks.run("XADD", "mystream", "1-0", "a", "1");
        ks.run("XGROUP", "CREATE", "mystream", "mygroup", "0");
        ks.run("XREADGROUP", "GROUP", "mygroup", "consumer1", "COUNT", "1", "STREAMS",
            "mystream", ">");

        // re-claim by the same consumer that already owns it still returns the entry
        ks.run("XCLAIM", "mystream", "mygroup", "consumer1", "0", "1-0")
            .expect.to.equal("*1\r\n" ~ entry("1-0", "a", "1"));

        // still exactly one pending row, still owned by consumer1
        auto p = ks.run("XPENDING", "mystream", "mygroup", "-", "+", "10");
        p.startsWith("*1\r\n").should.equal(true);
        p.canFind(bulk("consumer1")).should.equal(true);
    }

    @("valkey.streamcg.xclaim_with_xdel")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        ks.run("XADD", "x", "1-0", "f", "v");
        ks.run("XADD", "x", "2-0", "f", "v");
        ks.run("XADD", "x", "3-0", "f", "v");
        ks.run("XGROUP", "CREATE", "x", "grp", "0");
        ks.run("XREADGROUP", "GROUP", "grp", "Alice", "STREAMS", "x", ">")
            .expect.to.equal(streamReply("x", "*3\r\n"
                ~ entry("1-0", "f", "v") ~ entry("2-0", "f", "v") ~ entry("3-0", "f", "v")));
        ks.run("XDEL", "x", "2-0");

        // claiming 1-0 2-0 3-0: 2-0 was deleted so it's dropped from the reply (and PEL)
        ks.run("XCLAIM", "x", "grp", "Bob", "0", "1-0", "2-0", "3-0")
            .expect.to.equal("*2\r\n" ~ entry("1-0", "f", "v") ~ entry("3-0", "f", "v"));

        // Alice no longer owns any of them
        ks.run("XPENDING", "x", "grp", "-", "+", "10", "Alice").expect.to.equal("*0\r\n");
    }

    @("valkey.streamcg.xautoclaim")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        ks.run("XADD", "mystream", "1-0", "a", "1");
        ks.run("XADD", "mystream", "2-0", "b", "2");
        ks.run("XADD", "mystream", "3-0", "c", "3");
        ks.run("XGROUP", "CREATE", "mystream", "mygroup", "0");
        ks.run("XREADGROUP", "GROUP", "mygroup", "consumer1", "STREAMS", "mystream", ">");

        // XAUTOCLAIM COUNT 2: [cursor, [claimed entries], [deleted ids]]
        // cursor points at the next unscanned id (3-0), deleted list empty
        ks.run("XAUTOCLAIM", "mystream", "mygroup", "consumer2", "0", "-", "COUNT", "2")
            .expect.to.equal("*3\r\n" ~ bulk("3-0")
                ~ "*2\r\n" ~ entry("1-0", "a", "1") ~ entry("2-0", "b", "2")
                ~ "*0\r\n");

        // JUSTID form returns the id list; cursor 0-0 at end of scan
        ks.run("XAUTOCLAIM", "mystream", "mygroup", "consumer3", "0", "-", "JUSTID")
            .expect.to.equal("*3\r\n" ~ bulk("0-0")
                ~ "*3\r\n" ~ bulk("1-0") ~ bulk("2-0") ~ bulk("3-0")
                ~ "*0\r\n");
    }

    @("valkey.streamcg.xautoclaim_with_xdel")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        ks.run("XADD", "x", "1-0", "f", "v");
        ks.run("XADD", "x", "2-0", "f", "v");
        ks.run("XADD", "x", "3-0", "f", "v");
        ks.run("XGROUP", "CREATE", "x", "grp", "0");
        ks.run("XREADGROUP", "GROUP", "grp", "Alice", "STREAMS", "x", ">");
        ks.run("XDEL", "x", "2-0");

        // deleted 2-0 is reported in the 3rd (deleted-ids) element
        ks.run("XAUTOCLAIM", "x", "grp", "Bob", "0", "0-0")
            .expect.to.equal("*3\r\n" ~ bulk("0-0")
                ~ "*2\r\n" ~ entry("1-0", "f", "v") ~ entry("3-0", "f", "v")
                ~ "*1\r\n" ~ bulk("2-0"));
        ks.run("XPENDING", "x", "grp", "-", "+", "10", "Alice").expect.to.equal("*0\r\n");
    }

    @("valkey.streamcg.xautoclaim_xdel_count")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        ks.run("XADD", "x", "1-0", "f", "v");
        ks.run("XADD", "x", "2-0", "f", "v");
        ks.run("XADD", "x", "3-0", "f", "v");
        ks.run("XGROUP", "CREATE", "x", "grp", "0");
        ks.run("XREADGROUP", "GROUP", "grp", "Alice", "STREAMS", "x", ">");
        ks.run("XDEL", "x", "1-0");
        ks.run("XDEL", "x", "2-0");

        // iterate with COUNT 1; deleted entries advance the cursor and land in list 3
        ks.run("XAUTOCLAIM", "x", "grp", "Bob", "0", "0-0", "COUNT", "1")
            .expect.to.equal("*3\r\n" ~ bulk("2-0") ~ "*0\r\n" ~ "*1\r\n" ~ bulk("1-0"));
        ks.run("XAUTOCLAIM", "x", "grp", "Bob", "0", "2-0", "COUNT", "1")
            .expect.to.equal("*3\r\n" ~ bulk("3-0") ~ "*0\r\n" ~ "*1\r\n" ~ bulk("2-0"));
        ks.run("XAUTOCLAIM", "x", "grp", "Bob", "0", "3-0", "COUNT", "1")
            .expect.to.equal("*3\r\n" ~ bulk("0-0")
                ~ "*1\r\n" ~ entry("3-0", "f", "v") ~ "*0\r\n");
        ks.run("XPENDING", "x", "grp", "-", "+", "10", "Alice").expect.to.equal("*0\r\n");
    }

    @("valkey.streamcg.xautoclaim_count_errors")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        // COUNT must be > 0
        ks.run("XAUTOCLAIM", "key", "group", "consumer", "1", "1", "COUNT", "0")
            .startsWith("-ERR COUNT must be > 0").should.equal(true);
        // out-of-range COUNT
        ks.run("XAUTOCLAIM", "x", "grp", "Bob", "0", "3-0", "COUNT", "8070450532247928833")
            .startsWith("-ERR COUNT").should.equal(true);
    }

    @("valkey.streamcg.createconsumer")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        ks.run("XGROUP", "CREATE", "mystream", "mygroup", "$", "MKSTREAM");
        ks.run("XADD", "mystream", "1-0", "f", "v");

        // no consumers yet
        ks.run("XINFO", "GROUPS", "mystream").canFind(bulk("consumers") ~ ":0\r\n")
            .should.equal(true);

        // XREADGROUP implicitly creates the consumer
        ks.run("XREADGROUP", "GROUP", "mygroup", "Alice", "COUNT", "1", "STREAMS", "mystream", ">");
        ks.run("XINFO", "CONSUMERS", "mystream", "mygroup").canFind(bulk("Alice"))
            .should.equal(true);

        // CREATECONSUMER: 0 when it already exists, 1 when newly created
        ks.run("XGROUP", "CREATECONSUMER", "mystream", "mygroup", "Alice").expect.to.equal(":0\r\n");
        ks.run("XGROUP", "CREATECONSUMER", "mystream", "mygroup", "Bob").expect.to.equal(":1\r\n");

        auto cons = ks.run("XINFO", "CONSUMERS", "mystream", "mygroup");
        cons.canFind(bulk("Alice")).should.equal(true);
        cons.canFind(bulk("Bob")).should.equal(true);
    }

    @("valkey.streamcg.createconsumer_nogroup")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        ks.run("XADD", "mystream", "1-0", "f", "v");
        ks.run("XGROUP", "CREATECONSUMER", "mystream", "mygroup", "consumer")
            .startsWith("-NOGROUP").should.equal(true);
    }

    @("valkey.streamcg.delconsumer_destroy")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        ks.run("XGROUP", "CREATE", "s", "g", "0", "MKSTREAM");
        ks.run("XADD", "s", "1-0", "a", "1");
        ks.run("XREADGROUP", "GROUP", "g", "c1", "STREAMS", "s", ">");

        // DELCONSUMER returns the number of pending entries the consumer had
        ks.run("XGROUP", "DELCONSUMER", "s", "g", "c1").expect.to.equal(":1\r\n");
        // deleting a non-existent consumer -> 0
        ks.run("XGROUP", "DELCONSUMER", "s", "g", "nope").expect.to.equal(":0\r\n");

        // DESTROY returns 1 for an existing group, 0 otherwise
        ks.run("XGROUP", "DESTROY", "s", "g").expect.to.equal(":1\r\n");
        ks.run("XGROUP", "DESTROY", "s", "g").expect.to.equal(":0\r\n");
    }

    @("valkey.streamcg.readgroup_noack")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        ks.run("XGROUP", "CREATE", "s", "g", "$", "MKSTREAM");
        ks.run("XADD", "s", "1-0", "f", "v");

        // NOACK delivers but does NOT put the entry in the PEL
        ks.run("XREADGROUP", "GROUP", "g", "Alice", "NOACK", "STREAMS", "s", ">")
            .expect.to.equal(streamReply("s", "*1\r\n" ~ entry("1-0", "f", "v")));
        ks.run("XPENDING", "s", "g").expect.to.equal("*4\r\n:0\r\n" ~ NIL ~ NIL ~ NILARR);

        // the consumer is still created
        ks.run("XINFO", "CONSUMERS", "s", "g").canFind(bulk("Alice")).should.equal(true);
    }

    @("valkey.streamcg.xinfo_groups_entriesread_lag")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        // helper: the full 12-field XINFO GROUPS reply for a single-group stream
        static string groups1(string name, string consumers, string pending,
            string lastId, string er, string lag)
        {
            return "*1\r\n*12\r\n"
                ~ bulk("name") ~ bulk(name)
                ~ bulk("consumers") ~ consumers
                ~ bulk("pending") ~ pending
                ~ bulk("last-delivered-id") ~ bulk(lastId)
                ~ bulk("entries-read") ~ er
                ~ bulk("lag") ~ lag;
        }

        // empty stream: entries-read nil, lag 0, last-delivered-id 0-0
        ks.run("XGROUP", "CREATE", "x", "g1", "0", "MKSTREAM");
        ks.run("XINFO", "GROUPS", "x").expect.to.equal(
            groups1("g1", ":0\r\n", ":0\r\n", "0-0", NIL, ":0\r\n"));

        // populate 5; g1 was created on the (then) empty stream, so it is caught
        // up to 0-0 with entries-read nil and lag = 5 (all five are unread)
        ks.run("XADD", "x", "1-0", "data", "a");
        ks.run("XADD", "x", "2-0", "data", "b");
        ks.run("XADD", "x", "3-0", "data", "c");
        ks.run("XADD", "x", "4-0", "data", "d");
        ks.run("XADD", "x", "5-0", "data", "e");
        ks.run("XINFO", "GROUPS", "x").expect.to.equal(
            groups1("g1", ":0\r\n", ":0\r\n", "0-0", NIL, ":5\r\n"));

        // read 1 -> entries-read 1, lag 4, last-delivered-id 1-0, one consumer/pending
        ks.run("XREADGROUP", "GROUP", "g1", "c1", "COUNT", "1", "STREAMS", "x", ">");
        ks.run("XINFO", "GROUPS", "x").expect.to.equal(
            groups1("g1", ":1\r\n", ":1\r\n", "1-0", ":1\r\n", ":4\r\n"));

        // read the rest -> entries-read 5, lag 0
        ks.run("XREADGROUP", "GROUP", "g1", "c2", "COUNT", "10", "STREAMS", "x", ">");
        ks.run("XINFO", "GROUPS", "x").expect.to.equal(
            groups1("g1", ":2\r\n", ":5\r\n", "5-0", ":5\r\n", ":0\r\n"));

        // add one more -> entries-read stays 5, lag 1
        ks.run("XADD", "x", "6-0", "data", "f");
        ks.run("XINFO", "GROUPS", "x").expect.to.equal(
            groups1("g1", ":2\r\n", ":5\r\n", "5-0", ":5\r\n", ":1\r\n"));
    }

    @("valkey.streamcg.xinfo_groups_tombstone_lag")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        // a tombstone after the group's last_id makes the lag incalculable (nil)
        foreach (i; 1 .. 11)
            ks.run("XADD", "x", i.to!string ~ "-0", "data", "x");
        ks.run("XGROUP", "CREATE", "x", "g1", "0");
        // read the first 8 (COUNT 8), then delete an unread entry (9-0)
        ks.run("XREADGROUP", "GROUP", "g1", "c1", "COUNT", "8", "STREAMS", "x", ">");
        ks.run("XDEL", "x", "9-0");

        // entries-read 8, but lag is nil because of the tombstone past last_id
        auto r = ks.run("XINFO", "GROUPS", "x");
        r.canFind(bulk("entries-read") ~ ":8\r\n").should.equal(true);
        r.canFind(bulk("lag") ~ NIL).should.equal(true);
    }

    @("valkey.streamcg.wrongtype")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        ks.run("SET", "str", "v");
        ks.run("XGROUP", "CREATE", "str", "g", "$").startsWith("-WRONGTYPE").should.equal(true);
        ks.run("XREADGROUP", "GROUP", "g", "c", "STREAMS", "str", ">")
            .startsWith("-WRONGTYPE").should.equal(true);
        ks.run("XACK", "str", "g", "1-0").startsWith("-WRONGTYPE").should.equal(true);
        ks.run("XPENDING", "str", "g").startsWith("-WRONGTYPE").should.equal(true);
        ks.run("XCLAIM", "str", "g", "c", "0", "1-0").startsWith("-WRONGTYPE").should.equal(true);
    }

    @("valkey.streamcg.nogroup_errors")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        ks.run("XADD", "s", "1-0", "f", "v");
        // reading a group that doesn't exist -> NOGROUP
        ks.run("XREADGROUP", "GROUP", "nope", "c", "STREAMS", "s", ">")
            .startsWith("-NOGROUP").should.equal(true);
        ks.run("XACK", "s", "nope", "1-0").expect.to.equal(":0\r\n"); // ack on missing group is 0
        ks.run("XPENDING", "s", "nope").startsWith("-NOGROUP").should.equal(true);
    }
}
