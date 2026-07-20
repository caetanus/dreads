module tests.raftlog_tests;

// Durable Raft storage: everything must survive close/reopen, torn tails
// must be dropped, truncation must be honored by recovery.

version (unittest)
{
    import core.stdc.stdio : fclose, fopen, fwrite;
    import core.stdc.stdio : remove;

    import fluent.asserts;

    import dreads.raftlog : RaftLog;
    import raft.types : LogEntry;

    private const(ubyte)[] pay(string s) nothrow
    {
        return cast(const(ubyte)[]) s;
    }

    private void rm(string base)
    {
        remove((base ~ ".raftlog\0").ptr);
        remove((base ~ ".raftlog.old\0").ptr);
        remove((base ~ ".raftmeta\0").ptr);
        remove((base ~ ".raftsnap\0").ptr);
    }

    @("raftlog.roundtrip_and_reopen")
    unittest
    {
        enum base = "/tmp/dreads_rl_rt";
        rm(base);
        scope (exit)
            rm(base);

        auto log = RaftLog.open(base);
        (log !is null).expect.to.equal(true);
        log.setCurrentTerm(7);
        log.setVotedFor(3);
        LogEntry[3] batch = [
            LogEntry(1, 1, pay("one")), LogEntry(1, 2, pay("two")),
            LogEntry(2, 3, pay("three")),
        ];
        log.append(batch[]);
        log.lastIndex.expect.to.equal(3);
        log.termAt(2).expect.to.equal(1);
        log.termAt(3).expect.to.equal(2);
        auto es = log.entriesFrom(2, 10);
        es.length.expect.to.equal(2);
        (cast(string) es[0].payload).expect.to.equal("two");
        log.close();

        auto back = RaftLog.open(base);
        back.currentTerm.expect.to.equal(7);
        back.votedFor.expect.to.equal(3);
        back.lastIndex.expect.to.equal(3);
        (cast(string) back.entriesFrom(3, 1)[0].payload).expect.to.equal("three");
        back.termAt(99).expect.to.equal(0);
        back.entriesFrom(4, 1).length.expect.to.equal(0);
        back.close();
    }

    // Regression (the "restart deadlock" the chaos test would have caught): after
    // recovery, async durability MUST treat the reopened log's entries as already
    // durable. Otherwise the first awaitDurable(recoveredLastIndex) waits for a
    // sync that never happens — deadlocking the raft loop (no tick/election/leader)
    // and leaving the recovered data un-applied. The baseline must == lastIndex.
    @("raftlog.recovery_seeds_durable_baseline")
    unittest
    {
        enum base = "/tmp/dreads_rl_durbase";
        rm(base);
        scope (exit)
            rm(base);

        auto log = RaftLog.open(base);
        LogEntry[3] batch = [
            LogEntry(1, 1, pay("a")), LogEntry(1, 2, pay("b")), LogEntry(2, 3, pay("c")),
        ];
        log.append(batch[]);
        log.close();

        // Reopen (recovery) and turn on async durability, exactly as the server
        // does at boot (gConfig.synchronous defaults to "full").
        auto back = RaftLog.open(base);
        back.lastIndex.expect.to.equal(3);
        back.enableAsyncDurability();
        // The whole recovered prefix is durable from the first tick — so
        // awaitDurable(3) returns immediately instead of blocking forever.
        back.testDurableBaseline().expect.to.equal(3UL);
        back.close();
    }

    @("raftlog.truncate_survives_reopen")
    unittest
    {
        enum base = "/tmp/dreads_rl_tr";
        rm(base);
        scope (exit)
            rm(base);

        auto log = RaftLog.open(base);
        foreach (i; 1 .. 6)
        {
            LogEntry[1] e = [LogEntry(1, i, pay("entry"))];
            log.append(e[]);
        }
        log.truncateFrom(3); // conflict resolution: drop 3..5
        log.lastIndex.expect.to.equal(2);
        // append diverging entries after the cut
        LogEntry[1] e2 = [LogEntry(2, 3, pay("newer"))];
        log.append(e2[]);
        log.close();

        auto back = RaftLog.open(base);
        back.lastIndex.expect.to.equal(3);
        back.termAt(3).expect.to.equal(2);
        (cast(string) back.entriesFrom(3, 1)[0].payload).expect.to.equal("newer");
        back.close();
    }

    @("raftlog.compaction_rotation_survives_reopen")
    unittest
    {
        // Non-blocking compaction rotates the log (seal + fresh file, no
        // rewrite-in-place, no fsync). Snapshot + surviving entries + entries
        // appended after the rotation must all survive a clean reopen.
        enum base = "/tmp/dreads_rl_comp";
        rm(base);
        scope (exit)
            rm(base);

        auto log = RaftLog.open(base);
        foreach (i; 1 .. 11)
        {
            LogEntry[1] e = [LogEntry(1, i, pay("v"))];
            log.append(e[]);
        }
        log.lastIndex.expect.to.equal(10);
        log.saveSnapshot(6, 1, pay("STATE@6")); // covers 1..6, keeps 7..10
        log.snapshotIndex.expect.to.equal(6);
        log.lastIndex.expect.to.equal(10);
        (cast(string) log.entriesFrom(7, 10)[0].payload).expect.to.equal("v");
        LogEntry[1] e2 = [LogEntry(2, 11, pay("post"))]; // append after rotation
        log.append(e2[]);
        log.close();

        auto back = RaftLog.open(base);
        back.snapshotIndex.expect.to.equal(6);
        back.lastIndex.expect.to.equal(11);
        (cast(string) back.snapshotData).expect.to.equal("STATE@6");
        back.entriesFrom(6, 1).length.expect.to.equal(0); // covered by snapshot
        (cast(string) back.entriesFrom(7, 1)[0].payload).expect.to.equal("v");
        (cast(string) back.entriesFrom(11, 1)[0].payload).expect.to.equal("post");
        back.termAt(11).expect.to.equal(2);
        back.close();
    }

    @("raftlog.compaction_recovers_from_backup_on_crash")
    unittest
    {
        // Simulate a crash right after a rotation: the .raftlog.old backup is
        // still present (no clean close). Recovery must replay it + the active
        // log and reconstruct the exact surviving tail.
        enum base = "/tmp/dreads_rl_compcrash";
        rm(base);
        scope (exit)
            rm(base);

        auto log = RaftLog.open(base);
        foreach (i; 1 .. 9)
        {
            LogEntry[1] e = [LogEntry(1, i, pay("x"))];
            log.append(e[]);
        }
        log.saveSnapshot(5, 1, pay("STATE@5")); // rotation leaves .raftlog.old
        // deliberately DO NOT close(log): mimic a crash — the backup survives.

        auto back = RaftLog.open(base);
        back.snapshotIndex.expect.to.equal(5);
        back.lastIndex.expect.to.equal(8); // 6,7,8 recovered
        (cast(string) back.snapshotData).expect.to.equal("STATE@5");
        back.entriesFrom(5, 1).length.expect.to.equal(0);
        back.entriesFrom(6, 3).length.expect.to.equal(3);
        // writable again after recovery
        LogEntry[1] e = [LogEntry(2, 9, pay("resumed"))];
        back.append(e[]);
        back.lastIndex.expect.to.equal(9);
        back.close();

        auto again = RaftLog.open(base);
        again.lastIndex.expect.to.equal(9);
        (cast(string) again.entriesFrom(9, 1)[0].payload).expect.to.equal("resumed");
        again.close();
    }

    @("raftlog.truncate_after_compaction")
    unittest
    {
        // A conflict truncation after a compaction must operate on the fresh
        // active segment and survive reopen.
        enum base = "/tmp/dreads_rl_comptr";
        rm(base);
        scope (exit)
            rm(base);

        auto log = RaftLog.open(base);
        foreach (i; 1 .. 11)
        {
            LogEntry[1] e = [LogEntry(1, i, pay("a"))];
            log.append(e[]);
        }
        log.saveSnapshot(6, 1, pay("S")); // keeps 7..10 in a fresh segment
        LogEntry[1] e11 = [LogEntry(1, 11, pay("a"))];
        log.append(e11[]);
        log.truncateFrom(9); // drop 9,10,11 (all in the post-rotation segment)
        log.lastIndex.expect.to.equal(8);
        LogEntry[1] div = [LogEntry(3, 9, pay("branch"))];
        log.append(div[]);
        log.close();

        auto back = RaftLog.open(base);
        back.lastIndex.expect.to.equal(9);
        back.termAt(9).expect.to.equal(3);
        (cast(string) back.entriesFrom(9, 1)[0].payload).expect.to.equal("branch");
        back.snapshotIndex.expect.to.equal(6);
        back.close();
    }

    @("raftlog.torn_tail_is_dropped")
    unittest
    {
        enum base = "/tmp/dreads_rl_torn";
        rm(base);
        scope (exit)
            rm(base);

        auto log = RaftLog.open(base);
        LogEntry[2] batch = [LogEntry(1, 1, pay("good")), LogEntry(1, 2, pay("also"))];
        log.append(batch[]);
        log.close();

        // simulate a crash mid-write: garbage half-frame at the tail
        auto f = fopen((base ~ ".raftlog\0").ptr, "ab");
        ubyte[7] junk = [9, 9, 9, 9, 9, 9, 9];
        fwrite(junk.ptr, 1, junk.length, f);
        fclose(f);

        auto back = RaftLog.open(base);
        back.lastIndex.expect.to.equal(2); // junk truncated away
        (cast(string) back.entriesFrom(2, 1)[0].payload).expect.to.equal("also");
        // and the log is writable again at the right offset
        LogEntry[1] e = [LogEntry(2, 3, pay("post"))];
        back.append(e[]);
        back.close();
        auto again = RaftLog.open(base);
        again.lastIndex.expect.to.equal(3);
        again.close();
    }
}
