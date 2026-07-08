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
        remove((base ~ ".raftmeta\0").ptr);
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
