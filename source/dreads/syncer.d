module dreads.syncer;

// Asynchronous group-commit durability. A synchronous fdatasync on the
// single-threaded event loop would freeze every connection, not just the
// one waiting — so the fsync runs on a dedicated OS thread. Writers (fibers)
// append + fflush on the loop thread, then yield until their sequence is
// durable; the fsync thread coalesces all pending requests behind one
// fdatasync (Postgres-style group commit) and wakes the fibers via a shared
// vibe event.
//
// Raft stays correct: a follower never acks (replies success) before the
// entries are durable — awaitDurable() gates the reply. The win is that the
// wait does not block the loop, and one fsync amortizes many appends.

import core.atomic : atomicLoad, atomicStore;
import core.stdc.errno : errno;
import core.stdc.stdio : stderr, fprintf;
import core.stdc.stdlib : abort;
import core.sync.condition : Condition;
import core.sync.mutex : Mutex;
import core.thread : Thread;

version (CRuntime_Musl)
    extern (C) int fdatasync(int) @nogc nothrow; // druntime omits it for musl
else version (Posix)
    import core.sys.posix.unistd : fdatasync;

version (linux)
    import during;

import vibe.core.sync : createSharedManualEvent, ManualEvent;

/// Pure group-commit bookkeeping — no threads, no I/O, fully testable.
/// Sequences are monotonic (the raft log index works directly).
struct SyncState
{
    private ulong requested_; // highest seq a writer needs durable
    private ulong durable_; // highest seq fdatasync'd
    private ulong syncing_; // seq the in-flight fsync will cover (0 = idle)

    void request(ulong seq) @safe nothrow @nogc
    {
        if (seq > requested_)
            requested_ = seq;
    }

    /// Seed the baseline at startup: everything up to `seq` is ALREADY durable
    /// (it was recovered by reading the on-disk log), so awaitDurable(<= seq)
    /// returns immediately and no spurious re-sync of recovered entries is
    /// requested. Without this, the first awaitDurable(recoveredLastIndex) after
    /// a restart blocks forever (durable_ starts at 0 and those entries are never
    /// re-synced), deadlocking the raft loop — no tick, no election, no leader.
    void seed(ulong seq) @safe nothrow @nogc
    {
        if (seq > durable_)
            durable_ = seq;
        if (seq > requested_)
            requested_ = seq;
    }

    /// Claims the next batch to sync, or 0 when idle-with-nothing or busy.
    /// Because it always claims up to the latest request, requests that
    /// arrived during the previous fsync are covered by the next single one.
    ulong claim() @safe nothrow @nogc
    {
        if (syncing_ != 0 || requested_ <= durable_)
            return 0;
        syncing_ = requested_;
        return syncing_;
    }

    void complete() @safe nothrow @nogc
    {
        if (syncing_ > durable_)
            durable_ = syncing_;
        syncing_ = 0;
    }

    bool isDurable(ulong seq) const @safe nothrow @nogc
    {
        return durable_ >= seq;
    }

    /// Work waits beyond the current (or next) sync.
    bool pending() const @safe nothrow @nogc
    {
        return requested_ > durable_;
    }

    @property ulong durable() const @safe nothrow @nogc
    {
        return durable_;
    }
}

/// Threaded group-commit durability for one file descriptor.
final class Durability
{
    private shared int fd; // atomic: retarget() swaps it after a log rotation
    private SyncState st;
    private Mutex mtx;
    private Condition cond; // wakes the fsync thread
    private Thread thr;
    private shared bool running;
    private shared(ManualEvent) fiberEvent; // wakes waiting fibers, cross-thread
    // fsync backend: the default is a blocking fdatasync on this dedicated
    // thread (the "threadpool" fallback). Optionally io_uring submits the
    // fdatasync to a per-thread ring — same group-commit shape, but the syscall
    // path goes through the ring (Linux only; falls back if setup fails).
    private bool useIoUring;
    version (linux) private Uring ring;
    private bool ringReady;

    this(int fd, ulong durableBaseline, bool ioUring = false)
    {
        atomicStore(this.fd, fd);
        // Recovered entries (already on disk) are durable from the first tick.
        st.seed(durableBaseline);
        this.useIoUring = ioUring;
        mtx = new Mutex;
        cond = new Condition(mtx);
        fiberEvent = createSharedManualEvent();
        running = true;
        thr = new Thread(&loop);
        thr.isDaemon = true;
        thr.start();
    }

    // One fdatasync of `fd`, via io_uring when enabled+available, else the
    // blocking syscall. Runs only on the fsync thread.
    private void doFsync() nothrow
    {
        auto d = atomicLoad(fd); // may have been retargeted after a log rotation
        version (linux)
        {
            if (ringReady)
            {
                try
                {
                    auto sqe = &ring.next();
                    *sqe = SubmissionEntry.init;
                    sqe.opcode = Operation.FSYNC;
                    sqe.fd = d;
                    sqe.fsync_flags = FsyncFlags.DATASYNC; // fdatasync semantics
                    ring.submit(1); // submit + wait for the completion
                    // A negative completion result is a real fsync error (not a
                    // ring problem) — the data the fibers are about to be told is
                    // durable is NOT on disk. Never swallow it: fail-stop.
                    if (!ring.empty)
                    {
                        immutable res = ring.front.res;
                        ring.popFront();
                        if (res < 0)
                            syncFailedOrDie(-res);
                    }
                    return;
                }
                catch (Exception)
                {
                    ringReady = false; // fall back permanently on error
                }
            }
        }
        version (Posix)
        {
            if (fdatasync(d) != 0)
                syncFailedOrDie(errno);
        }
    }

    // Same rationale as raftlog.fsyncOrDie: acknowledging a write as durable when
    // the fsync failed silently loses committed data (fsyncgate — the kernel may
    // clear the writeback error on the next call). The only safe response is to
    // crash before any fiber's awaitDurable() returns success.
    private static void syncFailedOrDie(int err) nothrow @nogc
    {
        cast(void) fprintf(stderr,
            "dreads: FATAL: fdatasync failed (errno=%d) — refusing to acknowledge a non-durable raft write\n",
            err);
        abort();
    }

    /// Point the fsync thread at a new fd after the log file was rotated
    /// (compaction opens a fresh log file). The old fd is fsync'd harmlessly by
    /// any in-flight call; the caller keeps it open until the next rotation so
    /// this never races a close.
    void retarget(int newFd) nothrow
    {
        atomicStore(fd, newFd);
    }

    private void notify() nothrow
    {
        try
            cond.notify();
        catch (Exception)
        {
        }
    }

    /// Event-loop thread: a writer needs everything up to `seq` durable.
    /// Cheap — just records the request and nudges the fsync thread.
    void requestSync(ulong seq) nothrow
    {
        mtx.lock_nothrow();
        st.request(seq);
        notify();
        mtx.unlock_nothrow();
    }

    /// Fiber: yield until `seq` is durable (does not block the event loop).
    void awaitDurable(ulong seq) nothrow
    {
        for (;;)
        {
            auto ec = fiberEvent.emitCount;
            mtx.lock_nothrow();
            auto done = st.isDurable(seq);
            mtx.unlock_nothrow();
            if (done)
                return;
            fiberEvent.waitUninterruptible(ec);
        }
    }

    void stop() nothrow
    {
        mtx.lock_nothrow();
        running = false;
        notify();
        mtx.unlock_nothrow();
        try
            thr.join();
        catch (Exception)
        {
        }
    }

    /// Highest seq currently considered durable (seeded baseline + synced work).
    /// Package-visible so RaftLog can assert the recovery baseline in tests.
    package ulong durableSeq() nothrow
    {
        mtx.lock_nothrow();
        auto d = st.durable;
        mtx.unlock_nothrow();
        return d;
    }

    private void loop() nothrow
    {
        version (linux)
            if (useIoUring)
            {
                try
                    ringReady = ring.setup(8) == 0;
                catch (Exception)
                    ringReady = false;
            }
        for (;;)
        {
            mtx.lock_nothrow();
            ulong seq;
            while (running && (seq = st.claim()) == 0)
            {
                try
                    cond.wait();
                catch (Exception)
                {
                }
            }
            auto stop_ = !running;
            mtx.unlock_nothrow();
            if (stop_)
                return;
            doFsync();
            mtx.lock_nothrow();
            st.complete();
            mtx.unlock_nothrow();
            fiberEvent.emit(); // wake fibers whose seq is now durable
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

version (unittest)
{
    import fluent.asserts;

    // Regression (the "restart deadlock"): async durability was added after the
    // raft loop already worked, and it introduced a starvation that only fires
    // deterministically on the log-replay path. On restart, the recovered log
    // entries were durable BEFORE the crash but are never re-synced this session,
    // so with a fresh baseline of 0 the FIRST awaitDurable(recoveredLastIndex)
    // waits for a sync that never comes — forever — holding the node lock and
    // wedging the whole cluster (no tick -> no election -> no leader -> data
    // never re-applied). seed() fixes it: recovered entries are durable at once.
    @("syncer.seed_marks_recovered_durable")
    unittest
    {
        SyncState st;
        // fresh: nothing is durable yet (this is what deadlocked on restart)
        st.isDurable(1).expect.to.equal(false);

        // seed the recovered baseline (e.g. the reopened log's lastIndex)
        st.seed(100);
        st.durable.expect.to.equal(100UL);
        st.isDurable(1).expect.to.equal(true); // <= baseline -> awaitDurable returns
        st.isDurable(100).expect.to.equal(true);
        st.isDurable(101).expect.to.equal(false); // beyond baseline still waits

        // and NO spurious re-sync of the already-durable recovered prefix
        st.claim().expect.to.equal(0UL);

        // a real new append past the baseline still syncs normally
        st.request(150);
        st.claim().expect.to.equal(150UL);
        st.complete();
        st.isDurable(150).expect.to.equal(true);
    }

    // seed never moves the baseline backwards (idempotent / monotonic).
    @("syncer.seed_is_monotonic")
    unittest
    {
        SyncState st;
        st.seed(50);
        st.seed(10); // lower — ignored
        st.durable.expect.to.equal(50UL);
        st.seed(80); // higher — advances
        st.durable.expect.to.equal(80UL);
    }
}
