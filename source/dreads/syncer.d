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
import core.sync.condition : Condition;
import core.sync.mutex : Mutex;
import core.thread : Thread;

version (Posix)
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

    this(int fd, bool ioUring = false)
    {
        atomicStore(this.fd, fd);
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
                    if (!ring.empty)
                        ring.popFront();
                    return;
                }
                catch (Exception)
                {
                    ringReady = false; // fall back permanently on error
                }
            }
        }
        version (Posix)
            fdatasync(d);
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
