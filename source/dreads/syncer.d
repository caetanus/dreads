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

import core.sync.condition : Condition;
import core.sync.mutex : Mutex;
import core.thread : Thread;

version (Posix)
    import core.sys.posix.unistd : fdatasync;

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
    private int fd;
    private SyncState st;
    private Mutex mtx;
    private Condition cond; // wakes the fsync thread
    private Thread thr;
    private shared bool running;
    private shared(ManualEvent) fiberEvent; // wakes waiting fibers, cross-thread

    this(int fd)
    {
        this.fd = fd;
        mtx = new Mutex;
        cond = new Condition(mtx);
        fiberEvent = createSharedManualEvent();
        running = true;
        thr = new Thread(&loop);
        thr.isDaemon = true;
        thr.start();
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
            version (Posix)
                fdatasync(fd);
            mtx.lock_nothrow();
            st.complete();
            mtx.unlock_nothrow();
            fiberEvent.emit(); // wake fibers whose seq is now durable
        }
    }
}
