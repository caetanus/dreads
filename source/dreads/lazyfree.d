module dreads.lazyfree;

// Off-loop free of large values (lazyfree-lazy-server-del: UNLINK, and DEL /
// overwrite of a huge collection). Freeing a million-element container means
// chasing a scattered pointer structure and running a destructor per element —
// cache-miss-bound work that would stall the single event loop for milliseconds.
//
// Split of labour (the model the USER chose: "thread não toca allocator, re-injeta
// no loop"):
//   * the FREE-THREAD does the scattered TRAVERSAL — the expensive, allocation-free
//     part — gathering the value's backing blocks into a dense batch. Its own
//     scratch (the batch array) is Mallocator-backed (jemalloc, thread-safe) so it
//     never touches the KeyspaceAllocator (a single-thread composed allocator whose
//     freelist/StatsCollector would race).
//   * the EVENT LOOP deallocates the gathered keyspace blocks via KeyspaceAllocator
//     in a tick — over a DENSE array, no pointer-chase — and frees the batch scratch.
//
// So the cache-miss traversal leaves the loop; only the cheap freelist-push per
// block (which must stay single-writer) remains on it. Two SPSC rings connect the
// two threads:
//   loop  --submit-->  free-thread   (values to tear down; job = ctx + gather fn)
//   free-thread --reclaim-->  loop   (dense block batches to deallocate)
//
// Both are strictly single-producer/single-consumer (the loop is one logical
// producer — all fibers are cooperatively scheduled on it — and the free-thread is
// the sole consumer; mirror for reclaim), so we REUSE the lock-free Lamport ring
// already proven for the raft hand-off, dreads.raftq.RingCore (atomic head/tail,
// acquire/release, no mutex on the hot path). Each job is two pointers, so it rides
// the ring's `tag`/`meta` slots with an EMPTY byte payload — no second ring type.
// The free-thread's idle wait uses a Condition (it is a plain OS thread, not a vibe
// loop; the dreads.syncer pattern). Submit is NON-BLOCKING: a full ring means the
// caller frees inline (a stall is always better than dropping a free — a leak).

import core.atomic : atomicLoad, atomicStore;
import core.sync.condition : Condition;
import core.sync.mutex : Mutex;
import core.thread : Thread;
import core.stdc.stdlib : free, realloc;

import dreads.alloc : KeyspaceAllocator;
import dreads.raftq : RingCore;

/// One backing block to hand back to the loop for deallocation: a pointer and the
/// size the KeyspaceAllocator needs to route it to the right freelist bucket.
struct Block
{
    void* ptr;
    size_t size;
}

/// A gather sink handed to a value's tear-down routine ON THE FREE-THREAD. The
/// routine walks its structure and calls `add(ptr, size)` for every backing block
/// (running any non-allocating element finalisation as it goes), but frees NOTHING
/// — the blocks are deallocated later on the loop. The batch grows via Mallocator
/// (thread-safe scratch), never the KeyspaceAllocator.
struct BatchSink
{
    private Block* blocks;
    private size_t len;
    private size_t cap;

    /// Record a backing block for deferred deallocation. @nogc/nothrow: the
    /// tear-down path is hot and must not throw or touch the GC.
    void add(void* ptr, size_t size) @nogc nothrow @trusted
    {
        if (ptr is null)
            return;
        if (len == cap)
        {
            immutable ncap = cap == 0 ? 64 : cap * 2;
            auto p = cast(Block*) realloc(blocks, ncap * Block.sizeof);
            assert(p !is null, "lazyfree: batch grow OOM");
            blocks = p;
            cap = ncap;
        }
        blocks[len++] = Block(ptr, size);
    }

    // Free the scratch array (only when the batch was NOT handed to the reclaim
    // ring — on the empty-job and inline-free paths). A published batch's array is
    // owned by the ring entry and freed by the loop after deallocation.
    private void dropScratch() @nogc nothrow @trusted
    {
        if (blocks !is null)
            free(blocks);
        blocks = null;
        len = 0;
        cap = 0;
    }
}

/// The tear-down routine: given the job context, walk the value and push every
/// backing block into `sink`. Runs on the free-thread; must NOT deallocate and
/// must NOT touch the KeyspaceAllocator.
alias GatherFn = void function(void* ctx, ref BatchSink sink) @nogc nothrow;

/// The lazyfree worker: a dedicated OS thread draining a submit ring, plus a
/// reclaim ring the loop drains. One instance, owned by the server.
final class LazyFree
{
    private RingCore submit; // loop -> thread: tag = gather fn, meta = ctx
    private RingCore reclaim; // thread -> loop: tag = Block* array, meta = length
    private Mutex mtx;
    private Condition cond; // wakes the free-thread when a job arrives
    private Thread thr;
    private shared bool running;

    // stats (single-writer each; loop reads them, thread never does)
    private shared ulong submittedJobs;
    private shared ulong inlineJobs; // freed on the loop because the ring was full
    private shared ulong reclaimedBlocks;

    // partially-drained batch carried across ticks so a huge value's deallocate is
    // BOUNDED per drain call (no one-burst stall). Loop-thread-only state.
    private Block* curBlocks;
    private size_t curLen;
    private size_t curPos;

    this(size_t submitCap = 1024, size_t reclaimCap = 1024)
    {
        submit.setup(submitCap);
        reclaim.setup(reclaimCap);
        mtx = new Mutex;
        cond = new Condition(mtx);
        running = true;
        thr = new Thread(&loop);
        thr.isDaemon = true;
        thr.start();
    }

    /// Loop thread: hand a value off for off-loop tear-down. Non-blocking — if the
    /// submit ring is full the value is torn down INLINE (right here on the loop)
    /// so a free is never dropped. Returns true if it was queued off-loop.
    bool enqueue(GatherFn gather, void* ctx) @nogc nothrow
    {
        if (submit.push(null, cast(void*) gather, cast(ulong) cast(size_t) ctx))
        {
            atomicStore(submittedJobs, atomicLoad(submittedJobs) + 1);
            wake();
            return true;
        }
        // ring full: fall back to freeing inline on the loop (never leak).
        atomicStore(inlineJobs, atomicLoad(inlineJobs) + 1);
        freeInline(gather, ctx);
        return false;
    }

    /// Loop thread: deallocate up to `budget` gathered blocks (default: all). Call
    /// from the server tick with a bound so a giant value's deallocate is spread
    /// over ticks instead of stalling the loop in one burst; a partially-drained
    /// batch is carried to the next call. Returns the number of blocks freed.
    size_t drainReclaimed(size_t budget = size_t.max) nothrow @trusted
    {
        size_t n = 0;
        while (n < budget)
        {
            if (curBlocks is null)
            {
                const(ubyte)[] payload;
                void* tag;
                ulong meta;
                uint kind;
                if (!reclaim.front(payload, tag, meta, kind))
                    break; // nothing pending
                curBlocks = cast(Block*) tag;
                curLen = cast(size_t) meta;
                curPos = 0;
                reclaim.pop(); // the array pointer is ours now; free the ring slot
            }
            while (curPos < curLen && n < budget)
            {
                auto blk = curBlocks[curPos];
                KeyspaceAllocator.instance.deallocate((cast(ubyte*) blk.ptr)[0 .. blk.size]);
                curPos++;
                n++;
            }
            if (curPos >= curLen) // batch drained: free the Mallocator scratch array
            {
                if (curBlocks !is null)
                    free(curBlocks);
                curBlocks = null;
                curLen = curPos = 0;
            }
        }
        if (n)
            atomicStore(reclaimedBlocks, atomicLoad(reclaimedBlocks) + n);
        return n;
    }

    @property bool reclaimPending() nothrow
    {
        return curBlocks !is null || !reclaim.empty();
    }

    @property ulong statSubmitted() nothrow
    {
        return atomicLoad(submittedJobs);
    }

    @property ulong statInline() nothrow
    {
        return atomicLoad(inlineJobs);
    }

    @property ulong statReclaimedBlocks() nothrow
    {
        return atomicLoad(reclaimedBlocks);
    }

    /// Stop the thread and drain anything left (so nothing leaks on shutdown). The
    /// caller must ensure no more enqueue() calls race this.
    void stop() nothrow @trusted
    {
        mtx.lock_nothrow();
        atomicStore(running, false);
        try
            cond.notify();
        catch (Exception)
        {
        }
        mtx.unlock_nothrow();
        try
            thr.join();
        catch (Exception)
        {
        }
        drainReclaimed(); // free whatever the thread gathered before exiting
        submit.teardown();
        reclaim.teardown();
    }

    // Tear a value down entirely on the calling thread (the full-ring fallback and
    // the shutdown path). Gathers into a local batch then deallocates immediately.
    private void freeInline(GatherFn gather, void* ctx) @nogc nothrow @trusted
    {
        BatchSink sink;
        gather(ctx, sink);
        foreach (i; 0 .. sink.len)
            KeyspaceAllocator.instance.deallocate(
                (cast(ubyte*) sink.blocks[i].ptr)[0 .. sink.blocks[i].size]);
        sink.dropScratch();
    }

    private void wake() @nogc nothrow @trusted
    {
        mtx.lock_nothrow();
        // Condition.notify() is pthread_cond_signal underneath (no allocation) but
        // isn't annotated @nogc; call it through a @nogc-typed function pointer so
        // enqueue() stays @nogc (the command dispatch path is @nogc). Sound: single
        // caller, no GC work, exceptions swallowed.
        static void doNotify(Condition c) nothrow
        {
            try
                c.notify();
            catch (Exception)
            {
            }
        }

        (cast(void function(Condition) @nogc nothrow)&doNotify)(cond);
        mtx.unlock_nothrow();
    }

    // Consumer-side take (free-thread only): peek + decode + pop one submit job.
    private bool tryTake(out GatherFn gather, out void* ctx) nothrow @trusted
    {
        const(ubyte)[] payload;
        void* tag;
        ulong meta;
        uint kind;
        if (!submit.front(payload, tag, meta, kind))
            return false;
        gather = cast(GatherFn) tag;
        ctx = cast(void*) cast(size_t) meta;
        submit.pop();
        return true;
    }

    // Free-thread body: block until a job (or stop), gather its blocks, publish the
    // batch to the loop.
    private void loop() nothrow @trusted
    {
        for (;;)
        {
            mtx.lock_nothrow();
            GatherFn g = null;
            void* c = null;
            bool took = false;
            while (atomicLoad(running))
            {
                if (tryTake(g, c))
                {
                    took = true;
                    break;
                }
                try
                    cond.wait();
                catch (Exception)
                {
                }
            }
            immutable stopping = !atomicLoad(running);
            mtx.unlock_nothrow();

            if (took)
                gatherAndPublish(g, c);
            if (stopping)
            {
                // producer has stopped; we are the sole consumer, so drain the rest
                // without the lock so nothing queued before stop() leaks.
                GatherFn g2;
                void* c2;
                while (tryTake(g2, c2))
                    gatherAndPublish(g2, c2);
                return;
            }
        }
    }

    private void gatherAndPublish(GatherFn gather, void* ctx) nothrow @trusted
    {
        BatchSink sink;
        gather(ctx, sink);
        if (sink.len == 0)
        {
            sink.dropScratch();
            return;
        }
        // publish; the array ownership transfers to the ring entry on a successful
        // push, so DON'T drop the scratch here. If the reclaim ring is momentarily
        // full, yield until the loop drains it (bounded by the loop's tick cadence).
        while (!reclaim.push(null, cast(void*) sink.blocks, cast(ulong) sink.len))
            Thread.yield();
    }
}

// ---------------------------------------------------------------------------
// Tests. RingCore's FIFO/full/empty/wrap is proven in dreads.raftq, so here we
// drive a REAL free-thread roundtrip and check blocks actually leave the
// KeyspaceAllocator (bytesUsed returns to baseline).
// ---------------------------------------------------------------------------

version (unittest)
{
    import fluent.asserts;
    import dreads.alloc : keyspaceBytesUsed;

    private struct TestCtx
    {
        Block* blocks;
        size_t n;
    }

    private void testGather(void* ctx, ref BatchSink sink) @nogc nothrow @trusted
    {
        auto c = cast(TestCtx*) ctx;
        foreach (i; 0 .. c.n)
            sink.add(c.blocks[i].ptr, c.blocks[i].size);
    }

    @("lazyfree.roundtrip_frees_blocks_off_loop")
    unittest
    {
        import core.thread : Thread;
        import core.time : msecs;

        immutable before = keyspaceBytesUsed();
        enum N = 40;
        Block[N] blks;
        foreach (i; 0 .. N)
        {
            auto b = KeyspaceAllocator.instance.allocate(128 + i * 8);
            blks[i] = Block(b.ptr, b.length);
        }
        (keyspaceBytesUsed() > before).should.equal(true);

        auto ctx = TestCtx(blks.ptr, N);
        auto lf = new LazyFree(64, 64);
        lf.enqueue(&testGather, &ctx).should.equal(true); // queued off-loop

        // act as the event loop: drain the reclaim ring until every block is freed.
        size_t freed = 0;
        foreach (_; 0 .. 2000)
        {
            freed += lf.drainReclaimed();
            if (freed >= N)
                break;
            Thread.sleep(1.msecs);
        }
        freed.should.equal(cast(size_t) N);
        lf.stop();

        keyspaceBytesUsed().should.equal(before); // all blocks returned to the allocator
        lf.statSubmitted.should.equal(1UL);
        lf.statInline.should.equal(0UL);
        lf.statReclaimedBlocks.should.equal(cast(ulong) N);
    }

    @("lazyfree.budgeted_drain_spreads_over_calls")
    unittest
    {
        import core.thread : Thread;
        import core.time : msecs;

        immutable before = keyspaceBytesUsed();
        enum N = 100;
        Block[N] blks;
        foreach (i; 0 .. N)
        {
            auto b = KeyspaceAllocator.instance.allocate(64 + i);
            blks[i] = Block(b.ptr, b.length);
        }
        auto ctx = TestCtx(blks.ptr, N);
        auto lf = new LazyFree(64, 64);
        lf.enqueue(&testGather, &ctx);
        foreach (_; 0 .. 500) // wait for the batch to be published
        {
            if (lf.reclaimPending)
                break;
            Thread.sleep(1.msecs);
        }
        // drain 30 blocks at a time — the partial batch must carry across calls.
        size_t total = 0;
        immutable a = lf.drainReclaimed(30);
        (a <= 30).should.equal(true);
        total += a;
        (lf.reclaimPending).should.equal(true); // batch not fully drained yet
        while (lf.reclaimPending)
            total += lf.drainReclaimed(30);
        total.should.equal(cast(size_t) N);
        lf.stop();
        keyspaceBytesUsed().should.equal(before);
    }

    @("lazyfree.stop_drains_inflight")
    unittest
    {
        import core.thread : Thread;
        import core.time : msecs;

        immutable before = keyspaceBytesUsed();
        enum N = 16;
        Block[N] blks;
        foreach (i; 0 .. N)
        {
            auto b = KeyspaceAllocator.instance.allocate(200 + i);
            blks[i] = Block(b.ptr, b.length);
        }
        auto ctx = TestCtx(blks.ptr, N);
        auto lf = new LazyFree(64, 64);
        lf.enqueue(&testGather, &ctx);
        Thread.sleep(5.msecs); // let the thread gather + publish the batch
        lf.stop(); // stop() must drain the reclaim ring before teardown

        keyspaceBytesUsed().should.equal(before);
    }
}
