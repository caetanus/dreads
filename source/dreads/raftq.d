module dreads.raftq;

// Cross-thread hand-off queues for the dedicated raft event loop.
//
// dreads runs consensus on its own event-loop thread so a client flood on the
// main loop cannot starve raft ack-processing or delay heartbeats (which was
// causing spurious elections and wild throughput spikes). The two threads talk
// only through these FIFOs:
//
//   main loop  --proposals-->  raft loop      (client writes to replicate)
//   raft loop  --commits---->  main loop      (committed entries to apply)
//
// The keyspace stays single-threaded on the main loop: the raft loop never
// touches gKeys, it only reaches consensus and ships committed payloads back.
//
// RingCore is the pure, @nogc, single-thread ring — fully unit-tested (the
// "theory"). CrossQueue wraps it with a Mutex + two shared ManualEvents for
// the actual cross-thread wakeups, reusing the same lost-wakeup-safe emitCount
// protocol the durability syncer already relies on.

import core.stdc.stdlib : calloc, free;
import core.sync.mutex : Mutex;

import vibe.core.sync : createSharedManualEvent, ManualEvent;

import dreads.mem : ByteBuffer;

// One queued item: a copied byte payload plus an opaque tag pointer (the
// pending reply slot, or null for non-local commits) and a u64 meta (the log
// index / clock). The payload buffer is owned by the slot and reused as the
// ring wraps, so steady-state traffic allocates nothing.
private struct Slot
{
    ByteBuffer buf;
    void* tag;
    ulong meta;
    uint kind; // consumer-defined discriminator (commitQ: apply/snapshot/fail)
}

/// Pure single-thread ring of `Slot`. No locks, no I/O — the concurrency lives
/// entirely in CrossQueue. Kept separate so the index arithmetic (FIFO order,
/// full/empty, power-of-two wraparound) is testable without a vibe event loop.
struct RingCore
{
    private Slot* slots;
    private size_t mask; // cap - 1 (cap is a power of two)
    private size_t head; // pop cursor, monotonic
    private size_t tail; // push cursor, monotonic

    /// calloc gives zeroed memory, and a zeroed ByteBuffer (null,0,0) is a
    /// valid empty buffer — so every slot starts usable without construction.
    void setup(size_t capPow2) @nogc nothrow
    {
        assert(capPow2 >= 2 && (capPow2 & (capPow2 - 1)) == 0, "cap must be pow2 >= 2");
        slots = cast(Slot*) calloc(capPow2, Slot.sizeof);
        assert(slots !is null, "raftq: out of memory");
        mask = capPow2 - 1;
        head = tail = 0;
    }

    void teardown() @nogc nothrow
    {
        if (slots is null)
            return;
        // Free each slot's owned buffer, then the array.
        foreach (i; 0 .. mask + 1)
            slots[i].buf.__dtor();
        free(slots);
        slots = null;
        head = tail = mask = 0;
    }

    size_t length() const @nogc nothrow
    {
        return tail - head;
    }

    bool empty() const @nogc nothrow
    {
        return head == tail;
    }

    bool full() const @nogc nothrow
    {
        return tail - head > mask;
    }

    /// Copy `payload` into the tail slot. Returns false when full (the caller
    /// applies backpressure). The bytes are copied, so the source may be reused
    /// immediately after.
    bool push(scope const(ubyte)[] payload, void* tag, ulong meta, uint kind = 0) @nogc nothrow
    {
        if (full())
            return false;
        auto s = &slots[tail & mask];
        s.buf.clear();
        if (payload.length)
            s.buf.append(payload);
        s.tag = tag;
        s.meta = meta;
        s.kind = kind;
        ++tail;
        return true;
    }

    /// Peek the head item without removing it. The returned slice stays valid
    /// until pop(): a producer can never overwrite the head slot while it is
    /// held, because a write to that slot index requires occupancy == cap,
    /// which full() rejects. Returns false when empty.
    bool front(out const(ubyte)[] payload, out void* tag, out ulong meta, out uint kind) @nogc nothrow
    {
        if (empty())
            return false;
        auto s = &slots[head & mask];
        payload = s.buf.data;
        tag = s.tag;
        meta = s.meta;
        kind = s.kind;
        return true;
    }

    void pop() @nogc nothrow
    {
        assert(!empty(), "raftq: pop on empty");
        ++head;
    }
}

/// Cross-thread FIFO: many producers on one event-loop thread, a single
/// consumer on another. The Mutex serializes the ring; the two shared events
/// wake a blocked consumer (notEmpty) or a back-pressured producer (notFull).
final class CrossQueue
{
    private RingCore ring;
    private Mutex mtx;
    private shared(ManualEvent) notEmpty;
    private shared(ManualEvent) notFull;

    this(size_t capPow2)
    {
        ring.setup(capPow2);
        mtx = new Mutex;
        notEmpty = createSharedManualEvent();
        notFull = createSharedManualEvent();
    }

    // --- producer side ---

    /// Push, blocking (yielding the fiber) until there is room. Never drops:
    /// a dropped proposal is a lost write, a dropped commit corrupts the state
    /// machine — both unacceptable.
    void put(scope const(ubyte)[] payload, void* tag, ulong meta, uint kind = 0) nothrow
    {
        for (;;)
        {
            auto ec = notFull.emitCount;
            bool ok;
            {
                mtx.lock_nothrow();
                scope (exit)
                    mtx.unlock_nothrow();
                ok = ring.push(payload, tag, meta, kind);
            }
            if (ok)
            {
                notEmpty.emit();
                return;
            }
            notFull.waitUninterruptible(ec); // recheck via emitCount: no lost wakeup
        }
    }

    // --- consumer side ---

    /// Wait until at least one item is available, then leave it at the head for
    /// drain(). Yields the fiber while empty.
    void waitData() nothrow
    {
        for (;;)
        {
            auto ec = notEmpty.emitCount;
            bool has;
            {
                mtx.lock_nothrow();
                scope (exit)
                    mtx.unlock_nothrow();
                has = !ring.empty();
            }
            if (has)
                return;
            notEmpty.waitUninterruptible(ec);
        }
    }

    /// Pop the head into a caller-owned buffer (copied out so the slot frees
    /// immediately for producers). Returns false when empty. `sink` is cleared
    /// and filled with the payload.
    bool take(ref ByteBuffer sink, out void* tag, out ulong meta, out uint kind) nothrow
    {
        bool has;
        {
            mtx.lock_nothrow();
            scope (exit)
                mtx.unlock_nothrow();
            const(ubyte)[] p;
            has = ring.front(p, tag, meta, kind);
            if (has)
            {
                sink.clear();
                if (p.length)
                    sink.append(p);
                ring.pop();
            }
        }
        if (has)
            notFull.emit();
        return has;
    }

    size_t length() nothrow
    {
        mtx.lock_nothrow();
        scope (exit)
            mtx.unlock_nothrow();
        return ring.length();
    }
}

// ---------------------------------------------------------------------------
// Tests — RingCore is pure, so its FIFO/full/empty/wrap behaviour is fully
// deterministic without any threads or event loop.
// ---------------------------------------------------------------------------

version (unittest)
{
    import fluent.asserts;

    private const(ubyte)[] b(string s) @nogc nothrow
    {
        return cast(const(ubyte)[]) s;
    }

    @("raftq.ring_fifo_order")
    unittest
    {
        RingCore r;
        r.setup(8);
        scope (exit)
            r.teardown();

        r.push(b("a"), null, 1).expect.to.equal(true);
        r.push(b("bb"), null, 2).expect.to.equal(true);
        r.push(b("ccc"), null, 3).expect.to.equal(true);
        r.length.expect.to.equal(3UL);

        const(ubyte)[] p;
        void* tag;
        ulong meta;
        uint kind;
        r.front(p, tag, meta, kind).expect.to.equal(true);
        (cast(string) p).expect.to.equal("a");
        meta.expect.to.equal(1UL);
        r.pop();
        r.front(p, tag, meta, kind).expect.to.equal(true);
        (cast(string) p).expect.to.equal("bb");
        meta.expect.to.equal(2UL);
        r.pop();
        r.front(p, tag, meta, kind).expect.to.equal(true);
        (cast(string) p).expect.to.equal("ccc");
        r.pop();
        r.empty.expect.to.equal(true);
        r.front(p, tag, meta, kind).expect.to.equal(false);
    }

    @("raftq.ring_full_rejects")
    unittest
    {
        RingCore r;
        r.setup(4); // capacity 4
        scope (exit)
            r.teardown();

        foreach (i; 0 .. 4)
            r.push(b("x"), null, i).expect.to.equal(true);
        r.full.expect.to.equal(true);
        r.push(b("y"), null, 99).expect.to.equal(false); // rejected, not overwritten
        r.length.expect.to.equal(4UL);

        // draining one makes room for exactly one more
        const(ubyte)[] p;
        void* tag;
        ulong meta;
        uint kind;
        r.front(p, tag, meta, kind);
        meta.expect.to.equal(0UL);
        r.pop();
        r.push(b("z"), null, 100).expect.to.equal(true);
        r.full.expect.to.equal(true);
    }

    @("raftq.ring_wraparound_reuses_slots")
    unittest
    {
        RingCore r;
        r.setup(4);
        scope (exit)
            r.teardown();

        // Cycle far past capacity so tail/head wrap the power-of-two mask many
        // times; FIFO + payload integrity must hold throughout.
        ulong next = 0;
        const(ubyte)[] p;
        void* tag;
        ulong meta;
        uint kind;
        foreach (round; 0 .. 100)
        {
            r.push(b("payload"), cast(void*) round, next).expect.to.equal(true);
            r.front(p, tag, meta, kind).expect.to.equal(true);
            (cast(string) p).expect.to.equal("payload");
            meta.expect.to.equal(next);
            (cast(size_t) tag).expect.to.equal(round);
            r.pop();
            ++next;
        }
        r.empty.expect.to.equal(true);
    }

    @("raftq.ring_interleaved_partial_drain")
    unittest
    {
        RingCore r;
        r.setup(8);
        scope (exit)
            r.teardown();

        // Push 5, drain 2, push 4 (wraps), drain the rest — order preserved.
        foreach (i; 0 .. 5)
            r.push(b("p"), null, i).expect.to.equal(true);
        const(ubyte)[] p;
        void* tag;
        ulong meta;
        uint kind;
        foreach (expect; 0 .. 2)
        {
            r.front(p, tag, meta, kind);
            meta.expect.to.equal(cast(ulong) expect);
            r.pop();
        }
        foreach (i; 5 .. 9)
            r.push(b("p"), null, i).expect.to.equal(true);
        r.length.expect.to.equal(7UL);
        foreach (expect; 2 .. 9)
        {
            r.front(p, tag, meta, kind).expect.to.equal(true);
            meta.expect.to.equal(cast(ulong) expect);
            r.pop();
        }
        r.empty.expect.to.equal(true);
    }
}
