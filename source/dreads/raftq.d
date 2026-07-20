module dreads.raftq;

// Cross-thread hand-off queues for the dedicated raft event loop.
//
// dreads runs consensus on its own event-loop thread so a client flood on the
// main loop cannot starve raft ack-processing or delay heartbeats. The two
// threads talk only through these FIFOs:
//
//   main loop  --proposals-->  raft loop      (client writes to replicate)
//   raft loop  --commits---->  main loop      (committed entries to apply)
//
// Each queue has exactly ONE producer thread and ONE consumer thread (many
// client fibers push propQ, but they all run on the one main-loop thread,
// cooperatively scheduled, so they are a single logical producer). That makes
// this the textbook single-producer/single-consumer case, so RingCore is a
// LOCK-FREE Lamport ring: atomic head/tail with acquire/release, no mutex on
// the push/take hot path.
//
// Wakeups (for the idle case only) use a shared ManualEvent, but the producer
// emits ONLY when the consumer has actually parked (a flag it publishes before
// sleeping). Under load the consumer never parks, so the hot path is pure ring
// atomics — no lock, no syscall. The park/wake handshake uses seq_cst fences to
// order "publish tail" vs "load parked" (and the mirror on the consumer side)
// so a wakeup is never lost. Backpressure (a full ring) is the symmetric case.

import core.atomic : atomicFence, atomicLoad, atomicStore, MemoryOrder;
import core.stdc.stdlib : calloc, free;

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

/// Lock-free single-producer/single-consumer ring. The producer only writes
/// `tail`, the consumer only writes `head`; each sits on its own cache line to
/// avoid false sharing. The release-store of `tail` after filling a slot, paired
/// with the acquire-load of `tail` before reading it, publishes the slot's
/// contents cross-thread. Kept a plain struct so the FIFO/wrap logic stays unit-
/// testable without a vibe loop (single-threaded, the atomics are sequential).
struct RingCore
{
    private Slot* slots;
    private size_t mask; // cap - 1 (cap is a power of two)
    align(64) private shared size_t head_; // pop cursor (consumer-owned)
    align(64) private shared size_t tail_; // push cursor (producer-owned)

    /// calloc gives zeroed memory, and a zeroed ByteBuffer (null,0,0) is a
    /// valid empty buffer — so every slot starts usable without construction.
    void setup(size_t capPow2) @nogc nothrow
    {
        assert(capPow2 >= 2 && (capPow2 & (capPow2 - 1)) == 0, "cap must be pow2 >= 2");
        slots = cast(Slot*) calloc(capPow2, Slot.sizeof);
        assert(slots !is null, "raftq: out of memory");
        mask = capPow2 - 1;
        atomicStore(head_, cast(size_t) 0);
        atomicStore(tail_, cast(size_t) 0);
    }

    void teardown() @nogc nothrow
    {
        if (slots is null)
            return;
        foreach (i; 0 .. mask + 1)
            slots[i].buf.__dtor();
        free(slots);
        slots = null;
        atomicStore(head_, cast(size_t) 0);
        atomicStore(tail_, cast(size_t) 0);
        mask = 0;
    }

    size_t length() const @nogc nothrow
    {
        return atomicLoad!(MemoryOrder.acq)(tail_) - atomicLoad!(MemoryOrder.acq)(head_);
    }

    bool empty() const @nogc nothrow
    {
        return atomicLoad!(MemoryOrder.acq)(head_) == atomicLoad!(MemoryOrder.acq)(tail_);
    }

    bool full() const @nogc nothrow
    {
        return atomicLoad!(MemoryOrder.acq)(tail_) - atomicLoad!(MemoryOrder.acq)(head_) > mask;
    }

    /// Producer: copy `payload` into the tail slot and publish it. Returns false
    /// when full (the caller applies backpressure). The bytes are copied, so the
    /// source may be reused immediately after.
    bool push(scope const(ubyte)[] payload, void* tag, ulong meta, uint kind = 0) @nogc nothrow
    {
        const t = atomicLoad!(MemoryOrder.raw)(tail_); // producer owns tail
        const h = atomicLoad!(MemoryOrder.acq)(head_); // see the consumer's progress
        if (t - h > mask)
            return false; // full
        auto s = &slots[t & mask];
        s.buf.clear();
        if (payload.length)
            s.buf.append(payload);
        s.tag = tag;
        s.meta = meta;
        s.kind = kind;
        atomicStore!(MemoryOrder.rel)(tail_, t + 1); // publish (release the fill)
        return true;
    }

    /// Consumer: peek the head item. The returned slice stays valid until pop():
    /// the producer can't overwrite the head slot while it is held, because a
    /// write to that slot index needs occupancy == cap, which push() rejects.
    /// Returns false when empty.
    bool front(out const(ubyte)[] payload, out void* tag, out ulong meta, out uint kind) @nogc nothrow
    {
        const h = atomicLoad!(MemoryOrder.raw)(head_); // consumer owns head
        const t = atomicLoad!(MemoryOrder.acq)(tail_); // see the producer's publish
        if (h == t)
            return false; // empty
        auto s = &slots[h & mask];
        payload = s.buf.data;
        tag = s.tag;
        meta = s.meta;
        kind = s.kind;
        return true;
    }

    void pop() @nogc nothrow
    {
        const h = atomicLoad!(MemoryOrder.raw)(head_);
        atomicStore!(MemoryOrder.rel)(head_, h + 1); // free the slot for the producer
    }
}

/// SPSC cross-thread FIFO: a lock-free ring plus a park/wake handshake. The
/// ManualEvents fire only when a side has parked, so the push/take hot path is
/// pure ring atomics with no lock and no syscall.
final class CrossQueue
{
    private RingCore ring;
    private shared(ManualEvent) notEmpty; // consumer parks here when ring empty
    private shared(ManualEvent) notFull; // producer parks here when ring full
    private shared bool consumerParked;
    private shared bool producerParked;

    this(size_t capPow2)
    {
        ring.setup(capPow2);
        notEmpty = createSharedManualEvent();
        notFull = createSharedManualEvent();
    }

    // --- producer side ---

    /// Push, blocking (yielding the fiber) until there is room. Never drops: a
    /// dropped proposal is a lost write, a dropped commit corrupts the state
    /// machine — both unacceptable.
    void put(scope const(ubyte)[] payload, void* tag, ulong meta, uint kind = 0) nothrow
    {
        for (;;)
        {
            if (ring.push(payload, tag, meta, kind))
            {
                wakeConsumer();
                return;
            }
            // Full: park on notFull. Capture emitCount first so a drain between
            // our recheck and the wait can't be missed; the seq_cst fence orders
            // our parked publish before the recheck of full().
            const ec = notFull.emitCount;
            atomicStore(producerParked, true);
            atomicFence(); // seq_cst
            if (ring.full())
                notFull.waitUninterruptible(ec);
            atomicStore(producerParked, false);
        }
    }

    /// Non-blocking push that does NOT wake the consumer — @nogc-safe for the
    /// data path (an expiry/eviction DEL proposed from @nogc code, where blocking
    /// on backpressure OR a non-@nogc event emit is forbidden). Returns false if
    /// full (the item is DROPPED — only valid where loss is recoverable, e.g. an
    /// expiry DEL re-proposed next access; NEVER a client write). A parked
    /// consumer is woken separately by `nudge()` on the periodic timer, so a
    /// tryPut'd item is drained within one timer tick if no other put wakes first.
    bool tryPut(scope const(ubyte)[] payload, void* tag, ulong meta, uint kind = 0) @nogc nothrow
    {
        return ring.push(payload, tag, meta, kind);
    }

    /// Wake a parked consumer (non-@nogc: event emit). Called from a timer to
    /// drain items enqueued by tryPut, which intentionally skips the wake.
    void nudge() nothrow
    {
        wakeConsumer();
    }

    // Wake the consumer iff it has parked. The fence orders push()'s release of
    // tail before this load of consumerParked, mirroring the consumer's
    // park-then-recheck, so a wakeup is never lost.
    private void wakeConsumer() nothrow
    {
        atomicFence(); // seq_cst: order the just-published tail before the load
        if (atomicLoad(consumerParked))
            notEmpty.emit();
    }

    // --- consumer side ---

    /// Block until at least one item is available (yields the fiber while empty).
    void waitData() nothrow
    {
        if (!ring.empty())
            return; // fast path: no lock, no syscall
        for (;;)
        {
            const ec = notEmpty.emitCount; // capture before parking (no lost emit)
            atomicStore(consumerParked, true);
            atomicFence(); // seq_cst: publish parked before rechecking the ring
            if (!ring.empty())
            {
                atomicStore(consumerParked, false);
                return; // producer's push became visible — don't sleep
            }
            notEmpty.waitUninterruptible(ec); // returns at once if emitCount moved
            atomicStore(consumerParked, false);
            if (!ring.empty())
                return;
        }
    }

    /// Pop the head into a caller-owned buffer (copied out so the slot frees
    /// immediately for the producer). Returns false when empty.
    bool take(ref ByteBuffer sink, out void* tag, out ulong meta, out uint kind) nothrow
    {
        const(ubyte)[] p;
        if (!ring.front(p, tag, meta, kind))
            return false;
        sink.clear();
        if (p.length)
            sink.append(p);
        ring.pop();
        wakeProducer();
        return true;
    }

    private void wakeProducer() nothrow
    {
        atomicFence(); // seq_cst: order the freed slot before the load
        if (atomicLoad(producerParked))
            notFull.emit();
    }

    size_t length() nothrow
    {
        return ring.length();
    }
}

// ---------------------------------------------------------------------------
// Tests — RingCore is single-threaded-deterministic (the atomics are sequential
// on one thread), so its FIFO/full/empty/wrap behaviour is fully testable
// without any threads or event loop.
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
        meta.expect.to.equal(3UL);
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
