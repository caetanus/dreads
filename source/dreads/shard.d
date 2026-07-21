module dreads.shard;

// Thread-per-shard runtime. The DAY-1 architecture (single-thread was the interim to
// get a functional Redis first). Each shard is a full main-loop node: its own event
// loop, Keyspace, listener, and (phase 2b) Raft group — shared-nothing, one core each.
//
// HARD RULE: shards == 1 (the default) must NOT cost the single-thread path a thing.
// Everything here is gated on `gShardCount > 1`; when it is 1, the hot path never
// hashes a key, never consults a shard map, never hops. The gate is a single branch
// on a constant global — measured against the pre-sharding baseline to prove it.
//
// The routing model is dictated by the CLIENT protocol (req→resp same-conn in-order):
//   - a multi-server / cluster-aware client keeps one conn per shard and routes itself
//     → main-loop-per-shard, no router, no hop (Amdahl-optimal);
//   - a dumb single-conn client forces a central router: a foreign-key command is
//     hopped to the owner shard internally and answered on the same conn, in order.
// See memory/sharding-design.md + SHARDING.md.

import dreads.slots : keyToSlot, SLOTS;
import dreads.obj : Keyspace;
import dreads.raftq : RingCore;
import dreads.mem : ByteBuffer;

/// Number of data shards. 1 = single-thread (default), no router/shard split. Set once
/// at boot from `gConfig.shards`; read on the hot path as `gShardCount > 1`.
public __gshared uint gShardCount = 1;

/// True iff sharding is active. The ONE hot-path check — a load of a constant global,
/// so a shards=1 build predicts it not-taken and pays ~nothing.
public bool sharded() @nogc nothrow @trusted
{
    return gShardCount > 1;
}

/// Contiguous slot ranges per shard. shardOf(slot) is the owner. Built at boot when
/// sharded(); untouched (and unread) when shards == 1.
private __gshared uint[] gSlotBase; // gSlotBase[shard] = first slot owned by that shard

/// Owner shard of a slot (contiguous partition of the 16384 slots). O(log N) or O(1)
/// via the base table. Only called when sharded().
public uint shardOfSlot(ushort slot) @nogc nothrow @trusted
{
    // even split: slot / (SLOTS / gShardCount), clamped. Contiguous ranges keep this
    // a single divide; a base table lets us do arbitrary ranges later for rebalancing.
    immutable per = SLOTS / gShardCount;
    immutable s = slot / (per == 0 ? 1 : per);
    return s >= gShardCount ? gShardCount - 1 : s;
}

/// Owner shard of a key (hash the tag, map slot→shard). Only called when sharded().
public uint shardOfKey(scope const(char)[] key) @nogc nothrow @trusted
{
    return shardOfSlot(keyToSlot(key));
}

// --- per-shard state (shared-NOTHING) ------------------------------------------
// Each shard owns exactly ONE Keyspace (cluster/shard mode is DB-0-only, like Redis
// Cluster). A shard thread touches ONLY gShardKs[its own id] directly; another
// shard's keyspace is reached only by HOPPING the command to that shard's thread
// (never a cross-thread keyspace deref — that is what keeps it lock-free).
public __gshared Keyspace[] gShardKs; // length gShardCount when sharded; empty otherwise

/// Which shard THIS thread serves. Thread-local: set once when the shard loop starts;
/// stays 0 on the main thread and in the unsharded build, so `gShardKs[tShard]` and
/// the routing are never even consulted unless sharded().
public uint tShard = 0;

/// The keyspace this thread's connections read/write directly (its own shard's).
public Keyspace* myKeyspace() @nogc nothrow @trusted
{
    return &gShardKs[tShard];
}

// --- the cross-shard hop: SHARE-NOTHING, per-pair SPSC -------------------------
// The data path uses NO thread synchronization — that is the whole point of SPSC.
// Each shard's inbound is a BUNDLE of N single-producer/single-consumer ring lanes,
// one per peer: only thread `src` ever writes gInbound[dst].lanes[src], only thread
// `dst` ever reads it. So a producer never contends with another producer (each owns
// its own lane's tail) and never with the consumer (SPSC ring: producer owns tail,
// consumer owns head) — no mutex, no CAS on a shared cursor, nothing shared on the
// hot path. A shard writing to itself uses lanes[self] like any other (the caller's
// self-queue fast-path avoids even that).
//
// The ONLY cross-thread signal is a single per-shard wake event: a producer emits it
// (batched — once per pipeline batch, and only if the consumer is actually parked) so
// an idle consumer's event loop comes back to poll its lanes. Under load the consumer
// is never parked, so even that is skipped — the hop is pure lock-free ring traffic.
// A CLASS (not a struct), mirroring raftq.CrossQueue: its ManualEvent is reached
// through a class reference, which is the shape DMD's @safe/scope inference accepts
// for `waitUninterruptible` (a struct-in-__gshared-array reached by pointer trips a
// latent vibe-core 2.14.0 dip1000 inference bug in the unittest build).
final class ShardInbound
{
    import dreads.raftq : RingCore;
    import vibe.core.sync : ManualEvent, createSharedManualEvent;

    RingCore[] lanes; // lanes[src] = the SPSC ring from shard src to THIS shard
    shared(ManualEvent) hasData; // consumer parks here when all lanes are empty
    shared bool parked; // consumer published it is parked (so producers know to wake)

    this(uint peers) nothrow
    {
        lanes.length = peers;
        foreach (ref lane; lanes)
            lane.setup(INBOUND_CAP);
        try
            hasData = createSharedManualEvent();
        catch (Exception)
            assert(false, "shard inbound event alloc failed");
    }

    /// Any lane non-empty?
    bool anyReady() nothrow
    {
        foreach (ref lane; lanes)
            if (!lane.empty)
                return true;
        return false;
    }

    /// Consumer: block until a lane has data. Parks on the shard's single event only
    /// when genuinely idle. `waitUninterruptible` is called on `this.hasData` (a class
    /// method, like CrossQueue) — the shape DMD's dip1000 scope inference accepts.
    void waitData() nothrow
    {
        import core.atomic : atomicStore, atomicFence;

        if (anyReady())
            return;
        for (;;)
        {
            const ec = hasData.emitCount;
            atomicStore(parked, true);
            atomicFence(); // publish parked before rechecking the lanes
            if (anyReady())
            {
                atomicStore(parked, false);
                return;
            }
            hasData.waitUninterruptible(ec);
            atomicStore(parked, false);
            if (anyReady())
                return;
        }
    }

    /// Producer: wake the consumer iff it is parked (no-op while it is actively
    /// draining — the common case under load, so no syscall).
    void wake() nothrow
    {
        import core.atomic : atomicLoad, atomicFence;

        atomicFence(); // order our lane publish before the parked load
        if (atomicLoad(parked))
            hasData.emit();
    }
}

public __gshared ShardInbound[] gInbound; // gInbound[dst] — inbound bundle for a shard

// A `cmd` goes
// router→owner (meta = the requester's shard, so the owner knows where to reply); a
// `reply` goes owner→router (tag = the requester's Pending, filled + woken there).
public enum ShardMsg : uint
{
    cmd = 0,
    reply = 1,
}

// A reply slot owned by the REQUESTER thread. Passed by pointer to the owner (which
// never derefs it — just carries it back in the reply), then the requester's drain
// fiber fills `reply` and emits `done`; the connection fiber (same thread) reaps. Same
// LocalManualEvent-is-same-thread rule the raft Pending uses.
public struct ShardPending
{
    import dreads.mem : ByteBuffer;
    import vibe.core.sync : LocalManualEvent;

    ByteBuffer reply;
    LocalManualEvent done;
    bool ready;
    ShardPending* next; // thread-local free-list link (no per-op alloc)
}

// Per-router-thread free-list of reply slots. new only as the pool grows (raft rule);
// reused after — no GC on the routing hot path once warm. Thread-local: each router
// owns its slots (a slot's `done` is same-thread with the connection fiber waiting).
private ShardPending* tPendFree;

/// Take a reply slot for one hop (reset + ready=false). Only called on a router fiber.
public ShardPending* acquireShardPending() nothrow @trusted
{
    import vibe.core.sync : createManualEvent;

    auto p = tPendFree;
    if (p !is null)
    {
        tPendFree = p.next;
        p.ready = false;
        p.reply.clear();
        return p;
    }
    p = new ShardPending;
    try
        p.done = createManualEvent();
    catch (Exception)
        assert(false, "shard pending event alloc failed");
    return p;
}

/// Return a slot to this thread's free-list.
public void releaseShardPending(ShardPending* p) nothrow @trusted
{
    p.next = tPendFree;
    tPendFree = p;
}

import core.atomic : atomicLoad, atomicStore, atomicFence;

/// Enqueue a message from THIS shard (tShard) onto shard `dst`'s inbound, WITHOUT
/// waking the consumer. Pure SPSC: writes only lanes[tShard], which only this thread
/// produces and only `dst` consumes — no lock, no shared cursor. On a full ring it
/// wakes the consumer (else the batched-wake scheme could deadlock waiting on a
/// consumer we never signalled) and yields, then retries.
///
/// The wake is DEFERRED (`shardWake`) and BATCHED: a pipeline batch fires several
/// commands at the same owner, so ONE wake per (batch, owner) replaces one per command.
public void shardEnqueue(uint dst, scope const(ubyte)[] payload, void* tag, ulong meta, ShardMsg k) nothrow
{
    import vibe.core.core : yield;

    auto lane = &gInbound[dst].lanes[tShard]; // my SPSC lane to dst
    for (;;)
    {
        if (lane.push(payload, tag, meta, cast(uint) k))
            return;
        shardWake(dst); // full: let the consumer drain (never yield without a wake)
        try
            yield();
        catch (Exception)
        {
        }
    }
}

/// Wake shard `dst`'s drain if it is parked (a no-op if it is actively draining — the
/// common case under load, so no syscall). Called ONCE per batch per touched shard.
public void shardWake(uint dst) nothrow
{
    gInbound[dst].wake();
}

/// Enqueue + wake in one call (single-message path, e.g. a lone reply). Prefer the
/// split shardEnqueue/shardWake when firing a batch.
public void shardPush(uint dst, scope const(ubyte)[] payload, void* tag, ulong meta, ShardMsg k) nothrow
{
    shardEnqueue(dst, payload, tag, meta, k);
    shardWake(dst);
}

/// Consumer side (called on shard tShard's own thread): pop the next message from any
/// of my inbound lanes into `sink`, scanning lanes in order. Returns false if all empty.
/// Pure SPSC read: only this thread touches any lane's head.
/// ZERO-COPY consumer drain: for every message queued across my inbound lanes, call
/// `fn(payload, tag, meta, kind)` with a slice pointing straight into the ring (valid
/// until `fn` returns), then pop it. No intermediate buffer — the owner parses/dispatches
/// the command, or copies the reply into the requester's pending, directly from the ring.
/// Returns true if at least one message was processed. SPSC: only this thread pops.
public bool shardDrainOnce(alias fn)() nothrow
{
    bool any = false;
    foreach (ref lane; gInbound[tShard].lanes)
    {
        const(ubyte)[] p;
        void* tag;
        ulong meta;
        uint kind;
        while (lane.front(p, tag, meta, kind))
        {
            any = true;
            fn(p, tag, meta, kind); // p is a live slice into the slot; valid until pop
            lane.pop();
        }
    }
    return any;
}

/// Consumer side: block until at least one inbound lane has data (delegates to the
/// ShardInbound method so the vibe `waitUninterruptible` is instantiated on `this`).
public void shardWaitInbound() nothrow
{
    gInbound[tShard].waitData();
}

/// Initialize the shard runtime from config. Called once at boot. When count <= 1 this
/// is a no-op and the single-thread path stays exactly as it was.
public void shardInit(uint count) @trusted nothrow
{
    // Sharding is a POSIX feature BY DEFINITION. The model is the MULTI-ROUTER: N
    // listeners on the same port via SO_REUSEPORT, the kernel spreading accepts so each
    // thread is its own router (the Amdahl serial fraction distributed). Windows has no
    // SO_REUSEPORT → no multi-router (you'd be stuck with a single router = the exact
    // bottleneck we avoid), so sharding is OFF there — single-thread, and say so.
    version (Windows)
    {
        import core.stdc.stdio : printf;

        if (count > 1)
            printf("dreads: sharding needs the SO_REUSEPORT multi-router (not on Windows) — single-thread\n");
        gShardCount = 1;
        return;
    }
    else
    {
    if (count <= 1)
    {
        gShardCount = 1;
        return;
    }
    // the per-shard allocator slots are a fixed array (never resized so live freelist
    // state is never moved) — clamp to its capacity.
    import dreads.alloc : MAX_SHARDS;
    import core.stdc.stdio : printf;

    if (count > MAX_SHARDS)
    {
        printf("dreads: shards clamped to %u (max)\n", cast(uint) MAX_SHARDS);
        count = MAX_SHARDS;
    }
    gShardCount = count;
    // slot→shard base table (contiguous even split); phase 2b makes ranges movable.
    gSlotBase.length = count;
    immutable per = SLOTS / count;
    foreach (i; 0 .. count)
        gSlotBase[i] = cast(uint)(i * per);

    // per-shard keyspace (DB-0-only in shard mode) + inbound SPSC lane bundle.
    gShardKs.length = count;
    foreach (i, ref ks; gShardKs)
        ks.db = 0;
    gInbound.length = count;
    try
    {
        // one inbound bundle per shard: N SPSC ring lanes (lanes[src] → shard i, its
        // producer thread src, its consumer thread i — no lock) + one wake event.
        foreach (i; 0 .. count)
            gInbound[i] = new ShardInbound(count);
    }
    catch (Exception e)
        assert(false, "shard runtime alloc failed at boot");
    } // version(Windows) else
}

private enum size_t INBOUND_CAP = 1 << 14; // 16384 in-flight cross-shard commands / shard

unittest // slot partition math (no threads, pure mapping)
{
    shardInit(1);
    assert(!sharded());

    shardInit(4);
    assert(sharded() && gShardCount == 4);
    // 16_384 / 4 = 4096 slots each: slot 0 -> shard 0, 4096 -> 1, 8192 -> 2, 16_383 -> 3
    assert(shardOfSlot(0) == 0);
    assert(shardOfSlot(4095) == 0);
    assert(shardOfSlot(4096) == 1);
    assert(shardOfSlot(8192) == 2);
    assert(shardOfSlot(16_383) == 3);

    shardInit(3); // uneven: 16_384 / 3 = 5461 per, last shard soaks the remainder
    assert(shardOfSlot(0) == 0);
    assert(shardOfSlot(5_461) == 1);
    assert(shardOfSlot(10_922) == 2);
    assert(shardOfSlot(16_383) == 2); // clamped, never overflows gShardCount

    shardInit(1); // restore the default for other tests
}
