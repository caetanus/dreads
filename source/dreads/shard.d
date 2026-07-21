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
import dreads.raftq : CrossQueue;

/// Number of data shards. 1 = single-thread (default), no router/shard split. Set once
/// at boot from `gConfig.shards`; read on the hot path as `gShardCount > 1`.
public __gshared uint gShardCount = 1;

/// True iff sharding is active. The ONE hot-path check — a load of a constant global,
/// so a shards=1 build predicts it not-taken and pays ~nothing.
pragma(inline, true)
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
pragma(inline, true)
public Keyspace* myKeyspace() @nogc nothrow @trusted
{
    return &gShardKs[tShard];
}

// --- the cross-shard hop (dumb-client path), the STUPIDLY-SIMPLE v1 ------------
// ONE inbound command queue per shard. Any router thread pushes a command destined
// for shard O onto gInbound[O]; O's drain fiber executes it on O's keyspace and wakes
// the requester. The reply travels in a `Pending` the REQUESTER owns (passed by
// pointer, filled by O, its cross-thread event woken) — exactly the raft Pending
// pattern, generalized from 1 worker to N.
//
// v1 is intentionally dumb, TWO things to OPTIMIZE later (tracked here + in
// memory/sharding-design.md):
//   1. SELF-QUEUE: when the owner O == the router's own shard, we STILL round-trip
//      through the queue. The obvious win is a local fast-path (execute inline, skip
//      the queue + wake) — big for slot-affine traffic. DO NOT ship without this once
//      correctness is proven.
//   2. The inbound queue takes many producers (MPSC) → v1 guards the producer side
//      with a per-shard lock. Replace with lock-free MPSC (or per-pair SPSC) once the
//      hop shows up as the serial bottleneck in the Amdahl `s` measurement.
public __gshared CrossQueue[] gInbound; // gInbound[shard] — commands for that shard

/// The inbound queue of shard `s` (that shard's thread is the sole consumer).
pragma(inline, true)
public CrossQueue inbound(uint s) @nogc nothrow @trusted
{
    return gInbound[s];
}

// The one queue per shard carries BOTH directions; `kind` says which. A `cmd` goes
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
    uint reqShard; // who to reply to (this thread's shard), stamped at send
}

// v1 MPSC guard: a shard's inbound takes many producers (routers sending cmds + owners
// sending replies). v1 locks the producer side around the (non-yielding) ring push.
// TODO(perf): replace with lock-free MPSC once the hop shows as the Amdahl bottleneck.
private __gshared Mutex[] gInboundLock;
import core.sync.mutex : Mutex;

/// Push a message onto shard `s`'s inbound, MPSC-safe. Locks ONLY around the ring push
/// (never across a fiber yield); on a full ring it drops the lock, yields, and retries,
/// so backpressure never holds a cross-thread lock. Wakes `s`'s drain fiber.
public void shardPush(uint s, scope const(ubyte)[] payload, void* tag, ulong meta, ShardMsg k) nothrow
{
    import vibe.core.core : yield;

    for (;;)
    {
        gInboundLock[s].lock_nothrow();
        immutable ok = gInbound[s].tryPut(payload, tag, meta, cast(uint) k);
        gInboundLock[s].unlock_nothrow();
        if (ok)
        {
            gInbound[s].nudge(); // wake the consumer (drain fiber) if parked
            return;
        }
        try
            yield(); // ring full: let the consumer drain, then retry (no lock held)
        catch (Exception)
        {
        }
    }
}

/// Initialize the shard runtime from config. Called once at boot. When count <= 1 this
/// is a no-op and the single-thread path stays exactly as it was.
public void shardInit(uint count) @trusted nothrow
{
    if (count <= 1)
    {
        gShardCount = 1;
        return;
    }
    gShardCount = count;
    // slot→shard base table (contiguous even split); phase 2b makes ranges movable.
    gSlotBase.length = count;
    immutable per = SLOTS / count;
    foreach (i; 0 .. count)
        gSlotBase[i] = cast(uint)(i * per);

    // per-shard keyspace (DB-0-only in shard mode) + inbound command queue.
    gShardKs.length = count;
    foreach (i, ref ks; gShardKs)
        ks.db = 0;
    gInbound.length = count;
    gInboundLock.length = count;
    try
    {
        foreach (i; 0 .. count)
        {
            gInbound[i] = new CrossQueue(INBOUND_CAP);
            gInboundLock[i] = new Mutex;
        }
    }
    catch (Exception e)
        assert(false, "shard runtime alloc failed at boot");
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
