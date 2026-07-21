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
}

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
