module dreads.alloc;

// Swappable data-plane allocator for the keyspace.
//
// WHY this exists (the reason for the whole refactor) is NOT throughput and NOT
// even the ~5% fragmentation edge — it is INDEPENDENCE FROM jemalloc:
//   * a StatsCollector gives a REAL, PORTABLE live-byte count (keyspaceBytesUsed /
//     connBytesUsed) that drives maxmemory on ANY platform. jemalloc's mallctl was
//     Linux-only and "virtual"; now OOM is real everywhere.
//   * WE own the reclaim/reuse policy (a size-segregated freelist) instead of being
//     hostage to jemalloc's arena + dirty-decay behaviour — note freed blocks do
//     NOT return to the OS under jemalloc either (it holds dirty pages until decay),
//     so the composed backend wins by handing jemalloc a COARSE pattern (few big
//     blocks) and doing the fine pooling ourselves.
//   * it is swappable by BUILD (composed / Mallocator / GC / bitmap / region), which
//     is what makes the A/B below possible and lets us run under the GC for the
//     embedded Python/Go test libs.
//
// Fragmentation A/B (bench/rss_churn.sh, 265 MB live, 40 churn rounds, RSS/used):
//   composed 1.40  <  malloc/jemalloc 1.47  <  bitmap 1.79  <  bump/region 3.53.
// The composed (freelist + bucketizer + bitmapped mid) is the best of the tested
// compositions — bucketizer beats a bitmap-heavy layout (BitmappedBlock rounds to
// its block size ⇒ internal waste); a bump/region barely reclaims. The bitmap/bump
// variants stay version-gated (zero cost) as the A/B infrastructure that proved it.
//
// It is exposed as a global singleton with an `instance` accessor so it drops
// straight into emplace containers' existing `Allocator.instance` pattern (no
// per-container plumbing beyond swapping the alias). Single event-loop thread, so
// a plain __gshared instance is race-free — same reasoning as the rest of dreads.

// Two swappable planes with the SAME design but SEPARATE instances/counters:
//  - the KEYSPACE (data) plane — drives maxmemory (keyspaceBytesUsed);
//  - the CONNECTION plane — network/reply buffers, per-command scratch, the write
//    queue ring (connBytesUsed → client-buffer accounting).
// Both default to the composed backend below; each is overridable to GC or straight
// Mallocator by build (DreadsData*/DreadsConn* versions) so either plane can be A/B'd
// or run under the GC independently.

import std.experimental.allocator.mallocator : Mallocator;
import std.experimental.allocator.building_blocks.stats_collector : StatsCollector, Options;
import std.experimental.allocator : unbounded;
import std.experimental.allocator.building_blocks.free_list : FreeList;
import std.experimental.allocator.building_blocks.bucketizer : Bucketizer;
import std.experimental.allocator.building_blocks.segregator : Segregator;
import std.experimental.allocator.building_blocks.allocator_list : AllocatorList;
import std.experimental.allocator.building_blocks.bitmapped_block : BitmappedBlock;
import std.experimental.allocator.building_blocks.region : Region;
import std.experimental.allocator.building_blocks.null_allocator : NullAllocator;
import std.algorithm.comparison : max;

private alias FList = FreeList!(Mallocator, 0, unbounded);

// The canonical general-purpose composition from the std.experimental.allocator
// docs, re-parented onto Mallocator (we are zero-GC): size-segregated freelists
// that RECLAIM individual frees, bitmapped blocks for the mid range, malloc for the
// huge tail. Shared TYPE; each plane gets its own StatsCollector instance below.
private alias ComposedBackend = Segregator!(
        8, FreeList!(Mallocator, 0, 8),
        128, Bucketizer!(FList, 1, 128, 16),
        256, Bucketizer!(FList, 129, 256, 32),
        512, Bucketizer!(FList, 257, 512, 64),
        1024, Bucketizer!(FList, 513, 1024, 128),
        2048, Bucketizer!(FList, 1025, 2048, 256),
        3584, Bucketizer!(FList, 2049, 3584, 512),
        4072 * 1024, AllocatorList!((size_t n) => BitmappedBlock!(4096)(
            cast(ubyte[]) Mallocator.instance.allocate(max(n, 4072 * 1024))), Mallocator),
        Mallocator);

// --- composition A/B variants (fragmentation study; throughput is allocator-neutral) ---
// Bitmap-heavy: BitmappedBlock tiers instead of freelists — reclaims by clearing a
// bit, fine granularity, no per-size free list.
private alias BitmapBackend = Segregator!(
        256, AllocatorList!((size_t n) => BitmappedBlock!(16)(
            cast(ubyte[]) Mallocator.instance.allocate(max(n, 1024 * 1024))), Mallocator),
        4072 * 1024, AllocatorList!((size_t n) => BitmappedBlock!(256)(
            cast(ubyte[]) Mallocator.instance.allocate(max(n, 4072 * 1024))), Mallocator),
        Mallocator);

// Bump/region: a growable Region that bump-allocates and only reclaims LIFO (an
// arbitrary free is a no-op) — the "arena that barely frees" extreme, to show what
// the freelist buys under churn.
private alias BumpBackend = Segregator!(
        4072 * 1024, AllocatorList!((size_t n) => Region!NullAllocator(
            cast(ubyte[]) Mallocator.instance.allocate(max(n, 4072 * 1024))), Mallocator),
        Mallocator);

// NEGATIVE RESULT (2026-07-19, perf-stat A/B, kept as a note not code): replacing the
// small-range Bucketizer tiers with a fine Segregator of per-size-class FreeLists
// (same 16B granularity, dispatch by compile-time comparisons, no runtime bucket
// division and no `roundUpToMultipleOf(s, step)` runtime modulo — that std helper is
// 0.51% self on the SET path because `step` reaches it as a `uint` runtime arg) was
// MEASURED SLOWER: ins/op 4310 vs 4226, rps 1.39M vs 1.44M. The 9-way compile-time
// if-chain costs MORE branches than the Bucketizer's one division saves. The
// Bucketizer stays; the ~0.5% is not cheaply reclaimable this way.

version (DreadsDataGC)
{
    import std.experimental.allocator.gc_allocator : GCAllocator;
    alias DataBackend = GCAllocator; // embedded/test builds: GC-managed data
}
else version (DreadsDataMalloc)
    alias DataBackend = Mallocator; // baseline: straight malloc/free (jemalloc)
else version (DreadsDataBitmap)
    alias DataBackend = BitmapBackend;
else version (DreadsDataBump)
    alias DataBackend = BumpBackend;
else
    alias DataBackend = ComposedBackend;

version (DreadsConnGC)
{
    import std.experimental.allocator.gc_allocator : GCAllocator;
    alias ConnBackend = GCAllocator;
}
else version (DreadsConnMalloc)
    alias ConnBackend = Mallocator;
else
    alias ConnBackend = ComposedBackend;

// Each backend is wrapped in a StatsCollector tracking live bytes: the REAL,
// PORTABLE memory count (works on any build, not just jemalloc+Linux). For the
// keyspace this drives maxmemory (stops being "virtual"); for connections it feeds
// client-buffer accounting.

// Debug-only live-range tracker (version=DreadsAllocTrack). Sits under the stats
// collector and asserts the moment the backend hands out a block overlapping a
// still-live one (aliasing) or frees a block that isn't live (double/foreign
// free) — the definitive diagnostic for a freelist that a bad free has poisoned.
version (DreadsAllocTrack)
{
    struct Tracked(Backend)
    {
        Backend b;
        private static struct Rec
        {
            void* p;
            size_t n;
        }

        private __gshared Rec[1 << 18] live;
        private __gshared size_t nlive;

        enum alignment = Backend.alignment;

        void[] allocate(size_t n) @nogc nothrow @trusted
        {
            auto r = b.allocate(n);
            if (r.ptr !is null)
            {
                foreach (i; 0 .. nlive)
                {
                    auto s = live[i];
                    immutable overlap = r.ptr < s.p + s.n && s.p < r.ptr + r.length;
                    assert(!overlap, "ALLOC HANDED OUT A BLOCK OVERLAPPING A LIVE ONE (freelist poisoned)");
                }
                assert(nlive < live.length, "tracker table full");
                live[nlive++] = Rec(r.ptr, r.length);
            }
            return r;
        }

        bool deallocate(void[] blk) @nogc nothrow @trusted
        {
            if (blk.ptr is null)
                return true;
            foreach (i; 0 .. nlive)
                if (live[i].p == blk.ptr)
                {
                    live[i] = live[nlive - 1];
                    nlive--;
                    return b.deallocate(blk);
                }
            assert(false, "DEALLOCATE OF A NON-LIVE BLOCK (double-free or foreign free)");
        }

        static if (__traits(hasMember, Backend, "reallocate"))
            bool reallocate(ref void[] blk, size_t n) @nogc nothrow @trusted
            {
                void* old = blk.ptr;
                immutable ok = b.reallocate(blk, n);
                if (ok)
                {
                    foreach (i; 0 .. nlive)
                        if (live[i].p == old)
                        {
                            live[i] = Rec(blk.ptr, blk.length);
                            return ok;
                        }
                    if (old is null && blk.ptr !is null)
                        live[nlive++] = Rec(blk.ptr, blk.length);
                }
                return ok;
            }
    }

    alias DataAllocator = StatsCollector!(Tracked!DataBackend, Options.bytesUsed);
}
else
    alias DataAllocator = StatsCollector!(DataBackend, Options.bytesUsed);

alias ConnAllocatorT = StatsCollector!(ConnBackend, Options.bytesUsed);

// Always stateful now (the collectors hold counters), so one global instance per
// plane with an `instance` accessor — drops straight into the `Allocator.instance`
// pattern. Single event-loop thread ⇒ the __gshared instances are race-free.
private __gshared DataAllocator gDataAlloc;
private __gshared ConnAllocatorT gConnAlloc;

/// Keyspace (data) plane — every RObj value routes here; drives maxmemory.
struct KeyspaceAllocator
{
    static @property ref DataAllocator instance() @nogc nothrow @trusted
    {
        return gDataAlloc;
    }
}

/// Connection plane — network/reply buffers, per-command scratch, the write-queue
/// ring. Separate instance/counter so its bytes never touch maxmemory (keyspace).
struct ConnAllocator
{
    static @property ref ConnAllocatorT instance() @nogc nothrow @trusted
    {
        return gConnAlloc;
    }
}

/// Live bytes currently held by the keyspace data allocator — the real,
/// build-portable figure that drives maxmemory (replaces the jemalloc-only mallctl).
ulong keyspaceBytesUsed() @nogc nothrow @trusted
{
    return cast(ulong) gDataAlloc.bytesUsed;
}

/// Live bytes currently held by the connection-plane allocator (network/reply
/// buffers, scratch, write queue) — the basis for client-buffer accounting.
ulong connBytesUsed() @nogc nothrow @trusted
{
    return cast(ulong) gConnAlloc.bytesUsed;
}

// Feasibility probe: the data plane is @nogc nothrow, so the backend must
// allocate/free in that context, and the collector must count.
@nogc nothrow unittest
{
    immutable before = keyspaceBytesUsed();
    auto b = KeyspaceAllocator.instance.allocate(500);
    assert(b.length >= 500);
    assert(keyspaceBytesUsed() >= before + 500);
    KeyspaceAllocator.instance.deallocate(b);
    assert(keyspaceBytesUsed() == before);
}

// Contract guard: `reallocate` across a Segregator threshold (b.length <= t < s)
// must still resize correctly and NOT overflow a neighbor. Segregator.reallocate
// returns false on a cross-threshold move, but the std free-function falls back to
// allocate-new/copy/deallocate-old, so the grow succeeds. The containers grow by
// doubling (crossing thresholds), so this path is load-bearing — guards sandwich
// the block to catch any regression that leaves it undersized.
@nogc nothrow unittest
{
    import std.experimental.allocator : reallocate;

    void[] g1 = KeyspaceAllocator.instance.allocate(256);
    void[] b = KeyspaceAllocator.instance.allocate(64);
    void[] g2 = KeyspaceAllocator.instance.allocate(256);
    (cast(ubyte[]) g1)[] = 0xEE;
    (cast(ubyte[]) g2)[] = 0xEE;

    immutable ok = reallocate(KeyspaceAllocator.instance, b, 256); // 64 -> 256 crosses 128
    assert(ok, "reallocate across the 128 threshold returned false");
    assert(b.length == 256, "reallocate did not actually resize");
    (cast(ubyte[]) b)[] = 0x22; // write the full claimed size

    foreach (x; cast(ubyte[]) g1)
        assert(x == 0xEE, "reallocate overflowed into the low neighbor");
    foreach (x; cast(ubyte[]) g2)
        assert(x == 0xEE, "reallocate overflowed into the high neighbor");

    KeyspaceAllocator.instance.deallocate(g1);
    KeyspaceAllocator.instance.deallocate(b);
    KeyspaceAllocator.instance.deallocate(g2);
}
