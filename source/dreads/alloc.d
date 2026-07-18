module dreads.alloc;

// Swappable data-plane allocator for the keyspace. The point is to A/B the D
// std.experimental.allocator building blocks (region/freelist/bitmapped) against
// Mallocator (jemalloc) and the GC — speed AND fragmentation — by BUILD (a
// `version`), and later to run the engine under the GC for embedded test libs.
//
// It is exposed as a global singleton with an `instance` accessor so it drops
// straight into emplace containers' existing `Allocator.instance` pattern (no
// per-container plumbing beyond swapping the alias). Single event-loop thread, so
// a plain __gshared instance is race-free — same reasoning as the rest of dreads.

import std.experimental.allocator.mallocator : Mallocator;

version (DreadsDataGC)
{
    import std.experimental.allocator.gc_allocator : GCAllocator;
    alias DataBackend = GCAllocator; // embedded/test builds: GC-managed data
}
else version (DreadsDataMalloc)
{
    alias DataBackend = Mallocator; // baseline: straight malloc/free (jemalloc)
}
else
{
    // The canonical general-purpose composition from the std.experimental.allocator
    // docs, re-parented onto Mallocator (we are zero-GC): size-segregated freelists
    // that RECLAIM individual frees, bitmapped blocks for the mid range, malloc for
    // the huge tail. `deallocate` returns blocks to the freelist for reuse.
    import std.experimental.allocator : unbounded;
    import std.experimental.allocator.building_blocks.free_list : FreeList;
    import std.experimental.allocator.building_blocks.bucketizer : Bucketizer;
    import std.experimental.allocator.building_blocks.segregator : Segregator;
    import std.experimental.allocator.building_blocks.allocator_list : AllocatorList;
    import std.experimental.allocator.building_blocks.bitmapped_block : BitmappedBlock;
    import std.algorithm.comparison : max;

    private alias FList = FreeList!(Mallocator, 0, unbounded);
    alias DataBackend = Segregator!(
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
}

// Wrap the backend in a StatsCollector tracking live bytes: this is the REAL,
// PORTABLE data-memory count (works on any build, not just jemalloc+Linux), so
// maxmemory enforcement stops being "virtual". The existing pre-check reads
// `keyspaceBytesUsed()` and refuses/evicts at the limit — the builder "fails at
// maxmemory" via that gate, never by a null the containers would assert on.
import std.experimental.allocator.building_blocks.stats_collector : StatsCollector, Options;

alias DataAllocator = StatsCollector!(DataBackend, Options.bytesUsed);

// Always stateful now (the collector holds counters), so one global instance with
// an `instance` accessor — drops straight into emplace's `Allocator.instance`
// pattern. Single event-loop thread ⇒ the __gshared instance is race-free.
private __gshared DataAllocator gDataAlloc;

struct KeyspaceAllocator
{
    static @property ref DataAllocator instance() @nogc nothrow @trusted
    {
        return gDataAlloc;
    }
}

/// Live bytes currently held by the keyspace data allocator — the real,
/// build-portable figure that drives maxmemory (replaces the jemalloc-only mallctl).
ulong keyspaceBytesUsed() @nogc nothrow @trusted
{
    return cast(ulong) gDataAlloc.bytesUsed;
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
