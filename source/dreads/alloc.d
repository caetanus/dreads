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

static if (is(typeof(DataBackend.instance)))
{
    // Stateless singleton backend (Mallocator/GCAllocator): use it as-is.
    alias KeyspaceAllocator = DataBackend;
}
else
{
    // Stateful composed backend: one global instance, exposed via `instance` so the
    // emplace containers' `Allocator.instance.allocate/deallocate` reach its state.
    private __gshared DataBackend gDataAlloc;

    struct KeyspaceAllocator
    {
        static @property ref DataBackend instance() @nogc nothrow @trusted
        {
            return gDataAlloc;
        }
    }
}

// Feasibility probe: the data plane is @nogc nothrow, so the chosen backend must
// allocate/free in that context. Compiling this is the whole point of phase 0.
@nogc nothrow unittest
{
    auto b = KeyspaceAllocator.instance.allocate(500);
    assert(b.length >= 500);
    KeyspaceAllocator.instance.deallocate(b);
}
