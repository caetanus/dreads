// Which allocator keeps the reply oracle cheap? The oracle's overhead is ~2
// node allocations per reply field. This measures exactly that pattern — alloc
// N node-sized blocks, then release them — for the three Alexandrescu
// strategies our Uniq can plug in:
//   Mallocator  — malloc/free per node (the current default)
//   FreeList    — reuse freed nodes (the pool)
//   Region      — bump-allocate all, reset in one shot (arena; ideal for a
//                 build-once / encode / drop reply tree)
import std.stdio;
import core.time : MonoTime;

import std.experimental.allocator.mallocator : Mallocator;
import std.experimental.allocator.building_blocks.free_list : FreeList;
import std.experimental.allocator.building_blocks.region : Region, InSituRegion;

enum NODE = 48; // ~ sizeof(RVariant node)

void main()
{
    enum ITERS = 200_000;

    foreach (fields; [4, 16, 64, 256])
    {
        immutable nodes = fields * 2; // key + value node per field
        void*[512] slots = void;

        // --- Mallocator: malloc + free each node ---
        auto t0 = MonoTime.currTime;
        foreach (_; 0 .. ITERS)
        {
            foreach (i; 0 .. nodes)
                slots[i] = Mallocator.instance.allocate(NODE).ptr;
            foreach (i; 0 .. nodes)
                Mallocator.instance.deallocate(slots[i][0 .. NODE]);
        }
        immutable mallocNs = (MonoTime.currTime - t0).total!"nsecs" / cast(double)(ITERS * nodes);

        // --- FreeList: reuses freed nodes across replies ---
        FreeList!(Mallocator, NODE) fl;
        auto t1 = MonoTime.currTime;
        foreach (_; 0 .. ITERS)
        {
            foreach (i; 0 .. nodes)
                slots[i] = fl.allocate(NODE).ptr;
            foreach (i; 0 .. nodes)
                fl.deallocate(slots[i][0 .. NODE]);
        }
        immutable freelistNs = (MonoTime.currTime - t1).total!"nsecs" / cast(double)(ITERS * nodes);

        // --- Region: bump-allocate, reset the whole arena per reply ---
        auto reg = Region!Mallocator(nodes * NODE + 64);
        auto t2 = MonoTime.currTime;
        foreach (_; 0 .. ITERS)
        {
            foreach (i; 0 .. nodes)
                cast(void) reg.allocate(NODE);
            reg.deallocateAll();
        }
        immutable regionNs = (MonoTime.currTime - t2).total!"nsecs" / cast(double)(ITERS * nodes);

        // --- InSituRegion: a stack buffer the optimizer can see through (the
        //     "inline the oracle" path — no opaque malloc, elidable) ---
        double insituNs = 0;
        {
            auto t3 = MonoTime.currTime;
            foreach (_; 0 .. ITERS)
            {
                InSituRegion!(512 * NODE) sr; // lives on the stack
                foreach (i; 0 .. nodes)
                    cast(void) sr.allocate(NODE);
                // no reset needed: dies with the stack frame each iteration
            }
            insituNs = (MonoTime.currTime - t3).total!"nsecs" / cast(double)(ITERS * nodes);
        }

        writefln("fields=%4d (%4d nodes) | malloc %5.1f | freelist %5.1f (%.1fx) | region %5.1f (%.1fx) | in-situ/stack %5.2f (%.1fx) ns/node",
                fields, nodes, mallocNs, freelistNs, mallocNs / freelistNs, regionNs,
                mallocNs / regionNs, insituNs, mallocNs / insituNs);
        stdout.flush();
    }
}
