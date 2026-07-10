// Isolated allocator A/B for the pubsub frame: raw malloc/free (jemalloc, as
// linked by dreads) vs std.experimental.allocator FreeList recycling. Answers
// whether a FreeList is worth it over jemalloc's per-thread tcache for the
// 1-alloc-per-publish frame workload. Run under perf:
//   ldc2 -O3 -release bench/rcalloc_bench.d -L-ljemalloc -of=/tmp/rcbench
//   perf stat -e cycles,instructions,cache-misses /tmp/rcbench
import std.stdio : writefln;
import core.time : MonoTime;
import core.stdc.stdlib : malloc, free;
import std.experimental.allocator.building_blocks.free_list : FreeList;
import std.experimental.allocator.mallocator : Mallocator;

__gshared size_t g_sink; // defeat DCE

enum SZ = 48; // a typical small RESP message frame
enum N = 50_000_000;

void main()
{
    // Keep a window of live allocations and observe each address, so the
    // optimizer cannot elide the malloc/free pair (which zeroed the last run).
    enum LIVE = 16;

    // A: raw malloc/free (jemalloc when linked)
    void*[LIVE] liveA = null;
    auto t0 = MonoTime.currTime;
    foreach (i; 0 .. N)
    {
        auto p = malloc(SZ);
        g_sink += cast(size_t) p; // observe the address -> no elision
        immutable s = i & (LIVE - 1);
        if (liveA[s] !is null)
            free(liveA[s]);
        liveA[s] = p;
    }
    foreach (p; liveA)
        if (p !is null)
            free(p);
    auto mallocNs = (MonoTime.currTime - t0).total!"nsecs" / cast(double) N;

    // B: std FreeList over Mallocator — recycles freed blocks in-process
    FreeList!(Mallocator, 0, 64) fl;
    void[][LIVE] liveB;
    auto t1 = MonoTime.currTime;
    foreach (i; 0 .. N)
    {
        auto raw = fl.allocate(SZ);
        g_sink += cast(size_t) raw.ptr;
        immutable s = i & (LIVE - 1);
        if (liveB[s].ptr !is null)
            fl.deallocate(liveB[s]);
        liveB[s] = raw;
    }
    foreach (b; liveB)
        if (b.ptr !is null)
            fl.deallocate(b);
    auto flNs = (MonoTime.currTime - t1).total!"nsecs" / cast(double) N;

    writefln("malloc/free (jemalloc) : %6.2f ns/op", mallocNs);
    writefln("FreeList (std)         : %6.2f ns/op", flNs);
    writefln("delta                  : %6.2f ns/op  (%.0f%%)", mallocNs - flNs,
            100.0 * (mallocNs - flNs) / mallocNs);
    writefln("sink=%d", g_sink);
}
