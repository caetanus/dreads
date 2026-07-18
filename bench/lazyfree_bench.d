// Lazyfree end-to-end: free a huge SCATTERED value two ways, comparing LOOP-TIME.
//
// This is the case lazyfree is FOR (async UNLINK of one giant container), unlike
// active-expire (where a dominant on-loop d.del made it a tie). A big value's
// teardown = chase a scattered pointer structure (cache-miss-bound) AND deallocate
// each block. The chase can leave the loop; the deallocate (a freelist push that
// writes into the block) cannot (the composed KeyspaceAllocator is single-thread).
//
//   INLINE   (today): loop chases the structure AND deallocates — both on the loop.
//   LAZYFREE (new)  : the free-thread chases + gathers the block pointers into a
//                     dense batch (off-loop); the loop deallocates from that dense
//                     batch in a tick. Loop-time = enqueue (O(1)) + drainReclaimed.
//
// So lazyfree trades the loop's SCATTERED chase for a DENSE deallocate pass — the
// win is exactly the chase's cache-miss cost. We model the value as a randomly
// linked list of scattered nodes (a big LIST / the internal chain of a big hash),
// each node a KeyspaceAllocator block whose first word is the next pointer.
//
// Build:  dub build -b release --compiler=ldc2 --config=lazyfree-bench
// Run:    bin/lazyfree_bench [nnodes ...]      (default 100k 1M 4M)

import core.stdc.stdio : printf;
import core.thread : Thread;
import core.time : MonoTime;
import dreads.alloc : KeyspaceAllocator;
import dreads.lazyfree : LazyFree, BatchSink;

enum NODE_BYTES = 64; // scattered node; first 8 bytes = next pointer

// xorshift for a random link order (defeats the prefetcher → real chase misses).
uint xs(ref uint s) @nogc nothrow
{
    s ^= s << 13;
    s ^= s >> 17;
    s ^= s << 5;
    return s;
}

// Build a linked list of `n` scattered nodes in RANDOM address order. Allocate all
// nodes, shuffle their order, then thread next-pointers along the shuffle so the
// chase jumps around memory (like a real hash chain / list after churn). Returns
// the head; `nodes` receives the (unshuffled) block pointers for cleanup safety.
void* buildList(int n) @trusted
{
    auto arr = cast(void**) KeyspaceAllocator.instance.allocate(n * (void*).sizeof).ptr;
    foreach (i; 0 .. n)
        arr[i] = KeyspaceAllocator.instance.allocate(NODE_BYTES).ptr;
    // Fisher-Yates shuffle so link order != allocation order.
    uint s = 0x1234_5678;
    foreach_reverse (i; 1 .. n)
    {
        immutable j = xs(s) % (i + 1);
        auto t = arr[i];
        arr[i] = arr[j];
        arr[j] = t;
    }
    foreach (i; 0 .. n - 1)
        *(cast(void**) arr[i]) = arr[i + 1]; // node.next = next in shuffle
    *(cast(void**) arr[n - 1]) = null;
    auto head = arr[0];
    KeyspaceAllocator.instance.deallocate(arr[0 .. n]); // the index array is scratch
    return head;
}

// Free the list INLINE: chase + deallocate, fused, on the calling (loop) thread.
void freeInline(void* head) @trusted
{
    auto p = head;
    while (p !is null)
    {
        auto next = *(cast(void**) p);
        KeyspaceAllocator.instance.deallocate(p[0 .. NODE_BYTES]);
        p = next;
    }
}

// GatherFn: chase the list OFF the loop, recording each node block (frees nothing).
void gatherList(void* ctx, ref BatchSink sink) nothrow @trusted
{
    auto p = ctx;
    while (p !is null)
    {
        auto next = *(cast(void**) p);
        sink.add(p, NODE_BYTES);
        p = next;
    }
}

__gshared ulong gSink;

void run(int n) @trusted
{
    enum reps = 5;

    // A) inline free — loop-time = the whole chase + deallocate.
    double inlineNs = double.max;
    foreach (_; 0 .. reps)
    {
        auto head = buildList(n);
        immutable t0 = MonoTime.currTime;
        freeInline(head);
        immutable dt = cast(double)(MonoTime.currTime - t0).total!"nsecs";
        if (dt < inlineNs)
            inlineNs = dt;
    }

    // B) lazyfree — loop-time = enqueue + drainReclaimed (the chase is off-loop,
    // overlapped, and not charged to the loop).
    double lazyLoopNs = double.max;
    auto lf = new LazyFree(64, 64);
    foreach (_; 0 .. reps)
    {
        auto head = buildList(n);
        immutable tA = MonoTime.currTime;
        lf.enqueue(&gatherList, head); // O(1) hand-off
        immutable tB = MonoTime.currTime;
        // loop is free here; wait (idle) for the thread to finish the chase+gather.
        while (!lf.reclaimPending)
            Thread.yield();
        immutable tC = MonoTime.currTime;
        gSink += lf.drainReclaimed(); // loop deallocates the dense batch
        immutable tD = MonoTime.currTime;
        immutable loop = cast(double)((tB - tA).total!"nsecs" + (tD - tC).total!"nsecs");
        if (loop < lazyLoopNs)
            lazyLoopNs = loop;
    }
    lf.stop();

    printf("nodes=%-8d | INLINE loop %6.1f ns/node | LAZYFREE loop %6.1f ns/node | LOOP speedup %.2fx\n",
            n, inlineNs / n, lazyLoopNs / n, inlineNs / lazyLoopNs);
}

void main(string[] args) @trusted
{
    static immutable int[] defaults = [100_000, 1_000_000, 4_000_000];
    if (args.length > 1)
    {
        import std.conv : to;

        foreach (a; args[1 .. $])
            run(a.to!int);
    }
    else
        foreach (nn; defaults)
            run(nn);
}
