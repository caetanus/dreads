// Active-expire drain micro-benchmark: the emplace.Map (ordered expiry index)
// drained two ways, in ISOLATION — no network, no d.del, no notify (those are
// identical on-loop costs in production, so they'd only add noise to the delta
// we care about). This measures ONLY the part the RB-tree split changes:
//
//   A) removeRight(now)  — consuming range: remove each due bucket node from the
//      tree one at a time (O(1) amortised/node) and dispose its value as we go.
//      This is what active-expire does TODAY.
//   B) detachLedge(now) + walkDetached + disposeDetached — split the whole due
//      prefix off in ONE O(log n) rebalance, then walk + tear the detached side
//      down. This is the split path; in production the teardown moves off-loop.
//
// The question (per the "benchmark primeiro" call): does the split alone, ON the
// loop, beat removeRight? Prediction: roughly a TIE — both walk O(K) nodes to
// dispose them; the split only wins once disposeDetached runs off the event loop.
// This bench proves/refutes that before we build a whole threaded subsystem.
//
// Build:  dub build -b release --compiler=ldc2 --config=expire-bench
// Run:    bin/expire_bench [nbuckets keysPerBucket ...]

import core.stdc.stdlib : malloc;
import core.stdc.stdio : printf;
import core.time : MonoTime;
import emplace.map : Map;
import emplace.vector : Vector;

// A bucket of key slices, exactly like the production ExpBucket (a Vector of
// non-owning Dict-key slices). Its destructor frees the backing array — the
// teardown cost both drains must pay.
alias Bucket = Vector!(const(char)[]);
alias ExpMap = Map!(ulong, Bucket);

// "key:<i>" in a permanent (leaked) buffer — slices stay valid for the whole run.
const(char)[] makeKey(int i) @trusted
{
    auto p = cast(char*) malloc(24);
    int n = 0;
    foreach (c; "key:")
        p[n++] = c;
    if (i == 0)
        p[n++] = '0';
    else
    {
        char[12] tmp;
        int t = 0, v = i;
        while (v)
        {
            tmp[t++] = cast(char)('0' + v % 10);
            v /= 10;
        }
        while (t)
            p[n++] = tmp[--t];
    }
    return p[0 .. n];
}

// Fill a fresh map: `nbuckets` deadlines (1..nbuckets), each holding
// `keysPerBucket` key slices. Total live keys = nbuckets * keysPerBucket.
void fill(ref ExpMap m, int nbuckets, int keysPerBucket) @trusted
{
    int id = 0;
    foreach (d; 1 .. nbuckets + 1)
    {
        auto bkt = m.emplace(cast(ulong) d); // build the bucket in place
        foreach (_; 0 .. keysPerBucket)
            bkt.put(makeKey(id++));
    }
}

// Sink so the compiler can't elide the walk.
__gshared size_t gSink;

void run(int nbuckets, int keysPerBucket) @trusted
{
    immutable keys = cast(long) nbuckets * keysPerBucket;
    enum reps = 5;
    immutable now = cast(ulong) nbuckets; // everything is due (full drain)

    // A) removeRight — build fresh each rep, time the drain.
    double aNs = double.max;
    foreach (_; 0 .. reps)
    {
        ExpMap m;
        fill(m, nbuckets, keysPerBucket);
        immutable t0 = MonoTime.currTime;
        foreach (e; m.removeRight(now))
            gSink += e.value.length; // touch the bucket (== per-key work stand-in)
        immutable dt = (MonoTime.currTime - t0).total!"nsecs";
        if (dt < aNs)
            aNs = cast(double) dt;
    }

    // B) detachLedge + walk + dispose — build fresh each rep.
    double bNs = double.max, detachOnlyNs = double.max;
    foreach (_; 0 .. reps)
    {
        ExpMap m;
        fill(m, nbuckets, keysPerBucket);
        immutable t0 = MonoTime.currTime;
        auto det = m.detachLedge(now);
        immutable t1 = MonoTime.currTime;
        cast(void) ExpMap.walkDetached(det, (ref ulong k, ref Bucket v) @nogc nothrow{
            gSink += v.length;
            return 0;
        });
        ExpMap.disposeDetached(det);
        immutable dt = (MonoTime.currTime - t0).total!"nsecs";
        immutable detachDt = (t1 - t0).total!"nsecs";
        if (dt < bNs)
            bNs = cast(double) dt;
        if (detachDt < detachOnlyNs)
            detachOnlyNs = cast(double) detachDt;
    }

    printf("nbuckets=%-8d keys/bkt=%-4d nodes=%-8d keys=%-9lld | ",
            nbuckets, keysPerBucket, nbuckets, keys);
    printf("removeRight %7.1f ns/node  %6.2f ns/key | ", aNs / nbuckets, aNs / keys);
    printf("detach+dispose %7.1f ns/node  %6.2f ns/key | ", bNs / nbuckets, bNs / keys);
    printf("detach-only %7.0f ns total (%.3f ns/node) | ", detachOnlyNs, detachOnlyNs / nbuckets);
    printf("split/removeRight %.2fx\n", bNs / aNs);
}

void main(string[] args) @trusted
{
    // pairs: nbuckets keysPerBucket. Default sweep: tree-heavy (1 key/bucket) →
    // bucket-heavy (many keys/bucket), so the split's node-count sensitivity shows.
    static immutable int[2][] defaults = [
        [100_000, 1], [1_000_000, 1], // tree-heavy: nodes == keys (split-favorable)
        [10_000, 10], [10_000, 100], // mixed
        [1_000, 1000], // bucket-heavy: few nodes, many keys (split-neutral)
    ];
    if (args.length > 2)
    {
        for (size_t i = 1; i + 1 < args.length; i += 2)
        {
            import std.conv : to;

            run(args[i].to!int, args[i + 1].to!int);
        }
    }
    else
        foreach (p; defaults)
            run(p[0], p[1]);
}
