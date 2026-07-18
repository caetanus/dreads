// Active-expire drain micro-benchmark — END-TO-END, with the real d.del.
//
// An earlier version isolated only the expiry-index teardown and (reframed as
// loop-time, dispose offloaded) showed the split winning 3-14x. But d.del — the
// per-key unlink from the keyspace + value free + notify — stays ON the loop in
// BOTH strategies and dominates. This version includes a real keyspace HashMap
// (168-byte values == RObj.sizeof) and does an actual remove() per expired key,
// so the numbers reflect what the event loop truly pays.
//
//   A) removeRight(now): drain each due bucket, remove() each key. Node + bucket
//      teardown is FUSED into the drain — all on the loop.
//   B) detachLedge(now) + walk(remove() each key) ON loop, then disposeDetached
//      OFF loop (lazyfree thread). Loop-time(B) = detach + walk.
//
// The question: once d.del is present, does offloading the tree teardown still
// move the loop-time needle, or does d.del swamp the difference?
//
// Build:  dub build -b release --compiler=ldc2 --config=expire-bench
// Run:    bin/expire_bench [nbuckets keysPerBucket ...]

import core.stdc.stdlib : malloc;
import core.stdc.stdio : printf;
import core.time : MonoTime;
import emplace.map : Map;
import emplace.vector : Vector;
import emplace.hashmap : HashMap;

alias Bucket = Vector!(const(char)[]); // == production ExpBucket (non-owning key slices)
alias ExpMap = Map!(ulong, Bucket);

enum VAL_BYTES = 168; // == RObj.sizeof
struct BigVal
{
    ubyte[VAL_BYTES] pad;
}

alias Keyspace = HashMap!(BigVal); // the keyspace table d.del removes from

// Persistent key pool, generated once and reused across reps/paths so the same
// logical key is a stable slice (content-compared by HashMap either way).
__gshared const(char)[]* gKeys;
__gshared int gKeysGen;

void ensureKeys(int n) @trusted
{
    if (n <= gKeysGen)
        return;
    gKeys = cast(const(char)[]*) realloc_keys(gKeys, n);
    foreach (i; gKeysGen .. n)
        gKeys[i] = makeKey(i);
    gKeysGen = n;
}

void* realloc_keys(void* p, int n) @trusted
{
    import core.stdc.stdlib : realloc;

    return realloc(p, n * (const(char)[]).sizeof);
}

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

// Build a fresh keyspace (all keys) and expiry index (nbuckets deadlines, each
// with keysPerBucket key slices) over the shared key pool.
void build(ref Keyspace ks, ref ExpMap m, int nbuckets, int keysPerBucket) @trusted
{
    int id = 0;
    foreach (d; 1 .. nbuckets + 1)
    {
        auto bkt = m.emplace(cast(ulong) d);
        foreach (_; 0 .. keysPerBucket)
        {
            ks.set(gKeys[id], BigVal.init);
            bkt.put(gKeys[id]);
            id++;
        }
    }
}

__gshared size_t gSink;

void run(int nbuckets, int keysPerBucket) @trusted
{
    enum reps = 3;
    immutable now = cast(ulong) nbuckets; // everything is due (full drain)
    ensureKeys(nbuckets * keysPerBucket);

    // A) removeRight + d.del — all on the loop.
    double aNs = double.max;
    foreach (_; 0 .. reps)
    {
        Keyspace ks;
        ExpMap m;
        build(ks, m, nbuckets, keysPerBucket);
        immutable t0 = MonoTime.currTime;
        foreach (e; m.removeRight(now))
            foreach (k; e.value[])
                gSink += ks.remove(k); // real d.del: unlink + free the 168B value
        immutable dt = cast(double)(MonoTime.currTime - t0).total!"nsecs";
        if (dt < aNs)
            aNs = dt;
    }

    // B) detachLedge + walk(d.del) ON loop, disposeDetached OFF loop.
    double detachNs = double.max, walkNs = double.max, disposeNs = double.max;
    foreach (_; 0 .. reps)
    {
        Keyspace ks;
        ExpMap m;
        build(ks, m, nbuckets, keysPerBucket);
        Keyspace* ksp = &ks;
        immutable t0 = MonoTime.currTime;
        auto det = m.detachLedge(now);
        immutable t1 = MonoTime.currTime;
        cast(void) ExpMap.walkDetached(det, (ref ulong dl, ref Bucket v) @trusted{
            foreach (k; v[])
                gSink += ksp.remove(k); // real d.del on the loop
            return 0;
        });
        immutable t2 = MonoTime.currTime;
        ExpMap.disposeDetached(det);
        immutable t3 = MonoTime.currTime;
        immutable dNs = cast(double)(t1 - t0).total!"nsecs";
        immutable wNs = cast(double)(t2 - t1).total!"nsecs";
        immutable zNs = cast(double)(t3 - t2).total!"nsecs";
        if (dNs < detachNs)
            detachNs = dNs;
        if (wNs < walkNs)
            walkNs = wNs;
        if (zNs < disposeNs)
            disposeNs = zNs;
    }
    immutable loopSplit = detachNs + walkNs;
    immutable keys = cast(double)(cast(long) nbuckets * keysPerBucket);

    printf("nbuckets=%-8d keys/bkt=%-4d keys=%-9lld | ", nbuckets, keysPerBucket,
            cast(long) nbuckets * keysPerBucket);
    printf("LOOP removeRight %6.1f | split(detach+walk) %6.1f ns/key ", aNs / keys, loopSplit / keys);
    printf("(detach %5.2f + walk %5.1f) | offloop dispose %5.2f ns/key | ",
            detachNs / keys, walkNs / keys, disposeNs / keys);
    printf("LOOP speedup %.2fx\n", aNs / loopSplit);
}

void main(string[] args) @trusted
{
    static immutable int[2][] defaults = [
        [100_000, 1], [1_000_000, 1],
        [10_000, 10], [10_000, 100],
        [1_000, 1000],
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
