// Keyspace-structure micro-benchmark: the REAL emplace containers (no wheel
// reinvented) — HashMap (the current keyspace table) vs Map (a CLRS red-black
// tree, the ordered map) — on two access patterns, with a 168-byte value
// (== RObj.sizeof) so the per-entry footprint matches production:
//   - POINT: random get of an existing key (the GET/SET hot path)
//   - SCAN : full in-order/though-table iteration (KEYS/SCAN/range pattern)
// Goal: decide keyspace structure BY NUMBER for each workload, not by theory.
//
// Build:  dub build -b release --compiler=ldc2 --config=lookup-bench
// Run:    bin/lookup_bench [nkeys ...]     (default 100000 300000 1000000)

import core.stdc.stdlib : malloc;
import core.stdc.stdio : printf;
import core.time : MonoTime;
import emplace.hashmap : HashMap;
import emplace.map : Map;

enum VAL_BYTES = 168; // == RObj.sizeof

struct BigVal
{
    ubyte[VAL_BYTES] pad;
}

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

// xorshift so the lookup order is random (defeats the prefetcher) without libc rand
uint xs(ref uint s) @nogc nothrow
{
    s ^= s << 13;
    s ^= s >> 17;
    s ^= s << 5;
    return s;
}

void run(int nkeys) @trusted
{
    auto keys = cast(const(char)[]*) malloc(nkeys * (const(char)[]).sizeof);
    foreach (i; 0 .. nkeys)
        keys[i] = makeKey(i);
    BigVal v;

    HashMap!BigVal h;
    Map!(const(char)[], BigVal) t;
    foreach (i; 0 .. nkeys)
    {
        h.set(keys[i], v);
        t.set(keys[i], v);
    }

    enum LOOKUPS = 5_000_000;
    auto order = cast(int*) malloc(LOOKUPS * int.sizeof);
    uint s = 0x9e3779b9;
    foreach (i; 0 .. LOOKUPS)
        order[i] = xs(s) % nkeys;

    // ---- POINT: random get ----
    ulong sink = 0;
    auto p0 = MonoTime.currTime;
    foreach (i; 0 .. LOOKUPS)
        sink += cast(size_t) cast(void*) h.get(keys[order[i]]);
    auto p1 = MonoTime.currTime;
    foreach (i; 0 .. LOOKUPS)
        sink += cast(size_t) cast(void*) t.get(keys[order[i]]);
    auto p2 = MonoTime.currTime;

    // ---- SCAN: full iteration (hash slot order vs tree sorted order) ----
    ulong hsum = 0, tsum = 0;
    enum PASSES = 20;
    auto sc0 = MonoTime.currTime;
    foreach (_; 0 .. PASSES)
        h.opApply((const(char)[] k, ref BigVal val) @nogc nothrow {
            hsum += val.pad[0] + k.length;
            return 0;
        });
    auto sc1 = MonoTime.currTime;
    foreach (_; 0 .. PASSES)
        t.opApply((ref const(char)[] k, ref BigVal val) @nogc nothrow {
            tsum += val.pad[0] + k.length;
            return 0;
        });
    auto sc2 = MonoTime.currTime;

    immutable hPt = (p1 - p0).total!"nsecs" / cast(double) LOOKUPS;
    immutable tPt = (p2 - p1).total!"nsecs" / cast(double) LOOKUPS;
    immutable hSc = (sc1 - sc0).total!"nsecs" / cast(double)(PASSES * nkeys);
    immutable tSc = (sc2 - sc1).total!"nsecs" / cast(double)(PASSES * nkeys);

    printf("--- nkeys=%d (hash.len=%zu tree.len=%zu) ---\n", nkeys, h.length, t.length);
    printf("  POINT  hash=%6.1f ns   tree=%6.1f ns   (hash %.2fx)\n", hPt, tPt, tPt / hPt);
    printf("  SCAN   hash=%6.1f ns   tree=%6.1f ns   (tree %.2fx)   sink=%llu s=%llu\n",
        hSc, tSc, hSc / tSc, sink, hsum + tsum);
}

// Value mimicking ExpBucket (Vector!(const(char)[]) ~= 24 bytes)
struct Bucket
{
    ubyte[24] b;
}

// Expiry-index workload: insert N deadlines (getOrPut, like registerExpire), then
// drain them all via removeRight (like the active-expire cycle). Exposes the
// O(M log n) redundant-find + re-descend in RemoveRange.popFront.
void runExpiry(int n) @trusted
{
    auto ds = cast(ulong*) malloc(n * ulong.sizeof);
    uint s = 0x00C0FFEE;
    foreach (i; 0 .. n)
        ds[i] = (cast(ulong) xs(s) << 21) ^ xs(s); // spread-out absolute deadlines
    Bucket bv;

    Map!(ulong, Bucket) m;
    auto i0 = MonoTime.currTime;
    foreach (i; 0 .. n)
        m.getOrPut(ds[i], bv);
    auto i1 = MonoTime.currTime;

    ulong sink = 0;
    auto d0 = MonoTime.currTime;
    foreach (e; m.removeRight(ulong.max)) // drains every entry, ascending
        sink += e.key;
    auto d1 = MonoTime.currTime;

    immutable insNs = (i1 - i0).total!"nsecs" / cast(double) n;
    immutable drnNs = (d1 - d0).total!"nsecs" / cast(double) n;
    printf("--- expiry nkeys=%d (drained=%zu) ---\n", n, m.length);
    printf("  insert=%6.1f ns   drain(removeRight)=%6.1f ns/entry   sink=%llu\n",
        insNs, drnNs, sink);
}

void main(string[] args) @trusted
{
    int[] sizes;
    if (args.length > 1)
        foreach (a; args[1 .. $])
        {
            int n = 0;
            foreach (c; a)
                if (c >= '0' && c <= '9')
                    n = n * 10 + (c - '0');
            if (n > 0)
                sizes ~= n;
        }
    else
        sizes = [100_000, 300_000, 1_000_000];

    foreach (n; sizes)
        run(n);
    foreach (n; sizes)
        runExpiry(n);
}
