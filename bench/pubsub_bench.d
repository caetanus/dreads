// Micro-benchmark: publish cost vs pattern count P.
//
// Proves PUBSUB.md's claim — the header-indexed pattern match is O(len(channel)),
// independent of P, versus the naive "glob every pattern" scan which is O(P).
// No sockets, no server: it drives PubSub.publish directly and, as a baseline,
// loops globMatch over the same pattern set (what the old publish did).
//
//   dub run --config=pubsub-bench --compiler=ldc2 --build=release

import std.stdio : writefln, writeln;
import std.conv : to;
import core.time : MonoTime;

import dreads.pubsub : PubSub, Subscriber;
import dreads.commands : globMatch;

__gshared ulong g_sink; // defeat dead-code elimination

extern (C) void noopSink(void* ctx, scope const(ubyte)[] b) nothrow
{
    g_sink += b.length;
}

void main()
{
    static void sink(void* ctx, scope const(ubyte)[] b) nothrow
    {
        g_sink += b.length;
    }

    immutable size_t[] Ns = [100, 1000, 10_000, 100_000];
    writeln("      P   new (ns/pub)   naive (ns/pub)   speedup");
    foreach (N; Ns)
    {
        auto subs = new Subscriber[](N);
        auto pats = new char[][](N);
        auto chans = new string[](N);
        PubSub ps;
        foreach (i; 0 .. N)
        {
            subs[i].sink = &sink;
            pats[i] = ("chan:" ~ i.to!string ~ ":*").dup; // prefix pattern -> header index
            chans[i] = ("chan:" ~ i.to!string ~ ":evt"); // matches exactly pats[i]
            ps.psubscribe(&subs[i], pats[i]);
        }

        // Work budget so the naive O(P) side stays bounded at large P.
        size_t iters = 200_000_000 / N;
        if (iters < 500)
            iters = 500;
        if (iters > 100_000)
            iters = 100_000;

        // New path: PubSub.publish uses the header-indexed match.
        g_sink = 0;
        auto t0 = MonoTime.currTime;
        foreach (it; 0 .. iters)
            ps.publish(chans[it % N], "x");
        auto newNs = (MonoTime.currTime - t0).total!"nsecs" / cast(double) iters;

        // Baseline: scan every pattern with globMatch (the old behaviour).
        g_sink = 0;
        auto t1 = MonoTime.currTime;
        foreach (it; 0 .. iters)
        {
            auto ch = chans[it % N];
            foreach (p; pats)
                if (globMatch(p, ch))
                    g_sink++;
        }
        auto naiveNs = (MonoTime.currTime - t1).total!"nsecs" / cast(double) iters;

        writefln("%7d   %11.1f   %13.1f   %6.1fx", N, newNs, naiveNs, naiveNs / newNs);
    }
}
