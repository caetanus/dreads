// Microbenchmark: HGETALL reply encoding — the lazy oracle vs hand-written
// direct emit. The lazy oracle streams the reply from the hash through a scope
// delegate (no materialized tree), so it should match direct emit. Run:
//   ldc2 -O3 bench/hgetall_oracle_bench.d source/dreads/mem.d source/dreads/dict.d \
//        source/dreads/resp.d -of=/tmp/hb && /tmp/hb
import std.stdio;
import std.format : sformat;
import core.time : MonoTime;
import core.stdc.stdio : snprintf;

import dreads.dict : Dict, StrVal;
import dreads.mem : ByteBuffer;
import dreads.resp : repArrayHeader, repBulk;

// The lazy oracle primitive (same as dreads.respvariant.lazyMap): proto-aware
// header + a scope delegate that streams the pairs. Stack-allocated delegate,
// no materialized tree, no allocation.
void lazyMap(ref ByteBuffer o, int proto, size_t pairs,
        scope void delegate(ref ByteBuffer, int) @nogc nothrow emit) @nogc nothrow
{
    o.appendByte(proto >= 3 ? '%' : '*');
    char[24] b = void;
    immutable n = snprintf(b.ptr, b.length, "%lld", cast(long)(proto >= 3 ? pairs : pairs * 2));
    o.append(b[0 .. n]);
    o.append("\r\n");
    emit(o, proto);
}

void main()
{
    foreach (N; [4, 16, 64, 256])
    {
        Dict!StrVal h;
        foreach (i; 0 .. N)
        {
            char[24] kb = void, vb = void;
            auto k = sformat(kb, "field%05d", i);
            auto v = sformat(vb, "value%05d", i);
            h.set(k, StrVal.of(v));
        }

        enum ITERS = 300_000;
        ByteBuffer o;

        auto t0 = MonoTime.currTime;
        foreach (_; 0 .. ITERS)
        {
            o.clear();
            repArrayHeader(o, N * 2);
            foreach (k, ref val; h)
            {
                repBulk(o, k);
                repBulk(o, val.s);
            }
        }
        immutable directNs = (MonoTime.currTime - t0).total!"nsecs" / cast(double) ITERS;

        auto t1 = MonoTime.currTime;
        foreach (_; 0 .. ITERS)
        {
            o.clear();
            lazyMap(o, 3, h.length, (ref oo, p) {
                foreach (k, ref val; h)
                {
                    repBulk(oo, k);
                    repBulk(oo, val.s);
                }
            });
        }
        immutable lazyNs = (MonoTime.currTime - t1).total!"nsecs" / cast(double) ITERS;

        writefln("N=%4d | direct %8.1f ns | lazy %8.1f ns (%.2fx)", N, directNs, lazyNs, lazyNs / directNs);
        stdout.flush();
    }
}
