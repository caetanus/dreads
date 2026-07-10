// Microbenchmark: HGETALL reply encoding cost, oracle (materialized RVariant
// map) vs direct streaming emit. Isolates the encode path (no sockets) so the
// per-node allocation cost of the reply IR is visible. Run:
//   ldc2 -O3 -I<automem>/source -I<stdx>/source bench/hgetall_oracle_bench.d \
//        source/dreads/mem.d source/dreads/dict.d source/dreads/resp.d \
//        source/dreads/respvariant.d -of=/tmp/hb && /tmp/hb
import std.stdio;
import std.format : sformat;
import core.time : MonoTime;
import core.lifetime : move;

import dreads.dict : Dict, StrVal;
import dreads.mem : ByteBuffer;
import dreads.resp : repArrayHeader, repBulk;
import dreads.respvariant : MapT, Bulk, rv, encode;

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

        enum ITERS = 100_000;
        ByteBuffer o;

        // warmup + direct streaming emit (RESP3 array header is identical to
        // RESP2 for a flat list; measures the zero-alloc path)
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

        // oracle: build a MapT tree then encode (RESP3 %N)
        auto t1 = MonoTime.currTime;
        foreach (_; 0 .. ITERS)
        {
            o.clear();
            MapT m;
            foreach (k, ref val; h)
                m.add(Bulk(k), Bulk(val.s));
            auto reply = rv(move(m));
            encode(reply, o, 3);
        }
        immutable oracleNs = (MonoTime.currTime - t1).total!"nsecs" / cast(double) ITERS;

        writefln("N=%4d fields | direct %7.1f ns | oracle %7.1f ns | %.1fx slower | +%.0f ns/field",
                N, directNs, oracleNs, oracleNs / directNs, (oracleNs - directNs) / N);
        stdout.flush();
    }
}
