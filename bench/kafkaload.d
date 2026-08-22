// kafkaload — pipelined Kafka load generator (same-binary, both brokers).
// Produce mode: acks=1, correlation-pipelined with a sliding window of
// in-flight Produce requests (B messages each) — the broker must append and
// answer every request. Fetch mode: sequential fetches counting messages.
// Speaks the pre-flexible dialect (Produce v2 / Fetch v3 / MessageSet v1)
// that both dreads' skin and Apache Kafka accept.
//
//   ldc2 -O2 bench/kafkaload.d -of kafkaload
//   ./kafkaload pub host port topic n [payloadLen] [window]
//   ./kafkaload sub host port topic n
module kafkaload;

import core.stdc.stdio : printf, fflush, stdout;
import std.socket : TcpSocket, InternetAddress, SocketOptionLevel, SocketOption;
import std.datetime.stopwatch : StopWatch, AutoStart;
import std.conv : to;
import std.digest.crc : crc32Of;

__gshared ubyte[1 << 24] rbuf; // fetch responses fill partition_max_bytes
__gshared size_t rlen;
TcpSocket gs;

void putI16(ref ubyte[] o, short v) { o ~= cast(ubyte)(cast(ushort) v >> 8); o ~= cast(ubyte)(v & 0xFF); }
void putI32(ref ubyte[] o, int v)
{
    o ~= cast(ubyte)(cast(uint) v >> 24); o ~= cast(ubyte)(cast(uint) v >> 16);
    o ~= cast(ubyte)(cast(uint) v >> 8); o ~= cast(ubyte)(v & 0xFF);
}
void putI64(ref ubyte[] o, long v) { putI32(o, cast(int)(v >> 32)); putI32(o, cast(int)(v & 0xFFFFFFFF)); }
void putStr(ref ubyte[] o, const(char)[] s) { putI16(o, cast(short) s.length); o ~= cast(const(ubyte)[]) s; }

uint rdU32(const(ubyte)[] p, size_t i)
{
    return (cast(uint) p[i] << 24) | (cast(uint) p[i + 1] << 16) | (cast(uint) p[i + 2] << 8) | p[i + 3];
}

// read one complete size-prefixed response into a fresh slice window
const(ubyte)[] readResp()
{
    for (;;)
    {
        if (rlen >= 4)
        {
            immutable sz = rdU32(rbuf[], 0);
            if (rlen >= 4 + sz)
            {
                auto r = rbuf[4 .. 4 + sz].dup;
                immutable tot = 4 + sz;
                foreach (i; 0 .. rlen - tot)
                    rbuf[i] = rbuf[tot + i];
                rlen -= tot;
                return r;
            }
        }
        auto n = gs.receive(rbuf[rlen .. $]);
        assert(n > 0, "broker closed");
        rlen += n;
    }
}

ubyte[] request(short api, short ver, int corr, const(ubyte)[] body_)
{
    ubyte[] r;
    putI16(r, api);
    putI16(r, ver);
    putI32(r, corr);
    putStr(r, "kafkaload"); // client_id
    r ~= body_;
    ubyte[] f;
    putI32(f, cast(int) r.length);
    f ~= r;
    return f;
}

int main(string[] args)
{
    if (args.length < 6)
    {
        printf("usage: kafkaload pub|sub host port topic n [payloadLen] [window]\n");
        return 2;
    }
    auto mode = args[1];
    auto host = args[2];
    immutable port = args[3].to!ushort;
    auto topic = args[4];
    immutable n = args[5].to!long;
    gs = new TcpSocket(new InternetAddress(host, port));
    gs.setOption(SocketOptionLevel.TCP, SocketOption.TCP_NODELAY, true);

    // Metadata v1 (also serves as auto-create nudge on real Kafka)
    {
        ubyte[] b;
        putI32(b, 1);
        putStr(b, topic);
        gs.send(request(3, 1, 1, b));
        readResp();
    }

    if (mode == "pub")
    {
        immutable plen = args.length > 6 ? args[6].to!size_t : 16;
        immutable window = args.length > 7 ? args[7].to!long : 64; // in-flight REQUESTS
        enum B = 32; // messages per produce request
        // one MessageSet v1 message with a plen payload
        auto payload = new ubyte[plen];
        payload[] = 'x';
        ubyte[] msg;
        {
            ubyte[] m;
            m ~= 1; // magic
            m ~= 0; // attrs
            {
                import std.datetime.systime : Clock;

                putI64(m, Clock.currTime.toUnixTime() * 1000); // real timestamp:
            }   // a stale one trips Kafka's retention reaper instantly
            putI32(m, -1); // null key
            putI32(m, cast(int) plen);
            m ~= payload;
            auto crc = crc32Of(m);
            ubyte[] full;
            // crc32Of returns little-endian byte order of the digest; wire wants big-endian value
            full ~= [crc[3], crc[2], crc[1], crc[0]];
            full ~= m;
            putI64(msg, 0); // producer-side offset (ignored)
            putI32(msg, cast(int) full.length);
            msg ~= full;
        }
        ubyte[] setB;
        foreach (i; 0 .. B)
            setB ~= msg;
        // produce v2 request body: acks, timeout, topics
        ubyte[] body_;
        putI16(body_, 1);
        putI32(body_, 30000);
        putI32(body_, 1);
        putStr(body_, topic);
        putI32(body_, 1);
        putI32(body_, 0); // partition 0 (single-partition stream bench)
        putI32(body_, cast(int) setB.length);
        body_ ~= setB;

        // prebuild ONE produce request; patch the correlation id in place per
        // send (a rebuild+alloc per request made the CLIENT the bottleneck)
        auto reqBytes = request(0, 2, 0, body_);
        enum corrAt = 4 + 2 + 2; // after size, api, version
        // send several requests per syscall
        enum RB = 8;
        auto burst = new ubyte[reqBytes.length * RB];
        long sentReqs = 0, ackedReqs = 0;
        immutable long totalReqs = (n + B - 1) / B;
        auto sw = StopWatch(AutoStart.yes);
        int corr = 100;
        while (ackedReqs < totalReqs)
        {
            while (sentReqs < totalReqs && sentReqs - ackedReqs + RB <= window)
            {
                foreach (k; 0 .. RB)
                {
                    auto dst = burst[k * reqBytes.length .. (k + 1) * reqBytes.length];
                    dst[] = reqBytes[];
                    immutable c2 = corr++;
                    dst[corrAt] = cast(ubyte)(c2 >> 24);
                    dst[corrAt + 1] = cast(ubyte)(c2 >> 16);
                    dst[corrAt + 2] = cast(ubyte)(c2 >> 8);
                    dst[corrAt + 3] = cast(ubyte)(c2 & 0xFF);
                }
                auto nn = gs.send(burst);
                assert(nn == burst.length, "short send");
                sentReqs += RB;
            }
            // drain EVERY complete response in the buffer, not one per recv
            auto got = gs.receive(rbuf[rlen .. $]);
            assert(got > 0, "broker closed");
            rlen += got;
            size_t pos = 0;
            while (rlen - pos >= 4)
            {
                immutable sz = rdU32(rbuf[], pos);
                if (rlen - pos < 4 + sz)
                    break;
                pos += 4 + sz;
                ackedReqs++;
            }
            if (pos > 0)
            {
                foreach (i; 0 .. rlen - pos)
                    rbuf[i] = rbuf[pos + i];
                rlen -= pos;
            }
        }
        auto ms = sw.peek.total!"msecs";
        immutable msgs = totalReqs * B;
        printf("kafka pub acked: %lld msgs in %lld ms = %lld msg/s\n", msgs, ms,
                ms ? msgs * 1000 / ms : 0);
    }
    else // sub: fetch loop from offset 0 on partition 0
    {
        long off = 0;
        long seen = 0;
        // prebuild the fetch request; patch the offset (i64) in place
        ubyte[] fb;
        putI32(fb, -1);
        putI32(fb, 100);
        putI32(fb, 1);
        putI32(fb, 8 * 1024 * 1024);
        putI32(fb, 1);
        putStr(fb, topic);
        putI32(fb, 1);
        putI32(fb, 0);
        immutable offAtBody = fb.length;
        putI64(fb, 0);
        putI32(fb, 1024 * 1024);
        auto freq = request(1, 3, 7, fb);
        immutable offAt = freq.length - fb.length + offAtBody;
        auto sw = StopWatch(AutoStart.yes);
        while (seen < n)
        {
            foreach (k; 0 .. 8)
                freq[offAt + k] = cast(ubyte)(cast(ulong) off >> ((7 - k) * 8));
            gs.send(freq);
            auto r = readResp();
            // walk: corr i32, throttle i32, topic_count i32, topic str,
            // part_count i32, part i32, err i16, hw i64, records i32+bytes
            size_t i = 4 + 4 + 4;
            immutable tl = (r[i] << 8) | r[i + 1];
            i += 2 + tl;
            i += 4; // parts count
            i += 4; // partition
            immutable err = (r[i] << 8) | r[i + 1];
            i += 2;
            i += 8; // hw
            immutable rsz = rdU32(r, i);
            i += 4;
            if (err != 0 && err != 1)
            {
                printf("fetch err=%d, first bytes:", cast(int) err);
                foreach (k; 0 .. (r.length < 48 ? r.length : 48))
                    printf(" %02x", r[k]);
                printf("\n");
                assert(false, "fetch error");
            }
            size_t end = i + rsz;
            long got = 0;
            while (i + 12 <= end)
            {
                immutable msz = rdU32(r, i + 8);
                if (i + 12 + msz > end)
                    break;
                got++;
                i += 12 + msz;
            }
            if (got == 0)
                continue; // poll again (message not yet visible)
            off += got;
            seen += got;
        }
        auto ms = sw.peek.total!"msecs";
        printf("kafka sub fetched: %lld msgs in %lld ms = %lld msg/s\n", seen, ms,
                ms ? seen * 1000 / ms : 0);
    }
    return 0;
}
