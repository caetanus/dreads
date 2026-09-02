// amqpload — pipelined AMQP 0-9-1 load generator (the redis-benchmark of the
// AMQP skin): the SAME binary drives every broker. Publisher-confirm mode
// with a sliding inflight window — the broker must PROCESS AND ACK every
// message (backpressure, no fire-and-forget lies). Sub mode counts
// basic.deliver frames.
//
//   ldc2 -O2 bench/amqpload.d -of amqpload
//   ./amqpload pub host port queue nmsgs [payloadLen] [window]
//   ./amqpload sub host port queue nmsgs
module amqpload;

import core.stdc.stdio : printf;
import std.socket : TcpSocket, InternetAddress, SocketOptionLevel, SocketOption;
import std.datetime.stopwatch : StopWatch, AutoStart;
import std.conv : to;

__gshared ubyte[1 << 20] rbuf;
__gshared size_t rlen;
TcpSocket gs;

void putU16(ref ubyte[] o, ushort v) { o ~= cast(ubyte)(v >> 8); o ~= cast(ubyte)(v & 0xFF); }
void putU32(ref ubyte[] o, uint v)
{
    o ~= cast(ubyte)(v >> 24); o ~= cast(ubyte)(v >> 16);
    o ~= cast(ubyte)(v >> 8); o ~= cast(ubyte)(v & 0xFF);
}
void putU64(ref ubyte[] o, ulong v) { putU32(o, cast(uint)(v >> 32)); putU32(o, cast(uint) v); }
void putShort(ref ubyte[] o, const(char)[] s) { o ~= cast(ubyte) s.length; o ~= cast(const(ubyte)[]) s; }
void putLong(ref ubyte[] o, const(char)[] s) { putU32(o, cast(uint) s.length); o ~= cast(const(ubyte)[]) s; }

ubyte[] frame(ubyte type, ushort chan, const(ubyte)[] payload)
{
    ubyte[] f;
    f ~= type;
    putU16(f, chan);
    putU32(f, cast(uint) payload.length);
    f ~= payload;
    f ~= 0xCE;
    return f;
}

ubyte[] methodFrame(ushort chan, ushort cls, ushort mth, const(ubyte)[] args)
{
    ubyte[] p;
    putU16(p, cls);
    putU16(p, mth);
    p ~= args;
    return frame(1, chan, p);
}

// Read frames until we see (cls,mth) on any channel; other frames discarded.
void awaitMethod(ushort cls, ushort mth)
{
    for (;;)
    {
        size_t pos = 0;
        while (rlen - pos >= 8)
        {
            immutable fsize = (cast(uint) rbuf[pos + 3] << 24) | (cast(uint) rbuf[pos + 4] << 16)
                | (cast(uint) rbuf[pos + 5] << 8) | rbuf[pos + 6];
            if (rlen - pos < 7 + fsize + 1)
                break;
            if (rbuf[pos] == 1 && fsize >= 4)
            {
                immutable c2 = (rbuf[pos + 7] << 8) | rbuf[pos + 8];
                immutable m2 = (rbuf[pos + 9] << 8) | rbuf[pos + 10];
                if (c2 == cls && m2 == mth)
                {
                    // consume through this frame
                    immutable end = pos + 7 + fsize + 1;
                    foreach (i; 0 .. rlen - end)
                        rbuf[i] = rbuf[end + i];
                    rlen -= end;
                    return;
                }
            }
            pos += 7 + fsize + 1;
        }
        // drop fully-scanned non-target frames
        if (pos > 0)
        {
            foreach (i; 0 .. rlen - pos)
                rbuf[i] = rbuf[pos + i];
            rlen -= pos;
        }
        auto n = gs.receive(rbuf[rlen .. $]);
        assert(n > 0, "broker closed during handshake");
        rlen += n;
    }
}

void dial(string host, ushort port, const(char)[] queue)
{
    gs = new TcpSocket(new InternetAddress(host, port));
    gs.setOption(SocketOptionLevel.TCP, SocketOption.TCP_NODELAY, true);
    gs.send(cast(const(ubyte)[]) "AMQP\x00\x00\x09\x01");
    awaitMethod(10, 10); // Start
    {
        ubyte[] a;
        putU32(a, 0); // client-properties: empty table
        putShort(a, "PLAIN");
        putLong(a, "\x00guest\x00guest");
        putShort(a, "en_US");
        gs.send(methodFrame(0, 10, 11, a));
    }
    awaitMethod(10, 30); // Tune
    {
        ubyte[] a;
        putU16(a, 2047);
        putU32(a, 131072);
        putU16(a, 0);
        gs.send(methodFrame(0, 10, 31, a)); // TuneOk
        ubyte[] b;
        putShort(b, "/");
        putShort(b, "");
        b ~= 0;
        gs.send(methodFrame(0, 10, 40, b)); // Open
    }
    awaitMethod(10, 41); // OpenOk
    {
        ubyte[] a;
        putShort(a, ""); // reserved-1 is a SHORTSTR (rabbit rejects a longstr)
        gs.send(methodFrame(1, 20, 10, a)); // Channel.Open
    }
    awaitMethod(20, 11);
    {
        ubyte[] a; // Queue.Declare
        putU16(a, 0);
        putShort(a, queue);
        a ~= 0; // bits
        putU32(a, 0); // args
        gs.send(methodFrame(1, 50, 10, a));
    }
    awaitMethod(50, 11);
}

int main(string[] args)
{
    if (args.length < 6)
    {
        printf("usage: amqpload pub|sub|suback host port queue n [payloadLen] [window]\n");
        return 2;
    }
    auto mode = args[1];
    auto host = args[2];
    immutable port = args[3].to!ushort;
    auto queue = args[4];
    immutable n = args[5].to!long;
    dial(host, port, queue);

    if (mode == "pub")
    {
        immutable plen = args.length > 6 ? args[6].to!size_t : 16;
        immutable window = args.length > 7 ? args[7].to!long : 256;
        gs.send(methodFrame(1, 85, 10, [cast(ubyte) 0])); // confirm.select (nowait=0)
        awaitMethod(85, 11);
        // one publish = method + header + body frames, prebuilt
        auto payload = new ubyte[plen];
        payload[] = 'x';
        ubyte[] one;
        {
            ubyte[] a;
            putU16(a, 0);
            putShort(a, "");
            putShort(a, queue);
            a ~= 0;
            one ~= methodFrame(1, 60, 40, a);
            ubyte[] h;
            putU16(h, 60);
            putU16(h, 0);
            putU64(h, plen);
            putU16(h, 0);
            one ~= frame(2, 1, h);
            one ~= frame(3, 1, payload);
        }
        enum B = 32;
        ubyte[] batch;
        foreach (i; 0 .. B)
            batch ~= one;
        long sent = 0, acked = 0;
        auto sw = StopWatch(AutoStart.yes);
        while (acked < n)
        {
            while (sent < n && sent - acked + B <= window)
            {
                auto nn = gs.send(batch);
                assert(nn == batch.length, "short send");
                sent += B;
            }
            auto r = gs.receive(rbuf[rlen .. $]);
            assert(r > 0, "broker closed");
            rlen += r;
            size_t pos = 0;
            while (rlen - pos >= 8)
            {
                immutable fsize = (cast(uint) rbuf[pos + 3] << 24) | (cast(uint) rbuf[pos + 4] << 16)
                    | (cast(uint) rbuf[pos + 5] << 8) | rbuf[pos + 6];
                if (rlen - pos < 7 + fsize + 1)
                    break;
                if (rbuf[pos] == 1 && fsize >= 4)
                {
                    immutable c2 = (rbuf[pos + 7] << 8) | rbuf[pos + 8];
                    immutable m2 = (rbuf[pos + 9] << 8) | rbuf[pos + 10];
                    if (c2 == 60 && (m2 == 80 || m2 == 120)) // ack / nack
                    {
                        // multiple bit: last arg byte of the frame
                        immutable mult = rbuf[pos + 7 + fsize - 1] & 1;
                        if (mult)
                        {
                            ulong tag = 0;
                            foreach (k; 0 .. 8)
                                tag = (tag << 8) | rbuf[pos + 11 + k];
                            acked = cast(long) tag;
                        }
                        else
                            acked++;
                    }
                }
                pos += 7 + fsize + 1;
            }
            if (pos > 0)
            {
                foreach (i; 0 .. rlen - pos)
                    rbuf[i] = rbuf[pos + i];
                rlen -= pos;
            }
        }
        auto ms = sw.peek.total!"msecs";
        printf("amqp pub confirmed: %lld msgs in %lld ms = %lld msg/s\n", acked, ms,
                ms ? acked * 1000 / ms : 0);
    }
    else // sub / suback: count deliver METHOD frames
    {
        // `suback` consumes with no-ack=FALSE and sends one basic.ack per
        // delivery, so the unacked window and the settle path are exercised —
        // the shape PerfTest drives. `sub` (no-ack=true) measures delivery
        // alone. Keep both: a scaling claim made with one does not carry to the
        // other, and conflating them is how a broker gets credited with
        // throughput on a path it never ran.
        immutable doAck = mode == "suback";
        ubyte[] a;
        putU16(a, 0);
        putShort(a, queue);
        putShort(a, "lt");
        a ~= doAck ? 0 : 2; // bit 1 = no-ack
        putU32(a, 0);
        gs.send(methodFrame(1, 60, 20, a));
        awaitMethod(60, 21);
        long seen = 0;
        ubyte[] ackBuf;
        bool started = false;
        StopWatch sw;
        while (seen < n)
        {
            auto r = gs.receive(rbuf[rlen .. $]);
            assert(r > 0, "broker closed");
            if (!started)
            {
                sw = StopWatch(AutoStart.yes);
                started = true;
            }
            rlen += r;
            size_t pos = 0;
            while (rlen - pos >= 8)
            {
                immutable fsize = (cast(uint) rbuf[pos + 3] << 24) | (cast(uint) rbuf[pos + 4] << 16)
                    | (cast(uint) rbuf[pos + 5] << 8) | rbuf[pos + 6];
                if (rlen - pos < 7 + fsize + 1)
                    break;
                if (rbuf[pos] == 1 && fsize >= 4)
                {
                    immutable c2 = (rbuf[pos + 7] << 8) | rbuf[pos + 8];
                    immutable m2 = (rbuf[pos + 9] << 8) | rbuf[pos + 10];
                    if (c2 == 60 && m2 == 60)
                    {
                        seen++;
                        if (doAck)
                        {
                            // basic.deliver args: consumer-tag shortstr, then
                            // the u64 delivery-tag we have to settle.
                            size_t ap = pos + 11;
                            if (ap < rlen)
                            {
                                ap += 1 + rbuf[ap]; // skip consumer-tag
                                if (ap + 8 <= pos + 7 + fsize)
                                {
                                    ulong tag = 0;
                                    foreach (k; 0 .. 8)
                                        tag = (tag << 8) | rbuf[ap + k];
                                    ubyte[] ak;
                                    putU64(ak, tag);
                                    ak ~= 0; // multiple = 0: settle one by one
                                    ackBuf ~= methodFrame(1, 60, 80, ak);
                                }
                            }
                        }
                    }
                }
                pos += 7 + fsize + 1;
            }
            if (ackBuf.length)
            {
                gs.send(ackBuf); // one write per read: TCP coalesces the batch
                ackBuf.length = 0;
                ackBuf.assumeSafeAppend();
            }
            if (pos > 0)
            {
                foreach (i; 0 .. rlen - pos)
                    rbuf[i] = rbuf[pos + i];
                rlen -= pos;
            }
        }
        auto ms = sw.peek.total!"msecs";
        printf("amqp %s delivered: %lld msgs in %lld ms = %lld msg/s\n",
                doAck ? "suback".ptr : "sub".ptr, seen, ms,
                ms ? seen * 1000 / ms : 0);
    }
    return 0;
}
