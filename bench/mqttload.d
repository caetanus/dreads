// mqttload — pipelined MQTT load generator (the redis-benchmark of the MQTT
// skin): the SAME client binary drives every broker, so comparisons are
// apples-to-apples. Raw blocking sockets, no external deps.
//
//   pub:  QoS-1 publisher with a sliding inflight window (default 256): the
//         broker must PROCESS AND ACK every message — no QoS-0 drop lies,
//         backpressure like redis-benchmark -P.
//   sub:  subscriber counting deliveries to N.
//
//   ldc2 -O2 bench/mqttload.d -of mqttload
//   ./mqttload pub  <host> <port> <topic> <nmsgs> [payloadLen] [window]
//   ./mqttload sub  <host> <port> <filter> <nmsgs> [durationMs]
//     with durationMs: SUSTAINED-rate mode — count deliveries for that long
//     starting at the FIRST delivery (drops upstream simply don't arrive);
//     without: count-to-N mode (burst drains report absurd rates — a windowed
//     run is the honest end-to-end number)
module mqttload;

import core.stdc.stdio : printf;
import std.socket : TcpSocket, InternetAddress, SocketOptionLevel, SocketOption;
import std.datetime.stopwatch : StopWatch, AutoStart;
import std.conv : to;

__gshared ubyte[1 << 20] rbuf;

void encodeVarint(ref ubyte[] o, uint v)
{
    do
    {
        ubyte b = v & 0x7F;
        v >>= 7;
        if (v)
            b |= 0x80;
        o ~= b;
    }
    while (v);
}

bool decodeVarint(const(ubyte)[] buf, ref size_t pos, out uint val)
{
    uint mul = 1;
    val = 0;
    foreach (k; 0 .. 4)
    {
        if (pos >= buf.length)
            return false;
        immutable b = buf[pos++];
        val += (b & 0x7F) * mul;
        if ((b & 0x80) == 0)
            return true;
        mul *= 128;
    }
    return false;
}

void putStr(ref ubyte[] o, const(char)[] s)
{
    o ~= cast(ubyte)(s.length >> 8);
    o ~= cast(ubyte)(s.length & 0xFF);
    o ~= cast(const(ubyte)[]) s;
}

TcpSocket dial(string host, ushort port)
{
    auto s = new TcpSocket(new InternetAddress(host, port));
    s.setOption(SocketOptionLevel.TCP, SocketOption.TCP_NODELAY, true);
    // CONNECT, clean session
    ubyte[] c;
    c ~= 0x10;
    ubyte[] vh;
    putStr(vh, "MQTT");
    vh ~= 4; // 3.1.1
    vh ~= 2; // clean session
    vh ~= 0;
    vh ~= 30; // keepalive
    putStr(vh, "mqttload");
    encodeVarint(c, cast(uint) vh.length);
    c ~= vh;
    s.send(c);
    ubyte[4] ack;
    size_t got = 0;
    while (got < 4)
    {
        auto n = s.receive(ack[got .. $]);
        assert(n > 0, "CONNACK read failed");
        got += n;
    }
    assert(ack[0] == 0x20 && ack[3] == 0, "CONNACK refused");
    return s;
}

int main(string[] args)
{
    if (args.length < 6)
    {
        printf("usage: mqttload pub|sub host port topic n [payloadLen] [window]\n");
        return 2;
    }
    auto mode = args[1];
    auto host = args[2];
    immutable port = args[3].to!ushort;
    auto topic = args[4];
    immutable n = args[5].to!long;
    auto s = dial(host, port);

    if (mode == "pub")
    {
        immutable plen = args.length > 6 ? args[6].to!size_t : 16;
        immutable window = args.length > 7 ? args[7].to!long : 256;
        auto payload = new char[plen];
        payload[] = 'x';
        // pre-build one PUBLISH QoS1 packet; patch the pid per send
        ubyte[] pkt;
        pkt ~= 0x32; // PUBLISH qos1
        ubyte[] vh;
        putStr(vh, topic);
        immutable pidOff0 = vh.length;
        vh ~= 0;
        vh ~= 1;
        vh ~= cast(const(ubyte)[]) payload;
        encodeVarint(pkt, cast(uint) vh.length);
        immutable pidOff = pkt.length + pidOff0;
        pkt ~= vh;
        // batch B packets per send() to amortize syscalls (like -P)
        enum B = 64;
        auto batch = new ubyte[pkt.length * B];
        foreach (i; 0 .. B)
            batch[i * pkt.length .. (i + 1) * pkt.length] = pkt[];

        long sent = 0, acked = 0;
        ushort pid = 1;
        auto sw = StopWatch(AutoStart.yes);
        size_t rlen = 0;
        while (acked < n)
        {
            // fill the window with whole batches
            while (sent < n && sent - acked + B <= window)
            {
                foreach (i; 0 .. B)
                {
                    batch[i * pkt.length + pidOff] = cast(ubyte)(pid >> 8);
                    batch[i * pkt.length + pidOff + 1] = cast(ubyte)(pid & 0xFF);
                    pid = pid == 0xFFFF ? 1 : cast(ushort)(pid + 1);
                }
                auto nn = s.send(batch);
                assert(nn == batch.length, "short send");
                sent += B;
            }
            // drain acks
            auto r = s.receive(rbuf[rlen .. $]);
            assert(r > 0, "broker closed");
            rlen += r;
            size_t pos = 0;
            while (pos < rlen)
            {
                size_t hp = pos + 1;
                uint rem;
                if (!decodeVarint(rbuf[0 .. rlen], hp, rem) || hp + rem > rlen)
                    break;
                if ((rbuf[pos] >> 4) == 4)
                    acked++;
                pos = hp + rem;
            }
            // compact
            if (pos > 0)
            {
                foreach (i; 0 .. rlen - pos)
                    rbuf[i] = rbuf[pos + i];
                rlen -= pos;
            }
        }
        auto ms = sw.peek.total!"msecs";
        printf("pub qos1 acked: %lld msgs in %lld ms = %lld msg/s\n", acked, ms,
                ms ? acked * 1000 / ms : 0);
    }
    else // sub
    {
        ubyte[] sub;
        sub ~= 0x82;
        ubyte[] vh;
        vh ~= 0;
        vh ~= 1; // pid
        putStr(vh, topic);
        vh ~= 0; // qos 0
        encodeVarint(sub, cast(uint) vh.length);
        sub ~= vh;
        s.send(sub);
        long seen = 0;
        size_t rlen = 0;
        bool started = false;
        immutable long durMs = args.length > 6 ? args[6].to!long : 0;
        StopWatch sw;
        while (seen < n)
        {
            if (durMs > 0 && started && sw.peek.total!"msecs" >= durMs)
                break;
            auto r = s.receive(rbuf[rlen .. $]);
            assert(r > 0, "broker closed");
            rlen += r;
            size_t pos = 0;
            while (pos < rlen)
            {
                size_t hp = pos + 1;
                uint rem;
                if (!decodeVarint(rbuf[0 .. rlen], hp, rem) || hp + rem > rlen)
                    break;
                if ((rbuf[pos] >> 4) == 3)
                {
                    // clock starts at the FIRST DELIVERY — the first bytes on
                    // this socket are the SUBACK, which arrives during the
                    // idle gap before the publisher even starts (that gap
                    // used to be silently counted into the E2E denominator)
                    if (!started)
                    {
                        sw = StopWatch(AutoStart.yes);
                        started = true;
                    }
                    seen++;
                }
                pos = hp + rem;
            }
            if (pos > 0)
            {
                foreach (i; 0 .. rlen - pos)
                    rbuf[i] = rbuf[pos + i];
                rlen -= pos;
            }
        }
        auto ms = sw.peek.total!"msecs";
        printf("sub delivered: %lld msgs in %lld ms = %lld msg/s\n", seen, ms,
                ms ? seen * 1000 / ms : 0);
    }
    return 0;
}
