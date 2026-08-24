/// AMQP 1.0 skin — M1: protocol-header dispatch, SASL layer, type-system
/// codec, open/close with idle-timeout heartbeats. (AMQP10-PLAN.md; the wire
/// protocol shares NOTHING with 0-9-1 beyond the port — links/sessions land
/// in M2/M3.)
///
/// EXTEND-only: this module owns the 1.0 conversation; dreads.amqp merely
/// dispatches on the 8-byte protocol header. Everything here follows the
/// hard-won 0-9-1 rules: stack-copy across yields, half-interval heartbeats,
/// monotonic clocks for dead-peer, hold replies until their flush.
module dreads.amqp10;

import vibe.core.net : TCPConnection;
import vibe.core.core : runTask, sleep;
import vibe.core.sync : TaskMutex;
import core.time : msecs;
import dreads.mem : ByteBuffer;

// ---------------------------------------------------------------------------
// Wire constants

/// Protocol headers ("AMQP" + protocol-id + 1.0.0)
package immutable ubyte[8] AMQP10_HDR_BARE = ['A', 'M', 'Q', 'P', 0, 1, 0, 0];
package immutable ubyte[8] AMQP10_HDR_SASL = ['A', 'M', 'Q', 'P', 3, 1, 0, 0];

enum ubyte FRAME_TYPE_AMQP = 0;
enum ubyte FRAME_TYPE_SASL = 1;

// performative descriptors (smallulong codes)
enum ulong PERF_OPEN = 0x10;
enum ulong PERF_BEGIN = 0x11;
enum ulong PERF_ATTACH = 0x12;
enum ulong PERF_FLOW = 0x13;
enum ulong PERF_TRANSFER = 0x14;
enum ulong PERF_DISPOSITION = 0x15;
enum ulong PERF_DETACH = 0x16;
enum ulong PERF_END = 0x17;
enum ulong PERF_CLOSE = 0x18;
enum ulong SASL_MECHANISMS = 0x40;
enum ulong SASL_INIT = 0x41;
enum ulong SASL_OUTCOME = 0x44;

// ---------------------------------------------------------------------------
// Type-system DECODER. AMQP 1.0 values are (constructor, data); performative
// bodies are described lists. The decoder is a cursor over one frame body.

struct A10Val
{
    enum Kind : ubyte
    {
        null_,
        boolean,
        u64, // all unsigned ints normalize here
        i64, // all signed ints
        f64,
        str, // string/symbol/binary share the slice
        list, // fields: byte range of the CONTENTS + count
        map,
        array,
        described // descriptor consumed; value follows at `i`
    }

    Kind kind;
    bool b;
    ulong u;
    long i;
    double f;
    const(ubyte)[] bytes; // str/sym/bin content, or list/map/array contents
    uint count; // list/map/array element count
}

struct A10Dec
{
    const(ubyte)[] p;
    size_t i;
    bool ok = true;

    private ubyte u8() @nogc nothrow
    {
        if (i >= p.length)
        {
            ok = false;
            return 0;
        }
        return p[i++];
    }

    private ulong be(size_t n) @nogc nothrow
    {
        if (i + n > p.length)
        {
            ok = false;
            return 0;
        }
        ulong v = 0;
        foreach (k; 0 .. n)
            v = (v << 8) | p[i + k];
        i += n;
        return v;
    }

    private const(ubyte)[] take(size_t n) @nogc nothrow
    {
        if (i + n > p.length)
        {
            ok = false;
            return null;
        }
        auto s = p[i .. i + n];
        i += n;
        return s;
    }

    /// Decode ONE value at the cursor (descriptors are consumed and reported
    /// as Kind.described with `u` = descriptor code when it is a ulong; the
    /// caller then reads the described value with another readValue()).
    A10Val readValue() @nogc nothrow
    {
        A10Val v;
        immutable c = u8();
        if (!ok)
            return v;
        switch (c)
        {
        case 0x00: // described: descriptor value, then the value itself
            {
                auto d = readValue();
                v.kind = A10Val.Kind.described;
                v.u = d.kind == A10Val.Kind.u64 ? d.u : ulong.max;
                return v;
            }
        case 0x40:
            v.kind = A10Val.Kind.null_;
            return v;
        case 0x41:
            v.kind = A10Val.Kind.boolean;
            v.b = true;
            return v;
        case 0x42:
            v.kind = A10Val.Kind.boolean;
            v.b = false;
            return v;
        case 0x56:
            v.kind = A10Val.Kind.boolean;
            v.b = u8() != 0;
            return v;
        case 0x43: // uint0
        case 0x44: // ulong0
            v.kind = A10Val.Kind.u64;
            v.u = 0;
            return v;
        case 0x50: // ubyte
        case 0x52: // smalluint
        case 0x53: // smallulong
            v.kind = A10Val.Kind.u64;
            v.u = u8();
            return v;
        case 0x60: // ushort
            v.kind = A10Val.Kind.u64;
            v.u = be(2);
            return v;
        case 0x70: // uint
            v.kind = A10Val.Kind.u64;
            v.u = be(4);
            return v;
        case 0x80: // ulong
            v.kind = A10Val.Kind.u64;
            v.u = be(8);
            return v;
        case 0x51: // byte
        case 0x55: // smalllong
        case 0x54: // smallint
            v.kind = A10Val.Kind.i64;
            v.i = cast(byte) u8();
            return v;
        case 0x61: // short
            v.kind = A10Val.Kind.i64;
            v.i = cast(short) be(2);
            return v;
        case 0x71: // int
            v.kind = A10Val.Kind.i64;
            v.i = cast(int) be(4);
            return v;
        case 0x81: // long
        case 0x83: // timestamp (ms since epoch)
            v.kind = A10Val.Kind.i64;
            v.i = cast(long) be(8);
            return v;
        case 0x72: // float
            {
                v.kind = A10Val.Kind.f64;
                uint raw = cast(uint) be(4);
                v.f = *cast(float*)&raw;
                return v;
            }
        case 0x82: // double
            {
                v.kind = A10Val.Kind.f64;
                ulong raw = be(8);
                v.f = *cast(double*)&raw;
                return v;
            }
        case 0x98: // uuid
            v.kind = A10Val.Kind.str;
            v.bytes = take(16);
            return v;
        case 0xA0: // vbin8
        case 0xA1: // str8
        case 0xA3: // sym8
            v.kind = A10Val.Kind.str;
            v.bytes = take(u8());
            return v;
        case 0xB0: // vbin32
        case 0xB1: // str32
        case 0xB3: // sym32
            v.kind = A10Val.Kind.str;
            v.bytes = take(cast(size_t) be(4));
            return v;
        case 0x45: // list0
            v.kind = A10Val.Kind.list;
            v.count = 0;
            return v;
        case 0xC0: // list8
        case 0xC1: // map8
            {
                immutable sz = u8();
                if (!ok || sz < 1)
                {
                    ok = false;
                    return v;
                }
                immutable n = u8();
                v.kind = c == 0xC0 ? A10Val.Kind.list : A10Val.Kind.map;
                v.count = n;
                v.bytes = take(sz - 1);
                return v;
            }
        case 0xD0: // list32
        case 0xD1: // map32
            {
                immutable sz = cast(size_t) be(4);
                if (!ok || sz < 4)
                {
                    ok = false;
                    return v;
                }
                immutable n = cast(uint) be(4);
                v.kind = c == 0xD0 ? A10Val.Kind.list : A10Val.Kind.map;
                v.count = n;
                v.bytes = take(sz - 4);
                return v;
            }
        case 0xE0: // array8
            {
                immutable sz = u8();
                if (!ok || sz < 1)
                {
                    ok = false;
                    return v;
                }
                immutable n = u8();
                v.kind = A10Val.Kind.array;
                v.count = n;
                v.bytes = take(sz - 1);
                return v;
            }
        case 0xF0: // array32
            {
                immutable sz = cast(size_t) be(4);
                if (!ok || sz < 4)
                {
                    ok = false;
                    return v;
                }
                immutable n = cast(uint) be(4);
                v.kind = A10Val.Kind.array;
                v.count = n;
                v.bytes = take(sz - 4);
                return v;
            }
        default:
            ok = false;
            return v;
        }
    }

    /// After a described SECTION's descriptor was consumed, skip its value.
    void skipValue2(ulong code) @nogc nothrow
    {
        cast(void) code;
        cast(void) readValue();
    }

    /// Skip one COMPLETE value: a described constructor is TWO reads
    /// (descriptor + value) — rhea's attach taught us the hard way (its
    /// source/target fields are described lists; a one-read skip left the
    /// cursor inside them and every later field misparsed).
    void skipValue() @nogc nothrow
    {
        auto v = readValue();
        if (v.kind == A10Val.Kind.described)
            cast(void) readValue();
    }
}

/// Decode a performative frame body: 0x00 descriptor(list...). Returns the
/// descriptor code (ulong.max on malformed) and leaves `fields` as a decoder
/// positioned over the LIST CONTENTS (count in fieldCount).
package ulong a10Performative(scope const(ubyte)[] body_, out A10Dec fields,
        out uint fieldCount) @nogc nothrow
{
    auto d = A10Dec(body_);
    auto head = d.readValue();
    if (!d.ok || head.kind != A10Val.Kind.described || head.u == ulong.max)
        return ulong.max;
    auto lst = d.readValue();
    if (!d.ok || lst.kind != A10Val.Kind.list)
        return ulong.max;
    fields = A10Dec(lst.bytes);
    fieldCount = lst.count;
    return head.u;
}

// ---------------------------------------------------------------------------
// ENCODER helpers (mirror amqp.d's putU32/patch pattern)

package void a10PutU32(ref ByteBuffer o, uint v) @nogc nothrow
{
    o.appendByte(cast(char)(v >> 24));
    o.appendByte(cast(char)(v >> 16));
    o.appendByte(cast(char)(v >> 8));
    o.appendByte(cast(char)(v & 0xFF));
}

package void a10PatchU32(ref ByteBuffer o, size_t at, uint v) @nogc nothrow @trusted
{
    auto d = cast(ubyte[]) o.data;
    if (at + 4 <= d.length)
    {
        d[at] = cast(ubyte)(v >> 24);
        d[at + 1] = cast(ubyte)(v >> 16);
        d[at + 2] = cast(ubyte)(v >> 8);
        d[at + 3] = cast(ubyte)(v & 0xFF);
    }
}

package void a10Null(ref ByteBuffer o) @nogc nothrow
{
    o.appendByte(0x40);
}

package void a10Bool(ref ByteBuffer o, bool v) @nogc nothrow
{
    o.appendByte(v ? 0x41 : 0x42);
}

package void a10UInt(ref ByteBuffer o, ulong v) @nogc nothrow
{
    if (v == 0)
        o.appendByte(0x43);
    else if (v <= 255)
    {
        o.appendByte(0x52);
        o.appendByte(cast(char) v);
    }
    else
    {
        o.appendByte(0x70);
        a10PutU32(o, cast(uint) v);
    }
}

package void a10UShort(ref ByteBuffer o, ushort v) @nogc nothrow
{
    o.appendByte(0x60);
    o.appendByte(cast(char)(v >> 8));
    o.appendByte(cast(char)(v & 0xFF));
}

package void a10SmallUlong(ref ByteBuffer o, ubyte v) @nogc nothrow
{
    o.appendByte(0x53);
    o.appendByte(cast(char) v);
}

package void a10Str(ref ByteBuffer o, scope const(char)[] s) @nogc nothrow
{
    if (s.length <= 255)
    {
        o.appendByte(0xA1);
        o.appendByte(cast(char) s.length);
    }
    else
    {
        o.appendByte(0xB1);
        a10PutU32(o, cast(uint) s.length);
    }
    o.append(s);
}

package void a10Sym(ref ByteBuffer o, scope const(char)[] s) @nogc nothrow
{
    if (s.length <= 255)
    {
        o.appendByte(0xA3);
        o.appendByte(cast(char) s.length);
    }
    else
    {
        o.appendByte(0xB3);
        a10PutU32(o, cast(uint) s.length);
    }
    o.append(s);
}

package void a10Bin(ref ByteBuffer o, scope const(ubyte)[] b) @nogc nothrow
{
    if (b.length <= 255)
    {
        o.appendByte(0xA0);
        o.appendByte(cast(char) b.length);
    }
    else
    {
        o.appendByte(0xB0);
        a10PutU32(o, cast(uint) b.length);
    }
    o.append(cast(const(char)[]) b);
}

/// Open a described list32 (descriptor = smallulong `code`); returns the
/// patch cookie for a10CloseList.
package struct A10List
{
    size_t sizeAt;
    size_t countAt;
    size_t start;
    uint n;
}

package A10List a10OpenPerf(ref ByteBuffer o, ubyte code) @nogc nothrow
{
    o.appendByte(0x00); // described
    a10SmallUlong(o, code);
    o.appendByte(0xD0); // list32
    A10List l;
    l.sizeAt = o.length;
    a10PutU32(o, 0);
    l.countAt = o.length;
    a10PutU32(o, 0);
    l.start = l.countAt; // size counts from AFTER the size field (count incl.)
    return l;
}

package void a10Close(ref ByteBuffer o, ref A10List l) @nogc nothrow
{
    // list32 size = bytes AFTER the size field (count + items) — the codec
    // unittest caught a +4 double-count the smoke client didn't validate
    a10PatchU32(o, l.sizeAt, cast(uint)(o.length - l.countAt));
    a10PatchU32(o, l.countAt, l.n);
}

/// Wrap a staged frame BODY into an AMQP 1.0 frame: size(4) doff(1)=2
/// type(1) channel(2). `bodyStart` = offset where the body began in `o`.
package void a10FrameFinish(ref ByteBuffer o, size_t frameStart) @nogc nothrow
{
    a10PatchU32(o, frameStart, cast(uint)(o.length - frameStart));
}

package size_t a10FrameStart(ref ByteBuffer o, ubyte type, ushort channel) @nogc nothrow
{
    immutable at = o.length;
    a10PutU32(o, 0); // size, patched by a10FrameFinish
    o.appendByte(2); // doff
    o.appendByte(cast(char) type);
    o.appendByte(cast(char)(channel >> 8));
    o.appendByte(cast(char)(channel & 0xFF));
    return at;
}

// message-section descriptors
enum ulong SEC_HEADER = 0x70;
enum ulong SEC_DELIVERY_ANN = 0x71;
enum ulong SEC_MESSAGE_ANN = 0x72;
enum ulong SEC_PROPERTIES = 0x73;
enum ulong SEC_APP_PROPERTIES = 0x74;
enum ulong SEC_DATA = 0x75;
enum ulong SEC_AMQP_SEQUENCE = 0x76;
enum ulong SEC_AMQP_VALUE = 0x77;
enum ulong SEC_FOOTER = 0x78;
enum ulong STATE_ACCEPTED = 0x24;
enum ulong DESC_ERROR = 0x1D;

// ---------------------------------------------------------------------------
// Connection state

private struct A10Link
{
    string name;
    uint handle; // client's handle
    bool clientSender; // role=false in the client's attach: it SENDS to us
    string exchange; // resolved target ("" = default exchange)
    string rkey; // resolved routing key (queue name for direct addresses)
    ulong deliveryCount; // sender's count at attach + settled transfers
    uint creditGranted;
    ubyte[] pending; // more=true transfer fragments accumulate here (GC array)
    bool pendingActive;
    ulong pendingDeliveryId;
    bool pendingSettled;
    // --- client-RECEIVER links (we send): ---
    uint outCredit; // link-credit the client granted us via flow
    bool drain;
    bool detached; // stops the delivery fiber
    bool fiberLive;
    bool isMgmt; // "/management" pseudo-node (HTTP-over-AMQP topology ops)
}

/// One in-flight (unsettled) delivery we sent on a receiver link.
private struct A10Out
{
    string queue;
    immutable(ubyte)[] blob;
    uint handle;
}

private struct A10Session
{
    ushort remoteCh; // the client's channel for this session
    uint nextIncomingId; // next transfer-id we expect
    uint incomingWindow = 2048;
    uint nextOutgoingId; // delivery-id source for OUR transfers
    uint mgmtRecvHandle = uint.max; // the client's management RECEIVER link
    A10Link[uint] links; // keyed by the client's handle
    A10Out[ulong] unsettled; // delivery-id -> in-flight delivery (we sent)
}

private final class A10Conn
{
    TCPConnection tcp;
    bool closing;
    uint peerIdleMs; // peer's open.idle-time-out: we SEND empties at half it
    long lastReadMs; // MonoTime ms (dead-peer, 0-9-1 lesson: never gClock)
    bool hbStarted;
    A10Session[ushort] sessions; // keyed by the CLIENT channel
    TaskMutex wlock; // two writers (read-loop replies + delivery fibers)

    this(TCPConnection c) nothrow
    {
        tcp = c;
        try
            wlock = new TaskMutex;
        catch (Exception)
            assert(false);
    }
}

private void a10TeardownRequeue(A10Conn c) nothrow
{
    try
        foreach (ch6, ref sess; c.sessions)
            a10RequeueUnsettled(c, ch6, uint.max);
    catch (Exception)
    {
    }
}

private void closeQuiet(TCPConnection tcp) nothrow
{
    try
        tcp.close();
    catch (Exception)
    {
    }
}

enum uint A10_OUR_IDLE_MS = 30_000; // what we advertise in open
enum uint A10_MAX_FRAME = 1 << 20;

private long monoMs10() nothrow @trusted
{
    import core.time : MonoTime;

    try
        return MonoTime.currTime.ticks / (MonoTime.ticksPerSecond / 1000);
    catch (Exception)
        return 0;
}

private void a10Send(A10Conn c, scope const(ubyte)[] bytes) nothrow
{
    try
    {
        c.wlock.lock();
        scope (exit)
            c.wlock.unlock();
        c.tcp.write(bytes);
    }
    catch (Exception)
        c.closing = true;
}

// ---------------------------------------------------------------------------
// SASL layer

/// PLAIN initial-response: authzid NUL authcid NUL passwd (same as 0-9-1).
private bool a10SaslCheck(scope const(ubyte)[] mech, scope const(ubyte)[] resp) nothrow @trusted
{
    import dreads.acl : aclUser, aclCheckPassword, aclUserCount;

    if (aclUserCount() <= 1)
        return true; // legacy accept-any (same gate as every other skin)
    if (mech == cast(const(ubyte)[]) "ANONYMOUS")
        return false; // ACL configured: anonymous is refused
    const(char)[] auser, apass;
    auto sr = cast(const(char)[]) resp;
    size_t z1 = sr.length, z2 = sr.length;
    foreach (k, ch2; sr)
        if (ch2 == '\0')
        {
            if (z1 == sr.length)
                z1 = k;
            else
            {
                z2 = k;
                break;
            }
        }
    if (z1 < sr.length && z2 < sr.length)
    {
        auser = sr[z1 + 1 .. z2];
        apass = sr[z2 + 1 .. $];
    }
    auto au = aclUser(auser.length ? auser : "default");
    return au !is null && au.enabled && aclCheckPassword(au, apass);
}

// ---------------------------------------------------------------------------
// Serve loop

/// Entry point from dreads.amqp's header dispatch. `saslLayer` = the client
/// sent the SASL header (protocol-id 3); bare (0) skips straight to open.
/// The dispatching caller has ALREADY consumed the 8-byte header.
public void amqp10Serve(TCPConnection tcp, bool saslLayer) nothrow
{
    auto c = new A10Conn(tcp);
    scope (exit)
    {
        c.closing = true;
        a10TeardownRequeue(c);
        closeQuiet(tcp);
    }

    ByteBuffer outb;
    if (saslLayer)
    {
        // echo the SASL header, advertise mechanisms, run one init/outcome
        a10Send(c, AMQP10_HDR_SASL[]);
        outb.clear();
        {
            auto f = a10FrameStart(outb, FRAME_TYPE_SASL, 0);
            auto l = a10OpenPerf(outb, cast(ubyte) SASL_MECHANISMS);
            // sasl-server-mechanisms: array of symbols
            outb.appendByte(0xE0); // array8
            immutable szAt = outb.length;
            outb.appendByte(0); // size (patched by hand below)
            outb.appendByte(2); // count
            outb.appendByte(0xA3); // element constructor: sym8
            outb.appendByte(5);
            outb.append("PLAIN");
            outb.appendByte(9);
            outb.append("ANONYMOUS");
            {
                auto d = cast(ubyte[]) outb.data;
                d[szAt] = cast(ubyte)(outb.length - szAt - 1);
            }
            l.n = 1;
            a10Close(outb, l);
            a10FrameFinish(outb, f);
        }
        a10Send(c, cast(const(ubyte)[]) outb.data);
        // read frames until sasl-init
        bool authed = false;
        while (!c.closing && !authed)
        {
            const(ubyte)[] body_;
            ubyte ftype;
            ushort fchan;
            if (!a10ReadFrame(c, body_, ftype, fchan))
                return;
            if (body_.length == 0)
                continue; // keepalive
            A10Dec fields;
            uint nf;
            immutable code = a10Performative(body_, fields, nf);
            if (code != SASL_INIT)
                return; // protocol violation at the SASL layer: hang up
            auto mech = fields.readValue(); // symbol
            const(ubyte)[] resp;
            if (nf >= 2)
            {
                auto r2 = fields.readValue(); // initial-response (binary)
                if (r2.kind == A10Val.Kind.str)
                    resp = r2.bytes;
            }
            immutable pass = mech.kind == A10Val.Kind.str
                && a10SaslCheck(mech.bytes, resp);
            outb.clear();
            {
                auto f = a10FrameStart(outb, FRAME_TYPE_SASL, 0);
                auto l = a10OpenPerf(outb, cast(ubyte) SASL_OUTCOME);
                // sasl-code is a UBYTE (0x50): proton-j rejects smallulong here
                outb.appendByte(0x50);
                outb.appendByte(pass ? 0 : 1);
                l.n = 1;
                a10Close(outb, l);
                a10FrameFinish(outb, f);
            }
            a10Send(c, cast(const(ubyte)[]) outb.data);
            if (!pass)
                return;
            authed = true;
        }
        // post-SASL: both sides restate the BARE header
        ubyte[8] hdr2;
        if (!a10ReadExact(c, hdr2[]))
            return;
        if (hdr2 != AMQP10_HDR_BARE)
        {
            a10Send(c, AMQP10_HDR_BARE[]);
            return;
        }
        a10Send(c, AMQP10_HDR_BARE[]);
    }
    else
        a10Send(c, AMQP10_HDR_BARE[]);

    // ---- AMQP layer: expect open ----
    bool opened = false;
    c.lastReadMs = monoMs10();
    while (!c.closing)
    {
        const(ubyte)[] body_;
        ubyte ftype;
        ushort fchan;
        if (!a10ReadFrame(c, body_, ftype, fchan))
            return;
        c.lastReadMs = monoMs10();
        if (body_.length == 0)
            continue; // empty frame = keepalive
        A10Dec fields;
        uint nf;
        immutable code = a10Performative(body_, fields, nf);
        switch (code)
        {
        case PERF_OPEN:
            {
                // fields: container-id, hostname, max-frame-size, channel-max,
                // idle-time-out, ...
                if (nf >= 1)
                    fields.skipValue(); // container-id
                if (nf >= 2)
                    fields.skipValue(); // hostname
                if (nf >= 3)
                    fields.skipValue(); // max-frame-size
                if (nf >= 4)
                    fields.skipValue(); // channel-max
                if (nf >= 5)
                {
                    auto it = fields.readValue();
                    if (it.kind == A10Val.Kind.u64)
                        c.peerIdleMs = cast(uint) it.u;
                }
                outb.clear();
                {
                    auto f = a10FrameStart(outb, FRAME_TYPE_AMQP, 0);
                    auto l = a10OpenPerf(outb, cast(ubyte) PERF_OPEN);
                    a10Str(outb, "dreads");
                    l.n++;
                    a10Null(outb); // hostname
                    l.n++;
                    outb.appendByte(0x70); // max-frame-size uint
                    a10PutU32(outb, A10_MAX_FRAME);
                    l.n++;
                    a10UShort(outb, 1024); // channel-max
                    l.n++;
                    outb.appendByte(0x70); // idle-time-out
                    a10PutU32(outb, A10_OUR_IDLE_MS);
                    l.n++;
                    a10Null(outb); // outgoing-locales
                    l.n++;
                    a10Null(outb); // incoming-locales
                    l.n++;
                    a10Null(outb); // offered-capabilities
                    l.n++;
                    a10Null(outb); // desired-capabilities
                    l.n++;
                    {
                        // properties: the java 1.0 client version-gates its
                        // tests on these (same trick as 0-9-1 server-properties)
                        outb.appendByte(0xC1); // map8
                        immutable szAt2 = outb.length;
                        outb.appendByte(0);
                        outb.appendByte(6); // count
                        a10Sym(outb, "product");
                        a10Str(outb, "RabbitMQ");
                        a10Sym(outb, "version");
                        a10Str(outb, "4.1.0");
                        a10Sym(outb, "node");
                        a10Str(outb, "rabbit@dreads");
                        auto dd = cast(ubyte[]) outb.data;
                        dd[szAt2] = cast(ubyte)(outb.length - szAt2 - 1);
                    }
                    l.n++;
                    a10Close(outb, l);
                    a10FrameFinish(outb, f);
                }
                a10Send(c, cast(const(ubyte)[]) outb.data);
                opened = true;
                a10StartHeartbeat(c);
                break;
            }
        case PERF_CLOSE:
            {
                outb.clear();
                {
                    auto f = a10FrameStart(outb, FRAME_TYPE_AMQP, 0);
                    auto l = a10OpenPerf(outb, cast(ubyte) PERF_CLOSE);
                    a10Close(outb, l);
                    a10FrameFinish(outb, f);
                }
                a10Send(c, cast(const(ubyte)[]) outb.data);
                return;
            }
        case PERF_BEGIN:
            {
                if (!opened)
                    return;
                A10Session sess;
                sess.remoteCh = fchan;
                if (nf >= 2)
                {
                    fields.skipValue(); // remote-channel (null from initiator)
                    auto noid = fields.readValue(); // next-outgoing-id
                    if (noid.kind == A10Val.Kind.u64)
                        sess.nextIncomingId = cast(uint) noid.u;
                }
                try
                    c.sessions[fchan] = sess;
                catch (Exception)
                {
                }
                outb.clear();
                {
                    auto f = a10FrameStart(outb, FRAME_TYPE_AMQP, fchan);
                    auto l = a10OpenPerf(outb, cast(ubyte) PERF_BEGIN);
                    a10UShort(outb, fchan); // remote-channel = theirs
                    l.n++;
                    a10UInt(outb, 0); // next-outgoing-id
                    l.n++;
                    a10UInt(outb, 2048); // incoming-window
                    l.n++;
                    a10UInt(outb, 2048); // outgoing-window
                    l.n++;
                    a10Close(outb, l);
                    a10FrameFinish(outb, f);
                }
                a10Send(c, cast(const(ubyte)[]) outb.data);
                break;
            }
        case PERF_END:
            {
                a10RequeueUnsettled(c, fchan, uint.max);
                try
                    c.sessions.remove(fchan);
                catch (Exception)
                {
                }
                outb.clear();
                {
                    auto f = a10FrameStart(outb, FRAME_TYPE_AMQP, fchan);
                    auto l = a10OpenPerf(outb, cast(ubyte) PERF_END);
                    a10Close(outb, l);
                    a10FrameFinish(outb, f);
                }
                a10Send(c, cast(const(ubyte)[]) outb.data);
                break;
            }
        case PERF_ATTACH:
            a10HandleAttach(c, fchan, fields, nf, outb);
            break;
        case PERF_TRANSFER:
            a10HandleTransfer(c, fchan, fields, nf, body_, outb);
            break;
        case PERF_FLOW:
            {
                // fields: next-incoming-id(0) incoming-window(1)
                // next-outgoing-id(2) outgoing-window(3) handle(4)
                // delivery-count(5) link-credit(6) ... drain(9)
                if (auto psf = fchan in c.sessions)
                {
                    foreach (k4; 0 .. 4)
                        if (nf > k4)
                            fields.skipValue();
                    uint handle = uint.max;
                    if (nf >= 5)
                    {
                        auto h4 = fields.readValue();
                        if (h4.kind == A10Val.Kind.u64)
                            handle = cast(uint) h4.u;
                    }
                    if (nf >= 6)
                        fields.skipValue(); // delivery-count (their view)
                    uint credit = 0;
                    if (nf >= 7)
                    {
                        auto cr = fields.readValue();
                        if (cr.kind == A10Val.Kind.u64)
                            credit = cast(uint) cr.u;
                    }
                    if (nf >= 8)
                        fields.skipValue(); // available
                    bool drain = false;
                    if (nf >= 9)
                    {
                        auto dr = fields.readValue();
                        drain = dr.kind == A10Val.Kind.boolean && dr.b;
                    }
                    if (handle != uint.max)
                        if (auto plf = handle in psf.links)
                        {
                            plf.outCredit = credit;
                            plf.drain = drain;
                        }
                }
                break;
            }
        case PERF_DISPOSITION:
            a10HandleDisposition(c, fchan, fields, nf);
            break;
        case PERF_DETACH:
            {
                uint handle;
                if (nf >= 1)
                {
                    auto h2 = fields.readValue();
                    if (h2.kind == A10Val.Kind.u64)
                        handle = cast(uint) h2.u;
                }
                if (auto ps = fchan in c.sessions)
                {
                    if (auto pl4 = handle in ps.links)
                        pl4.detached = true; // delivery fiber exits on its next pass
                    a10RequeueUnsettled(c, fchan, handle);
                    ps.links.remove(handle);
                }
                outb.clear();
                {
                    auto f = a10FrameStart(outb, FRAME_TYPE_AMQP, fchan);
                    auto l = a10OpenPerf(outb, cast(ubyte) PERF_DETACH);
                    a10UInt(outb, handle);
                    l.n++;
                    a10Bool(outb, true); // closed
                    l.n++;
                    a10Close(outb, l);
                    a10FrameFinish(outb, f);
                }
                a10Send(c, cast(const(ubyte)[]) outb.data);
                break;
            }
        default:
            if (!opened)
                return;
            outb.clear();
            {
                auto f = a10FrameStart(outb, FRAME_TYPE_AMQP, 0);
                auto l = a10OpenPerf(outb, cast(ubyte) PERF_CLOSE);
                auto el = a10OpenPerf(outb, cast(ubyte) DESC_ERROR);
                a10Sym(outb, "amqp:not-implemented");
                el.n++;
                a10Str(outb, "M2 speaks open/begin/attach/transfer/detach/end/close");
                el.n++;
                a10Close(outb, el);
                l.n++;
                a10Close(outb, l);
                a10FrameFinish(outb, f);
            }
            a10Send(c, cast(const(ubyte)[]) outb.data);
            return;
        }
    }
}

/// attach: the client-sender case (role=false) — resolve the target address,
/// ensure the queue, echo the attach with our (receiver) role, grant credit.
private void a10HandleAttach(A10Conn c, ushort fchan, ref A10Dec fields,
        uint nf, ref ByteBuffer outb) nothrow
{
    import dreads.amqp : a10EnsureQueue;

    auto ps = fchan in c.sessions;
    if (ps is null)
        return;
    A10Link lk;
    // fields: name(0) handle(1) role(2) snd-settle(3) rcv-settle(4)
    //         source(5) target(6) unsettled(7) ... initial-delivery-count(9)
    if (nf >= 1)
    {
        auto nm = fields.readValue();
        if (nm.kind == A10Val.Kind.str)
            try
                lk.name = (cast(const(char)[]) nm.bytes).idup;
            catch (Exception)
            {
            }
    }
    if (nf >= 2)
    {
        auto h = fields.readValue();
        if (h.kind == A10Val.Kind.u64)
            lk.handle = cast(uint) h.u;
    }
    if (nf >= 3)
    {
        // role: ABSENT/null defaults to false = sender (rhea omits defaults)
        auto role = fields.readValue();
        lk.clientSender = !(role.kind == A10Val.Kind.boolean && role.b);
    }
    else
        lk.clientSender = true;
    if (nf >= 4)
        fields.skipValue(); // snd-settle-mode
    if (nf >= 5)
        fields.skipValue(); // rcv-settle-mode
    const(char)[] address;
    char[512] addrBuf = void;
    size_t addrLen = 0;
    // client-sender: the address is the TARGET (field 6); client-receiver:
    // it is the SOURCE (field 5). Both are described lists with the address
    // string at field 0.
    static bool grabAddr(ref A10Dec fs, ref char[512] buf, ref size_t blen) nothrow
    {
        auto tgt = fs.readValue();
        if (tgt.kind != A10Val.Kind.described)
            return false;
        auto inner = fs.readValue();
        if (inner.kind != A10Val.Kind.list || inner.count < 1)
            return false;
        auto td = A10Dec(inner.bytes);
        auto av = td.readValue();
        if (av.kind != A10Val.Kind.str || av.bytes.length > buf.length)
            return false;
        buf[0 .. av.bytes.length] = cast(const(char)[]) av.bytes;
        blen = av.bytes.length;
        return true;
    }

    if (nf >= 6)
    {
        if (!lk.clientSender && grabAddr(fields, addrBuf, addrLen))
            address = addrBuf[0 .. addrLen];
        else if (lk.clientSender)
            fields.skipValue(); // source unused for a client sender
    }
    if (nf >= 7 && lk.clientSender)
    {
        if (grabAddr(fields, addrBuf, addrLen))
            address = addrBuf[0 .. addrLen];
    }
    // resolve address -> (exchange, rkey): "/exchanges/X/RK" or "/exchange/X/RK"
    // routes through X; a plain name is a queue on the default exchange
    const(char)[] exch = "";
    const(char)[] rk = address;
    foreach (pfx; ["/exchanges/", "/exchange/"])
        if (address.length > pfx.length && address[0 .. pfx.length] == pfx)
        {
            auto rest = address[pfx.length .. $];
            size_t slash = rest.length;
            foreach (k, ch3; rest)
                if (ch3 == '/')
                {
                    slash = k;
                    break;
                }
            exch = rest[0 .. slash];
            rk = slash < rest.length ? rest[slash + 1 .. $] : "";
            break;
        }
    try
    {
        lk.exchange = exch.idup;
        lk.rkey = rk.idup;
    }
    catch (Exception)
    {
    }
    if (address == "/management")
    {
        lk.isMgmt = true;
        if (!lk.clientSender)
            ps.mgmtRecvHandle = lk.handle; // responses flow back on this link
    }
    else if (lk.exchange.length == 0 && lk.rkey.length)
        a10EnsureQueue(lk.rkey); // queue address: attach declares it
    try
        ps.links[lk.handle] = lk;
    catch (Exception)
    {
    }
    // reply attach (our role: receiver=true for a client sender, sender=false
    // for a client receiver — always the flip of theirs)
    outb.clear();
    {
        auto f = a10FrameStart(outb, FRAME_TYPE_AMQP, fchan);
        auto l = a10OpenPerf(outb, cast(ubyte) PERF_ATTACH);
        a10Str(outb, lk.name);
        l.n++;
        a10UInt(outb, lk.handle);
        l.n++;
        a10Bool(outb, lk.clientSender); // true=receiver iff the client sends
        l.n++;
        a10Null(outb); // snd-settle-mode
        l.n++;
        a10Null(outb); // rcv-settle-mode
        l.n++;
        if (lk.clientSender)
            a10Null(outb); // source
        else
        {
            auto sl = a10OpenPerf(outb, 0x28); // source (we deliver FROM it)
            a10Str(outb, address);
            sl.n++;
            a10Close(outb, sl);
        }
        l.n++;
        {
            auto tl = a10OpenPerf(outb, 0x29); // target
            if (lk.clientSender)
            {
                a10Str(outb, address);
                tl.n++;
            }
            a10Close(outb, tl);
        }
        l.n++;
        if (!lk.clientSender)
        {
            a10Null(outb); // unsettled
            l.n++;
            a10Null(outb); // incomplete-unsettled
            l.n++;
            a10UInt(outb, 0); // initial-delivery-count (we are the sender)
            l.n++;
        }
        a10Close(outb, l);
        a10FrameFinish(outb, f);
    }
    if (!lk.clientSender && !lk.isMgmt)
        a10StartDelivery(c, fchan, lk.handle);
    // grant link-credit to the client sender via flow
    if (lk.clientSender)
    {
        auto ps2 = fchan in c.sessions;
        auto f2 = a10FrameStart(outb, FRAME_TYPE_AMQP, fchan);
        auto l2 = a10OpenPerf(outb, cast(ubyte) PERF_FLOW);
        a10UInt(outb, ps2 !is null ? ps2.nextIncomingId : 0); // next-incoming-id
        l2.n++;
        a10UInt(outb, 2048); // incoming-window
        l2.n++;
        a10UInt(outb, 0); // next-outgoing-id
        l2.n++;
        a10UInt(outb, 2048); // outgoing-window
        l2.n++;
        a10UInt(outb, lk.handle); // handle
        l2.n++;
        a10UInt(outb, lk.deliveryCount); // delivery-count
        l2.n++;
        a10UInt(outb, 1000); // link-credit
        l2.n++;
        a10Close(outb, l2);
        a10FrameFinish(outb, f2);
        if (auto plk = lk.handle in ps.links)
            plk.creditGranted = 1000;
    }
    a10Send(c, cast(const(ubyte)[]) outb.data);
}

/// transfer: decode the bare message, map to a 0-9-1 record, route, settle.
private void a10HandleTransfer(A10Conn c, ushort fchan, ref A10Dec fields,
        uint nf, scope const(ubyte)[] body_, ref ByteBuffer outb) nothrow
{
    import dreads.amqp : a10Publish;

    auto ps = fchan in c.sessions;
    if (ps is null)
        return;
    uint handle;
    ulong deliveryId;
    bool settled = false;
    bool more = false;
    if (nf >= 1)
    {
        auto h = fields.readValue();
        if (h.kind == A10Val.Kind.u64)
            handle = cast(uint) h.u;
    }
    if (nf >= 2)
    {
        auto d2 = fields.readValue();
        if (d2.kind == A10Val.Kind.u64)
            deliveryId = d2.u;
    }
    if (nf >= 3)
        fields.skipValue(); // delivery-tag
    if (nf >= 4)
        fields.skipValue(); // message-format
    if (nf >= 5)
    {
        auto st = fields.readValue();
        settled = st.kind == A10Val.Kind.boolean && st.b;
    }
    if (nf >= 6)
    {
        auto mo = fields.readValue();
        more = mo.kind == A10Val.Kind.boolean && mo.b;
    }
    auto plk = handle in ps.links;
    if (plk is null)
        return;
    // the message payload = frame body AFTER the performative's list (the
    // fields decoder is a window over the list CONTENTS; its end marks where
    // the bare message begins)
    immutable msgOff = cast(size_t)(fields.p.ptr - body_.ptr) + fields.p.length;
    if (msgOff > body_.length)
        return;
    auto msg = body_[msgOff .. $];
    ps.nextIncomingId++;
    if (more || plk.pendingActive)
    {
        // accumulate fragments until more=false
        if (!plk.pendingActive)
        {
            plk.pendingActive = true;
            plk.pending = null;
            plk.pendingDeliveryId = deliveryId;
            plk.pendingSettled = settled;
        }
        try
            plk.pending ~= msg;
        catch (Exception)
        {
            plk.pendingActive = false;
            return;
        }
        if (more)
            return;
        msg = plk.pending;
        deliveryId = plk.pendingDeliveryId;
        settled = settled || plk.pendingSettled;
        plk.pendingActive = false;
    }
    plk.deliveryCount++;
    // decode sections -> 0-9-1 props + body
    if (plk.isMgmt)
        a10HandleMgmt(c, fchan, msg);
    else
    {
        static ByteBuffer props; // TLS: consumed by a10Publish before any yield
        static ByteBuffer bodyBuf; // TLS
        a10MapMessage(msg, props, bodyBuf);
        cast(void) a10Publish(plk.exchange, plk.rkey,
                cast(const(ubyte)[]) props.data, cast(const(ubyte)[]) bodyBuf.data);
    }
    // settle back (rcv-settle-mode first): disposition accepted+settled
    if (!settled)
    {
        outb.clear();
        auto f = a10FrameStart(outb, FRAME_TYPE_AMQP, fchan);
        auto l = a10OpenPerf(outb, cast(ubyte) PERF_DISPOSITION);
        a10Bool(outb, true); // role: receiver
        l.n++;
        a10UInt(outb, deliveryId); // first
        l.n++;
        a10UInt(outb, deliveryId); // last
        l.n++;
        a10Bool(outb, true); // settled
        l.n++;
        {
            auto st2 = a10OpenPerf(outb, cast(ubyte) STATE_ACCEPTED);
            a10Close(outb, st2);
        }
        l.n++;
        a10Close(outb, l);
        a10FrameFinish(outb, f);
        a10Send(c, cast(const(ubyte)[]) outb.data);
    }
    // replenish credit when half-consumed
    if (plk.creditGranted && plk.deliveryCount % 500 == 0)
    {
        outb.clear();
        auto f2 = a10FrameStart(outb, FRAME_TYPE_AMQP, fchan);
        auto l2 = a10OpenPerf(outb, cast(ubyte) PERF_FLOW);
        a10UInt(outb, ps.nextIncomingId);
        l2.n++;
        a10UInt(outb, 2048);
        l2.n++;
        a10UInt(outb, 0);
        l2.n++;
        a10UInt(outb, 2048);
        l2.n++;
        a10UInt(outb, handle);
        l2.n++;
        a10UInt(outb, plk.deliveryCount);
        l2.n++;
        a10UInt(outb, 1000);
        l2.n++;
        a10Close(outb, l2);
        a10FrameFinish(outb, f2);
        a10Send(c, cast(const(ubyte)[]) outb.data);
    }
}

/// Map a 1.0 bare message onto 0-9-1 (props table + body): header.durable ->
/// delivery-mode, header.priority/ttl, properties -> content-type/
/// correlation-id/reply-to, application-properties -> headers table,
/// data/amqp-value -> body. Sections we don't map are skipped whole.
private void a10MapMessage(scope const(ubyte)[] msg, ref ByteBuffer props,
        ref ByteBuffer bodyBuf) nothrow @trusted
{
    props.clear();
    bodyBuf.clear();
    ushort flags = 0;
    // staging for the fixed-order 0-9-1 property list
    const(char)[] contentType, correlationId, replyTo;
    char[24] expBuf = void;
    const(char)[] expiration;
    ubyte deliveryMode = 0, priority = 0;
    static ByteBuffer hdrTbl; // TLS: application-properties -> headers table
    hdrTbl.clear();

    auto d = A10Dec(msg);
    while (d.ok && d.i < d.p.length)
    {
        auto sec = d.readValue();
        if (!d.ok || sec.kind != A10Val.Kind.described)
            break;
        immutable code = sec.u;
        auto val = d.readValue();
        if (!d.ok)
            break;
        switch (code)
        {
        case SEC_HEADER:
            if (val.kind == A10Val.Kind.list)
            {
                auto hd = A10Dec(val.bytes);
                if (val.count >= 1)
                {
                    auto durable = hd.readValue();
                    if (durable.kind == A10Val.Kind.boolean && durable.b)
                        deliveryMode = 2;
                }
                if (val.count >= 2)
                {
                    auto prio = hd.readValue();
                    if (prio.kind == A10Val.Kind.u64)
                        priority = cast(ubyte) prio.u;
                }
                if (val.count >= 3)
                {
                    auto ttl = hd.readValue();
                    if (ttl.kind == A10Val.Kind.u64 && ttl.u > 0)
                    {
                        // decimal ms, 0-9-1 expiration-property style
                        size_t ep = expBuf.length;
                        ulong ev = ttl.u;
                        do
                        {
                            expBuf[--ep] = cast(char)('0' + ev % 10);
                            ev /= 10;
                        }
                        while (ev);
                        expiration = expBuf[ep .. $];
                    }
                }
            }
            break;
        case SEC_PROPERTIES:
            if (val.kind == A10Val.Kind.list)
            {
                auto pd = A10Dec(val.bytes);
                // message-id(0) user-id(1) to(2) subject(3) reply-to(4)
                // correlation-id(5) content-type(6) ...
                foreach (fi; 0 .. val.count)
                {
                    auto v2 = pd.readValue();
                    if (!pd.ok)
                        break;
                    if (fi == 4 && v2.kind == A10Val.Kind.str)
                        replyTo = cast(const(char)[]) v2.bytes;
                    else if (fi == 5 && v2.kind == A10Val.Kind.str)
                        correlationId = cast(const(char)[]) v2.bytes;
                    else if (fi == 6 && v2.kind == A10Val.Kind.str)
                        contentType = cast(const(char)[]) v2.bytes;
                }
            }
            break;
        case SEC_APP_PROPERTIES:
            if (val.kind == A10Val.Kind.map)
            {
                auto md = A10Dec(val.bytes);
                foreach (mi; 0 .. val.count / 2)
                {
                    auto k2 = md.readValue();
                    auto v2 = md.readValue();
                    if (!md.ok || k2.kind != A10Val.Kind.str || k2.bytes.length > 127)
                        break;
                    hdrTbl.appendByte(cast(char) k2.bytes.length);
                    hdrTbl.append(cast(const(char)[]) k2.bytes);
                    final switch (v2.kind)
                    {
                    case A10Val.Kind.str:
                        hdrTbl.appendByte('S');
                        a10PutU32(hdrTbl, cast(uint) v2.bytes.length);
                        hdrTbl.append(cast(const(char)[]) v2.bytes);
                        break;
                    case A10Val.Kind.u64:
                    case A10Val.Kind.i64:
                        hdrTbl.appendByte('l');
                        immutable lv = v2.kind == A10Val.Kind.u64
                            ? cast(long) v2.u : v2.i;
                        foreach (k3; 0 .. 8)
                            hdrTbl.appendByte(cast(char)(lv >> ((7 - k3) * 8)));
                        break;
                    case A10Val.Kind.boolean:
                        hdrTbl.appendByte('t');
                        hdrTbl.appendByte(v2.b ? 1 : 0);
                        break;
                    case A10Val.Kind.null_:
                        hdrTbl.appendByte('V');
                        break;
                    case A10Val.Kind.f64:
                    case A10Val.Kind.list:
                    case A10Val.Kind.map:
                    case A10Val.Kind.array:
                    case A10Val.Kind.described:
                        hdrTbl.appendByte('V'); // unmapped value kinds -> void
                        break;
                    }
                }
            }
            break;
        case SEC_DATA:
            if (val.kind == A10Val.Kind.str) // vbin slice
                bodyBuf.append(cast(const(char)[]) val.bytes);
            break;
        case SEC_AMQP_VALUE:
            if (val.kind == A10Val.Kind.str)
                bodyBuf.append(cast(const(char)[]) val.bytes);
            break;
        default:
            break; // delivery/message annotations, footer, sequence: skipped
        }
    }

    if (contentType.length)
        flags |= 0x8000;
    if (hdrTbl.length)
        flags |= 0x2000;
    if (deliveryMode)
        flags |= 0x1000;
    if (priority)
        flags |= 0x0800;
    if (correlationId.length)
        flags |= 0x0400;
    if (replyTo.length)
        flags |= 0x0200;
    if (expiration.length)
        flags |= 0x0100;
    props.appendByte(cast(char)(flags >> 8));
    props.appendByte(cast(char)(flags & 0xFF));
    if (flags & 0x8000)
    {
        props.appendByte(cast(char) contentType.length);
        props.append(contentType);
    }
    if (flags & 0x2000)
    {
        a10PutU32(props, cast(uint) hdrTbl.length);
        props.append(cast(const(char)[]) hdrTbl.data);
    }
    if (flags & 0x1000)
        props.appendByte(cast(char) deliveryMode);
    if (flags & 0x0800)
        props.appendByte(cast(char) priority);
    if (flags & 0x0400)
    {
        props.appendByte(cast(char) correlationId.length);
        props.append(correlationId);
    }
    if (flags & 0x0200)
    {
        props.appendByte(cast(char) replyTo.length);
        props.append(replyTo);
    }
    if (flags & 0x0100)
    {
        props.appendByte(cast(char) expiration.length);
        props.append(expiration);
    }
}

/// Requeue every unsettled delivery of `handle` (uint.max = all handles) on
/// session `fchan` — link detach, session end, connection teardown.
private void a10RequeueUnsettled(A10Conn c, ushort fchan, uint handle) nothrow
{
    import dreads.amqp : a10Requeue;

    auto ps = fchan in c.sessions;
    if (ps is null)
        return;
    try
    {
        ulong[] drop;
        foreach (id, ref o; ps.unsettled)
            if (handle == uint.max || o.handle == handle)
                drop ~= id;
        foreach (id; drop)
        {
            if (auto po = id in ps.unsettled)
                a10Requeue(po.queue, po.blob);
            ps.unsettled.remove(id);
        }
    }
    catch (Exception)
    {
    }
}

/// Client disposition over OUR deliveries: accepted settles, released
/// requeues, rejected dead-letters, modified requeues.
private void a10HandleDisposition(A10Conn c, ushort fchan, ref A10Dec fields,
        uint nf) nothrow
{
    import dreads.amqp : a10Requeue, a10Reject;

    auto ps = fchan in c.sessions;
    if (ps is null)
        return;
    // fields: role(0) first(1) last(2) settled(3) state(4)
    bool role;
    if (nf >= 1)
    {
        auto r = fields.readValue();
        role = r.kind == A10Val.Kind.boolean && r.b;
    }
    if (!role)
        return; // only receiver dispositions settle OUR sends
    ulong first, last;
    if (nf >= 2)
    {
        auto f2 = fields.readValue();
        if (f2.kind == A10Val.Kind.u64)
            first = f2.u;
    }
    last = first;
    if (nf >= 3)
    {
        auto l2 = fields.readValue();
        if (l2.kind == A10Val.Kind.u64)
            last = l2.u;
        // null last = just `first`
    }
    if (nf >= 4)
        fields.skipValue(); // settled
    ulong state = STATE_ACCEPTED;
    if (nf >= 5)
    {
        auto st = fields.readValue();
        if (st.kind == A10Val.Kind.described && st.u != ulong.max)
        {
            state = st.u;
            fields.skipValue(); // the state's (empty) list
        }
    }
    foreach (id; first .. last + 1)
    {
        auto po = id in ps.unsettled;
        if (po is null)
            continue;
        switch (state)
        {
        case 0x26: // released
        case 0x27: // modified
            a10Requeue(po.queue, po.blob);
            break;
        case 0x25: // rejected
            a10Reject(po.queue, po.blob);
            break;
        default: // accepted (0x24)
            break;
        }
        try
            ps.unsettled.remove(id);
        catch (Exception)
        {
        }
    }
}

// ---------------------------------------------------------------------------
// $management node (HTTP-over-AMQP, RabbitMQ 4.x AMQP 1.0 management API):
// requests arrive as messages with properties {message-id, to=/queues/...,
// subject=GET|PUT|POST|DELETE} + an amqp-value map body; responses echo the
// message-id as correlation-id with subject = the HTTP status code.

/// %XX-decode one path segment into `buf`; returns the decoded slice.
private const(char)[] a10UriDecode(scope const(char)[] src, return scope char[] buf) @nogc nothrow
{
    size_t o2 = 0;
    size_t i2 = 0;
    while (i2 < src.length && o2 < buf.length)
    {
        auto ch7 = src[i2];
        if (ch7 == '%' && i2 + 2 < src.length)
        {
            static int hex1(char h) @nogc nothrow
            {
                if (h >= '0' && h <= '9')
                    return h - '0';
                if (h >= 'a' && h <= 'f')
                    return h - 'a' + 10;
                if (h >= 'A' && h <= 'F')
                    return h - 'A' + 10;
                return -1;
            }

            immutable hi = hex1(src[i2 + 1]);
            immutable lo = hex1(src[i2 + 2]);
            if (hi >= 0 && lo >= 0)
            {
                buf[o2++] = cast(char)((hi << 4) | lo);
                i2 += 3;
                continue;
            }
        }
        buf[o2++] = cast(char) ch7;
        i2++;
    }
    return buf[0 .. o2];
}

/// Pull a value by key from an amqp-value MAP (str keys).
private A10Val a10MapGet(scope const(ubyte)[] mapBytes, uint count,
        scope const(char)[] key) @nogc nothrow
{
    A10Val none;
    auto md = A10Dec(mapBytes);
    foreach (mi; 0 .. count / 2)
    {
        auto k2 = md.readValue();
        auto v2 = md.readValue();
        if (!md.ok)
            break;
        if (k2.kind == A10Val.Kind.str && cast(const(char)[]) k2.bytes == key)
            return v2;
    }
    return none;
}

/// Send a management RESPONSE on the session's mgmt receiver link.
/// bodyKind: 0 = none, 1 = map (pre-encoded map WITH constructor), 2 = string.
private void a10MgmtRespond(A10Conn c, ushort fchan, scope const(char)[] code,
        scope const(ubyte)[] corrRaw, int bodyKind,
        scope const(ubyte)[] bodyMap, scope const(char)[] bodyStr) nothrow
{
    auto ps = fchan in c.sessions;
    if (ps is null || ps.mgmtRecvHandle == uint.max)
        return;
    ByteBuffer o;
    immutable did = ps.nextOutgoingId++;
    auto f = a10FrameStart(o, FRAME_TYPE_AMQP, fchan);
    auto l = a10OpenPerf(o, cast(ubyte) PERF_TRANSFER);
    a10UInt(o, ps.mgmtRecvHandle);
    l.n++;
    a10UInt(o, did);
    l.n++;
    {
        ubyte[4] dt = void;
        dt[0] = cast(ubyte)(did >> 24);
        dt[1] = cast(ubyte)(did >> 16);
        dt[2] = cast(ubyte)(did >> 8);
        dt[3] = cast(ubyte)(did & 0xFF);
        a10Bin(o, dt[]);
    }
    l.n++;
    a10UInt(o, 0); // message-format
    l.n++;
    a10Bool(o, true); // settled (mgmt responses are presettled)
    l.n++;
    a10Close(o, l);
    // properties: message-id(null) user-id(null) to(null) subject(code)
    //             reply-to(null) correlation-id(echo)
    {
        auto pl = a10OpenPerf(o, cast(ubyte) SEC_PROPERTIES);
        a10Null(o);
        pl.n++;
        a10Null(o);
        pl.n++;
        a10Null(o);
        pl.n++;
        a10Str(o, code);
        pl.n++;
        a10Null(o);
        pl.n++;
        if (corrRaw.length)
            o.append(cast(const(char)[]) corrRaw); // raw re-emit (any id type)
        else
            a10Null(o);
        pl.n++;
        a10Close(o, pl);
    }
    if (bodyKind == 1)
    {
        o.appendByte(0x00);
        a10SmallUlong(o, cast(ubyte) SEC_AMQP_VALUE);
        o.append(cast(const(char)[]) bodyMap); // pre-encoded map value
    }
    else if (bodyKind == 2)
    {
        o.appendByte(0x00);
        a10SmallUlong(o, cast(ubyte) SEC_AMQP_VALUE);
        a10Str(o, bodyStr);
    }
    a10FrameFinish(o, f);
    a10Send(c, cast(const(ubyte)[]) o.data);
}

/// Encode a queue-info map value (client's DefaultQueueInfo contract).
private void a10QueueInfoMap(ref ByteBuffer o, scope const(char)[] name,
        ubyte flags, long msgs) @nogc nothrow
{
    o.appendByte(0xD1); // map32
    immutable szAt = o.length;
    a10PutU32(o, 0);
    immutable cntAt = o.length;
    a10PutU32(o, 0);
    uint n2 = 0;
    a10Str(o, "name");
    a10Str(o, name);
    n2 += 2;
    a10Str(o, "durable");
    a10Bool(o, (flags & 2) != 0);
    n2 += 2;
    a10Str(o, "auto_delete");
    a10Bool(o, (flags & 8) != 0);
    n2 += 2;
    a10Str(o, "exclusive");
    a10Bool(o, (flags & 4) != 0);
    n2 += 2;
    a10Str(o, "type");
    a10Str(o, "classic");
    n2 += 2;
    a10Str(o, "arguments");
    o.appendByte(0xC1); // empty map8
    o.appendByte(1);
    o.appendByte(0);
    n2 += 2;
    a10Str(o, "leader");
    a10Str(o, "dreads-0");
    n2 += 2;
    a10Str(o, "message_count");
    o.appendByte(0x80);
    foreach (k; 0 .. 8)
        o.appendByte(cast(char)(cast(ulong) msgs >> ((7 - k) * 8)));
    n2 += 2;
    a10Str(o, "consumer_count");
    a10UInt(o, 0);
    n2 += 2;
    a10PatchU32(o, szAt, cast(uint)(o.length - cntAt));
    a10PatchU32(o, cntAt, n2);
}

/// One management request: parse, execute against the shared topology, reply.
private void a10HandleMgmt(A10Conn c, ushort fchan, scope const(ubyte)[] msg) nothrow
{
    import dreads.amqp : a10QueueExists, a10QueueLen, a10QueueFlags,
        a10DeclareQueue, a10DeleteQueue, a10PurgeQueue, a10ExchangeExists,
        a10DeclareExchange, a10DeleteExchange, a10Bind, a10Unbind;

    // walk the sections: properties (to/subject/message-id) + amqp-value body
    const(ubyte)[] corrRaw;
    const(char)[] to, subject;
    const(ubyte)[] bodyMapBytes;
    uint bodyMapCount;
    bool bodyIsMap = false;
    auto d = A10Dec(msg);
    while (d.ok && d.i < d.p.length)
    {
        auto sec = d.readValue();
        if (!d.ok || sec.kind != A10Val.Kind.described)
            break;
        immutable code = sec.u;
        if (code == SEC_PROPERTIES)
        {
            auto val = d.readValue();
            if (val.kind != A10Val.Kind.list)
                continue;
            auto pd = A10Dec(val.bytes);
            foreach (fi; 0 .. val.count)
            {
                immutable at0 = pd.i;
                auto v2 = pd.readValue();
                if (!pd.ok)
                    break;
                if (fi == 0) // message-id: keep the RAW encoding for the echo
                    corrRaw = val.bytes[at0 .. pd.i];
                else if (fi == 2 && v2.kind == A10Val.Kind.str)
                    to = cast(const(char)[]) v2.bytes;
                else if (fi == 3 && v2.kind == A10Val.Kind.str)
                    subject = cast(const(char)[]) v2.bytes;
            }
        }
        else if (code == SEC_AMQP_VALUE)
        {
            auto val = d.readValue();
            if (val.kind == A10Val.Kind.map)
            {
                bodyIsMap = true;
                bodyMapBytes = val.bytes;
                bodyMapCount = val.count;
            }
        }
        else
            d.skipValue2(code);
    }

    char[512] nb = void;
    ByteBuffer bodyOut;

    static const(char)[] strOf(A10Val v) @nogc nothrow
    {
        return v.kind == A10Val.Kind.str ? cast(const(char)[]) v.bytes : null;
    }

    static bool boolOf(A10Val v) @nogc nothrow
    {
        return v.kind == A10Val.Kind.boolean && v.b;
    }

    // ---- /queues/{name} ----
    enum QPFX = "/queues/";
    enum XPFX = "/exchanges/";
    if (to.length > QPFX.length && to[0 .. QPFX.length] == QPFX)
    {
        auto rest = to[QPFX.length .. $];
        bool purge = false;
        enum MSFX = "/messages";
        if (rest.length > MSFX.length
                && rest[$ - MSFX.length .. $] == MSFX)
        {
            purge = true;
            rest = rest[0 .. $ - MSFX.length];
        }
        auto qn = a10UriDecode(rest, nb);
        if (subject == "PUT" && !purge)
        {
            ubyte flags = 0;
            if (bodyIsMap)
            {
                if (boolOf(a10MapGet(bodyMapBytes, bodyMapCount, "durable")))
                    flags |= 2;
                if (boolOf(a10MapGet(bodyMapBytes, bodyMapCount, "exclusive")))
                    flags |= 4;
                if (boolOf(a10MapGet(bodyMapBytes, bodyMapCount, "auto_delete")))
                    flags |= 8;
            }
            immutable created = a10DeclareQueue(qn, flags, false, 0, false, 0,
                    false, 0, "", false, "");
            bodyOut.clear();
            a10QueueInfoMap(bodyOut, qn, flags, a10QueueLen(qn));
            a10MgmtRespond(c, fchan, created ? "201" : "200", corrRaw, 1,
                    cast(const(ubyte)[]) bodyOut.data, "");
            return;
        }
        if (subject == "DELETE")
        {
            immutable n3 = a10QueueLen(qn);
            if (purge)
                a10PurgeQueue(qn);
            else
                a10DeleteQueue(qn);
            bodyOut.clear();
            {
                bodyOut.appendByte(0xD1);
                immutable szAt = bodyOut.length;
                a10PutU32(bodyOut, 0);
                immutable cntAt = bodyOut.length;
                a10PutU32(bodyOut, 0);
                a10Str(bodyOut, "message_count");
                bodyOut.appendByte(0x80);
                foreach (k; 0 .. 8)
                    bodyOut.appendByte(cast(char)(cast(ulong) n3 >> ((7 - k) * 8)));
                a10PatchU32(bodyOut, szAt, cast(uint)(bodyOut.length - cntAt));
                a10PatchU32(bodyOut, cntAt, 2);
            }
            a10MgmtRespond(c, fchan, "200", corrRaw, 1,
                    cast(const(ubyte)[]) bodyOut.data, "");
            return;
        }
        if (subject == "GET")
        {
            if (!a10QueueExists(qn))
            {
                // the client parses THIS phrasing for EntityDoesNotExist
                char[600] eb = void;
                import core.stdc.stdio : snprintf;

                immutable en = snprintf(eb.ptr, eb.length,
                        "no queue '%.*s' in vhost '/'", cast(int) qn.length, qn.ptr);
                a10MgmtRespond(c, fchan, "404", corrRaw, 2, null,
                        eb[0 .. en]);
                return;
            }
            bodyOut.clear();
            a10QueueInfoMap(bodyOut, qn, a10QueueFlags(qn), a10QueueLen(qn));
            a10MgmtRespond(c, fchan, "200", corrRaw, 1,
                    cast(const(ubyte)[]) bodyOut.data, "");
            return;
        }
    }
    // ---- /exchanges/{name} ----
    else if (to.length > XPFX.length && to[0 .. XPFX.length] == XPFX)
    {
        auto xn = a10UriDecode(to[XPFX.length .. $], nb);
        if (subject == "PUT")
        {
            const(char)[] typ = "direct";
            ubyte flags = 0;
            if (bodyIsMap)
            {
                auto t2 = strOf(a10MapGet(bodyMapBytes, bodyMapCount, "type"));
                if (t2.length)
                    typ = t2;
                if (boolOf(a10MapGet(bodyMapBytes, bodyMapCount, "durable")))
                    flags |= 2;
                if (boolOf(a10MapGet(bodyMapBytes, bodyMapCount, "auto_delete")))
                    flags |= 4;
                if (boolOf(a10MapGet(bodyMapBytes, bodyMapCount, "internal")))
                    flags |= 8;
            }
            a10DeclareExchange(xn, typ, flags, "");
            a10MgmtRespond(c, fchan, "204", corrRaw, 0, null, "");
            return;
        }
        if (subject == "DELETE")
        {
            a10DeleteExchange(xn);
            a10MgmtRespond(c, fchan, "204", corrRaw, 0, null, "");
            return;
        }
    }
    // ---- /bindings ----
    else if (to == "/bindings" && subject == "POST" && bodyIsMap)
    {
        auto src = strOf(a10MapGet(bodyMapBytes, bodyMapCount, "source"));
        auto key = strOf(a10MapGet(bodyMapBytes, bodyMapCount, "binding_key"));
        auto dq = strOf(a10MapGet(bodyMapBytes, bodyMapCount, "destination_queue"));
        auto dx = strOf(a10MapGet(bodyMapBytes, bodyMapCount, "destination_exchange"));
        if (src.length && (dq.length || dx.length))
            a10Bind(src, dq.length ? dq : dx, key, dx.length != 0);
        a10MgmtRespond(c, fchan, "204", corrRaw, 0, null, "");
        return;
    }
    else if (to.length > 10 && to[0 .. 10] == "/bindings/" && subject == "DELETE")
    {
        // /bindings/src=S;dstq=D;key=K;args= (or dste=)
        auto spec = to[10 .. $];
        const(char)[] src, dst, key;
        bool dstIsX = false;
        size_t i3 = 0;
        while (i3 < spec.length)
        {
            size_t semi = spec.length;
            foreach (k5, ch8; spec[i3 .. $])
                if (ch8 == ';')
                {
                    semi = i3 + k5;
                    break;
                }
            auto part = spec[i3 .. semi];
            i3 = semi < spec.length ? semi + 1 : spec.length;
            size_t eq = part.length;
            foreach (k5, ch8; part)
                if (ch8 == '=')
                {
                    eq = k5;
                    break;
                }
            if (eq == part.length)
                continue;
            auto pk = part[0 .. eq];
            auto pv = part[eq + 1 .. $];
            if (pk == "src")
                src = pv;
            else if (pk == "dstq")
                dst = pv;
            else if (pk == "dste")
            {
                dst = pv;
                dstIsX = true;
            }
            else if (pk == "key")
                key = pv;
        }
        char[256] sb2 = void, db2 = void, kb2 = void;
        auto srcD = a10UriDecode(src, sb2);
        auto dstD = a10UriDecode(dst, db2);
        auto keyD = a10UriDecode(key, kb2);
        if (srcD.length && dstD.length)
            a10Unbind(srcD, dstD, keyD, dstIsX);
        a10MgmtRespond(c, fchan, "204", corrRaw, 0, null, "");
        return;
    }
    // unknown target/verb
    a10MgmtRespond(c, fchan, "404", corrRaw, 2, null, "Not found");
}

/// Map a stored 0-9-1 record back onto a 1.0 bare message: header (durable/
/// priority/ttl), properties (reply-to/correlation-id/content-type),
/// application-properties from the headers table, one data section.
private void a10BuildMessage(scope const(ubyte)[] blob, ref ByteBuffer o) nothrow @trusted
{
    import dreads.amqp : splitRecord, propsHeaders, propsReplyTo, tableWalk;

    long pm;
    int deaths;
    const(char)[] rk;
    const(ubyte)[] props, body_;
    splitRecord(blob, pm, deaths, rk, props, body_);

    // fixed props walk (flags order)
    ushort flags = 0;
    size_t i = 2;
    const(char)[] contentType, correlationId, replyTo;
    ubyte deliveryMode = 0, priority = 0;
    if (props.length >= 2)
    {
        flags = cast(ushort)((props[0] << 8) | props[1]);
        static const(char)[] ss(scope const(ubyte)[] pp, ref size_t j) @nogc nothrow
        {
            if (j >= pp.length || j + 1 + pp[j] > pp.length)
                return null;
            auto s2 = cast(const(char)[]) pp[j + 1 .. j + 1 + pp[j]];
            j += 1 + pp[j];
            return s2;
        }

        if (flags & 0x8000)
            contentType = ss(props, i);
        if (flags & 0x4000)
            cast(void) ss(props, i);
        if (flags & 0x2000)
        {
            if (i + 4 <= props.length)
            {
                immutable n = (cast(size_t) props[i] << 24) | (cast(size_t) props[i + 1] << 16)
                    | (cast(size_t) props[i + 2] << 8) | props[i + 3];
                i += 4 + n;
            }
        }
        if ((flags & 0x1000) && i < props.length)
            deliveryMode = props[i++];
        if ((flags & 0x0800) && i < props.length)
            priority = props[i++];
        if (flags & 0x0400)
            correlationId = ss(props, i);
        if (flags & 0x0200)
            replyTo = ss(props, i);
    }

    // header section
    {
        auto hl = a10OpenPerf(o, cast(ubyte) SEC_HEADER);
        a10Bool(o, deliveryMode == 2); // durable
        hl.n++;
        if (priority)
        {
            o.appendByte(0x50); // ubyte
            o.appendByte(cast(char) priority);
            hl.n++;
        }
        a10Close(o, hl);
    }
    // properties section
    if (contentType.length || correlationId.length || replyTo.length)
    {
        auto pl = a10OpenPerf(o, cast(ubyte) SEC_PROPERTIES);
        a10Null(o); // message-id
        pl.n++;
        a10Null(o); // user-id
        pl.n++;
        a10Null(o); // to
        pl.n++;
        a10Null(o); // subject
        pl.n++;
        if (replyTo.length)
            a10Str(o, replyTo);
        else
            a10Null(o);
        pl.n++;
        if (correlationId.length)
            a10Str(o, correlationId);
        else
            a10Null(o);
        pl.n++;
        if (contentType.length)
            a10Sym(o, contentType);
        else
            a10Null(o);
        pl.n++;
        a10Close(o, pl);
    }
    // application-properties from the 0-9-1 headers table
    auto hdrs = propsHeaders(props);
    if (hdrs !is null && hdrs.length)
    {
        o.appendByte(0x00);
        a10SmallUlong(o, cast(ubyte) SEC_APP_PROPERTIES);
        o.appendByte(0xD1); // map32
        immutable szAt = o.length;
        a10PutU32(o, 0);
        immutable cntAt = o.length;
        a10PutU32(o, 0);
        uint n2 = 0;
        cast(void) tableWalk(hdrs, (scope const(char)[] k, char ty,
                scope const(ubyte)[] v) nothrow {
            if (ty == 'S')
            {
                a10Str(o, k);
                a10Str(o, cast(const(char)[]) v);
                n2 += 2;
            }
            else if ((ty == 'l' || ty == 'T') && v.length == 8)
            {
                long lv = 0;
                foreach (b3; v)
                    lv = (lv << 8) | b3;
                a10Str(o, k);
                o.appendByte(0x81);
                foreach (k3; 0 .. 8)
                    o.appendByte(cast(char)(lv >> ((7 - k3) * 8)));
                n2 += 2;
            }
            else if (ty == 'I' && v.length == 4)
            {
                a10Str(o, k);
                o.appendByte(0x71);
                foreach (k3; 0 .. 4)
                    o.appendByte(cast(char) v[k3]);
                n2 += 2;
            }
            else if (ty == 't' && v.length == 1)
            {
                a10Str(o, k);
                a10Bool(o, v[0] != 0);
                n2 += 2;
            }
            return true;
        });
        a10PatchU32(o, szAt, cast(uint)(o.length - cntAt));
        a10PatchU32(o, cntAt, n2);
    }
    // data section
    {
        o.appendByte(0x00);
        a10SmallUlong(o, cast(ubyte) SEC_DATA);
        a10Bin(o, body_);
    }
}

/// Delivery fiber for one client-receiver link: pops from the source queue
/// while the client has granted credit, sends transfers, tracks unsettled.
private void a10StartDelivery(A10Conn c, ushort fchan, uint handle) nothrow
{
    try
        cast(void) runTask((A10Conn cc, ushort ch5, uint h5) nothrow {
            import dreads.amqp : a10Pop;

            ByteBuffer pay;
            ByteBuffer outd;
            ByteBuffer msg;
            // park until the attach reply flush (0-9-1 flushSeq lesson: the
            // attach echo must hit the wire before the first transfer)
            try
                sleep(2.msecs);
            catch (Exception)
                return;
            while (!cc.closing)
            {
                auto ps5 = ch5 in cc.sessions;
                if (ps5 is null)
                    return;
                auto pl5 = h5 in ps5.links;
                if (pl5 is null || pl5.detached)
                    return;
                if (pl5.outCredit == 0)
                {
                    if (pl5.drain)
                    {
                        // drain with nothing to send: burn the credit and echo
                        pl5.drain = false;
                    }
                    try
                        sleep(1.msecs);
                    catch (Exception)
                        return;
                    continue;
                }
                pay.clear();
                if (!a10Pop(pl5.rkey, pay))
                {
                    if (pl5.drain)
                    {
                        // drain: advance delivery-count by remaining credit,
                        // zero the credit, echo a flow so the client unblocks
                        pl5.deliveryCount += pl5.outCredit;
                        pl5.outCredit = 0;
                        pl5.drain = false;
                        outd.clear();
                        auto fD = a10FrameStart(outd, FRAME_TYPE_AMQP, ch5);
                        auto lD = a10OpenPerf(outd, cast(ubyte) PERF_FLOW);
                        a10UInt(outd, ps5.nextIncomingId);
                        lD.n++;
                        a10UInt(outd, 2048);
                        lD.n++;
                        a10UInt(outd, ps5.nextOutgoingId);
                        lD.n++;
                        a10UInt(outd, 2048);
                        lD.n++;
                        a10UInt(outd, h5);
                        lD.n++;
                        a10UInt(outd, pl5.deliveryCount);
                        lD.n++;
                        a10UInt(outd, 0); // link-credit exhausted
                        lD.n++;
                        a10Close(outd, lD);
                        a10FrameFinish(outd, fD);
                        a10Send(cc, cast(const(ubyte)[]) outd.data);
                    }
                    try
                        sleep(1.msecs);
                    catch (Exception)
                        return;
                    continue;
                }
                // build transfer + message
                immutable did = ps5.nextOutgoingId++;
                pl5.deliveryCount++;
                pl5.outCredit--;
                try
                    ps5.unsettled[did] = A10Out(pl5.rkey,
                            (cast(const(ubyte)[]) pay.data).idup, h5);
                catch (Exception)
                {
                }
                outd.clear();
                auto fT = a10FrameStart(outd, FRAME_TYPE_AMQP, ch5);
                auto lT = a10OpenPerf(outd, cast(ubyte) PERF_TRANSFER);
                a10UInt(outd, h5); // handle
                lT.n++;
                a10UInt(outd, did); // delivery-id
                lT.n++;
                {
                    ubyte[4] dt = void;
                    dt[0] = cast(ubyte)(did >> 24);
                    dt[1] = cast(ubyte)(did >> 16);
                    dt[2] = cast(ubyte)(did >> 8);
                    dt[3] = cast(ubyte)(did & 0xFF);
                    a10Bin(outd, dt[]);
                }
                lT.n++;
                a10UInt(outd, 0); // message-format
                lT.n++;
                a10Bool(outd, false); // settled: client dispositions decide
                lT.n++;
                a10Bool(outd, false); // more
                lT.n++;
                a10Close(outd, lT);
                msg.clear();
                a10BuildMessage(cast(const(ubyte)[]) pay.data, msg);
                outd.append(cast(const(char)[]) msg.data);
                a10FrameFinish(outd, fT);
                a10Send(cc, cast(const(ubyte)[]) outd.data);
            }
        }, c, fchan, handle);
    catch (Exception)
    {
    }
}

/// Half-interval empty-frame heartbeats + 2x dead-peer (0-9-1 lessons).
private void a10StartHeartbeat(A10Conn c) nothrow
{
    if (c.hbStarted || c.peerIdleMs == 0)
        return;
    c.hbStarted = true;
    try
        cast(void) runTask((A10Conn cc) nothrow {
            static immutable ubyte[8] empty = [0, 0, 0, 8, 2, 0, 0, 0];
            immutable dur = cc.peerIdleMs / 2 < 100 ? 100 : cc.peerIdleMs / 2;
            while (!cc.closing)
            {
                try
                    sleep(dur.msecs);
                catch (Exception)
                    return;
                if (cc.closing)
                    return;
                if (cc.lastReadMs != 0
                        && monoMs10() - cc.lastReadMs > cast(long) A10_OUR_IDLE_MS * 2 + 500)
                {
                    cc.closing = true;
                    try
                        cc.tcp.close();
                    catch (Exception)
                    {
                    }
                    return;
                }
                a10Send(cc, empty[]);
            }
        }, c);
    catch (Exception)
    {
    }
}

private bool a10ReadExact(A10Conn c, scope ubyte[] dst) nothrow
{
    size_t got = 0;
    while (got < dst.length)
    {
        try
        {
            if (!c.tcp.waitForData())
                return false;
            auto n = c.tcp.leastSize;
            if (n == 0)
                return false;
            auto want = dst.length - got;
            auto take = n < want ? cast(size_t) n : want;
            c.tcp.read(dst[got .. got + take]);
            got += take;
        }
        catch (Exception)
            return false;
    }
    return true;
}

/// Read one 1.0 frame; yields the BODY (after the extended header). An empty
/// body = keepalive. False = connection gone/malformed.
private bool a10ReadFrame(A10Conn c, out const(ubyte)[] body_, out ubyte ftype,
        out ushort fchan) nothrow
{
    ubyte[8] h;
    if (!a10ReadExact(c, h[]))
        return false;
    immutable size = (cast(uint) h[0] << 24) | (cast(uint) h[1] << 16)
        | (cast(uint) h[2] << 8) | h[3];
    immutable doff = h[4];
    ftype = h[5];
    fchan = cast(ushort)((h[6] << 8) | h[7]);
    if (size < 8 || doff < 2 || size > A10_MAX_FRAME || cast(size_t) doff * 4 > size)
        return false;
    static ubyte[] buf; // TLS scratch (consumed before return; no yield holds it)
    immutable rest = size - 8;
    if (buf.length < rest)
        buf.length = rest;
    if (rest && !a10ReadExact(c, buf[0 .. rest]))
        return false;
    immutable skip = cast(size_t) doff * 4 - 8; // extended header
    body_ = buf[skip .. rest];
    return true;
}

// ---------------------------------------------------------------------------
// Codec unittests (dub test coverage for M1)

unittest // primitive roundtrip
{
    ByteBuffer o;
    a10Str(o, "hello");
    a10Sym(o, "amqp:ok");
    a10UInt(o, 0);
    a10UInt(o, 7);
    a10UInt(o, 70_000);
    a10Bool(o, true);
    a10Null(o);
    auto d = A10Dec(cast(const(ubyte)[]) o.data);
    auto s = d.readValue();
    assert(d.ok && s.kind == A10Val.Kind.str && cast(const(char)[]) s.bytes == "hello");
    auto y = d.readValue();
    assert(d.ok && y.kind == A10Val.Kind.str && cast(const(char)[]) y.bytes == "amqp:ok");
    assert(d.readValue().u == 0);
    assert(d.readValue().u == 7);
    assert(d.readValue().u == 70_000);
    assert(d.readValue().b == true);
    assert(d.readValue().kind == A10Val.Kind.null_);
}

unittest // performative roundtrip (open)
{
    ByteBuffer o;
    auto f = a10FrameStart(o, FRAME_TYPE_AMQP, 0);
    auto l = a10OpenPerf(o, cast(ubyte) PERF_OPEN);
    a10Str(o, "cid");
    l.n++;
    a10Null(o);
    l.n++;
    o.appendByte(0x70);
    a10PutU32(o, 65_536);
    l.n++;
    a10Close(o, l);
    a10FrameFinish(o, f);
    auto raw = cast(const(ubyte)[]) o.data;
    immutable size = (cast(uint) raw[0] << 24) | (cast(uint) raw[1] << 16)
        | (cast(uint) raw[2] << 8) | raw[3];
    assert(size == raw.length);
    A10Dec fields;
    uint nf;
    immutable code = a10Performative(raw[8 .. $], fields, nf);
    assert(code == PERF_OPEN);
    assert(nf == 3);
    auto cid = fields.readValue();
    assert(cast(const(char)[]) cid.bytes == "cid");
    fields.skipValue();
    auto mf = fields.readValue();
    assert(mf.kind == A10Val.Kind.u64 && mf.u == 65_536);
}
