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

import dreads.tls : TlsLeg, legPump, legTake, legSend;

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
    int depth; // recursion guard for nested described types (stack-overflow DoS)
    enum int MAX_DEPTH = 32;

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
        if (depth >= MAX_DEPTH) // bound nested described-type recursion
        {
            ok = false;
            return v;
        }
        immutable c = u8();
        if (!ok)
            return v;
        switch (c)
        {
        case 0x00: // described: descriptor value, then the value itself
            {
                depth++;
                auto d = readValue();
                depth--;
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

// snprintf returns the would-be length (can exceed the buffer); clamp it to a
// valid slice length so `buf[0 .. a10ClampN(ret, buf.length)]` never reads OOB.
private size_t a10ClampN(int n, size_t bufLen) @safe @nogc nothrow
{
    if (n <= 0)
        return 0;
    immutable un = cast(size_t) n;
    return un >= bufLen ? (bufLen ? bufLen - 1 : 0) : un;
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
    bool anonymous; // empty sender target: per-message properties.to routing
    bool v2Queue; // "/queues/..." address: existence is ENFORCED (the v2
    // client declares via $management; attach/deliver must 404 when gone)
    int prio; // attach properties "rabbitmq:priority" (consumer preference)
    immutable(ubyte)[] srcFilterRaw; // client source filter-set (echoed back:
    // stream consumers refuse an attach whose source drops their filters)
    // --- STREAM consumption (v2): non-destructive positional reads ---
    bool stream; // source queue is mgmt-declared type=stream
    long streamPos; // next absolute offset to read
    ubyte offKind; // 0 none, 1 first, 2 last, 3 next, 4 offset, 5 timestamp
    long offVal;
    string[] streamFilterVals; // rabbitmq:stream-filter (bloom values)
    bool matchUnfiltered; // rabbitmq:stream-match-unfiltered
    immutable(ubyte)[] propFilterRaw; // amqp:properties-filter map contents
    uint propFilterCount;
    immutable(ubyte)[] appFilterRaw; // amqp:application-properties-filter map
    uint appFilterCount;
    bool hasFilters; // any bloom/expression filter present
    long bloomChunk = -1; // chunk index the cached bloom decision is for
    bool bloomPass; // cached: does the current chunk pass the value filter?
}

/// Address grammar shared by attach targets/sources and per-message `to`.
private void a10ResolveAddress(scope const(char)[] address,
        return scope char[] exBuf, return scope char[] rkBuf,
        out const(char)[] exch, out const(char)[] rk) @nogc nothrow
{
    exch = "";
    rk = address;
    enum QP = "/queues/";
    if (address.length > QP.length && address[0 .. QP.length] == QP)
    {
        rk = a10UriDecode(address[QP.length .. $], rkBuf);
        return;
    }
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
            exch = a10UriDecode(rest[0 .. slash], exBuf);
            rk = slash < rest.length ? a10UriDecode(rest[slash + 1 .. $], rkBuf) : "";
            return;
        }
}

/// One in-flight (unsettled) delivery we sent on a receiver link.
private struct A10Out
{
    string queue;
    immutable(ubyte)[] blob;
    uint handle;
    bool stream; // stream deliveries: dispositions never touch the log
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
    TlsLeg* tlsLeg; // null = plaintext (handed over by the 0-9-1 accept)
    TCPConnection tcp;
    bool closing;
    uint peerIdleMs; // peer's open.idle-time-out: we SEND empties at half it
    long lastReadMs; // MonoTime ms (dead-peer, 0-9-1 lesson: never gClock)
    bool hbStarted;
    ulong connId; // exclusivity token (shared generator with 0-9-1 conns)
    A10Session[ushort] sessions; // keyed by the CLIENT channel
    ulong[] dispScratch; // disposition settle-id snapshot (read fiber only)
    size_t pendingBytes; // aggregate multi-frame fragment bytes in flight
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
/// Per-connection link cap: each attach inserts ps.links[handle] for any u32
/// and (for receivers) spawns a delivery fiber able to buffer a 16MiB fragment.
/// Bound the total so one connection can't exhaust memory/fibers.
enum uint A10_MAX_LINKS_PER_CONN = 4096;
/// Per-connection aggregate cap on concurrently-accumulating multi-frame
/// fragment bytes: without it 4096 links x 16MiB per-link pending ~= 64GiB.
enum size_t A10_MAX_PENDING_BYTES_PER_CONN = 128UL << 20;
/// Per-session cap on undisposed (unsettled) deliveries: a client can grant huge
/// link-credit and never settle, so unsettled[] (each entry holds an idup'd blob)
/// would grow without bound. At the cap the delivery fiber stops popping (spec-
/// legal backpressure) until the client disposes some deliveries.
enum size_t A10_MAX_UNSETTLED_PER_SESSION = 8192;

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
        if (c.tlsLeg !is null)
        {
            if (!legSend(c.tlsLeg, c.tcp, bytes))
                c.closing = true;
        }
        else
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
public void amqp10Serve(TCPConnection tcp, bool saslLayer, TlsLeg* leg = null) nothrow
{
    import dreads.amqp : a10NewConnId;

    auto c = new A10Conn(tcp);
    c.tlsLeg = leg; // ownership transferred from the 0-9-1 accept
    c.connId = a10NewConnId();
    scope (exit)
    {
        c.closing = true;
        a10TeardownRequeue(c);
        closeQuiet(tcp);
        if (c.tlsLeg !is null)
        {
            c.tlsLeg.free(); // owning thread, after every sender is done
            c.tlsLeg = null;
        }
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
    {
        import dreads.acl : aclUserCount;

        // ACL configured: a bare (SASL-less) header MUST NOT reach the AMQP
        // layer unauthenticated. Offer the SASL header and hang up so the
        // client renegotiates with credentials. (accept-any stays for <=1 user.)
        if (aclUserCount() > 1)
        {
            a10Send(c, AMQP10_HDR_SASL[]);
            return;
        }
        a10Send(c, AMQP10_HDR_BARE[]);
    }

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
                    {
                        // offered-capabilities: the java client refuses to
                        // build anonymous publishers without ANONYMOUS-RELAY
                        outb.appendByte(0xE0); // array8
                        immutable szC = outb.length;
                        outb.appendByte(0);
                        outb.appendByte(1); // count
                        outb.appendByte(0xA3); // element ctor: sym8
                        outb.appendByte(15);
                        outb.append("ANONYMOUS-RELAY");
                        auto dc = cast(ubyte[]) outb.data;
                        dc[szC] = cast(ubyte)(outb.length - szC - 1);
                    }
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
                debug (a10wire)
                {
                    import core.stdc.stdio : fprintf, stderr;
                    fprintf(stderr, "A10 DETACH-IN ch=%u\n", cast(uint) fchan);
                }
                uint handle = uint.max;
                if (nf >= 1)
                {
                    auto h2 = fields.readValue();
                    if (h2.kind == A10Val.Kind.u64)
                        handle = cast(uint) h2.u;
                }
                if (handle == uint.max)
                    break; // detach with no handle: ignore (don't tear down link 0
                // or mass-requeue via the uint.max "all handles" sentinel)
                bool weAlreadyDetached = false;
                if (auto ps = fchan in c.sessions)
                {
                    if (auto pl4 = handle in ps.links)
                    {
                        // detached==true means WE already sent a detach for
                        // this link (spontaneous resource-deleted etc) and the
                        // client's detach CROSSED ours on the wire. The pair
                        // is complete — echoing again is a THIRD detach for a
                        // handle the client has forgotten, which proton treats
                        // as a connection error (uncorrelated handle).
                        weAlreadyDetached = pl4.detached;
                        pl4.detached = true; // delivery fiber exits on its next pass
                        if (pl4.pendingActive) // reclaim any in-flight fragment budget
                            c.pendingBytes = c.pendingBytes >= pl4.pending.length
                                ? c.pendingBytes - pl4.pending.length : 0;
                    }
                    a10RequeueUnsettled(c, fchan, handle);
                    ps.links.remove(handle);
                }
                if (weAlreadyDetached)
                    break;
                debug (a10wire)
                {
                    import core.stdc.stdio : fprintf, stderr;
                    fprintf(stderr, "A10 DETACH-ECHO ch=%u h=%u\n",
                            cast(uint) fchan, handle);
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
    // Per-connection link cap (aggregate across sessions): no bound would let a
    // client attach 2^32 links, each spawning a fiber / buffering 16MiB. Refuse
    // past the cap — lk.name/handle/role are set, so the refuse frame is valid.
    {
        size_t totalLinks = 0;
        foreach (ref s2; c.sessions)
            totalLinks += s2.links.length;
        if (totalLinks >= A10_MAX_LINKS_PER_CONN)
        {
            a10RefuseAttach(c, fchan, lk, "amqp:resource-limit-exceeded", "link cap");
            return;
        }
    }
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
        if (!lk.clientSender)
        {
            // source: address (0) + FILTER set (7) — parse both
            auto tgt = fields.readValue();
            if (tgt.kind == A10Val.Kind.described)
            {
                auto inner = fields.readValue();
                if (inner.kind == A10Val.Kind.list && inner.count >= 1)
                {
                    auto td = A10Dec(inner.bytes);
                    auto av = td.readValue();
                    if (av.kind == A10Val.Kind.str && av.bytes.length <= addrBuf.length)
                    {
                        addrBuf[0 .. av.bytes.length] = cast(const(char)[]) av.bytes;
                        addrLen = av.bytes.length;
                        address = addrBuf[0 .. addrLen];
                    }
                    // skip fields 1..6, capture field 7 (filter) RAW
                    foreach (fi3; 1 .. 7)
                        if (inner.count > fi3)
                            td.skipValue();
                    if (inner.count >= 8)
                    {
                        immutable fAt = td.i;
                        td.skipValue();
                        if (td.ok && td.i > fAt)
                            try
                                lk.srcFilterRaw = inner.bytes[fAt .. td.i].idup;
                            catch (Exception)
                            {
                            }
                    }
                }
            }
        }
        else
            fields.skipValue(); // source unused for a client sender
    }
    if (nf >= 7 && lk.clientSender)
    {
        if (grabAddr(fields, addrBuf, addrLen))
            address = addrBuf[0 .. addrLen];
    }
    // fields 7..12 skipped; field 13 = link properties (rabbitmq:priority)
    foreach (fi2; 7 .. 13)
        if (nf > fi2)
            fields.skipValue();
    if (nf >= 14)
    {
        auto lp = fields.readValue();
        if (lp.kind == A10Val.Kind.map)
        {
            auto pv2 = a10MapGet(lp.bytes, lp.count, "rabbitmq:priority");
            if (pv2.kind == A10Val.Kind.u64)
                lk.prio = cast(int) pv2.u;
            else if (pv2.kind == A10Val.Kind.i64)
                lk.prio = cast(int) pv2.i;
        }
    }
    // resolve address -> (exchange, rkey): "/queues/N" is a queue,
    // "/exchanges/X[/RK]" routes through X, a plain name is a queue on the
    // default exchange, and an EMPTY sender target is the anonymous relay
    // (each message routes by its own properties.to)
    char[512] exBuf = void, rkBuf = void;
    const(char)[] exch, rk;
    a10ResolveAddress(address, exBuf, rkBuf, exch, rk);
    if (lk.clientSender && addrLen == 0)
        lk.anonymous = true;
    try
    {
        lk.exchange = exch.idup;
        lk.rkey = rk.idup;
    }
    catch (Exception)
    {
    }
    debug (a10wire)
    {
        import core.stdc.stdio : fprintf, stderr;
        fprintf(stderr, "A10 ATTACH ch=%u h=%u sender=%d addr='%.*s' ex='%.*s' rk='%.*s'\n",
                cast(uint) fchan, lk.handle, lk.clientSender ? 1 : 0,
                cast(int) address.length, address.ptr,
                cast(int) lk.exchange.length, lk.exchange.ptr,
                cast(int) lk.rkey.length, lk.rkey.ptr);
    }
    enum QP2 = "/queues/";
    immutable isV2Q = address.length > QP2.length && address[0 .. QP2.length] == QP2;
    if (address == "/management")
    {
        lk.isMgmt = true;
        if (!lk.clientSender)
            ps.mgmtRecvHandle = lk.handle; // responses flow back on this link
    }
    else if (isV2Q)
    {
        lk.v2Queue = true;
        import dreads.amqp : a10QueueExists;

        if (!a10QueueExists(lk.rkey))
        {
            a10RefuseAttach(c, fchan, lk, "amqp:not-found", lk.rkey);
            return;
        }
    }
    else if (lk.exchange.length)
    {
        import dreads.amqp : a10ExchangeExists;

        if (!a10ExchangeExists(lk.exchange))
        {
            a10RefuseAttach(c, fchan, lk, "amqp:not-found", lk.exchange);
            return;
        }
    }
    else if (lk.exchange.length == 0 && lk.rkey.length)
        a10EnsureQueue(lk.rkey); // BARE-name address keeps the declare-on-attach
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
            if (lk.srcFilterRaw.length)
            {
                // pad fields 1-6, then echo the client's filter-set verbatim
                foreach (fi4; 1 .. 7)
                {
                    a10Null(outb);
                    sl.n++;
                }
                outb.append(cast(const(char)[]) lk.srcFilterRaw);
                sl.n++;
            }
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
        else
        {
            a10Null(outb); // unsettled
            l.n++;
            a10Null(outb); // incomplete-unsettled
            l.n++;
            a10Null(outb); // initial-delivery-count (receiver: none)
            l.n++;
            // max-message-size (ulong): the client enforces it sender-side
            outb.appendByte(0x80);
            immutable ulong mms = 16 * 1024 * 1024;
            foreach (k9; 0 .. 8)
                outb.appendByte(cast(char)(mms >> ((7 - k9) * 8)));
            l.n++;
        }
        a10Close(outb, l);
        a10FrameFinish(outb, f);
    }
    if (!lk.clientSender && !lk.isMgmt)
    {
        import dreads.amqp : a10ConsumerInc, a10PrioAdd, a10QueueLen;

        // v2 STREAMS: a receiver on a mgmt-declared stream queue reads
        // non-destructively by position; the source filter-set carries the
        // offset spec + bloom/expression filters (parsed once here)
        {
            const(char)[] qt;
            try
                if (auto pt = (cast(string) lk.rkey) in gA10QueueType)
                    qt = *pt;
            catch (Exception)
            {
            }
            if (qt == "stream")
            {
                auto plk = (){ auto ps9 = fchan in c.sessions; return ps9 !is null
                        ? lk.handle in ps9.links : null; }();
                if (plk !is null)
                {
                    a10ParseFilters(plk);
                    plk.stream = true;
                    immutable qlen = a10QueueLen(plk.rkey);
                    switch (plk.offKind)
                    {
                    default:
                    case 0: // RabbitMQ stream default: "next"
                    case 3:
                        plk.streamPos = qlen;
                        break;
                    case 1: // first
                        plk.streamPos = 0;
                        break;
                    case 2: // last: the final existing message
                        plk.streamPos = qlen > 0 ? qlen - 1 : 0;
                        break;
                    case 4: // absolute offset (clamped)
                        plk.streamPos = plk.offVal < 0 ? 0 : plk.offVal;
                        break;
                    case 5: // absolute timestamp: first record at/after it
                    case 6: // interval "<n><unit>": first record younger than Δ
                        {
                            import dreads.amqp : a10PeekAt, splitRecord;

                            long target;
                            if (plk.offKind == 6)
                            {
                                long nowWall = 0;
                                try
                                {
                                    import std.datetime.systime : Clock;

                                    nowWall = Clock.currTime.toUnixTime!long * 1000;
                                }
                                catch (Exception)
                                {
                                }
                                target = nowWall - plk.offVal;
                            }
                            else
                                target = plk.offVal;
                            static ByteBuffer sb; // TLS: consumed per peek
                            long pos = 0;
                            while (pos < qlen)
                            {
                                sb.clear();
                                if (!a10PeekAt(plk.rkey, pos, sb))
                                    break;
                                long pm;
                                int dth;
                                const(char)[] rk0;
                                const(ubyte)[] pr0, bd0;
                                splitRecord(cast(const(ubyte)[]) sb.data, pm, dth,
                                        rk0, pr0, bd0);
                                if (pm >= target)
                                    break;
                                pos++;
                            }
                            plk.streamPos = pos;
                            break;
                        }
                    }
                }
            }
            else
            {
                // non-stream links still parse expression filters (harmless;
                // evaluated per delivery)
                auto plk2 = (){ auto ps9 = fchan in c.sessions; return ps9 !is null
                        ? lk.handle in ps9.links : null; }();
                if (plk2 !is null)
                    a10ParseFilters(plk2);
            }
        }
        a10ConsumerInc(lk.rkey); // replicated count: queue-info + x-expires
        a10PrioAdd(lk.rkey, lk.prio);
        a10StartDelivery(c, fchan, lk.handle);
    }
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
        if (plk.pending.length + msg.length > 16 * 1024 * 1024)
        {
            // RabbitMQ's default max message size: refuse with the 1.0
            // link error the client maps to a size exception
            c.pendingBytes = c.pendingBytes >= plk.pending.length
                ? c.pendingBytes - plk.pending.length : 0;
            plk.pendingActive = false;
            plk.pending = null;
            a10SendDetachError(c, fchan, handle,
                    "amqp:link:message-size-exceeded", plk.rkey);
            plk.detached = true;
            return;
        }
        // Per-connection aggregate budget: a client could open A10_MAX_LINKS_PER_CONN
        // links and hold a near-16MiB fragment on each (~64GiB) by never sending the
        // final (more=false) frame. Bound the concurrently-accumulating total.
        if (c.pendingBytes + msg.length > A10_MAX_PENDING_BYTES_PER_CONN)
        {
            c.pendingBytes = c.pendingBytes >= plk.pending.length
                ? c.pendingBytes - plk.pending.length : 0;
            plk.pendingActive = false;
            plk.pending = null;
            a10SendDetachError(c, fchan, handle,
                    "amqp:resource-limit-exceeded", plk.rkey);
            plk.detached = true;
            return;
        }
        try
            plk.pending ~= msg;
        catch (Exception)
        {
            // failed append: reclaim what was already counted for this link
            c.pendingBytes = c.pendingBytes >= plk.pending.length
                ? c.pendingBytes - plk.pending.length : 0;
            plk.pendingActive = false;
            return;
        }
        c.pendingBytes += msg.length; // count only a successful append
        if (more)
            return;
        msg = plk.pending;
        deliveryId = plk.pendingDeliveryId;
        settled = settled || plk.pendingSettled;
        c.pendingBytes = c.pendingBytes >= plk.pending.length
            ? c.pendingBytes - plk.pending.length : 0; // delivery complete: reclaim
        plk.pendingActive = false;
    }
    plk.deliveryCount++;
    // decode sections -> 0-9-1 props + body
    int transferRouted = plk.isMgmt ? 1 : 0; // mgmt "routes" by definition
    if (plk.isMgmt)
    {
        // copy the body out of a10ReadFrame's shared TLS `buf`: a10HandleMgmt
        // reads corrRaw/bodyMapBytes (slices of msg) AFTER hops (a10DeclareQueue
        // /... -> amqpDataExec park); a sibling connection's a10ReadFrame would
        // refill `buf` during the park -> cross-connection disclosure.
        ByteBuffer mgmtCopy;
        mgmtCopy.append(msg);
        a10HandleMgmt(c, fchan, cast(const(ubyte)[]) mgmtCopy.data);
    }
    else
    {
        if (plk.v2Queue)
        {
            import dreads.amqp : a10QueueExists, a10QueueFull;

            if (!a10QueueExists(plk.rkey))
            {
                a10SendDetachError(c, fchan, handle, "amqp:resource-deleted",
                        plk.rkey);
                plk.detached = true;
                return;
            }
            if (a10QueueFull(plk.rkey))
            {
                // x-max-length reached: REJECT the publish (1.0 semantics —
                // the classic head-drop stays for the 0-9-1 path)
                if (!settled)
                {
                    ByteBuffer or;
                    auto fr = a10FrameStart(or, FRAME_TYPE_AMQP, fchan);
                    auto lr = a10OpenPerf(or, cast(ubyte) PERF_DISPOSITION);
                    a10Bool(or, true); // role: receiver
                    lr.n++;
                    a10UInt(or, deliveryId);
                    lr.n++;
                    a10UInt(or, deliveryId);
                    lr.n++;
                    a10Bool(or, true); // settled
                    lr.n++;
                    {
                        auto sr = a10OpenPerf(or, 0x25); // rejected
                        a10Close(or, sr);
                    }
                    lr.n++;
                    a10Close(or, lr);
                    a10FrameFinish(or, fr);
                    a10Send(c, cast(const(ubyte)[]) or.data);
                }
                return;
            }
        }
        else if (plk.exchange.length)
        {
            import dreads.amqp : a10ExchangeExists;

            if (!a10ExchangeExists(plk.exchange))
            {
                a10SendDetachError(c, fchan, handle, "amqp:not-found",
                        plk.exchange);
                plk.detached = true;
                return;
            }
        }
        static ByteBuffer props; // TLS: consumed by a10Publish before any yield
        static ByteBuffer bodyBuf; // TLS
        const(char)[] msgTo;
        a10MapMessage(msg, props, bodyBuf, msgTo);
        auto exch = cast(const(char)[]) plk.exchange;
        auto rkey = cast(const(char)[]) plk.rkey;
        char[512] exB = void, rkB = void;
        if (plk.anonymous && msgTo.length)
            a10ResolveAddress(msgTo, exB, rkB, exch, rkey); // anonymous relay
        transferRouted = a10Publish(exch, rkey,
                cast(const(ubyte)[]) props.data, cast(const(ubyte)[]) bodyBuf.data);
    }
    debug (a10wire)
    {
        import core.stdc.stdio : fprintf, stderr;
        fprintf(stderr, "A10 TRANSFER ch=%u h=%u settled=%d routed=%d\n",
                cast(uint) fchan, handle, settled ? 1 : 0, transferRouted);
    }
    // settle back (rcv-settle-mode first): ACCEPTED when routed, RELEASED
    // when the message matched no queue (RabbitMQ's unroutable signal)
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
            auto st2 = a10OpenPerf(outb,
                    transferRouted > 0 ? cast(ubyte) STATE_ACCEPTED : 0x26);
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
        ref ByteBuffer bodyBuf, out const(char)[] msgTo) nothrow @trusted
{
    props.clear();
    bodyBuf.clear();
    ushort flags = 0;
    // staging for the fixed-order 0-9-1 property list
    const(char)[] contentType, correlationId, replyTo;
    msgTo = null;
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
        immutable valAt = d.i; // raw section VALUE start (with constructor)
        auto val = d.readValue();
        if (!d.ok)
            break;
        immutable(ubyte)[] secRaw;
        if (code == SEC_PROPERTIES)
            try
                secRaw = msg[valAt .. d.i].idup;
            catch (Exception)
            {
            }
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
            // the WHOLE section rides a reserved raw header: the 1.0 rebuild
            // replays it verbatim, so every field (numeric correlation-ids,
            // user-id, timestamps, group-*) survives losslessly — the filter
            // expressions match against the real values
            if (secRaw.length)
            {
                hdrTbl.appendByte(cast(char) 11);
                hdrTbl.append("x-a10-props");
                hdrTbl.appendByte('x');
                a10PutU32(hdrTbl, cast(uint) secRaw.length);
                hdrTbl.append(cast(const(char)[]) secRaw);
            }
            if (val.kind == A10Val.Kind.list)
            {
                auto pd = A10Dec(val.bytes);
                // message-id(0) user-id(1) to(2) subject(3) reply-to(4)
                // correlation-id(5) content-type(6) ...
                foreach (fi; 0 .. val.count)
                {
                    immutable at9 = pd.i;
                    auto v2 = pd.readValue();
                    if (!pd.ok)
                        break;
                    if (fi == 0 && v2.kind != A10Val.Kind.null_)
                    {
                        // message-id: ANY type — preserve the RAW 1.0 encoding
                        // in a reserved 'x'-typed header for lossless replay
                        auto raw9 = val.bytes[at9 .. pd.i];
                        hdrTbl.appendByte(cast(char) 9);
                        hdrTbl.append("x-a10-mid");
                        hdrTbl.appendByte('x');
                        a10PutU32(hdrTbl, cast(uint) raw9.length);
                        hdrTbl.append(cast(const(char)[]) raw9);
                    }
                    else if (fi == 3 && v2.kind == A10Val.Kind.str)
                    {
                        // subject: preserved for 1.0 consumers (reserved header)
                        hdrTbl.appendByte(cast(char) 10);
                        hdrTbl.append("x-a10-subj");
                        hdrTbl.appendByte('S');
                        a10PutU32(hdrTbl, cast(uint) v2.bytes.length);
                        hdrTbl.append(cast(const(char)[]) v2.bytes);
                    }
                    else if (fi == 2 && v2.kind == A10Val.Kind.str)
                        msgTo = cast(const(char)[]) v2.bytes;
                    else if (fi == 4 && v2.kind == A10Val.Kind.str)
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
                    immutable vAtA = md.i;
                    auto v2 = md.readValue();
                    auto rawA = val.bytes[vAtA .. md.i];
                    if (!md.ok || k2.kind != A10Val.Kind.str || k2.bytes.length > 127)
                        break;
                    hdrTbl.appendByte(cast(char) k2.bytes.length);
                    hdrTbl.append(cast(const(char)[]) k2.bytes);
                    // Types 0-9-1 tables can't represent losslessly keep the
                    // RAW 1.0 encoding ('x'): float 0x72, symbol 0xA3/0xB3,
                    // timestamp 0x83, uuid 0x98, binary 0xA0/0xB0 (the filter
                    // tests assert exact type round-trips)
                    if (rawA.length && (rawA[0] == 0x72 || rawA[0] == 0xA3
                            || rawA[0] == 0xB3 || rawA[0] == 0x83
                            || rawA[0] == 0x98 || rawA[0] == 0xA0
                            || rawA[0] == 0xB0))
                    {
                        hdrTbl.appendByte('x');
                        a10PutU32(hdrTbl, cast(uint) rawA.length);
                        hdrTbl.append(cast(const(char)[]) rawA);
                        continue;
                    }
                    final switch (v2.kind)
                    {
                    case A10Val.Kind.str:
                        hdrTbl.appendByte('S');
                        a10PutU32(hdrTbl, cast(uint) v2.bytes.length);
                        hdrTbl.append(cast(const(char)[]) v2.bytes);
                        break;
                    case A10Val.Kind.u64:
                    case A10Val.Kind.i64:
                        immutable lv = v2.kind == A10Val.Kind.u64
                            ? cast(long) v2.u : v2.i;
                        // INT-encoded (0x71/0x54): keep 32-bit identity so
                        // the consumer gets an Integer back (filter tests)
                        if (rawA.length && (rawA[0] == 0x71 || rawA[0] == 0x54)
                                && lv >= int.min && lv <= int.max)
                        {
                            hdrTbl.appendByte('I');
                            foreach (k3; 0 .. 4)
                                hdrTbl.appendByte(cast(char)(lv >> ((3 - k3) * 8)));
                            break;
                        }
                        hdrTbl.appendByte('l');
                        foreach (k3; 0 .. 8)
                            hdrTbl.appendByte(cast(char)(lv >> ((7 - k3) * 8)));
                        break;
                    case A10Val.Kind.boolean:
                        hdrTbl.appendByte('t');
                        hdrTbl.appendByte(v2.b ? 1 : 0);
                        break;
                    case A10Val.Kind.f64:
                        hdrTbl.appendByte('d'); // 0-9-1 double
                        {
                            ulong raw8 = *cast(ulong*)&v2.f;
                            foreach (k3; 0 .. 8)
                                hdrTbl.appendByte(cast(char)(raw8 >> ((7 - k3) * 8)));
                        }
                        break;
                    case A10Val.Kind.null_:
                        hdrTbl.appendByte('V');
                        break;
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
        case SEC_MESSAGE_ANN:
            // published annotations round-trip to 1.0 consumers: convert into
            // x-* header entries (raw 'x' for list/map/array/uuid values)
            if (val.kind == A10Val.Kind.map)
                a10MapToTable(val.bytes, val.count, hdrTbl);
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
        // 0-9-1 shortstr length is ONE byte: >255 truncates the length and
        // misframes the record. Clamp value+len together (extend-only).
        immutable ctl = contentType.length > 255 ? 255 : contentType.length;
        props.appendByte(cast(char) ctl);
        props.append(contentType[0 .. ctl]);
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
        immutable cil = correlationId.length > 255 ? 255 : correlationId.length;
        props.appendByte(cast(char) cil);
        props.append(correlationId[0 .. cil]);
    }
    if (flags & 0x0200)
    {
        immutable rtl = replyTo.length > 255 ? 255 : replyTo.length;
        props.appendByte(cast(char) rtl);
        props.append(replyTo[0 .. rtl]);
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
    const(ubyte)[] modAnnBytes;
    uint modAnnCount;
    bool modUndeliverable;
    if (nf >= 5)
    {
        auto st = fields.readValue();
        if (st.kind == A10Val.Kind.described && st.u != ulong.max)
        {
            state = st.u;
            auto stv = fields.readValue(); // the state's field list
            if (state == 0x27 && stv.kind == A10Val.Kind.list)
            {
                // modified: delivery-failed(0) undeliverable-here(1)
                // message-annotations(2)
                auto sd = A10Dec(stv.bytes);
                if (stv.count >= 1)
                    sd.skipValue();
                if (stv.count >= 2)
                {
                    auto uh = sd.readValue();
                    modUndeliverable = uh.kind == A10Val.Kind.boolean && uh.b;
                }
                if (stv.count >= 3)
                {
                    auto ann = sd.readValue();
                    if (ann.kind == A10Val.Kind.map)
                    {
                        modAnnBytes = ann.bytes;
                        modAnnCount = ann.count;
                    }
                }
            }
        }
    }
    // Copy the modified-state annotation bytes out of a10ReadFrame's shared TLS
    // `buf`: the settle loop below parks in a10RequeueAnn/a10Requeue/a10Reject
    // (data-plane hop) and re-parses modAnnBytes on later iterations; a sibling
    // connection's a10ReadFrame would refill `buf` during a park -> cross-client
    // annotation corruption. (dispScratch is already connection-scoped for the
    // same reason; modAnnBytes was the missed sibling.)
    ByteBuffer modAnnCopy;
    if (modAnnBytes.length)
    {
        modAnnCopy.append(modAnnBytes);
        modAnnBytes = cast(const(ubyte)[]) modAnnCopy.data;
    }
    // first/last are client-controlled: iterating the raw numeric span would
    // spin the shard loop over up to 2^64 ids probing the AA. Snapshot the
    // unsettled SET filtered to [first,last] and walk that (bounded by real
    // in-flight deliveries) — same settle result, no unbounded loop.
    // Connection-scoped, NOT a TLS static: the foreach below parks in
    // a10Requeue/a10Reject (data-plane hop), and a static shared across every
    // connection's read fiber could be reset/realloc'd under a parked fiber ->
    // cross-client settle/requeue corruption. One read fiber per connection, so
    // c.dispScratch is stable across the park within this connection.
    c.dispScratch.length = 0;
    try
    {
        foreach (uid, ref uo; ps.unsettled)
            if (uid >= first && uid <= last)
                c.dispScratch ~= uid;
    }
    catch (Exception)
    {
    }
    foreach (id; c.dispScratch)
    {
        auto po = id in ps.unsettled;
        if (po is null)
            continue;
        if (po.stream)
        {
            // stream consumers never mutate the log: accepted/released/
            // rejected all just settle the delivery
            try
                ps.unsettled.remove(id);
            catch (Exception)
            {
            }
            continue;
        }
        switch (state)
        {
        case 0x27: // modified: requeue, splicing any annotations into headers
            if (modAnnBytes.length)
            {
                import dreads.amqp : a10RequeueAnn;

                static ByteBuffer annTbl; // TLS: consumed synchronously
                annTbl.clear();
                auto md2 = A10Dec(modAnnBytes);
                foreach (mi; 0 .. modAnnCount / 2)
                {
                    auto k6 = md2.readValue();
                    immutable vAt6 = md2.i;
                    auto v6 = md2.readValue();
                    auto raw6 = modAnnBytes[vAt6 .. md2.i];
                    if (!md2.ok || k6.kind != A10Val.Kind.str || k6.bytes.length > 127)
                        break;
                    annTbl.appendByte(cast(char) k6.bytes.length);
                    annTbl.append(cast(const(char)[]) k6.bytes);
                    if (v6.kind == A10Val.Kind.str)
                    {
                        annTbl.appendByte('S');
                        a10PutU32(annTbl, cast(uint) v6.bytes.length);
                        annTbl.append(cast(const(char)[]) v6.bytes);
                    }
                    else if (v6.kind == A10Val.Kind.u64 || v6.kind == A10Val.Kind.i64)
                    {
                        annTbl.appendByte('l');
                        immutable lv6 = v6.kind == A10Val.Kind.u64 ? cast(long) v6.u : v6.i;
                        foreach (k7; 0 .. 8)
                            annTbl.appendByte(cast(char)(lv6 >> ((7 - k7) * 8)));
                    }
                    else if (v6.kind == A10Val.Kind.boolean)
                    {
                        annTbl.appendByte('t');
                        annTbl.appendByte(v6.b ? 1 : 0);
                    }
                    else
                    {
                        // ANY other 1.0 value: keep the RAW encoding ('x'
                        // byte-array header) for a lossless redelivery
                        annTbl.appendByte('x');
                        a10PutU32(annTbl, cast(uint) raw6.length);
                        annTbl.append(cast(const(char)[]) raw6);
                    }
                }
                // modified deliveries count: splice the reserved marker so
                // the redelivery carries delivery-count=1 (released doesn't)
                annTbl.appendByte(cast(char) 8);
                annTbl.append("x-a10-dc");
                annTbl.appendByte('t');
                annTbl.appendByte(1);
                a10RequeueAnn(po.queue, po.blob, cast(const(ubyte)[]) annTbl.data,
                        !modUndeliverable);
            }
            else if (modUndeliverable)
                a10Reject(po.queue, po.blob); // undeliverable-here: dead-letter
            else
            {
                import dreads.amqp : a10RequeueAnn;

                static ByteBuffer dcTbl; // TLS
                dcTbl.clear();
                dcTbl.appendByte(cast(char) 8);
                dcTbl.append("x-a10-dc");
                dcTbl.appendByte('t');
                dcTbl.appendByte(1);
                a10RequeueAnn(po.queue, po.blob, cast(const(ubyte)[]) dcTbl.data);
            }
            break;
        case 0x26: // released
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

/// Declared-arguments fingerprint + queue type per queue (shard-local: the
/// managing connection's shard sees its own declares; cross-shard redeclare
/// equivalence rides the replicated meta for the KNOWN args).
private ulong[string] gA10ArgsHash; // TLS
private string[string] gA10QueueType; // TLS
private immutable(ubyte)[][string] gA10ArgsRaw; // TLS: declared args map CONTENTS
private uint[string] gA10ArgsCount; // TLS: element count of that map

private ulong a10Fnv(scope const(ubyte)[] b) @nogc nothrow
{
    ulong h = 0xCBF29CE484222325;
    foreach (x; b)
    {
        h ^= x;
        h *= 0x100000001B3;
    }
    return h;
}

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

/// Convert a 1.0 map's CONTENTS into a 0-9-1 field table (str/long/bool/
/// double mapped; anything else RAW as 'x').
private void a10MapToTable(scope const(ubyte)[] mapBytes, uint count,
        ref ByteBuffer tbl) nothrow @trusted
{
    auto md = A10Dec(mapBytes);
    foreach (mi; 0 .. count / 2)
    {
        auto k2 = md.readValue();
        immutable vAt = md.i;
        auto v2 = md.readValue();
        auto raw = mapBytes[vAt .. md.i];
        if (!md.ok || k2.kind != A10Val.Kind.str || k2.bytes.length > 127)
            break;
        tbl.appendByte(cast(char) k2.bytes.length);
        tbl.append(cast(const(char)[]) k2.bytes);
        if (v2.kind == A10Val.Kind.str)
        {
            tbl.appendByte('S');
            a10PutU32(tbl, cast(uint) v2.bytes.length);
            tbl.append(cast(const(char)[]) v2.bytes);
        }
        else if (v2.kind == A10Val.Kind.u64 || v2.kind == A10Val.Kind.i64)
        {
            immutable lv = v2.kind == A10Val.Kind.u64 ? cast(long) v2.u : v2.i;
            // an INT-encoded value (0x71 int / 0x54 smallint) keeps 32-bit
            // identity through the 0-9-1 'I' type, so a 1.0 consumer gets an
            // Integer back, not a Long (filter-expression round-trip)
            if (raw.length && (raw[0] == 0x71 || raw[0] == 0x54)
                    && lv >= int.min && lv <= int.max)
            {
                tbl.appendByte('I');
                foreach (k3; 0 .. 4)
                    tbl.appendByte(cast(char)(lv >> ((3 - k3) * 8)));
            }
            else
            {
                tbl.appendByte('l');
                foreach (k3; 0 .. 8)
                    tbl.appendByte(cast(char)(lv >> ((7 - k3) * 8)));
            }
        }
        else if (v2.kind == A10Val.Kind.boolean)
        {
            tbl.appendByte('t');
            tbl.appendByte(v2.b ? 1 : 0);
        }
        else
        {
            tbl.appendByte('x');
            a10PutU32(tbl, cast(uint) raw.length);
            tbl.append(cast(const(char)[]) raw);
        }
    }
}

/// Convert a 0-9-1 field table back to a 1.0 MAP value (encoded with
/// constructor) — the inverse of a10MapToTable for management listings.
private void a10TableToMap(scope const(ubyte)[] tbl, ref ByteBuffer o) nothrow @trusted
{
    import dreads.amqp : tableWalk;

    o.appendByte(0xD1); // map32
    immutable szAt = o.length;
    a10PutU32(o, 0);
    immutable cntAt = o.length;
    a10PutU32(o, 0);
    uint n2 = 0;
    if (tbl !is null && tbl.length)
        cast(void) tableWalk(tbl, (scope const(char)[] k, char ty,
                scope const(ubyte)[] v) nothrow {
            if (ty == 'S')
            {
                a10Str(o, k);
                a10Str(o, cast(const(char)[]) v);
                n2 += 2;
            }
            else if (ty == 'I' && v.length == 4)
            {
                int iv = 0;
                foreach (b3; v)
                    iv = (iv << 8) | b3;
                a10Str(o, k);
                o.appendByte(0x71); // int
                foreach (k3; 0 .. 4)
                    o.appendByte(cast(char)(iv >> ((3 - k3) * 8)));
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
            else if (ty == 't' && v.length == 1)
            {
                a10Str(o, k);
                a10Bool(o, v[0] != 0);
                n2 += 2;
            }
            else if (ty == 'x')
            {
                a10Str(o, k);
                o.append(cast(const(char)[]) v);
                n2 += 2;
            }
            return true;
        });
    a10PatchU32(o, szAt, cast(uint)(o.length - cntAt));
    a10PatchU32(o, cntAt, n2);
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
        ubyte flags, long msgs) nothrow
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
    {
        const(char)[] qt = "classic";
        try
            if (auto pt2 = (cast(string) name) in gA10QueueType)
                qt = *pt2;
        catch (Exception)
        {
        }
        a10Str(o, qt);
    }
    n2 += 2;
    a10Str(o, "arguments");
    {
        const(ubyte)[] raw9;
        uint cnt9;
        try
        {
            if (auto pr9 = (cast(string) name) in gA10ArgsRaw)
                raw9 = *pr9;
            if (auto pc9 = (cast(string) name) in gA10ArgsCount)
                cnt9 = *pc9;
        }
        catch (Exception)
        {
        }
        if (raw9.length)
        {
            o.appendByte(0xD1); // map32: declared args echoed verbatim
            a10PutU32(o, cast(uint)(raw9.length + 4));
            a10PutU32(o, cnt9);
            o.append(cast(const(char)[]) raw9);
        }
        else
        {
            o.appendByte(0xC1); // empty map8
            o.appendByte(1);
            o.appendByte(0);
        }
    }
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
    {
        import dreads.amqp : a10ConsumerCount;

        a10UInt(o, a10ConsumerCount(name));
    }
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
            bool ttlSet, expSet, mlSet, dlxSet;
            long ttlV, expV, mlV;
            const(char)[] dlx, dlrk;
            {
                // RabbitMQ 4 v2 queues are DURABLE BY DEFAULT: only an
                // explicit durable=false clears the bit
                auto dv = a10MapGet(bodyMapBytes, bodyMapCount, "durable");
                if (!(dv.kind == A10Val.Kind.boolean && !dv.b))
                    flags |= 2;
            }
            if (bodyIsMap)
            {
                if (boolOf(a10MapGet(bodyMapBytes, bodyMapCount, "exclusive")))
                    flags |= 4;
                if (boolOf(a10MapGet(bodyMapBytes, bodyMapCount, "auto_delete")))
                    flags |= 8;
                auto args = a10MapGet(bodyMapBytes, bodyMapCount, "arguments");
                if (args.kind == A10Val.Kind.map)
                {
                    static bool numOf(A10Val v, out long outv) @nogc nothrow
                    {
                        if (v.kind == A10Val.Kind.u64)
                        {
                            outv = cast(long) v.u;
                            return true;
                        }
                        if (v.kind == A10Val.Kind.i64)
                        {
                            outv = v.i;
                            return true;
                        }
                        return false;
                    }

                    ttlSet = numOf(a10MapGet(args.bytes, args.count,
                            "x-message-ttl"), ttlV);
                    expSet = numOf(a10MapGet(args.bytes, args.count,
                            "x-expires"), expV);
                    mlSet = numOf(a10MapGet(args.bytes, args.count,
                            "x-max-length"), mlV);
                    dlx = strOf(a10MapGet(args.bytes, args.count,
                            "x-dead-letter-exchange"));
                    dlxSet = dlx !is null;
                    dlrk = strOf(a10MapGet(args.bytes, args.count,
                            "x-dead-letter-routing-key"));
                }
            }
            // argument validation: unknown x-* argument names are refused
            // (RabbitMQ 4 validates them)
            ulong argsHash = 0;
            const(char)[] qType = "classic";
            {
                auto args = a10MapGet(bodyMapBytes, bodyMapCount, "arguments");
                if (args.kind == A10Val.Kind.map)
                {
                    argsHash = a10Fnv(args.bytes);
                    const(char)[] badArg;
                    auto ad2 = A10Dec(args.bytes);
                    foreach (mi; 0 .. args.count / 2)
                    {
                        auto k8 = ad2.readValue();
                        auto v8 = ad2.readValue();
                        if (!ad2.ok || k8.kind != A10Val.Kind.str)
                            break;
                        auto kn8 = cast(const(char)[]) k8.bytes;
                        if (kn8 == "x-queue-type")
                        {
                            auto tv = cast(const(char)[]) v8.bytes;
                            if (v8.kind == A10Val.Kind.str && tv.length)
                                qType = tv;
                        }
                        else if (kn8.length >= 2 && kn8[0] == 'x' && kn8[1] == '-'
                                && kn8 != "x-message-ttl" && kn8 != "x-expires"
                                && kn8 != "x-max-length" && kn8 != "x-max-length-bytes"
                                && kn8 != "x-dead-letter-exchange"
                                && kn8 != "x-dead-letter-routing-key"
                                && kn8 != "x-single-active-consumer"
                                && kn8 != "x-overflow" && kn8 != "x-delivery-limit"
                                && kn8 != "x-max-age" && kn8 != "x-initial-cluster-size"
                                && kn8 != "x-quorum-initial-group-size")
                            badArg = kn8;
                    }
                    // per-type validity (RabbitMQ 4): x-max-age is
                    // stream-only; dead-lettering is NOT a stream feature
                    if (!badArg.length)
                    {
                        auto chk = A10Dec(args.bytes);
                        foreach (mi2; 0 .. args.count / 2)
                        {
                            auto k9 = chk.readValue();
                            chk.skipValue();
                            if (!chk.ok || k9.kind != A10Val.Kind.str)
                                break;
                            auto kn9 = cast(const(char)[]) k9.bytes;
                            if (kn9 == "x-max-age" && qType != "stream")
                                badArg = kn9;
                            else if ((kn9 == "x-dead-letter-exchange"
                                    || kn9 == "x-dead-letter-routing-key")
                                    && qType == "stream")
                                badArg = kn9;
                        }
                    }
                    if (badArg.length)
                    {
                        char[300] bb2 = void;
                        import core.stdc.stdio : snprintf;

                        immutable bn2 = snprintf(bb2.ptr, bb2.length,
                                "invalid argument '%.*s' for queue",
                                cast(int) badArg.length, badArg.ptr);
                        a10MgmtRespond(c, fchan, "409", corrRaw, 2, null,
                                bb2[0 .. a10ClampN(bn2, bb2.length)]);
                        return;
                    }
                }
            }
            // exclusive queues belong to ONE connection: a second create of
            // the same name from another conn is a 405 RESOURCE_LOCKED
            if (a10QueueExists(qn))
            {
                import dreads.amqp : a10ExclusiveOwner;

                immutable owner = a10ExclusiveOwner(qn);
                if (owner != 0 && owner != c.connId)
                {
                    a10MgmtRespond(c, fchan, "405", corrRaw, 2, null,
                            "cannot obtain exclusive access to locked queue - "
                            ~ "the exclusive property value does not match that "
                            ~ "of the original declaration");
                    return;
                }
            }
            // redeclare with DIFFERENT flags/args is a 409 conflict (the
            // client-named retry flow depends on it)
            if (a10QueueExists(qn))
            {
                ulong prevHash = 0;
                try
                    if (auto ph = (cast(string) qn) in gA10ArgsHash)
                        prevHash = *ph;
                catch (Exception)
                {
                }
                if (prevHash != argsHash)
                {
                    a10MgmtRespond(c, fchan, "409", corrRaw, 2, null,
                            "inequivalent arguments");
                    return;
                }
                import dreads.amqp : a10QueueMetaGet;

                bool sTtl, sExp, sDlx;
                long sTtlV, sExpV, sMlEnc;
                const(char)[] sDlxN, sDlrk;
                a10QueueMetaGet(qn, sTtl, sTtlV, sExp, sExpV, sMlEnc, sDlx,
                        sDlxN, sDlrk);
                immutable mismatch = a10QueueFlags(qn) != (flags & 0x0E)
                    || sTtl != ttlSet || (sTtl && sTtlV != ttlV)
                    || sExp != expSet || (sExp && sExpV != expV)
                    || sMlEnc != (mlSet ? mlV + 1 : 0)
                    || sDlx != dlxSet || (sDlx && sDlxN != dlx)
                    || sDlrk != (dlrk is null ? "" : dlrk);
                if (mismatch)
                {
                    a10MgmtRespond(c, fchan, "409", corrRaw, 2, null,
                            "inequivalent arguments");
                    return;
                }
            }
            immutable created = a10DeclareQueue(qn, flags, ttlSet, ttlV,
                    expSet, expV, mlSet, mlV, dlx, dlxSet,
                    dlrk is null ? "" : dlrk);
            if (created && (flags & 4))
            {
                import dreads.amqp : a10ClaimExclusive;

                a10ClaimExclusive(qn, c.connId); // op-10, like 0-9-1 declares
            }
            try
            {
                auto qk9 = cast(string) qn.idup;
                gA10ArgsHash[qk9] = argsHash;
                gA10QueueType[qk9] = cast(string) qType.idup;
                auto args9 = a10MapGet(bodyMapBytes, bodyMapCount, "arguments");
                if (args9.kind == A10Val.Kind.map)
                {
                    gA10ArgsRaw[qk9] = args9.bytes.idup;
                    gA10ArgsCount[qk9] = args9.count;
                }
            }
            catch (Exception)
            {
            }
            bodyOut.clear();
            a10QueueInfoMap(bodyOut, qn, flags, a10QueueLen(qn));
            a10MgmtRespond(c, fchan, created ? "201" : "200", corrRaw, 1,
                    cast(const(ubyte)[]) bodyOut.data, "");
            return;
        }
        if (subject == "DELETE")
        {
            if (!a10QueueExists(qn))
            {
                char[600] eb2 = void;
                import core.stdc.stdio : snprintf;

                immutable en2 = snprintf(eb2.ptr, eb2.length,
                        "no queue '%.*s' in vhost '/'", cast(int) qn.length, qn.ptr);
                a10MgmtRespond(c, fchan, "404", corrRaw, 2, null,
                        eb2[0 .. a10ClampN(en2, eb2.length)]);
                return;
            }
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
                        eb[0 .. a10ClampN(en, eb.length)]);
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
            {
                import dreads.amqp : a10ExchangeExists, a10ExchangeType;

                if (a10ExchangeExists(xn) && a10ExchangeType(xn) != typ)
                {
                    a10MgmtRespond(c, fchan, "409", corrRaw, 2, null,
                            "inequivalent arguments");
                    return;
                }
            }
            a10DeclareExchange(xn, typ, flags, "");
            a10MgmtRespond(c, fchan, "204", corrRaw, 0, null, "");
            return;
        }
        if (subject == "DELETE")
        {
            debug (a10wire)
            {
                import core.stdc.stdio : fprintf, stderr;
                fprintf(stderr, "A10 MGMT DELETE-X '%.*s'\n", cast(int) xn.length, xn.ptr);
            }
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
        static ByteBuffer bArgs; // TLS: consumed by the broadcast synchronously
        bArgs.clear();
        {
            auto am = a10MapGet(bodyMapBytes, bodyMapCount, "arguments");
            if (am.kind == A10Val.Kind.map && am.count)
                a10MapToTable(am.bytes, am.count, bArgs);
        }
        if (src.length && (dq.length || dx.length))
            a10Bind(src, dq.length ? dq : dx, key, dx.length != 0,
                    cast(const(ubyte)[]) bArgs.data);
        a10MgmtRespond(c, fchan, "204", corrRaw, 0, null, "");
        return;
    }
    else if (to.length > 10 && to[0 .. 10] == "/bindings?" && subject == "GET")
    {
        // /bindings?src=S&dstq|dste=D&key=K -> 200 list of
        // {binding_key, arguments, location}
        auto spec = to[10 .. $];
        const(char)[] src, dst, key;
        bool dstIsX = false;
        size_t i3 = 0;
        while (i3 < spec.length)
        {
            size_t amp = spec.length;
            foreach (k5, ch8; spec[i3 .. $])
                if (ch8 == '&')
                {
                    amp = i3 + k5;
                    break;
                }
            auto part = spec[i3 .. amp];
            i3 = amp < spec.length ? amp + 1 : spec.length;
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
        char[256] sb3 = void, db3 = void, kb3 = void;
        auto srcD = a10UriDecode(src, sb3);
        auto dstD = a10UriDecode(dst, db3);
        auto keyD = a10UriDecode(key, kb3);
        bodyOut.clear();
        // amqp-value LIST of binding maps
        bodyOut.appendByte(0xD0);
        immutable lszAt = bodyOut.length;
        a10PutU32(bodyOut, 0);
        immutable lcntAt = bodyOut.length;
        a10PutU32(bodyOut, 0);
        uint ln2 = 0;
        {
            import dreads.amqp : a10ListBindings;

            a10ListBindings(srcD, dstD, dstIsX,
                    (scope const(char)[] bkey, scope const(ubyte)[] bargs) nothrow {
                if (keyD.length && bkey != keyD)
                    return;
                // one map: binding_key, arguments, location
                bodyOut.appendByte(0xD1);
                immutable mszAt = bodyOut.length;
                a10PutU32(bodyOut, 0);
                immutable mcntAt = bodyOut.length;
                a10PutU32(bodyOut, 0);
                a10Str(bodyOut, "binding_key");
                a10Str(bodyOut, bkey);
                a10Str(bodyOut, "arguments");
                a10TableToMap(bargs, bodyOut);
                a10Str(bodyOut, "location");
                {
                    char[900] loc = void;
                    import core.stdc.stdio : snprintf;

                    immutable lnn = snprintf(loc.ptr, loc.length,
                            "/bindings/src=%.*s;%s=%.*s;key=%.*s;args=",
                            cast(int) src.length, src.ptr,
                            dstIsX ? "dste".ptr : "dstq".ptr,
                            cast(int) dst.length, dst.ptr,
                            cast(int) key.length, key.ptr);
                    a10Str(bodyOut, loc[0 .. a10ClampN(lnn, loc.length)]);
                }
                a10PatchU32(bodyOut, mszAt, cast(uint)(bodyOut.length - mcntAt));
                a10PatchU32(bodyOut, mcntAt, 6);
                ln2++;
            });
        }
        a10PatchU32(bodyOut, lszAt, cast(uint)(bodyOut.length - lcntAt));
        a10PatchU32(bodyOut, lcntAt, ln2);
        a10MgmtRespond(c, fchan, "200", corrRaw, 1,
                cast(const(ubyte)[]) bodyOut.data, "");
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
private void a10BuildMessage(scope const(ubyte)[] blob, ref ByteBuffer o,
        long streamOff = -1) nothrow @trusted
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

    // delivery-count rule: released requeues do NOT bump it; modified
    // requeues (x-a10-dc marker) and dead-letter hops (deaths>0) do
    int hdrDeliveryCount0 = 0;
    {
        import dreads.amqp : splitRecord;

        long pmh;
        int dh;
        const(char)[] rkh;
        const(ubyte)[] ph, bh;
        splitRecord(blob, pmh, dh, rkh, ph, bh);
        if (dh > 0)
            hdrDeliveryCount0 = 1; // ONLY dead-letter hops count in the header
    }
    // header section (delivery-count: the 1.0 client asserts it on the DLQ
    // side and on modified redeliveries)
    {
        immutable hdrDeliveryCount = hdrDeliveryCount0;
        auto hl = a10OpenPerf(o, cast(ubyte) SEC_HEADER);
        a10Bool(o, deliveryMode == 2); // durable
        hl.n++;
        if (priority)
        {
            o.appendByte(0x50); // ubyte
            o.appendByte(cast(char) priority);
        }
        else
            a10Null(o);
        hl.n++;
        if (hdrDeliveryCount)
        {
            a10Null(o); // ttl
            hl.n++;
            a10Null(o); // first-acquirer
            hl.n++;
            a10UInt(o, cast(uint) hdrDeliveryCount);
            hl.n++;
        }
        a10Close(o, hl);
    }
    // (x-delivery-count joins the single annotations section below — two
    // message-annotations sections are invalid and the client keeps the last)
    // The ANNOTATION appears on ANY redelivery (released included); the
    // header delivery-count above counts only modified requeues + DLX hops.
    bool redelivered;
    {
        import dreads.amqp : recordRedelivered;

        redelivered = recordRedelivered(blob) || hdrDeliveryCount0 > 0;
    }
    // properties section: when the message was published via 1.0, the WHOLE
    // original section rides the reserved x-a10-props header — replay it
    // VERBATIM (lossless: numeric correlation-ids, user-id, timestamps,
    // group-* all survive; the filter expressions depend on it)
    const(ubyte)[] propsRaw;
    {
        auto hdrsP = propsHeaders(props);
        if (hdrsP !is null && hdrsP.length)
            cast(void) tableWalk(hdrsP, (scope const(char)[] k, char ty,
                    scope const(ubyte)[] v) nothrow {
                if (k == "x-a10-props" && ty == 'x')
                {
                    propsRaw = v;
                    return false;
                }
                return true;
            });
    }
    const(ubyte)[] midRaw;
    auto hdrs0 = propsHeaders(props);
    if (hdrs0 !is null && hdrs0.length)
        cast(void) tableWalk(hdrs0, (scope const(char)[] k, char ty,
                scope const(ubyte)[] v) nothrow {
            if (k == "x-a10-mid" && ty == 'x')
            {
                midRaw = v;
                return false;
            }
            return true;
        });
    const(char)[] subj;
    if (hdrs0 !is null && hdrs0.length)
        cast(void) tableWalk(hdrs0, (scope const(char)[] k, char ty,
                scope const(ubyte)[] v) nothrow {
            if (k == "x-a10-subj" && ty == 'S')
            {
                subj = cast(const(char)[]) v;
                return false;
            }
            return true;
        });
    if (propsRaw.length)
    {
        o.appendByte(0x00);
        a10SmallUlong(o, cast(ubyte) SEC_PROPERTIES);
        o.append(cast(const(char)[]) propsRaw); // the original section, verbatim
    }
    else if (contentType.length || correlationId.length || replyTo.length
            || midRaw.length || subj.length)
    {
        auto pl = a10OpenPerf(o, cast(ubyte) SEC_PROPERTIES);
        if (midRaw.length)
            o.append(cast(const(char)[]) midRaw); // raw 1.0 value re-emit
        else
            a10Null(o); // message-id
        pl.n++;
        a10Null(o); // user-id
        pl.n++;
        a10Null(o); // to
        pl.n++;
        if (subj.length)
            a10Str(o, subj);
        else
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
    // x-* headers re-emit as MESSAGE ANNOTATIONS (1.0 convention: annotation
    // keys are x-prefixed symbols); everything else is application-properties
    auto hdrs = propsHeaders(props);
    {
        bool anyX = redelivered;
        if (hdrs !is null && hdrs.length)
            cast(void) tableWalk(hdrs, (scope const(char)[] k, char ty,
                    scope const(ubyte)[] v) nothrow {
                if (k.length >= 2 && k[0] == 'x' && k[1] == '-' && k != "x-death")
                    anyX = true;
                return true;
            });
        if (anyX || streamOff >= 0)
        {
            o.appendByte(0x00);
            a10SmallUlong(o, cast(ubyte) SEC_MESSAGE_ANN);
            o.appendByte(0xD1); // map32
            immutable szX = o.length;
            a10PutU32(o, 0);
            immutable cntX = o.length;
            a10PutU32(o, 0);
            uint nx = 0;
            if (streamOff >= 0)
            {
                a10Sym(o, "x-stream-offset");
                o.appendByte(0x81); // long
                foreach (k9b; 0 .. 8)
                    o.appendByte(cast(char)(streamOff >> ((7 - k9b) * 8)));
                nx += 2;
            }
            if (redelivered)
            {
                a10Sym(o, "x-delivery-count");
                o.appendByte(0x55); // smalllong: the client asserts a Long
                o.appendByte(1);
                nx += 2;
            }
            if (hdrs !is null && hdrs.length)
            cast(void) tableWalk(hdrs, (scope const(char)[] k, char ty,
                    scope const(ubyte)[] v) nothrow {
                if (!(k.length >= 2 && k[0] == 'x' && k[1] == '-') || k == "x-death"
                        || k == "x-a10-mid" || k == "x-a10-subj" || k == "x-a10-dc"
                        || k == "x-a10-props")
                    return true;
                if (ty == 'S')
                {
                    a10Sym(o, k);
                    a10Str(o, cast(const(char)[]) v);
                    nx += 2;
                }
                else if ((ty == 'l' || ty == 'T') && v.length == 8)
                {
                    long lv = 0;
                    foreach (b3; v)
                        lv = (lv << 8) | b3;
                    a10Sym(o, k);
                    o.appendByte(0x81);
                    foreach (k3; 0 .. 8)
                        o.appendByte(cast(char)(lv >> ((7 - k3) * 8)));
                    nx += 2;
                }
                else if (ty == 't' && v.length == 1)
                {
                    a10Sym(o, k);
                    a10Bool(o, v[0] != 0);
                    nx += 2;
                }
                else if (ty == 'x')
                {
                    a10Sym(o, k);
                    o.append(cast(const(char)[]) v); // raw 1.0 value re-emit
                    nx += 2;
                }
                return true;
            });
            a10PatchU32(o, szX, cast(uint)(o.length - cntX));
            a10PatchU32(o, cntX, nx);
        }
    }
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
            if (k.length >= 2 && k[0] == 'x' && k[1] == '-')
                return true; // x-* went out as message annotations
            if (ty == 'S')
            {
                a10Str(o, k);
                a10Str(o, cast(const(char)[]) v);
                n2 += 2;
            }
            else if (ty == 'd' && v.length == 8)
            {
                a10Str(o, k);
                o.appendByte(0x82); // double
                foreach (k3; 0 .. 8)
                    o.appendByte(cast(char) v[k3]);
                n2 += 2;
            }
            else if (ty == 'x')
            {
                a10Str(o, k);
                o.append(cast(const(char)[]) v); // raw 1.0 value re-emit
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

/// Refuse an attach: echo it with NULL terminus, then detach(closed) with an
/// error condition — the 1.0 not-found pattern the java client maps to
/// AmqpEntityDoesNotExistException.
private void a10RefuseAttach(A10Conn c, ushort fchan, ref A10Link lk,
        scope const(char)[] condition, scope const(char)[] entity) nothrow
{
    ByteBuffer o;
    {
        auto f = a10FrameStart(o, FRAME_TYPE_AMQP, fchan);
        auto l = a10OpenPerf(o, cast(ubyte) PERF_ATTACH);
        a10Str(o, lk.name);
        l.n++;
        a10UInt(o, lk.handle);
        l.n++;
        a10Bool(o, lk.clientSender);
        l.n++;
        a10Null(o); // snd
        l.n++;
        a10Null(o); // rcv
        l.n++;
        a10Null(o); // source: NULL = not granted
        l.n++;
        a10Null(o); // target
        l.n++;
        if (!lk.clientSender)
        {
            // we'd be the SENDER: proton-j drops an attach missing the
            // initial-delivery-count and then calls our detach uncorrelated
            a10Null(o); // unsettled
            l.n++;
            a10Null(o); // incomplete-unsettled
            l.n++;
            a10UInt(o, 0); // initial-delivery-count
            l.n++;
        }
        a10Close(o, l);
        a10FrameFinish(o, f);
    }
    a10SendDetachError(c, fchan, lk.handle, condition, entity, &o);
    a10Send(c, cast(const(ubyte)[]) o.data);
}

private void a10SendDetachError(A10Conn c, ushort fchan, uint handle,
        scope const(char)[] condition, scope const(char)[] entity,
        ByteBuffer* stage = null) nothrow
{
    debug (a10wire)
    {
        import core.stdc.stdio : fprintf, stderr;
        fprintf(stderr, "A10 DETACH-ERR ch=%u h=%u cond='%.*s' ent='%.*s'\n",
                cast(uint) fchan, handle, cast(int) condition.length, condition.ptr,
                cast(int) entity.length, entity.ptr);
    }
    ByteBuffer local;
    ByteBuffer* o = stage !is null ? stage : &local;
    {
        auto f = a10FrameStart(*o, FRAME_TYPE_AMQP, fchan);
        auto l = a10OpenPerf(*o, cast(ubyte) PERF_DETACH);
        a10UInt(*o, handle);
        l.n++;
        a10Bool(*o, true); // closed
        l.n++;
        {
            auto el = a10OpenPerf(*o, cast(ubyte) DESC_ERROR);
            a10Sym(*o, condition);
            el.n++;
            char[600] eb = void;
            import core.stdc.stdio : snprintf;

            immutable en = snprintf(eb.ptr, eb.length,
                    "no queue '%.*s' in vhost '/'", cast(int) entity.length,
                    entity.ptr);
            a10Str(*o, eb[0 .. en]);
            el.n++;
            a10Close(*o, el);
        }
        l.n++;
        a10Close(*o, l);
        a10FrameFinish(*o, f);
    }
    if (stage is null)
        a10Send(c, cast(const(ubyte)[]) local.data);
}

/// Delivery fiber for one client-receiver link: pops from the source queue
/// while the client has granted credit, sends transfers, tracks unsettled.
/// Parse the attach's source filter-set into the link's stream/filter fields.
/// Layout: map { symbol -> described(symbol, value) }.
private void a10ParseFilters(A10Link* lk) nothrow @trusted
{
    if (lk.srcFilterRaw.length == 0)
        return;
    auto dec = A10Dec(lk.srcFilterRaw);
    auto m = dec.readValue();
    if (!dec.ok || m.kind != A10Val.Kind.map)
        return;
    auto md = A10Dec(m.bytes);
    foreach (_; 0 .. m.count / 2)
    {
        auto k = md.readValue();
        auto v = md.readValue();
        if (!md.ok || k.kind != A10Val.Kind.str)
            break;
        if (v.kind == A10Val.Kind.described)
            v = md.readValue(); // the described VALUE follows
        if (!md.ok)
            break;
        auto key = cast(const(char)[]) k.bytes;
        if (key == "rabbitmq:stream-offset-spec")
        {
            if (v.kind == A10Val.Kind.str)
            {
                auto sv = cast(const(char)[]) v.bytes;
                if (sv == "first")
                    lk.offKind = 1;
                else if (sv == "last")
                    lk.offKind = 2;
                else if (sv == "next")
                    lk.offKind = 3;
                else if (sv.length >= 2)
                {
                    // interval spec "<n><unit>": messages from the last n
                    // units (RabbitMQ stream offset intervals)
                    long n2 = 0;
                    bool digits = true;
                    foreach (ch; sv[0 .. $ - 1])
                        if (ch >= '0' && ch <= '9')
                            n2 = n2 * 10 + (ch - '0');
                        else
                            digits = false;
                    long unitMs = 0;
                    switch (sv[$ - 1])
                    {
                    case 's': unitMs = 1000; break;
                    case 'm': unitMs = 60_000; break;
                    case 'h': unitMs = 3_600_000; break;
                    case 'D': unitMs = 86_400_000; break;
                    case 'M': unitMs = 2_592_000_000L; break;
                    case 'Y': unitMs = 31_536_000_000L; break;
                    default: break;
                    }
                    if (digits && unitMs > 0)
                    {
                        lk.offKind = 6; // interval: now - n*unit
                        lk.offVal = n2 * unitMs;
                    }
                }
            }
            else if (v.kind == A10Val.Kind.u64)
            {
                lk.offKind = 4;
                lk.offVal = cast(long) v.u;
            }
            else if (v.kind == A10Val.Kind.i64)
            {
                // timestamps decode as i64 too; treat as offset only when
                // small, else as a timestamp spec (start from first: streams
                // here have no per-record retention clock)
                lk.offKind = v.i > 4_000_000_000L ? 5 : 4;
                lk.offVal = v.i;
            }
        }
        else if (key == "rabbitmq:stream-filter")
        {
            if (v.kind == A10Val.Kind.list || v.kind == A10Val.Kind.array)
            {
                auto ld = A10Dec(v.bytes);
                foreach (_2; 0 .. v.count)
                {
                    auto e = ld.readValue();
                    if (!ld.ok)
                        break;
                    if (e.kind == A10Val.Kind.str)
                        try
                            lk.streamFilterVals ~= (cast(const(char)[]) e.bytes).idup;
                        catch (Exception)
                        {
                        }
                }
            }
            else if (v.kind == A10Val.Kind.str)
                try
                    lk.streamFilterVals ~= (cast(const(char)[]) v.bytes).idup;
                catch (Exception)
                {
                }
        }
        else if (key == "rabbitmq:stream-match-unfiltered")
        {
            if (v.kind == A10Val.Kind.boolean)
                lk.matchUnfiltered = v.b;
        }
        else if (key == "amqp:properties-filter")
        {
            if (v.kind == A10Val.Kind.map)
            {
                lk.propFilterRaw = v.bytes.idup;
                lk.propFilterCount = v.count;
            }
        }
        else if (key == "amqp:application-properties-filter")
        {
            if (v.kind == A10Val.Kind.map)
            {
                lk.appFilterRaw = v.bytes.idup;
                lk.appFilterCount = v.count;
            }
        }
    }
    lk.hasFilters = lk.streamFilterVals.length > 0 || lk.propFilterCount > 0
        || lk.appFilterCount > 0;
}

/// Locate one section of a built bare message by descriptor code.
private bool a10FindSection(scope const(ubyte)[] msg, ulong code,
        out A10Val val) nothrow @trusted
{
    auto dec = A10Dec(msg);
    while (dec.ok && dec.i < msg.length)
    {
        auto d = dec.readValue();
        if (!dec.ok || d.kind != A10Val.Kind.described)
            return false;
        // the descriptor itself was consumed into d (kind described exposes
        // the descriptor's value in u for smallulong codes)
        auto v = dec.readValue();
        if (!dec.ok)
            return false;
        if (d.u == code)
        {
            val = v;
            return true;
        }
    }
    return false;
}

/// Compare a filter value against a message value (type-aware; string
/// modifiers "&p:" prefix, "&s:" suffix, "&&" literal-escape).
private bool a10FilterEq(const ref A10Val f, const ref A10Val v) nothrow @trusted
{
    final switch (f.kind)
    {
    case A10Val.Kind.str:
        if (v.kind != A10Val.Kind.str)
            return false;
        auto fb = cast(const(char)[]) f.bytes;
        auto vb = cast(const(char)[]) v.bytes;
        if (fb.length >= 3 && fb[0 .. 3] == "&p:")
            return vb.length >= fb.length - 3 && vb[0 .. fb.length - 3] == fb[3 .. $];
        if (fb.length >= 3 && fb[0 .. 3] == "&s:")
            return vb.length >= fb.length - 3
                && vb[$ - (fb.length - 3) .. $] == fb[3 .. $];
        if (fb.length >= 2 && fb[0 .. 2] == "&&")
            return vb == fb[1 .. $];
        return vb == fb;
    case A10Val.Kind.u64:
        if (v.kind == A10Val.Kind.u64)
            return v.u == f.u;
        if (v.kind == A10Val.Kind.i64)
            return v.i >= 0 && cast(ulong) v.i == f.u;
        return false;
    case A10Val.Kind.i64:
        if (v.kind == A10Val.Kind.i64)
            return v.i == f.i;
        if (v.kind == A10Val.Kind.u64)
            return f.i >= 0 && v.u == cast(ulong) f.i;
        return false;
    case A10Val.Kind.boolean:
        return v.kind == A10Val.Kind.boolean && v.b == f.b;
    case A10Val.Kind.f64:
        return v.kind == A10Val.Kind.f64 && v.f == f.f;
    case A10Val.Kind.null_:
        return true; // null filter value: field presence not enforced
    case A10Val.Kind.list:
    case A10Val.Kind.map:
    case A10Val.Kind.array:
    case A10Val.Kind.described:
        return false;
    }
}

/// Does the BUILT message pass the link's VALUE filter (bloom family)?
private bool a10BloomHit(scope const(ubyte)[] msg, A10Link* lk) nothrow @trusted
{
    A10Val ann;
    const(char)[] fv;
    if (a10FindSection(msg, SEC_MESSAGE_ANN, ann) && ann.kind == A10Val.Kind.map)
    {
        auto ad = A10Dec(ann.bytes);
        foreach (_; 0 .. ann.count / 2)
        {
            auto k = ad.readValue();
            auto v = ad.readValue();
            if (!ad.ok)
                break;
            if (k.kind == A10Val.Kind.str
                    && cast(const(char)[]) k.bytes == "x-stream-filter-value"
                    && v.kind == A10Val.Kind.str)
            {
                fv = cast(const(char)[]) v.bytes;
                break;
            }
        }
    }
    if (fv is null || fv.length == 0)
        return lk.matchUnfiltered;
    foreach (want; lk.streamFilterVals)
        if (want == fv)
            return true;
    return false;
}

/// Does the BUILT message pass the link's EXPRESSION filters (properties +
/// application-properties, AND semantics)?
private bool a10ExprMatch(scope const(ubyte)[] msg, A10Link* lk) nothrow @trusted
{
    // properties filter: field-by-name against the properties list
    if (lk.propFilterCount)
    {
        A10Val props;
        if (!a10FindSection(msg, SEC_PROPERTIES, props)
                || props.kind != A10Val.Kind.list)
            return false;
        // extract the 13 property fields once
        A10Val[13] fields;
        {
            auto pd = A10Dec(props.bytes);
            foreach (fi; 0 .. (props.count < 13 ? props.count : 13))
            {
                fields[fi] = pd.readValue();
                if (!pd.ok)
                    break;
            }
        }
        auto fd = A10Dec(lk.propFilterRaw);
        foreach (_; 0 .. lk.propFilterCount / 2)
        {
            auto k = fd.readValue();
            auto v = fd.readValue();
            if (!fd.ok || k.kind != A10Val.Kind.str)
                return false;
            auto name = cast(const(char)[]) k.bytes;
            int idx = -1;
            switch (name)
            {
            case "message-id": idx = 0; break;
            case "user-id": idx = 1; break;
            case "to": idx = 2; break;
            case "subject": idx = 3; break;
            case "reply-to": idx = 4; break;
            case "correlation-id": idx = 5; break;
            case "content-type": idx = 6; break;
            case "content-encoding": idx = 7; break;
            case "absolute-expiry-time": idx = 8; break;
            case "creation-time": idx = 9; break;
            case "group-id": idx = 10; break;
            case "group-sequence": idx = 11; break;
            case "reply-to-group-id": idx = 12; break;
            default: break;
            }
            if (idx < 0)
                return false; // unknown property name: no match
            if (!a10FilterEq(v, fields[idx]))
                return false;
        }
    }
    // application-properties filter: key lookup in the app-props map
    if (lk.appFilterCount)
    {
        A10Val ap;
        if (!a10FindSection(msg, SEC_APP_PROPERTIES, ap)
                || ap.kind != A10Val.Kind.map)
            return false;
        auto fd = A10Dec(lk.appFilterRaw);
        foreach (_; 0 .. lk.appFilterCount / 2)
        {
            auto k = fd.readValue();
            auto v = fd.readValue();
            if (!fd.ok || k.kind != A10Val.Kind.str)
                return false;
            bool matched = false;
            auto ad = A10Dec(ap.bytes);
            foreach (_2; 0 .. ap.count / 2)
            {
                auto mk = ad.readValue();
                auto mv = ad.readValue();
                if (!ad.ok)
                    break;
                if (mk.kind == A10Val.Kind.str && mk.bytes == k.bytes)
                {
                    matched = a10FilterEq(v, mv);
                    break;
                }
            }
            if (!matched)
                return false;
        }
    }
    return true;
}

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
            scope (exit)
            {
                import dreads.amqp : a10ConsumerDec, a10PrioRemove;

                auto psx = ch5 in cc.sessions;
                if (psx !is null)
                    if (auto plx = h5 in psx.links)
                    {
                        a10ConsumerDec(plx.rkey);
                        a10PrioRemove(plx.rkey, plx.prio);
                    }
            }
            while (!cc.closing)
            {
                auto ps5 = ch5 in cc.sessions;
                if (ps5 is null)
                    return;
                auto pl5 = h5 in ps5.links;
                if (pl5 is null || pl5.detached)
                    return;
                if (pl5.v2Queue)
                {
                    import dreads.amqp : a10QueueExists;

                    if (!a10QueueExists(pl5.rkey))
                    {
                        // v2 semantics: a deleted source closes the link
                        a10SendDetachError(cc, ch5, h5, "amqp:resource-deleted",
                                pl5.rkey);
                        pl5.detached = true;
                        return;
                    }
                }
                if (pl5.outCredit == 0)
                {
                    debug (a10wire)
                    {
                        import core.stdc.stdio : fprintf, stderr;
                        static int thr;
                        if (thr++ % 4000 == 0)
                            fprintf(stderr, "A10 FIBER h=%u nocredit\n", h5);
                    }
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
                // Unsettled backpressure: a client granting huge credit and never
                // disposing would grow ps5.unsettled (each entry an idup'd blob)
                // without bound -> memory exhaustion. Stop delivering (don't pop or
                // burn credit) until it settles some deliveries. Popped-but-unsettled
                // messages are still safe (a10TeardownRequeue returns them on close).
                if (ps5.unsettled.length >= A10_MAX_UNSETTLED_PER_SESSION)
                {
                    try
                        sleep(1.msecs);
                    catch (Exception)
                        return;
                    continue;
                }
                {
                    // consumer priority: defer while a HIGHER-priority
                    // consumer is live on this queue (shared 0-9-1 registry)
                    import dreads.amqp : a10PrioMax;

                    if (pl5.prio < a10PrioMax(pl5.rkey))
                    {
                        try
                            sleep(1.msecs);
                        catch (Exception)
                            return;
                        continue;
                    }
                }
                // capture link identity BEFORE the fetch park: a concurrent
                // detach/attach during a10Pop/a10PeekAt can free/move the A10Link
                // that pl5 aliases (ps5.links is A10Link-by-value).
                immutable bool linkStream = pl5.stream;
                auto linkRkey = pl5.rkey;
                long streamOffThis = -1;
                pay.clear();
                bool got;
                if (pl5.stream)
                {
                    import dreads.amqp : a10PeekAt;

                    got = a10PeekAt(pl5.rkey, pl5.streamPos, pay);
                    if (got)
                    {
                        streamOffThis = pl5.streamPos;
                        pl5.streamPos++;
                        // bloom value filters run CHUNK-coarse (16 messages),
                        // like real stream bloom filters: a chunk with any
                        // hit ships whole (false positives by design — the
                        // client-side-filtering test asserts they exist).
                        // Partial tail chunks fall back to exact matching.
                        if (pl5.streamFilterVals.length)
                        {
                            enum long CHUNK = 16;
                            immutable chunk = streamOffThis / CHUNK;
                            bool pass;
                            if (chunk == pl5.bloomChunk)
                                pass = pl5.bloomPass;
                            else
                            {
                                ByteBuffer sb2, mb2;
                                bool full = true;
                                bool anyHit = false;
                                // scan the WHOLE chunk (no early-out): a
                                // partial tail chunk must be detected even
                                // when an early message hits, so the tail
                                // falls back to exact matching
                                foreach (k9c; 0 .. CHUNK)
                                {
                                    sb2.clear();
                                    if (!a10PeekAt(pl5.rkey, chunk * CHUNK + k9c, sb2))
                                    {
                                        full = false;
                                        break;
                                    }
                                    if (anyHit)
                                        continue; // fullness check only
                                    mb2.clear();
                                    a10BuildMessage(cast(const(ubyte)[]) sb2.data, mb2);
                                    if (a10BloomHit(cast(const(ubyte)[]) mb2.data, pl5))
                                        anyHit = true;
                                }
                                if (full)
                                {
                                    pl5.bloomChunk = chunk;
                                    pl5.bloomPass = anyHit;
                                    pass = anyHit;
                                }
                                else
                                {
                                    // partial tail chunk: exact per-message
                                    mb2.clear();
                                    a10BuildMessage(cast(const(ubyte)[]) pay.data, mb2);
                                    pass = a10BloomHit(cast(const(ubyte)[]) mb2.data, pl5);
                                }
                            }
                            if (!pass)
                                continue; // advance WITHOUT burning credit
                        }
                        if (pl5.propFilterCount || pl5.appFilterCount)
                        {
                            msg.clear();
                            a10BuildMessage(cast(const(ubyte)[]) pay.data, msg,
                                    streamOffThis);
                            if (!a10ExprMatch(cast(const(ubyte)[]) msg.data, pl5))
                                continue; // advance WITHOUT burning credit
                        }
                    }
                }
                else
                    got = a10Pop(pl5.rkey, pay);
                // Re-validate link+session after the fetch park: a DETACH/END the
                // serve loop processed during the hop removes/rehashes ps5.links
                // (freeing/moving the A10Link pl5 aliases) and already requeued
                // this link's unsettled set. Using the stale pl5 would
                // use-after-free, mutate the wrong link and transfer on a detached
                // handle; and a just-popped message would be stranded in unsettled
                // AFTER that requeue -> permanent loss. Refresh, or requeue+stop.
                {
                    auto ps5r = ch5 in cc.sessions;
                    auto pl5r = ps5r !is null ? h5 in ps5r.links : null;
                    if (cc.closing || pl5r is null || pl5r.detached
                            || pl5r.rkey != linkRkey)
                    {
                        if (got && !linkStream)
                        {
                            import dreads.amqp : a10Requeue;

                            a10Requeue(linkRkey, cast(const(ubyte)[]) pay.data);
                        }
                        return;
                    }
                    ps5 = ps5r;
                    pl5 = pl5r;
                }
                if (!got)
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
                debug (a10wire)
                {
                    import core.stdc.stdio : fprintf, stderr;
                    fprintf(stderr, "A10 DELIVER h=%u credit=%u pos=%lld\n", h5,
                            pl5.outCredit, pl5.streamPos);
                }
                // build transfer + message
                immutable did = ps5.nextOutgoingId++;
                pl5.deliveryCount++;
                pl5.outCredit--;
                try
                    ps5.unsettled[did] = A10Out(pl5.rkey,
                            (cast(const(ubyte)[]) pay.data).idup, h5, pl5.stream);
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
                a10BuildMessage(cast(const(ubyte)[]) pay.data, msg, streamOffThis);
                debug (a10wire)
                {
                    import core.stdc.stdio : fprintf, stderr;
                    auto md0 = cast(const(ubyte)[]) msg.data;
                    fprintf(stderr, "A10 MSGBYTES len=%zu: ", md0.length);
                    foreach (bx; md0[0 .. (md0.length < 96 ? md0.length : 96)])
                        fprintf(stderr, "%02x", bx);
                    fprintf(stderr, "\n");
                }
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
        if (c.tlsLeg !is null)
        {
            got += legTake(c.tlsLeg, dst[got .. $]);
            if (got == dst.length)
                break;
            // a10Send(null) flushes handshake cipher under the wlock
            if (!legPump(c.tlsLeg, c.tcp))
                return false;
            a10Send(c, null);
            if (c.closing)
                return false;
            continue;
        }
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
