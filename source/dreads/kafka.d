module dreads.kafka;

// Kafka frontend — the THIRD non-RESP skin over the sharded core, and the one
// whose model maps most naturally onto ours: a Kafka PARTITION is a LIST in
// the keyspace of the shard owning keyToSlot("kafka.t.<topic>.<p>") — so
// partitions spread across shards exactly like Kafka spreads them across
// brokers, offsets are list indices (RPUSH's :length reply assigns the offset
// ATOMICALLY on the owner), Fetch is an LRANGE, the high watermark is LLEN,
// and the log is durable via the per-shard AOF with zero Kafka-specific
// persistence code.
//
// Protocol strategy: we PIN pre-flexible, pre-RecordBatch API versions —
// ApiVersions v0, Metadata v1, Produce v2, Fetch v3, ListOffsets v1 — which
// pairs with the MessageSet v1 record format (magic 1, plain CRC-32): no
// varints, no CRC32C, no tagged fields. Real clients (kafka-python,
// librdkafka) negotiate down to these happily; they are the 0.10.x-broker
// dialect every client still speaks.
//
// v1 scope (documented): produce acks 0/1 (treated alike — the write IS
// applied on the owner before we reply), uncompressed messages only
// (compressed sets are rejected with CORRUPT_MESSAGE), manual-assignment
// consumers (no consumer-group APIs yet: FindCoordinator and friends come
// with v2), no log truncation (earliest offset is always 0), topics
// auto-exist with KAFKA_PARTITIONS partitions (metadata is STATELESS — no
// registry, no cross-shard control plane at all).

import vibe.core.net : TCPConnection;
import vibe.core.sync : TaskMutex;

import dreads.mem : ByteBuffer;
import std.digest.crc : crc32Of;

/// Data-plane hook installed by server.d: execute a synthesized RESP command
/// (args[1] is the routing key) on the owner shard, reply RESP bytes.
public __gshared void delegate(scope const(char)[][] args, ref ByteBuffer reply) nothrow gKafkaExec;

public enum uint KAFKA_PARTITIONS = 4; // partitions advertised per topic

private enum short API_PRODUCE = 0, API_FETCH = 1, API_LIST_OFFSETS = 2,
        API_METADATA = 3, API_API_VERSIONS = 18;

private enum short E_NONE = 0, E_CORRUPT = 2, E_UNKNOWN_TOPIC = 3,
        E_OFFSET_OUT_OF_RANGE = 1, E_UNSUPPORTED_VERSION = 35;

/// Hard bound on any wire array count (topics/partitions/records). Kafka
/// counts are SIGNED i32; a hostile 0x7FFFFFFF made the response-building
/// foreach run ~2.1e9 times, each iteration appending to the reply until the
/// allocator aborts the whole broker. A real request names a handful.
private enum int KAFKA_MAX_ARRAY = 65536;
/// Longest accepted topic name (Kafka's own limit is 249).
private enum size_t KAFKA_MAX_TOPIC = 249;
/// Trim response/input buffers back to this after a spike.
private enum size_t KAFKA_BUF_KEEP = 4 << 20;
/// Cap on records in ONE produce partition: bounds the crc32 work + the single
/// variadic RPUSH (a 64MB frame of 18-byte records = ~3.7M crc32 calls with no
/// yield = a cooperative CPU stall of the whole shard thread).
private enum int KAFKA_MAX_RECORDS = 1 << 20;
/// Absolute per-request response ceiling. safeCount bounds each array count,
/// but nparts×ntopics (65536×65536) repeated emission still builds a multi-GB
/// reply that aborts the allocator (broker death). Once the body passes this,
/// remaining partitions emit empty record sets.
private enum size_t KAFKA_MAX_RESP = 128 << 20;

/// Clamp a wire array count to a safe iteration bound (negative or absurd -> 0).
private int safeCount(int n) @nogc nothrow pure
{
    return (n < 0 || n > KAFKA_MAX_ARRAY) ? 0 : n;
}

/// Highest request version we actually parse for each api key (must match the
/// ApiVersions table). A higher version has a DIFFERENT wire layout, so parsing
/// it at the pinned dialect shifts every field — that misparse fed the count
/// OOM above. Reject instead.
private short maxApiVer(short apiKey) @nogc nothrow pure
{
    switch (apiKey)
    {
    case API_PRODUCE: return 2;
    case API_FETCH: return 3;
    case API_LIST_OFFSETS: return 1;
    case API_METADATA: return 1;
    case API_API_VERSIONS: return 0;
    default: return 0;
    }
}

// ---------------------------------------------------------------------------
// wire helpers

private void putI16(ref ByteBuffer o, short v) @nogc nothrow
{
    o.appendByte(cast(char)(cast(ushort) v >> 8));
    o.appendByte(cast(char)(v & 0xFF));
}

private void putI32(ref ByteBuffer o, int v) @nogc nothrow
{
    o.appendByte(cast(char)(cast(uint) v >> 24));
    o.appendByte(cast(char)(cast(uint) v >> 16));
    o.appendByte(cast(char)(cast(uint) v >> 8));
    o.appendByte(cast(char)(v & 0xFF));
}

// Overwrite a previously-reserved i32 count field in place (big-endian). Used to
// BACKPATCH a count-prefixed array to the ACTUAL number of entries emitted, so an
// early exit (the response ceiling, or a truncated request tripping !r.ok) can't
// advertise more entries than are present — which would underflow the requesting
// client's decoder and desync only that connection.
private void patchI32(ref ByteBuffer o, size_t off, int v) @nogc nothrow
{
    auto d = o.data;
    if (off + 4 > d.length)
        return;
    d[off] = cast(ubyte)(cast(uint) v >> 24);
    d[off + 1] = cast(ubyte)(cast(uint) v >> 16);
    d[off + 2] = cast(ubyte)(cast(uint) v >> 8);
    d[off + 3] = cast(ubyte)(v & 0xFF);
}

private void putI64(ref ByteBuffer o, long v) @nogc nothrow
{
    putI32(o, cast(int)(v >> 32));
    putI32(o, cast(int)(v & 0xFFFF_FFFF));
}

private void putStr(ref ByteBuffer o, scope const(char)[] s) @nogc nothrow
{
    putI16(o, cast(short) s.length);
    o.append(s);
}

private struct Rd
{
    const(ubyte)[] p;
    size_t i;
    bool ok = true;

    short i16() @nogc nothrow
    {
        if (i + 2 > p.length)
        {
            ok = false;
            return 0;
        }
        auto v = cast(short)((p[i] << 8) | p[i + 1]);
        i += 2;
        return v;
    }

    int i32() @nogc nothrow
    {
        if (i + 4 > p.length)
        {
            ok = false;
            return 0;
        }
        int v = (cast(int) p[i] << 24) | (cast(int) p[i + 1] << 16)
            | (cast(int) p[i + 2] << 8) | p[i + 3];
        i += 4;
        return v;
    }

    long i64() @nogc nothrow
    {
        long hi = i32();
        return (hi << 32) | (cast(long) i32() & 0xFFFF_FFFF);
    }

    const(char)[] str() @nogc nothrow
    {
        immutable n = i16();
        if (n < 0)
            return null; // nullable string
        if (!ok || i + n > p.length)
        {
            ok = false;
            return null;
        }
        auto s = cast(const(char)[]) p[i .. i + n];
        i += n;
        return s;
    }

    const(ubyte)[] bytesI32() @nogc nothrow
    {
        immutable n = i32();
        if (n < 0)
            return null;
        if (!ok || i + n > p.length)
        {
            ok = false;
            return null;
        }
        auto b = p[i .. i + n];
        i += n;
        return b;
    }
}

/// Valid topic name for the flat keyspace key kafka.t.<topic>.<p>: non-empty,
/// bounded, and no '.'/control bytes that would make (topic="a.5",p=0) and
/// (topic="a",p=5) collide on the same list key.
private bool validTopic(scope const(char)[] t) @nogc nothrow pure
{
    if (t.length == 0 || t.length > KAFKA_MAX_TOPIC)
        return false;
    foreach (ch; t)
        if (ch == '.' || ch == ' ' || cast(ubyte) ch < 0x21)
            return false;
    return true;
}

// partition key: kafka.t.<topic>.<p>
private void partKey(scope const(char)[] topic, int part, ref ByteBuffer o) @nogc nothrow
{
    import core.stdc.stdio : snprintf;

    o.clear();
    o.append("kafka.t.");
    o.append(topic);
    char[16] nb = void;
    immutable n = snprintf(nb.ptr, nb.length, ".%d", part);
    o.append(nb[0 .. n]);
}

// data-plane wrappers -------------------------------------------------------

/// RPUSH a BATCH of stored records in one data-plane call (RPUSH is variadic
/// and atomic on the owner): returns the new length — base offset of the
/// batch = length - count. One exec per produce request instead of one per
/// message (the per-message path measured ~1.8µs each = the whole bottleneck).
private long pushRecords(scope const(char)[] key, scope const(char)[][] blobs) nothrow
{
    static ByteBuffer rb; // TLS
    if (gKafkaExec is null || blobs.length == 0)
        return -1;
    static const(char)[][] argv; // TLS scratch
    if (argv.length < blobs.length + 2)
        argv.length = blobs.length + 2;
    argv[0] = "rpush";
    argv[1] = key;
    foreach (i, b; blobs)
        argv[2 + i] = b;
    gKafkaExec(argv[0 .. blobs.length + 2], rb);
    if (argv.length > 65536)
        argv = null; // don't pin a huge scratch array after one big request
    auto d = rb.data;
    if (d.length < 2 || d[0] != ':')
        return -1;
    long v = 0;
    size_t i = 1;
    while (i < d.length && d[i] >= '0' && d[i] <= '9')
    {
        v = v * 10 + (d[i] - '0');
        i++;
    }
    return v;
}

/// Direct owner-shard fetch fast path (installed by dreads.server): appends
/// [offset i64][stored blob] per record straight into `o` — no synthesized
/// RESP, no LRANGE reply parse, no per-record re-copy. Walks the packed list
/// segment in cache order. Returns records appended, or -1 when this thread
/// doesn't own the key (caller falls back to the LRANGE data-plane path).
public __gshared int function(scope const(char)[] key, long from, int maxN,
        size_t budget, long startOff, ref ByteBuffer o) nothrow gKafkaFetchRaw;
/// Direct owner-shard list length; -1 = not owner (fall back to LLEN).
public __gshared long function(scope const(char)[] key) nothrow gKafkaLenRaw;

private long partLen(scope const(char)[] key) nothrow
{
    static ByteBuffer rb2; // TLS
    if (gKafkaExec is null)
        return 0;
    const(char)[][2] a = ["llen", key];
    gKafkaExec(a[], rb2);
    auto d = rb2.data;
    if (d.length < 2 || d[0] != ':')
        return 0;
    long v = 0;
    size_t i = 1;
    while (i < d.length && d[i] >= '0' && d[i] <= '9')
    {
        v = v * 10 + (d[i] - '0');
        i++;
    }
    return v;
}

/// LRANGE [from .. from+max-1]; calls sink(blob) per record, returns count.
private int rangeRecords(scope const(char)[] key, long from, int maxN,
        scope void delegate(scope const(ubyte)[]) nothrow sink) nothrow @trusted
{
    import core.stdc.stdio : snprintf;

    static ByteBuffer rb3; // TLS
    if (gKafkaExec is null || maxN <= 0)
        return 0;
    char[24] b1 = void, b2 = void;
    immutable n1 = snprintf(b1.ptr, b1.length, "%lld", from);
    immutable n2 = snprintf(b2.ptr, b2.length, "%lld", from + maxN - 1);
    const(char)[][4] a = ["lrange", key, cast(const(char)[]) b1[0 .. n1],
        cast(const(char)[]) b2[0 .. n2]];
    gKafkaExec(a[], rb3);
    // parse *N followed by bulks
    auto d = rb3.data;
    if (d.length < 4 || d[0] != '*')
        return 0;
    size_t i = 1;
    int cnt = 0;
    long arr = 0;
    while (i < d.length && d[i] != '\r')
    {
        arr = arr * 10 + (d[i] - '0');
        i++;
    }
    i += 2;
    foreach (_; 0 .. arr)
    {
        if (i >= d.length || d[i] != '$')
            break;
        i++;
        long bl = 0;
        while (i < d.length && d[i] != '\r')
        {
            bl = bl * 10 + (d[i] - '0');
            i++;
        }
        i += 2;
        if (i + bl + 2 > d.length)
            break;
        sink(d[i .. i + cast(size_t) bl]);
        i += cast(size_t) bl + 2;
        cnt++;
    }
    return cnt;
}

// ---------------------------------------------------------------------------
// serve loop

public void serveKafkaClient(TCPConnection tcp) nothrow
{
    try
        tcp.tcpNoDelay = true;
    catch (Exception)
    {
    }
    auto wlock = new TaskMutex;
    ByteBuffer inb;
    ByteBuffer outb;
    for (;;)
    {
        bool alive;
        try
            alive = tcp.waitForData();
        catch (Exception)
            alive = false;
        if (!alive)
            return;
        ulong avail;
        try
            avail = tcp.leastSize;
        catch (Exception)
            return;
        if (avail == 0)
            return;
        auto space = inb.freeSpace(cast(size_t) avail);
        try
            tcp.read(space[0 .. cast(size_t) avail]);
        catch (Exception)
            return;
        inb.grow(cast(size_t) avail);

        size_t pos = 0;
        for (;;)
        {
            auto d = inb.data;
            if (d.length - pos < 4)
                break;
            immutable uint sz = (cast(uint) d[pos] << 24) | (cast(uint) d[pos + 1] << 16)
                | (cast(uint) d[pos + 2] << 8) | d[pos + 3];
            if (sz > 64 * 1024 * 1024)
                return; // insane frame
            if (d.length - pos < 4 + sz)
                break;
            handleRequest(d[pos + 4 .. pos + 4 + sz], outb);
            pos += 4 + sz;
        }
        if (!outb.empty)
        {
            try
            {
                import dreads.amqp : gAmqpAofFlush;

                if (gAmqpAofFlush !is null)
                    gAmqpAofFlush(); // AOF durable before acks=1 reaches the producer
                wlock.lock();
                scope (exit)
                    wlock.unlock();
                tcp.write(outb.data);
            }
            catch (Exception)
                return;
            outb.trim(KAFKA_BUF_KEEP); // a 64MB fetch/produce spike must not pin
        }
        inb.consume(pos);
        if (inb.empty && inb.capacity > KAFKA_BUF_KEEP)
            inb.trim(KAFKA_BUF_KEEP);
    }
}

private void handleRequest(scope const(ubyte)[] req, ref ByteBuffer o) nothrow @trusted
{
    Rd r = Rd(req);
    immutable apiKey = r.i16();
    immutable apiVer = r.i16();
    immutable corr = r.i32();
    cast(void) r.str(); // client_id (nullable)
    if (!r.ok)
        return;

    // response: [i32 size][i32 correlation][payload]; size patched at the end
    immutable sizeAt = o.length;
    putI32(o, 0);
    putI32(o, corr);
    immutable bodyAt = o.length;

    // A version beyond what we parse has a shifted layout; parsing it at the
    // pinned dialect misreads counts (OOM) — reject with the error shell. (The
    // ApiVersions dance already tells honest clients our max.)
    if (apiKey != API_API_VERSIONS && apiVer > maxApiVer(apiKey))
    {
        putI16(o, E_UNSUPPORTED_VERSION);
        auto de = cast(ubyte[]) o.data;
        immutable esz = o.length - sizeAt - 4;
        de[sizeAt] = cast(ubyte)(esz >> 24);
        de[sizeAt + 1] = cast(ubyte)(esz >> 16);
        de[sizeAt + 2] = cast(ubyte)(esz >> 8);
        de[sizeAt + 3] = cast(ubyte)(esz & 0xFF);
        return;
    }

    switch (apiKey)
    {
    case API_API_VERSIONS:
        // reply v0 regardless; UNSUPPORTED_VERSION + the table lets clients
        // downgrade (the standard dance)
        putI16(o, apiVer == 0 ? E_NONE : E_UNSUPPORTED_VERSION);
        putI32(o, 5); // array count
        static void row(ref ByteBuffer o2, short k, short lo, short hi) @nogc nothrow
        {
            putI16(o2, k);
            putI16(o2, lo);
            putI16(o2, hi);
        }

        row(o, API_PRODUCE, 0, 2);
        row(o, API_FETCH, 0, 3);
        row(o, API_LIST_OFFSETS, 0, 1);
        row(o, API_METADATA, 0, 1);
        row(o, API_API_VERSIONS, 0, 0);
        break;

    case API_METADATA:
        handleMetadata(r, apiVer, o);
        break;

    case API_PRODUCE:
        if (handleProduce(r, apiVer, o))
        {
            o.truncate(sizeAt); // acks=0: consume the request, send nothing
            return;
        }
        break;

    case API_FETCH:
        handleFetch(r, apiVer, o);
        break;

    case API_LIST_OFFSETS:
        handleListOffsets(r, apiVer, o);
        break;

    default:
        // unknown api: minimal error shell (correlation already written).
        // Clients that see our ApiVersions table never send these.
        putI16(o, E_UNSUPPORTED_VERSION);
        break;
    }

    // patch the size
    auto d2 = cast(ubyte[]) o.data;
    immutable size = o.length - sizeAt - 4;
    d2[sizeAt] = cast(ubyte)(size >> 24);
    d2[sizeAt + 1] = cast(ubyte)(size >> 16);
    d2[sizeAt + 2] = cast(ubyte)(size >> 8);
    d2[sizeAt + 3] = cast(ubyte)(size & 0xFF);
    cast(void) bodyAt;
}

// Advertised identity (set by server.d at listener setup)
public shared ulong gKafkaProduced; // records stored via Produce (dashboard)
public shared ulong gKafkaFetched;  // records served via Fetch (dashboard)
public __gshared const(char)[] gKafkaHost = "127.0.0.1";
public __gshared ushort gKafkaPort = 9092;

private void handleMetadata(ref Rd r, short ver, ref ByteBuffer o) nothrow
{
    // request: [topics: array of string] (null/empty = all — we answer only
    // named topics; a fresh producer always names what it wants)
    immutable ntopics = safeCount(r.i32());
    static const(char)[][64] topics;
    size_t nt = 0;
    foreach (_; 0 .. ntopics)
    {
        if (!r.ok)
            break;
        auto t = r.str();
        if (nt < topics.length && t !is null && validTopic(t))
            topics[nt++] = t;
    }

    // brokers: just us
    putI32(o, 1);
    putI32(o, 0); // node_id 0
    putStr(o, gKafkaHost);
    putI32(o, gKafkaPort);
    if (ver >= 1)
        putI16(o, -1); // rack: null
    if (ver >= 1)
        putI32(o, 0); // controller_id
    // topics — STATELESS: every named topic exists with KAFKA_PARTITIONS
    putI32(o, cast(int) nt);
    foreach (t; topics[0 .. nt])
    {
        putI16(o, E_NONE);
        putStr(o, t);
        if (ver >= 1)
            o.appendByte(0); // is_internal = false
        putI32(o, KAFKA_PARTITIONS);
        foreach (int p2; 0 .. KAFKA_PARTITIONS)
        {
            putI16(o, E_NONE);
            putI32(o, p2);
            putI32(o, 0); // leader: us
            putI32(o, 1); // replicas
            putI32(o, 0);
            putI32(o, 1); // isr
            putI32(o, 0);
        }
    }
}

// MessageSet v1 entry on the wire:
//   [offset i64][size i32][crc u32][magic i8][attrs i8][timestamp i64][key][value]
// We store [size i32][crc..value] (WITHOUT the offset) as the list record;
// Fetch prepends the real offset per record — the CRC covers magic..value, so
// the stored bytes replay verbatim.
/// Returns true when the producer set acks=0 and expects NO response bytes
/// (the caller rolls the response back — emitting one makes the client match a
/// stale correlation id against no in-flight request and disconnect).
private bool handleProduce(ref Rd r, short ver, ref ByteBuffer o) nothrow @trusted
{
    if (ver >= 1)
    {
    }
    immutable acks = r.i16();
    cast(void) r.i32(); // timeout
    immutable suppress = acks == 0;
    immutable respStart = o.length;
    immutable ntopics = safeCount(r.i32());
    immutable topicsCountOff = o.length; // backpatched to emittedTopics below
    putI32(o, ntopics);
    int emittedTopics = 0;
    foreach (_; 0 .. ntopics)
    {
        if (!r.ok)
            break;
        auto topic = r.str();
        immutable nparts = safeCount(r.i32());
        putStr(o, topic);
        immutable partsCountOff = o.length; // backpatched to emittedParts below
        putI32(o, nparts);
        emittedTopics++;
        int emittedParts = 0;
        foreach (_2; 0 .. nparts)
        {
            if (!r.ok)
                break;
            immutable part = r.i32();
            auto records = r.bytesI32();
            long baseOffset = -1;
            short err = E_NONE;
            if (!validTopic(topic) || part < 0)
                err = E_UNKNOWN_TOPIC;
            static ByteBuffer kb; // TLS (consumed into `raw` before any hop yield)
            partKey(topic, part, kb);
            // collect the whole message set, ONE atomic variadic RPUSH
            static ByteBuffer blobArena; // TLS: all records back-to-back
            static const(char)[][] slices; // TLS: their slices (offsets fixed later)
            static size_t[] offs; // (start,len) pairs into blobArena
            blobArena.clear();
            size_t nrec = 0;
            size_t i = 0;
            while (i + 12 <= records.length)
            {
                if (nrec >= KAFKA_MAX_RECORDS)
                {
                    err = E_CORRUPT; // too many records in one partition set
                    break;
                }
                immutable msz = (cast(uint) records[i + 8] << 24)
                    | (cast(uint) records[i + 9] << 16)
                    | (cast(uint) records[i + 10] << 8) | records[i + 11];
                if (i + 12 + msz > records.length)
                {
                    // a truncated trailing message: reject the whole set. A
                    // silent partial-accept ACKs a baseOffset for records that
                    // were never stored -> the producer's offset accounting
                    // desyncs from the log.
                    err = E_CORRUPT;
                    break;
                }
                auto msg = records[i + 12 .. i + 12 + msz];
                if (msz < 6)
                {
                    err = E_CORRUPT; // too short to hold crc+magic+attrs
                    break;
                }
                if (msg[4] != 1)
                {
                    err = E_CORRUPT; // not MessageSet v1 magic (a v2 RecordBatch
                    break;           // walked as v1 would store garbage)
                }
                if ((msg[5] & 0x07) != 0)
                {
                    err = E_CORRUPT; // compressed sets unsupported in v1
                    break;
                }
                // validate the stored CRC (crc32 over magic..value = msg[4..]);
                // an unchecked bad CRC is a poison pill that aborts every
                // consumer of the partition on replay
                {
                    auto want = (cast(uint) msg[0] << 24) | (cast(uint) msg[1] << 16)
                        | (cast(uint) msg[2] << 8) | msg[3];
                    auto dg = crc32Of(msg[4 .. $]); // ubyte[4], little-endian digest
                    immutable uint got = (cast(uint) dg[3] << 24) | (cast(uint) dg[2] << 16)
                        | (cast(uint) dg[1] << 8) | dg[0];
                    if (got != want)
                    {
                        err = E_CORRUPT;
                        break;
                    }
                }
                if (offs.length < (nrec + 1) * 2)
                    offs.length = (nrec + 1) * 2;
                offs[nrec * 2] = blobArena.length;
                putI32(blobArena, cast(int) msz);
                blobArena.append(msg);
                offs[nrec * 2 + 1] = blobArena.length - offs[nrec * 2];
                nrec++;
                i += 12 + msz;
            }
            if (err == E_NONE && nrec > 0 && validTopic(topic) && part >= 0)
            {
                if (slices.length < nrec)
                    slices.length = nrec;
                auto base = blobArena.data;
                foreach (k; 0 .. nrec)
                    slices[k] = cast(const(char)[]) base[offs[k * 2] .. offs[k * 2] + offs[k * 2 + 1]];
                immutable newLen = pushRecords(kb.data.asChars, slices[0 .. nrec]);
                if (blobArena.capacity > KAFKA_BUF_KEEP)
                    blobArena.trim(KAFKA_BUF_KEEP); // a 64MB set must not pin
                if (offs.length > 131072)
                    offs = null; // release the scratch after a large set
                if (slices.length > 65536)
                    slices = null;
                if (newLen < 0)
                    err = E_CORRUPT;
                else
                {
                    baseOffset = newLen - cast(long) nrec;
                    import core.atomic : atomicOp;
                    atomicOp!"+="(gKafkaProduced, nrec);
                }
            }
            if (o.length - respStart > KAFKA_MAX_RESP)
                continue; // response ceiling: skip this entry (count is backpatched)
            putI32(o, part);
            putI16(o, err);
            putI64(o, baseOffset < 0 ? 0 : baseOffset);
            if (ver >= 2)
                putI64(o, -1); // log_append_time (CreateTime in use)
            emittedParts++;
        }
        patchI32(o, partsCountOff, emittedParts); // count == entries actually emitted
    }
    patchI32(o, topicsCountOff, emittedTopics);
    if (ver >= 1)
        putI32(o, 0); // throttle_ms
    return suppress;
}

private void handleFetch(ref Rd r, short ver, ref ByteBuffer o) nothrow @trusted
{
    cast(void) r.i32(); // replica_id
    cast(void) r.i32(); // max_wait
    cast(void) r.i32(); // min_bytes
    if (ver >= 3)
        cast(void) r.i32(); // max_bytes (whole request)
    immutable ntopics = safeCount(r.i32());
    if (ver >= 1)
        putI32(o, 0); // throttle
    immutable topicsCountOff = o.length; // backpatched to emittedTopics below
    putI32(o, ntopics);
    int emittedTopics = 0;
    foreach (_; 0 .. ntopics)
    {
        if (!r.ok)
            break;
        auto topic = r.str();
        immutable nparts = safeCount(r.i32());
        putStr(o, topic);
        immutable partsCountOff = o.length; // backpatched to emittedParts below
        putI32(o, nparts);
        emittedTopics++;
        int emittedParts = 0;
        foreach (_2; 0 .. nparts)
        {
            if (!r.ok)
                break;
            immutable part = r.i32();
            immutable fetchOff = r.i64();
            immutable partMax = r.i32();
            // Build the key in a reused TLS buffer, then COPY it to a stack
            // array: partLen()'s cross-shard hop YIELDS, and using the TLS
            // buffer after the park would read a key another fetch fiber
            // rewrote during the park (cross-tenant leak). The stack copy
            // survives the yield with zero per-partition heap alloc (the
            // stack-local ByteBuffer that first fixed this churned the
            // allocator on the fetch hot path).
            static ByteBuffer kbBuild; // TLS scratch (only used pre-yield)
            partKey(topic, part, kbBuild);
            char[8 + KAFKA_MAX_TOPIC + 16] keyStore = void;
            immutable klen = kbBuild.length <= keyStore.length ? kbBuild.length : keyStore.length;
            keyStore[0 .. klen] = cast(const(char)[]) kbBuild.data[0 .. klen];
            auto key = cast(const(char)[]) keyStore[0 .. klen];
            long hw = gKafkaLenRaw !is null ? gKafkaLenRaw(key) : -1;
            if (hw < 0)
                hw = partLen(key);
            immutable overCap = o.length > KAFKA_MAX_RESP; // response ceiling
            immutable bad = fetchOff < 0 || fetchOff > hw || !validTopic(topic) || part < 0;
            putI32(o, part);
            putI16(o, bad ? E_OFFSET_OUT_OF_RANGE : E_NONE);
            putI64(o, hw); // high watermark
            // records: rebuild [offset][stored blob] until ~partMax bytes
            immutable recAt = o.length;
            putI32(o, 0); // records byte size, patched below
            if (!bad && !overCap && fetchOff < hw)
            {
                // budget: partMax bytes, capped count
                int maxN = 16384; // deep batches: fewer walks per fetch
                long off = fetchOff;
                size_t budget = partMax > 0 ? cast(size_t) partMax : 65536;
                int direct = -1;
                if (gKafkaFetchRaw !is null)
                    direct = gKafkaFetchRaw(key, fetchOff, maxN,
                            budget, fetchOff, o);
                if (direct > 0)
                {
                    import core.atomic : atomicOp;
                    atomicOp!"+="(gKafkaFetched, cast(ulong) direct);
                }
                if (direct < 0)
                {
                    immutable startLen = o.length;
                    bool first = true;
                    cast(void) rangeRecords(key, fetchOff, maxN,
                            (scope const(ubyte)[] blob) nothrow {
                        // check the budget BEFORE appending (was after: a single
                        // large record always blew past a tiny partMax); always
                        // emit at least one so a consumer makes progress
                        if (!first && o.length - startLen + 8 + blob.length > budget)
                            return;
                        first = false;
                        putI64(o, off);
                        o.append(blob); // [size i32][message] stored verbatim
                        off++;
                    });
                }
            }
            // patch records size
            auto d3 = cast(ubyte[]) o.data;
            immutable rsz = o.length - recAt - 4;
            d3[recAt] = cast(ubyte)(rsz >> 24);
            d3[recAt + 1] = cast(ubyte)(rsz >> 16);
            d3[recAt + 2] = cast(ubyte)(rsz >> 8);
            d3[recAt + 3] = cast(ubyte)(rsz & 0xFF);
            emittedParts++;
        }
        patchI32(o, partsCountOff, emittedParts); // count == entries actually emitted
    }
    patchI32(o, topicsCountOff, emittedTopics);
}

private void handleListOffsets(ref Rd r, short ver, ref ByteBuffer o) nothrow
{
    cast(void) r.i32(); // replica_id
    immutable ntopics = safeCount(r.i32());
    immutable topicsCountOff = o.length; // backpatched to emittedTopics below
    putI32(o, ntopics);
    int emittedTopics = 0;
    foreach (_; 0 .. ntopics)
    {
        if (!r.ok)
            break;
        auto topic = r.str();
        immutable nparts = safeCount(r.i32());
        putStr(o, topic);
        immutable partsCountOff = o.length; // backpatched to emittedParts below
        putI32(o, nparts);
        emittedTopics++;
        int emittedParts = 0;
        foreach (_2; 0 .. nparts)
        {
            if (!r.ok)
                break;
            immutable part = r.i32();
            immutable ts = r.i64();
            if (ver == 0)
                cast(void) r.i32(); // max_num_offsets (v0)
            static ByteBuffer kb3build; // TLS scratch (pre-yield only)
            partKey(topic, part, kb3build);
            char[8 + KAFKA_MAX_TOPIC + 16] k3store = void;
            immutable k3len = kb3build.length <= k3store.length ? kb3build.length : k3store.length;
            k3store[0 .. k3len] = cast(const(char)[]) kb3build.data[0 .. k3len];
            auto k3 = cast(const(char)[]) k3store[0 .. k3len];
            long hw = gKafkaLenRaw !is null ? gKafkaLenRaw(k3) : -1;
            if (hw < 0)
                hw = partLen(k3);
            immutable off = ts == -2 ? 0 : hw; // earliest : latest
            putI32(o, part);
            putI16(o, E_NONE);
            if (ver >= 1)
            {
                putI64(o, -1); // timestamp
                putI64(o, off);
            }
            else
            {
                putI32(o, 1); // v0: array of offsets
                putI64(o, off);
            }
            emittedParts++;
        }
        patchI32(o, partsCountOff, emittedParts); // count == entries actually emitted
    }
    patchI32(o, topicsCountOff, emittedTopics);
}

private auto asChars(const(ubyte)[] b) @nogc nothrow
{
    return cast(const(char)[]) b;
}

// ---------------------------------------------------------------------------
// Tests

unittest // wire helpers round-trip
{
    ByteBuffer b;
    putI16(b, -1);
    putI32(b, 0x01020304);
    putI64(b, 0x0102030405060708);
    putStr(b, "abc");
    Rd r = Rd(cast(const(ubyte)[]) b.data);
    assert(r.i16() == -1);
    assert(r.i32() == 0x01020304);
    assert(r.i64() == 0x0102030405060708);
    assert(r.str() == "abc");
    assert(r.ok);
}
