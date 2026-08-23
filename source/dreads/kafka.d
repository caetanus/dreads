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
    case API_PRODUCE: return 3;
    case API_FETCH: return 4;
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

    byte i8() @nogc nothrow
    {
        if (i + 1 > p.length)
        {
            ok = false;
            return 0;
        }
        return cast(byte) p[i++];
    }

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

/// Direct owner-shard fetch fast path (installed by dreads.server): calls
/// `sink(blob)` per stored record, walking the packed list segment in cache
/// order — no synthesized RESP, no LRANGE reply parse. The sink returns non-zero
/// to stop early (budget filled) and does the offset-prefixing + wire encoding
/// (v1 verbatim/down-convert or v2 batch) so the format stays in kafka.d.
/// Returns records visited, or -1 when this thread doesn't own the key (caller
/// falls back to the LRANGE data-plane path).
public __gshared int function(scope const(char)[] key, long from, int maxN,
        scope int delegate(scope const(ubyte)[] blob) @nogc nothrow sink) nothrow gKafkaFetchRaw;
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

        row(o, API_PRODUCE, 0, 3);
        row(o, API_FETCH, 0, 4);
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
    tKafkaDecompUsed = 0; // reset the per-request decompression budget
    if (ver >= 3)
        cast(void) r.str(); // transactional_id (nullable) — ignored (no txn support)
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
        if (!r.ok)
            break; // truncated mid-topic: don't emit a phantom (zeroed) topic entry
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
            if (!r.ok)
                break; // truncated mid-partition: don't emit/store a phantom entry
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
            // A v2 RecordBatch (magic byte at offset 16 == 2) and a v1 MessageSet
            // both put the magic at offset 16; branch on it. v2 carries headers.
            immutable bool isV2Batch = records.length >= 17 && records[16] == 2;
            if (isV2Batch)
            {
                bool decErr = false;
                immutable dn = decodeV2Batch(cast(const(ubyte)[]) records, (long ts,
                        scope const(ubyte)[] k, bool kn, scope const(ubyte)[] v, bool vn,
                        scope const(ubyte)[] hdr) nothrow{
                    if (nrec >= KAFKA_MAX_RECORDS)
                    {
                        decErr = true;
                        return;
                    }
                    if (offs.length < (nrec + 1) * 2)
                        offs.length = (nrec + 1) * 2;
                    offs[nrec * 2] = blobArena.length;
                    putInternalRec(blobArena, ts, k, kn, v, vn, hdr);
                    offs[nrec * 2 + 1] = blobArena.length - offs[nrec * 2];
                    nrec++;
                });
                if (dn < 0 || decErr)
                {
                    err = E_CORRUPT;
                    nrec = 0;
                }
            }
            else
            {
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
    if (ver >= 4)
        cast(void) r.i8(); // isolation_level (read_uncommitted assumed)
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
        if (!r.ok)
            break; // truncated mid-topic: don't emit a phantom (zeroed) topic entry
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
            if (!r.ok)
                break; // truncated mid-partition: don't emit a phantom entry
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
            if (ver >= 4)
            {
                putI64(o, hw); // last_stable_offset (no transactions => == hw)
                putI32(o, 0); // aborted_transactions: empty array
            }
            // records: re-encode stored blobs per fetch version — v0-v3 as a v1
            // MessageSet (down-converting v2-origin blobs, dropping headers),
            // v4+ as ONE RecordBatch v2 (carrying headers).
            immutable recAt = o.length;
            putI32(o, 0); // records byte size, patched below
            if (!bad && !overCap && fetchOff < hw)
            {
                immutable int maxN = 16384; // deep batches: fewer walks per fetch
                // Clamp the client's partition_max_bytes to the response ceiling.
                // An unclamped ~int.max budget both defeats KAFKA_MAX_RESP within a
                // single partition (a large-allocation broker-death vector) and can
                // push a v4 batch body past 2^31 bytes, wrapping the i32 batchLength
                // NEGATIVE and desyncing the client's RecordBatch decoder. Honest
                // clients send tens of MB, so this is a no-op for them.
                size_t budget = partMax > 0 ? cast(size_t) partMax : 65536;
                if (budget > KAFKA_MAX_RESP)
                    budget = KAFKA_MAX_RESP;
                immutable startLen = o.length;
                import core.atomic : atomicOp;

                if (ver >= 4)
                {
                    // collect stored blobs for the range (slices stay valid: the
                    // owner-shard walk is synchronous with no yield), emit ONE batch
                    static const(ubyte)[][] fblobs; // TLS, pre-sized to maxN
                    if (fblobs.length < cast(size_t) maxN)
                        fblobs.length = maxN;
                    size_t nb = 0, fbytes = 0;
                    int direct = -1;
                    if (gKafkaFetchRaw !is null)
                        direct = gKafkaFetchRaw(key, fetchOff, maxN,
                                (scope const(ubyte)[] blob) @nogc nothrow{
                            if (nb >= cast(size_t) maxN || (nb > 0 && fbytes + blob.length > budget))
                                return 1;
                            fblobs[nb++] = blob;
                            fbytes += blob.length;
                            return 0;
                        });
                    if (direct < 0)
                    {
                        nb = 0;
                        fbytes = 0;
                        cast(void) rangeRecords(key, fetchOff, maxN,
                                (scope const(ubyte)[] blob) nothrow{
                            if (nb < cast(size_t) maxN && (nb == 0 || fbytes + blob.length <= budget))
                            {
                                fblobs[nb++] = blob;
                                fbytes += blob.length;
                            }
                        });
                    }
                    if (nb > 0)
                    {
                        encodeV2BatchFromInternal(o, fetchOff, fblobs[0 .. nb]);
                        atomicOp!"+="(gKafkaFetched, cast(ulong) nb);
                    }
                }
                else
                {
                    long off = fetchOff;
                    int direct = -1;
                    if (gKafkaFetchRaw !is null)
                        direct = gKafkaFetchRaw(key, fetchOff, maxN,
                                (scope const(ubyte)[] blob) @nogc nothrow{
                            if (off > fetchOff && o.length - startLen > budget)
                                return 1; // budget filled (always emit at least one)
                            emitV1Record(o, off, blob);
                            off++;
                            return 0;
                        });
                    if (direct > 0)
                        atomicOp!"+="(gKafkaFetched, cast(ulong) direct);
                    if (direct < 0)
                    {
                        bool first = true;
                        cast(void) rangeRecords(key, fetchOff, maxN,
                                (scope const(ubyte)[] blob) nothrow{
                            if (!first && o.length - startLen + 8 + blob.length > budget)
                                return;
                            first = false;
                            emitV1Record(o, off, blob);
                            off++;
                        });
                    }
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
        if (!r.ok)
            break; // truncated mid-topic: don't emit a phantom (zeroed) topic entry
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
            if (!r.ok)
                break; // truncated mid-partition: don't emit a phantom entry
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

// ---------------------------------------------------------------------------
// RecordBatch v2 (magic 2) primitives: CRC-32C, LEB128 varints, zigzag.
// These underpin record-headers support (Produce v3+/Fetch v4+). They ONLY add
// new code — the v1 MessageSet path is untouched. See the produce/fetch v2
// integration that consumes them.

/// CRC-32C (Castagnoli, reflected poly 0x82F63B78) — the checksum a v2
/// RecordBatch carries (distinct from the v1 message's CRC-32/IEEE `crc32Of`).
private immutable uint[256] CRC32C_TABLE = () {
    uint[256] t;
    foreach (i; 0 .. 256)
    {
        uint c = cast(uint) i;
        foreach (_; 0 .. 8)
            c = (c & 1) ? (0x82F63B78U ^ (c >> 1)) : (c >> 1);
        t[i] = c;
    }
    return t;
}();

private uint crc32c(scope const(ubyte)[] data) @nogc nothrow pure @safe
{
    uint crc = 0xFFFFFFFFU;
    foreach (b; data)
        crc = CRC32C_TABLE[(crc ^ b) & 0xFF] ^ (crc >> 8);
    return crc ^ 0xFFFFFFFFU;
}

/// LEB128 unsigned varint append.
private void putUVarint(ref ByteBuffer o, ulong v) @nogc nothrow
{
    while (v >= 0x80)
    {
        o.appendByte(cast(ubyte)(v | 0x80));
        v >>= 7;
    }
    o.appendByte(cast(ubyte) v);
}

/// Zigzag-encoded signed varint append (Kafka v2 uses these for lengths/deltas).
private void putVarlong(ref ByteBuffer o, long v) @nogc nothrow
{
    putUVarint(o, (cast(ulong) v << 1) ^ cast(ulong)(v >> 63));
}

/// Read a LEB128 unsigned varint; sets ok=false on truncation or overlong (>10
/// bytes). Advances i past the varint.
private ulong getUVarint(scope const(ubyte)[] p, ref size_t i, ref bool ok) @nogc nothrow
{
    ulong result = 0;
    int shift = 0;
    foreach (_; 0 .. 10)
    {
        if (i >= p.length)
        {
            ok = false;
            return 0;
        }
        immutable ubyte b = p[i++];
        result |= cast(ulong)(b & 0x7F) << shift;
        if ((b & 0x80) == 0)
            return result;
        shift += 7;
    }
    ok = false; // more than 10 bytes: malformed
    return 0;
}

/// Read a zigzag-encoded signed varint.
private long getVarlong(scope const(ubyte)[] p, ref size_t i, ref bool ok) @nogc nothrow
{
    immutable ulong u = getUVarint(p, i, ok);
    return cast(long)(u >> 1) ^ -cast(long)(u & 1);
}

unittest // CRC-32C against the canonical "123456789" vector
{
    assert(crc32c(cast(const(ubyte)[]) "123456789") == 0xE3069283U);
    assert(crc32c(cast(const(ubyte)[]) "") == 0U);
}

unittest // varint / zigzag round-trip, including boundaries and negatives
{
    foreach (long v; [
            0L, 1L, -1L, 2L, -2L, 63L, 64L, -64L, 127L, 128L, -128L,
            300L, -300L, 2147483647L, -2147483648L, long.max, long.min
        ])
    {
        ByteBuffer b;
        putVarlong(b, v);
        auto p = cast(const(ubyte)[]) b.data;
        size_t i = 0;
        bool ok = true;
        immutable got = getVarlong(p, i, ok);
        assert(ok && got == v && i == b.length);
    }
    // unsigned round-trip + truncation detection
    {
        ByteBuffer b;
        putUVarint(b, 300);
        auto p = cast(const(ubyte)[]) b.data;
        size_t i = 0;
        bool ok = true;
        assert(getUVarint(p, i, ok) == 300 && ok);
        // a lone 0x80 (continuation with no terminator) must fail, not hang
        const(ubyte)[1] trunc = [0x80];
        size_t j = 0;
        ok = true;
        cast(void) getUVarint(trunc[], j, ok);
        assert(!ok);
    }
}

// ---------------------------------------------------------------------------
// Internal stored-record blob: a v2-origin record kept as ONE list element so
// the existing list storage engine is reused unchanged. The leading 0xFF tag
// distinguishes it from a legacy v1 blob `[i32 size][crc..]`, whose size high
// byte is 0x00 for any real (<16MB) message.
//   [u8 0xFF][i64 ts][i32 keyLen(-1=null)][key][i32 valLen(-1=null)][val]
//   [i32 hdrLen][hdrSection] — hdrSection is the record's raw v2 header bytes
//   ([svarint count]([svarint kLen][k][svarint vLen][v])*), verbatim-copyable
//   into a re-encoded batch because header key/value lengths are absolute.
private enum ubyte KREC_TAG = 0xFF;

private struct KRec2
{
    long ts;
    const(ubyte)[] key;
    bool keyNull;
    const(ubyte)[] val;
    bool valNull;
    const(ubyte)[] hdrSection; // empty for a v1-origin record
    bool ok = true;
}

private void putBytesI32(ref ByteBuffer o, scope const(ubyte)[] b, bool isNull) @nogc nothrow
{
    if (isNull)
    {
        putI32(o, -1);
        return;
    }
    putI32(o, cast(int) b.length);
    o.append(b);
}

private void putInternalRec(ref ByteBuffer o, long ts, scope const(ubyte)[] key, bool keyNull,
        scope const(ubyte)[] val, bool valNull, scope const(ubyte)[] hdrSection) @nogc nothrow
{
    o.appendByte(cast(char) KREC_TAG);
    putI64(o, ts);
    putBytesI32(o, key, keyNull);
    putBytesI32(o, val, valNull);
    putI32(o, cast(int) hdrSection.length);
    o.append(hdrSection);
}

/// Parse a stored list element into common fields, handling BOTH the internal
/// v2 blob (0xFF tag) and a legacy v1 message blob `[i32 size][crc magic attrs
/// ts key val]`. A partition may hold a mix (v1 produce + v2 produce).
private KRec2 parseStoredRec(scope const(ubyte)[] b) @nogc nothrow
{
    KRec2 r;
    if (b.length >= 1 && b[0] == KREC_TAG)
    {
        Rd rd = Rd(b);
        rd.i = 1;
        r.ts = rd.i64();
        immutable kl = rd.i32();
        if (!rd.ok)
        {
            r.ok = false;
            return r;
        }
        if (kl < 0)
            r.keyNull = true;
        else if (rd.i + kl <= b.length)
        {
            r.key = b[rd.i .. rd.i + kl];
            rd.i += kl;
        }
        else
        {
            r.ok = false;
            return r;
        }
        immutable vl = rd.i32();
        if (!rd.ok)
        {
            r.ok = false;
            return r;
        }
        if (vl < 0)
            r.valNull = true;
        else if (rd.i + vl <= b.length)
        {
            r.val = b[rd.i .. rd.i + vl];
            rd.i += vl;
        }
        else
        {
            r.ok = false;
            return r;
        }
        immutable hl = rd.i32();
        if (!rd.ok || hl < 0 || rd.i + hl > b.length)
        {
            r.ok = false;
            return r;
        }
        r.hdrSection = b[rd.i .. rd.i + hl];
        return r;
    }
    // legacy v1 message blob: [i32 size][crc i32][magic i8][attrs i8][ts i64][key][val]
    Rd rd = Rd(b);
    cast(void) rd.i32(); // size
    cast(void) rd.i32(); // crc
    if (rd.i + 2 > b.length)
    {
        r.ok = false;
        return r;
    }
    rd.i += 2; // magic + attrs
    r.ts = rd.i64();
    auto k = rd.bytesI32();
    r.keyNull = (k is null);
    r.key = cast(const(ubyte)[]) k;
    auto v = rd.bytesI32();
    r.valNull = (v is null);
    r.val = cast(const(ubyte)[]) v;
    r.ok = rd.ok;
    return r;
}

/// Validate a raw v2 header section so a later verbatim re-emit is safe.
private bool validHeaderSection(scope const(ubyte)[] h) @nogc nothrow
{
    size_t i = 0;
    bool ok = true;
    immutable long n = getVarlong(h, i, ok);
    if (!ok || n < 0)
        return false;
    foreach (_; 0 .. n)
    {
        immutable long kl = getVarlong(h, i, ok);
        if (!ok || kl < 0 || i + cast(size_t) kl > h.length)
            return false;
        i += cast(size_t) kl;
        immutable long vl = getVarlong(h, i, ok);
        if (!ok)
            return false;
        if (vl >= 0)
        {
            if (i + cast(size_t) vl > h.length)
                return false;
            i += cast(size_t) vl;
        }
    }
    return i == h.length;
}

/// Decompression-bomb ceiling: a compressed batch's plaintext must not exceed
/// this, else we reject rather than let a tiny frame expand to gigabytes.
private enum size_t KAFKA_DECOMP_MAX = 128 << 20;

/// Bounded gzip inflate into `dst`. Kafka's v2 gzip payload is a standard gzip
/// stream (magic 1f 8b). Returns false on malformed input OR if the output would
/// exceed capMax (bomb guard). @nogc — streams through a stack chunk.
private bool gunzipInto(scope const(ubyte)[] src, ref ByteBuffer dst, size_t capMax) @nogc nothrow @trusted
{
    import etc.c.zlib : z_stream, inflateInit2_, inflate, inflateEnd, zlibVersion,
        Z_OK, Z_STREAM_END, Z_NO_FLUSH;

    if (src.length == 0 || src.length > uint.max)
        return false;
    z_stream zs;
    // windowBits 15 + 16 selects gzip framing (not raw/zlib).
    if (inflateInit2_(&zs, 15 + 16, zlibVersion(), cast(int) z_stream.sizeof) != Z_OK)
        return false;
    scope (exit)
        inflateEnd(&zs);
    dst.clear();
    zs.next_in = cast(ubyte*) src.ptr;
    zs.avail_in = cast(uint) src.length;
    ubyte[65536] chunk = void;
    for (;;)
    {
        zs.next_out = chunk.ptr;
        zs.avail_out = chunk.length;
        immutable r = inflate(&zs, Z_NO_FLUSH);
        immutable produced = chunk.length - zs.avail_out;
        if (produced)
        {
            if (dst.length + produced > capMax)
                return false; // decompression bomb
            dst.append(chunk[0 .. produced]);
        }
        if (r == Z_STREAM_END)
            return true;
        if (r != Z_OK)
            return false; // Z_BUF_ERROR (truncated), data error, etc.
    }
}

// liblz4 FRAME API (Kafka's v2 lz4 payload is an LZ4 frame: magic 04 22 4d 18),
// distinct from the block API (LZ4_decompress_safe) dreads.lz4 uses for raft.
// Symbols are in the same vendored liblz4.a.
private extern (C) @nogc nothrow @system
{
    struct LZ4F_dctx;
    size_t LZ4F_createDecompressionContext(LZ4F_dctx** ctxPtr, uint ver);
    size_t LZ4F_freeDecompressionContext(LZ4F_dctx* ctx);
    size_t LZ4F_decompress(LZ4F_dctx* ctx, void* dst, size_t* dstSize,
            const(void)* src, size_t* srcSize, const(void)* opt);
    uint LZ4F_isError(size_t code);
}

private enum uint LZ4F_VERSION = 100;

/// Bounded LZ4-frame decompress into `dst`. Returns false on malformed input,
/// codec error, truncation, or if the plaintext would exceed capMax (bomb).
private bool lz4FrameInto(scope const(ubyte)[] src, ref ByteBuffer dst, size_t capMax) @nogc nothrow @trusted
{
    if (src.length == 0)
        return false;
    LZ4F_dctx* ctx;
    if (LZ4F_isError(LZ4F_createDecompressionContext(&ctx, LZ4F_VERSION)))
        return false;
    scope (exit)
        LZ4F_freeDecompressionContext(ctx);
    dst.clear();
    ubyte[65536] chunk = void;
    size_t srcPos = 0;
    for (;;)
    {
        size_t dstSize = chunk.length;
        size_t srcSize = src.length - srcPos;
        immutable hint = LZ4F_decompress(ctx, chunk.ptr, &dstSize,
                src.ptr + srcPos, &srcSize, null);
        if (LZ4F_isError(hint))
            return false;
        srcPos += srcSize;
        if (dstSize)
        {
            if (dst.length + dstSize > capMax)
                return false; // decompression bomb
            dst.append(chunk[0 .. dstSize]);
        }
        if (hint == 0)
            return true; // frame fully decoded
        if (srcSize == 0 && dstSize == 0)
            return false; // no progress: truncated frame
    }
}

// libsnappy C API (vendored static libsnappy.a). Kafka's v2 snappy payload is a
// RAW snappy stream (leading uncompressed-length varint), NOT the v1 xerial
// block framing — so the plain C API decodes it directly.
private extern (C) @nogc nothrow @system
{
    int snappy_uncompressed_length(const(char)* compressed, size_t compressed_length, size_t* result);
    int snappy_uncompress(const(char)* compressed, size_t compressed_length,
            char* uncompressed, size_t* uncompressed_length);
}

/// Bounded raw-snappy decompress into `dst`. Returns false on malformed input or
/// if the plaintext would exceed capMax (bomb — checked BEFORE allocating).
private bool snappyInto(scope const(ubyte)[] src, ref ByteBuffer dst, size_t capMax) @nogc nothrow @trusted
{
    if (src.length == 0)
        return false;
    size_t ulen;
    if (snappy_uncompressed_length(cast(const(char)*) src.ptr, src.length, &ulen) != 0)
        return false;
    if (ulen == 0 || ulen > capMax)
        return false; // empty or decompression bomb
    dst.clear();
    auto space = dst.freeSpace(ulen); // writable region >= ulen bytes
    size_t outLen = ulen;
    if (snappy_uncompress(cast(const(char)*) src.ptr, src.length,
            cast(char*) space.ptr, &outLen) != 0)
        return false;
    if (outLen != ulen)
        return false;
    dst.grow(outLen); // mark the decompressed bytes as filled
    return true;
}

/// Request-level ceiling on TOTAL decompressed bytes, reset per Produce request
/// (tKafkaDecompUsed). The per-partition KAFKA_DECOMP_MAX bounds ONE batch, but a
/// request enumerating many partitions each carrying a ~1000:1 frame could
/// otherwise amplify a 64 MB request into tens of GB of decompress+store work.
private enum size_t KAFKA_DECOMP_REQ_MAX = 512 << 20;
private size_t tKafkaDecompUsed; // TLS, reset at the top of handleProduce

/// Decompress a v2 batch's compressed records region (codec = attrs & 0x07)
/// into `dst`. 1=gzip, 2=snappy, 3=lz4 implemented; zstd(4) not yet wired
/// (return false → the batch is rejected, exactly as before). Bounded by both
/// the per-batch cap and the running per-request budget.
private bool decompressRecords(ubyte codec, scope const(ubyte)[] src, ref ByteBuffer dst) nothrow @trusted
{
    if (tKafkaDecompUsed >= KAFKA_DECOMP_REQ_MAX)
        return false; // request-level decompression budget exhausted
    immutable size_t remain = KAFKA_DECOMP_REQ_MAX - tKafkaDecompUsed;
    immutable size_t cap = remain < KAFKA_DECOMP_MAX ? remain : KAFKA_DECOMP_MAX;
    bool ok;
    switch (codec)
    {
    case 1:
        ok = gunzipInto(src, dst, cap);
        break;
    case 2:
        ok = snappyInto(src, dst, cap);
        break;
    case 3:
        ok = lz4FrameInto(src, dst, cap);
        break;
    default:
        return false; // zstd: not yet supported
    }
    if (ok)
        tKafkaDecompUsed += dst.length;
    return ok;
}

/// Decode a RecordBatch v2. Calls rec() per record with ABSOLUTE timestamp and
/// the record's raw (verbatim-copyable) header section. Compressed batches
/// (attrs & 0x07 != 0) are decompressed via the supported codecs. Returns the
/// record count, or -1 on any malformation / unsupported-codec / bomb.
private int decodeV2Batch(scope const(ubyte)[] b, scope void delegate(long ts,
        scope const(ubyte)[] key, bool keyNull, scope const(ubyte)[] val, bool valNull,
        scope const(ubyte)[] hdrSection) nothrow rec) nothrow
{
    if (b.length < 61)
        return -1; // fixed v2 batch header is 61 bytes
    Rd h = Rd(b);
    cast(void) h.i64(); // baseOffset
    cast(void) h.i32(); // batchLength
    cast(void) h.i32(); // partitionLeaderEpoch
    immutable ubyte magic = b[h.i];
    h.i += 1;
    if (magic != 2)
        return -1;
    immutable uint crcStored = cast(uint) h.i32();
    immutable size_t crcCoverStart = h.i; // attributes onward
    immutable short attrs = h.i16();
    immutable ubyte codec = cast(ubyte)(attrs & 0x07);
    cast(void) h.i32(); // lastOffsetDelta
    immutable long firstTs = h.i64();
    cast(void) h.i64(); // maxTimestamp
    cast(void) h.i64(); // producerId
    cast(void) h.i16(); // producerEpoch
    cast(void) h.i32(); // baseSequence
    immutable int nrec = h.i32();
    if (!h.ok || nrec < 0 || nrec > KAFKA_MAX_RECORDS)
        return -1;
    if (crcCoverStart > b.length || crc32c(b[crcCoverStart .. $]) != crcStored)
        return -1;
    // Records region: b[h.i .. $]. If compressed, decompress into `plain` and
    // parse that; the CRC above already validated the compressed bytes.
    scope const(ubyte)[] rp = b;
    size_t i = h.i;
    static ByteBuffer plain; // TLS: decompressed records (codec != 0)
    if (codec != 0)
    {
        if (!decompressRecords(codec, b[h.i .. $], plain))
            return -1; // unsupported codec / malformed / decompression bomb
        rp = cast(const(ubyte)[]) plain.data;
        i = 0;
    }
    foreach (_; 0 .. nrec)
    {
        bool ok = true;
        immutable long rlen = getVarlong(rp, i, ok);
        if (!ok || rlen < 0 || i + cast(size_t) rlen > rp.length)
            return -1;
        immutable size_t recEnd = i + cast(size_t) rlen;
        if (i >= recEnd)
            return -1;
        i += 1; // per-record attributes (unused)
        immutable long tsDelta = getVarlong(rp, i, ok);
        cast(void) getVarlong(rp, i, ok); // offsetDelta (contiguous — recomputed)
        immutable long kl = getVarlong(rp, i, ok);
        if (!ok)
            return -1;
        const(ubyte)[] key;
        bool keyNull;
        if (kl < 0)
            keyNull = true;
        else
        {
            if (i + cast(size_t) kl > recEnd)
                return -1;
            key = rp[i .. i + cast(size_t) kl];
            i += cast(size_t) kl;
        }
        immutable long vl = getVarlong(rp, i, ok);
        if (!ok)
            return -1;
        const(ubyte)[] val;
        bool valNull;
        if (vl < 0)
            valNull = true;
        else
        {
            if (i + cast(size_t) vl > recEnd)
                return -1;
            val = rp[i .. i + cast(size_t) vl];
            i += cast(size_t) vl;
        }
        if (i > recEnd)
            return -1;
        scope const(ubyte)[] hdrSection = rp[i .. recEnd];
        if (!validHeaderSection(hdrSection))
            return -1;
        rec(firstTs + tsDelta, key, keyNull, val, valNull, hdrSection);
        i = recEnd;
    }
    return nrec;
}

/// Encode stored records (internal or v1 blobs) as ONE RecordBatch v2 at
/// baseOffset — the Fetch v4+ emit path.
private void encodeV2BatchFromInternal(ref ByteBuffer o, long baseOffset,
        scope const(ubyte)[][] blobs) @nogc nothrow
{
    static ByteBuffer bodyB; // TLS: attributes..end (the CRC-covered region)
    static ByteBuffer rbuf; // TLS: one record body
    bodyB.clear();
    long firstTs = 0, maxTs = 0;
    bool haveTs = false;
    foreach (bl; blobs)
    {
        auto rr = parseStoredRec(bl);
        if (!rr.ok)
            continue;
        if (!haveTs)
        {
            firstTs = rr.ts;
            maxTs = rr.ts;
            haveTs = true;
        }
        else
        {
            if (rr.ts < firstTs)
                firstTs = rr.ts;
            if (rr.ts > maxTs)
                maxTs = rr.ts;
        }
    }
    immutable int count = cast(int) blobs.length;
    putI16(bodyB, 0); // attributes
    putI32(bodyB, count > 0 ? count - 1 : 0); // lastOffsetDelta
    putI64(bodyB, firstTs);
    putI64(bodyB, maxTs);
    putI64(bodyB, -1); // producerId
    putI16(bodyB, -1); // producerEpoch
    putI32(bodyB, -1); // baseSequence
    putI32(bodyB, count); // record count
    foreach (idx, bl; blobs)
    {
        auto rr = parseStoredRec(bl);
        rbuf.clear();
        rbuf.appendByte(0); // record attributes
        putVarlong(rbuf, rr.ok ? rr.ts - firstTs : 0);
        putVarlong(rbuf, cast(long) idx); // offsetDelta
        if (!rr.ok || rr.keyNull)
            putVarlong(rbuf, -1);
        else
        {
            putVarlong(rbuf, cast(long) rr.key.length);
            rbuf.append(rr.key);
        }
        if (!rr.ok || rr.valNull)
            putVarlong(rbuf, -1);
        else
        {
            putVarlong(rbuf, cast(long) rr.val.length);
            rbuf.append(rr.val);
        }
        if (rr.ok && rr.hdrSection.length)
            rbuf.append(rr.hdrSection);
        else
            putVarlong(rbuf, 0); // zero headers
        putVarlong(bodyB, cast(long) rbuf.length);
        bodyB.append(rbuf.data);
    }
    immutable uint crc = crc32c(cast(const(ubyte)[]) bodyB.data);
    putI64(o, baseOffset);
    immutable size_t blenOff = o.length;
    putI32(o, 0); // batchLength placeholder
    putI32(o, -1); // partitionLeaderEpoch
    o.appendByte(2); // magic = 2
    putI32(o, cast(int) crc);
    o.append(bodyB.data);
    patchI32(o, blenOff, cast(int)(4 + 1 + 4 + bodyB.length));
}

/// Emit one record as a v1 MessageSet entry `[offset i64][size i32][message]`
/// — the Fetch v0-v3 path. A legacy v1 blob is copied verbatim (the fast, common
/// case); an internal v2 blob is down-converted to v1, DROPPING headers (v1 has
/// no header field — the standard broker down-conversion for old clients).
private void emitV1Record(ref ByteBuffer o, long offset, scope const(ubyte)[] blob) @nogc nothrow
{
    if (blob.length >= 1 && blob[0] == KREC_TAG)
    {
        auto rr = parseStoredRec(blob);
        static ByteBuffer m; // TLS: message body magic..value (for CRC)
        m.clear();
        m.appendByte(1); // magic = 1
        m.appendByte(0); // attributes
        putI64(m, rr.ok ? rr.ts : -1);
        putBytesI32(m, rr.key, rr.keyNull || !rr.ok);
        putBytesI32(m, rr.val, rr.valNull || !rr.ok);
        auto dg = crc32Of(cast(const(ubyte)[]) m.data); // little-endian digest
        immutable uint crc = (cast(uint) dg[3] << 24) | (cast(uint) dg[2] << 16) | (
                cast(uint) dg[1] << 8) | dg[0];
        putI64(o, offset);
        putI32(o, cast(int)(4 + m.length)); // message size = crc(4) + body
        putI32(o, cast(int) crc); // crc (big-endian on the wire)
        o.append(m.data);
    }
    else
    {
        putI64(o, offset);
        o.append(blob); // stored `[size i32][v1 message]` verbatim
    }
}

unittest // v2 batch codec round-trip: internal blobs -> encode -> decode -> back
{
    // build a v2 header section with two headers (h1=><bytes>, h2=null)
    ByteBuffer hs;
    putVarlong(hs, 2); // header count
    putVarlong(hs, 2);
    hs.append("hk"); // header 0 key
    putVarlong(hs, 3);
    hs.append(cast(const(ubyte)[])[1, 2, 3]); // header 0 value
    putVarlong(hs, 3);
    hs.append("hk2"); // header 1 key
    putVarlong(hs, -1); // header 1 value = null
    assert(validHeaderSection(cast(const(ubyte)[]) hs.data));

    ByteBuffer zeroHdr; // a valid v2 header section with zero headers
    putVarlong(zeroHdr, 0);

    ByteBuffer r0, r1, r2;
    putInternalRec(r0, 1000, cast(const(ubyte)[]) "key0", false,
            cast(const(ubyte)[]) "val0", false, cast(const(ubyte)[]) hs.data);
    putInternalRec(r1, 1005, null, true, cast(const(ubyte)[]) "val1", false,
            cast(const(ubyte)[]) zeroHdr.data); // null key, zero headers
    putInternalRec(r2, 1002, cast(const(ubyte)[]) "k2", false, null, true,
            cast(const(ubyte)[]) zeroHdr.data); // null value, zero headers

    const(ubyte)[][3] blobs = [
        cast(const(ubyte)[]) r0.data, cast(const(ubyte)[]) r1.data, cast(const(ubyte)[]) r2.data
    ];
    ByteBuffer batch;
    encodeV2BatchFromInternal(batch, 42, blobs[]);

    // decode and rebuild internal blobs; must equal the originals
    ByteBuffer[3] out_;
    size_t seen = 0;
    immutable n = decodeV2Batch(cast(const(ubyte)[]) batch.data, (long ts, scope const(ubyte)[] key,
            bool keyNull, scope const(ubyte)[] val, bool valNull, scope const(ubyte)[] hdr) nothrow{
        if (seen < 3)
            putInternalRec(out_[seen], ts, key, keyNull, val, valNull, hdr);
        seen++;
    });
    assert(n == 3 && seen == 3);
    assert(out_[0].data == r0.data);
    assert(out_[1].data == r1.data);
    assert(out_[2].data == r2.data);

    // a corrupted CRC must be rejected
    auto bad = cast(ubyte[]) batch.data.dup;
    bad[17] ^= 0xFF; // flip a byte inside the crc field region
    assert(decodeV2Batch(cast(const(ubyte)[]) bad, (long, scope const(ubyte)[], bool,
            scope const(ubyte)[], bool, scope const(ubyte)[]) nothrow{}) == -1);
}
