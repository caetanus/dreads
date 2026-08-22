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

/// Data-plane hook installed by server.d: execute a synthesized RESP command
/// (args[1] is the routing key) on the owner shard, reply RESP bytes.
public __gshared void delegate(scope const(char)[][] args, ref ByteBuffer reply) nothrow gKafkaExec;

public enum uint KAFKA_PARTITIONS = 4; // partitions advertised per topic

private enum short API_PRODUCE = 0, API_FETCH = 1, API_LIST_OFFSETS = 2,
        API_METADATA = 3, API_API_VERSIONS = 18;

private enum short E_NONE = 0, E_CORRUPT = 2, E_UNKNOWN_TOPIC = 3,
        E_OFFSET_OUT_OF_RANGE = 1, E_UNSUPPORTED_VERSION = 35;

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

/// RPUSH one stored record; returns the new length (= assigned offset + 1),
/// or -1 on failure.
private long pushRecord(scope const(char)[] key, scope const(char)[] blob) nothrow
{
    static ByteBuffer rb; // TLS
    if (gKafkaExec is null)
        return -1;
    const(char)[][3] a = ["rpush", key, blob];
    gKafkaExec(a[], rb);
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
                wlock.lock();
                scope (exit)
                    wlock.unlock();
                tcp.write(outb.data);
            }
            catch (Exception)
                return;
            outb.clear();
        }
        inb.consume(pos);
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
        handleProduce(r, apiVer, o);
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
public __gshared const(char)[] gKafkaHost = "127.0.0.1";
public __gshared ushort gKafkaPort = 9092;

private void handleMetadata(ref Rd r, short ver, ref ByteBuffer o) nothrow
{
    // request: [topics: array of string] (null/empty = all — we answer only
    // named topics; a fresh producer always names what it wants)
    int ntopics = r.i32();
    static const(char)[][64] topics;
    size_t nt = 0;
    if (ntopics > 0)
        foreach (_; 0 .. ntopics)
        {
            auto t = r.str();
            if (nt < topics.length && t !is null)
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
private void handleProduce(ref Rd r, short ver, ref ByteBuffer o) nothrow @trusted
{
    if (ver >= 1)
    {
    }
    immutable acks = r.i16();
    cast(void) r.i32(); // timeout
    cast(void) acks;
    immutable ntopics = r.i32();
    putI32(o, ntopics < 0 ? 0 : ntopics);
    foreach (_; 0 .. (ntopics < 0 ? 0 : ntopics))
    {
        auto topic = r.str();
        immutable nparts = r.i32();
        putStr(o, topic);
        putI32(o, nparts < 0 ? 0 : nparts);
        foreach (_2; 0 .. (nparts < 0 ? 0 : nparts))
        {
            immutable part = r.i32();
            auto records = r.bytesI32();
            long baseOffset = -1;
            short err = E_NONE;
            static ByteBuffer kb; // TLS
            partKey(topic, part, kb);
            // walk the producer's message set
            size_t i = 0;
            while (i + 12 <= records.length)
            {
                // producer-side offset ignored
                immutable msz = (cast(uint) records[i + 8] << 24)
                    | (cast(uint) records[i + 9] << 16)
                    | (cast(uint) records[i + 10] << 8) | records[i + 11];
                if (i + 12 + msz > records.length)
                    break;
                auto msg = records[i + 12 .. i + 12 + msz];
                if (msz >= 6 && (msg[5] & 0x07) != 0)
                {
                    err = E_CORRUPT; // compressed sets unsupported in v1
                    break;
                }
                // stored record: [size i32][message]
                static ByteBuffer blob; // TLS
                blob.clear();
                putI32(blob, cast(int) msz);
                blob.append(msg);
                immutable newLen = pushRecord(kb.data.asChars, blob.data.asChars);
                if (newLen < 0)
                {
                    err = E_CORRUPT;
                    break;
                }
                if (baseOffset < 0)
                    baseOffset = newLen - 1;
                i += 12 + msz;
            }
            putI32(o, part);
            putI16(o, err);
            putI64(o, baseOffset < 0 ? 0 : baseOffset);
            if (ver >= 2)
                putI64(o, -1); // log_append_time (CreateTime in use)
        }
    }
    if (ver >= 1)
        putI32(o, 0); // throttle_ms
}

private void handleFetch(ref Rd r, short ver, ref ByteBuffer o) nothrow @trusted
{
    cast(void) r.i32(); // replica_id
    cast(void) r.i32(); // max_wait
    cast(void) r.i32(); // min_bytes
    if (ver >= 3)
        cast(void) r.i32(); // max_bytes (whole request)
    immutable ntopics = r.i32();
    if (ver >= 1)
        putI32(o, 0); // throttle
    putI32(o, ntopics < 0 ? 0 : ntopics);
    foreach (_; 0 .. (ntopics < 0 ? 0 : ntopics))
    {
        auto topic = r.str();
        immutable nparts = r.i32();
        putStr(o, topic);
        putI32(o, nparts < 0 ? 0 : nparts);
        foreach (_2; 0 .. (nparts < 0 ? 0 : nparts))
        {
            immutable part = r.i32();
            immutable fetchOff = r.i64();
            immutable partMax = r.i32();
            static ByteBuffer kb2; // TLS
            partKey(topic, part, kb2);
            immutable hw = partLen(kb2.data.asChars);
            putI32(o, part);
            putI16(o, fetchOff > hw ? E_OFFSET_OUT_OF_RANGE : E_NONE);
            putI64(o, hw); // high watermark
            // records: rebuild [offset][stored blob] until ~partMax bytes
            immutable recAt = o.length;
            putI32(o, 0); // records byte size, patched below
            if (fetchOff < hw)
            {
                // budget: partMax bytes, capped count
                int maxN = 1000;
                long off = fetchOff;
                size_t budget = partMax > 0 ? cast(size_t) partMax : 65536;
                immutable startLen = o.length;
                cast(void) rangeRecords(kb2.data.asChars, fetchOff, maxN,
                        (scope const(ubyte)[] blob) nothrow {
                    if (o.length - startLen > budget)
                        return; // budget exhausted: stop appending
                    putI64(o, off);
                    o.append(blob); // [size i32][message] stored verbatim
                    off++;
                });
            }
            // patch records size
            auto d3 = cast(ubyte[]) o.data;
            immutable rsz = o.length - recAt - 4;
            d3[recAt] = cast(ubyte)(rsz >> 24);
            d3[recAt + 1] = cast(ubyte)(rsz >> 16);
            d3[recAt + 2] = cast(ubyte)(rsz >> 8);
            d3[recAt + 3] = cast(ubyte)(rsz & 0xFF);
        }
    }
}

private void handleListOffsets(ref Rd r, short ver, ref ByteBuffer o) nothrow
{
    cast(void) r.i32(); // replica_id
    immutable ntopics = r.i32();
    putI32(o, ntopics < 0 ? 0 : ntopics);
    foreach (_; 0 .. (ntopics < 0 ? 0 : ntopics))
    {
        auto topic = r.str();
        immutable nparts = r.i32();
        putStr(o, topic);
        putI32(o, nparts < 0 ? 0 : nparts);
        foreach (_2; 0 .. (nparts < 0 ? 0 : nparts))
        {
            immutable part = r.i32();
            immutable ts = r.i64();
            if (ver == 0)
                cast(void) r.i32(); // max_num_offsets (v0)
            static ByteBuffer kb3; // TLS
            partKey(topic, part, kb3);
            immutable hw = partLen(kb3.data.asChars);
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
        }
    }
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
