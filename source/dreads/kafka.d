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

import dreads.mem : ByteBuffer, tByteBufferOom;
import std.digest.crc : crc32Of;

/// Data-plane hook installed by server.d: execute a synthesized RESP command
/// (args[1] is the routing key) on the owner shard, reply RESP bytes.
public __gshared void delegate(scope const(char)[][] args, ref ByteBuffer reply) nothrow gKafkaExec;

public enum uint KAFKA_PARTITIONS = 4; // partitions advertised per topic
private enum int KAFKA_MAX_PARTITIONS = 1024; // cap a topic's partition count (DoS)

private enum short API_PRODUCE = 0, API_FETCH = 1, API_LIST_OFFSETS = 2,
        API_METADATA = 3, API_API_VERSIONS = 18;
// consumer-group coordinator APIs (feature build — driving golib Consumer/
// Inspector/Transaction conformance to pass)
private enum short API_OFFSET_COMMIT = 8, API_OFFSET_FETCH = 9,
        API_FIND_COORDINATOR = 10, API_JOIN_GROUP = 11, API_HEARTBEAT = 12,
        API_LEAVE_GROUP = 13, API_SYNC_GROUP = 14, API_DESCRIBE_CONFIGS = 32,
        API_CREATE_TOPICS = 19, API_DESCRIBE_GROUPS = 15;
// Transaction coordinator APIs (single-node = own coordinator).
private enum short API_CREATE_PARTITIONS = 37, API_DELETE_TOPICS = 20,
        API_INCREMENTAL_ALTER_CONFIGS = 44;
private enum short API_INIT_PRODUCER_ID = 22, API_ADD_PARTITIONS_TO_TXN = 24,
        API_ADD_OFFSETS_TO_TXN = 25, API_END_TXN = 26, API_TXN_OFFSET_COMMIT = 28;

private enum short E_NONE = 0, E_CORRUPT = 2, E_UNKNOWN_TOPIC = 3,
        E_OFFSET_OUT_OF_RANGE = 1, E_INVALID_TOPIC = 17, E_UNSUPPORTED_VERSION = 35,
        E_INVALID_RECORD = 87;

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
    case API_PRODUCE: return 8; // v9+ flexible (v8 non-flexible; KIP-467)
    case API_FETCH: return 11; // v12+ flexible (v11 non-flexible)
    case API_LIST_OFFSETS: return 5; // v6+ flexible (v5 non-flexible)
    case API_METADATA: return 9; // v9+ flexible (handleMetadataFlex)
    case API_API_VERSIONS: return 0;
    case API_OFFSET_COMMIT: return 7; // v8+ flexible
    case API_OFFSET_FETCH: return 7; // v6+ flexible (v7 adds require_stable)
    case API_FIND_COORDINATOR: return 3; // v3 flexible (single key)
    case API_JOIN_GROUP: return 7; // v6+ flexible (v7 adds response protocol_type)
    case API_HEARTBEAT: return 4; // v4 flexible
    case API_LEAVE_GROUP: return 4; // v4 flexible
    case API_SYNC_GROUP: return 5; // v4+ flexible (v5 adds protocol_type)
    case API_DESCRIBE_CONFIGS: return 3; // v4+ flexible
    case API_CREATE_TOPICS: return 4; // v5+ flexible
    case API_CREATE_PARTITIONS: return 1; // v2+ flexible
    case API_DELETE_TOPICS: return 1; // v4+ flexible
    case API_INCREMENTAL_ALTER_CONFIGS: return 0; // v1+ flexible
    case API_DESCRIBE_GROUPS: return 4; // v5+ flexible
    case API_INIT_PRODUCER_ID: return 3; // v2+ flexible
    case API_ADD_PARTITIONS_TO_TXN: return 2; // v3+ flexible
    case API_ADD_OFFSETS_TO_TXN: return 2; // v3+ flexible
    case API_END_TXN: return 2; // v3+ flexible
    case API_TXN_OFFSET_COMMIT: return 3; // v3 flexible
    default: return 0;
    }
}

/// The apiVersion at which each API switches to the flexible (KIP-482) encoding
/// (compact lengths + tagged fields). A request/response at >= this uses it.
private short flexibleSince(short apiKey) @nogc nothrow pure
{
    switch (apiKey)
    {
    case API_PRODUCE: return 9;
    case API_FETCH: return 12;
    case API_LIST_OFFSETS: return 6;
    case API_METADATA: return 9;
    case API_OFFSET_COMMIT: return 8;
    case API_OFFSET_FETCH: return 6;
    case API_FIND_COORDINATOR: return 3;
    case API_JOIN_GROUP: return 6;
    case API_HEARTBEAT: return 4;
    case API_LEAVE_GROUP: return 4;
    case API_SYNC_GROUP: return 4;
    case API_DESCRIBE_GROUPS: return 5;
    case API_API_VERSIONS: return 3;
    case API_CREATE_TOPICS: return 5;
    case API_INIT_PRODUCER_ID: return 2;
    case API_ADD_PARTITIONS_TO_TXN: return 3;
    case API_ADD_OFFSETS_TO_TXN: return 3;
    case API_END_TXN: return 3;
    case API_TXN_OFFSET_COMMIT: return 3;
    case API_DESCRIBE_CONFIGS: return 4;
    default: return short.max;
    }
}

private bool isFlexible(short apiKey, short apiVer) @nogc nothrow pure
{
    return apiVer >= flexibleSince(apiKey);
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

// ---------------------------------------------------------------------------
// Flexible-dialect (KIP-482) wire helpers. Flexible request/response versions
// encode lengths as unsigned varints (compact), strings/arrays/bytes as
// length+1 (0 = null), and append a tagged-fields section (a uvarint count,
// 0 = none) after every struct and after the request/response header. dreads
// emits only EMPTY tagged fields. Non-flexible versions keep the classic i16/
// i32 length prefixes above — the handlers branch on the request version.

/// Unsigned LEB128 varint.
private void putUvarint(ref ByteBuffer o, ulong v) @nogc nothrow
{
    while (v >= 0x80)
    {
        o.appendByte(cast(char)((v & 0x7F) | 0x80));
        v >>= 7;
    }
    o.appendByte(cast(char)(v & 0x7F));
}

/// Compact string: uvarint(length+1) then bytes; length 0 encodes as uvarint 1.
private void putCStr(ref ByteBuffer o, scope const(char)[] s) @nogc nothrow
{
    putUvarint(o, cast(ulong) s.length + 1);
    o.append(s);
}

/// Compact nullable string: null -> uvarint 0.
private void putCStrNull(ref ByteBuffer o, scope const(char)[] s, bool isNull) @nogc nothrow
{
    if (isNull)
    {
        putUvarint(o, 0);
        return;
    }
    putCStr(o, s);
}

/// Compact bytes: uvarint(length+1) then bytes.
private void putCBytes(ref ByteBuffer o, scope const(ubyte)[] b, bool isNull) @nogc nothrow
{
    if (isNull)
    {
        putUvarint(o, 0);
        return;
    }
    putUvarint(o, cast(ulong) b.length + 1);
    o.append(b);
}

/// Compact array length prefix: uvarint(count+1). A null array is count -1 -> 0.
private void putCArrLen(ref ByteBuffer o, int count) @nogc nothrow
{
    putUvarint(o, count < 0 ? 0 : cast(ulong) count + 1);
}

/// Empty tagged-fields section (dreads never emits tagged fields).
private void putTaggedFields(ref ByteBuffer o) @nogc nothrow
{
    putUvarint(o, 0);
}

/// Backpatch a compact array count that was reserved as a FIXED-width 5-byte
/// uvarint placeholder (see reserveCArrLen). Encodes count+1 into the 5 bytes at
/// off using continuation bits, so the width never changes regardless of value.
private void patchCArrLen(ref ByteBuffer o, size_t off, int count) @nogc nothrow
{
    auto d = o.data;
    if (off + 5 > d.length)
        return;
    ulong v = count < 0 ? 0 : cast(ulong) count + 1;
    foreach (k; 0 .. 4)
    {
        d[off + k] = cast(ubyte)((v & 0x7F) | 0x80);
        v >>= 7;
    }
    d[off + 4] = cast(ubyte)(v & 0x7F); // final byte, no continuation
}

/// Reserve a 5-byte fixed-width uvarint slot for a compact array count to be
/// backpatched later (when the count isn't known up front). Returns its offset.
private size_t reserveCArrLen(ref ByteBuffer o) @nogc nothrow
{
    immutable off = o.length;
    foreach (_; 0 .. 5)
        o.appendByte(0x80); // placeholder continuation bytes; patched later
    return off;
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

    // --- flexible-dialect readers ---

    /// Unsigned LEB128 varint, capped at 5 bytes (32-bit values on the wire).
    uint uvarint() @nogc nothrow
    {
        uint v = 0;
        int shift = 0;
        foreach (_; 0 .. 5)
        {
            if (i >= p.length)
            {
                ok = false;
                return 0;
            }
            immutable b = p[i++];
            v |= (cast(uint)(b & 0x7F)) << shift;
            if ((b & 0x80) == 0)
                return v;
            shift += 7;
        }
        ok = false; // overlong
        return 0;
    }

    /// Compact string: uvarint(length+1); 0 = null.
    const(char)[] cstr() @nogc nothrow
    {
        immutable n = uvarint();
        if (!ok || n == 0)
            return null;
        immutable len = n - 1;
        if (i + len > p.length)
        {
            ok = false;
            return null;
        }
        auto s = cast(const(char)[]) p[i .. i + len];
        i += len;
        return s;
    }

    /// Compact bytes: uvarint(length+1); 0 = null.
    const(ubyte)[] cbytes() @nogc nothrow
    {
        immutable n = uvarint();
        if (!ok || n == 0)
            return null;
        immutable len = n - 1;
        if (i + len > p.length)
        {
            ok = false;
            return null;
        }
        auto b = p[i .. i + len];
        i += len;
        return b;
    }

    /// Compact array length: uvarint(count+1) -> count, or -1 for a null array.
    int carrlen() @nogc nothrow
    {
        immutable n = uvarint();
        if (!ok)
            return 0;
        return n == 0 ? -1 : cast(int)(n - 1);
    }

    /// Skip a tagged-fields section: uvarint count, then each {tag uvarint, size
    /// uvarint, size bytes}. dreads understands no tags, so it skips them all.
    void skipTaggedFields() @nogc nothrow
    {
        immutable cnt = uvarint();
        if (!ok)
            return;
        foreach (_; 0 .. cnt)
        {
            if (!ok)
                return;
            cast(void) uvarint(); // tag
            immutable sz = uvarint();
            if (!ok || i + sz > p.length)
            {
                ok = false;
                return;
            }
            i += sz;
        }
    }
}

/// Valid topic name for the flat keyspace key kafka.t.<topic>.<p>: non-empty,
/// bounded, and no '.'/control bytes that would make (topic="a.5",p=0) and
/// (topic="a",p=5) collide on the same list key.
private bool validTopic(scope const(char)[] t) @nogc nothrow pure
{
    // Kafka's real rule: 1..249 chars from [a-zA-Z0-9._-], and not "." / "..".
    // The old rule rejected legal DOTTED topics (my.topic.name) and accepted
    // illegal specials ($#!) — librdkafka 0057 expects INVALID_TOPIC for those.
    // Dots are safe in the flat key kafka.t.<topic>.<p>: keys are constructed,
    // never split back.
    if (t.length == 0 || t.length > 249 || t.length > KAFKA_MAX_TOPIC)
        return false;
    if (t == "." || t == "..")
        return false;
    foreach (ch; t)
    {
        immutable ok = (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z')
            || (ch >= '0' && ch <= '9') || ch == '.' || ch == '_' || ch == '-';
        if (!ok)
            return false;
    }
    return true;
}

/// Topic registry backing all-topics Metadata: topics auto-exist statelessly,
/// but LISTING them needs state — a keyspace set (`kafka.topics`, in the
/// kafka-db) written on the FIRST produce per shard (TLS-deduped, so the hot
/// path pays one AA probe). Keyspace-backed = AOF-persisted + cross-shard via
/// the data plane, no new broadcast op.
private bool[string] tTopicsSeen; // TLS dedupe cache
private void registerTopic(scope const(char)[] t) nothrow @trusted
{
    try
    {
        if ((cast(string) t in tTopicsSeen) !is null)
            return;
        if (gKafkaExec is null)
            return;
        static ByteBuffer rreg; // TLS
        // ONE registry: the same hash CreateTopics writes (topic -> partition
        // count). HSETNX so an explicit CreateTopics count is never clobbered
        // by a later auto-create default.
        const(char)[][4] a = ["hsetnx", KAFKA_TOPIC_REGISTRY, t, "4"];
        gKafkaExec(a[], rreg);
        tTopicsSeen[t.idup] = true;
    }
    catch (Exception)
    {
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

/// KIP-79 time-based lookup: earliest offset whose record timestamp >= ts, or
/// -1 when none matches (Kafka's contract — also the empty-partition answer).
/// Stored blobs are MessageSet v1 entries: [size i32][crc u32][magic i8]
/// [attrs i8][timestamp i64]... — the timestamp sits at bytes 10..18. Scan is
/// budget-capped (conformance partitions are tiny); past the cap = -1.
private long offsetForTime(scope const(char)[] key, long hw, long ts) nothrow @trusted
{
    enum CHUNK = 512;
    enum SCAN_CAP = 4096;
    long scanned = 0;
    while (scanned < hw && scanned < SCAN_CAP)
    {
        long found = -1;
        long idx = scanned;
        immutable got = rangeRecords(key, scanned, CHUNK,
            (scope const(ubyte)[] blob) nothrow {
                if (found >= 0)
                {
                    idx++;
                    return;
                }
                auto rec = parseStoredRec(blob); // internal-v2 OR legacy-v1 blob
                if (rec.ok && rec.ts >= ts)
                    found = idx;
                idx++;
            });
        tHopProbes++; // each chunk is one keyspace hop against the budget
        if (found >= 0)
            return found;
        if (got < CHUNK)
            break; // end of list
        scanned += got;
    }
    return -1;
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
        if (space.length < cast(size_t) avail)
            return; // OOM growing the input buffer: drop THIS client, not the broker
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
            tByteBufferOom = false; // clear any stale flag (leaked from another skin)
            handleRequest(d[pos + 4 .. pos + 4 + sz], outb);
            if (tByteBufferOom)
                return; // OOM building this reply: drop THIS client, not the broker
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
    cast(void) r.str(); // client_id (nullable) — a normal i16 string even in the
    // flexible request header v2, which then adds tagged fields:
    immutable flex = isFlexible(apiKey, apiVer);
    if (flex)
        r.skipTaggedFields(); // flexible request-header tagged fields
    if (!r.ok)
        return;

    // response: [i32 size][i32 correlation][payload]; size patched at the end
    immutable sizeAt = o.length;
    putI32(o, 0);
    putI32(o, corr);
    // Flexible response header v1 adds tagged fields after correlation_id — EXCEPT
    // ApiVersions, which always uses response header v0 (no tagged fields) so a
    // client can parse it before it knows the negotiated version.
    if (flex && apiKey != API_API_VERSIONS)
        putTaggedFields(o);
    immutable bodyAt = o.length;

    // A version beyond what we parse has a shifted layout; parsing it at the
    // pinned dialect misreads counts (OOM) — reject with the error shell. (The
    // ApiVersions dance already tells honest clients our max.)
    if (apiKey != API_API_VERSIONS && apiVer > maxApiVer(apiKey))
    {
        putI16(o, E_UNSUPPORTED_VERSION);
        if (o.length >= sizeAt + 4) // skip the raw patch on OOM (see epilogue note)
        {
            auto de = cast(ubyte[]) o.data;
            immutable esz = o.length - sizeAt - 4;
            de[sizeAt] = cast(ubyte)(esz >> 24);
            de[sizeAt + 1] = cast(ubyte)(esz >> 16);
            de[sizeAt + 2] = cast(ubyte)(esz >> 8);
            de[sizeAt + 3] = cast(ubyte)(esz & 0xFF);
        }
        return;
    }

    switch (apiKey)
    {
    case API_API_VERSIONS:
        // reply v0 regardless; UNSUPPORTED_VERSION + the table lets clients
        // downgrade (the standard dance)
        putI16(o, apiVer == 0 ? E_NONE : E_UNSUPPORTED_VERSION);
        putI32(o, 23); // array count
        static void row(ref ByteBuffer o2, short k, short lo, short hi) @nogc nothrow
        {
            putI16(o2, k);
            putI16(o2, lo);
            putI16(o2, hi);
        }

        row(o, API_PRODUCE, 0, 8);
        row(o, API_FETCH, 0, 11);
        row(o, API_LIST_OFFSETS, 0, 5);
        row(o, API_METADATA, 0, 9);
        row(o, API_API_VERSIONS, 0, 0);
        row(o, API_OFFSET_COMMIT, 0, 7);
        row(o, API_OFFSET_FETCH, 0, 7);
        row(o, API_FIND_COORDINATOR, 0, 3);
        row(o, API_JOIN_GROUP, 0, 7);
        row(o, API_HEARTBEAT, 0, 4);
        row(o, API_LEAVE_GROUP, 0, 4);
        row(o, API_SYNC_GROUP, 0, 5);
        row(o, API_DESCRIBE_CONFIGS, 0, 3);
        row(o, API_CREATE_TOPICS, 0, 4);
        row(o, API_CREATE_PARTITIONS, 0, 1);
        row(o, API_DELETE_TOPICS, 0, 1);
        row(o, API_INCREMENTAL_ALTER_CONFIGS, 0, 0);
        row(o, API_DESCRIBE_GROUPS, 0, 4);
        row(o, API_INIT_PRODUCER_ID, 0, 3);
        row(o, API_ADD_PARTITIONS_TO_TXN, 0, 2);
        row(o, API_ADD_OFFSETS_TO_TXN, 0, 2);
        row(o, API_END_TXN, 0, 2);
        row(o, API_TXN_OFFSET_COMMIT, 0, 3);
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

    case API_FIND_COORDINATOR:
        handleFindCoordinator(r, apiVer, o);
        break;

    case API_JOIN_GROUP:
        handleJoinGroup(r, apiVer, o);
        break;

    case API_SYNC_GROUP:
        handleSyncGroup(r, apiVer, o);
        break;

    case API_LEAVE_GROUP:
        handleLeaveGroup(r, apiVer, o);
        break;

    case API_OFFSET_FETCH:
        handleOffsetFetch(r, apiVer, o);
        break;

    case API_HEARTBEAT:
        handleHeartbeat(r, apiVer, o);
        break;

    case API_OFFSET_COMMIT:
        handleOffsetCommit(r, apiVer, o);
        break;

    case API_DESCRIBE_CONFIGS:
        handleDescribeConfigs(r, apiVer, o);
        break;

    case API_CREATE_TOPICS:
        handleCreateTopics(r, apiVer, o);
        break;

    case API_CREATE_PARTITIONS:
        handleCreatePartitions(r, apiVer, o);
        break;

    case API_DELETE_TOPICS:
        handleDeleteTopics(r, apiVer, o);
        break;

    case API_INCREMENTAL_ALTER_CONFIGS:
        handleIncrementalAlterConfigs(r, apiVer, o);
        break;

    case API_DESCRIBE_GROUPS:
        handleDescribeGroups(r, apiVer, o);
        break;

    case API_INIT_PRODUCER_ID:
        handleInitProducerId(r, apiVer, o);
        break;

    case API_ADD_PARTITIONS_TO_TXN:
        handleAddPartitionsToTxn(r, apiVer, o);
        break;

    case API_ADD_OFFSETS_TO_TXN:
        handleAddOffsetsToTxn(r, apiVer, o);
        break;

    case API_END_TXN:
        handleEndTxn(r, apiVer, o);
        break;

    case API_TXN_OFFSET_COMMIT:
        handleTxnOffsetCommit(r, apiVer, o);
        break;

    default:
        // unknown api: minimal error shell (correlation already written).
        // Clients that see our ApiVersions table never send these.
        putI16(o, E_UNSUPPORTED_VERSION);
        break;
    }

    // patch the size — but only if the 4 reserved length bytes were actually
    // written. If the reserving putI32(o,0) above hit OOM (tByteBufferOom: the
    // append no-ops and o.length stays at sizeAt), the buffer may have no space
    // at [sizeAt..sizeAt+4], so this raw patch would be a 4-byte OOB write past
    // the allocation under -release (heap corruption -> broker crash). Skip it;
    // serveKafkaClient sees tByteBufferOom and drops THIS client (F2 semantics).
    if (o.length >= sizeAt + 4)
    {
        auto d2 = cast(ubyte[]) o.data;
        immutable size = o.length - sizeAt - 4;
        d2[sizeAt] = cast(ubyte)(size >> 24);
        d2[sizeAt + 1] = cast(ubyte)(size >> 16);
        d2[sizeAt + 2] = cast(ubyte)(size >> 8);
        d2[sizeAt + 3] = cast(ubyte)(size & 0xFF);
    }
    cast(void) bodyAt;
}

// Advertised identity (set by server.d at listener setup)
public shared ulong gKafkaProduced; // records stored via Produce (dashboard)
public shared ulong gKafkaFetched;  // records served via Fetch (dashboard)
public __gshared const(char)[] gKafkaHost = "127.0.0.1";
public __gshared ushort gKafkaPort = 9092;
/// When true (default), any named topic auto-exists with KAFKA_PARTITIONS (the
/// stateless model — clients need no CreateTopics). When false, a topic exists
/// only once created (CreateTopics) or produced-into, and Metadata returns
/// UNKNOWN_TOPIC for the rest — which lets an inspector see a missing topic.
public __gshared bool gKafkaAutoCreate = true;

private shared int gKafkaMemberCtr;

/// Generate a broker-assigned member id into `buf`, returns the slice.
private const(char)[] genMemberId(ref char[64] buf) nothrow @trusted
{
    import core.atomic : atomicOp;
    import core.stdc.stdio : snprintf;

    immutable n = atomicOp!"+="(gKafkaMemberCtr, 1);
    immutable k = snprintf(buf.ptr, buf.length, "dreads-%d", n);
    return cast(const(char)[]) buf[0 .. (k > 0 ? k : 0)];
}

/// JoinGroup (v0-v4): single-node coordinator. The joining member is always made
/// the leader of a one-member group at generation 1; franz-go (as leader) then
/// computes the assignment and ships it back in SyncGroup. We echo the member's
/// own subscription metadata so the leader has the member list it needs.
private void handleJoinGroupFlex(ref Rd r, short ver, ref ByteBuffer o) nothrow @trusted
{
    cast(void) r.cstr(); // group_id
    cast(void) r.i32(); // session_timeout_ms
    cast(void) r.i32(); // rebalance_timeout_ms (present from v1; v6+ is flexible)
    auto memberId = r.cstr();
    cast(void) r.cstr(); // group_instance_id (nullable, v5+)
    auto protocolType = r.cstr();
    immutable nproto = r.carrlen();
    const(char)[] protoName;
    const(ubyte)[] protoMeta;
    bool haveProto;
    foreach (i; 0 .. (nproto < 0 ? 0 : safeCount(nproto))) // clamp like siblings
    {
        if (!r.ok)
            break;
        auto name = r.cstr();
        auto meta = r.cbytes();
        r.skipTaggedFields(); // per-protocol tagged fields
        if (!haveProto && r.ok)
        {
            protoName = name;
            protoMeta = meta;
            haveProto = true;
        }
    }
    static char[64] midBuf;
    const(char)[] mid = memberId.length ? memberId : genMemberId(midBuf);
    putI32(o, 0); // throttle_time_ms
    if (!r.ok || !haveProto)
    {
        putI16(o, 0x0010); // COORDINATOR_LOAD_IN_PROGRESS-ish: safe retryable
        putI32(o, -1); // generation
        putCStrNull(o, null, true); // protocol_type (v7+) = null
        putCStrNull(o, null, true); // protocol_name = null
        putCStr(o, ""); // leader
        putCStr(o, mid); // member_id
        putCArrLen(o, 0); // members
        putTaggedFields(o);
        return;
    }
    putI16(o, E_NONE); // error_code
    putI32(o, 1); // generation_id
    putCStrNull(o, protocolType, protocolType is null); // protocol_type (v7+)
    putCStrNull(o, protoName, protoName is null); // protocol_name
    putCStr(o, mid); // leader = this member
    putCStr(o, mid); // member_id
    putCArrLen(o, 1); // members: 1
    putCStr(o, mid); // member_id
    putCStrNull(o, null, true); // group_instance_id (v5+) = null
    putCBytes(o, protoMeta, protoMeta is null); // subscription metadata (echoed)
    putTaggedFields(o); // member tagged fields
    putTaggedFields(o); // response tagged fields
}

/// JoinGroup (v0-v4): single-node coordinator. The joining member is always made
/// the leader of a one-member group at generation 1; franz-go (as leader) then
/// computes the assignment and ships it back in SyncGroup. We echo the member's
/// own subscription metadata so the leader has the member list it needs.
private void handleJoinGroup(ref Rd r, short ver, ref ByteBuffer o) nothrow @trusted
{
    if (isFlexible(API_JOIN_GROUP, ver)) // v6/v7 flexible
    {
        handleJoinGroupFlex(r, ver, o);
        return;
    }
    cast(void) r.str(); // group_id
    cast(void) r.i32(); // session_timeout_ms
    if (ver >= 1)
        cast(void) r.i32(); // rebalance_timeout_ms
    auto memberId = r.str();
    cast(void) r.str(); // protocol_type
    immutable nproto = safeCount(r.i32());
    const(char)[] protoName;
    const(ubyte)[] protoMeta;
    bool haveProto;
    foreach (i; 0 .. nproto)
    {
        if (!r.ok)
            break;
        auto name = r.str();
        auto meta = r.bytesI32();
        if (!haveProto && r.ok)
        {
            protoName = name;
            protoMeta = meta;
            haveProto = true;
        }
    }
    static char[64] midBuf;
    immutable(char)[] emptyName = "";
    const(char)[] mid = memberId.length ? memberId : genMemberId(midBuf);
    if (ver >= 2)
        putI32(o, 0); // throttle_time_ms
    if (!r.ok || !haveProto)
    {
        putI16(o, 0x0010); // COORDINATOR_LOAD_IN_PROGRESS-ish: safe retryable
        putI32(o, -1); // generation
        putStr(o, emptyName);
        putStr(o, emptyName);
        putStr(o, mid);
        putI32(o, 0); // members
        return;
    }
    putI16(o, E_NONE); // error_code
    putI32(o, 1); // generation_id
    putStr(o, protoName); // protocol_name
    putStr(o, mid); // leader = this member
    putStr(o, mid); // member_id
    putI32(o, 1); // members: 1
    putStr(o, mid); // member_id
    putBytesI32(o, protoMeta, protoMeta is null); // subscription metadata (echoed)
}

// Consumer-group committed offsets live in a per-group hash `kafka.cg.<group>`,
// field `<topic>/<partition>` -> decimal offset, via the RESP executor (routed
// to the owning shard exactly like partition data).
private void groupOffKey(scope const(char)[] group, ref ByteBuffer keyb) nothrow @trusted
{
    keyb.clear();
    keyb.append("kafka.cg.");
    keyb.append(group);
}

private const(char)[] partField(scope const(char)[] topic, int part, ref char[KAFKA_MAX_TOPIC + 16] buf) nothrow @trusted
{
    import core.stdc.stdio : snprintf;

    size_t n = topic.length <= KAFKA_MAX_TOPIC ? topic.length : KAFKA_MAX_TOPIC;
    buf[0 .. n] = topic[0 .. n];
    immutable k = snprintf(buf.ptr + n, buf.length - n, "/%d", part);
    return cast(const(char)[]) buf[0 .. n + (k > 0 ? k : 0)];
}

/// HGET the committed offset for one partition, or -1 if none/unset.
private long fetchGroupOffset(scope const(char)[] group, scope const(char)[] topic, int part) nothrow @trusted
{
    if (gKafkaExec is null)
        return -1;
    static ByteBuffer keyb, rb;
    groupOffKey(group, keyb);
    char[KAFKA_MAX_TOPIC + 16] fb = void;
    auto field = partField(topic, part, fb);
    const(char)[][3] a = ["hget", cast(const(char)[]) keyb.data, field];
    gKafkaExec(a[], rb);
    auto d = rb.data;
    // RESP bulk: `$-1\r\n` (nil) or `$<len>\r\n<digits>\r\n`
    if (d.length < 4 || d[0] != '$')
        return -1;
    size_t i = 1;
    bool neg = d[i] == '-';
    if (neg)
        return -1; // nil
    long blen = 0;
    while (i < d.length && d[i] >= '0' && d[i] <= '9')
        blen = blen * 10 + (d[i++] - '0');
    if (i + 2 > d.length)
        return -1;
    i += 2; // skip \r\n
    long off = 0;
    size_t got = 0;
    while (i < d.length && d[i] >= '0' && d[i] <= '9' && got < blen)
    {
        off = off * 10 + (d[i++] - '0');
        got++;
    }
    return got ? off : -1;
}

/// HSET the committed offset for one partition.
private void storeGroupOffset(scope const(char)[] group, scope const(char)[] topic, int part, long off) nothrow @trusted
{
    import core.stdc.stdio : snprintf;

    if (gKafkaExec is null)
        return;
    static ByteBuffer keyb, rb;
    groupOffKey(group, keyb);
    char[KAFKA_MAX_TOPIC + 16] fb = void;
    auto field = partField(topic, part, fb);
    char[24] ob = void;
    immutable k = snprintf(ob.ptr, ob.length, "%lld", off);
    const(char)[][4] a = ["hset", cast(const(char)[]) keyb.data, field,
        cast(const(char)[]) ob[0 .. (k > 0 ? k : 0)]];
    gKafkaExec(a[], rb);
}

/// Committed-offset METADATA lives beside the offset in the same group hash,
/// field `<topic>/<partition>#m` — a separate field so the existing offset
/// value format stays untouched (extend-only). Empty/absent metadata clears
/// the field; OffsetFetch answers null (-1) when the field is absent.
private void storeGroupMeta(scope const(char)[] group, scope const(char)[] topic,
        int part, scope const(char)[] meta) nothrow @trusted
{
    if (gKafkaExec is null)
        return;
    static ByteBuffer keyb, rb;
    groupOffKey(group, keyb);
    char[KAFKA_MAX_TOPIC + 16] fb = void;
    auto field = partField(topic, part, fb);
    char[KAFKA_MAX_TOPIC + 18] mfb = void;
    immutable fl = field.length + 2 <= mfb.length ? field.length : mfb.length - 2;
    mfb[0 .. fl] = field[0 .. fl];
    mfb[fl] = '#';
    mfb[fl + 1] = 'm';
    auto mfield = cast(const(char)[]) mfb[0 .. fl + 2];
    if (meta.length == 0)
    {
        const(char)[][3] a = ["hdel", cast(const(char)[]) keyb.data, mfield];
        gKafkaExec(a[], rb);
    }
    else
    {
        const(char)[][4] a = ["hset", cast(const(char)[]) keyb.data, mfield, meta];
        gKafkaExec(a[], rb);
    }
}

/// HGET the committed metadata for one partition into mb. Returns false when
/// absent (fetch answers a null string).
private bool fetchGroupMeta(scope const(char)[] group, scope const(char)[] topic,
        int part, ref ByteBuffer mb) nothrow @trusted
{
    mb.clear();
    if (gKafkaExec is null)
        return false;
    static ByteBuffer keyb, rb;
    groupOffKey(group, keyb);
    char[KAFKA_MAX_TOPIC + 16] fb = void;
    auto field = partField(topic, part, fb);
    char[KAFKA_MAX_TOPIC + 18] mfb = void;
    immutable fl = field.length + 2 <= mfb.length ? field.length : mfb.length - 2;
    mfb[0 .. fl] = field[0 .. fl];
    mfb[fl] = '#';
    mfb[fl + 1] = 'm';
    auto mfield = cast(const(char)[]) mfb[0 .. fl + 2];
    const(char)[][3] a = ["hget", cast(const(char)[]) keyb.data, mfield];
    gKafkaExec(a[], rb);
    auto d = rb.data;
    if (d.length < 4 || d[0] != '$' || d[1] == '-')
        return false; // nil
    size_t i = 1;
    long blen = 0;
    while (i < d.length && d[i] >= '0' && d[i] <= '9')
        blen = blen * 10 + (d[i++] - '0');
    if (i + 2 > d.length)
        return false;
    i += 2;
    if (i + blen > d.length)
        return false;
    mb.append(cast(const(char)[]) d[i .. i + cast(size_t) blen]);
    return true;
}

/// OffsetCommit (v0-v7): persist the committed offset per partition.
private void handleOffsetCommit(ref Rd r, short ver, ref ByteBuffer o) nothrow @trusted
{
    auto group = r.str();
    if (ver >= 1)
    {
        cast(void) r.i32(); // generation_id
        cast(void) r.str(); // member_id
    }
    if (ver >= 7)
        cast(void) r.str(); // group_instance_id (nullable)
    if (ver >= 2 && ver <= 4)
        cast(void) r.i64(); // retention_time_ms (v2-v4 only)
    immutable ntopics = safeCount(r.i32());
    immutable respStart = o.length;
    if (ver >= 3)
        putI32(o, 0); // throttle_time_ms
    immutable tOff = o.length;
    putI32(o, ntopics);
    int et = 0;
    foreach (_; 0 .. ntopics)
    {
        if (!r.ok || o.length - respStart > KAFKA_MAX_RESP)
            break; // response ceiling: also bounds the HSET hop count
        auto topic = r.str();
        immutable nparts = safeCount(r.i32());
        if (!r.ok)
            break;
        putStr(o, topic);
        immutable pOff = o.length;
        putI32(o, nparts);
        et++;
        int ep = 0;
        foreach (_2; 0 .. nparts)
        {
            if (!r.ok || o.length - respStart > KAFKA_MAX_RESP)
                break;
            immutable part = r.i32();
            immutable off = r.i64();
            if (ver == 1)
                cast(void) r.i64(); // commit_timestamp (v1 only)
            if (ver >= 6)
                cast(void) r.i32(); // committed_leader_epoch
            auto meta = r.str(); // committed_metadata (nullable)
            if (r.ok && validTopic(topic) && part >= 0 && off >= 0)
            {
                storeGroupOffset(group, topic, part, off);
                storeGroupMeta(group, topic, part, meta);
            }
            putI32(o, part);
            putI16(o, E_NONE); // error_code
            ep++;
        }
        patchI32(o, pOff, ep);
    }
    patchI32(o, tOff, et);
}

// ---------------------------------------------------------------------------
// Transaction coordinator (single-node = own coordinator). dreads stores every
// record read-uncommitted, so a transaction needs no control markers and no
// client-record buffering: the producer is handed an id, its partitions/offsets
// are accepted, and EndTxn simply acks. Commit AND abort both leave the produced
// records in the log — a read-uncommitted consumer sees them, and read-committed
// clients page by offset (the golib harness reads uncommitted and distinguishes
// the two purely by record count). This delivers the exactly-once PRODUCE path
// the golib Transaction seam exercises; it does NOT implement broker-side
// producer fencing (the seam's "callback fencing" is enforced client-side).
private shared long gNextProducerId = 1000; // monotonic id source (atomic)

/// InitProducerID (v0-v1): hand a transactional/idempotent producer a fresh
/// producer_id and epoch 0.
private void handleInitProducerId(ref Rd r, short ver, ref ByteBuffer o) nothrow @trusted
{
    import core.atomic : atomicOp;

    if (isFlexible(API_INIT_PRODUCER_ID, ver)) // v2/v3 flexible
    {
        cast(void) r.cstr(); // transactional_id (nullable compact)
        cast(void) r.i32(); // transaction_timeout_ms
        if (ver >= 3)
        {
            cast(void) r.i64(); // producer_id (v3+, for resume/fence)
            cast(void) r.i16(); // producer_epoch (v3+)
        }
        immutable pidf = atomicOp!"+="(gNextProducerId, 1);
        putI32(o, 0); // throttle_time_ms
        putI16(o, E_NONE); // error_code
        putI64(o, pidf); // producer_id
        putI16(o, 0); // producer_epoch
        putTaggedFields(o);
        return;
    }
    cast(void) r.str(); // transactional_id (nullable)
    cast(void) r.i32(); // transaction_timeout_ms
    immutable pid = atomicOp!"+="(gNextProducerId, 1);
    putI32(o, 0); // throttle_time_ms (v0+)
    putI16(o, E_NONE); // error_code
    putI64(o, pid); // producer_id
    putI16(o, 0); // producer_epoch
}

/// AddPartitionsToTxn (v0-v2): accept every partition into the txn (echo E_NONE).
/// Pure echo, no cross-shard hop.
private void handleAddPartitionsToTxn(ref Rd r, short ver, ref ByteBuffer o) nothrow @trusted
{
    cast(void) r.str(); // transactional_id
    cast(void) r.i64(); // producer_id
    cast(void) r.i16(); // producer_epoch
    immutable ntopics = safeCount(r.i32());
    putI32(o, 0); // throttle_time_ms
    immutable tOff = o.length;
    putI32(o, ntopics);
    int et = 0;
    foreach (_; 0 .. ntopics)
    {
        if (!r.ok || o.length > KAFKA_MAX_RESP)
            break;
        auto topic = r.str();
        immutable nparts = safeCount(r.i32());
        if (!r.ok)
            break;
        putStr(o, topic);
        immutable pOff = o.length;
        putI32(o, nparts);
        et++;
        int ep = 0;
        foreach (_2; 0 .. nparts)
        {
            if (!r.ok)
                break;
            immutable part = r.i32();
            if (!r.ok)
                break;
            putI32(o, part);
            putI16(o, E_NONE); // error_code
            ep++;
        }
        patchI32(o, pOff, ep);
    }
    patchI32(o, tOff, et);
}

/// AddOffsetsToTxn (v0-v2): register the consumer group with the txn — ack only
/// (the offsets themselves arrive via TxnOffsetCommit).
private void handleAddOffsetsToTxn(ref Rd r, short ver, ref ByteBuffer o) nothrow @trusted
{
    cast(void) r.str(); // transactional_id
    cast(void) r.i64(); // producer_id
    cast(void) r.i16(); // producer_epoch
    cast(void) r.str(); // group
    putI32(o, 0); // throttle_time_ms
    putI16(o, E_NONE); // error_code
}

/// EndTxn (v0-v2): commit or abort. Both just ack — records are already stored
/// (read-uncommitted), so there is nothing to flush or roll back.
private void handleEndTxn(ref Rd r, short ver, ref ByteBuffer o) nothrow @trusted
{
    cast(void) r.str(); // transactional_id
    cast(void) r.i64(); // producer_id
    cast(void) r.i16(); // producer_epoch
    cast(void) r.i8(); // committed (commit vs abort)
    putI32(o, 0); // throttle_time_ms
    putI16(o, E_NONE); // error_code
}

/// TxnOffsetCommit (v0-v2): persist consumer offsets as part of the txn. Same
/// keyspace as OffsetCommit (kafka.cg.<group>), so OffsetFetch reads them back;
/// mirrors handleOffsetCommit's interleaved parse+HSET-hop+emit (hop-safe: the
/// group/topic slices point into the fiber's own request buffer, o is the
/// per-connection reply buffer, storeGroupOffset's TLS args are consumed before
/// the park).
/// TxnOffsetCommit v3+ (flexible): v3 adds generation_id/member_id/
/// group_instance_id (KIP-447). Persists offsets to kafka.cg.<group> like the
/// classic path (hop-safe interleave; see handleOffsetCommit).
private void handleTxnOffsetCommitFlex(ref Rd r, short ver, ref ByteBuffer o) nothrow @trusted
{
    cast(void) r.cstr(); // transactional_id
    auto group = r.cstr();
    cast(void) r.i64(); // producer_id
    cast(void) r.i16(); // producer_epoch
    cast(void) r.i32(); // generation_id (v3+)
    cast(void) r.cstr(); // member_id (v3+)
    cast(void) r.cstr(); // group_instance_id (nullable, v3+)
    immutable rawn = r.carrlen();
    immutable ntopics = rawn < 0 ? 0 : safeCount(rawn);
    immutable respStart = o.length;
    putI32(o, 0); // throttle_time_ms
    immutable tOff = reserveCArrLen(o);
    int et = 0;
    foreach (_; 0 .. ntopics)
    {
        if (!r.ok || o.length - respStart > KAFKA_MAX_RESP)
            break;
        auto topic = r.cstr();
        immutable rawp = r.carrlen();
        immutable nparts = rawp < 0 ? 0 : safeCount(rawp);
        if (!r.ok)
            break;
        putCStr(o, topic);
        immutable pOff = reserveCArrLen(o);
        int ep = 0;
        foreach (_2; 0 .. nparts)
        {
            if (!r.ok || o.length - respStart > KAFKA_MAX_RESP)
                break;
            immutable part = r.i32();
            immutable off = r.i64();
            cast(void) r.i32(); // committed_leader_epoch (v2+)
            cast(void) r.cstr(); // metadata (nullable compact)
            r.skipTaggedFields(); // partition tagged fields
            if (r.ok && validTopic(topic) && part >= 0 && off >= 0)
                storeGroupOffset(group, topic, part, off);
            putI32(o, part);
            putI16(o, E_NONE); // error_code
            putTaggedFields(o); // partition tagged fields
            ep++;
        }
        r.skipTaggedFields(); // topic tagged fields
        patchCArrLen(o, pOff, ep);
        putTaggedFields(o); // topic tagged fields
        et++;
    }
    patchCArrLen(o, tOff, et);
    putTaggedFields(o); // response tagged fields
}

private void handleTxnOffsetCommit(ref Rd r, short ver, ref ByteBuffer o) nothrow @trusted
{
    if (isFlexible(API_TXN_OFFSET_COMMIT, ver)) // v3 flexible
    {
        handleTxnOffsetCommitFlex(r, ver, o);
        return;
    }
    cast(void) r.str(); // transactional_id
    auto group = r.str();
    cast(void) r.i64(); // producer_id
    cast(void) r.i16(); // producer_epoch
    immutable ntopics = safeCount(r.i32());
    immutable respStart = o.length;
    putI32(o, 0); // throttle_time_ms
    immutable tOff = o.length;
    putI32(o, ntopics);
    int et = 0;
    foreach (_; 0 .. ntopics)
    {
        if (!r.ok || o.length - respStart > KAFKA_MAX_RESP)
            break; // response ceiling: also bounds the HSET hop count
        auto topic = r.str();
        immutable nparts = safeCount(r.i32());
        if (!r.ok)
            break;
        putStr(o, topic);
        immutable pOff = o.length;
        putI32(o, nparts);
        et++;
        int ep = 0;
        foreach (_2; 0 .. nparts)
        {
            if (!r.ok || o.length - respStart > KAFKA_MAX_RESP)
                break;
            immutable part = r.i32();
            immutable off = r.i64();
            if (ver >= 2)
                cast(void) r.i32(); // committed_leader_epoch (v2+)
            cast(void) r.str(); // metadata (nullable)
            if (r.ok && validTopic(topic) && part >= 0 && off >= 0)
                storeGroupOffset(group, topic, part, off);
            putI32(o, part);
            putI16(o, E_NONE); // error_code
            ep++;
        }
        patchI32(o, pOff, ep);
    }
    patchI32(o, tOff, et);
}

/// Read one RESP bulk string `$<len>\r\n<bytes>\r\n` from d at i; advances i.
/// Returns null on nil (`$-1`) or malformation (and parks i at end).
private const(ubyte)[] respBulk(scope const(ubyte)[] d, ref size_t i) nothrow @trusted
{
    if (i >= d.length || d[i] != '$')
    {
        i = d.length;
        return null;
    }
    i++;
    if (i < d.length && d[i] == '-')
    { // nil: `$-1\r\n`
        while (i < d.length && d[i] != '\n')
            i++;
        if (i < d.length)
            i++;
        return null;
    }
    long len = 0;
    while (i < d.length && d[i] >= '0' && d[i] <= '9')
        len = len * 10 + (d[i++] - '0');
    i += 2; // \r\n
    if (len < 0 || i + cast(size_t) len > d.length)
    {
        i = d.length;
        return null;
    }
    auto s = d[i .. i + cast(size_t) len];
    i += cast(size_t) len + 2; // value + trailing \r\n
    return s;
}

/// OffsetFetch fetch-all (topics == null): HGETALL the group hash and emit every
/// committed offset, grouped by topic. Used by kadm.FetchOffsets(group).
// Fetch-all group offsets: HGETALL kafka.cg.<group>, parsed once into these
// per-shard TLS arrays (slices point into tGoRb, valid until the next exec),
// then emitted by either the classic or the flexible OffsetFetch responder.
private const(char)[][4096] tGoTf; // topic slice
private int[4096] tGoTp; // partition
private long[4096] tGoTo; // committed offset
private bool[4096] tGoDone; // grouping marker
private ByteBuffer tGoRb; // HGETALL reply (backing for tGoTf slices)

private size_t parseGroupOffsets(scope const(char)[] group) nothrow @trusted
{
    static ByteBuffer keyb;
    if (gKafkaExec is null)
        return 0;
    groupOffKey(group, keyb);
    const(char)[][2] a = ["hgetall", cast(const(char)[]) keyb.data];
    gKafkaExec(a[], tGoRb);
    auto d = cast(const(ubyte)[]) tGoRb.data;
    size_t nf = 0;
    size_t i = 0;
    if (d.length >= 1 && d[0] == '*')
    {
        i = 1;
        long n = 0;
        while (i < d.length && d[i] >= '0' && d[i] <= '9')
            n = n * 10 + (d[i++] - '0');
        i += 2; // \r\n
        for (long e = 0; e + 1 < n && nf < tGoTf.length; e += 2)
        {
            auto field = respBulk(d, i);
            auto val = respBulk(d, i);
            if (field is null || val is null)
                break;
            size_t sl = field.length; // split "topic/partition" on the LAST '/'
            foreach_reverse (k; 0 .. field.length)
                if (field[k] == '/')
                {
                    sl = k;
                    break;
                }
            if (sl >= field.length)
                continue;
            if (field.length >= 2 && field[$ - 2] == '#' && field[$ - 1] == 'm')
                continue; // metadata sibling field (`topic/part#m`), not a partition
            int part = 0;
            foreach (c; field[sl + 1 .. $])
                if (c >= '0' && c <= '9')
                    part = part * 10 + (c - '0');
            long off = 0;
            bool neg = val.length && val[0] == '-';
            foreach (c; val[(neg ? 1 : 0) .. $])
                if (c >= '0' && c <= '9')
                    off = off * 10 + (c - '0');
            tGoTf[nf] = cast(const(char)[]) field[0 .. sl];
            tGoTp[nf] = part;
            tGoTo[nf] = neg ? -off : off;
            tGoDone[nf] = false;
            nf++;
        }
    }
    return nf;
}

private void emitAllGroupOffsets(scope const(char)[] group, short ver, ref ByteBuffer o) nothrow @trusted
{
    immutable nf = parseGroupOffsets(group);
    immutable tOff = o.length;
    putI32(o, 0);
    int et = 0;
    foreach (idx; 0 .. nf)
    {
        if (tGoDone[idx])
            continue;
        putStr(o, tGoTf[idx]);
        immutable pOff = o.length;
        putI32(o, 0);
        int ep = 0;
        foreach (j; idx .. nf)
        {
            if (tGoDone[j] || tGoTf[j] != tGoTf[idx])
                continue;
            tGoDone[j] = true;
            putI32(o, tGoTp[j]);
            putI64(o, tGoTo[j]);
            if (ver >= 5)
                putI32(o, -1); // committed_leader_epoch
            putI16(o, -1); // metadata = null
            putI16(o, E_NONE);
            ep++;
        }
        patchI32(o, pOff, ep);
        et++;
    }
    patchI32(o, tOff, et);
}

/// Flexible (v6+) fetch-all: compact topics/partitions arrays + tagged fields.
private void emitAllGroupOffsetsFlex(scope const(char)[] group, short ver, ref ByteBuffer o) nothrow @trusted
{
    immutable nf = parseGroupOffsets(group);
    immutable tOff = reserveCArrLen(o);
    int et = 0;
    foreach (idx; 0 .. nf)
    {
        if (tGoDone[idx])
            continue;
        putCStr(o, tGoTf[idx]);
        immutable pOff = reserveCArrLen(o);
        int ep = 0;
        foreach (j; idx .. nf)
        {
            if (tGoDone[j] || tGoTf[j] != tGoTf[idx])
                continue;
            tGoDone[j] = true;
            putI32(o, tGoTp[j]);
            putI64(o, tGoTo[j]);
            putI32(o, -1); // committed_leader_epoch (v5+)
            putCStrNull(o, null, true); // metadata = null
            putI16(o, E_NONE);
            putTaggedFields(o); // partition tagged fields
            ep++;
        }
        patchCArrLen(o, pOff, ep);
        putTaggedFields(o); // topic tagged fields
        et++;
    }
    patchCArrLen(o, tOff, et);
}

/// OffsetFetch (v0-v5): committed offsets for the requested partitions, or all
/// (topics == null) via HGETALL.
/// OffsetFetch v6+ (flexible): compact strings/arrays + tagged fields; v7 adds
/// require_stable to the request.
private void handleOffsetFetchFlex(ref Rd r, short ver, ref ByteBuffer o) nothrow @trusted
{
    auto group = r.cstr();
    immutable rawN = r.carrlen(); // -1 = null topics (fetch all)
    immutable respStart = o.length;
    putI32(o, 0); // throttle_time_ms
    if (rawN < 0)
    {
        emitAllGroupOffsetsFlex(group, ver, o);
    }
    else
    {
        immutable ntopics = safeCount(rawN);
        immutable topicsOff = reserveCArrLen(o);
        int emittedTopics = 0;
        foreach (_; 0 .. ntopics)
        {
            if (!r.ok || o.length - respStart > KAFKA_MAX_RESP)
                break;
            auto topic = r.cstr();
            immutable rawP = r.carrlen();
            immutable nparts = rawP < 0 ? 0 : safeCount(rawP);
            if (!r.ok)
                break;
            putCStr(o, topic);
            immutable partsOff = reserveCArrLen(o);
            int emittedParts = 0;
            foreach (_2; 0 .. nparts)
            {
                if (!r.ok || o.length - respStart > KAFKA_MAX_RESP)
                    break;
                immutable part = r.i32();
                immutable off = (r.ok && validTopic(topic) && part >= 0)
                    ? fetchGroupOffset(group, topic, part) : -1;
                static ByteBuffer mbf; // TLS: consumed before the next hop
                immutable hasMeta = off >= 0 && fetchGroupMeta(group, topic, part, mbf);
                putI32(o, part);
                putI64(o, off); // committed_offset (-1 = none)
                putI32(o, -1); // committed_leader_epoch (v5+)
                if (hasMeta)
                    putCStr(o, cast(const(char)[]) mbf.data);
                else
                    putCStrNull(o, null, true); // metadata = null
                putI16(o, E_NONE); // error_code
                putTaggedFields(o); // partition tagged fields
                emittedParts++;
            }
            r.skipTaggedFields(); // per-topic request tagged fields
            patchCArrLen(o, partsOff, emittedParts);
            putTaggedFields(o); // topic tagged fields
            emittedTopics++;
        }
        patchCArrLen(o, topicsOff, emittedTopics);
    }
    putI16(o, E_NONE); // top-level error_code (v2+)
    putTaggedFields(o); // response tagged fields
}

private void handleOffsetFetch(ref Rd r, short ver, ref ByteBuffer o) nothrow @trusted
{
    if (isFlexible(API_OFFSET_FETCH, ver)) // v6/v7 flexible
    {
        handleOffsetFetchFlex(r, ver, o);
        return;
    }
    auto group = r.str();
    immutable rawN = r.i32();
    immutable respStart = o.length;
    if (ver >= 3)
        putI32(o, 0); // throttle_time_ms
    if (rawN < 0)
    {
        emitAllGroupOffsets(group, ver, o); // null topics = fetch all
    }
    else
    {
        immutable ntopics = safeCount(rawN);
        immutable topicsCountOff = o.length;
        putI32(o, ntopics);
        int emittedTopics = 0;
        foreach (_; 0 .. ntopics)
        {
            if (!r.ok || o.length - respStart > KAFKA_MAX_RESP)
                break; // response ceiling: also bounds the HGET hop count
            auto topic = r.str();
            immutable nparts = safeCount(r.i32());
            if (!r.ok)
                break;
            putStr(o, topic);
            immutable partsOff = o.length;
            putI32(o, nparts);
            emittedTopics++;
            int emittedParts = 0;
            foreach (_2; 0 .. nparts)
            {
                if (!r.ok || o.length - respStart > KAFKA_MAX_RESP)
                    break;
                immutable part = r.i32();
                immutable off = (r.ok && validTopic(topic) && part >= 0)
                    ? fetchGroupOffset(group, topic, part) : -1;
                static ByteBuffer mb; // TLS: consumed before the next hop
                immutable hasMeta = off >= 0 && fetchGroupMeta(group, topic, part, mb);
                putI32(o, part);
                putI64(o, off); // committed_offset (-1 = none)
                if (ver >= 5)
                    putI32(o, -1); // committed_leader_epoch
                if (hasMeta)
                    putStr(o, cast(const(char)[]) mb.data);
                else
                    putI16(o, -1); // metadata = null
                putI16(o, E_NONE); // error_code
                emittedParts++;
            }
            patchI32(o, partsOff, emittedParts);
        }
        patchI32(o, topicsCountOff, emittedTopics);
    }
    if (ver >= 2)
        putI16(o, E_NONE); // top-level error_code
}

/// Heartbeat (v0-v3): always OK (single-member group never rebalances).
private void handleHeartbeat(ref Rd r, short ver, ref ByteBuffer o) nothrow @trusted
{
    if (isFlexible(API_HEARTBEAT, ver)) // v4 flexible
    {
        cast(void) r.cstr(); // group_id
        cast(void) r.i32(); // generation_id
        cast(void) r.cstr(); // member_id
        cast(void) r.cstr(); // group_instance_id (nullable, v3+)
        putI32(o, 0); // throttle_time_ms
        putI16(o, E_NONE); // error_code
        putTaggedFields(o);
        return;
    }
    cast(void) r.str(); // group_id
    cast(void) r.i32(); // generation_id
    cast(void) r.str(); // member_id
    if (ver >= 3)
        cast(void) r.str(); // group_instance_id
    if (ver >= 1)
        putI32(o, 0); // throttle_time_ms
    putI16(o, E_NONE); // error_code
}

/// SyncGroup (v0-v3): the leader shipped the computed assignment; echo this
/// member's assignment back. Single-member group, so we return the (only)
/// assignment the leader provided.
private void handleSyncGroupFlex(ref Rd r, short ver, ref ByteBuffer o) nothrow @trusted
{
    cast(void) r.cstr(); // group_id
    cast(void) r.i32(); // generation_id
    auto memberId = r.cstr(); // this member
    cast(void) r.cstr(); // group_instance_id (nullable, v3+)
    auto protocolType = r.cstr(); // v5+ nullable
    auto protocolName = r.cstr(); // v5+ nullable
    immutable nassign = r.carrlen();
    const(ubyte)[] myAssignment;
    bool found;
    foreach (i; 0 .. (nassign < 0 ? 0 : safeCount(nassign))) // clamp like siblings
    {
        if (!r.ok)
            break;
        auto mid = r.cstr();
        auto assign = r.cbytes();
        r.skipTaggedFields();
        if (r.ok && (!found || mid == memberId))
        {
            myAssignment = assign;
            found = true;
        }
    }
    putI32(o, 0); // throttle_time_ms
    putI16(o, E_NONE); // error_code
    putCStrNull(o, protocolType, protocolType is null); // v5+
    putCStrNull(o, protocolName, protocolName is null); // v5+
    putCBytes(o, myAssignment, myAssignment is null); // member assignment
    putTaggedFields(o);
}

private void handleSyncGroup(ref Rd r, short ver, ref ByteBuffer o) nothrow @trusted
{
    if (isFlexible(API_SYNC_GROUP, ver)) // v4/v5 flexible
    {
        handleSyncGroupFlex(r, ver, o);
        return;
    }
    cast(void) r.str(); // group_id
    cast(void) r.i32(); // generation_id
    auto memberId = r.str(); // this member
    if (ver >= 3)
        cast(void) r.str(); // group_instance_id (nullable)
    immutable nassign = safeCount(r.i32());
    const(ubyte)[] myAssignment;
    bool found;
    foreach (i; 0 .. nassign)
    {
        if (!r.ok)
            break;
        auto mid = r.str();
        auto assign = r.bytesI32();
        if (r.ok && (!found || mid == memberId))
        {
            myAssignment = assign;
            found = true;
        }
    }
    if (ver >= 1)
        putI32(o, 0); // throttle_time_ms
    putI16(o, E_NONE); // error_code
    putBytesI32(o, myAssignment, myAssignment is null); // member assignment
}

/// LeaveGroup (v0-v3): acknowledge. v3+ is a batched leave (members array).
private void handleLeaveGroup(ref Rd r, short ver, ref ByteBuffer o) nothrow @trusted
{
    if (isFlexible(API_LEAVE_GROUP, ver)) // v4 flexible (batched members)
    {
        cast(void) r.cstr(); // group_id
        const(char)[][32] fmids; // stack-local (no hop in this handler)
        const(char)[][32] fgiis;
        bool[32] fgiiNull;
        size_t nm;
        immutable rawc = r.carrlen();
        immutable cnt = rawc < 0 ? 0 : safeCount(rawc);
        foreach (i; 0 .. cnt)
        {
            if (!r.ok)
                break;
            auto mid = r.cstr();
            auto gii = r.cstr(); // group_instance_id (nullable compact)
            r.skipTaggedFields(); // member tagged fields
            if (nm < fmids.length && r.ok)
            {
                fmids[nm] = mid;
                fgiis[nm] = gii;
                fgiiNull[nm] = (gii is null);
                nm++;
            }
        }
        putI32(o, 0); // throttle_time_ms
        putI16(o, E_NONE); // error_code
        putCArrLen(o, cast(int) nm); // members
        foreach (i; 0 .. nm)
        {
            putCStr(o, fmids[i]); // member_id
            putCStrNull(o, fgiis[i], fgiiNull[i]); // group_instance_id
            putI16(o, E_NONE); // member error_code
            putTaggedFields(o); // member tagged fields
        }
        putTaggedFields(o); // response tagged fields
        return;
    }
    cast(void) r.str(); // group_id
    static const(char)[][32] mids;
    static bool[32] giiNull;
    static const(char)[][32] giis;
    size_t nm;
    if (ver >= 3)
    {
        immutable cnt = safeCount(r.i32());
        foreach (i; 0 .. cnt)
        {
            if (!r.ok)
                break;
            auto mid = r.str();
            auto gii = r.str(); // group_instance_id (nullable)
            if (nm < mids.length && r.ok)
            {
                mids[nm] = mid;
                giis[nm] = gii;
                giiNull[nm] = (gii is null);
                nm++;
            }
        }
    }
    else
    {
        auto mid = r.str();
        if (r.ok)
        {
            mids[0] = mid;
            nm = 1;
        }
    }
    if (ver >= 1)
        putI32(o, 0); // throttle_time_ms (LeaveGroup: v1+)
    putI16(o, E_NONE); // error_code
    if (ver >= 3)
    {
        putI32(o, cast(int) nm); // members
        foreach (i; 0 .. nm)
        {
            putStr(o, mids[i]); // member_id
            if (giiNull[i])
                putI16(o, -1); // group_instance_id = null
            else
                putStr(o, giis[i]);
            putI16(o, E_NONE); // member error_code
        }
    }
}

/// DescribeConfigs (v0-v3): a stateless broker exposes no per-resource config,
/// so echo each requested resource with an empty config list (error 0). This is
/// enough for kadm/franz-go Topics() (Inspector conformance) to enumerate topics.
private void handleDescribeConfigs(ref Rd r, short ver, ref ByteBuffer o) nothrow @trusted
{
    immutable nres = safeCount(r.i32());
    static byte[256] rtype;
    static const(char)[][256] rname;
    size_t nr;
    foreach (_; 0 .. nres)
    {
        if (!r.ok)
            break;
        immutable t = r.i8();
        auto name = r.str();
        immutable nkeys = r.i32(); // configuration_keys (-1 = null/all)
        if (nkeys >= 0)
            foreach (_2; 0 .. safeCount(nkeys))
            {
                if (!r.ok)
                    break;
                cast(void) r.str();
            }
        if (nr < rtype.length && r.ok)
        {
            rtype[nr] = cast(byte) t;
            rname[nr] = name;
            nr++;
        }
    }
    putI32(o, 0); // throttle_time_ms
    putI32(o, cast(int) nr); // results
    foreach (i; 0 .. nr)
    {
        putI16(o, E_NONE); // error_code
        putI16(o, -1); // error_message = null
        o.appendByte(cast(char) rtype[i]); // resource_type
        putStr(o, rname[i]); // resource_name
        // resource_type 2 == TOPIC: emit the fixed defaults an inspector needs
        // (a stateless broker has no per-topic overrides); others: empty.
        if (rtype[i] == 2)
        {
            putI32(o, cast(int) KAFKA_TOPIC_CONFIGS.length);
            foreach (cfg; KAFKA_TOPIC_CONFIGS)
                putConfigEntry(o, ver, cfg[0], cfg[1]);
        }
        else
            putI32(o, 0);
    }
}

/// Fixed topic-config defaults (a stateless broker has no per-topic state). The
/// keys/values satisfy the golib inspector's required set + its parsers.
private static immutable string[2][11] KAFKA_TOPIC_CONFIGS = [
    ["min.insync.replicas", "1"],
    ["cleanup.policy", "delete"],
    ["retention.ms", "-1"],
    ["retention.bytes", "-1"],
    ["delete.retention.ms", "86400000"],
    ["min.compaction.lag.ms", "0"],
    ["max.compaction.lag.ms", "9223372036854775807"],
    ["min.cleanable.dirty.ratio", "0.5"],
    ["segment.bytes", "1073741824"],
    ["segment.ms", "604800000"],
    ["unclean.leader.election.enable", "false"],
];

/// Emit one DescribeConfigs config entry, version-correct.
private void putConfigEntry(ref ByteBuffer o, short ver, scope const(char)[] name,
        scope const(char)[] value) @nogc nothrow
{
    putStr(o, name);
    putStr(o, value); // value (non-null)
    o.appendByte(0); // read_only = false
    o.appendByte(0); // v0 is_default / v1+ config_source (both a single byte)
    o.appendByte(0); // is_sensitive = false
    if (ver >= 1)
        putI32(o, 0); // synonyms: empty array
    if (ver >= 3)
    {
        o.appendByte(0); // config_type (0 = unknown)
        putI16(o, -1); // documentation = null
    }
}

/// True if the group has any committed offsets (HLEN kafka.cg.<group> > 0).
private bool groupExists(scope const(char)[] group) nothrow @trusted
{
    if (gKafkaExec is null)
        return false;
    static ByteBuffer keyb, rb;
    groupOffKey(group, keyb);
    const(char)[][2] a = ["hlen", cast(const(char)[]) keyb.data];
    gKafkaExec(a[], rb);
    auto d = rb.data;
    return d.length >= 3 && d[0] == ':' && d[1] != '0'; // :N\r\n with N>0
}

/// DescribeGroups (v0-v4): a group with committed offsets is "Empty" (our group
/// model doesn't persist live membership), an unknown group is "Dead". Members
/// are reported empty; the inspector derives partitions from OffsetFetch.
private void handleDescribeGroups(ref Rd r, short ver, ref ByteBuffer o) nothrow @trusted
{
    immutable ngroups = safeCount(r.i32());
    // STACK-local: groupExists hops cross-shard and yields; a shared static would
    // be clobbered by another connection's handleDescribeGroups during the park.
    const(char)[][64] groups;
    size_t ng;
    foreach (_; 0 .. ngroups)
    {
        if (!r.ok)
            break;
        auto g = r.str();
        if (ng < groups.length && r.ok)
            groups[ng++] = g;
    }
    if (ver >= 1)
        putI32(o, 0); // throttle_time_ms
    putI32(o, cast(int) ng); // groups count
    foreach (i; 0 .. ng)
    {
        immutable exists = groupExists(groups[i]);
        putI16(o, E_NONE); // error_code
        putStr(o, groups[i]); // group_id
        putStr(o, exists ? "Empty" : "Dead"); // group_state
        putStr(o, exists ? "consumer" : ""); // protocol_type
        putStr(o, ""); // protocol_data / assignment protocol
        putI32(o, 0); // members: empty
        if (ver >= 3)
            putI32(o, 0); // authorized_operations
    }
}

private void handleFindCoordinator(ref Rd r, short ver, ref ByteBuffer o) nothrow @trusted
{
    // request: [key string] (v0); v1+ adds [key_type i8]. Ignored — the
    // single-node broker is always its own group/txn coordinator.
    if (isFlexible(API_FIND_COORDINATOR, ver)) // v3 (single key; v4+ batches keys)
    {
        cast(void) r.cstr(); // key (compact string)
        cast(void) r.i8(); // key_type
        putI32(o, 0); // throttle_time_ms
        putI16(o, E_NONE); // error_code
        putCStrNull(o, null, true); // error_message = null
        putI32(o, 0); // node_id = us
        putCStr(o, gKafkaHost);
        putI32(o, gKafkaPort);
        putTaggedFields(o);
        return;
    }
    cast(void) r.str();
    if (ver >= 1)
        cast(void) r.i8();
    if (ver >= 1)
        putI32(o, 0); // throttle_time_ms
    putI16(o, E_NONE); // error_code
    if (ver >= 1)
        putI16(o, -1); // error_message = null
    putI32(o, 0); // node_id = us
    putStr(o, gKafkaHost);
    putI32(o, gKafkaPort);
}

// Topic registry: created topics live in the hash `kafka.topics`, field=name,
// value=partition count. This gives topic EXISTENCE (an unregistered topic with
// no data is UNKNOWN — so an inspector sees it missing) plus the exact created
// partition count. Auto-create is OFF: clients CreateTopics before producing.
private enum string KAFKA_TOPIC_REGISTRY = "kafka.topics";

private void registerTopic(scope const(char)[] topic, int nparts) nothrow @trusted
{
    import core.stdc.stdio : snprintf;

    if (gKafkaExec is null)
        return;
    static ByteBuffer rb;
    char[16] nb = void;
    immutable k = snprintf(nb.ptr, nb.length, "%d", nparts);
    const(char)[][4] a = ["hset", KAFKA_TOPIC_REGISTRY, topic,
        cast(const(char)[]) nb[0 .. (k > 0 ? k : 0)]];
    gKafkaExec(a[], rb);
}

/// Registered partition count for a topic, or -1 if not in the registry.
private int registeredTopicPartitions(scope const(char)[] topic) nothrow @trusted
{
    if (gKafkaExec is null || !validTopic(topic))
        return -1;
    static ByteBuffer rb;
    const(char)[][3] a = ["hget", KAFKA_TOPIC_REGISTRY, topic];
    gKafkaExec(a[], rb);
    auto d = rb.data;
    if (d.length < 4 || d[0] != '$' || d[1] == '-')
        return -1; // nil = not registered
    size_t i = 1;
    long blen = 0;
    while (i < d.length && d[i] >= '0' && d[i] <= '9')
        blen = blen * 10 + (d[i++] - '0');
    if (i + 2 > d.length)
        return -1;
    i += 2; // \r\n
    long v = 0;
    size_t got = 0;
    while (i < d.length && d[i] >= '0' && d[i] <= '9' && got < blen)
    {
        v = v * 10 + (d[i++] - '0');
        got++;
    }
    return got ? cast(int) v : -1;
}

/// CreateTopics (v0-v4): register each topic's partition count. Auto-create is
/// off, so this is how a topic comes into existence.
private void handleCreateTopics(ref Rd r, short ver, ref ByteBuffer o) nothrow @trusted
{
    immutable ntopics = safeCount(r.i32());
    // STACK-local (not TLS static): registerTopic below hops cross-shard and
    // yields; a shared static would be clobbered by another connection's
    // handleCreateTopics during the park, registering the wrong topic.
    const(char)[][64] names;
    int[64] nparts;
    size_t nt;
    foreach (_; 0 .. ntopics)
    {
        if (!r.ok)
            break;
        auto name = r.str();
        immutable np = r.i32(); // num_partitions
        cast(void) r.i16(); // replication_factor
        immutable nassign = safeCount(r.i32());
        foreach (_2; 0 .. nassign)
        {
            if (!r.ok)
                break;
            cast(void) r.i32(); // partition_index
            immutable nb = safeCount(r.i32());
            foreach (_3; 0 .. nb)
                cast(void) r.i32(); // broker id
        }
        immutable ncfg = safeCount(r.i32());
        foreach (_2; 0 .. ncfg)
        {
            if (!r.ok)
                break;
            cast(void) r.str(); // config name
            cast(void) r.str(); // config value (nullable)
        }
        if (nt < names.length && r.ok)
        {
            names[nt] = name;
            immutable c = np > 0 ? np : cast(int) KAFKA_PARTITIONS;
            nparts[nt] = c > KAFKA_MAX_PARTITIONS ? KAFKA_MAX_PARTITIONS : c; // cap
            nt++;
        }
    }
    foreach (i; 0 .. nt)
        if (validTopic(names[i]))
            registerTopic(names[i], nparts[i]);
    if (ver >= 2)
        putI32(o, 0); // throttle_time_ms
    putI32(o, cast(int) nt); // topics count
    foreach (i; 0 .. nt)
    {
        putStr(o, names[i]);
        putI16(o, E_NONE); // error_code
        if (ver >= 1)
            putI16(o, -1); // error_message = null
    }
}

/// CreatePartitions (v0-v1): grow a registered topic's partition count. The
/// registry is the source of truth; unknown topic = 3, shrink/same = 37
/// (INVALID_PARTITIONS), growth re-registers the new count (capped).
private void handleCreatePartitions(ref Rd r, short ver, ref ByteBuffer o) nothrow @trusted
{
    immutable ntopics = safeCount(r.i32());
    const(char)[][64] names;
    int[64] counts;
    short[64] errs;
    size_t nt;
    foreach (_; 0 .. ntopics)
    {
        if (!r.ok)
            break;
        auto name = r.str();
        immutable cnt = r.i32(); // new total partition count
        immutable nassign = r.i32(); // assignments: nullable array (-1 = null)
        if (nassign > 0)
            foreach (_2; 0 .. safeCount(nassign))
            {
                if (!r.ok)
                    break;
                immutable nb = safeCount(r.i32()); // broker_ids per new partition
                foreach (_3; 0 .. nb)
                    cast(void) r.i32();
            }
        if (nt < names.length && r.ok)
        {
            names[nt] = name;
            counts[nt] = cnt;
            nt++;
        }
    }
    cast(void) r.i32(); // timeout_ms
    immutable validateOnly = r.ok && r.i8() != 0;
    foreach (i; 0 .. nt)
    {
        immutable cur = registeredTopicPartitions(names[i]);
        if (!validTopic(names[i]) || cur < 0)
            errs[i] = E_UNKNOWN_TOPIC;
        else if (counts[i] <= cur)
            errs[i] = 37; // INVALID_PARTITIONS
        else
        {
            errs[i] = E_NONE;
            if (!validateOnly)
            {
                immutable c = counts[i] > KAFKA_MAX_PARTITIONS
                    ? KAFKA_MAX_PARTITIONS : counts[i];
                registerTopic(names[i], c);
            }
        }
    }
    putI32(o, 0); // throttle_time_ms (v0+)
    putI32(o, cast(int) nt); // results
    foreach (i; 0 .. nt)
    {
        putStr(o, names[i]);
        putI16(o, errs[i]);
        putI16(o, -1); // error_message = null
    }
}

// Topic configs (IncrementalAlterConfigs) live in a per-topic hash
// `kafka.tcfg.<topic>`, field = config name, value = config value. Only
// storage + the compaction gate read them; DescribeConfigs keeps its static
// defaults (extend-only).
private void topicCfgKey(scope const(char)[] topic, ref ByteBuffer keyb) nothrow @trusted
{
    keyb.clear();
    keyb.append("kafka.tcfg.");
    keyb.append(topic);
}

/// HGET one topic config into vb; false when unset.
private bool topicCfgGet(scope const(char)[] topic, scope const(char)[] name,
        ref ByteBuffer vb) nothrow @trusted
{
    vb.clear();
    if (gKafkaExec is null)
        return false;
    static ByteBuffer keyb, rb;
    topicCfgKey(topic, keyb);
    const(char)[][3] a = ["hget", cast(const(char)[]) keyb.data, name];
    gKafkaExec(a[], rb);
    auto d = rb.data;
    if (d.length < 4 || d[0] != '$' || d[1] == '-')
        return false;
    size_t i = 1;
    long blen = 0;
    while (i < d.length && d[i] >= '0' && d[i] <= '9')
        blen = blen * 10 + (d[i++] - '0');
    if (i + 2 > d.length || i + 2 + blen > d.length)
        return false;
    i += 2;
    vb.append(cast(const(char)[]) d[i .. i + cast(size_t) blen]);
    return true;
}

/// Does cleanup.policy contain "compact"? (comma-separated list semantics)
private bool topicCompacted(scope const(char)[] topic) nothrow @trusted
{
    static ByteBuffer vb;
    if (!topicCfgGet(topic, "cleanup.policy", vb))
        return false;
    auto v = cast(const(char)[]) vb.data;
    // substring scan is enough: "compact" only appears as a policy token
    if (v.length < 7)
        return false;
    foreach (i; 0 .. v.length - 6)
        if (v[i .. i + 7] == "compact")
            return true;
    return false;
}

/// IncrementalAlterConfigs v0: SET/DELETE/APPEND/SUBTRACT on per-topic configs.
/// Non-topic resource types are acked without effect (broker configs are
/// compile-time here). validate_only skips the writes.
private void handleIncrementalAlterConfigs(ref Rd r, short ver, ref ByteBuffer o) nothrow @trusted
{
    immutable nres = safeCount(r.i32());
    // response staged AFTER the full parse (writes hop cross-shard mid-parse
    // is fine — slices point into the stable request buffer)
    byte[16] rtypes;
    const(char)[][16] rnames;
    short[16] rerrs;
    size_t nr;
    // first pass records the ops; they are applied after validate_only is read
    struct Op
    {
        size_t res;
        const(char)[] name;
        byte op;
        const(char)[] value;
        bool valueNull;
    }

    Op[64] ops;
    size_t nops;
    foreach (_; 0 .. nres)
    {
        if (!r.ok)
            break;
        immutable rtype = r.i8();
        auto rname = r.str();
        immutable ncfg = safeCount(r.i32());
        foreach (_2; 0 .. ncfg)
        {
            if (!r.ok)
                break;
            auto cname = r.str();
            immutable opt = r.i8();
            auto cval = r.str(); // nullable (null slice when absent)
            if (nops < ops.length && r.ok)
            {
                ops[nops] = Op(nr, cname, opt, cval, cval is null);
                nops++;
            }
        }
        if (nr < rnames.length && r.ok)
        {
            rtypes[nr] = rtype;
            rnames[nr] = rname;
            rerrs[nr] = E_NONE;
            nr++;
        }
    }
    immutable validateOnly = r.ok && r.i8() != 0;
    if (!validateOnly && gKafkaExec !is null)
        foreach (i; 0 .. nops)
        {
            auto op = ops[i];
            if (op.res >= nr || rtypes[op.res] != 2) // 2 = TOPIC resource
                continue;
            auto topic = rnames[op.res];
            if (!validTopic(topic) || op.name.length == 0 || op.name.length > 64)
                continue;
            static ByteBuffer keyb, rb, curb;
            topicCfgKey(topic, keyb);
            // copy the key: topicCfgGet below reuses its own TLS keyb safely,
            // but gKafkaExec hops may interleave other fibers' handlers.
            char[128 + 16] kstore = void;
            immutable kl = keyb.length <= kstore.length ? keyb.length : kstore.length;
            kstore[0 .. kl] = cast(const(char)[]) keyb.data[0 .. kl];
            auto key = cast(const(char)[]) kstore[0 .. kl];
            switch (op.op)
            {
            default: // unknown op: ignore (acked E_NONE like a no-op)
                break;
            case 0: // SET
                {
                    const(char)[][4] a = ["hset", key, op.name, op.value];
                    gKafkaExec(a[], rb);
                    break;
                }
            case 1: // DELETE
                {
                    const(char)[][3] a = ["hdel", key, op.name];
                    gKafkaExec(a[], rb);
                    break;
                }
            case 2: // APPEND (comma-list union)
            case 3: // SUBTRACT (comma-list removal)
                {
                    cast(void) topicCfgGet(topic, op.name, curb);
                    // build the new list in a stack buffer
                    char[512] nb = void;
                    size_t nl;
                    auto cur = cast(const(char)[]) curb.data;
                    bool removed_or_present = false;
                    size_t st = 0;
                    foreach (ci; 0 .. cur.length + 1)
                    {
                        if (ci == cur.length || cur[ci] == ',')
                        {
                            auto tok = cur[st .. ci];
                            st = ci + 1;
                            if (tok.length == 0)
                                continue;
                            immutable isTarget = tok == op.value;
                            if (isTarget)
                                removed_or_present = true;
                            if (op.op == 3 && isTarget)
                                continue; // SUBTRACT drops it
                            if (nl + tok.length + 1 <= nb.length)
                            {
                                if (nl)
                                    nb[nl++] = ',';
                                nb[nl .. nl + tok.length] = tok;
                                nl += tok.length;
                            }
                        }
                    }
                    if (op.op == 2 && !removed_or_present && op.value.length
                            && nl + op.value.length + 1 <= nb.length)
                    {
                        if (nl)
                            nb[nl++] = ',';
                        nb[nl .. nl + op.value.length] = op.value;
                        nl += op.value.length;
                    }
                    if (nl == 0)
                    {
                        const(char)[][3] a = ["hdel", key, op.name];
                        gKafkaExec(a[], rb);
                    }
                    else
                    {
                        const(char)[][4] a = ["hset", key, op.name,
                            cast(const(char)[]) nb[0 .. nl]];
                        gKafkaExec(a[], rb);
                    }
                    break;
                }
            }
        }
    putI32(o, 0); // throttle_time_ms
    putI32(o, cast(int) nr); // responses
    foreach (i; 0 .. nr)
    {
        putI16(o, rerrs[i]);
        putI16(o, -1); // error_message = null
        o.appendByte(cast(char) rtypes[i]);
        putStr(o, rnames[i]);
    }
}

/// DeleteTopics (v0-v1): unregister the topic, drop its partition lists and
/// configs. The stateless-metadata compat default still auto-exists names on
/// Metadata, so "deleted" means "registry + data gone".
private void handleDeleteTopics(ref Rd r, short ver, ref ByteBuffer o) nothrow @trusted
{
    immutable ntopics = safeCount(r.i32());
    const(char)[][64] names;
    size_t nt;
    foreach (_; 0 .. ntopics)
    {
        if (!r.ok)
            break;
        auto name = r.str();
        if (nt < names.length && r.ok)
            names[nt++] = name;
    }
    cast(void) r.i32(); // timeout_ms
    foreach (i; 0 .. nt)
    {
        if (gKafkaExec is null || !validTopic(names[i]))
            continue;
        immutable np = registeredTopicPartitions(names[i]);
        immutable cnt = np > 0 ? (np > KAFKA_MAX_PARTITIONS ? KAFKA_MAX_PARTITIONS : np)
            : cast(int) KAFKA_PARTITIONS;
        static ByteBuffer kb2, rb;
        foreach (p2; 0 .. cnt)
        {
            partKey(names[i], p2, kb2);
            char[8 + KAFKA_MAX_TOPIC + 16] kst = void;
            immutable kl = kb2.length <= kst.length ? kb2.length : kst.length;
            kst[0 .. kl] = cast(const(char)[]) kb2.data[0 .. kl];
            const(char)[][2] a = ["del", cast(const(char)[]) kst[0 .. kl]];
            gKafkaExec(a[], rb);
        }
        static ByteBuffer cfgk;
        topicCfgKey(names[i], cfgk);
        char[128 + 16] cst = void;
        immutable cl = cfgk.length <= cst.length ? cfgk.length : cst.length;
        cst[0 .. cl] = cast(const(char)[]) cfgk.data[0 .. cl];
        const(char)[][2] a2 = ["del", cast(const(char)[]) cst[0 .. cl]];
        gKafkaExec(a2[], rb);
        const(char)[][3] a3 = ["hdel", KAFKA_TOPIC_REGISTRY, names[i]];
        gKafkaExec(a3[], rb);
    }
    if (ver >= 1)
        putI32(o, 0); // throttle_time_ms
    putI32(o, cast(int) nt); // results
    foreach (i; 0 .. nt)
    {
        putStr(o, names[i]);
        putI16(o, E_NONE);
    }
}

/// Partition count reported in Metadata for a topic: the contiguous run of
/// POPULATED partitions from 0 (probing the keyspace kafka.t.<topic>.<p>), so a
/// topic produced into with N contiguous partitions reports N. Falls back to
/// KAFKA_PARTITIONS for an empty/fresh topic so a producer always has partitions
/// to write to. Derived from the keyspace — no per-topic partition-count state.
/// Per-Metadata-request budget on CROSS-SHARD LLEN probes, so a request naming
/// many topics (each with many populated partitions) can't drive a hop storm.
/// Owner-shard probes (gKafkaLenRaw, no hop) are free and don't count.
private enum size_t KAFKA_META_PROBE_BUDGET = 1024;
private size_t tMetaProbes; // TLS, reset at the top of handleMetadata
/// Per-request budget on cross-shard partLen hops in Fetch/ListOffsets, reset at
/// each handler's top. Owner-shard reads (gKafkaLenRaw, no hop) are free and
/// never counted, so single-shard mode never trips it — only sharded mode, where
/// a 64 MB request naming millions of tiny partition entries would otherwise
/// drive a partLen hop storm across sibling shards (same guard as Metadata).
private size_t tHopProbes;

private int topicPartitionCount(scope const(char)[] topic) nothrow @trusted
{
    if (!validTopic(topic))
        return cast(int) KAFKA_PARTITIONS;
    static ByteBuffer kb; // TLS scratch (consumed synchronously per probe)
    int count = 0;
    foreach (p; 0 .. 64) // hard cap on partitions per topic
    {
        partKey(topic, p, kb);
        auto key = kb.data.asChars;
        long len = gKafkaLenRaw !is null ? gKafkaLenRaw(key) : -1;
        if (len < 0)
        {
            if (tMetaProbes >= KAFKA_META_PROBE_BUDGET)
                break; // hop budget exhausted: stop probing cross-shard
            tMetaProbes++;
            len = partLen(key); // not the owner shard: data-plane LLEN (hops)
        }
        if (len <= 0)
            break; // first empty partition ends the contiguous run
        count++;
    }
    return count; // 0 for an empty topic (caller decides exists-vs-unknown)
}

/// Resolve a topic's advertised partition count and error code (shared by the
/// classic and flexible Metadata responses). reg>=0 = created; else glob-probe;
/// else auto-exist (compat) or UNKNOWN_TOPIC (registry mode).
private void metaTopicParts(scope const(char)[] t, ref int np, ref short terr) nothrow @trusted
{
    immutable reg = registeredTopicPartitions(t);
    terr = E_NONE;
    if (reg >= 0)
        np = reg;
    else
    {
        np = topicPartitionCount(t);
        if (np == 0)
        {
            if (gKafkaAutoCreate)
                np = cast(int) KAFKA_PARTITIONS;
            else
                terr = E_UNKNOWN_TOPIC;
        }
    }
    if (np > KAFKA_MAX_PARTITIONS)
        np = KAFKA_MAX_PARTITIONS;
}

/// Metadata v9+ (flexible dialect: compact strings/arrays + tagged fields).
private void handleMetadataFlex(ref Rd r, short ver, ref ByteBuffer o) nothrow @trusted
{
    // request: topics COMPACT array of { name compact-string, TAGGED_FIELDS };
    // then allow_auto_topic_creation + include_*_authorized_operations bools.
    immutable rawn = r.carrlen(); // -1 = null array (all topics)
    immutable ntopics = rawn < 0 ? 0 : safeCount(rawn);
    const(char)[][512] topics; // all-topics window: a full-suite run registers hundreds
    size_t nt = 0;
    static ByteBuffer allBuf; // TLS: SMEMBERS reply for the all-topics form
    if (rawn < 0 && gKafkaExec !is null)
    {
        // all-topics request: list the produce-time registry (kafka.topics).
        allBuf.clear();
        const(char)[][2] aq = ["hkeys", KAFKA_TOPIC_REGISTRY];
        gKafkaExec(aq[], allBuf);
        // parse *N\r\n($L\r\n<member>\r\n)* into the same topics[] window
        auto d = cast(const(char)[]) allBuf.data;
        size_t i = 0;
        if (i < d.length && d[i] == '*')
        {
            i++;
            long n2 = 0;
            while (i < d.length && d[i] != '\r')
                n2 = n2 * 10 + (d[i++] - '0');
            i += 2;
            foreach (_k; 0 .. n2)
            {
                if (i >= d.length || d[i] != '$')
                    break;
                i++;
                long ln = 0;
                while (i < d.length && d[i] != '\r')
                    ln = ln * 10 + (d[i++] - '0');
                i += 2;
                if (ln < 0 || i + ln > d.length)
                    break;
                auto m = d[i .. i + cast(size_t) ln];
                i += cast(size_t) ln + 2;
                if (nt < topics.length && validTopic(m))
                    topics[nt++] = m;
            }
        }
    }
    foreach (_; 0 .. ntopics)
    {
        if (!r.ok)
            break;
        auto t = r.cstr();
        r.skipTaggedFields(); // per-topic tagged fields
        if (nt < topics.length && t !is null && t.length)
        {
            bool dup = false;
            foreach (k; 0 .. nt)
                if (topics[k] == t)
                {
                    dup = true;
                    break;
                }
            if (!dup)
                topics[nt++] = t; // invalid names STAY: emitted with error 17
        }
    }
    tMetaProbes = 0;
    // The explicit-topic Metadata request is Kafka's auto-create moment:
    // register each VALID requested topic so the all-topics form lists it.
    // Safe to yield here — only fiber-local state (o) is staged.
    foreach (t; topics[0 .. nt])
        if (validTopic(t))
            registerTopic(t);

    putI32(o, 0); // throttle_time_ms
    // brokers: just us
    putCArrLen(o, 1);
    putI32(o, 0); // node_id
    putCStr(o, gKafkaHost);
    putI32(o, gKafkaPort);
    putCStrNull(o, null, true); // rack: null
    putTaggedFields(o); // broker tagged fields
    putCStrNull(o, "dreads-cluster", false); // cluster_id (0063/0121 read it)
    putI32(o, 0); // controller_id
    // topics
    putCArrLen(o, cast(int) nt);
    foreach (t; topics[0 .. nt])
    {
        if (!validTopic(t))
        {
            // requested-but-illegal name: report it WITH error 17 — filtering
            // it out left producers retrying metadata forever (librdkafka 0057).
            putI16(o, E_INVALID_TOPIC);
            putCStr(o, t);
            o.appendByte(0); // is_internal
            putCArrLen(o, 0); // no partitions
            putI32(o, -2147483648);
            putTaggedFields(o);
            continue;
        }
        int np;
        short terr;
        metaTopicParts(t, np, terr);
        putI16(o, terr);
        putCStr(o, t);
        o.appendByte(0); // is_internal = false
        putCArrLen(o, np);
        foreach (int p2; 0 .. np)
        {
            putI16(o, E_NONE);
            putI32(o, p2); // partition_index
            putI32(o, 0); // leader_id
            putI32(o, 0); // leader_epoch (v7+)
            putCArrLen(o, 1); // replica_nodes
            putI32(o, 0);
            putCArrLen(o, 1); // isr_nodes
            putI32(o, 0);
            putCArrLen(o, 0); // offline_replicas (v5+): none
            putTaggedFields(o); // partition tagged fields
        }
        putI32(o, -2147483648); // topic_authorized_operations (v8+): not computed
        putTaggedFields(o); // topic tagged fields
    }
    putI32(o, -2147483648); // cluster_authorized_operations (v8..v10)
    putTaggedFields(o); // response-level tagged fields
}

private void handleMetadata(ref Rd r, short ver, ref ByteBuffer o) nothrow
{
    if (isFlexible(API_METADATA, ver))
    {
        handleMetadataFlex(r, ver, o);
        return;
    }
    // request: [topics: array of string] (null/empty = all — we answer only
    // named topics; a fresh producer always names what it wants)
    immutable ntopics = safeCount(r.i32());
    // STACK-local (not TLS static): topicPartitionCount below hops cross-shard
    // and YIELDS, and a shared static would be clobbered by another connection's
    // handleMetadata during the park (its topic slices would replace ours). The
    // slices point into the request buffer, which this fiber owns across the yield.
    const(char)[][512] topics; // all-topics window: a full-suite run registers hundreds
    size_t nt = 0;
    foreach (_; 0 .. ntopics)
    {
        if (!r.ok)
            break;
        auto t = r.str();
        if (nt < topics.length && t !is null && validTopic(t))
        {
            bool dup = false;
            foreach (k; 0 .. nt)
                if (topics[k] == t)
                {
                    dup = true; // dedup: a repeated name must not multiply the probes
                    break;
                }
            if (!dup)
                topics[nt++] = t;
        }
    }
    tMetaProbes = 0; // reset the per-request cross-shard LLEN-probe budget

    // brokers: just us
    putI32(o, 1);
    putI32(o, 0); // node_id 0
    putStr(o, gKafkaHost);
    putI32(o, gKafkaPort);
    if (ver >= 1)
        putI16(o, -1); // rack: null
    if (ver >= 2)
    {
        // cluster_id (nullable string): a real id — clusterid tests read it
        enum cid = "dreads-cluster";
        putI16(o, cast(short) cid.length);
        o.append(cid);
    }
    if (ver >= 1)
        putI32(o, 0); // controller_id
    // topics — STATELESS: every named topic exists with KAFKA_PARTITIONS
    putI32(o, cast(int) nt);
    foreach (t; topics[0 .. nt])
    {
        immutable reg = registeredTopicPartitions(t); // -1 if not created
        int np;
        short terr = E_NONE;
        if (reg >= 0)
            np = reg; // created topic: exact registered partition count
        else
        {
            np = topicPartitionCount(t); // glob: 0 if empty
            if (np == 0)
            {
                if (gKafkaAutoCreate)
                    np = cast(int) KAFKA_PARTITIONS; // auto-exist (compat default)
                else
                    terr = E_UNKNOWN_TOPIC; // registry mode: topic is missing
            }
        }
        if (np > KAFKA_MAX_PARTITIONS)
            np = KAFKA_MAX_PARTITIONS; // defense-in-depth vs a huge registered count
        putI16(o, terr);
        putStr(o, t);
        if (ver >= 1)
            o.appendByte(0); // is_internal = false
        putI32(o, np);
        foreach (int p2; 0 .. np)
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
            if (!validTopic(topic))
                err = E_INVALID_TOPIC; // illegal NAME is 17, not unknown-topic
            else if (part < 0)
                err = E_UNKNOWN_TOPIC;
            // Compaction gate (KIP-purgatory parity): a compacted topic
            // rejects keyless records with INVALID_RECORD + record_errors.
            // The config HGET hops cross-shard, so it runs HERE — before any
            // of the TLS statics below are staged (topic/records slice the
            // stable request buffer).
            immutable compacted = err == E_NONE && validTopic(topic) && part >= 0
                && topicCompacted(topic);
            int[1024] invIdx = void; // batch indices of keyless records
            size_t ninv = 0;
            // topic registration happens in Metadata (the auto-create moment):
            // a gKafkaExec here would YIELD mid-produce with kb/blobArena TLS
            // statics staged — the exact clobber hazard documented above.
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
                    if (compacted && kn && ninv < invIdx.length)
                        invIdx[ninv++] = cast(int) nrec;
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
                if (compacted && msz >= 18)
                {
                    // v1 message: key length i32 sits after crc+magic+attrs+ts
                    immutable int klen = (cast(int) msg[14] << 24) | (cast(int) msg[15] << 16)
                        | (cast(int) msg[16] << 8) | msg[17];
                    if (klen == -1 && ninv < invIdx.length)
                        invIdx[ninv++] = cast(int) nrec;
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
            if (ninv > 0 && err == E_NONE)
            {
                err = E_INVALID_RECORD; // whole batch rejected, nothing stored
                nrec = 0;
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
            if (ver >= 5)
                putI64(o, 0); // log_start_offset (v5+)
            if (ver >= 8)
            {
                if (err == E_INVALID_RECORD && ninv > 0)
                {
                    // record_errors names the offending batch indices; the
                    // client fails those with INVALID_RECORD and the rest of
                    // the batch with _INVALID_DIFFERENT_RECORD (KIP-467).
                    putI32(o, cast(int) ninv);
                    foreach (k2; 0 .. ninv)
                    {
                        putI32(o, invIdx[k2]);
                        putI16(o, -1); // batch_index_error_message = null
                    }
                    putStr(o, "Compacted topic cannot accept message without key");
                }
                else
                {
                    putI32(o, 0); // record_errors: empty array (v8+, KIP-467)
                    putI16(o, -1); // error_message: null (v8+)
                }
            }
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
    if (ver >= 7)
    {
        cast(void) r.i32(); // session_id
        cast(void) r.i32(); // session_epoch
    }
    tHopProbes = 0; // per-request cross-shard partLen budget (sharded mode)
    immutable ntopics = safeCount(r.i32());
    if (ver >= 1)
        putI32(o, 0); // throttle
    if (ver >= 7)
    {
        putI16(o, E_NONE); // error_code (v7+)
        putI32(o, 0); // session_id (v7+)
    }
    immutable topicsCountOff = o.length; // backpatched to emittedTopics below
    putI32(o, ntopics);
    int emittedTopics = 0;
    foreach (_; 0 .. ntopics)
    {
        if (!r.ok)
            break;
        if (o.length > KAFKA_MAX_RESP || tHopProbes >= KAFKA_META_PROBE_BUDGET)
            break; // capped: stop before misreading the next topic (desync-safe)
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
            if (o.length > KAFKA_MAX_RESP || tHopProbes >= KAFKA_META_PROBE_BUDGET)
                break; // response ceiling / cross-shard hop budget (DoS)
            immutable part = r.i32();
            if (ver >= 9)
                cast(void) r.i32(); // current_leader_epoch (v9+)
            immutable fetchOff = r.i64();
            if (ver >= 5)
                cast(void) r.i64(); // log_start_offset (v5+)
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
            {
                tHopProbes++; // count the cross-shard hop against the budget
                hw = partLen(key);
            }
            immutable overCap = o.length > KAFKA_MAX_RESP; // response ceiling
            immutable bad = fetchOff < 0 || fetchOff > hw || !validTopic(topic) || part < 0;
            putI32(o, part);
            putI16(o, bad ? E_OFFSET_OUT_OF_RANGE : E_NONE);
            putI64(o, hw); // high watermark
            if (ver >= 4)
            {
                putI64(o, hw); // last_stable_offset (no transactions => == hw)
                if (ver >= 5)
                    putI64(o, 0); // log_start_offset (v5+)
                putI32(o, 0); // aborted_transactions: empty array
                if (ver >= 11)
                    putI32(o, -1); // preferred_read_replica (v11+): none
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
    if (ver >= 2)
        cast(void) r.i8(); // isolation_level (v2+)
    tHopProbes = 0; // per-request cross-shard partLen budget (sharded mode)
    immutable ntopics = safeCount(r.i32());
    if (ver >= 2)
        putI32(o, 0); // throttle_time_ms (v2+)
    immutable topicsCountOff = o.length; // backpatched to emittedTopics below
    putI32(o, ntopics);
    int emittedTopics = 0;
    foreach (_; 0 .. ntopics)
    {
        if (!r.ok)
            break;
        if (o.length > KAFKA_MAX_RESP || tHopProbes >= KAFKA_META_PROBE_BUDGET)
            break; // capped: stop before misreading the next topic (desync-safe)
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
            if (o.length > KAFKA_MAX_RESP || tHopProbes >= KAFKA_META_PROBE_BUDGET)
                break; // response ceiling / cross-shard hop budget (DoS)
            immutable part = r.i32();
            if (ver >= 4)
                cast(void) r.i32(); // current_leader_epoch (v4+)
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
            {
                tHopProbes++; // count the cross-shard hop against the budget
                hw = partLen(k3);
            }
            long off;
            if (ts == -2)
                off = 0; // earliest
            else if (ts < 0)
                off = hw; // -1 = latest (high watermark)
            else
                off = offsetForTime(k3, hw, ts); // KIP-79; -1 = no match
            putI32(o, part);
            putI16(o, E_NONE);
            if (ver >= 1)
            {
                putI64(o, -1); // timestamp
                putI64(o, off);
                if (ver >= 4)
                    putI32(o, -1); // leader_epoch (v4+)
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
    if (space.length < ulen)
        return false; // allocation failed (OOM)
    size_t outLen = ulen;
    if (snappy_uncompress(cast(const(char)*) src.ptr, src.length,
            cast(char*) space.ptr, &outLen) != 0)
        return false;
    if (outLen != ulen)
        return false;
    dst.grow(outLen); // mark the decompressed bytes as filled
    return true;
}

// libzstd C API (vendored static libzstd.a). Kafka's v2 zstd payload is a raw
// zstd frame; franz-go writes the content size into the frame header, so
// ZSTD_getFrameContentSize gives a bomb-bound before we allocate.
private extern (C) @nogc nothrow @system
{
    ulong ZSTD_getFrameContentSize(const(void)* src, size_t srcSize);
    size_t ZSTD_decompress(void* dst, size_t dstCap, const(void)* src, size_t srcSize);
    uint ZSTD_isError(size_t code);
    void* ZSTD_createDCtx();
    size_t ZSTD_freeDCtx(void* dctx);
    size_t ZSTD_decompressStream(void* zds, ZstdOutBuf* output, ZstdInBuf* input);
}

private struct ZstdInBuf
{
    const(void)* src;
    size_t size;
    size_t pos;
}

private struct ZstdOutBuf
{
    void* dst;
    size_t size;
    size_t pos;
}

/// Bounded zstd decompress into `dst`. Rejects frames with unknown/erroneous
/// content size (can't bomb-bound) or plaintext > capMax.
private bool zstdInto(scope const(ubyte)[] src, ref ByteBuffer dst, size_t capMax) @nogc nothrow @trusted
{
    if (src.length == 0)
        return false;
    immutable ulong content = ZSTD_getFrameContentSize(src.ptr, src.length);
    if (content == ulong.max - 1)
        return false; // ZSTD_CONTENTSIZE_ERROR: not a zstd frame
    if (content == ulong.max)
    {
        // ZSTD_CONTENTSIZE_UNKNOWN: streaming-API writers (librdkafka) omit
        // the size from the frame header. Decompress in bounded chunks —
        // capMax still caps the plaintext (bomb defense), just incrementally.
        auto dctx = ZSTD_createDCtx();
        if (dctx is null)
            return false;
        scope (exit)
            ZSTD_freeDCtx(dctx);
        dst.clear();
        ZstdInBuf inb = ZstdInBuf(src.ptr, src.length, 0);
        enum size_t CHUNK = 64 * 1024;
        for (;;)
        {
            immutable want = dst.length + CHUNK <= capMax ? CHUNK : capMax - dst.length;
            if (want == 0)
                return false; // plaintext exceeds capMax (bomb)
            auto space = dst.freeSpace(want);
            if (space.length < want)
                return false; // allocation failed (OOM)
            ZstdOutBuf outb = ZstdOutBuf(space.ptr, want, 0);
            immutable size_t rc = ZSTD_decompressStream(dctx, &outb, &inb);
            if (ZSTD_isError(rc) != 0)
                return false;
            dst.grow(outb.pos);
            if (rc == 0 && inb.pos >= inb.size)
                return dst.length > 0; // frame complete
            if (outb.pos == 0 && inb.pos >= inb.size)
                return false; // no progress: truncated frame
        }
    }
    if (content == 0 || content > capMax)
        return false; // empty or decompression bomb
    dst.clear();
    auto space = dst.freeSpace(cast(size_t) content);
    if (space.length < cast(size_t) content)
        return false; // allocation failed (OOM)
    immutable size_t n = ZSTD_decompress(space.ptr, cast(size_t) content, src.ptr, src.length);
    if (ZSTD_isError(n) != 0 || n != content)
        return false;
    dst.grow(n);
    return true;
}

/// Request-level ceiling on TOTAL decompressed bytes, reset per Produce request
/// (tKafkaDecompUsed). The per-partition KAFKA_DECOMP_MAX bounds ONE batch, but a
/// request enumerating many partitions each carrying a ~1000:1 frame could
/// otherwise amplify a 64 MB request into tens of GB of decompress+store work.
private enum size_t KAFKA_DECOMP_REQ_MAX = 512 << 20;
private size_t tKafkaDecompUsed; // TLS, reset at the top of handleProduce

/// Decompress a v2 batch's compressed records region (codec = attrs & 0x07)
/// into `dst`. 1=gzip, 2=snappy, 3=lz4, 4=zstd — all Kafka codecs supported;
/// undefined 5/6/7 reject. Bounded by both the per-batch cap and the running
/// per-request budget.
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
    case 4:
        ok = zstdInto(src, dst, cap);
        break;
    default:
        return false; // codecs 5/6/7 are undefined in Kafka
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
