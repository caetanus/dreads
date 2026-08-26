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
import dreads.kafkagroup : KGOP_JOIN, KGOP_JOIN_POLL, KGOP_SYNC, KGOP_HEARTBEAT,
    KGOP_LEAVE, KGOP_DESCRIBE, KGOP_COMMIT_CHECK, KG_NONE, KG_WAIT,
    KG_ILLEGAL_GENERATION, KG_INCONSISTENT_PROTOCOL, KG_UNKNOWN_MEMBER,
    KG_REBALANCE_IN_PROGRESS, KG_MEMBER_ID_REQUIRED, KGOP_DROP, KGOP_SUBSCRIBED,
    KGOP_TXN_INIT, KGOP_TXN_ADD, KGOP_TXN_END, KGOP_TXN_OFFSETS;
import std.digest.crc : crc32Of;

/// Data-plane hook installed by server.d: execute a synthesized RESP command
/// (args[1] is the routing key) on the owner shard, reply RESP bytes.
public __gshared void delegate(scope const(char)[][] args, ref ByteBuffer reply) nothrow gKafkaExec;

/// Group-coordinator hook installed by server.d: run ONE FSM op (see
/// dreads.kafkagroup) on the shard owning `key`, reply the op payload.
public __gshared void delegate(scope const(char)[] key, scope const(ubyte)[] req,
        ref ByteBuffer reply) nothrow gKafkaGroupHop;

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
        API_INCREMENTAL_ALTER_CONFIGS = 44, API_ALTER_CONFIGS = 33,
        API_DESCRIBE_ACLS = 29, API_CREATE_ACLS = 30, API_DELETE_ACLS = 31,
        API_DELETE_RECORDS = 21, API_LIST_GROUPS = 16, API_DELETE_GROUPS = 42,
        API_OFFSET_DELETE = 47;
private enum short API_INIT_PRODUCER_ID = 22, API_ADD_PARTITIONS_TO_TXN = 24,
        API_ADD_OFFSETS_TO_TXN = 25, API_END_TXN = 26, API_TXN_OFFSET_COMMIT = 28;
private enum short API_SASL_HANDSHAKE = 17, API_SASL_AUTHENTICATE = 36;

private enum short E_NONE = 0, E_CORRUPT = 2, E_UNKNOWN_TOPIC = 3,
        E_OFFSET_OUT_OF_RANGE = 1, E_INVALID_TOPIC = 17, E_UNSUPPORTED_VERSION = 35,
        E_INVALID_RECORD = 87, E_OUT_OF_ORDER_SEQ = 45, E_INVALID_PRODUCER_EPOCH = 47,
        E_UNSUPPORTED_SASL_MECHANISM = 33, E_ILLEGAL_SASL_STATE = 34,
        E_SASL_AUTH_FAILED = 58, E_TOPIC_AUTH_FAILED = 29,
        E_GROUP_AUTH_FAILED = 30, E_CLUSTER_AUTH_FAILED = 31;

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
    case API_SASL_HANDSHAKE: return 1; // v1 = tokens via SaslAuthenticate(36)
    case API_SASL_AUTHENTICATE: return 1; // v2+ flexible
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
    case API_ALTER_CONFIGS: return 1; // v2+ flexible
    case API_DESCRIBE_ACLS: return 1; // v2+ flexible
    case API_CREATE_ACLS: return 1; // v2+ flexible
    case API_DELETE_ACLS: return 1; // v2+ flexible
    case API_DELETE_RECORDS: return 1; // v2+ flexible
    case API_LIST_GROUPS: return 0; // v3+ flexible
    case API_DELETE_GROUPS: return 1; // v2+ flexible
    case API_OFFSET_DELETE: return 0; // v0 only (KIP-496)
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

// --- log start offset (DeleteRecords truncation, KIP-107) -----------------
// A partition's base offset lives in the keyspace string `kafka.tb.<t>.<p>`
// (a NEW key family — the list layout is untouched). The HOT PATHS pay ZERO
// for it until the first truncation anywhere: gKafkaTruncEpoch stays 0 and
// partBase() answers 0 with one relaxed atomic load. After a truncation the
// epoch bumps and lookups go through a per-shard TLS cache keyed by epoch.
public shared ulong gKafkaTruncEpoch;

private long[string] tBaseVal; // TLS cache: partition key -> base
private ulong[string] tBaseEpoch; // TLS: epoch the cached value was read at

private void baseKey(scope const(char)[] topic, int part, ref ByteBuffer o) @nogc nothrow
{
    import core.stdc.stdio : snprintf;

    o.clear();
    o.append("kafka.tb.");
    o.append(topic);
    char[16] nb = void;
    immutable n = snprintf(nb.ptr, nb.length, ".%d", part);
    o.append(nb[0 .. n]);
}

/// The partition's log start offset. Zero-cost while nothing was ever
/// truncated; cached per shard afterwards (invalidated by epoch bumps).
private long partBase(scope const(char)[] topic, int part) nothrow @trusted
{
    import core.atomic : atomicLoad;

    immutable ep = atomicLoad(gKafkaTruncEpoch);
    if (ep == 0)
        return 0;
    static ByteBuffer kb4; // TLS: consumed before the hop below
    partKey(topic, part, kb4);
    auto pk = cast(const(char)[]) kb4.data;
    if (auto pe = pk in tBaseEpoch)
        if (*pe == ep)
            if (auto pv = pk in tBaseVal)
                return *pv;
    // miss/stale: GET the base (routed hop), then cache under this epoch
    long v = 0;
    if (gKafkaExec !is null)
    {
        static ByteBuffer bkb, rb;
        baseKey(topic, part, bkb);
        char[10 + KAFKA_MAX_TOPIC + 16] bst = void;
        immutable bl = bkb.length <= bst.length ? bkb.length : bst.length;
        bst[0 .. bl] = cast(const(char)[]) bkb.data[0 .. bl];
        char[8 + KAFKA_MAX_TOPIC + 16] pst = void;
        immutable pl = pk.length <= pst.length ? pk.length : pst.length;
        pst[0 .. pl] = pk[0 .. pl];
        const(char)[][2] a = ["get", cast(const(char)[]) bst[0 .. bl]];
        gKafkaExec(a[], rb);
        auto d = rb.data;
        if (d.length >= 4 && d[0] == '$' && d[1] != '-')
        {
            size_t i2 = 1;
            long blen = 0;
            while (i2 < d.length && d[i2] >= '0' && d[i2] <= '9')
                blen = blen * 10 + (d[i2++] - '0');
            i2 += 2;
            long got = 0;
            while (i2 < d.length && d[i2] >= '0' && d[i2] <= '9' && got < blen)
            {
                v = v * 10 + (d[i2++] - '0');
                got++;
            }
        }
        if (tBaseVal.length > 4096)
        {
            tBaseVal = null; // bounded cache: reset on overflow
            tBaseEpoch = null;
        }
        auto pkStr = (cast(const(char)[]) pst[0 .. pl]).idup;
        tBaseVal[pkStr] = v;
        tBaseEpoch[pkStr] = ep;
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

public void serveKafkaClient(TCPConnection tcp, bool tls = false) nothrow
{
    import dreads.tls : TlsLeg, legPump, legDrainInto, legSend;

    try
        tcp.tcpNoDelay = true;
    catch (Exception)
    {
    }
    TlsLeg* leg;
    if (tls)
    {
        leg = TlsLeg.create(true);
        if (leg is null)
            return;
    }
    scope (exit)
        if (leg !is null)
            leg.free();
    auto wlock = new TaskMutex;
    ByteBuffer inb;
    ByteBuffer outb;
    KafkaConnCtx ctx;
    for (;;)
    {
        if (leg !is null)
        {
            // cipher in -> plaintext lands in inb; the frame loop is unchanged.
            // Single fiber: the handshake flush right after the pump is safe.
            if (!legPump(leg, tcp))
                return;
            legDrainInto(leg, inb);
            if (!legSend(leg, tcp, null))
                return;
        }
        else
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
        }

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
            tKafkaAdvPort = leg !is null && gKafkaTlsPort != 0 ? gKafkaTlsPort : gKafkaPort;
            if (ctx.saslRawMode && !ctx.authed)
            {
                // legacy SaslHandshake v0: this frame is a BARE SASL token
                // (no request header). Reply = [i32 len][server token] — the
                // empty frame for PLAIN, server-first/final for SCRAM rounds.
                static ByteBuffer rawTok; // TLS scratch, consumed right below
                rawTok.clear();
                if (!kafkaSaslStep(d[pos + 4 .. pos + 4 + sz], &ctx, rawTok))
                    return;
                putI32(outb, cast(int) rawTok.length);
                if (!rawTok.empty)
                    outb.append(rawTok.data);
                pos += 4 + sz;
                continue;
            }
            handleRequest(d[pos + 4 .. pos + 4 + sz], outb, &ctx);
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
                if (leg !is null)
                {
                    if (!legSend(leg, tcp, cast(const(ubyte)[]) outb.data))
                        return;
                }
                else
                    tcp.write(outb.data);
            }
            catch (Exception)
                return;
            outb.trim(KAFKA_BUF_KEEP); // a 64MB fetch/produce spike must not pin
        }
        inb.consume(pos);
        if (ctx.closeConn)
            return; // failed/required SASL: reply already flushed above
        if (inb.empty && inb.capacity > KAFKA_BUF_KEEP)
            inb.trim(KAFKA_BUF_KEEP);
    }
}

private void handleRequest(scope const(ubyte)[] req, ref ByteBuffer o,
        KafkaConnCtx* ctx = null) nothrow @trusted
{
    Rd r = Rd(req);
    immutable apiKey = r.i16();
    immutable apiVer = r.i16();
    immutable corr = r.i32();
    tKafkaClientId = r.str(); // client_id (nullable) — a normal i16 string even in the
    tKafkaCtx = ctx; // enforcement principal (read into stack before any hop)
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

    // kafka-require-sasl: everything but the bootstrap trio must authenticate.
    // The bare error shell is length-bounded (the librdkafka lesson: clients
    // read size-first, a short body underflows without desyncing) and the
    // connection drops right after, like a real SASL listener.
    if (gKafkaRequireSasl && ctx !is null && !ctx.authed
            && apiKey != API_API_VERSIONS && apiKey != API_SASL_HANDSHAKE
            && apiKey != API_SASL_AUTHENTICATE)
    {
        putI16(o, E_ILLEGAL_SASL_STATE);
        ctx.closeConn = true;
        goto epilogue;
    }

    switch (apiKey)
    {
    case API_SASL_HANDSHAKE:
        {
            auto mech = r.str();
            immutable isPlain = mech == "PLAIN";
            immutable isS256 = mech == "SCRAM-SHA-256";
            immutable isS512 = mech == "SCRAM-SHA-512";
            immutable known = r.ok && (isPlain || isS256 || isS512);
            putI16(o, known ? E_NONE : E_UNSUPPORTED_SASL_MECHANISM);
            putI32(o, 3); // enabled_mechanisms
            putStr(o, "PLAIN");
            putStr(o, "SCRAM-SHA-256");
            putStr(o, "SCRAM-SHA-512");
            if (known && ctx !is null)
            {
                ctx.saslHandshook = true;
                ctx.mechScram = isS256 || isS512;
                ctx.scram512 = isS512;
                ctx.scramStage = 0;
                if (apiVer == 0)
                    ctx.saslRawMode = true; // next frame(s) = bare tokens
            }
            break;
        }
    case API_SASL_AUTHENTICATE:
        {
            auto tok = r.bytesI32();
            if (ctx is null || !ctx.saslHandshook || !r.ok)
            {
                putI16(o, E_ILLEGAL_SASL_STATE);
                putI16(o, -1); // error_message null
                putI32(o, 0); // auth_bytes empty
                if (apiVer >= 1)
                    putI64(o, 0);
                if (ctx !is null)
                    ctx.closeConn = true;
                break;
            }
            static ByteBuffer rtok; // TLS scratch: consumed synchronously below
            rtok.clear();
            if (kafkaSaslStep(tok, ctx, rtok))
            {
                putI16(o, E_NONE);
                putI16(o, -1);
                putI32(o, cast(int) rtok.length); // server token (SCRAM rounds)
                if (!rtok.empty)
                    o.append(rtok.data);
                if (apiVer >= 1)
                    putI64(o, 0); // session_lifetime_ms: unlimited
            }
            else
            {
                putI16(o, E_SASL_AUTH_FAILED);
                putStr(o, "SASL authentication failed");
                putI32(o, 0);
                if (apiVer >= 1)
                    putI64(o, 0);
                ctx.closeConn = true; // the broker drops after a failed auth
            }
            break;
        }
    case API_API_VERSIONS:
        // reply v0 regardless; UNSUPPORTED_VERSION + the table lets clients
        // downgrade (the standard dance)
        putI16(o, apiVer == 0 ? E_NONE : E_UNSUPPORTED_VERSION);
        putI32(o, 33); // array count
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
        row(o, API_SASL_HANDSHAKE, 0, 1);
        row(o, API_SASL_AUTHENTICATE, 0, 1);
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
        row(o, API_ALTER_CONFIGS, 0, 1);
        row(o, API_DESCRIBE_ACLS, 0, 1);
        row(o, API_CREATE_ACLS, 0, 1);
        row(o, API_DELETE_ACLS, 0, 1);
        row(o, API_DELETE_RECORDS, 0, 1);
        row(o, API_LIST_GROUPS, 0, 0);
        row(o, API_DELETE_GROUPS, 0, 1);
        row(o, API_OFFSET_DELETE, 0, 0);
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

    case API_ALTER_CONFIGS:
        handleAlterConfigs(r, apiVer, o);
        break;

    case API_DESCRIBE_ACLS:
        handleDescribeAcls(r, apiVer, o);
        break;

    case API_CREATE_ACLS:
        handleCreateAcls(r, apiVer, o);
        break;

    case API_DELETE_ACLS:
        handleDeleteAcls(r, apiVer, o);
        break;

    case API_DELETE_RECORDS:
        handleDeleteRecords(r, apiVer, o);
        break;

    case API_LIST_GROUPS:
        handleListGroups(r, apiVer, o);
        break;

    case API_DELETE_GROUPS:
        handleDeleteGroups(r, apiVer, o);
        break;

    case API_OFFSET_DELETE:
        handleOffsetDelete(r, apiVer, o);
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

epilogue:
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
public __gshared ushort gKafkaTlsPort = 0; // the TLS listener (advertised to TLS conns)

// Which port THIS connection should see in Metadata/FindCoordinator broker
// entries: a client that connected over TLS must be pointed back at the TLS
// listener, or its next (re)connect handshakes plaintext and times out. Set
// per REQUEST (before handleRequest) — a fiber switch mid-response cannot
// clobber it because the advertise happens before any yield in the handlers,
// but keep it per-request anyway (cheap, and immune to handler reordering).
private ushort tKafkaAdvPort;

/// Require SASL authentication before any data/admin API (config
/// kafka-require-sasl). Off by default: SASL stays available but optional —
/// the legacy accept-any gate below mirrors every other skin.
public __gshared bool gKafkaRequireSasl = false;

/// Per-connection auth state, owned by the serve loop's frame and threaded
/// into handleRequest (SASL is the only conn-stateful surface in the skin).
public struct KafkaConnCtx
{
    const(void)* user; // AclUser* of the authenticated principal (null = anon)
    char[64] principalBuf = void;
    ubyte principalLen;
    bool saslHandshook; // mechanism accepted
    bool saslRawMode; // handshake v0: the NEXT frame is a bare SASL token
    bool authed;
    bool closeConn; // fatal auth state: serve loop drops the connection
    // SCRAM (RFC 5802) server state — verifier keys COPIED out of the AclUser
    // at client-first (the user could be deleted mid-handshake; no dangling)
    bool mechScram, scram512;
    ubyte scramStage; // 0 = await client-first, 1 = await client-final
    ubyte[64] scramStored = void, scramServer = void;
    char[128] nonceBuf = void; // combined client+server nonce
    ubyte nonceLen;
    char[256] cfbBuf = void; // client-first-bare (AuthMessage part 1)
    ushort cfbLen;
    char[192] sfBuf = void; // server-first (AuthMessage part 2)
    ushort sfLen;

    const(char)[] principal() const return scope @nogc nothrow
    {
        return principalLen ? principalBuf[0 .. principalLen] : "ANONYMOUS";
    }
}

/// SCRAM-SHA-256/512 server flow. Returns false = auth failed (drop). A true
/// return with !ctx.authed means "round accepted, more to come" (server-first
/// went out). Verifiers come from the ACL user (derived at `>password`).
private bool kafkaScramStep(scope const(ubyte)[] tok, KafkaConnCtx* ctx,
        ref ByteBuffer rtok) nothrow @trusted
{
    import dreads.acl : aclUser;
    import dreads.tls : b64enc, b64dec, scramHmac, scramSha, tlsRandBytes;

    immutable dl = ctx.scram512 ? 64 : 32;
    auto sr = cast(const(char)[]) tok;
    if (ctx.scramStage == 0)
    {
        // client-first: gs2-header "n,," (or "y,,") + bare "n=user,r=cnonce"
        if (sr.length < 4 || (sr[0] != 'n' && sr[0] != 'y') || sr[1] != ',')
            return false;
        size_t c2 = sr.length;
        foreach (k; 2 .. sr.length)
            if (sr[k] == ',')
            {
                c2 = k;
                break;
            }
        if (c2 >= sr.length)
            return false;
        auto bare = sr[c2 + 1 .. $];
        if (bare.length >= 2 && bare[0] == 'm')
            return false; // mandatory extensions unsupported, per RFC
        const(char)[] user, cnonce;
        size_t st;
        foreach (k; 0 .. bare.length + 1)
        {
            if (k == bare.length || bare[k] == ',')
            {
                auto attr = bare[st .. k];
                if (attr.length >= 2 && attr[0] == 'n' && attr[1] == '=')
                    user = attr[2 .. $];
                else if (attr.length >= 2 && attr[0] == 'r' && attr[1] == '=')
                    cnonce = attr[2 .. $];
                st = k + 1;
            }
        }
        if (user.length == 0 || cnonce.length == 0 || cnonce.length > 96
                || bare.length > ctx.cfbBuf.length)
            return false;
        auto au = aclUser(user);
        if (au is null || !au.enabled || !au.hasScram)
            return false; // SCRAM needs a real user with a stored verifier
        if (ctx.scram512)
        {
            ctx.scramStored[0 .. 64] = au.scram512Stored;
            ctx.scramServer[0 .. 64] = au.scram512Server;
        }
        else
        {
            ctx.scramStored[0 .. 32] = au.scram256Stored;
            ctx.scramServer[0 .. 32] = au.scram256Server;
        }
        if (user.length <= ctx.principalBuf.length)
        {
            ctx.principalBuf[0 .. user.length] = user;
            ctx.principalLen = cast(ubyte) user.length;
        }
        // combined nonce = cnonce + 24 chars of ours
        ubyte[18] rnd = void;
        if (!tlsRandBytes(rnd))
            return false;
        char[24] snb = void;
        auto sn = b64enc(rnd, snb);
        ctx.nonceBuf[0 .. cnonce.length] = cnonce;
        ctx.nonceBuf[cnonce.length .. cnonce.length + sn.length] = sn;
        ctx.nonceLen = cast(ubyte)(cnonce.length + sn.length);
        ctx.cfbBuf[0 .. bare.length] = bare;
        ctx.cfbLen = cast(ushort) bare.length;
        // server-first: r=<nonce>,s=<b64 salt>,i=<iter>
        char[32] sb = void;
        auto saltB = b64enc(au.scramSalt, sb);
        char[192] sf = void;
        size_t o;
        sf[o .. o + 2] = "r=";
        o += 2;
        sf[o .. o + ctx.nonceLen] = ctx.nonceBuf[0 .. ctx.nonceLen];
        o += ctx.nonceLen;
        sf[o .. o + 3] = ",s=";
        o += 3;
        sf[o .. o + saltB.length] = saltB;
        o += saltB.length;
        sf[o .. o + 3] = ",i=";
        o += 3;
        uint iv = au.scramIter;
        char[10] itmp = void;
        size_t il;
        do
        {
            itmp[il++] = cast(char)('0' + iv % 10);
            iv /= 10;
        }
        while (iv);
        foreach_reverse (k; 0 .. il)
            sf[o++] = itmp[k];
        ctx.sfBuf[0 .. o] = sf[0 .. o];
        ctx.sfLen = cast(ushort) o;
        rtok.append(sf[0 .. o]);
        ctx.scramStage = 1;
        return true;
    }
    // stage 1 — client-final: c=biws,r=<nonce>,p=<b64 proof>
    size_t pAt = sr.length;
    foreach_reverse (k; 0 .. sr.length >= 3 ? sr.length - 2 : 0)
        if (sr[k] == ',' && sr[k + 1] == 'p' && sr[k + 2] == '=')
        {
            pAt = k;
            break;
        }
    if (pAt >= sr.length)
        return false;
    auto cfwp = sr[0 .. pAt];
    auto proofB = sr[pAt + 3 .. $];
    // channel binding must be "biws" (base64 "n,,") and the nonce must match
    if (cfwp.length < 7 || cfwp[0 .. 7] != "c=biws,")
        return false;
    auto rAttr = cfwp[7 .. $];
    if (rAttr.length < 2 + ctx.nonceLen || rAttr[0 .. 2] != "r="
            || rAttr[2 .. 2 + ctx.nonceLen] != ctx.nonceBuf[0 .. ctx.nonceLen])
        return false;
    ubyte[64] proof = void;
    auto pf = b64dec(proofB, proof);
    if (pf is null || pf.length != dl)
        return false;
    // AuthMessage = client-first-bare , server-first , client-final-no-proof
    ubyte[704] am = void;
    size_t ao;
    am[ao .. ao + ctx.cfbLen] = cast(const(ubyte)[]) ctx.cfbBuf[0 .. ctx.cfbLen];
    ao += ctx.cfbLen;
    am[ao++] = ',';
    am[ao .. ao + ctx.sfLen] = cast(const(ubyte)[]) ctx.sfBuf[0 .. ctx.sfLen];
    ao += ctx.sfLen;
    am[ao++] = ',';
    if (ao + cfwp.length > am.length)
        return false;
    am[ao .. ao + cfwp.length] = cast(const(ubyte)[]) cfwp;
    ao += cfwp.length;
    ubyte[64] csig = void, ckey = void, hck = void;
    scramHmac(ctx.scram512, ctx.scramStored[0 .. dl], am[0 .. ao], csig[0 .. dl]);
    foreach (k; 0 .. dl)
        ckey[k] = pf[k] ^ csig[k];
    scramSha(ctx.scram512, ckey[0 .. dl], hck[0 .. dl]);
    if (hck[0 .. dl] != ctx.scramStored[0 .. dl])
        return false; // wrong password
    ubyte[64] ssig = void;
    scramHmac(ctx.scram512, ctx.scramServer[0 .. dl], am[0 .. ao], ssig[0 .. dl]);
    char[96] vb = void;
    auto v = b64enc(ssig[0 .. dl], vb);
    rtok.append("v=");
    rtok.append(v);
    ctx.authed = true;
    return true;
}

/// One SASL token exchange (any mechanism). True + !authed = more rounds.
private bool kafkaSaslStep(scope const(ubyte)[] tok, KafkaConnCtx* ctx,
        ref ByteBuffer rtok) nothrow @trusted
{
    if (ctx.mechScram)
        return kafkaScramStep(tok, ctx, rtok);
    return kafkaPlainCheck(tok, ctx); // PLAIN: empty reply token
}

/// PLAIN token: [authzid] NUL authcid NUL passwd — validated against the SAME
/// ACL users as RESP/AMQP/MQTT (one-ring auth; a10SaslCheck's exact gate:
/// only the seeded default user => legacy accept-any).
private bool kafkaPlainCheck(scope const(ubyte)[] tok, KafkaConnCtx* ctx) nothrow @trusted
{
    import dreads.acl : aclUser, aclCheckPassword, aclUserCount;

    auto sr = cast(const(char)[]) tok;
    size_t z1 = sr.length, z2 = sr.length;
    foreach (k, ch; sr)
        if (ch == '\0')
        {
            if (z1 == sr.length)
                z1 = k;
            else
            {
                z2 = k;
                break;
            }
        }
    const(char)[] auser, apass;
    if (z1 < sr.length && z2 < sr.length)
    {
        auser = sr[z1 + 1 .. z2];
        apass = sr[z2 + 1 .. $];
    }
    if (aclUserCount() <= 1)
    {
        // legacy accept-any (no ACL users): the connection is UNAUTHENTICATED.
        // Do NOT record the client-claimed principal — otherwise an attacker
        // claiming a super-user name would be treated as that principal once
        // Kafka ACLs/super-users are configured (gKafkaAclActive>0).
        ctx.principalLen = 0; // principal() => "ANONYMOUS"
        ctx.authed = true;
        return true;
    }
    auto au = aclUser(auser.length ? auser : "default");
    if (au is null || !au.enabled)
        return false;
    bool ok;
    try
        ok = aclCheckPassword(au, apass);
    catch (Exception)
        ok = false;
    if (!ok)
        return false;
    ctx.user = au;
    if (auser.length && auser.length <= ctx.principalBuf.length)
    {
        ctx.principalBuf[0 .. auser.length] = auser;
        ctx.principalLen = cast(ubyte) auser.length;
    }
    ctx.authed = true;
    return true;
}
/// When true (default), any named topic auto-exists with KAFKA_PARTITIONS (the
/// stateless model — clients need no CreateTopics). When false, a topic exists
/// only once created (CreateTopics) or produced-into, and Metadata returns
/// UNKNOWN_TOPIC for the rest — which lets an inspector see a missing topic.
public __gshared bool gKafkaAutoCreate = true;
/// Per-request Metadata gate: the request's allow_auto_topic_creation flag
/// (v4+; librdkafka consumers send FALSE — KIP-361/0109). ANDed with
/// gKafkaAutoCreate wherever a missing topic would auto-exist.
private bool tMetaAllowAuto = true;
/// The current request's header client_id (slice of the request buffer —
/// valid for the request's lifetime; joins record it per member).
private const(char)[] tKafkaClientId;

/// Emit a JoinGroup ERROR response (classic or flex). MEMBER_ID_REQUIRED
/// carries the broker-assigned id the client must re-join with.
private void emitJoinErr(ref ByteBuffer o, short ver, bool flex, short err,
        scope const(char)[] mid) nothrow @trusted
{
    if (flex)
    {
        putI32(o, 0); // throttle_time_ms
        putI16(o, err);
        putI32(o, -1); // generation
        if (ver >= 7)
            putCStrNull(o, null, true); // protocol_type (v7+)
        if (ver >= 7)
            putCStrNull(o, null, true); // protocol_name nullable at v7
        else
            putCStr(o, ""); // v6: non-nullable compact string
        putCStr(o, ""); // leader
        putCStr(o, mid);
        putCArrLen(o, 0); // members
        putTaggedFields(o);
        return;
    }
    if (ver >= 2)
        putI32(o, 0); // throttle_time_ms
    putI16(o, err);
    putI32(o, -1); // generation
    putStr(o, ""); // protocol_name
    putStr(o, ""); // leader
    putStr(o, mid);
    putI32(o, 0); // members
}

/// Group-name registry backing ListGroups: hash `kafka.groups`, field =
/// group, value = protocol_type. Written on every join (cheap, joins are
/// rebalance-rare), removed by DeleteGroups.
private void registerGroupName(scope const(char)[] group, scope const(char)[] ptype) nothrow @trusted
{
    if (gKafkaExec is null || group.length == 0)
        return;
    static ByteBuffer rb;
    const(char)[][4] a = ["hset", "kafka.groups", group,
        ptype.length ? ptype : "consumer"];
    gKafkaExec(a[], rb);
}

/// ListGroups (v0): every registered group. No filters at v0 — clients
/// filter client-side.
private void handleListGroups(ref Rd r, short ver, ref ByteBuffer o) nothrow @trusted
{
    if (!authorize(tKafkaCtx, KRES_CLUSTER, "kafka-cluster", KOP_DESCRIBE))
    {
        if (ver >= 1)
            putI32(o, 0); // throttle_time_ms (v1+)
        putI16(o, E_CLUSTER_AUTH_FAILED); // error_code
        putI32(o, 0); // zero groups
        return;
    }
    static ByteBuffer rb;
    if (ver >= 1)
        putI32(o, 0); // throttle_time_ms (v1+)
    putI16(o, E_NONE); // error_code
    immutable gOff = o.length;
    putI32(o, 0);
    int ng = 0;
    if (gKafkaExec !is null)
    {
        const(char)[][2] a = ["hgetall", "kafka.groups"];
        gKafkaExec(a[], rb);
        auto d = cast(const(char)[]) rb.data;
        if (d.length >= 4 && d[0] == '*')
        {
            size_t i2 = 1;
            long cnt = 0;
            while (i2 < d.length && d[i2] != '\r')
                cnt = cnt * 10 + (d[i2++] - '0');
            i2 += 2;
            for (long e = 0; e + 1 < cnt && ng < 512; e += 2)
            {
                if (i2 >= d.length || d[i2] != '$')
                    break;
                i2++;
                long fl = 0;
                while (i2 < d.length && d[i2] != '\r')
                    fl = fl * 10 + (d[i2++] - '0');
                i2 += 2;
                if (i2 + fl + 2 > d.length)
                    break;
                auto g = d[i2 .. i2 + cast(size_t) fl];
                i2 += cast(size_t) fl + 2;
                if (i2 >= d.length || d[i2] != '$')
                    break;
                i2++;
                long vl = 0;
                while (i2 < d.length && d[i2] != '\r')
                    vl = vl * 10 + (d[i2++] - '0');
                i2 += 2;
                if (i2 + vl + 2 > d.length)
                    break;
                auto pt = d[i2 .. i2 + cast(size_t) vl];
                i2 += cast(size_t) vl + 2;
                putStr(o, g); // group_id
                putStr(o, pt); // protocol_type
                ng++;
            }
        }
    }
    patchI32(o, gOff, ng);
}

/// DeleteGroups (v0-v1): drop empty groups' coordinator state + registry
/// entry. Unknown = 69 (GROUP_ID_NOT_FOUND), live members = 68
/// (NON_EMPTY_GROUP).
private void handleDeleteGroups(ref Rd r, short ver, ref ByteBuffer o) nothrow @trusted
{
    immutable ngr = safeCount(r.i32());
    const(char)[][64] names;
    size_t ng;
    foreach (_; 0 .. ngr)
    {
        if (!r.ok)
            break;
        auto g = r.str();
        if (ng < names.length && r.ok)
            names[ng++] = g;
    }
    short[64] errs = 69;
    foreach (i; 0 .. ng)
    {
        if (names[i].length == 0)
            continue;
        if (names[i].length > 249) // Kafka group-id max: reject vs. truncating the key
        {
            errs[i] = 24; // INVALID_GROUP_ID
            continue;
        }
        if (!authorize(tKafkaCtx, KRES_GROUP, names[i], KOP_DELETE))
        {
            errs[i] = E_GROUP_AUTH_FAILED; // per-group ACL denial (echoed below)
            continue;
        }
        static ByteBuffer req, rep; // TLS: consumed synchronously per hop
        req.clear();
        req.appendByte(cast(char) KGOP_DROP);
        putStr(req, names[i]);
        if (kgOp(names[i], cast(const(ubyte)[]) req.data, rep))
        {
            Rd rr = Rd(cast(const(ubyte)[]) rep.data);
            errs[i] = rr.i16();
        }
        // dropped (or never had FSM state): a registered name with committed
        // offsets still deletes — clear the registry entry unless non-empty
        if (errs[i] == 69 && gKafkaExec !is null)
        {
            // known in the registry (joined at some point) but FSM-empty on
            // its owner: treat as deletable
            static ByteBuffer rb2;
            const(char)[][3] ha = ["hexists", "kafka.groups", names[i]];
            gKafkaExec(ha[], rb2);
            auto d = rb2.data;
            if (d.length >= 4 && d[0] == ':' && d[1] == '1')
                errs[i] = E_NONE;
        }
        if (errs[i] == E_NONE && gKafkaExec !is null)
        {
            static ByteBuffer rb3;
            const(char)[][3] a = ["hdel", "kafka.groups", names[i]];
            gKafkaExec(a[], rb3);
            // a deleted group loses its committed offsets too
            static ByteBuffer kb6, rb4;
            groupOffKey(names[i], kb6);
            char[9 + 256] gst = void;
            immutable gl = kb6.length <= gst.length ? kb6.length : gst.length;
            gst[0 .. gl] = cast(const(char)[]) kb6.data[0 .. gl];
            const(char)[][2] a2 = ["del", cast(const(char)[]) gst[0 .. gl]];
            gKafkaExec(a2[], rb4);
        }
    }
    putI32(o, 0); // throttle_time_ms
    putI32(o, cast(int) ng); // results
    foreach (i; 0 .. ng)
    {
        putStr(o, names[i]);
        putI16(o, errs[i]);
    }
}

/// OffsetDelete (v0, KIP-496): remove committed offsets for the named
/// partitions. Partitions of a topic the group's LIVE members still subscribe
/// to answer GROUP_SUBSCRIBED_TO_TOPIC(86). Response order is peculiar:
/// top-level error FIRST, then throttle, then topics.
private void handleOffsetDelete(ref Rd r, short ver, ref ByteBuffer o) nothrow @trusted
{
    import core.stdc.stdio : snprintf;

    auto group = r.str();
    if (!authorize(tKafkaCtx, KRES_GROUP, group, KOP_DELETE))
    {
        putI16(o, E_GROUP_AUTH_FAILED); // top-level error_code
        putI32(o, 0); // throttle_time_ms
        putI32(o, 0); // zero topics
        return;
    }
    if (group.length > 249) // Kafka group-id max: reject vs. truncating the key
    {
        putI16(o, 24); // INVALID_GROUP_ID (top-level error_code)
        putI32(o, 0); // throttle_time_ms
        putI32(o, 0); // zero topics
        return;
    }
    immutable ntopics = safeCount(r.i32());
    putI16(o, E_NONE); // top-level error_code
    putI32(o, 0); // throttle_time_ms
    immutable tOff = o.length;
    putI32(o, ntopics);
    int et = 0;
    foreach (_; 0 .. ntopics)
    {
        if (!r.ok)
            break;
        auto topic = r.str();
        immutable nparts = safeCount(r.i32());
        if (!r.ok)
            break;
        putStr(o, topic);
        immutable pOff = o.length;
        putI32(o, nparts);
        et++;
        // live subscription check, once per topic (routed FSM op)
        bool subscribed = false;
        {
            static ByteBuffer req, rep; // TLS: consumed synchronously per hop
            req.clear();
            req.appendByte(cast(char) KGOP_SUBSCRIBED);
            putStr(req, group);
            putStr(req, topic);
            if (kgOp(group, cast(const(ubyte)[]) req.data, rep))
            {
                Rd rr = Rd(cast(const(ubyte)[]) rep.data);
                cast(void) rr.i16();
                subscribed = rr.i8() != 0;
            }
        }
        int ep = 0;
        foreach (_2; 0 .. nparts)
        {
            if (!r.ok)
                break;
            immutable part = r.i32();
            short perr = E_NONE;
            if (subscribed)
                perr = 86; // GROUP_SUBSCRIBED_TO_TOPIC
            else if (gKafkaExec !is null && validTopic(topic) && part >= 0)
            {
                static ByteBuffer keyb2, rb;
                groupOffKey(group, keyb2);
                char[9 + 256] gst = void;
                immutable gl = keyb2.length <= gst.length ? keyb2.length : gst.length;
                gst[0 .. gl] = cast(const(char)[]) keyb2.data[0 .. gl];
                auto gkey = cast(const(char)[]) gst[0 .. gl];
                char[KAFKA_MAX_TOPIC + 16] fb = void;
                auto field = partField(topic, part, fb);
                const(char)[][3] a = ["hdel", gkey, field];
                gKafkaExec(a[], rb);
                // the metadata sibling too
                char[KAFKA_MAX_TOPIC + 18] mfb = void;
                immutable fl = field.length + 2 <= mfb.length ? field.length : mfb.length - 2;
                mfb[0 .. fl] = field[0 .. fl];
                mfb[fl] = '#';
                mfb[fl + 1] = 'm';
                const(char)[][3] a2 = ["hdel", gkey,
                    cast(const(char)[]) mfb[0 .. fl + 2]];
                gKafkaExec(a2[], rb);
            }
            putI32(o, part);
            putI16(o, perr);
            ep++;
        }
        patchI32(o, pOff, ep);
    }
    patchI32(o, tOff, et);
}

/// Drive the join barrier for a parsed JoinGroup request: send KGOP_JOIN once,
/// then poll KGOP_JOIN_POLL every 15ms until the coordinator closes the
/// barrier (or the cap passes), then emit the wire response. Holding the
/// connection is spec-faithful: brokers process per-connection in order and a
/// JoinGroup can legitimately take up to the rebalance timeout.
private void joinLoop(ref ByteBuffer o, short ver, bool flex,
        scope const(char)[] group, ref ByteBuffer req) nothrow @trusted
{
    static ByteBuffer rep; // TLS: parsed synchronously after each hop
    char[128] midBuf = void;
    size_t midLen;
    immutable deadline = kMonoMs() + 70_000; // rebalance cap + slack
    for (;;)
    {
        if (!kgOp(group, cast(const(ubyte)[]) req.data, rep))
        {
            emitJoinErr(o, ver, flex, KG_REBALANCE_IN_PROGRESS,
                    cast(const(char)[]) midBuf[0 .. midLen]);
            return;
        }
        Rd rr = Rd(cast(const(ubyte)[]) rep.data);
        immutable e = rr.i16();
        if (e == KG_WAIT)
        {
            auto m = rr.str();
            midLen = m.length <= midBuf.length ? m.length : midBuf.length;
            midBuf[0 .. midLen] = m[0 .. midLen];
            if (kMonoMs() >= deadline)
            {
                emitJoinErr(o, ver, flex, KG_REBALANCE_IN_PROGRESS,
                        cast(const(char)[]) midBuf[0 .. midLen]);
                return;
            }
            kSleepMs(15);
            req.clear(); // TLS req may have been reused during the sleep: rebuild
            req.appendByte(cast(char) KGOP_JOIN_POLL);
            putStr(req, group);
            putStr(req, cast(const(char)[]) midBuf[0 .. midLen]);
            continue;
        }
        if (e == KG_MEMBER_ID_REQUIRED)
        {
            emitJoinErr(o, ver, flex, KG_MEMBER_ID_REQUIRED, rr.str());
            return;
        }
        if (e != KG_NONE)
        {
            emitJoinErr(o, ver, flex, e, cast(const(char)[]) midBuf[0 .. midLen]);
            return;
        }
        // ok: [str mid][i32 gen][u8 isLeader][str proto][str ptype][str leader]
        //     [i32 n]{[str mid][u8 giiNull][str gii][bytes meta]}*
        auto mid = rr.str();
        immutable gen = rr.i32();
        cast(void) rr.i8(); // isLeader (implied by leader field)
        auto proto = rr.str();
        auto ptype = rr.str();
        auto leader = rr.str();
        immutable nmembRaw = rr.i32();
        immutable nmemb = nmembRaw < 0 ? 0 : nmembRaw;
        if (flex)
        {
            putI32(o, 0); // throttle_time_ms
            putI16(o, E_NONE);
            putI32(o, gen);
            if (ver >= 7)
                putCStrNull(o, ptype, ptype is null); // protocol_type (v7+)
            putCStr(o, proto);
            putCStr(o, leader);
            putCStr(o, mid);
            putCArrLen(o, nmemb);
            foreach (_; 0 .. nmemb)
            {
                auto m2 = rr.str();
                immutable gN = rr.i8() != 0;
                auto g2 = rr.str();
                auto meta = rr.bytesI32();
                putCStr(o, m2);
                putCStrNull(o, g2, gN); // group_instance_id (v5+)
                putCBytes(o, meta, false);
                putTaggedFields(o); // member tagged fields
            }
            putTaggedFields(o); // response tagged fields
            return;
        }
        if (ver >= 2)
            putI32(o, 0); // throttle_time_ms
        putI16(o, E_NONE);
        putI32(o, gen);
        putStr(o, proto);
        putStr(o, leader);
        putStr(o, mid);
        putI32(o, nmemb);
        foreach (_; 0 .. nmemb)
        {
            auto m2 = rr.str();
            immutable gN = rr.i8() != 0;
            auto g2 = rr.str();
            auto meta = rr.bytesI32();
            putStr(o, m2);
            if (ver >= 5)
            {
                if (gN)
                    putI16(o, -1); // group_instance_id = null
                else
                    putStr(o, g2);
            }
            putBytesI32(o, meta, false);
        }
        return;
    }
}

/// Stage the fixed head of a KGOP_JOIN request from parsed fields; the caller
/// appends the [i32 nproto]{name,meta}* tail itself.
private int buildJoinReq(ref ByteBuffer req, scope const(char)[] group,
        scope const(char)[] memberId, scope const(char)[] gii, bool giiNull,
        int sessMs, int rebMs, bool v4plus, scope const(char)[] protocolType) nothrow @trusted
{
    req.clear();
    req.appendByte(cast(char) KGOP_JOIN);
    putStr(req, group);
    putStr(req, memberId);
    req.appendByte(giiNull ? 1 : 0);
    putStr(req, giiNull ? "" : gii);
    putI32(req, sessMs);
    putI32(req, rebMs);
    req.appendByte(v4plus ? 1 : 0);
    putStr(req, protocolType);
    putStr(req, tKafkaClientId is null ? "" : tKafkaClientId);
    return cast(int) req.length; // caller appends [i32 nproto]{...} itself
}

/// JoinGroup v6/v7 (flexible): real coordinator via dreads.kafkagroup.
private void handleJoinGroupFlex(ref Rd r, short ver, ref ByteBuffer o) nothrow @trusted
{
auto group = r.cstr();
    immutable sessMs = r.i32();
    immutable rebMs = r.i32();
    auto memberId = r.cstr();
    auto gii = r.cstr(); // group_instance_id (nullable, v5+)
    auto protocolType = r.cstr();
    if (!authorize(tKafkaCtx, KRES_GROUP, group, KOP_READ))
    {
        emitJoinErr(o, ver, true, E_GROUP_AUTH_FAILED, memberId);
        return;
    }
    immutable nprotoRaw = r.carrlen();
    immutable nproto = nprotoRaw < 0 ? 0 : safeCount(nprotoRaw);
    static ByteBuffer req; // TLS: consumed synchronously by the hop copy
    cast(void) buildJoinReq(req, group, memberId, gii, gii is null, sessMs,
            rebMs, true, protocolType is null ? "consumer" : protocolType);
    immutable npOff = req.length;
    putI32(req, 0);
    int np = 0;
    foreach (_; 0 .. nproto)
    {
        if (!r.ok)
            break;
        auto name = r.cstr();
        auto meta = r.cbytes();
        r.skipTaggedFields(); // per-protocol tagged fields
        if (!r.ok)
            break;
        if (np < 64)
        {
            putStr(req, name);
            putI32(req, cast(int)(meta is null ? 0 : meta.length));
            if (meta !is null)
                req.append(cast(const(char)[]) meta);
            np++;
        }
    }
    patchI32(req, npOff, np);
    if (!r.ok || np == 0 || group is null || group.length == 0)
    {
        emitJoinErr(o, ver, true, KG_INCONSISTENT_PROTOCOL, memberId);
        return;
    }
    registerGroupName(group, protocolType is null ? "consumer" : protocolType);
    joinLoop(o, ver, true, group, req);
}

/// JoinGroup v0-v5 (classic): real coordinator via dreads.kafkagroup. v5 adds
/// group_instance_id to BOTH the request and the response members (the old
/// stub missed it — librdkafka v5 joins misparsed into a retry loop).
private void handleJoinGroup(ref Rd r, short ver, ref ByteBuffer o) nothrow @trusted
{
    if (isFlexible(API_JOIN_GROUP, ver)) // v6/v7 flexible
    {
        handleJoinGroupFlex(r, ver, o);
        return;
    }
    auto group = r.str();
    immutable sessMs = r.i32();
    immutable rebMs = ver >= 1 ? r.i32() : 60_000;
    auto memberId = r.str();
    const(char)[] gii = null;
    if (ver >= 5)
        gii = r.str(); // group_instance_id (nullable) — v5 classic
    auto protocolType = r.str();
    immutable nproto = safeCount(r.i32());
    if (!authorize(tKafkaCtx, KRES_GROUP, group, KOP_READ))
    {
        emitJoinErr(o, ver, false, E_GROUP_AUTH_FAILED, memberId);
        return;
    }
    static ByteBuffer req; // TLS: consumed synchronously by the hop copy
    cast(void) buildJoinReq(req, group, memberId, gii, gii is null, sessMs,
            rebMs, ver >= 4, protocolType);
    immutable npOff = req.length;
    putI32(req, 0);
    int np = 0;
    foreach (_; 0 .. nproto)
    {
        if (!r.ok)
            break;
        auto name = r.str();
        auto meta = r.bytesI32();
        if (!r.ok)
            break;
        if (np < 64)
        {
            putStr(req, name);
            putI32(req, cast(int)(meta is null ? 0 : meta.length));
            if (meta !is null)
                req.append(cast(const(char)[]) meta);
            np++;
        }
    }
    patchI32(req, npOff, np);
    if (!r.ok || np == 0 || group.length == 0)
    {
        emitJoinErr(o, ver, false, KG_INCONSISTENT_PROTOCOL, memberId);
        return;
    }
    registerGroupName(group, protocolType);
    joinLoop(o, ver, false, group, req);
}

private void kSleepMs(long ms) nothrow @trusted
{
    import vibe.core.core : sleep;
    import core.time : msecs;

    try
        sleep(msecs(ms));
    catch (Exception)
    {
    }
}

private long kMonoMs() @nogc nothrow @trusted
{
    import core.time : MonoTime;

    auto t = MonoTime.currTime;
    return t.ticks / (MonoTime.ticksPerSecond / 1000);
}

/// One group-coordinator op on the owner shard (routing key = the group's
/// offsets hash, so membership and offsets are co-owned). False = transport
/// failure — callers treat it as retryable (REBALANCE_IN_PROGRESS).
private bool kgOp(scope const(char)[] group, scope const(ubyte)[] req,
        ref ByteBuffer rep) nothrow @trusted
{
    if (gKafkaGroupHop is null)
        return false;
    static ByteBuffer keyb; // TLS: consumed before the hop's first yield
    groupOffKey(group, keyb);
    gKafkaGroupHop(cast(const(char)[]) keyb.data, req, rep);
    return rep.length >= 2;
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

/// Committed leader epoch lives in sibling field `<topic>/<partition>#e`
/// (stored only when the commit names one; OffsetFetch answers -1 otherwise).
private void storeGroupEpoch(scope const(char)[] group, scope const(char)[] topic,
        int part, int epoch) nothrow @trusted
{
    import core.stdc.stdio : snprintf;

    if (gKafkaExec is null)
        return;
    static ByteBuffer keyb, rb;
    groupOffKey(group, keyb);
    char[KAFKA_MAX_TOPIC + 16] fb = void;
    auto field = partField(topic, part, fb);
    char[KAFKA_MAX_TOPIC + 18] efb = void;
    immutable fl = field.length + 2 <= efb.length ? field.length : efb.length - 2;
    efb[0 .. fl] = field[0 .. fl];
    efb[fl] = '#';
    efb[fl + 1] = 'e';
    auto efield = cast(const(char)[]) efb[0 .. fl + 2];
    if (epoch < 0)
    {
        const(char)[][3] a = ["hdel", cast(const(char)[]) keyb.data, efield];
        gKafkaExec(a[], rb);
        return;
    }
    char[16] eb = void;
    immutable k = snprintf(eb.ptr, eb.length, "%d", epoch);
    const(char)[][4] a = ["hset", cast(const(char)[]) keyb.data, efield,
        cast(const(char)[]) eb[0 .. (k > 0 ? k : 0)]];
    gKafkaExec(a[], rb);
}

/// The committed leader epoch for one partition, or -1 when unset.
private int fetchGroupEpoch(scope const(char)[] group, scope const(char)[] topic,
        int part) nothrow @trusted
{
    if (gKafkaExec is null)
        return -1;
    static ByteBuffer keyb, rb;
    groupOffKey(group, keyb);
    char[KAFKA_MAX_TOPIC + 16] fb = void;
    auto field = partField(topic, part, fb);
    char[KAFKA_MAX_TOPIC + 18] efb = void;
    immutable fl = field.length + 2 <= efb.length ? field.length : efb.length - 2;
    efb[0 .. fl] = field[0 .. fl];
    efb[fl] = '#';
    efb[fl + 1] = 'e';
    const(char)[][3] a = ["hget", cast(const(char)[]) keyb.data,
        cast(const(char)[]) efb[0 .. fl + 2]];
    gKafkaExec(a[], rb);
    auto d = rb.data;
    if (d.length < 4 || d[0] != '$' || d[1] == '-')
        return -1;
    size_t i2 = 1;
    long blen = 0;
    while (i2 < d.length && d[i2] >= '0' && d[i2] <= '9')
        blen = blen * 10 + (d[i2++] - '0');
    if (i2 + 2 > d.length)
        return -1;
    i2 += 2;
    int v = 0;
    long got = 0;
    while (i2 < d.length && d[i2] >= '0' && d[i2] <= '9' && got < blen)
    {
        v = v * 10 + (d[i2++] - '0');
        got++;
    }
    return got ? v : -1;
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
    if (!authorize(tKafkaCtx, KRES_GROUP, group, KOP_READ))
    {
        if (ver >= 3)
            putI32(o, 0); // throttle_time_ms
        putI32(o, 0); // zero topics = well-formed GROUP authorization denial
        return;
    }
    int genId = -1;
    const(char)[] cMember = null;
    if (ver >= 1)
    {
        genId = r.i32(); // generation_id
        cMember = r.str(); // member_id
    }
    if (ver >= 7)
        cast(void) r.str(); // group_instance_id (nullable)
    if (ver >= 2 && ver <= 4)
        cast(void) r.i64(); // retention_time_ms (v2-v4 only)
    // Generation fencing (real consumer groups): ONLY when the commit names a
    // generation (>= 0). Simple/assign() consumers commit with generation -1
    // and keep the historic unfenced path untouched.
    short fenceErr = E_NONE;
    if (ver >= 1 && genId >= 0 && r.ok && group.length)
    {
        static ByteBuffer fq, fr; // TLS: consumed synchronously per hop
        fq.clear();
        fq.appendByte(cast(char) KGOP_COMMIT_CHECK);
        putStr(fq, group);
        putStr(fq, cMember is null ? "" : cMember);
        putI32(fq, genId);
        if (kgOp(group, cast(const(ubyte)[]) fq.data, fr))
        {
            Rd fr2 = Rd(cast(const(ubyte)[]) fr.data);
            fenceErr = fr2.i16();
            if (fenceErr == KG_WAIT)
                fenceErr = KG_REBALANCE_IN_PROGRESS;
        }
    }
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
        // a commit against a topic nobody created/produced answers 3 per
        // partition (0030); the registry is the existence source of truth
        immutable texists = validTopic(topic) && registeredTopicPartitions(topic) >= 0;
        immutable short topicErr = fenceErr != E_NONE ? fenceErr
            : (texists ? E_NONE : E_UNKNOWN_TOPIC);
        int ep = 0;
        foreach (_2; 0 .. nparts)
        {
            if (!r.ok || o.length - respStart > KAFKA_MAX_RESP)
                break;
            immutable part = r.i32();
            immutable off = r.i64();
            if (ver == 1)
                cast(void) r.i64(); // commit_timestamp (v1 only)
            int cEpoch = -1;
            if (ver >= 6)
                cEpoch = r.i32(); // committed_leader_epoch
            auto meta = r.str(); // committed_metadata (nullable)
            if (topicErr == E_NONE && r.ok && part >= 0 && off >= 0)
            {
                storeGroupOffset(group, topic, part, off);
                storeGroupMeta(group, topic, part, meta);
                storeGroupEpoch(group, topic, part, cEpoch);
            }
            putI32(o, part);
            putI16(o, topicErr); // fence/unknown-topic: same error per partition
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

/// Global "a transaction has ever run" gate: 0 keeps every fetch/produce hot
/// path at zero extra cost; set on the first transactional produce/EndTxn.
public shared ubyte gKafkaTxnSeen;

/// Resolve (pid, epoch) for a TRANSACTIONAL id via the coordinator (stable
/// pid; epoch++ per init = zombie fencing; timeout validated). Returns the
/// error code.
private short txnInit(scope const(char)[] tid, int txnTimeoutMs, out long pid,
        out short epoch) nothrow @trusted
{
    pid = -1;
    epoch = -1;
    static ByteBuffer req, rep; // TLS: consumed synchronously per hop
    req.clear();
    req.appendByte(cast(char) KGOP_TXN_INIT);
    putStr(req, tid);
    putI32(req, txnTimeoutMs);
    if (!kgOp(tid, cast(const(ubyte)[]) req.data, rep))
        return E_UNSUPPORTED_VERSION; // transport failure: retryable-ish
    Rd rr = Rd(cast(const(ubyte)[]) rep.data);
    immutable e = rr.i16();
    if (e != E_NONE)
        return e;
    pid = rr.i64();
    epoch = rr.i16();
    return E_NONE;
}

/// InitProducerID (v0-v3): transactional ids go through the coordinator
/// (stable pid + fencing epochs + timeout validation); bare idempotent
/// producers keep the historic fresh-pid counter.
private void handleInitProducerId(ref Rd r, short ver, ref ByteBuffer o) nothrow @trusted
{
    import core.atomic : atomicOp;

    const(char)[] tid;
    int txnTimeout;
    immutable flex = isFlexible(API_INIT_PRODUCER_ID, ver); // v2/v3 flexible
    if (flex)
    {
        tid = r.cstr(); // transactional_id (nullable compact)
        txnTimeout = r.i32();
        if (ver >= 3)
        {
            cast(void) r.i64(); // producer_id (v3+, for resume/fence)
            cast(void) r.i16(); // producer_epoch (v3+)
        }
    }
    else
    {
        tid = r.str(); // transactional_id (nullable)
        txnTimeout = r.i32();
    }
    if (tid !is null && tid.length
            && !authorize(tKafkaCtx, KRES_TXNID, tid, KOP_WRITE))
    {
        putI32(o, 0); // throttle_time_ms
        putI16(o, 53); // TRANSACTIONAL_ID_AUTHORIZATION_FAILED
        putI64(o, -1); // producer_id
        putI16(o, -1); // producer_epoch
        if (flex)
            putTaggedFields(o);
        return;
    }
    short err = E_NONE;
    long pid;
    short epoch = 0;
    if (tid !is null && tid.length)
        err = txnInit(tid, txnTimeout, pid, epoch);
    else
        pid = atomicOp!"+="(gNextProducerId, 1);
    if (flex)
    {
        putI32(o, 0); // throttle_time_ms
        putI16(o, err);
        putI64(o, err == E_NONE ? pid : -1);
        putI16(o, err == E_NONE ? epoch : -1);
        putTaggedFields(o);
        return;
    }
    putI32(o, 0); // throttle_time_ms (v0+)
    putI16(o, err);
    putI64(o, err == E_NONE ? pid : -1);
    putI16(o, err == E_NONE ? epoch : -1);
}

/// AddPartitionsToTxn (v0-v2): register the partitions with the coordinator
/// (fencing validated); the response echoes one error for all partitions.
private void handleAddPartitionsToTxn(ref Rd r, short ver, ref ByteBuffer o) nothrow @trusted
{
    auto tid = r.str();
    if (!authorize(tKafkaCtx, KRES_TXNID, tid, KOP_WRITE))
    {
        putI32(o, 0); // throttle_time_ms
        putI32(o, 0); // zero topics = well-formed authorization denial
        return;
    }
    immutable pid = r.i64();
    immutable epoch = r.i16();
    immutable ntopics = safeCount(r.i32());
    // stage the coordinator op while parsing (slices = stable request buffer)
    static ByteBuffer req, rep; // TLS: consumed synchronously by the hop
    req.clear();
    req.appendByte(cast(char) KGOP_TXN_ADD);
    putStr(req, tid);
    foreach_reverse (k; 0 .. 8)
        req.appendByte(cast(char)((pid >> (k * 8)) & 0xFF));
    putI32(req, epoch);
    immutable nOff = req.length;
    putI32(req, 0);
    int nAll = 0;
    // remember the topic/partition layout for the response echo
    const(char)[][32] tnames;
    size_t[32] tnparts;
    int[512] tparts;
    size_t nt, npAll;
    foreach (_; 0 .. ntopics)
    {
        if (!r.ok)
            break;
        auto topic = r.str();
        immutable nparts = safeCount(r.i32());
        if (!r.ok)
            break;
        if (nt < tnames.length)
        {
            tnames[nt] = topic;
            tnparts[nt] = 0;
        }
        foreach (_2; 0 .. nparts)
        {
            if (!r.ok)
                break;
            immutable part = r.i32();
            if (!r.ok)
                break;
            if (nt < tnames.length && npAll < tparts.length)
            {
                tparts[npAll++] = part;
                tnparts[nt]++;
            }
            putStr(req, topic);
            putI32(req, part);
            nAll++;
        }
        if (nt < tnames.length)
            nt++;
    }
    patchI32(req, nOff, nAll);
    short err = E_NONE;
    if (tid.length && kgOp(tid, cast(const(ubyte)[]) req.data, rep))
    {
        Rd rr = Rd(cast(const(ubyte)[]) rep.data);
        err = rr.i16();
    }
    putI32(o, 0); // throttle_time_ms
    putI32(o, cast(int) nt);
    size_t pi;
    foreach (i; 0 .. nt)
    {
        putStr(o, tnames[i]);
        putI32(o, cast(int) tnparts[i]);
        foreach (_; 0 .. tnparts[i])
        {
            putI32(o, tparts[pi++]);
            putI16(o, err);
        }
    }
}

/// AddOffsetsToTxn/// AddOffsetsToTxn (v0-v2): register the consumer group with the txn — ack only
/// (the offsets themselves arrive via TxnOffsetCommit).
private void handleAddOffsetsToTxn(ref Rd r, short ver, ref ByteBuffer o) nothrow @trusted
{
    auto tid = r.str(); // transactional_id
    cast(void) r.i64(); // producer_id
    cast(void) r.i16(); // producer_epoch
    cast(void) r.str(); // group
    if (!authorize(tKafkaCtx, KRES_TXNID, tid, KOP_WRITE))
    {
        putI32(o, 0); // throttle_time_ms
        putI16(o, 53); // TRANSACTIONAL_ID_AUTHORIZATION_FAILED
        return;
    }
    putI32(o, 0); // throttle_time_ms
    putI16(o, E_NONE); // error_code
}

/// EndTxn (v0-v2): close the transaction at the coordinator, then write a
/// CONTROL MARKER record into every partition the txn touched (markers occupy
/// offsets, like the real broker). Aborts also record the aborted range in
/// `kafka.txa.<t>.<p>` for Fetch's aborted_transactions.
private void handleEndTxn(ref Rd r, short ver, ref ByteBuffer o) nothrow @trusted
{
    import core.atomic : atomicStore;
    import core.stdc.stdio : snprintf;

    auto tid = r.str();
    immutable pid = r.i64();
    immutable epoch = r.i16();
    immutable committed = r.i8() != 0;
    if (!authorize(tKafkaCtx, KRES_TXNID, tid, KOP_WRITE))
    {
        putI32(o, 0); // throttle_time_ms
        putI16(o, 53); // TRANSACTIONAL_ID_AUTHORIZATION_FAILED
        return;
    }
    short err = E_NONE;
    static ByteBuffer req, rep; // TLS: rep parsed into stack copies below
    req.clear();
    req.appendByte(cast(char) KGOP_TXN_END);
    putStr(req, tid);
    foreach_reverse (k; 0 .. 8)
        req.appendByte(cast(char)((pid >> (k * 8)) & 0xFF));
    putI32(req, epoch);
    req.appendByte(committed ? 1 : 0);
    // partitions copied to STACK before the marker writes below hop
    char[256][64] tbuf = void;
    size_t[64] tlen;
    int[64] parts;
    size_t np;
    if (tid.length && kgOp(tid, cast(const(ubyte)[]) req.data, rep))
    {
        Rd rr = Rd(cast(const(ubyte)[]) rep.data);
        err = rr.i16();
        if (err == E_NONE)
        {
            immutable n2 = rr.i32();
            foreach (_; 0 .. (n2 < 0 ? 0 : n2))
            {
                auto topic = rr.str();
                immutable part = rr.i32();
                if (!rr.ok)
                    break;
                if (np < parts.length)
                {
                    immutable tl = topic.length <= 256 ? topic.length : 256;
                    tbuf[np][0 .. tl] = topic[0 .. tl];
                    tlen[np] = tl;
                    parts[np] = part;
                    np++;
                }
            }
            // buffered TxnOffsetCommit offsets: applied ONLY on commit.
            // Copies go to STACK buffers first — the stores below hop and
            // rep is a shared TLS buffer.
            auto ogrp = rr.str();
            immutable nOffs = rr.i32();
            char[256] gbuf = void;
            immutable ogl = ogrp.length <= 256 ? ogrp.length : 256;
            gbuf[0 .. ogl] = ogrp[0 .. ogl];
            char[256][64] otb = void;
            size_t[64] otl;
            int[64] oparts;
            long[64] ooffs;
            bool[64] ometaHas;
            char[128][64] ometab = void;
            size_t[64] ometal;
            size_t nol;
            foreach (_; 0 .. (nOffs < 0 ? 0 : nOffs))
            {
                auto t2 = rr.str();
                immutable p2 = rr.i32();
                immutable o2 = rr.i64();
                immutable hasM = rr.i8() != 0;
                auto m2 = rr.str();
                if (!rr.ok)
                    break;
                // ogrp.length > 249 would truncate into gbuf[256] and alias a
                // different group's key — skip applying rather than mis-store.
                if (committed && ogrp.length <= 249 && nol < oparts.length)
                {
                    immutable tl2 = t2.length <= 256 ? t2.length : 256;
                    otb[nol][0 .. tl2] = t2[0 .. tl2];
                    otl[nol] = tl2;
                    oparts[nol] = p2;
                    ooffs[nol] = o2;
                    ometaHas[nol] = hasM;
                    immutable ml2 = m2.length <= 128 ? m2.length : 128;
                    ometab[nol][0 .. ml2] = m2[0 .. ml2];
                    ometal[nol] = ml2;
                    nol++;
                }
            }
            foreach (i2; 0 .. nol)
            {
                auto g2 = cast(const(char)[]) gbuf[0 .. ogl];
                auto t3 = cast(const(char)[]) otb[i2][0 .. otl[i2]];
                storeGroupOffset(g2, t3, oparts[i2], ooffs[i2]);
                if (ometaHas[i2])
                    storeGroupMeta(g2, t3, oparts[i2],
                            cast(const(char)[]) ometab[i2][0 .. ometal[i2]]);
            }
        }
    }
    if (err == E_NONE && np > 0)
    {
        atomicStore(gKafkaTxnSeen, cast(ubyte) 1);
        foreach (i; 0 .. np)
        {
            auto topic = cast(const(char)[]) tbuf[i][0 .. tlen[i]];
            immutable part = parts[i];
            if (!validTopic(topic) || part < 0)
                continue;
            immutable base = partBase(topic, part);
            // the open txn's first offset (recorded at first transactional
            // produce); absent = the txn produced nothing here
            static ByteBuffer pk9, rb9;
            pidKey(topic, part, pk9);
            char[12 + KAFKA_MAX_TOPIC + 16] pst = void;
            immutable pl = pk9.length <= pst.length ? pk9.length : pst.length;
            pst[0 .. pl] = cast(const(char)[]) pk9.data[0 .. pl];
            auto pkey = cast(const(char)[]) pst[0 .. pl];
            char[32] fb = void;
            immutable fl = snprintf(fb.ptr, fb.length, "txn:%lld", pid);
            auto tf = cast(const(char)[]) fb[0 .. (fl > 0 ? fl : 0)];
            long firstOff = -1;
            {
                const(char)[][3] a = ["hget", pkey, tf];
                gKafkaExec(a[], rb9);
                auto d = rb9.data;
                if (d.length >= 4 && d[0] == '$' && d[1] != '-')
                {
                    size_t i2 = 1;
                    long blen = 0;
                    while (i2 < d.length && d[i2] >= '0' && d[i2] <= '9')
                        blen = blen * 10 + (d[i2++] - '0');
                    i2 += 2;
                    long v = 0;
                    long got = 0;
                    while (i2 < d.length && d[i2] >= '0' && d[i2] <= '9' && got < blen)
                    {
                        v = v * 10 + (d[i2++] - '0');
                        got++;
                    }
                    if (got)
                        firstOff = v;
                }
            }
            {
                const(char)[][3] a = ["hdel", pkey, tf];
                gKafkaExec(a[], rb9);
            }
            // control marker record: [0xFE][pid 8B][type i16] (1=commit 0=abort)
            char[11] ctl = void;
            ctl[0] = cast(char) 0xFE;
            foreach (k; 0 .. 8)
                ctl[1 + k] = cast(char)((pid >> ((7 - k) * 8)) & 0xFF);
            ctl[9] = 0;
            ctl[10] = committed ? 1 : 0;
            static ByteBuffer kb9;
            partKey(topic, part, kb9);
            char[8 + KAFKA_MAX_TOPIC + 16] kst = void;
            immutable kl = kb9.length <= kst.length ? kb9.length : kst.length;
            kst[0 .. kl] = cast(const(char)[]) kb9.data[0 .. kl];
            const(char)[][1] one = [cast(const(char)[]) ctl[0 .. 11]];
            immutable newLen = pushRecords(cast(const(char)[]) kst[0 .. kl], one[]);
            if (!committed && firstOff >= 0 && newLen >= 0)
            {
                // aborted range for Fetch: "pid:firstOffset"
                char[64] ab = void;
                immutable al = snprintf(ab.ptr, ab.length, "%lld:%lld", pid, firstOff);
                char[12 + KAFKA_MAX_TOPIC + 16] xst = void;
                immutable xn = snprintf(xst.ptr, xst.length, "kafka.txa.%.*s.%d",
                        cast(int) topic.length, topic.ptr, part);
                const(char)[][3] a = ["rpush",
                    cast(const(char)[]) xst[0 .. (xn > 0 ? xn : 0)],
                    cast(const(char)[]) ab[0 .. (al > 0 ? al : 0)]];
                gKafkaExec(a[], rb9);
            }
            cast(void) base;
        }
    }
    putI32(o, 0); // throttle_time_ms
    putI16(o, err); // error_code
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
    auto tid = r.cstr(); // transactional_id
    if (!authorize(tKafkaCtx, KRES_TXNID, tid, KOP_WRITE))
    {
        putI32(o, 0); // throttle_time_ms
        putCArrLen(o, 0); // zero topics (compact) = well-formed authz denial
        putTaggedFields(o);
        return;
    }
    auto group = r.cstr();
    immutable pid = r.i64();
    immutable epoch = r.i16();
    cast(void) r.i32(); // generation_id (v3+)
    cast(void) r.cstr(); // member_id (v3+)
    cast(void) r.cstr(); // group_instance_id (nullable, v3+)
    immutable rawn = r.carrlen();
    immutable ntopics = rawn < 0 ? 0 : safeCount(rawn);
    // buffer the offsets at the coordinator: they apply on COMMIT only
    static ByteBuffer req, rep; // TLS: consumed synchronously by the hop
    req.clear();
    req.appendByte(cast(char) KGOP_TXN_OFFSETS);
    putStr(req, tid);
    foreach_reverse (k; 0 .. 8)
        req.appendByte(cast(char)((pid >> (k * 8)) & 0xFF));
    putI32(req, epoch);
    putStr(req, group);
    immutable nOff2 = req.length;
    putI32(req, 0);
    int nAll = 0;
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
            auto meta = r.cstr(); // metadata (nullable compact)
            r.skipTaggedFields(); // partition tagged fields
            if (r.ok && validTopic(topic) && part >= 0 && off >= 0)
            {
                putStr(req, topic);
                putI32(req, part);
                foreach_reverse (k; 0 .. 8)
                    req.appendByte(cast(char)((off >> (k * 8)) & 0xFF));
                req.appendByte(meta !is null ? 1 : 0);
                putStr(req, meta is null ? "" : meta);
                nAll++;
            }
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
    patchI32(req, nOff2, nAll);
    if (tid !is null && tid.length && nAll > 0)
        cast(void) kgOp(tid, cast(const(ubyte)[]) req.data, rep);
}

private void handleTxnOffsetCommit(ref Rd r, short ver, ref ByteBuffer o) nothrow @trusted
{
    if (isFlexible(API_TXN_OFFSET_COMMIT, ver)) // v3 flexible
    {
        handleTxnOffsetCommitFlex(r, ver, o);
        return;
    }
    auto tid = r.str(); // transactional_id
    if (!authorize(tKafkaCtx, KRES_TXNID, tid, KOP_WRITE))
    {
        putI32(o, 0); // throttle_time_ms
        putI32(o, 0); // zero topics = well-formed authorization denial
        return;
    }
    auto group = r.str();
    immutable pid = r.i64();
    immutable epoch = r.i16();
    immutable ntopics = safeCount(r.i32());
    static ByteBuffer req, rep; // TLS: consumed synchronously by the hop
    req.clear();
    req.appendByte(cast(char) KGOP_TXN_OFFSETS);
    putStr(req, tid);
    foreach_reverse (k; 0 .. 8)
        req.appendByte(cast(char)((pid >> (k * 8)) & 0xFF));
    putI32(req, epoch);
    putStr(req, group);
    immutable nOff2 = req.length;
    putI32(req, 0);
    int nAll = 0;
    immutable respStart = o.length;
    putI32(o, 0); // throttle_time_ms
    immutable tOff = o.length;
    putI32(o, ntopics);
    int et = 0;
    foreach (_; 0 .. ntopics)
    {
        if (!r.ok || o.length - respStart > KAFKA_MAX_RESP)
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
            if (!r.ok || o.length - respStart > KAFKA_MAX_RESP)
                break;
            immutable part = r.i32();
            immutable off = r.i64();
            if (ver >= 2)
                cast(void) r.i32(); // committed_leader_epoch (v2+)
            auto meta = r.str(); // metadata (nullable)
            if (r.ok && validTopic(topic) && part >= 0 && off >= 0)
            {
                putStr(req, topic);
                putI32(req, part);
                foreach_reverse (k; 0 .. 8)
                    req.appendByte(cast(char)((off >> (k * 8)) & 0xFF));
                req.appendByte(meta !is null ? 1 : 0);
                putStr(req, meta is null ? "" : meta);
                nAll++;
            }
            putI32(o, part);
            putI16(o, E_NONE); // error_code
            ep++;
        }
        patchI32(o, pOff, ep);
    }
    patchI32(o, tOff, et);
    patchI32(req, nOff2, nAll);
    if (tid !is null && tid.length && nAll > 0)
        cast(void) kgOp(tid, cast(const(ubyte)[]) req.data, rep);
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
private int[4096] tGoEp; // committed leader epoch (-1 = none)
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
        immutable ri = i; // entries start: the epoch pass re-walks from here
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
            if (field.length >= 2 && field[$ - 2] == '#'
                    && (field[$ - 1] == 'm' || field[$ - 1] == 'e'))
                continue; // metadata/epoch sibling fields, matched below
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
            tGoEp[nf] = -1;
            tGoDone[nf] = false;
            nf++;
        }
        // second pass over the SAME reply: match `#e` epoch fields to their
        // partitions (hash field order is arbitrary, so this runs after all
        // partitions are collected — no extra hop, same buffer)
        i = ri;
        for (long e = 0; e + 1 < n; e += 2)
        {
            auto field = respBulk(d, i);
            auto val = respBulk(d, i);
            if (field is null || val is null)
                break;
            if (field.length < 3 || field[$ - 2] != '#' || field[$ - 1] != 'e')
                continue;
            auto core = field[0 .. $ - 2];
            size_t sl = core.length;
            foreach_reverse (k; 0 .. core.length)
                if (core[k] == '/')
                {
                    sl = k;
                    break;
                }
            if (sl >= core.length)
                continue;
            int part = 0;
            foreach (c; core[sl + 1 .. $])
                if (c >= '0' && c <= '9')
                    part = part * 10 + (c - '0');
            int ev = 0;
            long got;
            foreach (c; val)
                if (c >= '0' && c <= '9')
                {
                    ev = ev * 10 + (c - '0');
                    got++;
                }
            foreach (j; 0 .. nf)
                if (tGoTp[j] == part && tGoTf[j] == cast(const(char)[]) core[0 .. sl])
                {
                    tGoEp[j] = got ? ev : -1;
                    break;
                }
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
                putI32(o, tGoEp[j]); // committed_leader_epoch
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
            putI32(o, tGoEp[j]); // committed_leader_epoch (v5+)
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
    if (!authorize(tKafkaCtx, KRES_GROUP, group, KOP_READ))
    {
        putI32(o, 0); // throttle_time_ms
        putCArrLen(o, 0); // zero topics (compact)
        putI16(o, E_GROUP_AUTH_FAILED); // top-level error_code
        putTaggedFields(o);
        return;
    }
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
                putI32(o, part);
                putI64(o, off); // committed_offset (-1 = none)
                putI32(o, off >= 0 ? fetchGroupEpoch(group, topic, part) : -1);
                // fill meta LAST: no hop between fill and read.
                static ByteBuffer mbf;
                immutable hasMeta = off >= 0 && fetchGroupMeta(group, topic, part, mbf);
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
    if (!authorize(tKafkaCtx, KRES_GROUP, group, KOP_READ))
    {
        if (ver >= 3)
            putI32(o, 0); // throttle_time_ms
        putI32(o, 0); // zero topics
        if (ver >= 2)
            putI16(o, E_GROUP_AUTH_FAILED); // top-level error_code
        return;
    }
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
                putI32(o, part);
                putI64(o, off); // committed_offset (-1 = none)
                if (ver >= 5)
                    putI32(o, off >= 0 ? fetchGroupEpoch(group, topic, part) : -1);
                // fill meta LAST — no hop between the fill and the read, so a
                // sibling OffsetFetch cannot clobber the shared TLS buffer.
                static ByteBuffer mb;
                immutable hasMeta = off >= 0 && fetchGroupMeta(group, topic, part, mb);
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

/// Heartbeat (v0-v4): real coordinator — validates member + generation,
/// answers REBALANCE_IN_PROGRESS while a barrier is open (the client's cue to
/// re-join).
private void handleHeartbeat(ref Rd r, short ver, ref ByteBuffer o) nothrow @trusted
{
    immutable flex = isFlexible(API_HEARTBEAT, ver); // v4 flexible
    const(char)[] group, mid;
    int gen;
    if (flex)
    {
        group = r.cstr();
        gen = r.i32();
        mid = r.cstr();
        cast(void) r.cstr(); // group_instance_id (nullable, v3+)
    }
    else
    {
        group = r.str();
        gen = r.i32();
        mid = r.str();
        if (ver >= 3)
            cast(void) r.str(); // group_instance_id
    }
    if (group.length && !authorize(tKafkaCtx, KRES_GROUP, group, KOP_READ))
    {
        if (flex)
        {
            putI32(o, 0); // throttle_time_ms
            putI16(o, E_GROUP_AUTH_FAILED); // error_code
            putTaggedFields(o);
        }
        else
        {
            if (ver >= 1)
                putI32(o, 0); // throttle_time_ms
            putI16(o, E_GROUP_AUTH_FAILED); // error_code
        }
        return;
    }
    short err = KG_REBALANCE_IN_PROGRESS; // transport failure: safe retryable
    if (r.ok && group.length)
    {
        static ByteBuffer req, rep; // TLS: consumed synchronously per hop
        req.clear();
        req.appendByte(cast(char) KGOP_HEARTBEAT);
        putStr(req, group);
        putStr(req, mid);
        putI32(req, gen);
        if (kgOp(group, cast(const(ubyte)[]) req.data, rep))
        {
            Rd rr = Rd(cast(const(ubyte)[]) rep.data);
            err = rr.i16();
        }
    }
    if (flex)
    {
        putI32(o, 0); // throttle_time_ms
        putI16(o, err);
        putTaggedFields(o);
        return;
    }
    if (ver >= 1)
        putI32(o, 0); // throttle_time_ms
    putI16(o, err);
}

/// SyncGroup (v0-v3): the leader shipped the computed assignment; echo this
/// member's assignment back. Single-member group, so we return the (only)
/// assignment the leader provided.
/// Emit a SyncGroup ERROR response (classic or flex).
private void emitSyncErr(ref ByteBuffer o, short ver, bool flex, short err) nothrow @trusted
{
    if (flex)
    {
        putI32(o, 0); // throttle_time_ms
        putI16(o, err);
        if (ver >= 5)
        {
            putCStrNull(o, null, true); // protocol_type
            putCStrNull(o, null, true); // protocol_name
        }
        putCBytes(o, null, false); // assignment: empty
        putTaggedFields(o);
        return;
    }
    if (ver >= 1)
        putI32(o, 0); // throttle_time_ms
    putI16(o, err);
    putBytesI32(o, null, false); // assignment: empty
}

/// Drive SyncGroup against the coordinator: the leader's first op carries the
/// assignments; followers (and the leader once delivered) poll until the
/// group turns Stable, then receive their own assignment.
private void syncLoop(ref ByteBuffer o, short ver, bool flex, scope const(char)[] group,
        scope const(char)[] memberId, int gen, ref ByteBuffer req,
        scope const(char)[] ptype, bool ptypeNull, scope const(char)[] pname,
        bool pnameNull) nothrow @trusted
{
    static ByteBuffer rep; // TLS: parsed synchronously after each hop
    immutable deadline = kMonoMs() + 70_000;
    for (;;)
    {
        if (!kgOp(group, cast(const(ubyte)[]) req.data, rep))
        {
            emitSyncErr(o, ver, flex, KG_REBALANCE_IN_PROGRESS);
            return;
        }
        Rd rr = Rd(cast(const(ubyte)[]) rep.data);
        immutable e = rr.i16();
        if (e == KG_WAIT)
        {
            if (kMonoMs() >= deadline)
            {
                emitSyncErr(o, ver, flex, KG_REBALANCE_IN_PROGRESS);
                return;
            }
            kSleepMs(15);
            req.clear(); // TLS req may have been reused during the sleep: rebuild
            req.appendByte(cast(char) KGOP_SYNC);
            putStr(req, group);
            putStr(req, memberId);
            putI32(req, gen);
            putI32(req, 0); // polls never re-deliver assignments
            continue;
        }
        if (e != KG_NONE)
        {
            emitSyncErr(o, ver, flex, e);
            return;
        }
        auto assign = rr.bytesI32();
        if (flex)
        {
            putI32(o, 0); // throttle_time_ms
            putI16(o, E_NONE);
            if (ver >= 5)
            {
                putCStrNull(o, ptype, ptypeNull); // echoed (v5+)
                putCStrNull(o, pname, pnameNull);
            }
            putCBytes(o, assign, false);
            putTaggedFields(o);
            return;
        }
        if (ver >= 1)
            putI32(o, 0); // throttle_time_ms
        putI16(o, E_NONE);
        putBytesI32(o, assign, false);
        return;
    }
}

/// SyncGroup v4/v5 (flexible): real coordinator. v5 adds protocol_type/name
/// (the old stub read them at v4 too — a misparse).
private void handleSyncGroupFlex(ref Rd r, short ver, ref ByteBuffer o) nothrow @trusted
{
    auto group = r.cstr();
    if (group.length && !authorize(tKafkaCtx, KRES_GROUP, group, KOP_READ))
    {
        emitSyncErr(o, ver, true, E_GROUP_AUTH_FAILED);
        return;
    }
    immutable gen = r.i32();
    auto memberId = r.cstr();
    cast(void) r.cstr(); // group_instance_id (nullable, v3+)
    const(char)[] ptype = null, pname = null;
    if (ver >= 5)
    {
        ptype = r.cstr(); // nullable (v5+)
        pname = r.cstr();
    }
    immutable nassignRaw = r.carrlen();
    immutable nassign = nassignRaw < 0 ? 0 : safeCount(nassignRaw);
    static ByteBuffer req; // TLS: consumed synchronously by the hop copy
    req.clear();
    req.appendByte(cast(char) KGOP_SYNC);
    putStr(req, group);
    putStr(req, memberId);
    putI32(req, gen);
    immutable naOff = req.length;
    putI32(req, 0);
    int na = 0;
    foreach (_; 0 .. nassign)
    {
        if (!r.ok)
            break;
        auto mid = r.cstr();
        auto assign = r.cbytes();
        r.skipTaggedFields(); // per-assignment tagged fields
        if (!r.ok)
            break;
        if (na < 512)
        {
            putStr(req, mid);
            putI32(req, cast(int)(assign is null ? 0 : assign.length));
            if (assign !is null)
                req.append(cast(const(char)[]) assign);
            na++;
        }
    }
    patchI32(req, naOff, na);
    if (!r.ok || group is null || group.length == 0)
    {
        emitSyncErr(o, ver, true, KG_UNKNOWN_MEMBER);
        return;
    }
    syncLoop(o, ver, true, group, memberId, gen, req, ptype, ptype is null,
            pname, pname is null);
}

/// SyncGroup v0-v3 (classic): real coordinator.
private void handleSyncGroup(ref Rd r, short ver, ref ByteBuffer o) nothrow @trusted
{
    if (isFlexible(API_SYNC_GROUP, ver)) // v4/v5 flexible
    {
        handleSyncGroupFlex(r, ver, o);
        return;
    }
    auto group = r.str();
    if (group.length && !authorize(tKafkaCtx, KRES_GROUP, group, KOP_READ))
    {
        emitSyncErr(o, ver, false, E_GROUP_AUTH_FAILED);
        return;
    }
    immutable gen = r.i32();
    auto memberId = r.str();
    if (ver >= 3)
        cast(void) r.str(); // group_instance_id (nullable)
    immutable nassign = safeCount(r.i32());
    static ByteBuffer req; // TLS: consumed synchronously by the hop copy
    req.clear();
    req.appendByte(cast(char) KGOP_SYNC);
    putStr(req, group);
    putStr(req, memberId);
    putI32(req, gen);
    immutable naOff = req.length;
    putI32(req, 0);
    int na = 0;
    foreach (_; 0 .. nassign)
    {
        if (!r.ok)
            break;
        auto mid = r.str();
        auto assign = r.bytesI32();
        if (!r.ok)
            break;
        if (na < 512)
        {
            putStr(req, mid);
            putI32(req, cast(int)(assign is null ? 0 : assign.length));
            if (assign !is null)
                req.append(cast(const(char)[]) assign);
            na++;
        }
    }
    patchI32(req, naOff, na);
    if (!r.ok || group.length == 0)
    {
        emitSyncErr(o, ver, false, KG_UNKNOWN_MEMBER);
        return;
    }
    syncLoop(o, ver, false, group, memberId, gen, req, null, true, null, true);
}

/// LeaveGroup (v0-v4): real coordinator — removes the member(s) and opens a
/// rebalance for the survivors. v3+ batches members.
private void handleLeaveGroup(ref Rd r, short ver, ref ByteBuffer o) nothrow @trusted
{
    immutable flex = isFlexible(API_LEAVE_GROUP, ver); // v4 flexible
    const(char)[] group;
    const(char)[][32] mids;
    const(char)[][32] giis;
    bool[32] giiNull;
    size_t nm;
    if (flex)
    {
        group = r.cstr();
        immutable rawc = r.carrlen();
        immutable cnt = rawc < 0 ? 0 : safeCount(rawc);
        foreach (i; 0 .. cnt)
        {
            if (!r.ok)
                break;
            auto mid = r.cstr();
            auto gii = r.cstr(); // group_instance_id (nullable compact)
            r.skipTaggedFields(); // member tagged fields
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
        group = r.str();
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
                giiNull[0] = true;
                nm = 1;
            }
        }
    }
    debug (kgroup)
    {
        import core.stdc.stdio : fprintf, stderr;
        fprintf(stderr, "KG WIRE-LEAVE v%d flex=%d group=%.*s nm=%d ok=%d\n",
                cast(int) ver, flex ? 1 : 0, cast(int) group.length, group.ptr,
                cast(int) nm, r.ok ? 1 : 0);
    }
    if (group.length && !authorize(tKafkaCtx, KRES_GROUP, group, KOP_READ))
    {
        if (flex)
        {
            putI32(o, 0); // throttle_time_ms
            putI16(o, E_GROUP_AUTH_FAILED); // error_code
            putCArrLen(o, 0); // members
            putTaggedFields(o);
        }
        else
        {
            if (ver >= 1)
                putI32(o, 0); // throttle_time_ms
            putI16(o, E_GROUP_AUTH_FAILED); // error_code
            if (ver >= 3)
                putI32(o, 0); // members
        }
        return;
    }
    // one atomic LEAVE op removes every named member and rebalances once
    short[32] perr = KG_UNKNOWN_MEMBER;
    if (r.ok && group.length && nm > 0)
    {
        static ByteBuffer req, rep; // TLS: consumed synchronously per hop
        req.clear();
        req.appendByte(cast(char) KGOP_LEAVE);
        putStr(req, group);
        putI32(req, cast(int) nm);
        foreach (i; 0 .. nm)
            putStr(req, mids[i]);
        if (kgOp(group, cast(const(ubyte)[]) req.data, rep))
        {
            Rd rr = Rd(cast(const(ubyte)[]) rep.data);
            cast(void) rr.i16(); // top-level (always NONE from the FSM)
            immutable n2 = rr.i32();
            foreach (i; 0 .. (n2 < 0 ? 0 : n2))
            {
                immutable pe = rr.i16();
                if (cast(size_t) i < nm && rr.ok)
                    perr[i] = pe;
            }
        }
    }
    // classic v<3: the single member's error IS the response error
    immutable short topErr = (!flex && ver < 3 && nm > 0) ? perr[0] : E_NONE;
    if (flex)
    {
        putI32(o, 0); // throttle_time_ms
        putI16(o, E_NONE); // error_code
        putCArrLen(o, cast(int) nm); // members
        foreach (i; 0 .. nm)
        {
            putCStr(o, mids[i]); // member_id
            putCStrNull(o, giis[i], giiNull[i]); // group_instance_id
            putI16(o, perr[i]); // member error_code
            putTaggedFields(o); // member tagged fields
        }
        putTaggedFields(o); // response tagged fields
        return;
    }
    if (ver >= 1)
        putI32(o, 0); // throttle_time_ms (LeaveGroup: v1+)
    putI16(o, topErr); // error_code
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
            putI16(o, perr[i]); // member error_code
        }
    }
}
private void handleDescribeConfigs(ref Rd r, short ver, ref ByteBuffer o) nothrow @trusted
{
    if (!authorize(tKafkaCtx, KRES_CLUSTER, "kafka-cluster", KOP_DESCRIBE))
    {
        putI32(o, 0); // throttle_time_ms
        putI32(o, 0); // zero results = well-formed authorization denial
        return;
    }
    immutable nres = safeCount(r.i32());
    // STACK-local: the existence probe below hops cross-shard and yields; a
    // TLS static would be clobbered by another connection during the park.
    byte[256] rtype;
    const(char)[][256] rname;
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
        // describing a topic nobody created answers 3 with no entries (0081);
        // GROUP configs (wire type 32) answer INVALID_REQUEST in this dialect
        immutable missing = rtype[i] == 2
            && (!validTopic(rname[i]) || registeredTopicPartitions(rname[i]) < 0);
        immutable isGroup = rtype[i] == 32 || rtype[i] == 3;
        putI16(o, isGroup ? cast(short) 42
                : (missing ? E_UNKNOWN_TOPIC : E_NONE)); // error_code
        putI16(o, -1); // error_message = null
        o.appendByte(cast(char) rtype[i]); // resource_type
        putStr(o, rname[i]); // resource_name
        // resource_type 2 == TOPIC: emit the fixed defaults an inspector needs
        // (a stateless broker has no per-topic overrides); others: empty.
        if (rtype[i] == 2 && !missing)
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
    // STACK-local: the ops below hop cross-shard and yield; a shared static
    // would be clobbered by another connection during the park.
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
    bool wantAuthz = false;
    if (ver >= 3 && r.ok)
    {
        immutable f = r.i8(); // include_authorized_operations (v3+)
        if (r.ok)
            wantAuthz = f != 0;
        else
            r.ok = true;
    }
    if (ver >= 1)
        putI32(o, 0); // throttle_time_ms
    putI32(o, cast(int) ng); // groups count
    static immutable string[4] stateNames = ["Empty", "PreparingRebalance",
        "CompletingRebalance", "Stable"];
    foreach (i; 0 .. ng)
    {
        if (!authorize(tKafkaCtx, KRES_GROUP, groups[i], KOP_DESCRIBE))
        {
            putI16(o, E_GROUP_AUTH_FAILED); // error_code
            putStr(o, groups[i]); // group_id
            putStr(o, ""); // group_state
            putStr(o, ""); // protocol_type
            putStr(o, ""); // protocol_data
            putI32(o, 0); // members
            if (ver >= 3)
                putI32(o, int.min); // authorized_operations (denied/not requested)
            continue;
        }
        static ByteBuffer req, rep; // TLS: consumed synchronously per hop
        req.clear();
        req.appendByte(cast(char) KGOP_DESCRIBE);
        putStr(req, groups[i]);
        bool live = kgOp(groups[i], cast(const(ubyte)[]) req.data, rep);
        Rd rr = Rd(cast(const(ubyte)[]) rep.data);
        if (live)
            cast(void) rr.i16(); // FSM error (always NONE)
        immutable st = live ? rr.i8() : 0;
        immutable gen = live ? rr.i32() : 0;
        auto proto = live ? rr.str() : null;
        auto ptype = live ? rr.str() : null;
        immutable nmRaw = live ? rr.i32() : 0;
        immutable nmemb = nmRaw < 0 ? 0 : nmRaw;
        cast(void) gen;
        // a group with no live members but committed offsets is "Empty";
        // fully unknown is "Dead"
        immutable dead = nmemb == 0 && st == 0 && !groupExists(groups[i]);
        putI16(o, E_NONE); // error_code
        putStr(o, groups[i]); // group_id
        putStr(o, dead ? "Dead" : stateNames[st < 4 ? st : 0]); // group_state
        putStr(o, nmemb ? ptype : (dead ? "" : "consumer")); // protocol_type
        putStr(o, nmemb ? proto : ""); // protocol_data
        putI32(o, nmemb); // members
        foreach (_; 0 .. nmemb)
        {
            auto mid = rr.str();
            immutable gN = rr.i8() != 0;
            auto gii = rr.str();
            auto cid = rr.str();
            auto meta = rr.bytesI32();
            auto assign = rr.bytesI32();
            putStr(o, mid); // member_id
            if (ver >= 4)
            {
                if (gN)
                    putI16(o, -1); // group_instance_id = null
                else
                    putStr(o, gii);
            }
            putStr(o, cid.length ? cid : mid); // client_id (recorded at join)
            putStr(o, ""); // client_host (not tracked)
            putBytesI32(o, meta, false); // member_metadata
            putBytesI32(o, assign, false); // member_assignment
        }
        if (ver >= 3)
        {
            // INT32_MIN = "not requested" (clients must see NULL); when
            // requested: READ+DELETE+DESCRIBE — the group ops we allow
            enum int GROUP_OPS = (1 << 3) | (1 << 6) | (1 << 8);
            putI32(o, wantAuthz ? GROUP_OPS : int.min);
        }
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
        putI32(o, tKafkaAdvPort);
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
    putI32(o, tKafkaAdvPort);
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
/// The broker's topic-config registry: names a client may set (CreateTopics
/// configs, AlterConfigs, IncrementalAlterConfigs). An unknown name answers
/// INVALID_CONFIG(40) — 0081 probes with "dummy.doesntexist".
private bool knownTopicConfig(scope const(char)[] n) @nogc nothrow pure
{
    switch (n)
    {
    case "cleanup.policy": case "compression.type": case "delete.retention.ms":
    case "file.delete.delay.ms": case "flush.messages": case "flush.ms":
    case "follower.replication.throttled.replicas": case "index.interval.bytes":
    case "leader.replication.throttled.replicas": case "local.retention.bytes":
    case "local.retention.ms": case "max.compaction.lag.ms":
    case "max.message.bytes": case "message.downconversion.enable":
    case "message.format.version": case "message.timestamp.difference.max.ms":
    case "message.timestamp.type": case "message.timestamp.after.max.ms":
    case "message.timestamp.before.max.ms": case "min.cleanable.dirty.ratio":
    case "min.compaction.lag.ms": case "min.insync.replicas": case "preallocate":
    case "remote.storage.enable": case "remote.log.copy.disable":
    case "remote.log.delete.on.disable": case "retention.bytes":
    case "retention.ms": case "segment.bytes": case "segment.index.bytes":
    case "segment.jitter.ms": case "segment.ms":
    case "unclean.leader.election.enable":
        return true;
    default:
        return false;
    }
}

private enum short E_INVALID_CONFIG = 40;

private void handleCreateTopics(ref Rd r, short ver, ref ByteBuffer o) nothrow @trusted
{
    if (!authorize(tKafkaCtx, KRES_CLUSTER, "kafka-cluster", KOP_CREATE))
    {
        if (ver >= 2)
            putI32(o, 0); // throttle_time_ms
        putI32(o, 0); // zero topics = well-formed authorization denial
        return;
    }
    immutable ntopics = safeCount(r.i32());
    // STACK-local (not TLS static): registerTopic below hops cross-shard and
    // yields; a shared static would be clobbered by another connection's
    // handleCreateTopics during the park, registering the wrong topic.
    const(char)[][64] names;
    int[64] nparts;
    short[64] errs;
    // per-topic configs, flattened (slices point into the stable request buf)
    enum size_t MAXCFG = 256;
    size_t[64] cfgFrom;
    size_t[64] cfgCount;
    const(char)[][MAXCFG] cfgNames;
    const(char)[][MAXCFG] cfgVals;
    size_t ncfgAll;
    size_t nt;
    foreach (_; 0 .. ntopics)
    {
        if (!r.ok)
            break;
        auto name = r.str();
        immutable np = r.i32(); // num_partitions (-1 = from assignments / default)
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
        // manual replica assignment: num_partitions is -1 on the wire and the
        // partition count is the assignment count (0081 creates 22 this way)
        immutable effNp = np > 0 ? np : (nassign > 0 ? nassign : np);
        short terr = E_NONE;
        immutable cfrom = ncfgAll;
        immutable ncfg = safeCount(r.i32());
        foreach (_2; 0 .. ncfg)
        {
            if (!r.ok)
                break;
            auto cname = r.str(); // config name
            auto cval = r.str(); // config value (nullable)
            if (!knownTopicConfig(cname))
                terr = E_INVALID_CONFIG; // 0081: unknown name fails the topic
            else if (ncfgAll < MAXCFG && cval !is null)
            {
                cfgNames[ncfgAll] = cname;
                cfgVals[ncfgAll] = cval;
                ncfgAll++;
            }
        }
        if (nt < names.length && r.ok)
        {
            names[nt] = name;
            errs[nt] = validTopic(name) ? terr : E_INVALID_TOPIC;
            immutable c = effNp > 0 ? effNp : cast(int) KAFKA_PARTITIONS;
            nparts[nt] = c > KAFKA_MAX_PARTITIONS ? KAFKA_MAX_PARTITIONS : c; // cap
            cfgFrom[nt] = cfrom;
            cfgCount[nt] = ncfgAll - cfrom;
            nt++;
        }
    }
    cast(void) r.i32(); // timeout_ms
    immutable validateOnly = ver >= 1 && r.ok && r.i8() != 0;
    if (!validateOnly)
        foreach (i; 0 .. nt)
            if (errs[i] == E_NONE)
            {
                registerTopic(names[i], nparts[i]);
                // persist the topic's declared configs (kafka.tcfg.<topic>)
                foreach (k; cfgFrom[i] .. cfgFrom[i] + cfgCount[i])
                {
                    if (gKafkaExec is null)
                        break;
                    static ByteBuffer ckey, crb;
                    topicCfgKey(names[i], ckey);
                    const(char)[][4] a = ["hset", cast(const(char)[]) ckey.data,
                        cfgNames[k], cfgVals[k]];
                    gKafkaExec(a[], crb);
                }
            }
    if (ver >= 2)
        putI32(o, 0); // throttle_time_ms
    putI32(o, cast(int) nt); // topics count
    foreach (i; 0 .. nt)
    {
        putStr(o, names[i]);
        putI16(o, errs[i]); // error_code
        if (ver >= 1)
            putI16(o, -1); // error_message = null
    }
}

/// AlterConfigs (v0-v1, KIP-133 legacy full-set alter): apply SETs for the
/// provided entries on kafka.tcfg.<topic>; unknown config name answers
/// INVALID_CONFIG for the resource. Non-topic resources are acked untouched.
private void handleAlterConfigs(ref Rd r, short ver, ref ByteBuffer o) nothrow @trusted
{
    if (!authorize(tKafkaCtx, KRES_CLUSTER, "kafka-cluster", KOP_ALTER_CONFIGS))
    {
        putI32(o, 0); // throttle_time_ms
        putI32(o, 0); // zero resources = well-formed authorization denial
        return;
    }
    immutable nres = safeCount(r.i32());
    byte[16] rtypes;
    const(char)[][16] rnames;
    short[16] rerrs;
    enum size_t MAXCFG = 128;
    size_t[16] cfgFrom;
    size_t[16] cfgCount;
    const(char)[][MAXCFG] cfgNames;
    const(char)[][MAXCFG] cfgVals;
    bool[MAXCFG] cfgNull;
    size_t ncfgAll;
    size_t nr;
    foreach (_; 0 .. nres)
    {
        if (!r.ok)
            break;
        immutable rtype = r.i8();
        auto rname = r.str();
        short rerr = E_NONE;
        immutable cfrom = ncfgAll;
        immutable ncfg = safeCount(r.i32());
        foreach (_2; 0 .. ncfg)
        {
            if (!r.ok)
                break;
            auto cname = r.str();
            auto cval = r.str(); // nullable
            if (rtype == 2 && !knownTopicConfig(cname))
                rerr = E_INVALID_CONFIG;
            else if (ncfgAll < MAXCFG)
            {
                cfgNames[ncfgAll] = cname;
                cfgVals[ncfgAll] = cval;
                cfgNull[ncfgAll] = cval is null;
                ncfgAll++;
            }
        }
        if (nr < rnames.length && r.ok)
        {
            rtypes[nr] = rtype;
            rnames[nr] = rname;
            rerrs[nr] = rerr;
            cfgFrom[nr] = cfrom;
            cfgCount[nr] = ncfgAll - cfrom;
            nr++;
        }
    }
    immutable validateOnly = r.ok && r.i8() != 0;
    foreach (i; 0 .. nr)
    {
        if (rerrs[i] != E_NONE || rtypes[i] != 2)
            continue;
        // altering a non-existent topic: pre-2.7 brokers answer
        // UNKNOWN_SERVER_ERROR(-1) — the dialect we present (0081 expects it)
        if (!validTopic(rnames[i]) || registeredTopicPartitions(rnames[i]) < 0)
        {
            rerrs[i] = -1;
            continue;
        }
        if (validateOnly || gKafkaExec is null)
            continue;
        foreach (k; cfgFrom[i] .. cfgFrom[i] + cfgCount[i])
        {
            static ByteBuffer ckey, crb;
            topicCfgKey(rnames[i], ckey);
            if (cfgNull[k])
            {
                const(char)[][3] a = ["hdel", cast(const(char)[]) ckey.data,
                    cfgNames[k]];
                gKafkaExec(a[], crb);
            }
            else
            {
                const(char)[][4] a = ["hset", cast(const(char)[]) ckey.data,
                    cfgNames[k], cfgVals[k]];
                gKafkaExec(a[], crb);
            }
        }
    }
    putI32(o, 0); // throttle_time_ms
    putI32(o, cast(int) nr); // resources
    foreach (i; 0 .. nr)
    {
        putI16(o, rerrs[i]);
        putI16(o, -1); // error_message = null
        o.appendByte(cast(char) rtypes[i]);
        putStr(o, rnames[i]);
    }
}

/// DeleteRecords (v0-v1, KIP-107): advance a partition's log start offset,
/// dropping the truncated head records. offset -1 = truncate to the high
/// watermark; past-the-end/negative = OFFSET_OUT_OF_RANGE(1).
private void handleDeleteRecords(ref Rd r, short ver, ref ByteBuffer o) nothrow @trusted
{
    import core.atomic : atomicOp;
    import core.stdc.stdio : snprintf;

    if (!authorize(tKafkaCtx, KRES_CLUSTER, "kafka-cluster", KOP_DELETE))
    {
        putI32(o, 0); // throttle_time_ms (v0+)
        putI32(o, 0); // zero topics = well-formed authorization denial
        return;
    }
    immutable ntopics = safeCount(r.i32());
    putI32(o, 0); // throttle_time_ms (v0+)
    immutable tOff = o.length;
    putI32(o, ntopics);
    int et = 0;
    foreach (_; 0 .. ntopics)
    {
        if (!r.ok)
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
            immutable target = r.i64();
            if (!r.ok)
                break;
            long low = -1;
            short err = E_NONE;
            if (!validTopic(topic) || part < 0)
                err = E_UNKNOWN_TOPIC;
            else
            {
                static ByteBuffer kb5; // TLS: consumed before the hops below
                partKey(topic, part, kb5);
                char[8 + KAFKA_MAX_TOPIC + 16] kst = void;
                immutable kl = kb5.length <= kst.length ? kb5.length : kst.length;
                kst[0 .. kl] = cast(const(char)[]) kb5.data[0 .. kl];
                auto key = cast(const(char)[]) kst[0 .. kl];
                immutable base = partBase(topic, part);
                immutable llen = partLen(key);
                immutable hw = base + llen;
                immutable want = target == -1 ? hw : target;
                if (want < base || want > hw)
                {
                    err = E_OFFSET_OUT_OF_RANGE;
                    low = -1;
                }
                else
                {
                    immutable drop = want - base;
                    if (drop > 0 && gKafkaExec !is null)
                    {
                        static ByteBuffer rb;
                        char[24] b1 = void;
                        immutable n1 = snprintf(b1.ptr, b1.length, "%lld", drop);
                        const(char)[][4] a = ["ltrim", key,
                            cast(const(char)[]) b1[0 .. n1], "-1"];
                        gKafkaExec(a[], rb);
                        // persist the new base + invalidate every shard's cache
                        static ByteBuffer bkb, rb2;
                        baseKey(topic, part, bkb);
                        char[10 + KAFKA_MAX_TOPIC + 16] bst = void;
                        immutable bl = bkb.length <= bst.length ? bkb.length : bst.length;
                        bst[0 .. bl] = cast(const(char)[]) bkb.data[0 .. bl];
                        char[24] b2 = void;
                        immutable n2 = snprintf(b2.ptr, b2.length, "%lld", want);
                        const(char)[][3] a2 = ["set",
                            cast(const(char)[]) bst[0 .. bl],
                            cast(const(char)[]) b2[0 .. n2]];
                        gKafkaExec(a2[], rb2);
                        atomicOp!"+="(gKafkaTruncEpoch, 1);
                    }
                    low = want;
                }
            }
            putI32(o, part);
            putI64(o, low); // low_watermark
            putI16(o, err);
            ep++;
        }
        patchI32(o, pOff, ep);
    }
    patchI32(o, tOff, et);
}

/// CreatePartitions (v0-v1): grow a registered topic's partition count. The
/// registry is the source of truth; unknown topic = 3, shrink/same = 37
/// (INVALID_PARTITIONS), growth re-registers the new count (capped).
private void handleCreatePartitions(ref Rd r, short ver, ref ByteBuffer o) nothrow @trusted
{
    if (!authorize(tKafkaCtx, KRES_CLUSTER, "kafka-cluster", KOP_ALTER))
    {
        putI32(o, 0); // throttle_time_ms (v0+)
        putI32(o, 0); // zero results = well-formed authorization denial
        return;
    }
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

// --- Kafka ACL admin (KIP-140): CreateAcls(30)/DescribeAcls(29)/DeleteAcls(31)
// v0-v1, LITERAL+PREFIXED patterns. Bindings live in the keyspace hash
// `kafka.acls`, field = type\x1f pattern\x1f name\x1f principal\x1f host\x1f
// op\x1f perm — routed like any key, AOF-persisted for free. This is the
// ADMIN surface (store + filter semantics); broker-side enforcement is a
// separate (gated) concern.
private enum string KAFKA_ACL_KEY = "kafka.acls";

private struct KAclBinding
{
    byte rtype;
    byte pattern;
    const(char)[] name;
    const(char)[] principal;
    const(char)[] host;
    byte op;
    byte perm;
}

/// Serialize a binding into the hash-field form.
private void aclField(const ref KAclBinding b, ref ByteBuffer o) nothrow
{
    o.clear();
    o.appendByte(cast(char)('0' + b.rtype));
    o.appendByte('\x1f');
    o.appendByte(cast(char)('0' + b.pattern));
    o.appendByte('\x1f');
    o.append(b.name);
    o.appendByte('\x1f');
    o.append(b.principal);
    o.appendByte('\x1f');
    o.append(b.host);
    o.appendByte('\x1f');
    o.appendByte(cast(char)('0' + b.op));
    o.appendByte('\x1f');
    o.appendByte(cast(char)('0' + b.perm));
}

/// Parse a stored field back; false on malformed.
private bool aclParse(scope const(char)[] f, out KAclBinding b) nothrow @trusted
{
    const(char)[][7] parts;
    size_t np, st;
    foreach (i2, ch; f)
        if (ch == '\x1f')
        {
            if (np < 7)
                parts[np++] = f[st .. i2];
            st = i2 + 1;
        }
    if (np != 6)
        return false;
    parts[np++] = f[st .. $];
    if (parts[0].length != 1 || parts[1].length != 1 || parts[5].length != 1
            || parts[6].length != 1)
        return false;
    b.rtype = cast(byte)(parts[0][0] - '0');
    b.pattern = cast(byte)(parts[1][0] - '0');
    b.name = parts[2];
    b.principal = parts[3];
    b.host = parts[4];
    b.op = cast(byte)(parts[5][0] - '0');
    b.perm = cast(byte)(parts[6][0] - '0');
    return true;
}

/// ACL ENFORCEMENT (drop-in M3c). Zero-cost until a user actually creates
/// ACLs (gKafkaAclActive stays 0 => authorize() returns allow immediately —
/// one relaxed atomic load on the hot path). Once ACLs exist, every data/
/// group/admin API checks the SASL principal against the stored bindings.
/// Semantics = Apache Kafka's SimpleAclAuthorizer: an explicit DENY wins;
/// else an ALLOW grants; else (no matching binding) DENY. The super-user
/// short-circuit: a principal listed in `kafka-super-users` bypasses checks.
import core.atomic : atomicLoad, atomicStore, MemoryOrder;

public shared long gKafkaAclActive; // >0 once any ACL binding exists

/// Per-request auth context for the enforcement checks in the handlers. Set
/// before the dispatch switch; authorize() copies the principal to the stack
/// before its own cross-shard hop, so a fiber switch cannot clobber it.
private KafkaConnCtx* tKafkaCtx;

/// Kafka ResourceType / AclOperation / PermissionType numeric constants.
private enum KRES_TOPIC = 2, KRES_GROUP = 3, KRES_CLUSTER = 4, KRES_TXNID = 5;
private enum KOP_ALL = 1, KOP_READ = 3, KOP_WRITE = 4, KOP_CREATE = 5,
        KOP_DELETE = 6, KOP_ALTER = 7, KOP_DESCRIBE = 8, KOP_CLUSTER_ACTION = 9,
        KOP_DESCRIBE_CONFIGS = 10, KOP_ALTER_CONFIGS = 11, KOP_IDEMPOTENT_WRITE = 12;
private enum KPERM_DENY = 2, KPERM_ALLOW = 3;
private enum KPAT_LITERAL = 3, KPAT_PREFIXED = 4;

/// Comma-separated super-user list (config kafka-super-users, e.g.
/// "User:admin,User:kafka"); a matching principal skips all checks.
public __gshared string gKafkaSuperUsers;

/// Boot primer: if the ACL store already holds bindings (AOF/keyspace replay),
/// turn enforcement ON — otherwise a restart would silently drop to allow-all
/// until the next CreateAcls. Called once after gKafkaExec is installed.
public void kafkaAclPrime() nothrow @trusted
{
    if (gKafkaExec is null)
        return;
    static ByteBuffer rb;
    const(char)[][2] a = ["hlen", KAFKA_ACL_KEY];
    gKafkaExec(a[], rb);
    // RESP integer ":N\r\n" — any N>0 means bindings exist
    auto d = cast(const(char)[]) rb.data;
    if (d.length >= 3 && d[0] == ':' && !(d.length == 4 && d[1] == '0'))
    {
        bool any = false;
        foreach (ch; d[1 .. $])
        {
            if (ch == '\r')
                break;
            if (ch >= '1' && ch <= '9')
                any = true;
        }
        if (any)
            atomicStore!(MemoryOrder.raw)(gKafkaAclActive, 1);
    }
}

private bool isSuperUser(scope const(char)[] principal) nothrow @trusted
{
    if (gKafkaSuperUsers.length == 0 || principal.length == 0)
        return false;
    auto sr = gKafkaSuperUsers;
    size_t st;
    foreach (i; 0 .. sr.length + 1)
        if (i == sr.length || sr[i] == ',')
        {
            auto entry = sr[st .. i];
            // entries are "User:<name>"; compare against "User:<principal>"
            if (entry.length > 5 && entry[0 .. 5] == "User:"
                    && entry[5 .. $] == principal)
                return true;
            st = i + 1;
        }
    return false;
}

/// True if op is authorized on (rtype,name) for principal. resourceName is the
/// topic/group/txn id; cluster ops pass "kafka-cluster".
private bool authorize(KafkaConnCtx* ctx, byte rtype, scope const(char)[] name,
        byte op) nothrow @trusted
{
    if (atomicLoad!(MemoryOrder.raw)(gKafkaAclActive) == 0)
        return true; // no ACLs configured: allow-all (legacy, zero cost)
    const(char)[] principal = ctx is null ? "ANONYMOUS" : ctx.principal;
    if (isSuperUser(principal))
        return true;
    // build the "User:<principal>" form once
    char[96] pb = void;
    size_t pl = 0;
    immutable pfx = "User:";
    if (5 + principal.length <= pb.length)
    {
        pb[0 .. 5] = pfx;
        pb[5 .. 5 + principal.length] = principal;
        pl = 5 + principal.length;
    }
    auto princForm = cast(const(char)[]) pb[0 .. pl];

    static ByteBuffer rb;
    KAclBinding[128] all;
    const(char)[][128] fields;
    immutable n = aclLoadAll(rb, all, fields);
    bool allow = false;
    foreach (i; 0 .. n)
    {
        auto b = all[i];
        if (b.rtype != rtype)
            continue;
        // principal: exact "User:x" or the wildcard "User:*"
        if (b.principal != princForm && b.principal != "User:*")
            continue;
        // operation: exact or ALL
        if (b.op != op && b.op != KOP_ALL)
            continue;
        // resource name: LITERAL exact or "*", PREFIXED prefix, else skip
        bool nameHit;
        if (b.pattern == KPAT_LITERAL)
            nameHit = b.name == name || b.name == "*";
        else if (b.pattern == KPAT_PREFIXED)
            nameHit = name.length >= b.name.length && name[0 .. b.name.length] == b.name;
        if (!nameHit)
            continue;
        if (b.perm == KPERM_DENY)
            return false; // explicit DENY always wins
        if (b.perm == KPERM_ALLOW)
            allow = true;
    }
    return allow;
}

/// KIP-140 filter match. Filter name/principal/host may be null (= any);
/// pattern: ANY(1) = any pattern (name exact when given), MATCH(2) = LITERAL
/// equal OR PREFIXED prefixing the filter name, LITERAL(3)/PREFIXED(4) exact.
private bool aclMatch(const ref KAclBinding f, const ref KAclBinding b) nothrow @trusted
{
    if (f.rtype != 1 && f.rtype != 0 && f.rtype != b.rtype)
        return false;
    if (f.op != 1 && f.op != 0 && f.op != b.op)
        return false;
    if (f.perm != 1 && f.perm != 0 && f.perm != b.perm)
        return false;
    if (f.principal !is null && f.principal.length && f.principal != b.principal)
        return false;
    if (f.host !is null && f.host.length && f.host != b.host)
        return false;
    switch (f.pattern)
    {
    default:
        return false;
    case 0: // UNKNOWN -> treat as ANY
    case 1: // ANY: any pattern type; name exact when given
        return f.name is null || f.name.length == 0 || f.name == b.name;
    case 2: // MATCH: acls affecting the named resource
        if (f.name is null || f.name.length == 0)
            return true;
        if (b.pattern == 3) // LITERAL: equal or the wildcard resource
            return b.name == f.name || b.name == "*";
        if (b.pattern == 4) // PREFIXED: binding name prefixes the filter name
            return b.name.length <= f.name.length && f.name[0 .. b.name.length] == b.name;
        return false;
    case 3: // LITERAL
    case 4: // PREFIXED
        return b.pattern == f.pattern
            && (f.name is null || f.name.length == 0 || f.name == b.name);
    }
}

private bool aclMatchDefault(const ref KAclBinding f, const ref KAclBinding b) nothrow @trusted
{
    if (f.pattern >= 0 && f.pattern <= 4)
        return aclMatch(f, b);
    return false;
}

/// Read one ACL creation/filter from the classic wire (v0 has no pattern
/// field — LITERAL implied).
private KAclBinding aclReadWire(ref Rd r, short ver, bool filter) nothrow @trusted
{
    KAclBinding b;
    b.rtype = r.i8();
    b.name = r.str(); // nullable in filters
    b.pattern = ver >= 1 ? r.i8() : cast(byte)(filter ? 1 : 3);
    b.principal = r.str();
    b.host = r.str();
    b.op = r.i8();
    b.perm = r.i8();
    return b;
}

/// Enumerate every stored binding into bs (parsing the HGETALL reply).
/// Returns the count. The slices point into the TLS reply buffer — consume
/// them before the next hop.
private size_t aclLoadAll(ref ByteBuffer rb, ref KAclBinding[128] bs,
        ref const(char)[][128] fields) nothrow @trusted
{
    if (gKafkaExec is null)
        return 0;
    const(char)[][2] a = ["hgetall", KAFKA_ACL_KEY];
    gKafkaExec(a[], rb);
    auto d = cast(const(char)[]) rb.data;
    size_t n, i2;
    if (d.length < 4 || d[0] != '*')
        return 0;
    i2 = 1;
    long cnt = 0;
    while (i2 < d.length && d[i2] != '\r')
        cnt = cnt * 10 + (d[i2++] - '0');
    i2 += 2;
    for (long e = 0; e + 1 < cnt && n < bs.length; e += 2)
    {
        // field bulk
        if (i2 >= d.length || d[i2] != '$')
            break;
        i2++;
        long fl = 0;
        while (i2 < d.length && d[i2] != '\r')
            fl = fl * 10 + (d[i2++] - '0');
        i2 += 2;
        if (i2 + fl + 2 > d.length)
            break;
        auto f = d[i2 .. i2 + cast(size_t) fl];
        i2 += cast(size_t) fl + 2;
        // value bulk (ignored)
        if (i2 >= d.length || d[i2] != '$')
            break;
        i2++;
        long vl = 0;
        while (i2 < d.length && d[i2] != '\r')
            vl = vl * 10 + (d[i2++] - '0');
        i2 += 2;
        if (i2 + vl + 2 > d.length)
            break;
        i2 += cast(size_t) vl + 2;
        KAclBinding b;
        if (aclParse(f, b))
        {
            bs[n] = b;
            fields[n] = f;
            n++;
        }
    }
    return n;
}

private void putStrNullable(ref ByteBuffer o, scope const(char)[] v, bool isNull) nothrow
{
    if (isNull)
        putI16(o, -1);
    else
        putStr(o, v);
}

/// CreateAcls (v0-v1): store each binding. LITERAL/PREFIXED only.
private void handleCreateAcls(ref Rd r, short ver, ref ByteBuffer o) nothrow @trusted
{
    // ACL administration requires cluster ALTER (super-users pass); otherwise an
    // anonymous client could self-grant a wildcard binding (AOF-persisted).
    if (!authorize(tKafkaCtx, KRES_CLUSTER, "kafka-cluster", KOP_ALTER))
    {
        putI32(o, 0); // throttle_time_ms
        putI32(o, 0); // zero results = well-formed authorization denial
        return;
    }
    immutable n = safeCount(r.i32());
    KAclBinding[64] bs;
    short[64] errs;
    size_t nb;
    foreach (_; 0 .. n)
    {
        if (!r.ok)
            break;
        auto b = aclReadWire(r, ver, false);
        if (nb < bs.length && r.ok)
        {
            errs[nb] = (b.pattern == 3 || b.pattern == 4) ? E_NONE
                : cast(short) 44; // INVALID_REQUEST-ish for filter patterns
            bs[nb] = b;
            nb++;
        }
    }
    foreach (i; 0 .. nb)
    {
        if (errs[i] != E_NONE || gKafkaExec is null)
            continue;
        static ByteBuffer fb, rb;
        aclField(bs[i], fb);
        const(char)[][4] a = ["hset", KAFKA_ACL_KEY,
            cast(const(char)[]) fb.data, "1"];
        gKafkaExec(a[], rb);
        atomicStore!(MemoryOrder.raw)(gKafkaAclActive, 1); // enforcement ON
    }
    putI32(o, 0); // throttle_time_ms
    putI32(o, cast(int) nb); // results
    foreach (i; 0 .. nb)
    {
        putI16(o, errs[i]);
        putI16(o, -1); // error_message = null
    }
}

/// DescribeAcls (v0-v1): filter the store, grouped by resource.
private void handleDescribeAcls(ref Rd r, short ver, ref ByteBuffer o) nothrow @trusted
{
    // The ACL store dump (principals/hosts) requires cluster DESCRIBE.
    if (!authorize(tKafkaCtx, KRES_CLUSTER, "kafka-cluster", KOP_DESCRIBE))
    {
        putI32(o, 0); // throttle_time_ms
        putI16(o, E_CLUSTER_AUTH_FAILED); // error_code
        putI16(o, -1); // error_message = null
        putI32(o, 0); // zero resources
        return;
    }
    auto f = aclReadWire(r, ver, true);
    static ByteBuffer rb;
    KAclBinding[128] all;
    const(char)[][128] fields;
    immutable n = aclLoadAll(rb, all, fields);
    bool[128] hit;
    bool[128] done;
    size_t nhit;
    foreach (i; 0 .. n)
        if (aclMatchDefault(f, all[i]))
        {
            hit[i] = true;
            nhit++;
        }
    putI32(o, 0); // throttle_time_ms
    putI16(o, E_NONE); // error_code
    putI16(o, -1); // error_message = null
    // group matches by (type, name, pattern)
    immutable resOff = o.length;
    putI32(o, 0);
    int nres = 0;
    foreach (i; 0 .. n)
    {
        if (!hit[i] || done[i])
            continue;
        o.appendByte(cast(char) all[i].rtype);
        putStr(o, all[i].name);
        if (ver >= 1)
            o.appendByte(cast(char) all[i].pattern);
        immutable aOff = o.length;
        putI32(o, 0);
        int na = 0;
        foreach (j; i .. n)
        {
            if (!hit[j] || done[j] || all[j].rtype != all[i].rtype
                    || all[j].pattern != all[i].pattern || all[j].name != all[i].name)
                continue;
            done[j] = true;
            putStr(o, all[j].principal);
            putStr(o, all[j].host);
            o.appendByte(cast(char) all[j].op);
            o.appendByte(cast(char) all[j].perm);
            na++;
        }
        patchI32(o, aOff, na);
        nres++;
    }
    patchI32(o, resOff, nres);
}

/// DeleteAcls (v0-v1): per filter, delete + echo the matching bindings.
private void handleDeleteAcls(ref Rd r, short ver, ref ByteBuffer o) nothrow @trusted
{
    // ACL administration requires cluster ALTER (super-users pass).
    if (!authorize(tKafkaCtx, KRES_CLUSTER, "kafka-cluster", KOP_ALTER))
    {
        putI32(o, 0); // throttle_time_ms
        putI32(o, 0); // zero filter_results = well-formed authorization denial
        return;
    }
    immutable nf = safeCount(r.i32());
    KAclBinding[16] filters;
    size_t nfl;
    foreach (_; 0 .. nf)
    {
        if (!r.ok)
            break;
        auto f = aclReadWire(r, ver, true);
        if (nfl < filters.length && r.ok)
            filters[nfl++] = f;
    }
    putI32(o, 0); // throttle_time_ms
    putI32(o, cast(int) nfl); // filter_results
    foreach (fi; 0 .. nfl)
    {
        static ByteBuffer rb;
        KAclBinding[128] all;
        const(char)[][128] fields;
        immutable n = aclLoadAll(rb, all, fields);
        putI16(o, E_NONE); // filter error_code
        putI16(o, -1); // error_message
        immutable mOff = o.length;
        putI32(o, 0);
        int nm = 0;
        // emit matches FIRST (slices point into rb — the HDEL hops below
        // would clobber them), collecting the field copies for deletion
        // STACK-local: the HDEL hops below yield; a TLS static would be
        // clobbered by another fiber's DeleteAcls during the park.
        ByteBuffer delArena;
        size_t[64] delFrom;
        size_t[64] delLen;
        size_t ndel;
        foreach (i; 0 .. n)
        {
            if (!aclMatchDefault(filters[fi], all[i]))
                continue;
            putI16(o, E_NONE); // matching acl error_code
            putI16(o, -1); // error_message
            o.appendByte(cast(char) all[i].rtype);
            putStr(o, all[i].name);
            if (ver >= 1)
                o.appendByte(cast(char) all[i].pattern);
            putStr(o, all[i].principal);
            putStr(o, all[i].host);
            o.appendByte(cast(char) all[i].op);
            o.appendByte(cast(char) all[i].perm);
            nm++;
            if (ndel < delFrom.length)
            {
                delFrom[ndel] = delArena.length;
                delArena.append(fields[i]);
                delLen[ndel] = fields[i].length;
                ndel++;
            }
        }
        patchI32(o, mOff, nm);
        foreach (k; 0 .. ndel)
        {
            if (gKafkaExec is null)
                break;
            static ByteBuffer rb2;
            auto fslice = cast(const(char)[]) delArena.data[delFrom[k] .. delFrom[k] + delLen[k]];
            const(char)[][3] a = ["hdel", KAFKA_ACL_KEY, fslice];
            gKafkaExec(a[], rb2);
        }
    }
}

/// IncrementalAlterConfigs v0: SET/DELETE/APPEND/SUBTRACT on per-topic configs.
/// Non-topic resource types are acked without effect (broker configs are
/// compile-time here). validate_only skips the writes.
private void handleIncrementalAlterConfigs(ref Rd r, short ver, ref ByteBuffer o) nothrow @trusted
{
    if (!authorize(tKafkaCtx, KRES_CLUSTER, "kafka-cluster", KOP_ALTER_CONFIGS))
    {
        putI32(o, 0); // throttle_time_ms
        putI32(o, 0); // zero responses = well-formed authorization denial
        return;
    }
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
    foreach (i; 0 .. nr)
    {
        if (rerrs[i] != E_NONE)
            continue;
        if (rtypes[i] == 32 || rtypes[i] == 3)
            rerrs[i] = 42; // GROUP configs (wire 32): INVALID_REQUEST here (0081)
        else if (rtypes[i] == 2
                && (!validTopic(rnames[i]) || registeredTopicPartitions(rnames[i]) < 0))
            rerrs[i] = -1; // unknown topic: UNKNOWN_SERVER_ERROR pre-2.7
    }
    if (!validateOnly && gKafkaExec !is null)
        foreach (i; 0 .. nops)
        {
            auto op = ops[i];
            if (op.res >= nr || rtypes[op.res] != 2 || rerrs[op.res] != E_NONE)
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
    if (!authorize(tKafkaCtx, KRES_CLUSTER, "kafka-cluster", KOP_DELETE))
    {
        if (ver >= 1)
            putI32(o, 0); // throttle_time_ms
        putI32(o, 0); // zero results = well-formed authorization denial
        return;
    }
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
    short[64] derrs = E_NONE;
    foreach (i; 0 .. nt)
    {
        if (gKafkaExec is null || !validTopic(names[i]))
        {
            derrs[i] = E_UNKNOWN_TOPIC;
            continue;
        }
        immutable np = registeredTopicPartitions(names[i]);
        if (np < 0)
        {
            derrs[i] = E_UNKNOWN_TOPIC; // deleting a topic nobody created (0081)
            continue;
        }
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
        putI16(o, derrs[i]);
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
            if (gKafkaAutoCreate && tMetaAllowAuto)
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
    // FIBER-LOCAL (not static): topics[] slices point INTO this buffer and are
    // read across registerTopic's cross-shard park below. A shared TLS buffer
    // would be clobbered by a sibling all-topics Metadata during that park.
    ByteBuffer allBuf; // SMEMBERS reply for the all-topics form
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
    // v4+ request flag: consumers with allow.auto.create.topics=false ask for
    // metadata WITHOUT creating (KIP-361, test 0109). Default true when the
    // read fails (truncated request) — the historic behavior.
    tMetaAllowAuto = true;
    bool wantTopicAuthz = false;
    bool wantClusterAuthz = false;
    if (r.ok)
    {
        immutable aat = r.i8();
        if (r.ok)
            tMetaAllowAuto = aat != 0;
        else
            r.ok = true; // flag absent: tolerate (header-only probes)
    }
    if (r.ok && ver <= 10)
    {
        if (ver >= 8 && ver <= 10)
        {
            immutable ica = r.i8(); // include_cluster_authorized_operations
            if (r.ok)
                wantClusterAuthz = ica != 0;
        }
        immutable ita = r.i8(); // include_topic_authorized_operations (v8+)
        if (r.ok)
            wantTopicAuthz = ita != 0;
        else
            r.ok = true;
    }
    // The explicit-topic Metadata request is Kafka's auto-create moment:
    // register each VALID requested topic so the all-topics form lists it —
    // but ONLY when BOTH the broker mode and the request allow it. Registry
    // mode (DREADS_KAFKA_AUTOCREATE=false) must keep a merely-queried topic
    // MISSING (golib Inspector).
    // Safe to yield here — only fiber-local state (o) is staged.
    if (gKafkaAutoCreate && tMetaAllowAuto)
        foreach (t; topics[0 .. nt])
            if (validTopic(t))
                registerTopic(t);

    putI32(o, 0); // throttle_time_ms
    // brokers: just us
    putCArrLen(o, 1);
    putI32(o, 0); // node_id
    putCStr(o, gKafkaHost);
    putI32(o, tKafkaAdvPort);
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
        // INT32_MIN = "not requested"; else the full topic-op set the
        // DescribeTopics admin test expects (ALTER, ALTER_CONFIGS, CREATE,
        // DELETE, DESCRIBE, DESCRIBE_CONFIGS, READ, WRITE)
        enum int TOPIC_OPS = (1 << 3) | (1 << 4) | (1 << 5) | (1 << 6)
            | (1 << 7) | (1 << 8) | (1 << 10) | (1 << 11);
        putI32(o, wantTopicAuthz ? TOPIC_OPS : int.min); // (v8+)
        putTaggedFields(o); // topic tagged fields
    }
    // cluster ops when requested: ALTER, ALTER_CONFIGS, CLUSTER_ACTION,
    // CREATE, DESCRIBE, DESCRIBE_CONFIGS, IDEMPOTENT_WRITE (0081)
    enum int CLUSTER_OPS = (1 << 5) | (1 << 7) | (1 << 8) | (1 << 9)
        | (1 << 10) | (1 << 11) | (1 << 12);
    putI32(o, wantClusterAuthz ? CLUSTER_OPS : int.min); // (v8..v10)
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
    tMetaAllowAuto = true;
    if (ver >= 4 && r.ok)
    {
        immutable aat = r.i8(); // allow_auto_topic_creation (v4+)
        if (r.ok)
            tMetaAllowAuto = aat != 0;
        else
            r.ok = true; // tolerate a truncated tail
    }

    // brokers: just us
    putI32(o, 1);
    putI32(o, 0); // node_id 0
    putStr(o, gKafkaHost);
    putI32(o, tKafkaAdvPort);
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
                if (gKafkaAutoCreate && tMetaAllowAuto)
                    np = cast(int) KAFKA_PARTITIONS; // auto-exist (compat default)
                else
                    terr = E_UNKNOWN_TOPIC; // registry mode / request said no
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

// --- idempotent-producer state (KIP-98 T1; see KAFKA-TXN-PLAN.md) ----------
// Per-partition producer state: hash `kafka.pid.<topic>.<p>`, field = pid,
// value = "<epoch>:<lastBaseSeq>:<lastCount>:<lastBaseOffset>". Producers
// without a producer id (pid -1 in the v2 batch header) never touch it.
private void pidKey(scope const(char)[] topic, int part, ref ByteBuffer o) @nogc nothrow
{
    import core.stdc.stdio : snprintf;

    o.clear();
    o.append("kafka.pid.");
    o.append(topic);
    char[16] nb = void;
    immutable n = snprintf(nb.ptr, nb.length, ".%d", part);
    o.append(nb[0 .. n]);
}

/// Sequence/epoch admission for one idempotent batch. NONE = accept;
/// dupBase >= 0 = exact retry of the last acked batch (ack its offset, store
/// nothing); 45/47 otherwise.
private short pidCheck(scope const(char)[] topic, int part, long pid, short epoch,
        int seq, int cnt, out long dupBase) nothrow @trusted
{
    import core.stdc.stdio : snprintf;

    dupBase = -1;
    if (gKafkaExec is null)
        return E_NONE;
    static ByteBuffer kb7, rb;
    pidKey(topic, part, kb7);
    char[24] fb = void;
    immutable fl = snprintf(fb.ptr, fb.length, "%lld", pid);
    const(char)[][3] a = ["hget", cast(const(char)[]) kb7.data,
        cast(const(char)[]) fb[0 .. (fl > 0 ? fl : 0)]];
    gKafkaExec(a[], rb);
    auto d = rb.data;
    if (d.length < 4 || d[0] != '$' || d[1] == '-')
        return E_NONE; // first batch from this pid
    size_t i2 = 1;
    long blen = 0;
    while (i2 < d.length && d[i2] >= '0' && d[i2] <= '9')
        blen = blen * 10 + (d[i2++] - '0');
    if (i2 + 2 + blen > d.length)
        return E_NONE;
    i2 += 2;
    auto v = cast(const(char)[]) d[i2 .. i2 + cast(size_t) blen];
    long[4] parts2;
    size_t np2, st2;
    foreach (k, ch; v)
        if (ch == ':')
        {
            if (np2 < 3)
            {
                long x = 0;
                bool neg = st2 < k && v[st2] == '-';
                foreach (c; v[(neg ? st2 + 1 : st2) .. k])
                    if (c >= '0' && c <= '9')
                        x = x * 10 + (c - '0');
                parts2[np2++] = neg ? -x : x;
            }
            st2 = k + 1;
        }
    {
        long x = 0;
        bool neg = st2 < v.length && v[st2] == '-';
        foreach (c; v[(neg ? st2 + 1 : st2) .. $])
            if (c >= '0' && c <= '9')
                x = x * 10 + (c - '0');
        parts2[np2++] = neg ? -x : x;
    }
    if (np2 != 4)
        return E_NONE; // malformed state: accept rather than wedge the producer
    immutable sEpoch = cast(short) parts2[0];
    immutable lastSeq = cast(int) parts2[1];
    immutable lastCnt = cast(int) parts2[2];
    immutable lastBase = parts2[3];
    if (epoch < sEpoch)
        return E_INVALID_PRODUCER_EPOCH; // fenced zombie
    if (epoch > sEpoch)
        return E_NONE; // new epoch resets the sequence
    immutable int expected = cast(int)(cast(uint) lastSeq + cast(uint) lastCnt);
    if (seq == expected)
        return E_NONE;
    if (seq == lastSeq && cnt == lastCnt)
    {
        dupBase = lastBase; // exact retry of the last acked batch
        return E_NONE;
    }
    return E_OUT_OF_ORDER_SEQ;
}

/// Record the last accepted batch for the pid (after a successful append).
private void pidUpdate(scope const(char)[] topic, int part, long pid, short epoch,
        int seq, int cnt, long baseOff) nothrow @trusted
{
    import core.stdc.stdio : snprintf;

    if (gKafkaExec is null)
        return;
    static ByteBuffer kb8, rb;
    pidKey(topic, part, kb8);
    char[24] fb = void;
    immutable fl = snprintf(fb.ptr, fb.length, "%lld", pid);
    char[80] vb = void;
    immutable vl = snprintf(vb.ptr, vb.length, "%d:%d:%d:%lld", cast(int) epoch,
            seq, cnt, baseOff);
    const(char)[][4] a = ["hset", cast(const(char)[]) kb8.data,
        cast(const(char)[]) fb[0 .. (fl > 0 ? fl : 0)],
        cast(const(char)[]) vb[0 .. (vl > 0 ? vl : 0)]];
    gKafkaExec(a[], rb);
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
            else if (!authorize(tKafkaCtx, KRES_TOPIC, topic, KOP_WRITE))
                err = E_TOPIC_AUTH_FAILED; // ACL: no WRITE on this topic
            // Compaction gate (KIP-purgatory parity): a compacted topic
            // rejects keyless records with INVALID_RECORD + record_errors.
            // The config HGET hops cross-shard, so it runs HERE — before any
            // of the TLS statics below are staged (topic/records slice the
            // stable request buffer).
            immutable compacted = err == E_NONE && validTopic(topic) && part >= 0
                && topicCompacted(topic);
            // log start offset for the reply's baseOffset (epoch-gated: free
            // until a DeleteRecords ever ran) — hops BEFORE the TLS staging
            immutable pbase = err == E_NONE && validTopic(topic) && part >= 0
                ? partBase(topic, part) : 0;
            // Idempotent producer (KIP-98): the fixed v2 header carries
            // attributes@21, producerId@43, producerEpoch@51, baseSequence@53,
            // recordCount@57. pid -1 (the default) skips ALL of this — and
            // the admission HGET hops HERE, before the TLS staging below.
            long bPid = -1;
            short bEpoch = -1;
            short bAttrs = 0;
            int bSeq = -1, bCount = 0;
            if (records.length >= 61 && records[16] == 2)
            {
                bAttrs = cast(short)((cast(ushort) records[21] << 8) | records[22]);
                bPid = 0;
                foreach (k2; 43 .. 51)
                    bPid = (bPid << 8) | records[k2];
                bEpoch = cast(short)((cast(ushort) records[51] << 8) | records[52]);
                bSeq = (cast(int) records[53] << 24) | (cast(int) records[54] << 16)
                    | (cast(int) records[55] << 8) | records[56];
                bCount = (cast(int) records[57] << 24) | (cast(int) records[58] << 16)
                    | (cast(int) records[59] << 8) | records[60];
            }
            long dupBase = -1;
            bool skipStore = false;
            if (err == E_NONE && bPid >= 0 && validTopic(topic) && part >= 0)
            {
                immutable ie = pidCheck(topic, part, bPid, bEpoch, bSeq, bCount, dupBase);
                if (ie != E_NONE)
                    err = ie;
                else if (dupBase >= 0)
                    skipStore = true; // exact retry: ack the original offset
            }
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
                    if (bPid >= 0)
                        putInternalRecPid(blobArena, bPid, (bAttrs & 0x10) != 0,
                                ts, k, kn, v, vn, hdr);
                    else
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
            if (skipStore)
                nrec = 0; // duplicate: nothing stored, offset answered below
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
                    baseOffset = pbase + newLen - cast(long) nrec;
                    import core.atomic : atomicOp;
                    atomicOp!"+="(gKafkaProduced, nrec);
                    // idempotence bookkeeping (post-push: blobArena consumed)
                    if (bPid >= 0)
                    {
                        pidUpdate(topic, part, bPid, bEpoch, bSeq, bCount, baseOffset);
                        if ((bAttrs & 0x10) != 0) // transactional batch
                        {
                            import core.atomic : atomicStore;

                            atomicStore(gKafkaTxnSeen, cast(ubyte) 1);
                            txnNoteStart(topic, part, bPid, baseOffset);
                        }
                    }
                }
            }
            if (skipStore)
                baseOffset = dupBase; // duplicate retry acks the ORIGINAL offset
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
    byte isolation = 0;
    if (ver >= 4)
        isolation = r.i8(); // isolation_level (1 = read_committed)
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
            // base BEFORE staging kbBuild: partBase may hop (only after a
            // truncation ever happened — zero-cost epoch gate otherwise)
            immutable base = validTopic(topic) && part >= 0 ? partBase(topic, part) : 0;
            static ByteBuffer kbBuild; // TLS scratch (only used pre-yield)
            partKey(topic, part, kbBuild);
            char[8 + KAFKA_MAX_TOPIC + 16] keyStore = void;
            immutable klen = kbBuild.length <= keyStore.length ? kbBuild.length : keyStore.length;
            keyStore[0 .. klen] = cast(const(char)[]) kbBuild.data[0 .. klen];
            auto key = cast(const(char)[]) keyStore[0 .. klen];
            long llen = gKafkaLenRaw !is null ? gKafkaLenRaw(key) : -1;
            if (llen < 0)
            {
                tHopProbes++; // count the cross-shard hop against the budget
                llen = partLen(key);
            }
            immutable hw = base + llen; // absolute high watermark
            immutable fetchIdx = fetchOff - base; // list index of the offset
            immutable overCap = o.length > KAFKA_MAX_RESP; // response ceiling
            immutable noRead = !authorize(tKafkaCtx, KRES_TOPIC, topic, KOP_READ);
            immutable bad = fetchOff < base || fetchOff > hw || !validTopic(topic)
                || part < 0 || noRead;
            // transactions: zero-cost while gKafkaTxnSeen is 0 (one atomic
            // load); afterwards LSO + aborted ranges hop BEFORE emission
            long lso = hw;
            long[64] abPids = void;
            long[64] abOffs = void;
            size_t nab;
            bool txnActive;
            {
                import core.atomic : atomicLoad;

                txnActive = atomicLoad(gKafkaTxnSeen) != 0;
            }
            if (txnActive && !bad && ver >= 4)
            {
                lso = computeLso(topic, part, hw);
                nab = loadAborted(topic, part, abPids, abOffs);
            }
            // read_committed: records are served only BELOW the LSO — data of
            // an OPEN transaction stays invisible until its marker lands
            immutable long servEnd = (isolation == 1 && txnActive && lso < hw) ? lso : hw;
            putI32(o, part);
            putI16(o, noRead ? E_TOPIC_AUTH_FAILED
                    : (bad ? E_OFFSET_OUT_OF_RANGE : E_NONE));
            putI64(o, hw); // high watermark
            if (ver >= 4)
            {
                putI64(o, lso); // last_stable_offset
                if (ver >= 5)
                    putI64(o, base); // log_start_offset (v5+)
                putI32(o, cast(int) nab); // aborted_transactions
                foreach (k2; 0 .. nab)
                {
                    putI64(o, abPids[k2]); // producer_id
                    putI64(o, abOffs[k2]); // first_offset
                }
                if (ver >= 11)
                    putI32(o, -1); // preferred_read_replica (v11+): none
            }
            // records: re-encode stored blobs per fetch version — v0-v3 as a v1
            // MessageSet (down-converting v2-origin blobs, dropping headers),
            // v4+ as ONE RecordBatch v2 (carrying headers).
            immutable recAt = o.length;
            putI32(o, 0); // records byte size, patched below
            if (!bad && !overCap && fetchOff < servEnd)
            {
                int maxN = 16384; // deep batches: fewer walks per fetch
                if (servEnd - fetchOff < maxN)
                    maxN = cast(int)(servEnd - fetchOff); // LSO cap (read_committed)
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
                        direct = gKafkaFetchRaw(key, fetchIdx, maxN,
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
                        cast(void) rangeRecords(key, fetchIdx, maxN,
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
                        if (txnActive)
                        {
                            // control markers get their OWN batches, and data
                            // runs split whenever producer provenance changes
                            // (one batch = one pid, so read_committed clients
                            // can match aborted_transactions)
                            size_t st2 = 0;
                            long runPid = long.min; // unset
                            foreach (bi; 0 .. nb)
                            {
                                auto bl = fblobs[bi];
                                if (bl.length == 11 && bl[0] == 0xFE)
                                {
                                    if (bi > st2)
                                        encodeV2BatchFromInternal(o,
                                                fetchOff + st2, fblobs[st2 .. bi]);
                                    long cpid = 0;
                                    foreach (k3; 1 .. 9)
                                        cpid = (cpid << 8) | bl[k3];
                                    immutable short ctype =
                                        cast(short)((cast(short) bl[9] << 8) | bl[10]);
                                    encodeControlBatch(o, fetchOff + bi, cpid, ctype);
                                    st2 = bi + 1;
                                    runPid = long.min;
                                    continue;
                                }
                                long thisPid = -1;
                                if (bl.length >= 10 && bl[0] == 0xFD)
                                {
                                    thisPid = 0;
                                    foreach (k3; 1 .. 9)
                                        thisPid = (thisPid << 8) | bl[k3];
                                }
                                if (runPid == long.min)
                                    runPid = thisPid;
                                else if (thisPid != runPid)
                                {
                                    encodeV2BatchFromInternal(o, fetchOff + st2,
                                            fblobs[st2 .. bi]);
                                    st2 = bi;
                                    runPid = thisPid;
                                }
                            }
                            if (st2 < nb)
                                encodeV2BatchFromInternal(o, fetchOff + st2,
                                        fblobs[st2 .. nb]);
                        }
                        else
                            encodeV2BatchFromInternal(o, fetchOff, fblobs[0 .. nb]);
                        atomicOp!"+="(gKafkaFetched, cast(ulong) nb);
                    }
                }
                else
                {
                    long off = fetchOff;
                    int direct = -1;
                    if (gKafkaFetchRaw !is null)
                        direct = gKafkaFetchRaw(key, fetchIdx, maxN,
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
                        cast(void) rangeRecords(key, fetchIdx, maxN,
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
    byte isolation = 0;
    if (ver >= 2)
        isolation = r.i8(); // isolation_level (v2+; 1 = read_committed)
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
            immutable base = validTopic(topic) && part >= 0 ? partBase(topic, part) : 0;
            static ByteBuffer kb3build; // TLS scratch (pre-yield only)
            partKey(topic, part, kb3build);
            char[8 + KAFKA_MAX_TOPIC + 16] k3store = void;
            immutable k3len = kb3build.length <= k3store.length ? kb3build.length : k3store.length;
            k3store[0 .. k3len] = cast(const(char)[]) kb3build.data[0 .. k3len];
            auto k3 = cast(const(char)[]) k3store[0 .. k3len];
            long llen = gKafkaLenRaw !is null ? gKafkaLenRaw(k3) : -1;
            if (llen < 0)
            {
                tHopProbes++; // count the cross-shard hop against the budget
                llen = partLen(k3);
            }
            long off;
            long lend = base + llen; // latest
            if (isolation == 1)
            {
                import core.atomic : atomicLoad;

                // read_committed: "latest" is the last STABLE offset
                if (atomicLoad(gKafkaTxnSeen) != 0 && validTopic(topic) && part >= 0)
                    lend = computeLso(topic, part, lend);
            }
            if (ts == -2)
                off = base; // earliest = log start offset
            else if (ts < 0)
                off = lend; // -1 = latest (LSO under read_committed)
            else
            {
                off = offsetForTime(k3, llen, ts); // KIP-79; -1 = no match
                if (off >= 0)
                    off += base;
            }
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
/// v2 internal record WITH producer provenance: [0xFD][pid i64][u8 txnFlag]
/// then the same tail as KREC_TAG. Only idempotent/transactional producers
/// write it; read_committed clients need the pid on the data batches to match
/// the aborted-transactions list.
private enum ubyte KREC_TAG_PID = 0xFD;

private struct KRec2
{
    long pid = -1; // producer id (KREC_TAG_PID records; -1 otherwise)
    bool txn; // transactional-batch provenance
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

private void putInternalRecPid(ref ByteBuffer o, long pid, bool txn, long ts,
        scope const(ubyte)[] key, bool keyNull, scope const(ubyte)[] val,
        bool valNull, scope const(ubyte)[] hdrSection) @nogc nothrow
{
    o.appendByte(cast(char) KREC_TAG_PID);
    putI64(o, pid);
    o.appendByte(txn ? 1 : 0);
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
    if (b.length >= 10 && b[0] == KREC_TAG_PID)
    {
        Rd rd0 = Rd(b);
        rd0.i = 1;
        r.pid = rd0.i64();
        r.txn = rd0.i8() != 0;
        Rd rd = rd0; // continue with the shared tail
        r.ts = rd.i64();
        immutable kl0 = rd.i32();
        if (!rd.ok)
        {
            r.ok = false;
            return r;
        }
        if (kl0 < 0)
            r.keyNull = true;
        else if (rd.i + kl0 <= b.length)
        {
            r.key = b[rd.i .. rd.i + kl0];
            rd.i += kl0;
        }
        else
        {
            r.ok = false;
            return r;
        }
        immutable vl0 = rd.i32();
        if (!rd.ok)
        {
            r.ok = false;
            return r;
        }
        if (vl0 < 0)
            r.valNull = true;
        else if (rd.i + vl0 <= b.length)
        {
            r.val = b[rd.i .. rd.i + vl0];
            rd.i += vl0;
        }
        else
        {
            r.ok = false;
            return r;
        }
        immutable hl0 = rd.i32();
        if (rd.ok && hl0 >= 0 && rd.i + hl0 <= b.length)
            r.hdrSection = b[rd.i .. rd.i + hl0];
        else if (!rd.ok || hl0 > 0)
            r.ok = false;
        return r;
    }
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
/// ZigZag varint writer (the record-level varint encoding).
private void putZig(ref ByteBuffer o, long v) @nogc nothrow
{
    ulong u = (cast(ulong)(v) << 1) ^ cast(ulong)(v >> 63);
    while (u >= 0x80)
    {
        o.appendByte(cast(char)((u & 0x7F) | 0x80));
        u >>= 7;
    }
    o.appendByte(cast(char)(u & 0x7F));
}

/// One CONTROL batch (attrs isControl|isTransactional, count 1) for a stored
/// marker record. Clients skip it and learn the txn outcome from its key.
private void encodeControlBatch(ref ByteBuffer o, long offset, long pid,
        short ctype) @nogc nothrow
{
    static ByteBuffer bodyB, rbuf; // TLS: yield-free build
    rbuf.clear();
    rbuf.appendByte(0); // record attributes
    putZig(rbuf, 0); // timestampDelta
    putZig(rbuf, 0); // offsetDelta
    putZig(rbuf, 4); // key length
    putI16(rbuf, 0); // key: version
    putI16(rbuf, ctype); // key: type (0 abort, 1 commit)
    putZig(rbuf, 6); // value length
    putI16(rbuf, 0); // value: version
    putI32(rbuf, 0); // value: coordinator epoch
    putZig(rbuf, 0); // headers count
    bodyB.clear();
    putI16(bodyB, 0x0030); // attributes: isTransactional | isControl
    putI32(bodyB, 0); // lastOffsetDelta
    putI64(bodyB, 0); // firstTimestamp
    putI64(bodyB, 0); // maxTimestamp
    putI64(bodyB, pid); // producerId
    putI16(bodyB, 0); // producerEpoch
    putI32(bodyB, -1); // baseSequence
    putI32(bodyB, 1); // record count
    putZig(bodyB, cast(long) rbuf.length); // record length prefix
    bodyB.append(cast(const(char)[]) rbuf.data);
    putI64(o, offset); // baseOffset
    putI32(o, cast(int)(4 + 1 + 4 + bodyB.length)); // batchLength after this field
    putI32(o, 0); // partitionLeaderEpoch
    o.appendByte(2); // magic
    putI32(o, cast(int) crc32c(cast(const(ubyte)[]) bodyB.data)); // crc32c
    o.append(cast(const(char)[]) bodyB.data);
}

/// First transactional produce of a (pid, txn) on a partition records the
/// txn's first offset (HSETNX: later batches don't move it).
private void txnNoteStart(scope const(char)[] topic, int part, long pid,
        long firstOff) nothrow @trusted
{
    import core.stdc.stdio : snprintf;

    if (gKafkaExec is null)
        return;
    static ByteBuffer kb10, rb;
    pidKey(topic, part, kb10);
    char[32] fb = void;
    immutable fl = snprintf(fb.ptr, fb.length, "txn:%lld", pid);
    char[24] vb = void;
    immutable vl = snprintf(vb.ptr, vb.length, "%lld", firstOff);
    const(char)[][4] a = ["hsetnx", cast(const(char)[]) kb10.data,
        cast(const(char)[]) fb[0 .. (fl > 0 ? fl : 0)],
        cast(const(char)[]) vb[0 .. (vl > 0 ? vl : 0)]];
    gKafkaExec(a[], rb);
}

/// Last stable offset: the smallest first-offset among OPEN transactions on
/// the partition (fields `txn:<pid>` of the kafka.pid hash), else hw.
private long computeLso(scope const(char)[] topic, int part, long hw) nothrow @trusted
{
    if (gKafkaExec is null)
        return hw;
    static ByteBuffer kb11, rb;
    pidKey(topic, part, kb11);
    const(char)[][2] a = ["hgetall", cast(const(char)[]) kb11.data];
    gKafkaExec(a[], rb);
    auto d = cast(const(char)[]) rb.data;
    long lso = hw;
    if (d.length < 4 || d[0] != '*')
        return lso;
    size_t i2 = 1;
    long cnt = 0;
    while (i2 < d.length && d[i2] != '\r')
        cnt = cnt * 10 + (d[i2++] - '0');
    i2 += 2;
    for (long e = 0; e + 1 < cnt; e += 2)
    {
        // field bulk
        if (i2 >= d.length || d[i2] != '$')
            break;
        i2++;
        long fl = 0;
        while (i2 < d.length && d[i2] != '\r')
            fl = fl * 10 + (d[i2++] - '0');
        i2 += 2;
        if (i2 + fl + 2 > d.length)
            break;
        auto f = d[i2 .. i2 + cast(size_t) fl];
        i2 += cast(size_t) fl + 2;
        // value bulk
        if (i2 >= d.length || d[i2] != '$')
            break;
        i2++;
        long vl = 0;
        while (i2 < d.length && d[i2] != '\r')
            vl = vl * 10 + (d[i2++] - '0');
        i2 += 2;
        if (i2 + vl + 2 > d.length)
            break;
        auto v = d[i2 .. i2 + cast(size_t) vl];
        i2 += cast(size_t) vl + 2;
        if (f.length > 4 && f[0 .. 4] == "txn:")
        {
            long x = 0;
            foreach (c; v)
                if (c >= '0' && c <= '9')
                    x = x * 10 + (c - '0');
            if (x < lso)
                lso = x;
        }
    }
    return lso;
}

/// Aborted ranges for the partition into stack arrays; returns the count.
private size_t loadAborted(scope const(char)[] topic, int part,
        ref long[64] pids, ref long[64] offs) nothrow @trusted
{
    import core.stdc.stdio : snprintf;

    if (gKafkaExec is null)
        return 0;
    static ByteBuffer rb;
    char[12 + KAFKA_MAX_TOPIC + 16] xst = void;
    immutable xn = snprintf(xst.ptr, xst.length, "kafka.txa.%.*s.%d",
            cast(int) topic.length, topic.ptr, part);
    const(char)[][4] a = ["lrange", cast(const(char)[]) xst[0 .. (xn > 0 ? xn : 0)],
        "0", "-1"];
    gKafkaExec(a[], rb);
    auto d = cast(const(char)[]) rb.data;
    size_t n;
    if (d.length < 4 || d[0] != '*')
        return 0;
    size_t i2 = 1;
    long cnt = 0;
    while (i2 < d.length && d[i2] != '\r')
        cnt = cnt * 10 + (d[i2++] - '0');
    i2 += 2;
    foreach (_; 0 .. cnt)
    {
        if (n >= pids.length || i2 >= d.length || d[i2] != '$')
            break;
        i2++;
        long bl = 0;
        while (i2 < d.length && d[i2] != '\r')
            bl = bl * 10 + (d[i2++] - '0');
        i2 += 2;
        if (i2 + bl + 2 > d.length)
            break;
        auto v = d[i2 .. i2 + cast(size_t) bl];
        i2 += cast(size_t) bl + 2;
        long pv = 0, ov = 0;
        bool sec = false;
        foreach (c; v)
        {
            if (c == ':')
            {
                sec = true;
                continue;
            }
            if (c >= '0' && c <= '9')
            {
                if (sec)
                    ov = ov * 10 + (c - '0');
                else
                    pv = pv * 10 + (c - '0');
            }
        }
        pids[n] = pv;
        offs[n] = ov;
        n++;
    }
    return n;
}

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
    // producer provenance from the first record (the fetch assembler groups
    // runs by pid/txn, so one batch is homogeneous): read_committed clients
    // match batch.producerId against aborted_transactions
    long bpid = -1;
    bool btxn = false;
    if (blobs.length)
    {
        auto r0 = parseStoredRec(blobs[0]);
        if (r0.ok)
        {
            bpid = r0.pid;
            btxn = r0.txn;
        }
    }
    putI16(bodyB, btxn ? 0x0010 : 0); // attributes (isTransactional)
    putI32(bodyB, count > 0 ? count - 1 : 0); // lastOffsetDelta
    putI64(bodyB, firstTs);
    putI64(bodyB, maxTs);
    putI64(bodyB, bpid); // producerId
    putI16(bodyB, bpid >= 0 ? 0 : -1); // producerEpoch
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
    if (blob.length >= 1 && (blob[0] == KREC_TAG || blob[0] == KREC_TAG_PID))
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
