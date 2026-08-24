module dreads.aof;

// Append-only file, phase 1. Write commands are logged as their original RESP
// bytes (zero-copy from the connection buffer), staged in a malloc'd buffer,
// fwrite+fflush'd once per network batch and fsync'd at most once per second
// (Redis's "everysec" policy). On boot the file is replayed through the same
// parser and dispatch that serve sockets.

import core.stdc.stdio : FILE, fclose, fopen, fflush, fread, fwrite, fprintf, snprintf, stderr;

version (Posix)
{
    import core.sys.posix.stdio : fileno;
    import core.sys.posix.unistd : fsync;
}
version (Windows)
{
    import core.stdc.stdio : fileno; // druntime exposes fileno (not _fileno) here

    extern (C) int _commit(int fd) nothrow @nogc;

    private alias fsync = _commit;
}

import dreads.commands : dispatch;
import dreads.mem : Arena, ByteBuffer;
import dreads.obj : Keyspace, RObj, gDbs, NUM_DBS;

// Emit a `SELECT <db>` RESP command into `buf` — the multi-db framing marker used
// by both the live log (on a db change) and the rewrite (before each db's dump).
private void emitSelect(ref ByteBuffer buf, int db) @nogc nothrow
{
    import core.stdc.stdio : snprintf;

    char[12] nb = void;
    immutable n = snprintf(nb.ptr, nb.length, "%d", db);
    if (n <= 0)
        return;
    repArrayHeader(buf, 2);
    repBulk(buf, "SELECT");
    repBulk(buf, nb[0 .. n]);
}
import dreads.resp;
import dreads.scripting : evalCommand;

// ---------------------------------------------------------------------------
// AOF v2 — the RESP-IR format ("o AOF em RESP-IR"). The file opens with an
// 8-byte magic + the build's command-table hash; every entry is a framed
// record `[u32 len][u8 kind][u16 db]` where len counts the bytes AFTER the
// len field. kind 0 = RAW (one or more whole RESP commands — dumps, synthetic
// DELs, script effects; replay parses them exactly like the legacy stream,
// SELECT frames included). kind 1 = IR: `[u16 opcode][u16 argc]
// [(u32 off,u32 len) x argc][raw resp]` — the hop descriptor's layout, so
// replay rebuilds the RVal from offsets with NO RESP scan and dispatches by
// opcode with NO name resolution. Opcodes are only trusted when the file's
// command-table hash matches this build (indices shift when the table
// changes); on mismatch replay falls back to resolving arg0 — still
// parse-free. A pre-existing legacy file keeps its format until the next
// rewrite (sticky per file; a mid-file format flip would corrupt replay).
public enum ubyte[8] AOF_IR_MAGIC = cast(ubyte[8]) "#AOFIR2\n";

/// FNV-1a over the command table's names in order: opcode validity fingerprint.
public enum ulong aofCmdTableHash = ()
{
    import dreads.aclcat : gCmdCats;

    ulong h = 1469598103934665603UL;
    foreach (c; gCmdCats)
    {
        foreach (ch; c.name)
        {
            h ^= cast(ubyte) ch;
            h *= 1099511628211UL;
        }
        h ^= 0xFF; // name separator
        h *= 1099511628211UL;
    }
    return h;
}();

private void aofPutU32(ref ByteBuffer b, uint v) @nogc nothrow
{
    b.appendByte(cast(char)(v >> 24));
    b.appendByte(cast(char)(v >> 16));
    b.appendByte(cast(char)(v >> 8));
    b.appendByte(cast(char)(v & 0xFF));
}

private uint aofGetU32(scope const(ubyte)[] d, size_t i) @nogc nothrow
{
    return (cast(uint) d[i] << 24) | (cast(uint) d[i + 1] << 16)
        | (cast(uint) d[i + 2] << 8) | d[i + 3];
}

public struct Aof
{
    private FILE* f;
    private ByteBuffer pending;
    private bool dirty;
    private bool irMode; // v2 framing (fresh files + rewrites); legacy files stay raw
    // The db the log stream is currently positioned on (-1 = unknown, force a
    // SELECT on the next append). A write on a different db emits `SELECT <db>`
    // first, so replay routes each command to the right database.
    private int lastDb = -1;

    // Prepend a `SELECT` if the ambient command db (gNotifyDb) changed since the
    // last logged write. Called by every append path.
    private void maybeSelect() @nogc nothrow
    {
        import dreads.notify : gNotifyDb;

        if (gNotifyDb == lastDb)
            return;
        lastDb = gNotifyDb;
        emitSelect(pending, gNotifyDb);
    }

    @property bool enabled() const @nogc nothrow
    {
        return f !is null;
    }

    bool open(scope const(char)[] path) @nogc nothrow
    {
        char[512] zpath = void;
        if (path.length == 0 || path.length >= zpath.length)
            return false;
        zpath[0 .. path.length] = path;
        zpath[path.length] = 0;
        f = fopen(zpath.ptr, "ab");
        irMode = false;
        if (f !is null)
        {
            import core.stdc.stdio : ftell;

            immutable long sz = ftell(f); // "ab" opens positioned at EOF
            if (sz == 0)
            {
                // fresh file: v2 header (magic + this build's table hash)
                ubyte[16] hdr = void;
                hdr[0 .. 8] = AOF_IR_MAGIC[];
                foreach (k; 0 .. 8)
                    hdr[8 + k] = cast(ubyte)(aofCmdTableHash >> ((7 - k) * 8));
                if (fwrite(hdr.ptr, 1, 16, f) == 16)
                    irMode = true;
            }
            else
            {
                // existing file: the format is whatever it opens with (sticky)
                auto probe = fopen(zpath.ptr, "rb");
                if (probe !is null)
                {
                    ubyte[8] m = void;
                    if (fread(m.ptr, 1, 8, probe) == 8 && m == AOF_IR_MAGIC)
                        irMode = true;
                    fclose(probe);
                }
            }
        }
        if (f !is null)
        {
            // UNBUFFERED: the AOF already batches in its own `pending` buffer, so
            // stdio's buffer adds nothing AND hides where a write error surfaces
            // (a full-buffered stream defers the real write — and its ENOSPC — to
            // fflush, past flush()'s short-write check). Unbuffered makes fwrite a
            // direct write(): its return reflects exactly what reached the fd, so
            // the partial-write consume(wrote) path is actually correct.
            import core.stdc.stdio : setvbuf, _IONBF;

            cast(void) setvbuf(f, null, _IONBF, 0);
        }
        lastDb = -1; // fresh handle: the first append re-emits its SELECT
        return f !is null;
    }

    void close() @nogc nothrow
    {
        if (f is null)
            return;
        flush();
        fsyncNow();
        fclose(f);
        f = null;
    }

    /// Stages one command's raw RESP bytes; cheap, no I/O. Under the v2
    /// format the bytes travel as ONE RAW record stamped with the ambient db.
    void append(scope const(ubyte)[] bytes) @nogc nothrow
    {
        if (f is null)
            return;
        if (!irMode)
        {
            maybeSelect();
            pending.append(bytes);
            return;
        }
        import dreads.notify : gNotifyDb;

        aofPutU32(pending, cast(uint)(3 + bytes.length));
        pending.appendByte(0); // kind RAW
        immutable ushort db = cast(ushort)(gNotifyDb < 0 ? 0 : gNotifyDb);
        pending.appendByte(cast(char)(db >> 8));
        pending.appendByte(cast(char)(db & 0xFF));
        pending.append(bytes);
    }

    /// Stages one command as an IR record: opcode + per-arg offsets + the raw
    /// RESP — replay rebuilds the args with no scan and dispatches by opcode.
    /// Falls back to append() when the file is legacy or the shape is odd.
    void appendIR(const ref RVal cmd, int opcode, scope const(ubyte)[] raw) @nogc nothrow
    {
        if (f is null)
            return;
        if (!irMode || opcode < 0 || cmd.type != RType.Array
                || cmd.arr.length == 0 || cmd.arr.length > 0xFFFF)
        {
            append(raw);
            return;
        }
        // every arg must be a slice INTO raw (the parse is zero-copy, so this
        // holds for wire commands; synthesized ones go through append())
        auto base = cast(const(char)*) raw.ptr;
        foreach (ref a; cmd.arr)
        {
            if (a.type != RType.BulkString || a.str.ptr < base
                    || a.str.ptr + a.str.length > base + raw.length)
            {
                append(raw);
                return;
            }
        }
        import dreads.notify : gNotifyDb;

        immutable uint plen = cast(uint)(3 + 2 + 2 + cmd.arr.length * 8 + raw.length);
        aofPutU32(pending, plen);
        pending.appendByte(1); // kind IR
        immutable ushort db = cast(ushort)(gNotifyDb < 0 ? 0 : gNotifyDb);
        pending.appendByte(cast(char)(db >> 8));
        pending.appendByte(cast(char)(db & 0xFF));
        pending.appendByte(cast(char)(opcode >> 8));
        pending.appendByte(cast(char)(opcode & 0xFF));
        immutable ushort argc = cast(ushort) cmd.arr.length;
        pending.appendByte(cast(char)(argc >> 8));
        pending.appendByte(cast(char)(argc & 0xFF));
        foreach (ref a; cmd.arr)
        {
            aofPutU32(pending, cast(uint)(a.str.ptr - base));
            aofPutU32(pending, cast(uint) a.str.length);
        }
        pending.append(raw);
    }

    /// Re-encodes EVALSHA as EVAL so replay does not depend on the script cache.
    /// rest = [numkeys, keys..., argv...].
    void appendEval(scope const(char)[] body_, const(RVal)[] rest) @nogc nothrow
    {
        if (f is null)
            return;
        // built in a scratch so the v2 path can frame it as one RAW record
        static ByteBuffer eb; // TLS: consumed synchronously below
        eb.clear();
        repArrayHeader(eb, rest.length + 2);
        repBulk(eb, "EVAL");
        repBulk(eb, body_);
        foreach (ref a; rest)
            repBulk(eb, a.str);
        append(cast(const(ubyte)[]) eb.data);
    }

    /// Hands staged bytes to the OS (survives a process crash).
    void flush() @nogc nothrow
    {
        if (f is null || pending.empty)
            return;
        // A short fwrite (ENOSPC / IO error) must NOT clear pending: dropping
        // the un-written bytes silently loses data AND leaves a partial record
        // in the file that breaks replay mid-stream. Keep pending and retry on
        // the next flush; the bytes already written are a whole-record prefix
        // (append() only ever adds complete commands), so no torn record ships.
        immutable wrote = fwrite(pending.data.ptr, 1, pending.length, f);
        if (wrote != pending.length)
        {
            // partial write: `wrote` bytes reached the file, so DROP that prefix
            // and keep the tail for the next flush (retaining the whole buffer
            // would re-write the prefix = duplicate/corrupt; clearing it would
            // lose the tail). append() only adds whole commands, so the file
            // still ends on a record boundary or mid-record that the tail
            // completes on retry.
            pending.consume(wrote);
            dirty = true;
            return;
        }
        fflush(f);
        pending.clear();
        dirty = true;
    }

    /// Called by the 1s timer; the only place paying fsync latency.
    void fsyncNow() @nogc nothrow
    {
        import core.stdc.time : time;

        if (f is null || !dirty)
            return;
        fsync(fileno(f));
        dirty = false;
        lastFsyncUnix = time(null);
    }

    long lastFsyncUnix; // LASTSAVE
}

/// Serializes the whole keyspace as the minimal canonical command set that
/// rebuilds it (SET/RPUSH/HSET/ZADD/XADD..., PEXPIREAT for live TTLs, expired
/// keys skipped). This IS the compaction: dead history (SET-then-DEL,
/// overwrites, expired) collapses into current state. Shared by BGREWRITEAOF
/// and the Raft snapshot.
/// Dump EVERY non-empty database as a SELECT-framed rebuild command stream — the
/// shared serialization for the AOF rewrite and the raft snapshot. Replay
/// (aofLoad / loadSnapshot) restores each key to the db named by its SELECT.
public void dumpAllKeyspaces(ref ByteBuffer buf) nothrow
{
    dumpKeyspaces(buf, gDbs[]);
}

/// Slice-aware dump (AOF-per-shard, phase 2.6): a shard's rewrite serialises
/// ONLY its own 16-db partition. SELECT frames come from each keyspace's own
/// `db` field, so the slice works for gDbs and for a gShardKs partition alike.
public void dumpKeyspaces(ref ByteBuffer buf, Keyspace[] dbs) nothrow
{
    foreach (ref d; dbs)
    {
        if (d.d.length == 0)
            continue;
        emitSelect(buf, d.db);
        dumpKeyspace(d, buf);
    }
}

public void dumpKeyspace(ref Keyspace ks, ref ByteBuffer buf) nothrow
{
    import dreads.stream : nowMs;

    auto now = nowMs();
    foreach (i; 0 .. ks.d.capacity)
    {
        if (!ks.d.slotLive(i))
            continue;
        auto obj = ks.d.valAt(i);
        if (obj.expireAtMs != 0 && now >= obj.expireAtMs)
            continue; // expired keys stay dead
        dumpKey(buf, ks.d.keyAt(i), obj, false);
    }
}

/// Emit the canonical rebuild commands for ONE key's value. `valueOnly` (DUMP
/// payload) omits the key-level PEXPIREAT — RESTORE carries the TTL as an
/// argument; otherwise a trailing PEXPIREAT preserves it (AOF rewrite / raft
/// snapshot). Hash field TTLs (HEXPIRE) are always re-emitted as HPEXPIREAT, so
/// they survive compaction and translate into the RDB hash-with-expiry form.
public void dumpKey(ref ByteBuffer buf, scope const(char)[] key, RObj* obj, bool valueOnly) @nogc nothrow
{
    import dreads.commands : fmtDouble;
    import dreads.obj : ObjType;
    import dreads.stream : StreamID;

    enum CHUNK = 128;
        final switch (obj.type)
        {
        case ObjType.str:
            char[24] sb = void;
            repArrayHeader(buf, 3);
            repBulk(buf, "SET");
            repBulk(buf, key);
            repBulk(buf, obj.str.bytes(sb)); // int-encoded values dump as their digits
            break;
        case ObjType.list:
            {
                size_t emitted = 0;
                auto total = obj.list.length;
                while (emitted < total)
                {
                    auto n = total - emitted > CHUNK ? CHUNK : total - emitted;
                    repArrayHeader(buf, n + 2);
                    repBulk(buf, "RPUSH");
                    repBulk(buf, key);
                    obj.list.walkRange(cast(long) emitted, n, (v) {
                        repBulk(buf, v);
                        return 0;
                    });
                    emitted += n;
                }
                break;
            }
        case ObjType.hash:
            emitDictChunks(buf, "HSET", key, obj, true);
            emitHashFieldTTLs(buf, key, obj);
            break;
        case ObjType.set:
            emitDictChunks(buf, "SADD", key, obj, false);
            break;
        case ObjType.zset:
            {
                size_t emitted = 0;
                auto total = obj.zset.length;
                while (emitted < total)
                {
                    auto n = total - emitted > CHUNK ? CHUNK : total - emitted;
                    repArrayHeader(buf, 2 + n * 2);
                    repBulk(buf, "ZADD");
                    repBulk(buf, key);
                    obj.zset.walkRange(emitted, n, false, (m, s) {
                        char[40] fb = void;
                        repBulk(buf, fmtDouble(fb, s));
                        repBulk(buf, m);
                        return 0;
                    });
                    emitted += n;
                }
                break;
            }
        case ObjType.stream:
            {
                obj.stream.walkRange(StreamID.minId, StreamID.maxId, 0, (id, pairs) {
                    repArrayHeader(buf, 3 + pairs.length * 2);
                    repBulk(buf, "XADD");
                    repBulk(buf, key);
                    char[48] ib = void;
                    auto ilen = snprintf(ib.ptr, ib.length, "%llu-%llu", id.ms, id.seq);
                    repBulk(buf, ib[0 .. ilen]);
                    foreach (ref p; pairs)
                    {
                        repBulk(buf, p.field);
                        repBulk(buf, p.value);
                    }
                    return 0;
                });
                char[48] lb = void;
                auto llen = snprintf(lb.ptr, lb.length, "%llu-%llu",
                        obj.stream.lastId.ms, obj.stream.lastId.seq);
                if (obj.stream.length == 0
                        && (obj.stream.lastId.ms != 0 || obj.stream.lastId.seq != 0))
                {
                    // empty stream with history: materialize then delete
                    repArrayHeader(buf, 5);
                    repBulk(buf, "XADD");
                    repBulk(buf, key);
                    repBulk(buf, lb[0 .. llen]);
                    repBulk(buf, "f");
                    repBulk(buf, "v");
                    repArrayHeader(buf, 3);
                    repBulk(buf, "XDEL");
                    repBulk(buf, key);
                    repBulk(buf, lb[0 .. llen]);
                }
                // groups (their PEL is volatile and not persisted — DRIFT)
                bool first = true;
                foreach (gi; 0 .. obj.stream.groups.capacity)
                {
                    if (!obj.stream.groups.slotLive(gi))
                        continue;
                    auto g = obj.stream.groups.valAt(gi);
                    char[48] gb = void;
                    auto glen = snprintf(gb.ptr, gb.length, "%llu-%llu",
                            g.lastDelivered.ms, g.lastDelivered.seq);
                    repArrayHeader(buf, first
                            && obj.stream.length == 0 && obj.stream.lastId.ms == 0
                            && obj.stream.lastId.seq == 0 ? 6 : 5);
                    repBulk(buf, "XGROUP");
                    repBulk(buf, "CREATE");
                    repBulk(buf, key);
                    repBulk(buf, obj.stream.groups.keyAt(gi));
                    repBulk(buf, gb[0 .. glen]);
                    if (first && obj.stream.length == 0 && obj.stream.lastId.ms == 0
                            && obj.stream.lastId.seq == 0)
                        repBulk(buf, "MKSTREAM");
                    first = false;
                }
                if (obj.stream.length != 0 || obj.stream.lastId.ms != 0
                        || obj.stream.lastId.seq != 0)
                {
                    repArrayHeader(buf, 3);
                    repBulk(buf, "XSETID");
                    repBulk(buf, key);
                    repBulk(buf, lb[0 .. llen]);
                }
                break;
            }
        }
    if (!valueOnly && obj.expireAtMs != 0)
    {
        repArrayHeader(buf, 3);
        repBulk(buf, "PEXPIREAT");
        repBulk(buf, key);
        char[24] eb = void;
        auto elen = snprintf(eb.ptr, eb.length, "%llu", obj.expireAtMs);
        repBulk(buf, eb[0 .. elen]);
    }
}

/// Re-emit a hash's field TTLs (HEXPIRE) as one HPEXPIREAT per TTL'd field, so
/// they survive AOF rewrite / raft snapshot and carry into the RDB translation.
private void emitHashFieldTTLs(ref ByteBuffer buf, scope const(char)[] key, RObj* obj) @nogc nothrow
{
    if (!obj.hash.hasFieldTTL)
        return;
    foreach (slot; 0 .. obj.hash.capacity)
    {
        if (!obj.hash.slotLive(slot))
            continue;
        auto field = obj.hash.keyAt(slot);
        immutable ttl = obj.hash.getFieldTTL(field);
        if (ttl == 0)
            continue;
        repArrayHeader(buf, 6);
        repBulk(buf, "HPEXPIREAT");
        repBulk(buf, key);
        char[24] tb = void;
        repBulk(buf, tb[0 .. snprintf(tb.ptr, tb.length, "%llu", ttl)]);
        repBulk(buf, "FIELDS");
        repBulk(buf, "1");
        repBulk(buf, field);
    }
}

/// BGREWRITEAOF: rewrites the log as the canonical rebuild command set.
/// Runs synchronously — the event loop is single-threaded, so the keyspace
/// cannot change under us. Reopens the live handle on success.
public bool aofRewrite(ref Aof live, scope const(char)[] path,
        Keyspace[] dbs = null, bool emitGlobals = true) nothrow
{
    import core.stdc.stdio : rename;

    char[512] zpath = void;
    char[520] ztmp = void;
    if (path.length == 0 || path.length >= zpath.length)
        return false;
    zpath[0 .. path.length] = path;
    zpath[path.length] = 0;
    ztmp[0 .. path.length] = path;
    ztmp[path.length .. path.length + 9] = ".rewrite\0";

    auto f = fopen(ztmp.ptr, "wb");
    if (f is null)
        return false;

    ByteBuffer buf;
    if (dbs is null)
        dumpAllKeyspaces(buf); // every non-empty db, SELECT-framed
    else
        dumpKeyspaces(buf, dbs); // this shard's partition only (AOF-per-shard)
    // ACL registry is global (not per-db): re-emit users so the compacted AOF
    // still recreates them on replay. Under AOF-per-shard only shard 0's file
    // carries them (a per-shard copy would re-apply N times on boot).
    if (emitGlobals)
    {
        import dreads.acl : aclDumpUsers;

        aclDumpUsers(buf);
    }
    // the rewrite is the format-upgrade point: always v2 — header + ONE RAW
    // record wrapping the SELECT-framed dump (it already sits fully in buf)
    bool ioOk;
    {
        ubyte[16] hdr = void;
        hdr[0 .. 8] = AOF_IR_MAGIC[];
        foreach (k; 0 .. 8)
            hdr[8 + k] = cast(ubyte)(aofCmdTableHash >> ((7 - k) * 8));
        ioOk = fwrite(hdr.ptr, 1, 16, f) == 16;
        if (ioOk && !buf.empty)
        {
            if (buf.length > uint.max - 3)
                ioOk = false; // cannot frame a >4GB dump in one record
            else
            {
                immutable uint plen = cast(uint)(3 + buf.length);
                ubyte[7] rh = [cast(ubyte)(plen >> 24), cast(ubyte)(plen >> 16),
                    cast(ubyte)(plen >> 8), cast(ubyte)(plen & 0xFF),
                    0 /* kind RAW */ , 0, 0 /* db 0; inner SELECTs route */ ];
                ioOk = fwrite(rh.ptr, 1, 7, f) == 7
                    && fwrite(buf.data.ptr, 1, buf.length, f) == buf.length;
            }
        }
    }
    fflush(f);
    version (Posix)
        fsync(fileno(f));
    fclose(f);
    if (!ioOk)
        return false;
    live.close();
    if (rename(ztmp.ptr, zpath.ptr) != 0)
        return false;
    return live.open(path);
}

/// One SADD/HSET per chunk of a dict-backed container.
private void emitDictChunks(ref ByteBuffer buf, scope const(char)[] verb,
        scope const(char)[] key, RObj* obj, bool withValues) @nogc nothrow
{
    enum CHUNK = 128;
    // count live entries
    size_t total = withValues ? obj.hash.length : obj.set.length;
    size_t emitted = 0;
    size_t slot = 0;
    while (emitted < total)
    {
        auto n = total - emitted > CHUNK ? CHUNK : total - emitted;
        repArrayHeader(buf, 2 + n * (withValues ? 2 : 1));
        repBulk(buf, verb);
        repBulk(buf, key);
        size_t inChunk = 0;
        while (inChunk < n)
        {
            bool live = withValues ? obj.hash.slotLive(slot) : obj.set.slotLive(slot);
            if (live)
            {
                if (withValues)
                {
                    char[24] sb = void;
                    repBulk(buf, obj.hash.keyAt(slot));
                    repBulk(buf, obj.hash.valAt(slot).bytes(sb));
                }
                else
                    repBulk(buf, obj.set.keyAt(slot));
                inChunk++;
            }
            slot++;
        }
        emitted += n;
    }
}

/// The server's logging policy, factored out so recovery tests exercise the
/// real thing: after dispatch, log the propagation override when a handler
/// set one, else the raw command when it is a write; errors are never logged.
/// Consumes (clears) the override either way.
public void logAfterDispatch(ref Aof aof, scope const(ubyte)[] rawCmd,
        scope const(char)[] unameUpper, scope const(ubyte)[] reply) nothrow
{
    import dreads.commands : isWriteCommand, propagationOverride;

    if (aof.enabled && reply.length > 0 && reply[0] != '-')
    {
        if (!propagationOverride.empty)
            aof.append(propagationOverride.data);
        else if (isWriteCommand(unameUpper))
            aof.append(rawCmd);
    }
    propagationOverride.clear();
}

/// Replays one logged command. EVAL goes to the scripting engine; everything
/// else through the regular dispatch.
private void replayCommand(const ref RVal cmd, ref Keyspace ks, ref ByteBuffer sink,
        ref Arena arena, int knownIdx = -1) nothrow
{
    if (cmd.type == RType.Array && cmd.arr.length > 0 && cmd.arr[0].type == RType.BulkString)
    {
        auto name = cmd.arr[0].str;
        if (name.length == 4)
        {
            char[4] up = void;
            foreach (i, c; name)
                up[i] = c >= 'a' && c <= 'z' ? cast(char)(c - 32) : c;
            if (up == "EVAL")
            {
                evalCommand(cmd.arr[1 .. $], ks, sink, arena, false);
                return;
            }
        }
    }
    dispatch(cmd, ks, sink, arena, 0, knownIdx);
}

/// Recognise a replay-stream `SELECT <n>` (0 <= n < NUM_DBS) and return its db.
/// Shared by AOF replay and the raft snapshot loader.
public bool aofIsSelect(ref const RVal cmd, out int db) @nogc nothrow
{
    if (cmd.type != RType.Array || cmd.arr.length != 2 || cmd.arr[0].str.length != 6)
        return false;
    static immutable sel = "select";
    foreach (i, ch; cmd.arr[0].str)
        if ((ch | 0x20) != sel[i])
            return false;
    long v = 0;
    auto s = cmd.arr[1].str;
    if (s.length == 0 || s.length > 2)
        return false;
    foreach (ch; s)
    {
        if (ch < '0' || ch > '9')
            return false;
        v = v * 10 + (ch - '0');
    }
    if (v < 0 || v >= NUM_DBS)
        return false;
    db = cast(int) v;
    return true;
}

// ---------------------------------------------------------------------------
// v2 replay plumbing (shared by aofLoad and aofLoadSharded)
// ---------------------------------------------------------------------------

/// Streams v2 records off an open file. Payload slices stay valid until the
/// next next() call. A partial trailing record sets truncated (tolerated, like
/// the legacy truncated-tail case); an impossible frame sets corrupt.
private struct AofRecReader
{
    FILE* f;
    ByteBuffer inb;
    size_t pos;
    bool truncated;
    bool corrupt;

    private bool fill(size_t need) nothrow
    {
        if (inb.length - pos >= need)
            return true;
        // compact ONCE per refill — a per-record consume() would memmove the
        // whole buffered tail for every ~80-byte record (quadratic replay)
        if (pos > 0)
        {
            inb.consume(pos);
            pos = 0;
        }
        while (inb.length < need)
        {
            auto space = inb.freeSpace(64 * 1024);
            auto n = fread(space.ptr, 1, space.length, f);
            if (n == 0)
                return false;
            inb.grow(n);
        }
        return true;
    }

    bool next(out ubyte kind, out ushort db, out const(ubyte)[] payload) nothrow
    {
        if (!fill(4))
        {
            truncated = inb.length - pos > 0;
            return false;
        }
        auto d = cast(const(ubyte)[]) inb.data;
        immutable uint len = aofGetU32(d, pos);
        if (len < 3)
        {
            corrupt = true;
            return false;
        }
        if (!fill(4 + cast(size_t) len))
        {
            truncated = true;
            return false;
        }
        d = cast(const(ubyte)[]) inb.data; // fill() may have reallocated
        kind = d[pos + 4];
        db = cast(ushort)((d[pos + 5] << 8) | d[pos + 6]);
        payload = d[pos + 7 .. pos + 4 + len];
        pos += 4 + len;
        return true;
    }
}

/// Sniffs the 16-byte v2 header off a just-opened file. On a legacy file the
/// consumed bytes are pushed into `spill` so the legacy parse loop starts from
/// byte 0. trustOps = the stored command-table hash matches this build, so IR
/// opcodes index gCmdCats directly; otherwise replay re-resolves from arg0.
private bool aofSniffV2(FILE* f, ref ByteBuffer spill, out bool trustOps) nothrow
{
    ubyte[16] hdr = void;
    auto hn = fread(hdr.ptr, 1, 16, f);
    if (hn >= 8 && hdr[0 .. 8] == AOF_IR_MAGIC)
    {
        ulong h = 0;
        if (hn == 16)
            foreach (k; 0 .. 8)
                h = (h << 8) | hdr[8 + k];
        trustOps = hn == 16 && h == aofCmdTableHash;
        return true;
    }
    trustOps = false;
    spill.append(hdr[0 .. hn]);
    return false;
}

/// Decodes one IR record payload: `[u16 opcode][u16 argc][(u32,u32) x argc]
/// [raw resp]` into an arena-backed BulkString array. opcode comes out -1
/// unless trustOps (indices are only meaningful under a matching table hash).
private bool aofDecodeIR(const(ubyte)[] payload, ref Arena arena, bool trustOps,
        out RVal cmd, out int opcode) nothrow
{
    import dreads.aclcat : gCmdCats;

    opcode = -1;
    if (payload.length < 4)
        return false;
    immutable int op = (payload[0] << 8) | payload[1];
    immutable size_t argc = (payload[2] << 8) | payload[3];
    immutable size_t tbl = 4 + argc * 8;
    if (argc == 0 || payload.length < tbl)
        return false;
    auto raw = payload[tbl .. $];
    auto items = arena.allocArray!RVal(argc);
    foreach (i; 0 .. argc)
    {
        immutable size_t off = aofGetU32(payload, 4 + i * 8);
        immutable size_t len = aofGetU32(payload, 4 + i * 8 + 4);
        if (off > raw.length || len > raw.length - off)
            return false;
        items[i].type = RType.BulkString;
        items[i].str = cast(const(char)[]) raw[off .. off + len];
    }
    cmd.type = RType.Array;
    cmd.arr = items;
    if (trustOps && op >= 0 && op < gCmdCats.length)
        opcode = op;
    return true;
}

/// Loads an AOF into ks (the db-0 keyspace). A `SELECT <n>` in the stream routes
/// subsequent commands into `gDbs[n]`. Returns the number of commands replayed,
/// or -1 when the file exists but is unreadable. A truncated tail is tolerated.
public long aofLoad(scope const(char)[] path, ref Keyspace ks) nothrow
{
    char[512] zpath = void;
    if (path.length == 0 || path.length >= zpath.length)
        return -1;
    zpath[0 .. path.length] = path;
    zpath[path.length] = 0;
    auto f = fopen(zpath.ptr, "rb");
    if (f is null)
        return 0; // nothing to replay yet

    ByteBuffer inb;
    ByteBuffer sink;
    Arena arena;
    long count = 0;
    bool corrupt = false;
    Keyspace* curKs = &ks; // a `SELECT <n>` re-points this into gDbs

    bool trustOps;
    if (aofSniffV2(f, inb, trustOps))
    {
        AofRecReader rd;
        rd.f = f;
        ubyte kind;
        ushort rdb;
        const(ubyte)[] payload;
        while (rd.next(kind, rdb, payload))
        {
            if (rdb >= NUM_DBS)
            {
                corrupt = true;
                break;
            }
            auto recKs = rdb == 0 ? &ks : &gDbs[rdb];
            if (kind == 1)
            {
                RVal cmd;
                int op;
                if (!aofDecodeIR(payload, arena, trustOps, cmd, op))
                {
                    corrupt = true;
                    break;
                }
                replayCommand(cmd, *recKs, sink, arena, op);
                sink.clear();
                arena.reset();
                {
                    import dreads.commands : propagationOverride;

                    propagationOverride.clear();
                }
                count++;
            }
            else if (kind == 0)
            {
                // whole RESP commands; inner SELECT frames re-route (dumps)
                size_t pp = 0;
                auto cur = recKs;
                for (;;)
                {
                    RVal cmd;
                    auto st = parseValue(payload, pp, arena, cmd);
                    if (st == ParseStatus.incomplete)
                    {
                        if (pp != payload.length)
                            corrupt = true; // a delivered record holds whole commands
                        break;
                    }
                    if (st == ParseStatus.protocolError)
                    {
                        corrupt = true;
                        break;
                    }
                    int selDb;
                    if (aofIsSelect(cmd, selDb))
                    {
                        cur = selDb == 0 ? &ks : &gDbs[selDb];
                        continue;
                    }
                    replayCommand(cmd, *cur, sink, arena);
                    sink.clear();
                    {
                        import dreads.commands : propagationOverride;

                        propagationOverride.clear();
                    }
                    count++;
                }
                arena.reset();
                if (corrupt)
                    break;
            }
            else
            {
                corrupt = true; // unknown record kind
                break;
            }
        }
        if (rd.corrupt)
            corrupt = true;
        fclose(f);
        if (corrupt)
            fprintf(stderr, "dreads: AOF corrupt after %lld commands; stopped replay\n", count);
        else if (rd.truncated)
            fprintf(stderr, "dreads: AOF has a truncated trailing record (ignored)\n");
        return count;
    }

    for (;;)
    {
        auto space = inb.freeSpace(64 * 1024);
        auto n = fread(space.ptr, 1, space.length, f);
        inb.grow(n); // n == 0 still parses what the v2 sniff spilled into inb

        size_t pos = 0;
        for (;;)
        {
            RVal cmd;
            auto st = parseValue(inb.data, pos, arena, cmd);
            if (st == ParseStatus.incomplete)
                break;
            if (st == ParseStatus.protocolError)
            {
                corrupt = true;
                break;
            }
            int selDb;
            if (aofIsSelect(cmd, selDb))
            {
                // db 0 is the passed-in keyspace itself (which IS db 0 — for a
                // standalone unit-test ks it is NOT gDbs[0]); higher dbs are gDbs.
                curKs = selDb == 0 ? &ks : &gDbs[selDb];
                continue; // a SELECT marker is routing, not a replayed command
            }
            replayCommand(cmd, *curKs, sink, arena);
            sink.clear();
            arena.reset();
            // replayed handlers (PEXPIREAT, XADD, ...) write the propagation
            // override; a stale one must never leak into post-boot logging
            {
                import dreads.commands : propagationOverride;

                propagationOverride.clear();
            }
            count++;
        }
        inb.consume(pos);
        if (corrupt || n == 0)
            break;
    }
    fclose(f);

    if (corrupt)
        fprintf(stderr, "dreads: AOF corrupt after %lld commands; stopped replay\n", count);
    else if (!inb.empty)
        fprintf(stderr, "dreads: AOF has a truncated trailing command (%zu bytes ignored)\n",
                inb.length);
    return count;
}

/// AOF-per-shard replay (phase 2.6): load `path` into the 16-db slice `dbs`,
/// applying ONLY the commands owned by `ownerShard` under `shardCount` shards
/// (first-key slot routing — the SAME router the live path uses, so a file
/// written under a different shard count re-shards correctly on boot).
/// Keyless commands (ACL SETUSER, EVAL with no keys) apply only on shard 0;
/// keyless WRITES that act on a whole database (FLUSHDB/FLUSHALL) apply on
/// every shard's pass (each clears its own slice). SELECT frames re-point
/// inside `dbs`. Returns commands APPLIED on this pass (skipped ones don't
/// count), or -1 when the file exists but is unreadable.
///
/// ALLOCATOR CONTRACT: the caller must set gAllocShard = ownerShard around this
/// call (boot, single-threaded) — every value stored into dbs must come from
/// that shard's allocator, or the owning shard thread later frees a foreign
/// block (the cross-allocator SIGSEGV class). The scratch buffers here are
/// created and destroyed inside the call, under the same slot.
/// First-key ownership routing shared by the legacy and v2 sharded replays.
private bool aofRouteApplies(const ref RVal cmd, int ci, scope const(char)[] lname,
        uint ownerShard, bool applyGlobals) nothrow
{
    import dreads.acl : commandRouteKeyIx;
    import dreads.slots : keyToSlot;
    import dreads.shard : shardOfSlot;
    import dreads.aclcat : cmdIx;

    auto k = commandRouteKeyIx(ci, cast(string) lname, cmd.arr);
    if (k !is null)
        return shardOfSlot(keyToSlot(k)) == ownerShard;
    if (ci == cmdIx!"flushdb" || ci == cmdIx!"flushall")
        return true; // db-wide: every shard clears its own slice
    return applyGlobals;
}

public long aofLoadSharded(scope const(char)[] path, Keyspace[] dbs,
        uint ownerShard, uint shardCount, bool applyGlobals = true) nothrow
{
    import dreads.acl : aclCmdIndex, commandRouteKeyIx;
    import dreads.slots : keyToSlot;
    import dreads.shard : shardOfSlot;
    import dreads.aclcat : cmdIx;

    char[512] zpath = void;
    if (path.length == 0 || path.length >= zpath.length)
        return -1;
    zpath[0 .. path.length] = path;
    zpath[path.length] = 0;
    auto f = fopen(zpath.ptr, "rb");
    if (f is null)
        return 0; // nothing to replay

    ByteBuffer inb;
    ByteBuffer sink;
    Arena arena;
    long count = 0;
    bool corrupt = false;
    size_t curDb = 0;

    bool trustOps;
    if (aofSniffV2(f, inb, trustOps))
    {
        import dreads.aclcat : gCmdCats;

        AofRecReader rd;
        rd.f = f;
        ubyte kind;
        ushort rdb;
        const(ubyte)[] payload;

        // one command: route by first key, then replay if it lands here
        void routeReplay(const ref RVal cmd, int op, size_t db) nothrow
        {
            bool apply = true;
            int ki = op;
            if (shardCount > 1 && cmd.type == RType.Array && cmd.arr.length > 0)
            {
                if (op >= 0)
                    apply = aofRouteApplies(cmd, op, gCmdCats[op].name,
                            ownerShard, applyGlobals);
                else
                {
                    auto name = cmd.arr[0].str;
                    char[16] lb = void;
                    if (name.length <= lb.length)
                    {
                        foreach (i, ch; name)
                            lb[i] = ch >= 'A' && ch <= 'Z' ? cast(char)(ch + 32) : ch;
                        auto lname = cast(const(char)[]) lb[0 .. name.length];
                        immutable ci = aclCmdIndex(lname);
                        if (ci >= 0)
                            ki = ci; // resolved once — replay stays integer-dispatched
                        apply = aofRouteApplies(cmd, ci, lname, ownerShard, applyGlobals);
                    }
                    else
                        apply = applyGlobals;
                }
            }
            if (apply && db < dbs.length)
            {
                replayCommand(cmd, dbs[db], sink, arena, ki);
                sink.clear();
                count++;
            }
            {
                import dreads.commands : propagationOverride;

                propagationOverride.clear();
            }
        }

        while (rd.next(kind, rdb, payload))
        {
            if (kind == 1)
            {
                RVal cmd;
                int op;
                if (!aofDecodeIR(payload, arena, trustOps, cmd, op))
                {
                    corrupt = true;
                    break;
                }
                routeReplay(cmd, op, rdb);
                arena.reset();
            }
            else if (kind == 0)
            {
                size_t pp = 0;
                size_t cur = rdb;
                for (;;)
                {
                    RVal cmd;
                    auto st = parseValue(payload, pp, arena, cmd);
                    if (st == ParseStatus.incomplete)
                    {
                        if (pp != payload.length)
                            corrupt = true;
                        break;
                    }
                    if (st == ParseStatus.protocolError)
                    {
                        corrupt = true;
                        break;
                    }
                    int selDb;
                    if (aofIsSelect(cmd, selDb))
                    {
                        cur = cast(size_t) selDb;
                        continue;
                    }
                    routeReplay(cmd, -1, cur);
                }
                arena.reset();
                if (corrupt)
                    break;
            }
            else
            {
                corrupt = true;
                break;
            }
        }
        if (rd.corrupt)
            corrupt = true;
        fclose(f);
        if (corrupt)
            fprintf(stderr, "dreads: AOF %s corrupt after %lld commands; stopped replay\n",
                    zpath.ptr, count);
        return count;
    }

    for (;;)
    {
        auto space = inb.freeSpace(64 * 1024);
        auto n = fread(space.ptr, 1, space.length, f);
        inb.grow(n); // n == 0 still parses what the v2 sniff spilled into inb

        size_t pos = 0;
        for (;;)
        {
            RVal cmd;
            auto st = parseValue(inb.data, pos, arena, cmd);
            if (st == ParseStatus.incomplete)
                break;
            if (st == ParseStatus.protocolError)
            {
                corrupt = true;
                break;
            }
            int selDb;
            if (aofIsSelect(cmd, selDb))
            {
                curDb = cast(size_t) selDb;
                continue;
            }
            // route: does this command belong to ownerShard?
            bool apply = true;
            if (shardCount > 1 && cmd.type == RType.Array && cmd.arr.length > 0)
            {
                auto name = cmd.arr[0].str;
                char[16] lb = void;
                if (name.length <= lb.length)
                {
                    foreach (i, ch; name)
                        lb[i] = ch >= 'A' && ch <= 'Z' ? cast(char)(ch + 32) : ch;
                    auto lname = cast(const(char)[]) lb[0 .. name.length];
                    immutable ci = aclCmdIndex(lname);
                    auto k = commandRouteKeyIx(ci, cast(string) lname, cmd.arr);
                    if (k !is null)
                        apply = shardOfSlot(keyToSlot(k)) == ownerShard;
                    else if (ci == cmdIx!"flushdb" || ci == cmdIx!"flushall")
                        apply = true; // db-wide: every shard clears its own slice
                    else
                        apply = applyGlobals; // globals (ACL, keyless EVAL): see caller
                }
                else
                    apply = applyGlobals;
            }
            if (apply && curDb < dbs.length)
            {
                replayCommand(cmd, dbs[curDb], sink, arena);
                sink.clear();
                count++;
            }
            arena.reset();
            {
                import dreads.commands : propagationOverride;

                propagationOverride.clear();
            }
        }
        inb.consume(pos);
        if (corrupt || n == 0)
            break;
    }
    fclose(f);
    if (corrupt)
        fprintf(stderr, "dreads: AOF %s corrupt after %lld commands; stopped replay\n",
                zpath.ptr, count);
    return count;
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

version (unittest)
{
    import core.stdc.stdio : remove;

    private string respCmd(string[] args...)
    {
        import std.conv : to;

        string r = "*" ~ args.length.to!string ~ "\r\n";
        foreach (a; args)
            r ~= "$" ~ a.length.to!string ~ "\r\n" ~ a ~ "\r\n";
        return r;
    }

    private string runOne(ref Keyspace ks, string[] args...)
    {
        Arena arena;
        ByteBuffer o;
        RVal v;
        size_t pos = 0;
        auto encoded = respCmd(args);
        assert(parseValue(cast(const(ubyte)[]) encoded, pos, arena, v) == ParseStatus.ok);
        dispatch(v, ks, o, arena);
        return (cast(string) o.data).idup;
    }

    private void rmPath(string path)
    {
        remove((path ~ "\0").ptr);
    }
}

unittest // roundtrip: log writes, replay into a fresh keyspace
{
    enum path = "/tmp/dreads_aof_test_roundtrip.aof";
    rmPath(path);
    scope (exit)
        rmPath(path);

    Aof aof;
    assert(aof.open(path));
    aof.append(cast(const(ubyte)[]) respCmd("SET", "k", "v1"));
    aof.append(cast(const(ubyte)[]) respCmd("SET", "k", "v2")); // overwrite wins
    aof.append(cast(const(ubyte)[]) respCmd("RPUSH", "l", "a", "b"));
    aof.append(cast(const(ubyte)[]) respCmd("ZADD", "z", "1.5", "m"));
    aof.append(cast(const(ubyte)[]) respCmd("SET", "gone", "x"));
    aof.append(cast(const(ubyte)[]) respCmd("DEL", "gone"));
    aof.flush();
    aof.fsyncNow();
    aof.close();

    Keyspace ks;
    scope (exit)
        ks.d.free();
    assert(aofLoad(path, ks) == 6);
    assert(runOne(ks, "GET", "k") == "$2\r\nv2\r\n");
    assert(runOne(ks, "LRANGE", "l", "0", "-1") == "*2\r\n$1\r\na\r\n$1\r\nb\r\n");
    assert(runOne(ks, "ZSCORE", "z", "m") == "$3\r\n1.5\r\n");
    assert(runOne(ks, "EXISTS", "gone") == ":0\r\n");
}

unittest // missing file is fine; truncated tail is tolerated
{
    Keyspace ks;
    scope (exit)
        ks.d.free();
    assert(aofLoad("/tmp/dreads_aof_test_missing.aof", ks) == 0);

    enum path = "/tmp/dreads_aof_test_trunc.aof";
    rmPath(path);
    scope (exit)
        rmPath(path);
    Aof aof;
    assert(aof.open(path));
    auto full = respCmd("SET", "ok", "yes");
    auto partial = respCmd("SET", "cut", "off");
    aof.append(cast(const(ubyte)[])(full ~ partial[0 .. partial.length - 7]));
    aof.close();

    assert(aofLoad(path, ks) == 1);
    assert(runOne(ks, "GET", "ok") == "$3\r\nyes\r\n");
    assert(runOne(ks, "EXISTS", "cut") == ":0\r\n");
}

unittest // EVAL is replayed through the scripting engine
{
    enum path = "/tmp/dreads_aof_test_eval.aof";
    rmPath(path);
    scope (exit)
        rmPath(path);

    Aof aof;
    assert(aof.open(path));
    aof.append(cast(const(ubyte)[]) respCmd("EVAL",
            "redis.call('SET', KEYS[1], 'fromlua')", "1", "k2"));
    aof.close();

    Keyspace ks;
    scope (exit)
        ks.d.free();
    assert(aofLoad(path, ks) == 1);
    assert(runOne(ks, "GET", "k2") == "$7\r\nfromlua\r\n");
}

unittest // a translated XADD (resolved ID) replays with the identical ID
{
    enum path = "/tmp/dreads_aof_test_xadd.aof";
    rmPath(path);
    scope (exit)
        rmPath(path);

    Aof aof;
    assert(aof.open(path));
    aof.append(cast(const(ubyte)[]) respCmd("XADD", "st", "1234-7", "f", "v"));
    aof.close();

    Keyspace ks;
    scope (exit)
        ks.d.free();
    assert(aofLoad(path, ks) == 1);
    assert(runOne(ks, "XLEN", "st") == ":1\r\n");
    auto entries = runOne(ks, "XRANGE", "st", "-", "+");
    import std.algorithm : canFind;

    assert(entries.canFind("1234-7")); // same ID, not a re-generated one
}

unittest // appendEval writes an EVAL the loader accepts
{
    enum path = "/tmp/dreads_aof_test_evalsha.aof";
    rmPath(path);
    scope (exit)
        rmPath(path);

    Aof aof;
    assert(aof.open(path));
    RVal[3] rest;
    rest[0].type = RType.BulkString;
    rest[0].str = "1";
    rest[1].type = RType.BulkString;
    rest[1].str = "k3";
    rest[2].type = RType.BulkString;
    rest[2].str = "value!";
    aof.appendEval("redis.call('SET', KEYS[1], ARGV[1])", rest[]);
    aof.close();

    Keyspace ks;
    scope (exit)
        ks.d.free();
    assert(aofLoad(path, ks) == 1);
    assert(runOne(ks, "GET", "k3") == "$6\r\nvalue!\r\n");
}

unittest // v2: IR records replay by opcode; offsets rebuild the args exactly
{
    import dreads.acl : aclCmdIndex;

    enum path = "/tmp/dreads_aof_test_ir.aof";
    rmPath(path);
    scope (exit)
        rmPath(path);

    Aof aof;
    assert(aof.open(path));
    Arena pa;
    auto enc = respCmd("SET", "irk", "irv");
    RVal cmd;
    size_t pos = 0;
    assert(parseValue(cast(const(ubyte)[]) enc, pos, pa, cmd) == ParseStatus.ok);
    aof.appendIR(cmd, aclCmdIndex("set"), cast(const(ubyte)[]) enc);
    aof.append(cast(const(ubyte)[]) respCmd("RPUSH", "irl", "x")); // RAW record
    aof.close();

    Keyspace ks;
    scope (exit)
        ks.d.free();
    assert(aofLoad(path, ks) == 2);
    assert(runOne(ks, "GET", "irk") == "$3\r\nirv\r\n");
    assert(runOne(ks, "LRANGE", "irl", "0", "-1") == "*1\r\n$1\r\nx\r\n");
}

unittest // v2: a foreign table hash demotes opcodes — replay resolves by name
{
    enum path = "/tmp/dreads_aof_test_irhash.aof";
    rmPath(path);
    scope (exit)
        rmPath(path);

    Aof aof;
    assert(aof.open(path));
    Arena pa;
    auto enc = respCmd("SET", "hk", "hv");
    RVal cmd;
    size_t pos = 0;
    assert(parseValue(cast(const(ubyte)[]) enc, pos, pa, cmd) == ParseStatus.ok);
    // a WRONG opcode on purpose: it must be ignored once the hash mismatches
    aof.appendIR(cmd, 1, cast(const(ubyte)[]) enc);
    aof.close();

    { // flip one byte of the stored table hash
        import core.stdc.stdio : fseek, SEEK_SET;

        auto fh = fopen(path.ptr, "r+b");
        assert(fh !is null);
        ubyte b;
        fseek(fh, 15, SEEK_SET);
        assert(fread(&b, 1, 1, fh) == 1);
        b ^= 0xFF;
        fseek(fh, 15, SEEK_SET);
        assert(fwrite(&b, 1, 1, fh) == 1);
        fclose(fh);
    }

    Keyspace ks;
    scope (exit)
        ks.d.free();
    assert(aofLoad(path, ks) == 1);
    assert(runOne(ks, "GET", "hk") == "$2\r\nhv\r\n");
}

unittest // v2: legacy files smaller than the 16-byte sniff still replay fully
{
    enum path = "/tmp/dreads_aof_test_tiny.aof";
    rmPath(path);
    scope (exit)
        rmPath(path);

    { // hand-written legacy file, 14 bytes < the sniff window
        auto fh = fopen(path.ptr, "wb");
        assert(fh !is null);
        auto raw = respCmd("SET", "t", ""); // *3..$1 t $0 — still < 32B? build exact
        fwrite(raw.ptr, 1, raw.length, fh);
        fclose(fh);
    }
    Keyspace ks;
    scope (exit)
        ks.d.free();
    assert(aofLoad(path, ks) == 1);
    assert(runOne(ks, "EXISTS", "t") == ":1\r\n");
}

unittest // v2: kill-9 mid-record — the partial trailing record is tolerated
{
    enum path = "/tmp/dreads_aof_test_irtrunc.aof";
    rmPath(path);
    scope (exit)
        rmPath(path);

    Aof aof;
    assert(aof.open(path));
    aof.append(cast(const(ubyte)[]) respCmd("SET", "whole", "1"));
    aof.append(cast(const(ubyte)[]) respCmd("SET", "cut", "2"));
    aof.close();

    { // truncate inside the LAST record's payload
        import core.stdc.stdio : fseek, ftell, SEEK_END;
        import core.sys.posix.unistd : truncate;

        auto fh = fopen(path.ptr, "rb");
        fseek(fh, 0, SEEK_END);
        immutable long sz = ftell(fh);
        fclose(fh);
        assert(truncate(path.ptr, sz - 5) == 0);
    }

    Keyspace ks;
    scope (exit)
        ks.d.free();
    assert(aofLoad(path, ks) == 1);
    assert(runOne(ks, "GET", "whole") == "$1\r\n1\r\n");
    assert(runOne(ks, "EXISTS", "cut") == ":0\r\n");
}
