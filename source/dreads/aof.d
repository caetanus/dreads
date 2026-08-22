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

public struct Aof
{
    private FILE* f;
    private ByteBuffer pending;
    private bool dirty;
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

    /// Stages one command's raw RESP bytes; cheap, no I/O.
    void append(scope const(ubyte)[] bytes) @nogc nothrow
    {
        if (f is null)
            return;
        maybeSelect();
        pending.append(bytes);
    }

    /// Re-encodes EVALSHA as EVAL so replay does not depend on the script cache.
    /// rest = [numkeys, keys..., argv...].
    void appendEval(scope const(char)[] body_, const(RVal)[] rest) @nogc nothrow
    {
        if (f is null)
            return;
        maybeSelect();
        repArrayHeader(pending, rest.length + 2);
        repBulk(pending, "EVAL");
        repBulk(pending, body_);
        foreach (ref a; rest)
            repBulk(pending, a.str);
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
    bool ioOk = buf.empty || fwrite(buf.data.ptr, 1, buf.length, f) == buf.length;
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
private void replayCommand(const ref RVal cmd, ref Keyspace ks, ref ByteBuffer sink, ref Arena arena) nothrow
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
    dispatch(cmd, ks, sink, arena);
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

    for (;;)
    {
        auto space = inb.freeSpace(64 * 1024);
        auto n = fread(space.ptr, 1, space.length, f);
        if (n == 0)
            break;
        inb.grow(n);

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
        if (corrupt)
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

    for (;;)
    {
        auto space = inb.freeSpace(64 * 1024);
        auto n = fread(space.ptr, 1, space.length, f);
        if (n == 0)
            break;
        inb.grow(n);

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
        if (corrupt)
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
