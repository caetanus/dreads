module dreads.raftlog;

// Durable raft.storage.Storage: a framed log file plus a small meta file.
// The in-memory copy is the working set (entriesFrom returns slices into
// it); the files exist for recovery. Every mutation fsyncs before returning
// — Raft's correctness depends on persistence preceding the RPC reply.
//
// Log frame:  [u64 term][u64 index][u32 len][payload]
// Meta file:  [u64 currentTerm][u32 votedFor]

import core.stdc.stdio : FILE, fclose, fflush, fopen, fread, fseek, ftell, fwrite,
    remove, rename, SEEK_END, SEEK_SET, stderr, fprintf;
import core.stdc.stdlib : crealloc = realloc, cfree = free, abort;
import core.stdc.errno : errno;

// POSIX-first by project decision; Windows is not a target for the server.
import core.sys.posix.stdio : fileno;
import core.sys.posix.unistd : ftruncate, fsync;
// druntime binds fdatasync only for glibc; it exists in musl libc too, so declare
// it directly there (lets dreads build against musl for small static images).
version (CRuntime_Musl)
    extern (C) int fdatasync(int) @nogc nothrow;
else
    import core.sys.posix.unistd : fdatasync;

import raft.storage : Storage;
import raft.types;

import dreads.mem : freeSlice, mallocDup;
import dreads.syncer : Durability;

private enum FRAME_HDR = 20; // term(8) + index(8) + len(4)

// A failed fdatasync must NEVER be swallowed: it means the bytes the caller is
// about to acknowledge as committed are not on stable storage, which breaks the
// module invariant "persistence precedes the RPC reply". There is no safe retry
// — the kernel may have already discarded the writeback error and cleared it on
// the next call (the "fsyncgate" behaviour, matched by Postgres/Redis) — so the
// only correct response is fail-stop: crash before we ack or drop a backup.
private void fsyncOrDie(int fd) nothrow @nogc
{
    if (fdatasync(fd) != 0)
    {
        cast(void) fprintf(stderr,
            "dreads: FATAL: fdatasync failed (errno=%d) — refusing to acknowledge a non-durable raft write\n",
            errno);
        abort();
    }
}

final class RaftLog : Storage
{
    private FILE* logF;
    private FILE* oldLogF; // sealed crash-backup from the last rotation (kept
    // open one rotation so the durability thread never races its close)
    private FILE* metaF;
    // async group-commit durability; null in tests / synchronous mode
    private Durability durability_;
    private Term term_;
    private NodeId voted_;
    // in-memory working copy (payloads malloc'd)
    private LogEntry* entries; // in-memory entries with index > snapIdx_
    private size_t len;
    private size_t cap;
    private ulong* offsets; // file offset of each entry (malloc'd, shares len/cap with entries)
    // snapshot / compaction
    private Index snapIdx_; // lastIncludedIndex (0 = none)
    private Term snapTerm_;
    private const(char)[] snapData_; // malloc'd
    private const(char)[] snapConfig_; // malloc'd: membership as of snapIdx_ (encodeConfig form)
    private char[512] base_ = void;
    private size_t baseLen_;

    /// Opens (creating if absent) <base>.raftlog and <base>.raftmeta,
    /// rebuilding the in-memory log. A torn tail from a crash mid-write is
    /// truncated away. Returns null on I/O failure.
    static RaftLog open(scope const(char)[] base) nothrow
    {
        auto self = new RaftLog;
        char[512] zp = void;
        if (base.length + 10 >= zp.length)
            return null;
        self.base_[0 .. base.length] = base;
        self.baseLen_ = base.length;
        self.loadSnapshotFile();

        zp[0 .. base.length] = base;
        zp[base.length .. base.length + 10] = ".raftmeta\0";
        self.metaF = fopen(zp.ptr, "r+b");
        if (self.metaF is null)
            self.metaF = fopen(zp.ptr, "w+b");
        if (self.metaF is null)
            return null;
        ubyte[12] meta;
        fseek(self.metaF, 0, SEEK_SET);
        if (fread(meta.ptr, 1, 12, self.metaF) == 12)
        {
            self.term_ = readLE64(meta[0 .. 8]);
            self.voted_ = cast(NodeId) readLE32(meta[8 .. 12]);
        }

        zp[base.length .. base.length + 9] = ".raftlog\0";
        self.logF = fopen(zp.ptr, "r+b");
        if (self.logF is null)
            self.logF = fopen(zp.ptr, "w+b");
        if (self.logF is null)
            return null;
        self.recover();
        return self;
    }

    /// Attaches async group-commit durability (server mode). Once set,
    /// append() no longer fdatasyncs inline; the caller gates its RPC reply
    /// with awaitDurable(). The fd is this log's own file.
    void enableAsyncDurability()
    {
        import dreads.config : gConfig;

        // Only "full" gates the RPC reply on a per-batch fdatasync (Raft-strict
        // durability). "normal"/"off" must NOT pay that fsync+await on every
        // write's critical path — with the fiber-lock held across awaitDurable
        // it also serialises ack processing. Leaving durability_ null makes
        // append() OS-buffered and awaitDurable() a no-op (matching Redis
        // appendfsync everysec/no).
        if (gConfig.synchronous != "full")
            return;
        if (durability_ is null && logF !is null)
            // Seed the durable baseline with the recovered lastIndex: those
            // entries were just read off disk, so they are already durable. Else
            // the first awaitDurable(recoveredLastIndex) blocks forever (durable_
            // starts at 0) and deadlocks the raft loop on restart.
            durability_ = new Durability(fileno(logF), snapIdx_ + len,
                    gConfig.fsyncBackend == "io_uring");
    }

    /// Fiber-side: yield until `index` is on disk (no-op when synchronous).
    void awaitDurable(Index index) nothrow
    {
        // Anything the snapshot covers is already on disk via the snapshot file,
        // NOT the append stream — the durability thread only tracks appends, so
        // waiting on it for a snapshot index would block forever (a freshly
        // snapshot-installed follower never appended those entries). This is the
        // hang that stalled a newcomer's InstallSnapshotReply behind a durability
        // wait during compaction.
        if (index <= snapIdx_)
            return;
        if (durability_ !is null)
            durability_.awaitDurable(index);
    }

    /// Test hook: the durable baseline the async layer was seeded with (0 if
    /// async durability is off). After a recovery+enableAsyncDurability this MUST
    /// equal the recovered lastIndex, else awaitDurable(lastIndex) deadlocks the
    /// raft loop on restart (see the syncer regression test).
    version (unittest) ulong testDurableBaseline() nothrow
    {
        return durability_ is null ? 0 : durability_.durableSeq;
    }

    void close() nothrow
    {
        if (durability_ !is null)
        {
            durability_.stop();
            durability_ = null;
        }
        // Clean shutdown: make the active log durable and drop the crash backup
        // so the next start takes the fast (single-segment) recovery path.
        if (logF !is null)
        {
            fflush(logF);
            fsyncOrDie(fileno(logF));
        }
        if (oldLogF !is null)
        {
            fclose(oldLogF);
            oldLogF = null;
        }
        char[520] oldp = void;
        oldp[0 .. baseLen_] = base_[0 .. baseLen_];
        oldp[baseLen_ .. baseLen_ + 13] = ".raftlog.old\0";
        remove(oldp.ptr);
        foreach (i; 0 .. len)
            (cast(const(char)[]) entries[i].payload).freeSlice;
        if (entries !is null)
            cfree(entries);
        entries = null;
        if (offsets !is null)
            cfree(offsets);
        offsets = null;
        len = cap = 0;
        snapData_.freeSlice;
        snapData_ = null;
        snapConfig_.freeSlice;
        snapConfig_ = null;
        if (logF !is null)
            fclose(logF);
        if (metaF !is null)
            fclose(metaF);
        logF = metaF = null;
    }

    private void recover() nothrow
    {
        // A crash-backup segment (.raftlog.old) exists only if a compaction
        // rotation was interrupted, or on the first reopen after one. Replay it
        // first, then the active log, which is authoritative: a frame whose
        // index already exists truncates the in-memory tail and overwrites it
        // (a rewrite/truncation always wins), so the newest on-disk state wins.
        char[520] oldp = void;
        oldp[0 .. baseLen_] = base_[0 .. baseLen_];
        oldp[baseLen_ .. baseLen_ + 13] = ".raftlog.old\0";
        auto of = fopen(oldp.ptr, "rb");
        bool hadOld = of !is null;
        if (hadOld)
        {
            replaySegment(of);
            fclose(of);
        }
        replaySegment(logF);
        if (hadOld)
        {
            // Fold both segments into a single fresh active file so offsets are
            // consistent, then drop the redundant backup.
            rewriteActiveFromMemory();
            remove(oldp.ptr);
        }
        else
        {
            // Drop any torn tail and position for appends.
            ftruncate(fileno(logF), cast(long) retainedBytes());
            fseek(logF, 0, SEEK_END);
        }
    }

    // Reads framed entries from `f` on top of the in-memory log: skip entries
    // the snapshot already covers, append the next contiguous entry, or (on a
    // conflicting index) truncate the in-memory tail and take the new one.
    // Stops at the first torn/garbage frame.
    private void replaySegment(FILE* f) nothrow
    {
        fseek(f, 0, SEEK_SET);
        for (;;)
        {
            ubyte[FRAME_HDR] hdr;
            if (fread(hdr.ptr, 1, FRAME_HDR, f) != FRAME_HDR)
                break;
            auto term = readLE64(hdr[0 .. 8]);
            auto index = readLE64(hdr[8 .. 16]);
            auto plen = readLE32(hdr[16 .. 20]);
            if (plen > 512 * 1024 * 1024)
                break;
            auto payload = cast(ubyte*) crealloc(null, plen ? plen : 1);
            if (fread(payload, 1, plen, f) != plen)
            {
                cfree(payload);
                break; // torn tail
            }
            if (index <= snapIdx_)
            {
                cfree(payload);
                continue; // covered by the snapshot
            }
            auto expected = snapIdx_ + len + 1;
            if (index == expected)
                push(LogEntry(term, index, payload[0 .. plen]));
            else if (index < expected)
            {
                dropFrom(cast(size_t)(index - snapIdx_ - 1)); // conflict: newer wins
                push(LogEntry(term, index, payload[0 .. plen]));
            }
            else
            {
                cfree(payload);
                break; // gap: torn
            }
        }
    }

    // Frees and drops in-memory entries from position `keep` onward.
    private void dropFrom(size_t keep) nothrow
    {
        if (keep >= len)
            return;
        foreach (i; keep .. len)
            (cast(const(char)[]) entries[i].payload).freeSlice;
        len = keep;
    }

    // Rewrites the active log file from the in-memory entries — recovery-time
    // normalisation only, never the hot path, so the fsync here is fine (it runs
    // once at startup, not during live compaction).
    private void rewriteActiveFromMemory() nothrow
    {
        fseek(logF, 0, SEEK_SET);
        ftruncate(fileno(logF), 0);
        ulong off = 0;
        foreach (i; 0 .. len)
        {
            ubyte[FRAME_HDR] h;
            writeLE64(h[0 .. 8], entries[i].term);
            writeLE64(h[8 .. 16], entries[i].index);
            writeLE32(h[16 .. 20], cast(uint) entries[i].payload.length);
            fwrite(h.ptr, 1, FRAME_HDR, logF);
            if (entries[i].payload.length)
                fwrite(entries[i].payload.ptr, 1, entries[i].payload.length, logF);
            offsets[i] = off;
            off += FRAME_HDR + entries[i].payload.length;
        }
        fflush(logF);
        fsyncOrDie(fileno(logF));
        fseek(logF, 0, SEEK_END);
    }

    private void push(LogEntry e) nothrow
    {
        if (len == cap)
        {
            cap = cap ? cap * 2 : 64;
            entries = cast(LogEntry*) crealloc(entries, cap * LogEntry.sizeof);
            offsets = cast(ulong*) crealloc(offsets, cap * ulong.sizeof);
            assert(entries !is null && offsets !is null, "out of memory");
        }
        entries[len] = e;
        offsets[len] = len == 0 ? 0
            : offsets[len - 1] + FRAME_HDR + entries[len - 1].payload.length;
        len++;
    }

nothrow:

    Term currentTerm()
    {
        return term_;
    }

    void setCurrentTerm(Term t)
    {
        term_ = t;
        persistMeta();
    }

    NodeId votedFor()
    {
        return voted_;
    }

    void setVotedFor(NodeId id)
    {
        voted_ = id;
        persistMeta();
    }

    Index lastIndex()
    {
        return snapIdx_ + len;
    }

    Term termAt(Index i)
    {
        if (i == snapIdx_)
            return snapTerm_;
        if (i > snapIdx_ && i <= snapIdx_ + len)
            return entries[cast(size_t)(i - snapIdx_ - 1)].term;
        return 0;
    }

    const(LogEntry)[] entriesFrom(Index from, size_t max)
    {
        if (from <= snapIdx_ || from > snapIdx_ + len)
            return null;
        auto start = cast(size_t)(from - snapIdx_ - 1);
        auto end = start + max;
        if (end > len)
            end = len;
        return entries[start .. end];
    }

    Index snapshotIndex()
    {
        return snapIdx_;
    }

    /// Bytes the retained (post-snapshot) log occupies. Drives the compaction
    /// policy: a small, recent log is not worth the (blocking) rewrite, so
    /// compaction is gated on size rather than raw entry count — a hot
    /// single-key workload has a tiny snapshot but a huge op count, and must
    /// not trigger a rewrite every few thousand ops.
    size_t retainedBytes()
    {
        if (len == 0)
            return 0;
        return cast(size_t)(offsets[len - 1] + FRAME_HDR + entries[len - 1].payload.length);
    }

    /// Number of retained (post-snapshot) log entries.
    size_t retainedEntries()
    {
        return len;
    }

    /// Bytes that a compaction to `upTo` would RECLAIM — i.e. the entries in
    /// (snapIdx_, upTo]. This is the "dirty" metric the compaction policy gates
    /// on: gating on total retained size instead would thrash (if the retained
    /// log stays above the threshold because of an un-applied tail, it would
    /// compact on every write); gating on the reclaimable delta resets to ~0
    /// right after a compaction, so we compact once per threshold of NEW
    /// applied log, never per write.
    size_t reclaimableBytes(Index upTo)
    {
        if (upTo <= snapIdx_ || len == 0)
            return 0;
        auto k = cast(size_t)(upTo - snapIdx_); // entries that would be dropped
        if (k >= len)
            return retainedBytes();
        return cast(size_t) offsets[k]; // cumulative bytes of the first k entries
    }

    Term snapshotTerm()
    {
        return snapTerm_;
    }

    const(ubyte)[] snapshotData()
    {
        return cast(const(ubyte)[]) snapData_;
    }

    const(ubyte)[] snapshotConfig()
    {
        return cast(const(ubyte)[]) snapConfig_;
    }

    void append(scope const(LogEntry)[] batch)
    {
        foreach (ref e; batch)
        {
            ubyte[FRAME_HDR] hdr;
            writeLE64(hdr[0 .. 8], e.term);
            writeLE64(hdr[8 .. 16], e.index);
            writeLE32(hdr[16 .. 20], cast(uint) e.payload.length);
            fwrite(hdr.ptr, 1, FRAME_HDR, logF);
            if (e.payload.length)
                fwrite(e.payload.ptr, 1, e.payload.length, logF);
            auto copy = mallocDup(cast(const(char)[]) e.payload);
            push(LogEntry(e.term, e.index, cast(const(ubyte)[]) copy));
        }
        if (durability_ !is null)
        {
            fflush(logF); // ensure the batch is in the OS before the fsync
            durability_.requestSync(snapIdx_ + len); // async: caller awaits before reply
        }
        // else (off/normal): leave the batch in the stdio buffer — it flushes in
        // ~BUFSIZ chunks or via the periodic fsync (normal) / clean close. This
        // turns N tiny write syscalls per group-commit cycle into ~N/BUFSIZ,
        // which is the bulk of the single-node raft-pipeline overhead. truncate/
        // rotate/recover flush explicitly before touching the fd directly.
    }

    void truncateFrom(Index from)
    {
        if (from <= snapIdx_ || from > snapIdx_ + len)
            return;
        auto keep = cast(size_t)(from - snapIdx_ - 1);
        foreach (i; keep .. len)
            (cast(const(char)[]) entries[i].payload).freeSlice;
        len = keep;
        fflush(logF); // sync stdio buffer with the fd before truncating it
        ftruncate(fileno(logF), cast(long)(keep == 0 ? 0 : offsets[keep - 1]
                + FRAME_HDR + entries[keep - 1].payload.length));
        fseek(logF, 0, SEEK_END);
        maybeFsync(logF);
    }

    void saveSnapshot(Index lastIncludedIndex, Term lastIncludedTerm,
            scope const(ubyte)[] config, scope const(ubyte)[] data)
    {
        if (lastIncludedIndex <= snapIdx_)
            return;
        // Capture the config FIRST: it may alias an entry payload in the prefix
        // we are about to free (the config entry sits at index <= lastIncluded).
        // Empty config = carry the previous membership forward (don't clobber it).
        if (config.length)
        {
            snapConfig_.freeSlice;
            snapConfig_ = mallocDup(cast(const(char)[]) config);
        }
        // drop the covered in-memory prefix, keeping entries after it
        size_t drop = lastIncludedIndex >= snapIdx_ + len ? len
            : cast(size_t)(lastIncludedIndex - snapIdx_);
        foreach (i; 0 .. drop)
            (cast(const(char)[]) entries[i].payload).freeSlice;
        foreach (i; drop .. len)
            entries[i - drop] = entries[i];
        len -= drop;
        snapIdx_ = lastIncludedIndex;
        snapTerm_ = lastIncludedTerm;
        snapData_.freeSlice;
        snapData_ = mallocDup(cast(const(char)[]) data);
        writeSnapshotFile();
        rotateLog(); // seal + fresh log, no rewrite-in-place, no fsync
    }

    /// Snapshot file layout: [u64 lastIncludedIndex][u64 lastIncludedTerm][data]
    private void loadSnapshotFile() nothrow
    {
        char[520] zp = void;
        zp[0 .. baseLen_] = base_[0 .. baseLen_];
        zp[baseLen_ .. baseLen_ + 10] = ".raftsnap\0";
        auto f = fopen(zp.ptr, "rb");
        if (f is null)
            return;
        ubyte[20] hdr; // [u64 idx][u64 term][u32 configLen]
        if (fread(hdr.ptr, 1, 20, f) == 20)
        {
            snapIdx_ = readLE64(hdr[0 .. 8]);
            snapTerm_ = readLE64(hdr[8 .. 16]);
            auto clen = cast(size_t) readLE32(hdr[16 .. 20]);
            fseek(f, 0, SEEK_END);
            auto total = ftell(f);
            auto body_ = total >= 20 ? cast(size_t)(total - 20) : 0;
            fseek(f, 20, SEEK_SET);
            // membership captured with the snapshot (survives compaction)
            if (clen && clen <= body_)
            {
                auto cbuf = cast(char*) crealloc(null, clen);
                if (fread(cbuf, 1, clen, f) == clen)
                    snapConfig_ = cbuf[0 .. clen];
                else
                    cfree(cbuf);
            }
            auto dlen = body_ >= clen ? body_ - clen : 0;
            if (dlen)
            {
                auto buf = cast(char*) crealloc(null, dlen);
                if (fread(buf, 1, dlen, f) == dlen)
                    snapData_ = buf[0 .. dlen];
                else
                    cfree(buf);
            }
        }
        fclose(f);
    }

    private void writeSnapshotFile() nothrow
    {
        char[520] zp = void;
        char[528] tmp = void;
        zp[0 .. baseLen_] = base_[0 .. baseLen_];
        zp[baseLen_ .. baseLen_ + 10] = ".raftsnap\0";
        tmp[0 .. baseLen_] = base_[0 .. baseLen_];
        tmp[baseLen_ .. baseLen_ + 14] = ".raftsnap.tmp\0";
        auto f = fopen(tmp.ptr, "wb");
        if (f is null)
            return;
        ubyte[20] hdr; // [u64 idx][u64 term][u32 configLen]
        writeLE64(hdr[0 .. 8], snapIdx_);
        writeLE64(hdr[8 .. 16], snapTerm_);
        writeLE32(hdr[16 .. 20], cast(uint) snapConfig_.length);
        fwrite(hdr.ptr, 1, 20, f);
        if (snapConfig_.length)
            fwrite(snapConfig_.ptr, 1, snapConfig_.length, f);
        if (snapData_.length)
            fwrite(snapData_.ptr, 1, snapData_.length, f);
        fflush(f);
        // No fdatasync: compaction must never block on disk sync — that would
        // stall the raft loop and starve heartbeats (node-to-node sync). The
        // atomic rename plus the retained .raftlog.old backup cover a crash;
        // the async durability thread makes it durable on its own schedule.
        fclose(f);
        rename(tmp.ptr, zp.ptr); // atomic replace
    }

    /// Compaction rotation: seal the current log as a crash backup and start a
    /// fresh one, re-writing the surviving in-memory entries into it BUFFERED
    /// (page cache, no fdatasync). Appends — and therefore replication — keep
    /// flowing across a compaction because nothing here waits on disk sync. The
    /// sealed .raftlog.old covers a torn write until the async durability thread
    /// catches up; recovery replays it then the active log.
    private void rotateLog() nothrow
    {
        char[520] active = void, oldp = void;
        active[0 .. baseLen_] = base_[0 .. baseLen_];
        active[baseLen_ .. baseLen_ + 9] = ".raftlog\0";
        oldp[0 .. baseLen_] = base_[0 .. baseLen_];
        oldp[baseLen_ .. baseLen_ + 13] = ".raftlog.old\0";
        // Retire the previous backup: the durability thread was retargeted off
        // its fd a full rotation ago, so closing it now cannot race an fsync.
        if (oldLogF !is null)
        {
            fclose(oldLogF);
            oldLogF = null;
        }
        remove(oldp.ptr);
        fflush(logF);
        rename(active.ptr, oldp.ptr); // seal current active as the crash backup
        oldLogF = logF; // keep its fd open one rotation (durability grace period)
        logF = fopen(active.ptr, "w+b"); // fresh active segment
        if (logF is null)
        {
            logF = oldLogF; // pathological: reuse the sealed handle, skip rotate
            oldLogF = null;
            return;
        }
        ulong off = 0;
        foreach (i; 0 .. len) // surviving entries (> snapIdx_)
        {
            ubyte[FRAME_HDR] h;
            writeLE64(h[0 .. 8], entries[i].term);
            writeLE64(h[8 .. 16], entries[i].index);
            writeLE32(h[16 .. 20], cast(uint) entries[i].payload.length);
            fwrite(h.ptr, 1, FRAME_HDR, logF);
            if (entries[i].payload.length)
                fwrite(entries[i].payload.ptr, 1, entries[i].payload.length, logF);
            offsets[i] = off;
            off += FRAME_HDR + entries[i].payload.length;
        }
        fflush(logF); // page cache only — the durability thread fsyncs later
        if (durability_ !is null)
            durability_.retarget(fileno(logF));
    }

    /// Periodic fsync for the "normal" durability level (host's 1s timer).
    void fsyncNow()
    {
        if (logF !is null)
        {
            fflush(logF);
            fsyncOrDie(fileno(logF));
        }
    }

    private void persistMeta()
    {
        import dreads.config : gConfig;

        ubyte[12] meta;
        writeLE64(meta[0 .. 8], term_);
        writeLE32(meta[8 .. 12], voted_);
        fseek(metaF, 0, SEEK_SET);
        fwrite(meta.ptr, 1, 12, metaF);
        fflush(metaF);
        // votedFor/term are vote-safety critical: fsync unless explicitly off
        if (gConfig.synchronous != "off")
            fsync(fileno(metaF));
    }

    /// SQLite-style: full = fsync now, normal = periodic timer, off = never.
    private void maybeFsync(FILE* f)
    {
        import dreads.config : gConfig;

        if (gConfig.synchronous == "full")
            fsyncOrDie(fileno(f));
    }
}

private ulong readLE64(scope const(ubyte)[] b) nothrow @nogc
{
    ulong v = 0;
    foreach_reverse (x; b)
        v = (v << 8) | x;
    return v;
}

private uint readLE32(scope const(ubyte)[] b) nothrow @nogc
{
    uint v = 0;
    foreach_reverse (x; b)
        v = (v << 8) | x;
    return v;
}

private void writeLE64(ubyte[] b, ulong v) nothrow @nogc
{
    foreach (i; 0 .. 8)
        b[i] = cast(ubyte)(v >> (8 * i));
}

private void writeLE32(ubyte[] b, uint v) nothrow @nogc
{
    foreach (i; 0 .. 4)
        b[i] = cast(ubyte)(v >> (8 * i));
}
