module dreads.raftlog;

// Durable raft.storage.Storage: a framed log file plus a small meta file.
// The in-memory copy is the working set (entriesFrom returns slices into
// it); the files exist for recovery. Every mutation fsyncs before returning
// — Raft's correctness depends on persistence preceding the RPC reply.
//
// Log frame:  [u64 term][u64 index][u32 len][payload]
// Meta file:  [u64 currentTerm][u32 votedFor]

import core.stdc.stdio : FILE, fclose, fflush, fopen, fread, fseek, ftell, fwrite,
    rename, SEEK_END, SEEK_SET;
import core.stdc.stdlib : crealloc = realloc, cfree = free;

// POSIX-first by project decision; Windows is not a target for the server.
import core.sys.posix.stdio : fileno;
import core.sys.posix.unistd : fdatasync, ftruncate, fsync;

import raft.storage : Storage;
import raft.types;

import dreads.mem : freeSlice, mallocDup;
import dreads.syncer : Durability;

private enum FRAME_HDR = 20; // term(8) + index(8) + len(4)

final class RaftLog : Storage
{
    private FILE* logF;
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

        if (durability_ is null && logF !is null)
            durability_ = new Durability(fileno(logF), gConfig.fsyncBackend == "io_uring");
    }

    /// Fiber-side: yield until `index` is on disk (no-op when synchronous).
    void awaitDurable(Index index) nothrow
    {
        if (durability_ !is null)
            durability_.awaitDurable(index);
    }

    void close() nothrow
    {
        if (durability_ !is null)
        {
            durability_.stop();
            durability_ = null;
        }
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
        if (logF !is null)
            fclose(logF);
        if (metaF !is null)
            fclose(metaF);
        logF = metaF = null;
    }

    private void recover() nothrow
    {
        fseek(logF, 0, SEEK_SET);
        ulong offset = 0;
        for (;;)
        {
            ubyte[FRAME_HDR] hdr;
            if (fread(hdr.ptr, 1, FRAME_HDR, logF) != FRAME_HDR)
                break;
            auto term = readLE64(hdr[0 .. 8]);
            auto index = readLE64(hdr[8 .. 16]);
            auto plen = readLE32(hdr[16 .. 20]);
            if (index != snapIdx_ + len + 1 || plen > 512 * 1024 * 1024)
                break; // corrupt or torn: stop here
            auto payload = cast(ubyte*) crealloc(null, plen ? plen : 1);
            if (fread(payload, 1, plen, logF) != plen)
            {
                cfree(payload);
                break; // torn tail
            }
            push(LogEntry(term, index, payload[0 .. plen]));
            offset += FRAME_HDR + plen;
        }
        // drop anything after the last good frame
        ftruncate(fileno(logF), cast(long) offset);
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

    Term snapshotTerm()
    {
        return snapTerm_;
    }

    const(ubyte)[] snapshotData()
    {
        return cast(const(ubyte)[]) snapData_;
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
        fflush(logF);
        if (durability_ !is null)
            durability_.requestSync(snapIdx_ + len); // async: caller awaits before reply
        else
            maybeFsync(logF); // sync path: "full" fdatasyncs before returning
    }

    void truncateFrom(Index from)
    {
        if (from <= snapIdx_ || from > snapIdx_ + len)
            return;
        auto keep = cast(size_t)(from - snapIdx_ - 1);
        foreach (i; keep .. len)
            (cast(const(char)[]) entries[i].payload).freeSlice;
        len = keep;
        ftruncate(fileno(logF), cast(long)(keep == 0 ? 0 : offsets[keep - 1]
                + FRAME_HDR + entries[keep - 1].payload.length));
        fseek(logF, 0, SEEK_END);
        maybeFsync(logF);
    }

    void saveSnapshot(Index lastIncludedIndex, Term lastIncludedTerm, scope const(ubyte)[] data)
    {
        if (lastIncludedIndex <= snapIdx_)
            return;
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
        rewriteLogFile(); // the on-disk log now holds only surviving entries
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
        ubyte[16] hdr;
        if (fread(hdr.ptr, 1, 16, f) == 16)
        {
            snapIdx_ = readLE64(hdr[0 .. 8]);
            snapTerm_ = readLE64(hdr[8 .. 16]);
            fseek(f, 0, SEEK_END);
            auto total = ftell(f);
            auto dlen = total >= 16 ? cast(size_t)(total - 16) : 0;
            if (dlen)
            {
                auto buf = cast(char*) crealloc(null, dlen);
                fseek(f, 16, SEEK_SET);
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
        ubyte[16] hdr;
        writeLE64(hdr[0 .. 8], snapIdx_);
        writeLE64(hdr[8 .. 16], snapTerm_);
        fwrite(hdr.ptr, 1, 16, f);
        if (snapData_.length)
            fwrite(snapData_.ptr, 1, snapData_.length, f);
        fflush(f);
        fdatasync(fileno(f));
        fclose(f);
        rename(tmp.ptr, zp.ptr); // atomic replace
    }

    /// Rewrites the .raftlog file with only the surviving in-memory entries,
    /// rebuilding the offset table. Called after compaction.
    private void rewriteLogFile() nothrow
    {
        fseek(logF, 0, SEEK_SET);
        ftruncate(fileno(logF), 0);
        ulong off = 0;
        foreach (i; 0 .. len) // cap >= len, so offsets[i] is in bounds
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
        fdatasync(fileno(logF));
    }

    /// Periodic fsync for the "normal" durability level (host's 1s timer).
    void fsyncNow()
    {
        if (logF !is null)
        {
            fflush(logF);
            fdatasync(fileno(logF));
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
            fdatasync(fileno(f));
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
