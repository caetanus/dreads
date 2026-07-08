module dreads.raftlog;

// Durable raft.storage.Storage: a framed log file plus a small meta file.
// The in-memory copy is the working set (entriesFrom returns slices into
// it); the files exist for recovery. Every mutation fsyncs before returning
// — Raft's correctness depends on persistence preceding the RPC reply.
//
// Log frame:  [u64 term][u64 index][u32 len][payload]
// Meta file:  [u64 currentTerm][u32 votedFor]

import core.stdc.stdio : FILE, fclose, fflush, fopen, fread, fseek, fwrite,
    SEEK_END, SEEK_SET;
import core.stdc.stdlib : crealloc = realloc, cfree = free;

// POSIX-first by project decision; Windows is not a target for the server.
import core.sys.posix.stdio : fileno;
import core.sys.posix.unistd : fdatasync, ftruncate, fsync;

import raft.storage : Storage;
import raft.types;

import dreads.mem : freeSlice, mallocDup;

private enum FRAME_HDR = 20; // term(8) + index(8) + len(4)

final class RaftLog : Storage
{
    private FILE* logF;
    private FILE* metaF;
    private Term term_;
    private NodeId voted_;
    // in-memory working copy (payloads malloc'd)
    private LogEntry* entries;
    private size_t len;
    private size_t cap;
    private ulong[] offsets; // file offset of each entry (for truncation)

    /// Opens (creating if absent) <base>.raftlog and <base>.raftmeta,
    /// rebuilding the in-memory log. A torn tail from a crash mid-write is
    /// truncated away. Returns null on I/O failure.
    static RaftLog open(scope const(char)[] base) nothrow
    {
        auto self = new RaftLog;
        char[512] zp = void;
        if (base.length + 10 >= zp.length)
            return null;

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

    void close() nothrow
    {
        foreach (i; 0 .. len)
            (cast(const(char)[]) entries[i].payload).freeSlice;
        if (entries !is null)
            cfree(entries);
        entries = null;
        len = cap = 0;
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
            if (index != len + 1 || plen > 512 * 1024 * 1024)
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
            assert(entries !is null, "out of memory");
        }
        entries[len] = e;
        offsets.length = len + 1;
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
        return len;
    }

    Term termAt(Index i)
    {
        return i >= 1 && i <= len ? entries[cast(size_t) i - 1].term : 0;
    }

    const(LogEntry)[] entriesFrom(Index from, size_t max)
    {
        if (from < 1 || from > len)
            return null;
        auto end = cast(size_t)(from - 1) + max;
        if (end > len)
            end = len;
        return entries[cast(size_t) from - 1 .. end];
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
        maybeFsync(logF); // "full": before any RPC reply leaves this node
    }

    void truncateFrom(Index from)
    {
        if (from < 1 || from > len)
            return;
        auto keep = cast(size_t)(from - 1);
        foreach (i; keep .. len)
            (cast(const(char)[]) entries[i].payload).freeSlice;
        len = keep;
        ftruncate(fileno(logF), cast(long)(keep == 0 ? 0 : offsets[keep - 1]
                + FRAME_HDR + entries[keep - 1].payload.length));
        fseek(logF, 0, SEEK_END);
        maybeFsync(logF);
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
