module bench.lz4_bench;

// Measures the LZ4 wire-compression gain on realistic Raft AppendEntries
// frames (the "log sending" path). For several representative dreads write
// workloads and several group-commit batch sizes it reports:
//   - compression ratio + bandwidth saved (the bandwidth win)
//   - compress / decompress throughput in MB/s (the CPU cost on the loop)
// It builds the ACTUAL wire frame via raft.wire.encodeAppendEntries and
// compresses frame[4..] exactly as VibeTransport.maybeCompress does, so the
// numbers reflect the real per-entry binary headers, not just the payloads.
//
// Run: dub run --build=release --config=lz4-bench --compiler=ldc2

import core.time : MonoTime;
import std.stdio : writef, writefln, writeln;

import raft.types : ByteVec, LogEntry, appendBytes, data;
import raft.wire : encodeAppendEntries;
import raft.types : AppendEntries;

import dreads.lz4 : lz4Compress, lz4Decompress;

// A tiny deterministic PRNG (no Math.random): xorshift64.
struct Rng
{
    ulong s = 0x9E3779B97F4A7C15;
     ulong next() @nogc nothrow
    {
        s ^= s << 13;
        s ^= s >> 7;
        s ^= s << 17;
        return s;
    }
    uint below(uint n) @nogc nothrow
    {
        return cast(uint)(next() % n);
    }
}

// A workload builds one RESP command payload per call into `out_`.
alias Gen = void function(ref ByteVec out_, size_t i, ref Rng rng) @system;

void put(ref ByteVec o, string s) @system
{
    appendBytes(o, cast(const(ubyte)[]) s);
}

// SET key:<i> <short value> — cache-style small writes.
void genSetSmall(ref ByteVec o, size_t i, ref Rng rng) @system
{
    import std.conv : to;

    o.clear();
    put(o, "*3\r\n$3\r\nSET\r\n$");
    auto k = "key:" ~ i.to!string;
    put(o, k.length.to!string);
    put(o, "\r\n");
    put(o, k);
    put(o, "\r\n$3\r\n");
    put(o, (rng.below(900) + 100).to!string); // 3-digit number value
    put(o, "\r\n");
}

// SET user:session:<i> <~120B semi-structured value> — session/cache blobs.
void genSetSession(ref ByteVec o, size_t i, ref Rng rng) @system
{
    import std.conv : to;
    import std.format : sformat;

    o.clear();
    char[160] vbuf = void;
    auto v = sformat(vbuf[], `{"uid":%d,"role":"member","ttl":3600,"tok":"%08x%08x","seen":%d}`,
        i, rng.next() & 0xFFFF_FFFF, rng.next() & 0xFFFF_FFFF, i * 7 + 100_000);
    auto k = "user:session:" ~ i.to!string;
    put(o, "*3\r\n$3\r\nSET\r\n$");
    put(o, k.length.to!string);
    put(o, "\r\n");
    put(o, k);
    put(o, "\r\n$");
    put(o, v.length.to!string);
    put(o, "\r\n");
    appendBytes(o, cast(const(ubyte)[]) v);
    put(o, "\r\n");
}

// HSET hash:<i/10> field<i> value<random> — hash field churn.
void genHset(ref ByteVec o, size_t i, ref Rng rng) @system
{
    import std.conv : to;

    o.clear();
    auto k = "hash:" ~ (i / 10).to!string;
    auto f = "field" ~ (i % 10).to!string;
    auto v = "val" ~ (rng.next() & 0xFFFF).to!string;
    put(o, "*4\r\n$4\r\nHSET\r\n$");
    put(o, k.length.to!string); put(o, "\r\n"); put(o, k); put(o, "\r\n$");
    put(o, f.length.to!string); put(o, "\r\n"); put(o, f); put(o, "\r\n$");
    put(o, v.length.to!string); put(o, "\r\n"); put(o, v); put(o, "\r\n");
}

// Build a full AppendEntries wire frame carrying `batch` entries from `gen`,
// return frame[4..] (the body VibeTransport actually compresses).
ByteVec buildFrameBody(Gen gen, size_t batch, ref Rng rng) @system
{
    static ByteVec payloadPool; // holds all payload bytes contiguously
    payloadPool.clear();
    auto entries = new LogEntry[batch];
    size_t[] offs = new size_t[batch];
    size_t[] lens = new size_t[batch];
    ByteVec one;
    foreach (i; 0 .. batch)
    {
        gen(one, i, rng);
        offs[i] = payloadPool.data.length;
        lens[i] = one.data.length;
        appendBytes(payloadPool, one.data);
    }
    auto all = payloadPool.data;
    foreach (i; 0 .. batch)
        entries[i] = LogEntry(1, i + 1, all[offs[i] .. offs[i] + lens[i]]);
    AppendEntries m = {
        term: 1, leaderId: 1, prevLogIndex: 0, prevLogTerm: 0,
        leaderCommit: 0, entries: entries
    };
    auto framed = encodeAppendEntries(1, m);
    ByteVec body_;
    appendBytes(body_, framed[4 .. $]); // copy out (framed scratch is reused)
    return body_;
}

void benchOne(string name, Gen gen, size_t batch) @system
{
    Rng rng;
    auto body_ = buildFrameBody(gen, batch, rng);
    immutable orig = body_.data.length;

    ByteVec comp, back;
    auto clen = lz4Compress(body_.data, comp);
    immutable compressed = clen ? clen : orig; // 0 = not worth compressing
    // verify roundtrip
    bool ok = true;
    if (clen)
    {
        ok = lz4Decompress(comp.data, orig, back) && back.data == body_.data;
        assert(ok, "roundtrip mismatch in bench");
    }

    // throughput: many iterations over the same body
    enum ITERS = 20_000;
    ByteVec c2, d2;
    auto t0 = MonoTime.currTime;
    foreach (_; 0 .. ITERS)
        cast(void) lz4Compress(body_.data, c2);
    auto t1 = MonoTime.currTime;
    foreach (_; 0 .. ITERS)
        cast(void) lz4Decompress(c2.data, orig, d2);
    auto t2 = MonoTime.currTime;

    immutable cMs = (t1 - t0).total!"usecs" / 1000.0;
    immutable dMs = (t2 - t1).total!"usecs" / 1000.0;
    immutable cMBs = (cast(double) orig * ITERS) / (cMs / 1000.0) / (1024 * 1024);
    immutable dMBs = (cast(double) orig * ITERS) / (dMs / 1000.0) / (1024 * 1024);
    immutable ratio = cast(double) orig / compressed;
    immutable saved = 100.0 * (1.0 - cast(double) compressed / orig);

    writef("  %-14s batch=%-4d  %6d -> %6d B  ratio %4.2fx  saved %5.1f%%",
        name, batch, orig, compressed, ratio, saved);
    writefln("   comp %6.0f MB/s  decomp %6.0f MB/s", cMBs, dMBs);
}

void main() @system
{
    writeln("LZ4 Raft wire-compression gain (realistic AppendEntries frames)");
    writeln("  frame body = [sender][kind][ae header][ per entry: term,index,len,RESP ]");
    writeln();
    Gen[string] gens = [
        "SET small": &genSetSmall,
        "SET session": &genSetSession,
        "HSET": &genHset,
    ];
    // deterministic key order
    foreach (name; ["SET small", "SET session", "HSET"])
    {
        foreach (batch; [1UL, 8, 64, 256])
            benchOne(name, gens[name], batch);
        writeln();
    }
    writeln("Note: batch=1 shows the tiny-frame case (near/under the 256B compress");
    writeln("threshold — sent plaintext); larger batches are the real replication win.");
}
