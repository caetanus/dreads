module dreads.lz4;

// Optional LZ4 block compression for the Raft replication wire. The transport
// (draft's VibeTransport) is compression-agnostic: it owns the frame format
// (a flag bit in the length prefix + an original-length field) but calls out
// to these two function pointers for the actual codec. dreads installs them so
// the standalone raft library keeps ZERO dependency on liblz4 — only the final
// dreads executable links it.
//
// Why LZ4: raft log entries are raw RESP command bytes (SET/HSET/... with their
// keys and values), which are extremely repetitive across a batch; LZ4 gets a
// good ratio at multi-GB/s decompress and ~500MB/s+ compress, so the CPU it
// costs on the single-threaded event loop is dwarfed by the bytes it keeps off
// a real (non-loopback) link. Opt-in via `raft-compress yes`.

import raft.types : ByteVec, data;

// liblz4 (LZ4_compress_default / LZ4_decompress_safe): the stable, simplest
// block API. All @nogc nothrow — pure buffer-to-buffer, no allocation, no
// errno. Linked via dub.json libs (liblz4).
extern (C) @nogc nothrow @system
{
    int LZ4_compressBound(int inputSize);
    int LZ4_compress_default(const(char)* src, char* dst, int srcSize, int dstCapacity);
    int LZ4_decompress_safe(const(char)* src, char* dst, int compressedSize, int dstCapacity);
}

/// Compress `src` into `dst` (a reused, malloc-backed transport scratch buffer).
/// Returns the compressed length, or 0 to signal "don't compress this frame"
/// (empty/oversized input, or an unexpected liblz4 failure — the caller then
/// sends the frame uncompressed, so a codec hiccup never drops a raft message).
size_t lz4Compress(scope const(ubyte)[] src, ref ByteVec dst) @nogc nothrow @system
{
    if (src.length == 0 || src.length > int.max)
        return 0;
    immutable bound = LZ4_compressBound(cast(int) src.length);
    if (bound <= 0)
        return 0;
    dst.clear();
    dst.length = cast(size_t) bound; // ensure capacity; length trimmed to actual below
    immutable n = LZ4_compress_default(cast(const(char)*) src.ptr,
        cast(char*) dst.data.ptr, cast(int) src.length, bound);
    if (n <= 0)
    {
        dst.clear();
        return 0;
    }
    dst.length = cast(size_t) n;
    return cast(size_t) n;
}

/// Decompress `src` (exactly `origLen` bytes of plaintext) into `dst`. Returns
/// false on any malformed input (corrupt frame / wrong length) — the caller
/// drops the frame and lets raft retry, never trusting a partial decode.
bool lz4Decompress(scope const(ubyte)[] src, size_t origLen, ref ByteVec dst) @nogc nothrow @system
{
    if (origLen == 0)
    {
        dst.clear();
        return true;
    }
    if (origLen > int.max || src.length > int.max || src.length == 0)
        return false;
    dst.clear();
    dst.length = origLen;
    immutable n = LZ4_decompress_safe(cast(const(char)*) src.ptr,
        cast(char*) dst.data.ptr, cast(int) src.length, cast(int) origLen);
    if (n < 0 || cast(size_t) n != origLen)
        return false;
    return true;
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

version (unittest)
{
    import fluent.asserts;
    import raft.types : appendBytes;

    // A realistic raft AppendEntries batch: many RESP SET commands (repetitive
    // framing + keys) is exactly the shape LZ4 shrinks well.
    private ByteVec respBatch(size_t n) @system
    {
        import std.format : sformat;

        ByteVec b;
        char[64] tmp = void;
        foreach (i; 0 .. n)
        {
            auto k = sformat(tmp[], "user:session:%d", i);
            auto cmd = "*3\r\n$3\r\nSET\r\n$" ~ "18\r\n"; // header-ish; content below is what matters
            appendBytes(b, cast(const(ubyte)[]) cmd);
            appendBytes(b, cast(const(ubyte)[]) k);
            appendBytes(b, cast(const(ubyte)[]) "\r\n$5\r\nhello\r\n");
        }
        return b;
    }

    @("lz4.roundtrip_preserves_bytes")
    unittest
    {
        auto src = respBatch(200);
        (src.data.length > 0).expect.to.equal(true);

        ByteVec comp;
        auto clen = lz4Compress(src.data, comp);
        (clen > 0).expect.to.equal(true);
        (clen < src.data.length).expect.to.equal(true); // it actually shrank

        ByteVec back;
        lz4Decompress(comp.data, src.data.length, back).expect.to.equal(true);
        back.data.length.expect.to.equal(src.data.length);
        (back.data == src.data).expect.to.equal(true); // byte-exact
    }

    @("lz4.binary_safe_roundtrip")
    unittest
    {
        // NUL / high bytes must survive (payloads are binary-safe RESP).
        ubyte[512] raw = void;
        foreach (i; 0 .. raw.length)
            raw[i] = cast(ubyte)((i * 37) ^ (i >> 3));
        ByteVec comp, back;
        auto clen = lz4Compress(raw[], comp);
        (clen > 0).expect.to.equal(true);
        lz4Decompress(comp.data, raw.length, back).expect.to.equal(true);
        (back.data == raw[]).expect.to.equal(true);
    }

    @("lz4.corrupt_input_is_rejected")
    unittest
    {
        auto src = respBatch(50);
        ByteVec comp;
        lz4Compress(src.data, comp);
        // Truncate the compressed block: decode must fail, not read OOB / lie.
        auto chopped = comp.data[0 .. comp.data.length / 2];
        ByteVec back;
        lz4Decompress(chopped, src.data.length, back).expect.to.equal(false);
        // Wrong declared origLen also fails.
        lz4Decompress(comp.data, src.data.length + 999, back).expect.to.equal(false);
    }

    @("lz4.empty_input")
    unittest
    {
        ByteVec comp, back;
        lz4Compress(null, comp).expect.to.equal(0UL); // nothing to do
        lz4Decompress(null, 0, back).expect.to.equal(true); // trivially ok
        back.data.length.expect.to.equal(0UL);
    }
}
