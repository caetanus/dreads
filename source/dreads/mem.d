module dreads.mem;

// Zero-GC building blocks: everything here is malloc-backed and @nogc.
// The GC must never run in the data plane (see logo: "Arena memory. Zero-GC overhead").

import core.stdc.string : memcpy, memmove, memset;
import std.experimental.allocator.mallocator : Mallocator;
import std.experimental.allocator : reallocate;
import dreads.alloc : ConnAllocator;

/// Growable malloc-backed byte buffer (network in/out buffers, reply building).
public struct ByteBuffer
{
    private ubyte* ptr;
    private size_t len;
    private size_t cap;

    @disable this(this);

    ~this() @nogc nothrow @trusted
    {
        if (ptr !is null)
        {
            ConnAllocator.instance.deallocate((cast(void*) ptr)[0 .. cap]);
            ptr = null;
        }
        len = cap = 0;
    }

    @property size_t length() const @nogc nothrow
    {
        return len;
    }

    @property bool empty() const @nogc nothrow
    {
        return len == 0;
    }

    /// Current contents (valid until the next mutation).
    inout(ubyte)[] data() inout @nogc nothrow
    {
        return ptr[0 .. len];
    }

    void reserve(size_t need) @nogc nothrow @trusted
    {
        if (len + need <= cap)
            return;
        size_t ncap = cap ? cap : 256;
        while (ncap < len + need)
            ncap *= 2;
        void[] blk = ptr is null ? null : (cast(void*) ptr)[0 .. cap];
        immutable ok = reallocate(ConnAllocator.instance, blk, ncap);
        assert(ok, "out of memory");
        ptr = cast(ubyte*) blk.ptr;
        cap = ncap;
    }

    void append(scope const(void)[] bytes) @nogc nothrow
    {
        if (bytes.length == 0)
            return;
        reserve(bytes.length);
        memcpy(ptr + len, bytes.ptr, bytes.length);
        len += bytes.length;
    }

    void appendByte(ubyte b) @nogc nothrow
    {
        reserve(1);
        ptr[len++] = b;
    }

    /// Space to read into directly (e.g. from a socket); commit with grow().
    ubyte[] freeSpace(size_t atLeast) @nogc nothrow
    {
        reserve(atLeast);
        return ptr[len .. cap];
    }

    /// Marks n bytes of freeSpace() as filled.
    void grow(size_t n) @nogc nothrow
    {
        assert(len + n <= cap);
        len += n;
    }

    /// Drops the first n bytes (consumed by the parser).
    void consume(size_t n) @nogc nothrow
    {
        assert(n <= len);
        if (n == 0)
            return;
        if (n < len)
            memmove(ptr, ptr + n, len - n);
        len -= n;
    }

    void clear() @nogc nothrow
    {
        len = 0;
    }

    /// Drops everything past the first n bytes (n must be <= length). Used to
    /// roll back a reply that CLIENT REPLY OFF/SKIP suppresses.
    void truncate(size_t n) @nogc nothrow
    {
        if (n < len)
            len = n;
    }
}

/// Region allocator: bump-pointer allocation, freed all at once with reset().
/// One arena per connection; reset after each command.
public struct Arena
{
    private static struct Block
    {
        Block* next;
        size_t cap;
        size_t used;
        // payload follows the header
    }

    private Block* head;
    private size_t blockSize = 16 * 1024;

    @disable this(this);

    ~this() @nogc nothrow @trusted
    {
        Block* b = head;
        while (b !is null)
        {
            Block* next = b.next;
            ConnAllocator.instance.deallocate((cast(void*) b)[0 .. Block.sizeof + b.cap]);
            b = next;
        }
        head = null;
    }

    void* alloc(size_t size, size_t alignment = 16) @nogc nothrow @trusted
    {
        if (size == 0)
            return null;
        if (head !is null)
        {
            size_t base = cast(size_t)(cast(ubyte*) head + Block.sizeof);
            size_t at = (base + head.used + alignment - 1) & ~(alignment - 1);
            if (at + size <= base + head.cap)
            {
                head.used = at + size - base;
                return cast(void*) at;
            }
        }
        size_t cap = size + alignment > blockSize ? size + alignment : blockSize;
        auto b = cast(Block*) ConnAllocator.instance.allocate(Block.sizeof + cap).ptr;
        assert(b !is null, "out of memory");
        b.next = head;
        b.cap = cap;
        b.used = 0;
        head = b;
        return alloc(size, alignment);
    }

    T[] allocArray(T)(size_t n) @nogc nothrow
    {
        if (n == 0)
            return null;
        auto p = cast(T*) alloc(T.sizeof * n, T.alignof);
        return p[0 .. n];
    }

    /// Copies bytes into the arena and returns the arena-owned slice.
    const(char)[] dupString(scope const(char)[] s) @nogc nothrow
    {
        if (s.length == 0)
            return "";
        auto p = cast(char*) alloc(s.length, 1);
        memcpy(p, s.ptr, s.length);
        return p[0 .. s.length];
    }

    /// Frees everything allocated so far, keeping the newest block for reuse.
    void reset() @nogc nothrow @trusted
    {
        while (head !is null && head.next !is null)
        {
            Block* next = head.next;
            ConnAllocator.instance.deallocate((cast(void*) head)[0 .. Block.sizeof + head.cap]);
            head = next;
        }
        if (head !is null)
            head.used = 0;
    }
}

// Owned-slice helpers, templatized on the allocator (`Alloc = Mallocator`). Every
// allocation goes through `Alloc.instance.allocate`; the returned slice length is
// exactly the requested size, so `freeSlice` can `deallocate` with the correct
// size for size-aware backends (Mallocator ignores it). Keyspace data passes
// `KeyspaceAllocator` (dreads.alloc); control-plane callers keep the default so
// their bytes stay out of the maxmemory accounting.

/// Owned copy of a byte slice (storage keys/values). Free with freeSlice!Alloc.
public char[] mallocDup(Alloc = Mallocator)(scope const(char)[] s) @nogc nothrow @trusted
{
    if (s.length == 0)
        return null;
    auto p = cast(char*) Alloc.instance.allocate(s.length).ptr;
    assert(p !is null, "out of memory");
    memcpy(p, s.ptr, s.length);
    return p[0 .. s.length];
}

/// A zero-filled owned buffer of n bytes. Free with freeSlice!Alloc.
public char[] allocZeroed(Alloc = Mallocator)(size_t n) @nogc nothrow @trusted
{
    if (n == 0)
        return null;
    auto p = cast(char*) Alloc.instance.allocate(n).ptr;
    assert(p !is null, "out of memory");
    memset(p, 0, n);
    return p[0 .. n];
}

public void freeSlice(Alloc = Mallocator)(const(char)[] s) @nogc nothrow @trusted
{
    if (s.ptr !is null)
        Alloc.instance.deallocate(cast(void[]) s);
}

/// New owned slice holding a ~ b; frees a (APPEND-style in-place growth).
public const(char)[] mallocAppend(Alloc = Mallocator)(const(char)[] a, scope const(char)[] b) @nogc nothrow @trusted
{
    if (b.length == 0)
        return a;
    auto p = cast(char*) Alloc.instance.allocate(a.length + b.length).ptr;
    assert(p !is null, "out of memory");
    if (a.length)
        memcpy(p, a.ptr, a.length);
    memcpy(p + a.length, b.ptr, b.length);
    auto r = p[0 .. a.length + b.length];
    freeSlice!Alloc(a);
    return r;
}

unittest
{
    ByteBuffer b;
    b.append("hello ");
    b.append("world");
    assert(cast(string) b.data == "hello world");
    b.consume(6);
    assert(cast(string) b.data == "world");
    auto space = b.freeSpace(10);
    space[0] = '!';
    b.grow(1);
    assert(cast(string) b.data == "world!");
    b.clear();
    assert(b.empty);
}

unittest
{
    Arena a;
    auto s1 = a.dupString("foo");
    auto s2 = a.dupString("bar");
    assert(s1 == "foo" && s2 == "bar");
    auto arr = a.allocArray!int(1000);
    assert(arr.length == 1000);
    arr[999] = 42;
    assert(arr[999] == 42);
    // force a second block
    auto big = a.allocArray!ubyte(64 * 1024);
    assert(big.length == 64 * 1024);
    a.reset();
    auto s3 = a.dupString("baz");
    assert(s3 == "baz");
}

// ---------------------------------------------------------------------------
// keyspace memory accounting — the REAL, build-portable figure that drives
// maxmemory (INFO used_memory + eviction). Now that every keyspace value routes
// through KeyspaceAllocator, its StatsCollector counts exactly the data plane's
// live bytes: portable (no jemalloc/Linux dependency), and tight for the eviction
// loop (evicting a key directly lowers this number, unlike a process-wide count
// that non-keyspace overhead can pin above the limit). See dreads.alloc.
// ---------------------------------------------------------------------------

/// Live bytes held by the keyspace data allocator. Drives maxmemory.
public ulong usedMemory() nothrow @nogc
{
    import dreads.alloc : keyspaceBytesUsed;

    return keyspaceBytesUsed();
}
