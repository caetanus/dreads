module emplace.vector;

// A GC-free, allocator-aware dynamic array — the automem.Vector replacement.
// Same shape (`Vector!(T, Allocator)`, Mallocator default) and the same small
// hot-path API (put / popBack / clear / length get+set / opSlice), but ours:
// automem's Vector link-breaks on smart-pointer elements and its per-element
// bounds-checked shift is not nothrow. This is a plain grow-by-doubling buffer
// allocated through std.experimental.allocator, copyable (deep copy) so it is a
// drop-in for POD buffers like raft's ByteVec.

import std.experimental.allocator : reallocate;
import std.experimental.allocator.mallocator : Mallocator;

struct Vector(T, Allocator = Mallocator)
{
    private T* _ptr;
    private size_t _len;
    private size_t _cap;

    // Construct from a slice of initial elements.
    this(scope const(T)[] initial) @trusted
    {
        put(initial);
    }

    this(this) @trusted
    {
        if (_cap == 0)
            return;
        import core.stdc.string : memcpy;

        auto old = _ptr;
        _ptr = cast(T*) Allocator.instance.allocate(_cap * T.sizeof).ptr;
        assert(_ptr !is null, "out of memory");
        memcpy(_ptr, old, _len * T.sizeof);
    }

    ~this() @trusted
    {
        if (_ptr !is null)
        {
            Allocator.instance.deallocate((cast(void*) _ptr)[0 .. _cap * T.sizeof]);
            _ptr = null;
            _len = _cap = 0;
        }
    }

    private void ensure(size_t need) @trusted
    {
        if (need <= _cap)
            return;
        size_t nc = _cap ? _cap * 2 : 4;
        if (nc < need)
            nc = need;
        auto block = (cast(void*) _ptr)[0 .. _cap * T.sizeof];
        immutable ok = reallocate(Allocator.instance, block, nc * T.sizeof);
        assert(ok, "out of memory");
        _ptr = cast(T*) block.ptr;
        _cap = nc;
    }

    /// Append one element.
    void put(T x) @trusted
    {
        ensure(_len + 1);
        _ptr[_len++] = x;
    }

    /// Append a slice (bulk) — a single memcpy, never a per-element loop (the
    /// automem wart this replaces). memcpy also sidesteps const-transitivity for
    /// pointer element types (const(T*) -> T*). Valid for the POD / pointer
    /// element types this container targets.
    void put(scope const(T)[] xs) @trusted
    {
        if (xs.length == 0)
            return;
        import core.stdc.string : memcpy;

        ensure(_len + xs.length);
        memcpy(_ptr + _len, xs.ptr, xs.length * T.sizeof);
        _len += xs.length;
    }

    /// Drop the last element (length only; capacity retained).
    void popBack() @nogc nothrow
    {
        if (_len)
            _len--;
    }

    /// Reset to empty, keeping capacity for reuse.
    void clear() @nogc nothrow
    {
        _len = 0;
    }

    void reserve(size_t n) @trusted
    {
        ensure(n);
    }

    @property size_t length() const @nogc nothrow
    {
        return _len;
    }

    /// Resize. Growing keeps capacity (reuse-friendly); new elements are
    /// default-initialized only in the sense that the backing bytes are whatever
    /// was there — callers that grow then memcpy (raft's appendBytes) fill them.
    @property void length(size_t n) @trusted
    {
        ensure(n);
        _len = n;
    }

    @property bool empty() const @nogc nothrow
    {
        return _len == 0;
    }

    /// The contiguous slice. @system like automem's (aliases interior memory).
    inout(T)[] opSlice() inout @nogc nothrow @trusted return
    {
        return _ptr[0 .. _len];
    }

    ref inout(T) opIndex(size_t i) inout @nogc nothrow @trusted return
    {
        return _ptr[i];
    }

    size_t opDollar() const @nogc nothrow
    {
        return _len;
    }
}

@nogc nothrow @system unittest // put/slice/clear/length reuse
{
    Vector!ubyte v;
    ubyte[3] src = [1, 2, 3];
    v.put(src[]);
    v.put(cast(ubyte) 4);
    assert(v.length == 4 && v[] == cast(ubyte[])[1, 2, 3, 4]);
    v.popBack();
    assert(v.length == 3 && v[$ - 1] == 3);

    immutable capBefore = v._cap;
    v.clear();
    assert(v.length == 0 && v._cap == capBefore); // capacity kept

    // length setter grows then the caller fills (raft appendBytes pattern)
    v.length = 2;
    v[0] = 9;
    v[1] = 8;
    assert(v[] == cast(ubyte[])[9, 8]);
}

@nogc nothrow @system unittest // deep copy is independent
{
    Vector!ubyte a;
    a.put(cast(ubyte) 7);
    auto b = a; // this(this) deep copy
    b.put(cast(ubyte) 8);
    assert(a.length == 1 && b.length == 2 && a[0] == 7);
    a[0] = 1;
    assert(b[0] == 7); // independent buffers
}
