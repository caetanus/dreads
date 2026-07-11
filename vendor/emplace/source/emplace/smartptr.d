module emplace.smartptr;

// Our own smart pointers, modeled on libc++'s <memory> (unique_ptr / shared_ptr
// semantics), allocated through Alexandrescu's std.experimental.allocator so any
// allocator plugs in — Mallocator by default, but FreeList / Region / GCAllocator
// et al. work unchanged (a FreeList gives the reply-IR node pool that keeps the
// oracle allocation-cheap). automem is a small third-party lib whose Vector
// link-breaks on smart-pointer elements and whose Unique misbehaves under -O3
// here, so we own these instead: tiny, @nogc, and correct on this toolchain.

import std.experimental.allocator : allocMake = make, allocDispose = dispose;
import std.experimental.allocator.mallocator : Mallocator;
import core.lifetime : emplace, forward;

/// unique_ptr: single ownership, move-only (`@disable this(this)` — a bitwise
/// copy would double-free; the compiler moves it, leaving the source null so its
/// destructor is a no-op). `make` allocates + constructs; the dtor disposes.
struct Uniq(T, Allocator = Mallocator)
{
    private T* _p;

    @disable this(this);

    /// Move constructor: steal the source's pointer (it is left null, so its
    /// destructor is a no-op). Enables `auto b = move(a);` and makes the type
    /// unambiguously move-only for traits under forward references.
    this(U)(Uniq!(U, Allocator) other) @trusted if (is(U : T))
    {
        _p = other._p;
        other._p = null;
    }

    static Uniq make(Args...)(auto ref Args args) @trusted
    {
        Uniq u;
        u._p = allocMake!T(Allocator.instance, forward!args);
        return u;
    }

    ~this() @trusted
    {
        if (_p !is null)
        {
            allocDispose(Allocator.instance, _p); // runs T's dtor + deallocates
            _p = null;
        }
    }

    /// Move-assign (the parameter is moved in; the old owner's pointer rides out
    /// on `rhs` and is freed by its destructor). No copy — Uniq is move-only.
    void opAssign(Uniq rhs) @trusted
    {
        auto tmp = _p;
        _p = rhs._p;
        rhs._p = tmp; // rhs's dtor frees our previous object
    }

    ref inout(T) get() inout @trusted return
    {
        return *_p;
    }

    /// Rust `Box`/borrow: an immutable/mutable reference to the owned value
    /// (borrow the box without taking ownership). Same as `get`.
    alias borrow = get;

    /// Rust `Box::into_inner`: consume the pointer, moving the value out by
    /// value; the box is freed without running the value's destructor (it moved
    /// out). Self is left null.
    T intoInner()() @trusted
    {
        import core.lifetime : move;

        T val = move(*_p); // *_p left in .init, dtor would be a no-op
        Allocator.instance.deallocate((cast(void*) _p)[0 .. T.sizeof]);
        _p = null;
        return val;
    }

    /// Rust `Option::take`: transfer ownership out into the returned Uniq,
    /// leaving self empty (null). Frees nothing — ownership just moves.
    Uniq take() @trusted
    {
        Uniq u;
        u._p = _p;
        _p = null;
        return u;
    }

    bool isNull() const
    {
        return _p is null;
    }
}

/// shared_ptr: reference-counted, copyable (copy bumps the strong count, the
/// last strong owner destroys the value). Like libc++'s make_shared, the value
/// lives inline in the control block, so a shared object is ONE allocation, and
/// the block carries a weak count too (weak owners keep the block, not the
/// value). Non-atomic counts: dreads runs a single event-loop thread (same model
/// as the pub/sub RcMsg) — never share a Shared across threads.
struct Shared(T, Allocator = Mallocator)
{
    private struct Ctrl
    {
        T val;
        long strong;
        long weak;
    }

    private Ctrl* _c;

    static Shared make(Args...)(auto ref Args args) @trusted
    {
        Shared s;
        auto mem = Allocator.instance.allocate(Ctrl.sizeof);
        assert(mem.length == Ctrl.sizeof, "out of memory");
        s._c = cast(Ctrl*) mem.ptr;
        emplace(&s._c.val, forward!args);
        s._c.strong = 1;
        s._c.weak = 0;
        return s;
    }

    this(this) @trusted
    {
        if (_c !is null)
            _c.strong++;
    }

    ~this() @trusted
    {
        if (_c is null)
            return;
        if (--_c.strong == 0)
        {
            destroy!false(_c.val); // value dies with the last strong owner
            if (_c.weak == 0)
                Allocator.instance.deallocate((cast(void*) _c)[0 .. Ctrl.sizeof]);
        }
        _c = null;
    }

    ref inout(T) get() inout @trusted return
    {
        return _c.val;
    }

    bool isNull() const
    {
        return _c is null;
    }

    size_t useCount() const
    {
        return _c is null ? 0 : cast(size_t) _c.strong;
    }

    /// A non-owning observer of this object.
    Weak!(T, Allocator) weaken() @trusted
    {
        return Weak!(T, Allocator)(this);
    }
}

/// weak_ptr: a non-owning observer of a `Shared`. It keeps the control block
/// alive (so it can tell whether the object still exists) but not the value.
/// `lock()` promotes to a `Shared` if the object is still alive, else empty.
/// The block is freed once the last strong AND last weak owner are gone.
struct Weak(T, Allocator = Mallocator)
{
    private alias S = Shared!(T, Allocator);
    private S.Ctrl* _c; // same module: may touch Shared's private control block

    this(ref S s) @trusted
    {
        _c = s._c;
        if (_c !is null)
            _c.weak++;
    }

    this(this) @trusted
    {
        if (_c !is null)
            _c.weak++;
    }

    ~this() @trusted
    {
        if (_c is null)
            return;
        // Last weak owner AND the value is already gone -> free the block.
        if (--_c.weak == 0 && _c.strong == 0)
            Allocator.instance.deallocate((cast(void*) _c)[0 .. S.Ctrl.sizeof]);
        _c = null;
    }

    bool expired() const
    {
        return _c is null || _c.strong == 0;
    }

    /// Promote to a strong owner, or an empty Shared if the object has died.
    S lock() @trusted
    {
        S s;
        if (_c !is null && _c.strong > 0)
        {
            _c.strong++;
            s._c = _c;
        }
        return s;
    }
}

version (unittest) private struct Counter
{
    __gshared int live;
    int v;
    this(int x) @nogc nothrow
    {
        v = x;
        live++;
    }

    ~this() @nogc nothrow
    {
        live--;
    }
}

@nogc nothrow unittest // Uniq: ownership, move, destruction
{
    import core.lifetime : move;

    Counter.live = 0;
    {
        auto a = Uniq!Counter.make(7);
        assert(!a.isNull && a.get.v == 7 && Counter.live == 1);
        auto b = move(a); // a emptied, b owns
        assert(a.isNull && b.get.v == 7 && Counter.live == 1);
    }
    assert(Counter.live == 0);
}

@nogc nothrow unittest // Uniq Rust semantics: borrow, take, into_inner
{
    import core.lifetime : move;

    Counter.live = 0;
    auto a = Uniq!Counter.make(11);
    assert(a.borrow.v == 11); // borrow == get

    auto b = a.take(); // Option::take: a emptied, b owns
    assert(a.isNull && b.get.v == 11 && Counter.live == 1);

    auto inner = b.intoInner(); // move the value out, free the box
    assert(b.isNull && inner.v == 11 && Counter.live == 1); // still one live (moved)
    // `inner` (a stack Counter) dies at scope end
}

@nogc nothrow unittest // Shared: copy bumps refcount, last owner frees
{
    Counter.live = 0;
    {
        auto a = Shared!Counter.make(5);
        assert(a.useCount == 1 && Counter.live == 1);
        {
            auto b = a; // shared
            assert(a.useCount == 2 && b.get.v == 5 && Counter.live == 1);
        }
        assert(a.useCount == 1 && Counter.live == 1);
    }
    assert(Counter.live == 0);
}

@nogc nothrow unittest // Weak: observes without owning; lock/expired lifecycle
{
    Counter.live = 0;
    auto w = () @nogc nothrow {
        auto s = Shared!Counter.make(9);
        auto wk = s.weaken();
        assert(!wk.expired && s.useCount == 1 && Counter.live == 1);
        {
            auto locked = wk.lock(); // promote while alive
            assert(!locked.isNull && locked.get.v == 9 && s.useCount == 2);
        }
        assert(s.useCount == 1);
        return wk; // s dies at scope exit -> value destroyed, block kept for wk
    }();
    assert(Counter.live == 0); // value gone
    assert(w.expired); // object died
    assert(w.lock().isNull); // cannot promote a dead object
    // w's destructor frees the surviving control block (no leak)
}

unittest // a different allocator plugs in unchanged (GCAllocator isn't @nogc,
{ //  which is exactly why attributes are inferred, not forced)
    import std.experimental.allocator.gc_allocator : GCAllocator;

    // Proves the Allocator param is honored end-to-end. Stateful allocators
    // (FreeList/Region pools) need a stateless facade over a shared instance —
    // that's the reply-IR node-pool path, added when we tune the oracle.
    auto a = Uniq!(int, GCAllocator).make(42);
    assert(a.get == 42);
}
