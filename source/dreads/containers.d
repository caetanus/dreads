module dreads.containers;
import std.container.rbtree;
import std.typecons : tuple, SafeRefCounted, RefCountedAutoInitialize;
import std.stdio : writeln;

struct AutoNew(T)
{
    static assert(is(T : Object), "Error: " ~ T.stringof ~ ": Auto new can only be used with classes");
    private T val;
    @property auto instance()
    {
        if (val is null)
        {
            val = new T;
        }
        return val;
    }
    @property auto constInstance() const {
        return val;
    }

    alias instance this;
}

public struct OrderedSet(T)
{
    AutoNew!(RedBlackTree!T) tree;
    this(T[] items...)
    {
        foreach (item; items)
        {
            tree.insert(item);

        }
    }

    this(T[] items)
    {
        foreach (item; items)
        {
            tree.insert(item);
        }
    }

    @safe OrderedSet!T dup()
    {
        OrderedSet!T copy;
        foreach (item; tree)
        {
            copy.add(cast(T) item);
        }
        return copy;
    }

    @safe void add(T item)
    {
        tree.stableInsert(item);
    }

    @safe void remove(T item)
    {
        auto r = tree.equalRange(item);
        {
            tree.remove(r);
        }
    }

    @safe bool has(T item)
    {
        return item in tree.instance;
    }

    @safe bool opBinaryRight(string op : "in")(const T lhs)
    {
        return has(lhs);
    }

    @safe size_t length()
    {
        return tree.length;
    }

    @safe void clear()
    {
        tree.clear;
    }

    @safe @property auto opSlice()
    {
        return tree[];
    }

    @safe int opApply(scope int delegate(T) @safe dg) const
    {
        if (tree.constInstance is null) {
            return 0;
        }
        if (dg is null) return 0;
        foreach (e; tree.constInstance[])
        {
            auto r = dg(e);
            if (r)
                return r;
        }
        return 0;
    }

    @safe OrderedSet!T opBinary(string op : "+")(OrderedSet!T other)
    {
        return union_(other);
    }

    @safe OrderedSet!T opBinary(string op : "-")(OrderedSet!T other)
    {
        return difference(other);
    }

    @safe bool opEquals(const OrderedSet!T other) const
    {
        T[] a, b;
        foreach (item; this)
            a ~= item;
        foreach (item; other)
            b ~= item;
        return a == b;
    }

    // Kept consistent with opEquals: iteration is in sorted order, so two equal
    // sets yield the same element sequence and therefore the same hash.
    @safe size_t toHash() const nothrow
    {
        size_t h = 0;
        try
            foreach (item; this)
                h = h * 31 + hashOf(item);
        catch (Exception)
        {
        }
        return h;
    }

    @safe auto range()
    {
        return tree.instance[];
    }

    @safe OrderedSet!T union_(const OrderedSet!T other)
    {
        OrderedSet!T result = this.dup;
        foreach (item; other)
        {
            result.add(item);
        }
        return result;

    }

    @safe OrderedSet!T intersection(ref OrderedSet!T other)
    {
        OrderedSet!T result;
        foreach (item; this)
        {
            if (other.has(item))
            {
                result.add(item);
            }
        }
        return result;
    }

    @safe OrderedSet!T difference(ref OrderedSet!T other)
    {
        OrderedSet!T result;
        foreach (item; this)
        {
            if (!other.has(item))
            {
                result.add(item);
            }
        }
        return result;
    }

    @safe OrderedSet!T symmetricDifference(OrderedSet!T other)
    {
        return (this - other) + (other - this);
    }

    @safe bool isSubsetOf(ref OrderedSet!T other)
    {
        foreach (item; this)
        {
            if (!other.has(item))
                return false;
        }
        return true;
    }

    @safe bool isSuperSetOf(ref OrderedSet!T other)
    {
        return other.isSubsetOf(this);
    }

    @safe string toString() const
    {
        import std.array : appender;
        import std.format : format;

        auto a = appender!string();
        a.put("{");
        bool first = true;
        foreach (item; this)
        {
            if (!first)
                a.put(", ");
            static if (is(T == string))
            {
                a.put(item.format!"\"%s\"");
            }
            else
                a.put(item.format!"%s");
            first = false;
        }
        a.put("}");
        return a.data;
    }

}

unittest
{
    import std.stdio : writeln;

    writeln("Oi");
    OrderedSet!int a;
    assert(a.tree !is null, "tree is null");
    a.add(1);
    a.add(22);
    a.add(22);
    a.add(22);
    a.add(3);
    writeln("my set is:", a);
}
