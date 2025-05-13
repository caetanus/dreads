module dreads.containers;
import std.container.rbtree;
import std.typecons : tuple;
import std.stdio: writeln;

public struct OrderedSet(T)

{ 
    auto tree = new RedBlackTree!T ;
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

    OrderedSet!T dup() const
    {
        OrderedSet!T copy;
        foreach (item; tree[])
        {   
            copy.add(cast(T)item);
        }
        return copy;
    }

    void add(T item)
    {
        tree.stableInsert(item);
    }

    void remove(T item)
    {
        auto r = tree.equalRange(item);
        {
            tree.remove(r);
        }
    }

    @safe bool has(T item) const
    {
        return item in tree;
    }
    @safe bool opBinary(string op: "in")(const T rhs) 
    {
        return has(rhs);   
    }

    size_t length() const
    {
        return tree.length;
    }

    void clear()
    {
        tree.clear;
    }

    @safe auto opSlice()
    {
        return tree[];
    }

    int opApply(int delegate(T) @safe dg) const @safe
    {
        foreach (e; tree[])
        {
            auto r = dg(e);
            if (r)
                return r;
        }
        return 0;
    }

    @safe OrderedSet!T opBinary(string op : "+")(const ref OrderedSet!T other) const
    {
        return union_(other);
    }

    @safe OrderedSet!T opBinary(string op : "-")(const ref OrderedSet!T other) const
    {
        return difference(other);
    }

    //bool opEquals(const ref OrderedSet!T other) const
    //{
    //    import std.algorithm : equal;

    //    return this.tree[].equal(other.tree);
    //}

    bool opEquals(R)(const R!T other) const
    {
        import std.algorithm : equal;

        return this.tree[].equal(other);

    }

    auto range() const
    {
        return tree[];
    }

    OrderedSet!T union_(const ref OrderedSet!T other) const
    {
        OrderedSet!T result = this.dup;
        foreach (item; other)
        {
            result.add(item);
        }
        return result;

    }

    OrderedSet!T intersection(const ref OrderedSet!T other) const
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

    OrderedSet!T difference(const ref OrderedSet!T other) const
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

    OrderedSet!T symmetricDifference(const ref OrderedSet!T other) const
    {
        return (this - other) + (other - this);
    }

    bool isSubsetOf(const ref OrderedSet!T other) const
    {
        foreach (item; this)
        {
            if (!other.has(item))
                return false;
        }
        return true;
    }

    bool isSuperSetOf(const ref OrderedSet!T other) const
    {
        return other.isSubsetOf(this);
    }

    string toString() const
    {
        import std.array : appender;
        import std.format : format;
        auto a = appender!string();
        a.put("{");
        bool first = true;
        foreach (item; tree[])
        {
            if (!first) a.put(", ");
            a.put(item.format!"%s");
            first = false;
        }
        a.put("}");
        return a.data;
    }

}

unittest
{
    import std.stdio: writeln;
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
