module emplace.map;

// @nogc ordered containers — a classic CLRS red-black tree, plus an OrderedSet
// built on it. Neither automem nor Phobos offers a @nogc associative container
// (std.container.rbtree is GC-backed; automem ships only Vector), so the tree
// is hand-rolled: O(log n) insert/lookup/remove, in-order (sorted-key)
// iteration, nodes allocated through std.experimental.allocator's Mallocator
// (the allocator automem uses) and freed explicitly.
//
// Keys compare with `cmpKey` (byte-lexicographic for const(char)[]). Both
// containers are move-only (`@disable this(this)`): they own a raw node graph,
// so a bitwise copy would double-free. Pass by `ref`, or `dup()` explicitly.

import std.experimental.allocator : make, dispose;
import std.experimental.allocator.mallocator : Mallocator;

private int cmpKey(K)(const K a, const K b) @nogc nothrow
{
    static if (is(K : const(char)[]))
    {
        immutable n = a.length < b.length ? a.length : b.length;
        foreach (i; 0 .. n)
        {
            if (a[i] != b[i])
                return a[i] < b[i] ? -1 : 1;
        }
        if (a.length != b.length)
            return a.length < b.length ? -1 : 1;
        return 0;
    }
    else
        return a < b ? -1 : (a > b ? 1 : 0);
}

private enum Color : ubyte
{
    red,
    black
}

struct Map(K, V)
{
    private struct Node
    {
        K key;
        V val;
        Node* left, right, parent;
        Color color;
    }

    private Node* root;
    private size_t count;

    @disable this(this); // move-only: owns a raw node graph

    ~this() @nogc nothrow
    {
        clear();
    }

    @property size_t length() const @nogc nothrow
    {
        return count;
    }

    @property bool empty() const @nogc nothrow
    {
        return count == 0;
    }

    private Node* alloc(K key, V val) @nogc nothrow @trusted
    {
        auto n = Mallocator.instance.make!Node;
        n.key = key;
        n.val = val;
        n.color = Color.red;
        return n;
    }

    private void rotateLeft(Node* x) @nogc nothrow
    {
        auto y = x.right;
        x.right = y.left;
        if (y.left !is null)
            y.left.parent = x;
        y.parent = x.parent;
        if (x.parent is null)
            root = y;
        else if (x is x.parent.left)
            x.parent.left = y;
        else
            x.parent.right = y;
        y.left = x;
        x.parent = y;
    }

    private void rotateRight(Node* x) @nogc nothrow
    {
        auto y = x.left;
        x.left = y.right;
        if (y.right !is null)
            y.right.parent = x;
        y.parent = x.parent;
        if (x.parent is null)
            root = y;
        else if (x is x.parent.right)
            x.parent.right = y;
        else
            x.parent.left = y;
        y.right = x;
        x.parent = y;
    }

    /// Insert or overwrite the value for `key`.
    void set(K key, V val) @nogc nothrow @trusted
    {
        Node* parent = null;
        auto cur = root;
        while (cur !is null)
        {
            immutable c = cmpKey(key, cur.key);
            if (c == 0)
            {
                cur.val = val; // overwrite
                return;
            }
            parent = cur;
            cur = c < 0 ? cur.left : cur.right;
        }
        auto n = alloc(key, val);
        n.parent = parent;
        if (parent is null)
            root = n;
        else if (cmpKey(key, parent.key) < 0)
            parent.left = n;
        else
            parent.right = n;
        count++;
        insertFixup(n);
    }

    private void insertFixup(Node* z) @nogc nothrow
    {
        while (z.parent !is null && z.parent.color == Color.red)
        {
            auto gp = z.parent.parent;
            if (z.parent is gp.left)
            {
                auto y = gp.right; // uncle
                if (y !is null && y.color == Color.red)
                {
                    z.parent.color = Color.black;
                    y.color = Color.black;
                    gp.color = Color.red;
                    z = gp;
                }
                else
                {
                    if (z is z.parent.right)
                    {
                        z = z.parent;
                        rotateLeft(z);
                    }
                    z.parent.color = Color.black;
                    gp.color = Color.red;
                    rotateRight(gp);
                }
            }
            else
            {
                auto y = gp.left; // uncle
                if (y !is null && y.color == Color.red)
                {
                    z.parent.color = Color.black;
                    y.color = Color.black;
                    gp.color = Color.red;
                    z = gp;
                }
                else
                {
                    if (z is z.parent.left)
                    {
                        z = z.parent;
                        rotateRight(z);
                    }
                    z.parent.color = Color.black;
                    gp.color = Color.red;
                    rotateLeft(gp);
                }
            }
        }
        root.color = Color.black;
    }

    /// Pointer to the stored value, or null if absent.
    V* get(K key) @nogc nothrow return
    {
        auto n = find(key);
        return n is null ? null : &n.val;
    }

    private Node* find(K key) @nogc nothrow
    {
        auto cur = root;
        while (cur !is null)
        {
            immutable c = cmpKey(key, cur.key);
            if (c == 0)
                return cur;
            cur = c < 0 ? cur.left : cur.right;
        }
        return null;
    }

    bool contains(K key) @nogc nothrow
    {
        return find(key) !is null;
    }

    // --- bound queries: one O(log n) descent, no second index / double search.
    // leftBound  = ceiling: the smallest key >= `key` (left edge of a range
    //              starting at `key`), i.e. tree.find_left(key).
    // rightBound = floor:   the largest key <= `key` (right edge of a range
    //              ending at `key`), i.e. tree.find_right(key).

    private Node* ceilNode(K key) @nogc nothrow
    {
        Node* best = null;
        auto cur = root;
        while (cur !is null)
        {
            immutable c = cmpKey(key, cur.key);
            if (c == 0)
                return cur;
            if (c < 0)
            {
                best = cur; // cur.key > key: candidate, look for a smaller one
                cur = cur.left;
            }
            else
                cur = cur.right;
        }
        return best;
    }

    private Node* floorNode(K key) @nogc nothrow
    {
        Node* best = null;
        auto cur = root;
        while (cur !is null)
        {
            immutable c = cmpKey(key, cur.key);
            if (c == 0)
                return cur;
            if (c > 0)
            {
                best = cur; // cur.key < key: candidate, look for a larger one
                cur = cur.right;
            }
            else
                cur = cur.left;
        }
        return best;
    }

    /// Ceiling. Value of the smallest key >= `key`, its key via `foundKey`;
    /// null when every key is smaller.
    V* leftBound(K key, out K foundKey) @nogc nothrow return
    {
        auto n = ceilNode(key);
        if (n is null)
            return null;
        foundKey = n.key;
        return &n.val;
    }

    /// Floor. Value of the largest key <= `key`, its key via `foundKey`;
    /// null when every key is larger.
    V* rightBound(K key, out K foundKey) @nogc nothrow return
    {
        auto n = floorNode(key);
        if (n is null)
            return null;
        foundKey = n.key;
        return &n.val;
    }

    /// In-order iteration restricted to keys in [lo, hi] (inclusive). Descends
    /// to the left bound then walks forward, so it costs O(log n + hits), not a
    /// full traversal.
    int foreachRange(K lo, K hi, scope int delegate(ref K, ref V) @nogc nothrow dg) @nogc nothrow
    {
        return walkRange(root, lo, hi, dg);
    }

    private static int walkRange(Node* n, K lo, K hi,
            scope int delegate(ref K, ref V) @nogc nothrow dg) @nogc nothrow
    {
        if (n is null)
            return 0;
        immutable cl = cmpKey(n.key, lo);
        immutable ch = cmpKey(n.key, hi);
        if (cl > 0) // n.key > lo: left subtree may still be in range
            if (auto r = walkRange(n.left, lo, hi, dg))
                return r;
        if (cl >= 0 && ch <= 0) // lo <= n.key <= hi
            if (auto r = dg(n.key, n.val))
                return r;
        if (ch < 0) // n.key < hi: right subtree may still be in range
            if (auto r = walkRange(n.right, lo, hi, dg))
                return r;
        return 0;
    }

    private void transplant(Node* u, Node* v) @nogc nothrow
    {
        if (u.parent is null)
            root = v;
        else if (u is u.parent.left)
            u.parent.left = v;
        else
            u.parent.right = v;
        if (v !is null)
            v.parent = u.parent;
    }

    private static Node* minimum(Node* n) @nogc nothrow
    {
        while (n.left !is null)
            n = n.left;
        return n;
    }

    /// Remove `key` if present; returns true when a node was removed.
    bool remove(K key) @nogc nothrow @trusted
    {
        auto z = find(key);
        if (z is null)
            return false;
        Node* y = z;
        Color yColor = y.color;
        Node* x, xParent;
        if (z.left is null)
        {
            x = z.right;
            xParent = z.parent;
            transplant(z, z.right);
        }
        else if (z.right is null)
        {
            x = z.left;
            xParent = z.parent;
            transplant(z, z.left);
        }
        else
        {
            y = minimum(z.right);
            yColor = y.color;
            x = y.right;
            if (y.parent is z)
                xParent = y;
            else
            {
                xParent = y.parent;
                transplant(y, y.right);
                y.right = z.right;
                y.right.parent = y;
            }
            transplant(z, y);
            y.left = z.left;
            y.left.parent = y;
            y.color = z.color;
        }
        if (yColor == Color.black)
            deleteFixup(x, xParent);
        Mallocator.instance.dispose(z);
        count--;
        return true;
    }

    private static bool isBlack(Node* n) @nogc nothrow
    {
        return n is null || n.color == Color.black; // null leaves are black
    }

    private void deleteFixup(Node* x, Node* parent) @nogc nothrow
    {
        while (x !is root && isBlack(x))
        {
            if (x is parent.left)
            {
                auto w = parent.right; // sibling (non-null: black-height >= 1)
                if (w.color == Color.red)
                {
                    w.color = Color.black;
                    parent.color = Color.red;
                    rotateLeft(parent);
                    w = parent.right;
                }
                if (isBlack(w.left) && isBlack(w.right))
                {
                    w.color = Color.red;
                    x = parent;
                    parent = x.parent;
                }
                else
                {
                    if (isBlack(w.right))
                    {
                        if (w.left !is null)
                            w.left.color = Color.black;
                        w.color = Color.red;
                        rotateRight(w);
                        w = parent.right;
                    }
                    w.color = parent.color;
                    parent.color = Color.black;
                    if (w.right !is null)
                        w.right.color = Color.black;
                    rotateLeft(parent);
                    x = root;
                    parent = null;
                }
            }
            else
            {
                auto w = parent.left; // mirror image
                if (w.color == Color.red)
                {
                    w.color = Color.black;
                    parent.color = Color.red;
                    rotateRight(parent);
                    w = parent.left;
                }
                if (isBlack(w.right) && isBlack(w.left))
                {
                    w.color = Color.red;
                    x = parent;
                    parent = x.parent;
                }
                else
                {
                    if (isBlack(w.left))
                    {
                        if (w.right !is null)
                            w.right.color = Color.black;
                        w.color = Color.red;
                        rotateLeft(w);
                        w = parent.left;
                    }
                    w.color = parent.color;
                    parent.color = Color.black;
                    if (w.left !is null)
                        w.left.color = Color.black;
                    rotateRight(parent);
                    x = root;
                    parent = null;
                }
            }
        }
        if (x !is null)
            x.color = Color.black;
    }

    /// In-order (ascending key) iteration.
    int opApply(scope int delegate(ref K, ref V) @nogc nothrow dg) @nogc nothrow
    {
        return walk(root, dg);
    }

    private static int walk(Node* n, scope int delegate(ref K, ref V) @nogc nothrow dg) @nogc nothrow
    {
        if (n is null)
            return 0;
        if (auto r = walk(n.left, dg))
            return r;
        if (auto r = dg(n.key, n.val))
            return r;
        return walk(n.right, dg);
    }

    /// Free every node. Empty afterwards.
    void clear() @nogc nothrow @trusted
    {
        freeSubtree(root);
        root = null;
        count = 0;
    }

    private static void freeSubtree(Node* n) @nogc nothrow @trusted
    {
        if (n is null)
            return;
        freeSubtree(n.left);
        freeSubtree(n.right);
        Mallocator.instance.dispose(n);
    }
}

/// The valueless companion: an ordered set on the same red-black tree.
struct OrderedSet(T)
{
    private struct Unit
    {
    }

    private Map!(T, Unit) tree;

    @disable this(this); // move-only (owns the tree)

    void add(T item) @nogc nothrow
    {
        tree.set(item, Unit.init);
    }

    bool remove(T item) @nogc nothrow
    {
        return tree.remove(item);
    }

    bool has(T item) @nogc nothrow
    {
        return tree.contains(item);
    }

    bool opBinaryRight(string op : "in")(T item) @nogc nothrow
    {
        return has(item);
    }

    @property size_t length() const @nogc nothrow
    {
        return tree.length;
    }

    @property bool empty() const @nogc nothrow
    {
        return tree.empty;
    }

    void clear() @nogc nothrow
    {
        tree.clear();
    }

    /// Iterate members in ascending order.
    int opApply(scope int delegate(ref T) @nogc nothrow dg) @nogc nothrow
    {
        return tree.opApply((ref T k, ref Unit _) => dg(k));
    }

    OrderedSet dup() @nogc nothrow
    {
        OrderedSet r;
        foreach (ref k, ref _; tree)
            r.add(k);
        return r;
    }

    OrderedSet union_(ref OrderedSet other) @nogc nothrow
    {
        OrderedSet r;
        foreach (ref k, ref _; tree)
            r.add(k);
        foreach (item; other)
            r.add(item);
        return r;
    }

    OrderedSet intersection(ref OrderedSet other) @nogc nothrow
    {
        OrderedSet r;
        foreach (item; this)
            if (other.has(item))
                r.add(item);
        return r;
    }

    OrderedSet difference(ref OrderedSet other) @nogc nothrow
    {
        OrderedSet r;
        foreach (item; this)
            if (!other.has(item))
                r.add(item);
        return r;
    }

    OrderedSet symmetricDifference(ref OrderedSet other) @nogc nothrow
    {
        OrderedSet r;
        foreach (item; this)
            if (!other.has(item))
                r.add(item);
        foreach (item; other)
            if (!has(item))
                r.add(item);
        return r;
    }

    OrderedSet opBinary(string op : "+")(ref OrderedSet other) @nogc nothrow
    {
        return union_(other);
    }

    OrderedSet opBinary(string op : "-")(ref OrderedSet other) @nogc nothrow
    {
        return difference(other);
    }

    bool isSubsetOf(ref OrderedSet other) @nogc nothrow
    {
        foreach (item; this)
            if (!other.has(item))
                return false;
        return true;
    }

    bool isSupersetOf(ref OrderedSet other) @nogc nothrow
    {
        return other.isSubsetOf(this);
    }

    bool equals(ref OrderedSet other) @nogc nothrow
    {
        return length == other.length && isSubsetOf(other);
    }
}

@nogc nothrow unittest // ordered map: balanced insert, lookup, sorted iteration
{
    Map!(const(char)[], int) m;
    m.set("banana", 2);
    m.set("apple", 1);
    m.set("cherry", 3);
    m.set("apple", 10); // overwrite, no new node
    assert(m.length == 3);
    assert(*m.get("apple") == 10);
    assert(m.get("durian") is null);
    assert(m.contains("banana") && !m.contains("durian"));

    const(char)[][3] seen;
    size_t n = 0;
    foreach (ref k, ref v; m)
        seen[n++] = k;
    assert(seen[0] == "apple" && seen[1] == "banana" && seen[2] == "cherry");
}

@nogc nothrow unittest // stays balanced/correct across ascending inserts (BST worst case)
{
    Map!(int, int) m;
    foreach (i; 0 .. 1000)
        m.set(i, i * i);
    assert(m.length == 1000);
    foreach (i; 0 .. 1000)
        assert(*m.get(i) == i * i);

    long prev = -1;
    bool sorted = true;
    foreach (ref k, ref v; m)
    {
        if (k <= prev)
            sorted = false;
        prev = k;
    }
    assert(sorted);
}

@nogc nothrow unittest // RB deletion: membership and ordering hold after removals
{
    Map!(int, int) m;
    foreach (i; 0 .. 500)
        m.set(i, i);
    // remove all evens
    foreach (i; 0 .. 500)
        if (i % 2 == 0)
            assert(m.remove(i));
    assert(m.length == 250);
    assert(!m.remove(0)); // already gone
    foreach (i; 0 .. 500)
    {
        if (i % 2 == 0)
            assert(m.get(i) is null);
        else
            assert(*m.get(i) == i);
    }
    // still sorted after all the rebalancing
    long prev = -1;
    bool sorted = true;
    foreach (ref k, ref v; m)
    {
        if (k <= prev)
            sorted = false;
        prev = k;
    }
    assert(sorted);
}

@nogc nothrow unittest // pseudo-random insert/delete churn stays consistent
{
    Map!(int, int) m;
    bool[256] present;
    uint seed = 0x1234_5678;
    foreach (_; 0 .. 20_000)
    {
        seed = seed * 1_664_525 + 1_013_904_223; // LCG (no Math.random in scripts/tests)
        immutable key = seed % 256;
        if ((seed >> 16) & 1)
        {
            m.set(key, cast(int) key);
            present[key] = true;
        }
        else
        {
            immutable had = present[key];
            assert(m.remove(key) == had);
            present[key] = false;
        }
    }
    size_t expect = 0;
    foreach (p; present)
        if (p)
            expect++;
    assert(m.length == expect);
    foreach (k; 0 .. 256)
        assert((m.get(k) !is null) == present[k]);
}

@nogc nothrow unittest // leftBound (ceiling) / rightBound (floor) / range
{
    Map!(const(char)[], int) m; // keys: a c e g
    foreach (i, k; ["a", "c", "e", "g"])
        m.set(k, cast(int) i);

    const(char)[] fk;
    // exact hit resolves to itself for both bounds
    assert(*m.leftBound("c", fk) == 1 && fk == "c");
    assert(*m.rightBound("c", fk) == 1 && fk == "c");
    // between keys: ceiling rounds up, floor rounds down
    assert(*m.leftBound("b", fk) == 1 && fk == "c"); // smallest >= "b"
    assert(*m.rightBound("d", fk) == 1 && fk == "c"); // largest <= "d"
    // edges
    assert(*m.leftBound("g", fk) == 3 && fk == "g");
    assert(m.leftBound("h", fk) is null); // nothing >= "h"
    assert(m.rightBound("Z", fk) is null); // nothing <= "Z" (uppercase sorts first)
    assert(*m.rightBound("z", fk) == 3 && fk == "g"); // floor past the end

    // foreachRange [b, f] -> c, e (O(log n + hits), not a full walk)
    const(char)[][8] hit;
    size_t n = 0;
    m.foreachRange("b", "f", (ref k, ref v) { hit[n++] = k; return 0; });
    assert(n == 2 && hit[0] == "c" && hit[1] == "e");
}

@nogc nothrow unittest // OrderedSet: membership, in, dedup, ordered iteration
{
    OrderedSet!int s;
    s.add(22);
    s.add(1);
    s.add(22); // dedup
    s.add(3);
    assert(s.length == 3);
    assert(1 in s && !(9 in s));
    assert(s.remove(22) && !s.has(22));
    assert(s.length == 2);

    int[2] seen;
    size_t n = 0;
    foreach (v; s)
        seen[n++] = v;
    assert(seen[0] == 1 && seen[1] == 3); // sorted
}

@nogc nothrow unittest // OrderedSet algebra
{
    OrderedSet!int a, b;
    foreach (v; [1, 2, 3, 4])
        a.add(v);
    foreach (v; [3, 4, 5, 6])
        b.add(v);

    auto u = a.union_(b);
    assert(u.length == 6); // 1..6
    auto i = a.intersection(b);
    assert(i.length == 2 && i.has(3) && i.has(4));
    auto d = a.difference(b);
    assert(d.length == 2 && d.has(1) && d.has(2) && !d.has(3));
    auto sd = a.symmetricDifference(b);
    assert(sd.length == 4 && sd.has(1) && sd.has(6) && !sd.has(3));

    OrderedSet!int sub;
    sub.add(1);
    sub.add(2);
    assert(sub.isSubsetOf(a) && a.isSupersetOf(sub));
    assert(!a.isSubsetOf(sub));
}
