module dreads.obj;

// Typed value objects and the keyspace. RObj is a tagged union over the five
// Redis data structures; all payloads are plain @nogc data freed via free().

import dreads.dict : Dict, StrVal, Unit;
import dreads.list : DList;
import dreads.stream : Stream, StreamID, nowMs;
import dreads.det : detNow = now;
import dreads.notify : notifyKeyspaceEvent, NClass;
import dreads.zset : ZSet;

public enum ObjType : ubyte
{
    str,
    list,
    hash,
    set,
    zset,
    stream
}

/// Coarse LRU clock in seconds, refreshed by the server's 1s timer — keeps
/// per-lookup touching to a single store instead of a clock_gettime call.
public __gshared uint lruClock;

public struct RObj
{
    ObjType type;
    ulong expireAtMs; // 0 = no expiry; absolute epoch ms otherwise
    uint lruSecs; // last access, in lruClock units
    union
    {
        StrVal str;
        DList list;
        Dict!StrVal hash;
        Dict!Unit set;
        ZSet zset;
        Stream stream;
    }

    void free() @nogc nothrow
    {
        final switch (type)
        {
        case ObjType.str:
            str.free();
            break;
        case ObjType.list:
            list.free();
            break;
        case ObjType.hash:
            hash.free();
            break;
        case ObjType.set:
            set.free();
            break;
        case ObjType.zset:
            zset.free();
            break;
        case ObjType.stream:
            stream.free();
            break;
        }
    }

    static RObj ofStr(scope const(char)[] v) @nogc nothrow
    {
        RObj o;
        o.type = ObjType.str;
        o.str = StrVal.of(v);
        return o;
    }

    /// Empty object of any type; the zeroed union is a valid empty container.
    static RObj empty(ObjType t) @nogc nothrow
    {
        RObj o;
        o.type = t;
        return o;
    }

    /// Element count of container types (0 for strings).
    size_t containerLen() const @nogc nothrow
    {
        final switch (type)
        {
        case ObjType.str:
            return 0;
        case ObjType.list:
            return list.length;
        case ObjType.hash:
            return hash.length;
        case ObjType.set:
            return set.length;
        case ObjType.zset:
            return zset.length;
        case ObjType.stream:
            return stream.length;
        }
    }

    /// Deep copy for COPY: fresh allocations for every element; keeps TTL.
    RObj deepDup() const @nogc nothrow
    {
        RObj c;
        c.type = type;
        c.expireAtMs = expireAtMs;
        final switch (type)
        {
        case ObjType.str:
            c.str = StrVal.of(str.s);
            break;
        case ObjType.list:
            foreach (v; list)
                c.list.pushBack(v);
            break;
        case ObjType.hash:
            foreach (i; 0 .. hash.capacity)
            {
                if (hash.slotLive(i))
                    c.hash.set(hash.keyAt(i), StrVal.of(hash.valAt(i).s));
            }
            break;
        case ObjType.set:
            foreach (i; 0 .. set.capacity)
            {
                if (set.slotLive(i))
                    c.set.set(set.keyAt(i), Unit());
            }
            break;
        case ObjType.zset:
            zset.walkRange(0, zset.length, false, (m, s) {
                c.zset.add(s, m);
                return 0;
            });
            break;
        case ObjType.stream:
            stream.walkRange(StreamID.minId, StreamID.maxId, 0, (id, pairs) {
                c.stream.add(id, pairs);
                return 0;
            });
            c.stream.lastId = stream.lastId; // survives an empty stream
            break;
        }
        return c;
    }

    const(char)[] typeName() const @nogc nothrow
    {
        final switch (type)
        {
        case ObjType.str:
            return "string";
        case ObjType.list:
            return "list";
        case ObjType.hash:
            return "hash";
        case ObjType.set:
            return "set";
        case ObjType.zset:
            return "zset";
        case ObjType.stream:
            return "stream";
        }
    }
}

public struct Keyspace
{
    Dict!RObj d;

    @property size_t length() const @nogc nothrow
    {
        return d.length;
    }

    /// Live object or null — lazily drops the key when its TTL has passed.
    RObj* lookup(scope const(char)[] k) @nogc nothrow
    {
        auto o = d.get(k);
        if (o is null)
            return null;
        if (o.expireAtMs != 0 && detNow() >= o.expireAtMs)
        {
            d.del(k);
            notifyKeyspaceEvent(NClass.expired, "expired", k);
            return null;
        }
        o.lruSecs = lruClock;
        return o;
    }

    /// Typed lookup: null when missing; wrong=true when another type holds k.
    RObj* lookupTyped(scope const(char)[] k, ObjType t, out bool wrong) @nogc nothrow
    {
        auto o = lookup(k);
        if (o is null)
            return null;
        if (o.type != t)
        {
            wrong = true;
            return null;
        }
        return o;
    }

    /// Existing typed object, or a fresh empty one bound to k.
    RObj* getOrCreate(scope const(char)[] k, ObjType t, out bool wrong) @nogc nothrow
    {
        auto o = lookup(k);
        if (o !is null)
        {
            if (o.type != t)
            {
                wrong = true;
                return null;
            }
            return o;
        }
        d.set(k, RObj.empty(t));
        return d.get(k);
    }

    /// SET semantics: overwrites whatever type currently holds k.
    void setStr(scope const(char)[] k, scope const(char)[] v) @nogc nothrow
    {
        d.set(k, RObj.ofStr(v));
    }

    bool del(scope const(char)[] k) @nogc nothrow
    {
        // an already-expired key must read as missing, not as deleted-now
        return lookup(k) !is null && d.del(k);
    }

    bool exists(scope const(char)[] k) @nogc nothrow
    {
        return lookup(k) !is null;
    }

    /// Moves the object (with its TTL) to a new name, overwriting any
    /// destination. False when the source is missing.
    bool rename(scope const(char)[] from, scope const(char)[] to) @nogc nothrow
    {
        if (lookup(from) is null) // also drops it if expired
            return false;
        RObj obj;
        d.steal(from, obj);
        d.set(to, obj);
        return true;
    }

    /// Containers left empty by removals disappear, like in Redis.
    /// Streams are the exception: an empty stream keeps existing (lastId lives on).
    void delIfEmpty(scope const(char)[] k, const(RObj)* o) @nogc nothrow
    {
        if (o.type != ObjType.str && o.type != ObjType.stream && o.containerLen == 0)
        {
            d.del(k);
            notifyKeyspaceEvent(NClass.generic, "del", k); // emptied container is removed
        }
    }

    void clear() @nogc nothrow
    {
        d.clear();
    }

    int opApply(scope int delegate(const(char)[] key, ref RObj val) @nogc nothrow dg) @nogc nothrow
    {
        return d.opApply(dg);
    }
}

unittest // typed keyspace flow and WRONGTYPE detection
{
    Keyspace ks;
    scope (exit)
        ks.d.free();

    ks.setStr("s", "hello");
    auto o = ks.lookup("s");
    assert(o !is null && o.type == ObjType.str && o.str.s == "hello");
    assert(o.typeName == "string");

    bool wrong;
    assert(ks.lookupTyped("s", ObjType.list, wrong) is null && wrong);
    wrong = false;
    assert(ks.getOrCreate("s", ObjType.hash, wrong) is null && wrong);

    wrong = false;
    auto l = ks.getOrCreate("mylist", ObjType.list, wrong);
    assert(l !is null && !wrong);
    l.list.pushBack("x");
    assert(ks.lookup("mylist").containerLen == 1);

    // SET overwrites the list, freeing it
    ks.setStr("mylist", "now-a-string");
    assert(ks.lookup("mylist").type == ObjType.str);

    // empty containers vanish
    wrong = false;
    auto st = ks.getOrCreate("myset", ObjType.set, wrong);
    st.set.set("m", Unit());
    st.set.del("m");
    ks.delIfEmpty("myset", st);
    assert(!ks.exists("myset"));

    assert(ks.del("s"));
    assert(ks.length == 1); // only mylist (as string) remains
}

unittest // every type frees through the union without leaking valgrind-wise
{
    Keyspace ks;
    scope (exit)
        ks.d.free();
    bool wrong;
    ks.setStr("a", "1");
    ks.getOrCreate("b", ObjType.list, wrong).list.pushBack("x");
    ks.getOrCreate("c", ObjType.hash, wrong).hash.set("f", StrVal.of("v"));
    ks.getOrCreate("d", ObjType.set, wrong).set.set("m", Unit());
    ks.getOrCreate("e", ObjType.zset, wrong).zset.add(1.5, "m");
    assert(ks.length == 5);
    ks.clear();
    assert(ks.length == 0);
}
