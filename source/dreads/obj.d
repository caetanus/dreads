module dreads.obj;

// Typed value objects and the keyspace. RObj is a tagged union over the five
// Redis data structures; all payloads are plain @nogc data freed via free().

import dreads.dict : Dict, StrVal, Unit;
import dreads.list : DList;
import dreads.stream : Stream, StreamID, nowMs;
import dreads.det : detNow = now;
import dreads.notify : notifyKeyspaceEvent, NClass;
import dreads.zset : ZSet;
import emplace.map : Map;
import emplace.vector : Vector;

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

/// Whether the drop-soon active-expiration timer runs (mirrors
/// Config.activeExpire). Off = lazy-only expiry — the fast default. Gates every
/// index-maintenance path, so SET-with-TTL pays nothing when active expiry is off.
public __gshared bool gActiveExpire;

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

    /// Logically expired right now (deadline set and reached). Read-only — the
    /// caller decides whether to drop it.
    bool expired() const @nogc nothrow
    {
        return expireAtMs != 0 && detNow() >= expireAtMs;
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

    /// The "drop-soon" index for active expiration: absolute deadline (ms) ->
    /// the keys that expire at exactly that instant. Ordered by deadline, so a
    /// sweep visits only the buckets that have come due (`bisect_right(now)` =
    /// `foreachRange(0, now)`), never the whole keyspace. It owns its own copies
    /// of the key strings. Purely local reclamation: the deadline is already
    /// replicated as an absolute PEXPIREAT, so every node/replay expires
    /// deterministically off its own copy — no DEL is propagated.
    private Map!(ulong, Vector!(const(char)[])) expires;

    @disable this(this);

    ~this() @nogc nothrow
    {
        freeExpiresIndex();
    }

    @property size_t length() const @nogc nothrow
    {
        return d.length;
    }

    /// Record that `k` expires at absolute `at` (ms); no-op for at == 0.
    void armExpire(scope const(char)[] k, ulong at) @nogc nothrow
    {
        if (!gActiveExpire || at == 0)
            return;
        auto sk = d.storedKey(k); // the Dict's own stable key bytes — no dup
        if (sk is null)
            return; // not in the keyspace, nothing to expire
        auto bucket = expires.getOrPut(at, Vector!(const(char)[]).init); // one descent
        bucket.put(sk); // non-owning slice into Dict-owned memory
    }

    /// Remove `k`'s entry from the bucket at `at`, freeing its key copy; drops
    /// the bucket when it empties. No-op if the entry isn't there.
    void disarmExpire(scope const(char)[] k, ulong at) @nogc nothrow
    {
        if (!gActiveExpire || at == 0)
            return;
        auto bucket = expires.get(at);
        if (bucket is null)
            return;
        foreach (i; 0 .. bucket.length)
        {
            if ((*bucket)[i] == k)
            {
                (*bucket)[i] = (*bucket)[bucket.length - 1]; // swap-remove: order is irrelevant
                bucket.popBack();
                break;
            }
        }
        if (bucket.length == 0)
            expires.remove(at); // the empty Vector's array is freed by its dtor
    }

    /// Move `k` from deadline `oldAt` to `newAt` in the index (0 = none). Used
    /// on re-EXPIRE / PERSIST so the map never keeps a stale deadline for a key.
    void retimeExpire(scope const(char)[] k, ulong oldAt, ulong newAt) @nogc nothrow
    {
        if (!gActiveExpire || oldAt == newAt)
            return;
        disarmExpire(k, oldAt);
        armExpire(k, newAt);
    }

    /// Active expiration: drop every key whose deadline has passed. Returns how
    /// many were dropped. A bucket entry only fires when the key's *live*
    /// deadline still equals it — so any residual entry (an overwrite/DEL that
    /// bypassed disarm) self-heals here instead of resurrecting a key.
    size_t activeExpireCycle() @nogc nothrow
    {
        if (!gActiveExpire || expires.empty)
            return 0;
        immutable now = detNow();
        Vector!ulong due; // materialise first — we can't remove nodes mid-walk
        foreach (e; expires[]) // ascending by deadline; stop once we pass `now`
        {
            if (e.key > now)
                break;
            due.put(e.key);
        }
        size_t dropped = 0;
        foreach (i; 0 .. due.length)
        {
            immutable at = due[i];
            if (auto bucket = expires.get(at))
            {
                foreach (j; 0 .. bucket.length)
                {
                    auto key = (*bucket)[j];
                    auto obj = d.get(key);
                    if (obj !is null && obj.expireAtMs == at) // still the live TTL
                    {
                        notifyKeyspaceEvent(NClass.expired, "expired", key); // copies before d.del frees it
                        d.del(key);
                        dropped++;
                    }
                    // key is a non-owning slice into Dict memory — nothing to free
                }
            }
            expires.remove(at);
        }
        return dropped;
    }

    private void freeExpiresIndex() @nogc nothrow
    {
        // buckets hold non-owning slices into Dict key memory — only the tree
        // and its bucket arrays need releasing
        expires.clear();
    }

    /// Live object or null — lazily drops the key when its TTL has passed.
    RObj* lookup(scope const(char)[] k) @nogc nothrow
    {
        auto o = d.get(k);
        if (o is null)
            return null;
        if (o.expired())
        {
            disarmExpire(k, o.expireAtMs);
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
        auto o = lookup(k);
        if (o is null)
            return false;
        disarmExpire(k, o.expireAtMs); // drop its pending drop-soon entry
        return d.del(k);
    }

    bool exists(scope const(char)[] k) @nogc nothrow
    {
        return lookup(k) !is null;
    }

    /// Moves the object (with its TTL) to a new name, overwriting any
    /// destination. False when the source is missing.
    bool rename(scope const(char)[] from, scope const(char)[] to) @nogc nothrow
    {
        auto src = lookup(from); // also drops it if expired
        if (src is null)
            return false;
        immutable ttl = src.expireAtMs;
        disarmExpire(from, ttl);
        if (auto dst = lookup(to)) // overwriting a live destination drops its TTL entry
            disarmExpire(to, dst.expireAtMs);
        RObj obj;
        d.steal(from, obj);
        d.set(to, obj);
        armExpire(to, ttl);
        return true;
    }

    /// Containers left empty by removals disappear, like in Redis.
    /// Streams are the exception: an empty stream keeps existing (lastId lives on).
    void delIfEmpty(scope const(char)[] k, const(RObj)* o) @nogc nothrow
    {
        if (o.type != ObjType.str && o.type != ObjType.stream && o.containerLen == 0)
        {
            disarmExpire(k, o.expireAtMs);
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

@nogc nothrow unittest // drop-soon index: active expiration, self-heal, disarm
{
    import dreads.det : gClock;

    Keyspace ks;
    scope (exit)
    {
        ks.d.free();
        gClock = 0;
        gActiveExpire = false;
    }
    gActiveExpire = true; // this test exercises the active path
    gClock = 1_000_000; // freeze "now" at t = 1e6 ms

    ks.setStr("due", "1");
    ks.lookup("due").expireAtMs = 900_000; // already past now
    ks.armExpire("due", 900_000);
    ks.setStr("later", "2");
    ks.lookup("later").expireAtMs = 1_500_000; // future
    ks.armExpire("later", 1_500_000);
    ks.setStr("forever", "3"); // no TTL

    assert(ks.length == 3); // raw count still includes the expired-but-unswept key
    assert(ks.activeExpireCycle() == 1); // only "due" has come due
    assert(ks.length == 2 && !ks.exists("due") && ks.exists("later"));

    gClock = 2_000_000; // "later" now comes due
    assert(ks.activeExpireCycle() == 1);
    assert(!ks.exists("later") && ks.length == 1);

    // PERSIST/retime disarms: no stale entry ever fires
    ks.lookup("forever").expireAtMs = 2_500_000;
    ks.armExpire("forever", 2_500_000);
    ks.retimeExpire("forever", 2_500_000, 0); // PERSIST
    ks.lookup("forever").expireAtMs = 0;
    gClock = 3_000_000;
    assert(ks.activeExpireCycle() == 0); // disarmed, nothing dropped
    assert(ks.exists("forever") && ks.length == 1);

    // re-EXPIRE moves the key to a new deadline without leaving the old one
    ks.lookup("forever").expireAtMs = 3_500_000;
    ks.armExpire("forever", 3_500_000);
    ks.retimeExpire("forever", 3_500_000, 5_000_000); // pushed further out
    ks.lookup("forever").expireAtMs = 5_000_000;
    gClock = 4_000_000; // past the OLD deadline but not the new one
    assert(ks.activeExpireCycle() == 0); // old entry was disarmed
    assert(ks.exists("forever"));
}
