module dreads.obj;

// Typed value objects and the keyspace. RObj is a tagged union over the five
// Redis data structures; all payloads are plain @nogc data freed via free().

import dreads.dict : Dict, StrVal, Unit;
import dreads.list : DList;
import dreads.stream : Stream, StreamID, nowMs;
import dreads.det : detNow = now;
import dreads.notify : notifyKeyspaceEvent, NClass;
import dreads.zset : ZSet;
import dreads.smallset : SmallSet;
import dreads.smallhash : SmallHash;
import dreads.smallzset : SmallZSet;
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

/// INFO stats: lifetime count of keys dropped by lazy or active expiration.
public __gshared ulong gExpiredKeys;

/// INFO stats `expired_fields`: hash fields dropped by TTL (lazy/active reap or a
/// past-deadline HEXPIRE/HGETEX/HSETEX). The per-field analog of gExpiredKeys.
public __gshared ulong gExpiredFields;

/// Import mode (`CONFIG SET import-mode yes` + `CLIENT IMPORT-SOURCE ON`): a
/// bulk-load window for migration tools. While on, expiration is PAUSED so a
/// stream of RESTOREs with absolute TTLs (some already past) loads consistently
/// instead of racing the expiry cycle. Turned off, normal expiry resumes.
public __gshared bool gImportMode;

/// INFO clients: clients currently parked in a blocking wait (B*POP etc.).
/// Also counts clients parked by a CLIENT PAUSE barrier (Valkey counts postponed
/// clients as blocked), so `wait_for_blocked_clients_count` sees a paused client.
public __gshared long gBlockedClients;

/// CLIENT PAUSE state (server layer owns the barrier/replay; kept here so INFO in
/// the data plane can read it without a module cycle). `gPauseUntilMs` is the
/// absolute deadline (0 = not paused); `gPauseAll` true = pause ALL, false = WRITE
/// only. Stacking keeps the higher end-time and the most restrictive action.
public __gshared ulong gPauseUntilMs;
public __gshared bool gPauseAll = true;
public __gshared ulong gPauseIssuer; // conn id that set the pause (exempt from it)

/// The logical databases (Redis SELECT 0..15). The *current* db is per-client
/// (`Conn.db`); the connection dispatches against `gDbs[conn.db]`, SELECT just
/// moves that per-connection index, and the replay/apply path takes the db from
/// the log. Default db 0, so single-DB workloads are unchanged.
public enum NUM_DBS = 16;
public __gshared Keyspace[NUM_DBS] gDbs;

public struct RObj
{
    ObjType type;
    ulong expireAtMs; // 0 = no expiry; absolute epoch ms otherwise
    uint lruSecs; // last access, in lruClock units
    uint expireSlot; // active expiry: this key's index in its deadline bucket (O(1) removal within it)
    ulong subExpireAt; // secondary index: the container's currently-registered nearest internal deadline (0 = none)
    uint subExpireSlot; // this container's index in its sub-expiry bucket (O(1) removal)
    union
    {
        StrVal str;
        DList list;
        SmallHash hash;
        SmallSet set;
        SmallZSet zset;
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

    static RObj ofInt(long v) @nogc nothrow
    {
        RObj o;
        o.type = ObjType.str;
        o.str = StrVal.ofInt(v);
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
            c.str = str.dup();
            break;
        case ObjType.list:
            foreach (v; list)
                c.list.pushBack(v);
            break;
        case ObjType.hash:
            foreach (i; 0 .. hash.capacity)
            {
                if (!hash.slotLive(i))
                    continue;
                auto f = hash.keyAt(i);
                c.hash.set(f, hash.valAt(i).dup());
                immutable ttl = (cast() hash).getFieldTTL(f); // set() cleared it; restore
                if (ttl != 0)
                    c.hash.setFieldTTL(f, ttl);
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
    private alias ExpBucket = Vector!(const(char)[]);
    private Map!(ulong, ExpBucket) expires;

    /// The secondary "sub-expiry" index: container-INTERNAL TTLs (hash fields
    /// today, zset members later). Distinct from `expires`, which keys a whole
    /// key. An entry is tagged by TYPE and names the container by key, registered
    /// at its *nearest* internal deadline; firing it hands the container back to
    /// itself to reap its own expired members (one entry per container, not per
    /// field — the per-field deadlines live inside the container). This keeps the
    /// hot common case (plain key TTL) on the fast `expires` path untouched.
    private struct SubEnt
    {
        ObjType type;
        const(char)[] key; // non-owning slice into Dict-owned key memory
    }

    private alias SubBucket = Vector!SubEnt;
    private Map!(ulong, SubBucket) subExpires;

    @disable this(this);

    ~this() @nogc nothrow
    {
        freeExpiresIndex();
    }

    @property size_t length() const @nogc nothrow
    {
        return d.length;
    }

    /// Record that `k` expires at absolute `at` (ms); no-op for at == 0. Stashes
    /// the key's slot in its bucket on the object, so removing it from the bucket
    /// is O(1) (disarm is then just the O(log n) tree lookup, not O(bucket)).
    void armExpire(scope const(char)[] k, ulong at) @nogc nothrow
    {
        if (!gActiveExpire || at == 0)
            return;
        auto o = d.get(k);
        if (o is null)
            return; // not in the keyspace, nothing to expire
        auto sk = d.storedKey(k); // the Dict's own stable key bytes — no dup
        auto bucket = expires.getOrPut(at, ExpBucket.init); // one descent
        o.expireSlot = cast(uint) bucket.length; // its position, for O(1) removal
        bucket.put(sk); // non-owning slice into Dict-owned memory
    }

    /// Remove `k`'s entry from its deadline bucket. O(log n) to find the bucket
    /// (the RB-tree lookup), then O(1) to remove within it: swap the tail into the
    /// key's stored slot (fixing up the moved key's slot) and pop — no O(bucket)
    /// scan. Drops the bucket node when it empties. No-op if the entry isn't there
    /// (e.g. armed while active expiry was off, then toggled on) — byte-compare guards it.
    void disarmExpire(scope const(char)[] k, ulong at) @nogc nothrow
    {
        if (!gActiveExpire || at == 0)
            return;
        auto o = d.get(k);
        if (o is null)
            return; // key already gone; a stale bucket entry self-heals in the sweep
        auto bucket = expires.get(at);
        if (bucket is null)
            return;
        immutable slot = o.expireSlot;
        if (slot >= bucket.length || (*bucket)[slot] != k)
            return; // not actually indexed here
        immutable last = bucket.length - 1;
        if (slot != last)
        {
            auto moved = (*bucket)[last];
            (*bucket)[slot] = moved;
            if (auto mo = d.get(moved)) // the relocated key learns its new slot
                mo.expireSlot = cast(uint) slot;
        }
        bucket.popBack();
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
        if (!gActiveExpire || gImportMode || expires.empty)
            return 0; // import mode pauses active expiry (bulk-load window)
        immutable now = detNow();
        size_t dropped = 0;
        // removeRight is a consuming range: it drains every deadline <= now in one
        // pass, yielding each bucket live just before its node is dropped.
        foreach (e; expires.removeRight(now))
        {
            foreach (j; 0 .. e.value.length)
            {
                auto key = e.value[j];
                auto obj = d.get(key);
                if (obj !is null && obj.expireAtMs == e.key) // still the live TTL
                {
                    notifyKeyspaceEvent(NClass.expired, "expired", key); // copies before d.del frees it
                    d.del(key);
                    dropped++;
                    gExpiredKeys++;
                }
                // key is a non-owning slice into Dict memory — nothing to free
            }
        }
        return dropped;
    }

    // ----- secondary index: container-internal (hash-field) active expiry -----

    /// Register container `k` (of type `t`) at its nearest internal deadline `at`.
    /// One entry per container; the object remembers its bucket slot for O(1)
    /// removal (mirrors `armExpire`). No-op when active expiry is off.
    void armSubExpire(scope const(char)[] k, ObjType t, ulong at) @nogc nothrow
    {
        if (!gActiveExpire || at == 0)
            return;
        auto o = d.get(k);
        if (o is null)
            return;
        auto sk = d.storedKey(k); // stable Dict-owned key bytes — no dup
        auto bucket = subExpires.getOrPut(at, SubBucket.init);
        o.subExpireSlot = cast(uint) bucket.length;
        o.subExpireAt = at;
        bucket.put(SubEnt(t, sk));
    }

    /// Remove `k`'s entry from the sub-expiry index (uses its remembered deadline
    /// and slot; O(log n) tree lookup + O(1) swap-pop). No-op if not indexed.
    void disarmSubExpire(scope const(char)[] k) @nogc nothrow
    {
        if (!gActiveExpire)
            return;
        auto o = d.get(k);
        if (o is null)
            return;
        immutable at = o.subExpireAt;
        if (at == 0)
            return;
        auto bucket = subExpires.get(at);
        if (bucket is null)
        {
            o.subExpireAt = 0;
            return;
        }
        immutable slot = o.subExpireSlot;
        if (slot >= bucket.length || (*bucket)[slot].key != k)
        {
            o.subExpireAt = 0;
            return;
        }
        immutable last = bucket.length - 1;
        if (slot != last)
        {
            auto moved = (*bucket)[last];
            (*bucket)[slot] = moved;
            if (auto mo = d.get(moved.key)) // relocated container learns its new slot
                mo.subExpireSlot = cast(uint) slot;
        }
        bucket.popBack();
        if (bucket.length == 0)
            subExpires.remove(at);
        o.subExpireAt = 0;
    }

    /// Re-register `k` at a new nearest internal deadline `newAt` (0 = none left).
    /// Used after a HEXPIRE/HPERSIST changes a hash's minimum field deadline.
    void retimeSubExpire(scope const(char)[] k, ObjType t, ulong newAt) @nogc nothrow
    {
        if (!gActiveExpire)
            return;
        auto o = d.get(k);
        if (o is null || o.subExpireAt == newAt)
            return;
        disarmSubExpire(k);
        armSubExpire(k, t, newAt);
    }

    /// Active sub-expiry: for every container whose nearest internal deadline has
    /// passed, hand it back to itself to reap its own expired members, then
    /// re-register at its new nearest deadline (or drop the key if it emptied).
    /// Same self-heal rule as `activeExpireCycle`: a bucket entry only fires when
    /// the container's *live* registration still equals it.
    size_t activeSubExpireCycle() @nogc nothrow
    {
        if (!gActiveExpire || gImportMode || subExpires.empty)
            return 0; // import mode pauses active expiry
        immutable now = detNow();
        size_t reaped = 0;
        foreach (e; subExpires.removeRight(now))
        {
            foreach (j; 0 .. e.value.length)
            {
                auto ent = e.value[j];
                auto o = d.get(ent.key);
                if (o is null || o.subExpireAt != e.key)
                    continue; // stale/relocated entry self-heals
                o.subExpireAt = 0; // this registration is now consumed
                final switch (ent.type)
                {
                case ObjType.hash:
                    immutable n = o.hash.reapExpired(now);
                    if (n > 0)
                    {
                        reaped += n;
                        gExpiredFields += n;
                        notifyKeyspaceEvent(NClass.hash, "hexpired", ent.key);
                    }
                    if (o.hash.length == 0)
                    {
                        disarmExpire(ent.key, o.expireAtMs);
                        d.del(ent.key);
                        notifyKeyspaceEvent(NClass.generic, "del", ent.key);
                    }
                    else if (immutable nextAt = o.hash.minFieldTTL()) // re-register at the new nearest
                        armSubExpire(ent.key, ObjType.hash, nextAt);
                    break;
                case ObjType.str:
                case ObjType.list:
                case ObjType.set:
                case ObjType.zset:
                case ObjType.stream:
                    break; // only hashes carry internal TTLs today
                }
            }
        }
        return reaped;
    }

    private void freeExpiresIndex() @nogc nothrow
    {
        // buckets hold non-owning slices into Dict key memory — only the tree
        // and its bucket arrays need releasing
        expires.clear();
        subExpires.clear();
    }

    /// Live object or null — lazily drops the key when its TTL has passed.
    // touch=false skips the LRU/LFU bump — OBJECT ENCODING/FREQ/IDLETIME must not
    // count as an access (Redis's LOOKUP_NOTOUCH).
    RObj* lookup(scope const(char)[] k, bool touch = true) @nogc nothrow
    {
        auto o = d.get(k);
        if (o is null)
            return null;
        if (o.expired() && !gImportMode) // import mode: bulk-load window, no expiry
        {
            disarmExpire(k, o.expireAtMs);
            disarmSubExpire(k);
            d.del(k);
            notifyKeyspaceEvent(NClass.expired, "expired", k);
            gExpiredKeys++;
            return null;
        }
        // Lazy field expiry: reap any expired hash fields before the key is used
        // (the per-field analog of the key-level check above). The absolute field
        // deadlines are replicated via HPEXPIREAT, so every node reaps off its own
        // copy — no HDEL is propagated, exactly like key TTL.
        if (o.type == ObjType.hash && o.hash.hasFieldTTL && !gImportMode)
        {
            immutable reaped = o.hash.reapExpired(detNow());
            if (reaped > 0)
            {
                gExpiredFields += reaped;
                notifyKeyspaceEvent(NClass.hash, "hexpired", k);
                if (o.hash.length == 0)
                {
                    disarmExpire(k, o.expireAtMs);
                    disarmSubExpire(k);
                    d.del(k);
                    notifyKeyspaceEvent(NClass.generic, "del", k);
                    return null;
                }
            }
        }
        if (touch)
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

    /// SET of an int-encoded value (INCR on a missing key).
    void setInt(scope const(char)[] k, long v) @nogc nothrow
    {
        d.set(k, RObj.ofInt(v));
    }

    bool del(scope const(char)[] k) @nogc nothrow
    {
        // an already-expired key must read as missing, not as deleted-now
        auto o = lookup(k);
        if (o is null)
            return false;
        disarmExpire(k, o.expireAtMs); // drop its pending drop-soon entry
        disarmSubExpire(k); // and any sub-expiry (hash field) registration
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
        // a hash's field-TTL registration must move with it (its bucket entry
        // points at `from`'s key bytes, which d.steal frees)
        immutable bool subHash = src.type == ObjType.hash && src.hash.hasFieldTTL;
        immutable subAt = subHash ? src.hash.minFieldTTL() : 0;
        disarmExpire(from, ttl);
        disarmSubExpire(from);
        if (auto dst = lookup(to)) // overwriting a live destination drops its TTL entries
        {
            disarmExpire(to, dst.expireAtMs);
            disarmSubExpire(to);
        }
        RObj obj;
        d.steal(from, obj);
        obj.subExpireAt = 0; // stale slot from the old registration; re-armed below
        d.set(to, obj);
        armExpire(to, ttl);
        if (subAt != 0)
            armSubExpire(to, ObjType.hash, subAt);
        return true;
    }

    /// Containers left empty by removals disappear, like in Redis.
    /// Streams are the exception: an empty stream keeps existing (lastId lives on).
    void delIfEmpty(scope const(char)[] k, const(RObj)* o) @nogc nothrow
    {
        if (o.type != ObjType.str && o.type != ObjType.stream && o.containerLen == 0)
        {
            disarmExpire(k, o.expireAtMs);
            disarmSubExpire(k);
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
    char[24] sb = void;
    assert(o !is null && o.type == ObjType.str && o.str.bytes(sb) == "hello");
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
