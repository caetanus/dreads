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
import emplace.hashmap : HashMap;
import dreads.alloc : KeyspaceAllocator;
import dreads.lazyfree : BatchSink, LazyFree;
import emplace.vector : Vector;
import std.experimental.allocator.mallocator : Mallocator;

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

/// Background (timer) eviction: opt-in like gActiveExpire (default off — the
/// write path already frees on demand; the cycle only matters for the Redis
/// contract that a key can be evicted without a subsequent write). INFO stats
/// `evicted_keys` counts keys dropped by maxmemory pressure (write path + cycle).
public __gshared bool gActiveEviction;
public __gshared ulong gEvictedKeys;

/// INFO stats `expired_fields`: hash fields dropped by TTL (lazy/active reap or a
/// past-deadline HEXPIRE/HGETEX/HSETEX). The per-field analog of gExpiredKeys.
public __gshared ulong gExpiredFields;

/// Import mode (`CONFIG SET import-mode yes` + `CLIENT IMPORT-SOURCE ON`): a
/// bulk-load window for migration tools. While on, expiration is PAUSED so a
/// stream of RESTOREs with absolute TTLs (some already past) loads consistently
/// instead of racing the expiry cycle. Turned off, normal expiry resumes.
public __gshared bool gImportMode;

/// The CURRENT command's client is in `CLIENT IMPORT-SOURCE ON` — set per command
/// by the serve loop (mirrors gRespProto/gCmdConn). Only such a client may VISIT
/// an expired-but-kept key while import-mode is on; normal clients see it as gone.
public __gshared bool gImportSourceActive;

/// INFO clients: clients currently parked in a blocking wait (B*POP etc.).
/// Also counts clients parked by a CLIENT PAUSE barrier (Valkey counts postponed
/// clients as blocked), so `wait_for_blocked_clients_count` sees a paused client.
public __gshared long gBlockedClients;

/// INFO clients: number of live client connections. TLS (not __gshared) on
/// purpose — with the coming per-thread sharding each shard accepts and serves
/// its own connections, so this is a per-thread tally that INFO aggregates;
/// a shared counter would both race and need migrating. See register/unregister.
public long gConnectedClients;

/// CLIENT TRACKING invalidation hook: when a key is physically removed by an
/// expiry (active or lazy), the server layer queues a tracking invalidation for
/// it. Null until the server installs it (so obj.d stays independent of tracking).
public __gshared void function(scope const(char)[] key) nothrow @nogc gTrackInvalidateHook;

/// Master-authoritative expiry (avoids the ontological fork: two nodes with
/// conflicting truth about whether a key is alive). A key's DELETION is a side
/// effect that ONLY the master produces — the data layer never reaps a key on
/// its own initiative under replication. When `lookup`/`activeExpireCycle` find
/// `key` (in db `db`) past its deadline, they ask this hook how its removal
/// propagates and whether to delete it locally NOW:
///   true  = delete locally now (standalone; OR deterministic apply re-execution
///           where every node reaps identically under the injected clock)
///   false = do NOT delete — the leader proposed a DEL that will delete it on
///           commit (the single agreed truth), or this is a follower that must
///           never self-expire. The key reads as nil meanwhile (the view), but
///           its physical death waits for the committed DEL.
/// Null (unit tests / pre-install) = behave as before: delete locally.
public alias ExpireReapHook = bool function(scope const(char)[] key, ubyte db) @nogc nothrow;
public __gshared ExpireReapHook gExpireReapHook;

/// Set by the replicator around apply of a committed entry. When true, a lazy
/// expiry is happening during DETERMINISTIC re-execution (the injected clock is
/// frozen to the entry's stamp), so every node reaps identically and the delete
/// is local with no new DEL to propagate — reading it lets the reap hook tell a
/// deterministic apply apart from a live, node-local read.
public __gshared bool gApplying;

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

/// Stamp each slot with its identity at startup, so `gDbs[i].db == i`.
shared static this()
{
    foreach (int i, ref d; gDbs)
        d.db = i;
}

/// Global off-loop free worker (UNLINK / lazyfree-lazy-server-del). Null unless the
/// server enabled it at startup; when null, deletes free inline.
__gshared LazyFree gLazyFree;

/// Only offload a value this big or bigger — below it the scattered chase fits in
/// cache and the thread-handoff fixed cost isn't worth it (bench: the win grows with
/// size, 1.5x at 100k nodes to 7.6x at 1M). Tunable; validated by the UNLINK bench.
enum size_t LAZYFREE_MIN_ELEMS = 512;

/// GatherFn (runs on the free-thread) for a DETACHED RObj wrapper (Mallocator-
/// allocated by `Keyspace.unlink`). Gathers the value's backing blocks — the
/// scattered chase — into `sink` for the loop to deallocate, then frees the wrapper
/// STRUCT itself (Mallocator is thread-safe; the element blocks belong to the loop).
void gatherRObjLazy(void* ctx, ref BatchSink sink) @nogc nothrow @trusted
{
    auto ro = cast(RObj*) ctx;
    ro.gatherBlocks((void* p, size_t s) @nogc nothrow => sink.add(p, s));
    Mallocator.instance.deallocate((cast(void*) ro)[0 .. RObj.sizeof]);
}

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

    /// Off-loop free (lazyfree) HEURISTIC: offload pays ONLY for a value whose
    /// teardown is a DEPENDENT-POINTER CHASE — list next-ptr, zset skiplist
    /// forward-ptr — a serial scattered traversal the free-thread absorbs off the
    /// loop. Hash / set / stream traverse a CONTIGUOUS index (slots / entry array);
    /// their scattered cost is in the per-leaf deallocate, which can't leave the
    /// single-writer loop, so offloading them LOSES (bench/lazyfree_bench.d: CHAIN
    /// ~7.4x vs INDEX ~0.9x). Those free inline. The size gate (caller) then ensures
    /// the chase is long enough to beat the thread-handoff fixed cost.
    bool lazyFreeable() const @nogc nothrow
    {
        return type == ObjType.list || type == ObjType.zset;
    }

    /// Record every backing block of this value via `add`, freeing NOTHING (mirrors
    /// each type's free() exactly — a miss leaks, a double-add double-frees). Only
    /// lazyFreeable() types are handled; the caller MUST gate on lazyFreeable().
    void gatherBlocks(scope void delegate(void*, size_t) @nogc nothrow add) @nogc nothrow
    {
        switch (type)
        {
        case ObjType.list:
            list.gatherBlocks(add);
            break;
        case ObjType.zset:
            zset.gatherBlocks(add); // skiplist nodes (the chase) + scores dict
            break;
        default:
            break; // not offloaded; gated out in the caller (lazyFreeable())
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
                immutable ttl = hash.getFieldTTL(f); // set() cleared it; restore (getFieldTTL is const)
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
            c.stream = stream.dup(); // deep copy: entries + metadata + groups/PEL
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
    // The keyspace's key->value table routes through KeyspaceAllocator (the
    // swappable region/freelist composition), not the default Mallocator, so the
    // data plane's backing is a build-time choice. (Dict = HashMap; we spell the
    // allocator here rather than change the global Dict alias, which also serves
    // non-keyspace maps — the conn registry, scripts.)
    HashMap!(RObj, KeyspaceAllocator) d;

    /// This keyspace's logical database index — its SLOT identity in `gDbs`, set
    /// once at startup. It is the transparent way to get the db number (e.g. for a
    /// `__keyspace@<db>__` channel) without recovering it by pointer arithmetic
    /// (`&ks - &gDbs[0]`). SWAPDB swaps slot *contents*, not identities, so it
    /// restores `db` afterwards. A standalone Keyspace (unit tests) keeps db 0.
    int db;

    /// The "drop-soon" index for active expiration: absolute deadline (ms) ->
    /// the keys that expire at exactly that instant. Ordered by deadline, so a
    /// sweep visits only the buckets that have come due (`bisect_right(now)` =
    /// `foreachRange(0, now)`), never the whole keyspace. It owns its own copies
    /// of the key strings. Purely local reclamation: the deadline is already
    /// replicated as an absolute PEXPIREAT, so every node/replay expires
    /// deterministically off its own copy — no DEL is propagated.
    private alias ExpBucket = Vector!(const(char)[], KeyspaceAllocator);
    private Map!(ulong, ExpBucket, KeyspaceAllocator) expires;

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

    private alias SubBucket = Vector!(SubEnt, KeyspaceAllocator);
    private Map!(ulong, SubBucket, KeyspaceAllocator) subExpires;

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
        auto bucket = expires.emplace(at); // one descent, bucket built in place
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
    /// Max keys reaped per cycle. Bounds the loop-blocking cost so a big backlog
    /// (e.g. an import-mode window closing on a million past-deadline keys) drains
    /// over several 200ms ticks instead of stalling the event loop in one pass —
    /// lazy expiry still hides the not-yet-reaped ones on access. Redis budgets the
    /// same way (a per-cycle sample cap). The tree op is already O(1)-amortised;
    /// the irreducible cost is the per-key d.del + "expired" notify, so we cap that.
    enum size_t ACTIVE_EXPIRE_BUDGET = 20_000;

    size_t activeExpireCycle() @nogc nothrow
    {
        if (!gActiveExpire || gImportMode || expires.empty)
            return 0; // import mode pauses active expiry (bulk-load window)
        immutable now = detNow();
        size_t dropped = 0;
        // removeRight is a consuming range: it drains every deadline <= now in one
        // pass, yielding each bucket live just before its node is dropped. The
        // budget check sits at the top of each bucket, so a bucket is never left
        // half-drained — processed buckets are gone, the rest wait for the next tick.
        foreach (e; expires.removeRight(now))
        {
            if (dropped >= ACTIVE_EXPIRE_BUDGET)
                break;
            foreach (j; 0 .. e.value.length)
            {
                auto key = e.value[j];
                auto obj = d.get(key);
                if (obj !is null && obj.expireAtMs == e.key) // still the live TTL
                {
                    // Master-authoritative: standalone reaps locally; a raft
                    // leader proposes a DEL (the hook) and lets the commit remove
                    // it on every node. (This cycle only runs on the leader/
                    // standalone — the server gates the call — so a follower never
                    // self-expires.) `dropped` counts it either way: the work
                    // (delete or propose) is done and the index entry consumed.
                    immutable reapNow = gExpireReapHook is null ? true
                        : gExpireReapHook(key, cast(ubyte) db);
                    if (reapNow)
                    {
                        notifyKeyspaceEvent(NClass.expired, "expired", key); // copies before d.del frees it
                        if (gTrackInvalidateHook !is null)
                            gTrackInvalidateHook(key); // invalidate client-side caches
                        d.del(key);
                        gExpiredKeys++;
                    }
                    dropped++;
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
        auto bucket = subExpires.emplace(at); // bucket built in place
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
        if (o.expired())
        {
            if (!gImportMode)
            {
                // A CLIENT PAUSE holds the expiry DEL (a replicated write): the key
                // reads as logically expired (null) but is NOT deleted/counted until
                // the window lifts and a later access reaps it. gPauseUntilMs is
                // runtime-only (never logged), so raft replay is unaffected.
                if (gPauseUntilMs != 0 && detNow() < gPauseUntilMs)
                    return null;
                // Master-authoritative: only the master produces the DELETE side
                // effect. On a raft follower — or on a leader's live read — the
                // key reads as nil now but its physical removal waits for the
                // committed DEL, so no node forks the truth about its life.
                immutable reapNow = gExpireReapHook is null ? true
                    : gExpireReapHook(k, cast(ubyte) db);
                if (reapNow)
                {
                    disarmExpire(k, o.expireAtMs);
                    disarmSubExpire(k);
                    d.del(k);
                    notifyKeyspaceEvent(NClass.expired, "expired", k);
                    if (gTrackInvalidateHook !is null)
                        gTrackInvalidateHook(k); // invalidate client-side caches
                    gExpiredKeys++;
                }
                return null;
            }
            // Import window: the expired key is physically KEPT (bulk-load
            // consistency), but only a client in `CLIENT IMPORT-SOURCE ON` may
            // VISIT it — a normal client still sees it as gone (nil / ttl -2).
            if (!gImportSourceActive)
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

    /// Fire the `new` keyevent for a key that just came into existence (Redis's
    /// dbAdd hook). `d.set` returns true only on a genuine insert, so this is free
    /// on overwrites and — since the class bit is off by default — on the hot path.
    private void dbAdd(scope const(char)[] k) @nogc nothrow
    {
        notifyKeyspaceEvent(NClass.newkey, "new", k);
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
        if (d.set(k, RObj.empty(t)))
            dbAdd(k);
        return d.get(k);
    }

    /// SET semantics: overwrites whatever type currently holds k.
    void setStr(scope const(char)[] k, scope const(char)[] v) @nogc nothrow
    {
        if (d.set(k, RObj.ofStr(v)))
            dbAdd(k);
    }

    /// SET of an int-encoded value (INCR on a missing key).
    void setInt(scope const(char)[] k, long v) @nogc nothrow
    {
        if (d.set(k, RObj.ofInt(v)))
            dbAdd(k);
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

    /// Delete `k`, freeing a big SCATTERED value OFF the event loop (UNLINK) when
    /// lazyfree is enabled and the value is large + offloadable; otherwise a plain
    /// synchronous del. The value is moved out of the table into a stable
    /// Mallocator wrapper (so it survives the table slot's reuse), then handed to
    /// the free-thread which chases + gathers its blocks for the loop to deallocate.
    bool unlink(scope const(char)[] k) @nogc nothrow
    {
        auto o = lookup(k);
        if (o is null)
            return false;
        disarmExpire(k, o.expireAtMs);
        disarmSubExpire(k);
        if (gLazyFree !is null && o.lazyFreeable() && o.containerLen() >= LAZYFREE_MIN_ELEMS)
        {
            // move the value into a stable heap home, remove the table entry WITHOUT
            // freeing it (steal), then offload the teardown. enqueue() falls back to
            // an inline free (on the loop) if its ring is momentarily full.
            auto w = cast(RObj*) Mallocator.instance.allocate(RObj.sizeof).ptr;
            d.steal(k, *w);
            gLazyFree.enqueue(&gatherRObjLazy, w);
            return true;
        }
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

@nogc nothrow unittest // master-authoritative expiry: a follower NEVER self-reaps
{
    import dreads.det : gClock;

    Keyspace ks;
    scope (exit)
    {
        ks.d.free();
        gClock = 0;
        gExpireReapHook = null;
    }
    gClock = 1_000_000;
    ks.setStr("k", "v");
    ks.lookup("k").expireAtMs = 900_000; // already past "now"
    ks.armExpire("k", 900_000);

    // FOLLOWER: the reap hook says "do not delete" — the key reads as nil (the
    // view) but stays PHYSICALLY present, waiting for the leader's committed DEL.
    // No node forks the truth about whether the key is alive. (Old behaviour, and
    // a broken fix, would reap it here — this is the regression guard.)
    static bool followerHook(scope const(char)[] key, ubyte db) @nogc nothrow
    {
        return false;
    }

    gExpireReapHook = &followerHook;
    assert(ks.lookup("k") is null); // logically expired -> nil
    assert(ks.d.get("k") !is null); // but NOT reaped locally (the follower rule)

    // LEADER / standalone: the hook says "delete" — lookup reaps it in place.
    static bool leaderHook(scope const(char)[] key, ubyte db) @nogc nothrow
    {
        return true;
    }

    gExpireReapHook = &leaderHook;
    assert(ks.lookup("k") is null); // nil view
    assert(ks.d.get("k") is null); // and now physically gone

    // active cycle honours the same rule: a follower's cycle drops nothing.
    gActiveExpire = true; // must be on for armExpire to register the deadline
    scope (exit)
        gActiveExpire = false;
    ks.setStr("k2", "v");
    ks.lookup("k2").expireAtMs = 900_000;
    ks.armExpire("k2", 900_000);
    gExpireReapHook = &followerHook;
    assert(ks.activeExpireCycle() == 1); // processed (index consumed)...
    assert(ks.d.get("k2") !is null); // ...but NOT physically reaped (follower)
}

@nogc nothrow unittest // active-expire budget: a big backlog drains over cycles, not one stall
{
    import core.stdc.stdio : snprintf;
    import dreads.det : gClock;

    Keyspace ks;
    scope (exit)
    {
        ks.d.free();
        gClock = 0;
        gActiveExpire = false;
    }
    gActiveExpire = true;
    gClock = 10_000_000;
    enum size_t N = Keyspace.ACTIVE_EXPIRE_BUDGET + 5000; // a backlog past the cap
    foreach (i; 0 .. N)
    {
        char[16] kb = void;
        immutable n = snprintf(kb.ptr, kb.length, "k%zu", i);
        auto k = kb[0 .. n];
        ks.setStr(k, "v");
        immutable ulong dl = 1000 + i; // distinct deadlines, all already past
        ks.lookup(k).expireAtMs = dl;
        ks.armExpire(k, dl);
    }
    assert(ks.length == N);
    // one cycle reaps exactly the budget; the rest stay (lazy expiry hides them)
    assert(ks.activeExpireCycle() == Keyspace.ACTIVE_EXPIRE_BUDGET);
    assert(ks.length == 5000);
    // the next cycle drains the remainder
    assert(ks.activeExpireCycle() == 5000);
    assert(ks.length == 0);
}

@("obj.unlink_offloads_big_list_to_lazyfree")
unittest // UNLINK hands a big list to the free-thread; small ones free inline
{
    import dreads.lazyfree : LazyFree;
    import core.thread : Thread;
    import core.time : msecs;

    gLazyFree = new LazyFree();
    scope (exit)
    {
        gLazyFree.stop();
        gLazyFree = null;
    }

    Keyspace ks;
    scope (exit)
        ks.d.free();
    bool wrong;

    // a small list is BELOW the threshold → freed inline (never offloaded)
    foreach (i; 0 .. 8)
        ks.getOrCreate("small", ObjType.list, wrong).list.pushBack("x");
    assert(ks.unlink("small"));
    assert(ks.length == 0);
    assert(gLazyFree.statSubmitted == 0); // inline, not queued

    // a big list is offloaded: removed from the table at once, torn down off-loop
    enum size_t NEL = LAZYFREE_MIN_ELEMS + 100;
    foreach (i; 0 .. NEL)
        ks.getOrCreate("big", ObjType.list, wrong).list.pushBack("payload-bytes");
    assert(ks.length == 1);
    assert(ks.unlink("big"));
    assert(ks.length == 0); // gone from the keyspace immediately
    assert(ks.lookup("big") is null);
    assert(gLazyFree.statSubmitted == 1); // went off-loop

    // drain what the free-thread gathered, as the server tick would
    foreach (_; 0 .. 3000)
    {
        if (gLazyFree.reclaimPending)
            gLazyFree.drainReclaimed();
        if (gLazyFree.statReclaimedBlocks >= NEL)
            break;
        Thread.sleep(1.msecs);
    }
    assert(gLazyFree.statReclaimedBlocks == NEL); // every node block freed on the loop
}

@("obj.unlink_offloads_big_zset_no_leak")
unittest // UNLINK offloads a big sorted set (skiplist chase); every block reclaimed
{
    import dreads.lazyfree : LazyFree;
    import dreads.alloc : keyspaceBytesUsed;
    import core.thread : Thread;
    import core.time : msecs;
    import core.stdc.stdio : snprintf;

    gLazyFree = new LazyFree();
    scope (exit)
    {
        gLazyFree.stop();
        gLazyFree = null;
    }

    Keyspace ks;
    scope (exit)
        ks.d.free();
    bool wrong;
    ks.setStr("dummy", "x");
    ks.del("dummy"); // force the keyspace table to allocate so `base` is stable
    immutable base = keyspaceBytesUsed();

    // > LAZYFREE_MIN_ELEMS and past the listpack spill threshold → skiplist-backed
    enum size_t NEL = 600;
    foreach (i; 0 .. NEL)
    {
        char[16] mb = void;
        immutable m = snprintf(mb.ptr, mb.length, "m%zu", i);
        ks.getOrCreate("z", ObjType.zset, wrong).zset.add(cast(double) i, mb[0 .. m]);
    }
    assert(ks.getOrCreate("z", ObjType.zset, wrong).zset.length == NEL);
    assert(keyspaceBytesUsed() > base);

    assert(ks.unlink("z"));
    assert(ks.length == 0);
    assert(gLazyFree.statSubmitted == 1); // a zset is a dependent chase → offloaded

    // drain until every block is back; exact return to base proves gatherBlocks
    // mirrors free() with no missed block (leak) and no double-add (double-free).
    foreach (_; 0 .. 4000)
    {
        if (gLazyFree.reclaimPending)
            gLazyFree.drainReclaimed();
        if (keyspaceBytesUsed() == base)
            break;
        Thread.sleep(1.msecs);
    }
    assert(keyspaceBytesUsed() == base);
}
