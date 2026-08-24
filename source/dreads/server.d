module dreads.server;

// TCP front-end on vibe-core (fiber per connection, single-threaded event
// loop) feeding the @nogc data plane: ByteBuffer I/O staging, zero-copy RESP
// parsing with a per-connection Arena, and command dispatch against the typed
// keyspace. Non-subscriber connections write replies synchronously (the hot
// path). Once a connection subscribes it flips to an async output queue drained
// by a dedicated writer fiber: the publisher never blocks on a slow subscriber's
// socket, output stays ordered (replies and messages share the one queue), and
// the bounded queue drops on overflow (pub/sub is fire-and-forget). See PUBSUB.md.
// vibe-core owns only the socket lifecycle; nothing here allocates on the GC
// heap per request (the mutex and TCPConnection are one-time per connection).

import core.builtins : expect;
import core.stdc.stdio : printf;

import core.time : seconds, msecs;

import vibe.core.core : runEventLoop, runTask, setTimer, yield;
import vibe.core.net : TCPConnection, connectTCP, listenTCP, TCPListenOptions;
import vibe.core.stream : IOMode;
import vibe.core.sync : LocalManualEvent, TaskMutex, createManualEvent;
import vibe.core.task : Task;

import dreads.acl : AclUser, aclUser, aclInit, aclCheckPassword, aclGetOrCreate,
    aclApplyRule, aclDelUser, aclEachUser, aclCatNames, aclCmdIndex, aclCanRunCmd,
    aclUnrestricted, aclDescribeCommands, aclDescribeKeys, aclDescribeChannels,
    aclEncodeCanonicalSetuser, aclApplyCanonical, aclCanAccessChannel, aclKeyDenied,
    aclDeniedKey, aclCanRunCmdSub, aclCmdHasSubRule, aclIsContainer, aclLogAdd,
    aclLogReset, aclLogCount, aclLogAt, gAclLogMaxLen, gAclActive, aclDeniedDb,
    aclCanAccessDb, aclCanAccessKey, forEachCommandKey;
import dreads.aclcat : cmdIx, gCmdCats;
import dreads.authpw : initAuthPw;
import dreads.aof : Aof, aofLoad, aofRewrite;
import dreads.commands : dispatch, globMatch, isWriteCommand, isPausedByWrite,
    cmdWriteByIdx, cmdDenyOomByIdx, gScriptWritesHook, propagationOverride, parseLong, gWriteNoOp;
import dreads.shard : sharded, myKeyspace, ShardPending;
import vibe.core.taskpool : TaskPool;
import dreads.stats : gTotalErrorReplies, statErrorReply, resetErrorStats,
    gCmdStats, CmdStat, statCall, statRejected, resetCmdStats;

// A command held by a WRITE-mode CLIENT PAUSE: the may-replicate write set, plus
// EVAL/EVALSHA/FCALL only when the script actually may write (the scripting hook
// reads the shebang / function flags; read-only scripts pass the barrier).
private bool heldByWritePause(scope const(char)[] uname, const ref RVal cmd) @nogc nothrow
{
    if (uname == "EVAL" || uname == "EVALSHA" || uname == "FCALL")
        return gScriptWritesHook !is null && gScriptWritesHook(uname, cmd);
    return isPausedByWrite(uname);
}
import dreads.config : applyDirective, gConfig, isRuntimeSettable, isCompatModeParam, parseMemory;
import dreads.mem : Arena, ByteBuffer;
import dreads.alloc : ConnAllocator;
import emplace.vector : Vector;
import emplace.smartptr : Shared, Weak;
import dreads.notify : flushPendingNotify, gNotifyFlags, gNotifyDb;
import dreads.stream : nowMs;
import dreads.obj : Keyspace, gDbs, NUM_DBS, RESP_DBS,
    ObjType, gBlockedClients, gConnectedClients,
    gImportSourceActive;
import dreads.dict : Dict, Unit;
import dreads.pubsub : PubSub, Subscriber, RcMsg, rcFromBytes, rcData, rcRetain, rcRelease, rcAsPush,
    pubsubTapArm, pubsubTapDrain, pubsubTapExpire, pubsubTapPending;
import dreads.replicator : gReplicator;
import dreads.resp;
import dreads.scripting : cachedScript, evalCommand, scriptCommand, scriptSetPendingUser;

private enum READ_CHUNK = 16 * 1024;

// The event loop is single-threaded, so shared state needs no locking.
// The logical databases live in `gDbs` (dreads.obj); client commands dispatch
// against the *connection's* selected db (`Conn.dbp`). `gKeys` is just an alias
// for db 0 (`gDbs[0]`), used where db 0 is genuinely the starting point (the AOF
// replay begins there and follows `SELECT` markers into the other dbs). The
// persistence (AOF live/rewrite/replay + raft snapshot), eviction, blocking, and
// keyspace-notification paths are ALL multi-db now.
private ref Keyspace gKeys() @property @nogc nothrow @trusted
{
    return gDbs[0];
}

// THREAD-LOCAL (share-nothing rule): a subscriber's sink writes into its Conn's
// buffers, which only that conn's thread may touch — a __gshared registry would
// make every cross-shard PUBLISH a cross-thread buffer write (UB) on top of the
// unsynchronized table itself. v1 semantics: pub/sub is SHARD-LOCAL — subscriber
// and publisher must land on the same shard thread (SHARDING.md gap; the 2b fix
// is PUBLISH fan-out through the SPSC hop, delivering same-thread on each shard).
// Unsharded (shards=1) every conn lives on the main thread — identical to before.
private PubSub gPubSub;
private PubSub gShardPubSub; // single node: shard = plain, own namespace
// AOF-per-shard (phase 2.6): one file per shard, owned and appended by that
// shard's thread ONLY (share-nothing — a hopped write logs on its OWNER's file,
// which is also what makes per-key ordering correct by construction: a key has
// exactly one owner). Shard 0 keeps the plain configured filename, shards 1..
// N-1 use `<name>.<i>`; a `<name>.shards` sidecar records the layout so a boot
// under a DIFFERENT shard count re-routes every command through the live
// router (aofLoadSharded) and compacts fresh per-shard files.
private __gshared Aof[] gAofs;
private __gshared Aof gAofFallback; // pre-boot / unit tests: a disabled sink
private __gshared const(char)[] gAofPath;

// Boot-time AOF setup (phase 2.6): discover the per-shard files, replay them
// into the per-shard keyspaces, open the writers, and — when the shard count
// CHANGED since the files were written (the `<path>.shards` sidecar) — re-route
// every command through the live router and compact fresh per-shard files.
//
// ALLOCATOR CONTRACT: runs on the main thread BEFORE any shard thread exists.
// Each replay pass sets gAllocShard to the target shard so every stored value
// comes from the allocator its future owner thread will free into (a foreign
// block is the cross-allocator SIGSEGV class). Scratch lives inside each
// aofLoadSharded call, under the same slot.
private bool setupAof(const(char)[] path) nothrow
{
    import core.stdc.stdio : fclose, fopen, fprintf, fscanf, remove, snprintf;

    import dreads.alloc : gAllocShard;
    import dreads.aof : aofLoadSharded, aofRewrite;
    import dreads.shard : gShardCount, gShardKs;

    immutable uint n = gShardCount > 1 ? gShardCount : 1;
    Keyspace[] slice(uint i) @trusted nothrow
    {
        return sharded() ? gShardKs[i * NUM_DBS .. (i + 1) * NUM_DBS] : gDbs[];
    }
    // the layout the existing files were written under (absent sidecar = 1)
    uint storedN = 1;
    char[520] sb = void;
    cast(void) snprintf(sb.ptr, sb.length, "%.*s.shards", cast(int) path.length, path.ptr);
    if (auto f = fopen(sb.ptr, "rb"))
    {
        uint v;
        if (fscanf(f, "%u", &v) == 1 && v >= 1 && v <= 1024)
            storedN = v;
        fclose(f);
    }
    immutable bool relayout = storedN != n;
    immutable uint span = storedN > n ? storedN : n; // superset (crash-mid-migration safe)
    char[520] nb = void;
    long total = 0;
    foreach (uint i; 0 .. n)
    {
        gAllocShard = i; // this pass's values belong to shard i's allocator
        scope (exit)
            gAllocShard = 0;
        if (!relayout)
        {
            // layout matches: file i holds exactly shard i's commands
            immutable r = aofLoadSharded(aofFileFor(path, i, nb[]), slice(i), i, n, true);
            if (r < 0)
            {
                printf("dreads: cannot read AOF (shard %u)\n", i);
                return false;
            }
            total += r;
        }
        else
        {
            // shard count changed: every old file may hold keys now owned by
            // ANY shard — route every command through the live router. Globals
            // (ACL) apply once, on the shard-0 pass.
            foreach (uint fsrc; 0 .. span)
            {
                immutable r = aofLoadSharded(aofFileFor(path, fsrc, nb[]), slice(i),
                        i, n, i == 0);
                if (r < 0)
                {
                    printf("dreads: cannot read AOF (%u)\n", fsrc);
                    return false;
                }
                total += r;
            }
        }
    }
    printf("dreads: AOF replayed %lld commands across %u shard(s)%s\n", total, n,
            relayout ? " (re-sharded)".ptr : "".ptr);
    gAofPath = path;
    foreach (uint i; 0 .. n)
        if (!gAofs[i].open(aofFileFor(path, i, nb[])))
        {
            printf("dreads: cannot open AOF for append (shard %u)\n", i);
            return false;
        }
    if (relayout)
    {
        // compact each shard's file down to exactly its own keys, then drop the
        // files of the old layout that no shard owns any more
        foreach (uint i; 0 .. n)
        {
            gAllocShard = i;
            scope (exit)
                gAllocShard = 0;
            if (!aofRewrite(gAofs[i], aofFileFor(path, i, nb[]), slice(i), i == 0))
            {
                printf("dreads: AOF re-shard rewrite failed (shard %u)\n", i);
                return false;
            }
        }
        foreach (uint j; n .. span)
        {
            char[520] zb = void;
            cast(void) snprintf(zb.ptr, zb.length, "%.*s.%u",
                    cast(int) path.length, path.ptr, j);
            cast(void) remove(zb.ptr);
        }
    }
    if (auto f = fopen(sb.ptr, "wb")) // sb still holds the sidecar path
    {
        fprintf(f, "%u\n", n);
        fclose(f);
    }
    return true;
}

// THIS thread's AOF (its shard's own file; slot 0 when unsharded). The slot
// is resolved ONCE per thread and cached in TLS — this sits on the per-write
// tail, where the array indexing measured +14 instr/op.
private Aof* tAof;

private ref Aof myAof() @nogc nothrow @trusted
{
    import dreads.shard : tShard;

    auto a = tAof;
    if (a is null)
    {
        auto arr = gAofs;
        a = arr.length ? &arr[tShard < arr.length ? tShard : 0] : &gAofFallback;
        tAof = a;
    }
    return *a;
}

// The per-shard AOF filename: shard 0 = the configured path (shards=1 compat).
private const(char)[] aofFileFor(return scope const(char)[] base, uint i, return scope char[] buf) @nogc nothrow
{
    import core.stdc.stdio : snprintf;

    if (i == 0)
        return base;
    immutable n = snprintf(buf.ptr, buf.length, "%.*s.%u",
            cast(int) base.length, base.ptr, i);
    return buf[0 .. n];
}
// THREAD-LOCAL (share-nothing rule): WATCH is v1 same-shard, and every write to a
// key executes on its owner's thread (local or hopped) — so the epoch a WATCHer
// compares moves on exactly the thread whose keyspace it watches. As __gshared,
// N threads' unsynchronized `++` lost increments (a lost bump = a missed WATCH
// invalidation = EXEC on stale data).
public ulong gWriteEpoch; // bumped on every effective write (WATCH + INFO changes)
// Monotonic client-id source. `shared` + atomic increment: under thread-per-shard
// every listener thread mints ids, and a plain `++` would hand two conns the same
// id (breaking the no-ABA guarantee CLIENT KILL/UNBLOCK rely on). Connect-time
// only — never on the command path.
private shared ulong gClientIds;
// The connection whose command is executing right NOW (single-thread, set by the
// serve loop around handleCommand). A pub/sub message published to THIS connection
// (publish-to-self) must trail the running command's reply, not interleave before
// it — connSink defers such a message to pendingInval (drained after outb). Null
// between commands so an ordinary cross-client delivery goes straight to the queue.
// THREAD-LOCAL (share-nothing rule): "the conn running THIS thread's current
// command" is inherently per-thread state — as __gshared, shard threads scribbled
// each other's publish-to-self detection every command.
private Conn* gCmdConn;
// MONITOR feed: the SET of monitor conn ids (not raw Conn*), resolved to a strong
// lock via connById at feed time — so a monitor freed cross-fiber can never dangle.
// Mirrors the CLIENT TRACKING id-set registries (gBcastConns), the Phase-C model.
// THREAD-LOCAL: a monitor watches ITS OWN shard's command stream (same v1 shard
// scope as tConnById, which the feed resolves through anyway); a shared Dict here
// is a rehash double-free across listener threads.
private Dict!Unit gMonitors;

// blocked clients (BLPOP & co.) wake on any write and re-check their keys.
// THREAD-LOCAL: LocalManualEvent is same-thread-only BY CONTRACT — as __gshared
// under shards>1 a cross-thread emit/wait is UB. Blocking is v1 same-shard
// (SHARDING.md): each shard wakes its own blocked conns on its own writes.
private LocalManualEvent gKeyActivity;

// CLIENT PAUSE barrier: while `gPauseUntilMs` is in the future, commands that match
// the mode (ALL, or WRITE-only) are NOT executed — each connection's fiber buffers
// their raw bytes in `Conn.pausedBuf` (barriered) and REPLAYS them once the window
// lifts (timeout or CLIENT UNPAUSE). Transient connection state: never AOF-logged,
// never replicated (raft handles failover; this is for online migration windows).
// gPauseUntilMs / gPauseAll / gPauseIssuer live in dreads.obj (so INFO can read
// them without a module cycle); imported publicly here for the rest of server.d.
public import dreads.obj : gPauseUntilMs, gPauseAll, gPauseIssuer;
// THREAD-LOCAL (with the whole pause-state group below): LocalManualEvent is
// same-thread-only, and CLIENT PAUSE is v1 SHARD-LOCAL — it barriers only the
// conns of the shard the issuer landed on (SHARDING.md gap; whole-server pause
// needs a cross-shard hop broadcast, phase 2b).
private LocalManualEvent gPauseEvt; // parked fibers wake on UNPAUSE / timeout
// Replay re-entrancy guard (the CLIENT PAUSE heisenbug). replayPaused() drains a
// connection's held commands through the normal pipeline, and that path does IO
// (flushOut / myAof().flush) which yields — another connection's fiber can land a
// fresh CLIENT PAUSE mid-drain. If that pause took effect while we're still
// replaying, the remaining held commands would re-barrier against a window that
// only exists because of the yield, and the emit/park ordering can strand a fiber.
// Fix: clear the (already-lifted) window BEFORE the drain, and while `gReplaying`
// is set DEFER any incoming CLIENT PAUSE into gPausePending* — applied AFTER the
// drain finishes, so it never interleaves with the very commands it must follow.
private bool gReplaying;         // a replayPaused() drain is in progress (TLS: per-shard pause)
private bool gPausePending;      // a CLIENT PAUSE arrived mid-replay
private ulong gPausePendingEnd;  // its (already absolute) deadline
private bool gPausePendingAll;   // its ALL(true)/WRITE(false) mode
private ulong gPausePendingIssuer; // its issuer conn id (exempt)
// Backstop re-check interval for a quiet barriered fiber: CLIENT UNPAUSE wakes it
// via gPauseEvt at once, but this caps the wait so a client that resumes flooding
// after going idle is drained into pausedBuf (and trips the overflow guard) within
// this bound rather than sitting in the kernel until the window's own timeout.
private enum ulong PAUSE_POLL_MS = 100;

private extern (C) int flock(int fd, int operation) nothrow @nogc;
private __gshared int gPortLockFd = -1;

/// Refuse to start if a LIVE dreads already holds this port. SO_REUSEPORT would
/// otherwise let a second instance silently co-bind and split client traffic —
/// a whole class of "phantom" benchmark/test bugs (a subscribe lands on one, the
/// publish on another; a stale old binary answers a command). flock is advisory
/// AND auto-released when the holder dies, so a crashed/killed instance frees it
/// instantly — which is exactly why we keep reusePort for fast restarts. Path is
/// `$XDG_RUNTIME_DIR/dreadlock-<port>.lck` (= /var/run/user/<uid>, per-user, no
/// root) by default, overridable with `--lockfile=`. Always kill servers by PORT.
private bool acquirePortLock(ushort port, scope const(char)[] lockPath) @trusted nothrow
{
    version (Posix)
    {
    import core.sys.posix.fcntl : open, O_CREAT, O_RDWR;
    import core.sys.posix.unistd : close;
    import core.stdc.stdio : snprintf;

    char[512] path = void;
    if (lockPath.length && lockPath.length < path.length)
    {
        path[0 .. lockPath.length] = lockPath;
        path[lockPath.length] = 0;
    }
    else
    {
        // per-user runtime dir (systemd's $XDG_RUNTIME_DIR = /run/user/<uid> =
        // /var/run/user/<uid>) — writable without root, tmpfs, auto-cleaned on
        // logout. Falls back to /var/run (needs root) when it isn't set.
        import core.stdc.stdlib : getenv;

        auto xdg = getenv("XDG_RUNTIME_DIR");
        if (xdg !is null && *xdg != '\0')
            snprintf(path.ptr, path.length, "%s/dreadlock-%u.lck", xdg, cast(uint) port);
        else
            snprintf(path.ptr, path.length, "/var/run/dreadlock-%u.lck", cast(uint) port);
    }
    int fd = open(path.ptr, O_CREAT | O_RDWR, 420); // 0644
    if (fd < 0)
        return true; // can't create the lock file (e.g. /var/run not writable) —
    // don't block startup on it; pass --lockfile= for an alternative location
    enum LOCK_EX = 2, LOCK_NB = 4;
    if (flock(fd, LOCK_EX | LOCK_NB) != 0)
    {
        close(fd);
        return false; // another live instance holds this port
    }
    gPortLockFd = fd; // held for the process lifetime (auto-unlocks on exit)
    return true;
    }
    else
        return true; // Windows: best-effort standalone, no advisory port lock
}

public int runServer(ushort port, const(char)[] aofPath = null, const(char)[] lockPath = null)
{
    // One live dreads per port: reusePort makes a silent co-bind possible, so
    // gate on an flock before doing any work (see acquirePortLock).
    if (!acquirePortLock(port, lockPath))
    {
        printf("dreads: port %u is already held by a live dreads instance\n", cast(uint) port);
        return 1;
    }
    // ACL must be live BEFORE any AOF replay / raft catch-up: replayed
    // "ACL SETUSER … reset …" entries apply through gAclApplyHook.
    initAuthPw(); // libsodium (Argon2 builds); no-op otherwise
    aclInit(); // seed the default ACL user (on nopass +@all ~* &*)
    {
        import dreads.commands : gAclApplyHook;

        gAclApplyHook = &aclApplyCanonical; // apply-path (replay/commit) ACL
    }
    // AOF replay is DEFERRED until after shardInit (phase 2.6): the keyspaces
    // it loads into are per-shard, and each shard's values must come from that
    // shard's allocator (see setupAof).
    gKeyActivity = createManualEvent();
    gPauseEvt = createManualEvent();
    {
        import dreads.notify : gNotifyPublish, gPublishHook, parseNotifyFlags;

        cast(void) parseNotifyFlags(gConfig.notifyKeyspaceEvents, gNotifyFlags);
        gNotifyPublish = (scope const(char)[] chan, scope const(char)[] msg) nothrow{
            gPubSub.publish(chan, msg);
            // phase 2.5c: a notification fires on the shard that ran the write —
            // subscribers may sit on any router, so fan it out (gated: no
            // subscriber anywhere ⇒ nothing to deliver, skip the lanes)
            if (sharded())
            {
                import core.atomic : atomicLoad, MemoryOrder;
                import dreads.pubsub : gSubTotal;

                if (atomicLoad!(MemoryOrder.raw)(gSubTotal) != 0)
                    shardPubFanout(chan, msg);
            }
        };
        // lets a script's redis.call('publish'/'spublish') reach the pub/sub layer.
        // Under sharding the RETURN stays the local receiver count (scripts across
        // shards are a documented v1 gap) but delivery still fans out.
        gPublishHook = (scope const(char)[] chan, scope const(char)[] msg, bool shard) nothrow{
            if (!shard && sharded())
            {
                import core.atomic : atomicLoad, MemoryOrder;
                import dreads.pubsub : gSubTotal;

                if (atomicLoad!(MemoryOrder.raw)(gSubTotal) != 0)
                    shardPubFanout(chan, msg);
            }
            return shard ? gShardPubSub.publish(chan, msg, "smessage")
                : gPubSub.publish(chan, msg);
        };
    }
    {
        import dreads.cluster : initCluster;
        import dreads.shard : shardInit, gShardCount;

        // thread-per-shard: no-op when shards<=1 (single-thread path untouched).
        shardInit(gConfig.shards);
        if (gShardCount > 1)
            printf("dreads: %u shards\n", gShardCount);
        gAofs = new Aof[](gShardCount > 1 ? gShardCount : 1);
        if (aofPath !is null && !setupAof(aofPath))
            return 1;

        if (gConfig.clusterEnabled)
        {
            initCluster();
            printf("dreads: cluster mode\n");
        }
    }
    {
        import dreads.obj : lruClock, gActiveExpire, gActiveEviction, gTrackInvalidateHook,
            gExpireReapHook;
        import dreads.rand : seedRand;
        import dreads.stream : nowMs;

        gActiveExpire = gConfig.activeExpire; // drop-soon timer only runs when enabled
        gActiveEviction = gConfig.activeEviction; // background maxmemory eviction
        if (gConfig.lazyfreeLazyServerDel)
        {
            import dreads.lazyfree : LazyFree;
            import dreads.obj : gLazyFree;

            gLazyFree = new LazyFree(); // spawns the off-loop free-thread (UNLINK)
        }
        lruClock = cast(uint)(nowMs() / 1000);
        seedRand(nowMs()); // shuffle the random-pick commands per boot
        {
            // effects replication: script writes reach the AOF one by one
            import dreads.scripting : gScriptEffectSink, startLuaScriptPool;

            gScriptEffectSink = (scope const(ubyte)[] fx) @nogc nothrow {
                if (myAof().enabled)
                    myAof().append(fx);
            };
            // scripts run on a dedicated thread (off the event loop), so a
            // busy script can't stall the loop and SCRIPT KILL can reach it
            startLuaScriptPool();
        }
        // built-in web dashboard — opt-in (`dashboard yes`), its own isolated
        // event-loop thread; a no-op when disabled (no thread, no port bound).
        // Defaults to the RESP port + 1. A main-loop timer publishes a metrics
        // snapshot every dashboard-interval, but only while a client is watching.
        // Compile-time optional: `version(DreadsDashboard)` (on by default; the
        // `no-dashboard` dub config drops the module + the embedded UI entirely).
        version (DreadsDashboard)
        {
            import dreads.dashboard : startDashboard, dashboardPort, snapshotMetrics,
                startDashCmdBridge;

            startDashboard(port);
            if (gConfig.dashboard)
            {
                startDashCmdBridge(); // main-side drain for dashboard write/admin ops
                setTimer(gConfig.dashboardIntervalMs.msecs, () nothrow {
                    snapshotMetrics(gPubSub.channelCount(), gPubSub.patternCount());
                }, true);
                if (auto dp = dashboardPort())
                    printf("dreads dashboard on http://%.*s:%u\n",
                        cast(int) gConfig.dashboardBind.length, gConfig.dashboardBind.ptr,
                        cast(uint) dp);
            }
        }
        // Wire the expiry->tracking-invalidation hook (a key removed by expiry
        // queues a CLIENT TRACKING invalidation, gated by gTrackCount).
        gTrackInvalidateHook = (scope const(char)[] key) @nogc nothrow {
            if (gTrackCount)
            {
                trackInvalidateKey(key);
                gExpireKeys.set(key, Unit()); // server-caused: exempt from NOLOOP
            }
        };
        // Master-authoritative expiry: the data layer asks this hook how a
        // key's DELETE side effect propagates (see expireReap / obj.gExpireReapHook).
        gExpireReapHook = &expireReap;
        // Active expiry runs on its OWN fast 200ms timer (Valkey sweeps ~10x/s;
        // a 1s cadence left keys logically-expired-but-present far too long). Kept
        // separate from the 1s cron below so the fsync/eviction/lru work does NOT
        // also run 5x more often.
        cast(void) setTimer(200.msecs, delegate() @trusted nothrow {
            import dreads.obj : gLazyFree;

            // Reclaim blocks the free-thread gathered from off-loop UNLINKs. Bounded
            // per tick so a giant value's deallocate spreads over ticks instead of
            // stalling the loop in one burst. Runs regardless of active expiry.
            if (gLazyFree !is null)
                gLazyFree.drainReclaimed(100_000);

            // Wake the raft proposal consumer for any expiry/eviction DELs that a
            // @nogc lazy expiry tryPut without waking it. Runs regardless of active
            // expiry (lazy expiry fires on plain reads). One emit, only when pending.
            if (gExpireDelPending && gReplicator !is null)
            {
                gReplicator.nudgeProposals();
                gExpireDelPending = false;
            }
            maintExpireTick(); // sweep the databases THIS thread owns
        }, true);
        cast(void) setTimer(1.seconds, delegate() @trusted nothrow {
            maintEvictionTick(); // clock pin + LRU clock + eviction + AOF fsync
            releaseIdleMigrateConns(); // close MIGRATE sockets idle > 10s
            pubsubTapExpire(nowMs()); // disarm the dashboard message tap if polling stopped
        }, true);
        if (gConfig.amqpPort != 0)
            cast(void) setTimer(50.msecs, () @trusted nothrow { amqpTtlTick(); }, true);
        if (gConfig.kafkaPort != 0)
            cast(void) setTimer(50.msecs, () @trusted nothrow {
                import dreads.kafkagroup : kgroupSweep;

                kgroupSweep(); // group barriers/evictions on THIS shard
            }, true);
    }
    initReplication();
    // SO_REUSEADDR + SO_REUSEPORT: without reusePort a restarted server can
    // find the port stuck in TIME_WAIT for a long while (vibe's default only
    // sets reuseAddress). Both let a fresh instance rebind immediately.
    // Windows has no SO_REUSEPORT (SO_REUSEADDR there already permits rebind),
    // so drop the flag rather than rely on vibe's Windows handling of it.
    version (Windows)
        enum listenOpts = TCPListenOptions.reuseAddress;
    else
        enum listenOpts = TCPListenOptions.reuseAddress | TCPListenOptions.reusePort;
    cast(void) listenTCP(port, delegate(TCPConnection conn) @trusted nothrow {
        serveClient(conn);
    }, listenOpts);
    printf("dreads listening on port %u\n", cast(uint) port);
    if (gConfig.mqttPort != 0)
    {
        // the MQTT skin (dreads.mqtt): same SO_REUSEPORT share-nothing model,
        // one listener per shard thread (shard 0 = here; the rest in
        // shardThreadEntry), fibers per connection on the accepting thread
        import dreads.mqtt : serveMqttClient, gMqttFanout, mqttDeliverLocal;
        import dreads.mqtt : gMqttSubTotal, gMqttConnBcast, gMqttExec, gMqttResume;

        gMqttExec = (scope const(char)[][] args, ref ByteBuffer reply) nothrow {
            amqpDataExec(args, reply, cast(int) gConfig.mqttDb); // persistent-session state, MQTT's db
        };
        cast(void) listenTCP(gConfig.mqttPort, delegate(TCPConnection conn) @trusted nothrow {
            serveMqttClient(conn);
        }, listenOpts);
        gMqttFanout = (scope const(char)[] topic, scope const(char)[] payload,
                bool retain, ulong seq, ubyte pubQos, scope const(char)[] props) nothrow {
            shardMqttFanout(topic, payload, retain, seq, pubQos, props);
        };
        gMqttConnBcast = (scope const(char)[] clientId, ulong gen) nothrow {
            shardMqttConnBcast(clientId, gen);
        };
        gMqttResume = (uint dstShard, scope const(char)[] clientId) nothrow {
            shardMqttResume(dstShard, clientId);
        };
        printf("dreads MQTT skin on port %u\n", cast(uint) gConfig.mqttPort);
    }
    if (gConfig.amqpPort != 0)
    {
        import dreads.amqp : serveAmqpClient;

        amqpInstallHooks();
        cast(void) listenTCP(gConfig.amqpPort, delegate(TCPConnection conn) @trusted nothrow {
            serveAmqpClient(conn);
        }, listenOpts);
        printf("dreads AMQP skin on port %u\n", cast(uint) gConfig.amqpPort);
    }
    if (gConfig.kafkaPort != 0)
    {
        import dreads.kafka : serveKafkaClient, gKafkaExec, gKafkaPort,
            gKafkaFetchRaw, gKafkaLenRaw;

        gKafkaExec = (scope const(char)[][] args, ref ByteBuffer reply) nothrow {
            amqpDataExec(args, reply, cast(int) gConfig.kafkaDb); // generic exec, Kafka's db
        };
        {
            import dreads.kafka : gKafkaGroupHop;

            gKafkaGroupHop = (scope const(char)[] key, scope const(ubyte)[] req,
                    ref ByteBuffer reply) nothrow {
                kafkaGroupHopImpl(key, req, reply);
            };
        }
        gKafkaFetchRaw = &kafkaFetchDirect;
        gKafkaLenRaw = &kafkaLenDirect;
        gKafkaPort = gConfig.kafkaPort;
        {
            import core.stdc.stdlib : getenv;
            import core.stdc.string : strcmp;
            import dreads.kafka : gKafkaAutoCreate;

            auto ac = getenv("DREADS_KAFKA_AUTOCREATE");
            if (ac !is null && (strcmp(ac, "0") == 0 || strcmp(ac, "false") == 0
                    || strcmp(ac, "off") == 0))
                gKafkaAutoCreate = false; // registry mode (Inspector: sees missing topics)
        }
        cast(void) listenTCP(gConfig.kafkaPort, delegate(TCPConnection conn) @trusted nothrow {
            serveKafkaClient(conn);
        }, listenOpts);
        printf("dreads Kafka skin on port %u\n", cast(uint) gConfig.kafkaPort);
    }
    // sharded: main thread becomes shard 0 (this listener is its router); spawn the
    // other N-1 shard threads, each its own SO_REUSEPORT listener + drain. No-op when
    // shards==1. The listenTCP above already opened shard 0's listener on `port`.
    if (sharded())
        startShards(port);
    auto rc = runEventLoop();
    // Clean shutdown: stop the non-daemon worker-pool threads (Lua, raft) so
    // druntime doesn't block joining their infinite loops when main() returns —
    // otherwise SIGTERM appears to hang for seconds. Daemon threads (lazyfree,
    // syncer) don't need this.
    import dreads.scripting : shutdownLuaScriptPool;

    shutdownLuaScriptPool();
    version (DreadsDashboard)
    {
        import dreads.dashboard : shutdownDashboard;

        shutdownDashboard();
    }
    if (gReplicator !is null)
        gReplicator.stop();
    return rc;
}

/// Builds the Replicator from config when raft-node-id is set. Peers list:
/// "2@host:port,3@host:port". Standalone (id 0) leaves gReplicator null.
private void initReplication()
{
    import std.array : split;
    import std.conv : to;

    import raft.node : Config;
    import raft.vibetransport : PeerAddress;

    import dreads.replicator : gReplicator, Replicator;

    if (gConfig.raftNodeId == 0)
        return;
    PeerAddress[] peers;
    foreach (spec; gConfig.raftPeers.split(","))
    {
        if (spec.length == 0)
            continue;
        auto at = spec.split("@");
        auto hp = at[1].split(":");
        try
            peers ~= PeerAddress(at[0].to!uint, hp[0].idup, hp[1].to!ushort);
        catch (Exception)
        {
            printf("dreads: bad raft-peers entry\n");
        }
    }
    Config cfg;
    cfg.self = gConfig.raftNodeId;
    foreach (ref p; peers)
        cfg.peers ~= p.id;
    cfg.seed = gConfig.raftNodeId * 2_654_435_761UL;
    // Election timeout must dwarf the heartbeat. Raft runs on its own dedicated
    // event-loop thread, so client write load no longer starves the tick timer
    // (that was the reason for the conservative 50-tick default: heartbeat gaps
    // of 120-370ms during a 5000-command burst would trip a tight timeout into
    // spurious elections). Default 50 ticks -> ~1-2s randomized; tunable down via
    // raft-election-timeout for faster failover now that the thread is isolated.
    cfg.electionTimeoutTicks = gConfig.raftElectionTimeoutTicks; // config: raft-election-timeout (default 50)
    cfg.heartbeatTicks = 2; // ~40ms
    cfg.joinMode = gConfig.raftJoin; // passive learner until a config adds us
    // Cap an accepted InstallSnapshot's declared size. A follower stages the
    // whole snapshot in RAM before installing, so an unbounded declared totalLen
    // is a remote-OOM lever on the (unauthenticated) raft transport. Bound it to
    // maxmemory when set (a valid snapshot can't exceed the dataset it captures);
    // 0 (unlimited memory) keeps 0 = no cap, matching the operator's own choice.
    cfg.maxSnapshotBytes = gConfig.maxmemory;
    auto raftPort = gConfig.raftPort != 0 ? gConfig.raftPort : cast(ushort)(gConfig.port + 10_000);
    string base = gConfig.appendfilename.length ? gConfig.appendfilename : "dreads";
    gReplicator = new Replicator(cfg, peers, raftPort, base ~ ".raft", &gDbs[0]);
    gReplicator.compress = gConfig.raftCompress; // LZ4 outbound (raft-compress)
    if (gConfig.raftSecret.length)
    {
        // Derive the frame key here on the main thread, before the raft thread
        // starts (publication happens-before via thread creation).
        import dreads.mac : raftAuthInit;

        if (raftAuthInit(gConfig.raftSecret))
            gReplicator.auth = true;
        else
            printf("dreads: raft-secret set but libsodium init failed — auth DISABLED\n");
    }
    gReplicator.start();
    printf("dreads: raft node %u active on port %u (%zu peers)\n",
            cast(uint) gConfig.raftNodeId, cast(uint) raftPort, peers.length);
}

// Max raft writes a single connection can hold in flight before we reap them.
// Bounds per-connection state (8 bytes each) and the group-commit batch depth.
private enum PIPELINE_CAP = 256;

private struct Conn
{
    TCPConnection tcp;
    TaskMutex wlock;
    Subscriber sub;
    Subscriber shardSub;
    ulong id;
    Keyspace* dbp; // current db (SELECT); a direct pointer avoids re-indexing gDbs per command
    Vector!char clientName; // owned (RAII); freed with the Conn, never manually
Vector!char addr; // "ip:port" of the peer, captured at connect (CLIENT LIST addr=)
Vector!char laddr; // "ip:port" of the local end the peer connected to (laddr=)
long connMs; // wall time at connect (CLIENT LIST age=)
long lastActiveMs; // wall time of the last command (CLIENT LIST idle=)
Vector!char libName, libVer; // CLIENT SETINFO lib-name / lib-ver
// CLIENT LIST/INFO byte + command statistics. netIn counts raw request bytes
// consumed off the socket (per parsed command), netOut counts reply bytes
// written, cmds counts every command executed (incl. redis.call sub-commands).
ulong totNetIn, totNetOut, totCmds;
bool capaRedirect; // CLIENT CAPA redirect: advertises the `r` capability (capa=r)
bool readonlyFlag; // READONLY issued (cluster read-only mode); surfaces as flags=r
// CLIENT REPLY: replyOff silences every reply until CLIENT REPLY ON; replySkipNext
// silences the single next command's reply (CLIENT REPLY SKIP). replyCmdExempt is
// a per-command latch so the CLIENT REPLY command itself is never suppressed.
bool replyOff, replySkipNext, replyCmdExempt;
    bool resp3; // negotiated RESP3 via HELLO 3 (default RESP2)
    // ACL: the connection's user (default at connect) and whether it has cleared
    // authentication (nopass default => true immediately; requirepass => set at
    // AUTH). Enforcement is `command in c.user's cap_set` (see dreads.acl).
    AclUser* user;
    bool authed;
    bool importSource; // CLIENT IMPORT-SOURCE ON: a migration/sync feeder (flags=I)
    // CLIENT PAUSE: raw bytes of commands barriered during a pause window, replayed
    // in order once it lifts. Owned by this fiber, so a disconnect frees it cleanly.
    ByteBuffer pausedBuf;
    ByteBuffer pauseReplayBuf; // scratch: the batch being re-injected on unbarrier
    bool pauseBlocked; // parked on the pause barrier => counted in gBlockedClients
    // MULTI state: queued raw commands, back to back
    bool inMulti;
    bool multiHasWrite; // a queued command writes => EXEC is held by a WRITE pause
    bool multiDirty; // a queue-time error occurred => EXEC must EXECABORT
    size_t multiCount;
    ByteBuffer multiQueue;
    // WATCH state: conservative — any write since WATCH aborts EXEC
    bool watching;
    ulong watchEpoch;
    // Write pipelining (raft): consecutive writes are fired without blocking and
    // reaped in order at the next flush point (before any non-write, or at the
    // end of the read chunk). inExec forces the synchronous path so EXEC keeps
    // its transaction reply shape.
    void*[PIPELINE_CAP] pendingWrites;
    size_t pendingCount;
    bool inExec;
    // Sharding pipeline (independent of raft): a keyed command whose owner is any
    // shard is fired to that shard WITHOUT blocking, its ShardPending recorded here
    // in command order, and its reply reaped in order at the next flush point (a
    // keyless/inline-reply command, PIPELINE_CAP, or the end of the read chunk).
    // This is what lets all shards run their slice concurrently instead of the
    // connection fiber round-tripping one command at a time. Same discipline as the
    // raft write pipeline above, but a separate array (raft is future/its own thread).
    void*[PIPELINE_CAP] shardPends; // ShardPending* handles, in command order
    size_t shardPendCount;
    // Bitmask of shards this pipeline batch has enqueued commands to but not yet woken
    // (bit s => shard s touched). Wakes are BATCHED: fire many commands, then one wake
    // per touched shard at the flush point — cuts cross-thread futex wakeups ~Nx. Only
    // tracks shards < 64; a rare higher shard id wakes immediately (see shardFire).
    ulong shardTouch;
    // HOP COALESCING: per-owner staging buffers. A pipeline batch's commands to
    // the SAME owner accumulate here and travel as ONE ring slot at the flush
    // point (one contiguous copy, one cross-core line handoff, one wake) instead
    // of one slot each — the per-hop cycle cost is cache-line TRANSFER latency
    // (measured growing 1030→2840 cyc/hop from 2 to 8 shards while instructions
    // stayed flat), and it amortizes exactly like syscalls under pipelining.
    // Indexed by owner shard; 64 mirrors the shardTouch wake bitmap (an owner
    // >= 64 is fired unbatched, same as its wake). Per-conn, not a shared
    // static: staging must survive a backpressure yield inside the flush.
    ByteBuffer[64] hopBatch;
    // scratch for the rare unbatched fire (owner >= 64, outside the bitmaps)
    ByteBuffer shardBc;
    // Async output, engaged on first (P)SUBSCRIBE (see PUBSUB.md fan-out): once
    // `subMode` is set, all output (replies and pub/sub messages) is enqueued on
    // `oq` and drained by the `oqWriter` fiber, so the publisher never blocks.
    bool subMode;
    OutQueue oq;
    LocalManualEvent oqEvt;
    Task oqWriter;
    bool oqClosing;
    // Blocked-client wait (BLPOP family — see the `event-driven` skill). One
    // single-shot event per connection (reused each block) + a per-block
    // generation so a returned/re-blocking fiber's stale deque entries
    // self-invalidate (they carry the gen they were registered with). blockFiredKey
    // is the key that woke it (a hint; the wake path re-verifies via lookup).
    LocalManualEvent blockEvt;
    bool blockEvtInit;
    uint bwGen;
    const(char)[] blockFiredKey;
    // Last command name (lowercase) for CLIENT LIST's `cmd=` field. Fixed buffer:
    // every command token fits in 32 (longest is GEORADIUSBYMEMBER_RO = 20).
    char[32] lastCmdBuf = void;
    ubyte lastCmdLen;
    // Raw arg[1] (subcommand token) of the last command, for CLIENT INFO's
    // `cmd=container|sub` form. Stored cheaply here (a bounded copy) and joined
    // LAZILY in appendConnInfo — the container test (aclIsContainer scans ~80
    // entries) must NOT run on the command hot path.
    char[32] lastArgBuf = void;
    ubyte lastArgLen;
    // CLIENT UNBLOCK: `blocked` is true while parked in a blocking command;
    // `unblockReq` is set by another client (1 = TIMEOUT reply, 2 = -UNBLOCKED).
    bool blocked;
    ubyte unblockReq;
    // REMOTE BLOCKING (phase 2.5b): this Conn is a synthetic, fiber-local
    // stand-in for a client parked from ANOTHER shard's router (see
    // remoteBlockServe). It has no socket; the requester's liveness travels
    // through remotePend.cancel, which connAlive/peerGone/blockWait map onto
    // the normal block-loop exits.
    bool remoteBlock;
    ShardPending* remotePend;
    // CLIENT TRACKING (client-side caching invalidation). `tracking` gates it all
    // (mirrored by gTrackCount, the `unlikely` fast-path gate). Default mode: keys
    // the client READS are recorded in gInvalTable (key -> conn-id set); a later
    // write to such a key sends an `invalidate` message. BCAST mode: no per-key
    // table — any write whose key matches one of `trackPrefixes` is invalidated.
    // Delivery is a RESP3 push on this conn (redir==0) or a pub/sub message to
    // `__redis__:invalidate` on the conn whose id is `trackRedir`.
    bool tracking;
    bool trackBcast; // BCAST: invalidate by key-prefix, not by recorded key
    bool trackOptin; // OPTIN: record a read only right after CLIENT CACHING YES
    bool trackOptout; // OPTOUT: record every read unless CLIENT CACHING NO
    bool trackNoloop; // NOLOOP: don't invalidate keys THIS conn just modified
    bool trackCachingYes; // one-shot: OPTIN armed / OPTOUT disarmed for next cmd
    bool trackRedirBroken; // redirect target gone -> one tracking-redir-broken push
    ulong trackRedir; // REDIRECT target conn id (0 = RESP3 push to self)
    Dict!Unit trackPrefixes; // BCAST prefixes (owned keys); empty = whole keyspace
    // Self-invalidations (this conn wrote a key IT cached, RESP3 no-redirect) are
    // staged here and flushed AFTER the command reply, so the push trails the
    // reply. Cross-conn and redirect invalidations go straight out (other fiber).
    ByteBuffer pendingInval;

    @property size_t totalSubs() const @nogc nothrow
    {
        return sub.subCount + shardSub.subCount;
    }
}

// Registry of THIS THREAD's live connections: an id→Weak!Conn index. It is the
// single source of truth for both O(1) lookup (CLIENT UNBLOCK, tracking redirect)
// and iteration (CLIENT LIST / KILL, ACL revoke) — the old intrusive list is gone.
// Each connection lives in a Shared!Conn owned by its serveClient fiber; the
// registry holds a WEAK observer, so a cross-fiber user resolves it via lock()
// and keeps the Conn (and its RAII resources) alive for the duration of the
// access — the UAF that killed tracking is impossible by construction.
//
// THREAD-LOCAL, not __gshared: under thread-per-shard every listener thread
// registers its own conns, and an unsynchronized shared HashMap means concurrent
// rehash → double-free (caught by ASan; as `__gshared` this was the shards>1
// crash). Share-nothing like everything else on the serve path: a shard resolves
// only its OWN clients — a cross-shard id is simply not found, the same v1
// same-shard scope MULTI/blocking/tracking already have (SHARDING.md).
private Dict!(Weak!Conn) tConnById; // keyed by the id's raw 8 bytes

// The 8 raw bytes of a client id, used as the tConnById key (HashMap.set dups it).
private const(char)[] connIdKey(ref const ulong id) @nogc nothrow @trusted
{
    return (cast(const(char)*)&id)[0 .. ulong.sizeof];
}

/// O(1) live-connection lookup by id. Returns a STRONG lock (empty if the id is
/// unknown or its connection has died) — hold it while touching the Conn so it
/// cannot be freed under a yield.
private Shared!Conn connById(ulong id) nothrow @trusted
{
    if (auto w = tConnById.get(connIdKey(id)))
        return w.lock();
    return Shared!Conn.init;
}

private void registerConn(ref Shared!Conn sc) @nogc nothrow
{
    tConnById.set(connIdKey(sc.get().id), sc.weaken());
    gConnectedClients++;
}

private void unregisterConn(ulong id) @nogc nothrow
{
    if (tConnById.remove(connIdKey(id)))
        gConnectedClients--;
}

/// Snapshot every live connection's id into `outv`. Iterating the registry
/// directly while killing/closing conns is unsafe (tcp.close may yield and let a
/// target fiber unregister mid-iteration, mutating tConnById). Callers walk the
/// id snapshot and re-resolve each via connById() (which returns a strong lock),
/// so a conn that died in the meantime is simply skipped and the one being acted
/// on is kept alive by its lock. ids are monotonic, so there is no ABA reuse.
private void snapshotConnIds(ref Vector!ulong outv) nothrow @trusted
{
    foreach (key, ref w; tConnById)
        if (key.length == ulong.sizeof)
            outv.put(*cast(const(ulong)*) key.ptr);
}

// --- CLIENT TRACKING (client-side caching invalidation) ---------------------
// Two server-global registries, both gated by `gTrackCount` (the unlikely
// fast-path check on the command path): when it is 0 nothing here runs.
//   * gInvalTable — DEFAULT mode: a read records `key -> {conn ids that cached
//     it}`; a later write to that key sends each an `invalidate` and drops the
//     entry (one-shot, like Redis's invalidation table).
//   * gBcastConns — BCAST mode: the id set of BCAST clients; a write whose key
//     matches one of the client's prefixes is invalidated (no per-key table).
// Delivery resolves the target through the Weak!Conn registry (connById -> a
// strong lock) so the target Conn stays alive across the push — the Phase-C
// guarantee is what makes this safe from another fiber's write path.
// THREAD-LOCAL (share-nothing rule, whole tracking group): tracking is v1
// SHARD-LOCAL — a shard invalidates its own tracking clients for writes IT
// executes (hopped writes run on the key's owner thread, so a same-shard tracker
// sees them; a tracker on another shard does NOT — SHARDING.md gap, 2b hop fix).
// As __gshared these Dicts were rehash double-frees across listener threads, and
// delivery resolves through tConnById (thread-local) anyway.
private Dict!(Dict!Unit) gInvalTable; // default mode: key -> conn-id set
private Dict!Unit gBcastConns; // bcast mode: conn-id set
private size_t gTrackCount; // # tracking conns (the unlikely gate)

// Append a single-key (or null = FLUSHALL/FLUSHDB "everything") invalidation
// frame to `o`, framed by the TARGET's protocol: a RESP3 client gets an
// `invalidate` PUSH, a RESP2 redirection connection a pub/sub `message` on
// __redis__:invalidate.
private void buildInvalFrame(ref ByteBuffer o, bool resp3, scope const(char)[] key) nothrow
{
    if (resp3)
    {
        o.append(">2\r\n");
        repBulk(o, "invalidate");
    }
    else
    {
        o.append("*3\r\n");
        repBulk(o, "message");
        repBulk(o, "__redis__:invalidate");
    }
    if (key is null)
        o.append("*-1\r\n");
    else
    {
        o.append("*1\r\n");
        repBulk(o, key);
    }
}

// Enqueue an invalidation on `target`'s async output. The target must be in
// subMode (a redirect target is — it subscribed to __redis__:invalidate; a
// RESP3 self-tracking conn is put in subMode when tracking is enabled).
private void sendInvalToConn(ref Conn target, scope const(char)[] key) nothrow
{
    if (!target.subMode)
        return;
    static ByteBuffer fb; // TLS scratch
    fb.clear();
    buildInvalFrame(fb, target.resp3, key);
    auto m = rcFromBytes(fb.data);
    if (target.oq.push(m))
        target.oqEvt.emit();
    rcRelease(m);
}

// Route one invalidation to the tracking conn `trackedId` (key null = all).
// `writerId` is the conn that modified the key (for NOLOOP). Redirect => deliver
// to the redirect target as a message; self (no redirect) writer => defer to the
// conn's pendingInval so the push trails its own reply; otherwise push now.
private void deliverInvalidateTo(ulong trackedId, ulong writerId, scope const(char)[] key) nothrow
{
    auto s = connById(trackedId);
    if (s.isNull)
        return;
    auto tc = &s.get();
    if (!tc.tracking)
        return;
    if (tc.trackNoloop && trackedId == writerId)
        return; // NOLOOP: don't tell a client about keys it changed itself
    if (tc.trackRedir != 0)
    {
        auto rs = connById(tc.trackRedir);
        if (rs.isNull)
        {
            tc.trackRedirBroken = true; // target gone: flagged for a redir-broken push
            return;
        }
        sendInvalToConn(rs.get(), key);
    }
    else if (trackedId == writerId)
    {
        if (tc.subMode) // deferred self-push (RESP3 self-tracking has async output)
            buildInvalFrame(tc.pendingInval, tc.resp3, key);
    }
    else
        sendInvalToConn(*tc, key);
}

// Does a BCAST client's prefixes cover `key`? On a match, `group` is the matching
// prefix (or "" for whole-keyspace), which is how invalidations are grouped into
// one message per prefix. No prefixes => whole keyspace, group "".
private bool bcastMatch(ref Conn tc, scope const(char)[] key, out const(char)[] group) @nogc nothrow @trusted
{
    if (tc.trackPrefixes.length == 0)
    {
        group = "";
        return true;
    }
    foreach (pfx, ref _; tc.trackPrefixes)
        if (key.length >= pfx.length && key[0 .. pfx.length] == pfx)
        {
            group = pfx;
            return true;
        }
    return false;
}

// Record that conn `c` cached read-key `key` (DEFAULT mode invalidation table).
private void trackRecordKey(ref Conn c, scope const(char)[] key) nothrow
{
    auto set = gInvalTable.get(key);
    if (set is null)
    {
        gInvalTable.set(key, Dict!Unit.init);
        set = gInvalTable.get(key);
    }
    set.set(connIdKey(c.id), Unit());
}

// Pending invalidations accumulated during ONE top-level command (grouped so a
// client gets ONE message per group): key = idBytes(8) ++ group, value = key set.
// Default mode uses group ""; BCAST uses the matching prefix. Flushed (delivered)
// at the top-level command boundary — after the command's own reply is staged so
// a self-push trails it.
private Dict!(Dict!Unit) gPend; // TLS: tracking group (see gInvalTable — shard-local v1)
// BCAST keys written this command (raw). Which bcast client/prefix each belongs to
// is resolved at flush (connById is not @nogc, but the expiry accumulation path is).
private Dict!Unit gBcastPendingKeys; // TLS: tracking group (shard-local v1)
// Keys invalidated by a SERVER-caused event (expiry) this cycle. NOLOOP suppresses
// keys the CLIENT modified, but not these — so an expiry of a key the client also
// wrote in the same command still reaches it. Cleared at flush.
private Dict!Unit gExpireKeys; // TLS: tracking group (shard-local v1)

private void pendAdd(ulong id, scope const(char)[] group, scope const(char)[] key) @nogc nothrow @trusted
{
    static ByteBuffer ck; // TLS composite-key scratch
    ck.clear();
    ck.append((cast(const(char)*)&id)[0 .. ulong.sizeof]);
    ck.append(group);
    auto comp = cast(const(char)[]) ck.data;
    auto set = gPend.get(comp);
    if (set is null)
    {
        gPend.set(comp, Dict!Unit.init);
        set = gPend.get(comp);
    }
    set.set(key, Unit()); // HashMap.set dups the key -> owned past the command
}

// A write (or an expiry/eviction) touched `key`: queue invalidations (default
// table + bcast prefixes) into gPend, grouped. NOLOOP is applied at flush, so no
// writer identity is needed here. @nogc-safe (pure accumulation), so the expiry
// path can call it. Conn-id sets are collected in a @nogc pass first.
private void trackInvalidateKey(scope const(char)[] key) @nogc nothrow @trusted
{
    static Vector!ulong ids; // TLS
    if (auto set = gInvalTable.get(key))
    {
        ids.clear();
        foreach (idk, ref _; *set)
            if (idk.length == ulong.sizeof)
                ids.put(*cast(const(ulong)*) idk.ptr);
        gInvalTable.remove(key); // one-shot: the cached copies are now stale
        foreach (id; ids[])
            pendAdd(id, "", key); // default mode: one message per client
    }
    // BCAST: just stash the key; flush resolves which client/prefix it hits.
    if (gBcastConns.length)
        gBcastPendingKeys.set(key, Unit());
}

// Build a grouped invalidation frame (multiple keys) into `o`. The framing keys
// off the TARGET's protocol, not self-vs-redirect: a RESP3 client (whether it is
// the tracker itself or a RESP3 redirection connection) receives an `invalidate`
// PUSH; a RESP2 redirection connection receives a pub/sub `message` on
// __redis__:invalidate. The key payload is a RESP array of the set's keys.
private void buildGroupedFrame(ref ByteBuffer o, bool resp3, ref Dict!Unit keys) nothrow @trusted
{
    if (resp3)
    {
        o.append(">2\r\n");
        repBulk(o, "invalidate");
    }
    else
    {
        o.append("*3\r\n");
        repBulk(o, "message");
        repBulk(o, "__redis__:invalidate");
    }
    repArrayHeader(o, keys.length);
    foreach (k, ref _; keys)
        repBulk(o, k);
}

// Enqueue a grouped invalidation frame on `target`'s async output (subMode only).
private void sendGrouped(ref Conn target, ref Dict!Unit keys) nothrow @trusted
{
    if (!target.subMode)
        return;
    static ByteBuffer fb;
    fb.clear();
    buildGroupedFrame(fb, target.resp3, keys);
    auto m = rcFromBytes(fb.data);
    if (target.oq.push(m))
        target.oqEvt.emit();
    rcRelease(m);
}

// Route one grouped keyset to a tracking conn `id` (NOLOOP-aware, redirect/self).
// `bcastMode` must match the conn's current mode: a default-table entry left over
// from before the client switched to BCAST (or vice-versa) is dropped, not sent.
private void deliverGroup(ulong id, ulong writerId, ref Dict!Unit keys, bool bcastMode) nothrow @trusted
{
    auto s = connById(id);
    if (s.isNull)
        return;
    auto tc = &s.get();
    if (!tc.tracking || tc.trackBcast != bcastMode)
        return;
    // NOLOOP suppresses keys THIS client changed — but not server-caused expiries
    // (in gExpireKeys), even when the client also wrote the key this command.
    static Dict!Unit kept; // TLS: the NOLOOP-surviving subset
    if (tc.trackNoloop && id == writerId)
    {
        kept.clear();
        foreach (k, ref _; keys)
            if (gExpireKeys.exists(k))
                kept.set(k, Unit());
        if (kept.length == 0)
            return;
    }
    auto eff = (tc.trackNoloop && id == writerId) ? &kept : &keys;
    if (tc.trackRedir != 0)
    {
        auto rs = connById(tc.trackRedir);
        if (rs.isNull)
        {
            tc.trackRedirBroken = true;
            return;
        }
        sendGrouped(rs.get(), *eff);
    }
    else if (tc.subMode)
    {
        if (id == writerId) // self-push trails the reply (via pendingInval)
            buildGroupedFrame(tc.pendingInval, tc.resp3, *eff);
        else
            sendGrouped(*tc, *eff);
    }
}

// Deliver every accumulated group at the top-level command boundary. `writerId`
// is the conn that ran the command (for NOLOOP; 0 for server-side expiry).
private void flushTrackingInval(ulong writerId) nothrow @trusted
{
    // DEFAULT mode: gPend is already grouped as idBytes ++ "" -> key set.
    if (gPend.length)
    {
        static Vector!(const(char)[]) comps; // slices into gPend keys, valid until free
        comps.clear();
        foreach (ck, ref _; gPend)
            comps.put(ck);
        foreach (ck; comps[])
        {
            if (ck.length < ulong.sizeof)
                continue;
            if (auto set = gPend.get(ck))
                deliverGroup(*cast(const(ulong)*) ck.ptr, writerId, *set, false);
        }
        gPend.clear();
    }
    // BCAST mode: for each bcast client, split the written keys by matching prefix
    // and deliver one message per prefix (group).
    if (gBcastPendingKeys.length && gBcastConns.length)
    {
        static Vector!ulong bids;
        bids.clear();
        foreach (idk, ref _; gBcastConns)
            if (idk.length == ulong.sizeof)
                bids.put(*cast(const(ulong)*) idk.ptr);
        static Vector!(const(char)[]) bkeys;
        bkeys.clear();
        foreach (k, ref _; gBcastPendingKeys)
            bkeys.put(k);
        static Dict!(Dict!Unit) groups; // group -> key set (owned); reused per conn
        foreach (id; bids[])
        {
            auto s = connById(id);
            if (s.isNull)
                continue;
            auto tc = &s.get();
            if (!tc.tracking || !tc.trackBcast)
                continue;
            // group keys by matching prefix (only the prefixes this key hits)
            groups.clear();
            foreach (k; bkeys[])
            {
                const(char)[] grp;
                if (!bcastMatch(*tc, k, grp))
                    continue;
                auto g = groups.get(grp);
                if (g is null)
                {
                    groups.set(grp, Dict!Unit.init);
                    g = groups.get(grp);
                }
                g.set(k, Unit());
            }
            static Vector!(const(char)[]) gnames;
            gnames.clear();
            foreach (gn, ref _; groups)
                gnames.put(gn);
            foreach (gn; gnames[])
                if (auto g = groups.get(gn))
                    deliverGroup(id, writerId, *g, true);
        }
        groups.clear();
    }
    gBcastPendingKeys.clear();
    gExpireKeys.clear(); // consumed: this cycle's server-caused keys are delivered
}

// FLUSHALL / FLUSHDB: tell every tracking client "everything is invalid" and
// drop the whole default table.
private void trackInvalidateAll(ulong writerId) nothrow
{
    if (gTrackCount == 0)
        return;
    Vector!ulong ids;
    snapshotConnIds(ids);
    foreach (id; ids[])
    {
        auto s = connById(id);
        if (!s.isNull && s.get().tracking)
            deliverInvalidateTo(id, writerId, null);
    }
    gInvalTable.clearShrink(); // every cached key is gone: reset and reclaim the table
}

// Turn tracking OFF for `c`, releasing its registry membership and prefixes.
private void trackDisable(ref Conn c) nothrow
{
    if (!c.tracking)
        return;
    c.tracking = false;
    gBcastConns.remove(connIdKey(c.id));
    c.trackBcast = c.trackOptin = c.trackOptout = c.trackNoloop = false;
    c.trackCachingYes = false;
    c.trackRedir = 0;
    c.trackRedirBroken = false;
    c.trackPrefixes.clearShrink(); // the conn lives on (tracking off) — reset, not delete
    if (gTrackCount)
        gTrackCount--;
    // Stale ids may linger in gInvalTable key-sets; they resolve to no/again-non-
    // tracking conns on delivery and are dropped when the key is next written, so
    // a full O(table) sweep here is unnecessary (delivery re-checks .tracking).
}

// Whether a read by `c` should record its keys (OPTIN/OPTOUT gate the default).
private bool trackShouldRecord(ref Conn c) nothrow
{
    if (c.trackOptin)
        return c.trackCachingYes; // OPTIN: only right after CLIENT CACHING YES
    if (c.trackOptout)
        return !c.trackCachingYes; // OPTOUT: always, unless CLIENT CACHING NO
    return true; // default mode records every read
}

// Post-command tracking hook (only reached when gTrackCount > 0). A write fans
// out invalidations for its written keys; a read by a tracking client records
// its read keys (default mode only — BCAST needs no per-key table). Keys are
// collected in a @nogc pass (forEachCommandKey is @nogc), then acted on.
private void trackAfterCommand(ref Conn c, scope const(char)[] uname,
        scope const(RVal)[] arr, bool isWrite) nothrow @trusted
{
    char[16] lc = void;
    if (uname.length > lc.length)
        return;
    foreach (i, ch; uname)
        lc[i] = ch >= 'A' && ch <= 'Z' ? cast(char)(ch + 32) : ch;
    auto lname = cast(const(char)[]) lc[0 .. uname.length];
    if (isWrite)
    {
        // FLUSHALL/FLUSHDB touch no named key — they invalidate EVERYTHING.
        if (lname == "flushall" || lname == "flushdb")
        {
            trackInvalidateAll(c.id);
            return;
        }
        static Vector!(const(char)[]) wk; // TLS: slices into `arr`, used within call
        wk.clear();
        forEachCommandKey!((scope const(char)[] key, bool, bool nw) @nogc nothrow @trusted {
            if (nw)
                wk.put(key);
            return true;
        })(lname, arr);
        foreach (key; wk[])
            trackInvalidateKey(key);
    }
    else if (c.tracking && !c.trackBcast && trackShouldRecord(c))
    {
        static Vector!(const(char)[]) rk;
        rk.clear();
        forEachCommandKey!((scope const(char)[] key, bool nr, bool) @nogc nothrow @trusted {
            if (nr)
                rk.put(key);
            return true;
        })(lname, arr);
        foreach (key; rk[])
            trackRecordKey(c, key);
    }
}

// --- blocked-client FIFO registry (BLPOP family) ----------------------------
// Per-(db,key) FIFO deque of waiting connections. A blocked call registers ONE
// entry per key it waits on (wait-on-any: its single event joins N keys); a
// producer wakes ONLY the live front of a touched key (FIFO for free — no race,
// no gating). Single event-loop thread ⇒ no locking. Entries carry the
// connection's per-block generation; a returned/re-blocking/dead connection's
// entries mismatch and are trimmed lazily from the deque front. See the
// `event-driven` skill. XREAD BLOCK is deliberately NOT here (fan-out).
private struct BWEntry
{
    Conn* c;
    uint gen; // the c.bwGen at register time; != c.bwGen ⇒ stale, trim
}

private struct WaiterQ
{
    import emplace.deque : Deque;

    Deque!BWEntry q;

    void push(Conn* c, uint gen) nothrow @trusted
    {
        q.pushBack(BWEntry(c, gen));
    }

    // Trim stale/dead entries off the front; return the live front conn or null.
    Conn* front() nothrow @trusted
    {
        while (!q.empty)
        {
            auto e = q.front;
            if (e.c is null || e.c.bwGen != e.gen || !connAlive(e.c))
            {
                q.popFront();
                continue;
            }
            return e.c;
        }
        return null;
    }

    bool empty() nothrow @trusted
    {
        return front() is null;
    }

    // Drop every entry for `c` (order-preserving rotate). O(len), on conn death.
    void removeConn(Conn* c) nothrow @trusted
    {
        immutable n = q.length;
        foreach (_; 0 .. n)
        {
            auto e = q.front;
            q.popFront();
            if (e.c !is c)
                q.pushBack(e);
        }
    }
}

// THREAD-LOCAL (share-nothing rule): blocking is v1 same-shard — each shard
// parks/wakes its own conns against its own keyspace writes (a __gshared Dict
// here is the same rehash double-free class as the conn registry was).
private Dict!WaiterQ[NUM_DBS] gWaiters;

// Fibers of THIS thread parked on gKeyActivity (the XREAD fan-out wait).
// waitForActivity brackets it; the write-tails consult it so a write emits the
// broadcast event ONLY when someone is actually parked — vibe's emit is not
// free and this runs per WRITE (event-driven rule: only wake live waiters).
// Safe on the cooperative loop: a waiter increments BEFORE its wait with no
// yield in between, so a same-thread write can never miss a registered waiter.
private int tKeyWaiters;

// The gated XREAD fan-out wake (see tKeyWaiters).
private void wakeKeyActivity() nothrow
{
    if (tKeyWaiters != 0)
        cast(void) gKeyActivity.emit();
}

// The 16 logical databases THIS thread owns: its shard's partition under
// sharding, the classic gDbs otherwise. The maintenance sweeps (active expire,
// eviction) iterate this — sweeping gDbs under sharding touches keyspaces that
// hold no data (and another shard's keys would never be reaped/evicted).
private Keyspace[] myDbSlice() nothrow @trusted
{
    import dreads.shard : gShardKs, tShard;

    return sharded() ? gShardKs[tShard * NUM_DBS .. (tShard + 1) * NUM_DBS] : gDbs[];
}

// Is the connection still usable to receive a wakeup? (peer may have vanished
// while its fiber was parked — never fire at a dead conn.)
private bool connAlive(Conn* c) nothrow
{
    if (c is null)
        return false;
    if (c.remoteBlock) // synthetic (no socket): alive until the requester cancels
    {
        import core.atomic : atomicLoad;

        return atomicLoad(c.remotePend.cancel) == 0;
    }
    try
        return c.tcp.connected;
    catch (Exception)
        return false;
}

// Non-consuming EOF probe for a parked blocked fiber. `tcp.connected` stays true
// after the peer's FIN until we actually read the EOF, so it can't tell a live
// idle client from a vanished one. waitForDataEx with a zero timeout returns the
// socket's current read state WITHOUT consuming: noMoreData ⇒ the peer closed
// (the event loop already processed the FD's EOF while we yielded); timeout ⇒
// alive but idle; dataAvailable ⇒ alive with buffered input (a pipelined command
// left for the serve loop to handle after the block ends — NOT consumed here).
private bool peerGone(Conn* c) nothrow
{
    import core.time : Duration;
    import vibe.core.net : WaitForDataStatus;

    if (c is null)
        return true;
    if (c.remoteBlock) // synthetic: "peer gone" = the requester flagged a disconnect
    {
        import core.atomic : atomicLoad;

        return atomicLoad(c.remotePend.cancel) == 1;
    }
    try
    {
        if (!c.tcp.connected)
            return true;
        return c.tcp.waitForDataEx(Duration.zero) == WaitForDataStatus.noMoreData;
    }
    catch (Exception)
        return true;
}

private WaiterQ* waitQ(int db, scope const(char)[] key, bool create) nothrow @trusted
{
    auto p = gWaiters[db].get(key);
    if (p is null && create)
    {
        gWaiters[db].set(key, WaiterQ.init);
        p = gWaiters[db].get(key);
    }
    return p;
}

// Register `c` as a waiter on each key (dedup) for a fresh block. Bumps the
// connection's generation so any lingering entries from a previous block become
// stale (trimmed lazily). Call once, at the start of blocking.
private void waitRegister(int db, scope const(RVal)[] keys, Conn* c) nothrow @trusted
{
    c.bwGen++;
    foreach (i, ref k; keys)
    {
        bool dup = false;
        foreach (j; 0 .. i)
            if (keys[j].str == k.str)
            {
                dup = true;
                break;
            }
        if (dup)
            continue;
        waitQ(db, k.str, true).push(c, c.bwGen);
    }
}

// End of a block: invalidate this block's entries (stale gen ⇒ front-trimmed).
private void waitFinish(Conn* c) nothrow
{
    c.bwGen++;
}

// Remove a dying connection from every waiter deque (no dangling Conn* after the
// serveClient stack frame is freed). Called from connection teardown.
private void waitPurgeConn(Conn* c) nothrow @trusted
{
    c.bwGen++;
    static const(char)[][256] keys;
    foreach (db; 0 .. NUM_DBS)
    {
        if (gWaiters[db].length == 0)
            continue;
        size_t n = 0;
        foreach (key, ref _wq; gWaiters[db]) // @nogc collect (removeConn may alloc)
        {
            if (n == keys.length)
                break;
            keys[n++] = key;
        }
        foreach (i; 0 .. n)
        {
            auto p = gWaiters[db].get(keys[i]);
            if (p !is null)
                p.removeConn(c);
        }
    }
}

// Wake EVERY live block-waiter on this shard (blockKick: a cancel must be
// observed now, not at the next poll tick). Rotates each deque in place, so
// FIFO order is untouched; spurious wakes are safe (see the kick handler).
private void wakeAllBlockWaiters() nothrow @trusted
{
    static const(char)[][256] keys; // @nogc collect first (emit may alloc)
    foreach (db; 0 .. NUM_DBS)
    {
        if (gWaiters[db].length == 0)
            continue;
        size_t n = 0;
        foreach (key, ref _wq; gWaiters[db])
        {
            if (n == keys.length)
                break;
            keys[n++] = key;
        }
        foreach (i; 0 .. n)
        {
            auto wq = gWaiters[db].get(keys[i]);
            if (wq is null)
                continue;
            immutable ln = wq.q.length;
            foreach (_; 0 .. ln)
            {
                auto e = wq.q.front;
                wq.q.popFront();
                if (e.c !is null && e.c.bwGen == e.gen && e.c.blockEvtInit)
                    e.c.blockEvt.emit();
                wq.q.pushBack(e);
            }
        }
    }
}

// Wake the live front waiter of `key` (posts the wake; the fiber resumes in loop
// context and re-verifies via lookup). Front-trims stale/dead first.
private void signalKey(int db, scope const(char)[] key) nothrow @trusted
{
    auto p = gWaiters[db].get(key);
    if (p is null)
        return;
    auto c = p.front();
    if (c is null)
        return;
    c.blockFiredKey = key;
    if (c.blockEvtInit)
        c.blockEvt.emit();
}

// FIFO fairness gate. True when `key` already has a live blocked waiter ahead of
// `c` — so `c` must NOT serve the key inline, but queue behind. This is what makes
// a pipelined `LPUSH k v` + `BLPOP k 0` on one connection hand the value to the
// client that blocked FIRST: fibers are cooperative, so the earlier waiter's fiber
// hasn't resumed between the two pipelined commands, and without this gate the
// second command would steal the value it just pushed. The woken front waiter, by
// contrast, IS the front (or the deque is empty), so it serves normally.
private bool keyHeldByOther(int db, scope const(char)[] key, Conn* c) nothrow @trusted
{
    auto p = gWaiters[db].get(key);
    if (p is null)
        return false;
    auto f = p.front(); // trims stale/dead, returns live front or null
    return f !is null && f !is c;
}

// After a write, wake the front of every waited key that now holds data. Guarded
// by the blocked-client count so the no-blocker common path is free.
private void signalReadyKeys(int db, ref Keyspace ks) nothrow @trusted
{
    import dreads.obj : gBlockedClients, ObjType;

    import core.atomic : atomicLoad, MemoryOrder;

    if (atomicLoad!(MemoryOrder.raw)(gBlockedClients) == 0 || gWaiters[db].length == 0)
        return;
    // collect keys first (signalKey/remove must not mutate during iteration)
    static const(char)[][256] buf;
    size_t n = 0;
    foreach (key, ref _wq; gWaiters[db])
    {
        if (n == buf.length)
            break;
        buf[n++] = key;
    }
    foreach (i; 0 .. n)
    {
        auto key = buf[i];
        auto o = ks.lookup(key);
        if (o !is null && o.type != ObjType.str && o.containerLen > 0)
            signalKey(db, key);
        else
        {
            // no servable data and no live waiter ⇒ drop the empty deque entry
            auto p = gWaiters[db].get(key);
            if (p !is null && p.empty)
                gWaiters[db].remove(key);
        }
    }
}

// Force-close another connection: its serveClient fiber unblocks from
// waitForData with an error and runs its own scope(exit) cleanup (which
// unregisters it). Safe to call from a different fiber on the one event loop.
private void killConn(Conn* c) nothrow
{
    try
        c.tcp.close();
    catch (Exception)
    {
    }
}

// After an ACL SETUSER changes a user's channel permissions, disconnect any of
// that user's connections whose active (P)subscriptions include a channel the
// user may no longer access — Valkey's kill-on-revoke. A connection that retains
// permission for ALL of its subscriptions (or the user gaining allchannels) is
// pardoned.
private void aclKillRevokedSubscribers(const(AclUser)* u) nothrow
{
    if (u is null || u.root.allChannels)
        return; // gaining allchannels can never revoke an existing subscription
    Vector!ulong ids;
    snapshotConnIds(ids);
    foreach (id; ids[])
    {
        auto s = connById(id);
        if (s.isNull)
            continue;
        auto p = &s.get();
        if (p.user is u && p.totalSubs > 0)
        {
            bool revoked = false;
            foreach (ch, ref _u1; p.sub.channels)
                if (!aclCanAccessChannel(u, ch))
                {
                    revoked = true;
                    break;
                }
            if (!revoked)
                foreach (pat, ref _u2; p.sub.patterns)
                    if (!aclCanAccessChannel(u, pat, true)) // literal match for patterns
                    {
                        revoked = true;
                        break;
                    }
            if (!revoked)
                foreach (ch, ref _u3; p.shardSub.channels)
                    if (!aclCanAccessChannel(u, ch))
                    {
                        revoked = true;
                        break;
                    }
            if (revoked)
                killConn(p);
        }
    }
}

private enum OUTQ_CAP = 4096; // buffered messages before a slow subscriber drops

// Bounded ring of refcounted output frames for a subscriber connection. Single
// event-loop thread, so no locking: the request/publisher fibers push, the
// writer fiber pops; neither yields between the index updates. The ring holds a
// reference per slot (push retains, pop's consumer releases), so the shared
// frame outlives every subscriber's queue without a per-subscriber copy.
private struct OutQueue
{
    private RcMsg** ring;
    private size_t cap, head, tail, count;
    ulong dropped;

    void setup(size_t capacity) @nogc nothrow @trusted
    {
        cap = capacity;
        ring = cast(RcMsg**) ConnAllocator.instance.allocate(cap * (RcMsg*).sizeof).ptr;
        assert(ring !is null, "out of memory");
        head = tail = count = 0;
    }

    /// Enqueue a reference to the shared frame; returns false (and counts a
    /// drop) when full. Retains on success.
    bool push(RcMsg* m) @nogc nothrow
    {
        if (count == cap)
        {
            dropped++;
            return false;
        }
        rcRetain(m);
        ring[tail] = m;
        tail = (tail + 1) % cap;
        count++;
        return true;
    }

    bool pop(out RcMsg* m) @nogc nothrow
    {
        if (count == 0)
            return false;
        m = ring[head];
        head = (head + 1) % cap;
        count--;
        return true;
    }

    void free() @nogc nothrow
    {
        RcMsg* m;
        while (pop(m))
            rcRelease(m);
        if (ring !is null)
            ConnAllocator.instance.deallocate((cast(void*) ring)[0 .. cap * (RcMsg*).sizeof]);
        ring = null;
        cap = head = tail = count = 0;
    }

    // RAII: the ring (and the refs it still holds) is released when the owning
    // Conn is destroyed — no manual free() in the teardown path. Idempotent
    // (free() nulls ring). A raw-pointer resource ⇒ move-only, never copied.
    ~this() @nogc nothrow { free(); }
    @disable this(this);
}

// Drains a subscriber connection's output queue to its socket. The only writer
// of that socket once subMode is on, so writes stay ordered without a lock.
private void oqWriterLoop(Conn* c) nothrow
{
    ByteBuffer batch; // coalesce every queued message into one write per wakeup
    try
    {
        while (true)
        {
            immutable ec = c.oqEvt.emitCount;
            RcMsg* m;
            batch.clear();
            while (c.oq.pop(m)) // drain the whole ring, staging into one buffer
            {
                batch.append(rcData(m));
                rcRelease(m);
            }
            // One syscall for the batch instead of one per message — the fan-out
            // fix: under N subscribers a publish storm was N writes per message.
            if (batch.length && c.tcp.connected)
            {
                try
                    c.tcp.write(batch.data);
                catch (Exception)
                {
                }
            }
            if (c.oqClosing)
                break;
            c.oqEvt.wait(ec); // returns immediately if an emit raced the drain
        }
    }
    catch (Exception)
    {
    }
}

// Flip a connection to async output on its first subscription.
private void enterSubMode(ref Conn c) nothrow
{
    if (c.subMode)
        return;
    c.subMode = true;
    c.oq.setup(OUTQ_CAP);
    c.oqEvt = createManualEvent();
    c.oqWriter = runTask(&oqWriterLoop, &c);
}

// Close a socket, swallowing the throw — scope(exit) can't contain a `catch`.
private void closeQuiet(ref TCPConnection tcp) nothrow
{
    try
        tcp.close();
    catch (Exception)
    {
    }
}

// Stop the writer fiber and free the queue at connection teardown. A plain
// nothrow function because scope(exit) may not contain a catch.
private void shutdownOutput(ref Conn c) nothrow
{
    if (!c.subMode)
        return;
    c.oqClosing = true;
    c.oqEvt.emit();
    try
        c.oqWriter.join();
    catch (Exception)
    {
    }
    // The ring itself is freed by OutQueue.~this (via Conn.~this) — not here,
    // so a cross-fiber connSink racing this teardown can't hit a freed ring.
}

/// Pub/sub delivery sink: runs on the *publisher's* fiber. It only enqueues on
/// the target's output queue (never touches the socket), so a slow subscriber
/// can never stall the publisher; the subscriber's writer fiber does the write.
/// A subscribed connection is always in subMode, so its queue is live here.
private void connSink(void* ctx, RcMsg* msg) nothrow
{
    auto c = cast(Conn*) ctx;
    if (!c.subMode)
        return;
    // Publish-to-self: a message to the connection whose command is running now
    // must trail that command's own reply (RESP3 publish-to-self inside MULTI/EVAL).
    // Stash the frame in pendingInval, which flushOut drains AFTER outb.
    if (c is gCmdConn)
    {
        if (c.resp3)
        {
            auto pm = rcAsPush(msg);
            c.pendingInval.append(rcData(pm));
            rcRelease(pm);
        }
        else
            c.pendingInval.append(rcData(msg));
        return;
    }
    if (c.resp3)
    {
        // RESP3 wants Push framing; hand the queue our own reframed copy.
        auto pm = rcAsPush(msg);
        if (c.oq.push(pm)) // push retains -> queue holds a ref
            c.oqEvt.emit();
        rcRelease(pm); // drop our ref: queue owns it, or it's freed if unqueued
        return;
    }
    if (c.oq.push(msg)) // push retains; publisher owns the release
        c.oqEvt.emit();
}

// Per-shard drain fiber (runs on each shard thread). Consumes THIS shard's one inbound
// queue, which carries both directions (CrossQueue `kind`):
//   - cmd  (a routed command from some router): execute it on MY keyspace and ship the
//     reply back to the requester shard (meta) — the raft router→worker apply pattern.
//   - reply (a hop result for one of MY connections): fill the requester's Pending and
//     wake it (same-thread; the connection fiber is parked on it).
// v1 is dumb: it re-parses the raw command (RVal has arena pointers, not thread-portable)
// and dispatches directly (no per-shard raft yet — that's 2b). SELF-QUEUE (owner==self)
// still round-trips here; local fast-path is the marked next optimization.
private void shardDrainLoop() nothrow
{
    import dreads.shard : myKeyspace, ShardMsg, ShardPending, shardWaitInbound,
        shardDrainOnce, shardEnqueue, shardWake, tShard;
    import dreads.det : refreshWall;
    import core.bitop : bsf;

    static ByteBuffer reply; // execute scratch (owner side)
    static Arena arena;
    // COALESCED replies: per-requester staging (TLS — one set per shard thread).
    // A drain pass executes K commands from many batches; the replies for each
    // requester accumulate here and ship as ONE ring slot per requester at the
    // end of the pass (with its single wake) — the return path amortizes its
    // cross-core line handoffs exactly like the command path does. Section form:
    //   [u32 bytes][u64 pending][reply bytes]
    static ByteBuffer[64] replyBatch;
    // Process one message straight from the ring slice `p` (zero-copy):
    //   cmd   → walk the coalesced batch, dispatch each section on MY keyspace,
    //           stage its reply for the requester shard (meta).
    //   reply → walk the coalesced reply batch, fill each pending, same-thread.
    ulong replyTouch = void;
    void handle(scope const(ubyte)[] p, void* tag, ulong meta, uint kind) nothrow
    {
        if (cast(ShardMsg) kind == ShardMsg.cmd)
        {
            // owner: K commands in ONE slot; each section rebuilds by slicing
            // and jumps by opcode (no re-parse, no name re-resolution)
            size_t pos = 0;
            while (pos < p.length)
            {
                arena.reset();
                reply.clear();
                RVal cmd;
                int opcode;
                uint db;
                bool blocking, resp3, noblock;
                void* pend;
                const(ubyte)[] rawSect;
                if (decodeHopSection(p, pos, arena, cmd, opcode, db, blocking, resp3,
                        noblock, pend, rawSect))
                {
                    if (blocking)
                    {
                        // BLOCKING serve (phase 2.5b): never park the drain —
                        // a fiber parks instead and replies on its own later.
                        spawnRemoteBlock(cmd, cast(uint) meta,
                                cast(ShardPending*) pend, db, resp3, noblock);
                        continue;
                    }
                    import dreads.aclcat : cmdIx;
                    import dreads.commands : cmdWriteByIdx;

                    gRespProto = resp3 ? 3 : 2; // encode in the REQUESTER's protocol
                    gWriteNoOp = false;
                    if (opcode == cmdIx!"client")
                    {
                        // server-layer hop: only CLIENT UNBLOCK is broadcast
                        // (see executeCommand); serve it on THIS shard's registry.
                        auto wp = drainClientUnblock(cmd, reply);
                        if (wp !is null)
                        {
                            // cancelled a REMOTE park: defer the :1 until the
                            // park's round-trip completes (Redis contract — see
                            // watchUnblockDone), then reply from that fiber
                            immutable wgen = wp.genq;
                            try
                            {
                                cast(void) runTask((ShardPending* w, uint g,
                                        uint rq, void* pd) nothrow {
                                    watchUnblockDone(w, g, rq, pd);
                                }, wp, wgen, cast(uint) meta, pend);
                                continue; // no staged reply for this section
                            }
                            catch (Exception)
                                repInt(reply, 1); // spawn failed: reply now
                        }
                        // a LOCALLY-parked fiber this woke must run (and count
                        // its stats) BEFORE any later message — YIELD to it
                        try
                            yield();
                        catch (Exception)
                        {
                        }
                    }
                    else if (opcode == cmdIx!"config")
                        configCmd(cmd.arr[1 .. $], reply); // only RESETSTAT is broadcast
                    else if (opcode == cmdIx!"bgrewriteaof")
                    {
                        // rewrite THIS shard's file from THIS shard's keyspaces
                        import dreads.shard : tShard;

                        char[520] rb = void;
                        if (!myAof().enabled || aofRewrite(myAof(),
                                aofFileFor(gAofPath, tShard, rb[]), myDbSlice(),
                                tShard == 0))
                            repSimple(reply, "OK");
                        else
                            repError(reply, "ERR AOF rewrite failed");
                    }
                    else if (opcode == cmdIx!"publish")
                        repInt(reply, gPubSub.publish(cmd.arr[1].str, cmd.arr[2].str));
                    else if (opcode == cmdIx!"spublish")
                        repInt(reply, gShardPubSub.publish(cmd.arr[1].str,
                                cmd.arr[2].str, "smessage"));
                    else if (opcode == cmdIx!"pubsub")
                    {
                        // NUMPAT answers this shard's distinct pattern NAMES —
                        // internal wire for the unionCount merge (a :count per
                        // shard cannot be deduped); the rest introspect locally.
                        if (cmd.arr.length >= 2 && eqICDebug(cmd.arr[1].str, "NUMPAT"))
                        {
                            size_t np = 0;
                            gPubSub.eachPattern((pat) { np++; return 0; });
                            repArrayHeader(reply, np);
                            gPubSub.eachPattern((pat) { repBulk(reply, pat); return 0; });
                        }
                        else
                            pubsubIntrospect(cmd.arr[1 .. $], reply);
                    }
                    else
                    {
                        immutable errPrev = gTotalErrorReplies;
                        cast(void) dispatch(cmd, *myKeyspace(db), reply, arena, 0, opcode);
                        // commandstats/errorstats on the OWNER (phase 2.5c): the
                        // requester's executeCommand returns at the hop, before
                        // its stats tail, so a hopped command was counted
                        // NOWHERE. Count where it executed — INFO merges the sum.
                        immutable errored = reply.length && reply.data[0] == '-';
                        if (errored && gTotalErrorReplies == errPrev)
                            statErrorReply(cast(const(char)[]) reply.data);
                        statCall(opcode, errored);
                    }
                    // The write-tail the LOCAL path runs after dispatch (see
                    // executeCommand): wake THIS shard's parked blockers. The
                    // wake must fire where the keyspace changed — without this a
                    // blocked BLPOP/XREAD whose data arrives by hop never wakes.
                    if (cmdWriteByIdx(opcode) && !gWriteNoOp && reply.length
                            && reply.data[0] != '-')
                    {
                        gWriteEpoch++;
                        wakeKeyActivity();
                        signalReadyKeys(cast(int) db, *myKeyspace(db));
                        // AOF-per-shard (phase 2.6): the OWNER logs the hopped
                        // write — same tail contract as the local path (an
                        // effects override wins over the verbatim command).
                        if (myAof().enabled)
                        {
                            if (!propagationOverride.empty)
                                myAof().append(propagationOverride.data);
                            else
                                myAof().appendIR(cmd, opcode, rawSect);
                        }
                    }
                    // A hopped command's propagation override must NEVER leak
                    // into the next command this THREAD logs (serve loop and
                    // drain share the TLS override).
                    propagationOverride.clear();
                    // keyspace notifications the dispatch QUEUED (it is @nogc and
                    // cannot publish) — the serve loop flushes after each local
                    // command; the drain must do the same or a hopped write's
                    // events sit in this shard's TLS buffer until some unrelated
                    // local command flushes a stale backlog (phase 2.5c).
                    if (gNotifyFlags)
                        flushPendingNotify();
                }
                else
                {
                    repError(reply, "ERR shard: malformed bytecode hop");
                    pos = p.length; // poisoned batch: stop walking
                }
                if (pend is null)
                {
                    // framing-level decode failure recovered no pending pointer
                    // (only reachable on a corrupt SPSC ring). There is nobody
                    // to route the reply to; drop the section rather than stage
                    // a null the requester's drain would dereference (crash).
                }
                else if (meta < 64)
                {
                    // stage: [bytes][pending][reply] into the requester's batch
                    auto rb = &replyBatch[cast(size_t) meta];
                    immutable size_t sect = 12 + reply.length;
                    auto raw = rb.freeSpace(sect);
                    if (raw.length >= sect) // OOM-safe: skip staging if it can't grow
                    {
                        auto space = raw[0 .. sect];
                        *cast(uint*) space.ptr = cast(uint)(8 + reply.length);
                        *cast(ulong*)(space.ptr + 4) = cast(ulong) pend;
                        space[12 .. $] = reply.data[];
                        rb.grow(sect);
                        replyTouch |= 1UL << meta;
                    }
                }
                else // rare wide shard id: unbatched reply, immediate wake
                {
                    myAof().flush(); // durable before this immediate confirm ships
                    shardEnqueue(cast(uint) meta, reply.data, pend, 0, ShardMsg.reply);
                    shardWake(cast(uint) meta);
                }
            }
        }
        else if (cast(ShardMsg) kind == ShardMsg.blockKick)
        {
            // a requester cancelled a remote block (phase 2.5b): wake the parked
            // XREAD/XREADGROUP fibers so they observe the cancel flag NOW —
            // and the pop-family fibers too (spurious wakes are safe: the block
            // loops re-check and re-wait WITHOUT re-registering, so the FIFO
            // order is preserved). Cancels are rare; this is off the hot path.
            cast(void) gKeyActivity.emit();
            wakeAllBlockWaiters();
            // YIELD so the woken fibers observe the cancel and finish (reply +
            // stats) before this drain processes any later message (e.g. the
            // INFO that a test issues immediately after CLIENT UNBLOCK).
            try
                yield();
            catch (Exception)
            {
            }
        }
        else if (cast(ShardMsg) kind == ShardMsg.execBatch)
        {
            // Same-slot transaction (phase 2.5d): execute ALL sections
            // back-to-back — this loop never yields, so the transaction is
            // ATOMIC on this shard — then fire wakes/notifications ONCE at the
            // end (a parked blocker must not wake mid-transaction). One reply
            // (the concatenated per-command replies) answers the single pending.
            import dreads.aclcat : cmdIx;
            import dreads.commands : cmdWriteByIdx;

            reply.clear();
            void* pend0 = null;
            bool anyWrite = false;
            uint db0 = 0;
            size_t pos = 0;
            Conn cx; // synthetic: serves blocking sections' one-shot form
            cx.remoteBlock = true;
            cx.inExec = true;
            cx.authed = true;
            while (pos < p.length)
            {
                arena.reset();
                RVal cmd;
                int opcode;
                uint db;
                bool blocking, resp3, noblock;
                void* pend;
                const(ubyte)[] rawSect;
                if (!decodeHopSection(p, pos, arena, cmd, opcode, db, blocking,
                        resp3, noblock, pend, rawSect))
                {
                    repError(reply, "ERR shard: malformed exec batch");
                    break;
                }
                if (pend0 is null)
                    pend0 = pend;
                db0 = db;
                cx.dbp = myKeyspace(db);
                cx.resp3 = resp3;
                cx.remotePend = cast(ShardPending*) pend0;
                gRespProto = resp3 ? 3 : 2;
                gWriteNoOp = false;
                immutable errPrev = gTotalErrorReplies;
                immutable ob = reply.length;
                if (blocking)
                {
                    // one-shot serve on OUR keyspace (cx.inExec ⇒ never parks);
                    // stats come from statBlockingReply inside the switch
                    auto name = cmd.arr[0].str;
                    char[16] nbuf = void;
                    bool served = false;
                    if (name.length <= nbuf.length)
                    {
                        foreach (i2, ch; name)
                            nbuf[i2] = ch >= 'a' && ch <= 'z' ? cast(char)(ch - 32) : ch;
                        served = serveBlockingSwitch(cx,
                                cast(const(char)[]) nbuf[0 .. name.length], cmd,
                                reply, arena);
                    }
                    if (!served)
                        cast(void) dispatch(cmd, *myKeyspace(db), reply, arena);
                }
                else if (opcode == cmdIx!"debug")
                    debugCmd(cx, cmd.arr[1 .. $], reply); // SLEEP only (see router)
                else
                {
                    cast(void) dispatch(cmd, *myKeyspace(db), reply, arena, 0, opcode);
                    immutable errored = reply.length > ob && reply.data[ob] == '-';
                    if (errored && gTotalErrorReplies == errPrev)
                        statErrorReply(cast(const(char)[]) reply.data[ob .. $]);
                    statCall(opcode, errored);
                }
                if (cmdWriteByIdx(opcode) && !gWriteNoOp
                        && !(reply.length > ob && reply.data[ob] == '-'))
                {
                    gWriteEpoch++;
                    anyWrite = true;
                    if (myAof().enabled) // per-section: replay-equivalent (v1
                    {                    // doesn't wrap the txn in MULTI/EXEC)
                        if (!propagationOverride.empty)
                            myAof().append(propagationOverride.data);
                        else
                            myAof().appendIR(cmd, opcode, rawSect);
                    }
                }
                propagationOverride.clear();
            }
            if (anyWrite)
            {
                wakeKeyActivity();
                signalReadyKeys(cast(int) db0, *myKeyspace(db0));
            }
            if (gNotifyFlags)
                flushPendingNotify();
            if (pend0 !is null)
            {
                myAof().flush(); // durable before the EXEC reply ships
                shardEnqueue(cast(uint) meta, reply.data, pend0, 0, ShardMsg.reply);
                shardWake(cast(uint) meta);
            }
        }
        else if (cast(ShardMsg) kind == ShardMsg.kafkaGroup)
        {
            // Kafka group-coordinator op: one atomic FSM transition on THIS
            // (owner) shard, reply routed back by pending pointer. The drain
            // never yields inside kgroupApply, so the transition is atomic.
            import dreads.kafkagroup : kgroupApply;

            if (p.length >= 8)
            {
                auto pend = cast(void*)*cast(const(ulong)*) p.ptr;
                // STACK-local: shardEnqueue below yields under ring
                // backpressure; a TLS static would be clobbered by the next
                // drained kafkaGroup message during that yield.
                ByteBuffer kgReply;
                kgroupApply(p[8 .. $], kgReply);
                if (pend !is null)
                {
                    shardEnqueue(cast(uint) meta, kgReply.data, pend, 0, ShardMsg.reply);
                    shardWake(cast(uint) meta);
                }
            }
        }
        else if (cast(ShardMsg) kind == ShardMsg.amqpCtl)
        {
            // AMQP skin control plane: replicate the declare/bind locally
            import dreads.amqp : amqpApplyCtl;

            amqpApplyCtl(p);
        }
        else if (cast(ShardMsg) kind == ShardMsg.mqttConnect)
        {
            // MQTT client-id takeover fan-in: close our older session for the id
            import dreads.mqtt : mqttTakeover;

            if (p.length >= 8)
            {
                ulong gen = 0;
                foreach (k; 0 .. 8)
                    gen = (gen << 8) | p[k];
                mqttTakeover(cast(const(char)[]) p[8 .. $], gen);
            }
        }
        else if (cast(ShardMsg) kind == ShardMsg.mqttResume)
        {
            // cross-shard reconnect handshake: freeze our parked session for this id
            import dreads.mqtt : mqttResumeSignal;

            mqttResumeSignal(cast(const(char)[]) p[0 .. $]);
        }
        else if (cast(ShardMsg) kind == ShardMsg.mqttPub)
        {
            // MQTT skin fan-in: deliver to THIS thread's topic trie / retained map
            import dreads.mqtt : mqttDeliverLocal;

            if (p.length >= 12)
            {
                immutable ubyte pubQos = p[1];
                ulong seq = 0;
                foreach (k; 0 .. 8)
                    seq = (seq << 8) | p[2 + k];
                immutable size_t tl = (cast(size_t) p[10] << 8) | p[11];
                // [topic][propsLen u32][props][payload]
                if (12 + tl + 4 <= p.length)
                {
                    size_t po = 12 + tl;
                    immutable size_t pl = (cast(size_t) p[po] << 24) | (cast(size_t) p[po + 1] << 16)
                        | (cast(size_t) p[po + 2] << 8) | p[po + 3];
                    po += 4;
                    if (po + pl <= p.length)
                        mqttDeliverLocal(cast(const(char)[]) p[12 .. 12 + tl],
                                cast(const(char)[]) p[po + pl .. $], p[0] != 0, seq,
                                pubQos, null, cast(const(char)[]) p[po .. po + pl]);
                }
            }
        }
        else if (cast(ShardMsg) kind == ShardMsg.pub)
        {
            // cross-shard keyspace notification / script publish (phase 2.5c):
            // deliver to THIS shard's local subscribers, fire-and-forget
            if (p.length >= 4)
            {
                immutable uint cl = *cast(const(uint)*) p.ptr;
                if (4 + cast(size_t) cl <= p.length)
                    cast(void) gPubSub.publish(cast(const(char)[]) p[4 .. 4 + cl],
                            cast(const(char)[]) p[4 + cl .. $]);
            }
        }
        else // ShardMsg.reply — walk the coalesced batch, fill each pending
        {
            if (tag !is null) // unbatched single (wide shard id): tag is the pending
            {
                auto pend = cast(ShardPending*) tag;
                if (pend is null)
                    return; // defensive: corrupt frame, nothing to fill
                pend.reply.clear();
                pend.reply.append(p);
                pend.ready = true;
                pend.done.emit();
                return;
            }
            size_t pos = 0;
            while (pos + 12 <= p.length)
            {
                immutable uint sect = *cast(const(uint)*)(p.ptr + pos);
                if (sect < 8 || p.length - pos - 4 < sect)
                    break; // malformed tail: drop (a pending never filled is a bug loud in tests)
                auto pend = cast(ShardPending*)*cast(const(ulong)*)(p.ptr + pos + 4);
                if (pend is null)
                {
                    pos += 4 + sect; // corrupt frame staged no route: skip it
                    continue;
                }
                pend.reply.clear();
                pend.reply.append(p[pos + 12 .. pos + 4 + sect]);
                pend.ready = true;
                pend.done.emit();
                pos += 4 + sect;
            }
        }
    }

    // ADAPTIVE SPIN before parking (CCX campaign): at high N a shard runs well
    // under capacity and parks constantly (measured 34.5k voluntary switches/s
    // at shards=8 vs 3.8k at shards=4) — and EVERY park makes the next
    // producer pay a futex syscall plus this consumer a scheduler wakeup, on
    // top of the cross-CCX line transfers. Yield-polling keeps this fiber
    // runnable: the event loop polls epoll with zero timeout (serve fibers
    // keep running between yields — no starvation), `parked` is never
    // published, and producers skip the wake entirely. The budget bounds the
    // idle burn on a dedicated pinned core; an idle server still sleeps.
    enum DRAIN_SPIN_BUDGET = 4096;
    while (true)
    {
        try
        {
            {
                import dreads.shard : gInbound;

                auto inb = gInbound.length > tShard ? gInbound[tShard] : null;
                int spins = 0;
                while (inb !is null && !inb.anyReady() && spins++ < DRAIN_SPIN_BUDGET)
                    yield();
            }
            shardWaitInbound(); // park on the per-shard event ONLY when all lanes idle
            refreshWall(); // one clock read per drain pass (owner dispatches hopped cmds)
            replyTouch = 0; // requester shards we owe a batch + single wake this pass
            cast(void) shardDrainOnce!handle();
            {
                // MQTT fan-in deliveries accumulated this pass flush here
                import dreads.mqtt : mqttFlushDirty;

                mqttFlushDirty();
            }
            // Durability before the reply: a hopped write (a cross-shard skin
            // publish — the COMMON case at shards>1, since a skin client's queue
            // owner is any shard by key hash) was applied to THIS owner's AOF
            // pending during shardDrainOnce. Flush it to the OS now, before the
            // reply (which carries the basic.ack / acks=1 confirm) ships back —
            // otherwise the confirm reaches the client while the write lives
            // only in the owner's un-flushed buffer, lost on the owner's kill-9.
            // No-op when nothing was written (pending.empty).
            myAof().flush();
            // ship each requester's coalesced reply batch, then its ONE wake
            while (replyTouch)
            {
                immutable s = cast(uint) bsf(replyTouch);
                if (replyBatch[s].length)
                {
                    shardEnqueue(s, replyBatch[s].data, null, 0, ShardMsg.reply);
                    replyBatch[s].clear();
                }
                shardWake(s);
                replyTouch &= replyTouch - 1;
            }
        }
        catch (Exception)
        {
        }
    }
}

// Serve a broadcast CLIENT UNBLOCK section on THIS shard (phase 2.5b): look the
// target id up in this thread's own conn registry and wake it if parked. The
// requester already validated the form (see executeCommand); each shard replies
// :1 (found + unblocked) or :0 and the sumInt merge yields the client's answer.
// A locally-parked conn wakes via its blockEvt; a conn parked as a REQUESTER of
// a remote block (shardFireBlocking) picks its unblockReq up at the next block
// tick and cancels its remote park through the pending.
private ShardPending* drainClientUnblock(const ref RVal cmd, ref ByteBuffer reply) nothrow
{
    import dreads.commands : parseLong;
    import dreads.stream : nowMs;

    long id = 0;
    cast(void) parseLong(cmd.arr[2].str, id);
    immutable ubyte mode = cmd.arr.length == 4
        && eqICDebug(cmd.arr[3].str, "ERROR") ? 2 : 1;
    // CLIENT PAUSE holds blocked clients in place (see clientCmd's UNBLOCK)
    immutable paused = gPauseUntilMs != 0 && nowMs() < gPauseUntilMs;
    long unblocked = 0;
    if (!paused && id >= 0)
    {
        auto s = connById(cast(ulong) id);
        auto p = s.isNull ? null : &s.get();
        if (p !is null && p.blocked)
        {
            p.unblockReq = mode;
            if (p.blockEvtInit)
                p.blockEvt.emit(); // wake it; the block loop honours unblockReq
            if (!p.remoteBlock && p.remotePend !is null)
            {
                // Parked as the REQUESTER of a remote block (shardFireBlocking,
                // on this very thread): write the cancel NOW and kick every
                // shard so the owner's parked fiber observes it immediately.
                // The :1 reply is DEFERRED (see the drain's watcher): Redis's
                // contract is that when CLIENT UNBLOCK returns, the unblock has
                // HAPPENED — a test reads INFO commandstats right after, and
                // the owner's stats must already be written.
                import core.atomic : atomicStore;
                import dreads.shard : gShardCount, shardEnqueue, shardWake,
                    ShardMsg, tShard;

                atomicStore(p.remotePend.cancel, cast(ubyte)(mode == 2 ? 3 : 2));
                static immutable ubyte[1] kb = [0];
                foreach (uint s2; 0 .. gShardCount)
                    if (s2 != tShard)
                    {
                        shardEnqueue(s2, kb[], null, tShard, ShardMsg.blockKick);
                        shardWake(s2);
                    }
                return p.remotePend; // caller defers the :1 until this completes
            }
            unblocked = 1;
        }
    }
    repInt(reply, unblocked);
    return null;
}

// Deferred CLIENT UNBLOCK reply (phase 2.5c): wait until the cancelled remote
// park completes — its pending goes ready, or the slot's acquire-generation
// moves (the conn already reaped and reused it) — then send the :1 to the
// broadcast's requester. Runs in its own fiber on the conn's router thread.
private void watchUnblockDone(ShardPending* wp, uint gen, uint reqShard, void* pend) nothrow
{
    import core.time : msecs;
    import dreads.shard : shardEnqueue, shardWake, ShardMsg;

    for (;;)
    {
        if (wp.ready || wp.genq != gen)
            break;
        immutable ec = wp.done.emitCount;
        if (wp.ready || wp.genq != gen)
            break;
        try
            cast(void) wp.done.waitUninterruptible(msecs(BLOCK_POLL_MS), ec);
        catch (Exception)
        {
        }
    }
    ByteBuffer rb;
    repInt(rb, 1);
    shardEnqueue(reqShard, rb.data, pend, 0, ShardMsg.reply);
    shardWake(reqShard);
}

// Fan a fire-and-forget publish (keyspace notification / script publish) out
// to every OTHER shard's local subscriber registry (phase 2.5c). Callers gate
// on gSubTotal — with no subscriber anywhere this is never reached. Runs on
// whichever shard thread generated the event (the enqueue writes only this
// thread's own SPSC lanes).
private void shardPubFanout(scope const(char)[] chan, scope const(char)[] msg) nothrow
{
    import dreads.shard : gShardCount, tShard, shardEnqueue, shardWake, ShardMsg;

    static ByteBuffer pb; // TLS: one staging per shard thread, no yield inside
    pb.clear();
    immutable size_t len = 4 + chan.length + msg.length;
    auto raw = pb.freeSpace(len);
    if (raw.length < len)
        return; // OOM: drop the cross-shard fanout, keep the broker up
    auto space = raw[0 .. len];
    *cast(uint*) space.ptr = cast(uint) chan.length;
    space[4 .. 4 + chan.length] = cast(const(ubyte)[]) chan[];
    space[4 + chan.length .. $] = cast(const(ubyte)[]) msg[];
    pb.grow(len);
    foreach (uint s2; 0 .. gShardCount)
        if (s2 != tShard)
        {
            shardEnqueue(s2, pb.data, null, tShard, ShardMsg.pub);
            shardWake(s2);
        }
}

// --- AMQP skin plumbing -----------------------------------------------------
// The AMQP skin's queues ARE lists in the keyspace: these helpers execute a
// synthesized RESP command through the SAME data plane every client write uses
// — self-shard direct dispatch (with the full write tail: epoch, wakes, AOF)
// or a synchronous cross-shard hop. Runs on AMQP connection fibers.
private void amqpDataExec(scope const(char)[][] args, ref ByteBuffer reply,
        int db = -1) nothrow @trusted
{
    import dreads.acl : aclCmdIndex;
    import dreads.commands : cmdWriteByIdx;
    import dreads.shard : tShard, acquireShardPending, releaseShardPending,
        shardEnqueue, shardWake, ShardMsg, shardOfSlot;
    import dreads.slots : keyToSlot;

    if (db < 0)
        db = cast(int) gConfig.amqpDb; // default: the AMQP skin's configured db
    static ByteBuffer raw; // TLS: the synthesized RESP bytes
    raw.clear();
    repArrayHeader(raw, args.length);
    foreach (a; args)
        repBulk(raw, a);
    static Arena arena; // TLS scratch
    arena.reset();
    RVal cmd;
    size_t pp = 0;
    if (parseValue(raw.data, pp, arena, cmd) != ParseStatus.ok)
        return;
    immutable opcode = aclCmdIndex(args[0]);
    immutable owner = sharded() ? cast(int) shardOfSlot(keyToSlot(args[1])) : cast(int) tShard;
    reply.clear();
    if (!sharded() || cast(uint) owner == tShard)
    {
        gRespProto = 2;
        gWriteNoOp = false;
        {
            // ambient db: the AOF prefixes a `SELECT <gNotifyDb>` frame on
            // change and notifications name their channel by it. Without this,
            // a skin write after a RESP client's SELECT n was framed under n —
            // replaying broker state into a USER db (latent AOF corruption).
            import dreads.notify : gNotifyDb;

            gNotifyDb = db;
        }
        cast(void) dispatch(cmd, *(sharded() ? myKeyspace2(cast(uint) db) : &gDbs[db]), reply, arena, 0, opcode);
        if (cmdWriteByIdx(opcode) && !gWriteNoOp && reply.length && reply.data[0] != '-')
        {
            gWriteEpoch++;
            wakeKeyActivity();
            signalReadyKeys(db, *(sharded() ? myKeyspace2(cast(uint) db) : &gDbs[db]));
            if (myAof().enabled)
            {
                import dreads.commands : propagationOverride;

                if (!propagationOverride.empty)
                    myAof().append(propagationOverride.data);
                else
                    myAof().appendIR(cmd, opcode, raw.data);
            }
        }
        {
            import dreads.commands : propagationOverride;

            propagationOverride.clear();
        }
        if (gNotifyFlags)
            flushPendingNotify();
        return;
    }
    // cross-shard: synchronous hop (AMQP fibers may block; the drain delivers).
    // hb is a STACK-local, not a TLS static: shardEnqueue yields under ring
    // backpressure, and a shared static would be clobbered by another AMQP
    // fiber's hop during that yield (its hop bytes would then replay ours).
    auto p = acquireShardPending();
    ByteBuffer hb;
    appendHopCmd(hb, cmd, raw.data, opcode, cast(uint) db, cast(void*) p);
    if (hb.length == 0)
    {
        // OOM: appendHopCmd staged nothing. Don't enqueue an empty hop and then
        // block forever on a reply that never comes — release the pending and
        // return an empty reply (surfaced to the client as a failed command).
        releaseShardPending(p);
        reply.clear();
        return;
    }
    shardEnqueue(cast(uint) owner, hb.data, null, tShard, ShardMsg.cmd);
    shardWake(cast(uint) owner);
    while (!p.ready)
    {
        immutable ec = p.done.emitCount;
        if (p.ready)
            break;
        try
            p.done.wait(ec);
        catch (Exception)
        {
        }
    }
    // Re-clear AFTER the park: `reply` is a caller-shared TLS static (gAmqpPop's
    // rb2 etc). Another AMQP fiber that ran its own amqpDataExec while we were
    // parked left its bytes in this same buffer; without this re-clear we would
    // parse the leading reply = the other consumer's message (misdelivery).
    // Everything from here to the caller's parse is yield-free.
    reply.clear();
    reply.append(p.reply.data);
    releaseShardPending(p);
}

/// Kafka group-coordinator hop: run ONE FSM op on the shard owning
/// `routingKey` ("kafka.cg.<group>" — same slot as the group's offsets hash).
/// Same shard = direct call; cross-shard = ShardMsg.kafkaGroup + pending wait
/// (the amqpDataExec pattern). An empty reply means the hop failed (OOM) —
/// callers treat it as a retryable error.
private void kafkaGroupHopImpl(scope const(char)[] routingKey,
        scope const(ubyte)[] req, ref ByteBuffer reply) nothrow @trusted
{
    import dreads.kafkagroup : kgroupApply;
    import dreads.shard : tShard, acquireShardPending, releaseShardPending,
        shardEnqueue, shardWake, ShardMsg, shardOfSlot;
    import dreads.slots : keyToSlot;

    immutable owner = sharded() ? cast(int) shardOfSlot(keyToSlot(routingKey))
        : cast(int) tShard;
    if (!sharded() || cast(uint) owner == tShard)
    {
        kgroupApply(req, reply);
        return;
    }
    auto pnd = acquireShardPending();
    // hb is STACK-local: shardEnqueue yields under ring backpressure and a TLS
    // static would be clobbered by another fiber's hop during that yield.
    ByteBuffer hb;
    auto space = hb.freeSpace(8 + req.length);
    if (space.length < 8 + req.length)
    {
        releaseShardPending(pnd);
        reply.clear(); // empty reply = failed hop (caller retries)
        return;
    }
    *cast(ulong*) space.ptr = cast(ulong) cast(void*) pnd;
    space[8 .. 8 + req.length] = req[];
    hb.grow(8 + req.length);
    shardEnqueue(cast(uint) owner, hb.data, null, tShard, ShardMsg.kafkaGroup);
    shardWake(cast(uint) owner);
    while (!pnd.ready)
    {
        immutable ec = pnd.done.emitCount;
        if (pnd.ready)
            break;
        try
            pnd.done.wait(ec);
        catch (Exception)
        {
        }
    }
    // re-clear AFTER the park: `reply` may be a caller-shared TLS static that
    // another fiber wrote to while we were parked (see amqpDataExec).
    reply.clear();
    reply.append(pnd.reply.data);
    releaseShardPending(pnd);
}

// Kafka fetch fast path: when THIS thread owns the key, walk the packed list
// segment and append [offset i64][stored blob] per record straight into the
// response buffer — no synthesized RESP, no LRANGE reply, no re-copy. The
// stored blob is already verbatim wire bytes ([size i32][MessageSet v1
// message]); the offset is implicit (= list index), stamped here on the fly.
private int kafkaFetchDirect(scope const(char)[] key, long from, int maxN,
        scope int delegate(scope const(ubyte)[] blob) @nogc nothrow sink) nothrow @trusted
{
    import dreads.shard : tShard, shardOfSlot;
    import dreads.slots : keyToSlot;
    import dreads.obj : ObjType;
    import dreads.list : ListSeekHint;

    if (sharded() && cast(uint) shardOfSlot(keyToSlot(key)) != tShard)
        return -1; // not the owner: caller takes the data-plane hop
    bool wrong;
    auto ks = sharded() ? myKeyspace2(gConfig.kafkaDb) : &gDbs[gConfig.kafkaDb];
    auto obj = ks.lookupTyped(key, ObjType.list, wrong);
    if (wrong || obj is null || maxN <= 0 || from < 0)
        return 0;
    // Per-thread resume-cursor cache: a sequential consumer fetching offsets
    // 0, 16384, 32768... would otherwise pay an O(from) head seek per fetch
    // (quadratic over the partition — a 12M-record drain never finishes).
    // Keyed by key hash; epoch-validated inside walkRangeHinted, so a stale
    // or colliding entry degrades to the head walk, never to wrong bytes.
    static struct FetchCursor
    {
        ulong kh;
        ListSeekHint h;
    }

    static FetchCursor[64] tFetchCur; // TLS
    ulong kh = 1469598103934665603UL;
    foreach (ch; key)
    {
        kh ^= ch;
        kh *= 1099511628211UL;
    }
    auto ce = &tFetchCur[cast(size_t)(kh & 63)];
    if (ce.kh != kh)
    {
        ce.kh = kh;
        ce.h = ListSeekHint.init;
    }
    int cnt = 0;
    cast(void) obj.list.walkRangeHinted(ce.h, from, cast(size_t) maxN,
            (const(char)[] v) @nogc nothrow {
        if (sink(cast(const(ubyte)[]) v) != 0)
            return 1; // sink asked to stop (budget filled)
        cnt++;
        return 0;
    });
    return cnt;
}

private long kafkaLenDirect(scope const(char)[] key) nothrow @trusted
{
    import dreads.shard : tShard, shardOfSlot;
    import dreads.slots : keyToSlot;
    import dreads.obj : ObjType;

    if (sharded() && cast(uint) shardOfSlot(keyToSlot(key)) != tShard)
        return -1;
    bool wrong;
    auto ks = sharded() ? myKeyspace2(gConfig.kafkaDb) : &gDbs[gConfig.kafkaDb];
    auto obj = ks.lookupTyped(key, ObjType.list, wrong);
    if (wrong || obj is null)
        return 0;
    return cast(long) obj.list.length;
}

// myKeyspace under a different name to dodge the local import shadowing above
private Keyspace* myKeyspace2(uint db) nothrow @trusted
{
    import dreads.shard : myKeyspace;

    return myKeyspace(db);
}

private void amqpInstallHooks() nothrow
{
    import dreads.amqp : gAmqpPush, gAmqpPushFront, gAmqpPop, gAmqpLen, gAmqpDelKey, gAmqpAofFlush, gAmqpPeekHead, gAmqpPeekAt, gAmqpOwns, gAmqpCtlFanout;

    gAmqpPush = (scope const(char)[] key, scope const(char)[] payload) nothrow {
        static ByteBuffer rb; // TLS
        const(char)[][3] a = ["rpush", key, payload];
        amqpDataExec(a[], rb);
    };
    gAmqpPushFront = (scope const(char)[] key, scope const(char)[] payload) nothrow {
        static ByteBuffer rbf; // TLS
        const(char)[][3] a = ["lpush", key, payload]; // requeue goes to the FRONT
        amqpDataExec(a[], rbf);
    };
    gAmqpPop = (scope const(char)[] key, ref ByteBuffer outPayload) nothrow {
        static ByteBuffer rb2; // TLS
        const(char)[][2] a = ["lpop", key];
        amqpDataExec(a[], rb2);
        // parse $N\r\n<payload>\r\n (nil = $-1)
        auto d = rb2.data;
        if (d.length < 4 || d[0] != '$' || d[1] == '-')
            return false;
        size_t i = 1;
        size_t n = 0;
        while (i < d.length && d[i] != '\r')
        {
            n = n * 10 + (d[i] - '0');
            i++;
        }
        i += 2;
        if (i + n > d.length)
            return false;
        outPayload.append(d[i .. i + n]);
        return true;
    };
    gAmqpLen = (scope const(char)[] key) nothrow {
        static ByteBuffer rb3; // TLS
        const(char)[][2] a = ["llen", key];
        amqpDataExec(a[], rb3);
        auto d = rb3.data;
        if (d.length < 2 || d[0] != ':')
            return 0L;
        long v = 0;
        size_t i = 1;
        while (i < d.length && d[i] >= '0' && d[i] <= '9')
        {
            v = v * 10 + (d[i] - '0');
            i++;
        }
        return v;
    };
    gAmqpDelKey = (scope const(char)[] key) nothrow {
        static ByteBuffer rbd; // TLS
        const(char)[][2] a = ["del", key];
        amqpDataExec(a[], rbd);
    };
    gAmqpAofFlush = () nothrow { myAof().flush(); };
    gAmqpPeekHead = (scope const(char)[] key, ref ByteBuffer outHead) nothrow {
        static ByteBuffer rbk; // TLS
        const(char)[][3] a = ["lindex", key, "0"];
        amqpDataExec(a[], rbk);
        auto d = rbk.data; // $N\r\n<blob>\r\n or $-1 (empty/nil)
        if (d.length < 4 || d[0] != '$' || d[1] == '-')
            return false;
        size_t i = 1, n = 0;
        while (i < d.length && d[i] != '\r')
        {
            n = n * 10 + (d[i] - '0');
            i++;
        }
        i += 2;
        if (i + n > d.length)
            return false;
        outHead.append(d[i .. i + n]);
        return true;
    };
    gAmqpPeekAt = (scope const(char)[] key, long index, ref ByteBuffer outPayload) nothrow {
        import core.stdc.stdio : snprintf;

        static ByteBuffer rbk2; // TLS
        char[24] ib = void;
        immutable n2 = snprintf(ib.ptr, ib.length, "%lld", index);
        const(char)[][3] a = ["lindex", key, cast(const(char)[]) ib[0 .. (n2 > 0 ? n2 : 0)]];
        amqpDataExec(a[], rbk2);
        auto d = rbk2.data;
        if (d.length < 4 || d[0] != '$' || d[1] == '-')
            return false;
        size_t i = 1, n = 0;
        while (i < d.length && d[i] != '\r')
        {
            n = n * 10 + (d[i] - '0');
            i++;
        }
        i += 2;
        if (i + n > d.length)
            return false;
        outPayload.append(d[i .. i + n]);
        return true;
    };
    gAmqpOwns = (scope const(char)[] key) nothrow {
        import dreads.shard : tShard, shardOfSlot, sharded;
        import dreads.slots : keyToSlot;

        return !sharded() || cast(uint) shardOfSlot(keyToSlot(key)) == tShard;
    };
    gAmqpCtlFanout = (scope const(ubyte)[] ctl) nothrow {
        import dreads.shard : gShardCount, tShard, shardEnqueue, shardWake,
            ShardMsg, sharded;

        if (!sharded())
            return;
        foreach (uint s2; 0 .. gShardCount)
            if (s2 != tShard)
            {
                shardEnqueue(s2, ctl, null, tShard, ShardMsg.amqpCtl);
                shardWake(s2);
            }
    };
}

// Fan an MQTT publish out to every OTHER shard's local topic trie (the MQTT
// skin — same fabric and gating pattern as shardPubFanout). Wire:
// [u8 retain][u16 topicLen][topic][payload]. Retained messages fan out even
// with zero subscribers (every thread's retained map must converge).
// Broadcast an MQTT client-id takeover to every OTHER shard ([MQTT-3.1.4-2]).
// Wire: [u64 gen BE][clientId]. Each shard closes its own older session for id.
private void shardMqttConnBcast(scope const(char)[] clientId, ulong gen) nothrow
{
    import dreads.shard : gShardCount, tShard, shardEnqueue, shardWake, ShardMsg, sharded;

    if (!sharded())
        return;
    ByteBuffer mb;
    immutable size_t len = 8 + clientId.length;
    auto raw = mb.freeSpace(len);
    if (raw.length < len)
        return; // OOM: drop the broadcast, keep the broker up
    auto space = raw[0 .. len];
    foreach (k; 0 .. 8)
        space[k] = cast(ubyte)(gen >> ((7 - k) * 8));
    space[8 .. $] = cast(const(ubyte)[]) clientId[];
    mb.grow(len);
    foreach (uint s2; 0 .. gShardCount)
        if (s2 != tShard)
        {
            shardEnqueue(s2, mb.data, null, tShard, ShardMsg.mqttConnect);
            shardWake(s2);
        }
}

/// Cross-shard reconnect handshake (dreads.mqtt): ask `dstShard` to freeze the
/// parked session for `clientId`. Payload is just the client id; the receiving
/// drain calls mqttResumeSignal. Fire-and-forget (the reconnecting shard then
/// waits on the session's `frozen` flag).
private void shardMqttResume(uint dstShard, scope const(char)[] clientId) nothrow
{
    import dreads.shard : tShard, shardEnqueue, shardWake, ShardMsg, sharded;

    if (!sharded())
        return;
    ByteBuffer mb;
    auto raw = mb.freeSpace(clientId.length);
    if (raw.length < clientId.length)
        return;
    raw[0 .. clientId.length] = cast(const(ubyte)[]) clientId[];
    mb.grow(clientId.length);
    shardEnqueue(dstShard, mb.data, null, tShard, ShardMsg.mqttResume);
    shardWake(dstShard);
}

private void shardMqttFanout(scope const(char)[] topic, scope const(char)[] msg,
        bool retain, ulong seq, ubyte pubQos, scope const(char)[] props) nothrow
{
    import core.atomic : atomicLoad, MemoryOrder;

    import dreads.mqtt : gMqttSubTotal;
    import dreads.shard : gShardCount, tShard, shardEnqueue, shardWake, ShardMsg, sharded;

    if (!sharded())
        return;
    if (!retain && atomicLoad!(MemoryOrder.raw)(gMqttSubTotal) == 0)
        return;
    // shardEnqueue YIELDS under lane backpressure; if another publisher fiber
    // on this thread enters meanwhile, a single static staging buffer would
    // be cleared/reallocated under the parked fiber's feet (its lane slices
    // would then replay foreign or freed bytes). Fast path keeps the reused
    // TLS buffer; reentrant callers pay a fresh local one.
    static ByteBuffer mb; // TLS staging
    static bool mbBusy;
    ByteBuffer local;
    ByteBuffer* buf = &local;
    if (!mbBusy)
    {
        mbBusy = true;
        buf = &mb;
    }
    scope (exit)
        if (buf is &mb)
            mbBusy = false;
    buf.clear();
    // wire: [retain u8][pubQos u8][seq u64 BE][topicLen u16][topic]
    //       [propsLen u32][props (v5 forwarded properties)][payload]
    // propsLen is u32 (not u16): v5 PUBLISH properties (many/large user-props) can
    // exceed 64KB — bounded only by MQTT_MAX_PACKET. A u16 here silently TRUNCATED
    // >64KB props mid-property, so cross-shard subscribers got a corrupt PUBLISH
    // (dropped) while same-shard subscribers got the full block — inconsistent,
    // silent loss. u32 carries the whole block, matching local delivery.
    immutable size_t plen = props.length;
    immutable size_t len = 12 + topic.length + 4 + plen + msg.length;
    auto raw = buf.freeSpace(len);
    if (raw.length < len)
        return; // OOM: drop the fanout, keep the broker up
    auto space = raw[0 .. len];
    space[0] = retain ? 1 : 0;
    space[1] = pubQos;
    foreach (k; 0 .. 8)
        space[2 + k] = cast(ubyte)(seq >> ((7 - k) * 8));
    space[10] = cast(ubyte)(topic.length >> 8);
    space[11] = cast(ubyte)(topic.length & 0xFF);
    space[12 .. 12 + topic.length] = cast(const(ubyte)[]) topic[];
    size_t w = 12 + topic.length;
    space[w] = cast(ubyte)(plen >> 24);
    space[w + 1] = cast(ubyte)(plen >> 16);
    space[w + 2] = cast(ubyte)(plen >> 8);
    space[w + 3] = cast(ubyte)(plen & 0xFF);
    w += 4;
    space[w .. w + plen] = (cast(const(ubyte)[]) props)[0 .. plen];
    w += plen;
    space[w .. $] = cast(const(ubyte)[]) msg[];
    buf.grow(len);
    foreach (uint s2; 0 .. gShardCount)
        if (s2 != tShard)
        {
            shardEnqueue(s2, buf.data, null, tShard, ShardMsg.mqttPub);
            shardWake(s2);
        }
}

// Copy a hopped BLOCKING command out of the transient ring slice and park a
// fiber that serves it as a LOCAL blocking command on this owner shard (phase
// 2.5b, see remoteBlockServe). Rare path: the GC alloc for the copy + task is
// fine here (same class as acquireShardPending's pool growth).
private void spawnRemoteBlock(const ref RVal cmd, uint reqShard, ShardPending* pend,
        uint db, bool resp3, bool noBlock) nothrow
{
    import dreads.shard : shardEnqueue, shardWake, ShardMsg;

    // re-encode: the RVal's slices die when the drain pops the ring slot; the
    // fiber owns a GC copy (also what roots it for the fiber's lifetime)
    static ByteBuffer enc; // TLS: only the drain fiber runs here, no yield before dup
    enc.clear();
    repArrayHeader(enc, cmd.arr.length);
    foreach (ref a; cmd.arr)
        repBulk(enc, a.str);
    auto bytes = enc.data.dup;
    try
        cast(void) runTask((ubyte[] by, uint rq, ShardPending* pd, uint dbx, bool r3,
                bool nb) nothrow {
            remoteBlockServe(by, rq, pd, dbx, r3, nb);
        }, bytes, reqShard, pend, db, resp3, noBlock);
    catch (Exception)
    {
        // spawn failed: reply an error so the requester never hangs
        static ByteBuffer eb; // TLS, drain fiber only
        eb.clear();
        repError(eb, "ERR shard: blocking task spawn failed");
        shardEnqueue(reqShard, eb.data, cast(void*) pend, 0, ShardMsg.reply);
        shardWake(reqShard);
    }
}

// Serve a hopped BLOCKING command on its key-owner shard, in its own fiber
// (phase 2.5b). A synthetic Conn (remoteBlock=true) stands in for the remote
// client, so the existing block machinery — gWaiters FIFO hand-off, the
// gKeyActivity fan-out, blockWait's tick — runs UNCHANGED against this shard's
// keyspace: the waiter and the wake are finally on the same thread. The
// requester's liveness (peer gone / CLIENT UNBLOCK) travels through
// pend.cancel; we ALWAYS reply — possibly empty on a disconnect — so the
// pending handshake completes and the requester's fiber never leaks.
private void remoteBlockServe(ubyte[] cmdBytes, uint reqShard, ShardPending* pend,
        uint db, bool resp3, bool noBlock) nothrow
{
    import dreads.shard : myKeyspace, shardEnqueue, shardWake, ShardMsg;

    ByteBuffer reply;
    Arena arena;
    Conn c; // fiber-local stand-in; NEVER in the conn registry, has no socket
    c.remoteBlock = true;
    c.remotePend = pend;
    c.dbp = myKeyspace(db);
    c.resp3 = resp3;
    c.inExec = noBlock; // MULTI/EXEC: the block loops bail after the first pass
    gRespProto = resp3 ? 3 : 2; // entry-time forms (nil hoists) in the requester's proto
    c.authed = true; // ACL ran on the requester before the hop (v1 hop contract)
    // gWaiters entries hold Conn* into THIS stack frame — purge before it dies
    scope (exit)
        waitPurgeConn(&c);
    RVal cmd;
    size_t pos = 0;
    bool served = false;
    if (parseValue(cmdBytes, pos, arena, cmd) == ParseStatus.ok
            && cmd.type == RType.Array && cmd.arr.length > 0)
    {
        auto name = cmd.arr[0].str;
        char[16] nbuf = void;
        if (name.length <= nbuf.length)
        {
            foreach (i, ch; name)
                nbuf[i] = ch >= 'a' && ch <= 'z' ? cast(char)(ch - 32) : ch;
            auto uname = cast(const(char)[]) nbuf[0 .. name.length];
            served = serveBlockingSwitch(c, uname, cmd, reply, arena);
            if (!served)
            {
                // predicate drift (e.g. XREAD with a malformed BLOCK form):
                // plain one-shot dispatch on this owner's keyspace is correct
                cast(void) dispatch(cmd, *c.dbp, reply, arena);
                served = true;
            }
        }
    }
    if (!served)
        repError(reply, "ERR shard: malformed blocking hop");
    if (gNotifyFlags) // events queued by the blocking serve (nobody else flushes here)
        flushPendingNotify();
    shardEnqueue(reqShard, reply.data, cast(void*) pend, 0, ShardMsg.reply);
    shardWake(reqShard);
}

// Owner shard of a command's first key, or -1 if it is keyless (runs locally on this
// router's own keyspace). `uname` is UPPERCASE (dispatch switch) but getCommandKeys
// matches the LOWERCASE gCmdKeySpecs, so lowercase it back — else every lookup misses
// and the command wrongly runs local (no hop). v1 routes by the FIRST key only.
// Owner shard of a command, or -1 (keyless → local) / SHARD_CROSSSLOT (a multi-key
// command whose keys span slots). `uname` is UPPERCASE; lowercase it for the spec
// tables. Single-key = O(1) first-key; multi-key = all-keys same-slot check.
enum int SHARD_CROSSSLOT = -2;

// Bit 15 of the packed opcode half of a hop section marks a BLOCKING serve
// (phase 2.5b): the owner must NOT dispatch it inline on the drain (a parked
// drain would stall the whole shard) but spawn a fiber that parks against the
// owner's own gWaiters/gKeyActivity (see remoteBlockServe). Command indexes are
// far below 0x8000, so the bit is free inside the 16-bit opcode field.
enum uint HOP_BLOCKING = 0x8000;

// Bit 14: the requester connection negotiated RESP3 (HELLO 3). The owner must
// encode the reply — nil forms, double scores, map/push frames — in the
// REQUESTER's protocol, not whatever its own last local command left in the
// thread-global gRespProto. Carried per hop section; the drain (and a blocking
// fiber) sets gRespProto from it around the dispatch.
enum uint HOP_RESP3 = 0x4000;

// Bit 13: serve the blocking command's NON-blocking form (MULTI/EXEC: Redis
// runs the one-shot equivalent and replies nil when there is nothing to serve).
// The owner fiber sets its synthetic conn's inExec, so the existing block loops
// bail after the first pass — against the OWNER's keyspace, which is the point
// (the local path read the ROUTER's keyspace and saw the wrong data).
enum uint HOP_NOBLOCK = 0x2000;

// IR-1: the caller resolved the opcode (one lowercase+hash per command, at the
// top of executeCommand) — this only maps it to a slot and a shard.
private int shardOwnerOf(const ref RVal cmd, int opcode, scope const(char)[] lname) nothrow
{
    import dreads.shard : shardOfSlot;
    import dreads.acl : commandRouteSlot, ROUTE_CROSSSLOT;

    immutable slot = commandRouteSlot(opcode, cast(string) lname, cmd.arr);
    if (slot == ROUTE_CROSSSLOT)
        return SHARD_CROSSSLOT;
    if (slot < 0)
        return -1; // keyless → run locally on this router's own keyspace
    return cast(int) shardOfSlot(cast(ushort) slot);
}

// FIRE a keyed command at its owning shard WITHOUT blocking (the raw RESP bytes are
// copied into the owner's inbound ring; the owner's drain fiber runs it on its own
// keyspace and ships the reply back). The ShardPending is recorded on the connection
// in command order; its reply is reaped later by flushShardPending. This is the async
// hop: many commands (to many shards) are in flight at once, all shards busy in
// parallel — instead of one blocking round-trip per command. The connection only ever
// blocks at a flush point, in order. Reaping-when-full keeps per-conn state bounded.
// --- RESP→bytecode cross-shard hop -------------------------------------------------
// The owner used to RE-PARSE the raw RESP bytes on every hop (perf: ~505 ins/op).
// The requester ALREADY parsed the command in the serve loop, so instead of shipping
// raw RESP for the owner to re-scan+validate, ship a tiny descriptor it computed for
// free — the arg offsets into the raw bytes — and the owner rebuilds the RVal by
// slicing, no parseValue. Wire form (all args are bulk strings, as every client
// command is):  [u32 argc][ (u32 off, u32 len) × argc ][ raw command bytes ]
// `off` is relative to the start of the raw bytes (== the requester's rawCmd).
// Append ONE command as a SECTION of an owner's coalesced hop message:
//   [u32 sectionBytes][u64 pending][u32 argc][i32 opcode][(u32 off,u32 len)×argc][raw]
// `off` is relative to the section's own raw bytes. The pending pointer rides in
// the section (the slot carries K commands, so the slot-level tag can't).
private void appendHopCmd(ref ByteBuffer bc, const ref RVal cmd, scope const(ubyte)[] rawCmd,
        int opcode, uint db, void* pend) @trusted nothrow
{
    immutable uint argc = cast(uint) cmd.arr.length;
    immutable size_t hdrBytes = 4 + 8 + 8 + cast(size_t) argc * 8;
    immutable size_t sect = hdrBytes + rawCmd.length;
    // one reserve for the whole section, then indexed stores through typed
    // slices (bounds carried by the slice)
    auto raw = bc.freeSpace(sect);
    if (raw.length < sect)
        return; // OOM: can't stage the hop command; drop it rather than crash
    auto space = raw[0 .. sect];
    auto w = cast(uint[]) space[0 .. 4];
    w[0] = cast(uint)(sect - 4); // bytes after this length field
    *cast(ulong*)(space.ptr + 4) = cast(ulong) pend;
    auto h = cast(uint[]) space[12 .. hdrBytes];
    h[0] = argc;
    // opcode in the low 16 bits, the requester's current db in the high 16 — the
    // owner runs the command on ITS OWN gShardKs[owner][db] (per-shard 16 DBs)
    h[1] = (cast(uint) opcode & 0xFFFF) | (db << 16);
    auto base = cast(const(char)*) rawCmd.ptr;
    size_t j = 2;
    foreach (ref a; cmd.arr)
    {
        h[j++] = cast(uint)(a.str.ptr - base); // offset into rawCmd
        h[j++] = cast(uint) a.str.length;
    }
    space[hdrBytes .. $] = cast(const(ubyte)[]) rawCmd[];
    bc.grow(sect);
}

// Owner side: rebuild the RVal from the descriptor + raw bytes (which live in the
// ring slice `payload`, valid until the drain pops it — same lifetime parseValue's
// zero-copy slices had). No framing scan, no validation loop.
// Decode the section at `pos` of a coalesced hop message (see appendHopCmd),
// advancing `pos` past it. The rebuilt slices point into the ring slice — valid
// until the drain pops, exactly like the single-command descriptor was.
private bool decodeHopSection(scope const(ubyte)[] payload, ref size_t pos, ref Arena arena,
        out RVal cmd, out int opcode, out uint db, out bool blocking, out bool resp3,
        out bool noblock, out void* pend, out const(ubyte)[] rawOut) @trusted nothrow
{
    if (payload.length - pos < 4)
        return false;
    immutable uint sect = *cast(const(uint)*)(payload.ptr + pos);
    if (sect < 16 || payload.length - pos - 4 < sect)
        return false;
    auto sec = payload[pos + 4 .. pos + 4 + sect];
    pos += 4 + sect;
    pend = cast(void*)*cast(const(ulong)*) sec.ptr;
    immutable uint argc = *cast(const(uint)*)(sec.ptr + 8);
    immutable uint packed = *cast(const(uint)*)(sec.ptr + 12);
    opcode = cast(int)(packed & 0x1FFF);
    blocking = (packed & HOP_BLOCKING) != 0;
    resp3 = (packed & HOP_RESP3) != 0;
    noblock = (packed & HOP_NOBLOCK) != 0;
    db = packed >> 16;
    immutable size_t hdr = 16 + cast(size_t) argc * 8;
    if (argc == 0 || sec.length < hdr)
        return false;
    auto raw = sec[hdr .. $];
    rawOut = raw; // the original RESP bytes (the owner's AOF logs these verbatim)
    auto arr = arena.allocArray!RVal(argc);
    auto offs = sec.ptr + 16;
    foreach (i; 0 .. argc)
    {
        immutable uint off = *cast(const(uint)*)(offs + i * 8);
        immutable uint len = *cast(const(uint)*)(offs + i * 8 + 4);
        if (cast(size_t) off + len > raw.length)
            return false;
        arr[i].type = RType.BulkString;
        arr[i].str = cast(const(char)[]) raw[off .. off + len];
    }
    cmd.type = RType.Array;
    cmd.arr = arr;
    return true;
}

// --- BROADCAST primitive (keyless keyspace-wide commands) ------------------------
// A keyless command whose answer spans ALL shards (KEYS/DBSIZE/SCAN/FLUSH*/…) is
// fired to every shard over the SAME hop transport; each shard runs it on its own
// gShardKs[shard][db] (the drain's dispatch already does this — the partial reply),
// and the router MERGES the N replies. This is Redis-cluster-shaped, but it is also
// the fan-out core of the broker engine (pub/sub delivery, per-topic aggregation).

enum BroadcastKind : ubyte
{
    none = 0, // not a broadcast command
    sumInt, // DBSIZE / PUBLISH / *numsub — Σ of the :N replies
    gateOk, // FLUSHALL / FLUSHDB — +OK iff every shard replied +OK
    concatArr, // KEYS — one flat array = concat of the N arrays
    randomNonNil, // RANDOMKEY — a RANDOM non-$-1 reply (uniform across shards)
    unionArr, // PUBSUB (SHARD)CHANNELS — union of the N bulk arrays, deduped
    sumPairs, // PUBSUB (SHARD)NUMSUB — [ch,:n]* pairs, counts summed pairwise
    unionCount, // PUBSUB NUMPAT — :|union| of per-shard pattern-name arrays
    infoMerge, // INFO — per-field numeric aggregation of the N section texts
}

// CTFE opcode → BroadcastKind (indexed by aclCmdIndex, like gRouteFirstKey).
private immutable BroadcastKind[gCmdCats.length] gBroadcastKind = () {
    import dreads.aclcat : cmdIx;

    BroadcastKind[gCmdCats.length] t;
    t[cmdIx!"dbsize"] = BroadcastKind.sumInt;
    t[cmdIx!"flushdb"] = BroadcastKind.gateOk; // clears each shard's current db
    t[cmdIx!"flushall"] = BroadcastKind.gateOk; // each shard clears its own 16 dbs
    t[cmdIx!"keys"] = BroadcastKind.concatArr;
    t[cmdIx!"randomkey"] = BroadcastKind.randomNonNil;
    // deferred: publish (dual pub/sub delivery path) — see the pub/sub phase.
    return t;
}();

BroadcastKind broadcastKindOf(int opcode) @nogc nothrow
{
    return (opcode >= 0 && opcode < cast(int) gBroadcastKind.length)
        ? gBroadcastKind[opcode] : BroadcastKind.none;
}

// SCAN across shards: NOT a broadcast — a COMPOSITE cursor walks one shard at a
// time. cursor = (shardIdx << 56) | innerCursor. Each call routes SCAN (with the
// inner cursor) to the current shard; when that shard is exhausted (inner→0) the
// next call advances to shard+1; cursor 0 when all shards are done. Keys are
// returned as they come — the same weak SCAN guarantee Redis gives.
private enum int SCAN_SHARD_SHIFT = 56;
private enum ulong SCAN_INNER_MASK = (1UL << SCAN_SHARD_SHIFT) - 1;

private void scanSharded(ref Conn c, int opcode, uint db, const ref RVal cmd,
        ref ByteBuffer o, ref Arena arena) nothrow
{
    import dreads.shard : tShard, gShardCount, shardEnqueue, shardWake, ShardMsg,
        ShardPending, acquireShardPending, releaseShardPending;
    import dreads.resp : parseValue, ParseStatus;

    if (c.shardPendCount > 0)
        flushShardPending(c, o);

    // decode the composite cursor
    long cur;
    if (cmd.arr.length < 2 || !parseLong(cmd.arr[1].str, cur) || cur < 0)
    {
        repError(o, "ERR invalid cursor");
        return;
    }
    immutable ulong ucur = cast(ulong) cur;
    uint shard = cast(uint)(ucur >> SCAN_SHARD_SHIFT);
    immutable ulong inner = ucur & SCAN_INNER_MASK;
    if (shard >= gShardCount)
    {
        scanEmit(o, 0, null); // out of range → done
        return;
    }

    // re-serialize SCAN with the INNER cursor (arg[1]), keeping the rest verbatim
    static ByteBuffer mbuf;
    mbuf.clear();
    char[24] nb = void;
    immutable nn = snprintfLong(nb[], cast(long) inner);
    mbuf.append("*");
    appendLongAscii(mbuf, cast(long) cmd.arr.length);
    mbuf.append("\r\n");
    foreach (i, ref a; cmd.arr)
    {
        auto part = (i == 1) ? cast(const(char)[]) nb[0 .. nn] : a.str;
        mbuf.append("$");
        appendLongAscii(mbuf, cast(long) part.length);
        mbuf.append("\r\n");
        mbuf.append(part);
        mbuf.append("\r\n");
    }

    // parse it back into an RVal so the hop descriptor's arg offsets are valid
    RVal mcmd;
    size_t pp = 0;
    if (parseValue(mbuf.data, pp, arena, mcmd) != ParseStatus.ok)
    {
        repError(o, "ERR scan: internal");
        return;
    }

    // fire the modified SCAN to `shard`, collect its reply
    auto pend = acquireShardPending();
    c.shardBc.clear();
    appendHopCmd(c.shardBc, mcmd, mbuf.data,
            cast(int)(cast(uint) opcode | (c.resp3 ? HOP_RESP3 : 0)), db, cast(void*) pend);
    shardEnqueue(shard, c.shardBc.data, null, tShard, ShardMsg.cmd);
    shardWake(shard);
    while (!pend.ready)
    {
        auto ec = pend.done.emitCount;
        if (pend.ready)
            break;
        try
            pend.done.wait(ec);
        catch (Exception)
        {
        }
    }
    // reply is *2\r\n$<L>\r\n<innerNext>\r\n<keys-array-bytes>, or an error
    auto r = pend.reply.data;
    if (r.length && r[0] == '-') // the shard rejected it (bad TYPE, etc.) — propagate
    {
        o.append(r);
        releaseShardPending(pend);
        return;
    }
    ulong innerNext;
    size_t keysAt;
    immutable okParse = scanReplyParse(r, innerNext, keysAt);
    // compute the next composite cursor
    ulong nextCur;
    if (!okParse)
        nextCur = 0;
    else if (innerNext != 0)
        nextCur = (cast(ulong) shard << SCAN_SHARD_SHIFT) | (innerNext & SCAN_INNER_MASK);
    else if (shard + 1 < gShardCount)
        nextCur = cast(ulong)(shard + 1) << SCAN_SHARD_SHIFT; // next shard, inner 0
    else
        nextCur = 0; // all shards done
    // emit: [nextCur, <keys array bytes verbatim from the shard reply>]
    o.append("*2\r\n$");
    char[24] cb = void;
    immutable cn = snprintfLong(cb[], cast(long) nextCur);
    appendLongAscii(o, cast(long) cn);
    o.append("\r\n");
    o.append(cb[0 .. cn]);
    o.append("\r\n");
    if (okParse && keysAt <= r.length)
        o.append(r[keysAt .. $]);
    else
        o.append("*0\r\n");
    releaseShardPending(pend);
}

// Parse a SCAN reply `*2\r\n$L\r\n<cursor>\r\n<keys...>` → the cursor value and
// the offset where the keys array begins. false on a malformed reply.
private bool scanReplyParse(scope const(ubyte)[] r, out ulong cursor, out size_t keysAt) @nogc nothrow
{
    // skip "*2\r\n"
    if (r.length < 4 || r[0] != '*')
        return false;
    size_t i = 0;
    while (i < r.length && r[i] != '\n')
        i++;
    i++; // past *2\r\n
    if (i >= r.length || r[i] != '$')
        return false;
    // bulk length of the cursor
    size_t j = i + 1;
    long blen = 0;
    while (j < r.length && r[j] >= '0' && r[j] <= '9')
        blen = blen * 10 + (r[j++] - '0');
    j += 2; // past \r\n
    // the cursor digits
    cursor = 0;
    foreach (k; 0 .. cast(size_t) blen)
    {
        if (j + k >= r.length)
            return false;
        immutable d = r[j + k];
        if (d >= '0' && d <= '9')
            cursor = cursor * 10 + (d - '0');
    }
    keysAt = j + cast(size_t) blen + 2; // past the cursor + \r\n
    return keysAt <= r.length;
}

// SCAN reply with an explicit cursor + a key list (used for the empty/out-of-range case).
private void scanEmit(ref ByteBuffer o, ulong cursor, const(char)[][] keys) nothrow
{
    o.append("*2\r\n$");
    char[24] cb = void;
    immutable cn = snprintfLong(cb[], cast(long) cursor);
    appendLongAscii(o, cast(long) cn);
    o.append("\r\n");
    o.append(cb[0 .. cn]);
    o.append("\r\n");
    appendLongAscii2Array(o, keys);
}

// helpers: decimal ascii of a long into a buffer (returns length)
private size_t snprintfLong(char[] buf, long v) @nogc nothrow
{
    import core.stdc.stdio : snprintf;

    return cast(size_t) snprintf(buf.ptr, buf.length, "%lld", v);
}

private void appendLongAscii(ref ByteBuffer o, long v) nothrow
{
    char[24] b = void;
    immutable n = snprintfLong(b[], v);
    o.append(b[0 .. n]);
}

private void appendLongAscii2Array(ref ByteBuffer o, const(char)[][] keys) nothrow
{
    o.append("*");
    appendLongAscii(o, cast(long) keys.length);
    o.append("\r\n");
    foreach (k; keys)
    {
        o.append("$");
        appendLongAscii(o, cast(long) k.length);
        o.append("\r\n");
        o.append(k);
        o.append("\r\n");
    }
}

// Fire `cmd` to EVERY shard (each runs it locally), collect the N replies, merge
// them into `o` per `kind`. Blocks at the collect point like flushShardPending.
private void broadcastCommand(ref Conn c, int opcode, uint db, const ref RVal cmd,
        scope const(ubyte)[] rawCmd, BroadcastKind kind, ref ByteBuffer o) nothrow
{
    import dreads.shard : tShard, gShardCount, shardEnqueue, shardWake, ShardMsg,
        ShardPending, acquireShardPending, releaseShardPending;

    // any in-flight keyed hops must be reaped first — this reply comes after them
    if (c.shardPendCount > 0)
        flushShardPending(c, o);

    immutable n = gShardCount;
    ShardPending*[64] pend = void; // gShardCount <= MAX_SHARDS; 64 covers the wake bitmap
    if (n > pend.length)
    {
        repError(o, "ERR broadcast: too many shards");
        return;
    }
    // fire one single-section batch to each shard, carrying its own pending
    foreach (uint sIdx; 0 .. n)
    {
        auto p = acquireShardPending();
        pend[sIdx] = p;
        c.shardBc.clear();
        appendHopCmd(c.shardBc, cmd, rawCmd,
                cast(int)(cast(uint) opcode | (c.resp3 ? HOP_RESP3 : 0)), db, cast(void*) p);
        shardEnqueue(sIdx, c.shardBc.data, null, tShard, ShardMsg.cmd);
    }
    foreach (uint sIdx; 0 .. n)
        shardWake(sIdx);
    // collect (block per pending; the drain fiber delivers while we wait)
    foreach (uint sIdx; 0 .. n)
    {
        auto p = pend[sIdx];
        while (!p.ready)
        {
            auto ec = p.done.emitCount;
            if (p.ready)
                break;
            try
                p.done.wait(ec);
            catch (Exception)
            {
            }
        }
    }
    // merge
    mergeBroadcast(kind, pend[0 .. n], o);
    foreach (uint sIdx; 0 .. n)
        releaseShardPending(pend[sIdx]);
}

// Merge the N partial RESP replies into `o` according to `kind`.
private void mergeBroadcast(BroadcastKind kind, ShardPending*[] pend, ref ByteBuffer o) nothrow
{

    final switch (kind)
    {
    case BroadcastKind.none:
        break;
    case BroadcastKind.sumInt:
        long total = 0;
        foreach (p; pend)
            total += parseRespInt(p.reply.data);
        repInt(o, total);
        break;
    case BroadcastKind.gateOk:
        // propagate the first error, else +OK
        foreach (p; pend)
            if (p.reply.data.length && p.reply.data[0] == '-')
            {
                o.append(p.reply.data);
                return;
            }
        repSimple(o, "OK");
        break;
    case BroadcastKind.concatArr:
        // each reply is *M\r\n<M bulk strings>; emit *ΣM then all the bodies
        long total = 0;
        foreach (p; pend)
            total += parseRespArrayLen(p.reply.data);
        repArrayHeader(o, total < 0 ? 0 : total);
        foreach (p; pend)
        {
            auto d = p.reply.data;
            immutable body_ = respArrayBody(d);
            if (body_ > 0 && body_ <= d.length)
                o.append(d[body_ .. $]);
        }
        break;
    case BroadcastKind.randomNonNil:
        // RANDOMKEY: each shard replied with one of its own keys at random (or
        // nil if empty). Pick uniformly among the non-nil shard replies so the
        // draw isn't biased toward the lowest-indexed shard. Not weighted by a
        // shard's key count — same approximation Redis Cluster makes (it samples
        // one node), good enough for a random-key probe.
        import dreads.rand : randBelow;

        static bool isNil(scope const(ubyte)[] d) @nogc nothrow
        {
            return !(d.length >= 3 && !(d[0] == '$' && d[1] == '-'));
        }

        size_t live = 0;
        foreach (p; pend)
            if (!isNil(p.reply.data))
                live++;
        if (live == 0)
        {
            o.append("$-1\r\n");
            break;
        }
        size_t pick = randBelow(live);
        foreach (p; pend)
        {
            auto d = p.reply.data;
            if (isNil(d))
                continue;
            if (pick == 0)
            {
                o.append(d);
                break;
            }
            pick--;
        }
        break;
    case BroadcastKind.unionArr:
        // PUBSUB (SHARD)CHANNELS: a channel can have subscribers on several
        // routers — union the per-shard arrays and dedup (reply order is
        // unspecified, like Redis).
        static const(char)[][1024] uniqA; // TLS; slices point into pend replies
        immutable n = collectUnionBulks(pend, uniqA[]);
        repArrayHeader(o, n);
        foreach (u; uniqA[0 .. n])
            repBulk(o, u);
        break;
    case BroadcastKind.unionCount:
        // PUBSUB NUMPAT: each shard replied its distinct pattern NAMES (see the
        // drain's pubsub special-case) — the answer is the size of the union
        // (a sum would overcount a pattern subscribed on two routers).
        static const(char)[][1024] uniqC; // TLS
        repInt(o, cast(long) collectUnionBulks(pend, uniqC[]));
        break;
    case BroadcastKind.infoMerge:
        mergeInfoTexts(pend, o);
        break;
    case BroadcastKind.sumPairs:
        // PUBSUB (SHARD)NUMSUB: every shard answers the SAME requested channels
        // in the SAME order — walk the replies in lockstep and sum the counts.
        import dreads.commands : parseLong;

        static const(char)[][512] chNames; // TLS; slices into the first reply
        static long[512] chSums;
        size_t k = 0;
        bool first = true;
        foreach (p; pend)
        {
            auto d = p.reply.data;
            size_t pos = 0;
            const(char)[] hdr;
            if (!respLine(d, pos, hdr) || hdr.length < 2 || hdr[0] != '*')
                continue;
            size_t i = 0;
            while (pos < d.length && d[pos] == '$')
            {
                const(char)[] lenLine;
                if (!respLine(d, pos, lenLine))
                    break;
                long bl;
                if (lenLine.length < 2 || !parseLong(lenLine[1 .. $], bl) || bl < 0
                        || pos + cast(size_t) bl + 2 > d.length)
                    break;
                auto ch = cast(const(char)[]) d[pos .. pos + cast(size_t) bl];
                pos += cast(size_t) bl + 2;
                if (pos >= d.length || d[pos] != ':')
                    break;
                const(char)[] numLine;
                if (!respLine(d, pos, numLine))
                    break;
                long cnt = 0;
                cast(void) parseLong(numLine[1 .. $], cnt);
                if (first)
                {
                    if (i < chNames.length)
                    {
                        chNames[i] = ch;
                        chSums[i] = cnt;
                        k = i + 1;
                    }
                }
                else if (i < k)
                    chSums[i] += cnt;
                i++;
            }
            first = false;
        }
        repArrayHeader(o, k * 2);
        foreach (i; 0 .. k)
        {
            repBulk(o, chNames[i]);
            repInt(o, chSums[i]);
        }
        break;
    }
}

// --- INFO cross-shard aggregation (phase 2.5c, workstream (d)) -----------------
// Each shard rendered its own full INFO text (the drain's plain dispatch — its
// keyspace section iterates that shard's own db slice). The merge takes THIS
// shard's text as the base and rewrites the per-shard numeric fields:
//   - scalar counters that are TLS per shard (connected_clients, used_memory,
//     expired/evicted, total_error_replies, rdb_changes_since_last_save, ACL
//     denies, migrate sockets) are SUMMED across the N texts;
//   - blocked_clients is already a shared global (every text agrees) — kept;
//   - cmdstat_/errorstat_/dbN lines are UNIONED (a command counted only on the
//     key-owner shard must still appear) with their numeric fields summed.
// Everything else (versions, config mirrors, paused state) keeps the base value.
private immutable string[] INFO_SUM_FIELDS = [
    "connected_clients", "used_memory", "mem_clients_normal", "expired_keys",
    "expired_fields", "evicted_keys", "total_error_replies",
    "migrate_cached_sockets", "acl_access_denied_auth", "acl_access_denied_cmd",
    "acl_access_denied_key", "acl_access_denied_channel",
    "rdb_changes_since_last_save",
];

// Extract the text payload of a bulk ($) or verbatim (=) INFO reply.
private const(char)[] infoReplyText(return scope const(ubyte)[] d) @nogc nothrow @trusted
{
    if (d.length < 4 || (d[0] != '$' && d[0] != '='))
        return null;
    size_t pos = 0;
    const(char)[] hdr;
    if (!respLine(d, pos, hdr))
        return null;
    auto t = cast(const(char)[]) d[pos .. d.length >= pos + 2 ? d.length - 2 : pos];
    if (d[0] == '=' && t.length >= 4 && t[0 .. 4] == "txt:")
        t = t[4 .. $]; // verbatim carries a format prefix
    return t;
}

// Find `name` as a line key in `text` ("name:..." at a line start), returning
// the value slice up to the line end (null if absent).
private const(char)[] infoFindLine(return scope const(char)[] text, scope const(char)[] name) @nogc nothrow @trusted
{
    size_t i = 0;
    while (i < text.length)
    {
        if (i + name.length < text.length && text[i .. i + name.length] == name
                && text[i + name.length] == ':')
        {
            size_t v = i + name.length + 1, e = v;
            while (e < text.length && text[e] != '\r' && text[e] != '\n')
                e++;
            return text[v .. e];
        }
        while (i < text.length && text[i] != '\n')
            i++;
        i++;
    }
    return null;
}

// Parse the leading unsigned integer of `v` (0 if none/null).
private ulong infoNum(scope const(char)[] v) @nogc nothrow
{
    ulong n = 0;
    foreach (ch; v)
    {
        if (ch < '0' || ch > '9')
            break;
        n = n * 10 + (ch - '0');
    }
    return n;
}

// Extract `field=` inside a comma-separated value like "calls=3,usec=0,...".
private ulong infoField(scope const(char)[] v, scope const(char)[] field) @nogc nothrow
{
    size_t i = 0;
    while (i < v.length)
    {
        if (i + field.length + 1 <= v.length && v[i .. i + field.length] == field
                && v[i + field.length] == '=')
            return infoNum(v[i + field.length + 1 .. $]);
        while (i < v.length && v[i] != ',')
            i++;
        i++;
    }
    return 0;
}

private bool infoIsSumField(scope const(char)[] name) @nogc nothrow
{
    foreach (f; INFO_SUM_FIELDS)
        if (name == f)
            return true;
    return false;
}

// Advance past a family's lines (prefix-keyed) following a section header.
private size_t infoSkipFamily(scope const(char)[] t, size_t i, scope const(char)[] prefix) @nogc nothrow
{
    while (i < t.length)
    {
        if (!(i + prefix.length < t.length && t[i .. i + prefix.length] == prefix))
            break;
        while (i < t.length && t[i] != '\n')
            i++;
        i++;
    }
    return i;
}

// Collect the distinct `prefix`-keyed line keys across all texts into `keys`.
private size_t infoCollectKeys(scope const(char)[][] texts, scope const(char)[] prefix,
        const(char)[][] keys, bool dbNumeric) @nogc nothrow
{
    size_t nk = 0;
    foreach (t; texts)
    {
        size_t i = 0;
        while (i < t.length)
        {
            if (i + prefix.length < t.length && t[i .. i + prefix.length] == prefix)
            {
                size_t e = i;
                while (e < t.length && t[e] != ':' && t[e] != '\n')
                    e++;
                if (e < t.length && t[e] == ':')
                {
                    auto key = t[i .. e];
                    bool ok = true;
                    if (dbNumeric) // require db<digits> exactly (not "dbsize")
                    {
                        ok = key.length > prefix.length;
                        foreach (ch; key[prefix.length .. $])
                            if (ch < '0' || ch > '9')
                            {
                                ok = false;
                                break;
                            }
                    }
                    if (ok)
                    {
                        bool dup = false;
                        foreach (k2; keys[0 .. nk])
                            if (k2 == key)
                            {
                                dup = true;
                                break;
                            }
                        if (!dup && nk < keys.length)
                            keys[nk++] = key;
                    }
                }
            }
            while (i < t.length && t[i] != '\n')
                i++;
            i++;
        }
    }
    return nk;
}

// cmdstat lines: calls/usec summed, the literal usec_per_call kept, rejected/
// failed summed — rendered exactly like the local emit so `cmdrstat` matches.
private void infoEmitCmdstats(ref ByteBuffer ob, scope const(char)[][] texts) nothrow
{
    import core.stdc.stdio : snprintf;

    static const(char)[][1024] keys; // TLS
    immutable nk = infoCollectKeys(texts, "cmdstat_", keys[], false);
    char[256] lb = void;
    foreach (key; keys[0 .. nk])
    {
        ulong calls = 0, usec = 0, rej = 0, fail = 0;
        foreach (t; texts)
        {
            auto v = infoFindLine(t, key);
            if (v is null)
                continue;
            calls += infoField(v, "calls");
            usec += infoField(v, "usec");
            rej += infoField(v, "rejected_calls");
            fail += infoField(v, "failed_calls");
        }
        auto n = snprintf(lb.ptr, lb.length,
                "%.*s:calls=%llu,usec=%llu,usec_per_call=0.00,rejected_calls=%llu,failed_calls=%llu\r\n",
                cast(int) key.length, key.ptr, calls, usec, rej, fail);
        ob.append(lb[0 .. n]);
    }
}

private void infoEmitErrorstats(ref ByteBuffer ob, scope const(char)[][] texts) nothrow
{
    import core.stdc.stdio : snprintf;

    static const(char)[][512] keys; // TLS
    immutable nk = infoCollectKeys(texts, "errorstat_", keys[], false);
    char[256] lb = void;
    foreach (key; keys[0 .. nk])
    {
        ulong count = 0;
        foreach (t; texts)
        {
            auto v = infoFindLine(t, key);
            if (v !is null)
                count += infoField(v, "count");
        }
        auto n = snprintf(lb.ptr, lb.length, "%.*s:count=%llu\r\n",
                cast(int) key.length, key.ptr, count);
        ob.append(lb[0 .. n]);
    }
}

// db lines: keys/expires/volatile-items summed, avg_ttl kept 0.
private void infoEmitKeyspace(ref ByteBuffer ob, scope const(char)[][] texts) nothrow
{
    import core.stdc.stdio : snprintf;

    static const(char)[][32] keys; // TLS
    immutable nk = infoCollectKeys(texts, "db", keys[], true);
    char[256] lb = void;
    foreach (key; keys[0 .. nk])
    {
        ulong keysN = 0, expires = 0, vol = 0;
        foreach (t; texts)
        {
            auto v = infoFindLine(t, key);
            if (v is null)
                continue;
            keysN += infoField(v, "keys");
            expires += infoField(v, "expires");
            vol += infoField(v, "keys_with_volatile_items");
        }
        auto n = snprintf(lb.ptr, lb.length,
                "%.*s:keys=%llu,expires=%llu,avg_ttl=0,keys_with_volatile_items=%llu\r\n",
                cast(int) key.length, key.ptr, keysN, expires, vol);
        ob.append(lb[0 .. n]);
    }
}

// The infoMerge body: base = this shard's own text, fields rewritten per policy.
private void mergeInfoTexts(ShardPending*[] pend, ref ByteBuffer o) nothrow
{
    import core.stdc.stdio : snprintf;
    import dreads.resp : repVerbatim;
    import dreads.shard : tShard;

    static const(char)[][64] textsBuf; // TLS
    size_t nt = 0;
    foreach (p; pend)
    {
        auto t = infoReplyText(p.reply.data);
        if (t !is null && nt < textsBuf.length)
            textsBuf[nt++] = t;
    }
    auto texts = textsBuf[0 .. nt];
    if (texts.length == 0)
    {
        repVerbatim(o, "txt", "");
        return;
    }
    auto base = tShard < texts.length ? texts[tShard] : texts[0];
    static ByteBuffer mb; // TLS: the merged text
    mb.clear();
    char[256] lb = void;
    size_t i = 0;
    while (i < base.length)
    {
        size_t le = i;
        while (le < base.length && base[le] != '\r' && base[le] != '\n')
            le++;
        auto line = base[i .. le];
        size_t next = le;
        while (next < base.length && (base[next] == '\r' || base[next] == '\n'))
        {
            if (base[next] == '\n')
            {
                next++;
                break;
            }
            next++;
        }
        size_t c = 0;
        while (c < line.length && line[c] != ':')
            c++;
        auto key = line[0 .. c];
        if (c < line.length && infoIsSumField(key))
        {
            ulong sum = 0;
            foreach (t; texts)
                sum += infoNum(infoFindLine(t, key));
            auto n = snprintf(lb.ptr, lb.length, "%.*s:%llu\r\n",
                    cast(int) key.length, key.ptr, sum);
            mb.append(lb[0 .. n]);
        }
        else if (line == "# Commandstats")
        {
            mb.append("# Commandstats\r\n");
            infoEmitCmdstats(mb, texts);
            i = infoSkipFamily(base, next, "cmdstat_");
            continue;
        }
        else if (line == "# Errorstats")
        {
            mb.append("# Errorstats\r\n");
            infoEmitErrorstats(mb, texts);
            i = infoSkipFamily(base, next, "errorstat_");
            continue;
        }
        else if (line == "# Keyspace")
        {
            mb.append("# Keyspace\r\n");
            infoEmitKeyspace(mb, texts);
            i = infoSkipFamily(base, next, "db");
            continue;
        }
        else
        {
            mb.append(line);
            mb.append("\r\n");
        }
        i = next;
    }
    repVerbatim(o, "txt", cast(const(char)[]) mb.data);
}

// Collect the union of the bulk strings of N array replies into `uniq`
// (deduped, order of first appearance). Bounded: past uniq.length the tail is
// dropped (introspection scale, not data). Returns the union size.
private size_t collectUnionBulks(ShardPending*[] pend, const(char)[][] uniq) nothrow
{
    import dreads.commands : parseLong;

    size_t n = 0;
    foreach (p; pend)
    {
        auto d = p.reply.data;
        size_t pos = 0;
        const(char)[] hdr;
        if (!respLine(d, pos, hdr) || hdr.length < 2 || hdr[0] != '*')
            continue;
        while (pos < d.length && d[pos] == '$')
        {
            const(char)[] lenLine;
            if (!respLine(d, pos, lenLine))
                break;
            long bl;
            if (lenLine.length < 2 || !parseLong(lenLine[1 .. $], bl) || bl < 0
                    || pos + cast(size_t) bl + 2 > d.length)
                break;
            auto ch = cast(const(char)[]) d[pos .. pos + cast(size_t) bl];
            pos += cast(size_t) bl + 2;
            bool dup = false;
            foreach (u; uniq[0 .. n])
                if (u == ch)
                {
                    dup = true;
                    break;
                }
            if (!dup && n < uniq.length)
                uniq[n++] = ch;
        }
    }
    return n;
}

// Advance `pos` past one CRLF-terminated line of `d`, yielding its body.
private bool respLine(scope const(ubyte)[] d, ref size_t pos, out const(char)[] line) @nogc nothrow
{
    size_t e = pos;
    while (e + 1 < d.length && !(d[e] == '\r' && d[e + 1] == '\n'))
        e++;
    if (e + 1 >= d.length)
        return false;
    line = cast(const(char)[]) d[pos .. e];
    pos = e + 2;
    return true;
}

// Parse a leading RESP integer reply `:N\r\n` → N (0 on anything else).
private long parseRespInt(scope const(ubyte)[] d) @nogc nothrow
{
    if (d.length < 3 || d[0] != ':')
        return 0;
    long v = 0;
    bool neg = false;
    size_t i = 1;
    if (i < d.length && d[i] == '-')
    {
        neg = true;
        i++;
    }
    for (; i < d.length && d[i] >= '0' && d[i] <= '9'; i++)
        v = v * 10 + (d[i] - '0');
    return neg ? -v : v;
}

// Parse a RESP array header `*M\r\n` → M (0 on anything else / empty).
private long parseRespArrayLen(scope const(ubyte)[] d) @nogc nothrow
{
    if (d.length < 4 || d[0] != '*')
        return 0;
    long v = 0;
    for (size_t i = 1; i < d.length && d[i] >= '0' && d[i] <= '9'; i++)
        v = v * 10 + (d[i] - '0');
    return v;
}

// Offset of the array BODY (past `*M\r\n`), or -1.
private size_t respArrayBody(scope const(ubyte)[] d) @nogc nothrow
{
    foreach (i; 0 .. d.length - 1)
        if (d[i] == '\r' && d[i + 1] == '\n')
            return i + 2;
    return 0;
}

private void shardFire(ref Conn c, int owner, int opcode, uint db, const ref RVal cmd,
        scope const(ubyte)[] rawCmd, ref ByteBuffer o) nothrow
{
    import dreads.shard : tShard, shardEnqueue, shardEnqueue2, shardWake,
        acquireShardPending, ShardMsg;

    auto p = acquireShardPending();
    if (owner < 64)
    {
        // COALESCED: stage the command in this owner's batch buffer; the whole
        // batch travels as ONE ring slot at the flush point (with the wakes).
        appendHopCmd(c.hopBatch[owner], cmd, rawCmd,
                cast(int)(cast(uint) opcode | (c.resp3 ? HOP_RESP3 : 0)), db, cast(void*) p);
        c.shardTouch |= 1UL << owner;
    }
    else
    {
        // rare high shard id (beyond the touch/batch bitmaps): fire a batch of
        // one immediately, and wake now
        c.shardBc.clear();
        appendHopCmd(c.shardBc, cmd, rawCmd,
                cast(int)(cast(uint) opcode | (c.resp3 ? HOP_RESP3 : 0)), db, cast(void*) p);
        shardEnqueue(cast(uint) owner, c.shardBc.data, null, tShard, ShardMsg.cmd);
        shardWake(cast(uint) owner);
    }
    if (c.shardPendCount == PIPELINE_CAP)
        flushShardPending(c, o); // buffer full: flush batches + reap in order, then continue
    c.shardPends[c.shardPendCount++] = cast(void*) p;
}

// Reap every in-flight shard hop, appending each reply in command order (so the output
// stays in pipeline order and a following inline command observes them). The ONLY place
// the connection fiber blocks on a hop — at the batch boundary, not per command; while
// it waits the event loop runs this thread's drain fiber (which delivers the replies)
// and the other connections.
private void flushShardPending(ref Conn c, ref ByteBuffer o) nothrow
{
    import dreads.shard : ShardMsg, ShardPending, releaseShardPending, shardEnqueue,
        shardWake, tShard;
    import core.bitop : bsf;

    // Ship every owner's staged batch as ONE ring slot, then wake — once each.
    // The push must come first (a wake with an empty ring is a lost signal for
    // commands still sitting in staging), and the whole batch travels in one
    // cross-core cache-line handoff instead of one per command.
    auto t = c.shardTouch;
    c.shardTouch = 0;
    while (t)
    {
        immutable s = cast(uint) bsf(t);
        if (c.hopBatch[s].length)
        {
            shardEnqueue(s, c.hopBatch[s].data, null, tShard, ShardMsg.cmd);
            c.hopBatch[s].clear();
        }
        shardWake(s);
        t &= t - 1;
    }

    foreach (i; 0 .. c.shardPendCount)
    {
        auto p = cast(ShardPending*) c.shardPends[i];
        while (!p.ready) // same-thread wake: my drain fiber emits done on delivery
        {
            auto ec = p.done.emitCount;
            if (p.ready)
                break;
            try
                p.done.wait(ec);
            catch (Exception)
            {
            }
        }
        o.append(p.reply.data);
        releaseShardPending(p);
    }
    c.shardPendCount = 0;
}

// Ship a same-slot transaction to its key-owner shard as ONE unit (phase
// 2.5d): the drain executes the queued sections back-to-back with no yield —
// ATOMIC on the owner — and fires wakes/notifications once, at the end (a
// blocked client must not wake mid-transaction; its key may expire before the
// transaction finishes — the "reprocessing" contract). Returns false when the
// transaction cannot ship: keys span owners or slots, a keyless/server-layer
// command is queued (only DEBUG SLEEP is allowed through — it sleeps the
// owner's thread exactly like Redis's whole-server stall), a restricted user
// needs the per-slot ACL re-check, or the owner is this very shard.
private bool shardExecAsUnit(ref Conn c, ref ByteBuffer o, ref Arena arena) nothrow
{
    import dreads.acl : aclCmdIndex;
    import dreads.shard : tShard, acquireShardPending, releaseShardPending,
        shardEnqueue, shardWake, ShardMsg;

    if (c.multiCount == 0)
        return false;
    if (gAclActive && c.user !is null && !c.user.root.allKeys)
        return false; // per-command replay re-checks key ACL per slot
    int owner = -1;
    {
        // pass 1: classify every queued command and find the single owner
        size_t qpos = 0;
        foreach (_; 0 .. c.multiCount)
        {
            RVal qcmd;
            if (parseValue(c.multiQueue.data, qpos, arena, qcmd) != ParseStatus.ok)
                return false;
            if (qcmd.type != RType.Array || qcmd.arr.length == 0)
                return false;
            auto name = qcmd.arr[0].str;
            char[16] nbuf = void;
            if (name.length > nbuf.length)
                return false;
            foreach (i, ch; name)
                nbuf[i] = ch >= 'A' && ch <= 'Z' ? cast(char)(ch + 32) : ch;
            auto qlname = cast(const(char)[]) nbuf[0 .. name.length];
            switch (qlname)
            {
            case "debug":
                if (qcmd.arr.length < 2 || !eqICDebug(qcmd.arr[1].str, "SLEEP"))
                    return false;
                continue; // keyless but shippable (see the drain's exec handler)
            case "migrate", "eval", "evalsha", "eval_ro", "evalsha_ro",
                 "fcall", "fcall_ro":
                return false; // keyed but server-layer — cannot run on the drain
            default:
                break;
            }
            immutable int opc = aclCmdIndex(qlname);
            immutable so = shardOwnerOf(qcmd, opc, qlname);
            if (so == SHARD_CROSSSLOT || so < 0)
                return false; // cross-slot, or keyless/server-layer
            if (owner < 0)
                owner = so;
            else if (owner != so)
                return false;
        }
    }
    if (owner < 0 || cast(uint) owner == tShard)
        return false;
    // pass 2: encode the whole transaction as one coalesced batch
    auto p = acquireShardPending();
    c.shardBc.clear();
    {
        size_t qpos = 0;
        foreach (_; 0 .. c.multiCount)
        {
            RVal qcmd;
            immutable size_t qs = qpos;
            cast(void) parseValue(c.multiQueue.data, qpos, arena, qcmd);
            auto name = qcmd.arr[0].str;
            char[16] nbuf = void;
            foreach (i, ch; name)
                nbuf[i] = ch >= 'A' && ch <= 'Z' ? cast(char)(ch + 32) : ch;
            auto qlname = cast(const(char)[]) nbuf[0 .. name.length];
            uint flags = c.resp3 ? HOP_RESP3 : 0;
            immutable int opc = aclCmdIndex(qlname);
            if (qlname != "debug" && opc >= 0 && gCmdBlockingHop[opc]
                    && isBlockingHopForm(opc, qcmd.arr[1 .. $]))
                flags |= HOP_BLOCKING | HOP_NOBLOCK; // one-shot form on the owner
            appendHopCmd(c.shardBc, qcmd, c.multiQueue.data[qs .. qpos],
                    cast(int)(cast(uint) opc | flags), cast(uint) c.dbp.db, cast(void*) p);
        }
    }
    shardEnqueue(cast(uint) owner, c.shardBc.data, null, tShard, ShardMsg.execBatch);
    shardWake(cast(uint) owner);
    // synchronous wait: every section is one-shot (HOP_NOBLOCK), so the batch
    // completes in bounded time (DEBUG SLEEP included — it bounds itself)
    while (!p.ready)
    {
        immutable ec = p.done.emitCount;
        if (p.ready)
            break;
        try
            p.done.wait(ec);
        catch (Exception)
        {
        }
    }
    repArrayHeader(o, c.multiCount);
    o.append(p.reply.data);
    releaseShardPending(p);
    return true;
}

// IR-1: opcode → pure data command, served ENTIRELY by dispatch() — no case in
// executeCommand's (or handleCommand's) server-layer string switch touches it.
// The hot path skips the uppercase conversion and the string switch for these.
// CONSERVATIVE whitelist: a data command missing here merely stays on the slow
// path; NEVER add a command that has a `case` in either switch.
private immutable bool[gCmdCats.length] gPureDispatch = () {
    bool[gCmdCats.length] t;
    foreach (i, c; gCmdCats)
        foreach (name; ["set", "get", "incr", "decr", "incrby", "decrby",
                "incrbyfloat", "append", "strlen", "getset", "getdel", "getex",
                "setex", "psetex", "setnx", "setrange", "getrange", "mget",
                "mset", "msetnx", "del", "unlink", "exists", "type", "touch",
                "expire", "pexpire", "expireat", "pexpireat", "ttl", "pttl",
                "persist", "expiretime", "pexpiretime", "lpush", "rpush",
                "lpushx", "rpushx", "lpop", "rpop", "llen", "lrange", "lindex",
                "lset", "linsert", "lrem", "ltrim", "lpos", "lmove",
                "rpoplpush", "lmpop", "hset", "hget", "hdel", "hlen", "hmget",
                "hmset", "hgetall", "hkeys", "hvals", "hexists", "hincrby",
                "hincrbyfloat", "hsetnx", "hstrlen", "hrandfield", "sadd",
                "srem", "sismember", "smismember", "scard", "smembers", "spop",
                "srandmember", "smove", "sinter", "sunion", "sdiff",
                "sinterstore", "sunionstore", "sdiffstore", "sintercard",
                "zadd", "zrem", "zscore", "zmscore", "zcard", "zcount",
                "zincrby", "zrange", "zrevrange", "zrangebyscore",
                "zrevrangebyscore", "zrangebylex", "zrevrangebylex", "zrank",
                "zrevrank", "zpopmin", "zpopmax", "zmpop", "zrandmember",
                "xadd", "xlen", "xrange", "xrevrange", "xdel", "xack",
                "setbit", "getbit", "bitcount", "bitpos", "pfadd", "pfcount",
                "hscan", "sscan", "zscan", "ping", "echo"])
            if (c.name == name)
                t[i] = true;
    return t;
}();

// CTFE: opcode → is a blocking-hop candidate (phase 2.5b). Pop family true
// unconditionally; XREAD/XREADGROUP marked but need the BLOCK-keyword check
// (isBlockingHopForm); WAIT/WAITAOF are AclCat.blocking but keyless — never
// routed here. Indexed by shardOpcode: the hot path pays ONE array load per
// keyed command instead of a string switch (measured 0.5% in shards=2 profile).
private immutable bool[gCmdCats.length] gCmdBlockingHop = () {
    bool[gCmdCats.length] t;
    foreach (i, c; gCmdCats)
        foreach (name; ["blpop", "brpop", "bzpopmin", "bzpopmax", "blmove",
                "brpoplpush", "blmpop", "bzmpop", "xread", "xreadgroup"])
            if (c.name == name)
                t[i] = true;
    return t;
}();

// The slow half of the gate, reached ONLY for marked opcodes: XREAD/XREADGROUP
// block only when a BLOCK keyword is present (the pop family always does).
// Over-marking is harmless: the owner's serveBlockingSwitch falls back to a
// plain dispatch when the form turns out not to block.
private bool isBlockingHopForm(int opcode, const(RVal)[] args) nothrow
{
    import dreads.aclcat : cmdIx;

    if (opcode != cmdIx!"xread" && opcode != cmdIx!"xreadgroup")
        return true; // pop family: always a blocking form
    foreach (ref a; args)
        if (eqICDebug(a.str, "BLOCK"))
            return true;
    return false;
}

// Fire a BLOCKING command at its key-owner shard and wait SYNCHRONOUSLY for the
// reply (phase 2.5b). The owner parks a fiber (see remoteBlockServe); this conn
// fiber polls at the block tick so a vanished peer or a CLIENT UNBLOCK still
// reaches the remote park (through the pending's cancel flag). The owner ALWAYS
// replies after observing a cancel — never release a slot it may still write.
private void shardFireBlocking(ref Conn c, int owner, int opcode, uint db,
        const ref RVal cmd, scope const(ubyte)[] rawCmd, ref ByteBuffer o,
        bool noBlock = false) nothrow
{
    import core.atomic : atomicLoad, atomicStore;
    import core.time : msecs;

    import dreads.obj : gBlockedClients;
    import dreads.shard : acquireShardPending, releaseShardPending, shardEnqueue,
        shardWake, ShardMsg, tShard;

    // pipeline order: everything in flight replies BEFORE the block's reply,
    // and replies already staged reach the client before we park
    if (c.shardPendCount > 0)
        flushShardPending(c, o);
    flushBeforeBlock(c, o);
    auto p = acquireShardPending();
    c.shardBc.clear();
    appendHopCmd(c.shardBc, cmd, rawCmd,
            cast(int)(cast(uint) opcode | HOP_BLOCKING | (c.resp3 ? HOP_RESP3 : 0)
                | (noBlock ? HOP_NOBLOCK : 0)),
            db, cast(void*) p);
    shardEnqueue(cast(uint) owner, c.shardBc.data, null, tShard, ShardMsg.cmd);
    shardWake(cast(uint) owner);
    import core.atomic : atomicOp;

    atomicOp!"+="(gBlockedClients, 1); // INFO clients: this router's client is parked (remotely)
    c.blocked = true; // eligible for CLIENT UNBLOCK while parked here
    c.remotePend = p; // lets drainClientUnblock cancel the remote park DIRECTLY
    scope (exit)
    {
        atomicOp!"-="(gBlockedClients, 1);
        c.blocked = false;
        c.unblockReq = 0;
        c.remotePend = null;
    }
    while (!p.ready)
    {
        immutable ec = p.done.emitCount;
        if (p.ready)
            break;
        try
            cast(void) p.done.waitUninterruptible(msecs(BLOCK_POLL_MS), ec);
        catch (Exception)
        {
        }
        if (p.ready)
            break;
        if (atomicLoad(p.cancel) == 0)
        {
            ubyte k = 0;
            if (c.unblockReq != 0)
                k = c.unblockReq == 2 ? 3 : 2;
            else if (peerGone(&c))
                k = 1;
            if (k != 0)
            {
                atomicStore(p.cancel, k);
                // kick the owner so a parked XREAD fiber re-checks NOW (the pop
                // family polls at its own block tick and needs no kick)
                static immutable ubyte[1] kickByte = [0];
                shardEnqueue(cast(uint) owner, kickByte[], null, tShard, ShardMsg.blockKick);
                shardWake(cast(uint) owner);
            }
        }
    }
    o.append(p.reply.data);
    releaseShardPending(p);
}

private __gshared shared(TaskPool)[] gShardPools; // keep the worker pools alive

// A shard worker thread's entry (shards 1..N-1; shard 0 is the main thread). Sets this
// thread's shard id, opens its OWN listener on the shared port (SO_REUSEPORT → the
// kernel spreads connections across the N listeners, so EACH thread is a router — no
// central accept, Amdahl distributed), and starts this shard's drain fiber. Returns;
// the TaskPool worker's event loop then serves its connections + drains.
private void shardThreadEntry(uint sid, ushort port) nothrow
{
    import dreads.shard : tShard, pinShardThread;
    import dreads.alloc : gAllocShard;

    tShard = sid;
    gAllocShard = sid; // route this thread's allocations to its own share-nothing slot
    if (gConfig.shardPin)
        pinShardThread(sid);
    {
        // this thread's PRNG stream (TLS): distinct from every other shard's
        import dreads.rand : seedRand;
        import dreads.stream : nowMs;

        seedRand(nowMs() ^ (ulong(sid) * 0x9E37_79B9_7F4A_7C15UL));
    }
    // this thread's wake events (TLS: LocalManualEvent is same-thread-only; the
    // main thread's pair is created at boot — see runServer)
    try
    {
        gKeyActivity = createManualEvent();
        gPauseEvt = createManualEvent();
    }
    catch (Exception)
        assert(false, "shard thread event alloc failed");
    version (Windows)
        enum sopts = TCPListenOptions.reuseAddress;
    else
        enum sopts = TCPListenOptions.reuseAddress | TCPListenOptions.reusePort;
    try
        cast(void) listenTCP(port, delegate(TCPConnection conn) @trusted nothrow {
            serveClient(conn);
        }, sopts);
    catch (Exception)
    {
    }
    if (gConfig.mqttPort != 0)
    {
        import dreads.mqtt : serveMqttClient;

        try
            cast(void) listenTCP(gConfig.mqttPort, delegate(TCPConnection conn) @trusted nothrow {
                serveMqttClient(conn);
            }, sopts);
        catch (Exception)
        {
        }
    }
    if (gConfig.amqpPort != 0)
    {
        import dreads.amqp : serveAmqpClient;

        try
            cast(void) listenTCP(gConfig.amqpPort, delegate(TCPConnection conn) @trusted nothrow {
                serveAmqpClient(conn);
            }, sopts);
        catch (Exception)
        {
        }
    }
    if (gConfig.kafkaPort != 0)
    {
        import dreads.kafka : serveKafkaClient;

        try
            cast(void) listenTCP(gConfig.kafkaPort, delegate(TCPConnection conn) @trusted nothrow {
                serveKafkaClient(conn);
            }, sopts);
        catch (Exception)
        {
        }
    }
    // per-shard maintenance (phase 2.5c): each shard reaps its OWN expired keys
    // and evicts from its OWN partition, on its own event loop
    try
    {
        cast(void) setTimer(200.msecs, () @trusted nothrow { maintExpireTick(); }, true);
        cast(void) setTimer(1.seconds, () @trusted nothrow { maintEvictionTick(); }, true);
        if (gConfig.amqpPort != 0)
            cast(void) setTimer(50.msecs, () @trusted nothrow { amqpTtlTick(); }, true);
        if (gConfig.kafkaPort != 0)
            cast(void) setTimer(50.msecs, () @trusted nothrow {
                import dreads.kafkagroup : kgroupSweep;

                kgroupSweep(); // group barriers/evictions on THIS shard
            }, true);
    }
    catch (Exception)
    {
    }
    runTask(() nothrow { shardDrainLoop(); });
}

// Bring the shard fabric up. Main thread becomes shard 0 (its listener is runServer's
// existing listenTCP); shards 1..N-1 get their own worker thread + listener.
private void startShards(ushort port) nothrow
{
    import dreads.shard : gShardCount, tShard, pinShardThread;
    import vibe.core.taskpool : TaskPool;

    tShard = 0;
    if (gConfig.shardPin)
        pinShardThread(0); // shard 0 = the main thread
    runTask(() nothrow { shardDrainLoop(); }); // shard 0's drain fiber (main thread)
    try
        foreach (i; 1 .. gShardCount)
        {
            auto pool = new shared TaskPool(1, "shard");
            pool.runTaskH(&shardThreadEntry, cast(uint) i, port);
            gShardPools ~= pool;
        }
    catch (Exception)
    {
    }
}

private void serveClient(TCPConnection tcp) nothrow
{
    ByteBuffer inb;
    ByteBuffer outb;
    Arena arena;
    // The connection lives in a refcounted control block; this fiber holds the
    // sole strong ref (`sc`). `c` is a stable pointer into that block, valid for
    // the whole scope. A cross-fiber lock() on the registry's Weak can keep the
    // Conn alive past this fiber's return, so an in-flight delivery never dangles.
    auto sc = Shared!Conn.make();
    Conn* c = &sc.get();
    c.tcp = tcp;
    // Capture the peer + local "ip:port" once, for CLIENT LIST/INFO addr=/laddr=
    // and CLIENT KILL ADDR/LADDR. vibe's toString may throw / GC-allocate; it's a
    // one-time connect cost, copied into the conn's owned buffers.
    try
    {
        auto ra = tcp.remoteAddress.toString();
        c.addr.put(cast(const(char)[]) ra);
        auto la = tcp.localAddress.toString();
        c.laddr.put(cast(const(char)[]) la);
    }
    catch (Exception)
    {
    }
    c.connMs = nowMs(); // for CLIENT LIST age=/idle=
    c.lastActiveMs = c.connMs;
    {
        import core.atomic : atomicFetchAdd;

        c.id = atomicFetchAdd(gClientIds, 1) + 1;
    }
    // sharded: this connection reads/writes THIS thread's shard keyspace (DB-0-only);
    // unsharded: the classic 16-DB gDbs[0]. myKeyspace() reads tShard (thread-local),
    // so a conn served on shard-thread T binds to shard T's data.
    c.dbp = sharded() ? myKeyspace(0) : &gDbs[0]; // default keyspace (db 0)
    // ACL: start as the default user; a nopass default is authenticated at once,
    // a password-protected one (requirepass) must AUTH first. A DISABLED default
    // (`ACL SETUSER default off`) never pre-authenticates — new connections start
    // unauthenticated and get NOAUTH until they AUTH as another user.
    c.user = aclUser("default");
    c.authed = c.user is null || (c.user.enabled && c.user.nopass);
    c.sub.ctx = c; // connSink resolves ctx back to this Conn* (lives in the block)
    c.sub.sink = &connSink;
    c.shardSub.ctx = c;
    c.shardSub.sink = &connSink;
    registerConn(sc);
    scope (exit)
    {
        gPubSub.dropAll(&c.sub); // no further connSink after this
        gShardPubSub.dropAll(&c.shardSub);
        shutdownOutput(*c); // stops the writer fiber; the ring is freed by Conn.~this
        trackDisable(*c); // drop tracking registry membership + gTrackCount
        unregisterMonitor(c);
        unregisterConn(c.id);
        if (c.pauseBlocked) // parked on the pause barrier at disconnect — un-count
        {
            import core.atomic : atomicOp;

            atomicOp!"-="(gBlockedClients, 1);
        }
        waitPurgeConn(c); // drop any lingering block-waiter entries (no dangling c)
        // Close the socket LAST — after shutdownOutput has drained the output
        // queue. Closing earlier would make the writer see a disconnected socket
        // and silently drop the final reply (e.g. QUIT's +OK on a subscriber).
        closeQuiet(tcp);
        // sub/shardSub Dicts, oq ring and clientName are released by Conn.~this,
        // which runs when `sc`'s last strong ref drops (after every registry
        // unlink above, and after any outstanding cross-fiber lock releases) —
        // no manual free() here.
    }
    try
    {
        tcp.tcpNoDelay = true; // small RESP replies must not wait on Nagle
        c.wlock = new TaskMutex;
        bool keep = true;

        // Write the accumulated reply buffer (shares the one ordered path with
        // pub/sub messages so a subscribe confirmation never trails a later message).
        // A tracking client's own-key invalidation is staged in pendingInval and
        // enqueued AFTER the reply, so the `invalidate` push always trails it.
        void flushOut()
        {
            if (outb.empty && c.pendingInval.empty)
                return;
            if (c.subMode)
            {
                if (!outb.empty)
                {
                    auto m = rcFromBytes(outb.data);
                    if (c.oq.push(m))
                        c.oqEvt.emit();
                    rcRelease(m);
                }
                if (!c.pendingInval.empty)
                {
                    auto pm = rcFromBytes(c.pendingInval.data);
                    if (c.oq.push(pm))
                        c.oqEvt.emit();
                    rcRelease(pm);
                    c.pendingInval.clear();
                }
            }
            else
            {
                c.wlock.lock();
                scope (exit)
                    c.wlock.unlock();
                tcp.write(outb.data);
            }
            c.totNetOut += outb.length;
            outb.clear();
        }

        // Drop this connection's pause-park accounting (see gBlockedClients).
        void pauseUnblock()
        {
            if (c.pauseBlocked)
            {
                import core.atomic : atomicOp;

                c.pauseBlocked = false;
                atomicOp!"-="(gBlockedClients, 1);
            }
        }

        // Replay commands barriered during a CLIENT PAUSE by re-injecting them into
        // the NORMAL command pipeline (handleCommand's own gate). We do NOT clear the
        // pause here: each held command is re-evaluated by the gate, so if a pause is
        // still in force — a *stacked* one, or the window simply not expired yet — it
        // is re-buffered for the next round; only genuinely-lifted commands execute.
        // Stacked pauses thus reorganize themselves with no manual end-time juggling.
        // Snapshot-then-clear so the re-buffering (which appends to pausedBuf) can't
        // race the cursor walking it.
        void replayPaused()
        {
            pauseUnblock();
            // The window that barriered these commands has already lifted (this is
            // only ever called when gPauseUntilMs is 0 or elapsed). Clear it HARD
            // before the drain so the held commands run through a clean gate — and
            // set gReplaying so any CLIENT PAUSE that lands during the drain's IO
            // yields is DEFERRED (gPausePending) rather than re-barriering the very
            // commands it must follow. The deferred pause is applied after the drain.
            gPauseUntilMs = 0;
            immutable outerReplay = !gReplaying; // nested replays shouldn't own the guard
            if (outerReplay)
                gReplaying = true;
            // per-connection scratch (handleCommand may yield to other fibers mid-
            // replay, so this must not be shared TLS)
            c.pauseReplayBuf.clear();
            c.pauseReplayBuf.append(c.pausedBuf.data);
            c.pausedBuf.clear();
            auto buf = c.pauseReplayBuf.data;
            size_t p = 0;
            while (keep && p < buf.length)
            {
                RVal cmd;
                immutable start = p;
                if (parseValue(buf, p, arena, cmd) != ParseStatus.ok)
                    break;
                if (cmd.type == RType.Array && cmd.arr.length == 0)
                    continue;
                gRespProto = c.resp3 ? 3 : 2;
                c.totNetIn += p - start; // count request bytes at read (a blocked cmd still shows them)
                immutable replyPre = outb.length;
                c.replyCmdExempt = false;
                gCmdConn = c; // publish-to-self during this command trails its reply
                gImportSourceActive = c.importSource; // gate expired-key visits
                keep = handleCommand(*c, cmd, buf[start .. p], outb, arena);
                gCmdConn = null;
                gImportSourceActive = false;
                postCommand(*c, outb, replyPre);
                if (gNotifyFlags)
                    flushPendingNotify();
                if (gTrackCount)
                    flushTrackingInval(c.id); // grouped invalidations for this command
                arena.reset();
            }
            if (c.shardPendCount > 0)
                flushShardPending(*c, outb);
            if (c.pendingCount > 0)
                flushPending(*c, outb);
            myAof().flush();
            flushOut();
            if (outerReplay)
            {
                gReplaying = false;
                // Apply a CLIENT PAUSE that arrived mid-drain, now that the held
                // commands have all run ahead of it (arrival order preserved).
                if (gPausePending)
                {
                    gPausePending = false;
                    gPauseUntilMs = gPausePendingEnd;
                    gPauseAll = gPausePendingAll;
                    gPauseIssuer = gPausePendingIssuer;
                    cast(void) gPauseEvt.emit();
                }
            }
        }

        while (keep && tcp.connected)
        {
            import core.time : msecs;
            import vibe.core.net : WaitForDataStatus;

            // No separate "park": the socket keeps draining even under a pause, so
            // a flooding client hits the overflow guard in handleCommand (a bounded
            // server-side buffer) instead of piling up in the kernel.
            //
            // Quiet-and-barriered special case: while THIS connection holds commands
            // barriered by an active window and the socket is momentarily idle, wait
            // on the pause event instead of the socket, so CLIENT UNPAUSE (which
            // emits it) wakes us AT ONCE to replay — "unpause replays, then resumes".
            // A short cap bounds the wait so a client that resumes flooding is drained
            // into the server-side buffer (and trips the overflow guard) promptly, and
            // so the window's own timeout still fires. On any wake we loop back: replay
            // if the window has lifted, otherwise drain whatever just arrived.
            if (gPauseUntilMs != 0 && c.pausedBuf.length && !tcp.dataAvailableForRead)
            {
                immutable now = nowMs();
                if (now < gPauseUntilMs)
                {
                    if (!c.pauseBlocked) // count this parked client as blocked (once)
                    {
                        import core.atomic : atomicOp;

                        c.pauseBlocked = true;
                        atomicOp!"+="(gBlockedClients, 1);
                    }
                    immutable rem = gPauseUntilMs - now;
                    immutable cap = rem < PAUSE_POLL_MS ? rem : PAUSE_POLL_MS;
                    immutable ec = gPauseEvt.emitCount;
                    cast(void) gPauseEvt.waitUninterruptible(msecs(cap), ec);
                }
                if (c.pausedBuf.length && (gPauseUntilMs == 0 || nowMs() >= gPauseUntilMs))
                    replayPaused();
                continue;
            }

            immutable ws = tcp.waitForDataEx();
            if (ws == WaitForDataStatus.noMoreData)
                break; // peer disconnected

            // Unbarrier BEFORE handling anything freshly arrived: the held commands
            // arrived earlier, so once the window has lifted (CLIENT UNPAUSE zeroed
            // it) they must replay ahead of this chunk to preserve arrival order.
            if (c.pausedBuf.length && (gPauseUntilMs == 0 || nowMs() >= gPauseUntilMs))
                replayPaused();

            if (ws == WaitForDataStatus.dataAvailable)
            {
                auto space = inb.freeSpace(READ_CHUNK);
                auto n = tcp.read(space, IOMode.once);
                if (n == 0)
                    break;
                inb.grow(n);
                // ONE clock read per chunk, amortized over the whole pipeline
                // batch (a per-command gettimeofday was a real hit): primes the
                // coarse cache freezeClock() reads AND stamps activity (idle=).
                import dreads.det : refreshWall;

                c.lastActiveMs = refreshWall();

                size_t pos = 0;
                while (keep)
                {
                    RVal cmd;
                    size_t cmdStart = pos;
                    auto st = parseValue(inb.data, pos, arena, cmd);
                    if (st == ParseStatus.incomplete)
                    {
                        // Reclaim this parse's speculative allocations. The parser
                        // re-parses the whole buffer from scratch on the next read,
                        // so nothing allocated here is needed. Without this reset a
                        // slowloris feeding an incomplete multibulk (`*1000000\r\n`
                        // + trickle) grows the arena ~MAX_ARRAY RVals (~24MB) PER
                        // read — unbounded, unauthenticated OOM.
                        arena.reset();
                        break;
                    }
                    if (st == ParseStatus.protocolError)
                    {
                        repError(outb, "ERR Protocol error");
                        keep = false;
                        break;
                    }
                    if (cmd.type == RType.Array && cmd.arr.length == 0)
                        continue; // blank inline line — Redis ignores it silently
                    gRespProto = c.resp3 ? 3 : 2; // reply encoding for this command
                    c.totNetIn += pos - cmdStart; // count request bytes at read (a blocked cmd still shows them)
                    immutable replyPre = outb.length;
                    c.replyCmdExempt = false;
                    gCmdConn = c; // publish-to-self during this command trails its reply
                    gImportSourceActive = c.importSource; // gate expired-key visits
                    keep = handleCommand(*c, cmd, inb.data[cmdStart .. pos], outb, arena);
                    gCmdConn = null;
                    gImportSourceActive = false;
                    postCommand(*c, outb, replyPre);
                    if (gNotifyFlags)
                        flushPendingNotify(); // publish keyspace events the command queued
                    if (gTrackCount)
                        flushTrackingInval(c.id); // grouped invalidations for this command
                    arena.reset();
                }
                inb.consume(pos);
                // Reap the chunk's in-flight shard hops + pipelined writes (their
                // replies come last, in order) before flushing the batch to the client.
                if (c.shardPendCount > 0)
                    flushShardPending(*c, outb);
                if (c.pendingCount > 0)
                    flushPending(*c, outb);
                myAof().flush();
                flushOut();
            }
        }
    }
    catch (Exception)
    {
        // peer vanished mid read/write; just drop the connection
    }
    // NOTE: the socket is closed in the scope(exit) above, AFTER shutdownOutput
    // drains the output queue — so a final reply (QUIT's +OK on a subscriber) is
    // written before the close instead of being dropped against a dead socket.
}

// True when `cmd` is a raft write that may be pipelined (fired without blocking
// and reaped later): replication configured, leader, and not inside a
// transaction. Everything else is a "flush point".
private bool isDeferrableWrite(ref Conn c, const ref RVal cmd) nothrow
{
    if (gReplicator is null || c.inMulti || c.inExec)
        return false;
    if (cmd.type != RType.Array || cmd.arr.length == 0)
        return false;
    auto name = cmd.arr[0].str;
    if (name.length == 0 || name.length > 16)
        return false;
    char[16] nbuf = void;
    foreach (i, ch; name)
        nbuf[i] = ch >= 'a' && ch <= 'z' ? cast(char)(ch - 32) : ch;
    return isWriteCommand(cast(string) nbuf[0 .. name.length]) && gReplicator.isLeader;
}

// Flush a connection's accumulated reply buffer to the socket BEFORE its fiber
// parks in a blocking command. The serve loop only flushes `outb` after the whole
// pipeline batch, but a parked block never returns to that flush — so replies to
// commands that PRECEDED the block in the same pipeline (e.g. the `:1` from the
// LPUSH in `LPUSH k v` + `BLPOP k 0`) would sit unsent while the client waits
// forever to read them. Send them now, then keep waiting on an empty buffer.
private void flushBeforeBlock(ref Conn c, ref ByteBuffer o) nothrow
{
    if (c.remoteBlock)
        return; // synthetic conn: `o` is the hop reply staging, nothing to flush
    if (o.empty)
        return;
    try
    {
        if (c.subMode)
        {
            auto m = rcFromBytes(o.data);
            if (c.oq.push(m))
                c.oqEvt.emit();
            rcRelease(m);
        }
        else
        {
            c.wlock.lock();
            scope (exit)
                c.wlock.unlock();
            c.tcp.write(o.data);
        }
        c.totNetOut += o.length;
        o.clear();
    }
    catch (Exception)
    {
    }
}

// Reap every in-flight pipelined write, appending its reply in order (so the
// output stays in command order and a following read observes the writes).
private void flushPending(ref Conn c, ref ByteBuffer o) nothrow
{
    foreach (i; 0 .. c.pendingCount)
    {
        try
        {
            if (!gReplicator.awaitWrite(c.pendingWrites[i], o))
                repError(o, "READONLY You can't write against a read only replica.");
        }
        catch (Exception)
            repError(o, "ERR replication error");
    }
    c.pendingCount = 0;
}

/// Transaction control plus queueing, then the executor. rawCmd holds the
/// command's original RESP bytes for AOF logging and MULTI queueing.
/// Runs on a vibe worker thread (via `async`): try the plaintext against each
/// (isolated, immutable) Argon2/SHA-256 hash. Free function + immutable args so
/// vibe schedules it off the event loop.
private bool authVerifyJob(string pw, immutable(string)[] hashes) nothrow
{
    import dreads.authpw : verifyPassword;

    foreach (h; hashes)
        if (verifyPassword(pw, h))
            return true;
    return false;
}

// Replicate an ACL mutation. Under raft: propose the log form and block until
// commit — the commit re-applies it on every node (leader included, idempotent).
// Standalone: append to the AOF. Returns false (error already written) if a
// follower can't take the write. Rare control-plane command, so a blocking
// round-trip is fine.
private bool propagateAclLog(scope const(ubyte)[] logForm, ref ByteBuffer o) nothrow
{
    import dreads.stream : nowMs;

    if (gReplicator !is null)
    {
        static ByteBuffer discard; // the commit's OK reply is already sent locally
        discard.clear();
        try
        {
            if (!gReplicator.proposeWrite(logForm, nowMs(), 0, discard))
            {
                repError(o, "READONLY You can't write against a read only replica.");
                return false;
            }
        }
        catch (Exception)
        {
            repError(o, "ERR replication error");
            return false;
        }
    }
    else if (myAof().enabled)
        myAof().append(logForm);
    return true;
}

// Channel ACL for the pub/sub commands: which args are channels depends on the
// command (PUBLISH/SPUBLISH → arg 1; SUBSCRIBE/SSUBSCRIBE/PSUBSCRIBE → args 1..).
// PSUBSCRIBE patterns match literally, plain channels glob-match. Returns the
// first denied channel (for the ACL LOG object) or null. Called from the
// top-level enforcement block so it also gates a channel at MULTI queue time.
private const(char)[] aclCmdDeniedChannel(const(AclUser)* u, scope const(char)[] lname,
        scope const(RVal)[] arr) @trusted nothrow @nogc
{
    if (lname == "publish" || lname == "spublish")
        return (arr.length >= 2 && !aclCanAccessChannel(u, arr[1].str)) ? arr[1].str : null;
    if (lname == "subscribe" || lname == "ssubscribe" || lname == "psubscribe")
    {
        immutable isPat = lname[0] == 'p';
        foreach (i; 1 .. arr.length)
            if (!aclCanAccessChannel(u, arr[i].str, isPat))
                return arr[i].str;
    }
    return null;
}

// Record a denied attempt in the ACL LOG with a minimal client-info (the suite
// greps `cmd=<name>`). Context is multi inside a transaction, else toplevel.
private void aclLogViolation(ref Conn c, string reason, scope const(char)[] obj,
        scope const(char)[] cmdName) nothrow
{
    import dreads.stream : nowMs;

    import core.stdc.stdio : snprintf;

    static ByteBuffer ci; // TLS
    ci.clear();
    char[24] idb = void;
    ci.append("id=");
    ci.append(idb[0 .. snprintf(idb.ptr, idb.length, "%llu", c.id)]);
    ci.append(" addr=");
    ci.append(c.addr.length ? cast(const(char)[]) c.addr[] : "?");
    ci.append(" name=");
    if (c.clientName.length)
        ci.append(c.clientName[]);
    // client-info reports the command the CLIENT issued, not the denied object:
    // inside EXEC that is "exec" (the queued command is only the LOG object).
    ci.append(" cmd=");
    ci.append(c.inExec ? "exec" : cmdName);
    // a denial while queuing (inMulti) OR while replaying (inExec) is a "multi"
    // context; only a plain toplevel command is "toplevel".
    aclLogAdd(reason, (c.inMulti || c.inExec) ? "multi" : "toplevel", obj,
            c.user !is null ? c.user.name : "default", cast(const(char)[]) ci.data, nowMs());
}

// Authenticate `c` as user `who` with `pass`. On success sets c.user/c.authed
// and returns true; on failure returns false (the caller emits the error) after
// logging the attempt. Shared by the AUTH command and the HELLO AUTH option.
// Argon2 verify runs on a vibe WORKER THREAD — never inline: the KDF is
// ~15-30 ms and would stall the single event loop (a DoS vector under a flood).
private bool authenticateConn(ref Conn c, scope const(char)[] who, scope const(char)[] pass) @trusted nothrow
{
    import dreads.stream : nowMs;

    auto u = aclUser(who);
    bool ok = false;
    if (u !is null && u.enabled)
    {
        if (u.nopass)
            ok = true;
        else
        {
            try
            {
                import vibe.core.concurrency : async;
                import dreads.acl : aclPasswordHashes;

                auto pw = pass.idup; // isolate for the worker thread
                auto hashes = aclPasswordHashes(u);
                ok = async(&authVerifyJob, pw, hashes).getResult();
            }
            catch (Exception)
                ok = false;
        }
    }
    // re-validate after yielding: the user may have been deleted/disabled
    u = aclUser(who);
    if (!ok || u is null || !u.enabled)
    {
        import core.stdc.stdio : snprintf;

        static ByteBuffer ai; // TLS
        ai.clear();
        char[24] idb = void;
        ai.append("id=");
        ai.append(idb[0 .. snprintf(idb.ptr, idb.length, "%llu", c.id)]);
        ai.append(" addr=");
        ai.append(c.addr.length ? cast(const(char)[]) c.addr[] : "?");
        ai.append(" cmd=auth");
        aclLogAdd("auth", c.inMulti ? "multi" : "toplevel", "AUTH", who,
                cast(const(char)[]) ai.data, nowMs());
        return false;
    }
    c.user = u;
    c.authed = true;
    return true;
}

// ACL enforcement — a single `command ∈ cap_set` test (plus key/channel/db),
// run only while ACL is in use. Returns true if the command is DENIED (having
// emitted the error reply and recorded the ACL LOG violation). Assumes the
// caller already checked `gAclActive && c.user !is null`. Runs at toplevel/queue
// time AND again on EXEC replay — a user's permissions may have been revoked
// after a command was queued, and Valkey re-checks at execution time.
private bool aclDenies(ref Conn c, const ref RVal cmd, string uname,
        scope const(char)[] name, ref ByteBuffer o) nothrow
{
    // An all-permissions, already-authed user (default/admin) can't be denied
    // anything — skip the per-command lookup entirely.
    if (c.authed && aclUnrestricted(c.user))
        return false;
    char[32] lb = void;
    if (name.length > lb.length)
        return false; // >32-char names aren't ACL-catalogued; dispatch handles them
    foreach (i, ch; name)
        lb[i] = (ch >= 'A' && ch <= 'Z') ? cast(char)(ch + 32) : ch;
    auto lname = cast(const(char)[]) lb[0 .. name.length];
    // Permission bit first: an authed user holding this command passes on a
    // single bitset test and short-circuits everything below — the hot path for
    // any real ACL user. `expect(..., false)`: in real traffic a client almost
    // never issues a command it lacks, so the deny path is cold.
    immutable cidx = aclCmdIndex(lname);
    // per-subcommand ACL: lowercase arg[1] (e.g. CLIENT KILL → "kill")
    char[32] sb = void;
    const(char)[] sub;
    if (cmd.arr.length >= 2 && cmd.arr[1].str.length <= sb.length)
    {
        foreach (i, ch; cmd.arr[1].str)
            sb[i] = (ch >= 'A' && ch <= 'Z') ? cast(char)(ch + 32) : ch;
        sub = cast(const(char)[]) sb[0 .. cmd.arr[1].str.length];
    }
    if (expect(!(c.authed && aclCanRunCmdSub(c.user, cidx, sub)), false))
    {
        bool alwaysOk = uname == "AUTH" || uname == "HELLO"
            || uname == "RESET" || uname == "QUIT";
        if (!alwaysOk)
        {
            if (!c.authed)
            {
                statRejected(cidx);
                repError(o, "NOAUTH Authentication required.");
                return true;
            }
            // object = command, or command|sub when under sub-ACL
            static ByteBuffer ob; // TLS
            ob.clear();
            ob.append(lname);
            if (sub.length && (aclIsContainer(lname) || aclCmdHasSubRule(c.user, cidx)))
            {
                ob.append("|");
                ob.append(sub);
            }
            auto obj = cast(const(char)[]) ob.data;
            statRejected(cidx);
            aclLogViolation(c, "command", obj, lname);
            static ByteBuffer eb; // TLS
            eb.clear();
            eb.append("NOPERM User ");
            eb.append(c.user.name);
            eb.append(" has no permissions to run the '");
            eb.append(obj);
            eb.append("' command");
            repError(o, cast(const(char)[]) eb.data);
            return true;
        }
    }
    // key-pattern ACL: the command is allowed — now reject it if it touches a
    // key outside the user's ~patterns (allkeys users skip).
    auto dk = aclDeniedKey(c.user, lname, cmd.arr);
    if (dk !is null)
    {
        statRejected(cidx);
        aclLogViolation(c, "key", dk, lname);
        repError(o, "NOPERM No permissions to access a key");
        return true;
    }
    // channel-pattern ACL for pub/sub — checked before the switch AND before
    // MULTI queuing, so an unauthorized channel is rejected at queue time.
    if (!c.user.root.allChannels)
    {
        auto dch = aclCmdDeniedChannel(c.user, lname, cmd.arr);
        if (dch !is null)
        {
            statRejected(cidx);
            aclLogViolation(c, "channel", dch, lname);
            repError(o, "NOPERM No permissions to access a channel");
            return true;
        }
    }
    // database ACL (`db=`): restrict which DBs the user may touch.
    if (!c.user.root.allDbs)
    {
        auto ddb = aclDeniedDb(c.user, lname, cmd.arr, c.dbp.db);
        if (ddb !is null)
        {
            statRejected(cidx);
            aclLogViolation(c, "database", ddb, lname);
            repError(o, "NOPERM No permissions to access database");
            return true;
        }
    }
    return false;
}

// age-seconds as a float string ("0.001"), from a millisecond delta.
private void appendAge(ref ByteBuffer o, long ms) @nogc nothrow
{
    import core.stdc.stdio : snprintf;

    if (ms < 0)
        ms = 0;
    char[32] b = void;
    auto n = snprintf(b.ptr, b.length, "%.3f", cast(double) ms / 1000.0);
    if (n > 0)
        o.append(b[0 .. n]);
}

// Per-command CLIENT bookkeeping, run by the serve loop AFTER handleCommand
// returns (a command blocked in BLPOP completes only when handleCommand returns,
// so tot-cmds ticks at completion for free — no scope(exit) needed). tot-net-in is
// NOT here: input bytes are counted at read time by the serve loop, so a command
// blocked in BLPOP still shows its request bytes while parked (Redis does the same).
// Kept out of handleCommand: a try/finally there pessimizes the hot path.
// `replyPre` is outb.length captured before the command.
// NOTE: do NOT `pragma(inline, true)` this — force-inlining its @safe body into
// serveClient flips serveClient to inferred-@safe, which makes dmd @safe-check
// vibe's waitForDataEx and trips a latent @safe/@system bug in vibe-core 2.14.0.
// LDC inlines it on its own at -O, so the pragma bought nothing anyway.
private void postCommand(ref Conn c, ref ByteBuffer o, size_t replyPre) @nogc nothrow
{
    c.totCmds++;
    // CLIENT REPLY OFF/SKIP roll the reply back; the CLIENT REPLY command itself
    // latched replyCmdExempt so it is never suppressed. SKIP is one-shot.
    if (!c.replyCmdExempt)
    {
        if (c.replySkipNext)
        {
            o.truncate(replyPre);
            c.replySkipNext = false;
        }
        else if (c.replyOff)
            o.truncate(replyPre);
    }
}

private bool handleCommand(ref Conn c, const ref RVal cmd, scope const(ubyte)[] rawCmd,
        ref ByteBuffer o, ref Arena arena) nothrow
{
    // NOTE: per-command CLIENT bookkeeping (tot-net-in/tot-cmds, CLIENT REPLY
    // suppression, idle= timestamp) is done by the serve loop around this call, NOT
    // here — a scope(exit) in this hot nothrow function measurably hurt throughput
    // (LDC emits try/finally cleanup that pessimizes the common path).
    // CLIENT TRACKING: the redirect target died while an invalidation was pending.
    // Tell this (RESP3) client once, ahead of its next reply, then drop the dead
    // redirect. It can re-arm with a fresh REDIRECT afterwards.
    if (c.trackRedirBroken && c.resp3)
    {
        o.append(">2\r\n");
        repBulk(o, "tracking-redir-broken");
        repInt(o, cast(long) c.trackRedir);
        c.trackRedirBroken = false;
        c.trackRedir = 0;
    }
    // Pipelining flush point: anything that is not itself a pipelinable write
    // must first reap all in-flight writes, in order.
    if (c.pendingCount > 0 && !isDeferrableWrite(c, cmd))
        flushPending(c, o);
    if (cmd.type != RType.Array || cmd.arr.length == 0)
        return dispatch(cmd, *c.dbp, o, arena);
    foreach (ref a; cmd.arr)
    {
        if (a.type != RType.BulkString && a.type != RType.SimpleString)
            return dispatch(cmd, *c.dbp, o, arena);
    }
    auto name = cmd.arr[0].str;
    // 32 covers every command token (longest is GEORADIUSBYMEMBER_RO, 20) — must
    // NOT be shorter than the longest name, or that command would take the raw
    // `return dispatch` path below and SKIP ACL enforcement (a silent bypass).
    char[32] nbuf = void;
    if (name.length > nbuf.length)
        return dispatch(cmd, *c.dbp, o, arena);
    foreach (i, ch; name)
        nbuf[i] = ch >= 'a' && ch <= 'z' ? cast(char)(ch - 32) : ch;
    auto uname = cast(string) nbuf[0 .. name.length];

    // record the last command name (lowercase) for CLIENT LIST's cmd= field, and
    // stash the raw arg[1] token so a container command can render as
    // `container|subcommand` — the container test is deferred to appendConnInfo.
    if (name.length <= c.lastCmdBuf.length)
    {
        foreach (i, ch; name)
            c.lastCmdBuf[i] = ch >= 'A' && ch <= 'Z' ? cast(char)(ch + 32) : ch;
        c.lastCmdLen = cast(ubyte) name.length;
        if (cmd.arr.length >= 2)
        {
            auto sub = cmd.arr[1].str;
            immutable sn = sub.length <= c.lastArgBuf.length ? sub.length : c.lastArgBuf.length;
            foreach (i; 0 .. sn)
            {
                immutable ch = sub[i];
                c.lastArgBuf[i] = ch >= 'A' && ch <= 'Z' ? cast(char)(ch + 32) : ch;
            }
            c.lastArgLen = cast(ubyte) sn;
        }
        else
            c.lastArgLen = 0;
    }
    // IR-1: the bookkeeping above already lowercased the name — resolve the
    // opcode ONCE from it; the pause/ACL gates and the transaction switch below
    // are all rare (or gated) for a pure data command, which runs straight
    // through to executeCommand with the opcode in hand.
    auto lname = cast(const(char)[]) c.lastCmdBuf[0 .. c.lastCmdLen];
    immutable int opcode = aclCmdIndex(lname);

    // CLIENT PAUSE barrier — before ACL/dispatch/AOF. A matching command (ALL, or a
    // write in WRITE mode) is buffered raw and replayed when the window lifts; CLIENT
    // is exempt so UNPAUSE always lands. Never executed nor logged while barriered.
    // MULTI queuing is NOT barriered (commands still return QUEUED under a pause);
    // the transaction is instead held as a unit at EXEC iff it queued a write.
    // A pause is rare — keep it off the branch predictor's hot path (expect false).
    if (expect(gPauseUntilMs != 0, false) && opcode != cmdIx!"client" && c.id != gPauseIssuer)
    {
        if (nowMs() >= gPauseUntilMs)
            gPauseUntilMs = 0; // window elapsed — run normally
        else
        {
            bool barrier;
            if (opcode == cmdIx!"exec")
                barrier = gPauseAll || c.multiHasWrite; // hold the whole txn
            else if (c.inMulti)
                barrier = false; // let MULTI/queued commands reach the queue
            else
                barrier = gPauseAll || heldByWritePause(uname, cmd);
            if (barrier)
            {
                c.pausedBuf.append(rawCmd);
                if (c.pausedBuf.length > gConfig.clientQueryBufferLimit)
                    return false; // guard: a client flooding the barrier is disconnected
                return true; // barriered
            }
        }
    }

    // ACL enforcement — a single `command ∈ cap_set` test, only while ACL is in
    // use (gAclActive false in the default no-ACL deployment => zero cost). The
    // always-allowed connection commands run regardless so a client can (re)auth.
    // (Key/channel-pattern checks are a follow-up; command-level only for now.)
    if (gAclActive && c.user !is null)
    {
        // Ride the caller's identity into the script bridge: a redis.call inside
        // EVAL/FCALL re-checks THIS user's permissions on the writer thread, so
        // `+eval -set` can't smuggle a SET through a script. Invisible to Lua.
        scriptSetPendingUser(c.user.id);
        if (aclDenies(c, cmd, uname, name, o))
            return true;
    }

    // IR-1 fast path: a pure data command has no case below.
    if (opcode < 0 || !gPureDispatch[opcode])
    switch (uname)
    {
    case "SELECT":
        {
            // per-connection database switch — pure connection state, like HELLO
            long n = -1;
            if (cmd.arr.length == 2)
                parseLong(cmd.arr[1].str, n);
            if (cmd.arr.length != 2)
                repError(o, "ERR wrong number of arguments for 'select' command");
            else if (n < 0 || n >= RESP_DBS) // skin dbs (16-18) are not selectable
                repError(o, "ERR DB index is out of range");
            else if (sharded())
            {
                // per-shard 16 DBs: SELECT picks the db WITHIN this shard's own
                // partition — never touches another thread's keyspace.
                c.dbp = myKeyspace(cast(uint) n);
                repSimple(o, "OK");
            }
            else
            {
                c.dbp = &gDbs[cast(size_t) n];
                repSimple(o, "OK");
            }
            return true;
        }
    case "MULTI":
        if (c.inMulti)
        {
            // a command rejected at queue time dirties the transaction: EXEC aborts
            c.multiDirty = true;
            repError(o, "ERR Command 'multi' not allowed inside a transaction");
        }
        else
        {
            c.inMulti = true;
            c.multiCount = 0;
            c.multiHasWrite = false;
            c.multiDirty = false;
            c.multiQueue.clear();
            repSimple(o, "OK");
        }
        return true;
    case "DISCARD":
        if (!c.inMulti)
            repError(o, "ERR DISCARD without MULTI");
        else
        {
            c.inMulti = false;
            c.multiHasWrite = false;
            c.multiDirty = false;
            c.multiQueue.clear();
            c.watching = false;
            repSimple(o, "OK");
        }
        return true;
    case "WATCH":
        if (c.inMulti)
        {
            c.multiDirty = true; // queue-time rejection dirties the transaction
            repError(o, "ERR Command 'watch' not allowed inside a transaction");
        }
        else
        {
            // conservative: EXEC aborts if ANY write happened since WATCH
            // (stricter than Redis's per-key tracking — see DRIFT.md)
            if (!c.watching)
            {
                c.watching = true;
                c.watchEpoch = gWriteEpoch;
            }
            repSimple(o, "OK");
        }
        return true;
    case "UNWATCH":
        c.watching = false;
        repSimple(o, "OK");
        return true;
    case "EXEC":
        {
            if (!c.inMulti)
            {
                repError(o, "ERR EXEC without MULTI");
                return true;
            }
            // a command errored while being queued -> abort the whole transaction
            if (c.multiDirty)
            {
                c.inMulti = false;
                c.multiHasWrite = false;
                c.multiDirty = false;
                c.multiQueue.clear();
                c.watching = false;
                repError(o, "EXECABORT Transaction discarded because of previous errors.");
                return true;
            }
            c.inMulti = false;
            c.multiHasWrite = false;
            scope (exit)
            {
                c.multiQueue.clear();
                c.watching = false;
            }
            if (c.watching && c.watchEpoch != gWriteEpoch)
            {
                repNullArray(o); // aborted EXEC: RESP3 null
                return true;
            }
            // Same-slot transaction whose keys live on a REMOTE owner: ship it
            // as ONE atomic unit (phase 2.5d). Falls through to the per-command
            // replay when it cannot (mixed owners, server-layer commands,
            // restricted ACL) — the documented non-atomic v1 behavior.
            if (sharded() && shardExecAsUnit(c, o, arena))
                return true;
            repArrayHeader(o, c.multiCount);
            size_t qpos = 0;
            bool keep = true;
            c.inExec = true; // queued writes stay synchronous inside EXEC
            scope (exit)
                c.inExec = false;
            foreach (_; 0 .. c.multiCount)
            {
                RVal qcmd;
                size_t qstart = qpos;
                if (parseValue(c.multiQueue.data, qpos, arena, qcmd) != ParseStatus.ok)
                    break; // impossible: queued bytes were already parsed once
                // re-check ACL at execution time: the user's permissions may have
                // been revoked after this command was queued (c.inExec makes the
                // LOG client-info report cmd=exec). A denial becomes this slot's
                // reply in the EXEC array; skip running the command.
                if (gAclActive && c.user !is null && qcmd.type == RType.Array
                        && qcmd.arr.length > 0)
                    if (aclDenies(c, qcmd, null, qcmd.arr[0].str, o))
                        continue;
                keep = executeCommand(c, qcmd, c.multiQueue.data[qstart .. qpos], o, arena) && keep;
            }
            return keep;
        }
    case "ACL":
        {
            if (cmd.arr.length < 2)
            {
                repError(o, "ERR wrong number of arguments for 'acl' command");
                return true;
            }
            auto sub = cmd.arr[1].str;
            char[12] sbuf = void;
            const(char)[] su = sub;
            if (sub.length <= sbuf.length)
            {
                foreach (i, ch; sub)
                    sbuf[i] = (ch >= 'a' && ch <= 'z') ? cast(char)(ch - 32) : ch;
                su = sbuf[0 .. sub.length];
            }
            switch (su)
            {
            case "WHOAMI":
                repBulk(o, c.user !is null ? c.user.name : "default");
                return true;
            case "DRYRUN":
                {
                    // ACL DRYRUN <user> <command> [args...] — run the same
                    // command/key/channel check without executing, reply +OK or
                    // the verbose reason (a bulk string, not an error).
                    if (cmd.arr.length < 4)
                    {
                        repError(o,
                            "ERR wrong number of arguments for 'acl|dryrun' command");
                        return true;
                    }
                    auto du = aclUser(cmd.arr[2].str);
                    static ByteBuffer de; // TLS
                    if (du is null)
                    {
                        de.clear();
                        de.append("ERR User '");
                        de.append(cmd.arr[2].str);
                        de.append("' not found");
                        repError(o, cast(const(char)[]) de.data);
                        return true;
                    }
                    auto targ = cmd.arr[3 .. $]; // target command + its args
                    auto tname = targ[0].str;
                    char[32] tlb = void;
                    if (tname.length > tlb.length)
                    {
                        repError(o, "ERR Command not found");
                        return true;
                    }
                    foreach (i, ch; tname)
                        tlb[i] = (ch >= 'A' && ch <= 'Z') ? cast(char)(ch + 32) : ch;
                    auto tlname = cast(const(char)[]) tlb[0 .. tname.length];
                    immutable tci = aclCmdIndex(tlname);
                    if (tci < 0)
                    {
                        de.clear();
                        de.append("ERR Command '");
                        de.append(tname);
                        de.append("' not found");
                        repError(o, cast(const(char)[]) de.data);
                        return true;
                    }
                    // subcommand / first-arg
                    char[32] dsb = void;
                    const(char)[] dsub;
                    if (targ.length >= 2 && targ[1].str.length <= dsb.length)
                    {
                        foreach (i, ch; targ[1].str)
                            dsb[i] = (ch >= 'A' && ch <= 'Z') ? cast(char)(ch + 32) : ch;
                        dsub = cast(const(char)[]) dsb[0 .. targ[1].str.length];
                    }
                    de.clear();
                    if (!aclCanRunCmdSub(du, tci, dsub))
                    {
                        de.append("User ");
                        de.append(du.name);
                        de.append(" has no permissions to run the '");
                        de.append(tlname);
                        if (dsub.length && (aclIsContainer(tlname) || aclCmdHasSubRule(du, tci)))
                        {
                            de.append("|");
                            de.append(dsub);
                        }
                        de.append("' command");
                        repBulk(o, cast(const(char)[]) de.data);
                        return true;
                    }
                    if (!du.root.allKeys && aclKeyDenied(du, tlname, targ))
                    {
                        de.append("User ");
                        de.append(du.name);
                        de.append(" has no permissions to access one of the keys used as arguments");
                        repBulk(o, cast(const(char)[]) de.data);
                        return true;
                    }
                    repSimple(o, "OK");
                    return true;
                }
            case "GETUSER":
                {
                    if (cmd.arr.length != 3)
                    {
                        repError(o, "ERR wrong number of arguments for 'acl|getuser' command");
                        return true;
                    }
                    auto u = aclUser(cmd.arr[2].str);
                    if (u is null)
                    {
                        repNullBulk(o); // Valkey addReplyNull for an unknown user
                        return true;
                    }
                    static ByteBuffer db; // TLS scratch for describe strings
                    repMapHeader(o, 6);
                    repBulk(o, "flags");
                    repSetHeader(o, 1 + (u.nopass ? 1 : 0));
                    repBulk(o, u.enabled ? "on" : "off");
                    if (u.nopass)
                        repBulk(o, "nopass");
                    repBulk(o, "passwords");
                    repArrayHeader(o, u.passwords.length);
                    foreach (i; 0 .. u.passwords.length)
                        repBulk(o, u.passwords[i]);
                    repBulk(o, "commands");
                    db.clear();
                    aclDescribeCommands(u, db);
                    repBulk(o, cast(const(char)[]) db.data);
                    repBulk(o, "keys");
                    db.clear();
                    aclDescribeKeys(u, db);
                    repBulk(o, cast(const(char)[]) db.data);
                    repBulk(o, "channels");
                    db.clear();
                    aclDescribeChannels(u, db);
                    repBulk(o, cast(const(char)[]) db.data);
                    repBulk(o, "selectors");
                    repArrayHeader(o, 0); // ACL v2 selectors: Phase 2
                    return true;
                }
            case "SETUSER":
                {
                    if (cmd.arr.length < 3)
                    {
                        repError(o, "ERR wrong number of arguments for 'acl|setuser' command");
                        return true;
                    }
                    foreach (ch; cmd.arr[2].str)
                        if (ch == ' ' || ch == '\0')
                        {
                            repError(o, "ERR Usernames can't contain spaces or null characters");
                            return true;
                        }
                    // ACL mutations replicate; a follower can't accept them
                    if (gReplicator !is null && !gReplicator.isLeader)
                    {
                        repError(o, "READONLY You can't write against a read only replica.");
                        return true;
                    }
                    auto u = aclGetOrCreate(cmd.arr[2].str);
                    const(char)[] err;
                    try
                    {
                        foreach (ref r; cmd.arr[3 .. $])
                            if (!aclApplyRule(u, r.str, err))
                            {
                                repError(o, err); // "ERR Error in ACL SETUSER modifier…"
                                return true;
                            }
                    }
                    catch (Exception)
                    {
                        repError(o, "ERR ACL SETUSER failed to hash a password");
                        return true;
                    }
                    gAclActive = true; // enforcement turns on once ACL is used
                    // a channel permission change may revoke a live subscriber's
                    // active subscriptions — disconnect those (pardon the rest)
                    aclKillRevokedSubscribers(u);
                    // replicate the canonical, fully-hashed form (deterministic
                    // replay — followers never re-run the Argon2 KDF)
                    static ByteBuffer canon;
                    canon.clear();
                    aclEncodeCanonicalSetuser(u, canon);
                    if (!propagateAclLog(canon.data, o))
                        return true;
                    repSimple(o, "OK");
                    return true;
                }
            case "DELUSER":
                {
                    if (cmd.arr.length < 3)
                    {
                        repError(o, "ERR wrong number of arguments for 'acl|deluser' command");
                        return true;
                    }
                    if (gReplicator !is null && !gReplicator.isLeader)
                    {
                        repError(o, "READONLY You can't write against a read only replica.");
                        return true;
                    }
                    // the default user can never be removed
                    foreach (ref a; cmd.arr[2 .. $])
                        if (a.str == "default")
                        {
                            repError(o, "ERR The 'default' user cannot be removed");
                            return true;
                        }
                    // a client authed as a to-be-deleted user is disconnected after
                    // this reply (Valkey behaviour). Decide BEFORE deleting — the
                    // AclUser (and c.user.name) is freed by aclDelUser.
                    bool selfDeleted = false;
                    if (c.authed && c.user !is null)
                        foreach (ref a; cmd.arr[2 .. $])
                            if (a.str == c.user.name)
                            {
                                selfDeleted = true;
                                break;
                            }
                    // disconnect OTHER sessions authed as a deleted user (the
                    // self connection is handled after the reply via selfDeleted).
                    // Done BEFORE aclDelUser frees the AclUser (we compare c.user).
                    {
                        Vector!ulong ids;
                        snapshotConnIds(ids);
                        foreach (ref a; cmd.arr[2 .. $])
                            foreach (id; ids[])
                            {
                                auto s = connById(id);
                                if (s.isNull)
                                    continue;
                                auto p = &s.get();
                                if (p !is &c && p.user !is null && p.user.name == a.str)
                                    killConn(p);
                            }
                    }
                    long n = 0;
                    foreach (ref a; cmd.arr[2 .. $])
                        if (aclDelUser(a.str))
                            n++;
                    // DELUSER is already canonical + idempotent — log it verbatim
                    if (!propagateAclLog(rawCmd, o))
                        return true;
                    repInt(o, n);
                    if (selfDeleted)
                    {
                        c.user = null; // avoid dangling deref before the socket closes
                        c.authed = false;
                        return false; // close the connection after the reply flushes
                    }
                    return true;
                }
            case "USERS":
                {
                    size_t n = 0;
                    aclEachUser((AclUser* u) @nogc nothrow { n++; return 0; });
                    repArrayHeader(o, n);
                    aclEachUser((AclUser* u) @nogc nothrow {
                        repBulk(o, u.name);
                        return 0;
                    });
                    return true;
                }
            case "LIST":
                {
                    // config-file format per user: "user <name> <flags> <keys>
                    // <channels> <commands>" (matches Valkey ACLDescribeUser order)
                    size_t n = 0;
                    aclEachUser((AclUser* u) @nogc nothrow { n++; return 0; });
                    repArrayHeader(o, n);
                    static ByteBuffer lb; // TLS
                    aclEachUser((AclUser* u) @nogc nothrow {
                        lb.clear();
                        lb.append("user ");
                        lb.append(u.name);
                        lb.append(u.enabled ? " on" : " off");
                        if (u.nopass)
                            lb.append(" nopass");
                        foreach (i; 0 .. u.passwords.length)
                        {
                            lb.append(" #");
                            lb.append(u.passwords[i]);
                        }
                        // keys section is omitted entirely when the user has no
                        // key access (no `~*`, no patterns) — Valkey emits nothing
                        // there, so a blank one would leave a stray double space.
                        if (u.root.allKeys || u.root.keyPats.length)
                        {
                            lb.append(" ");
                            aclDescribeKeys(u, lb);
                        }
                        lb.append(" ");
                        aclDescribeChannels(u, lb, true); // LIST form: resetchannels prefix
                        lb.append(" ");
                        aclDescribeCommands(u, lb);
                        repBulk(o, cast(const(char)[]) lb.data);
                        return 0;
                    });
                    return true;
                }
            case "CAT":
                {
                    if (cmd.arr.length == 2) // no arg: list the categories
                    {
                        repArrayHeader(o, aclCatNames.length);
                        foreach (nm; aclCatNames)
                            repBulk(o, nm);
                        return true;
                    }
                    if (cmd.arr.length == 3) // one arg: list the category's members
                    {
                        import dreads.aclcat : aclCatBit, gCmdCats;
                        import dreads.aclsub : gSubCmds;

                        auto cn = cmd.arr[2].str;
                        char[32] clb = void;
                        uint bit = 0;
                        if (cn.length <= clb.length)
                        {
                            foreach (i, ch; cn)
                                clb[i] = (ch >= 'A' && ch <= 'Z') ? cast(char)(ch + 32) : ch;
                            bit = aclCatBit(clb[0 .. cn.length]);
                        }
                        if (bit == 0)
                        {
                            static ByteBuffer ce; // TLS
                            ce.clear();
                            ce.append("ERR Unknown category '");
                            ce.append(cn.length > 128 ? cn[0 .. 128] : cn);
                            ce.append("'");
                            repError(o, cast(const(char)[]) ce.data);
                            return true;
                        }
                        size_t n = 0;
                        foreach (ref cc; gCmdCats)
                            if (cc.cats & bit)
                                n++;
                        foreach (ref sc; gSubCmds)
                            if (sc.cats & bit)
                                n++;
                        repArrayHeader(o, n);
                        foreach (ref cc; gCmdCats)
                            if (cc.cats & bit)
                                repBulk(o, cc.name);
                        static ByteBuffer sb; // TLS: assemble "container|sub"
                        foreach (ref sc; gSubCmds)
                            if (sc.cats & bit)
                            {
                                sb.clear();
                                sb.append(sc.container);
                                sb.append("|");
                                sb.append(sc.sub);
                                repBulk(o, cast(const(char)[]) sb.data);
                            }
                        return true;
                    }
                    repUnknownSubcommand(o, "ACL", "CAT"); // too many args
                    return true;
                }
            case "GENPASS":
                {
                    import dreads.rand : nextRand;

                    long bits = 256;
                    if (cmd.arr.length >= 3 && (!parseLong(cmd.arr[2].str, bits)
                            || bits <= 0 || bits > 4096))
                    {
                        repError(o, "ERR ACL GENPASS argument must be the number"
                                ~ " of bits for the output password, a positive number up to 4096");
                        return true;
                    }
                    auto nchars = cast(size_t)((bits + 3) / 4);
                    static immutable hexd = "0123456789abcdef";
                    char[1024] hb = void;
                    foreach (i; 0 .. nchars)
                        hb[i] = hexd[nextRand() & 0xf];
                    repBulk(o, hb[0 .. nchars]);
                    return true;
                }
            case "LOG":
                {
                    if (cmd.arr.length == 3 && eqICDebug(cmd.arr[2].str, "RESET"))
                    {
                        aclLogReset();
                        repSimple(o, "OK");
                        return true;
                    }
                    if (cmd.arr.length > 3)
                    {
                        repError(o, "ERR unknown subcommand or wrong number of"
                                ~ " arguments for 'LOG'. Try ACL HELP.");
                        return true;
                    }
                    long lim = 10; // Valkey's default entry count
                    if (cmd.arr.length == 3 && !parseLong(cmd.arr[2].str, lim))
                    {
                        repError(o, "ERR Got a non-integer or invalid count argument for 'ACL LOG'");
                        return true;
                    }
                    immutable total = aclLogCount();
                    size_t n = (lim < 0 || lim > total) ? total : cast(size_t) lim;
                    import dreads.stream : nowMs;

                    auto now = nowMs();
                    repArrayHeader(o, n);
                    static ByteBuffer ab; // TLS for age-seconds
                    foreach (i; 0 .. n)
                    {
                        auto e = aclLogAt(i); // 0 = newest
                        repMapHeader(o, 10);
                        repBulk(o, "count");
                        repInt(o, cast(long) e.count);
                        repBulk(o, "reason");
                        repBulk(o, e.reason);
                        repBulk(o, "context");
                        repBulk(o, e.ctx);
                        repBulk(o, "object");
                        repBulk(o, e.obj);
                        repBulk(o, "username");
                        repBulk(o, e.user);
                        repBulk(o, "age-seconds");
                        ab.clear();
                        appendAge(ab, now - e.created);
                        repBulk(o, cast(const(char)[]) ab.data);
                        repBulk(o, "client-info");
                        repBulk(o, e.cinfo);
                        repBulk(o, "entry-id");
                        repInt(o, cast(long) e.id);
                        repBulk(o, "timestamp-created");
                        repInt(o, e.created);
                        repBulk(o, "timestamp-last-updated");
                        repInt(o, e.updated);
                    }
                    return true;
                }
            case "HELP":
                if (cmd.arr.length > 2)
                {
                    repError(o, "ERR unknown subcommand or wrong number of"
                            ~ " arguments for 'acl|help' command");
                    return true;
                }
                repHelp!"ACL"(o);
                return true;
            case "LOAD":
            case "SAVE":
                // dreads has no ACL file — users persist through the AOF/raft log
                // (ACL SETUSER/DELUSER are logged), not an aclfile. Match Valkey's
                // "no aclfile configured" error.
                repError(o, "ERR This instance is not configured to use an ACL"
                        ~ " file. You may want to specify users via the ACL"
                        ~ " SETUSER command and then issue a CONFIG REWRITE"
                        ~ " (assuming you have a configuration file set) in order"
                        ~ " to store users in the configuration.");
                return true;
            default:
                repUnknownSubcommand(o, "ACL", sub);
                return true;
            }
        }
    case "AUTH":
        {
            const(char)[] who, pass;
            if (cmd.arr.length == 2)
            {
                // AUTH <pass> — the default user; if it has no password set,
                // Redis returns the classic hint rather than WRONGPASS
                auto def = aclUser("default");
                if (def !is null && def.nopass)
                {
                    repError(o, "ERR Client sent AUTH, but no password is set."
                            ~ " Did you mean AUTH <username> <password>?");
                    return true;
                }
                who = "default";
                pass = cmd.arr[1].str;
            }
            else if (cmd.arr.length == 3)
            {
                who = cmd.arr[1].str;
                pass = cmd.arr[2].str;
            }
            else
            {
                repError(o, "ERR wrong number of arguments for 'auth' command");
                return true;
            }
            // getResult() (inside authenticateConn) yields this fiber; the loop
            // keeps serving other clients until the Argon2 worker signals.
            if (!authenticateConn(c, who, pass))
            {
                repError(o, "WRONGPASS invalid username-password pair or user is disabled.");
                return true;
            }
            repSimple(o, "OK");
            return true;
        }
    case "READONLY":
        c.readonlyFlag = true; // cluster read-only mode (flags=r); no-op otherwise
        repSimple(o, "OK");
        return true;
    case "READWRITE":
        c.readonlyFlag = false;
        repSimple(o, "OK");
        return true;
    case "RESET":
        {
            c.inMulti = false;
            c.multiHasWrite = false;
            c.multiDirty = false;
            c.multiQueue.clear();
            c.watching = false;
            c.readonlyFlag = false;
            gPubSub.dropAll(&c.sub);
            gShardPubSub.dropAll(&c.shardSub);
            trackDisable(c); // RESET clears CLIENT TRACKING state too
            c.user = aclUser("default"); // back to the default user
            c.authed = c.user is null || c.user.nopass;
            repSimple(o, "RESET");
            return true;
        }
    default:
        break;
    }

    if (c.inMulti)
    {
        c.multiQueue.append(rawCmd);
        c.multiCount++;
        if (heldByWritePause(uname, cmd)) // remember so EXEC can be held by a WRITE pause
            c.multiHasWrite = true;
        repSimple(o, "QUEUED");
        return true;
    }
    return executeCommand(c, cmd, rawCmd, o, arena, opcode, lname);
}

/// Executes one non-transactional command: pub/sub and connection commands
/// (which need the connection identity), scripting, persistence hooks, and
/// the @nogc dispatch for everything else.
private bool executeCommand(ref Conn c, const ref RVal cmd, scope const(ubyte)[] rawCmd,
        ref ByteBuffer o, ref Arena arena, int preOpcode = int.min,
        scope const(char)[] preLname = null) nothrow
{
    auto name = cmd.arr[0].str;
    auto args = cmd.arr[1 .. $];
    char[16] nbuf = void;
    if (name.length > nbuf.length)
        return dispatch(cmd, *c.dbp, o, arena);
    // IR-1 (bytecode campaign): ONE lowercase pass + ONE hash lookup resolves
    // the opcode for the WHOLE command — routing, hop, dispatch, stats and the
    // write/OOM classification all key off it. handleCommand usually hands both
    // in (resolved off its lastCmdBuf bookkeeping); the EXEC replay and other
    // internal callers let this head resolve them.
    char[16] lnbuf = void;
    const(char)[] lname;
    int opcode;
    if (preOpcode != int.min)
    {
        opcode = preOpcode;
        lname = preLname;
    }
    else
    {
        foreach (i, ch; name)
            lnbuf[i] = ch >= 'A' && ch <= 'Z' ? cast(char)(ch + 32) : ch;
        lname = cast(const(char)[]) lnbuf[0 .. name.length];
        opcode = aclCmdIndex(lname);
    }
    foreach (i, ch; lname)
        nbuf[i] = ch >= 'a' && ch <= 'z' ? cast(char)(ch - 32) : ch;
    auto uname = cast(string) nbuf[0 .. name.length];

    // Sharding: resolve the owning shard of this command's first key up front.
    // owner >= 0 => a keyed data command we DEFER to that shard (fired async below,
    // reaped in order at a flush point). owner < 0 => keyless / runs inline HERE, so
    // its reply must come AFTER any shard hops already in flight — reap them first to
    // keep pipeline order. Free when shards==1.
    int shardOpcode = opcode; // the resolved command index, reused as the hop opcode
    immutable int shardOwner = sharded() ? shardOwnerOf(cmd, opcode, lname) : -1;
    if (shardOwner == SHARD_CROSSSLOT)
    {
        repError(o, "CROSSSLOT Keys in request don't hash to the same slot");
        return true;
    }
    // BROADCAST: a keyless keyspace-wide command (KEYS/DBSIZE/…) is fired to every
    // shard and its N replies merged — before any local handling.
    if (sharded())
    {
        import dreads.aclcat : cmdIx;

        if (shardOpcode == cmdIx!"scan")
        {
            scanSharded(c, shardOpcode, cast(uint) c.dbp.db, cmd, o, arena);
            return true;
        }
        immutable bk = broadcastKindOf(shardOpcode);
        if (bk != BroadcastKind.none)
        {
            broadcastCommand(c, shardOpcode, cast(uint) c.dbp.db, cmd, rawCmd, bk, o);
            return true;
        }
        // CLIENT UNBLOCK <id> [TIMEOUT|ERROR] (phase 2.5b): the target client's
        // connection lives on ONE router's TLS registry — broadcast so its owner
        // finds it (each shard tries its own registry, sumInt merges the :0/:1s).
        // Only a VALID form is broadcast; malformed falls through to clientCmd
        // for the proper local error.
        if (shardOpcode == cmdIx!"client" && cmd.arr.length >= 3 && cmd.arr.length <= 4
                && eqICDebug(cmd.arr[1].str, "UNBLOCK"))
        {
            import dreads.commands : parseLong;

            long ubId;
            immutable idOk = parseLong(cmd.arr[2].str, ubId) && ubId >= 0;
            immutable modeOk = cmd.arr.length == 3
                || eqICDebug(cmd.arr[3].str, "TIMEOUT") || eqICDebug(cmd.arr[3].str, "ERROR");
            if (idOk && modeOk)
            {
                broadcastCommand(c, shardOpcode, cast(uint) c.dbp.db, cmd, rawCmd,
                        BroadcastKind.sumInt, o);
                return true;
            }
        }
        // PUBLISH / SPUBLISH (phase 2.5c): subscribers register on their OWN
        // router's TLS PubSub, so a publish must reach every shard; each shard
        // delivers locally and replies its receiver count, sumInt merges the
        // total (Redis's contract). Zero subscribers anywhere ⇒ :0 without a
        // hop (the gSubTotal gate). RESP2 subscribe-mode conns fall through to
        // the switch's restriction error, malformed arity to the local error.
        if ((shardOpcode == cmdIx!"publish" || shardOpcode == cmdIx!"spublish")
                && cmd.arr.length == 3 && !(c.totalSubs > 0 && !c.resp3))
        {
            import core.atomic : atomicLoad, MemoryOrder;
            import dreads.pubsub : gSubTotal;

            if (atomicLoad!(MemoryOrder.raw)(gSubTotal) == 0)
                repInt(o, 0);
            else
                broadcastCommand(c, shardOpcode, cast(uint) c.dbp.db, cmd, rawCmd,
                        BroadcastKind.sumInt, o);
            return true;
        }
        // PUBSUB introspection (phase 2.5c): subscriber registries are per-router
        // TLS, so CHANNELS/NUMSUB/NUMPAT must aggregate across shards. NUMPAT is
        // a sum of per-shard unique counts (overcounts only when the SAME pattern
        // is subscribed on two routers — acceptable for an introspection probe).
        if (shardOpcode == cmdIx!"pubsub" && cmd.arr.length >= 2)
        {
            auto psub = cmd.arr[1].str;
            BroadcastKind pk = BroadcastKind.none;
            if (eqICDebug(psub, "CHANNELS") || eqICDebug(psub, "SHARDCHANNELS"))
                pk = BroadcastKind.unionArr;
            else if ((eqICDebug(psub, "NUMSUB") || eqICDebug(psub, "SHARDNUMSUB"))
                    && cmd.arr.length > 2)
                pk = BroadcastKind.sumPairs;
            else if (eqICDebug(psub, "NUMPAT"))
                pk = BroadcastKind.unionCount;
            if (pk != BroadcastKind.none)
            {
                broadcastCommand(c, shardOpcode, cast(uint) c.dbp.db, cmd, rawCmd, pk, o);
                return true;
            }
        }
        // BGREWRITEAOF (phase 2.6): each shard rewrites its OWN file, on its own
        // thread (the rewrite reads that shard's keyspaces). gateOk merges +OK.
        if (shardOpcode == cmdIx!"bgrewriteaof")
        {
            broadcastCommand(c, shardOpcode, cast(uint) c.dbp.db, cmd, rawCmd,
                    BroadcastKind.gateOk, o);
            return true;
        }
        // INFO (phase 2.5c, workstream (d)): stats are TLS per shard — every
        // shard renders its own text (the drain's plain dispatch; its keyspace
        // section iterates that shard's own slice) and the merge sums the
        // per-shard numeric fields. See mergeInfoTexts for the field policy.
        if (shardOpcode == cmdIx!"info")
        {
            broadcastCommand(c, shardOpcode, cast(uint) c.dbp.db, cmd, rawCmd,
                    BroadcastKind.infoMerge, o);
            return true;
        }
        // CONFIG RESETSTAT clears TLS counters — reset every shard's, or the
        // INFO merge keeps resurrecting the other shards' stale counts.
        if (shardOpcode == cmdIx!"config" && cmd.arr.length == 2
                && eqICDebug(cmd.arr[1].str, "RESETSTAT"))
        {
            broadcastCommand(c, shardOpcode, cast(uint) c.dbp.db, cmd, rawCmd,
                    BroadcastKind.gateOk, o);
            return true;
        }
    }
    if (shardOwner < 0 && c.shardPendCount > 0)
        flushShardPending(c, o);

    // cluster (phase 2a): serve CLUSTER, and MOVED-redirect keys this shard
    // doesn't own so a cluster-aware client re-routes.
    if (gConfig.clusterEnabled)
    {
        import dreads.cluster : redirectIfForeign, clusterCommand;

        if (uname == "CLUSTER")
            return clusterCommand(cmd.arr, o);
        if (redirectIfForeign(uname, cmd.arr, o))
            return true;
    }

    if (gMonitors.length > 0)
        feedMonitors(c, cmd);

    // A RESP3 client may run ANY command while subscribed (push frames keep
    // messages out-of-band); a RESP2 client is restricted to the pub/sub verbs
    // plus PING/QUIT/RESET/HELLO (HELLO lets it upgrade to RESP3 and escape this).
    if (c.totalSubs > 0 && !c.resp3)
    {
        // In RESP2 subscribe mode a bare reply would be ambiguous with a pushed
        // message, so PING answers as a 2-element array ["pong", <arg or "">].
        if (uname == "PING")
        {
            if (cmd.arr.length > 2)
            {
                repError(o, "ERR wrong number of arguments for 'ping' command");
                return true;
            }
            repArrayHeader(o, 2);
            repBulk(o, "pong");
            repBulk(o, cmd.arr.length == 2 ? cmd.arr[1].str : "");
            return true;
        }
        switch (uname)
        {
        case "SUBSCRIBE", "UNSUBSCRIBE", "PSUBSCRIBE", "PUNSUBSCRIBE":
        case "SSUBSCRIBE", "SUNSUBSCRIBE", "PING", "QUIT", "RESET", "HELLO":
            break;
        default:
            o.append("-ERR Can't execute '");
            foreach (ch; name)
                o.appendByte(ch == '\r' || ch == '\n' ? ' ' : ch);
            o.append(
                "': only (P)SUBSCRIBE / (P)UNSUBSCRIBE / PING / QUIT / HELLO / RESET are allowed in this context\r\n");
            return true;
        }
    }

    // BLOCKING command whose keys live on ANOTHER shard (phase 2.5b): hop it
    // there and park THERE. The wake (XADD/LPUSH) fires on the key-owner shard,
    // so the wait must live where the wake is: the owner parks a fiber against
    // its OWN gWaiters/gKeyActivity (FIFO fairness intact — every waiter of a
    // key queues in ONE place) and replies when served/timed-out/cancelled.
    // This conn's fiber waits synchronously (a blocked client cannot pipeline
    // past its block). Inside MULTI/EXEC the hop carries HOP_NOBLOCK: Redis
    // serves the one-shot equivalent (nil when empty) — but it must still be
    // served on the OWNER's keyspace, not this router's.
    if (shardOwner >= 0 && shardOpcode >= 0 && gCmdBlockingHop[shardOpcode]
            && isBlockingHopForm(shardOpcode, args))
    {
        import dreads.shard : tShard;

        if (cast(uint) shardOwner != tShard)
        {
            shardFireBlocking(c, shardOwner, shardOpcode, cast(uint) c.dbp.db, cmd,
                    rawCmd, o, c.inMulti || c.inExec);
            return true;
        }
    }

    // IR-1 fast path: a pure data command has no case below — skip the string
    // switch entirely and fall to the shared routing/dispatch tail.
    if (opcode < 0 || !gPureDispatch[opcode])
    switch (uname)
    {
    case "SSUBSCRIBE":
        {
            if (args.length == 0)
            {
                repError(o, "ERR wrong number of arguments for 'ssubscribe' command");
                return true;
            }
            enterSubMode(c); // async output before any message can be delivered
            c.replyCmdExempt = true; // ssubscribe confirmations bypass CLIENT REPLY OFF/SKIP
            foreach (ref a; args)
            {
                gShardPubSub.subscribe(&c.shardSub, a.str);
                subReply(o, "ssubscribe", a.str, c.shardSub.subCount);
            }
            return true;
        }
    case "SUNSUBSCRIBE":
        {
            c.replyCmdExempt = true; // sunsubscribe confirmations bypass CLIENT REPLY OFF/SKIP
            if (args.length == 0)
            {
                subReply(o, "sunsubscribe", null, c.shardSub.subCount);
                return true;
            }
            foreach (ref a; args)
            {
                gShardPubSub.unsubscribe(&c.shardSub, a.str);
                subReply(o, "sunsubscribe", a.str, c.shardSub.subCount);
            }
            return true;
        }
    case "SPUBLISH":
        {
            if (args.length != 2)
            {
                repError(o, "ERR wrong number of arguments for 'spublish' command");
                return true;
            }
            repInt(o, gShardPubSub.publish(args[0].str, args[1].str, "smessage"));
            return true;
        }
    case "CLIENT":
        {
            return clientCmd(c, args, o); // false ⇒ CLIENT KILL closed this conn
        }
    case "CONFIG":
        {
            configCmd(args, o);
            return true;
        }
    case "RAFT":
        {
            raftCmd(args, o);
            return true;
        }
    case "BLPOP", "BRPOP", "BZPOPMIN", "BZPOPMAX", "BLMOVE", "BRPOPLPUSH",
         "XREAD", "XREADGROUP", "BLMPOP", "BZMPOP":
        // Blocking family: served by the shared switch (also entered by
        // remoteBlockServe for hopped blocks — phase 2.5b). False ⇒ not a
        // blocking form (XREAD without BLOCK, XREADGROUP without `>`/BLOCK or
        // in MULTI): fall through to the normal one-shot dispatch path.
        if (serveBlockingSwitch(c, uname, cmd, o, arena))
            return true;
        break;
    case "HELLO":
        {
            int ver = c.resp3 ? 3 : 2;
            if (args.length >= 1)
            {
                if (args[0].str == "2")
                    ver = 2;
                else if (args[0].str == "3")
                    ver = 3;
                else
                {
                    repError(o, "NOPROTO unsupported protocol version");
                    return true;
                }
            }
            // optional [AUTH user pass] [SETNAME name]. Multiple AUTH/SETNAME
            // options are collected with the LAST taking effect (Valkey
            // precedence). Nothing is APPLIED until the whole line is validated:
            // an invalid setname or a failing AUTH leaves the connection's prior
            // name and auth untouched. SETNAME is validated here but only applied
            // AFTER a successful AUTH.
            const(char)[] authWho, authPass, setName;
            bool haveAuth, haveSetname;
            for (size_t i = 1; i < args.length;)
            {
                if (eqICDebug(args[i].str, "AUTH") && i + 2 < args.length)
                {
                    authWho = args[i + 1].str;
                    authPass = args[i + 2].str;
                    haveAuth = true;
                    i += 3;
                }
                else if (eqICDebug(args[i].str, "SETNAME") && i + 1 < args.length)
                {
                    setName = args[i + 1].str;
                    foreach (ch; setName)
                        if (ch == ' ' || ch == '\n' || ch == '\r')
                        {
                            repError(o, "ERR Client names cannot contain spaces,"
                                    ~ " newlines or special characters.");
                            return true;
                        }
                    haveSetname = true;
                    i += 2;
                }
                else
                {
                    repError(o, "ERR Syntax error in HELLO");
                    return true;
                }
            }
            if (haveAuth)
            {
                if (!authenticateConn(c, authWho, authPass))
                {
                    repError(o, "WRONGPASS invalid username-password pair or user is disabled.");
                    return true;
                }
            }
            else if (!c.authed)
            {
                // not authenticated and no AUTH supplied — Valkey requires the
                // client to be authenticated before HELLO can report the info map.
                repError(o, "NOAUTH HELLO must be called with the client already"
                        ~ " authenticated, otherwise the HELLO <proto> AUTH <user> <pass>"
                        ~ " option can be used to authenticate the client and"
                        ~ " select the RESP protocol version at the same time");
                return true;
            }
            // auth (if any) succeeded — now it's safe to apply the client name
            if (haveSetname)
            {
                c.clientName.clear();
                c.clientName.put(setName);
            }
            c.resp3 = ver == 3;
            // A client that was tracking (no redirect) over RESP2 only starts
            // receiving invalidation pushes once it upgrades to RESP3 — engage
            // its async output now so cross-fiber pushes have a queue to land on.
            if (c.resp3 && c.tracking && c.trackRedir == 0)
                enterSubMode(c);
            gRespProto = ver; // the HELLO reply itself is encoded in the new proto
            repMapHeader(o, 7);
            repBulk(o, "server");
            repBulk(o, "redis");
            repBulk(o, "version");
            repBulk(o, "7.4.0");
            repBulk(o, "proto");
            repInt(o, ver);
            repBulk(o, "id");
            repInt(o, cast(long) c.id);
            repBulk(o, "mode");
            repBulk(o, "standalone");
            repBulk(o, "role");
            repBulk(o, "master");
            repBulk(o, "modules");
            repArrayHeader(o, 0);
            return true;
        }
    case "SUBSCRIBE":
    case "PSUBSCRIBE":
        {
            bool pattern = uname[0] == 'P';
            if (args.length == 0)
            {
                repError(o, pattern
                        ? "ERR wrong number of arguments for 'psubscribe' command"
                        : "ERR wrong number of arguments for 'subscribe' command");
                return true;
            }
            enterSubMode(c); // async output before any message can be delivered
            c.replyCmdExempt = true; // (un)subscribe confirmations bypass CLIENT REPLY OFF/SKIP
            foreach (ref a; args)
            {
                if (pattern)
                    gPubSub.psubscribe(&c.sub, a.str);
                else
                    gPubSub.subscribe(&c.sub, a.str);
                subReply(o, pattern ? "psubscribe" : "subscribe", a.str, c.sub.subCount);
            }
            return true;
        }
    case "UNSUBSCRIBE":
    case "PUNSUBSCRIBE":
        {
            bool pattern = uname[0] == 'P';
            auto verb = pattern ? "punsubscribe" : "unsubscribe";
            c.replyCmdExempt = true; // (un)subscribe confirmations bypass CLIENT REPLY OFF/SKIP
            if (args.length > 0)
            {
                foreach (ref a; args)
                {
                    if (pattern)
                        gPubSub.punsubscribe(&c.sub, a.str);
                    else
                        gPubSub.unsubscribe(&c.sub, a.str);
                    subReply(o, verb, a.str, c.sub.subCount);
                }
                return true;
            }
            // no args: drop every subscription of this kind
            auto reg = pattern ? &c.sub.patterns : &c.sub.channels;
            auto names = arena.allocArray!(const(char)[])(reg.length);
            size_t n = 0;
            foreach (k, ref u; *reg)
                names[n++] = arena.dupString(k);
            if (n == 0)
            {
                subReply(o, verb, null, c.sub.subCount);
                return true;
            }
            foreach (nm; names[0 .. n])
            {
                if (pattern)
                    gPubSub.punsubscribe(&c.sub, nm);
                else
                    gPubSub.unsubscribe(&c.sub, nm);
                subReply(o, verb, nm, c.sub.subCount);
            }
            return true;
        }
    case "PUBLISH":
        {
            if (args.length != 2)
            {
                repError(o, "ERR wrong number of arguments for 'publish' command");
                return true;
            }
            repInt(o, gPubSub.publish(args[0].str, args[1].str));
            return true;
        }
    case "PUBSUB":
        {
            pubsubIntrospect(args, o);
            return true;
        }
    case "WAITAOF":
        {
            myAof().flush();
            myAof().fsyncNow();
            repArrayHeader(o, 2);
            repInt(o, myAof().enabled ? 1 : 0);
            repInt(o, 0);
            return true;
        }
    case "BGREWRITEAOF":
        {
            // Durability is Raft's job (see sync-is-noop-raft): a client asking to
            // compact the append-only log is served by the real rewrite when our
            // AOF is enabled, and is a success NO-OP otherwise — never an error
            // (Redis itself allows BGREWRITEAOF regardless of `appendonly`, and the
            // durable state here is the Raft log, not a client-triggered AOF dump).
            if (myAof().enabled && !aofRewrite(myAof(), gAofPath))
                repError(o, "ERR AOF rewrite failed");
            else
                repSimple(o, "Background append only file rewriting started");
            return true;
        }
    case "MONITOR":
        {
            if (gMonitors.length < 64)
            {
                enterSubMode(c); // monitors receive an async stream, like subscribers
                gMonitors.set(connIdKey(c.id), Unit());
                repSimple(o, "OK");
            }
            else
                repError(o, "ERR too many monitors");
            return true;
        }
    case "SAVE":
    case "BGSAVE":
        {
            // no RDB: durability is the AOF, so force it out
            myAof().flush();
            myAof().fsyncNow();
            if (uname.length == 4)
                repSimple(o, "OK");
            else
                repSimple(o, "Background saving started");
            return true;
        }
    case "LASTSAVE":
        {
            repInt(o, myAof().lastFsyncUnix);
            return true;
        }
    case "SHUTDOWN":
        {
            import core.stdc.stdlib : exit;

            myAof().flush();
            myAof().fsyncNow();
            exit(0);
        }
    case "DEBUG":
        {
            debugCmd(c, args, o);
            return true;
        }
    case "EVAL":
    case "EVALSHA":
    case "EVAL_RO":
    case "EVALSHA_RO":
        {
            bool bySha = uname == "EVALSHA" || uname == "EVALSHA_RO";
            bool readOnly = uname == "EVAL_RO" || uname == "EVALSHA_RO";
            // effects replication: the EVAL itself NEVER enters the log.
            // Each write the script performs is captured by the redis.call
            // bridge — its propagation form goes to the AOF (sink installed
            // at boot) or through raft consensus, one entry per write. A
            // script that fails halfway keeps its earlier writes in the log,
            // exactly like it keeps them in the dataset.
            import dreads.scripting : gScriptWrote, scriptSetCallerCmds;

            gScriptWrote = false;
            immutable evalPrev = gTotalErrorReplies;
            immutable evalOut = o.length;
            scriptSetCallerCmds(&c.totCmds); // each redis.call counts on the caller
            evalCommand(args, *c.dbp, o, arena, bySha, readOnly);
            scriptSetCallerCmds(null);
            propagationOverride.clear();
            // commandstats/errorstats for the EVAL itself: a Lua-level error is a
            // real leaf error; an error propagated from a redis.call already
            // bumped the counter (during the round-trip), so don't re-count it.
            immutable evalErrored = o.length > evalOut && o.data[evalOut] == '-';
            if (evalErrored && gTotalErrorReplies == evalPrev)
                statErrorReply(cast(const(char)[]) o.data[evalOut .. $]);
            {
                char[16] lc = void;
                foreach (i, ch; name)
                    lc[i] = ch >= 'A' && ch <= 'Z' ? cast(char)(ch + 32) : ch;
                statCall(aclCmdIndex(cast(const(char)[]) lc[0 .. name.length]), evalErrored);
            }
            if (gScriptWrote)
            {
                gWriteEpoch++;
                wakeKeyActivity();
                signalReadyKeys(c.dbp.db, *c.dbp);
            }
            return true;
        }
    case "SCRIPT":
        {
            immutable sob = o.length;
            scriptCommand(args, o);
            // upload persistence (mirrors propagateAclLog's AOF leg): LOAD and
            // FLUSH are durable global state, logged verbatim on this shard's
            // file — replayed via replayCommand's SCRIPT case, applyGlobals
            // routing. EVAL never enters the log (effects replication above).
            if (myAof().enabled && args.length >= 1
                    && !(o.length > sob && o.data[sob] == '-'))
            {
                auto ssub = args[0].str;
                char[8] sub2 = void;
                if (ssub.length <= sub2.length)
                {
                    foreach (i, ch; ssub)
                        sub2[i] = ch >= 'a' && ch <= 'z' ? cast(char)(ch - 32) : ch;
                    auto su2 = cast(const(char)[]) sub2[0 .. ssub.length];
                    if (su2 == "LOAD" || su2 == "FLUSH")
                        myAof().append(rawCmd);
                }
            }
            return true;
        }
    case "MIGRATE":
        {
            migrateCommand(c, cmd.arr, o);
            return true;
        }
    default:
        break;
    }

    // Sharded routing. A keyed data command whose owner is ANOTHER shard is fired
    // there without blocking (reaped in order at the next flush point). A command
    // owned by THIS shard needs no hop:
    //   - nothing queued ahead → fall through to the normal path and run inline on our
    //     own keyspace, at full single-thread speed WITH complete ACL/stats/notify;
    //   - replies queued ahead → run into an in-order ready slot (bare dispatch, like
    //     the drain) so it keeps its pipeline position — still no queue, no wakeup.
    // The SELF-QUEUE fast-path: local traffic (conn-affine, or 1/N of uniform keys)
    // never pays the cross-thread hop. Keyless commands already ran/fell through above.
    if (shardOwner >= 0)
    {
        import dreads.shard : tShard, acquireShardPending;

        if (cast(uint) shardOwner != tShard)
        {
            shardFire(c, shardOwner, shardOpcode, cast(uint) c.dbp.db, cmd, rawCmd, o); // remote hop, carries current db
            return true;
        }
        if (c.shardPendCount > 0)
        {
            // self-shard, but cross-shard replies sit ahead of us in the pipeline:
            // execute into an ordered ready slot instead of straight to `o`.
            auto p = acquireShardPending();
            p.reply.clear();
            cast(void) dispatch(cmd, *c.dbp, p.reply, arena, 0, shardOpcode);
            p.ready = true;
            if (c.shardPendCount == PIPELINE_CAP)
                flushShardPending(c, o);
            c.shardPends[c.shardPendCount++] = cast(void*) p;
            return true;
        }
        // self-shard, nothing pending → fall through to the full local dispatch below.
    }

    // IR-1: the opcode was resolved ONCE at the top — the whole tail (write/OOM
    // classification, commandstats, prefetch, dispatch) keys off it.
    immutable cidx = shardOpcode;
    {
        import dreads.acl : routeFirstKeyPos;
        immutable kp = routeFirstKeyPos(cidx);
        if (kp >= 1 && kp < cmd.arr.length)
            c.dbp.d.prefetchKey(cmd.arr[kp].str);
    }
    immutable cmdIsWrite = cmdWriteByIdx(cidx); // one array load; used across the tail

    // Raft policy gate — only when replication is configured; standalone
    // (gReplicator is null) falls straight through with zero added cost.
    if (gReplicator !is null)
    {
        // GETEX is not a logged write (a plain GETEX changes nothing and the
        // AOF stays clean via the PEXPIREAT/PERSIST override), but under raft
        // its TTL mutation must still reach followers: propose it and let the
        // injected clock keep the replay deterministic.
        if (cmdIsWrite || uname == "GETEX")
        {
            import dreads.stream : nowMs;

            // Inside a transaction keep it synchronous (atomicity + EXEC reply
            // shape); otherwise pipeline: fire without blocking and reap the
            // reply at the next flush point, so a connection's consecutive
            // writes are in flight together instead of one round-trip each.
            if (c.inMulti || c.inExec)
            {
                if (!gReplicator.isLeader)
                    repError(o, "READONLY You can't write against a read only replica.");
                else
                {
                    try
                        gReplicator.proposeWrite(rawCmd, nowMs(), cast(ushort) c.dbp.db, o);
                    catch (Exception)
                        repError(o, "ERR replication error");
                }
                return true;
            }
            auto h = gReplicator.proposeAsync(rawCmd, nowMs(), cast(ushort) c.dbp.db);
            if (h is null) // lost leadership since the flush-point check
            {
                repError(o, "READONLY You can't write against a read only replica.");
                return true;
            }
            if (c.pendingCount == PIPELINE_CAP)
                flushPending(c, o); // buffer full: reap in order, then continue
            c.pendingWrites[c.pendingCount++] = h;
            return true;
        }
        // reads are served locally (leader or follower); no AOF in raft mode
    }

    if (gConfig.maxmemory && cmdDenyOomByIdx(cidx) && !freeMemoryIfNeeded())
    {
        // refused before running: a rejected_call (not a call) + a leaf OOM error
        statRejected(cidx);
        enum oom = "OOM command not allowed when used memory > 'maxmemory'.";
        statErrorReply(oom);
        repError(o, oom);
        return true;
    }
    immutable errPrev = gTotalErrorReplies; // leaf-vs-propagated guard (see stats.d)
    auto outBefore = o.length;
    gWriteNoOp = false; // a write command may flag itself a no-op (SETBIT/BITFIELD)
    auto keep = dispatch(cmd, *c.dbp, o, arena, 0, cidx); // integer dispatch: no re-resolution
    immutable errored = o.length > outBefore && o.data[outBefore] == '-';
    // errorstats/total: only a REAL leaf error (a nested command — e.g. a script's
    // redis.call — that failed already bumped the counter during dispatch, so the
    // outer command must not re-count its propagated error).
    if (errored && gTotalErrorReplies == errPrev)
        statErrorReply(cast(const(char)[]) o.data[outBefore .. $]);
    // INFO commandstats: count the executed data command. Blocking/pubsub/connection
    // commands handled before this point are not counted — see BLACKBOX-TODO.md.
    statCall(cidx, errored);
    // A write that flagged itself a no-op (no data changed) neither dirties, wakes,
    // nor propagates — matches Redis's dirty-delta model (SETBIT/BITFIELD SET only
    // count when the bit/field actually changed).
    if (o.length > outBefore && o.data[outBefore] != '-' && !gWriteNoOp)
    {
        immutable pureWrite = cmdIsWrite; // resolved once at the tail head
        immutable isW = pureWrite || !propagationOverride.empty;
        if (isW)
        {
            gWriteEpoch++; // WATCH visibility
            wakeKeyActivity(); // wake blocked XREAD readers (fan-out)
            signalReadyKeys(c.dbp.db, *c.dbp); // wake pop-family fronts
        }
        if (myAof().enabled)
        {
            if (!propagationOverride.empty)
                myAof().append(propagationOverride.data);
            else if (pureWrite)
                myAof().appendIR(cmd, opcode, rawCmd);
        }
        // CLIENT TRACKING: a write invalidates the cached copies of its keys; a
        // read by a tracking client records its keys. Gated by gTrackCount so a
        // server with no tracking clients pays only this one comparison.
        if (gTrackCount > 0)
            trackAfterCommand(c, uname, cmd.arr, isW);
    }
    propagationOverride.clear();
    // The one-shot CLIENT CACHING toggle is consumed by the command it preceded.
    if (c.trackCachingYes && (c.trackOptin || c.trackOptout))
        c.trackCachingYes = false;
    return keep;
}

/// RAFT STATUS | LEADER | ADDNODE id@host:port | REMOVENODE id
/// Dynamic membership over joint consensus (dreads-specific admin command).
private void raftCmd(const(RVal)[] args, ref ByteBuffer o) nothrow
{
    import core.stdc.stdio : snprintf;
    import std.array : split;
    import std.conv : to;

    import raft.types : NodeId;
    import raft.vibetransport : PeerAddress;

    if (gReplicator is null)
    {
        repError(o, "ERR replication is not enabled (set raft-node-id)");
        return;
    }
    if (args.length == 0)
    {
        repError(o, "ERR wrong number of arguments for 'raft' command");
        return;
    }
    if (eqICDebug(args[0].str, "STATUS"))
    {
        auto ms = gReplicator.members;
        repArrayHeader(o, 6);
        repBulk(o, "role");
        repBulk(o, gReplicator.isLeader ? "leader" : "follower");
        repBulk(o, "leader");
        repInt(o, cast(long) gReplicator.leaderId);
        repBulk(o, "members");
        repArrayHeader(o, ms.length);
        foreach (m; ms)
            repInt(o, cast(long) m);
        return;
    }
    if (eqICDebug(args[0].str, "LEADER"))
    {
        repInt(o, cast(long) gReplicator.leaderId);
        return;
    }
    if (eqICDebug(args[0].str, "COMPACT"))
    {
        try
        {
            gReplicator.forceCompact();
            repSimple(o, "OK");
        }
        catch (Exception)
            repError(o, "ERR compaction failed");
        return;
    }
    if (!gReplicator.isLeader)
    {
        repError(o, "ERR membership changes must go through the leader");
        return;
    }
    if (eqICDebug(args[0].str, "ADDNODE") && args.length == 2)
    {
        // id@host:port
        try
        {
            auto at = (cast(string) args[1].str).split("@");
            auto hp = at[1].split(":");
            auto id = at[0].to!uint;
            auto p = PeerAddress(id, hp[0].idup, hp[1].to!ushort);
            NodeId[] target;
            foreach (m; gReplicator.members)
                target ~= m;
            foreach (m; target)
                if (m == id)
                {
                    repError(o, "ERR node already a member");
                    return;
                }
            target ~= id;
            PeerAddress[1] np = [p];
            if (gReplicator.changeMembership(target, np[]))
                repSimple(o, "OK");
            else
                repError(o, "ERR change already in flight");
        }
        catch (Exception)
            repError(o, "ERR usage: RAFT ADDNODE id@host:port");
        return;
    }
    if (eqICDebug(args[0].str, "REMOVENODE") && args.length == 2)
    {
        try
        {
            auto id = (cast(string) args[1].str).to!uint;
            NodeId[] target;
            foreach (m; gReplicator.members)
                if (m != id)
                    target ~= m;
            if (target.length == gReplicator.members.length)
            {
                repError(o, "ERR node is not a member");
                return;
            }
            PeerAddress[0] np;
            if (gReplicator.changeMembership(target, np[]))
                repSimple(o, "OK");
            else
                repError(o, "ERR change already in flight");
        }
        catch (Exception)
            repError(o, "ERR usage: RAFT REMOVENODE id");
        return;
    }
    repError(o, "ERR unknown RAFT subcommand");
}

private void repLong(ref ByteBuffer o, scope char[] buf, long v) nothrow
{
    import core.stdc.stdio : snprintf;

    auto n = snprintf(buf.ptr, buf.length, "%lld", v);
    repBulk(o, buf[0 .. n]);
}

/// DEBUG: developer/test backdoor. Real for the semantics tests rely on
/// (SLEEP freezes the loop, SET-ACTIVE-EXPIRE toggles the reaper,
/// STRINGMATCH-LEN / OBJECT introspect); no-op OK for the benign internals;
/// unknown subcommands still error like Redis.
private void debugCmd(ref Conn c, const(RVal)[] args, ref ByteBuffer o) nothrow
{
    import dreads.commands : globMatch, objEncoding;

    if (args.length == 0)
    {
        repError(o, "ERR wrong number of arguments for 'debug' command");
        return;
    }
    auto sub = args[0].str;
    if (eqICDebug(sub, "SLEEP") && args.length >= 2)
    {
        // A real, blocking sleep on the event-loop thread — like Redis, the
        // whole server (every fiber) stalls, not just this connection.
        import core.thread : Thread;
        import core.time : usecs;
        import std.conv : to;

        double secs = 0;
        try
            secs = (cast(string) args[1].str).to!double;
        catch (Exception)
        {
        }
        if (secs > 0)
        {
            try
                Thread.sleep(usecs(cast(long)(secs * 1_000_000)));
            catch (Exception)
            {
            }
        }
        repSimple(o, "OK");
    }
    else if (eqICDebug(sub, "SET-ACTIVE-EXPIRE") && args.length >= 2)
    {
        import dreads.obj : gActiveExpire;

        gActiveExpire = args[1].str != "0";
        repSimple(o, "OK");
    }
    else if (eqICDebug(sub, "STRINGMATCH-LEN") && args.length >= 3)
        repInt(o, globMatch(args[1].str, args[2].str) ? 1 : 0);
    else if (eqICDebug(sub, "OBJECT") && args.length >= 2)
    {
        import core.stdc.stdio : snprintf;

        auto obj = (*c.dbp).lookup(args[1].str);
        if (obj is null)
        {
            repError(o, "ERR no such key");
            return;
        }
        auto enc = objEncoding(obj);
        char[160] b = void;
        auto n = snprintf(b.ptr, b.length,
                "Value at:0x0 refcount:1 encoding:%.*s serializedlength:0 lru:0 lru_seconds_idle:0",
                cast(int) enc.length, enc.ptr);
        repSimple(o, b[0 .. n]);
    }
    else
    {
        // DEBUG is test/dev infrastructure, never used by real clients, so we
        // are permissive: unknown subcommands return OK rather than aborting a
        // test file. NOTE: RELOAD/LOADAOF are stubbed no-ops here — they do NOT
        // round-trip through the AOF, so "survives reload" tests pass without
        // actually exercising persistence. dreads HAS an AOF (replayed on boot,
        // covered by the storage-recovery suite); wiring an in-process AOF
        // flush+replay into DEBUG RELOAD is a TODO (see BLACKBOX-TODO.md).
        repSimple(o, "OK");
    }
}

/// CONFIG GET pattern | SET name value | REWRITE | RESETSTAT
private void configCmd(const(RVal)[] args, ref ByteBuffer o) nothrow
{
    import core.stdc.stdio : snprintf;

    if (args.length == 0)
    {
        repError(o, "ERR wrong number of arguments for 'config' command");
        return;
    }
    if (eqICDebug(args[0].str, "GET") && args.length >= 2)
    {
        // Iterate the SAME canonical registry CONFIG INFO uses, so the two commands
        // return the same directives in the same order (name, value) pairs.
        char[64] b = void;
        size_t matches = 0;
        foreach (ref m; gCfgMeta)
        {
            foreach (ref pat; args[1 .. $])
            {
                if (globMatch(pat.str, m.name))
                {
                    matches++;
                    break;
                }
            }
        }
        repMapHeader(o, matches); // CONFIG GET is a map -> %N in RESP3
        foreach (ref m; gCfgMeta)
        {
            bool hit = false;
            foreach (ref pat; args[1 .. $])
            {
                if (globMatch(pat.str, m.name))
                {
                    hit = true;
                    break;
                }
            }
            if (!hit)
                continue;
            immutable nm = m.name;
            repBulk(o, nm);
            switch (nm)
            {
            case "port":
                auto n = snprintf(b.ptr, b.length, "%u", cast(uint) gConfig.port);
                repBulk(o, b[0 .. n]);
                break;
            case "appendonly":
                repBulk(o, gConfig.appendonly ? "yes" : "no");
                break;
            case "active-expire":
                repBulk(o, gConfig.activeExpire ? "yes" : "no");
                break;
            case "active-eviction":
                repBulk(o, gConfig.activeEviction ? "yes" : "no");
                break;
            case "aof-use-rdb-preamble":
                repBulk(o, "no"); // accepted but inert — dreads' AOF is its own format
                break;
            case "appendfsync":
                repBulk(o, "everysec"); // accepted but inert — dreads uses `synchronous`
                break;
            case "rdb-version-check":
                repBulk(o, gConfig.rdbVersionCheck);
                break;
            case "appendfilename":
                repBulk(o, gConfig.appendfilename);
                break;
            case "dir":
                repBulk(o, gConfig.dir);
                break;
            case "maxmemory":
                auto n = snprintf(b.ptr, b.length, "%llu", gConfig.maxmemory);
                repBulk(o, b[0 .. n]);
                break;
            case "maxmemory-policy":
                repBulk(o, gConfig.maxmemoryPolicy);
                break;
            case "lua-time-limit":
                auto n = snprintf(b.ptr, b.length, "%lld", gConfig.luaTimeLimitMs);
                repBulk(o, b[0 .. n]);
                break;
            case "lua-memory-limit":
                auto n = snprintf(b.ptr, b.length, "%llu", gConfig.luaMemoryLimit);
                repBulk(o, b[0 .. n]);
                break;
            case "notify-keyspace-events":
                repBulk(o, gConfig.notifyKeyspaceEvents);
                break;
            case "lazyfree-lazy-server-del":
                repBulk(o, gConfig.lazyfreeLazyServerDel ? "yes" : "no");
                break;
            case "hash-max-listpack-entries", "hash-max-ziplist-entries":
                repLong(o, b, gConfig.hashMaxListpackEntries);
                break;
            case "hash-max-listpack-value", "hash-max-ziplist-value":
                repLong(o, b, gConfig.hashMaxListpackValue);
                break;
            case "list-max-listpack-size", "list-max-ziplist-size":
                repLong(o, b, gConfig.listMaxListpackSize);
                break;
            case "list-compress-depth":
                repLong(o, b, gConfig.listCompressDepth);
                break;
            case "set-max-intset-entries":
                repLong(o, b, gConfig.setMaxIntsetEntries);
                break;
            case "set-max-listpack-entries":
                repLong(o, b, gConfig.setMaxListpackEntries);
                break;
            case "set-max-listpack-value":
                repLong(o, b, gConfig.setMaxListpackValue);
                break;
            case "zset-max-listpack-entries", "zset-max-ziplist-entries":
                repLong(o, b, gConfig.zsetMaxListpackEntries);
                break;
            case "zset-max-listpack-value", "zset-max-ziplist-value":
                repLong(o, b, gConfig.zsetMaxListpackValue);
                break;
            case "stream-node-max-entries":
                repLong(o, b, gConfig.streamNodeMaxEntries);
                break;
            case "stream-node-max-bytes":
                auto n = snprintf(b.ptr, b.length, "%llu", gConfig.streamNodeMaxBytes);
                repBulk(o, b[0 .. n]);
                break;
            case "proto-max-bulk-len":
                auto n = snprintf(b.ptr, b.length, "%llu", gConfig.protoMaxBulkLen);
                repBulk(o, b[0 .. n]);
                break;
            case "client-query-buffer-limit":
                auto n = snprintf(b.ptr, b.length, "%llu", gConfig.clientQueryBufferLimit);
                repBulk(o, b[0 .. n]);
                break;
            case "acllog-max-len":
                auto n = snprintf(b.ptr, b.length, "%lld", gAclLogMaxLen);
                repBulk(o, b[0 .. n]);
                break;
            // Metadata-carried directives (no gConfig backing) — report stable
            // Redis-shaped values so CONFIG GET matches CONFIG INFO's directive set.
            case "activerehashing":
                repBulk(o, "yes"); // dreads rehashes incrementally, always on
                break;
            case "maxclients":
                repBulk(o, "10000");
                break;
            case "databases":
                auto n = snprintf(b.ptr, b.length, "%d", cast(int) RESP_DBS);
                repBulk(o, b[0 .. n]);
                break;
            case "repl-diskless-load":
                repBulk(o, "disabled");
                break;
            case "dbfilename":
                repBulk(o, "dump.rdb");
                break;
            case "save", "requirepass", "replicaof", "slaveof":
                repBulk(o, ""); // no scheduled RDB save / no auth / no legacy replicaof
                break;
            default:
                repBulk(o, ""); // known name with no value formatter
            }
        }
        return;
    }
    // CONFIG SET key value [key value ...] — Redis 7 accepts multiple pairs.
    if (eqICDebug(args[0].str, "SET") && args.length >= 3 && args.length % 2 == 1)
    {
        import std.uni : toLower;

        // apply one directive; 0 = ok, 1 = unknown/startup-only, 2 = bad value
        int applyOne(string lname, string value) nothrow
        {
            if (lname == "acllog-max-len") // maps to the ACL LOG cap, not gConfig
            {
                long v;
                if (parseLong(value, v) && v >= 0)
                {
                    gAclLogMaxLen = v; // no retroactive trim of existing entries
                    return 0;
                }
                return 2;
            }
            if (!isRuntimeSettable(lname))
            {
                // COMPAT MODE: a known Valkey param we don't model returns OK and
                // does nothing (an explicit shim, not real support); a genuinely
                // unknown name is still rejected.
                if (isCompatModeParam(lname))
                    return 0;
                return 1; // startup-only or unknown parameters
            }
            bool ok = false;
            try
                ok = applyDirective(lname, value, gConfig);
            catch (Exception)
            {
            }
            return ok ? 0 : 2;
        }

        for (size_t i = 1; i + 1 < args.length; i += 2)
        {
            string lname, value;
            try
            {
                lname = (cast(string) args[i].str).idup.toLower;
                value = (cast(string) args[i + 1].str).idup;
            }
            catch (Exception)
            {
            }
            immutable rc = applyOne(lname, value);
            if (rc == 1)
            {
                repError(o, "ERR Unsupported CONFIG parameter");
                return;
            }
            else if (rc == 2)
            {
                repError(o, "ERR CONFIG SET failed - unable to set the value");
                return;
            }
        }
        import dreads.notify : parseNotifyFlags, notifyFlagsToString;
        import dreads.obj : gActiveExpire, gActiveEviction;

        gActiveExpire = gConfig.activeExpire; // mirror the runtime toggles
        gActiveEviction = gConfig.activeEviction;
        if (parseNotifyFlags(gConfig.notifyKeyspaceEvents, gNotifyFlags))
        {
            // store back the CANONICAL form so CONFIG GET round-trips normalized
            // (Redis: `KA` -> `AK`, `EA` -> `AE`, class flags in a fixed order).
            char[24] fb = void;
            gConfig.notifyKeyspaceEvents = notifyFlagsToString(gNotifyFlags, fb[]).idup;
        }
        repSimple(o, "OK");
        return;
    }
    if (eqICDebug(args[0].str, "REWRITE") || eqICDebug(args[0].str, "RESETSTAT"))
    {
        if (eqICDebug(args[0].str, "RESETSTAT"))
        {
            import dreads.obj : gExpiredKeys, gExpiredFields;

            resetCmdStats(); // clear INFO commandstats counters
            resetErrorStats(); // clear errorstats + total_error_replies
            gExpiredKeys = 0;
            gExpiredFields = 0;
        }
        repSimple(o, "OK");
        return;
    }
    if (eqICDebug(args[0].str, "HELP"))
    {
        repHelp!"CONFIG"(o);
        return;
    }
    if (eqICDebug(args[0].str, "INFO"))
    {
        configInfo(args[1 .. $], o);
        return;
    }
    repUnknownSubcommand(o, "CONFIG", args.length ? args[0].str : "");
}

// CONFIG INFO metadata: enough shape (name/type/values/range) for tooling and
// the Valkey suite's type probes; not an exhaustive mirror of Valkey's table.
private struct CfgMeta
{
    string name;
    string type; // bool | numeric | string | enum | special
    immutable(string)[] values; // enum choices
    bool hasRange;
    long lo, hi;
    immutable(string)[] flags; // e.g. immutable | sensitive | alias
    string aliasName; // the paired name for aliased directives ("" = none)
}

// Single canonical, ORDERED config registry. BOTH `CONFIG GET` and `CONFIG INFO`
// iterate this one list, so the two commands always return the same directives in
// the same order (the "CONFIG INFO ordering is consistent across calls" contract).
// The `-ziplist-` names are the legacy aliases of the `-listpack-` ones (carried
// as plain numeric entries — CONFIG GET maps them to the same gConfig field).
private static immutable CfgMeta[] gCfgMeta = [
    {"appendonly", "bool"},
    {"appendfsync", "enum", ["always", "everysec", "no"]},
    {"appendfilename", "string"},
    {"aof-use-rdb-preamble", "bool"},
    {"acllog-max-len", "numeric", null, true, 0, long.max},
    {"active-expire", "bool"},
    {"active-eviction", "bool"},
    {"activerehashing", "bool"},
    {"lazyfree-lazy-server-del", "bool"},
    {"port", "numeric", null, true, 0, 65_535},
    {"maxclients", "numeric", null, true, 1, long.max},
    {"maxmemory", "numeric", null, true, 0, long.max},
    {"maxmemory-policy", "enum", [
        "noeviction", "allkeys-lru", "volatile-lru", "allkeys-random",
        "volatile-random", "volatile-ttl"
    ]},
    {"proto-max-bulk-len", "numeric", null, true, 0, long.max},
    {"client-query-buffer-limit", "numeric", null, true, 0, long.max},
    {"lua-time-limit", "numeric", null, true, 0, long.max},
    {"lua-memory-limit", "numeric", null, true, 0, long.max},
    {"hash-max-listpack-entries", "numeric", null, true, 0, long.max},
    {"hash-max-listpack-value", "numeric", null, true, 0, long.max},
    {"hash-max-ziplist-entries", "numeric", null, true, 0, long.max},
    {"hash-max-ziplist-value", "numeric", null, true, 0, long.max},
    {"list-max-listpack-size", "numeric", null, true, long.min, long.max},
    {"list-max-ziplist-size", "numeric", null, true, long.min, long.max},
    {"list-compress-depth", "numeric", null, true, 0, long.max},
    {"set-max-intset-entries", "numeric", null, true, 0, long.max},
    {"set-max-listpack-entries", "numeric", null, true, 0, long.max},
    {"set-max-listpack-value", "numeric", null, true, 0, long.max},
    {"zset-max-listpack-entries", "numeric", null, true, 0, long.max},
    {"zset-max-listpack-value", "numeric", null, true, 0, long.max},
    {"zset-max-ziplist-entries", "numeric", null, true, 0, long.max},
    {"zset-max-ziplist-value", "numeric", null, true, 0, long.max},
    {"stream-node-max-entries", "numeric", null, true, 0, long.max},
    {"stream-node-max-bytes", "numeric", null, true, 0, long.max},
    {"dir", "string"},
    {"dbfilename", "string"},
    {"rdb-version-check", "enum", ["strict", "relaxed"]},
    {"repl-diskless-load", "enum", ["disabled", "on-empty-db", "swapdb"]},
    {"save", "special"},
    {"notify-keyspace-events", "special"},
    {"databases", "numeric", null, true, 1, 65_535, ["immutable"]},
    {"requirepass", "string", null, false, 0, 0, ["sensitive"]},
    {"replicaof", "string", null, false, 0, 0, null, "slaveof"},
    {"slaveof", "string", null, false, 0, 0, ["alias"], "replicaof"},
];

/// CONFIG INFO [name-or-glob ...] — array of per-directive metadata maps.
private void configInfo(const(RVal)[] pats, ref ByteBuffer o) nothrow
{
    static bool hit(const(RVal)[] pats, string nm) nothrow
    {
        if (pats.length == 0)
            return true;
        foreach (ref p; pats)
            if (globMatch(p.str, nm))
                return true;
        return false;
    }

    size_t matches = 0;
    foreach (ref m; gCfgMeta)
        if (hit(pats, m.name))
            matches++;
    repArrayHeader(o, matches);
    foreach (ref m; gCfgMeta)
    {
        if (!hit(pats, m.name))
            continue;
        // name + type + flags are always present; values/range/alias vary
        auto pairs = 3 + (m.values.length ? 1 : 0) + (m.hasRange ? 1 : 0)
            + (m.aliasName.length ? 1 : 0);
        repMapHeader(o, pairs);
        repBulk(o, "name");
        repBulk(o, m.name);
        repBulk(o, "type");
        repBulk(o, m.type);
        if (m.values.length)
        {
            repBulk(o, "values");
            repArrayHeader(o, m.values.length);
            foreach (v; m.values)
                repBulk(o, v);
        }
        if (m.hasRange)
        {
            repBulk(o, "range");
            repArrayHeader(o, 2);
            repInt(o, m.lo);
            repInt(o, m.hi);
        }
        repBulk(o, "flags");
        repArrayHeader(o, m.flags.length);
        foreach (f; m.flags)
            repBulk(o, f);
        if (m.aliasName.length)
        {
            repBulk(o, "alias");
            repBulk(o, m.aliasName);
        }
    }
}

// ---------------------------------------------------------------------------
// maxmemory / LRU eviction (jemalloc-backed accounting; Linux only)
// ---------------------------------------------------------------------------

import dreads.mem : usedMemory; // jemalloc accounting (shared with INFO)

private size_t gEvictCursor; // TLS: eviction walks THIS shard's keyspace only

// Set when an expiry/eviction DEL was tryPut to the raft proposal queue (which
// skips the consumer wake to stay @nogc); the periodic timer nudges the consumer
// to drain them. Avoids waking the raft loop when nothing is pending.
private bool gExpireDelPending; // TLS: per-shard active-expire state

/// Approximate LRU eviction: sample live keys, evict the coldest, repeat.
/// Returns false when memory stays over the limit (noeviction, or nothing
/// evictable under volatile-lru).
// Master-authoritative reap decision — installed as obj.gExpireReapHook. Given an
// expired `key` in db `db`, it propagates the DELETE side effect and returns
// whether the data layer should delete `key` locally NOW. This is what keeps a
// key's death a single agreed truth instead of a per-node clock race.
private bool expireReap(scope const(char)[] key, ubyte db) @nogc nothrow
{
    import dreads.obj : gApplying;

    // Deterministic apply re-execution: every node reaps identically under the
    // entry's injected clock, so delete in place — no fresh DEL to propagate.
    if (gApplying)
        return true;
    static ByteBuffer del; // main-loop-only scratch (single writer)
    del.clear();
    repArrayHeader(del, 2);
    repBulk(del, "DEL");
    repBulk(del, key);
    if (gReplicator is null)
    {
        // standalone: the DEL is the master's own — log it, then delete locally.
        if (myAof().enabled)
            myAof().append(del.data);
        return true;
    }
    if (gReplicator.isLeader)
    {
        // leader (Redis-primary model): propose the DEL so every follower drops
        // the same key from the log — the removal becomes a single agreed truth —
        // and delete locally ONLY if it was enqueued. If the propose was dropped
        // (queue full), KEEP the key (it reads nil, retries next access): deleting
        // it locally without propagating would fork the truth on the followers.
        if (gReplicator.proposeServerWrite(del.data, db))
        {
            gExpireDelPending = true; // wake the consumer on the next timer tick
            return true;
        }
        return false;
    }
    return false; // follower: never self-expire — wait for the leader's DEL
}

// volatile-* only touches keys with a TTL; *-random picks a sampled key blindly
// instead of by LRU/LFU (both share obj.lruSecs, lowest = evict-first).
private void evictionMode(out bool volatileOnly, out bool randomPick) nothrow
{
    auto p = gConfig.maxmemoryPolicy;
    volatileOnly = p.length >= 9 && p[0 .. 9] == "volatile-";
    randomPick = p.length >= 7 && p[$ - 7 .. $] == "-random";
}

// Evict one victim from `ks` per the policy: sample up to 5 live keys from a
// rotating cursor, pick by LRU/LFU (or at random), propagate DEL, notify, count.
// Returns false when nothing is evictable (empty, or volatile-only with no TTLs).
private bool evictOneVictim(ref Keyspace ks, bool volatileOnly, bool randomPick) nothrow
{
    import dreads.notify : notifyKeyspaceEvent, NClass, gNotifyDb;
    import dreads.obj : gEvictedKeys;

    gNotifyDb = ks.db; // "evicted" fires on the victim db's channel
    auto cap = ks.d.capacity;
    if (cap == 0 || ks.length == 0)
        return false;
    const(char)[] victim;
    uint victimLru = uint.max;
    size_t seen = 0;
    size_t i = gEvictCursor % cap;
    size_t scanned = 0;
    import dreads.det : detNow = now;

    while (seen < 5 && scanned < cap)
    {
        if (ks.d.slotLive(i))
        {
            auto obj = ks.d.valAt(i);
            // An already-expired key sampled during the eviction scan is reaped
            // for free — clean the dead before evicting the living (no live data
            // lost). lookup() does the disarm+del+notify+expired-count.
            if (obj.expireAtMs != 0 && detNow() >= obj.expireAtMs)
            {
                cast(void) ks.lookup(ks.d.keyAt(i));
                gEvictCursor = i + 1;
                return true;
            }
            if (!volatileOnly || obj.expireAtMs != 0)
            {
                seen++;
                if (randomPick)
                {
                    victim = ks.d.keyAt(i);
                    break;
                }
                if (obj.lruSecs <= victimLru)
                {
                    victimLru = obj.lruSecs;
                    victim = ks.d.keyAt(i);
                }
            }
        }
        i = (i + 1) % cap;
        scanned++;
    }
    gEvictCursor = i + 1;
    if (victim is null)
        return false; // nothing evictable
    // Propagate the eviction DEL so every node drops the SAME victim — else a
    // follower keeps a key the leader evicted (a fork). Under raft the leader
    // proposes it (commit applies it everywhere); standalone logs it to the AOF.
    // Built BEFORE ks.d.del below (victim is a non-owning slice freed by del).
    {
        static ByteBuffer delCmd; // TLS scratch
        delCmd.clear();
        repArrayHeader(delCmd, 2);
        repBulk(delCmd, "DEL");
        repBulk(delCmd, victim);
        if (gReplicator !is null)
        {
            // Eviction frees memory NOW (leader deletes below regardless), so —
            // unlike expiry — we propose best-effort and don't gate the local
            // delete on it: memory pressure can't wait. A dropped eviction DEL is
            // a rare edge (queue full under load) that self-heals on re-eviction.
            cast(void) gReplicator.proposeServerWrite(delCmd.data, cast(ushort) ks.db);
            gExpireDelPending = true; // wake the consumer on the next timer tick
        }
        else if (myAof().enabled)
            myAof().append(delCmd.data);
    }
    notifyKeyspaceEvent(NClass.evicted, "evicted", victim);
    if (gTrackCount) // CLIENT TRACKING: an evicted key invalidates cached copies
    {
        trackInvalidateKey(victim);
        gExpireKeys.set(victim, Unit()); // server-caused: exempt from NOLOOP
    }
    ks.d.del(victim);
    gWriteEpoch++;
    gEvictedKeys++;
    return true;
}

// Write path: free memory synchronously before an allocating write. Evicts across
// every database (like Redis samples all dbs); budgeted so one command can't stall.
private bool freeMemoryIfNeeded() nothrow
{
    if (usedMemory() <= gConfig.maxmemory)
        return true;
    if (gConfig.maxmemoryPolicy == "noeviction")
        return false;
    // The OOM gate runs before dispatch sets the per-command clock, so refresh it
    // here — evictOneVictim reaps expired keys it samples (against detNow()).
    refreshDetClock();
    bool volatileOnly, randomPick;
    evictionMode(volatileOnly, randomPick);
    // Evict across EVERY database (Redis samples all dbs), not just db 0.
    foreach (_; 0 .. 128) // eviction budget per triggering command
    {
        bool any = false;
        foreach (ref d; gDbs)
        {
            if (usedMemory() <= gConfig.maxmemory)
                return true;
            if (evictOneVictim(d, volatileOnly, randomPick))
                any = true;
        }
        if (!any)
            break; // nothing evictable in any db
    }
    return usedMemory() <= gConfig.maxmemory;
}

// Timer path (opt-in `active-eviction`): Redis evicts on a cron too — a key can
// be dropped without a subsequent write. Sweeps every db. CLIENT PAUSE holds it
// back: no eviction while a pause window is open (the effect waits for unpause).
// Active-expire sweep of the databases THIS thread owns (phase 2.5c: every
// shard runs its own maintenance — the main thread's timers only see shard 0's
// slice, so a TTL'd key on shard 2 would otherwise never actively expire and
// its `expired` notification would never fire).
private void maintExpireTick() @trusted nothrow
{
    import dreads.det : freezeClock;
    import dreads.obj : gActiveExpire;

    // Cheap early-out: with active expiry off (the default) this fast timer
    // must do NOTHING — no clock read, no db sweep, no flush.
    if (!gActiveExpire)
        return;
    // Master-authoritative: ONLY the leader actively reaps and propagates
    // the expiry DELs; a follower applies those from the log and never
    // self-expires (that would fork the truth about a key's life).
    if (gReplicator !is null && !gReplicator.isLeader)
        return;
    freezeClock(0); // pin this cycle's clock to wall time (see maintEvictionTick)
    // A CLIENT PAUSE freezes replicated mutation: active expiry (an expiry
    // is a propagated DEL) is held until the window lifts, like eviction.
    immutable paused = gPauseUntilMs != 0 && nowMs() < gPauseUntilMs;
    if (paused)
        return;
    foreach (ref d; myDbSlice()) // drop-soon sweep across every database WE own
    {
        gNotifyDb = d.db; // "expired" fires on THIS db's channel
        d.activeExpireCycle();
        d.activeSubExpireCycle(); // reap due hash-field TTLs (the "path pro resto")
    }
    flushPendingNotify(); // deliver the "expired"/"hexpired" events queued
    if (gTrackCount) // deliver invalidations queued by active expiry (no writer)
        flushTrackingInval(0);
}

// Active AMQP x-message-ttl expiry on its own FAST timer (50ms): RabbitMQ
// fires a per-queue timer, so expiry latency is ~TTL + tens of ms — riding the
// 1s eviction tick left 1ms-TTL messages sitting for up to a second. Cheap
// when no TTL queues exist (one pass over this shard's gQueueMeta).
private void amqpTtlTick() @trusted nothrow
{
    import dreads.det : freezeClock;
    import dreads.amqp : amqpTtlSweep;

    freezeClock(0); // pin this cycle's clock to wall time (see maintEvictionTick)
    amqpTtlSweep();
    flushPendingNotify(); // deliver any events the sweep queued
}

// Eviction tick for the databases THIS thread owns (see maintExpireTick).
private void maintEvictionTick() @trusted nothrow
{
    // Pin THIS cycle's clock to wall time. detNow() otherwise returns the
    // last command's frozen gClock (never reset to 0 after dispatch), which
    // is stale here — so a background eviction cycle would compare against a
    // frozen "now".
    import dreads.det : freezeClock;
    import dreads.obj : lruClock;

    freezeClock(0); // 0 => freeze the current wall clock into gClock
    lruClock = cast(uint)(nowMs() / 1000);
    runEvictionCycle(); // opt-in background maxmemory eviction (skips under pause)
    flushPendingNotify(); // deliver any events the eviction cycle queued
    if (gTrackCount)
        flushTrackingInval(0);
    // (the AMQP x-message-ttl reaper runs on its own fast 50ms timer —
    // amqpTtlTick — so a 1ms-TTL queue expires promptly, RabbitMQ-style)
    // $SYS/broker/* stats every ~10 ticks (10s), per shard, to its subscribers
    if (gConfig.mqttPort != 0)
    {
        import dreads.mqtt : mqttPublishSys, mqttExpireRetained, mqttReapOfflineConns;

        // reap v5-message-expired retained messages every tick so dead data
        // can't pin the retained caps between SUBSCRIBEs (cheap: no-op when
        // nothing is expired)
        mqttExpireRetained();
        mqttReapOfflineConns(); // reap persistent sessions past their expiry
        static uint sysTick;
        if (++sysTick >= 10)
        {
            sysTick = 0;
            mqttPublishSys();
        }
    }
    // AOF-per-shard: each shard fsyncs its OWN file (the "everysec" contract,
    // now per shard — this tick runs on every shard thread and on main).
    myAof().flush();
    myAof().fsyncNow();
}

private void runEvictionCycle() nothrow
{
    import dreads.obj : gActiveEviction;

    if (!gActiveEviction || gConfig.maxmemory == 0
        || gConfig.maxmemoryPolicy == "noeviction")
        return;
    if (gPauseUntilMs != 0 && nowMs() < gPauseUntilMs)
        return; // eviction is skipped during a client pause
    bool volatileOnly, randomPick;
    evictionMode(volatileOnly, randomPick);
    foreach (ref d; myDbSlice())
    {
        size_t budget = 0;
        while (usedMemory() > gConfig.maxmemory && budget++ < 1024)
            if (!evictOneVictim(d, volatileOnly, randomPick))
                break; // nothing evictable in this db
        if (usedMemory() <= gConfig.maxmemory)
            break;
    }
}

// ---------------------------------------------------------------------------
// MIGRATE — option 2: DUMP the key(s) here, RESTORE them onto the target over a
// cached outbound socket, then DEL locally (unless COPY). The socket is cached
// per host:port and released after idle (INFO migrate_cached_sockets), matching
// Redis. This is server-layer (owns an outbound TCPConnection), not data-plane.
// ---------------------------------------------------------------------------
private struct MigrateSock
{
    TCPConnection conn;
    char[128] hp = void; // "host:port" cache key (inline, no allocation)
    size_t hplen;
    ulong lastUsed; // nowMs of last use — for idle release
    bool alive;
}

private MigrateSock[8] gMigrateCache; // TLS: a socket may only be used by its opening thread
private enum ulong MIGRATE_IDLE_MS = 10_000; // release a cached socket after 10s idle

public size_t migrateCachedCount() @nogc nothrow
{
    size_t n = 0;
    foreach (ref m; gMigrateCache)
        if (m.alive)
            n++;
    return n;
}

// Close cached sockets idle longer than MIGRATE_IDLE_MS (called from the 1s timer).
private void releaseIdleMigrateConns() nothrow
{
    immutable now = nowMs();
    foreach (ref m; gMigrateCache)
        if (m.alive && now - m.lastUsed >= MIGRATE_IDLE_MS)
        {
            try
                m.conn.close();
            catch (Exception)
            {
            }
            m.alive = false;
            m.hplen = 0;
        }
}

// Read one RESP simple-status/error line ("+OK", "-ERR ...") into `line`.
private bool migrateReadLine(ref TCPConnection conn, ref char[512] line, out size_t n) nothrow
{
    import vibe.core.stream : IOMode;

    n = 0;
    try
        while (n < line.length)
        {
            ubyte[1] ch;
            if (conn.read(ch[], IOMode.all) != 1)
                return false;
            if (ch[0] == '\n')
            {
                if (n > 0 && line[n - 1] == '\r')
                    n--; // strip CRLF
                return true;
            }
            line[n++] = cast(char) ch[0];
        }
    catch (Exception)
        return false;
    return false;
}

// Send one command as a RESP array of bulk strings.
private bool migrateSend(ref TCPConnection conn, scope const(const(char)[])[] parts) nothrow
{
    static ByteBuffer wb; // TLS
    wb.clear();
    repArrayHeader(wb, cast(uint) parts.length);
    foreach (p; parts)
        repBulk(wb, p);
    try
    {
        conn.write(cast(const(ubyte)[]) wb.data);
        return true;
    }
    catch (Exception)
        return false;
}

private void migrateCommand(ref Conn c, const(RVal)[] arr, ref ByteBuffer o) nothrow
{
    import dreads.commands : dumpKeyPayload, MigrateArgs, parseMigrateArgs;
    import dreads.mem : ByteBuffer, Arena;

    // MIGRATE host port key destdb timeout [COPY] [REPLACE] [AUTH pw|AUTH2 u p] [KEYS k...]
    MigrateArgs ma;
    if (!parseMigrateArgs(arr, ma))
    {
        repError(o, ma.err);
        return;
    }
    auto host = ma.host;
    immutable port = ma.port, destdb = ma.destdb;
    immutable copy = ma.copy, replace = ma.replace, hasAuth = ma.hasAuth;
    auto authUser = ma.authUser, authPw = ma.authPw;
    auto keyList = ma.keyList;
    const(char)[] singleKey = ma.singleKey;

    // collect the keys that actually exist locally (Redis skips missing ones)
    static const(char)[][256] keybuf;
    size_t nk = 0;
    void consider(scope const(char)[] k) @nogc nothrow
    {
        if (nk < keybuf.length && c.dbp.lookup(k, false) !is null)
            keybuf[nk++] = k;
    }

    if (keyList.length)
        foreach (ref k; keyList)
            consider(k.str);
    else
        consider(singleKey);

    if (nk == 0)
    {
        repSimple(o, "NOKEY");
        return;
    }

    // get / open the cached outbound socket for host:port
    static ByteBuffer hpbuf; // TLS
    hpbuf.clear();
    hpbuf.append(host);
    hpbuf.appendByte(':');
    {
        char[8] pb = void;
        import core.stdc.stdio : snprintf;

        auto pl = snprintf(pb.ptr, pb.length, "%lld", port);
        hpbuf.append(pb[0 .. pl]);
    }
    auto hpkey = cast(const(char)[]) hpbuf.data;
    MigrateSock* slot;
    foreach (ref m; gMigrateCache)
        if (m.alive && m.hp[0 .. m.hplen] == hpkey)
        {
            slot = &m;
            break;
        }
    if (slot is null)
    {
        // find a free slot (or fail gracefully if the small cache is full)
        foreach (ref m; gMigrateCache)
            if (!m.alive)
            {
                slot = &m;
                break;
            }
        if (slot is null)
            slot = &gMigrateCache[0]; // reuse slot 0 (close the old one below)
        if (slot.alive)
        {
            try
                slot.conn.close();
            catch (Exception)
            {
            }
        }
        try
        {
            slot.conn = connectTCP(host.idup, cast(ushort) port);
            slot.conn.tcpNoDelay = true;
        }
        catch (Exception)
        {
            slot.alive = false;
            repError(o, "IOERR error or timeout connecting to the client");
            return;
        }
        slot.hplen = hpkey.length <= slot.hp.length ? hpkey.length : slot.hp.length;
        slot.hp[0 .. slot.hplen] = hpkey[0 .. slot.hplen];
        slot.alive = true;
    }
    slot.lastUsed = nowMs();

    char[512] line = void;
    size_t ln;
    bool fail(string msg) nothrow
    {
        // a broken socket must not be reused
        try
            slot.conn.close();
        catch (Exception)
        {
        }
        slot.alive = false;
        slot.hplen = 0;
        repError(o, msg);
        return false;
    }

    // AUTH (optional), then SELECT the destination db
    if (hasAuth)
    {
        immutable okAuth = authUser.length
            ? migrateSend(slot.conn, ["AUTH", authUser, authPw])
            : migrateSend(slot.conn, ["AUTH", authPw]);
        if (!okAuth || !migrateReadLine(slot.conn, line, ln))
        {
            fail("IOERR error or timeout writing to target instance");
            return;
        }
        if (ln == 0 || line[0] != '+')
        {
            fail(cast(string)("ERR Target instance replied with error: " ~ (ln > 1
                    ? line[1 .. ln].idup : "auth failed")));
            return;
        }
    }
    {
        char[24] db = void;
        import core.stdc.stdio : snprintf;

        auto dl = snprintf(db.ptr, db.length, "%lld", destdb);
        if (!migrateSend(slot.conn, ["SELECT", db[0 .. dl]])
            || !migrateReadLine(slot.conn, line, ln) || ln == 0 || line[0] != '+')
        {
            fail("IOERR error or timeout reading from target instance");
            return;
        }
    }

    // DUMP + RESTORE each key (pipeline the RESTOREs, then read replies)
    Arena arena;
    static ByteBuffer payload; // TLS: the DUMP payload for one key
    foreach (ki; 0 .. nk)
    {
        auto k = keybuf[ki];
        if (!dumpKeyPayload(*c.dbp, k, arena, payload))
        {
            fail("ERR DUMP is not supported for this value type");
            return;
        }
        // remaining TTL in ms (0 = no expiry) — RESTORE re-arms it
        auto obj = c.dbp.lookup(k, false);
        long ttl = 0;
        if (obj !is null && obj.expireAtMs != 0)
        {
            immutable now = nowMs();
            ttl = obj.expireAtMs > now ? cast(long)(obj.expireAtMs - now) : 1;
        }
        char[24] tb = void;
        import core.stdc.stdio : snprintf;

        auto tl = snprintf(tb.ptr, tb.length, "%lld", ttl);
        immutable ok = replace
            ? migrateSend(slot.conn, ["RESTORE", k, tb[0 .. tl],
                    cast(const(char)[]) payload.data, "REPLACE"])
            : migrateSend(slot.conn, ["RESTORE", k, tb[0 .. tl],
                    cast(const(char)[]) payload.data]);
        if (!ok)
        {
            fail("IOERR error or timeout writing to target instance");
            return;
        }
        arena.reset();
    }
    // read the RESTORE replies in order; any error aborts (nothing deleted)
    foreach (ki; 0 .. nk)
    {
        if (!migrateReadLine(slot.conn, line, ln))
        {
            fail("IOERR error or timeout reading from target instance");
            return;
        }
        if (ln == 0 || line[0] != '+')
        {
            // surface the target's error (e.g. BUSYKEY without REPLACE)
            repError(o, ln > 1 ? cast(string) line[1 .. ln].idup
                    : "ERR Target instance replied with error");
            return;
        }
    }

    // success: delete the migrated keys locally unless COPY, and log the DELs
    if (!copy)
    {
        static ByteBuffer delCmd; // TLS
        foreach (ki; 0 .. nk)
        {
            auto k = keybuf[ki];
            c.dbp.del(k);
            gWriteEpoch++;
            if (myAof().enabled)
            {
                delCmd.clear();
                repArrayHeader(delCmd, 2);
                repBulk(delCmd, "DEL");
                repBulk(delCmd, k);
                myAof().append(delCmd.data);
            }
        }
    }
    repSimple(o, "OK");
}

// ---------------------------------------------------------------------------
// Blocking commands (BLPOP family, XREAD BLOCK)
// ---------------------------------------------------------------------------

/// Redis timeouts are seconds as a double; 0 = block forever.
// Parse a blocking timeout (seconds, float). Returns null on success, else the
// exact Redis error: negative, out of range, or not-a-float.
private const(char)[] parseTimeout(scope const(char)[] s, out ulong ms) nothrow
{
    import dreads.commands : parseDouble;

    double secs;
    if (!parseDouble(s, secs))
        return "ERR timeout is not a float or out of range";
    if (secs < 0)
        return "ERR timeout is negative";
    if (secs > 1e9) // *1000 + mstime would overflow a long long (Redis)
        return "ERR timeout is out of range";
    ms = cast(ulong)(secs * 1000);
    return null;
}

/// True while the caller should keep waiting (updates the emit count).
// XREAD BLOCK is fan-out (all readers wake and read the same new entries — no
// hand-off), so it stays on the global broadcast event, not the per-key FIFO.
private bool waitForActivity(Conn* c, ref int ec, ref long remainingMs, ulong timeoutMs) nothrow
{
    import core.time : MonoTime, msecs;

    import dreads.obj : gBlockedClients;

    import core.atomic : atomicOp;

    if (timeoutMs != 0 && remainingMs <= 0)
        return false;
    // a synthetic remote-block conn is already counted by its requester's router
    immutable count = c is null || !c.remoteBlock;
    if (count)
        atomicOp!"+="(gBlockedClients, 1);
    scope (exit)
    {
        if (count)
            atomicOp!"-="(gBlockedClients, 1);
        if (c !is null) // restore this conn's reply protocol (see blockWait)
            gRespProto = c.resp3 ? 3 : 2;
    }
    // NB: a remote block (phase 2.5b) waits the FULL slice like a local one —
    // re-waking on a poll tick would RE-REGISTER on gKeyActivity each tick and
    // shuffle the event's FIFO waiter order (fairness: the first-blocked client
    // must be first to re-dispatch on a wake). A requester-side cancel wakes us
    // through ShardMsg.blockKick (the owner's drain emits gKeyActivity).
    auto slice = timeoutMs == 0 ? 3_600_000 : remainingMs;
    tKeyWaiters++; // gates the write-tail emit (see wakeKeyActivity)
    scope (exit)
        tKeyWaiters--;
    auto before = MonoTime.currTime;
    ec = gKeyActivity.waitUninterruptible(msecs(slice), ec);
    if (timeoutMs != 0)
        remainingMs -= (MonoTime.currTime - before).total!"msecs";
    return true;
}

// Refresh the deterministic clock to wall time. Blocking commands serve INLINE
// (not through dispatch, which freezes gClock per command), so after a wait real
// time has passed — the lazy-expiry check must see it, or a woken client would
// serve a key that expired while it waited (BZPOPMIN reprocessing).
private void refreshDetClock() @nogc nothrow
{
    import dreads.det : gClock;
    import dreads.stream : nowMs;

    gClock = nowMs();
}

// If a CLIENT UNBLOCK targeted this connection while it was parked, emit the
// unblock reply and return true (the caller returns). ERROR ⇒ -UNBLOCKED;
// TIMEOUT ⇒ the command's normal nil reply.
private bool handleUnblock(ref Conn c, ref ByteBuffer o, scope const(char)[] nilReply) nothrow
{
    if (c.unblockReq == 0)
        return false;
    immutable e = c.unblockReq;
    c.unblockReq = 0;
    if (e == 2)
        repError(o, "UNBLOCKED client unblocked via CLIENT UNBLOCK");
    else
        o.append(nilReply);
    return true;
}

// Lazily create the connection's single-shot wake event (reused each block).
private void ensureBlockEvt(Conn* c) nothrow
{
    if (!c.blockEvtInit)
    {
        c.blockEvt = createManualEvent();
        c.blockEvtInit = true;
    }
}

// Outcome of a blocking wait: the caller re-checks its keys on `ready`, replies
// nil on `timedOut`, and silently returns (peer gone) on `disconnected`.
private enum BlockWake : ubyte
{
    ready,
    timedOut,
    disconnected
}

// A parked blocked fiber isn't reading its socket, so a peer that vanishes while
// the fiber waits would never be noticed (the serve loop only sees EOF at
// waitForData). So the wait POLLS at a bounded interval and checks the connection
// each tick — a dead peer wakes the fiber, which returns `disconnected` and runs
// its own scope(exit) cleanup (decrementing gBlockedClients, unregistering). This
// is what stops a client that BLPOPs with an infinite timeout and then disconnects
// from leaking the blocked-client count forever.
private enum ulong BLOCK_POLL_MS = 100;

// Park the blocked fiber on its own event until signalKey wakes it, the timeout
// fires, or the peer disconnects. `ec` is snapshotted by the caller right before
// this call — there is no yield between waitRegister and here, so no producer can
// interleave (cooperative loop): no lost wakeup. Between polls the blocked-client
// count is held (the ++/-- brackets the WHOLE wait, not each poll tick), so a
// concurrent INFO never samples a transient dip.
private BlockWake blockWait(Conn* c, int ec, ref long remainingMs, ulong timeoutMs) nothrow
{
    import core.time : MonoTime, msecs;

    import dreads.obj : gBlockedClients;

    import core.atomic : atomicOp;

    if (timeoutMs != 0 && remainingMs <= 0)
        return BlockWake.timedOut;
    // a synthetic remote-block conn is already counted by its requester's router
    immutable count = !c.remoteBlock;
    if (count)
        atomicOp!"+="(gBlockedClients, 1); // INFO clients: parked in a blocking wait
    c.blocked = true; // eligible for CLIENT UNBLOCK while parked here
    scope (exit)
    {
        if (count)
            atomicOp!"-="(gBlockedClients, 1);
        c.blocked = false;
        // restore THIS conn's reply protocol: other fibers ran while we were
        // parked and reset the thread-global (RESP3 nil/double forms would
        // otherwise come out in the LAST client's protocol — also a latent
        // single-shard bug, not just a hop one)
        gRespProto = c.resp3 ? 3 : 2;
    }
    // Decrement the caller's `remainingMs` by ACTUAL elapsed per tick — never
    // recompute it from the original timeoutMs, or a re-block (caller re-enters
    // after a spurious wake) would reset the countdown (the BZPOPMIN
    // "reprocessing" contract: the timeout must survive across re-blocks).
    for (;;)
    {
        // slice = the poll tick, capped by any remaining finite timeout
        long slice = cast(long) BLOCK_POLL_MS;
        if (timeoutMs != 0 && remainingMs < slice)
            slice = remainingMs;
        immutable before = MonoTime.currTime;
        immutable n = c.blockEvt.waitUninterruptible(msecs(slice), ec);
        if (timeoutMs != 0)
            remainingMs -= (MonoTime.currTime - before).total!"msecs";
        if (n != ec) // genuine signal (emit count advanced) or CLIENT UNBLOCK
            return BlockWake.ready;
        if (c.unblockReq != 0)
            return BlockWake.ready;
        if (c.remoteBlock) // remote CLIENT UNBLOCK arrives via the pending's cancel
        {
            import core.atomic : atomicLoad;

            immutable k = atomicLoad(c.remotePend.cancel);
            if (k == 2 || k == 3)
            {
                c.unblockReq = k == 3 ? 2 : 1; // handleUnblock: 2 ⇒ -UNBLOCKED, else nil
                return BlockWake.ready;
            }
        }
        if (peerGone(c)) // peer vanished while we were parked (EOF probe)
            return BlockWake.disconnected;
        if (timeoutMs != 0 && remainingMs <= 0)
            return BlockWake.timedOut;
        // else: poll tick elapsed with no event — loop and wait again
    }
}

// The blocking-command serve switch, shared by executeCommand (a client whose
// keys live on its OWN shard — or single-shard mode) and remoteBlockServe (a
// hopped blocking command parked on this owner shard in its own fiber, phase
// 2.5b). Returns false when the form doesn't block (XREAD without BLOCK,
// XREADGROUP without `>`/BLOCK or inside MULTI) — the caller falls through to
// the plain one-shot dispatch path.
private bool serveBlockingSwitch(ref Conn c, scope const(char)[] uname,
        const ref RVal cmd, ref ByteBuffer o, ref Arena arena) nothrow
{
    auto args = cmd.arr[1 .. $];
    switch (uname)
    {
    case "BLPOP":
    case "BRPOP":
        {
            immutable eb = gTotalErrorReplies, ob = o.length;
            blockingPop(c, args, uname[1] == 'L', o, arena);
            statBlockingReply(uname, o, ob, eb); // count once when it finally returns
            return true;
        }
    case "BZPOPMIN":
    case "BZPOPMAX":
        {
            immutable eb = gTotalErrorReplies, ob = o.length;
            blockingZPop(c, args, uname == "BZPOPMAX", o, arena);
            statBlockingReply(uname, o, ob, eb);
            return true;
        }
    case "BLMOVE":
    case "BRPOPLPUSH":
        {
            // rewrite into the non-blocking form and retry until data/timeout
            if ((uname == "BLMOVE" && args.length != 5) || (uname == "BRPOPLPUSH"
                    && args.length != 3))
            {
                repError(o, "ERR wrong number of arguments");
                return true;
            }
            ulong timeoutMs;
            if (auto terr = parseTimeout(args[$ - 1].str, timeoutMs))
            {
                repError(o, terr);
                return true;
            }
            immutable eb = gTotalErrorReplies, ob = o.length;
            blockingRetry(c, cmd.arr[0 .. $ - 1], uname == "BLMOVE" ? "LMOVE"
                    : "RPOPLPUSH", "$-1\r\n", timeoutMs, o, arena);
            statBlockingReply(uname, o, ob, eb);
            return true;
        }
    case "XREAD":
        {
            // only the BLOCK form is handled here; plain XREAD dispatches
            ptrdiff_t blockAt = -1;
            foreach (i, ref a; args)
            {
                if (eqICDebug(a.str, "BLOCK"))
                {
                    blockAt = cast(ptrdiff_t) i;
                    break;
                }
            }
            if (blockAt < 0)
                return false;
            import dreads.commands : parseLong;

            long blockMs;
            if (blockAt + 1 >= args.length || !parseLong(args[blockAt + 1].str, blockMs)
                    || blockMs < 0)
            {
                repError(o, "ERR timeout is not an integer or out of range");
                return true;
            }
            xreadBlock(c, args, cast(size_t) blockAt, cast(ulong) blockMs, o, arena);
            return true;
        }
    case "XREADGROUP":
        {
            // Only the blocking form on a `>` id parks here; history reads
            // (explicit ids from the PEL), the non-BLOCK form, and MULTI/EXEC all
            // fall through to the normal dispatch/raft path (a single attempt).
            import dreads.commands : parseLong;

            ptrdiff_t blockAt = -1, streamsAt = -1;
            foreach (i, ref a; args)
            {
                if (blockAt < 0 && eqICDebug(a.str, "BLOCK"))
                    blockAt = cast(ptrdiff_t) i;
                else if (streamsAt < 0 && eqICDebug(a.str, "STREAMS"))
                    streamsAt = cast(ptrdiff_t) i;
            }
            // no BLOCK option (blockAt must precede STREAMS to be the keyword),
            // malformed, or inside a transaction ⇒ normal one-shot dispatch
            if (blockAt < 0 || streamsAt < 0 || blockAt > streamsAt
                    || c.inMulti || c.inExec)
                return false;
            auto after = args[streamsAt + 1 .. $];
            if (after.length == 0 || after.length % 2 != 0)
                return false; // let dispatch surface the syntax error
            auto half = after.length / 2;
            bool hasGt = false;
            foreach (ref idTok; after[half .. $])
                if (idTok.str == ">")
                {
                    hasGt = true;
                    break;
                }
            if (!hasGt)
                return false; // only `>` (new messages) can block; explicit ids read now
            long blockMs;
            if (blockAt + 1 >= cast(ptrdiff_t) args.length
                    || !parseLong(args[blockAt + 1].str, blockMs) || blockMs < 0)
            {
                repError(o, "ERR timeout is not an integer or out of range");
                return true;
            }
            immutable xrgErrPrev = gTotalErrorReplies;
            immutable xrgOutBefore = o.length;
            xreadgroupBlock(c, args, cast(size_t) blockAt, cast(ulong) blockMs, o, arena);
            statBlockingReply("xreadgroup", o, xrgOutBefore, xrgErrPrev);
            return true;
        }
    case "BLMPOP":
    case "BZMPOP":
        {
            // B*MPOP timeout numkeys key [key ...] WHERE -> *MPOP numkeys ...
            if (args.length < 4)
            {
                repError(o, uname == "BLMPOP"
                        ? "ERR wrong number of arguments for 'blmpop' command"
                        : "ERR wrong number of arguments for 'bzmpop' command");
                return true;
            }
            ulong timeoutMs;
            if (auto terr = parseTimeout(args[0].str, timeoutMs))
            {
                repError(o, terr);
                return true;
            }
            immutable eb = gTotalErrorReplies, ob = o.length;
            blockingRetry(c, cmd.arr[1 .. $], uname == "BLMPOP" ? "LMPOP" : "ZMPOP",
                    "*-1\r\n", timeoutMs, o, arena, true);
            statBlockingReply(uname, o, ob, eb);
            return true;
        }
    default:
        return false;
    }
}

/// BLPOP / BRPOP: keys..., timeout. Reply *2 [key, value] or nil array.
private void blockingPop(ref Conn c, const(RVal)[] args, bool fromLeft,
        ref ByteBuffer o, ref Arena arena) nothrow
{
    import dreads.obj : ObjType;

    if (args.length < 2)
    {
        repError(o, "ERR wrong number of arguments");
        return;
    }
    ulong timeoutMs;
    if (auto terr = parseTimeout(args[$ - 1].str, timeoutMs))
    {
        repError(o, terr);
        return;
    }
    auto keys = args[0 .. $ - 1];
    immutable db = c.dbp.db;
    long remaining = cast(long) timeoutMs;
    bool firstPass = true;
    bool registered = false;
    // On exit: invalidate this block's deque entries, THEN wake the next front of
    // any still-ready key (self is now stale so the signal advances to the next
    // waiter — this covers the served-key cascade AND the errored/timed-out case).
    scope (exit)
        if (registered)
        {
            waitFinish(&c);
            signalReadyKeys(db, *c.dbp);
        }
    for (;;)
    {
        refreshDetClock(); // real time passed while parked ⇒ observe expiries
        // re-check key ACL on every pass: a blocked client whose key permission
        // was revoked while it waited must be rejected when the command is
        // reprocessed on wake (Valkey behaviour). BLPOP/BRPOP need read+write.
        if (gAclActive && c.user !is null && !c.user.root.allKeys)
            foreach (ref k; keys)
                if (!aclCanAccessKey(c.user, k.str, true, true))
                {
                    statRejected(aclCmdIndex(fromLeft ? "blpop" : "brpop"));
                    aclLogViolation(c, "key", k.str, fromLeft ? "blpop" : "brpop");
                    repError(o, "NOPERM No permissions to access a key");
                    return;
                }
        foreach (ref k; keys)
        {
            bool wrong;
            auto obj = c.dbp.lookupTyped(k.str, ObjType.list, wrong);
            if (wrong)
            {
                // once blocked, a wrong-typed key never wakes the client
                if (firstPass)
                {
                    repError(o, "WRONGTYPE Operation against a key holding the wrong kind of value");
                    return;
                }
                continue;
            }
            if (obj is null || obj.list.length == 0)
                continue;
            if (keyHeldByOther(db, k.str, &c))
                continue; // FIFO: an earlier-blocked client serves this key first
            repArrayHeader(o, 2);
            repBulk(o, k.str);
            repBulk(o, fromLeft ? obj.list.front : obj.list.back);
            if (fromLeft)
                obj.list.popFront();
            else
                obj.list.popBack();
            c.dbp.delIfEmpty(k.str, obj);
            logEffect(fromLeft ? "LPOP" : "RPOP", k.str);
            return; // scope(exit) wakes the next front if the list still has data
        }
        firstPass = false;
        if (c.inMulti || c.inExec)
        {
            repNullArray(o);
            return;
        }
        if (!registered)
        {
            ensureBlockEvt(&c);
            waitRegister(db, keys, &c);
            registered = true;
        }
        flushBeforeBlock(c, o); // send replies to earlier pipelined cmds before parking
        immutable ec = c.blockEvt.emitCount; // no yield since register ⇒ no lost/spurious wake
        final switch (blockWait(&c, ec, remaining, timeoutMs))
        {
        case BlockWake.timedOut:
            repNullArray(o);
            return;
        case BlockWake.disconnected:
            return; // peer gone; scope(exit) unregisters + drops the blocked count
        case BlockWake.ready:
            break;
        }
        if (handleUnblock(c, o, gRespProto >= 3 ? "_\r\n" : "*-1\r\n"))
            return;
    }
}

/// BZPOPMIN / BZPOPMAX: keys..., timeout. Reply *3 [key, member, score] or nil.
private void blockingZPop(ref Conn c, const(RVal)[] args, bool popMax,
        ref ByteBuffer o, ref Arena arena) nothrow
{
    import dreads.commands : repDouble;
    import dreads.obj : ObjType;

    if (args.length < 2)
    {
        repError(o, "ERR wrong number of arguments");
        return;
    }
    ulong timeoutMs;
    if (auto terr = parseTimeout(args[$ - 1].str, timeoutMs))
    {
        repError(o, terr);
        return;
    }
    auto keys = args[0 .. $ - 1];
    immutable db = c.dbp.db;
    long remaining = cast(long) timeoutMs;
    bool firstPass = true;
    bool registered = false;
    scope (exit)
        if (registered)
        {
            waitFinish(&c);
            signalReadyKeys(db, *c.dbp);
        }
    for (;;)
    {
        refreshDetClock();
        // re-check key ACL on every pass (see blockingPop) — perms may have been
        // revoked while blocked. BZPOPMIN/MAX need read+write.
        if (gAclActive && c.user !is null && !c.user.root.allKeys)
            foreach (ref k; keys)
                if (!aclCanAccessKey(c.user, k.str, true, true))
                {
                    statRejected(aclCmdIndex(popMax ? "bzpopmax" : "bzpopmin"));
                    aclLogViolation(c, "key", k.str, popMax ? "bzpopmax" : "bzpopmin");
                    repError(o, "NOPERM No permissions to access a key");
                    return;
                }
        foreach (ref k; keys)
        {
            bool wrong;
            auto obj = c.dbp.lookupTyped(k.str, ObjType.zset, wrong);
            if (wrong)
            {
                // once blocked, a wrong-typed key never wakes the client
                if (firstPass)
                {
                    repError(o, "WRONGTYPE Operation against a key holding the wrong kind of value");
                    return;
                }
                continue;
            }
            if (obj is null || obj.zset.length == 0)
                continue;
            if (keyHeldByOther(db, k.str, &c))
                continue; // FIFO: an earlier-blocked client serves this key first
            const(char)[] victim;
            repArrayHeader(o, 3);
            repBulk(o, k.str);
            obj.zset.walkRange(0, 1, popMax, (m, s) {
                repBulk(o, m);
                repDouble(o, s);
                victim = arena.dupString(m);
                return 0;
            });
            obj.zset.remove(victim);
            c.dbp.delIfEmpty(k.str, obj);
            logEffect(popMax ? "ZPOPMAX" : "ZPOPMIN", k.str);
            return; // scope(exit) wakes the next front if the zset still has data
        }
        firstPass = false;
        if (c.inMulti || c.inExec)
        {
            repNullArray(o);
            return;
        }
        if (!registered)
        {
            ensureBlockEvt(&c);
            waitRegister(db, keys, &c);
            registered = true;
        }
        flushBeforeBlock(c, o); // send replies to earlier pipelined cmds before parking
        immutable ec = c.blockEvt.emitCount;
        final switch (blockWait(&c, ec, remaining, timeoutMs))
        {
        case BlockWake.timedOut:
            repNullArray(o);
            return;
        case BlockWake.disconnected:
            return; // peer gone; scope(exit) unregisters + drops the blocked count
        case BlockWake.ready:
            break;
        }
        if (handleUnblock(c, o, gRespProto >= 3 ? "_\r\n" : "*-1\r\n"))
            return;
    }
}

// Classify the blocking source keys: 1 = some key has servable data (right type
// + non-empty), -1 = none has data but some key is wrong-typed, 0 = all empty.
// Blocking commands only SERVE (dispatch) when the source has data; an empty
// source blocks regardless of the destination type (matching Redis) — so a
// wrong-typed dest never surfaces until there's something to move.
private int blockSourceState(ref Conn c, const(RVal)[] keys, ObjType t) nothrow
{
    bool wrongSeen = false;
    foreach (ref k; keys)
    {
        bool wrong;
        auto o = c.dbp.lookupTyped(k.str, t, wrong);
        if (wrong)
            wrongSeen = true;
        else if (o !is null && o.containerLen > 0)
            return 1;
    }
    return wrongSeen ? -1 : 0;
}

/// Generic retry loop: rewrites the blocking command into its non-blocking
/// form (parts = original tokens minus the timeout), dispatches it, and
/// waits when the reply equals nilReply. The effective command is what the
/// AOF sees (via the normal executeCommand path is bypassed here, so log it).
private void blockingRetry(ref Conn c, const(RVal)[] parts, string verb,
        string nilReply, ulong timeoutMs, ref ByteBuffer o, ref Arena arena,
        bool skipFirst = false) nothrow
{
    // NOT static: a blocked fiber yields while OTHER fibers run blockingRetry, and
    // a shared TLS buffer would be overwritten with another client's command — the
    // woken fiber would then re-parse someone else's rewritten command. Per-call.
    ByteBuffer synth; // rebuilt command bytes (must survive across the block's yields)
    ByteBuffer attempt; // dispatch reply staging
    synth.clear();
    auto argTokens = skipFirst ? parts[1 .. $] : parts[1 .. $];
    repArrayHeader(synth, 1 + argTokens.length);
    repBulk(synth, verb);
    foreach (ref p; argTokens)
        repBulk(synth, p.str);

    // the blocking key set: LMOVE/RPOPLPUSH block on the source (argTokens[0]);
    // LMPOP/ZMPOP block on the `numkeys` keys after the count (argTokens[1..1+N]).
    const(RVal)[] blockKeys;
    if (verb == "LMOVE" || verb == "RPOPLPUSH")
    {
        if (argTokens.length >= 1)
            blockKeys = argTokens[0 .. 1];
    }
    else
    {
        long nk;
        if (argTokens.length >= 2 && parseLong(argTokens[0].str, nk) && nk > 0
                && 1 + nk <= argTokens.length)
            blockKeys = argTokens[1 .. 1 + cast(size_t) nk];
    }

    import dreads.obj : ObjType;

    immutable srcType = verb == "ZMPOP" ? ObjType.zset : ObjType.list;
    immutable db = c.dbp.db;
    long remaining = cast(long) timeoutMs;
    bool firstPass = true;
    bool registered = false;
    scope (exit)
        if (registered)
        {
            waitFinish(&c);
            signalReadyKeys(db, *c.dbp);
        }
    // the rewritten command replies through the connection's protocol, so the
    // "nothing to serve" sentinel is `_` under RESP3
    auto nil = gRespProto >= 3 ? "_\r\n" : nilReply;
    for (;;)
    {
        refreshDetClock();
        attempt.clear();
        RVal cmd2;
        size_t pos = 0;
        if (parseValue(synth.data, pos, arena, cmd2) != ParseStatus.ok)
        {
            repError(o, "ERR internal blocking rewrite failed");
            return;
        }
        // re-check ACL on every pass — perms may have been revoked while blocked
        // (the woken command is reprocessed, so it must be re-validated).
        if (gAclActive && c.user !is null && aclDenies(c, cmd2, null, verb, o))
            return;
        // Dispatch on the FIRST attempt (to validate args / surface a wrong-typed
        // source) and whenever the source has data. An empty source blocks.
        immutable st = blockSourceState(c, blockKeys, srcType);
        // FIFO fairness (single-source BLMOVE/BRPOPLPUSH): if an earlier-blocked
        // client is queued ahead on the source, don't let this one steal the value
        // inline — queue behind it (see keyHeldByOther / blockingPop). Multi-key
        // BLMPOP/BZMPOP is left to dispatch's own first-with-data pick.
        immutable srcHeld = blockKeys.length == 1
            && keyHeldByOther(db, blockKeys[0].str, &c);
        if ((firstPass || st == 1) && !srcHeld)
        {
            dispatch(cmd2, *c.dbp, attempt, arena);
            propagationOverride.clear();
            auto rep = cast(const(char)[]) attempt.data;
            if (rep.length > 0 && rep[0] == '-')
            {
                // Surface real errors; but a WRONGTYPE with an EMPTY source is a
                // destination error we shouldn't raise yet — an empty source must
                // block regardless of the dest type (Redis). Surface a WRONGTYPE
                // only when the source has data (dst error, source intact via
                // lmove's dst-first check) or the source itself is wrong-typed on
                // the first attempt. Non-WRONGTYPE errors (bad numkeys/syntax) are
                // validation and always surface.
                immutable isWrongType = rep.length >= 10 && rep[1 .. 10] == "WRONGTYPE";
                if (!isWrongType || st == 1 || (firstPass && st == -1))
                {
                    o.append(attempt.data);
                    return;
                }
                rep = nil; // WRONGTYPE dst on an empty source ⇒ keep waiting
            }
            if (rep != nil)
            {
                o.append(attempt.data);
                if (myAof().enabled)
                    myAof().append(synth.data);
                gWriteEpoch++;
                wakeKeyActivity();
                return;
            }
        }
        firstPass = false;
        if (c.inMulti || c.inExec)
        {
            o.append(nil);
            return;
        }
        if (!registered)
        {
            ensureBlockEvt(&c);
            if (blockKeys.length)
                waitRegister(db, blockKeys, &c);
            registered = true;
        }
        flushBeforeBlock(c, o); // send replies to earlier pipelined cmds before parking
        immutable ec = c.blockEvt.emitCount;
        final switch (blockWait(&c, ec, remaining, timeoutMs))
        {
        case BlockWake.timedOut:
            o.append(nil);
            return;
        case BlockWake.disconnected:
            return; // peer gone; scope(exit) unregisters + drops the blocked count
        case BlockWake.ready:
            break;
        }
        if (handleUnblock(c, o, nil))
            return;
    }
}

/// XREAD ... BLOCK ms ... : strips BLOCK, resolves "$" to each stream's
/// current last id ONCE (Redis semantics), then retries until data/timeout.
private void xreadBlock(ref Conn c, const(RVal)[] args, size_t blockAt,
        ulong timeoutMs, ref ByteBuffer o, ref Arena arena) nothrow
{
    import core.stdc.stdio : snprintf;

    import dreads.obj : ObjType;

    // per-call (NOT static/TLS): a parked fiber yields in waitForActivity while
    // OTHER blocked fibers on this thread run this same function — a shared TLS
    // buffer would be clobbered and the woken fiber would re-parse someone
    // else's rewritten command (same rule blockingRetry documents).
    ByteBuffer synth;
    synth.clear();
    // locate STREAMS to know where ids start
    ptrdiff_t streamsAt = -1;
    foreach (i, ref a; args)
    {
        if (eqICDebug(a.str, "STREAMS"))
        {
            streamsAt = cast(ptrdiff_t) i;
            break;
        }
    }
    if (streamsAt < 0 || (args.length - streamsAt - 1) % 2 != 0)
    {
        repError(o, "ERR syntax error");
        return;
    }
    auto half = (args.length - streamsAt - 1) / 2;

    // count synth tokens: original minus the BLOCK pair, plus the verb
    repArrayHeader(synth, args.length - 2 + 1);
    repBulk(synth, "XREAD");
    foreach (i, ref a; args)
    {
        if (i == blockAt || i == blockAt + 1)
            continue;
        bool isIdSlot = i > cast(size_t) streamsAt + half;
        if (isIdSlot && a.str == "$")
        {
            // resolve to the stream's current last id
            auto keyIdx = i - half;
            bool wrong;
            auto obj = c.dbp.lookupTyped(args[keyIdx].str, ObjType.stream, wrong);
            char[48] b = void;
            auto ms = obj is null ? 0 : obj.stream.lastId.ms;
            auto seq = obj is null ? 0 : obj.stream.lastId.seq;
            auto n = snprintf(b.ptr, b.length, "%llu-%llu", ms, seq);
            repBulk(synth, b[0 .. n]);
        }
        else
            repBulk(synth, a.str);
    }

    ByteBuffer attempt; // per-call, same rule as `synth` above
    auto ec = gKeyActivity.emitCount;
    long remaining = cast(long) timeoutMs;
    bool firstPass = true;
    immutable xnil = gRespProto >= 3 ? "_\r\n" : "*-1\r\n"; // XREAD nil per protocol
    for (;;)
    {
        // remote block (phase 2.5b): the requester may have cancelled while we
        // waited — disconnect ⇒ empty reply (discarded), UNBLOCK ⇒ nil/-UNBLOCKED
        if (c.remoteBlock && remoteBlockCancelled(c, o, xnil))
            return;
        attempt.clear();
        RVal cmd2;
        size_t pos = 0;
        if (parseValue(synth.data, pos, arena, cmd2) != ParseStatus.ok)
        {
            repError(o, "ERR internal blocking rewrite failed");
            return;
        }
        // re-check ACL on every pass — perms may have been revoked while blocked.
        if (gAclActive && c.user !is null && aclDenies(c, cmd2, null, "xread", o))
            return;
        dispatch(cmd2, *c.dbp, attempt, arena);
        auto rep = cast(const(char)[]) attempt.data;
        auto nil = xnil;
        // A WRONGTYPE that appears only AFTER blocking means the key changed type
        // while we waited (XADD then DEL then LPUSH): keep waiting, don't wake with
        // an error. On the first attempt a wrong-typed key is a real error.
        immutable isWrongType = rep.length >= 10 && rep[1 .. 10] == "WRONGTYPE";
        if (rep != nil && !(isWrongType && !firstPass))
        {
            o.append(attempt.data);
            return;
        }
        firstPass = false;
        if (c.inMulti || c.inExec || !waitForActivity(&c, ec, remaining, timeoutMs))
        {
            o.append(nil);
            return;
        }
    }
}

// A remote block's cancel check (phase 2.5b), shared by the XREAD/XREADGROUP
// retry loops (the pop family handles cancel inside blockWait/peerGone). True ⇒
// the caller must return: disconnect (1) leaves `o` untouched (an empty reply
// completes the pending handshake; the requester's peer is gone), CLIENT
// UNBLOCK maps to the command's nil (2) or -UNBLOCKED (3).
private bool remoteBlockCancelled(ref Conn c, ref ByteBuffer o, scope const(char)[] nilReply) nothrow
{
    import core.atomic : atomicLoad;

    immutable k = atomicLoad(c.remotePend.cancel);
    if (k == 0)
        return false;
    if (k == 2)
        o.append(nilReply);
    else if (k == 3)
        repError(o, "UNBLOCKED client unblocked via CLIENT UNBLOCK");
    return true;
}

/// Count a blocking command's final reply into INFO commandstats/errorstats —
/// blocking serves happen in the server layer and bypass executeCommand's
/// accounting, so a blocked XREADGROUP that wakes with -NOGROUP would otherwise
/// not register (calls/failed_calls/total_error_replies). Mirrors that path.
private void statBlockingReply(scope const(char)[] uname, ref ByteBuffer o,
        size_t outBefore, ulong errPrev) nothrow
{
    immutable errored = o.length > outBefore && o.data[outBefore] == '-';
    if (errored && gTotalErrorReplies == errPrev)
        statErrorReply(cast(const(char)[]) o.data[outBefore .. $]);
    char[16] lc = void; // aclCmdIndex wants the lowercase name
    immutable n = uname.length <= lc.length ? uname.length : lc.length;
    foreach (i; 0 .. n)
    {
        immutable ch = uname[i];
        lc[i] = ch >= 'A' && ch <= 'Z' ? cast(char)(ch + 32) : ch;
    }
    statCall(aclCmdIndex(cast(const(char)[]) lc[0 .. n]), errored);
}

/// XREADGROUP ... BLOCK ms ... >  : strips the BLOCK pair and retries the group
/// read until a `>` delivers (or timeout). Fan-out like XREAD (all group members
/// wake on any XADD via gKeyActivity — the group's lastDelivered cursor serializes
/// who gets which entry, so no per-key FIFO hand-off is needed). Unlike XREAD it
/// is a WRITE — a served pass advances the cursor and registers the PEL, so it is
/// logged (rewritten without BLOCK, exactly what the non-blocking path logs).
private void xreadgroupBlock(ref Conn c, const(RVal)[] args, size_t blockAt,
        ulong timeoutMs, ref ByteBuffer o, ref Arena arena) nothrow
{
    // per-call (not TLS): the fiber yields across the block while other fibers
    // run this same function — a shared buffer would be clobbered.
    ByteBuffer synth;
    synth.clear();
    // rewrite XREADGROUP without the BLOCK pair; `>` is kept verbatim (the group
    // cursor, not a resolvable id like XREAD's `$`).
    repArrayHeader(synth, args.length - 2 + 1);
    repBulk(synth, "XREADGROUP");
    foreach (i, ref a; args)
    {
        if (i == blockAt || i == blockAt + 1)
            continue;
        repBulk(synth, a.str);
    }

    ByteBuffer attempt;
    auto ec = gKeyActivity.emitCount;
    long remaining = cast(long) timeoutMs;
    immutable nil = gRespProto >= 3 ? "_\r\n" : "*-1\r\n"; // XREADGROUP empty reply
    for (;;)
    {
        // remote block (phase 2.5b): requester cancelled while we waited
        if (c.remoteBlock && remoteBlockCancelled(c, o, nil))
            return;
        attempt.clear();
        RVal cmd2;
        size_t pos = 0;
        if (parseValue(synth.data, pos, arena, cmd2) != ParseStatus.ok)
        {
            repError(o, "ERR internal blocking rewrite failed");
            return;
        }
        // re-check ACL every pass — perms may have been revoked while blocked
        if (gAclActive && c.user !is null && aclDenies(c, cmd2, null, "xreadgroup", o))
            return;
        dispatch(cmd2, *c.dbp, attempt, arena);
        propagationOverride.clear();
        auto rep = cast(const(char)[]) attempt.data;
        // a real error (NOGROUP, WRONGTYPE, bad id) surfaces immediately
        if (rep.length > 0 && rep[0] == '-')
        {
            o.append(attempt.data);
            return;
        }
        if (rep != nil)
        {
            // served: a `>` delivery advanced the cursor + PEL — reply, log, wake
            o.append(attempt.data);
            if (myAof().enabled)
                myAof().append(synth.data);
            gWriteEpoch++;
            wakeKeyActivity();
            return;
        }
        if (!waitForActivity(&c, ec, remaining, timeoutMs))
        {
            o.append(nil);
            return;
        }
    }
}

/// Logs a single-key effect command (LPOP key / ZPOPMIN key ...) to the AOF.
private void logEffect(string verb, scope const(char)[] key) nothrow
{
    gWriteEpoch++;
    wakeKeyActivity();
    if (!myAof().enabled)
        return;
    static ByteBuffer eff; // TLS
    eff.clear();
    repArrayHeader(eff, 2);
    repBulk(eff, verb);
    repBulk(eff, key);
    myAof().append(eff.data);
}

private void unregisterMonitor(Conn* c) nothrow
{
    gMonitors.remove(connIdKey(c.id));
}

/// MONITOR feed line: +<unix>.<usec> [0 ?] "CMD" "arg" ...
private void feedMonitors(ref Conn from, const ref RVal cmd) nothrow
{
    import core.stdc.stdio : snprintf;

    import dreads.stream : nowMs;

    static ByteBuffer line; // TLS; single-threaded event loop
    line.clear();
    auto ms = nowMs();
    char[64] hdr = void;
    auto n = snprintf(hdr.ptr, hdr.length, "+%llu.%03llu000 [0 client:%llu]",
            ms / 1000, ms % 1000, from.id);
    line.append(hdr[0 .. n]);
    foreach (ref a; cmd.arr)
    {
        line.append(` "`);
        // sdscatrepr: named escapes for the common controls, \xHH for the rest,
        // everything else printable verbatim (matches Redis MONITOR quoting).
        foreach (ch; a.str)
        {
            switch (ch)
            {
            case '\\':
                line.append(`\\`);
                break;
            case '"':
                line.append(`\"`);
                break;
            case '\n':
                line.append(`\n`);
                break;
            case '\r':
                line.append(`\r`);
                break;
            case '\t':
                line.append(`\t`);
                break;
            case '\a':
                line.append(`\a`);
                break;
            case '\b':
                line.append(`\b`);
                break;
            default:
                if (ch >= 0x20 && ch < 0x7f)
                    line.appendByte(ch);
                else
                {
                    char[8] hx = void;
                    auto hn = snprintf(hx.ptr, hx.length, "\\x%02x",
                            cast(uint)(cast(ubyte) ch));
                    line.append(hx[0 .. hn]);
                }
                break;
            }
        }
        line.appendByte('"');
    }
    line.append("\r\n");
    auto m = rcFromBytes(line.data); // encode once, share across monitors
    // Resolve each monitor id to a STRONG lock so it can't be freed under the
    // delivery; a monitor that died is simply skipped. Index iteration (not the
    // @nogc opApply — connSink isn't @nogc); feedMonitors never yields, so walking
    // the table directly is safe (no concurrent mutation).
    foreach (i; 0 .. gMonitors.capacity)
    {
        if (!gMonitors.slotLive(i))
            continue;
        auto key = gMonitors.keyAt(i);
        if (key.length != ulong.sizeof)
            continue;
        immutable id = *cast(const(ulong)*) key.ptr;
        if (id == from.id)
            continue; // a monitor never sees its own command
        auto s = connById(id);
        if (s.isNull)
            continue;
        connSink(cast(void*)&s.get(), m);
    }
    rcRelease(m);
}

/// CLIENT GETNAME/SETNAME/ID/INFO/NO-EVICT/NO-TOUCH/LIST (minimal).
// One CLIENT LIST / CLIENT INFO line for a connection (newline-terminated).
// CLIENT SETINFO <lib-name|lib-ver> <value> — attach a client library identity
// to this connection (surfaced in CLIENT INFO/LIST). The value may not contain
// spaces, newlines or other control characters; RESET does NOT clear it.
private void clientSetInfo(ref Conn c, const(RVal)[] args, ref ByteBuffer o) nothrow
{
    if (args.length != 2)
    {
        repError(o, "ERR wrong number of arguments for 'client|setinfo' command");
        return;
    }
    auto attr = args[0].str;
    auto val = args[1].str;
    foreach (ch; val)
    {
        if (ch == ' ' || ch == '\n' || ch == '\r' || ch < 0x21 || ch == 0x7f)
        {
            repError(o,
                    "ERR lib-name/lib-ver cannot contain spaces, newlines or special characters.");
            return;
        }
    }
    if (eqICDebug(attr, "lib-name"))
    {
        c.libName.clear();
        c.libName.put(val);
    }
    else if (eqICDebug(attr, "lib-ver"))
    {
        c.libVer.clear();
        c.libVer.put(val);
    }
    else
    {
        repError(o, "ERR Unrecognized option");
        return;
    }
    repSimple(o, "OK");
}

private void appendConnInfo(Conn* c, ref ByteBuffer o) nothrow
{
    import core.stdc.stdio : snprintf;

    // Build cmd= lazily (this runs only on CLIENT LIST/INFO, never the hot path):
    // a container command renders as `container|subcommand`.
    char[65] cmdBuf = void;
    const(char)[] lastCmd = "NULL";
    if (c.lastCmdLen)
    {
        auto name = cast(const(char)[]) c.lastCmdBuf[0 .. c.lastCmdLen];
        if (c.lastArgLen && aclIsContainer(name))
        {
            size_t k;
            foreach (ch; name)
                cmdBuf[k++] = ch;
            cmdBuf[k++] = '|';
            foreach (ch; c.lastArgBuf[0 .. c.lastArgLen])
                cmdBuf[k++] = ch;
            lastCmd = cast(const(char)[]) cmdBuf[0 .. k];
        }
        else
            lastCmd = name;
    }
    auto addr = c.addr.length ? cast(const(char)[]) c.addr[] : "?";
    auto laddr = c.laddr.length ? cast(const(char)[]) c.laddr[] : "?";
    auto ln = c.libName.length ? cast(const(char)[]) c.libName[] : "";
    auto lv = c.libVer.length ? cast(const(char)[]) c.libVer[] : "";
    immutable now = nowMs();
    long age = c.connMs ? (now - c.connMs) / 1000 : 0;
    long idle = c.lastActiveMs ? (now - c.lastActiveMs) / 1000 : 0;
    if (idle < 0)
        idle = 0;
    // redir: the tracking redirection target id, or -1 when there is none.
    long redir = (c.tracking && c.trackRedir) ? cast(long) c.trackRedir : -1;
    char[8] fbuf = void;
    auto pf = connFlags(*c, fbuf[]);
    char[9] flagsz = void; // null-terminated for %s
    foreach (k, ch; pf)
        flagsz[k] = ch;
    flagsz[pf.length] = '\0';
    // Per-connection memory/buffer accounting (qbuf*, argv-mem, rbs, obl/oll/omem,
    // tot-mem) is NOT tracked — the input/output buffers live on the serve fiber's
    // stack, not on Conn. Report 0 (honest "not measured") rather than a fabricated
    // plausible number; the introspection globs accept it, and a monitoring client
    // reads 0 as unpopulated instead of trusting a made-up size. fd is likewise not
    // the real socket fd (vibe abstracts it) — report the connection id as a stable,
    // unique per-client handle.
    char[512] b = void;
    auto n = snprintf(b.ptr, b.length,
            "id=%llu addr=%.*s laddr=%.*s fd=%llu name=%.*s age=%lld idle=%lld flags=%s"
            ~ " capa=%s db=%d sub=%d psub=%d ssub=%d multi=%d watch=%d qbuf=0 qbuf-free=0"
            ~ " argv-mem=0 multi-mem=0 rbs=0 rbp=0 obl=0 oll=0 omem=0 tot-mem=0"
            ~ " events=r cmd=%.*s user=%.*s redir=%lld resp=%d lib-name=%.*s lib-ver=%.*s"
            ~ " tot-net-in=%llu tot-net-out=%llu tot-cmds=%llu\n",
            c.id, cast(int) addr.length, addr.ptr, cast(int) laddr.length, laddr.ptr,
            c.id, cast(int) c.clientName.length, c.clientName[].ptr,
            age, idle, flagsz.ptr,
            c.capaRedirect ? "r".ptr : "".ptr,
            c.dbp.db, cast(int) c.sub.channels.length, cast(int) c.sub.patterns.length,
            cast(int) c.shardSub.channels.length,
            c.inMulti ? cast(int) c.multiCount : -1, c.watching ? 1 : 0,
            cast(int) lastCmd.length, lastCmd.ptr,
            cast(int)(c.user !is null ? c.user.name.length : 7),
            c.user !is null ? c.user.name.ptr : "default".ptr,
            redir, c.resp3 ? 3 : 2,
            cast(int) ln.length, ln.ptr, cast(int) lv.length, lv.ptr,
            c.totNetIn, c.totNetOut, c.totCmds);
    if (n > 0)
        o.append(b[0 .. n]);
}

private bool clientCmd(ref Conn c, const(RVal)[] args, ref ByteBuffer o) nothrow
{
    import core.stdc.stdio : snprintf;

    if (args.length == 0)
    {
        repError(o, "ERR wrong number of arguments for 'client' command");
        return true;
    }
    auto sub = args[0].str;
    if (eqICDebug(sub, "ID"))
        repInt(o, cast(long) c.id);
    else if (eqICDebug(sub, "GETNAME"))
        repBulk(o, c.clientName[]);
    else if (eqICDebug(sub, "SETNAME") && args.length == 2)
    {
        foreach (ch; args[1].str)
        {
            if (ch == ' ' || ch == '\n' || ch == '\r')
            {
                repError(o, "ERR Client names cannot contain spaces, newlines or special characters.");
                return true;
            }
        }
        c.clientName.clear();
        c.clientName.put(args[1].str);
        repSimple(o, "OK");
    }
    else if (eqICDebug(sub, "INFO"))
    {
        static ByteBuffer lb; // TLS
        lb.clear();
        appendConnInfo(&c, lb);
        repBulk(o, cast(const(char)[]) lb.data);
    }
    else if (eqICDebug(sub, "LIST"))
        return clientList(c, args[1 .. $], o);
    else if (eqICDebug(sub, "KILL"))
        return clientKill(c, args[1 .. $], o);
    else if (eqICDebug(sub, "UNBLOCK"))
    {
        // CLIENT UNBLOCK <id> [TIMEOUT|ERROR] — wake a client parked in a
        // blocking command: TIMEOUT (default) replies as if it timed out, ERROR
        // replies -UNBLOCKED. :1 if it was blocked and got unblocked, else :0.
        if (args.length < 2 || args.length > 3)
        {
            repError(o, "ERR wrong number of arguments for 'client|unblock' command");
            return true;
        }
        long id;
        if (!parseLong(args[1].str, id) || id < 0)
        {
            repError(o, "ERR value is not an integer or out of range");
            return true;
        }
        ubyte mode = 1; // TIMEOUT
        if (args.length == 3)
        {
            if (eqICDebug(args[2].str, "ERROR"))
                mode = 2;
            else if (!eqICDebug(args[2].str, "TIMEOUT"))
            {
                repError(o, "ERR syntax error");
                return true;
            }
        }
        // A CLIENT PAUSE holds blocked clients in place — unblocking one would let
        // it proceed past the barrier, so CLIENT UNBLOCK is a no-op (returns 0)
        // while a window is open; it works again after UNPAUSE.
        immutable paused = gPauseUntilMs != 0 && nowMs() < gPauseUntilMs;
        long unblocked = 0;
        if (!paused)
        {
            auto s = connById(cast(ulong) id); // O(1) id lookup -> strong lock
            auto p = s.isNull ? null : &s.get();
            if (p !is null && p !is &c && p.blocked)
            {
                p.unblockReq = mode;
                if (p.blockEvtInit)
                    p.blockEvt.emit(); // wake it; the block loop honours unblockReq
                unblocked = 1;
            }
        }
        repInt(o, unblocked);
        return true;
    }
    else if (eqICDebug(sub, "IMPORT-SOURCE") && args.length == 2)
    {
        import dreads.obj : gImportMode;

        if (eqICDebug(args[1].str, "ON"))
        {
            if (!gImportMode)
                repError(o, "ERR Server is not in import mode");
            else
            {
                c.importSource = true;
                repSimple(o, "OK");
            }
        }
        else if (eqICDebug(args[1].str, "OFF"))
        {
            c.importSource = false;
            repSimple(o, "OK");
        }
        else
            repError(o, "ERR syntax error");
    }
    else if (eqICDebug(sub, "PAUSE") && (args.length == 2 || args.length == 3))
    {
        import dreads.stream : nowMs;

        long ms;
        if (!parseLong(args[1].str, ms))
            repError(o, "ERR timeout is not an integer or out of range");
        else if (ms < 0)
            repError(o, "ERR timeout is negative");
        else
        {
            bool all = true; // default ALL (matches Valkey)
            bool badMode;
            if (args.length == 3)
            {
                if (eqICDebug(args[2].str, "WRITE"))
                    all = false;
                else if (eqICDebug(args[2].str, "ALL"))
                    all = true;
                else
                    badMode = true;
            }
            if (badMode)
                repError(o, "ERR CLIENT PAUSE mode must be WRITE or ALL");
            else
            {
                immutable now = nowMs();
                immutable newEnd = now + cast(ulong) ms;
                if (gReplaying)
                {
                    // A drain is in progress on some connection's fiber (we got here
                    // via its IO yield). Defer: stacking against whatever is already
                    // pending, applied when the drain finishes (see replayPaused).
                    gPausePendingAll = all || (gPausePending && gPausePendingAll);
                    if (!gPausePending || newEnd > gPausePendingEnd)
                        gPausePendingEnd = newEnd;
                    gPausePendingIssuer = c.id;
                    gPausePending = true;
                }
                else
                {
                    // Stacking (Valkey pauseClientsByClient): keep the HIGHER end-time
                    // and the MOST RESTRICTIVE action. A new WRITE pause can't
                    // downgrade an ALL pause still in force, and a shorter timeout
                    // can't cut a longer one — the two overlap as the strictest.
                    immutable active = gPauseUntilMs > now;
                    gPauseAll = all || (active && gPauseAll); // ALL wins while it lasts
                    if (!active || newEnd > gPauseUntilMs)
                        gPauseUntilMs = newEnd; // never shorten a running window
                    gPauseIssuer = c.id; // the pauser's own connection is exempt
                    cast(void) gPauseEvt.emit(); // re-arm any fiber parked on a prior window
                }
                repSimple(o, "OK");
            }
        }
    }
    else if (eqICDebug(sub, "UNPAUSE") && args.length == 1)
    {
        gPauseUntilMs = 0; // lift the barrier
        cast(void) gPauseEvt.emit(); // wake parked fibers to replay their held commands
        repSimple(o, "OK");
    }
    else if (eqICDebug(sub, "NO-EVICT") || eqICDebug(sub, "NO-TOUCH"))
    {
        // both take ON|OFF; no-op semantically but the argument is validated.
        if (args.length == 2 && (eqICDebug(args[1].str, "ON") || eqICDebug(args[1].str, "OFF")))
            repSimple(o, "OK");
        else
            repError(o, "ERR syntax error");
    }
    else if (eqICDebug(sub, "SETINFO"))
        clientSetInfo(c, args[1 .. $], o);
    else if (eqICDebug(sub, "CAPA"))
    {
        // CLIENT CAPA <cap> [cap ...] — advertise client capabilities. Only
        // `redirect` is modelled (surfaces as capa=r); unknown caps are ignored.
        foreach (ref a; args[1 .. $])
            if (eqICDebug(a.str, "REDIRECT"))
                c.capaRedirect = true;
        repSimple(o, "OK");
    }
    else if (eqICDebug(sub, "REPLY") && args.length == 2)
    {
        // CLIENT REPLY ON|OFF|SKIP — control reply delivery for this connection.
        // The REPLY command itself is exempt from suppression (see handleCommand).
        c.replyCmdExempt = true;
        if (eqICDebug(args[1].str, "ON"))
        {
            c.replyOff = false;
            c.replySkipNext = false;
            repSimple(o, "OK");
        }
        else if (eqICDebug(args[1].str, "OFF"))
            c.replyOff = true; // silent from here until ON
        else if (eqICDebug(args[1].str, "SKIP"))
        {
            if (!c.replyOff)
                c.replySkipNext = true; // silence the next command's reply
        }
        else
            repError(o, "ERR syntax error");
    }
    else if (eqICDebug(sub, "TRACKING") && args.length >= 2)
        clientTracking(c, args[1 .. $], o);
    else if (eqICDebug(sub, "CACHING"))
    {
        if (args.length != 2)
            repError(o, "ERR wrong number of arguments for 'client|caching' command");
        else
            clientCaching(c, args[1].str, o);
    }
    else if (eqICDebug(sub, "GETREDIR") && args.length == 1)
        repInt(o, c.tracking ? cast(long) c.trackRedir : -1);
    else if (eqICDebug(sub, "TRACKINGINFO") && args.length == 1)
        clientTrackingInfo(c, o);
    else if (eqICDebug(sub, "HELP"))
        repHelp!"CLIENT"(o);
    else
        repUnknownSubcommand(o, "CLIENT", sub);
    return true;
}

// CLIENT TRACKING <ON|OFF> [REDIRECT id] [PREFIX p ...] [BCAST] [OPTIN] [OPTOUT]
// [NOLOOP] — enable/disable client-side caching invalidation for this connection.
private void clientTracking(ref Conn c, const(RVal)[] opts, ref ByteBuffer o) nothrow
{
    bool on;
    if (eqICDebug(opts[0].str, "ON"))
        on = true;
    else if (eqICDebug(opts[0].str, "OFF"))
        on = false;
    else
    {
        repError(o, "ERR syntax error");
        return;
    }
    bool bcast, optin, optout, noloop, haveRedir;
    long redir = 0;
    Dict!Unit prefixes; // parsed into a scratch, only applied on success (its
    // ~this reclaims it at scope exit — no manual teardown)
    size_t i = 1;
    while (i < opts.length)
    {
        auto a = opts[i].str;
        if (eqICDebug(a, "REDIRECT") && i + 1 < opts.length)
        {
            if (!parseLong(opts[i + 1].str, redir) || redir < 0)
            {
                repError(o, "ERR Invalid client ID");
                return;
            }
            haveRedir = true;
            i += 2;
        }
        else if (eqICDebug(a, "PREFIX") && i + 1 < opts.length)
        {
            prefixes.set(opts[i + 1].str, Unit());
            i += 2;
        }
        else if (eqICDebug(a, "BCAST"))
        {
            bcast = true;
            i++;
        }
        else if (eqICDebug(a, "OPTIN"))
        {
            optin = true;
            i++;
        }
        else if (eqICDebug(a, "OPTOUT"))
        {
            optout = true;
            i++;
        }
        else if (eqICDebug(a, "NOLOOP"))
        {
            noloop = true;
            i++;
        }
        else
        {
            repError(o, "ERR syntax error");
            return;
        }
    }
    if (optin && optout)
    {
        repError(o, "ERR You can't specify both OPTIN mode and OPTOUT mode");
        return;
    }
    if (prefixes.length && !bcast)
    {
        repError(o, "ERR PREFIX option requires BCAST mode to be enabled");
        return;
    }
    if (bcast && (optin || optout))
    {
        repError(o, "ERR OPTIN and OPTOUT are not compatible with BCAST");
        return;
    }
    if (!on)
    {
        trackDisable(c);
        repSimple(o, "OK");
        return;
    }
    if (haveRedir && redir != 0 && connById(cast(ulong) redir).isNull)
    {
        repError(o, "ERR The client ID you want redirect to does not exist");
        return;
    }
    // Can't flip OPTIN<->OPTOUT without disabling tracking first (Valkey guard).
    // Enabling on RESP2 without a redirect is allowed but inert: nothing is
    // delivered until the client switches to RESP3 (which engages async output).
    if (c.tracking && ((c.trackOptin && optout) || (c.trackOptout && optin)))
    {
        repError(o, "ERR You can't switch OPTIN/OPTOUT mode before disabling "
                ~ "tracking for this client, and then re-enabling it with a different mode.");
        return;
    }
    immutable wasTracking = c.tracking;
    c.tracking = true;
    c.trackBcast = bcast;
    c.trackOptin = optin;
    c.trackOptout = optout;
    c.trackNoloop = noloop;
    c.trackRedir = haveRedir ? cast(ulong) redir : 0;
    c.trackRedirBroken = false;
    c.trackPrefixes.free();
    foreach (p, ref _; prefixes)
        c.trackPrefixes.set(p, Unit());
    if (bcast)
        gBcastConns.set(connIdKey(c.id), Unit());
    else
        gBcastConns.remove(connIdKey(c.id));
    // RESP3 self-tracking pushes arrive from other fibers, so its output must be
    // the async (oq) path; a redirect target already engaged it by subscribing.
    if (c.trackRedir == 0 && c.resp3)
        enterSubMode(c);
    if (!wasTracking)
        gTrackCount++;
    repSimple(o, "OK");
}

// CLIENT CACHING <YES|NO> — arm the one-shot per-command caching toggle for the
// next read (only meaningful in OPTIN/OPTOUT mode).
private void clientCaching(ref Conn c, scope const(char)[] yn, ref ByteBuffer o) nothrow
{
    if (!c.tracking || !(c.trackOptin || c.trackOptout))
    {
        repError(o, "ERR CLIENT CACHING can be called only when the client is in "
                ~ "tracking mode with OPTIN or OPTOUT mode enabled");
        return;
    }
    if (eqICDebug(yn, "YES"))
    {
        if (c.trackOptout)
        {
            repError(o, "ERR CLIENT CACHING YES is only valid when tracking is enabled in OPTIN mode.");
            return;
        }
        c.trackCachingYes = true;
    }
    else if (eqICDebug(yn, "NO"))
    {
        if (c.trackOptin)
        {
            repError(o, "ERR CLIENT CACHING NO is only valid when tracking is enabled in OPTOUT mode.");
            return;
        }
        c.trackCachingYes = true; // one-shot exception armed (mode decides its sense)
    }
    else
    {
        repError(o, "ERR syntax error");
        return;
    }
    repSimple(o, "OK");
}

// CLIENT TRACKINGINFO — a map of this connection's tracking state.
private void clientTrackingInfo(ref Conn c, ref ByteBuffer o) nothrow @trusted
{
    repMapHeader(o, 3);
    // 1) flags — collect the active flag names into a small stack list
    repBulk(o, "flags");
    size_t nf = 0;
    const(char)[][8] flags;
    if (!c.tracking)
        flags[nf++] = "off";
    else
    {
        flags[nf++] = "on";
        if (c.trackBcast)
            flags[nf++] = "bcast";
        if (c.trackOptin)
            flags[nf++] = "optin";
        if (c.trackOptout)
            flags[nf++] = "optout";
        if (c.trackCachingYes && c.trackOptin)
            flags[nf++] = "caching-yes";
        if (c.trackCachingYes && c.trackOptout)
            flags[nf++] = "caching-no";
        if (c.trackNoloop)
            flags[nf++] = "noloop";
        if (c.trackRedirBroken)
            flags[nf++] = "broken_redirect";
    }
    repArrayHeader(o, nf);
    foreach (k; 0 .. nf)
        repBulk(o, flags[k]);
    // 2) redirect
    repBulk(o, "redirect");
    repInt(o, c.tracking ? cast(long) c.trackRedir : -1);
    // 3) prefixes
    repBulk(o, "prefixes");
    repArrayHeader(o, c.trackPrefixes.length);
    foreach (p, ref _; c.trackPrefixes)
        repBulk(o, p);
}

// CLIENT KILL — the operational lever to sever a rogue user/connection so it
// can't take the server down with it (see [[acl-script-enforcement]]). Two forms:
//   CLIENT KILL <addr:port>                      (legacy: +OK / -No such client)
//   CLIENT KILL <FILTER value>...                (new: reply = count killed)
// Filters: ID <id>, USER <name>, ADDR/LADDR <ip:port>, TYPE <t>, SKIPME yes|no
// (default yes). ADDR/LADDR match the peer/local address captured at connect;
// TYPE/MAXAGE are accepted but unmodelled (match nothing). Returns false only when
// the CALLER killed itself (SKIPME no) so the read loop closes it AFTER the reply.
// Client type as CLIENT LIST/KILL's TYPE/NOT-TYPE filter sees it. dreads has no
// replication clients (raft replaces it), so only NORMAL and PUBSUB ever occur;
// MASTER/REPLICA are recognized names that simply match no live connection.
private enum : int
{
    CT_NORMAL = 0,
    CT_REPLICA = 1,
    CT_MASTER = 2,
    CT_PUBSUB = 3,
}

private int connType(ref Conn p) @nogc nothrow
{
    return (p.sub.subCount > 0 || p.shardSub.subCount > 0) ? CT_PUBSUB : CT_NORMAL;
}

private int parseClientType(scope const(char)[] v) @nogc nothrow
{
    if (eqICDebug(v, "NORMAL"))
        return CT_NORMAL;
    if (eqICDebug(v, "MASTER"))
        return CT_MASTER;
    if (eqICDebug(v, "REPLICA") || eqICDebug(v, "SLAVE"))
        return CT_REPLICA;
    if (eqICDebug(v, "PUBSUB"))
        return CT_PUBSUB;
    return -1;
}

// Recognized CLIENT LIST `flags=` letters (used to validate a FLAGS filter).
private enum string VALID_CLIENT_FLAGS = "AbcdeiMNOPrRSTtuUx";

private bool flagKnown(char ch) @nogc nothrow
{
    foreach (v; VALID_CLIENT_FLAGS)
        if (v == ch)
            return true;
    return false;
}

// The IP portion of an "ip:port" string (everything before the last colon).
// For a bracketed IPv6 "[::1]:port" the brackets are stripped too.
private const(char)[] ipOf(scope const(char)[] addr) @nogc nothrow
{
    size_t colon = addr.length;
    foreach (i, ch; addr)
        if (ch == ':')
            colon = i;
    auto ip = colon < addr.length ? addr[0 .. colon] : addr;
    if (ip.length >= 2 && ip[0] == '[' && ip[$ - 1] == ']')
        ip = ip[1 .. $ - 1];
    return ip;
}

private void repMsgErr(ref ByteBuffer o, scope const(char)[] prefix,
        scope const(char)[] name) nothrow
{
    import core.stdc.stdio : snprintf;

    char[256] b = void;
    auto n = snprintf(b.ptr, b.length, "%.*s%.*s",
            cast(int) prefix.length, prefix.ptr, cast(int) name.length, name.ptr);
    if (n > 0)
        repError(o, cast(const(char)[]) b[0 .. n]);
}

private void repQuotedErr(ref ByteBuffer o, scope const(char)[] prefix,
        scope const(char)[] name) nothrow
{
    import core.stdc.stdio : snprintf;

    char[192] b = void;
    auto n = snprintf(b.ptr, b.length, "%.*s'%.*s'",
            cast(int) prefix.length, prefix.ptr, cast(int) name.length, name.ptr);
    if (n > 0)
        repError(o, cast(const(char)[]) b[0 .. n]);
}

// Regression coverage for the pure CLIENT LIST/KILL filter helpers (see the
// Valkey introspection suite: ADDR/IP/TYPE/FLAGS filters and the flag validation).
unittest
{
    // ipOf: strip the trailing :port; unwrap [..] for IPv6.
    assert(ipOf("127.0.0.1:12345") == "127.0.0.1");
    assert(ipOf("[::1]:6379") == "::1");
    assert(ipOf("noport") == "noport");

    // parseClientType is case-insensitive; unknown => -1.
    assert(parseClientType("normal") == CT_NORMAL);
    assert(parseClientType("PubSub") == CT_PUBSUB);
    assert(parseClientType("replica") == CT_REPLICA);
    assert(parseClientType("slave") == CT_REPLICA);
    assert(parseClientType("bogus") == -1);

    // flagKnown accepts documented letters, rejects the rest (FLAGS validation).
    assert(flagKnown('N') && flagKnown('r') && flagKnown('O'));
    assert(!flagKnown('Q') && !flagKnown('Z'));

    // flagsSubset: every requested letter must be present in the client's set.
    assert(flagsSubset("N", "N"));
    assert(!flagsSubset("N", "r"));
    assert(flagsSubset("", "N")); // empty request matches anything
    assert(flagsSubset("Ir", "Ir") && !flagsSubset("Ir", "I"));
}

// CLIENT LIST [ID id...] [TYPE t] [NOT-TYPE t] [ADDR a] [LADDR a] [USER u]
// [NOT-USER u] [SKIPME y/n] [MAXAGE s] [NAME n] [FLAGS f] — one info line per
// connection matching every filter (repeated NOT-TYPE keeps the last one).
private bool flagsSubset(scope const(char)[] need, scope const(char)[] have) @nogc nothrow
{
    foreach (ch; need)
    {
        bool has = false;
        foreach (h; have)
            if (h == ch)
            {
                has = true;
                break;
            }
        if (!has)
            return false;
    }
    return true;
}

// Compose a client's `flags=` string into `buf`. Plain clients are "N"; special
// states add a letter (I import-source, r read-only). Empty => "N".
private const(char)[] connFlags(ref Conn p, return scope char[] buf) @nogc nothrow
{
    size_t n = 0;
    if (p.importSource)
        buf[n++] = 'I';
    if (p.readonlyFlag)
        buf[n++] = 'r';
    if (n == 0)
        buf[n++] = 'N';
    return buf[0 .. n];
}

// Every CLIENT LIST / CLIENT KILL filter, positive and its NOT- negation.
private struct ClientFilter
{
    Vector!ulong ids, notIds;
    int type = -1, notType = -1;
    long db = -1, notDb = -1, maxage = -1, minIdle = -1;
    const(char)[] addr, laddr, ip, notIp, user, notUser, name, notName,
        flags, notFlags, capa, notCapa, libName, notLibName, libVer, notLibVer;
    bool hasAddr, hasLaddr, hasIp, hasNotIp, hasUser, hasNotUser, hasName, hasNotName,
        hasFlags, hasNotFlags, hasCapa, hasNotCapa, hasLibName, hasNotLibName,
        hasLibVer, hasNotLibVer, skipme;
}

// Consume one ID list into `dst` (>0 each). rc: 0 ok, 1 syntax (no int), 2 range.
private int parseIdList(ref Vector!ulong dst, const(RVal)[] args, ref size_t i) nothrow
{
    bool any = false;
    while (i < args.length)
    {
        long v;
        if (!parseLong(args[i].str, v))
            break;
        if (v <= 0)
            return 2;
        dst.put(cast(ulong) v);
        any = true;
        i++;
    }
    return any ? 0 : 1;
}

private bool parseNonNeg(scope const(char)[] v, ref long dst, ref ByteBuffer o) nothrow
{
    if (!parseLong(v, dst))
    {
        repError(o, "ERR value is not an integer or out of range");
        return false;
    }
    if (dst < 0)
    {
        repError(o, "ERR value should be greater than 0");
        return false;
    }
    return true;
}

// Parse CLIENT LIST/KILL filter tokens into `fl`. On a bad argument writes the
// error to `o` and returns false. `fl.skipme` should hold the caller's default
// before the call (KILL: true, LIST: false); a SKIPME token overrides it.
private bool parseClientFilter(ref ClientFilter fl, const(RVal)[] args, ref ByteBuffer o) nothrow
{
    size_t i = 0;
    while (i < args.length)
    {
        auto f = args[i].str;
        // ID / NOT-ID take one or more client ids (stop at the next keyword).
        if (eqICDebug(f, "ID") || eqICDebug(f, "NOT-ID"))
        {
            immutable neg = eqICDebug(f, "NOT-ID");
            i++;
            immutable rc = parseIdList(neg ? fl.notIds : fl.ids, args, i);
            if (rc == 1)
            {
                repError(o, "ERR syntax error");
                return false;
            }
            if (rc == 2)
            {
                repError(o, "ERR client-id should be greater than 0");
                return false;
            }
            continue;
        }
        if (i + 1 >= args.length)
        {
            repError(o, "ERR syntax error");
            return false;
        }
        auto v = args[i + 1].str;
        if (eqICDebug(f, "TYPE"))
        {
            fl.type = parseClientType(v);
            if (fl.type < 0)
            {
                repQuotedErr(o, "ERR Unknown client type ", v);
                return false;
            }
        }
        else if (eqICDebug(f, "NOT-TYPE"))
        {
            fl.notType = parseClientType(v);
            if (fl.notType < 0)
            {
                repQuotedErr(o, "ERR Unknown client type ", v);
                return false;
            }
        }
        else if (eqICDebug(f, "USER"))
        {
            if (aclUser(v) is null)
            {
                repQuotedErr(o, "ERR No such user ", v);
                return false;
            }
            fl.user = v;
            fl.hasUser = true;
        }
        else if (eqICDebug(f, "NOT-USER"))
        {
            if (aclUser(v) is null)
            {
                repQuotedErr(o, "ERR No such user ", v);
                return false;
            }
            fl.notUser = v;
            fl.hasNotUser = true;
        }
        else if (eqICDebug(f, "FLAGS") || eqICDebug(f, "NOT-FLAGS"))
        {
            foreach (ch; v)
                if (!flagKnown(ch))
                {
                    repMsgErr(o, "ERR Unknown flags found in the provided filter: ", v);
                    return false;
                }
            if (eqICDebug(f, "NOT-FLAGS"))
            {
                fl.notFlags = v;
                fl.hasNotFlags = true;
            }
            else
            {
                fl.flags = v;
                fl.hasFlags = true;
            }
        }
        else if (eqICDebug(f, "ADDR"))
        {
            fl.addr = v;
            fl.hasAddr = true;
        }
        else if (eqICDebug(f, "LADDR"))
        {
            fl.laddr = v;
            fl.hasLaddr = true;
        }
        else if (eqICDebug(f, "IP"))
        {
            fl.ip = v;
            fl.hasIp = true;
        }
        else if (eqICDebug(f, "NOT-IP"))
        {
            fl.notIp = v;
            fl.hasNotIp = true;
        }
        else if (eqICDebug(f, "NAME"))
        {
            fl.name = v;
            fl.hasName = true;
        }
        else if (eqICDebug(f, "NOT-NAME"))
        {
            fl.notName = v;
            fl.hasNotName = true;
        }
        else if (eqICDebug(f, "CAPA"))
        {
            fl.capa = v;
            fl.hasCapa = true;
        }
        else if (eqICDebug(f, "NOT-CAPA"))
        {
            fl.notCapa = v;
            fl.hasNotCapa = true;
        }
        else if (eqICDebug(f, "LIB-NAME"))
        {
            fl.libName = v;
            fl.hasLibName = true;
        }
        else if (eqICDebug(f, "NOT-LIB-NAME"))
        {
            fl.notLibName = v;
            fl.hasNotLibName = true;
        }
        else if (eqICDebug(f, "LIB-VER"))
        {
            fl.libVer = v;
            fl.hasLibVer = true;
        }
        else if (eqICDebug(f, "NOT-LIB-VER"))
        {
            fl.notLibVer = v;
            fl.hasNotLibVer = true;
        }
        else if (eqICDebug(f, "DB"))
        {
            if (!parseNonNeg(v, fl.db, o))
                return false;
        }
        else if (eqICDebug(f, "NOT-DB"))
        {
            if (!parseNonNeg(v, fl.notDb, o))
                return false;
        }
        else if (eqICDebug(f, "MAXAGE"))
        {
            if (!parseNonNeg(v, fl.maxage, o))
                return false;
        }
        else if (eqICDebug(f, "IDLE"))
        {
            if (!parseNonNeg(v, fl.minIdle, o))
                return false;
        }
        else if (eqICDebug(f, "SKIPME"))
        {
            if (eqICDebug(v, "YES"))
                fl.skipme = true;
            else if (eqICDebug(v, "NO"))
                fl.skipme = false;
            else
            {
                repError(o, "ERR syntax error");
                return false;
            }
        }
        else
        {
            repError(o, "ERR syntax error");
            return false;
        }
        i += 2;
    }
    return true;
}

private bool matchesFilter(ref ClientFilter fl, Conn* p, Conn* self, long now) @nogc nothrow
{
    if (fl.ids.length)
    {
        bool inSet = false;
        foreach (fid; fl.ids[])
            if (fid == p.id)
            {
                inSet = true;
                break;
            }
        if (!inSet)
            return false;
    }
    foreach (fid; fl.notIds[])
        if (fid == p.id)
            return false;
    immutable pt = connType(*p);
    if (fl.type >= 0 && pt != fl.type)
        return false;
    if (fl.notType >= 0 && pt == fl.notType)
        return false;
    if (fl.hasAddr && p.addr[] != fl.addr)
        return false;
    if (fl.hasLaddr && p.laddr[] != fl.laddr)
        return false;
    if (fl.hasIp && ipOf(p.addr[]) != fl.ip)
        return false;
    if (fl.hasNotIp && ipOf(p.addr[]) == fl.notIp)
        return false;
    if (fl.hasUser && !(p.user !is null && p.user.name == fl.user))
        return false;
    if (fl.hasNotUser && p.user !is null && p.user.name == fl.notUser)
        return false;
    if (fl.hasName && p.clientName[] != fl.name)
        return false;
    if (fl.hasNotName && p.clientName[] == fl.notName)
        return false;
    if (fl.hasLibName && p.libName[] != fl.libName)
        return false;
    if (fl.hasNotLibName && p.libName[] == fl.notLibName)
        return false;
    if (fl.hasLibVer && p.libVer[] != fl.libVer)
        return false;
    if (fl.hasNotLibVer && p.libVer[] == fl.notLibVer)
        return false;
    if (fl.db >= 0 && p.dbp.db != fl.db)
        return false;
    if (fl.notDb >= 0 && p.dbp.db == fl.notDb)
        return false;
    char[8] fbuf = void;
    auto pf = connFlags(*p, fbuf[]);
    if (fl.hasFlags && !flagsSubset(fl.flags, pf))
        return false;
    if (fl.hasNotFlags && flagsSubset(fl.notFlags, pf))
        return false;
    auto pcapa = p.capaRedirect ? "r" : "";
    if (fl.hasCapa && !flagsSubset(fl.capa, pcapa))
        return false;
    if (fl.hasNotCapa && flagsSubset(fl.notCapa, pcapa))
        return false;
    if (fl.maxage >= 0)
    {
        immutable age = p.connMs ? (now - p.connMs) / 1000 : 0;
        if (age < fl.maxage)
            return false;
    }
    if (fl.minIdle >= 0)
    {
        immutable idle = p.lastActiveMs ? (now - p.lastActiveMs) / 1000 : 0;
        if (idle < fl.minIdle)
            return false;
    }
    if (fl.skipme && p is self)
        return false;
    return true;
}

private bool clientList(ref Conn c, const(RVal)[] args, ref ByteBuffer o) nothrow
{
    ClientFilter fl; // LIST default: skipme = false (the caller is listed)
    if (!parseClientFilter(fl, args, o))
        return true;
    static ByteBuffer lb; // TLS
    lb.clear();
    immutable now = nowMs();
    Vector!ulong ids;
    snapshotConnIds(ids);
    foreach (id; ids[])
    {
        auto s = connById(id);
        if (s.isNull)
            continue;
        auto p = &s.get();
        if (matchesFilter(fl, p, &c, now))
            appendConnInfo(p, lb);
    }
    repBulk(o, cast(const(char)[]) lb.data);
    return true;
}

private bool clientKill(ref Conn c, const(RVal)[] args, ref ByteBuffer o) nothrow
{
    if (args.length == 0)
    {
        repError(o, "ERR wrong number of arguments for 'client|kill' command");
        return true;
    }
    // legacy single-argument form: CLIENT KILL addr:port -> +OK / -No such client
    if (args.length == 1)
    {
        auto want = args[0].str;
        Vector!ulong ids;
        snapshotConnIds(ids);
        foreach (id; ids[])
        {
            auto s = connById(id);
            if (s.isNull)
                continue;
            auto p = &s.get();
            if (p.addr[] == want && p !is &c)
            {
                killConn(p);
                repSimple(o, "OK");
                return true;
            }
        }
        repError(o, "ERR No such client");
        return true;
    }
    ClientFilter fl;
    fl.skipme = true; // KILL default: spare the caller unless SKIPME no
    if (!parseClientFilter(fl, args, o))
        return true;
    long killed = 0;
    bool killSelf = false;
    immutable now = nowMs();
    Vector!ulong ids;
    snapshotConnIds(ids);
    foreach (id; ids[])
    {
        auto s = connById(id);
        if (s.isNull)
            continue;
        auto p = &s.get();
        if (!matchesFilter(fl, p, &c, now))
            continue;
        killed++;
        if (p is &c)
            killSelf = true; // defer: reply must flush before we close
        else
            killConn(p);
    }
    repInt(o, killed);
    if (killSelf)
    {
        c.user = null;
        c.authed = false;
        return false; // close self after the count reply flushes
    }
    return true;
}

private bool eqICDebug(scope const(char)[] s, scope const(char)[] upper) @nogc nothrow
{
    if (s.length != upper.length)
        return false;
    foreach (i, c; s)
    {
        auto u = c >= 'a' && c <= 'z' ? cast(char)(c - 32) : c;
        if (u != upper[i])
            return false;
    }
    return true;
}

/// *3 [verb][channel-or-nil][:active-subscription-count]
private void subReply(ref ByteBuffer o, scope const(char)[] verb,
        scope const(char)[] channel, size_t count) @nogc nothrow
{
    repPushHeader(o, 3); // RESP3 delivers (un)subscribe confirmations as pushes
    repBulk(o, verb);
    if (channel is null)
        repNullBulk(o);
    else
        repBulk(o, channel);
    repInt(o, cast(long) count);
}

// package-visible so the dashboard bridge can serve PUBSUB introspection: it is a
// server-layer command (not in the commands.d dispatch executeScriptCommand uses), so
// the bridge intercepts it and calls this directly on the main thread (reads gPubSub).
package void pubsubIntrospect(const(RVal)[] args, ref ByteBuffer o) nothrow
{
    if (args.length == 0)
    {
        repError(o, "ERR wrong number of arguments for 'pubsub' command");
        return;
    }
    auto sub = args[0].str;
    char[16] sbuf = void;
    if (sub.length > sbuf.length)
    {
        repUnknownSubcommand(o, "PUBSUB", sub);
        return;
    }
    foreach (i, ch; sub)
        sbuf[i] = ch >= 'a' && ch <= 'z' ? cast(char)(ch - 32) : ch;

    switch (cast(string) sbuf[0 .. sub.length])
    {
    case "CHANNELS":
        {
            auto pat = args.length > 1 ? args[1].str : null;
            size_t n = 0;
            gPubSub.eachChannel(pat, (ch, cnt) { n++; return 0; });
            repArrayHeader(o, n);
            gPubSub.eachChannel(pat, (ch, cnt) { repBulk(o, ch); return 0; });
            break;
        }
    case "NUMSUB":
        {
            repArrayHeader(o, (args.length - 1) * 2);
            foreach (ref a; args[1 .. $])
            {
                repBulk(o, a.str);
                repInt(o, cast(long) gPubSub.channelSubCount(a.str));
            }
            break;
        }
    case "NUMPAT":
        {
            repInt(o, cast(long) gPubSub.patternCount);
            break;
        }
    case "SHARDCHANNELS":
        {
            auto pat = args.length > 1 ? args[1].str : null;
            size_t n = 0;
            gShardPubSub.eachChannel(pat, (ch, cnt) { n++; return 0; });
            repArrayHeader(o, n);
            gShardPubSub.eachChannel(pat, (ch, cnt) { repBulk(o, ch); return 0; });
            break;
        }
    case "SHARDNUMSUB":
        {
            repArrayHeader(o, (args.length - 1) * 2);
            foreach (ref a; args[1 .. $])
            {
                repBulk(o, a.str);
                repInt(o, cast(long) gShardPubSub.channelSubCount(a.str));
            }
            break;
        }
    case "TAP":
        {
            // dreads-native: dashboard message tail. Arm/refresh the tap and drain
            // everything buffered since the last poll as a flat [ch, msg, …] array.
            // Self-expiring — the 1s cron disarms when polling stops.
            import dreads.stream : nowMs;

            pubsubTapArm(nowMs());
            repArrayHeader(o, pubsubTapPending() * 2);
            pubsubTapDrain((ch, msg) { repBulk(o, ch); repBulk(o, msg); });
            break;
        }
    case "HELP":
        {
            repHelp!"PUBSUB"(o);
            break;
        }
    default:
        repUnknownSubcommand(o, "PUBSUB", sub);
    }
}

// Focused ACL handler for the dashboard bridge. ACL is a server-layer command (not in
// the commands.d dispatch the bridge uses), so it was unreachable via /api/exec. This
// reuses the SAME primitives the client path uses (aclGetOrCreate / aclApplyRule /
// aclEncodeCanonicalSetuser / propagateAclLog / aclDelUser / aclEachUser) — writes
// propagate identically — minus the client-session self-delete logic (the dashboard is
// admin, has no ACL session). Gated upstream by dashboard-admin. LIST/GETUSER return
// each user as its canonical "ACL SETUSER …" array (dashboard-friendly, editable).
package void aclDashboardCommand(scope const(RVal)[] arr, scope const(ubyte)[] rawCmd, ref ByteBuffer o) nothrow
{
    if (arr.length < 2)
    {
        repError(o, "ERR wrong number of arguments for 'acl' command");
        return;
    }
    char[12] sbuf = void;
    auto sub = arr[1].str;
    const(char)[] su = sub;
    if (sub.length <= sbuf.length)
    {
        foreach (i, ch; sub)
            sbuf[i] = (ch >= 'a' && ch <= 'z') ? cast(char)(ch - 32) : ch;
        su = sbuf[0 .. sub.length];
    }
    switch (su)
    {
    case "WHOAMI":
        repBulk(o, "default");
        return;
    case "CAT":
        repArrayHeader(o, aclCatNames.length);
        foreach (name; aclCatNames)
            repBulk(o, name);
        return;
    case "USERS":
        size_t nu = 0;
        aclEachUser((u) { nu++; return 0; });
        repArrayHeader(o, nu);
        aclEachUser((u) { repBulk(o, u.name); return 0; });
        return;
    case "LIST":
        size_t nl = 0;
        aclEachUser((u) { nl++; return 0; });
        repArrayHeader(o, nl);
        aclEachUser((u) { aclEncodeCanonicalSetuser(u, o); return 0; });
        return;
    case "GETUSER":
        if (arr.length < 3)
        {
            repError(o, "ERR wrong number of arguments for 'acl|getuser' command");
            return;
        }
        AclUser* found;
        aclEachUser((u) { if (u.name == arr[2].str) { found = u; return 1; } return 0; });
        if (found is null)
            repNullArray(o);
        else
            aclEncodeCanonicalSetuser(found, o);
        return;
    case "SETUSER":
        if (arr.length < 3)
        {
            repError(o, "ERR wrong number of arguments for 'acl|setuser' command");
            return;
        }
        if (gReplicator !is null && !gReplicator.isLeader)
        {
            repError(o, "READONLY You can't write against a read only replica.");
            return;
        }
        auto u = aclGetOrCreate(arr[2].str);
        const(char)[] err;
        try
        {
            foreach (ref r; arr[3 .. $])
                if (!aclApplyRule(u, r.str, err))
                {
                    repError(o, err);
                    return;
                }
        }
        catch (Exception)
        {
            repError(o, "ERR ACL SETUSER failed to hash a password");
            return;
        }
        gAclActive = true;
        aclKillRevokedSubscribers(u);
        static ByteBuffer canon;
        canon.clear();
        aclEncodeCanonicalSetuser(u, canon);
        if (!propagateAclLog(canon.data, o))
            return;
        repSimple(o, "OK");
        return;
    case "DELUSER":
        if (arr.length < 3)
        {
            repError(o, "ERR wrong number of arguments for 'acl|deluser' command");
            return;
        }
        if (gReplicator !is null && !gReplicator.isLeader)
        {
            repError(o, "READONLY You can't write against a read only replica.");
            return;
        }
        foreach (ref a; arr[2 .. $])
            if (a.str == "default")
            {
                repError(o, "ERR The 'default' user cannot be removed");
                return;
            }
        // disconnect any live session authed as a deleted user (security)
        {
            Vector!ulong ids;
            snapshotConnIds(ids);
            foreach (ref a; arr[2 .. $])
                foreach (id; ids[])
                {
                    auto s = connById(id);
                    if (s.isNull)
                        continue;
                    auto p = &s.get();
                    if (p.user !is null && p.user.name == a.str)
                        killConn(p);
                }
        }
        long n = 0;
        foreach (ref a; arr[2 .. $])
            if (aclDelUser(a.str))
                n++;
        if (!propagateAclLog(rawCmd, o)) // ACL DELUSER is already canonical + idempotent
            return;
        repInt(o, n);
        return;
    default:
        repError(o, "ERR unknown ACL subcommand or wrong number of arguments");
        return;
    }
}
