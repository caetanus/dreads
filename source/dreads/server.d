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

import vibe.core.core : runEventLoop, runTask, setTimer;
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
import dreads.aclcat : gCmdCats;
import dreads.authpw : initAuthPw;
import dreads.aof : Aof, aofLoad, aofRewrite;
import dreads.commands : dispatch, globMatch, isWriteCommand, isPausedByWrite,
    isDenyOomCommand, gScriptWritesHook, propagationOverride, parseLong, gWriteNoOp;
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
import dreads.obj : Keyspace, gDbs, NUM_DBS, ObjType, gBlockedClients, gConnectedClients,
    gImportSourceActive;
import dreads.dict : Dict, Unit;
import dreads.pubsub : PubSub, Subscriber, RcMsg, rcFromBytes, rcData, rcRetain, rcRelease, rcAsPush;
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

private __gshared PubSub gPubSub;
private __gshared PubSub gShardPubSub; // single node: shard = plain, own namespace
private __gshared Aof gAof;
private __gshared const(char)[] gAofPath;
public __gshared ulong gWriteEpoch; // bumped on every effective write (WATCH + INFO changes)
private __gshared ulong gClientIds;
// The connection whose command is executing right NOW (single-thread, set by the
// serve loop around handleCommand). A pub/sub message published to THIS connection
// (publish-to-self) must trail the running command's reply, not interleave before
// it — connSink defers such a message to pendingInval (drained after outb). Null
// between commands so an ordinary cross-client delivery goes straight to the queue.
private __gshared Conn* gCmdConn;
// MONITOR feed: registered connections receive every executed command
private __gshared Conn*[64] gMonitors;
private __gshared size_t gMonitorCount;

// blocked clients (BLPOP & co.) wake on any write and re-check their keys
private __gshared LocalManualEvent gKeyActivity;

// CLIENT PAUSE barrier: while `gPauseUntilMs` is in the future, commands that match
// the mode (ALL, or WRITE-only) are NOT executed — each connection's fiber buffers
// their raw bytes in `Conn.pausedBuf` (barriered) and REPLAYS them once the window
// lifts (timeout or CLIENT UNPAUSE). Transient connection state: never AOF-logged,
// never replicated (raft handles failover; this is for online migration windows).
// gPauseUntilMs / gPauseAll / gPauseIssuer live in dreads.obj (so INFO can read
// them without a module cycle); imported publicly here for the rest of server.d.
public import dreads.obj : gPauseUntilMs, gPauseAll, gPauseIssuer;
private __gshared LocalManualEvent gPauseEvt; // parked fibers wake on UNPAUSE / timeout
// Replay re-entrancy guard (the CLIENT PAUSE heisenbug). replayPaused() drains a
// connection's held commands through the normal pipeline, and that path does IO
// (flushOut / gAof.flush) which yields — another connection's fiber can land a
// fresh CLIENT PAUSE mid-drain. If that pause took effect while we're still
// replaying, the remaining held commands would re-barrier against a window that
// only exists because of the yield, and the emit/park ordering can strand a fiber.
// Fix: clear the (already-lifted) window BEFORE the drain, and while `gReplaying`
// is set DEFER any incoming CLIENT PAUSE into gPausePending* — applied AFTER the
// drain finishes, so it never interleaves with the very commands it must follow.
private __gshared bool gReplaying;         // a replayPaused() drain is in progress
private __gshared bool gPausePending;      // a CLIENT PAUSE arrived mid-replay
private __gshared ulong gPausePendingEnd;  // its (already absolute) deadline
private __gshared bool gPausePendingAll;   // its ALL(true)/WRITE(false) mode
private __gshared ulong gPausePendingIssuer; // its issuer conn id (exempt)
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
    if (aofPath !is null)
    {
        auto replayed = aofLoad(aofPath, gKeys);
        if (replayed < 0)
        {
            printf("dreads: cannot read AOF\n");
            return 1;
        }
        printf("dreads: AOF replayed %lld commands\n", replayed);
        gAofPath = aofPath;
        if (!gAof.open(aofPath))
        {
            printf("dreads: cannot open AOF for append\n");
            return 1;
        }
    }
    gKeyActivity = createManualEvent();
    gPauseEvt = createManualEvent();
    {
        import dreads.notify : gNotifyPublish, gPublishHook, parseNotifyFlags;

        cast(void) parseNotifyFlags(gConfig.notifyKeyspaceEvents, gNotifyFlags);
        gNotifyPublish = (scope const(char)[] chan, scope const(char)[] msg) nothrow{
            gPubSub.publish(chan, msg);
        };
        // lets a script's redis.call('publish'/'spublish') reach the pub/sub layer
        gPublishHook = (scope const(char)[] chan, scope const(char)[] msg, bool shard) nothrow{
            return shard ? gShardPubSub.publish(chan, msg, "smessage")
                : gPubSub.publish(chan, msg);
        };
    }
    {
        import dreads.cluster : initCluster;

        if (gConfig.clusterEnabled)
        {
            initCluster();
            printf("dreads: cluster mode\n");
        }
    }
    {
        import dreads.obj : lruClock, gActiveExpire, gActiveEviction, gTrackInvalidateHook;
        import dreads.rand : seedRand;
        import dreads.stream : nowMs;

        gActiveExpire = gConfig.activeExpire; // drop-soon timer only runs when enabled
        gActiveEviction = gConfig.activeEviction; // background maxmemory eviction
        lruClock = cast(uint)(nowMs() / 1000);
        seedRand(nowMs()); // shuffle the random-pick commands per boot
        {
            // effects replication: script writes reach the AOF one by one
            import dreads.scripting : gScriptEffectSink, startLuaScriptPool;

            gScriptEffectSink = (scope const(ubyte)[] fx) @nogc nothrow {
                if (gAof.enabled)
                    gAof.append(fx);
            };
            // scripts run on a dedicated thread (off the event loop), so a
            // busy script can't stall the loop and SCRIPT KILL can reach it
            startLuaScriptPool();
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
        // Active expiry runs on its OWN fast 200ms timer (Valkey sweeps ~10x/s;
        // a 1s cadence left keys logically-expired-but-present far too long). Kept
        // separate from the 1s cron below so the fsync/eviction/lru work does NOT
        // also run 5x more often.
        setTimer(200.msecs, delegate() @trusted nothrow {
            import dreads.det : freezeClock;
            import dreads.obj : gActiveExpire;

            // Cheap early-out: with active expiry off (the default) this fast timer
            // must do NOTHING — no clock read, no db sweep, no flush — so it never
            // costs the hot path a wasted wakeup's worth of work.
            if (!gActiveExpire)
                return;
            freezeClock(0); // pin this cycle's clock to wall time (see below)
            // A CLIENT PAUSE freezes replicated mutation: active expiry (an expiry
            // is a propagated DEL) is held until the window lifts, like eviction.
            immutable paused = gPauseUntilMs != 0 && nowMs() < gPauseUntilMs;
            if (paused)
                return;
            foreach (ref d; gDbs) // drop-soon sweep across every database
            {
                gNotifyDb = d.db; // "expired" fires on THIS db's channel
                d.activeExpireCycle();
                d.activeSubExpireCycle(); // reap due hash-field TTLs (the "path pro resto")
            }
            flushPendingNotify(); // deliver the "expired"/"hexpired"/"evicted" events queued
            if (gTrackCount) // deliver invalidations queued by active expiry (no writer)
                flushTrackingInval(0);
        }, true);
        setTimer(1.seconds, delegate() @trusted nothrow {
            // Pin THIS cycle's clock to wall time. detNow() otherwise returns the
            // last command's frozen gClock (never reset to 0 after dispatch), which
            // is stale here — so a background eviction cycle would compare against a
            // frozen "now".
            import dreads.det : freezeClock;

            freezeClock(0); // 0 => freeze the current wall clock into gClock
            lruClock = cast(uint)(nowMs() / 1000);
            runEvictionCycle(); // opt-in background maxmemory eviction (skips under pause)
            releaseIdleMigrateConns(); // close MIGRATE sockets idle > 10s
            flushPendingNotify(); // deliver any events the eviction cycle queued
            if (gTrackCount)
                flushTrackingInval(0);
            gAof.fsyncNow();
        }, true);
    }
    initReplication();
    // SO_REUSEADDR + SO_REUSEPORT: without reusePort a restarted server can
    // find the port stuck in TIME_WAIT for a long while (vibe's default only
    // sets reuseAddress). Both let a fresh instance rebind immediately.
    listenTCP(port, delegate(TCPConnection conn) @trusted nothrow {
        serveClient(conn);
    }, TCPListenOptions.reuseAddress | TCPListenOptions.reusePort);
    printf("dreads listening on port %u\n", cast(uint) port);
    return runEventLoop();
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
    // Election timeout must dwarf the heartbeat: the single-threaded event
    // loop can starve the tick timer under heavy write load (measured
    // heartbeat gaps of 120-370ms during a 5000-command burst), so a tight
    // 200ms timeout triggers spurious elections and drops in-flight writes.
    // 50 ticks -> ~1-2s randomized (etcd/Redis-Raft use ~1s) leaves ample
    // margin for a busy leader; heartbeat stays at 40ms so ~25 fit per window.
    cfg.electionTimeoutTicks = 50; // 20ms tick -> ~1000-2000ms randomized
    cfg.heartbeatTicks = 2; // ~40ms
    cfg.joinMode = gConfig.raftJoin; // passive learner until a config adds us
    auto raftPort = gConfig.raftPort != 0 ? gConfig.raftPort : cast(ushort)(gConfig.port + 10_000);
    string base = gConfig.appendfilename.length ? gConfig.appendfilename : "dreads";
    gReplicator = new Replicator(cfg, peers, raftPort, base ~ ".raft", &gDbs[0]);
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

// Registry of every live connection: an id→Weak!Conn index. It is the single
// source of truth for both O(1) lookup (CLIENT UNBLOCK, tracking redirect) and
// iteration (CLIENT LIST / KILL, ACL revoke) — the old intrusive list is gone.
// Each connection lives in a Shared!Conn owned by its serveClient fiber; the
// registry holds a WEAK observer, so a cross-fiber user resolves it via lock()
// and keeps the Conn (and its RAII resources) alive for the duration of the
// access — the UAF that killed tracking is impossible by construction. All
// access is on the single event-loop thread, so the counts need no atomics.
private __gshared Dict!(Weak!Conn) gConnById; // keyed by the id's raw 8 bytes

// The 8 raw bytes of a client id, used as the gConnById key (HashMap.set dups it).
private const(char)[] connIdKey(ref const ulong id) @nogc nothrow @trusted
{
    return (cast(const(char)*)&id)[0 .. ulong.sizeof];
}

/// O(1) live-connection lookup by id. Returns a STRONG lock (empty if the id is
/// unknown or its connection has died) — hold it while touching the Conn so it
/// cannot be freed under a yield.
private Shared!Conn connById(ulong id) nothrow @trusted
{
    if (auto w = gConnById.get(connIdKey(id)))
        return w.lock();
    return Shared!Conn.init;
}

private void registerConn(ref Shared!Conn sc) @nogc nothrow
{
    gConnById.set(connIdKey(sc.get().id), sc.weaken());
    gConnectedClients++;
}

private void unregisterConn(ulong id) @nogc nothrow
{
    if (gConnById.remove(connIdKey(id)))
        gConnectedClients--;
}

/// Snapshot every live connection's id into `outv`. Iterating the registry
/// directly while killing/closing conns is unsafe (tcp.close may yield and let a
/// target fiber unregister mid-iteration, mutating gConnById). Callers walk the
/// id snapshot and re-resolve each via connById() (which returns a strong lock),
/// so a conn that died in the meantime is simply skipped and the one being acted
/// on is kept alive by its lock. ids are monotonic, so there is no ABA reuse.
private void snapshotConnIds(ref Vector!ulong outv) nothrow @trusted
{
    foreach (key, ref w; gConnById)
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
private __gshared Dict!(Dict!Unit) gInvalTable; // default mode: key -> conn-id set
private __gshared Dict!Unit gBcastConns; // bcast mode: conn-id set
private __gshared size_t gTrackCount; // # tracking conns (the unlikely gate)

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
private __gshared Dict!(Dict!Unit) gPend;
// BCAST keys written this command (raw). Which bcast client/prefix each belongs to
// is resolved at flush (connById is not @nogc, but the expiry accumulation path is).
private __gshared Dict!Unit gBcastPendingKeys;
// Keys invalidated by a SERVER-caused event (expiry) this cycle. NOLOOP suppresses
// keys the CLIENT modified, but not these — so an expiry of a key the client also
// wrote in the same command still reaches it. Cleared at flush.
private __gshared Dict!Unit gExpireKeys;

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
        forEachCommandKey!((scope const(char)[] key, bool nr, bool nw) @nogc nothrow @trusted {
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
        forEachCommandKey!((scope const(char)[] key, bool nr, bool nw) @nogc nothrow @trusted {
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

private __gshared Dict!WaiterQ[NUM_DBS] gWaiters;

// Is the connection still usable to receive a wakeup? (peer may have vanished
// while its fiber was parked — never fire at a dead conn.)
private bool connAlive(Conn* c) nothrow
{
    if (c is null)
        return false;
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

    if (gBlockedClients == 0 || gWaiters[db].length == 0)
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
    c.id = ++gClientIds;
    c.dbp = &gDbs[0]; // default to db 0
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
            gBlockedClients--;
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
                c.pauseBlocked = false;
                gBlockedClients--;
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
            if (c.pendingCount > 0)
                flushPending(*c, outb);
            gAof.flush();
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
                    gPauseEvt.emit();
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
                        c.pauseBlocked = true;
                        gBlockedClients++;
                    }
                    immutable rem = gPauseUntilMs - now;
                    immutable cap = rem < PAUSE_POLL_MS ? rem : PAUSE_POLL_MS;
                    immutable ec = gPauseEvt.emitCount;
                    gPauseEvt.waitUninterruptible(msecs(cap), ec);
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
                // Stamp activity ONCE per read (drives CLIENT LIST idle=). One
                // clock read amortized over the whole pipeline batch — a per-command
                // clock_gettime was a real throughput hit.
                c.lastActiveMs = nowMs();

                size_t pos = 0;
                while (keep)
                {
                    RVal cmd;
                    size_t cmdStart = pos;
                    auto st = parseValue(inb.data, pos, arena, cmd);
                    if (st == ParseStatus.incomplete)
                        break;
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
                // Reap the chunk's trailing run of pipelined writes (their replies
                // come last, in order) before flushing the batch to the client.
                if (c.pendingCount > 0)
                    flushPending(*c, outb);
                gAof.flush();
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
    else if (gAof.enabled)
        gAof.append(logForm);
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

    // CLIENT PAUSE barrier — before ACL/dispatch/AOF. A matching command (ALL, or a
    // write in WRITE mode) is buffered raw and replayed when the window lifts; CLIENT
    // is exempt so UNPAUSE always lands. Never executed nor logged while barriered.
    // MULTI queuing is NOT barriered (commands still return QUEUED under a pause);
    // the transaction is instead held as a unit at EXEC iff it queued a write.
    // A pause is rare — keep it off the branch predictor's hot path (expect false).
    if (expect(gPauseUntilMs != 0, false) && uname != "CLIENT" && c.id != gPauseIssuer)
    {
        if (nowMs() >= gPauseUntilMs)
            gPauseUntilMs = 0; // window elapsed — run normally
        else
        {
            bool barrier;
            if (uname == "EXEC")
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
            else if (n < 0 || n >= NUM_DBS)
                repError(o, "ERR DB index is out of range");
            else
            {
                c.dbp = &gDbs[cast(size_t) n];
                repSimple(o, "OK");
            }
            return true;
        }
    case "MULTI":
        if (c.inMulti)
            repError(o, "ERR MULTI calls can not be nested");
        else
        {
            c.inMulti = true;
            c.multiCount = 0;
            c.multiHasWrite = false;
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
            c.multiQueue.clear();
            c.watching = false;
            repSimple(o, "OK");
        }
        return true;
    case "WATCH":
        if (c.inMulti)
            repError(o, "ERR WATCH inside MULTI is not allowed");
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
    return executeCommand(c, cmd, rawCmd, o, arena);
}

/// Executes one non-transactional command: pub/sub and connection commands
/// (which need the connection identity), scripting, persistence hooks, and
/// the @nogc dispatch for everything else.
private bool executeCommand(ref Conn c, const ref RVal cmd, scope const(ubyte)[] rawCmd,
        ref ByteBuffer o, ref Arena arena) nothrow
{
    auto name = cmd.arr[0].str;
    auto args = cmd.arr[1 .. $];
    char[16] nbuf = void;
    if (name.length > nbuf.length)
        return dispatch(cmd, *c.dbp, o, arena);
    foreach (i, ch; name)
        nbuf[i] = ch >= 'a' && ch <= 'z' ? cast(char)(ch - 32) : ch;
    auto uname = cast(string) nbuf[0 .. name.length];

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

    if (gMonitorCount > 0)
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
                break;
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
                break;
            auto after = args[streamsAt + 1 .. $];
            if (after.length == 0 || after.length % 2 != 0)
                break; // let dispatch surface the syntax error
            auto half = after.length / 2;
            bool hasGt = false;
            foreach (ref idTok; after[half .. $])
                if (idTok.str == ">")
                {
                    hasGt = true;
                    break;
                }
            if (!hasGt)
                break; // only `>` (new messages) can block; explicit ids read now
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
            gAof.flush();
            gAof.fsyncNow();
            repArrayHeader(o, 2);
            repInt(o, gAof.enabled ? 1 : 0);
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
            if (gAof.enabled && !aofRewrite(gAof, gAofPath))
                repError(o, "ERR AOF rewrite failed");
            else
                repSimple(o, "Background append only file rewriting started");
            return true;
        }
    case "MONITOR":
        {
            if (gMonitorCount < gMonitors.length)
            {
                enterSubMode(c); // monitors receive an async stream, like subscribers
                gMonitors[gMonitorCount++] = &c;
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
            gAof.flush();
            gAof.fsyncNow();
            if (uname.length == 4)
                repSimple(o, "OK");
            else
                repSimple(o, "Background saving started");
            return true;
        }
    case "LASTSAVE":
        {
            repInt(o, gAof.lastFsyncUnix);
            return true;
        }
    case "SHUTDOWN":
        {
            import core.stdc.stdlib : exit;

            gAof.flush();
            gAof.fsyncNow();
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
                gKeyActivity.emit();
                signalReadyKeys(c.dbp.db, *c.dbp);
            }
            return true;
        }
    case "SCRIPT":
        {
            scriptCommand(args, o);
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

    // Raft policy gate — only when replication is configured; standalone
    // (gReplicator is null) falls straight through with zero added cost.
    if (gReplicator !is null)
    {
        // GETEX is not a logged write (a plain GETEX changes nothing and the
        // AOF stays clean via the PEXPIREAT/PERSIST override), but under raft
        // its TTL mutation must still reach followers: propose it and let the
        // injected clock keep the replay deterministic.
        if (isWriteCommand(uname) || uname == "GETEX")
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

    if (gConfig.maxmemory && isDenyOomCommand(uname) && !freeMemoryIfNeeded())
    {
        // refused before running: a rejected_call (not a call) + a leaf OOM error
        char[16] lc = void;
        foreach (i, ch; name)
            lc[i] = ch >= 'A' && ch <= 'Z' ? cast(char)(ch + 32) : ch;
        statRejected(aclCmdIndex(cast(const(char)[]) lc[0 .. name.length]));
        enum oom = "OOM command not allowed when used memory > 'maxmemory'.";
        statErrorReply(oom);
        repError(o, oom);
        return true;
    }
    immutable errPrev = gTotalErrorReplies; // leaf-vs-propagated guard (see stats.d)
    auto outBefore = o.length;
    gWriteNoOp = false; // a write command may flag itself a no-op (SETBIT/BITFIELD)
    auto keep = dispatch(cmd, *c.dbp, o, arena);
    immutable errored = o.length > outBefore && o.data[outBefore] == '-';
    // errorstats/total: only a REAL leaf error (a nested command — e.g. a script's
    // redis.call — that failed already bumped the counter during dispatch, so the
    // outer command must not re-count its propagated error).
    if (errored && gTotalErrorReplies == errPrev)
        statErrorReply(cast(const(char)[]) o.data[outBefore .. $]);
    // INFO commandstats: count the executed data command (name.length <= 16 here,
    // guaranteed by the nbuf check above). Blocking/pubsub/connection commands
    // handled before this point are not counted — see BLACKBOX-TODO.md.
    {
        char[16] lc = void;
        foreach (i, ch; name)
            lc[i] = ch >= 'A' && ch <= 'Z' ? cast(char)(ch + 32) : ch;
        statCall(aclCmdIndex(cast(const(char)[]) lc[0 .. name.length]), errored);
    }
    // A write that flagged itself a no-op (no data changed) neither dirties, wakes,
    // nor propagates — matches Redis's dirty-delta model (SETBIT/BITFIELD SET only
    // count when the bit/field actually changed).
    if (o.length > outBefore && o.data[outBefore] != '-' && !gWriteNoOp)
    {
        immutable isW = isWriteCommand(uname) || !propagationOverride.empty;
        if (isW)
        {
            gWriteEpoch++; // WATCH visibility
            gKeyActivity.emit(); // wake blocked XREAD readers (fan-out)
            signalReadyKeys(c.dbp.db, *c.dbp); // wake pop-family fronts
        }
        if (gAof.enabled)
        {
            if (!propagationOverride.empty)
                gAof.append(propagationOverride.data);
            else if (isWriteCommand(uname))
                gAof.append(rawCmd);
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
                auto n = snprintf(b.ptr, b.length, "%d", cast(int) NUM_DBS);
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

private __gshared size_t gEvictCursor;

/// Approximate LRU eviction: sample live keys, evict the coldest, repeat.
/// Returns false when memory stays over the limit (noeviction, or nothing
/// evictable under volatile-lru).
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
    if (gAof.enabled)
    {
        static ByteBuffer delCmd; // TLS scratch for AOF propagation
        delCmd.clear();
        repArrayHeader(delCmd, 2);
        repBulk(delCmd, "DEL");
        repBulk(delCmd, victim);
        gAof.append(delCmd.data);
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
    foreach (ref d; gDbs)
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

private __gshared MigrateSock[8] gMigrateCache;
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
            if (gAof.enabled)
            {
                delCmd.clear();
                repArrayHeader(delCmd, 2);
                repBulk(delCmd, "DEL");
                repBulk(delCmd, k);
                gAof.append(delCmd.data);
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
private bool waitForActivity(ref int ec, ref long remainingMs, ulong timeoutMs) nothrow
{
    import core.time : MonoTime, msecs;

    import dreads.obj : gBlockedClients;

    if (timeoutMs != 0 && remainingMs <= 0)
        return false;
    gBlockedClients++;
    scope (exit)
        gBlockedClients--;
    auto slice = timeoutMs == 0 ? 3_600_000 : remainingMs;
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

    if (timeoutMs != 0 && remainingMs <= 0)
        return BlockWake.timedOut;
    gBlockedClients++; // INFO clients: parked in a blocking wait
    c.blocked = true; // eligible for CLIENT UNBLOCK while parked here
    scope (exit)
    {
        gBlockedClients--;
        c.blocked = false;
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
        if (peerGone(c)) // peer vanished while we were parked (EOF probe)
            return BlockWake.disconnected;
        if (timeoutMs != 0 && remainingMs <= 0)
            return BlockWake.timedOut;
        // else: poll tick elapsed with no event — loop and wait again
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
                if (gAof.enabled)
                    gAof.append(synth.data);
                gWriteEpoch++;
                gKeyActivity.emit();
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

    static ByteBuffer synth; // TLS
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

    static ByteBuffer attempt; // TLS
    auto ec = gKeyActivity.emitCount;
    long remaining = cast(long) timeoutMs;
    bool firstPass = true;
    for (;;)
    {
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
        auto nil = gRespProto >= 3 ? "_\r\n" : "*-1\r\n"; // XREAD nil per protocol
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
        if (c.inMulti || c.inExec || !waitForActivity(ec, remaining, timeoutMs))
        {
            o.append(nil);
            return;
        }
    }
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
            if (gAof.enabled)
                gAof.append(synth.data);
            gWriteEpoch++;
            gKeyActivity.emit();
            return;
        }
        if (!waitForActivity(ec, remaining, timeoutMs))
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
    gKeyActivity.emit();
    if (!gAof.enabled)
        return;
    static ByteBuffer eff; // TLS
    eff.clear();
    repArrayHeader(eff, 2);
    repBulk(eff, verb);
    repBulk(eff, key);
    gAof.append(eff.data);
}

private void unregisterMonitor(Conn* c) nothrow
{
    foreach (i; 0 .. gMonitorCount)
    {
        if (gMonitors[i] is c)
        {
            gMonitors[i] = gMonitors[gMonitorCount - 1];
            gMonitorCount--;
            return;
        }
    }
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
    foreach (i; 0 .. gMonitorCount)
    {
        if (gMonitors[i] !is &from)
            connSink(cast(void*) gMonitors[i], m);
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
                    gPauseEvt.emit(); // re-arm any fiber parked on a prior window
                }
                repSimple(o, "OK");
            }
        }
    }
    else if (eqICDebug(sub, "UNPAUSE") && args.length == 1)
    {
        gPauseUntilMs = 0; // lift the barrier
        gPauseEvt.emit(); // wake parked fibers to replay their held commands
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

private void pubsubIntrospect(const(RVal)[] args, ref ByteBuffer o) nothrow
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
    case "HELP":
        {
            repHelp!"PUBSUB"(o);
            break;
        }
    default:
        repUnknownSubcommand(o, "PUBSUB", sub);
    }
}
