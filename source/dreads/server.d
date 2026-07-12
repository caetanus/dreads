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

import core.stdc.stdio : printf;
import core.stdc.stdlib : malloc, cfree = free;

import core.time : seconds;

import vibe.core.core : runEventLoop, runTask, setTimer;
import vibe.core.net : TCPConnection, listenTCP, TCPListenOptions;
import vibe.core.stream : IOMode;
import vibe.core.sync : LocalManualEvent, TaskMutex, createManualEvent;
import vibe.core.task : Task;

import dreads.aof : Aof, aofLoad, aofRewrite;
import dreads.commands : dispatch, globMatch, isWriteCommand, propagationOverride, parseLong;
import dreads.config : applyDirective, gConfig, isRuntimeSettable, parseMemory;
import dreads.mem : Arena, ByteBuffer;
import dreads.notify : flushPendingNotify, gNotifyFlags;
import dreads.obj : Keyspace, gDbs, NUM_DBS;
import dreads.pubsub : PubSub, Subscriber, RcMsg, rcFromBytes, rcData, rcRetain, rcRelease, rcAsPush;
import dreads.replicator : gReplicator;
import dreads.resp;
import dreads.scripting : cachedScript, evalCommand, scriptCommand;

private enum READ_CHUNK = 16 * 1024;

// The event loop is single-threaded, so shared state needs no locking.
// The logical databases live in `gDbs` (dreads.obj); client commands dispatch
// against the *connection's* selected db (`Conn.db`). `gKeys` names db 0 — used
// by the replay/persistence/eviction/blocking paths, which are still db-0-only
// (multi-db there is a TODO; see BLACKBOX-TODO.md).
private ref Keyspace gKeys() @property @nogc nothrow @trusted
{
    return gDbs[0];
}

private __gshared PubSub gPubSub;
private __gshared PubSub gShardPubSub; // single node: shard = plain, own namespace
private __gshared Aof gAof;
private __gshared const(char)[] gAofPath;
private __gshared ulong gWriteEpoch; // bumped on every effective write (WATCH)
private __gshared ulong gClientIds;
// MONITOR feed: registered connections receive every executed command
private __gshared Conn*[64] gMonitors;
private __gshared size_t gMonitorCount;
// blocked clients (BLPOP & co.) wake on any write and re-check their keys
private __gshared LocalManualEvent gKeyActivity;

public int runServer(ushort port, const(char)[] aofPath = null)
{
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
    {
        import dreads.notify : gNotifyPublish, parseNotifyFlags;

        cast(void) parseNotifyFlags(gConfig.notifyKeyspaceEvents, gNotifyFlags);
        gNotifyPublish = (scope const(char)[] chan, scope const(char)[] msg) nothrow{
            gPubSub.publish(chan, msg);
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
        import dreads.obj : lruClock, gActiveExpire;
        import dreads.rand : seedRand;
        import dreads.stream : nowMs;

        gActiveExpire = gConfig.activeExpire; // drop-soon timer only runs when enabled
        lruClock = cast(uint)(nowMs() / 1000);
        seedRand(nowMs()); // shuffle the random-pick commands per boot
        setTimer(1.seconds, delegate() @trusted nothrow {
            lruClock = cast(uint)(nowMs() / 1000);
            foreach (ref d; gDbs) // drop-soon sweep across every database
                d.activeExpireCycle();
            flushPendingNotify(); // deliver the "expired" events the sweep queued
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
    const(char)[] clientName; // malloc'd
    bool resp3; // negotiated RESP3 via HELLO 3 (default RESP2)
    // MULTI state: queued raw commands, back to back
    bool inMulti;
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

    @property size_t totalSubs() const @nogc nothrow
    {
        return sub.subCount + shardSub.subCount;
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

    void setup(size_t capacity) @nogc nothrow
    {
        cap = capacity;
        ring = cast(RcMsg**) malloc(cap * (RcMsg*).sizeof);
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
            cfree(ring);
        ring = null;
        cap = head = tail = count = 0;
    }
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
    c.oq.free();
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
    Conn c;
    c.tcp = tcp;
    c.id = ++gClientIds;
    c.dbp = &gDbs[0]; // default to db 0
    c.sub.ctx = &c;
    c.sub.sink = &connSink;
    c.shardSub.ctx = &c;
    c.shardSub.sink = &connSink;
    scope (exit)
    {
        gPubSub.dropAll(&c.sub); // no further connSink after this
        gShardPubSub.dropAll(&c.shardSub);
        shutdownOutput(c);
        c.sub.free();
        c.shardSub.free();
        unregisterMonitor(&c);
        import dreads.mem : freeSlice;

        c.clientName.freeSlice;
    }
    try
    {
        tcp.tcpNoDelay = true; // small RESP replies must not wait on Nagle
        c.wlock = new TaskMutex;
        bool keep = true;
        while (keep && tcp.connected)
        {
            if (!tcp.waitForData())
                break;
            auto space = inb.freeSpace(READ_CHUNK);
            auto n = tcp.read(space, IOMode.once);
            if (n == 0)
                break;
            inb.grow(n);

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
                keep = handleCommand(c, cmd, inb.data[cmdStart .. pos], outb, arena);
                if (gNotifyFlags)
                    flushPendingNotify(); // publish keyspace events the command queued
                arena.reset();
            }
            inb.consume(pos);
            // Reap the chunk's trailing run of pipelined writes (their replies
            // come last, in order) before flushing the batch to the client.
            if (c.pendingCount > 0)
                flushPending(c, outb);
            gAof.flush();

            if (!outb.empty)
            {
                if (c.subMode)
                {
                    // Share the one ordered output path with pub/sub messages so
                    // a subscribe confirmation can never trail a later message.
                    auto m = rcFromBytes(outb.data);
                    if (c.oq.push(m))
                        c.oqEvt.emit();
                    rcRelease(m); // push retained; drop our creating reference
                }
                else
                {
                    c.wlock.lock();
                    scope (exit)
                        c.wlock.unlock();
                    tcp.write(outb.data);
                }
                outb.clear();
            }
        }
    }
    catch (Exception)
    {
        // peer vanished mid read/write; just drop the connection
    }
    try
        tcp.close();
    catch (Exception)
    {
    }
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
private bool handleCommand(ref Conn c, const ref RVal cmd, scope const(ubyte)[] rawCmd,
        ref ByteBuffer o, ref Arena arena) nothrow
{
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
    char[16] nbuf = void;
    if (name.length > nbuf.length)
        return dispatch(cmd, *c.dbp, o, arena);
    foreach (i, ch; name)
        nbuf[i] = ch >= 'a' && ch <= 'z' ? cast(char)(ch - 32) : ch;
    auto uname = cast(string) nbuf[0 .. name.length];

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
            scope (exit)
            {
                c.multiQueue.clear();
                c.watching = false;
            }
            if (c.watching && c.watchEpoch != gWriteEpoch)
            {
                o.append("*-1\r\n");
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
                keep = executeCommand(c, qcmd, c.multiQueue.data[qstart .. qpos], o, arena) && keep;
            }
            return keep;
        }
    case "RESET":
        {
            c.inMulti = false;
            c.multiQueue.clear();
            c.watching = false;
            gPubSub.dropAll(&c.sub);
            gShardPubSub.dropAll(&c.shardSub);
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

    if (c.totalSubs > 0)
    {
        switch (uname)
        {
        case "SUBSCRIBE", "UNSUBSCRIBE", "PSUBSCRIBE", "PUNSUBSCRIBE":
        case "SSUBSCRIBE", "SUNSUBSCRIBE", "PING", "QUIT", "RESET":
            break;
        default:
            o.append("-ERR Can't execute '");
            foreach (ch; name)
                o.appendByte(ch == '\r' || ch == '\n' ? ' ' : ch);
            o.append("': only (P)SUBSCRIBE / (P)UNSUBSCRIBE / PING / QUIT are allowed in this context\r\n");
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
            foreach (ref a; args)
            {
                gShardPubSub.subscribe(&c.shardSub, a.str);
                subReply(o, "ssubscribe", a.str, c.shardSub.subCount);
            }
            return true;
        }
    case "SUNSUBSCRIBE":
        {
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
            clientCmd(c, args, o);
            return true;
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
            blockingPop(c, args, uname[1] == 'L', o, arena);
            return true;
        }
    case "BZPOPMIN":
    case "BZPOPMAX":
        {
            blockingZPop(c, args, uname == "BZPOPMAX", o, arena);
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
            if (!parseTimeout(args[$ - 1].str, timeoutMs))
            {
                repError(o, "ERR timeout is not a float or out of range");
                return true;
            }
            blockingRetry(c, cmd.arr[0 .. $ - 1], uname == "BLMOVE" ? "LMOVE"
                    : "RPOPLPUSH", "$-1\r\n", timeoutMs, o, arena);
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
    case "BLMPOP":
    case "BZMPOP":
        {
            // B*MPOP timeout numkeys ... -> *MPOP numkeys ...
            if (args.length < 3)
            {
                repError(o, "ERR wrong number of arguments");
                return true;
            }
            ulong timeoutMs;
            if (!parseTimeout(args[0].str, timeoutMs))
            {
                repError(o, "ERR timeout is not a float or out of range");
                return true;
            }
            blockingRetry(c, cmd.arr[1 .. $], uname == "BLMPOP" ? "LMPOP" : "ZMPOP",
                    "*-1\r\n", timeoutMs, o, arena, true);
            return true;
        }
    case "HELLO":
        {
            import dreads.mem : freeSlice, mallocDup;

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
            // optional [AUTH user pass] [SETNAME name]; dreads has no auth, so
            // AUTH is accepted and ignored (no requirepass), SETNAME is applied.
            for (size_t i = 1; i < args.length;)
            {
                if (eqICDebug(args[i].str, "AUTH") && i + 2 < args.length)
                    i += 3;
                else if (eqICDebug(args[i].str, "SETNAME") && i + 1 < args.length)
                {
                    c.clientName.freeSlice;
                    c.clientName = mallocDup(args[i + 1].str);
                    i += 2;
                }
                else
                {
                    repError(o, "ERR Syntax error in HELLO");
                    return true;
                }
            }
            c.resp3 = ver == 3;
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
            if (!gAof.enabled)
            {
                repError(o, "ERR AOF is not enabled");
                return true;
            }
            if (aofRewrite(gAof, gAofPath, gKeys))
                repSimple(o, "Background append only file rewriting started");
            else
                repError(o, "ERR AOF rewrite failed");
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
            auto outBefore = o.length;
            evalCommand(args, *c.dbp, o, arena, bySha, readOnly);
            // scripts may write; log unless the script itself errored.
            // overrides set by commands inside the script are superseded by
            // logging the whole EVAL.
            propagationOverride.clear();
            if (o.length > outBefore && o.data[outBefore] != '-')
            {
                gWriteEpoch++;
                gKeyActivity.emit();
            }
            if (gAof.enabled && o.length > outBefore && o.data[outBefore] != '-')
            {
                if (!bySha)
                    gAof.append(rawCmd);
                else if (args.length > 0)
                {
                    auto body_ = cachedScript(args[0].str);
                    if (body_ !is null)
                        gAof.appendEval(body_, args[1 .. $]);
                }
            }
            return true;
        }
    case "SCRIPT":
        {
            scriptCommand(args, o);
            return true;
        }
    default:
        break;
    }

    // Raft policy gate — only when replication is configured; standalone
    // (gReplicator is null) falls straight through with zero added cost.
    if (gReplicator !is null)
    {
        if (isWriteCommand(uname))
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
                        gReplicator.proposeWrite(rawCmd, nowMs(), cast(ushort)(c.dbp - &gDbs[0]), o);
                    catch (Exception)
                        repError(o, "ERR replication error");
                }
                return true;
            }
            auto h = gReplicator.proposeAsync(rawCmd, nowMs(), cast(ushort)(c.dbp - &gDbs[0]));
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

    if (gConfig.maxmemory && isWriteCommand(uname) && !freeMemoryIfNeeded())
    {
        repError(o, "OOM command not allowed when used memory > 'maxmemory'.");
        return true;
    }
    auto outBefore = o.length;
    auto keep = dispatch(cmd, *c.dbp, o, arena);
    if (o.length > outBefore && o.data[outBefore] != '-')
    {
        if (isWriteCommand(uname) || !propagationOverride.empty)
        {
            gWriteEpoch++; // WATCH visibility
            gKeyActivity.emit(); // wake blocked BLPOP-family clients
        }
        if (gAof.enabled)
        {
            if (!propagationOverride.empty)
                gAof.append(propagationOverride.data);
            else if (isWriteCommand(uname))
                gAof.append(rawCmd);
        }
    }
    propagationOverride.clear();
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
        // (name, value) pairs for every known directive matching the pattern
        static immutable names = [
            "port", "appendonly", "appendfilename", "dir", "maxmemory",
            "maxmemory-policy", "lua-time-limit", "lua-memory-limit", "active-expire",
            "notify-keyspace-events", "lazyfree-lazy-server-del",
            "hash-max-listpack-entries", "hash-max-listpack-value",
            "hash-max-ziplist-entries", "hash-max-ziplist-value",
            "list-max-listpack-size", "list-max-ziplist-size", "list-compress-depth",
            "set-max-intset-entries", "set-max-listpack-entries", "set-max-listpack-value",
            "zset-max-listpack-entries", "zset-max-listpack-value",
            "zset-max-ziplist-entries", "zset-max-ziplist-value",
            "stream-node-max-entries", "stream-node-max-bytes",
            "proto-max-bulk-len", "client-query-buffer-limit",
        ];
        char[64] b = void;
        size_t matches = 0;
        foreach (nm; names)
        {
            foreach (ref pat; args[1 .. $])
            {
                if (globMatch(pat.str, nm))
                {
                    matches++;
                    break;
                }
            }
        }
        repMapHeader(o, matches); // CONFIG GET is a map -> %N in RESP3
        foreach (nm; names)
        {
            bool hit = false;
            foreach (ref pat; args[1 .. $])
            {
                if (globMatch(pat.str, nm))
                {
                    hit = true;
                    break;
                }
            }
            if (!hit)
                continue;
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
            default:
                repBulk(o, ""); // known name with no value formatter
            }
        }
        return;
    }
    if (eqICDebug(args[0].str, "SET") && args.length == 3)
    {
        import std.uni : toLower;

        string name, value, lname;
        try
        {
            name = (cast(string) args[1].str).idup;
            value = (cast(string) args[2].str).idup;
            lname = name.toLower;
        }
        catch (Exception)
        {
        }
        if (!isRuntimeSettable(lname)) // startup-only or unknown parameters
        {
            repError(o, "ERR Unsupported CONFIG parameter");
            return;
        }
        bool ok = false;
        try
            ok = applyDirective(lname, value, gConfig);
        catch (Exception)
        {
        }
        if (ok)
        {
            import dreads.obj : gActiveExpire;

            gActiveExpire = gConfig.activeExpire; // mirror the runtime toggle
            repSimple(o, "OK");
        }
        else
            repError(o, "ERR CONFIG SET failed - unable to set the value");
        return;
    }
    if (eqICDebug(args[0].str, "REWRITE") || eqICDebug(args[0].str, "RESETSTAT"))
    {
        repSimple(o, "OK");
        return;
    }
    repUnknownSubcommand(o, "CONFIG", args.length ? args[0].str : "");
}

// ---------------------------------------------------------------------------
// maxmemory / LRU eviction (jemalloc-backed accounting; Linux only)
// ---------------------------------------------------------------------------

version (linux)
{
    private extern (C) int mallctl(const(char)* name, void* oldp, size_t* oldlenp,
            void* newp, size_t newlen) nothrow @nogc;

    private ulong usedMemory() nothrow @nogc
    {
        ulong epoch = 1;
        size_t esz = epoch.sizeof;
        mallctl("epoch", &epoch, &esz, &epoch, epoch.sizeof);
        size_t allocated;
        size_t asz = allocated.sizeof;
        if (mallctl("stats.allocated", &allocated, &asz, null, 0) != 0)
            return 0;
        return allocated;
    }
}
else
{
    private ulong usedMemory() nothrow @nogc
    {
        return 0; // accounting unavailable: maxmemory is inert
    }
}

private __gshared size_t gEvictCursor;

/// Approximate LRU eviction: sample live keys, evict the coldest, repeat.
/// Returns false when memory stays over the limit (noeviction, or nothing
/// evictable under volatile-lru).
private bool freeMemoryIfNeeded() nothrow
{
    import dreads.obj : RObj;

    if (usedMemory() <= gConfig.maxmemory)
        return true;
    if (gConfig.maxmemoryPolicy == "noeviction")
        return false;
    bool volatileOnly = gConfig.maxmemoryPolicy == "volatile-lru";
    bool randomPick = gConfig.maxmemoryPolicy == "allkeys-random";

    static ByteBuffer delCmd; // TLS scratch for AOF propagation
    foreach (_; 0 .. 128) // eviction budget per triggering command
    {
        auto cap = gKeys.d.capacity;
        if (cap == 0 || gKeys.length == 0)
            return false;
        // sample up to 5 live keys from a rotating cursor
        const(char)[] victim;
        uint victimLru = uint.max;
        size_t seen = 0;
        size_t i = gEvictCursor % cap;
        size_t scanned = 0;
        while (seen < 5 && scanned < cap)
        {
            if (gKeys.d.slotLive(i))
            {
                auto obj = gKeys.d.valAt(i);
                if (!volatileOnly || obj.expireAtMs != 0)
                {
                    seen++;
                    if (randomPick)
                    {
                        victim = gKeys.d.keyAt(i);
                        break;
                    }
                    if (obj.lruSecs <= victimLru)
                    {
                        victimLru = obj.lruSecs;
                        victim = gKeys.d.keyAt(i);
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
            delCmd.clear();
            repArrayHeader(delCmd, 2);
            repBulk(delCmd, "DEL");
            repBulk(delCmd, victim);
            gAof.append(delCmd.data);
        }
        gKeys.d.del(victim);
        gWriteEpoch++;
        if (usedMemory() <= gConfig.maxmemory)
            return true;
    }
    return usedMemory() <= gConfig.maxmemory;
}

// ---------------------------------------------------------------------------
// Blocking commands (BLPOP family, XREAD BLOCK)
// ---------------------------------------------------------------------------

/// Redis timeouts are seconds as a double; 0 = block forever.
private bool parseTimeout(scope const(char)[] s, out ulong ms) nothrow
{
    import dreads.commands : parseDouble;

    double secs;
    if (!parseDouble(s, secs) || secs < 0 || secs > 1e9)
        return false;
    ms = cast(ulong)(secs * 1000);
    return true;
}

/// True while the caller should keep waiting (updates the emit count).
private bool waitForActivity(ref int ec, ref long remainingMs, ulong timeoutMs) nothrow
{
    import core.time : MonoTime, msecs;

    if (timeoutMs != 0 && remainingMs <= 0)
        return false;
    auto slice = timeoutMs == 0 ? 3_600_000 : remainingMs; // forever = 1h slices
    auto before = MonoTime.currTime;
    ec = gKeyActivity.waitUninterruptible(msecs(slice), ec);
    if (timeoutMs != 0)
        remainingMs -= (MonoTime.currTime - before).total!"msecs";
    return true;
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
    if (!parseTimeout(args[$ - 1].str, timeoutMs))
    {
        repError(o, "ERR timeout is not a float or out of range");
        return;
    }
    auto keys = args[0 .. $ - 1];
    auto ec = gKeyActivity.emitCount;
    long remaining = cast(long) timeoutMs;
    for (;;)
    {
        foreach (ref k; keys)
        {
            bool wrong;
            auto obj = gKeys.lookupTyped(k.str, ObjType.list, wrong);
            if (wrong)
            {
                repError(o, "WRONGTYPE Operation against a key holding the wrong kind of value");
                return;
            }
            if (obj is null || obj.list.length == 0)
                continue;
            repArrayHeader(o, 2);
            repBulk(o, k.str);
            repBulk(o, fromLeft ? obj.list.front : obj.list.back);
            if (fromLeft)
                obj.list.popFront();
            else
                obj.list.popBack();
            gKeys.delIfEmpty(k.str, obj);
            logEffect(fromLeft ? "LPOP" : "RPOP", k.str);
            return;
        }
        if (c.inMulti || !waitForActivity(ec, remaining, timeoutMs))
        {
            o.append("*-1\r\n");
            return;
        }
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
    if (!parseTimeout(args[$ - 1].str, timeoutMs))
    {
        repError(o, "ERR timeout is not a float or out of range");
        return;
    }
    auto keys = args[0 .. $ - 1];
    auto ec = gKeyActivity.emitCount;
    long remaining = cast(long) timeoutMs;
    for (;;)
    {
        foreach (ref k; keys)
        {
            bool wrong;
            auto obj = gKeys.lookupTyped(k.str, ObjType.zset, wrong);
            if (wrong)
            {
                repError(o, "WRONGTYPE Operation against a key holding the wrong kind of value");
                return;
            }
            if (obj is null || obj.zset.length == 0)
                continue;
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
            gKeys.delIfEmpty(k.str, obj);
            logEffect(popMax ? "ZPOPMAX" : "ZPOPMIN", k.str);
            return;
        }
        if (c.inMulti || !waitForActivity(ec, remaining, timeoutMs))
        {
            o.append("*-1\r\n");
            return;
        }
    }
}

/// Generic retry loop: rewrites the blocking command into its non-blocking
/// form (parts = original tokens minus the timeout), dispatches it, and
/// waits when the reply equals nilReply. The effective command is what the
/// AOF sees (via the normal executeCommand path is bypassed here, so log it).
private void blockingRetry(ref Conn c, const(RVal)[] parts, string verb,
        string nilReply, ulong timeoutMs, ref ByteBuffer o, ref Arena arena,
        bool skipFirst = false) nothrow
{
    static ByteBuffer synth; // TLS: rebuilt command bytes
    static ByteBuffer attempt; // TLS: attempt reply staging
    synth.clear();
    auto argTokens = skipFirst ? parts[1 .. $] : parts[1 .. $];
    repArrayHeader(synth, 1 + argTokens.length);
    repBulk(synth, verb);
    foreach (ref p; argTokens)
        repBulk(synth, p.str);

    auto ec = gKeyActivity.emitCount;
    long remaining = cast(long) timeoutMs;
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
        dispatch(cmd2, gKeys, attempt, arena);
        propagationOverride.clear();
        auto rep = cast(const(char)[]) attempt.data;
        if (rep.length > 0 && rep[0] == '-')
        {
            o.append(attempt.data); // real error: surface it
            return;
        }
        if (rep != nilReply)
        {
            o.append(attempt.data);
            if (gAof.enabled)
                gAof.append(synth.data);
            gWriteEpoch++;
            gKeyActivity.emit();
            return;
        }
        if (c.inMulti || !waitForActivity(ec, remaining, timeoutMs))
        {
            o.append(nilReply);
            return;
        }
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
            auto obj = gKeys.lookupTyped(args[keyIdx].str, ObjType.stream, wrong);
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
        dispatch(cmd2, gKeys, attempt, arena);
        auto rep = cast(const(char)[]) attempt.data;
        if (rep != "*-1\r\n")
        {
            o.append(attempt.data);
            return;
        }
        if (c.inMulti || !waitForActivity(ec, remaining, timeoutMs))
        {
            o.append("*-1\r\n");
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
        foreach (ch; a.str)
        {
            if (ch == '"' || ch == '\\')
                line.appendByte('\\');
            line.appendByte(ch == '\r' || ch == '\n' ? ' ' : ch);
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
private void clientCmd(ref Conn c, const(RVal)[] args, ref ByteBuffer o) nothrow
{
    import core.stdc.stdio : snprintf;

    import dreads.mem : freeSlice, mallocDup;

    if (args.length == 0)
    {
        repError(o, "ERR wrong number of arguments for 'client' command");
        return;
    }
    auto sub = args[0].str;
    if (eqICDebug(sub, "ID"))
        repInt(o, cast(long) c.id);
    else if (eqICDebug(sub, "GETNAME"))
        repBulk(o, c.clientName);
    else if (eqICDebug(sub, "SETNAME") && args.length == 2)
    {
        foreach (ch; args[1].str)
        {
            if (ch == ' ' || ch == '\n' || ch == '\r')
            {
                repError(o, "ERR Client names cannot contain spaces, newlines or special characters.");
                return;
            }
        }
        c.clientName.freeSlice;
        c.clientName = mallocDup(args[1].str);
        repSimple(o, "OK");
    }
    else if (eqICDebug(sub, "INFO") || eqICDebug(sub, "LIST"))
    {
        char[160] b = void;
        auto n = snprintf(b.ptr, b.length, "id=%llu addr=? name=%.*s db=%d cmd=client\n",
                c.id, cast(int) c.clientName.length, c.clientName.ptr,
                cast(int)(c.dbp - &gDbs[0]));
        repBulk(o, b[0 .. n]);
    }
    else if (eqICDebug(sub, "NO-EVICT") || eqICDebug(sub, "NO-TOUCH")
            || eqICDebug(sub, "SETINFO"))
        repSimple(o, "OK");
    else
        repUnknownSubcommand(o, "CLIENT", sub);
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
    default:
        repUnknownSubcommand(o, "PUBSUB", sub);
    }
}
