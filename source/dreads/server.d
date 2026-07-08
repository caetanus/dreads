module dreads.server;

// TCP front-end on vibe-core (fiber per connection, single-threaded event
// loop) feeding the @nogc data plane: ByteBuffer I/O staging, zero-copy RESP
// parsing with a per-connection Arena, and command dispatch against the typed
// keyspace. Pub/sub pushes are written by the publisher's fiber directly to
// the subscriber's socket, serialized by a per-connection write mutex.
// vibe-core owns only the socket lifecycle; nothing here allocates on the GC
// heap per request (the mutex and TCPConnection are one-time per connection).

import core.stdc.stdio : printf;

import core.time : seconds;

import vibe.core.core : runEventLoop, setTimer;
import vibe.core.net : TCPConnection, listenTCP;
import vibe.core.stream : IOMode;
import vibe.core.sync : LocalManualEvent, TaskMutex, createManualEvent;

import dreads.aof : Aof, aofLoad, aofRewrite;
import dreads.commands : dispatch, globMatch, isWriteCommand, propagationOverride;
import dreads.config : applyDirective, gConfig, parseMemory;
import dreads.mem : Arena, ByteBuffer;
import dreads.obj : Keyspace;
import dreads.pubsub : PubSub, Subscriber;
import dreads.replicator : gReplicator;
import dreads.resp;
import dreads.scripting : cachedScript, evalCommand, scriptCommand;

private enum READ_CHUNK = 16 * 1024;

// The event loop is single-threaded, so shared state needs no locking.
private __gshared Keyspace gKeys;
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
        import dreads.obj : lruClock;
        import dreads.stream : nowMs;

        lruClock = cast(uint)(nowMs() / 1000);
        setTimer(1.seconds, delegate() @trusted nothrow {
            lruClock = cast(uint)(nowMs() / 1000);
            gAof.fsyncNow();
        }, true);
    }
    initReplication();
    listenTCP(port, delegate(TCPConnection conn) @trusted nothrow {
        serveClient(conn);
    });
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
    cfg.electionTimeoutTicks = 10; // 20ms tick -> ~200-400ms randomized
    cfg.heartbeatTicks = 2; // ~40ms
    cfg.joinMode = gConfig.raftJoin; // passive learner until a config adds us
    auto raftPort = gConfig.raftPort != 0 ? gConfig.raftPort : cast(ushort)(gConfig.port + 10_000);
    string base = gConfig.appendfilename.length ? gConfig.appendfilename : "dreads";
    gReplicator = new Replicator(cfg, peers, raftPort, base ~ ".raft", &gKeys);
    gReplicator.start();
    printf("dreads: raft node %u active on port %u (%zu peers)\n",
            cast(uint) gConfig.raftNodeId, cast(uint) raftPort, peers.length);
}

private struct Conn
{
    TCPConnection tcp;
    TaskMutex wlock;
    Subscriber sub;
    Subscriber shardSub;
    ulong id;
    const(char)[] clientName; // malloc'd
    // MULTI state: queued raw commands, back to back
    bool inMulti;
    size_t multiCount;
    ByteBuffer multiQueue;
    // WATCH state: conservative — any write since WATCH aborts EXEC
    bool watching;
    ulong watchEpoch;

    @property size_t totalSubs() const @nogc nothrow
    {
        return sub.subCount + shardSub.subCount;
    }
}

/// Pub/sub delivery sink: runs on the *publisher's* fiber, so it must take
/// the target connection's write lock to avoid interleaving with its replies.
private void connSink(void* ctx, scope const(ubyte)[] bytes) nothrow
{
    auto c = cast(Conn*) ctx;
    try
    {
        c.wlock.lock();
        scope (exit)
            c.wlock.unlock();
        c.tcp.write(bytes);
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
    Conn c;
    c.tcp = tcp;
    c.id = ++gClientIds;
    c.sub.ctx = &c;
    c.sub.sink = &connSink;
    c.shardSub.ctx = &c;
    c.shardSub.sink = &connSink;
    scope (exit)
    {
        gPubSub.dropAll(&c.sub);
        gShardPubSub.dropAll(&c.shardSub);
        c.sub.free();
        c.shardSub.free();
        unregisterMonitor(&c);
        import dreads.mem : freeSlice;

        c.clientName.freeSlice;
    }
    try
    {
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
                keep = handleCommand(c, cmd, inb.data[cmdStart .. pos], outb, arena);
                arena.reset();
            }
            inb.consume(pos);
            gAof.flush();

            if (!outb.empty)
            {
                c.wlock.lock();
                scope (exit)
                    c.wlock.unlock();
                tcp.write(outb.data);
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

/// Transaction control plus queueing, then the executor. rawCmd holds the
/// command's original RESP bytes for AOF logging and MULTI queueing.
private bool handleCommand(ref Conn c, const ref RVal cmd, scope const(ubyte)[] rawCmd,
        ref ByteBuffer o, ref Arena arena) nothrow
{
    if (cmd.type != RType.Array || cmd.arr.length == 0)
        return dispatch(cmd, gKeys, o, arena);
    foreach (ref a; cmd.arr)
    {
        if (a.type != RType.BulkString && a.type != RType.SimpleString)
            return dispatch(cmd, gKeys, o, arena);
    }
    auto name = cmd.arr[0].str;
    char[16] nbuf = void;
    if (name.length > nbuf.length)
        return dispatch(cmd, gKeys, o, arena);
    foreach (i, ch; name)
        nbuf[i] = ch >= 'a' && ch <= 'z' ? cast(char)(ch - 32) : ch;
    auto uname = cast(string) nbuf[0 .. name.length];

    switch (uname)
    {
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
        return dispatch(cmd, gKeys, o, arena);
    foreach (i, ch; name)
        nbuf[i] = ch >= 'a' && ch <= 'z' ? cast(char)(ch - 32) : ch;
    auto uname = cast(string) nbuf[0 .. name.length];

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
            if (args.length >= 1 && args[0].str != "2")
            {
                repError(o,
                        "NOPROTO unsupported protocol version");
                return true;
            }
            repArrayHeader(o, 14);
            repBulk(o, "server");
            repBulk(o, "redis");
            repBulk(o, "version");
            repBulk(o, "7.4.0");
            repBulk(o, "proto");
            repInt(o, 2);
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
            if (args.length >= 2 && eqICDebug(args[0].str, "SLEEP"))
            {
                import core.time : msecs;
                import std.conv : to;
                import vibe.core.core : vsleep = sleep;

                double secs = 0;
                try
                    secs = (cast(string) args[1].str).to!double;
                catch (Exception)
                {
                }
                try
                    vsleep(msecs(cast(long)(secs * 1000)));
                catch (Exception)
                {
                }
                repSimple(o, "OK");
            }
            else if (args.length >= 1 && eqICDebug(args[0].str, "SET-ACTIVE-EXPIRE"))
                repSimple(o, "OK");
            else if (args.length >= 1 && eqICDebug(args[0].str, "JMAP"))
                repSimple(o, "OK");
            else
                repError(o, "ERR DEBUG subcommand not supported");
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
            evalCommand(args, gKeys, o, arena, bySha, readOnly);
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
            if (!gReplicator.isLeader)
            {
                repError(o, "READONLY You can't write against a read only replica.");
                return true;
            }
            import dreads.stream : nowMs;

            // log [clock][raw command] and block until it commits + applies
            try
                gReplicator.proposeWrite(rawCmd, nowMs(), o);
            catch (Exception)
                repError(o, "ERR replication error");
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
    auto keep = dispatch(cmd, gKeys, o, arena);
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
            "maxmemory-policy", "lua-time-limit", "lua-memory-limit",
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
        repArrayHeader(o, matches * 2);
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
            default:
                auto n = snprintf(b.ptr, b.length, "%llu", gConfig.luaMemoryLimit);
                repBulk(o, b[0 .. n]);
            }
        }
        return;
    }
    if (eqICDebug(args[0].str, "SET") && args.length == 3)
    {
        // only runtime-safe parameters are settable
        if (eqICDebug(args[1].str, "MAXMEMORY") || eqICDebug(args[1].str, "MAXMEMORY-POLICY")
                || eqICDebug(args[1].str, "LUA-TIME-LIMIT")
                || eqICDebug(args[1].str, "LUA-MEMORY-LIMIT"))
        {
            string name, value;
            try
            {
                name = (cast(string) args[1].str).idup;
                value = (cast(string) args[2].str).idup;
            }
            catch (Exception)
            {
            }
            import std.uni : toLower;

            bool ok = false;
            try
                ok = applyDirective(name.toLower, value, gConfig);
            catch (Exception)
            {
            }
            if (ok)
                repSimple(o, "OK");
            else
                repError(o, "ERR Invalid argument");
            return;
        }
        repError(o, "ERR Unsupported CONFIG parameter");
        return;
    }
    if (eqICDebug(args[0].str, "REWRITE") || eqICDebug(args[0].str, "RESETSTAT"))
    {
        repSimple(o, "OK");
        return;
    }
    repError(o, "ERR Unknown CONFIG subcommand");
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
    foreach (i; 0 .. gMonitorCount)
    {
        if (gMonitors[i] !is &from)
            connSink(cast(void*) gMonitors[i], line.data);
    }
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
        auto n = snprintf(b.ptr, b.length, "id=%llu addr=? name=%.*s db=0 cmd=client\n",
                c.id, cast(int) c.clientName.length, c.clientName.ptr);
        repBulk(o, b[0 .. n]);
    }
    else if (eqICDebug(sub, "NO-EVICT") || eqICDebug(sub, "NO-TOUCH")
            || eqICDebug(sub, "SETINFO"))
        repSimple(o, "OK");
    else
        repError(o, "ERR Unknown CLIENT subcommand");
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
    repArrayHeader(o, 3);
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
        repError(o, "ERR Unknown PUBSUB subcommand");
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
        repError(o, "ERR Unknown PUBSUB subcommand");
    }
}
