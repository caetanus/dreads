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
import vibe.core.sync : TaskMutex;

import dreads.aof : Aof, aofLoad, aofRewrite;
import dreads.commands : dispatch, isWriteCommand, propagationOverride;
import dreads.mem : Arena, ByteBuffer;
import dreads.obj : Keyspace;
import dreads.pubsub : PubSub, Subscriber;
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
        setTimer(1.seconds, delegate() @trusted nothrow { gAof.fsyncNow(); }, true);
    }
    listenTCP(port, delegate(TCPConnection conn) @trusted nothrow {
        serveClient(conn);
    });
    printf("dreads listening on port %u\n", cast(uint) port);
    return runEventLoop();
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
                gWriteEpoch++;
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

    auto outBefore = o.length;
    auto keep = dispatch(cmd, gKeys, o, arena);
    if (o.length > outBefore && o.data[outBefore] != '-')
    {
        if (isWriteCommand(uname) || !propagationOverride.empty)
            gWriteEpoch++; // WATCH visibility
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
