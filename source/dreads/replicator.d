module dreads.replicator;

// Raft replication for dreads — instantiated ONLY when raft-node-id != 0.
// Standalone dreads never constructs this (gReplicator stays null) and pays
// nothing: the write path is byte-identical to before.
//
// The log entry is [u64 clock][raw RESP command]. The leader stamps the
// frozen clock and appends the RAW command WITHOUT executing it (Raft only
// mutates state on commit). On commit every replica applies raw+clock through
// the same @nogc dispatch with the injected clock, so time/state-dependent
// commands (EXPIRE, XADD *, SPOP) resolve identically. The proposing client
// waits until its entry commits+applies and gets that reply.
//
// Concurrency: one TaskMutex serializes RaftNode access across fibers; it is
// released during the async-durability await so appends from concurrent
// cycles can batch behind one fsync (the loop keeps serving reads meanwhile).

import core.time : msecs;

import vibe.core.core : runTask, setTimer;
import vibe.core.sync : createManualEvent, LocalManualEvent, TaskMutex;

import raft.node : Config, NOOP_PAYLOAD, RaftNode;
import raft.types;
import raft.vibetransport : PeerAddress, VibeTransport;
import raft.wire;

import dreads.mem : Arena, ByteBuffer;
import dreads.obj : Keyspace;
import dreads.raftlog : RaftLog;
import dreads.resp;

/// Installed by the server when replication is configured; null = standalone.
public __gshared Replicator gReplicator;

private struct Pending
{
    ByteBuffer reply;
    LocalManualEvent done;
    bool ready;
}

final class Replicator
{
    private RaftLog log;
    private RaftNode* node;
    private VibeTransport transport;
    private Keyspace* keys;
    private TaskMutex nodeMtx;
    private Pending*[Index] pending;
    private ushort raftPort;

    this(Config cfg, PeerAddress[] peers, ushort raftPort, scope const(char)[] logBase, Keyspace* keys)
    {
        this.keys = keys;
        this.raftPort = raftPort;
        log = RaftLog.open(logBase);
        assert(log !is null, "cannot open raft log");
        log.enableAsyncDurability();
        node = new RaftNode(cfg, log);
        transport = new VibeTransport(cfg.self, peers);
        transport.setHandler(&onWire);
        nodeMtx = new TaskMutex;
    }

    void start()
    {
        transport.start(raftPort);
        setTimer(20.msecs, () @trusted nothrow { onTick(); }, true);
    }

    @property bool isLeader() nothrow
    {
        return node.currentRole == Role.leader;
    }

    @property NodeId leaderId() nothrow
    {
        return node.currentLeader;
    }

    /// Current voting members (for RAFT STATUS).
    const(NodeId)[] members() nothrow
    {
        return node.members;
    }

    @property Index snapshotIndex() nothrow
    {
        return log.snapshotIndex;
    }

    /// Forces log compaction now (RAFT COMPACT / ops): snapshots the keyspace
    /// and discards the covered entries.
    void forceCompact()
    {
        import dreads.aof : dumpKeyspace;

        auto upto = node.commitIndex;
        if (upto == 0)
            return;
        static ByteBuffer dump;
        dump.clear();
        dumpKeyspace(*keys, dump);
        nodeMtx.lock();
        node.compact(upto, dump.data);
        nodeMtx.unlock();
    }

    /// Adds a peer's address so we can reach it, then proposes a joint config
    /// that includes it. Leader-only. `newMembers` is the full target set.
    bool changeMembership(scope const(NodeId)[] newMembers, scope const(PeerAddress)[] newPeers)
    {
        nodeMtx.lock();
        scope (exit)
            nodeMtx.unlock();
        foreach (ref p; newPeers)
            transport.addPeer(p);
        return node.changeMembership(newMembers);
    }

    // --- client write: propose [clock][rawCmd], await commit+apply ---

    /// Returns false when this node is not the leader (caller redirects).
    bool proposeWrite(scope const(ubyte)[] rawCmd, ulong clock, ref ByteBuffer o)
    {
        // entry payload = 8-byte clock header + raw command bytes
        ubyte[] payload = new ubyte[8 + rawCmd.length];
        foreach (i; 0 .. 8)
            payload[i] = cast(ubyte)(clock >> (8 * i));
        payload[8 .. $] = rawCmd;

        nodeMtx.lock();
        auto idx = node.propose(payload);
        if (idx == 0)
        {
            nodeMtx.unlock();
            return false;
        }
        auto slot = new Pending;
        slot.done = createManualEvent();
        pending[idx] = slot;
        auto rd = node.takeReady();
        nodeMtx.unlock();

        commitReady(rd);

        // wait until our entry is committed and applied (its reply is filled)
        while (!slot.ready)
        {
            auto ec = slot.done.emitCount;
            if (slot.ready)
                break;
            slot.done.wait(ec);
        }
        o.append(slot.reply.data);
        pending.remove(idx);
        return true;
    }

    // --- the Ready cycle shared by proposals, incoming messages, and ticks ---

    private void commitReady(Ready rd)
    {
        // a leader's snapshot must replace the keyspace before anything else
        if (rd.applySnapshot !is null)
            loadSnapshot(rd.applySnapshot.data);
        if (rd.persistUpto > 0)
            log.awaitDurable(rd.persistUpto); // yields; loop serves others
        nodeMtx.lock();
        node.onPersisted(rd.persistUpto);
        auto committed = node.takeCommitted();
        nodeMtx.unlock();
        foreach (ref m; rd.messages)
            transport.send(m);
        applyCommitted(committed);
        maybeCompact();
    }

    /// Compacts the raft log once it grows past a threshold: dumps the
    /// keyspace as a snapshot (the BGREWRITEAOF canonical form) and discards
    /// the covered entries, collapsing dead history (SET-then-DEL, expired,
    /// overwrites) into the minimal state.
    private enum COMPACT_THRESHOLD = 10_000;
    private void maybeCompact() nothrow
    {
        try
        {
            auto applied = node.commitIndex;
            if (applied <= log.snapshotIndex + COMPACT_THRESHOLD)
                return;
            static ByteBuffer dump;
            dump.clear();
            import dreads.aof : dumpKeyspace;

            dumpKeyspace(*keys, dump);
            nodeMtx.lock();
            node.compact(applied, dump.data);
            nodeMtx.unlock();
        }
        catch (Exception)
        {
        }
    }

    /// Replaces the keyspace with a snapshot (a canonical command dump).
    private void loadSnapshot(scope const(ubyte)[] data) nothrow
    {
        try
        {
            keys.clear();
            Arena arena;
            ByteBuffer sink;
            size_t pos = 0;
            auto bytes = data;
            while (pos < bytes.length)
            {
                RVal cmd;
                if (parseValue(bytes, pos, arena, cmd) != ParseStatus.ok)
                    break;
                import dreads.commands : dispatch;

                sink.clear();
                dispatch(cmd, *keys, sink, arena);
                arena.reset();
            }
        }
        catch (Exception)
        {
        }
    }

    private void onTick() nothrow
    {
        try
        {
            nodeMtx.lock();
            node.tick();
            auto rd = node.takeReady();
            nodeMtx.unlock();
            commitReady(rd);
        }
        catch (Exception)
        {
        }
    }

    private void onWire(NodeId from, MsgKind kind, scope const(ubyte)[] body_) nothrow
    {
        try
        {
            nodeMtx.lock();
            final switch (kind)
            {
            case MsgKind.requestVote:
                RequestVote m;
                if (decodeRequestVote(body_, m))
                    node.onRequestVote(from, m);
                break;
            case MsgKind.requestVoteReply:
                RequestVoteReply m;
                if (decodeRequestVoteReply(body_, m))
                    node.onRequestVoteReply(from, m);
                break;
            case MsgKind.appendEntries:
                AppendEntries m;
                if (decodeAppendEntries(body_, m))
                    node.onAppendEntries(from, m);
                break;
            case MsgKind.appendEntriesReply:
                AppendEntriesReply m;
                if (decodeAppendEntriesReply(body_, m))
                    node.onAppendEntriesReply(from, m);
                break;
            case MsgKind.installSnapshot:
                InstallSnapshot m;
                if (decodeInstallSnapshot(body_, m))
                    node.onInstallSnapshot(from, m);
                break;
            case MsgKind.installSnapshotReply:
                InstallSnapshotReply m;
                if (decodeInstallSnapshotReply(body_, m))
                    node.onInstallSnapshotReply(from, m);
                break;
            }
            auto rd = node.takeReady();
            nodeMtx.unlock();
            commitReady(rd);
        }
        catch (Exception)
        {
        }
    }

    /// Applies committed entries to the keyspace, resolving each against its
    /// logged clock; fills the reply slot for a locally-proposed entry.
    private void applyCommitted(const(LogEntry)[] committed) nothrow
    {
        foreach (ref e; committed)
        {
            if (e.payload == NOOP_PAYLOAD || e.payload.length < 8)
                continue;
            ulong clock = 0;
            foreach (i; 0 .. 8)
                clock |= cast(ulong) e.payload[i] << (8 * i);
            auto raw = cast(const(ubyte)[]) e.payload[8 .. $];

            Arena arena;
            ByteBuffer reply;
            RVal cmd;
            size_t pos = 0;
            if (parseValue(raw, pos, arena, cmd) != ParseStatus.ok)
                continue;
            import dreads.commands : dispatch;

            dispatch(cmd, *keys, reply, arena, clock); // injected clock
            auto pp = e.index in pending;
            if (pp !is null)
            {
                auto slot = *pp;
                slot.reply.clear();
                slot.reply.append(reply.data);
                slot.ready = true;
                slot.done.emit();
            }
        }
    }
}
