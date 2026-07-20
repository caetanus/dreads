module dreads.replicator;

// Raft replication for dreads — instantiated ONLY when raft-node-id != 0.
// Standalone dreads never constructs this (gReplicator stays null) and pays
// nothing: the write path is byte-identical to before.
//
// The log entry is [u64 clock][raw RESP command]. The leader stamps the frozen
// clock and appends the RAW command WITHOUT executing it (Raft only mutates
// state on commit). On commit every replica applies raw+clock through the same
// @nogc dispatch with the injected clock, so time/state-dependent commands
// (EXPIRE, XADD *, SPOP) resolve identically.
//
// ── Dedicated raft event loop (producer/consumer) ──────────────────────────
// Consensus runs on its OWN event-loop thread so a client flood on the main
// loop can never starve raft ack-processing or delay heartbeats (which caused
// spurious elections and wild throughput swings). The two threads share NO
// mutable state except three lock-guarded FIFOs and a few atomics:
//
//   main loop (Replicator)                 raft loop (RaftWorker)
//   ──────────────────────                 ──────────────────────
//   proposeWrite: push [clock][cmd] ─propQ─► drain, batch proposeLocal + flush,
//     + slot, block on slot                  replicate; owns node/transport/log
//   changeMembership / compaction ──ctlQ──►  membership + log compaction
//   applyLoop: dispatch → gKeys      ◄commitQ─ ship committed entries + snapshots
//     fill slot.reply, wake client
//
// The keyspace (gKeys) stays single-threaded on the main loop — the raft loop
// never touches it, it only reaches consensus and ships committed payloads
// back for the main loop to apply in log order. Status the hot path needs
// (isLeader/leaderId) is published as atomics; the rare members list behind a
// short mutex. The whole design is deadlock-free: the main applier never pushes
// back into propQ, so it always drains commitQ and the raft loop never blocks
// for long on backpressure.

import core.atomic : atomicLoad, atomicStore;
import core.sync.mutex : Mutex;
import core.time : msecs;

import vibe.core.core : runTask, setTimer;
import vibe.core.sync : createManualEvent, LocalManualEvent, TaskMutex;
import vibe.core.taskpool : TaskPool;

import raft.node : Config, NOOP_PAYLOAD, RaftNode;
import raft.types;
import raft.vibetransport : PeerAddress, VibeTransport;
import raft.wire;

import dreads.mem : Arena, ByteBuffer;
import dreads.obj : Keyspace, gDbs, NUM_DBS;
import dreads.raftlog : RaftLog;
import dreads.raftq : CrossQueue;
import dreads.resp;

/// Installed by the server when replication is configured; null = standalone.
public __gshared Replicator gReplicator;

// In-flight proposals keyed by log index on the raft thread (idx % RING).
// Bounded by concurrent in-flight writes (connections x pipeline depth), far
// below RING; a stale wrap-around is impossible because the slot travels with
// the entry rather than being looked up long after.
private enum RING = 1 << 16;

// Max messages drained from one Ready cycle onto the stack (broadcast to a tiny
// cluster + a reply); any excess is simply re-sent next heartbeat.
private enum MSG_CAP = 64;

// Queue capacities (power of two). Sized far above realistic in-flight depth so
// backpressure is essentially never hit under normal load.
private enum PROP_CAP = 1 << 16;
private enum COMMIT_CAP = 1 << 16;
private enum CTL_CAP = 1 << 8;

// commitQ item discriminator (raft loop -> main loop).
private enum CommitKind : uint
{
    apply = 0, // payload = [clock][rawCmd], meta = index, tag = slot|null
    snapshot = 1, // payload = keyspace snapshot, meta = snapshot index
    fail = 2, // tag = slot; proposal rejected (not leader) — wake with error
    membershipAck = 3, // tag = slot, meta = 1/0 result of a membership change
}

// ctlQ item discriminator (main loop -> raft loop).
private enum CtlKind : uint
{
    membership = 0, // payload = encoded(members + peers), tag = ack slot
    compact = 1, // payload = keyspace snapshot, meta = applied index to compact to
}

// A pending client write. Lives entirely on the main loop: proposeWrite fills
// reqBuf and blocks on `done`; applyLoop fills reply and emits. The raft loop
// only carries the opaque pointer, never dereferencing it.
private struct Pending
{
    ByteBuffer reqBuf; // [clock][rawCmd], stable across the propQ backpressure wait
    ByteBuffer reply;
    LocalManualEvent done;
    Index idx; // log index this proposal was appended at (0 = not yet / control)
    bool ready;
    bool failed; // proposal rejected (leadership lost)
    bool ackResult; // membership-change outcome
}

// ---------------------------------------------------------------------------
// Main-loop façade
// ---------------------------------------------------------------------------

final class Replicator
{
    // Boot parameters — set in the ctor, read by the raft thread after start().
    // Stable for the process lifetime (publication happens-before via thread
    // creation), so no synchronization is needed to read them.
    package Config bootCfg;
    package PeerAddress[] bootPeers;
    package ushort raftPort;
    package string logBase;
    package Keyspace* keys;
    // Compress outbound raft frames (LZ4). Set from `raft-compress` before
    // start(); read once by the raft thread when it wires the transport codec.
    package bool compress;

    // Cross-thread FIFOs.
    package CrossQueue propQ; // main -> raft: proposals
    package CrossQueue ctlQ; // main -> raft: membership / compaction
    package CrossQueue commitQ; // raft -> main: committed entries / snapshots

    // Published status: the raft thread writes, the main loop reads on the hot
    // path (isLeader is checked on every write).
    package shared bool leaderFlag;
    package shared uint leaderIdVal;
    package shared ulong snapIndexVal;
    // Compaction handshake: raft raises this when the log is safe+worthwhile to
    // compact; the main loop (which owns the keyspace) supplies the snapshot.
    package shared bool compactWanted;
    // Highest index the main loop has applied to the keyspace. Published for the
    // raft loop's compaction policy: it decides how much log is reclaimable (a
    // snapshot can only cover up to what the keyspace has actually applied).
    package shared ulong appliedIndexPub;

    // Members snapshot: rare (RAFT STATUS / membership ops), guarded by a short
    // mutex. membersView is filled and returned on the main loop only.
    private Mutex metaMtx;
    package NodeId[64] membersBuf;
    package size_t membersLen;
    private NodeId[64] membersView;

    // Slot pool (main loop only): slots and their events are allocated on demand
    // as the pool grows, then reused forever — never per write (malloc-backed
    // Vec keeps the free list zero-GC).
    private Vec!(Pending*) freeSlots;

    // Applier bookkeeping (main loop only).
    private ulong appliedIndex;
    private ulong lastCompactApplied;

    private shared TaskPool raftPool;

    this(Config cfg, PeerAddress[] peers, ushort raftPort, string logBase, Keyspace* keys)
    {
        this.bootCfg = cfg;
        this.bootPeers = peers;
        this.raftPort = raftPort;
        this.logBase = logBase;
        this.keys = keys;
        metaMtx = new Mutex;
        propQ = new CrossQueue(PROP_CAP);
        ctlQ = new CrossQueue(CTL_CAP);
        commitQ = new CrossQueue(COMMIT_CAP);
    }

    void start()
    {
        // Main-loop applier: drains committed entries into the keyspace.
        runTask(() nothrow { applyLoop(); });
        // Dedicated single-thread event loop for consensus. Its task reads the
        // __gshared gReplicator (this) for the queues and boot params.
        raftPool = new shared TaskPool(1, "raft");
        raftPool.runTaskH(&raftEntry);
    }

    /// Stop and join the raft worker thread. Like the Lua pool, this is a
    /// NON-daemon thread running an infinite event loop, so without terminating
    /// it druntime blocks on it when main() returns (slow/hung SIGTERM). Call
    /// once at shutdown, after the main event loop has returned.
    void stop() nothrow
    {
        if (raftPool is null)
            return;
        try
            (cast(TaskPool) raftPool).terminate();
        catch (Exception)
        {
        }
    }

    // --- status (main loop reads) ---

    @property bool isLeader() nothrow
    {
        return atomicLoad(leaderFlag);
    }

    @property NodeId leaderId() nothrow
    {
        return cast(NodeId) atomicLoad(leaderIdVal);
    }

    @property Index snapshotIndex() nothrow
    {
        return cast(Index) atomicLoad(snapIndexVal);
    }

    /// Current voting members (for RAFT STATUS / membership ops). Copies the
    /// published snapshot into a main-loop-owned view and returns a slice of it.
    const(NodeId)[] members() nothrow
    {
        size_t n;
        {
            metaMtx.lock_nothrow();
            scope (exit)
                metaMtx.unlock_nothrow();
            n = membersLen;
            foreach (i; 0 .. n)
                membersView[i] = membersBuf[i];
        }
        return membersView[0 .. n];
    }

    // --- pending-slot pool (main loop only) ---

    private Pending* acquireSlot() nothrow
    {
        Pending* p;
        if (freeSlots.length)
        {
            auto s = freeSlots[];
            p = s[s.length - 1];
            freeSlots.popBack();
        }
        else
        {
            p = new Pending; // GC: one-time as the pool grows, then reused
            p.done = createManualEvent();
        }
        p.ready = false;
        p.failed = false;
        p.ackResult = false;
        p.idx = 0; // set when proposed; a reused slot must not carry a stale index
        p.reply.clear();
        return p;
    }

    private void releaseSlot(Pending* p) nothrow
    {
        freeSlots.put(p);
    }

    // --- client write: propose [clock][rawCmd], await commit+apply ---

    /// Fire a write WITHOUT waiting; returns an opaque handle, or null if this
    /// node is not the leader (caller redirects). Lets a connection pipeline its
    /// consecutive writes: proposeAsync them all, then awaitWrite each in order.
    /// The handle MUST be passed to awaitWrite exactly once (to reap the reply
    /// and release the slot).
    void* proposeAsync(scope const(ubyte)[] rawCmd, ulong clock, ushort db) nothrow
    {
        if (!atomicLoad(leaderFlag))
            return null;
        auto slot = acquireSlot();
        // entry payload = 8-byte clock header + 2-byte db index + raw command
        // bytes, built into the slot's own buffer so it stays valid across a
        // backpressure wait. The db index routes the apply to gDbs[db] (multi-db).
        slot.reqBuf.clear();
        ubyte[10] hdr = void;
        foreach (i; 0 .. 8)
            hdr[i] = cast(ubyte)(clock >> (8 * i));
        hdr[8] = cast(ubyte) db;
        hdr[9] = cast(ubyte)(db >> 8);
        slot.reqBuf.append(hdr[]);
        slot.reqBuf.append(rawCmd);
        propQ.put(slot.reqBuf.data, cast(void*) slot, 0, CommitKind.apply);
        return cast(void*) slot;
    }

    /// Wait for a proposeAsync handle to commit+apply, append its reply, release
    /// the slot. Returns false when the proposal was rejected (leadership lost in
    /// flight) — the caller redirects.
    bool awaitWrite(void* handle, ref ByteBuffer o)
    {
        auto slot = cast(Pending*) handle;
        while (!slot.ready)
        {
            auto ec = slot.done.emitCount;
            if (slot.ready)
                break;
            slot.done.wait(ec);
        }
        auto failed = slot.failed;
        if (!failed)
            o.append(slot.reply.data);
        releaseSlot(slot);
        return !failed;
    }

    /// Synchronous write (used inside MULTI/EXEC and by non-pipelined callers).
    /// Returns false when this node is not the leader or the proposal is rejected
    /// because leadership was lost in flight.
    bool proposeWrite(scope const(ubyte)[] rawCmd, ulong clock, ushort db, ref ByteBuffer o)
    {
        auto h = proposeAsync(rawCmd, clock, db);
        if (h is null)
            return false;
        return awaitWrite(h, o);
    }

    /// Leader-only membership change: adds the peer addresses so we can reach
    /// them, then proposes a joint config to the target member set. Blocks for
    /// the raft loop's result.
    bool changeMembership(scope const(NodeId)[] newMembers, scope const(PeerAddress)[] newPeers)
    {
        auto slot = acquireSlot();
        slot.reqBuf.clear();
        encodeMembership(slot.reqBuf, newMembers, newPeers);
        ctlQ.put(slot.reqBuf.data, cast(void*) slot, 0, CtlKind.membership);
        while (!slot.ready)
        {
            auto ec = slot.done.emitCount;
            if (slot.ready)
                break;
            slot.done.wait(ec);
        }
        auto ok = slot.ackResult;
        releaseSlot(slot);
        return ok;
    }

    /// Forces log compaction now (RAFT COMPACT / ops): dumps the keyspace at the
    /// current applied index and hands it to the raft loop to discard the
    /// covered entries.
    void forceCompact()
    {
        if (appliedIndex == 0)
            return;
        sendCompaction();
    }

    // --- main-loop applier ---

    // Drains committed entries from the raft loop and applies them to the
    // keyspace in log order (single consumer => sequential apply, as Raft
    // requires). Reused thread-locals keep it allocation-free per entry.
    private void applyLoop() nothrow
    {
        static Arena arena;
        static ByteBuffer reply;
        static ByteBuffer payload;
        while (true)
        {
            try
            {
                commitQ.waitData();
                void* tag;
                ulong meta;
                uint kind;
                while (commitQ.take(payload, tag, meta, kind))
                {
                    final switch (cast(CommitKind) kind)
                    {
                    case CommitKind.apply:
                        applyOne(payload.data, tag, arena, reply);
                        appliedIndex = meta;
                        break;
                    case CommitKind.snapshot:
                        loadSnapshot(payload.data, arena, reply);
                        appliedIndex = meta;
                        break;
                    case CommitKind.fail:
                        if (tag !is null)
                            wakeSlot(cast(Pending*) tag, true, false);
                        break;
                    case CommitKind.membershipAck:
                        if (tag !is null)
                            wakeSlot(cast(Pending*) tag, false, meta != 0);
                        break;
                    }
                }
                atomicStore(appliedIndexPub, appliedIndex); // for the compaction policy
                maybeCompact();
            }
            catch (Exception)
            {
            }
        }
    }

    private void wakeSlot(Pending* slot, bool failed, bool ackResult) nothrow
    {
        slot.failed = failed;
        slot.ackResult = ackResult;
        slot.ready = true;
        slot.done.emit();
    }

    // Apply one committed [clock:8][db:2][rawCmd] entry to the keyspace,
    // resolving it against its logged clock and routing to its logged db;
    // fill the reply slot for a locally-proposed one.
    private void applyOne(scope const(ubyte)[] p, void* tag, ref Arena arena, ref ByteBuffer reply) nothrow
    {
        if (p == NOOP_PAYLOAD || p.length < 10)
        {
            if (tag !is null) // a NOOP with a slot shouldn't happen, but never hang
                wakeSlot(cast(Pending*) tag, false, false);
            return;
        }
        ulong clock = 0;
        foreach (i; 0 .. 8)
            clock |= cast(ulong) p[i] << (8 * i);
        size_t db = cast(size_t)(p[8] | (cast(uint) p[9] << 8));
        if (db >= NUM_DBS)
            db = 0; // corrupt index — never index out of bounds
        auto raw = cast(const(ubyte)[]) p[10 .. $];

        arena.reset();
        reply.clear();
        RVal cmd;
        size_t pos = 0;
        if (parseValue(raw, pos, arena, cmd) != ParseStatus.ok)
        {
            if (tag !is null)
                wakeSlot(cast(Pending*) tag, false, false);
            return;
        }
        import dreads.commands : dispatch;

        dispatch(cmd, gDbs[db], reply, arena, clock); // injected clock, routed db
        if (tag !is null)
        {
            auto slot = cast(Pending*) tag;
            slot.reply.clear();
            slot.reply.append(reply.data);
            wakeSlot(slot, false, false);
        }
    }

    /// Replaces the keyspace with a snapshot (a canonical command dump).
    private void loadSnapshot(scope const(ubyte)[] data, ref Arena arena, ref ByteBuffer sink) nothrow
    {
        try
        {
            foreach (ref d; gDbs) // a snapshot is authoritative for EVERY db
                d.clear();
            import dreads.commands : dispatch;
            import dreads.aof : aofIsSelect;

            auto curKs = keys; // a `SELECT <n>` re-points into gDbs
            size_t pos = 0;
            while (pos < data.length)
            {
                RVal cmd;
                if (parseValue(data, pos, arena, cmd) != ParseStatus.ok)
                    break;
                int selDb;
                if (aofIsSelect(cmd, selDb))
                {
                    curKs = &gDbs[selDb];
                    continue;
                }
                sink.clear();
                dispatch(cmd, *curKs, sink, arena);
                arena.reset();
            }
        }
        catch (Exception)
        {
        }
    }

    // If the raft loop signalled it wants a compaction and we have applied new
    // entries since the last one, dump the keyspace at the applied index and
    // hand it over. The keyspace is ours (main loop), so only we can snapshot.
    private void maybeCompact() nothrow
    {
        if (!atomicLoad(compactWanted))
            return;
        if (appliedIndex <= lastCompactApplied)
            return;
        try
            sendCompaction();
        catch (Exception)
        {
        }
        atomicStore(compactWanted, false); // raft re-raises if still oversized
    }

    private void sendCompaction()
    {
        import dreads.aof : dumpAllKeyspaces;

        import dreads.acl : aclDumpUsers;

        static ByteBuffer dump;
        dump.clear();
        dumpAllKeyspaces(dump); // every non-empty db, SELECT-framed
        aclDumpUsers(dump); // global ACL registry rides in the snapshot too
        ctlQ.put(dump.data, null, appliedIndex, CtlKind.compact);
        lastCompactApplied = appliedIndex;
    }

    // Entry point on the dedicated raft thread. Constructs the worker (whose
    // node/transport/log are thus affine to this thread) and runs it forever.
    private static void raftEntry() nothrow
    {
        try
        {
            auto w = new RaftWorker(gReplicator);
            w.run();
        }
        catch (Exception)
        {
        }
    }
}

// ---------------------------------------------------------------------------
// Raft-thread worker — owns the node, transport and log; never touches gKeys.
// ---------------------------------------------------------------------------

private final class RaftWorker
{
    private Replicator rep;
    private RaftLog log;
    private RaftNode* node;
    private VibeTransport transport;
    private TaskMutex nodeMtx; // serialises node access across this thread's fibers
    private Pending*[RING] pendingByIndex; // index -> client slot (or null)

    this(Replicator rep)
    {
        this.rep = rep;
    }

    void run()
    {
        log = RaftLog.open(rep.logBase);
        assert(log !is null, "cannot open raft log");
        log.enableAsyncDurability();
        node = new RaftNode(rep.bootCfg, log);
        transport = new VibeTransport(rep.bootCfg.self, rep.bootPeers);
        transport.setHandler(&onWire);
        // Optional LZ4 wire compression. Always install the decompressor so we
        // understand a compressing peer even when our own outbound is plaintext
        // (rolling config change); the compressor is installed only when the
        // `raft-compress` flag is on. dreads owns liblz4 — draft stays codec-free.
        {
            import dreads.lz4 : lz4Compress, lz4Decompress;

            transport.setCompression(rep.compress ? &lz4Compress : null, &lz4Decompress);
        }
        nodeMtx = new TaskMutex;
        transport.start(rep.raftPort);
        cast(void) setTimer(20.msecs, () @trusted nothrow { onTick(); }, true);
        runTask(() nothrow { ctlLoop(); });
        proposalLoop(); // never returns: this fiber IS the group-commit drainer
    }

    // Group-commit: drain every queued proposal, append them all, then flush +
    // replicate in one cycle. Turns N concurrent per-write round-trips into one
    // batched broadcast (throughput), and the dedicated loop keeps ack-handling
    // punctual regardless of client load (stability).
    private void proposalLoop() nothrow
    {
        static ByteBuffer buf;
        Pending*[MSG_CAP] failed = void;
        while (true)
        {
            try
            {
                rep.propQ.waitData();
                size_t nFail = 0;
                {
                    nodeMtx.lock();
                    scope (exit)
                        nodeMtx.unlock();
                    void* tag;
                    ulong meta;
                    uint kind;
                    while (rep.propQ.take(buf, tag, meta, kind))
                    {
                        auto idx = node.proposeLocal(buf.data);
                        if (idx == 0)
                        {
                            if (nFail < MSG_CAP)
                                failed[nFail++] = cast(Pending*) tag;
                        }
                        else
                        {
                            auto slot = cast(Pending*) tag;
                            slot.idx = idx; // so a step-down can fail uncommitted ones
                            pendingByIndex[idx % RING] = slot;
                        }
                    }
                    node.flush();
                    auto rd = node.takeReady();
                    processReadyLocked(rd);
                }
                // Reject lost-leadership proposals after releasing the lock.
                foreach (i; 0 .. nFail)
                    rep.commitQ.put(null, cast(void*) failed[i], 0, CommitKind.fail);
            }
            catch (Exception)
            {
            }
        }
    }

    // Membership + compaction commands from the main loop (off the hot path).
    private void ctlLoop() nothrow
    {
        static ByteBuffer buf;
        while (true)
        {
            try
            {
                rep.ctlQ.waitData();
                void* tag;
                ulong meta;
                uint kind;
                while (rep.ctlQ.take(buf, tag, meta, kind))
                {
                    final switch (cast(CtlKind) kind)
                    {
                    case CtlKind.membership:
                        applyMembership(buf.data, tag);
                        break;
                    case CtlKind.compact:
                        {
                            nodeMtx.lock();
                            scope (exit)
                                nodeMtx.unlock();
                            node.compact(cast(Index) meta, buf.data);
                            publishStatus();
                        }
                        break;
                    }
                }
            }
            catch (Exception)
            {
            }
        }
    }

    private void applyMembership(scope const(ubyte)[] data, void* tag) nothrow
    {
        NodeId[64] members = void;
        size_t nMembers;
        PeerAddress[16] peers = void;
        size_t nPeers;
        auto ok = decodeMembership(data, members, nMembers, peers, nPeers);
        bool result = false;
        if (ok)
        {
            try
            {
                nodeMtx.lock();
                scope (exit)
                    nodeMtx.unlock();
                foreach (i; 0 .. nPeers)
                    transport.addPeer(peers[i]); // may throw (connect setup)
                result = node.changeMembership(members[0 .. nMembers]);
                auto rd = node.takeReady();
                processReadyLocked(rd);
            }
            catch (Exception)
            {
                result = false;
            }
        }
        // Always ack so the caller never hangs, even on a decode/connect failure.
        rep.commitQ.put(null, tag, result ? 1 : 0, CommitKind.membershipAck);
    }

    // The Ready cycle, run ENTIRELY under nodeMtx (the caller holds it). Sending
    // under the same fiber-lock that guards compaction makes the two mutually
    // exclusive: a compaction can never run mid-send and pull the snapshot/log
    // slices out from under an outgoing message. transport.send only enqueues,
    // and awaitDurable / commitQ.put merely yield the fiber — the fiber-lock is
    // safe to hold across a yield (other raft fibers wait; new proposals still
    // queue in propQ on the main loop, so group-commit batching is unaffected).
    private void processReadyLocked(ref Ready rd) nothrow
    {
        // A conflicting append just truncated our uncommitted tail: those entries
        // will never commit, so redirect their waiting clients (and clear the
        // ring slots before the replacement entries reuse them).
        if (rd.truncatedFrom > 0)
            failTruncated(rd.truncatedFrom);
        // A leader's snapshot must reach the keyspace before its later entries:
        // ship it first, in order, through the same commit stream.
        if (rd.applySnapshot !is null)
            rep.commitQ.put(rd.applySnapshot.data, null,
                    cast(ulong) rd.applySnapshot.lastIncludedIndex, CommitKind.snapshot);
        if (rd.persistUpto > 0)
            log.awaitDurable(rd.persistUpto); // yields under the fiber-lock
        node.onPersisted(rd.persistUpto);
        auto committed = node.takeCommitted();
        shipCommitted(committed);
        // rd.messages is node-owned (takeReady snapshot); safe to send here
        // because no other cycle can run takeReady while we hold the lock.
        foreach (ref m; rd.messages)
            transport.send(m);
        maybeCompactSignal();
        publishStatus();
    }

    // Ship committed entries to the main loop, pairing each with its pending
    // client slot (consumed once, so a re-applied index never double-fires).
    private void shipCommitted(const(LogEntry)[] committed) nothrow
    {
        foreach (ref e; committed)
        {
            Pending* slot = null;
            auto slotp = pendingByIndex[e.index % RING];
            if (slotp !is null)
            {
                slot = slotp;
                pendingByIndex[e.index % RING] = null;
            }
            rep.commitQ.put(e.payload, cast(void*) slot, cast(ulong) e.index, CommitKind.apply);
        }
    }

    // Compaction policy (all raft-thread state). Compaction rewrites the log
    // file while holding nodeMtx, stalling every proposal — so it must be RARE
    // and only when it actually reclaims something. Gate on the retained log's
    // BYTE size, not entry count: a hot single-key workload has a tiny snapshot
    // but a huge op count, and compacting its small, recent log every few
    // thousand ops is pure churn. Below the floor we never compact.
    //
    // We do NOT defer for lagging followers. Shipping a snapshot (a few MB,
    // lz4-compressible) is cheap on any modern link, so stranding a laggard on
    // a snapshot-install is fine — far better than letting the log grow
    // unbounded just to keep feeding it entries. The main loop (keyspace owner)
    // supplies the snapshot when we signal.
    private enum COMPACT_MIN_BYTES = 64 * 1024 * 1024; // worth a rewrite
    private void maybeCompactSignal() nothrow
    {
        // Gate on RECLAIMABLE bytes (entries the keyspace has already applied and
        // could drop), not total retained size — otherwise a large un-applied
        // tail keeps us above the threshold and we compact on every write. This
        // resets to ~0 right after each compaction, so we compact once per 64MB
        // of newly-applied log, never per write.
        auto applied = cast(Index) atomicLoad(rep.appliedIndexPub);
        if (log.reclaimableBytes(applied) >= COMPACT_MIN_BYTES)
            atomicStore(rep.compactWanted, true);
    }

    // Fail every pending client write at index >= `from`: those log entries were
    // just truncated (a new leader overwrote this node's uncommitted tail), so
    // they will never commit — the waiting client must be redirected, not left
    // hanging, and the slot must be cleared before the replacement entries reuse
    // the same ring positions. This is driven by the node reporting the exact
    // truncation point (Ready.truncatedFrom); step-down alone is NOT enough,
    // since an uncommitted entry may still commit via a follower that had it.
    private void failTruncated(Index from) nothrow
    {
        if (from == 0)
            return;
        foreach (i; 0 .. RING)
        {
            auto slot = pendingByIndex[i];
            if (slot !is null && slot.idx >= from)
            {
                pendingByIndex[i] = null;
                rep.commitQ.put(null, cast(void*) slot, 0, CommitKind.fail);
            }
        }
    }

    private void publishStatus() nothrow
    {
        atomicStore(rep.leaderFlag, node.currentRole == Role.leader);
        atomicStore(rep.leaderIdVal, cast(uint) node.currentLeader);
        atomicStore(rep.snapIndexVal, cast(ulong) log.snapshotIndex);
        auto ms = node.members;
        auto n = ms.length > rep.membersBuf.length ? rep.membersBuf.length : ms.length;
        rep.metaMtx.lock_nothrow();
        foreach (i; 0 .. n)
            rep.membersBuf[i] = ms[i];
        rep.membersLen = n;
        rep.metaMtx.unlock_nothrow();
    }

    private void onTick() nothrow
    {
        try
        {
            nodeMtx.lock();
            scope (exit)
                nodeMtx.unlock();
            node.tick();
            auto rd = node.takeReady();
            processReadyLocked(rd);
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
            scope (exit)
                nodeMtx.unlock();
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
            processReadyLocked(rd);
        }
        catch (Exception)
        {
        }
    }
}

// ---------------------------------------------------------------------------
// Membership (de)serialization for the ctlQ hand-off.
//   [u32 nMembers] nMembers x u32 id
//   [u32 nPeers]   nPeers x (u32 id, u32 hostLen, host bytes, u16 port)
// ---------------------------------------------------------------------------

private void putU32(ref ByteBuffer b, uint v) nothrow
{
    ubyte[4] x = void;
    foreach (i; 0 .. 4)
        x[i] = cast(ubyte)(v >> (8 * i));
    b.append(x[]);
}

private void encodeMembership(ref ByteBuffer b, scope const(NodeId)[] members,
        scope const(PeerAddress)[] peers) nothrow
{
    b.clear();
    putU32(b, cast(uint) members.length);
    foreach (m; members)
        putU32(b, cast(uint) m);
    putU32(b, cast(uint) peers.length);
    foreach (ref p; peers)
    {
        putU32(b, cast(uint) p.id);
        putU32(b, cast(uint) p.host.length);
        b.append(cast(const(ubyte)[]) p.host);
        ubyte[2] port = [cast(ubyte)(p.port & 0xff), cast(ubyte)(p.port >> 8)];
        b.append(port[]);
    }
}

private uint getU32(scope const(ubyte)[] d, ref size_t pos, ref bool ok) nothrow
{
    if (pos + 4 > d.length)
    {
        ok = false;
        return 0;
    }
    uint v = d[pos] | (cast(uint) d[pos + 1] << 8) | (cast(uint) d[pos + 2] << 16) | (
            cast(uint) d[pos + 3] << 24);
    pos += 4;
    return v;
}

private bool decodeMembership(scope const(ubyte)[] d, ref NodeId[64] members, out size_t nMembers,
        ref PeerAddress[16] peers, out size_t nPeers) nothrow
{
    bool ok = true;
    size_t pos = 0;
    auto nm = getU32(d, pos, ok);
    if (!ok || nm > members.length)
        return false;
    foreach (i; 0 .. nm)
        members[i] = cast(NodeId) getU32(d, pos, ok);
    if (!ok)
        return false;
    nMembers = nm;
    auto np = getU32(d, pos, ok);
    if (!ok || np > peers.length)
        return false;
    foreach (i; 0 .. np)
    {
        auto id = getU32(d, pos, ok);
        auto hlen = getU32(d, pos, ok);
        if (!ok || pos + hlen + 2 > d.length)
            return false;
        auto host = cast(string) d[pos .. pos + hlen].idup;
        pos += hlen;
        ushort port = cast(ushort)(d[pos] | (cast(ushort) d[pos + 1] << 8));
        pos += 2;
        peers[i] = PeerAddress(cast(NodeId) id, host, port);
    }
    nPeers = np;
    return true;
}
